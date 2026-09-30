"""File-backed key space with versioned, crash-safe commits.

Persistence guarantees:

* Every commit writes an envelope (version number plus a checksum of the rows)
  to a temporary file, fsyncs it and publishes it with ``os.replace``: a crash
  leaves either the old whole document or the new whole document, never half a
  document.
* The previous good document is published atomically (hard link) before every
  commit, so when a later document fails verification on read the store rolls
  back to it. Rejected bytes are quarantined, never deleted.
* Every rollback or reset is journaled first in an append-only recovery log,
  keyed by the checksum of the rejected bytes. Repeating recovery therefore
  neither repeats the repair nor records it twice.
"""

import os
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from ..errors import StoreError, UnknownKeyError
from . import persistence, snapshot as snapshot_module
from .checksum import digest
from .codec import encode
from .query import select
from .persistence import CorruptDocument, ParsedDocument, inspect_bytes, raw_digest
from .recovery import (
    LEGACY_IMPORT,
    RESET,
    ROLLBACK,
    RecoveryEvent,
    RecoveryJournal,
)

MAIN_SUFFIX_TMP = ".tmp"
PREV_SUFFIX = ".prev"
PREV_TMP_SUFFIX = ".prevtmp"
JOURNAL_SUFFIX = ".recovery.log"
CORRUPT_DIR_SUFFIX = ".corrupt"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fsync_directory(directory: str) -> None:
    """Flush a directory's rename/link metadata so a crash cannot undo it."""
    if not directory:
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class RecoveryReport:
    """Outcome of one recovery pass."""

    action: str
    event: RecoveryEvent | None
    quarantined: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "event": self.event.to_dict() if self.event is not None else None,
            "quarantined": list(self.quarantined),
        }


class Store:
    """A small persisted mapping used as the service's source of truth."""

    def __init__(
        self, path: str | None = None, *, now: Callable[[], str] | None = None
    ) -> None:
        self._path = path
        self._now = now or _utc_now
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._version = 0
        self._committed_checksum: str | None = None
        self._dirty = False
        self._journal = RecoveryJournal(self.journal_path) if path is not None else None
        self._boot = RecoveryReport("none", None, ())
        self.load()

    @property
    def path(self) -> str | None:
        """Location of the persisted document, if any."""
        return self._path

    @property
    def prev_path(self) -> str | None:
        """Location holding the previous good document."""
        return f"{self._path}{PREV_SUFFIX}" if self._path is not None else None

    @property
    def journal_path(self) -> str | None:
        """Location of the append-only recovery journal."""
        return f"{self._path}{JOURNAL_SUFFIX}" if self._path is not None else None

    @property
    def boot_report(self) -> RecoveryReport:
        """What startup recovery did before adopting the active version."""
        return self._boot

    # ------------------------------------------------------------- load/recover

    def load(self) -> None:
        """Reconcile the disk state and adopt the verified active version.

        A missing or blank file leaves the store empty. A corrupt file rolls
        back to the previous good document; with no previous document it resets
        to empty and quarantines the rejected bytes.
        """
        with self._lock:
            self._boot = self._reconcile(initial=True)

    def recover(self) -> RecoveryReport:
        """Run recovery again; a healthy disk makes this a no-op.

        Safe to call any number of times: an already repaired document is
        neither rolled back again nor journaled again.
        """
        with self._lock:
            return self._reconcile(initial=False)

    def _reconcile(self, *, initial: bool) -> RecoveryReport:
        if self._path is None:
            return RecoveryReport("none", None, ())
        self._cleanup_tempfiles()
        main = self._read_file(self._path)
        prev = self._read_file(self.prev_path)

        if main is None:
            if isinstance(prev, ParsedDocument):
                return self._restore_prev(prev, reason="missing-document", initial=initial)
            if initial:
                self._adopt_empty()
            return RecoveryReport("fresh", None, ())

        if isinstance(main, ParsedDocument):
            if main.legacy:
                return self._import_legacy(main, initial=initial)
            if initial:
                self._adopt(main)
            return RecoveryReport("loaded", None, ())

        return self._repair_corrupt(main, prev, initial=initial)

    def _import_legacy(self, parsed: ParsedDocument, *, initial: bool) -> RecoveryReport:
        """Record the one-off import of a pre-envelope bare JSON document."""
        event = self._legacy_event(parsed, raw_digest(self._raw(self._path)))
        appended = self._journal.append_once(event)
        if initial:
            self._adopt(parsed)
        return RecoveryReport(
            LEGACY_IMPORT if appended else "loaded",
            event if appended else None,
            (),
        )

    def _repair_corrupt(
        self,
        main: CorruptDocument,
        prev: ParsedDocument | CorruptDocument | None,
        *,
        initial: bool,
    ) -> RecoveryReport:
        if isinstance(prev, ParsedDocument):
            return self._restore_prev(prev, reason=main.reason, corrupt=main, initial=initial)
        quarantined: list[str] = [self._quarantine("main", main.evidence_checksum)]
        if isinstance(prev, CorruptDocument):
            # _quarantine already removes the rejected slot itself.
            quarantined.append(self._quarantine("prev", prev.evidence_checksum))
        event = self._reset_event(main)
        # Journaled after quarantine but the identity makes a retry harmless.
        self._journal.append_once(event)
        self._adopt_empty()
        self._version = self._highest_version()
        return RecoveryReport(RESET, event, tuple(quarantined))

    def _restore_prev(
        self,
        prev: ParsedDocument,
        *,
        reason: str,
        corrupt: CorruptDocument | None = None,
        initial: bool,
    ) -> RecoveryReport:
        quarantined: tuple[str, ...] = ()
        if corrupt is not None:
            quarantined = (self._quarantine("main", corrupt.evidence_checksum),)
        event = self._rollback_event(
            reason=reason,
            corrupt=corrupt,
            restored=prev,
            evidence=corrupt.evidence_checksum
            if corrupt is not None
            else raw_digest(b""),
        )
        # Journal first: a crash between this line and the restore simply
        # replays the same idempotent rollback on the next start. Whether the
        # line was new or replayed, this pass physically restores the document.
        self._journal.append_once(event)
        self._publish_bytes(self._path, self._raw(self.prev_path))
        if initial:
            self._adopt(prev)
        return RecoveryReport(ROLLBACK, event, quarantined)

    # -------------------------------------------------------------- accessors

    def put(self, key: str, value: Any) -> None:
        """Write a key and flush the document."""
        if not key:
            raise StoreError("key must not be empty")
        with self._lock:
            self._data[key] = value
            self.flush()

    def get(self, key: str, default: Any = None) -> Any:
        """Read a key, returning ``default`` when it was never written."""
        with self._lock:
            return self._data.get(key, default)

    def require(self, key: str) -> Any:
        """Read a key, raising when it was never written."""
        with self._lock:
            if key not in self._data:
                raise UnknownKeyError(f"no stored value for {key}")
            return self._data[key]

    def has(self, key: str) -> bool:
        """Whether a key holds a value."""
        with self._lock:
            return key in self._data

    def keys(self, prefix: str = "") -> tuple[str, ...]:
        """Sorted keys under a prefix."""
        with self._lock:
            return select(self._data.keys(), prefix)

    def count(self) -> int:
        """Number of stored keys."""
        with self._lock:
            return len(self._data)

    def export(self) -> dict[str, Any]:
        """A detached copy of the whole mapping."""
        with self._lock:
            return dict(self._data)

    def checksum(self) -> str:
        """Digest of the current in-memory content."""
        with self._lock:
            return digest(self._data)

    def verify(self, checksum: str) -> bool:
        """Whether ``checksum`` still describes the current content."""
        return self.checksum() == checksum

    def snapshot(self, prefix: str = "") -> snapshot_module.Snapshot:
        """Frozen count, digest and key list."""
        with self._lock:
            return snapshot_module.build(self._data, prefix)

    # ------------------------------------------------------------- versioning

    @property
    def version(self) -> int:
        """Version number of the active document (0 before the first commit)."""
        with self._lock:
            return self._version

    @property
    def next_version(self) -> int:
        """Version number the next commit will carry; versions are never reused."""
        with self._lock:
            return self._highest_version() + 1

    @property
    def dirty(self) -> bool:
        """Whether the memory holds uncommitted changes (e.g. failed flush)."""
        with self._lock:
            return self._dirty

    def committed_checksum(self) -> str | None:
        """Checksum recorded by the active document on disk."""
        with self._lock:
            return self._committed_checksum

    def recovery_events(self) -> tuple[RecoveryEvent, ...]:
        """All journaled recovery actions, oldest first."""
        if self._journal is None:
            return ()
        with self._lock:
            return self._journal.events()

    def recovery_summary(self) -> dict[str, Any]:
        """Rollback counters and history for the console."""
        events = self.recovery_events()
        return {
            "rollback_count": sum(1 for event in events if event.kind == ROLLBACK),
            "reset_count": sum(1 for event in events if event.kind == RESET),
            "events": [event.to_dict() for event in events],
        }

    def consistency(self) -> dict[str, Any]:
        """Compare the in-memory runtime state with the document on disk."""
        with self._lock:
            memory_checksum = digest(self._data)
            result: dict[str, Any] = {
                "path": self._path,
                "memory_version": self._version,
                "memory_checksum": memory_checksum,
                "dirty": self._dirty,
                "disk_present": False,
                "disk_version": None,
                "disk_checksum": None,
                "matches": False,
            }
            if self._path is None:
                result["path"] = None
                result["matches"] = True
                return result
            disk = self._read_file(self._path)
            if isinstance(disk, ParsedDocument) and not disk.legacy:
                result["disk_present"] = True
                result["disk_version"] = disk.version
                result["disk_checksum"] = disk.checksum
                result["matches"] = (
                    disk.version == self._version
                    and disk.checksum == memory_checksum
                    and not self._dirty
                )
            elif disk is None:
                result["matches"] = (
                    not self._dirty and self._version == 0 and memory_checksum == digest({})
                )
            return result

    def persistence_view(self) -> dict[str, Any]:
        """Current version and recovery history, for the console."""
        with self._lock:
            return {
                "path": self._path,
                "format": persistence.ENVELOPE_FORMAT,
                "version": self._version,
                "next_version": self._highest_version() + 1,
                "checksum": self._committed_checksum,
                "rows": len(self._data),
                "dirty": self._dirty,
                "consistent": self.consistency()["matches"],
                "boot_action": self._boot.action,
                "recovery": self.recovery_summary(),
            }

    def boot_summary(self) -> dict[str, Any]:
        """Which version the process actually started from, and why."""
        with self._lock:
            report = self._boot
            view: dict[str, Any] = {
                "action": report.action,
                "version": self._version,
                "checksum": self._committed_checksum,
                "quarantined": list(report.quarantined),
            }
            if report.event is not None:
                event = report.event
                view.update(
                    {
                        "reason": event.reason,
                        "rejected_version": event.rejected_version,
                        "rejected_checksum": event.rejected_checksum,
                        "evidence_checksum": event.evidence_checksum,
                        "restored_version": event.restored_version,
                        "restored_checksum": event.restored_checksum,
                        "at": event.at,
                    }
                )
            return view

    # ----------------------------------------------------------------- commit

    def flush(self) -> None:
        """Commit a new whole document atomically, when a path is configured."""
        if self._path is None:
            self._version += 1
            self._committed_checksum = digest(self._data)
            self._dirty = False
            return
        directory = os.path.dirname(self._path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with self._lock:
            version = self._highest_version() + 1
            envelope = persistence.pack(self._data, version, self._now())
            temporary = f"{self._path}{MAIN_SUFFIX_TMP}"
            try:
                # Preserve the previous good document before publishing the new one.
                self._publish_previous()
                with open(temporary, "w", encoding="utf-8") as handle:
                    handle.write(encode(envelope))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self._path)
                _fsync_directory(directory)
            except OSError as error:
                self._dirty = True
                raise StoreError(
                    f"could not persist {self._path}: {error}"
                ) from error
            self._version = version
            self._committed_checksum = envelope["checksum"]
            self._dirty = False

    def _publish_previous(self) -> None:
        """Atomically turn the current main document into the previous slot."""
        if not os.path.exists(self._path):
            return
        temporary = f"{self._path}{PREV_TMP_SUFFIX}"
        try:
            os.remove(temporary)
        except FileNotFoundError:
            pass
        try:
            os.link(self._path, temporary)
        except OSError:
            # Filesystems without hard links: copy through a temp file instead.
            shutil.copyfile(self._path, temporary)
        os.replace(temporary, self.prev_path)
        _fsync_directory(os.path.dirname(self._path))

    # ---------------------------------------------------------------- helpers

    def _adopt(self, parsed: ParsedDocument) -> None:
        self._data = dict(parsed.rows)
        self._version = 0 if parsed.legacy else parsed.version
        self._committed_checksum = parsed.checksum
        self._dirty = False

    def _adopt_empty(self) -> None:
        self._data = {}
        self._version = 0
        self._committed_checksum = None
        self._dirty = False

    def _highest_version(self) -> int:
        highest = self._version
        for raw in (self._read_file(self._path), self._read_file(self.prev_path)):
            if isinstance(raw, ParsedDocument) and not raw.legacy:
                highest = max(highest, raw.version)
        if self._journal is not None:
            for event in self._journal.events():
                highest = max(highest, event.restored_version)
                if event.rejected_version:
                    highest = max(highest, event.rejected_version)
            # The newest rollback was followed by a failed commit; even when
            # its envelope was too torn to carry a version, it reserved at
            # least restored + 1, so that number must not be reused.
            rollbacks = [
                event for event in self._journal.events() if event.kind == ROLLBACK
            ]
            if rollbacks:
                newest = rollbacks[-1]
                highest = max(highest, newest.restored_version + 1)
        return highest

    def _cleanup_tempfiles(self) -> None:
        for suffix in (MAIN_SUFFIX_TMP, PREV_TMP_SUFFIX):
            stale = f"{self._path}{suffix}"
            if os.path.isfile(stale):
                try:
                    os.remove(stale)
                except FileNotFoundError:
                    pass

    def _raw(self, path: str) -> bytes:
        with open(path, "rb") as handle:
            return handle.read()

    def _read_file(self, path: str | None) -> ParsedDocument | CorruptDocument | None:
        if path is None or not os.path.exists(path):
            return None
        try:
            raw = self._raw(path)
        except OSError as error:
            raise StoreError(f"could not read {path}: {error}") from error
        if not raw.strip():
            return None
        return inspect_bytes(raw)

    def _publish_bytes(self, path: str, raw: bytes) -> None:
        """Publish already verified bytes atomically (rollback restore)."""
        temporary = f"{path}{MAIN_SUFFIX_TMP}"
        with open(temporary, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(os.path.dirname(path))

    def _quarantine(self, role: str, evidence_checksum: str) -> str:
        """Copy rejected bytes into quarantine and remove the rejected slot."""
        source = self._path if role == "main" else self.prev_path
        directory = f"{self._path}{CORRUPT_DIR_SUFFIX}"
        os.makedirs(directory, exist_ok=True)
        target = os.path.join(
            directory,
            f"{os.path.basename(self._path)}.{role}.{evidence_checksum[:12]}",
        )
        shutil.copyfile(source, target)
        os.remove(source)
        _fsync_directory(directory)
        _fsync_directory(os.path.dirname(self._path))
        return target

    def _rollback_event(
        self,
        *,
        reason: str,
        corrupt: CorruptDocument | None,
        restored: ParsedDocument,
        evidence: str,
    ) -> RecoveryEvent:
        return self._event(
            kind=ROLLBACK,
            reason=reason,
            evidence=evidence,
            rejected_checksum=corrupt.rejected_checksum if corrupt is not None else None,
            rejected_version=corrupt.rejected_version if corrupt is not None else None,
            restored=restored,
            recovered_from=self.prev_path,
        )

    def _reset_event(self, corrupt: CorruptDocument) -> RecoveryEvent:
        return self._event(
            kind=RESET,
            reason=corrupt.reason,
            evidence=corrupt.evidence_checksum,
            rejected_checksum=corrupt.rejected_checksum,
            rejected_version=corrupt.rejected_version,
            restored=None,
            recovered_from="",
        )

    def _legacy_event(self, parsed: ParsedDocument, evidence_checksum: str) -> RecoveryEvent:
        return self._event(
            kind=LEGACY_IMPORT,
            reason=persistence.LEGACY_REASON,
            evidence=evidence_checksum,
            rejected_checksum=None,
            rejected_version=None,
            restored=parsed,
            recovered_from=self._path,
        )

    def _event(
        self,
        *,
        kind: str,
        reason: str,
        evidence: str,
        rejected_checksum: str | None,
        rejected_version: int | None,
        restored: ParsedDocument | None,
        recovered_from: str,
    ) -> RecoveryEvent:
        return RecoveryEvent(
            seq=self._next_event_seq(),
            kind=kind,
            reason=reason,
            evidence_checksum=evidence,
            rejected_checksum=rejected_checksum,
            rejected_version=rejected_version,
            restored_version=restored.version
            if restored is not None and not restored.legacy
            else 0,
            restored_checksum=restored.checksum if restored is not None else "",
            at=self._now(),
            recovered_from=recovered_from,
        )

    def _next_event_seq(self) -> int:
        if self._journal is None:
            return 1
        events = self._journal.events()
        return (events[-1].seq + 1) if events else 1
