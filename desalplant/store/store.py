"""File-backed key space with generation versioning, checksums and crash recovery.

The store keeps the whole document in memory and persists it as a self-describing
envelope (format, generation, checksum, data). Every flush is crash-safe: the new
generation is written to a temporary file, fsynced, renamed into place and the
directory is fsynced, so a crash mid-write leaves either the old or the new
generation intact, never a half-written one.

On load the head is validated against its checksum. When it is incomplete or
corrupt the store rolls back to the newest valid retained generation, records the
rollback in the audit log, and rewrites the head so a repeated recovery is a
no-op. Recovery is therefore idempotent: ``recover`` can be called any number of
times without moving an already recovered state twice. The in-memory state and the
on-disk version can be reconciled at any time with ``verify_consistency``.
"""

import os
import threading
from typing import Any

from ..errors import StoreError, UnknownKeyError
from ..ns.ids import new_id
from . import generations, keys, snapshot as snapshot_module
from .checksum import digest
from .envelope import build as build_envelope
from .query import select
from .recovery import RecoveryReport, RollbackRecord

ROLLBACK_KIND = "rollback"


class Store:
    """A small persisted, versioned mapping used as the service's source of truth."""

    def __init__(self, path: str | None = None) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._generation = 0
        self._checksum = digest({})
        self._recovered = False
        self._rollbacks: list[RollbackRecord] = []
        self._recovery_report: RecoveryReport | None = None
        self.load()

    @property
    def path(self) -> str | None:
        """Location of the head file, if any."""
        return self._path

    @property
    def generation(self) -> int:
        """Generation number of the document currently in effect."""
        return self._generation

    @property
    def recovered(self) -> bool:
        """Whether the most recent recovery pass had to roll back."""
        return self._recovered

    @property
    def rollback_count(self) -> int:
        """How many rollbacks have been recorded so far."""
        return len(self._rollbacks)

    def rollbacks(self) -> tuple[RollbackRecord, ...]:
        """Recorded rollbacks, oldest first."""
        return tuple(self._rollbacks)

    def recovery_report(self) -> RecoveryReport | None:
        """Report from the most recent recovery pass, if any."""
        return self._recovery_report

    # ------------------------------------------------------------- loading

    def load(self) -> None:
        """Read the head, validating it and rolling back when it is corrupt.

        A missing or empty head with no retained generations is a fresh start:
        no rollback is recorded. When valid retained generations exist, or the
        head has content that fails validation, the store rolls back to the
        newest valid generation.
        """
        if self._path is None:
            self._recovery_report = self._noop_report()
            return
        head = self._read_head()
        if head is not None and head.valid():
            self._apply_head(head)
            return
        directory = self._directory()
        target = generations.newest_valid(directory)
        if target is None and self._head_is_missing_or_empty():
            self._apply_empty()
            return
        self._recover(head, target)

    def _read_head(self):
        """The head envelope (even when invalid), or ``None`` when unreadable."""
        head = generations.read_envelope(self._path)
        if head is None:
            head = generations.read_legacy(self._path)
        return head

    def _apply_head(self, head) -> None:
        """Set the in-memory state from a valid head envelope."""
        self._data = dict(head.data)
        self._generation = head.generation
        self._checksum = head.checksum
        self._recovered = False
        self._recovery_report = self._noop_report()

    def _apply_empty(self) -> None:
        """Set the in-memory state to the initial empty document."""
        self._data = {}
        self._generation = 0
        self._checksum = digest({})
        self._recovered = False
        self._recovery_report = self._noop_report()

    def _head_is_missing_or_empty(self) -> bool:
        """Whether the head file is absent or holds only whitespace."""
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                text = handle.read()
        except OSError:
            return True
        return not text.strip()

    def _noop_report(self) -> RecoveryReport:
        return RecoveryReport(
            rolled_back=False,
            generation=self._generation,
            checksum=self._checksum,
            rollback_count=len(self._rollbacks),
            record=None,
        )

    def _directory(self) -> str:
        return os.path.dirname(self._path) or "."

    def _recover(self, head, target=None) -> None:
        """Roll back to the newest valid generation, then persist the result."""
        directory = self._directory()
        if target is None:
            target = generations.newest_valid(directory)
        if target is None:
            self._data = {}
            self._generation = 0
            self._checksum = digest({})
            to_generation = 0
            to_checksum = self._checksum
            reason = "no valid generation found"
        else:
            self._data = dict(target.data)
            self._generation = target.generation
            self._checksum = target.checksum
            to_generation = target.generation
            to_checksum = target.checksum
            reason = "head failed validation"
        self._recovered = True

        from_generation = head.generation if head is not None else 0
        from_checksum = head.checksum if head is not None else ""

        record = self._record_rollback(
            from_generation=from_generation,
            to_generation=to_generation,
            from_checksum=from_checksum,
            to_checksum=to_checksum,
            reason=reason,
        )
        self._persist_recovered(directory)

        self._rollbacks.append(record)
        self._recovery_report = RecoveryReport(
            rolled_back=True,
            generation=self._generation,
            checksum=self._checksum,
            rollback_count=len(self._rollbacks),
            record=record,
        )

    def _record_rollback(
        self,
        from_generation: int,
        to_generation: int,
        from_checksum: str,
        to_checksum: str,
        reason: str,
    ) -> RollbackRecord:
        """Append a rollback event to the audit record and return it."""
        sequence = int(self._data.get(keys.AUDIT_SEQUENCE, 0) or 0) + 1
        event = {
            "id": new_id(),
            "sequence": sequence,
            "kind": ROLLBACK_KIND,
            "detail": (
                f"rolled back from generation {from_generation} to "
                f"{to_generation}: {reason}"
            ),
            "tick": 0,
        }
        self._data[keys.AUDIT_SEQUENCE] = sequence
        self._data[keys.audit_event_key(sequence)] = event
        self._data[keys.audit_last_key(ROLLBACK_KIND)] = sequence
        return RollbackRecord(
            sequence=sequence,
            from_generation=from_generation,
            to_generation=to_generation,
            from_checksum=from_checksum,
            to_checksum=to_checksum,
            reason=reason,
        )

    def _next_generation(self, directory: str) -> int:
        """Generation number strictly above every retained generation."""
        highest = self._generation
        for generation in generations.list_generations(directory):
            if generation > highest:
                highest = generation
        return highest + 1

    def _persist_recovered(self, directory: str) -> None:
        """Persist the rolled-back document as a new generation."""
        generation = self._next_generation(directory)
        envelope = build_envelope(generation, self._data)
        generations.write_atomic(
            generations.generation_path(directory, generation), envelope
        )
        generations.write_atomic(self._path, envelope)
        self._generation = generation
        self._checksum = envelope.checksum

    # ------------------------------------------------------------- access

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

    # ------------------------------------------------------- consistency

    def verify_consistency(self) -> bool:
        """Whether the in-memory state matches the committed checksum and head.

        Recomputes the digest of the in-memory document, compares it to the
        committed checksum, and re-reads the head file to confirm the on-disk
        generation and checksum agree. Detects drift between the runtime state
        and the persisted version.
        """
        with self._lock:
            if digest(self._data) != self._checksum:
                return False
            if self._path is None:
                return True
            head = generations.read_valid_envelope(self._path)
            if head is None:
                # No head on disk: consistent only at the initial empty state.
                return self._generation == 0 and not self._data
            return (
                head.generation == self._generation
                and head.checksum == self._checksum
            )

    def recover(self) -> RecoveryReport:
        """Reconcile in-memory state with the on-disk version, idempotently.

        Safe to call repeatedly: when the head is valid it is a no-op and never
        rolls back an already recovered state. When the head is corrupt it rolls
        back to the newest valid retained generation, records the rollback, and
        rewrites the head so the next call is a no-op.
        """
        with self._lock:
            if self._path is None:
                return self._recovery_report or self._noop_report()
            head = self._read_head()
            if head is not None and head.valid():
                if (
                    head.generation != self._generation
                    or head.checksum != self._checksum
                ):
                    self._apply_head(head)
                self._recovered = False
                self._recovery_report = self._noop_report()
                return self._recovery_report
            directory = self._directory()
            target = generations.newest_valid(directory)
            if target is None and self._head_is_missing_or_empty():
                self._apply_empty()
                return self._recovery_report
            self._recover(head, target)
            return self._recovery_report

    # ------------------------------------------------------------ flush

    def flush(self) -> None:
        """Rewrite the document atomically as a new generation."""
        if self._path is None:
            return
        with self._lock:
            directory = self._directory()
            generation = self._next_generation(directory)
            envelope = build_envelope(generation, self._data)
            generations.write_atomic(
                generations.generation_path(directory, generation), envelope
            )
            generations.write_atomic(self._path, envelope)
            self._generation = generation
            self._checksum = envelope.checksum
            self._recovered = False
