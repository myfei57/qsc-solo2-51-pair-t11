"""Durable, append-only journal of state recovery actions.

The journal lives next to the state file (``plant.json.recovery.log``) and is
deliberately independent of the key space: a rollback rewinds the key space and
must never erase evidence that the rollback happened.

Each entry is one JSON line carrying its own sha256 over a canonical encoding,
so a crash while appending can at most tear the final line; that line is
recognisable and is truncated on the next open. ``append_once`` keys entries on
the corrupt bytes' checksum plus the repair kind, which makes recovery
idempotent: triggering it again never records or performs the repair twice.
"""

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

from .codec import canonical

ROLLBACK = "rollback"
RESET = "reset"
LEGACY_IMPORT = "legacy-import"
REPAIR_KINDS = (ROLLBACK, RESET, LEGACY_IMPORT)


def entry_digest(entry: dict[str, Any]) -> str:
    """Per-line integrity digest, excluding the line's own checksum field."""
    payload = {key: value for key, value in entry.items() if key != "record_checksum"}
    return hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RecoveryEvent:
    """One durable record of a recovery action."""

    seq: int
    kind: str
    reason: str
    evidence_checksum: str
    rejected_checksum: str | None
    rejected_version: int | None
    restored_version: int
    restored_checksum: str
    at: str
    recovered_from: str

    def identity(self) -> tuple[str, str, int, str]:
        """Key under which the event is only recorded once."""
        return (self.kind, self.evidence_checksum, self.restored_version, self.reason)

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly projection used by the console."""
        return {
            "seq": self.seq,
            "kind": self.kind,
            "reason": self.reason,
            "evidence_checksum": self.evidence_checksum,
            "rejected_checksum": self.rejected_checksum,
            "rejected_version": self.rejected_version,
            "restored_version": self.restored_version,
            "restored_checksum": self.restored_checksum,
            "at": self.at,
            "recovered_from": self.recovered_from,
        }


class RecoveryJournal:
    """Append-only recovery log with per-line verification."""

    def __init__(self, path: str) -> None:
        self._path = path
        self._events: list[RecoveryEvent] = []
        self._identities: set[tuple[str, str, int]] = set()
        self._load()

    @property
    def path(self) -> str:
        return self._path

    def events(self) -> tuple[RecoveryEvent, ...]:
        """All recovery events, oldest first."""
        return tuple(self._events)

    def rollback_count(self) -> int:
        """How many state versions were rolled back over the file's life."""
        return sum(1 for event in self._events if event.kind == ROLLBACK)

    def has_identity(self, event: RecoveryEvent) -> bool:
        """Whether an equivalent repair was already journaled."""
        return event.identity() in self._identities

    def append_once(self, event: RecoveryEvent) -> bool:
        """Append an event unless one with the same identity exists.

        Returns ``True`` when a new line was durably appended.
        """
        if self.has_identity(event):
            return False
        line = canonical(event.to_dict())
        record = {**json.loads(line), "record_checksum": entry_digest(event.to_dict())}
        data = (canonical(record) + "\n").encode("utf-8")
        directory = os.path.dirname(self._path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(self._path, "ab") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        self._events.append(event)
        self._identities.add(event.identity())
        return True

    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "rb") as handle:
            raw = handle.read()
        good_bytes = b""
        for line in raw.splitlines(keepends=True):
            body = line.strip()
            if not body or self._valid_line(body):
                good_bytes += line
                continue
            break
        if good_bytes != raw:
            self._rewrite(good_bytes)
        for body in good_bytes.splitlines():
            body = body.strip()
            if body:
                self._remember(json.loads(body))

    @staticmethod
    def _valid_line(body: bytes) -> bool:
        try:
            record = json.loads(body)
        except ValueError:
            return False
        if not isinstance(record, dict):
            return False
        claimed = record.get("record_checksum")
        if not isinstance(claimed, str):
            return False
        return claimed == entry_digest(record)

    def _remember(self, record: dict[str, Any]) -> None:
        event = RecoveryEvent(
            seq=int(record["seq"]),
            kind=str(record["kind"]),
            reason=str(record["reason"]),
            evidence_checksum=str(record["evidence_checksum"]),
            rejected_checksum=record.get("rejected_checksum"),
            rejected_version=record.get("rejected_version"),
            restored_version=int(record["restored_version"]),
            restored_checksum=str(record["restored_checksum"]),
            at=str(record["at"]),
            recovered_from=str(record["recovered_from"]),
        )
        self._events.append(event)
        self._identities.add(event.identity())

    def _rewrite(self, good_bytes: bytes) -> None:
        """Atomically drop a torn tail line, keeping the verified prefix."""
        temporary = f"{self._path}.tmp"
        with open(temporary, "wb") as handle:
            if good_bytes:
                handle.write(good_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self._path)
