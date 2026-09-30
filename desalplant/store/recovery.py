"""Recovery types and rollback bookkeeping.

A rollback record captures which generation the store fell back from and to, the
checksum each side claimed, and why recovery ran. Records are appended to the
audit log as ``rollback`` events so the console can report how many rollbacks
happened and which checksum each one used.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RollbackRecord:
    """One rollback: which generation we fell back from and to."""

    sequence: int
    from_generation: int
    to_generation: int
    from_checksum: str
    to_checksum: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "from_generation": self.from_generation,
            "to_generation": self.to_generation,
            "from_checksum": self.from_checksum,
            "to_checksum": self.to_checksum,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class RecoveryReport:
    """Outcome of one recovery pass."""

    rolled_back: bool
    generation: int
    checksum: str
    rollback_count: int
    record: RollbackRecord | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rolled_back": self.rolled_back,
            "generation": self.generation,
            "checksum": self.checksum,
            "rollback_count": self.rollback_count,
            "record": None if self.record is None else self.record.to_dict(),
        }
