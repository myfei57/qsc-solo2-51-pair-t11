"""Self-describing, checksummed generation envelope persisted to disk.

Every persisted document is wrapped in an envelope that carries its format, a
monotonic generation number and a content checksum, so a reader can tell a
complete, valid document from a partial or corrupt one without trusting the
filename alone.
"""

from dataclasses import dataclass
from typing import Any, Mapping

from .checksum import digest
from .codec import decode, encode

FORMAT = "desalplant-state"


@dataclass(frozen=True)
class Envelope:
    """A versioned document generation with its own content checksum."""

    generation: int
    checksum: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": FORMAT,
            "generation": self.generation,
            "checksum": self.checksum,
            "data": self.data,
        }

    def text(self) -> str:
        """Pretty textual form persisted to disk."""
        return encode(self.to_dict())

    def valid(self) -> bool:
        """Whether the embedded checksum matches the data."""
        return digest(self.data) == self.checksum


def build(generation: int, data: Mapping[str, Any]) -> Envelope:
    """Wrap a key space in a new envelope with a fresh checksum."""
    return Envelope(
        generation=generation,
        checksum=digest(data),
        data=dict(data),
    )


def parse(text: str) -> Envelope:
    """Parse an envelope from its textual form.

    Raises ``ValueError`` when the text is not a well-formed envelope.
    """
    document = decode(text)
    if not isinstance(document, dict):
        raise ValueError("envelope is not an object")
    if document.get("format") != FORMAT:
        raise ValueError("envelope format mismatch")
    generation = document.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise ValueError("envelope generation must be a positive integer")
    checksum = document.get("checksum")
    if not isinstance(checksum, str) or not checksum:
        raise ValueError("envelope checksum must be a non-empty string")
    data = document.get("data")
    if not isinstance(data, dict):
        raise ValueError("envelope data must be an object")
    return Envelope(
        generation=generation,
        checksum=checksum,
        data=dict(data),
    )
