"""On-disk envelope for versioned, checksummed state documents.

Every committed state file is a JSON object of the shape::

    {
      "format": "desalplant-state/v1",
      "version": 7,
      "created_at": "2026-09-29T10:00:00+00:00",
      "checksum": "<sha256 hexdigest of rows>",
      "rows": { ...the key space... }
    }

The checksum is recomputed from ``rows`` on every read, so a torn or altered
document is always detected as a whole: a reader can never mistake part of a
document for a committed one. Documents written before envelope versioning
exist as a bare JSON object; those are imported once and reported as a legacy
document.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .checksum import digest

ENVELOPE_FORMAT = "desalplant-state/v1"

REASON_MALFORMED = "malformed-json"
REASON_NOT_OBJECT = "not-an-object"
REASON_BAD_FORMAT = "unsupported-format"
REASON_BAD_VERSION = "bad-version"
REASON_BAD_CHECKSUM_FIELD = "bad-checksum-field"
REASON_ROWS_NOT_OBJECT = "rows-not-an-object"
REASON_CHECKSUM_MISMATCH = "checksum-mismatch"
LEGACY_REASON = "legacy-document"


@dataclass(frozen=True)
class ParsedDocument:
    """A document that proved to be a complete, verified state version."""

    rows: dict[str, Any]
    version: int
    checksum: str
    legacy: bool = False


@dataclass(frozen=True)
class CorruptDocument:
    """A document that failed verification and must not be used as state."""

    reason: str
    evidence_checksum: str
    rejected_checksum: str | None
    rejected_version: int | None


def raw_digest(raw: bytes) -> str:
    """Sha256 of the raw rejected bytes, used as audit evidence."""
    return hashlib.sha256(raw).hexdigest()


def inspect_bytes(raw: bytes) -> ParsedDocument | CorruptDocument:
    """Verify raw file bytes without ever raising on a bad document."""
    evidence = raw_digest(raw)
    try:
        text = raw.decode("utf-8")
        document = json.loads(text)
    except (UnicodeDecodeError, ValueError):
        return CorruptDocument(REASON_MALFORMED, evidence, None, None)
    return inspect_document(document, evidence)


def inspect_document(
    document: Any, evidence_checksum: str
) -> ParsedDocument | CorruptDocument:
    """Verify a decoded JSON value."""
    if not isinstance(document, dict):
        return CorruptDocument(REASON_NOT_OBJECT, evidence_checksum, None, None)
    if "format" not in document:
        parsed = ParsedDocument(
            rows=dict(document),
            version=0,
            checksum=digest(document),
            legacy=True,
        )
        return parsed
    if document.get("format") != ENVELOPE_FORMAT:
        return CorruptDocument(REASON_BAD_FORMAT, evidence_checksum, None, None)
    rows = document.get("rows")
    if not isinstance(rows, dict):
        return CorruptDocument(REASON_ROWS_NOT_OBJECT, evidence_checksum, None, None)
    version = document.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        return CorruptDocument(REASON_BAD_VERSION, evidence_checksum, None, None)
    claimed = document.get("checksum")
    if not isinstance(claimed, str) or not claimed:
        return CorruptDocument(REASON_BAD_CHECKSUM_FIELD, evidence_checksum, None, version)
    actual = digest(rows)
    if actual != claimed:
        return CorruptDocument(
            REASON_CHECKSUM_MISMATCH, evidence_checksum, claimed, version
        )
    return ParsedDocument(rows=dict(rows), version=version, checksum=claimed)


def pack(rows: dict[str, Any], version: int, created_at: str) -> dict[str, Any]:
    """Build the envelope for a new committed version."""
    return {
        "format": ENVELOPE_FORMAT,
        "version": version,
        "created_at": created_at,
        "checksum": digest(rows),
        "rows": rows,
    }
