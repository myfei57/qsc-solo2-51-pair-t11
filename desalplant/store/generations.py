"""On-disk generation layout: head file plus retained generations.

The head (``plant.json``) is a copy of the newest valid generation. Retained
generations live under ``versions/plant.<N>.json``. Every write is crash-safe:
the document is written to a temporary file, fsynced, renamed into place and the
directory is fsynced, so a crash mid-write leaves either the old or the new
generation intact, never a half-written one.
"""

import os
import re

from ..errors import StoreError
from .codec import decode
from .envelope import Envelope, build as build_envelope, parse as parse_envelope

HEAD_NAME = "plant.json"
VERSIONS_DIR = "versions"
GENERATION_RE = re.compile(r"^plant\.(\d+)\.json$")


def head_path(directory: str) -> str:
    """Location of the head file."""
    return os.path.join(directory, HEAD_NAME)


def versions_path(directory: str) -> str:
    """Location of the retained generations directory."""
    return os.path.join(directory, VERSIONS_DIR)


def generation_path(directory: str, generation: int) -> str:
    """Location of one retained generation."""
    return os.path.join(versions_path(directory), f"plant.{generation}.json")


def list_generations(directory: str) -> list[int]:
    """Retained generation numbers, sorted ascending."""
    root = versions_path(directory)
    try:
        names = os.listdir(root)
    except FileNotFoundError:
        return []
    generations = []
    for name in names:
        match = GENERATION_RE.match(name)
        if match is not None:
            generations.append(int(match.group(1)))
    return sorted(generations)


def read_envelope(path: str) -> Envelope | None:
    """Read an envelope, or ``None`` when missing or unparseable."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return None
    if not text.strip():
        return None
    try:
        return parse_envelope(text)
    except (ValueError, TypeError):
        return None


def read_legacy(path: str) -> Envelope | None:
    """Read a pre-versioning raw key space and wrap it as generation 1."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return None
    if not text.strip():
        return None
    try:
        document = decode(text)
    except (ValueError, TypeError):
        return None
    if not isinstance(document, dict):
        return None
    return build_envelope(1, document)


def read_valid_envelope(path: str) -> Envelope | None:
    """Read an envelope only when it parses and its checksum matches."""
    envelope = read_envelope(path)
    if envelope is None or not envelope.valid():
        return None
    return envelope


def newest_valid(directory: str) -> Envelope | None:
    """Newest retained generation that still validates, or ``None``."""
    for generation in reversed(list_generations(directory)):
        envelope = read_valid_envelope(generation_path(directory, generation))
        if envelope is not None:
            return envelope
    return None


def fsync_directory(directory: str) -> None:
    """Best-effort fsync of a directory to persist renames."""
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def write_atomic(path: str, envelope: Envelope) -> None:
    """Write an envelope atomically: temp file, fsync, rename, fsync dir."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    try:
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(envelope.text())
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(directory or ".")
    except OSError as error:
        raise StoreError(f"could not persist {path}: {error}") from error
