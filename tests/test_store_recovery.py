"""Generation versioning, checksums, crash recovery and idempotency."""

import json
import os

import pytest

from desalplant.store import keys
from desalplant.store.envelope import Envelope, build as build_envelope, parse as parse_envelope
from desalplant.store.generations import (
    generation_path,
    head_path,
    list_generations,
    newest_valid,
    read_valid_envelope,
)
from desalplant.store.recovery import RecoveryReport, RollbackRecord
from desalplant.store.store import Store


def _read_head(path: str) -> Envelope:
    with open(path, "r", encoding="utf-8") as handle:
        return parse_envelope(handle.read())


def test_each_flush_bumps_the_generation(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    assert store.generation == 0
    store.put("a", 1)
    assert store.generation == 1
    store.put("b", 2)
    assert store.generation == 2
    store.put("c", 3)
    assert store.generation == 3
    assert list_generations(str(tmp_path)) == [1, 2, 3]


def test_the_head_is_a_copy_of_the_newest_generation(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    head = _read_head(path)
    assert head.generation == 2
    assert head.data == {"a": 1, "b": 2}
    assert head.valid() is True
    retained = read_valid_envelope(generation_path(str(tmp_path), 2))
    assert retained is not None
    assert retained.data == head.data


def test_a_tampered_envelope_fails_validation(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    head = _read_head(path)
    # Tamper with the data without updating the checksum.
    tampered = Envelope(
        generation=head.generation,
        checksum=head.checksum,
        data={"a": 999},
    )
    assert tampered.valid() is False


def test_reopen_resumes_from_the_head(tmp_path):
    path = str(tmp_path / "plant.json")
    first = Store(path)
    first.put("a", 1)
    first.put("b", 2)
    reopened = Store(path)
    assert reopened.generation == 2
    assert reopened.get("a") == 1
    assert reopened.get("b") == 2
    assert reopened.recovered is False


def test_a_corrupt_head_rolls_back_to_the_newest_valid_generation(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    store.put("c", 3)
    # Simulate a crash that left the head with a bad checksum.
    tampered = {
        "format": "desalplant-state",
        "generation": 3,
        "checksum": "deadbeef",
        "data": {"a": 1, "b": 2, "c": 3},
    }
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(tampered))
    reopened = Store(path)
    assert reopened.recovered is True
    assert reopened.get("a") == 1
    assert reopened.get("b") == 2
    assert reopened.get("c") == 3
    report = reopened.recovery_report()
    assert report.rolled_back is True
    assert report.record.from_generation == 3
    assert report.record.to_generation == 3
    assert report.record.reason == "head failed validation"


def test_a_lost_head_rolls_back_to_the_newest_valid_generation(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    # Simulate a crash that lost the head rename.
    os.remove(path)
    reopened = Store(path)
    assert reopened.recovered is True
    assert reopened.get("a") == 1
    assert reopened.get("b") == 2


def test_recovery_is_idempotent(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write('{"format": "desalplant-state", "generation": 2, "checksum": "deadbeef", "data": {')
    first = Store(path)
    assert first.rollback_count == 1
    assert first.recovered is True
    # Repeated recovery must be a no-op: no new rollback, no state change.
    report = first.recover()
    assert report.rolled_back is False
    assert first.rollback_count == 1
    assert first.recovered is False
    assert first.get("a") == 1
    assert first.get("b") == 2
    # And the head is now valid again.
    assert _read_head(path).valid() is True


def test_recovery_after_a_fresh_start_is_a_noop(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    report = store.recover()
    assert report.rolled_back is False
    assert store.generation == 0


def test_verify_consistency_agrees_after_a_write(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    assert store.verify_consistency() is True
    store.put("b", 2)
    assert store.verify_consistency() is True


def test_verify_consistency_detects_in_memory_drift(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    # Mutate the in-memory document without flushing.
    store._data["a"] = 999
    assert store.verify_consistency() is False


def test_verify_consistency_detects_a_tampered_head(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    # Tamper with the head file.
    head = _read_head(path)
    tampered = Envelope(
        generation=head.generation,
        checksum=head.checksum,
        data={"a": 999},
    )
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(tampered.text())
    assert store.verify_consistency() is False


def test_a_rollback_is_recorded_in_the_audit_log(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    original_checksum = store.checksum()
    tampered = {
        "format": "desalplant-state",
        "generation": 2,
        "checksum": "deadbeef",
        "data": {"a": 1, "b": 2},
    }
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(tampered))
    reopened = Store(path)
    assert reopened.rollback_count == 1
    record = reopened.rollbacks()[0]
    assert record.from_generation == 2
    assert record.to_generation == 2
    assert record.from_checksum == "deadbeef"
    assert record.to_checksum == original_checksum
    # The audit event is in the key space.
    event = reopened.get(keys.audit_event_key(1))
    assert event["kind"] == "rollback"
    assert event["sequence"] == 1
    assert reopened.get(keys.AUDIT_SEQUENCE) == 1
    assert reopened.get(keys.audit_last_key("rollback")) == 1


def test_a_rollback_preserves_the_audit_sequence(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    # Append an audit event through the recorder.
    from desalplant.audit.recorder import Recorder
    from desalplant.clock import LogicalClock

    recorder = Recorder(store, LogicalClock())
    recorder.log("start", "line started")
    assert store.get(keys.AUDIT_SEQUENCE) == 1
    # Corrupt the head and recover.
    with open(path, "w", encoding="utf-8") as handle:
        handle.write('{"format": "desalplant-state", "generation": 2, "checksum": "deadbeef", "data": {')
    reopened = Store(path)
    # The original audit event is preserved, and the rollback is appended.
    assert reopened.get(keys.AUDIT_SEQUENCE) == 2
    assert reopened.get(keys.audit_event_key(1))["kind"] == "start"
    assert reopened.get(keys.audit_event_key(2))["kind"] == "rollback"


def test_newest_valid_skips_corrupt_generations(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    store.put("c", 3)
    # Corrupt the newest retained generation.
    newest = generation_path(str(tmp_path), 3)
    with open(newest, "w", encoding="utf-8") as handle:
        handle.write("not json")
    target = newest_valid(str(tmp_path))
    assert target is not None
    assert target.generation == 2
    assert target.data == {"a": 1, "b": 2}


def test_an_envelope_round_trips():
    envelope = build_envelope(3, {"a": 1, "b": [1, 2]})
    text = envelope.text()
    parsed = parse_envelope(text)
    assert parsed.generation == 3
    assert parsed.data == {"a": 1, "b": [1, 2]}
    assert parsed.valid() is True


def test_a_malformed_envelope_is_rejected():
    with pytest.raises(ValueError):
        parse_envelope("[1, 2]")
    with pytest.raises(ValueError):
        parse_envelope('{"format": "wrong", "generation": 1, "checksum": "x", "data": {}}')
    with pytest.raises(ValueError):
        parse_envelope('{"format": "desalplant-state", "generation": 0, "checksum": "x", "data": {}}')
    with pytest.raises(ValueError):
        parse_envelope('{"format": "desalplant-state", "generation": 1, "checksum": "", "data": {}}')
    with pytest.raises(ValueError):
        parse_envelope('{"format": "desalplant-state", "generation": 1, "checksum": "x", "data": []}')


def test_a_legacy_raw_document_is_migrated(tmp_path):
    path = tmp_path / "plant.json"
    # Write a pre-versioning raw key space.
    path.write_text(json.dumps({"plant/state": "running"}), encoding="utf-8")
    store = Store(str(path))
    assert store.generation == 1
    assert store.get("plant/state") == "running"
    assert store.recovered is False
    # The next flush persists it as a proper envelope.
    store.put("plant/started_at", 1)
    assert store.generation == 2
    assert _read_head(str(path)).valid() is True


def test_a_crash_mid_write_leaves_a_valid_head(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("b", 2)
    assert store.generation == 2
    # Simulate a crash during the next write: the new generation lands in
    # versions/, but the head rename never happens (a partial .tmp is left).
    store.put("c", 3)
    # Now crash again before the head update: leave a partial .tmp.
    with open(f"{path}.tmp", "w", encoding="utf-8") as handle:
        handle.write('{"format": "desalplant-state", "generation": 3, "checksum": "x", "data": {')
    # The head is still the last committed generation, intact.
    head = _read_head(path)
    assert head.valid() is True
    assert head.generation == 3
    # Reopening needs no rollback: the head is complete.
    reopened = Store(path)
    assert reopened.recovered is False
    assert reopened.get("a") == 1
    assert reopened.get("b") == 2
    assert reopened.get("c") == 3


def test_a_partial_tmp_file_is_ignored(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    # A leftover .tmp from a crashed write must not affect the head.
    with open(f"{path}.tmp", "w", encoding="utf-8") as handle:
        handle.write("partial")
    reopened = Store(path)
    assert reopened.recovered is False
    assert reopened.get("a") == 1
    assert reopened.verify_consistency() is True
