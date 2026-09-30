"""Versioned, checksummed, crash-safe state persistence and recovery."""

import json
import os

import pytest

from desalplant.store.checksum import digest
from desalplant.store.persistence import (
    ENVELOPE_FORMAT,
    inspect_bytes,
    pack,
    raw_digest,
)
from desalplant.store.recovery import (
    LEGACY_IMPORT,
    RESET,
    ROLLBACK,
    RecoveryJournal,
    entry_digest,
)
from desalplant.store.store import Store


def _corrupt_main(path, payload=b"{not json"):
    with open(path, "wb") as handle:
        handle.write(payload)


def _tamper_version(path):
    with open(path, encoding="utf-8") as handle:
        envelope = json.load(handle)
    envelope["rows"]["new/key"] = "unchecksummed"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(envelope, handle)


# ----------------------------------------------------------- envelope on disk


def test_committed_document_is_a_versioned_checksummed_envelope(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path, now=lambda: "2026-09-29T00:00:00+00:00")
    store.put("a", 1)
    on_disk = json.loads((tmp_path / "plant.json").read_text(encoding="utf-8"))
    assert on_disk["format"] == ENVELOPE_FORMAT
    assert on_disk["version"] == 1
    assert on_disk["created_at"] == "2026-09-29T00:00:00+00:00"
    assert on_disk["checksum"] == digest({"a": 1})
    assert on_disk["rows"] == {"a": 1}


def test_versions_increase_monotonically_and_are_never_reused(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    assert store.version == 2
    assert store.next_version == 3
    _corrupt_main(path)
    rolled = Store(path)
    assert rolled.boot_report.action == ROLLBACK
    assert rolled.version == 1  # restored previous
    assert rolled.get("a") == 1
    rolled.put("a", 3)
    assert rolled.version == 3  # version 2 never reused
    reopened = Store(path)
    assert reopened.version == 3
    assert reopened.get("a") == 3

def test_inspect_flags_every_form_of_bad_document():
    good = pack({"a": 1}, 1, "t")
    parsed = inspect_bytes(json.dumps(good).encode())
    assert parsed.version == 1 and parsed.checksum == good["checksum"]
    assert inspect_bytes(b"{nope").reason == "malformed-json"
    assert inspect_bytes(b"[1,2]").reason == "not-an-object"
    bad_format = {**good, "format": "something-else"}
    assert inspect_bytes(json.dumps(bad_format).encode()).reason == "unsupported-format"
    bad_version = {**good, "version": 0}
    assert inspect_bytes(json.dumps(bad_version).encode()).reason == "bad-version"
    tampered = {**good, "rows": {"a": 2}}
    result = inspect_bytes(json.dumps(tampered).encode())
    assert result.reason == "checksum-mismatch"
    assert result.rejected_version == 1
    assert result.rejected_checksum == good["checksum"]
    legacy = inspect_bytes(json.dumps({"a": 1}).encode())
    assert legacy.legacy is True and legacy.version == 0


# ------------------------------------------------------------- rollback paths


def test_truncated_write_rolls_back_to_the_previous_version(tmp_path):
    path = str(tmp_path / "plant.json")
    first = Store(path)
    first.put("plant/state", "running")
    first.put("plant/state", "stopping")
    assert first.version == 2
    # a torn write of what would have been version 3 lands in the main slot
    _corrupt_main(path, b'{"format": "' + ENVELOPE_FORMAT.encode() + b'", "version": 3, "rows": {')
    reopened = Store(path)
    assert reopened.boot_report.action == ROLLBACK
    assert reopened.version == 1  # previous good slot
    assert reopened.get("plant/state") == "running"
    event = reopened.recovery_events()[0]
    assert event.kind == ROLLBACK
    assert event.reason == "malformed-json"
    assert event.restored_version == 1
    assert event.restored_checksum == reopened.committed_checksum()
    reopened.put("plant/state", "stopping-again")
    assert reopened.version == 3  # torn v3's number is not reused


def test_checksum_mismatch_rolls_back(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    expected_checksum = store.committed_checksum()
    _tamper_version(path)
    reopened = Store(path)
    assert reopened.boot_report.action == ROLLBACK
    assert reopened.version == 1
    assert reopened.committed_checksum() == digest({"a": 1})
    assert reopened.get("a") == 1


def test_missing_main_file_restores_from_previous(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    os.remove(path)
    reopened = Store(path)
    assert reopened.boot_report.action == ROLLBACK
    assert reopened.version == 1
    assert reopened.get("a") == 1
    assert reopened.recovery_events()[0].reason == "missing-document"


def test_rollback_with_no_previous_resets_and_quarantines(tmp_path):
    path = str(tmp_path / "plant.json")
    _corrupt_main(path, b"garbage")
    store = Store(path)
    assert store.boot_report.action == RESET
    assert store.count() == 0
    events = store.recovery_events()
    assert events[0].kind == RESET
    assert events[0].evidence_checksum == raw_digest(b"garbage")
    assert (tmp_path / "plant.json").exists() is False
    assert len(list((tmp_path / "plant.json.corrupt").iterdir())) == 1


def test_reset_when_both_slots_are_corrupt_quarantines_each(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    _corrupt_main(path, b"broken-main")
    _corrupt_main(f"{path}.prev", b"broken-prev")
    recovered = Store(path)
    assert recovered.boot_report.action == RESET
    assert recovered.count() == 0
    quarantined = sorted(p.name for p in (tmp_path / "plant.json.corrupt").iterdir())
    assert len(quarantined) == 2
    assert any(".main." in name for name in quarantined)
    assert any(".prev." in name for name in quarantined)
    assert not (tmp_path / "plant.json.prev").exists()
    # and recovery is repeatable without raising or duplicating
    recovered.recover()
    assert len(recovered.recovery_events()) == 1


# --------------------------------------------------------------- idempotency


def test_recovery_is_idempotent_across_repeated_starts(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    _corrupt_main(path)
    first = Store(path)
    assert first.boot_report.action == ROLLBACK
    assert first.version == 1
    second = Store(path)
    assert second.boot_report.action == "loaded"
    third = Store(path)
    assert third.version == 1 and third.get("a") == 1
    events = first.recovery_events()
    assert len(events) == 1
    assert events[0].evidence_checksum


def test_explicit_recover_is_a_noop_when_healthy(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    report = store.recover()
    assert report.action == "loaded"
    assert report.event is None
    assert store.version == 1


def test_repeated_recover_does_not_move_the_rolled_back_state(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    _corrupt_main(path)
    rolled = Store(path)
    rolled.recover()
    rolled.recover()
    assert rolled.version == 1
    assert rolled.get("a") == 1
    assert len(rolled.recovery_events()) == 1
    assert rolled.consistency()["matches"] is True


def test_crash_between_journal_and_restore_converges_on_restart(tmp_path):
    """Journal line durable, but the file swap never happened."""
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    evidence = b"half-written-bytes"
    _corrupt_main(path, evidence)
    # pre-create the journal line exactly as a crashed rollback would leave it
    from desalplant.store.persistence import inspect_bytes

    prev = inspect_bytes(open(f"{path}.prev", "rb").read())
    journal = RecoveryJournal(f"{path}.recovery.log")
    from desalplant.store.recovery import RecoveryEvent

    journal.append_once(
        RecoveryEvent(
            seq=1,
            kind=ROLLBACK,
            reason="malformed-json",
            evidence_checksum=raw_digest(evidence),
            rejected_checksum=None,
            rejected_version=None,
            restored_version=prev.version,
            restored_checksum=prev.checksum,
            at="t",
            recovered_from=f"{path}.prev",
        )
    )
    second = Store(path)  # completes the rollback, records nothing new
    assert second.boot_report.action == ROLLBACK
    assert len(second.recovery_events()) == 1
    assert second.version == 1
    # a third start finds a healthy disk and still records nothing new
    third = Store(path)
    assert third.boot_report.action == "loaded"
    assert len(third.recovery_events()) == 1


# ------------------------------------------------------------- journal audit


def test_journal_detects_and_truncates_a_torn_tail(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    store.put("a", 2)
    _corrupt_main(path)
    Store(path)
    journal_path = f"{path}.recovery.log"
    with open(journal_path, "ab") as handle:
        handle.write(b'{"seq": 9, "kind": "rollback", "broken\n')
    reopened = Store(path)
    events = reopened.recovery_events()
    assert len(events) == 1  # torn line dropped, earlier evidence intact
    with open(journal_path, "rb") as handle:
        lines = [line for line in handle.read().splitlines() if line.strip()]
    assert len(lines) == 1


def test_tampered_journal_line_is_rejected(tmp_path):
    path = str(tmp_path / "plant.json")
    journal = RecoveryJournal(f"{path}.recovery.log")
    # hand-craft one valid line then corrupt its tail successor
    from desalplant.store.recovery import RecoveryEvent

    good = RecoveryEvent(1, ROLLBACK, "x", "ev", None, None, 1, "sum", "t", "p")
    journal.append_once(good)
    with open(journal.path, "ab") as handle:
        handle.write(b'{"seq": 2, "record_checksum": "wrong"}\n')
    reopened = RecoveryJournal(journal.path)
    assert [event.seq for event in reopened.events()] == [1]


def test_entry_digest_guards_the_record_payload():
    record = {"seq": 1, "kind": ROLLBACK, "evidence_checksum": "e"}
    record["record_checksum"] = entry_digest(record)
    assert entry_digest(record) == record["record_checksum"]
    tampered = {**record, "reason": "changed"}
    assert entry_digest(tampered) != record["record_checksum"]


def test_legacy_bare_object_document_is_imported_once(tmp_path):
    path = str(tmp_path / "plant.json")
    (tmp_path / "plant.json").write_text(json.dumps({"a": 1}), encoding="utf-8")
    first = Store(path)
    assert first.boot_report.action == LEGACY_IMPORT
    assert first.get("a") == 1
    first.put("b", 2)  # repackaged as an envelope
    second = Store(path)
    assert second.boot_report.action == "loaded"
    assert second.get("a") == 1 and second.get("b") == 2
    kinds = [event.kind for event in second.recovery_events()]
    assert kinds == [LEGACY_IMPORT]


# ---------------------------------------------------- memory/disk consistency


def test_consistency_matches_a_healthy_store_and_reports_drift(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    view = store.consistency()
    assert view["matches"] is True
    assert view["disk_version"] == 1
    assert view["disk_checksum"] == view["memory_checksum"]
    store._data["a"] = 2
    assert store.consistency()["matches"] is False


def test_failed_flush_marks_memory_dirty(tmp_path):
    path = tmp_path / "plant.json"
    store = Store(str(path))
    store.put("a", 1)
    (tmp_path / "plant.json.tmp").mkdir()  # makes the temp open fail
    with pytest.raises(Exception):
        store.put("b", 2)
    assert store.dirty is True
    assert store.consistency()["matches"] is False
    os.rmdir(tmp_path / "plant.json.tmp")
    store.flush()
    assert store.dirty is False
    assert store.consistency()["matches"] is True


def test_persistence_view_summarises_version_and_recovery(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    view = store.persistence_view()
    assert view["version"] == 1
    assert view["next_version"] == 2
    assert view["consistent"] is True
    assert view["recovery"]["rollback_count"] == 0
    assert view["boot_action"] in ("fresh", "loaded")


# -------------------------------------------------------------- console/API


def test_state_endpoint_reports_current_version(services, console):
    from support import ok

    services.plant.start()
    body = ok(console, "GET", "/api/state")
    assert body["version"] >= 1
    assert body["format"] == ENVELOPE_FORMAT
    assert body["consistent"] is True
    assert "checksum" in body and body["checksum"]
    assert body["recovery"]["events"] == []


def test_state_endpoint_includes_rollback_history(tmp_path):
    from desalplant.config import Config

    from support import build, ok

    config = Config(data_dir=str(tmp_path / "data"))
    services = build(config)
    services.plant.start()
    services.plant.shutdown()
    path = config.store_path()
    _corrupt_main(path)
    rebuilt = build(config)
    body = ok(rebuilt.console, "GET", "/api/state")
    assert body["version"] >= 2
    assert body["boot_action"] == ROLLBACK
    assert body["recovery"]["rollback_count"] == 1
    event = body["recovery"]["events"][0]
    assert event["kind"] == ROLLBACK
    assert event["restored_version"] >= 1
    # the operational event record also carries the startup rollback
    kinds = [event.kind for event in rebuilt.recorder.events()]
    assert "state-recovery-rollback" in kinds


# --------------------------------------------------------- power-loss sims


def test_simulated_power_loss_during_writes_never_loses_more_than_one(tmp_path):
    """Half of the committed versions survive, and every reopen is valid."""
    path = str(tmp_path / "plant.json")
    store = Store(path)
    versions = []
    for tick in range(1, 9):
        store.put("tick", tick)
        versions.append((store.version, store.committed_checksum()))
    # crash on every possible "newest" version in turn
    for victim in range(1, 9):
        # rebuild the sequence of commits then tear the victim
        rebuild_path = tmp_path / f"sim-{victim}" / "plant.json"
        sim = Store(str(rebuild_path))
        for tick in range(1, victim + 1):
            sim.put("tick", tick)
        # corrupt main; previous must be readable
        _corrupt_main(str(rebuild_path), b"{torn")
        recovered = Store(str(rebuild_path))
        if victim == 1:
            assert recovered.boot_report.action == RESET
            assert recovered.count() == 0
        else:
            assert recovered.boot_report.action == ROLLBACK
            assert recovered.version == victim - 1
            assert recovered.get("tick") == victim - 1
        # recovered store keeps serving commits
        recovered.put("tick", 99)
        again = Store(str(rebuild_path))
        assert again.get("tick") == 99
        assert again.consistency()["matches"] is True


def test_temp_file_left_by_crash_is_cleaned_and_ignored(tmp_path):
    path = str(tmp_path / "plant.json")
    store = Store(path)
    store.put("a", 1)
    with open(f"{path}.tmp", "w", encoding="utf-8") as handle:
        handle.write('{"garbage": true}')
    reopened = Store(path)
    assert reopened.version == 1
    assert reopened.get("a") == 1
    assert not os.path.exists(f"{path}.tmp")
