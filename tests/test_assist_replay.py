from __future__ import annotations

import json
from pathlib import Path
import re

from fixtures.assist_replay.replay import FIXTURE_PATH, run_replay


_MACHINE_OR_SECRET = re.compile(
    r"(?:/home/|/root/|/tmp/|[A-Za-z]:[\\/]|wt/|origin/|bearer\\s+|sk-|gh[pousr]_)",
    re.IGNORECASE,
)
_LIVE_ID = re.compile(r"(?:t_[0-9a-f]{8,}|run_[0-9a-f]{8,}|chat[-_]?[0-9]{5,})", re.IGNORECASE)


def test_replay_twice_has_identical_fingerprint_signature_refs_and_order() -> None:
    first = run_replay()
    second = run_replay()

    assert first["fixture_hash"] == second["fixture_hash"]
    assert first["dedupe"]["replay_signature"] == second["dedupe"]["replay_signature"]
    assert [row["evidence_refs"] for row in first["windows"]] == [
        row["evidence_refs"] for row in second["windows"]
    ]
    assert [row["window_id"] for row in first["windows"]] == ["window-001", "window-002"]


def test_fixture_safety_scan_allows_only_synthetic_symbolic_values() -> None:
    text = FIXTURE_PATH.read_text(encoding="utf-8")

    assert not _MACHINE_OR_SECRET.search(text)
    assert not _LIVE_ID.search(text)
    assert "profile-A" not in text
    assert "board-A" not in text
    assert "task-impl-1" not in text
    assert "run-1" not in text
    assert "synthetic" in FIXTURE_PATH.parent.joinpath("README.md").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    assert all(row["window_id"].startswith("window-") for row in rows)
    assert all(row["evidence_id"].startswith("evidence-") for row in rows)


def test_fixture_and_replay_source_contain_no_machine_or_secret_shaped_values() -> None:
    fixture_dir = FIXTURE_PATH.parent
    committed_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in fixture_dir.iterdir()
        if path.suffix in {".jsonl", ".py"}
    )

    assert not _MACHINE_OR_SECRET.search(committed_text)
    assert not _LIVE_ID.search(committed_text)


def test_recurrence_transition_and_first_seen_are_evidence_based() -> None:
    result = run_replay()

    assert [row["recurrence"] for row in result["windows"]] == [
        "first_seen",
        "recurs_in_2_windows",
    ]
    assert all(row["evidence_preserved"] for row in result["windows"])
    assert result["classifier"]["signature"] == result["windows"][0]["signature"]


def test_malformed_and_unavailable_model_fail_closed_without_losing_evidence() -> None:
    classifier = run_replay()["classifier"]

    for key in ("malformed_model", "unavailable_model"):
        output = classifier[key]
        assert output["ai_status"] == "error"
        assert output["recommendation"] == "not_actionable"
        assert output["evidence_preserved"] is True
        assert output["evidence_refs"]


def test_zero_mutation_proof_and_deferred_pending_controller_state() -> None:
    result = run_replay()
    proof = result["zero_mutation_proof"]
    candidate = result["recommendation"]
    ledger = result["ledger"]

    assert all(value == 0 for key, value in proof.items() if isinstance(value, int))
    assert proof["controller_records"] == ["pending", "not_applied"]
    assert candidate["state"] == "pending"
    assert candidate["action"] == "not_applied"
    assert candidate["executable_action"] is False
    assert ledger["operator_event"] == "operator_deferred"
    assert ledger["controller_action_state"] == "not_applied"
    assert ledger["append_only"] is True
    assert ledger["pending_record_unchanged"] is True
    assert ledger["record_count_before"] == 1
    assert ledger["record_count_after"] == 2
    assert [row["state"] for row in ledger["records"]] == ["pending", "deferred"]
    assert ledger["records"][0]["event"] == "candidate_pending"
    assert ledger["records"][1]["event"] == "operator_deferred"


def test_dedupe_check_ignores_replayed_symbolic_events() -> None:
    dedupe = run_replay()["dedupe"]

    assert dedupe["input_events"] == 8
    assert dedupe["unique_event_ids"] == 6
    assert dedupe["duplicates_ignored"] == 2
    assert dedupe["audit_event_ids"] == [
        "event-001",
        "event-002",
        "event-003",
        "event-004",
        "event-005",
        "event-006",
        "event-001",
        "event-002",
    ]
    assert dedupe["deduped_event_ids"] == [
        "event-001",
        "event-002",
        "event-003",
        "event-004",
        "event-005",
        "event-006",
    ]
    assert dedupe["duplicate_event_ids"] == ["event-001", "event-002"]
    assert dedupe["finding_ids"] == ["finding-window-001", "finding-window-002"]
    assert dedupe["findings"] == [
        {
            "finding_id": "finding-window-001",
            "event_ids": ["event-001", "event-002", "event-003"],
        },
        {
            "finding_id": "finding-window-002",
            "event_ids": ["event-004", "event-005", "event-006"],
        },
    ]
    assert dedupe["duplicate_findings_created"] == 0


def test_html_report_is_self_contained_and_synthetic_labelled() -> None:
    result = run_replay()
    report = result["html_report"]

    assert "SYNTHETIC_OFFLINE_FIXTURE" in report
    assert "before" in report.lower() and "after" in report.lower()
    assert "pending" in report
    assert "not applied" in report
    assert 'class="badge strength-badge"' in report
    assert "strength: candidate" in report
    assert 'id="evidence-evidence-004"' in report
    assert 'href="#evidence-evidence-004"' in report
    assert "before[before:" in report
    assert "after[after:" in report
    assert "<script" not in report.lower()
    assert "https://" not in report.lower()
    assert "/home/" not in report


def test_replay_has_no_mutation_or_cli_import_boundary() -> None:
    source = Path(__file__).parent / "fixtures" / "assist_replay" / "replay.py"
    text = source.read_text(encoding="utf-8")

    assert "subprocess" not in text
    assert "sqlite" not in text.lower()
