"""Ledger retention + report counts basis (t_5f25b8a9 AC2-AC4)."""

from __future__ import annotations

import json
from pathlib import Path

from hkrc.harness_loop import load_state, prune_stale_entries, save_state

from test_harness_loop import queue_entry

NOW = 1_788_000_000
DAY = 86_400


def _entry(fp: str, *, fix_status: str, last_seen: int) -> dict:
    return queue_entry(
        fp,
        pattern="decision-latency",
        key=fp,
        severity="medium",
        fix_status=fix_status,
        last_seen=last_seen,
        first_seen=last_seen - 5 * DAY,
    )


def _state_file(tmp_path: Path, entries: list[dict]) -> Path:
    state_file = tmp_path / "state" / "hkrc" / "harness-loop-state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": entries,
            }
        ),
        encoding="utf-8",
    )
    return state_file


def test_prune_removes_old_stale_keeps_everything_else(tmp_path: Path) -> None:
    state_file = _state_file(
        tmp_path,
        [
            _entry("fp_old_stale", fix_status="stale", last_seen=NOW - 31 * DAY),
            _entry("fp_edge_stale", fix_status="stale", last_seen=NOW - 30 * DAY),
            _entry("fp_fresh_stale", fix_status="stale", last_seen=NOW - 3 * DAY),
            _entry("fp_old_resolved", fix_status="resolved", last_seen=NOW - 400 * DAY),
            _entry("fp_open", fix_status="open", last_seen=NOW - 400 * DAY),
            _entry("fp_deferred", fix_status="deferred", last_seen=NOW - 400 * DAY),
        ],
    )
    state = load_state(state_file)
    pruned, backup = prune_stale_entries(
        state, state_file, retention_days=30, now=NOW
    )
    assert pruned == 1
    by_fp = {entry["fingerprint"]: entry for entry in state["open_findings"]}
    assert set(by_fp) == {
        "fp_edge_stale",
        "fp_fresh_stale",
        "fp_old_resolved",
        "fp_open",
        "fp_deferred",
    }
    assert backup is not None and backup.is_file()
    backup_state = json.loads(backup.read_text(encoding="utf-8"))
    assert len(backup_state["open_findings"]) == 6
    assert "harness-loop-state" in backup.name and backup.name.endswith(".json")


def test_no_prune_no_backup(tmp_path: Path) -> None:
    state_file = _state_file(
        tmp_path,
        [_entry("fp_open", fix_status="open", last_seen=NOW - 400 * DAY)],
    )
    state = load_state(state_file)
    pruned, backup = prune_stale_entries(state, state_file, retention_days=30, now=NOW)
    assert pruned == 0 and backup is None
    assert list(tmp_path.joinpath("state", "hkrc").glob("*.json")) == [state_file]


def test_second_prune_makes_no_backup(tmp_path: Path) -> None:
    state_file = _state_file(
        tmp_path,
        [_entry("fp_stale", fix_status="stale", last_seen=NOW - 31 * DAY)],
    )
    state = load_state(state_file)
    pruned, backup = prune_stale_entries(state, state_file, retention_days=30, now=NOW)
    save_state(state_file, state)
    state = load_state(state_file)
    pruned, backup = prune_stale_entries(state, state_file, retention_days=30, now=NOW)
    assert pruned == 0 and backup is None


def test_custom_retention_days(tmp_path: Path) -> None:
    state_file = _state_file(
        tmp_path,
        [
            _entry("fp_stale_10d", fix_status="stale", last_seen=NOW - 11 * DAY),
            _entry("fp_stale_5d", fix_status="stale", last_seen=NOW - 5 * DAY),
        ],
    )
    state = load_state(state_file)
    pruned, _ = prune_stale_entries(state, state_file, retention_days=10, now=NOW)
    assert pruned == 1
    assert len(state["open_findings"]) == 1
    assert state["open_findings"][0]["fingerprint"] == "fp_stale_5d"
