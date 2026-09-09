# Controls for the daemon-intervention regression detector (t_129a1a08).
#
# The five controls the card mandates (detector-verification.md):
#   1. live count vs independent audit      -> run manually against the real DB
#   2. empty/no-interventions fixture       -> test_empty_interventions_table_*
#   3. nonexistent DB path                  -> test_missing_db_*
#   4. status-blind negative control        -> test_done_card_with_ok_intervention_*
#   5. positive control                     -> test_ok_handoff_then_reblock_*

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.config import ControllerConfig
from hkrc.harness_loop import (
    BoardEvidence,
    InterventionRow,
    collect_interventions,
    detect_daemon_regression,
    fingerprint,
)

T0 = 1_788_300_000  # fixed epoch anchor (2026-09-01-ish); nothing reads wall clock
HOUR = 3600
DAY = 86_400


def iso(epoch: int) -> str:
    """UTC ISO stamp exactly as hkrc.state._utc_now writes them."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def make_intervention_db(path: Path, rows: list[dict]) -> Path:
    """Fixture DB with the real interventions schema (state.py lines 134-149)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE interventions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            board_slug TEXT NOT NULL,
            task_id TEXT NOT NULL,
            phase TEXT NOT NULL,
            outcome TEXT,
            error TEXT,
            started_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (board_slug, task_id)
        );
        """
    )
    for row in rows:
        connection.execute(
            "INSERT INTO interventions(board_slug, task_id, phase, outcome, "
            "error, started_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row.get("board_slug", "fixture"),
                row["task_id"],
                row.get("phase", "complete"),
                row.get("outcome"),
                row.get("error"),
                row.get("started_at", iso(T0 - HOUR)),
                row["updated_at"],
            ),
        )
    connection.commit()
    connection.close()
    return path


def board_evidence(
    *,
    blocked_rows: tuple = (),
    failure_events: tuple = (),
    slug: str = "fixture",
) -> BoardEvidence:
    """Hand-built board snapshot; only the re-block evidence slots are filled."""
    return BoardEvidence(
        slug=slug,
        status_counts=(),
        tasks_in_window=(),
        runs_in_window=(),
        failure_events=failure_events,
        children={},
        blocked_rows=blocked_rows,
        open_task_rows=(),
    )


def ok_intervention(task_id: str = "t_reg1", updated: int = T0) -> InterventionRow:
    return InterventionRow(
        board_slug="fixture",
        task_id=task_id,
        phase="complete",
        outcome="ok",
        error=None,
        started_at=iso(updated - 30),
        updated_at=iso(updated),
    )


# --- control 5: positive -----------------------------------------------------


def test_ok_handoff_then_reblock_is_one_high_finding() -> None:
    interventions = (ok_intervention(),)
    boards = (
        board_evidence(blocked_rows=((("t_reg1", "impl: x", T0 + HOUR, "need input", None),)),),
    )
    findings = detect_daemon_regression(interventions, boards, now=T0 + 2 * DAY)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "daemon-regression"
    assert finding.key == "t_reg1"
    assert finding.severity == "high"
    assert finding.apply_kind == "none"
    assert fingerprint(finding) == "daemon-regression:t_reg1"
    evidence = "\n".join(finding.evidence)
    assert "t_reg1" in evidence and "1.0h" in evidence


def test_ok_handoff_then_failure_event_reblock_fires() -> None:
    interventions = (ok_intervention("t_reg2"),)
    boards = (
        board_evidence(
            failure_events=(type("F", (), {"task_id": "t_reg2", "kind": "gave_up",
                                           "created_at": T0 + 2 * HOUR, "payload": None})(),),
        ),
    )
    findings = detect_daemon_regression(interventions, boards, now=T0 + 2 * DAY)
    assert len(findings) == 1
    assert findings[0].key == "t_reg2"
    assert "gave_up" in "\n".join(findings[0].evidence)


# --- control 4: status-blind negative ----------------------------------------


def test_done_card_with_ok_intervention_emits_zero_findings() -> None:
    # The card recovered: currently done/archived, no blocked row, no failure
    # event.  The historical ok handoff alone must NEVER fire.
    interventions = (ok_intervention(),)
    boards = (board_evidence(),)
    assert detect_daemon_regression(interventions, boards, now=T0 + 2 * DAY) == ()


def test_reblock_before_intervention_is_not_a_regression() -> None:
    # The block PRECEDED the handoff — that is the problem the daemon fixed,
    # not a regression.  Only events AFTER updated_at count.
    interventions = (ok_intervention(),)
    boards = (
        board_evidence(blocked_rows=((("t_reg1", "impl: x", T0 - HOUR, "need input", None),)),),
    )
    assert detect_daemon_regression(interventions, boards, now=T0 + 2 * DAY) == ()


def test_reblock_outside_24h_window_is_silent_at_boundary_edge() -> None:
    interventions = (ok_intervention(),)
    just_outside = board_evidence(
        blocked_rows=((("t_reg1", "impl: x", T0 + DAY + HOUR, "need input", None),)),
    )
    assert detect_daemon_regression(interventions, (just_outside,), now=T0 + 3 * DAY) == ()
    at_boundary = board_evidence(
        blocked_rows=((("t_reg1", "impl: x", T0 + DAY, "need input", None),)),
    )
    findings = detect_daemon_regression(interventions, (at_boundary,), now=T0 + 3 * DAY)
    assert len(findings) == 1  # exactly 24h after: inside the inclusive window


def test_error_outcome_intervention_never_fires() -> None:
    failed = InterventionRow(
        board_slug="fixture",
        task_id="t_reg1",
        phase="complete",
        outcome="error",
        error="unblock rejected",
        started_at=iso(T0 - 30),
        updated_at=iso(T0),
    )
    boards = (
        board_evidence(blocked_rows=((("t_reg1", "impl: x", T0 + HOUR, "need input", None),)),),
    )
    assert detect_daemon_regression((failed,), boards, now=T0 + 2 * DAY) == ()


def test_unparseable_updated_at_row_is_skipped_not_coerced() -> None:
    broken = InterventionRow(
        board_slug="fixture",
        task_id="t_reg1",
        phase="complete",
        outcome="ok",
        error=None,
        started_at="not-a-timestamp",
        updated_at="also-not-a-timestamp",
    )
    boards = (
        board_evidence(blocked_rows=((("t_reg1", "impl: x", T0 + HOUR, "need input", None),)),),
    )
    assert detect_daemon_regression((broken,), boards, now=T0 + 2 * DAY) == ()


def test_naive_iso_updated_at_treated_as_utc() -> None:
    naive = InterventionRow(
        board_slug="fixture",
        task_id="t_reg1",
        phase="complete",
        outcome="ok",
        error=None,
        started_at=iso(T0 - 30),
        updated_at=datetime.fromtimestamp(T0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )
    boards = (
        board_evidence(blocked_rows=((("t_reg1", "impl: x", T0 + HOUR, "need input", None),)),),
    )
    findings = detect_daemon_regression((naive,), boards, now=T0 + 2 * DAY)
    assert len(findings) == 1


# --- control 3: nonexistent DB path ------------------------------------------


def test_missing_db_returns_zero_without_raising(tmp_path: Path) -> None:
    missing = tmp_path / "does" / "not" / "exist.sqlite3"
    assert collect_interventions(missing, now=T0, window_hours=24) == ()
    assert detect_daemon_regression((), (), now=T0) == ()


# --- control 2: empty / no-interventions fixture ------------------------------


def test_empty_interventions_table_returns_zero_without_raising(tmp_path: Path) -> None:
    db = make_intervention_db(tmp_path / "state.sqlite3", [])
    assert collect_interventions(db, now=T0, window_hours=24) == ()
    assert detect_daemon_regression((), (), now=T0) == ()


def test_missing_interventions_table_is_fail_safe(tmp_path: Path) -> None:
    path = tmp_path / "other.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE unrelated (x INTEGER)")
    connection.commit()
    connection.close()
    assert collect_interventions(path, now=T0, window_hours=24) == ()


# --- collector window + parity knob ------------------------------------------


def test_collector_window_filters_on_iso_updated_at(tmp_path: Path) -> None:
    rows = [
        {"task_id": t, "updated_at": iso(T0 + offset), "outcome": "ok"}
        for t, offset in (("t_fresh", HOUR), ("t_stale", -25 * HOUR))
    ]
    db = make_intervention_db(tmp_path / "state.sqlite3", rows)
    collected = collect_interventions(db, now=T0 + 2 * HOUR, window_hours=24)
    assert [row.task_id for row in collected] == ["t_fresh"]
    assert collected[0].updated_at.endswith("+00:00")


def test_interventions_db_env_overrides_state_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hkrc.harness_loop import _interventions_db

    config = ControllerConfig(
        instance_name="interventions-knob",
        native_boards_root=tmp_path / "boards",
        state_db=tmp_path / "default.sqlite3",
    )
    assert _interventions_db(config) == tmp_path / "default.sqlite3"
    override = tmp_path / "override.sqlite3"
    monkeypatch.setenv("HKRC_INTERVENTIONS_DB", str(override))
    assert _interventions_db(config) == override
    monkeypatch.delenv("HKRC_INTERVENTIONS_DB")
    assert _interventions_db(config) == tmp_path / "default.sqlite3"
