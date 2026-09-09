"""collect_friction_flags + report section — t_ec5271e7 (report-only).

The daily harness loop REVISITS agent-appended friction flags; it has NO
detection mechanism of its own (grilling t_21b774b5 amendment).  Coverage:

- append-inert proof: a flagged run writes the flag row, leaves
  harness-loop-state.json byte-identical (dry-run contract), and routes
  zero cards;
- watermark consumption: rows are consumed via the ledger ``last_run``
  anchor — a CONSUMED store re-reports "0 new", never re-fires;
- explicit "unknown" lines when the store is unreadable (never silent);
- collector negative controls: missing DB / missing table / bad rows ->
  empty tuple, never a raise; LIVE count equals an independent sqlite3
  audit; strict ``>`` watermark boundary;
- report section rendering: total + by-severity + by-kind summary,
  xN-collapsed identical notes, "0 new" consumption proof.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hkrc.config import ControllerConfig, HarnessLoopConfig
from hkrc.harness_loop import (
    FrictionFlagRow,
    _friction_flag_lines,
    collect_friction_flags,
    default_state_path,
    load_state,
    render_report,
    run,
)
from test_harness_loop import (
    make_config,
    make_sessions_db,
    make_ticket_runner,
)

NOW = 100_000
DAY = 86_400
# ISO-8601 strings exactly as hkrc.state._utc_now writes them (UTC, second
# precision).  NOW=100_000 is 1970-01-02 UTC; the ledger anchor derived from
# it is "1970-01-02T03:46:40+00:00".
T_BEFORE = "1970-01-01T03:46:40+00:00"  # before any ledger anchor
T_AT = "1970-01-02T03:46:40+00:00"  # == the derived anchor (boundary)
T_AFTER = "1970-01-02T15:46:40+00:00"  # strictly after the anchor


def make_flag_store(path: Path, rows: tuple[dict, ...]) -> Path:
    """Seed a state DB with the schema-8 friction_flags table contract.

    Columns exactly as shipped by t_bfa10d50 (state.py SCHEMA 7->8); the
    CLI writer is NOT imported on purpose — this impl ticket owns
    harness_loop.py only, and the raw contract is the fixture.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS friction_flags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                severity TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high')),
                kind TEXT NOT NULL,
                note TEXT NOT NULL,
                session_ref TEXT,
                profile_ref TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_friction_flags_created_at "
            "ON friction_flags(created_at)"
        )
        connection.executemany(
            """
            INSERT INTO friction_flags
                (severity, kind, note, session_ref, profile_ref, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["severity"],
                    row["kind"],
                    row["note"],
                    row.get("session_ref"),
                    row.get("profile_ref"),
                    row["created_at"],
                )
                for row in rows
            ],
        )
        connection.commit()
    finally:
        connection.close()
    return path


def seeded_config(tmp_path: Path, rows: tuple[dict, ...]) -> ControllerConfig:
    """Config whose controller state DB carries the seeded flag store."""
    config = make_config(tmp_path)
    make_flag_store(Path(config.state_db), rows)
    return config


def friction_section(report: str) -> str:
    """Slice the report between the friction header and the next section.

    All section assertions go through this: the STORY line legitimately
    contains substrings like "0 new findings in this 24h window", so
    whole-report substring checks would pass for the wrong reason.
    """
    return report.split("Session friction flags", 1)[1].split("Next action", 1)[0]


def flag_row(
    created_at: str,
    *,
    severity: str = "medium",
    kind: str = "loop-friction",
    note: str = "flag note",
) -> dict:
    return {
        "severity": severity,
        "kind": kind,
        "note": note,
        "session_ref": None,
        "profile_ref": None,
        "created_at": created_at,
    }


# --- collector: negative controls -------------------------------------------


def test_collect_missing_db_returns_empty_no_raise(tmp_path: Path) -> None:
    notes: list[str] = []
    result = collect_friction_flags(
        tmp_path / "nope" / "state.sqlite3", notes=notes
    )
    assert result == ()
    assert notes and "missing" in notes[0]


def test_collect_db_without_table_returns_empty_no_raise(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE other (x INTEGER)")
    connection.commit()
    connection.close()
    notes: list[str] = []
    assert collect_friction_flags(db, notes=notes) == ()
    assert notes and "no friction_flags table" in notes[0]


def test_collect_unreadable_row_skipped_not_coerced(tmp_path: Path) -> None:
    store = make_flag_store(
        tmp_path / "state.sqlite3",
        (
            flag_row(T_AFTER),
            flag_row("not-a-timestamp", note="garbage row"),
        ),
    )
    result = collect_friction_flags(store, since=None)
    assert len(result) == 1
    assert result[0].created_at == T_AFTER


def test_collect_live_count_equals_independent_sqlite_audit(
    tmp_path: Path,
) -> None:
    """Alive-check: the collector's count equals a raw sqlite3 COUNT over
    the same window — proves the fixture rows are real and the collector
    is not silently dropping or inventing them."""
    rows = (
        flag_row(T_BEFORE),
        flag_row(T_AT),
        flag_row(T_AFTER),
        flag_row(T_AFTER, note="second distinct note"),
    )
    store = make_flag_store(tmp_path / "state.sqlite3", rows)
    result = collect_friction_flags(store, since=T_AT)
    connection = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    try:
        audit_total, audit_new = connection.execute(
            "SELECT COUNT(*), SUM(created_at > ?) FROM friction_flags",
            (T_AT,),
        ).fetchone()
    finally:
        connection.close()
    assert audit_total == 4
    # Strict >: the T_AT row itself is excluded from BOTH counts — the
    # collector and the independent SQL audit must agree exactly.
    assert len(result) == int(audit_new) == 2


def test_collect_returns_exactly_n_seeded_rows(tmp_path: Path) -> None:
    """Alive-check: an EMPTY-seeded-then-N-row fixture returns exactly N
    rows — the collector sees every seeded row (no window filtering when
    since=None, no per-row loss)."""
    seeded = (
        flag_row(T_BEFORE, severity="low"),
        flag_row(T_BEFORE, severity="high", note="another"),
    )
    store = make_flag_store(tmp_path / "state.sqlite3", seeded)
    result = collect_friction_flags(store, since=None)
    assert len(result) == 2
    assert {row.severity for row in result} == {"low", "high"}


def test_collect_strict_gt_watermark_boundary(tmp_path: Path) -> None:
    """A row stamped exactly AT the anchor belongs to the NEXT window
    (strict >): consumed rows never re-fire, boundary rows are not lost."""
    store = make_flag_store(
        tmp_path / "state.sqlite3", (flag_row(T_AT, note="boundary"),)
    )
    assert collect_friction_flags(store, since=T_AT) == ()
    assert len(collect_friction_flags(store, since=T_BEFORE)) == 1


# --- report section ----------------------------------------------------------


def test_friction_flag_lines_summary_and_collapse() -> None:
    flags = (
        FrictionFlagRow("medium", "loop-friction", "same note", None, None, T_AFTER),
        FrictionFlagRow("medium", "loop-friction", "same note", None, None, T_AFTER),
        FrictionFlagRow("high", "tool-failure", "other note", None, None, T_AFTER),
    )
    lines = _friction_flag_lines(flags)
    assert lines[0] == (
        "3 new (severity: high x1, medium x2; "
        "kind: loop-friction x2, tool-failure x1)"
    )
    assert "[medium] loop-friction: same note (x2)" in lines
    assert "[high] tool-failure: other note" in lines


def test_friction_flag_lines_zero_and_unknown() -> None:
    assert _friction_flag_lines(()) == ("0 new",)
    assert _friction_flag_lines((), unknown_note="store missing") == (
        "unknown — store missing",
    )


def test_report_renders_friction_section_when_nothing_new(
    tmp_path: Path,
) -> None:
    """Consumed-proof discipline: a readable store whose only row is at-or
    before the ledger anchor still renders the section with an explicit
    '0 new' — never silence."""
    config = seeded_config(tmp_path, (flag_row(T_BEFORE),))
    store = Path(config.state_db)
    state_file = default_state_path(store)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps({"last_run": NOW}))
    report = run(config, now=NOW, dry_run=True)
    assert "Session friction flags (new since last run, report-only)" in report
    assert "0 new" in friction_section(report)


def test_report_renders_unknown_when_store_unreadable(
    tmp_path: Path,
) -> None:
    """A MISSING store renders an explicit unknown line — silence never
    masquerades as health (never a bare '0 new' for an unreadable store)."""
    config = make_config(tmp_path)
    report = run(config, now=NOW, dry_run=True)
    assert "unknown — friction flag store missing" in friction_section(report)


# --- end-to-end: append-inert + watermark consumption ------------------------


def test_flagged_run_append_inert_and_zero_cards(tmp_path: Path) -> None:
    """AC: the flagged run itself is append-inert — the new row lands in
    the report, harness-loop-state.json stays byte-identical (dry run
    persists nothing), and zero kanban cards are routed."""
    sessions_db = make_sessions_db(tmp_path / "profiles" / "main" / "state.db", [])
    config = make_config(tmp_path, sessions_db=sessions_db)
    make_flag_store(
        Path(config.state_db),
        (
            flag_row(T_AFTER, severity="high", kind="tool-failure", note="bash guard"),
            flag_row(T_AFTER, note="bash guard"),
        ),
    )
    state_file = default_state_path(config.state_db)
    state_before = state_file.read_bytes() if state_file.exists() else None
    runner, calls, _flags = make_ticket_runner()
    report = run(config, now=NOW, dry_run=True, runner=runner)
    section = friction_section(report)
    assert "2 new" in section
    assert "[high] tool-failure: bash guard" in section
    assert "[medium] loop-friction: bash guard" in section
    assert calls == []  # zero cards — report-only, no detection mechanism
    if state_before is None:
        assert not state_file.exists()  # dry run created no ledger
    else:
        assert state_file.read_bytes() == state_before


def test_consumed_flags_do_not_refire_next_live_run(tmp_path: Path) -> None:
    """AC: the ledger last_run anchor is the watermark.  The first run has
    no anchor, so it sweeps the backlog (all rows); its live run persists
    last_run=NOW, and rows stamped at-or-before that anchor are consumed —
    the NEXT run reports '0 new' for the same store (no re-fire) while a
    row stamped after the anchor IS picked up."""
    sessions_db = make_sessions_db(tmp_path / "profiles" / "main" / "state.db", [])
    config = make_config(tmp_path, sessions_db=sessions_db)
    store = Path(config.state_db)
    # Backlog row: created before the anchor the first run will persist.
    make_flag_store(store, (flag_row(T_BEFORE, note="first wave"),))
    runner, calls, _flags = make_ticket_runner()
    first = run(config, now=NOW, dry_run=False, runner=runner)
    assert "first wave" in friction_section(first)
    assert calls == []
    # The live run persisted the anchor at NOW.
    assert load_state(default_state_path(store))["last_run"] == NOW
    # Next run, same store, nothing added: consumed rows never re-fire.
    second = run(config, now=NOW + DAY, dry_run=True, runner=runner)
    section = friction_section(second)
    assert "0 new" in section
    assert "first wave" not in section
    # A row stamped after the persisted anchor is fresh again.
    make_flag_store(
        store, (flag_row("1970-01-03T03:46:40+00:00", note="second wave"),)
    )
    third = run(config, now=NOW + 2 * DAY, dry_run=True, runner=runner)
    section = friction_section(third)
    assert "second wave" in section
    assert "0 new" not in section


def test_first_run_anchors_from_ledger_last_run_not_now(tmp_path: Path) -> None:
    """The watermark is the LEDGER's last_run anchor, not the wall clock:
    a pre-seeded ledger with last_run=NOW-DAY makes a row created between
    NOW-DAY and NOW new, even though the run's `now` is NOW."""
    sessions_db = make_sessions_db(tmp_path / "profiles" / "main" / "state.db", [])
    config = make_config(tmp_path, sessions_db=sessions_db)
    store = Path(config.state_db)
    make_flag_store(store, (flag_row(T_AFTER, note="midwindow"),))
    state_file = default_state_path(store)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps({"last_run": NOW - DAY}))
    report = run(config, now=NOW, dry_run=True)
    assert "midwindow" in friction_section(report)


def test_trace_carries_friction_flag_stats(tmp_path: Path) -> None:
    sessions_db = make_sessions_db(tmp_path / "profiles" / "main" / "state.db", [])
    config = make_config(tmp_path, sessions_db=sessions_db)
    make_flag_store(
        Path(config.state_db),
        (
            flag_row(T_AFTER, severity="high", kind="tool-failure", note="n1"),
            flag_row(T_AFTER, severity="high", kind="tool-failure", note="n1"),
            flag_row(T_AFTER, severity="low", kind="loop-friction", note="n2"),
        ),
    )
    trace: list[dict] = []
    run(config, now=NOW, dry_run=True, trace=trace)
    stats = trace[-1]["friction_flags"]
    assert stats["new_total"] == 3
    assert stats["by_severity"] == {"high": 2, "low": 1}
    assert stats["by_kind"] == {"tool-failure": 2, "loop-friction": 1}
    assert stats["since"] is None
    assert stats["until"] != ""
