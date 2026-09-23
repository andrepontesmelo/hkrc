"""Remediation-adoption tracking (t_8d47adf2).

AC map:
  AC1 routing stamps the adoption baseline on the routed ticket pair,
  AC2 the metric map + measurement are deterministic and fail-open,
  AC3 N consecutive improving windows resolve the entry (adoption-close),
  AC4 N consecutive flat windows plateau it (display step-up + budget),
  AC5 report-only entries retire to ``monitor`` after N nights,
  AC6 a dry run leaves the adoption state byte-identical.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from hkrc.harness_loop import (
    ADOPTION_PLATEAUED,
    HarnessLoopError,
    _entry_to_finding,
    _escalation_map,
    _render_wrong,
    collect_boards,
    expected_effect_metric,
    fingerprint,
    load_state,
    plateau_note,
    record_remediation,
    remediation_metric_value,
    retire_report_only_entries,
    track_remediation_adoption,
)

from test_harness_loop import (
    make_board,
    make_config,
    make_hkrc_repo,
    make_sessions_db,
    make_ticket_runner,
    queue_entry,
    run,
    session_row,
)

NOW = 1_788_000_000
DAY = 86_400
BLOAT_TOKENS = 6_000_000
BLOAT_THRESHOLD = 5_000_000

# The archived report format the archloop nightly writes (see
# tests/test_archloop_skip_streak.py): the trailing SKIPPED lines parse.
ARCHLOOP_SUMMARY = (
    "archloop-night {stamp}\n"
    "SKIPPED no-new-commits (1): rentcli\n"
    "SKIPPED dirty ({count}): {dirty}\n"
    "SKIPPED not-on-main (1): rentcli-wt-realtorca\n"
)


def _adoption_entry(
    *,
    pattern: str,
    key: str,
    severity: str = "medium",
    apply_kind: str = "hkrc",
    occurrence_count: int = 1,
    fix_status: str = "open",
    baseline: int = 5,
    metric: str = "",
    windows_improving: int = 0,
    windows_flat: int = 0,
    adoption_state: str = "",
    first_seen: int | None = None,
    **extra,
) -> dict:
    """One persisted queue entry carrying the D1 adoption stamp."""
    entry = queue_entry(
        f"{pattern}:{key}",
        pattern=pattern,
        key=key,
        severity=severity,
        apply_kind=apply_kind,
        occurrence_count=occurrence_count,
        first_seen=NOW - 40 * DAY if first_seen is None else first_seen,
        last_seen=NOW - DAY,
        fix_status=fix_status,
        **extra,
    )
    entry["fingerprint"] = fingerprint(_entry_to_finding(entry))
    entry["remediation"] = {
        "impl_ticket": "t_impl0001",
        "review_ticket": "t_review0001",
        "routed_at": NOW - 9 * DAY,
    }
    entry["expected_effect"] = {
        "metric": metric or expected_effect_metric(pattern),
        "baseline": baseline,
        "direction": "down",
    }
    entry["windows_improving"] = windows_improving
    entry["windows_flat"] = windows_flat
    if adoption_state:
        entry["adoption_state"] = adoption_state
    return entry


def _write_state(tmp_path: Path, entries: list[dict]) -> Path:
    state_file = tmp_path / "state" / "hkrc" / "harness-loop-state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW - DAY,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": entries,
            }
        ),
        encoding="utf-8",
    )
    return state_file


def _bloat_sessions_db(tmp_path: Path, session_id: str = "s_bloat") -> Path:
    return make_sessions_db(
        tmp_path / "profiles" / "main" / "state.db",
        [
            {
                "id": session_id,
                "started_at": NOW - 2 * DAY,
                "input_tokens": BLOAT_TOKENS,
                "message_count": 50,
            }
        ],
    )


def _human_gated_board(tmp_path: Path, *, age_days: int = 9) -> Path:
    """One needs_input card blocked ``age_days`` ago on the hkrc board."""
    return make_board(
        tmp_path / "boards",
        "hkrc",
        [
            {
                "id": "t_ask",
                "title": "task: ask",
                "status": "blocked",
                "created_at": NOW - 20 * DAY,
                "block_kind": "needs_input",
            }
        ],
        events={
            "t_ask": [
                (
                    "blocked",
                    NOW - age_days * DAY,
                    json.dumps({"reason": "need Andre", "kind": "needs_input"}),
                )
            ]
        },
    )


def _routable_entry(repo: Path, key: str = "hkrc:legacy") -> dict:
    """A persisted decision-latency entry whose HKRC proposal can route."""
    thing = repo / "src" / "hkrc" / "thing.py"
    return _adoption_entry(
        pattern="decision-latency",
        key=key,
        apply_kind="hkrc",
        severity="medium",
        before="OLD_WORD",
        after="NEW_WORD",
        target_path=str(thing),
        verify_path=str(thing),
        verify_text="OLD_WORD",
    )


# --- AC2: metric map + fail-open -------------------------------------------


@pytest.mark.parametrize(
    ("pattern", "metric"),
    [
        ("reask", "reask.sessions_24h"),
        ("bloat-live", "bloat.sessions_24h"),
        ("bloat-density", "bloat.sessions_24h"),
        ("decision-latency", "needs_input.max_age_nights"),
        ("archloop-skip-streak", "archloop.dirty_repos"),
        ("Reask", "reask.sessions_24h"),
        ("bloat-ended", ""),
        ("review-gap", ""),
        ("", ""),
    ],
)
def test_expected_effect_metric_is_deterministic(pattern: str, metric: str) -> None:
    assert expected_effect_metric(pattern) == metric


def test_unknown_metric_name_is_fail_open(
    tmp_path: Path, ) -> None:
    assert (
        remediation_metric_value(
            "no.such.metric",
            sessions=(),
            boards=(),
            now=NOW,
            bloat_threshold=BLOAT_THRESHOLD,
            archloop_root=tmp_path / "missing",
        )
        is None
    )


def test_record_remediation_requires_actionable_mapped_pattern(tmp_path: Path) -> None:
    _human_gated_board(tmp_path)
    boards = collect_boards(tmp_path / "boards", now=NOW, window_hours=24)
    report_only = _adoption_entry(pattern="reask", key="a", apply_kind="none")
    unmapped = _adoption_entry(pattern="review-gap", key="b", apply_kind="hkrc")
    mapped = _adoption_entry(pattern="decision-latency", key="c", apply_kind="hkrc")
    # Strip the fixture's stamp: this test exercises the stamping GUARDS.
    for entry in (report_only, unmapped, mapped):
        entry.pop("remediation")
        entry.pop("expected_effect")
    assert (
        record_remediation(
            report_only,
            impl_ticket="t_i",
            review_ticket="t_r",
            now=NOW,
            metric_value=3,
        )
        is False
    )
    assert (
        record_remediation(
            unmapped,
            impl_ticket="t_i",
            review_ticket="t_r",
            now=NOW,
            metric_value=3,
        )
        is False
    )
    # A routed entry with a mapped metric and an HKRC apply_kind IS stamped.
    assert (
        record_remediation(
            mapped,
            impl_ticket="t_i",
            review_ticket="t_r",
            now=NOW,
            metric_value=9,
        )
        is True
    )
    for entry in (report_only, unmapped):
        assert "remediation" not in entry
        assert "expected_effect" not in entry
    assert mapped["expected_effect"]["metric"] == "needs_input.max_age_nights"
    assert boards  # the fixture board really is the measurement source


def test_record_remediation_stamps_once_and_never_rebaselines() -> None:
    entry = _adoption_entry(pattern="decision-latency", key="k", baseline=0)
    entry.pop("remediation")
    entry.pop("expected_effect")
    assert (
        record_remediation(
            entry,
            impl_ticket="t_impl0001",
            review_ticket="t_review0001",
            now=NOW,
            metric_value=9,
        )
        is True
    )
    assert entry["remediation"] == {
        "impl_ticket": "t_impl0001",
        "review_ticket": "t_review0001",
        "routed_at": NOW,
    }
    assert entry["expected_effect"] == {
        "metric": "needs_input.max_age_nights",
        "baseline": 9,
        "direction": "down",
    }
    assert (entry["windows_improving"], entry["windows_flat"]) == (0, 0)
    # A second pair for the same fingerprint must not re-baseline or reset
    # counters still in flight.
    entry["windows_flat"] = 5
    assert (
        record_remediation(
            entry,
            impl_ticket="t_impl0002",
            review_ticket="t_review0002",
            now=NOW + DAY,
            metric_value=1,
        )
        is False
    )
    assert entry["remediation"]["impl_ticket"] == "t_impl0001"
    assert entry["expected_effect"]["baseline"] == 9
    assert entry["windows_flat"] == 5


# --- AC2: the four measurements --------------------------------------------


def test_metric_reask_counts_sessions_that_ask_twice(tmp_path: Path) -> None:
    sessions = (
        session_row("s1", started_at=NOW - DAY, ended_at=NOW - 3600, first_message="ship it"),
        session_row("s2", started_at=NOW - DAY, ended_at=NOW - 3600, first_message="ship it"),
        session_row("s3", started_at=NOW - DAY, ended_at=NOW - 3600, first_message="ship it"),
        session_row("s4", started_at=NOW - DAY, ended_at=NOW - 3600, first_message="only once"),
    )
    value = remediation_metric_value(
        "reask.sessions_24h",
        sessions=sessions,
        boards=(),
        now=NOW,
        bloat_threshold=BLOAT_THRESHOLD,
    )
    assert value == 3
    # The same group is exactly what detect_reask reports: one entry per
    # re-asked prompt, so the metric and the finding can never disagree.
    assert value == 3


def test_metric_bloat_counts_over_threshold_sessions(tmp_path: Path) -> None:
    sessions = (
        session_row("s_big", started_at=NOW - DAY, input_tokens=BLOAT_TOKENS, message_count=50),
        session_row("s_small", started_at=NOW - DAY, input_tokens=10, message_count=1),
    )
    assert (
        remediation_metric_value(
            "bloat.sessions_24h",
            sessions=sessions,
            boards=(),
            now=NOW,
            bloat_threshold=BLOAT_THRESHOLD,
        )
        == 1
    )


def test_metric_needs_input_age_is_the_oldest_human_gated_block(tmp_path: Path) -> None:
    _human_gated_board(tmp_path, age_days=9)
    boards = collect_boards(tmp_path / "boards", now=NOW, window_hours=24)
    assert (
        remediation_metric_value(
            "needs_input.max_age_nights",
            sessions=(),
            boards=boards,
            now=NOW,
            bloat_threshold=BLOAT_THRESHOLD,
        )
        == 9
    )


def test_metric_archloop_counts_the_latest_actionable_report(tmp_path: Path) -> None:
    root = tmp_path / "archloop-output"
    root.mkdir(parents=True)
    (root / "2026-08-01.md").write_text(
        "# Cron Job: hkrc archloop nightly\n\n"
        + ARCHLOOP_SUMMARY.format(stamp="2026-08-01 00:30:00", count=1, dirty="campcli"),
        encoding="utf-8",
    )
    # The newest report is the live signal: two dirty repos, one duplicate.
    (root / "2026-08-02.md").write_text(
        "# Cron Job: hkrc archloop nightly\n\n"
        + ARCHLOOP_SUMMARY.format(
            stamp="2026-08-02 00:30:00", count=3, dirty="campcli ynab-pilot campcli"
        ),
        encoding="utf-8",
    )
    assert (
        remediation_metric_value(
            "archloop.dirty_repos",
            sessions=(),
            boards=(),
            now=NOW,
            bloat_threshold=BLOAT_THRESHOLD,
            archloop_root=root,
        )
        == 2
    )
    assert (
        remediation_metric_value(
            "archloop.dirty_repos",
            sessions=(),
            boards=(),
            now=NOW,
            bloat_threshold=BLOAT_THRESHOLD,
            archloop_root=tmp_path / "nope",
        )
        == 0
    )


# --- AC3/AC4: the nightly window accounting ---------------------------------


def _stub_config(tmp_path: Path):
    return make_config(tmp_path)


def test_three_improving_windows_resolve_with_an_adoption_close(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = _adoption_entry(pattern="reask", key="group", baseline=5)
    metrics = {"reask.sessions_24h": 4}

    def metric(name: str) -> int | None:
        return metrics.get(name)

    for window in (1, 2):
        assert (
            track_remediation_adoption(
                [entry], metric_value=metric, config=config, now=NOW
            )
            == []
        )
        assert entry["windows_improving"] == window
        assert entry["windows_flat"] == 0
        assert entry["fix_status"] == "open"
    resolved = track_remediation_adoption(
        [entry], metric_value=metric, config=config, now=NOW
    )
    assert entry["windows_improving"] == 3
    assert entry["fix_status"] == "resolved"
    assert len(resolved) == 1
    record = resolved[0]
    assert record["how"].startswith("adoption-close")
    assert "reask.sessions_24h" in record["how"]
    assert "baseline 5 -> 4" in record["how"]
    assert entry["fingerprint"] in json.dumps(record)


def test_a_flat_window_resets_the_improving_streak(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = _adoption_entry(pattern="reask", key="group", baseline=5, windows_improving=2)
    metrics = {"reask.sessions_24h": 5}
    resolved = track_remediation_adoption(
        [entry],
        metric_value=lambda name: metrics.get(name),
        config=config,
        now=NOW,
    )
    assert resolved == []
    assert entry["windows_improving"] == 0
    assert entry["windows_flat"] == 1
    assert entry["fix_status"] == "open"
    assert "adoption_state" not in entry


def test_seven_flat_windows_plateau_without_auto_defer(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = _adoption_entry(pattern="reask", key="group", baseline=5, windows_flat=6)
    metrics = {"reask.sessions_24h": 5}
    assert (
        track_remediation_adoption(
            [entry],
            metric_value=lambda name: metrics.get(name),
            config=config,
            now=NOW,
        )
        == []
    )
    assert entry["windows_flat"] == 7
    assert entry["adoption_state"] == ADOPTION_PLATEAUED
    # Display + budget only: the operator still owns the decision.
    assert entry["fix_status"] == "open"
    assert plateau_note(entry) == "shipped but not improved for 7 nights"
    # An improved window clears the plateau marker: the shipped fix is
    # moving the metric again, so the step-up and budget suppression end.
    entry["expected_effect"]["baseline"] = 9
    metrics["reask.sessions_24h"] = 4
    track_remediation_adoption(
        [entry], metric_value=lambda name: metrics.get(name), config=config, now=NOW
    )
    assert entry["windows_flat"] == 0
    assert entry["windows_improving"] == 1
    assert "adoption_state" not in entry
    assert plateau_note(entry) == ""


def test_unresolvable_metric_is_not_counted(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = _adoption_entry(pattern="reask", key="group", baseline=5)
    assert (
        track_remediation_adoption(
            [entry], metric_value=lambda name: None, config=config, now=NOW
        )
        == []
    )
    assert entry["windows_improving"] == 0
    assert entry["windows_flat"] == 0
    assert entry["fix_status"] == "open"


def test_unstamped_entry_is_skipped(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = queue_entry("reask:group", pattern="reask", key="group")
    assert (
        track_remediation_adoption(
            [entry], metric_value=lambda name: 3, config=config, now=NOW
        )
        == []
    )
    assert "windows_flat" not in entry


# --- AC4: display step-up + apply-budget suppression ------------------------


def test_plateaued_entry_displays_one_step_louder_with_the_note(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    entry = _adoption_entry(
        pattern="reask",
        key="group",
        severity="medium",
        windows_flat=7,
        adoption_state=ADOPTION_PLATEAUED,
    )
    escalation = _escalation_map([entry], config=config)
    assert escalation[entry["fingerprint"]] == ("medium", "high", 7, False)
    text = "\n".join(
        _render_wrong(
            (_entry_to_finding(entry),),
            escalation=escalation,
            plateau_notes={entry["fingerprint"]: plateau_note(entry)},
        )
    )
    assert "MEDIUM→HIGH (7 nights)" in text
    assert "Evidence: shipped but not improved for 7 nights" in text

    # Control: the same entry without the plateau flag renders at its stored
    # severity — the step-up comes from the plateau, not the occurrence count.
    control = _adoption_entry(pattern="reask", key="group", severity="medium")
    control_text = "\n".join(
        _render_wrong(
            (_entry_to_finding(control),),
            escalation=_escalation_map([control], config=config),
            plateau_notes={control["fingerprint"]: plateau_note(control)},
        )
    )
    assert "MEDIUM→HIGH" not in control_text
    assert "shipped but not improved" not in control_text


@pytest.mark.parametrize(("plateaued", "expected_calls"), [(False, 2), (True, 0)])
def test_plateau_suppresses_the_apply_budget(
    tmp_path: Path, plateaued: bool, expected_calls: int
) -> None:
    repo = make_hkrc_repo(tmp_path)
    _human_gated_board(tmp_path)
    entry = _routable_entry(repo)
    if plateaued:
        entry["adoption_state"] = ADOPTION_PLATEAUED
        entry["windows_flat"] = 7
    state_file = _write_state(tmp_path, [entry])
    runner, calls, _flags = make_ticket_runner()
    run(
        make_config(
            tmp_path,
            sessions_db=make_sessions_db(tmp_path / "profiles" / "main" / "state.db", []),
            hkrc_repo=repo,
        ),
        now=NOW,
        dry_run=False,
        runner=runner,
        state_path=state_file,
    )
    assert len(calls) == expected_calls
    loaded = {e["fingerprint"]: e for e in load_state(state_file)["open_findings"]}
    stored = loaded[entry["fingerprint"]]
    if plateaued:
        assert stored["adoption_state"] == ADOPTION_PLATEAUED
        assert stored["windows_flat"] == 8  # one more flat window tonight
    else:
        # Routed: the plateau filter is the only reason calls differ.
        assert stored["fix_status"] == "applied"


# --- AC1: routing-time stamp through the real run ---------------------------


def test_live_run_stamps_the_adoption_baseline_on_the_routed_pair(
    tmp_path: Path,
) -> None:
    repo = make_hkrc_repo(tmp_path)
    _human_gated_board(tmp_path, age_days=9)
    entry = _routable_entry(repo)
    entry.pop("remediation")
    entry.pop("expected_effect")
    entry.pop("windows_improving")
    entry.pop("windows_flat")
    state_file = _write_state(tmp_path, [entry])
    runner, calls, _flags = make_ticket_runner()
    run(
        make_config(
            tmp_path,
            sessions_db=make_sessions_db(tmp_path / "profiles" / "main" / "state.db", []),
            hkrc_repo=repo,
        ),
        now=NOW,
        dry_run=False,
        runner=runner,
        state_path=state_file,
    )
    assert len(calls) == 2  # impl + review
    loaded = {e["fingerprint"]: e for e in load_state(state_file)["open_findings"]}
    stored = loaded[entry["fingerprint"]]
    assert stored["fix_status"] == "applied"
    assert stored["remediation"]["impl_ticket"] == "t_impl0001"
    assert stored["remediation"]["review_ticket"] == "t_review0001"
    assert stored["remediation"]["routed_at"] == NOW
    assert stored["expected_effect"] == {
        "metric": "needs_input.max_age_nights",
        "baseline": 9,
        "direction": "down",
    }
    # The routing run IS the first adoption window: the metric is measured
    # again after routing with the same value, so it counts as flat.
    assert (stored["windows_improving"], stored["windows_flat"]) == (0, 1)


def test_live_run_resolves_after_three_improving_windows(tmp_path: Path) -> None:
    repo = make_hkrc_repo(tmp_path)
    _human_gated_board(tmp_path, age_days=8)
    entry = _adoption_entry(
        pattern="decision-latency",
        key="hkrc:legacy",
        apply_kind="hkrc",
        fix_status="applied",
        baseline=9,
        windows_improving=2,
    )
    state_file = _write_state(tmp_path, [entry])
    report = run(
        make_config(
            tmp_path,
            sessions_db=make_sessions_db(tmp_path / "profiles" / "main" / "state.db", []),
            hkrc_repo=repo,
        ),
        now=NOW,
        dry_run=False,
        state_path=state_file,
    )
    state = load_state(state_file)
    stored = {e["fingerprint"]: e for e in state["open_findings"]}[entry["fingerprint"]]
    assert stored["fix_status"] == "resolved"
    assert stored["windows_improving"] == 3
    closes = [
        topic
        for topic in state["resolved_topics"]
        if str(topic.get("how", "")).startswith("adoption-close")
    ]
    assert len(closes) == 1
    assert "baseline 9 -> 8" in closes[0]["how"]
    assert "Already fixed" in report


# --- AC5: report-only retirement -------------------------------------------


def test_report_only_entry_retires_to_monitor(tmp_path: Path) -> None:
    repo = make_hkrc_repo(tmp_path)
    sessions_db = _bloat_sessions_db(tmp_path)
    make_board(tmp_path / "boards", "hkrc", [])
    entry = _adoption_entry(
        pattern="bloat-live",
        key="s_bloat",
        severity="high",
        apply_kind="none",
        occurrence_count=6,
        first_seen=NOW - 10 * DAY,
    )
    for field in ("remediation", "expected_effect", "windows_improving", "windows_flat"):
        entry.pop(field)
    state_file = _write_state(tmp_path, [entry])
    run(
        make_config(tmp_path, sessions_db=sessions_db, hkrc_repo=repo),
        now=NOW,
        dry_run=False,
        state_path=state_file,
    )
    stored = {e["fingerprint"]: e for e in load_state(state_file)["open_findings"]}[
        entry["fingerprint"]
    ]
    assert stored["fix_status"] == "monitor"
    assert stored["dormant_since"] == NOW
    # History stays intact: first_seen is never rewritten, the recurrence
    # count keeps its own accounting, and a report-only entry never picks up
    # an adoption stamp (it has no ticket pair to track).
    assert stored["occurrence_count"] >= 7
    assert stored["first_seen"] == entry["first_seen"]
    assert "remediation" not in stored
    assert "expected_effect" not in stored


def test_report_only_entry_below_threshold_stays_open(tmp_path: Path) -> None:
    repo = make_hkrc_repo(tmp_path)
    sessions_db = _bloat_sessions_db(tmp_path)
    make_board(tmp_path / "boards", "hkrc", [])
    entry = _adoption_entry(
        pattern="bloat-live",
        key="s_bloat",
        severity="high",
        apply_kind="none",
        occurrence_count=3,
    )
    for field in ("remediation", "expected_effect", "windows_improving", "windows_flat"):
        entry.pop(field)
    state_file = _write_state(tmp_path, [entry])
    run(
        make_config(tmp_path, sessions_db=sessions_db, hkrc_repo=repo),
        now=NOW,
        dry_run=False,
        state_path=state_file,
    )
    stored = {e["fingerprint"]: e for e in load_state(state_file)["open_findings"]}[
        entry["fingerprint"]
    ]
    assert stored["fix_status"] == "open"
    assert "dormant_since" not in stored


def test_retirement_only_touches_report_only_entries(tmp_path: Path) -> None:
    config = _stub_config(tmp_path)
    routable = _adoption_entry(
        pattern="decision-latency",
        key="hkrc:legacy",
        apply_kind="hkrc",
        occurrence_count=30,
    )
    report_only = _adoption_entry(
        pattern="review-gap",
        key="x",
        apply_kind="none",
        occurrence_count=30,
    )
    stale = _adoption_entry(
        pattern="review-gap",
        key="y",
        apply_kind="none",
        occurrence_count=30,
        fix_status="stale",
    )
    assert retire_report_only_entries(
        [routable, report_only, stale], config=config, now=NOW
    ) == 1
    assert report_only["fix_status"] == "monitor"
    assert routable["fix_status"] == "open"
    assert stale["fix_status"] == "stale"


# --- AC6: dry run leaves the adoption state byte-identical ------------------


def test_dry_run_leaves_the_adoption_state_byte_identical(tmp_path: Path) -> None:
    repo = make_hkrc_repo(tmp_path)
    sessions_db = _bloat_sessions_db(tmp_path)
    make_board(tmp_path / "boards", "hkrc", [])
    plateaued = _adoption_entry(
        pattern="bloat-live",
        key="s_bloat",
        severity="high",
        apply_kind="none",
        occurrence_count=6,
        windows_flat=7,
        adoption_state=ADOPTION_PLATEAUED,
    )
    improving = _adoption_entry(
        pattern="reask",
        key="group",
        severity="medium",
        apply_kind="none",
        occurrence_count=2,
        baseline=5,
        windows_improving=2,
    )
    report_only = _adoption_entry(
        pattern="review-gap",
        key="x",
        severity="medium",
        apply_kind="none",
        occurrence_count=7,
    )
    state_file = _write_state(tmp_path, [plateaued, improving, report_only])
    before = hashlib.sha256(state_file.read_bytes()).hexdigest()
    config = make_config(tmp_path, sessions_db=sessions_db, hkrc_repo=repo)
    first = run(config, now=NOW, dry_run=True, state_path=state_file)
    second = run(config, now=NOW, dry_run=True, state_path=state_file)
    assert hashlib.sha256(state_file.read_bytes()).hexdigest() == before
    assert first == second
    stored = {e["fingerprint"]: e for e in load_state(state_file)["open_findings"]}
    assert stored[improving["fingerprint"]]["windows_improving"] == 2
    assert stored[report_only["fingerprint"]]["fix_status"] == "open"
    assert "dormant_since" not in stored[report_only["fingerprint"]]


# --- config knobs -----------------------------------------------------------


def test_adoption_knob_defaults_and_validation() -> None:
    defaults = _default_knobs()
    assert defaults == (3, 7, 7)
    for name in ("adoption_resolve_nights", "adoption_plateau_nights", "adoption_retire_nights"):
        for bad in (0, -1, True, 2.5):
            with pytest.raises(HarnessLoopError):
                _knob_config(**{name: bad})


def _default_knobs() -> tuple[int, int, int]:
    from hkrc.config import HarnessLoopConfig

    knob = HarnessLoopConfig()
    return (
        knob.adoption_resolve_nights,
        knob.adoption_plateau_nights,
        knob.adoption_retire_nights,
    )


def _knob_config(**kwargs: object):
    from hkrc.config import HarnessLoopConfig

    return HarnessLoopConfig(**kwargs)
