"""Loop self-health — t_38102b45.

The nightly loop audits ~20 boards but never audits its own funnel
(detect -> propose -> route).  These tests cover the two stall signatures,
the funnel/oldest-HIGH/prune-vs-resolve report block, the unknown-analyzer
freeze (an analysis outage must never manufacture a stall), the report-only
stance, and the dry-run/live ledger split.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from hkrc.config import (
    ConfigError,
    ControllerConfig,
    HarnessLoopConfig,
    load_config,
    write_config,
)
from hkrc.harness_loop import (
    FUNNEL_STAGES,
    SELF_HEALTH_EVENT_RETENTION_DAYS,
    SELF_HEALTH_TREND_DAYS,
    STALLED_LOOP_PATTERN,
    STALLED_PROPOSALS_KEY,
    STALLED_ROUTING_KEY,
    HarnessLoopError,
    HarnessReport,
    _detect_all,
    _funnel_streaks,
    _git_log_indicates_fixed,
    _loop_self_health_section,
    _self_health_event_total,
    default_state_path,
    detect_stalled_loop,
    fingerprint,
    load_state,
    record_funnel_streaks,
    record_self_health_events,
    render_report,
    run,
    save_state,
)

NOW = 1_700_000_000
DAY = 86400


def _today(now: int) -> str:
    return datetime.fromtimestamp(int(now), tz=timezone.utc).strftime("%Y-%m-%d")


def make_config(tmp_path: Path, **harness: Any) -> ControllerConfig:
    return ControllerConfig(
        "test",
        tmp_path / "boards",
        tmp_path / "state" / "hkrc" / "state.sqlite3",
        harness_loop=HarnessLoopConfig(
            enabled=True,
            sessions_db=tmp_path / "profiles" / "main" / "state.db",
            external_dirs=(str(tmp_path / "dist"),),
            hkrc_repo=tmp_path / "repo",
            dist_skills_root=str(tmp_path / "dist"),
            profiles_root=str(tmp_path / "profiles"),
            archloop_output_dir=str(tmp_path / "archloop-output"),
            cron_jobs_path=str(tmp_path / "cron" / "jobs.json"),
            **harness,
        ),
    )


def high_entry(
    fp: str = "reask:abc",
    *,
    severity: str = "high",
    first_seen: int = NOW,
    last_seen: int = NOW,
    fix_status: str = "open",
) -> dict:
    pattern, _, key = fp.partition(":")
    return {
        "fingerprint": fp,
        "pattern": pattern,
        "key": key,
        "severity": severity,
        "occurrence_count": 1,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "fix_status": fix_status,
        "evidence": ["seeded"],
        "suggestion": "seeded",
        "apply_kind": "none",
    }


def stalled_state(proposed: int, routed: int, *, detected: int = 0) -> dict:
    """Ledger view with the given per-stage zero-night streaks."""
    return {
        "funnel_streaks": {
            "detected": {"zero_nights": detected, "last": 0},
            "proposed": {"zero_nights": proposed, "last": 0},
            "routed": {"zero_nights": routed, "last": 0},
        }
    }


# --- streak accounting -------------------------------------------------


def test_funnel_streaks_fold_over_mixed_nights() -> None:
    state: dict = {}
    assert _funnel_streaks(state) == {stage: 0 for stage in FUNNEL_STAGES}
    record_funnel_streaks(state, detected=3, proposed=1, routed=1)
    assert _funnel_streaks(state) == {"detected": 0, "proposed": 0, "routed": 0}
    for _ in range(3):
        record_funnel_streaks(state, detected=0, proposed=0, routed=0)
    assert _funnel_streaks(state) == {"detected": 3, "proposed": 3, "routed": 3}
    # record_funnel_streaks persists a FRESH blob dict, so re-read it.
    blob = state["funnel_streaks"]
    assert blob["routed"]["last"] == 0
    assert blob["routed_total"] == 1
    # Analysis failed: proposals/routed are UNKNOWN -> both streaks FREEZE
    # (neither bump nor reset), so an analyzer outage cannot fake a stall.
    record_funnel_streaks(state, detected=4, proposed=None, routed=None)
    assert _funnel_streaks(state) == {"detected": 0, "proposed": 3, "routed": 3}
    blob = state["funnel_streaks"]
    assert blob["detected"]["last"] == 4
    assert blob["proposed"]["last"] == 0
    # A non-zero proposal resets only the proposal streak.
    record_funnel_streaks(state, detected=0, proposed=2, routed=0)
    assert _funnel_streaks(state) == {"detected": 1, "proposed": 0, "routed": 4}
    blob = state["funnel_streaks"]
    assert blob["routed_total"] == 1
    # Garbage in the blob is tolerated defensively.
    assert _funnel_streaks({"funnel_streaks": "nope"}) == {
        stage: 0 for stage in FUNNEL_STAGES
    }


# --- detector ----------------------------------------------------------


def test_stall_fires_at_threshold_only() -> None:
    working = [high_entry()]
    assert detect_stalled_loop(
        stalled_state(6, 0), working, stall_after_nights=7
    ) == ()
    fired = detect_stalled_loop(stalled_state(7, 0), working, stall_after_nights=7)
    assert len(fired) == 1
    assert fired[0].pattern == STALLED_LOOP_PATTERN
    assert fired[0].key == STALLED_PROPOSALS_KEY
    assert fired[0].severity == "high"
    assert fired[0].apply_kind == "none"  # report-only: never a commit
    # A stall threshold of 0 (disabled) never fires.
    assert detect_stalled_loop(stalled_state(30, 30), working, stall_after_nights=0) == ()


def test_stalled_proposals_needs_a_high_in_the_working_set() -> None:
    state = stalled_state(9, 0)
    assert detect_stalled_loop(state, [], stall_after_nights=7) == ()
    assert detect_stalled_loop(
        state, [high_entry(severity="medium")], stall_after_nights=7
    ) == ()
    # A display-escalated medium counts as HIGH.
    escalated = {"reask:abc": ("medium", "high", 7, False)}
    assert detect_stalled_loop(
        state,
        [high_entry(severity="medium")],
        stall_after_nights=7,
        escalation=escalated,
    )
    # Stored HIGH is enough on its own.
    assert detect_stalled_loop(state, [high_entry()], stall_after_nights=7)


def test_routing_signature_and_precedence() -> None:
    working = [high_entry()]
    routed = detect_stalled_loop(stalled_state(2, 7), working, stall_after_nights=7)
    assert [finding.key for finding in routed] == [STALLED_ROUTING_KEY]
    assert routed[0].severity == "high" and routed[0].apply_kind == "none"
    # Both stalled -> proposals wins, exactly one finding.
    both = detect_stalled_loop(stalled_state(9, 9), working, stall_after_nights=7)
    assert [finding.key for finding in both] == [STALLED_PROPOSALS_KEY]
    # Proposals flowing (but under threshold) with routing stalled -> routing.
    assert [
        finding.key
        for finding in detect_stalled_loop(
            stalled_state(1, 30), working, stall_after_nights=7
        )
    ] == [STALLED_ROUTING_KEY]
    # Routing stalled while proposals are BELOW threshold but the loop is
    # fine -> nothing fires below the threshold.
    assert (
        detect_stalled_loop(stalled_state(0, 6), working, stall_after_nights=7) == ()
    )


def test_stall_recurrence_dedupes_to_one_entry() -> None:
    # The signature fires every following night; the ledger keeps ONE entry
    # and grows occurrence_count (no nightly duplicate rows).
    from hkrc.harness_loop import dedupe

    finding = detect_stalled_loop(
        stalled_state(7, 7), [high_entry()], stall_after_nights=7
    )[0]
    fp = fingerprint(finding)
    state: dict = {}
    for night in range(3):
        fresh, state = dedupe(
            (finding,), state, now=NOW + night * DAY, cooldown_days=30
        )
    entries = [
        entry
        for entry in state["open_findings"]
        if entry.get("fingerprint") == fp
    ]
    assert len(entries) == 1
    assert entries[0]["occurrence_count"] == 3
    # Report-only: never recorded as a suggestion (so it re-reports nightly).
    assert fp not in {
        entry.get("fingerprint") for entry in state["suggested_fingerprints"]
    }


def test_git_log_mention_never_resolves_a_stall() -> None:
    # This feature's own branch/commit name must not resolve a live stall:
    # the fix is the loop producing/routing again, not an HKRC commit.
    finding = detect_stalled_loop(
        stalled_state(7, 0), [high_entry()], stall_after_nights=7
    )[0]
    fp = fingerprint(finding)
    log = "abc123 feat: stalled-loop detector (wt/t_38102b45)\n"
    assert _git_log_indicates_fixed(finding, log, fp) is False


def test_detect_all_emits_the_stall_with_the_configured_threshold(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, stall_after_nights=14)
    state = stalled_state(7, 7)
    state["open_findings"] = [high_entry()]
    assert [
        finding.key
        for finding in _detect_all((), (), "", NOW, config, state=state)
        if finding.pattern == STALLED_LOOP_PATTERN
    ] == []
    state["funnel_streaks"]["proposed"]["zero_nights"] = 14
    state["funnel_streaks"]["routed"]["zero_nights"] = 14
    assert [
        finding.key
        for finding in _detect_all((), (), "", NOW, config, state=state)
        if finding.pattern == STALLED_LOOP_PATTERN
    ] == [STALLED_PROPOSALS_KEY]


# --- report block ------------------------------------------------------


def test_section_renders_three_lines_healthy_and_verdict_when_stalled() -> None:
    state = stalled_state(2, 0)
    state["funnel_streaks"]["routed_total"] = 4
    state["self_health_events"] = {
        "pruned": [{"date": _today(NOW), "count": 3}],
        "resolved": [{"date": _today(NOW), "count": 1}],
    }
    working = [high_entry(first_seen=NOW - 9 * DAY)]
    lines = _loop_self_health_section(
        state, working, detected=12, proposed=3, routed=1, now=NOW
    )
    assert len(lines) == 3
    assert "detected 12 -> proposed 3 -> routed 1" in lines[0]
    assert "per-stage zero-streak nights:" in lines[0]
    assert "proposed 2" in lines[0] and "routed 0" in lines[0]
    assert "routed total 4" in lines[0]
    assert "oldest HIGH in the working set: 9 nights" in lines[1]
    assert f"rolling {SELF_HEALTH_TREND_DAYS}d" in lines[2]
    assert "3 pruned, 1 resolved" in lines[2]
    # Analyzer failed: proposals render "-" and no HIGH is inventable.
    failed = _loop_self_health_section(
        state, [], detected=2, proposed=None, routed=0, now=NOW
    )
    assert "proposed -" in failed[0]
    assert "oldest HIGH: - (no HIGH in the working set)" in failed[1]
    # Verdict line appears ONLY while a signature fires.
    stalled = detect_stalled_loop(
        stalled_state(7, 0), [high_entry()], stall_after_nights=7
    )
    verdict = _loop_self_health_section(
        state,
        [high_entry()],
        detected=0,
        proposed=0,
        routed=0,
        now=NOW,
        stalled=stalled,
    )
    assert len(verdict) == 4
    assert verdict[3].startswith(f"• stalled verdict: {STALLED_PROPOSALS_KEY} fired")
    assert "report-only" in verdict[3]


def test_report_places_loop_self_health_after_cron_before_next_action() -> None:
    report = HarnessReport(
        story="story",
        wrong=(),
        skipped=(),
        applied=(),
        deploy_ready="none",
        right=(),
        next_action="do the thing",
        cron_self_health=("• supervisor: enabled, last_status success",),
        loop_self_health=("• funnel: detected 1 -> proposed 0 -> routed 0",),
    )
    text = render_report(report)
    assert (
        text.index("Cron self-health")
        < text.index("Loop self-health")
        < text.index("Next action")
    )
    assert "detected 1 -> proposed 0 -> routed 0" in text
    # A report built without the block (every older caller) still renders.
    bare = render_report(replace(report, loop_self_health=()))
    assert "Loop self-health" in bare


# --- prune/resolve trend ----------------------------------------------


def test_self_health_events_record_real_events_only_and_cap_at_60d() -> None:
    state: dict = {}
    record_self_health_events(state, resolved=0, pruned=0, now=NOW)
    assert state["self_health_events"] == {"pruned": [], "resolved": []}
    assert _self_health_event_total(state, "pruned", now=NOW) == 0
    record_self_health_events(state, resolved=2, pruned=3, now=NOW)
    assert state["self_health_events"]["resolved"] == [
        {"date": _today(NOW), "count": 2}
    ]
    assert _self_health_event_total(state, "pruned", now=NOW) == 3
    # The rolling window is 14d: an older row leaves the total but stays
    # in the list (the trend line is a window, not a lifetime total).
    later = NOW + (SELF_HEALTH_TREND_DAYS + 1) * DAY
    record_self_health_events(state, resolved=1, pruned=0, now=later)
    assert _self_health_event_total(state, "pruned", now=later) == 0
    assert _self_health_event_total(state, "resolved", now=later) == 1
    assert len(state["self_health_events"]["pruned"]) == 1
    # 60-day cap bounds the state file: an old row drops out, rows still
    # inside the retention window stay.
    record_self_health_events(
        state,
        resolved=5,
        pruned=0,
        now=NOW + (SELF_HEALTH_EVENT_RETENTION_DAYS + 1) * DAY,
    )
    assert [row["date"] for row in state["self_health_events"]["resolved"]] == [
        _today(later),
        _today(NOW + (SELF_HEALTH_EVENT_RETENTION_DAYS + 1) * DAY),
    ]


# --- run() wiring -----------------------------------------------------


def test_dry_run_reports_the_stall_and_touches_nothing(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    seeded = stalled_state(7, 7)
    seeded["open_findings"] = [high_entry()]
    save_state(state_file, seeded)
    before = state_file.read_bytes()
    report = run(config, now=NOW, dry_run=True, state_path=state_file)
    assert "Loop self-health (report-only)" in report
    assert f"stalled verdict: {STALLED_PROPOSALS_KEY} fired" in report
    # A dry run is idempotent and never folds a night into the ledger.
    assert run(config, now=NOW, dry_run=True, state_path=state_file) == report
    assert state_file.read_bytes() == before
    persisted = load_state(state_file)
    assert "self_health_events" not in persisted
    assert not [
        entry
        for entry in persisted["open_findings"]
        if entry.get("pattern") == STALLED_LOOP_PATTERN
    ]


def test_live_run_folds_the_funnel_and_enters_the_stall(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    seeded = stalled_state(7, 7)
    seeded["funnel_streaks"]["routed_total"] = 4
    seeded["open_findings"] = [high_entry()]
    save_state(state_file, seeded)
    report = run(config, now=NOW, dry_run=False, state_path=state_file)
    assert "Loop self-health (report-only)" in report
    assert f"stalled verdict: {STALLED_PROPOSALS_KEY} fired" in report
    persisted = load_state(state_file)
    # Tonight's zero night is folded on top of the seed.
    assert persisted["funnel_streaks"]["proposed"]["zero_nights"] == 8
    assert persisted["funnel_streaks"]["routed"]["zero_nights"] == 8
    assert persisted["funnel_streaks"]["routed_total"] == 4
    stalled = [
        entry
        for entry in persisted["open_findings"]
        if entry.get("pattern") == STALLED_LOOP_PATTERN
    ]
    assert len(stalled) == 1
    assert stalled[0]["key"] == STALLED_PROPOSALS_KEY
    assert stalled[0]["severity"] == "high"
    assert stalled[0]["apply_kind"] == "none"
    # Nothing routed -> no resolve/prune events were worth recording.
    assert persisted["self_health_events"] == {"pruned": [], "resolved": []}


def test_live_run_records_a_resolve_event_only_on_a_real_flip(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    gone = high_entry("reask:gone", severity="medium")
    gone["verify_path"] = str(tmp_path / "not-there.txt")
    gone["verify_text"] = "needle"
    save_state(state_file, {"open_findings": [gone]})
    report = run(config, now=NOW, dry_run=False, state_path=state_file)
    persisted = load_state(state_file)
    assert persisted["self_health_events"]["resolved"] == [
        {"date": _today(NOW), "count": 1}
    ]
    assert persisted["self_health_events"]["pruned"] == []
    assert "1 resolved" in report
    # Next night nothing flips -> no new row (a zero row is noise).
    run(config, now=NOW + DAY, dry_run=False, state_path=state_file)
    assert load_state(state_file)["self_health_events"]["resolved"] == [
        {"date": _today(NOW), "count": 1}
    ]


def test_live_run_records_a_prune_event_only_on_a_real_prune(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    stale = high_entry(
        "bloat:old",
        severity="medium",
        last_seen=NOW - 20 * DAY,
        fix_status="stale",
    )
    save_state(state_file, {"open_findings": [stale]})
    run(config, now=NOW, dry_run=False, state_path=state_file)
    persisted = load_state(state_file)
    assert persisted["self_health_events"]["pruned"] == [
        {"date": _today(NOW), "count": 1}
    ]
    assert not [
        entry
        for entry in persisted["open_findings"]
        if entry.get("fingerprint") == "bloat:old"
    ]


# --- config ------------------------------------------------------------


def test_stall_after_nights_round_trips_and_validates(tmp_path: Path) -> None:
    assert HarnessLoopConfig().stall_after_nights == 7
    with pytest.raises(HarnessLoopError, match="stall_after_nights"):
        HarnessLoopConfig(stall_after_nights=0)
    config = make_config(tmp_path, stall_after_nights=5)
    path = tmp_path / "config.toml"
    write_config(path, config)
    text = path.read_text(encoding="utf-8")
    assert "stall_after_nights = 5" in text
    assert load_config(path) == config
    # A legacy config without the key keeps the documented default.
    # Drop only the key's own line: tmp_path embeds this test's name, so a
    # substring filter would also delete every path line referencing it.
    legacy = tmp_path / "legacy.toml"
    legacy.write_text(
        "".join(
            line
            for line in text.splitlines(keepends=True)
            if not line.startswith("stall_after_nights")
        ),
        encoding="utf-8",
    )
    reloaded = load_config(legacy)
    assert reloaded.harness_loop.stall_after_nights == 7
    assert reloaded == replace(
        config, harness_loop=replace(config.harness_loop, stall_after_nights=7)
    )
    # Out-of-range values are rejected at load time, not silently clamped.
    bad = tmp_path / "bad.toml"
    bad.write_text(
        text.replace("stall_after_nights = 5", "stall_after_nights = 0"),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="stall_after_nights"):
        load_config(bad)


def test_config_default_floor_is_documented_next_to_the_ladder() -> None:
    # The stalled-loop knob shares the ladder block; a nonsense value in
    # the dataclass is rejected the same way chronic/escalate is.
    with pytest.raises(HarnessLoopError, match="stall_after_nights"):
        HarnessLoopConfig(chronic_after_nights=21, stall_after_nights=True)
    with pytest.raises(HarnessLoopError, match="chronic_after_nights"):
        HarnessLoopConfig(escalate_after_nights=9, chronic_after_nights=3)


def test_state_file_gains_no_new_keys_from_a_stall(tmp_path: Path) -> None:
    # The stall rides the existing ledger: funnel streaks and self-health
    # events live inside the approved state file, no schema bump.
    config = make_config(tmp_path)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    save_state(state_file, stalled_state(7, 7) | {"open_findings": [high_entry()]})
    run(config, now=NOW, dry_run=False, state_path=state_file)
    persisted = load_state(state_file)
    assert set(persisted) <= {
        "created",
        "last_run",
        "open_findings",
        "resolved_topics",
        "suggested_fingerprints",
        "funnel_streaks",
        "self_health_events",
        "cron_self_health",
        # t_c9da2f07 (main): the next-action dedupe map is the sibling
        # ladder's approved ledger key, not one the stalled-loop feature
        # introduced -- this test only guards the latter.
        "next_action_dedupe",
        "claimed",
        "queued",
    }
    assert set(persisted["funnel_streaks"]) <= {
        "detected",
        "proposed",
        "routed",
        "routed_total",
    }


def test_oldest_high_age_is_days_since_first_seen() -> None:
    # Guard the trend column against a hardcoded "monitor" style display:
    # the age always comes from first_seen, never from last_seen.
    entry = high_entry(first_seen=NOW - 30 * DAY, last_seen=NOW)
    lines = _loop_self_health_section(
        {}, [entry], detected=1, proposed=1, routed=1, now=NOW
    )
    assert "oldest HIGH in the working set: 30 nights" in lines[1]
    fresh = _loop_self_health_section(
        {}, [entry], detected=1, proposed=1, routed=1, now=NOW - 40 * DAY
    )
    assert "oldest HIGH in the working set: 0 nights" in fresh[1]


def test_event_window_semantics_are_a_lower_bound_only() -> None:
    # The rolling window is a LOWER bound on the date (ISO string compare):
    # a row dated ahead of ``now`` (skewed clock) still counts once it is
    # inside the 60-day retention cap, and the recorder never drops it.
    # Documented here so nobody "fixes" the window into an upper bound
    # without thinking about the retention cap that keeps rows.
    state = {
        "self_health_events": {
            "pruned": [{"date": _today(NOW + 5 * DAY), "count": 9}],
            "resolved": [],
        }
    }
    record_self_health_events(state, resolved=0, pruned=0, now=NOW)
    assert _self_health_event_total(state, "pruned", now=NOW) == 9
    # Rows older than the window (and than the 60d cap) drop out.
    state["self_health_events"]["pruned"].append(
        {"date": _today(NOW - (SELF_HEALTH_EVENT_RETENTION_DAYS + 1) * DAY), "count": 4}
    )
    record_self_health_events(state, resolved=0, pruned=0, now=NOW)
    assert _self_health_event_total(state, "pruned", now=NOW) == 9
    assert [
        row["date"] for row in state["self_health_events"]["pruned"]
    ] == [_today(NOW + 5 * DAY)]
