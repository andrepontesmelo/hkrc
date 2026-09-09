"""detect_cron_self_health — t_78c47d92.

Covers the seven required negative/positive controls from the ticket, plus
the wiring contracts: resolver precedence, config round-trip, report
section rendering, the double-count guard (detected_fps), and the
dry-run/live ledger persistence split.
"""

from __future__ import annotations

import json
from pathlib import Path

from hkrc.config import ControllerConfig, HarnessLoopConfig, load_config, write_config
from hkrc.harness_loop import (
    CRON_SELF_HEALTH_WATCHED,
    HarnessReport,
    _cron_self_health_scan,
    _cron_self_health_section,
    _cron_self_health_streaks,
    _detect_all,
    default_state_path,
    fingerprint,
    load_state,
    record_cron_self_health_streaks,
    run,
)

NOW = 100_000

IDS = [job_id for job_id, _label in CRON_SELF_HEALTH_WATCHED]
SUPERVISOR_ID = "1369f0027b78"
SUPERVISOR_LABEL = "HKRC harness supervisor"


def make_store(path: Path, **overrides: dict) -> Path:
    """Write a jobs.json in the live wrapper-dict shape; all 6 healthy."""
    jobs = []
    for index, job_id in enumerate(IDS):
        job = {
            "id": job_id,
            "name": f"job-{index}",
            "enabled": True,
            "last_status": "success",
            # schedule is a NESTED dict in the live store, never a string.
            "schedule": {"kind": "cron", "expr": "0 3 * * *", "display": "daily"},
        }
        job.update(overrides.get(job_id, {}))
        jobs.append(job)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"jobs": jobs, "updated_at": "2026-09-02T00:00:00"}))
    return path


def make_config(tmp_path: Path, store: Path) -> ControllerConfig:
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
            cron_jobs_path=str(store),
        ),
    )


# --- Control 1 (live) is executed manually against the real store and
# --- pasted into the task comment; it cannot live in pytest because it
# --- depends on operator state that changes between runs.


def test_control_2_missing_store_zero_findings_no_raise(tmp_path: Path) -> None:
    # MUST return zero findings and MUST NOT raise (fail-safe; the store
    # could plausibly be mid-write when read).
    missing = tmp_path / "cron" / "jobs.json"
    assert _cron_self_health_scan(missing) == ()
    # Corrupt (mid-write shape) store: same fail-safe contract.
    corrupt = tmp_path / "cron" / "corrupt.json"
    corrupt.parent.mkdir(parents=True, exist_ok=True)
    corrupt.write_text('{"jobs": [')
    assert _cron_self_health_scan(corrupt) == ()
    # Wrong top-level type: tolerated defensively, yields nothing.
    wrong = tmp_path / "cron" / "wrong.json"
    wrong.write_text('{"not": "jobs"}')
    assert _cron_self_health_scan(wrong) == ()


def test_control_3_all_enabled_ok_zero_findings(tmp_path: Path) -> None:
    store = make_store(tmp_path / "cron" / "jobs.json")
    assert _cron_self_health_scan(store) == ()


def test_control_4_disabled_job_immediate_high(tmp_path: Path) -> None:
    store = make_store(
        tmp_path / "cron" / "jobs.json", **{SUPERVISOR_ID: {"enabled": False}}
    )
    findings = _cron_self_health_scan(store)
    assert len(findings) == 1
    assert findings[0].pattern == "cron-self-health"
    assert findings[0].key == SUPERVISOR_ID
    assert findings[0].severity == "high"
    assert findings[0].apply_kind == "none"
    # No streak needed even with a pre-existing zero/reset ledger entry.
    assert _cron_self_health_scan(store, {SUPERVISOR_ID: 0}) != ()


def test_control_5_day1_error_zero_findings(tmp_path: Path) -> None:
    # First observation of last_status=error: the self-heal window is
    # open (the 2026-09-02 supervisor incident shape) — no HIGH on day 1.
    store = make_store(
        tmp_path / "cron" / "jobs.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "gateway shutdown"}},
    )
    assert _cron_self_health_scan(store) == ()
    assert _cron_self_health_scan(store, {SUPERVISOR_ID: 0}) == ()
    # End to end through _detect_all with an empty ledger.
    config = make_config(tmp_path, store)
    assert _detect_all((), (), "", NOW, config, state={}) == ()


def test_control_6_second_consecutive_observation_high(tmp_path: Path) -> None:
    store = make_store(
        tmp_path / "cron" / "jobs.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "gateway shutdown"}},
    )
    # Night 1: recorder inserts the dormant day-1 observation.
    night1: dict = {"open_findings": []}
    record_cron_self_health_streaks(store, night1, now=NOW)
    entry = night1["open_findings"][0]
    assert entry["fingerprint"] == f"cron-self-health:{SUPERVISOR_ID}"
    assert entry["occurrence_count"] == 1
    assert entry["fix_status"] == "monitor"
    # Night 2: same error -> exactly one HIGH finding.
    findings = _cron_self_health_scan(
        store, _cron_self_health_streaks(night1)
    )
    assert len(findings) == 1
    assert findings[0].key == SUPERVISOR_ID
    assert findings[0].severity == "high"
    assert fingerprint(findings[0]) == f"cron-self-health:{SUPERVISOR_ID}"
    # The recorder must NOT double-bump the fingerprint the scan already
    # counted via the dedupe upsert this run.
    night2: dict = {"open_findings": [dict(entry)]}
    record_cron_self_health_streaks(
        store,
        night2,
        now=NOW + 86400,
        detected_fps=frozenset({f"cron-self-health:{SUPERVISOR_ID}"}),
    )
    assert night2["open_findings"][0]["occurrence_count"] == 1
    # Without detected_fps (e.g. the scan emitted nothing): the bump lands.
    record_cron_self_health_streaks(store, night2, now=NOW + 86400)
    assert night2["open_findings"][0]["occurrence_count"] == 2
    assert night2["open_findings"][0]["fix_status"] == "open"
    assert night2["open_findings"][0]["severity"] == "high"


def test_control_7_recovery_resets_streak(tmp_path: Path) -> None:
    error_store = make_store(
        tmp_path / "cron" / "jobs.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "gateway shutdown"}},
    )
    ledger: dict = {"open_findings": []}
    record_cron_self_health_streaks(error_store, ledger, now=NOW)
    record_cron_self_health_streaks(
        error_store, ledger, now=NOW + 86400
    )
    assert ledger["open_findings"][0]["occurrence_count"] == 2
    # The job recovers to ok.
    ok_store = make_store(tmp_path / "cron" / "jobs.json")
    record_cron_self_health_streaks(ok_store, ledger, now=NOW + 2 * 86400)
    entry = ledger["open_findings"][0]
    assert entry["occurrence_count"] == 0
    assert entry["fix_status"] == "monitor"
    # Phantom-escalation probe: months later it errors again — the streak
    # starts at 1 (dormant), never at the stale 2+.
    later = make_store(
        tmp_path / "cron" / "relapse.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "different failure"}},
    )
    # The scan reads the PRE-run ledger (count 0 after the reset): silent.
    assert _cron_self_health_scan(later, _cron_self_health_streaks(ledger)) == ()
    record_cron_self_health_streaks(later, ledger, now=NOW + 90 * 86400)
    assert ledger["open_findings"][0]["occurrence_count"] == 1
    assert ledger["open_findings"][0]["fix_status"] == "monitor"


def test_recorder_resets_streak_for_absent_job(tmp_path: Path) -> None:
    # A watched job id deleted from a READABLE store = no error
    # observation: the stale streak resets (phantom-escalation guard),
    # while an unreadable store would return early and freeze entries.
    store = make_store(tmp_path / "cron" / "jobs.json")
    del_jobs = [j for j in json.loads(store.read_text())["jobs"] if j["id"] != SUPERVISOR_ID]
    store.write_text(json.dumps({"jobs": del_jobs, "updated_at": "x"}))
    ledger: dict = {
        "open_findings": [
            {
                "fingerprint": f"cron-self-health:{SUPERVISOR_ID}",
                "pattern": "cron-self-health",
                "key": SUPERVISOR_ID,
                "occurrence_count": 2,
                "fix_status": "open",
            }
        ]
    }
    record_cron_self_health_streaks(store, ledger, now=NOW)
    entry = ledger["open_findings"][0]
    assert entry["occurrence_count"] == 0
    assert entry["fix_status"] == "monitor"
    assert "absent from the cron store" in entry["revalidation"]["reason"]


def test_recorder_skips_disabled_jobs_frozen_streak(tmp_path: Path) -> None:
    # While disabled the streak is frozen: the immediate HIGH stands on
    # its own and the job cannot self-heal, so neither grow nor reset.
    disabled_store = make_store(
        tmp_path / "cron" / "jobs.json", **{SUPERVISOR_ID: {"enabled": False}}
    )
    ledger: dict = {
        "open_findings": [
            {
                "fingerprint": f"cron-self-health:{SUPERVISOR_ID}",
                "pattern": "cron-self-health",
                "key": SUPERVISOR_ID,
                "occurrence_count": 2,
                "fix_status": "open",
            }
        ]
    }
    before = dict(ledger["open_findings"][0])
    record_cron_self_health_streaks(disabled_store, ledger, now=NOW)
    assert ledger["open_findings"][0] == before


def test_resolver_explicit_knob_wins(tmp_path: Path, monkeypatch) -> None:
    store = tmp_path / "explicit.json"
    config = make_config(tmp_path, store)
    monkeypatch.delenv("HERMES_HOME", raising=False)
    assert config.harness_loop.cron_jobs_path == str(store)


def test_config_round_trip(tmp_path: Path) -> None:
    config = ControllerConfig(
        instance_name="work-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
        harness_loop=HarnessLoopConfig(cron_jobs_path="/tmp/custom-jobs.json"),
    )
    path = tmp_path / "config.toml"
    write_config(path, config)
    assert 'cron_jobs_path = "/tmp/custom-jobs.json"' in path.read_text()
    assert load_config(path) == config
    # Default (empty = auto) survives the round trip too.
    default_config = ControllerConfig(
        instance_name="work-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
    )
    write_config(path, default_config, overwrite=True)
    assert 'cron_jobs_path = ""' in path.read_text()
    assert load_config(path) == default_config


def test_report_section_renders_all_six_jobs(tmp_path: Path) -> None:
    report = HarnessReport(
        story="s",
        wrong=(),
        skipped=(),
        applied=(),
        deploy_ready="none",
        right=(),
        next_action="n",
        cron_self_health=(
            f"• {SUPERVISOR_LABEL}: enabled, last_status success",
        ),
    )
    from hkrc.harness_loop import render_report

    text = render_report(report)
    assert "Cron self-health (6 watched jobs, report-only)" in text
    assert f"• {SUPERVISOR_LABEL}: enabled, last_status success" in text


def test_section_unknown_store_reports_unknown(tmp_path: Path) -> None:
    # Silence never masquerades as health: an unreadable store renders
    # every watched job as unknown.
    lines = _cron_self_health_section(tmp_path / "missing.json")
    assert len(lines) == 6
    assert all("unknown" in line for line in lines)


def test_run_dry_run_never_persists_streak(tmp_path: Path) -> None:
    store = make_store(
        tmp_path / "cron" / "jobs.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "gateway shutdown"}},
    )
    config = make_config(tmp_path, store)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    run(config, now=NOW, dry_run=True, state_path=state_file)
    persisted = load_state(state_file)
    assert not [
        e
        for e in persisted.get("open_findings", [])
        if e.get("pattern") == "cron-self-health"
    ]


def test_run_live_persists_then_escalates_next_night(tmp_path: Path) -> None:
    store = make_store(
        tmp_path / "cron" / "jobs.json",
        **{SUPERVISOR_ID: {"last_status": "error", "last_error": "gateway shutdown"}},
    )
    config = make_config(tmp_path, store)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    # Night 1 (live): streak recorded, report has NO cron finding.
    report1 = run(config, now=NOW, dry_run=False, state_path=state_file)
    night1 = load_state(state_file)
    entries = [
        e
        for e in night1.get("open_findings", [])
        if e.get("pattern") == "cron-self-health"
    ]
    assert len(entries) == 1
    assert entries[0]["occurrence_count"] == 1
    assert entries[0]["fix_status"] == "monitor"
    assert "Daemon intervention regression" not in report1  # sanity: sections render
    assert "Cron self-health (6 watched jobs, report-only)" in report1
    assert SUPERVISOR_LABEL in report1
    # Night 2 (live): same error -> exactly one HIGH in "What's wrong".
    report2 = run(config, now=NOW + 86400, dry_run=False, state_path=state_file)
    night2 = load_state(state_file)
    entries2 = [
        e
        for e in night2.get("open_findings", [])
        if e.get("pattern") == "cron-self-health"
    ]
    assert len(entries2) == 1
    assert entries2[0]["occurrence_count"] == 2
    assert entries2[0]["fix_status"] == "open"
    assert "Cron self-health (watched job disabled or stuck in error)" in report2
    assert "HIGH" in report2
    assert (
        f"• {SUPERVISOR_LABEL}: error, 2nd consecutive observation -> HIGH"
        in report2
    )
    # Night 3 (live): job recovered -> the streak resets to 0 in the ledger.
    make_store(store)
    run(config, now=NOW + 2 * 86400, dry_run=False, state_path=state_file)
    night3 = load_state(state_file)
    entries3 = [
        e
        for e in night3.get("open_findings", [])
        if e.get("pattern") == "cron-self-health"
    ]
    assert len(entries3) == 1
    assert entries3[0]["occurrence_count"] == 0
    assert entries3[0]["fix_status"] == "monitor"
