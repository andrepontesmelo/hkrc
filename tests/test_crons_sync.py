"""Contract tests for manifest-driven cron reconciliation (`hkrc crons sync`).

The live-cron integration tests are hermetic: they point ``HOME`` at a temp
dir and unset ``HERMES_HOME`` so the real ``hermes cron`` CLI (and the sync's
read path) resolve to the temp profile store — never the operator's store.
"""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest

from hkrc.config import ControllerConfig
from hkrc.crons import (
    CronManifestError,
    ManifestJob,
    default_manifest_path,
    load_manifest,
    plan_sync,
    read_live_jobs,
    resolve_cron_store_path,
    run_sync,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config" / "hkrc" / "cron_manifest.json"

EXPECTED_NAMES = {
    "kanban needs input watcher",
    "kanban stale block watch",
    "kanban review gap watchdog",
    "hkrc archloop nightly",
    "harness-learning-loop (daily 7-day self-review)",
    "HKRC harness supervisor",
}


def manifest_jobs() -> list[ManifestJob]:
    return load_manifest(MANIFEST)


def job(
    job_id: str,
    name: str,
    *,
    schedule: str = "every 5m",
    no_agent: bool = True,
    script: str | None = "needs-input-watcher.py",
    deliver: str = "local",
    skills: list[str] | None = None,
    enabled: bool = True,
) -> dict:
    return {
        "id": job_id,
        "name": name,
        "schedule_display": schedule,
        "schedule": {"kind": "interval", "minutes": 5, "display": schedule},
        "no_agent": no_agent,
        "script": script,
        "deliver": deliver,
        "skills": skills or [],
        "skill": (skills or [None])[0],
        "enabled": enabled,
        "state": "scheduled" if enabled else "paused",
    }


def isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the hermes cron store at a throwaway profile home.

    Also scrubs cron-context session vars (HERMES_CRON_SESSION,
    HERMES_CRON_AUTO_DELIVER_*, HERMES_SESSION_*) so the real CLI subprocess
    sees the same environment in any caller context — otherwise a supervisor
    session's auto-deliver context would resolve a literal ``--deliver origin``
    into ``platform:chat_id`` at create time and break idempotence.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_HOME", raising=False)
    for var in (
        "HERMES_CRON_SESSION",
        "HERMES_CRON_AUTO_DELIVER_PLATFORM",
        "HERMES_CRON_AUTO_DELIVER_CHAT_ID",
        "HERMES_CRON_AUTO_DELIVER_THREAD_ID",
        "HERMES_SESSION_PLATFORM",
        "HERMES_SESSION_CHAT_ID",
        "HERMES_SESSION_THREAD_ID",
    ):
        monkeypatch.delenv(var, raising=False)


def store_path(tmp_path: Path) -> Path:
    return tmp_path / ".hermes" / "cron" / "jobs.json"


def write_store(tmp_path: Path, jobs: list[dict]) -> None:
    path = store_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"jobs": jobs, "updated_at": "2026-08-05T00:00:00-07:00"}, indent=2),
        encoding="utf-8",
    )


def read_store(tmp_path: Path) -> list[dict]:
    data = json.loads(store_path(tmp_path).read_text(encoding="utf-8"))
    return data["jobs"] if isinstance(data, dict) else data


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def test_shipped_manifest_declares_all_expected_jobs() -> None:
    # Schedule times are intentionally NOT asserted: Andre moves cron
    # schedules frequently and the manifest is the single source of truth.
    jobs = manifest_jobs()
    assert {job.name for job in jobs} == EXPECTED_NAMES
    by_name = {job.name: job for job in jobs}

    blocker = by_name["kanban needs input watcher"]
    assert blocker.no_agent and blocker.script == "needs-input-watcher.py"
    assert blocker.deliver == "local"

    review_gap = by_name["kanban review gap watchdog"]
    assert review_gap.no_agent and review_gap.script == "review-gap.py"
    assert review_gap.deliver == "local"

    archloop = by_name["hkrc archloop nightly"]
    assert archloop.no_agent and archloop.script == "archloop-night-cron.sh"
    assert archloop.deliver == "local"

    harness = by_name["harness-learning-loop (daily 7-day self-review)"]
    assert harness.no_agent and harness.script == "harness-loop.py"
    assert harness.deliver == "local"
    assert harness.skills == ()
    assert harness.prompt is None


def test_shipped_manifest_manages_harness_supervisor() -> None:
    """The supervisor of the supervisors must be manifest-managed: a pause
    wave that hits it has to be repairable by `hkrc crons sync` alone (it
    went dark for 14 days in 2026-08 precisely because it was not listed)."""
    supervisor = {j.name: j for j in manifest_jobs()}["HKRC harness supervisor"]
    assert supervisor.no_agent is False
    assert supervisor.script is None
    assert supervisor.deliver == "origin"
    assert supervisor.skills == (
        "kanban-operations",
        "cron-automation",
        "local-deployment",
        "tts-with-me",
        "hermes-agent",
        "diagnosing-bugs",
    )
    # Prompt is seeded at create time only and never diffed; a from-scratch
    # recreate must yield a working job, so the manifest has to carry one.
    assert supervisor.prompt is not None
    assert "supervisor-mission.md" in supervisor.prompt
    # DEF-001: cron agents run with a profile-scoped $HOME, so a tilde
    # mission path expands to a nonexistent file and the prompt's own
    # failure branch fires on every tick. The path must be absolute.
    assert "~/.hermes" not in supervisor.prompt
    assert (
        "/home/example-user/.hermes/hkrc/config/hkrc/supervisor-mission.md"
        in supervisor.prompt
    )
    # DEF-002: the mission file is the source of truth and says 05:00
    # (schedule moved from noon on 2026-08-15); stale wording must not
    # re-enter via the manifest.
    assert "05:00" in supervisor.prompt
    assert "noon" not in supervisor.prompt


def test_plan_is_silent_for_converged_harness_supervisor() -> None:
    """The manifest entry must match the live supervisor job field-for-field
    on everything plan_sync diffs, or the next sync rewrites a healthy job.
    The schedule derives from the manifest (times are never pinned by tests);
    the remaining fields mirror the live record verbatim. The other manifest
    jobs are present and converged, isolating the supervisor assertion."""
    manifest = manifest_jobs()
    supervisor = {j.name: j for j in manifest}["HKRC harness supervisor"]
    live = [
        job(
            "1369f0027b78",
            "HKRC harness supervisor",
            schedule=supervisor.schedule,
            no_agent=False,
            script=None,
            deliver="origin",
            skills=list(supervisor.skills),
        )
    ]
    live += [
        job(
            f"conv{i}",
            j.name,
            schedule=j.schedule,
            no_agent=j.no_agent,
            script=j.script,
            deliver=j.deliver,
            skills=list(j.skills),
        )
        for i, j in enumerate(manifest)
        if j.name != "HKRC harness supervisor"
    ]
    assert plan_sync(manifest, live) == []


def test_manifest_rejects_missing_name_or_schedule(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"jobs": [{"schedule": "every 5m"}]}), encoding="utf-8"
    )
    with pytest.raises(CronManifestError, match="name"):
        load_manifest(manifest)
    manifest.write_text(
        json.dumps({"jobs": [{"name": "no schedule"}]}), encoding="utf-8"
    )
    with pytest.raises(CronManifestError, match="schedule"):
        load_manifest(manifest)


def test_default_manifest_path_sits_next_to_config() -> None:
    config = ROOT / "config" / "hkrc" / "config.toml"
    assert default_manifest_path(config) == config.parent / "cron_manifest.json"


# ---------------------------------------------------------------------------
# Store path resolution mirrors the hermes CLI
# ---------------------------------------------------------------------------


def test_store_path_prefers_native_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    isolated_env(tmp_path, monkeypatch)
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3", native_profile="main")
    assert resolve_cron_store_path(config) == Path.home() / ".hermes" / "profiles" / "main" / "cron" / "jobs.json"


def test_store_path_native_profile_wins_over_profile_homed_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The kanban-worker session env: HERMES_HOME points at one named profile
    # while the config targets another. The CLI (`hermes --profile main`)
    # resolves the profile against the profiles ROOT, not the env home.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes" / "profiles" / "developer"))
    monkeypatch.setenv("HOME", str(tmp_path / "profile-home"))
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3", native_profile="main")
    assert resolve_cron_store_path(config) == tmp_path / ".hermes" / "profiles" / "main" / "cron" / "jobs.json"


def test_store_path_docker_home_hosts_profiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HOME", raising=False)
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3", native_profile="coder")
    assert resolve_cron_store_path(config) == tmp_path / "data" / "profiles" / "coder" / "cron" / "jobs.json"


def test_store_path_honors_hermes_home_when_parent_is_profiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes" / "profiles" / "coder"))
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3")
    assert resolve_cron_store_path(config) == tmp_path / ".hermes" / "profiles" / "coder" / "cron" / "jobs.json"


def test_store_path_falls_back_to_default_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    isolated_env(tmp_path, monkeypatch)
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3")
    assert resolve_cron_store_path(config) == tmp_path / ".hermes" / "cron" / "jobs.json"


# ---------------------------------------------------------------------------
# Planning (unit-level)
# ---------------------------------------------------------------------------


def test_plan_empty_store_creates_every_manifest_job() -> None:
    actions = plan_sync(manifest_jobs(), [])
    assert [action.kind for action in actions] == ["create"] * 6
    assert {action.job.name for action in actions} == EXPECTED_NAMES


def test_plan_is_silent_when_live_matches_manifest() -> None:
    # Schedules are read from the manifest so this stays green when Andre
    # moves cron times; only the shape of the jobs is exercised here.
    manifest = {j.name: j for j in manifest_jobs()}
    live = [
        job("a", "kanban needs input watcher", schedule=manifest["kanban needs input watcher"].schedule),
        job("b", "kanban review gap watchdog", schedule=manifest["kanban review gap watchdog"].schedule, script="review-gap.py"),
    ]
    stale = job("e", "kanban stale block watch", schedule=manifest["kanban stale block watch"].schedule, script="stale-block-watch.py")
    harness = job("c", "harness-learning-loop (daily 7-day self-review)", schedule=manifest["harness-learning-loop (daily 7-day self-review)"].schedule, no_agent=True, script="harness-loop.py")
    archloop = job("d", "hkrc archloop nightly", schedule=manifest["hkrc archloop nightly"].schedule, script="archloop-night-cron.sh")
    supervisor = job(
        "f",
        "HKRC harness supervisor",
        schedule=manifest["HKRC harness supervisor"].schedule,
        no_agent=False,
        script=None,
        deliver="origin",
        skills=["kanban-operations", "cron-automation", "local-deployment", "tts-with-me", "hermes-agent", "diagnosing-bugs"],
    )
    assert plan_sync(manifest_jobs(), [live[0], live[1], stale, harness, archloop, supervisor]) == []


def test_plan_resumes_paused_job() -> None:
    paused = job("paused1", "kanban needs input watcher", enabled=False)
    actions = plan_sync(manifest_jobs(), [paused])
    assert len(actions) == 6
    resume = next(action for action in actions if action.kind == "resume")
    assert resume.job_id == "paused1"
    assert resume.job.name == "kanban needs input watcher"


def test_plan_updates_stale_fields() -> None:
    stale = job("stale1", "kanban needs input watcher", schedule="every 7m", deliver="telegram", script="old.py")
    actions = plan_sync(manifest_jobs(), [stale])
    update = next(action for action in actions if action.kind == "update")
    assert update.job_id == "stale1"
    joined = " ".join(update.changes)
    assert "every 7m" in joined and "every 5m" in joined
    assert "old.py" in joined
    assert "local" in joined


def test_plan_flips_harness_job_to_no_agent_shim() -> None:
    """The production switch: a live harness job still in the pre-switch LLM
    state (no_agent false, no script, self-review skill) must be planned as a
    single update to the no_agent shim form (script + cleared skills)."""
    live = job(
        "f69651252ba1",
        "harness-learning-loop (daily 7-day self-review)",
        no_agent=False,
        script=None,
        skills=["self-review"],
    )
    actions = plan_sync(manifest_jobs(), [live])
    # 4 creates (other manifest jobs absent from the store) + 1 update (the flip).
    updates = [action for action in actions if action.kind == "update"]
    assert len(updates) == 1
    update = updates[0]
    assert update.job_id == "f69651252ba1"
    joined = " ".join(update.changes)
    assert "no_agent" in joined
    assert "harness-loop.py" in joined
    assert "self-review" in joined  # skills drift -> cleared


def test_plan_never_touches_unlisted_jobs() -> None:
    live = [
        job("x", "Monthly session prune (90d)", schedule="0 4 1 * *", no_agent=False, script=None),
        job("y", "butzenlake pre-window watchdog", schedule="45 6 * * *", script="butzenlake-watchdog.sh"),
    ]
    actions = plan_sync(manifest_jobs(), live)
    assert all(action.job.name in EXPECTED_NAMES for action in actions)
    assert len(actions) == 6  # all manifest jobs missing -> creates only


def test_plan_raises_on_ambiguous_name_match() -> None:
    live = [job("dup1", "kanban needs input watcher"), job("dup2", "kanban needs input watcher")]
    with pytest.raises(CronManifestError, match="multiple live cron jobs"):
        plan_sync(manifest_jobs(), live)


def test_read_live_jobs_handles_missing_file_and_bare_list(tmp_path: Path) -> None:
    assert read_live_jobs(tmp_path / "nope" / "jobs.json") == []
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps([job("a", "alpha")]), encoding="utf-8")
    assert [j["id"] for j in read_live_jobs(path)] == ["a"]


# ---------------------------------------------------------------------------
# Dry-run and mutation via the real hermes cron CLI (isolated HOME)
# ---------------------------------------------------------------------------


def test_dry_run_reports_without_writing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    isolated_env(tmp_path, monkeypatch)
    write_store(tmp_path, [job("stale1", "kanban needs input watcher", schedule="every 7m")])
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3")
    before = store_path(tmp_path).read_bytes()

    actions = run_sync(config, MANIFEST, dry_run=True)

    out = capsys.readouterr().out
    assert "create" in out and "update" in out
    assert store_path(tmp_path).read_bytes() == before  # nothing mutated
    assert len(actions) == 6  # 1 update + 5 creates


def test_run_sync_end_to_end_and_idempotence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    hermes = shutil.which("hermes")
    if hermes is None:
        pytest.skip("hermes CLI not on PATH")
    isolated_env(tmp_path, monkeypatch)
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3", native_cli=hermes)

    # Fresh run: empty store -> all six manifest jobs created.
    actions = run_sync(config, MANIFEST)
    assert [action.kind for action in actions] == ["create"] * 6
    live = read_store(tmp_path)
    assert {job["name"] for job in live} == EXPECTED_NAMES
    blocker = next(job for job in live if job["name"] == "kanban needs input watcher")
    assert blocker["enabled"] is True
    assert blocker["deliver"] == "local"
    harness = next(job for job in live if job["name"].startswith("harness-learning-loop"))
    assert harness["no_agent"] is True
    assert harness["script"] == "harness-loop.py"
    assert not harness.get("skills")
    # Shipped schedule is applied as-is (time itself is not pinned by tests).
    manifest_harness = next(j for j in manifest_jobs() if j.name.startswith("harness-learning-loop"))
    assert harness["schedule_display"] == manifest_harness.schedule
    # Regression: `--deliver origin` must be stored verbatim. Without the
    # sync's scrubbed subprocess env, a cron-context caller resolves it to the
    # auto-deliver target at create time (telegram:-1004433470689), so every
    # later sync plans a flip-flopping update.
    supervisor = next(job for job in live if job["name"] == "HKRC harness supervisor")
    assert supervisor["deliver"] == "origin"

    # Re-run: silent, zero diff.
    capsys.readouterr()
    assert run_sync(config, MANIFEST) == []
    assert capsys.readouterr().out == ""

    # Pause one job -> sync resumes it.
    paused_id = blocker["id"]
    subprocess.run([hermes, "cron", "pause", paused_id], check=True, capture_output=True, text=True)
    actions = run_sync(config, MANIFEST)
    assert [(a.kind, a.job.name) for a in actions] == [("resume", "kanban needs input watcher")]

    # Drift the (now enabled) job -> sync edits it back.
    subprocess.run([hermes, "cron", "edit", paused_id, "--schedule", "every 7m"], check=True, capture_output=True, text=True)
    actions = run_sync(config, MANIFEST)
    assert [(a.kind, a.job.name) for a in actions] == [("update", "kanban needs input watcher")]
    assert any("every 7m" in change for change in actions[0].changes)

    # A non-manifest job is never touched by a later sync.
    subprocess.run(
        [hermes, "cron", "create", "45 6 * * *", "--no-agent", "--script", "butzenlake-watchdog.sh", "--deliver", "telegram", "--name", "butzenlake pre-window watchdog"],
        check=True, capture_output=True, text=True,
    )
    capsys.readouterr()
    run_sync(config, MANIFEST)
    live = read_store(tmp_path)
    assert any(job["name"] == "butzenlake pre-window watchdog" for job in live)
    assert {job["name"] for job in live} == EXPECTED_NAMES | {"butzenlake pre-window watchdog"}


def test_runner_receives_exact_cli_commands(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    isolated_env(tmp_path, monkeypatch)
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3", native_profile="main")
    paused = job("paused1", "kanban needs input watcher", enabled=False)
    profile_store = Path.home() / ".hermes" / "profiles" / "main" / "cron" / "jobs.json"
    profile_store.parent.mkdir(parents=True, exist_ok=True)
    profile_store.write_text(
        json.dumps({"jobs": [paused], "updated_at": "2026-08-05T00:00:00-07:00"}),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def runner(command: list[str]) -> int:
        calls.append(list(command))
        return 0

    run_sync(config, MANIFEST, runner=runner)

    assert calls[0] == [
        "hermes", "--profile", "main", "cron", "resume", "paused1",
    ]
    create = next(call for call in calls if call[4] == "create")
    assert create[:4] == ["hermes", "--profile", "main", "cron"]
    assert create[5] == "every 10m"
    assert "--name" in create and "--deliver" in create and "--no-agent" in create
