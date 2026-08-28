"""Manifest-driven Hermes cron reconciliation (``hkrc crons sync``).

The repository ships ``config/hkrc/cron_manifest.json`` declaring the expected
cron jobs for an instance.  This module reads the manifest, reads the live
cron store for the target Hermes profile (read-only), diffs the two, and
mutates the store exclusively through the ``hermes cron`` CLI surface
(``create`` / ``resume`` / ``edit``) — it never writes ``jobs.json`` itself.

Reconciliation rules (deterministic and idempotent):

- A manifest job with no live match is created.
- A manifest job whose live match is paused (``enabled`` false) is resumed.
- A manifest job whose live match differs on schedule / no_agent / script /
  delivery / skills is edited to the manifest state.  Prompt text is not
  compared (it is descriptive; the manifest only seeds it at create time).
- Live jobs not named in the manifest are never touched.

The sync prints one line per planned action and is silent when in sync.
``--dry-run`` reports the plan without mutating anything.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess

from .config import ControllerConfig


CRON_MANIFEST_FILENAME = "cron_manifest.json"


class CronManifestError(RuntimeError):
    """Raised when the manifest or the live cron store is malformed."""


@dataclass(frozen=True)
class ManifestJob:
    """One expected cron job declared by the manifest."""

    name: str
    schedule: str
    no_agent: bool
    deliver: str
    script: str | None = None
    prompt: str | None = None
    skills: tuple[str, ...] = ()


@dataclass(frozen=True)
class SyncAction:
    """One planned reconciliation step (create / resume / update)."""

    kind: str
    job: ManifestJob
    job_id: str | None = None
    changes: tuple[str, ...] = ()

    def diff_line(self) -> str:
        if self.kind == "create":
            return f"crons sync: create {self.job.name!r}"
        if self.kind == "resume":
            return f"crons sync: resume {self.job_id} {self.job.name!r}"
        detail = ""
        if self.changes:
            detail = " (" + ", ".join(self.changes) + ")"
        return f"crons sync: update {self.job_id} {self.job.name!r}{detail}"


def default_manifest_path(config_path: Path) -> Path:
    """Manifest lives next to the instance config file."""
    return Path(config_path).expanduser().parent / CRON_MANIFEST_FILENAME


def load_manifest(path: Path) -> list[ManifestJob]:
    """Parse and validate the cron manifest JSON file."""
    path = Path(path).expanduser()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CronManifestError(f"cron manifest not found: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CronManifestError(f"cannot read cron manifest {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise CronManifestError(f"cron manifest {path} must be a JSON object")
    entries = raw.get("jobs")
    if not isinstance(entries, list):
        raise CronManifestError(f"cron manifest {path} is missing a 'jobs' list")
    jobs: list[ManifestJob] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise CronManifestError(f"cron manifest {path}: jobs[{index}] is not an object")
        name = str(entry.get("name") or "").strip()
        schedule = str(entry.get("schedule") or "").strip()
        if not name:
            raise CronManifestError(f"cron manifest {path}: jobs[{index}] is missing 'name'")
        if not schedule:
            raise CronManifestError(f"cron manifest {path}: jobs[{index}] ({name}) is missing 'schedule'")
        script = entry.get("script")
        prompt = entry.get("prompt")
        skills_raw = entry.get("skills") or []
        if not isinstance(skills_raw, list) or not all(isinstance(s, str) for s in skills_raw):
            raise CronManifestError(f"cron manifest {path}: jobs[{index}] ({name}) 'skills' must be a list of strings")
        jobs.append(
            ManifestJob(
                name=name,
                schedule=schedule,
                no_agent=bool(entry.get("no_agent", False)),
                deliver=str(entry.get("deliver") or "local").strip(),
                script=str(script).strip() if script else None,
                prompt=str(prompt).strip() if prompt else None,
                skills=tuple(skills_raw),
            )
        )
    return jobs


def hermes_root() -> Path:
    """Default Hermes root, mirroring ``get_default_hermes_root()``.

    ``~/.hermes`` in standard deployments; ``HERMES_HOME`` itself in Docker /
    custom deployments; the profiles parent when ``HERMES_HOME`` points at a
    named profile (``<root>/profiles/<name>``).
    """

    native_home = Path.home() / ".hermes"
    env_home = os.environ.get("HERMES_HOME", "").strip()
    if not env_home:
        return native_home
    env_path = Path(env_home).expanduser()
    try:
        env_path.resolve().relative_to(native_home.resolve())
        return native_home
    except ValueError:
        pass
    if env_path.parent.name == "profiles":
        return env_path.parent.parent
    return env_path


def profile_home(profile_name: str) -> Path:
    """Profile home path, mirroring ``resolve_profile_env()`` for the standard layout."""
    if profile_name in ("", "default"):
        return hermes_root()
    return hermes_root() / "profiles" / profile_name


def resolve_cron_store_path(config: ControllerConfig) -> Path:
    """Resolve the live ``jobs.json`` path the ``hermes cron`` CLI would use.

    Mirrors the CLI's resolution chain (``hermes_cli/main.py``
    ``_apply_profile_override``): an explicit native profile wins, then an
    inherited ``HERMES_HOME`` whose parent directory is ``profiles``, then the
    sticky ``active_profile`` marker, then the default home.
    """

    if config.native_profile:
        return profile_home(config.native_profile) / "cron" / "jobs.json"
    env_home = os.environ.get("HERMES_HOME", "").strip()
    if env_home and Path(env_home).expanduser().parent.name == "profiles":
        return Path(env_home).expanduser() / "cron" / "jobs.json"
    active = hermes_root() / "active_profile"
    try:
        if active.is_file():
            name = active.read_text(encoding="utf-8").strip()
            if name and name != "default":
                return profile_home(name) / "cron" / "jobs.json"
    except OSError:
        pass
    return hermes_root() / "cron" / "jobs.json"


def read_live_jobs(store_path: Path) -> list[dict]:
    """Read live cron jobs from the store without writing anything.

    Mirrors ``cron.jobs.load_jobs()`` shape handling: a ``{"jobs": [...]}``
    dict or a bare list; a missing file is an empty store.
    """

    store_path = Path(store_path)
    if not store_path.is_file():
        return []
    try:
        data = json.loads(store_path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CronManifestError(f"cannot read cron store {store_path}: {exc}") from exc
    if isinstance(data, dict):
        jobs = data.get("jobs", [])
    elif isinstance(data, list):
        jobs = data
    else:
        raise CronManifestError(f"cron store {store_path}: expected a jobs list, got {type(data).__name__}")
    if not isinstance(jobs, list):
        raise CronManifestError(f"cron store {store_path}: 'jobs' is not a list")
    return [job for job in jobs if isinstance(job, dict)]


def _live_schedule_display(job: dict) -> str:
    value = job.get("schedule_display")
    if value:
        return str(value)
    schedule = job.get("schedule")
    if isinstance(schedule, dict):
        return str(schedule.get("display") or schedule.get("value") or schedule.get("expr") or "")
    return str(schedule or "")


def _live_deliver(job: dict) -> str:
    deliver = job.get("deliver")
    if isinstance(deliver, list):
        return ",".join(str(item) for item in deliver)
    return str(deliver or "local")


def _live_skills(job: dict) -> list[str]:
    skills = job.get("skills")
    if isinstance(skills, list):
        return [str(skill) for skill in skills]
    if job.get("skill"):
        return [str(job["skill"])]
    return []


def plan_sync(manifest: Sequence[ManifestJob], live_jobs: Sequence[dict]) -> list[SyncAction]:
    """Diff manifest expectations against live jobs; never plans for unlisted jobs."""

    by_name: dict[str, list[dict]] = {}
    for job in live_jobs:
        name = str(job.get("name") or "").strip()
        by_name.setdefault(name, []).append(job)

    actions: list[SyncAction] = []
    for expected in manifest:
        matches = by_name.get(expected.name, [])
        if not matches:
            actions.append(SyncAction("create", expected))
            continue
        if len(matches) > 1:
            ids = ", ".join(str(job.get("id")) for job in matches)
            raise CronManifestError(
                f"multiple live cron jobs are named {expected.name!r} ({ids}); "
                "deduplicate them before syncing"
            )
        live = matches[0]
        if not live.get("enabled", True):
            actions.append(SyncAction("resume", expected, job_id=live.get("id")))
            continue
        changes: list[str] = []
        if _live_schedule_display(live) != expected.schedule:
            changes.append(
                f"schedule {_live_schedule_display(live)!r} -> {expected.schedule!r}"
            )
        if bool(live.get("no_agent")) != expected.no_agent:
            changes.append(f"no_agent {bool(live.get('no_agent'))} -> {expected.no_agent}")
        if (live.get("script") or None) != expected.script:
            changes.append(f"script {live.get('script')!r} -> {expected.script!r}")
        if _live_deliver(live) != expected.deliver:
            changes.append(f"deliver {_live_deliver(live)!r} -> {expected.deliver!r}")
        if _live_skills(live) != list(expected.skills):
            changes.append(f"skills {_live_skills(live)!r} -> {list(expected.skills)!r}")
        if changes:
            actions.append(SyncAction("update", expected, job_id=live.get("id"), changes=tuple(changes)))
    return actions


def _cron_command(config: ControllerConfig) -> list[str]:
    command = [config.native_cli]
    if config.native_profile:
        command.extend(["--profile", config.native_profile])
    return command + ["cron"]


def _create_args(job: ManifestJob) -> list[str]:
    args: list[str] = [job.schedule]
    if job.prompt:
        args.append(job.prompt)
    args.extend(["--name", job.name, "--deliver", job.deliver])
    if job.no_agent:
        args.append("--no-agent")
        if job.script:
            args.extend(["--script", job.script])
    for skill in job.skills:
        args.extend(["--skill", skill])
    return args


def _edit_args(job: ManifestJob, live: dict) -> list[str]:
    args = ["--schedule", job.schedule, "--deliver", job.deliver]
    args.append("--no-agent" if job.no_agent else "--agent")
    if job.script is not None:
        args.extend(["--script", job.script])
    elif live.get("script"):
        args.extend(["--script", ""])
    if job.skills:
        for skill in job.skills:
            args.extend(["--skill", skill])
    elif _live_skills(live):
        args.append("--clear-skills")
    return args


def _action_command(action: SyncAction, config: ControllerConfig, live: dict) -> list[str]:
    base = _cron_command(config)
    if action.kind == "create":
        return base + ["create"] + _create_args(action.job)
    if action.kind == "resume":
        return base + ["resume", action.job_id or ""]
    return base + ["edit", action.job_id or ""] + _edit_args(action.job, live)


def run_sync(
    config: ControllerConfig,
    manifest_path: Path,
    *,
    dry_run: bool = False,
    runner: Callable[[list[str]], int] | None = None,
) -> list[SyncAction]:
    """Reconcile live cron jobs with the manifest.

    Prints one ``crons sync: ...`` line per planned action and is silent when
    in sync.  With ``dry_run=True`` nothing is mutated.  ``runner`` exists for
    deterministic tests and defaults to ``subprocess.run``.
    """

    manifest = load_manifest(manifest_path)
    live_jobs = read_live_jobs(resolve_cron_store_path(config))
    by_id = {str(job.get("id")): job for job in live_jobs}
    actions = plan_sync(manifest, live_jobs)
    for action in actions:
        print(action.diff_line())
    if dry_run:
        return actions
    for action in actions:
        command = _action_command(action, config, by_id.get(action.job_id or "", {}))
        if runner is None:
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False, timeout=120
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "").strip()
                raise CronManifestError(
                    f"hermes cron {action.kind} failed for {action.job.name!r}: "
                    f"{detail or 'exit code ' + str(completed.returncode)}"
                )
        else:
            exit_code = runner(list(command))
            if exit_code != 0:
                raise CronManifestError(
                    f"hermes cron {action.kind} failed for {action.job.name!r} "
                    f"(exit code {exit_code})"
                )
    return actions
