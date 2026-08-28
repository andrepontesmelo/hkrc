"""Silent death-block watchdog (``gave_up`` / spawn-failure class) — v1.

The recovery controller and the stream daemon classify blockers from events:
a task that reaches ``status='blocked'`` via a dispatcher death event
(``gave_up`` / ``spawn_failed`` / ``timed_out`` / ``crashed``) with NO
``blocked``-kind event and NO typed block kind is invisible to every
event-driven watchdog (verified against a live board on 2026-08-06: a
task sat blocked 02:04-09:00 with ``elapsed 61s > limit 0s`` runs
and zero ``blocked`` events; the WS stream never delivered the events and
``blocker-ping``/``needs-input-watcher`` only key on ``needs_input``).

This module is the complement:

- ``discover_silent_death_blocks`` reads ALL non-archived boards through the
  native CLI only (``boards list --json``, ``list --status blocked --json``,
  ``show <id> --json`` — the 2026-08-04 Andre rule: shipped kanban automation
  never opens native sqlite), and returns blocked tasks whose latest event is
  a death kind with no typed block kind, plus the exact death signature.
- The config-defect signature ``limit_seconds: 0`` / ``elapsed Ns > limit 0s``
  (a task created with ``--max-runtime 0``) is flagged so the operator gets
  the precise fix (``max_runtime_seconds`` to NULL + breaker reset) instead of
  a re-derivation loop. Verified 2026-08-06: three gate cards
  on one board shared this signature.
- ``render_fresh`` dedupes per episode via a controller-owned state file keyed
  ``board:task_id:<latest_event_id>``; the cron ``no_agent`` contract stays
  silent when nothing is fresh and prints a digest otherwise.

Intended surface: ``hkrc stale-block-watch`` (CLI) and the cron shim
``stale-block-watch.py`` installed next to the other instance shims.

The daemon's reconcile sweep (``runtime.py``) imports the same detection
predicate (``is_silent_death_block``) so state-based reconciliation and this
digest agree on what counts as the silent class.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import json
from pathlib import Path
import re

from .config import ControllerConfig
from .review_gap import (
    CliRunner,
    NativeTimeoutError,
    ReviewGapError,
    discover_boards,
    list_blocked_tasks,
    show_task,
)

STATE_FILENAME = "stale-block-watch-state.json"

# Dispatcher death kinds that leave status='blocked' WITHOUT a 'blocked' event
# (the native spawn-failure block class, verified 2026-08-03 t_e0ab2f49 and
# 2026-08-06 t_8d170db9/t_60450e0d).
DEATH_KINDS = frozenset({"gave_up", "spawn_failed", "timed_out", "crashed"})

# Config-defect signature: task created with --max-runtime 0 → every run is
# SIGTERM'd at ~60s ("elapsed 61s > limit 0s", limit_seconds: 0). The task is
# blocked, but the blocker is the create-time config, not the work.
_LIMIT_ZERO_RE = re.compile(r"limit\s*0s|limit_seconds.{0,4}0\b|elapsed \d+s > limit 0s")


class StaleBlockWatchError(RuntimeError):
    """Raised when a native board cannot be inspected safely through the CLI."""


@dataclass(frozen=True, slots=True)
class DeathBlockCandidate:
    """One silent death block: ``status='blocked'`` with a death-kind latest event.

    ``latest_event_id`` anchors the episode dedupe key (the death event that
    opened the episode); unrelated later events cannot re-trigger a ping.
    ``config_defect`` is True when the death signature matches the
    ``--max-runtime 0`` class, carrying the exact operator fix.
    """

    board_slug: str
    task_id: str
    title: str
    latest_event_kind: str
    latest_event_id: int
    latest_run_error: str
    config_defect: bool

    @property
    def key(self) -> str:
        return f"{self.board_slug}:{self.task_id}:{self.latest_event_id}"


def default_state_path(state_db: Path) -> Path:
    """Controller-owned state file next to the controller state database."""
    return state_db.parent / STATE_FILENAME


def _load_state(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StaleBlockWatchError(
            f"cannot read stale-block-watch state {path}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise StaleBlockWatchError(
            f"stale-block-watch state must be an object: {path}"
        )
    state: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(value, int):
            raise StaleBlockWatchError(
                f"stale-block-watch state values must be integers: {path}"
            )
        state[str(key)] = value
    return state


def _save_state(path: Path, state: dict[str, int]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(state, sort_keys=True, indent=1) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    except OSError as exc:
        raise StaleBlockWatchError(
            f"cannot persist stale-block-watch state {path}: {exc}"
        ) from exc


def is_config_defect(run_error: str | None) -> bool:
    """True when a run error carries the ``--max-runtime 0`` signature."""
    return bool(run_error) and _LIMIT_ZERO_RE.search(run_error) is not None


def is_silent_death_block(
    *,
    status: str,
    latest_event_kind: str | None,
    run_error: str | None,
) -> bool:
    """Shared detection predicate for the silent death-block class.

    ``status='blocked'`` AND the latest event is a death kind.  No typed
    ``blocked`` event is required — that absence IS the class (a task blocked
    via ``gave_up``/``spawn_failed``/``timed_out``/``crashed`` has no
    ``blocked``-kind event by construction).  Tasks whose latest event is a
    real ``blocked`` event (typed needs_input/capability/...) are NOT this
    class; needs-input-watcher and the classifier own those.
    """
    if status != "blocked":
        return False
    return latest_event_kind in DEATH_KINDS


def _latest_event_kind(show: dict) -> str | None:
    """Latest event kind from a ``show --json`` document (events reversed)."""
    events = show.get("events")
    if not isinstance(events, list):
        return None
    for raw_event in reversed(events):
        if not isinstance(raw_event, dict):
            continue
        kind = raw_event.get("kind")
        if isinstance(kind, str) and kind:
            return kind
    return None


def _latest_event_id(show: dict) -> int | None:
    """Latest event anchor: ``id`` when present, else ``created_at``.

    Old-schema events carry NO ``id`` field at all (verified 2026-08-06 on the
    default board: t_1a9669a8's events are ``{kind, payload, created_at,
    run_id}``); ``created_at`` is the stable insertion-order anchor for those
    rows and keeps the dedupe key deterministic.
    """
    events = show.get("events")
    if not isinstance(events, list):
        return None
    for raw_event in reversed(events):
        if not isinstance(raw_event, dict):
            continue
        event_id = raw_event.get("id")
        if isinstance(event_id, int) and not isinstance(event_id, bool):
            return event_id
        created_at = raw_event.get("created_at")
        if isinstance(created_at, int) and not isinstance(created_at, bool):
            return created_at
    return None


def _latest_run_error(show: dict) -> str | None:
    runs = show.get("runs")
    if not isinstance(runs, list):
        return None
    latest: dict | None = None
    latest_id: int | None = None
    for raw_run in runs:
        if not isinstance(raw_run, dict):
            continue
        run_id = raw_run.get("id")
        if not (isinstance(run_id, int) and not isinstance(run_id, bool)):
            continue
        if latest_id is None or run_id > latest_id:
            latest_id = run_id
            latest = raw_run
    if latest is None:
        return None
    error = latest.get("error")
    return error if isinstance(error, str) and error else None


def candidate_from_show(board_slug: str, task_id: str, show: dict) -> DeathBlockCandidate | None:
    """Build a candidate from one ``show --json`` document; None when not the class."""
    task = show.get("task")
    if not isinstance(task, dict):
        raise StaleBlockWatchError(
            f"kanban show on {board_slug}/{task_id} omitted task"
        )
    if task.get("id") != task_id:
        raise StaleBlockWatchError(
            f"kanban show on {board_slug} returned a different task"
        )
    status = task.get("status")
    if not isinstance(status, str) or not status:
        raise StaleBlockWatchError(
            f"kanban show on {board_slug}/{task_id} omitted status"
        )
    title = task.get("title")
    if not isinstance(title, str):
        title = ""
    latest_event_kind = _latest_event_kind(show)
    if not is_silent_death_block(
        status=status,
        latest_event_kind=latest_event_kind,
        run_error=_latest_run_error(show),
    ):
        return None
    latest_event_id = _latest_event_id(show)
    if latest_event_id is None:
        raise StaleBlockWatchError(
            f"kanban show on {board_slug}/{task_id} omitted the latest event id"
        )
    run_error = _latest_run_error(show)
    return DeathBlockCandidate(
        board_slug=board_slug,
        task_id=task_id,
        title=title,
        latest_event_kind=latest_event_kind or "unknown",
        latest_event_id=latest_event_id,
        latest_run_error=run_error or "",
        config_defect=is_config_defect(run_error),
    )


def discover_silent_death_blocks(
    config: ControllerConfig,
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> list[DeathBlockCandidate]:
    """Return every silent death block across all non-archived boards (CLI-only)."""
    candidates: list[DeathBlockCandidate] = []
    for board in discover_boards(
        config.native_cli, runner=runner, timeout=timeout
    ):
        blocked = list_blocked_tasks(
            config.native_cli, board.slug, runner=runner, timeout=timeout
        )
        for entry in blocked:
            task_id = entry.get("id")
            if not isinstance(task_id, str) or not task_id:
                continue
            try:
                show = show_task(
                    config.native_cli, board.slug, task_id, runner=runner, timeout=timeout
                )
            except (ReviewGapError, NativeTimeoutError):
                # A transient CLI failure on one task must not kill the sweep;
                # the episode re-surfaces next tick.
                continue
            candidate = candidate_from_show(board.slug, task_id, show)
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def render_fresh(
    candidates: Sequence[DeathBlockCandidate],
    state: dict[str, int],
) -> tuple[list[DeathBlockCandidate], dict[str, int]]:
    """Split candidates into fresh (not yet pinged) and update dedupe state."""
    fresh: list[DeathBlockCandidate] = []
    updated = dict(state)
    for candidate in sorted(candidates, key=lambda item: item.latest_event_id):
        if candidate.key not in updated:
            fresh.append(candidate)
        updated[candidate.key] = candidate.latest_event_id
    return fresh, updated


def format_message(fresh: Sequence[DeathBlockCandidate]) -> str:
    """Render the digest; empty string when there is nothing fresh."""
    if not fresh:
        return ""
    lines: list[str] = []
    for candidate in fresh:
        lines.append(
            f"💀 Silent death block: {candidate.board_slug} `{candidate.task_id}` "
            f"({candidate.title})"
        )
        lines.append(f"• latest event: {candidate.latest_event_kind}")
        if candidate.config_defect:
            lines.append(
                "• CONFIG DEFECT: created with --max-runtime 0 — every run is "
                "SIGTERM'd at ~60s (elapsed Ns > limit 0s). Fix: set "
                "max_runtime_seconds to NULL and reset consecutive_failures "
                "(task config, not the work)."
            )
        elif candidate.latest_run_error:
            lines.append(f"• run error: {candidate.latest_run_error[:160]}")
    return "\n".join(lines)


def run(
    config: ControllerConfig,
    state_path: Path,
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> str:
    """Scan, dedupe, and render the digest; empty string when nothing fresh.

    Read-only against native boards through the CLI; the only controller-owned
    mutation is the atomic dedupe state file.  ``runner`` exists only for
    deterministic tests; production uses the installed ``hermes`` CLI via
    ``review_gap.run_native``.
    """
    state = _load_state(state_path)
    candidates = discover_silent_death_blocks(config, runner=runner, timeout=timeout)
    fresh, updated = render_fresh(candidates, state)
    _save_state(state_path, updated)
    return format_message(fresh)


__all__ = [
    "DEATH_KINDS",
    "DeathBlockCandidate",
    "StaleBlockWatchError",
    "STATE_FILENAME",
    "candidate_from_show",
    "default_state_path",
    "discover_silent_death_blocks",
    "format_message",
    "is_config_defect",
    "is_silent_death_block",
    "render_fresh",
    "run",
]
