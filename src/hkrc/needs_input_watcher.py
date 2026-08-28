"""Blocked-for-human-input watchdog (``needs_input`` blockers) — v2.

The recovery controller deliberately skips ``needs_input`` blockers — they
wait on a human, not on a controller. This module is the complement: it
surfaces tasks whose latest blocked episode waits on human input so an
operator (or a cron ``no_agent`` job delivering stdout to Telegram) can act.

Design notes
------------
- Read-only against native boards: plain ``mode=ro`` + ``query_only``
  connections. Unlike ``discovery._open_native_read_only`` (which fails
  closed on live WAL snapshots because it feeds reservations), this
  watchdog only *reads*; it must work against the live boards the gateway
  writes, so it uses a standard read-only connection.
- Episode semantics (v2): a task is eligible only while ``status='blocked'``
  AND the latest ``blocked``/``unblocked`` transition (event ``created_at``
  then event ``id`` as the insertion-order tie-breaker) is a ``blocked``
  event whose payload kind is ``needs_input``. Rows blocked with
  ``gave_up``/``capability``/unknown kinds, or unblocked-then-stale rows,
  are never pinged. The episode age is ``now - blocked_event.created_at``
  and must reach the configurable ``min_block_seconds`` (default 300).
  Episodes whose block reason starts with ``review-required`` are never
  pinged either: workers author ``needs_input`` blocks for review gates,
  and those gates are owned by the reviewer profile, not the human
  operator (operator verdict 2026-08-09).
- Dedupe per episode via a controller-owned state file keyed by
  ``board:task_id:<blocked_event_id>``: comments, heartbeats, and unrelated
  events never re-trigger, while a genuinely new blocked episode (new event
  id) does.
- Summarization (v2): for each fresh eligible episode the controller invokes
  exactly one configurable cheap text profile through the installed native
  CLI as an argv list — ``hermes -p <llm_profile> chat -q <prompt>
  --max-turns 4 --yolo -Q --reasoning none`` — with explicit
  ``HOME``/``HERMES_HOME``, ``_HERMES_GATEWAY`` and every ``HERMES_KANBAN_*``
  variable removed (the nested run must not boot into kanban goal-loop mode
  or record events against the parent card), captured output, and a
  configurable timeout.
  ``--max-turns 4`` gives the tool-using summarizer room to complete its one
  ``kanban show`` lookup and still answer inside the budget (a failed tool
  attempt must not exhaust the run); ``-Q`` keeps stdout machine-readable
  and ``--reasoning none`` suppresses the reasoning panel so only the final
  response is printed.  ``_is_valid_llm_output`` remains the backstop: any
  residual CLI-transcript chrome (reasoning panel, session info,
  max-iterations notice) fails shape/content validation and the episode
  renders as one deterministic fallback line.  Only complete valid output is
  emitted; on timeout, nonzero exit, empty output, or non-empty output that
  fails shape/content validation the episode renders as one deterministic
  fallback line.
- Intended surface: ``hkrc needs-input-watcher`` (CLI) and the cron shim
  ``needs-input-watcher.py`` installed next to this module's release wrapper.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from .config import ControllerConfig
from .discovery import discover_boards
from .handoff import NativeResult

STATE_FILENAME = "needs-input-watcher-state.json"
LEGACY_STATE_FILENAME = "blocker-ping-state.json"
DEFAULT_PROMPT_TEMPLATE = "{task_id}"

# Machinery-gate marker. Workers author ``needs_input`` blocks for review gates
# too (reason starts with this prefix), and only the reviewer profile can act
# on them — pinging the human operator for a pure review gate is noise. This
# watcher surfaces tasks waiting on *human* input, so any episode whose block
# reason starts with ``review-required`` is skipped. Trade-off (operator
# verdict 2026-08-09): mixed gates that bundle operator decisions under a
# ``review-required`` prefix (e.g. t_0ae4861f) go unpinged too.
REVIEW_GATE_REASON_PREFIX = "review-required"


class NeedsInputWatcherError(RuntimeError):
    """Raised when a configured native board cannot be inspected safely."""


@dataclass(frozen=True, slots=True)
class BlockedEpisode:
    """One active ``needs_input`` blocking episode on a native board.

    ``blocked_event_id`` is the native ``task_events.id`` of the ``blocked``
    transition that opened the episode; it anchors both the episode age and
    the dedupe key, so unrelated later events cannot re-trigger a ping.
    """

    board_slug: str
    task_id: str
    title: str
    block_kind: str
    reason: str
    blocked_event_id: int
    blocked_at: int

    @property
    def key(self) -> str:
        """Dedupe key: ``board:task_id:<blocked_event_id>``."""
        return f"{self.board_slug}:{self.task_id}:{self.blocked_event_id}"


def default_state_path(state_db: Path) -> Path:
    """Controller-owned state file next to the controller state database."""
    return state_db.parent / STATE_FILENAME


def _load_state(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NeedsInputWatcherError(f"cannot read needs-input-watcher state {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise NeedsInputWatcherError(f"needs-input-watcher state must be an object: {path}")
    state: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(value, int):
            raise NeedsInputWatcherError(
                f"needs-input-watcher state values must be integers: {path}"
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
        raise NeedsInputWatcherError(f"cannot persist needs-input-watcher state {path}: {exc}") from exc


def _open_read_only(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection
    except sqlite3.Error as exc:
        raise NeedsInputWatcherError(f"cannot open native board read-only {path}: {exc}") from exc


def _parse_block_payload(payload: str | None) -> dict[str, object] | None:
    """Parse a native ``blocked`` event payload; ``None`` when unparseable."""
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def discover_needs_input(
    native_boards_root: Path,
    *,
    now: int | None = None,
    min_block_seconds: int = 300,
) -> list[BlockedEpisode]:
    """Return active ``needs_input`` episodes across all non-archived boards.

    Eligibility (v2 contract): ``tasks.status='blocked'`` plus a latest
    ``blocked``/``unblocked`` transition — ordered by ``created_at`` then
    event ``id`` as insertion-order tie-breaker — that is a ``blocked``
    event whose payload kind is ``needs_input``. ``gave_up``-only rows and
    unblocked-but-stale rows are excluded. The episode must have lasted at
    least ``min_block_seconds``. Episodes whose block reason starts with
    ``REVIEW_GATE_REASON_PREFIX`` (``review-required``) are skipped: those
    are review gates the reviewer profile owns, not human-input waits.
    """
    current_time = int(time.time()) if now is None else int(now)
    episodes: list[BlockedEpisode] = []
    for board in discover_boards(native_boards_root):
        connection = _open_read_only(board.path / "kanban.db")
        try:
            rows = connection.execute(
                """
                SELECT t.id AS tid, t.title AS title,
                       latest.id AS event_id,
                       latest.payload AS payload,
                       latest.created_at AS blocked_at
                  FROM tasks AS t
                  JOIN task_events AS latest
                    ON latest.id = (
                        SELECT e.id
                          FROM task_events AS e
                         WHERE e.task_id = t.id
                           AND e.kind IN ('blocked', 'unblocked')
                         ORDER BY e.created_at DESC, e.id DESC
                         LIMIT 1
                    )
                 WHERE t.status = 'blocked'
                   AND latest.kind = 'blocked'
                 ORDER BY t.id ASC
                """
            ).fetchall()
        except sqlite3.Error as exc:
            raise NeedsInputWatcherError(
                f"cannot query native board {board.slug}: {exc}"
            ) from exc
        finally:
            connection.close()
        for row in rows:
            payload = _parse_block_payload(row["payload"])
            if payload is None or payload.get("kind") != "needs_input":
                # Fail closed on typing: an event-backed needs_input episode
                # is required; gave_up-only and untyped rows never ping.
                continue
            blocked_at = int(row["blocked_at"])
            if current_time - blocked_at < int(min_block_seconds):
                continue
            reason = payload.get("reason")
            reason_text = str(reason) if isinstance(reason, str) else ""
            if reason_text.startswith(REVIEW_GATE_REASON_PREFIX):
                # Review gates are owned by the reviewer profile, not the
                # human operator: never ping them (operator verdict
                # 2026-08-09 — 60% of that day's pings were review-required
                # machinery gates that needed no human action).
                continue
            episodes.append(
                BlockedEpisode(
                    board_slug=board.slug,
                    task_id=str(row["tid"]),
                    title=str(row["title"]),
                    block_kind="needs_input",
                    reason=reason_text,
                    blocked_event_id=int(row["event_id"]),
                    blocked_at=blocked_at,
                )
            )
    return episodes


def render_fresh(
    episodes: Sequence[BlockedEpisode],
    state: dict[str, int],
) -> tuple[list[BlockedEpisode], dict[str, int]]:
    """Split episodes into fresh (not yet pinged) and update dedupe state."""
    fresh: list[BlockedEpisode] = []
    updated = dict(state)
    for episode in sorted(episodes, key=lambda item: item.blocked_at):
        if episode.key not in updated:
            fresh.append(episode)
        updated[episode.key] = episode.blocked_event_id
    return fresh, updated


def build_prompt(template: str, task_id: str, board_slug: str) -> str:
    """Render a prompt template; the default contains only the task id.

    Both ``{task_id}`` and ``{board_slug}`` placeholders are provided so the
    template can instruct the summarizer to look the task up on the board
    that actually holds the blocked episode (``hermes kanban --board
    {board_slug} show {task_id} --json``) instead of whatever board happens
    to be current.
    """
    try:
        return template.format(task_id=task_id, board_slug=board_slug)
    except (KeyError, ValueError, IndexError) as exc:
        raise NeedsInputWatcherError(
            f"invalid needs_input_watcher prompt template: {exc}"
        ) from exc


def _load_prompt_template(path: str | None) -> str:
    if not path:
        return DEFAULT_PROMPT_TEMPLATE
    template_path = Path(path).expanduser()
    try:
        return template_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise NeedsInputWatcherError(
            f"cannot read needs_input_watcher prompt_template_path {template_path}: {exc}"
        ) from exc


def build_llm_command(config: ControllerConfig, prompt: str) -> list[str]:
    """Return the exact argv list for one summarizer invocation.

    The invocation is a product contract: ``hermes -p <llm_profile> chat -q
    <prompt> --max-turns 4 --yolo -Q --reasoning none``.  ``--max-turns 4``
    gives the tool-using summarizer room to complete its one ``kanban show``
    lookup and still answer inside the budget (a failed tool attempt must not
    exhaust the run, which previously leaked the tool trace and the CLI's
    max-iterations notice into the delivered ping); ``-Q`` (quiet) suppresses
    banner/spinner/tool previews so stdout carries only the final response,
    and ``--reasoning none`` suppresses the reasoning panel that quiet mode
    alone does not (observed leaking the model's thinking into the ping).
    ``_is_valid_llm_output`` remains the backstop against any residual
    CLI-transcript chrome reaching stdout.
    """
    profile = config.needs_input_watcher.llm_profile
    if not profile:
        raise NeedsInputWatcherError(
            "needs_input_watcher llm_profile is required for LLM summarization"
        )
    return [
        config.native_cli,
        "-p",
        profile,
        "chat",
        "-q",
        prompt,
        "--max-turns",
        "4",
        "--yolo",
        "-Q",
        "--reasoning",
        "none",
    ]


# Environment variables that must never reach the summarizer subprocess.
# The kanban dispatcher exports HERMES_KANBAN_* (task id, run id, claim
# lock, board, db, goal mode, ...) into worker environments; leaking them
# into the nested CLI would boot it into kanban goal-loop mode, print the
# goal-loop transcript to stdout, and let the nested run record a
# timed_out event against the parent card.  _HERMES_GATEWAY would let the
# summarizer hijack the running gateway's live stream.
_LLM_ENV_STRIP_PREFIXES = ("HERMES_KANBAN_",)
_LLM_ENV_STRIP_EXACT = ("_HERMES_GATEWAY",)


def build_llm_environment(
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the summarizer environment with explicit HOME/HERMES_HOME.

    ``_HERMES_GATEWAY`` and every ``HERMES_KANBAN_*`` variable are removed
    so the summarizer cannot hijack the running gateway's live stream, boot
    into kanban goal-loop mode, or record events against the parent task;
    ``HOME`` is pinned and ``HERMES_HOME`` defaults to ``<HOME>/.hermes``
    when absent.
    """
    env = dict(os.environ if base is None else base)
    for key in list(env):
        if key.startswith(_LLM_ENV_STRIP_PREFIXES) or key in _LLM_ENV_STRIP_EXACT:
            env.pop(key, None)
    home = env.get("HOME") or str(Path.home())
    env["HOME"] = home
    env.setdefault("HERMES_HOME", os.path.join(home, ".hermes"))
    return env


LlmRunner = Callable[[Sequence[str], Mapping[str, str], int], NativeResult]

# A usable summary has at least two whitespace-separated words and a
# minimum length; a bare token (an echoed task id, "garbage", ...) is not
# a summary and must render the deterministic fallback instead.
_MIN_LLM_OUTPUT_WORDS = 2
_MIN_LLM_OUTPUT_CHARS = 10

# CLI-transcript chrome markers. Quiet mode (-Q) plus --reasoning none keep
# stdout down to the final response, but a CLI regression or a reasoning-model
# quirk can still leak the reasoning panel, session info, or the max-iterations
# notice onto stdout (observed 2026-08-09: a delivered ping carried the
# reasoning trace, 'Command Hermes isn't found...' tool output, and
# 'Reached maximum iterations (2). Requesting summary.'). Any of these
# signatures marks the output as chrome so it is rejected and the
# deterministic fallback renders instead. The set is deliberately precise:
# markers are CLI-specific phrases or box-drawing frame corners, never bare
# words or status glyphs (e.g. 'Reasoning', '✓') that a legitimate summary
# could contain — a false rejection costs a rich summary and renders the
# one-line fallback. _contains_cli_chrome additionally rejects any
# box-drawing character, which covers the full reasoning-panel frame.
# Single source of truth: scripts/e2e_canonical_invocation.py imports this
# same constant for its stdout-purity verdict.
_LLM_OUTPUT_CHROME_MARKERS = (
    "Resume this session with",
    "Initializing agent",
    "session_id",
    "Query:",
    "Reached maximum iterations",
    "Iteration budget exhausted",
    "Requesting summary",
    "couldn't summarize",
    "┌─",
    "└─",
)


def _contains_cli_chrome(output: str) -> bool:
    """Return True when stdout carries CLI-transcript chrome.

    Matches the exit-summary resume line, the startup banner, session-info
    lines, the query echo, the max-iterations/iteration-budget notices, and
    any box-drawing characters (the reasoning panel and response box
    frames) — none of which belong in a summary.
    """
    if any(marker in output for marker in _LLM_OUTPUT_CHROME_MARKERS):
        return True
    return any("\u2500" <= char <= "\u257f" for char in output)


def _is_valid_llm_output(output: str) -> bool:
    """Return True when stdout is a plausible summary rather than degenerate text.

    Shape/content rule: the stripped text must carry at least two
    whitespace-separated words and a minimum character count, and must not
    contain CLI-transcript chrome.  This keeps genuine multi-word summaries
    while rejecting single-token, near-empty, or chrome-laden output that
    would otherwise be emitted as
    ``Summary and suggested next steps: <garbage>``.
    """
    text = output.strip()
    if len(text) < _MIN_LLM_OUTPUT_CHARS:
        return False
    if len(text.split()) < _MIN_LLM_OUTPUT_WORDS:
        return False
    return not _contains_cli_chrome(text)


def run_llm(
    command: Sequence[str],
    env: Mapping[str, str],
    timeout_seconds: int,
    *,
    runner: LlmRunner | None = None,
) -> str | None:
    """Invoke the summarizer once; return complete valid stdout or ``None``.

    ``None`` covers timeout, nonzero exit, a failed spawn, empty output,
    and non-empty output that fails shape/content validation — the caller
    renders the deterministic fallback instead and never emits partial or
    degenerate output.
    """
    if runner is not None:
        result = runner(list(command), dict(env), int(timeout_seconds))
    else:
        try:
            completed = subprocess.run(
                list(command),
                capture_output=True,
                text=True,
                check=False,
                timeout=int(timeout_seconds),
                env=dict(env),
            )
            result = NativeResult(completed.returncode, completed.stdout, completed.stderr)
        except subprocess.TimeoutExpired:
            return None
        except OSError:
            return None
    if result.returncode != 0:
        return None
    output = (result.stdout or "").strip()
    if not output or not _is_valid_llm_output(output):
        return None
    return output


def fallback_line(episode: BlockedEpisode, now: int) -> str:
    """Deterministic one-line fallback for a failed summarizer invocation."""
    age = int(now) - episode.blocked_at
    return (
        "needs-input-watcher fallback "
        f"board={episode.board_slug} task={episode.task_id} "
        f"kind={episode.block_kind} age={age} episode={episode.key}"
    )


def format_message(
    fresh: Sequence[BlockedEpisode],
    llm_outputs: Mapping[str, str | None],
    *,
    now: int | None = None,
) -> str:
    """Render the digest; empty string when there is nothing fresh."""
    if not fresh:
        return ""
    current_time = int(time.time()) if now is None else int(now)
    blocks: list[str] = []
    for episode in fresh:
        output = llm_outputs.get(episode.key)
        if output is None:
            blocks.append(fallback_line(episode, current_time))
            continue
        age = current_time - episode.blocked_at
        reason = episode.reason or "no reason recorded"
        blocks.append(
            f"⏸ Blocked — needs your input: {episode.board_slug} `{episode.task_id}`\n"
            f"• {reason} (blocked for {age}s)\n\n"
            f"Summary and suggested next steps:\n{output}"
        )
    return "\n\n".join(blocks)


def migrate_state_file(state_path: Path) -> None:
    """Rename the legacy blocker-ping state file onto the new name when present.

    The rename keeps the dedupe history: without it, the first run after an
    upgrade would start from an empty state file and re-ping every currently
    blocked episode once. Only the exact legacy filename is migrated, so an
    explicitly chosen ``--state-file`` name is never touched.
    """
    if state_path.exists() or state_path.name != STATE_FILENAME:
        return
    legacy = state_path.with_name(LEGACY_STATE_FILENAME)
    if not legacy.is_file():
        return
    legacy.rename(state_path)


def run(
    native_boards_root: Path,
    state_path: Path,
    config: ControllerConfig,
    *,
    now: int | None = None,
    runner: LlmRunner | None = None,
) -> str:
    """Scan, summarize fresh eligible episodes (or fall back), persist dedupe.

    Read-only against native boards; the only controller-owned mutation is
    the atomic dedupe state file (plus a one-time rename of a legacy
    blocker-ping state file onto the new name). An empty fresh set stays
    silent. ``runner`` exists only for deterministic tests; production uses
    ``subprocess.run`` with an argv list, never a shell command.
    """
    if not config.needs_input_watcher.enabled:
        return ""
    migrate_state_file(state_path)
    state = _load_state(state_path)
    episodes = discover_needs_input(
        native_boards_root,
        now=now,
        min_block_seconds=config.needs_input_watcher.min_block_seconds,
    )
    fresh, updated = render_fresh(episodes, state)
    _save_state(state_path, updated)
    if not fresh:
        return ""
    llm_outputs: dict[str, str | None] = {}
    if config.needs_input_watcher.llm_profile:
        template = _load_prompt_template(config.needs_input_watcher.prompt_template_path)
        for episode in fresh:
            prompt = build_prompt(template, episode.task_id, episode.board_slug)
            command = build_llm_command(config, prompt)
            env = build_llm_environment()
            llm_outputs[episode.key] = run_llm(
                command,
                env,
                config.needs_input_watcher.timeout_seconds,
                runner=runner,
            )
    return format_message(fresh, llm_outputs, now=now)


__all__ = [
    "NeedsInputWatcherError",
    "BlockedEpisode",
    "DEFAULT_PROMPT_TEMPLATE",
    "REVIEW_GATE_REASON_PREFIX",
    "STATE_FILENAME",
    "build_llm_command",
    "build_llm_environment",
    "build_prompt",
    "default_state_path",
    "discover_needs_input",
    "fallback_line",
    "format_message",
    "migrate_state_file",
    "render_fresh",
    "run",
    "run_llm",
]
