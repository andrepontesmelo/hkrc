"""One-shot native Hermes Kanban recovery handoff.

All native mutations go through the installed Hermes CLI as subprocesses. The
controller never imports Hermes internals and never opens a native board DB for
writing. A reservation is consumed before the first native command; failures
are recorded and the task is never retried or rolled back automatically.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import os
import subprocess

from .config import ControllerConfig
from .discovery import (
    RECENCY_WINDOW_SECONDS,
    UNCLAIMED_CHILD_KIND,
    discover_and_reserve,
    discover_stale_blockers,
    stale_blocker_note,
)
from .state import ControllerState


HANDOFF_COMMENT = (
    "[blocker-recovery-controller] This task was blocked recently and received "
    "one controlled recovery handoff. Review the current blocker and decide "
    "whether work can safely continue. If recoverable, perform required "
    "recovery, choose the appropriate assignee, and resume the task. If not "
    "recoverable, keep the task blocked, enable native blocked-task notification "
    "for Andre, record the precise reason, and do not create a recovery loop. "
    "The controller will not attempt another unblock for this task."
)
UNCLAIMED_CHILD_COMMENT = (
    "[blocker-recovery-controller] This task is an unclaimed child whose parent "
    "is done or blocked, and it has been sitting in todo/ready beyond the "
    "recovery window. It received one controlled alert. Review why the assigned "
    "profile did not claim it: if the parent is done, promote or dispatch this "
    "task so its assigned profile can pick it up; if the parent is blocked, "
    "resolve the parent's blocker or explicitly assign this task. The controller "
    "will not re-alert for this task."
)
LEAD_ASSIGNEE = "lead-orchestrator"


class HandoffError(RuntimeError):
    """Raised when a one-shot handoff cannot be started safely."""


@dataclass(frozen=True, slots=True)
class NativeResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


NativeRunner = Callable[[Sequence[str]], NativeResult]


@dataclass(frozen=True, slots=True)
class HandoffReport:
    lines: tuple[str, ...]
    reserved: int
    started: int
    completed: int
    failed: int
    skipped: int

    @property
    def exit_code(self) -> int:
        return 0 if self.failed == 0 else 1


def execute_handoff(
    config: ControllerConfig,
    state: ControllerState,
    *,
    now: int | None = None,
    unclaimed_after: int | None = None,
    window_seconds: int | None = RECENCY_WINDOW_SECONDS,
    runner: NativeRunner | None = None,
    stop_requested: Callable[[], bool] | None = None,
) -> HandoffReport:
    """Discover and perform each currently pending one-ever reservation.

    ``runner`` exists only for deterministic tests. Production calls use
    ``subprocess.run`` and pass a list of arguments, never a shell command.

    ``window_seconds`` is the effective recency window forwarded to discovery;
    ``None`` disables the lower bound (full backfill).  The CLI resolves the
    configured ``[discovery] recency_window_seconds`` before calling this; the
    default here keeps direct callers on the historic 3600-second behavior.
    Blocked tasks outside the effective window are reported as visible
    ``note`` lines (``stale_blocker_note``) instead of being silently omitted,
    and are never reserved.
    """

    telegram_chat_id = _validate_destination(config)
    resolutions = discover_and_reserve(
        config.native_boards_root,
        state,
        now=now,
        unclaimed_after=(
            config.unclaimed_child_after_seconds
            if unclaimed_after is None
            else unclaimed_after
        ),
        window_seconds=window_seconds,
    )
    stale_notes = tuple(
        stale_blocker_note(candidate, now=now)
        for candidate in discover_stale_blockers(
            config.native_boards_root, now=now, window_seconds=window_seconds
        )
    )
    return execute_reserved_handoff(
        config,
        state,
        reserved=sum(resolution.action == "reserved" for resolution in resolutions),
        skipped=sum(resolution.action == "skipped" for resolution in resolutions),
        lines=tuple(resolution.stdout_line() for resolution in resolutions) + stale_notes,
        runner=runner,
        stop_requested=stop_requested,
        telegram_chat_id=telegram_chat_id,
    )


def execute_reserved_handoff(
    config: ControllerConfig,
    state: ControllerState,
    *,
    reserved: int = 0,
    skipped: int = 0,
    lines: tuple[str, ...] = (),
    runner: NativeRunner | None = None,
    stop_requested: Callable[[], bool] | None = None,
    telegram_chat_id: str | None = None,
) -> HandoffReport:
    """Perform the official CLI sequence for existing controller reservations.

    ``execute_handoff`` is the compatibility one-shot entry point and still
    performs native read-only discovery.  The continuous stream runtime calls
    this lower boundary only after it has classified and durably reserved
    candidates, so it never re-enters native observation.
    """

    telegram_chat_id = telegram_chat_id or _validate_destination(config)
    native_runner = runner or _run_native
    pending = state.pending_reservations()
    # Stream reservations are already durable and must not be rediscovered from
    # the native board.  One-shot callers still use execute_handoff above.
    started = completed = failed = 0
    output_lines = list(lines)

    should_stop = stop_requested or (lambda: False)
    for reservation in pending:
        # A stop signal never interrupts a native command, but it prevents a
        # later reservation from starting after the active handoff returns.
        if should_stop():
            break
        board_slug = str(reservation["board_slug"])
        task_id = str(reservation["task_id"])
        if not state.begin_intervention(board_slug, task_id):
            output_lines.append(_status_line(board_slug, task_id, "started", "already_started"))
            continue
        started += 1
        output_lines.append(_status_line(board_slug, task_id, "started", "ok"))
        if str(reservation["blocker_kind"]) == UNCLAIMED_CHILD_KIND:
            # An unclaimed child is not blocked: never reassign it away from
            # its assigned profile and never unblock it.  The controlled
            # intervention is the Telegram alert plus the child-specific
            # comment; promotion stays with the native dispatcher once the
            # parent actually completes.
            phases = (
                (
                    "subscription",
                    _subscription_command(config, board_slug, task_id, telegram_chat_id),
                ),
                ("comment", _child_comment_command(config, board_slug, task_id)),
            )
        else:
            phases = (
                (
                    "subscription",
                    _subscription_command(config, board_slug, task_id, telegram_chat_id),
                ),
                ("comment", _comment_command(config, board_slug, task_id)),
                ("reassign", _reassign_command(config, board_slug, task_id)),
                ("unblock", _unblock_command(config, board_slug, task_id)),
            )
        task_failed = False
        for phase, command in phases:
            try:
                result = native_runner(command)
            except Exception as exc:  # noqa: BLE001 - task-local native boundary
                result = NativeResult(
                    1,
                    stderr=f"native runner failed: {type(exc).__name__}: {exc}",
                )
            if result.stdout:
                output_lines.append(_native_output_line(board_slug, task_id, phase, result.stdout))
            if result.returncode != 0:
                error = _command_error(result)
                state.record_intervention_phase(
                    board_slug, task_id, phase, outcome="error", error=error
                )
                output_lines.append(_status_line(board_slug, task_id, phase, "error", error))
                task_failed = True
                failed += 1
                break
            state.record_intervention_phase(
                board_slug, task_id, phase, outcome="ok", error=None
            )
            output_lines.append(_status_line(board_slug, task_id, phase, "ok"))
            if should_stop():
                # The active native command has returned and its result is
                # durable. Do not begin another phase during graceful stop.
                break

        if should_stop():
            continue
        if not task_failed:
            state.record_intervention_phase(
                board_slug, task_id, "complete", outcome="ok", error=None
            )
            output_lines.append(_status_line(board_slug, task_id, "complete", "ok"))
            completed += 1

    output_lines.append(
        "summary "
        f"reserved={reserved} started={started} completed={completed} "
        f"failed={failed} skipped={skipped}"
    )
    return HandoffReport(tuple(output_lines), reserved, started, completed, failed, skipped)


def _validate_destination(config: ControllerConfig) -> str:
    if config.telegram_chat_id_env:
        destination = os.environ.get(config.telegram_chat_id_env, "").strip()
    else:
        destination = config.telegram_chat_id.strip()
    if not destination:
        raise HandoffError("telegram destination is required: configure [telegram] chat_id")
    return destination


def _base_command(config: ControllerConfig, board_slug: str) -> list[str]:
    command = [config.native_cli]
    if config.native_profile:
        command.extend(["--profile", config.native_profile])
    command.extend(["kanban", "--board", board_slug])
    return command


def _subscription_command(
    config: ControllerConfig,
    board_slug: str,
    task_id: str,
    telegram_chat_id: str,
) -> list[str]:
    command = _base_command(config, board_slug)
    command.extend(
        [
            "notify-subscribe", task_id,
            "--platform", "telegram",
            "--chat-id", telegram_chat_id,
            "--chat-type", config.telegram_chat_type,
        ]
    )
    _append_optional(command, "--thread-id", config.telegram_thread_id)
    _append_optional(command, "--user-id", config.telegram_user_id)
    _append_optional(command, "--notifier-profile", config.telegram_notifier_profile)
    return command


def _comment_command(config: ControllerConfig, board_slug: str, task_id: str) -> list[str]:
    return _base_command(config, board_slug) + ["comment", task_id, HANDOFF_COMMENT]


def _child_comment_command(config: ControllerConfig, board_slug: str, task_id: str) -> list[str]:
    return _base_command(config, board_slug) + ["comment", task_id, UNCLAIMED_CHILD_COMMENT]


def _reassign_command(config: ControllerConfig, board_slug: str, task_id: str) -> list[str]:
    return _base_command(config, board_slug) + ["reassign", task_id, LEAD_ASSIGNEE]


def _unblock_command(config: ControllerConfig, board_slug: str, task_id: str) -> list[str]:
    return _base_command(config, board_slug) + ["unblock", task_id]


def _append_optional(command: list[str], option: str, value: str | None) -> None:
    if value:
        command.extend([option, value])


def _run_native(command: Sequence[str]) -> NativeResult:
    try:
        completed = subprocess.run(
            list(command), capture_output=True, text=True, check=False, timeout=120
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _decode_output(exc.stdout)
        stderr = _decode_output(exc.stderr)
        return NativeResult(124, stdout, stderr or "native CLI timed out after 120 seconds")
    except OSError as exc:
        return NativeResult(127, "", str(exc))
    return NativeResult(completed.returncode, completed.stdout, completed.stderr)


def _decode_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def _command_error(result: NativeResult) -> str:
    return (result.stderr or result.stdout or f"native CLI exited with {result.returncode}").strip()


def _status_line(
    board_slug: str,
    task_id: str,
    phase: str,
    outcome: str,
    error: str | None = None,
) -> str:
    line = f"board_slug={board_slug} task_id={task_id} phase={phase} outcome={outcome}"
    return f"{line} error={error}" if error else line


def _native_output_line(board_slug: str, task_id: str, phase: str, stdout: str) -> str:
    return f"native_stdout board_slug={board_slug} task_id={task_id} phase={phase}\n{stdout.rstrip()}"


__all__ = [
    "HANDOFF_COMMENT", "HandoffError", "HandoffReport", "LEAD_ASSIGNEE",
    "NativeResult", "UNCLAIMED_CHILD_COMMENT",
    "execute_handoff", "execute_reserved_handoff",
]
