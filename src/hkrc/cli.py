"""Command-line entry point for the controller foundation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys

from . import __version__
from .admission import AdmissionError, admit_child
from .config import (
    ConfigError,
    ControllerConfig,
    NeedsInputWatcherConfig,
    ReviewGapConfig,
    StreamConfig,
    WatcherConfig,
    default_config,
    default_config_path,
    load_config,
    write_config,
)
from .crons import CRON_MANIFEST_FILENAME, CronManifestError, default_manifest_path, run_sync
from .discovery import (
    DiscoveryError,
    discover_and_reserve,
    discover_stale_blockers,
    stale_blocker_note,
)
from .git_enforce import (
    GitEnforceError,
    PROTECTED_REF_DEFAULT,
    hook_status,
    install_hook,
    run_hook_command,
    uninstall_hook,
)
from .handoff import HandoffError, execute_handoff
from .harness_loop import (
    STATE_FILENAME as HARNESS_LOOP_STATE_FILENAME,
    HarnessLoopConfig,
    HarnessLoopError,
    default_state_path as harness_loop_default_state_path,
    run as run_harness_loop,
)
from .live import build_live_stream_wiring
from .simulation import run_simulation
from .needs_input_watcher import (
    STATE_FILENAME,
    NeedsInputWatcherError,
    default_state_path,
    run as run_needs_input_watcher,
)
from .outcome_guard import OutcomeGuard, OutcomeGuardError
from .review_gap import (
    STATE_FILENAME as REVIEW_GAP_STATE_FILENAME,
    ReviewGapError,
    default_state_path as review_gap_default_state_path,
    run as run_review_gap,
)
from .runtime import DaemonRuntime, InstanceLock, LockError
from .stale_block_watch import (
    STATE_FILENAME as STALE_BLOCK_WATCH_STATE_FILENAME,
    StaleBlockWatchError,
    default_state_path as stale_block_watch_default_state_path,
    run as run_stale_block_watch,
)
from .state import ControllerState, StateError
from .watcher import (
    STATE_FILENAME as WATCHER_STATE_FILENAME,
    WatcherError,
    build_watcher_wiring,
    default_state_path as default_watcher_state_path,
    run as run_watcher,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hkrc",
        description=(
            "Portable Hermes Kanban blocker-recovery controller. "
            "Discovery is read-only against native boards."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser(
        "init",
        help="create instance-scoped config and controller-owned SQLite state",
    )
    init.add_argument("--config", type=Path, default=default_config_path())
    init.add_argument("--instance-name")
    init.add_argument("--native-boards-root", type=Path)
    init.add_argument("--state-db", type=Path)
    init.add_argument("--workspace", type=Path)
    init.add_argument("--native-cli")
    init.add_argument("--native-profile")
    init.add_argument("--telegram-chat-id")
    init.add_argument("--telegram-chat-id-env")
    init.add_argument("--telegram-chat-type")
    init.add_argument("--telegram-thread-id")
    init.add_argument("--telegram-user-id")
    init.add_argument("--telegram-notifier-profile")
    init.add_argument(
        "--stream-enabled",
        action="store_true",
        help="enable only the declarative approved stream gate (no adapter is auto-created)",
    )
    init.add_argument("--stream-adapter", choices=("none", "approved_websocket"))
    init.add_argument("--stream-endpoint")
    init.add_argument("--stream-board", action="append", dest="stream_boards")
    init.add_argument("--stream-credential-env")
    init.add_argument("--stream-current-state-reader")
    init.add_argument(
        "--stream-alert-after-consecutive-failures",
        type=int,
        help="consecutive stream failures per board before a self-health journald alert (default: 3)",
    )
    init.add_argument(
        "--needs-input-watcher-enabled",
        "--blocker-ping-enabled",
        action="store_true",
        help="enable the human-input blocker watchdog (default: enabled)",
    )
    init.add_argument(
        "--needs-input-watcher-min-block-seconds",
        "--blocker-ping-min-block-seconds",
        type=int,
        help="minimum blocked duration before an episode pings (default: 300)",
    )
    init.add_argument(
        "--needs-input-watcher-llm-profile",
        "--blocker-ping-llm-profile",
        help="cheap text profile for one summarizer call per fresh episode (empty = deterministic lines)",
    )
    init.add_argument(
        "--needs-input-watcher-prompt-template-path",
        "--blocker-ping-prompt-template-path",
        help="instance-local prompt template file containing a {task_id} placeholder",
    )
    init.add_argument(
        "--needs-input-watcher-timeout-seconds",
        "--blocker-ping-timeout-seconds",
        type=int,
        help="summarizer subprocess timeout (default: 90)",
    )
    init.add_argument(
        "--review-gap-enabled",
        action="store_true",
        help="enable the review-gap watchdog (default: enabled)",
    )
    init.add_argument(
        "--review-gap-min-age-seconds",
        type=int,
        help="minimum age of a done task before its gap is closed (default: 300)",
    )
    init.add_argument(
        "--review-gap-recency-hours",
        type=int,
        help="how far back done worktree tasks are considered (default: 48)",
    )
    init.add_argument(
        "--review-gap-stalled-alert-hours",
        type=int,
        help="unmerged review age before a stall alert fires (default: 6)",
    )
    init.add_argument(
        "--review-gap-auto-create",
        action="store_true",
        help="auto-create the missing review card (default: true)",
    )
    init.add_argument(
        "--review-gap-trigger-c-enabled",
        action="store_true",
        help="auto-complete review-required blocked parents with shipped work and a review child (default: enabled)",
    )
    init.add_argument(
        "--review-gap-trigger-d-enabled",
        action="store_true",
        help="create re-apply cards for reverted, un-re-merged kanban merges (default: enabled)",
    )
    init.add_argument(
        "--review-gap-cli-timeout-seconds",
        type=int,
        help="per native CLI/git subprocess timeout, bounds a hung board (default: 30)",
    )
    init.add_argument(
        "--review-gap-tick-timeout-seconds",
        type=int,
        help="whole-tick wall-clock budget; remaining boards are skipped with an alert (default: 120)",
    )
    init.add_argument(
        "--review-gap-max-workers",
        type=int,
        help="parallel read workers for the done/blocked candidate passes (default: 16)",
    )
    init.add_argument(
        "--watcher-enabled",
        action="store_true",
        help="enable the decision-latency watcher (default: enabled)",
    )
    init.add_argument(
        "--watcher-reviewer-profiles",
        action="append",
        dest="watcher_reviewer_profiles",
        help="reviewer profile allowlist (repeatable; empty = assignee contains 'reviewer')",
    )
    init.add_argument(
        "--watcher-fix-assignee",
        help="assignee for auto-created fix cards (default: developer)",
    )
    init.add_argument(
        "--watcher-max-block-age-seconds",
        type=int,
        help="H1 recency window for defect blocks (default: 1800)",
    )
    init.add_argument(
        "--watcher-canonical-branch",
        help="canonical branch for H2 merge verification (default: main)",
    )
    init.add_argument(
        "--watcher-canonical-branch-fallback",
        help="fallback canonical branch when the primary does not exist (default: master)",
    )
    init.add_argument(
        "--watcher-hold-comment-window-seconds",
        type=int,
        help="H3 recent-hold-comment window (default: 3600)",
    )
    init.add_argument(
        "--watcher-recv-timeout-seconds",
        type=float,
        help="WebSocket read timeout, below the cron cycle interval (default: 10.0)",
    )
    init.add_argument(
        "--watcher-cycle-interval-seconds",
        type=float,
        help="cron cycle interval the watcher runs under; recv_timeout must stay below (default: 300.0)",
    )
    init.add_argument(
        "--watcher-deadlock-min-age-seconds",
        type=float,
        help="H5 debounce: review-required block episode age before the deadlock archive fires (default: 900.0)",
    )
    init.add_argument(
        "--harness-loop-enabled",
        action="store_true",
        help="enable the daily noon harness-learning loop (default: enabled)",
    )
    init.add_argument(
        "--harness-loop-window-hours",
        type=int,
        help="audit window in hours (default: 24)",
    )
    init.add_argument(
        "--harness-loop-max-applies",
        type=int,
        choices=(1, 2),
        help="max applied changes per run, 1 orchestration + 1 hkrc (default: 2)",
    )
    init.add_argument(
        "--harness-loop-cooldown-days",
        type=int,
        help="fingerprint cooldown in days for apply/suggest candidates (default: 30)",
    )
    init.add_argument(
        "--harness-loop-bloat-threshold-tokens",
        type=int,
        help="session input-token bloat threshold (default: 5000000)",
    )
    init.add_argument(
        "--harness-loop-bloat-top-n",
        type=int,
        help="top-N bloat sessions reported each run (default: 3)",
    )
    init.add_argument(
        "--harness-loop-sessions-db",
        help="live sessions database (default: <instance-root>/profiles/main/state.db)",
    )
    init.add_argument(
        "--harness-loop-external-dir",
        action="append",
        dest="harness_loop_external_dirs",
        help="worker-facing skill dist root (repeatable; empty = git dist default)",
    )
    init.add_argument(
        "--harness-loop-hkrc-repo",
        help="HKRC repository for gated applies (default: ~/git/hermes-kanban-recovery-controller)",
    )
    init.add_argument("--force", action="store_true", help="replace an existing config")
    init.set_defaults(handler=_init)

    status = subparsers.add_parser(
        "status",
        help="show config and controller-state health without scanning native data",
    )
    status.add_argument("--config", type=Path, default=default_config_path())
    status.set_defaults(handler=_status)

    discover = subparsers.add_parser(
        "discover",
        help="discover recent blocked tasks and reserve eligible candidates read-only",
    )
    discover.add_argument("--config", type=Path, default=default_config_path())
    discover.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    _add_backfill_argument(discover)
    discover.set_defaults(handler=_discover)
    run = subparsers.add_parser(
        "run",
        help="discover and perform one native CLI recovery handoff",
    )
    run.add_argument("--config", type=Path, default=default_config_path())
    run.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    _add_backfill_argument(run)
    run.set_defaults(handler=_run)

    daemon = subparsers.add_parser(
        "daemon",
        help=(
            "run continuous recovery only with an injected approved authenticated "
            "stream adapter; never falls back to native DB or CLI watch/tail"
        ),
    )
    daemon.add_argument("--config", type=Path, default=default_config_path())
    daemon.add_argument(
        "--event-batch-size", type=int, default=200,
        help="maximum stream event rows accepted per frame",
    )
    daemon.add_argument(
        "--max-cycles", type=int,
        help="stop after this many cycles (primarily useful for tests)",
    )
    daemon.set_defaults(handler=_daemon)

    needs_input_watcher = subparsers.add_parser(
        "needs-input-watcher",
        aliases=["blocker-ping"],
        help=(
            "report tasks blocked waiting for human input (needs_input) — "
            "designed for cron no_agent delivery; silent when nothing new"
        ),
    )
    needs_input_watcher.add_argument("--config", type=Path, default=default_config_path())
    needs_input_watcher.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help=f"dedupe state file (default: <instance-root>/state/hkrc/{STATE_FILENAME})",
    )
    needs_input_watcher.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    needs_input_watcher.set_defaults(handler=_needs_input_watcher)

    stale_block_watch = subparsers.add_parser(
        "stale-block-watch",
        help=(
            "report blocked tasks whose latest event is a dispatcher death "
            "kind (gave_up/spawn_failed/timed_out/crashed) with no typed "
            "blocked event — the silent spawn-failure block class; flags the "
            "--max-runtime 0 config defect; designed for cron no_agent "
            "delivery; silent when nothing new"
        ),
    )
    stale_block_watch.add_argument("--config", type=Path, default=default_config_path())
    stale_block_watch.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help=(
            f"dedupe state file (default: <instance-root>/state/hkrc/"
            f"{STALE_BLOCK_WATCH_STATE_FILENAME})"
        ),
    )
    stale_block_watch.add_argument(
        "--timeout", type=float, default=None, help="per-CLI-call timeout in seconds"
    )
    stale_block_watch.set_defaults(handler=_stale_block_watch)

    review_gap = subparsers.add_parser(
        "review-gap",
        help=(
            "auto-create missing review cards for done worktree tasks and alert "
            "on stalled reviews — designed for cron no_agent delivery; silent "
            "when nothing new"
        ),
    )
    review_gap.add_argument("--config", type=Path, default=default_config_path())
    review_gap.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help=f"dedupe state file (default: <instance-root>/state/hkrc/{REVIEW_GAP_STATE_FILENAME})",
    )
    review_gap.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    review_gap.set_defaults(handler=_review_gap)

    watcher = subparsers.add_parser(
        "watcher",
        help=(
            "decision-latency automation: auto-create fix cards (H1), supersede "
            "close (H2), pick-gate advance (H3), blocked-without-event guard (H4) "
            "— designed for cron no_agent delivery; silent when nothing new"
        ),
    )
    watcher.add_argument("--config", type=Path, default=default_config_path())
    watcher.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help=(
            f"watcher state file with per-board cursors + action keys "
            f"(default: <instance-root>/state/hkrc/{WATCHER_STATE_FILENAME})"
        ),
    )
    watcher.add_argument(
        "--dry-run",
        action="store_true",
        help="report would-have actions without mutating anything (default for first deploy)",
    )
    watcher.add_argument(
        "--replay",
        action="store_true",
        help="reprocess full history from cursor zero (only legal with --dry-run)",
    )
    watcher.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    watcher.set_defaults(handler=_watcher)

    harness_loop = subparsers.add_parser(
        "harness-loop",
        help=(
            "daily noon harness-learning loop: audit sessions + boards, render "
            "the self-review report, and (live mode only) apply up to 2 "
            "orchestration/hkrc fixes"
        ),
    )
    harness_loop_sub = harness_loop.add_subparsers(dest="harness_loop_command", required=True)
    harness_loop_run = harness_loop_sub.add_parser(
        "run",
        help=(
            "audit + render the report; dry-run by default (zero applies) — "
            "the cron shim flips --no-dry-run only after operator review"
        ),
    )
    harness_loop_run.add_argument("--config", type=Path, default=default_config_path())
    harness_loop_run.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help=(
            f"harness-loop dedupe state file "
            f"(default: <instance-root>/state/hkrc/{HARNESS_LOOP_STATE_FILENAME})"
        ),
    )
    harness_loop_run.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="audit+report only, zero applies (default)",
    )
    harness_loop_run.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="allow up to max_applies applied changes (operator review gate)",
    )
    harness_loop_run.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    harness_loop_run.set_defaults(handler=_harness_loop)
    harness_loop_simulate = harness_loop_sub.add_parser(
        "simulate",
        help=(
            "run the real harness pipeline against copied state and an isolated "
            "shadow board; fail if live state, board counts, or repo status move"
        ),
    )
    harness_loop_simulate.add_argument(
        "--config", type=Path, default=default_config_path()
    )
    harness_loop_simulate.add_argument(
        "--shadow-dir",
        type=Path,
        default=None,
        help="directory for copied state, shadow board, and report artifacts",
    )
    harness_loop_simulate.add_argument(
        "--now", type=int, help="override Unix time for deterministic runs and tests"
    )
    harness_loop_simulate.set_defaults(handler=_harness_loop_simulate)

    crons = subparsers.add_parser(
        "crons",
        help="manifest-driven reconciliation of Hermes cron jobs",
    )
    crons_sub = crons.add_subparsers(dest="crons_command", required=True)
    crons_sync = crons_sub.add_parser(
        "sync",
        help=(
            "bring live Hermes cron jobs in line with the manifest "
            "(config/hkrc/cron_manifest.json); creates missing jobs, resumes "
            "paused ones, updates stale schedule/script/delivery/skills; "
            "never touches unlisted jobs; silent when in sync"
        ),
    )
    crons_sync.add_argument("--config", type=Path, default=default_config_path())
    crons_sync.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=f"path to the cron manifest (default: <config dir>/{CRON_MANIFEST_FILENAME})",
    )
    crons_sync.add_argument(
        "--dry-run",
        action="store_true",
        help="report would-be changes without writing anything",
    )
    crons_sync.set_defaults(handler=_crons_sync)

    outcome_guard = subparsers.add_parser(
        "outcome-guard",
        help="deterministic outcome contracts, child admission, and git enforcement",
    )
    outcome_guard_sub = outcome_guard.add_subparsers(
        dest="outcome_guard_command", required=True
    )
    register = outcome_guard_sub.add_parser(
        "register", help="register a validated immutable JSON contract"
    )
    register.add_argument("--config", type=Path, default=default_config_path())
    register.add_argument("--contract-file", type=Path, required=True)
    register.set_defaults(handler=_outcome_guard_register)
    check_effect = outcome_guard_sub.add_parser(
        "check-effect", help="check one requested effect and emit JSON"
    )
    check_effect.add_argument("--config", type=Path, default=default_config_path())
    check_effect.add_argument("--contract-ref", required=True)
    check_effect.add_argument("--effect", required=True)
    check_effect.set_defaults(handler=_outcome_guard_check_effect)
    check_outcome = outcome_guard_sub.add_parser(
        "check-outcome", help="check typed terminal evidence and emit JSON"
    )
    check_outcome.add_argument("--config", type=Path, default=default_config_path())
    check_outcome.add_argument("--contract-ref", required=True)
    check_outcome.add_argument("--evidence-file", type=Path, required=True)
    check_outcome.add_argument("--task-status")
    check_outcome.set_defaults(handler=_outcome_guard_check_outcome)
    admit_child = outcome_guard_sub.add_parser(
        "admit-child",
        help=(
            "HKRC-mediated child admission: create in blocked state via the "
            "native kanban CLI, validate the effect, record evidence, then promote"
        ),
    )
    admit_child.add_argument("--config", type=Path, default=default_config_path())
    admit_child.add_argument("--parent-task-id", required=True)
    admit_child.add_argument("--contract-ref", required=True)
    admit_child.add_argument("--effect", required=True)
    admit_child.add_argument("--board", required=True)
    admit_child.add_argument("--title", required=True)
    admit_child.add_argument("--assignee", required=True)
    admit_child.add_argument("--body")
    admit_child.set_defaults(handler=_outcome_guard_admit_child)
    authorize_merge = outcome_guard_sub.add_parser(
        "authorize-merge",
        help=(
            "bind a task, contract, and review evidence to a protected ref "
            "for the git reference-transaction hook"
        ),
    )
    authorize_merge.add_argument("--config", type=Path, default=default_config_path())
    authorize_merge.add_argument("--ref", default=PROTECTED_REF_DEFAULT)
    authorize_merge.add_argument("--task-id", required=True)
    authorize_merge.add_argument("--contract-ref", required=True)
    authorize_merge.add_argument(
        "--evidence-file",
        type=Path,
        help=(
            "typed terminal evidence JSON list; re-authorize to bind "
            "independent-review evidence later"
        ),
    )
    authorize_merge.set_defaults(handler=_outcome_guard_authorize_merge)
    git_hook = outcome_guard_sub.add_parser(
        "git-hook",
        help=(
            "portable reference-transaction adapter: run --state <state> when "
            "invoked by git, or manage the hook with install/uninstall/status"
        ),
    )
    git_hook.add_argument(
        "--state",
        choices=("prepared", "committed", "aborted"),
        help="transaction state passed by git as the single hook argument",
    )
    git_hook.add_argument("--config", type=Path, default=default_config_path())
    git_hook.add_argument(
        "--audit-only",
        action="store_true",
        help="report the decision on stdout without ever denying",
    )
    git_hook.set_defaults(handler=_outcome_guard_git_hook_run)
    git_hook_sub = git_hook.add_subparsers(dest="git_hook_command")
    git_hook_install = git_hook_sub.add_parser(
        "install", help="install the reference-transaction hook into a repo"
    )
    git_hook_install.add_argument("--config", type=Path, default=default_config_path())
    git_hook_install.add_argument("--repo", type=Path, default=Path.cwd())
    git_hook_install.add_argument(
        "--wrapper",
        type=Path,
        default=None,
        help="hkrc wrapper to embed (default: <instance-root>/bin/hkrc)",
    )
    git_hook_install.set_defaults(handler=_outcome_guard_git_hook_install)
    git_hook_uninstall = git_hook_sub.add_parser(
        "uninstall", help="remove the hook and restore any chained original"
    )
    git_hook_uninstall.add_argument("--config", type=Path, default=default_config_path())
    git_hook_uninstall.add_argument("--repo", type=Path, default=Path.cwd())
    git_hook_uninstall.set_defaults(handler=_outcome_guard_git_hook_uninstall)
    git_hook_status = git_hook_sub.add_parser(
        "status", help="report hook installation state without mutating anything"
    )
    git_hook_status.add_argument("--config", type=Path, default=default_config_path())
    git_hook_status.add_argument("--repo", type=Path, default=Path.cwd())
    git_hook_status.set_defaults(handler=_outcome_guard_git_hook_status)
    return parser


def _add_backfill_argument(parser: argparse.ArgumentParser) -> None:
    """Add the ``--backfill``/``--since`` recency-window override to a one-shot parser.

    A bare flag means full backfill (every blocked task); a duration value
    (``5h``, ``90m``, ``2d``, or plain seconds) replaces the effective recency
    window for this invocation.
    """

    parser.add_argument(
        "--backfill",
        "--since",
        dest="backfill",
        nargs="?",
        const="all",
        metavar="DURATION",
        type=_parse_backfill,
        help=(
            "widen the effective recency window beyond the configured "
            "[discovery] recency_window_seconds: seconds or a duration such as "
            "5h/90m/2d; bare --backfill includes every blocked task"
        ),
    )


def parse_duration(value: str) -> int:
    """Parse a duration such as ``3600``, ``5h``, ``90m``, ``2d`` or ``1w``."""

    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("duration must not be empty")
    if normalized.isdigit():
        seconds = int(normalized)
    else:
        match = re.fullmatch(r"(\d+)([smhdw])", normalized)
        if not match:
            raise ValueError(
                f"invalid duration {value!r}: use plain seconds or a unit suffix (s/m/h/d/w)"
            )
        amount = int(match.group(1))
        unit = match.group(2)
        seconds = amount * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
    if seconds <= 0:
        raise ValueError(f"invalid duration {value!r}: must be a positive number of seconds")
    return seconds


def _parse_backfill(value: str) -> int | str:
    """Convert a ``--backfill``/``--since`` value, preserving the bare-flag ``all``."""

    normalized = value.strip().lower()
    if normalized == "all":
        return "all"
    try:
        return parse_duration(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _effective_window(backfill: int | str | None, config_window: int) -> int | None:
    """Resolve the effective recency window for one discovery run.

    A bare ``--backfill``/``--since`` flag (``"all"``) disables the lower bound;
    a duration replaces the window; nothing means the configured window.  Only
    the full-backfill case yields ``None``.
    """

    if backfill is None:
        return config_window
    if backfill == "all":
        return None
    seconds = int(backfill)
    if seconds <= 0:
        raise ValueError(
            f"invalid backfill window {backfill!r}: must be a positive number of seconds"
        )
    return seconds


def _init(args: argparse.Namespace) -> int:
    base = default_config()
    config = ControllerConfig(
        instance_name=args.instance_name or base.instance_name,
        native_boards_root=args.native_boards_root or base.native_boards_root,
        state_db=args.state_db or base.state_db,
        workspace=args.workspace or base.workspace,
        native_cli=args.native_cli or base.native_cli,
        native_profile=args.native_profile or base.native_profile,
        telegram_chat_id=args.telegram_chat_id or base.telegram_chat_id,
        telegram_chat_id_env=args.telegram_chat_id_env or base.telegram_chat_id_env,
        telegram_chat_type=args.telegram_chat_type or base.telegram_chat_type,
        telegram_thread_id=args.telegram_thread_id or base.telegram_thread_id,
        telegram_user_id=args.telegram_user_id or base.telegram_user_id,
        telegram_notifier_profile=args.telegram_notifier_profile or base.telegram_notifier_profile,
        stream=StreamConfig(
            enabled=args.stream_enabled or base.stream.enabled,
            adapter=args.stream_adapter or base.stream.adapter,
            endpoint=args.stream_endpoint or base.stream.endpoint,
            boards=tuple(args.stream_boards or base.stream.boards),
            credential_env=args.stream_credential_env or base.stream.credential_env,
            current_state_reader=args.stream_current_state_reader or base.stream.current_state_reader,
            alert_after_consecutive_failures=(
                args.stream_alert_after_consecutive_failures
                if args.stream_alert_after_consecutive_failures is not None
                else base.stream.alert_after_consecutive_failures
            ),
        ),
        needs_input_watcher=NeedsInputWatcherConfig(
            enabled=args.needs_input_watcher_enabled or base.needs_input_watcher.enabled,
            min_block_seconds=(
                args.needs_input_watcher_min_block_seconds
                if args.needs_input_watcher_min_block_seconds is not None
                else base.needs_input_watcher.min_block_seconds
            ),
            llm_profile=(
                args.needs_input_watcher_llm_profile
                or base.needs_input_watcher.llm_profile
            ),
            prompt_template_path=(
                args.needs_input_watcher_prompt_template_path
                or base.needs_input_watcher.prompt_template_path
            ),
            timeout_seconds=(
                args.needs_input_watcher_timeout_seconds
                if args.needs_input_watcher_timeout_seconds is not None
                else base.needs_input_watcher.timeout_seconds
            ),
        ),
        review_gap=ReviewGapConfig(
            enabled=args.review_gap_enabled or base.review_gap.enabled,
            min_age_seconds=(
                args.review_gap_min_age_seconds
                if args.review_gap_min_age_seconds is not None
                else base.review_gap.min_age_seconds
            ),
            recency_hours=(
                args.review_gap_recency_hours
                if args.review_gap_recency_hours is not None
                else base.review_gap.recency_hours
            ),
            stalled_alert_hours=(
                args.review_gap_stalled_alert_hours
                if args.review_gap_stalled_alert_hours is not None
                else base.review_gap.stalled_alert_hours
            ),
            auto_create=args.review_gap_auto_create or base.review_gap.auto_create,
            trigger_c_enabled=(
                args.review_gap_trigger_c_enabled or base.review_gap.trigger_c_enabled
            ),
            trigger_d_enabled=(
                args.review_gap_trigger_d_enabled or base.review_gap.trigger_d_enabled
            ),
            cli_timeout_seconds=(
                args.review_gap_cli_timeout_seconds
                if args.review_gap_cli_timeout_seconds is not None
                else base.review_gap.cli_timeout_seconds
            ),
            tick_timeout_seconds=(
                args.review_gap_tick_timeout_seconds
                if args.review_gap_tick_timeout_seconds is not None
                else base.review_gap.tick_timeout_seconds
            ),
            max_workers=(
                args.review_gap_max_workers
                if args.review_gap_max_workers is not None
                else base.review_gap.max_workers
            ),
        ),
        watcher=WatcherConfig(
            enabled=args.watcher_enabled or base.watcher.enabled,
            reviewer_profiles=tuple(
                args.watcher_reviewer_profiles or base.watcher.reviewer_profiles
            ),
            fix_assignee=args.watcher_fix_assignee or base.watcher.fix_assignee,
            max_block_age_seconds=(
                args.watcher_max_block_age_seconds
                if args.watcher_max_block_age_seconds is not None
                else base.watcher.max_block_age_seconds
            ),
            canonical_branch=(
                args.watcher_canonical_branch or base.watcher.canonical_branch
            ),
            canonical_branch_fallback=(
                args.watcher_canonical_branch_fallback
                or base.watcher.canonical_branch_fallback
            ),
            hold_comment_window_seconds=(
                args.watcher_hold_comment_window_seconds
                if args.watcher_hold_comment_window_seconds is not None
                else base.watcher.hold_comment_window_seconds
            ),
            recv_timeout_seconds=(
                args.watcher_recv_timeout_seconds
                if args.watcher_recv_timeout_seconds is not None
                else base.watcher.recv_timeout_seconds
            ),
            cycle_interval_seconds=(
                args.watcher_cycle_interval_seconds
                if args.watcher_cycle_interval_seconds is not None
                else base.watcher.cycle_interval_seconds
            ),
            deadlock_min_age_seconds=(
                args.watcher_deadlock_min_age_seconds
                if args.watcher_deadlock_min_age_seconds is not None
                else base.watcher.deadlock_min_age_seconds
            ),
        ),
        harness_loop=HarnessLoopConfig(
            enabled=args.harness_loop_enabled or base.harness_loop.enabled,
            window_hours=(
                args.harness_loop_window_hours
                if args.harness_loop_window_hours is not None
                else base.harness_loop.window_hours
            ),
            max_applies=(
                args.harness_loop_max_applies
                if args.harness_loop_max_applies is not None
                else base.harness_loop.max_applies
            ),
            cooldown_days=(
                args.harness_loop_cooldown_days
                if args.harness_loop_cooldown_days is not None
                else base.harness_loop.cooldown_days
            ),
            bloat_threshold_tokens=(
                args.harness_loop_bloat_threshold_tokens
                if args.harness_loop_bloat_threshold_tokens is not None
                else base.harness_loop.bloat_threshold_tokens
            ),
            bloat_top_n=(
                args.harness_loop_bloat_top_n
                if args.harness_loop_bloat_top_n is not None
                else base.harness_loop.bloat_top_n
            ),
            sessions_db=(
                Path(args.harness_loop_sessions_db).expanduser()
                if args.harness_loop_sessions_db
                else base.harness_loop.sessions_db
            ),
            external_dirs=tuple(
                args.harness_loop_external_dirs or base.harness_loop.external_dirs
            ),
            hkrc_repo=(
                Path(args.harness_loop_hkrc_repo).expanduser()
                if args.harness_loop_hkrc_repo
                else base.harness_loop.hkrc_repo
            ),
        ),
    )
    write_config(args.config, config, overwrite=args.force)
    if config.workspace is not None:
        config.workspace.mkdir(parents=True, exist_ok=True)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        print(f"initialized instance={state.instance_name}")
        print(f"config={Path(args.config).expanduser()}")
        print(f"state_db={state.path}")
        print(f"native_boards_root={config.native_boards_root} (read-only boundary)")
    return 0


def _status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = ControllerState.open_existing(config.state_db)
    try:
        if state.instance_name != config.instance_name:
            raise StateError(
                f"state instance {state.instance_name!r} does not match config "
                f"{config.instance_name!r}"
            )
        print(f"instance={config.instance_name}")
        print(f"config={Path(args.config).expanduser()}")
        print(f"state_db={state.path}")
        print(f"schema_version={state.schema_version}")
        print(f"native_boards_root={config.native_boards_root} (not scanned)")
        print(f"stream_mode={config.stream.mode}")
        print(f"stream_enabled={str(config.stream.enabled).lower()}")
        print(f"needs_input_watcher_enabled={str(config.needs_input_watcher.enabled).lower()}")
    finally:
        state.close()
    return 0


def _discover(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with InstanceLock(config.lock_path):
        with ControllerState.open_existing(config.state_db) as state:
            _check_instance(config, state)
            window = _effective_window(args.backfill, config.recency_window_seconds)
            for resolution in discover_and_reserve(
                config.native_boards_root,
                state,
                now=args.now,
                unclaimed_after=config.unclaimed_child_after_seconds,
                window_seconds=window,
            ):
                print(resolution.stdout_line())
            for candidate in discover_stale_blockers(
                config.native_boards_root, now=args.now, window_seconds=window
            ):
                print(stale_blocker_note(candidate, now=args.now))
    return 0


def _run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with InstanceLock(config.lock_path):
        with ControllerState.open_existing(config.state_db) as state:
            _check_instance(config, state)
            report = execute_handoff(
                config,
                state,
                now=args.now,
                window_seconds=_effective_window(args.backfill, config.recency_window_seconds),
            )
    for line in report.lines:
        print(line)
    return report.exit_code


def _daemon(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if not config.stream.enabled:
        raise HandoffError(
            "continuous stream mode is disabled; use one-shot discover/run or "
            "configure [stream] enabled=true with an approved authenticated adapter"
        )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    adapters, credentials, current_state_reader, blocked_lister = build_live_stream_wiring(config)
    runtime = DaemonRuntime(
        config,
        event_batch_size=args.event_batch_size,
        stream_adapters=adapters,
        stream_credentials=credentials,
        current_state_reader=current_state_reader,
        wiring_builder=lambda: build_live_stream_wiring(config),
        reconcile_interval_cycles=config.stream.reconcile_interval_cycles,
    )
    return runtime.run(max_cycles=args.max_cycles)


def _needs_input_watcher(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_file = args.state_file or default_state_path(config.state_db)
    message = run_needs_input_watcher(
        config.native_boards_root,
        state_file,
        config,
        now=getattr(args, "now", None),
    )
    if message:
        print(message)
    return 0


def _stale_block_watch(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_file = args.state_file or stale_block_watch_default_state_path(config.state_db)
    message = run_stale_block_watch(
        config,
        state_file,
        timeout=getattr(args, "timeout", None),
    )
    if message:
        print(message)
    return 0


def _review_gap(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_file = args.state_file or review_gap_default_state_path(config.state_db)
    digest = run_review_gap(config, state_file, now=getattr(args, "now", None))
    if digest:
        print(digest)
    return 0


def _watcher(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    adapters, credentials = build_watcher_wiring(config)
    state_file = args.state_file or default_watcher_state_path(config.state_db)
    actions, message = run_watcher(
        config,
        state_path=state_file,
        dry_run=args.dry_run,
        now=getattr(args, "now", None),
        replay=args.replay,
        adapters=adapters,
        credentials=credentials,
    )
    if message:
        print(message)
    return 0


def _harness_loop(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state_file = args.state_file or harness_loop_default_state_path(config.state_db)
    report = run_harness_loop(
        config,
        now=getattr(args, "now", None),
        dry_run=args.dry_run,
        state_path=state_file,
    )
    if report:
        print(report)
    return 0


def _harness_loop_simulate(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    result = run_simulation(
        config,
        now=getattr(args, "now", None),
        shadow_dir=args.shadow_dir,
    )
    print(result.report)
    return 0 if result.passed else 2


def _crons_sync(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    manifest_path = args.manifest or default_manifest_path(args.config)
    run_sync(config, manifest_path, dry_run=args.dry_run)
    return 0


def _outcome_guard_check_effect(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with ControllerState.open_read_only(config.state_db) as state:
        _check_instance(config, state)
        result = OutcomeGuard(state).check_effect(args.contract_ref, args.effect)
    print(json.dumps(result.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.allowed else 3


def _outcome_guard_check_outcome(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    try:
        evidence = json.loads(args.evidence_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OutcomeGuardError(f"evidence file is not valid JSON: {exc.msg}") from exc
    if not isinstance(evidence, list):
        raise OutcomeGuardError("evidence file must contain a JSON list")
    with ControllerState.open_read_only(config.state_db) as state:
        _check_instance(config, state)
        result = OutcomeGuard(state).check_outcome(
            args.contract_ref, evidence=evidence, task_status=args.task_status
        )
    print(json.dumps(result.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.allowed else 3


def _outcome_guard_register(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    try:
        document = json.loads(args.contract_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OutcomeGuardError(f"contract file is not valid JSON: {exc.msg}") from exc
    if not isinstance(document, dict):
        raise OutcomeGuardError("contract file must contain a JSON object")
    with ControllerState.open_existing(config.state_db) as state:
        _check_instance(config, state)
        result = OutcomeGuard(state).register_contract(document)
    print(json.dumps(result.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if result.allowed else 3


def _outcome_guard_admit_child(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with ControllerState.open_existing(config.state_db) as state:
        _check_instance(config, state)
        report = admit_child(
            config,
            state,
            parent_task_id=args.parent_task_id,
            contract_ref=args.contract_ref,
            effect=args.effect,
            board_slug=args.board,
            title=args.title,
            assignee=args.assignee,
            body=args.body,
        )
    print(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if report.allowed else 3


def _outcome_guard_authorize_merge(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if not args.ref.startswith("refs/"):
        raise OutcomeGuardError("--ref must be a refs path starting with 'refs/'")
    evidence: list[dict[str, object]] = []
    if args.evidence_file is not None:
        try:
            raw = json.loads(args.evidence_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise OutcomeGuardError(f"evidence file is not valid JSON: {exc.msg}") from exc
        if not isinstance(raw, list):
            raise OutcomeGuardError("evidence file must contain a JSON list")
        evidence = raw
    with ControllerState.open_existing(config.state_db) as state:
        _check_instance(config, state)
        guard = OutcomeGuard(state)
        effect = guard.check_effect(args.contract_ref, "merge_main")
        if not effect.allowed:
            print(json.dumps(effect.to_dict(), sort_keys=True, separators=(",", ":")))
            return 3
        outcome = guard.check_outcome(args.contract_ref, evidence=evidence)
        evidence_json = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
        state.connection.execute(
            """
            INSERT INTO outcome_merge_authorizations
                (ref, task_id, contract_ref, evidence_json, authorized_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(ref, task_id) DO UPDATE SET
                contract_ref = excluded.contract_ref,
                evidence_json = excluded.evidence_json,
                authorized_at = excluded.authorized_at
            """,
            (
                args.ref,
                args.task_id,
                args.contract_ref,
                evidence_json,
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ),
        )
        state.connection.commit()
    result = {
        "schema_version": "hkrc.merge-authorization.v1",
        "allowed": True,
        "reason_code": "merge_authorized",
        "ref": args.ref,
        "task_id": args.task_id,
        "contract_ref": args.contract_ref,
        "outcome_reached": outcome.outcome_reached,
        "missing_evidence_refs": list(outcome.missing_evidence_refs),
    }
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


def _outcome_guard_git_hook_run(args: argparse.Namespace) -> int:
    if args.state is None:
        raise OutcomeGuardError(
            "git-hook requires --state (prepared/committed/aborted) or a "
            "subcommand (install/uninstall/status)"
        )
    return run_hook_command(args.config, hook_state=args.state, audit_only=args.audit_only)


def _outcome_guard_git_hook_install(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    wrapper = args.wrapper
    if wrapper is None:
        wrapper = Path(args.config).resolve().parents[2] / "bin" / "hkrc"
    for line in install_hook(
        args.repo, wrapper=wrapper, config_path=args.config, config=config
    ):
        print(line)
    return 0


def _outcome_guard_git_hook_uninstall(args: argparse.Namespace) -> int:
    for line in uninstall_hook(args.repo):
        print(line)
    return 0


def _outcome_guard_git_hook_status(args: argparse.Namespace) -> int:
    for line in hook_status(args.repo):
        print(line)
    return 0


def _check_instance(config: ControllerConfig, state: ControllerState) -> None:
    if state.instance_name != config.instance_name:
        raise StateError(
            f"state instance {state.instance_name!r} does not match config "
            f"{config.instance_name!r}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (AdmissionError, ConfigError, CronManifestError, DiscoveryError, GitEnforceError, HandoffError, LockError, StateError, sqlite3.Error, OSError, NeedsInputWatcherError, ReviewGapError, WatcherError, HarnessLoopError, StaleBlockWatchError, OutcomeGuardError) as exc:
        print(f"hkrc: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
