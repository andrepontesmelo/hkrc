"""Instance-scoped configuration for the controller.

The configuration deliberately names the native Hermes boards root but never
creates, opens, or mutates native files.  Runtime state belongs to the
controller-owned SQLite path instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import math
import os
from pathlib import Path
import re
import tomllib
from urllib.parse import urlsplit

from .harness_loop import (
    ACTIONABLE_SKIP_CLASSES,
    ARCHLOOP_HIGH_NIGHTS,
    ARCHLOOP_MEDIUM_NIGHTS,
    DEFAULT_ARCHLOOP_OUTPUT_DIR,
    DECISION_LATENCY_HUMAN_SECONDS,
    DECISION_LATENCY_SECONDS,
    DEFAULT_DIST_SKILLS_ROOT,
    DEFAULT_PROFILES_ROOT,
    HarnessLoopConfig,
)


class ConfigError(ValueError):
    """Raised when a controller configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class AssistConfig:
    """Configuration for the recommendation-only HKRC Assist sidecar."""

    human_in_loop: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.human_in_loop, bool):
            raise ConfigError("assist human_in_loop must be a boolean")


@dataclass(frozen=True, slots=True)
class OutcomeGuardConfig:
    """Policy enforcement surface for the deterministic outcome guard.

    ``protected_refs`` names the canonical refs the portable Git
    ``reference-transaction`` hook refuses to update unless an operator has
    recorded a merge authorization binding a task, contract, and review
    evidence in controller-owned state. The default is ``refs/heads/main``;
    a release operation never silently enables enforcement, and the hook is
    only active after an explicit ``outcome-guard git-hook install``.
    """

    protected_refs: tuple[str, ...] = ("refs/heads/main",)

    def __post_init__(self) -> None:
        if not isinstance(self.protected_refs, tuple):
            raise ConfigError("outcome_guard protected_refs must be an array of strings")
        for ref in self.protected_refs:
            if not isinstance(ref, str) or not ref.strip() or not ref.startswith("refs/"):
                raise ConfigError(
                    "outcome_guard protected_refs entries must be refs paths "
                    "starting with 'refs/'"
                )
        if len(set(self.protected_refs)) != len(self.protected_refs):
            raise ConfigError("outcome_guard protected_refs must not contain duplicates")


_INSTANCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_BOARD_SLUG = re.compile(r"^[A-Za-z0-9._-]+$")
_STREAM_ADAPTERS = frozenset({"none", "approved_websocket"})


def _is_positive_number(value: object, *, integers_only: bool = False) -> bool:
    """Return True when ``value`` is a positive finite number, never a bool.

    Python's ``bool`` subclasses ``int``, so ``isinstance(True, int)`` is
    true; numeric config values must reject booleans explicitly.  When
    ``integers_only`` is set, floats (and therefore ``True``/``False``) are
    rejected as well.
    """
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    if integers_only and not isinstance(value, int):
        return False
    return math.isfinite(value) and value > 0


def default_config_path() -> Path:
    """Return the user-local config path for the current Hermes instance."""

    installed_root = os.environ.get("HKRC_INSTANCE_ROOT", "").strip()
    if installed_root:
        return Path(installed_root).expanduser() / "config" / "hkrc" / "config.toml"
    return Path.home() / ".config" / "hermes-kanban-recovery-controller" / "config.toml"


def default_config() -> "ControllerConfig":
    """Build a safe default without inspecting the native Hermes installation."""

    instance_root = default_instance_root()
    return ControllerConfig(
        instance_name="default",
        native_boards_root=instance_root / "kanban" / "boards",
        state_db=instance_root / "state" / "hkrc" / "state.sqlite3",
        workspace=instance_root / "workspace" / "hkrc",
        telegram_chat_id_env="HKRC_TELEGRAM_CHAT_ID",
    )


def default_instance_root() -> Path:
    """Return the active Hermes instance root without inspecting its contents."""

    configured = os.environ.get("HKRC_INSTANCE_ROOT", "").strip()
    return Path(configured).expanduser() if configured else Path.home() / ".hermes"


@dataclass(frozen=True, slots=True)
class StreamConfig:
    """Declarative gate for the optional continuous stream integration.

    This is deliberately only a configuration contract.  ``approved_websocket``
    identifies an externally injected, authenticated adapter; it does not make
    the CLI discover an endpoint, credential, or current-state reader.  The
    daemon must fail closed when that wiring is absent.
    """

    enabled: bool = False
    adapter: str = "none"
    endpoint: str | None = None
    # Empty means "observe every non-archived board at runtime" (resolved
    # from the read-only native boards root); non-empty is an explicit
    # allowlist.  The daemon re-resolves the set on every cycle.
    boards: tuple[str, ...] = ()
    credential_env: str | None = None
    current_state_reader: str | None = None
    # Self-health threshold: a board that records this many consecutive
    # transport/auth failures without a successfully accepted frame triggers
    # one journald alert per outage episode (deduped until the stream
    # resumes).  Alerts never leave the journal (2026-08-11 operator mute);
    # no Telegram send is attempted.
    alert_after_consecutive_failures: int = 3
    # Blocked-state reconcile sweep cadence: every N cycles the daemon lists
    # ``status='blocked'`` per board through the CLI and reserves the silent
    # death class (latest event is a dispatcher death kind) that the stream
    # never delivered.  0 disables the sweep (event-driven only).
    reconcile_interval_cycles: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ConfigError("stream enabled must be a boolean")
        if not isinstance(self.reconcile_interval_cycles, int) or isinstance(
            self.reconcile_interval_cycles, bool
        ) or self.reconcile_interval_cycles < 0:
            raise ConfigError("stream reconcile_interval_cycles must be a non-negative integer")
        if not isinstance(self.adapter, str) or self.adapter not in _STREAM_ADAPTERS:
            raise ConfigError(
                "stream adapter must be 'none' or 'approved_websocket'"
            )
        if not isinstance(self.boards, tuple):
            raise ConfigError("stream boards must be a tuple of strings")
        if any(
            not isinstance(board, str) or not _BOARD_SLUG.fullmatch(board)
            for board in self.boards
        ):
            raise ConfigError(
                "stream boards must be non-empty slugs of letters, numbers, "
                "'.', '_', or '-'"
            )
        if len(set(self.boards)) != len(self.boards):
            raise ConfigError("stream boards must not contain duplicates")
        if self.endpoint is not None and (
            not isinstance(self.endpoint, str) or not self.endpoint.strip()
        ):
            raise ConfigError("stream endpoint must be a non-empty string or null")
        if self.endpoint is not None:
            try:
                endpoint = urlsplit(self.endpoint)
                hostname = endpoint.hostname
            except ValueError as exc:
                raise ConfigError(
                    "stream endpoint must be an absolute wss:// URL or loopback ws:// URL"
                ) from exc
            is_secure = endpoint.scheme == "wss" and bool(endpoint.netloc)
            is_loopback = (
                endpoint.scheme == "ws"
                and bool(endpoint.netloc)
                and _is_loopback_hostname(hostname)
            )
            if not (is_secure or is_loopback):
                raise ConfigError(
                    "stream endpoint must be an absolute wss:// URL or loopback ws:// URL"
                )
        if self.credential_env is not None and (
            not isinstance(self.credential_env, str)
            or not _ENV_NAME.fullmatch(self.credential_env)
        ):
            raise ConfigError("stream credential_env must be a valid environment variable name")
        if self.current_state_reader is not None and (
            not isinstance(self.current_state_reader, str)
            or not self.current_state_reader.strip()
        ):
            raise ConfigError(
                "stream current_state_reader must be a non-empty string or null"
            )
        if not _is_positive_number(
            self.alert_after_consecutive_failures, integers_only=True
        ):
            raise ConfigError(
                "stream alert_after_consecutive_failures must be a positive integer"
            )
        if self.enabled:
            missing = [
                name
                for name, value in (
                    ("adapter", self.adapter == "approved_websocket"),
                    ("endpoint", bool(self.endpoint)),
                    ("credential_env", bool(self.credential_env)),
                    ("current_state_reader", bool(self.current_state_reader)),
                )
                if not value
            ]
            if missing:
                raise ConfigError(
                    "enabled stream mode requires approved_websocket, endpoint, "
                    "credential_env, and current_state_reader; "
                    f"missing {', '.join(missing)}"
                )
        elif self.adapter != "none":
            raise ConfigError(
                "stream adapter is configured but stream is disabled; use "
                "enabled = true only after approved adapter wiring is ready"
            )

    @property
    def mode(self) -> str:
        """Return a stable human-facing mode label."""

        return "approved_websocket" if self.enabled else "manual_compatibility"


def _is_loopback_hostname(hostname: str | None) -> bool:
    if not hostname:
        return False
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True, slots=True)
class NeedsInputWatcherConfig:
    """Gate and tunables for the human-input blocker watchdog.

    ``enabled`` defaults to ``True`` so the pre-existing cron ``no_agent``
    contract keeps pinging on an unmodified config; set it to ``false`` to
    silence the watchdog.  ``llm_profile`` selects the cheap text profile used
    for one summarizer call per fresh blocked episode; an empty value keeps
    the deterministic fallback line (no LLM invocation).  Paths are
    instance-local: the prompt template lives under the configured instance
    root, never inside native Hermes data.

    There is deliberately no ``max_turns`` option: the summarizer invocation
    contract fixes ``--max-turns 4`` (``hermes -p <llm_profile> chat -q
    <prompt> --max-turns 4 --yolo -Q --reasoning none``), so exposing a config
    knob here would contradict the documented product contract.  The four-turn
    budget lets the tool-using summarizer complete its one ``kanban show``
    lookup without exhausting the run; ``-Q`` and ``--reasoning none`` keep
    stdout down to the final response.
    """

    enabled: bool = True
    min_block_seconds: int | float = 300
    llm_profile: str = ""
    prompt_template_path: str | None = None
    timeout_seconds: int | float = 90

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ConfigError("needs_input_watcher enabled must be a boolean")
        if not _is_positive_number(self.min_block_seconds):
            raise ConfigError("needs_input_watcher min_block_seconds must be a positive number")
        if not isinstance(self.llm_profile, str):
            raise ConfigError("needs_input_watcher llm_profile must be a string")
        if self.prompt_template_path is not None and (
            not isinstance(self.prompt_template_path, str)
            or not self.prompt_template_path.strip()
        ):
            raise ConfigError(
                "needs_input_watcher prompt_template_path must be a non-empty string or null"
            )
        if not _is_positive_number(self.timeout_seconds):
            raise ConfigError("needs_input_watcher timeout_seconds must be a positive number")


@dataclass(frozen=True, slots=True)
class ReviewGapConfig:
    """Gate and tunables for the review-gap watchdog.

    The watchdog is the deterministic backstop for done implementation/fix
    cards whose paired review card was never created: it auto-creates the
    missing review card (``auto_create``) or, when a review exists but has
    stalled unmerged, emits an alert line. A DONE review is verified by git
    truth: the impl commit must be an ancestor of the canonical branch
    (``origin/<default>`` when resolvable, local ``<default>`` otherwise),
    else the review is a *stalled merge* (deferred-merge loose end) and —
    after ``stalled_alert_hours`` — a re-validation review card is
    auto-created (or, with ``auto_create`` disabled, an alert is emitted).
    ``min_age_seconds`` prevents racing
    the worker that just completed the card (it may still be creating the
    review pair itself); ``recency_hours`` bounds how far back completed cards
    are still considered. ``trigger_c_enabled`` gates the third trigger: a
    blocked ``review-required:`` parent whose work shipped on an unmerged
    ``wt/<task_id>`` branch and already has a review child is auto-completed so
    the review child promotes (the reviewer still gates the merge).
    ``trigger_d_enabled`` gates the fourth trigger: a revert of a kanban merge
    on canonical main with no re-merge after it gets a ``re-apply reverted
    change`` card (assignee reviewer, parent = the original task) so the
    reverted work never sits off main silently.

    The bounded-time knobs (0.13.1) keep the full tick predictable on the
    live board set: every native CLI/git subprocess is capped by
    ``cli_timeout_seconds`` (a hung ``hermes kanban`` call cannot hang the
    tick), the whole tick is capped by ``tick_timeout_seconds`` (when the
    budget is exhausted the remaining boards are skipped with an alert line,
    and the dedupe state carries progress to the next tick), and the
    read-bound per-candidate work (show + child shows + git checks) runs in
    parallel with ``max_workers`` subprocesses.
    """

    enabled: bool = True
    min_age_seconds: int | float = 300
    recency_hours: int | float = 48
    stalled_alert_hours: int | float = 6
    auto_create: bool = True
    trigger_c_enabled: bool = True
    trigger_d_enabled: bool = True
    cli_timeout_seconds: int | float = 30.0
    tick_timeout_seconds: int | float = 120.0
    max_workers: int = 16

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ConfigError("review_gap enabled must be a boolean")
        if not _is_positive_number(self.min_age_seconds):
            raise ConfigError("review_gap min_age_seconds must be a positive number")
        if not _is_positive_number(self.recency_hours):
            raise ConfigError("review_gap recency_hours must be a positive number")
        if not _is_positive_number(self.stalled_alert_hours):
            raise ConfigError("review_gap stalled_alert_hours must be a positive number")
        if not isinstance(self.auto_create, bool):
            raise ConfigError("review_gap auto_create must be a boolean")
        if not isinstance(self.trigger_c_enabled, bool):
            raise ConfigError("review_gap trigger_c_enabled must be a boolean")
        if not isinstance(self.trigger_d_enabled, bool):
            raise ConfigError("review_gap trigger_d_enabled must be a boolean")
        if not _is_positive_number(self.cli_timeout_seconds):
            raise ConfigError("review_gap cli_timeout_seconds must be a positive number")
        if not _is_positive_number(self.tick_timeout_seconds):
            raise ConfigError("review_gap tick_timeout_seconds must be a positive number")
        if not _is_positive_number(self.max_workers, integers_only=True):
            raise ConfigError("review_gap max_workers must be a positive integer")


@dataclass(frozen=True, slots=True)
class WatcherConfig:
    """Gate and tunables for the decision-latency watcher (v0.9.0).

    ``enabled`` defaults to ``True`` so an unmodified config keeps the
    watcher command usable; set it to ``false`` to make ``hkrc watcher``
    exit silently without scanning.  ``reviewer_profiles`` is an explicit
    allowlist; when empty the reviewer-profile test falls back to a
    profile name containing ``reviewer``.  ``max_block_age_seconds`` is the
    H1 recency window: a defect block older than this is never acted on in
    live mode (replay/dry-run mode deliberately ignores it so historical
    stall patterns are still reported).  ``canonical_branch`` names the
    merge-verification branch checked first, then
    ``canonical_branch_fallback``.  ``recv_timeout_seconds`` is the
    WebSocket read timeout and must stay strictly below the cron cycle
    interval (``cycle_interval_seconds``) so an idle board cannot hold the
    connection past the watcher's cycle lifetime (the #77833 leak rule).
    ``deadlock_min_age_seconds`` is the H5 debounce: a ``review-required:``
    block episode must be at least this old before the promotion-deadlock
    archive may fire (a freshly blocked parent may still complete on its
    own, so the watcher never races it).
    """

    enabled: bool = True
    reviewer_profiles: tuple[str, ...] = ()
    fix_assignee: str = "developer"
    max_block_age_seconds: int | float = 1800
    canonical_branch: str = "main"
    canonical_branch_fallback: str = "master"
    hold_comment_window_seconds: int | float = 3600
    pick_gate_prefix: str = "One-at-a-time:"
    guard_reason: str = "blocked on creation without event — queueing guard"
    recv_timeout_seconds: int | float = 10.0
    # The cron cycle interval the watcher runs under (the no_agent shim
    # registers ``every 5m`` by default).  ``recv_timeout_seconds`` must stay
    # strictly below it per the #77833 leak rule: an idle board socket must
    # not hold the watcher past its cycle lifetime.
    cycle_interval_seconds: int | float = 300.0
    # H5 debounce: a ``review-required:`` block episode must be at least this
    # old before the promotion-deadlock archive fires (the worker that just
    # blocked may still complete on its own; the watcher never races it).
    deadlock_min_age_seconds: int | float = 900

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ConfigError("watcher enabled must be a boolean")
        if not isinstance(self.reviewer_profiles, tuple):
            raise ConfigError("watcher reviewer_profiles must be a tuple of strings")
        if any(not isinstance(profile, str) or not profile.strip() for profile in self.reviewer_profiles):
            raise ConfigError("watcher reviewer_profiles must contain non-empty strings")
        if len(set(self.reviewer_profiles)) != len(self.reviewer_profiles):
            raise ConfigError("watcher reviewer_profiles must not contain duplicates")
        if not isinstance(self.fix_assignee, str) or not self.fix_assignee.strip():
            raise ConfigError("watcher fix_assignee must be a non-empty string")
        if not _is_positive_number(self.max_block_age_seconds):
            raise ConfigError("watcher max_block_age_seconds must be a positive number")
        if not isinstance(self.canonical_branch, str) or not self.canonical_branch.strip():
            raise ConfigError("watcher canonical_branch must be a non-empty string")
        if not isinstance(self.canonical_branch_fallback, str) or not self.canonical_branch_fallback.strip():
            raise ConfigError("watcher canonical_branch_fallback must be a non-empty string")
        if self.canonical_branch == self.canonical_branch_fallback:
            raise ConfigError("watcher canonical_branch and fallback must differ")
        if not _is_positive_number(self.hold_comment_window_seconds):
            raise ConfigError("watcher hold_comment_window_seconds must be a positive number")
        if not isinstance(self.pick_gate_prefix, str) or not self.pick_gate_prefix.strip():
            raise ConfigError("watcher pick_gate_prefix must be a non-empty string")
        if not isinstance(self.guard_reason, str) or not self.guard_reason.strip():
            raise ConfigError("watcher guard_reason must be a non-empty string")
        if not _is_positive_number(self.recv_timeout_seconds):
            raise ConfigError("watcher recv_timeout_seconds must be a positive number")
        if not _is_positive_number(self.cycle_interval_seconds):
            raise ConfigError("watcher cycle_interval_seconds must be a positive number")
        if self.recv_timeout_seconds >= self.cycle_interval_seconds:
            raise ConfigError(
                "watcher recv_timeout_seconds must be below the cron cycle "
                "interval (cycle_interval_seconds): an idle board socket would "
                "otherwise hold the watcher past its cycle lifetime (#77833)"
            )
        if not _is_positive_number(self.deadlock_min_age_seconds):
            raise ConfigError("watcher deadlock_min_age_seconds must be a positive number")


@dataclass(frozen=True, slots=True)
class ControllerConfig:
    """Configuration scoped to one Hermes instance.

    ``native_boards_root`` is a read-only integration boundary for later
    discovery work.  ``state_db`` is owned exclusively by this controller.
    """

    instance_name: str
    native_boards_root: Path
    state_db: Path
    format_version: int = 1
    native_cli: str = "hermes"
    native_profile: str | None = None
    telegram_chat_id: str = ""
    telegram_chat_id_env: str | None = None
    telegram_chat_type: str = "dm"
    telegram_thread_id: str | None = None
    telegram_user_id: str | None = None
    telegram_notifier_profile: str | None = None
    workspace: Path | None = None
    unclaimed_child_after_seconds: int = 1800
    recency_window_seconds: int = 3600
    stream: StreamConfig = field(default_factory=StreamConfig)
    needs_input_watcher: NeedsInputWatcherConfig = field(default_factory=NeedsInputWatcherConfig)
    review_gap: ReviewGapConfig = field(default_factory=ReviewGapConfig)
    watcher: WatcherConfig = field(default_factory=WatcherConfig)
    harness_loop: HarnessLoopConfig = field(default_factory=HarnessLoopConfig)
    assist: AssistConfig = field(default_factory=AssistConfig)
    outcome_guard: OutcomeGuardConfig = field(default_factory=OutcomeGuardConfig)

    def __post_init__(self) -> None:
        if isinstance(self.format_version, bool) or self.format_version != 1:
            raise ConfigError(f"unsupported config format_version: {self.format_version}")
        if not _INSTANCE_NAME.fullmatch(self.instance_name):
            raise ConfigError(
                "instance_name must be 1-64 characters of letters, numbers, '.', '_', or '-'; "
                "it must start with a letter or number"
            )
        if not self.native_boards_root:
            raise ConfigError("native_boards_root must not be empty")
        if not self.state_db:
            raise ConfigError("state_db must not be empty")
        if self.workspace is not None and not self.workspace:
            raise ConfigError("workspace must not be empty")
        if not _is_positive_number(self.unclaimed_child_after_seconds, integers_only=True):
            raise ConfigError("unclaimed_child_after_seconds must be a positive integer")
        if not _is_positive_number(self.recency_window_seconds, integers_only=True):
            raise ConfigError("recency_window_seconds must be a positive integer")
        if not self.native_cli.strip():
            raise ConfigError("native_cli must not be empty")
        if not self.telegram_chat_type.strip():
            raise ConfigError("telegram_chat_type must not be empty")
        if self.telegram_chat_id_env and not _ENV_NAME.fullmatch(self.telegram_chat_id_env):
            raise ConfigError("telegram_chat_id_env must be a valid environment variable name")

    @property
    def lock_path(self) -> Path:
        """Return the controller-owned advisory lock path for this instance."""

        return self.state_db.parent / "controller.lock"

    def as_toml(self) -> str:
        """Serialize deterministically for a human-editable config file."""

        return (
            "# Hermes Kanban Recovery Controller configuration\n"
            "# The native boards root is an integration boundary; never write to it.\n"
            f"format_version = {self.format_version}\n\n"
            "[instance]\n"
            f"name = {_toml_string(self.instance_name)}\n"
            f"native_boards_root = {_toml_string(str(self.native_boards_root))}\n\n"
            "[controller]\n"
            f"state_db = {_toml_string(str(self.state_db))}\n"
            f"workspace = {_toml_optional_string(str(self.workspace) if self.workspace else None)}\n"
            "\n[native]\n"
            f"cli = {_toml_string(self.native_cli)}\n"
            f"profile = {_toml_optional_string(self.native_profile)}\n"
            "\n[telegram]\n"
            f"chat_id = {_toml_string(self.telegram_chat_id)}\n"
            f"chat_id_env = {_toml_optional_string(self.telegram_chat_id_env)}\n"
            f"chat_type = {_toml_string(self.telegram_chat_type)}\n"
            f"thread_id = {_toml_optional_string(self.telegram_thread_id)}\n"
            f"user_id = {_toml_optional_string(self.telegram_user_id)}\n"
            f"notifier_profile = {_toml_optional_string(self.telegram_notifier_profile)}\n"
            "\n[discovery]\n"
            f"unclaimed_child_after_seconds = {self.unclaimed_child_after_seconds}\n"
            f"recency_window_seconds = {self.recency_window_seconds}\n"
            "\n[stream]\n"
            f"enabled = {'true' if self.stream.enabled else 'false'}\n"
            f"adapter = {_toml_string(self.stream.adapter)}\n"
            f"endpoint = {_toml_optional_string(self.stream.endpoint)}\n"
            f"boards = {_toml_string_array(self.stream.boards)}  # empty = all non-archived boards\n"
            f"credential_env = {_toml_optional_string(self.stream.credential_env)}\n"
            f"current_state_reader = {_toml_optional_string(self.stream.current_state_reader)}\n"
            f"alert_after_consecutive_failures = {self.stream.alert_after_consecutive_failures}\n"
            f"reconcile_interval_cycles = {self.stream.reconcile_interval_cycles}  # 0 = event-driven only\n"
            "\n[needs_input_watcher]\n"
            f"enabled = {'true' if self.needs_input_watcher.enabled else 'false'}\n"
            f"min_block_seconds = {self.needs_input_watcher.min_block_seconds}\n"
            f"llm_profile = {_toml_string(self.needs_input_watcher.llm_profile)}\n"
            f"prompt_template_path = {_toml_optional_string(self.needs_input_watcher.prompt_template_path)}\n"
            f"timeout_seconds = {self.needs_input_watcher.timeout_seconds}\n"
            "\n[review_gap]\n"
            f"enabled = {'true' if self.review_gap.enabled else 'false'}\n"
            f"min_age_seconds = {self.review_gap.min_age_seconds}\n"
            f"recency_hours = {self.review_gap.recency_hours}\n"
            f"stalled_alert_hours = {self.review_gap.stalled_alert_hours}\n"
            f"auto_create = {'true' if self.review_gap.auto_create else 'false'}\n"
            f"trigger_c_enabled = {'true' if self.review_gap.trigger_c_enabled else 'false'}\n"
            f"trigger_d_enabled = {'true' if self.review_gap.trigger_d_enabled else 'false'}\n"
            f"cli_timeout_seconds = {self.review_gap.cli_timeout_seconds}\n"
            f"tick_timeout_seconds = {self.review_gap.tick_timeout_seconds}\n"
            f"max_workers = {self.review_gap.max_workers}\n"
            "\n[watcher]\n"
            f"enabled = {'true' if self.watcher.enabled else 'false'}\n"
            f"reviewer_profiles = {_toml_string_array(self.watcher.reviewer_profiles)}  # empty = assignee contains 'reviewer'\n"
            f"fix_assignee = {_toml_string(self.watcher.fix_assignee)}\n"
            f"max_block_age_seconds = {self.watcher.max_block_age_seconds}\n"
            f"canonical_branch = {_toml_string(self.watcher.canonical_branch)}\n"
            f"canonical_branch_fallback = {_toml_string(self.watcher.canonical_branch_fallback)}\n"
            f"hold_comment_window_seconds = {self.watcher.hold_comment_window_seconds}\n"
            f"pick_gate_prefix = {_toml_string(self.watcher.pick_gate_prefix)}\n"
            f"guard_reason = {_toml_string(self.watcher.guard_reason)}\n"
            f"recv_timeout_seconds = {self.watcher.recv_timeout_seconds}\n"
            f"cycle_interval_seconds = {self.watcher.cycle_interval_seconds}  # cron cycle; recv_timeout must stay below\n"
            f"deadlock_min_age_seconds = {self.watcher.deadlock_min_age_seconds}  # H5 review-required deadlock debounce\n"
            "\n[harness_loop]\n"
            f"enabled = {'true' if self.harness_loop.enabled else 'false'}\n"
            f"window_hours = {self.harness_loop.window_hours}\n"
            f"max_applies = {self.harness_loop.max_applies}  # 1 hkrc ticket pair max per run (orchestration is scope-gate rejected)\n"
            f"cooldown_days = {self.harness_loop.cooldown_days}\n"
            f"bloat_threshold_tokens = {self.harness_loop.bloat_threshold_tokens}\n"
            f"bloat_top_n = {self.harness_loop.bloat_top_n}\n"
            f"sessions_db = {_toml_optional_string(str(self.harness_loop.sessions_db) if self.harness_loop.sessions_db else None)}\n"
            f"external_dirs = {_toml_string_array(self.harness_loop.external_dirs)}  # empty = git dist default\n"
            f"dist_skills_root = {_toml_string(self.harness_loop.dist_skills_root)}  # worker-profile skill dist (pin sweep reads it)\n"
            f"profiles_root = {_toml_string(self.harness_loop.profiles_root)}  # empty = HKRC_PROFILES_ROOT env, else {DEFAULT_PROFILES_ROOT}\n"
            f"config_drift_allowed_profiles = {_toml_string_array(self.harness_loop.config_drift_allowed_profiles)}  # deliberate model.default pins; empty = flag all divergence\n"
            f"hkrc_repo = {_toml_optional_string(str(self.harness_loop.hkrc_repo) if self.harness_loop.hkrc_repo else None)}\n"
            f"analysis_profile = {_toml_string(self.harness_loop.analysis_profile)}  # empty = authoritative analysis disabled\n"
            f"analysis_timeout_seconds = {self.harness_loop.analysis_timeout_seconds}\n"
            f"analysis_max_attempts = {self.harness_loop.analysis_max_attempts}\n"
            f"escalate_after_nights = {self.harness_loop.escalate_after_nights}\n"
            f"chronic_after_nights = {self.harness_loop.chronic_after_nights}\n"
            f"stale_retention_days = {self.harness_loop.stale_retention_days}\n"
            f"archloop_output_dir = {_toml_string(self.harness_loop.archloop_output_dir)}  # empty = HKRC_ARCHLOOP_OUTPUT_DIR env, else {DEFAULT_ARCHLOOP_OUTPUT_DIR} (enabled)\n"
            f"archloop_actionable_classes = {_toml_string_array(self.harness_loop.archloop_actionable_classes)}  # skip classes that escalate\n"
            f"archloop_medium_nights = {self.harness_loop.archloop_medium_nights}\n"
            f"archloop_high_nights = {self.harness_loop.archloop_high_nights}\n"
            f"decision_latency_seconds = {self.harness_loop.decision_latency_seconds}  # machine-blocked defect threshold\n"
            f"decision_latency_human_seconds = {self.harness_loop.decision_latency_human_seconds}  # needs_input waits on Andre; default 7d\n"
            "\n[assist]\n"
            f"human_in_loop = {'true' if self.assist.human_in_loop else 'false'}\n"
            "\n[outcome_guard]\n"
            f"protected_refs = {_toml_string_array(self.outcome_guard.protected_refs)}  # canonical refs the reference-transaction hook protects\n"
        )


def _toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _toml_optional_string(value: str | None) -> str:
    return _toml_string(value or "")


def _toml_string_array(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(_toml_string(value) for value in values) + "]"


def load_config(path: Path) -> ControllerConfig:
    """Load and validate one config file without touching native Hermes data."""

    path = Path(path).expanduser()
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    try:
        instance = raw["instance"]
        controller = raw["controller"]
        format_version = raw.get("format_version", 1)
        name = instance["name"]
        native_root = instance["native_boards_root"]
        state_db = controller["state_db"]
        workspace = controller.get("workspace")
        native = raw.get("native", {})
        telegram = raw.get("telegram", {})
        discovery = raw.get("discovery", {})
        stream = raw.get("stream", {})
        # The needs-input-watcher section was renamed from [blocker_ping] in
        # one release; keep reading the legacy section name so an
        # operator-edited config that was never re-initialized keeps its
        # settings (the alias must behave identically to the new name).
        needs_input_watcher = raw.get("needs_input_watcher", raw.get("blocker_ping", {}))
        review_gap = raw.get("review_gap", {})
        watcher = raw.get("watcher", {})
        harness_loop = raw.get("harness_loop", {})
        assist = raw.get("assist", {})
        outcome_guard = raw.get("outcome_guard", {})
    except (KeyError, TypeError) as exc:
        raise ConfigError(
            "config must contain [instance] name/native_boards_root and "
            "[controller] state_db"
        ) from exc
    if isinstance(format_version, bool) or not isinstance(format_version, int):
        raise ConfigError("format_version must be an integer")
    if not all(isinstance(value, str) for value in (name, native_root, state_db)):
        raise ConfigError("instance name, native_boards_root, and state_db must be strings")
    if workspace is not None and not isinstance(workspace, str):
        raise ConfigError("controller workspace must be a string or null")
    if not isinstance(native, dict) or not isinstance(telegram, dict) or not isinstance(stream, dict):
        raise ConfigError("native, telegram, and stream config sections must be tables")
    if not isinstance(discovery, dict):
        raise ConfigError("discovery config section must be a table")
    if not isinstance(needs_input_watcher, dict):
        raise ConfigError("needs_input_watcher config section must be a table")
    if not isinstance(review_gap, dict):
        raise ConfigError("review_gap config section must be a table")
    if not isinstance(watcher, dict):
        raise ConfigError("watcher config section must be a table")
    if not isinstance(harness_loop, dict):
        raise ConfigError("harness_loop config section must be a table")
    if not isinstance(assist, dict):
        raise ConfigError("assist config section must be a table")
    if not isinstance(outcome_guard, dict):
        raise ConfigError("outcome_guard config section must be a table")
    native_cli = native.get("cli", "hermes")
    native_profile = native.get("profile") or None
    telegram_chat_id = telegram.get("chat_id", "")
    telegram_chat_id_env = telegram.get("chat_id_env") or None
    telegram_chat_type = telegram.get("chat_type", "dm")
    telegram_thread_id = telegram.get("thread_id") or None
    telegram_user_id = telegram.get("user_id") or None
    telegram_notifier_profile = telegram.get("notifier_profile") or None
    unclaimed_child_after_seconds = discovery.get("unclaimed_child_after_seconds", 1800)
    if not _is_positive_number(unclaimed_child_after_seconds, integers_only=True):
        raise ConfigError("unclaimed_child_after_seconds must be a positive integer")
    recency_window_seconds = discovery.get("recency_window_seconds", 3600)
    if not _is_positive_number(recency_window_seconds, integers_only=True):
        raise ConfigError("recency_window_seconds must be a positive integer")
    stream_enabled = stream.get("enabled", False)
    stream_adapter = stream.get("adapter", "none")
    stream_endpoint = stream.get("endpoint")
    stream_boards = stream.get("boards", [])
    stream_credential_env = stream.get("credential_env")
    stream_current_state_reader = stream.get("current_state_reader")
    stream_alert_after = stream.get("alert_after_consecutive_failures", 3)
    stream_reconcile_interval = stream.get("reconcile_interval_cycles", 0)
    optional_values = (
        native_profile,
        telegram_chat_id_env,
        telegram_thread_id,
        telegram_user_id,
        telegram_notifier_profile,
    )
    if not isinstance(native_cli, str) or not isinstance(telegram_chat_id, str):
        raise ConfigError("native cli and telegram chat_id must be strings")
    if not isinstance(telegram_chat_type, str):
        raise ConfigError("telegram chat_type must be a string")
    if not isinstance(stream_enabled, bool):
        raise ConfigError("stream enabled must be a boolean")
    if not isinstance(stream_adapter, str):
        raise ConfigError("stream adapter must be a string")
    if not _is_positive_number(stream_alert_after, integers_only=True):
        raise ConfigError(
            "stream alert_after_consecutive_failures must be a positive integer"
        )
    if not isinstance(stream_reconcile_interval, int) or isinstance(
        stream_reconcile_interval, bool
    ) or stream_reconcile_interval < 0:
        raise ConfigError(
            "stream reconcile_interval_cycles must be a non-negative integer"
        )
    if not isinstance(stream_boards, list) or any(
        not isinstance(board, str) for board in stream_boards
    ):
        raise ConfigError("stream boards must be an array of strings")
    stream_optional_values = (
        ("endpoint", stream_endpoint),
        ("credential_env", stream_credential_env),
        ("current_state_reader", stream_current_state_reader),
    )
    if any(value is not None and not isinstance(value, str) for _, value in stream_optional_values):
        raise ConfigError("optional stream values must be strings or null")
    if any(value is not None and not isinstance(value, str) for value in optional_values):
        raise ConfigError("optional native/telegram destination values must be strings or null")
    needs_input_watcher_enabled = needs_input_watcher.get("enabled", True)
    needs_input_watcher_min_block_seconds = needs_input_watcher.get("min_block_seconds", 300)
    needs_input_watcher_llm_profile = needs_input_watcher.get("llm_profile", "")
    needs_input_watcher_template = needs_input_watcher.get("prompt_template_path")
    needs_input_watcher_timeout = needs_input_watcher.get("timeout_seconds", 90)
    if not isinstance(needs_input_watcher_enabled, bool):
        raise ConfigError("needs_input_watcher enabled must be a boolean")
    if not isinstance(needs_input_watcher_llm_profile, str):
        raise ConfigError("needs_input_watcher llm_profile must be a string")
    if needs_input_watcher_template is not None and not isinstance(needs_input_watcher_template, str):
        raise ConfigError("needs_input_watcher prompt_template_path must be a string or null")
    review_gap_enabled = review_gap.get("enabled", True)
    review_gap_min_age = review_gap.get("min_age_seconds", 300)
    review_gap_recency_hours = review_gap.get("recency_hours", 48)
    review_gap_stalled_hours = review_gap.get("stalled_alert_hours", 6)
    review_gap_auto_create = review_gap.get("auto_create", True)
    review_gap_trigger_c = review_gap.get("trigger_c_enabled", True)
    review_gap_trigger_d = review_gap.get("trigger_d_enabled", True)
    review_gap_cli_timeout = review_gap.get("cli_timeout_seconds", 30.0)
    review_gap_tick_timeout = review_gap.get("tick_timeout_seconds", 120.0)
    review_gap_max_workers = review_gap.get("max_workers", 16)
    if not isinstance(review_gap_enabled, bool):
        raise ConfigError("review_gap enabled must be a boolean")
    if not _is_positive_number(review_gap_min_age):
        raise ConfigError("review_gap min_age_seconds must be a positive number")
    if not _is_positive_number(review_gap_recency_hours):
        raise ConfigError("review_gap recency_hours must be a positive number")
    if not _is_positive_number(review_gap_stalled_hours):
        raise ConfigError("review_gap stalled_alert_hours must be a positive number")
    if not isinstance(review_gap_auto_create, bool):
        raise ConfigError("review_gap auto_create must be a boolean")
    if not isinstance(review_gap_trigger_c, bool):
        raise ConfigError("review_gap trigger_c_enabled must be a boolean")
    if not isinstance(review_gap_trigger_d, bool):
        raise ConfigError("review_gap trigger_d_enabled must be a boolean")
    if not _is_positive_number(review_gap_cli_timeout):
        raise ConfigError("review_gap cli_timeout_seconds must be a positive number")
    if not _is_positive_number(review_gap_tick_timeout):
        raise ConfigError("review_gap tick_timeout_seconds must be a positive number")
    if not _is_positive_number(review_gap_max_workers, integers_only=True):
        raise ConfigError("review_gap max_workers must be a positive integer")
    watcher_enabled = watcher.get("enabled", True)
    watcher_reviewer_profiles = watcher.get("reviewer_profiles", [])
    watcher_fix_assignee = watcher.get("fix_assignee", "developer")
    watcher_max_block_age = watcher.get("max_block_age_seconds", 1800)
    watcher_canonical = watcher.get("canonical_branch", "main")
    watcher_canonical_fallback = watcher.get("canonical_branch_fallback", "master")
    watcher_hold_window = watcher.get("hold_comment_window_seconds", 3600)
    watcher_pick_prefix = watcher.get("pick_gate_prefix", "One-at-a-time:")
    watcher_guard_reason = watcher.get("guard_reason", "blocked on creation without event — queueing guard")
    watcher_recv_timeout = watcher.get("recv_timeout_seconds", 10.0)
    watcher_cycle_interval = watcher.get("cycle_interval_seconds", 300.0)
    watcher_deadlock_min_age = watcher.get("deadlock_min_age_seconds", 900)
    if not isinstance(watcher_enabled, bool):
        raise ConfigError("watcher enabled must be a boolean")
    if not isinstance(watcher_reviewer_profiles, list) or any(
        not isinstance(profile, str) for profile in watcher_reviewer_profiles
    ):
        raise ConfigError("watcher reviewer_profiles must be an array of strings")
    watcher_string_values = (
        ("fix_assignee", watcher_fix_assignee),
        ("canonical_branch", watcher_canonical),
        ("canonical_branch_fallback", watcher_canonical_fallback),
        ("pick_gate_prefix", watcher_pick_prefix),
        ("guard_reason", watcher_guard_reason),
    )
    if any(not isinstance(value, str) for _, value in watcher_string_values):
        raise ConfigError("watcher string values must be strings")
    harness_loop_enabled = harness_loop.get("enabled", True)
    harness_loop_window_hours = harness_loop.get("window_hours", 24)
    harness_loop_max_applies = harness_loop.get("max_applies", 2)
    harness_loop_cooldown_days = harness_loop.get("cooldown_days", 30)
    harness_loop_bloat_threshold = harness_loop.get("bloat_threshold_tokens", 5_000_000)
    harness_loop_bloat_top_n = harness_loop.get("bloat_top_n", 3)
    harness_loop_sessions_db = harness_loop.get("sessions_db")
    harness_loop_external_dirs = harness_loop.get("external_dirs", [])
    harness_loop_dist_skills_root = harness_loop.get(
        "dist_skills_root", DEFAULT_DIST_SKILLS_ROOT
    )
    harness_loop_profiles_root = harness_loop.get("profiles_root", "")
    harness_loop_config_drift_allowed = harness_loop.get(
        "config_drift_allowed_profiles", []
    )
    harness_loop_hkrc_repo = harness_loop.get("hkrc_repo")
    harness_loop_analysis_profile = harness_loop.get("analysis_profile", "")
    harness_loop_analysis_timeout = harness_loop.get("analysis_timeout_seconds", 120)
    harness_loop_analysis_max_attempts = harness_loop.get("analysis_max_attempts", 2)
    harness_loop_escalate_after_nights = harness_loop.get("escalate_after_nights", 7)
    harness_loop_chronic_after_nights = harness_loop.get("chronic_after_nights", 21)
    harness_loop_stale_retention_days = harness_loop.get("stale_retention_days", 14)
    harness_loop_archloop_output_dir = harness_loop.get(
        "archloop_output_dir", DEFAULT_ARCHLOOP_OUTPUT_DIR
    )
    harness_loop_archloop_classes = harness_loop.get(
        "archloop_actionable_classes", list(ACTIONABLE_SKIP_CLASSES)
    )
    harness_loop_archloop_medium = harness_loop.get(
        "archloop_medium_nights", ARCHLOOP_MEDIUM_NIGHTS
    )
    harness_loop_archloop_high = harness_loop.get(
        "archloop_high_nights", ARCHLOOP_HIGH_NIGHTS
    )
    harness_loop_decision_latency = harness_loop.get(
        "decision_latency_seconds", DECISION_LATENCY_SECONDS
    )
    harness_loop_decision_latency_human = harness_loop.get(
        "decision_latency_human_seconds", DECISION_LATENCY_HUMAN_SECONDS
    )
    assist_human_in_loop = assist.get("human_in_loop", True)
    if not isinstance(assist_human_in_loop, bool):
        raise ConfigError("assist human_in_loop must be a boolean")
    if not isinstance(harness_loop_enabled, bool):
        raise ConfigError("harness_loop enabled must be a boolean")
    if not _is_positive_number(harness_loop_window_hours):
        raise ConfigError("harness_loop window_hours must be a positive number")
    if (
        not isinstance(harness_loop_max_applies, int)
        or isinstance(harness_loop_max_applies, bool)
        or harness_loop_max_applies not in (1, 2)
    ):
        raise ConfigError("harness_loop max_applies must be 1 or 2")
    if not _is_positive_number(harness_loop_cooldown_days):
        raise ConfigError("harness_loop cooldown_days must be a positive number")
    if (
        not isinstance(harness_loop_bloat_threshold, int)
        or isinstance(harness_loop_bloat_threshold, bool)
        or harness_loop_bloat_threshold <= 0
    ):
        raise ConfigError("harness_loop bloat_threshold_tokens must be a positive integer")
    if (
        not isinstance(harness_loop_bloat_top_n, int)
        or isinstance(harness_loop_bloat_top_n, bool)
        or harness_loop_bloat_top_n <= 0
    ):
        raise ConfigError("harness_loop bloat_top_n must be a positive integer")
    if harness_loop_sessions_db is not None and not isinstance(harness_loop_sessions_db, str):
        raise ConfigError("harness_loop sessions_db must be a string or null")
    if not isinstance(harness_loop_external_dirs, list) or any(
        not isinstance(directory, str) or not directory.strip()
        for directory in harness_loop_external_dirs
    ):
        raise ConfigError("harness_loop external_dirs must be an array of non-empty strings")
    if len(set(harness_loop_external_dirs)) != len(harness_loop_external_dirs):
        raise ConfigError("harness_loop external_dirs must not contain duplicates")
    if not isinstance(harness_loop_dist_skills_root, str) or not harness_loop_dist_skills_root.strip():
        raise ConfigError("harness_loop dist_skills_root must be a non-empty string")
    if not isinstance(harness_loop_profiles_root, str):
        raise ConfigError("harness_loop profiles_root must be a string or empty")
    if not isinstance(harness_loop_config_drift_allowed, list) or any(
        not isinstance(profile, str) or not profile.strip()
        for profile in harness_loop_config_drift_allowed
    ):
        raise ConfigError(
            "harness_loop config_drift_allowed_profiles must be an array of "
            "non-empty strings"
        )
    if len(set(harness_loop_config_drift_allowed)) != len(
        harness_loop_config_drift_allowed
    ):
        raise ConfigError(
            "harness_loop config_drift_allowed_profiles must not contain duplicates"
        )
    if harness_loop_hkrc_repo is not None and not isinstance(harness_loop_hkrc_repo, str):
        raise ConfigError("harness_loop hkrc_repo must be a string or null")
    if not isinstance(harness_loop_analysis_profile, str):
        raise ConfigError("harness_loop analysis_profile must be a string")
    if (
        not isinstance(harness_loop_analysis_timeout, int)
        or isinstance(harness_loop_analysis_timeout, bool)
        or harness_loop_analysis_timeout <= 0
    ):
        raise ConfigError("harness_loop analysis_timeout_seconds must be a positive integer")
    if (
        not isinstance(harness_loop_analysis_max_attempts, int)
        or isinstance(harness_loop_analysis_max_attempts, bool)
        or harness_loop_analysis_max_attempts <= 0
    ):
        raise ConfigError("harness_loop analysis_max_attempts must be a positive integer")
    if (
        not isinstance(harness_loop_escalate_after_nights, int)
        or isinstance(harness_loop_escalate_after_nights, bool)
        or harness_loop_escalate_after_nights <= 0
    ):
        raise ConfigError("harness_loop escalate_after_nights must be a positive integer")
    if (
        not isinstance(harness_loop_chronic_after_nights, int)
        or isinstance(harness_loop_chronic_after_nights, bool)
        or harness_loop_chronic_after_nights <= 0
    ):
        raise ConfigError("harness_loop chronic_after_nights must be a positive integer")
    if harness_loop_chronic_after_nights < harness_loop_escalate_after_nights:
        raise ConfigError(
            "harness_loop chronic_after_nights must be >= escalate_after_nights"
        )
    if (
        not isinstance(harness_loop_stale_retention_days, int)
        or isinstance(harness_loop_stale_retention_days, bool)
        or harness_loop_stale_retention_days <= 0
    ):
        raise ConfigError("harness_loop stale_retention_days must be a positive integer")
    if not isinstance(harness_loop_archloop_output_dir, str):
        raise ConfigError("harness_loop archloop_output_dir must be a string or empty")
    if not isinstance(harness_loop_archloop_classes, list) or any(
        not isinstance(skip_class, str) or not skip_class.strip()
        for skip_class in harness_loop_archloop_classes
    ):
        raise ConfigError(
            "harness_loop archloop_actionable_classes must be an array of non-empty strings"
        )
    if len(set(harness_loop_archloop_classes)) != len(harness_loop_archloop_classes):
        raise ConfigError(
            "harness_loop archloop_actionable_classes must not contain duplicates"
        )
    if (
        not isinstance(harness_loop_archloop_medium, int)
        or isinstance(harness_loop_archloop_medium, bool)
        or harness_loop_archloop_medium <= 0
    ):
        raise ConfigError("harness_loop archloop_medium_nights must be a positive integer")
    if (
        not isinstance(harness_loop_archloop_high, int)
        or isinstance(harness_loop_archloop_high, bool)
        or harness_loop_archloop_high < harness_loop_archloop_medium
    ):
        raise ConfigError(
            "harness_loop archloop_high_nights must be an integer >= archloop_medium_nights"
        )
    if not _is_positive_number(harness_loop_decision_latency):
        raise ConfigError(
            "harness_loop decision_latency_seconds must be a positive number"
        )
    if not _is_positive_number(harness_loop_decision_latency_human):
        raise ConfigError(
            "harness_loop decision_latency_human_seconds must be a positive number"
        )
    outcome_guard_protected_refs = outcome_guard.get(
        "protected_refs", ["refs/heads/main"]
    )
    if not isinstance(outcome_guard_protected_refs, list) or any(
        not isinstance(ref, str) or not ref.strip() or not ref.startswith("refs/")
        for ref in outcome_guard_protected_refs
    ):
        raise ConfigError(
            "outcome_guard protected_refs must be an array of refs paths "
            "starting with 'refs/'"
        )
    if len(set(outcome_guard_protected_refs)) != len(outcome_guard_protected_refs):
        raise ConfigError("outcome_guard protected_refs must not contain duplicates")
    return ControllerConfig(
        instance_name=name,
        native_boards_root=Path(native_root).expanduser(),
        state_db=Path(state_db).expanduser(),
        workspace=Path(workspace).expanduser() if workspace else None,
        format_version=format_version,
        native_cli=native_cli,
        native_profile=native_profile,
        telegram_chat_id=telegram_chat_id,
        telegram_chat_id_env=telegram_chat_id_env,
        telegram_chat_type=telegram_chat_type,
        telegram_thread_id=telegram_thread_id,
        telegram_user_id=telegram_user_id,
        telegram_notifier_profile=telegram_notifier_profile,
        unclaimed_child_after_seconds=unclaimed_child_after_seconds,
        recency_window_seconds=recency_window_seconds,
        stream=StreamConfig(
            enabled=stream_enabled,
            adapter=stream_adapter,
            endpoint=stream_endpoint or None,
            boards=tuple(stream_boards),
            credential_env=stream_credential_env or None,
            current_state_reader=stream_current_state_reader or None,
            alert_after_consecutive_failures=stream_alert_after,
            reconcile_interval_cycles=stream_reconcile_interval,
        ),
        needs_input_watcher=NeedsInputWatcherConfig(
            enabled=needs_input_watcher_enabled,
            min_block_seconds=needs_input_watcher_min_block_seconds,
            llm_profile=needs_input_watcher_llm_profile,
            prompt_template_path=(
                needs_input_watcher_template if needs_input_watcher_template else None
            ),
            timeout_seconds=needs_input_watcher_timeout,
        ),
        review_gap=ReviewGapConfig(
            enabled=review_gap_enabled,
            min_age_seconds=review_gap_min_age,
            recency_hours=review_gap_recency_hours,
            stalled_alert_hours=review_gap_stalled_hours,
            auto_create=review_gap_auto_create,
            trigger_c_enabled=review_gap_trigger_c,
            trigger_d_enabled=review_gap_trigger_d,
            cli_timeout_seconds=review_gap_cli_timeout,
            tick_timeout_seconds=review_gap_tick_timeout,
            max_workers=review_gap_max_workers,
        ),
        watcher=WatcherConfig(
            enabled=watcher_enabled,
            reviewer_profiles=tuple(watcher_reviewer_profiles),
            fix_assignee=watcher_fix_assignee,
            max_block_age_seconds=watcher_max_block_age,
            canonical_branch=watcher_canonical,
            canonical_branch_fallback=watcher_canonical_fallback,
            hold_comment_window_seconds=watcher_hold_window,
            pick_gate_prefix=watcher_pick_prefix,
            guard_reason=watcher_guard_reason,
            recv_timeout_seconds=watcher_recv_timeout,
            cycle_interval_seconds=watcher_cycle_interval,
            deadlock_min_age_seconds=watcher_deadlock_min_age,
        ),
        harness_loop=HarnessLoopConfig(
            enabled=harness_loop_enabled,
            window_hours=harness_loop_window_hours,
            max_applies=harness_loop_max_applies,
            cooldown_days=harness_loop_cooldown_days,
            bloat_threshold_tokens=harness_loop_bloat_threshold,
            bloat_top_n=harness_loop_bloat_top_n,
            sessions_db=(
                Path(harness_loop_sessions_db).expanduser()
                if harness_loop_sessions_db
                else None
            ),
            external_dirs=tuple(harness_loop_external_dirs),
            dist_skills_root=harness_loop_dist_skills_root,
            profiles_root=harness_loop_profiles_root,
            config_drift_allowed_profiles=tuple(harness_loop_config_drift_allowed),
            hkrc_repo=(
                Path(harness_loop_hkrc_repo).expanduser()
                if harness_loop_hkrc_repo
                else None
            ),
            analysis_profile=harness_loop_analysis_profile,
            analysis_timeout_seconds=harness_loop_analysis_timeout,
            analysis_max_attempts=harness_loop_analysis_max_attempts,
            escalate_after_nights=harness_loop_escalate_after_nights,
            chronic_after_nights=harness_loop_chronic_after_nights,
            stale_retention_days=harness_loop_stale_retention_days,
            archloop_output_dir=harness_loop_archloop_output_dir,
            archloop_actionable_classes=tuple(harness_loop_archloop_classes),
            archloop_medium_nights=harness_loop_archloop_medium,
            archloop_high_nights=harness_loop_archloop_high,
            decision_latency_seconds=harness_loop_decision_latency,
            decision_latency_human_seconds=harness_loop_decision_latency_human,
        ),
        assist=AssistConfig(human_in_loop=assist_human_in_loop),
        outcome_guard=OutcomeGuardConfig(
            protected_refs=tuple(outcome_guard_protected_refs)
        ),
    )


def write_config(path: Path, config: ControllerConfig, *, overwrite: bool = False) -> None:
    """Write a controller config atomically; never creates native directories."""

    path = Path(path).expanduser()
    if path.exists() and not overwrite:
        raise ConfigError(f"config already exists: {path} (use --force to replace it)")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(config.as_toml(), encoding="utf-8")
    temporary.replace(path)
