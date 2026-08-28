"""Continuous, single-flight blocker recovery runtime.

The daemon is intentionally separate from the one-shot ``discover`` and
``run`` commands.  Continuous mode consumes an injected, authenticated,
board-scoped event-stream observer and never falls back to native SQLite or
CLI watch/tail parsing.  Native CLI calls remain confined to the handoff
boundary after stream classification and controller-state reservation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import errno
import fcntl
import json
import logging
from pathlib import Path
import signal
import threading
import time
from types import FrameType
from typing import Any, TextIO, cast

import uuid

from .classifier import (
    ClassificationResult,
    ClassifierInputError,
    CurrentTaskState,
    NormalizedEvent,
    PayloadState as ClassifierPayloadState,
    classify_event,
    classify_stream_error,
)
from .config import ControllerConfig
from .discovery import DiscoveryError
from .event_stream import (
    EventBatch,
    StreamAdapter,
    StreamCredentials,
    StreamError,
    StreamErrorCode,
    StreamEvent,
)
from .handoff import (
    HandoffError,
    HandoffReport,
    execute_reserved_handoff,
)
from .self_health import format_stream_alert, format_stream_recovery
from .live import CurrentStateReaderError
from .stale_block_watch import is_config_defect, is_silent_death_block
from .state import ControllerState, StateError, StreamCursorState, StreamEventKey


DEFAULT_POLL_INTERVAL = 30.0
DEFAULT_EVENT_BATCH_SIZE = 200

# The stream error categories that count as self-health transport/auth
# failures.  Only these accumulate a board's outage episode and can fire an
# alert: the controller's own stream transport is broken (auth rejected,
# endpoint unreachable, connection dropped).  Malformed frames, invalid
# cursors, board-validation, and retention outcomes are adapter, protocol,
# or configuration faults; counting them could false-alert the operator.
SELF_HEALTH_TRANSPORT_CODES = frozenset(
    {
        StreamErrorCode.AUTH_FAILED,
        StreamErrorCode.TRANSPORT,
        StreamErrorCode.DISCONNECTED,
    }
)


class LockError(RuntimeError):
    """Raised when another controller process owns the instance lock."""


@dataclass(frozen=True, slots=True)
class ObservationBatch:
    """Events accepted from one serial stream pass and resulting cursors."""

    events: tuple[StreamEvent, ...]
    cursors: dict[str, int]
    reserved: int = 0
    skipped: int = 0


class InstanceLock:
    """Lifetime Linux advisory lock owned by a controller instance.

    A lock file is controller-owned.  Its existence is never used for
    ownership, so stale files after a crash are harmless; ``flock`` releases
    the lock automatically when the process exits.
    """

    def __init__(self, path: Path):
        self.path = Path(path).expanduser()
        self._file: TextIO | None = None

    @property
    def acquired(self) -> bool:
        return self._file is not None

    def acquire(self) -> None:
        if self._file is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            file.close()
            if isinstance(exc, BlockingIOError) or getattr(exc, "errno", None) in (errno.EAGAIN, errno.EACCES):
                raise LockError(f"controller already running: {self.path}") from exc
            raise LockError(f"cannot acquire controller lock: {self.path}: {exc}") from exc
        self._file = file

    def release(self) -> None:
        file, self._file = self._file, None
        if file is None:
            return
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        finally:
            file.close()

    def __enter__(self) -> "InstanceLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.release()


class _FatalStateBoundary:
    """Turn every controller-state operation failure into a fatal state error.

    The cycle handler deliberately isolates native observation and handoff
    failures.  It must not isolate failures from this controller's own state,
    because continuing after a failed cursor or reservation write can violate
    the one-ever invariant.  Keeping this boundary around the state object also
    catches injected/unexpected state exceptions, not only ``sqlite3.Error``.
    """

    def __init__(self, state: ControllerState):
        self._state = state

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._state, name)
        if not callable(attribute):
            return attribute

        def operation(*args: object, **kwargs: object) -> object:
            try:
                return attribute(*args, **kwargs)
            except StateError:
                raise
            except Exception as exc:
                raise StateError(
                    f"controller state operation {name} failed: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

        return operation


class StreamObserver:
    """Serial, authenticated event-stream observer with no native fallback.

    Connections are persistent: ``poll`` reuses the adapter's open socket for
    a board across cycles and only connects when the socket is absent, after a
    transport failure, or when the durable cursor moved (identity/reset).
    Idle boards (no frame within the receive timeout) keep their connection and
    advance nothing.  Transport failures are recorded fail-closed per board and
    the next connect attempt is deferred by the adapter's bounded backoff.
    """

    def __init__(
        self,
        adapters: Mapping[str, StreamAdapter],
        state: ControllerState,
        *,
        credentials: StreamCredentials | None = None,
        current_state_reader: Callable[[str, str], CurrentTaskState | Mapping[str, Any] | None] | None = None,
        stream_identity: Mapping[str, str] | Callable[[str], str] | None = None,
        backoff: dict[str, float] | None = None,
        monotonic: Callable[[], float] | None = None,
        # ``blocked_lister`` powers the periodic state-based reconcile sweep:
        # a board-level CLI read of every ``status='blocked'`` task id.  It is
        # the backstop for death events the stream never delivered (verified
        # 2026-08-06: a task blocked 02:04-09:00 with
        # zero ``blocked`` events and zero stream deliveries).  ``None``
        # disables the sweep.
        blocked_lister: Callable[[str], list[str]] | None = None,
        # Self-health alerting: ``alert_after_consecutive_failures`` is the
        # per-board consecutive transport/auth failure threshold (``None``
        # disables alerting); ``alerter`` delivers one rendered message and
        # returns True when the delivery succeeded.  At most one alert attempt
        # is made per outage episode (a failed delivery is recorded durably
        # and not retried on every later failure), and the alerter must never
        # raise: alerting is additive and a failed alert must not suppress
        # stream recovery.
        alert_after_consecutive_failures: int | None = None,
        alerter: Callable[[str], bool] | None = None,
        # ``batch_size`` and the two legacy poll filters are intentionally not
        # accepted: continuous observation is exclusively the approved stream
        # contract.  The old native observer was removed rather than retained as
        # a fallback path.
    ) -> None:
        if not adapters:
            raise ValueError("at least one approved stream adapter is required")
        if credentials is None or not isinstance(credentials, StreamCredentials):
            raise ValueError("approved stream credentials are required")
        if current_state_reader is None or not callable(current_state_reader):
            raise ValueError("current_state_reader is required")
        if alert_after_consecutive_failures is not None and (
            isinstance(alert_after_consecutive_failures, bool)
            or not isinstance(alert_after_consecutive_failures, int)
            or alert_after_consecutive_failures < 1
        ):
            raise ValueError(
                "alert_after_consecutive_failures must be a positive integer or None"
            )
        if alerter is not None and not callable(alerter):
            raise ValueError("alerter must be callable or None")
        self.adapters = dict(adapters)
        self.state = _FatalStateBoundary(state)
        self.credentials = credentials
        self.current_state_reader = current_state_reader
        self.stream_identity = stream_identity
        self.blocked_lister = blocked_lister
        # The backoff map is caller-shareable so a daemon that rebuilds its
        # observer each cycle still keeps bounded reconnect scheduling.
        self._backoff_until = backoff if backoff is not None else {}
        self._monotonic = monotonic or time.monotonic
        self.alert_after_consecutive_failures = alert_after_consecutive_failures
        self._alerter = alerter

    def poll(
        self, *, stop_requested: Callable[[], bool] | None = None
    ) -> ObservationBatch:
        """Consume one complete batch from each board, serially.

        Every event in a frame must have a confirmed current-state snapshot and
        a fail-closed classification before the durable frame commit.  Only
        actionable classifier results create the existing one-ever reservation;
        lifecycle, malformed, unknown, and transport outcomes never reserve.

        ``stop_requested`` is a shutdown probe: it is checked before each
        board's socket/CLI work so a mid-cycle SIGTERM does not drain every
        board before the daemon can exit.  A stop observed mid-frame abandons
        that frame's durable commit entirely: nothing is reserved and the
        cursor stays put, so the whole frame re-delivers after restart and is
        classified fresh.  A stop observed between boards simply ends the
        sweep.
        """

        should_stop = stop_requested or (lambda: False)
        accepted: list[StreamEvent] = []
        reserved = skipped = 0
        for board_slug in sorted(self.adapters):
            if should_stop():
                break
            adapter = self.adapters[board_slug]
            if self._backoff_until.get(board_slug, 0.0) > self._monotonic():
                # A transport failure was recorded recently; respect the
                # adapter's bounded backoff instead of churning a reconnect.
                # The board is not reconciled either: reconciliation clears the
                # durable transport record, which would erase the fail-closed
                # failure while the board is deliberately not being attempted.
                continue
            identity = self._identity(board_slug, adapter)
            reconciled = self.state.reconcile_stream_cursor(
                board_slug, identity=identity
            )
            connection_error = self._ensure_connected(
                adapter, board_slug, reconciled.cursor
            )
            if connection_error is not None:
                self._record_error(board_slug, connection_error, stop_requested=should_stop)
                self._schedule_backoff(board_slug, adapter, connection_error)
                continue
            frame = adapter.recv()
            if isinstance(frame, StreamError):
                self._record_error(board_slug, frame, stop_requested=should_stop)
                self._schedule_backoff(board_slug, adapter, frame)
                continue
            if not isinstance(frame, EventBatch):
                raise StateError("approved stream adapter returned an invalid frame")
            self._backoff_until.pop(board_slug, None)
            # The pre-commit snapshot decides whether this accepted frame ends
            # an alerted outage episode (a recovery notice fires exactly once).
            prior = self.state.get_stream_cursor(board_slug)
            classifications = self._classify_frame(
                board_slug, frame, stop_requested=should_stop
            )
            if len(classifications) != len(frame.events):
                # Shutdown probe fired mid-frame: the tail was never
                # classified, so this frame must not be committed at the
                # cursor.  Abandon the whole frame — nothing was reserved
                # and the cursor stays put, so every event re-delivers
                # after restart and is classified fresh.
                break
            for event, result in zip(frame.events, classifications, strict=True):
                if result.reserve:
                    if self.state.reserve_stream_event(
                        board_slug,
                        event.task_id,
                        blocker_kind=_reservation_kind(result),
                        latest_event_at=event.created_at,
                    ):
                        reserved += 1
                    else:
                        skipped += 1
                else:
                    skipped += 1
                accepted.append(event)
            self.state.commit_stream_frame(
                board_slug,
                identity=identity,
                cursor=frame.cursor,
                events=tuple(
                    StreamEventKey(event.id, event.task_id, event.run_id)
                    for event in frame.events
                ),
            )
            if prior.alert_sent or prior.alert_attempted:
                # The episode produced an alert attempt (delivered or not): a
                # recovery notice must still be attempted.  A failed alert
                # send leaves ``alert_sent`` false, so gating only on it would
                # suppress the required recovery notice after a swallowed
                # alert failure.
                self._send_recovery(board_slug, prior, stop_requested=should_stop)
        return ObservationBatch(
            tuple(accepted), self._cursors(), reserved=reserved, skipped=skipped
        )

    def reconcile_blocked_state(
        self, *, stop_requested: Callable[[], bool] | None = None
    ) -> int:
        """Reserve silent death blocks found by current state, not the stream.

        The WS stream only delivers events it actually received; when it never
        saw a board's death events (verified 2026-08-06: a task blocked
        02:04-09:00, zero stream deliveries all night), a blocked task with a
        death-kind latest event stays invisible to the event-driven classifier
        forever.  This sweep is the state-based backstop: it lists every
        ``status='blocked'`` task per board through the injected lister, reads
        each one's current state, and reserves the silent death class (latest
        event is a death kind, no typed block kind).  Config-defect cards
        (the ``--max-runtime 0`` signature) are deliberately NOT reserved —
        the classifier escalates those to a human instead of burning the
        one-ever reservation on a blind unblock that re-dies at ~60s.

        Returns the number of new reservations.  Per-board failures are
        logged and isolated: one board's CLI failure never suppresses the
        sweep on healthy boards, and a failed sweep must not fail the cycle.

        ``stop_requested`` is a shutdown probe: it is checked before each
        board's list/read CLI work and between per-task current-state reads,
        so a mid-cycle SIGTERM never drains the whole blocked set before the
        daemon can exit.
        """

        if self.blocked_lister is None:
            return 0
        should_stop = stop_requested or (lambda: False)
        reserved = 0
        for board_slug in sorted(self.adapters):
            if should_stop():
                break
            if self._backoff_until.get(board_slug, 0.0) > self._monotonic():
                continue
            try:
                task_ids = self.blocked_lister(board_slug)
            except CurrentStateReaderError:
                # A CLI failure on one board is not a controller-state
                # failure; the next cycle retries the sweep for this board.
                continue
            for task_id in task_ids:
                if should_stop():
                    break
                if self.state.has_reservation(board_slug, task_id):
                    continue
                try:
                    current = self.current_state_reader(board_slug, task_id)
                except CurrentStateReaderError:
                    continue
                if current is None:
                    continue
                if not isinstance(current, CurrentTaskState):
                    if isinstance(current, Mapping):
                        try:
                            current = CurrentTaskState.from_mapping(dict(current))
                        except Exception:
                            continue
                    else:
                        continue
                if not is_silent_death_block(
                    status=current.status,
                    latest_event_kind=current.latest_event_kind,
                    run_error=current.run_error,
                ):
                    continue
                if is_config_defect(current.run_error):
                    # G4: escalate, never blind-unblock a --max-runtime 0 card.
                    continue
                if self.state.reserve_stream_event(
                    board_slug,
                    task_id,
                    blocker_kind=current.latest_event_kind,
                    latest_event_at=int(time.time()),
                ):
                    reserved += 1
        return reserved

    def _ensure_connected(
        self, adapter: StreamAdapter, board_slug: str, cursor: int
    ) -> StreamError | None:
        """Return the adapter to a connected state at ``cursor`` or an error.

        An already-connected adapter whose in-memory cursor matches the durable
        cursor keeps its socket.  A durable cursor that moved underneath an
        open connection (identity change, rollback, or retention reset) forces
        a clean resync from the durable position.
        """

        if adapter.connected:
            if adapter.cursor == cursor:
                return None
            adapter.close()
        return adapter.connect(board_slug, cursor, self.credentials)

    def _schedule_backoff(
        self, board_slug: str, adapter: StreamAdapter, error: StreamError
    ) -> None:
        retry_after = error.retry_after
        if retry_after is None:
            # Non-retryable outcomes (auth, malformed, terminal transport)
            # still keep the board observed with a bounded cadence instead of
            # hammering the endpoint or dropping the board silently.
            retry_after = adapter.reconnect_policy.max_delay
        self._backoff_until[board_slug] = self._monotonic() + max(retry_after, 0.0)

    def close(self) -> None:
        """Close every watched connection; the observer is unusable after."""

        for adapter in self.adapters.values():
            try:
                adapter.close()
            except Exception:
                # Closing is best effort; the caller is tearing down anyway.
                pass
        self.adapters.clear()
        self._backoff_until.clear()

    def _classify_frame(
        self,
        board_slug: str,
        frame: EventBatch,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> tuple[ClassificationResult, ...]:
        """Classify every event in a frame, stopping early on a shutdown probe.

        When ``stop_requested`` turns true mid-frame the prefix classified so
        far is returned; the caller must not durably commit a frame whose tail
        was never classified (the unprocessed events would be lost at the
        cursor).  ``poll`` treats a short result as an abandoned frame.
        """

        should_stop = stop_requested or (lambda: False)
        results: list[ClassificationResult] = []
        for event in frame.events:
            if should_stop():
                break
            current = self.current_state_reader(board_slug, event.task_id)
            if current is None:
                raise StateError(
                    f"current state unavailable for ({board_slug!r}, {event.task_id!r})"
                )
            try:
                normalized = NormalizedEvent(
                    board_slug=board_slug,
                    event_id=event.id,
                    task_id=event.task_id,
                    run_id=event.run_id,
                    kind=event.kind,
                    payload=event.payload,
                    payload_state=ClassifierPayloadState(event.payload_state.value),
                    created_at=event.created_at,
                )
                results.append(classify_event(normalized, current))
            except (ClassifierInputError, ValueError) as exc:
                raise StateError(f"stream event classification failed closed: {exc}") from exc
        return tuple(results)

    def _record_error(
        self,
        board_slug: str,
        error: StreamError,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        # Only transport/auth stream categories accumulate self-health
        # failures; the poll loop still schedules bounded backoff for the
        # other (adapter, protocol, or configuration) outcomes so the board
        # stays observed without churning a reconnect.
        if error.code not in SELF_HEALTH_TRANSPORT_CODES:
            return
        # Auth/transport failures are never converted into native observation;
        # they are isolated to this board and leave its accepted cursor intact.
        result = classify_stream_error(error)
        code = result.reason.removeprefix("transport_error:")
        updated = self.state.record_stream_transport_failure(
            board_slug, code=code, message=error.message
        )
        self._maybe_alert(board_slug, updated, stop_requested=stop_requested)

    def _maybe_alert(
        self,
        board_slug: str,
        state: StreamCursorState,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Send at most one alert per outage episode.

        ``alert_sent`` is set only after a confirmed delivery; ``alert_attempted``
        is set for any attempt (delivered or not).  Gating on both flags means a
        failed send is recorded once and never retried on every later failure of
        the same episode, so the operator cannot receive duplicate or
        count-ambiguous alerts for one outage.  Once the episode ends (a frame is
        accepted), both flags reset and the next episode alerts again.  A failed
        send (raise or False) still records ``alert_attempted`` so the episode is
        known to have produced an alert attempt and a recovery notice is
        attempted when the stream resumes.  The alerter must never raise out of
        the observer and a failed alert never suppresses recovery.
        """

        threshold = self.alert_after_consecutive_failures
        if threshold is None or state.alert_sent or state.alert_attempted:
            return
        if state.consecutive_failures < threshold:
            return
        if self._alerter is None:
            return
        if stop_requested is not None and stop_requested():
            # Shutdown probe fired: the native alert send would hold the
            # daemon in stop-sigterm.  The episode stays alert-pending and
            # the next process alerts for it after restart.
            return
        delivered = False
        try:
            delivered = bool(
                self._alerter(
                    format_stream_alert(
                        board_slug,
                        failure_count=state.consecutive_failures,
                        error_code=_error_code(state),
                        first_failure_at=state.episode_first_failure_at
                        or state.last_transport_at
                        or "unknown",
                        last_failure_at=state.last_transport_at
                        or state.episode_last_failure_at
                        or "unknown",
                    )
                )
            )
        except Exception:
            delivered = False
        if delivered:
            self.state.mark_stream_alert_sent(board_slug)
        else:
            self.state.mark_stream_alert_attempted(board_slug)

    def _send_recovery(
        self,
        board_slug: str,
        state: StreamCursorState,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Best-effort recovery notice after an alerted episode ends."""

        if stop_requested is not None and stop_requested():
            # Shutdown probe fired; the daemon is exiting and the next
            # process re-evaluates the episode from durable state.
            return
        try:
            if self._alerter is not None:
                self._alerter(
                    format_stream_recovery(
                        board_slug,
                        failure_count=state.consecutive_failures,
                        first_failure_at=state.episode_first_failure_at
                        or state.episode_last_failure_at
                        or "unknown",
                        last_failure_at=state.episode_last_failure_at,
                    )
                )
        except Exception:
            # A failed recovery notice is not fatal and never re-queued; the
            # stream is already healthy again.
            pass

    def _identity(self, board_slug: str, adapter: StreamAdapter) -> str:
        if callable(self.stream_identity):
            identity = self.stream_identity(board_slug)
        elif self.stream_identity is not None:
            identity = self.stream_identity.get(board_slug, "")
        else:
            identity = f"{adapter.endpoint}|{board_slug}"
        if not isinstance(identity, str) or not identity:
            raise StateError(f"approved stream identity unavailable for board {board_slug!r}")
        return identity

    def _cursors(self) -> dict[str, int]:
        return {
            board: self.state.get_stream_cursor(board).cursor
            for board in sorted(self.adapters)
        }


# Kept as a descriptive compatibility alias for callers that used the initial
# runtime name; it is now stream-only and never opens native Hermes files.
EventObserver = StreamObserver


def _reservation_kind(result: ClassificationResult) -> str | None:
    if result.classification.value == "actionable_typed_capability":
        return "capability"
    if result.classification.value == "actionable_circuit_breaker":
        return "gave_up"
    return "crashed"


def _error_code(state: StreamCursorState) -> str:
    """Return the stable error category from a durable transport record."""

    record = state.last_transport_error or ""
    return record.split(":", 1)[0] or "unknown"


@dataclass(frozen=True, slots=True)
class CycleResult:
    cycle_id: str
    observed: int = 0
    report: HandoffReport | None = None
    error: str | None = None


class DaemonRuntime:
    """One instance-scoped, serial daemon scheduler."""

    def __init__(
        self,
        config: ControllerConfig,
        *,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        event_batch_size: int = DEFAULT_EVENT_BATCH_SIZE,
        stream_observer: StreamObserver | None = None,
        stream_adapters: Mapping[str, StreamAdapter] | None = None,
        stream_credentials: StreamCredentials | None = None,
        current_state_reader: Callable[[str, str], CurrentTaskState | Mapping[str, Any] | None] | None = None,
        stream_identity: Mapping[str, str] | Callable[[str], str] | None = None,
        wiring_builder: Callable[
            [],
            tuple[
                Mapping[str, StreamAdapter],
                StreamCredentials,
                Callable[[str, str], CurrentTaskState | Mapping[str, Any] | None],
                Callable[[str], list[str]] | None,
            ],
        ]
        | None = None,
        runner: NativeRunner | None = None,
        wait: Callable[[float], bool] | None = None,
        logger: logging.Logger | None = None,
        now: Callable[[], int] | None = None,
        # ``blocked_lister`` lists ``status='blocked'`` task ids per board for
        # the reconcile sweep; ``None`` (default) uses the wiring-builder
        # lister when one is supplied.
        blocked_lister: Callable[[str], list[str]] | None = None,
        # ``reconcile_interval_cycles`` runs the blocked-state sweep every N
        # cycles (0 disables it).  The sweep is the state-based backstop for
        # death events the stream never delivered (see
        # ``StreamObserver.reconcile_blocked_state``).
        reconcile_interval_cycles: int = 0,
    ) -> None:
        if poll_interval < 0:
            raise ValueError("poll_interval must not be negative")
        if event_batch_size < 1:
            raise ValueError("event batch_size must be positive")
        self.config = config
        self.poll_interval = float(poll_interval)
        self.event_batch_size = event_batch_size
        self.stream_observer = stream_observer
        self.stream_adapters = dict(stream_adapters or {})
        self.stream_credentials = stream_credentials
        self.current_state_reader = current_state_reader
        self.stream_identity = stream_identity
        self.wiring_builder = wiring_builder
        self.runner = runner
        self.reconcile_interval_cycles = int(reconcile_interval_cycles)
        self._cycle_count = 0
        self._blocked_lister = blocked_lister
        self.stop_event = threading.Event()
        self.logger = logger or logging.getLogger("hkrc.daemon")
        self._wait = wait or self.stop_event.wait
        self._now = now or (lambda: int(time.time()))
        self._cycle_lock = threading.Lock()
        self._running = False
        self._previous_handlers: dict[int, Any] = {}
        # Reconnect scheduling shared across per-cycle observers so bounded
        # backoff survives wiring refreshes.
        self._stream_backoff: dict[str, float] = {}

    @property
    def running(self) -> bool:
        return self._running

    def request_stop(self, reason: str = "requested") -> None:
        self._log("shutdown_requested", reason=reason)
        self.stop_event.set()

    def run(self, *, max_cycles: int | None = None, install_signals: bool = True) -> int:
        """Run until a signal/stop request; return a process exit status."""

        if self._running:
            raise RuntimeError("daemon runtime is already running")
        if max_cycles is not None and max_cycles < 1:
            raise ValueError("max_cycles must be positive")
        lock = InstanceLock(self.config.lock_path)
        lock.acquire()
        self._running = True
        state: ControllerState | None = None
        completed_cycles = 0
        try:
            state = ControllerState.open_existing(self.config.state_db)
            state_boundary = _FatalStateBoundary(state)
            if state_boundary.instance_name != self.config.instance_name:
                raise StateError(
                    f"state instance {state_boundary.instance_name!r} does not match config "
                    f"{self.config.instance_name!r}"
                )
            if install_signals:
                self._install_signal_handlers()
            self._log(
                "startup",
                pending=len(state_boundary.pending_reservations()),
                started=state_boundary.started_intervention_count(),
            )
            while not self.stop_event.is_set():
                self.run_cycle(state)
                completed_cycles += 1
                if max_cycles is not None and completed_cycles >= max_cycles:
                    break
                if self.stop_event.is_set():
                    break
                self._wait(self.poll_interval)
            self._log("shutdown", reason="requested" if self.stop_event.is_set() else "cycle_limit")
            return 0
        finally:
            self._restore_signal_handlers()
            if state is not None:
                state.close()
            self._close_stream()
            lock.release()
            self._running = False

    def run_cycle(self, state: ControllerState) -> CycleResult:
        """Serialize cycle callers so discovery and handoff never overlap."""

        with self._cycle_lock:
            return self._run_cycle(state)

    def _run_cycle(self, state: ControllerState) -> CycleResult:
        """Perform exactly one discovery/serial-handoff cycle."""

        cycle_id = uuid.uuid4().hex
        started = time.monotonic()
        self._log("cycle_start", cycle_id=cycle_id)
        try:
            # Keep state failures outside the cycle-level failure-isolation
            # boundary.  Native/config/destination errors may be retried;
            # controller state errors must terminate the daemon so supervision
            # can restart it without trusting possibly-lost reservations.
            state_boundary = _FatalStateBoundary(state)
            # Match one-shot run's safety guarantee: no reservation before the
            # destination is known to be usable.
            self._validate_destination()
            observer = self._observer(cast(ControllerState, state_boundary))
            stop_probe = self.stop_event.is_set
            observed = observer.poll(stop_requested=stop_probe)
            self._cycle_count += 1
            reconcile_reserved = 0
            if (
                self.reconcile_interval_cycles > 0
                and self._cycle_count % self.reconcile_interval_cycles == 0
            ):
                # State-based backstop for death events the stream missed.
                reconcile_reserved = observer.reconcile_blocked_state(
                    stop_requested=stop_probe
                )
            if self.stop_event.is_set():
                # A shutdown request landed mid-cycle (poll or reconcile):
                # reservations made above are durable and will be handed off
                # by the next process; do not start new native work now.
                report = HandoffReport(
                    (), observed.reserved + reconcile_reserved, 0, 0, 0, observed.skipped
                )
            else:
                report = execute_reserved_handoff(
                    self.config,
                    cast(ControllerState, state_boundary),
                    reserved=observed.reserved + reconcile_reserved,
                    skipped=observed.skipped,
                    runner=self.runner,
                    stop_requested=stop_probe,
                )
            self._log_report(cycle_id, report)
        except (DiscoveryError, HandoffError) as exc:
            message = _safe_error(str(exc), self._destination())
            self._log("cycle_error", cycle_id=cycle_id, error=message)
            return CycleResult(cycle_id, error=message)
        except StateError:
            raise
        except Exception as exc:  # cycle-level failures must not busy-loop
            message = f"{type(exc).__name__}: {_safe_error(str(exc), self._destination())}"
            self._log("cycle_error", cycle_id=cycle_id, error=message)
            return CycleResult(cycle_id, error=message)
        self._log(
            "cycle_summary",
            cycle_id=cycle_id,
            observed=len(observed.events),
            reserved=report.reserved,
            started=report.started,
            completed=report.completed,
            failed=report.failed,
            skipped=report.skipped,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
        return CycleResult(cycle_id, len(observed.events), report)

    def _validate_destination(self) -> None:
        from .handoff import _validate_destination

        _validate_destination(self.config)

    def _observer(self, state: ControllerState) -> StreamObserver:
        if self.stream_observer is not None:
            return self.stream_observer
        if self.wiring_builder is not None:
            adapters, credentials, current_state_reader, blocked_lister = self.wiring_builder()
            self._merge_stream_wiring(adapters)
            self.stream_credentials = credentials
            self.current_state_reader = current_state_reader
            if self._blocked_lister is None:
                self._blocked_lister = blocked_lister
        if not self.stream_adapters or self.stream_credentials is None or self.current_state_reader is None:
            raise HandoffError(
                "continuous stream mode requires an approved authenticated adapter, "
                "credentials, and current-state read boundary"
            )
        return StreamObserver(
            self.stream_adapters,
            state,
            credentials=self.stream_credentials,
            current_state_reader=self.current_state_reader,
            stream_identity=self.stream_identity,
            backoff=self._stream_backoff,
            alert_after_consecutive_failures=(
                self.config.stream.alert_after_consecutive_failures
            ),
            alerter=self._stream_alert_sender,
            blocked_lister=self._blocked_lister,
        )

    def _stream_alert_sender(self, text: str) -> bool:
        """Deliver one self-health message through the journald log channel.

        Since the 2026-08-11 operator mute (no automatic Telegram messages),
        self-health alerts never leave the journal: the alert text is emitted
        as a structured ``stream_alert`` record through the daemon logger,
        which the systemd unit routes to journald.  Journal emission cannot
        fail, so the sender reports delivered and the caller records the
        durable ``alert_sent`` gate; recovery is never suppressed and the
        episode is not re-alerted on later failures.
        """

        self._log("stream_alert", text=text)
        return True

    def _merge_stream_wiring(self, fresh: Mapping[str, StreamAdapter]) -> None:
        """Refresh the watched board set without churning live connections.

        Boards removed from the wiring are closed; boards already watched keep
        their existing open connection (the freshly built duplicate adapter is
        discarded) so the daemon never connects/recv/closes per cycle.  An
        adapter is replaced only when its endpoint changed.
        """

        for board in list(self.stream_adapters):
            if board not in fresh:
                adapter = self.stream_adapters.pop(board)
                try:
                    adapter.close()
                except Exception:
                    pass
                self._stream_backoff.pop(board, None)
        for board, adapter in fresh.items():
            existing = self.stream_adapters.get(board)
            if existing is None:
                self.stream_adapters[board] = adapter
            elif existing is not adapter:
                if existing.endpoint != adapter.endpoint:
                    try:
                        existing.close()
                    except Exception:
                        pass
                    self.stream_adapters[board] = adapter
                    self._stream_backoff.pop(board, None)
                else:
                    # The fresh adapter is an unconnected duplicate of a board
                    # we already watch; keeping the live connection wins.
                    try:
                        adapter.close()
                    except Exception:
                        pass

    def _close_stream(self) -> None:
        """Release every persistent stream connection on daemon shutdown."""

        for adapter in self.stream_adapters.values():
            try:
                adapter.close()
            except Exception:
                pass
        self.stream_adapters.clear()
        self._stream_backoff.clear()
        if self.stream_observer is not None:
            try:
                self.stream_observer.close()
            except Exception:
                pass
            self.stream_observer = None

    def _destination(self) -> str:
        from .handoff import _validate_destination

        try:
            return _validate_destination(self.config)
        except HandoffError:
            return ""

    def _install_signal_handlers(self) -> None:
        for number in (signal.SIGTERM, signal.SIGINT):
            self._previous_handlers[number] = signal.getsignal(number)
            signal.signal(number, self._signal_handler)

    def _restore_signal_handlers(self) -> None:
        for number, previous in self._previous_handlers.items():
            signal.signal(number, previous)
        self._previous_handlers.clear()

    def _signal_handler(self, number: int, frame: FrameType | None) -> None:
        del frame
        self.request_stop(signal.Signals(number).name)

    def _log(self, event: str, **fields: object) -> None:
        # Key/value JSON is safe for journald and intentionally excludes argv.
        record = {"event": event, "instance": self.config.instance_name, **fields}
        self.logger.info(json.dumps(record, sort_keys=True, default=str))

    def _log_report(self, cycle_id: str, report: HandoffReport) -> None:
        destination = self._destination()
        for line in report.lines:
            self._log(
                "handoff",
                cycle_id=cycle_id,
                line=_safe_error(line, destination),
            )


def _safe_error(value: str, destination: str) -> str:
    if destination:
        return value.replace(destination, "<telegram-destination>")
    return value


__all__ = [
    "DEFAULT_EVENT_BATCH_SIZE",
    "DEFAULT_POLL_INTERVAL",
    "CycleResult",
    "DaemonRuntime",
    "EventObserver",
    "StreamObserver",
    "InstanceLock",
    "LockError",
    "ObservationBatch",
]
