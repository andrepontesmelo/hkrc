from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import socket
import threading

import pytest

from hkrc.config import ControllerConfig
from hkrc.event_stream import StreamAdapter, StreamCredentials, StreamSocket
from hkrc.handoff import NativeResult
from hkrc.runtime import DaemonRuntime, InstanceLock, StreamObserver
from hkrc.state import ControllerState, StateError


@dataclass
class FakeSocket:
    frames: list[str]
    closed: bool = False

    def recv(self) -> str:
        if not self.frames:
            raise StopIteration
        return self.frames.pop(0)

    def close(self) -> None:
        self.closed = True


@dataclass
class SilentSocket:
    closed: bool = False

    def recv(self) -> str:
        raise socket.timeout("silent stream")

    def close(self) -> None:
        self.closed = True


class Connector:
    def __init__(self, socket: StreamSocket | None = None, error: Exception | None = None):
        self.socket = socket
        self.error = error
        self.calls: list[str] = []

    def __call__(self, url: str, _headers: dict[str, str]) -> StreamSocket:
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        assert self.socket is not None
        return self.socket


def event(event_id: int, task_id: str = "task-1", *, kind: str = "blocked", payload: object | None = None) -> dict[str, object]:
    if payload is None:
        payload = {"kind": "capability", "reason": "runtime-test"} if kind == "blocked" else {}
    return {"id": event_id, "task_id": task_id, "run_id": 1, "kind": kind, "payload": payload, "created_at": 100}


def frame(*events: dict[str, object]) -> str:
    return json.dumps({"events": list(events), "cursor": events[-1]["id"] if events else 0})


def config_for(tmp_path: Path) -> ControllerConfig:
    return ControllerConfig("test", tmp_path / "native-do-not-touch", tmp_path / "state.sqlite3", native_cli="fake-hermes", telegram_chat_id="-1000")


def adapter_for(board: str, socket: StreamSocket | None = None, *, connector: Connector | None = None) -> StreamAdapter:
    return StreamAdapter(f"wss://example.test/{board}/events", allowed_boards={board}, connector=connector or Connector(socket))


def current_state(_board: str, task_id: str):
    return {"task_id": task_id, "status": "blocked", "block_kind": "capability"}


def test_stream_observer_classifies_and_reserves_only_actionable_events(tmp_path: Path) -> None:
    socket = FakeSocket([frame(event(2, "capability-task"), event(5, "input-task", payload={"kind": "needs_input"}), event(9, "running-task", kind="heartbeat"))])
    config = config_for(tmp_path)
    connector = Connector(socket)
    adapter = adapter_for("main", connector=connector)

    def reader(_board: str, task_id: str):
        return {"task_id": task_id, "status": "blocked" if task_id != "running-task" else "running", "block_kind": "capability" if task_id == "capability-task" else "needs_input" if task_id == "input-task" else None}

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observed = StreamObserver({"main": adapter}, state, credentials=StreamCredentials(ticket="opaque-ticket"), current_state_reader=reader).poll()
        assert [item.id for item in observed.events] == [2, 5, 9]
        assert observed.reserved == 1
        assert observed.skipped == 2
        assert state.get_stream_cursor("main").cursor == 9
        assert state.reservation_count() == 1
        assert connector.calls and "since=0" in connector.calls[0]
    assert not (tmp_path / "native-do-not-touch").exists()


def test_runtime_reserves_before_official_native_mutations(tmp_path: Path) -> None:
    socket = FakeSocket([frame(event(7))])
    connector = Connector(socket)
    adapter = adapter_for("main", connector=connector)
    config = config_for(tmp_path)
    calls: list[str] = []

    def runner(command):
        calls.append(command[command.index("kanban") + 3])
        return NativeResult(0)

    runtime = DaemonRuntime(config, stream_adapters={"main": adapter}, stream_credentials=StreamCredentials(ticket="ticket"), current_state_reader=current_state, runner=runner)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        result = runtime.run_cycle(state)
        assert result.error is None
        assert result.observed == 1
        assert result.report is not None and result.report.reserved == 1
        assert calls == ["notify-subscribe", "comment", "reassign", "unblock"]
        assert state.get_stream_cursor("main").cursor == 7
        assert state.reservation_count() == 1
    assert not (tmp_path / "native-do-not-touch").exists()


def test_reconcile_sweep_reserves_silent_death_blocks_stream_missed(tmp_path: Path) -> None:
    # The stream delivered nothing (empty frame); the blocked-state sweep is
    # the only observer that sees the death-blocked task.
    socket = FakeSocket([frame()])
    adapter = adapter_for("main", socket)
    config = config_for(tmp_path)

    def blocked_lister(board_slug: str) -> list[str]:
        assert board_slug == "main"
        return ["t_dead"]

    def reader(_board: str, task_id: str):
        if task_id == "t_dead":
            return {
                "task_id": task_id,
                "status": "blocked",
                "block_kind": None,
                "latest_event_kind": "gave_up",
                "latest_event_id": 55,
                "run_error": "iteration budget exhausted",
            }
        return {"task_id": task_id, "status": "ready", "block_kind": None}

    runtime = DaemonRuntime(
        config,
        stream_adapters={"main": adapter},
        stream_credentials=StreamCredentials(ticket="ticket"),
        current_state_reader=reader,
        blocked_lister=blocked_lister,
        reconcile_interval_cycles=1,
    )
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        first = runtime.run_cycle(state)
        assert first.report is not None and first.report.reserved == 1
        assert state.reservation_count() == 1
        # Second cycle: sweep skips the already-reserved task.
        second = runtime.run_cycle(state)
        assert second.report is not None and second.report.reserved == 0
        assert state.reservation_count() == 1


def test_reconcile_sweep_skips_config_defect_and_typed_kinds(tmp_path: Path) -> None:
    socket = FakeSocket([frame()])
    adapter = adapter_for("main", socket)
    config = config_for(tmp_path)

    def blocked_lister(_board_slug: str) -> list[str]:
        return ["t_capzero", "t_typed"]

    def reader(_board: str, task_id: str):
        if task_id == "t_capzero":
            return {
                "task_id": task_id,
                "status": "blocked",
                "block_kind": None,
                "latest_event_kind": "gave_up",
                "latest_event_id": 55,
                "run_error": "elapsed 61s > limit 0s",
            }
        return {
            "task_id": task_id,
            "status": "blocked",
            "block_kind": "needs_input",
            "latest_event_kind": "blocked",
            "latest_event_id": 56,
            "run_error": None,
        }

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observed = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(ticket="ticket"),
            current_state_reader=reader,
            blocked_lister=blocked_lister,
        )
        assert observed.reconcile_blocked_state() == 0
        assert state.reservation_count() == 0


def test_runtime_replay_uses_durable_cursor_and_one_ever_reservation(tmp_path: Path) -> None:
    socket = FakeSocket([frame(event(7)), frame(event(12))])
    adapter = adapter_for("main", socket)
    config = config_for(tmp_path)
    calls: list[list[str]] = []
    runtime = DaemonRuntime(config, stream_adapters={"main": adapter}, stream_credentials=StreamCredentials(ticket="ticket"), current_state_reader=current_state, runner=lambda command: calls.append(list(command)) or NativeResult(0))
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        first = runtime.run_cycle(state)
        second = runtime.run_cycle(state)
        assert first.report is not None and first.report.completed == 1
        assert second.report is not None and second.report.reserved == 0
        assert second.observed == 1
        assert state.get_stream_cursor("main").cursor == 12
        assert state.stream_event_count("main") == 2
        assert state.reservation_count() == 1
        assert len(calls) == 4


def test_board_local_auth_failure_does_not_stop_healthy_board(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    bad = adapter_for("bad", connector=Connector(error=PermissionError("not authorized")))
    good = adapter_for("good", connector=Connector(FakeSocket([frame(event(3, "healthy-task"))])))
    runtime = DaemonRuntime(config, stream_adapters={"bad": bad, "good": good}, stream_credentials=StreamCredentials(token="session"), current_state_reader=current_state)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        result = runtime.run_cycle(state)
        assert result.error is None
        assert result.observed == 1
        assert state.get_stream_cursor("bad").cursor == 0
        assert state.get_stream_cursor("bad").last_transport_error == "auth_failed: stream authentication failed"
        assert state.get_stream_cursor("good").cursor == 3
        assert state.reservation_count() == 1


def test_silent_stream_stays_idle_connected_without_transport_error(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    silent_socket = SilentSocket()
    connector = Connector(silent_socket)
    runtime = DaemonRuntime(
        config,
        stream_adapters={"main": adapter_for("main", connector=connector)},
        stream_credentials=StreamCredentials(token="session"),
        current_state_reader=current_state,
    )

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        first = runtime.run_cycle(state)
        second = runtime.run_cycle(state)

        assert first.error is None and second.error is None
        assert first.observed == 0 and second.observed == 0
        # One connection serves both cycles: an idle board is polled in place,
        # never reconnected, and never recorded as a transport failure.
        assert len(connector.calls) == 1
        assert silent_socket.closed is False
        assert state.get_stream_cursor("main").cursor == 0
        assert state.get_stream_cursor("main").last_transport_error is None


def test_continuous_mode_fails_closed_without_authenticated_adapter(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        result = DaemonRuntime(config).run_cycle(state)
        assert result.error is not None and "approved authenticated adapter" in result.error
        assert state.reservation_count() == 0
    assert not (tmp_path / "native-do-not-touch").exists()


def test_current_state_unavailable_is_fatal_and_cursor_does_not_advance(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    adapter = adapter_for("main", FakeSocket([frame(event(4))]))
    runtime = DaemonRuntime(config, stream_adapters={"main": adapter}, stream_credentials=StreamCredentials(token="session"), current_state_reader=lambda _board, _task: None)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        with pytest.raises(StateError, match="current state unavailable"):
            runtime.run_cycle(state)
        assert state.get_stream_cursor("main").cursor == 0
        assert state.reservation_count() == 0


def test_controller_state_commit_failure_is_fatal(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    adapter = adapter_for("main", FakeSocket([frame(event(4))]))
    runtime = DaemonRuntime(config, stream_adapters={"main": adapter}, stream_credentials=StreamCredentials(token="session"), current_state_reader=current_state)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        def fail_commit(*_args, **_kwargs):
            raise RuntimeError("simulated state failure")
        state.commit_stream_frame = fail_commit  # type: ignore[method-assign]
        with pytest.raises(StateError, match="controller state operation commit_stream_frame failed"):
            runtime.run_cycle(state)


def test_runtime_run_releases_instance_lock_after_stream_startup_failure(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    ControllerState.initialize(config.state_db, config.instance_name).close()

    # Missing stream wiring is a cycle-level fail-closed error.  The daemon still
    # owns the normal supervised lifecycle and must release its lock on exit.
    assert DaemonRuntime(config).run(max_cycles=1, install_signals=False) == 0
    lock = InstanceLock(config.lock_path)
    lock.acquire()
    lock.release()


def test_runtime_serializes_overlapping_cycles(tmp_path: Path) -> None:
    import threading
    import time

    config = config_for(tmp_path)
    entered = threading.Event()
    release = threading.Event()
    adapter = adapter_for("main", FakeSocket([frame(event(4))]))

    def runner(_command):
        entered.set()
        release.wait(timeout=2)
        return NativeResult(0)

    runtime = DaemonRuntime(
        config,
        stream_adapters={"main": adapter},
        stream_credentials=StreamCredentials(token="session"),
        current_state_reader=current_state,
        runner=runner,
    )
    ControllerState.initialize(config.state_db, config.instance_name).close()
    errors: list[BaseException] = []

    def invoke() -> None:
        state = ControllerState.open_existing(config.state_db)
        try:
            runtime.run_cycle(state)
        except BaseException as exc:
            errors.append(exc)
        finally:
            state.close()

    first = threading.Thread(target=invoke)
    second = threading.Thread(target=invoke)
    first.start()
    assert entered.wait(timeout=2)
    second.start()
    time.sleep(0.05)
    assert second.is_alive()
    release.set()
    first.join(timeout=2)
    second.join(timeout=2)
    assert errors == []


def test_runtime_reresolves_board_set_each_cycle_without_connection_churn(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)
    beta_socket = SilentSocket()
    connectors = {
        "alpha": Connector(SilentSocket()),
        "beta": Connector(beta_socket),
    }
    adapters = {
        board: adapter_for(board, connector=connectors[board]) for board in connectors
    }

    def builder():
        return dict(adapters), StreamCredentials(ticket="ticket"), current_state, None

    runtime = DaemonRuntime(
        config,
        wiring_builder=builder,
        runner=lambda command: NativeResult(0),
    )
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        first = runtime.run_cycle(state)
        assert first.error is None
        assert len(connectors["alpha"].calls) == 1
        assert len(connectors["beta"].calls) == 1

        # A board created while the daemon runs is picked up on the next cycle
        # with a fresh cursor; existing boards keep their ONE open connection
        # instead of reconnecting (no per-cycle connect/recv/close churn).
        connectors["gamma"] = Connector(SilentSocket())
        adapters["gamma"] = adapter_for("gamma", connector=connectors["gamma"])
        second = runtime.run_cycle(state)
        assert second.error is None
        assert len(connectors["gamma"].calls) == 1
        assert len(connectors["alpha"].calls) == 1
        assert len(connectors["beta"].calls) == 1
        assert state.get_stream_cursor("gamma").cursor == 0

        # An archived board stops being polled without a restart: it is closed
        # and never reconnected; still-watched boards keep their connections.
        del adapters["beta"]
        third = runtime.run_cycle(state)
        assert third.error is None
        assert len(connectors["alpha"].calls) == 1
        assert len(connectors["gamma"].calls) == 1
        assert len(connectors["beta"].calls) == 1
        assert beta_socket.closed is True


def test_transport_failure_triggers_reconnect_with_cursor_continuity(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)

    class ScriptedConnector:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def __call__(self, url: str, _headers: dict[str, str]) -> StreamSocket:
            self.calls.append(url)
            if len(self.calls) == 1:
                return FakeSocket([frame(event(5, "task-1"))])
            return FakeSocket([frame(event(9, "task-1"))])

    connector = ScriptedConnector()
    adapter = adapter_for("main", connector=connector)  # type: ignore[arg-type]

    class Clock:
        value = 0.0

        def __call__(self) -> float:
            return self.value

    clock = Clock()
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observer = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=current_state,
            monotonic=clock,
        )
        first = observer.poll()
        assert first.reserved == 1
        assert state.get_stream_cursor("main").cursor == 5
        assert len(connector.calls) == 1

        # Connection 1 is exhausted: the next poll sees a transport failure,
        # records it fail-closed, and defers the reconnect by the backoff.
        second = observer.poll()
        assert second.events == ()
        assert state.get_stream_cursor("main").last_transport_error == (
            "disconnected: stream disconnected"
        )
        assert len(connector.calls) == 1

        # Backoff elapses; the reconnect resumes from the durable cursor.
        clock.value = 10.0
        third = observer.poll()
        assert len(third.events) == 1
        assert [item.id for item in third.events] == [9]
        assert third.reserved == 0
        assert len(connector.calls) == 2
        assert "since=5" in connector.calls[1]
        assert state.get_stream_cursor("main").cursor == 9


def test_poll_stops_between_boards_when_shutdown_requested(tmp_path: Path) -> None:
    # A SIGTERM mid-cycle must not drain every board before the daemon exits:
    # the stop probe is checked before each board's socket work.  The stop
    # fires while the first board's frame is being classified, so the second
    # board's connector must never be invoked.
    config = config_for(tmp_path)
    alpha_connector = Connector(FakeSocket([frame(event(2))]))
    beta_connector = Connector(FakeSocket([frame(event(9))]))
    adapters = {
        "alpha": adapter_for("alpha", connector=alpha_connector),
        "beta": adapter_for("beta", connector=beta_connector),
    }
    stop = threading.Event()

    def probe() -> bool:
        return stop.is_set()

    def reader(_board: str, task_id: str):
        stop.set()  # fire the probe during the first board's classification
        return current_state(_board, task_id)

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observer = StreamObserver(
            adapters,
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=reader,
        )
        observed = observer.poll(stop_requested=probe)
        assert observed.reserved == 1
        assert [item.id for item in observed.events] == [2]
        # Cursor advanced only for the board that was polled before the stop.
        assert state.get_stream_cursor("alpha").cursor == 2
        assert state.get_stream_cursor("beta").cursor == 0
        assert beta_connector.calls == []
        # A stop requested before the next poll ends the sweep immediately.
        empty = observer.poll(stop_requested=probe)
        assert empty.events == ()
        assert empty.reserved == 0


def test_poll_abandons_frame_whose_tail_was_not_classified(tmp_path: Path) -> None:
    # A stop probe firing mid-frame must not durably commit a frame whose
    # tail was never classified: the cursor stays put so the whole frame
    # re-delivers after restart and is classified fresh.
    config = config_for(tmp_path)
    adapter = adapter_for(
        "main", FakeSocket([frame(event(2), event(5), event(9))])
    )
    stop = threading.Event()

    def probe() -> bool:
        return stop.is_set()

    def reader(_board: str, task_id: str):
        if task_id == "task-1":
            stop.set()  # fire the probe after the first classification
        return current_state(_board, task_id)

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observer = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=reader,
        )
        observed = observer.poll(stop_requested=probe)
        assert observed.events == ()
        assert observed.reserved == 0
        # Frame was abandoned: cursor never advanced past the partial tail.
        assert state.get_stream_cursor("main").cursor == 0
        assert state.reservation_count() == 0


def test_reconcile_stops_between_tasks_when_shutdown_requested(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    adapter = adapter_for("main", FakeSocket([frame()]))
    stop = threading.Event()
    reads: list[str] = []

    def blocked_lister(board_slug: str) -> list[str]:
        assert board_slug == "main"
        return ["t_first", "t_second"]

    def reader(_board: str, task_id: str):
        reads.append(task_id)
        if task_id == "t_first":
            stop.set()  # fire the probe after the first task read
        return {"task_id": task_id, "status": "blocked", "block_kind": None, "latest_event_kind": "gave_up", "run_error": None}

    def probe() -> bool:
        return stop.is_set()

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observer = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=reader,
            blocked_lister=blocked_lister,
        )
        reserved = observer.reconcile_blocked_state(stop_requested=probe)
        assert reserved == 1
        assert reads == ["t_first"]
        assert state.has_reservation("main", "t_first")
        assert not state.has_reservation("main", "t_second")


@dataclass
class BlockingSocket:
    entered: threading.Event
    release: threading.Event
    closed: bool = False

    def recv(self) -> str:
        self.entered.set()
        self.release.wait(timeout=10)
        raise socket.timeout("blocked socket released")

    def close(self) -> None:
        self.closed = True


def test_daemon_run_exits_promptly_when_stop_requested_mid_cycle(tmp_path: Path) -> None:
    # End-to-end: a stop requested while a cycle is in flight must return from
    # run() without waiting for the remaining boards' socket work.  The stop is
    # fired while the first board's frame is being classified; the second
    # board's socket never gets a chance to block the daemon.
    config = config_for(tmp_path)
    reader_entered = threading.Event()
    reader_release = threading.Event()

    def reader(_board: str, task_id: str):
        reader_entered.set()
        reader_release.wait(timeout=10)
        return current_state(_board, task_id)

    beta_blocker = BlockingSocket(
        threading.Event(), threading.Event()
    )
    adapters = {
        "alpha": adapter_for("alpha", connector=Connector(FakeSocket([frame(event(2))]))),
        "beta": adapter_for("beta", connector=Connector(beta_blocker)),
    }
    runtime = DaemonRuntime(
        config,
        stream_adapters=adapters,
        stream_credentials=StreamCredentials(token="session"),
        current_state_reader=reader,
        runner=lambda command: NativeResult(0),
    )
    ControllerState.initialize(config.state_db, config.instance_name).close()
    result: list[int] = []
    errors: list[BaseException] = []

    def run_daemon() -> None:
        try:
            result.append(runtime.run(max_cycles=None, install_signals=False))
        except BaseException as exc:  # pragma: no cover - failure surface
            errors.append(exc)

    thread = threading.Thread(target=run_daemon)
    thread.start()
    # Wait until the daemon is inside the first board's classification, then
    # request a stop mid-cycle.
    assert reader_entered.wait(timeout=5), "daemon never began a cycle"
    runtime.request_stop("test-mid-cycle")
    reader_release.set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "daemon did not exit after stop request"
    assert errors == []
    assert result == [0]
    # The beta board's socket was never touched: no connect, no recv.
    assert beta_blocker.entered.is_set() is False
