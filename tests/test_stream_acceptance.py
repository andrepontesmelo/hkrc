from __future__ import annotations

from pathlib import Path
from typing import Callable

import pytest

from fixtures.event_stream import (
    StreamEvent,
    StreamScenario,
    WebSocketScenario,
)
from hkrc.classifier import (
    ClassificationKind,
    CurrentTaskState,
    NormalizedEvent,
    PayloadState,
    StreamError,
    classify_event,
    classify_stream_error,
)
from hkrc.config import ControllerConfig
from hkrc.event_stream import (
    EventBatch,
    PayloadState as AdapterPayloadState,
    StreamAdapter,
    StreamCredentials,
    StreamErrorCode,
)
from hkrc.handoff import NativeResult, execute_handoff
from hkrc.runtime import DaemonRuntime, StreamObserver
from hkrc.state import ControllerState, StateError, StreamEventKey


NOW = 10_000
_DEFAULT_PAYLOAD = object()


def stream_event(
    event_id: int,
    *,
    task_id: str = "task-1",
    run_id: int | str | None = 7,
    kind: str = "blocked",
    payload: object = _DEFAULT_PAYLOAD,
    created_at: int | None = None,
) -> StreamEvent:
    if payload is _DEFAULT_PAYLOAD and kind == "blocked":
        payload = {"kind": "capability", "reason": "acceptance"}
    return StreamEvent(
        event_id,
        kind,
        payload,
        task_id=task_id,
        run_id=run_id,
        created_at=event_id if created_at is None else created_at,
    )


def adapter_for(
    scenario: WebSocketScenario,
    *,
    board: str = "main",
) -> StreamAdapter:
    return StreamAdapter(
        "wss://dashboard.example.test/api/kanban/events?format=batch",
        allowed_boards={board},
        connector=scenario.connector,
    )


def current(
    task_id: str = "task-1",
    *,
    status: str = "blocked",
    block_kind: str | None = "capability",
    latest_run_id: int | str | None = 7,
) -> CurrentTaskState:
    return CurrentTaskState(
        task_id=task_id,
        status=status,
        block_kind=block_kind,
        latest_run_id=latest_run_id,
    )


def event_for_classifier(
    event_id: int = 1,
    *,
    kind: str = "blocked",
    payload: object = None,
    payload_state: PayloadState = PayloadState.OBJECT,
) -> NormalizedEvent:
    if payload is None and payload_state is PayloadState.OBJECT:
        payload = {"kind": "capability", "reason": "oracle"}
    return NormalizedEvent(
        board_slug="main",
        event_id=event_id,
        task_id="task-1",
        run_id=7,
        kind=kind,
        payload=payload,
        payload_state=payload_state,
        created_at=NOW,
    )


def config_for(tmp_path: Path) -> ControllerConfig:
    return ControllerConfig(
        "acceptance",
        tmp_path / "native-do-not-touch",
        tmp_path / "state.sqlite3",
        native_cli="fake-hermes",
        telegram_chat_id="-1000",
    )


# The classifier contract is intentionally kept as an explicit oracle. The
# numbered cases correspond to the 17 contract decisions in the design map.
ORACLE_CASES: tuple[
    tuple[str, Callable[[], object], ClassificationKind, bool], ...
] = (
    (
        "01-typed-capability",
        lambda: classify_event(
            event_for_classifier(), current(),
        ),
        ClassificationKind.ACTIONABLE_TYPED_CAPABILITY,
        True,
    ),
    (
        "02-needs-input",
        lambda: classify_event(
            event_for_classifier(payload={"kind": "needs_input"}),
            current(block_kind="needs_input"),
        ),
        ClassificationKind.HUMAN_INPUT_REQUIRED,
        False,
    ),
    (
        "03-transient",
        lambda: classify_event(
            event_for_classifier(payload={"kind": "transient"}),
            current(block_kind="transient"),
        ),
        ClassificationKind.TRANSIENT_RETRYABLE,
        False,
    ),
    (
        "04-dependency-wait",
        lambda: classify_event(
            event_for_classifier(kind="dependency_wait", payload={"kind": "dependency"}),
            current(block_kind="dependency"),
        ),
        ClassificationKind.DEPENDENCY_WAIT,
        False,
    ),
    (
        "05-typed-dependency-block",
        lambda: classify_event(
            event_for_classifier(payload={"kind": "dependency"}),
            current(block_kind="dependency"),
        ),
        ClassificationKind.DEPENDENCY_WAIT,
        False,
    ),
    (
        "06-gave-up-circuit-breaker",
        lambda: classify_event(
            event_for_classifier(
                kind="gave_up",
                payload={"error": "timed out", "trigger_outcome": "timed_out"},
            ),
            current(block_kind=None, latest_run_id=7),
        ),
        ClassificationKind.ACTIONABLE_CIRCUIT_BREAKER,
        True,
    ),
    (
        "07-crashed-audit-only",
        lambda: classify_event(
            event_for_classifier(kind="crashed"),
            current(status="ready", block_kind=None),
        ),
        ClassificationKind.AUDIT_ONLY_FAILURE,
        False,
    ),
    (
        "08-timed-out-audit-only",
        lambda: classify_event(
            event_for_classifier(kind="timed_out"),
            current(status="ready", block_kind=None),
        ),
        ClassificationKind.AUDIT_ONLY_FAILURE,
        False,
    ),
    (
        "09-protocol-violation-audit-only",
        lambda: classify_event(
            event_for_classifier(kind="protocol_violation"),
            current(status="ready", block_kind=None),
        ),
        ClassificationKind.AUDIT_ONLY_FAILURE,
        False,
    ),
    (
        "10-current-terminal",
        lambda: classify_event(event_for_classifier(), current(status="done")),
        ClassificationKind.TERMINAL_SUCCESS,
        False,
    ),
    (
        "11-terminal-event",
        lambda: classify_event(
            event_for_classifier(kind="completed", payload={}), current()
        ),
        ClassificationKind.TERMINAL_SUCCESS,
        False,
    ),
    (
        "12-unblocked-lifecycle",
        lambda: classify_event(
            event_for_classifier(kind="unblocked", payload={}), current()
        ),
        ClassificationKind.LIFECYCLE_CLEARING,
        False,
    ),
    (
        "13-current-triage",
        lambda: classify_event(event_for_classifier(), current(status="triage")),
        ClassificationKind.HUMAN_TRIAGE_REQUIRED,
        False,
    ),
    (
        "14-block-loop-triage",
        lambda: classify_event(
            event_for_classifier(kind="block_loop_detected", payload={}), current()
        ),
        ClassificationKind.HUMAN_TRIAGE_REQUIRED,
        False,
    ),
    (
        "15-malformed-payload",
        lambda: classify_event(
            event_for_classifier(
                payload=None, payload_state=PayloadState.MALFORMED_JSON
            ),
            current(),
        ),
        ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK,
        False,
    ),
    (
        "16-unknown-event",
        lambda: classify_event(
            event_for_classifier(kind="future_event", payload={}), current()
        ),
        ClassificationKind.UNKNOWN_EVENT,
        False,
    ),
    (
        "17-transport-error",
        lambda: classify_stream_error(
            StreamError(code="disconnected", message="socket closed")
        ),
        ClassificationKind.TRANSPORT_ERROR,
        False,
    ),
)


@pytest.mark.parametrize(
    ("case", "evaluate", "expected", "reserve"),
    ORACLE_CASES,
    ids=[case[0] for case in ORACLE_CASES],
)
def test_17_case_classification_oracle(
    case: str,
    evaluate: Callable[[], object],
    expected: ClassificationKind,
    reserve: bool,
) -> None:
    result = evaluate()
    assert result.classification is expected, case  # type: ignore[attr-defined]
    assert result.reserve is reserve  # type: ignore[attr-defined]


def test_fixture_drives_exact_batch_envelope_and_payload_identity() -> None:
    scenario = WebSocketScenario.from_batches(
        [
            [
                stream_event(2, payload=None),
                stream_event(7, task_id="task-2", payload=["not", "object"]),
                stream_event(11, kind="future_kind", payload={"status": "blocked"}),
            ]
        ]
    )
    adapter = adapter_for(scenario)

    assert adapter.connect("main", 0, StreamCredentials(ticket="opaque-ticket")) is None
    batch = adapter.recv()

    assert isinstance(batch, EventBatch)
    assert batch.cursor == 11
    assert [item.id for item in batch.events] == [2, 7, 11]
    assert batch.events[0].payload_state is AdapterPayloadState.NULL
    assert batch.events[1].payload_state is AdapterPayloadState.MALFORMED
    assert batch.events[1].task_id == "task-2"
    assert batch.events[2].kind == "future_kind"
    assert scenario.query(scenario.calls[0][0]) == {"board": "main", "since": "0"}
    assert scenario.calls[0][1] == {}


def test_fixture_reconnect_uses_last_accepted_cursor_and_preserves_sparse_ids() -> None:
    scenario = WebSocketScenario.from_batches(
        [[stream_event(3)], [stream_event(12)]]
    )
    adapter = adapter_for(scenario)
    credentials = StreamCredentials(token="session")

    assert adapter.connect("main", 0, credentials) is None
    first = adapter.recv()
    assert isinstance(first, EventBatch) and first.cursor == 3
    adapter.close()

    assert adapter.connect("main", 3, credentials) is None
    second = adapter.recv()
    assert isinstance(second, EventBatch) and second.cursor == 12
    assert [scenario.query(call[0])["since"] for call in scenario.calls] == ["0", "3"]


def test_observer_end_to_end_reserves_only_actionable_stream_events(tmp_path: Path) -> None:
    scenario = WebSocketScenario.from_batches(
        [
            [
                stream_event(2, task_id="capability-task"),
                stream_event(
                    5,
                    task_id="input-task",
                    payload={"kind": "needs_input", "reason": "ask Andre"},
                ),
                stream_event(9, task_id="running-task", kind="heartbeat", payload={}),
            ]
        ]
    )
    adapter = adapter_for(scenario)
    config = config_for(tmp_path)

    def reader(_board: str, task_id: str) -> CurrentTaskState:
        if task_id == "capability-task":
            return current(task_id)
        if task_id == "input-task":
            return current(task_id, block_kind="needs_input")
        return current(task_id, status="running", block_kind=None)

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observed = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(ticket="ticket"),
            current_state_reader=reader,
        ).poll()

        assert [event.id for event in observed.events] == [2, 5, 9]
        assert observed.reserved == 1
        assert observed.skipped == 2
        assert state.reservation_count() == 1
        assert state.get_stream_cursor("main").cursor == 9
        assert state.stream_event_count("main") == 3
    assert not (tmp_path / "native-do-not-touch").exists()


def test_runtime_proves_reservation_precedes_each_native_mutation(tmp_path: Path) -> None:
    scenario = WebSocketScenario.from_batches([[stream_event(7)]])
    adapter = adapter_for(scenario)
    config = config_for(tmp_path)
    calls: list[list[str]] = []
    state_ref: list[ControllerState] = []

    def runner(command: list[str]) -> NativeResult:
        assert state_ref[0].started_intervention_count() == 1
        calls.append(list(command))
        return NativeResult(0)

    runtime = DaemonRuntime(
        config,
        stream_adapters={"main": adapter},
        stream_credentials=StreamCredentials(ticket="ticket"),
        current_state_reader=lambda _board, task_id: current(task_id),
        runner=runner,
    )
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        state_ref.append(state)
        result = runtime.run_cycle(state)
        assert result.error is None
        assert result.report is not None
        assert result.report.completed == 1
        assert [command[command.index("kanban") + 3] for command in calls] == [
            "notify-subscribe",
            "comment",
            "reassign",
            "unblock",
        ]
        assert state.reservation_count() == 1
        assert state.get_stream_cursor("main").cursor == 7
    assert not (tmp_path / "native-do-not-touch").exists()


def test_reconnect_and_duplicate_replay_after_identity_reset_are_idempotent(
    tmp_path: Path,
) -> None:
    scenario = WebSocketScenario.from_batches(
        [[stream_event(5, task_id="same-task")], [stream_event(5, task_id="same-task")]]
    )
    adapter = adapter_for(scenario)
    config = config_for(tmp_path)
    identity = {"main": "generation-a"}

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observer = StreamObserver(
            {"main": adapter},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=lambda _board, task_id: current(task_id),
            stream_identity=identity,
        )
        first = observer.poll()
        assert first.reserved == 1
        assert state.get_stream_cursor("main").cursor == 5

        # A board replacement resets only the accepted cursor. The durable
        # handled-event and reservation keys survive, so replay is harmless.
        identity["main"] = "generation-b"
        second = observer.poll()
        cursor = state.get_stream_cursor("main")
        assert second.reserved == 0
        assert second.skipped == 1
        assert cursor.cursor == 5
        assert cursor.reset_required is False
        assert cursor.reset_reason is None
        assert cursor.reset_count == 1
        assert state.reservation_count() == 1
        assert state.stream_event_count("main") == 1
        assert [scenario.query(call[0])["since"] for call in scenario.calls] == ["0", "0"]


def test_retention_reset_preserves_handled_identity_and_reservation(tmp_path: Path) -> None:
    scenario = StreamScenario.from_events([stream_event(4), stream_event(5)])
    scenario.retain_from(5)

    with ControllerState.initialize(tmp_path / "state.sqlite3", "retention") as state:
        state.reconcile_stream_cursor("main", identity="generation-a")
        state.reserve_stream_event(
            "main", "task-1", blocker_kind="capability", latest_event_at=4
        )
        state.commit_stream_frame(
            "main",
            identity="generation-a",
            cursor=4,
            events=(StreamEventKey(4, "task-1", 7),),
        )
        reset = state.reconcile_stream_cursor(
            "main", identity="generation-a", retention_floor=scenario.retention_floor
        )

        assert scenario.retention_floor == 5
        assert reset.cursor == 0
        assert reset.reset_required is True
        assert reset.reset_reason == "retention_gap"
        assert state.reservation_count() == 1
        assert state.stream_event_count("main") == 1


def test_malformed_frame_and_identity_fail_closed_without_cursor_advance() -> None:
    malformed_frames = [
        "not-json",
        '{"events":[{"id":1,"run_id":7,"kind":"blocked","payload":{},"created_at":1}],"cursor":1}',
    ]
    for raw in malformed_frames:
        scenario = WebSocketScenario(connections=[[raw]])
        adapter = adapter_for(scenario)
        assert adapter.connect("main", 0, StreamCredentials(token="session")) is None
        result = adapter.recv()
        assert result.code is StreamErrorCode.MALFORMED_FRAME
        assert result.retryable is False
        assert result.cursor == 0
        assert adapter.cursor == 0


def test_malformed_payload_and_unknown_kind_are_committed_but_never_reserved(
    tmp_path: Path,
) -> None:
    scenario = WebSocketScenario.from_batches(
        [
            [
                stream_event(1, payload=None),
                stream_event(2, payload=["wrong-shape"]),
                stream_event(3, kind="future_kind", payload={}),
            ]
        ]
    )
    config = config_for(tmp_path)
    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        observed = StreamObserver(
            {"main": adapter_for(scenario)},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=lambda _board, task_id: current(task_id),
        ).poll()
        assert observed.reserved == 0
        assert observed.skipped == 3
        assert state.reservation_count() == 0
        assert state.stream_event_count("main") == 3
        assert state.get_stream_cursor("main").cursor == 3


def test_board_local_auth_error_does_not_block_a_healthy_board(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    bad_scenario = WebSocketScenario(
        connect_errors=[PermissionError("unauthorized")]
    )
    good_scenario = WebSocketScenario.from_batches([[stream_event(8, task_id="healthy")]])
    runtime = DaemonRuntime(
        config,
        stream_adapters={
            "bad": adapter_for(bad_scenario, board="bad"),
            "good": adapter_for(good_scenario, board="good"),
        },
        stream_credentials=StreamCredentials(token="session"),
        current_state_reader=lambda _board, task_id: current(task_id),
    )

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        result = runtime.run_cycle(state)
        assert result.error is None
        assert result.observed == 1
        assert state.get_stream_cursor("bad").cursor == 0
        assert state.get_stream_cursor("bad").last_transport_error == (
            "auth_failed: stream authentication failed"
        )
        assert state.get_stream_cursor("good").cursor == 8
        assert state.reservation_count() == 1


def test_state_write_failure_is_fatal_after_reservation_and_before_native_calls(
    tmp_path: Path,
) -> None:
    scenario = WebSocketScenario.from_batches([[stream_event(4)]])
    config = config_for(tmp_path)
    native_calls: list[list[str]] = []
    runtime = DaemonRuntime(
        config,
        stream_adapters={"main": adapter_for(scenario)},
        stream_credentials=StreamCredentials(token="session"),
        current_state_reader=lambda _board, task_id: current(task_id),
        runner=lambda command: native_calls.append(list(command)) or NativeResult(0),
    )

    with ControllerState.initialize(config.state_db, config.instance_name) as state:
        def fail_commit(*_args: object, **_kwargs: object) -> object:
            raise RuntimeError("simulated state write failure")

        state.commit_stream_frame = fail_commit  # type: ignore[method-assign]
        with pytest.raises(StateError, match="commit_stream_frame"):
            runtime.run_cycle(state)
        assert state.get_stream_cursor("main").cursor == 0
        assert state.reservation_count() == 1
        assert native_calls == []


def test_one_shot_execute_handoff_remains_native_discovery_compatible(tmp_path: Path) -> None:
    # This uses only a synthetic, test-owned board fixture; continuous tests
    # above never open a native board database.
    from test_handoff import handoff_config, make_board, task

    root = tmp_path / "boards"
    make_board(root, "main", [task("one-shot", "capability", NOW)])
    state_path = tmp_path / "one-shot-state.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    with ControllerState.initialize(state_path, config.instance_name) as state:
        report = execute_handoff(
            config,
            state,
            now=NOW,
            runner=lambda command: calls.append(list(command)) or NativeResult(0),
        )

    assert report.completed == 1
    assert [command[command.index("kanban") + 3] for command in calls] == [
        "notify-subscribe",
        "comment",
        "reassign",
        "unblock",
    ]
    assert not (tmp_path / "native-do-not-touch").exists()


def test_stream_error_classification_remains_non_actionable() -> None:
    result = classify_stream_error(
        {"code": StreamErrorCode.TRANSPORT, "message": "socket closed"}
    )
    assert result.classification is ClassificationKind.TRANSPORT_ERROR
    assert result.actionable is False
    assert result.reserve is False
