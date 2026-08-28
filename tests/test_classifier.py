from __future__ import annotations

import pytest

from hkrc.classifier import (
    ClassificationKind,
    ClassifierInputError,
    CurrentTaskState,
    NormalizedEvent,
    PayloadState,
    StreamError,
    classify_event,
    classify_stream_error,
)
from hkrc.event_stream import (
    PayloadState as AdapterPayloadState,
    StreamError as AdapterStreamError,
    StreamErrorCode,
)


NOW = 10_000


def event(
    event_id: int = 1,
    kind: str = "blocked",
    payload: object | None = None,
    *,
    payload_state: PayloadState = PayloadState.OBJECT,
    board_slug: str = "default",
    task_id: str = "task-1",
    run_id: int | str | None = 7,
) -> NormalizedEvent:
    if payload is None and payload_state is PayloadState.OBJECT:
        payload = {"kind": "capability", "reason": "missing access"}
    return NormalizedEvent(
        board_slug=board_slug,
        event_id=event_id,
        task_id=task_id,
        run_id=run_id,
        kind=kind,
        payload=payload,
        payload_state=payload_state,
        created_at=NOW,
    )


def current(
    *,
    status: str = "blocked",
    block_kind: str | None = "capability",
    task_id: str = "task-1",
    latest_run_id: int | str | None = 7,
    run_outcome: str | None = None,
    run_error: str | None = None,
) -> CurrentTaskState:
    return CurrentTaskState(
        task_id=task_id,
        status=status,
        block_kind=block_kind,
        latest_run_id=latest_run_id,
        run_outcome=run_outcome,
        run_error=run_error,
    )


def test_01_typed_capability_is_actionable_after_current_confirmation() -> None:
    result = classify_event(event(), current())

    assert result.classification is ClassificationKind.ACTIONABLE_TYPED_CAPABILITY
    assert result.actionable is True
    assert result.reserve is True
    assert result.reservation_key == ("default", "task-1")
    assert "missing access" in result.reason


def test_02_needs_input_wins_over_gave_up_prose() -> None:
    result = classify_event(
        event(payload={"kind": "needs_input", "summary": "gave up waiting"}),
        current(block_kind="needs_input"),
    )

    assert result.classification is ClassificationKind.HUMAN_INPUT_REQUIRED
    assert not result.reserve


def test_03_transient_is_non_actionable() -> None:
    result = classify_event(event(payload={"kind": "transient"}), current(block_kind="transient"))

    assert result.classification is ClassificationKind.TRANSIENT_RETRYABLE
    assert not result.actionable
    assert not result.reserve


@pytest.mark.parametrize(
    ("event_kind", "payload"),
    [
        ("dependency_wait", {"kind": "dependency", "reason": "parent is open"}),
        ("blocked", {"kind": "dependency", "reason": "parent is open"}),
    ],
)
def test_04_dependency_wait_is_non_actionable(
    event_kind: str, payload: dict[str, str]
) -> None:
    result = classify_event(event(kind=event_kind, payload=payload), current(block_kind="dependency"))

    assert result.classification is ClassificationKind.DEPENDENCY_WAIT
    assert not result.reserve


def test_05_gave_up_requires_current_block_and_preserves_diagnostics() -> None:
    result = classify_event(
        event(
            kind="gave_up",
            payload={
                "error": "iteration budget exhausted",
                "trigger_outcome": "timed_out",
                "failures": 3,
            },
        ),
        current(block_kind=None, run_outcome="gave_up"),
    )

    assert result.classification is ClassificationKind.ACTIONABLE_CIRCUIT_BREAKER
    assert result.reserve
    assert "iteration budget exhausted" in result.reason
    assert "timed_out" in result.reason


def test_05b_gave_up_with_runtime_cap_zero_escalates_not_reserves() -> None:
    result = classify_event(
        event(
            kind="gave_up",
            payload={
                "error": "elapsed 61s > limit 0s",
                "trigger_outcome": "timed_out",
                "failures": 2,
            },
        ),
        current(block_kind=None, run_outcome="gave_up", run_error="elapsed 61s > limit 0s"),
    )

    assert result.classification is ClassificationKind.HUMAN_TRIAGE_REQUIRED
    assert not result.reserve
    assert "config_defect=per_task_runtime_cap_zero" in result.reason


def test_05c_gave_up_runtime_cap_zero_detected_from_payload_only() -> None:
    result = classify_event(
        event(kind="gave_up", payload={"error": "elapsed 62s > limit 0s"}),
        current(block_kind=None, run_outcome="gave_up"),
    )

    assert result.classification is ClassificationKind.HUMAN_TRIAGE_REQUIRED
    assert not result.reserve


@pytest.mark.parametrize("event_kind", ["crashed", "timed_out", "protocol_violation"])
def test_06_hard_failure_is_audit_only_without_current_block(event_kind: str) -> None:
    result = classify_event(event(kind=event_kind), current(status="ready", block_kind=None))

    assert result.classification is ClassificationKind.AUDIT_ONLY_FAILURE
    assert not result.reserve


def test_07_terminal_current_state_suppresses_stale_history() -> None:
    result = classify_event(event(payload={"kind": "capability"}), current(status="done"))

    assert result.classification is ClassificationKind.TERMINAL_SUCCESS
    assert not result.reserve


@pytest.mark.parametrize("event_kind", ["completed", "unblocked"])
def test_08_terminal_or_unblocked_event_never_reserves(event_kind: str) -> None:
    result = classify_event(event(kind=event_kind, payload={}), current(status="blocked"))

    assert result.classification in {
        ClassificationKind.TERMINAL_SUCCESS,
        ClassificationKind.LIFECYCLE_CLEARING,
    }
    assert not result.reserve


def test_09_triage_and_block_loop_are_human_only() -> None:
    triage = classify_event(event(kind="blocked", payload={"kind": "capability"}), current(status="triage"))
    loop = classify_event(event(kind="block_loop_detected", payload={"kind": "capability"}), current())

    assert triage.classification is ClassificationKind.HUMAN_TRIAGE_REQUIRED
    assert loop.classification is ClassificationKind.HUMAN_TRIAGE_REQUIRED
    assert not triage.reserve and not loop.reserve


@pytest.mark.parametrize(
    ("payload_state", "payload", "kind"),
    [
        (PayloadState.MALFORMED_JSON, None, "blocked"),
        (PayloadState.NULL, None, "blocked"),
        (PayloadState.OBJECT, {"kind": "future_kind"}, "blocked"),
        (PayloadState.OBJECT, {"kind": "capability"}, "future_kind"),
    ],
)
def test_10_malformed_null_and_unknown_data_fail_closed(
    payload_state: PayloadState, payload: object, kind: str
) -> None:
    result = classify_event(
        event(kind=kind, payload=payload, payload_state=payload_state),
        current(),
    )

    assert result.classification in {
        ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK,
        ClassificationKind.UNKNOWN_EVENT,
    }
    assert not result.actionable
    assert not result.reserve


def test_11_replay_is_pure_and_has_stable_event_and_reservation_keys() -> None:
    original = classify_event(event(event_id=44), current())
    replay = classify_event(event(event_id=44), current())

    assert replay == original
    assert original.event_key == ("default", 44)
    assert original.reservation_key == ("default", "task-1")


def test_12_board_is_part_of_reservation_identity() -> None:
    first = classify_event(event(board_slug="one"), current())
    second = classify_event(event(board_slug="two"), current())

    assert first.reserve and second.reserve
    assert first.reservation_key != second.reservation_key


def test_13_sparse_event_ids_do_not_change_pure_classification() -> None:
    result = classify_event(event(event_id=10_001), current())

    assert result.event_key == ("default", 10_001)
    assert result.reserve


def test_14_transport_errors_are_non_actionable() -> None:
    result = classify_stream_error(StreamError(code="transport", message="socket closed"))

    assert result.classification is ClassificationKind.TRANSPORT_ERROR
    assert not result.actionable
    assert not result.reserve


def test_15_classifier_does_not_attempt_state_or_native_mutation() -> None:
    state = current()
    result = classify_event(event(), state)

    assert result.reserve
    assert state == current()


def test_16_native_phase_failures_are_not_reclassified_as_blockers() -> None:
    result = classify_stream_error(StreamError(code="native_phase_failure", message="comment failed"))

    assert result.classification is ClassificationKind.TRANSPORT_ERROR
    assert not result.reserve


def test_17_invalid_event_identity_is_rejected_before_classification() -> None:
    with pytest.raises(ClassifierInputError, match="event_id"):
        classify_event(
            {
                "board_slug": "default",
                "event_id": -1,
                "task_id": "task-1",
                "run_id": None,
                "kind": "blocked",
                "payload": None,
                "payload_state": "null",
                "created_at": NOW,
            },
            current(),
        )

    valid_malformed = classify_event(
        event(payload=None, payload_state=PayloadState.MALFORMED_JSON),
        current(),
    )
    assert not valid_malformed.reserve


def test_adapter_normalized_event_and_error_are_accepted_without_transport_coupling() -> None:
    from hkrc.event_stream import StreamEvent

    result = classify_event(
        StreamEvent(
            id=1,
            task_id="task-1",
            run_id=7,
            kind="blocked",
            payload={"kind": "capability", "reason": "access"},
            payload_state=AdapterPayloadState.OBJECT,
            created_at=NOW,
        ),
        current(),
        board_slug="default",
    )
    error_result = classify_stream_error(
        AdapterStreamError(StreamErrorCode.TRANSPORT, "closed")
    )

    assert result.classification is ClassificationKind.ACTIONABLE_TYPED_CAPABILITY
    assert error_result.classification is ClassificationKind.TRANSPORT_ERROR


def test_current_run_mismatch_is_stale_and_fail_closed() -> None:
    result = classify_event(event(kind="gave_up"), current(latest_run_id=99))

    assert result.classification is ClassificationKind.STALE_EVENT
    assert not result.reserve


def test_current_latest_unblocked_suppresses_same_cursor_block_history() -> None:
    result = classify_event(
        event(event_id=44, payload={"kind": "capability", "reason": "old"}),
        CurrentTaskState(
            task_id="task-1",
            status="blocked",
            block_kind="capability",
            latest_run_id=7,
            latest_event_kind="unblocked",
            latest_event_id=44,
        ),
    )

    assert result.classification is ClassificationKind.STALE_EVENT
    assert result.reason == "current_state_supersedes_event"
    assert not result.reserve


def test_current_latest_completed_suppresses_history_even_if_status_is_stale() -> None:
    result = classify_event(
        event(event_id=44),
        CurrentTaskState(
            task_id="task-1",
            status="blocked",
            block_kind="capability",
            latest_event_kind="completed",
            latest_event_id=44,
        ),
    )

    assert result.classification is ClassificationKind.TERMINAL_SUCCESS
    assert result.reason == "current_state_terminal_event"
    assert not result.reserve


def test_adapter_error_mapping_accepts_enum_code() -> None:
    result = classify_stream_error(
        {"code": StreamErrorCode.TRANSPORT, "message": "socket closed"}
    )

    assert result.classification is ClassificationKind.TRANSPORT_ERROR
    assert result.reason == "transport_error:transport"
    assert not result.actionable


def test_board_context_mismatch_is_rejected_before_reservation() -> None:
    with pytest.raises(ClassifierInputError, match="board_slug"):
        classify_event(
            {
                "board_slug": "other",
                "event_id": 1,
                "task_id": "task-1",
                "run_id": 7,
                "kind": "blocked",
                "payload": {"kind": "capability"},
                "payload_state": "object",
                "created_at": NOW,
            },
            current(),
            board_slug="default",
        )


def test_malformed_hard_failure_is_fail_closed_even_when_currently_blocked() -> None:
    result = classify_event(
        event(kind="timed_out", payload=None, payload_state=PayloadState.NULL),
        current(),
    )

    assert result.classification is ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK
    assert not result.actionable
    assert not result.reserve


def test_negative_created_at_is_rejected_as_invalid_normalized_identity() -> None:
    with pytest.raises(ClassifierInputError, match="created_at"):
        classify_event(
            NormalizedEvent(
                board_slug="default",
                event_id=1,
                task_id="task-1",
                run_id=7,
                kind="blocked",
                payload={"kind": "capability"},
                payload_state=PayloadState.OBJECT,
                created_at=-1,
            ),
            current(),
        )