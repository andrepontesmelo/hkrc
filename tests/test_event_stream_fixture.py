from __future__ import annotations

import pytest

from fixtures.event_stream import (
    EventStreamSimulator,
    StreamErrorCode,
    StreamEvent,
    StreamProtocolError,
    StreamScenario,
)


def event(event_id: int, kind: str = "blocked", payload: object | None = None) -> StreamEvent:
    return StreamEvent(event_id, kind, {"status": kind} if payload is None else payload)


def test_connect_delivers_retained_events_in_order_and_exposes_cursor() -> None:
    stream = EventStreamSimulator(StreamScenario.from_events([event(2), event(5), event(9)]))

    stream.connect()
    frames = list(stream.frames())

    assert [frame.event.event_id for frame in frames if frame.event is not None] == [2, 5, 9]


def test_duplicate_delivery_is_visible_to_consumer_for_idempotency() -> None:
    stream = EventStreamSimulator(
        StreamScenario.from_events([event(1), event(2)], duplicate_ids=frozenset({2}))
    )

    stream.connect()
    frames = list(stream.frames())

    assert [frame.event.event_id for frame in frames if frame.event is not None] == [1, 2, 2]


def test_disconnect_reconnect_resumes_after_last_delivered_cursor() -> None:
    stream = EventStreamSimulator(
        StreamScenario.from_events([event(1), event(2), event(3)], disconnect_after=2)
    )

    stream.connect()
    first, second = stream.read(), stream.read()
    assert first.event is not None and second.event is not None
    assert [first.event.event_id, second.event.event_id] == [1, 2]
    with pytest.raises(StreamProtocolError) as disconnected:
        stream.read()
    assert disconnected.value.code is StreamErrorCode.DISCONNECTED

    stream.connect(cursor=2)
    resumed = stream.read()
    assert resumed.event is not None
    assert resumed.event.event_id == 3


def test_disconnect_does_not_advance_consumer_cursor_for_unread_event() -> None:
    stream = EventStreamSimulator(
        StreamScenario.from_events([event(1), event(2)], disconnect_after=1)
    )

    stream.connect()
    first = stream.read()
    assert first.event is not None
    with pytest.raises(StreamProtocolError):
        stream.read()

    stream.connect(cursor=1)
    resumed = stream.read()
    assert resumed.event is not None
    assert resumed.event.event_id == 2


def test_stale_cursor_emits_retention_reset_and_cursor_zero_can_resume() -> None:
    scenario = StreamScenario.from_events([event(4), event(5), event(6)])
    scenario.retain_from(5)
    stream = EventStreamSimulator(scenario)

    with pytest.raises(StreamProtocolError) as reset:
        stream.connect(cursor=1)
    assert reset.value.code is StreamErrorCode.RETENTION_RESET
    assert reset.value.cursor == 1

    stream.disconnect()
    stream.connect(cursor=0)
    assert [frame.event.event_id for frame in stream.frames() if frame.event is not None] == [5, 6]


def test_malformed_payload_and_unknown_kind_are_explicit_non_event_frames() -> None:
    scenario = StreamScenario.from_events(
        [event(1, payload=["not", "object"]), event(2, "future_kind")],
        known_kinds=frozenset({"blocked"}),
    )
    stream = EventStreamSimulator(scenario)
    stream.connect()

    first, second = list(stream.frames())

    assert first.event is None
    assert first.error is not None
    assert first.error.code is StreamErrorCode.MALFORMED_PAYLOAD
    assert first.error.cursor == 1
    assert second.event is None
    assert second.error is not None
    assert second.error.code is StreamErrorCode.UNKNOWN_EVENT_KIND
    assert second.error.cursor == 2


def test_stream_errors_are_deterministic_and_connection_must_be_explicit() -> None:
    stream = EventStreamSimulator(StreamScenario.from_events([]))

    with pytest.raises(StreamProtocolError) as error:
        stream.read()

    assert error.value.code is StreamErrorCode.DISCONNECTED
    assert not stream.connected


def test_scenario_rejects_unordered_or_duplicate_event_ids() -> None:
    with pytest.raises(ValueError, match="ordered"):
        StreamScenario.from_events([event(2), event(1)])
    with pytest.raises(ValueError, match="unique"):
        StreamScenario.from_events([event(1), event(1)])
