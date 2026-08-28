from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from hkrc.event_stream import (
    EventBatch,
    PayloadState,
    ReconnectPolicy,
    StreamAdapter,
    StreamCredentials,
    StreamError,
    StreamErrorCode,
    StreamRetentionError,
    StreamTransportError,
)


@dataclass
class FakeSocket:
    frames: list[str | bytes]
    closed: bool = False

    def recv(self) -> str | bytes:
        if not self.frames:
            raise StopIteration
        return self.frames.pop(0)

    def close(self) -> None:
        self.closed = True


class Connector:
    def __init__(self, socket: FakeSocket | None = None, error: Exception | None = None):
        self.socket = socket
        self.error = error
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers: dict[str, str]) -> FakeSocket:
        self.calls.append((url, headers))
        if self.error is not None:
            raise self.error
        assert self.socket is not None
        return self.socket


def frame(*, cursor: int, events: list[dict[str, Any]]) -> str:
    return json.dumps({"events": events, "cursor": cursor})


def event(
    event_id: int,
    *,
    task_id: str = "task-1",
    run_id: int | str | None = 7,
    kind: str = "blocked",
    payload: object = None,
    created_at: int = 100,
) -> dict[str, Any]:
    return {
        "id": event_id,
        "task_id": task_id,
        "run_id": run_id,
        "kind": kind,
        "payload": payload,
        "created_at": created_at,
    }


def connected_adapter(
    socket: FakeSocket,
    *,
    connector: Connector | None = None,
    policy: ReconnectPolicy | None = None,
) -> tuple[StreamAdapter, Connector]:
    actual_connector = connector or Connector(socket)
    adapter = StreamAdapter(
        "wss://dashboard.example.test/api/plugins/kanban/events",
        allowed_boards={"main"},
        connector=actual_connector,
        reconnect_policy=policy,
    )
    assert adapter.connect("main", 0, StreamCredentials(ticket="opaque-ticket")) is None
    return adapter, actual_connector


def test_connect_builds_authenticated_board_scoped_url_and_decodes_event_batch() -> None:
    socket = FakeSocket([
        # The server contract promises ascending event ids; the malformed payload
        # row remains in-order while its identity is preserved.
        frame(
            cursor=12,
            events=[
                event(4, payload=None),
                event(11, task_id="task-2", run_id=None, payload=["not", "an", "object"]),
                event(12, kind="future_kind", payload={"status": "blocked"}),
            ],
        )
    ])
    adapter, connector = connected_adapter(socket)

    batch = adapter.recv()

    assert isinstance(batch, EventBatch)
    assert batch.cursor == 12
    assert [item.id for item in batch.events] == [4, 11, 12]
    assert batch.events[0].payload_state is PayloadState.NULL
    assert batch.events[0].payload is None
    assert batch.events[1].payload_state is PayloadState.MALFORMED
    assert batch.events[1].task_id == "task-2"
    assert batch.events[1].run_id is None
    assert batch.events[2].kind == "future_kind"
    assert batch.events[2].payload_state is PayloadState.OBJECT
    assert adapter.cursor == 12

    url, headers = connector.calls[0]
    assert "since=0" in url
    assert "board=main" in url
    assert "ticket=opaque-ticket" in url
    assert headers == {}


def test_null_and_unknown_payload_rows_are_not_discarded_for_cursor_progress() -> None:
    socket = FakeSocket(
        [
            frame(
                cursor=8,
                events=[
                    event(8, kind="unrecognized", payload=None),
                ],
            )
        ]
    )
    adapter, _ = connected_adapter(socket)

    batch = adapter.recv()

    assert isinstance(batch, EventBatch)
    assert batch.events[0].kind == "unrecognized"
    assert batch.events[0].payload_state is PayloadState.NULL
    assert adapter.cursor == 8


def test_malformed_frame_fails_closed_without_advancing_cursor() -> None:
    socket = FakeSocket([json.dumps({"events": [{"id": "wrong"}], "cursor": 9})])
    adapter, _ = connected_adapter(socket)

    result = adapter.recv()

    assert result.code is StreamErrorCode.MALFORMED_FRAME
    assert result.retryable is False
    assert adapter.cursor == 0


def test_cursor_regression_is_reported_as_machine_readable_cursor_error() -> None:
    socket = FakeSocket([frame(cursor=2, events=[event(2)])])
    adapter, _ = connected_adapter(socket)
    first = adapter.recv()
    assert isinstance(first, EventBatch)
    socket.frames.append(frame(cursor=1, events=[]))

    result = adapter.recv()

    assert result.code is StreamErrorCode.CURSOR_INVALID
    assert result.retryable is False
    assert adapter.cursor == 2


def test_empty_frame_cannot_advance_cursor_without_an_event_identity() -> None:
    socket = FakeSocket([json.dumps({"events": [], "cursor": 9})])
    adapter, _ = connected_adapter(socket)

    result = adapter.recv()

    assert isinstance(result, StreamError)
    assert result.code is StreamErrorCode.CURSOR_INVALID
    assert adapter.cursor == 0


def test_board_cursor_and_credentials_are_validated_before_connector_call() -> None:
    connector = Connector(FakeSocket([]))
    adapter = StreamAdapter(
        "wss://dashboard.example.test/events",
        allowed_boards={"main"},
        connector=connector,
    )

    invalid_board = adapter.connect("other", 0, StreamCredentials(ticket="t"))
    invalid_cursor = adapter.connect("main", -1, StreamCredentials(ticket="t"))
    invalid_credentials = adapter.connect("main", 0, StreamCredentials())
    conflicting_credentials = adapter.connect(
        "main", 0, StreamCredentials(token="session", ticket="ticket")
    )

    assert invalid_board.code is StreamErrorCode.BOARD_INVALID
    assert invalid_cursor.code is StreamErrorCode.CURSOR_INVALID
    assert isinstance(invalid_credentials, StreamError)
    assert isinstance(conflicting_credentials, StreamError)
    assert invalid_credentials.code is StreamErrorCode.AUTH_FAILED
    assert conflicting_credentials.code is StreamErrorCode.AUTH_FAILED
    assert connector.calls == []


def test_auth_failure_is_non_retryable_and_transport_failures_have_bounded_backoff() -> None:
    auth_connector = Connector(FakeSocket([]), error=PermissionError("401"))
    auth_adapter = StreamAdapter(
        "wss://dashboard.example.test/events",
        allowed_boards={"main"},
        connector=auth_connector,
    )
    auth_result = auth_adapter.connect("main", 0, StreamCredentials(token="session"))
    assert auth_result.code is StreamErrorCode.AUTH_FAILED
    assert auth_result.retryable is False

    transport_connector = Connector(FakeSocket([]), error=StreamTransportError("down"))
    transport_adapter = StreamAdapter(
        "wss://dashboard.example.test/events",
        allowed_boards={"main"},
        connector=transport_connector,
        reconnect_policy=ReconnectPolicy(initial_delay=1.0, multiplier=2.0, max_delay=3.0, max_attempts=2),
    )
    first = transport_adapter.connect("main", 0, StreamCredentials(token="session"))
    second = transport_adapter.connect("main", 0, StreamCredentials(token="session"))
    third = transport_adapter.connect("main", 0, StreamCredentials(token="session"))

    assert (first.code, first.retryable, first.retry_after) == (StreamErrorCode.TRANSPORT, True, 1.0)
    assert (second.code, second.retryable, second.retry_after) == (StreamErrorCode.TRANSPORT, True, 2.0)
    assert (third.code, third.retryable, third.retry_after) == (StreamErrorCode.TRANSPORT, False, 3.0)


def test_disconnect_and_close_are_explicit_and_close_is_idempotent() -> None:
    socket = FakeSocket([frame(cursor=1, events=[event(1)])])
    adapter, _ = connected_adapter(socket)
    assert isinstance(adapter.recv(), EventBatch)
    socket.frames.append(frame(cursor=2, events=[event(2)]))
    socket.frames.insert(0, "")

    result = adapter.recv()
    assert result.code is StreamErrorCode.DISCONNECTED
    assert result.retryable is True
    assert socket.closed is True

    adapter.close()
    adapter.close()
    closed_result = adapter.recv()
    assert isinstance(closed_result, StreamError)
    assert closed_result.code is StreamErrorCode.DISCONNECTED
    assert closed_result.retryable is False


def test_explicit_retention_signal_is_machine_readable_and_non_retryable() -> None:
    connector = Connector(FakeSocket([]), error=StreamRetentionError("history expired"))
    adapter = StreamAdapter(
        "wss://dashboard.example.test/events",
        allowed_boards={"main"},
        connector=connector,
    )

    result = adapter.connect("main", 8, StreamCredentials(ticket="t"))

    assert result.code is StreamErrorCode.RETENTION_UNKNOWN
    assert result.retryable is False
    assert result.cursor == 8


def test_idle_timeout_returns_empty_batch_and_keeps_connection_open() -> None:
    class IdleSocket:
        closed = False

        def recv(self) -> str:
            raise TimeoutError("silent stream")

        def close(self) -> None:
            self.closed = True

    socket = IdleSocket()
    adapter, connector = connected_adapter(socket)  # type: ignore[arg-type]

    batch = adapter.recv()
    second_batch = adapter.recv()

    assert isinstance(batch, EventBatch)
    assert batch.events == ()
    assert batch.cursor == 0
    assert second_batch is not None
    assert adapter.connected is True
    assert adapter.cursor == 0
    assert socket.closed is False
    assert len(connector.calls) == 1
