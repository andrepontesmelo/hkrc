"""Deterministic in-process event-stream protocol simulator.

This fixture intentionally models transport semantics only.  It does not open
Hermes files, use the native Kanban database, or decide whether a blocker is
recoverable.  Tests can use it to drive a future observer through connect,
ordered delivery, reconnect/cursor resume, duplicate delivery, and explicit
protocol failures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Any, Iterable, Iterator, Mapping
from urllib.parse import parse_qs, urlsplit


class StreamErrorCode(str, Enum):
    """Stable simulator error signals an observer must classify explicitly."""

    DISCONNECTED = "disconnected"
    MALFORMED_PAYLOAD = "malformed_payload"
    RETENTION_RESET = "retention_reset"
    UNKNOWN_EVENT_KIND = "unknown_event_kind"


class StreamProtocolError(RuntimeError):
    """A deterministic stream failure with a machine-readable code."""

    def __init__(self, code: StreamErrorCode, message: str, *, cursor: int | None = None):
        super().__init__(message)
        self.code = code
        self.cursor = cursor


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """Wire-level event fixture.

    ``event_id`` is the resume cursor.  The simulator permits gaps because a
    cursor is an ordered position, not a promise that every integer exists.
    ``kind`` is intentionally a string so unknown kinds can be exercised.
    ``payload`` is a JSON-like object, or a raw value used to test validation.
    """

    event_id: int
    kind: str
    payload: object
    task_id: str = "task-1"
    run_id: int | str | None = None
    created_at: int = 0


@dataclass(frozen=True, slots=True)
class StreamEnvelope:
    """One transport frame delivered to a subscriber."""

    event: StreamEvent | None = None
    error: StreamProtocolError | None = None


@dataclass(slots=True)
class StreamScenario:
    """Mutable deterministic scenario consumed by ``EventStreamSimulator``.

    ``events`` is the retained ordered history.  ``disconnect_after`` is a
    delivery count per connection; after that many frames the next read emits
    ``DISCONNECTED``.  ``duplicate_ids`` causes selected events to be delivered
    twice, including on resume, which lets a consumer prove idempotency.
    """

    events: list[StreamEvent] = field(default_factory=list)
    known_kinds: frozenset[str] = frozenset({"blocked", "unblocked", "heartbeat"})
    disconnect_after: int | None = None
    duplicate_ids: frozenset[int] = frozenset()
    malformed_ids: frozenset[int] = frozenset()
    retention_floor: int | None = None
    reset_on_connect: bool = False

    @classmethod
    def from_events(
        cls,
        events: Iterable[StreamEvent],
        **kwargs: Any,
    ) -> "StreamScenario":
        ordered = list(events)
        if [event.event_id for event in ordered] != sorted(event.event_id for event in ordered):
            raise ValueError("scenario events must be ordered by event_id")
        if len({event.event_id for event in ordered}) != len(ordered):
            raise ValueError("scenario event ids must be unique")
        return cls(events=ordered, **kwargs)

    def append(self, event: StreamEvent) -> None:
        if self.events and event.event_id <= self.events[-1].event_id:
            raise ValueError("appended event id must be greater than retained history")
        self.events.append(event)

    def retain_from(self, event_id: int) -> None:
        """Drop history below ``event_id`` and emit a retention floor signal."""

        self.events = [event for event in self.events if event.event_id >= event_id]
        self.retention_floor = event_id


@dataclass
class WebSocketScenario:
    """Scripted wire frames for exercising the production adapter offline.

    Each item in ``connections`` is the complete sequence returned by one
    connector call. Keeping connection scripts explicit makes reconnect tests
    deterministic and avoids pretending that this fixture is a WebSocket
    implementation. ``connect_errors`` injects connector-side failures before
    a socket is returned.  ``idle`` models a persistent socket: when the
    scripted frames are exhausted the socket raises ``TimeoutError`` (idle at
    a frame boundary) instead of ``StopIteration``, so the adapter keeps the
    connection open and returns an empty batch — the persistent-WS behavior
    the watcher relies on across passes.
    """

    connections: list[list[str | bytes]] = field(default_factory=list)
    connect_errors: list[BaseException | None] = field(default_factory=list)
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    idle: bool = False

    @classmethod
    def from_batches(
        cls,
        batches: Iterable[Iterable[StreamEvent]],
        *,
        connect_errors: Iterable[BaseException | None] = (),
    ) -> "WebSocketScenario":
        """Build exact ``{"events", "cursor"}`` frames from event rows."""

        connections: list[list[str | bytes]] = []
        for events in batches:
            rows = tuple(events)
            if not rows:
                raise ValueError("wire batches must contain at least one event")
            ids = [event.event_id for event in rows]
            if ids != sorted(ids) or len(ids) != len(set(ids)):
                raise ValueError("wire batch event ids must be strictly increasing")
            connections.append([wire_batch(rows, cursor=ids[-1])])
        return cls(connections, list(connect_errors))

    def connector(self, url: str, headers: Mapping[str, str]) -> "ScriptedWebSocket":
        """Return the next scripted socket and record URL/header boundaries."""

        self.calls.append((url, dict(headers)))
        if self.connect_errors:
            error = self.connect_errors.pop(0)
            if error is not None:
                raise error
        if not self.connections:
            raise ConnectionError("no scripted WebSocket connection remains")
        return ScriptedWebSocket(self.connections.pop(0), idle=self.idle)

    @staticmethod
    def query(url: str) -> dict[str, str]:
        """Decode the adapter query contract without exposing credentials."""

        values = parse_qs(urlsplit(url).query, keep_blank_values=True)
        return {
            key: value[0]
            for key, value in values.items()
            if key in {"board", "since"}
        }


@dataclass
class ScriptedWebSocket:
    """Minimal socket used by ``StreamAdapter`` acceptance tests.

    ``idle`` models a persistent connection: once the scripted frames are
    exhausted the socket raises ``TimeoutError`` (idle at a frame boundary)
    instead of ``StopIteration``, so the adapter keeps the connection open
    and reports an empty batch — exactly what the watcher's persistent-WS
    passes rely on.
    """

    frames: list[str | bytes]
    closed: bool = False
    idle: bool = False

    def recv(self) -> str | bytes:
        if not self.frames:
            if self.idle:
                raise TimeoutError("idle at frame boundary")
            raise StopIteration
        return self.frames.pop(0)

    def close(self) -> None:
        self.closed = True


def wire_batch(events: Iterable[StreamEvent], *, cursor: int) -> str:
    """Serialize one exact dashboard batch/envelope frame.

    The helper uses the production adapter's field set and keeps null and
    non-object payloads intact for identity-preservation tests.
    """

    rows = tuple(events)
    return json.dumps(
        {
            "events": [
                {
                    "id": event.event_id,
                    "task_id": event.task_id,
                    "run_id": event.run_id,
                    "kind": event.kind,
                    "payload": event.payload,
                    "created_at": event.created_at,
                }
                for event in rows
            ],
            "cursor": cursor,
        },
        separators=(",", ":"),
    )


@dataclass(slots=True)
class _Connection:
    cursor: int
    delivered: int = 0
    pending: list[StreamEnvelope] = field(default_factory=list)


class EventStreamSimulator:
    """Small in-process source with explicit connection and cursor semantics."""

    def __init__(self, scenario: StreamScenario):
        self.scenario = scenario
        self._connection: _Connection | None = None
        self._reset_signal_emitted = False

    @property
    def connected(self) -> bool:
        return self._connection is not None

    def connect(self, *, cursor: int = 0) -> None:
        if cursor < 0:
            raise ValueError("cursor must not be negative")
        if self.connected:
            raise StreamProtocolError(StreamErrorCode.DISCONNECTED, "already connected")
        floor = self.scenario.retention_floor
        if self.scenario.reset_on_connect and not self._reset_signal_emitted:
            self._reset_signal_emitted = True
            self._connection = _Connection(cursor=0)
            raise StreamProtocolError(
                StreamErrorCode.RETENTION_RESET,
                "server requires cursor reset",
                cursor=cursor,
            )
        # Cursor zero is the explicit consumer-selected reset position.  Any
        # nonzero cursor older than the retained floor is stale and must be
        # surfaced rather than silently skipping history.
        if floor is not None and cursor != 0 and cursor < floor - 1:
            self._connection = _Connection(cursor=0)
            raise StreamProtocolError(
                StreamErrorCode.RETENTION_RESET,
                f"cursor {cursor} is older than retention floor {floor}",
                cursor=cursor,
            )
        self._connection = _Connection(cursor=cursor)

    def disconnect(self) -> None:
        if self._connection is not None:
            self._connection = None

    def read(self) -> StreamEnvelope:
        """Return one frame, or raise a transport error for stream failures."""

        connection = self._require_connection()
        if connection.pending:
            return connection.pending.pop(0)
        if (
            self.scenario.disconnect_after is not None
            and connection.delivered >= self.scenario.disconnect_after
        ):
            self.disconnect()
            raise StreamProtocolError(StreamErrorCode.DISCONNECTED, "simulated disconnect")
        event = next((item for item in self.scenario.events if item.event_id > connection.cursor), None)
        if event is None:
            raise StopIteration
        connection.cursor = event.event_id
        connection.delivered += 1
        envelope = self._envelope(event)
        if event.event_id in self.scenario.duplicate_ids:
            connection.pending.append(envelope)
        return envelope

    def frames(self) -> Iterator[StreamEnvelope]:
        """Read until retained history is exhausted; transport errors propagate."""

        while True:
            try:
                yield self.read()
            except StopIteration:
                return

    def _require_connection(self) -> _Connection:
        if self._connection is None:
            raise StreamProtocolError(StreamErrorCode.DISCONNECTED, "not connected")
        return self._connection

    def _envelope(self, event: StreamEvent) -> StreamEnvelope:
        if event.event_id in self.scenario.malformed_ids:
            return StreamEnvelope(
                error=StreamProtocolError(
                    StreamErrorCode.MALFORMED_PAYLOAD,
                    f"malformed payload for event {event.event_id}",
                    cursor=event.event_id,
                )
            )
        if event.kind not in self.scenario.known_kinds:
            return StreamEnvelope(
                error=StreamProtocolError(
                    StreamErrorCode.UNKNOWN_EVENT_KIND,
                    f"unknown event kind: {event.kind}",
                    cursor=event.event_id,
                )
            )
        if not isinstance(event.payload, Mapping):
            return StreamEnvelope(
                error=StreamProtocolError(
                    StreamErrorCode.MALFORMED_PAYLOAD,
                    f"event {event.event_id} payload is not an object",
                    cursor=event.event_id,
                )
            )
        return StreamEnvelope(event=event)


__all__ = [
    "EventStreamSimulator",
    "ScriptedWebSocket",
    "StreamEnvelope",
    "StreamErrorCode",
    "StreamEvent",
    "StreamProtocolError",
    "StreamScenario",
    "WebSocketScenario",
    "wire_batch",
]
