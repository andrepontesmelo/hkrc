"""Authenticated, read-only adapter for the Hermes Kanban event WebSocket.

The adapter is deliberately transport-only.  It does not inspect Hermes paths,
open a native database, parse human-oriented CLI output, classify events, or
persist a cursor.  A caller owns durable cursor persistence and supplies a
connector (usually an injected WebSocket client) so this package has no runtime
WebSocket dependency.

The supported wire frame is the dashboard Kanban envelope::

    {"events": [{"id", "task_id", "run_id", "kind", "payload", "created_at"}],
     "cursor": <last event id>}

Authentication is operator-owned input.  The adapter only places a supplied
session ``token`` or one-use ``ticket`` in the connection URL; it never reads
credentials from files or environment variables.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field as dataclass_field
from enum import Enum
import json
from math import isfinite
from typing import Protocol, TypeAlias
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class StreamErrorCode(str, Enum):
    """Stable failure categories exposed to the runtime and classifiers."""

    AUTH_FAILED = "auth_failed"
    DISCONNECTED = "disconnected"
    TRANSPORT = "transport"
    MALFORMED_FRAME = "malformed_frame"
    CURSOR_INVALID = "cursor_invalid"
    BOARD_INVALID = "board_invalid"
    RETENTION_UNKNOWN = "retention_unknown"


class PayloadState(str, Enum):
    """State of an event payload after wire normalization.

    ``NULL`` is a valid payload state.  ``MALFORMED`` preserves the event's
    identity even when a producer emits a non-object JSON value; downstream
    classification must decide how to handle it rather than losing its cursor.
    """

    NULL = "null"
    OBJECT = "object"
    MALFORMED = "malformed"


@dataclass(frozen=True, slots=True)
class StreamCredentials:
    """Operator-supplied WebSocket authentication/session input.

    Exactly one non-empty credential is required for a connection.  ``repr=False``
    prevents a session secret from appearing in logs or test failure output.
    The server accepts either a loopback session ``token`` or a gated one-use
    ``ticket``; the adapter does not mint, refresh, or discover either value.
    The passive container lets ``connect`` return a machine-readable auth error
    for missing or conflicting input instead of raising during construction.
    """

    token: str | None = dataclass_field(repr=False, default=None)
    ticket: str | None = dataclass_field(repr=False, default=None)

    @property
    def query_name(self) -> str:
        return "ticket" if isinstance(self.ticket, str) and self.ticket else "token"

    @property
    def query_value(self) -> str:
        value = self.ticket if isinstance(self.ticket, str) and self.ticket else self.token
        assert value is not None
        return value


@dataclass(frozen=True, slots=True)
class StreamEvent:
    """Normalized event identity and payload state from one wire row."""

    id: int
    task_id: str
    run_id: int | str | None
    kind: str
    payload: object | None
    payload_state: PayloadState
    created_at: int

    @property
    def event_id(self) -> int:
        """Compatibility alias for controller code using native event naming."""

        return self.id


@dataclass(frozen=True, slots=True)
class EventBatch:
    """One complete accepted ``{events, cursor}`` WebSocket frame."""

    events: tuple[StreamEvent, ...]
    cursor: int


@dataclass(frozen=True, slots=True)
class StreamError:
    """Machine-readable adapter outcome; no automatic retry is performed."""

    code: StreamErrorCode
    message: str
    retryable: bool = False
    retry_after: float | None = None
    cursor: int | None = None


class StreamAuthError(RuntimeError):
    """Connector signal that supplied authentication was rejected."""


class StreamRetentionError(RuntimeError):
    """Connector signal that the requested cursor is no longer retained."""


class StreamTransportError(RuntimeError):
    """Connector or socket signal for a transport-level failure."""


class StreamSocket(Protocol):
    """Minimal socket seam required by the adapter."""

    def recv(self) -> str | bytes: ...

    def close(self) -> None: ...


Connector: TypeAlias = Callable[[str, Mapping[str, str]], StreamSocket]


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Bounded exponential-backoff values signalled on retryable failures.

    The adapter reports ``retry_after``; it never sleeps or reconnects itself.
    ``max_attempts`` counts retryable failures before the next failure becomes
    terminal.  Delays are always capped by ``max_delay``.
    """

    initial_delay: float = 0.5
    multiplier: float = 2.0
    max_delay: float = 30.0
    max_attempts: int = 5

    def __post_init__(self) -> None:
        if not isfinite(self.initial_delay) or self.initial_delay < 0:
            raise ValueError("initial_delay must be finite and nonnegative")
        if not isfinite(self.multiplier) or self.multiplier < 1:
            raise ValueError("multiplier must be finite and at least one")
        if not isfinite(self.max_delay) or self.max_delay < 0:
            raise ValueError("max_delay must be finite and nonnegative")
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int) or self.max_attempts < 0:
            raise ValueError("max_attempts must be a nonnegative integer")

    def delay_for(self, failure_number: int) -> float:
        if failure_number < 1:
            raise ValueError("failure_number must be positive")
        return min(self.max_delay, self.initial_delay * self.multiplier ** (failure_number - 1))


class StreamAdapter:
    """Typed lifecycle adapter around the authenticated dashboard WebSocket."""

    def __init__(
        self,
        endpoint: str,
        *,
        allowed_boards: Iterable[str],
        connector: Connector,
        reconnect_policy: ReconnectPolicy | None = None,
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError("endpoint must be a non-empty URL")
        boards = frozenset(allowed_boards)
        if not boards or any(not isinstance(board, str) or not board for board in boards):
            raise ValueError("allowed_boards must contain at least one non-empty board slug")
        if not callable(connector):
            raise ValueError("connector must be callable")
        self.endpoint = endpoint
        self.allowed_boards = boards
        self._connector = connector
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._socket: StreamSocket | None = None
        self._board: str | None = None
        self._cursor = 0
        self._failure_number = 0
        self._closed = False

    @property
    def connected(self) -> bool:
        return self._socket is not None and not self._closed

    @property
    def board(self) -> str | None:
        return self._board

    @property
    def cursor(self) -> int:
        return self._cursor

    def connect(
        self,
        board: str,
        since: int,
        credentials: StreamCredentials,
    ) -> StreamError | None:
        """Connect a board-scoped stream, returning an error instead of raising.

        Validation happens before invoking the injected connector.  A successful
        connection adopts ``since`` as the adapter's last safely accepted
        cursor.  Reconnect scheduling remains the caller's responsibility.
        """

        validation = self._validate_connect_inputs(board, since, credentials)
        if validation is not None:
            return validation
        if self.connected:
            return StreamError(
                StreamErrorCode.DISCONNECTED,
                "stream is already connected; close before reconnecting",
                retryable=False,
                cursor=self._cursor,
            )

        self._closed = False
        url = _connection_url(self.endpoint, board, since, credentials)
        try:
            socket = self._connector(url, {})
            if socket is None or not callable(getattr(socket, "recv", None)):
                raise StreamTransportError("connector returned an invalid socket")
        except StreamAuthError as exc:
            return self._error(StreamErrorCode.AUTH_FAILED, "stream authentication failed", exc)
        except StreamRetentionError:
            return StreamError(
                StreamErrorCode.RETENTION_UNKNOWN,
                "requested stream history is no longer retained",
                retryable=False,
                cursor=since,
            )
        except PermissionError as exc:
            return self._error(StreamErrorCode.AUTH_FAILED, "stream authentication failed", exc)
        except Exception as exc:
            return self._failure(StreamErrorCode.TRANSPORT, "stream connection failed", exc)

        self._socket = socket
        self._board = board
        self._cursor = since
        self._failure_number = 0
        return None

    def recv(self) -> EventBatch | StreamError:
        """Receive and validate one complete frame from the connected stream.

        A ``TimeoutError`` from the socket means no frame arrived within the
        receive timeout and provably no frame bytes were consumed (the
        connector's frame reader only surfaces an idle timeout at a frame
        boundary).  The board is idle, not failing: the connection stays open
        and an empty batch with the unchanged cursor is returned so the caller
        can keep polling in place without reconnecting.
        """

        if self._socket is None:
            return StreamError(
                StreamErrorCode.DISCONNECTED,
                "stream is not connected",
                retryable=not self._closed,
                cursor=self._cursor,
            )
        try:
            raw = self._socket.recv()
        except TimeoutError:
            # Idle board: no data within the receive timeout and nothing was
            # consumed, so the connection stays frame-aligned and open.  This
            # is not a failure: treating idle as a failure is what forced the
            # old per-cycle connect/recv/close churn.  The failure counter is
            # intentionally not reset so flapping boards keep their backoff.
            return EventBatch((), self._cursor)
        except (StopIteration, EOFError, ConnectionError) as exc:
            self._drop_socket()
            return self._failure(StreamErrorCode.DISCONNECTED, "stream disconnected", exc)
        except StreamAuthError as exc:
            self._drop_socket()
            return self._error(StreamErrorCode.AUTH_FAILED, "stream authentication failed", exc)
        except StreamRetentionError:
            self._drop_socket()
            return StreamError(
                StreamErrorCode.RETENTION_UNKNOWN,
                "requested stream history is no longer retained",
                retryable=False,
                cursor=self._cursor,
            )
        except Exception as exc:
            self._drop_socket()
            return self._failure(StreamErrorCode.TRANSPORT, "stream receive failed", exc)

        if raw == "" or raw == b"":
            self._drop_socket()
            return self._failure(StreamErrorCode.DISCONNECTED, "stream disconnected", None)
        try:
            batch = _decode_frame(raw, previous_cursor=self._cursor)
        except _FrameError as exc:
            self._drop_socket()
            return StreamError(
                exc.code,
                exc.message,
                retryable=False,
                cursor=self._cursor,
            )
        self._cursor = batch.cursor
        self._failure_number = 0
        return batch

    def close(self) -> None:
        """Close the current socket; repeated calls are safe and do not retry."""

        socket, self._socket = self._socket, None
        self._closed = True
        if socket is not None:
            try:
                socket.close()
            except Exception:
                # Closing is best effort; the adapter is disconnected regardless
                # and must not turn cleanup into an unbounded retry loop.
                pass

    def _validate_connect_inputs(
        self,
        board: str,
        since: int,
        credentials: StreamCredentials,
    ) -> StreamError | None:
        if not isinstance(board, str) or not board or board not in self.allowed_boards:
            return StreamError(StreamErrorCode.BOARD_INVALID, "board is not in the adapter allowlist")
        if not _is_nonnegative_int(since):
            return StreamError(StreamErrorCode.CURSOR_INVALID, "cursor must be a nonnegative integer")
        if not isinstance(credentials, StreamCredentials):
            return StreamError(StreamErrorCode.AUTH_FAILED, "credentials are invalid")
        if any(
            value is not None and not isinstance(value, str)
            for value in (credentials.token, credentials.ticket)
        ):
            return StreamError(StreamErrorCode.AUTH_FAILED, "credentials are invalid")
        supplied = [
            value
            for value in (credentials.token, credentials.ticket)
            if isinstance(value, str) and value
        ]
        if len(supplied) != 1:
            return StreamError(StreamErrorCode.AUTH_FAILED, "credentials are invalid")
        return None

    def _drop_socket(self) -> None:
        socket, self._socket = self._socket, None
        if socket is not None:
            try:
                socket.close()
            except Exception:
                pass

    def _failure(
        self,
        code: StreamErrorCode,
        message: str,
        exc: BaseException | None,
    ) -> StreamError:
        self._failure_number += 1
        retry_after = self.reconnect_policy.delay_for(self._failure_number)
        retryable = self._failure_number <= self.reconnect_policy.max_attempts
        return StreamError(
            code,
            message,
            retryable=retryable,
            retry_after=retry_after,
            cursor=self._cursor,
        )

    def _error(
        self,
        code: StreamErrorCode,
        message: str,
        exc: BaseException | None,
    ) -> StreamError:
        # Keep the exception out of the public message.  Connector exceptions
        # can contain URLs, query strings, or other operator-owned credentials.
        return StreamError(code, message, retryable=False, cursor=self._cursor)


@dataclass(frozen=True, slots=True)
class _FrameError(ValueError):
    code: StreamErrorCode
    message: str


def _connection_url(
    endpoint: str,
    board: str,
    since: int,
    credentials: StreamCredentials,
) -> str:
    split = urlsplit(endpoint)
    query = parse_qsl(split.query, keep_blank_values=True)
    query.extend((("since", str(since)), ("board", board), (credentials.query_name, credentials.query_value)))
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def _decode_frame(raw: str | bytes, *, previous_cursor: int) -> EventBatch:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "frame is not valid UTF-8") from exc
    if not isinstance(raw, str):
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "frame must be text or UTF-8 bytes")
    try:
        value = json.loads(raw, parse_constant=_reject_json_constant)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "frame is not valid JSON") from exc
    if not isinstance(value, dict) or set(value) != {"events", "cursor"}:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "frame must contain exactly events and cursor")
    cursor = value["cursor"]
    if not _is_nonnegative_int(cursor) or cursor < previous_cursor:
        raise _FrameError(StreamErrorCode.CURSOR_INVALID, "frame cursor regressed or is invalid")
    events_value = value["events"]
    if not isinstance(events_value, list) or len(events_value) > 200:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "events must be a list of at most 200 rows")

    events: list[StreamEvent] = []
    prior_id = previous_cursor
    for raw_event in events_value:
        event = _decode_event(raw_event)
        if event.id <= prior_id:
            raise _FrameError(StreamErrorCode.CURSOR_INVALID, "event ids must increase after the cursor")
        events.append(event)
        prior_id = event.id
    if not events and cursor != previous_cursor:
        raise _FrameError(StreamErrorCode.CURSOR_INVALID, "empty frame cursor must not advance")
    if events and events[-1].id != cursor:
        raise _FrameError(StreamErrorCode.CURSOR_INVALID, "frame cursor must equal the final event id")
    return EventBatch(tuple(events), cursor)


def _decode_event(raw_event: object) -> StreamEvent:
    if not isinstance(raw_event, dict) or set(raw_event) != {
        "id",
        "task_id",
        "run_id",
        "kind",
        "payload",
        "created_at",
    }:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event identity envelope is malformed")
    event_id = raw_event["id"]
    task_id = raw_event["task_id"]
    run_id = raw_event["run_id"]
    kind = raw_event["kind"]
    created_at = raw_event["created_at"]
    if not _is_positive_int(event_id):
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event id must be a positive integer")
    if not isinstance(task_id, str) or not task_id:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event task_id must be a non-empty string")
    if run_id is not None and not (
        (isinstance(run_id, int) and not isinstance(run_id, bool))
        or (isinstance(run_id, str) and bool(run_id))
    ):
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event run_id has an invalid type")
    if not isinstance(kind, str) or not kind:
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event kind must be a non-empty string")
    if not _is_nonnegative_int(created_at):
        raise _FrameError(StreamErrorCode.MALFORMED_FRAME, "event created_at must be a nonnegative integer")

    payload = raw_event["payload"]
    if payload is None:
        payload_state = PayloadState.NULL
    elif isinstance(payload, dict):
        payload_state = PayloadState.OBJECT
    else:
        payload_state = PayloadState.MALFORMED
    return StreamEvent(
        id=event_id,
        task_id=task_id,
        run_id=run_id,
        kind=kind,
        payload=payload,
        payload_state=payload_state,
        created_at=created_at,
    )


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-standard JSON constant: {value}")


def _is_nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_positive_int(value: object) -> bool:
    return _is_nonnegative_int(value) and bool(value)


__all__ = [
    "Connector",
    "EventBatch",
    "PayloadState",
    "ReconnectPolicy",
    "StreamAdapter",
    "StreamAuthError",
    "StreamCredentials",
    "StreamError",
    "StreamErrorCode",
    "StreamEvent",
    "StreamRetentionError",
    "StreamSocket",
    "StreamTransportError",
]
