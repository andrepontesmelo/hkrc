"""Approved live Hermes dashboard integration seams.

This module is the only production wiring between the stream-only controller
runtime and a live Hermes installation.  The WebSocket connector consumes the
URL already authenticated and scoped by :class:`hkrc.event_stream.StreamAdapter`.
The current-state reader invokes the official Hermes CLI's machine-readable
``kanban show --json`` surface and never imports Hermes internals or opens a
native SQLite database.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import base64
import hashlib
import json
import os
import secrets
import socket
import ssl
import subprocess
import struct
from typing import Any
from urllib.parse import urlsplit

from .classifier import CurrentTaskState
from .config import ControllerConfig
from .discovery import discover_boards
from .event_stream import (
    StreamAdapter,
    StreamAuthError,
    StreamCredentials,
)
from .handoff import HandoffError


SUPPORTED_CURRENT_STATE_READERS = frozenset(
    {"approved-dashboard-snapshot", "hermes-kanban-cli"}
)


class CurrentStateReaderError(RuntimeError):
    """Raised when the approved Hermes CLI snapshot cannot be trusted."""


class WebSocketProtocolError(OSError):
    """Raised when the dashboard WebSocket violates RFC 6455."""


class _WebSocketSocket:
    """Small dependency-free RFC 6455 client for the dashboard stream.

    The controller's release is intentionally stdlib-only.  This client only
    needs the server-to-client text/binary data path plus ping/pong and close
    control frames; client frames are masked as required by RFC 6455.
    """

    def __init__(self, raw_socket: socket.socket) -> None:
        self._socket = raw_socket
        self._closed = False

    @classmethod
    def connect(
        cls,
        url: str,
        headers: Mapping[str, str],
        *,
        open_timeout: float,
        recv_timeout: float = 20.0,
    ) -> "_WebSocketSocket":
        split = urlsplit(url)
        if split.scheme not in {"ws", "wss"} or not split.hostname:
            raise ValueError("WebSocket URL must be an absolute ws:// or wss:// URL")
        port = split.port or (443 if split.scheme == "wss" else 80)
        host_header = split.hostname
        if (split.scheme == "ws" and port != 80) or (split.scheme == "wss" and port != 443):
            host_header = f"{host_header}:{port}"
        path = split.path or "/"
        if split.query:
            path += "?" + split.query
        raw_socket = socket.create_connection((split.hostname, port), timeout=open_timeout)
        try:
            if split.scheme == "wss":
                context = ssl.create_default_context()
                raw_socket = context.wrap_socket(raw_socket, server_hostname=split.hostname)
            key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
            request_headers = {
                "Host": host_header,
                "Upgrade": "websocket",
                "Connection": "Upgrade",
                "Sec-WebSocket-Key": key,
                "Sec-WebSocket-Version": "13",
                **dict(headers),
            }
            request = "GET " + path + " HTTP/1.1\r\n" + "\r\n".join(
                f"{name}: {value}" for name, value in request_headers.items()
            ) + "\r\n\r\n"
            raw_socket.sendall(request.encode("ascii"))
            response = _read_http_headers(raw_socket)
            status_line, response_headers = _parse_http_headers(response)
            status_code = int(status_line.split(" ", 2)[1])
            if status_code != 101:
                if status_code in (401, 403):
                    raise StreamAuthError("WebSocket authentication failed")
                raise OSError(f"WebSocket upgrade failed with HTTP {status_code}")
            expected = base64.b64encode(
                hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
            ).decode("ascii")
            if response_headers.get("sec-websocket-accept", "") != expected:
                raise OSError("WebSocket upgrade returned an invalid accept key")
            raw_socket.settimeout(recv_timeout)
            return cls(raw_socket)
        except Exception:
            raw_socket.close()
            raise

    def recv(self) -> str | bytes:
        fragments: list[bytes] = []
        message_opcode: int | None = None
        while True:
            try:
                fin, opcode, payload = self._recv_frame()
            except socket.timeout as exc:
                if not fragments and message_opcode is None:
                    # Idle at a frame boundary: the first header byte read
                    # provably consumed nothing, so the connection stays
                    # frame-aligned and the adapter keeps it open.
                    raise
                self._socket.close()
                raise ConnectionError("WebSocket frame timed out mid-message") from exc
            if opcode == 0x8:
                self.close()
                raise EOFError("WebSocket peer closed")
            if opcode == 0x9:  # ping
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode in (0x1, 0x2):
                if message_opcode is not None:
                    raise OSError("WebSocket received a new message before continuation")
                message_opcode = opcode
            elif opcode != 0x0 or message_opcode is None:
                raise OSError("WebSocket continuation frame is invalid")
            fragments.append(payload)
            if not fin:
                continue
            body = b"".join(fragments)
            if message_opcode == 0x1:
                return body.decode("utf-8")
            return body

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        finally:
            self._socket.close()

    def _recv_frame(self) -> tuple[bool, int, bytes]:
        # The first header byte is read on its own so an idle timeout provably
        # consumes zero bytes and the connection stays frame-aligned.  A
        # timeout anywhere after the first byte is a mid-frame failure:
        # partial header or payload bytes have been consumed and the stream
        # can no longer be trusted, so the socket is dropped as a transport
        # failure instead of being misread as idle.
        try:
            first = _recv_exact(self._socket, 1)[0]
        except socket.timeout:
            raise
        try:
            second = _recv_exact(self._socket, 1)[0]
            fin = bool(first & 0x80)
            opcode = first & 0x0F
            masked = bool(second & 0x80)
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", _recv_exact(self._socket, 2))[0]
            elif length == 127:
                length = struct.unpack("!Q", _recv_exact(self._socket, 8))[0]
            if opcode >= 0x8 and (not fin or length > 125):
                raise WebSocketProtocolError("WebSocket control frame is invalid")
            if opcode not in {0x0, 0x1, 0x2, 0x8, 0x9, 0xA}:
                raise WebSocketProtocolError("WebSocket opcode is invalid")
            if length > 16 * 1024 * 1024:
                raise OSError("WebSocket frame exceeds the 16 MiB safety limit")
            mask = _recv_exact(self._socket, 4) if masked else None
            payload = _recv_exact(self._socket, length)
        except socket.timeout as exc:
            self._socket.close()
            raise ConnectionError("WebSocket frame timed out mid-frame") from exc
        if mask is not None:
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        return fin, opcode, payload

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        length = len(payload)
        if length < 126:
            header = struct.pack("!BB", 0x80 | opcode, 0x80 | length)
        elif length <= 0xFFFF:
            header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, length)
        else:
            header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, length)
        mask = secrets.token_bytes(4)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self._socket.sendall(header + mask + masked)


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("WebSocket peer closed during frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_http_headers(sock: socket.socket) -> bytes:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        # Read one byte at a time so a server that writes the first data frame
        # immediately after the 101 response cannot have that frame consumed
        # and discarded with the handshake headers.
        chunk = sock.recv(1)
        if not chunk:
            raise EOFError("WebSocket peer closed during handshake")
        data.extend(chunk)
        if len(data) > 64 * 1024:
            raise OSError("WebSocket handshake headers are too large")
    return bytes(data)


def _parse_http_headers(response: bytes) -> tuple[str, dict[str, str]]:
    head = response.split(b"\r\n\r\n", 1)[0].decode("iso-8859-1")
    lines = head.split("\r\n")
    if not lines or not lines[0].startswith("HTTP/"):
        raise OSError("WebSocket handshake response was malformed")
    parsed: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            parsed[name.strip().lower()] = value.strip()
    return lines[0], parsed


class WebSocketConnector:
    """Create authenticated synchronous WebSocket connections."""

    def __init__(
        self,
        *,
        open_timeout: float = 15.0,
        recv_timeout: float = 20.0,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        if open_timeout <= 0:
            raise ValueError("open_timeout must be positive")
        if recv_timeout <= 0:
            raise ValueError("recv_timeout must be positive")
        self.open_timeout = float(open_timeout)
        self.recv_timeout = float(recv_timeout)
        self._connect = connect

    def __call__(self, url: str, headers: Mapping[str, str]) -> _WebSocketSocket:
        connect = self._connect or _WebSocketSocket.connect
        try:
            if self._connect is None:
                return _WebSocketSocket.connect(
                    url,
                    headers,
                    open_timeout=self.open_timeout,
                    recv_timeout=self.recv_timeout,
                )
            connection = connect(url, headers)
        except Exception as exc:  # noqa: BLE001 - map vendor transport errors
            if _is_authentication_failure(exc):
                raise StreamAuthError("WebSocket authentication failed") from exc
            raise
        if connection is None:
            raise RuntimeError("WebSocket connector returned no connection")
        # Injected connectors in tests may already implement the stream socket
        # protocol; production always takes the stdlib path above.
        return connection


def _is_authentication_failure(exc: BaseException) -> bool:
    """Recognize HTTP auth rejection without exposing response details."""

    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code in (401, 403):
        return True
    # Older websockets releases expose a status code directly on the exception.
    return getattr(exc, "status_code", None) in (401, 403)


@dataclass(frozen=True, slots=True)
class HermesCliCurrentStateReader:
    """Read and validate one task snapshot through the Hermes CLI."""

    cli: str = "hermes"
    profile: str | None = None
    timeout: float = 30.0
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None

    def __post_init__(self) -> None:
        if not self.cli.strip():
            raise ValueError("cli must not be empty")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

    @property
    def _runner(self) -> Callable[..., subprocess.CompletedProcess[str]]:
        return self.runner or subprocess.run

    def __call__(self, board_slug: str, task_id: str) -> CurrentTaskState:
        if not board_slug or not task_id:
            raise CurrentStateReaderError("board and task identifiers are required")
        command = [self.cli]
        if self.profile:
            command.extend(["--profile", self.profile])
        command.extend(["kanban", "--board", board_slug, "show", task_id, "--json"])
        try:
            completed = self._runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise CurrentStateReaderError("Hermes CLI current-state read timed out") from exc
        except OSError as exc:
            raise CurrentStateReaderError("Hermes CLI current-state read failed") from exc
        if completed.returncode != 0:
            raise CurrentStateReaderError(
                f"Hermes CLI current-state read exited with {completed.returncode}"
            )
        try:
            payload = json.loads(completed.stdout)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CurrentStateReaderError("Hermes CLI current-state output was not valid JSON") from exc
        return _current_state_from_payload(payload, task_id)


@dataclass(frozen=True, slots=True)
class HermesCliBlockedLister:
    """List every ``status='blocked'`` task id on a board through the CLI.

    Powers the daemon's periodic blocked-state reconcile sweep (runtime.py):
    a board-level ``list --status blocked --json`` is the state-based backstop
    for death events the stream never delivered.  Fail-closed like the
    reader: malformed output, a nonzero exit, or a timeout raises
    ``CurrentStateReaderError`` so the daemon logs the sweep failure instead
    of silently treating the board as empty.
    """

    cli: str = "hermes"
    profile: str | None = None
    timeout: float = 30.0
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None

    def __post_init__(self) -> None:
        if not self.cli.strip():
            raise ValueError("cli must not be empty")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")

    @property
    def _runner(self) -> Callable[..., subprocess.CompletedProcess[str]]:
        return self.runner or subprocess.run

    def __call__(self, board_slug: str) -> list[str]:
        if not board_slug:
            raise CurrentStateReaderError("board identifier is required")
        command = [self.cli]
        if self.profile:
            command.extend(["--profile", self.profile])
        command.extend(["kanban", "--board", board_slug, "list", "--status", "blocked", "--json"])
        try:
            completed = self._runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout,
            )
        except subprocess.TimeoutExpired as exc:
            raise CurrentStateReaderError("Hermes CLI blocked-list read timed out") from exc
        except OSError as exc:
            raise CurrentStateReaderError("Hermes CLI blocked-list read failed") from exc
        if completed.returncode != 0:
            raise CurrentStateReaderError(
                f"Hermes CLI blocked-list read exited with {completed.returncode}"
            )
        try:
            payload = json.loads(completed.stdout)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CurrentStateReaderError(
                "Hermes CLI blocked-list output was not valid JSON"
            ) from exc
        if not isinstance(payload, list):
            raise CurrentStateReaderError("Hermes CLI blocked-list output was not an array")
        task_ids: list[str] = []
        for entry in payload:
            if not isinstance(entry, dict):
                raise CurrentStateReaderError(
                    "Hermes CLI blocked-list entry was not an object"
                )
            task_id = entry.get("id")
            if not isinstance(task_id, str) or not task_id:
                raise CurrentStateReaderError(
                    "Hermes CLI blocked-list entry omitted task id"
                )
            task_ids.append(task_id)
        return task_ids


def _current_state_from_payload(value: object, requested_task_id: str) -> CurrentTaskState:
    if not isinstance(value, dict):
        raise CurrentStateReaderError("Hermes CLI current-state output was not an object")
    task = value.get("task")
    if not isinstance(task, dict):
        raise CurrentStateReaderError("Hermes CLI current-state output omitted task")
    actual_task_id = task.get("id")
    if actual_task_id != requested_task_id:
        raise CurrentStateReaderError("Hermes CLI returned a different task")
    status = task.get("status")
    if not isinstance(status, str) or not status:
        raise CurrentStateReaderError("Hermes CLI task snapshot omitted status")

    events = value.get("events", [])
    if not isinstance(events, list):
        raise CurrentStateReaderError("Hermes CLI task snapshot events were malformed")
    latest_event_kind: str | None = None
    block_kind: str | None = None
    latest_event_id: int | None = None
    for raw_event in reversed(events):
        if not isinstance(raw_event, dict):
            raise CurrentStateReaderError("Hermes CLI task snapshot contained a malformed event")
        if latest_event_kind is None:
            candidate_kind = raw_event.get("kind")
            if isinstance(candidate_kind, str) and candidate_kind:
                latest_event_kind = candidate_kind
                candidate_event_id = raw_event.get("id")
                if isinstance(candidate_event_id, int) and not isinstance(candidate_event_id, bool):
                    latest_event_id = candidate_event_id
        if raw_event.get("kind") != "blocked":
            continue
        event_payload = raw_event.get("payload")
        if isinstance(event_payload, dict):
            candidate_block_kind = event_payload.get("kind")
            if isinstance(candidate_block_kind, str) and candidate_block_kind:
                block_kind = candidate_block_kind
                break

    runs = value.get("runs", [])
    if not isinstance(runs, list):
        raise CurrentStateReaderError("Hermes CLI task snapshot runs were malformed")
    latest_run: dict[str, Any] | None = None
    for raw_run in runs:
        if not isinstance(raw_run, dict):
            raise CurrentStateReaderError("Hermes CLI task snapshot contained a malformed run")
        if latest_run is None or _run_sort_key(raw_run) >= _run_sort_key(latest_run):
            latest_run = raw_run

    latest_run_id: int | str | None = None
    run_outcome: str | None = None
    run_error: str | None = None
    run_summary: str | None = None
    if latest_run is not None:
        candidate_run_id = latest_run.get("id")
        if not (
            (isinstance(candidate_run_id, int) and not isinstance(candidate_run_id, bool))
            or (isinstance(candidate_run_id, str) and bool(candidate_run_id))
        ):
            raise CurrentStateReaderError("Hermes CLI latest run had an invalid id")
        latest_run_id = candidate_run_id
        run_outcome = _optional_string(latest_run.get("outcome"))
        run_error = _optional_string(latest_run.get("error"))
        run_summary = _optional_string(latest_run.get("summary"))

    return CurrentTaskState(
        task_id=requested_task_id,
        status=status,
        block_kind=block_kind,
        latest_run_id=latest_run_id,
        run_outcome=run_outcome,
        run_error=run_error,
        run_summary=run_summary,
        latest_event_kind=latest_event_kind,
        latest_event_id=latest_event_id,
    )


def _run_sort_key(run: Mapping[str, Any]) -> tuple[int, int]:
    started_at = run.get("started_at")
    run_id = run.get("id")
    started = started_at if isinstance(started_at, int) and not isinstance(started_at, bool) else -1
    numeric_id = run_id if isinstance(run_id, int) and not isinstance(run_id, bool) else -1
    return started, numeric_id


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def build_live_stream_wiring(
    config: ControllerConfig,
) -> tuple[
    dict[str, StreamAdapter],
    StreamCredentials,
    HermesCliCurrentStateReader,
    HermesCliBlockedLister,
]:
    """Build all approved live seams from validated controller configuration.

    The fourth element (``HermesCliBlockedLister``) powers the daemon's
    periodic blocked-state reconcile sweep: even when the stream never
    delivered a death event (the 2026-08-06 silent-block blind window), the
    sweep lists ``status='blocked'`` per board through the CLI and feeds the
    classifier a synthetic ``gave_up`` confirmation so recoverable cards are
    reserved without waiting for an event that already happened.
    """

    stream = config.stream
    if not stream.enabled or stream.adapter != "approved_websocket":
        raise HandoffError("approved WebSocket stream mode is not enabled")
    if stream.current_state_reader not in SUPPORTED_CURRENT_STATE_READERS:
        raise HandoffError(
            "continuous stream current_state_reader must be approved-dashboard-snapshot"
        )
    if not stream.endpoint or not stream.credential_env:
        raise HandoffError("approved WebSocket endpoint and credential environment are required")
    raw_credential = os.environ.get(stream.credential_env, "").strip()
    if not raw_credential:
        raise HandoffError(
            f"approved WebSocket credential environment {stream.credential_env} is unset"
        )
    credentials = _credentials_from_environment_name(stream.credential_env, raw_credential)
    connector = WebSocketConnector()
    boards = stream.boards
    if not boards:
        boards = tuple(board.slug for board in discover_boards(config.native_boards_root))
        if not boards:
            raise HandoffError(
                "no non-archived kanban boards discovered under "
                f"{config.native_boards_root}"
            )
    adapters = {
        board: StreamAdapter(
            stream.endpoint,
            allowed_boards={board},
            connector=connector,
        )
        for board in boards
    }
    reader = HermesCliCurrentStateReader(
        cli=config.native_cli,
        profile=config.native_profile,
    )
    lister = HermesCliBlockedLister(
        cli=config.native_cli,
        profile=config.native_profile,
    )
    return adapters, credentials, reader, lister


def _credentials_from_environment_name(name: str, value: str) -> StreamCredentials:
    lowered = name.lower()
    if "ticket" in lowered:
        return StreamCredentials(ticket=value)
    if "token" in lowered:
        return StreamCredentials(token=value)
    # Do not guess a query parameter: sending an opaque credential to the wrong
    # auth scheme is both non-functional and unsafe.
    raise HandoffError(
        "approved WebSocket credential environment name must contain token or ticket"
    )


__all__ = [
    "CurrentStateReaderError",
    "HermesCliBlockedLister",
    "HermesCliCurrentStateReader",
    "SUPPORTED_CURRENT_STATE_READERS",
    "WebSocketConnector",
    "WebSocketProtocolError",
    "build_live_stream_wiring",
]
