from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import socket
import subprocess

import pytest

from hkrc.classifier import CurrentTaskState
from hkrc.config import ControllerConfig, StreamConfig
from hkrc.event_stream import StreamAuthError
from hkrc.handoff import HandoffError
from hkrc.live import (
    CurrentStateReaderError,
    HermesCliBlockedLister,
    HermesCliCurrentStateReader,
    WebSocketConnector,
    build_live_stream_wiring,
)


def snapshot(task_id: str = "t_1") -> str:
    return json.dumps(
        {
            "task": {"id": task_id, "status": "blocked"},
            "events": [
                {"kind": "created", "payload": {}, "created_at": 1, "run_id": None},
                {
                    "kind": "blocked",
                    "payload": {"kind": "capability", "reason": "x"},
                    "created_at": 2,
                    "run_id": 7,
                },
            ],
            "runs": [
                {
                    "id": 7,
                    "status": "blocked",
                    "outcome": "blocked",
                    "summary": "gave up",
                    "error": None,
                    "started_at": 2,
                }
            ],
        }
    )


def test_cli_reader_uses_json_show_and_builds_typed_state() -> None:
    calls: list[list[str]] = []

    def runner(command, **kwargs):
        calls.append(list(command))
        assert kwargs["capture_output"] is True
        return subprocess.CompletedProcess(command, 0, snapshot(), "")

    reader = HermesCliCurrentStateReader(cli="hermes", profile="default", runner=runner)
    state = reader("main", "t_1")

    assert isinstance(state, CurrentTaskState)
    assert state.task_id == "t_1"
    assert state.status == "blocked"
    assert state.block_kind == "capability"
    assert state.latest_run_id == 7
    assert state.latest_event_kind == "blocked"
    assert calls == [["hermes", "--profile", "default", "kanban", "--board", "main", "show", "t_1", "--json"]]


def test_cli_reader_fails_closed_on_bad_json_and_task_mismatch() -> None:
    def bad_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, "not json", "")

    with pytest.raises(CurrentStateReaderError, match="valid JSON"):
        HermesCliCurrentStateReader(runner=bad_runner)("main", "t_1")

    def mismatch_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 0, snapshot("other"), "")

    with pytest.raises(CurrentStateReaderError, match="different task"):
        HermesCliCurrentStateReader(runner=mismatch_runner)("main", "t_1")


def test_cli_reader_preserves_nonzero_and_timeout_as_safe_errors() -> None:
    def failed_runner(command, **kwargs):
        return subprocess.CompletedProcess(command, 1, "", "private error")

    with pytest.raises(CurrentStateReaderError, match="exited with 1"):
        HermesCliCurrentStateReader(runner=failed_runner)("main", "t_1")

    def timeout_runner(command, **kwargs):
        raise subprocess.TimeoutExpired(command, 30)

    with pytest.raises(CurrentStateReaderError, match="timed out"):
        HermesCliCurrentStateReader(runner=timeout_runner)("main", "t_1")


def test_live_wiring_reads_only_named_credential_and_creates_one_adapter_per_board(monkeypatch) -> None:
    config = ControllerConfig(
        "test",
        Path("/unused/native"),
        Path("/unused/state"),
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=("main", "ops"),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    monkeypatch.setenv("HKRC_STREAM_TICKET", "opaque")
    adapters, credentials, reader, lister = build_live_stream_wiring(config)
    assert sorted(adapters) == ["main", "ops"]
    assert credentials.ticket == "opaque"
    assert credentials.token is None
    assert isinstance(reader, HermesCliCurrentStateReader)
    assert isinstance(lister, HermesCliBlockedLister)


def test_live_wiring_rejects_missing_or_ambiguous_credentials(monkeypatch) -> None:
    config = ControllerConfig(
        "test",
        Path("/unused/native"),
        Path("/unused/state"),
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/events",
            boards=("main",),
            credential_env="HKRC_STREAM_AUTH",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    with pytest.raises(Exception, match="unset"):
        build_live_stream_wiring(config)
    monkeypatch.setenv("HKRC_STREAM_AUTH", "opaque")
    with pytest.raises(Exception, match="token or ticket"):
        build_live_stream_wiring(config)


def test_websocket_connector_maps_http_auth_failure_without_dependency() -> None:
    class FakeResponse:
        status_code = 401

    class FakeError(Exception):
        response = FakeResponse()

    def connect(_url, _headers):
        raise FakeError()

    with pytest.raises(StreamAuthError):
        WebSocketConnector(connect=connect)("wss://example.test/events", {})


def test_live_wiring_discovers_all_non_archived_boards_when_allowlist_empty(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "boards"
    for slug in ("alpha", "beta"):
        board = root / slug
        board.mkdir(parents=True)
        (board / "kanban.db").write_text("")
        (board / "board.json").write_text(json.dumps({"slug": slug}), encoding="utf-8")
    archived = root / "old"
    archived.mkdir(parents=True)
    (archived / "kanban.db").write_text("")
    (archived / "board.json").write_text(
        json.dumps({"slug": "old", "archived": True}), encoding="utf-8"
    )
    config = ControllerConfig(
        "test",
        root,
        Path("/unused/state"),
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=(),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    monkeypatch.setenv("HKRC_STREAM_TICKET", "opaque")

    adapters, credentials, reader, lister = build_live_stream_wiring(config)

    assert sorted(adapters) == ["alpha", "beta"]
    assert credentials.ticket == "opaque"
    assert isinstance(reader, HermesCliCurrentStateReader)
    assert isinstance(lister, HermesCliBlockedLister)


def test_live_wiring_fails_closed_when_no_boards_are_discovered(
    monkeypatch, tmp_path: Path
) -> None:
    root = tmp_path / "empty-boards"
    root.mkdir(parents=True)
    config = ControllerConfig(
        "test",
        root,
        Path("/unused/state"),
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=(),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    monkeypatch.setenv("HKRC_STREAM_TICKET", "opaque")

    with pytest.raises(HandoffError, match="no non-archived kanban boards"):
        build_live_stream_wiring(config)


def test_websocket_connector_sets_receive_timeout_and_decodes_frame(monkeypatch) -> None:
    class HandshakeSocket:
        def __init__(self) -> None:
            self.incoming = b""
            self.sent: list[bytes] = []
            self.timeouts: list[float | None] = []
            self.closed = False

        def sendall(self, data: bytes) -> None:
            self.sent.append(data)
            request = data.decode("ascii")
            key = next(
                line.split(":", 1)[1].strip()
                for line in request.split("\r\n")
                if line.lower().startswith("sec-websocket-key:")
            )
            accept = base64.b64encode(
                hashlib.sha1(
                    (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                ).digest()
            ).decode("ascii")
            body = b'{"events": [], "cursor": 0}'
            self.incoming = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "\r\n"
            ).encode("ascii") + bytes((0x81, len(body))) + body

        def recv(self, length: int) -> bytes:
            if not self.incoming:
                raise socket.timeout("silent stream")
            chunk, self.incoming = self.incoming[:length], self.incoming[length:]
            return chunk

        def settimeout(self, value: float | None) -> None:
            self.timeouts.append(value)

        def close(self) -> None:
            self.closed = True

    raw_socket = HandshakeSocket()
    monkeypatch.setattr(
        "hkrc.live.socket.create_connection",
        lambda _address, timeout: raw_socket,
    )

    connection = WebSocketConnector()("ws://127.0.0.1/events", {})

    assert raw_socket.timeouts == [20.0]
    assert connection.recv() == '{"events": [], "cursor": 0}'
    with pytest.raises(socket.timeout, match="silent stream"):
        connection.recv()


class _HandshakeSocket:
    """Raw-socket stand-in that completes the 101 handshake then goes silent."""

    def __init__(self) -> None:
        self.incoming = b""
        self.sent: list[bytes] = []
        self.closed = False

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)
        request = data.decode("ascii")
        key = next(
            line.split(":", 1)[1].strip()
            for line in request.split("\r\n")
            if line.lower().startswith("sec-websocket-key:")
        )
        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
            ).digest()
        ).decode("ascii")
        self.incoming = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        ).encode("ascii")

    def recv(self, length: int) -> bytes:
        if not self.incoming:
            raise socket.timeout("silent stream")
        chunk, self.incoming = self.incoming[:length], self.incoming[length:]
        return chunk

    def settimeout(self, value: float | None) -> None:
        del value

    def close(self) -> None:
        self.closed = True


def test_websocket_socket_idle_timeout_at_frame_boundary_keeps_connection_open(
    monkeypatch,
) -> None:
    raw_socket = _HandshakeSocket()
    monkeypatch.setattr(
        "hkrc.live.socket.create_connection",
        lambda _address, timeout: raw_socket,
    )

    connection = WebSocketConnector()("ws://127.0.0.1/events", {})

    with pytest.raises(socket.timeout, match="silent stream"):
        connection.recv()
    assert raw_socket.closed is False


class _PartialFrameSocket(_HandshakeSocket):
    """Delivers one frame header byte after the handshake, then goes silent."""

    def __init__(self) -> None:
        super().__init__()
        self.partial_sent = False

    def recv(self, length: int) -> bytes:
        if not self.incoming:
            if not self.partial_sent:
                self.partial_sent = True
                return b"\x81"  # fin + text opcode, first header byte only
            raise socket.timeout("silent mid-frame")
        chunk, self.incoming = self.incoming[:length], self.incoming[length:]
        return chunk


def test_websocket_socket_mid_frame_timeout_drops_connection(monkeypatch) -> None:
    raw_socket = _PartialFrameSocket()
    monkeypatch.setattr(
        "hkrc.live.socket.create_connection",
        lambda _address, timeout: raw_socket,
    )

    connection = WebSocketConnector()("ws://127.0.0.1/events", {})

    # A timeout after frame bytes were consumed is a transport failure: the
    # stream can no longer be trusted and the socket must be dropped, never
    # misread as an idle board.
    with pytest.raises(ConnectionError, match="mid-frame"):
        connection.recv()
    assert raw_socket.closed is True
