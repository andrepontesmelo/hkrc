"""Live-ish end-to-end verification of HKRC self-health alerting.

Drives the real DaemonRuntime + StreamObserver + ControllerState through a
simulated outage (3 transport failures, then recovery) with an injected
logger that captures the journald records instead of touching Telegram.
Prints the alert/recovery messages the operator would see in journald.

Usage: python3 scripts/verify_self_health.py
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from hkrc.config import ControllerConfig
from hkrc.event_stream import StreamAdapter, StreamCredentials, StreamSocket
from hkrc.runtime import DaemonRuntime
from hkrc.state import ControllerState


class _RecordLogHandler(logging.Handler):
    """Collect emitted LogRecords so the script can assert on journald output."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _event(record: logging.LogRecord) -> str:
    return json.loads(record.getMessage())["event"]


def _text(record: logging.LogRecord) -> str:
    return json.loads(record.getMessage())["text"]


class ScriptedSocket:
    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)

    def recv(self) -> str:
        if not self.frames:
            raise StopIteration
        return self.frames.pop(0)

    def close(self) -> None:
        self.frames.clear()


class OutageConnector:
    """Fail the first `failures` connects, then serve one real frame."""

    def __init__(self, failures: int, frames: list[str]) -> None:
        self.failures = failures
        self.frames = frames
        self.calls = 0

    def __call__(self, url: str, headers: Mapping[str, str]) -> StreamSocket:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("dashboard endpoint unreachable")
        return ScriptedSocket(self.frames)


def frame(*events: dict[str, object]) -> str:
    return json.dumps(
        {"events": list(events), "cursor": events[-1]["id"] if events else 0}
    )


def event(event_id: int) -> dict[str, object]:
    return {
        "id": event_id,
        "task_id": "t_healthcheck",
        "run_id": 1,
        "kind": "blocked",
        "payload": {"kind": "capability", "reason": "verification"},
        "created_at": 100,
    }


def current_state(_board: str, task_id: str) -> dict[str, object]:
    return {"task_id": task_id, "status": "blocked", "block_kind": "capability"}


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hkrc-self-health-") as tmp:
        root = Path(tmp)
        config = ControllerConfig(
            "verify",
            root / "native",
            root / "state" / "state.sqlite3",
            native_cli="hermes",
            telegram_chat_id="123456",
        )
        handler = _RecordLogHandler()
        logger = logging.getLogger("hkrc.verify")
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        adapter = StreamAdapter(
            "wss://dashboard.example.test/api/plugins/kanban/events",
            allowed_boards={"main"},
            connector=OutageConnector(3, [frame(event(7))]),
        )
        runtime = DaemonRuntime(
            config,
            stream_adapters={"main": adapter},
            stream_credentials=StreamCredentials(ticket="opaque-ticket"),
            current_state_reader=current_state,
            logger=logger,
        )

        with ControllerState.initialize(config.state_db, config.instance_name) as state:
            # Three consecutive transport failures: threshold is 3, so the
            # journald alert fires exactly on the third failure.  The
            # adapter's bounded backoff (0.5s, 1s, 2s, ...) gates real
            # wall-clock reconnects, so each cycle waits out the previous
            # failure's backoff.
            for cycle, wait in ((1, 0.0), (2, 0.6), (3, 1.1)):
                if wait:
                    time.sleep(wait)
                result = runtime.run_cycle(state)
                assert result.error is None, result.error
                cursor = state.get_stream_cursor("main")
                print(
                    f"cycle {cycle}: failures={cursor.consecutive_failures} "
                    f"alert_sent={cursor.alert_sent}"
                )
            stream_alerts = [
                record for record in handler.records if _event(record) == "stream_alert"
            ]
            assert len(stream_alerts) == 1, (
                f"expected exactly one alert, got {len(stream_alerts)}"
            )
            # The dashboard returns: the next cycle (after the failure-3
            # backoff) accepts a frame and sends the recovery notice.
            time.sleep(2.1)
            result = runtime.run_cycle(state)
            assert result.error is None, result.error
            cursor = state.get_stream_cursor("main")
            print(
                f"recovery cycle: cursor={cursor.cursor} "
                f"failures={cursor.consecutive_failures} "
                f"alert_sent={cursor.alert_sent}"
            )
            assert cursor.cursor == 7
            assert cursor.alert_sent is False
            stream_alerts = [
                record for record in handler.records if _event(record) == "stream_alert"
            ]
            assert len(stream_alerts) == 2, (
                f"expected alert + recovery, got {len(stream_alerts)}"
            )
            assert "is blind" in _text(stream_alerts[0])
            assert "recovered" in _text(stream_alerts[1])
            print("alert message:")
            print(_text(stream_alerts[0]))
            print("recovery message:")
            print(_text(stream_alerts[1]))
            print(
                "VERIFY OK: exactly one journald alert after 3 failures, "
                "then one recovery notice; no Telegram send attempted"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
