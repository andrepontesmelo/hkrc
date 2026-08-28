"""Self-health alerting: failure accumulation, threshold, dedupe, recovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import sqlite3

import pytest

from hkrc.cli import main
from hkrc.config import (
    ConfigError,
    ControllerConfig,
    StreamConfig,
    load_config,
    write_config,
)
from hkrc.event_stream import (
    StreamAdapter,
    StreamAuthError,
    StreamCredentials,
    StreamRetentionError,
    StreamSocket,
)
from hkrc.runtime import DaemonRuntime, StreamObserver
from hkrc.self_health import format_stream_alert, format_stream_recovery
from hkrc.state import ControllerState, StreamEventKey


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


class Connector:
    def __init__(
        self,
        *,
        socket: StreamSocket | None = None,
        failures: int = 0,
        error: BaseException | None = None,
    ) -> None:
        self.socket = socket
        self.failures = failures
        self.error = error
        self.calls = 0

    def __call__(self, url: str, _headers: Mapping[str, str]) -> StreamSocket:
        self.calls += 1
        if self.calls <= self.failures:
            if self.error is not None:
                raise self.error
            raise ConnectionError("endpoint unreachable")
        assert self.socket is not None
        return self.socket


class Clock:
    value = 0.0

    def __call__(self) -> float:
        return self.value


def event(event_id: int, task_id: str = "task-1") -> dict[str, object]:
    return {
        "id": event_id,
        "task_id": task_id,
        "run_id": 1,
        "kind": "blocked",
        "payload": {"kind": "capability", "reason": "self-health-test"},
        "created_at": 100,
    }


def frame(*events: dict[str, object]) -> str:
    return json.dumps({"events": list(events), "cursor": events[-1]["id"] if events else 0})


def adapter_for(board: str, connector: Connector) -> StreamAdapter:
    return StreamAdapter(
        f"wss://example.test/{board}/events",
        allowed_boards={board},
        connector=connector,
    )


def current_state(_board: str, task_id: str):
    return {"task_id": task_id, "status": "blocked", "block_kind": "capability"}


def open_state(tmp_path: Path) -> ControllerState:
    return ControllerState.initialize(tmp_path / "state.sqlite3", "self-health-test")


def make_observer(
    state: ControllerState,
    adapter: StreamAdapter,
    *,
    threshold: int = 3,
    alerter=None,
    clock: Clock | None = None,
) -> StreamObserver:
    return StreamObserver(
        {"main": adapter},
        state,
        credentials=StreamCredentials(token="session"),
        current_state_reader=current_state,
        monotonic=clock or Clock(),
        alert_after_consecutive_failures=threshold,
        alerter=alerter,
    )


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------


def test_format_stream_alert_states_board_code_and_timestamps() -> None:
    text = format_stream_alert(
        "main",
        failure_count=3,
        error_code="transport",
        first_failure_at="2026-08-04T09:00:00+00:00",
        last_failure_at="2026-08-04T09:02:00+00:00",
    )
    assert "board main is blind" in text
    assert "consecutive stream failures: 3" in text
    assert "error code: transport" in text
    assert "first failure: 2026-08-04T09:00:00+00:00" in text
    assert "last failure: 2026-08-04T09:02:00+00:00" in text


def test_format_stream_recovery_states_board_and_outage_window() -> None:
    text = format_stream_recovery(
        "main",
        failure_count=3,
        first_failure_at="2026-08-04T09:00:00+00:00",
        last_failure_at="2026-08-04T09:02:00+00:00",
    )
    assert "stream recovered: board main" in text
    assert "after 3 consecutive failures" in text
    assert "outage window: 2026-08-04T09:00:00+00:00 to 2026-08-04T09:02:00+00:00" in text


def test_format_stream_recovery_handles_single_timestamp() -> None:
    text = format_stream_recovery(
        "main",
        failure_count=1,
        first_failure_at="2026-08-04T09:00:00+00:00",
        last_failure_at=None,
    )
    assert "outage window: 2026-08-04T09:00:00+00:00" in text


def test_format_stream_alert_renders_hostile_slug_without_line_injection() -> None:
    text = format_stream_alert(
        "main\n- forged line",
        failure_count=3,
        error_code="transport",
        first_failure_at="2026-08-04T09:00:00+00:00",
        last_failure_at="2026-08-04T09:02:00+00:00",
    )
    # The slug's newline must not survive into the operator-facing alert.
    assert text.count("\n") == 4
    assert "\n- forged" not in text
    # Every unsafe character (newline, space) is replaced with '?'.
    assert "board main?-?forged?line is blind" in text


def test_format_stream_recovery_renders_hostile_slug_without_line_injection() -> None:
    text = format_stream_recovery(
        "main\n- forged line",
        failure_count=3,
        first_failure_at="2026-08-04T09:00:00+00:00",
        last_failure_at="2026-08-04T09:02:00+00:00",
    )
    assert text.count("\n") == 2
    assert "\n- forged" not in text
    assert "recovered: board main?-?forged?line" in text


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_stream_alert_threshold_defaults_to_three() -> None:
    assert StreamConfig().alert_after_consecutive_failures == 3


def test_stream_alert_threshold_rejects_invalid_values() -> None:
    for bad in (0, -1, 1.5, True, "3"):
        with pytest.raises(ConfigError):
            StreamConfig(alert_after_consecutive_failures=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad_board",
    [
        "main\n- forged line",
        "main\nother",
        "main other",
        "main/other",
        "main:other",
        "main\tother",
        "",
        "  ",
    ],
)
def test_stream_boards_reject_hostile_or_unsafe_slugs(bad_board: str) -> None:
    with pytest.raises(ConfigError, match="non-empty slugs"):
        StreamConfig(boards=(bad_board,))


def test_stream_boards_accept_safe_slug_grammar() -> None:
    safe = StreamConfig(boards=("main", "ops", "sub.board_1-a"))
    assert safe.boards == ("main", "ops", "sub.board_1-a")


def test_stream_alert_threshold_round_trips_through_config_file(tmp_path: Path) -> None:
    config = ControllerConfig(
        "instance-a",
        tmp_path / "native",
        tmp_path / "state.sqlite3",
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=("main",),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
            alert_after_consecutive_failures=7,
        ),
    )
    path = tmp_path / "config.toml"
    write_config(path, config)
    assert load_config(path) == config
    assert "alert_after_consecutive_failures = 7" in path.read_text()


def test_legacy_stream_config_defaults_alert_threshold(tmp_path: Path) -> None:
    path = tmp_path / "legacy.toml"
    path.write_text(
        """format_version = 1

[instance]
name = "legacy-a"
native_boards_root = "/tmp/native"

[controller]
state_db = "/tmp/state.sqlite3"

[stream]
enabled = false
adapter = "none"
boards = []
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.stream.alert_after_consecutive_failures == 3


def test_cli_init_accepts_stream_alert_threshold(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "instance.toml"
    assert (
        main(
            [
                "init",
                "--config",
                str(config_path),
                "--instance-name",
                "test-instance",
                "--native-boards-root",
                str(tmp_path / "native"),
                "--state-db",
                str(tmp_path / "state.sqlite3"),
                "--stream-alert-after-consecutive-failures",
                "5",
            ]
        )
        == 0
    )
    capsys.readouterr()
    config = load_config(config_path)
    assert config.stream.alert_after_consecutive_failures == 5


# ---------------------------------------------------------------------------
# Durable episode state
# ---------------------------------------------------------------------------


def test_failures_accumulate_per_board_and_reset_on_accepted_frame(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")
        state.reconcile_stream_cursor("beta", identity="b")

        first = state.record_stream_transport_failure(
            "alpha", code="transport", message="endpoint unreachable"
        )
        assert first.consecutive_failures == 1
        assert first.episode_first_failure_at is not None
        assert first.episode_last_failure_at is not None
        assert first.alert_sent is False
        assert state.get_stream_cursor("beta").consecutive_failures == 0

        second = state.record_stream_transport_failure(
            "alpha", code="auth_failed", message="token rotated"
        )
        assert second.consecutive_failures == 2
        # The episode start is anchored on the first failure, not the latest.
        assert second.episode_first_failure_at == first.episode_first_failure_at
        assert second.alert_sent is False

        committed = state.commit_stream_frame(
            "alpha",
            identity="a",
            cursor=5,
            events=(StreamEventKey(5, "task-1", 1),),
        )
        assert committed.consecutive_failures == 0
        assert committed.episode_first_failure_at is None
        assert committed.episode_last_failure_at is None
        assert committed.alert_sent is False
        assert committed.last_transport_error is None
        assert state.get_stream_cursor("beta").consecutive_failures == 0


def test_alert_sent_flag_survives_reconciliation_until_frame_acceptance(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")
        state.record_stream_transport_failure("alpha", code="transport", message="down")
        marked = state.mark_stream_alert_sent("alpha")
        assert marked.alert_sent is True

        # A fresh attempt (reconciliation) is not a recovery: the episode and
        # the delivered-alert flag survive so the alert is not re-sent.
        reconciled = state.reconcile_stream_cursor("alpha", identity="a")
        assert reconciled.alert_sent is True
        assert reconciled.consecutive_failures == 1
        assert reconciled.episode_first_failure_at is not None
        # The transient latest-failure diagnostic is cleared, the episode is not.
        assert reconciled.last_transport_error is None

        state.commit_stream_frame("alpha", identity="a", cursor=3, events=(StreamEventKey(3, "task-1", 1),))
        assert state.get_stream_cursor("alpha").alert_sent is False


def test_accepted_empty_frame_ends_failure_episode(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")
        state.record_stream_transport_failure("alpha", code="transport", message="down")
        state.mark_stream_alert_sent("alpha")

        # An idle board resumes with an empty, cursor-unchanged frame; that
        # still proves the transport is alive and must end the episode.
        idle = state.commit_stream_frame("alpha", identity="a", cursor=0, events=())
        assert idle.consecutive_failures == 0
        assert idle.episode_first_failure_at is None
        assert idle.episode_last_failure_at is None
        assert idle.alert_sent is False
        assert idle.last_transport_error is None


def test_schema_v3_database_upgrades_to_v5_with_episode_columns(tmp_path: Path) -> None:
    db = tmp_path / "v3.sqlite3"
    connection = sqlite3.connect(db)
    connection.executescript(
        """
        CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE controller_identity (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            instance_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE stream_cursors (
            board_slug TEXT PRIMARY KEY,
            identity TEXT NOT NULL,
            cursor INTEGER NOT NULL CHECK (cursor >= 0),
            retention_floor INTEGER,
            reset_required INTEGER NOT NULL DEFAULT 0 CHECK (reset_required IN (0, 1)),
            reset_reason TEXT,
            reset_count INTEGER NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
            last_transport_error TEXT,
            last_transport_at TEXT,
            updated_at TEXT NOT NULL
        );
        INSERT INTO stream_cursors
            (board_slug, identity, cursor, reset_required, reset_count,
             last_transport_error, updated_at)
        VALUES ('alpha', 'gen-a', 42, 0, 0, 'transport: old failure', '2026-01-01T00:00:00+00:00');
        """
    )
    connection.commit()
    connection.close()

    with ControllerState.initialize(db, "self-health-test") as state:
        assert state.schema_version == 7
        upgraded = state.get_stream_cursor("alpha")
        assert upgraded.cursor == 42
        assert upgraded.identity == "gen-a"
        assert upgraded.consecutive_failures == 0
        assert upgraded.alert_sent is False
        assert upgraded.alert_attempted is False
        assert upgraded.episode_first_failure_at is None
        # A v3 board carries its pre-upgrade transport diagnostic forward.
        assert upgraded.last_transport_error == "transport: old failure"
        # The upgraded row accepts episode bookkeeping immediately.
        failed = state.record_stream_transport_failure(
            "alpha", code="transport", message="new failure"
        )
        assert failed.consecutive_failures == 1
        assert failed.episode_first_failure_at is not None


def test_schema_v4_database_upgrades_to_v5_with_alert_attempted_column(
    tmp_path: Path,
) -> None:
    db = tmp_path / "v4.sqlite3"
    connection = sqlite3.connect(db)
    connection.executescript(
        """
        CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE controller_identity (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            instance_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE stream_cursors (
            board_slug TEXT PRIMARY KEY,
            identity TEXT NOT NULL,
            cursor INTEGER NOT NULL CHECK (cursor >= 0),
            retention_floor INTEGER,
            reset_required INTEGER NOT NULL DEFAULT 0 CHECK (reset_required IN (0, 1)),
            reset_reason TEXT,
            reset_count INTEGER NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
            last_transport_error TEXT,
            last_transport_at TEXT,
            consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
            episode_first_failure_at TEXT,
            episode_last_failure_at TEXT,
            alert_sent INTEGER NOT NULL DEFAULT 0 CHECK (alert_sent IN (0, 1)),
            updated_at TEXT NOT NULL
        );
        INSERT INTO stream_cursors
            (board_slug, identity, cursor, reset_required, reset_count,
             last_transport_error, last_transport_at, consecutive_failures,
             episode_first_failure_at, episode_last_failure_at, alert_sent,
             updated_at)
        VALUES ('alpha', 'gen-a', 42, 0, 0, 'transport: old failure',
                '2026-01-01T00:00:00+00:00', 3, '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:01:00+00:00', 0, '2026-01-01T00:01:00+00:00');
        """
    )
    connection.commit()
    connection.close()

    with ControllerState.initialize(db, "self-health-test") as state:
        assert state.schema_version == 7
        upgraded = state.get_stream_cursor("alpha")
        # The v4 episode fields survive the upgrade untouched; the new column
        # defaults to false for the in-flight episode.
        assert upgraded.cursor == 42
        assert upgraded.consecutive_failures == 3
        assert upgraded.alert_sent is False
        assert upgraded.alert_attempted is False
        # The upgraded row records an alert attempt immediately.
        attempted = state.mark_stream_alert_attempted("alpha")
        assert attempted.alert_attempted is True
        assert attempted.alert_sent is False
        # A confirmed delivery marks both flags.
        sent = state.mark_stream_alert_sent("alpha")
        assert sent.alert_sent is True
        assert sent.alert_attempted is True


# ---------------------------------------------------------------------------
# Observer alerting
# ---------------------------------------------------------------------------


def test_threshold_crossing_sends_one_alert_and_dedupes_until_recovery(
    tmp_path: Path,
) -> None:
    sent: list[str] = []
    connector = Connector(
        failures=5,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=3,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        # Failure 1 of 3: below the threshold, no alert.
        clock.value = 0.0
        observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 1
        assert sent == []
        # Backoff elapses; failure 2 of 3: still below the threshold.
        clock.value = 0.5
        observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 2
        assert sent == []
        # Failure 3 of 3: threshold crossed, exactly one alert fires.
        clock.value = 1.5
        observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 3
        assert state.get_stream_cursor("main").alert_sent is True
        assert len(sent) == 1
        alert = sent[0]
        assert "board main is blind" in alert
        assert "consecutive stream failures: 3" in alert
        assert "error code: transport" in alert
        assert "first failure:" in alert
        assert "last failure:" in alert
        # Further failures while the dashboard stays down dedupe: no re-alert.
        clock.value = 3.5
        observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 4
        assert len(sent) == 1
        clock.value = 7.5
        observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 5
        assert len(sent) == 1


def test_recovery_notice_sent_when_stream_resumes(tmp_path: Path) -> None:
    sent: list[str] = []
    connector = Connector(
        failures=3,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=3,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        # Failures 1..3: threshold crossed on the third, one alert fires.
        for when in (0.0, 0.5, 1.5):
            clock.value = when
            observer.poll()
        assert len(sent) == 1
        assert "is blind" in sent[0]
        # Backoff expires; the dashboard is back; a frame is accepted.
        clock.value = 3.5
        result = observer.poll()
        assert result.events and result.events[0].id == 9
        assert state.get_stream_cursor("main").cursor == 9
        assert state.get_stream_cursor("main").alert_sent is False
        assert len(sent) == 2
        recovery = sent[1]
        assert "stream recovered: board main" in recovery
        assert "after 3 consecutive failures" in recovery
        assert "outage window:" in recovery
        # The next healthy cycle sends nothing more.
        connector.failures = -1
        observer.poll()
        assert len(sent) == 2


def test_auth_failure_alert_carries_the_stable_error_code(tmp_path: Path) -> None:
    sent: list[str] = []
    connector = Connector(
        failures=3,
        error=StreamAuthError("token rejected"),
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=3,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        for when in (0.0, 30.0, 60.0):
            clock.value = when
            observer.poll()
        assert len(sent) == 1
        assert "error code: auth_failed" in sent[0]


def test_alert_send_failure_does_not_raise_and_recovery_still_proceeds(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def exploding_alerter(text: str) -> bool:
        calls.append(text)
        raise RuntimeError("telegram gateway down")

    connector = Connector(
        failures=2,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=exploding_alerter,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()  # failure 1 -> one alert attempt raises, swallowed
        clock.value = 0.5
        observer.poll()  # failure 2 -> NOT retried: one attempt per episode
        assert state.get_stream_cursor("main").consecutive_failures == 2
        assert state.get_stream_cursor("main").alert_sent is False
        # The episode is still remembered as having produced an alert attempt
        # even though delivery failed, so recovery is not silenced.
        assert state.get_stream_cursor("main").alert_attempted is True
        assert len(calls) == 1  # no per-failure retry spam within one episode
        assert "is blind" in calls[0]
        # The stream recovers and the accepted frame still commits: a failed
        # alert never suppresses recovery (fail-closed additive).
        clock.value = 1.5
        result = observer.poll()
        assert result.events and result.events[0].id == 9
        assert state.get_stream_cursor("main").cursor == 9
        assert state.reservation_count() == 1
        # Exactly one recovery notice is attempted after the episode ends.
        assert len(calls) == 2
        assert "stream recovered: board main" in calls[1]
        assert "after 2 consecutive failures" in calls[1]
        # The episode bookkeeping is fully reset by the accepted frame.
        assert state.get_stream_cursor("main").alert_sent is False
        assert state.get_stream_cursor("main").alert_attempted is False


def test_alert_send_returning_false_still_attempts_recovery_notice(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def failing_alerter(text: str) -> bool:
        calls.append(text)
        return False  # delivery rejected without raising

    connector = Connector(
        failures=1,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=failing_alerter,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()  # failure 1 -> alert attempt returns False
        assert len(calls) == 1
        assert "is blind" in calls[0]
        assert state.get_stream_cursor("main").alert_sent is False
        assert state.get_stream_cursor("main").alert_attempted is True
        # Recovery still commits and attempts the recovery notice: a rejected
        # alert send must not suppress the notice either.
        clock.value = 0.5
        result = observer.poll()
        assert result.events and result.events[0].id == 9
        assert state.get_stream_cursor("main").cursor == 9
        assert len(calls) == 2
        assert "stream recovered: board main" in calls[1]
        assert state.get_stream_cursor("main").alert_attempted is False


def test_failed_alert_send_is_not_retried_on_later_failures(tmp_path: Path) -> None:
    # ADV-001 regression: a rejected alert send (returns False) is recorded
    # once per outage episode and never retried on every later failure, so the
    # operator cannot receive duplicate or count-ambiguous alerts.
    calls: list[str] = []

    def failing_alerter(text: str) -> bool:
        calls.append(text)
        return False  # delivery rejected without raising

    connector = Connector(
        failures=3,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=failing_alerter,
            clock=clock,
        )
        # Three consecutive transport failures, all above the threshold: the
        # first produces the single alert attempt; the later failures dedupe.
        for when in (0.0, 0.5, 1.5):
            clock.value = when
            observer.poll()
        cursor = state.get_stream_cursor("main")
        assert cursor.consecutive_failures == 3
        assert cursor.alert_sent is False
        assert cursor.alert_attempted is True
        assert len(calls) == 1
        assert "consecutive stream failures: 1" in calls[0]
        # Recovery still fires exactly once after the stream resumes.
        clock.value = 3.5
        result = observer.poll()
        assert result.events and result.events[0].id == 9
        assert len(calls) == 2
        assert "stream recovered: board main" in calls[1]


def test_malformed_frame_does_not_accumulate_or_alert(tmp_path: Path) -> None:
    # ADV-003 regression: a protocol/data fault (malformed frame) is not a
    # transport outage and must neither accumulate failures nor false-alert.
    sent: list[str] = []
    connector = Connector(
        failures=0,
        socket=FakeSocket(["not-json", "not-json"]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()  # malformed frame
        assert sent == []
        cursor = state.get_stream_cursor("main")
        assert cursor.consecutive_failures == 0
        assert cursor.alert_sent is False
        assert cursor.alert_attempted is False
        assert cursor.cursor == 0
        # Backoff keeps the board observed; a repeated malformed frame after
        # the bounded delay still does not accumulate or alert.
        clock.value = 31.0
        observer.poll()
        assert sent == []
        cursor = state.get_stream_cursor("main")
        assert cursor.consecutive_failures == 0
        assert cursor.alert_sent is False
        assert cursor.alert_attempted is False


def test_retention_error_does_not_accumulate_or_alert(tmp_path: Path) -> None:
    # ADV-003 regression: retention outcomes are not transport failures.
    sent: list[str] = []
    connector = Connector(
        failures=1,
        error=StreamRetentionError("history not retained"),
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()
        assert sent == []
        cursor = state.get_stream_cursor("main")
        assert cursor.consecutive_failures == 0
        assert cursor.alert_sent is False
        assert cursor.alert_attempted is False


def test_cursor_invalid_frame_does_not_accumulate_or_alert(tmp_path: Path) -> None:
    # ADV-003 regression: a protocol/data fault (empty frame advancing the
    # cursor) is not a transport outage.
    sent: list[str] = []
    connector = Connector(
        failures=0,
        socket=FakeSocket([json.dumps({"events": [], "cursor": 5})]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()
        assert sent == []
        cursor = state.get_stream_cursor("main")
        assert cursor.consecutive_failures == 0
        assert cursor.alert_sent is False
        assert cursor.alert_attempted is False
        assert cursor.cursor == 0


class _RecordLogHandler(logging.Handler):
    """Collect emitted LogRecords for journald-channel assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_stream_alert_sender_logs_to_journal_and_returns_true(
    tmp_path: Path,
) -> None:
    # t_ad7068b9: self-health alerts go to the journald log channel only.
    # The sender emits one structured ``stream_alert`` record and reports
    # delivered so the episode dedupe (alert_sent) still holds.
    handler = _RecordLogHandler()
    logger = logging.getLogger("hkrc.test.journald")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        config = ControllerConfig(
            "instance-a",
            tmp_path / "native",
            tmp_path / "state.sqlite3",
            native_cli="fake-hermes",
            telegram_chat_id="123",
        )
        runtime = DaemonRuntime(config, logger=logger)
        assert runtime._stream_alert_sender("probe message") is True
        assert len(handler.records) == 1
        payload = json.loads(handler.records[0].getMessage())
        assert payload["event"] == "stream_alert"
        assert payload["text"] == "probe message"
    finally:
        logger.removeHandler(handler)


def test_stream_alert_sender_has_no_telegram_send_path(tmp_path: Path) -> None:
    # The 2026-08-11 Telegram mute is structural: DaemonRuntime no longer
    # accepts an alert runner, so the alerter cannot produce a native
    # ``hermes send`` argv for any destination or profile.
    with pytest.raises(TypeError):
        DaemonRuntime(
            ControllerConfig(
                "instance-a",
                tmp_path / "native",
                tmp_path / "state.sqlite3",
                native_cli="fake-hermes",
                telegram_chat_id="123",
            ),
            alert_runner=lambda command: None,  # type: ignore[call-arg]
        )


def test_recovery_notice_send_failure_does_not_raise(tmp_path: Path) -> None:
    calls: list[str] = []

    def alerter(text: str) -> bool:
        calls.append(text)
        if "is blind" in text:
            return True  # the alert itself delivers
        raise RuntimeError("recovery send failed")

    connector = Connector(
        failures=1,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=1,
            alerter=alerter,
            clock=clock,
        )
        clock.value = 0.0
        observer.poll()  # failure 1 -> alert delivered, alert_sent set
        assert state.get_stream_cursor("main").alert_sent is True
        clock.value = 0.5
        result = observer.poll()  # recovery -> notice send raises, swallowed
        assert result.events and result.events[0].id == 9
        assert state.get_stream_cursor("main").cursor == 9
        assert state.get_stream_cursor("main").alert_sent is False
        assert len(calls) == 2
        assert "stream recovered" in calls[1]


def test_alert_dedupe_and_recovery_survive_daemon_restart(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    identity = "wss://example.test/main/events|main"
    with ControllerState.initialize(state_path, "self-health-test") as state:
        state.reconcile_stream_cursor("main", identity=identity)
        state.record_stream_transport_failure(
            "main", code="transport", message="down"
        )
        state.mark_stream_alert_sent("main")
        assert state.get_stream_cursor("main").alert_sent is True

    # A daemon restart opens the same durable state: the delivered alert still
    # dedupes (no second alert) and the first accepted frame after the resume
    # still sends exactly one recovery notice with the pre-restart episode data.
    sent: list[str] = []
    connector = Connector(
        failures=0,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with ControllerState.open_existing(state_path) as state:
        observer = make_observer(
            state,
            adapter_for("main", connector),
            threshold=3,
            alerter=lambda text: sent.append(text) or True,
            clock=clock,
        )
        clock.value = 0.0
        result = observer.poll()
        assert result.events and result.events[0].id == 9
        assert state.get_stream_cursor("main").cursor == 9
        assert state.get_stream_cursor("main").alert_sent is False
        assert state.get_stream_cursor("main").consecutive_failures == 0
        assert len(sent) == 1
        assert "stream recovered" in sent[0]
        assert "after 1 consecutive failures" in sent[0]
        # The healthy cycle after the recovery sends nothing more.
        observer.poll()
        assert len(sent) == 1


def test_alerting_is_off_when_threshold_not_configured(tmp_path: Path) -> None:
    connector = Connector(
        failures=3,
        socket=FakeSocket([frame(event(9))]),
    )
    clock = Clock()
    with open_state(tmp_path) as state:
        observer = StreamObserver(
            {"main": adapter_for("main", connector)},
            state,
            credentials=StreamCredentials(token="session"),
            current_state_reader=current_state,
            monotonic=clock,
        )
        for when in (0.0, 0.5, 1.5):
            clock.value = when
            observer.poll()
        assert state.get_stream_cursor("main").consecutive_failures == 3
        assert state.get_stream_cursor("main").alert_sent is False
