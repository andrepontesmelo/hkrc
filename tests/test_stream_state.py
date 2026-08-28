from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from hkrc.state import (
    ControllerState,
    StateError,
    StreamEventKey,
)


def open_state(tmp_path: Path) -> ControllerState:
    return ControllerState.initialize(tmp_path / "state.sqlite3", "stream-test")


def test_stream_cursor_commits_only_a_complete_frame_and_replays_safely(
    tmp_path: Path,
) -> None:
    with open_state(tmp_path) as state:
        initial = state.reconcile_stream_cursor(
            "alpha", identity="generation-a", retention_floor=1
        )
        assert initial.cursor == 0
        assert initial.identity == "generation-a"
        assert initial.reset_count == 0

        committed = state.commit_stream_frame(
            "alpha",
            identity="generation-a",
            cursor=12,
            events=(
                StreamEventKey(4, "task-a", 1),
                StreamEventKey(12, "task-b", 2),
            ),
        )
        assert committed.cursor == 12
        assert state.get_stream_cursor("alpha").cursor == 12

        # A redelivered frame is harmless: the composite event keys are durable
        # and the accepted cursor does not move backwards.
        replayed = state.commit_stream_frame(
            "alpha",
            identity="generation-a",
            cursor=12,
            events=(
                StreamEventKey(4, "task-a", 1),
                StreamEventKey(12, "task-b", 2),
            ),
        )
        assert replayed.cursor == 12
        assert state.stream_event_count("alpha") == 2


def test_stream_frame_requires_event_identity_before_advancing_cursor(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="generation-a")

        with pytest.raises(StateError, match="event identity"):
            state.commit_stream_frame(
                "alpha", identity="generation-a", cursor=9, events=()
            )

        assert state.get_stream_cursor("alpha").cursor == 0
        assert state.stream_event_count("alpha") == 0


def test_stream_identity_rollback_and_retention_gap_reset_without_losing_idempotency(
    tmp_path: Path,
) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="generation-a")
        state.commit_stream_frame(
            "alpha",
            identity="generation-a",
            cursor=100,
            events=(StreamEventKey(100, "same-task", 7),),
        )

        replaced = state.reconcile_stream_cursor(
            "alpha",
            identity="generation-b",
            retention_floor=4,
            observed_cursor=3,
        )
        assert replaced.cursor == 0
        assert replaced.identity == "generation-b"
        assert replaced.reset_required is True
        assert replaced.reset_reason == "identity_changed"
        assert replaced.reset_count == 1

        state.commit_stream_frame(
            "alpha",
            identity="generation-b",
            cursor=3,
            events=(StreamEventKey(3, "same-task", 7),),
        )
        assert state.stream_event_count("alpha") == 2

        rollback = state.reconcile_stream_cursor(
            "alpha",
            identity="generation-b",
            retention_floor=4,
            observed_cursor=2,
        )
        assert rollback.cursor == 0
        assert rollback.reset_reason == "id_rollback"
        assert rollback.reset_count == 2

        state.commit_stream_frame(
            "alpha",
            identity="generation-b",
            cursor=2,
            events=(StreamEventKey(2, "new-task", None),),
        )
        retention = state.reconcile_stream_cursor(
            "alpha",
            identity="generation-b",
            retention_floor=8,
        )
        assert retention.cursor == 0
        assert retention.reset_reason == "retention_gap"
        assert retention.reset_count == 3


def test_one_ever_reservation_is_board_and_task_scoped(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        assert state.reserve_blocker(
            "alpha", "same-task", blocker_kind="capability", latest_event_at=1
        )
        assert not state.reserve_blocker(
            "alpha", "same-task", blocker_kind="capability", latest_event_at=2
        )
        assert state.reserve_blocker(
            "beta", "same-task", blocker_kind="capability", latest_event_at=3
        )
        assert state.reservation_count() == 2


def test_transport_failure_is_board_local_and_does_not_reset_cursor(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")
        state.reconcile_stream_cursor("beta", identity="b")
        state.commit_stream_frame(
            "alpha", identity="a", cursor=5, events=(StreamEventKey(5, "task", 1),)
        )

        failed = state.record_stream_transport_failure(
            "alpha", code="disconnected", message="socket closed"
        )
        assert failed.cursor == 5
        assert failed.last_transport_error == "disconnected: socket closed"
        assert state.get_stream_cursor("beta").cursor == 0
        assert state.get_stream_cursor("beta").last_transport_error is None

        recovered = state.reconcile_stream_cursor("alpha", identity="a")
        assert recovered.cursor == 5
        assert recovered.last_transport_error is None


def test_stream_state_write_failure_rolls_back_event_and_cursor_atomically(
    tmp_path: Path,
) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")

        deny_update = True

        def authorizer(action: int, *_args: str) -> int:
            if deny_update and action == sqlite3.SQLITE_UPDATE:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        state.connection.set_authorizer(authorizer)
        with pytest.raises(StateError, match="commit_stream_frame"):
            state.commit_stream_frame(
                "alpha",
                identity="a",
                cursor=5,
                events=(StreamEventKey(5, "task", 1),),
            )
        deny_update = False
        state.connection.set_authorizer(None)

        assert state.get_stream_cursor("alpha").cursor == 0
        assert state.stream_event_count("alpha") == 0


def test_stream_cursor_rejects_identity_mismatch_in_atomic_commit(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        state.reconcile_stream_cursor("alpha", identity="a")
        with pytest.raises(StateError, match="identity"):
            state.commit_stream_frame(
                "alpha",
                identity="b",
                cursor=1,
                events=(StreamEventKey(1, "task", 1),),
            )
        assert state.get_stream_cursor("alpha").cursor == 0
        assert state.stream_event_count("alpha") == 0


def test_stream_state_schema_is_controller_owned(tmp_path: Path) -> None:
    with open_state(tmp_path) as state:
        assert state.schema_version == 7
    with sqlite3.connect(tmp_path / "state.sqlite3") as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert "stream_cursors" in tables
    assert "stream_handled_events" in tables
    assert "kanban.db" not in str(tmp_path / "state.sqlite3")
