from __future__ import annotations

from pathlib import Path

from hkrc.config import ControllerConfig
from hkrc.handoff import (
    HANDOFF_COMMENT,
    LEAD_ASSIGNEE,
    UNCLAIMED_CHILD_COMMENT,
    NativeResult,
    execute_handoff,
)
from hkrc.state import ControllerState
import json
import sqlite3
from typing import Any


NOW = 10_000


def make_board(
    root: Path,
    slug: str,
    tasks: list[dict[str, Any]],
    *,
    archived: bool = False,
    links: list[tuple[str, str]] | None = None,
) -> Path:
    board = root / slug
    board.mkdir(parents=True)
    (board / "board.json").write_text(
        json.dumps({"slug": slug, "archived": archived}), encoding="utf-8"
    )
    connection = sqlite3.connect(board / "kanban.db")
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            block_kind TEXT,
            assignee TEXT,
            created_at INTEGER,
            claim_lock TEXT,
            claim_expires INTEGER,
            current_run_id INTEGER
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            run_id INTEGER,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE task_links (
            parent_id TEXT NOT NULL,
            child_id TEXT NOT NULL,
            PRIMARY KEY (parent_id, child_id)
        );
        """
    )
    for number, item in enumerate(tasks, 1):
        connection.execute(
            "INSERT INTO tasks(id, title, status, block_kind, assignee, created_at, "
            "claim_lock, claim_expires, current_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item["id"],
                item.get("title", item["id"]),
                item["status"],
                item.get("block_kind"),
                item.get("assignee"),
                item.get("created_at"),
                item.get("claim_lock"),
                item.get("claim_expires"),
                item.get("current_run_id"),
            ),
        )
        for event in item.get("events", []):
            connection.execute(
                "INSERT INTO task_events(id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, NULL, ?, ?, ?)",
                (number * 100 + event.get("id", 0), item["id"], event["kind"], json.dumps(event.get("payload", {})), event["created_at"]),
            )
    for parent_id, child_id in links or []:
        connection.execute(
            "INSERT INTO task_links(parent_id, child_id) VALUES (?, ?)",
            (parent_id, child_id),
        )
    connection.commit()
    connection.close()
    return board


def task(task_id: str, kind: str | None, event_time: int, *, event_kind: str = "blocked") -> dict[str, Any]:
    return {
        "id": task_id,
        "status": "blocked",
        "block_kind": kind,
        "events": [{"id": 1, "kind": event_kind, "created_at": event_time}],
    }


def child_task(task_id: str, status: str, event_time: int, **extra: Any) -> dict[str, Any]:
    return {
        "id": task_id,
        "status": status,
        "events": [{"id": 1, "kind": "created", "created_at": event_time}],
        **extra,
    }


def handoff_config(root: Path, state_path: Path) -> ControllerConfig:
    return ControllerConfig(
        "test",
        root,
        state_path,
        native_cli="fake-hermes",
        native_profile="target-instance",
        telegram_chat_id="-1001234567890",
        telegram_chat_type="group",
        telegram_thread_id="42",
        telegram_user_id="andre",
        telegram_notifier_profile="default",
    )


def test_handoff_orders_subscription_comment_reassign_unblock_and_prints_stdout(
    tmp_path: Path,
) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0, stdout=f"fake-{command[command.index('kanban') + 1]}")

    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(config, state, now=NOW, runner=fake_cli)
        intervention = state.get_intervention("alpha", "t_cap")

    assert [command[command.index("kanban") + 3] for command in calls] == [
        "notify-subscribe",
        "comment",
        "reassign",
        "unblock",
    ]
    assert calls[0] == [
        "fake-hermes",
        "--profile",
        "target-instance",
        "kanban",
        "--board",
        "alpha",
        "notify-subscribe",
        "t_cap",
        "--platform",
        "telegram",
        "--chat-id",
        "-1001234567890",
        "--chat-type",
        "group",
        "--thread-id",
        "42",
        "--user-id",
        "andre",
        "--notifier-profile",
        "default",
    ]
    assert calls[1][-1] == HANDOFF_COMMENT
    assert calls[2][-1] == LEAD_ASSIGNEE
    assert report.completed == 1
    assert report.failed == 0
    assert report.exit_code == 0
    assert intervention is not None
    assert intervention["phase"] == "complete"
    assert "native_stdout" in "\n".join(report.lines)
    assert "summary reserved=1 started=1 completed=1 failed=0 skipped=0" in report.lines[-1]


def test_handoff_partial_failure_records_phase_and_never_retries_or_rolls_back(
    tmp_path: Path,
) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        if command[command.index("kanban") + 3] == "comment":
            return NativeResult(7, stderr="comment failed")
        return NativeResult(0)

    with ControllerState.initialize(state_path, "test") as state:
        first = execute_handoff(config, state, now=NOW, runner=fake_cli)
        second = execute_handoff(config, state, now=NOW, runner=fake_cli)
        intervention = state.get_intervention("alpha", "t_cap")
        reservation = state.connection.execute(
            "SELECT reservation_state FROM blocker_reservations WHERE board_slug = ? AND task_id = ?",
            ("alpha", "t_cap"),
        ).fetchone()

    assert len(calls) == 2
    assert first.failed == 1
    assert first.exit_code == 1
    assert second.started == 0
    assert second.failed == 0
    assert any("action=already_reserved" in line for line in second.lines)
    assert intervention is not None
    assert intervention["phase"] == "comment"
    assert intervention["outcome"] == "error"
    assert intervention["error"] == "comment failed"
    assert reservation["reservation_state"] == "started"


def test_duplicate_invocation_after_success_has_no_native_calls(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0)

    with ControllerState.initialize(state_path, "test") as state:
        first = execute_handoff(config, state, now=NOW, runner=fake_cli)
        second = execute_handoff(config, state, now=NOW, runner=fake_cli)

    assert first.completed == 1
    assert second.started == 0
    assert len(calls) == 4
    assert any("action=already_reserved" in line for line in second.lines)
    assert second.lines[-1] == "summary reserved=0 started=0 completed=0 failed=0 skipped=0"


def test_handoff_backfill_reserves_older_and_default_notes_stale(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0)

    with ControllerState.initialize(state_path, "test") as state:
        default_report = execute_handoff(config, state, now=NOW, runner=fake_cli)

    # Default 3600s window: the old blocker is neither reserved nor handed off,
    # but it is visible as a stale note instead of silently omitted.
    assert default_report.reserved == 0
    assert default_report.started == 0
    assert calls == []
    assert any(line.startswith("note ") for line in default_report.lines)
    assert any("outside recency window, use --backfill" in line for line in default_report.lines)
    assert "summary reserved=0 started=0 completed=0 failed=0 skipped=0" in default_report.lines[-1]

    calls.clear()
    with ControllerState.initialize(state_path, "test") as state:
        widened_report = execute_handoff(
            config, state, now=NOW, window_seconds=7200, runner=fake_cli
        )

    # A wider window reserves and hands off the same task, with no stale note.
    assert widened_report.reserved == 1
    assert widened_report.completed == 1
    assert len(calls) == 4
    assert not any(line.startswith("note ") for line in widened_report.lines)


def test_run_requires_configured_telegram_destination_without_reserving(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = ControllerConfig("test", root, state_path)

    with ControllerState.initialize(state_path, "test") as state:
        try:
            execute_handoff(config, state, now=NOW, runner=lambda _: NativeResult(0))
        except RuntimeError as exc:
            assert "telegram destination" in str(exc)
        else:
            raise AssertionError("missing destination must fail before reservation")
        assert state.reservation_count() == 0


def test_subscription_is_task_specific_for_each_board_and_task(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "one", [task("same", "capability", NOW)])
    make_board(root, "two", [task("same", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0)

    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(config, state, now=NOW, runner=fake_cli)

    subscriptions = [command for command in calls if "notify-subscribe" in command]
    assert report.completed == 2
    assert [(command[command.index("--board") + 1], command[command.index("notify-subscribe") + 1]) for command in subscriptions] == [
        ("one", "same"),
        ("two", "same"),
    ]


def test_only_reserved_candidates_are_started(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_skip", "transient", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(
            config,
            state,
            now=NOW,
            runner=lambda command: calls.append(list(command)) or NativeResult(0),
        )

    assert report.skipped == 1
    assert report.started == 0
    assert report.completed == 0
    assert calls == []
    assert "action=skipped" in report.lines[0]


def test_child_handoff_is_alert_only_subscription_and_comment(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            child_task("t_child", "todo", NOW - 5000, assignee="reviewer"),
        ],
        links=[("t_parent", "t_child")],
    )
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0, stdout=f"fake-{command[command.index('kanban') + 1]}")

    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(config, state, now=NOW, runner=fake_cli)
        intervention = state.get_intervention("alpha", "t_child")

    assert [command[command.index("kanban") + 3] for command in calls] == [
        "notify-subscribe",
        "comment",
    ]
    assert calls[1][-1] == UNCLAIMED_CHILD_COMMENT
    assert UNCLAIMED_CHILD_COMMENT != HANDOFF_COMMENT
    assert not any("reassign" in command for command in calls)
    assert not any("unblock" in command for command in calls)
    assert report.completed == 1
    assert report.failed == 0
    assert report.exit_code == 0
    assert intervention is not None
    assert intervention["phase"] == "complete"
    assert "summary reserved=1 started=1 completed=1 failed=0 skipped=0" in report.lines[-1]


def test_child_handoff_failed_comment_records_phase_and_never_retries(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            child_task("t_child", "todo", NOW - 5000, assignee="reviewer"),
        ],
        links=[("t_parent", "t_child")],
    )
    state_path = tmp_path / "controller.sqlite3"
    config = handoff_config(root, state_path)
    calls: list[list[str]] = []

    def fake_cli(command: list[str]) -> NativeResult:
        calls.append(list(command))
        if command[command.index("kanban") + 3] == "comment":
            return NativeResult(7, stderr="comment failed")
        return NativeResult(0)

    with ControllerState.initialize(state_path, "test") as state:
        first = execute_handoff(config, state, now=NOW, runner=fake_cli)
        second = execute_handoff(config, state, now=NOW, runner=fake_cli)
        intervention = state.get_intervention("alpha", "t_child")
        reservation = state.connection.execute(
            "SELECT reservation_state FROM blocker_reservations WHERE board_slug = ? AND task_id = ?",
            ("alpha", "t_child"),
        ).fetchone()

    assert len(calls) == 2
    assert [command[command.index("kanban") + 3] for command in calls] == [
        "notify-subscribe",
        "comment",
    ]
    assert first.failed == 1
    assert first.exit_code == 1
    assert second.started == 0
    assert second.failed == 0
    assert any("action=already_reserved" in line for line in second.lines)
    assert intervention is not None
    assert intervention["phase"] == "comment"
    assert intervention["outcome"] == "error"
    assert intervention["error"] == "comment failed"
    assert reservation["reservation_state"] == "started"
