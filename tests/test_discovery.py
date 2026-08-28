from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os
from typing import Any

import pytest

from hkrc.cli import main, parse_duration
from hkrc.config import ControllerConfig, write_config
from hkrc.discovery import (
    RECENCY_WINDOW_SECONDS,
    UNCLAIMED_CHILD_KIND,
    DiscoveryError,
    discover_and_reserve,
    discover_candidates,
    discover_stale_blockers,
    discover_unclaimed_children,
    stale_blocker_note,
)
from hkrc.state import ControllerState


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
    for number, task in enumerate(tasks, 1):
        connection.execute(
            "INSERT INTO tasks(id, title, status, block_kind, assignee, created_at, "
            "claim_lock, claim_expires, current_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task["id"],
                task.get("title", task["id"]),
                task["status"],
                task.get("block_kind"),
                task.get("assignee"),
                task.get("created_at"),
                task.get("claim_lock"),
                task.get("claim_expires"),
                task.get("current_run_id"),
            ),
        )
        for event in task.get("events", []):
            connection.execute(
                "INSERT INTO task_events(id, task_id, run_id, kind, payload, created_at) "
                "VALUES (?, ?, NULL, ?, ?, ?)",
                (
                    number * 100 + event.get("id", 0),
                    task["id"],
                    event["kind"],
                    json.dumps(event.get("payload", {})),
                    event["created_at"],
                ),
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


def child_task(
    task_id: str,
    status: str,
    event_time: int,
    *,
    assignee: str | None = None,
    claim_lock: str | None = None,
    claim_expires: int | None = None,
    current_run_id: int | None = None,
) -> dict[str, Any]:
    return {
        "id": task_id,
        "status": status,
        "assignee": assignee,
        "claim_lock": claim_lock,
        "claim_expires": claim_expires,
        "current_run_id": current_run_id,
        "events": [{"id": 1, "kind": "created", "created_at": event_time}],
    }


def test_discovery_filters_recent_blockers_and_uses_latest_event(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            task("t_cap", "capability", NOW - 10),
            task("t_input", "needs_input", NOW - 10),
            task("t_old", "capability", NOW - 3601),
            task("t_gave", None, NOW - 20, event_kind="gave_up"),
            {
                **task("t_latest", "capability", NOW - 50),
                "events": [
                    {"id": 1, "kind": "blocked", "created_at": NOW - 100},
                    {"id": 2, "kind": "heartbeat", "created_at": NOW - 50},
                ],
            },
        ],
    )
    make_board(root, "archived", [task("t_archived", "capability", NOW)], archived=True)

    candidates = discover_candidates(root, now=NOW)

    assert [(item.board_slug, item.task_id) for item in candidates] == [
        ("alpha", "t_cap"),
        ("alpha", "t_gave"),
        ("alpha", "t_input"),
        ("alpha", "t_latest"),
    ]
    gave_up = next(item for item in candidates if item.task_id == "t_gave")
    assert gave_up.block_kind is None
    assert gave_up.kind_label == "missing"
    assert gave_up.eligible


def test_reservation_replay_and_board_isolation(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "one", [task("same", "capability", NOW)])
    make_board(root, "two", [task("same", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    with ControllerState.initialize(state_path, "test") as state:
        first = discover_and_reserve(root, state, now=NOW)
        second = discover_and_reserve(root, state, now=NOW)
        assert [item.action for item in first] == ["reserved", "reserved"]
        assert [item.action for item in second] == ["already_reserved", "already_reserved"]
        assert state.reservation_count() == 2
        assert state.resolution_count() == 4


def test_reservation_is_atomic_under_concurrency(tmp_path: Path) -> None:
    state_path = tmp_path / "controller.sqlite3"
    ControllerState.initialize(state_path, "test").close()

    def reserve_once() -> bool:
        with ControllerState.open_existing(state_path) as state:
            return state.reserve_blocker("alpha", "same", blocker_kind="capability", latest_event_at=NOW)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: reserve_once(), range(8)))

    assert sum(results) == 1
    with ControllerState.open_existing(state_path) as state:
        assert state.reservation_count() == 1


def test_discover_cli_prints_one_resolution_for_each_recent_candidate(
    tmp_path: Path, capsys
) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_cap", "capability", NOW), task("t_skip", "transient", NOW)],
    )
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(
        config_path,
        ControllerConfig("test", root, state_path),
    )
    ControllerState.initialize(state_path, "test").close()

    assert main(["discover", "--config", str(config_path), "--now", str(NOW)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 2
    assert "board_slug=alpha" in lines[0]
    assert "task_id=t_cap" in lines[0]
    assert "action=reserved" in lines[0]
    assert "action=skipped" in lines[1]
    assert "kind=transient" in lines[1]


def test_discovery_does_not_write_native_database(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    native = board / "kanban.db"
    before = native.read_bytes()
    before_sidecars = {path.name: path.read_bytes() for path in board.glob("kanban.db-*")}

    state_path = tmp_path / "controller.sqlite3"
    with ControllerState.initialize(state_path, "test") as state:
        discover_and_reserve(root, state, now=NOW)

    assert native.read_bytes() == before
    after_sidecars = {path.name: path.read_bytes() for path in board.glob("kanban.db-*")}
    assert after_sidecars == before_sidecars


def test_active_wal_observation_fails_closed_without_changing_native_files(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    native = board / "kanban.db"
    writer = sqlite3.connect(native)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() == "wal"
        writer.execute("INSERT INTO task_events(id, task_id, run_id, kind, payload, created_at) "
                       "VALUES (?, ?, NULL, ?, NULL, ?)", (999, "t_cap", "heartbeat", NOW))
        writer.commit()
        assert (board / "kanban.db-wal").is_file()
        assert (board / "kanban.db-shm").is_file()

        native_paths = tuple(sorted(board.glob("kanban.db*")))
        before = {
            path: (path.read_bytes(), os.stat(path, follow_symlinks=False))
            for path in native_paths
        }
        with pytest.raises(DiscoveryError, match="live WAL snapshot"):
            discover_candidates(root, now=NOW)
        after = {
            path: (path.read_bytes(), os.stat(path, follow_symlinks=False))
            for path in native_paths
        }
        assert after == before
    finally:
        writer.close()


def test_unknown_typed_kind_is_eligible_and_known_skips_are_not(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_unknown", "future_kind", NOW), task("t_dependency", "dependency", NOW)],
    )

    candidates = discover_candidates(root, now=NOW)

    unknown = next(item for item in candidates if item.task_id == "t_unknown")
    dependency = next(item for item in candidates if item.task_id == "t_dependency")
    assert unknown.kind_label == "unknown"
    assert unknown.eligible
    assert not dependency.eligible


def test_unclaimed_child_parent_done_child_todo_is_flagged(tmp_path: Path) -> None:
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

    candidates = discover_unclaimed_children(root, now=NOW)

    assert [(item.task_id, item.parent_task_id, item.parent_status) for item in candidates] == [
        ("t_child", "t_parent", "done")
    ]
    assert candidates[0].status == "todo"
    assert candidates[0].assignee == "reviewer"
    assert candidates[0].kind_label == UNCLAIMED_CHILD_KIND
    assert candidates[0].eligible


def test_unclaimed_child_parent_blocked_child_ready_is_flagged(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "blocked",
                "block_kind": "capability",
                "events": [{"id": 1, "kind": "blocked", "created_at": NOW - 10}],
            },
            child_task("t_child", "ready", NOW - 5000, assignee="reviewer"),
        ],
        links=[("t_parent", "t_child")],
    )

    candidates = discover_unclaimed_children(root, now=NOW)

    assert [(item.task_id, item.parent_task_id, item.parent_status) for item in candidates] == [
        ("t_child", "t_parent", "blocked")
    ]
    assert candidates[0].status == "ready"
    assert candidates[0].parent_block_kind == "capability"


def test_unclaimed_child_recently_active_is_not_flagged(tmp_path: Path) -> None:
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
            child_task("t_child", "todo", NOW - 100),
        ],
        links=[("t_parent", "t_child")],
    )

    assert discover_unclaimed_children(root, now=NOW) == ()


def test_unclaimed_child_threshold_boundary_strictly_after_window(tmp_path: Path) -> None:
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
            child_task("t_exact", "todo", NOW - 1800),
            child_task("t_inside", "todo", NOW - 1799),
            child_task("t_outside", "todo", NOW - 1801),
        ],
        links=[("t_parent", "t_exact"), ("t_parent", "t_inside"), ("t_parent", "t_outside")],
    )

    candidates = discover_unclaimed_children(root, now=NOW, unclaimed_after=1800)

    assert [item.task_id for item in candidates] == ["t_outside"]


def test_unclaimed_child_skips_open_parent_and_non_waiting_child(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_running",
                "status": "running",
                "events": [{"id": 1, "kind": "spawned", "created_at": NOW - 100}],
            },
            child_task("t_waiting", "todo", NOW - 5000),
            {
                "id": "t_done",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            {
                "id": "t_finished_child",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            child_task("t_blocked_child", "blocked", NOW - 5000),
        ],
        links=[
            ("t_running", "t_waiting"),
            ("t_done", "t_finished_child"),
            ("t_done", "t_blocked_child"),
        ],
    )

    assert discover_unclaimed_children(root, now=NOW) == ()


def test_unclaimed_child_open_second_parent_suppresses_alert(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_done_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            {
                "id": "t_open_parent",
                "status": "running",
                "events": [{"id": 1, "kind": "spawned", "created_at": NOW - 100}],
            },
            child_task("t_child", "todo", NOW - 5000),
        ],
        links=[("t_done_parent", "t_child"), ("t_open_parent", "t_child")],
    )

    assert discover_unclaimed_children(root, now=NOW) == ()


def test_unclaimed_child_already_claimed_is_not_flagged(tmp_path: Path) -> None:
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
            child_task("t_run", "todo", NOW - 5000, current_run_id=7),
            child_task("t_lock", "todo", NOW - 5000, claim_lock="worker", claim_expires=NOW + 1000),
            child_task("t_stale", "todo", NOW - 5000, claim_lock="worker", claim_expires=NOW - 100),
        ],
        links=[("t_parent", "t_run"), ("t_parent", "t_lock"), ("t_parent", "t_stale")],
    )

    assert discover_unclaimed_children(root, now=NOW) == ()


def test_unclaimed_child_claim_lock_with_null_expiry_is_skipped(tmp_path: Path) -> None:
    # Native semantics: a task is claimable only while claim_lock IS NULL, and
    # the native reclaim path clears lock and expiry together.  A non-null lock
    # with a null expiry is malformed/active, never guessed as expired.
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
            child_task("t_locked", "todo", NOW - 5000, claim_lock="worker"),
        ],
        links=[("t_parent", "t_locked")],
    )

    assert discover_unclaimed_children(root, now=NOW) == ()


def test_unclaimed_child_review_repro_exact_threshold_and_malformed_lock(
    tmp_path: Path,
) -> None:
    # Review repro (DEF-001 + DEF-002) end-to-end through discover_and_reserve:
    # now=10000, default window 1800.  The child whose latest event sits exactly
    # at the cutoff (8200) must NOT be flagged (unclaimed for exactly N, not >N),
    # and a child with a non-null claim_lock and null expiry must NOT be flagged
    # even though it is old.  Only the strictly-outside, lock-free child survives.
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": 9000}],
            },
            child_task("c_exact", "todo", 8200),
            child_task("c_inside", "todo", 8201),
            child_task("c_outside", "todo", 8199),
            child_task(
                "c_malformed",
                "todo",
                8000,
                claim_lock="worker",
                claim_expires=None,
            ),
        ],
        links=[
            ("t_parent", "c_exact"),
            ("t_parent", "c_inside"),
            ("t_parent", "c_outside"),
            ("t_parent", "c_malformed"),
        ],
    )
    state_path = tmp_path / "controller.sqlite3"

    with ControllerState.initialize(state_path, "test") as state:
        resolutions = discover_and_reserve(root, state, now=10_000, unclaimed_after=1800)

    flagged = [
        item.candidate.task_id for item in resolutions if item.action == "reserved"
    ]
    assert flagged == ["c_outside"]
    for item in resolutions:
        if item.action == "reserved":
            assert item.candidate.latest_event_at == 8199
            assert item.reason == "one_ever_unclaimed_child_alert"


@pytest.mark.parametrize("block_kind", ["needs_input", "transient", "dependency"])
def test_skipped_blocker_parent_stays_skipped_while_child_is_flagged(
    tmp_path: Path, block_kind: str
) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "blocked",
                "block_kind": block_kind,
                "events": [{"id": 1, "kind": "blocked", "created_at": NOW - 10}],
            },
            child_task("t_child", "todo", NOW - 5000, assignee="reviewer"),
        ],
        links=[("t_parent", "t_child")],
    )
    state_path = tmp_path / "controller.sqlite3"

    with ControllerState.initialize(state_path, "test") as state:
        resolutions = discover_and_reserve(root, state, now=NOW)

    parent = next(item for item in resolutions if item.candidate.task_id == "t_parent")
    child = next(item for item in resolutions if item.candidate.task_id == "t_child")
    assert parent.action == "skipped"
    assert parent.reason == "blocker_kind_not_recoverable"
    assert child.action == "reserved"
    assert child.reason == "one_ever_unclaimed_child_alert"
    assert "kind=unclaimed_child" in child.stdout_line()
    assert "parent_task_id=t_parent" in child.stdout_line()
    assert "parent_status=blocked" in child.stdout_line()


def test_unclaimed_child_reservation_is_one_ever(tmp_path: Path) -> None:
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
            child_task("t_child", "todo", NOW - 5000),
        ],
        links=[("t_parent", "t_child")],
    )
    state_path = tmp_path / "controller.sqlite3"

    with ControllerState.initialize(state_path, "test") as state:
        first = discover_and_reserve(root, state, now=NOW)
        second = discover_and_reserve(root, state, now=NOW)
        count = state.reservation_count()

    assert [item.action for item in first] == ["reserved"]
    assert [item.action for item in second] == ["already_reserved"]
    assert count == 1


def test_unclaimed_child_does_not_write_native_database(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            child_task("t_child", "todo", NOW - 5000),
        ],
        links=[("t_parent", "t_child")],
    )
    native = board / "kanban.db"
    before = native.read_bytes()
    before_sidecars = {path.name: path.read_bytes() for path in board.glob("kanban.db-*")}

    state_path = tmp_path / "controller.sqlite3"
    with ControllerState.initialize(state_path, "test") as state:
        discover_and_reserve(root, state, now=NOW)

    assert native.read_bytes() == before
    after_sidecars = {path.name: path.read_bytes() for path in board.glob("kanban.db-*")}
    assert after_sidecars == before_sidecars


def test_missing_task_links_table_keeps_blocked_only_behavior(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    connection = sqlite3.connect(root / "alpha" / "kanban.db")
    connection.execute("DROP TABLE task_links")
    connection.commit()
    connection.close()

    candidates = discover_unclaimed_children(root, now=NOW)
    blocked = discover_candidates(root, now=NOW)

    assert candidates == ()
    assert [(item.task_id, item.block_kind) for item in blocked] == [("t_cap", "capability")]


def test_unclaimed_child_scan_fails_closed_on_live_wal_without_changing_files(
    tmp_path: Path,
) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "alpha",
        [
            {
                "id": "t_parent",
                "status": "done",
                "events": [{"id": 1, "kind": "completed", "created_at": NOW - 100}],
            },
            child_task("t_child", "todo", NOW - 5000),
        ],
        links=[("t_parent", "t_child")],
    )
    native = board / "kanban.db"
    writer = sqlite3.connect(native)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() == "wal"
        writer.execute(
            "INSERT INTO task_events(id, task_id, run_id, kind, payload, created_at) "
            "VALUES (?, ?, NULL, ?, NULL, ?)",
            (999, "t_child", "heartbeat", NOW),
        )
        writer.commit()
        assert (board / "kanban.db-wal").is_file()
        assert (board / "kanban.db-shm").is_file()

        native_paths = tuple(sorted(board.glob("kanban.db*")))
        before = {
            path: (path.read_bytes(), os.stat(path, follow_symlinks=False))
            for path in native_paths
        }
        with pytest.raises(DiscoveryError, match="live WAL snapshot"):
            discover_unclaimed_children(root, now=NOW)
        after = {
            path: (path.read_bytes(), os.stat(path, follow_symlinks=False))
            for path in native_paths
        }
        assert after == before
    finally:
        writer.close()


def test_discover_cli_prints_unclaimed_child_and_honors_config_threshold(
    tmp_path: Path, capsys
) -> None:
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
            child_task("t_young", "todo", NOW - 50),
        ],
        links=[("t_parent", "t_child"), ("t_parent", "t_young")],
    )
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(
        config_path,
        ControllerConfig(
            "test",
            root,
            state_path,
            unclaimed_child_after_seconds=60,
        ),
    )
    ControllerState.initialize(state_path, "test").close()

    assert main(["discover", "--config", str(config_path), "--now", str(NOW)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert "task_id=t_child" in lines[0]
    assert "action=reserved" in lines[0]
    assert "kind=unclaimed_child" in lines[0]
    assert "parent_task_id=t_parent" in lines[0]
    assert "parent_status=done" in lines[0]
    assert "child_status=todo" in lines[0]
    assert "latest_event_at=5000" in lines[0]


def test_discover_candidates_window_override_and_full_backfill(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_old", "capability", NOW - 5000), task("t_recent", "capability", NOW - 10)],
    )

    default = discover_candidates(root, now=NOW)
    widened = discover_candidates(root, now=NOW, window_seconds=7200)
    full = discover_candidates(root, now=NOW, window_seconds=None)

    assert [item.task_id for item in default] == ["t_recent"]
    assert [item.task_id for item in widened] == ["t_old", "t_recent"]
    assert [item.task_id for item in full] == ["t_old", "t_recent"]


def test_default_window_boundary_is_inclusive_at_exactly_3600(tmp_path: Path) -> None:
    # Case 1 + case 3: the default window is exactly RECENCY_WINDOW_SECONDS and
    # the boundary is inclusive (latest_event_at >= now - window).  A task
    # whose latest event is exactly one second inside the window is a default
    # candidate; one exactly one second outside is not, and surfaces only as a
    # stale-blocker note.
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            task("t_just_inside", "capability", NOW - (RECENCY_WINDOW_SECONDS - 1)),
            task("t_exact_boundary", "capability", NOW - RECENCY_WINDOW_SECONDS),
            task("t_just_outside", "capability", NOW - (RECENCY_WINDOW_SECONDS + 1)),
        ],
    )

    default = discover_candidates(root, now=NOW)
    stale = discover_stale_blockers(root, now=NOW)

    assert sorted(item.task_id for item in default) == ["t_exact_boundary", "t_just_inside"]
    assert [item.task_id for item in stale] == ["t_just_outside"]


def test_stale_note_reports_exact_seconds_and_backfill_hint_at_boundary(tmp_path: Path) -> None:
    # Case 4: a task just outside the default window yields a visible note
    # naming the age and suggesting --backfill, never a silent omission.
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_just_outside", "capability", NOW - (RECENCY_WINDOW_SECONDS + 1))],
    )

    stale = discover_stale_blockers(root, now=NOW)
    note = stale_blocker_note(stale[0], now=NOW)

    assert note.startswith("note board_slug=alpha task_id=t_just_outside status=blocked")
    assert f"blocked_seconds_ago={RECENCY_WINDOW_SECONDS + 1}" in note
    assert "outside recency window, use --backfill" in note
    assert discover_candidates(root, now=NOW) == ()


def test_discover_cli_default_window_boundary_reserves_inside_and_notes_outside(
    tmp_path: Path, capsys
) -> None:
    # Cases 1 + 3 + 4 end to end: with no flag the default 3600s window still
    # applies, the just-inside task is reserved, and the just-outside task is
    # reported as a visible stale note suggesting --backfill.
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            task("t_inside", "capability", NOW - (RECENCY_WINDOW_SECONDS - 1)),
            task("t_outside", "capability", NOW - (RECENCY_WINDOW_SECONDS + 1)),
        ],
    )
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert main(["discover", "--config", str(config_path), "--now", str(NOW)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 2
    assert "task_id=t_inside" in lines[0]
    assert "action=reserved" in lines[0]
    assert lines[1].startswith("note ")
    assert "task_id=t_outside" in lines[1]
    assert "use --backfill" in lines[1]
    with ControllerState.open_existing(state_path) as state:
        assert state.reservation_count() == 1


def test_discover_cli_backfill_boundary_includes_exactly_one_second_outside(
    tmp_path: Path, capsys
) -> None:
    # Case 2: an explicit window one second wider than the default pulls in a
    # task that default discovery would have omitted as stale.
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_outside", "capability", NOW - (RECENCY_WINDOW_SECONDS + 1))],
    )
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert (
        main(
            [
                "discover",
                "--config",
                str(config_path),
                "--now",
                str(NOW),
                "--backfill",
                f"{RECENCY_WINDOW_SECONDS + 1}",
            ]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert "task_id=t_outside" in lines[0]
    assert "action=reserved" in lines[0]
    assert "note " not in lines[0]


def test_discover_stale_blockers_reports_outside_window_with_note(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [task("t_recent", "capability", NOW - 10), task("t_old", "capability", NOW - 3601)],
    )

    stale = discover_stale_blockers(root, now=NOW)
    in_window = discover_candidates(root, now=NOW)

    assert [item.task_id for item in in_window] == ["t_recent"]
    assert [item.task_id for item in stale] == ["t_old"]
    note = stale_blocker_note(stale[0], now=NOW)
    assert note.startswith("note board_slug=alpha task_id=t_old status=blocked")
    assert "blocked_seconds_ago=3601" in note
    assert "blocked 1h ago" in note
    assert "outside recency window, use --backfill" in note
    # A full backfill has no stale tasks by definition.
    assert discover_stale_blockers(root, now=NOW, window_seconds=None) == ()


def test_discover_cli_backfill_includes_older_blockers_and_prints_stale_note(
    tmp_path: Path, capsys
) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "alpha",
        [
            task("t_oldish", "capability", NOW - 4 * 3600),
            task("t_ancient", "capability", NOW - 10 * 3600),
        ],
    )
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert (
        main(
            [
                "discover",
                "--config",
                str(config_path),
                "--now",
                str(NOW),
                "--backfill",
                "5h",
            ]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 2
    assert "task_id=t_oldish" in lines[0]
    assert "action=reserved" in lines[0]
    assert "task_id=t_ancient" in lines[1]
    assert lines[1].startswith("note ")
    assert "blocked 10h ago" in lines[1]
    assert "outside recency window, use --backfill" in lines[1]


def test_discover_cli_default_emits_stale_note_without_reserving(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 3601)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert main(["discover", "--config", str(config_path), "--now", str(NOW)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert lines[0].startswith("note ")
    assert "action=" not in lines[0]
    assert "use --backfill" in lines[0]
    with ControllerState.open_existing(state_path) as state:
        assert state.reservation_count() == 0


def test_discover_cli_bare_backfill_includes_every_blocked_task(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert (
        main(
            ["discover", "--config", str(config_path), "--now", str(NOW), "--backfill"]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert "task_id=t_old" in lines[0]
    assert "action=reserved" in lines[0]
    assert "note " not in lines[0]


def test_discover_cli_since_alias_matches_backfill(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    assert (
        main(
            ["discover", "--config", str(config_path), "--now", str(NOW), "--since", "2h"]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert "task_id=t_old" in lines[0]
    assert "action=reserved" in lines[0]


def test_discover_cli_honors_config_recency_window_without_flag(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(
        config_path,
        ControllerConfig("test", root, state_path, recency_window_seconds=7200),
    )
    ControllerState.initialize(state_path, "test").close()

    assert main(["discover", "--config", str(config_path), "--now", str(NOW)]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert "task_id=t_old" in lines[0]
    assert "action=reserved" in lines[0]


def test_discover_cli_flag_wins_over_config_window(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(
        config_path,
        ControllerConfig("test", root, state_path, recency_window_seconds=7200),
    )
    ControllerState.initialize(state_path, "test").close()

    # A narrower flag window overrides the wider configured window.
    assert (
        main(
            [
                "discover",
                "--config",
                str(config_path),
                "--now",
                str(NOW),
                "--backfill",
                "1h",
            ]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()

    assert len(lines) == 1
    assert lines[0].startswith("note ")
    assert "task_id=t_old" in lines[0]
    assert "use --backfill" in lines[0]


def test_discover_cli_rejects_invalid_backfill_duration(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "discover",
                "--config",
                str(config_path),
                "--now",
                str(NOW),
                "--backfill",
                "5x",
            ]
        )
    assert excinfo.value.code == 2
    assert "invalid duration" in capsys.readouterr().err


def test_discover_cli_rejects_zero_backfill_duration(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "controller.sqlite3"
    write_config(config_path, ControllerConfig("test", root, state_path))
    ControllerState.initialize(state_path, "test").close()

    for flag, value in (("--backfill", "0"), ("--since", "0"), ("--backfill", "-5")):
        with pytest.raises(SystemExit) as excinfo:
            main(
                [
                    "discover",
                    "--config",
                    str(config_path),
                    "--now",
                    str(NOW),
                    flag,
                    value,
                ]
            )
        assert excinfo.value.code == 2, f"{flag} {value} should be rejected"
        assert "invalid" in capsys.readouterr().err, f"{flag} {value} error message"

        with pytest.raises(SystemExit) as excinfo:
            main(
                [
                    "run",
                    "--config",
                    str(config_path),
                    "--now",
                    str(NOW),
                    flag,
                    value,
                ]
            )
        assert excinfo.value.code == 2, f"run {flag} {value} should be rejected"
        assert "invalid" in capsys.readouterr().err, f"run {flag} {value} error message"


def test_backfill_duration_parsing() -> None:
    assert parse_duration("3600") == 3600
    assert parse_duration("45s") == 45
    assert parse_duration("90m") == 5400
    assert parse_duration("5h") == 18000
    assert parse_duration("2d") == 172800
    assert parse_duration("1w") == 604800
    assert parse_duration(" 5H ") == 18000
    with pytest.raises(ValueError):
        parse_duration("")
    with pytest.raises(ValueError):
        parse_duration("5x")
    with pytest.raises(ValueError):
        parse_duration("h")
    # Non-positive durations are invalid: zero and negative are rejected.
    with pytest.raises(ValueError):
        parse_duration("0")
    with pytest.raises(ValueError):
        parse_duration("0s")
    with pytest.raises(ValueError):
        parse_duration("0h")
    with pytest.raises(ValueError):
        parse_duration("-5")
    with pytest.raises(ValueError):
        parse_duration("-5h")
