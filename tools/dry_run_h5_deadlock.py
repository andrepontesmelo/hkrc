"""Dry-run H5 review-required deadlock discovery against the live boards.

Read-only: discovery never mutates; archive is only triggered through run().
Mirrors the production path: boards whose kanban.db lacks the native schema
(e.g. the empty `default` placeholder) never deliver stream events, so the
live scan is never reached for them — the tool skips them the same way.
"""
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

from hkrc.config import load_config
from hkrc.discovery import discover_boards
from hkrc.watcher import _discover_review_required_deadlocks_on_board, _open_read_only

CONFIG = os.path.expanduser("~/.hermes/hkrc/config/hkrc/config.toml")


def _has_native_schema(db_path: Path) -> bool:
    """True when the board database carries the native task_events table."""
    try:
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        try:
            row = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'task_events'"
            ).fetchone()
            return row is not None
        finally:
            connection.close()
    except sqlite3.Error:
        return False


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    config = load_config(Path(CONFIG))
    root = Path(args[0]) if args else Path(config.native_boards_root)
    now = int(time.time())
    cutoff = now - int(config.watcher.deadlock_min_age_seconds)
    for board in discover_boards(root):
        if not _has_native_schema(board.path / "kanban.db"):
            print("skipped schema-less board: %s" % board.slug)
            continue
        connection = _open_read_only(board.path / "kanban.db")
        try:
            deadlocks = _discover_review_required_deadlocks_on_board(
                connection,
                board,
                config,
                now=now,
                cutoff=cutoff,
                enforce_min_age=True,
            )
        finally:
            connection.close()
        if not deadlocks:
            print("board %s: no review-required deadlocks" % board.slug)
            continue
        for deadlock in deadlocks:
            print(
                json.dumps(
                    {
                        "board_slug": deadlock.board_slug,
                        "task_id": deadlock.task_id,
                        "title": deadlock.title,
                        "reason": deadlock.reason,
                        "blocked_event_id": deadlock.blocked_event_id,
                        "blocked_at": deadlock.blocked_at,
                        "review_child_ids": list(deadlock.review_child_ids),
                    },
                    indent=2,
                )
            )
            print(
                "  archive command: hermes kanban --board %s archive %s"
                % (deadlock.board_slug, deadlock.task_id)
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
