"""Real-CLI E2E for the backfill/stale-note feature (scratch board only)."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NOW = int(time.time())

def make_board(root: Path) -> Path:
    board = root / "alpha"
    board.mkdir(parents=True)
    (board / "board.json").write_text(json.dumps({"slug": "alpha"}), encoding="utf-8")
    connection = sqlite3.connect(board / "kanban.db")
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
            block_kind TEXT, assignee TEXT, created_at INTEGER,
            claim_lock TEXT, claim_expires INTEGER, current_run_id INTEGER
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id INTEGER,
            kind TEXT NOT NULL, payload TEXT, created_at INTEGER NOT NULL
        );
        CREATE TABLE task_links (
            parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
            PRIMARY KEY (parent_id, child_id)
        );
        """
    )
    rows = [
        ("t_recent", "capability", NOW - 60),
        ("t_stale_5h", "capability", NOW - 5 * 3600),
        ("t_ancient", "gave_up", NOW - 3 * 86400),
    ]
    for number, (task_id, kind, created_at) in enumerate(rows, 1):
        connection.execute(
            "INSERT INTO tasks(id, title, status, block_kind, assignee, created_at) "
            "VALUES (?, ?, 'blocked', ?, NULL, ?)",
            (task_id, task_id, kind, created_at),
        )
        connection.execute(
            "INSERT INTO task_events(id, task_id, run_id, kind, payload, created_at) "
            "VALUES (?, ?, NULL, 'blocked', '{}', ?)",
            (number * 100 + 1, task_id, created_at),
        )
    connection.commit()
    connection.close()
    return board

def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hkrc-backfill-e2e-") as tmp:
        root = Path(tmp)
        boards = root / "boards"
        board = make_board(boards)
        native = board / "kanban.db"
        native_before = native.read_bytes()
        sidecars_before = {p.name: p.read_bytes() for p in board.glob("kanban.db-*")}

        config = root / "config.toml"
        state = root / "controller.sqlite3"
        fake_cli = root / "fake-hermes"
        fake_cli.write_text(
            "#!/bin/sh\n"
            'echo "fake-native args: $*"\n'
            "exit 0\n",
            encoding="utf-8",
        )
        fake_cli.chmod(0o755)

        init = subprocess.run(
            [
                "uv", "run", "hkrc", "init",
                "--config", str(config),
                "--instance-name", "e2e",
                "--native-boards-root", str(boards),
                "--state-db", str(state),
                "--native-cli", str(fake_cli),
                "--telegram-chat-id", "e2e-chat",
                "--force",
            ],
            cwd=REPO,
            text=True,
            capture_output=True,
            check=False,
        )
        if init.returncode != 0:
            print("init failed:", init.stderr, file=sys.stderr)
            return 1

        def run(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                ["uv", "run", "hkrc", *args, "--config", str(config), "--now", str(NOW)],
                cwd=REPO,
                text=True,
                capture_output=True,
                check=False,
            )

        print("=== default discover (no flag) ===")
        result = run("discover")
        print(result.stdout, end="")
        if result.returncode != 0:
            print("STDERR:", result.stderr, file=sys.stderr)
            return 1

        print("=== discover --backfill 5h ===")
        result = run("discover", "--backfill", "5h")
        print(result.stdout, end="")
        if result.returncode != 0:
            print("STDERR:", result.stderr, file=sys.stderr)
            return 1

        print("=== discover bare --backfill (every blocked task) ===")
        result = run("discover", "--backfill")
        print(result.stdout, end="")
        if result.returncode != 0:
            print("STDERR:", result.stderr, file=sys.stderr)
            return 1

        print("=== run --backfill 5h (fake native CLI, all in window now reserved) ===")
        result = run("run", "--backfill", "5h")
        print(result.stdout, end="")
        if result.returncode != 0:
            print("STDERR:", result.stderr, file=sys.stderr)
            return 1

        print("=== read-only boundary check ===")
        native_after = native.read_bytes()
        sidecars_after = {p.name: p.read_bytes() for p in board.glob("kanban.db-*")}
        print(f"native db unchanged: {native_before == native_after}")
        print(f"native sidecars unchanged: {sidecars_before == sidecars_after}")
        if native_before != native_after or sidecars_before != sidecars_after:
            return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
