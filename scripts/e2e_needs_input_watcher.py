#!/usr/bin/env python3
"""End-to-end exercise of needs-input-watcher v2 through the installed wrapper.

Builds a temp instance root, installs the current repo release into it,
creates a synthetic native board with one needs_input blocked episode,
and runs `hkrc needs-input-watcher` twice: once with llm_profile empty
(deterministic fallback line) and once with a fake `hermes` on PATH that
echoes the prompt (LLM summarizer path, real subprocess/env/argv).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "hkrc_release.py"
NOW = 100_000
_VERSION_MATCH = re.search(
    r'__version__ = "([^"]+)"',
    (ROOT / "src" / "hkrc" / "__init__.py").read_text(encoding="utf-8"),
)
assert _VERSION_MATCH is not None
VERSION = _VERSION_MATCH.group(1)


def make_board(root: Path, slug: str) -> None:
    board = root / slug
    board.mkdir(parents=True)
    (board / "board.json").write_text(json.dumps({"slug": slug}), encoding="utf-8")
    connection = sqlite3.connect(board / "kanban.db")
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            block_kind TEXT
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            run_id INTEGER,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at INTEGER NOT NULL
        );
        """
    )
    connection.execute(
        "INSERT INTO tasks(id, title, status, block_kind) VALUES ('t_e2e', 'task: e2e', 'blocked', 'needs_input')"
    )
    connection.execute(
        "INSERT INTO task_events(id, task_id, kind, payload, created_at) VALUES (1, 't_e2e', 'blocked', ?, ?)",
        (json.dumps({"reason": "needs Andre's decision", "kind": "needs_input"}), NOW - 600),
    )
    connection.commit()
    connection.close()


def run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False, env=env, cwd=cwd)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hkrc-e2e-") as raw:
        tmp = Path(raw)
        instance = tmp / "instance"
        boards = tmp / "boards"
        make_board(boards, "campcli")

        install = run([sys.executable, str(RELEASE), "install", "--source-root", str(ROOT), "--instance-root", str(instance)])
        assert install.returncode == 0, install.stderr
        wrapper = instance / "bin" / "hkrc"

        # Deterministic path: no llm_profile.
        config_dir = instance / "config" / "hkrc"
        # The release installer seeds needs-input-watcher-prompt.txt here, so the
        # directory already exists on a fresh install.
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "config.toml"
        config_path.write_text(
            f"""
[instance]
name = "e2e"
native_boards_root = "{boards}"

[controller]
state_db = "{instance}/state/hkrc/state.sqlite3"
""",
            encoding="utf-8",
        )
        state_file = instance / "state" / "hkrc" / "needs-input-watcher-state.json"
        env = dict(os.environ)
        result = run(
            [str(wrapper), "needs-input-watcher", "--config", str(config_path), "--state-file", str(state_file), "--now", str(NOW)],
            env=env,
        )
        print("=== deterministic run ===")
        print("returncode:", result.returncode)
        print("stdout:", result.stdout.strip())
        print("stderr:", result.stderr.strip())
        assert result.returncode == 0, result.stderr
        assert "needs-input-watcher fallback board=campcli task=t_e2e kind=needs_input age=600 episode=campcli:t_e2e:1" in result.stdout
        state = json.loads(state_file.read_text(encoding="utf-8"))
        assert state == {"campcli:t_e2e:1": 1}, state
        # Second run: silent (episode consumed).
        second = run(
            [str(wrapper), "needs-input-watcher", "--config", str(config_path), "--state-file", str(state_file), "--now", str(NOW)],
            env=env,
        )
        assert second.returncode == 0 and second.stdout.strip() == "", second.stdout
        print("second run silent: OK")

        # LLM path: fake hermes on PATH that echoes argv back.
        fake_bin = tmp / "bin"
        fake_bin.mkdir()
        fake_hermes = fake_bin / "hermes"
        fake_hermes.write_text(
            "#!/bin/sh\n"
            "echo \"FAKE_SUMMARY argv=$*\"\n",
            encoding="utf-8",
        )
        fake_hermes.chmod(0o755)
        config_path.write_text(
            f"""
[instance]
name = "e2e"
native_boards_root = "{boards}"

[controller]
state_db = "{instance}/state/hkrc/state.sqlite3"

[needs_input_watcher]
llm_profile = "summarizer"
""",
            encoding="utf-8",
        )
        llm_state = tmp / "llm-state.json"
        env = dict(os.environ)
        env["PATH"] = str(fake_bin) + os.pathsep + env.get("PATH", "")
        env["HOME"] = str(tmp / "home")
        env.pop("_HERMES_GATEWAY", None)
        result = run(
            [str(wrapper), "needs-input-watcher", "--config", str(config_path), "--state-file", str(llm_state), "--now", str(NOW)],
            env=env,
        )
        print("=== llm run ===")
        print("returncode:", result.returncode)
        print("stdout:", result.stdout.strip())
        print("stderr:", result.stderr.strip())
        assert result.returncode == 0, result.stderr
        assert "needs Andre's decision" in result.stdout
        assert "FAKE_SUMMARY" in result.stdout
        assert "t_e2e" in result.stdout
        print("llm path: OK")

        # Version report through the wrapper.
        version = run([str(wrapper), "--version"], env=env)
        print("=== version ===")
        print(version.stdout.strip())
        assert version.stdout.strip() == VERSION, version.stdout

    print("E2E OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
