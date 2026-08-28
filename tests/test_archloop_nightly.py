from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import stat
import subprocess


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "archloop-night-cron.sh"


def make_repo(root: Path, name: str) -> Path:
    repo = root / name
    (repo / ".archloop").mkdir(parents=True)
    (repo / ".archloop" / "config").write_text(
        "TEST=pytest\nLINT=ruff check\n", encoding="utf-8"
    )
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "test"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.invalid"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "init"], check=True)
    return repo


def executable(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def test_dry_run_excludes_boards_moved_to_archive_store(tmp_path: Path) -> None:
    repo_root = tmp_path / "repos"
    archived_repo = make_repo(repo_root, "archived-project")
    active_repo = make_repo(repo_root, "active-project")

    archived_dir = tmp_path / "archived" / "archived-project-123"
    archived_dir.mkdir(parents=True)
    (archived_dir / "board.json").write_text(
        json.dumps(
            {
                "slug": "archived-project",
                "archived": False,
                "default_workdir": str(archived_repo),
            }
        ),
        encoding="utf-8",
    )

    boards_json = json.dumps(
        [
            {
                "slug": "active-project",
                "archived": False,
                "default_workdir": str(active_repo),
            }
        ]
    )
    hermes = executable(
        tmp_path / "hermes",
        f"#!/bin/sh\nprintf '%s\\n' {shlex.quote(boards_json)}\n",
    )
    loop_driver = executable(tmp_path / "archloop-loop.sh", "#!/bin/sh\nexit 0\n")

    env = {
        **os.environ,
        "ARCHLOOP_REPO_ROOT": str(repo_root),
        "ARCHIVED_BOARDS_DIR": str(tmp_path / "archived"),
        "DRY_RUN": "1",
        "HERMES": str(hermes),
        "LOOP_DRIVER": str(loop_driver),
        "NIGHT_LOG": str(tmp_path / "night.log"),
    }
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "SKIP archived-project: board 'archived-project' archived (retired)" in result.stdout
    assert "SKIPPED board-archived (1): archived-project" in result.stdout
    assert "STARTED (1): active-project" in result.stdout
