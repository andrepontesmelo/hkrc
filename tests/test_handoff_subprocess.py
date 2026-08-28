from __future__ import annotations

import stat
from pathlib import Path

from hkrc.cli import main
from hkrc.config import ControllerConfig, write_config
from hkrc.handoff import execute_handoff
from hkrc.state import ControllerState
from test_handoff import NOW, make_board, task


def test_handoff_uses_real_subprocess_and_returns_fake_cli_stdout(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    log_path = tmp_path / "calls.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log_path}\n"
        "printf 'fake-cli-stdout\\n'\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    config = ControllerConfig(
        "test",
        root,
        state_path,
        native_cli=str(fake_cli),
        telegram_chat_id="-1001234567890",
    )

    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(config, state, now=NOW)

    assert report.completed == 1
    assert report.failed == 0
    assert sum("fake-cli-stdout" in line for line in report.lines) == 4
    calls = log_path.read_text(encoding="utf-8").splitlines()
    assert [line.split()[3] for line in calls] == [
        "notify-subscribe",
        "comment",
        "reassign",
        "unblock",
    ]


def test_cli_run_keeps_one_shot_native_sequence_with_stream_disabled(tmp_path: Path, capsys) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "controller.sqlite3"
    config_path = tmp_path / "config.toml"
    log_path = tmp_path / "calls.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log_path}\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    config = ControllerConfig(
        "test",
        root,
        state_path,
        native_cli=str(fake_cli),
        telegram_chat_id="-1001234567890",
    )
    write_config(config_path, config)
    ControllerState.initialize(state_path, "test").close()

    assert main(["run", "--config", str(config_path), "--now", str(NOW)]) == 0

    output = capsys.readouterr().out
    assert "summary reserved=1 started=1 completed=1 failed=0 skipped=0" in output
    calls = log_path.read_text(encoding="utf-8").splitlines()
    assert [line.split()[3] for line in calls] == [
        "notify-subscribe",
        "comment",
        "reassign",
        "unblock",
    ]


def test_cli_run_honors_config_recency_window_without_flags(tmp_path: Path, capsys) -> None:
    # The Telegram fallback invokes a fixed `hkrc run --config` with no flags,
    # so the [discovery] recency_window_seconds knob is the only lever.  A
    # widened config window must include a task the 3600s default would drop.
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    state_path = tmp_path / "controller.sqlite3"
    config_path = tmp_path / "config.toml"
    log_path = tmp_path / "calls.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log_path}\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    config = ControllerConfig(
        "test",
        root,
        state_path,
        native_cli=str(fake_cli),
        telegram_chat_id="-1001234567890",
        recency_window_seconds=7200,
    )
    write_config(config_path, config)
    ControllerState.initialize(state_path, "test").close()

    assert main(["run", "--config", str(config_path), "--now", str(NOW)]) == 0

    output = capsys.readouterr().out
    assert "summary reserved=1 started=1 completed=1 failed=0 skipped=0" in output
    assert "task_id=t_old" in output
    assert "note " not in output
    calls = log_path.read_text(encoding="utf-8").splitlines()
    assert [line.split()[3] for line in calls] == [
        "notify-subscribe",
        "comment",
        "reassign",
        "unblock",
    ]


def test_cli_run_default_notes_stale_task_without_any_native_calls(
    tmp_path: Path, capsys
) -> None:
    # Case 4 on the Telegram-fallback path: with the default 3600s window a
    # task older than the window produces a visible note suggesting --backfill
    # and zero native CLI calls (never reserved, never silently dropped).
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_old", "capability", NOW - 5000)])
    state_path = tmp_path / "controller.sqlite3"
    config_path = tmp_path / "config.toml"
    log_path = tmp_path / "calls.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log_path}\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    config = ControllerConfig(
        "test",
        root,
        state_path,
        native_cli=str(fake_cli),
        telegram_chat_id="-1001234567890",
    )
    write_config(config_path, config)
    ControllerState.initialize(state_path, "test").close()

    assert main(["run", "--config", str(config_path), "--now", str(NOW)]) == 0

    output = capsys.readouterr().out
    assert "task_id=t_old" in output
    assert output.strip().startswith("note ")
    assert "use --backfill" in output
    assert "summary reserved=0 started=0 completed=0 failed=0 skipped=0" in output
    assert not log_path.exists() or log_path.read_text(encoding="utf-8") == ""
