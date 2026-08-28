from __future__ import annotations

from pathlib import Path
import stat
import subprocess
import sys

from hkrc.config import ControllerConfig, default_config, load_config
from hkrc.handoff import NativeResult, execute_handoff, HandoffError
from hkrc.state import ControllerState
from test_handoff import NOW, make_board, task


ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "hkrc_release.py"


def release(action: str, root: Path, source: Path = ROOT, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(RELEASE), action, "--source-root", str(source), "--instance-root", str(root), *extra],
        text=True,
        capture_output=True,
        check=False,
    )


def copy_source(source: Path, *, version: str | None = None) -> None:
    source.mkdir()
    for relative in ("src", "skills", "systemd", "config", "docs"):
        subprocess.run(["cp", "-a", str(ROOT / relative), str(source / relative)], check=True)
    if version is not None:
        (source / "src" / "hkrc" / "__init__.py").write_text(
            f'__version__ = "{version}"\n', encoding="utf-8"
        )


def test_install_upgrade_rollback_and_instance_isolation(tmp_path: Path) -> None:
    source = tmp_path / "source"
    copy_source(source, version="1.0.0")

    one = tmp_path / "one"
    two = tmp_path / "two"
    assert release("install", one, source).returncode == 0
    assert release("install", two, source).returncode == 0
    assert (one / "current").is_symlink()
    assert (two / "current").is_symlink()
    assert (one / "bin" / "hkrc").stat().st_mode & stat.S_IXUSR
    assert (one / "skills" / "blocker-recovery" / "SKILL.md").is_file()
    # The versioned needs-input-watcher prompt template ships in the release and is
    # seeded into the instance config directory.
    assert (one / "releases" / "1.0.0" / "config" / "hkrc" / "needs-input-watcher-prompt.txt").is_file()
    seeded = one / "config" / "hkrc" / "needs-input-watcher-prompt.txt"
    assert seeded.is_file()
    assert "--board {board_slug}" in seeded.read_text(encoding="utf-8")
    # The cron manifest ships in the release and is seeded for `hkrc crons sync`.
    assert (one / "releases" / "1.0.0" / "config" / "hkrc" / "cron_manifest.json").is_file()
    manifest = one / "config" / "hkrc" / "cron_manifest.json"
    assert manifest.is_file()
    assert "kanban review gap watchdog" in manifest.read_text(encoding="utf-8")

    (source / "src" / "hkrc" / "__init__.py").write_text('__version__ = "2.0.0"\n', encoding="utf-8")
    assert release("upgrade", one, source, "--version", "2.0.0").returncode == 0
    assert (one / "current").resolve().name == "2.0.0"
    assert (one / "previous").resolve().name == "1.0.0"
    assert (two / "current").resolve().name == "1.0.0"
    assert release("rollback", one, source).returncode == 0
    assert (one / "current").resolve().name == "1.0.0"
    assert (one / "previous").resolve().name == "2.0.0"


def test_two_instance_default_paths_and_state_are_isolated(tmp_path: Path, monkeypatch) -> None:
    roots = [tmp_path / "hermes-a", tmp_path / "hermes-b"]
    configs = []
    for index, root in enumerate(roots, 1):
        monkeypatch.setenv("HKRC_INSTANCE_ROOT", str(root))
        config = default_config()
        assert config.native_boards_root == root / "kanban" / "boards"
        assert config.state_db == root / "state" / "hkrc" / "state.sqlite3"
        configs.append(config)
        ControllerState.initialize(config.state_db, f"instance-{index}").close()
    assert configs[0].state_db != configs[1].state_db


def test_env_destination_is_not_written_as_a_secret(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "state.sqlite3"
    config = ControllerConfig(
        "test", root, state_path, telegram_chat_id_env="HKRC_CHAT_ID", telegram_chat_id=""
    )
    text = config.as_toml()
    assert "HKRC_CHAT_ID" in text
    assert "-100-secret-looking" not in text
    monkeypatch.setenv("HKRC_CHAT_ID", "chat-from-environment")
    calls: list[list[str]] = []
    with ControllerState.initialize(state_path, "test") as state:
        report = execute_handoff(config, state, now=NOW, runner=lambda command: calls.append(list(command)) or NativeResult(0))
    assert report.completed == 1
    assert "chat-from-environment" in calls[0]


def test_missing_destination_and_lead_orchestrator_are_real_failures(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "alpha", [task("t_cap", "capability", NOW)])
    state_path = tmp_path / "state.sqlite3"
    config = ControllerConfig("test", root, state_path)
    with ControllerState.initialize(state_path, "test") as state:
        try:
            execute_handoff(config, state, now=NOW, runner=lambda _: NativeResult(0))
        except HandoffError as exc:
            assert "telegram destination" in str(exc)
        else:
            raise AssertionError("missing destination must fail")
        assert state.reservation_count() == 0

    config = ControllerConfig("test", root, state_path, telegram_chat_id="chat")
    with ControllerState.initialize(state_path, "test") as state:
        def failing_lead(command: list[str]) -> NativeResult:
            if "reassign" in command:
                return NativeResult(1, stderr="assignee not found: lead-orchestrator")
            return NativeResult(0)

        report = execute_handoff(config, state, now=NOW, runner=failing_lead)
        assert report.failed == 1
        assert any("assignee not found" in line for line in report.lines)
        intervention = state.get_intervention("alpha", "t_cap")
        assert intervention is not None
        assert intervention["phase"] == "reassign"


def test_installed_wrapper_uses_instance_root_and_config(tmp_path: Path) -> None:
    instance = tmp_path / "instance"
    assert release("install", instance).returncode == 0
    result = subprocess.run([str(instance / "bin" / "hkrc"), "init"], text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    config_path = instance / "config" / "hkrc" / "config.toml"
    assert config_path.is_file()
    config = load_config(config_path)
    assert config.state_db == instance / "state" / "hkrc" / "state.sqlite3"
    assert config.workspace == instance / "workspace" / "hkrc"
    assert config.native_boards_root == instance / "kanban" / "boards"
    assert "chat_id_env" in config_path.read_text(encoding="utf-8")


def test_install_generates_opt_in_instance_scoped_systemd_unit(tmp_path: Path) -> None:
    instance = tmp_path / "instance with spaces"
    result = release("install", instance)

    assert result.returncode == 0, result.stderr
    unit = instance / "systemd" / "hkrc.service"
    text = unit.read_text(encoding="utf-8")
    assert "Type=simple" in text
    assert f'Environment="HKRC_INSTANCE_ROOT={instance}"' in text
    assert f'ExecStart="{instance / "bin" / "hkrc"}" daemon --config "{instance / "config" / "hkrc" / "config.toml"}"' in text
    assert 'Environment="PYTHONUNBUFFERED=1"' in text
    escaped_instance = str(instance).replace(" ", r"\x20")
    assert f"WorkingDirectory={escaped_instance}" in text
    assert "SyslogIdentifier=hkrc" in text
    assert "Restart=on-failure" in text
    assert "RestartSec=5s" in text
    assert "KillSignal=SIGTERM" in text
    assert "KillMode=mixed" in text
    assert "TimeoutStopSec=150s" in text
    assert "StandardOutput=journal" in text
    assert "StandardError=journal" in text
    assert "ProtectSystem=full" in text
    assert f"ReadWritePaths={escaped_instance}" in text
    assert "PrivateTmp=true" in text
    assert "NoNewPrivileges=true" in text
    assert "ProtectKernelTunables=true" in text
    assert "ProtectKernelModules=true" in text
    assert "ProtectControlGroups=true" in text
    assert "RestrictSUIDSGID=true" in text
    assert "RestrictRealtime=true" in text
    assert "LockPersonality=true" in text
    assert "RestrictNamespaces=true" in text
    assert "SystemCallArchitectures=native" in text
    assert "ProtectHome=" not in text
    assert "ReadOnlyPaths=" not in text
    assert "systemctl" not in text
    assert "stream connector or credentials" in text
    assert "fails closed" in text
    assert "native DB" in text
    assert "watch/tail" in text
    assert "daemon" in text and " run " not in text


def test_release_paths_preserve_instance_config_state_workspace_lock_and_unit(tmp_path: Path) -> None:
    instance = tmp_path / "instance"
    assert release("install", instance).returncode == 0
    wrapper = instance / "bin" / "hkrc"
    assert subprocess.run([str(wrapper), "init"], text=True, capture_output=True).returncode == 0

    config_path = instance / "config" / "hkrc" / "config.toml"
    state_path = instance / "state" / "hkrc" / "state.sqlite3"
    workspace = instance / "workspace" / "hkrc"
    lock_path = instance / "state" / "hkrc" / "controller.lock"
    config_before = config_path.read_bytes()
    state_before = state_path.read_bytes()
    workspace_marker = workspace / "operator-owned.txt"
    workspace_marker.write_text("keep", encoding="utf-8")
    lock_path.write_text("stale lock file is harmless", encoding="utf-8")
    unit_before = (instance / "systemd" / "hkrc.service").read_bytes()
    prompt_path = instance / "config" / "hkrc" / "needs-input-watcher-prompt.txt"
    assert prompt_path.is_file()
    # An operator-customized prompt template must survive upgrade and
    # rollback (the seed only fills missing files, never overwrites).
    prompt_path.write_text("customized local prompt\n", encoding="utf-8")

    source = tmp_path / "source"
    copy_source(source, version="2.0.0")
    assert release("upgrade", instance, source, "--version", "2.0.0").returncode == 0

    assert config_path.read_bytes() == config_before
    assert state_path.read_bytes() == state_before
    assert workspace_marker.read_text(encoding="utf-8") == "keep"
    assert lock_path.read_text(encoding="utf-8") == "stale lock file is harmless"
    assert (instance / "systemd" / "hkrc.service").read_bytes() == unit_before
    assert prompt_path.read_text(encoding="utf-8") == "customized local prompt\n"

    assert release("rollback", instance).returncode == 0
    assert config_path.read_bytes() == config_before
    assert state_path.read_bytes() == state_before
    assert workspace_marker.read_text(encoding="utf-8") == "keep"
    assert lock_path.read_text(encoding="utf-8") == "stale lock file is harmless"
    assert (instance / "systemd" / "hkrc.service").read_bytes() == unit_before
    assert prompt_path.read_text(encoding="utf-8") == "customized local prompt\n"


def test_unit_action_is_reproducible_and_does_not_install_service(tmp_path: Path) -> None:
    instance = tmp_path / "instance"
    assert release("unit", instance).returncode == 0
    first = (instance / "systemd" / "hkrc.service").read_bytes()
    assert release("unit", instance).returncode == 0
    assert (instance / "systemd" / "hkrc.service").read_bytes() == first
    assert not (instance / "current").exists()


def test_upgrade_runs_cron_sync_preview_without_mutating(tmp_path: Path, monkeypatch) -> None:
    instance = tmp_path / "instance"
    assert release("install", instance).returncode == 0
    wrapper = instance / "bin" / "hkrc"
    assert subprocess.run([str(wrapper), "init"], text=True, capture_output=True).returncode == 0

    # Point the sync preview at a throwaway cron home so it can never see or
    # touch the operator's real Hermes cron store.
    cron_home = tmp_path / "cronhome"
    monkeypatch.setenv("HOME", str(cron_home))
    monkeypatch.delenv("HERMES_HOME", raising=False)

    source = tmp_path / "source"
    copy_source(source, version="2.0.0")
    result = release("upgrade", instance, source, "--version", "2.0.0")
    assert result.returncode == 0, result.stderr

    # The final deploy step prints the dry-run diff for operator review ...
    assert "cron reconciliation preview" in result.stdout
    assert "crons sync: create" in result.stdout
    # ... and never writes anything to the cron store.
    assert not (cron_home / ".hermes" / "cron" / "jobs.json").exists()
    assert (instance / "config" / "hkrc" / "cron_manifest.json").is_file()
