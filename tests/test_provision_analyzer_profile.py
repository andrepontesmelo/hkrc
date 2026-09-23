"""Provisioner + release-payload tests (kanban t_1e29fe8d).

The provisioner (scripts/provision_analyzer_profile.py) repairs LIVE runtime
state under ``<hermes home>/profiles`` that the installer contract never
touches. These tests pin its behavior on fixture instances/profiles: fresh
seed, idempotence, --force, fail-closed YAML editing, --check immutability,
guard-verdict propagation, and the hkrc_release.py instance-root seeding.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from test_release import ROOT, copy_source, release

PROVISIONER = ROOT / "scripts" / "provision_analyzer_profile.py"
GUARD = ROOT / "scripts" / "e2e_analysis_profile_readonly.py"
PAYLOAD = ROOT / "config" / "hkrc" / "analyzer-readonly-plugin"
LIVE_PLUGIN = Path.home() / ".hermes" / "profiles" / "authoritative" / "plugins" / "readonly-file"


def load_provisioner():
    spec = importlib.util.spec_from_file_location("provision_analyzer_profile", PROVISIONER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_config(path: Path, analysis_profile: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "[instance]\n"
        'name = "default"\n'
        f'native_boards_root = "{path.parent}"\n'
        "[controller]\n"
        f'state_db = "{path.parent / "state.db"}"\n'
        "[harness_loop]\n"
        f'analysis_profile = "{analysis_profile}"\n',
        encoding="utf-8",
    )
    return path


def make_instance(tmp_path: Path) -> Path:
    """Instance root carrying the seeded release payload."""
    instance = tmp_path / "instance"
    payload = instance / "config" / "hkrc" / "analyzer-readonly-plugin"
    payload.mkdir(parents=True)
    for name in ("plugin.yaml", "__init__.py"):
        shutil.copy2(PAYLOAD / name, payload / name)
    return instance


def make_profile_home(tmp_path: Path, config_yaml: str, *, with_plugin: bool) -> Path:
    home = tmp_path / "hermes-home"
    profile_dir = home / "profiles" / "authoritative"
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "config.yaml").write_text(config_yaml, encoding="utf-8")
    if with_plugin:
        plugin = profile_dir / "plugins" / "readonly-file"
        plugin.mkdir(parents=True, exist_ok=True)
        for name in ("plugin.yaml", "__init__.py"):
            shutil.copy2(PAYLOAD / name, plugin / name)
    return home


FULL_CONFIG = (
    "# profile config\n"
    "model:\n"
    "  provider: test\n"
    "platform_toolsets:\n"
    "  cli:\n"
    "    - kanban\n"
    "    - readonly_file\n"
    "  telegram:\n"
    "    - browser\n"
    "plugins:\n"
    "  enabled:\n"
    "    - readonly-file\n"
    "session_reset:\n"
    "  mode: both\n"
)
MISSING_LINES_CONFIG = (
    "# profile config\n"
    "model:\n"
    "  provider: test\n"
    "platform_toolsets:\n"
    "  cli:\n"
    "    - kanban\n"
    "  telegram:\n"
    "    - browser\n"
    "plugins:\n"
    "  enabled:\n"
    "session_reset:\n"
    "  mode: both\n"
)


@pytest.fixture
def env(tmp_path, monkeypatch):
    instance = make_instance(tmp_path)
    config = write_config(tmp_path / "config.toml", "authoritative")
    monkeypatch.setenv("HKRC_INSTANCE_ROOT", str(instance))
    return tmp_path, instance, config


def prepare(
    tmp_path: Path, monkeypatch, config_yaml: str, *, with_plugin: bool, guard_result: int = 0
):
    module = load_provisioner()
    home = make_profile_home(tmp_path, config_yaml, with_plugin=with_plugin)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HOME", str(tmp_path / "shell-home"))
    (tmp_path / "shell-home").mkdir(exist_ok=True)
    calls: list[Path] = []

    def stub_guard(config_path: Path) -> int:
        calls.append(config_path)
        return guard_result

    monkeypatch.setattr(module, "run_guard", stub_guard)
    return module, home / "profiles" / "authoritative", calls


# --- payload fidelity ----------------------------------------------------


def test_payload_matches_live_authoritative_plugin() -> None:
    if not (LIVE_PLUGIN / "plugin.yaml").is_file():
        pytest.skip("live authoritative profile plugin not present on this host")
    for name in ("plugin.yaml", "__init__.py"):
        assert (PAYLOAD / name).read_bytes() == (LIVE_PLUGIN / name).read_bytes(), name


def test_payload_dir_carries_exactly_the_two_files() -> None:
    names = sorted(path.name for path in PAYLOAD.iterdir())
    assert names == ["__init__.py", "plugin.yaml"]


def test_payload_plugin_is_read_only() -> None:
    import ast

    tree = ast.parse((PAYLOAD / "__init__.py").read_text(encoding="utf-8"))
    literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert "readonly_file" in literals
    assert {"read_file", "search_files"} <= literals
    assert "patch" not in literals and "write_file" not in literals
    assert "name: readonly-file" in (PAYLOAD / "plugin.yaml").read_text(encoding="utf-8")


# --- seeding -------------------------------------------------------------


def test_fresh_seed_and_missing_line_insertion(env, tmp_path, monkeypatch) -> None:
    module, profile_dir, guard_calls = prepare(
        tmp_path, monkeypatch, MISSING_LINES_CONFIG, with_plugin=False
    )
    _, _, config = env
    assert module.main(["--config", str(config)]) == 0

    plugin = profile_dir / "plugins" / "readonly-file"
    for name in ("plugin.yaml", "__init__.py"):
        assert (plugin / name).read_bytes() == (PAYLOAD / name).read_bytes(), name

    text = (profile_dir / "config.yaml").read_text(encoding="utf-8")
    assert "    - readonly_file\n" in text
    assert "    - readonly-file\n" in text
    # byte-exact preservation: removing the two inserted lines restores the file
    restored = text.replace("    - readonly_file\n", "").replace("    - readonly-file\n", "", 1)
    assert restored == MISSING_LINES_CONFIG
    # a timestamped backup was written before the first edit
    backups = list(profile_dir.glob("config.yaml.bak-*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == MISSING_LINES_CONFIG
    assert guard_calls == [config]


def test_second_run_is_idempotent(env, tmp_path, monkeypatch) -> None:
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, MISSING_LINES_CONFIG, with_plugin=False)
    _, _, config = env
    assert module.main(["--config", str(config)]) == 0
    after_first = (profile_dir / "config.yaml").read_text(encoding="utf-8")
    backups_after_first = list(profile_dir.glob("config.yaml.bak-*"))

    assert module.main(["--config", str(config)]) == 0
    assert (profile_dir / "config.yaml").read_text(encoding="utf-8") == after_first
    # no second backup: nothing was edited
    assert list(profile_dir.glob("config.yaml.bak-*")) == backups_after_first


def test_existing_plugin_dir_skipped_without_force(env, tmp_path, monkeypatch) -> None:
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, FULL_CONFIG, with_plugin=True)
    _, _, config = env
    sentinel = profile_dir / "plugins" / "readonly-file" / "sentinel.txt"
    sentinel.write_text("operator customization", encoding="utf-8")

    assert module.main(["--config", str(config)]) == 0
    assert sentinel.read_text(encoding="utf-8") == "operator customization"
    assert (profile_dir / "config.yaml").read_text(encoding="utf-8") == FULL_CONFIG


def test_force_reseeds_plugin_dir(env, tmp_path, monkeypatch) -> None:
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, FULL_CONFIG, with_plugin=True)
    _, _, config = env
    plugin = profile_dir / "plugins" / "readonly-file"
    (plugin / "sentinel.txt").write_text("stale", encoding="utf-8")

    assert module.main(["--config", str(config), "--force"]) == 0
    assert not (plugin / "sentinel.txt").exists()
    for name in ("plugin.yaml", "__init__.py"):
        assert (plugin / name).read_bytes() == (PAYLOAD / name).read_bytes(), name


# --- fail-closed config editing ------------------------------------------


@pytest.mark.parametrize(
    "yaml_text",
    [
        # flow style parent
        "platform_toolsets: {cli: [kanban]}\nplugins:\n  enabled:\n    - readonly-file\n",
        # child section missing
        "platform_toolsets:\n  telegram:\n    - browser\nplugins:\n  enabled:\n    - readonly-file\n",
        # duplicate parent section
        "platform_toolsets:\n  cli:\n    - kanban\nplatform_toolsets:\n  cli:\n    - kanban\n"
        "plugins:\n  enabled:\n    - readonly-file\n",
        # non-list entry under cli
        "platform_toolsets:\n  cli:\n    entry: not-a-list\nplugins:\n  enabled:\n    - readonly-file\n",
        # valued (flow) child header
        "platform_toolsets:\n  cli: [kanban]\nplugins:\n  enabled:\n    - readonly-file\n",
        # plugins.enabled missing
        "platform_toolsets:\n  cli:\n    - kanban\nplugins:\n  other:\n    - x\n",
    ],
)
def test_refuses_unexpected_yaml_structure(env, tmp_path, monkeypatch, yaml_text) -> None:
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, yaml_text, with_plugin=True)
    _, _, config = env
    assert module.main(["--config", str(config)]) == 1
    # config.yaml untouched and no backup written
    assert (profile_dir / "config.yaml").read_text(encoding="utf-8") == yaml_text
    assert not list(profile_dir.glob("config.yaml.bak-*"))


def test_insertion_keeps_sibling_sections_intact(env, tmp_path, monkeypatch) -> None:
    config_yaml = (
        "platform_toolsets:\n"
        "  cli:\n"
        "    - kanban\n"
        "  telegram:\n"
        "    - browser\n"
        "plugins:\n"
        "  enabled:\n"
        "    - other-plugin\n"
    )
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, config_yaml, with_plugin=True)
    _, _, config = env
    assert module.main(["--config", str(config)]) == 0
    assert (profile_dir / "config.yaml").read_text(encoding="utf-8") == (
        "platform_toolsets:\n"
        "  cli:\n"
        "    - kanban\n"
        "    - readonly_file\n"
        "  telegram:\n"
        "    - browser\n"
        "plugins:\n"
        "  enabled:\n"
        "    - other-plugin\n"
        "    - readonly-file\n"
    )


# --- --check --------------------------------------------------------------


def test_check_reports_and_changes_nothing(env, tmp_path, monkeypatch, capsys) -> None:
    module, profile_dir, guard_calls = prepare(
        tmp_path, monkeypatch, MISSING_LINES_CONFIG, with_plugin=False
    )
    _, _, config = env
    assert module.main(["--config", str(config), "--check"]) == 1
    out = capsys.readouterr().out
    assert "CHECK: FAIL" in out
    assert "readonly_file" in out and "readonly-file" in out
    # nothing changed: no plugin dir, no backup, config untouched, guard not run
    assert not (profile_dir / "plugins" / "readonly-file").exists()
    assert (profile_dir / "config.yaml").read_text(encoding="utf-8") == MISSING_LINES_CONFIG
    assert not list(profile_dir.glob("config.yaml.bak-*"))
    assert guard_calls == []

    # healthy state passes
    module2, _, _ = prepare(tmp_path, monkeypatch, FULL_CONFIG, with_plugin=True)
    assert module2.main(["--config", str(config), "--check"]) == 0
    assert "CHECK: PASS" in capsys.readouterr().out


# --- guard propagation ----------------------------------------------------


def test_provisioner_fails_when_guard_fails(env, tmp_path, monkeypatch) -> None:
    module, _, guard_calls = prepare(
        tmp_path, monkeypatch, MISSING_LINES_CONFIG, with_plugin=False, guard_result=1
    )
    _, _, config = env
    assert module.main(["--config", str(config)]) == 1
    assert guard_calls == [config]


def test_disabled_analysis_stage_passes(tmp_path, monkeypatch, capsys) -> None:
    module = load_provisioner()
    config = write_config(tmp_path / "config.toml", "")
    assert module.main(["--config", str(config)]) == 0
    assert "stage is disabled" in capsys.readouterr().out


def test_missing_payload_fails_closed(env, tmp_path, monkeypatch) -> None:
    module, profile_dir, _ = prepare(tmp_path, monkeypatch, FULL_CONFIG, with_plugin=False)
    monkeypatch.setattr(module, "script_repo_root", lambda: tmp_path / "elsewhere")
    shutil.rmtree(tmp_path / "instance" / "config" / "hkrc" / "analyzer-readonly-plugin")
    _, _, config = env
    assert module.main(["--config", str(config)]) == 1
    assert not (profile_dir / "plugins" / "readonly-file").exists()


def test_end_to_end_guard_failure_fails_provisioner(tmp_path, monkeypatch) -> None:
    """Real guard integration: a builtin 'file' toolset survives provisioning,
    so the committed guard (not a stub) fails and so does the provisioner."""
    instance = make_instance(tmp_path)
    config = write_config(tmp_path / "config.toml", "authoritative")
    monkeypatch.setenv("HKRC_INSTANCE_ROOT", str(instance))
    config_yaml = (
        "platform_toolsets:\n"
        "  cli:\n"
        "    - kanban\n"
        "    - file\n"
        "plugins:\n"
        "  enabled:\n"
        "    - readonly-file\n"
    )
    home = make_profile_home(tmp_path, config_yaml, with_plugin=False)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HOME", str(tmp_path / "shell-home"))
    (tmp_path / "shell-home").mkdir(exist_ok=True)
    assert GUARD.is_file()

    completed = subprocess.run(
        [sys.executable, str(PROVISIONER), "--config", str(config)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode != 0
    assert "guard FAILED" in completed.stdout


# --- hkrc_release.py instance-root seeding --------------------------------


def test_release_seeds_analyzer_payload_into_instance_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    copy_source(source, version="1.0.0")
    (source / "config" / "hkrc" / "analyzer-readonly-plugin" / "__pycache__").mkdir()
    (source / "config" / "hkrc" / "analyzer-readonly-plugin" / "__pycache__" / "x.pyc").write_text(
        "stale", encoding="utf-8"
    )
    instance = tmp_path / "instance"
    completed = release("install", instance, source)
    assert completed.returncode == 0, completed.stderr

    seeded = instance / "config" / "hkrc" / "analyzer-readonly-plugin"
    for name in ("plugin.yaml", "__init__.py"):
        assert (seeded / name).read_bytes() == (PAYLOAD / name).read_bytes(), name
    assert not (seeded / "__pycache__").exists()
    # the versioned release carries the payload too
    release_copy = instance / "releases" / "1.0.0" / "config" / "hkrc" / "analyzer-readonly-plugin"
    assert (release_copy / "plugin.yaml").is_file()
    # the provision command hint names the script
    assert "provision_analyzer_profile.py" in completed.stdout


def test_release_refreshes_payload_on_upgrade(tmp_path: Path) -> None:
    source = tmp_path / "source"
    copy_source(source, version="1.0.0")
    instance = tmp_path / "instance"
    assert release("install", instance, source).returncode == 0
    seeded = instance / "config" / "hkrc" / "analyzer-readonly-plugin" / "__init__.py"
    seeded.write_text("# corrupted by hand\n", encoding="utf-8")

    assert release("upgrade", instance, source, "--version", "2.0.0").returncode == 0
    assert seeded.read_bytes() == (PAYLOAD / "__init__.py").read_bytes()


def test_rollback_to_release_without_payload_drops_stale_copy(tmp_path: Path) -> None:
    """Releases materialized before t_1e29fe8d carry no payload: rollback must
    not crash (symlinks already flipped) and must drop the stale seeded copy."""
    source = tmp_path / "source"
    copy_source(source, version="1.0.0")
    instance = tmp_path / "instance"
    assert release("install", instance, source).returncode == 0
    # simulate a pre-payload release 1.0.0
    shutil.rmtree(
        instance / "releases" / "1.0.0" / "config" / "hkrc" / "analyzer-readonly-plugin"
    )
    assert release("upgrade", instance, source, "--version", "2.0.0").returncode == 0
    seeded = instance / "config" / "hkrc" / "analyzer-readonly-plugin"
    assert (seeded / "plugin.yaml").is_file()

    assert release("rollback", instance, source).returncode == 0
    assert not seeded.exists()
