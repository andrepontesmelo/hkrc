"""Guard test for scripts/e2e_analysis_profile_readonly.py (kanban t_93d60f39).

The live analysis profile config and its read-only toolset plugin are runtime
state under ``~/.hermes/profiles`` and are NOT vendored in this repo, so the
guard script is the nightly's only automated check against a repo-blind (or
suddenly writable) analyzer.  These tests pin the guard's own verdicts on
fixture profiles so a guard that stopped detecting anything would fail here.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
GUARD = ROOT / "scripts" / "e2e_analysis_profile_readonly.py"


def load_guard():
    spec = importlib.util.spec_from_file_location("e2e_analysis_profile_readonly", GUARD)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_config(path: Path, analysis_profile: str) -> Path:
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


def write_profile(profile_dir: Path, *, toolsets: str, plugin_source: str) -> None:
    (profile_dir / "plugins" / "readonly-file").mkdir(parents=True)
    (profile_dir / "config.yaml").write_text(
        "plugins:\n"
        "  enabled:\n"
        "    - readonly-file\n"
        "platform_toolsets:\n"
        "  cli:\n"
        f"{toolsets}",
        encoding="utf-8",
    )
    (profile_dir / "plugins" / "readonly-file" / "__init__.py").write_text(
        plugin_source, encoding="utf-8"
    )


GOOD_TOOLSETS = "    - kanban\n    - readonly_file\n"
GOOD_PLUGIN = (
    "TOOLSET = \"readonly_file\"\n"
    "TOOLS = (\"read_file\", \"search_files\")\n"
)


def test_guard_config_check_accepts_readonly_profile(tmp_path: Path) -> None:
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(profile_dir, toolsets=GOOD_TOOLSETS, plugin_source=GOOD_PLUGIN)
    problems, notes = guard.check_config(profile_dir)
    assert problems == [], problems
    assert any("config.yaml" in note for note in notes)


def test_guard_config_check_flags_repo_blind_profile(tmp_path: Path) -> None:
    """The live defect: cli toolsets without any file toolset."""
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(profile_dir, toolsets="    - kanban\n", plugin_source=GOOD_PLUGIN)
    problems, _ = guard.check_config(profile_dir)
    assert any("repo-blind" in problem for problem in problems), problems


def test_guard_config_check_flags_builtin_file_toolset(tmp_path: Path) -> None:
    """The built-in file toolset ships patch/write_file: never enable it here."""
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(profile_dir, toolsets="    - kanban\n    - file\n", plugin_source=GOOD_PLUGIN)
    problems, _ = guard.check_config(profile_dir)
    assert any("patch/write_file" in problem for problem in problems), problems


def test_guard_config_check_flags_write_tools_in_plugin(tmp_path: Path) -> None:
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(
        profile_dir,
        toolsets=GOOD_TOOLSETS,
        plugin_source='TOOLS = ("read_file", "search_files", "patch")\n',
    )
    problems, _ = guard.check_config(profile_dir)
    assert any("string literal 'patch'" in problem for problem in problems), problems


def test_guard_ignores_write_tool_names_in_prose(tmp_path: Path) -> None:
    """Docstrings may explain why write tools are excluded; that is not a grant."""
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(
        profile_dir,
        toolsets=GOOD_TOOLSETS,
        plugin_source=(
            '"""Read-only toolset: the built-in file toolset ships patch and '
            'write_file, so we never enable it."""\n'
            'TOOLS = ("read_file", "search_files")  # no patch, no write_file\n'
        ),
    )
    problems, _ = guard.check_config(profile_dir)
    assert problems == [], problems


def test_guard_fails_when_plugin_source_is_unparseable(tmp_path: Path) -> None:
    guard = load_guard()
    profile_dir = tmp_path / "profiles" / "authoritative"
    write_profile(profile_dir, toolsets=GOOD_TOOLSETS, plugin_source="TOOLS = (\n")
    problems, _ = guard.check_config(profile_dir)
    assert any("not parseable" in problem for problem in problems), problems


def test_guard_coerces_file_row_count() -> None:
    """A 4-tool file row means write tools are back; 0 means repo-blind."""
    guard = load_guard()
    assert guard.EXPECTED_FILE_ROW_TOOLS == 2
    assert guard.READ_TOOLS == ("read_file", "search_files")
    assert guard.WRITE_TOOLS == ("patch", "write_file")


def test_guard_reports_disabled_stage_without_touching_hermes(tmp_path: Path) -> None:
    config = write_config(tmp_path / "config.toml", analysis_profile="")
    completed = subprocess.run(
        [sys.executable, str(GUARD), "--config", str(config)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "stage is disabled" in completed.stdout


def test_guard_fails_closed_when_profile_config_missing(tmp_path: Path) -> None:
    config = write_config(tmp_path / "config.toml", analysis_profile="authoritative")
    home = tmp_path / "hermes"
    (home / "profiles").mkdir(parents=True)
    env = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path), "HERMES_HOME": str(home)}
    completed = subprocess.run(
        [sys.executable, str(GUARD), "--config", str(config)],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    assert completed.returncode == 1
    assert "VERDICT: FAIL" in completed.stdout
    assert "profile config missing" in completed.stdout
    # fail fast: no hermes invocation once the config wiring is already wrong
    assert "command:" not in completed.stdout
