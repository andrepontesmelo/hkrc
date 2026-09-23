#!/usr/bin/env python3
"""Analysis-profile read-only tool contract E2E (kanban t_93d60f39).

Asserts that the LIVE profile the harness-loop analysis stage invokes
(``hermes -p <analysis_profile> chat -q ... --yolo -Q``) grants the analyzer
read access to the repo and no write access.

Why a script and not a unit test: the analysis profile's config and its
read-only toolset plugin are live runtime state under ``~/.hermes/profiles``,
NOT vendored in this repo, so nothing in ``pytest`` can see them.  This guard is
the only automated check that the nightly analyzer is neither repo-blind (the
defect this task fixes: profile ran ``platform_toolsets.cli: [kanban]`` with no
file tools at all) nor silently re-armed with write tools.

Checks (all fail-closed):

1. the analysis profile config wires a read-only toolset into
   ``platform_toolsets.cli`` and enables the profile-local plugin that defines
   it (Hermes has no per-tool enable/disable config key, so the read tools come
   from the plugin's ``read_file`` + ``search_files`` toolset, never from the
   built-in ``file`` toolset which also ships ``patch``/``write_file``);
2. that plugin's source registers the two read tools and neither write tool;
3. the live resolution (``hermes -p <profile> prompt-size --json``, offline, no
   model call, analyzer-like environment from ``_analysis_environment``) yields
   exactly one file-toolset row carrying exactly 2 tools — 4 would mean the
   built-in ``file`` toolset is back, 0 means the analyzer is blind again.

Run from the repo root: ``uv run python scripts/e2e_analysis_profile_readonly.py``
Point it at another instance/config with ``--config <path>`` or by exporting
``HKRC_INSTANCE_ROOT``.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.config import default_config_path, load_config  # noqa: E402
from hkrc.harness_loop import _analysis_environment, _hermes_bin  # noqa: E402

# Toolset the read-only plugin registers, and the plugin's profile-local name.
READ_ONLY_TOOLSET = "readonly_file"
PLUGIN_NAME = "readonly-file"
READ_TOOLS = ("read_file", "search_files")
WRITE_TOOLS = ("patch", "write_file")
# Registry attribution: both read tools live in the built-in `file` toolset, so
# prompt-size reports them under that row even though the session was granted
# the custom read-only toolset.
FILE_TOOLSET_ROW = "file"
EXPECTED_FILE_ROW_TOOLS = len(READ_TOOLS)
TIMEOUT_SECONDS = 300


def profiles_root(profile: str) -> Path:
    """Directory holding per-profile homes (``<hermes home>/profiles``).

    A session running inside a profile has ``HERMES_HOME`` pointing at that
    profile's own home, which would give the nonsense
    ``<profile home>/profiles/<profile>``; prefer the candidate that actually
    holds the analysis profile.
    """
    candidates: list[Path] = []
    env_home = os.environ.get("HERMES_HOME")
    if env_home:
        here = Path(env_home).expanduser()
        candidates.extend([here, here.parent.parent])  # profile home, then main home
    candidates.append(Path.home() / ".hermes")
    for base in candidates:
        if (base / "profiles" / profile / "config.yaml").is_file():
            return base / "profiles"
    return candidates[-1] / "profiles"


def check_config(profile_dir: Path) -> tuple[list[str], list[str]]:
    """Check (1)/(2): config wiring, plugin artifact, read-only plugin source."""
    problems: list[str] = []
    notes: list[str] = []
    config_path = profile_dir / "config.yaml"
    if not config_path.is_file():
        return [f"profile config missing: {config_path}"], notes
    text = config_path.read_text(encoding="utf-8")
    notes.append(f"config: {config_path}")
    if f"- {READ_ONLY_TOOLSET}" not in text:
        problems.append(
            f"platform_toolsets.cli does not list {READ_ONLY_TOOLSET!r} "
            "(analyzer would be repo-blind)"
        )
    if "- file" in text.split("platform_toolsets:")[1].split("telegram:")[0]:
        problems.append(
            "platform_toolsets.cli lists the built-in 'file' toolset, which "
            "ships patch/write_file"
        )
    if f"- {PLUGIN_NAME}" not in text:
        problems.append(f"plugins.enabled does not list {PLUGIN_NAME!r} (toolset never registers)")

    plugin_src_path = profile_dir / "plugins" / PLUGIN_NAME / "__init__.py"
    if not plugin_src_path.is_file():
        problems.append(f"read-only toolset plugin missing: {plugin_src_path}")
        return problems, notes
    notes.append(f"plugin: {plugin_src_path}")
    names = plugin_tool_names(plugin_src_path.read_text(encoding="utf-8"))
    if names is None:
        problems.append(f"plugin source is not parseable Python: {plugin_src_path}")
        return problems, notes
    notes.append(f"plugin literal tool names: {sorted(names)}")
    for tool in WRITE_TOOLS:
        if tool in names:
            problems.append(f"plugin source registers string literal {tool!r}")
    for tool in READ_TOOLS:
        if tool not in names:
            problems.append(f"plugin source never names read tool {tool!r}")
    return problems, notes


def plugin_tool_names(source: str) -> set[str] | None:
    """Tool-name string literals in the plugin source (``None`` when unparseable).

    Parsed as AST and matched on EXACT literals, so prose in a docstring or
    comment that explains why patch/write_file are excluded cannot trip the
    check, while a real grant (``tools=["patch"]``, a tuple, a list) is caught.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    literals = {
        str(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    return literals & (set(READ_TOOLS) | set(WRITE_TOOLS))


def check_resolution(profile: str) -> tuple[list[str], list[str]]:
    """Check (3): live tool resolution for the profile (offline prompt-size)."""
    problems: list[str] = []
    notes: list[str] = []
    command = [_hermes_bin(), "-p", profile, "prompt-size", "--json"]
    notes.append("command: " + " ".join(command))
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=TIMEOUT_SECONDS,
            env=_analysis_environment(),
        )
    except subprocess.TimeoutExpired:
        return [f"prompt-size timed out after {TIMEOUT_SECONDS}s"], notes
    if completed.returncode != 0:
        return [
            f"prompt-size exit {completed.returncode}: "
            f"{(completed.stderr or completed.stdout)[-400:]}"
        ], notes
    try:
        payload = json.loads(completed.stdout or "")
    except ValueError:
        return [f"prompt-size stdout is not JSON: {completed.stdout[:200]!r}"], notes

    rows = {
        str(row.get("toolset")): int(row.get("tool_count") or 0)
        for row in payload.get("toolsets_breakdown") or []
    }
    notes.append(f"toolset rows: {rows}")
    file_tools = rows.get(FILE_TOOLSET_ROW, 0)
    if file_tools != EXPECTED_FILE_ROW_TOOLS:
        problems.append(
            f"resolved file toolset carries {file_tools} tool(s), expected "
            f"{EXPECTED_FILE_ROW_TOOLS} (read-only pair); 0 = repo-blind, "
            f"{EXPECTED_FILE_ROW_TOOLS + len(WRITE_TOOLS)} = write tools back"
        )
    unexpected = sorted(set(rows) - {FILE_TOOLSET_ROW, "kanban"})
    if unexpected:
        problems.append(f"unexpected toolsets granted to the analysis session: {unexpected}")
    if READ_ONLY_TOOLSET not in rows and file_tools == EXPECTED_FILE_ROW_TOOLS:
        notes.append(f"{READ_ONLY_TOOLSET} resolves under the built-in 'file' row (expected)")
    total = int((payload.get("tools") or {}).get("count") or 0)
    if total != sum(rows.values()):
        problems.append(f"tools.count {total} != toolset row sum {sum(rows.values())}")
    return problems, notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="controller config.toml (default: %(default)s)",
    )
    args = parser.parse_args()
    config_path = Path(args.config).expanduser()

    print(f"CONFIG: {config_path}")
    if not config_path.is_file():
        print("VERDICT: FAIL")
        print("  config not found — set HKRC_INSTANCE_ROOT or pass --config")
        return 1
    config = load_config(config_path)
    profile = config.harness_loop.analysis_profile
    print(f"ANALYSIS PROFILE: {profile!r}")
    if not profile:
        print("VERDICT: PASS — analysis_profile empty: the analysis stage is disabled")
        return 0

    profile_dir = profiles_root(profile) / profile
    print(f"PROFILE DIR: {profile_dir}")

    problems: list[str] = []
    notes: list[str] = []
    config_problems, config_notes = check_config(profile_dir)
    problems.extend(config_problems)
    notes.extend(config_notes)
    # Fail fast: a hermes invocation is pointless (and slow) when the live
    # profile wiring is already wrong.
    if not config_problems:
        resolution_problems, resolution_notes = check_resolution(profile)
        problems.extend(resolution_problems)
        notes.extend(resolution_notes)

    print("EVIDENCE:")
    for note in notes:
        print("  " + note)
    print("=" * 60)
    if problems:
        print("VERDICT: FAIL")
        for problem in problems:
            print("  " + problem)
        return 1
    print(
        "VERDICT: PASS — analysis session resolves to read_file + search_files "
        "only (no patch/write_file, not repo-blind)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
