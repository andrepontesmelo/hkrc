#!/usr/bin/env python3
"""Provision the analysis profile's read-only toolset (kanban t_1e29fe8d).

Repairs the two pieces of LIVE runtime state the installer contract
deliberately never touches (hkrc_release.py writes only the instance root):

1. seeds ``<hermes home>/profiles/<analysis_profile>/plugins/readonly-file/``
   from the release payload (``config/hkrc/analyzer-readonly-plugin/``, seeded
   into the instance root by ``_sync_instance_files`` on every release);
2. adds the two config.yaml lines the profile needs — ``readonly_file`` under
   ``platform_toolsets.cli`` and ``readonly-file`` under ``plugins.enabled`` —
   idempotently and fail-closed: the edit is line-oriented, only ever inserts
   a missing list entry, writes a ``.bak-<timestamp>`` copy before the first
   edit, and refuses (nonzero exit) on any YAML structure it does not
   understand instead of guessing.

``--check`` verifies plugin files and both config lines and changes nothing.
The default run ends by executing the committed guard
(``scripts/e2e_analysis_profile_readonly.py``); if the guard fails, the
provisioner fails.

Run from the repo root: ``uv run python scripts/provision_analyzer_profile.py``
Point it at another instance/config with ``--config <path>`` or by exporting
``HKRC_INSTANCE_ROOT`` (same resolution as the guard).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.config import default_config_path, load_config  # noqa: E402

READ_ONLY_TOOLSET = "readonly_file"
PLUGIN_NAME = "readonly-file"
PAYLOAD_DIR = Path("config") / "hkrc" / "analyzer-readonly-plugin"
PAYLOAD_FILES = ("plugin.yaml", "__init__.py")
GUARD_SCRIPT = Path("scripts") / "e2e_analysis_profile_readonly.py"


class ProvisionError(RuntimeError):
    """Raised when provisioning cannot continue safely."""


def script_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def payload_candidates(config_path: Path) -> list[Path]:
    """Release-payload locations, most authoritative first.

    The instance root's seeded copy is fresher than any repo checkout (the
    installer refreshes it on every release), so it wins when present.
    """

    candidates: list[Path] = []
    instance_root = os.environ.get("HKRC_INSTANCE_ROOT", "").strip()
    if instance_root:
        candidates.append(Path(instance_root).expanduser() / PAYLOAD_DIR)
    # config.toml normally lives at <instance root>/config/hkrc/config.toml.
    if len(config_path.parents) >= 2 and config_path.parents[1].name == "config":
        candidates.append(config_path.parents[2] / PAYLOAD_DIR)
    candidates.append(script_repo_root() / PAYLOAD_DIR)
    return candidates


def resolve_payload(config_path: Path) -> Path:
    for candidate in payload_candidates(config_path):
        if all((candidate / name).is_file() for name in PAYLOAD_FILES):
            return candidate
    searched = ", ".join(str(path) for path in payload_candidates(config_path))
    raise ProvisionError(f"release payload not found (searched: {searched})")


def profiles_root(profile: str) -> Path:
    """Directory holding per-profile homes (``<hermes home>/profiles``).

    A session running inside a profile has ``HERMES_HOME`` pointing at that
    profile's own home, which would give the nonsense
    ``<profile home>/profiles/<profile>``; prefer the candidate that actually
    holds the analysis profile. Mirrors the guard's resolution.
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


def seed_plugin(profile_dir: Path, payload: Path, *, force: bool) -> str:
    """Seed the profile-local plugin dir from the release payload."""

    destination = profile_dir / "plugins" / PLUGIN_NAME
    if destination.exists() and not force:
        return f"already provisioned: {destination} (use --force to reseed)"
    parent = destination.parent
    if not parent.is_dir():
        parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.rmtree(temporary, ignore_errors=True)
    shutil.copytree(payload, temporary, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.rmtree(destination, ignore_errors=True)
    temporary.rename(destination)
    return f"seeded {destination} from {payload}"


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip())


def _is_comment(line: str) -> bool:
    return line.lstrip().startswith("#")


def _find_block(lines: list[str], parent_key: str, child_key: str) -> tuple[int, int, int]:
    """Locate a ``parent_key:`` / ``child_key:`` block mapping, fail-closed.

    Returns ``(child_index, child_indent, entry_indent)``: the line index of
    the ``child_key:`` header, its indentation, and the indentation of its
    list entries (equal-indent block sequences are valid YAML, so entries may
    sit at ``child_indent`` too). Every non-blank, non-comment line inside the
    child block must be a ``- entry`` item at one consistent indent. Anything
    unexpected — duplicated sections, flow style, nested mappings — raises
    ProvisionError instead of guessing.
    """

    parent_indexes = [
        index for index, line in enumerate(lines) if line.strip() == f"{parent_key}:"
    ]
    if len(parent_indexes) != 1:
        raise ProvisionError(
            f"config.yaml must have exactly one top-level '{parent_key}:' mapping "
            f"(found {len(parent_indexes)})"
        )
    parent_index = parent_indexes[0]
    parent_indent = _indent_of(lines[parent_index])

    child_index: int | None = None
    child_indent: int | None = None
    level_indent: int | None = None
    for index in range(parent_index + 1, len(lines)):
        line = lines[index]
        if not line.strip() or _is_comment(line):
            continue
        indent = _indent_of(line)
        if indent <= parent_indent:
            break  # left the parent block
        if level_indent is None:
            level_indent = indent  # first direct child fixes the level
        if indent > level_indent:
            continue  # content inside a sibling key's own block
        stripped = line.strip()
        if stripped == f"{child_key}:":
            if child_index is not None:
                raise ProvisionError(
                    f"config.yaml has duplicate '{child_key}:' under '{parent_key}:'"
                )
            child_index = index
            child_indent = indent
        elif not stripped.startswith("- "):
            continue  # another direct child key; keep looking
    if child_index is None or child_indent is None:
        raise ProvisionError(
            f"config.yaml has no '{child_key}:' mapping under '{parent_key}:' "
            "(section missing or inline/flow style) — refusing to guess"
        )

    entry_indent: int | None = None
    for index in range(child_index + 1, len(lines)):
        line = lines[index]
        if not line.strip() or _is_comment(line):
            continue
        indent = _indent_of(line)
        if indent < child_indent:
            break  # block ended
        stripped = line.strip()
        if indent == child_indent and not stripped.startswith("- "):
            break  # next sibling key under the same parent
        if not stripped.startswith("- "):
            raise ProvisionError(
                f"unexpected YAML structure under '{parent_key}.{child_key}:' "
                f"(line {index + 1}: {stripped!r} is not a list entry)"
            )
        if entry_indent is None:
            entry_indent = indent
        elif indent != entry_indent:
            raise ProvisionError(
                f"unexpected YAML structure under '{parent_key}.{child_key}:' "
                f"(inconsistent entry indent at line {index + 1})"
            )
    if entry_indent is None:
        entry_indent = child_indent + 2  # empty list: conventional deeper indent
    return child_index, child_indent, entry_indent


def _iter_block_entries(lines: list[str], child_index: int, child_indent: int):
    """Yield ``(index, text)`` for each list entry of the child block."""

    for index in range(child_index + 1, len(lines)):
        line = lines[index]
        if not line.strip() or _is_comment(line):
            continue
        indent = _indent_of(line)
        if indent < child_indent:
            break
        stripped = line.strip()
        if indent == child_indent and not stripped.startswith("- "):
            break
        if not stripped.startswith("- "):
            break  # unreachable after _find_block validated; stay safe
        yield index, stripped


def _block_entry_present(lines: list[str], child_index: int, child_indent: int, entry: str) -> bool:
    return any(text == f"- {entry}" for _, text in _iter_block_entries(lines, child_index, child_indent))


def _block_end(lines: list[str], child_index: int, child_indent: int) -> int:
    """Index of the child block's last list entry (header when the list is empty)."""

    end = child_index
    for index, _ in _iter_block_entries(lines, child_index, child_indent):
        end = index
    return end


def _insert_entry(lines: list[str], end: int, entry_indent: int, entry: str) -> list[str]:
    new_line = " " * entry_indent + f"- {entry}\n"
    if not lines[end].endswith("\n"):
        # Last line of the file without a terminator: keep its bytes intact
        # and start the appended entry on a fresh line.
        return lines[: end + 1] + [f"\n{new_line}"] + lines[end + 1 :]
    return lines[: end + 1] + [new_line] + lines[end + 1 :]


def check_config_lines(text: str) -> tuple[bool, bool]:
    """Report whether the toolset line and the plugin line are wired."""

    lines = text.splitlines(keepends=True)
    try:
        toolset_index, toolset_indent, _ = _find_block(lines, "platform_toolsets", "cli")
        plugin_index, plugin_indent, _ = _find_block(lines, "plugins", "enabled")
    except ProvisionError:
        return False, False
    toolset = _block_entry_present(lines, toolset_index, toolset_indent, READ_ONLY_TOOLSET)
    plugin = _block_entry_present(lines, plugin_index, plugin_indent, PLUGIN_NAME)
    return toolset, plugin


def _ensure_entry(lines: list[str], parent_key: str, child_key: str, entry: str) -> tuple[list[str], str | None]:
    """Insert ``entry`` into the ``parent_key.child_key`` list when absent.

    The block is located FRESH each call: an earlier insertion in the same
    run shifts every later line index, so reusing a resolved index would
    write into the wrong section.
    """

    child_index, child_indent, entry_indent = _find_block(lines, parent_key, child_key)
    if _block_entry_present(lines, child_index, child_indent, entry):
        return lines, None
    end = _block_end(lines, child_index, child_indent)
    return _insert_entry(lines, end, entry_indent, entry), f"{parent_key}.{child_key} += {entry!r}"


def apply_config_lines(config_path: Path) -> list[str]:
    """Insert the two config lines when absent; return the applied changes."""

    text = config_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    lines, toolset_change = _ensure_entry(lines, "platform_toolsets", "cli", READ_ONLY_TOOLSET)
    lines, plugin_change = _ensure_entry(lines, "plugins", "enabled", PLUGIN_NAME)
    changes = [change for change in (toolset_change, plugin_change) if change]
    if not changes:
        return []
    backup = config_path.with_name(f"{config_path.name}.bak-{time.strftime('%Y%m%d-%H%M%S')}")
    shutil.copy2(config_path, backup)
    config_path.write_text("".join(lines), encoding="utf-8")
    return [f"{change} (backup: {backup.name})" for change in changes]


def run_guard(config_path: Path) -> int:
    """Execute the committed guard; its verdict is the provisioner's verdict."""

    guard = script_repo_root() / GUARD_SCRIPT
    if not guard.is_file():
        print(f"guard script not found, skipping: {guard}")
        return 0
    print(f"running guard: {guard}")
    completed = subprocess.run(
        [sys.executable, str(guard), "--config", str(config_path)],
        check=False,
    )
    if completed.returncode != 0:
        print(f"guard FAILED (exit {completed.returncode}) — provisioning did not verify")
        return completed.returncode
    print("guard passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(default_config_path()),
        help="controller config.toml (default: %(default)s)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="reseed the profile plugin dir even when it already exists",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify plugin files and config lines, change nothing",
    )
    args = parser.parse_args(argv)
    config_path = Path(args.config).expanduser()

    print(f"CONFIG: {config_path}")
    if not config_path.is_file():
        print("FAIL: config not found — set HKRC_INSTANCE_ROOT or pass --config")
        return 1
    try:
        config = load_config(config_path)
    except Exception as exc:  # noqa: BLE001 - fail-closed on any config error
        print(f"FAIL: cannot load config: {exc}")
        return 1
    profile = config.harness_loop.analysis_profile
    print(f"ANALYSIS PROFILE: {profile!r}")
    if not profile:
        print("PASS — analysis_profile empty: the analysis stage is disabled")
        return 0

    profile_dir = profiles_root(profile) / profile
    print(f"PROFILE DIR: {profile_dir}")
    try:
        payload = resolve_payload(config_path)
        print(f"PAYLOAD: {payload}")
        yaml_path = profile_dir / "config.yaml"
        if not yaml_path.is_file():
            if args.check:
                print(f"CHECK: FAIL — profile config missing: {yaml_path}")
                return 1
            raise ProvisionError(f"profile config missing: {yaml_path}")
        toolset_wired, plugin_wired = check_config_lines(yaml_path.read_text(encoding="utf-8"))
        plugin_dir = profile_dir / "plugins" / PLUGIN_NAME
        plugin_files_ok = all((plugin_dir / name).is_file() for name in PAYLOAD_FILES)

        if args.check:
            problems = []
            if not plugin_files_ok:
                problems.append(f"plugin files missing under {plugin_dir}")
            if not toolset_wired:
                problems.append(f"platform_toolsets.cli does not list {READ_ONLY_TOOLSET!r}")
            if not plugin_wired:
                problems.append(f"plugins.enabled does not list {PLUGIN_NAME!r}")
            if problems:
                print("CHECK: FAIL")
                for problem in problems:
                    print("  " + problem)
                return 1
            print(
                f"CHECK: PASS — plugin files present, {READ_ONLY_TOOLSET!r} and "
                f"{PLUGIN_NAME!r} wired"
            )
            return 0

        if plugin_dir.exists() and not args.force:
            print(f"already provisioned: {plugin_dir} (use --force to reseed)")
        else:
            print(seed_plugin(profile_dir, payload, force=args.force))
        changes = apply_config_lines(yaml_path)
        for change in changes:
            print(f"config.yaml: {change}")
        if not changes:
            print("config.yaml: both lines already present (no edit needed)")
    except ProvisionError as exc:
        print(f"FAIL: {exc}")
        return 1
    return run_guard(config_path)


if __name__ == "__main__":
    raise SystemExit(main())
