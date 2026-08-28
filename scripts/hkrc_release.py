#!/usr/bin/env python3
"""Install, upgrade, and rollback an instance-scoped HKRC release.

This installer writes only the supplied Hermes instance root. It does not inspect
or modify native Hermes source, config, boards, services, or SQLite databases.
The generated systemd unit is an opt-in artifact only: this script never installs,
enables, or starts it, and the unit contains no stream credentials or connector.
The application itself is stdlib-only, so an installed wrapper uses python3 with
an instance-local release on PYTHONPATH. As its final step, install/upgrade run
the installed wrapper's ``crons sync --dry-run``, which reads the target profile's
cron store read-only and prints the manifest reconciliation diff for the operator
to review; the real sync is an explicit operator command and nothing is auto-deployed.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time


RELEASE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}$")
PACKAGE_DIR = Path("src") / "hkrc"
SKILL_DIR = Path("skills") / "hermes-kanban-blocker-recovery"
SERVICE_TEMPLATE = Path("systemd") / "hkrc.service.in"
SERVICE_RELATIVE_PATH = Path("systemd") / "hkrc.service"
SKILL_DEST_NAME = "blocker-recovery"
PROMPT_TEMPLATE = Path("config") / "hkrc" / "needs-input-watcher-prompt.txt"
MANIFEST_TEMPLATE = Path("config") / "hkrc" / "cron_manifest.json"
DOCS_DIR = Path("docs")
OUTCOME_GUARD_ASSETS = Path("config") / "hkrc" / "outcome-guard-assets.json"
OUTCOME_GUARD_EXAMPLE_CONTRACT = (
    Path("config") / "hkrc" / "outcome-guard-example-contract.json"
)


class ReleaseError(RuntimeError):
    """Raised when a release operation cannot be completed safely."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "upgrade", "rollback", "unit"))
    parser.add_argument("--instance-root", type=Path, required=True)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository contents to install (default: this repository)",
    )
    parser.add_argument(
        "--version",
        help="release identifier for install/upgrade; defaults to src/hkrc/__init__.py version",
    )
    parser.add_argument("--force", action="store_true", help="replace an existing release id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        root = args.instance_root.expanduser().resolve()
        source = args.source_root.expanduser().resolve()
        if args.action == "rollback":
            rollback(root)
        else:
            version = args.version or read_version(source)
            validate_release_id(version)
            if args.action == "unit":
                write_service_unit(root, source)
            elif args.action == "install":
                install(root, source, version, replace=args.force)
            else:
                upgrade(root, source, version, replace=args.force)
    except ReleaseError as exc:
        print(f"hkrc-release: error: {exc}", file=sys.stderr)
        return 2
    return 0


def install(root: Path, source: Path, version: str, *, replace: bool = False) -> None:
    """Install the first release and its instance-local wrapper/skill."""

    root.mkdir(parents=True, exist_ok=True)
    _validate_source(source)
    _ensure_layout(root)
    if _link_target(root / "current") is not None:
        raise ReleaseError(f"instance already has a current release: {root}")
    _materialize_release(root, source, version, replace=replace)
    _activate(root, version, old_current=None)
    write_service_unit(root, source, replace=False)
    _run_cron_sync_preview(root)
    print(f"installed version={version} instance_root={root}")


def upgrade(root: Path, source: Path, version: str, *, replace: bool = False) -> None:
    """Install a new release while retaining the current release for rollback."""

    _validate_source(source)
    _ensure_layout(root)
    old_current = _link_target(root / "current")
    if old_current is None:
        raise ReleaseError(f"cannot upgrade an instance without a current release: {root}")
    _materialize_release(root, source, version, replace=replace)
    _activate(root, version, old_current=old_current)
    write_service_unit(root, source, replace=False)
    _run_cron_sync_preview(root)
    print(f"upgraded version={version} previous={old_current.name} instance_root={root}")


def _run_cron_sync_preview(root: Path) -> None:
    """Run ``hkrc crons sync --dry-run`` as the final deploy step.

    Deploys are operator-controlled: this preview only REPORTS the diff
    between the shipped cron manifest and the live Hermes cron store. It
    never mutates the store — the operator applies the reconciliation with
    the real ``hkrc crons sync --config ...`` command from the deploy
    checklist after reviewing this output. Skipped when the instance has not
    been initialized (no config.toml yet).
    """

    wrapper = root / "bin" / "hkrc"
    config = root / "config" / "hkrc" / "config.toml"
    if not wrapper.is_file() or not config.is_file():
        return
    completed = subprocess.run(
        [str(wrapper), "crons", "sync", "--config", str(config), "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ReleaseError(f"crons sync --dry-run failed: {detail or 'unknown error'}")
    if completed.stdout.strip():
        print("cron reconciliation preview (hkrc crons sync --dry-run):")
        print(completed.stdout.rstrip())
        print(
            "review the diff, then apply with: "
            f"{wrapper} crons sync --config {config}"
        )


def rollback(root: Path) -> None:
    """Swap current and previous releases, preserving a second rollback point."""

    previous = _link_target(root / "previous")
    current = _link_target(root / "current")
    if previous is None or current is None:
        raise ReleaseError(f"rollback requires both current and previous releases: {root}")
    _replace_link(root / "current", Path("releases") / previous.name)
    _replace_link(root / "previous", Path("releases") / current.name)
    _sync_instance_files(root, previous.name)
    print(f"rolled back version={previous.name} previous={current.name} instance_root={root}")


def _validate_source(source: Path) -> None:
    for relative in (
        PACKAGE_DIR / "__init__.py",
        PACKAGE_DIR / "cli.py",
        SKILL_DIR / "SKILL.md",
        SERVICE_TEMPLATE,
        PROMPT_TEMPLATE,
        MANIFEST_TEMPLATE,
        DOCS_DIR,
        OUTCOME_GUARD_ASSETS,
    ):
        if not (source / relative).is_file() and not (source / relative).is_dir():
            raise ReleaseError(f"source repository is missing {relative}: {source}")
    _validate_outcome_guard_manifest(source)


def _validate_outcome_guard_manifest(source: Path) -> None:
    """Gate releases on the checked outcome-guard asset manifest.

    Missing packaged assets (hook adapter, docs, example contract, schema
    state) fail the release instead of shipping a broken package.
    """

    manifest_path = source / OUTCOME_GUARD_ASSETS
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"invalid outcome-guard asset manifest: {manifest_path}") from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("required"), list):
        raise ReleaseError(f"outcome-guard asset manifest must have a 'required' list: {manifest_path}")
    for relative in manifest["required"]:
        if not isinstance(relative, str) or not (source / relative).is_file():
            raise ReleaseError(
                f"outcome-guard asset manifest requires missing file {relative!r}: {source}"
            )


def read_version(source: Path) -> str:
    init = source / PACKAGE_DIR / "__init__.py"
    _validate_source(source)
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', init.read_text(encoding="utf-8"))
    if match is None:
        raise ReleaseError(f"cannot determine package version from {init}")
    return match.group(1)


def validate_release_id(version: str) -> None:
    if not RELEASE_ID.fullmatch(version):
        raise ReleaseError("version must be 1-64 safe release characters (letters, numbers, . _ + -)")


def _ensure_layout(root: Path) -> None:
    (root / "releases").mkdir(parents=True, exist_ok=True)
    (root / "bin").mkdir(parents=True, exist_ok=True)
    (root / "skills").mkdir(parents=True, exist_ok=True)


def _materialize_release(root: Path, source: Path, version: str, *, replace: bool) -> None:
    target = root / "releases" / version
    if target.exists() or target.is_symlink():
        if not replace:
            raise ReleaseError(f"release already exists: {target} (use --force to replace it)")
        shutil.rmtree(target)
    temporary = Path(tempfile.mkdtemp(prefix=f".{version}.", dir=root / "releases"))
    try:
        shutil.copytree(source / "src", temporary / "src")
        shutil.copytree(source / "skills", temporary / "skills")
        shutil.copytree(source / "config", temporary / "config")
        shutil.copytree(source / "docs", temporary / "docs")
        shutil.copytree(source / "systemd", temporary / "systemd")
        (temporary / "release.json").write_text(
            json.dumps({"version": version, "installed_at": int(time.time())}, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.rename(target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _activate(root: Path, version: str, *, old_current: Path | None) -> None:
    if old_current is not None:
        _replace_link(root / "previous", Path("releases") / old_current.name)
    _replace_link(root / "current", Path("releases") / version)
    _write_wrapper(root)
    _sync_instance_files(root, version)


def _sync_instance_files(root: Path, version: str) -> None:
    """Copy the release skill into Hermes' normal instance skill directory.

    Also seed the versioned needs-input-watcher prompt template and the cron
    manifest into the instance config directory. The prompt seed never
    overwrites an existing file: the prompt is operator-customizable, so
    install/upgrade/rollback preserve a local copy once it exists. The cron
    manifest is the repository-controlled source of truth for `hkrc crons
    sync`, so it is refreshed unconditionally on every release.
    """

    source = root / "releases" / version / SKILL_DIR
    destination = root / "skills" / SKILL_DEST_NAME
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.rmtree(temporary, ignore_errors=True)
    shutil.copytree(source, temporary)
    shutil.rmtree(destination, ignore_errors=True)
    temporary.rename(destination)

    prompt_destination = root / PROMPT_TEMPLATE
    if not prompt_destination.exists():
        prompt_destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / "releases" / version / PROMPT_TEMPLATE, prompt_destination)

    manifest_destination = root / MANIFEST_TEMPLATE
    manifest_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / "releases" / version / MANIFEST_TEMPLATE, manifest_destination)

    example_contract_destination = root / OUTCOME_GUARD_EXAMPLE_CONTRACT
    if not example_contract_destination.exists():
        example_contract_destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            root / "releases" / version / OUTCOME_GUARD_EXAMPLE_CONTRACT,
            example_contract_destination,
        )

    assets_destination = root / OUTCOME_GUARD_ASSETS
    assets_destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / "releases" / version / OUTCOME_GUARD_ASSETS, assets_destination)


def write_service_unit(root: Path, source: Path, *, replace: bool = True) -> None:
    """Render the opt-in user-service artifact for one instance.

    This deliberately writes a file under the selected instance root only. It
    never invokes systemctl or copies anything into a systemd search path.
    Existing units are preserved during install/upgrade/rollback so an
    operator's local service customizations are not silently replaced; use the
    explicit ``unit`` action (or ``--force`` through the Python API) to refresh
    one from repository contents.
    """

    root = Path(root).expanduser().resolve()
    source = Path(source).expanduser().resolve()
    _validate_source(source)
    destination = root / SERVICE_RELATIVE_PATH
    if destination.exists() and not replace:
        return
    template = (source / SERVICE_TEMPLATE).read_text(encoding="utf-8")
    values = {
        "__INSTANCE_ROOT__": _systemd_escape(str(root)),
        "__WRAPPER__": _systemd_escape(str(root / "bin" / "hkrc")),
        "__CONFIG__": _systemd_escape(str(root / "config" / "hkrc" / "config.toml")),
        "__WORKING_DIRECTORY__": _systemd_path_escape(str(root)),
        "__INSTANCE_ROOT_PATH__": _systemd_path_escape(str(root)),
    }
    rendered = template
    for marker, value in values.items():
        rendered = rendered.replace(marker, value)
    if "__" in rendered:
        raise ReleaseError(f"service template contains an unknown placeholder: {SERVICE_TEMPLATE}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.chmod(0o644)
    temporary.replace(destination)
    print(f"wrote service_unit={destination}")


def _systemd_escape(value: str) -> str:
    """Escape a value inserted inside a systemd unit's double quotes."""

    if any(character in value for character in "\r\n"):
        raise ReleaseError("instance paths must not contain newlines")
    # Percent is doubled because systemd expands specifiers in unit values.
    return value.replace("%", "%%").replace("\\", "\\\\").replace('"', '\\"')


def _systemd_path_escape(value: str) -> str:
    """Escape a path used by a systemd setting that is not quoted."""

    if any(character in value for character in "\r\n"):
        raise ReleaseError("instance paths must not contain newlines")
    return (
        value.replace("%", "%%")
        .replace("\\", "\\\\")
        .replace(" ", "\\x20")
        .replace("\t", "\\x09")
    )


def _write_wrapper(root: Path) -> None:
    wrapper = root / "bin" / "hkrc"
    content = f"""#!/bin/sh
set -eu
INSTANCE_ROOT={_shell_quote(str(root))}
export HKRC_INSTANCE_ROOT="$INSTANCE_ROOT"
export PYTHONPATH="$INSTANCE_ROOT/current/src${{PYTHONPATH:+:$PYTHONPATH}}"
exec python3 -m hkrc.cli "$@"
"""
    temporary = wrapper.with_name(".hkrc.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.chmod(0o755)
    temporary.replace(wrapper)


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def _link_target(path: Path) -> Path | None:
    if not path.is_symlink():
        return None
    target = Path(os.readlink(path))
    return target if target.is_absolute() else path.parent / target


def _replace_link(path: Path, target: Path) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists() or temporary.is_symlink():
        temporary.unlink()
    temporary.symlink_to(target)
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
