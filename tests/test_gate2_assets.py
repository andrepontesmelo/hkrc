"""Gate 2 acceptance: self-contained package, manifest gate, clean-root E2E."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "hkrc_release.py"
E2E = ROOT / "scripts" / "e2e_outcome_guard_gate2.py"
ASSETS_MANIFEST = ROOT / "config" / "hkrc" / "outcome-guard-assets.json"
VERSION_MATCH = re.search(
    r'__version__ = "([^"]+)"',
    (ROOT / "src" / "hkrc" / "__init__.py").read_text(encoding="utf-8"),
)
assert VERSION_MATCH is not None
VERSION = VERSION_MATCH.group(1)


def release(action: str, root: Path, source: Path = ROOT, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(RELEASE), action, "--source-root", str(source), "--instance-root", str(root), *extra],
        text=True,
        capture_output=True,
        check=False,
    )


def copy_source(source: Path) -> None:
    source.mkdir()
    for relative in ("src", "skills", "systemd", "config", "docs"):
        subprocess.run(["cp", "-a", str(ROOT / relative), str(source / relative)], check=True)


def test_asset_manifest_entries_exist_in_repo() -> None:
    manifest = json.loads(ASSETS_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "hkrc.outcome-guard-assets.v1"
    required = manifest["required"]
    assert required, "manifest must list required assets"
    for relative in required:
        assert (ROOT / relative).is_file(), f"manifest entry missing in repo: {relative}"


def test_release_install_ships_docs_assets_and_wrapper_works(tmp_path: Path) -> None:
    instance = tmp_path / "instance"
    result = release("install", instance)
    assert result.returncode == 0, result.stderr
    release_dir = instance / "releases" / VERSION
    # Docs ship inside the release: no path back into the source checkout.
    assert (release_dir / "docs" / "outcome-guard.md").is_file()
    # Runtime adapter files ship.
    assert (release_dir / "src" / "hkrc" / "admission.py").is_file()
    assert (release_dir / "src" / "hkrc" / "git_enforce.py").is_file()
    # Seeded instance files.
    assert (instance / "config" / "hkrc" / "outcome-guard-example-contract.json").is_file()
    assert (instance / "config" / "hkrc" / "outcome-guard-assets.json").is_file()
    # The installed wrapper exposes the Gate 2 surface without any source path.
    wrapper = instance / "bin" / "hkrc"
    help_run = subprocess.run(
        [str(wrapper), "outcome-guard", "git-hook", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_run.returncode == 0
    assert "install" in help_run.stdout and "uninstall" in help_run.stdout


def test_release_ships_every_installed_manifest_entry(tmp_path: Path) -> None:
    """Every asset declared by the installed manifest must ship inside the release.

    Regression for DEF-002: systemd/hkrc.service.in was declared in the checked
    manifest but _materialize_release copied only src/skills/config/docs, so a
    fresh install lacked current/systemd/hkrc.service.in. An installed release
    must contain every required runtime/service asset; the release must need no
    path back into the source checkout.
    """
    instance = tmp_path / "instance"
    result = release("install", instance)
    assert result.returncode == 0, result.stderr
    release_dir = instance / "releases" / VERSION
    installed_manifest = instance / "config" / "hkrc" / "outcome-guard-assets.json"
    assert installed_manifest.is_file()
    manifest = json.loads(installed_manifest.read_text(encoding="utf-8"))
    required = manifest["required"]
    assert required, "installed manifest must list required assets"
    for relative in required:
        assert (release_dir / relative).is_file(), (
            f"installed manifest entry missing in release: {relative}"
        )


def test_release_fails_closed_when_manifest_asset_missing(tmp_path: Path) -> None:
    source = tmp_path / "source"
    copy_source(source)
    missing = source / "docs" / "outcome-guard.md"
    assert missing.is_file()
    missing.unlink()
    result = release("install", tmp_path / "instance", source)
    assert result.returncode == 2
    assert "outcome-guard asset manifest requires missing file" in result.stderr
    assert not (tmp_path / "instance" / "current").exists()  # nothing was activated


def test_clean_root_e2e_passes_using_shipped_artifacts(tmp_path: Path) -> None:
    """Acceptance H: a fresh root installed from shipped artifacts passes E2E."""
    completed = subprocess.run(
        [sys.executable, str(E2E)],
        text=True,
        capture_output=True,
        check=False,
        env=dict(os.environ),
        timeout=300,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    assert "E2E GATE2 OK" in completed.stdout
