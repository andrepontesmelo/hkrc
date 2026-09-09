"""Fix-ticket findings for t_5c75b8e6 (DEF-t_5c75b8e6-1).

Regression: rollback to a release materialized by an older installer that did
not ship `skills/friction-flag` crashed with FileNotFoundError from
shutil.copytree in _sync_instance_files, AFTER rollback() had already flipped
the current/previous symlinks (half-mutated instance state). The fix makes
_sync_instance_files skip a skill directory missing from the RELEASE while
still removing any stale installed copy of it (e.g. the newer skill left over
from the release being rolled back). Forward installs/upgrades are unchanged:
_validate_source still requires every SKILL_DIR in the SOURCE.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

from test_release import ROOT, copy_source, release

REPO_SCRIPT = ROOT / "scripts" / "hkrc_release.py"


def make_old_installer(tmp_path: Path) -> Path:
    """Copy of the repo installer as the pre-friction-flag version.

    Mirrors main's installer before t_5c75b8e6: SKILL_DIRS lists only
    blocker-recovery, so it materializes and validates releases without a
    friction-flag skill dir.
    """

    script = tmp_path / "hkrc_release_old.py"
    text = REPO_SCRIPT.read_text(encoding="utf-8")
    entry = '    (Path("skills") / "friction-flag", "friction-flag"),\n'
    assert entry in text
    script.write_text(text.replace(entry, ""), encoding="utf-8")
    return script


def run_installer(
    script: Path, action: str, root: Path, source: Path | None = None, *extra: str
) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(script), action, "--instance-root", str(root)]
    if source is not None:
        command += ["--source-root", str(source)]
    return subprocess.run(command, text=True, capture_output=True, check=False)


def test_rollback_to_release_without_friction_flag_succeeds(tmp_path: Path) -> None:
    # Old source, main-tree shape: no skills/friction-flag at all. Materialized
    # by the OLD installer, exactly as a real pre-friction-flag instance was.
    old_source = tmp_path / "old-source"
    copy_source(old_source, version="1.0.0")
    shutil.rmtree(old_source / "skills" / "friction-flag")
    old_installer = make_old_installer(tmp_path)

    instance = tmp_path / "instance"
    assert run_installer(old_installer, "install", instance, old_source).returncode == 0
    assert not (instance / "skills" / "friction-flag").exists()

    # Upgrade with the current installer/source, which ship friction-flag.
    assert release("upgrade", instance).returncode == 0
    assert (instance / "skills" / "friction-flag" / "SKILL.md").is_file()
    assert (instance / "current").resolve().name != "1.0.0"

    # Rollback to the pre-friction-flag release: previously rc=1 with
    # FileNotFoundError from shutil.copytree, after the symlinks were flipped.
    result = release("rollback", instance)
    assert result.returncode == 0, result.stderr
    assert (instance / "current").resolve().name == "1.0.0"
    assert (instance / "previous").resolve().name != "1.0.0"

    # blocker-recovery stays synced from the old release; the stale
    # new-release friction-flag skill is removed from the instance skills dir
    # so old code runs with no stale skill.
    assert (instance / "skills" / "blocker-recovery" / "SKILL.md").is_file()
    assert not (instance / "skills" / "friction-flag").exists()


def test_sync_skips_skill_dir_missing_from_release_without_stale_copy(tmp_path: Path) -> None:
    # Same shape, driven through the Python API: a release without the
    # friction-flag dir must sync cleanly and must not leave an older
    # installed copy of that skill behind.
    import sys

    sys.path.insert(0, str(ROOT / "scripts"))
    try:
        import hkrc_release
    finally:
        sys.path.remove(str(ROOT / "scripts"))

    source = tmp_path / "source"
    copy_source(source)
    shutil.rmtree(source / "skills" / "friction-flag")

    root = tmp_path / "instance"
    hkrc_release._ensure_layout(root)
    hkrc_release._materialize_release(root, source, "1.0.0", replace=False)
    (root / "skills" / "friction-flag").mkdir()  # stale installed copy

    hkrc_release._sync_instance_files(root, "1.0.0")

    assert (root / "skills" / "blocker-recovery" / "SKILL.md").is_file()
    assert not (root / "skills" / "friction-flag").exists()
