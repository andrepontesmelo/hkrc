# Operator preflight UX for the archloop nightly launcher (t_495f8ac7).
#
# `plan` prints the exact steps that would unblock ONE dirty/off-base checkout and
# stores them in <repo>/.archloop/preflight-plan.txt; `apply` re-derives the plan,
# refuses on any drift, and otherwise executes only the classified steps.  These
# tests drive the real launcher against throwaway git repos and, where it matters,
# execute the emitted steps and inspect the resulting checkout.

from __future__ import annotations

from datetime import date
import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.harness_loop import _ARCHLOOP_SKIP_LINE, _parse_archloop_skips

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "archloop-night-cron.sh"

# Never emitted, never executed: these destroy operator work in a checkout.
FORBIDDEN = ("git clean", "git checkout --", "git reset --hard", "git stash drop")

# The pre-change nightly digest block for the fixture built by `nightly_fixture`
# (captured from the launcher before this card): only these SKIPPED lines are
# frozen, the per-repo/indented porcelain detail is additive.
GOLDEN_SKIPPED_LINES = (
    "SKIPPED no-new-commits (1): stale-repo",
    "SKIPPED dirty (1): dirty-repo",
    "SKIPPED not-on-main (1): offmain-repo",
)


def today() -> str:
    return date.today().isoformat()


def git(repo: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed: {result.stderr}")
    return result.stdout


def executable(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def make_repo(
    root: Path,
    name: str,
    *,
    branch: str = "main",
    config_base: str | None = None,
    exclude_archloop: bool = True,
) -> Path:
    """A minimal archloop-ready repo: .archloop/config committed, on `branch`."""
    repo = root / name
    repo.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", "-b", branch, str(repo)], check=True)
    git(repo, "config", "user.name", "test")
    git(repo, "config", "user.email", "test@example.invalid")
    (repo / ".archloop").mkdir(parents=True, exist_ok=True)
    config = "TEST=pytest\nLINT=ruff check\n"
    if config_base:
        config += f"ARCHLOOP_BASE_BRANCH={config_base}\n"
    (repo / ".archloop" / "config").write_text(config, encoding="utf-8")
    if exclude_archloop:
        # Same convention the archloop loop driver creates before run.sh.
        (repo / ".git" / "info" / "exclude").write_text(
            ".archloop/\n", encoding="utf-8"
        )
    (repo / "src").mkdir()
    (repo / "src" / "tracked.txt").write_text("x\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "init")
    return repo


def env_for(root: Path, tmp_path: Path, **extra: str) -> dict[str, str]:
    hermes = executable(tmp_path / "hermes", "#!/bin/sh\nprintf '%s\\n' '[]'\n")
    loop_driver = executable(tmp_path / "archloop-loop.sh", "#!/bin/sh\nexit 0\n")
    return {
        **os.environ,
        "ARCHLOOP_REPO_ROOT": str(root),
        "ARCHIVED_BOARDS_DIR": str(tmp_path / "archived"),
        "DRY_RUN": "1",
        "HERMES": str(hermes),
        "LOOP_DRIVER": str(loop_driver),
        "NIGHT_LOG": str(tmp_path / "night.log"),
        **extra,
    }


def run_script(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def commands_block(stdout: str) -> str:
    """The steps `apply --yes` would execute, stripped of the plan's comments."""
    marker = "# commands (apply --yes executes exactly these; nothing else)\n"
    assert marker in stdout, stdout
    block = stdout.split(marker, 1)[1].split("# stranded ship", 1)[0]
    return "\n".join(
        line for line in block.splitlines() if line.strip() and not line.startswith("#")
    ).strip()


def plan(env: dict[str, str], repo: Path) -> subprocess.CompletedProcess[str]:
    return run_script(env, "plan", "--repo", str(repo))


def apply_plan(
    env: dict[str, str], repo: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    return run_script(env, "apply", "--repo", str(repo), *args)


def porcelain(repo: Path) -> str:
    return git(repo, "status", "--porcelain")


def digest_of(repo: Path) -> str:
    return hashlib.sha256(porcelain(repo).encode()).hexdigest()


def exclude_path(repo: Path) -> Path:
    raw = git(repo, "rev-parse", "--path-format=absolute", "--git-path", "info/exclude")
    return Path(raw.strip())


def plan_file(repo: Path) -> Path:
    return repo / ".archloop" / "preflight-plan.txt"


def command_block(text: str) -> list[str]:
    """The `# commands` block of a plan body: exactly what apply --yes may run."""
    lines = text.splitlines()
    start = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("# commands (apply")
    )
    block: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("# stranded ship"):
            break
        if line:
            block.append(line)
    return block


def apply_steps(text: str) -> list[str]:
    """The steps apply reports under `  steps:`."""
    lines = text.splitlines()
    start = lines.index("  steps:")
    steps: list[str] = []
    for line in lines[start + 1 :]:
        if not line.startswith("    "):
            break
        steps.append(line.strip())
    return steps


def execute(block: list[str], cwd: Path) -> None:
    for line in block:
        result = subprocess.run(
            ["bash", "-c", line],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"emitted step failed: {line}\n{result.stderr}"


def stash_subjects(repo: Path) -> list[str]:
    return [line for line in git(repo, "stash", "list").splitlines() if line]


def mixed_repo(root: Path, name: str = "mixed-repo") -> Path:
    """Tracked modification + tool-junk dir + loose untracked file."""
    repo = make_repo(root, name)
    (repo / "src" / "tracked.txt").write_text("modified\n", encoding="utf-8")
    (repo / ".claude").mkdir()
    (repo / ".claude" / "state.json").write_text("c\n", encoding="utf-8")
    (repo / "notes.txt").write_text("loose\n", encoding="utf-8")
    return repo


# --- plan: classification ----------------------------------------------------


def test_plan_tracked_mods_and_loose_untracked_emit_one_stash(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "tracked-repo")
    (repo / "src" / "tracked.txt").write_text("modified\n", encoding="utf-8")
    (repo / "notes.txt").write_text("loose\n", encoding="utf-8")

    result = plan(env_for(root, tmp_path), repo)

    assert result.returncode == 0, result.stderr
    block = command_block(result.stdout)
    assert block == [
        f'git -C {repo} stash push -u -m "archloop preflight {today()} tracked-repo"'
    ]


def test_plan_junk_dir_is_appended_to_info_exclude_not_stashed(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "junk-repo")
    (repo / ".horizon").mkdir()
    (repo / ".horizon" / "state.json").write_text("j\n", encoding="utf-8")

    result = plan(env_for(root, tmp_path), repo)

    assert result.returncode == 0, result.stderr
    assert command_block(result.stdout) == [
        f"printf '%s\\n' '.horizon/' >> {exclude_path(repo)}"
    ]
    assert "stash push" not in result.stdout


def test_plan_mixed_case_classifies_each_class_and_block_clears_the_checkout(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repos"
    repo = mixed_repo(root)
    assert set(porcelain(repo).splitlines()) == {
        " M src/tracked.txt",
        "?? .claude/",
        "?? notes.txt",
    }

    result = plan(env_for(root, tmp_path), repo)

    assert result.returncode == 0, result.stderr
    block = command_block(result.stdout)
    assert len(block) == 2, block
    assert block[0] == f"printf '%s\\n' '.claude/' >> {exclude_path(repo)}"
    assert block[1].startswith(f"git -C {repo} stash push -u -m ")

    # The emitted block is the whole unblock: run it verbatim and the checkout is
    # clean, with the operator's work preserved in one stash.
    execute(block, repo)
    assert porcelain(repo) == ""
    assert len(stash_subjects(repo)) == 1
    assert "archloop preflight" in stash_subjects(repo)[0]


def test_plan_artifact_dir_is_excluded_when_repo_does_not_exclude_it(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "bare-repo", exclude_archloop=False)
    (repo / "notes.txt").write_text("loose\n", encoding="utf-8")
    env = env_for(root, tmp_path)

    block = command_block(plan(env, repo).stdout)
    assert block[0] == f"printf '%s\\n' '.archloop/' >> {exclude_path(repo)}"

    result = apply_plan(env, repo, "--yes")

    assert result.returncode == 0, result.stderr
    assert ".archloop/" in exclude_path(repo).read_text(encoding="utf-8").splitlines()
    # Writing the plan file must not leave the checkout dirty for the nightly walk.
    assert porcelain(repo) == ""
    assert len(stash_subjects(repo)) == 1


def test_plan_and_apply_never_name_a_destructive_command(tmp_path: Path) -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    for forbidden in FORBIDDEN:
        assert forbidden not in source, f"{forbidden!r} appears in the launcher"

    root = tmp_path / "repos"
    repo = mixed_repo(root, "destructive-repo")
    env = env_for(root, tmp_path)

    texts = [plan(env, repo).stdout, apply_plan(env, repo).stdout]
    assert command_block(texts[0])
    for forbidden in FORBIDDEN:
        for text in texts:
            assert forbidden not in text, f"{forbidden!r} emitted in:\n{text}"


# --- plan: branch handling ---------------------------------------------------


def test_on_configured_base_branch_prints_on_intended_and_no_checkout(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "develop-repo", branch="develop", config_base="develop")

    result = plan(env_for(root, tmp_path), repo)

    assert "branch: develop" in result.stdout
    assert "base: develop" in result.stdout
    assert "ON-INTENDED (develop)" in result.stdout
    # Already on its base and clean: the command block must be empty, so applying
    # this plan would run nothing at all.
    assert commands_block(result.stdout) == ""


def test_not_on_base_emits_checkout_of_the_configured_base(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "develop-repo", branch="develop", config_base="develop")
    git(repo, "checkout", "-qb", "feature-x")

    result = plan(env_for(root, tmp_path), repo)

    assert "base: develop" in result.stdout
    assert "not on develop" in result.stdout
    assert command_block(result.stdout) == [f"git -C {repo} checkout develop"]


def test_apply_yes_checks_out_the_configured_base_branch(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "develop-repo", branch="develop", config_base="develop")
    git(repo, "checkout", "-qb", "feature-x")
    env = env_for(root, tmp_path)

    assert plan(env, repo).returncode == 0
    result = apply_plan(env, repo, "--yes")

    assert result.returncode == 0, result.stderr
    assert git(repo, "branch", "--show-current").strip() == "develop"


# --- plan file + digest -----------------------------------------------------


def test_plan_file_records_branch_and_sha256_of_porcelain(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = mixed_repo(root, "digest-repo")
    before = digest_of(repo)

    result = plan(env_for(root, tmp_path), repo)

    assert result.returncode == 0, result.stderr
    stored = plan_file(repo)
    assert stored.is_file()
    text = stored.read_text(encoding="utf-8")
    assert text == result.stdout.split("plan written:")[0].rstrip("\n") + "\n"
    assert "branch: main\n" in text
    assert f"digest: {before}\n" in text
    assert command_block(text) == command_block(result.stdout)


def test_replanning_is_idempotent_despite_its_own_plan_artifact(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = mixed_repo(root, "digest-repo")
    env = env_for(root, tmp_path)

    first = plan(env, repo)
    second = plan(env, repo)

    first_digest = re.search(r"^digest: (\S+)$", first.stdout, re.MULTILINE)
    second_digest = re.search(r"^digest: (\S+)$", second.stdout, re.MULTILINE)
    assert first_digest and second_digest
    assert first_digest.group(1) == digest_of(repo)
    assert first_digest.group(1) == second_digest.group(1)


# --- apply ------------------------------------------------------------------


def test_apply_without_yes_is_print_only_and_executes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = mixed_repo(root)
    env = env_for(root, tmp_path)
    plan_result = plan(env, repo)
    before_dirty = porcelain(repo)
    before_exclude = exclude_path(repo).read_bytes()

    result = apply_plan(env, repo)

    assert result.returncode == 0, result.stderr
    assert "print-only (no --yes)" in result.stdout
    assert apply_steps(result.stdout) == command_block(plan_result.stdout)
    assert stash_subjects(repo) == []
    assert porcelain(repo) == before_dirty
    assert exclude_path(repo).read_bytes() == before_exclude


def test_apply_yes_executes_exactly_the_planned_steps(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = mixed_repo(root)
    env = env_for(root, tmp_path)
    plan_result = plan(env, repo)

    result = apply_plan(env, repo, "--yes")

    assert result.returncode == 0, result.stderr
    assert apply_steps(result.stdout) == command_block(plan_result.stdout)
    assert porcelain(repo) == ""
    excluded = exclude_path(repo).read_text(encoding="utf-8").splitlines()
    assert ".claude/" in excluded
    subjects = stash_subjects(repo)
    assert len(subjects) == 1
    assert "archloop preflight" in subjects[0]


def test_apply_refuses_on_dirty_set_drift_and_executes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "tracked-repo")
    (repo / "src" / "tracked.txt").write_text("modified\n", encoding="utf-8")
    env = env_for(root, tmp_path)
    assert plan(env, repo).returncode == 0

    # Anything that touches the checkout after the plan invalidates the approval.
    (repo / "second.txt").write_text("drift\n", encoding="utf-8")
    before_dirty = porcelain(repo)
    before_exclude = exclude_path(repo).read_bytes()

    result = apply_plan(env, repo, "--yes")

    assert result.returncode != 0
    assert "REFUSED" in result.stdout
    assert "re-run" in result.stdout
    assert stash_subjects(repo) == []
    assert porcelain(repo) == before_dirty
    assert exclude_path(repo).read_bytes() == before_exclude


def test_apply_refuses_when_the_branch_changed_after_the_plan(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "branch-repo")
    git(repo, "checkout", "-qb", "feature-x")
    env = env_for(root, tmp_path)
    assert plan(env, repo).returncode == 0

    git(repo, "checkout", "-q", "main")
    result = apply_plan(env, repo, "--yes")

    assert result.returncode != 0
    assert "REFUSED" in result.stdout
    assert git(repo, "branch", "--show-current").strip() == "main"


def test_apply_without_a_plan_refuses_to_guess(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "fresh-repo")
    (repo / "notes.txt").write_text("loose\n", encoding="utf-8")

    result = apply_plan(env_for(root, tmp_path), repo, "--yes")

    assert result.returncode != 0
    assert "no plan at" in result.stdout
    assert stash_subjects(repo) == []


def test_repo_flag_is_refused_outside_the_repo_root(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    root.mkdir()
    outside = make_repo(tmp_path / "elsewhere", "outside-repo")
    env = env_for(root, tmp_path)

    result = plan(env, outside)

    assert result.returncode != 0
    assert "refusing: --repo must live under" in result.stderr
    assert not plan_file(outside).exists()


# --- stranded SHIP (plan-only) ---------------------------------------------


def test_plan_lists_stranded_shipped_sha_and_apply_never_merges(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "ledger-repo")
    git(repo, "checkout", "-qb", "archloop/side")
    (repo / "src" / "side.txt").write_text("side\n", encoding="utf-8")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "side work")
    sha = git(repo, "rev-parse", "--short", "HEAD").strip()
    git(repo, "checkout", "-q", "main")
    (repo / ".archloop" / "ledger.md").write_text(
        f"- 2026-09-01 some-item: SHIPPED on archloop/side ({sha})\n",
        encoding="utf-8",
    )
    before_head = git(repo, "rev-parse", "HEAD").strip()
    env = env_for(root, tmp_path)

    plan_result = plan(env, repo)

    assert f"#   {sha} (archloop/side) not on main:" in plan_result.stdout
    assert f"git -C {repo} merge --no-ff archloop/side" in plan_result.stdout
    # Advice only: the executable block must never carry the merge.
    assert not any("merge" in line for line in command_block(plan_result.stdout))

    result = apply_plan(env, repo, "--yes")

    assert result.returncode == 0, result.stderr
    assert git(repo, "rev-parse", "HEAD").strip() == before_head
    assert git(repo, "branch", "--contains", sha).strip() == "archloop/side"


def test_ledger_sha_already_on_base_is_not_reported_as_stranded(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    repo = make_repo(root, "ledger-repo")
    sha = git(repo, "rev-parse", "--short", "HEAD").strip()
    (repo / ".archloop" / "ledger.md").write_text(
        f"- 2026-09-01 some-item: SHIPPED on main ({sha})\n", encoding="utf-8"
    )

    result = plan(env_for(root, tmp_path), repo)

    assert "#   none" in result.stdout
    assert "merge --no-ff" not in result.stdout


# --- nightly output ---------------------------------------------------------


def nightly_fixture(root: Path) -> None:
    """One repo per skip class, matching the frozen golden digest block."""
    dirty = make_repo(root, "dirty-repo")
    (dirty / "src" / "tracked.txt").write_text("modified\n", encoding="utf-8")
    (dirty / ".horizon").mkdir()
    (dirty / ".horizon" / "state.json").write_text("j\n", encoding="utf-8")
    (dirty / "notes.txt").write_text("loose\n", encoding="utf-8")

    offmain = make_repo(root, "offmain-repo")
    git(offmain, "checkout", "-qb", "feature-x")

    stale = make_repo(root, "stale-repo")
    (stale / ".archloop" / "ledger.md").write_text(
        "- 2026-09-01 item: SHIPPED on side (deadbee)\n", encoding="utf-8"
    )


def test_nightly_skipped_lines_are_byte_stable_and_carry_file_detail(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repos"
    root.mkdir()
    nightly_fixture(root)

    result = run_script(env_for(root, tmp_path))

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    skipped = [line for line in lines if line.strip().startswith("SKIPPED")]
    assert tuple(skipped) == GOLDEN_SKIPPED_LINES
    # Every line the detector would read as a digest line is unindented.
    assert all(
        line == line.lstrip()
        for line in lines
        if _ARCHLOOP_SKIP_LINE.match(line.strip())
    )

    # Both the per-repo SKIP dirty line and the dirty digest line carry the
    # porcelain listing, 2-space indented, so evidence names the files.
    detail = [line for line in lines if line.startswith("  ")]
    assert detail, lines
    assert "  ?? .horizon/" in detail
    assert "   M src/tracked.txt" in detail
    assert "  ?? notes.txt" in detail
    stripped_detail = {line.strip() for line in detail}
    assert {"?? .horizon/", "M src/tracked.txt", "?? notes.txt"} <= stripped_detail

    per_repo = next(
        index
        for index, line in enumerate(lines)
        if line.endswith("SKIP dirty-repo: dirty canonical checkout")
    )
    assert lines[per_repo + 1].startswith("  ")
    assert "  ?? .horizon/" in lines[per_repo + 1 : per_repo + 4]

    dirty_digest = lines.index("SKIPPED dirty (1): dirty-repo")
    assert lines[dirty_digest + 1].startswith("  ")
    assert "  ?? .horizon/" in lines[dirty_digest + 1 : dirty_digest + 4]

    # The converter's view is unchanged: parsing the new stdout is identical to
    # parsing it with the indented listing removed.
    without_detail = "\n".join(line for line in lines if not line.startswith("  "))
    assert _parse_archloop_skips(result.stdout) == _parse_archloop_skips(without_detail)
    assert _parse_archloop_skips(result.stdout) == {
        "no-new-commits": ["stale-repo"],
        "dirty": ["dirty-repo"],
        "not-on-main": ["offmain-repo"],
    }


def test_nightly_detail_listing_is_capped(tmp_path: Path) -> None:
    root = tmp_path / "repos"
    root.mkdir()
    repo = make_repo(root, "busy-repo")
    for index in range(25):
        (repo / f"loose-{index:02d}.txt").write_text("x\n", encoding="utf-8")
    env = env_for(root, tmp_path, ARCHLOOP_PREFLIGHT_DETAIL_MAX="5")

    result = run_script(env)

    assert "SKIPPED dirty (1): busy-repo" in result.stdout
    detail = [line for line in result.stdout.splitlines() if line.startswith("  ")]
    assert detail.count("  ... (20 more)") == 2
    assert len([line for line in detail if not line.startswith("  ...")]) == 10
