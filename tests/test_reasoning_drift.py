"""Repo-wide reasoning-config drift test (t_751e4f8f, t_7dca44ce).

FAIL-LOUD POLICY (t_7dca44ce batch-1 #4)
========================================
A failing drift test is a REGRESSION of the analyzer session contract.
Operators MUST abort analysis, route ZERO tickets, and emit a
needs_input/blocker finding. There is NO silent fix and NO auto-correction
of argv: the drift is repaired by a human re-applying the lean latch
(strip ``--reasoning``/``--model``/``--max-turns`` from the analyzer
invocation), never by editing this test.

ANALYZER SESSION CONTRACT (t_f4f9ccc2, t_7dca44ce latch B)
==========================================================
The analyzer runs as ONE autonomous Hermes session whose profile config
is the single source of truth: the reasoning level (pro@high) and the
turn budget (``agent.max_turns``, Hermes default 500, well above the
50-turn floor) ride the config, never argv. The invocation is exactly::

    hermes -p <profile> chat -q <prompt> --yolo -Q

``--yolo`` grants tool access (read the repo, verify ``target_path``);
``-Q`` keeps stdout machine-readable. Fallback-A (an explicit positive
``--max-turns 50``) was NOT applied — the turn budget is expressible via
profile config, so no ``--max-turns`` token of any value is allowed
(t_49dcd702 merged contract).

SUMMARIZER ALLOWLIST (t_7dca44ce batch-1 #2)
=============================================
``needs_input_watcher.build_llm_command`` keeps
``--max-turns 4 --yolo -Q --reasoning none`` (stdout-purity exemption).
That invocation is the SOLE ``--reasoning none`` in the repo's CODE
outside the allowlist surface; any other occurrence fails the test.
``src/hkrc/persona_drift.py`` (the drift flagger) is allowlisted because
it documents the latch by design: its docstrings name ``--reasoning none``
as the removed path (t_7dca44ce), never as an invocation.

This file is the sentinel: it must name the tokens to forbid them, so the
repo-wide grep excludes it from its own scan.
"""

from __future__ import annotations

import os
from pathlib import Path

from hkrc.config import ControllerConfig, NeedsInputWatcherConfig, WatcherConfig
from hkrc.harness_loop import HarnessLoopConfig, _analyzer_command
from hkrc.needs_input_watcher import build_llm_command
from hkrc.simulation import _analyzer_argv

REPO_ROOT = Path(__file__).resolve().parents[1]
_SENTINEL = Path(__file__).resolve()

# The three override-token families the analyzer session contract forbids
# on its invocation argv (t_7dca44ce latch B).
OVERRIDE_FAMILIES = ("--reasoning", "--model", "--max-turns")

# Files that legitimately carry override tokens: the summarizer allowlist
# invocation (needs_input_watcher.build_llm_command), the tests, e2e
# replica, and docs that pin its exact command, and the drift flagger
# (persona_drift.py) that documents the latch by design.
SUMMARIZER_ALLOWLIST_FILES = frozenset(
    {
        "src/hkrc/needs_input_watcher.py",
        "src/hkrc/persona_drift.py",
        "tests/test_needs_input_watcher.py",
        "scripts/e2e_canonical_invocation.py",
        "README.md",
        "src/hkrc/config.py",
    }
)

# Directories that are not CODE, TESTS, or DOCS and must not be scanned.
# `.worktrees/` holds sibling git worktree checkouts (each a full copy of
# the repo, many pre-fix) and is git-ignored; scanning it from the
# canonical repo root would drown the test in stale false positives.
_SKIP_DIRS = frozenset(
    {".git", ".venv", "__pycache__", ".mypy_cache", ".ruff_cache", ".worktrees"}
)


def _repo_text_files() -> list[tuple[str, str]]:
    """Every text file under the repo root as ``(repo-relative path, text)``.

    Covers CODE + TESTS + DOCS. Binary and undecodable files are skipped;
    the sentinel drift test file is excluded from its own scan.
    """
    found: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        rel_dir = Path(dirpath).relative_to(REPO_ROOT)
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in filenames:
            path = Path(dirpath) / name
            if path == _SENTINEL:
                continue
            rel = (rel_dir / name).as_posix()
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            found.append((rel, text))
    return found


def _make_config(
    *,
    analysis_profile: str = "",
    llm_profile: str = "",
) -> ControllerConfig:
    """Minimal controller config; analyzer/summarizer profiles on demand."""
    return ControllerConfig(
        "test",
        Path("/tmp/nonexistent-boards"),
        Path("/tmp/nonexistent-state.sqlite3"),
        harness_loop=HarnessLoopConfig(enabled=True, analysis_profile=analysis_profile),
        needs_input_watcher=NeedsInputWatcherConfig(llm_profile=llm_profile),
        watcher=WatcherConfig(),
    )


def test_analyzer_invocation_carries_no_override_tokens() -> None:
    """Assertion (i): the analyzer argv carries no reasoning/model/turn overrides.

    Whole-session contract pinned atomically (t_7dca44ce batch-1 #5):
    ``hermes -p <profile> chat -q <prompt> --yolo -Q`` and nothing else —
    reasoning and turn budget ride the profile config, ``--yolo`` is the
    tool-access grant, ``-Q`` the quiet flag.
    """
    config = _make_config(analysis_profile="nightly-analysis")
    command = _analyzer_command(config, "analyze this")
    assert all(token not in command for token in OVERRIDE_FAMILIES)
    assert command[0].endswith("hermes")
    assert command[1:4] == ["-p", "nightly-analysis", "chat"]
    assert command[4:6] == ["-q", "analyze this"]
    assert command[6:] == ["--yolo", "-Q"]


def test_simulation_analyzer_argv_mirror_carries_no_override_tokens() -> None:
    """The simulation mirror pins the same token-free analyzer argv."""
    argv = _analyzer_argv("nightly-analysis")
    assert all(token not in argv for token in OVERRIDE_FAMILIES)
    assert argv[1:4] == ("-p", "nightly-analysis", "chat")
    assert argv[4:6] == ("-q", "<prompt>")
    assert argv[6:] == ("--yolo", "-Q")


def test_repo_wide_override_token_drift() -> None:
    """Repo-wide literal grep across CODE + TESTS + DOCS for the 3 families.

    Every occurrence of ``--reasoning``/``--model``/``--max-turns`` must be
    either inside the summarizer allowlist surface or on a line that names
    the token only to forbid it (negative assertion ``not in``, or latch
    documentation like "no ... override tokens are emitted"). Any other
    occurrence fails the test.
    """
    offenders: list[str] = []
    for rel, text in _repo_text_files():
        if rel in SUMMARIZER_ALLOWLIST_FILES:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if not any(token in line for token in OVERRIDE_FAMILIES):
                continue
            if _line_only_forbids_token(line):
                continue
            offenders.append(f"{rel}:{lineno}")
    assert not offenders, (
        "FAIL-LOUD: analyzer session contract regressed — override tokens "
        "found outside the summarizer allowlist: " + ", ".join(offenders)
    )


def _line_only_forbids_token(line: str) -> bool:
    """True when a line names an override token solely to forbid it.

    Accepted shapes: a negative assertion (``"... " not in command``) or
    latch documentation ("no/never ... override tokens", "... is NOT
    needed"). An affirmative invocation line never matches.
    """
    lowered = line.lower()
    if "not in" in line:
        return True
    if "override tokens" in line and ("no " in lowered or "never" in lowered):
        return True
    if "not needed" in lowered:
        return True
    return False


def test_sole_reasoning_none_is_the_summarizer_allowlist() -> None:
    """Assertion (ii): the SOLE ``--reasoning none`` in the repo is the
    summarizer allowlist invocation; any other occurrence fails the test.
    """
    hits = [
        rel
        for rel, text in _repo_text_files()
        if "--reasoning none" in text and rel not in SUMMARIZER_ALLOWLIST_FILES
    ]
    assert not hits, (
        "FAIL-LOUD: --reasoning none leaked outside the summarizer allowlist "
        "surface: " + ", ".join(hits)
    )


def test_summarizer_allowlist_invocation_is_sole_reasoning_none() -> None:
    """The summarizer exemption is intact and is the only allowed occurrence.

    Guards against stripping the exemption: if ``build_llm_command`` lost its
    ``--reasoning none``, the sole-occurrence check would pass vacuously.
    """
    command = build_llm_command(_make_config(llm_profile="summarizer"), "t_1")
    assert command[-6:] == ["--max-turns", "4", "--yolo", "-Q", "--reasoning", "none"]
    assert "--reasoning none" in (
        REPO_ROOT / "src/hkrc" / "needs_input_watcher.py"
    ).read_text(encoding="utf-8")
