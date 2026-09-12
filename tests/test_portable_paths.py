"""Regression: no tracked file is pinned to one operator's home directory.

A literal ``/home/<username>`` in a shipped file silently binds the
deployment to one machine and one account.  The 2026-09 portability sweep
removed the last ones (the supervisor mission path in the cron manifest,
the harness-loop instance defaults, the live CLI hint, the three-way
control root); this test greps every git-tracked file for the pattern so a
new hardcoded path cannot sneak back into config/, src/, scripts/, docs/,
or anywhere else.

The allowlist below is the complete set of legitimate exceptions — test
fixtures only, each with its reason.  Shipped dirs carry none.  When a
listed file stops carrying a match, its entry must be removed with it
(the stale-entry assertion enforces that).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Matches the home prefix of any operator account (not just this machine's)
# followed by at least one path character.  A bare "/home/" (as in the scrub
# assertions ``assert "/home/" not in report``) and unrelated words such as
# "/homebrew" do not match.
HOME_LITERAL = re.compile(r"/home/[A-Za-z0-9._-]+")

# Legitimate exceptions: per tracked file, with the reason.  Everything
# here is a test fixture; no shipped file (config/, src/, scripts/, docs/)
# may ever appear on this list.
ALLOWLIST: dict[str, str] = {
    # Redaction fixtures: deliberately carry a home-shaped path so the
    # scrub/normalization layer must keep it out of operator-visible output.
    "tests/test_assist_observer.py": "fixture message exercises packet path redaction",
    "tests/test_friction_flags.py": "fixture path the report normalizer must scrub",
    # Generic fake-operator fixture environments, never a real account.
    "tests/test_needs_input_watcher.py": "generic fixture HOME values",
    "tests/test_review_gap.py": "generic fixture HOME values",
    # Incident history quoted verbatim in a test docstring (t_ae960b7d).
    "tests/test_harness_loop.py": "quoted incident narrative in a docstring",
    # Ledger round-trip fixture state path (shape only; never opened).
    "tests/test_ledger_roundtrip.py": "fixture state path",
}


def _tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z"],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return [
        ROOT / name
        for name in out.stdout.decode("utf-8").split("\0")
        if name
    ]


def _home_literal_hits(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []  # unreadable/binary tracked asset
    return [
        f"{path.relative_to(ROOT).as_posix()}:{lineno}: {line.strip()[:120]}"
        for lineno, line in enumerate(text.splitlines(), start=1)
        if HOME_LITERAL.search(line)
    ]


def test_tracked_files_carry_no_hardcoded_home_dirs_outside_allowlist() -> None:
    hits: dict[str, list[str]] = {}
    for path in _tracked_files():
        lines = _home_literal_hits(path)
        if lines:
            hits[path.relative_to(ROOT).as_posix()] = lines

    offenders = sorted(set(hits) - set(ALLOWLIST))
    stale = sorted(set(ALLOWLIST) - set(hits))
    detail = ""
    if offenders:
        detail += (
            "\nnew hardcoded home-dir literal(s) — resolve the path at runtime"
            " or extend the commented ALLOWLIST:\n"
            + "\n".join(line for name in offenders for line in hits[name])
        )
    if stale:
        detail += (
            "\nstale ALLOWLIST entries (file no longer matches): "
            + ", ".join(stale)
        )
    assert not offenders and not stale, detail
