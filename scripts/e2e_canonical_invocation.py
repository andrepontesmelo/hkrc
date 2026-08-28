#!/usr/bin/env python3
"""Exact canonical needs-input-watcher invocation E2E against a real blocked task.

Runs ``hermes -p developer chat -q <rendered prompt> --max-turns 4 --yolo -Q
--reasoning none`` exactly as HKRC's ``build_llm_command``/``build_llm_environment``
produce it, with stdout and stderr captured separately. Prints the raw evidence
plus a pass/fail verdict on the stdout-purity contract (no reasoning/transcript/
query echo/max-iterations/banner/box-drawing/exit chrome on stdout).

Run from the repo root so ``hkrc`` is importable: ``uv run python
scripts/e2e_canonical_invocation.py``. Point TASK_ID/BOARD_SLUG at a currently
blocked ``needs_input`` card for a meaningful summary; the stdout-purity verdict
holds regardless.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess

from hkrc.needs_input_watcher import _LLM_OUTPUT_CHROME_MARKERS

PROMPT_FILE = "~/.hermes/hkrc/config/hkrc/needs-input-watcher-prompt.txt"
TASK_ID = "t_00844f60"
BOARD_SLUG = "hkrc"
CLI = "hermes"
PROFILE = "developer"
MAX_TURNS = "4"
TIMEOUT_SECONDS = 100

# The canonical needs-input-watcher environment (fix-parent t_5c1068b9 contract):
# HOME/HERMES_HOME pinned to the main Hermes instance, _HERMES_GATEWAY and
# every HERMES_KANBAN_* variable scrubbed so the nested CLI cannot hijack the
# gateway live stream, boot into kanban goal-loop mode, or record a timed_out
# event against the parent card.
PINNED_HOME = os.path.expanduser("~")
PINNED_HERMES_HOME = os.path.expanduser("~/.hermes")

# Chrome markers that must never appear on stdout (DEF-001 contract). Single
# source of truth: hkrc.needs_input_watcher._LLM_OUTPUT_CHROME_MARKERS — the
# production run_llm validator uses the same set, so a leak this script flags
# is a leak the deployed watchdog rejects too.


def build_environment() -> dict[str, str]:
    """Replicate ``hkrc.needs_input_watcher.build_llm_environment`` exactly.

    HOME/HERMES_HOME are pinned to the main instance; ``_HERMES_GATEWAY`` and
    every ``HERMES_KANBAN_*`` variable are scrubbed from the child env.
    """
    env = dict(os.environ)
    env.pop("_HERMES_GATEWAY", None)
    for key in list(env):
        if key.startswith("HERMES_KANBAN_"):
            env.pop(key, None)
    env["HOME"] = PINNED_HOME
    env["HERMES_HOME"] = PINNED_HERMES_HOME
    return env


def render_prompt() -> str:
    template = Path(os.path.expanduser(PROMPT_FILE)).read_text(encoding="utf-8")
    return template.format(task_id=TASK_ID, board_slug=BOARD_SLUG)


def main() -> int:
    prompt = render_prompt()
    command = [
        CLI, "-p", PROFILE, "chat", "-q", prompt,
        "--max-turns", MAX_TURNS, "--yolo", "-Q", "--reasoning", "none",
    ]
    print("COMMAND:")
    print("  " + " ".join(command))
    print("PROMPT RENDERED (first 200 chars):")
    print("  " + prompt[:200].replace("\n", "\\n"))
    env = build_environment()
    print("ENV:")
    print(f"  HOME={env.get('HOME')}")
    print(f"  HERMES_HOME={env.get('HERMES_HOME')}")
    print(f"  _HERMES_GATEWAY present: {'_HERMES_GATEWAY' in env}")
    kanban_vars = sorted(k for k in env if k.startswith("HERMES_KANBAN_"))
    print(f"  HERMES_KANBAN_* present: {kanban_vars}")

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=TIMEOUT_SECONDS,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        print("TIMEOUT")
        print("stdout so far:", (exc.stdout or "")[-2000:])
        print("stderr so far:", (exc.stderr or "")[-2000:])
        return 2

    print("=" * 60)
    print("RETURNCODE:", completed.returncode)
    print("STDOUT_BYTES:", len(completed.stdout.encode("utf-8")))
    print("STDERR_BYTES:", len(completed.stderr.encode("utf-8")))
    print("=" * 60)
    print("--- STDOUT START ---")
    print(completed.stdout)
    print("--- STDOUT END ---")
    print("--- STDERR START ---")
    print(completed.stderr)
    print("--- STDERR END ---")

    verdicts: list[str] = []
    stdout_lower = completed.stdout
    for marker in _LLM_OUTPUT_CHROME_MARKERS:
        if marker in stdout_lower:
            verdicts.append(f"LEAK: stdout contains {marker!r}")
    stripped = completed.stdout.strip()
    if not stripped:
        verdicts.append("EMPTY STDOUT")
    if completed.returncode != 0:
        verdicts.append(f"NONZERO EXIT {completed.returncode}")
    print("=" * 60)
    if verdicts:
        print("VERDICT: FAIL")
        for line in verdicts:
            print("  " + line)
        return 1
    print("VERDICT: PASS — stdout is final-response-only, stderr may hold session_id")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
