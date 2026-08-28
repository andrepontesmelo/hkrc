#!/usr/bin/env python3
"""Cron ``no_agent`` shim for ``hkrc harness-loop`` (v0.15.1+).

Mirrors the watcher/needs-input-watcher shims exactly: execs the installed instance
wrapper, prints nothing when the loop has nothing new (cron ``no_agent``
delivers stdout verbatim, so silence = no Telegram ping).

Canonical instance: ``<instance-root>`` = ``~/.hermes/hkrc`` (the release
install root; ``bin/hkrc`` is the wrapper and ``config/hkrc/config.toml`` the
config). Install this file as ``~/.hermes/scripts/harness-loop.py`` and register
the cron job (daily at 03:00, deliver telegram) — the manifest job
``harness-learning-loop (daily 7-day self-review)`` in
``config/hkrc/cron_manifest.json`` reconciles it via ``hkrc crons sync``; the
job name matches the live Hermes cron job ``f69651252ba1`` so sync updates it
in place, and the manifest schedule is
``0 3 * * *`` (once daily at 03:00 America/Vancouver).
Do NOT edit the paths in a copy — the wrapper is ``<instance-root>/bin/hkrc``
and the config ``<instance-root>/config/hkrc/config.toml``.
"""

from __future__ import annotations

import os
import subprocess

WRAPPER = "~/.hermes/hkrc/bin/hkrc"
CONFIG = "~/.hermes/hkrc/config/hkrc/config.toml"
# ---------------------------------------------------------------------------
# FIRST-DEPLOY CONTRACT: the repo copy ships with DRY_RUN = True.  The operator
# installs this file as ~/.hermes/scripts/harness-loop.py, reviews 24h of dry-run
# output, then flips DRY_RUN to False in the INSTALLED copy (never the repo).
# DRY_RUN = False passes --no-dry-run because `hkrc harness-loop run` DEFAULTS
# to dry-run (zero applies); dropping --dry-run alone would silently stay dry.
# ---------------------------------------------------------------------------
DRY_RUN = True


def main() -> int:
    command = [os.path.expanduser(WRAPPER), "harness-loop", "run", "--config", os.path.expanduser(CONFIG)]
    if DRY_RUN:
        command.append("--dry-run")
    else:
        command.append("--no-dry-run")
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
