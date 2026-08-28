#!/usr/bin/env python3
"""Cron ``no_agent`` shim for ``hkrc watcher`` (v0.9.0).

Mirrors the needs-input-watcher shim exactly: execs the installed instance wrapper,
prints nothing when the watcher has nothing new (cron ``no_agent`` delivers
stdout verbatim, so silence = no Telegram ping).

First-deploy contract: run with ``--dry-run`` until the operator has reviewed
24h of dry-run logs, then remove the flag to go live.  This file is the
template; after installing release 0.9.0 into the instance, copy it next to
the needs-input-watcher shim (e.g. ``~/.hermes/profiles/main/scripts/watcher.py``)
and register the cron job.  Do NOT edit the paths in a copy — the wrapper is
``<instance-root>/bin/hkrc`` and the config ``<instance-root>/config/hkrc/config.toml``.
"""

from __future__ import annotations

import os
import subprocess

WRAPPER = "~/.hermes/profiles/main/hkrc/bin/hkrc"
CONFIG = "~/.hermes/profiles/main/hkrc/config/hkrc/config.toml"
# Keep --dry-run until the 24h dry-run log review is done; then remove it.
DRY_RUN = True


def main() -> int:
    command = [os.path.expanduser(WRAPPER), "watcher", "--config", os.path.expanduser(CONFIG)]
    if DRY_RUN:
        command.append("--dry-run")
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
