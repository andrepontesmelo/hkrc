#!/usr/bin/env python3
"""Cron ``no_agent`` shim for ``hkrc same-file-guard``.

Mirrors the watcher shim exactly: execs the installed instance wrapper,
prints nothing when the guard has nothing new (cron ``no_agent`` delivers
stdout verbatim, so silence = no Telegram ping).

First-deploy contract: run with ``--dry-run`` until the operator has
reviewed 24h of dry-run logs, then remove the flag to go live.  This file
is the template; after installing the release into the instance, copy it
next to the watcher shim (e.g. ``~/.hermes/profiles/main/scripts/``) and
register the cron job (``kanban same-file guard`` in cron_manifest.json).
Do NOT edit the paths in a copy — the wrapper is
``<instance-root>/bin/hkrc`` and the config ``<instance-root>/config/hkrc/config.toml``.
"""

from __future__ import annotations

import os
import subprocess

WRAPPER = "~/.hermes/hkrc/bin/hkrc"
CONFIG = "~/.hermes/hkrc/config/hkrc/config.toml"
# Keep --dry-run until the 24h dry-run log review is done; then remove it.
DRY_RUN = True


def main() -> int:
    command = [os.path.expanduser(WRAPPER), "same-file-guard", "--config", os.path.expanduser(CONFIG)]
    if DRY_RUN:
        command.append("--dry-run")
    return subprocess.call(command)


if __name__ == "__main__":
    raise SystemExit(main())
