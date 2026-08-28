#!/usr/bin/env python3
"""Cron ``no_agent`` shim for ``hkrc stale-block-watch`` (v0.14.0).

Mirrors the needs-input-watcher shim exactly: execs the installed instance
wrapper, prints nothing when the watchdog has nothing new (cron ``no_agent``
delivers stdout verbatim, so silence = no Telegram ping).

First-deploy contract: run ``hkrc stale-block-watch --config ...`` manually
once to verify the digest renders, then let the cron job take over.  The
manifest job (``kanban stale block watch``, every 10m) is created by
``hkrc crons sync`` after this shim is copied next to the needs-input-watcher
shim (e.g. ``~/.hermes/profiles/main/scripts/stale-block-watch.py``).  Do NOT
edit the paths in a copy — the wrapper is ``<instance-root>/bin/hkrc`` and the
config ``<instance-root>/config/hkrc/config.toml``.
"""

from __future__ import annotations

import os
import subprocess
import sys

INSTANCE_ROOT = os.path.expanduser("~/.hermes/profiles/main/hkrc")
WRAPPER = os.path.join(INSTANCE_ROOT, "bin", "hkrc")
CONFIG = os.path.join(INSTANCE_ROOT, "config", "hkrc", "config.toml")


def main() -> int:
    env = dict(os.environ)
    env["HKRC_INSTANCE_ROOT"] = INSTANCE_ROOT
    try:
        completed = subprocess.run(
            [WRAPPER, "stale-block-watch", "--config", CONFIG],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        print(f"stale-block-watch shim: cannot exec {WRAPPER}: {exc}", file=sys.stderr)
        return 2
    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
