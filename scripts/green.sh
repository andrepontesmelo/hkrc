#!/usr/bin/env bash
# Full test gate: run the whole pytest suite green from this checkout.
#
# In a git worktree the .venv is a symlink to the main checkout's venv and
# the editable hkrc install maps to the PARENT src tree, so src/ must be
# prepended to PYTHONPATH (pytest would otherwise silently test stale code).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -e .venv ]; then
    main_root="$(git rev-parse --path-format=absolute --git-common-dir)"
    main_root="$(dirname "$main_root")"
    ln -s "$main_root/.venv" .venv
fi

PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}" exec .venv/bin/python -m pytest "$@"
