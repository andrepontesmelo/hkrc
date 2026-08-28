"""Runtime argv-contract tests for the harness-loop cron shim.

The shim is a standalone cron script (not part of the ``hkrc`` package), so it
is loaded by path and ``subprocess.call`` is captured: these tests pin the
exact argv the installed cron job would exec — wrapper path, config path, and
the dry-run flip contract (``--dry-run`` in the shipped template,
``--no-dry-run`` once the operator flips ``DRY_RUN``).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHIM = ROOT / "scripts" / "harness-loop-cron.py"

# The shim expands these at runtime via os.path.expanduser, so mirror that
# here instead of pinning an operator-specific absolute path.
WRAPPER = str(Path("~/.hermes/hkrc/bin/hkrc").expanduser())
CONFIG = str(Path("~/.hermes/hkrc/config/hkrc/config.toml").expanduser())


def load_shim(monkeypatch: pytest.MonkeyPatch, dry_run: bool) -> tuple[ModuleType, list[str]]:
    spec = importlib.util.spec_from_file_location("harness_loop_cron_shim", SHIM)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "DRY_RUN", dry_run)

    argv: list[str] = []

    def fake_call(command: list[str], *args: object, **kwargs: object) -> int:
        argv.extend(command)
        return 0

    monkeypatch.setattr(subprocess, "call", fake_call)
    return module, argv


def test_shim_execs_canonical_wrapper_with_dry_run_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, argv = load_shim(monkeypatch, dry_run=True)
    assert module.main() == 0
    assert argv == [
        WRAPPER,
        "harness-loop",
        "run",
        "--config",
        CONFIG,
        "--dry-run",
    ]


def test_shim_flip_drops_dry_run_and_passes_no_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator flip must not just drop --dry-run: ``hkrc harness-loop run``
    defaults to dry-run, so the live shim must pass --no-dry-run explicitly."""
    module, argv = load_shim(monkeypatch, dry_run=False)
    assert module.main() == 0
    assert "--dry-run" not in argv
    assert argv[-1] == "--no-dry-run"
