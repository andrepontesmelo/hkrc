"""Contract tests for the harness-loop docs/shim layer (task t_ef7bd370).

Import-free on purpose: ``hkrc.harness_loop`` may not exist yet on this branch
(the core card owns it).  These tests only assert that the reference doc, the
cron shim, and the README section exist and carry the expected contract.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PROMPT_REF = ROOT / "references" / "harness-loop-prompt.md"
ESCALATION_REF = ROOT / "references" / "orchestrator-escalation-rule.md"
CRON_SHIM = ROOT / "scripts" / "harness-loop-cron.py"
README = ROOT / "README.md"

PROMPT_SECTIONS = (
    "Dedupe protocol",
    "Apply policy",
    "Deploy-ready",
    "review-pair",
    "session-bloat",
)


def test_prompt_reference_exists() -> None:
    assert PROMPT_REF.is_file(), f"missing {PROMPT_REF}"


def test_prompt_reference_contains_key_sections() -> None:
    text = PROMPT_REF.read_text(encoding="utf-8").lower()
    for section in PROMPT_SECTIONS:
        assert section.lower() in text, f"prompt reference missing section: {section!r}"


def test_prompt_reference_supersedes_cron_prompt() -> None:
    text = PROMPT_REF.read_text(encoding="utf-8")
    assert "supersedes" in text.lower()
    assert "f69651252ba1" in text


def test_cron_shim_exists() -> None:
    assert CRON_SHIM.is_file(), f"missing {CRON_SHIM}"


def test_cron_shim_constants() -> None:
    tree = ast.parse(CRON_SHIM.read_text(encoding="utf-8"))
    constants: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant):
                constants[target.id] = node.value.value
    assert constants["WRAPPER"] == "~/.hermes/hkrc/bin/hkrc"
    assert (
        constants["CONFIG"]
        == "~/.hermes/hkrc/config/hkrc/config.toml"
    )
    assert constants["DRY_RUN"] is True


def test_cron_shim_builds_expected_command() -> None:
    source = CRON_SHIM.read_text(encoding="utf-8")
    assert '"harness-loop"' in source
    assert '"run"' in source
    assert '"--config"' in source
    assert '"--dry-run"' in source
    assert '"--no-dry-run"' in source


def test_readme_contains_harness_loop_section() -> None:
    text = README.read_text(encoding="utf-8")
    assert "## Harness learning loop (`harness-loop`)" in text
    assert "harness-loop run" in text
    assert "references/harness-loop-prompt.md" in text
    assert "f69651252ba1" in text
    assert "DRY_RUN" in text or "--dry-run" in text


def test_escalation_rule_reference_exists() -> None:
    assert ESCALATION_REF.is_file(), f"missing {ESCALATION_REF}"


def test_escalation_rule_names_senior_dev_direct_order() -> None:
    """The reference rule text exists and names the senior-dev-direct order:
    the retry-exhausted card is re-dispatched/reassigned straight to
    senior-dev — the lead-orchestrator hop is skipped (decision t_9f7cf77a,
    supersedes t_d2fb8917 #525)."""
    text = ESCALATION_REF.read_text(encoding="utf-8")
    lower = text.lower()
    assert "senior-dev" in lower
    assert "directly" in lower
    assert "lead-orchestrator" in lower
    assert "skipping" in lower or "skip" in lower
    assert "t_9f7cf77a" in text
    assert "rescue" in lower and "drop" in lower
    assert "precise reason" in lower
    assert "never silently" in lower or "silent drop" in lower


def test_harness_loop_escalation_constant_cites_decision() -> None:
    """The harness_loop ESCALATION_ASSIGNEE comment block cites this card and
    the 2026-08-14 decision, and the constant targets senior-dev directly —
    never lead-orchestrator (regression guard for t_9f7cf77a)."""
    source = (ROOT / "src" / "hkrc" / "harness_loop.py").read_text(encoding="utf-8")
    block = source.split("ESCALATION_ASSIGNEE = ", 1)[1].splitlines()[0]
    # The constant definition itself must target senior-dev.
    assert block.strip().strip('"') == "senior-dev"
    comment = source.split("ESCALATION_ASSIGNEE = ", 1)[0]
    assert "t_9f7cf77a" in comment
    assert "2026-08-14" in comment
    assert "supersedes t_d2fb8917" in comment
    assert "lead-orchestrator hop is skipped" in comment
    assert "no per-card model/reasoning" in comment
    assert "overrides" in comment
    assert "no silent drops" in comment


def test_readme_harness_loop_section_after_watcher() -> None:
    text = README.read_text(encoding="utf-8")
    watcher = text.index("## Decision-latency watcher (`watcher`)")
    harness = text.index("## Harness learning loop (`harness-loop`)")
    assert harness > watcher, "harness-loop section must come after the watcher section"
