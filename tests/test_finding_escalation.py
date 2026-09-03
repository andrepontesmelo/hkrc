"""Escalation ladder: render-time severity derivation (t_5f25b8a9 AC1)."""

from __future__ import annotations

from hkrc.config import ControllerConfig
from hkrc.harness_loop import (
    _entry_to_finding,
    _escalation_for_entry,
    _escalation_map,
    _render_wrong,
    fingerprint,
)

from test_harness_loop import queue_entry

NOW = 1_788_000_000
DAY = 86_400


def _entry(occ: int, *, fix_status: str = "open", severity: str = "medium") -> dict:
    entry = queue_entry(
        f"decision-latency:k_{occ}_{fix_status}",
        pattern="decision-latency",
        key=f"k_{occ}_{fix_status}",
        severity=severity,
        occurrence_count=occ,
        first_seen=NOW - 40 * DAY,
        last_seen=NOW - DAY,
        fix_status=fix_status,
    )
    # The persisted fingerprint is fingerprint() of the finding payload (the
    # real pipeline stores exactly that); derive it so render-time escalation
    # lookups match.
    entry["fingerprint"] = fingerprint(_entry_to_finding(entry))
    return entry


def _config() -> ControllerConfig:
    from pathlib import Path

    from test_harness_loop import make_config

    return make_config(Path("/tmp/escalation-unused"))


def test_escalation_derivation_thresholds() -> None:
    config = _config()
    assert _escalation_for_entry(_entry(1), config=config) is None
    assert _escalation_for_entry(_entry(6), config=config) is None
    assert _escalation_for_entry(_entry(7), config=config) == ("medium", "high", 7, False)
    assert _escalation_for_entry(_entry(8), config=config) == ("medium", "high", 8, False)
    assert _escalation_for_entry(_entry(21), config=config) == ("medium", "high", 21, True)
    assert _escalation_for_entry(_entry(29), config=config) == ("medium", "high", 29, True)


def test_escalation_skips_stale_and_caps_at_high() -> None:
    config = _config()
    assert _escalation_for_entry(_entry(29, fix_status="stale"), config=config) is None
    assert _escalation_for_entry(_entry(29, fix_status="resolved"), config=config) is None
    high = _entry(29, severity="high")
    assert _escalation_for_entry(high, config=config) == ("high", "high", 29, True)


def test_occ_presentation_renders_three_levels() -> None:
    config = _config()
    entries = [_entry(1), _entry(8), _entry(29)]
    escalation = _escalation_map(entries, config=config)
    findings = tuple(_entry_to_finding(entry) for entry in entries)
    lines = "\n".join(
        _render_wrong(findings, first_seen_by_fp={}, escalation=escalation)
    )
    assert "MEDIUM — Slow decision on blocked tasks" in lines
    assert "MEDIUM→HIGH" in lines
    assert "CHRONIC, 29 nights" in lines
    assert lines.count("MEDIUM→HIGH") == 2
    stored = [entry["severity"] for entry in entries]
    assert stored == ["medium", "medium", "medium"]


def test_stale_renders_nothing_even_when_chronic() -> None:
    config = _config()
    entry = _entry(29, fix_status="stale")
    # Stale entries never escalate (not in _OPEN_FIX_STATUSES), and the run()
    # pipeline filters them out of "What's wrong" entirely — proven e2e by
    # test_run_wayfinder_chronic_inline (exactly 3 sections, none for k_stale).
    assert _escalation_map([entry], config=config) == {}


def test_apply_kind_untouched_by_derivation() -> None:
    config = _config()
    entry = _entry(29)
    _escalation_map([entry], config=config)
    assert _entry_to_finding(entry).apply_kind == "none"
    assert entry["apply_kind"] == "none"
    assert fingerprint(_entry_to_finding(entry)) == entry["fingerprint"]
