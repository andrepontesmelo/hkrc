"""End-to-end run() contract: retention, basis line, escalation, ledger round-trip."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from hkrc.harness_loop import (
    _entry_to_finding,
    _escalation_map,
    _render_wrong,
    fingerprint,
    load_state,
    prune_stale_entries,
    run,
)

from test_harness_loop import make_config, make_hkrc_repo, queue_entry

NOW = 1_788_000_000
DAY = 86_400
REAL_LEDGER = Path(
    "/home/example-user/.hermes/hkrc/state/hkrc/harness-loop-state.json"
)


def _seed(tmp_path: Path, entries: list[dict]) -> tuple[Path, Path]:
    state_file = tmp_path / "state" / "hkrc" / "harness-loop-state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW - DAY,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": entries,
            }
        ),
        encoding="utf-8",
    )
    return state_file, state_file.parent


def _entry(
    fp: str,
    *,
    occ: int,
    fix_status: str = "open",
    last_seen: int = NOW - 2 * DAY,
    severity: str = "medium",
    pattern: str = "decision-latency",
) -> dict:
    return queue_entry(
        fp,
        pattern=pattern,
        key=fp,
        severity=severity,
        occurrence_count=occ,
        first_seen=last_seen - 30 * DAY,
        last_seen=last_seen,
        fix_status=fix_status,
    )


def _derived_entry(
    fp: str,
    *,
    occ: int,
    fix_status: str = "open",
    last_seen: int = NOW - 2 * DAY,
    severity: str = "medium",
    pattern: str = "decision-latency",
) -> dict:
    """Fixture entry whose stored fingerprint is the REAL derived one.

    The live pipeline persists ``fingerprint(finding)`` (see
    ``_upsert_open_finding``); render-time escalation looks up exactly that,
    so fixtures used for rendering must store the derived fingerprint.
    """
    entry = _entry(
        fp, occ=occ, fix_status=fix_status, last_seen=last_seen,
        severity=severity, pattern=pattern,
    )
    entry["fingerprint"] = fingerprint(_entry_to_finding(entry))
    return entry


def _open_proof(tmp_path: Path, entries: list[dict]) -> None:
    """Write the verify file every open fixture entry points at."""
    proof = tmp_path / "proof" / "verify.txt"
    proof.parent.mkdir(parents=True, exist_ok=True)
    proof.write_text("FINDING-STILL-PRESENT marker\n", encoding="utf-8")
    for entry in entries:
        entry["verify_path"] = str(proof)
        entry["verify_text"] = "FINDING-STILL-PRESENT marker"


def _run_report(tmp_path: Path, entries: list[dict]) -> str:
    repo = make_hkrc_repo(tmp_path)
    config = make_config(
        tmp_path, sessions_db=tmp_path / "no-sessions.db", hkrc_repo=repo
    )
    state_file, _ = _seed(tmp_path, entries)
    return run(config, now=NOW, dry_run=True, state_path=state_file)


def test_run_wayfinder_chronic_inline(tmp_path: Path) -> None:
    """AC1/AC3/AC6 e2e: CHRONIC inline; stored severity intact; dry-run
    leaves the ledger byte-identical; the live run prunes behind a backup."""
    entries = [
        _derived_entry("k1", occ=1),
        _derived_entry("k8", occ=8),
        _derived_entry("k29", occ=29),
        _derived_entry(
            "k_stale", occ=29, fix_status="stale", last_seen=NOW - 45 * DAY
        ),
    ]
    _open_proof(tmp_path, entries)
    import hashlib

    repo = make_hkrc_repo(tmp_path)
    config = make_config(
        tmp_path, sessions_db=tmp_path / "no-sessions.db", hkrc_repo=repo
    )
    state_file, _ = _seed(tmp_path, entries)
    before_hash = hashlib.sha256(state_file.read_bytes()).hexdigest()

    # Dry-run: report only — no prune line, no backup, byte-identical file.
    report = run(config, now=NOW, dry_run=True, state_path=state_file)
    assert "MEDIUM→HIGH (CHRONIC, 29 nights)" in report
    assert "MEDIUM→HIGH (8 nights)" in report
    assert "MEDIUM — Slow decision on blocked tasks" in report
    assert "pruned" not in report
    assert (
        hashlib.sha256(state_file.read_bytes()).hexdigest() == before_hash
    ), "dry-run must not mutate the state file"
    assert not list((tmp_path / "state" / "hkrc").glob("*.backup-*.json"))

    # Live run: stale entry pruned behind a one-time backup; the working
    # set keeps exactly the 3 open entries; stored severity untouched.
    live_report = run(config, now=NOW, dry_run=False, state_path=state_file)
    assert "pruned 1 stale finding older than 14d" in live_report
    by_key = {
        entry["key"]: entry
        for entry in load_state(state_file)["open_findings"]
    }
    # Stored severity is the detector's verdict: never rewritten (AC4).
    assert by_key["k29"]["severity"] == "medium"
    assert by_key["k29"]["occurrence_count"] == 29
    assert by_key["k1"]["severity"] == "medium"
    assert by_key["k8"]["apply_kind"] == "none"
    assert set(by_key) == {"k1", "k8", "k29"}
    backups = list((tmp_path / "state" / "hkrc").glob("*.backup-*.json"))
    assert len(backups) == 1
    backup_state = json.loads(backups[0].read_text(encoding="utf-8"))
    assert len(backup_state["open_findings"]) == 4


def test_counts_line_states_basis(tmp_path: Path) -> None:
    """AC8 e2e: the counts line states open/deferred basis + ledger total."""
    entries = [_derived_entry("k1", occ=1), _derived_entry("k2", occ=2)]
    _open_proof(tmp_path, entries)
    report = _run_report(tmp_path, entries)
    assert "2 open/deferred findings in the working set" in report
    assert "(2 total ledger entries; counts cover the" in report
    assert "open/deferred working set only)" in report


def test_round_trip_on_copy_of_real_ledger(tmp_path: Path) -> None:
    """AC3: on a COPY of the real ledger, prune keeps open+resolved intact.

    Never mutates the live file; every pruned row was stale AND past the
    retention cutoff on last_seen; the copy reloads as valid JSON.
    """
    if not REAL_LEDGER.is_file():
        import pytest

        pytest.skip("live harness-loop-state.json not present on this machine")
    work = tmp_path / "state" / "hkrc"
    work.mkdir(parents=True, exist_ok=True)
    ledger_copy = work / "harness-loop-state.json"
    shutil.copyfile(REAL_LEDGER, ledger_copy)
    before = json.loads(ledger_copy.read_text(encoding="utf-8"))
    before_of = before["open_findings"]
    import time

    now = int(time.time())
    cutoff = now - 14 * DAY
    open_fps = {
        e["fingerprint"] for e in before_of if e.get("fix_status") == "open"
    }
    resolved_fps = {
        e["fingerprint"] for e in before_of if e.get("fix_status") == "resolved"
    }
    state = load_state(ledger_copy)
    pruned, backup = prune_stale_entries(
        state, ledger_copy, retention_days=14, now=now
    )
    # Structural integrity: valid dicts, unique fingerprints, no corruption.
    after = state["open_findings"]
    fps = [e["fingerprint"] for e in after]
    assert len(fps) == len(set(fps)) == len(after)
    assert all(isinstance(e, dict) and "fix_status" in e for e in after)
    # Open + resolved survive unconditionally.
    assert open_fps <= set(fps)
    assert resolved_fps <= set(fps)
    # Every pruned row was stale and past the cutoff.
    by_fp = {e["fingerprint"]: e for e in before_of}
    before_fps = {e["fingerprint"] for e in before_of}
    assert pruned == len(before_fps - set(fps))
    for fp in before_fps - set(fps):
        entry = by_fp[fp]
        assert entry["fix_status"] == "stale"
        assert entry["last_seen"] < cutoff
    # Backup written next to the copy, full 210-entry content.
    assert backup is not None and backup.is_file() and backup.parent == work
    backup_state = json.loads(backup.read_text(encoding="utf-8"))
    assert len(backup_state["open_findings"]) == len(before_of)
    # The live file was never touched.
    live_now = json.loads(REAL_LEDGER.read_text(encoding="utf-8"))
    assert len(live_now["open_findings"]) == len(before_of)
    # Measured live effect (2026-09-01): 14d prunes 126 of 210 — never zero.
    stale_count = sum(1 for e in before_of if e.get("fix_status") == "stale")
    assert 0 < pruned <= stale_count


def test_default_retention_not_a_noop_on_real_age_distribution(tmp_path: Path) -> None:
    """Mandatory no-op guard: fixture built from MEASURED live stale ages.

    Live stale last_seen ages (2026-09-01): min 0.8d, median 17.1d, max
    20.6d.  A 30d retention prunes ZERO of these — the old default would be
    a silent no-op forever.  The 14d default must prune the 17d and 20.6d
    masses and keep the 0.8d tail.
    """
    from hkrc.harness_loop import HarnessLoopConfig, prune_stale_entries as prune

    assert HarnessLoopConfig().stale_retention_days == 14
    entries = [
        _entry("fp_0.8d", occ=30, fix_status="stale", last_seen=NOW - int(0.8 * DAY)),
        _entry("fp_17d", occ=30, fix_status="stale", last_seen=NOW - int(17.1 * DAY)),
        _entry("fp_20.6d", occ=30, fix_status="stale", last_seen=NOW - int(20.6 * DAY)),
        _entry("fp_open", occ=29, fix_status="open", last_seen=NOW - 400 * DAY),
    ]
    state_file, _ = _seed(tmp_path, entries)
    state = load_state(state_file)
    # The rejected 30d default really is a no-op on this distribution:
    pruned_30, _ = prune(state, state_file, retention_days=30, now=NOW)
    assert pruned_30 == 0
    # The shipped 14d default is not:
    pruned_14, backup = prune(state, state_file, retention_days=14, now=NOW)
    assert pruned_14 == 2
    kept = {e["fingerprint"] for e in state["open_findings"]}
    assert kept == {"fp_0.8d", "fp_open"}
    assert backup is not None


def test_operator_preview_eight_live_open_entries() -> None:
    """Derivation reproduces the operator's measured preview exactly."""
    from hkrc.config import ControllerConfig
    from hkrc.harness_loop import _escalation_for_entry

    from test_harness_loop import make_config
    from pathlib import Path

    config: ControllerConfig = make_config(Path("/tmp/preview-unused"))
    shapes = [
        ("decision-latency", "wayfinder", 29, "medium"),
        ("config-drift", "model.default", 18, "low"),
        ("retry-exhaustion", "casa-gungalilin:t_4135c346", 9, "high"),
        ("retry-exhaustion", "casa-gungalilin:t_57a8f2c2", 9, "high"),
        ("retry-exhaustion", "hermes-agent:t_6dc881ca", 8, "high"),
        ("retry-exhaustion", "hkrc:t_ae960b7d", 8, "high"),
        ("decision-latency", "casa-gungalilin", 2, "medium"),
        ("retry-exhaustion", "casa-gungalilin:t_13d3bbfa", 1, "high"),
    ]
    entries = [
        _derived_entry(
            f"{pattern}:{key}",
            occ=occ,
            severity=severity,
            pattern=pattern,
        )
        for pattern, key, occ, severity in shapes
    ]
    derived = [_escalation_for_entry(e, config=config) for e in entries]
    assert derived[0] == ("medium", "high", 29, True)  # -> HIGH (CHRONIC, 29 nights)
    assert derived[1] == ("low", "medium", 18, False)  # -> LOW -> MEDIUM
    assert derived[2] == ("high", "high", 9, False)  # HIGH (already max, streak shown)
    assert derived[3] == ("high", "high", 9, False)
    assert derived[4] == ("high", "high", 8, False)
    assert derived[5] == ("high", "high", 8, False)
    assert derived[6] is None  # MEDIUM unchanged
    assert derived[7] is None  # HIGH unchanged
    # Rendering: escalation-aware grouping gives each level its own section.
    # The decisive three render together (within the 5-section budget):
    subset = entries[:3]
    escalation3 = _escalation_map(subset, config=config)
    findings3 = tuple(_entry_to_finding(e) for e in subset)
    lines3 = "\n".join(
        _render_wrong(findings3, first_seen_by_fp={}, escalation=escalation3)
    )
    assert "MEDIUM→HIGH (CHRONIC, 29 nights)" in lines3
    assert "LOW→MEDIUM (18 nights)" in lines3
    assert "HIGH (9 nights)" in lines3
    # And the full live set still renders the chronic streak inline.
    escalation = _escalation_map(entries, config=config)
    findings = tuple(_entry_to_finding(e) for e in entries)
    lines = "\n".join(
        _render_wrong(findings, first_seen_by_fp={}, escalation=escalation)
    )
    assert "MEDIUM→HIGH (CHRONIC, 29 nights)" in lines

