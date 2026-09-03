# RED-first fixtures for the archloop skip-streak detector (t_ba4092e4).

from __future__ import annotations

import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.harness_loop import detect_archloop_skip_streak

NIGHT = "2026-09-01 00:30:23"
DAY = 86_400
SUMMARY = (
    "archloop-night {stamp}\n"
    "SKIPPED no-new-commits (3): andre-archloop rentcli vtcli\n"
    "SKIPPED dirty (2): {dirty}\n"
    "SKIPPED not-on-main (1): rentcli-wt-realtorca\n"
    "SKIPPED board-archived (3): cockpit night-run-explorer show-dex\n"
    "(nothing started this night)"
)


def make_reports(root: Path, nights: list[str], *, dirty: str = "campcli") -> Path:
    """One report file per night; the trailing summary block is what parses."""
    root.mkdir(parents=True, exist_ok=True)
    for stamp in nights:
        date = stamp.split()[0]
        path = root / f"{date}.md"
        path.write_text(
            f"# Cron Job: hkrc archloop nightly\n\n{SUMMARY.format(stamp=stamp, dirty=dirty)}\n",
            encoding="utf-8",
        )
    return root


def test_seventeen_night_dirty_streak_is_one_high_finding(tmp_path: Path) -> None:
    reports = make_reports(tmp_path / "cron-output", [f"2026-08-{day:02d} 00:30:00" for day in range(1, 18)])
    findings = detect_archloop_skip_streak(
        reports,
        actionable_classes=("dirty",),
        medium_nights=3,
        high_nights=7,
    )
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "archloop-skip-streak"
    assert finding.key == "campcli"
    assert finding.severity == "high"
    assert finding.apply_kind == "none"
    evidence = "\n".join(finding.evidence)
    assert "campcli" in evidence and "dirty" in evidence
    assert "17" in evidence and "2026-08-01" in evidence
    assert "report file" in evidence


def test_no_new_commits_streak_is_never_a_finding(tmp_path: Path) -> None:
    root = tmp_path / "cron-output"
    root.mkdir()
    for day in range(1, 18):
        stamp = f"2026-08-{day:02d} 00:30:00"
        (root / f"2026-08-{day:02d}.md").write_text(
            "archloop-night " + stamp
            + "\nSKIPPED no-new-commits (2): andre-archloop rentcli\n"
            "(nothing started this night)\n",
            encoding="utf-8",
        )
    assert (
        detect_archloop_skip_streak(
            root, actionable_classes=("dirty",), medium_nights=3, high_nights=7
        )
        == ()
    )


def test_not_on_main_is_not_actionable_by_default(tmp_path: Path) -> None:
    reports = make_reports(tmp_path / "cron-output", [NIGHT] * 17)
    findings = detect_archloop_skip_streak(
        reports,
        actionable_classes=("dirty",),
        medium_nights=3,
        high_nights=7,
    )
    assert all(f.key != "rentcli-wt-realtorca" for f in findings)


def test_gap_between_report_files_does_not_inflate_streak(tmp_path: Path) -> None:
    # 4 consecutive files, a cron outage (missing calendar nights), then 4
    # more.  Consecutive REPORT FILES = 8 -> high; calendar-day span = 12.
    # The evidence must say 8, proving the outage did not inflate anything.
    nights = [f"2026-08-{day:02d} 00:30:00" for day in (1, 2, 3, 4)]
    nights += [f"2026-08-{day:02d} 00:30:00" for day in (9, 10, 11, 12)]
    reports = make_reports(tmp_path / "cron-output", nights)
    findings = detect_archloop_skip_streak(
        reports,
        actionable_classes=("dirty",),
        medium_nights=3,
        high_nights=7,
    )
    assert len(findings) == 1
    assert findings[0].severity == "high"
    evidence = "\n".join(findings[0].evidence)
    assert "for 8 consecutive" in evidence and "for 12 consecutive" not in evidence


def test_streak_thresholds_are_config_driven(tmp_path: Path) -> None:
    nights = [f"2026-08-{day:02d} 00:30:00" for day in range(1, 5)]
    reports = make_reports(tmp_path / "cron-output", nights)
    strict = detect_archloop_skip_streak(
        reports, actionable_classes=("dirty",), medium_nights=5, high_nights=9
    )
    assert strict == ()
    loose = detect_archloop_skip_streak(
        reports, actionable_classes=("dirty",), medium_nights=2, high_nights=9
    )
    assert len(loose) == 1 and loose[0].severity == "medium"


def test_missing_dir_yields_zero_findings_without_raising(tmp_path: Path) -> None:
    assert (
        detect_archloop_skip_streak(
            tmp_path / "nope",
            actionable_classes=("dirty",),
            medium_nights=3,
            high_nights=7,
        )
        == ()
    )


def test_empty_dir_yields_zero_findings(tmp_path: Path) -> None:
    root = tmp_path / "cron-output"
    root.mkdir()
    assert (
        detect_archloop_skip_streak(
            root, actionable_classes=("dirty",), medium_nights=3, high_nights=7
        )
        == ()
    )


def test_fingerprint_key_is_repo_name_and_occurrences_track_across_nights(
    tmp_path: Path,
) -> None:
    from hkrc.harness_loop import dedupe, fingerprint

    nights = [f"2026-08-{day:02d} 00:30:00" for day in range(1, 5)]
    reports = make_reports(tmp_path / "cron-output", nights)
    first = detect_archloop_skip_streak(
        reports, actionable_classes=("dirty",), medium_nights=3, high_nights=7
    )
    assert [fingerprint(f) for f in first] == ["archloop-skip-streak:campcli"]
    # Same repo flagged again the next night -> same fingerprint, the
    # persistent open-findings queue bumps occurrence_count instead of
    # duplicating the entry.
    make_reports(tmp_path / "cron-output", ["2026-09-02 00:30:23"], dirty="campcli")
    again = detect_archloop_skip_streak(
        tmp_path / "cron-output",
        actionable_classes=("dirty",),
        medium_nights=3,
        high_nights=7,
    )
    state: dict = {}
    _, updated = dedupe(tuple(first), state, now=100_000)
    _, updated = dedupe(tuple(again), updated, now=100_000 + DAY)
    entries = [
        entry
        for entry in updated["open_findings"]
        if entry["fingerprint"] == "archloop-skip-streak:campcli"
    ]
    assert len(entries) == 1
    assert int(entries[0]["occurrence_count"]) == 2


def test_malformed_report_files_are_skipped_not_fatal(tmp_path: Path) -> None:
    nights = [f"2026-08-{day:02d} 00:30:00" for day in range(1, 6)]
    root = make_reports(tmp_path / "cron-output", nights)
    (root / "2026-08-99.md").write_text("", encoding="utf-8")
    (root / "not-a-report.txt").write_text("archloop-night garbage", encoding="utf-8")
    (root / "truncated.md").write_text(
        "archloop-night 2026-09-02 00:00:00\nSKIPPED dirty", encoding="utf-8"
    )
    findings = detect_archloop_skip_streak(
        root, actionable_classes=("dirty",), medium_nights=3, high_nights=7
    )
    assert len(findings) == 1 and findings[0].severity == "medium"


def test_resolve_archloop_output_dir_config_then_env_then_default(tmp_path: Path, monkeypatch) -> None:
    from hkrc.harness_loop import (
        DEFAULT_ARCHLOOP_OUTPUT_DIR,
        _archloop_output_dir,
    )
    from hkrc.config import ControllerConfig, HarnessLoopConfig

    monkeypatch.delenv("HKRC_ARCHLOOP_OUTPUT_DIR", raising=False)
    config = ControllerConfig(
        "t",
        tmp_path / "boards",
        tmp_path / "state.sqlite3",
        harness_loop=HarnessLoopConfig(archloop_output_dir=str(tmp_path / "cfg")),
    )
    assert _archloop_output_dir(config) == Path(tmp_path / "cfg")
    empty = ControllerConfig(
        "t",
        tmp_path / "boards",
        tmp_path / "state.sqlite3",
        harness_loop=HarnessLoopConfig(archloop_output_dir=""),
    )
    monkeypatch.setenv("HKRC_ARCHLOOP_OUTPUT_DIR", str(tmp_path / "env"))
    assert _archloop_output_dir(empty) == Path(tmp_path / "env")
    monkeypatch.delenv("HKRC_ARCHLOOP_OUTPUT_DIR")
    assert _archloop_output_dir(empty) == Path(DEFAULT_ARCHLOOP_OUTPUT_DIR)
    # Never derived from $HOME or the sessions-db path (t_ae960b7d class).
    assert "andre" not in str(_archloop_output_dir(empty)) or str(
        _archloop_output_dir(empty)
    ) == DEFAULT_ARCHLOOP_OUTPUT_DIR
