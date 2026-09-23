"""Shipped-fix retro pre-pass contract (task t_85199c5e, design item #6).

Covers the deterministic pre-pass that runs inside the 03:00 harness-learning
loop: the shipped-fix register, the three-way classifier, the rolling metrics,
the typed detector_requirement ledger entries (and their dormancy), the
one-shot backfill, and the report-only / never-a-card guarantee.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
from typing import Any

from hkrc import shipped_fix_retro as retro
import pytest

from hkrc.cli import main as cli_main
from hkrc.config import write_config
from hkrc.harness_loop import (
    _OPEN_FIX_STATUSES,
    fingerprint as loop_fingerprint,
    load_state,
    rank_open_findings,
    run as run_harness_loop,
)

from test_harness_loop import (
    make_config,
    make_hkrc_repo,
    make_runner,
    make_ticket_runner,
    queue_entry,
)

NOW = 1_788_000_000
DAY = 86_400

# Live instance evidence for the acceptance criterion that names a real fix
# (408e985).  Read-only: the backfill runs with dry_run defaulted True.
# Home-relative on purpose (tests/test_portable_paths.py forbids a literal
# operator home dir in any tracked file).
_HOME = Path.home()
REAL_REPO = _HOME / "git" / "hermes-kanban-recovery-controller"
REAL_LEDGER = (
    _HOME / ".hermes" / "hkrc" / "state" / "hkrc" / "harness-loop-state.json"
)
REAL_BOARD_DB = _HOME / ".hermes" / "kanban" / "boards" / "hkrc" / "kanban.db"
REAL_RELEASES = _HOME / ".hermes" / "hkrc" / "releases"


# --- fixtures ---------------------------------------------------------------


def _git(repo: Path, *argv: str, when: int | None = None) -> None:
    env = dict(os.environ)
    if when is not None:
        stamp = f"{int(when)} +0000"
        env["GIT_AUTHOR_DATE"] = stamp
        env["GIT_COMMITTER_DATE"] = stamp
    subprocess.run(
        ["git", "-C", str(repo), *argv],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


def _head_branch(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--abbrev-ref", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def make_shipped_repo(tmp_path: Path, merges: list[tuple[str, int]]) -> Path:
    """Real git repo with one ``--no-ff`` merge commit per ``(subject, ts)``."""
    repo = make_hkrc_repo(tmp_path)
    base = _head_branch(repo)
    for index, (subject, ts) in enumerate(merges):
        side = f"side{index}"
        _git(repo, "checkout", "-q", "-b", side, base, when=ts)
        (repo / f"work{index}.txt").write_text(f"{index}\n", encoding="utf-8")
        _git(repo, "add", "-A", when=ts)
        _git(repo, "commit", "-qm", f"work {index}", when=ts)
        _git(repo, "checkout", "-q", base, when=ts)
        _git(repo, "merge", "--no-ff", "-m", subject, side, when=ts)
    return repo


def make_board_db(path: Path, rows: list[tuple[str, str, str, str]]) -> Path:
    """Minimal ``tasks`` table carrying id/title/body/idempotency_key."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "create table tasks (id text primary key, title text, body text, "
            "idempotency_key text)"
        )
        connection.executemany("insert into tasks values (?, ?, ?, ?)", rows)
        connection.commit()
    finally:
        connection.close()
    return path


def make_release(release_root: Path, version: str, installed_at: int) -> Path:
    entry = release_root / version
    entry.mkdir(parents=True, exist_ok=True)
    (entry / "release.json").write_text(
        json.dumps({"version": version, "installed_at": installed_at}),
        encoding="utf-8",
    )
    return entry


def seed_state(path: Path, entries: list[dict[str, Any]], *, last_run: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": last_run,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": entries,
            }
        ),
        encoding="utf-8",
    )
    return path


def _runner(stdout: str, code: int = 0, stderr: str = ""):
    def run_fn(argv):
        return retro._RunResult(code, stdout, stderr)

    return run_fn


# --- register ---------------------------------------------------------------


def test_parse_register_keeps_kanban_merges_only() -> None:
    log = "\n".join(
        [
            f"{NOW}|aaa1111|merge: reask detector (kanban t_aaaaaaaa)",
            f"{NOW}|bbb2222|merge: pair (kanban t_bbbbbbbb, t_cccccccc)",
            f"{NOW}|ccc3333|merge: impl (review t_dddddddd, impl t_eeeeeeee, "
            "DEF-t_eeeeeeee-1)",
            f"{NOW}|ddd4444|fix: not a merge (kanban t_ffffffff)",
            f"{NOW}|eee5555|Merge branch 'main' into side",
            f'{NOW}|fff6666|Revert "merge: reask detector (kanban t_aaaaaaaa)"',
            "",
            "garbage",
        ]
    )
    fixes = retro.parse_register(log)
    assert [fix.sha for fix in fixes] == ["aaa1111", "bbb2222", "ccc3333"]
    assert fixes[1].task_ids == ("t_bbbbbbbb", "t_cccccccc")
    assert fixes[2].task_ids == ("t_dddddddd", "t_eeeeeeee")
    assert fixes[0].subject.startswith("merge: reask")


def test_parse_register_collapses_duplicate_shas() -> None:
    line = f"{NOW}|abc1234|merge: one (kanban t_aaaaaaaa)"
    assert len(retro.parse_register("\n".join([line, line]))) == 1


def test_git_merge_log_reads_real_history_and_honours_since(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [
            ("merge: old fix (kanban t_11111111)", NOW - 10 * DAY),
            ("merge: new fix (kanban t_22222222)", NOW - DAY),
        ],
    )
    every = retro.parse_register(retro.git_merge_log(repo, None))
    assert len(every) == 2
    recent = retro.parse_register(retro.git_merge_log(repo, NOW - 2 * DAY))
    assert [fix.task_ids for fix in recent] == [("t_22222222",)]
    assert abs(recent[0].ts - (NOW - DAY)) < 300


def test_git_merge_log_raises_on_failure() -> None:
    try:
        retro.git_merge_log(Path("/nonexistent"), None, run_fn=_runner("", 128, "boom"))
    except retro.ShippedFixRetroError as exc:
        assert "boom" in str(exc)
    else:  # pragma: no cover - the raise is the contract
        raise AssertionError("expected ShippedFixRetroError")


# --- classifier -------------------------------------------------------------


def _fix(sha: str, ts: int, subject: str, *task_ids: str) -> retro.ShippedFix:
    return retro.ShippedFix(
        sha=sha, ts=ts, subject=subject, task_ids=tuple(task_ids)
    )


def test_classify_detector_born_from_router_card_key() -> None:
    fix = _fix(
        "aaaaaaa",
        NOW - DAY,
        "merge: reask fix (kanban t_aaaaaaaa)",
        "t_aaaaaaaa",
    )
    lineage = {
        "t_aaaaaaaa": retro.CardLineage(
            task_id="t_aaaaaaaa",
            title="reask detector",
            body="",
            idempotency_key="harness-hkrc-impl:reask:s_12345678",
        )
    }
    verdict = retro.classify_fix(fix, lineage=lineage)
    assert verdict.verdict == retro.VERDICT_DETECTOR_BORN
    assert verdict.fingerprint == "reask:s_12345678"
    assert verdict.pattern == "reask"


def test_classify_detector_born_from_supervisor_authored_card_text() -> None:
    finding = queue_entry("review-required-loop:t_12345678", pattern="review-required-loop")
    fix = _fix(
        "bbbbbbb",
        NOW - DAY,
        "merge: review loop fix (kanban t_bbbbbbbb)",
        "t_bbbbbbbb",
    )
    lineage = {
        "t_bbbbbbbb": retro.CardLineage(
            task_id="t_bbbbbbbb",
            title="fix review-required-loop",
            body="ledger fingerprint review-required-loop:t_12345678 is looping",
            idempotency_key="",
        )
    }
    verdict = retro.classify_fix(fix, lineage=lineage, findings=(finding,))
    assert verdict.verdict == retro.VERDICT_DETECTOR_BORN
    assert verdict.fingerprint == "review-required-loop:t_12345678"


def test_classify_ledger_miss_counts_nights_early() -> None:
    finding = queue_entry(
        "decision-latency:k9",
        pattern="decision-latency",
        first_seen=NOW - 4 * DAY,
    )
    fix = _fix(
        "ccccccc",
        NOW - DAY,
        "merge: decision-latency guard (kanban t_cccccccc)",
        "t_cccccccc",
    )
    verdict = retro.classify_fix(fix, findings=(finding,))
    assert verdict.verdict == retro.VERDICT_LEDGER_MISS
    assert verdict.pattern == "decision-latency"
    assert verdict.nights_early == 3
    assert verdict.fingerprint == "decision-latency:k9"


def test_classify_ledger_miss_only_when_coverage_predates_the_fix() -> None:
    later = queue_entry(
        "decision-latency:k9",
        pattern="decision-latency",
        first_seen=NOW + DAY,
    )
    fix = _fix(
        "ddddddd",
        NOW - 2 * DAY,
        "merge: decision-latency guard (kanban t_dddddddd)",
        "t_dddddddd",
    )
    assert retro.classify_fix(fix, findings=(later,)).verdict == retro.VERDICT_BLIND_SPOT


def test_classify_blind_spot_proposes_detector_name() -> None:
    fix = _fix(
        "eeeeeee",
        NOW,
        "merge: fix stale assignee profile (kanban t_eeeeeeee)",
        "t_eeeeeeee",
    )
    verdict = retro.classify_fix(fix)
    assert verdict.verdict == retro.VERDICT_BLIND_SPOT
    assert verdict.pattern == "fix-stale-assignee-profile"
    assert verdict.candidate_detector == "detect_fix_stale_assignee_profile"


def test_classify_fails_safe_without_board_or_releases(tmp_path: Path) -> None:
    """Missing board db / release root: still classified, never detector-born."""
    verdict = retro.classify_register(
        [_fix("fffffff", NOW, "merge: thing (kanban t_ffffffff)", "t_ffffffff")],
        board_db=tmp_path / "missing" / "kanban.db",
        findings=(),
        release_index=(),
    )[0]
    assert verdict.verdict == retro.VERDICT_BLIND_SPOT
    assert verdict.deploy_version == ""
    assert verdict.deployed is False


def test_classify_same_night_finding_is_a_zero_night_ledger_miss() -> None:
    """Ambiguous case, resolved deliberately: a finding first seen the SAME night
    as the merge is a ledger-miss with 0 nights-early, not a blind spot.

    Calling it a blind spot would write a detector_requirement whose evidence
    says "no ledger coverage" about a pattern the ledger demonstrably holds.
    """
    same_night = queue_entry(
        "reask:k1", pattern="reask", first_seen=NOW - DAY
    )
    fix = _fix(
        "aaaaaaa", NOW - DAY, "merge: reask guard (kanban t_aaaaaaaa)", "t_aaaaaaaa"
    )
    verdict = retro.classify_fix(fix, findings=(same_night,))
    assert verdict.verdict == retro.VERDICT_LEDGER_MISS
    assert verdict.nights_early == 0


def test_unreadable_board_db_is_empty_lineage(tmp_path: Path) -> None:
    broken = tmp_path / "kanban.db"
    broken.write_text("not a database", encoding="utf-8")
    assert retro.load_card_lineage(broken, ["t_aaaaaaaa"]) == {}


# --- evidence loaders -------------------------------------------------------


def test_load_ledger_history_unions_backups_and_keeps_earliest_first_seen(
    tmp_path: Path,
) -> None:
    state = seed_state(
        tmp_path / "state" / "hkrc" / "harness-loop-state.json",
        [queue_entry("pattern-a:aaaa", pattern="pattern-a", first_seen=NOW - DAY)],
        last_run=NOW - DAY,
    )
    (state.parent / "harness-loop-state.backup-20260901T000000Z.json").write_text(
        json.dumps(
            {
                "open_findings": [
                    queue_entry(
                        "pattern-a:aaaa", pattern="pattern-a", first_seen=NOW - 9 * DAY
                    ),
                    queue_entry(
                        "pattern-b:bbbb", pattern="pattern-b", first_seen=NOW - 5 * DAY
                    ),
                ]
            }
        ),
        encoding="utf-8",
    )
    history = retro.load_ledger_history(state)
    by_fp = {str(entry["fingerprint"]): entry for entry in history}
    assert set(by_fp) == {"pattern-a:aaaa", "pattern-b:bbbb"}
    assert by_fp["pattern-a:aaaa"]["first_seen"] == NOW - 9 * DAY


def test_ledger_findings_drops_requirement_and_monitor_bookkeeping() -> None:
    findings = retro.ledger_findings(
        [
            queue_entry("open:1111", pattern="open"),
            queue_entry("monitor:2222", pattern="monitor", fix_status="monitor"),
            {
                "kind": retro.DETECTOR_REQUIREMENT_KIND,
                "fingerprint": "detector-requirement:blind-spot",
                "pattern": "blind-spot",
                "fix_status": retro.DETECTOR_REQUIREMENT_STATUS,
            },
            {"fix_status": "requirement", "fingerprint": ""},
        ]
    )
    assert [str(entry["fingerprint"]) for entry in findings] == ["open:1111"]


def test_release_index_joins_sha_to_deploy(tmp_path: Path) -> None:
    root = tmp_path / "releases"
    make_release(root, "0.15.16-implwave.408e985", NOW - 3 * DAY)
    make_release(root, "0.15.17-implwave.408e985", NOW + DAY)
    make_release(root, "0.15.15-implwave.1111111", NOW - 9 * DAY)
    index = retro.load_release_index(root)
    assert len(index) == 3
    version, installed = retro._deployed_for("408e985", NOW - 2 * DAY, index)
    assert version == "0.15.17-implwave.408e985"
    assert installed == NOW + DAY
    assert retro._deployed_for("deadbee", NOW, index) == ("", None)


def test_release_index_missing_root_is_empty(tmp_path: Path) -> None:
    assert retro.load_release_index(tmp_path / "nope") == ()


# --- metrics ----------------------------------------------------------------


def _verdict(sha: str, ts: int, kind: str, nights: int | None = None) -> retro.FixVerdict:
    return retro.FixVerdict(
        sha=sha,
        ts=ts,
        subject=f"merge: {sha} (kanban t_00000000)",
        verdict=kind,
        pattern="p",
        nights_early=nights,
    )


def test_rolling_metrics_share_and_median() -> None:
    verdicts = (
        _verdict("a", NOW - DAY, retro.VERDICT_DETECTOR_BORN),
        _verdict("b", NOW - DAY, retro.VERDICT_LEDGER_MISS, nights=3),
        _verdict("c", NOW - DAY, retro.VERDICT_LEDGER_MISS, nights=5),
        _verdict("d", NOW - DAY, retro.VERDICT_BLIND_SPOT),
    )
    window, born, miss, blind, share, median = retro.rolling_metrics(verdicts, now=NOW)
    assert (window, born, miss, blind) == (4, 1, 2, 1)
    assert share == 0.25
    assert median == 4.0


def test_rolling_metrics_empty_window_is_none_not_zero() -> None:
    stale = (_verdict("a", NOW - 90 * DAY, retro.VERDICT_DETECTOR_BORN),)
    window, _, _, _, share, median = retro.rolling_metrics(stale, now=NOW)
    assert window == 0
    assert share is None
    assert median is None
    assert "n/a" in retro._percent(share)


def test_share_numerator_and_denominator_come_from_one_window() -> None:
    """Regression: a 45-day backfill must not pair a windowed count with a
    register-wide total (the share then reports a fraction that cannot add up)."""
    verdicts = (
        tuple(
            _verdict(f"old{index}", NOW - 40 * DAY, retro.VERDICT_BLIND_SPOT)
            for index in range(6)
        )
        + (_verdict("staleborn", NOW - 40 * DAY, retro.VERDICT_DETECTOR_BORN),)
        + (_verdict("new", NOW - DAY, retro.VERDICT_DETECTOR_BORN),)
    )
    window, born, _miss, _blind, share, _median = retro.rolling_metrics(
        verdicts, now=NOW
    )
    assert window == 1
    assert born == 1  # windowed count
    assert share == 1.0
    assert len(verdicts) == 8  # register-wide total is bigger than the window
    result = retro.RetroResult(
        verdicts=verdicts,
        since=NOW - 40 * DAY,
        now=NOW,
        detector_born=2,
        ledger_miss=0,
        blind_spot=6,
        window_total=window,
        window_detector_born=born,
        detector_caught_share=share,
        median_nights_early=None,
    )
    share_line = retro.render_lines(result)[-2]
    assert "1 detector-born / 1 shipped fixes in window" in share_line
    assert "100%" in share_line


# --- typed requirement entries ---------------------------------------------


def test_blind_spot_writes_one_dormant_requirement_entry_per_pattern() -> None:
    verdicts = (
        _verdict("a", NOW - DAY, retro.VERDICT_BLIND_SPOT),
        _verdict("b", NOW - DAY, retro.VERDICT_LEDGER_MISS, nights=1),
        _verdict("c", NOW - DAY, retro.VERDICT_BLIND_SPOT),
    )
    state: dict[str, Any] = {"open_findings": []}
    written = retro.write_detector_requirements(state, verdicts, now=NOW)
    assert len(written) == 1  # one pattern, two fixes
    entry = written[0]
    assert entry["kind"] == retro.DETECTOR_REQUIREMENT_KIND
    assert entry["fingerprint"] == "detector-requirement:p"
    assert entry["fix_status"] == retro.DETECTOR_REQUIREMENT_STATUS
    assert entry["signal_source"] == retro.SHIPPED_FIX_SIGNAL_SOURCE
    assert entry["occurrence_count"] == 2
    assert entry["first_seen"] == NOW - DAY
    assert entry["last_seen"] == NOW
    assert entry["apply_kind"] == "none"
    assert "backfill" not in entry
    assert len(entry["evidence"]) == 2
    assert "a" in entry["evidence"][0]
    assert retro.detector_requirements(state) == (entry,)
    # Dormant: the loop's own finding machinery never surfaces it.
    assert entry["fix_status"] not in _OPEN_FIX_STATUSES
    loop_finding = retro.ledger_findings([entry])
    assert loop_finding == ()
    assert rank_open_findings([entry]) == [entry]  # ranking is not a filter
    assert entry["fix_status"] not in _OPEN_FIX_STATUSES


def test_requirement_write_is_idempotent_across_runs() -> None:
    verdicts = (_verdict("a", NOW - DAY, retro.VERDICT_BLIND_SPOT),)
    state: dict[str, Any] = {"open_findings": []}
    assert len(retro.write_detector_requirements(state, verdicts, now=NOW)) == 1
    assert retro.write_detector_requirements(state, verdicts, now=NOW + DAY) == ()
    entries = retro.detector_requirements(state)
    assert len(entries) == 1  # no duplicate row
    assert entries[0]["occurrence_count"] == 2
    assert entries[0]["last_seen"] == NOW + DAY
    assert len(entries[0]["evidence"]) == 1  # sha-deduped


def test_requirement_write_survives_broken_ledger_shapes() -> None:
    state: dict[str, Any] = {"open_findings": "not a list"}
    verdicts = (_verdict("a", NOW, retro.VERDICT_BLIND_SPOT),)
    assert len(retro.write_detector_requirements(state, verdicts, now=NOW)) == 1
    assert isinstance(state["open_findings"], list)
    assert retro.detector_requirements({"open_findings": None}) == ()


def test_requirement_entry_is_marked_on_backfill() -> None:
    entry = retro.detector_requirement_entry(
        "pattern-x", [_verdict("a", NOW, retro.VERDICT_BLIND_SPOT)], now=NOW, backfill=True
    )
    assert entry["backfill"] is retro.BACKFILL_MARKER


# --- pre-pass ---------------------------------------------------------------


def test_prepass_end_to_end_classifies_and_reports(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [
            ("merge: reask detector fix (kanban t_11111111)", NOW - 2 * DAY),
            ("merge: decision-latency guard (kanban t_22222222)", NOW - DAY),
            ("merge: brand new thing (kanban t_33333333)", NOW - 3600),
        ],
    )
    board_db = make_board_db(
        tmp_path / "boards" / "hkrc" / "kanban.db",
        [
            (
                "t_11111111",
                "reask detector",
                "",
                "harness-hkrc-impl:reask:s_12345678",
            ),
            ("t_22222222", "decision latency guard", "ledger work", ""),
            ("t_33333333", "unrelated", "", ""),
        ],
    )
    release_root = tmp_path / "releases"
    shipped = {
        fix.task_ids[0]: fix
        for fix in retro.parse_register(retro.git_merge_log(repo, None))
    }
    make_release(
        release_root,
        f"0.15.16-implwave.{shipped['t_22222222'].sha}",
        shipped["t_22222222"].ts + 60,
    )
    state = seed_state(
        tmp_path / "state" / "hkrc" / "harness-loop-state.json",
        [
            queue_entry(
                "decision-latency:k9",
                pattern="decision-latency",
                first_seen=NOW - 5 * DAY,
            )
        ],
        last_run=NOW - 3 * DAY,
    )
    result = retro.run_prepass(
        repo=repo,
        since=NOW - 3 * DAY,
        now=NOW,
        ledger_entries=retro.load_ledger_history(state),
        board_db=board_db,
        release_root=release_root,
    )
    kinds = {verdict.sha: verdict.verdict for verdict in result.verdicts}
    assert len(result.verdicts) == 3
    assert sorted(kinds.values()) == sorted(
        [
            retro.VERDICT_DETECTOR_BORN,
            retro.VERDICT_LEDGER_MISS,
            retro.VERDICT_BLIND_SPOT,
        ]
    )
    assert (result.detector_born, result.ledger_miss, result.blind_spot) == (1, 1, 1)
    assert result.window_total == 3
    assert result.detector_caught_share == 1 / 3
    assert result.median_nights_early == 4.0
    lines = retro.render_lines(result)
    assert "3 shipped fix(es) since last run" in lines[0]
    assert "1 detector-born, 1 ledger-miss, 1 blind-spot" in lines[0]
    assert "detector-caught share (rolling 30d): 33%" in lines[-2]
    assert any("queued for pattern" in line for line in lines)
    assert "0 detector_requirement entry(ies)" in lines[0]
    assert "detector-born — pattern reask" in "\n".join(lines)
    assert "deployed 0.15.16-implwave." in "\n".join(lines)


def test_prepass_deploy_join_and_empty_state(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path, [("merge: thing (kanban t_44444444)", NOW - DAY)]
    )
    fix = retro.parse_register(retro.git_merge_log(repo, None))[0]
    release_root = tmp_path / "releases"
    version = f"0.15.16-implwave.{fix.sha}"
    make_release(release_root, version, fix.ts + 60)
    result = retro.run_prepass(
        repo=repo,
        since=NOW - 7 * DAY,
        now=NOW,
        release_root=release_root,
    )
    assert result.verdicts[0].deploy_version == version
    assert result.verdicts[0].deployed_at == fix.ts + 60
    assert "not deployed yet" not in retro.render_lines(result)[1]

    empty = retro.run_prepass(repo=repo, since=NOW, now=NOW)
    assert empty.verdicts == ()
    assert "empty state" in retro.render_lines(empty)[0]


def test_degraded_section_is_a_labelled_report_line() -> None:
    reason = "git merge log failed in /x: boom"
    lines = retro.degraded_lines(reason)
    assert len(lines) == 1
    assert lines[0].startswith("unavailable this run")
    assert reason in lines[0]
    assert "unaffected" in lines[0]
    degraded = retro.RetroResult(
        verdicts=(),
        since=NOW,
        now=NOW,
        detector_born=0,
        ledger_miss=0,
        blind_spot=0,
        window_total=0,
        window_detector_born=0,
        detector_caught_share=None,
        median_nights_early=None,
        degraded=reason,
    )
    assert retro.render_lines(degraded) == lines


# --- backfill ---------------------------------------------------------------


def test_backfill_dry_run_leaves_the_caller_state_untouched(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [
            ("merge: old thing (kanban t_55555555)", NOW - 20 * DAY),
            ("merge: newer thing (kanban t_66666666)", NOW - 2 * DAY),
        ],
    )
    state_path = seed_state(
        tmp_path / "state" / "hkrc" / "harness-loop-state.json", [], last_run=NOW - DAY
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    before = json.dumps(state, sort_keys=True)
    report = retro.run_backfill(
        repo=repo,
        state=state,
        state_path=state_path,
        now=NOW,
        dry_run=True,
    )
    assert json.dumps(state, sort_keys=True) == before
    assert "WOULD be written (dry run: ledger untouched)" in report
    assert "in git history" in report
    assert "depth bounds" in report

    live = retro.run_backfill(
        repo=repo, state=state, state_path=state_path, now=NOW, dry_run=False
    )
    entries = retro.detector_requirements(state)
    assert len(entries) == 2
    assert all(entry["backfill"] is retro.BACKFILL_MARKER for entry in entries)
    assert "written" in live.splitlines()[2]


def test_backfill_second_run_writes_nothing_new(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path, [("merge: thing (kanban t_77777777)", NOW - 3 * DAY)]
    )
    state_path = seed_state(
        tmp_path / "state" / "hkrc" / "harness-loop-state.json", [], last_run=NOW - DAY
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    first = retro.run_backfill(
        repo=repo, state=state, state_path=state_path, now=NOW, dry_run=False
    )
    second = retro.run_backfill(
        repo=repo, state=state, state_path=state_path, now=NOW, dry_run=False
    )
    assert "1 detector_requirement entry(ies) written" in first
    assert "0 detector_requirement entry(ies) written" in second
    assert len(retro.detector_requirements(state)) == 1


def test_ledger_depth_bounds_names_the_earliest_backup(tmp_path: Path) -> None:
    state_path = seed_state(
        tmp_path / "state" / "hkrc" / "harness-loop-state.json", [], last_run=NOW
    )
    (state_path.parent / "harness-loop-state.backup-20260901T000000Z.json").write_text(
        "{}", encoding="utf-8"
    )
    (state_path.parent / "harness-loop-state.backup-20260910T000000Z.json").write_text(
        "{}", encoding="utf-8"
    )
    bounds = retro.ledger_depth_bounds(state_path)
    assert "20260901T000000Z" in bounds
    assert "LOWER BOUNDS" in bounds
    assert "live ledger" in bounds


# --- harness-loop + CLI integration ----------------------------------------


def test_harness_loop_run_reports_the_section_and_persists_dormant_entries(
    tmp_path: Path,
) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [("merge: brand new detector gap (kanban t_88888888)", NOW - 3600)],
    )
    make_board_db(
        tmp_path / "boards" / "hkrc" / "kanban.db",
        [("t_88888888", "unrelated card", "", "")],
    )
    config = make_config(tmp_path, hkrc_repo=repo)
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)
    runner, calls, _flags = make_ticket_runner()

    report = run_harness_loop(
        config,
        now=NOW,
        dry_run=False,
        runner=runner,
        state_path=state_file,
    )
    assert retro.RETRO_SECTION_TITLE in report
    assert "1 shipped fix(es) since last run" in report
    loaded = load_state(state_file)
    entries = retro.detector_requirements(loaded)
    assert len(entries) == 1
    assert entries[0]["fix_status"] == retro.DETECTOR_REQUIREMENT_STATUS
    assert entries[0]["signal_source"] == retro.SHIPPED_FIX_SIGNAL_SOURCE
    # Report-only: the retro never created a card (the fake CLI records argv).
    assert not any(
        retro.DETECTOR_REQUIREMENT_KIND in call
        or retro.SHIPPED_FIX_SIGNAL_SOURCE in call
        or "blind" in call
        for call in calls
    )


def test_harness_loop_joins_the_deploy_from_the_instance_release_root(
    tmp_path: Path,
) -> None:
    """Regression: the wired release root is ``<instance>/releases``.

    ``state_db.parent.parent`` is ``<instance>/state``, so the deploy join
    silently degraded to "not deployed yet" for every shipped fix on the live
    instance (caught by running the real CLI, not by the unit tests).
    """
    repo = make_shipped_repo(
        tmp_path,
        [("merge: brand new detector gap (kanban t_d1d1d1d1)", NOW - 3600)],
    )
    fix = retro.parse_register(retro.git_merge_log(repo, None))[0]
    version = f"0.15.16-implwave.{fix.sha}"
    make_release(tmp_path / "releases", version, fix.ts + 60)
    config = make_config(tmp_path, hkrc_repo=repo)
    assert config.state_db == tmp_path / "state" / "hkrc" / "state.sqlite3"
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)

    report = run_harness_loop(
        config,
        now=NOW,
        dry_run=False,
        runner=make_runner(),
        state_path=state_file,
    )
    assert f"deployed {version}" in report
    assert "not deployed yet" not in report


def test_harness_loop_dry_run_writes_no_requirement_entries(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [("merge: brand new detector gap (kanban t_99999999)", NOW - 3600)],
    )
    config = make_config(tmp_path, hkrc_repo=repo)
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)
    before = state_file.read_text(encoding="utf-8")

    report = run_harness_loop(
        config,
        now=NOW,
        dry_run=True,
        runner=make_runner(),
        state_path=state_file,
    )
    assert "WOULD be written (dry run: ledger untouched)" in report
    assert state_file.read_text(encoding="utf-8") == before


def test_harness_loop_degrades_when_the_git_log_fails(tmp_path: Path) -> None:
    config = make_config(tmp_path, hkrc_repo=tmp_path / "no-such-repo")
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)

    report = run_harness_loop(
        config,
        now=NOW,
        dry_run=False,
        runner=make_runner(),
        state_path=state_file,
    )
    assert retro.RETRO_SECTION_TITLE in report
    assert "unavailable this run" in report
    # The deterministic body still renders: the retro failure is a section,
    # not a run failure.
    assert len(report.splitlines()) > 5
    assert report.index(retro.RETRO_SECTION_TITLE) > 0


def test_harness_loop_trace_carries_the_retro_facts(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path,
        [("merge: brand new detector gap (kanban t_a1a1a1a1)", NOW - 3600)],
    )
    config = make_config(tmp_path, hkrc_repo=repo)
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)
    trace: list[dict[str, Any]] = []

    run_harness_loop(
        config,
        now=NOW,
        dry_run=False,
        runner=make_runner(),
        state_path=state_file,
        trace=trace,
    )
    facts = trace[-1]["shipped_fix_retro"]
    assert facts["total"] == 1
    assert facts["blind_spot"] == 1
    assert facts["window_total"] == 1
    assert facts["requirements_written"] == 1
    assert facts["since"] == NOW - DAY


def test_backfill_entry_point_is_read_only_by_default(tmp_path: Path) -> None:
    repo = make_shipped_repo(
        tmp_path, [("merge: thing (kanban t_b1b1b1b1)", NOW - 2 * DAY)]
    )
    config = make_config(tmp_path, hkrc_repo=repo)
    state_file = config.state_db.parent / "harness-loop-state.json"
    seed_state(state_file, [], last_run=NOW - DAY)
    before = state_file.read_text(encoding="utf-8")

    from hkrc.harness_loop import run_shipped_fix_backfill

    report = run_shipped_fix_backfill(config, now=NOW, state_path=state_file)
    assert "Backfill:" in report
    assert state_file.read_text(encoding="utf-8") == before

    run_shipped_fix_backfill(config, now=NOW, dry_run=False, state_path=state_file)
    loaded = load_state(state_file)
    assert len(retro.detector_requirements(loaded)) == 1


def test_cli_exit_code_unchanged_when_the_retro_prepass_fails(
    tmp_path: Path, capsys
) -> None:
    good = make_shipped_repo(
        tmp_path / "good", [("merge: thing (kanban t_c1c1c1c1)", NOW - 3600)]
    )
    broken_config = make_config(tmp_path, hkrc_repo=tmp_path / "no-such-repo")
    broken_path = tmp_path / "broken.toml"
    write_config(broken_path, broken_config, overwrite=True)
    broken_code = cli_main(
        [
            "harness-loop",
            "run",
            "--config",
            str(broken_path),
            "--dry-run",
            "--state-file",
            str(tmp_path / "broken-state.json"),
            "--now",
            str(NOW),
        ]
    )
    good_config = make_config(tmp_path / "good", hkrc_repo=good)
    good_path = tmp_path / "good.toml"
    write_config(good_path, good_config, overwrite=True)
    good_code = cli_main(
        [
            "harness-loop",
            "run",
            "--config",
            str(good_path),
            "--dry-run",
            "--state-file",
            str(tmp_path / "good-state.json"),
            "--now",
            str(NOW),
        ]
    )
    out = capsys.readouterr().out
    assert broken_code == good_code == 0
    assert retro.RETRO_SECTION_TITLE in out
    assert "unavailable this run" in out


@pytest.mark.skipif(
    not (REAL_REPO.is_dir() and REAL_BOARD_DB.is_file()),
    reason="live hkrc checkout / kanban board not present",
)
def test_backfill_acceptance_three_classifies_the_known_fix_408e985(
    tmp_path: Path,
) -> None:
    """Acceptance #3: the backfill classifies 408e985 as detector-born and
    states its depth bounds honestly.  Live evidence, read-only.

    The verdict is asserted against the classifier, not the rendered report:
    the render caps its per-fix detail at ``_MAX_FIX_LINES``, so naming a fix
    in the report is only stable while the register stays under the cap.  The
    report assertions kept here are cap-independent.
    """
    state = json.loads(REAL_LEDGER.read_text(encoding="utf-8"))
    before = json.dumps(state, sort_keys=True)
    verdicts = retro.classify_register(
        retro.parse_register(retro.git_merge_log(REAL_REPO, None)),
        board_db=REAL_BOARD_DB,
        findings=retro.ledger_findings(retro.load_ledger_history(REAL_LEDGER)),
        release_index=retro.load_release_index(REAL_RELEASES),
    )
    known = [verdict for verdict in verdicts if verdict.sha.startswith("408e985")]
    assert len(known) == 1
    assert known[0].verdict == retro.VERDICT_DETECTOR_BORN
    assert known[0].deployed is True
    report = retro.run_backfill(
        repo=REAL_REPO,
        state=state,
        state_path=REAL_LEDGER,
        now=NOW,
        board_db=REAL_BOARD_DB,
        release_root=REAL_RELEASES,
        dry_run=True,
    )
    assert json.dumps(state, sort_keys=True) == before  # read-only
    assert "Backfill:" in report
    assert "depth bounds" in report and "LOWER BOUNDS" in report
    assert "git history" in report


def test_render_caps_per_fix_detail_and_keeps_every_count(
    tmp_path: Path,
) -> None:
    """The cap is presentation only: an over-cap register still renders every
    count, and exactly the first ``_MAX_FIX_LINES`` fixes get a detail line."""
    total = retro._MAX_FIX_LINES + 2
    repo = make_shipped_repo(
        tmp_path,
        [
            (f"merge: capped fix {index} (kanban t_{index:08d})", NOW - DAY + index)
            for index in range(total)
        ],
    )
    result = retro.run_prepass(repo=repo, since=NOW - 7 * DAY, now=NOW)
    assert result.total == total
    lines = retro.render_lines(result)
    verdict_lines = [retro._verdict_line(verdict) for verdict in result.verdicts]
    rendered_detail = [line for line in lines if line in verdict_lines]
    assert rendered_detail == verdict_lines[: retro._MAX_FIX_LINES]
    assert f"(+{total - retro._MAX_FIX_LINES} more shipped fix(es))" in lines
    assert f"{total} shipped fix(es) since last run" in lines[0]
    assert "detector-caught share (rolling 30d)" in lines[-2]
    assert "median nights-early (ledger-miss, rolling 30d)" in lines[-1]


def test_cli_retro_command_defaults_to_dry_run() -> None:
    from hkrc.cli import build_parser

    args = build_parser().parse_args(["harness-loop", "retro"])
    assert args.dry_run is True
    assert args.command == "harness-loop"
    live = build_parser().parse_args(["harness-loop", "retro", "--no-dry-run"])
    assert live.dry_run is False


def test_module_never_creates_a_kanban_card() -> None:
    """Report-only contract: no card-creation call exists in the pre-pass."""
    source = (Path(retro.__file__) if retro.__file__ else Path("")).read_text(
        encoding="utf-8"
    )
    assert "kanban create" not in source
    assert "_kanban_create" not in source
    assert "subprocess" in source  # only the read-only git log runs a command


def test_retro_section_title_and_constant_are_wired() -> None:
    from hkrc import harness_loop

    assert harness_loop.RETRO_SECTION_TITLE == retro.RETRO_SECTION_TITLE
    assert "requirement" not in harness_loop._OPEN_FIX_STATUSES
    assert loop_fingerprint is harness_loop.fingerprint
