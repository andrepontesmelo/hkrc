"""Advisory N1-N5 detectors (t_2a1dc07d) — unit coverage per detector.

Style follows test_daemon_regression.py: hand-built BoardEvidence fixtures,
direct detect_* calls, fingerprint assertions.  A collector section at the
bottom exercises the new evidence SQL (comments/events/parent_edges) and the
read-only branch-position probe through collect_boards.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
import sqlite3
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hkrc.config import ControllerConfig, HarnessLoopConfig, load_config, write_config
from hkrc.harness_loop import (
    BoardEvidence,
    BranchPosition,
    CommentRow,
    EventRow,
    ParentEdge,
    ProcessResult,
    RunRow,
    TaskRow,
    collect_boards,
    detect_complete_without_evidence,
    detect_parent_link_deadlock,
    detect_stale_branch,
    detect_unblock_without_record,
    detect_zero_terminal_call,
    fingerprint,
    run,
)

NOW = 1_800_000_000  # fixed epoch; nothing reads wall clock
HOUR = 3600


def board(
    *,
    slug: str = "hkrc",
    tasks: tuple[TaskRow, ...] = (),
    open_tasks: tuple[TaskRow, ...] = (),
    runs: tuple[RunRow, ...] = (),
    comments: tuple[CommentRow, ...] = (),
    events: tuple[EventRow, ...] = (),
    edges: tuple[ParentEdge, ...] = (),
    positions: tuple[BranchPosition, ...] = (),
) -> BoardEvidence:
    return BoardEvidence(
        slug=slug,
        status_counts=(),
        tasks_in_window=tasks,
        runs_in_window=runs,
        failure_events=(),
        children={},
        blocked_rows=(),
        open_task_rows=open_tasks,
        comments=comments,
        events=events,
        parent_edges=edges,
        branch_positions=positions,
    )


def task(
    task_id: str,
    title: str,
    status: str,
    *,
    workspace_kind: str | None = "worktree",
) -> TaskRow:
    return TaskRow(
        id=task_id,
        title=title,
        status=status,
        assignee="developer",
        created_at=NOW - 2 * HOUR,
        completed_at=None,
        block_kind=None,
        workspace_kind=workspace_kind,
    )


def comment(
    task_id: str,
    author: str,
    created_at: int,
    body: str = "decisions: X",
) -> CommentRow:
    return CommentRow(
        task_id=task_id, author=author, created_at=created_at, body=body
    )


def event(
    task_id: str,
    kind: str,
    created_at: int,
    payload: str | None = None,
) -> EventRow:
    return EventRow(task_id=task_id, kind=kind, created_at=created_at, payload=payload)


def run_row(
    task_id: str,
    *,
    outcome: str = "success",
    status: str = "completed",
    started_at: int = NOW - HOUR,
    ended_at: int | None = NOW - HOUR + 600,
) -> RunRow:
    return RunRow(
        id=1,
        task_id=task_id,
        profile="developer",
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        outcome=outcome,
        error=None,
    )


def edge(
    parent_id: str,
    parent_title: str,
    parent_status: str,
    child_id: str,
    child_title: str,
    child_status: str,
    *,
    child_ws: str | None = "worktree",
    parent_ws: str | None = "worktree",
    parent_created: int | None = NOW - 3 * HOUR,
    child_created: int | None = NOW - 2 * HOUR,
) -> ParentEdge:
    return ParentEdge(
        parent_id=parent_id,
        parent_title=parent_title,
        parent_status=parent_status,
        parent_workspace_kind=parent_ws,
        parent_created_at=parent_created,
        child_id=child_id,
        child_title=child_title,
        child_status=child_status,
        child_workspace_kind=child_ws,
        child_created_at=child_created,
    )


# --- N1 unblock-without-record ------------------------------------------------


def test_n1_blind_unblock_fires() -> None:
    boards = (
        board(
            events=(
                event("t_1", "unblocked", NOW, json.dumps({"author": "Andre"})),
            ),
        ),
    )
    findings = detect_unblock_without_record(boards)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "unblock-without-record"
    assert finding.key == "hkrc/t_1"
    assert finding.severity == "high"
    assert finding.apply_kind == "none"
    assert fingerprint(finding) == "unblock-without-record:hkrc/t_1"


def test_n1_prior_comment_by_same_actor_denies() -> None:
    boards = (
        board(
            comments=(comment("t_1", "Andre", NOW - 600),),
            events=(
                event("t_1", "unblocked", NOW, json.dumps({"author": "Andre"})),
            ),
        ),
    )
    assert detect_unblock_without_record(boards) == ()


def test_n1_unattributable_unblock_denies() -> None:
    boards = (
        board(
            events=(event("t_1", "unblocked", NOW, None),),
        ),
    )
    assert detect_unblock_without_record(boards) == ()


def test_n1_comment_after_unblock_still_fires() -> None:
    # Edge: the record must PRECEDE the unblock; a same-actor comment
    # written after the fact does not clear it.
    boards = (
        board(
            comments=(comment("t_1", "Andre", NOW + 600),),
            events=(
                event("t_1", "unblocked", NOW, json.dumps({"author": "Andre"})),
            ),
        ),
    )
    findings = detect_unblock_without_record(boards)
    assert len(findings) == 1
    assert "no earlier comment" in findings[0].evidence[0]


def test_n1_comment_by_other_actor_still_fires() -> None:
    boards = (
        board(
            comments=(comment("t_1", "someone-else", NOW - 600),),
            events=(
                event("t_1", "unblocked", NOW, json.dumps({"author": "Andre"})),
            ),
        ),
    )
    assert len(detect_unblock_without_record(boards)) == 1


# --- N2 complete-without-evidence ----------------------------------------------


def test_n2_bare_complete_fires() -> None:
    boards = (
        board(
            tasks=(task("t_1", "feat: thing", "done"),),
            events=(event("t_1", "completed", NOW, '{"result_len": 297}'),),
        ),
    )
    findings = detect_complete_without_evidence(boards)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "complete-without-evidence"
    assert finding.key == "hkrc/t_1"
    assert finding.severity == "medium"
    assert finding.apply_kind == "none"
    assert fingerprint(finding) == "complete-without-evidence:hkrc/t_1"


def test_n2_sha_in_payload_denies() -> None:
    boards = (
        board(
            tasks=(task("t_1", "feat: thing", "done"),),
            events=(
                event(
                    "t_1",
                    "completed",
                    NOW,
                    '{"summary": "shipped abc1234 on the branch"}',
                ),
            ),
        ),
    )
    assert detect_complete_without_evidence(boards) == ()


def test_n2_sha_in_comment_denies() -> None:
    boards = (
        board(
            tasks=(task("t_1", "feat: thing", "done"),),
            comments=(comment("t_1", "Andre", NOW - 30, "commit 9f2c1ab landed"),),
            events=(event("t_1", "completed", NOW, None),),
        ),
    )
    assert detect_complete_without_evidence(boards) == ()


def test_n2_run_in_window_denies() -> None:
    boards = (
        board(
            tasks=(task("t_1", "feat: thing", "done"),),
            runs=(run_row("t_1"),),
            events=(event("t_1", "completed", NOW, None),),
        ),
    )
    assert detect_complete_without_evidence(boards) == ()


def test_n2_other_board_denies() -> None:
    boards = (
        board(
            slug="general",
            tasks=(task("t_1", "feat: thing", "done"),),
            events=(event("t_1", "completed", NOW, None),),
        ),
    )
    assert detect_complete_without_evidence(boards) == ()


def test_n2_review_card_exempt() -> None:
    boards = (
        board(
            tasks=(task("t_1", "review: validate thing", "done"),),
            events=(event("t_1", "completed", NOW, None),),
        ),
    )
    assert detect_complete_without_evidence(boards) == ()


# --- N3 zero-terminal-call -----------------------------------------------------


def test_n3_success_run_open_card_no_terminal_fires() -> None:
    open_card = task("t_1", "feat: thing", "running")
    boards = (
        board(
            open_tasks=(open_card,),
            runs=(run_row("t_1"),),
        ),
    )
    findings = detect_zero_terminal_call(boards)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "zero-terminal-call"
    assert finding.key == "hkrc/t_1"
    assert finding.severity == "medium"
    assert finding.apply_kind == "none"
    assert fingerprint(finding) == "zero-terminal-call:hkrc/t_1"


def test_n3_native_completed_outcome_fires() -> None:
    # The native CLI writes outcome='completed' (never 'success') for a
    # successful run — the live vocabulary must fire too.
    open_card = task("t_1", "feat: thing", "running")
    boards = (
        board(
            open_tasks=(open_card,),
            runs=(run_row("t_1", outcome="completed"),),
        ),
    )
    assert len(detect_zero_terminal_call(boards)) == 1


def test_n3_blocked_after_run_denies() -> None:
    open_card = task("t_1", "feat: thing", "blocked")
    boards = (
        board(
            open_tasks=(open_card,),
            runs=(run_row("t_1"),),
            events=(
                event(
                    "t_1",
                    "blocked",
                    NOW - HOUR + 650,
                    json.dumps({"kind": "needs_input", "reason": "waiting"}),
                ),
            ),
        ),
    )
    assert detect_zero_terminal_call(boards) == ()


def test_n3_terminal_card_denies() -> None:
    # The card left the loop (done) — nothing to flag even without a
    # windowed event (window may have cut it off).
    done_card = task("t_1", "feat: thing", "done")
    boards = (
        board(
            open_tasks=(),
            tasks=(done_card,),
            runs=(run_row("t_1"),),
        ),
    )
    assert detect_zero_terminal_call(boards) == ()


def test_n3_unsuccessful_outcome_denies() -> None:
    open_card = task("t_1", "feat: thing", "running")
    boards = (
        board(
            open_tasks=(open_card,),
            runs=(run_row("t_1", outcome="timed_out", status="timed_out"),),
        ),
    )
    assert detect_zero_terminal_call(boards) == ()


def test_n3_terminal_call_outside_window_still_fires() -> None:
    # Edge: a terminal event BEFORE the run start (old episode) is not
    # evidence for THIS run; one after the run-end grace window neither.
    open_card = task("t_1", "feat: thing", "running")
    boards = (
        board(
            open_tasks=(open_card,),
            runs=(run_row("t_1"),),
            events=(
                event("t_1", "completed", NOW - HOUR - 120, None),
                event("t_1", "blocked", NOW + DAY, None),
            ),
        ),
    )
    assert len(detect_zero_terminal_call(boards)) == 1


DAY = 86_400  # used by the edge test above


# --- N4 parent-link-deadlock ----------------------------------------------------


def test_n4_impl_child_under_open_review_fires() -> None:
    boards = (
        board(
            edges=(
                edge(
                    "t_rev", "review: validate feature", "blocked",
                    "t_impl", "feat: feature", "todo",
                ),
            ),
        ),
    )
    findings = detect_parent_link_deadlock(boards)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "parent-link-deadlock"
    assert finding.key == "hkrc/t_impl@t_rev"
    assert finding.severity == "medium"
    assert finding.apply_kind == "none"
    assert "review-gated implementation" in finding.evidence[0]


def test_n4_later_impl_under_earlier_impl_fires() -> None:
    boards = (
        board(
            edges=(
                edge(
                    "t_a", "feat: first stage", "running",
                    "t_b", "feat: second stage", "todo",
                ),
            ),
        ),
    )
    findings = detect_parent_link_deadlock(boards)
    assert len(findings) == 1
    assert "later stage under earlier stage" in findings[0].evidence[0]


def test_n4_terminal_child_denies() -> None:
    boards = (
        board(
            edges=(
                edge(
                    "t_rev", "review: validate feature", "blocked",
                    "t_impl", "feat: feature", "done",
                ),
            ),
        ),
    )
    assert detect_parent_link_deadlock(boards) == ()


def test_n4_fan_in_child_created_before_parent_denies() -> None:
    boards = (
        board(
            edges=(
                edge(
                    "t_gate", "grilling: the plan", "blocked",
                    "t_impl", "feat: feature", "todo",
                    parent_ws="scratch",
                    parent_created=NOW - HOUR,
                    child_created=NOW - 2 * HOUR,
                ),
            ),
        ),
    )
    assert detect_parent_link_deadlock(boards) == ()


def test_n4_non_impl_child_denies() -> None:
    boards = (
        board(
            edges=(
                edge(
                    "t_rev", "review: validate feature", "blocked",
                    "t_doc", "task: trim docs", "todo",
                    child_ws="scratch",
                ),
            ),
        ),
    )
    assert detect_parent_link_deadlock(boards) == ()


def test_n4_impl_child_under_impl_with_missing_timestamps_fires() -> None:
    # Edge: unknown creation times — fan-in cannot be proven, so the
    # later-stage class still flags (fail toward reporting a real deadlock).
    boards = (
        board(
            edges=(
                edge(
                    "t_a", "feat: first", "done",
                    "t_b", "feat: second", "todo",
                    parent_created=None, child_created=None,
                ),
            ),
        ),
    )
    assert len(detect_parent_link_deadlock(boards)) == 1


def test_n4_impl_child_under_scratch_impl_anchor_denies() -> None:
    # A scratch card whose TITLE looks like impl work ("feat:") is the
    # epic/anchor shape (wayfinder "task:" anchors, planning scratch);
    # workspace kind, not title, decides impl-vs-planning for parents.
    boards = (
        board(
            edges=(
                edge(
                    "t_anchor", "feat: rewrite the loop", "running",
                    "t_impl", "feat: second stage", "todo",
                    parent_ws="scratch",
                ),
            ),
        ),
    )
    assert detect_parent_link_deadlock(boards) == ()


# --- N5 stale-branch ------------------------------------------------------------


def _stale_fixture(
    *, behind: int, ahead: int = 1, trigger: str = "promoted"
) -> tuple[BoardEvidence, ...]:
    open_card = task("t_1", "feat: thing", "running")
    return (
        board(
            open_tasks=(open_card,),
            events=(event("t_1", trigger, NOW - 30, None),),
            positions=(
                BranchPosition(task_id="t_1", branch="wt/t_1", ahead=ahead, behind=behind),
            ),
        ),
    )


def test_n5_stale_branch_at_promotion_fires() -> None:
    findings = detect_stale_branch(_stale_fixture(behind=7))
    assert len(findings) == 1
    finding = findings[0]
    assert finding.pattern == "stale-branch"
    assert finding.key == "hkrc/t_1"
    assert finding.severity == "medium"
    assert finding.apply_kind == "none"
    assert fingerprint(finding) == "stale-branch:hkrc/t_1"
    assert "7 commits behind" in finding.evidence[0]


def test_n5_below_threshold_denies() -> None:
    assert detect_stale_branch(_stale_fixture(behind=3)) == ()


def test_n5_no_trigger_event_denies() -> None:
    # Same stale branch but no promoted/unblocked event in the window —
    # the noise gate keeps old worktrees silent.
    boards = (
        board(
            open_tasks=(task("t_1", "feat: thing", "running"),),
            positions=(
                BranchPosition(task_id="t_1", branch="wt/t_1", ahead=0, behind=9),
            ),
        ),
    )
    assert detect_stale_branch(boards) == ()


def test_n5_unmeasured_branch_denies() -> None:
    boards = (
        board(
            open_tasks=(task("t_1", "feat: thing", "running"),),
            events=(event("t_1", "unblocked", NOW - 30, None),),
            positions=(),
        ),
    )
    assert detect_stale_branch(boards) == ()


def test_n5_threshold_knob() -> None:
    findings = detect_stale_branch(_stale_fixture(behind=1), behind_threshold=1)
    assert len(findings) == 1


# --- collector wiring -----------------------------------------------------------


def _make_native_board(root: Path, *, with_comments: bool = True) -> Path:
    """Fixture board with tasks/events/links/runs (+optional comments)."""
    board_dir = root / "hkrc"
    board_dir.mkdir(parents=True)
    (board_dir / "board.json").write_text(
        json.dumps({"slug": "hkrc"}), encoding="utf-8"
    )
    connection = sqlite3.connect(board_dir / "kanban.db")
    statements = [
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT,
            assignee TEXT, status TEXT NOT NULL, priority INTEGER,
            created_at INTEGER, completed_at INTEGER, block_kind TEXT,
            workspace_kind TEXT, branch_name TEXT, skills TEXT
        );
        """,
        """
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id INTEGER,
            kind TEXT NOT NULL, payload TEXT, created_at INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE task_links (
            parent_id TEXT NOT NULL, child_id TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, profile TEXT,
            status TEXT NOT NULL, started_at INTEGER NOT NULL,
            ended_at INTEGER, outcome TEXT, error TEXT
        );
        """,
    ]
    if with_comments:
        statements.append(
            """
            CREATE TABLE task_comments (
                id INTEGER PRIMARY KEY, task_id TEXT NOT NULL,
                author TEXT NOT NULL, body TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            """
        )
    connection.executescript("".join(statements))
    connection.execute(
        "INSERT INTO tasks(id, title, status, assignee, created_at, "
        "workspace_kind, branch_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("t_p", "review: parent", "blocked", "reviewer", NOW - 100, "worktree", None),
    )
    connection.execute(
        "INSERT INTO tasks(id, title, status, assignee, created_at, "
        "workspace_kind, branch_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("t_c", "feat: child", "running", "developer", NOW - 90, "worktree", "wt/t_c"),
    )
    connection.execute(
        "INSERT INTO task_links(parent_id, child_id) VALUES ('t_p', 't_c')"
    )
    connection.execute(
        "INSERT INTO task_events(task_id, kind, payload, created_at) "
        "VALUES (?, ?, ?, ?)",
        ("t_c", "unblocked", json.dumps({"author": "Andre"}), NOW - 50),
    )
    if with_comments:
        connection.execute(
            "INSERT INTO task_comments(task_id, author, body, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("t_c", "Andre", "decisions recorded", NOW - 60),
        )
    connection.commit()
    connection.close()
    return root


def test_collect_boards_captures_comments_events_and_edges(tmp_path: Path) -> None:
    root = _make_native_board(tmp_path / "boards")
    evidences = collect_boards(root, now=NOW, window_hours=24)
    assert len(evidences) == 1
    evidence = evidences[0]
    assert [(c.task_id, c.author) for c in evidence.comments] == [("t_c", "Andre")]
    assert [(e.task_id, e.kind) for e in evidence.events] == [("t_c", "unblocked")]
    assert len(evidence.parent_edges) == 1
    edge_row = evidence.parent_edges[0]
    assert (edge_row.parent_id, edge_row.child_id) == ("t_p", "t_c")
    assert edge_row.parent_status == "blocked"
    assert edge_row.child_status == "running"


def test_collect_boards_schema_optional_tables(tmp_path: Path) -> None:
    # A board without task_comments still collects (comments empty).
    root = _make_native_board(tmp_path / "boards", with_comments=False)
    evidences = collect_boards(root, now=NOW, window_hours=24)
    assert evidences[0].comments == ()
    assert len(evidences[0].parent_edges) == 1


def test_collect_branch_positions_via_readonly_revlist(tmp_path: Path) -> None:
    root = _make_native_board(tmp_path / "boards")
    seen_argv: list[list[str]] = []

    def fake_runner(
        argv: Sequence[str], env: Mapping[str, str], timeout: int
    ) -> ProcessResult:
        seen_argv.append(list(argv))
        assert argv[:3] == ["git", "-C", str(tmp_path / "canonical")]
        assert argv[3:6] == ["rev-list", "--left-right", "--count"]
        assert argv[6] == "wt/t_c...main"
        return ProcessResult(0, "3   7\n", "")

    evidences = collect_boards(
        root,
        now=NOW,
        window_hours=24,
        canonical_repo=tmp_path / "canonical",
        runner=fake_runner,
    )
    assert len(seen_argv) == 1
    positions = evidences[0].branch_positions
    assert positions == (
        BranchPosition(task_id="t_c", branch="wt/t_c", ahead=3, behind=7),
    )


def test_collect_branch_positions_unmeasurable_branch_omitted(
    tmp_path: Path,
) -> None:
    root = _make_native_board(tmp_path / "boards")

    def failing_runner(
        argv: Sequence[str], env: Mapping[str, str], timeout: int
    ) -> ProcessResult:
        return ProcessResult(128, "", "fatal: not a git repository")

    evidences = collect_boards(
        root,
        now=NOW,
        window_hours=24,
        canonical_repo=tmp_path / "nowhere",
        runner=failing_runner,
    )
    assert evidences[0].branch_positions == ()


# --- production run() wiring (DEF-t_2a1dc07d-1) ------------------------------


def _make_canonical_repo(path: Path) -> Path:
    """Real git repo with main plus the fixture worktree branch wt/t_c.

    rev-list only measures refs the canonical repo knows, so wt/t_c must
    exist there (worktrees share refs with their repo in production).
    """
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "test"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "test@local"], check=True)
    (path / "file.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-qm", "init"], check=True)
    subprocess.run(["git", "-C", str(path), "branch", "wt/t_c"], check=True)
    return path


def _make_run_config(tmp_path: Path, *, canonical_branch: str = "main") -> ControllerConfig:
    """Minimal hermetic ControllerConfig for run() (mirrors make_config in
    test_harness_loop: hermetic profiles_root, archloop, cron store pins)."""
    return ControllerConfig(
        "test",
        tmp_path / "boards",
        tmp_path / "state" / "hkrc" / "state.sqlite3",
        harness_loop=HarnessLoopConfig(
            enabled=True,
            sessions_db=tmp_path / "profiles" / "main" / "state.db",
            external_dirs=(str(tmp_path / "dist"),),
            hkrc_repo=tmp_path / "canonical",
            canonical_branch=canonical_branch,
            dist_skills_root=str(tmp_path / "dist"),
            profiles_root=str(tmp_path / "profiles"),
            archloop_output_dir=str(tmp_path / "archloop-output"),
            cron_jobs_path=str(tmp_path / "cron" / "jobs.json"),
        ),
    )


def test_run_collects_branch_positions_production_path(tmp_path: Path) -> None:
    """run() itself must feed the collector a canonical repo (DEF-t_2a1dc07d-1).

    Before the fix, run() called collect_boards without canonical_repo, so
    branch_positions stayed () in every production path and N5 was dead.
    """
    _make_native_board(tmp_path / "boards")  # slug hkrc, open card t_c on wt/t_c
    canonical = _make_canonical_repo(tmp_path / "canonical")
    for index in range(7):
        subprocess.run(
            ["git", "-C", str(canonical), "commit", "-q", "--allow-empty", "-m", f"c{index}"],
            check=True,
        )
    trace: list[dict] = []
    report = run(
        _make_run_config(tmp_path),
        now=NOW,
        state_path=tmp_path / "state.json",
        trace=trace,
    )
    assert isinstance(report, str)
    assert trace, "run() must populate the trace dict"
    # The production path measured wt/t_c: 0 ahead, 7 behind main.
    assert trace[0]["branch_positions_count"] == 1
    assert collect_boards(
        tmp_path / "boards",
        now=NOW,
        canonical_repo=canonical,
    )[0].branch_positions[0].behind == 7


def test_run_smoke_stale_branch_finding_via_production_path(tmp_path: Path) -> None:
    """A promoted/unblocked card >= 5 behind fires N5 through the real run()."""
    root = _make_native_board(tmp_path / "boards")
    canonical = _make_canonical_repo(tmp_path / "canonical")
    for index in range(5):
        subprocess.run(
            ["git", "-C", str(canonical), "commit", "-q", "--allow-empty", "-m", f"c{index}"],
            check=True,
        )
    trace: list[dict] = []
    report = run(
        _make_run_config(tmp_path),
        now=NOW,
        state_path=tmp_path / "state.json",
        trace=trace,
    )
    assert trace[0]["branch_positions_count"] == 1
    # The finding renders under its prose title (pattern ids never appear
    # in the report body).
    assert "Branch far behind the canonical branch" in report


def test_run_unmeasurable_branch_yields_no_positions(tmp_path: Path) -> None:
    """Fail-safe: canonical repo that cannot measure branches -> no positions."""
    _make_native_board(tmp_path / "boards")
    (tmp_path / "canonical").mkdir()  # not a git repo
    trace: list[dict] = []
    report = run(
        _make_run_config(tmp_path),
        now=NOW,
        state_path=tmp_path / "state.json",
        trace=trace,
    )
    assert isinstance(report, str)
    assert trace[0]["branch_positions_count"] == 0


def test_harness_loop_config_canonical_branch_roundtrip(tmp_path: Path) -> None:
    """canonical_branch survives write_config -> load_config and validates."""
    config = _make_run_config(tmp_path, canonical_branch="trunk")
    path = tmp_path / "config.toml"
    write_config(path, config, overwrite=True)
    loaded = load_config(path)
    assert loaded.harness_loop.canonical_branch == "trunk"
    assert HarnessLoopConfig().canonical_branch == "main"
