from __future__ import annotations

from collections.abc import Sequence
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from hkrc.config import ConfigError, ControllerConfig, WatcherConfig, load_config, write_config
from hkrc.event_stream import StreamAdapter, StreamCredentials
from hkrc.handoff import NativeResult
from hkrc.watcher import (
    COMPLETION_EVIDENCE_PATTERN,
    Action,
    DefectBlock,
    REVIEW_REQUIRED_PREFIX,
    ReviewRequiredDeadlock,
    WatcherError,
    board_has_capability_block,
    build_fix_card_body,
    consume_board_events,
    defect_severity,
    discover_defect_blocks,
    discover_missing_block_events,
    discover_pick_gate_candidates,
    discover_review_required_deadlocks,
    discover_supersede_candidates,
    existing_fix_cards,
    extract_shas,
    extract_task_ids,
    fix_card_title,
    format_message,
    git_repo_root,
    has_open_fix_card,
    is_reviewer_assignee,
    pick_gate_skip_reason,
    plan_fix_card,
    run,
    select_pick_gate,
    verify_merged,
)

NOW = 1785820000

RENTCLI_HIGH_DEFECT = {
    "reason": (
        "HIGH defect: start_sync_in_background calls db.connect() using "
        "RENTCLI_DB/default instead of the database path belonging to the app "
        "repository. Explicit-path create_app() syncs and persists terminal "
        "state to the wrong DB; see comment 11 for exact reproduction and "
        "expected/actual results. Developer must fix and reviewer must retest "
        "before merge."
    ),
    "kind": "needs_input",
    "recurrences": 1,
}

HKRC_FIX_READY = {
    "reason": (
        "FIX-READY: MEDIUM config validation defect found — NeedsInputWatcherConfig "
        "accepts boolean True for min_block_seconds/timeout_seconds; developer "
        "must reject bool, then reviewer will retest and rerun related/full gates."
    ),
    "kind": "needs_input",
    "recurrences": 1,
}

PICK_GATE_REASON = "One-at-a-time: Andre picks which fix to run next. Unblock to start."


def make_board(
    root: Path,
    slug: str,
    tasks: list[dict[str, Any]],
    *,
    events: dict[str, list[tuple[str, int, str | None]]] | None = None,
    links: list[tuple[str, str]] | None = None,
    comments: dict[str, list[tuple[str, int, str]]] | None = None,
    default_workdir: str | None = None,
) -> Path:
    """Build a native board with tasks, events, links, and comments."""
    board = root / slug
    board.mkdir(parents=True, exist_ok=True)
    metadata: dict[str, object] = {"slug": slug}
    if default_workdir is not None:
        metadata["default_workdir"] = default_workdir
    (board / "board.json").write_text(json.dumps(metadata), encoding="utf-8")
    for stale in board.glob("kanban.db*"):
        stale.unlink()
    connection = open_board(board)
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            assignee TEXT,
            status TEXT NOT NULL,
            priority INTEGER,
            created_at INTEGER,
            block_kind TEXT,
            workspace_kind TEXT,
            workspace_path TEXT,
            idempotency_key TEXT
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            run_id INTEGER,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE task_links (
            parent_id TEXT NOT NULL,
            child_id TEXT NOT NULL
        );
        CREATE TABLE task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            author TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )
    for task in tasks:
        connection.execute(
            "INSERT INTO tasks(id, title, body, assignee, status, priority, created_at, "
            "block_kind, workspace_kind, workspace_path, idempotency_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task["id"],
                task.get("title", task["id"]),
                task.get("body"),
                task.get("assignee"),
                task["status"],
                task.get("priority", 0),
                task.get("created_at", NOW),
                task.get("block_kind"),
                task.get("workspace_kind"),
                task.get("workspace_path"),
                task.get("idempotency_key"),
            ),
        )
    event_id = 1
    for task_id, event_list in (events or {}).items():
        for kind, created_at, payload in event_list:
            connection.execute(
                "INSERT INTO task_events(id, task_id, kind, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, task_id, kind, payload, created_at),
            )
            event_id += 1
    for parent_id, child_id in (links or []):
        connection.execute(
            "INSERT INTO task_links(parent_id, child_id) VALUES (?, ?)",
            (parent_id, child_id),
        )
    for task_id, comment_list in (comments or {}).items():
        for author, created_at, body in comment_list:
            connection.execute(
                "INSERT INTO task_comments(task_id, author, body, created_at) "
                "VALUES (?, ?, ?, ?)",
                (task_id, author, body, created_at),
            )
    connection.commit()
    connection.close()
    return board


def review_task(
    task_id: str = "t_review01",
    title: str = "review: validate web sync — SSE progress bar",
    status: str = "blocked",
    workspace: str | None = "/srv/repos/webapp",
    **overrides: Any,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "assignee": "reviewer",
        "status": status,
        "workspace_kind": "worktree" if workspace else None,
        "workspace_path": workspace,
        "created_at": NOW - 7200,
    }
    task.update(overrides)
    return task


def fix_task(
    task_id: str = "t_fix0001",
    title: str = "fix: validate web sync — SSE progress bar findings",
    status: str = "done",
    workspace: str | None = "/srv/repos/webapp/.worktrees/t_fix0001",
    **overrides: Any,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "assignee": "developer",
        "status": status,
        "workspace_kind": "worktree" if workspace else None,
        "workspace_path": workspace,
        "created_at": NOW - 3600,
    }
    task.update(overrides)
    return task


def make_config(native_boards_root: Path | None = None, **overrides: Any) -> ControllerConfig:
    return ControllerConfig(
        "test",
        native_boards_root or Path("/tmp/nonexistent-boards"),
        Path("/tmp/nonexistent-state.sqlite3"),
        watcher=WatcherConfig(**overrides),
    )


def open_board(board: Path) -> sqlite3.Connection:
    """Open a fixture board with the production row factory."""
    connection = sqlite3.connect(board / "kanban.db")
    connection.row_factory = sqlite3.Row
    return connection


def make_git_repo(tmp_path: Path) -> tuple[Path, str, str]:
    """Create a real git repo with a canonical branch and one merged commit."""
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", "-b", "master"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=repo, check=True)
    (repo / "f.txt").write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "add", "f.txt"], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "impl"], cwd=repo, check=True
    )
    impl_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    (repo / "f.txt").write_text("two\n", encoding="utf-8")
    subprocess.run(["git", "add", "f.txt"], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "fix"], cwd=repo, check=True
    )
    fix_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    return repo, impl_sha, fix_sha


# ---------------------------------------------------------------------------
# H1 - defect-block discovery and fix-card planning
# ---------------------------------------------------------------------------


def test_discovers_high_defect_block_replay(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "rentcli",
        [review_task(workspace="/srv/repos/webapp")],
        events={
            "t_review01": [
                ("created", NOW - 7600, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT)),
            ]
        },
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    assert len(blocks) == 1
    block = blocks[0]
    assert block.task_id == "t_review01"
    assert block.severity == "HIGH"
    assert block.event_id == 2
    assert "RENTCLI_DB" in block.payload


def test_discovers_fix_ready_block(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_review02", "review: validate needs-input-watcher v2 implementation")],
        events={"t_review02": [("blocked", NOW - 7200, json.dumps(HKRC_FIX_READY))]},
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    assert len(blocks) == 1
    assert blocks[0].severity == "MEDIUM"
    assert blocks[0].task_id == "t_review02"


def test_pick_gate_reason_is_not_a_defect_block(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_parked", status="blocked")],
        events={"t_parked": [("blocked", NOW - 600, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"}))]},
    )
    assert discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False) == []


def test_non_reviewer_assignee_is_not_a_defect_block(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "rentcli",
        [review_task("t_dev", assignee="developer")],
        events={"t_dev": [("blocked", NOW - 600, json.dumps(RENTCLI_HIGH_DEFECT))]},
    )
    assert discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False) == []


def test_recency_window_skips_old_blocks(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "rentcli",
        [review_task("t_old", workspace=None)],
        events={"t_old": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
    )
    config = make_config(max_block_age_seconds=1800)
    # Enforced: the 2h-old block is skipped.
    assert discover_defect_blocks(root, config, now=NOW, enforce_recency=True) == []
    # Replay: the window is ignored.
    assert len(discover_defect_blocks(root, config, now=NOW, enforce_recency=False)) == 1


def test_explicit_reviewer_profiles_allowlist(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "rentcli",
        [review_task("t_r", assignee="codechecker")],
        events={"t_r": [("blocked", NOW - 600, json.dumps(RENTCLI_HIGH_DEFECT))]},
    )
    # Default heuristic matches names containing "reviewer" only.
    assert discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False) == []
    config = make_config(reviewer_profiles=("codechecker",))
    blocks = discover_defect_blocks(root, config, now=NOW, enforce_recency=False)
    assert len(blocks) == 1


def test_defect_block_author_fallback_triggers_h1(tmp_path: Path) -> None:
    """DEF-3: H1 triggers when the blocked event's author/actor is a
    reviewer profile even when the task assignee is not a reviewer."""
    root = tmp_path / "boards"
    payload = {
        "reason": "HIGH defect: start_sync_in_background uses the wrong DB path; developer must fix.",
        "kind": "needs_input",
        "author": "reviewer",
    }
    make_board(
        root,
        "rentcli",
        [review_task("t_dev", assignee="developer", status="blocked")],
        events={"t_dev": [("blocked", NOW - 600, json.dumps(payload))]},
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    assert len(blocks) == 1
    assert blocks[0].task_id == "t_dev"
    # Actor key works too.
    payload2 = dict(payload, author="", actor="code-reviewer")
    make_board(
        root,
        "other",
        [review_task("t_dev2", assignee="developer", status="blocked")],
        events={"t_dev2": [("blocked", NOW - 600, json.dumps(payload2))]},
    )
    config = make_config(reviewer_profiles=("code-reviewer",))
    blocks2 = discover_defect_blocks(root, config, now=NOW, enforce_recency=False)
    assert any(block.task_id == "t_dev2" for block in blocks2)
    # Neither author nor actor reviewer -> no block (fresh root so the
    # qualifying boards above do not leak into the negative case).
    payload3 = {"reason": "HIGH defect: x", "kind": "needs_input", "author": "developer"}
    make_board(
        tmp_path / "neg",
        "none",
        [review_task("t_dev3", assignee="developer", status="blocked")],
        events={"t_dev3": [("blocked", NOW - 600, json.dumps(payload3))]},
    )
    assert discover_defect_blocks(tmp_path / "neg", make_config(), now=NOW, enforce_recency=False) == []


def test_fix_card_title_strips_review_prefix() -> None:
    assert (
        fix_card_title("review: validate web sync — SSE progress bar behind settings panel")
        == "fix: validate web sync — SSE progress bar behind settings panel findings"
    )
    assert fix_card_title("review:validate X") == "fix: validate X findings"


def test_severity_parsing() -> None:
    assert defect_severity(RENTCLI_HIGH_DEFECT["reason"]) == "HIGH"
    assert defect_severity(HKRC_FIX_READY["reason"]) == "MEDIUM"
    assert defect_severity("LOW defect: typo in help text") == "LOW"
    assert defect_severity("FIX-READY: nitpick") == "LOW"


def test_is_reviewer_assignee() -> None:
    config = make_config()
    assert is_reviewer_assignee(config, "reviewer")
    assert is_reviewer_assignee(config, "Reviewer-2")
    assert not is_reviewer_assignee(config, "developer")
    assert not is_reviewer_assignee(config, None)
    assert is_reviewer_assignee(make_config(reviewer_profiles=("code-reviewer",)), "code-reviewer")
    assert not is_reviewer_assignee(make_config(reviewer_profiles=("code-reviewer",)), "reviewer")


def test_plan_fix_card_workspace_from_review(tmp_path: Path) -> None:
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [review_task("t_review01", workspace=str(repo))],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    assert len(blocks) == 1
    connection = open_board(board)
    try:
        plan = plan_fix_card(make_config(), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()
    assert plan.title == "fix: validate web sync — SSE progress bar findings"
    assert plan.review_id == "t_review01"
    assert plan.episode_key == "hkrc-fix-t_review01-1"
    assert plan.priority == 90  # HIGH
    assert plan.workspace == f"worktree:{repo}"
    assert "FIX-READY-ON-UNMERGED-IMPL RULE" in plan.body
    assert "RENTCLI_DB" in plan.body
    assert "origin/wt/" in plan.body
    # Completion contract: fix cards COMPLETE with review evidence when a
    # review child exists; review-required block is reserved for the
    # no-review-child case (review-required-promotion-deadlock convention).
    assert "COMPLETION CONTRACT" in plan.body
    assert "COMPLETE this fix card with review evidence" in plan.body
    assert "Block with `review-required` ONLY when no review child" in plan.body


def test_plan_fix_card_workspace_falls_back_to_impl(tmp_path: Path) -> None:
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [
            review_task("t_review01", workspace=None, status="blocked"),
            fix_task("t_impl01", title="task: implement web sync", status="done", workspace=str(repo)),
        ],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
        links=[("t_impl01", "t_review01")],
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    connection = open_board(board)
    try:
        plan = plan_fix_card(make_config(), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()
    assert plan.workspace == f"worktree:{repo}"


def test_plan_fix_card_skips_scratch_review_workspace(tmp_path: Path) -> None:
    """DEF-4: a review workspace with workspace_kind 'scratch' is ephemeral
    and must never anchor the fix card — the parent impl workspace (or board
    default_workdir) is used instead."""
    scratch_repo, _, _ = make_git_repo(tmp_path)
    impl_repo, _, _ = make_git_repo(tmp_path / "impl")
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [
            review_task(
                "t_review01",
                status="blocked",
                workspace=str(scratch_repo),
                workspace_kind="scratch",
            ),
            fix_task("t_impl01", title="task: implement web sync", status="done", workspace=str(impl_repo)),
        ],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
        links=[("t_impl01", "t_review01")],
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    connection = open_board(board)
    try:
        plan = plan_fix_card(make_config(), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()
    # The scratch review repo must NOT be selected; the impl workspace is.
    assert plan.workspace == f"worktree:{impl_repo}"
    assert plan.workspace != f"worktree:{scratch_repo}"


def test_plan_fix_card_scratch_review_falls_back_to_board_workdir(tmp_path: Path) -> None:
    """DEF-4 (board fallback): no usable impl workspace -> the board
    default_workdir is used when the review workspace is scratch."""
    scratch_repo, _, _ = make_git_repo(tmp_path)
    workdir_repo, _, _ = make_git_repo(tmp_path / "workdir")
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [
            review_task(
                "t_review01",
                status="blocked",
                workspace=str(scratch_repo),
                workspace_kind="scratch",
            ),
            fix_task("t_impl01", title="task: implement web sync", status="done", workspace=None),
        ],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
        links=[("t_impl01", "t_review01")],
        default_workdir=str(workdir_repo),
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    connection = open_board(board)
    try:
        plan = plan_fix_card(make_config(root), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()
    assert plan.workspace == f"worktree:{workdir_repo}"


def test_plan_fix_card_workspace_falls_back_to_board_workdir(tmp_path: Path) -> None:
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [review_task("t_review01", workspace=None)],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
        default_workdir=str(repo),
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    connection = open_board(board)
    try:
        plan = plan_fix_card(make_config(root), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()
    assert plan.workspace == f"worktree:{repo}"


def test_plan_fix_card_no_repo_raises(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [review_task("t_review01", workspace=None)],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
    )
    blocks = discover_defect_blocks(root, make_config(), now=NOW, enforce_recency=False)
    connection = open_board(board)
    try:
        with pytest.raises(WatcherError):
            plan_fix_card(make_config(), blocks[0], connection, None)  # type: ignore[arg-type]
    finally:
        connection.close()


def test_body_embeds_reviewer_reproduction_comments(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [review_task("t_review01")],
        events={"t_review01": [("blocked", NOW - 7200, json.dumps(RENTCLI_HIGH_DEFECT))]},
        comments={
            "t_review01": [
                ("reviewer", NOW - 7300, "Review found one blocking defect.\nRepro:\n- Set RENTCLI_DB=/tmp/x.db\n- Open /tmp/app.db explicitly"),
            ]
        },
    )
    connection = open_board(board)
    try:
        block = DefectBlock(
            board_slug="rentcli",
            task_id="t_review01",
            title="review: validate web sync",
            assignee="reviewer",
            event_id=1,
            blocked_at=NOW - 7200,
            reason=RENTCLI_HIGH_DEFECT["reason"],
            severity="HIGH",
            payload=json.dumps(RENTCLI_HIGH_DEFECT, indent=2),
        )
        body = build_fix_card_body(make_config(), block, connection)
    finally:
        connection.close()
    assert "REPRODUCTION STEPS" in body
    assert "Set RENTCLI_DB=/tmp/x.db" in body
    assert '"reason": "HIGH defect:' in body  # payload verbatim


def test_existing_fix_cards_and_open_rule(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "rentcli",
        [
            review_task("t_review01"),
            fix_task("t_fix_child", status="done"),
            fix_task("t_fix_legacy", title="fix: web sync worker carries app DB path (HIGH defect t_review01)", status="running"),
            fix_task("t_fix_key", status="done", idempotency_key="hkrc-fix-t_review01-42"),
        ],
        links=[("t_review01", "t_fix_child")],
    )
    connection = open_board(board)
    try:
        cards = existing_fix_cards(connection, "t_review01")
    finally:
        connection.close()
    ids = {card[0] for card in cards}
    assert ids == {"t_fix_child", "t_fix_legacy", "t_fix_key"}
    # Any open (non-done/archived) fix card blocks a new one.
    assert has_open_fix_card(cards)
    assert not has_open_fix_card([("a", "fix: x", "done"), ("b", "fix: y", "archived")])


# ---------------------------------------------------------------------------
# H2 - supersede discovery, merge verification
# ---------------------------------------------------------------------------


def test_extract_shas_and_task_ids() -> None:
    shas = extract_shas("merged fix 704d81d into main as 7eac6ff", "commit abec308", "no shas here")
    assert shas == ("704d81d", "7eac6ff", "abec308")
    assert extract_shas("nothing to see 1234") == ()
    assert extract_task_ids("fix: web sync worker carries app DB path (HIGH defect t_b992ba30)") == (
        "t_b992ba30",
    )
    assert extract_task_ids("no ids") == ()


def test_verify_merged_accepts_ancestor_and_rejects_others(tmp_path: Path) -> None:
    repo, impl_sha, fix_sha = make_git_repo(tmp_path)
    config = make_config()
    # The fix commit is an ancestor of master -> verified with the full sha.
    assert verify_merged(repo, [fix_sha], config, None) == fix_sha  # type: ignore[arg-type]
    # An abbreviated ancestor resolves too.
    assert verify_merged(repo, [fix_sha[:7]], config, None) == fix_sha  # type: ignore[arg-type]
    # A commit that never existed is not verified.
    assert verify_merged(repo, ["deadbeef"], config, None) is None
    # A dangling commit (impl on a side branch) is not verified either.
    (repo / "other.txt").write_text("x\n", encoding="utf-8")
    import subprocess

    subprocess.run(["git", "checkout", "-q", "-b", "side"], cwd=repo, check=True)
    subprocess.run(["git", "add", "other.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "side"], cwd=repo, check=True)
    side_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    subprocess.run(["git", "checkout", "-q", "master"], cwd=repo, check=True)
    assert verify_merged(repo, [side_sha], config, None) is None
    assert impl_sha  # impl commit is an ancestor of master too
    assert verify_merged(repo, [impl_sha], config, None) == impl_sha


def test_verify_merged_falls_back_to_main(tmp_path: Path) -> None:
    import subprocess

    repo = tmp_path / "repo-main"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "test"], cwd=repo, check=True)
    (repo / "f.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "f.txt"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "x"], cwd=repo, check=True)
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    assert verify_merged(repo, [sha], make_config(), None) == sha  # type: ignore[arg-type]


def test_discover_supersede_candidates_parent_edge_and_title_id(tmp_path: Path) -> None:
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "rentcli",
        [
            review_task("t_a1111111", status="blocked", workspace=str(repo)),
            fix_task("t_b2222222", workspace=str(repo), title="fix: web sync findings"),
            fix_task("t_c3333333", workspace=str(repo), title="fix: web sync worker carries app DB path (HIGH defect t_a1111111)"),
            fix_task("t_d4444444", workspace=str(repo), title="fix: something unrelated"),
        ],
        links=[("t_a1111111", "t_b2222222")],
        events={
            "t_b2222222": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
            "t_c3333333": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
            "t_d4444444": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
        },
    )
    candidates = discover_supersede_candidates(root, make_config(root), None)  # type: ignore[arg-type]
    by_fix = {candidate.fix_id: candidate for candidate in candidates}
    # The parent edge and the legacy title-id path both map to the review;
    # the orphan (no edge, no review id in title) maps to nothing.
    assert set(by_fix) == {"t_b2222222", "t_c3333333"}
    assert by_fix["t_b2222222"].review_id == "t_a1111111"
    assert by_fix["t_c3333333"].review_id == "t_a1111111"
    assert by_fix["t_b2222222"].shas == (fix_sha,)
    # A review that is not blocked yields no candidate.
    make_board(
        root,
        "other",
        [
            review_task("t_e5555555", status="done", workspace=str(repo)),
            fix_task("t_f6666666", workspace=str(repo)),
        ],
        links=[("t_e5555555", "t_f6666666")],
        events={"t_f6666666": [("completed", NOW - 100, json.dumps({"summary": f"merged {fix_sha}"}))]},
    )
    candidates2 = discover_supersede_candidates(root, make_config(root), None)  # type: ignore[arg-type]
    assert all(candidate.board_slug == "rentcli" for candidate in candidates2)


# ---------------------------------------------------------------------------
# H3 - pick-gate candidates, safety, selection
# ---------------------------------------------------------------------------


def parked_task(
    task_id: str,
    priority: int = 0,
    created_at: int = NOW - 600,
    reason: str = PICK_GATE_REASON,
    **overrides: Any,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": f"task: {task_id}",
        "assignee": "developer",
        "status": "blocked",
        "priority": priority,
        "created_at": created_at,
        "block_kind": "needs_input",
    }
    task.update(overrides)
    return task


def test_discover_pick_gate_candidates(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "hkrc",
        [
            parked_task("t_gate1"),
            parked_task("t_gate2", reason="different reason"),
            parked_task("t_capability", reason="capability block", block_kind="capability"),
        ],
        events={
            "t_gate1": [("blocked", NOW - 600, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"}))],
            "t_gate2": [("blocked", NOW - 600, json.dumps({"reason": "different reason", "kind": "needs_input"}))],
            "t_capability": [("blocked", NOW - 600, json.dumps({"reason": "capability block", "kind": "capability"}))],
        },
    )
    connection = open_board(board)
    try:
        candidates = discover_pick_gate_candidates(connection, make_config())
        assert [candidate.task_id for candidate in candidates] == ["t_gate1"]
        assert board_has_capability_block(connection)
    finally:
        connection.close()


def test_select_pick_gate_priority_then_created_at(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "hkrc",
        [
            parked_task("t_low_early", priority=0, created_at=100),
            parked_task("t_high_late", priority=10, created_at=300),
            parked_task("t_low_late", priority=0, created_at=200),
        ],
        events={
            "t_low_early": [("blocked", NOW - 600, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"}))],
            "t_high_late": [("blocked", NOW - 300, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"}))],
            "t_low_late": [("blocked", NOW - 400, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"}))],
        },
    )
    connection = open_board(board)
    try:
        candidates = discover_pick_gate_candidates(connection, make_config())
    finally:
        connection.close()
    # Highest priority wins regardless of age.
    assert select_pick_gate(candidates).task_id == "t_high_late"  # type: ignore[union-attr]
    # Tie on priority: earliest created_at wins.
    low = [candidate for candidate in candidates if candidate.priority == 0]
    assert select_pick_gate(low).task_id == "t_low_early"  # type: ignore[union-attr]


def test_pick_gate_skip_reason_hold_and_parents(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "hkrc",
        [
            parked_task("t_hold_reason", reason="One-at-a-time: hold for Andre"),
            parked_task("t_hold_comment"),
            parked_task("t_parented"),
            parked_task("t_open_parent"),
            parked_task("t_clear"),
            {"id": "t_done_parent", "title": "task: done parent", "assignee": "developer",
             "status": "done", "created_at": NOW - 9000},
            {"id": "t_running_parent", "title": "task: running parent", "assignee": "developer",
             "status": "running", "created_at": NOW - 9000},
        ],
        links=[("t_done_parent", "t_parented"), ("t_running_parent", "t_open_parent")],
        comments={"t_hold_comment": [("main", NOW - 60, "Please hold this card")]},
    )
    config = make_config(hold_comment_window_seconds=3600)
    connection = open_board(board)
    try:
        from hkrc.watcher import PickGateCandidate

        base = PickGateCandidate("hkrc", "t_x", "task: t_x", 0, NOW - 600, PICK_GATE_REASON)
        assert pick_gate_skip_reason(connection, config, base, now=NOW) is None
        hold_reason = PickGateCandidate("hkrc", "t_hold_reason", "task", 0, NOW - 600, "One-at-a-time: hold for Andre")
        assert pick_gate_skip_reason(connection, config, hold_reason, now=NOW) is not None
        hold_comment = PickGateCandidate("hkrc", "t_hold_comment", "task", 0, NOW - 600, PICK_GATE_REASON)
        assert pick_gate_skip_reason(connection, config, hold_comment, now=NOW) is not None
        parented = PickGateCandidate("hkrc", "t_parented", "task", 0, NOW - 600, PICK_GATE_REASON)
        assert pick_gate_skip_reason(connection, config, parented, now=NOW) is None  # parent done
        open_parent = PickGateCandidate("hkrc", "t_open_parent", "task", 0, NOW - 600, PICK_GATE_REASON)
        assert pick_gate_skip_reason(connection, config, open_parent, now=NOW) is not None
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# H4 - blocked-without-event discovery
# ---------------------------------------------------------------------------


def test_discover_missing_block_events(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    board = make_board(
        root,
        "hkrc",
        [
            parked_task("t_trap", status="blocked"),
            parked_task("t_ok", status="blocked"),
        ],
        events={
            "t_trap": [("created", NOW - 100, json.dumps({"assignee": "developer", "status": "blocked"}))],
            "t_ok": [
                ("created", NOW - 100, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", NOW - 50, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"})),
            ],
        },
    )
    connection = open_board(board)
    try:
        missing = discover_missing_block_events(connection, "hkrc")
    finally:
        connection.close()
    assert [item.task_id for item in missing] == ["t_trap"]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_watcher_config_validation() -> None:
    with pytest.raises(ConfigError):
        WatcherConfig(enabled="yes")  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        WatcherConfig(max_block_age_seconds=True)
    with pytest.raises(ConfigError):
        WatcherConfig(max_block_age_seconds=0)
    with pytest.raises(ConfigError):
        WatcherConfig(reviewer_profiles=("a", "a"))
    with pytest.raises(ConfigError):
        WatcherConfig(canonical_branch="master", canonical_branch_fallback="master")
    with pytest.raises(ConfigError):
        WatcherConfig(guard_reason="")
    WatcherConfig(reviewer_profiles=("reviewer", "code-reviewer"))
    WatcherConfig(recv_timeout_seconds=2.5)
    # DEF-9: recv_timeout_seconds must stay strictly below the cron cycle
    # interval (#77833 leak rule).
    with pytest.raises(ConfigError):
        WatcherConfig(recv_timeout_seconds=300, cycle_interval_seconds=300)
    with pytest.raises(ConfigError):
        WatcherConfig(recv_timeout_seconds=301, cycle_interval_seconds=300)
    with pytest.raises(ConfigError):
        WatcherConfig(cycle_interval_seconds=0)
    WatcherConfig(recv_timeout_seconds=10, cycle_interval_seconds=300)
    # H5 debounce must be a positive number.
    with pytest.raises(ConfigError):
        WatcherConfig(deadlock_min_age_seconds=True)  # type: ignore[arg-type]
    with pytest.raises(ConfigError):
        WatcherConfig(deadlock_min_age_seconds=0)
    WatcherConfig(deadlock_min_age_seconds=900)


def test_watcher_config_toml_roundtrip(tmp_path: Path) -> None:
    config = make_config(
        reviewer_profiles=("reviewer",),
        max_block_age_seconds=900,
        canonical_branch="main",
        canonical_branch_fallback="master",
    )
    path = tmp_path / "config.toml"
    write_config(path, config)
    loaded = load_config(path)
    assert loaded.watcher == config.watcher
    assert "[watcher]" in path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# run() orchestration with fake stream wiring
# ---------------------------------------------------------------------------


def _scenario_adapter(
    batches: list[list[tuple[int, str, str, object]]],
    board_slug: str = "hkrc",
) -> StreamAdapter:
    """Build a real StreamAdapter fed by scripted wire frames.

    Each batch is a list of ``(event_id, kind, task_id, payload)`` rows.
    Frames are duplicated so the adapter supports two consecutive runs: the
    first connection script is consumed by the first pass, and a reconnect
    (replay from cursor zero, or a dry-run->live cursor cutover) consumes the
    second.  Sockets go idle after their frames (``idle=True``) so a reused
    persistent socket times out cleanly instead of disconnecting.
    """
    from fixtures.event_stream import StreamEvent, WebSocketScenario

    scenario = WebSocketScenario(idle=True)
    for batch in batches:
        rows = [
            StreamEvent(
                event_id=event_id,
                kind=kind,
                task_id=task_id,
                payload=payload,
                created_at=NOW - 1000,
            )
            for event_id, kind, task_id, payload in batch
        ]
        frame = json.dumps({
            "events": [
                {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
                 "payload": e.payload, "created_at": e.created_at}
                for e in rows
            ],
            "cursor": rows[-1].event_id if rows else 0,
        })
        scenario.connections.append([frame])
        scenario.connections.append([frame])
    return StreamAdapter(
        "ws://127.0.0.1:1/events",
        allowed_boards={board_slug},
        connector=scenario.connector,
    )


class RecordingRunner:
    """Fake native runner that records every argv list."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.results: dict[str, NativeResult] = {}

    def __call__(self, command: Sequence[str]) -> NativeResult:
        self.calls.append(list(command))
        key = " ".join(command)
        return self.results.get(key, NativeResult(0, "ok", ""))


def test_run_replay_dry_run_reproduces_all_four_patterns(tmp_path: Path) -> None:
    """Replay from cursor zero reports H1/H2/H3/H4 would-have actions."""
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_review02", title="review: validate needs-input-watcher v2 implementation", workspace=str(repo), status="blocked"),
            parked_task("t_gate1", created_at=NOW - 400),
            fix_task("t_fix01", workspace=str(repo), status="done"),
        ],
        events={
            "t_review02": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 4000, json.dumps(HKRC_FIX_READY)),
            ],
            "t_gate1": [
                ("created", NOW - 500, json.dumps({"assignee": "developer", "status": "blocked"})),
                ("blocked", NOW - 400, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"})),
            ],
            "t_fix01": [
                ("created", NOW - 300, json.dumps({"assignee": "developer", "status": "ready"})),
                ("completed", NOW - 200, json.dumps({"summary": f"Fixed and merged {fix_sha} on master."})),
            ],
        },
        links=[("t_review02", "t_fix01")],
    )
    adapter = _scenario_adapter([
        [
            (1, "created", "t_review02", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_review02", HKRC_FIX_READY),
            (3, "created", "t_gate1", {"assignee": "developer", "status": "blocked"}),
            (4, "blocked", "t_gate1", {"reason": PICK_GATE_REASON, "kind": "needs_input"}),
            (5, "created", "t_fix01", {"assignee": "developer", "status": "ready"}),
            (6, "completed", "t_fix01", {"summary": f"Fixed and merged {fix_sha} on master."}),
            (7, "completed", "t_gate1", {"summary": "done"}),
        ]
    ], "hkrc")
    state_path = tmp_path / "watcher-state.json"
    actions, message = run(
        make_config(root),
        state_path=state_path,
        dry_run=True,
        now=NOW,
        replay=True,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
    )
    kinds = {(action.handler, action.kind) for action in actions}
    # H1: defect block -> fix card. H2: fix done + merged -> supersede.
    # H3: completion -> pick-gate advance. H4: created-with-status-blocked
    # without a blocked event -> guard.
    assert ("1", "create_fix_card") in kinds
    assert ("2", "supersede_review") in kinds
    assert ("3", "advance_pick_gate") in kinds
    assert ("4", "write_block_event") in kinds
    assert all(action.would for action in actions)
    assert "watcher dry-run:" in message
    # The H2 would-have names the verified merge SHA.
    h2 = [action for action in actions if action.handler == "2"]
    assert h2 and f"merged {fix_sha} (verified)" in h2[0].detail
    # State file records cursors and would-namespaced action keys.
    state = json.loads(state_path.read_text(encoding="utf-8"))
    # Replay is a dry-run: it advances the dry-run cursor namespace only, so
    # the live cursor is never consumed by the review period (DEF-2).
    assert state["dry_run_cursors"]["hkrc"] == 7
    assert "hkrc" not in state["cursors"]
    assert any(key.startswith("would:fix:hkrc:t_review02:") for key in state["actions"])
    assert any(key.startswith("would:supersede:hkrc:t_review02") for key in state["actions"])
    assert any(key.startswith("would:pick:hkrc:t_gate1") for key in state["actions"])
    assert any(key.startswith("would:guard:hkrc:t_gate1") for key in state["actions"])


def test_run_replay_h2_supersedes_via_fix_review_child(tmp_path: Path) -> None:
    """The rentcli pattern: the fix card's own summary has no SHA, the merged
    SHA lives in its done fix review's completion — H2 fires when the fix
    review completes, not when the developer says done."""
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_a1111111", status="blocked", workspace=str(repo)),
            fix_task("t_b2222222", workspace=str(repo), status="done"),
            fix_task("t_c3333333", title="review: validate fix (t_b2222222)", assignee="reviewer", status="done", workspace=str(repo)),
        ],
        links=[("t_a1111111", "t_b2222222"), ("t_b2222222", "t_c3333333")],
        events={
            "t_a1111111": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 4000, json.dumps(HKRC_FIX_READY)),
            ],
            "t_b2222222": [
                ("created", NOW - 300, json.dumps({"assignee": "developer", "status": "ready"})),
                ("completed", NOW - 200, json.dumps({"summary": "Implemented the fix on wt/t_b2222222."})),
            ],
            "t_c3333333": [
                ("created", NOW - 150, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("completed", NOW - 100, json.dumps({"summary": f"Approved and merged fix {fix_sha} into master."})),
            ],
        },
    )
    adapter = _scenario_adapter([
        [
            (1, "created", "t_a1111111", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_a1111111", HKRC_FIX_READY),
            (3, "created", "t_b2222222", {"assignee": "developer", "status": "ready"}),
            (4, "completed", "t_b2222222", {"summary": "Implemented the fix on wt/t_b2222222."}),
            (5, "created", "t_c3333333", {"assignee": "reviewer", "status": "ready"}),
            (6, "completed", "t_c3333333", {"summary": f"Approved and merged fix {fix_sha} into master."}),
        ]
    ], "hkrc")
    actions, _ = run(
        make_config(root),
        state_path=tmp_path / "h2-state.json",
        dry_run=True,
        now=NOW,
        replay=True,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
    )
    h2 = [action for action in actions if action.handler == "2"]
    assert len(h2) == 1
    assert h2[0].target_id == "t_a1111111"
    assert f"merged {fix_sha} (verified)" in h2[0].detail


def test_run_double_run_is_idempotent(tmp_path: Path) -> None:
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_review02", workspace=str(repo), status="blocked")],
        events={
            "t_review02": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 4000, json.dumps(HKRC_FIX_READY)),
            ]
        },
    )
    config = make_config(root)
    adapter = _scenario_adapter([
        [
            (1, "created", "t_review02", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_review02", HKRC_FIX_READY),
        ]
    ], "hkrc")
    state_path = tmp_path / "watcher-state.json"
    first, _ = run(
        config,
        state_path=state_path,
        dry_run=True,
        now=NOW,
        replay=True,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
    )
    assert len(first) == 1 and first[0].handler == "1"
    # Second identical run against the same state file: the would-key
    # suppresses the duplicate would-have create (no duplicate fix cards).
    second, message = run(
        config,
        state_path=state_path,
        dry_run=True,
        now=NOW,
        replay=True,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
    )
    assert second == []
    assert message == ""


def test_run_dry_run_then_live_cutover_performs_action(tmp_path: Path) -> None:
    """DEF-2: a dry-run review period never consumes the live cursor or live
    action eligibility — after the dry-run, live with the same state file and
    no newer event still performs the would-have action."""
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_review02", workspace=str(repo), status="blocked")],
        events={
            "t_review02": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 300, json.dumps(HKRC_FIX_READY)),
            ]
        },
    )
    config = make_config(root)
    adapter = _scenario_adapter([
        [
            (1, "created", "t_review02", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_review02", HKRC_FIX_READY),
        ]
    ], "hkrc")
    state_path = tmp_path / "watcher-state.json"
    runner = RecordingRunner()

    # Dry-run pass: would-have action recorded, dry-run cursor advanced, live
    # cursor untouched.
    dry_actions, _ = run(
        config,
        state_path=state_path,
        dry_run=True,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
    )
    assert len(dry_actions) == 1 and dry_actions[0].would
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["dry_run_cursors"]["hkrc"] == 2
    assert "hkrc" not in state["cursors"]
    assert any(key.startswith("would:fix:hkrc:t_review02:") for key in state["actions"])

    # Live cutover with the same state file and the same adapter, no newer
    # event: the fix card is actually created (live cursor/eligibility were
    # never consumed by the dry-run).
    live_actions, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    create = [call for call in runner.calls if "create" in call]
    assert len(create) == 1
    assert any("fix:" in " ".join(call) for call in create)
    assert any(not action.would and action.kind == "create_fix_card" for action in live_actions)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["cursors"]["hkrc"] == 2
    assert any(key.startswith("fix:hkrc:t_review02:") for key in state["actions"])


def test_run_replay_without_dry_run_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(WatcherError):
        run(
            make_config(),
            state_path=tmp_path / "state.json",
            dry_run=False,
            replay=True,
            adapters={},
            credentials=StreamCredentials(token="s"),
        )


def test_run_live_creates_fix_card_and_advances_gate(tmp_path: Path) -> None:
    """Live mode performs the native CLI mutations via the injected runner."""
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_review02", workspace=str(repo), status="blocked"),
            parked_task("t_gate1"),
        ],
        events={
            "t_review02": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 300, json.dumps(HKRC_FIX_READY)),
            ],
            "t_gate1": [
                ("created", NOW - 500, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", NOW - 400, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"})),
                ("completed", NOW - 300, json.dumps({"summary": "done"})),
            ],
        },
    )
    config = make_config(root)
    adapter = _scenario_adapter([
        [
            (1, "created", "t_review02", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_review02", HKRC_FIX_READY),
            (3, "created", "t_gate1", {"assignee": "developer", "status": "ready"}),
            (4, "blocked", "t_gate1", {"reason": PICK_GATE_REASON, "kind": "needs_input"}),
            (5, "completed", "t_gate1", {"summary": "done"}),
        ]
    ], "hkrc")
    runner = RecordingRunner()
    actions, message = run(
        config,
        state_path=tmp_path / "live-state.json",
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    assert not any(action.would for action in actions)
    create = [call for call in runner.calls if "create" in call]
    assert len(create) == 1
    create_call = create[0]
    assert "fix:" in " ".join(create_call)
    assert "--idempotency-key" in create_call
    assert "hkrc-fix-t_review02-2" in create_call
    assert "--parent" in create_call and "t_review02" in create_call
    assert "--priority" in create_call and "60" in create_call  # MEDIUM
    assert "--workspace" in create_call
    assert any("unblock" in call for call in runner.calls)
    assert message.startswith("watcher: ")


def test_run_live_h4_guard_writes_block_event_and_falls_back(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [parked_task("t_trap", status="blocked")],
        events={
            "t_trap": [("created", NOW - 100, json.dumps({"assignee": "developer", "status": "blocked"}))],
        },
    )
    config = make_config(root)
    adapter = _scenario_adapter([
        [(1, "created", "t_trap", {"assignee": "developer", "status": "blocked"})]
    ], "hkrc")
    runner = RecordingRunner()

    def block_fails_once(command: Sequence[str]) -> NativeResult:
        runner.calls.append(list(command))
        if "block" in command and "unblock" not in command:
            already_unblocked = any(
                "unblock" in call and "t_trap" in call for call in runner.calls[:-1]
            )
            if not already_unblocked:
                return NativeResult(1, "", "cannot block (not running/ready)")
        return NativeResult(0, "ok", "")

    actions, _ = run(
        config,
        state_path=tmp_path / "h4-state.json",
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=block_fails_once,  # type: ignore[arg-type]
    )
    guard = [action for action in actions if action.handler == "4"]
    assert len(guard) == 1
    assert not guard[0].would
    assert "wrote missing block event" in guard[0].detail
    # Direct block failed (task still 'blocked') -> unblock then block fallback.
    command_texts = [" ".join(call) for call in runner.calls]
    assert any("unblock" in text and "t_trap" in text for text in command_texts)
    block_calls = [text for text in command_texts if " block " in text]
    assert len(block_calls) == 2  # first attempt + retry after unblock


def test_stream_error_fails_closed_per_board(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "hkrc", [parked_task("t_gate1")])
    config = make_config(root)

    from fixtures.event_stream import WebSocketScenario

    scenario = WebSocketScenario()
    scenario.connect_errors.append(ConnectionError("no route to host"))
    adapter = StreamAdapter(
        "ws://127.0.0.1:1/events",
        allowed_boards={"hkrc"},
        connector=scenario.connector,
    )
    actions, _ = run(
        config,
        state_path=tmp_path / "err-state.json",
        dry_run=True,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        git_runner=lambda _command: NativeResult(2, "", "no git"),
    )
    assert len(actions) == 1
    assert actions[0].kind == "stream_error"
    state = json.loads((tmp_path / "err-state.json").read_text(encoding="utf-8"))
    assert "hkrc" not in state["cursors"]  # cursor untouched on failure


# ---------------------------------------------------------------------------
# Regression tests for the t_c2dc0d72 review findings
# ---------------------------------------------------------------------------


class FlakyRunner:
    """Native runner that fails a chosen command the first N times."""

    def __init__(self, fail_keyword: str, failures: int = 1) -> None:
        self.calls: list[list[str]] = []
        self.fail_keyword = fail_keyword
        self.failures = failures
        self.failed_attempts = 0

    def __call__(self, command: Sequence[str]) -> NativeResult:
        self.calls.append(list(command))
        if self.fail_keyword in " ".join(command) and self.failed_attempts < self.failures:
            self.failed_attempts += 1
            return NativeResult(1, "", "transient native failure")
        return NativeResult(0, "ok", "")


def test_failed_h1_create_is_retried_next_pass(tmp_path: Path) -> None:
    """DEF-5: a failed H1 create does not consume the action key, so the live
    H1 state scan retries the mutation on the next pass and the fix card is
    eventually created."""
    repo, _, _ = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_review02", workspace=str(repo), status="blocked")],
        events={
            "t_review02": [
                ("created", NOW - 5000, json.dumps({"assignee": "reviewer", "status": "ready"})),
                ("blocked", NOW - 300, json.dumps(HKRC_FIX_READY)),
            ]
        },
    )
    config = make_config(root)
    state_path = tmp_path / "live-state.json"
    # Fail both the event-path create AND the same-pass state-scan retry, so
    # the second pass genuinely has to retry.
    flaky = FlakyRunner("create", failures=2)

    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([[
            (1, "created", "t_review02", {"assignee": "reviewer", "status": "ready"}),
            (2, "blocked", "t_review02", HKRC_FIX_READY),
        ]], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
    )
    # The failure is reported in the digest but the action key is NOT consumed.
    assert any("FAILED create fix card" in action.detail for action in first)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert not any(key.startswith("fix:hkrc:t_review02:") for key in state["actions"])
    assert not any(key.startswith("would:fix:hkrc:t_review02:") for key in state["actions"])

    # Second live pass: the durable cursor already advanced past the blocked
    # event, so only the H1 state scan sees it — and now the create succeeds.
    flaky.failures = 0
    from fixtures.event_stream import WebSocketScenario

    scenario = WebSocketScenario(idle=True)
    scenario.connections.append([json.dumps({"events": [], "cursor": 2})])
    idle_adapter = StreamAdapter(
        "ws://127.0.0.1:1/events",
        allowed_boards={"hkrc"},
        connector=scenario.connector,
    )
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": idle_adapter},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
    )
    create = [call for call in flaky.calls if "create" in call]
    assert len(create) == 3  # event path + same-pass scan failed, next pass succeeded
    assert any(action.kind == "create_fix_card" and not action.would and "FAILED" not in action.detail
               for action in second)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("fix:hkrc:t_review02:") for key in state["actions"])


def test_failed_h2_complete_is_retried_next_pass(tmp_path: Path) -> None:
    """DEF-5/DEF-7: a failed supersede complete (or an unverified merge) does
    not consume the supersede key; once the merge lands, the review closes."""
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_a1111111", status="blocked", workspace=str(repo)),
            fix_task("t_b2222222", workspace=str(repo), status="done"),
        ],
        links=[("t_a1111111", "t_b2222222")],
        events={
            "t_b2222222": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
        },
    )
    config = make_config(root)
    state_path = tmp_path / "h2-state.json"
    flaky = FlakyRunner("complete", failures=1)

    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(0, fix_sha, ""),
    )
    assert any("FAILED complete" in action.detail for action in first)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert not any(key.startswith("supersede:hkrc:") for key in state["actions"])

    flaky.failures = 0
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(0, fix_sha, ""),
    )
    assert any(action.kind == "supersede_review" and not action.would and "FAILED" not in action.detail
               for action in second)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("supersede:hkrc:t_a1111111") for key in state["actions"])


def test_h2_unverified_merge_defers_then_supersedes(tmp_path: Path) -> None:
    """DEF-7: an unverified merge-base check must not memoize the supersede;
    after the SHA is merged to canonical the original review is superseded."""
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_a1111111", status="blocked", workspace=str(repo)),
            fix_task("t_b2222222", workspace=str(repo), status="done"),
        ],
        links=[("t_a1111111", "t_b2222222")],
        events={
            "t_b2222222": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
        },
    )
    config = make_config(root)
    state_path = tmp_path / "h2-state.json"
    runner = RecordingRunner()
    # First pass: merge-base verification fails -> deferred silently, no key.
    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(2, "", "merge-base check failed"),
    )
    assert first == [] or all("FAILED" not in action.detail for action in first)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert not any(key.startswith("supersede:hkrc:") for key in state["actions"])

    # Second pass: the SHA is now merged -> the supersede fires.
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(0, fix_sha, ""),
    )
    h2 = [action for action in second if action.kind == "supersede_review"]
    assert len(h2) == 1 and not h2[0].would
    assert f"merged {fix_sha} (verified)" in h2[0].detail
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("supersede:hkrc:t_a1111111") for key in state["actions"])


def test_h3_safety_skip_defers_until_parent_done(tmp_path: Path) -> None:
    """DEF-6: a pick-gate candidate skipped for 'parents not done' must not
    consume the pick key; after the parent completes the candidate unblocks."""
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            parked_task("t_gate1"),
            {
                "id": "t_running_parent", "title": "task: running parent",
                "assignee": "developer", "status": "running", "created_at": NOW - 9000,
            },
        ],
        links=[("t_running_parent", "t_gate1")],
        events={
            "t_gate1": [
                ("created", NOW - 500, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", NOW - 400, json.dumps({"reason": PICK_GATE_REASON, "kind": "needs_input"})),
            ],
        },
    )
    config = make_config(root)
    state_path = tmp_path / "h3-state.json"
    # Pass 1: a completion arrives while the candidate's parent is still
    # running -> the safety skip defers WITHOUT consuming the pick key.
    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([
            [(1, "completed", "t_other1", {"summary": "done"})]
        ], "hkrc")},
        credentials=StreamCredentials(token="secret"),
    )
    assert any("skipped:" in action.detail for action in first)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert not any(key.startswith("pick:hkrc:t_gate1") for key in state["actions"])

    # The parent is now done; the next completion re-evaluates and unblocks.
    connection = open_board(root / "hkrc")
    connection.execute("UPDATE tasks SET status = 'done' WHERE id = 't_running_parent'")
    connection.commit()
    connection.close()
    runner = RecordingRunner()
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([
            [(2, "completed", "t_other2", {"summary": "done again"})]
        ], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    assert any("unblock" in call for call in runner.calls)
    assert any(action.kind == "advance_pick_gate" and not action.would for action in second)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("pick:hkrc:t_gate1") for key in state["actions"])


def test_promote_failure_reported_and_retried(tmp_path: Path) -> None:
    """DEF-8: a failed gated-child promotion is reported and stays eligible;
    the pending-promotion scan retries it on the next pass."""
    repo, _, fix_sha = make_git_repo(tmp_path)
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [
            review_task("t_a1111111", status="blocked", workspace=str(repo)),
            fix_task("t_b2222222", workspace=str(repo), status="done"),
            {"id": "t_deploy1", "title": "deploy: roll out", "assignee": "developer",
             "status": "todo", "created_at": NOW - 200},
        ],
        links=[("t_a1111111", "t_b2222222"), ("t_a1111111", "t_deploy1")],
        events={
            "t_b2222222": [("completed", NOW - 100, json.dumps({"summary": f"Fixed; merged {fix_sha} on master."}))],
        },
    )
    config = make_config(root)
    state_path = tmp_path / "promote-state.json"
    flaky = FlakyRunner("promote", failures=1)

    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(0, fix_sha, ""),
    )
    # The H2 complete succeeds; the failed promotion is reported in the detail
    # and no promote key is recorded for the child.
    h2 = [action for action in first if action.kind == "supersede_review"]
    assert len(h2) == 1
    assert "FAILED to promote gated children: t_deploy1" in h2[0].detail
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert not any(key.startswith("promote:hkrc:t_deploy1") for key in state["actions"])

    # Simulate the native complete's effect on the fixture DB (the fake
    # runner does not mutate it): the review is now done with a completed
    # event whose payload carries the superseded marker — the pending-
    # promotion scan depends on that state.
    connection = open_board(root / "hkrc")
    connection.execute("UPDATE tasks SET status = 'done' WHERE id = 't_a1111111'")
    connection.execute(
        "INSERT INTO task_events(id, task_id, kind, payload, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (99, "t_a1111111", "completed",
         json.dumps({"summary": f"superseded: fixed by t_b2222222, merged {fix_sha} (verified)"}),
         NOW - 50),
    )
    connection.commit()
    connection.close()

    # Second pass: pending-promotion scan retries the child promotion.
    flaky.failures = 0
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": _scenario_adapter([], "hkrc")},
        credentials=StreamCredentials(token="secret"),
        runner=flaky,  # type: ignore[arg-type]
        git_runner=lambda _command: NativeResult(0, fix_sha, ""),
    )
    promote_actions = [action for action in second if action.kind == "promote_gated_child"]
    assert any(not action.would and "FAILED" not in action.detail for action in promote_actions)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("promote:hkrc:t_deploy1") for key in state["actions"])


# ---------------------------------------------------------------------------
# Consume / git boundaries
# ---------------------------------------------------------------------------


def test_consume_board_events_drains_frames(tmp_path: Path) -> None:
    adapter = _scenario_adapter([
        [(1, "created", "t_a", {"status": "ready"}), (2, "blocked", "t_a", {"kind": "needs_input", "reason": "x"})],
    ], "hkrc")
    events, error = consume_board_events(adapter, "hkrc", 0, StreamCredentials(token="s"))
    # A partial tail frame (< 200 events) ends the drain cleanly.
    assert error is None
    assert [event.id for event in events] == [1, 2]


def test_consume_board_events_keeps_persistent_socket_across_passes(tmp_path: Path) -> None:
    """DEF-1: the watcher keeps one board socket across passes and reconnects
    only on transport failure — a second pass on the same adapter must not
    reconnect or close the socket."""
    import json as _json
    from fixtures.event_stream import StreamEvent, WebSocketScenario

    scenario = WebSocketScenario(idle=True)
    frame = _json.dumps({
        "events": [
            {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
             "payload": e.payload, "created_at": e.created_at}
            for e in [
                StreamEvent(event_id=1, kind="created", task_id="t_a",
                            payload={"status": "ready"}, created_at=NOW - 1000),
                StreamEvent(event_id=2, kind="blocked", task_id="t_a",
                            payload={"kind": "needs_input", "reason": "x"}, created_at=NOW - 900),
            ]
        ],
        "cursor": 2,
    })
    # One connection script only: a persistent socket is reused, never
    # reconnected on a second pass.
    scenario.connections.append([frame])
    adapter = StreamAdapter(
        "ws://127.0.0.1:1/events",
        allowed_boards={"hkrc"},
        connector=scenario.connector,
    )
    first, error = consume_board_events(adapter, "hkrc", 0, StreamCredentials(token="s"))
    assert error is None
    assert [event.id for event in first] == [1, 2]
    assert adapter.connected  # socket stays open after a normal drain
    assert len(scenario.calls) == 1

    # Second pass on the same adapter: idle frame -> no events, no reconnect,
    # socket still open.
    second, error2 = consume_board_events(adapter, "hkrc", 2, StreamCredentials(token="s"))
    assert error2 is None
    assert second == []
    assert len(scenario.calls) == 1  # no reconnect
    assert adapter.connected  # still the same open socket


def test_consume_board_events_reconnects_after_transport_failure(tmp_path: Path) -> None:
    """DEF-1: a transport failure drops the socket; the next pass reconnects."""
    import json as _json
    from fixtures.event_stream import StreamEvent, WebSocketScenario

    scenario = WebSocketScenario(idle=True)
    # A FULL 200-event frame keeps the drain going; the following empty bytes
    # are a transport disconnect mid-drain.  The second connection script is
    # the reconnect.
    full = [
        StreamEvent(event_id=i, kind="created", task_id="t_a",
                    payload={"status": "ready"}, created_at=NOW - 1000)
        for i in range(1, 201)
    ]
    frame_full = _json.dumps({
        "events": [
            {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
             "payload": e.payload, "created_at": e.created_at}
            for e in full
        ],
        "cursor": 200,
    })
    resume = _json.dumps({
        "events": [
            {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
             "payload": e.payload, "created_at": e.created_at}
            for e in [StreamEvent(event_id=201, kind="created", task_id="t_a",
                                  payload={"status": "ready"}, created_at=NOW - 900)]
        ],
        "cursor": 201,
    })
    scenario.connections.append([frame_full, b""])
    scenario.connections.append([resume])
    adapter = StreamAdapter(
        "ws://127.0.0.1:1/events",
        allowed_boards={"hkrc"},
        connector=scenario.connector,
    )
    first, error = consume_board_events(adapter, "hkrc", 0, StreamCredentials(token="s"))
    # The accepted events survive the tail disconnect; the socket is dropped.
    assert [event.id for event in first] == list(range(1, 201))
    assert error is not None
    assert not adapter.connected
    assert len(scenario.calls) == 1

    # Next pass reconnects and resumes from the durable cursor.
    second, error2 = consume_board_events(adapter, "hkrc", 200, StreamCredentials(token="s"))
    assert error2 is None
    assert [event.id for event in second] == [201]
    assert len(scenario.calls) == 2


def test_consume_board_events_partial_tail_ends_drain(tmp_path: Path) -> None:
    """A full 200-event frame followed by a partial tail stops without the
    idle wait (the backlog is exhausted when the tail is shorter)."""
    from fixtures.event_stream import StreamEvent, WebSocketScenario

    scenario = WebSocketScenario()
    full = [
        StreamEvent(event_id=i, kind="created", task_id="t_a", payload={"status": "ready"}, created_at=100)
        for i in range(1, 201)
    ]
    tail = [StreamEvent(event_id=201, kind="created", task_id="t_b", payload={"status": "ready"}, created_at=100)]
    frame_full = json.dumps({
        "events": [
            {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
             "payload": e.payload, "created_at": e.created_at}
            for e in full
        ],
        "cursor": 200,
    })
    frame_tail = json.dumps({
        "events": [
            {"id": e.event_id, "task_id": e.task_id, "run_id": None, "kind": e.kind,
             "payload": e.payload, "created_at": e.created_at}
            for e in tail
        ],
        "cursor": 201,
    })
    scenario.connections.append([frame_full, frame_tail])
    adapter = StreamAdapter("ws://127.0.0.1:1/events", allowed_boards={"hkrc"}, connector=scenario.connector)
    events, error = consume_board_events(adapter, "hkrc", 0, StreamCredentials(token="s"))
    assert error is None
    assert len(events) == 201
    assert events[-1].id == 201


def test_git_repo_root_resolves_main_repo(tmp_path: Path) -> None:
    repo, _, _ = make_git_repo(tmp_path)
    worktree = tmp_path / "wt"
    import subprocess

    subprocess.run(
        ["git", "-C", str(repo), "worktree", "add", "-q", str(worktree), "-b", "wt/1"],
        check=True,
    )
    assert git_repo_root(repo, None) == repo  # type: ignore[arg-type]
    assert git_repo_root(worktree, None) == repo  # type: ignore[arg-type]
    assert git_repo_root(tmp_path / "missing", None) is None  # type: ignore[arg-type]


def test_format_message_silent_when_no_actions() -> None:
    assert format_message([]) == ""
    action = Action("hkrc", "3", "advance_pick_gate", "t_1", "would unblock t_1", True)
    message = format_message([action])
    assert message.startswith("watcher dry-run: H3 advance_pick_gate hkrc t_1:")


# ---------------------------------------------------------------------------
# H5 - review-required deadlock discovery + archive
# ---------------------------------------------------------------------------

REVIEW_REQUIRED_WITH_EVIDENCE = {
    "reason": (
        "review-required: FIX-READY — implementation complete, gate green, "
        "merge-base == main HEAD; reviewer gates the merge"
    ),
    "kind": "needs_input",
    "recurrences": 1,
}


def deadlock_board(
    root: Path,
    *,
    parent_id: str = "t_parent01",
    child_id: str = "t_review01",
    child_status: str = "todo",
    reason: str = REVIEW_REQUIRED_WITH_EVIDENCE["reason"],
    blocked_at: int = NOW - 4000,
    parent_status: str = "blocked",
) -> Path:
    """Build a blocked review-required parent + todo review child board."""
    return make_board(
        root,
        "hkrc",
        [
            review_task(parent_id, title=f"fix: work for {parent_id}", assignee="developer", status=parent_status),
            review_task(child_id, title=f"review: validate work ({parent_id})", assignee="reviewer", status=child_status),
        ],
        events={
            parent_id: [
                ("created", NOW - 5000, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", blocked_at, json.dumps({"reason": reason, "kind": "needs_input"})),
            ],
            child_id: [
                ("created", NOW - 3600, json.dumps({"assignee": "reviewer", "status": "todo"})),
            ],
        },
        links=[(parent_id, child_id)],
    )


def test_discovers_review_required_deadlock(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root)
    deadlocks = discover_review_required_deadlocks(root, make_config(), now=NOW)
    assert len(deadlocks) == 1
    deadlock = deadlocks[0]
    assert deadlock.board_slug == "hkrc"
    assert deadlock.task_id == "t_parent01"
    assert deadlock.review_child_ids == ("t_review01",)
    assert deadlock.blocked_event_id == 2
    assert "FIX-READY" in deadlock.reason


def test_no_review_child_is_not_a_deadlock(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "hkrc",
        [review_task("t_parent01", title="fix: work for t_parent01", assignee="developer", status="blocked")],
        events={
            "t_parent01": [
                ("created", NOW - 5000, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", NOW - 4000, json.dumps(REVIEW_REQUIRED_WITH_EVIDENCE)),
            ],
        },
        links=[],
    )
    assert discover_review_required_deadlocks(root, make_config(), now=NOW) == []


def test_no_completion_evidence_is_not_a_deadlock(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(
        root,
        reason="review-required: question about the scope of the fix",
    )
    assert discover_review_required_deadlocks(root, make_config(), now=NOW) == []


def test_deadlock_requires_blocked_parent(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root, parent_status="done")
    assert discover_review_required_deadlocks(root, make_config(), now=NOW) == []


def test_deadlock_child_must_be_todo(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root, child_status="running")
    assert discover_review_required_deadlocks(root, make_config(), now=NOW) == []


def test_deadlock_min_age_debounce(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    # Blocked 100s ago — below the 900s default debounce: never fired live.
    deadlock_board(root, blocked_at=NOW - 100)
    assert discover_review_required_deadlocks(root, make_config(), now=NOW) == []
    # Replay-style scan ignores the debounce so historical stalls show.
    assert len(discover_review_required_deadlocks(root, make_config(), now=NOW, enforce_min_age=False)) == 1


def test_review_child_by_reviewer_run_counts(tmp_path: Path) -> None:
    """A child whose created-event assignee is not the reviewer still counts
    when a reviewer task_run exists (delegation/reassignment)."""
    root = tmp_path / "boards"
    board = make_board(
        root,
        "hkrc",
        [
            review_task("t_parent01", title="fix: work for t_parent01", assignee="developer", status="blocked"),
            review_task("t_review01", title="review: validate work (t_parent01)", assignee="developer", status="todo"),
        ],
        events={
            "t_parent01": [
                ("created", NOW - 5000, json.dumps({"assignee": "developer", "status": "ready"})),
                ("blocked", NOW - 4000, json.dumps(REVIEW_REQUIRED_WITH_EVIDENCE)),
            ],
            "t_review01": [
                ("created", NOW - 3600, json.dumps({"assignee": "developer", "status": "todo"})),
            ],
        },
        links=[("t_parent01", "t_review01")],
    )
    connection = open_board(board)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS task_runs (id INTEGER PRIMARY KEY, task_id TEXT, profile TEXT)"
        )
        connection.execute(
            "INSERT INTO task_runs(id, task_id, profile) VALUES (1, 't_review01', 'reviewer')"
        )
        connection.commit()
    finally:
        connection.close()
    deadlocks = discover_review_required_deadlocks(root, make_config(), now=NOW)
    assert len(deadlocks) == 1
    assert deadlocks[0].review_child_ids == ("t_review01",)


def test_run_live_archives_deadlock_parent(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root)
    adapter = _scenario_adapter([[]], "hkrc")
    runner = RecordingRunner()
    state_path = tmp_path / "h5-state.json"
    actions, message = run(
        make_config(root),
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    h5 = [action for action in actions if action.handler == "5"]
    assert len(h5) == 1
    assert not h5[0].would
    assert h5[0].kind == "archive_review_required_parent"
    assert h5[0].target_id == "t_parent01"
    archive_calls = [call for call in runner.calls if "archive" in call]
    assert len(archive_calls) == 1
    assert archive_calls[0][-1] == "t_parent01"
    assert "review child t_review01" in h5[0].detail
    # The episode-scoped dedupe key is recorded.
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert any(key.startswith("deadlock:hkrc:t_parent01:2") for key in state["actions"])


def test_run_live_deadlock_archive_is_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root)
    adapter = _scenario_adapter([[], []], "hkrc")
    runner = RecordingRunner()
    state_path = tmp_path / "h5-state.json"
    config = make_config(root)
    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    assert any(action.kind == "archive_review_required_parent" for action in first)
    runner.calls.clear()
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    assert not any(action.kind == "archive_review_required_parent" for action in second)
    assert not any("archive" in call for call in runner.calls)


def test_run_dry_run_reports_would_archive(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root)
    adapter = _scenario_adapter([[]], "hkrc")
    runner = RecordingRunner()
    actions, message = run(
        make_config(root),
        state_path=tmp_path / "h5-state.json",
        dry_run=True,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    h5 = [action for action in actions if action.handler == "5"]
    assert len(h5) == 1
    assert h5[0].would
    assert "would archive review-required deadlock parent t_parent01" in h5[0].detail
    assert "t_review01" in h5[0].detail
    assert "hermes kanban --board hkrc archive t_parent01" in h5[0].detail
    assert not any("archive" in call for call in runner.calls)
    assert "watcher dry-run: H5 archive_review_required_parent hkrc t_parent01" in message


def test_run_live_failed_archive_stays_eligible(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    deadlock_board(root)
    adapter = _scenario_adapter([[], []], "hkrc")
    runner = RecordingRunner()
    runner.results["hermes kanban --board hkrc archive t_parent01"] = NativeResult(1, "", "cannot archive")
    state_path = tmp_path / "h5-state.json"
    config = make_config(root)
    first, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    failed = [action for action in first if action.kind == "archive_review_required_parent"]
    assert len(failed) == 1
    assert "FAILED archive" in failed[0].detail
    # Failed mutations do not consume eligibility: the next pass retries.
    second, _ = run(
        config,
        state_path=state_path,
        dry_run=False,
        now=NOW,
        adapters={"hkrc": adapter},
        credentials=StreamCredentials(token="secret"),
        runner=runner,  # type: ignore[arg-type]
    )
    assert any(action.kind == "archive_review_required_parent" for action in second)


def test_completion_evidence_pattern_markers() -> None:
    assert COMPLETION_EVIDENCE_PATTERN.search("FIX-READY")
    assert COMPLETION_EVIDENCE_PATTERN.search("fix ready")
    assert COMPLETION_EVIDENCE_PATTERN.search("gate green")
    assert COMPLETION_EVIDENCE_PATTERN.search("merge-base == main HEAD")
    assert COMPLETION_EVIDENCE_PATTERN.search("merge-base == main")
    assert COMPLETION_EVIDENCE_PATTERN.search("review-required: FIX-READY — gate green")
    assert not COMPLETION_EVIDENCE_PATTERN.search("review-required: question about scope")
    assert not COMPLETION_EVIDENCE_PATTERN.search("review-required: shipped on wt/x (commit abc)")
    assert REVIEW_REQUIRED_PREFIX == "review-required"
    assert ReviewRequiredDeadlock.__name__ == "ReviewRequiredDeadlock"
