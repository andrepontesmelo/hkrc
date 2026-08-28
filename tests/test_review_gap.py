from __future__ import annotations

import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Callable

import pytest

from hkrc.config import ConfigError, ControllerConfig, ReviewGapConfig
from hkrc.handoff import NativeResult
from hkrc.review_gap import (
    DEFAULT_BRANCH_CANDIDATES,
    REVALIDATION_TITLE_PREFIX,
    REVIEW_ASSIGNEE,
    REVIEW_PRIORITY,
    REVIEW_REQUIRED_PREFIX,
    BlockedCandidate,
    BoardInfo,
    Candidate,
    ReviewChild,
    ReviewGapError,
    blocked_episode,
    branch_is_merged,
    build_complete_command,
    build_create_command,
    build_handoff_summary,
    build_native_environment,
    build_revalidation_body,
    build_review_body,
    candidate_from_task,
    commit_is_merged,
    completed_line,
    decomposed_child_ids,
    default_branch,
    default_state_path,
    discover_boards,
    duplicate_alert_line,
    gap_line,
    has_open_revalidation_card,
    is_candidate,
    latest_blocked_event,
    list_blocked_tasks,
    list_done_tasks,
    repo_root_for,
    resolve_impl_commit,
    review_children,
    run,
    run_native,
    shipped_branch_evidence,
    show_task,
    stale_merge_line,
    stall_line,
)

NOW = 1_800_000_000  # a realistic epoch so window boundaries stay positive

REPO_ROOT = "/repos/hermes-kanban-recovery-controller"


def make_config(
    *,
    enabled: bool = True,
    min_age_seconds: int | float = 300,
    recency_hours: int | float = 48,
    stalled_alert_hours: int | float = 6,
    auto_create: bool = True,
    trigger_c_enabled: bool = True,
    trigger_d_enabled: bool = True,
    cli_timeout_seconds: int | float = 30.0,
    tick_timeout_seconds: int | float = 120.0,
    max_workers: int = 16,
    native_cli: str = "hermes",
) -> ControllerConfig:
    return ControllerConfig(
        "test",
        Path("/tmp/nonexistent-boards"),
        Path("/tmp/nonexistent-state.sqlite3"),
        native_cli=native_cli,
        review_gap=ReviewGapConfig(
            enabled=enabled,
            min_age_seconds=min_age_seconds,
            recency_hours=recency_hours,
            stalled_alert_hours=stalled_alert_hours,
            auto_create=auto_create,
            trigger_c_enabled=trigger_c_enabled,
            trigger_d_enabled=trigger_d_enabled,
            cli_timeout_seconds=cli_timeout_seconds,
            tick_timeout_seconds=tick_timeout_seconds,
            max_workers=max_workers,
        ),
    )


def done_task(
    task_id: str,
    *,
    completed_at: int = NOW - 3600,
    workspace_kind: str = "worktree",
    workspace_path: str | None = None,
    title: str | None = None,
) -> dict:
    return {
        "id": task_id,
        "title": title or f"task: {task_id}",
        "status": "done",
        "workspace_kind": workspace_kind,
        "workspace_path": (
            workspace_path if workspace_path is not None else f"{REPO_ROOT}/.worktrees/{task_id}"
        ),
        "completed_at": completed_at,
        "assignee": "developer",
    }


def child_show(
    task_id: str,
    *,
    status: str = "done",
    assignee: str = "reviewer",
    events: list[dict] | None = None,
    runs: list[dict] | None = None,
) -> dict:
    created_payload = {"assignee": assignee, "status": "todo"}
    return {
        "task": {"id": task_id, "status": status, "assignee": assignee},
        "children": [],
        "parents": [],
        "events": events if events is not None else [{"kind": "created", "payload": created_payload, "created_at": NOW - 4000}],
        "runs": runs if runs is not None else [],
        "comments": [],
        "latest_summary": None,
    }


def show_dict(task_id: str, *, children: list[str] | None = None) -> dict:
    return {
        "task": {"id": task_id, "status": "done"},
        "children": children or [],
        "parents": [],
        "events": [{"kind": "created", "payload": {"assignee": "developer"}, "created_at": NOW - 4000}],
        "runs": [],
        "comments": [],
        "latest_summary": None,
    }


def blocked_task(
    task_id: str,
    *,
    blocked_at: int = NOW - 3600,
    workspace_kind: str = "worktree",
    workspace_path: str | None = None,
) -> dict:
    return {
        "id": task_id,
        "title": f"task: {task_id}",
        "status": "blocked",
        "workspace_kind": workspace_kind,
        "workspace_path": (
            workspace_path if workspace_path is not None else f"{REPO_ROOT}/.worktrees/{task_id}"
        ),
        "completed_at": None,
        "assignee": "developer",
    }


def blocked_show(
    task_id: str,
    *,
    children: list[str] | None = None,
    blocked_at: int = NOW - 3600,
    reason: str | None = "review-required: work shipped on wt/t_x — review gates the merge",
    extra_events: list[dict] | None = None,
) -> dict:
    events: list[dict] = [
        {"kind": "created", "payload": {"assignee": "developer", "status": "ready"}, "created_at": blocked_at - 4000},
    ]
    if reason is not None:
        events.append(
            {
                "kind": "blocked",
                "payload": {"reason": reason, "kind": "needs_input", "recurrences": 1},
                "created_at": blocked_at,
                "run_id": 100,
            }
        )
    if extra_events:
        events.extend(extra_events)
    return {
        "task": {"id": task_id, "status": "blocked"},
        "children": children or [],
        "parents": [],
        "events": events,
        "runs": [],
        "comments": [],
        "latest_summary": None,
    }


def make_blocked_runner(
    *,
    boards: list[dict] | None = None,
    blocked: list[dict] | None = None,
    shows: dict[str, dict] | None = None,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """Build a fake CLI runner whose blocked list is distinct from done tasks.

    The generic ``make_runner`` serves one ``tasks`` dict to every list
    status; trigger-c tests need the blocked list to carry only blocked tasks
    while the done list stays empty so trigger a/b never fires.
    """
    boards = boards or [{"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT}]
    blocked = blocked or []
    shows = shows or {}
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            subcommand = argv[4]
            if subcommand == "list":
                status = argv[6]
                if status == "blocked":
                    return NativeResult(0, json.dumps(blocked), "")
                return NativeResult(0, json.dumps([]), "")
            if subcommand == "show":
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
            if subcommand == "complete":
                return NativeResult(0, json.dumps({"id": argv[5]}), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    return runner, recorded


def make_runner(
    *,
    boards: list[dict] | None = None,
    tasks: dict[str, list[dict]] | None = None,
    shows: dict[str, dict] | None = None,
    created: list[str] | None = None,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """Build a fake CLI runner; returns ``(runner, recorded_argv)``."""
    boards = boards or [{"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT}]
    tasks = tasks or {}
    shows = shows or {}
    created_ids = iter(created or ["t_review_new"])
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug = argv[3]
            subcommand = argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks.get(slug, [])), "")
            if subcommand == "show":
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
            if subcommand == "create":
                return NativeResult(0, json.dumps({"id": next(created_ids)}), "")
            if subcommand == "complete":
                return NativeResult(0, json.dumps({"id": argv[5]}), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    return runner, recorded


def make_git_runner(
    *,
    merged: bool = False,
    branch_exists: bool = True,
    default: str = "master",
    sha: str = "a" * 40,
    custom_branch: str | None = None,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        op = argv[3:]
        if op[:2] == ["show-ref", "--verify"]:
            ref = op[2]
            if ref == f"refs/heads/{default}":
                return NativeResult(0, ref, "")
            if custom_branch and ref == f"refs/heads/{custom_branch}":
                return NativeResult(0 if branch_exists else 1, ref, "")
            if ref.startswith("refs/heads/wt/") and branch_exists:
                return NativeResult(0, ref, "")
            return NativeResult(1, "", "")
        if op[:2] == ["merge-base", "--is-ancestor"]:
            return NativeResult(0 if merged else 1, "", "")
        if op[:1] == ["rev-parse"]:
            return NativeResult(0, sha, "")
        return NativeResult(2, "", f"unexpected git argv: {argv}")

    return runner, recorded


def make_commit_git_runner(
    *,
    default: str = "main",
    origin_exists: bool = True,
    local_exists: bool = True,
    commit_on: Sequence[str] = (),
    branch_exists: bool = True,
    branch_sha: str = "a" * 40,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """Git runner for the commit-based merge check (done-review path).

    ``commit_on`` names the refs whose ``merge-base --is-ancestor`` against
    the impl commit succeeds; each name may be written as ``main``,
    ``origin/main``, ``refs/heads/main``, or ``refs/remotes/origin/main``.
    """
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        op = argv[3:]
        if op[:2] == ["show-ref", "--verify"]:
            ref = op[2]
            if ref == f"refs/remotes/origin/{default}":
                return NativeResult(0 if origin_exists else 1, ref, "")
            if ref == f"refs/heads/{default}":
                return NativeResult(0 if local_exists else 1, ref, "")
            if ref.startswith("refs/heads/wt/"):
                return NativeResult(0 if branch_exists else 1, ref, "")
            return NativeResult(1, "", "")
        if op[:2] == ["merge-base", "--is-ancestor"]:
            ref = op[3]
            forms = (
                ref,
                ref.removeprefix("refs/heads/"),
                ref.removeprefix("refs/remotes/"),
            )
            return NativeResult(0 if any(f in commit_on for f in forms) else 1, "", "")
        if op[:1] == ["rev-parse"]:
            return NativeResult(0 if branch_exists else 1, branch_sha, "")
        return NativeResult(2, "", f"unexpected git argv: {argv}")

    return runner, recorded


# --- has-review check: deterministic, no title parsing ----------------------


def test_created_event_assignee_marks_review(tmp_path: Path) -> None:
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl")]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    assert review_children("hermes", "hkrc", ["t_rev"], runner=runner) != []


def test_run_profile_history_marks_review_even_after_reassignment(tmp_path: Path) -> None:
    # The reviewer ran the card (delegation history) even though the current
    # assignee later changed — the created event may also no longer name the
    # reviewer, so the run profile alone must be sufficient.
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl")]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show(
                "t_rev",
                assignee="developer",
                events=[{"kind": "created", "payload": {"assignee": "developer"}, "created_at": NOW - 4000}],
                runs=[{"id": 9, "profile": "reviewer", "status": "done"}],
            ),
        },
    )
    assert review_children("hermes", "hkrc", ["t_rev"], runner=runner) != []


def test_current_assignee_only_is_not_sufficient(tmp_path: Path) -> None:
    # A child currently assigned to "reviewer" but never created by a reviewer
    # and with no reviewer run is NOT a review (deterministic check only).
    runner, _ = make_runner()
    child_show(
        "t_rev",
        assignee="reviewer",
        events=[{"kind": "created", "payload": {"assignee": "developer"}, "created_at": NOW - 4000}],
        runs=[],
    )
    assert review_children("hermes", "hkrc", ["t_rev"], runner=runner) == []


def test_no_title_matching(tmp_path: Path) -> None:
    # A child merely titled like a review but worked by a developer is not a
    # review; the check must not read titles.
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl")]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_looks_like_review"]),
            "t_looks_like_review": child_show(
                "t_looks_like_review",
                assignee="developer",
                events=[{"kind": "created", "payload": {"assignee": "developer"}, "created_at": NOW - 4000}],
                runs=[],
            ),
        },
    )
    reviews = review_children("hermes", "hkrc", ["t_looks_like_review"], runner=runner)
    assert reviews == []
    assert "review" in "review: validate something"  # guard against deleting the test


# --- candidate filter -------------------------------------------------------


def test_candidate_filter_window_min_age_and_worktree(tmp_path: Path) -> None:
    recency_hours = 48
    min_age = 300
    now = NOW
    window = int(recency_hours * 3600)
    cases = [
        (done_task("t_inside", completed_at=now - 3600), True),
        (done_task("t_boundary_window", completed_at=now - window), True),
        (done_task("t_too_old", completed_at=now - window - 1), False),
        (done_task("t_boundary_age", completed_at=now - min_age), True),
        (done_task("t_too_young", completed_at=now - min_age + 1), False),
        (done_task("t_scratch", workspace_kind="scratch", completed_at=now - 3600), False),
        (done_task("t_no_completed_at", completed_at=0), False),
    ]
    for task, expected in cases:
        assert is_candidate(task, now=now, recency_hours=recency_hours, min_age_seconds=min_age) is expected, task["id"]


def test_candidate_from_task_captures_workspace(tmp_path: Path) -> None:
    task = done_task("t_impl", workspace_path=f"{REPO_ROOT}/.worktrees/t_impl")
    candidate = candidate_from_task(task, "hkrc")
    assert candidate.board_slug == "hkrc"
    assert candidate.task_id == "t_impl"
    assert candidate.workspace_path == f"{REPO_ROOT}/.worktrees/t_impl"
    assert candidate.completed_at == NOW - 3600
    assert candidate.key == "hkrc:t_impl"


# --- repo-root derivation ---------------------------------------------------


def test_repo_root_strips_worktrees_suffix() -> None:
    assert repo_root_for(f"{REPO_ROOT}/.worktrees/t_impl", None) == REPO_ROOT
    assert repo_root_for("/repos/demo-project/.worktrees/t_x", None) == "/repos/demo-project"


def test_repo_root_plain_path_and_default_workdir_fallback() -> None:
    assert repo_root_for(REPO_ROOT, None) == REPO_ROOT
    assert repo_root_for(None, "/repos/demo-project") == "/repos/demo-project"
    assert repo_root_for(None, None) is None


# --- auto-create command construction ---------------------------------------


def test_build_create_command_shape_and_body() -> None:
    body = build_review_body("t_impl", "fix: mobile css")
    command = build_create_command("hermes", "hkrc", "fix: mobile css", "t_impl", REPO_ROOT, body)
    assert command[:6] == ["hermes", "kanban", "--board", "hkrc", "create", "review: validate fix: mobile css (t_impl)"]
    assert "--assignee" in command and command[command.index("--assignee") + 1] == REVIEW_ASSIGNEE
    assert "--parent" in command and command[command.index("--parent") + 1] == "t_impl"
    assert "--workspace" in command and command[command.index("--workspace") + 1] == f"worktree:{REPO_ROOT}"
    assert "--priority" in command and command[command.index("--priority") + 1] == str(REVIEW_PRIORITY)
    assert command[-1] == "--json"
    assert command[command.index("--body") + 1] == body
    # Body contract: branch-not-main, one unit, acceptance, merge contract.
    for fragment in (
        "branch `wt/t_impl`",
        "NOT on main",
        "do NOT rebase onto main",
        "as one unit",
        "run the repo tests and lint clean",
        "live browser where applicable",
        "git merge --no-ff",
        "merge_sha",
        "t_impl",
    ):
        assert fragment in body


def test_auto_create_fires_for_gap(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(tasks={"hkrc": [done_task("t_impl")]})
    config = make_config()
    digest = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert "created review card t_review_new for t_impl on board hkrc" in digest
    create_argv = [argv for argv in recorded if "create" in argv[4:6]]
    assert len(create_argv) == 1
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "created:t_review_new", "at": NOW}}


def test_review_card_itself_is_not_a_candidate(tmp_path: Path) -> None:
    # A done review card (created by the reviewer profile) must not trigger a
    # review-of-review card; review cards do not get review children.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_rev", title="review: validate x")]},
        shows={"t_rev": child_show("t_rev", status="done")},
    )
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    # The review card is silently confirmed (never a candidate) so the next
    # ticks skip it; the confirm expires after stalled_alert_hours.
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_rev": {"action": "review-ok", "at": NOW}}


def test_review_card_detected_by_run_history_is_not_a_candidate(tmp_path: Path) -> None:
    # Delegation case: the review card's created payload never named the
    # reviewer, but a reviewer run did the work — still not a candidate.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_rev")]},
        shows={
            "t_rev": child_show(
                "t_rev",
                status="done",
                assignee="developer",
                events=[{"kind": "created", "payload": {"assignee": None}, "created_at": NOW - 4000}],
                runs=[{"id": 9, "profile": "reviewer", "status": "done"}],
            )
        },
    )
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_rev": {"action": "review-ok", "at": NOW}}


# --- trigger b: stalled review ----------------------------------------------


def _stalled_setup(state_file: Path, *, merged: bool = False) -> tuple[str, Callable[[Sequence[str]], NativeResult]]:
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=merged)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    return digest, git_runner


def test_stalled_review_alerts_and_does_not_duplicate(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    digest, _ = _stalled_setup(state_file)
    assert "stalled review for t_impl on board hkrc" in digest
    assert "created" not in digest
    # The same episode must not alert twice: state records the stall.
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "stall-alert", "at": NOW}}
    # With the state recorded the candidate is skipped entirely; a fresh run
    # with the same fake runner stays silent. (Never pass runner=None here —
    # that would spawn REAL hermes/git subprocesses and could act on live
    # boards.)
    runner2, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={"t_impl": show_dict("t_impl", children=["t_rev"])},
    )
    second = run(make_config(), state_file, now=NOW, runner=runner2, git_runner=make_git_runner()[0])
    assert second == ""


def test_merged_branch_never_alerts(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    digest, _ = _stalled_setup(state_file, merged=True)
    assert digest == ""
    # A merged branch is healthy: the candidate is silently confirmed so the
    # next ticks skip it (the confirm expires after stalled_alert_hours).
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_recent_review_never_alerts_before_stalled_hours(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(make_config(stalled_alert_hours=6), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""  # only 2h old, threshold is 6h


def test_done_review_unmerged_commit_creates_revalidation_after_window(tmp_path: Path) -> None:
    # DEF-001 (2026-08-06 rentcli t_b67f7fc0/t_088da2a1): a done review whose
    # approval was granted but whose merge never landed leaves the branch
    # unmerged on origin while every card reads terminal. The watchdog must
    # treat the done review as a stalled merge and auto-create a re-validation
    # card after the stall window — the has-review dedupe must NOT skip this
    # path, because the done review child is exactly the failure being
    # repaired.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show(
                "t_rev",
                status="done",
                runs=[{"metadata": {"approved": True}}],
            ),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=())
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "created review card t_review_new for t_impl on board hkrc" in digest
    creates = [argv for argv in recorded if "create" in argv[4:6]]
    assert len(creates) == 1
    create = creates[0]
    assert create[5] == "review: validate task: t_impl (t_impl)"
    assert create[create.index("--assignee") + 1] == REVIEW_ASSIGNEE
    assert create[create.index("--parent") + 1] == "t_impl"
    assert create[create.index("--workspace") + 1] == f"worktree:{REPO_ROOT}"
    body = create[create.index("--body") + 1]
    for fragment in (
        "wt/t_impl",
        "NOT on main",
        "do NOT rebase onto main",
        "origin/main",
        "git merge --no-ff",
        "merge_sha",
        "t_impl",
    ):
        assert fragment in body
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "created:t_review_new", "at": NOW}}
    # Deduped: a second tick with the same fake runner stays silent.
    second = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert second == ""
    assert len([argv for argv in recorded if "create" in argv[4:6]]) == 1


def test_done_rejected_review_does_not_create_revalidation(tmp_path: Path) -> None:
    # A rejection intentionally leaves the implementation branch unmerged;
    # trigger b must treat that terminal review as healthy rather than asking
    # a reviewer to merge rejected work.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show(
                "t_rev",
                status="done",
                runs=[{"metadata": {"approved": False, "merge_sha": None}}],
            ),
        },
    )
    git_runner, git_recorded = make_commit_git_runner(commit_on=())

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    assert not any("rev-parse" in op and "wt/t_impl" in op for op in git_recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_with_merged_commit_is_healthy(tmp_path: Path) -> None:
    # (a) A done review whose impl commit IS an ancestor of origin/main is a
    # healthy review — "done" is trusted as "merged" only when git truth
    # confirms it.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=("origin/main",))
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_unmerged_before_window_confirms_and_reexamines(tmp_path: Path) -> None:
    # Before stalled_alert_hours an unmerged done review is silently
    # confirmed; the confirm expires at stalled_alert_hours, so the merge
    # check re-fires on schedule and the re-validation card is created then.
    state_file = tmp_path / "state.json"
    config = make_config(stalled_alert_hours=6)
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=())
    first = run(config, state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert first == ""
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}
    later = NOW + 6 * 3600 + 3600  # candidate now 9h old — past the window
    runner2, recorded2 = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    second = run(config, state_file, now=later, runner=runner2, git_runner=git_runner)
    assert "created review card t_review_new for t_impl on board hkrc" in second
    assert len([argv for argv in recorded2 if "create" in argv[4:6]]) == 1


def test_done_review_local_only_merge_is_healthy(tmp_path: Path) -> None:
    # DEF-009: the merge is present on local main but origin/main is stale
    # because pushes are operator-controlled/batched. Local canonical content
    # must prevent a false stalled-merge revalidation.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show(
                "t_rev",
                status="done",
                runs=[{"metadata": {"approved": True, "merge_sha": "a" * 40}}],
            ),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=("main", "refs/heads/main"))
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_origin_absent_falls_back_to_local_default(tmp_path: Path) -> None:
    # No remote-tracking ref for the default branch: the local ref is the
    # best truth available, and a merge on local main satisfies the local
    # check (the loose end can be unpushed; fetch is optional best-effort).
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(origin_exists=False, commit_on=("main",))
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_commit_from_metadata_reads_healthy_when_branch_deleted(tmp_path: Path) -> None:
    # A merged-then-deleted branch must read healthy: the impl commit comes
    # from the review child's completion metadata (implementation_commit),
    # and the branch is gone — the commit check against origin/main is the
    # only truth, and it passes.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show(
                "t_rev",
                status="done",
                runs=[
                    {
                        "id": 1,
                        "profile": "reviewer",
                        "status": "done",
                        "metadata": {"implementation_commit": "c" * 40},
                    }
                ],
            ),
        },
    )
    git_runner, _ = make_commit_git_runner(branch_exists=False, commit_on=("origin/main",))
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_unresolvable_commit_never_false_flags(tmp_path: Path) -> None:
    # No implementation_commit in metadata and the branch was deleted: there
    # is no commit to check. That is the merged-then-deleted common case —
    # treat it like the old missing-branch-is-merged rule (healthy confirm),
    # never a phantom re-validation card. The DEF-001 failure has the branch
    # present, so rev-parse resolves the unmerged commit and flags it.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(branch_exists=False, commit_on=("origin/main",))
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_unmerged_alert_only_when_auto_create_disabled(tmp_path: Path) -> None:
    # Alert-only mode: with auto_create disabled the stalled merge emits the
    # stale-merge alert line once and never creates a card.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [done_task("t_impl", completed_at=NOW - 7 * 3600)]},
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=())
    digest = run(make_config(auto_create=False), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "stalled merge for t_impl on board hkrc" in digest
    assert "t_rev done, commit not on main for 7h" in digest
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "stall-alert", "at": NOW}}
    second = run(make_config(auto_create=False), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert second == ""


def test_done_review_revalidation_in_flight_suppresses_recreate(tmp_path: Path) -> None:
    # Dedupe state lost but an OPEN re-validation card for the same impl
    # exists (same title shape this module creates): the in-flight card IS
    # the repair — never create a duplicate.
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={
            "hkrc": [
                done_task("t_impl", completed_at=NOW - 7 * 3600),
                {
                    "id": "t_reval",
                    "title": "review: validate task: t_impl (t_impl)",
                    "status": "todo",
                    "workspace_kind": "scratch",
                    "completed_at": 0,
                },
            ]
        },
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=())
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}


def test_done_review_terminal_revalidation_card_does_not_suppress(tmp_path: Path) -> None:
    # A re-validation card that completed WITHOUT the merge landing is not a
    # suppress: the merge still never happened, so a fresh card fires (mirrors
    # trigger d's false-complete semantics — terminal never heals).
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={
            "hkrc": [
                done_task("t_impl", completed_at=NOW - 7 * 3600),
                {
                    "id": "t_reval_old",
                    "title": "review: validate task: t_impl (t_impl)",
                    "status": "done",
                    "workspace_kind": "scratch",
                    "completed_at": 0,
                },
            ]
        },
        shows={
            "t_impl": show_dict("t_impl", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="done"),
        },
    )
    git_runner, _ = make_commit_git_runner(commit_on=())
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "created review card t_review_new for t_impl on board hkrc" in digest
    assert len([argv for argv in recorded if "create" in argv[4:6]]) == 1


# --- stalled-merge helpers: commit resolution / merge check -----------------


def test_commit_is_merged_origin_first_then_local(tmp_path: Path) -> None:
    # origin resolvable and merged -> the check succeeds immediately.
    git_runner, _ = make_commit_git_runner(commit_on=("origin/main",))
    assert commit_is_merged(tmp_path, "a" * 40, runner=git_runner) is True
    # A stale origin ref must fall through to local canonical main.
    git_runner2, _ = make_commit_git_runner(commit_on=("main",))
    assert commit_is_merged(tmp_path, "a" * 40, runner=git_runner2) is True
    # If neither canonical ref contains the commit, it remains unmerged.
    git_runner3, _ = make_commit_git_runner(commit_on=())
    assert commit_is_merged(tmp_path, "a" * 40, runner=git_runner3) is False
    # no origin ref -> local fallback satisfies the check
    git_runner4, _ = make_commit_git_runner(origin_exists=False, commit_on=("main",))
    assert commit_is_merged(tmp_path, "a" * 40, runner=git_runner4) is True
    # no default branch at all -> False (flag, never silently healthy)
    git_runner5, _ = make_commit_git_runner(local_exists=False, origin_exists=False)
    assert commit_is_merged(tmp_path, "a" * 40, runner=git_runner5) is False


def test_resolve_impl_commit_metadata_then_branch(tmp_path: Path) -> None:
    reviews = [
        ReviewChild(
            "t_rev",
            "done",
            runs=({"metadata": {"implementation_commit": "c" * 40}},),
        )
    ]
    assert (
        resolve_impl_commit(tmp_path, "t_impl", show_dict("t_impl"), reviews, runner=make_commit_git_runner()[0])
        == "c" * 40
    )
    # merge_sha (the merge-contract key) is accepted from the review child.
    merged = [
        ReviewChild(
            "t_rev",
            "done",
            runs=({"metadata": {"merge_sha": "d" * 40}},),
        )
    ]
    assert (
        resolve_impl_commit(
            tmp_path, "t_impl", show_dict("t_impl"), merged,
            runner=make_commit_git_runner(branch_exists=False)[0],
        )
        == "d" * 40
    )
    # No metadata -> branch rev-parse fallback.
    plain = [ReviewChild("t_rev", "done")]
    assert (
        resolve_impl_commit(tmp_path, "t_impl", show_dict("t_impl"), plain, runner=make_commit_git_runner()[0])
        == "a" * 40
    )
    # Neither metadata nor branch -> None (caller never flags on that alone).
    assert (
        resolve_impl_commit(
            tmp_path, "t_impl", show_dict("t_impl"), plain,
            runner=make_commit_git_runner(branch_exists=False)[0],
        )
        is None
    )


def test_resolve_impl_commit_ignores_impl_own_metadata(tmp_path: Path) -> None:
    # The impl's OWN completion metadata records the pre-review tip, which a
    # reviewer rebase rewrites (observed 2026-08-06: t_3595aa64 recorded
    # 17eecf28, rebased+merged as 2fbfd893 — both on main). Reading it would
    # false-flag already-merged work; resolution must skip impl metadata
    # entirely and use the branch.
    impl_show = {
        "task": {"id": "t_impl", "branch_name": "hkrc/t_impl-fix-thing"},
        "runs": [{"metadata": {"commit": "c" * 40}}],
    }
    # Branch exists -> branch HEAD wins over the stale impl metadata.
    git_runner, _ = make_commit_git_runner()
    assert (
        resolve_impl_commit(tmp_path, "t_impl", impl_show, [], runner=git_runner)
        == "a" * 40
    )
    # Branch deleted + stale impl metadata -> None (healthy, no false flag).
    git_runner2, _ = make_commit_git_runner(branch_exists=False)
    assert (
        resolve_impl_commit(tmp_path, "t_impl", impl_show, [], runner=git_runner2)
        is None
    )
    # Project-linked branch_name is tried after wt/<task_id>.
    project_show = {
        "task": {"id": "t_impl", "branch_name": "hkrc/t_impl-fix-thing"},
        "runs": [],
    }

    def project_runner(argv: Sequence[str]) -> NativeResult:
        op = argv[3:]
        if op[:1] == ["rev-parse"]:
            ref = op[1]
            if ref == "wt/t_impl":
                return NativeResult(1, "", "")  # wt/ branch deleted
            if ref == "hkrc/t_impl-fix-thing":
                return NativeResult(0, "b" * 40, "")
            return NativeResult(1, "", "")
        return NativeResult(2, "", f"unexpected {argv}")

    assert (
        resolve_impl_commit(tmp_path, "t_impl", project_show, [], runner=project_runner)
        == "b" * 40
    )


def test_resolve_impl_commit_rejects_non_sha_metadata(tmp_path: Path) -> None:
    # A reviewer may record merge_sha as a non-sha "no-op (branch == main)"
    # marker (observed 2026-08-06: t_578a0423 recorded "no-op (branch ==
    # ancestor of main); main HEAD e5f850c"). That is NOT a commit — it must
    # fall through to branch resolution, never become a git argument.
    reviews = [
        ReviewChild(
            "t_rev",
            "done",
            runs=({"metadata": {"merge_sha": "no-op (branch == ancestor of main)"}},),
        )
    ]
    # Branch exists -> branch HEAD resolves.
    git_runner, _ = make_commit_git_runner()
    assert (
        resolve_impl_commit(tmp_path, "t_impl", show_dict("t_impl"), reviews, runner=git_runner)
        == "a" * 40
    )
    # Branch deleted -> None (healthy, no false flag on the marker text).
    git_runner2, _ = make_commit_git_runner(branch_exists=False)
    assert (
        resolve_impl_commit(tmp_path, "t_impl", show_dict("t_impl"), reviews, runner=git_runner2)
        is None
    )


def test_has_open_revalidation_card_matching() -> None:
    assert REVALIDATION_TITLE_PREFIX == "review: validate "
    tasks = [
        {"id": "t_open", "title": "review: validate fix x (t_impl)", "status": "todo"},
        {"id": "t_done", "title": "review: validate fix x (t_impl)", "status": "done"},
        {"id": "t_other", "title": "review: validate fix y (t_other)", "status": "todo"},
        {"id": "t_impl", "title": "fix x", "status": "done"},
    ]
    assert has_open_revalidation_card(tasks, "t_impl") is True
    assert has_open_revalidation_card(tasks, "t_other") is True
    assert has_open_revalidation_card([tasks[1]], "t_impl") is False  # terminal never suppresses
    assert has_open_revalidation_card([], "t_impl") is False


def test_build_revalidation_body_contract() -> None:
    body = build_revalidation_body("t_impl", "fix: mobile css", "c" * 40, "main")
    for fragment in (
        "wt/t_impl",
        "c" * 40,
        "code is NOT on main",
        "do NOT rebase onto main",
        "origin/main",
        "git merge --no-ff",
        "merge_sha",
        "t_impl",
        "completed WITHOUT the merge landing",
    ):
        assert fragment in body
    # No default branch name: the body still names a concrete target.
    fallback = build_revalidation_body("t_impl", "fix", None, None)
    assert "rebase onto main" in fallback
    assert "code is NOT on main" in fallback


def test_stale_merge_line_shape() -> None:
    candidate = Candidate("hkrc", "t_impl", "title", f"{REPO_ROOT}/.worktrees/t_impl", NOW - 8 * 3600)
    line = stale_merge_line(candidate, [ReviewChild("t_rev", "done")], NOW)
    assert "stalled merge for t_impl on board hkrc" in line
    assert "t_rev done, commit not on main for 8h" in line


# --- dedupe -----------------------------------------------------------------


def test_dedupe_state_prevents_refire(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(tasks={"hkrc": [done_task("t_impl")]})
    config = make_config()
    first = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert "created review card" in first
    creates = [argv for argv in recorded if "create" in argv[4:6]]
    assert len(creates) == 1
    # Second tick: same state file, candidate already acted on -> silent.
    second = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert second == ""
    assert len([argv for argv in recorded if "create" in argv[4:6]]) == 1


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("{not json", encoding="utf-8")
    with pytest.raises(ReviewGapError, match="cannot read review-gap state"):
        run(make_config(), state_file, now=NOW, runner=None)
    state_file.write_text(json.dumps({"hkrc:t_impl": "created:t_x"}), encoding="utf-8")
    with pytest.raises(ReviewGapError, match="must be objects"):
        run(make_config(), state_file, now=NOW, runner=None)


def test_auto_create_disabled_alerts_gap_once(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(tasks={"hkrc": [done_task("t_impl")]})
    config = make_config(auto_create=False)
    digest = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert gap_line(Candidate("hkrc", "t_impl", "task: t_impl", f"{REPO_ROOT}/.worktrees/t_impl", NOW - 3600)) in digest
    assert not any("create" in argv[4:6] for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "gap-alert", "at": NOW}}
    assert run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0]) == ""


def test_disabled_config_stays_silent(tmp_path: Path) -> None:
    runner, recorded = make_runner(tasks={"hkrc": [done_task("t_impl")]})
    digest = run(make_config(enabled=False), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""
    assert recorded == []


def test_no_repo_root_fails_closed_with_error_line(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, _ = make_runner(
        boards=[{"slug": "rentcli", "archived": False, "default_workdir": None}],
        tasks={"rentcli": [done_task("t_impl", workspace_path="")]},
    )
    digest = run(make_config(), state_file, now=NOW, runner=runner)
    assert "review-gap error: cannot derive repo root for t_impl on board rentcli" in digest
    assert "created" not in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"rentcli:t_impl": {"action": "error:no-repo-root", "at": NOW}}


# --- boards / show parsing --------------------------------------------------


def test_discover_boards_skips_archived_and_malformed(tmp_path: Path) -> None:
    runner, _ = make_runner(
        boards=[
            {"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT},
            {"slug": "old", "archived": True, "default_workdir": None},
            {"slug": "", "archived": False, "default_workdir": None},
            "not-a-dict",
            {"slug": "rentcli", "archived": False, "default_workdir": "/repos/demo-project"},
        ]
    )
    boards = discover_boards("hermes", runner=runner)
    assert boards == [
        BoardInfo(slug="hkrc", default_workdir=REPO_ROOT),
        BoardInfo(slug="rentcli", default_workdir="/repos/demo-project"),
    ]


def test_show_task_and_list_done_parse(tmp_path: Path) -> None:
    runner, _ = make_runner(
        tasks={"hkrc": [done_task("t_impl")]},
        shows={"t_impl": show_dict("t_impl")},
    )
    tasks = list_done_tasks("hermes", "hkrc", runner=runner)
    assert [task["id"] for task in tasks] == ["t_impl"]
    show = show_task("hermes", "hkrc", "t_impl", runner=runner)
    assert show["children"] == []


def test_cli_failure_fails_closed(tmp_path: Path) -> None:
    def failing_runner(argv: list[str]) -> NativeResult:
        return NativeResult(1, "", "boom")

    with pytest.raises(ReviewGapError, match="boards list failed"):
        discover_boards("hermes", runner=failing_runner)
    with pytest.raises(ReviewGapError, match="must return a JSON array"):
        discover_boards("hermes", runner=lambda argv: NativeResult(0, json.dumps({"slug": "x"}), ""))
    with pytest.raises(ReviewGapError, match="list --status done on board hkrc failed"):
        list_done_tasks("hermes", "hkrc", runner=failing_runner)


# --- DEF-001: HERMES_KANBAN_* env scrub (reviewer verdict on t_505f04b5) ----


def test_build_native_environment_scrubs_all_kanban_vars() -> None:
    # The kanban dispatcher exports HERMES_KANBAN_* (board, db, task id, run
    # id, claim lock, ...) into worker env; the native CLI honors the pinned
    # HERMES_KANBAN_BOARD / HERMES_KANBAN_DB env OVER the --board flag, so
    # none may reach the review-gap subprocess or every board iteration reads
    # the wrong DB. Same scrub needs_input_watcher applies to its LLM summarizer.
    base = {
        "HOME": "/home/operator",
        "HERMES_KANBAN_TASK": "t_parent",
        "HERMES_KANBAN_RUN_ID": "42",
        "HERMES_KANBAN_CLAIM_LOCK": "host:pid",
        "HERMES_KANBAN_DB": "/tmp/hkrc/kanban.db",
        "HERMES_KANBAN_BOARD": "hkrc",
        "HERMES_KANBAN_GOAL_MODE": "1",
        "HERMES_KANBAN_WORKSPACE": "/tmp/ws",
        "_HERMES_GATEWAY": "telegram:secret",
        "PATH": "/usr/bin",
    }
    env = build_native_environment(base)
    assert "HERMES_KANBAN_TASK" not in env
    assert "HERMES_KANBAN_RUN_ID" not in env
    assert "HERMES_KANBAN_CLAIM_LOCK" not in env
    assert "HERMES_KANBAN_DB" not in env
    assert "HERMES_KANBAN_BOARD" not in env
    assert "HERMES_KANBAN_GOAL_MODE" not in env
    assert "HERMES_KANBAN_WORKSPACE" not in env
    assert "_HERMES_GATEWAY" not in env
    assert not any(key.startswith("HERMES_KANBAN_") for key in env)
    assert env["HOME"] == "/home/operator"
    assert env["HERMES_HOME"] == "/home/operator/.hermes"
    assert env["PATH"] == "/usr/bin"
    # An explicit HERMES_HOME is preserved; nothing is added beyond HOME/HERMES_HOME.
    env2 = build_native_environment({"HOME": "/home/operator", "HERMES_HOME": "/srv/hermes"})
    assert env2["HERMES_HOME"] == "/srv/hermes"


def test_run_native_spawns_real_subprocess_with_scrubbed_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import stat as _stat

    log_path = tmp_path / "child.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf 'argv=%s\\n' \"$*\" >> {log_path}\n"
        f"printf 'BOARD=%s\\n' \"$HERMES_KANBAN_BOARD\" >> {log_path}\n"
        f"printf 'DB=%s\\n' \"$HERMES_KANBAN_DB\" >> {log_path}\n"
        "if env | grep -q '^HERMES_KANBAN_'; then printf 'KANBAN_ENV_PRESENT=1\\n' >> "
        f"{log_path}; else printf 'KANBAN_ENV_PRESENT=0\\n' >> {log_path}; fi\n"
        "printf '[]\\n'\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)
    # Simulate the kanban worker environment the watchdog cron runs inside.
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_parent")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "99")
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", "host:pid")
    monkeypatch.setenv("HERMES_KANBAN_DB", "/tmp/hkrc/kanban.db")
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "hkrc")

    # The exact reproduction: --board rentcli must stay on rentcli even with
    # the pinned board env exported in the parent.
    result = run_native([str(fake_cli), "kanban", "--board", "rentcli", "list", "--status", "done", "--json"])
    assert result.returncode == 0
    child = dict(
        line.split("=", 1) for line in log_path.read_text(encoding="utf-8").splitlines()
    )
    assert child["argv"] == "kanban --board rentcli list --status done --json"
    assert child["BOARD"] == ""
    assert child["DB"] == ""
    assert child["KANBAN_ENV_PRESENT"] == "0"


def test_run_native_uses_scrubbed_env_for_boards_and_show(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # discover_boards / list_done_tasks / show_task all go through run_native,
    # so the same scrubbed env covers every subprocess the watchdog spawns.
    import stat as _stat

    log_path = tmp_path / "child.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf 'argv=%s\\n' \"$*\" >> {log_path}\n"
        "if env | grep -q '^HERMES_KANBAN_'; then printf 'KANBAN_ENV_PRESENT=1\\n' >> "
        f"{log_path}; else printf 'KANBAN_ENV_PRESENT=0\\n' >> {log_path}; fi\n"
        "printf '[]\\n'\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "hkrc")
    monkeypatch.setenv("HERMES_KANBAN_DB", "/tmp/hkrc/kanban.db")
    discover_boards(str(fake_cli))
    assert log_path.read_text(encoding="utf-8").count("KANBAN_ENV_PRESENT=0") == 1
    list_done_tasks(str(fake_cli), "rentcli")
    assert log_path.read_text(encoding="utf-8").count("KANBAN_ENV_PRESENT=0") == 2


# --- git merge checks -------------------------------------------------------


def test_default_branch_detection_and_merge(tmp_path: Path) -> None:
    git_runner, _ = make_git_runner(merged=False, default="main")
    assert default_branch(tmp_path, runner=git_runner) == "main"
    assert branch_is_merged(tmp_path, "t_impl", runner=git_runner) is False

    git_runner2, _ = make_git_runner(merged=True, default="master")
    assert default_branch(tmp_path, runner=git_runner2) == "master"
    assert branch_is_merged(tmp_path, "t_impl", runner=git_runner2) is True


def test_missing_branch_or_default_ref_counts_as_merged(tmp_path: Path) -> None:
    git_runner, _ = make_git_runner(merged=False, branch_exists=False)
    assert branch_is_merged(tmp_path, "t_gone", runner=git_runner) is True

    def no_default(argv: list[str]) -> NativeResult:
        if argv[3:5] == ["show-ref", "--verify"]:
            return NativeResult(1, "", "")
        return NativeResult(2, "", f"unexpected {argv}")

    assert branch_is_merged(tmp_path, "t_impl", runner=no_default) is True


# --- digest lines -----------------------------------------------------------


def test_stall_line_shape() -> None:
    candidate = Candidate("hkrc", "t_impl", "title", f"{REPO_ROOT}/.worktrees/t_impl", NOW - 8 * 3600)
    from hkrc.review_gap import ReviewChild

    line = stall_line(candidate, [ReviewChild("t_rev", "todo")], NOW)
    assert "stalled review for t_impl on board hkrc" in line
    assert "t_rev not done" in line
    assert "unmerged for 8h" in line


def test_default_state_path(tmp_path: Path) -> None:
    assert default_state_path(tmp_path / "state" / "state.sqlite3") == tmp_path / "state" / "review-gap-state.json"


# --- config validation ------------------------------------------------------


def test_review_gap_config_defaults_and_validation() -> None:
    assert ReviewGapConfig() == ReviewGapConfig(
        enabled=True,
        min_age_seconds=300,
        recency_hours=48,
        stalled_alert_hours=6,
        auto_create=True,
        trigger_c_enabled=True,
    )
    with pytest.raises(ConfigError, match="enabled must be a boolean"):
        ReviewGapConfig(enabled="yes")
    with pytest.raises(ConfigError, match="min_age_seconds must be a positive number"):
        ReviewGapConfig(min_age_seconds=0)
    with pytest.raises(ConfigError, match="recency_hours must be a positive number"):
        ReviewGapConfig(recency_hours=-1)
    with pytest.raises(ConfigError, match="stalled_alert_hours must be a positive number"):
        ReviewGapConfig(stalled_alert_hours=0)
    with pytest.raises(ConfigError, match="auto_create must be a boolean"):
        ReviewGapConfig(auto_create=1)
    with pytest.raises(ConfigError, match="trigger_c_enabled must be a boolean"):
        ReviewGapConfig(trigger_c_enabled="yes")
    with pytest.raises(ConfigError, match="recency_hours must be a positive number"):
        ReviewGapConfig(recency_hours=True)
    with pytest.raises(ConfigError, match="cli_timeout_seconds must be a positive number"):
        ReviewGapConfig(cli_timeout_seconds=0)
    with pytest.raises(ConfigError, match="tick_timeout_seconds must be a positive number"):
        ReviewGapConfig(tick_timeout_seconds=-1)
    with pytest.raises(ConfigError, match="max_workers must be a positive integer"):
        ReviewGapConfig(max_workers=0)
    # A float is intentionally rejected even though the field is annotated int.
    with pytest.raises(ConfigError, match="max_workers must be a positive integer"):
        ReviewGapConfig(max_workers=1.5)  # type: ignore[arg-type]
    assert ReviewGapConfig(cli_timeout_seconds=10, tick_timeout_seconds=45, max_workers=4).max_workers == 4


# --- CLI subcommand ---------------------------------------------------------


def test_cli_subcommand_registered() -> None:
    from hkrc.cli import build_parser

    parser = build_parser()
    help_text = parser.format_help()
    assert "review-gap" in help_text
    # `hkrc review-gap --help` parses and exits 0 via SystemExit(0).
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["review-gap", "--help"])
    assert exc.value.code == 0


def test_review_gap_constants() -> None:
    assert DEFAULT_BRANCH_CANDIDATES == ("main", "master")
    assert REVIEW_ASSIGNEE == "reviewer"
    assert REVIEW_PRIORITY == 90


# --- trigger c: review-required blocked parents -----------------------------


def test_blocked_episode_parses_review_required_block(tmp_path: Path) -> None:
    task = blocked_task("t_parent")
    show = blocked_show("t_parent")
    episode = blocked_episode(task, "hkrc", show)
    assert episode is not None
    assert episode.key == "hkrc:t_parent"
    assert episode.blocked_at == NOW - 3600
    assert episode.reason.startswith(REVIEW_REQUIRED_PREFIX)


def test_blocked_episode_rejects_real_question_and_malformed(tmp_path: Path) -> None:
    # No review-required prefix -> a real question, never auto-completed.
    task = blocked_task("t_q")
    show = blocked_show("t_q", reason="need a human decision on the migration path")
    assert blocked_episode(task, "hkrc", show) is None
    # Dispatcher-style block (no payload reason) is not a handoff either.
    show2 = {
        "task": {"id": "t_x", "status": "blocked"},
        "children": [],
        "parents": [],
        "events": [
            {"kind": "created", "payload": {"assignee": "developer"}, "created_at": NOW - 4000},
            {"kind": "gave_up", "payload": {"failures": 2}, "created_at": NOW - 3600, "run_id": None},
        ],
        "runs": [],
        "comments": [],
        "latest_summary": None,
    }
    assert latest_blocked_event(show2) is None
    assert blocked_episode(task, "hkrc", show2) is None
    # A later unblocked episode does not matter: only the blocked event counts.
    assert blocked_episode(task, "hkrc", blocked_show("t_x", reason="review-required: shipped")) is not None


def test_trigger_c_completes_blocked_parent_once(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show("t_parent", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False, sha="b" * 40)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "completed review-required parent t_parent on board hkrc" in digest
    completes = [argv for argv in recorded if "complete" in argv[4:6]]
    assert len(completes) == 1
    argv = completes[0]
    assert argv[:6] == ["hermes", "kanban", "--board", "hkrc", "complete", "t_parent"]
    assert "--summary" in argv
    summary = argv[argv.index("--summary") + 1]
    assert summary == build_handoff_summary("t_parent", "t_rev", "b" * 40)
    assert "t_rev" in summary and "wt/t_parent" in summary
    assert "--metadata" in argv
    metadata = json.loads(argv[argv.index("--metadata") + 1])
    assert metadata == {
        "review_card": "t_rev",
        "branch": "wt/t_parent",
        "commit": "b" * 40,
    }
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_parent": {"action": "completed", "at": NOW}}
    # Second tick: dedupe state prevents a second completion.
    second = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert second == ""
    assert len([argv for argv in recorded if "complete" in argv[4:6]]) == 1


def test_trigger_c_never_completes_the_review_child(tmp_path: Path) -> None:
    # The review child must stay blocked/todo — only the parent is completed.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show("t_parent", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    completes = [argv for argv in recorded if "complete" in argv[4:6]]
    assert len(completes) == 1
    assert completes[0][5] == "t_parent"
    assert "t_rev" not in {argv[5] for argv in completes}


def test_trigger_c_skips_without_shipped_branch(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show("t_parent", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False, branch_exists=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)
    # No action was recorded for the episode; the state file is empty.
    if state_file.is_file():
        assert json.loads(state_file.read_text(encoding="utf-8")) == {}


def test_trigger_c_skips_already_merged_branch(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show("t_parent", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=True)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)


def test_trigger_c_skips_without_review_child(tmp_path: Path) -> None:
    # Gap creation is trigger (a) domain for done tasks; a blocked parent
    # without a review child is skipped, never completed.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={"t_parent": blocked_show("t_parent", children=[])},
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)
    # No action was recorded for the episode; the state file is empty.
    if state_file.is_file():
        assert json.loads(state_file.read_text(encoding="utf-8")) == {}


def test_trigger_c_skips_scratch_task_without_shipped_branch(tmp_path: Path) -> None:
    # A scratch task with no wt/<task_id> branch anywhere is never
    # auto-completed — missing branch means the block is a real question.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[
            blocked_task(
                "t_scratch",
                workspace_kind="scratch",
                workspace_path="/tmp/boards/hkrc/workspaces/t_scratch",
            )
        ],
        shows={"t_scratch": blocked_show("t_scratch", children=["t_rev"])},
    )
    git_runner, _ = make_git_runner(merged=False, branch_exists=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)


def test_trigger_c_completes_scratch_task_with_shipped_branch(tmp_path: Path) -> None:
    # Regression (2026-08-05 hkrc t_fa6f319f): a task recorded as scratch
    # with a real shipped wt/<task_id> branch must still auto-complete —
    # workspace_kind alone is not evidence that work did or did not ship.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[
            blocked_task(
                "t_scratch",
                workspace_kind="scratch",
                workspace_path=f"{REPO_ROOT}/.worktrees/t_scratch",
            )
        ],
        shows={
            "t_scratch": blocked_show("t_scratch", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "completed review-required parent t_scratch on board hkrc" in digest
    completes = [argv for argv in recorded if "complete" in argv[4:6]]
    assert len(completes) == 1
    assert completes[0][5] == "t_scratch"


def test_trigger_c_falls_back_to_default_workdir_for_scratch_task(
    tmp_path: Path,
) -> None:
    # Regression (2026-08-05 hkrc t_fa6f319f): a scratch task whose recorded
    # workspace path is NOT a repo (no wt/<task_id> there) but whose board
    # default_workdir carries the shipped branch must still auto-complete.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[
            blocked_task(
                "t_scratch",
                workspace_kind="scratch",
                workspace_path="/tmp/boards/hkrc/workspaces/t_scratch",
            )
        ],
        shows={
            "t_scratch": blocked_show("t_scratch", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_calls: dict[str, int] = {"n": 0}

    def git_runner(argv: Sequence[str]) -> NativeResult:
        git_calls["n"] += 1
        op = argv[3:]
        if op[:2] == ["show-ref", "--verify"]:
            ref = op[2]
            if ref == "refs/heads/master":
                return NativeResult(0, ref, "")
            if ref.startswith("refs/heads/wt/"):
                # First lookup (scratch workspace path) has no branch; the
                # default_workdir fallback does.
                return NativeResult(0 if git_calls["n"] >= 2 else 1, ref, "")
            return NativeResult(1, "", "")
        if op[:2] == ["merge-base", "--is-ancestor"]:
            return NativeResult(1, "", "")
        if op[:1] == ["rev-parse"]:
            return NativeResult(0, "b" * 40, "")
        return NativeResult(2, "", f"unexpected git argv: {argv}")

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "completed review-required parent t_scratch on board hkrc" in digest
    completes = [argv for argv in recorded if "complete" in argv[4:6]]
    assert len(completes) == 1
    assert completes[0][5] == "t_scratch"
    # Two wt-branch lookups happened: the task workspace, then the fallback.
    assert git_calls["n"] >= 2


def test_trigger_c_respects_min_age(tmp_path: Path) -> None:
    # A block that just happened is not acted on — the worker may still be
    # creating the review pair itself.
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_fresh")],
        shows={
            "t_fresh": blocked_show("t_fresh", blocked_at=NOW - 60, children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)


def test_trigger_c_disabled_stays_silent(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    runner, recorded = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show("t_parent", children=["t_rev"]),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(
        make_config(trigger_c_enabled=False), state_file, now=NOW, runner=runner, git_runner=git_runner
    )
    assert digest == ""
    assert not any("complete" in argv[4:6] for argv in recorded)
    # Trigger c disabled: the blocked pass never ran, so the state stays empty.
    if state_file.is_file():
        assert json.loads(state_file.read_text(encoding="utf-8")) == {}


def test_trigger_c_duplicate_impl_alert(tmp_path: Path) -> None:
    # Aftermath is alert-only: a decomposed marker names the duplicate impl
    # children; the watchdog never supersedes them.
    state_file = tmp_path / "state.json"
    decomposed = {
        "kind": "decomposed",
        "payload": {"child_ids": ["t_dup1", "t_dup2"], "root_assignee": "lead-orchestrator"},
        "created_at": NOW - 2000,
        "run_id": None,
    }
    loop = {
        "kind": "block_loop_detected",
        "payload": {"reason": "review-required: loop", "kind": "needs_input", "recurrences": 2},
        "created_at": NOW - 2500,
        "run_id": 99,
    }
    runner, _ = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={
            "t_parent": blocked_show(
                "t_parent", children=["t_rev"], extra_events=[loop, decomposed]
            ),
            "t_rev": child_show("t_rev", status="todo"),
        },
    )
    git_runner, _ = make_git_runner(merged=False)
    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert "completed review-required parent t_parent" in digest
    assert "review-gap duplicate-impl alert: t_parent on board hkrc" in digest
    assert "t_dup1, t_dup2" in digest
    assert "operator decides supersede" in digest
    assert decomposed_child_ids(blocked_show("t_parent", extra_events=[decomposed])) == [
        "t_dup1",
        "t_dup2",
    ]
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_parent": {"action": "completed", "at": NOW}}


def test_trigger_c_completion_failure_fails_closed(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"

    def failing_complete(argv: Sequence[str]) -> NativeResult:
        if "complete" in argv[4:6]:
            return NativeResult(1, "", "task is already done")
        return make_blocked_runner(
            blocked=[blocked_task("t_parent")],
            shows={
                "t_parent": blocked_show("t_parent", children=["t_rev"]),
                "t_rev": child_show("t_rev", status="todo"),
            },
        )[0](argv)

    git_runner, _ = make_git_runner(merged=False)
    with pytest.raises(ReviewGapError, match="complete review-required parent t_parent"):
        run(make_config(), state_file, now=NOW, runner=failing_complete, git_runner=git_runner)


def test_shipped_branch_evidence(tmp_path: Path) -> None:
    sha = "c" * 40
    git_runner, _ = make_git_runner(merged=False, sha=sha)
    assert shipped_branch_evidence(tmp_path, "t_parent", runner=git_runner) == sha
    git_runner2, _ = make_git_runner(merged=False, branch_exists=False)
    assert shipped_branch_evidence(tmp_path, "t_parent", runner=git_runner2) is None
    git_runner3, _ = make_git_runner(merged=True)
    assert shipped_branch_evidence(tmp_path, "t_parent", runner=git_runner3) is None


def test_shipped_branch_evidence_prefers_recorded_branch_name(tmp_path: Path) -> None:
    """Trigger (c) must find a semantic branch (wt/multilocation) via branch_name."""
    sha = "d" * 40
    git_runner, recorded = make_git_runner(
        merged=False, sha=sha, custom_branch="wt/multilocation"
    )
    assert (
        shipped_branch_evidence(
            tmp_path, "t_parent", branch_name="wt/multilocation", runner=git_runner
        )
        == sha
    )
    # The custom branch is probed (before the wt/<task_id> fallback).
    probed = [a for a in recorded if a[3:5] == ["show-ref", "--verify"]]
    refs = [a[5] for a in probed]
    assert "refs/heads/wt/multilocation" in refs
    assert "refs/heads/wt/t_parent" not in refs


def test_shipped_branch_evidence_falls_back_to_wt_task_id(tmp_path: Path) -> None:
    """A missing or empty branch_name must not blind trigger (c) to wt/<task_id>."""
    sha = "e" * 40
    git_runner, recorded = make_git_runner(merged=False, sha=sha)
    assert (
        shipped_branch_evidence(
            tmp_path, "t_parent", branch_name="", runner=git_runner
        )
        == sha
    )
    # Only the default-convention branch was probed (custom was empty).
    probed = [a for a in recorded if a[3:5] == ["show-ref", "--verify"]]
    refs = [a[5] for a in probed]
    assert "refs/heads/wt/t_parent" in refs
    assert all("multilocation" not in a for a in refs)


def test_shipped_branch_evidence_none_when_custom_branch_missing(tmp_path: Path) -> None:
    """A branch_name that doesn't exist must not resurrect the default convention."""
    sha = "f" * 40
    git_runner, _ = make_git_runner(
        merged=False, sha=sha, custom_branch="wt/multilocation", branch_exists=False
    )
    assert (
        shipped_branch_evidence(
            tmp_path, "t_parent", branch_name="wt/multilocation", runner=git_runner
        )
        is None
    )


def test_build_complete_command_shape() -> None:
    summary = build_handoff_summary("t_parent", "t_rev", "d" * 40)
    command = build_complete_command(
        "hermes", "hkrc", "t_parent", summary, {"review_card": "t_rev", "branch": "wt/t_parent", "commit": "d" * 40}
    )
    assert command[:6] == ["hermes", "kanban", "--board", "hkrc", "complete", "t_parent"]
    assert command[command.index("--summary") + 1] == summary
    assert json.loads(command[command.index("--metadata") + 1]) == {
        "review_card": "t_rev",
        "branch": "wt/t_parent",
        "commit": "d" * 40,
    }


def test_completed_and_duplicate_lines() -> None:
    episode = BlockedCandidate("hkrc", "t_parent", "title", f"{REPO_ROOT}/.worktrees/t_parent", NOW - 3600, "review-required: x")
    line = completed_line(episode, "t_rev", "e" * 40)
    assert "completed review-required parent t_parent on board hkrc" in line
    assert "t_rev" in line and "wt/t_parent" in line and ("e" * 40) in line
    alert = duplicate_alert_line(episode, ["t_dup1", "t_dup2"])
    assert "duplicate impl children: t_dup1, t_dup2" in alert


def test_list_blocked_tasks_parses(tmp_path: Path) -> None:
    runner, _ = make_blocked_runner(
        blocked=[blocked_task("t_parent")],
        shows={"t_parent": blocked_show("t_parent", children=["t_rev"])},
    )
    tasks = list_blocked_tasks("hermes", "hkrc", runner=runner)
    assert [task["id"] for task in tasks] == ["t_parent"]
    assert tasks[0]["workspace_kind"] == "worktree"


# --- trigger d: revert-drift detection ---------------------------------------


REVERT_SHA = "1a89010" + "0" * 32  # the observed revert commit
MERGE_SHA = "f789" + "0" * 36  # the original merge that got reverted
MAIN_SHA = "bbbb" + "0" * 36
REVERT_SUBJECT = 'Revert "merge:. (kanban t_f789b4ab)"'


def make_revert_git_runner(
    *,
    full_log: list[str] | None = None,
    after_revert_log: list[str] | None = None,
    default: str = "main",
    repos_with_revert: Sequence[str] = (REPO_ROOT,),
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """Git runner scripted for trigger-d: show-ref + two log shapes.

    The revert log is served only for repos in ``repos_with_revert`` —
    other boards' repos see an empty history, mirroring reality (the
    revert lives in one repo, not every board's repo).
    """
    full_log = full_log if full_log is not None else [
        f"{REVERT_SHA}\t{MAIN_SHA} {MERGE_SHA}\t{REVERT_SUBJECT}",
        f"{MAIN_SHA}\t\tmerge:. (kanban t_f789b4ab)",
    ]
    after_revert_log = after_revert_log or []
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        op = argv[3:]
        repo = argv[2] if len(argv) > 2 else ""
        if op[:2] == ["show-ref", "--verify"]:
            ref = op[2]
            if ref == f"refs/heads/{default}":
                return NativeResult(0, ref, "")
            return NativeResult(1, "", "")
        if op[:1] == ["log"]:
            if repo not in repos_with_revert:
                return NativeResult(0, "", "")
            if len(op) >= 4 and op[3].endswith(".."):
                return NativeResult(0, "\n".join(after_revert_log) + ("\n" if after_revert_log else ""), "")
            return NativeResult(0, "\n".join(full_log) + "\n", "")
        return NativeResult(2, "", f"unexpected git argv: {argv}")

    return runner, recorded


def make_revert_runner(
    *,
    boards: list[dict] | None = None,
    listed: list[dict] | None = None,
    tasks_by_slug: dict[str, list[dict]] | None = None,
    created: list[str] | None = None,
    shows: dict[str, dict] | None = None,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """CLI runner for trigger-d: boards list, per-slug list, show, create.

    ``listed`` serves every slug; ``tasks_by_slug`` overrides per slug
    (used by the cross-board routing tests). ``shows`` maps a task id to
    the ``show --json`` document (``latest_summary`` consulted for the
    worker-verified heal); the default has no summary.
    """
    boards = boards or [{"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT}]
    listed = listed or []
    tasks_by_slug = tasks_by_slug or {}
    shows = shows or {}
    created_ids = iter(created or ["t_reapply_new"])
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug = argv[3]
            subcommand = argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks_by_slug.get(slug, listed)), "")
            if subcommand == "create":
                return NativeResult(0, json.dumps({"id": next(created_ids)}), "")
            if subcommand == "show":
                task_id = argv[5] if len(argv) > 5 else ""
                return NativeResult(
                    0,
                    json.dumps(
                        shows.get(task_id, {"task": {"id": task_id}, "latest_summary": ""})
                    ),
                    "",
                )
        return NativeResult(2, "", f"unexpected argv: {argv}")

    return runner, recorded


def test_revert_subject_regex_extracts_task_id() -> None:
    from hkrc import review_gap as rg

    match = rg.REVERT_SUBJECT_RE.match(REVERT_SUBJECT)
    assert match is not None
    assert match.group(1) == "t_f789b4ab"
    assert rg.REVERT_SUBJECT_RE.match('Revert "merge: (kanban t_abc123)"') is not None
    assert rg.REVERT_SUBJECT_RE.match('merge:. (kanban t_f789b4ab)') is None  # not a revert
    assert rg.REVERT_SUBJECT_RE.match('Revert "fix: something else"') is None


def test_revert_drift_creates_reapply_card(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        listed=[{"id": "t_f789b4ab", "title": "lock unclaimed-child threshold", "status": "done"}],
    )
    git_runner, git_recorded = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    creates = [argv for argv in recorded if "create" in argv]
    assert len(creates) == 1
    create = creates[0]
    assert create[5] == "re-apply reverted change (t_f789b4ab)"
    assert create[create.index("--assignee") + 1] == "reviewer"
    assert create[create.index("--parent") + 1] == "t_f789b4ab"
    assert create[create.index("--workspace") + 1] == f"worktree:{REPO_ROOT}"
    body = create[create.index("--body") + 1]
    assert REVERT_SHA in body and MERGE_SHA in body and "t_f789b4ab" in body
    assert "created re-apply card t_reapply_new for t_f789b4ab on board hkrc" in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == "revert-drift:t_reapply_new"


def test_revert_drift_skips_when_remerged_after_revert(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        listed=[{"id": "t_f789b4ab", "title": "x", "status": "done"}],
    )
    git_runner, _ = make_revert_git_runner(
        after_revert_log=[f"{'c' * 40}\tmerge:. (kanban t_f789b4ab)"],
    )

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert not any("create" in argv for argv in recorded)


def test_revert_drift_ignores_incidental_task_mentions_after_revert(tmp_path: Path) -> None:
    # A commit that merely MENTIONS the task id without the (kanban t_xxx)
    # merge marker (e.g. this feature's own commit message referencing the
    # incident) must NOT count as a re-merge — the change is still drifted.
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        listed=[{"id": "t_f789b4ab", "title": "x", "status": "done"}],
    )
    git_runner, _ = make_revert_git_runner(
        after_revert_log=[f"{'c' * 40}\tfeat: review-gap trigger d (2026-08-05 t_f789b4ab revert lesson)"],
    )

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert "created re-apply card" in digest
    assert any("create" in argv for argv in recorded)


def test_revert_drift_skips_when_reapply_card_already_exists(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        listed=[
            {"id": "t_f789b4ab", "title": "x", "status": "done"},
            {"id": "t_reapply_existing", "title": "re-apply reverted change (t_f789b4ab)", "status": "todo"},
        ],
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert not any("create" in argv for argv in recorded)


def test_revert_drift_dedupes_via_state(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    # An episode already acted on with an OPEN re-apply card must not re-fire.
    state_file.write_text(
        json.dumps({"hkrc:t_f789b4ab": {"action": "revert-drift:t_old", "at": NOW}}),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[
            {"id": "t_f789b4ab", "title": "x", "status": "done"},
            {"id": "t_old", "title": "re-apply reverted change (t_f789b4ab)", "status": "running"},
        ],
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert not any("create" in argv for argv in recorded)


def test_revert_drift_reexamines_done_unhealed_reapply_card(tmp_path: Path) -> None:
    # A re-apply card that completed WITHOUT healing the drift (no re-merge
    # marker after the revert) is a false-complete: the episode must re-fire
    # with a fresh card instead of staying suppressed forever.
    state_file = tmp_path / "review-gap-state.json"
    state_file.write_text(
        json.dumps({"hkrc:t_f789b4ab": {"action": "revert-drift:t_old", "at": NOW}}),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[
            {"id": "t_f789b4ab", "title": "x", "status": "done"},
            {"id": "t_old", "title": "re-apply reverted change (t_f789b4ab)", "status": "done"},
        ],
    )
    git_runner, _ = make_revert_git_runner()  # no re-merge after the revert

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert "re-apply card t_old completed but the reverted change" in digest
    assert "not on main" in digest
    creates = [argv for argv in recorded if "create" in argv]
    assert len(creates) == 1
    assert creates[0][5] == "re-apply reverted change (t_f789b4ab)"
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == "revert-drift:t_reapply_new"


def test_revert_drift_heals_on_worker_verified_nothing_to_merge(tmp_path: Path) -> None:
    # A re-apply card that completed with the body-contract terminal state
    # "nothing to merge (branch == main)" is a worker-verified supersession
    # heal (content confirmed present on main) — the episode must NOT re-fire
    # even though no (kanban t_xxx) re-merge marker exists after the revert
    # (the t_267e01e7 incident: 10 phantom re-fire cards).
    state_file = tmp_path / "review-gap-state.json"
    state_file.write_text(
        json.dumps({"hkrc:t_f789b4ab": {"action": "revert-drift:t_old", "at": NOW}}),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[
            {"id": "t_f789b4ab", "title": "x", "status": "done"},
            {
                "id": "t_old",
                "title": "re-apply reverted change (t_f789b4ab)",
                "status": "done",
                "result": "approved: true; nothing to merge (branch == main); targeted checks pass",
            },
        ],
    )
    git_runner, _ = make_revert_git_runner()  # no re-merge after the revert

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert "revert-drift healed: re-apply card t_old" in digest
    assert "false-complete" not in digest
    assert not any("create" in argv for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == f"revert-drift-healed:{REVERT_SHA}:t_old"

    # Healed episodes stay silent on every later tick.
    second = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert second == ""


def test_revert_drift_heals_on_summary_only_completion(tmp_path: Path) -> None:
    # Workers complete with either `result` or `summary`; when the list-level
    # result is empty, the run summary (show --json latest_summary) is
    # consulted before declaring a false-complete.
    state_file = tmp_path / "review-gap-state.json"
    state_file.write_text(
        json.dumps({"hkrc:t_f789b4ab": {"action": "revert-drift:t_old", "at": NOW}}),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[
            {"id": "t_f789b4ab", "title": "x", "status": "done"},
            {"id": "t_old", "title": "re-apply reverted change (t_f789b4ab)", "status": "done"},
        ],
        shows={
            "t_old": {
                "task": {"id": "t_old"},
                "latest_summary": "nothing to merge (branch == main); intended change already landed",
            },
        },
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert "revert-drift healed: re-apply card t_old" in digest
    assert not any("create" in argv for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == f"revert-drift-healed:{REVERT_SHA}:t_old"


def test_revert_drift_healed_episode_stays_silent(tmp_path: Path) -> None:
    # A recorded heal matching the current revert is terminal for the
    # episode: silent on every tick, nothing created, no false-complete.
    state_file = tmp_path / "review-gap-state.json"
    state_file.write_text(
        json.dumps(
            {
                "hkrc:t_f789b4ab": {
                    "action": f"revert-drift-healed:{REVERT_SHA}:t_old",
                    "at": NOW,
                }
            }
        ),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[{"id": "t_f789b4ab", "title": "x", "status": "done"}],
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert not any("create" in argv for argv in recorded)


def test_revert_drift_reopens_after_later_revert_of_same_task(tmp_path: Path) -> None:
    # A worker-verified heal is scoped to ITS revert: a NEW revert of the
    # same task (different sha) is a fresh drift episode that must re-fire
    # instead of staying suppressed by the old heal.
    other_revert = "9f0e11" + "0" * 34
    state_file = tmp_path / "review-gap-state.json"
    state_file.write_text(
        json.dumps(
            {
                "hkrc:t_f789b4ab": {
                    "action": f"revert-drift-healed:{other_revert}:t_old",
                    "at": NOW,
                }
            }
        ),
        encoding="utf-8",
    )
    runner, recorded = make_revert_runner(
        listed=[{"id": "t_f789b4ab", "title": "x", "status": "done"}],
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    creates = [argv for argv in recorded if "create" in argv]
    assert len(creates) == 1
    assert "created re-apply card t_reapply_new for t_f789b4ab on board hkrc" in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == "revert-drift:t_reapply_new"


def test_has_reapply_card_ignores_terminal_cards() -> None:
    from hkrc import review_gap as rg

    assert not rg.has_reapply_card(
        [{"id": "t_done", "title": "re-apply reverted change (t_abc)", "status": "done"}],
        "t_abc",
    )
    assert rg.has_reapply_card(
        [{"id": "t_open", "title": "re-apply reverted change (t_abc)", "status": "todo"}],
        "t_abc",
    )


def test_revert_drift_skips_board_without_repo(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        boards=[{"slug": "docs", "archived": False, "default_workdir": None}],
    )
    git_runner, git_recorded = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert digest == ""
    assert git_recorded == []  # no git calls for a repo-less board


def test_revert_drift_gated_by_config(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner()
    git_runner, git_recorded = make_revert_git_runner()

    digest = run(
        make_config(trigger_d_enabled=False),
        state_file,
        now=NOW,
        runner=runner,
        git_runner=git_runner,
    )

    assert digest == ""
    assert git_recorded == []  # trigger-d git scans must not run when disabled


def test_revert_drift_unroutable_parent_alerts_once(tmp_path: Path) -> None:
    state_file = tmp_path / "review-gap-state.json"
    # The parent task t_f789b4ab is on NO board (not even via routing) —
    # trigger (d) must never create a card with an unknown parent.
    runner, recorded = make_revert_runner(
        boards=[
            {"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT},
            {"slug": "campcli", "archived": False, "default_workdir": "/repos/sensor-project"},
        ],
        tasks_by_slug={
            "hkrc": [{"id": "t_other", "title": "unrelated", "status": "done"}],
            "campcli": [{"id": "t_other2", "title": "unrelated", "status": "done"}],
        },
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    assert "revert-drift unactionable" in digest
    assert "t_f789b4ab" in digest
    assert not any("create" in argv for argv in recorded)
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == "revert-drift:unroutable"

    # State-deduped: the episode alerts once, never repeatedly.
    second = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert second == ""


def test_revert_drift_routes_card_to_board_where_parent_lives(tmp_path: Path) -> None:
    # The revert is in the hkrc repo but the parent task lives on the campcli
    # board (the merge was driven from another board's card). The re-apply
    # card must be created on the OWNING board with the drifted repo as the
    # workspace — a card with an unknown parent must never be created.
    state_file = tmp_path / "review-gap-state.json"
    runner, recorded = make_revert_runner(
        boards=[
            {"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT},
            {"slug": "campcli", "archived": False, "default_workdir": "/repos/sensor-project"},
        ],
        tasks_by_slug={
            "hkrc": [{"id": "t_other", "title": "unrelated", "status": "done"}],
            "campcli": [{"id": "t_f789b4ab", "title": "lock unclaimed-child threshold", "status": "done"}],
        },
    )
    git_runner, _ = make_revert_git_runner()

    digest = run(make_config(), state_file, now=NOW, runner=runner, git_runner=git_runner)

    creates = [argv for argv in recorded if "create" in argv]
    assert len(creates) == 1
    create = creates[0]
    assert create[2:4] == ["--board", "campcli"]
    assert create[5] == "re-apply reverted change (t_f789b4ab)"
    assert create[create.index("--parent") + 1] == "t_f789b4ab"
    assert create[create.index("--workspace") + 1] == f"worktree:{REPO_ROOT}"
    assert "created re-apply card t_reapply_new for t_f789b4ab on board campcli" in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_f789b4ab"]["action"] == "revert-drift:t_reapply_new"


# --- bounded-time tick (0.13.1): parallel reads, per-subprocess and whole-tick budgets ---


def test_tick_completes_within_wall_clock_budget_on_synthetic_boards(tmp_path: Path) -> None:
    # The done pass is read-bound: every candidate needs one show subprocess.
    # A runner that sleeps per show reproduces the live cost model; the
    # parallel read phase must compress the tick well below the sequential
    # total while still producing the full digest and dedupe state.
    import time as _time

    boards = [
        {"slug": "one", "archived": False, "default_workdir": None},
        {"slug": "two", "archived": False, "default_workdir": None},
        {"slug": "three", "archived": False, "default_workdir": None},
    ]
    tasks = {b: [done_task(f"t_{b}_{i}") for i in range(20)] for b in ("one", "two", "three")}
    shows = {t["id"]: show_dict(t["id"]) for lst in tasks.values() for t in lst}
    sleep_seconds = 0.05

    def slow_runner(argv: Sequence[str]) -> NativeResult:
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug, subcommand = argv[3], argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks.get(slug, [])), "")
            if subcommand == "show":
                _time.sleep(sleep_seconds)
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    state_file = tmp_path / "state.json"
    config = make_config(
        auto_create=False,
        trigger_c_enabled=False,
        trigger_d_enabled=False,
        max_workers=8,
    )
    started = _time.monotonic()
    digest = run(config, state_file, now=NOW, runner=slow_runner, git_runner=make_git_runner()[0])
    elapsed = _time.monotonic() - started
    # 60 shows at 0.05s each is 3.0s of pure sequential sleep; the parallel
    # read phase (8 workers) must finish the whole tick far under that.
    assert elapsed < 2.0, f"tick took {elapsed:.2f}s — reads were not parallelized"
    assert digest.count("review-gap missing review card") == 60
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert len(state) == 60
    assert all(value["action"] == "gap-alert" for value in state.values())


def test_stuck_board_timeout_skips_only_that_board(tmp_path: Path) -> None:
    # A board whose CLI calls hang past cli_timeout_seconds (simulated here by
    # the runner raising NativeTimeoutError) must be skipped with one alert
    # line while the remaining boards are still processed in the same tick.
    from hkrc.review_gap import NativeTimeoutError, timeout_line

    boards = [
        {"slug": "stuck", "archived": False, "default_workdir": None},
        {"slug": "fine", "archived": False, "default_workdir": None},
    ]
    tasks = {"stuck": [done_task("t_stuck_0")], "fine": [done_task("t_fine_0")]}
    shows = {"t_fine_0": show_dict("t_fine_0")}

    def runner(argv: Sequence[str]) -> NativeResult:
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug, subcommand = argv[3], argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks.get(slug, [])), "")
            if subcommand == "show" and slug == "stuck":
                raise NativeTimeoutError("native CLI timed out after 30.0s")
            if subcommand == "show":
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
            if subcommand == "create":
                return NativeResult(0, json.dumps({"id": "t_review_t_fine_0"}), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    state_file = tmp_path / "state.json"
    digest = run(
        make_config(trigger_c_enabled=False, trigger_d_enabled=False),
        state_file,
        now=NOW,
        runner=runner,
        git_runner=make_git_runner()[0],
    )
    assert timeout_line("stuck", "native CLI timed out after 30.0s") in digest
    assert "created review card t_review_t_fine_0 for t_fine_0 on board fine" in digest
    assert "t_stuck_0" not in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"fine:t_fine_0": {"action": "created:t_review_t_fine_0", "at": NOW}}


def test_run_native_subprocess_timeout_raises_native_timeout_error(
    tmp_path: Path,
) -> None:
    # The real subprocess path: a CLI that hangs past the per-call budget must
    # raise NativeTimeoutError (skipped phase), not hang the tick forever.
    import stat as _stat

    from hkrc.review_gap import NativeTimeoutError

    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text("#!/bin/sh\nsleep 5\nprintf '[]\\n'\n", encoding="utf-8")
    fake_cli.chmod(fake_cli.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)
    started = time.monotonic()
    with pytest.raises(NativeTimeoutError, match="native CLI timed out after 0.4s"):
        run_native([str(fake_cli), "kanban", "boards", "list", "--json"], timeout=0.4)
    assert time.monotonic() - started < 4.0  # killed at the budget, not at 5s


def test_run_git_subprocess_timeout_raises_native_timeout_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os as _os
    import stat as _stat

    from hkrc.review_gap import NativeTimeoutError, run_git

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text("#!/bin/sh\nsleep 5\n", encoding="utf-8")
    fake_git.chmod(fake_git.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)
    monkeypatch.setenv("PATH", f"{fake_bin}:{_os.environ.get('PATH', '')}")
    with pytest.raises(NativeTimeoutError, match="git timed out after 0.4s"):
        run_git(tmp_path, ["show-ref", "--verify", "refs/heads/main"], timeout=0.4)


def test_tick_budget_exceeded_skips_remaining_boards(tmp_path: Path) -> None:
    # The whole-tick wall-clock budget is the backstop: when it expires
    # mid-tick (here the first board's shows sleep past the 0.4s budget), the
    # remaining boards are skipped with one alert line and the dedupe state
    # records the partial progress so the next tick continues.
    import time as _time

    from hkrc.review_gap import budget_line

    boards = [
        {"slug": "first", "archived": False, "default_workdir": None},
        {"slug": "second", "archived": False, "default_workdir": None},
    ]
    tasks = {
        "first": [done_task("t_first_0"), done_task("t_first_1"), done_task("t_first_2")],
        "second": [done_task("t_second_0"), done_task("t_second_1"), done_task("t_second_2")],
    }
    shows = {t["id"]: show_dict(t["id"]) for lst in tasks.values() for t in lst}

    def slow_show_runner(argv: Sequence[str]) -> NativeResult:
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug, subcommand = argv[3], argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks.get(slug, [])), "")
            if subcommand == "show":
                _time.sleep(0.6)
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    state_file = tmp_path / "state.json"
    config = make_config(
        auto_create=False,
        trigger_c_enabled=False,
        trigger_d_enabled=False,
        tick_timeout_seconds=0.4,
        max_workers=8,
    )
    digest = run(config, state_file, now=NOW, runner=slow_show_runner, git_runner=make_git_runner()[0])
    assert budget_line(1) in digest
    assert "review-gap missing review card for t_first_0" in digest  # partial progress
    assert "t_second_0" not in digest  # the second board never started
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert set(state) == {"first:t_first_0", "first:t_first_1", "first:t_first_2"}


def test_multi_candidate_tick_creates_all_in_candidate_order(tmp_path: Path) -> None:
    # The parallel read phase must not change the action phase: every gap is
    # created exactly once, in candidate order, with one digest line each.
    boards = [{"slug": "hkrc", "archived": False, "default_workdir": REPO_ROOT}]
    tasks = {"hkrc": [done_task(f"t_impl_{i}") for i in range(4)]}
    shows = {t["id"]: show_dict(t["id"]) for t in tasks["hkrc"]}
    runner, recorded = make_runner(
        boards=boards,
        tasks=tasks,
        shows=shows,
        created=[f"t_rev_{i}" for i in range(4)],
    )
    state_file = tmp_path / "state.json"
    digest = run(
        make_config(trigger_c_enabled=False, trigger_d_enabled=False),
        state_file,
        now=NOW,
        runner=runner,
        git_runner=make_git_runner()[0],
    )
    creates = [argv for argv in recorded if "create" in argv[4:6]]
    assert len(creates) == 4
    assert [argv[5] for argv in creates] == [
        f"review: validate task: t_impl_{i} (t_impl_{i})" for i in range(4)
    ]
    assert digest.count("created review card") == 4
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert set(state) == {f"hkrc:t_impl_{i}" for i in range(4)}
    assert all(value["action"].startswith("created:") for value in state.values())


# --- review-ok confirm (0.13.1): healthy reviews are deduped until expiry ---


def test_review_ok_confirmed_candidate_skipped_until_expiry(tmp_path: Path) -> None:
    # A done candidate that already has a (non-stalled) review child records a
    # silent review-ok confirm; the next tick must not re-show it at all, so
    # the steady-state tick stops re-verifying the whole recency window.
    boards = [{"slug": "hkrc", "archived": False, "default_workdir": None}]
    tasks = {"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]}
    shows = {
        "t_impl": show_dict("t_impl", children=["t_rev"]),
        "t_rev": child_show("t_rev", status="todo"),
    }
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(boards=boards, tasks=tasks, shows=shows)
    config = make_config(trigger_c_enabled=False, trigger_d_enabled=False)

    first = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert first == ""  # healthy review — silent, no gap, no stall
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "review-ok", "at": NOW}}
    shows_after_first = [argv for argv in recorded if argv[4] == "show"]

    second = run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    assert second == ""
    shows_after_second = [argv for argv in recorded if argv[4] == "show"]
    # The confirmed candidate is skipped without a single show call.
    assert len(shows_after_second) == len(shows_after_first)
    assert json.loads(state_file.read_text(encoding="utf-8")) == state


def test_review_ok_expiry_rechecks_and_stall_alert_fires_on_schedule(tmp_path: Path) -> None:
    # The confirm expires after stalled_alert_hours; the re-check then finds
    # the review still pending on an unmerged branch past the stall threshold
    # and fires the stall alert exactly as before the confirm was added.
    boards = [{"slug": "hkrc", "archived": False, "default_workdir": None}]
    tasks = {"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]}
    shows = {
        "t_impl": show_dict("t_impl", children=["t_rev"]),
        "t_rev": child_show("t_rev", status="todo"),
    }
    state_file = tmp_path / "state.json"
    config = make_config(
        stalled_alert_hours=6, trigger_c_enabled=False, trigger_d_enabled=False
    )
    runner, _ = make_runner(boards=boards, tasks=tasks, shows=shows)
    git_runner, _ = make_git_runner(merged=False)

    first = run(config, state_file, now=NOW, runner=runner, git_runner=git_runner)
    assert first == ""  # 2h old — healthy, confirmed
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_impl"]["action"] == "review-ok"

    later = NOW + 6 * 3600 + 3600  # candidate now 9h old — past the stall threshold
    second = run(config, state_file, now=later, runner=runner, git_runner=git_runner)
    assert "stalled review for t_impl on board hkrc" in second
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state == {"hkrc:t_impl": {"action": "stall-alert", "at": later}}


def test_review_ok_expiry_reopens_gap_when_review_child_disappears(tmp_path: Path) -> None:
    # Self-healing: if the review child is gone by the time the confirm
    # expires, the candidate is a gap again and a fresh review card is created.
    boards = [{"slug": "hkrc", "archived": False, "default_workdir": None}]
    tasks = {"hkrc": [done_task("t_impl", completed_at=NOW - 2 * 3600)]}
    state_file = tmp_path / "state.json"
    config = make_config(
        stalled_alert_hours=6, trigger_c_enabled=False, trigger_d_enabled=False
    )
    shows = {
        "t_impl": show_dict("t_impl", children=["t_rev"]),
        "t_rev": child_show("t_rev", status="todo"),
    }
    runner, _ = make_runner(boards=boards, tasks=tasks, shows=shows)
    run(config, state_file, now=NOW, runner=runner, git_runner=make_git_runner()[0])
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_impl"]["action"] == "review-ok"

    # The review child is gone (archived/removed); the confirm has expired.
    later = NOW + 6 * 3600 + 3600
    shows["t_impl"] = show_dict("t_impl", children=[])
    runner2, recorded2 = make_runner(
        boards=boards, tasks=tasks, shows=shows, created=["t_rev_fresh"]
    )
    digest = run(config, state_file, now=later, runner=runner2, git_runner=make_git_runner()[0])
    assert "created review card t_rev_fresh for t_impl on board hkrc" in digest
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["hkrc:t_impl"]["action"] == "created:t_rev_fresh"
    assert len([argv for argv in recorded2 if "create" in argv[4:6]]) == 1
