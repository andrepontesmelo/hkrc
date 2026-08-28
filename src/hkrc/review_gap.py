"""Review-gap watchdog: auto-create missing review cards for done impl tasks.

A done implementation/fix card with no paired review card leaves its branch
lost and unmerged — the merge is done by the reviewer, and with no reviewer
task nothing ever merges. This module is the deterministic backstop: every
tick it finds done ``worktree`` tasks (implementation work, not research or
docs) completed within the recency window, and either auto-creates the missing
review card or alerts on a stalled review. It also resolves the loop where a
worker ships its work and blocks ``review-required:`` instead of completing:
a blocked parent whose branch shipped and whose review child already exists is
auto-completed so the review child promotes (trigger c). Finally, it detects
reverted kanban merges that were never re-applied and creates ``re-apply
reverted change`` cards for them (trigger d) — merges stay reviewer-controlled.
A re-apply card whose worker verified the reverted content on main
(completion text carrying ``nothing to merge (branch == main)``) heals the
episode; a terminal card without that verification is a false-complete and
the episode re-fires.

Hard constraint — CLI only, NO sqlite
------------------------------------
Never open hermes kanban sqlite databases directly. All kanban reads AND
writes go through the ``hermes kanban`` CLI (``boards list --json``, ``list
--status done --json``, ``list --status blocked --json``, ``show <id> --json``,
``create ... --json``, ``complete ... --summary --metadata``). Git
operations on project repos (branch existence/merge checks) are fine. Every
native CLI subprocess runs with ``HERMES_KANBAN_*`` and ``_HERMES_GATEWAY``
removed from its environment so the pinned board env from the dispatcher can
never override the ``--board`` flag (see ``build_native_environment``).

Detection logic (per tick)
--------------------------
1. Boards: ``hermes kanban boards list --json``. All non-archived boards, not
   hardcoded — no board allowlist.
2. Candidate tasks: ``hermes kanban --board <slug> list --status done --json``.
   Keep tasks where ``workspace_kind == "worktree"``, completed within the
   recency window (config, default 48h), and completed at least
   ``min_age_seconds`` ago (default 300s — don't race the worker that just
   completed; it may still be creating the pair itself). A task that is ITSELF
   a review card (created by the reviewer profile or worked by a reviewer
   run) is never a candidate — review cards do not get review children, so no
   review-of-review is ever created.
3. Has-review check (deterministic, no title parsing): for the candidate's
   children (from ``show <id> --json`` ``children[]``), a review exists if ANY
   child satisfies:
   - the child's ``created`` event payload has ``assignee: "reviewer"``
     (catches pending/stuck reviews), OR
   - the child has any ``task_run`` with ``profile: "reviewer"`` (catches
     delegation/reassignment — the current assignee may have changed after the
     reviewer passed the card along).
   Never match on the child's current assignee alone, and never match on title
   text.

Auto-create (trigger a) — no review child exists
------------------------------------------------
Create the review card via the CLI as ``review: validate <title> (<task_id>)``
with ``--assignee reviewer --parent <task_id> --workspace
worktree:<repo-root> --priority 90``. ``<repo-root>`` derives from the task's
``workspace_path`` (the ``.worktrees/<id>`` suffix is stripped when present;
the board ``default_workdir`` is the fallback). The body states that the code
lives on ``wt/<task_id>`` — NOT on main, do NOT rebase onto main — and the
merge contract (``git merge --no-ff`` from ``wt/<task_id>``, conventional
message referencing this task and the impl task, repo gate on merged main,
``merge_sha`` in completion metadata). The created card id is recorded in the
dedupe state so the same gap never re-fires.

Trigger b — stalled review / stalled merge
------------------------------------------
If a review card exists but is not ``done``, and the branch ``wt/<task_id>``
is still unmerged in the repo (``git merge-base --is-ancestor wt/<task_id>
main/master`` fails) for more than ``stalled_alert_hours`` (default 6h) since
the task completed — emit an alert line. A DONE review is not trusted as
"merged": a done impl whose review child is done is a *stalled merge* unless
git truth confirms the impl commit is an ancestor of the canonical branch
(``origin/<default>`` when resolvable, or local ``<default>`` when the local
canonical branch contains the commit). After
the stall window the watchdog auto-creates a re-validation review card (same
shape as trigger (a)'s create: ``review: validate <title> (<task_id>)``,
assignee reviewer, parent the impl, workspace ``worktree:<repo-root>``) whose
body names the source-of-truth branch/commit and the rebase-onto-origin merge
contract; with ``auto_create`` disabled it stays alert-only. The has-review
dedupe does NOT skip this path — the done review child is exactly the failure
being repaired.

Auto-complete (trigger c) — parent blocked review-required with shipped work
---------------------------------------------------------------------------
A worker that ships a commit and then blocks ``review-required:`` (block
reason prefix, kind ``needs_input``) strands its review child: the child
cannot promote while the parent is blocked, so the merge never happens. The
The watchdog finds ``status=blocked`` tasks whose latest ``blocked``
event reason starts with ``review-required:`` and, only when ALL of the
following hold, completes the parent via the CLI (never the review child):

1. The branch ``wt/<task_id>`` exists in the repo AND is not merged into
   ``main``/``master`` (``git merge-base --is-ancestor wt/<task_id> main``
   fails) — a missing branch means the block is a real question, not a
   handoff. The branch is looked up in the task's own workspace repo, then
   (for non-worktree tasks, e.g. scratch workspaces) in the board's
   ``default_workdir`` — the recorded workspace kind is not trusted as the
   sole signal that work shipped (observed 2026-08-05: hkrc t_fa6f319f
   recorded ``scratch`` but shipped a real ``wt/t_fa6f319f`` branch).
2. A review child exists (same deterministic has-review check as triggers
   a/b: created-event assignee or reviewer run).
3. The block happened at least ``min_age_seconds`` ago (default 300s) — don't
   race the worker that just blocked.

The completion summary records the review child id, the branch, and the
shipped commit sha; completing the parent promotes the review child, and the
reviewer still gates the merge. Aftermath is alert-only in v1: if the parent's
events carry a ``decomposed``/``block_loop_detected`` marker, one alert line
names the auto-decomposed impl children that duplicated the shipped work so
the operator can decide supersede — never auto-supersede.

Output contract (watchdog style)
--------------------------------
Telegram-ready digest: one line per action taken (created review card,
completed parent, alert line). Empty stdout = silent (nothing to report). Exit
0 on success, non-zero on real failure. Dedupe: state JSON keyed
``"<board>:<task_id>" -> {"action": ..., "at": <timestamp>}``; a gap
pings/creates/completes ONCE per episode, never repeatedly while the state
persists. A candidate with a healthy review child records a silent
``review-ok`` confirm and is skipped until the confirm expires
(``stalled_alert_hours``) — the steady-state tick re-shows only expired
confirms and new candidates instead of re-verifying the whole 48h window
every tick. Corrupt state fails closed (raises ``ReviewGapError`` so cron
delivers an error alert).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import cast

from .config import ControllerConfig, ReviewGapConfig
from .handoff import NativeResult

STATE_FILENAME = "review-gap-state.json"
REVIEW_ASSIGNEE = "reviewer"
REVIEW_PRIORITY = 90
DEFAULT_BRANCH_CANDIDATES = ("main", "master")
# A worker that ships a commit and wants a reviewer blocks with this reason
# prefix (kanban_block kind needs_input). The review-required loop fires when
# the worker blocks instead of completing: trigger (c) auto-completes the
# parent when the work is shipped and a review child exists.
REVIEW_REQUIRED_PREFIX = "review-required:"
# A revert of a kanban-driven merge, e.g. `Revert "merge:. (kanban t_f789b4ab)"`.
# Trigger (d) scans canonical-main history for these; a revert with no
# re-merge after it means the merged work is silently off main.
REVERT_SUBJECT_RE = re.compile(
    r'^Revert "merge:.*\(kanban (t_[a-z0-9]+)\)"$'
)
# Full 40-hex commit sha. Metadata values read as impl commits MUST match this
# (observed 2026-08-06: a reviewer recorded merge_sha = "no-op (branch ==
# ancestor of main); main HEAD e5f850c" — a non-sha "nothing to merge" marker
# that must fall through to branch resolution, never become a git argument).
_FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
# Title prefix of the re-apply cards trigger (d) itself creates; an existing
# open card (even if the dedupe state was lost) suppresses re-firing.
REAPPLY_TITLE_PREFIX = "re-apply reverted change"
# Title prefix of the re-validation cards the stalled-merge path creates
# (same shape as trigger (a)'s review cards: ``review: validate <title>
# (<task_id>)``). An OPEN re-validation card for the same impl suppresses
# re-creation; a terminal one does not — a done re-validation card whose
# merge still never landed is the failure being repaired.
REVALIDATION_TITLE_PREFIX = "review: validate "
# Canonical terminal-state marker the re-apply body contract tells the worker
# to state when the branch is identical to main and the reverted content is
# verified present: `nothing to merge (branch == main)`. A done re-apply card
# carrying this marker in its completion text is a worker-verified
# supersession heal — the episode must NOT re-fire (see
# _reapply_card_verified_healed). Matched case-insensitively.
NOTHING_TO_MERGE_MARKER = "nothing to merge (branch == main)"
# State action prefix for a worker-verified heal, recorded as
# `revert-drift-healed:<revert_sha>:<card_id>` — scoped to the revert so a
# LATER revert of the same task re-opens the episode.
_REVERT_DRIFT_HEALED_PREFIX = "revert-drift-healed:"
_NOTHING_TO_MERGE_RE = re.compile(re.escape(NOTHING_TO_MERGE_MARKER), re.IGNORECASE)
# A review child that reached either terminal state is no longer "pending";
# a pending review is what can stall a merge.
_TERMINAL_STATUSES = frozenset({"done", "archived"})
_STATE_ACTIONS = frozenset(
    {
        "created",
        "stall-alert",
        "gap-alert",
        "error:no-repo-root",
        "completed",
        "revert-drift",
        "revert-drift-healed",
        # A done candidate that already has a (non-stalled) review child is
        # confirmed here instead of being re-shown every tick; the entry
        # expires after stalled_alert_hours so the stall check still fires on
        # schedule and a review child that later disappears re-opens the gap.
        "review-ok",
    }
)


class ReviewGapError(RuntimeError):
    """Raised when the review-gap watchdog cannot inspect or mutate the board safely."""


class NativeTimeoutError(ReviewGapError):
    """Raised when a native CLI or git subprocess exceeds its deadline.

    Unlike other ReviewGapErrors, a timeout means a stuck phase, not a hard
    failure: the affected board/pass is skipped with an alert line and the
    tick continues with the remaining boards, so one hung ``hermes kanban``
    call cannot stall the rest of the tick.
    """


CliRunner = Callable[[Sequence[str]], NativeResult]


@dataclass(frozen=True, slots=True)
class BoardInfo:
    """One non-archived kanban board and its worktree fallback directory."""

    slug: str
    default_workdir: str | None


@dataclass(frozen=True, slots=True)
class Candidate:
    """A done ``worktree`` task eligible for the review-gap check."""

    board_slug: str
    task_id: str
    title: str
    workspace_path: str | None
    completed_at: int

    @property
    def key(self) -> str:
        """Dedupe key: ``<board>:<task_id>``."""
        return f"{self.board_slug}:{self.task_id}"


@dataclass(frozen=True, slots=True)
class ReviewChild:
    """A child task that satisfies the deterministic has-review check.

    ``runs`` carries the child's run history so the stalled-merge check can
    resolve the impl commit the reviewer validated (``implementation_commit``
    / ``commit`` keys in the completing run's metadata).
    """

    task_id: str
    status: str
    runs: tuple[Mapping[str, object], ...] = ()

    @property
    def pending(self) -> bool:
        return self.status not in _TERMINAL_STATUSES


@dataclass(frozen=True, slots=True)
class BlockedCandidate:
    """A ``worktree`` task blocked with a ``review-required:`` reason.

    ``blocked_at`` is the ``created_at`` of the latest ``blocked`` event — the
    moment the current episode opened — and anchors the min-age gate so the
    watchdog never races the worker that just blocked.
    """

    board_slug: str
    task_id: str
    title: str
    workspace_path: str | None
    blocked_at: int
    reason: str

    @property
    def key(self) -> str:
        """Dedupe key: ``<board>:<task_id>``."""
        return f"{self.board_slug}:{self.task_id}"


def default_state_path(state_db: Path) -> Path:
    """Controller-owned state file next to the controller state database."""
    return state_db.parent / STATE_FILENAME


# --- native CLI / git execution ---------------------------------------------


# Environment variables that must never reach the native CLI subprocess. The
# kanban dispatcher exports HERMES_KANBAN_* (board, db, task id, run id, claim
# lock, goal mode, ...) into worker environments; the native CLI honors the
# pinned HERMES_KANBAN_BOARD / HERMES_KANBAN_DB env OVER the --board flag, so
# leaking them would scan and alert on the wrong board. _HERMES_GATEWAY would
# let the nested CLI hijack the running gateway's live stream. Same scrub the
# needs-input-watcher LLM summarizer applies (see needs_input_watcher.build_llm_environment).
_NATIVE_ENV_STRIP_PREFIXES = ("HERMES_KANBAN_",)
_NATIVE_ENV_STRIP_EXACT = ("_HERMES_GATEWAY",)


def build_native_environment(
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the native CLI environment with explicit HOME/HERMES_HOME.

    Every ``HERMES_KANBAN_*`` variable and ``_HERMES_GATEWAY`` are removed so
    the ``--board`` flag — not the ambient dispatcher env — selects the board;
    ``HOME`` is pinned and ``HERMES_HOME`` defaults to ``<HOME>/.hermes`` when
    absent, matching ``needs_input_watcher.build_llm_environment``.
    """
    env = dict(os.environ if base is None else base)
    for key in list(env):
        if key.startswith(_NATIVE_ENV_STRIP_PREFIXES) or key in _NATIVE_ENV_STRIP_EXACT:
            env.pop(key, None)
    home = env.get("HOME") or str(Path.home())
    env["HOME"] = home
    env.setdefault("HERMES_HOME", os.path.join(home, ".hermes"))
    return env


def run_native(
    argv: Sequence[str],
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> NativeResult:
    """Run one native CLI invocation as an argv list; ``runner`` exists for tests.

    ``timeout`` (seconds, from ``review_gap.cli_timeout_seconds``) caps every
    subprocess so a hung CLI call cannot stall the tick; a timeout raises
    ``NativeTimeoutError`` (skipped phase) instead of failing the whole tick.
    """
    if runner is not None:
        return runner(list(argv))
    try:
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            env=build_native_environment(),
            timeout=timeout,
        )
        return NativeResult(completed.returncode, completed.stdout, completed.stderr)
    except subprocess.TimeoutExpired as exc:
        raise NativeTimeoutError(f"native CLI timed out after {timeout}s") from exc
    except OSError as exc:
        return NativeResult(127, "", str(exc))


def run_git(
    repo_root: Path,
    argv: Sequence[str],
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> NativeResult:
    """Run one git command scoped to ``repo_root``; never a shell command."""
    command = ["git", "-C", str(repo_root), *argv]
    if runner is not None:
        return runner(list(command))
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        return NativeResult(completed.returncode, completed.stdout, completed.stderr)
    except subprocess.TimeoutExpired as exc:
        raise NativeTimeoutError(f"git timed out after {timeout}s") from exc
    except OSError as exc:
        return NativeResult(127, "", str(exc))


# --- boards / tasks through the CLI -----------------------------------------


def _parse_json_stdout(result: NativeResult, what: str) -> object:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise ReviewGapError(f"{what} failed (exit {result.returncode}): {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ReviewGapError(f"{what} returned unparseable JSON: {exc}") from exc


def discover_boards(
    cli: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[BoardInfo]:
    """Return every non-archived board via ``hermes kanban boards list --json``."""
    result = run_native(
        [cli, "kanban", "boards", "list", "--json"], runner=runner, timeout=timeout
    )
    data = _parse_json_stdout(result, "kanban boards list")
    if not isinstance(data, list):
        raise ReviewGapError("kanban boards list must return a JSON array")
    boards: list[BoardInfo] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        archived = entry.get("archived")
        if archived is True:
            continue
        slug = entry.get("slug")
        if not isinstance(slug, str) or not slug.strip():
            continue
        default_workdir = entry.get("default_workdir")
        boards.append(
            BoardInfo(
                slug=slug,
                default_workdir=(
                    str(default_workdir) if isinstance(default_workdir, str) and default_workdir.strip() else None
                ),
            )
        )
    return boards


def list_done_tasks(
    cli: str, slug: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[dict]:
    """Return every done task on a board via ``list --status done --json``."""
    result = run_native(
        [cli, "kanban", "--board", slug, "list", "--status", "done", "--json"],
        runner=runner,
        timeout=timeout,
    )
    data = _parse_json_stdout(result, f"kanban list --status done on board {slug}")
    if not isinstance(data, list):
        raise ReviewGapError(f"kanban list on board {slug} must return a JSON array")
    return [entry for entry in data if isinstance(entry, dict)]


def list_blocked_tasks(
    cli: str, slug: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[dict]:
    """Return every blocked task on a board via ``list --status blocked --json``."""
    result = run_native(
        [cli, "kanban", "--board", slug, "list", "--status", "blocked", "--json"],
        runner=runner,
        timeout=timeout,
    )
    data = _parse_json_stdout(result, f"kanban list --status blocked on board {slug}")
    if not isinstance(data, list):
        raise ReviewGapError(f"kanban list on board {slug} must return a JSON array")
    return [entry for entry in data if isinstance(entry, dict)]


def list_tasks(
    cli: str, slug: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[dict]:
    """Return every non-archived task on a board via ``list --json``.

    No ``--status`` filter — trigger (d) needs to see open ``re-apply``
    cards in any state (todo/ready/running/blocked) to avoid re-firing.
    """
    result = run_native(
        [cli, "kanban", "--board", slug, "list", "--json"],
        runner=runner,
        timeout=timeout,
    )
    data = _parse_json_stdout(result, f"kanban list on board {slug}")
    if not isinstance(data, list):
        raise ReviewGapError(f"kanban list on board {slug} must return a JSON array")
    return [entry for entry in data if isinstance(entry, dict)]


def show_task(
    cli: str,
    slug: str,
    task_id: str,
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> dict:
    """Return the full ``show <id> --json`` document for one task."""
    result = run_native(
        [cli, "kanban", "--board", slug, "show", task_id, "--json"],
        runner=runner,
        timeout=timeout,
    )
    data = _parse_json_stdout(result, f"kanban show {task_id} on board {slug}")
    if not isinstance(data, dict):
        raise ReviewGapError(f"kanban show {task_id} on board {slug} must return an object")
    return data


# --- candidate filtering ----------------------------------------------------


def is_candidate(
    task: Mapping[str, object],
    *,
    now: int,
    recency_hours: int | float,
    min_age_seconds: int | float,
) -> bool:
    """True when a done task is implementation work inside the eligibility window.

    ``workspace_kind == "worktree"`` (implementation work, not research/docs),
    completed within the recency window, and completed at least
    ``min_age_seconds`` ago so the worker that just completed it has a chance
    to create the review pair itself.
    """
    if task.get("workspace_kind") != "worktree":
        return False
    completed_at = task.get("completed_at")
    if not isinstance(completed_at, int) or isinstance(completed_at, bool):
        return False
    if completed_at <= 0:
        return False  # a missing/never-set completion timestamp is not eligible
    if not isinstance(task.get("id"), str) or not task["id"]:
        return False
    age = int(now) - completed_at
    if age < int(min_age_seconds):
        return False
    return age <= int(recency_hours * 3600)


def _completed_at_key(task: Mapping[str, object]) -> int:
    value = task.get("completed_at")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def candidate_from_task(task: Mapping[str, object], board_slug: str) -> Candidate:
    workspace_path = task.get("workspace_path")
    return Candidate(
        board_slug=board_slug,
        task_id=str(task["id"]),
        title=str(task.get("title") or task["id"]),
        workspace_path=(
            str(workspace_path) if isinstance(workspace_path, str) and workspace_path.strip() else None
        ),
        completed_at=cast(int, task["completed_at"]),
    )


# --- deterministic has-review check -----------------------------------------


def _child_is_review(child_show: Mapping[str, object]) -> bool:
    """Deterministic review check: created-event assignee or reviewer run.

    Explicitly NOT the child's current assignee and NOT title text: a child
    reassigned away from the reviewer still carries its reviewer ``created``
    event and/or a reviewer ``task_run``, while a card merely titled like a
    review but worked by another profile is not a review.
    """
    for event in _iter_events(child_show):
        if not isinstance(event, dict) or event.get("kind") != "created":
            continue
        payload = event.get("payload")
        if isinstance(payload, dict) and payload.get("assignee") == REVIEW_ASSIGNEE:
            return True
    for run in _iter_runs(child_show):
        if isinstance(run, dict) and run.get("profile") == REVIEW_ASSIGNEE:
            return True
    return False


def _iter_events(child_show: Mapping[str, object]) -> Sequence[object]:
    events = child_show.get("events")
    return events if isinstance(events, list) else ()


def _iter_runs(child_show: Mapping[str, object]) -> Sequence[object]:
    runs = child_show.get("runs")
    return runs if isinstance(runs, list) else ()


def review_children(
    cli: str,
    slug: str,
    children_ids: Sequence[str],
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> list[ReviewChild]:
    """Return the children that satisfy the has-review check, with status.

    One ``show`` per child: the CLI returns ``children[]`` as bare task ids,
    so the deterministic check needs each child's ``events`` and ``runs``.
    """
    reviews: list[ReviewChild] = []
    for child_id in children_ids:
        child_show = show_task(cli, slug, child_id, runner=runner, timeout=timeout)
        if not _child_is_review(child_show):
            continue
        task = child_show.get("task")
        status = ""
        if isinstance(task, dict):
            status = str(task.get("status") or "")
        reviews.append(
            ReviewChild(
                task_id=child_id,
                status=status,
                runs=tuple(
                    run for run in _iter_runs(child_show) if isinstance(run, dict)
                ),
            )
        )
    return reviews


# --- trigger c: review-required blocked parents -----------------------------


def latest_blocked_event(show: Mapping[str, object]) -> dict | None:
    """Return the newest ``blocked`` event in a task's event list.

    The event list is in insertion order, so the last ``blocked`` event is the
    one that opened the current episode (a task whose latest transition is
    ``unblocked`` is never listed as blocked).
    """
    latest: dict | None = None
    for event in _iter_events(show):
        if isinstance(event, dict) and event.get("kind") == "blocked":
            latest = event
    return latest


def blocked_episode(
    task: Mapping[str, object],
    board_slug: str,
    show: Mapping[str, object],
) -> BlockedCandidate | None:
    """Return the review-required blocked episode for a blocked task, else None.

    The latest ``blocked`` event must carry a payload whose ``reason`` starts
    with ``review-required:`` (the ``kind`` is ``needs_input`` when present —
    an absent kind is tolerated so older payload shapes still match). Anything
    else — a real question, a dispatcher block, a malformed event — is not a
    handoff and is never auto-completed.
    """
    latest = latest_blocked_event(show)
    if latest is None:
        return None
    payload = latest.get("payload")
    if not isinstance(payload, dict):
        return None
    reason = payload.get("reason")
    if not isinstance(reason, str) or not reason.startswith(REVIEW_REQUIRED_PREFIX):
        return None
    kind = payload.get("kind")
    if kind is not None and kind != "needs_input":
        return None
    blocked_at = latest.get("created_at")
    if not isinstance(blocked_at, int) or isinstance(blocked_at, bool) or blocked_at <= 0:
        return None
    task_id = task.get("id")
    if not isinstance(task_id, str) or not task_id:
        return None
    workspace_path = task.get("workspace_path")
    return BlockedCandidate(
        board_slug=board_slug,
        task_id=task_id,
        title=str(task.get("title") or task_id),
        workspace_path=(
            str(workspace_path) if isinstance(workspace_path, str) and workspace_path.strip() else None
        ),
        blocked_at=blocked_at,
        reason=reason,
    )


def decomposed_child_ids(show: Mapping[str, object]) -> list[str]:
    """Return the impl children created by the auto-decomposer, in order.

    The ``decomposed`` event's ``payload.child_ids`` names the children that
    split up the shipped work after a block loop was detected. Deduped and
    order-preserving; these are the duplicate-impl candidates the operator may
    supersede — the watchdog only alerts about them, it never supersedes.
    """
    ids: list[str] = []
    for event in _iter_events(show):
        if not isinstance(event, dict) or event.get("kind") != "decomposed":
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        child_ids = payload.get("child_ids")
        if not isinstance(child_ids, list):
            continue
        for child_id in child_ids:
            if isinstance(child_id, str) and child_id and child_id not in ids:
                ids.append(child_id)
    return ids


# --- repo-root derivation and merge checks ----------------------------------


def repo_root_for(
    workspace_path: str | None, default_workdir: str | None
) -> str | None:
    """Derive the project repo root from a worktree task's workspace path.

    ``<repo>/.worktrees/<task_id>`` collapses to ``<repo>``; a plain path is
    used as-is; the board ``default_workdir`` is the fallback when no
    workspace path is recorded.
    """
    if workspace_path:
        path = Path(workspace_path)
        if path.parent.name == ".worktrees":
            return str(path.parent.parent)
        return str(path)
    return default_workdir


def default_branch(
    repo_root: Path, *, runner: CliRunner | None = None, timeout: float | None = None
) -> str | None:
    """Return ``main`` or ``master`` (whichever ref exists), else ``None``."""
    for candidate in DEFAULT_BRANCH_CANDIDATES:
        result = run_git(
            repo_root,
            ["show-ref", "--verify", f"refs/heads/{candidate}"],
            runner=runner,
            timeout=timeout,
        )
        if result.returncode == 0:
            return candidate
    return None


def branch_is_merged(
    repo_root: Path,
    task_id: str,
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> bool:
    """True when ``wt/<task_id>`` is merged into ``main``/``master``.

    A missing branch counts as merged (there is nothing left to merge); a
    missing ``main``/``master`` ref also counts as merged (nothing to merge
    into, so never a false stall alert).
    """
    branch = f"wt/{task_id}"
    exists = run_git(
        repo_root,
        ["show-ref", "--verify", f"refs/heads/{branch}"],
        runner=runner,
        timeout=timeout,
    )
    if exists.returncode != 0:
        return True
    default = default_branch(repo_root, runner=runner, timeout=timeout)
    if default is None:
        return True
    result = run_git(
        repo_root, ["merge-base", "--is-ancestor", branch, default], runner=runner, timeout=timeout
    )
    return result.returncode == 0


def shipped_branch_evidence(
    repo_root: Path,
    task_id: str,
    *,
    branch_name: str | None = None,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> str | None:
    """Return the shipped commit sha when the task's branch exists and is unmerged.

    Trigger (c) never auto-completes a parent without positive shipped-commit
    evidence, so a missing branch, a missing ``main``/``master`` ref, or an
    already-merged branch all yield ``None`` (nothing verifiably awaiting
    review). The sha is the branch HEAD from ``git rev-parse``.

    The task's recorded ``branch_name`` (project-linked worktrees branch
    semantically, e.g. ``wt/multilocation``) is checked FIRST; ``wt/<task_id>``
    remains the fallback for tasks that follow the default convention. A
    ``branch_name`` that is missing, empty, or already in the default
    ``wt/<task_id>`` shape is skipped so the fallback stays authoritative.
    """
    candidates = [branch_name.strip()] if branch_name and branch_name.strip() else []
    default_convention = f"wt/{task_id}"
    if default_convention not in candidates:
        candidates.append(default_convention)
    default = default_branch(repo_root, runner=runner, timeout=timeout)
    if default is None:
        return None
    for branch in candidates:
        exists = run_git(
            repo_root,
            ["show-ref", "--verify", f"refs/heads/{branch}"],
            runner=runner,
            timeout=timeout,
        )
        if exists.returncode != 0:
            continue
        merged = run_git(
            repo_root,
            ["merge-base", "--is-ancestor", branch, default],
            runner=runner,
            timeout=timeout,
        )
        if merged.returncode == 0:
            continue
        rev = run_git(repo_root, ["rev-parse", branch], runner=runner, timeout=timeout)
        if rev.returncode != 0:
            continue
        sha = rev.stdout.strip()
        if not sha:
            continue
        return sha
    return None


# --- stalled-merge detection: done reviews verified by git truth ------------
#
# A done review card is NOT trusted as "merged": the deferred-merge loose end
# (observed on a live board, 2026-08-06) is exactly a review that
# completed with approved:false/merge_sha:null while its branch sat unmerged
# on origin. "Healthy" requires git truth: the impl commit must be an
# ancestor of either canonical branch (origin/<default> first, then local
# <default>). A merge only on local main is healthy even when origin/main is
# stale because pushes are operator-controlled/batched. The commit resolves
# from the review child's completion metadata (implementation_commit /
# merge_sha), then the wt/<task_id> branch HEAD (or the task's recorded
# branch_name for project-linked worktrees). An unresolvable commit (no
# metadata, branch pruned after merge) reads healthy — a merged-then-deleted
# branch must never false-flag. The impl's own metadata is never used: it
# records the pre-review tip, which a reviewer rebase rewrites (observed
# 2026-08-06: t_3595aa64's recorded 17eecf28 was rebased+merged as
# 2fbfd893 — reading it would flag already-merged work).


def _completion_metadata_value(
    runs: Sequence[object], key: str
) -> str | None:
    """Return the last non-empty ``key`` from run completion metadata."""
    found: str | None = None
    for run in runs:
        if not isinstance(run, dict):
            continue
        metadata = run.get("metadata")
        if not isinstance(metadata, dict):
            continue
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            found = value.strip()
    return found


def _completion_metadata_bool(
    runs: Sequence[object], key: str
) -> bool | None:
    """Return the last explicit boolean ``key`` from run metadata."""
    found: bool | None = None
    for run in runs:
        if not isinstance(run, dict):
            continue
        metadata = run.get("metadata")
        if not isinstance(metadata, dict):
            continue
        value = metadata.get(key)
        if isinstance(value, bool):
            found = value
    return found


def _review_was_rejected(reviews: Sequence[ReviewChild]) -> bool:
    """Return true when a terminal review explicitly rejected the impl.

    An explicit ``approved: false`` is a deliberate review outcome: the
    implementation branch must not be merged, so trigger b must not treat its
    unmerged commit as a stalled merge. Missing or malformed approval metadata
    remains on the existing git-truth path.
    """
    return any(
        _completion_metadata_bool(review.runs, "approved") is False
        for review in reviews
    )


def resolve_impl_commit(
    repo_root: Path,
    task_id: str,
    impl_show: Mapping[str, object],
    reviews: Sequence[ReviewChild],
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> str | None:
    """Resolve the shipped commit for an impl task, else ``None``.

    Order: the review child's completion metadata (``implementation_commit``
    — the commit the reviewer validated/merged — then ``merge_sha``, the
    merged commit the merge contract records), then the ``wt/<task_id>``
    branch HEAD, then the impl task's recorded ``branch_name`` (project-linked
    worktrees branch as ``<project>/<task_id>-<slug>``, not ``wt/``). The
    impl's OWN completion metadata is deliberately NOT consulted: it records
    the pre-review tip, which a reviewer rebase rewrites, so it reads stale
    on merged work (false positive). ``None`` means the commit is
    unresolvable; the caller never flags on that alone — a
    merged-then-deleted branch must read healthy.
    """
    for review in reviews:
        for key in ("implementation_commit", "merge_sha"):
            commit = _completion_metadata_value(review.runs, key)
            if commit and _FULL_SHA_RE.fullmatch(commit):
                return commit
    for ref in _impl_branch_refs(task_id, impl_show):
        result = run_git(repo_root, ["rev-parse", ref], runner=runner, timeout=timeout)
        if result.returncode == 0:
            sha = result.stdout.strip()
            if sha:
                return sha
    return None


def _impl_branch_refs(
    task_id: str, impl_show: Mapping[str, object]
) -> list[str]:
    """Branch refs to try for the impl commit, in order.

    ``wt/<task_id>`` first (the worktree convention), then the impl task's
    recorded ``branch_name`` — project-linked worktrees branch as
    ``<project>/<task_id>-<slug>``, never ``wt/<task_id>`` (observed
    2026-08-06: hkrc t_3595aa64 shipped on
    ``hkrc/t_3595aa64-fix-review-gap-...``).
    """
    refs = [f"wt/{task_id}"]
    task = impl_show.get("task")
    if isinstance(task, dict):
        branch_name = task.get("branch_name")
        if (
            isinstance(branch_name, str)
            and branch_name.strip()
            and not branch_name.startswith("wt/")
        ):
            refs.append(branch_name.strip())
    return refs


def commit_is_merged(
    repo_root: Path,
    commit: str,
    *,
    runner: CliRunner | None = None,
    timeout: float | None = None,
) -> bool:
    """True when ``commit`` is an ancestor of the canonical branch.

    Check both canonical refs, preferring ``origin/<default>`` but also
    accepting local ``<default>``. A merge only on local main is healthy even
    when origin/main is stale because pushes are operator-controlled/batched.
    If neither ref contains the commit, return False. A repo with no default
    branch resolves to False — the watchdog flags rather than goes silent.
    """
    default = default_branch(repo_root, runner=runner, timeout=timeout)
    if default is None:
        return False
    for ref in (f"refs/remotes/origin/{default}", f"refs/heads/{default}"):
        exists = run_git(
            repo_root, ["show-ref", "--verify", ref], runner=runner, timeout=timeout
        )
        if exists.returncode != 0:
            continue
        ancestor = run_git(
            repo_root,
            ["merge-base", "--is-ancestor", commit, ref],
            runner=runner,
            timeout=timeout,
        )
        if ancestor.returncode == 0:
            return True
    return False


# --- review card construction -----------------------------------------------


def build_review_body(task_id: str, title: str) -> str:
    """Render the review card body — the merge contract lives in the body."""
    return (
        f"Validate implementation task {task_id} ({title}) as one unit.\n\n"
        f"Source of truth: the code lives on branch `wt/{task_id}` — NOT on "
        f"main, do NOT rebase onto main.\n\n"
        "Acceptance:\n"
        "- run the repo tests and lint clean;\n"
        "- verify in a live browser where applicable.\n\n"
        f"On approval: merge into main with `git merge --no-ff` from "
        f"`wt/{task_id}`, a conventional commit message referencing this "
        f"review task and impl task {task_id}, run the repo gate on merged "
        "main, and record `merge_sha` in completion metadata."
    )


def build_revalidation_body(
    task_id: str,
    title: str,
    commit: str | None,
    default_branch_name: str | None,
) -> str:
    """Render the re-validation card body — source branch + merge contract.

    The previous review card completed WITHOUT the merge landing: the impl
    commit is not an ancestor of the canonical branch, so nothing else will
    merge this work. The body names the source-of-truth branch and commit
    sha, states the code is NOT on main, and pins the rebase-onto-origin
    merge contract.
    """
    target = f"origin/{default_branch_name}" if default_branch_name else "main"
    source = f"branch `wt/{task_id}`"
    if commit:
        source = f"branch `wt/{task_id}` at commit {commit}"
    return (
        f"Review the completed review for implementation task {task_id} "
        f"({title}): the paired review card completed WITHOUT the merge "
        "landing on the canonical branch — the impl commit is not on main. "
        "Validate and merge it.\n\n"
        f"Source of truth: the code lives on {source} — code is NOT on main, "
        "do NOT rebase onto main.\n\n"
        "Acceptance:\n"
        "- run the repo tests and lint clean;\n"
        "- verify in a live browser where applicable.\n\n"
        "On approval: rebase onto "
        f"{target}, run the repo gate on base, merge into main with `git merge "
        f"--no-ff` from `wt/{task_id}`, a conventional commit message "
        f"referencing this review task and impl task {task_id}, run the repo "
        "gate on merged main, and record `merge_sha` in completion metadata."
    )


def has_open_revalidation_card(
    tasks: Sequence[Mapping[str, object]], task_id: str
) -> bool:
    """True when an OPEN re-validation card for ``task_id`` exists.

    The re-validation cards this module creates are titled ``review: validate
    <title> (<task_id>)`` — the same shape as trigger (a)'s review cards. An
    open card means the repair is already in flight (the dedupe state may have
    been lost); terminal cards do NOT suppress — a done re-validation card
    whose merge still never landed is the failure being repaired and must
    re-fire.
    """
    for task in tasks:
        title = task.get("title")
        if not isinstance(title, str):
            continue
        if title.startswith(REVALIDATION_TITLE_PREFIX) and f"({task_id})" in title:
            status = task.get("status")
            if isinstance(status, str) and status in _TERMINAL_STATUSES:
                continue
            return True
    return False


def build_create_command(
    cli: str,
    slug: str,
    title: str,
    task_id: str,
    repo_root: str,
    body: str,
) -> list[str]:
    """Return the exact argv for one review-card creation."""
    return [
        cli,
        "kanban",
        "--board",
        slug,
        "create",
        f"review: validate {title} ({task_id})",
        "--assignee",
        REVIEW_ASSIGNEE,
        "--parent",
        task_id,
        "--workspace",
        f"worktree:{repo_root}",
        "--priority",
        str(REVIEW_PRIORITY),
        "--body",
        body,
        "--json",
    ]


def parse_created_task_id(stdout: str, *, board_slug: str, task_id: str) -> str:
    """Extract the created card id from ``create --json`` stdout."""
    try:
        task = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ReviewGapError(
            f"create review card for {task_id} on board {board_slug} "
            f"returned unparseable JSON: {exc}"
        ) from exc
    if not isinstance(task, dict) or not isinstance(task.get("id"), str):
        raise ReviewGapError(
            f"create review card for {task_id} on board {board_slug} "
            "did not return a task object with an id"
        )
    return task["id"]


# --- trigger d: revert-drift detection --------------------------------------


def revert_episodes(
    log_lines: Sequence[str],
) -> list[tuple[str, str, str, str]]:
    """Parse ``git log --format=%H%x09%P%x09%s`` output for reverted merges.

    Returns ``(revert_sha, merge_sha, task_id, subject)`` tuples in log
    order. ``merge_sha`` is the second parent of the revert commit (the
    original merge it undoes); for a revert of a non-merge commit the single
    parent is the reverted commit itself. Lines that don't match the revert
    pattern are ignored.
    """
    episodes: list[tuple[str, str, str, str]] = []
    for line in log_lines:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue
        revert_sha, parents, subject = parts[0], parts[1], parts[2]
        match = REVERT_SUBJECT_RE.match(subject)
        if match is None:
            continue
        parent_shas = [p for p in parents.split() if p]
        merge_sha = parent_shas[1] if len(parent_shas) >= 2 else (
            parent_shas[0] if parent_shas else ""
        )
        episodes.append((revert_sha, merge_sha, match.group(1), subject))
    return episodes


def has_reapply_card(tasks: Sequence[Mapping[str, object]], task_id: str) -> bool:
    """True when an OPEN ``re-apply reverted change`` card for ``task_id`` exists.

    Matches our own deterministic title prefix plus the task id in the
    title — the fragile title-parsing rule that governs review detection
    does not apply to cards this module itself created. Terminal cards
    (done/archived) do NOT suppress: a completed re-apply card whose drift
    is unhealed is a false-complete and the episode must re-fire.
    """
    for task in tasks:
        title = task.get("title")
        if not isinstance(title, str):
            continue
        if title.lower().startswith(REAPPLY_TITLE_PREFIX) and task_id in title:
            status = task.get("status")
            if isinstance(status, str) and status in _TERMINAL_STATUSES:
                continue
            return True
    return False


def _remerged_subjects_after(log_stdout: str, task_id: str) -> bool:
    """True when any post-revert commit subject carries the kanban merge marker.

    Only the canonical ``(kanban t_xxx)`` marker (used by every merge in
    this repo) or the re-apply title form counts as a re-merge — a commit
    that merely mentions the task id (e.g. a changelog or an incident
    reference in a commit message) must not suppress the drift card.
    """
    for line in log_stdout.splitlines():
        if not line.strip():
            continue
        subject = line.split("\t")[-1]
        if f"(kanban {task_id})" in subject:
            return True
        if f"{REAPPLY_TITLE_PREFIX} ({task_id})" in subject:
            return True
    return False


def _reapply_card_verified_healed(
    cli: str,
    board_slug: str,
    card: Mapping[str, object],
    *,
    runner: CliRunner | None,
    timeout: float | None = None,
) -> bool:
    """True when a terminal re-apply card's completion text verifies the heal.

    The re-apply body contract tells the worker to state ``nothing to merge
    (branch == main)`` ONLY after verifying the reverted content is present
    on main — the supersession terminal state. The marker may live in the
    card's ``result`` (exposed by ``list --json``) or, when the worker
    completed with a summary only, in the run summary surfaced by
    ``show --json`` as ``latest_summary``. The match is case-insensitive;
    this helper only ever examines terminal re-apply cards for the task at
    hand, so incidental mentions elsewhere cannot suppress a drift card.
    """
    for field in ("result", "latest_summary"):
        value = card.get(field)
        if isinstance(value, str) and _NOTHING_TO_MERGE_RE.search(value):
            return True
    task_id = card.get("id")
    result = card.get("result")
    if (
        isinstance(task_id, str)
        and task_id
        and not (isinstance(result, str) and result.strip())
    ):
        summary = show_task(cli, board_slug, task_id, runner=runner, timeout=timeout).get(
            "latest_summary"
        )
        if isinstance(summary, str) and _NOTHING_TO_MERGE_RE.search(summary):
            return True
    return False


def build_reapply_body(task_id: str, revert_sha: str, merge_sha: str, subject: str) -> str:
    """Render the re-apply card body — revert evidence + re-merge contract."""
    return (
        f"Original merge for {task_id} was reverted on {revert_sha}: {subject}\n\n"
        "Why: the merged-main gate failed (typically pre-existing findings on "
        "base main) and the merge was rolled back; the change then sat off "
        "main silently because git still considers the branch merged. This "
        "card re-applies it once the gate is green.\n\n"
        "Re-apply steps:\n"
        "- cherry-pick the reverted change onto a fresh branch off current "
        f"main (source: merge {merge_sha}, or the change behind it);\n"
        "- run the repo gate on base main FIRST — if base is red, block with "
        "evidence and file a cleanup card, never merge onto a red base;\n"
        f"- merge with `git merge --no-ff`, conventional message referencing "
        f"{task_id} and this card;\n"
        "- run the repo gate on merged main and record `merge_sha` in "
        "completion metadata."
    )


def build_reapply_create_command(
    cli: str,
    slug: str,
    task_id: str,
    repo_root: str,
    body: str,
) -> list[str]:
    """Return the exact argv for one re-apply card creation."""
    return [
        cli,
        "kanban",
        "--board",
        slug,
        "create",
        f"{REAPPLY_TITLE_PREFIX} ({task_id})",
        "--assignee",
        REVIEW_ASSIGNEE,
        "--parent",
        task_id,
        "--workspace",
        f"worktree:{repo_root}",
        "--priority",
        str(REVIEW_PRIORITY),
        "--body",
        body,
        "--json",
    ]


# --- trigger c: handoff completion construction -----------------------------


def build_handoff_summary(task_id: str, review_id: str, sha: str) -> str:
    """Render the completion summary for a review-required handoff.

    Completing the parent promotes the review child; the reviewer still gates
    the merge, so the summary states exactly that contract.
    """
    return (
        f"review-required handoff: work shipped on wt/{task_id} (commit {sha}); "
        f"review child {review_id} is the gate \u2014 completing parent to promote review"
    )


def build_complete_command(
    cli: str,
    slug: str,
    task_id: str,
    summary: str,
    metadata: Mapping[str, object],
) -> list[str]:
    """Return the exact argv for one review-required parent completion."""
    return [
        cli,
        "kanban",
        "--board",
        slug,
        "complete",
        task_id,
        "--summary",
        summary,
        "--metadata",
        json.dumps(dict(metadata), sort_keys=True),
    ]


# --- state (dedupe) ---------------------------------------------------------


def _load_state(path: Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReviewGapError(f"cannot read review-gap state {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ReviewGapError(f"review-gap state must be an object: {path}")
    state: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise ReviewGapError(
                f"review-gap state values must be objects: {path} (key {key!r})"
            )
        action = value.get("action")
        at = value.get("at")
        if not isinstance(action, str) or action.split(":", 1)[0] not in _STATE_ACTIONS:
            raise ReviewGapError(
                f"review-gap state value has an invalid action: {path} (key {key!r})"
            )
        if not isinstance(at, int) or isinstance(at, bool):
            raise ReviewGapError(
                f"review-gap state value has an invalid timestamp: {path} (key {key!r})"
            )
        state[str(key)] = {"action": action, "at": at}
    return state


def _save_state(path: Path, state: Mapping[str, dict[str, object]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(dict(state), sort_keys=True, indent=1) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    except OSError as exc:
        raise ReviewGapError(f"cannot persist review-gap state {path}: {exc}") from exc


# --- digest rendering -------------------------------------------------------


def created_line(created_id: str, candidate: Candidate) -> str:
    return f"created review card {created_id} for {candidate.task_id} on board {candidate.board_slug}"


def reapply_line(created_id: str, task_id: str, board_slug: str) -> str:
    return f"created re-apply card {created_id} for {task_id} on board {board_slug}"


def unroutable_line(task_id: str, repo_root: str, board_slug: str) -> str:
    return (
        f"revert-drift unactionable: merge for {task_id} was reverted in repo "
        f"{repo_root} but the parent task is not on board {board_slug} — "
        "route a re-apply card manually"
    )


def false_complete_line(card_id: str, task_id: str, board_slug: str) -> str:
    return (
        f"revert-drift false-complete: re-apply card {card_id} completed but "
        f"the reverted change for {task_id} is not on main (board {board_slug}) "
        "— re-created the re-apply card"
    )


def healed_line(card_id: str, task_id: str, board_slug: str) -> str:
    return (
        f"revert-drift healed: re-apply card {card_id} for {task_id} "
        f"verified content on main (board {board_slug}) — no re-fire"
    )


def stall_line(candidate: Candidate, pending: Sequence[ReviewChild], now: int) -> str:
    hours = max(0, (int(now) - candidate.completed_at) // 3600)
    review_ids = ", ".join(child.task_id for child in pending)
    return (
        f"stalled review for {candidate.task_id} on board {candidate.board_slug} "
        f"(review {review_ids} not done, branch wt/{candidate.task_id} unmerged for {hours}h)"
    )


def stale_merge_line(
    candidate: Candidate, reviews: Sequence[ReviewChild], now: int
) -> str:
    hours = max(0, (int(now) - candidate.completed_at) // 3600)
    review_ids = ", ".join(child.task_id for child in reviews)
    return (
        f"stalled merge for {candidate.task_id} on board {candidate.board_slug} "
        f"(review {review_ids} done, commit not on main for {hours}h)"
    )


def gap_line(candidate: Candidate) -> str:
    return f"review-gap missing review card for {candidate.task_id} on board {candidate.board_slug}"


def error_line(candidate: Candidate) -> str:
    return (
        f"review-gap error: cannot derive repo root for {candidate.task_id} "
        f"on board {candidate.board_slug}"
    )


def completed_line(candidate: BlockedCandidate, review_id: str, sha: str) -> str:
    return (
        f"completed review-required parent {candidate.task_id} on board "
        f"{candidate.board_slug} (review child {review_id}, branch "
        f"wt/{candidate.task_id}, commit {sha})"
    )


def duplicate_alert_line(candidate: BlockedCandidate, duplicates: Sequence[str]) -> str:
    ids = ", ".join(duplicates)
    return (
        f"review-gap duplicate-impl alert: {candidate.task_id} on board "
        f"{candidate.board_slug} was auto-decomposed after its review-required "
        f"block; duplicate impl children: {ids} (operator decides supersede)"
    )


def timeout_line(slug: str, detail: str) -> str:
    """One alert line when a board/pass was skipped because a subprocess hung."""
    return f"review-gap timeout: board {slug} skipped ({detail})"


def budget_line(skipped: int) -> str:
    """One alert line when the whole-tick budget expired before all boards ran."""
    return (
        f"review-gap timeout: tick budget exceeded; skipped {skipped} "
        "remaining board(s)"
    )


# --- main entry -------------------------------------------------------------


def run(
    config: ControllerConfig,
    state_path: Path,
    *,
    now: int | None = None,
    runner: CliRunner | None = None,
    git_runner: CliRunner | None = None,
) -> str:
    """Scan done worktree tasks and review-required blocked parents.

    Per non-archived board: the done-task pass (triggers a/b: close review
    gaps, alert on stalled reviews), the blocked-task pass when
    ``review_gap.trigger_c_enabled`` (trigger c: auto-complete a
    ``review-required:`` parent whose work shipped on an unmerged
    ``wt/<task_id>`` branch and whose review child already exists —
    completing the parent promotes the review child; the reviewer still gates
    the merge), and the revert-drift pass when ``review_gap.trigger_d_enabled``.

    Bounded time (0.13.1): the tick is read-bound — every kanban read is one
    ``hermes kanban`` CLI subprocess, and per-candidate reads (show + child
    shows + git checks) are parallelized across ``review_gap.max_workers``
    workers, while mutations (create/complete) stay sequential and
    deterministic. Every subprocess is capped by
    ``review_gap.cli_timeout_seconds`` (a hung call raises
    ``NativeTimeoutError`` and skips only the affected board/pass with an
    alert line), and the whole tick is capped by
    ``review_gap.tick_timeout_seconds`` (remaining boards are skipped with an
    alert line; the dedupe state carries progress to the next tick).

    All kanban reads and writes go through the native CLI; the only other
    side effect is the atomic dedupe state file and (on gap) one review-card
    creation. Empty digest = silent. ``runner``/``git_runner`` exist only for
    deterministic tests; production uses ``subprocess.run`` argv lists, never
    a shell command.
    """
    if not config.review_gap.enabled:
        return ""
    current_time = int(time.time()) if now is None else int(now)
    state = _load_state(state_path)
    cli = config.native_cli
    cfg = config.review_gap
    lines: list[str] = []
    timeout = float(cfg.cli_timeout_seconds)
    boards = discover_boards(cli, runner=runner, timeout=timeout)
    deadline = time.monotonic() + float(cfg.tick_timeout_seconds)
    board_lists = _fetch_board_lists(cli, boards, cfg, runner=runner)
    list_cache: dict[str, list[dict]] = {}
    for index, board in enumerate(boards):
        if time.monotonic() >= deadline:
            lines.append(budget_line(len(boards) - index))
            break
        done_tasks, blocked_tasks = board_lists.get(board.slug, (None, None))
        if done_tasks is None:
            # The board's list phase hung past cli_timeout_seconds — skip the
            # whole board (alert once) so it cannot stall the rest of the tick.
            lines.append(timeout_line(board.slug, "list phase timed out"))
            continue
        board_done: list[dict] = done_tasks
        board_blocked: list[dict] = blocked_tasks or []

        def guarded(phase: Callable[[], None]) -> None:
            try:
                phase()
            except NativeTimeoutError as exc:
                lines.append(timeout_line(board.slug, str(exc)))

        guarded(
            lambda: _process_done_tasks(
                cli, board, board_done, state, lines, current_time, cfg,
                runner=runner, git_runner=git_runner, deadline=deadline,
            )
        )
        if cfg.trigger_c_enabled:
            guarded(
                lambda: _process_blocked_tasks(
                    cli, board, board_blocked, state, lines, current_time,
                    cfg, runner=runner, git_runner=git_runner, deadline=deadline,
                )
            )
        if cfg.trigger_d_enabled:
            guarded(
                lambda: _process_revert_drifts(
                    cli, board, boards, list_cache, state, lines, current_time,
                    cfg, runner=runner, git_runner=git_runner, deadline=deadline,
                )
            )
    _save_state(state_path, state)
    return "\n".join(lines)


def _fetch_board_lists(
    cli: str,
    boards: Sequence[BoardInfo],
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
) -> dict[str, tuple[list[dict] | None, list[dict] | None]]:
    """Fetch every board's done + blocked lists, one parallel unit per board.

    Boards have independent kanban DBs, so the fetches run concurrently; a
    board whose fetch hangs past ``cli_timeout_seconds`` yields ``(None,
    None)`` (the caller emits one timeout alert line and skips the board)
    while every other board's lists still load.
    """
    timeout = float(cfg.cli_timeout_seconds)
    results: dict[str, tuple[list[dict] | None, list[dict] | None]] = {}
    if len(boards) <= 1 or cfg.max_workers <= 1:
        for board in boards:
            try:
                done = list_done_tasks(cli, board.slug, runner=runner, timeout=timeout)
                blocked = list_blocked_tasks(cli, board.slug, runner=runner, timeout=timeout)
            except NativeTimeoutError:
                results[board.slug] = (None, None)
            else:
                results[board.slug] = (done, blocked)
        return results

    def fetch(board: BoardInfo) -> tuple[BoardInfo, list[dict], list[dict]]:
        done = list_done_tasks(cli, board.slug, runner=runner, timeout=timeout)
        blocked = list_blocked_tasks(cli, board.slug, runner=runner, timeout=timeout)
        return board, done, blocked

    with ThreadPoolExecutor(max_workers=min(cfg.max_workers, len(boards))) as pool:
        futures = [pool.submit(fetch, board) for board in boards]
        for board, future in zip(boards, futures):
            try:
                _, done, blocked = future.result()
            except NativeTimeoutError:
                results[board.slug] = (None, None)
            else:
                results[board.slug] = (done, blocked)
    return results


def _review_ok_expired(
    entry: Mapping[str, object],
    now: int,
    stalled_alert_hours: int | float,
) -> bool:
    """True when a ``review-ok`` confirm should be re-examined.

    A confirm records ``at`` = when the candidate was verified to have a
    (non-stalled) review child. It is skipped until ``at + stalled_alert_hours``
    so the stall alert still fires on schedule; any other recorded action is
    never re-examined (existing dedupe semantics). Corrupt entries fail
    closed by never expiring — the state loader already rejected them.
    """
    if entry.get("action") != "review-ok":
        return False
    at = entry.get("at")
    if not isinstance(at, int) or isinstance(at, bool):
        return False
    return int(now) >= at + int(stalled_alert_hours * 3600)


def _process_done_tasks(
    cli: str,
    board: BoardInfo,
    done_tasks: Sequence[Mapping[str, object]],
    state: dict[str, dict[str, object]],
    lines: list[str],
    current_time: int,
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
    deadline: float | None = None,
) -> None:
    """Trigger a/b pass: close review gaps for done worktree tasks.

    Reads (candidate show + child shows + git checks) run in parallel across
    ``max_workers``; actions (review-card creation) run sequentially in
    candidate order so the digest, dedupe state, and create order stay
    deterministic. Candidates past the ``deadline`` are left for the next
    tick. A candidate whose last examination confirmed a healthy review
    child (``review-ok``) is skipped until its confirmation expires, so the
    steady-state tick re-shows only expired confirmations and new candidates.
    """
    candidates: list[Candidate] = []
    for task in sorted(done_tasks, key=_completed_at_key):
        if not is_candidate(
            task,
            now=current_time,
            recency_hours=cfg.recency_hours,
            min_age_seconds=cfg.min_age_seconds,
        ):
            continue
        candidate = candidate_from_task(task, board.slug)
        entry = state.get(candidate.key)
        if entry is not None and not _review_ok_expired(
            entry, current_time, cfg.stalled_alert_hours
        ):
            continue  # this episode was already acted on (or confirmed healthy)
        candidates.append(candidate)
    if not candidates:
        return
    plans = _plan_done_candidates(
        cli, board, candidates, cfg, current_time,
        runner=runner, git_runner=git_runner, deadline=deadline,
    )
    _act_done_plans(cli, board, plans, state, lines, current_time, cfg, runner=runner)


@dataclass(frozen=True, slots=True)
class _DonePlan:
    """Read-phase result for one done candidate; acted on sequentially."""

    candidate: Candidate
    action: str  # "none" | "create" | "stall-alert" | "gap-alert" | "error:no-repo-root" | "review-ok"
    line: str | None = None
    command: list[str] | None = None


def _plan_done_candidates(
    cli: str,
    board: BoardInfo,
    candidates: Sequence[Candidate],
    cfg: ReviewGapConfig,
    current_time: int,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
    deadline: float | None = None,
) -> list[_DonePlan | None]:
    """Plan every done candidate in parallel; ``None`` = skipped by deadline."""
    if cfg.max_workers <= 1:
        return [
            _plan_done_candidate(cli, board, candidate, cfg, current_time, runner=runner, git_runner=git_runner)
            for candidate in candidates
        ]
    plans: list[_DonePlan | None] = [None] * len(candidates)
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
        submitted: list[tuple[int, Future[_DonePlan]]] = []
        for index, candidate in enumerate(candidates):
            if deadline is not None and time.monotonic() >= deadline:
                break  # budget exhausted — remaining candidates wait for the next tick
            future = pool.submit(
                _plan_done_candidate,
                cli=cli,
                board=board,
                candidate=candidate,
                cfg=cfg,
                current_time=current_time,
                runner=runner,
                git_runner=git_runner,
            )
            submitted.append((index, future))
        for index, future in submitted:
            plans[index] = future.result()
    return plans


def _plan_done_candidate(
    cli: str,
    board: BoardInfo,
    candidate: Candidate,
    cfg: ReviewGapConfig,
    current_time: int,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
) -> _DonePlan:
    """Read-phase for one candidate: show, child shows, git checks (no writes)."""
    timeout = float(cfg.cli_timeout_seconds)
    show = show_task(cli, board.slug, candidate.task_id, runner=runner, timeout=timeout)
    if _child_is_review(show):
        # The task is itself a review card (created by the reviewer profile
        # or worked by a reviewer run) — review cards do not get review
        # children, so never create a review-of-review. Confirm it like any
        # other healthy candidate: the confirm expires at stalled_alert_hours,
        # so a pathological reassignment away from the reviewer still re-opens
        # the candidate within one expiry window.
        return _DonePlan(candidate, "review-ok")
    children_ids = [
        str(child)
        for child in (show.get("children") or [])
        if isinstance(child, str) and child
    ]
    reviews = review_children(cli, board.slug, children_ids, runner=runner, timeout=timeout)
    repo_root = repo_root_for(candidate.workspace_path, board.default_workdir)
    if reviews:
        pending = [child for child in reviews if child.pending]
        stalled = (
            bool(pending)
            and repo_root is not None
            and (current_time - candidate.completed_at)
            >= int(cfg.stalled_alert_hours * 3600)
            and not branch_is_merged(Path(repo_root), candidate.task_id, runner=git_runner, timeout=timeout)
        )
        if stalled:
            return _DonePlan(
                candidate,
                "stall-alert",
                line=stall_line(candidate, pending, current_time),
            )
        if pending:
            # A healthy review child exists (pending but not yet stalled, or the
            # branch already merged): confirm so the next ticks skip this
            # candidate until the confirmation expires at stalled_alert_hours.
            return _DonePlan(candidate, "review-ok")
        if _review_was_rejected(reviews):
            # A rejected review intentionally leaves the implementation branch
            # unmerged. It is not a stalled merge and must not be revalidated.
            return _DonePlan(candidate, "review-ok")
        # Every review child is terminal. "done" is trusted as "merged" only
        # when git truth confirms it: the impl commit must be an ancestor of
        # the canonical branch. A done review whose commit is NOT on the
        # canonical branch is a stalled merge — the deferred-merge loose end
        # (DEF-001: review completed with approved:false/merge_sha:null while
        # the branch sits unmerged on origin). This is exactly the failure the
        # has-review dedupe must NOT skip: the done review child IS the
        # failure, not a healthy gate.
        if repo_root is None:
            return _DonePlan(candidate, "review-ok")
        commit = resolve_impl_commit(
            Path(repo_root), candidate.task_id, show, reviews,
            runner=git_runner, timeout=timeout,
        )
        if commit is None or commit_is_merged(
            Path(repo_root), commit, runner=git_runner, timeout=timeout
        ):
            # Merged — or unresolvable: a merged-then-deleted branch has no
            # commit to check, which reads healthy (the old missing-branch
            # rule), never a phantom re-validation card.
            return _DonePlan(candidate, "review-ok")
        if (current_time - candidate.completed_at) < int(
            cfg.stalled_alert_hours * 3600
        ):
            # Not yet past the stall window: confirm silently; the confirm
            # expires at stalled_alert_hours and the merge check re-fires on
            # schedule.
            return _DonePlan(candidate, "review-ok")
        if not cfg.auto_create:
            return _DonePlan(
                candidate,
                "stall-alert",
                line=stale_merge_line(candidate, reviews, current_time),
            )
        listed = list_tasks(cli, board.slug, runner=runner, timeout=timeout)
        if has_open_revalidation_card(listed, candidate.task_id):
            # An open re-validation card for this impl already exists (created
            # before the dedupe state was lost, or by hand): the in-flight
            # card IS the repair — never create a duplicate.
            return _DonePlan(candidate, "review-ok")
        default = default_branch(Path(repo_root), runner=git_runner, timeout=timeout)
        body = build_revalidation_body(
            candidate.task_id, candidate.title, commit, default
        )
        command = build_create_command(
            cli, board.slug, candidate.title, candidate.task_id, repo_root, body
        )
        return _DonePlan(candidate, "create", command=command)
    if not cfg.auto_create:
        return _DonePlan(candidate, "gap-alert", line=gap_line(candidate))
    if repo_root is None:
        return _DonePlan(candidate, "error:no-repo-root", line=error_line(candidate))
    body = build_review_body(candidate.task_id, candidate.title)
    command = build_create_command(
        cli, board.slug, candidate.title, candidate.task_id, repo_root, body
    )
    return _DonePlan(candidate, "create", command=command)


def _act_done_plans(
    cli: str,
    board: BoardInfo,
    plans: Sequence[_DonePlan | None],
    state: dict[str, dict[str, object]],
    lines: list[str],
    current_time: int,
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
) -> None:
    """Sequential action phase: run creates, emit lines, record dedupe state."""
    timeout = float(cfg.cli_timeout_seconds)
    for plan in plans:
        if plan is None or plan.action == "none":
            continue
        key = plan.candidate.key
        if plan.action == "create":
            command = plan.command
            assert command is not None
            result = run_native(command, runner=runner, timeout=timeout)
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()
                raise ReviewGapError(
                    f"create review card for {plan.candidate.task_id} on board "
                    f"{board.slug} failed (exit {result.returncode}): {detail}"
                )
            created_id = parse_created_task_id(
                result.stdout, board_slug=board.slug, task_id=plan.candidate.task_id
            )
            lines.append(created_line(created_id, plan.candidate))
            state[key] = {"action": f"created:{created_id}", "at": current_time}
            continue
        if plan.action == "review-ok":
            # Silent confirm: the candidate has a healthy review child and is
            # skipped until the confirmation expires (stalled_alert_hours).
            state[key] = {"action": "review-ok", "at": current_time}
            continue
        if plan.line is None:
            continue
        lines.append(plan.line)
        if plan.action == "stall-alert":
            state[key] = {"action": "stall-alert", "at": current_time}
        elif plan.action == "gap-alert":
            state[key] = {"action": "gap-alert", "at": current_time}
        else:
            state[key] = {"action": "error:no-repo-root", "at": current_time}


def _process_blocked_tasks(
    cli: str,
    board: BoardInfo,
    blocked_tasks: Sequence[Mapping[str, object]],
    state: dict[str, dict[str, object]],
    lines: list[str],
    current_time: int,
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
    deadline: float | None = None,
) -> None:
    """Trigger (c) pass: complete review-required parents with shipped work.

    All four gates must hold before the parent is completed via the CLI:
    a shipped unmerged ``wt/<task_id>`` branch (in the task's own workspace
    repo or, for non-worktree tasks, the board's ``default_workdir``), an
    existing review child, and a block at least ``min_age_seconds`` old.
    The review child is never completed — only the parent, so the review
    child promotes and the reviewer still gates the merge. Aftermath is
    alert-only: a ``decomposed`` marker names duplicate impl children for
    the operator. Reads are parallel like the done pass; completions run
    sequentially.
    """
    candidates = [
        task
        for task in blocked_tasks
        if isinstance(task.get("id"), str)
        and task["id"]
        and f"{board.slug}:{task['id']}" not in state
    ]
    if not candidates:
        return
    plans = _plan_blocked_candidates(
        cli, board, candidates, cfg, current_time,
        runner=runner, git_runner=git_runner, deadline=deadline,
    )
    _act_blocked_plans(cli, board, plans, state, lines, current_time, cfg, runner=runner)


@dataclass(frozen=True, slots=True)
class _BlockedPlan:
    """Read-phase result for one blocked candidate; acted on sequentially."""

    key: str
    action: str  # "none" | "complete"
    command: list[str] | None = None
    episode: BlockedCandidate | None = None
    review_id: str | None = None
    sha: str | None = None
    duplicates: list[str] | None = None


def _plan_blocked_candidates(
    cli: str,
    board: BoardInfo,
    candidates: Sequence[Mapping[str, object]],
    cfg: ReviewGapConfig,
    current_time: int,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
    deadline: float | None = None,
) -> list[_BlockedPlan | None]:
    """Plan every blocked candidate in parallel; ``None`` = skipped by deadline."""
    if cfg.max_workers <= 1:
        return [
            _plan_blocked_candidate(cli, board, task, cfg, current_time, runner=runner, git_runner=git_runner)
            for task in candidates
        ]
    plans: list[_BlockedPlan | None] = [None] * len(candidates)
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
        submitted: list[tuple[int, Future[_BlockedPlan]]] = []
        for index, task in enumerate(candidates):
            if deadline is not None and time.monotonic() >= deadline:
                break  # budget exhausted — remaining candidates wait for the next tick
            future = pool.submit(
                _plan_blocked_candidate,
                cli=cli,
                board=board,
                task=task,
                cfg=cfg,
                current_time=current_time,
                runner=runner,
                git_runner=git_runner,
            )
            submitted.append((index, future))
        for index, future in submitted:
            plans[index] = future.result()
    return plans


def _plan_blocked_candidate(
    cli: str,
    board: BoardInfo,
    task: Mapping[str, object],
    cfg: ReviewGapConfig,
    current_time: int,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
) -> _BlockedPlan:
    """Read-phase for one blocked task: show, episode parse, git, child shows."""
    timeout = float(cfg.cli_timeout_seconds)
    task_id = str(task["id"])
    key = f"{board.slug}:{task_id}"
    show = show_task(cli, board.slug, task_id, runner=runner, timeout=timeout)
    episode = blocked_episode(task, board.slug, show)
    if episode is None:
        return _BlockedPlan(key, "none")  # not a review-required block
    if current_time - episode.blocked_at < int(cfg.min_age_seconds):
        return _BlockedPlan(key, "none")  # don't race the worker that just blocked
    repo_root = repo_root_for(episode.workspace_path, board.default_workdir)
    if repo_root is None:
        return _BlockedPlan(key, "none")  # cannot verify shipped work without a repo
    task_branch_raw = task.get("branch_name")
    task_branch = (
        task_branch_raw.strip()
        if isinstance(task_branch_raw, str) and task_branch_raw.strip()
        else None
    )
    sha = shipped_branch_evidence(
        Path(repo_root), task_id, branch_name=task_branch, runner=git_runner, timeout=timeout
    )
    if (
        sha is None
        and board.default_workdir
        and str(repo_root) != str(board.default_workdir)
    ):
        # Non-worktree tasks (scratch etc.) may still have shipped on
        # wt/<task_id> in the board's canonical repo — the recorded
        # workspace path is not the repo (observed 2026-08-05: hkrc
        # t_fa6f319f blocked review-required, shipped branch, but created
        # with workspace_kind=scratch, so the primary lookup missed it).
        # Re-check against default_workdir; a missing branch still yields
        # None, so this stays fail-closed.
        repo_root = board.default_workdir
        sha = shipped_branch_evidence(
            Path(repo_root),
            task_id,
            branch_name=task_branch,
            runner=git_runner,
            timeout=timeout,
        )
    if sha is None:
        return _BlockedPlan(key, "none")  # no unmerged shipped branch
    children_ids = [
        str(child)
        for child in (show.get("children") or [])
        if isinstance(child, str) and child
    ]
    reviews = review_children(cli, board.slug, children_ids, runner=runner, timeout=timeout)
    if not reviews:
        return _BlockedPlan(key, "none")  # no review child — trigger (a) domain
    review_id = reviews[0].task_id
    summary = build_handoff_summary(task_id, review_id, sha)
    metadata = {
        "review_card": review_id,
        "branch": task_branch or f"wt/{task_id}",
        "commit": sha,
    }
    command = build_complete_command(cli, board.slug, task_id, summary, metadata)
    return _BlockedPlan(
        key,
        "complete",
        command=command,
        episode=episode,
        review_id=review_id,
        sha=sha,
        duplicates=decomposed_child_ids(show),
    )


def _act_blocked_plans(
    cli: str,
    board: BoardInfo,
    plans: Sequence[_BlockedPlan | None],
    state: dict[str, dict[str, object]],
    lines: list[str],
    current_time: int,
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
) -> None:
    """Sequential action phase: run completions, emit lines, record dedupe."""
    timeout = float(cfg.cli_timeout_seconds)
    for plan in plans:
        if plan is None or plan.action != "complete":
            continue
        assert plan.command is not None and plan.episode is not None
        assert plan.review_id is not None and plan.sha is not None
        result = run_native(plan.command, runner=runner, timeout=timeout)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise ReviewGapError(
                f"complete review-required parent {plan.episode.task_id} on board "
                f"{board.slug} failed (exit {result.returncode}): {detail}"
            )
        lines.append(completed_line(plan.episode, plan.review_id, plan.sha))
        if plan.duplicates:
            lines.append(duplicate_alert_line(plan.episode, plan.duplicates))
        state[plan.key] = {"action": "completed", "at": current_time}


def _cached_list_tasks(
    cli: str,
    slug: str,
    cache: dict[str, list[dict]],
    *,
    runner: CliRunner | None,
    timeout: float | None,
) -> list[dict]:
    """Return the full task list for ``slug``, fetched at most once per tick.

    Trigger (d) consults the full list on the repo board AND — when routing a
    cross-board episode — on every other board, so caching turns the
    O(boards x episodes) list-fetch cost into O(boards) per tick.
    """
    if slug not in cache:
        cache[slug] = list_tasks(cli, slug, runner=runner, timeout=timeout)
    return cache[slug]


def _locate_parent_board(
    cli: str,
    all_boards: Sequence[BoardInfo],
    task_id: str,
    list_cache: dict[str, list[dict]],
    *,
    runner: CliRunner | None,
    timeout: float | None,
) -> tuple[BoardInfo | None, list[dict]]:
    """Find the board whose task list contains ``task_id``.

    Returns ``(board, listed)`` for the first board that has the task, or
    ``(None, [])`` when the task is on no board. Used by trigger (d) to
    route the re-apply card to the board that OWNS the parent task — the
    merge that got reverted may have been driven from a card on another
    board (observed: the t_f789b4ab revert lives in the hkrc repo while
    the task sits on the campcli board).
    """
    for other in all_boards:
        listed = _cached_list_tasks(cli, other.slug, list_cache, runner=runner, timeout=timeout)
        if any(t.get("id") == task_id for t in listed if isinstance(t.get("id"), str)):
            return other, listed
    return None, []


def _process_revert_drifts(
    cli: str,
    board: BoardInfo,
    all_boards: Sequence[BoardInfo],
    list_cache: dict[str, list[dict]],
    state: dict[str, dict[str, object]],
    lines: list[str],
    current_time: int,
    cfg: ReviewGapConfig,
    *,
    runner: CliRunner | None,
    git_runner: CliRunner | None,
    deadline: float | None = None,
) -> None:
    """Trigger (d) pass: create re-apply cards for reverted, un-re-merged merges.

    Scans canonical-main history of the board's project repo (the board
    ``default_workdir``; boards without a repo are skipped) for commits whose
    subject matches a revert of a kanban merge. A revert is a drift episode
    when, AND ONLY when, all of:

    1. The original merge was never re-applied: no commit after the revert
       in ``main`` history carries the canonical ``(kanban t_xxx)`` merge
       marker for the task (incidental mentions don't count).
    2. No ``re-apply reverted change`` card for the task already exists
       (checked on the repo board and, when routed, on the owning board).
    3. The episode is not already recorded in the dedupe state.

    The re-apply card is created on the board that OWNS the parent task —
    the repo board when the task lives there, otherwise the first board
    whose list contains it (cross-board merge; the drifted repo is still
    the workspace). An episode whose parent is on NO board alerts once
    (``revert-drift:unroutable``) and is never acted on with an unknown
    parent. A re-apply card that reaches a terminal state heals the episode
    when EITHER the drift was re-merged after the revert (``(kanban t_xxx)``
    subject marker) OR the card's completion text carries the
    worker-verified ``nothing to merge (branch == main)`` marker (content
    confirmed present on main — supersession). A terminal card with neither
    condition is a false-complete: the episode re-fires with a fresh card
    and an alert so unhealed work never goes quiet. The worker-verified
    heal is scoped to its revert — a later revert of the same task re-opens
    the episode. Action is creation-only: merges stay
    reviewer-controlled — never auto-merge, never auto-re-apply. Full task
    lists are served from ``list_cache`` (at most one fetch per board per
    tick), so routing stays O(boards) even with many episodes.
    """
    if deadline is not None and time.monotonic() >= deadline:
        return  # budget exhausted — this pass waits for the next tick
    if board.default_workdir is None:
        return  # no repo reachable from this board — nothing to scan
    timeout = float(cfg.cli_timeout_seconds)
    repo_root = Path(board.default_workdir)
    default = default_branch(repo_root, runner=git_runner, timeout=timeout)
    if default is None:
        return  # no main/master ref — nothing can have been merged into
    result = run_git(
        repo_root,
        ["log", default, "--format=%H%x09%P%x09%s"],
        runner=git_runner,
        timeout=timeout,
    )
    if result.returncode != 0:
        return  # not a git repo (or unreadable) — skip silently
    episodes = revert_episodes(result.stdout.splitlines())
    if not episodes:
        return
    listed = _cached_list_tasks(cli, board.slug, list_cache, runner=runner, timeout=timeout)
    for revert_sha, merge_sha, task_id, subject in episodes:
        key = f"{board.slug}:{task_id}"
        after = run_git(
            repo_root,
            ["log", default, "--format=%H%x09%s", f"{revert_sha}.."],
            runner=git_runner,
            timeout=timeout,
        )
        if after.returncode == 0 and _remerged_subjects_after(after.stdout, task_id):
            continue  # healed — re-merged after the revert
        # Route to the board that owns the parent task. The card parents to
        # the ORIGINAL task, so an unknown parent means no card — alert once.
        target_board = board
        target_listed = listed
        if task_id not in {
            t.get("id") for t in listed if isinstance(t.get("id"), str)
        }:
            located, located_listed = _locate_parent_board(
                cli, all_boards, task_id, list_cache, runner=runner, timeout=timeout
            )
            if located is None:
                if state.get(key) is None:  # alert once per episode
                    lines.append(unroutable_line(task_id, str(repo_root), board.slug))
                    state[key] = {"action": "revert-drift:unroutable", "at": current_time}
                continue
            target_board = located
            target_listed = located_listed
        if has_reapply_card(target_listed, task_id):
            continue  # an OPEN re-apply card is already queued on the owning board
        entry = state.get(key)
        if entry is not None:
            action = str(entry.get("action") or "")
            if action.startswith("revert-drift:") and action != "revert-drift:unroutable":
                # Episode already carded. A card that reached a terminal state
                # WITHOUT healing the drift (no re-merge after the revert, and
                # no worker-verified "nothing to merge" terminal result) is a
                # false-complete — re-fire with a fresh card so the loop never
                # goes quiet on unhealed work. An open card was handled above.
                card_id = action.split(":", 1)[1]
                status = next(
                    (
                        t.get("status")
                        for t in target_listed
                        if isinstance(t.get("id"), str) and t["id"] == card_id
                    ),
                    "archived",
                )
                if not (isinstance(status, str) and status in _TERMINAL_STATUSES):
                    continue  # card still open — episode in flight, dedupe holds
                card = next(
                    (
                        t
                        for t in target_listed
                        if isinstance(t.get("id"), str) and t["id"] == card_id
                    ),
                    None,
                )
                if card is not None and _reapply_card_verified_healed(
                    cli, target_board.slug, card, runner=runner, timeout=timeout
                ):
                    # Worker-verified supersession heal: the re-apply body
                    # contract requires the worker to confirm the reverted
                    # content is on main before stating "nothing to merge
                    # (branch == main)". A terminal card carrying that marker
                    # means the drift is healed — record the heal scoped to
                    # THIS revert (a later revert of the same task re-opens
                    # the episode) and never re-fire.
                    state[key] = {
                        "action": (
                            f"{_REVERT_DRIFT_HEALED_PREFIX}{revert_sha}:{card_id}"
                        ),
                        "at": current_time,
                    }
                    lines.append(healed_line(card_id, task_id, target_board.slug))
                    continue
                lines.append(false_complete_line(card_id, task_id, target_board.slug))
            elif action.startswith(_REVERT_DRIFT_HEALED_PREFIX):
                # A worker-verified heal recorded for an earlier revert of
                # this task: the same revert stays healed and silent; a NEW
                # revert (different sha) re-opens the episode and falls
                # through to the re-fire path below.
                healed_revert = action[len(_REVERT_DRIFT_HEALED_PREFIX):].split(":", 1)[0]
                if healed_revert == revert_sha:
                    continue
            else:
                continue  # some other recorded action — never touch it
        body = build_reapply_body(task_id, revert_sha, merge_sha, subject)
        command = build_reapply_create_command(
            cli, target_board.slug, task_id, str(repo_root), body
        )
        created = run_native(command, runner=runner, timeout=timeout)
        if created.returncode != 0:
            detail = (created.stderr or created.stdout or "").strip()
            raise ReviewGapError(
                f"create re-apply card for {task_id} on board "
                f"{target_board.slug} failed (exit {created.returncode}): {detail}"
            )
        created_id = parse_created_task_id(
            created.stdout, board_slug=target_board.slug, task_id=task_id
        )
        lines.append(reapply_line(created_id, task_id, target_board.slug))
        state[key] = {"action": f"revert-drift:{created_id}", "at": current_time}


__all__ = [
    "DEFAULT_BRANCH_CANDIDATES",
    "NOTHING_TO_MERGE_MARKER",
    "REAPPLY_TITLE_PREFIX",
    "REVALIDATION_TITLE_PREFIX",
    "REVIEW_ASSIGNEE",
    "REVIEW_PRIORITY",
    "REVIEW_REQUIRED_PREFIX",
    "REVERT_SUBJECT_RE",
    "STATE_FILENAME",
    "BlockedCandidate",
    "BoardInfo",
    "Candidate",
    "CliRunner",
    "NativeTimeoutError",
    "ReviewChild",
    "ReviewGapError",
    "blocked_episode",
    "branch_is_merged",
    "budget_line",
    "build_complete_command",
    "build_create_command",
    "build_handoff_summary",
    "build_native_environment",
    "build_reapply_body",
    "build_reapply_create_command",
    "build_revalidation_body",
    "build_review_body",
    "candidate_from_task",
    "commit_is_merged",
    "completed_line",
    "created_line",
    "decomposed_child_ids",
    "default_branch",
    "default_state_path",
    "discover_boards",
    "duplicate_alert_line",
    "error_line",
    "false_complete_line",
    "gap_line",
    "has_open_revalidation_card",
    "has_reapply_card",
    "healed_line",
    "is_candidate",
    "latest_blocked_event",
    "list_blocked_tasks",
    "list_done_tasks",
    "list_tasks",
    "parse_created_task_id",
    "reapply_line",
    "repo_root_for",
    "resolve_impl_commit",
    "revert_episodes",
    "review_children",
    "run",
    "run_git",
    "run_native",
    "shipped_branch_evidence",
    "show_task",
    "stale_merge_line",
    "stall_line",
    "timeout_line",
    "unroutable_line",
]
