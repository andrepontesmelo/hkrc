"""Decision-latency watcher (v0.9.0) — closes the review/fix/pick-gate loops.

The watcher automates the four stall classes measured on 2026-08-03 (board-wide
audit: ~90% of pipeline time was waiting, not working):

H1 - Auto-create a fix card when a review blocks with a defect payload
     (``FIX-READY|HIGH|MEDIUM|LOW defect``) and the task belongs to a reviewer
     profile.  One fix card per ``(review id, block episode)`` via an
     ``--idempotency-key hkrc-fix-<review_id>-<episode>``; never created when a
     non-done/archived fix card already exists for the review.

H2 - Close the supersede loop: when a fix card completes (or its fix review
     reaches done) and the fix is verified merged to the canonical branch
     (``git merge-base --is-ancestor`` — version strings and claims lie, the
     merge state does not), complete the original defect-blocked review with
     ``superseded: fixed by <fix_id>, merged <sha> (verified)`` and promote any
     gated children (deploy cards).

H3 - Pick-gate auto-advance: after any task completes, unblock the highest
     priority ``One-at-a-time:`` ``needs_input`` card (tie-break earliest
     ``created_at``).  Safety: never advance when any blocked card on the board
     is ``capability`` (drill synthetic blockers), the card reason contains
     ``PAUSE``/``hold``, its parents are not all done, or a recent comment on
     the parked card contains ``hold``.

H4 - Promotable-blocked guard: a task with ``status='blocked'`` but no
    ``blocked``-kind event in ``task_events`` is auto-promoted by the
    dispatcher next tick (the ``create --initial-status blocked`` trap).  The
    watcher writes the missing block event (``kind=needs_input``) so the
    dispatcher stops auto-promoting it.

H5 - Review-required deadlock archive: a blocked ``review-required:`` parent
    whose block reason carries completion evidence (FIX-READY / gate green /
    merge-base == main HEAD) and whose review child is stuck in ``todo`` past
    the debounce is the promotion deadlock — ``recompute_ready`` only
    promotes children of ``done``/``archived`` parents, so the blocked parent
    strands its review child forever.  The watcher archives the parent
    (``archived`` counts as satisfied, so the review child promotes; the
    reviewer still gates the merge).  Fail-closed: never archive without
    completion evidence in the reason, never archive a parent with no review
    child, and dedupe per block episode via the action key.

Boundary with ``review_gap`` (do not duplicate)
-----------------------------------------------
``review_gap.py`` owns MISSING-REVIEW-CARD creation: a done impl/fix task with
no paired review card gets ``review: validate ...`` auto-created (or alerted).
The watcher deliberately does NOT create review cards; its H1 creates FIX
cards only (a review already exists and is blocked with a defect payload), H2
closes the supersede loop, H3 advances pick gates, H4 writes missing block
events.  Keep it that way: if a defect-blocked review turns out to have no
review card, that is a review_gap concern, not a watcher H1 concern.

Design notes
------------
- One persistent WebSocket connection per board (the needs-input-watcher v2 /
  fecc272 infra): the watcher keeps a single authenticated socket per board
  across passes and reconnects only on transport failure (the adapter drops
  the socket itself on a transport error; a normal idle drain leaves it
  open).  The socket read timeout (``watcher.recv_timeout_seconds``) must
  stay below the cron cycle interval (``watcher.cycle_interval_seconds``) per
  the #77833 leak rule, which ``WatcherConfig`` enforces.  The durable
  per-board cursor lives in the controller-owned ``watcher-state.json`` next
  to the controller state database, so a cron one-shot resumes where the
  previous tick stopped.
- Board metadata and current state are read from native boards with plain
  ``mode=ro`` connections (the needs-input-watcher v2 pattern; deliberately not the
  discovery fail-closed reader because this watchdog only reads).  Native
  mutations go exclusively through the installed Hermes CLI as argv lists,
  with an explicit ``--board <slug>`` and a scrubbed environment (the
  dispatcher's ``HERMES_KANBAN_*`` pins would otherwise override the board
  scope).
- Replay mode (``--replay --dry-run``) reprocesses full history from cursor
  zero against an in-order event model so the operator can see the historical
  would-have actions; it is the acceptance surface for the dry-run log and is
  deliberately forbidden from mutating anything.
- Idempotency: dry-run records ``would:``-namespaced action keys and its own
  cursor namespace (``dry_run_cursors``), live mode records plain keys and
  the ``cursors`` namespace, and the native ``--idempotency-key`` on the
  fix-card create provides a second layer — a double run never creates
  duplicate fix cards, duplicate supersede completes, or duplicate unblocks,
  and a dry-run review period never consumes live cursor or action
  eligibility, so the live cutover still performs every would-have action.
- Action keys are consumed only by successful mutations (or deliberate
  non-action no-ops such as "an open fix card already exists"); failed
  native mutations and safety/verification deferrals stay eligible and are
  retried on the next pass.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time

from .config import ControllerConfig
from .discovery import Board, discover_boards
from .event_stream import (
    EventBatch,
    StreamAdapter,
    StreamCredentials,
    StreamError,
    StreamErrorCode,
    StreamEvent,
)
from .handoff import NativeResult

STATE_FILENAME = "watcher-state.json"

REVIEW_PREFIX = "review:"
FIX_PREFIX = "fix:"
TASK_ID_PATTERN = re.compile(r"t_[0-9a-f]{8}")
_SHA_PATTERN = re.compile(r"\b[0-9a-f]{7,40}\b")
# H1 trigger: a review block whose reason names a defect (FIX-READY or an
# explicit HIGH/MEDIUM/LOW defect label).
DEFECT_REASON_PATTERN = re.compile(
    r"(FIX-READY|HIGH\s+defect|MEDIUM\s+defect|LOW\s+defect)", re.IGNORECASE
)
_SEVERITY_PATTERN = re.compile(r"\b(HIGH|MEDIUM|LOW)\b", re.IGNORECASE)
# Severity -> native task priority.  HIGH=90 matches the observed manual fix
# card convention (a legacy manual fix card used 90 for a HIGH defect).
SEVERITY_PRIORITY = {"HIGH": 90, "MEDIUM": 60, "LOW": 30}

# H5 trigger: a ``review-required:`` block reason whose work is complete.
# The parent is only archived when the reason carries completion evidence —
# FIX-READY (with or without the hyphen), gate green, or an explicit
# ``merge-base == main HEAD`` statement.  Fail-closed: a bare
# ``review-required: question about X`` block never matches.
REVIEW_REQUIRED_PREFIX = "review-required"
COMPLETION_EVIDENCE_PATTERN = re.compile(
    r"(FIX[\s-]?READY|GATE\s+GREEN|MERGE[\s-]?BASE\s*==?\s*MAIN(\s+HEAD)?)",
    re.IGNORECASE,
)

TERMINAL_STATUSES = frozenset({"done", "archived"})


class WatcherError(RuntimeError):
    """Raised when the watcher cannot operate safely."""


@dataclass(frozen=True, slots=True)
class Action:
    """One decided watcher intervention (actual in live mode, would-be in dry-run)."""

    board_slug: str
    handler: str  # H1..H4
    kind: str  # create_fix_card | supersede_review | advance_pick_gate | write_block_event
    target_id: str
    detail: str
    would: bool


@dataclass(frozen=True, slots=True)
class DefectBlock:
    """A review block episode carrying a defect payload (H1 trigger row)."""

    board_slug: str
    task_id: str
    title: str
    assignee: str
    event_id: int
    blocked_at: int
    reason: str
    severity: str
    payload: str


@dataclass(frozen=True, slots=True)
class FixCardPlan:
    """The H1 fix card that would be created for a defect block."""

    board_slug: str
    review_id: str
    episode_key: str
    title: str
    body: str
    priority: int
    workspace: str
    severity: str


@dataclass(frozen=True, slots=True)
class SupersedeCandidate:
    """A done fix card whose original review may be closed as superseded (H2)."""

    board_slug: str
    review_id: str
    fix_id: str
    repo: Path | None
    shas: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PickGateCandidate:
    """A parked ``One-at-a-time:`` ``needs_input`` card (H3)."""

    board_slug: str
    task_id: str
    title: str
    priority: int
    created_at: int
    reason: str


@dataclass(frozen=True, slots=True)
class MissingBlockEvent:
    """A blocked task with no blocked-kind event (H4)."""

    board_slug: str
    task_id: str
    title: str
    status: str


@dataclass(frozen=True, slots=True)
class ReviewRequiredDeadlock:
    """A blocked ``review-required:`` parent whose review child is stuck in
    ``todo`` (H5).

    ``blocked_event_id`` anchors the episode — the ``task_events.id`` of the
    ``blocked`` transition that opened the current block — and is part of the
    dedupe key, so a double run never archives twice.
    """

    board_slug: str
    task_id: str
    title: str
    reason: str
    blocked_event_id: int
    blocked_at: int
    review_child_ids: tuple[str, ...]


# ---------------------------------------------------------------------------
# State file (controller-owned, atomic writes like needs-input-watcher)
# ---------------------------------------------------------------------------


def default_state_path(state_db: Path) -> Path:
    """Controller-owned state file next to the controller state database."""
    return state_db.parent / STATE_FILENAME


def _load_state(path: Path) -> dict[str, object]:
    if not path.is_file():
        return {"cursors": {}, "dry_run_cursors": {}, "actions": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WatcherError(f"cannot read watcher state {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise WatcherError(f"watcher state must be an object: {path}")
    cursors = raw.get("cursors", {})
    dry_run_cursors = raw.get("dry_run_cursors", {})
    actions = raw.get("actions", {})
    if not isinstance(cursors, dict) or not isinstance(dry_run_cursors, dict) or not isinstance(actions, dict):
        raise WatcherError(
            "watcher state must contain cursors, dry_run_cursors and actions objects: {path}"
        )
    normalized: dict[str, object] = {
        "cursors": {str(key): int(value) for key, value in cursors.items()},
        "dry_run_cursors": {str(key): int(value) for key, value in dry_run_cursors.items()},
        "actions": {str(key): value for key, value in actions.items() if isinstance(value, dict)},
    }
    return normalized


def _save_state(path: Path, state: Mapping[str, object]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(state, sort_keys=True, indent=1) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    except OSError as exc:
        raise WatcherError(f"cannot persist watcher state {path}: {exc}") from exc


def _action_key(kind: str, board_slug: str, *identifiers: str) -> str:
    return ":".join((kind, board_slug, *identifiers))


def _stored_key(would: bool, key: str) -> str:
    return f"would:{key}" if would else key


# ---------------------------------------------------------------------------
# Native board reads (plain mode=ro, needs-input-watcher v2 pattern)
# ---------------------------------------------------------------------------


def _open_read_only(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection
    except sqlite3.Error as exc:
        raise WatcherError(f"cannot open native board read-only {path}: {exc}") from exc


def _parse_payload(payload: object) -> dict[str, object]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str) and payload:
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _optional_text(value: object) -> str:
    return value if isinstance(value, str) else ""


def is_reviewer_assignee(config: ControllerConfig, assignee: str | None) -> bool:
    """Return True when ``assignee`` is a configured reviewer profile.

    An explicit ``reviewer_profiles`` allowlist wins; when empty the heuristic
    matches a profile name containing ``reviewer`` (review cards are
    assigned to the ``reviewer`` profile).
    """
    if not assignee:
        return False
    if config.watcher.reviewer_profiles:
        return assignee in config.watcher.reviewer_profiles
    return "reviewer" in assignee.casefold()


def _block_actor(payload: Mapping[str, object]) -> str:
    """Return the author/actor of a blocked event payload (empty when absent).

    Native ``blocked`` events carry ``reason``/``kind``/``recurrences``; the
    author fallback covers event payloads that record who blocked the task
    (``author`` or ``actor``).  H1 triggers when the task assignee OR the
    blocked event's author/actor is a reviewer profile.
    """
    for key in ("author", "actor"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def defect_severity(reason: str) -> str:
    """Return HIGH/MEDIUM/LOW for a defect block reason (default LOW)."""
    match = _SEVERITY_PATTERN.search(reason or "")
    return match.group(1).upper() if match else "LOW"


def fix_card_title(review_title: str) -> str:
    """``review: validate X`` -> ``fix: validate X findings``."""
    title = (review_title or "").strip()
    if title.casefold().startswith(REVIEW_PREFIX):
        title = title[len(REVIEW_PREFIX):].lstrip()
    return f"fix: {title} findings"


def extract_task_ids(text: str) -> tuple[str, ...]:
    """Return ``t_<8-hex>`` task references found in a title/body."""
    seen: list[str] = []
    for match in TASK_ID_PATTERN.findall(text or ""):
        if match not in seen:
            seen.append(match)
    return tuple(seen)


def extract_shas(*texts: str) -> tuple[str, ...]:
    """Return unique 7-40 hex commit candidates found in summaries."""
    seen: list[str] = []
    for text in texts:
        for match in _SHA_PATTERN.findall(text or ""):
            if match not in seen:
                seen.append(match)
    return tuple(seen)


# ---------------------------------------------------------------------------
# Git boundaries (merge verification + workspace derivation)
# ---------------------------------------------------------------------------

GitRunner = Callable[[Sequence[str]], NativeResult]


def _default_git_runner(command: Sequence[str]) -> NativeResult:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        return NativeResult(completed.returncode, completed.stdout, completed.stderr)
    except (subprocess.TimeoutExpired, OSError) as exc:
        return NativeResult(2, "", str(exc))


def _git(
    cwd: Path,
    runner: GitRunner,
    *arguments: str,
) -> str | None:
    """Run git in ``cwd``; return trimmed stdout or ``None`` on failure.

    A successful invocation with empty stdout (e.g. ``git merge-base
    --is-ancestor``) returns ``""`` so callers can distinguish "ran and
    confirmed" from "failed".
    """
    result = runner(["git", "-C", str(cwd), *arguments])
    if result.returncode != 0:
        return None
    return (result.stdout or "").strip()


def _resolve_git_runner(runner: GitRunner | None) -> GitRunner:
    return runner or _default_git_runner


def git_repo_root(path: str | Path | None, runner: GitRunner | None = None) -> Path | None:
    """Resolve a path to its owning git repository root (main repo, not a
    linked worktree), or ``None`` when the path is not inside a repository."""
    if not path:
        return None
    candidate = Path(path).expanduser()
    if not candidate.is_dir():
        return None
    git = _resolve_git_runner(runner)
    toplevel = _git(candidate, git, "rev-parse", "--show-toplevel")
    if toplevel is None:
        return None
    common = _git(Path(toplevel), git, "rev-parse", "--git-common-dir")
    if common is None:
        return Path(toplevel)
    common_path = Path(common).expanduser()
    if not common_path.is_absolute():
        common_path = Path(toplevel) / common_path
    # A linked worktree reports the shared ``<main>/.git`` as its common dir;
    # the main repository is its parent.  Any other layout falls back to the
    # toplevel, which is still a usable repository for the native worktree
    # materialization.
    if common_path.name == ".git":
        return common_path.parent
    return Path(toplevel)


def verify_merged(
    repo: Path,
    shas: Sequence[str],
    config: ControllerConfig,
    runner: GitRunner | None = None,
) -> str | None:
    """Return the full verified merge SHA, or ``None``.

    A commit counts as merged only when ``git merge-base --is-ancestor``
    succeeds against the canonical branch (``master``, falling back to
    ``main`` when ``master`` does not exist).  Never trust a summary's claim.
    """
    git = _resolve_git_runner(runner)
    branches = (
        config.watcher.canonical_branch,
        config.watcher.canonical_branch_fallback,
    )
    for branch in branches:
        if _git(repo, git, "rev-parse", "--verify", "--quiet", f"{branch}^{{commit}}") is None:
            continue
        for sha in shas:
            probe = _git(repo, git, "merge-base", "--is-ancestor", sha, branch)
            if probe is not None:
                full = _git(repo, git, "rev-parse", sha)
                return full or sha
    return None


# ---------------------------------------------------------------------------
# H1 - defect-block discovery and fix-card planning
# ---------------------------------------------------------------------------


def discover_defect_blocks(
    native_boards_root: Path,
    config: ControllerConfig,
    *,
    now: int | None = None,
    enforce_recency: bool = True,
) -> list[DefectBlock]:
    """Return reviewer defect blocks across all non-archived boards.

    A block qualifies when the task is assigned to a reviewer profile (or the
    blocked event's author/actor is a reviewer) and its ``blocked`` event
    payload reason matches ``FIX-READY|HIGH|MEDIUM|LOW defect``.  With
    ``enforce_recency`` the block must be newer than
    ``watcher.max_block_age_seconds`` (live mode); replay mode disables the
    window so historical stalls are still reported.
    """
    current_time = int(time.time()) if now is None else int(now)
    cutoff = current_time - int(config.watcher.max_block_age_seconds)
    blocks: list[DefectBlock] = []
    for board in discover_boards(native_boards_root):
        connection = _open_read_only(board.path / "kanban.db")
        try:
            blocks.extend(
                _discover_defect_blocks_on_board(
                    connection, board, config, cutoff=cutoff,
                    enforce_recency=enforce_recency,
                )
            )
        finally:
            connection.close()
    return blocks


def _discover_defect_blocks_on_board(
    connection: sqlite3.Connection,
    board: Board,
    config: ControllerConfig,
    *,
    cutoff: int,
    enforce_recency: bool,
) -> list[DefectBlock]:
    """Defect blocks on one board (the per-board core of the H1 scan)."""
    try:
        rows = connection.execute(
            """
            SELECT t.id, t.title, t.assignee,
                   e.id AS event_id, e.created_at AS blocked_at, e.payload
              FROM task_events AS e
              JOIN tasks AS t ON t.id = e.task_id
             WHERE e.kind = 'blocked'
               AND e.payload IS NOT NULL
             ORDER BY e.id ASC
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise WatcherError(f"cannot query native board {board.slug}: {exc}") from exc
    blocks: list[DefectBlock] = []
    for row in rows:
        payload = _parse_payload(row["payload"])
        reason = _optional_text(payload.get("reason"))
        if not DEFECT_REASON_PATTERN.search(reason):
            continue
        assignee = _optional_text(row["assignee"])
        actor = _block_actor(payload)
        if not (is_reviewer_assignee(config, assignee) or is_reviewer_assignee(config, actor)):
            continue
        blocked_at = int(row["blocked_at"])
        if enforce_recency and blocked_at < cutoff:
            continue
        blocks.append(
            DefectBlock(
                board_slug=board.slug,
                task_id=str(row["id"]),
                title=str(row["title"]),
                assignee=assignee,
                event_id=int(row["event_id"]),
                blocked_at=blocked_at,
                reason=reason,
                severity=defect_severity(reason),
                payload=json.dumps(payload, indent=2, ensure_ascii=False),
            )
        )
    return blocks


def _task_row(connection: sqlite3.Connection, task_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT id, title, status, assignee, workspace_path, workspace_kind "
        "FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()


def _usable_workspace_repo(
    row: sqlite3.Row | None,
    runner: GitRunner | None,
) -> Path | None:
    """Resolve a task row's git toplevel, or ``None`` when unusable.

    A ``scratch`` workspace is an ephemeral tmp directory that must never
    anchor a fix card or a merge-verification repository — the controller
    falls back to the parent impl workspace or the board ``default_workdir``
    instead (the worktree-kind contract).
    """
    if row is None:
        return None
    if _optional_text(row["workspace_kind"]) == "scratch":
        return None
    return git_repo_root(row["workspace_path"], runner)


def _parent_task_id(connection: sqlite3.Connection, task_id: str) -> str | None:
    row = connection.execute(
        "SELECT parent_id FROM task_links WHERE child_id = ? ORDER BY parent_id LIMIT 1",
        (task_id,),
    ).fetchone()
    return str(row["parent_id"]) if row is not None else None


def _board_default_workdir(native_boards_root: Path, board_slug: str) -> Path | None:
    metadata = Path(native_boards_root).expanduser() / board_slug / "board.json"
    if not metadata.is_file():
        return None
    try:
        raw = json.loads(metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    workdir = raw.get("default_workdir") if isinstance(raw, dict) else None
    if isinstance(workdir, str) and workdir.strip():
        return Path(workdir).expanduser()
    return None


def resolve_fix_workspace(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    board_slug: str,
    review_id: str,
    runner: GitRunner | None = None,
) -> str | None:
    """Derive ``worktree:<repo>`` for the fix card.

    Order: the blocked review's workspace git toplevel, then the parent impl
    task's workspace (resolved via ``task_links``) git toplevel, then the
    board ``default_workdir``.  A ``scratch``-kind workspace is never used
    (ephemeral tmp dir); the fallback chain skips it.  ``None`` when nothing
    resolves to a repository (the fix card is then not created and the gap is
    logged).
    """
    review = _task_row(connection, review_id)
    if review is not None:
        repo = _usable_workspace_repo(review, runner)
        if repo is not None:
            return f"worktree:{repo}"
    impl_id = _parent_task_id(connection, review_id)
    if impl_id is not None:
        impl = _task_row(connection, impl_id)
        if impl is not None:
            repo = _usable_workspace_repo(impl, runner)
            if repo is not None:
                return f"worktree:{repo}"
    workdir = _board_default_workdir(config.native_boards_root, board_slug)
    if workdir is not None:
        repo = git_repo_root(workdir, runner)
        if repo is not None:
            return f"worktree:{repo}"
    return None


def _reviewer_comments(
    connection: sqlite3.Connection,
    task_id: str,
    config: ControllerConfig,
) -> tuple[str, ...]:
    """Return comment bodies authored by reviewer profiles (reproduction steps)."""
    rows = connection.execute(
        "SELECT author, body FROM task_comments WHERE task_id = ? ORDER BY id ASC",
        (task_id,),
    ).fetchall()
    bodies: list[str] = []
    for row in rows:
        if not is_reviewer_assignee(config, _optional_text(row["author"])):
            continue
        body = _optional_text(row["body"]).strip()
        if body:
            bodies.append(body)
    return tuple(bodies)


def build_fix_card_body(
    config: ControllerConfig,
    block: DefectBlock,
    connection: sqlite3.Connection,
) -> str:
    """The H1 fix-card body: the FIX-READY-on-unmerged-impl rule, the defect
    payload verbatim, and the review's reproduction comments."""
    impl_id = _parent_task_id(connection, block.task_id)
    canonical = config.watcher.canonical_branch
    lines = [
        "Fix card auto-created by hkrc watcher (decision-latency automation) for "
        f"review {block.task_id} on board {block.board_slug} (block episode {block.event_id}).",
        "",
        "FIX-READY-ON-UNMERGED-IMPL RULE (mandatory):",
    ]
    if impl_id is not None:
        lines.append(
            f"If the implementation branch for the parent impl task {impl_id} "
            f"(branch wt/{impl_id}) was never merged to {canonical}, branch this fix "
            f"worktree FROM origin/wt/{impl_id} — do NOT rebase onto {canonical}. "
            "Verify first:"
        )
        lines.append(f"  git merge-base --is-ancestor <impl commit> origin/{canonical}")
    else:
        lines.append(
            f"If the implementation branch was never merged to {canonical}, branch this "
            f"fix worktree FROM origin/wt/<impl_id> — do NOT rebase onto {canonical}."
        )
    lines.extend(
        [
            "",
            f"Original review: {block.task_id} — {block.title}",
            f"Blocked at event {block.event_id} (unix {block.blocked_at}), "
            f"severity {block.severity}.",
            "",
            "DEFECT PAYLOAD (verbatim):",
            block.payload,
        ]
    )
    comments = _reviewer_comments(connection, block.task_id, config)
    if comments:
        lines.extend(["", "REPRODUCTION STEPS (from the review comments):", ""])
        lines.extend(comments)
    lines.extend(
        [
            "",
            "Acceptance: fix the defect; the fix review will retest. The original "
            "review closes as superseded only after the fix is verified merged to "
            f"the canonical branch ({canonical}).",
            "",
            "COMPLETION CONTRACT (mandatory): when the fix work is complete AND a "
            "paired review card exists (a review: child of this fix card), "
            "COMPLETE this fix card with review evidence (status done — the "
            "review child is the gate and only promotes when this card is done). "
            "Do NOT block with `review-required` (kind needs_input) in that "
            "case. Block with `review-required` ONLY when no review child "
            "exists.",
        ]
    )
    return "\n".join(lines)


def plan_fix_card(
    config: ControllerConfig,
    block: DefectBlock,
    connection: sqlite3.Connection,
    runner: GitRunner | None = None,
) -> FixCardPlan:
    """Plan the H1 fix card for a defect block (never mutates)."""
    workspace = resolve_fix_workspace(
        config, connection, block.board_slug, block.task_id, runner
    )
    if workspace is None:
        raise WatcherError(
            f"cannot resolve a worktree repo for fix card of {block.task_id} "
            f"on {block.board_slug}"
        )
    return FixCardPlan(
        board_slug=block.board_slug,
        review_id=block.task_id,
        episode_key=f"hkrc-fix-{block.task_id}-{block.event_id}",
        title=fix_card_title(block.title),
        body=build_fix_card_body(config, block, connection),
        priority=SEVERITY_PRIORITY[block.severity],
        workspace=workspace,
        severity=block.severity,
    )


def existing_fix_cards(
    connection: sqlite3.Connection,
    review_id: str,
) -> tuple[tuple[str, str, str], ...]:
    """Return ``(id, title, status)`` of fix cards referencing ``review_id``.

    Matches children titled ``fix:``, cards whose title embeds the review id
    (legacy manual cards without a parent edge), and cards created under the
    H1 idempotency key namespace.
    """
    seen: dict[str, tuple[str, str, str]] = {}
    rows = connection.execute(
        """
        SELECT c.id, c.title, c.status
          FROM task_links AS l
          JOIN tasks AS c ON c.id = l.child_id
         WHERE l.parent_id = ? AND c.title LIKE ?
        """,
        (review_id, "fix:%"),
    ).fetchall()
    for row in rows:
        seen[str(row["id"])] = (str(row["id"]), str(row["title"]), str(row["status"]))
    rows = connection.execute(
        "SELECT id, title, status FROM tasks WHERE title LIKE ? AND title LIKE ?",
        (f"%{review_id}%", "fix:%"),
    ).fetchall()
    for row in rows:
        seen[str(row["id"])] = (str(row["id"]), str(row["title"]), str(row["status"]))
    rows = connection.execute(
        "SELECT id, title, status FROM tasks WHERE idempotency_key LIKE ?",
        (f"hkrc-fix-{review_id}-%",),
    ).fetchall()
    for row in rows:
        seen[str(row["id"])] = (str(row["id"]), str(row["title"]), str(row["status"]))
    return tuple(seen.values())


def has_open_fix_card(existing: Sequence[tuple[str, str, str]]) -> bool:
    """True when any referenced fix card is not done/archived (H1 skip rule)."""
    return any(status not in TERMINAL_STATUSES for _, _, status in existing)


# ---------------------------------------------------------------------------
# H2 - supersede discovery + merge verification
# ---------------------------------------------------------------------------


def _completed_summaries(
    connection: sqlite3.Connection,
    task_id: str,
) -> tuple[str, ...]:
    rows = connection.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'completed'",
        (task_id,),
    ).fetchall()
    summaries: list[str] = []
    for row in rows:
        summary = _optional_text(_parse_payload(row["payload"]).get("summary"))
        if summary:
            summaries.append(summary)
    return tuple(summaries)


def _done_review_children(
    connection: sqlite3.Connection,
    fix_id: str,
) -> tuple[str, ...]:
    """Done ``review:`` children of a fix card (the approving fix review)."""
    rows = connection.execute(
        """
        SELECT c.id FROM task_links AS l
        JOIN tasks AS c ON c.id = l.child_id
         WHERE l.parent_id = ? AND c.title LIKE 'review:%' AND c.status = 'done'
        """,
        (fix_id,),
    ).fetchall()
    return tuple(str(row["id"]) for row in rows)


def discover_supersede_candidates(
    native_boards_root: Path,
    config: ControllerConfig,
    runner: GitRunner | None = None,
    *,
    review_status: Callable[[str, str], str] | None = None,
) -> list[SupersedeCandidate]:
    """Find done fix cards whose original review is blocked (H2 state scan).

    ``review_status(board_slug, review_id)`` overrides the current DB status
    (replay mode feeds the event model); the default reads the native row.
    """
    candidates: list[SupersedeCandidate] = []
    for board in discover_boards(native_boards_root):
        connection = _open_read_only(board.path / "kanban.db")
        try:
            fix_rows = connection.execute(
                "SELECT id, title, status, workspace_path FROM tasks "
                "WHERE title LIKE 'fix:%' AND status = 'done'"
            ).fetchall()
        except sqlite3.Error as exc:
            raise WatcherError(f"cannot query native board {board.slug}: {exc}") from exc
        finally:
            connection.close()
        for fix in fix_rows:
            fix_id = str(fix["id"])
            review_id = _original_review_id(board_path=board.path, fix=fix)
            if review_id is None:
                continue
            status = (
                review_status(board.slug, review_id)
                if review_status is not None
                else _current_status(board.path, review_id)
            )
            if status != "blocked":
                continue
            connection = _open_read_only(board.path / "kanban.db")
            try:
                summaries = list(_completed_summaries(connection, fix_id))
                for child in _done_review_children(connection, fix_id):
                    summaries.extend(_completed_summaries(connection, child))
            finally:
                connection.close()
            shas = extract_shas(*summaries)
            if not shas:
                continue
            repo = _fix_repo(config, board.slug, fix_id, review_id, runner)
            candidates.append(
                SupersedeCandidate(
                    board_slug=board.slug,
                    review_id=review_id,
                    fix_id=fix_id,
                    repo=repo,
                    shas=shas,
                )
            )
    return candidates


def _original_review_id(
    *,
    board_path: Path,
    fix: sqlite3.Row,
) -> str | None:
    """Map a done fix card to its original review id.

    Primary: a parent edge to a ``review:`` task (H1-created fix cards).
    Secondary: a ``t_<hex>`` reference in the fix title resolving to a
    ``review:`` task on the same board (legacy manual cards whose title
    embeds the parent ``t_<hex>`` reference).
    """
    own = _open_read_only(board_path / "kanban.db")
    try:
        rows = own.execute(
            """
            SELECT p.id FROM task_links AS l
            JOIN tasks AS p ON p.id = l.parent_id
             WHERE l.child_id = ? AND p.title LIKE 'review:%'
            """,
            (str(fix["id"]),),
        ).fetchall()
        if rows:
            return str(rows[0]["id"])
        for task_id in extract_task_ids(str(fix["title"])):
            row = own.execute(
                "SELECT id FROM tasks WHERE id = ? AND title LIKE 'review:%'",
                (task_id,),
            ).fetchone()
            if row is not None:
                return task_id
        return None
    finally:
        own.close()


def _current_status(board_path: Path, task_id: str) -> str:
    connection = _open_read_only(board_path / "kanban.db")
    try:
        row = connection.execute(
            "SELECT status FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return str(row["status"]) if row is not None else ""
    finally:
        connection.close()


def _fix_repo(
    config: ControllerConfig,
    board_slug: str,
    fix_id: str,
    review_id: str,
    runner: GitRunner | None = None,
) -> Path | None:
    """Resolve the repository for merge verification of a fix card."""
    connection = _open_read_only(
        Path(config.native_boards_root).expanduser() / board_slug / "kanban.db"
    )
    try:
        fix = _task_row(connection, fix_id)
        review = _task_row(connection, review_id)
    finally:
        connection.close()
    for row in (fix, review):
        if row is not None:
            repo = _usable_workspace_repo(row, runner)
            if repo is not None:
                return repo
    workdir = _board_default_workdir(config.native_boards_root, board_slug)
    if workdir is not None:
        return git_repo_root(workdir, runner)
    return None


# ---------------------------------------------------------------------------
# H3 - pick-gate candidates and safety
# ---------------------------------------------------------------------------


def discover_pick_gate_candidates(
    connection: sqlite3.Connection,
    config: ControllerConfig,
) -> list[PickGateCandidate]:
    """Blocked ``needs_input`` cards whose reason starts with the pick-gate
    prefix (the ``One-at-a-time:`` queue)."""
    rows = connection.execute(
        """
        SELECT t.id, t.title, t.priority, t.created_at,
               latest.payload AS payload
          FROM tasks AS t
          JOIN task_events AS latest
            ON latest.id = (
                SELECT e.id
                  FROM task_events AS e
                 WHERE e.task_id = t.id
                   AND e.kind IN ('blocked', 'unblocked')
                 ORDER BY e.created_at DESC, e.id DESC
                 LIMIT 1
            )
         WHERE t.status = 'blocked'
           AND latest.kind = 'blocked'
         ORDER BY t.id ASC
        """
    ).fetchall()
    candidates: list[PickGateCandidate] = []
    for row in rows:
        payload = _parse_payload(row["payload"])
        if payload.get("kind") != "needs_input":
            continue
        reason = _optional_text(payload.get("reason"))
        if not reason.startswith(config.watcher.pick_gate_prefix):
            continue
        candidates.append(
            PickGateCandidate(
                board_slug="",
                task_id=str(row["id"]),
                title=str(row["title"]),
                priority=int(row["priority"] or 0),
                created_at=int(row["created_at"] or 0),
                reason=reason,
            )
        )
    return candidates


def board_has_capability_block(connection: sqlite3.Connection) -> bool:
    """True when any blocked card on the board is a capability blocker
    (drill synthetic blockers) — H3 never advances the pick gate then."""
    row = connection.execute(
        "SELECT 1 FROM tasks WHERE status = 'blocked' AND block_kind = 'capability' LIMIT 1"
    ).fetchone()
    return row is not None


def pick_gate_skip_reason(
    connection: sqlite3.Connection,
    config: ControllerConfig,
    candidate: PickGateCandidate,
    *,
    now: int,
) -> str | None:
    """Return a skip reason for an unsafe pick-gate candidate, else ``None``.

    Safety rules: reason containing ``PAUSE``/``hold``, any parent not
    done/archived (leave to the dependency gate), or a recent comment on the
    parked card containing ``hold``.
    """
    reason_text = candidate.reason.casefold()
    if "pause" in reason_text or "hold" in reason_text:
        return "reason contains PAUSE/hold"
    parents = connection.execute(
        """
        SELECT p.status FROM task_links AS l
        JOIN tasks AS p ON p.id = l.parent_id
         WHERE l.child_id = ?
        """,
        (candidate.task_id,),
    ).fetchall()
    if any(str(row["status"]) not in TERMINAL_STATUSES for row in parents):
        return "parents not done (dependency gate)"
    cutoff = now - int(config.watcher.hold_comment_window_seconds)
    comment = connection.execute(
        "SELECT 1 FROM task_comments WHERE task_id = ? AND created_at >= ? AND body LIKE ? LIMIT 1",
        (candidate.task_id, cutoff, "%hold%"),
    ).fetchone()
    if comment is not None:
        return "recent comment contains hold"
    return None


def select_pick_gate(
    candidates: Sequence[PickGateCandidate],
) -> PickGateCandidate | None:
    """Highest priority, then earliest ``created_at`` (the pick order)."""
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: (candidate.priority, -candidate.created_at))


# ---------------------------------------------------------------------------
# H4 - blocked-without-event discovery
# ---------------------------------------------------------------------------


def discover_missing_block_events(
    connection: sqlite3.Connection,
    board_slug: str,
) -> list[MissingBlockEvent]:
    """Blocked tasks with no ``blocked``-kind event (the ``--initial-status
    blocked`` trap) — the dispatcher auto-promotes them next tick."""
    rows = connection.execute(
        """
        SELECT t.id, t.title, t.status
          FROM tasks AS t
         WHERE t.status = 'blocked'
           AND NOT EXISTS (
               SELECT 1 FROM task_events AS e
                WHERE e.task_id = t.id AND e.kind = 'blocked'
           )
         ORDER BY t.id ASC
        """
    ).fetchall()
    return [
        MissingBlockEvent(
            board_slug=board_slug,
            task_id=str(row["id"]),
            title=str(row["title"]),
            status=str(row["status"]),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# H5 - review-required deadlock discovery
# ---------------------------------------------------------------------------


def discover_review_required_deadlocks(
    native_boards_root: Path,
    config: ControllerConfig,
    *,
    now: int | None = None,
    enforce_min_age: bool = True,
) -> list[ReviewRequiredDeadlock]:
    """Return blocked ``review-required:`` parents with a review child stuck
    in ``todo`` across all non-archived boards.

    A deadlock is only reported when ALL of the following hold (fail-closed):

    1. The task is currently ``blocked`` and its latest ``blocked`` event
       payload reason starts with ``review-required``.
    2. The reason carries completion evidence (FIX-READY / gate green /
       merge-base == main HEAD) — the work is done, only the review gate is
       stuck.
    3. The block episode is at least ``watcher.deadlock_min_age_seconds``
       old (with ``enforce_min_age``; live mode always enforces).
    4. At least one review child (created-event assignee or reviewer run)
       is stuck in ``todo``.

    A bare ``review-required: question`` block is never a deadlock.
    """
    current_time = int(time.time()) if now is None else int(now)
    cutoff = current_time - int(config.watcher.deadlock_min_age_seconds)
    deadlocks: list[ReviewRequiredDeadlock] = []
    for board in discover_boards(native_boards_root):
        connection = _open_read_only(board.path / "kanban.db")
        try:
            deadlocks.extend(
                _discover_review_required_deadlocks_on_board(
                    connection, board, config, now=current_time,
                    cutoff=cutoff, enforce_min_age=enforce_min_age,
                )
            )
        finally:
            connection.close()
    return deadlocks


def _discover_review_required_deadlocks_on_board(
    connection: sqlite3.Connection,
    board: Board,
    config: ControllerConfig,
    *,
    now: int,
    cutoff: int,
    enforce_min_age: bool,
) -> list[ReviewRequiredDeadlock]:
    """Review-required deadlocks on one board (the per-board core of H5)."""
    try:
        rows = connection.execute(
            """
            SELECT t.id, t.title, e.id AS event_id, e.created_at AS blocked_at, e.payload
              FROM task_events AS e
              JOIN tasks AS t ON t.id = e.task_id
             WHERE e.kind = 'blocked'
               AND e.payload IS NOT NULL
             ORDER BY e.id ASC
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise WatcherError(f"cannot query native board {board.slug}: {exc}") from exc
    # Events are in insertion order: the last ``blocked`` event per task is
    # the one that opened the current episode.
    latest_by_task: dict[str, sqlite3.Row] = {}
    for row in rows:
        latest_by_task[str(row["id"])] = row
    deadlocks: list[ReviewRequiredDeadlock] = []
    for task_id, row in latest_by_task.items():
        payload = _parse_payload(row["payload"])
        reason = _optional_text(payload.get("reason"))
        if not reason.casefold().startswith(REVIEW_REQUIRED_PREFIX):
            continue
        if not COMPLETION_EVIDENCE_PATTERN.search(reason):
            continue
        blocked_at = int(row["blocked_at"])
        if enforce_min_age and blocked_at > cutoff:
            continue
        task = _task_row(connection, task_id)
        if task is None or _optional_text(task["status"]) != "blocked":
            continue
        stuck = _stuck_review_children(connection, task_id, config)
        if not stuck:
            continue
        deadlocks.append(
            ReviewRequiredDeadlock(
                board_slug=board.slug,
                task_id=task_id,
                title=str(row["title"]),
                reason=reason,
                blocked_event_id=int(row["event_id"]),
                blocked_at=blocked_at,
                review_child_ids=stuck,
            )
        )
    return deadlocks


def _stuck_review_children(
    connection: sqlite3.Connection,
    parent_id: str,
    config: ControllerConfig,
) -> tuple[str, ...]:
    """Review children of a parent stuck in ``todo`` (the deadlock shape)."""
    rows = connection.execute(
        "SELECT child_id FROM task_links WHERE parent_id = ?",
        (parent_id,),
    ).fetchall()
    stuck: list[str] = []
    for row in rows:
        child_id = str(row["child_id"])
        child = _task_row(connection, child_id)
        if child is None or _optional_text(child["status"]) != "todo":
            continue
        if _child_is_review(connection, child_id, config):
            stuck.append(child_id)
    return tuple(stuck)


def _child_is_review(
    connection: sqlite3.Connection,
    child_id: str,
    config: ControllerConfig,
) -> bool:
    """Deterministic has-review check: created-event assignee or reviewer run.

    Never match on the child's current assignee alone and never on title
    text: a child reassigned away from the reviewer still carries its
    reviewer ``created`` event and/or a reviewer ``task_run``.
    """
    created = connection.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'created' ORDER BY id ASC",
        (child_id,),
    ).fetchone()
    if created is not None:
        payload = _parse_payload(created["payload"])
        if is_reviewer_assignee(config, _optional_text(payload.get("assignee"))):
            return True
    try:
        runs = connection.execute(
            "SELECT profile FROM task_runs WHERE task_id = ? ORDER BY id ASC",
            (child_id,),
        ).fetchall()
    except sqlite3.Error:
        runs = ()
    for run in runs:
        if is_reviewer_assignee(config, _optional_text(run["profile"])):
            return True
    return False


# ---------------------------------------------------------------------------
# Native mutations (CLI argv lists, board-scoped, scrubbed environment)
# ---------------------------------------------------------------------------

NativeRunner = Callable[[Sequence[str]], NativeResult]


def _native_command(config: ControllerConfig, board_slug: str, arguments: Sequence[str]) -> list[str]:
    command = [config.native_cli]
    if config.native_profile:
        command.extend(["--profile", config.native_profile])
    command.extend(["kanban", "--board", board_slug, *arguments])
    return command


def build_watcher_wiring(
    config: ControllerConfig,
) -> tuple[dict[str, StreamAdapter], StreamCredentials]:
    """Build the approved authenticated per-board adapters for the watcher.

    Mirrors the daemon's wiring but honors ``watcher.recv_timeout_seconds``
    (the #77833 rule: the socket read timeout must stay below the cron cycle
    interval) and does not require a current-state reader.
    """
    from .live import WebSocketConnector, _credentials_from_environment_name

    stream = config.stream
    if not stream.enabled or stream.adapter != "approved_websocket":
        raise WatcherError("watcher requires stream.enabled with the approved_websocket adapter")
    if not stream.endpoint or not stream.credential_env:
        raise WatcherError("watcher requires a stream endpoint and credential environment")
    raw_credential = os.environ.get(stream.credential_env, "").strip()
    if not raw_credential:
        raise WatcherError(
            f"watcher credential environment {stream.credential_env} is unset"
        )
    credentials = _credentials_from_environment_name(stream.credential_env, raw_credential)
    connector = WebSocketConnector(
        recv_timeout=float(config.watcher.recv_timeout_seconds)
    )
    boards = stream.boards
    if not boards:
        boards = tuple(board.slug for board in discover_boards(config.native_boards_root))
        if not boards:
            raise WatcherError(
                f"no non-archived kanban boards discovered under {config.native_boards_root}"
            )
    adapters = {
        board_slug: StreamAdapter(
            stream.endpoint,
            allowed_boards={board_slug},
            connector=connector,
        )
        for board_slug in boards
    }
    return adapters, credentials


def _default_runner(command: Sequence[str]) -> NativeResult:
    """Run one native CLI command with a scrubbed environment.

    ``HERMES_KANBAN_*`` pins (set by the dispatcher for workers) would
    override the explicit ``--board`` scope; the watcher strips them so every
    mutation lands on the intended board regardless of the calling context.
    """
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("HERMES_KANBAN_")
    }
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
            env=environment,
        )
        return NativeResult(completed.returncode, completed.stdout, completed.stderr)
    except (subprocess.TimeoutExpired, OSError) as exc:
        return NativeResult(2, "", str(exc))


# ---------------------------------------------------------------------------
# Replay model (in-order event view for dry-run history reproduction)
# ---------------------------------------------------------------------------


class _BoardModel:
    """Minimal per-board task state reconstructed from the event stream."""

    def __init__(self) -> None:
        self.status: dict[str, str] = {}
        self.blocked_events: dict[str, list[int]] = {}
        self.latest_block: dict[str, tuple[str, str]] = {}
        self.completed_summaries: dict[str, list[str]] = {}

    def apply(self, event: StreamEvent) -> None:
        payload = _parse_payload(event.payload)
        task = event.task_id
        kind = event.kind
        if kind == "created":
            status = payload.get("status")
            self.status[task] = status if isinstance(status, str) and status else "ready"
        elif kind == "blocked":
            self.status[task] = "blocked"
            self.blocked_events.setdefault(task, []).append(event.id)
            block_kind = _optional_text(payload.get("kind"))
            self.latest_block[task] = (
                block_kind,
                _optional_text(payload.get("reason")),
            )
        elif kind == "unblocked":
            self.status[task] = "ready"
        elif kind == "promoted":
            self.status[task] = "ready"
        elif kind == "claimed":
            self.status[task] = "running"
        elif kind == "completed":
            self.status[task] = "done"
            summary = payload.get("summary")
            if isinstance(summary, str) and summary:
                self.completed_summaries.setdefault(task, []).append(summary)

    def has_blocked_event(self, task_id: str) -> bool:
        return bool(self.blocked_events.get(task_id))

    def parked_candidates(self, config: ControllerConfig) -> list[PickGateCandidate]:
        """Pick-gate candidates from the event model (replay mode)."""
        candidates: list[PickGateCandidate] = []
        for task_id, status in self.status.items():
            if status != "blocked":
                continue
            block_kind, reason = self.latest_block.get(task_id, ("", ""))
            if block_kind != "needs_input":
                continue
            if not reason.startswith(config.watcher.pick_gate_prefix):
                continue
            candidates.append(
                PickGateCandidate(
                    board_slug="",
                    task_id=task_id,
                    title=task_id,
                    priority=0,
                    created_at=0,
                    reason=reason,
                )
            )
        return candidates


# ---------------------------------------------------------------------------
# Event consumption (one persistent WS per board)
# ---------------------------------------------------------------------------


def consume_board_events(
    adapter: StreamAdapter,
    board_slug: str,
    since: int,
    credentials: StreamCredentials,
    *,
    max_events: int = 200_000,
    frame_size: int = 200,
) -> tuple[list[StreamEvent], StreamError | None]:
    """Read every available event for one board on the persistent socket.

    One persistent WebSocket per board (the fecc272 contract): a connected
    adapter is reused as-is — the drain just reads frames — and a new
    connection is established only when the adapter is not connected (first
    pass or after a transport failure dropped the socket) or when the
    requested ``since`` no longer matches the connection's position (replay
    from cursor zero, a state reset, or a different board).

    The server chunks the backlog at exactly ``frame_size`` events per frame
    and then holds the connection (it never sends an empty frame), so the
    drain ends on one of two signals: a partial tail frame (fewer than
    ``frame_size`` events — the backlog is exhausted) or the receive timeout
    (the board is idle).  Both keep the durable cursor at the last accepted
    event and leave the socket open for the next pass; a mid-drain disconnect
    surfaces as a tail ``StreamError`` with the already-accepted events
    intact, and the adapter drops the socket itself so the next pass
    reconnects.
    """
    if not adapter.connected or adapter.board != board_slug or adapter.cursor != since:
        if adapter.connected:
            adapter.close()
        error = adapter.connect(board_slug, since, credentials)
        if error is not None:
            return [], error
    events: list[StreamEvent] = []
    while True:
        frame = adapter.recv()
        if isinstance(frame, StreamError):
            return events, frame
        if not isinstance(frame, EventBatch):
            adapter.close()
            return events, StreamError(
                StreamErrorCode.DISCONNECTED,
                "adapter returned an invalid frame",
                retryable=False,
                cursor=adapter.cursor,
            )
        events.extend(frame.events)
        if not frame.events or len(frame.events) < frame_size or len(events) >= max_events:
            break
    return events, None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _record_action(
    actions: list[Action],
    state: dict[str, object],
    *,
    would: bool,
    handler: str,
    kind: str,
    board_slug: str,
    target_id: str,
    detail: str,
    key: str | None = None,
) -> None:
    actions.append(
        Action(
            board_slug=board_slug,
            handler=handler,
            kind=kind,
            target_id=target_id,
            detail=detail,
            would=would,
        )
    )
    if key is not None:
        action_map = state["actions"]
        assert isinstance(action_map, dict)
        action_map[_stored_key(would, key)] = {
            "handler": handler,
            "at": int(time.time()),
            "detail": detail,
        }


def _is_handled(state: dict[str, object], would: bool, key: str) -> bool:
    action_map = state["actions"]
    assert isinstance(action_map, dict)
    stored = _stored_key(would, key)
    if stored in action_map:
        return True
    # A dry-run proposal is also suppressed when the live action already
    # happened (the plain key exists): the review must never re-suggest
    # something the watcher already performed.  The reverse is deliberate —
    # live eligibility is never consumed by ``would:`` keys (DEF-2).
    if would and key in action_map:
        return True
    return False


def run(
    config: ControllerConfig,
    *,
    state_path: Path,
    dry_run: bool,
    now: int | None = None,
    replay: bool = False,
    runner: NativeRunner | None = None,
    git_runner: GitRunner | None = None,
    adapters: Mapping[str, StreamAdapter] | None = None,
    credentials: StreamCredentials | None = None,
) -> tuple[list[Action], str]:
    """Run one watcher pass across every watched board; return actions + digest.

    The digest is empty when nothing new (cron ``no_agent`` silence).  ``runner``
    and ``git_runner`` exist for deterministic tests; production uses
    ``subprocess.run`` argv lists.  ``replay`` forces cursor zero and is only
    legal with ``dry_run`` (it must never mutate).
    """
    if not config.watcher.enabled:
        return [], ""
    if replay and not dry_run:
        raise WatcherError("--replay requires --dry-run (replay must never mutate)")
    if adapters is None or credentials is None:
        raise WatcherError("watcher requires an approved stream wiring (adapter + credentials)")
    current_time = int(time.time()) if now is None else int(now)
    state = _load_state(state_path)
    cursors = state["cursors"]
    dry_run_cursors = state["dry_run_cursors"]
    assert isinstance(cursors, dict)
    assert isinstance(dry_run_cursors, dict)
    # Dry-run and live keep separate cursor namespaces: a dry-run review
    # period must never consume the live cursor, otherwise the live cutover
    # would start past the would-have events and skip every action.  Replay
    # always starts from cursor zero in either namespace.
    cursor_store = dry_run_cursors if dry_run else cursors
    actions: list[Action] = []
    native_runner = runner or _default_runner
    git = git_runner or _default_git_runner

    boards = discover_boards(config.native_boards_root)
    for board in sorted(boards, key=lambda item: item.slug):
        adapter = adapters.get(board.slug)
        if adapter is None:
            continue  # board not in the approved wiring allowlist
        since = 0 if replay else int(cursor_store.get(board.slug, 0))
        events, error = consume_board_events(adapter, board.slug, since, credentials)
        if error is not None and not events:
            # Nothing was consumed (auth/transport failure before the first
            # frame): fail closed for this board, durable cursor stays intact.
            _record_action(
                actions,
                state,
                would=dry_run,
                handler="0",
                kind="stream_error",
                board_slug=board.slug,
                target_id="",
                detail=f"{error.code}: {error.message}",
            )
            continue
        # An error after complete frames is a transport hiccup at the tail of
        # the drain: the events already accepted are processed and the cursor
        # advances to the last one; the next tick resumes from there.
        model = _BoardModel()
        connection = _open_read_only(board.path / "kanban.db")
        try:
            for event in events:
                model.apply(event)
                payload = _parse_payload(event.payload)
                if event.kind == "blocked":
                    _handle_h1(
                        config,
                        connection,
                        actions,
                        state,
                        board,
                        event,
                        model,
                        dry_run=dry_run,
                        now=current_time,
                        replay=replay,
                        git_runner=git,
                        runner=native_runner,
                    )
                elif event.kind == "completed":
                    _handle_h2_completed(
                        config,
                        connection,
                        actions,
                        state,
                        board,
                        event,
                        model,
                        dry_run=dry_run,
                        git_runner=git,
                        runner=native_runner,
                    )
                    _handle_h3_completed(
                        config,
                        connection,
                        actions,
                        state,
                        board,
                        event,
                        model,
                        dry_run=dry_run,
                        now=current_time,
                        replay=replay,
                        runner=native_runner,
                    )
                if replay and event.kind == "created" and payload.get("status") == "blocked":
                    # H4 at event time: created with status blocked and no
                    # blocked event yet in the replay model.
                    if not model.has_blocked_event(event.task_id):
                        _handle_h4(
                            config,
                            actions,
                            state,
                            board.slug,
                            event.task_id,
                            title="",
                            status="blocked",
                            dry_run=dry_run,
                            runner=native_runner,
                        )
            if not replay:
                # H4 live: any tick, scan the board state directly.
                for missing in discover_missing_block_events(connection, board.slug):
                    _handle_h4(
                        config,
                        actions,
                        state,
                        board.slug,
                        missing.task_id,
                        title=missing.title,
                        status=missing.status,
                        dry_run=dry_run,
                        runner=native_runner,
                    )
                # H1 live state scan: a defect block whose fix-card create
                # failed (or was never attempted) stays eligible and is
                # retried every pass (DEF-5).  The event path alone cannot
                # retry because the durable cursor already advanced past the
                # blocked event.
                cutoff = current_time - int(config.watcher.max_block_age_seconds)
                for block in _discover_defect_blocks_on_board(
                    connection,
                    board,
                    config,
                    cutoff=cutoff,
                    enforce_recency=True,
                ):
                    _handle_h1_block(
                        config,
                        connection,
                        actions,
                        state,
                        board,
                        block,
                        dry_run=dry_run,
                        now=current_time,
                        replay=False,
                        model=None,
                        git_runner=git,
                        runner=native_runner,
                    )
                # H5 live state scan: a blocked ``review-required:`` parent
                # whose work is complete (completion evidence in the reason)
                # with a review child stuck in todo past the debounce is the
                # promotion deadlock — archive the parent so the review child
                # promotes (the reviewer still gates the merge).
                for deadlock in _discover_review_required_deadlocks_on_board(
                    connection,
                    board,
                    config,
                    now=current_time,
                    cutoff=current_time - int(config.watcher.deadlock_min_age_seconds),
                    enforce_min_age=True,
                ):
                    _handle_h5_deadlock(
                        config,
                        connection,
                        actions,
                        state,
                        board,
                        deadlock,
                        dry_run=dry_run,
                        runner=native_runner,
                    )
        finally:
            connection.close()
        if events:
            cursor_store[board.slug] = int(events[-1].id)
    if not replay:
        # H2 live: a done fix card whose review is still blocked is a pending
        # supersede regardless of when the fix completed.
        for candidate in discover_supersede_candidates(
            config.native_boards_root,
            config,
            git,
        ):
            _handle_h2_supersede(
                config,
                actions,
                state,
                candidate,
                dry_run=dry_run,
                git_runner=git,
                runner=native_runner,
            )
        # DEF-8 retry: gated children whose promotion failed earlier (no
        # recorded promote key) are re-attempted every pass so no deploy card
        # is left waiting after the supersede bookkeeping.
        for board in sorted(boards, key=lambda item: item.slug):
            adapter = adapters.get(board.slug)
            if adapter is None:
                continue
            connection = _open_read_only(board.path / "kanban.db")
            try:
                for review_id, child_id in discover_pending_promotions(
                    connection, board.slug
                ):
                    _attempt_promote(
                        config,
                        actions,
                        state,
                        board.slug,
                        review_id,
                        child_id,
                        dry_run=dry_run,
                        runner=native_runner,
                    )
            finally:
                connection.close()
    _save_state(state_path, state)
    return actions, format_message(actions)


def _handle_h1_block(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    actions: list[Action],
    state: dict[str, object],
    board: Board,
    block: DefectBlock,
    *,
    dry_run: bool,
    now: int,
    replay: bool,
    model: _BoardModel | None,
    git_runner: GitRunner,
    runner: NativeRunner,
) -> None:
    """Create the fix card for one defect block (H1 shared core).

    Used by both the event path (a ``blocked`` event in the stream) and the
    live state scan (``discover_defect_blocks``), so a failed create stays
    eligible and is retried on the next pass (DEF-5).
    """
    board_slug = board.slug
    row = _task_row(connection, block.task_id)
    if row is None:
        return
    # The review must be blocked at decision time (live: current DB row;
    # replay: the in-order model right after the blocked event).
    status = (
        model.status.get(block.task_id) if replay and model is not None else _optional_text(row["status"])
    )
    if status != "blocked":
        return
    if not replay:
        if now - block.blocked_at > int(config.watcher.max_block_age_seconds):
            return
    key = _action_key("fix", board_slug, block.task_id, str(block.event_id))
    if _is_handled(state, dry_run, key):
        return
    existing = existing_fix_cards(connection, block.task_id)
    if has_open_fix_card(existing):
        # Deliberate non-action no-op: an open fix card already covers the
        # review, so the episode is genuinely handled (consume the key).
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="1",
            kind="create_fix_card",
            board_slug=board_slug,
            target_id=block.task_id,
            detail=(
                f"skipped: an open fix card already exists for {block.task_id} "
                f"({', '.join(item[0] for item in existing)})"
            ),
            key=key,
        )
        return
    try:
        plan = plan_fix_card(config, block, connection, git_runner)
    except WatcherError:
        # Defer without consuming eligibility: the workspace may become
        # resolvable later (impl branch materialized, board default_workdir
        # configured).  Retried on the next pass.
        return
    detail = (
        f"would create fix card '{plan.title}' (parent {plan.review_id}, "
        f"priority {plan.priority}, workspace {plan.workspace}, episode {block.event_id})"
        if dry_run
        else f"created fix card '{plan.title}' (parent {plan.review_id})"
    )
    if not dry_run:
        result = runner(
            _native_command(
                config,
                board_slug,
                [
                    "create",
                    plan.title,
                    "--body",
                    plan.body,
                    "--assignee",
                    config.watcher.fix_assignee,
                    "--parent",
                    plan.review_id,
                    "--priority",
                    str(plan.priority),
                    "--workspace",
                    plan.workspace,
                    "--idempotency-key",
                    plan.episode_key,
                    "--json",
                ],
            )
        )
        if result.returncode != 0:
            # Failed mutation: report it but do NOT consume the action key, so
            # the defect block stays eligible and is retried on the next pass.
            _record_action(
                actions,
                state,
                would=dry_run,
                handler="1",
                kind="create_fix_card",
                board_slug=board_slug,
                target_id=block.task_id,
                detail=(
                    f"FAILED create fix card for {block.task_id}: "
                    f"exit {result.returncode}: {(result.stderr or result.stdout).strip()[:300]}"
                ),
            )
            return
    _record_action(
        actions,
        state,
        would=dry_run,
        handler="1",
        kind="create_fix_card",
        board_slug=board_slug,
        target_id=block.task_id,
        detail=detail,
        key=key,
    )


def _handle_h1(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    actions: list[Action],
    state: dict[str, object],
    board: Board,
    event: StreamEvent,
    model: _BoardModel,
    *,
    dry_run: bool,
    now: int,
    replay: bool,
    git_runner: GitRunner,
    runner: NativeRunner,
) -> None:
    """H1 event trigger: a ``blocked`` event carrying a defect payload."""
    payload = _parse_payload(event.payload)
    reason = _optional_text(payload.get("reason"))
    if not DEFECT_REASON_PATTERN.search(reason):
        return
    row = _task_row(connection, event.task_id)
    if row is None:
        return
    assignee = _optional_text(row["assignee"])
    actor = _block_actor(payload)
    if not (is_reviewer_assignee(config, assignee) or is_reviewer_assignee(config, actor)):
        return
    block = DefectBlock(
        board_slug=board.slug,
        task_id=event.task_id,
        title=str(row["title"]),
        assignee=assignee,
        event_id=event.id,
        blocked_at=int(event.created_at),
        reason=reason,
        severity=defect_severity(reason),
        payload=json.dumps(payload, indent=2, ensure_ascii=False),
    )
    _handle_h1_block(
        config,
        connection,
        actions,
        state,
        board,
        block,
        dry_run=dry_run,
        now=now,
        replay=replay,
        model=model,
        git_runner=git_runner,
        runner=runner,
    )


def _handle_h2_completed(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    actions: list[Action],
    state: dict[str, object],
    board: Board,
    event: StreamEvent,
    model: _BoardModel,
    *,
    dry_run: bool,
    git_runner: GitRunner,
    runner: NativeRunner,
) -> None:
    """H2 event trigger: a completed fix card, or a completed fix review whose
    parent is a fix card, may close the original review as superseded."""
    board_slug = board.slug
    title_row = _task_row(connection, event.task_id)
    if title_row is None:
        return
    fix_id: str | None = None
    if _optional_text(title_row["title"]).casefold().startswith(FIX_PREFIX):
        fix_id = event.task_id
    else:
        parent = _parent_task_id(connection, event.task_id)
        if parent is not None:
            parent_row = _task_row(connection, parent)
            if parent_row is not None and _optional_text(parent_row["title"]).casefold().startswith(FIX_PREFIX):
                fix_id = parent
    if fix_id is None:
        return
    review_id = _original_review_id_from_fix(connection, board_slug, fix_id)
    if review_id is None:
        return
    status = model.status.get(review_id, "")
    if status != "blocked":
        return
    summaries = list(model.completed_summaries.get(fix_id, ()))
    for child in _done_review_children(connection, fix_id):
        summaries.extend(model.completed_summaries.get(child, ()))
    shas = extract_shas(*summaries)
    if not shas:
        return
    repo = _fix_repo(config, board_slug, fix_id, review_id, git_runner)
    if repo is None:
        return
    candidate = SupersedeCandidate(
        board_slug=board_slug, review_id=review_id, fix_id=fix_id, repo=repo, shas=shas
    )
    _handle_h2_supersede(
        config, actions, state, candidate, dry_run=dry_run, git_runner=git_runner, runner=runner
    )


def _original_review_id_from_fix(
    connection: sqlite3.Connection,
    board_slug: str,
    fix_id: str,
) -> str | None:
    row = connection.execute(
        """
        SELECT p.id FROM task_links AS l
        JOIN tasks AS p ON p.id = l.parent_id
         WHERE l.child_id = ? AND p.title LIKE 'review:%'
        """,
        (fix_id,),
    ).fetchone()
    if row is not None:
        return str(row["id"])
    title_row = connection.execute(
        "SELECT title FROM tasks WHERE id = ?", (fix_id,)
    ).fetchone()
    if title_row is None:
        return None
    for task_id in extract_task_ids(_optional_text(title_row["title"])):
        candidate = connection.execute(
            "SELECT id FROM tasks WHERE id = ? AND title LIKE 'review:%'", (task_id,)
        ).fetchone()
        if candidate is not None:
            return task_id
    return None


def _handle_h2_supersede(
    config: ControllerConfig,
    actions: list[Action],
    state: dict[str, object],
    candidate: SupersedeCandidate,
    *,
    dry_run: bool,
    git_runner: GitRunner,
    runner: NativeRunner,
) -> None:
    key = _action_key("supersede", candidate.board_slug, candidate.review_id)
    if _is_handled(state, dry_run, key):
        return
    verified = None
    if candidate.repo is not None:
        verified = verify_merged(candidate.repo, candidate.shas, config, git_runner)
    if verified is None:
        # Defer without consuming eligibility: the fix SHA is not (yet)
        # verified merged to the canonical branch.  The next pass retries, so
        # a later verified merge still supersedes the original review.  Kept
        # silent so an unmerged fix does not ping the cron every cycle.
        return
    if dry_run:
        detail = (
            f"would complete {candidate.review_id} as superseded "
            f"(fixed by {candidate.fix_id}, merged {verified} (verified))"
        )
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="2",
            kind="supersede_review",
            board_slug=candidate.board_slug,
            target_id=candidate.review_id,
            detail=detail,
            key=key,
        )
        return
    result = runner(
        _native_command(
            config,
            candidate.board_slug,
            [
                "complete",
                candidate.review_id,
                "--result",
                f"superseded: fixed by {candidate.fix_id}, merged {verified} (verified)",
            ],
        )
    )
    if result.returncode != 0:
        # Failed mutation: report it but do NOT consume the action key, so the
        # supersede stays eligible and is retried on the next pass.
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="2",
            kind="supersede_review",
            board_slug=candidate.board_slug,
            target_id=candidate.review_id,
            detail=(
                f"FAILED complete {candidate.review_id} as superseded: exit "
                f"{result.returncode}: {(result.stderr or result.stdout).strip()[:300]}"
            ),
        )
        return
    detail = (
        f"completed {candidate.review_id} as superseded "
        f"(fixed by {candidate.fix_id}, merged {verified} (verified))"
    )
    promoted, failed = _promote_gated_children(
        config,
        actions,
        state,
        candidate.board_slug,
        candidate.review_id,
        dry_run=dry_run,
        runner=runner,
    )
    if promoted:
        detail += f"; promoted gated children: {', '.join(promoted)}"
    if failed:
        detail += f"; FAILED to promote gated children: {', '.join(failed)}"
    _record_action(
        actions,
        state,
        would=dry_run,
        handler="2",
        kind="supersede_review",
        board_slug=candidate.board_slug,
        target_id=candidate.review_id,
        detail=detail,
        key=key,
    )


def _attempt_promote(
    config: ControllerConfig,
    actions: list[Action],
    state: dict[str, object],
    board_slug: str,
    review_id: str,
    child_id: str,
    *,
    dry_run: bool,
    runner: NativeRunner,
) -> bool:
    """Promote one gated child; return True on success or deliberate skip.

    The per-child ``promote:<board>:<child>`` key is consumed only on success
    (or a dry-run would), so a failed promotion stays eligible and the next
    pass retries it.  Failures are reported in the digest.
    """
    key = _action_key("promote", board_slug, child_id)
    if _is_handled(state, dry_run, key):
        return True
    if dry_run:
        _record_action(
            actions,
            state,
            would=True,
            handler="2",
            kind="promote_gated_child",
            board_slug=board_slug,
            target_id=child_id,
            detail=(
                f"would promote gated child {child_id} of superseded review {review_id}"
            ),
            key=key,
        )
        return True
    result = runner(_native_command(config, board_slug, ["promote", child_id]))
    if result.returncode != 0:
        _record_action(
            actions,
            state,
            would=False,
            handler="2",
            kind="promote_gated_child",
            board_slug=board_slug,
            target_id=child_id,
            detail=(
                f"FAILED promote gated child {child_id} of {review_id}: exit "
                f"{result.returncode}: {(result.stderr or result.stdout).strip()[:300]}"
            ),
        )
        return False
    _record_action(
        actions,
        state,
        would=False,
        handler="2",
        kind="promote_gated_child",
        board_slug=board_slug,
        target_id=child_id,
        detail=f"promoted gated child {child_id} of superseded review {review_id}",
        key=key,
    )
    return True


def _promote_gated_children(
    config: ControllerConfig,
    actions: list[Action],
    state: dict[str, object],
    board_slug: str,
    review_id: str,
    *,
    dry_run: bool,
    runner: NativeRunner,
) -> tuple[list[str], list[str]]:
    """Promote todo/blocked children of the now-done review (deploy cards).

    Returns ``(promoted, failed)`` child ids.  Failures are reported (per-child
    ``promote`` actions) and remain eligible: the pending-promotion scan in
    ``run`` retries them on a later pass.
    """
    connection = _open_read_only(
        Path(config.native_boards_root).expanduser() / board_slug / "kanban.db"
    )
    try:
        rows = connection.execute(
            """
            SELECT c.id FROM task_links AS l
            JOIN tasks AS c ON c.id = l.child_id
             WHERE l.parent_id = ? AND c.status IN ('todo', 'blocked')
            """,
            (review_id,),
        ).fetchall()
        children = [str(row["id"]) for row in rows]
    finally:
        connection.close()
    promoted: list[str] = []
    failed: list[str] = []
    for child in children:
        if _attempt_promote(
            config, actions, state, board_slug, review_id, child,
            dry_run=dry_run, runner=runner,
        ):
            promoted.append(child)
        else:
            failed.append(child)
    return promoted, failed


def discover_pending_promotions(
    connection: sqlite3.Connection,
    board_slug: str,
) -> list[tuple[str, str]]:
    """Return ``(review_id, child_id)`` pairs still needing a gated promotion.

    A todo/blocked child of a done review that was completed with a
    ``superseded`` result has no recorded ``promote`` key when a previous
    promotion attempt failed (or the watcher never saw the completion).  The
    live scan promotes these so no deploy card is left behind.
    """
    rows = connection.execute(
        """
        SELECT p.id AS review_id, c.id AS child_id
          FROM task_links AS l
          JOIN tasks AS p ON p.id = l.parent_id
          JOIN tasks AS c ON c.id = l.child_id
         WHERE p.status = 'done'
           AND c.status IN ('todo', 'blocked')
           AND EXISTS (
               SELECT 1 FROM task_events AS e
                WHERE e.task_id = p.id AND e.kind = 'completed'
                  AND e.payload LIKE '%superseded%'
           )
         ORDER BY c.id ASC
        """
    ).fetchall()
    return [(str(row["review_id"]), str(row["child_id"])) for row in rows]


def _enrich_pick_candidate(
    connection: sqlite3.Connection,
    board_slug: str,
    candidate: PickGateCandidate,
) -> PickGateCandidate:
    """Fill priority/created_at/title from the native task row (replay path)."""
    row = connection.execute(
        "SELECT title, priority, created_at FROM tasks WHERE id = ?",
        (candidate.task_id,),
    ).fetchone()
    if row is None:
        return candidate
    return PickGateCandidate(
        board_slug=board_slug,
        task_id=candidate.task_id,
        title=str(row["title"]),
        priority=int(row["priority"] or 0),
        created_at=int(row["created_at"] or 0),
        reason=candidate.reason,
    )


def _handle_h3_completed(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    actions: list[Action],
    state: dict[str, object],
    board: Board,
    event: StreamEvent,
    model: _BoardModel,
    *,
    dry_run: bool,
    now: int,
    replay: bool,
    runner: NativeRunner,
) -> None:
    """H3 trigger: any completion on a watched board advances the pick gate."""
    board_slug = board.slug
    if board_has_capability_block(connection):
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="3",
            kind="advance_pick_gate",
            board_slug=board_slug,
            target_id="",
            detail="skipped: a capability blocker is parked on the board (drill guard)",
        )
        return
    if replay:
        candidates = [
            _enrich_pick_candidate(connection, board_slug, candidate)
            for candidate in model.parked_candidates(config)
        ]
    else:
        candidates = discover_pick_gate_candidates(connection, config)
    eligible: list[PickGateCandidate] = []
    for candidate in candidates:
        enriched = PickGateCandidate(
            board_slug=board_slug,
            task_id=candidate.task_id,
            title=candidate.title,
            priority=candidate.priority,
            created_at=candidate.created_at,
            reason=candidate.reason,
        )
        key = _action_key("pick", board_slug, candidate.task_id)
        if _is_handled(state, dry_run, key):
            continue
        skip = pick_gate_skip_reason(connection, config, enriched, now=now)
        if skip is not None:
            # Defer without consuming eligibility: report the safety skip but
            # leave the candidate eligible, so a later safe completion (parent
            # done, hold cleared) still unblocks it.
            _record_action(
                actions,
                state,
                would=dry_run,
                handler="3",
                kind="advance_pick_gate",
                board_slug=board_slug,
                target_id=candidate.task_id,
                detail=f"skipped: {skip}",
            )
            continue
        eligible.append(enriched)
    next_candidate = select_pick_gate(eligible)
    if next_candidate is None:
        return
    key = _action_key("pick", board_slug, next_candidate.task_id)
    detail = (
        f"would unblock {next_candidate.task_id} (pick-gate queue, priority "
        f"{next_candidate.priority})"
        if dry_run
        else f"unblocked {next_candidate.task_id} (pick-gate queue, priority {next_candidate.priority})"
    )
    if not dry_run:
        result = runner(
            _native_command(
                config,
                board_slug,
                [
                    "unblock",
                    next_candidate.task_id,
                    "--reason",
                    f"hkrc watcher pick-gate advance (priority {next_candidate.priority})",
                ],
            )
        )
        if result.returncode != 0:
            # Failed mutation: report it but do NOT consume the action key, so
            # the parked candidate stays eligible and is retried.
            _record_action(
                actions,
                state,
                would=dry_run,
                handler="3",
                kind="advance_pick_gate",
                board_slug=board_slug,
                target_id=next_candidate.task_id,
                detail=(
                    f"FAILED unblock {next_candidate.task_id}: exit {result.returncode}: "
                    f"{(result.stderr or result.stdout).strip()[:300]}"
                ),
            )
            return
    _record_action(
        actions,
        state,
        would=dry_run,
        handler="3",
        kind="advance_pick_gate",
        board_slug=board_slug,
        target_id=next_candidate.task_id,
        detail=detail,
        key=key,
    )


def _handle_h4(
    config: ControllerConfig,
    actions: list[Action],
    state: dict[str, object],
    board_slug: str,
    task_id: str,
    *,
    title: str,
    status: str,
    dry_run: bool,
    runner: NativeRunner,
) -> None:
    """H4: write the missing block event for a blocked-without-event task."""
    key = _action_key("guard", board_slug, task_id)
    if _is_handled(state, dry_run, key):
        return
    if dry_run:
        detail = (
            f"would write missing block event for {task_id} "
            f"(status {status}, no blocked event; reason: {config.watcher.guard_reason})"
        )
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="4",
            kind="write_block_event",
            board_slug=board_slug,
            target_id=task_id,
            detail=detail,
            key=key,
        )
        return
    result = runner(
        _native_command(
            config,
            board_slug,
            ["block", task_id, config.watcher.guard_reason, "--kind", "needs_input"],
        )
    )
    if result.returncode != 0 and status == "blocked":
        # The native block verb only fires from running/ready; a task still
        # parked in 'blocked' needs an unblock first so the guard event can
        # be written (deterministic two-step, recurrence counter stays 1).
        runner(_native_command(config, board_slug, ["unblock", task_id]))
        result = runner(
            _native_command(
                config,
                board_slug,
                ["block", task_id, config.watcher.guard_reason, "--kind", "needs_input"],
            )
        )
    if result.returncode != 0:
        # Failed mutation: report it but do NOT consume the action key, so
        # the guard stays eligible and is retried on the next pass.
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="4",
            kind="write_block_event",
            board_slug=board_slug,
            target_id=task_id,
            detail=(
                f"FAILED write block event for {task_id}: exit {result.returncode}: "
                f"{(result.stderr or result.stdout).strip()[:300]}"
            ),
        )
        return
    detail = f"wrote missing block event for {task_id} (queueing guard)"
    _record_action(
        actions,
        state,
        would=dry_run,
        handler="4",
        kind="write_block_event",
        board_slug=board_slug,
        target_id=task_id,
        detail=detail,
        key=key,
    )


def _handle_h5_deadlock(
    config: ControllerConfig,
    connection: sqlite3.Connection,
    actions: list[Action],
    state: dict[str, object],
    board: Board,
    deadlock: ReviewRequiredDeadlock,
    *,
    dry_run: bool,
    runner: NativeRunner,
) -> None:
    """H5: archive a review-required deadlock parent (live state scan).

    Fail-closed at decision time: the parent must still be blocked and its
    review child must still be stuck in ``todo`` — the episode may have
    resolved between discovery and the mutation.  The archive key is the
    block episode (``archive:<board>:<parent>:<blocked_event_id>``), so a
    double run never archives twice and a later, genuinely new block episode
    re-fires.
    """
    board_slug = board.slug
    key = _action_key(
        "deadlock", board_slug, deadlock.task_id, str(deadlock.blocked_event_id)
    )
    if _is_handled(state, dry_run, key):
        return
    row = _task_row(connection, deadlock.task_id)
    if row is None or _optional_text(row["status"]) != "blocked":
        return
    stuck = _stuck_review_children(connection, deadlock.task_id, config)
    if not stuck:
        return
    children = ", ".join(stuck)
    if dry_run:
        detail = (
            f"would archive review-required deadlock parent {deadlock.task_id} "
            f"(episode {deadlock.blocked_event_id}); review child {children} "
            f"stuck in todo; reason: {deadlock.reason[:160]} — archive command: "
            f"hermes kanban --board {board_slug} archive {deadlock.task_id}"
        )
        _record_action(
            actions,
            state,
            would=dry_run,
            handler="5",
            kind="archive_review_required_parent",
            board_slug=board_slug,
            target_id=deadlock.task_id,
            detail=detail,
            key=key,
        )
        return
    result = runner(
        _native_command(config, board_slug, ["archive", deadlock.task_id])
    )
    if result.returncode != 0:
        # Failed mutation: report it but do NOT consume the action key, so
        # the deadlock stays eligible and is retried on the next pass.
        _record_action(
            actions,
            state,
            would=False,
            handler="5",
            kind="archive_review_required_parent",
            board_slug=board_slug,
            target_id=deadlock.task_id,
            detail=(
                f"FAILED archive review-required deadlock parent "
                f"{deadlock.task_id}: exit {result.returncode}: "
                f"{(result.stderr or result.stdout).strip()[:300]}"
            ),
        )
        return
    detail = (
        f"archived review-required deadlock parent {deadlock.task_id} "
        f"(episode {deadlock.blocked_event_id}); review child {children} "
        f"promoted (archived counts as satisfied in recompute_ready)"
    )
    _record_action(
        actions,
        state,
        would=False,
        handler="5",
        kind="archive_review_required_parent",
        board_slug=board_slug,
        target_id=deadlock.task_id,
        detail=detail,
        key=key,
    )


def format_message(actions: Sequence[Action]) -> str:
    """Render the digest; empty string when there is nothing new."""
    if not actions:
        return ""
    lines: list[str] = []
    for action in actions:
        prefix = "watcher dry-run: " if action.would else "watcher: "
        lines.append(
            f"{prefix}H{action.handler} {action.kind} {action.board_slug} "
            f"{action.target_id}: {action.detail}"
        )
    return "\n".join(lines)


__all__ = [
    "Action",
    "COMPLETION_EVIDENCE_PATTERN",
    "DefectBlock",
    "FIX_PREFIX",
    "FixCardPlan",
    "GitRunner",
    "MissingBlockEvent",
    "NativeRunner",
    "PickGateCandidate",
    "REVIEW_PREFIX",
    "REVIEW_REQUIRED_PREFIX",
    "ReviewRequiredDeadlock",
    "STATE_FILENAME",
    "SupersedeCandidate",
    "WatcherError",
    "board_has_capability_block",
    "build_fix_card_body",
    "build_watcher_wiring",
    "consume_board_events",
    "default_state_path",
    "defect_severity",
    "discover_defect_blocks",
    "discover_missing_block_events",
    "discover_pending_promotions",
    "discover_pick_gate_candidates",
    "discover_review_required_deadlocks",
    "discover_supersede_candidates",
    "existing_fix_cards",
    "extract_shas",
    "extract_task_ids",
    "fix_card_title",
    "format_message",
    "git_repo_root",
    "has_open_fix_card",
    "is_reviewer_assignee",
    "pick_gate_skip_reason",
    "plan_fix_card",
    "resolve_fix_workspace",
    "run",
    "select_pick_gate",
    "verify_merged",
]
