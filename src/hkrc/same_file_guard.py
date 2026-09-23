"""Same-file guard: block overlapping implementation cards on one file.

On 2026-09-15 six implementation cards dispatched 08:01-08:07 all edited
``src/hkrc/harness_loop.py`` (the 9k-line repo hotspot): four rebase-conflict
defects, five extra fix + re-review card runs, nine merges in one day, 135M
input tokens (vs a typical 2-42M) that exhausted the opencode-go monthly
quota on two accounts. The rule already existed in the repo-root
``AGENTS.md`` ("Same-file tasks serialize... Never dispatch two tasks that
edit one file") but the orchestrator that fanned the cards out had no mechanical
gate, and HKRC's own ``_kanban_create`` is budget-capped (1-2 pairs/night)
so a gate inside HKRC's creation path would not have caught this. The only
HKRC lever that reaches live board state is a watcher — the pattern HKRC
already runs three of (needs-input, stale-block, review-gap).

Hard constraint — CLI only, NO sqlite
-------------------------------------
Never open hermes kanban sqlite databases directly. All kanban reads AND
writes go through the ``hermes kanban`` CLI (``boards list --json``, ``list
--json``, ``show <id> --json``, ``block <id> --reason``, ``comment <id>``),
exactly like ``review_gap.py``. Every native CLI subprocess runs with
``HERMES_KANBAN_*`` and ``_HERMES_GATEWAY`` removed from its environment so
the pinned board env from the dispatcher can never override the ``--board``
flag (see ``build_native_environment``).

Detection logic (per tick)
--------------------------
1. Boards: ``hermes kanban boards list --json``. All non-archived boards.
2. Candidate cards per board: ``hermes kanban --board <slug> list --json``
   filtered to open implementation states — ``ready``, ``running``,
   ``review``, ``todo`` — with an assignee other than ``reviewer`` (review
   cards do not edit files). Done/archived/blocked cards are skipped.
3. Declared paths: a regex over each card's body extracts repo-relative
   paths (``path_pattern`` config). Paths are normalized (leading ``./``
   stripped) and deduped. ``ignored_paths`` (config, default
   ``["scripts/green.sh"]``) removes gate/test-runner references that appear
   in nearly every card body and would produce universal false overlap.
   A card declaring ZERO paths is INVISIBLE to the guard — one digest line
   nags it (never a block) so the operator can amend the body.
4. Overlap: two candidates overlap when they declare at least one identical
   path. On overlap the earlier-created card is the HOLDER; every later card
   is a FOLLOWER.
5. Exemptions — a follower is never blocked when EITHER holds:
   - Dependency-linked: the follower can reach the holder through the
     parent/child graph (directly or transitively, from ``show --json``
     ``parents``/``children``). A "fix findings for t_X" card legitimately
     edits the same file as t_X; Hermes already serializes them.
   - Too young: the follower's ``created_at`` is younger than
     ``min_age_seconds`` (default 120s) — don't race a card mid-creation.

Action (non-exempt overlap)
---------------------------
With ``auto_block`` (default) the follower is blocked via
``hermes kanban block <id> --reason "same-file-overlap: <path> held by
<holder_id>"`` and a comment naming the other card and the shared path is
appended to BOTH cards. With ``auto_block`` false — or ``--dry-run`` — the
CLI mutation is skipped and the intent is reported only. ``--dry-run``
changes NOTHING on the board, ever.

Output contract (watchdog style)
--------------------------------
Telegram-ready digest: one line per blocked pair, nag, or error. Empty
stdout = silent (nothing to report). Exit 0 on success. Dedupe: state JSON
keyed ``"<board>:<follower>:<holder>:<path>"`` so the same overlap is acted
on and reported ONCE, never every tick. Corrupt state fails closed (raises
``SameFileGuardError`` so cron delivers an error alert).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import time

from .config import ControllerConfig, SameFileGuardConfig
from .handoff import NativeResult

STATE_FILENAME = "same-file-guard-state.json"
# Open implementation states only: these cards will edit files. Done,
# archived and blocked cards never merge anything new.
CANDIDATE_STATUSES = frozenset({"ready", "running", "review", "todo"})
REVIEWER_ASSIGNEE = "reviewer"
# Broadened default in 0.15.19: the 2026-09-15 corpus pulled the shared path
# from only 5 of 6 colliding cards — t_85199c5e declared none. The widened
# extensions and the bare-module tokens below catch it and similar bodies.
DEFAULT_PATH_PATTERN = (
    r"(?:src|tests|scripts|config|docs)/[A-Za-z0-9_./-]+\.(?:py|md|json|toml|sh|ya?ml)"
    r"|src/hkrc/[A-Za-z0-9_./-]+\.(?:py|md)"
    r"|\b(?:src|tests|scripts|config)/[A-Za-z0-9_./-]+"
)
_NAG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_./-]+\.(?:py|md|json|toml|sh|ya?ml)\b")

CliRunner = Callable[[Sequence[str]], NativeResult]


class SameFileGuardError(RuntimeError):
    """Raised when the same-file guard cannot inspect or mutate the board safely."""


@dataclass(frozen=True, slots=True)
class BoardInfo:
    """One non-archived kanban board."""

    slug: str


@dataclass(frozen=True, slots=True)
class GuardCandidate:
    """One open implementation card with its declared file paths."""

    board_slug: str
    task_id: str
    title: str
    assignee: str | None
    created_at: int
    status: str
    paths: tuple[str, ...]
    declares_anything: bool = False

    @property
    def key(self) -> str:
        """Dedupe key: ``<board>:<task_id>``."""
        return f"{self.board_slug}:{self.task_id}"


@dataclass(frozen=True, slots=True)
class Overlap:
    """One holder/follower pair sharing at least one declared path."""

    board_slug: str
    holder: GuardCandidate
    follower: GuardCandidate
    shared_paths: tuple[str, ...]

    @property
    def state_key(self) -> str:
        """Dedupe key scoped to the pair AND path."""
        return (
            f"{self.board_slug}:{self.follower.task_id}:"
            f"{self.holder.task_id}:{self.shared_paths[0]}"
        )


def default_state_path(state_db: Path) -> Path:
    """Controller-owned state file next to the controller state database."""
    return state_db.parent / STATE_FILENAME


# --- native CLI execution (mirrors review_gap) -------------------------------


_NATIVE_ENV_STRIP_PREFIXES = ("HERMES_KANBAN_",)
_NATIVE_ENV_STRIP_EXACT = (
    "_HERMES_GATEWAY",
    # A delegated child (hermes delegate/kanban worker) is refused kanban
    # reads/writes by the CLI ("delegate_task child contexts cannot mutate
    # Kanban tasks or boards") — the watcher must never inherit that context,
    # or a cron tick launched from a delegated session scans nothing.
    "HERMES_DELEGATED_CHILD_CONTEXT",
)


def build_native_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build the native CLI environment with explicit HOME/HERMES_HOME.

    Every ``HERMES_KANBAN_*`` variable and ``_HERMES_GATEWAY`` are removed so
    the ``--board`` flag — not the ambient dispatcher env — selects the board.
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
    """Run one native CLI invocation as an argv list; ``runner`` exists for tests."""
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
        raise SameFileGuardError(f"native CLI timed out after {timeout}s") from exc
    except OSError as exc:
        return NativeResult(127, "", str(exc))


def _parse_json_stdout(result: NativeResult, what: str) -> object:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise SameFileGuardError(f"{what} failed (exit {result.returncode}): {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SameFileGuardError(f"{what} returned unparseable JSON: {exc}") from exc


# --- boards / tasks through the CLI ------------------------------------------


def discover_boards(
    cli: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[BoardInfo]:
    """Return every non-archived board via ``hermes kanban boards list --json``."""
    result = run_native(
        [cli, "kanban", "boards", "list", "--json"], runner=runner, timeout=timeout
    )
    data = _parse_json_stdout(result, "kanban boards list")
    if not isinstance(data, list):
        raise SameFileGuardError("kanban boards list must return a JSON array")
    boards: list[BoardInfo] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if entry.get("archived") is True:
            continue
        slug = entry.get("slug")
        if not isinstance(slug, str) or not slug.strip():
            continue
        boards.append(BoardInfo(slug=slug))
    return boards


def list_tasks(
    cli: str, slug: str, *, runner: CliRunner | None = None, timeout: float | None = None
) -> list[dict]:
    """Return every non-archived task on a board via ``list --json``."""
    result = run_native(
        [cli, "kanban", "--board", slug, "list", "--json"],
        runner=runner,
        timeout=timeout,
    )
    data = _parse_json_stdout(result, f"kanban list on board {slug}")
    if not isinstance(data, list):
        raise SameFileGuardError(f"kanban list on board {slug} must return a JSON array")
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
        raise SameFileGuardError(f"kanban show {task_id} on board {slug} must return an object")
    return data


# --- path extraction ----------------------------------------------------------


def _normalize_path(raw: str) -> str:
    """Strip a leading ``./`` and trailing slashes; dedupe handled by callers."""
    path = raw.strip()
    while path.startswith("./"):
        path = path[2:]
    return path.rstrip("/")


def declared_paths(
    body: str,
    *,
    path_pattern: str,
    ignored_paths: Sequence[str] = (),
) -> tuple[str, ...]:
    """Extract normalized, deduped repo-relative paths from a card body.

    ``ignored_paths`` removes gate/test-runner references that appear in
    nearly every card body (``scripts/green.sh`` is quoted as the gate
    command and would produce universal false overlap).
    """
    try:
        pattern = re.compile(path_pattern)
    except re.error as exc:
        raise SameFileGuardError(f"same_file_guard path_pattern is invalid: {exc}") from exc
    ignored = {_normalize_path(p) for p in ignored_paths}
    paths: list[str] = []
    seen: set[str] = set()
    for match in pattern.finditer(body or ""):
        path = _normalize_path(match.group(0))
        if not path or path in ignored or path in seen:
            continue
        seen.add(path)
        paths.append(path)
    return tuple(paths)


def declares_anything(
    body: str,
    *,
    path_pattern: str,
    ignored_paths: Sequence[str] = (),
) -> bool:
    """True when the body matches the pattern at all, ignored paths included.

    A card whose only pattern hits are ``ignored_paths`` (the gate command
    quoted in nearly every body) is INVISIBLE to the guard by design, not
    silent-by-omission — it must not trigger the zero-path nag.
    """
    try:
        pattern = re.compile(path_pattern)
    except re.error as exc:
        raise SameFileGuardError(f"same_file_guard path_pattern is invalid: {exc}") from exc
    return pattern.search(body or "") is not None


def _nag_tokens(body: str) -> str:
    """First bare file-looking token in a body with zero declared paths."""
    match = _NAG_TOKEN_RE.search(body or "")
    return match.group(0) if match else ""


# --- candidate filtering -------------------------------------------------------


def is_candidate(task: Mapping[str, object], *, now: int, min_age_seconds: int | float) -> bool:
    """True when an open card is implementation work old enough to judge.

    Open implementation states only, never a ``reviewer`` assignee, and
    created at least ``min_age_seconds`` ago so the guard never races a card
    mid-creation.
    """
    status = task.get("status")
    if not isinstance(status, str) or status not in CANDIDATE_STATUSES:
        return False
    assignee = task.get("assignee")
    if isinstance(assignee, str) and assignee.strip() == REVIEWER_ASSIGNEE:
        return False
    task_id = task.get("id")
    if not isinstance(task_id, str) or not task_id:
        return False
    created_at = task.get("created_at")
    if not isinstance(created_at, int) or isinstance(created_at, bool):
        return False  # unknown age — leave for a later tick, never block blind
    if int(now) - created_at < int(min_age_seconds):
        return False
    return True


def candidate_from_show(
    slug: str,
    task_id: str,
    show: Mapping[str, object],
    *,
    path_pattern: str,
    ignored_paths: Sequence[str],
) -> GuardCandidate:
    """Build a candidate from a full ``show --json`` document (body included)."""
    task = show.get("task")
    task_map: Mapping[str, object] = task if isinstance(task, Mapping) else {}
    body = task_map.get("body")
    body_text = body if isinstance(body, str) else ""
    paths = declared_paths(
        body_text,
        path_pattern=path_pattern,
        ignored_paths=ignored_paths,
    )
    status = task_map.get("status")
    assignee = task_map.get("assignee")
    created_at = task_map.get("created_at")
    return GuardCandidate(
        board_slug=slug,
        task_id=task_id,
        title=str(task_map.get("title") or task_id),
        assignee=assignee if isinstance(assignee, str) else None,
        created_at=created_at if isinstance(created_at, int) and not isinstance(created_at, bool) else 0,
        status=status if isinstance(status, str) else "",
        paths=paths,
        declares_anything=declares_anything(
            body_text, path_pattern=path_pattern, ignored_paths=ignored_paths
        ),
    )


# --- dependency-chain exemption -------------------------------------------------


def _linked(
    show: Mapping[str, object],
) -> tuple[set[str], set[str]]:
    """Return the (parents, children) id sets from one ``show --json`` doc."""
    def ids(field: object) -> set[str]:
        if not isinstance(field, Sequence) or isinstance(field, (str, bytes)):
            return set()
        return {item for item in field if isinstance(item, str) and item}

    return ids(show.get("parents")), ids(show.get("children"))


def dependency_linked(
    holder_show: Mapping[str, object],
    follower_show: Mapping[str, object],
    shows: Mapping[str, Mapping[str, object]],
) -> bool:
    """True when holder and follower share a dependency chain, transitively.

    Undirected traversal over the parent/child graph: any path connecting the
    two cards (parent-links, child-links, or a chain through other cards)
    means Hermes already serializes them and the guard must not block.
    """
    start_ids: set[str] = set()
    for doc in (holder_show, follower_show):
        task = doc.get("task")
        task_id = task.get("id") if isinstance(task, Mapping) else None
        if isinstance(task_id, str) and task_id:
            start_ids.add(task_id)
    if len(start_ids) < 2:
        return False
    source, target = sorted(start_ids)[0], sorted(start_ids)[1]

    edges: dict[str, set[str]] = {}

    def add_edges(doc: Mapping[str, object]) -> None:
        task = doc.get("task")
        task_id = task.get("id") if isinstance(task, Mapping) else None
        if not isinstance(task_id, str) or not task_id:
            return
        parents, children = _linked(doc)
        for neighbor in parents | children:
            edges.setdefault(task_id, set()).add(neighbor)
            edges.setdefault(neighbor, set()).add(task_id)

    add_edges(holder_show)
    add_edges(follower_show)
    frontier = {source}
    visited = {source}
    while frontier:
        current = frontier.pop()
        if current == target:
            return True
        for neighbor in edges.get(current, ()):  # noqa: B007 — readability
            if neighbor in visited:
                continue
            visited.add(neighbor)
            frontier.add(neighbor)
            neighbor_doc = shows.get(neighbor)
            if neighbor_doc is not None:
                add_edges(neighbor_doc)
    return False


# --- action / digest lines ------------------------------------------------------


def block_reason(path: str, holder_id: str) -> str:
    """Native ``block --reason`` text for a same-file overlap."""
    return f"same-file-overlap: {path} held by {holder_id}"


def follower_comment(holder_id: str, path: str) -> str:
    return (
        f"same-file-overlap: blocked — this card declares {path}, also declared "
        f"by in-flight card {holder_id}. Same-file tasks serialize: parent-link "
        f"this card to {holder_id} (or its integration card) and wait, or split "
        "your target files."
    )


def holder_comment(follower_id: str, path: str) -> str:
    return (
        f"same-file-overlap: open card {follower_id} declares {path} and was "
        f"blocked pending this card. Serialize via parent-link or integration card."
    )


def blocked_line(follower_id: str, holder_id: str, path: str, board_slug: str) -> str:
    return (
        f"same-file-overlap: blocked {follower_id} on board {board_slug} — "
        f"{path} held by {holder_id}"
    )


def report_only_line(follower_id: str, holder_id: str, path: str, board_slug: str) -> str:
    return (
        f"same-file-overlap (report-only): {follower_id} overlaps {holder_id} "
        f"on {path} (board {board_slug}) — auto_block disabled"
    )


def nag_line(task_id: str, board_slug: str) -> str:
    return (
        f"same-file-guard: card {task_id} on board {board_slug} declares NO file "
        "paths in its body — invisible to the guard; amend the body to list "
        "target files"
    )


def error_line(board_slug: str, detail: str) -> str:
    return f"same-file-guard error on board {board_slug}: {detail}"


# --- state (dedupe) --------------------------------------------------------------


def _load_state(path: Path) -> dict[str, dict[str, object]]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SameFileGuardError(f"cannot read same-file-guard state {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise SameFileGuardError(f"same-file-guard state must be an object: {path}")
    state: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise SameFileGuardError(
                f"same-file-guard state values must be objects: {path} (key {key!r})"
            )
        at = value.get("at")
        if not isinstance(at, int) or isinstance(at, bool):
            raise SameFileGuardError(
                f"same-file-guard state value has an invalid timestamp: {path} (key {key!r})"
            )
        state[str(key)] = dict(value)
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
        raise SameFileGuardError(f"cannot persist same-file-guard state {path}: {exc}") from exc


# --- native mutation commands -----------------------------------------------------


def build_block_command(cli: str, slug: str, task_id: str, reason: str) -> list[str]:
    return [cli, "kanban", "--board", slug, "block", task_id, "--reason", reason]


def build_comment_command(cli: str, slug: str, task_id: str, body: str) -> list[str]:
    return [cli, "kanban", "--board", slug, "comment", task_id, "--body", body]


# --- main entry --------------------------------------------------------------------


def run(
    config: ControllerConfig,
    state_path: Path,
    *,
    now: int | None = None,
    dry_run: bool = False,
    runner: CliRunner | None = None,
) -> str:
    """Scan open implementation cards and block same-file overlaps.

    All kanban reads and writes go through the native CLI; the only other
    side effect is the atomic dedupe state file. Empty digest = silent.
    ``dry_run`` (or ``auto_block`` disabled) performs NO mutation — the
    intended block/comment is reported only. ``runner`` exists only for
    deterministic tests; production uses ``subprocess.run`` argv lists,
    never a shell command.
    """
    cfg: SameFileGuardConfig = config.same_file_guard
    if not cfg.enabled:
        return ""
    current_time = int(time.time()) if now is None else int(now)
    state = _load_state(state_path)
    cli = config.native_cli
    timeout = float(cfg.cli_timeout_seconds)
    lines: list[str] = []
    deadline = time.monotonic() + float(cfg.tick_timeout_seconds)
    boards = discover_boards(cli, runner=runner, timeout=timeout)
    for board in boards:
        slug = board.slug
        if time.monotonic() >= deadline:
            lines.append(error_line(slug, "tick budget exceeded — remaining boards skipped"))
            break
        try:
            listed = list_tasks(cli, slug, runner=runner, timeout=timeout)
        except SameFileGuardError as exc:
            lines.append(error_line(slug, str(exc)))
            continue
        candidate_ids = [
            str(task["id"])
            for task in listed
            if is_candidate(task, now=current_time, min_age_seconds=cfg.min_age_seconds)
        ]
        if not candidate_ids:
            continue
        # One show per candidate carries the body + parents/children in a
        # single subprocess; candidate statuses are trusted from list (the
        # show may race a state change — the LIST snapshot decides the tick).
        shows: dict[str, dict] = {}
        candidates: list[GuardCandidate] = []
        for task_id in candidate_ids:
            try:
                show = show_task(cli, slug, task_id, runner=runner, timeout=timeout)
            except SameFileGuardError as exc:
                lines.append(error_line(slug, str(exc)))
                continue
            shows[task_id] = show
            candidates.append(
                candidate_from_show(
                    slug,
                    task_id,
                    show,
                    path_pattern=cfg.path_pattern,
                    ignored_paths=cfg.ignored_paths,
                )
            )
        # Nag zero-path cards once (dedupe: never re-nag the same card).
        # A card whose only pattern hits are ignored_paths (the quoted gate
        # command) is invisible BY DESIGN — never nagged.
        for candidate in candidates:
            if candidate.paths or candidate.declares_anything:
                continue
            nag_key = f"{candidate.key}:nag"
            if nag_key in state:
                continue
            lines.append(nag_line(candidate.task_id, slug))
            if dry_run:
                continue  # dry-run mutates nothing and records nothing
            state[nag_key] = {"at": current_time}
        # Overlaps: older card holds, every later card is a follower.
        ordered = sorted(candidates, key=lambda c: (c.created_at, c.task_id))
        seen_pairs: set[tuple[str, str]] = set()
        for i, holder in enumerate(ordered):
            if not holder.paths:
                continue
            for follower in ordered[i + 1 :]:
                if not follower.paths:
                    continue
                shared = tuple(p for p in follower.paths if p in holder.paths)
                if not shared:
                    continue
                pair = (holder.task_id, follower.task_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                if dependency_linked(shows[holder.task_id], shows[follower.task_id], shows):
                    continue  # fix-findings chains are already serialized
                overlap = Overlap(
                    board_slug=slug,
                    holder=holder,
                    follower=follower,
                    shared_paths=shared,
                )
                if overlap.state_key in state:
                    continue  # acted on in a previous tick — dedupe holds
                path = overlap.shared_paths[0]
                if dry_run:
                    lines.append(
                        f"same-file-overlap (dry-run): would block {follower.task_id} "
                        f"on board {slug} — {path} held by {holder.task_id}"
                    )
                    continue  # dry-run mutates nothing and records nothing
                if not cfg.auto_block:
                    lines.append(report_only_line(follower.task_id, holder.task_id, path, slug))
                    state[overlap.state_key] = {"at": current_time}
                    continue
                reason = block_reason(path, holder.task_id)
                block_result = run_native(
                    build_block_command(cli, slug, follower.task_id, reason),
                    runner=runner,
                    timeout=timeout,
                )
                if block_result.returncode != 0:
                    detail = (block_result.stderr or block_result.stdout or "").strip()
                    lines.append(
                        error_line(
                            slug,
                            f"block {follower.task_id} failed (exit "
                            f"{block_result.returncode}): {detail}",
                        )
                    )
                    continue
                for comment_task, comment_body in (
                    (follower.task_id, follower_comment(holder.task_id, path)),
                    (holder.task_id, holder_comment(follower.task_id, path)),
                ):
                    comment_result = run_native(
                        build_comment_command(cli, slug, comment_task, comment_body),
                        runner=runner,
                        timeout=timeout,
                    )
                    if comment_result.returncode != 0:
                        detail = (
                            comment_result.stderr or comment_result.stdout or ""
                        ).strip()
                        lines.append(
                            error_line(
                                slug,
                                f"comment {comment_task} failed (exit "
                                f"{comment_result.returncode}): {detail}",
                            )
                        )
                lines.append(blocked_line(follower.task_id, holder.task_id, path, slug))
                state[overlap.state_key] = {"at": current_time}
    if not dry_run:
        _save_state(state_path, state)
    return "\n".join(lines)


__all__ = [
    "CANDIDATE_STATUSES",
    "DEFAULT_PATH_PATTERN",
    "REVIEWER_ASSIGNEE",
    "STATE_FILENAME",
    "BoardInfo",
    "CliRunner",
    "GuardCandidate",
    "Overlap",
    "SameFileGuardError",
    "block_reason",
    "blocked_line",
    "build_block_command",
    "build_comment_command",
    "candidate_from_show",
    "declared_paths",
    "default_state_path",
    "dependency_linked",
    "discover_boards",
    "error_line",
    "follower_comment",
    "holder_comment",
    "is_candidate",
    "list_tasks",
    "nag_line",
    "report_only_line",
    "run",
    "run_native",
    "show_task",
]
