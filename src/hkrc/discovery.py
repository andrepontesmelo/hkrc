"""Read-only native-board blocker discovery and controller reservations."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import quote

from .state import ControllerState


RECENCY_WINDOW_SECONDS = 3600
ELIGIBLE_BLOCKER_KINDS = frozenset(
    {"capability", "crashed", "timed_out", "protocol_violation", "gave_up"}
)
SKIPPED_BLOCKER_KINDS = frozenset({"needs_input", "transient", "dependency"})
# A child of a done/blocked parent is flagged only after it has been sitting
# unclaimed (latest event older than this window) so freshly created review
# cards get a chance to be dispatched before the controller alerts.  This is
# the default; instance config ``[discovery] unclaimed_child_after_seconds``
# overrides it at runtime.
UNCLAIMED_CHILD_WINDOW_SECONDS = 1800
UNCLAIMED_CHILD_KIND = "unclaimed_child"


class DiscoveryError(RuntimeError):
    """Raised when a configured native board cannot be inspected safely."""


@dataclass(frozen=True, slots=True)
class Board:
    slug: str
    path: Path


@dataclass(frozen=True, slots=True)
class BlockerCandidate:
    board_slug: str
    task_id: str
    title: str
    status: str
    block_kind: str | None
    latest_event_kind: str
    latest_event_at: int

    @property
    def kind_label(self) -> str:
        """Return a stable classification for reservation/output decisions."""

        if not self.block_kind:
            return "missing"
        if self.block_kind in ELIGIBLE_BLOCKER_KINDS:
            return self.block_kind
        if self.block_kind in SKIPPED_BLOCKER_KINDS:
            return self.block_kind
        return "unknown"

    @property
    def raw_block_kind(self) -> str | None:
        """Return the native typed kind, preserving ``None`` for legacy rows."""

        return self.block_kind

    @property
    def eligible(self) -> bool:
        return self.kind_label not in SKIPPED_BLOCKER_KINDS


@dataclass(frozen=True, slots=True)
class Resolution:
    candidate: BlockerCandidate
    action: str
    reason: str

    def stdout_line(self) -> str:
        kind = self.candidate.block_kind or "missing"
        return (
            f"board_slug={self.candidate.board_slug} "
            f"task_id={self.candidate.task_id} "
            f"action={self.action} kind={kind} "
            f"latest_event_at={self.candidate.latest_event_at} "
            f"reason={self.reason}"
        )


@dataclass(frozen=True, slots=True)
class UnclaimedChildCandidate:
    """A todo/ready child whose parent is done or blocked.

    The parent's block kind is intentionally not inspected: a parent blocked
    with ``needs_input`` (e.g. review-required) is exactly the historical
    stall this rule exists to catch, while the parent itself remains skipped
    by the existing blocker rule (never reserved for unblocking).
    """

    board_slug: str
    task_id: str
    title: str
    status: str
    assignee: str | None
    parent_task_id: str
    parent_title: str
    parent_status: str
    parent_block_kind: str | None
    latest_event_at: int

    @property
    def kind_label(self) -> str:
        return UNCLAIMED_CHILD_KIND

    @property
    def raw_block_kind(self) -> str | None:
        return None

    @property
    def eligible(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class ChildResolution:
    candidate: UnclaimedChildCandidate
    action: str
    reason: str

    def stdout_line(self) -> str:
        return (
            f"board_slug={self.candidate.board_slug} "
            f"task_id={self.candidate.task_id} "
            f"action={self.action} kind={UNCLAIMED_CHILD_KIND} "
            f"parent_task_id={self.candidate.parent_task_id} "
            f"parent_status={self.candidate.parent_status} "
            f"child_status={self.candidate.status} "
            f"latest_event_at={self.candidate.latest_event_at} "
            f"reason={self.reason}"
        )


def discover_boards(native_boards_root: Path) -> tuple[Board, ...]:
    """Find non-archived board directories without touching native databases."""

    root = Path(native_boards_root).expanduser()
    if not root.is_dir():
        raise DiscoveryError(f"native boards root not found or not a directory: {root}")

    boards: list[Board] = []
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        if not path.is_dir() or not (path / "kanban.db").is_file():
            continue
        metadata_path = path / "board.json"
        metadata: dict[str, object] = {}
        if metadata_path.is_file():
            try:
                raw = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise DiscoveryError(f"invalid board metadata: {metadata_path}") from exc
            if not isinstance(raw, dict):
                raise DiscoveryError(f"board metadata must be an object: {metadata_path}")
            metadata = raw
        if metadata.get("archived") is True or path.name == "_archived":
            continue
        slug = metadata.get("slug", path.name)
        if not isinstance(slug, str) or not slug:
            raise DiscoveryError(f"board metadata has invalid slug: {metadata_path}")
        boards.append(Board(slug=slug, path=path))
    return tuple(boards)


def discover_candidates(
    native_boards_root: Path,
    *,
    now: int | None = None,
    window_seconds: int | None = RECENCY_WINDOW_SECONDS,
) -> tuple[BlockerCandidate, ...]:
    """Read recent blocked tasks from every non-archived native board.

    Native connections use SQLite's read-only URI mode and ``query_only``.  The
    latest event is selected by ``created_at`` and then event id, because native
    ``tasks`` has no ``updated_at`` column.

    ``window_seconds`` is the effective recency window: a blocked task is a
    candidate when its latest event is at most that old.  ``None`` disables the
    lower bound entirely (full backfill).  Blocked tasks whose latest event is
    older than the window are not returned here; see ``discover_stale_blockers``
    for the informational counterpart that keeps them visible instead of
    silently dropping them.
    """

    current_time = int(time.time()) if now is None else int(now)
    cutoff = None if window_seconds is None else current_time - int(window_seconds)
    return tuple(
        candidate
        for candidate in _scan_blocked_tasks(native_boards_root, current_time)
        if cutoff is None or candidate.latest_event_at >= cutoff
    )


def discover_stale_blockers(
    native_boards_root: Path,
    *,
    now: int | None = None,
    window_seconds: int | None = RECENCY_WINDOW_SECONDS,
) -> tuple[BlockerCandidate, ...]:
    """Return blocked tasks whose latest event is outside the effective window.

    This is the informational counterpart of ``discover_candidates``: a blocked
    task that exists but is older than the effective recency window is reported
    here instead of being silently omitted, so callers can emit a visible note
    (``stale_blocker_note``) pointing the operator at ``--backfill``.  No
    reservation or other controller state is written for these tasks; the scan
    remains read-only.  A ``window_seconds`` of ``None`` (full backfill) has no
    stale tasks by definition.
    """

    current_time = int(time.time()) if now is None else int(now)
    if window_seconds is None:
        return ()
    cutoff = current_time - int(window_seconds)
    return tuple(
        candidate
        for candidate in _scan_blocked_tasks(native_boards_root, current_time)
        if candidate.latest_event_at < cutoff
    )


def stale_blocker_note(candidate: BlockerCandidate, *, now: int | None = None) -> str:
    """Render a visible note for a blocked task outside the recency window.

    The note is informational only: the candidate was not reserved and no state
    was written.  The operator can re-run with ``--backfill`` to include it.
    """

    current_time = int(time.time()) if now is None else int(now)
    seconds = max(0, current_time - candidate.latest_event_at)
    return (
        f"note board_slug={candidate.board_slug} task_id={candidate.task_id} "
        f"status=blocked blocked_seconds_ago={seconds} "
        f"hint=\"blocked {_humanize_duration(seconds)} ago — "
        "outside recency window, use --backfill\""
    )


def _humanize_duration(seconds: int) -> str:
    """Render a duration compactly (e.g. 18000 -> '5h', 90 -> '1m')."""

    seconds = int(max(0, seconds))
    for unit, span in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= span:
            return f"{seconds // span}{unit}"
    return f"{seconds}s"


def _scan_blocked_tasks(
    native_boards_root: Path, current_time: int
) -> tuple[BlockerCandidate, ...]:
    """Read every blocked task whose latest event is at or before ``current_time``."""

    candidates: list[BlockerCandidate] = []
    for board in discover_boards(native_boards_root):
        connection = _open_native_read_only(board.path / "kanban.db")
        try:
            rows = connection.execute(
                """
                SELECT t.id, t.title, t.status, t.block_kind,
                       latest.kind AS latest_event_kind,
                       latest.created_at AS latest_event_at
                  FROM tasks AS t
                  JOIN task_events AS latest
                    ON latest.id = (
                        SELECT e.id
                          FROM task_events AS e
                         WHERE e.task_id = t.id
                         ORDER BY e.created_at DESC, e.id DESC
                         LIMIT 1
                    )
                 WHERE t.status = 'blocked'
                   AND latest.created_at <= ?
                 ORDER BY t.id ASC
                """,
                (current_time,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise DiscoveryError(f"cannot query native board {board.slug}: {exc}") from exc
        finally:
            connection.close()
        for row in rows:
            # ``block_kind`` is the native task classification.  Event kind is
            # retained separately and never overrides a missing task's kind.
            task_kind = row["block_kind"]
            effective_kind = str(task_kind) if task_kind else None
            latest_event_kind = str(row["latest_event_kind"])
            candidates.append(
                BlockerCandidate(
                    board_slug=board.slug,
                    task_id=str(row["id"]),
                    title=str(row["title"]),
                    status=str(row["status"]),
                    block_kind=effective_kind,
                    latest_event_kind=latest_event_kind,
                    latest_event_at=int(row["latest_event_at"]),
                )
            )
    return tuple(candidates)


def discover_unclaimed_children(
    native_boards_root: Path,
    *,
    now: int | None = None,
    unclaimed_after: int | None = None,
) -> tuple[UnclaimedChildCandidate, ...]:
    """Find todo/ready children of done or blocked parents.

    The scan is read-only like blocker discovery.  For each native child of a
    terminal/stuck parent the child's latest event (at or before ``now``) must
    be older than the unclaimed window: a freshly created review card gets time
    to be dispatched before the controller alerts.

    A child is claimed (and never flagged) when it has an in-flight run
    (``current_run_id``) or a non-null ``claim_lock``.  Claim semantics match
    the native Hermes dispatcher exactly: a task is claimable only while
    ``claim_lock IS NULL``, and the native reclaim path clears both the lock
    and its expiry together.  A non-null lock with a null expiry is therefore
    malformed/active and skipped, not guessed as expired.

    A parent blocked with ``needs_input``/``transient``/``dependency`` is still
    a "blocked parent" for this rule: the existing blocker rule keeps skipping
    the parent itself (never reserved for unblocking), but an unclaimed child
    of such a parent is exactly the review-gate stall the controller missed.

    Legacy boards without a ``task_links`` table retain the old blocked-only
    behavior: they contribute no child candidates.  A present but malformed
    table raises ``DiscoveryError`` rather than silently producing a false
    negative.
    """

    current_time = int(time.time()) if now is None else int(now)
    window = UNCLAIMED_CHILD_WINDOW_SECONDS if unclaimed_after is None else int(unclaimed_after)
    cutoff = current_time - window
    candidates: list[UnclaimedChildCandidate] = []
    for board in discover_boards(native_boards_root):
        connection = _open_native_read_only(board.path / "kanban.db")
        try:
            links_table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'task_links'"
            ).fetchone()
            if links_table is None:
                continue
            rows = connection.execute(
                """
                SELECT c.id AS child_id, c.title AS child_title,
                       c.status AS child_status, c.assignee AS child_assignee,
                       p.id AS parent_id, p.title AS parent_title,
                       p.status AS parent_status, p.block_kind AS parent_block_kind,
                       latest.created_at AS latest_event_at
                  FROM tasks AS c
                  JOIN task_links AS l ON l.child_id = c.id
                  JOIN tasks AS p ON p.id = l.parent_id
                  JOIN task_events AS latest ON latest.id = (
                      SELECT e.id
                        FROM task_events AS e
                       WHERE e.task_id = c.id
                         AND e.created_at <= ?
                       ORDER BY e.created_at DESC, e.id DESC
                       LIMIT 1
                  )
                 WHERE c.status IN ('todo', 'ready')
                   AND c.current_run_id IS NULL
                   AND c.claim_lock IS NULL
                   AND p.status IN ('done', 'blocked')
                   AND latest.created_at < ?
                   AND NOT EXISTS (
                       SELECT 1
                         FROM task_links AS l2
                         JOIN tasks AS p2 ON p2.id = l2.parent_id
                        WHERE l2.child_id = c.id
                          AND p2.status NOT IN ('done', 'archived', 'blocked')
                   )
                 ORDER BY c.id ASC, p.id ASC
                """,
                (current_time, cutoff),
            ).fetchall()
        except sqlite3.Error as exc:
            raise DiscoveryError(f"cannot query native board {board.slug}: {exc}") from exc
        finally:
            connection.close()
        seen: set[str] = set()
        for row in rows:
            child_id = str(row["child_id"])
            if child_id in seen:
                continue
            seen.add(child_id)
            candidates.append(
                UnclaimedChildCandidate(
                    board_slug=board.slug,
                    task_id=child_id,
                    title=str(row["child_title"]),
                    status=str(row["child_status"]),
                    assignee=str(row["child_assignee"]) if row["child_assignee"] else None,
                    parent_task_id=str(row["parent_id"]),
                    parent_title=str(row["parent_title"]),
                    parent_status=str(row["parent_status"]),
                    parent_block_kind=(
                        str(row["parent_block_kind"]) if row["parent_block_kind"] else None
                    ),
                    latest_event_at=int(row["latest_event_at"]),
                )
            )
    return tuple(candidates)


def discover_and_reserve(
    native_boards_root: Path,
    state: ControllerState,
    *,
    now: int | None = None,
    unclaimed_after: int | None = None,
    window_seconds: int | None = RECENCY_WINDOW_SECONDS,
) -> tuple[Resolution | ChildResolution, ...]:
    """Discover candidates and atomically reserve each eligible item once."""

    resolutions: list[Resolution | ChildResolution] = []
    for candidate in discover_candidates(
        native_boards_root, now=now, window_seconds=window_seconds
    ):
        if not candidate.eligible:
            resolution = Resolution(candidate, "skipped", "blocker_kind_not_recoverable")
            state.record_resolution(
                candidate.board_slug,
                candidate.task_id,
                blocker_kind=candidate.block_kind,
                action=resolution.action,
                reason=resolution.reason,
                latest_event_at=candidate.latest_event_at,
            )
            resolutions.append(resolution)
            continue
        reserved = state.reserve_blocker(
            candidate.board_slug,
            candidate.task_id,
            blocker_kind=candidate.block_kind,
            latest_event_at=candidate.latest_event_at,
        )
        resolution = Resolution(
            candidate,
            "reserved" if reserved else "already_reserved",
            "one_ever_reservation" if reserved else "reservation_exists",
        )
        state.record_resolution(
            candidate.board_slug,
            candidate.task_id,
            blocker_kind=candidate.block_kind,
            action=resolution.action,
            reason=resolution.reason,
            latest_event_at=candidate.latest_event_at,
        )
        resolutions.append(resolution)
    for candidate in discover_unclaimed_children(
        native_boards_root, now=now, unclaimed_after=unclaimed_after
    ):
        reserved = state.reserve_blocker(
            candidate.board_slug,
            candidate.task_id,
            blocker_kind=UNCLAIMED_CHILD_KIND,
            latest_event_at=candidate.latest_event_at,
        )
        resolution = ChildResolution(
            candidate,
            "reserved" if reserved else "already_reserved",
            "one_ever_unclaimed_child_alert" if reserved else "reservation_exists",
        )
        state.record_resolution(
            candidate.board_slug,
            candidate.task_id,
            blocker_kind=UNCLAIMED_CHILD_KIND,
            action=resolution.action,
            reason=resolution.reason,
            latest_event_at=candidate.latest_event_at,
        )
        resolutions.append(resolution)
    return tuple(resolutions)


def _open_native_read_only(path: Path) -> sqlite3.Connection:
    """Open native SQLite without a writable journal or native-file mutation."""

    # SQLite's ordinary ``mode=ro`` may create ``-wal``/``-shm`` sidecars when
    # opening a rollback-journal database.  The native Hermes daemon already owns
    # those files, but this controller must not create or alter them.  Hermes' live
    # board files are WAL-backed; fail closed if a required sidecar is missing rather
    # than causing SQLite to create one during a read-only inspection.
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    wal_path = path.with_name(f"{path.name}-wal")
    shm_path = path.with_name(f"{path.name}-shm")
    sidecars_exist = wal_path.exists() or shm_path.exists()
    if not sidecars_exist:
        # With no WAL sidecars, the main database is a complete closed snapshot.
        # ``immutable=1`` prevents SQLite from creating lock/journal sidecars
        # while opening it read-only.
        uri = f"file:{quote(str(path), safe='/')}?mode=ro&immutable=1"
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            return connection
        except sqlite3.Error as exc:
            raise DiscoveryError(f"cannot open native board immutable read-only: {path}") from exc
    # A live WAL database cannot be opened safely by this process.  Even a
    # ``mode=ro`` connection may update SQLite's shared-memory index while it
    # joins the writer's WAL snapshot.  Checking mtimes around that operation
    # is insufficient: the bytes can change without an mtime change, and a
    # native board is an immutable integration boundary for this controller.
    # Fail closed before opening SQLite, leaving the live writer's database,
    # WAL, and shared-memory sidecars entirely untouched.
    if wal_path.is_file() and shm_path.is_file():
        raise DiscoveryError(f"native board has a live WAL snapshot; refusing to open: {path}")
    raise DiscoveryError(f"native WAL sidecars are incomplete: {path}")


__all__ = [
    "BlockerCandidate",
    "Board",
    "ChildResolution",
    "DiscoveryError",
    "ELIGIBLE_BLOCKER_KINDS",
    "RECENCY_WINDOW_SECONDS",
    "Resolution",
    "SKIPPED_BLOCKER_KINDS",
    "UNCLAIMED_CHILD_KIND",
    "UNCLAIMED_CHILD_WINDOW_SECONDS",
    "UnclaimedChildCandidate",
    "discover_and_reserve",
    "discover_boards",
    "discover_candidates",
    "discover_stale_blockers",
    "discover_unclaimed_children",
    "stale_blocker_note",
]
