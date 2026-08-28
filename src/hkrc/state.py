"""Controller-owned SQLite identity and one-ever reservation state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Self


SCHEMA_VERSION = 7


class StateError(RuntimeError):
    """Raised when controller-owned state cannot be initialized safely."""


@dataclass(frozen=True, slots=True)
class EventCursorState:
    """Controller-owned observation metadata for one native board."""

    cursor: int
    identity: str
    oldest_event_id: int | None


@dataclass(frozen=True, slots=True)
class StreamEventKey:
    """Stable identity for one normalized event accepted from the stream."""

    event_id: int
    task_id: str
    run_id: int | str | None


@dataclass(frozen=True, slots=True)
class StreamCursorState:
    """Durable controller-owned state for one board-scoped stream.

    ``last_transport_error``/``last_transport_at`` are transient latest-failure
    diagnostics (cleared when a fresh attempt is reconciled).  The episode
    fields — ``consecutive_failures``, ``episode_first_failure_at``,
    ``episode_last_failure_at``, ``alert_sent``, and ``alert_attempted`` —
    describe the current outage episode and survive reconciliation; they are
    cleared only when a frame is successfully accepted (the stream recovered).
    ``alert_sent`` is True only after a confirmed alert delivery; a failed
    send leaves it False (so the alert retries) while ``alert_attempted``
    records that the episode already produced an alert attempt (delivered or
    not), which guarantees a recovery notice is still attempted when the
    episode ends.
    """

    cursor: int
    identity: str
    retention_floor: int | None
    reset_required: bool
    reset_reason: str | None
    reset_count: int
    last_transport_error: str | None
    last_transport_at: str | None
    consecutive_failures: int
    episode_first_failure_at: str | None
    episode_last_failure_at: str | None
    alert_sent: bool
    alert_attempted: bool


_SCHEMA = """
PRAGMA user_version = 7;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outcome_authorities (
    authority_ref TEXT PRIMARY KEY,
    document_json TEXT NOT NULL,
    document_sha256 TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outcome_contracts (
    contract_ref TEXT PRIMARY KEY,
    document_json TEXT NOT NULL,
    document_sha256 TEXT NOT NULL,
    authority_ref TEXT NOT NULL,
    FOREIGN KEY (authority_ref) REFERENCES outcome_authorities(authority_ref)
);

CREATE TABLE IF NOT EXISTS outcome_admissions (
    admission_key TEXT PRIMARY KEY,
    parent_task_id TEXT NOT NULL,
    child_task_id TEXT,
    board_slug TEXT NOT NULL,
    contract_ref TEXT NOT NULL,
    effect TEXT NOT NULL,
    status TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outcome_merge_authorizations (
    ref TEXT NOT NULL,
    task_id TEXT NOT NULL,
    contract_ref TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    authorized_at TEXT NOT NULL,
    PRIMARY KEY (ref, task_id)
);

CREATE INDEX IF NOT EXISTS idx_merge_authorizations_ref
    ON outcome_merge_authorizations(ref, authorized_at DESC);

CREATE TABLE IF NOT EXISTS controller_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    instance_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocker_reservations (
    board_slug TEXT NOT NULL,
    task_id TEXT NOT NULL,
    blocker_kind TEXT,
    latest_event_at INTEGER NOT NULL,
    reserved_at TEXT NOT NULL,
    reservation_state TEXT NOT NULL DEFAULT 'reserved',
    PRIMARY KEY (board_slug, task_id)
);

CREATE INDEX IF NOT EXISTS idx_reservations_board_task
    ON blocker_reservations(board_slug, task_id);

CREATE TABLE IF NOT EXISTS interventions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board_slug TEXT NOT NULL,
    task_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    outcome TEXT,
    error TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (board_slug, task_id),
    FOREIGN KEY (board_slug, task_id)
        REFERENCES blocker_reservations(board_slug, task_id)
);

CREATE INDEX IF NOT EXISTS idx_interventions_board_task
    ON interventions(board_slug, task_id);

CREATE TABLE IF NOT EXISTS resolutions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    board_slug TEXT NOT NULL,
    task_id TEXT NOT NULL,
    blocker_kind TEXT,
    action TEXT NOT NULL,
    reason TEXT NOT NULL,
    latest_event_at INTEGER NOT NULL,
    resolved_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resolutions_board_task
    ON resolutions(board_slug, task_id);

CREATE TABLE IF NOT EXISTS event_cursors (
    board_slug TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL CHECK (cursor >= 0),
    identity TEXT NOT NULL DEFAULT '',
    oldest_event_id INTEGER,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS handled_events (
    board_slug TEXT NOT NULL,
    task_id TEXT NOT NULL,
    run_id_key TEXT NOT NULL,
    handled_at TEXT NOT NULL,
    PRIMARY KEY (board_slug, task_id, run_id_key)
);

CREATE TABLE IF NOT EXISTS stream_cursors (
    board_slug TEXT PRIMARY KEY,
    identity TEXT NOT NULL,
    cursor INTEGER NOT NULL CHECK (cursor >= 0),
    retention_floor INTEGER,
    reset_required INTEGER NOT NULL DEFAULT 0 CHECK (reset_required IN (0, 1)),
    reset_reason TEXT,
    reset_count INTEGER NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
    last_transport_error TEXT,
    last_transport_at TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    episode_first_failure_at TEXT,
    episode_last_failure_at TEXT,
    alert_sent INTEGER NOT NULL DEFAULT 0 CHECK (alert_sent IN (0, 1)),
    alert_attempted INTEGER NOT NULL DEFAULT 0 CHECK (alert_attempted IN (0, 1)),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stream_handled_events (
    board_slug TEXT NOT NULL,
    event_id INTEGER NOT NULL CHECK (event_id > 0),
    task_id TEXT NOT NULL,
    run_id_key TEXT NOT NULL,
    handled_at TEXT NOT NULL,
    PRIMARY KEY (board_slug, event_id, task_id, run_id_key)
);

CREATE INDEX IF NOT EXISTS idx_stream_handled_events_board
    ON stream_handled_events(board_slug, event_id);

CREATE TABLE IF NOT EXISTS assist_candidates (
    recommendation_id TEXT PRIMARY KEY,
    analysis_id TEXT NOT NULL,
    problem_signature TEXT NOT NULL,
    strength TEXT NOT NULL,
    intent TEXT NOT NULL,
    proposed_change TEXT NOT NULL,
    target_json TEXT NOT NULL,
    validation_plan_json TEXT NOT NULL,
    evidence_refs_json TEXT NOT NULL,
    safety_impact TEXT NOT NULL,
    human_in_loop TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'rejected', 'deferred')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS assist_candidate_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recommendation_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected', 'deferred')),
    note TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (recommendation_id) REFERENCES assist_candidates(recommendation_id)
);

CREATE INDEX IF NOT EXISTS idx_assist_candidates_state
    ON assist_candidates(state, created_at);
"""


class ControllerState:
    """A connection wrapper for the controller's own SQLite database."""

    def __init__(self, path: Path, connection: sqlite3.Connection):
        self.path = Path(path)
        self.connection = connection

    @classmethod
    def initialize(cls, path: Path, instance_name: str) -> Self:
        """Create/upgrade controller state and bind it to one instance name."""

        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA foreign_keys = ON")
            _ensure_schema(connection)
            existing = connection.execute(
                "SELECT instance_name FROM controller_identity WHERE singleton = 1"
            ).fetchone()
            if existing is not None and existing["instance_name"] != instance_name:
                raise StateError(
                    f"state database {path} belongs to instance "
                    f"{existing['instance_name']!r}, not {instance_name!r}"
                )
            if existing is None:
                connection.execute(
                    "INSERT INTO controller_identity(singleton, instance_name, created_at) "
                    "VALUES (1, ?, ?)",
                    (instance_name, _utc_now()),
                )
            connection.commit()
            return cls(path, connection)
        except Exception:
            connection.rollback()
            connection.close()
            raise

    @classmethod
    def open_existing(cls, path: Path) -> Self:
        """Open existing controller state without creating a database."""

        path = Path(path).expanduser()
        if not path.is_file():
            raise StateError(f"controller state not found: {path}")
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            row = connection.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            connection.close()
            raise StateError(f"not a controller state database: {path}") from exc
        if row is None:
            connection.close()
            raise StateError(f"controller state has no schema metadata: {path}")
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            _ensure_schema(connection)
        except sqlite3.Error as exc:
            connection.close()
            raise StateError(f"cannot upgrade controller state: {path}") from exc
        connection.commit()
        return cls(path, connection)

    @classmethod
    def open_read_only(cls, path: Path) -> Self:
        """Open current controller state without migrations or writes."""

        path = Path(path).expanduser().resolve()
        if not path.is_file():
            raise StateError(f"controller state not found: {path}")
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only = ON")
            row = connection.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            connection.close()
            raise StateError(f"not a controller state database: {path}") from exc
        if row is None:
            connection.close()
            raise StateError(f"controller state has no schema metadata: {path}")
        if int(row["value"]) != SCHEMA_VERSION:
            connection.close()
            raise StateError(
                f"controller state schema is {row['value']}; initialize it to "
                f"schema {SCHEMA_VERSION} before a read-only check"
            )
        return cls(path, connection)

    @property
    def instance_name(self) -> str:
        row = self.connection.execute(
            "SELECT instance_name FROM controller_identity WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise StateError("controller state has no instance identity")
        return str(row["instance_name"])

    @property
    def schema_version(self) -> int:
        row = self.connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        if row is None:
            raise StateError("controller state has no schema version")
        return int(row["value"])

    def reserve_stream_event(
        self,
        board_slug: str,
        task_id: str,
        *,
        blocker_kind: str | None,
        latest_event_at: int,
    ) -> bool:
        """Reserve a stream-confirmed blocker under the durable one-ever key."""

        return self.reserve_blocker(
            board_slug,
            task_id,
            blocker_kind=blocker_kind,
            latest_event_at=latest_event_at,
        )

    def reserve_blocker(
        self,
        board_slug: str,
        task_id: str,
        *,
        blocker_kind: str | None,
        latest_event_at: int,
    ) -> bool:
        """Atomically claim a composite board/task identity exactly once."""

        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO blocker_reservations
                (board_slug, task_id, blocker_kind, latest_event_at, reserved_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (board_slug, task_id, blocker_kind, latest_event_at, _utc_now()),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def reservation_count(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM blocker_reservations"
        ).fetchone()
        return int(row["count"])

    def has_reservation(self, board_slug: str, task_id: str) -> bool:
        """True when a one-ever reservation already exists for the task."""
        row = self.connection.execute(
            "SELECT 1 FROM blocker_reservations WHERE board_slug = ? AND task_id = ? LIMIT 1",
            (board_slug, task_id),
        ).fetchone()
        return row is not None

    def record_resolution(
        self,
        board_slug: str,
        task_id: str,
        *,
        blocker_kind: str | None,
        action: str,
        reason: str,
        latest_event_at: int,
    ) -> None:
        """Persist one printed discovery resolution, including skips."""

        self.connection.execute(
            """
            INSERT INTO resolutions
                (board_slug, task_id, blocker_kind, action, reason,
                 latest_event_at, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                board_slug,
                task_id,
                blocker_kind,
                action,
                reason,
                latest_event_at,
                _utc_now(),
            ),
        )
        self.connection.commit()

    def resolution_count(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM resolutions"
        ).fetchone()
        return int(row["count"])

    def begin_intervention(self, board_slug: str, task_id: str) -> bool:
        """Consume a reservation exactly once when native mutation begins."""

        now = _utc_now()
        cursor = self.connection.execute(
            """
            UPDATE blocker_reservations
               SET reservation_state = 'started'
             WHERE board_slug = ? AND task_id = ?
               AND reservation_state = 'reserved'
            """,
            (board_slug, task_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            return False
        self.connection.execute(
            """
            INSERT INTO interventions
                (board_slug, task_id, phase, started_at, updated_at)
            VALUES (?, ?, 'started', ?, ?)
            """,
            (board_slug, task_id, now, now),
        )
        self.connection.commit()
        return True

    def record_intervention_phase(
        self,
        board_slug: str,
        task_id: str,
        phase: str,
        *,
        outcome: str | None = None,
        error: str | None = None,
    ) -> None:
        """Persist the latest handoff phase and any real native error."""

        cursor = self.connection.execute(
            """
            UPDATE interventions
               SET phase = ?, outcome = ?, error = ?, updated_at = ?
             WHERE board_slug = ? AND task_id = ?
            """,
            (phase, outcome, error, _utc_now(), board_slug, task_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            raise StateError(
                f"intervention not found for ({board_slug!r}, {task_id!r})"
            )
        self.connection.commit()

    def get_intervention(self, board_slug: str, task_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT board_slug, task_id, phase, outcome, error, started_at, updated_at
              FROM interventions
             WHERE board_slug = ? AND task_id = ?
            """,
            (board_slug, task_id),
        ).fetchone()

    def pending_reservations(self) -> tuple[sqlite3.Row, ...]:
        """Return reservations that have not reached mutation start."""

        return tuple(
            self.connection.execute(
                """
                SELECT board_slug, task_id, blocker_kind, latest_event_at,
                       reserved_at, reservation_state
                  FROM blocker_reservations
                 WHERE reservation_state = 'reserved'
                 ORDER BY reserved_at ASC, board_slug ASC, task_id ASC
                """
            ).fetchall()
        )

    def started_intervention_count(self) -> int:
        """Return reservations that crossed the automatic-start boundary."""

        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM blocker_reservations "
            "WHERE reservation_state = 'started'"
        ).fetchone()
        return int(row["count"])

    def get_stream_cursor(self, board_slug: str) -> StreamCursorState:
        """Return durable stream state, or an uninitialized board state."""

        row = self.connection.execute(
            """
            SELECT cursor, identity, retention_floor, reset_required,
                   reset_reason, reset_count, last_transport_error,
                   last_transport_at, consecutive_failures,
                   episode_first_failure_at, episode_last_failure_at,
                   alert_sent, alert_attempted
             FROM stream_cursors
             WHERE board_slug = ?
            """,
            (board_slug,),
        ).fetchone()
        if row is None:
            return StreamCursorState(
                0, "", None, True, "uninitialized", 0, None, None, 0, None, None, False, False
            )
        return StreamCursorState(
            cursor=int(row["cursor"]),
            identity=str(row["identity"]),
            retention_floor=(
                int(row["retention_floor"])
                if row["retention_floor"] is not None
                else None
            ),
            reset_required=bool(row["reset_required"]),
            reset_reason=(
                str(row["reset_reason"]) if row["reset_reason"] is not None else None
            ),
            reset_count=int(row["reset_count"]),
            last_transport_error=(
                str(row["last_transport_error"])
                if row["last_transport_error"] is not None
                else None
            ),
            last_transport_at=(
                str(row["last_transport_at"])
                if row["last_transport_at"] is not None
                else None
            ),
            consecutive_failures=int(row["consecutive_failures"]),
            episode_first_failure_at=(
                str(row["episode_first_failure_at"])
                if row["episode_first_failure_at"] is not None
                else None
            ),
            episode_last_failure_at=(
                str(row["episode_last_failure_at"])
                if row["episode_last_failure_at"] is not None
                else None
            ),
            alert_sent=bool(row["alert_sent"]),
            alert_attempted=bool(row["alert_attempted"]),
        )

    def reconcile_stream_cursor(
        self,
        board_slug: str,
        *,
        identity: str,
        retention_floor: int | None = None,
        observed_cursor: int | None = None,
    ) -> StreamCursorState:
        """Record board generation metadata and reset safely when history moved.

        A changed board identity, maximum-id rollback, or retention floor beyond
        the accepted cursor requires a cursor-zero resync.  This method mutates
        only controller-owned state; durable stream event keys are intentionally
        retained across resets so replay cannot trigger duplicate handling.
        """

        self._validate_stream_metadata(identity, retention_floor, observed_cursor)
        try:
            current = self.get_stream_cursor(board_slug)
            reason: str | None = None
            if current.identity and current.identity != identity:
                reason = "identity_changed"
            elif (
                observed_cursor is not None and observed_cursor < current.cursor
            ):
                reason = "id_rollback"
            elif (
                current.cursor > 0
                and retention_floor is not None
                and retention_floor > current.cursor
            ):
                reason = "retention_gap"

            cursor = 0 if reason is not None else current.cursor
            reset_count = current.reset_count + (1 if reason is not None else 0)
            reset_required = reason is not None or current.reset_required
            if current.identity == "":
                reset_required = False
                reset_reason = None
            else:
                reset_reason = reason if reason is not None else current.reset_reason
            now = _utc_now()
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                """
                INSERT INTO stream_cursors(
                    board_slug, identity, cursor, retention_floor,
                    reset_required, reset_reason, reset_count,
                    last_transport_error, last_transport_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
                ON CONFLICT(board_slug) DO UPDATE SET
                    identity = excluded.identity,
                    cursor = excluded.cursor,
                    retention_floor = excluded.retention_floor,
                    reset_required = excluded.reset_required,
                    reset_reason = excluded.reset_reason,
                    reset_count = excluded.reset_count,
                    last_transport_error = NULL,
                    last_transport_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    board_slug,
                    identity,
                    cursor,
                    retention_floor,
                    int(reset_required),
                    reset_reason,
                    reset_count,
                    now,
                ),
            )
            self.connection.commit()
        except Exception as exc:
            self.connection.rollback()
            if isinstance(exc, StateError):
                raise
            raise StateError(
                f"reconcile_stream_cursor failed: {type(exc).__name__}: {exc}"
            ) from exc
        return self.get_stream_cursor(board_slug)

    def accept_stream_frame(
        self,
        board_slug: str,
        *,
        identity: str,
        cursor: int,
        events: tuple[StreamEventKey, ...],
    ) -> StreamCursorState:
        """Compatibility alias for the atomic stream-frame commit boundary."""

        return self.commit_stream_frame(
            board_slug,
            identity=identity,
            cursor=cursor,
            events=events,
        )

    def commit_stream_frame(
        self,
        board_slug: str,
        *,
        identity: str,
        cursor: int,
        events: tuple[StreamEventKey, ...],
    ) -> StreamCursorState:
        """Atomically accept event identities and then advance the cursor.

        A frame that cannot be fully persisted rolls back both the handled-event
        inserts and cursor update.  This is the crash boundary: callers may
        process a frame before this call, but must not trust it as accepted until
        this transaction returns successfully.
        """

        self._validate_stream_metadata(identity, None, cursor)
        if events:
            ids = [event.event_id for event in events]
            if max(ids) != cursor:
                raise StateError("commit_stream_frame cursor must equal final event id")
            if ids != sorted(ids) or len(ids) != len(set(ids)):
                raise StateError("commit_stream_frame event ids must be strictly increasing")
            for event in events:
                if event.event_id <= 0 or not event.task_id:
                    raise StateError("commit_stream_frame event identity is invalid")
                if event.run_id is not None and not (
                    (isinstance(event.run_id, int) and not isinstance(event.run_id, bool))
                    or (isinstance(event.run_id, str) and bool(event.run_id))
                ):
                    raise StateError("commit_stream_frame run identity is invalid")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            now = _utc_now()
            current = self.get_stream_cursor(board_slug)
            if not current.identity:
                raise StateError("stream cursor must be reconciled before frame commit")
            if current.identity != identity:
                raise StateError("stream frame identity does not match durable board identity")
            if not events:
                if cursor != current.cursor:
                    raise StateError(
                        "commit_stream_frame requires event identity before cursor advance"
                    )
                # An accepted empty frame (idle board) still confirms the
                # transport is alive: end the failure episode so a recovery
                # is observed exactly once and the alert dedupes.
                self.connection.execute(
                    """
                    UPDATE stream_cursors
                      SET last_transport_error = NULL,
                          last_transport_at = NULL,
                          consecutive_failures = 0,
                          episode_first_failure_at = NULL,
                          episode_last_failure_at = NULL,
                          alert_sent = 0,
                          alert_attempted = 0,
                          updated_at = ?
                    WHERE board_slug = ? AND identity = ?
                    """,
                    (now, board_slug, identity),
                )
                self.connection.commit()
                return self.get_stream_cursor(board_slug)
            if cursor > current.cursor and any(
                event.event_id <= current.cursor for event in events
            ):
                raise StateError(
                    "commit_stream_frame cannot skip an event before the accepted cursor"
                )
            if cursor == current.cursor:
                for event in events:
                    if not self.stream_event_handled(board_slug, event):
                        raise StateError(
                            "commit_stream_frame replay is missing durable event identity"
                        )
            if cursor < current.cursor:
                # A replay from an older connection is still safe to record as
                # handled, but it must never move the accepted cursor backwards.
                accepted_cursor = current.cursor
            else:
                accepted_cursor = cursor
            for event in events:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO stream_handled_events(
                        board_slug, event_id, task_id, run_id_key, handled_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        board_slug,
                        event.event_id,
                        event.task_id,
                        _run_id_key(event.run_id),
                        now,
                    ),
                )
            self.connection.execute(
                """
                UPDATE stream_cursors
                  SET cursor = ?, reset_required = 0,
                      reset_reason = NULL, last_transport_error = NULL,
                      last_transport_at = NULL,
                      consecutive_failures = 0,
                      episode_first_failure_at = NULL,
                      episode_last_failure_at = NULL,
                      alert_sent = 0,
                      alert_attempted = 0,
                      updated_at = ?
                WHERE board_slug = ? AND identity = ?
                """,
                (accepted_cursor, now, board_slug, identity),
            )
            self.connection.commit()
        except StateError:
            self.connection.rollback()
            raise
        except Exception as exc:
            self.connection.rollback()
            raise StateError(
                f"commit_stream_frame failed: {type(exc).__name__}: {exc}"
            ) from exc
        return self.get_stream_cursor(board_slug)

    def record_stream_transport_failure(
        self, board_slug: str, *, code: str, message: str
    ) -> StreamCursorState:
        """Record one board-local transport failure without touching other boards.

        The latest-failure diagnostics are refreshed and the outage episode
        counters are advanced: ``consecutive_failures`` increments,
        ``episode_first_failure_at`` is anchored on the first failure, and
        ``episode_last_failure_at`` tracks the most recent one.  Only a
        successfully accepted frame ends the episode.
        """

        if not code or not message:
            raise StateError("stream transport failure code and message are required")
        try:
            now = _utc_now()
            self.connection.execute(
                """
                UPDATE stream_cursors
                   SET last_transport_error = ?,
                       last_transport_at = ?,
                       consecutive_failures = consecutive_failures + 1,
                       episode_first_failure_at =
                           COALESCE(episode_first_failure_at, ?),
                       episode_last_failure_at = ?,
                       updated_at = ?
                 WHERE board_slug = ?
                """,
                (f"{code}: {message}", now, now, now, now, board_slug),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise StateError(f"stream cursor not found for board {board_slug!r}")
            self.connection.commit()
        except StateError:
            self.connection.rollback()
            raise
        except Exception as exc:
            self.connection.rollback()
            raise StateError(
                f"record_stream_transport_failure failed: {type(exc).__name__}: {exc}"
            ) from exc
        return self.get_stream_cursor(board_slug)

    def mark_stream_alert_sent(self, board_slug: str) -> StreamCursorState:
        """Mark the current outage episode's alert as delivered (dedupe)."""

        try:
            self.connection.execute(
                """
                UPDATE stream_cursors
                  SET alert_sent = 1, alert_attempted = 1, updated_at = ?
                WHERE board_slug = ?
                """,
                (_utc_now(), board_slug),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise StateError(f"stream cursor not found for board {board_slug!r}")
            self.connection.commit()
        except StateError:
            self.connection.rollback()
            raise
        except Exception as exc:
            self.connection.rollback()
            raise StateError(
                f"mark_stream_alert_sent failed: {type(exc).__name__}: {exc}"
            ) from exc
        return self.get_stream_cursor(board_slug)

    def mark_stream_alert_attempted(self, board_slug: str) -> StreamCursorState:
        """Record an alert attempt for the current episode without dedupe.

        ``alert_sent`` stays untouched (False until a confirmed delivery, so a
        failed send is retried on the next recorded failure), but the episode
        is remembered as having produced an alert attempt so a recovery notice
        is still attempted when the stream resumes.
        """

        try:
            self.connection.execute(
                """
                UPDATE stream_cursors
                  SET alert_attempted = 1, updated_at = ?
                WHERE board_slug = ?
                """,
                (_utc_now(), board_slug),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise StateError(f"stream cursor not found for board {board_slug!r}")
            self.connection.commit()
        except StateError:
            self.connection.rollback()
            raise
        except Exception as exc:
            self.connection.rollback()
            raise StateError(
                f"mark_stream_alert_attempted failed: {type(exc).__name__}: {exc}"
            ) from exc
        return self.get_stream_cursor(board_slug)

    def stream_event_handled(
        self, board_slug: str, event: StreamEventKey
    ) -> bool:
        """Return whether this exact board/event/task/run identity was accepted."""

        row = self.connection.execute(
            """
            SELECT 1 FROM stream_handled_events
             WHERE board_slug = ? AND event_id = ? AND task_id = ?
               AND run_id_key = ?
            """,
            (board_slug, event.event_id, event.task_id, _run_id_key(event.run_id)),
        ).fetchone()
        return row is not None

    def stream_event_count(self, board_slug: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS count FROM stream_handled_events WHERE board_slug = ?",
            (board_slug,),
        ).fetchone()
        return int(row["count"])

    @staticmethod
    def _validate_stream_metadata(
        identity: str, retention_floor: int | None, observed_cursor: int | None
    ) -> None:
        if not isinstance(identity, str) or not identity:
            raise StateError("stream cursor identity must not be empty")
        for name, value in (
            ("retention floor", retention_floor),
            ("observed cursor", observed_cursor),
        ):
            if value is not None and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise StateError(f"stream {name} must be a nonnegative integer")

    def get_event_cursor(self, board_slug: str) -> int:
        """Return the last observed native event id for one board."""

        state = self.get_event_cursor_state(board_slug)
        return state.cursor if state is not None else 0

    def get_event_cursor_state(self, board_slug: str) -> EventCursorState | None:
        """Return cursor plus controller-owned native-generation metadata."""

        row = self.connection.execute(
            """
            SELECT cursor, identity, oldest_event_id
              FROM event_cursors
             WHERE board_slug = ?
            """,
            (board_slug,),
        ).fetchone()
        if row is None:
            return None
        return EventCursorState(
            cursor=int(row["cursor"]),
            identity=str(row["identity"]),
            oldest_event_id=(
                int(row["oldest_event_id"])
                if row["oldest_event_id"] is not None
                else None
            ),
        )

    def set_event_cursor(self, board_slug: str, cursor: int) -> None:
        """Advance a board cursor monotonically in controller-owned state."""

        if cursor < 0:
            raise StateError("event cursor must not be negative")
        self.connection.execute(
            """
            INSERT INTO event_cursors(board_slug, cursor, identity, oldest_event_id, updated_at)
            VALUES (
                ?, ?,
                COALESCE((SELECT identity FROM event_cursors WHERE board_slug = ?), ''),
                (SELECT oldest_event_id FROM event_cursors WHERE board_slug = ?),
                ?
            )
            ON CONFLICT(board_slug) DO UPDATE SET
                cursor = CASE
                    WHEN excluded.cursor > event_cursors.cursor THEN excluded.cursor
                    ELSE event_cursors.cursor
                END,
                updated_at = excluded.updated_at
            """,
            (board_slug, int(cursor), board_slug, board_slug, _utc_now()),
        )
        self.connection.commit()

    def set_event_cursor_metadata(
        self,
        board_slug: str,
        *,
        identity: str,
        oldest_event_id: int | None,
    ) -> None:
        """Persist native-generation metadata without changing the cursor."""

        if not identity:
            raise StateError("event cursor identity must not be empty")
        if oldest_event_id is not None and oldest_event_id < 0:
            raise StateError("oldest event id must not be negative")
        self.connection.execute(
            """
            INSERT INTO event_cursors(board_slug, cursor, identity, oldest_event_id, updated_at)
            VALUES (?, 0, ?, ?, ?)
            ON CONFLICT(board_slug) DO UPDATE SET
                identity = excluded.identity,
                oldest_event_id = excluded.oldest_event_id,
                updated_at = excluded.updated_at
            """,
            (board_slug, identity, oldest_event_id, _utc_now()),
        )
        self.connection.commit()

    def reset_event_cursor(
        self,
        board_slug: str,
        *,
        identity: str,
        oldest_event_id: int | None,
    ) -> None:
        """Reset a cursor after native replacement or a retention gap.

        This only changes controller-owned state. Replaying retained rows is
        safe because handled task/run keys remain durable in ``handled_events``.
        """

        if not identity:
            raise StateError("event cursor identity must not be empty")
        if oldest_event_id is not None and oldest_event_id < 0:
            raise StateError("oldest event id must not be negative")
        self.connection.execute(
            """
            INSERT INTO event_cursors(board_slug, cursor, identity, oldest_event_id, updated_at)
            VALUES (?, 0, ?, ?, ?)
            ON CONFLICT(board_slug) DO UPDATE SET
                cursor = 0,
                identity = excluded.identity,
                oldest_event_id = excluded.oldest_event_id,
                updated_at = excluded.updated_at
            """,
            (board_slug, identity, oldest_event_id, _utc_now()),
        )
        self.connection.commit()

    def delete_event_cursor(self, board_slug: str) -> None:
        """Forget one board cursor when a native board is removed or rotated."""

        self.connection.execute(
            "DELETE FROM event_cursors WHERE board_slug = ?",
            (board_slug,),
        )
        self.connection.commit()

    def event_cursors(self) -> dict[str, int]:
        """Return all persisted native observation cursors."""

        return {
            str(row["board_slug"]): int(row["cursor"])
            for row in self.connection.execute(
                "SELECT board_slug, cursor FROM event_cursors ORDER BY board_slug"
            ).fetchall()
        }

    def claim_handled_event(
        self,
        board_slug: str,
        task_id: str,
        run_id: int | str | None,
    ) -> bool:
        """Claim one task/run event key exactly once."""

        run_id_key = _run_id_key(run_id)
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO handled_events
                (board_slug, task_id, run_id_key, handled_at)
            VALUES (?, ?, ?, ?)
            """,
            (board_slug, task_id, run_id_key, _utc_now()),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.close()


def _run_id_key(run_id: int | str | None) -> str:
    return "<null>" if run_id is None else str(run_id)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_schema(connection: sqlite3.Connection) -> None:
    """Install additive controller tables for fresh and pre-discovery state DBs."""

    connection.executescript(_SCHEMA)
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(blocker_reservations)")
    }
    if "reservation_state" not in columns:
        connection.execute(
            "ALTER TABLE blocker_reservations ADD COLUMN "
            "reservation_state TEXT NOT NULL DEFAULT 'reserved'"
        )
    cursor_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(event_cursors)")
    }
    if "identity" not in cursor_columns:
        connection.execute(
            "ALTER TABLE event_cursors ADD COLUMN identity TEXT NOT NULL DEFAULT ''"
        )
    if "oldest_event_id" not in cursor_columns:
        connection.execute(
            "ALTER TABLE event_cursors ADD COLUMN oldest_event_id INTEGER"
        )
    stream_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(stream_cursors)")
    }
    if "last_transport_at" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN last_transport_at TEXT"
        )
    if "consecutive_failures" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN "
            "consecutive_failures INTEGER NOT NULL DEFAULT 0 "
            "CHECK (consecutive_failures >= 0)"
        )
    if "episode_first_failure_at" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN episode_first_failure_at TEXT"
        )
    if "episode_last_failure_at" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN episode_last_failure_at TEXT"
        )
    if "alert_sent" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN "
            "alert_sent INTEGER NOT NULL DEFAULT 0 CHECK (alert_sent IN (0, 1))"
        )
    if "alert_attempted" not in stream_columns:
        connection.execute(
            "ALTER TABLE stream_cursors ADD COLUMN "
            "alert_attempted INTEGER NOT NULL DEFAULT 0 "
            "CHECK (alert_attempted IN (0, 1))"
        )
    connection.execute(
        """
        INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (str(SCHEMA_VERSION),),
    )
