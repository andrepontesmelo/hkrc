"""Versioned, bounded, append-only evidence ledger for HKRC Assist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping
from uuid import uuid4

SCHEMA_VERSION = "hkrc.assist.ledger.v1"
DEFAULT_RETENTION_RECORDS = 10_000
DEFAULT_MAX_DETAIL_STRING = 512
_MAX_DETAIL_DEPTH = 5
_MAX_DETAIL_ITEMS = 64
_PHASES = frozenset(
    {"observation", "classification", "ai", "recommendation", "explanation", "approval", "outcome", "error"}
)
_EVENTS = frozenset(
    {
        "started",
        "completed",
        "fallback",
        "timeout",
        "malformed",
        "pending",
        "approved",
        "rejected",
        "deferred",
        "not_applied",
        "artifact_created",
    }
)
_ACTORS = frozenset({"deterministic_controller", "local_model", "operator"})
_SENSITIVE_KEY = re.compile(
    r"(?:secret|password|passwd|token|credential|api[_-]?key|private[_-]?key|authorization|cookie)",
    re.IGNORECASE,
)


class LedgerValidationError(ValueError):
    """Raised when a ledger record would violate the safe data contract."""


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    schema_version: str
    record_id: str
    recorded_at_utc: str
    window_fingerprint: str | None
    subject: str
    phase: str
    event: str
    evidence_refs: tuple[str, ...]
    actor: str
    latency_ms: int | None
    error_code: str | None
    details: dict[str, Any]


class _LedgerConnection:
    """Narrow public facade that keeps ledger mutation controls private."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def execute(self, *args: Any, **kwargs: Any) -> sqlite3.Cursor:
        try:
            return self._connection.execute(*args, **kwargs)
        except sqlite3.DatabaseError as exc:
            if "not authorized" in str(exc).lower():
                raise sqlite3.IntegrityError(
                    "append-only ledger records cannot be mutated"
                ) from exc
            raise

    def create_function(self, name: str, *args: Any, **kwargs: Any) -> None:
        if name == "ledger_retention_active":
            return
        self._connection.create_function(name, *args, **kwargs)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class AppendOnlyLedger:
    """Persist versioned records without allowing in-place record edits.

    Retention is an explicit bounded-storage policy: the oldest records may be
    evicted when a new record is appended, while all retained records remain
    immutable. Direct SQL updates/deletes are rejected by database triggers.
    """

    def __init__(
        self,
        path: Path,
        *,
        retention_records: int = DEFAULT_RETENTION_RECORDS,
        max_detail_string: int = DEFAULT_MAX_DETAIL_STRING,
    ) -> None:
        if isinstance(retention_records, bool) or retention_records <= 0:
            raise LedgerValidationError("retention_records must be a positive integer")
        if isinstance(max_detail_string, bool) or max_detail_string <= 0:
            raise LedgerValidationError("max_detail_string must be a positive integer")
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_records = retention_records
        self.max_detail_string = max_detail_string
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self.connection = _LedgerConnection(self._connection)
        self._retention_active = False
        self._connection.set_authorizer(self._authorize)
        self._connection.create_function(
            "ledger_retention_active", 0, lambda: int(self._retention_active)
        )
        self._connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS ledger_records (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                schema_version TEXT NOT NULL,
                record_id TEXT NOT NULL UNIQUE,
                recorded_at_utc TEXT NOT NULL,
                window_fingerprint TEXT,
                subject TEXT NOT NULL,
                phase TEXT NOT NULL,
                event TEXT NOT NULL,
                evidence_refs TEXT NOT NULL,
                actor TEXT NOT NULL,
                latency_ms INTEGER,
                error_code TEXT,
                details TEXT NOT NULL,
                CHECK (schema_version = 'hkrc.assist.ledger.v1'),
                CHECK (latency_ms IS NULL OR latency_ms >= 0)
            );
            CREATE INDEX IF NOT EXISTS idx_ledger_records_sequence
                ON ledger_records(sequence);
            CREATE TRIGGER IF NOT EXISTS ledger_records_no_update
            BEFORE UPDATE ON ledger_records
            WHEN ledger_retention_active() = 0
            BEGIN
                SELECT RAISE(ABORT, 'append-only ledger records cannot be updated');
            END;
            CREATE TRIGGER IF NOT EXISTS ledger_records_no_delete
            BEFORE DELETE ON ledger_records
            WHEN ledger_retention_active() = 0
            BEGIN
                SELECT RAISE(ABORT, 'append-only ledger records cannot be deleted');
            END;
            """
        )
        self._connection.commit()

    def _authorize(
        self,
        action: int,
        table: str | None,
        column: str | None,
        database: str | None,
        source: str | None,
    ) -> int:
        if action in (sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE) and table == "ledger_records":
            if not self._retention_active:
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def append(
        self,
        *,
        subject: str,
        phase: str,
        event: str,
        actor: str,
        evidence_refs: tuple[str, ...] | list[str] = (),
        window_fingerprint: str | None = None,
        latency_ms: int | None = None,
        error_code: str | None = None,
        details: Mapping[str, Any] | None = None,
        recorded_at_utc: datetime | str | None = None,
    ) -> LedgerRecord:
        """Append one bounded record and evict only records beyond retention."""
        subject = _required_text(subject, "subject")
        phase = _required_text(phase, "phase")
        event = _required_text(event, "event")
        actor = _required_text(actor, "actor")
        if phase not in _PHASES:
            raise LedgerValidationError(f"unsupported ledger phase: {phase}")
        if event not in _EVENTS:
            raise LedgerValidationError(f"unsupported ledger event: {event}")
        if actor not in _ACTORS:
            raise LedgerValidationError(f"unsupported ledger actor: {actor}")
        if latency_ms is not None and (
            isinstance(latency_ms, bool) or not isinstance(latency_ms, int) or latency_ms < 0
        ):
            raise LedgerValidationError("latency_ms must be a non-negative integer")
        refs = tuple(_required_text(ref, "evidence ref") for ref in evidence_refs)
        if len(refs) > _MAX_DETAIL_ITEMS:
            raise LedgerValidationError("evidence_refs exceeds the bounded item limit")
        if details is None:
            clean_details: dict[str, Any] = {}
        elif not isinstance(details, Mapping):
            raise LedgerValidationError("details must be a mapping")
        else:
            clean_details = _bound_details(details, max_string=self.max_detail_string)
        recorded = _utc_text(recorded_at_utc)
        record_id = "ledger-" + uuid4().hex
        record = LedgerRecord(
            schema_version=SCHEMA_VERSION,
            record_id=record_id,
            recorded_at_utc=recorded,
            window_fingerprint=window_fingerprint,
            subject=subject,
            phase=phase,
            event=event,
            evidence_refs=refs,
            actor=actor,
            latency_ms=latency_ms,
            error_code=error_code,
            details=clean_details,
        )
        try:
            self._connection.execute(
                """
                INSERT INTO ledger_records
                    (schema_version, record_id, recorded_at_utc, window_fingerprint,
                     subject, phase, event, evidence_refs, actor, latency_ms,
                     error_code, details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.schema_version,
                    record.record_id,
                    record.recorded_at_utc,
                    record.window_fingerprint,
                    record.subject,
                    record.phase,
                    record.event,
                    _json(record.evidence_refs),
                    record.actor,
                    record.latency_ms,
                    record.error_code,
                    _json(record.details),
                ),
            )
            self._retention_active = True
            self._connection.execute(
                """
                DELETE FROM ledger_records
                 WHERE sequence NOT IN (
                    SELECT sequence FROM ledger_records ORDER BY sequence DESC LIMIT ?
                 )
                """,
                (self.retention_records,),
            )
            self._retention_active = False
            self._connection.commit()
        except Exception:
            self._retention_active = False
            self._connection.rollback()
            raise
        return record

    def records(self) -> tuple[LedgerRecord, ...]:
        rows = self._connection.execute(
            "SELECT * FROM ledger_records ORDER BY sequence ASC"
        ).fetchall()
        return tuple(_record_from_row(row) for row in rows)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "AppendOnlyLedger":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if exc_type is None:
            self._connection.commit()
        else:
            self._connection.rollback()
        self._connection.close()


def _record_from_row(row: sqlite3.Row) -> LedgerRecord:
    return LedgerRecord(
        schema_version=str(row["schema_version"]),
        record_id=str(row["record_id"]),
        recorded_at_utc=str(row["recorded_at_utc"]),
        window_fingerprint=row["window_fingerprint"],
        subject=str(row["subject"]),
        phase=str(row["phase"]),
        event=str(row["event"]),
        evidence_refs=tuple(json.loads(row["evidence_refs"])),
        actor=str(row["actor"]),
        latency_ms=row["latency_ms"],
        error_code=row["error_code"],
        details=json.loads(row["details"]),
    )


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _utc_text(value: datetime | str | None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise LedgerValidationError("recorded_at_utc must include a timezone")
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise LedgerValidationError("recorded_at_utc must be a valid timestamp") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise LedgerValidationError("recorded_at_utc must include a timezone")
        return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    raise LedgerValidationError("recorded_at_utc must be a UTC datetime or text")


def _bound_details(value: Any, *, max_string: int, depth: int = 0) -> Any:
    if depth > _MAX_DETAIL_DEPTH:
        raise LedgerValidationError("details exceed the maximum nesting depth")
    if isinstance(value, Mapping):
        if len(value) > _MAX_DETAIL_ITEMS:
            raise LedgerValidationError("details exceeds the bounded item limit")
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str) or not key.strip():
                raise LedgerValidationError("detail keys must be non-empty strings")
            if _SENSITIVE_KEY.search(key):
                raise LedgerValidationError(f"sensitive detail key is not allowed: {key}")
            result[key] = _bound_details(child, max_string=max_string, depth=depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_DETAIL_ITEMS:
            raise LedgerValidationError("details exceeds the bounded item limit")
        return [_bound_details(child, max_string=max_string, depth=depth + 1) for child in value]
    if isinstance(value, str):
        return value[:max_string]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise LedgerValidationError(f"unsupported detail value type: {type(value).__name__}")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
