from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pytest

from hkrc.assist_latency import (
    INSUFFICIENT_DATA_MESSAGE,
    LatencyReporter,
    MonotonicClock,
)
from hkrc.assist_ledger import (
    AppendOnlyLedger,
    LedgerValidationError,
)


class FakeClock(MonotonicClock):
    def __init__(self) -> None:
        self.monotonic = 10.0
        self.wall = datetime(2026, 1, 11, 0, 0, tzinfo=timezone.utc)

    def monotonic_seconds(self) -> float:
        return self.monotonic

    def utc_now(self) -> datetime:
        return self.wall


def test_ledger_append_is_immutable_and_versioned(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3", retention_records=10)
    record = ledger.append(
        subject="analysis-opaque",
        phase="classification",
        event="completed",
        actor="deterministic_controller",
        evidence_refs=("evidence:ev-1",),
        latency_ms=12,
        details={"bounded": True},
    )

    assert record.schema_version == "hkrc.assist.ledger.v1"
    assert ledger.records() == (record,)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        ledger.connection.execute(
            "UPDATE ledger_records SET event = 'approved' WHERE record_id = ?",
            (record.record_id,),
        )
    assert ledger.records()[0].event == "completed"
    ledger.close()


def test_ledger_connection_cannot_disable_append_only_protection(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3", retention_records=10)
    record = ledger.append(
        subject="analysis-opaque",
        phase="classification",
        event="completed",
        actor="deterministic_controller",
    )

    ledger.connection.create_function("ledger_retention_active", 0, lambda: 1)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        ledger.connection.execute(
            "UPDATE ledger_records SET event = 'approved' WHERE record_id = ?",
            (record.record_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        ledger.connection.execute(
            "DELETE FROM ledger_records WHERE record_id = ?",
            (record.record_id,),
        )
    assert ledger.records()[0].event == "completed"
    ledger.close()


def test_external_sqlite_connection_cannot_mutate_ledger_records(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    ledger = AppendOnlyLedger(path, retention_records=10)
    record = ledger.append(
        subject="analysis-opaque",
        phase="classification",
        event="completed",
        actor="deterministic_controller",
    )
    external = sqlite3.connect(path)

    with pytest.raises(sqlite3.DatabaseError, match="append-only|function"):
        external.execute(
            "UPDATE ledger_records SET event = 'approved' WHERE record_id = ?",
            (record.record_id,),
        )
    with pytest.raises(sqlite3.DatabaseError, match="append-only|function"):
        external.execute(
            "DELETE FROM ledger_records WHERE record_id = ?",
            (record.record_id,),
        )
    external.rollback()
    assert ledger.records()[0].event == "completed"

    external.close()
    ledger.close()


def test_ledger_records_failure_and_fallback_paths(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3")

    malformed = ledger.append(
        subject="analysis-opaque",
        phase="ai",
        event="malformed",
        actor="local_model",
        error_code="invalid_json",
        details={"bounded": True},
    )
    fallback = ledger.append(
        subject="analysis-opaque",
        phase="recommendation",
        event="fallback",
        actor="deterministic_controller",
        error_code="model_unavailable",
        details={"actionable": False},
    )

    assert malformed.error_code == "invalid_json"
    assert fallback.event == "fallback"
    assert [item.event for item in ledger.records()] == ["malformed", "fallback"]
    ledger.close()


def test_ledger_bounds_details_and_rejects_secret_keys(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3", max_detail_string=12)
    record = ledger.append(
        subject="analysis-opaque",
        phase="error",
        event="timeout",
        actor="deterministic_controller",
        details={"message": "abcdefghijklmnopqrstuvwxyz"},
    )
    assert record.details == {"message": "abcdefghijkl"}
    with pytest.raises(LedgerValidationError, match="sensitive"):
        ledger.append(
            subject="analysis-opaque",
            phase="error",
            event="malformed",
            actor="deterministic_controller",
            details={"api_token": "must-not-be-stored"},
        )
    ledger.close()


def test_ledger_retention_is_operator_configured(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3", retention_records=2)
    for event in ("started", "completed", "fallback"):
        ledger.append(
            subject="analysis-opaque",
            phase="observation",
            event=event,
            actor="deterministic_controller",
        )
    assert [item.event for item in ledger.records()] == ["completed", "fallback"]
    ledger.close()


def test_ledger_rejects_non_mapping_details(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3")
    with pytest.raises(LedgerValidationError, match="mapping"):
        ledger.append(
            subject="analysis-opaque",
            phase="error",
            event="malformed",
            actor="deterministic_controller",
            details="not-a-mapping",
        )
    ledger.close()


def test_ledger_rejects_invalid_recorded_at_utc(tmp_path: Path) -> None:
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3")
    with pytest.raises(LedgerValidationError, match="timezone|timestamp"):
        ledger.append(
            subject="analysis-opaque",
            phase="observation",
            event="started",
            actor="deterministic_controller",
            recorded_at_utc="not-a-timestamp",
        )
    with pytest.raises(LedgerValidationError, match="timezone"):
        ledger.append(
            subject="analysis-opaque",
            phase="observation",
            event="started",
            actor="deterministic_controller",
            recorded_at_utc="2026-01-11T00:00:00",
        )
    ledger.close()


def test_latency_only_records_finding_to_decision_for_real_operator_decision() -> None:
    clock = FakeClock()
    reporter = LatencyReporter(clock=clock)
    reporter.observation_started()
    clock.monotonic = 12.5
    reporter.finding_created()
    finding_only = reporter.snapshot()

    assert finding_only.detection_to_finding_ms == 2500
    assert finding_only.finding_to_decision_ms is None
    assert finding_only.end_to_end_decision_ms is None

    clock.monotonic = 18.0
    reporter.operator_decision("deferred")
    report = reporter.snapshot()
    assert report.finding_to_decision_ms == 5500
    assert report.end_to_end_decision_ms == 8000
    assert report.operator_decision == "deferred"


def test_latency_rejects_naive_wall_clock_values() -> None:
    class NaiveClock(FakeClock):
        def utc_now(self) -> datetime:
            return self.wall.replace(tzinfo=None)

    reporter = LatencyReporter(clock=NaiveClock())
    reporter.observation_started()
    with pytest.raises(ValueError, match="timezone"):
        reporter.snapshot()


def test_latency_guard_never_claims_improvement_in_phase_one() -> None:
    clock = FakeClock()
    reporter = LatencyReporter(clock=clock)
    reporter.observation_started()
    clock.monotonic = 12.0
    reporter.finding_created()

    output = reporter.render_summary()
    assert INSUFFICIENT_DATA_MESSAGE in output
    assert "speedup" not in output.lower()
    assert "improvement: true" not in output.lower()


def test_latency_rejects_non_operator_decision() -> None:
    reporter = LatencyReporter(clock=FakeClock())
    reporter.observation_started()
    with pytest.raises(ValueError, match="operator decision"):
        reporter.operator_decision("completed")


@pytest.fixture(autouse=True)
def close_open_sqlite_connections() -> None:
    # Tests explicitly close their ledger; this fixture documents that the
    # connection is intentionally exposed only for append-only verification.
    yield
