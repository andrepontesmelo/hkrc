"""Conservative descriptive latency measurements for HKRC Assist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

INSUFFICIENT_DATA_MESSAGE = "insufficient data for improvement claim"
_DECISIONS = frozenset({"approved", "rejected", "deferred"})


class MonotonicClock(Protocol):
    def monotonic_seconds(self) -> float: ...

    def utc_now(self) -> datetime: ...


class _SystemClock:
    def monotonic_seconds(self) -> float:
        import time

        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class LatencyReport:
    observation_started_at_utc: str | None
    finding_created_at_utc: str | None
    operator_decision_at_utc: str | None
    operator_decision: str | None
    detection_to_finding_ms: int | None
    finding_to_decision_ms: int | None
    end_to_end_decision_ms: int | None
    improvement_claim: str


class LatencyReporter:
    """Record phase durations without inferring a before/after improvement."""

    def __init__(self, *, clock: MonotonicClock | None = None) -> None:
        self._clock = clock or _SystemClock()
        self._observation_tick: float | None = None
        self._finding_tick: float | None = None
        self._decision_tick: float | None = None
        self._observation_wall: datetime | None = None
        self._finding_wall: datetime | None = None
        self._decision_wall: datetime | None = None
        self._decision: str | None = None

    def observation_started(self) -> None:
        self._observation_tick = self._clock.monotonic_seconds()
        self._observation_wall = self._clock.utc_now()

    def finding_created(self) -> None:
        self._require_started("finding")
        self._finding_tick = self._clock.monotonic_seconds()
        self._finding_wall = self._clock.utc_now()

    def operator_decision(self, decision: str) -> None:
        if decision not in _DECISIONS:
            raise ValueError("operator decision must be approved, rejected, or deferred")
        self._require_started("operator decision")
        self._require_finding("operator decision")
        self._decision = decision
        self._decision_tick = self._clock.monotonic_seconds()
        self._decision_wall = self._clock.utc_now()

    def snapshot(self) -> LatencyReport:
        detection = _elapsed_ms(self._observation_tick, self._finding_tick)
        finding = _elapsed_ms(self._finding_tick, self._decision_tick)
        end_to_end = _elapsed_ms(self._observation_tick, self._decision_tick)
        return LatencyReport(
            observation_started_at_utc=_wall_text(self._observation_wall),
            finding_created_at_utc=_wall_text(self._finding_wall),
            operator_decision_at_utc=_wall_text(self._decision_wall),
            operator_decision=self._decision,
            detection_to_finding_ms=detection,
            finding_to_decision_ms=finding,
            end_to_end_decision_ms=end_to_end,
            improvement_claim=INSUFFICIENT_DATA_MESSAGE,
        )

    def render_summary(self) -> str:
        report = self.snapshot()
        lines = [
            "HKRC Assist decision latency (descriptive only)",
            f"detection_to_finding_ms={report.detection_to_finding_ms}",
            f"finding_to_decision_ms={report.finding_to_decision_ms}",
            f"end_to_end_decision_ms={report.end_to_end_decision_ms}",
            f"{report.improvement_claim}",
        ]
        return "\n".join(lines)

    def _require_started(self, action: str) -> None:
        if self._observation_tick is None:
            raise ValueError(f"{action} requires observation_started first")

    def _require_finding(self, action: str) -> None:
        if self._finding_tick is None:
            raise ValueError(f"{action} requires finding_created first")


def _elapsed_ms(start: float | None, end: float | None) -> int | None:
    if start is None or end is None:
        return None
    if end < start:
        raise ValueError("monotonic clock moved backwards")
    return round((end - start) * 1000)


def _wall_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("wall-clock timestamp must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")
