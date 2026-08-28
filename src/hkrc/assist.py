"""HKRC Assist sidecar: read-only observation, context packets, fail-closed
AI classification/recommendation surfaces, and the Phase-1 recommendation queue.

Three deliberately separated surfaces live in this module:

1. Read-only observation (observer contract ``normalized_read_only_v1``):
   accepts an already-approved normalized observation source. It never opens
   Hermes databases, invokes the Hermes CLI, or exposes a mutation boundary.
   Raw source identifiers are used only to derive opaque references and are
   never copied into a context packet.

2. Fail-closed analysis (classifier/recommendation): deterministic predicates
   and a bounded AI seam that fails closed on every model problem and only
   emits prevention-only, human-gated recommendations.

3. Recommendation-only Phase-1 sidecar (review queue): renders bounded
   evidence and stores operator decisions. It has no execution or native
   Hermes integration.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import html
import json
import re
import sqlite3
from typing import Any, Protocol

from .state import ControllerState

WINDOW_SCHEMA = "hkrc.assist.window.v1"
EVIDENCE_SCHEMA = "hkrc.assist.evidence.v1"
CONTEXT_SCHEMA = "hkrc.assist.context.v1"
APPROVED_SOURCE_CONTRACT = "normalized_read_only_v1"
SCOPE = "all_profiles_all_boards"
_WINDOW_SECONDS = 24 * 60 * 60
MAX_EVIDENCE_ITEMS = 1_000
MAX_CONTEXT_PACKET_BYTES = 1_048_576
_SOURCE_INTEGRITY_VALUES = frozenset({"observed", "partial", "unverified"})
_SOURCE_SURFACES = (
    ("sessions", "session"),
    ("commands", "command"),
    ("tasks", "task_event"),
    ("events", "task_event"),
    ("runs", "task_run"),
    ("review_gaps", "review_gap"),
    ("review_gap", "review_gap"),
)
_SECRET_KEY = re.compile(r"(?:token|secret|password|credential|authorization|api[_-]?key)", re.I)
_IDENTIFIER_KEY = re.compile(r"(?:^|_)(?:id|ref|slug|path|uri|url)$|(?:profile|board|task|run|session|command)", re.I)
_MACHINE_VALUE = re.compile(r"(?:^|/)(?:home|root|tmp|var|etc|opt|mnt|workspace)(?:/|$)|[A-Za-z]:[\\/]")
_SECRET_VALUE = re.compile(r"(?:bearer\s+|sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_-]{8,}|secret[-_ ]?shaped)", re.I)
_SAFE_VALUE_KEYS = frozenset(
    {
        "observed_at",
        "observed_at_utc",
        "status",
        "kind",
        "event_kind",
        "outcome",
        "block_kind",
        "error_code",
        "source_integrity",
    }
)


class ObservationContractUnavailable(RuntimeError):
    """Raised when a source does not implement the approved read contract."""


class ObservationSource(Protocol):
    @property
    def contract(self) -> str | None: ...

    @property
    def snapshot(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StaticObservationSource:
    """Small dependency-injection source used by replay/tests.

    Production adapters can provide the same two read-only attributes without
    importing native database or CLI code into this module.
    """

    contract: str | None
    snapshot: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ObservationWindow:
    schema_version: str
    instance_ref: str
    window_start_utc: str
    window_end_utc: str
    window_fingerprint: str
    scope: str
    observer_run_id: str
    source_contract: str

    def as_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "instance_ref": self.instance_ref,
            "window_start_utc": self.window_start_utc,
            "window_end_utc": self.window_end_utc,
            "window_fingerprint": self.window_fingerprint,
            "scope": self.scope,
            "observer_run_id": self.observer_run_id,
            "source_contract": self.source_contract,
        }


@dataclass(frozen=True, slots=True)
class ObservationResult:
    window: ObservationWindow
    evidence: tuple[dict[str, Any], ...]
    normalized_snapshot: Mapping[str, Any]


def observe(
    source: ObservationSource,
    *,
    now: datetime | None = None,
    observer_run_id: str | None = None,
) -> ObservationResult:
    """Observe one closed UTC 24-hour window from an approved source."""

    if getattr(source, "contract", None) != APPROVED_SOURCE_CONTRACT:
        raise ObservationContractUnavailable(
            "approved normalized read-only observation contract is unavailable"
        )
    raw_snapshot = getattr(source, "snapshot", None)
    if not isinstance(raw_snapshot, Mapping):
        raise ObservationContractUnavailable("observation source snapshot is unavailable")

    end = _utc(now or datetime.now(timezone.utc))
    start = end - timedelta(seconds=_WINDOW_SECONDS)
    normalized = _normalize_snapshot(raw_snapshot, start=start, end=end)
    canonical = _canonical_json(
        {
            "window_start_utc": _iso(start),
            "window_end_utc": _iso(end),
            "scope": SCOPE,
            "snapshot": normalized,
        }
    )
    fingerprint = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    run_ref = _opaque(observer_run_id or ("observer:" + fingerprint))
    window = ObservationWindow(
        schema_version=WINDOW_SCHEMA,
        instance_ref=_opaque("instance:all-profiles"),
        window_start_utc=_iso(start),
        window_end_utc=_iso(end),
        window_fingerprint=fingerprint,
        scope=SCOPE,
        observer_run_id=run_ref,
        source_contract=APPROVED_SOURCE_CONTRACT,
    )
    evidence = _build_evidence(normalized, fingerprint)
    return ObservationResult(window=window, evidence=tuple(evidence), normalized_snapshot=normalized)


def build_context_packet(result: ObservationResult) -> dict[str, Any]:
    """Build a bounded, canonical packet suitable for downstream analysis."""

    if len(result.evidence) > MAX_EVIDENCE_ITEMS:
        raise ObservationContractUnavailable(
            f"context evidence exceeds item bound of {MAX_EVIDENCE_ITEMS}"
        )
    by_kind: dict[str, int] = {}
    integrity: dict[str, int] = {}
    for item in result.evidence:
        kind = str(item["event_kind"])
        by_kind[kind] = by_kind.get(kind, 0) + 1
        state = str(item["source_integrity"])
        integrity[state] = integrity.get(state, 0) + 1
    packet = {
        "schema_version": CONTEXT_SCHEMA,
        "window": result.window.as_dict(),
        "evidence": [dict(item) for item in result.evidence],
        "summary": {
            "evidence_count": len(result.evidence),
            "event_kinds": dict(sorted(by_kind.items())),
            "source_integrity": dict(sorted(integrity.items())),
        },
    }
    packet_size = len(_canonical_json(packet).encode("utf-8"))
    if packet_size > MAX_CONTEXT_PACKET_BYTES:
        raise ObservationContractUnavailable(
            f"context packet exceeds size bound of {MAX_CONTEXT_PACKET_BYTES} bytes"
        )
    return packet


def _build_evidence(snapshot: Mapping[str, Any], fingerprint: str) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for source_kind, rows in _rows(snapshot):
        for index, row in enumerate(rows):
            if len(evidence) >= MAX_EVIDENCE_ITEMS:
                raise ObservationContractUnavailable(
                    f"context evidence exceeds item bound of {MAX_EVIDENCE_ITEMS}"
                )
            evidence_source_kind = (
                "tool_result"
                if source_kind == "command" and row.get("kind") == "tool_result"
                else source_kind
            )
            observed = row.get("observed_at_utc", "")
            event_kind = _event_kind(source_kind, row)
            identity = _canonical_json({"source_kind": source_kind, "index": index, "row": row})
            evidence_id = _opaque("evidence:" + identity)
            evidence.append(
                {
                    "schema_version": EVIDENCE_SCHEMA,
                    "evidence_id": evidence_id,
                    "window_fingerprint": fingerprint,
                    "source_kind": evidence_source_kind,
                    "observed_at_utc": observed,
                    "scope_ref": _opaque("scope:" + str(row.get("profile", row.get("board", "all")))),
                    "task_ref": _opaque("task:" + str(row.get("task_id", "none"))),
                    "run_ref": _opaque("run:" + str(row.get("run_id", "none"))),
                    "event_kind": event_kind,
                    "normalized_payload": _payload(row, evidence_source_kind),
                    "redactions": _redactions(row),
                    "source_integrity": row.get("source_integrity", "observed"),
                }
            )
    evidence.sort(key=lambda item: (item["observed_at_utc"], item["source_kind"], item["evidence_id"]))
    return evidence


def _rows(snapshot: Mapping[str, Any]) -> list[tuple[str, list[Mapping[str, Any]]]]:
    result: list[tuple[str, list[Mapping[str, Any]]]] = []
    for key, source_kind in _SOURCE_SURFACES:
        raw_rows = snapshot.get(key, [])
        if raw_rows is None:
            continue
        if not isinstance(raw_rows, list) or any(not isinstance(row, Mapping) for row in raw_rows):
            raise ObservationContractUnavailable(f"normalized {key} surface is malformed")
        result.append((source_kind, [row for row in raw_rows if isinstance(row, Mapping)]))
    return result


def _normalize_snapshot(snapshot: Mapping[str, Any], *, start: datetime, end: datetime) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for source_key, _source_kind in _SOURCE_SURFACES:
        raw_rows = snapshot.get(source_key, [])
        if raw_rows is None:
            normalized[source_key] = []
            continue
        if not isinstance(raw_rows, list) or any(not isinstance(row, Mapping) for row in raw_rows):
            raise ObservationContractUnavailable(f"normalized {source_key} surface is malformed")
        kept = []
        for row in raw_rows:
            observed = _parse_time(row.get("observed_at", row.get("observed_at_utc")))
            if observed is None:
                raise ObservationContractUnavailable(
                    f"normalized {source_key} surface contains an invalid observation timestamp"
                )
            if start <= observed <= end:
                kept.append(_normalize_row(row))
        normalized[source_key] = sorted(kept, key=_canonical_json)
    return normalized


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in sorted(str(k) for k in row):
        value = row[key]
        if key in {"observed_at", "observed_at_utc"}:
            parsed = _parse_time(value)
            result["observed_at_utc"] = _iso(parsed) if parsed is not None else "[REDACTED]"
        elif _SECRET_KEY.search(key):
            result[key] = "[REDACTED]"
        elif key == "source_integrity":
            if value not in _SOURCE_INTEGRITY_VALUES:
                raise ObservationContractUnavailable(
                    "normalized row contains invalid source_integrity"
                )
            result[key] = value
        elif _IDENTIFIER_KEY.search(key):
            result[key] = _opaque(f"{key}:{value}")
        elif key not in _SAFE_VALUE_KEYS:
            result[key] = "[REDACTED]"
        else:
            result[key] = _normalize_value(value)
    return result


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _normalize_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0])) if not _SECRET_KEY.search(str(k))}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value[:20]]
    if isinstance(value, str):
        if _MACHINE_VALUE.search(value) or _SECRET_VALUE.search(value):
            return "[REDACTED]"
        return " ".join(value.split())[:500]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return "[REDACTED]"


def _payload(row: Mapping[str, Any], source_kind: str) -> dict[str, Any]:
    allowed = ("status", "kind", "event_kind", "outcome", "block_kind", "error_code", "source_integrity")
    payload = {key: _normalize_value(row[key]) for key in allowed if key in row}
    payload["source_surface"] = source_kind
    return payload


def _redactions(row: Mapping[str, Any]) -> list[str]:
    labels = {"machine_path", "opaque_identifier"}
    for key, value in row.items():
        if _SECRET_KEY.search(str(key)) or _SECRET_VALUE.search(str(value)):
            labels.add("secret_value")
        if isinstance(value, str) and _MACHINE_VALUE.search(value):
            labels.add("machine_path")
        if _IDENTIFIER_KEY.search(str(key)):
            labels.add("source_identifier")
    return sorted(labels)


def _event_kind(source_kind: str, row: Mapping[str, Any]) -> str:
    value = row.get("event_kind", row.get("kind", row.get("status", source_kind)))
    if not isinstance(value, str) or not value.strip():
        return "source_error"
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")[:80] or "source_error"


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).replace(microsecond=0)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _opaque(value: str) -> str:
    return "opaque:" + hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()[:24]


class _ValueEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class ClassificationClass(_ValueEnum):
    MISSING_REVIEW = "missing_review"
    STALLED_REVIEW = "stalled_review"
    BLOCKED_REVIEW_HANDOFF = "blocked_review_handoff"
    SOURCE_INTEGRITY = "source_integrity"
    UNKNOWN = "unknown"


class Confidence(_ValueEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class Recurrence(_ValueEnum):
    FIRST_SEEN = "first_seen"
    RECURS_IN_2_WINDOWS = "recurs_in_2_windows"
    LINKED_PRIOR_INCIDENT = "linked_prior_incident"
    UNKNOWN = "unknown"


class AIStatus(_ValueEnum):
    NOT_INVOKED = "not_invoked"
    SUCCEEDED = "succeeded"
    TIMEOUT = "timeout"
    MALFORMED = "malformed"
    UNAVAILABLE = "unavailable"
    REJECTED = "rejected"


class EvidenceIntegrity(_ValueEnum):
    OBSERVED = "observed"
    PARTIAL = "partial"
    UNVERIFIED = "unverified"


class RecommendationStrength(_ValueEnum):
    CANDIDATE = "candidate"
    STRONG_CANDIDATE = "strong_candidate"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


_ALLOWED_TARGET_KINDS = frozenset({"code", "config", "documentation", "test"})
_KNOWN_EVENT_CLASSES: dict[str, ClassificationClass] = {
    "review_missing": ClassificationClass.MISSING_REVIEW,
    "review_stalled": ClassificationClass.STALLED_REVIEW,
    "review_blocked": ClassificationClass.BLOCKED_REVIEW_HANDOFF,
    "review_handoff_blocked": ClassificationClass.BLOCKED_REVIEW_HANDOFF,
    "source_error": ClassificationClass.SOURCE_INTEGRITY,
}
_PROBLEM_SIGNATURES = {
    ClassificationClass.MISSING_REVIEW: "review-integrity/missing-review/v1",
    ClassificationClass.STALLED_REVIEW: "review-integrity/stalled-review/v1",
    ClassificationClass.BLOCKED_REVIEW_HANDOFF: "review-integrity/blocked-review-handoff/v1",
    ClassificationClass.SOURCE_INTEGRITY: "review-integrity/source-integrity/v1",
    ClassificationClass.UNKNOWN: "review-integrity/unknown/v1",
}


class RecommendationValidationError(ValueError):
    """Raised when a recommendation target crosses the HKRC safety boundary."""


class TextModel(Protocol):
    """The only model capability accepted by the analysis engine."""

    def complete(self, prompt: str) -> str:
        """Return text for a bounded prompt; no execution capability is exposed."""


@dataclass(frozen=True, slots=True)
class Evidence:
    """One bounded, normalized read-only observation."""

    evidence_id: str
    event_kind: str
    source_integrity: EvidenceIntegrity = EvidenceIntegrity.OBSERVED
    normalized_payload: Mapping[str, object] = field(default_factory=dict)
    observed_at_utc: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.evidence_id, str) or not self.evidence_id:
            raise ValueError("evidence_id must be a non-empty string")
        if not isinstance(self.event_kind, str) or not self.event_kind:
            raise ValueError("event_kind must be a non-empty string")
        if not isinstance(self.source_integrity, EvidenceIntegrity):
            object.__setattr__(self, "source_integrity", EvidenceIntegrity(self.source_integrity))
        if not isinstance(self.normalized_payload, Mapping):
            raise ValueError("normalized_payload must be a mapping")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hkrc.assist.evidence.v1",
            "evidence_id": self.evidence_id,
            "event_kind": self.event_kind,
            "observed_at_utc": self.observed_at_utc,
            "normalized_payload": dict(self.normalized_payload),
            "source_integrity": self.source_integrity.value,
        }


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    classification_class: ClassificationClass
    confidence: Confidence
    uncertainty: tuple[str, ...]
    recurrence: Recurrence
    problem_signature: str
    actionable: bool
    ai_status: AIStatus = AIStatus.NOT_INVOKED
    model_ref: str | None = None

    @property
    def classification(self) -> ClassificationClass:
        return self.classification_class

    def to_dict(self) -> dict[str, object]:
        return {
            "class": self.classification_class.value,
            "confidence": self.confidence.value,
            "uncertainty": list(self.uncertainty),
            "recurrence": self.recurrence.value,
        }


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    """Versioned classification record retaining all source evidence."""

    analysis_id: str
    window_fingerprint: str
    problem_signature: str
    summary: str
    evidence: tuple[Evidence, ...]
    classification: ClassificationResult
    ai_status: AIStatus
    model_ref: str | None = None

    @property
    def classification_class(self) -> ClassificationClass:
        return self.classification.classification_class

    @property
    def confidence(self) -> Confidence:
        return self.classification.confidence

    @property
    def recurrence(self) -> Recurrence:
        return self.classification.recurrence

    @property
    def uncertainty(self) -> tuple[str, ...]:
        return self.classification.uncertainty

    @property
    def actionable(self) -> bool:
        return self.classification.actionable and self.ai_status in {
            AIStatus.NOT_INVOKED,
            AIStatus.SUCCEEDED,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hkrc.assist.analysis.v1",
            "analysis_id": self.analysis_id,
            "window_fingerprint": self.window_fingerprint,
            "problem_signature": self.problem_signature,
            "summary": self.summary,
            "observations": [f"evidence:{item.evidence_id}" for item in self.evidence],
            "classification": self.classification.to_dict(),
            "ai_status": self.ai_status.value,
            "model_ref": self.model_ref,
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class RecommendationTarget:
    ownership: str = "hkrc"
    kind: str = "code"
    logical_target: str = "review-gap policy boundary"


@dataclass(frozen=True, slots=True)
class Recommendation:
    recommendation_id: str
    analysis_id: str
    strength: RecommendationStrength
    intent: str
    problem_signature: str
    proposed_change: str
    target: RecommendationTarget
    validation_plan: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    safety_impact: str
    human_in_loop: str
    state: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hkrc.assist.recommendation.v1",
            "recommendation_id": self.recommendation_id,
            "analysis_id": self.analysis_id,
            "strength": self.strength.value,
            "intent": self.intent,
            "problem_signature": self.problem_signature,
            "proposed_change": self.proposed_change,
            "target": {
                "ownership": self.target.ownership,
                "kind": self.target.kind,
                "logical_target": self.target.logical_target,
            },
            "validation_plan": list(self.validation_plan),
            "evidence_refs": list(self.evidence_refs),
            "safety_impact": self.safety_impact,
            "human_in_loop": self.human_in_loop,
            "state": self.state,
        }


class DeterministicStubModel:
    """Replay model used by tests and offline demos."""

    def __init__(self, response: str) -> None:
        self.response = response

    def complete(self, prompt: str) -> str:
        del prompt
        return self.response


class LocalHermesTextModel:
    """Provider-neutral local text seam with only bounded text completion."""

    def __init__(self, complete: Callable[[str], str], *, model_ref: str = "local-profile-opaque") -> None:
        self._complete = complete
        self.model_ref = model_ref

    def complete(self, prompt: str) -> str:
        return self._complete(prompt)


def classify(
    evidence: Iterable[Evidence],
    *,
    prior_problem_signatures: Sequence[str] = (),
    linked_prior_incident: bool = False,
) -> ClassificationResult:
    """Classify only deterministic predicates; uncertainty always wins."""
    items = tuple(evidence)
    if not items:
        return _unknown("no evidence")
    if any(item.source_integrity is not EvidenceIntegrity.OBSERVED for item in items):
        return _result(
            ClassificationClass.SOURCE_INTEGRITY,
            Confidence.NONE,
            ("source integrity is not fully observed",),
            items,
            prior_problem_signatures,
            linked_prior_incident,
        )

    known = [_KNOWN_EVENT_CLASSES.get(item.event_kind) for item in items]
    classes = {item_class for item_class in known if item_class is not None}
    unknown_events = [item.event_kind for item in items if item.event_kind not in _KNOWN_EVENT_CLASSES]
    if unknown_events or len(classes) != 1:
        reasons: list[str] = []
        if unknown_events:
            reasons.append("unknown event kind: " + ", ".join(sorted(set(unknown_events))))
        if len(classes) > 1:
            reasons.append("conflicting deterministic evidence")
        return _unknown("; ".join(reasons) or "no recognized failure predicate")

    failure_class = next(iter(classes))
    return _result(
        failure_class,
        Confidence.HIGH,
        (),
        items,
        prior_problem_signatures,
        linked_prior_incident,
    )


def analyze(
    evidence: Iterable[Evidence],
    *,
    model: TextModel | None = None,
    prior_problem_signatures: Sequence[str] = (),
    linked_prior_incident: bool = False,
    analysis_id: str = "analysis-opaque",
    window_fingerprint: str = "sha256:offline",
) -> AnalysisResult:
    """Produce an analysis record while failing closed on every model problem."""
    items = tuple(evidence)
    deterministic = classify(
        items,
        prior_problem_signatures=prior_problem_signatures,
        linked_prior_incident=linked_prior_incident,
    )
    ai_status = AIStatus.NOT_INVOKED
    model_ref: str | None = None
    summary = _default_summary(deterministic.classification_class)

    if model is not None:
        try:
            model_ref = getattr(model, "model_ref", "local-profile-opaque")
            raw = model.complete(_bounded_prompt(items, deterministic))
            if raw == "TIMEOUT":
                raise TimeoutError("model timeout")
            if raw == "UNAVAILABLE":
                raise OSError("model unavailable")
            parsed = json.loads(raw)
            if not isinstance(parsed, Mapping):
                raise ValueError("model response must be an object")
            candidate_summary = parsed.get("summary")
            if candidate_summary is not None and not isinstance(candidate_summary, str):
                raise ValueError("model summary must be a string")
            ai_status = AIStatus.SUCCEEDED
            summary = candidate_summary or summary
        except TimeoutError:
            ai_status = AIStatus.TIMEOUT
        except PermissionError:
            ai_status = AIStatus.REJECTED
        except (ConnectionError, OSError):
            ai_status = AIStatus.UNAVAILABLE
        except (TypeError, ValueError, json.JSONDecodeError):
            ai_status = AIStatus.MALFORMED
        except Exception:
            # Provider-specific failures must never escape the fail-closed seam.
            ai_status = AIStatus.UNAVAILABLE

    if ai_status not in {AIStatus.NOT_INVOKED, AIStatus.SUCCEEDED}:
        deterministic = _with_ai_status(deterministic, ai_status, model_ref)

    return AnalysisResult(
        analysis_id=analysis_id,
        window_fingerprint=window_fingerprint,
        problem_signature=deterministic.problem_signature,
        summary=summary,
        evidence=items,
        classification=deterministic,
        ai_status=ai_status,
        model_ref=model_ref,
    )


def recommend(analysis: AnalysisResult, *, target: RecommendationTarget) -> Recommendation:
    """Create a pending prevention candidate, never an executable action."""
    _validate_target(target)
    strength = (
        RecommendationStrength.INSUFFICIENT_EVIDENCE
        if not analysis.actionable
        else RecommendationStrength.STRONG_CANDIDATE
        if analysis.confidence is Confidence.HIGH
        else RecommendationStrength.CANDIDATE
    )
    proposed = {
        ClassificationClass.MISSING_REVIEW: "Add or enforce a deterministic review-pair integrity check in HKRC.",
        ClassificationClass.STALLED_REVIEW: "Add deterministic visibility and escalation for stalled review handoffs in HKRC.",
        ClassificationClass.BLOCKED_REVIEW_HANDOFF: "Add a deterministic guard for blocked review handoff ownership in HKRC.",
        ClassificationClass.SOURCE_INTEGRITY: "Improve bounded source-integrity validation before classifying review evidence.",
        ClassificationClass.UNKNOWN: "Collect additional bounded evidence before proposing a prevention change.",
    }[analysis.classification_class]
    return Recommendation(
        recommendation_id="rec-" + analysis.analysis_id.removeprefix("analysis-"),
        analysis_id=analysis.analysis_id,
        strength=strength,
        intent="prevention_only",
        problem_signature=analysis.problem_signature,
        proposed_change=proposed,
        target=target,
        validation_plan=("focused deterministic test", "full policy regression test"),
        evidence_refs=tuple(f"evidence:{item.evidence_id}" for item in analysis.evidence),
        safety_impact="prevents silent review handoff loss" if analysis.actionable else "preserves evidence without guessing",
        human_in_loop="yes",
        state="pending",
    )


def _result(
    failure_class: ClassificationClass,
    confidence: Confidence,
    uncertainty: tuple[str, ...],
    items: Sequence[Evidence],
    prior_problem_signatures: Sequence[str],
    linked_prior_incident: bool,
) -> ClassificationResult:
    signature = _PROBLEM_SIGNATURES[failure_class]
    if linked_prior_incident:
        recurrence = Recurrence.LINKED_PRIOR_INCIDENT
    elif signature in prior_problem_signatures:
        recurrence = Recurrence.RECURS_IN_2_WINDOWS
    else:
        recurrence = Recurrence.FIRST_SEEN if items else Recurrence.UNKNOWN
    return ClassificationResult(
        classification_class=failure_class,
        confidence=confidence,
        uncertainty=uncertainty,
        recurrence=recurrence,
        problem_signature=signature,
        actionable=failure_class not in {ClassificationClass.SOURCE_INTEGRITY, ClassificationClass.UNKNOWN},
    )


def _unknown(reason: str) -> ClassificationResult:
    return ClassificationResult(
        classification_class=ClassificationClass.UNKNOWN,
        confidence=Confidence.NONE,
        uncertainty=(reason,),
        recurrence=Recurrence.UNKNOWN,
        problem_signature=_PROBLEM_SIGNATURES[ClassificationClass.UNKNOWN],
        actionable=False,
    )


def _with_ai_status(result: ClassificationResult, status: AIStatus, model_ref: str | None) -> ClassificationResult:
    return ClassificationResult(
        classification_class=result.classification_class,
        confidence=result.confidence,
        uncertainty=result.uncertainty + (f"AI result {status.value} is non-actionable",),
        recurrence=result.recurrence,
        problem_signature=result.problem_signature,
        actionable=False,
        ai_status=status,
        model_ref=model_ref,
    )


def _default_summary(failure_class: ClassificationClass) -> str:
    return {
        ClassificationClass.MISSING_REVIEW: "A completed implementation has no usable review handoff.",
        ClassificationClass.STALLED_REVIEW: "A review handoff is present but has stalled.",
        ClassificationClass.BLOCKED_REVIEW_HANDOFF: "A review handoff is blocked.",
        ClassificationClass.SOURCE_INTEGRITY: "The source evidence cannot be fully verified.",
        ClassificationClass.UNKNOWN: "The evidence does not establish a controlled failure class.",
    }[failure_class]


def _bounded_prompt(items: Sequence[Evidence], result: ClassificationResult) -> str:
    return json.dumps(
        {
            "task": "Summarize bounded HKRC evidence; do not propose executable actions.",
            "deterministic_class": result.classification_class.value,
            "evidence": [item.to_dict() for item in items],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _validate_target(target: RecommendationTarget) -> None:
    if target.ownership != "hkrc":
        raise RecommendationValidationError("recommendation target ownership must be hkrc")
    if target.kind not in _ALLOWED_TARGET_KINDS:
        raise RecommendationValidationError("recommendation target kind is outside the HKRC-only boundary")
    if not target.logical_target or any(
        token in target.logical_target.lower() for token in ("shell", "network", "cli", "deploy", "merge")
    ):
        raise RecommendationValidationError("recommendation logical target is unsafe")


# Re-exporting only immutable, pure helpers is unnecessary for callers; the
# explicit public surface above is the intended sidecar boundary.
_STRENGTH_LABELS = {
    "candidate": "Candidate",
    "strong_candidate": "Strong candidate",
    "insufficient_evidence": "Insufficient evidence",
}
_TERMINAL_STATES = frozenset({"approved", "rejected", "deferred"})
_SAFE_INTENT = "prevention_only"
_SAFE_OWNERSHIP = "hkrc"


@dataclass(frozen=True, slots=True)
class AssistEvidence:
    evidence_id: str
    source_kind: str
    summary: str
    source_integrity: str = "observed"


@dataclass(frozen=True, slots=True)
class AssistClassification:
    class_name: str
    confidence: str
    uncertainty: tuple[str, ...] = ()
    recurrence: str = "unknown"


@dataclass(frozen=True, slots=True)
class AssistRecommendation:
    recommendation_id: str
    analysis_id: str
    strength: str
    problem_signature: str
    proposed_change: str
    logical_target: str
    validation_plan: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    safety_impact: str
    human_in_loop: str = "yes"
    state: str = "pending"


@dataclass(frozen=True, slots=True)
class Candidate:
    recommendation_id: str
    analysis_id: str
    problem_signature: str
    strength: str
    intent: str
    proposed_change: str
    target: Mapping[str, Any]
    validation_plan: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    safety_impact: str
    human_in_loop: str
    state: str
    created_at: str
    updated_at: str


class QueueTransitionError(ValueError):
    """Raised when a recommendation is unsafe or has no legal state transition."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _candidate_from_row(row: Mapping[str, Any]) -> Candidate:
    return Candidate(
        recommendation_id=str(row["recommendation_id"]),
        analysis_id=str(row["analysis_id"] or ""),
        problem_signature=str(row["problem_signature"] or ""),
        strength=str(row["strength"] or "insufficient_evidence"),
        intent=str(row["intent"]),
        proposed_change=str(row["proposed_change"] or ""),
        target=json.loads(str(row["target_json"] or "{}")),
        validation_plan=tuple(json.loads(str(row["validation_plan_json"] or "[]"))),
        evidence_refs=tuple(json.loads(str(row["evidence_refs_json"] or "[]"))),
        safety_impact=str(row["safety_impact"] or ""),
        human_in_loop=str(row["human_in_loop"]),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


class CandidateQueue:
    """Controller-owned pending recommendation queue.

    The queue only accepts prevention recommendations targeting HKRC. Decisions
    append an event and update the current projection; no decision invokes an
    executor or changes native Hermes state.
    """

    def __init__(self, state: ControllerState):
        self.state = state

    def enqueue(self, recommendation: Mapping[str, Any]) -> Candidate:
        required = ("recommendation_id", "intent", "state")
        missing = [key for key in required if key not in recommendation]
        if missing:
            raise QueueTransitionError(f"missing recommendation fields: {', '.join(missing)}")
        if recommendation["intent"] != _SAFE_INTENT:
            raise QueueTransitionError("recommendation intent must be prevention_only")
        if "target" not in recommendation:
            raise QueueTransitionError("recommendation target ownership must be hkrc")
        target = recommendation["target"]
        if not isinstance(target, Mapping) or target.get("ownership") != _SAFE_OWNERSHIP:
            raise QueueTransitionError("recommendation target ownership must be hkrc")
        if recommendation["state"] != "pending":
            raise QueueTransitionError("new recommendations must be pending")
        recommendation_id = str(recommendation["recommendation_id"])
        if not recommendation_id:
            raise QueueTransitionError("recommendation_id must not be empty")
        now = _now()
        values = (
            recommendation_id,
            str(recommendation.get("analysis_id", "")),
            str(recommendation.get("problem_signature", "")),
            str(recommendation.get("strength", "insufficient_evidence")),
            _SAFE_INTENT,
            str(recommendation.get("proposed_change", "")),
            json.dumps(dict(target), sort_keys=True, separators=(",", ":")),
            json.dumps(list(recommendation.get("validation_plan", ())), sort_keys=True),
            json.dumps(list(recommendation.get("evidence_refs", ())), sort_keys=True),
            str(recommendation.get("safety_impact", "")),
            "yes" if recommendation.get("human_in_loop", "yes") is True else str(recommendation.get("human_in_loop", "yes")),
            "pending",
            now,
            now,
        )
        try:
            self.state.connection.execute(
                """INSERT INTO assist_candidates
                (recommendation_id, analysis_id, problem_signature, strength, intent,
                 proposed_change, target_json, validation_plan_json, evidence_refs_json,
                 safety_impact, human_in_loop, state, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                values,
            )
            self.state.connection.commit()
        except Exception as exc:
            self.state.connection.rollback()
            raise QueueTransitionError(f"candidate could not be queued: {exc}") from exc
        return self.get(recommendation_id)

    def get(self, recommendation_id: str) -> Candidate:
        row = self.state.connection.execute(
            "SELECT * FROM assist_candidates WHERE recommendation_id = ?",
            (recommendation_id,),
        ).fetchone()
        if row is None:
            raise QueueTransitionError(f"candidate not found: {recommendation_id}")
        return _candidate_from_row(row)

    def pending(self) -> tuple[Candidate, ...]:
        rows = self.state.connection.execute(
            "SELECT * FROM assist_candidates WHERE state = 'pending' ORDER BY created_at, recommendation_id"
        ).fetchall()
        return tuple(_candidate_from_row(row) for row in rows)

    def decide(self, recommendation_id: str, decision: str, *, note: str = "") -> Candidate:
        if decision not in _TERMINAL_STATES:
            raise QueueTransitionError("decision must be approved, rejected, or deferred")
        now = _now()
        connection = self.state.connection
        try:
            connection.execute("BEGIN IMMEDIATE")
            candidate = self.get(recommendation_id)
            if candidate.state != "pending":
                raise QueueTransitionError(f"candidate is already {candidate.state}")
            cursor = connection.execute(
                "UPDATE assist_candidates SET state = ?, updated_at = ? "
                "WHERE recommendation_id = ? AND state = 'pending'",
                (decision, now, recommendation_id),
            )
            if cursor.rowcount != 1:
                raise QueueTransitionError("candidate transition lost the race")
            connection.execute(
                "INSERT INTO assist_candidate_events "
                "(recommendation_id, decision, note, occurred_at) VALUES (?, ?, ?, ?)",
                (recommendation_id, decision, note, now),
            )
            connection.commit()
        except QueueTransitionError:
            connection.rollback()
            raise
        except sqlite3.OperationalError as exc:
            connection.rollback()
            raise QueueTransitionError(f"candidate transition failed: {exc}") from exc
        except Exception as exc:
            connection.rollback()
            raise QueueTransitionError(f"candidate transition failed: {exc}") from exc
        return self.get(recommendation_id)

    def events(self, recommendation_id: str) -> tuple[dict[str, str], ...]:
        rows = self.state.connection.execute(
            "SELECT decision, note, occurred_at FROM assist_candidate_events WHERE recommendation_id = ? ORDER BY id",
            (recommendation_id,),
        ).fetchall()
        return tuple({"decision": str(row["decision"]), "note": str(row["note"]), "occurred_at": str(row["occurred_at"])} for row in rows)


_PATH_PATTERN = re.compile(r"(?:[A-Za-z]:)?/(?:[^\s<>\"']+/)*[^\s<>\"']+")
_WINDOWS_PATH_PATTERN = re.compile(r"(?:[A-Za-z]:\\)(?:[^\s<>\"']+\\)*[^\s<>\"']+")
_KEYED_HOST_PATTERN = re.compile(
    r"(?P<label>\b(?:host|hostname|node)\s*[:=]\s*)(?P<value>[A-Za-z0-9][A-Za-z0-9_.-]*)",
    re.IGNORECASE,
)
_KEYED_ID_PATTERN = re.compile(
    r"(?P<label>\b(?:task|run|session|event|analysis|recommendation)(?:[_ -]?id)?\s*[:=]\s*)"
    r"(?P<value>[A-Za-z0-9][A-Za-z0-9_.-]*)",
    re.IGNORECASE,
)
_OPAQUE_TASK_ID_PATTERN = re.compile(r"\bt_[A-Za-z0-9][A-Za-z0-9_-]{4,}\b")
_OPAQUE_PREFIX_ID_PATTERN = re.compile(r"\b(?:t|task|board)[-_][A-Za-z0-9]+\b", re.IGNORECASE)


def _safe_text(value: object) -> str:
    """Bound and normalize report text so it remains portable and inspectable."""
    text = str(value)
    text = _PATH_PATTERN.sub("<machine-path>", text)
    text = _WINDOWS_PATH_PATTERN.sub("<machine-path>", text)
    text = _KEYED_HOST_PATTERN.sub(r"\g<label><machine-host>", text)
    text = _KEYED_ID_PATTERN.sub(r"\g<label><machine-id>", text)
    text = _OPAQUE_TASK_ID_PATTERN.sub("<machine-id>", text)
    text = _OPAQUE_PREFIX_ID_PATTERN.sub("<opaque-id>", text)
    return text


def _evidence_list(evidence: Sequence[AssistEvidence]) -> str:
    return "\n".join(
        f"- {html.escape(_safe_text(item.evidence_id))}: {html.escape(_safe_text(item.summary))} "
        f"({html.escape(_safe_text(item.source_integrity))})"
        for item in evidence
    ) or "- No bounded evidence supplied"


def _evidence_anchor(item: AssistEvidence, index: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", _safe_text(item.evidence_id).lower()).strip("-")
    return f"evidence-{slug or 'item'}"


def _evidence_index(ref: str, evidence: Sequence[AssistEvidence]) -> int | None:
    for index, item in enumerate(evidence, start=1):
        if ref in {item.evidence_id, f"evidence:{item.evidence_id}"}:
            return index
    return None


def render_candidate_card(
    recommendation: AssistRecommendation,
    classification: AssistClassification,
    evidence: Sequence[AssistEvidence],
) -> str:
    """Render a portable plain-text candidate card with explicit pending semantics."""
    uncertainty = "; ".join(_safe_text(item) for item in classification.uncertainty) or "none recorded"
    evidence_ref_lines = tuple(
        f"- {_safe_text(ref)}" for ref in recommendation.evidence_refs
    ) or ("- None supplied",)
    return "\n".join(
        (
            f"HKRC Assist candidate: {_safe_text(recommendation.recommendation_id)}",
            f"Strength: {_STRENGTH_LABELS.get(recommendation.strength, 'Insufficient evidence')}",
            f"Classification: {_safe_text(classification.class_name)} ({_safe_text(classification.confidence)})",
            f"Uncertainty: {uncertainty}",
            f"Recommendation: {_safe_text(recommendation.proposed_change)}",
            f"Target: HKRC / {_safe_text(recommendation.logical_target)}",
            f"Safety impact: {_safe_text(recommendation.safety_impact)}",
            "State: pending (human approval required; Phase 1 does not apply changes)",
            "Evidence:",
            *(f"- {_safe_text(item.evidence_id)}: {_safe_text(item.summary)}" for item in evidence),
            "Evidence refs:",
            *evidence_ref_lines,
            "Explicit non-goals: no unblock, reassign, comment, create, merge, deploy, or live-state mutation.",
        )
    )


def render_html_report(
    recommendation: AssistRecommendation,
    classification: AssistClassification,
    evidence: Sequence[AssistEvidence],
) -> str:
    """Render a self-contained Tailwind-style HTML report with an inline SVG diagram."""
    strength = _STRENGTH_LABELS.get(recommendation.strength, "Insufficient evidence")
    uncertainty = ", ".join(_safe_text(item) for item in classification.uncertainty) or "None recorded"
    before = "Hermes evidence"  # symbolic labels keep reports portable.
    after = "HKRC prevention guard"
    diagram = f"""<svg class="diagram" viewBox="0 0 900 190" role="img" aria-label="Before and after prevention flow">
<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L0,6 L9,3 z" fill="#38bdf8"/></marker></defs>
<rect x="20" y="55" width="220" height="80" rx="14" class="node"/><text x="130" y="102" text-anchor="middle">{html.escape(before)}</text>
<path d="M240 95 H330" class="edge" marker-end="url(#arrow)"/>
<rect x="330" y="55" width="220" height="80" rx="14" class="node"/><text x="440" y="102" text-anchor="middle">Bounded evidence</text>
<path d="M550 95 H640" class="edge" marker-end="url(#arrow)"/>
<rect x="640" y="55" width="240" height="80" rx="14" class="node accent"/><text x="760" y="102" text-anchor="middle">{html.escape(after)}</text>
</svg>"""
    mermaid_source = (
        "flowchart LR\n"
        f"A[{html.escape(before)}] --> B{{bounded evidence}}\n"
        f"B --> C[{html.escape(after)}]\n"
        "C -. pending; operator decision required .-> D[no live mutation]"
    )
    validation = "".join(f"<li>{html.escape(_safe_text(item))}</li>" for item in recommendation.validation_plan)
    evidence_items = "".join(
        f'<li id="{_evidence_anchor(item, index)}">'
        f'<span class="font-semibold">{html.escape(_safe_text(item.evidence_id))}</span>: '
        f'{html.escape(_safe_text(item.summary))} '
        f'<span class="text-slate-400">({html.escape(_safe_text(item.source_integrity))})</span></li>'
        for index, item in enumerate(evidence, start=1)
    ) or "<li>No bounded evidence supplied</li>"
    evidence_refs = "".join(
        (
            f'<li><a class="text-sky-300 underline" href="#{_evidence_anchor(evidence[index - 1], index)}">'
            f'{html.escape(_safe_text(ref))}</a></li>'
            if (index := _evidence_index(ref, evidence)) is not None
            else f'<li id="evidence-ref-{ref_index}"><span class="font-semibold">'
            f'{html.escape(_safe_text(ref))}</span> (no matching bounded item)</li>'
        )
        for ref_index, ref in enumerate(recommendation.evidence_refs, start=1)
    ) or "<li>None supplied</li>"
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>HKRC Assist recommendation</title>
<style id="tailwind-css">
/* Self-contained Tailwind utility subset; no network or runtime dependency. */
:root {{ color-scheme: dark; font-family: ui-sans-serif, system-ui, sans-serif; }}
body {{ margin: 0; }}
.mx-auto {{ margin-left: auto; margin-right: auto; }} .max-w-6xl {{ max-width: 72rem; }}
.space-y-6 > * + * {{ margin-top: 1.5rem; }} .grid {{ display: grid; }}
.gap-4 {{ gap: 1rem; }} .gap-6 {{ gap: 1.5rem; }}
.md\\:grid-cols-2 {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
.bg-slate-950 {{ background: #020617; }} .bg-slate-900 {{ background: #0f172a; }}
.bg-slate-800 {{ background: #1e293b; }} .text-slate-100 {{ color: #f1f5f9; }}
.text-slate-300 {{ color: #cbd5e1; }} .text-slate-400 {{ color: #94a3b8; }}
.text-sky-300 {{ color: #7dd3fc; }} .bg-sky-300 {{ background: #7dd3fc; }} .border-slate-700 {{ border-color: #334155; }}
.rounded-xl {{ border-radius: 0.75rem; }} .rounded-full {{ border-radius: 9999px; }}
.border {{ border-width: 1px; border-style: solid; }} .p-4 {{ padding: 1rem; }} .p-6 {{ padding: 1.5rem; }}
.px-3 {{ padding-left: .75rem; padding-right: .75rem; }} .py-1 {{ padding-top: .25rem; padding-bottom: .25rem; }}
.text-3xl {{ font-size: 1.875rem; line-height: 2.25rem; }} .text-xl {{ font-size: 1.25rem; line-height: 1.75rem; }}
.font-semibold {{ font-weight: 600; }} .font-bold {{ font-weight: 700; }} .underline {{ text-decoration: underline; }}
.diagram {{ width: 100%; min-height: 190px; }}
.node {{ fill: #0f3b55; stroke: #38bdf8; stroke-width: 2; }}
.node.accent {{ fill: #164e63; stroke: #67e8f9; }}
.diagram text {{ fill: #e2e8f0; font: 600 16px system-ui, sans-serif; }}
.edge {{ stroke: #38bdf8; stroke-width: 3; fill: none; }}
@media (max-width: 768px) {{ .md\\:grid-cols-2 {{ grid-template-columns: 1fr; }} }}
</style></head><body>
<main class="mx-auto max-w-6xl space-y-6 bg-slate-950 text-slate-100 p-6">
<h1 class="text-3xl font-bold">HKRC Assist recommendation</h1>
<p><span class="strength-badge rounded-full bg-sky-300 px-3 py-1 font-bold">{html.escape(strength)}</span> <span class="text-slate-400">pending / human-in-the-loop</span></p>
<section class="grid md:grid-cols-2 gap-4">
<div class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Classification</h2><p>{html.escape(_safe_text(classification.class_name))}</p><p>Confidence: {html.escape(_safe_text(classification.confidence))}</p><p>Uncertainty: {html.escape(uncertainty)}</p></div>
<div class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Recommendation</h2><p>{html.escape(_safe_text(recommendation.proposed_change))}</p><p>Target: HKRC / {html.escape(_safe_text(recommendation.logical_target))}</p><p>Safety impact: {html.escape(_safe_text(recommendation.safety_impact))}</p></div>
</section>
<section class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Before / after candidate</h2>
{diagram}
<div class="mermaid" data-mermaid="flowchart LR">{mermaid_source}</div></section>
<section class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Evidence refs</h2><ul>{evidence_refs}</ul><h2 class="text-xl font-semibold">Evidence</h2><ul>{evidence_items}</ul></section>
<section class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Validation plan</h2><ul>{validation or '<li>None supplied</li>'}</ul></section>
<section class="rounded-xl border border-slate-700 bg-slate-900 p-4"><h2 class="text-xl font-semibold">Explicit non-goals</h2><p>No unblock, reassign, comment, create, merge, deploy, systemd, or live Hermes-state mutation. Phase 1 does not apply this recommendation.</p></section>
</main>
</body></html>"""


__all__ = [
    "AIStatus",
    "APPROVED_SOURCE_CONTRACT",
    "AnalysisResult",
    "AssistClassification",
    "AssistEvidence",
    "AssistRecommendation",
    "CONTEXT_SCHEMA",
    "Candidate",
    "CandidateQueue",
    "ClassificationClass",
    "ClassificationResult",
    "Confidence",
    "DeterministicStubModel",
    "EVIDENCE_SCHEMA",
    "Evidence",
    "EvidenceIntegrity",
    "LocalHermesTextModel",
    "MAX_CONTEXT_PACKET_BYTES",
    "MAX_EVIDENCE_ITEMS",
    "ObservationContractUnavailable",
    "ObservationResult",
    "ObservationSource",
    "ObservationWindow",
    "QueueTransitionError",
    "Recommendation",
    "RecommendationStrength",
    "RecommendationTarget",
    "RecommendationValidationError",
    "Recurrence",
    "StaticObservationSource",
    "TextModel",
    "WINDOW_SCHEMA",
    "analyze",
    "build_context_packet",
    "classify",
    "observe",
    "recommend",
    "render_candidate_card",
    "render_html_report",
]

assert _ALLOWED_TARGET_KINDS == {"code", "config", "documentation", "test"}
assert not any(name in globals() for name in ("subprocess", "socket", "pathlib"))
