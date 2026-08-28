from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from hkrc.assist import (
    APPROVED_SOURCE_CONTRACT,
    AssistClassification,
    AssistEvidence,
    AssistRecommendation,
    ClassificationClass,
    Confidence,
    Evidence,
    EvidenceIntegrity,
    RecommendationTarget,
    StaticObservationSource,
    analyze,
    build_context_packet,
    observe,
    recommend,
    render_candidate_card,
    render_html_report,
)
from hkrc.assist_ledger import AppendOnlyLedger
from fixtures.assist_replay.replay import load_events

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "assist_replay" / "events.jsonl"
WINDOW_END = datetime(2026, 1, 12, 2, 0, tzinfo=timezone.utc)

# The fixture's causal sequence: review rejected/deferred -> canonical gate
# blocked -> no merge in the window.  Both review-decision rows map to the
# classifier's missing-review predicate; the plain implementation_complete
# row carries no review decision and is excluded from the analysis evidence.
_REVIEW_DECISION_KINDS = frozenset({"review_complete", "canonical_gate_blocked"})


def _fixture_snapshot() -> dict[str, object]:
    """Shape the replay fixture events into the observer's approved source."""
    rows = []
    for event in load_events(FIXTURE_PATH):
        rows.append(
            {
                "board": "board-synthetic",
                "task_id": str(event["event_id"]),
                "run_id": str(event["event_id"]),
                "observed_at": str(event["observed_at_utc"]),
                "status": str(event["status"]),
                "event_kind": str(event["event_kind"]),
                "approval": str(event["approval"]),
                "merge_present": bool(event["merge_present"]),
                "gate_outcome": str(event["gate_outcome"]),
            }
        )
    return {"tasks": rows}


def _to_evidence(observed_evidence: tuple[dict[str, Any], ...]) -> tuple[Evidence, ...]:
    """Adapter across the observe -> classify seam: normalized rows to Evidence."""
    items = []
    for row in observed_evidence:
        payload = dict(row)
        payload.pop("schema_version", None)
        payload.pop("evidence_id", None)
        payload.pop("window_fingerprint", None)
        payload.pop("scope_ref", None)
        payload.pop("task_ref", None)
        payload.pop("run_ref", None)
        # only rows carrying an actual review decision enter the classifier
        if payload.get("event_kind") not in _REVIEW_DECISION_KINDS:
            continue
        items.append(
            Evidence(
                evidence_id=str(row["evidence_id"]),
                event_kind="review_missing",
                source_integrity=EvidenceIntegrity.OBSERVED,
                normalized_payload=payload,
                observed_at_utc=str(row.get("observed_at_utc")),
            )
        )
    return tuple(items)


def test_observe_classify_recommend_ledger_render_pipeline(tmp_path: Path) -> None:
    """End-to-end: observe -> classify -> recommend -> ledger -> render on the replay fixture."""
    # 1. observe (read-only, opaque evidence)
    observed = observe(
        StaticObservationSource(contract=APPROVED_SOURCE_CONTRACT, snapshot=_fixture_snapshot()),
        now=WINDOW_END,
        observer_run_id="integration-run-001",
    )
    assert observed.evidence
    packet = build_context_packet(observed)
    assert packet["schema_version"] == "hkrc.assist.context.v1"
    encoded_packet = json.dumps(packet, sort_keys=True)
    assert "/home/" not in encoded_packet and "opaque:" in encoded_packet

    # 2. classify/analyze (deterministic, model-free)
    evidence = _to_evidence(observed.evidence)
    assert len(evidence) >= 2, "fixture must yield at least two review-decision rows"
    analysis = analyze(
        evidence,
        model=None,
        analysis_id="analysis-integration-001",
        window_fingerprint=str(observed.window.window_fingerprint),
    )
    assert analysis.classification_class is ClassificationClass.MISSING_REVIEW
    assert analysis.confidence is Confidence.HIGH
    assert analysis.actionable
    assert all(item.evidence_id.startswith("opaque:") for item in analysis.evidence)

    # 3. recommend (prevention-only, human-gated)
    recommendation = recommend(
        analysis,
        target=RecommendationTarget(),
    )
    assert recommendation.intent == "prevention_only"
    assert recommendation.human_in_loop == "yes"
    assert recommendation.state == "pending"
    assert recommendation.evidence_refs == tuple(f"evidence:{item.evidence_id}" for item in analysis.evidence)

    # 4. render (portable card + self-contained HTML, no machine identifiers)
    assist_evidence = tuple(
        AssistEvidence(
            evidence_id=item.evidence_id,
            source_kind="task_event",
            summary=analysis.summary,
            source_integrity="observed",
        )
        for item in analysis.evidence
    )
    assist_classification = AssistClassification(
        class_name=analysis.classification_class.value,
        confidence=analysis.confidence.value,
        uncertainty=analysis.uncertainty,
        recurrence=analysis.recurrence.value,
    )
    assist_recommendation = AssistRecommendation(
        recommendation_id=recommendation.recommendation_id,
        analysis_id=recommendation.analysis_id,
        strength=recommendation.strength.value,
        problem_signature=recommendation.problem_signature,
        proposed_change=recommendation.proposed_change,
        logical_target=recommendation.target.logical_target,
        validation_plan=recommendation.validation_plan,
        evidence_refs=recommendation.evidence_refs,
        safety_impact=recommendation.safety_impact,
    )
    card = render_candidate_card(assist_recommendation, assist_classification, assist_evidence)
    html = render_html_report(assist_recommendation, assist_classification, assist_evidence)
    assert "HKRC Assist" in card
    assert "pending" in card.lower()
    assert "<html" in html and "HKRC Assist recommendation" in html
    assert "/home/" not in card and "/home/" not in html
    assert "pending" in html.lower()

    # 5. ledger (append-only record of the pending recommendation)
    ledger = AppendOnlyLedger(tmp_path / "ledger.sqlite3", retention_records=10)
    record = ledger.append(
        subject=analysis.analysis_id,
        phase="recommendation",
        event="pending",
        actor="deterministic_controller",
        evidence_refs=recommendation.evidence_refs,
        window_fingerprint=analysis.window_fingerprint,
        details={"intent": "prevention_only", "state": "pending"},
    )
    assert record.schema_version == "hkrc.assist.ledger.v1"
    assert record.phase == "recommendation" and record.event == "pending"
    assert record.evidence_refs == recommendation.evidence_refs
    stored = ledger.records()
    assert stored == (record,)
