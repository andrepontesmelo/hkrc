from __future__ import annotations

import json
from dataclasses import replace
from multiprocessing import get_context
from pathlib import Path

import pytest

from hkrc.assist import (
    AIStatus,
    AssistClassification,
    AssistEvidence,
    AssistRecommendation,
    CandidateQueue,
    ClassificationClass,
    Confidence,
    DeterministicStubModel,
    Evidence,
    EvidenceIntegrity,
    LocalHermesTextModel,
    QueueTransitionError,
    Recurrence,
    RecommendationStrength,
    RecommendationTarget,
    RecommendationValidationError,
    analyze,
    classify,
    recommend,
    render_candidate_card,
    render_html_report,
)
from hkrc.config import AssistConfig, ControllerConfig, load_config, write_config
from hkrc.state import ControllerState


def _decide_in_process(db: str, decision: str, start, results) -> None:
    with ControllerState.open_existing(Path(db)) as state:
        start.wait()
        try:
            result = CandidateQueue(state).decide("rec-race", decision)
        except QueueTransitionError as exc:
            results.put(("error", str(exc)))
        else:
            results.put(("success", result.state))


def test_queue_race_has_one_winner_and_one_event(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with ControllerState.initialize(db, "test") as state:
        CandidateQueue(state).enqueue(
            {
                "recommendation_id": "rec-race",
                "intent": "prevention_only",
                "target": {"ownership": "hkrc"},
                "state": "pending",
            }
        )

    context = get_context("spawn")
    start = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_decide_in_process,
            args=(str(db), decision, start, results),
        )
        for decision in ("approved", "rejected")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(10)

    assert all(process.exitcode == 0 for process in processes)
    observed = sorted(results.get() for _ in processes)
    assert observed in (
        sorted([("error", "candidate is already approved"), ("success", "approved")]),
        sorted([("error", "candidate is already rejected"), ("success", "rejected")]),
    )

    with ControllerState.open_existing(db) as state:
        queue = CandidateQueue(state)
        assert queue.get("rec-race").state in {"approved", "rejected"}
        assert len(queue.events("rec-race")) == 1


def test_html_report_redacts_live_identifiers_and_stays_offline(
    recommendation: AssistRecommendation, classification: AssistClassification
) -> None:
    evidence = (
        AssistEvidence(
            "ev-t_abcdef12",
            "task_event",
            "blocked task t_abcdef12 on board board-private",
            "observed",
        ),
    )
    unsafe_recommendation = replace(
        recommendation,
        proposed_change="Inspect /var/lib/hkrc/state.sqlite3 for task t_abcdef12",
    )

    report = render_html_report(unsafe_recommendation, classification, evidence)

    assert "t_abcdef12" not in report
    assert "board-private" not in report
    assert "/var/lib/hkrc/state.sqlite3" not in report
    assert "machine-path" in report
    assert "machine-id" in report
    assert "opaque-id" in report
    assert "<script src=" not in report
    assert "https://" not in report


@pytest.fixture
def recommendation() -> AssistRecommendation:
    return AssistRecommendation(
        recommendation_id="rec-opaque",
        analysis_id="analysis-opaque",
        strength="strong_candidate",
        problem_signature="review-integrity/missing-review/v1",
        proposed_change="Add a deterministic review-pair integrity check in HKRC.",
        logical_target="review-gap policy boundary",
        validation_plan=("focused deterministic test", "full policy regression test"),
        evidence_refs=("evidence:ev-1", "evidence:ev-2"),
        safety_impact="prevents silent review handoff loss",
    )


@pytest.fixture
def classification() -> AssistClassification:
    return AssistClassification(
        class_name="missing_review",
        confidence="high",
        uncertainty=("review child status was unavailable",),
        recurrence="first_seen",
    )


def test_renderer_is_portable_and_contains_candidate_report_sections(
    recommendation: AssistRecommendation, classification: AssistClassification
) -> None:
    evidence = (
        AssistEvidence("ev-1", "review_gap", "review missing", "observed"),
        AssistEvidence("ev-2", "task_event", "implementation completed", "observed"),
    )

    card = render_candidate_card(recommendation, classification, evidence)
    report = render_html_report(recommendation, classification, evidence)

    assert "Strong candidate" in card
    assert "pending" in card
    assert "before" in report.lower() and "after" in report.lower()
    assert "strength-badge" in report
    assert "class=\"mermaid\"" in report
    assert "<svg" in report
    assert "data-mermaid" in report
    assert "Evidence" in report
    assert "Safety impact" in report
    assert "Explicit non-goals" in report
    assert "<script src=" not in report
    assert "https://" not in report
    assert '<style id="tailwind-css">' in report
    assert "bg-slate-950" in report
    assert "rounded-xl" in report
    assert 'href="#evidence-ev-1"' in report
    assert 'id="evidence-ev-1"' in report
    assert "evidence:ev-1" in report
    assert "/home/" not in report
    assert "machine-specific" not in report


def test_renderers_redact_task_host_and_path_identifiers(
    recommendation: AssistRecommendation, classification: AssistClassification
) -> None:
    evidence = (
        AssistEvidence(
            "task:t_abc12345",
            "task_event",
            "task=t_abc12345 host=node-abc path /var/lib/hkrc/state.sqlite3",
        ),
    )

    card = render_candidate_card(recommendation, classification, evidence)
    report = render_html_report(recommendation, classification, evidence)

    for rendered in (card, report):
        assert "t_abc12345" not in rendered
        assert "node-abc" not in rendered
        assert "/var/lib/hkrc/state.sqlite3" not in rendered
        assert "machine-id" in rendered
        assert "machine-host" in rendered
        assert "machine-path" in rendered


def test_queue_transitions_are_explicit_and_append_only(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    with ControllerState.initialize(db, "test") as state:
        queue = CandidateQueue(state)
        candidate = queue.enqueue(
            {
                "recommendation_id": "rec-1",
                "analysis_id": "analysis-1",
                "problem_signature": "source-integrity/v1",
                "strength": "candidate",
                "intent": "prevention_only",
                "proposed_change": "Improve source validation.",
                "target": {"ownership": "hkrc", "kind": "test", "logical_target": "observer"},
                "validation_plan": ["focused test"],
                "evidence_refs": ["evidence:ev-1"],
                "safety_impact": "keeps unavailable evidence non-actionable",
                "human_in_loop": "yes",
                "state": "pending",
            }
        )
        assert candidate.state == "pending"
        assert [row.state for row in queue.pending()] == ["pending"]
        assert queue.decide(candidate.recommendation_id, "deferred", note="operator review later").state == "deferred"
        assert queue.get(candidate.recommendation_id).state == "deferred"
        assert queue.events(candidate.recommendation_id)[-1]["decision"] == "deferred"
        with pytest.raises(QueueTransitionError):
            queue.decide(candidate.recommendation_id, "approved")
        assert state.connection.execute("SELECT COUNT(*) FROM assist_candidate_events").fetchone()[0] == 1


def test_queue_rejects_non_prevention_or_non_hkrc_recommendations(tmp_path: Path) -> None:
    with ControllerState.initialize(tmp_path / "state.sqlite3", "test") as state:
        queue = CandidateQueue(state)
        with pytest.raises(QueueTransitionError, match="prevention_only"):
            queue.enqueue({"recommendation_id": "bad", "intent": "repair", "state": "pending"})
        with pytest.raises(QueueTransitionError, match="hkrc"):
            queue.enqueue(
                {
                    "recommendation_id": "bad-target",
                    "intent": "prevention_only",
                    "target": {"ownership": "native-hermes"},
                    "state": "pending",
                }
            )


def test_human_in_loop_config_defaults_true_and_round_trips(tmp_path: Path) -> None:
    config = ControllerConfig("test", tmp_path / "boards", tmp_path / "state.sqlite3")
    assert AssistConfig().human_in_loop is True
    path = tmp_path / "config.toml"
    write_config(path, config)
    assert load_config(path).assist.human_in_loop is True
    assert "[assist]" in path.read_text(encoding="utf-8")
    assert "human_in_loop = true" in path.read_text(encoding="utf-8")


def test_custom_assist_config_round_trips(tmp_path: Path) -> None:
    config = ControllerConfig(
        "test", tmp_path / "boards", tmp_path / "state.sqlite3", assist=AssistConfig(False)
    )
    path = tmp_path / "config.toml"
    write_config(path, config)
    assert load_config(path).assist == AssistConfig(False)


def evidence(*, kind: str = "review_missing", integrity: str = "observed", **payload: object) -> Evidence:
    return Evidence(
        evidence_id="ev-1",
        event_kind=kind,
        source_integrity=EvidenceIntegrity(integrity),
        normalized_payload=payload,
    )


def test_classifier_table_assigns_known_classes_and_confidence() -> None:
    cases = [
        ("review_missing", ClassificationClass.MISSING_REVIEW),
        ("review_stalled", ClassificationClass.STALLED_REVIEW),
        ("review_blocked", ClassificationClass.BLOCKED_REVIEW_HANDOFF),
        ("source_error", ClassificationClass.SOURCE_INTEGRITY),
    ]

    for kind, expected in cases:
        result = classify([evidence(kind=kind)])
        assert result.classification_class is expected
        assert result.confidence is Confidence.HIGH
        assert result.ai_status is AIStatus.NOT_INVOKED
        assert result.recurrence is Recurrence.FIRST_SEEN


def test_unknown_conflicting_and_unverified_evidence_fail_closed() -> None:
    unknown = classify([evidence(kind="future_event")])
    conflicting = classify([evidence(kind="review_missing"), evidence(kind="review_stalled")])
    unverified = classify([evidence(kind="review_missing", integrity="unverified")])

    for result in (unknown, conflicting, unverified):
        assert result.classification_class in {
            ClassificationClass.UNKNOWN,
            ClassificationClass.SOURCE_INTEGRITY,
        }
        assert result.confidence is Confidence.NONE
        assert result.uncertainty
        assert not result.actionable


def test_recurrence_uses_prior_window_signatures_without_guessing() -> None:
    result = classify(
        [evidence()],
        prior_problem_signatures=("review-integrity/missing-review/v1",),
    )
    assert result.recurrence is Recurrence.RECURS_IN_2_WINDOWS
    assert result.problem_signature == "review-integrity/missing-review/v1"


def test_ai_status_failures_preserve_evidence_and_are_not_actionable() -> None:
    for model in (DeterministicStubModel("not-json"), DeterministicStubModel("TIMEOUT")):
        result = analyze([evidence()], model=model)
        assert result.evidence == (evidence(),)
        assert result.ai_status in {
            AIStatus.MALFORMED,
            AIStatus.TIMEOUT,
            AIStatus.UNAVAILABLE,
        }
        assert not result.actionable
    unavailable = analyze([evidence()], model=DeterministicStubModel("UNAVAILABLE"))
    assert unavailable.ai_status is AIStatus.UNAVAILABLE
    assert not unavailable.actionable


def test_unexpected_model_exception_fails_closed_and_preserves_evidence() -> None:
    class BoomModel:
        model_ref = "boom-model"

        def complete(self, prompt: str) -> str:
            del prompt
            raise RuntimeError("boom")

    item = evidence()
    result = analyze([item], model=BoomModel())

    assert result.evidence == (item,)
    assert result.ai_status is AIStatus.UNAVAILABLE
    assert result.model_ref == "boom-model"
    assert not result.actionable


def test_raising_model_ref_property_fails_closed_and_preserves_evidence() -> None:
    class BoomRef:
        @property
        def model_ref(self) -> str:
            raise RuntimeError("ref boom")

        def complete(self, prompt: str) -> str:
            del prompt
            return "{}"

    item = evidence()
    result = analyze([item], model=BoomRef())

    assert result.evidence == (item,)
    assert result.ai_status is AIStatus.UNAVAILABLE
    assert result.model_ref is None
    assert result.classification.model_ref is None
    assert not result.actionable


def test_string_model_ref_propagates_to_analysis_result() -> None:
    class NamedModel:
        model_ref = "claude-sonnet-4"

        def complete(self, prompt: str) -> str:
            del prompt
            return json.dumps({"summary": "review handoff missing"})

    result = analyze([evidence()], model=NamedModel())

    assert result.ai_status is AIStatus.SUCCEEDED
    assert result.model_ref == "claude-sonnet-4"
    assert result.classification.model_ref is None


def test_deterministic_stub_replay_is_canonical_and_stable() -> None:
    model = DeterministicStubModel(
        json.dumps(
            {
                "summary": "A review handoff is missing.",
                "class": "missing_review",
                "confidence": "high",
            }
        )
    )
    first = analyze([evidence()], model=model)
    second = analyze([evidence()], model=DeterministicStubModel(model.response))
    assert first == second
    assert first.ai_status is AIStatus.SUCCEEDED


def test_local_model_adapter_is_provider_neutral_and_has_no_execution_surface() -> None:
    calls: list[str] = []
    model = LocalHermesTextModel(lambda prompt: calls.append(prompt) or "{}")
    result = model.complete("bounded prompt")
    assert result == "{}"
    assert calls == ["bounded prompt"]
    assert not hasattr(model, "tools")
    assert not hasattr(model, "argv")


def test_recommendation_is_prevention_only_pending_and_human_gated() -> None:
    analysis = analyze([evidence()])
    result = recommend(analysis, target=RecommendationTarget(kind="test"))

    assert result.intent == "prevention_only"
    assert result.target.ownership == "hkrc"
    assert result.target.kind == "test"
    assert result.strength is RecommendationStrength.STRONG_CANDIDATE
    assert result.state == "pending"
    assert result.human_in_loop == "yes"
    assert result.evidence_refs == ("evidence:ev-1",)


@pytest.mark.parametrize("kind", ["project", "shell", "network", "hermes_cli", "approved_orchestration_distribution"])
def test_unsafe_recommendation_targets_are_rejected(kind: str) -> None:
    analysis = analyze([evidence()])
    with pytest.raises(RecommendationValidationError):
        recommend(analysis, target=RecommendationTarget(ownership="other", kind=kind))


def test_non_actionable_analysis_yields_insufficient_evidence() -> None:
    analysis = analyze([evidence(kind="future_event")])
    result = recommend(analysis, target=RecommendationTarget(kind="code"))
    assert result.strength is RecommendationStrength.INSUFFICIENT_EVIDENCE
    assert result.state == "pending"
    assert result.human_in_loop == "yes"
    assert result.evidence_refs == ("evidence:ev-1",)


def test_contracts_serialize_with_versioned_names_and_evidence() -> None:
    analysis = analyze([evidence()])
    payload = analysis.to_dict()
    assert payload["schema_version"] == "hkrc.assist.analysis.v1"
    assert payload["classification"]["class"] == "missing_review"
    assert payload["ai_status"] == "not_invoked"
    recommendation = recommend(analysis, target=RecommendationTarget(kind="code"))
    assert recommendation.to_dict()["schema_version"] == "hkrc.assist.recommendation.v1"
    assert recommendation.to_dict()["state"] == "pending"
    assert analysis.evidence[0].evidence_id == "ev-1"


def test_evidence_is_immutable_and_preserved() -> None:
    item = evidence()
    result = analyze([item], model=DeterministicStubModel("TIMEOUT"))
    assert result.evidence[0] is item
    assert result.evidence == (item,)
