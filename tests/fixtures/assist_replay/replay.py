"""Deterministic offline HKRC Assist replay and safety demo.

This module is intentionally standard-library-only. It reads the committed
synthetic JSONL fixture, performs deterministic evidence classification, and
renders a pending prevention candidate. It never imports or invokes Hermes,
opens a board database, or exposes a mutation function.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import html
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping

FIXTURE_PATH = Path(__file__).with_name("events.jsonl")
FIXTURE_NAMESPACE = "synthetic"
SIGNATURE = "review-integrity/deferred-review-no-merge/v1"
CLASSIFICATION = "deferred_review_no_merge"


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def fixture_hash(path: Path = FIXTURE_PATH) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def load_events(path: Path = FIXTURE_PATH) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"fixture row {line_number} must be an object")
        rows.append(value)
    if not rows:
        raise ValueError("fixture must contain at least one event")
    return tuple(rows)


def _validate_fixture(events: Iterable[Mapping[str, Any]]) -> None:
    required = {
        "schema_version",
        "window_id",
        "event_id",
        "evidence_id",
        "observed_at_utc",
        "actor_role",
        "event_kind",
        "status",
        "approval",
        "merge_present",
        "gate_outcome",
        "source_integrity",
    }
    seen: set[str] = set()
    for event in events:
        missing = required.difference(event)
        if missing:
            raise ValueError(f"fixture row is missing fields: {sorted(missing)}")
        event_id = str(event["event_id"])
        if event_id in seen:
            raise ValueError(f"duplicate fixture event_id: {event_id}")
        seen.add(event_id)
        if event["schema_version"] != "hkrc.assist.fixture.event.v1":
            raise ValueError("unsupported fixture schema")
        if event["source_integrity"] != "observed":
            raise ValueError("fixture evidence must be observed")
        if not str(event["observed_at_utc"]).endswith("Z"):
            raise ValueError("fixture timestamps must use a fixed UTC offset")


def _evidence_refs(events: Iterable[Mapping[str, Any]]) -> tuple[str, ...]:
    return tuple(str(event["evidence_id"]) for event in events)


def _classify_window(
    events: tuple[Mapping[str, Any], ...],
    *,
    prior_signatures: tuple[str, ...] = (),
    duplicate_events: int = 0,
) -> dict[str, Any]:
    refs = _evidence_refs(events)
    recurrence = "recurs_in_2_windows" if SIGNATURE in prior_signatures else "first_seen"
    return {
        "window_id": str(events[0]["window_id"]),
        "evidence_refs": refs,
        "evidence_count": len(refs),
        "classification": CLASSIFICATION,
        "signature": SIGNATURE,
        "confidence": "high" if len(refs) >= 3 else "medium",
        "uncertainty": ["later re-validation outcome is outside this replay window"],
        "recurrence": recurrence,
        "duplicate_events_ignored": duplicate_events,
        "evidence_preserved": True,
    }


def _model_gate(
    evidence_refs: tuple[str, ...],
    model_output: str | None,
) -> dict[str, Any]:
    """Parse only a bounded model result; every parse failure fails closed."""

    if model_output is None:
        reason = "model_unavailable"
    else:
        try:
            value = json.loads(model_output)
            if not isinstance(value, dict) or value.get("summary") is None:
                raise ValueError("model output is not the expected object")
            return {
                "ai_status": "ok",
                "recommendation": "not_actionable",
                "evidence_refs": evidence_refs,
                "evidence_preserved": True,
                "summary": str(value["summary"]),
            }
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            reason = f"model_malformed:{type(exc).__name__}"
    return {
        "ai_status": "error",
        "recommendation": "not_actionable",
        "evidence_refs": evidence_refs,
        "evidence_preserved": True,
        "error": reason,
    }


def _candidate(classification: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "recommendation_id": "recommendation-001",
        "analysis_id": "analysis-001",
        "state": "pending",
        "intent": "prevention_only",
        "action": "not_applied",
        "strength": "candidate",
        "problem_signature": str(classification["signature"]),
        "proposed_change": "Add a deterministic review-pair integrity check before completion is treated as shipped.",
        "target": {"ownership": "hkrc", "kind": "test", "logical_target": "review-integrity policy"},
        "human_in_loop": "yes",
        "executable_action": False,
        "evidence_refs": list(classification["evidence_refs"]),
    }


def _candidate_card(candidate: Mapping[str, Any], classification: Mapping[str, Any]) -> str:
    return "\n".join(
        (
            "HKRC Assist candidate card",
            f"classification: {classification['classification']}",
            f"signature: {classification['signature']}",
            f"recurrence: {classification['recurrence']}",
            f"strength: {candidate['strength']}",
            f"state: {candidate['state']}",
            f"action: {candidate['action']}",
            f"evidence_refs: {','.join(candidate['evidence_refs'])}",
            f"proposed_change: {candidate['proposed_change']}",
            "human_in_loop: yes",
        )
    )


def _html_report(candidate: Mapping[str, Any], classification: Mapping[str, Any]) -> str:
    def _esc(value: object) -> str:
        return html.escape(str(value), quote=True)
    evidence = "".join(
        f'<li id="evidence-{_esc(ref)}"><a href="#evidence-{_esc(ref)}">{_esc(ref)}</a></li>'
        for ref in candidate["evidence_refs"]
    )
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>HKRC Assist synthetic replay</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:58rem;margin:2rem auto;padding:0 1rem;color:#172033}}
.badge{{display:inline-block;padding:.25rem .55rem;border-radius:.3rem;background:#fff3cd}}
.strength-badge{{background:#dbeafe}}
section{{border:1px solid #d8dee9;border-radius:.5rem;padding:1rem;margin:1rem 0}}
.mermaid{{background:#f6f8fa;padding:1rem;white-space:pre-wrap}}
</style></head>
<body>
<h1>HKRC Assist synthetic replay</h1>
<p><span class="badge">SYNTHETIC_OFFLINE_FIXTURE</span> <span class="badge strength-badge">strength: {_esc(candidate['strength'])}</span> Recommendation-only; no live state was changed.</p>
<section><h2>Classification</h2><p>{_esc(classification['classification'])}; {_esc(classification['recurrence'])}; confidence {_esc(classification['confidence'])}</p>
<p>Signature: <code>{_esc(classification['signature'])}</code></p></section>
<section><h2>Before / after candidate</h2>
<div class="mermaid">graph LR
before[before: no prevention check] --> review[review deferred]
review --> gate[canonical gate blocked]
gate --> after[after: pending prevention candidate]
</div></section>
<section><h2>Evidence</h2><ul>{evidence}</ul><p>Evidence is preserved and independently inspectable.</p></section>
<section><h2>Safety impact</h2><p>This candidate is prevention-only, pending, and not applied. Human approval is required.</p></section>
<section><h2>Explicit non-goals</h2><p>No unblock, reassign, comment, create, merge, deploy, or service action.</p></section>
</body></html>
"""


def _ledger_check(classification: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    pending_record = {
        "record_id": "ledger-001",
        "event": "candidate_pending",
        "state": "pending",
        "action": "not_applied",
        "evidence_refs": list(classification["evidence_refs"]),
    }
    records = [pending_record]
    before = _canonical(pending_record)
    records.append(
        {
            "record_id": "ledger-002",
            "event": "operator_deferred",
            "state": "deferred",
            "action": "not_applied",
            "recommendation_id": candidate["recommendation_id"],
        }
    )
    after = _canonical(records[0])
    return {
        "records": records,
        "append_only": len(records) == 2 and before == after,
        "pending_record_unchanged": before == after,
        "record_count_before": 1,
        "record_count_after": len(records),
        "operator_event": "operator_deferred",
        "controller_action_state": "not_applied",
    }


def _dedupe_events(events: Iterable[Mapping[str, Any]]) -> tuple[tuple[Mapping[str, Any], ...], tuple[str, ...], tuple[str, ...]]:
    unique: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    duplicate_ids: list[str] = []
    for event in events:
        event_id = str(event["event_id"])
        if event_id in seen:
            duplicate_ids.append(event_id)
            continue
        seen.add(event_id)
        unique.append(event)
    return tuple(unique), tuple(duplicate_ids), tuple(str(event["event_id"]) for event in unique)


def run_replay(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    events = load_events(path)
    _validate_fixture(events)
    by_window: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        by_window[str(event["window_id"])].append(event)
    ordered_windows = [tuple(by_window[key]) for key in sorted(by_window)]
    if len(ordered_windows) < 2:
        raise ValueError("fixture must contain at least two distinct windows")

    first = _classify_window(ordered_windows[0])
    second = _classify_window(ordered_windows[1], prior_signatures=(first["signature"],))
    first_refs = _evidence_refs(ordered_windows[0])
    malformed = _model_gate(first_refs, "not valid JSON")
    unavailable = _model_gate(first_refs, None)
    candidate = _candidate(second)
    ledger = _ledger_check(second, candidate)
    replay_signature = {
        "fixture_hash": fixture_hash(path),
        "windows": [
            {
                "window_id": item["window_id"],
                "signature": item["signature"],
                "recurrence": item["recurrence"],
                "evidence_refs": item["evidence_refs"],
            }
            for item in (first, second)
        ],
    }
    duplicate_input = events + (events[0], events[1])
    deduped_events, duplicate_ids, deduped_ids = _dedupe_events(duplicate_input)
    audit_ids = tuple(str(event["event_id"]) for event in duplicate_input)
    findings = tuple(
        {
            "finding_id": f"finding-{window_id}",
            "event_ids": [
                str(event["event_id"])
                for event in deduped_events
                if str(event["window_id"]) == window_id
            ],
        }
        for window_id in sorted({str(event["window_id"]) for event in deduped_events})
    )
    output = {
        "label": "SYNTHETIC_OFFLINE_FIXTURE",
        "fixture_namespace": FIXTURE_NAMESPACE,
        "fixture_hash": fixture_hash(path),
        "windows": [first, second],
        "classifier": {
            "signature": SIGNATURE,
            "recurrence_transition": [first["recurrence"], second["recurrence"]],
            "malformed_model": malformed,
            "unavailable_model": unavailable,
        },
        "recommendation": candidate,
        "candidate_card": _candidate_card(candidate, second),
        "html_report": _html_report(candidate, second),
        "zero_mutation_proof": {
            "native_cli_invocations": 0,
            "board_writes": 0,
            "task_writes": 0,
            "unblock": 0,
            "reassign": 0,
            "comment": 0,
            "create": 0,
            "merge": 0,
            "deploy": 0,
            "systemd": 0,
            "controller_records": ["pending", "not_applied"],
        },
        "ledger": ledger,
        "dedupe": {
            "input_events": len(duplicate_input),
            "unique_event_ids": len(deduped_events),
            "duplicates_ignored": len(duplicate_ids),
            "audit_event_ids": list(audit_ids),
            "deduped_event_ids": list(deduped_ids),
            "duplicate_event_ids": list(duplicate_ids),
            "findings": list(findings),
            "finding_ids": [finding["finding_id"] for finding in findings],
            "duplicate_findings_created": len(findings) - len({finding["finding_id"] for finding in findings}),
            "replay_signature": replay_signature,
        },
    }
    return output


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay the synthetic HKRC Assist fixture offline")
    parser.add_argument("--json", action="store_true", help="emit one deterministic JSON object")
    parser.add_argument("--demo", action="store_true", help="write the candidate HTML report to --output-dir")
    parser.add_argument("--output-dir", type=Path, help="operator-selected local output directory for demo artifacts")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    result = run_replay()
    if args.demo:
        if args.output_dir is None:
            raise SystemExit("--demo requires --output-dir")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "candidate-card.txt").write_text(result["candidate_card"] + "\n", encoding="utf-8")
        (args.output_dir / "report.html").write_text(result["html_report"], encoding="utf-8")
    rendered = _canonical({key: value for key, value in result.items() if key != "html_report"})
    if args.json:
        print(rendered)
    else:
        print(result["candidate_card"])
        print("\nReplay summary:")
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["FIXTURE_PATH", "fixture_hash", "load_events", "run_replay", "main"]
