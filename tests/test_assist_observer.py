from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, cast

import pytest

from hkrc.assist import (
    APPROVED_SOURCE_CONTRACT,
    MAX_CONTEXT_PACKET_BYTES,
    MAX_EVIDENCE_ITEMS,
    ObservationContractUnavailable,
    StaticObservationSource,
    build_context_packet,
    observe,
)


NOW = datetime(2026, 1, 11, 2, 0, tzinfo=timezone.utc)


def snapshot(*, second_window: bool = False) -> dict[str, object]:
    observed_at = "2026-01-10T12:34:56Z" if not second_window else "2026-01-11T01:34:56Z"
    return {
        "sessions": [
            {
                "profile": "profile-A",
                "session_id": "session-1",
                "observed_at": observed_at,
                "status": "completed",
                "message": "review stalled at /home/operator/private/project",
            }
        ],
        "commands": [
            {
                "profile": "profile-A",
                "command_id": "command-1",
                "observed_at": observed_at,
                "kind": "tool_result",
                "argv": ["hermes", "kanban", "show", "task-impl-1"],
                "api_token": "secret-shaped-value",
            }
        ],
        "tasks": [
            {
                "board": "board-A",
                "task_id": "task-impl-1",
                "run_id": "run-1",
                "observed_at": observed_at,
                "status": "blocked",
                "event_kind": "review_stalled",
            }
        ],
    }


def source(*, second_window: bool = False) -> StaticObservationSource:
    return StaticObservationSource(
        contract=APPROVED_SOURCE_CONTRACT,
        snapshot=snapshot(second_window=second_window),
    )


def test_observer_emits_versioned_window_and_opaque_evidence() -> None:
    result = observe(source(), now=NOW, observer_run_id="run-operator-1")
    packet = build_context_packet(result)

    assert packet["schema_version"] == "hkrc.assist.context.v1"
    assert packet["window"]["schema_version"] == "hkrc.assist.window.v1"
    assert packet["window"]["window_start_utc"] == "2026-01-10T02:00:00Z"
    assert packet["window"]["window_end_utc"] == "2026-01-11T02:00:00Z"
    assert packet["window"]["scope"] == "all_profiles_all_boards"
    assert packet["window"]["source_contract"] == APPROVED_SOURCE_CONTRACT
    assert packet["evidence"]
    for item in packet["evidence"]:
        assert item["schema_version"] == "hkrc.assist.evidence.v1"
        assert item["source_kind"] in {"session", "command", "tool_result", "task_event", "task_run"}
        assert item["source_integrity"] == "observed"
        assert item["evidence_id"].startswith("opaque:")
        assert item["task_ref"].startswith("opaque:")
        assert item["redactions"]


def test_observer_redacts_machine_paths_secrets_and_raw_identifiers() -> None:
    result = observe(source(), now=NOW)
    encoded = json.dumps(build_context_packet(result), sort_keys=True)

    assert "/home/" not in encoded
    assert "secret-shaped-value" not in encoded
    assert "task-impl-1" not in encoded
    assert "profile-A" not in encoded
    assert "board-A" not in encoded
    assert "opaque:" in encoded


def test_fingerprint_and_packet_are_stable_for_repeated_runs() -> None:
    first = build_context_packet(observe(source(), now=NOW, observer_run_id="run-1"))
    second = build_context_packet(observe(source(), now=NOW, observer_run_id="run-2"))

    assert first["window"]["window_fingerprint"] == second["window"]["window_fingerprint"]
    assert first["evidence"] == second["evidence"]
    assert first["summary"] == second["summary"]


def test_distinct_windows_have_distinct_fingerprints() -> None:
    first = observe(source(), now=NOW)
    second = observe(source(second_window=True), now=NOW)

    assert first.window.window_fingerprint != second.window.window_fingerprint


def test_observer_excludes_rows_outside_closed_window() -> None:
    fixture = snapshot()
    fixture["tasks"] = [
        *cast(list[dict[str, Any]], fixture["tasks"]),
        {
            "board": "board-A",
            "task_id": "task-old",
            "run_id": "run-old",
            "observed_at": "2026-01-09T01:34:56Z",
            "status": "blocked",
            "event_kind": "review_stalled",
        },
    ]

    result = observe(
        cast(
            Any,
            StaticObservationSource(
                contract=APPROVED_SOURCE_CONTRACT,
                snapshot=cast(dict[str, Any], fixture),
            ),
        ),
        now=NOW,
    )

    assert len(result.evidence) == 3
    assert all(item["observed_at_utc"] >= "2026-01-10T02:00:00Z" for item in result.evidence)


def test_missing_or_wrong_source_contract_fails_closed() -> None:
    missing = StaticObservationSource(contract=None, snapshot=snapshot())
    wrong = StaticObservationSource(contract="native-sqlite-v1", snapshot=snapshot())

    with pytest.raises(ObservationContractUnavailable):
        observe(missing, now=NOW)
    with pytest.raises(ObservationContractUnavailable):
        observe(wrong, now=NOW)


def test_window_accepts_only_closed_utc_range() -> None:
    result = observe(source(), now=datetime(2026, 1, 11, 2, 0), observer_run_id="run-1")

    assert result.window.window_end_utc.endswith("Z")
    assert result.window.window_start_utc.endswith("Z")
    assert result.window.window_end_utc > result.window.window_start_utc


def test_source_contract_is_read_only_shape() -> None:
    assert not hasattr(StaticObservationSource, "write")
    assert not hasattr(StaticObservationSource, "mutate")


def test_observer_preserves_valid_source_integrity() -> None:
    fixture = {"events": [{
        "observed_at": "2026-01-10T12:00:00Z",
        "status": "blocked",
        "source_integrity": "unverified",
    }]}

    result = observe(
        StaticObservationSource(APPROVED_SOURCE_CONTRACT, fixture),
        now=NOW,
    )

    assert result.evidence[0]["source_integrity"] == "unverified"
    assert result.evidence[0]["normalized_payload"]["source_integrity"] == "unverified"


def test_observer_rejects_invalid_source_integrity() -> None:
    fixture = {"events": [{
        "observed_at": "2026-01-10T12:00:00Z",
        "status": "blocked",
        "source_integrity": "invented",
    }]}

    with pytest.raises(ObservationContractUnavailable, match="source_integrity"):
        observe(StaticObservationSource(APPROVED_SOURCE_CONTRACT, fixture), now=NOW)


def test_observer_rejects_excessive_evidence_items() -> None:
    rows = [
        {
            "observed_at": "2026-01-10T12:00:00Z",
            "status": "blocked",
            "task_id": f"task-{index}",
        }
        for index in range(MAX_EVIDENCE_ITEMS + 1)
    ]

    with pytest.raises(ObservationContractUnavailable, match="item bound"):
        observe(
            StaticObservationSource(APPROVED_SOURCE_CONTRACT, {"tasks": rows}),
            now=NOW,
        )


def test_context_packet_rejects_excessive_serialized_size() -> None:
    result = observe(
        StaticObservationSource(
            APPROVED_SOURCE_CONTRACT,
            {"events": [{
                "observed_at": "2026-01-10T12:00:00Z",
                "status": "blocked",
            }]},
        ),
        now=NOW,
    )
    oversized = dict(result.evidence[0])
    oversized["normalized_payload"] = {"status": "x" * MAX_CONTEXT_PACKET_BYTES}
    oversized_result = result.__class__(result.window, (oversized,), result.normalized_snapshot)

    with pytest.raises(ObservationContractUnavailable, match="size bound"):
        build_context_packet(oversized_result)


def test_observer_supports_review_gap_surface() -> None:
    result = observe(
        StaticObservationSource(
            APPROVED_SOURCE_CONTRACT,
            {"review_gaps": [{
                "observed_at": "2026-01-10T12:00:00Z",
                "status": "missing",
                "task_id": "task-review-gap",
            }]},
        ),
        now=NOW,
    )

    assert len(result.evidence) == 1
    assert result.evidence[0]["source_kind"] == "review_gap"
    assert result.evidence[0]["normalized_payload"]["source_surface"] == "review_gap"