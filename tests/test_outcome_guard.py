from __future__ import annotations

import json
from pathlib import Path

import pytest

from hkrc.cli import main
from hkrc.config import ControllerConfig, write_config
from hkrc.outcome_guard import OutcomeGuard, OutcomeGuardError
from hkrc.state import ControllerState


def authority(authority_id: str = "auth-rentcli-selection") -> dict[str, str]:
    return {
        "authority_id": authority_id,
        "kind": "operator",
        "actor": "Andre",
        "authorized_at": "2026-08-12T12:00:00+00:00",
        "statement": "Authorize prototype selection only.",
    }


def contract(
    contract_id: str,
    *,
    allowed_effects: list[str],
    declared_outcome: str = "Andre selects one Rentcli prototype",
    terminal_evidence: list[dict[str, str]] | None = None,
    continuation_policy: str = "stop",
    authority_source: dict[str, str] | None = None,
    parent_contract_refs: list[str] | None = None,
    successor_of: str | None = None,
) -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": "hkrc.outcome-contract.v1",
        "contract_id": contract_id,
        "declared_outcome": declared_outcome,
        "terminal_evidence": terminal_evidence
        if terminal_evidence is not None
        else [{"evidence_type": "human_selection", "evidence_ref": "prototype-choice"}],
        "allowed_effects": allowed_effects,
        "continuation_policy": continuation_policy,
        "authority_source": authority_source or authority(),
        "parent_contract_refs": parent_contract_refs or [],
    }
    if successor_of is not None:
        document["successor_of"] = successor_of
    return document


def open_guard(tmp_path: Path) -> tuple[ControllerState, OutcomeGuard]:
    state = ControllerState.initialize(tmp_path / "state.sqlite3", "outcome-test")
    return state, OutcomeGuard(state)


def test_rentcli_prototype_policy_denies_merge_and_done_status_does_not_reach_outcome(
    tmp_path: Path,
) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        registered = guard.register_contract(
            contract("rentcli-prototype-selection", allowed_effects=["isolated_prototype"])
        )
        assert registered.allowed is True

        merge = guard.check_effect("rentcli-prototype-selection", "merge_main")
        assert merge.allowed is False
        assert merge.reason_code == "effect_not_allowed"
        assert merge.governing_contract_ref == "rentcli-prototype-selection"

        outcome = guard.check_outcome(
            "rentcli-prototype-selection", evidence=(), task_status="done"
        )
        assert outcome.allowed is False
        assert outcome.reason_code == "terminal_evidence_missing"
        assert outcome.outcome_reached is False
        assert outcome.missing_evidence_refs == ("prototype-choice",)


def test_child_cannot_broaden_any_governing_ancestor(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed

        result = guard.register_contract(
            contract(
                "broad-child",
                allowed_effects=["repository_modify"],
                parent_contract_refs=["root"],
            )
        )

        assert result.allowed is False
        assert result.reason_code == "effect_broadens_ancestor"
        assert result.governing_contract_ref == "root"


def test_child_strict_subset_is_allowed_and_enforced(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract(
                "root",
                allowed_effects=["isolated_prototype", "repository_modify"],
            )
        ).allowed
        result = guard.register_contract(
            contract(
                "narrow-child",
                allowed_effects=["isolated_prototype"],
                parent_contract_refs=["root"],
            )
        )

        assert result.allowed is True
        assert guard.check_effect("narrow-child", "isolated_prototype").allowed is True
        denied = guard.check_effect("narrow-child", "repository_modify")
        assert denied.allowed is False
        assert denied.governing_contract_ref == "narrow-child"


def test_explicit_successor_needs_fresh_authority_and_only_adds_named_effects(
    tmp_path: Path,
) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract(
                "prototype",
                allowed_effects=["isolated_prototype"],
                continuation_policy="explicitly-authorized-successor",
            )
        ).allowed
        stale = guard.register_contract(
            contract(
                "stale-successor",
                allowed_effects=["repository_modify"],
                authority_source=authority(),
                successor_of="prototype",
            )
        )
        assert stale.allowed is False
        assert stale.reason_code == "successor_requires_fresh_authority"

        fresh_authority = authority("auth-rentcli-implementation")
        fresh_authority["statement"] = "Authorize repository implementation, not merge."
        fresh = guard.register_contract(
            contract(
                "implementation",
                allowed_effects=["repository_modify"],
                authority_source=fresh_authority,
                successor_of="prototype",
            )
        )
        assert fresh.allowed is True
        assert guard.check_effect("implementation", "repository_modify").allowed is True
        merge = guard.check_effect("implementation", "merge_main")
        assert merge.allowed is False
        assert merge.governing_contract_ref == "implementation"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"allowed_effects": ["invented_effect"]}, "unknown effects"),
        ({"authority_source": {}}, "authority_source requires"),
        ({"authority_source": None}, "authority_source must be"),
    ],
)
def test_unknown_effects_and_malformed_authority_fail_closed(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    state, guard = open_guard(tmp_path)
    document = contract("invalid", allowed_effects=["isolated_prototype"])
    document.update(change)
    with state, pytest.raises(OutcomeGuardError, match=message):
        guard.register_contract(document)


def test_persistence_idempotency_and_conflicting_rewrite_fail_closed(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    document = contract("durable", allowed_effects=["isolated_prototype"])
    with state:
        assert guard.register_contract(document).reason_code == "contract_registered"
        duplicate = guard.register_contract(document)
        assert duplicate.allowed is True
        assert duplicate.reason_code == "contract_already_registered"

    with ControllerState.open_existing(tmp_path / "state.sqlite3") as reopened:
        persisted_guard = OutcomeGuard(reopened)
        assert persisted_guard.get_contract("durable") == document
        rewrite = contract("durable", allowed_effects=["repository_modify"])
        conflict = persisted_guard.register_contract(rewrite)
        assert conflict.allowed is False
        assert conflict.reason_code == "contract_conflict"
        assert persisted_guard.check_effect("durable", "isolated_prototype").allowed is True


def test_non_mutating_cli_check_emits_json_and_denial_exit_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.sqlite3"
    config_path = tmp_path / "config.toml"
    write_config(
        config_path,
        ControllerConfig("outcome-test", tmp_path / "native-read-only", state_path),
    )
    with ControllerState.initialize(state_path, "outcome-test") as state:
        assert OutcomeGuard(state).register_contract(
            contract("cli-contract", allowed_effects=["isolated_prototype"])
        ).allowed
    before = state_path.read_bytes()

    exit_code = main(
        [
            "outcome-guard",
            "check-effect",
            "--config",
            str(config_path),
            "--contract-ref",
            "cli-contract",
            "--effect",
            "merge_main",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 3
    assert result["allowed"] is False
    assert result["reason_code"] == "effect_not_allowed"
    assert result["governing_contract_ref"] == "cli-contract"
    assert state_path.read_bytes() == before


def test_non_mutating_cli_outcome_check_accepts_typed_evidence_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.sqlite3"
    config_path = tmp_path / "config.toml"
    evidence_path = tmp_path / "evidence.json"
    write_config(
        config_path,
        ControllerConfig("outcome-test", tmp_path / "native-read-only", state_path),
    )
    evidence_path.write_text(
        json.dumps(
            [
                {
                    "evidence_type": "human_selection",
                    "evidence_ref": "prototype-choice",
                }
            ]
        ),
        encoding="utf-8",
    )
    with ControllerState.initialize(state_path, "outcome-test") as state:
        OutcomeGuard(state).register_contract(
            contract("cli-outcome", allowed_effects=["isolated_prototype"])
        )

    exit_code = main(
        [
            "outcome-guard",
            "check-outcome",
            "--config",
            str(config_path),
            "--contract-ref",
            "cli-outcome",
            "--evidence-file",
            str(evidence_path),
            "--task-status",
            "done",
        ]
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result["allowed"] is True
    assert result["outcome_reached"] is True
