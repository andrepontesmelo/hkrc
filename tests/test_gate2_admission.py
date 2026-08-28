"""Gate 2 acceptance: contract registration + HKRC-mediated child admission."""

from __future__ import annotations

from collections.abc import Sequence
import json
from pathlib import Path

import pytest

from hkrc.admission import AdmissionError, NativeResult, admit_child, scrubbed_env
from hkrc.cli import main
from hkrc.config import ControllerConfig, write_config
from hkrc.outcome_guard import OutcomeGuard
from hkrc.state import ControllerState

from test_outcome_guard import authority, contract


def open_guard(tmp_path: Path) -> tuple[ControllerState, OutcomeGuard]:
    state = ControllerState.initialize(tmp_path / "state.sqlite3", "gate2-test")
    return state, OutcomeGuard(state)


def fake_runner(calls: list[list[str]], *, create_ok: bool = True, promote_ok: bool = True):
    def runner(command: Sequence[str]) -> NativeResult:
        calls.append(list(command))
        if "create" in command:
            if not create_ok:
                return NativeResult(1, "", "native create boom")
            key = command[command.index("--idempotency-key") + 1]
            return NativeResult(
                0,
                json.dumps({"ok": True, "task_id": f"t_child_{key[-8:]}", "status": "blocked"}),
            )
        if "promote" in command:
            if not promote_ok:
                return NativeResult(1, "", "native promote boom")
            return NativeResult(0, json.dumps({"ok": True, "status": "ready"}))
        return NativeResult(2, "", "unexpected native command")

    return runner


def base_config(tmp_path: Path, state: Path) -> ControllerConfig:
    return ControllerConfig(
        "gate2-test", tmp_path / "boards", state, native_cli="hermes"
    )


def test_broad_child_denied_before_dispatch_without_native_call(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed
        calls: list[list[str]] = []
        report = admit_child(
            base_config(tmp_path, state.path),
            state,
            parent_task_id="t_parent",
            contract_ref="root",
            effect="merge_main",
            board_slug="alpha",
            title="broad child",
            assignee="dev",
            runner=fake_runner(calls),
        )
        assert report.allowed is False
        assert report.reason_code == "effect_not_allowed"
        assert report.child_task_id is None
        assert calls == []  # denied before dispatch; no native CLI call at all
        row = state.connection.execute(
            "SELECT status FROM outcome_admissions WHERE contract_ref = 'root'"
        ).fetchone()
        assert row["status"] == "denied"


def test_valid_narrow_admission_allowed_exactly_once(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract(
                "root",
                allowed_effects=["isolated_prototype", "repository_modify"],
            )
        ).allowed
        assert guard.register_contract(
            contract(
                "narrow-child",
                allowed_effects=["isolated_prototype"],
                parent_contract_refs=["root"],
            )
        ).allowed
        calls: list[list[str]] = []
        config = base_config(tmp_path, state.path)
        first = admit_child(
            config,
            state,
            parent_task_id="t_parent",
            contract_ref="narrow-child",
            effect="isolated_prototype",
            board_slug="alpha",
            title="narrow child",
            assignee="dev",
            runner=fake_runner(calls),
        )
        assert first.allowed is True
        assert first.reason_code == "admitted"
        child_id = first.child_task_id
        assert child_id is not None

        second = admit_child(
            config,
            state,
            parent_task_id="t_parent",
            contract_ref="narrow-child",
            effect="isolated_prototype",
            board_slug="alpha",
            title="narrow child again",
            assignee="dev",
            runner=fake_runner(calls),
        )
        assert second.allowed is True
        assert second.duplicate is True
        assert second.reason_code == "admission_already_recorded"
        assert second.child_task_id == child_id
        # Exactly one create + one promote: no duplicate child, no duplicate lease.
        assert len(calls) == 2


def test_admission_creates_blocked_then_promotes_only_after_validation(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed
        calls: list[list[str]] = []
        report = admit_child(
            base_config(tmp_path, state.path),
            state,
            parent_task_id="t_parent",
            contract_ref="root",
            effect="isolated_prototype",
            board_slug="alpha",
            title="child",
            assignee="dev",
            runner=fake_runner(calls),
        )
        assert report.allowed is True
        create, promote = calls
        assert create[0] == "hermes"
        assert create[1] == "kanban"
        assert "--board" in create and "alpha" in create
        assert "--initial-status" in create and "blocked" in create
        assert "--idempotency-key" in create
        assert create[create.index("--idempotency-key") + 1].startswith("hkrc-admit:")
        assert "--parent" in create and "t_parent" in create
        assert "promote" in promote
        assert "--force" in promote  # parent may still be running; policy gate is ours
        assert report.child_task_id == promote[-2]
        row = state.connection.execute(
            "SELECT status, child_task_id FROM outcome_admissions"
        ).fetchone()
        assert row["status"] == "admitted"
        assert row["child_task_id"] == report.child_task_id


def test_admission_fails_closed_on_native_create_failure(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed
        calls: list[list[str]] = []
        with pytest.raises(AdmissionError, match="native kanban create failed: native create boom"):
            admit_child(
                base_config(tmp_path, state.path),
                state,
                parent_task_id="t_parent",
                contract_ref="root",
                effect="isolated_prototype",
                board_slug="alpha",
                title="child",
                assignee="dev",
                runner=fake_runner(calls, create_ok=False),
            )
        row = state.connection.execute(
            "SELECT status, child_task_id FROM outcome_admissions"
        ).fetchone()
        assert row["status"] == "failed"
        assert row["child_task_id"] is None


def test_admission_fails_closed_on_native_promote_failure(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed
        calls: list[list[str]] = []
        with pytest.raises(AdmissionError, match="native kanban promote failed"):
            admit_child(
                base_config(tmp_path, state.path),
                state,
                parent_task_id="t_parent",
                contract_ref="root",
                effect="isolated_prototype",
                board_slug="alpha",
                title="child",
                assignee="dev",
                runner=fake_runner(calls, promote_ok=False),
            )
        row = state.connection.execute(
            "SELECT status, child_task_id FROM outcome_admissions"
        ).fetchone()
        assert row["status"] == "held"  # child exists but was never dispatched
        assert row["child_task_id"] is not None


def test_admission_fails_closed_on_unknown_contract(tmp_path: Path) -> None:
    state, _ = open_guard(tmp_path)
    calls: list[list[str]] = []
    with state:
        report = admit_child(
            base_config(tmp_path, state.path),
            state,
            parent_task_id="t_parent",
            contract_ref="ghost",
            effect="isolated_prototype",
            board_slug="alpha",
            title="child",
            assignee="dev",
            runner=fake_runner(calls),
        )
    assert report.allowed is False
    assert report.reason_code == "contract_missing"
    assert calls == []


def test_scrubbed_env_removes_hermes_kanban_ambient_vars(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_leak")
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "leak-board")
    monkeypatch.setenv("HOME", "/tmp/leak-home")
    env = scrubbed_env()
    assert "HERMES_KANBAN_TASK" not in env
    assert "HERMES_KANBAN_BOARD" not in env
    assert env["HOME"] == "/tmp/leak-home"  # unrelated vars preserved


def test_successor_allows_repository_modify_but_merge_waits_for_review_evidence(
    tmp_path: Path,
) -> None:
    """Acceptance D policy half: fresh authority successor narrows nothing and
    merge_main stays gated on bound independent-review evidence."""
    state, guard = open_guard(tmp_path)
    with state:
        assert guard.register_contract(
            contract(
                "prototype",
                allowed_effects=["isolated_prototype"],
                continuation_policy="explicitly-authorized-successor",
            )
        ).allowed
        fresh_authority = authority("auth-impl")
        fresh_authority["statement"] = "Authorize repository work and merge after review."
        successor = contract(
            "implementation",
            allowed_effects=["repository_modify", "merge_main"],
            authority_source=fresh_authority,
            successor_of="prototype",
            terminal_evidence=[
                {"evidence_type": "independent_review", "evidence_ref": "gate2-review"}
            ],
        )
        assert guard.register_contract(successor).allowed
        assert guard.check_effect("implementation", "repository_modify").allowed is True
        merge = guard.check_effect("implementation", "merge_main")
        assert merge.allowed is True  # contract permits it ...
        outcome = guard.check_outcome("implementation", evidence=())
        assert outcome.allowed is False  # ... but evidence is what gates it
        assert outcome.reason_code == "terminal_evidence_missing"
        outcome = guard.check_outcome(
            "implementation",
            evidence=[{"evidence_type": "independent_review", "evidence_ref": "gate2-review"}],
        )
        assert outcome.allowed is True


def test_cli_register_and_admit_child_roundtrip(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    config_path = tmp_path / "config.toml"
    write_config(config_path, ControllerConfig("cli", tmp_path / "boards", state_path))
    with ControllerState.initialize(state_path, "cli") as state:
        assert OutcomeGuard(state).register_contract(
            contract("cli-admit", allowed_effects=["isolated_prototype"])
        ).allowed

    contract_path = tmp_path / "contract.json"
    contract_path.write_text(
        json.dumps(contract("cli-admit", allowed_effects=["isolated_prototype"])),
        encoding="utf-8",
    )
    exit_code = main(
        [
            "outcome-guard",
            "register",
            "--config",
            str(config_path),
            "--contract-file",
            str(contract_path),
        ]
    )
    assert exit_code == 0  # idempotent re-registration

    fake = tmp_path / "fake-hermes"
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "args = sys.argv[1:]\n"
        "if 'create' in args:\n"
        "    key = args[args.index('--idempotency-key') + 1]\n"
        "    print(json.dumps({'ok': True, 'task_id': 't_cli_' + key[-8:], 'status': 'blocked'}))\n"
        "elif 'promote' in args:\n"
        "    print(json.dumps({'ok': True, 'status': 'ready'}))\n"
        "else:\n"
        "    print(json.dumps({'ok': False, 'error': 'unexpected'})); raise SystemExit(1)\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    config_path.write_text(
        write_config_replace(tmp_path, state_path, str(fake)), encoding="utf-8"
    )
    exit_code = main(
        [
            "outcome-guard",
            "admit-child",
            "--config",
            str(config_path),
            "--parent-task-id",
            "t_parent",
            "--contract-ref",
            "cli-admit",
            "--effect",
            "isolated_prototype",
            "--board",
            "alpha",
            "--title",
            "cli child",
            "--assignee",
            "dev",
        ]
    )
    assert exit_code == 0


def write_config_replace(tmp_path: Path, state_path: Path, native_cli: str) -> str:
    return (
        f'[instance]\nname = "cli"\nnative_boards_root = "{tmp_path}/boards"\n\n'
        f"[controller]\nstate_db = \"{state_path}\"\n\n"
        f"[native]\ncli = \"{native_cli}\"\n"
    )
