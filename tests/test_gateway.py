"""Gateway create-time checks G1-G13: allow + deny coverage per check."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from hkrc.admission import AdmissionError, NativeResult, admit_child
from hkrc.config import ControllerConfig
from hkrc.gateway import (
    REJECT,
    WARN,
    GatewayRequest,
    check_g1,
    check_g10,
    check_g2,
    check_g3,
    check_g4,
    check_g5,
    check_g6,
    check_g7,
    check_g8,
    check_g9,
    check_g11,
    check_g12,
    check_g13,
    gateway_request_for_admission,
    run_gateway_checks,
)
from hkrc.outcome_guard import OutcomeGuard
from hkrc.state import ControllerState

from test_outcome_guard import contract

GOOD_BODY = (
    "Do the thing.\n\nDone criteria: unit tests pass and the CLI exits 0.\n"
    "TERMINAL INSTRUCTION: end by calling kanban_complete or kanban_block.\n"
)
GOOD_IDEMPOTENCY_KEY = "hkrc-test:0001"


def good_request(**overrides: object) -> GatewayRequest:
    """A maximally clean request; tests flip one field to force a denial."""

    base: dict[str, object] = {
        "title": "feat: valid gateway child",
        "board_slug": "alpha",
        "body": GOOD_BODY,
        "idempotency_key": GOOD_IDEMPOTENCY_KEY,
    }
    base.update(overrides)
    return GatewayRequest(**base)  # type: ignore[arg-type]


def codes(request: GatewayRequest) -> set[str]:
    return {violation.code for violation in run_gateway_checks(request).violations}


def rejects(request: GatewayRequest) -> bool:
    return not run_gateway_checks(request).ok


# G1 ------------------------------------------------------------------------


def test_g1_allows_complete_goal_body() -> None:
    assert check_g1(good_request(goal_mode=True)) == []


def test_g1_rejects_none_body_on_goal_card() -> None:
    assert check_g1(good_request(goal_mode=True, body=None)) != []
    assert rejects(good_request(goal_mode=True, body=None))


def test_g1_warns_on_none_body_for_non_goal_card() -> None:
    violations = check_g1(good_request(body=None))
    assert len(violations) == 1
    assert violations[0].severity == WARN


def test_g1_warns_when_body_lacks_done_criteria_and_terminal_line() -> None:
    violations = check_g1(good_request(body="just do it\n"))
    assert {v.code for v in violations} == {
        "g1_done_criteria_missing",
        "g1_terminal_instruction_missing",
    }
    assert all(v.severity == WARN for v in violations)


# G2 ------------------------------------------------------------------------


def test_g2_allows_idempotency_key_present() -> None:
    assert check_g2(good_request()) == []


@pytest.mark.parametrize("key", [None, "", "   "])
def test_g2_rejects_missing_or_blank_key(key: str | None) -> None:
    assert rejects(good_request(idempotency_key=key))


# G3 ------------------------------------------------------------------------


def test_g3_allows_branch_with_worktree() -> None:
    assert (
        check_g3(good_request(branch="feat/x", workspace_kind="worktree")) == []
    )


def test_g3_rejects_branch_with_scratch() -> None:
    assert rejects(good_request(branch="feat/x", workspace_kind="scratch"))


def test_g3_rejects_sim_without_worktree() -> None:
    assert rejects(good_request(card_kind="prototype", workspace_kind="scratch"))
    assert rejects(good_request(card_kind="sim"))


def test_g3_rejects_relative_worktree_path_on_sim() -> None:
    assert rejects(
        good_request(
            card_kind="prototype",
            workspace_kind="worktree",
            workspace_path="relative/path",
        )
    )
    assert (
        check_g3(
            good_request(
                card_kind="prototype",
                workspace_kind="worktree",
                workspace_path="/abs/path",
            )
        )
        == []
    )


# G4 ------------------------------------------------------------------------


def test_g4_allows_plain_review_card_without_project_workspace() -> None:
    assert check_g4(good_request(card_kind="review")) == []


def test_g4_rejects_project_or_workspace_on_review_card() -> None:
    assert rejects(good_request(card_kind="review", project="p_1"))
    assert rejects(good_request(card_kind="review", workspace="/tmp/x"))


# G5 ------------------------------------------------------------------------


def test_g5_allows_no_overrides() -> None:
    assert check_g5(good_request()) == []


def test_g5_rejects_model_or_provider_override() -> None:
    assert rejects(good_request(model="gpt-x"))
    assert rejects(good_request(provider="openrouter"))


# G6 ------------------------------------------------------------------------


def test_g6_allows_well_formed_title() -> None:
    assert check_g6(good_request()) == []


def test_g6_rejects_unsluggable_title() -> None:
    assert rejects(good_request(title="???"))


def test_g6_rejects_slug_failing_ref_format() -> None:
    assert rejects(good_request(ref_format_check=lambda slug: False))


def test_g6_rejects_when_git_probe_unavailable() -> None:
    # The default probe fails closed when git is unavailable.
    import hkrc.gateway as gateway_module

    original = gateway_module._git_ref_format_ok
    try:
        gateway_module._git_ref_format_ok = lambda slug: False
        assert rejects(good_request(title="ok title"))
    finally:
        gateway_module._git_ref_format_ok = original


# G7 ------------------------------------------------------------------------


def test_g7_allows_known_parent() -> None:
    request = good_request(
        parents=(("t_12345678", None),),
        parent_exists=lambda board, task_id: task_id == "t_12345678",
    )
    assert check_g7(request) == []


def test_g7_rejects_self_parent_cycle() -> None:
    assert rejects(
        good_request(
            task_id="t_12345678",
            parents=(("t_12345678", None),),
            parent_exists=lambda board, task_id: True,
        )
    )


def test_g7_rejects_unknown_parent() -> None:
    assert rejects(
        good_request(
            parents=(("t_deadbeef", None),),
            parent_exists=lambda board, task_id: False,
        )
    )


def test_g7_fails_closed_without_existence_probe() -> None:
    assert rejects(good_request(parents=(("t_12345678", None),)))


def test_g7_cross_board_parent_uses_parent_board() -> None:
    boards: list[str] = []

    def probe(board: str, task_id: str) -> bool:
        boards.append(board)
        return True

    request = good_request(
        board_slug="alpha",
        parents=(("t_12345678", "beta"),),
        parent_exists=probe,
    )
    assert check_g7(request) == []
    assert boards == ["beta"]


# G8 ------------------------------------------------------------------------


def test_g8_allows_ordinary_impl_body() -> None:
    assert check_g8(good_request()) == []


def test_g8_rejects_parent_reviewer_completion_deadlock() -> None:
    body = (
        "Do the thing.\n\nDone criteria: done when the parent reviewer "
        "approves and only then kanban_complete.\n"
        "TERMINAL INSTRUCTION: kanban_complete.\n"
    )
    assert rejects(good_request(body=body))


def test_g8_rejects_def_b7137bdd_missed_phrasings() -> None:
    """DEF-t_358b0330-1: inflections and until/must/before connectors."""
    missed = [
        "Do the thing.\n\ncompletion requires its parent reviewer.\n",
        "Do the thing.\n\ndo not complete until parent reviewer approves.\n",
        "Do the thing.\n\ncannot complete until parent reviewer approves.\n",
        "Do the thing.\n\nmust get parent review sign-off before "
        "completing work.\n",
    ]
    for body in missed:
        assert rejects(good_request(body=body)), body


def test_g8_allows_benign_review_mentions() -> None:
    bodies = [
        "Do the thing.\n\nThe parent review is recorded in the ledger "
        "once done.\n",
        "Do the thing.\n\nDo not block on review; complete with evidence.\n",
        "Do the thing.\n\nThe parent review is recorded in the ledger once "
        "the work is finished.\n",
    ]
    for body in bodies:
        assert check_g8(good_request(body=body)) == [], body


def test_g8_ignores_review_cards() -> None:
    body = (
        "Done criteria: done when the parent reviewer approves.\n"
        "TERMINAL INSTRUCTION: kanban_complete.\n"
    )
    assert check_g8(good_request(card_kind="review", body=body)) == []


# G9 ------------------------------------------------------------------------


def g9_body(leave: bool = True, corpus: bool = True) -> str:
    parts = ["Review the work."]
    if leave:
        parts.append("If verified, leave card t_1 blocked after recording.")
    if corpus:
        parts.append("Run the seeded-corpus probe suite before approving.")
    return "\n".join(parts)


def test_g9_allows_complete_review_body() -> None:
    assert check_g9(good_request(card_kind="review", body=g9_body())) == []


def test_g9_rejects_missing_leave_blocked_line() -> None:
    assert rejects(good_request(card_kind="review", body=g9_body(leave=False)))


def test_g9_rejects_missing_seeded_corpus() -> None:
    assert rejects(good_request(card_kind="review", body=g9_body(corpus=False)))


def test_g9_rejects_none_body() -> None:
    assert rejects(good_request(card_kind="review", body=None))


# G10 -----------------------------------------------------------------------


def test_g10_allows_complete_after_runs() -> None:
    assert check_g10("todo", 2) is None
    assert check_g10("in_progress", 0) is None


def test_g10_rejects_complete_on_never_started_todo() -> None:
    violation = check_g10("todo", 0)
    assert violation is not None
    assert violation.severity == REJECT
    assert violation.code == "g10_complete_never_started"


# G11 -----------------------------------------------------------------------


def test_g11_allows_title_first() -> None:
    command = ["hermes", "kanban", "--board", "alpha", "create", "my title",
               "--assignee", "dev"]
    assert check_g11(good_request(command=command)) == []


def test_g11_warns_when_flags_precede_title() -> None:
    command = ["hermes", "kanban", "create", "--assignee", "dev", "my title"]
    violations = check_g11(good_request(command=command))
    assert len(violations) == 1
    assert violations[0].severity == WARN
    assert violations[0].code == "g11_flags_before_title"


def test_g11_skips_non_create_commands() -> None:
    assert check_g11(good_request(command=["hermes", "kanban", "show", "t_1"])) == []


# G12 -----------------------------------------------------------------------


def test_g12_allows_none_and_positive_ints() -> None:
    assert check_g12(good_request(max_runtime_seconds=None)) == []
    assert check_g12(good_request(max_runtime_seconds=5400)) == []


@pytest.mark.parametrize("value", [0, -1, 1.5, True])
def test_g12_rejects_invalid_max_runtime(value: object) -> None:
    assert rejects(good_request(max_runtime_seconds=value))  # type: ignore[arg-type]


# G13 -----------------------------------------------------------------------


def test_g13_allows_template_with_board_slug() -> None:
    template = "hermes kanban --board {board_slug} show {task_id} --json"
    assert check_g13(good_request(prompt_template=template)) == []


def test_g13_rejects_task_id_only_template() -> None:
    assert rejects(good_request(prompt_template="{task_id}"))


def test_g13_rejects_unrenderable_template() -> None:
    assert rejects(
        good_request(prompt_template="{board_slug} {missing_placeholder}")
    )


# Aggregation ---------------------------------------------------------------


def test_run_gateway_checks_aggregates_all_violations() -> None:
    request = good_request(
        body=None,
        idempotency_key=None,
        model="gpt-x",
        max_runtime_seconds=0,
    )
    result = run_gateway_checks(request)
    assert result.ok is False
    assert result.reason_code is not None
    flattened = codes(request)
    assert {
        "g1_body_missing",
        "g2_idempotency_key_required",
        "g5_model_provider_override",
        "g12_max_runtime_invalid",
    } <= flattened


def test_gateway_result_reason_code_prefers_reject_over_warn() -> None:
    result = run_gateway_checks(good_request(body=None))
    assert result.reason_code is None  # warn-only result stays ok
    result = run_gateway_checks(good_request(idempotency_key=None, body=None))
    assert result.reason_code == "g2_idempotency_key_required"


def test_gateway_request_for_admission_sets_key() -> None:
    request = gateway_request_for_admission(good_request(idempotency_key=None), "k1")
    assert request.idempotency_key == "k1"


# Admission wiring ----------------------------------------------------------


def fake_runner(calls: list[list[str]]) -> object:
    def runner(command: Sequence[str]) -> NativeResult:
        calls.append(list(command))
        if "create" in command:
            return NativeResult(
                0, '{"ok": true, "task_id": "t_gateway_child", "status": "blocked"}'
            )
        if "promote" in command:
            return NativeResult(0, '{"ok": true, "status": "ready"}')
        return NativeResult(2, "", "unexpected native command")

    return runner


def test_admit_child_runs_gateway_before_native_create(tmp_path) -> None:
    state = ControllerState.initialize(tmp_path / "state.sqlite3", "gateway-test")
    with state:
        assert OutcomeGuard(state).register_contract(
            contract("root", allowed_effects=["isolated_prototype"])
        ).allowed
        calls: list[list[str]] = []

        def bad_runner(command: Sequence[str]) -> NativeResult:
            calls.append(list(command))
            return NativeResult(0, '{"ok": true, "task_id": "t_x"}')

        with pytest.raises(AdmissionError) as excinfo:
            admit_child(
                ControllerConfig(
                    "gateway-test", tmp_path / "boards", state.path, native_cli="hermes"
                ),
                state,
                parent_task_id="t_parent",
                contract_ref="root",
                effect="isolated_prototype",
                board_slug="alpha",
                title="",  # fails G6 -> unsluggable -> gateway reject
                assignee="dev",
                runner=bad_runner,
            )
        assert "gateway rejected child create" in str(excinfo.value)
        assert calls == []  # no native call after a gateway rejection
