"""Gate 2 acceptance: portable Git reference-transaction adapter."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from hkrc.config import ControllerConfig, write_config
from hkrc.git_enforce import (
    GitEnforceError,
    evaluate_transaction,
    hook_status,
    install_hook,
    parse_reference_transaction,
    run_hook_command,
    uninstall_hook,
)
from hkrc.outcome_guard import OutcomeGuard
from hkrc.state import ControllerState

from test_outcome_guard import authority, contract

ROOT = Path(__file__).resolve().parents[1]

MAIN = "refs/heads/main"
ZERO = "0000000000000000000000000000000000000000"
OID = "1111111111111111111111111111111111111111"
TUPLES = f"{ZERO} {OID} {MAIN}\n"


def open_guard(tmp_path: Path, instance: str = "gate2-git") -> tuple[ControllerState, OutcomeGuard]:
    state = ControllerState.initialize(tmp_path / "state.sqlite3", instance)
    return state, OutcomeGuard(state)


def register_merge_contract(state: ControllerState, guard: OutcomeGuard) -> str:
    """Register a contract that allows merge_main and demands review evidence."""
    fresh = authority("auth-merge")
    fresh["statement"] = "Authorize repository work and merge after independent review."
    document = contract(
        "implementation",
        allowed_effects=["repository_modify", "merge_main"],
        authority_source=fresh,
        terminal_evidence=[
            {"evidence_type": "independent_review", "evidence_ref": "gate2-review"}
        ],
    )
    assert guard.register_contract(document).allowed
    return "implementation"


def write_evidence(tmp_path: Path) -> Path:
    evidence = tmp_path / "evidence.json"
    evidence.write_text(
        json.dumps(
            [{"evidence_type": "independent_review", "evidence_ref": "gate2-review"}]
        ),
        encoding="utf-8",
    )
    return evidence


def authorize(
    state: ControllerState,
    ref: str = MAIN,
    *,
    contract_ref: str = "implementation",
    evidence: list[dict[str, str]] | None = None,
) -> None:
    state.connection.execute(
        """
        INSERT INTO outcome_merge_authorizations
            (ref, task_id, contract_ref, evidence_json, authorized_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ref, task_id) DO UPDATE SET
            contract_ref = excluded.contract_ref,
            evidence_json = excluded.evidence_json,
            authorized_at = excluded.authorized_at
        """,
        (ref, "t_task", contract_ref, json.dumps(evidence or []), "2026-08-12T12:00:00+00:00"),
    )
    state.connection.commit()


# --- parsing ----------------------------------------------------------------


def test_parse_valid_tuples_and_pseudo_refs() -> None:
    updates = parse_reference_transaction(
        [
            f"{ZERO} {OID} refs/heads/main",
            f"{OID} {ZERO} refs/heads/feature",
            f"{ZERO} {OID} HEAD",  # real git sends pseudo-ref lines
        ]
    )
    assert [u.ref for u in updates] == ["refs/heads/main", "refs/heads/feature", "HEAD"]


def test_parse_fails_closed_on_malformed_input() -> None:
    with pytest.raises(GitEnforceError, match="expected"):
        parse_reference_transaction(["single-token\n"])
    with pytest.raises(GitEnforceError, match="non-object-id"):
        parse_reference_transaction(["notanoid notanoid refs/heads/main\n"])


# --- policy evaluation ------------------------------------------------------


def test_non_protected_refs_pass_without_state(tmp_path: Path) -> None:
    state, _ = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    decision = evaluate_transaction(
        state, config, parse_reference_transaction([f"{ZERO} {OID} refs/heads/feature\n"])
    )
    assert decision.allowed is True
    assert decision.reason_code == "no_protected_refs"


def test_protected_ref_denied_without_authorization(tmp_path: Path) -> None:
    state, _ = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    decision = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert decision.allowed is False
    assert decision.reason_code == "no_merge_authorization"
    assert decision.denied_refs == (MAIN,)


def test_authorization_contract_missing_fails_closed(tmp_path: Path) -> None:
    state, _ = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    authorize(state, contract_ref="ghost")
    decision = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert decision.allowed is False
    assert decision.reason_code == "authorization_contract_missing"


def test_contract_without_merge_main_denied(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    assert guard.register_contract(
        contract("prototype", allowed_effects=["isolated_prototype"])
    ).allowed
    authorize(state, contract_ref="prototype")
    decision = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert decision.allowed is False
    assert decision.reason_code == "merge_main_not_allowed"


def test_merge_waits_for_bound_independent_review_evidence(tmp_path: Path) -> None:
    """Acceptance D git half: authorization without evidence denies the hook;
    re-authorization with evidence allows it."""
    state, guard = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    register_merge_contract(state, guard)
    authorize(state)  # no evidence bound yet
    first = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert first.allowed is False
    assert first.reason_code == "merge_evidence_missing"
    assert first.missing_evidence_refs == ("gate2-review",)
    authorize(
        state,
        evidence=[{"evidence_type": "independent_review", "evidence_ref": "gate2-review"}],
    )
    second = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert second.allowed is True
    assert second.reason_code == "authorized"


def test_latest_authorization_wins(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    config = ControllerConfig("gate2-git", tmp_path / "boards", state.path)
    assert guard.register_contract(
        contract("prototype", allowed_effects=["isolated_prototype"])
    ).allowed
    register_merge_contract(state, guard)
    # Older binding (prototype, which forbids merge_main) then newer binding.
    authorize(state, contract_ref="prototype")
    authorize(
        state,
        evidence=[{"evidence_type": "independent_review", "evidence_ref": "gate2-review"}],
    )
    decision = evaluate_transaction(state, config, parse_reference_transaction([TUPLES]))
    assert decision.allowed is True  # latest binding governs


# --- hook command (fail-closed surface) -------------------------------------


def test_hook_command_non_prepared_states_pass(tmp_path: Path) -> None:
    for state_name in ("committed", "aborted"):
        assert run_hook_command(tmp_path / "missing.toml", hook_state=state_name) == 0


def test_hook_command_malformed_input_fails_closed(tmp_path: Path) -> None:
    state, guard = open_guard(tmp_path)
    config_path = tmp_path / "config.toml"
    write_config(config_path, ControllerConfig("gate2-git", tmp_path / "boards", state.path))
    register_merge_contract(state, guard)
    assert (
        run_hook_command(config_path, hook_state="prepared", stdin_lines=["garbage line\n"])
        == 1
    )


def test_hook_command_unavailable_config_fails_closed(tmp_path: Path) -> None:
    assert (
        run_hook_command(tmp_path / "nope.toml", hook_state="prepared", stdin_lines=[TUPLES])
        == 1
    )


def test_hook_command_unavailable_state_fails_closed(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    write_config(
        config_path,
        ControllerConfig("gate2-git", tmp_path / "boards", tmp_path / "missing.sqlite3"),
    )
    assert run_hook_command(config_path, hook_state="prepared", stdin_lines=[TUPLES]) == 1


def test_hook_command_audit_only_never_denies(tmp_path: Path, capsys) -> None:
    state, guard = open_guard(tmp_path)
    config_path = tmp_path / "config.toml"
    write_config(config_path, ControllerConfig("gate2-git", tmp_path / "boards", state.path))
    register_merge_contract(state, guard)
    assert (
        run_hook_command(
            config_path,
            hook_state="prepared",
            audit_only=True,
            stdin_lines=[TUPLES],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["allowed"] is False
    assert payload["reason_code"] == "no_merge_authorization"


# --- installer (acceptance G) -----------------------------------------------


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    path = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "main", str(path)], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "t@e.c"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "t"], check=True)
    (path / "f.txt").write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "f.txt"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-q", "-m", "baseline"], check=True)
    return path


def write_wrapper(tmp_path: Path) -> Path:
    wrapper = tmp_path / "hkrc-wrapper"
    wrapper.write_text(
        "#!/bin/sh\n"
        f"export PYTHONPATH={ROOT / 'src'}\n"
        f"exec {sys.executable} -m hkrc.cli \"$@\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


def write_gate2_state(tmp_path: Path) -> tuple[Path, Path]:
    state_path = tmp_path / "state.sqlite3"
    config_path = tmp_path / "config.toml"
    write_config(config_path, ControllerConfig("gate2-git", tmp_path / "boards", state_path))
    with ControllerState.initialize(state_path, "gate2-git") as state:
        register_merge_contract(state, OutcomeGuard(state))
    return state_path, config_path


def commit(repo: Path, message: str) -> subprocess.CompletedProcess[str]:
    (repo / "f.txt").write_text(f"{message}\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
    return subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", message],
        text=True,
        capture_output=True,
    )


def test_install_idempotent_and_uninstall_restores(tmp_path: Path, repo: Path) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    hook_path = repo / ".git" / "hooks" / "reference-transaction"

    first = install_hook(repo, wrapper=wrapper, config_path=config_path)
    second = install_hook(repo, wrapper=wrapper, config_path=config_path)
    assert any("installed managed hook" in line for line in first)
    assert any("installed managed hook" in line for line in second)
    assert hook_path.is_file()
    assert not (repo / ".git" / "hooks" / "reference-transaction.hkrc-orig").exists()

    removed = uninstall_hook(repo)
    assert any("removed managed hook" in line for line in removed)
    assert not hook_path.exists()
    again = uninstall_hook(repo)  # idempotent
    assert any("nothing to do" in line for line in again)
    assert not hook_path.exists()
    assert commit(repo, "post-uninstall works").returncode == 0


def test_install_chains_foreign_hook_and_uninstall_restores_it(
    tmp_path: Path, repo: Path
) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    hooks_dir = repo / ".git" / "hooks"
    marker = tmp_path / "foreign-marker"
    foreign = hooks_dir / "reference-transaction"
    foreign.write_text(
        "#!/bin/sh\n"
        f"echo chained >> {marker}\n"
        "exit 0\n",
        encoding="utf-8",
    )
    foreign.chmod(0o755)

    lines = install_hook(repo, wrapper=wrapper, config_path=config_path)
    assert any("chained existing hook" in line for line in lines)
    assert (hooks_dir / "reference-transaction.hkrc-orig").is_file()
    assert not marker.exists()

    # Foreign hook still runs (chained) AND enforcement denies an unauthorized main.
    denied = commit(repo, "unauthorized")
    assert denied.returncode != 0
    assert "no_merge_authorization" in (denied.stderr + denied.stdout)
    assert marker.read_text().count("chained") >= 1

    removed = uninstall_hook(repo)
    assert any("restored original hook" in line for line in removed)
    assert not (hooks_dir / "reference-transaction.hkrc-orig").exists()
    assert "echo chained" in foreign.read_text(encoding="utf-8")  # original back
    marker.unlink(missing_ok=True)
    assert commit(repo, "foreign only now").returncode == 0
    assert marker.exists()  # the restored foreign hook ran


def test_install_refuses_when_core_hooks_path_redirects(tmp_path: Path, repo: Path) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", str(tmp_path / "other-hooks")],
        check=True,
    )
    with pytest.raises(GitEnforceError, match="core.hooksPath"):
        install_hook(repo, wrapper=wrapper, config_path=config_path)
    assert not (repo / ".git" / "hooks" / "reference-transaction").exists()


def test_install_refuses_non_git_repo(tmp_path: Path) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    with pytest.raises(GitEnforceError, match="cannot resolve git dir"):
        install_hook(tmp_path / "not-a-repo", wrapper=wrapper, config_path=config_path)


def test_status_reports_managed_and_not_installed(tmp_path: Path, repo: Path) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    lines = hook_status(repo)
    assert any("not installed" in line for line in lines)
    install_hook(repo, wrapper=wrapper, config_path=config_path)
    lines = hook_status(repo)
    assert any("managed=yes" in line for line in lines)


# --- real git integration (acceptance E) ------------------------------------


def test_authorized_reviewed_main_update_succeeds_and_uninstall_restores_git(
    tmp_path: Path, repo: Path
) -> None:
    state_path, config_path = write_gate2_state(tmp_path)
    wrapper = write_wrapper(tmp_path)
    write_evidence(tmp_path)
    install_hook(repo, wrapper=wrapper, config_path=config_path)

    # Unauthorized protected-main update is denied by the real hook.
    denied = commit(repo, "unauthorized")
    assert denied.returncode != 0
    assert "no_merge_authorization" in (denied.stderr + denied.stdout)

    # Authorize with review evidence; the same update now succeeds.
    with ControllerState.initialize(state_path, "gate2-git") as state:
        authorize(
            state,
            evidence=[
                {"evidence_type": "independent_review", "evidence_ref": "gate2-review"}
            ],
        )
    allowed = commit(repo, "authorized")
    assert allowed.returncode == 0, (allowed.stdout, allowed.stderr)

    # Non-protected refs work while the hook is installed.
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "-b", "feature"], check=True)
    feature = commit(repo, "feature work")
    assert feature.returncode == 0, (feature.stdout, feature.stderr)

    # Uninstall restores normal git on the protected ref.
    uninstall_hook(repo)
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "main"], check=True)
    normal = commit(repo, "post-uninstall")
    assert normal.returncode == 0
