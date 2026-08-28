"""Unit tests for the persona-matrix drift flagger (t_f7cc2828).

The flagger is report-only: HKRC never writes ``~/.hermes`` profile state
(effect boundary, t_7dca44ce batch-1 #3). Every check here uses fake
``ProfileSnapshot`` objects or fixture ``config.yaml`` files under
``tmp_path``; the live profiles root is never touched, and the read path is
proven to leave its input bytes unchanged.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from hkrc.persona_drift import (
    ELIMINATED_PERSONA,
    OVERRIDE_PRESENT,
    REASONING_MISMATCH,
    REASONING_NONE,
    REASONING_UNSET,
    WRONG_MODEL,
    DriftFinding,
    ProfileSnapshot,
    check_profiles,
    check_snapshot,
    check_snapshots,
    default_profiles_root,
    read_profile_config,
)

CODES = {WRONG_MODEL, REASONING_UNSET, REASONING_NONE, REASONING_MISMATCH,
         OVERRIDE_PRESENT, ELIMINATED_PERSONA}


def codes(findings: list[DriftFinding]) -> list[str]:
    return [f.code for f in findings]


# --- passes on matching config ----------------------------------------------


def test_developer_matching_config_passes() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="high",
    )
    assert check_snapshot(snapshot) == []


def test_reviewer_matching_config_passes() -> None:
    snapshot = ProfileSnapshot(
        persona="reviewer", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="high",
    )
    assert check_snapshot(snapshot) == []


def test_senior_dev_matching_config_passes() -> None:
    # senior-dev = pro; reasoning is not enforced by the matrix.
    snapshot = ProfileSnapshot(
        persona="senior-dev", model="zai/glm-5.3",
    )
    assert check_snapshot(snapshot) == []


def test_frontend_dev_and_orchestrator_and_explore_pass() -> None:
    # frontend-dev = luna-high (t_545a638a); lead-orchestrator and explore
    # stay flash.
    snapshots = [
        ProfileSnapshot(persona="frontend-dev", model="cx/gpt-5.6-luna-high"),
        ProfileSnapshot(
            persona="lead-orchestrator", model="opencode-go/deepseek-v4-flash",
        ),
        ProfileSnapshot(persona="explore", model="opencode-go/deepseek-v4-flash"),
    ]
    for snapshot in snapshots:
        assert check_snapshot(snapshot) == [], snapshot.persona


def test_frontend_dev_on_flash_now_flagged() -> None:
    # Regression (t_545a638a): frontend-dev moved flash -> luna-high; a
    # flash config is now drift even though the tier maps.
    snapshot = ProfileSnapshot(
        persona="frontend-dev", model="opencode-go/deepseek-v4-flash",
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [WRONG_MODEL]
    assert "flash" in findings[0].message
    assert "luna-high" in findings[0].message


def test_authoritative_matching_config_passes() -> None:
    snapshot = ProfileSnapshot(
        persona="authoritative", model="zai/glm-5.3",
        reasoning_effort="medium",
        reasoning_overrides=(("zai/glm-5.3", "max"),),
    )
    assert check_snapshot(snapshot) == []


# --- wrong model / unset model ----------------------------------------------


def test_wrong_model_flagged() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="zai/glm-5.3",
        reasoning_effort="high",
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [WRONG_MODEL]
    assert "zai/glm-5.3" in findings[0].message
    assert "flash" in findings[0].message


def test_unknown_tier_model_id_flagged() -> None:
    # An id naming neither tier (nor the explicit pro id) is unrecognized
    # and therefore drift.
    snapshot = ProfileSnapshot(
        persona="senior-dev", model="opencode-go/deepseek-v4-ultra",
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [WRONG_MODEL]
    assert "deepseek-v4-ultra" in findings[0].message


def test_model_unset_flagged() -> None:
    snapshot = ProfileSnapshot(persona="developer", reasoning_effort="high")
    assert codes(check_snapshot(snapshot)) == [WRONG_MODEL]


def test_senior_dev_on_flash_flagged() -> None:
    # The live pre-runbook state: senior-dev defaults to flash.
    snapshot = ProfileSnapshot(
        persona="senior-dev", model="opencode-go/deepseek-v4-flash",
    )
    assert codes(check_snapshot(snapshot)) == [WRONG_MODEL]


# --- reasoning unset / none / mismatch --------------------------------------


def test_reasoning_unset_flagged() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="opencode-go/deepseek-v4-flash",
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [REASONING_UNSET]
    assert "high" in findings[0].message


def test_reasoning_none_flagged_on_any_matrix_persona() -> None:
    # "none" is drift even on personas where the matrix does not enforce a
    # level: reasoning disabled is never part of the matrix.
    for persona in ("developer", "reviewer", "senior-dev", "explore",
                    "authoritative"):
        snapshot = ProfileSnapshot(
            persona=persona, model="zai/glm-5.3"
            if persona in ("senior-dev", "authoritative")
            else "opencode-go/deepseek-v4-flash",
            reasoning_effort="none",
        )
        assert REASONING_NONE in codes(check_snapshot(snapshot)), persona


def test_reasoning_mismatch_flagged() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="medium",
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [REASONING_MISMATCH]
    assert "medium" in findings[0].message


def test_reasoning_not_enforced_personas_pass_without_reasoning() -> None:
    snapshot = ProfileSnapshot(
        persona="explore", model="opencode-go/deepseek-v4-flash",
    )
    assert check_snapshot(snapshot) == []


# --- overrides --------------------------------------------------------------


def test_override_flagged_on_active_persona() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="high",
        reasoning_overrides=(("zai/glm-5.3", "high"),),
    )
    findings = check_snapshot(snapshot)
    assert OVERRIDE_PRESENT in codes(findings)
    assert "zai/glm-5.3" in findings[-1].message


def test_authoritative_override_exception_only_for_allowed_pair() -> None:
    allowed = ProfileSnapshot(
        persona="authoritative", model="zai/glm-5.3",
        reasoning_overrides=(("zai/glm-5.3", "max"),),
    )
    assert check_snapshot(allowed) == []
    other = ProfileSnapshot(
        persona="authoritative", model="zai/glm-5.3",
        reasoning_overrides=(("opencode-go/deepseek-v4-flash", "high"),),
    )
    assert codes(check_snapshot(other)) == [OVERRIDE_PRESENT]


# --- eliminated persona -----------------------------------------------------


def test_eliminated_persona_with_config_flagged() -> None:
    snapshot = ProfileSnapshot(
        persona="griller", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="high",
        reasoning_overrides=(("zai/glm-5.3", "high"),),
    )
    findings = check_snapshot(snapshot)
    assert codes(findings) == [ELIMINATED_PERSONA]
    assert "griller" in findings[0].message
    assert "retire" in findings[0].message


def test_eliminated_persona_without_config_passes() -> None:
    snapshot = ProfileSnapshot(persona="griller")
    assert check_snapshot(snapshot) == []


# --- scope / purity ---------------------------------------------------------


def test_out_of_scope_persona_skipped() -> None:
    for persona in ("adversary", "main", "default", "nobody-here"):
        snapshot = ProfileSnapshot(persona=persona, model="anything")
        assert check_snapshot(snapshot) == [], persona


def test_check_snapshots_aggregates_and_never_mutates_input() -> None:
    snapshots = [
        ProfileSnapshot(
            persona="developer", model="opencode-go/deepseek-v4-flash",
            reasoning_effort="high",
        ),
        ProfileSnapshot(persona="reviewer"),  # model unset + reasoning unset
        ProfileSnapshot(
            persona="senior-dev", model="opencode-go/deepseek-v4-flash",
        ),
        ProfileSnapshot(persona="griller", model="opencode-go/deepseek-v4-flash"),
        ProfileSnapshot(persona="adversary", model="whatever"),
    ]
    before = copy.deepcopy(snapshots)
    findings = check_snapshots(snapshots)
    assert snapshots == before, "check_snapshots mutated its input"
    assert codes(findings) == [WRONG_MODEL, REASONING_UNSET, WRONG_MODEL,
                               ELIMINATED_PERSONA]


def test_check_snapshot_never_mutates_snapshot() -> None:
    snapshot = ProfileSnapshot(
        persona="developer", model="opencode-go/deepseek-v4-flash",
        reasoning_effort="high",
    )
    before = copy.deepcopy(snapshot)
    check_snapshot(snapshot)
    assert snapshot == before


def test_finding_codes_are_stable_strings() -> None:
    assert CODES == {
        "wrong-model", "reasoning-unset", "reasoning-none",
        "reasoning-mismatch", "override-present", "eliminated-persona",
    }


# --- live read path (fixture config.yaml files under tmp_path) --------------


DEVELOPER_CONFIG = """\
model:
  default: opencode-go/deepseek-v4-flash
  provider: opencode-proxy
skills:
  external_dirs:
  - ~/git/hermes-skills-dist/skills
"""

GRILLER_CONFIG = """\
model:
  default: opencode-go/deepseek-v4-flash
  provider: opencode-proxy
agent:
  reasoning_effort: high
  reasoning_overrides: '{"zai/glm-5.3": "high"}'
"""

AUTHORITATIVE_CONFIG = """\
model:
  default: zai/glm-5.3
  provider: custom:omniroute
agent:
  reasoning_effort: medium
  reasoning_overrides:
    "zai/glm-5.3": max
"""


def _write(root: Path, name: str, content: str) -> Path:
    profile_dir = root / name
    profile_dir.mkdir()
    config = profile_dir / "config.yaml"
    config.write_text(content, encoding="utf-8")
    return config


def test_read_profile_config_developer_shape(tmp_path: Path) -> None:
    path = _write(tmp_path, "developer", DEVELOPER_CONFIG)
    snapshot = read_profile_config(path)
    assert snapshot.persona == "developer"
    assert snapshot.model == "opencode-go/deepseek-v4-flash"
    assert snapshot.reasoning_effort is None
    assert snapshot.reasoning_overrides == ()


def test_read_profile_config_griller_override_json_string(tmp_path: Path) -> None:
    path = _write(tmp_path, "griller", GRILLER_CONFIG)
    snapshot = read_profile_config(path)
    assert snapshot.model == "opencode-go/deepseek-v4-flash"
    assert snapshot.reasoning_effort == "high"
    assert snapshot.reasoning_overrides == (("zai/glm-5.3", "high"),)


def test_read_profile_config_authoritative_override_block(tmp_path: Path) -> None:
    path = _write(tmp_path, "authoritative", AUTHORITATIVE_CONFIG)
    snapshot = read_profile_config(path)
    assert snapshot.model == "zai/glm-5.3"
    assert snapshot.reasoning_effort == "medium"
    assert snapshot.reasoning_overrides == (("zai/glm-5.3", "max"),)


def test_read_profile_config_never_writes_input(tmp_path: Path) -> None:
    path = _write(tmp_path, "developer", DEVELOPER_CONFIG)
    before = path.read_bytes()
    read_profile_config(path)
    assert path.read_bytes() == before


def test_read_profile_config_ignores_sibling_agent_blocks(tmp_path: Path) -> None:
    # Regression: agent keys that follow reasoning_overrides (e.g.
    # personalities) must not leak into the override pairs.
    config = """\
model:
  default: zai/glm-5.3
agent:
  reasoning_effort: medium
  reasoning_overrides:
    "zai/glm-5.3": max
  personalities:
    helpful: You are a helpful, friendly AI assistant.
    kawaii: You are a kawaii assistant!
"""
    path = _write(tmp_path, "authoritative", config)
    snapshot = read_profile_config(path)
    assert snapshot.reasoning_effort == "medium"
    assert snapshot.reasoning_overrides == (("zai/glm-5.3", "max"),)
    assert check_snapshot(snapshot) == []


def test_check_profiles_sweeps_only_matrix_personas(tmp_path: Path) -> None:
    for name, content in (
        ("developer", DEVELOPER_CONFIG),
        ("senior-dev", "model:\n  default: opencode-go/deepseek-v4-flash\n"),
        ("griller", GRILLER_CONFIG),
        ("adversary", "model:\n  default: opencode-go/deepseek-v4-flash\n"),
        ("main", "model:\n  default: opencode-go/deepseek-v4-flash\n"),
    ):
        _write(tmp_path, name, content)
    findings = check_profiles(tmp_path)
    # adversary and main are out of scope. Directory order is alphabetical:
    # developer (reasoning-unset), griller (eliminated), senior-dev
    # (wrong-model).
    assert codes(findings) == [REASONING_UNSET, ELIMINATED_PERSONA, WRONG_MODEL]
    assert {f.persona for f in findings} == {"developer", "griller", "senior-dev"}


def test_check_profiles_creates_no_files(tmp_path: Path) -> None:
    _write(tmp_path, "developer", DEVELOPER_CONFIG)
    _write(tmp_path, "griller", GRILLER_CONFIG)
    before = sorted(str(p) for p in tmp_path.rglob("*"))
    check_profiles(tmp_path)
    after = sorted(str(p) for p in tmp_path.rglob("*"))
    assert after == before


def test_check_profiles_missing_root_returns_empty(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    assert check_profiles(missing) == []


def test_default_profiles_root_is_read_only_surface() -> None:
    assert default_profiles_root() == Path.home() / ".hermes" / "profiles"


def test_default_profiles_root_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HKRC_PROFILES_ROOT", "/tmp/overridden-profiles-root")
    assert default_profiles_root() == Path("/tmp/overridden-profiles-root")
    monkeypatch.delenv("HKRC_PROFILES_ROOT")
    assert default_profiles_root() == Path.home() / ".hermes" / "profiles"
