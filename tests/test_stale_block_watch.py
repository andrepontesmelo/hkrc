from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from hkrc.config import ControllerConfig
from hkrc.review_gap import NativeResult
from hkrc.stale_block_watch import (
    DEATH_KINDS,
    DeathBlockCandidate,
    StaleBlockWatchError,
    candidate_from_show,
    default_state_path,
    discover_silent_death_blocks,
    format_message,
    is_config_defect,
    is_silent_death_block,
    render_fresh,
    run,
)


def make_config() -> ControllerConfig:
    return ControllerConfig(
        "test",
        Path("/unused/native"),
        Path("/unused/state/hkrc/state.sqlite3"),
        native_cli="hermes",
    )


def blocked_show(
    task_id: str = "t_dead",
    status: str = "blocked",
    latest_kind: str = "gave_up",
    event_id: int = 100,
    run_error: str = "elapsed 61s > limit 0s",
    title: str = "some task",
) -> str:
    return json.dumps(
        {
            "task": {"id": task_id, "status": status, "title": title},
            "events": [
                {"kind": "created", "payload": {}, "created_at": 1, "run_id": None},
                {
                    "kind": latest_kind,
                    "payload": {"failures": 2, "error": run_error},
                    "created_at": 2,
                    "run_id": 9,
                    "id": event_id,
                },
            ],
            "runs": [
                {"id": 8, "status": "timed_out", "error": run_error, "outcome": "gave_up"},
            ],
        }
    )


def test_death_kinds_cover_the_silent_class() -> None:
    assert DEATH_KINDS == {"gave_up", "spawn_failed", "timed_out", "crashed"}


def test_is_silent_death_block_requires_blocked_status_and_death_kind() -> None:
    assert is_silent_death_block(status="blocked", latest_event_kind="gave_up", run_error=None)
    assert is_silent_death_block(status="blocked", latest_event_kind="spawn_failed", run_error=None)
    assert not is_silent_death_block(status="blocked", latest_event_kind="blocked", run_error=None)
    assert not is_silent_death_block(status="ready", latest_event_kind="gave_up", run_error=None)
    assert not is_silent_death_block(status="done", latest_event_kind="gave_up", run_error=None)
    assert not is_silent_death_block(status="blocked", latest_event_kind=None, run_error=None)


def test_is_config_defect_matches_limit_zero_signature() -> None:
    assert is_config_defect("elapsed 61s > limit 0s")
    assert is_config_defect("elapsed 62s > limit 0s")
    assert is_config_defect("run 1: elapsed 61s > limit 0s; worker killed")
    assert not is_config_defect("iteration budget exhausted")
    assert not is_config_defect(None)


def test_candidate_from_show_builds_typed_candidate() -> None:
    show = blocked_show()
    candidate = candidate_from_show("rentcli", "t_dead", json.loads(show))
    assert candidate is not None
    assert candidate.board_slug == "rentcli"
    assert candidate.task_id == "t_dead"
    assert candidate.latest_event_kind == "gave_up"
    assert candidate.latest_event_id == 100
    assert candidate.config_defect is True
    assert candidate.key == "rentcli:t_dead:100"


def test_candidate_from_show_skips_typed_blocked_latest() -> None:
    show = blocked_show(latest_kind="blocked", event_id=101)
    show_doc = json.loads(show)
    show_doc["events"][-1]["payload"] = {"kind": "needs_input", "reason": "pick"}
    candidate = candidate_from_show("rentcli", "t_dead", show_doc)
    assert candidate is None


def test_candidate_from_show_skips_terminal_status() -> None:
    candidate = candidate_from_show("rentcli", "t_dead", json.loads(blocked_show(status="done")))
    assert candidate is None


def test_candidate_from_show_rejects_wrong_task_and_missing_status() -> None:
    import pytest

    doc = json.loads(blocked_show())
    doc["task"]["id"] = "t_other"
    with pytest.raises(StaleBlockWatchError):
        candidate_from_show("rentcli", "t_dead", doc)
    doc = json.loads(blocked_show())
    del doc["task"]["status"]
    with pytest.raises(StaleBlockWatchError):
        candidate_from_show("rentcli", "t_dead", doc)


def test_candidate_from_show_anchors_on_created_at_when_event_id_missing() -> None:
    # Old-schema events carry NO id field at all (verified 2026-08-06 default
    # board t_1a9669a8); created_at must anchor the dedupe key instead.
    doc = json.loads(blocked_show())
    for raw_event in doc["events"]:
        raw_event.pop("id", None)
        raw_event["created_at"] = 1786003605
    candidate = candidate_from_show("default", "t_dead", doc)
    assert candidate is not None
    assert candidate.latest_event_id == 1786003605
    assert candidate.key == "default:t_dead:1786003605"


def test_render_fresh_dedupes_by_episode_key(tmp_path: Path) -> None:
    a = DeathBlockCandidate("b", "t1", "x", "gave_up", 10, "elapsed 61s > limit 0s", True)
    b = DeathBlockCandidate("b", "t2", "y", "timed_out", 11, "", False)
    fresh, updated = render_fresh([a, b], {})
    assert [c.task_id for c in fresh] == ["t1", "t2"]
    assert len(updated) == 2
    fresh2, updated2 = render_fresh([a, b], updated)
    assert fresh2 == []
    assert updated2 == updated


def test_format_message_flags_config_defect(tmp_path: Path) -> None:
    candidate = DeathBlockCandidate(
        "rentcli", "t_8d170db9", "gate", "gave_up", 100, "elapsed 61s > limit 0s", True
    )
    message = format_message([candidate])
    assert "Silent death block" in message
    assert "CONFIG DEFECT" in message
    assert "max_runtime" in message
    plain = DeathBlockCandidate("b", "t2", "y", "timed_out", 11, "boom", False)
    message2 = format_message([plain])
    assert "CONFIG DEFECT" not in message2
    assert "boom" in message2


def test_discover_silent_death_blocks_uses_cli_only() -> None:
    config = make_config()
    calls: list[list[str]] = []

    def fake_runner(argv: Sequence[str]) -> NativeResult:
        calls.append(list(argv))
        if "boards" in argv and "list" in argv:
            return NativeResult(
                0, json.dumps([{"slug": "rentcli", "archived": False}]), ""
            )
        if "list" in argv and "--status" in argv:
            return NativeResult(0, json.dumps([{"id": "t_dead"}]), "")
        if "show" in argv:
            return NativeResult(0, blocked_show(), "")
        return NativeResult(1, "", "unexpected")

    candidates = discover_silent_death_blocks(config, runner=fake_runner)
    assert len(candidates) == 1
    assert candidates[0].task_id == "t_dead"
    assert any("boards" in call for call in calls)
    assert all("--json" in call for call in calls)


def test_run_returns_empty_when_nothing_fresh(tmp_path: Path) -> None:
    config = ControllerConfig(
        "test",
        Path("/unused/native"),
        tmp_path / "state" / "hkrc" / "state.sqlite3",
        native_cli="hermes",
    )
    state_path = default_state_path(config.state_db)

    def fake_runner(argv: Sequence[str]) -> NativeResult:
        if "boards" in argv and "list" in argv:
            return NativeResult(0, json.dumps([{"slug": "rentcli", "archived": False}]), "")
        if "list" in argv and "--status" in argv:
            return NativeResult(0, json.dumps([{"id": "t_dead"}]), "")
        if "show" in argv:
            return NativeResult(0, blocked_show(), "")
        return NativeResult(1, "", "unexpected")

    # First run: fresh episode → digest.
    message = run(config, state_path, runner=fake_runner)
    assert "Silent death block" in message
    # Second run: deduped → silent.
    message2 = run(config, state_path, runner=fake_runner)
    assert message2 == ""
    assert state_path.is_file()
