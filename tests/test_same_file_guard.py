from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Callable

import pytest

from hkrc.config import ConfigError, ControllerConfig, SameFileGuardConfig
from hkrc.handoff import NativeResult
from hkrc.same_file_guard import (
    CANDIDATE_STATUSES,
    REVIEWER_ASSIGNEE,
    SameFileGuardError,
    block_reason,
    build_block_command,
    build_comment_command,
    candidate_from_show,
    declared_paths,
    default_state_path,
    dependency_linked,
    discover_boards,
    is_candidate,
    list_tasks,
    run,
    show_task,
)

NOW = 1_800_000_000
PATTERN = r"(?:src|tests|scripts|config|docs)/[A-Za-z0-9_./-]+\.(?:py|md|json|toml|sh|ya?ml)"


def make_config(
    *,
    enabled: bool = True,
    min_age_seconds: int | float = 120,
    auto_block: bool = True,
    ignored_paths: tuple[str, ...] = ("scripts/green.sh",),
    path_pattern: str = PATTERN,
    cli_timeout_seconds: int | float = 30.0,
    tick_timeout_seconds: int | float = 120.0,
    native_cli: str = "hermes",
) -> ControllerConfig:
    return ControllerConfig(
        "test",
        Path("/tmp/nonexistent-boards"),
        Path("/tmp/nonexistent-state.sqlite3"),
        native_cli=native_cli,
        same_file_guard=SameFileGuardConfig(
            enabled=enabled,
            min_age_seconds=min_age_seconds,
            auto_block=auto_block,
            ignored_paths=ignored_paths,
            path_pattern=path_pattern,
            cli_timeout_seconds=cli_timeout_seconds,
            tick_timeout_seconds=tick_timeout_seconds,
        ),
    )


def list_task(
    task_id: str,
    *,
    status: str = "ready",
    created_at: int = NOW - 3600,
    assignee: str = "developer",
) -> dict:
    return {
        "id": task_id,
        "title": f"task: {task_id}",
        "status": status,
        "created_at": created_at,
        "assignee": assignee,
    }


def show_doc(
    task_id: str,
    *,
    status: str = "ready",
    created_at: int = NOW - 3600,
    assignee: str = "developer",
    body: str = "",
    parents: list[str] | None = None,
    children: list[str] | None = None,
) -> dict:
    return {
        "task": {
            "id": task_id,
            "title": f"task: {task_id}",
            "status": status,
            "created_at": created_at,
            "assignee": assignee,
            "body": body,
        },
        "children": children or [],
        "parents": parents or [],
        "events": [],
        "runs": [],
        "comments": [],
        "latest_summary": None,
    }


def make_runner(
    *,
    boards: list[dict] | None = None,
    tasks: dict[str, list[dict]] | None = None,
    shows: dict[str, dict] | None = None,
) -> tuple[Callable[[Sequence[str]], NativeResult], list[list[str]]]:
    """Fake native CLI runner: ``(runner, recorded_argv)``. Never touches a board."""
    boards = boards or [{"slug": "hkrc", "archived": False}]
    tasks = tasks or {}
    shows = shows or {}
    recorded: list[list[str]] = []

    def runner(argv: Sequence[str]) -> NativeResult:
        recorded.append(list(argv))
        if argv[1:3] == ["kanban", "boards"]:
            return NativeResult(0, json.dumps(boards), "")
        if argv[1] == "kanban" and argv[2] == "--board":
            slug = argv[3]
            subcommand = argv[4]
            if subcommand == "list":
                return NativeResult(0, json.dumps(tasks.get(slug, [])), "")
            if subcommand == "show":
                return NativeResult(0, json.dumps(shows.get(argv[5], {})), "")
            if subcommand == "block":
                return NativeResult(0, json.dumps({"id": argv[5], "status": "blocked"}), "")
            if subcommand == "comment":
                return NativeResult(0, json.dumps({"id": argv[5]}), "")
        return NativeResult(2, "", f"unexpected argv: {argv}")

    return runner, recorded


# --- path extraction ----------------------------------------------------------


def test_declared_paths_extracts_and_ignores_green_sh() -> None:
    body = (
        "Edit src/hkrc/harness_loop.py and run `bash scripts/green.sh`; "
        "also see config/hkrc/cron_manifest.json."
    )
    paths = declared_paths(body, path_pattern=PATTERN, ignored_paths=("scripts/green.sh",))
    assert paths == ("src/hkrc/harness_loop.py", "config/hkrc/cron_manifest.json")


def test_declared_paths_dedupes_and_normalizes() -> None:
    body = "touch ./src/hkrc/cli.py then src/hkrc/cli.py again"
    paths = declared_paths(body, path_pattern=PATTERN, ignored_paths=())
    assert paths == ("src/hkrc/cli.py",)


def test_declared_paths_empty_body() -> None:
    assert declared_paths("", path_pattern=PATTERN, ignored_paths=()) == ()


# --- candidate filtering --------------------------------------------------------


def test_is_candidate_states_min_age_and_reviewer() -> None:
    young = list_task("t_young", created_at=NOW - 60)
    assert not is_candidate(young, now=NOW, min_age_seconds=120)
    old = list_task("t_old", created_at=NOW - 3600)
    assert is_candidate(old, now=NOW, min_age_seconds=120)
    reviewer = list_task("t_rev", assignee=REVIEWER_ASSIGNEE)
    assert not is_candidate(reviewer, now=NOW, min_age_seconds=120)
    for status in ("done", "archived", "blocked"):
        assert not is_candidate(list_task("t_x", status=status), now=NOW, min_age_seconds=120)
    assert CANDIDATE_STATUSES == frozenset({"ready", "running", "review", "todo"})


def test_candidate_from_show_reads_body_paths() -> None:
    show = show_doc("t_a", body="edits src/hkrc/harness_loop.py")
    candidate = candidate_from_show(
        "hkrc", "t_a", show, path_pattern=PATTERN, ignored_paths=("scripts/green.sh",)
    )
    assert candidate.paths == ("src/hkrc/harness_loop.py",)
    assert candidate.key == "hkrc:t_a"


# --- dependency exemption ---------------------------------------------------------


def test_dependency_linked_direct_parent() -> None:
    holder = show_doc("t_holder")
    follower = show_doc("t_follower", parents=["t_holder"])
    assert dependency_linked(holder, follower, {})


def test_dependency_linked_transitive_chain() -> None:
    holder = show_doc("t_holder", children=["t_mid"])
    mid = show_doc("t_mid", parents=["t_holder"], children=["t_follower"])
    follower = show_doc("t_follower", parents=["t_mid"])
    assert dependency_linked(holder, follower, {"t_mid": mid})


def test_dependency_linked_unrelated_cards() -> None:
    holder = show_doc("t_holder")
    follower = show_doc("t_follower")
    assert not dependency_linked(holder, follower, {})


# --- end-to-end runs (stubbed CLI, hermetic) --------------------------------------


def test_overlapping_cards_block_follower(tmp_path: Path) -> None:
    holder = show_doc(
        "t_holder",
        created_at=NOW - 7200,
        body="Implements the flag store in src/hkrc/harness_loop.py.",
    )
    follower = show_doc(
        "t_follower",
        created_at=NOW - 3600,
        body="Also edits src/hkrc/harness_loop.py.",
    )
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower")]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    digest = run(make_config(), state_file, now=NOW, runner=runner)
    assert f"same-file-overlap: blocked t_follower on board hkrc" in digest
    assert "src/hkrc/harness_loop.py held by t_holder" in digest
    argv_strings = [" ".join(argv) for argv in recorded]
    block_calls = [s for s in argv_strings if " block " in s]
    comment_calls = [s for s in argv_strings if " comment " in s]
    assert block_calls == [
        "hermes kanban --board hkrc block t_follower --reason "
        + json.dumps(block_reason("src/hkrc/harness_loop.py", "t_holder")).strip('"')
    ] or any(
        "block t_follower" in s and block_reason("src/hkrc/harness_loop.py", "t_holder") in s
        for s in block_calls
    )
    assert any("comment t_follower" in s for s in comment_calls)
    assert any("comment t_holder" in s for s in comment_calls)
    # follower comment names the holder and the path
    follower_comment_cmd = next(s for s in comment_calls if "comment t_follower" in s)
    assert "t_holder" in follower_comment_cmd and "src/hkrc/harness_loop.py" in follower_comment_cmd
    # state persisted
    state = json.loads(state_file.read_text())
    assert any("t_follower" in key and "t_holder" in key for key in state)


def test_parent_linked_cards_not_blocked(tmp_path: Path) -> None:
    holder = show_doc(
        "t_holder",
        created_at=NOW - 7200,
        body="Edits src/hkrc/harness_loop.py.",
        children=["t_follower"],
    )
    follower = show_doc(
        "t_follower",
        created_at=NOW - 3600,
        body="fix findings for the holder; edits src/hkrc/harness_loop.py.",
        parents=["t_holder"],
    )
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower")]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    digest = run(make_config(), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""  # nothing to report — the pair is already serialized
    assert not any(" block " in " ".join(argv) for argv in recorded)


def test_young_follower_not_blocked(tmp_path: Path) -> None:
    holder = show_doc("t_holder", created_at=NOW - 7200, body="edits src/hkrc/cli.py")
    follower = show_doc(
        "t_follower",
        created_at=NOW - 60,  # younger than min_age_seconds=120
        body="edits src/hkrc/cli.py",
    )
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower", created_at=NOW - 60)]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    digest = run(make_config(), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""
    assert not any(" block " in " ".join(argv) for argv in recorded)


def test_reviewer_card_never_a_candidate(tmp_path: Path) -> None:
    reviewer_card = show_doc(
        "t_review", created_at=NOW - 7200, assignee=REVIEWER_ASSIGNEE, body="edits src/hkrc/cli.py"
    )
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_review", assignee=REVIEWER_ASSIGNEE)]},
        shows={"t_review": reviewer_card},
    )
    digest = run(make_config(), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""
    # the reviewer card is filtered at the list stage — never shown, never judged
    assert not any("show t_review" in " ".join(argv) for argv in recorded)


def test_ignored_green_sh_not_an_overlap(tmp_path: Path) -> None:
    a = show_doc("t_a", created_at=NOW - 7200, body="gate: bash scripts/green.sh")
    b = show_doc("t_b", created_at=NOW - 3600, body="gate: bash scripts/green.sh")
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_a"), list_task("t_b")]},
        shows={"t_a": a, "t_b": b},
    )
    digest = run(make_config(), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""
    assert not any(" block " in " ".join(argv) for argv in recorded)


def test_zero_path_card_nags_once_never_blocks(tmp_path: Path) -> None:
    mystery = show_doc("t_mystery", body="do the thing without naming files")
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_mystery")]},
        shows={"t_mystery": mystery},
    )
    state_file = tmp_path / "state.json"
    first = run(make_config(), state_file, now=NOW, runner=runner)
    assert "declares NO file paths" in first
    assert "t_mystery" in first
    assert not any(" block " in " ".join(argv) for argv in recorded)
    second = run(make_config(), state_file, now=NOW, runner=runner)
    assert second == ""  # dedupe: nag once, not every tick


def test_dry_run_reports_without_mutating(tmp_path: Path) -> None:
    holder = show_doc("t_holder", created_at=NOW - 7200, body="edits src/hkrc/cli.py")
    follower = show_doc("t_follower", created_at=NOW - 3600, body="edits src/hkrc/cli.py")
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower")]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    state_file = tmp_path / "state.json"
    digest = run(make_config(), state_file, now=NOW, dry_run=True, runner=runner)
    assert "(dry-run)" in digest and "t_follower" in digest and "t_holder" in digest
    assert not any(" block " in " ".join(argv) for argv in recorded)
    assert not any(" comment " in " ".join(argv) for argv in recorded)
    assert not state_file.exists()  # dry-run writes no state
    # and a second dry-run tick reports the same intent again (nothing recorded)
    digest2 = run(make_config(), state_file, now=NOW, dry_run=True, runner=runner)
    assert "(dry-run)" in digest2


def test_auto_block_false_reports_only_no_mutation(tmp_path: Path) -> None:
    holder = show_doc("t_holder", created_at=NOW - 7200, body="edits src/hkrc/cli.py")
    follower = show_doc("t_follower", created_at=NOW - 3600, body="edits src/hkrc/cli.py")
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower")]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    digest = run(
        make_config(auto_block=False), tmp_path / "state.json", now=NOW, runner=runner
    )
    assert "report-only" in digest
    assert not any(" block " in " ".join(argv) for argv in recorded)
    assert not any(" comment " in " ".join(argv) for argv in recorded)
    # reported once — the state records the pair so the digest stays quiet
    digest2 = run(
        make_config(auto_block=False), tmp_path / "state.json", now=NOW, runner=runner
    )
    assert digest2 == ""


def test_state_dedupe_no_duplicate_block(tmp_path: Path) -> None:
    holder = show_doc("t_holder", created_at=NOW - 7200, body="edits src/hkrc/cli.py")
    follower = show_doc("t_follower", created_at=NOW - 3600, body="edits src/hkrc/cli.py")
    state_file = tmp_path / "state.json"
    runner, recorded = make_runner(
        tasks={"hkrc": [list_task("t_holder"), list_task("t_follower")]},
        shows={"t_holder": holder, "t_follower": follower},
    )
    first = run(make_config(), state_file, now=NOW, runner=runner)
    assert "blocked t_follower" in first
    blocks_first = sum(1 for argv in recorded if " block " in " ".join(argv))
    assert blocks_first == 1
    second = run(make_config(), state_file, now=NOW, runner=runner)
    assert second == ""
    blocks_second = sum(1 for argv in recorded if " block " in " ".join(argv))
    assert blocks_second == blocks_first  # no new block on the second tick


def test_disabled_config_noop(tmp_path: Path) -> None:
    runner, recorded = make_runner(tasks={"hkrc": [list_task("t_a")]}, shows={})
    digest = run(make_config(enabled=False), tmp_path / "state.json", now=NOW, runner=runner)
    assert digest == ""
    assert recorded == []  # not even a read — full no-op


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("{not json")
    runner, _ = make_runner()
    with pytest.raises(SameFileGuardError):
        run(make_config(), state_file, now=NOW, runner=runner)


# --- config validation --------------------------------------------------------------


def test_config_validation_rejects_bad_values() -> None:
    with pytest.raises(ConfigError, match="same_file_guard enabled must be a boolean"):
        SameFileGuardConfig(enabled="yes")  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="min_age_seconds must be a positive number"):
        SameFileGuardConfig(min_age_seconds=0)
    with pytest.raises(ConfigError, match="min_age_seconds must be a positive number"):
        SameFileGuardConfig(min_age_seconds=-5)
    with pytest.raises(ConfigError, match="auto_block must be a boolean"):
        SameFileGuardConfig(auto_block="no")  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="ignored_paths must be a tuple"):
        SameFileGuardConfig(ignored_paths=("",))  # type: ignore[list-item]
    with pytest.raises(ConfigError, match="ignored_paths must not contain duplicates"):
        SameFileGuardConfig(ignored_paths=("scripts/green.sh", "scripts/green.sh"))
    with pytest.raises(ConfigError, match="path_pattern must be a non-empty string"):
        SameFileGuardConfig(path_pattern="  ")
    with pytest.raises(ConfigError, match="path_pattern is not a valid regex"):
        SameFileGuardConfig(path_pattern="(unclosed")
    with pytest.raises(ConfigError, match="cli_timeout_seconds must be a positive number"):
        SameFileGuardConfig(cli_timeout_seconds=0)
    with pytest.raises(ConfigError, match="tick_timeout_seconds must be a positive number"):
        SameFileGuardConfig(tick_timeout_seconds=-1)


def test_config_toml_roundtrip() -> None:
    config = make_config()
    toml_text = config.as_toml()
    assert "[same_file_guard]" in toml_text
    assert 'ignored_paths = ["scripts/green.sh"]' in toml_text
    assert "auto_block = true" in toml_text


# --- CLI helper shapes ----------------------------------------------------------------


def test_block_and_comment_command_shapes() -> None:
    assert build_block_command("hermes", "hkrc", "t_f", "r") == [
        "hermes", "kanban", "--board", "hkrc", "block", "t_f", "--reason", "r",
    ]
    assert build_comment_command("hermes", "hkrc", "t_a", "b") == [
        "hermes", "kanban", "--board", "hkrc", "comment", "t_a", "--body", "b",
    ]


def test_default_state_path_follows_state_db(tmp_path: Path) -> None:
    assert default_state_path(tmp_path / "state.sqlite3") == (
        tmp_path / "same-file-guard-state.json"
    )


def test_discover_list_show_parse(tmp_path: Path) -> None:
    runner, _ = make_runner(
        boards=[
            {"slug": "hkrc", "archived": False},
            {"slug": "old", "archived": True},
            {"slug": "", "archived": False},
        ],
        tasks={"hkrc": [list_task("t_a")]},
        shows={"t_a": show_doc("t_a")},
    )
    boards = discover_boards("hermes", runner=runner)
    assert [b.slug for b in boards] == ["hkrc"]
    assert list_tasks("hermes", "hkrc", runner=runner)[0]["id"] == "t_a"
    assert show_task("hermes", "hkrc", "t_a", runner=runner)["task"]["id"] == "t_a"
