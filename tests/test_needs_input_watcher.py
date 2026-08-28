from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from hkrc.config import ConfigError, ControllerConfig, NeedsInputWatcherConfig, load_config
from hkrc.handoff import NativeResult
from hkrc.needs_input_watcher import (
    DEFAULT_PROMPT_TEMPLATE,
    NeedsInputWatcherError,
    BlockedEpisode,
    _LLM_OUTPUT_CHROME_MARKERS,
    _contains_cli_chrome,
    build_llm_command,
    build_llm_environment,
    build_prompt,
    default_state_path,
    discover_needs_input,
    fallback_line,
    format_message,
    migrate_state_file,
    render_fresh,
    run,
    run_llm,
)

NOW = 100_000


def blocked_payload(kind: str = "needs_input", reason: str = "waiting on Andre") -> str:
    return json.dumps({"reason": reason, "kind": kind})


def make_board(
    root: Path,
    slug: str,
    tasks: list[dict[str, Any]],
    *,
    events: dict[str, list[tuple[str, int, str | None]]] | None = None,
) -> Path:
    """Build a native board with ``tasks`` and per-task ``(kind, created_at, payload)`` events.

    Event ids are assigned in the order the per-task event lists are iterated,
    so tests control both ``created_at`` and insertion order for tie-breakers.
    """
    board = root / slug
    board.mkdir(parents=True, exist_ok=True)
    (board / "board.json").write_text(json.dumps({"slug": slug}), encoding="utf-8")
    for stale in board.glob("kanban.db*"):
        stale.unlink()
    connection = sqlite3.connect(board / "kanban.db")
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            status TEXT NOT NULL,
            block_kind TEXT
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            run_id INTEGER,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at INTEGER NOT NULL
        );
        """
    )
    for task in tasks:
        connection.execute(
            "INSERT INTO tasks(id, title, status, block_kind) VALUES (?, ?, ?, ?)",
            (
                task["id"],
                task.get("title", task["id"]),
                task["status"],
                task.get("block_kind"),
            ),
        )
    event_id = 1
    for task_id, event_list in (events or {}).items():
        for kind, created_at, payload in event_list:
            connection.execute(
                "INSERT INTO task_events(id, task_id, kind, payload, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, task_id, kind, payload, created_at),
            )
            event_id += 1
    connection.commit()
    connection.close()
    return board


def make_config(
    *,
    enabled: bool = True,
    min_block_seconds: int = 300,
    llm_profile: str = "",
    prompt_template_path: str | None = None,
    timeout_seconds: int = 90,
) -> ControllerConfig:
    return ControllerConfig(
        "test",
        Path("/tmp/nonexistent-boards"),
        Path("/tmp/nonexistent-state.sqlite3"),
        needs_input_watcher=NeedsInputWatcherConfig(
            enabled=enabled,
            min_block_seconds=min_block_seconds,
            llm_profile=llm_profile,
            prompt_template_path=prompt_template_path,
            timeout_seconds=timeout_seconds,
        ),
    )


def blocked_task(
    task_id: str = "t_1",
    kind: str | None = "needs_input",
    reason: str = "waiting on Andre",
    status: str = "blocked",
) -> dict[str, Any]:
    return {
        "id": task_id,
        "title": f"task: {task_id}",
        "status": status,
        "block_kind": kind,
    }


# --- discovery: latest active episode + age ---------------------------------


def test_discovers_active_needs_input_episode_with_age(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    episodes = discover_needs_input(root, now=NOW)
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.board_slug == "campcli"
    assert episode.task_id == "t_1"
    assert episode.block_kind == "needs_input"
    assert episode.reason == "waiting on Andre"
    assert episode.blocked_event_id == 1
    assert episode.blocked_at == NOW - 600
    assert episode.key == "campcli:t_1:1"


def test_latest_unblocked_transition_excludes_stale_blocked_row(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task(status="blocked")],
        events={
            "t_1": [
                ("blocked", NOW - 1000, blocked_payload()),
                ("unblocked", NOW - 500, None),
            ]
        },
    )
    assert discover_needs_input(root, now=NOW) == []


def test_review_gate_reason_episodes_are_skipped(tmp_path: Path) -> None:
    # Operator verdict 2026-08-09: workers author needs_input blocks for
    # review gates too (reason starts with 'review-required'), and those
    # gates are owned by the reviewer profile — pinging the human for them
    # is noise. Mixed gates under the same prefix are skipped as well
    # (documented trade-off, e.g. t_0ae4861f).
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [
            blocked_task("t_review", reason="review-required: DEF-001 fixed, re-review then merge"),
            blocked_task("t_mixed", reason="review-required: committed; operator decisions pending"),
            blocked_task("t_human", reason="waiting on Andre's decision"),
        ],
        events={
            "t_review": [("blocked", NOW - 600, blocked_payload(reason="review-required: DEF-001 fixed, re-review then merge"))],
            "t_mixed": [("blocked", NOW - 600, blocked_payload(reason="review-required: committed; operator decisions pending"))],
            "t_human": [("blocked", NOW - 600, blocked_payload(reason="waiting on Andre's decision"))],
        },
    )
    episodes = discover_needs_input(root, now=NOW)
    assert [episode.task_id for episode in episodes] == ["t_human"]
    # A non-prefixed reason that merely mentions review still pings: the
    # skip is keyed on the reason *prefix*, not the word.
    make_board(
        root,
        "campcli",
        [blocked_task("t_mention")],
        events={"t_mention": [("blocked", NOW - 600, blocked_payload(reason="needs review by Andre"))]},
    )
    episodes = discover_needs_input(root, now=NOW)
    assert [episode.task_id for episode in episodes] == ["t_mention"]


def test_event_id_is_insertion_order_tie_breaker(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    # Same created_at for all three transitions: the highest event id wins.
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 600, blocked_payload()),
                ("unblocked", NOW - 600, None),
                ("blocked", NOW - 600, blocked_payload()),
            ]
        },
    )
    episodes = discover_needs_input(root, now=NOW)
    assert len(episodes) == 1
    assert episodes[0].blocked_event_id == 3
    # A later unblocked event with an equal timestamp still wins by id.
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 600, blocked_payload()),
                ("blocked", NOW - 600, blocked_payload()),
                ("unblocked", NOW - 600, None),
            ]
        },
    )
    assert discover_needs_input(root, now=NOW) == []


def test_re_block_after_unblock_is_a_new_episode(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 1000, blocked_payload()),
                ("unblocked", NOW - 800, None),
                ("blocked", NOW - 600, blocked_payload()),
            ]
        },
    )
    episodes = discover_needs_input(root, now=NOW)
    assert len(episodes) == 1
    assert episodes[0].blocked_event_id == 3
    assert episodes[0].blocked_at == NOW - 600


def test_comment_and_heartbeat_events_do_not_change_episode(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 1000, blocked_payload()),
                ("commented", NOW - 900, json.dumps({"author": "reviewer"})),
                ("heartbeat", NOW - 800, None),
                ("commented", NOW - 700, json.dumps({"author": "main"})),
            ]
        },
    )
    episodes = discover_needs_input(root, now=NOW)
    assert len(episodes) == 1
    assert episodes[0].blocked_event_id == 1
    assert episodes[0].blocked_at == NOW - 1000


# --- discovery: min duration and typing --------------------------------------


def test_min_duration_gate(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task("t_young"), blocked_task("t_old")],
        events={
            "t_young": [("blocked", NOW - 100, blocked_payload())],
            "t_old": [("blocked", NOW - 400, blocked_payload())],
        },
    )
    episodes = discover_needs_input(root, now=NOW, min_block_seconds=300)
    assert [episode.task_id for episode in episodes] == ["t_old"]
    # Boundary: exactly at the minimum is eligible, on a separate board.
    make_board(
        root,
        "other",
        [blocked_task("t_boundary")],
        events={"t_boundary": [("blocked", NOW - 300, blocked_payload())]},
    )
    assert [episode.task_id for episode in discover_needs_input(root, now=NOW)] == [
        "t_old",
        "t_boundary",
    ]


def test_gave_up_and_untyped_rows_are_excluded(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [
            blocked_task("t_gave_up", kind="gave_up"),
            blocked_task("t_untyped", kind=None),
            blocked_task("t_capability", kind="capability"),
            blocked_task("t_bad_payload"),
            blocked_task("t_needs_input"),
        ],
        events={
            "t_gave_up": [("blocked", NOW - 600, blocked_payload("gave_up"))],
            "t_untyped": [("blocked", NOW - 600, None)],
            "t_capability": [("blocked", NOW - 600, blocked_payload("capability"))],
            "t_bad_payload": [("blocked", NOW - 600, "{not json")],
            "t_needs_input": [("blocked", NOW - 600, blocked_payload())],
        },
    )
    episodes = discover_needs_input(root, now=NOW)
    assert [episode.task_id for episode in episodes] == ["t_needs_input"]


# --- dedupe and rendering ----------------------------------------------------


def test_render_fresh_dedupes_by_episode_key(tmp_path: Path) -> None:
    first = BlockedEpisode("campcli", "t_1", "title", "needs_input", "why", 7, NOW - 600)
    second = BlockedEpisode("campcli", "t_1", "title", "needs_input", "why", 9, NOW - 300)
    fresh, state = render_fresh([first], {})
    assert [episode.key for episode in fresh] == ["campcli:t_1:7"]
    assert state == {"campcli:t_1:7": 7}
    # A comment (same episode) never re-triggers; a new episode does.
    fresh2, state2 = render_fresh([first, second], state)
    assert [episode.key for episode in fresh2] == ["campcli:t_1:9"]
    assert state2["campcli:t_1:9"] == 9


def test_format_message_empty_when_nothing_fresh() -> None:
    assert format_message([], {}, now=NOW) == ""
    episode = BlockedEpisode("campcli", "t_1", "title", "needs_input", "why", 7, NOW - 600)
    assert format_message([episode], {}, now=NOW) != ""


def test_format_message_renders_valid_llm_output() -> None:
    episode = BlockedEpisode("campcli", "t_1", "title", "needs_input", "why", 7, NOW - 600)
    message = format_message(
        [episode], {episode.key: "1. Reply in the comment.\n2. Unblock the card.\n3. Done."}, now=NOW
    )
    assert "campcli" in message and "t_1" in message
    assert "why" in message
    assert "1. Reply in the comment." in message
    assert "blocked for 600s" in message


def test_fallback_line_is_deterministic_one_liner() -> None:
    episode = BlockedEpisode("campcli", "t_1", "title", "needs_input", "why", 7, NOW - 600)
    line = fallback_line(episode, NOW)
    assert line.count("\n") == 0
    for fragment in ("board=campcli", "task=t_1", "kind=needs_input", "age=600", "episode=campcli:t_1:7"):
        assert fragment in line


# --- prompt / argv / env / timeout / fallback --------------------------------


def test_build_prompt_default_and_template(tmp_path: Path) -> None:
    assert build_prompt(DEFAULT_PROMPT_TEMPLATE, "t_abc", "campcli") == "t_abc"
    template = tmp_path / "prompt.txt"
    template.write_text(
        "Summarize kanban task {task_id} on board {board_slug} and suggest next steps.",
        encoding="utf-8",
    )
    rendered = build_prompt(template.read_text(encoding="utf-8"), "t_abc", "campcli")
    assert rendered == "Summarize kanban task t_abc on board campcli and suggest next steps."
    with pytest.raises(NeedsInputWatcherError):
        build_prompt("broken {task_id", "t_abc", "campcli")


def test_repo_prompt_template_targets_the_holding_board() -> None:
    # DEF: the summarizer must look the task up on the board that actually
    # holds the blocked episode, never the current default board.  The
    # versioned template must carry the --board {board_slug} flag and must
    # not fall back to hardcoded board variants.
    repo_root = Path(__file__).resolve().parents[1]
    template = (repo_root / "config" / "hkrc" / "needs-input-watcher-prompt.txt").read_text(
        encoding="utf-8"
    )
    assert "hermes kanban --board {board_slug} show {task_id} --json" in template
    rendered = build_prompt(template, "t_69779ab3", "butzenlake-pass")
    assert "hermes kanban --board butzenlake-pass show t_69779ab3 --json" in rendered
    assert "variants" not in template
    assert "--board campcli" not in template


def test_build_llm_command_argv_shape() -> None:
    config = make_config(llm_profile="summarizer")
    command = build_llm_command(config, "t_abc")
    assert command == [
        "hermes",
        "-p",
        "summarizer",
        "chat",
        "-q",
        "t_abc",
        "--max-turns",
        "4",
        "--yolo",
        "-Q",
        "--reasoning",
        "none",
    ]
    with pytest.raises(NeedsInputWatcherError):
        build_llm_command(make_config(), "t_abc")


def test_build_llm_command_is_exact_four_turn_contract() -> None:
    # The summarizer invocation is a fixed product contract: max-turns 4,
    # yolo, -Q quiet mode, and --reasoning none.  The four-turn budget gives
    # the tool-using summarizer room to complete its one kanban-show lookup
    # without exhausting the run; the reasoning suppression keeps the
    # reasoning panel off stdout (quiet mode alone does not).
    command = build_llm_command(make_config(llm_profile="developer"), "t_00844f60")
    assert command[:5] == ["hermes", "-p", "developer", "chat", "-q"]
    assert command[-6:] == ["--max-turns", "4", "--yolo", "-Q", "--reasoning", "none"]
    assert command.count("4") == 1  # the fixed four-turn budget
    assert command[-2:] == ["--reasoning", "none"]


def test_build_llm_environment_pins_home_and_drops_gateway() -> None:
    env = build_llm_environment(
        {
            "HOME": "/home/operator",
            "_HERMES_GATEWAY": "telegram:secret",
            "PATH": "/usr/bin",
        }
    )
    assert env["HOME"] == "/home/operator"
    assert env["HERMES_HOME"] == "/home/operator/.hermes"
    assert "_HERMES_GATEWAY" not in env
    assert env["PATH"] == "/usr/bin"
    # HERMES_HOME absent from the base environment gets the default.
    env2 = build_llm_environment({"HOME": "/home/operator"})
    assert env2["HERMES_HOME"] == "/home/operator/.hermes"
    # An explicit HERMES_HOME is preserved.
    env3 = build_llm_environment({"HOME": "/home/operator", "HERMES_HOME": "/srv/hermes"})
    assert env3["HERMES_HOME"] == "/srv/hermes"


def test_build_llm_environment_scrubs_all_kanban_vars() -> None:
    # The kanban dispatcher exports HERMES_KANBAN_* into worker env; none may
    # reach the nested summarizer (goal-loop boot / parent-card mutation).
    base = {
        "HOME": "/home/operator",
        "HERMES_KANBAN_TASK": "t_parent",
        "HERMES_KANBAN_RUN_ID": "42",
        "HERMES_KANBAN_CLAIM_LOCK": "host:pid",
        "HERMES_KANBAN_DB": "/tmp/kanban.db",
        "HERMES_KANBAN_BOARD": "hkrc",
        "HERMES_KANBAN_GOAL_MODE": "1",
        "HERMES_KANBAN_WORKSPACE": "/tmp/ws",
        "_HERMES_GATEWAY": "telegram:secret",
        "PATH": "/usr/bin",
    }
    env = build_llm_environment(base)
    assert "HERMES_KANBAN_TASK" not in env
    assert "HERMES_KANBAN_RUN_ID" not in env
    assert "HERMES_KANBAN_CLAIM_LOCK" not in env
    assert "HERMES_KANBAN_DB" not in env
    assert "HERMES_KANBAN_BOARD" not in env
    assert "HERMES_KANBAN_GOAL_MODE" not in env
    assert "HERMES_KANBAN_WORKSPACE" not in env
    assert "_HERMES_GATEWAY" not in env
    assert not any(key.startswith("HERMES_KANBAN_") for key in env)
    assert env["PATH"] == "/usr/bin"


def test_run_llm_timeout_nonzero_empty_all_fallback(tmp_path: Path) -> None:
    config = make_config(llm_profile="summarizer")
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    expected = fallback_line(
        BlockedEpisode(
            "campcli", "t_1", "task: t_1", "needs_input", "waiting on Andre", 1, NOW - 600
        ),
        NOW,
    )

    def timeout_runner(_command, _env, _timeout) -> NativeResult:
        return NativeResult(124, "partial output", "timed out")

    message = run(root, tmp_path / "state-timeout.json", config, now=NOW, runner=timeout_runner)
    assert message == expected
    assert "partial output" not in message  # never emit partial output

    def nonzero_runner(_command, _env, _timeout) -> NativeResult:
        return NativeResult(1, "", "boom")

    assert run(root, tmp_path / "state-nonzero.json", config, now=NOW, runner=nonzero_runner).startswith(
        "needs-input-watcher fallback"
    )

    def empty_runner(_command, _env, _timeout) -> NativeResult:
        return NativeResult(0, "   ", "")

    assert run(root, tmp_path / "state-empty.json", config, now=NOW, runner=empty_runner).startswith(
        "needs-input-watcher fallback"
    )


def test_run_llm_rejects_invalid_non_empty_output() -> None:
    # Non-empty but degenerate output ('garbage') is not a summary: fall back.
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, "garbage", ""),
    )
    assert result is None


def test_run_llm_rejects_bare_single_token_output() -> None:
    # An echoed task id is a bare token, never a usable summary.
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, "t_1", ""),
    )
    assert result is None


def test_run_llm_accepts_multi_word_output() -> None:
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, "1. Reply in the comment.\n2. Unblock the card.", ""),
    )
    assert result == "1. Reply in the comment.\n2. Unblock the card."


def test_run_llm_accepts_legit_glyphs_and_reasoning_word() -> None:
    # The chrome marker set must not false-positive on legitimate summaries:
    # status glyphs (✓, ℹ) and the word 'Reasoning' can appear in real text
    # and must still pass validation, or a good summary degrades to the
    # one-line fallback.
    summary = (
        "Summary: the task is blocked. ✓ The reviewer's reasoning is on file.\n"
        "Suggested next steps:\n"
        "- ℹ Re-review and merge.\n"
        "- Unblock the card."
    )
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, summary, ""),
    )
    assert result == summary


def test_run_llm_rejects_max_iterations_chrome() -> None:
    # The observed leak (2026-08-09, campcli t_806bff70): the CLI's
    # max-iterations notice and the tool-failure trace landed on stdout
    # despite -Q and were accepted by the old shape-only validation.  The
    # chrome markers must reject the whole class, not just the notice.
    leak = (
        "Command Hermes isn't found. Check the spelling or the PATH.\n"
        "$ hermes kanban --board campcli show t_806bff70 --json\n"
        "bash: hermes: command not found\n"
        "⚠️  Reached maximum iterations (2). Requesting summary...\n"
        "Summary: the task is blocked"
    )
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, leak, ""),
    )
    assert result is None


def test_run_llm_rejects_reasoning_panel_chrome() -> None:
    # Quiet mode (-Q) does not suppress the reasoning panel: verified
    # empirically that a tool-using summarizer run prints the boxed reasoning
    # to stdout even with -Q.  The box-drawing markers catch it.
    reasoning_panel = (
        "┌─ Reasoning ────────────────────────────────┐\n"
        "The user is asking me to run a command.\n"
        "└────────────────────────────────────────────┘\n"
        "Summary: blocked task waiting on Andre."
    )
    assert _contains_cli_chrome(reasoning_panel)
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, reasoning_panel, ""),
    )
    assert result is None


def test_run_llm_rejects_session_info_chrome() -> None:
    # Session info normally goes to stderr, but a CLI regression that emits it
    # on stdout must fall back rather than leak into the ping.
    output = "Summary: blocked task.\nsession_id: 20260809_225145_aaaa0001"
    assert _contains_cli_chrome(output)
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, output, ""),
    )
    assert result is None


def test_run_llm_chrome_markers_are_non_empty() -> None:
    # The marker set must never silently shrink to nothing.
    assert _LLM_OUTPUT_CHROME_MARKERS
    assert all(isinstance(marker, str) and marker for marker in _LLM_OUTPUT_CHROME_MARKERS)


def test_run_with_chrome_laden_llm_output_emits_fallback(tmp_path: Path) -> None:
    config = make_config(llm_profile="summarizer")
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task("t_806bff70")],
        events={"t_806bff70": [("blocked", NOW - 600, blocked_payload())]},
    )
    # The near-exact delivered leak of 2026-08-09 (cron output 22:26, campcli
    # t_806bff70): boxed reasoning panel + duplicated plain reasoning copy +
    # the CLI max-iterations notice, followed by the real summary.
    leak = (
        "┌─ Reasoning ──────────────────────────────────────────────────────────────────┐\n"
        "The command `Hermes` isn't found. Let me check for the correct binary name — likely\n"
        " `hermes` lowercase. Let me try that.The command `Hermes` isn't found. Let me check for the correct binary name — likely `hermes` lowercase. Let me try that.\n"
        "⚠️  Reached maximum iterations (2). Requesting summary...\n"
        "Summary: This frontend-dev task was to fix two MEDIUM defects (DEF-001, DEF-002)."
    )
    message = run(
        root,
        tmp_path / "state-chrome.json",
        config,
        now=NOW,
        runner=lambda *_: NativeResult(0, leak, ""),
    )
    assert message.startswith("needs-input-watcher fallback")
    assert "Reached maximum iterations" not in message
    assert "Hermes isn't found" not in message
    assert "Reasoning" not in message
    assert "Summary and suggested next steps" not in message


def test_invalid_llm_output_renders_fallback_not_garbage() -> None:
    # Reviewer reproduction: garbage stdout must never reach the success path.
    episode = BlockedEpisode(
        "campcli", "t_1", "title", "needs_input", "waiting on Andre", 7, 100
    )
    result = run_llm(
        ["fake"],
        {"HOME": "/tmp", "HERMES_HOME": "/tmp/.hermes"},
        90,
        runner=lambda *_: NativeResult(0, "garbage", ""),
    )
    message = format_message([episode], {episode.key: result}, now=700)
    assert message.startswith("needs-input-watcher fallback")
    assert "garbage" not in message
    assert "Summary and suggested next steps" not in message


def test_run_with_invalid_llm_output_emits_fallback(tmp_path: Path) -> None:
    config = make_config(llm_profile="summarizer")
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    message = run(
        root,
        tmp_path / "state-invalid.json",
        config,
        now=NOW,
        runner=lambda *_: NativeResult(0, "garbage", ""),
    )
    assert message.startswith("needs-input-watcher fallback")
    assert "garbage" not in message
    assert "Summary and suggested next steps" not in message


def test_run_invokes_summarizer_exactly_once_with_expected_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(llm_profile="summarizer", timeout_seconds=42)
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    state_path = tmp_path / "state" / "needs-input-watcher.json"
    calls: list[tuple[list[str], dict[str, str], int]] = []

    def capture(_command, _env, _timeout) -> NativeResult:
        calls.append((list(_command), dict(_env), int(_timeout)))
        return NativeResult(0, "suggested steps")

    # Simulate the kanban worker environment the cron runs inside: the child
    # env must never carry the dispatcher's kanban pins into the summarizer.
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_parent")
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", "99")
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", "host:pid")
    monkeypatch.setenv("HERMES_KANBAN_DB", "/tmp/kanban.db")
    message = run(root, state_path, config, now=NOW, runner=capture)
    assert len(calls) == 1
    command, env, timeout = calls[0]
    assert command == build_llm_command(config, "t_1")
    assert env["HOME"] and env["HERMES_HOME"]
    assert "_HERMES_GATEWAY" not in env
    assert not any(key.startswith("HERMES_KANBAN_") for key in env)
    assert timeout == 42
    assert "suggested steps" in message
    # The episode is consumed: a second run stays silent without another call.
    calls.clear()
    assert run(root, state_path, config, now=NOW, runner=capture) == ""
    assert calls == []


def test_run_renders_episode_board_slug_into_prompt(tmp_path: Path) -> None:
    # DEF: the prompt handed to the summarizer must carry the board that
    # actually holds the episode, so `kanban show` searches the right board.
    root = tmp_path / "boards"
    make_board(
        root,
        "butzenlake-pass",
        [blocked_task("t_69779ab3")],
        events={"t_69779ab3": [("blocked", NOW - 600, blocked_payload())]},
    )
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text(
        "run: hermes kanban --board {board_slug} show {task_id} --json",
        encoding="utf-8",
    )
    config = make_config(
        llm_profile="summarizer", prompt_template_path=str(prompt_file)
    )
    state_path = tmp_path / "state.json"
    calls: list[list[str]] = []

    def capture(command, _env, _timeout) -> NativeResult:
        calls.append(list(command))
        return NativeResult(0, "inspect butzenlake-pass task t_69779ab3")

    message = run(root, state_path, config, now=NOW, runner=capture)
    assert len(calls) == 1
    prompt = calls[0][calls[0].index("-q") + 1]
    assert "hermes kanban --board butzenlake-pass show t_69779ab3 --json" in prompt
    assert "butzenlake-pass" in message


def test_run_spawns_real_subprocess_with_exact_argv_and_scrubbed_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import stat as _stat

    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    log_path = tmp_path / "child.log"
    fake_cli = tmp_path / "fake-hermes"
    fake_cli.write_text(
        "#!/bin/sh\n"
        f"printf 'argv=%s\\n' \"$*\" >> {log_path}\n"
        f"printf 'HOME=%s\\n' \"$HOME\" >> {log_path}\n"
        f"printf 'HERMES_HOME=%s\\n' \"$HERMES_HOME\" >> {log_path}\n"
        "if env | grep -q '^HERMES_KANBAN_'; then printf 'KANBAN_ENV_PRESENT=1\\n' >> "
        f"{log_path}; else printf 'KANBAN_ENV_PRESENT=0\\n' >> {log_path}; fi\n"
        f"printf 'GATEWAY_PRESENT=%s\\n' \"${{_HERMES_GATEWAY:+1}}\" >> {log_path}\n"
        "printf 'valid suggested steps\\n'\n",
        encoding="utf-8",
    )
    fake_cli.chmod(fake_cli.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)
    config = ControllerConfig(
        "test",
        root,
        tmp_path / "state.sqlite3",
        native_cli=str(fake_cli),
        needs_input_watcher=NeedsInputWatcherConfig(
            enabled=True,
            min_block_seconds=300,
            llm_profile="summarizer",
            prompt_template_path=None,
            timeout_seconds=90,
        ),
    )
    monkeypatch.setenv("HOME", "/home/worker")
    monkeypatch.setenv("HERMES_HOME", "/home/worker/.hermes")
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_parent")
    monkeypatch.setenv("_HERMES_GATEWAY", "telegram:secret")

    message = run(root, tmp_path / "state.json", config, now=NOW)
    assert "valid suggested steps" in message
    child = dict(
        line.split("=", 1) for line in log_path.read_text(encoding="utf-8").splitlines()
    )
    # The real subprocess receives the exact canonical argv, pinned homes,
    # and no kanban/gateway environment from the dispatcher.
    assert child["argv"] == "-p summarizer chat -q t_1 --max-turns 4 --yolo -Q --reasoning none"
    assert child["HOME"] == "/home/worker"
    assert child["HERMES_HOME"] == "/home/worker/.hermes"
    assert child["KANBAN_ENV_PRESENT"] == "0"
    assert child["GATEWAY_PRESENT"] == ""


def test_missing_prompt_template_fails_closed(tmp_path: Path) -> None:
    config = make_config(
        llm_profile="summarizer",
        prompt_template_path=str(tmp_path / "missing" / "prompt.txt"),
    )
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    with pytest.raises(NeedsInputWatcherError):
        run(root, tmp_path / "state.json", config, now=NOW)


# --- run-level behavior ------------------------------------------------------


def test_run_is_silent_when_nothing_new(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "campcli", [])
    state_path = tmp_path / "state" / "needs-input-watcher.json"
    message = run(root, state_path, make_config(), now=NOW)
    assert message == ""
    assert state_path.is_file()  # state persisted even when silent


def test_run_emits_once_then_silent_without_llm_profile(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    state_path = tmp_path / "state" / "needs-input-watcher.json"
    first = run(root, state_path, make_config(), now=NOW)
    assert first.startswith("needs-input-watcher fallback")
    assert "campcli" in first and "t_1" in first and "age=600" in first
    second = run(root, state_path, make_config(), now=NOW)
    assert second == ""


def test_run_disabled_is_silent_and_never_writes_state(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    state_path = tmp_path / "state" / "needs-input-watcher.json"
    assert run(root, state_path, make_config(enabled=False), now=NOW) == ""
    assert not state_path.exists()


def test_run_new_episode_retriggers_after_unblock_reblock(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 1000, blocked_payload())]},
    )
    state_path = tmp_path / "state" / "needs-input-watcher.json"
    first = run(root, state_path, make_config(), now=NOW)
    assert "episode=campcli:t_1:1" in first
    # Comment + heartbeat on the same episode: still silent.
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 1000, blocked_payload()),
                ("commented", NOW - 900, json.dumps({"author": "reviewer"})),
                ("heartbeat", NOW - 800, None),
            ]
        },
    )
    assert run(root, state_path, make_config(), now=NOW) == ""
    # A genuinely new blocked episode (new event id) pings again.
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={
            "t_1": [
                ("blocked", NOW - 1000, blocked_payload()),
                ("unblocked", NOW - 900, None),
                ("blocked", NOW - 800, blocked_payload()),
            ]
        },
    )
    second = run(root, state_path, make_config(), now=NOW)
    assert "episode=campcli:t_1:3" in second
    assert "episode=campcli:t_1:1" not in second


def test_default_state_path_sits_beside_state_db(tmp_path: Path) -> None:
    state_db = tmp_path / "state" / "hkrc" / "state.sqlite3"
    assert default_state_path(state_db) == tmp_path / "state" / "hkrc" / "needs-input-watcher-state.json"


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(root, "campcli", [])
    state_path = tmp_path / "needs-input-watcher.json"
    state_path.write_text("{not json", encoding="utf-8")
    with pytest.raises(NeedsInputWatcherError):
        run(root, state_path, make_config(), now=NOW)


# --- config: load / serialize / validation -----------------------------------


def test_config_serializes_needs_input_watcher_defaults() -> None:
    config = make_config()
    text = config.as_toml()
    assert "[needs_input_watcher]" in text
    assert "enabled = true" in text
    assert "min_block_seconds = 300" in text
    assert 'llm_profile = ""' in text
    assert 'prompt_template_path = ""' in text
    assert "timeout_seconds = 90" in text


def test_config_round_trips_through_toml(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        """
[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"

[needs_input_watcher]
enabled = false
min_block_seconds = 1200
llm_profile = "summarizer"
prompt_template_path = "/tmp/instance/prompt.txt"
timeout_seconds = 45
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.needs_input_watcher.enabled is False
    assert config.needs_input_watcher.min_block_seconds == 1200
    assert config.needs_input_watcher.llm_profile == "summarizer"
    assert config.needs_input_watcher.prompt_template_path == "/tmp/instance/prompt.txt"
    assert config.needs_input_watcher.timeout_seconds == 45


def test_config_load_defaults_when_section_absent(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        """
[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.needs_input_watcher == NeedsInputWatcherConfig()


def test_config_validation_rejects_bad_values() -> None:
    for kwargs in (
        {"enabled": "yes"},
        {"min_block_seconds": 0},
        {"min_block_seconds": -5},
        {"llm_profile": None},
        {"prompt_template_path": "   "},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
    ):
        with pytest.raises(ConfigError):
            NeedsInputWatcherConfig(**kwargs)


def test_config_rejects_boolean_durations() -> None:
    # bool is not a number/duration: True must not be accepted as an int.
    for field in ("min_block_seconds", "timeout_seconds"):
        with pytest.raises(ConfigError, match=field):
            NeedsInputWatcherConfig(**{field: True})
        with pytest.raises(ConfigError, match=field):
            NeedsInputWatcherConfig(**{field: False})


def test_config_accepts_int_and_float_durations() -> None:
    for field in ("min_block_seconds", "timeout_seconds"):
        assert getattr(NeedsInputWatcherConfig(**{field: 300}), field) == 300
        assert getattr(NeedsInputWatcherConfig(**{field: 300.0}), field) == 300.0
        assert getattr(NeedsInputWatcherConfig(**{field: 90.5}), field) == 90.5


def test_config_load_rejects_boolean_durations_in_toml(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        """
[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"

[needs_input_watcher]
min_block_seconds = true
timeout_seconds = true
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="min_block_seconds"):
        load_config(path)


def test_config_load_rejects_non_table_needs_input_watcher(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        """
needs_input_watcher = ["not", "a", "table"]

[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(path)


def test_run_uses_configured_min_duration(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 200, blocked_payload())]},
    )
    state_path = tmp_path / "state.json"
    # Below the configured minimum: silent.
    assert run(root, state_path, make_config(min_block_seconds=300), now=NOW) == ""
    # Above the configured minimum: pings.
    assert run(root, state_path, make_config(min_block_seconds=60), now=NOW) != ""


# --- rename: CLI subcommand + alias, legacy config section, state migration ---


def test_cli_needs_input_watcher_subcommand_and_blocker_ping_alias() -> None:
    # The rename keeps `blocker-ping` as a hidden argparse alias that resolves
    # to the exact same handler (same config, same state file, same behavior).
    from hkrc.cli import build_parser

    parser = build_parser()
    canonical = parser.parse_args(["needs-input-watcher", "--config", "/tmp/c.toml"])
    alias = parser.parse_args(["blocker-ping", "--config", "/tmp/c.toml"])
    assert canonical.handler is alias.handler
    assert canonical.handler.__name__ == "_needs_input_watcher"
    # Both spellings resolve the same default state filename.
    assert canonical.state_file is None and alias.state_file is None


def test_cli_init_accepts_needs_input_watcher_flags(tmp_path: Path) -> None:
    from hkrc.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(
        [
            "init",
            "--config",
            str(tmp_path / "config.toml"),
            "--needs-input-watcher-min-block-seconds",
            "120",
        ]
    )
    assert args.needs_input_watcher_min_block_seconds == 120


def test_config_load_accepts_legacy_blocker_ping_section(tmp_path: Path) -> None:
    # The deployed instance config was written before the rename; the loader
    # must keep reading [blocker_ping] so settings are not silently dropped.
    path = tmp_path / "config.toml"
    path.write_text(
        """
[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"

[blocker_ping]
enabled = false
min_block_seconds = 1200
llm_profile = "summarizer"
prompt_template_path = "/tmp/instance/prompt.txt"
timeout_seconds = 45
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.needs_input_watcher.enabled is False
    assert config.needs_input_watcher.min_block_seconds == 1200
    assert config.needs_input_watcher.llm_profile == "summarizer"
    # The new section wins when both are present.
    path.write_text(
        """
[instance]
name = "test"
native_boards_root = "/tmp/boards"

[controller]
state_db = "/tmp/state.sqlite3"

[needs_input_watcher]
llm_profile = "new-profile"

[blocker_ping]
llm_profile = "old-profile"
""",
        encoding="utf-8",
    )
    assert load_config(path).needs_input_watcher.llm_profile == "new-profile"


def test_migrate_state_file_preserves_dedupe_history(tmp_path: Path) -> None:
    # The rename keeps the legacy blocker-ping state file so the first run
    # after an upgrade does not re-ping every currently blocked card.
    legacy = tmp_path / "state" / "hkrc" / "blocker-ping-state.json"
    new = tmp_path / "state" / "hkrc" / "needs-input-watcher-state.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"campcli:t_1:1": 1}\n', encoding="utf-8")
    migrate_state_file(new)
    assert new.is_file()
    assert not legacy.exists()
    assert json.loads(new.read_text(encoding="utf-8")) == {"campcli:t_1:1": 1}
    # Idempotent and never touches a custom --state-file name.
    migrate_state_file(new)
    assert new.is_file()
    custom = tmp_path / "custom.json"
    custom.write_text("{}", encoding="utf-8")
    migrate_state_file(custom)
    assert custom.read_text(encoding="utf-8") == "{}"


def test_run_migrates_legacy_state_and_stays_silent(tmp_path: Path) -> None:
    root = tmp_path / "boards"
    make_board(
        root,
        "campcli",
        [blocked_task()],
        events={"t_1": [("blocked", NOW - 600, blocked_payload())]},
    )
    state_dir = tmp_path / "state" / "hkrc"
    state_dir.mkdir(parents=True)
    legacy = state_dir / "blocker-ping-state.json"
    legacy.write_text('{"campcli:t_1:1": 1}\n', encoding="utf-8")
    state_path = state_dir / "needs-input-watcher-state.json"
    # The episode was already pinged under the legacy key: migration must keep
    # it silent instead of re-pinging.
    assert run(root, state_path, make_config(), now=NOW) == ""
    assert state_path.is_file()
    assert not legacy.exists()
