"""Storm-halt breaker (t_df407995): detector SQL, applier allowlist, wiring.

Coverage mandated by the card:
- storm SQL against fixture DBs: storm hit, single crash no-hit,
  pid-noise collapse onto the 60-char prefix, reclaim/spawn_failed/gave_up
  exclusion, profile-lane separation, window boundary;
- halt applier allowlist: blocks ready-only via the ONLY argv it can build,
  refuses any non-block verb by construction, continues past a failed block,
  empty lane is a no-op;
- ready-listing channel: parses ``list --status ready --json``, fails
  closed on non-zero exit / unparseable JSON;
- runtime wiring: cycle calls the storm check before handoff, halts once
  per episode (dedupe), never kills the cycle on detector errors, logs
  ``storm_halt`` with SQL evidence, operator ping text.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from hkrc.config import ControllerConfig
from hkrc.runtime import DaemonRuntime, _default_storm_halt, format_storm_halt_alert
from hkrc.storm_halt import (
    HaltResult,
    SIGNATURE_PREFIX_CHARS,
    STORM_MIN_COUNT,
    STORM_WINDOW_MINUTES,
    StormFinding,
    StormHaltError,
    apply_halt,
    detect_storms,
    getattr_result_code,
    halt_command,
    list_ready_task_ids,
    storm_signature,
)

NOW = 1_788_710_000
MINUTE = 60


def make_runs_db(path: Path, runs: list[dict]) -> Path:
    """Fixture board DB with the native task_runs shape the storm SQL reads."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    for stale in path.parent.glob("kanban.db*"):
        stale.unlink()
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
            created_at INTEGER
        );
        CREATE TABLE task_runs (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            profile TEXT,
            status TEXT NOT NULL,
            started_at INTEGER NOT NULL,
            ended_at INTEGER,
            outcome TEXT,
            error TEXT
        );
        """
    )
    for index, run in enumerate(runs, start=1):
        connection.execute(
            "INSERT INTO task_runs(id, task_id, profile, status, started_at, "
            "ended_at, outcome, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                index,
                run.get("task_id", f"t_{index}"),
                run.get("profile", "developer"),
                run.get("status", "crashed"),
                run["started_at"],
                run.get("ended_at"),
                run.get("outcome", "crashed"),
                run.get("error"),
            ),
        )
    connection.commit()
    connection.close()
    return path


PROTOCOL_ERROR = (
    "worker exited cleanly (rc=0) without calling kanban_complete or "
    "kanban_block — protocol violation.  (long tail that exceeds 60 chars)"
)


def crashed(profile: str, started_at: int, error: str = PROTOCOL_ERROR, task_id: str | None = None) -> dict:
    return {
        "task_id": task_id or f"t_{profile}_{started_at}",
        "profile": profile,
        "status": "crashed",
        "started_at": started_at,
        "outcome": "crashed",
        "error": error,
    }


# --- detector: the four mandated SQL controls --------------------------------


def test_storm_hit_two_same_signature_within_window(tmp_path: Path) -> None:
    db = make_runs_db(
        tmp_path / "kanban.db",
        [
            crashed("developer", NOW - 5 * MINUTE),
            crashed("developer", NOW - 2 * MINUTE),
        ],
    )
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    findings = detect_storms(connection, "fixture", now=NOW)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.board_slug == "fixture"
    assert finding.profile == "developer"
    assert finding.count == 2
    assert finding.signature == PROTOCOL_ERROR[:SIGNATURE_PREFIX_CHARS]
    assert finding.first_crashed_at == NOW - 5 * MINUTE
    assert finding.last_crashed_at == NOW - 2 * MINUTE
    assert "storm-halt" in finding.reason
    assert "unblock" in finding.reason


def test_single_crash_no_hit(tmp_path: Path) -> None:
    db = make_runs_db(tmp_path / "kanban.db", [crashed("developer", NOW - 5 * MINUTE)])
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    assert detect_storms(connection, "fixture", now=NOW) == ()


def test_pid_noise_cannot_form_or_pollute_a_storm(tmp_path: Path) -> None:
    # pid-noise safety (R3 section 1): pid error strings vary per run and
    # are shorter than the 60-char prefix, so each pid signature stands
    # alone at n=1 — it can never reach the storm threshold, and it never
    # merges into the protocol-violation group (they differ from char 1).
    assert len("pid 1460951 not alive") < SIGNATURE_PREFIX_CHARS
    assert storm_signature("pid 1460951 not alive") != storm_signature(
        "pid 1497671 not alive"
    )
    assert storm_signature(PROTOCOL_ERROR) != storm_signature("pid 1460951 not alive")
    # A lane with 1 protocol crash + 2 distinct pid crashes: no signature
    # reaches 2 from a pid contribution, so NO finding fires.
    db = make_runs_db(
        tmp_path / "kanban.db",
        [
            crashed("developer", NOW - 9 * MINUTE),
            crashed("developer", NOW - 5 * MINUTE, error="pid 1460951 not alive"),
            crashed("developer", NOW - 4 * MINUTE, error="pid 1497671 not alive"),
        ],
    )
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    assert detect_storms(connection, "fixture", now=NOW) == ()
    # Two crashes with the SAME pid-shaped error text (identical strings)
    # are one group and do storm — the grouping is by signature, not by
    # outcome or task.
    db2 = make_runs_db(
        tmp_path / "kanban2.db",
        [
            crashed("developer", NOW - 9 * MINUTE, error="pid 1460951 not alive"),
            crashed("developer", NOW - 4 * MINUTE, error="pid 1460951 not alive"),
        ],
    )
    connection2 = sqlite3.connect(db2)
    connection2.row_factory = sqlite3.Row
    findings = detect_storms(connection2, "fixture", now=NOW)
    assert len(findings) == 1
    assert findings[0].signature == "pid 1460951 not alive"
    assert findings[0].count == 2


def test_reclaim_and_other_outcomes_excluded(tmp_path: Path) -> None:
    db = make_runs_db(
        tmp_path / "kanban.db",
        [
            {"profile": "developer", "status": "reclaimed", "started_at": NOW - 5 * MINUTE,
             "outcome": "reclaimed", "error": "pid 11 not alive"},
            {"profile": "developer", "status": "reclaimed", "started_at": NOW - 4 * MINUTE,
             "outcome": "reclaimed", "error": "pid 12 not alive"},
            {"profile": "developer", "status": "spawn_failed", "started_at": NOW - 3 * MINUTE,
             "outcome": "spawn_failed", "error": "spawn"},
            {"profile": "developer", "status": "spawn_failed", "started_at": NOW - 2 * MINUTE,
             "outcome": "spawn_failed", "error": "spawn"},
            {"profile": "developer", "status": "gave_up", "started_at": NOW - MINUTE,
             "outcome": "gave_up", "error": PROTOCOL_ERROR},
            {"profile": "developer", "status": "gave_up", "started_at": NOW,
             "outcome": "gave_up", "error": PROTOCOL_ERROR},
        ],
    )
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    assert detect_storms(connection, "fixture", now=NOW) == ()


def test_window_boundary_and_profile_lanes_separate(tmp_path: Path) -> None:
    db = make_runs_db(
        tmp_path / "kanban.db",
        [
            # developer: 2 crashes inside the window -> storm
            crashed("developer", NOW - 29 * MINUTE),
            crashed("developer", NOW - MINUTE),
            # reviewer: same signature, 1 crash in window + 1 just outside -> no hit
            crashed("reviewer", NOW - 30 * MINUTE - 1),
            crashed("reviewer", NOW - 5 * MINUTE),
        ],
    )
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    findings = detect_storms(connection, "fixture", now=NOW)
    assert len(findings) == 1
    assert findings[0].profile == "developer"
    # The window predicate is strictly '>' against now - 30m.
    assert findings[0].first_crashed_at == NOW - 29 * MINUTE


def test_threshold_is_exact_count_not_more(tmp_path: Path) -> None:
    assert STORM_MIN_COUNT == 2
    assert STORM_WINDOW_MINUTES == 30
    db = make_runs_db(
        tmp_path / "kanban.db",
        [crashed("developer", NOW - i * MINUTE) for i in range(1, 8)],
    )
    connection = sqlite3.connect(db)
    connection.row_factory = sqlite3.Row
    findings = detect_storms(connection, "fixture", now=NOW)
    assert len(findings) == 1
    assert findings[0].count == 7


# --- halt applier: allowlist --------------------------------------------------


def _finding(board: str = "fixture", profile: str = "developer") -> StormFinding:
    return StormFinding(
        board_slug=board,
        profile=profile,
        signature=PROTOCOL_ERROR[:SIGNATURE_PREFIX_CHARS],
        count=2,
        first_crashed_at=NOW - 600,
        last_crashed_at=NOW - 60,
    )


def test_halt_command_is_block_only_on_board_scoped_cli() -> None:
    command = halt_command("hermes", "dev", "fixture", "t_1", "reason text")
    assert command == [
        "hermes", "--profile", "dev", "kanban", "--board", "fixture",
        "block", "t_1", "reason text",
    ]
    assert halt_command("hermes", None, "fixture", "t_1", "r") == [
        "hermes", "kanban", "--board", "fixture", "block", "t_1", "r",
    ]


def test_apply_halt_blocks_every_ready_card_and_audits_reason(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def runner(command):
        commands.append(list(command))
        return type("R", (), {"returncode": 0})()

    result = apply_halt(_finding(), ["t_a", "t_b"], runner)
    assert result.ok
    assert result.blocked_task_ids == ("t_a", "t_b")
    assert result.failed_task_ids == ()
    assert len(commands) == 2
    for command in commands:
        # argv shape: [cli, kanban, --board, slug, block, task_id, reason]
        assert command[0] == "hermes"
        assert command[1:3] == ["kanban", "--board"]
        assert command[3] == "fixture"
        assert command[4] == "block"
        assert command[5] in {"t_a", "t_b"}
        assert "storm-halt" in command[6]
        assert "unblock" in command[6]


def test_apply_halt_structurally_refuses_non_block_verbs() -> None:
    # The allowlist is the ONLY verb the argv builder can emit: complete,
    # reclaim, edit, unblock are unreachable by construction.
    import hkrc.storm_halt as module

    assert module._ALLOWED_VERBS == frozenset({"block"})
    command = halt_command("hermes", None, "fixture", "t_1", "r")
    verb = command[4]
    assert verb == "block"
    assert verb not in {"complete", "reclaim", "edit", "unblock", "comment", "reassign"}


def test_apply_halt_continues_past_failed_block() -> None:
    def runner(_command):
        return type("R", (), {"returncode": 3})()

    result = apply_halt(_finding(), ["t_a", "t_b"], runner)
    assert not result.ok
    assert result.blocked_task_ids == ()
    assert result.failed_task_ids == ("t_a", "t_b")
    assert "FAILED" not in result.summary_line() or "failed=[t_a,t_b]" in result.summary_line()


def test_apply_halt_empty_lane_is_noop() -> None:
    def runner(_command):  # pragma: no cover - must never run
        raise AssertionError("runner must not be called for an empty lane")

    result = apply_halt(_finding(), [], runner)
    assert result == HaltResult(_finding(), (), ())


def test_list_ready_task_ids_scopes_listing_to_storm_lane() -> None:
    """DEF-t_df407995-1: the listing argv carries the finding's profile."""

    def runner(command):
        assert command == [
            "hermes", "kanban", "--board", "fixture",
            "list", "--status", "ready", "--assignee", "developer", "--json",
        ]
        return type("R", (), {"returncode": 0, "stdout": json.dumps(
            [{"id": "t_ready1"}, {"id": "t_ready2"}]
        ), "stderr": ""})()

    assert list_ready_task_ids(
        "hermes", None, "fixture", profile="developer", runner=runner
    ) == ("t_ready1", "t_ready2")


def test_list_ready_task_ids_fails_closed_without_profile_lane() -> None:
    """DEF-t_df407995-1: profile=None blocks nothing, runner never runs."""

    def runner(_command):  # pragma: no cover - must never run
        raise AssertionError("runner must not be called without a profile lane")

    with pytest.raises(StormHaltError, match="no profile lane"):
        list_ready_task_ids(
            "hermes", None, "fixture", profile=None, runner=runner
        )


def test_list_ready_task_ids_parses_cli_json() -> None:
    def runner(command):
        assert command == [
            "hermes", "kanban", "--board", "fixture",
            "list", "--status", "ready", "--assignee", "developer", "--json",
        ]
        return type("R", (), {"returncode": 0, "stdout": json.dumps(
            [{"id": "t_ready1", "status": "ready"}, {"id": "t_ready2"}, "junk", {}]
        ), "stderr": ""})()

    assert list_ready_task_ids(
        "hermes", None, "fixture", profile="developer", runner=runner
    ) == (
        "t_ready1",
        "t_ready2",
    )


def test_list_ready_task_ids_fails_closed(tmp_path: Path) -> None:
    failing = type("R", (), {"returncode": 2, "stdout": "", "stderr": "boom"})()
    with pytest.raises(StormHaltError, match="ready listing failed"):
        list_ready_task_ids(
            "hermes", None, "fixture", profile="developer",
            runner=lambda _c: failing,
        )
    bad_json = type("R", (), {"returncode": 0, "stdout": "{not json", "stderr": ""})()
    with pytest.raises(StormHaltError, match="unparseable JSON"):
        list_ready_task_ids(
            "hermes", None, "fixture", profile="developer",
            runner=lambda _c: bad_json,
        )
    not_array = type("R", (), {"returncode": 0, "stdout": '{"id": "x"}', "stderr": ""})()
    with pytest.raises(StormHaltError, match="JSON array"):
        list_ready_task_ids(
            "hermes", None, "fixture", profile="developer",
            runner=lambda _c: not_array,
        )


def test_default_result_of_reads_returncode() -> None:
    assert getattr_result_code(type("R", (), {"returncode": 7})()) == 7
    assert getattr_result_code(object()) == 0


# --- runtime wiring -----------------------------------------------------------


def _runtime(tmp_path: Path, **kwargs) -> DaemonRuntime:
    config = ControllerConfig(
        "test", tmp_path / "native-do-not-touch", tmp_path / "state.sqlite3",
        native_cli="fake-hermes", telegram_chat_id="-1000",
    )
    return DaemonRuntime(config, **kwargs)


class _State:
    """Minimal stand-in: _check_storms never touches controller state."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _observerless_cycle(runtime: DaemonRuntime):
    """Run only the storm breaker + logging, skipping the stream observer.

    The tests here target the breaker wiring, not the stream pipeline; the
    daemon requires stream wiring to observe (covered by other suites), so
    these call ``_check_storms`` directly the way ``_run_cycle`` does.
    """

    return runtime._check_storms()


def test_cycle_runs_storm_check_and_halts_once_per_episode(tmp_path: Path) -> None:
    finding = _finding()
    calls: list[int] = []
    halted: list[str] = []

    def storm_check(now: int):
        calls.append(now)
        return (finding,)

    def storm_halt(f: StormFinding):
        halted.append(f.dedupe_key)
        return HaltResult(f, ("t_a", "t_b"), ())

    runtime = _runtime(tmp_path, storm_check=storm_check, storm_halt=storm_halt)
    assert _observerless_cycle(runtime) == 2
    assert _observerless_cycle(runtime) == 0  # deduped second tick
    assert len(calls) == 2  # checked every cycle
    assert halted == [finding.dedupe_key]  # halted ONCE despite two ticks
    assert runtime._storm_episode_keys == {finding.dedupe_key}


def test_cycle_swallows_detector_errors(tmp_path: Path) -> None:
    def storm_check(_now):
        raise RuntimeError("boards root exploded")

    runtime = _runtime(tmp_path, storm_check=storm_check)
    assert _observerless_cycle(runtime) == 0  # logged, cycle survives


def test_storm_halt_error_does_not_stop_later_findings(tmp_path: Path) -> None:
    finding_a = _finding(board="a")
    finding_b = _finding(board="b")
    applied: list[str] = []

    def storm_check(_now):
        return (finding_a, finding_b)

    def storm_halt(f: StormFinding):
        if f.board_slug == "a":
            raise StormHaltError("listing failed")
        applied.append(f.board_slug)
        return HaltResult(f, ("t_x",), ())

    runtime = _runtime(tmp_path, storm_check=storm_check, storm_halt=storm_halt)
    assert runtime._check_storms() == 1
    assert applied == ["b"]
    # The failed halt was NOT marked as halted (it may retry next cycle).
    assert finding_a.dedupe_key not in runtime._storm_episode_keys


def test_operator_ping_text_carries_evidence_and_reversal(tmp_path: Path) -> None:
    finding = _finding()
    result = HaltResult(finding, ("t_a", "t_b"), ("t_c",))
    text = format_storm_halt_alert(finding, result)
    assert "storm halt" in text
    assert "board=fixture" in text
    assert "profile=developer" in text
    assert "count=2" in text
    assert "t_a,t_b" in text
    assert "FAILED blocks: 1" in text
    assert "unblock" in text


# --- DEF-t_df407995-1: the halt target set is the storm lane ------------------


def test_default_storm_halt_blocks_only_finding_profile_cards(tmp_path: Path) -> None:
    """Mixed-assignee ready board: only the finding's lane is blocked.

    Reviewer repro: three lanes ready on one board; the halt must touch
    exactly the developer-lane cards and never the reviewer's or the
    orchestrator's.
    """

    block_commands: list[list[str]] = []
    ready_payload = json.dumps(
        [
            {"id": "t_dev_1", "assignee": "developer"},
            {"id": "t_dev_2", "assignee": "developer"},
            {"id": "t_rev_1", "assignee": "reviewer"},
            {"id": "t_orch_1", "assignee": "lead-orchestrator"},
        ]
    )

    def runner(command):
        if "list" in command:
            assert "--assignee" in command
            requested = command[command.index("--assignee") + 1]
            # Emulate the CLI contract: --assignee filters the ready set.
            scoped = [entry for entry in json.loads(ready_payload) if entry["assignee"] == requested]
            return type("R", (), {"returncode": 0, "stdout": json.dumps(scoped), "stderr": ""})()
        assert command[4] == "block"
        block_commands.append(list(command))
        return type("R", (), {"returncode": 0})()

    config = ControllerConfig(
        "test", tmp_path / "native-do-not-touch", tmp_path / "state.sqlite3",
        native_cli="fake-hermes", telegram_chat_id="-1000",
    )
    result = _default_storm_halt(_finding(), config, runner=runner)  # type: ignore[arg-type]
    assert result.blocked_task_ids == ("t_dev_1", "t_dev_2")
    assert all("t_rev_1" not in command for command in block_commands)
    assert all("t_orch_1" not in command for command in block_commands)
    assert len(block_commands) == 2


def test_default_storm_halt_fail_closed_when_finding_has_no_profile(tmp_path: Path) -> None:
    """A profile-less finding blocks nothing (fail-closed, R3 §3 lane rule)."""

    def runner(_command):  # pragma: no cover - must never run
        raise AssertionError("no listing may be built without a profile lane")

    config = ControllerConfig(
        "test", tmp_path / "native-do-not-touch", tmp_path / "state.sqlite3",
        native_cli="fake-hermes", telegram_chat_id="-1000",
    )
    laneless = StormFinding(
        board_slug="fixture",
        profile=None,
        signature=PROTOCOL_ERROR[:SIGNATURE_PREFIX_CHARS],
        count=2,
        first_crashed_at=NOW - 600,
        last_crashed_at=NOW - 60,
    )
    with pytest.raises(StormHaltError, match="no profile lane"):
        _default_storm_halt(laneless, config, runner=runner)  # type: ignore[arg-type]


# --- DEF-t_df407995-2: episodes end; failed halts do not consume ---------------


def test_episode_rehalts_after_storm_clears(tmp_path: Path) -> None:
    """Ticks 1-3 storm, tick 4 clear, ticks 5-6 same signature again.

    Expected: halt on tick 1 and AGAIN on tick 5 — the cleared episode key
    is reconciled away while the lane is quiet.
    """

    finding = _finding()
    check_calls: list[int] = []

    def storm_check(now: int):
        check_calls.append(now)
        return (finding,) if len(check_calls) not in (4,) else ()

    halt_calls: list[str] = []

    def storm_halt(f: StormFinding):
        halt_calls.append(f.dedupe_key)
        return HaltResult(f, ("t_a",), ())

    runtime = _runtime(tmp_path, storm_check=storm_check, storm_halt=storm_halt)
    counts = [runtime._check_storms() for _ in range(6)]
    assert counts == [1, 0, 0, 0, 1, 0]
    assert halt_calls == [finding.dedupe_key, finding.dedupe_key]


def test_all_blocks_failed_halt_retries_next_tick(tmp_path: Path) -> None:
    """A halt that blocks zero cards must not consume the episode."""

    finding = _finding()
    halt_calls: list[str] = []

    def storm_check(_now):
        return (finding,)

    def storm_halt(f: StormFinding):
        halt_calls.append(f.dedupe_key)
        if len(halt_calls) == 1:
            return HaltResult(f, (), ("t_a",))  # first attempt: every block refused
        return HaltResult(f, ("t_a",), ())  # retry succeeds

    runtime = _runtime(tmp_path, storm_check=storm_check, storm_halt=storm_halt)
    assert runtime._check_storms() == 0  # nothing blocked, nothing marked
    assert finding.dedupe_key not in runtime._storm_episode_keys
    assert runtime._check_storms() == 1  # retried and succeeded next tick
    assert halt_calls == [finding.dedupe_key, finding.dedupe_key]
    assert runtime._storm_episode_keys == {finding.dedupe_key}
