"""Shadow-live harness simulation tests (task t_f5538a0d).

The simulation must reproduce the nightly 2am harness execution as closely
as possible WITHOUT mutating live harness state, the live hkrc kanban
board, the canonical HKRC checkout, or the deployment.  These tests prove
the isolation contract: kanban creates land in an isolated shadow sink
(the same board schema, a separate SQLite file), live state hashes and
board counts are provably unchanged, and the report is labeled SIMULATION.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
import sqlite3
import subprocess
from typing import Any

from hkrc.config import ControllerConfig, WatcherConfig
from hkrc.harness_loop import (
    HarnessLoopConfig,
    ProcessResult,
    ProcessRunner,
)
from hkrc.simulation import (
    SHADOW_TASK_ID_PREFIX,
    SIMULATION_LABEL,
    ShadowSink,
    is_kanban_create_argv,
    run_simulation,
)

NOW = 100_000
DAY = 86_400


def file_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _create_argv(
    *,
    title: str = "fix: hkrc-fix (t_a)",
    assignee: str = "developer",
    body: str = "Nightly harness-loop HKRC proposal.",
    workspace: str = "worktree:/tmp/repo",
    idempotency_key: str = "harness-hkrc-impl:hkrc-fix:t_a",
    parent: str | None = None,
) -> list[str]:
    """Exact argv shape the ticket router passes to ``hermes kanban create``."""
    argv = [
        "hermes",
        "kanban",
        "--board",
        "hkrc",
        "create",
        title,
        "--assignee",
        assignee,
        "--body",
        body,
        "--workspace",
        workspace,
        "--idempotency-key",
        idempotency_key,
        "--json",
    ]
    if parent:
        argv += ["--parent", parent]
    return argv


def test_shadow_sink_captures_impl_and_parent_linked_review(tmp_path: Path) -> None:
    """A kanban create is captured as task + created event; a review create
    with ``--parent`` adds a parent link — all inside the shadow board."""
    sink = ShadowSink(tmp_path / "shadow" / "kanban.db")
    impl = sink.capture(_create_argv(idempotency_key="harness-hkrc-impl:hkrc-fix:t_a"))
    assert impl.returncode == 0
    impl_id = json.loads(impl.stdout)["id"]
    assert impl_id.startswith(SHADOW_TASK_ID_PREFIX)
    review = sink.capture(
        _create_argv(
            title=f"review: hkrc-fix (t_a) ({impl_id})",
            assignee="reviewer",
            idempotency_key="harness-hkrc-review:hkrc-fix:t_a",
            parent=impl_id,
        )
    )
    review_id = json.loads(review.stdout)["id"]
    assert review_id != impl_id
    connection = sqlite3.connect(sink.db_path)
    try:
        tasks = connection.execute(
            "SELECT id, title, assignee, status FROM tasks ORDER BY created_at"
        ).fetchall()
        assert [row[0] for row in tasks] == [impl_id, review_id]
        assert tasks[0][1] == "fix: hkrc-fix (t_a)"
        assert tasks[1][2] == "reviewer"
        assert tasks[1][3] == "todo"
        events = connection.execute(
            "SELECT task_id, kind FROM task_events ORDER BY id"
        ).fetchall()
        assert [(row[0], row[1]) for row in events] == [
            (impl_id, "created"),
            (review_id, "created"),
        ]
        links = connection.execute(
            "SELECT parent_id, child_id FROM task_links"
        ).fetchall()
        assert links == [(impl_id, review_id)]
    finally:
        connection.close()


def test_shadow_sink_retry_reuses_same_card_idempotently(tmp_path: Path) -> None:
    """Same idempotency key -> same task id, no duplicate rows (mirrors the
    real CLI's idempotency contract for router retries)."""
    sink = ShadowSink(tmp_path / "shadow" / "kanban.db")
    first = json.loads(sink.capture(_create_argv()).stdout)["id"]
    again = json.loads(sink.capture(_create_argv()).stdout)["id"]
    assert first == again
    tasks, _events, _links = sink.counts()
    assert tasks == 1


def test_shadow_sink_cards_expose_pair_details_with_parent(tmp_path: Path) -> None:
    """cards() returns machine-readable task records incl. the parent link."""
    sink = ShadowSink(tmp_path / "nested" / "kanban.db")
    impl = json.loads(sink.capture(_create_argv()).stdout)["id"]
    sink.capture(
        _create_argv(
            title=f"review: hkrc-fix (t_a) ({impl})",
            assignee="reviewer",
            idempotency_key="harness-hkrc-review:hkrc-fix:t_a",
            parent=impl,
        )
    )
    cards = sink.cards()
    assert [card["id"] for card in cards] == [impl, cards[1]["id"]]
    assert cards[0]["parent"] is None
    assert cards[1]["parent"] == impl
    assert cards[1]["title"].endswith(f"({impl})")


def test_is_kanban_create_argv_only_matches_create_calls() -> None:
    """The analyzer chat and git argv must never be captured by the sink."""
    assert is_kanban_create_argv(_create_argv())
    analyzer = [
        "hermes",
        "-p",
        "authoritative",
        "chat",
        "-q",
        "prompt",
        "--yolo",
        "-Q",
    ]
    assert not is_kanban_create_argv(analyzer)
    assert not is_kanban_create_argv(["git", "-C", "/tmp/repo", "log", "--oneline"])
    assert not is_kanban_create_argv(["hermes", "kanban", "list"])


# --- fixtures (mirror tests/test_harness_loop.py helpers) --------------------


def make_sessions_db(path: Path, sessions: list[dict[str, Any]]) -> Path:
    """Build a live-profile-style state.db with sessions + messages tables."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            started_at REAL NOT NULL,
            ended_at REAL,
            message_count INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            title TEXT,
            end_reason TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            timestamp REAL NOT NULL,
            active INTEGER DEFAULT 1
        );
        """
    )
    for session in sessions:
        connection.execute(
            "INSERT INTO sessions(id, source, started_at, ended_at, "
            "message_count, input_tokens, output_tokens, title, end_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session["id"],
                session.get("source", "cli"),
                session["started_at"],
                session.get("ended_at"),
                session.get("message_count", 0),
                session.get("input_tokens", 0),
                session.get("output_tokens", 0),
                session.get("title", ""),
                session.get("end_reason"),
            ),
        )
    connection.commit()
    connection.close()
    return path


def make_board(
    root: Path,
    slug: str,
    tasks: list[dict[str, Any]],
    *,
    events: dict[str, list[tuple[str, int, str | None]]] | None = None,
) -> Path:
    """Build a native board with the harness-loop collector schema."""
    board = root / slug
    board.mkdir(parents=True, exist_ok=True)
    (board / "board.json").write_text(json.dumps({"slug": slug}), encoding="utf-8")
    connection = sqlite3.connect(board / "kanban.db")
    connection.executescript(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT,
            assignee TEXT,
            status TEXT NOT NULL,
            priority INTEGER,
            created_at INTEGER,
            completed_at INTEGER,
            block_kind TEXT,
            workspace_kind TEXT,
            branch_name TEXT
        );
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY,
            task_id TEXT NOT NULL,
            run_id INTEGER,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE task_links (
            parent_id TEXT NOT NULL,
            child_id TEXT NOT NULL
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
    for task in tasks:
        connection.execute(
            "INSERT INTO tasks(id, title, body, assignee, status, priority, "
            "created_at, completed_at, block_kind, workspace_kind, branch_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task["id"],
                task.get("title", task["id"]),
                task.get("body"),
                task.get("assignee"),
                task["status"],
                task.get("priority", 0),
                task.get("created_at", NOW),
                task.get("completed_at"),
                task.get("block_kind"),
                task.get("workspace_kind", "worktree"),
                task.get("branch_name"),
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
    tmp_path: Path,
    *,
    sessions_db: Path | None = None,
    hkrc_repo: Path | None = None,
    analysis_profile: str = "",
) -> ControllerConfig:
    profiles = tmp_path / "profiles"
    return ControllerConfig(
        "test",
        tmp_path / "boards",
        tmp_path / "state" / "hkrc" / "state.sqlite3",
        harness_loop=HarnessLoopConfig(
            enabled=True,
            sessions_db=sessions_db or (profiles / "main" / "state.db"),
            external_dirs=(str(tmp_path / "dist"),),
            hkrc_repo=hkrc_repo or (tmp_path / "repo"),
            analysis_profile=analysis_profile,
            # Pin the sweep input hermetically (t_ae960b7d): unset would fall
            # back to the live instance default / HKRC_PROFILES_ROOT and leak
            # real config-drift findings into the shadow run.
            profiles_root=str(profiles),
            # Same hermetic pin for the archloop skip-streak sweep
            # (t_ba4092e4): the default resolves to the live cron output
            # dir and would leak real campcli/ynab-pilot findings into the
            # simulation.  A nonexistent dir yields zero findings.
            archloop_output_dir=str(tmp_path / "archloop-output"),
        ),
        watcher=WatcherConfig(reviewer_profiles=("reviewer",)),
    )


def make_hkrc_repo(tmp_path: Path) -> Path:
    """Create a real git repo with version files and one init commit."""
    repo = tmp_path / "repo"
    (repo / "src" / "hkrc").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        '[project]\nversion = "1.2.3"\n', encoding="utf-8"
    )
    (repo / "src" / "hkrc" / "thing.py").write_text("OLD_WORD\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "test"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@local"], check=True
    )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "init"], check=True)
    return repo


def queue_entry(
    fp: str,
    *,
    pattern: str = "skill-contradiction",
    key: str = "",
    severity: str = "high",
    apply_kind: str = "none",
    occurrence_count: int = 1,
    first_seen: int = NOW,
    last_seen: int = NOW,
    before: str = "",
    after: str = "",
    target_path: str = "",
    verify_path: str = "",
    verify_text: str = "",
) -> dict:
    """One persisted ``open_findings`` queue entry (v0.15.3 schema)."""
    return {
        "fingerprint": fp,
        "pattern": pattern,
        "key": key,
        "severity": severity,
        "evidence": (f"evidence {pattern} {key}",),
        "suggestion": "suggested fix",
        "apply_kind": apply_kind,
        "before": before,
        "after": after,
        "target_path": target_path,
        "verify_path": verify_path,
        "verify_text": verify_text,
        "match_subject": "",
        "first_seen": first_seen,
        "last_seen": last_seen,
        "occurrence_count": occurrence_count,
        "fix_status": "open",
        "last_suggestion": None,
        "last_deferral_reason": "",
    }


def valid_analysis_proposal(fp: str, **overrides: object) -> dict[str, object]:
    """One well-formed analyzer proposal referencing ``fp``."""
    proposal: dict[str, object] = {
        "evidence_references": [fp],
        "root_cause_hypothesis": (
            "Repeated first questions indicate the handoff detector misses "
            "the session_search route."
        ),
        "confidence": 0.9,
        "proposed_hkrc_change": {
            "target_path": "src/hkrc/thing.py",
            "before": "OLD_WORD",
            "after": "NEW_WORD",
            "suggestion": "extend the reask detector to the session_search route",
        },
        "acceptance_evidence": [
            "targeted harness tests pass",
            "reask count for the route drops to zero",
        ],
        "no_action_reason": "",
    }
    proposal.update(overrides)
    return proposal


def make_analysis_runner(
    *, stdout: str = "", returncode: int = 0
) -> tuple[ProcessRunner, list[str]]:
    """Fake the analyzer chat; every non-analyzer call runs for real."""

    def _real(argv: Sequence[str], env: Mapping[str, str], timeout: int) -> ProcessResult:
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            timeout=int(timeout),
            env=dict(env),
        )
        return ProcessResult(
            completed.returncode, completed.stdout or "", completed.stderr or ""
        )

    analyzer_calls: list[str] = []

    def runner(
        argv: Sequence[str], env: Mapping[str, str], timeout_secs: int
    ) -> ProcessResult:
        argv_list = list(argv)
        if (
            argv_list
            and argv_list[0].endswith("hermes")
            and "chat" in argv_list
            and "-p" in argv_list
        ):
            analyzer_calls.append(" ".join(argv_list))
            return ProcessResult(
                returncode, stdout, "" if returncode == 0 else "analyzer failed"
            )
        return _real(argv_list, env, timeout_secs)

    return runner, analyzer_calls


def _live_state(tmp_path: Path) -> tuple[ControllerConfig, Path, Path]:
    """Write a live harness state with one open hkrc-fix queue entry."""
    repo = make_hkrc_repo(tmp_path)
    thing = repo / "src" / "hkrc" / "thing.py"
    sessions_db = make_sessions_db(
        tmp_path / "profiles" / "main" / "state.db", []
    )
    make_board(tmp_path / "boards", "hkrc", [])
    config = make_config(
        tmp_path,
        sessions_db=sessions_db,
        hkrc_repo=repo,
        analysis_profile="nightly-analysis",
    )
    live_state = tmp_path / "state" / "hkrc" / "harness-loop-state.json"
    live_state.parent.mkdir(parents=True, exist_ok=True)
    live_state.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW - DAY,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": [
                    queue_entry(
                        "hkrc-fix:t_a",
                        pattern="hkrc-fix",
                        key="t_a",
                        severity="high",
                        apply_kind="hkrc",
                        occurrence_count=4,
                        first_seen=NOW - 20 * DAY,
                        before="OLD_WORD",
                        after="NEW_WORD",
                        target_path=str(thing),
                        verify_path=str(thing),
                        verify_text="OLD_WORD",
                    )
                ],
            }
        ),
        encoding="utf-8",
    )
    return config, repo, live_state


def board_counts(board_root: Path) -> tuple[int, int]:
    """(tasks, task create/archive events) on the live hkrc board, read-only."""
    connection = sqlite3.connect(
        f"file:{board_root / 'hkrc' / 'kanban.db'}?mode=ro", uri=True
    )
    try:
        tasks = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        events = connection.execute(
            "SELECT COUNT(*) FROM task_events WHERE kind IN ('created', 'archived')"
        ).fetchone()[0]
    finally:
        connection.close()
    return int(tasks), int(events)


# --- run_simulation ----------------------------------------------------------


def test_simulation_valid_proposal_creates_shadow_pair_and_proves_no_live_mutation(
    tmp_path: Path,
) -> None:
    """A valid proposal -> exactly one shadow impl + one parent-linked review;
    live state SHA, board counts, and git status are provably unchanged."""
    config, _repo, live_state = _live_state(tmp_path)
    shadow_dir = tmp_path / "shadow"
    live_sha = file_sha256(live_state)
    live_counts = board_counts(tmp_path / "boards")
    runner, _analyzer_calls = make_analysis_runner(
        stdout=json.dumps({"proposals": [valid_analysis_proposal("hkrc-fix:t_a")]})
    )
    result = run_simulation(
        config, now=NOW, shadow_dir=shadow_dir, runner=runner
    )
    assert result.passed is True
    # Exactly one implementation card plus one parent-linked review card.
    cards = result.shadow_cards
    assert len(cards) == 2
    impl, review = cards
    assert impl["parent"] is None
    assert impl["assignee"] == "developer"
    assert review["parent"] == impl["id"]
    assert review["assignee"] == "reviewer"
    # The shadow store holds the pair; the live board holds nothing new.
    assert result.shadow_board_counts == (2, 2, 1)  # tasks, events, links
    assert board_counts(tmp_path / "boards") == live_counts
    # Live harness state untouched (byte-identical), repo clean.
    assert result.live_state_sha_before == live_sha
    assert result.live_state_sha_after == live_sha
    assert result.live_state_unchanged is True
    assert result.git_status_before == ""
    assert result.git_status_after == ""
    assert result.git_unchanged is True
    assert result.board_unchanged is True
    # The shadow state copy was written by the pipeline; the live file is
    # byte-identical to the baseline (the run mutated only the copy).
    assert (shadow_dir / "harness-loop-state.json").is_file()
    # Report is labeled SIMULATION, carries the counts, and its narrative is
    # free of card ids (narrative audio must not leak shadow ids).
    assert SIMULATION_LABEL in result.report
    assert "generated=0" in result.report
    assert "routed=1" in result.report
    assert "t_shadow" not in result.narrative


def test_simulation_invalid_directory_target_creates_zero_cards_and_reports_disposition(
    tmp_path: Path,
) -> None:
    """A proposal naming a directory instead of a file fails deterministic
    validation BEFORE routing: zero shadow cards, zero kanban calls, and
    the report carries the concrete rejection disposition (acceptance:
    \"Invalid directory target creates zero shadow cards and reports its
    disposition\")."""
    config, _repo, live_state = _live_state(tmp_path)
    shadow_dir = tmp_path / "shadow-invalid"
    live_sha = file_sha256(live_state)
    live_counts = board_counts(tmp_path / "boards")
    runner, _analyzer_calls = make_analysis_runner(
        stdout=json.dumps(
            {
                "proposals": [
                    valid_analysis_proposal(
                        "hkrc-fix:t_a",
                        proposed_hkrc_change={
                            "target_path": "src/hkrc",  # directory, not a file
                            "before": "OLD_WORD",
                            "after": "NEW_WORD",
                            "suggestion": "edit the hkrc module",
                        },
                    )
                ]
            }
        )
    )
    result = run_simulation(config, now=NOW, shadow_dir=shadow_dir, runner=runner)
    assert result.passed is True
    # Zero shadow cards, zero rows in the isolated shadow store.
    assert result.shadow_cards == ()
    assert result.shadow_board_counts == (0, 0, 0)
    # The disposition is reported concretely (fail-closed before the router).
    assert "hkrc target is a directory" in result.report
    assert "rejected=1" in result.report
    assert "routed=0" in result.report
    # Live board untouched, live state byte-identical, canonical repo clean.
    assert board_counts(tmp_path / "boards") == live_counts
    assert result.live_state_sha_before == live_sha
    assert result.live_state_sha_after == live_sha
    assert result.live_state_unchanged is True
    assert result.git_status_before == ""
    assert result.git_status_after == ""
    assert result.git_unchanged is True
    assert result.board_unchanged is True
    assert SIMULATION_LABEL in result.report
    assert "t_shadow" not in result.narrative


def test_simulation_fails_closed_when_live_mutation_baseline_is_unavailable(
    tmp_path: Path,
) -> None:
    """Equal unavailable state sentinels are not mutation proof."""
    config, _repo, live_state = _live_state(tmp_path)
    missing_state = live_state.with_name("missing-state.json")
    runner, _calls = make_analysis_runner(stdout=json.dumps({"proposals": []}))

    result = run_simulation(
        config,
        now=NOW,
        shadow_dir=tmp_path / "shadow-unavailable",
        runner=runner,
        live_state_path=missing_state,
    )

    assert result.live_state_unchanged is True
    assert result.live_state_sha_before == "<missing>"
    assert result.passed is False
    assert "live harness state baseline unavailable" in result.report
    assert "live harness state baseline unavailable" in result.narrative


def test_simulation_fails_closed_when_live_board_baseline_is_unavailable(
    tmp_path: Path,
) -> None:
    config, _repo, live_state = _live_state(tmp_path)
    Path(config.native_boards_root, "hkrc", "kanban.db").unlink()
    runner, _calls = make_analysis_runner(stdout=json.dumps({"proposals": []}))

    result = run_simulation(
        config,
        now=NOW,
        shadow_dir=tmp_path / "shadow-board-unavailable",
        runner=runner,
        live_state_path=live_state,
    )

    assert result.board_counts_before is None
    assert result.board_counts_after is None
    assert result.board_unchanged is True
    assert result.passed is False
    assert "live board baseline unavailable" in result.report
    assert "live board baseline unavailable" in result.narrative


def test_operator_simulation_command_cannot_invoke_live_cron_wrapper() -> None:
    """The operator path dispatches simulation directly, never the cron shim/run."""
    from argparse import Namespace
    from unittest.mock import patch

    from hkrc.cli import _harness_loop_simulate

    result = type("Result", (), {"report": "SIMULATION", "passed": True})()
    args = Namespace(
        config=Path("/instance/config/hkrc/config.toml"),
        shadow_dir=Path("/tmp/hkrc-shadow"),
        now=NOW,
    )
    with (
        patch("hkrc.cli.load_config", return_value=object()),
        patch("hkrc.cli.run_simulation", return_value=result) as simulate,
        patch("hkrc.cli.run_harness_loop") as live_run,
        patch("subprocess.call") as cron_wrapper,
    ):
        assert _harness_loop_simulate(args) == 0

    simulate.assert_called_once()
    live_run.assert_not_called()
    cron_wrapper.assert_not_called()

