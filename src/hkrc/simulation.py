"""Shadow-live simulation for the nightly HKRC harness loop.

Operator-facing: ``hkrc harness-loop simulate`` reproduces the production
noon execution as closely as possible without mutating live harness
state, the live ``hkrc`` kanban board, the canonical HKRC checkout, or the
deployment:

- Live read-only evidence sources (sessions DB, board snapshots, git
  history) are used exactly as production does.
- ``harness-loop-state.json`` is copied to a task-specific shadow state
  file; the real ``run()`` pipeline (deterministic validation, policy
  gates, apply budget, implementation + parent-linked review pair
  construction) executes against that copy.
- The REAL configured authoritative Hermes profile is invoked in a fresh
  autonomous session (strict JSON analysis); analyzer output is never
  replaced with fixtures in the operator path.
- Kanban creates are captured by an isolated shadow sink (same board
  schema, separate SQLite file) — the live board is never written.
- Live state SHA-256, live ``hkrc`` board task/event counts, and the
  canonical ``git status --short`` are hashed/snapshotted before and
  after; any change fails the simulation.
- One report labeled SIMULATION carries counts, concrete reasons, shadow
  pair details (IDs only in the machine-readable block, never in the
  narrative), analyzer session/model evidence, and mutation proof.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .harness_loop import (
    DEFAULT_HKRC_REPO,
    HKRC_BOARD,
    HarnessLoopError,
    ProcessResult,
    ProcessRunner,
    STATE_FILENAME,
    _extract_model_default,
    default_state_path,
    run as run_harness_loop,
)

SIMULATION_LABEL = "SIMULATION"
SHADOW_TASK_ID_PREFIX = "t_shadow"
SHADOW_BOARD_FILENAME = "kanban.db"
SHADOW_STATE_FILENAME = STATE_FILENAME  # harness-loop-state.json

# Same schema as the live native board (harness-loop collector shape), so
# simulated task records and parent links are faithful to the real board.
_SHADOW_BOARD_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    assignee TEXT,
    status TEXT NOT NULL,
    priority INTEGER,
    created_at INTEGER,
    completed_at INTEGER,
    block_kind TEXT,
    branch_name TEXT
);
CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    run_id INTEGER,
    kind TEXT NOT NULL,
    payload TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS task_links (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    profile TEXT,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    outcome TEXT,
    error TEXT
);
"""


def is_kanban_create_argv(argv: Sequence[str]) -> bool:
    """True when ``argv`` is a ``hermes kanban --board <b> create`` call.

    Only the ticket router's board-create subprocess is captured by the
    shadow sink; the analyzer chat and git calls must pass through to the
    real subprocess (or the test-injected runner).
    """
    argv_list = list(argv)
    if not argv_list:
        return False
    if not argv_list[0].endswith("hermes"):
        return False
    return (
        "kanban" in argv_list
        and "--board" in argv_list
        and "create" in argv_list
    )


def _argv_value_after(argv: Sequence[str], flag: str) -> str:
    """Value of the token following ``flag`` (or ``""`` when absent)."""
    argv_list = list(argv)
    if flag in argv_list:
        index = argv_list.index(flag)
        if index + 1 < len(argv_list):
            return argv_list[index + 1]
    return ""


def _shadow_task_id(idempotency_key: str) -> str:
    """Deterministic shadow task id from the idempotency key.

    The same key always yields the same id, mirroring the real CLI's
    idempotency contract so router retries reuse the same simulated card.
    """
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    return f"{SHADOW_TASK_ID_PREFIX}{digest[:12]}"


class ShadowSink:
    """Isolated board-shaped capture sink for simulated kanban creates.

    Writes the same schema as the live board into a separate SQLite file
    so simulated task records and parent link evidence are faithful to
    production without ever touching the live ``hkrc`` board.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.executescript(_SHADOW_BOARD_SCHEMA)
            connection.commit()
        finally:
            connection.close()

    def capture(
        self, argv: Sequence[str], *, now: int | None = None
    ) -> ProcessResult:
        """Simulate one ``hermes kanban create``; return the CLI JSON reply.

        The task row, a ``created`` event, and (for ``--parent``) a
        ``task_links`` row are written to the shadow board.  A retry with
        the same idempotency key reuses the same task id and adds nothing.
        """
        if not is_kanban_create_argv(argv):
            return ProcessResult(2, "", "not a kanban create argv")
        created_at = int(time.time()) if now is None else int(now)
        title = _argv_value_after(argv, "create")
        assignee = _argv_value_after(argv, "--assignee")
        body = _argv_value_after(argv, "--body")
        key = _argv_value_after(argv, "--idempotency-key")
        parent = _argv_value_after(argv, "--parent")
        task_id = _shadow_task_id(key)
        connection = sqlite3.connect(self.db_path)
        try:
            existing = connection.execute(
                "SELECT id FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO tasks(id, title, body, assignee, status, "
                    "priority, created_at, completed_at, block_kind, "
                    "branch_name) VALUES (?, ?, ?, ?, 'todo', 0, ?, NULL, "
                    "NULL, NULL)",
                    (task_id, title, body, assignee, created_at),
                )
                connection.execute(
                    "INSERT INTO task_events(task_id, run_id, kind, payload, "
                    "created_at) VALUES (?, NULL, 'created', ?, ?)",
                    (task_id, json.dumps({"assignee": assignee}), created_at),
                )
                if parent:
                    connection.execute(
                        "INSERT INTO task_links(parent_id, child_id) "
                        "VALUES (?, ?)",
                        (parent, task_id),
                    )
                connection.commit()
        finally:
            connection.close()
        return ProcessResult(0, json.dumps({"id": task_id}), "")

    def counts(self) -> tuple[int, int, int]:
        """(tasks, events, links) rows currently recorded in the shadow board."""
        connection = sqlite3.connect(self.db_path)
        try:
            tasks = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM task_events"
            ).fetchone()[0]
            links = connection.execute(
                "SELECT COUNT(*) FROM task_links"
            ).fetchone()[0]
        finally:
            connection.close()
        return int(tasks), int(events), int(links)

    def cards(self) -> list[dict[str, object]]:
        """Machine-readable shadow task records with parent link evidence."""
        connection = sqlite3.connect(self.db_path)
        try:
            rows = connection.execute(
                "SELECT id, title, assignee, status, created_at FROM tasks "
                "ORDER BY created_at"
            ).fetchall()
            parents = dict(
                connection.execute(
                    "SELECT child_id, parent_id FROM task_links"
                ).fetchall()
            )
        finally:
            connection.close()
        return [
            {
                "id": row[0],
                "title": row[1],
                "assignee": row[2],
                "status": row[3],
                "created_at": row[4],
                "parent": parents.get(row[0]),
            }
            for row in rows
        ]

    def runner(self, passthrough: ProcessRunner | None = None) -> ProcessRunner:
        """Subprocess runner: kanban creates go to the sink; everything else
        falls through to ``passthrough`` (tests) or the real subprocess
        (operator path — the analyzer chat runs the REAL configured profile)."""

        def _runner(
            argv: Sequence[str], env: Mapping[str, str], timeout: int
        ) -> ProcessResult:
            if is_kanban_create_argv(argv):
                return self.capture(argv)
            if passthrough is not None:
                return passthrough(list(argv), env, int(timeout))
            return _real_subprocess(list(argv), env, int(timeout))

        return _runner


def _real_subprocess(
    argv: Sequence[str], env: Mapping[str, str], timeout: int
) -> ProcessResult:
    try:
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
    except subprocess.TimeoutExpired:
        return ProcessResult(124, "", "timeout")
    except OSError as exc:
        return ProcessResult(127, "", str(exc))


# --- live-state mutation proof ----------------------------------------------


def _hash_file(path: Path) -> str:
    """SHA-256 of a file's bytes; ``<missing>`` sentinel when absent."""
    path = Path(path)
    if not path.is_file():
        return "<missing>"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_status_short(repo: Path) -> str:
    """Canonical ``git status --short`` (read-only); error text on failure."""
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), "status", "--short"],
            capture_output=True,
            text=True,
            check=False,
            env=dict(os.environ),
        )
    except OSError as exc:
        return f"<git unavailable: {exc}>"
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        return f"<git error: {detail}>"
    return completed.stdout.strip()


def _board_counts(board_db: Path) -> tuple[int, int] | None:
    """(tasks, task create/archive events) on the live board; ``None`` when unreadable.

    Fail-closed like the harness collectors: a live WAL snapshot or a
    missing/unreadable database records ``None`` (the simulation then
    reports the mutation check as inconclusive rather than silently green).
    """
    board_db = Path(board_db)
    if not board_db.is_file():
        return None
    try:
        connection = sqlite3.connect(
            f"file:{board_db}?mode=ro", uri=True
        )
        try:
            tasks = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM task_events WHERE kind IN "
                "('created', 'archived')"
            ).fetchone()[0]
            return int(tasks), int(events)
        finally:
            connection.close()
    except sqlite3.Error:
        return None


def _analyzer_evidence(profile: str) -> tuple[str, Path | None, Path | None]:
    """(model, profile_config_path, session_db_path) for the analysis profile.

    The model is read from the REAL configured profile's ``config.yaml``
    (stdlib-only ``model.default`` extraction), so the report names the
    actual model Sol High the analyzer will run under, never a fixture.
    """
    if not profile:
        return "", None, None
    home = os.environ.get("HOME") or str(Path.home())
    hermes_home = os.environ.get("HERMES_HOME") or os.path.join(home, ".hermes")
    profile_dir = Path(hermes_home) / "profiles" / profile
    config_path = profile_dir / "config.yaml"
    model = ""
    try:
        if config_path.is_file():
            model = _extract_model_default(
                config_path.read_text(encoding="utf-8")
            )
    except OSError:
        model = ""
    return model, config_path, profile_dir / "state.db"


def _session_evidence(
    session_db: Path | None, since: int
) -> tuple[int, str]:
    """(new_sessions_since, error) in the analyzer profile's session DB.

    The real analyzer invocation writes a fresh session row into
    the profile's state.db — that row IS the session evidence the report
    names (the analyzer ran, the model answered).
    """
    if session_db is None or not session_db.is_file():
        return -1, "session DB unavailable"
    try:
        connection = sqlite3.connect(
            f"file:{session_db}?mode=ro", uri=True
        )
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM sessions WHERE started_at >= ?",
                (float(since),),
            ).fetchone()
            return int(row[0]) if row else 0, ""
        finally:
            connection.close()
    except sqlite3.Error as exc:
        return -1, str(exc)


def _analyzer_argv(profile: str) -> tuple[str, ...]:
    """Exact analyzer invocation (mirrors harness_loop._analyzer_command)."""
    if not profile:
        return ()
    from .harness_loop import _hermes_bin

    return (
        _hermes_bin(),
        "-p",
        profile,
        "chat",
        "-q",
        "<prompt>",
        "--yolo",
        "-Q",
    )


# --- simulation result + render ---------------------------------------------


def _trace_int(trace: dict[str, object], key: str, default: int = 0) -> int:
    """Coerce one trace fact to int; any missing/malformed value -> default."""
    value = trace.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _trace_list(trace: dict[str, object], key: str) -> list[object]:
    """Read one trace fact as a list (lists and tuples both accepted)."""
    value = trace.get(key)
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Outcome of one shadow-live simulation run.

    ``passed`` is True only when every live-mutation check held: the live
    harness state SHA-256, the canonical checkout ``git status --short``,
    and the live ``hkrc`` board task/event counts are all identical
    before and after.  ``report`` is the full SIMULATION-labeled report
    (machine block carries the shadow card ids); ``narrative`` is the
    audio-safe one-liner that never contains card ids.
    """

    simulation_id: str
    started_at: int
    ended_at: int
    shadow_dir: Path
    shadow_state_path: Path
    shadow_board_path: Path
    live_state_path: Path
    live_state_sha_before: str
    live_state_sha_after: str
    git_status_before: str
    git_status_after: str
    board_counts_before: tuple[int, int] | None
    board_counts_after: tuple[int, int] | None
    shadow_board_counts: tuple[int, int, int]
    live_state_unchanged: bool
    git_unchanged: bool
    board_unchanged: bool
    trace: dict[str, object]
    analyzer_profile: str
    analyzer_model: str
    analyzer_config_path: Path | None
    analyzer_session_db: Path | None
    analyzer_session_count: int
    analyzer_session_error: str
    shadow_cards: tuple[dict[str, object], ...]
    harness_report: str
    notes: tuple[str, ...]

    @property
    def failure_reasons(self) -> tuple[str, ...]:
        """Concrete fail-closed reasons for every unavailable or changed proof."""
        reasons: list[str] = []
        if self.live_state_sha_before == "<missing>":
            reasons.append("live harness state baseline unavailable")
        elif not self.live_state_unchanged:
            reasons.append("live harness state changed")
        if self.git_status_before or self.git_status_after:
            reasons.append("canonical checkout is not clean")
        elif not self.git_unchanged:
            reasons.append("canonical checkout changed")
        if self.board_counts_before is None or self.board_counts_after is None:
            reasons.append("live board baseline unavailable")
        elif not self.board_unchanged:
            reasons.append("live board changed")
        return tuple(reasons)

    @property
    def passed(self) -> bool:
        return not self.failure_reasons

    @property
    def narrative(self) -> str:
        return _render_narrative(self)

    @property
    def report(self) -> str:
        return _render_report(self)


def run_simulation(
    config: object,
    *,
    now: int | None = None,
    shadow_dir: Path | None = None,
    runner: ProcessRunner | None = None,
    live_state_path: Path | None = None,
) -> SimulationResult:
    """Run the real harness pipeline against a shadow state copy + shadow sink.

    Baseline (live state SHA-256, canonical ``git status --short``, live
    ``hkrc`` board task/event counts) -> copy live state to the shadow
    state file -> run the REAL pipeline with ``dry_run=False`` under the
    shadow runner (kanban creates captured, analyzer/git pass through) ->
    re-measure baseline -> fail when anything moved.
    """
    from .config import ControllerConfig

    if not isinstance(config, ControllerConfig):
        raise HarnessLoopError("run_simulation requires a ControllerConfig")
    if not config.harness_loop.enabled:
        raise HarnessLoopError(
            "harness_loop is disabled; cannot run a shadow-live simulation"
        )
    current = int(time.time()) if now is None else int(now)
    simulation_id = f"sim-{current}"
    live_state = (
        Path(live_state_path)
        if live_state_path
        else default_state_path(config.state_db)
    )
    repo = Path(config.harness_loop.hkrc_repo or DEFAULT_HKRC_REPO)
    board_db = Path(config.native_boards_root) / HKRC_BOARD / "kanban.db"
    notes: list[str] = []

    sha_before = _hash_file(live_state)
    git_before = _git_status_short(repo)
    counts_before = _board_counts(board_db)
    if counts_before is None:
        notes.append(
            f"live board {board_db} unreadable (fail-closed); "
            "task/event counts recorded as unavailable"
        )

    if shadow_dir is None:
        shadow_dir = Path(tempfile.mkdtemp(prefix="hkrc-shadow-"))
    shadow_dir = Path(shadow_dir)
    shadow_dir.mkdir(parents=True, exist_ok=True)
    shadow_state = shadow_dir / SHADOW_STATE_FILENAME
    if live_state.is_file():
        shutil.copyfile(live_state, shadow_state)
    else:
        notes.append(
            f"live state file missing: {live_state}; "
            "simulation starts from the pre-seeded state"
        )

    sink = ShadowSink(shadow_dir / SHADOW_BOARD_FILENAME)
    shadow_runner = sink.runner(passthrough=runner)
    trace: list[dict] = []
    harness_report = run_harness_loop(
        config,
        now=current,
        dry_run=False,
        runner=shadow_runner,
        state_path=shadow_state,
        trace=trace,
    )

    sha_after = _hash_file(live_state)
    git_after = _git_status_short(repo)
    counts_after = _board_counts(board_db)

    profile = str(config.harness_loop.analysis_profile)
    model, profile_config_path, session_db = _analyzer_evidence(profile)
    session_since = int(current) - int(config.harness_loop.window_hours * 3600)
    session_count, session_error = _session_evidence(session_db, session_since)

    return SimulationResult(
        simulation_id=simulation_id,
        started_at=current,
        ended_at=int(time.time()) if now is None else int(current),
        shadow_dir=shadow_dir,
        shadow_state_path=shadow_state,
        shadow_board_path=shadow_dir / SHADOW_BOARD_FILENAME,
        live_state_path=Path(live_state),
        live_state_sha_before=sha_before,
        live_state_sha_after=sha_after,
        git_status_before=git_before,
        git_status_after=git_after,
        board_counts_before=counts_before,
        board_counts_after=counts_after,
        shadow_board_counts=sink.counts(),
        live_state_unchanged=sha_before == sha_after,
        git_unchanged=git_before == git_after,
        board_unchanged=counts_before == counts_after,
        trace=trace[0] if trace else {},
        analyzer_profile=profile,
        analyzer_model=model,
        analyzer_config_path=profile_config_path,
        analyzer_session_db=session_db,
        analyzer_session_count=session_count,
        analyzer_session_error=session_error,
        shadow_cards=tuple(sink.cards()),
        harness_report=harness_report,
        notes=tuple(notes),
    )


def _render_narrative(result: SimulationResult) -> str:
    """One audio-safe line: counts + outcome, never card ids."""
    trace = result.trace
    fresh = _trace_int(trace, "fresh_count")
    routed = len(result.shadow_cards) // 2 if result.shadow_cards else 0
    rejected = len(_trace_list(trace, "analysis_rejections"))
    deferred = len(_trace_list(trace, "deferrals"))
    impl_count = sum(1 for card in result.shadow_cards if card.get("parent") is None)
    review_count = len(result.shadow_cards) - impl_count
    if result.passed:
        outcome = "zero live mutations"
    else:
        outcome = "FAILED: " + ", ".join(result.failure_reasons)
    return (
        f"{SIMULATION_LABEL} {result.simulation_id}: window "
        f"{_trace_int(trace, 'window_hours')}h: "
        f"{_trace_int(trace, 'sessions_count')} sessions, {fresh} new "
        f"findings, {routed} routed to shadow pair construction "
        f"({impl_count} implementation + {review_count} parent-linked "
        f"review), {rejected} rejected, {deferred} deferred; {outcome}"
    )


def _render_report(result: SimulationResult) -> str:
    """Full SIMULATION-labeled report: evidence window, analyzer evidence,
    counts + reasons, shadow pair details (ids only in the machine block),
    and mutation proof."""
    trace = result.trace
    window_hours = _trace_int(trace, "window_hours")
    generated = _trace_int(trace, "fresh_count")
    validated = _trace_int(trace, "analysis_proposals_count")
    routed = len(result.shadow_cards) // 2 if result.shadow_cards else 0
    rejected = len(_trace_list(trace, "analysis_rejections"))
    deferred = len(_trace_list(trace, "deferrals"))
    status = str(trace.get("analysis_status", "?"))
    reason = str(trace.get("analysis_reason", ""))
    if status == "ok":
        status_line = f"status: ok ({validated} validated proposal(s))"
    elif status == "disabled":
        status_line = "status: disabled (no analysis profile configured; deterministic routing)"
    else:
        status_line = f"status: failed — {reason}"
    lines = [
        "=== HKRC HARNESS-LOOP SIMULATION (shadow-live) ===",
        f"label: {SIMULATION_LABEL}",
        f"simulation_id: {result.simulation_id}",
        f"started_at: {result.started_at}",
        f"ended_at: {result.ended_at}",
        "",
        "Evidence window (real, read-only sources):",
        (
            f"  {window_hours}h window: {_trace_int(trace, 'sessions_count')} "
            f"sessions, {_trace_int(trace, 'boards_count')} boards, "
            f"{_trace_int(trace, 'git_commits_count')} commits in git log"
        ),
        "",
        "Analyzer (real configured authoritative profile, fresh autonomous session):",
        f"  profile: {result.analyzer_profile or '(none)'}",
        (
            f"  model: {result.analyzer_model or 'unresolved'}"
            + (
                f" (from {result.analyzer_config_path})"
                if result.analyzer_config_path is not None
                else ""
            )
        ),
        "  invocation: hermes -p <profile> chat -q <prompt> --yolo -Q "
        "(reasoning + turn budget from profile config)",
        f"  {status_line}",
        (
            f"  session evidence: {result.analyzer_session_count} new "
            f"session(s) in {result.analyzer_session_db or 'n/a'}"
            + (f" ({result.analyzer_session_error})" if result.analyzer_session_error else "")
        ),
        "",
        "Counts (generated/validated/routed/rejected/deferred):",
        (
            f"  generated={generated} validated={validated} routed={routed} "
            f"rejected={rejected} deferred={deferred}"
        ),
    ]
    rejections = [str(item) for item in _trace_list(trace, "analysis_rejections")]
    deferrals = [str(item) for item in _trace_list(trace, "deferrals")]
    if rejections:
        lines.append("Rejected (concrete reasons):")
        lines.extend(f"  - {item}" for item in rejections)
    if deferrals:
        lines.append("Deferred (concrete reasons):")
        lines.extend(f"  - {item}" for item in deferrals)
    impl_cards = [card for card in result.shadow_cards if card.get("parent") is None]
    review_cards = [
        card for card in result.shadow_cards if card.get("parent") is not None
    ]
    lines.append("Shadow pair construction (narrative-safe; ids only in the machine block):")
    for index, card in enumerate(impl_cards, start=1):
        lines.append(
            f"  implementation card {index}: title={card.get('title')} "
            f"assignee={card.get('assignee')}"
        )
    for index, card in enumerate(review_cards, start=1):
        lines.append(
            f"  review card {index}: title={card.get('title')} "
            f"assignee={card.get('assignee')} parent=implementation card {index}"
        )
    if not result.shadow_cards:
        lines.append("  none")
    lines += [
        "",
        "Mutation proof (live, before -> after):",
        (
            f"  live state sha256: {result.live_state_sha_before[:16]}... -> "
            f"{result.live_state_sha_after[:16]}... "
            f"identical={result.live_state_unchanged}"
        ),
        (
            f"  live hkrc board (tasks, events): {result.board_counts_before} -> "
            f"{result.board_counts_after} identical={result.board_unchanged}"
        ),
        (
            f"  canonical checkout git status --short: "
            f"{result.git_status_before!r} -> {result.git_status_after!r} "
            f"identical={result.git_unchanged}"
        ),
        f"  shadow artifacts: {result.shadow_dir}",
    ]
    if result.notes:
        lines.append("Notes:")
        lines.extend(f"  - {item}" for item in result.notes)
    lines.append(
        (
            f"Result: {SIMULATION_LABEL} OK (zero live mutations)"
            if result.passed
            else (
                f"Result: {SIMULATION_LABEL} FAILED "
                f"({'; '.join(result.failure_reasons)})"
            )
        )
    )
    lines += [
        "",
        "Machine-readable block (JSON; includes shadow card ids):",
        json.dumps(
            {
                "simulation_id": result.simulation_id,
                "started_at": result.started_at,
                "ended_at": result.ended_at,
                "analyzer": {
                    "profile": result.analyzer_profile,
                    "model": result.analyzer_model,
                    "invocation": list(_analyzer_argv(result.analyzer_profile)),
                    "session_db": str(result.analyzer_session_db)
                    if result.analyzer_session_db is not None
                    else None,
                    "session_count": result.analyzer_session_count,
                },
                "counts": {
                    "generated": generated,
                    "validated": validated,
                    "routed": routed,
                    "rejected": rejected,
                    "deferred": deferred,
                },
                "shadow_board_counts": {
                    "tasks": result.shadow_board_counts[0],
                    "events": result.shadow_board_counts[1],
                    "links": result.shadow_board_counts[2],
                },
                "shadow_cards": list(result.shadow_cards),
                "mutation_proof": {
                    "live_state_sha256_identical": result.live_state_unchanged,
                    "live_board_counts_identical": result.board_unchanged,
                    "canonical_git_status_identical": result.git_unchanged,
                    "passed": result.passed,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        "",
        "Harness report (reproduced below):",
        "",
        result.harness_report or "(harness loop disabled)",
    ]
    return "\n".join(lines)

