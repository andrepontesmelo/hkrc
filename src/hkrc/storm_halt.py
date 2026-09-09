"""Crash-storm halt breaker: daemon-tick detector + constrained halt applier.

Design source of truth: R3 research comment 671 on ``t_652caf9a``; grilling
gate ``t_8fa311b2`` decided daemon placement (storm-class only — drift-class
findings stay in the daily loop) and halt-with-constraints.

Shape (guardrail zone 3, state-mutating autonomy):

- The DETECTOR is read-only: it snapshots each non-archived board's
  ``kanban.db`` to a temp file (sqlite3 online backup API — the live board
  file and its ``-wal``/``-shm`` sidecars are only ever read, never created
  or altered) and runs the exact R3 SQL over the snapshot.  It emits one
  ``StormFinding`` per (board, profile, signature) group at or above the
  threshold; it never mutates anything.
- The APPLIER is the only writer.  It is allowlisted in code to exactly one
  verb — ``block`` — on exactly one target set — the ``ready`` cards of the
  storming (board, profile) lane.  Complete, reclaim, edit, reassign,
  comment, unblock, and any other verb are unreachable: the only argv the
  applier can construct is the block command (one call site, verb from
  ``_ALLOWED_VERBS``), listed through ``list --status ready --assignee
  <profile> --json`` so a ``None``-profile lane fails closed before any
  listing is built.  Blocked cards are reversible with
  ``hermes kanban unblock``.
- Both halves are pure functions over injected callables; production wiring
  (snapshot IO, subprocess) lives in the daemon runtime and CLI adapters.

Halt findings carry the SQL evidence (board, profile, signature prefix,
count, window) so the halt itself is the audit trail; the daemon logs them
as structured ``storm_halt`` records and dedupes per episode so one storm
cannot re-halt (or re-alert) on every tick.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import json
import sqlite3


class StormHaltError(RuntimeError):
    """Raised when the storm detector or halt applier fails closed."""


# --- R3 signal constants (exact; do not tune without a safety re-review) -----

# Crash signature prefix length.  Deliberate: 60 chars separates the storm
# classes while collapsing pid-noise ("pid 12345 not alive" varies per run
# and must NOT group by the full string).
SIGNATURE_PREFIX_CHARS = 60

# A (profile, signature) group is a storm at this many crashed runs...
STORM_MIN_COUNT = 2

# ...inside this window.
STORM_WINDOW_MINUTES = 30

# The only storm-class outcome: reclaim bursts are operator-initiated
# (manual_reclaim quota holds), spawn_failed/gave_up are not loop storms.
STORM_OUTCOME = "crashed"

HALT_BLOCK_REASON_TEMPLATE = (
    "storm-halt: profile {profile} crashed {count}x in "
    f"{STORM_WINDOW_MINUTES}m with signature {{signature!r}} (board {{board}}); "
    "auto-halted by hkrc daemon — reverse with: "
    "hermes kanban --board {board} unblock <task_id>"
)

# Ready cards are the only halt targets: they are the cards the dispatcher
# would pick next, i.e. the exact set a storm would burn through.
HALTED_STATUSES = ("ready",)

# The applier's verb allowlist.  One entry.  Anything else is unreachable:
# the only argv the applier builds passes through this set.
_ALLOWED_VERBS = frozenset({"block"})


def getattr_result_code(result: object) -> int:
    """Default ``result_of``: read ``returncode`` off a subprocess result."""

    return int(getattr(result, "returncode", 0))


@dataclass(frozen=True, slots=True)
class StormFinding:
    """One storming (board, profile, signature) lane with its SQL evidence."""

    board_slug: str
    profile: str | None
    signature: str
    count: int
    first_crashed_at: int
    last_crashed_at: int

    @property
    def reason(self) -> str:
        """Block reason written onto every halted card (self-auditing)."""

        return HALT_BLOCK_REASON_TEMPLATE.format(
            profile=self.profile or "unknown",
            count=self.count,
            signature=self.signature,
            board=self.board_slug,
        )

    @property
    def evidence_line(self) -> str:
        return (
            f"board={self.board_slug} profile={self.profile or 'unknown'} "
            f"count={self.count} signature={self.signature!r}"
        )

    @property
    def dedupe_key(self) -> str:
        """Stable per-episode key: one lane storms once until it stops."""

        return f"{self.board_slug}|{self.profile or ''}|{self.signature}"


# --- detector ----------------------------------------------------------------


def storm_signature(error: str | None) -> str:
    """Return the R3 signature prefix of a crash error string."""

    return (error or "")[:SIGNATURE_PREFIX_CHARS]


def detect_storms(
    connection: sqlite3.Connection,
    board_slug: str,
    *,
    now: int,
    window_minutes: int = STORM_WINDOW_MINUTES,
    min_count: int = STORM_MIN_COUNT,
) -> tuple[StormFinding, ...]:
    """Run the R3 storm SQL against one board snapshot connection.

    Pure read: the caller owns the connection (a temp snapshot opened
    read-only; never the live board file).  ``now`` is injectable for
    deterministic tests.  Groups with fewer than ``min_count`` crashed runs
    in the window produce no finding (single crash no-hit); pid-noise
    collapses onto the shared prefix; reclaim/spawn_failed/gave_up rows are
    excluded by the outcome filter.
    """

    cutoff = int(now) - int(window_minutes) * 60
    rows = connection.execute(
        """
        SELECT profile, substr(error, 1, ?) AS sig, COUNT(*) AS n,
               MIN(started_at) AS first_at, MAX(started_at) AS last_at
          FROM task_runs
         WHERE outcome = ?
           AND started_at > ?
         GROUP BY profile, sig
        HAVING COUNT(*) >= ?
        """,
        (SIGNATURE_PREFIX_CHARS, STORM_OUTCOME, cutoff, min_count),
    ).fetchall()
    return tuple(
        StormFinding(
            board_slug=board_slug,
            profile=row["profile"] if row["profile"] else None,
            signature=str(row["sig"]),
            count=int(row["n"]),
            first_crashed_at=int(row["first_at"]),
            last_crashed_at=int(row["last_at"]),
        )
        for row in rows
    )


# --- halt applier ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HaltResult:
    """Outcome of one halt application against one (board, profile) lane."""

    finding: StormFinding
    blocked_task_ids: tuple[str, ...]
    failed_task_ids: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return bool(self.blocked_task_ids) and not self.failed_task_ids

    def summary_line(self) -> str:
        blocked = ",".join(self.blocked_task_ids) or "-"
        failed = ",".join(self.failed_task_ids) or "-"
        return (
            f"storm_halt {self.finding.evidence_line} "
            f"blocked=[{blocked}] failed=[{failed}]"
        )


def halt_command(
    native_cli: str,
    native_profile: str | None,
    board_slug: str,
    task_id: str,
    reason: str,
) -> list[str]:
    """Build the block argv for one halted card (the ONLY argv this module builds).

    The verb is allowlisted through ``_ALLOWED_VERBS`` so a future edit that
    adds a verb must touch this check first; the argv list is passed to
    ``subprocess.run`` directly (never a shell), so shell-hostile task ids
    and reason text stay inert.
    """

    command = [native_cli]
    if native_profile:
        command.extend(["--profile", native_profile])
    verb = "block"
    if verb not in _ALLOWED_VERBS:  # pragma: no cover - structural allowlist
        raise AssertionError(f"halt applier verb must be allowlisted: {verb}")
    command.extend(["kanban", "--board", board_slug, verb, task_id, reason])
    return command


def apply_halt(
    finding: StormFinding,
    ready_task_ids: Sequence[str],
    runner: Callable[[Sequence[str]], object],
    *,
    native_cli: str = "hermes",
    native_profile: str | None = None,
    result_of: Callable[[object], int] = getattr_result_code,
) -> HaltResult:
    """Block every ready card in the storm lane; refuse everything else.

    ``runner`` mirrors the daemon/handoff NativeRunner shape (production:
    ``subprocess.run`` with an argv list; tests: an injected fake).  A
    non-zero runner exit records the task as failed-but-continued: one
    refused block never abandons the remaining ready cards of the lane.
    """

    if not ready_task_ids:
        return HaltResult(finding, (), ())
    blocked: list[str] = []
    failed: list[str] = []
    for task_id in ready_task_ids:
        result = runner(
            halt_command(
                native_cli, native_profile, finding.board_slug, task_id, finding.reason
            )
        )
        if result_of(result) == 0:
            blocked.append(task_id)
        else:
            failed.append(task_id)
    return HaltResult(finding, tuple(blocked), tuple(failed))


def list_ready_task_ids(
    native_cli: str,
    native_profile: str | None,
    board_slug: str,
    *,
    profile: str | None,
    runner: Callable[[Sequence[str]], object],
    result_of: Callable[[object], int] = getattr_result_code,
) -> tuple[str, ...]:
    """Return the storm lane's ready-card ids via ``list --status ready --json``.

    Read-only listing channel (same CLI the watcher/review-gap modules use);
    argv lists only, never a shell.  The lane is the (board, profile) pair:
    ``profile`` scopes the list to that assignee — R3 §3 blocks the ready
    cards of the storming lane only, never the whole board.  ``profile=None``
    fails closed with ``StormHaltError`` (a halt that cannot say whose cards
    it is blocking must block nothing).

    A failed or unparseable listing raises ``StormHaltError`` — the halt
    fails closed: an applier that cannot see the lane's ready set must not
    guess at targets.
    """

    if not profile:
        raise StormHaltError(
            f"storm halt for board {board_slug} has no profile lane; "
            "refusing to block board-wide (fail-closed)"
        )
    command = [native_cli]
    if native_profile:
        command.extend(["--profile", native_profile])
    command.extend(
        ["kanban", "--board", board_slug, "list", "--status", "ready", "--assignee", profile, "--json"]
    )
    result = runner(command)
    if result_of(result) != 0:
        detail = str(getattr(result, "stderr", "") or getattr(result, "stdout", "")).strip()
        raise StormHaltError(
            f"ready listing failed for board {board_slug} (exit {result_of(result)}): {detail[:300]}"
        )
    stdout = str(getattr(result, "stdout", "") or "")
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise StormHaltError(
            f"ready listing for board {board_slug} returned unparseable JSON: {exc}"
        ) from exc
    if not isinstance(data, list):
        raise StormHaltError(f"ready listing for board {board_slug} must be a JSON array")
    ids: list[str] = []
    for entry in data:
        if isinstance(entry, dict):
            task_id = entry.get("id")
            if isinstance(task_id, str) and task_id:
                ids.append(task_id)
    return tuple(ids)

