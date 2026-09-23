"""Shipped-fix retro pre-pass (design #6, kanban t_85199c5e).

Deterministic pre-pass inside the 03:00 harness-learning loop.  It builds a
register of shipped fixes (merge commits whose subject references kanban
``t_`` ids), classifies every fix into exactly ONE of three verdicts, writes a
typed ``detector_requirement`` ledger entry for every blind spot, and renders a
report section carrying the two headline metrics.

Design notes
------------
- Provenance is the nightly git scan: no merge-time ledger, no git hooks, no
  outcome-guard changes.  Deploy is metadata only, joined from disk by
  sha-embedding release versions (``releases/<ver>/release.json``).
- Taxonomy (exactly three verdicts, precedence in this order):
  ``detector-born`` (the fix's card lineage shows a detector finding — the
  router's ``harness-hkrc-impl:<fingerprint>`` idempotency key, or a card whose
  text cites the harness-loop ledger finding that motivated it),
  ``ledger-miss`` (an open ledger finding covers the same pattern and predates
  the merge; the ledger was ahead by N nights and the fix shipped anyway),
  ``blind-spot`` (no detector lineage, no ledger coverage).
- A blind spot writes a typed ledger entry into the SAME ``open_findings``
  ledger with ``fix_status="requirement"`` — outside the harness loop's open
  working set, exactly like the dormant ``monitor`` bookkeeping entries — so a
  requirement can never surface as a finding, consume ranking/apply budget, or
  route a ticket.  Gap cards are human-promotion only: this module has NO
  card-creation path at all (the fixture test asserts it).
- Any failure degrades to a report section; the pre-pass never blocks the loop
  and never blocks a deploy.
- Stdlib only, read-only against git and the native board database.  The only
  writes are the typed ledger entries (live runs only, never --dry-run).

Contract note (t_85199c5e): the locked taxonomy defines ``ledger-miss`` as an
open finding "present >= 1 night before the fix merge".  A same-pattern finding
whose first night IS the merge night is classified ``ledger-miss`` with
``nights_early == 0`` rather than ``blind-spot``: the pattern demonstrably has
ledger coverage, so writing a ``detector_requirement`` for it would be false
and would waste the human promotion queue.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import copy
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import subprocess
from typing import Any, NamedTuple

SECONDS_PER_NIGHT = 86400
ROLLING_WINDOW_DAYS = 30
RETRO_SECTION_TITLE = "Shipped-fix retro (detector coverage, report-only)"

# The ticket router's implementation-card idempotency key
# (``harness_loop._route_hkrc``): ``harness-hkrc-impl:<fingerprint>``.
DETECTOR_IMPL_KEY_PREFIX = "harness-hkrc-impl:"

# Typed ledger entry written for a blind spot.
DETECTOR_REQUIREMENT_KIND = "detector_requirement"
DETECTOR_REQUIREMENT_STATUS = "requirement"
DETECTOR_REQUIREMENT_PREFIX = "detector-requirement:"
SHIPPED_FIX_SIGNAL_SOURCE = "shipped-fix-retro"
BACKFILL_MARKER = True

VERDICT_DETECTOR_BORN = "detector-born"
VERDICT_LEDGER_MISS = "ledger-miss"
VERDICT_BLIND_SPOT = "blind-spot"
VERDICTS = (VERDICT_DETECTOR_BORN, VERDICT_LEDGER_MISS, VERDICT_BLIND_SPOT)

# Ledger entries that are bookkeeping rather than findings: never part of the
# cross-reference vocabulary (the ``monitor`` precedent, plus this module's own
# requirement rows, which would otherwise bootstrap fake coverage).
_NON_FINDING_STATUSES = frozenset({"monitor", DETECTOR_REQUIREMENT_STATUS})

# Merge subject shapes: ``merge: <what> (kanban t_8fd88ee0)`` — the canonical
# shape — plus the multi-id and review/impl variants already on main, e.g.
# ``merge: ... (kanban t_198432fc, t_93d60f39, t_29aad4d5)`` and
# ``merge: ... (review t_e1f4c8be, impl t_2a1dc07d, DEF-t_2a1dc07d-1)``.
_MERGE_PREFIX = "merge:"
_TASK_ID_RE = re.compile(r"\bt_[0-9a-z]{8}\b")
_REVERT_SUBJECT_RE = re.compile(r'^Revert "merge:')

_MAX_FIX_LINES = 5
# Evidence lines kept per requirement entry, newest discovery appended last.
_MAX_EVIDENCE_LINES = 8
_MIN_LEDGER_KEY_CHARS = 8


class ShippedFixRetroError(RuntimeError):
    """The shipped-fix retro pre-pass could not complete."""


class _RunResult(NamedTuple):
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class ShippedFix:
    """One merge commit carrying kanban task references (a shipped fix)."""

    sha: str
    ts: int
    subject: str
    task_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CardLineage:
    """The board card that drove a shipped fix."""

    task_id: str
    title: str
    body: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class FixVerdict:
    """The classification of exactly one shipped fix."""

    sha: str
    ts: int
    subject: str
    verdict: str
    pattern: str = ""
    fingerprint: str = ""
    nights_early: int | None = None
    candidate_detector: str = ""
    deploy_version: str = ""
    deployed_at: int | None = None

    @property
    def deployed(self) -> bool:
        return bool(self.deploy_version)


@dataclass(frozen=True, slots=True)
class RetroResult:
    """One pre-pass run: verdicts, metrics, and the rendered section lines."""

    verdicts: tuple[FixVerdict, ...]
    since: int
    now: int
    # Register-wide counts (every classified fix, not just the 30d window).
    detector_born: int
    ledger_miss: int
    blind_spot: int
    # Rolling 30d window the two headline metrics are computed over.
    window_total: int
    window_detector_born: int
    detector_caught_share: float | None
    median_nights_early: float | None
    requirements_written: int = 0
    degraded: str = ""

    @property
    def total(self) -> int:
        return len(self.verdicts)


# --- register builder -------------------------------------------------------


def parse_register(log_output: str) -> tuple[ShippedFix, ...]:
    """Parse ``git log --merges --format=%ct|%h|%s`` into shipped fixes.

    Kept: merge commits (subject prefixed ``merge:``) that reference at least
    one kanban ``t_`` id.  Dropped: non-merge commits, branch-merge subjects
    with no task reference, and reverts of kanban merges (the revert moves the
    work OFF main — it is not a shipped fix).  Order is git log order
    (newest first); duplicate shas collapse.
    """
    fixes: list[ShippedFix] = []
    seen: set[str] = set()
    for line in (log_output or "").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 2)
        if len(parts) < 3:
            continue
        try:
            ts = int(parts[0].strip())
        except ValueError:
            continue
        sha = parts[1].strip()
        subject = parts[2].strip()
        if not sha or sha in seen:
            continue
        if not subject.startswith(_MERGE_PREFIX):
            continue
        if _REVERT_SUBJECT_RE.match(subject):
            continue
        task_ids = tuple(dict.fromkeys(_TASK_ID_RE.findall(subject)))
        if not task_ids:
            continue
        seen.add(sha)
        fixes.append(
            ShippedFix(sha=sha, ts=ts, subject=subject, task_ids=task_ids)
        )
    return tuple(fixes)


def default_run(argv: Sequence[str]) -> _RunResult:
    """Run one subprocess read-only; the harness loop injects its own runner.

    The harness loop passes an adapter over its own ``_run`` (same env and
    timeout discipline) so tests and the cron path stay on one code path; this
    fallback exists for standalone use and keeps the failure modes identical
    (never a raise, always a ``returncode``).
    """
    try:
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return _RunResult(124, "", "timeout")
    except OSError as exc:
        return _RunResult(127, "", str(exc))
    return _RunResult(
        completed.returncode, completed.stdout or "", completed.stderr or ""
    )


def git_merge_log(
    repo: Path,
    since: int | None,
    *,
    run_fn: Callable[[Sequence[str]], Any] | None = None,
) -> str:
    """Read-only ``git -C <repo> log --merges`` (optionally ``--since``)."""
    argv = [
        "git",
        "-C",
        str(repo),
        "log",
        "--merges",
        "--format=%ct|%h|%s",
    ]
    if since is not None:
        argv.append(
            "--since="
            + datetime.fromtimestamp(int(since), tz=timezone.utc).isoformat()
        )
    result = (run_fn or default_run)(argv)
    if int(getattr(result, "returncode", 1)) != 0:
        detail = (
            getattr(result, "stderr", "") or getattr(result, "stdout", "") or ""
        ).strip()
        raise ShippedFixRetroError(f"git merge log failed in {repo}: {detail}")
    return str(getattr(result, "stdout", "") or "")


# --- evidence loaders (read-only) ------------------------------------------


def default_board_db(native_boards_root: Path, board: str = "hkrc") -> Path:
    """Native kanban database for the board the ticket router writes."""
    return Path(native_boards_root) / board / "kanban.db"


def load_card_lineage(
    board_db: Path, task_ids: Sequence[str]
) -> dict[str, CardLineage]:
    """Read the cards behind the merge's task references (read-only, fail-safe).

    An unreadable or missing board database returns ``{}`` — the classifier
    then has no lineage and falls through to the ledger cross-reference, which
    is the fail-safe direction (never a fabricated detector-born).
    """
    ids = [str(task_id) for task_id in dict.fromkeys(task_ids) if task_id]
    path = Path(board_db)
    if not ids or not path.is_file():
        return {}
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    except sqlite3.Error:
        return {}
    try:
        connection.execute("PRAGMA query_only = ON")
        placeholders = ",".join("?" for _ in ids)
        rows = connection.execute(
            "select id, title, body, idempotency_key from tasks "
            f"where id in ({placeholders})",
            ids,
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        connection.close()
    lineage: dict[str, CardLineage] = {}
    for row in rows:
        task_id = str(row[0])
        lineage[task_id] = CardLineage(
            task_id=task_id,
            title=str(row[1] or ""),
            body=str(row[2] or ""),
            idempotency_key=str(row[3] or ""),
        )
    return lineage


def ledger_findings(
    entries: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Filter ledger rows down to findings usable for coverage cross-reference.

    Bookkeeping rows (``monitor`` streaks and this module's own
    ``requirement`` rows) are dropped.  Current ``fix_status`` is deliberately
    NOT filtered: the finding that a shipped fix resolved is ``resolved`` in
    today's ledger, and it was open when the fix landed — dropping it would
    erase exactly the coverage this metric measures.
    """
    out: list[Mapping[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        if str(entry.get("fix_status", "open")) in _NON_FINDING_STATUSES:
            continue
        if not str(entry.get("fingerprint", "")).strip():
            continue
        out.append(entry)
    return tuple(out)


def load_ledger_history(state_path: Path) -> tuple[Mapping[str, Any], ...]:
    """Union of the live ledger and every ledger backup, deduped by fingerprint.

    Backups are the only surviving record of entries a later prune removed:
    the depth bound this pre-pass can honestly claim is "the earliest backup
    still on disk".  On a fingerprint collision the EARLIEST ``first_seen``
    wins (the earliest proof of coverage), which is what the nights-early
    metric needs.
    """
    path = Path(state_path)
    candidates = [path]
    if path.parent.is_dir():
        candidates.extend(
            sorted(path.parent.glob(f"{path.stem}.backup-*{path.suffix}"))
            + sorted(path.parent.glob(f"{path.stem}.bak-*"))
        )
    merged: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        for entry in _read_ledger_entries(candidate):
            fp = str(entry.get("fingerprint", ""))
            if not fp:
                continue
            previous = merged.get(fp)
            if previous is None or _first_seen(entry) < _first_seen(previous):
                merged[fp] = entry
    return tuple(merged[fp] for fp in sorted(merged))


def _read_ledger_entries(path: Path) -> tuple[Mapping[str, Any], ...]:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ()
    if not isinstance(raw, dict):
        return ()
    entries = raw.get("open_findings")
    if not isinstance(entries, list):
        return ()
    return tuple(entry for entry in entries if isinstance(entry, dict))


def _first_seen(entry: Mapping[str, Any]) -> int:
    try:
        return max(0, int(entry.get("first_seen", 0) or 0))
    except (TypeError, ValueError):
        return 0


def load_release_index(release_root: Path) -> tuple[tuple[str, int], ...]:
    """``(version, installed_at)`` for every materialized release on disk."""
    root = Path(release_root)
    if not root.is_dir():
        return ()
    index: list[tuple[str, int]] = []
    for entry in sorted(root.iterdir()):
        payload = entry / "release.json"
        if not payload.is_file():
            continue
        try:
            data = json.loads(payload.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        version = str(data.get("version") or entry.name)
        try:
            installed_at = int(data.get("installed_at") or 0)
        except (TypeError, ValueError):
            installed_at = 0
        index.append((version, installed_at))
    return tuple(index)


def _deployed_for(
    sha: str, ts: int, index: Sequence[tuple[str, int]]
) -> tuple[str, int | None]:
    """Join merge -> deploy from the sha embedded in the release version.

    Prefers the earliest release installed at or after the merge (a release
    built before the merge cannot carry it); falls back to the latest release
    naming the sha when none postdates the merge.  ``("", None)`` means no
    on-disk release carries the sha yet — metadata only, never a failure.
    """
    if not sha:
        return "", None
    matches = [(version, installed) for version, installed in index if sha in version]
    if not matches:
        return "", None
    after = [pair for pair in matches if pair[1] and pair[1] >= int(ts)]
    if after:
        return min(after, key=lambda pair: (pair[1], pair[0]))
    return max(matches, key=lambda pair: (pair[1], pair[0]))


# --- classifier -------------------------------------------------------------


def _pattern_in_text(pattern: str, text: str) -> bool:
    """Word-boundary, case-insensitive pattern mention (hyphens count as word)."""
    if not pattern:
        return False
    return (
        re.search(
            rf"(?<![a-z0-9-]){re.escape(pattern.casefold())}(?![a-z0-9-])",
            text.casefold(),
        )
        is not None
    )


def _patterns_in_text(
    text: str, findings: Sequence[Mapping[str, Any]]
) -> tuple[str, ...]:
    """Ledger pattern vocabulary mentioned in ``text``, longest first."""
    patterns = {
        str(entry.get("pattern", "")).strip() for entry in findings
    } - {""}
    matched = [pattern for pattern in patterns if _pattern_in_text(pattern, text)]
    return tuple(sorted(matched, key=lambda pattern: (-len(pattern), pattern)))


def detector_lineage(
    fix: ShippedFix,
    *,
    lineage: Mapping[str, CardLineage],
    findings: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    """Return ``(fingerprint, pattern)`` when the fix originated from a finding.

    Two deterministic signals, in order:

    1. the card's idempotency key is the router's detector key
       ``harness-hkrc-impl:<fingerprint>`` — the harness created the card from
       the finding;
    2. the card text (title + body) cites a ledger finding verbatim — an exact
       ``<pattern>:<key>`` fingerprint, or the finding's ``key`` together with
       its pattern name.  That is the "supervisor-created detector card" shape:
       the supervisor tick authored the card from the ledger even though the
       creator field says ``user``.
    """
    for task_id in fix.task_ids:
        card = lineage.get(task_id)
        if card is None:
            continue
        key = card.idempotency_key.strip()
        if key.startswith(DETECTOR_IMPL_KEY_PREFIX):
            fingerprint = key[len(DETECTOR_IMPL_KEY_PREFIX) :].strip()
            if fingerprint:
                return fingerprint, fingerprint.split(":", 1)[0]
        text = f"{card.title}\n{card.body}"
        for entry in findings:
            fingerprint = str(entry.get("fingerprint", ""))
            pattern = str(entry.get("pattern", "")).strip()
            if fingerprint and fingerprint in text:
                return fingerprint, pattern or fingerprint.split(":", 1)[0]
            entry_key = str(entry.get("key", "")).strip()
            if (
                len(entry_key) >= _MIN_LEDGER_KEY_CHARS
                and entry_key in text
                and _pattern_in_text(pattern, text)
            ):
                return fingerprint, pattern
    return "", ""


def _matching_coverage(
    pattern: str, fix_ts: int, findings: Sequence[Mapping[str, Any]]
) -> tuple[int, str] | None:
    """``(nights_early, fingerprint)`` for the earliest same-pattern finding.

    Only findings that predate the merge count as coverage; the earliest one
    gives the largest (and most generous) nights-early.  ``None`` means the
    pattern has no ledger coverage at all before the fix landed.
    """
    candidates = [
        entry
        for entry in findings
        if str(entry.get("pattern", "")).strip() == pattern
        and 0 < _first_seen(entry) <= int(fix_ts)
    ]
    if not candidates:
        return None
    earliest = min(candidates, key=_first_seen)
    nights = (int(fix_ts) - _first_seen(earliest)) // SECONDS_PER_NIGHT
    return int(nights), str(earliest.get("fingerprint", ""))


def _fix_tokens(subject: str) -> tuple[str, ...]:
    """Deterministic subject slug tokens (conventional prefix and task refs dropped)."""
    text = subject.split("(", 1)[0]
    head, _, rest = text.partition(":")
    if rest and head.strip().casefold() in {
        "merge", "fix", "feat", "docs", "test", "chore", "refactor",
        "perf", "build", "ci", "revert",
    }:
        text = rest
    return tuple(
        token for token in re.split(r"[^a-z0-9]+", text.casefold()) if token
    )


def candidate_detector_name(fix: ShippedFix) -> tuple[str, str]:
    """``(pattern, detector name)`` proposed for a blind-spot fix.

    Deterministic and bounded: the fix subject slug is the only pattern signal
    a blind spot has (if the ledger already covered the pattern it would be a
    ledger-miss), and the detector name follows the existing ``detect_*``
    convention so the human promotion queue gets a ready-to-file suggestion.
    """
    tokens = _fix_tokens(fix.subject)[:5]
    if not tokens:
        return f"unclassified-{fix.sha}", f"detect_unclassified_{fix.sha}"
    return "-".join(tokens), "detect_" + "_".join(tokens)


def classify_fix(
    fix: ShippedFix,
    *,
    lineage: Mapping[str, CardLineage] = {},
    findings: Sequence[Mapping[str, Any]] = (),
    release_index: Sequence[tuple[str, int]] = (),
) -> FixVerdict:
    """Classify one shipped fix into exactly one of the three verdicts."""
    deploy_version, deployed_at = _deployed_for(fix.sha, fix.ts, release_index)
    fingerprint, pattern = detector_lineage(
        fix, lineage=lineage, findings=findings
    )
    if fingerprint:
        return FixVerdict(
            sha=fix.sha,
            ts=fix.ts,
            subject=fix.subject,
            verdict=VERDICT_DETECTOR_BORN,
            pattern=pattern,
            fingerprint=fingerprint,
            deploy_version=deploy_version,
            deployed_at=deployed_at,
        )
    text = "\n".join(
        [fix.subject]
        + [
            f"{card.title}\n{card.body}"
            for task_id in fix.task_ids
            if (card := lineage.get(task_id)) is not None
        ]
    )
    for candidate in _patterns_in_text(text, findings):
        coverage = _matching_coverage(candidate, fix.ts, findings)
        if coverage is None:
            continue
        nights, covered_fp = coverage
        return FixVerdict(
            sha=fix.sha,
            ts=fix.ts,
            subject=fix.subject,
            verdict=VERDICT_LEDGER_MISS,
            pattern=candidate,
            fingerprint=covered_fp,
            nights_early=nights,
            deploy_version=deploy_version,
            deployed_at=deployed_at,
        )
    pattern, detector = candidate_detector_name(fix)
    return FixVerdict(
        sha=fix.sha,
        ts=fix.ts,
        subject=fix.subject,
        verdict=VERDICT_BLIND_SPOT,
        pattern=pattern,
        candidate_detector=detector,
        deploy_version=deploy_version,
        deployed_at=deployed_at,
    )


# --- metrics ----------------------------------------------------------------


def verdict_counts(verdicts: Sequence[FixVerdict]) -> tuple[int, int, int]:
    """``(detector_born, ledger_miss, blind_spot)`` over the whole register."""
    return (
        sum(1 for verdict in verdicts if verdict.verdict == VERDICT_DETECTOR_BORN),
        sum(1 for verdict in verdicts if verdict.verdict == VERDICT_LEDGER_MISS),
        sum(1 for verdict in verdicts if verdict.verdict == VERDICT_BLIND_SPOT),
    )


def _median(values: Sequence[int]) -> float | None:
    if not values:
        return None
    ordered = sorted(int(value) for value in values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


def rolling_metrics(
    verdicts: Sequence[FixVerdict], *, now: int
) -> tuple[int, int, int, int, float | None, float | None]:
    """``(window_total, detector_born, ledger_miss, blind_spot, share, median)``.

    Rolling 30-day window anchored on the run time: the numerator and the
    denominator of the share MUST come from the same window, otherwise a
    backfill over 45 days of history reports a share that does not add up.
    An empty window yields ``share=None`` (an explicit empty state, never a
    divide-by-zero or a fake 0%) and ``median=None`` when no ledger-miss
    landed in the window.
    """
    start = int(now) - ROLLING_WINDOW_DAYS * SECONDS_PER_NIGHT
    window = [verdict for verdict in verdicts if int(verdict.ts) >= start]
    detector_born = sum(
        1 for verdict in window if verdict.verdict == VERDICT_DETECTOR_BORN
    )
    ledger_miss = sum(
        1 for verdict in window if verdict.verdict == VERDICT_LEDGER_MISS
    )
    blind_spot = sum(
        1 for verdict in window if verdict.verdict == VERDICT_BLIND_SPOT
    )
    share = (detector_born / len(window)) if window else None
    window_total = len(window)
    median = _median(
        [
            int(verdict.nights_early)
            for verdict in window
            if verdict.verdict == VERDICT_LEDGER_MISS
            and verdict.nights_early is not None
        ]
    )
    return window_total, detector_born, ledger_miss, blind_spot, share, median


def classify_register(
    fixes: Sequence[ShippedFix],
    *,
    board_db: Path | None = None,
    findings: Sequence[Mapping[str, Any]] = (),
    release_index: Sequence[tuple[str, int]] = (),
) -> tuple[FixVerdict, ...]:
    """Classify a whole register; one read-only lineage query per fix batch."""
    lineage: dict[str, CardLineage] = {}
    if board_db is not None:
        task_ids = [task_id for fix in fixes for task_id in fix.task_ids]
        lineage = load_card_lineage(board_db, task_ids)
    return tuple(
        classify_fix(
            fix,
            lineage=lineage,
            findings=findings,
            release_index=release_index,
        )
        for fix in fixes
    )


# --- ledger writes (human-promotion queue; never a card) --------------------


def _verdict_evidence(verdict: FixVerdict) -> str:
    stamp = datetime.fromtimestamp(int(verdict.ts), tz=timezone.utc).isoformat()
    return (
        f"shipped fix {verdict.sha} at {stamp} had no detector lineage and no "
        f"ledger coverage: {verdict.subject}"
    )


def detector_requirement_entry(
    pattern: str,
    verdicts: Sequence[FixVerdict],
    *,
    now: int,
    backfill: bool = False,
) -> dict:
    """The typed ledger entry a blind spot writes — ONE entry per pattern.

    Pattern-level, not fix-level: the decision a human promotes is "this
    pattern needs a detector", so one row per pattern keeps the queue
    actionable and the shipped-fix shas land as evidence.  A re-run for the
    same pattern updates the existing row instead of appending a duplicate.

    ``fix_status="requirement"`` keeps the row OUT of the harness loop's open
    working set (the dormant ``monitor`` precedent): it never surfaces as a
    finding, never consumes ranking or apply budget, and can never route a
    ticket.  The ``kind`` marker plus ``signal_source`` / ``pattern`` /
    ``candidate_detector`` are what the human promotion queue reads back.
    """
    ordered = sorted(verdicts, key=lambda verdict: (int(verdict.ts), verdict.sha))
    first = ordered[0]
    entry = {
        "kind": DETECTOR_REQUIREMENT_KIND,
        "fingerprint": f"{DETECTOR_REQUIREMENT_PREFIX}{pattern}",
        "pattern": pattern,
        "key": pattern,
        "severity": "low",
        "evidence": [_verdict_evidence(verdict) for verdict in ordered[:5]],
        "suggestion": (
            f"candidate detector {first.candidate_detector} — human-authored; "
            "HKRC proposes, the operator decides"
        ),
        "signal_source": SHIPPED_FIX_SIGNAL_SOURCE,
        "candidate_detector": first.candidate_detector,
        "fix_status": DETECTOR_REQUIREMENT_STATUS,
        "first_seen": int(first.ts),
        "last_seen": int(now),
        "occurrence_count": len(ordered),
        "requirement_state": "proposed",
        "apply_kind": "none",
    }
    if backfill:
        entry["backfill"] = BACKFILL_MARKER
    return entry


def write_detector_requirements(
    state: dict,
    verdicts: Sequence[FixVerdict],
    *,
    now: int,
    backfill: bool = False,
) -> tuple[dict, ...]:
    """Upsert one typed requirement entry per blind-spot pattern; no card path.

    A pattern absent from the ledger appends one entry; a pattern already
    queued has its evidence extended (line-deduped, capped) and its
    ``occurrence_count`` / ``last_seen`` bumped, so repeated nights never grow
    duplicate rows.  Returns only the NEWLY created entries.  Mutates
    ``state['open_findings']`` in place (persist with the harness loop's
    ``save_state``).  There is no card-creation path here by design: gap cards
    are human-promotion only (HKRC proposes, the operator decides).
    """
    entries = state.get("open_findings")
    if not isinstance(entries, list):
        entries = []
        state["open_findings"] = entries
    by_fp = {
        str(entry.get("fingerprint", "")): entry
        for entry in entries
        if isinstance(entry, dict)
    }
    grouped: dict[str, list[FixVerdict]] = {}
    for verdict in verdicts:
        if verdict.verdict == VERDICT_BLIND_SPOT:
            grouped.setdefault(verdict.pattern, []).append(verdict)
    created: list[dict] = []
    for pattern in sorted(grouped):
        entry = detector_requirement_entry(
            pattern, grouped[pattern], now=now, backfill=backfill
        )
        existing = by_fp.get(entry["fingerprint"])
        if existing is None:
            entries.append(entry)
            by_fp[entry["fingerprint"]] = entry
            created.append(entry)
            continue
        evidence = existing.get("evidence")
        if not isinstance(evidence, list):
            evidence = []
        for line in entry["evidence"]:
            if line not in evidence:
                evidence.append(line)
        existing["evidence"] = evidence[-_MAX_EVIDENCE_LINES:]
        existing["last_seen"] = int(now)
        existing["occurrence_count"] = int(
            existing.get("occurrence_count", 0) or 0
        ) + len(grouped[pattern])
        if backfill:
            existing["backfill"] = BACKFILL_MARKER
    return tuple(created)


def detector_requirements(state: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Read back the typed requirement entries (the promotion queue)."""
    entries = state.get("open_findings")
    if not isinstance(entries, list):
        return ()
    return tuple(
        entry
        for entry in entries
        if isinstance(entry, Mapping)
        and entry.get("kind") == DETECTOR_REQUIREMENT_KIND
    )


# --- pre-pass + renderer ----------------------------------------------------


def run_prepass(
    *,
    repo: Path,
    since: int,
    now: int,
    ledger_entries: Sequence[Mapping[str, Any]] = (),
    board_db: Path | None = None,
    release_root: Path | None = None,
    run_fn: Callable[[Sequence[str]], Any] | None = None,
) -> RetroResult:
    """Build the register since ``since`` and classify every fix in it."""
    log_output = git_merge_log(repo, since, run_fn=run_fn)
    fixes = parse_register(log_output)
    findings = ledger_findings(ledger_entries)
    release_index = load_release_index(release_root) if release_root else ()
    verdicts = classify_register(
        fixes,
        board_db=board_db,
        findings=findings,
        release_index=release_index,
    )
    window_total, window_born, _, _, share, median = rolling_metrics(
        verdicts, now=now
    )
    born, miss, blind = verdict_counts(verdicts)
    return RetroResult(
        verdicts=verdicts,
        since=int(since),
        now=int(now),
        detector_born=born,
        ledger_miss=miss,
        blind_spot=blind,
        window_total=window_total,
        window_detector_born=window_born,
        detector_caught_share=share,
        median_nights_early=median,
    )


def _percent(share: float | None) -> str:
    return "n/a (0 shipped fixes in window)" if share is None else f"{share:.0%}"


def _nights(value: float | None) -> str:
    if value is None:
        return "n/a (0 ledger-misses in window)"
    return f"{value:g} night(s)"


def _deploy_label(verdict: FixVerdict) -> str:
    if verdict.deployed:
        return f"deployed {verdict.deploy_version}"
    return "not deployed yet (no on-disk release carries this sha)"


def _verdict_line(verdict: FixVerdict) -> str:
    head = f"{verdict.sha} {verdict.verdict}"
    if verdict.verdict == VERDICT_DETECTOR_BORN:
        detail = (
            f"pattern {verdict.pattern or 'unknown'} "
            f"[{verdict.fingerprint}]; {_deploy_label(verdict)}"
        )
    elif verdict.verdict == VERDICT_LEDGER_MISS:
        detail = (
            f"pattern {verdict.pattern} was in the ledger "
            f"{verdict.nights_early if verdict.nights_early is not None else 0} "
            f"night(s) before this fix shipped "
            f"[{verdict.fingerprint}]; {_deploy_label(verdict)}"
        )
    else:
        detail = (
            f"no detector lineage and no ledger coverage; "
            f"detector_requirement queued for pattern {verdict.pattern} "
            f"(candidate {verdict.candidate_detector}); {_deploy_label(verdict)}"
        )
    return f"{head} — {detail}"


def degraded_lines(reason: object) -> tuple[str, ...]:
    """The report section on any pre-pass failure (never a raise, never a block)."""
    return (
        f"unavailable this run — {reason} "
        "(report-only: the deterministic report, the loop outcome, and any "
        "deploy are unaffected)",
    )


def render_lines(
    result: RetroResult,
    *,
    requirements_written: int | None = None,
    scope_label: str = "since last run",
    dry_run: bool = False,
) -> tuple[str, ...]:
    """Render the retro report section (plain text, Telegram-friendly).

    ``requirements_written`` overrides the pre-pass's own count: the harness
    loop writes the blind-spot entries in the same run (live only, after this
    classification), so only the caller knows how many landed.  ``dry_run``
    only changes the wording of that count — a preview must never report
    ledger writes it did not perform.
    """
    if result.degraded:
        return degraded_lines(result.degraded)
    if not result.verdicts:
        since = datetime.fromtimestamp(result.since, tz=timezone.utc).isoformat()
        return (
            f"no merges {scope_label} ({since}): empty state — "
            "nothing to classify",
        )
    written = (
        result.requirements_written
        if requirements_written is None
        else int(requirements_written)
    )
    lines = [
        f"{result.total} shipped fix(es) {scope_label}: "
        f"{result.detector_born} detector-born, {result.ledger_miss} "
        f"ledger-miss, {result.blind_spot} blind-spot; "
        f"{written} detector_requirement entry(ies) "
        f"{'WOULD be written (dry run: ledger untouched)' if dry_run else 'written'}"
    ]
    for verdict in result.verdicts[:_MAX_FIX_LINES]:
        lines.append(_verdict_line(verdict))
    if result.total > _MAX_FIX_LINES:
        lines.append(f"(+{result.total - _MAX_FIX_LINES} more shipped fix(es))")
    lines.append(
        "detector-caught share (rolling 30d): "
        f"{_percent(result.detector_caught_share)} "
        f"({result.window_detector_born} detector-born / "
        f"{result.window_total} shipped fixes in window)"
    )
    lines.append(
        "median nights-early (ledger-miss, rolling 30d): "
        f"{_nights(result.median_nights_early)}"
    )
    return tuple(lines)


# --- one-shot historical backfill -------------------------------------------


def _iso(ts: int | None) -> str:
    if not ts:
        return "unknown"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()


def ledger_depth_bounds(state_path: Path) -> str:
    """Honest statement of how far back the ledger evidence actually reaches."""
    path = Path(state_path)
    backups = sorted(path.parent.glob(f"{path.stem}.backup-*{path.suffix}"))
    parts = []
    if path.is_file():
        parts.append("live ledger")
    if backups:
        parts.append(
            f"{len(backups)} backup(s) {backups[0].name} .. {backups[-1].name}"
        )
    if not parts:
        return (
            "no ledger snapshot readable: every fix classifies as blind-spot "
            "(zero coverage evidence)"
        )
    return (
        ", ".join(parts)
        + " — findings whose first_seen predates the earliest readable "
        "snapshot are only visible if they survive in the live ledger, so "
        "nights-early and detector-caught share for earlier fixes are LOWER "
        "BOUNDS"
    )


def run_backfill(
    *,
    repo: Path,
    state: dict,
    state_path: Path,
    now: int,
    board_db: Path | None = None,
    release_root: Path | None = None,
    run_fn: Callable[[Sequence[str]], Any] | None = None,
    dry_run: bool = False,
) -> str:
    """One-shot historical classification over surviving git + ledger history.

    Same classifier as the nightly pre-pass; the only difference is the
    absence of a ``--since`` anchor and the ``backfill: true`` marker on the
    entries written.  Returns the report section text (printable as-is).

    ``dry_run`` (the caller's default) mutates a deep copy of ``state`` so the
    caller's ledger stays byte-identical, and the report says how many entries
    a live run WOULD add instead of claiming writes that never happened.
    """
    log_output = git_merge_log(repo, None, run_fn=run_fn)
    fixes = parse_register(log_output)
    findings = ledger_findings(load_ledger_history(state_path))
    release_index = load_release_index(release_root) if release_root else ()
    verdicts = classify_register(
        fixes,
        board_db=board_db,
        findings=findings,
        release_index=release_index,
    )
    window_total, window_born, _, _, share, median = rolling_metrics(
        verdicts, now=now
    )
    detector_born, ledger_miss, blind_spot = verdict_counts(verdicts)
    written = write_detector_requirements(
        copy.deepcopy(state) if dry_run else state,
        verdicts,
        now=now,
        backfill=True,
    )
    result = RetroResult(
        verdicts=verdicts,
        since=min((verdict.ts for verdict in verdicts), default=0),
        now=int(now),
        detector_born=detector_born,
        ledger_miss=ledger_miss,
        blind_spot=blind_spot,
        window_total=window_total,
        window_detector_born=window_born,
        detector_caught_share=share,
        median_nights_early=median,
        requirements_written=len(written),
    )
    oldest = _iso(min((verdict.ts for verdict in verdicts), default=0))
    newest = _iso(max((verdict.ts for verdict in verdicts), default=0))
    header = (
        f"Backfill: {result.total} shipped fix(es) in git history "
        f"({oldest} .. {newest}); one-shot, same classifier as the nightly "
        "pre-pass; entries are marked backfill: true and land only with "
        "--no-dry-run"
    )
    depth = f"backfill depth bounds: {ledger_depth_bounds(state_path)}"
    return "\n".join(
        [
            header,
            depth,
            *render_lines(
                result,
                scope_label="in git history",
                requirements_written=len(written),
                dry_run=dry_run,
            ),
        ]
    )


__all__ = [
    "BACKFILL_MARKER",
    "CardLineage",
    "DETECTOR_IMPL_KEY_PREFIX",
    "DETECTOR_REQUIREMENT_KIND",
    "DETECTOR_REQUIREMENT_PREFIX",
    "DETECTOR_REQUIREMENT_STATUS",
    "FixVerdict",
    "RETRO_SECTION_TITLE",
    "ROLLING_WINDOW_DAYS",
    "RetroResult",
    "SECONDS_PER_NIGHT",
    "SHIPPED_FIX_SIGNAL_SOURCE",
    "ShippedFix",
    "ShippedFixRetroError",
    "VERDICT_BLIND_SPOT",
    "VERDICT_DETECTOR_BORN",
    "VERDICT_LEDGER_MISS",
    "VERDICTS",
    "candidate_detector_name",
    "classify_fix",
    "classify_register",
    "default_board_db",
    "default_run",
    "degraded_lines",
    "detector_lineage",
    "detector_requirement_entry",
    "detector_requirements",
    "git_merge_log",
    "ledger_depth_bounds",
    "ledger_findings",
    "load_card_lineage",
    "load_ledger_history",
    "load_release_index",
    "parse_register",
    "render_lines",
    "rolling_metrics",
    "run_backfill",
    "run_prepass",
    "verdict_counts",
    "write_detector_requirements",
]
