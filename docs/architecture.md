# Architecture

How the Hermes Kanban Recovery Controller (HKRC) observes boards and acts.
HKRC is a deterministic state machine — never an autonomous LLM agent. The one
exception is the daily reflection: a single LLM call over deterministically
collected metrics.

## One diagram

```
hermes kanban CLI (argv subprocesses, ambient HERMES_KANBAN_* scrubbed)
        │  read-only observation: blocked / review-required / done cards
        ▼
┌──────────────────┐  per-board cursors + action keys
│ tick watchdogs   │  (needs-input, stale-block, review-gap, watcher H1-H4)
│  observe → decide│──▶ idempotent actions: create fix card, complete review,
│  → act (or skip) │    unblock pick-gate card, write missing block event,
└────────┬─────────┘    archive evidence-backed deadlock
         │  admission path only (HKRC-mediated children)
         ▼
┌──────────────────┐  immutable JSON contracts, ancestor-chain validation,
│ Outcome Guard    │  durable authorization evidence
│  admit → enforce │──▶ reference-transaction hook denies unprotected
└────────┬─────────┘    canonical-ref updates (refs/heads/main default)
         │  daily 03:00, one LLM call
         ▼
┌──────────────────┐  dry-run default; live mode applies at most 2
│ harness-loop     │  orchestration/hkrc fixes, the rest route as tickets
│  collect → plan  │
└──────────────────┘
```

## Pieces

- **`src/hkrc/watcher.py` — decision-latency watcher (`hkrc watcher`).**
  Closes the four measured stalls: H1 auto-creates a fix card from a reviewer
  defect block (idempotent per review + block episode); H2 completes the
  original review once the fix is verified merged (`git merge-base
  --is-ancestor`, never a claimed SHA) and promotes gated children; H3 advances
  the pick gate (highest-priority parked `needs_input` card — never when a
  capability-blocked card exists, the card or its parents are on hold, or a
  recent comment says `hold`); H4 writes the missing block event for a task
  created as `blocked` without one (the dispatcher would otherwise
  silently auto-promote it). Plus the review-required deadlock archive:
  a blocked parent whose reason already carries completion evidence strands its
  review child forever, so the watcher archives the parent — only with
  evidence, only with a review child, never otherwise (fail-closed).
- **`src/hkrc/needs_input_watcher.py`, `stale_block_watch.py`,
  `review_gap.py` — cron watchdogs.** Ping when a task waits on human input,
  when a dispatcher death leaves a silent block (no typed blocked event),
  and when a done card is missing its review pair. Silent when nothing new,
  designed for cron `no_agent` delivery.
- **`src/hkrc/outcome_guard.py` + `git_enforce.py` + `admission.py` —
  Outcome Guard.** Operator-registered immutable contracts; HKRC-mediated
  admission creates a child in non-dispatchable `blocked` state, validates the
  requested effect against the governing contract and every ancestor, records
  durable authorization evidence, and promotes only after validation. The
  portable `reference-transaction` hook (Git 2.36+) denies protected
  canonical-ref updates without bound task + `merge_main`-allowing contract +
  required review/terminal evidence. Never opens or mutates a Hermes/Kanban
  SQLite database. See [Outcome Guard](outcome-guard.md).
- **`src/hkrc/harness_loop.py` — daily reflection (`hkrc harness-loop`).**
  Audits the instance's own sessions + boards over a 7-day window, renders the
  self-review report, and (live mode only) applies up to 2
  orchestration/hkrc fixes. Dry-run by default; the cron shim flips
  `--no-dry-run` only after operator review. Prompt verbatim in
  [references/harness-loop-prompt.md](../references/harness-loop-prompt.md).
- **`src/hkrc/config.py`, `state.py`, `crons.py` — instance plumbing.**
  Instance-scoped config + controller-owned SQLite state under the instance
  root; cron-manifest reconciliation. The release never installs, enables, or
  starts a service — that stays an operator action.

## Decision sequence (watcher tick)

1. Tick fires (cron cadence); each watchdog loads its per-board cursor and
   controller-owned state — never native Hermes state files.
2. Observe: list blocked / review-required / done-without-review cards through
   the native CLI.
3. Decide per card: H1 (defect block without fix card → create), H2 (fix
   merged per `merge-base --is-ancestor` → complete + promote), H3 (completed
   task → unblock highest-priority parked pick-gate card if all guards pass),
   H4 (blocked without event → write it), deadlock (evidence + review child
   → archive parent). Anything ambiguous → skip (fail-closed).
4. Act idempotently: per-board cursors + action keys make re-runs safe;
   `--dry-run` reports would-have actions without mutating anything.
5. Daily 03:00 only: the harness loop collects metrics, makes its single LLM
   call, and writes a plan — zero applies until the operator flips it on.
