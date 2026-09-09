# HKRC docs

A deterministic sidecar for the Hermes Kanban workflow: watchdogs observe task
execution and act on catalogued corner cases, plus one daily LLM reflection on
the orchestration layer itself.

## Start here

1. [README](../README.md) — what it does, install, quick start.
2. [Architecture](architecture.md) — daemon, watchdogs, admission, Git
   enforcement, daily reflection: pieces and the decision sequence.
3. [Development](development.md) — repo layout, the local gate
   (`uv run pytest`), conventions, pre-push checklist.

## Reference in this repo

- [Outcome Guard](outcome-guard.md) — contract registration, child admission,
  and the `reference-transaction` hook that enforces protected canonical refs.
- [Persona matrix runbook](persona-matrix-runbook.md) — one-time operator
  profile edits for role/persona drift detection
  (contract: `references/persona-matrix.md`, data: `src/hkrc/persona_matrix.py`).
- [Harness-learning-loop prompt](../references/harness-loop-prompt.md) — the
  verbatim nightly self-review prompt and escalation rule.
- [Orchestrator escalation rule](../references/orchestrator-escalation-rule.md) —
  why stalled decisions get escalated, and how the watchdog cadence is computed.
- [Persona matrix contract](../references/persona-matrix.md) — roles, drift
  classes, evidence shape.

## FAQ

**Does HKRC open or edit Hermes source, config, or task databases?**
Never. That safety boundary is absolute — it learns about boards only through
the native `hermes kanban` CLI as argv subprocesses, and it owns only its
instance-scoped controller state.

**How is a fix verified before the loop closes?**
With `git merge-base --is-ancestor` against the canonical branch — version
claims lie, merge state doesn't.

**What does the daily reflection cost?**
One LLM call per day over the previous day's deterministically collected
metrics. Configuration is the controller config plus the cron cadence (daily
03:00); dry-run is the default until the operator flips it on.
