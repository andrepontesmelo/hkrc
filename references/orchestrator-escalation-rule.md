# Orchestrator escalation rule — retry-exhausted cards

Status: in-repo reference for the operator. The rule text below is applied to
`personas/senior-dev/SOUL.md` in the skills-dist repository via dist-sync
(operator-controlled — HKRC never writes the SOUL.md itself; this card and the
loop only deliver the text).

APPLIED BY: OPERATOR via dist-sync. Never auto-applied by HKRC or the loop.

Source: decision `t_9f7cf77a` (Andre, 2026-08-14) — re-dispatch retry-exhausted
cards DIRECTLY to senior-dev, skipping the lead-orchestrator hop. Supersedes
`t_d2fb8917` #525 (lead-orchestrator decides rescue via senior-dev / DROP /
other). senior-dev SOUL.md was updated the same day to take ANY assigned ticket
to done-with-evidence or blocked-with-reason.

## Trigger

A card is retry-exhausted when the dispatch circuit breaker trips: its
`consecutive_failures` counter reached the effective failure limit — the
per-task `max_retries` override, else the dispatcher `kanban.failure_limit`
config, else the default of 2. The native board records this as a `gave_up`
event on the card, with `failures`, `effective_limit`, and `trigger_outcome`
(spawn_failed | crashed | timed_out) in the payload.

HKRC detects these cards deterministically (same signal, same routing, every
run) and emits a report-only escalation finding routed DIRECTLY to senior-dev,
plus a top line in the harness-loop report. Report-only means live mode never
auto-creates a ticket — the escalation is a deterministic reassignment
(developer -> senior-dev), never orchestrator discretion.

## Rule text (apply verbatim to the senior-dev SOUL.md)

> When you receive an escalated retry-exhausted card, you — senior-dev —
> drive it to a terminal disposition yourself. Escalation order is
> senior-dev DIRECT: the card is re-dispatched/reassigned to you, skipping
> the lead-orchestrator hop. Persona reassignment IS the escalation — no
> per-card model or reasoning overrides; your profile config is the source
> of truth (pro rescue lane). Proceed in this order:
>
> 1. RESCUE — take the card to done-with-evidence (keep its worktree
>    workspace and branch; link a paired review card if one does not exist).
> 2. DROP — a terminal disposition ONLY when you block the card with a
>    precise reason; never drop silently.
>
> Record your disposition on the card (a comment with the decision and the
> rationale) so the loop's audit trail stays complete.

Escalation order: senior-dev DIRECT; no lead-orchestrator intermediate hop.
DROP is valid only as a blocked-with-reason disposition by senior-dev;
silent drops are never an option.
