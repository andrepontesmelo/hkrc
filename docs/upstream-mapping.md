# Upstream mapping — retiring HKRC watchdogs via Hermes fixes

The README endgame says it outright: ideally Hermes Kanban shouldn't need a
sidecar at all — fixes should land in Hermes source so HKRC becomes
unnecessary. Today every catalogued corner case is handled inside HKRC
([Architecture](architecture.md), "Pieces"). This doc is the missing bridge:
one row per catalogued mechanism, saying where the durable fix belongs, and
what must be true before the HKRC watchdog is retired.

Ground rules:

- **Upstream status starts at "not filed" for every row.** No upstream issue
  or PR is part of this mapping; filing anything upstream requires the
  owner's (Andre's) explicit approval and is a separate slice.
- **Root-cause classes:** *Hermes defect* (Hermes contradicts its own
  documented behavior), *Hermes missing feature* (a capability Hermes simply
  has no primitive for), *HKRC-side policy* (a workflow convention this repo
  chose, enforced by an HKRC watchdog), or *unclear*.
- **Retirement criteria** follow one pattern: the fix is merged upstream and
  released in a Hermes version at or above the release carrying it
  (baseline inspected below), HKRC re-verifies in `--dry-run`/monitor mode
  that the corner case no longer occurs over an observation window, and only
  then is the watchdog retired (not merely disabled — removed with its
  tests).

## Basis of this mapping

The grouping follows [Architecture](architecture.md) "Pieces": the decision
latency watcher's four stalls (H1–H4) plus its review-required deadlock
archive (H5 in code), and the three cron watchdogs (`needs-input-watcher`,
`stale-block-watch`, `review-gap`). Eight mechanisms total.

**Hermes source inspected** (this is where the per-row grounding comes from):

- `which hermes` → `~/.local/bin/hermes` (bash wrapper) →
  `~/.hermes/hermes-agent/venv/bin/hermes` → `hermes_cli.main`.
- Package source: the git checkout **`~/.hermes/hermes-agent`**
  (Kanban lives in `hermes_cli/kanban_db*.py`, `gateway/kanban_watchers_*.py`,
  `tools/kanban_tools.py`).
- Version at inspection: **Hermes Agent v0.21.1 (2026.9.7), upstream
  `31d0a242`, local `16cceb8b` (+2 carried commits)** — the local checkout
  carries two local commits on top of upstream, and the install reported
  itself 207 commits behind upstream at inspection time. Hermes line numbers
  below refer to that checkout and will drift; mechanism-level claims will
  not.

## The mapping

| # | Mechanism (what it does) | HKRC anchor | Root cause class | Durable fix (why) | Upstream status |
|---|--------------------------|-------------|------------------|-------------------|-----------------|
| M1 | **H1 fix-card auto-create** — a review blocking with a defect payload (`FIX-READY` / `HIGH/MEDIUM/LOW defect`) gets one remediation card per (review, block episode), severity-derived priority, workspace inherited via review → impl → board fallback | `src/hkrc/watcher.py:490` (`discover_defect_blocks`), `:741` (`plan_fix_card`), `:1778` (`_handle_h1_block`) | Hermes missing feature — `block_task` records the block and stops (`hermes_cli/kanban_db.py:2906`); nothing consumes a block payload to spawn follow-up work. Grep for `FIX-READY` across the Kanban code (`hermes_cli/`, `gateway/`, `tools/`): zero hits; `remediation` matches only non-Kanban modules (e.g. `hermes_cli/security_advisories.py`) | **Hermes core** — a native follow-up/remediation primitive (block-with-child or a task hook that spawns a deduplicated child). The severity and defect payload are already in the block event; only Hermes can spawn and dedupe native children | not filed |
| M2 | **H2 supersede-loop close** — when the fix card's work is verified merged to the canonical branch (`git merge-base --is-ancestor`, never a claimed SHA), the original defect-blocked review is completed as superseded and gated children promote | `src/hkrc/watcher.py:457` (`verify_merged`), `:847` (`discover_supersede_candidates`), `:2038` (`_handle_h2_supersede`) | Hermes missing feature — review completion has no git-truth gate: `complete_task` accepts summaries/metadata without checking merge state, and no supersede relation exists between a defect-blocked review and its fix. Grep for `supersede`/`--is-ancestor` in Kanban code: absent (only unrelated `gitlock`/`banner` uses) | **Hermes core** — validate a `merge_sha` completion contract against git before a review may complete, or a native supersede edge fix↔review. Version claims lie; only the board owner can gate completion on merge state | not filed |
| M3 | **H3 pick-gate auto-advance** — after any task completes, the highest-priority parked `One-at-a-time:` `needs_input` card is unblocked, never when a capability-blocked card exists, the card or its parents are on hold, or a recent comment says `hold` | `src/hkrc/watcher.py:985` (`discover_pick_gate_candidates`), `:1040` (`pick_gate_skip_reason`), `:2297` (`_handle_h3_completed`) | Hermes missing feature — no per-queue serialization primitive: the dispatcher has only global `max_in_progress` / per-profile caps (`resolve_max_in_progress`, `hermes_cli/kanban_db_dispatch.py:1300–1333`; per-profile `max_in_progress_per_profile`, `:81`, `:111`), nothing that parks and orders a queue on the board. The `One-at-a-time:` prefix is an HKRC convention (`src/hkrc/config.py:405`); grep finds it nowhere in Hermes | **Hermes core** — a per-board/queue concurrency limit or native gate object with priority-ordered release. Config can cap concurrency globally but cannot express per-queue order, which is the point of the gate | not filed |
| M4 | **H4 promotable-blocked guard** — a task created `--initial-status blocked` has no `blocked` event, so the dispatcher would silently auto-promote it next tick; the watcher writes the missing block event (two-step unblock→block, since `block_task` only fires from running/ready) | `src/hkrc/watcher.py:1090` (`discover_missing_block_events`), `:2415` (`_handle_h4`) | **Hermes defect (grounded)** — create promises "``initial_status="blocked"`` parks it for human ops" (`hermes_cli/kanban_db.py:1239`) but `initial_task_state` returns `blocked` writing only a `created` event (`hermes_cli/kanban_db_graph.py:45`, event at `kanban_db.py:1349–1352`); `_has_sticky_block` then returns False "when there is no such event at all" (`hermes_cli/kanban_db.py:1958`) and `recompute_ready` promotes the parked task (`kanban_db.py:2007`) | **Hermes core** — write a sticky `blocked` event at create when `initial_status="blocked"` (or let `block_task` stamp an already-blocked task, removing HKRC's unblock→block two-step). The create-time promise and the promotion path disagree inside the same module | not filed |
| M5 | **Review-required deadlock archive** — a blocked `review-required:` parent whose reason carries completion evidence strands its review child forever; the watcher archives the parent (archived counts as satisfied, so the child promotes) — only with evidence, only with a review child | `src/hkrc/watcher.py:1124` (`discover_review_required_deadlocks`), `:2498` (`_handle_h5_deadlock`) | **Hermes defect + missing feature (grounded)** — `recompute_ready` promotes children only of `done`/`archived` parents (`hermes_cli/kanban_db.py:2038`), and a worker-authored block is sticky until explicit unblock (`kanban_db.py:1958`, #28712), so a parent blocked `review-required:` is never terminal and the child never promotes. Hermes already has the right primitive — a first-class review handoff, `request_review` (`kanban_db.py:2997`, running/ready → `review`, native review-lane dispatch at `kanban_db_dispatch.py:1784`, gated by `review_dispatch_enabled()` `:1259`, default on `:1265`) — but workers using block-as-handoff fall out of it | **Hermes core** (two acceptable shapes, owner to pick): teach the promotion gate that a review-handoff block with a review child is satisfied, or make the native `request_review` path cover the paired-review-card workflow so workers never block-as-handoff. Needs Andre for the shape choice | not filed |
| M6 | **needs-input-watcher** — pings the operator (cron `no_agent` digest, episode-deduped, LLM-summarized) when a task's latest blocked episode waits on human input; never pings `review-required:` review gates | `src/hkrc/needs_input_watcher.py:167` (`discover_needs_input`), `:79` (review-gate skip) | Hermes missing feature (partially covered natively) — Hermes *does* notify subscribed origin chats on `blocked` events (`gateway/kanban_watchers_notifier.py` — `TERMINAL_KINDS` `:33` / `_WAKE_KINDS` `:36` include `blocked`, subscriptions inherited to children). What it lacks is board-wide operator coverage: a `needs_input` episode on a card nobody subscribed to is silent. **Needs Andre**: whether the observed unpinged episodes were genuinely un-subscribed (vs. notifier failures) requires the operational history of those boards; that decides board-level-operator-subscription (Hermes feature) vs. config-only (subscribe the operator chat at graph creation) | **Hermes core or config** — pending the history check above: a configurable board-level operator subscription for `needs_input` episodes, with dedupe, would make the pinger redundant | not filed |
| M7 | **stale-block-watch** — pings when a dispatcher death leaves a silent block: `status='blocked'` whose latest event is a death kind (`gave_up`/`spawn_failed`/`timed_out`/`crashed`) with no typed `blocked` event; flags the `--max-runtime 0` config-defect signature with the exact fix | `src/hkrc/stale_block_watch.py:141` (`is_silent_death_block`), `:262` (`discover_silent_death_blocks`), `:59` (`DEATH_KINDS`) | **Hermes defect (grounded, two parts)** — (1) the breaker-trip path sets `status='blocked'` and appends only a `gave_up` event, never a typed `blocked` event (`hermes_cli/kanban_db_dispatch.py:1064–1101`: `UPDATE tasks SET status = 'blocked'` + `_append_event(conn, task_id, "gave_up", ...)`), so every event-driven consumer keyed on `blocked` misses it; (2) create accepts `--max-runtime 0` with no positive-value validation (stored via `_opt_int`, `kanban_db.py:1341`), and every run then SIGTERMs at ~60s (`elapsed 61s > limit 0s` — observed in live operation 2026-08-06, recorded in the HKRC source at `src/hkrc/stale_block_watch.py:7–11` and `:22–24`; not reproducible from Hermes sources) | **Hermes core** — emit a typed/visible block event on breaker trips (the state and the event vocabulary currently disagree), and reject or normalize `max_runtime_seconds=0` at create. Both are one-line-class fixes only Hermes can own | not filed |
| M8 | **review-gap watchdog** — for done impl/fix cards: (a) auto-creates a missing paired `review:` card, (b) re-validates stalled reviews/merges against git truth, (c) auto-completes a parent blocked `review-required:` with shipped work + existing review child, (d) creates re-apply cards for reverted kanban merges never re-merged | `src/hkrc/review_gap.py:1445` (`run`), `:499` (`is_candidate`), `:84–111` (trigger c) | Mixed — trigger (a) is **HKRC-side policy**: the paired-`review:`-child convention is this workflow's choice; Hermes natively dispatches a `review` lane instead (`kanban_db_dispatch.py:1784`; `review_dispatch_enabled()` `:1259`, `review_dispatch` default on `:1265`) and nothing requires or creates review children. Triggers (b)/(c) share the M2/M5 root causes (no git-truth gate; block-as-handoff stranding). Trigger (d) is a **Hermes missing feature**: no native record of kanban-driven merges to detect revert drift against. **Needs Andre**: adopting the native review lane (retiring `review:` children) vs. asking Hermes to enforce review pairing is an owner workflow decision that decides this row's fix destination | **Config/policy (a)** — or Hermes core if pairing should be native; **Hermes core (b, c)** via M2/M5; **Hermes core (d)** — a merge-provenance event on reviewer merges, the only durable basis for revert detection | not filed |

## Retirement criteria

No row can be retired while its upstream status is "not filed". For each
mechanism, retirement requires **all** of:

1. The upstream fix is merged and released in a Hermes version at or above
   the release carrying it (baseline inspected: v0.21.1 / upstream
   `31d0a242`), and this install runs that version or newer.
2. HKRC verifies the corner case no longer occurs: the watchdog runs in
   `--dry-run`/monitor mode (observe, report would-have actions, never act)
   for a full observation window — **14 consecutive days** is the default
   bar, longer for M1/M8 which depend on reviewer behavior, not just code.
3. The dry-run log shows zero would-have actions attributable to the
   mechanism over the window (for M4/M5/M7 additionally: no recurrence of
   the grounded defect signature in native boards — missing block events,
   stranded review-required parents, silent death blocks).
4. Only then: remove the watchdog and its tests in a dedicated HKRC change,
   citing the Hermes release and the observation window in the commit
   message.

Per-row specifics:

- **M1**: also requires the native remediation primitive to dedupe per block
  episode (the double-create trap HKRC guards with action keys).
- **M2**: also requires the completion gate to use merge state, not claimed
  SHAs (`merge-base --is-ancestor` semantics), before the supersede close can
  be trusted upstream.
- **M3**: also requires priority-ordered release to match
  `select_pick_gate` semantics (highest priority, earliest `created_at`
  tie-break), including the hold/capability safety guards or their native
  equivalent.
- **M4**: satisfied the moment create writes a sticky block event — HKRC's
  H4 scan finding zero rows in the live check (criterion 3) is then a
  formality.
- **M5**: depends on the owner's shape choice; if the native
  `request_review` path is adopted instead of a promotion-gate change, M5
  retires together with M8(c) once no worker authors `review-required:`
  blocks any more.
- **M6**: blocked on the subscription-history check above; retirement also
  needs the operator digest to be genuinely redundant (every `needs_input`
  episode reaches a human natively), not merely covered for currently active
  boards.
- **M7**: retires only when *both* halves ship — a visible/typed death-block
  event and create-time `max_runtime` validation — since either alone leaves
  half of the silent class observable only by HKRC.
- **M8**: retires trigger-by-trigger: (a) with the pairing decision, (b) and
  (c) with M2/M5, (d) only with a native merge-provenance record plus a
  revert scan window showing HKRC finds nothing native detection missed.

## What this doc deliberately does not do

- It does not file, draft, or link any upstream issue or PR — every row says
  "not filed", and filing requires Andre's explicit approval.
- It does not propose runtime changes to HKRC: the mapping table is the
  deliverable; the retirement criteria above are the acceptance tests for a
  *later* slice.
- It does not treat the inspected checkout's line numbers as stable — they
  anchor the analysis to `~/.hermes/hermes-agent` at `16cceb8b`
  and should be re-verified against the then-current upstream before any
  filing.
