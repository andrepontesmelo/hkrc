# Harness-learning-loop — daily 03:00 prompt spec (harness-loop)

> **Supersedes the Hermes-cron-embedded prompt.** This file is the source of truth for the
> daily 03:00 harness-learning loop once the `harness-loop` subsystem lands (hkrc repo, release
> 0.11.0). It ports, verbatim where possible, the prompt of Hermes cron job `f69651252ba1`
> (Andre's personalizations) plus `harness-learning-loop-design.md` (the design doc). The
> Hermes cron becomes a thin shim (`scripts/harness-loop-cron.py`) once 0.11.0 is deployed;
> the cron's embedded prompt is then stale and must not be edited independently.

## Purpose

Turn the manual self-review audit into a loop that also FIXES the harness: detect recurring
defects in skills/memory/config/guidance, apply at most 2 reversible fixes per run, touch
nothing on Andre's projects, deploy nothing. Audit the last 24 hours of Hermes sessions and
Kanban activity as TEST DATA, and find defects in the ORCHESTRATION LAYER — the harness:
skills, memory, config, guidance, routing rules — plus the hermes-kanban-recovery-controller
(HKRC) project, which is orchestration-layer machinery (board watcher, blocker recovery,
review-pair enforcement). Every other board (
etc.) is off-limits: read-only sensors.

## Scope (Andre's corrections, 2026-08-04 — all mandatory)

1. **Orchestration layer only** — skills under `~/.hermes/profiles/main/skills/`
   and external_dirs dist `~/git/hermes-skills-dist/skills`, MEMORY.md/USER.md,
   config.yaml, guidance texts. IN SCOPE (may be routed as report suggestions).
2. **HKRC is the single project exception** — repo
   `~/git/hermes-kanban-recovery-controller` + its kanban board; it IS orchestration
   machinery (board watcher, blocker recovery, review-pair enforcement). Live mode routes an
   accepted HKRC proposal into exactly one implementation card (absolute task worktree) plus one
   parent-linked reviewer card on board `hkrc` — the only board the loop ever writes. The
   canonical checkout is NEVER edited or committed by the loop.
3. **Every other board = read-only sensors.**
   No kanban cards created by the cron on any project-sensor board. Pure project defects → at
   most one line of context, never an action. OFF-LIMITS: all other projects (no edits, no
   kanban cards, no commits). Their boards are read-only evidence. Never touch other profiles'
   data. No destructive ops. No native DB mutation; reuse HKRC safe-read patterns
   (WAL-fail-closed like the observer).

## Dedupe protocol (non-negotiable — Andre ships fixes mid-day, the cron must never re-solve them)

- State file: `~/.hermes/profiles/main/cron/state/harness-loop-state.json` — shape:
  `{created, last_run, resolved_topics: [{topic, fingerprint, resolved_date, how, source}],
  suggested_fingerprints: []}`.
- Before ANY apply/suggest:
  1. Read state file (create if missing). Skip any finding whose fingerprint is in
     `suggested_fingerprints` (30-day cooldown).
  2. Verify the issue still exists: for repo fixes, read-only
     `git -C ~/git/<repo> log --since="<last_run date>" --oneline`; for skill/memory
     issues, re-read the actual file. If already fixed, append to `resolved_topics` in the
     state file (with date) and DO NOT re-solve or re-suggest.
  3. Only then apply/suggest.
  4. After the run, write the state file: `last_run` date, finding fingerprints,
     `resolved_topics`.
- Pre-seeded resolved topic (do not touch): review-pair enforcement shipped 2026-08-04 in
  hermes-kanban-recovery-controller. Seed known-shipped fixes so the first run can't
  false-positive on them.

## Apply policy

- **Max 2 ticket-pair routes per run total (1 orchestration + 1 hkrc budget
  slots; orchestration proposals are scope-gate rejected in live mode, so the
  effective live budget is 1 hkrc pair/run).** Never batch skill edits, never
  delete skills.
- **SCOPE GATE (live mode, before any ticket):** reject non-HKRC project fixes,
  credentials, runtime DB writes, deploy/systemd, merge, and canonical-checkout
  mutation. Rejected proposals are deferred with the reason and stay visible in
  the report.
- **HKRC ROUTE (max 1/run)**: a fix in
  `~/git/hermes-kanban-recovery-controller`. Live mode creates exactly
  ONE implementation card in an absolute task worktree (`--workspace
  worktree:<repo>`, so the worktree materializes under `<repo>/.worktrees/`)
  plus ONE parent-linked reviewer card on board `hkrc`, both keyed by an
  idempotency key derived from the finding fingerprint — retries reuse the same
  cards and a partial pair (impl created, review failed) is completed by the
  retry. The loop NEVER edits, version-bumps, or commits the canonical checkout;
  the pytest gate and conventional commit happen in the worktree by the
  implementation worker, and the reviewer merges. Red tests = reviewer blocks.
- **SUGGEST**: items that fail the apply bar, in the report, with fingerprints recorded for
  cooldown.

## Authoritative analysis stage (configurable, disabled by default)

- **Purpose**: an auditable intelligence stage between deterministic evidence collection
  and the ticket router. The deterministic layer remains the source of evidence and the
  FINAL policy gate; the configured Hermes analysis profile ranks, explains, and proposes
  fixes. The loop never executes model output directly.
- **Evidence handoff (AC1)**: bounded, secret-free JSON with stable finding fingerprints
  and explicit `real|probe|simulation` labels per finding and per evidence line; text is
  scrubbed (credentials, long hex blobs) and truncated; at most 20 findings / 64 KB.
- **Invocation**: `hermes -p <analysis_profile> chat -q <prompt> --yolo -Q`
  once per run; the profile config is the single source of truth for the
  session contract — reasoning level (pro@high) and turn budget
  (`agent.max_turns`, Hermes default 500, well above the 50-turn floor) come
  from the config, never from CLI override tokens. The evidence document is
  embedded as untrusted DATA and the prompt explicitly warns about prompt
  injection in observed text.
- **Strict output (AC2)**: proposals must carry evidence references, root-cause
  hypothesis, confidence 0..1, proposed HKRC change (repo-relative target naming
  ONE existing concrete source file — never a directory or nonexistent path —
  plus before/after, suggestion), acceptance evidence, and no-action reason.
- **Validation (AC3)**: reject unsupported/hallucinated claims, missing evidence
  references, duplicate fingerprints (within and across proposals), probe/simulation-
  grounded proposals, active suggestion cooldowns, non-HKRC scope, direct
  edit/merge/deploy requests, and malformed output. The router scope gate re-validates
  every validated proposal — defense in depth, deterministic final gate.
- **Routing (AC4)**: only validated proposals reach the Kanban router; the model's
  hypothesis/confidence/acceptance ride on the ticket for auditability. No model output
  executes directly.
- **Failure semantics (AC5)**: analyzer failure, timeout, or malformed output produces
  the deterministic report and ZERO tickets — never a partial apply. An empty
  `analysis_profile` disables the stage and preserves deterministic routing exactly.

## Deploy policy

- **NEVER automatic** (Andre's standing rule: deploys are operator-controlled and batched).
  The cron prepares fixes; deploying (`python3 scripts/hkrc_release.py upgrade` +
  `systemctl --user restart hkrc`) is Andre's call. After an applied hkrc fix, include the
  exact deploy command in the report under **"Deploy-ready:"**.
- Auto-deploy for hkrc was offered 2026-08-04, not taken; recommendation stands — keep the
  deploy step human.

## Steps (audit window)

1. **Window = now-24h UTC epoch.** Sessions:
   `sqlite3 ~/.hermes/profiles/main/state.db "SELECT id,title,source,started_at,ended_at,message_count FROM sessions WHERE started_at>=? OR ended_at>=? ORDER BY started_at"`.
   Read the first user message of each big session. (Profile state.db — NOT `~/.hermes/state.db`,
   the stale shell.)
2. **Per board**: glob `~/.hermes/kanban/boards/*/kanban.db` (skip the stray 0-byte
   `boards/kanban.db` FILE). Pull status counts; tasks created/completed in window; task_runs
   statuses in window (running|done|blocked|crashed|timed_out|failed|released); task_events
   failure kinds (blocked, claim_rejected, gave_up, spawn_failed, block_loop_detected, crashed,
   timed_out). Boards are evidence of harness behavior.
3. **Run the self-review pattern library** (below), always translating the finding into an
   orchestration-layer question: "which skill/memory/config/guidance caused this or failed to
   prevent it?" If a pattern is a pure project defect with no harness cause, report it at most
   as one line of context — never as an action.
4. **HKRC-specific**: also check hkrc board blocked/stuck cards and hkrc daemon health
   (`systemctl --user status hkrc`, `journalctl --user -u hkrc` tail, controller state DB
   cursors) — defects here are in scope to fix directly.
5. **Dedupe (above) BEFORE any apply/suggest.**
6. **Check `~/.hermes/profiles/main/logs/curator/`** reports from the last 7 days
   (consolidation signals, don't duplicate).

## Pattern library (detect these)

- **Re-ask in fresh sessions:** same troubleshoot question N× in N sessions → each session
  re-derives ~200 messages. Biggest waste lever. Fix: one thread per incident, or
  session_search handoff. Also the #1 token-saver in the bloat watchdog (identical first
  questions across sessions).
- **Fix-chain whack-a-mole:** one defect → 5+ impl/review cards in hours; every review spawns
  another fix. Cap: after 2 fix generations, stop and re-derive root cause + full-gate
  acceptance.
- **Outage detection latency:** first user report time vs fix landing (config mtime). 5h blind
  windows = the real cost; automation (watchdog) is the fix, not more config.
- **Decision latency:** blocked/fix-card gaps > 30 min; fix cards created hours after review
  blocks.
- **Review-pair gap (done task with no child AND unmerged wt/<branch>):** a done non-review
  task with no children AND an unmerged `wt/*` branch = feature lost. Use the
  **assignee-HISTORY check**, never the bare link row, never title text: a review exists if
  ANY child whose `created` event payload has `assignee: "reviewer"` OR any `task_run` with
  `profile: "reviewer"` — a delegated review (reassigned after the reviewer started) leaves a
  link but no reviewer as current assignee; checking only the link/current assignee
  false-positives and creates duplicates.
- **Review-required block loop:** parent blocked with reason prefix `review-required:` +
  `block_loop_detected` events + decomposed impl children with shipped commits = loop; check
  `task_links` children before re-reviewing. Workers must complete (not block) when a review
  child exists.
- **Stale/contradictory skill instructions:** skill text that teaches the wrong rule with a
  prominent code example while the correct rule is buried in a pitfall — workers follow the
  prominent instruction. Also the **skill-patch location trap**: worker profiles resolve
  kanban-worker from `external_dirs: [~/git/hermes-skills-dist/skills]` (git
  dist), NOT the main profile local copy; a fix added only to the main-local copy never
  reaches dispatched workers. Verify `grep external_dirs <worker-profile>/config.yaml` before
  patching any worker-facing skill.
- **Config drift:** mixed persona model state after an incident — diff `model.default` /
  fallback across profiles.

## Session-bloat watchdog (PREVENTIVE — Andre's "cleanup ≠ prevention" correction, 2026-08-04)

- **Flag LIVE sessions** when cumulative input_tokens cross 5M — list top-live in the daily
  03:00 report so Andre can `/new` or compact BEFORE ballooning, not after. Cleanup (archive/optimize)
  is hygiene, not prevention.
- **Keep ENDED-session flagging** for cleanup: ended sessions with input_tokens > 5M →
  recommend archive/optimize (current monsters: e.g. 44.4M, 31.6M, 28.4M input tokens). Report top-3 bloat sessions each run.
- **Per-message token density** = `input_tokens/message_count` — surface context-hygiene
  failures (44.4M/210msgs = ~211K/msg).
- **Re-ask detection** (identical first questions across fresh sessions) ranked the #1
  token-saver — one thread per incident.
- Cleanup commands (report-only, operator runs): `hermes sessions archive --min-tokens <N>
  --dry-run` → `--yes` (soft-hide, reversible); `hermes sessions optimize-storage` + `optimize`
  in a quiet window (see token-defrag `scripts/session-optimize.sh`).

## Report format (daily 03:00 delivery, Telegram formatting, no pipe tables)

1. **One line: the window's story.**
2. **"What's wrong (orchestration layer)"** — ranked, max 5, concrete numbers, labeled
   real/probe/simulation.
3. **"Already fixed — skipped"** — topics deduped against mid-day fixes (proves the dedupe
   worked).
4. **"Applied"** — up to 2 changes with before/after and commit SHA where relevant, or "none".
5. **"Deploy-ready"** — for hkrc changes, the exact operator command (release upgrade +
   restart), or "none".
6. **"What's right"** — up to 3 items.
7. **ONE next action doable in under 2 minutes.**

## Shaping corrections (why the design is what it is)

- "What if I already fixed it during mid day? Won't this Cron try to solve the reviewer issue
  that was already solved?" → dedupe protocol + resolved_topics.
- "It should not fix anything on my projects, just in the orchestration layer" → scope
  boundary, no project cards, boards as sensors.
- "One exception is the hkrc project/git/Kanban. There it's ok to apply fixes." → HKRC
  exception.
- "Will it apply and deploy automatically?" → apply yes; deploy never automatic;
  "Deploy-ready:" line instead.
- "How does this prevent bloat?" → watchdog is PREVENTIVE: flag LIVE sessions crossing 5M
  (top-live each run), keep ended-session cleanup flags, re-ask detection #1 token-saver, per-
  message token density, top-3 bloat each run.

## Identity and delivery

- Delivery: the SelfReview Telegram group (chat_id <telegram-chat-id>, configurable in config.toml).
  The cron is the accepted "recurring cron" offer from that skill's delivery format — never
  re-offer a cron in this group.
- Shipped schedule: daily at 03:00 (`0 3 * * *`, America/Vancouver). The cron shim
  (`scripts/harness-loop-cron.py`) runs `hkrc harness-loop run --config
  <instance>/config/hkrc/config.toml --dry-run` (dry-run until the operator reviewed 24h of
  dry-run logs) as manifest job `harness-learning-loop (daily 7-day self-review)`, replacing the
  legacy daily job `f69651252ba1` — operator step after the 0.11.0 release is deployed. The
  manifest job name matches the live Hermes cron job so `hkrc crons sync` updates
  `f69651252ba1` in place.
