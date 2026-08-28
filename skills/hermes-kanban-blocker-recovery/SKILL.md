---
name: blocker-recovery
description: Use when Telegram invokes /blocker-recovery run for a Hermes Kanban blocker handoff.
version: 0.15.1
author: Hermes Agent
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [hermes, kanban, blocker-recovery, telegram, operations]
    related_skills: []
---

# Hermes Kanban Blocker Recovery Controller

## Overview

This portable skill defines the `/blocker-recovery` Telegram entry point for the
standalone `hermes-kanban-recovery-controller` repository. It is installed per Hermes
instance and keeps native Kanban data, controller state, workspace, and skill files
isolated under that instance root.

The controller provides `hkrc init`, `hkrc status`, read-only `hkrc discover`, the
manual one-shot `hkrc run` operation, and an explicitly gated `hkrc daemon`. Discovery scans all non-archived boards, selects
recent blocked tasks using the latest native `task_events.created_at` within the
effective recency window (`[discovery] recency_window_seconds`, default 3600 seconds),
skips `needs_input`, `transient`, and `dependency`, and records a one-ever controller
reservation for eligible missing/unknown and recoverable kinds. A blocked task whose
latest event is older than the effective window is never silently omitted: `discover`
and `run` print a visible `note` line for it (``blocked N ago — outside recency window,
use --backfill``) and leave it unreserved. It also scans `task_links`
for `todo`/`ready` children of `done`/`blocked` parents whose latest event is older than
`[discovery] unclaimed_child_after_seconds` (default 1800 seconds) and reserves each such
child once as `kind=unclaimed_child`; this catches review/child cards that stall unclaimed
while their parent is terminal or stuck, including parents blocked with `needs_input` (the
parent itself stays skipped). Children with an in-flight run or a non-null `claim_lock` are
claimed and never flagged. `run` consumes each
reservation before invoking official Hermes CLI operations as subprocesses; it never
writes Hermes source, config, or native SQLite.

## When to Use

Use this skill when a human explicitly sends `/blocker-recovery run` in Telegram. Do not
use it for unattended polling, automatic retries, or sending a recovery result that was
not produced by a real command run.

The Telegram entry point is always manual compatibility mode. It must never start the
daemon, parse `hermes kanban watch`/`tail` output, or claim continuous event coverage.

## `/blocker-recovery run`

When invoked with the `run` argument:

1. Execute the installed instance wrapper exactly once:
   `HKRC_INSTANCE_ROOT=/absolute/path/to/instance /absolute/path/to/instance/bin/hkrc run --config /absolute/path/to/instance/config/hkrc/config.toml`.
2. Wait for the process to exit; do not start a daemon, poll, queue, or retry.
3. Return the command's raw combined stdout/stderr and real exit status. Preserve native
   stdout, phase/error lines, and the final `summary reserved=... started=...` line.
4. Add only this short controller count summary after the raw output:
   `HKRC counts: reserved=N started=N completed=N failed=N skipped=N`.

The counts must be copied from the actual final summary, never inferred. If the lead
orchestrator is missing, or the notification destination is absent/unusable, return the
real native error and nonzero status; do not retry or claim a recovery occurred.

Other arguments are unsupported and must be reported without executing a recovery.

## Instance boundary

1. Identify the configured instance and its config path before running a command.
2. Run the installed instance wrapper (or repository `uv run hkrc`) for that same root.
3. Treat `native_boards_root` as a read-only boundary. This controller must not create,
   write, or mutate anything under that path.
4. Store runtime data only in the configured controller-owned `state_db` and workspace.
5. Never substitute a different instance's config or state database.

## Release handling

Install an instance-local release from repository contents:

```bash
python3 scripts/hkrc_release.py install \
  --source-root /path/to/hermes-kanban-recovery-controller \
  --instance-root /absolute/path/to/instance
```

Upgrade while retaining a rollback release:

```bash
python3 scripts/hkrc_release.py upgrade \
  --source-root /path/to/hermes-kanban-recovery-controller \
  --instance-root /absolute/path/to/instance \
  --version 0.2.0
```

Rollback swaps `current` and `previous` without deleting either release:

```bash
python3 scripts/hkrc_release.py rollback --instance-root /absolute/path/to/instance
```

To render the optional, reviewable service artifact without installing or starting it:

```bash
python3 scripts/hkrc_release.py unit \
  --source-root /path/to/hermes-kanban-recovery-controller \
  --instance-root /absolute/path/to/instance
```

These commands write only the selected instance root. They do not install a service,
enable/start a service, or modify a live Hermes installation. The optional
`systemd/hkrc.service` artifact is a separate operator opt-in; installing or enabling it
must not change this Telegram entry point, which remains a one-shot `hkrc run` invocation.

The external `needs-input-watcher` cron shim always execs the installed wrapper
(`<instance-root>/bin/hkrc`), so after upgrading a release the operator must redeploy it
into the instance (`hkrc_release.py upgrade --version 0.7.0`) for the cron job to pick up
the new code; the shim itself needs no edit. Rollback swaps `current`/`previous` without
touching the shim.

## Initialize one Hermes instance

Run only when the operator has supplied the intended instance name, native boards path,
and controller state path:

```bash
/absolute/path/to/instance/bin/hkrc init \
  --config /absolute/path/to/instance/config/hkrc/config.toml \
  --instance-name INSTANCE \
  --native-boards-root /absolute/path/to/hermes/boards \
  --state-db /absolute/path/to/instance/state/hkrc/state.sqlite3
```

Initialization creates controller config/state only; it does not inspect native Hermes
files or start a service.

## Check controller health

```bash
/absolute/path/to/instance/bin/hkrc status \
  --config /absolute/path/to/instance/config/hkrc/config.toml
```

A valid result includes the instance name, state DB, schema version, and the explicit
line `native_boards_root=... (not scanned)`. If the command fails, report the actual
error and stop; do not infer health from a previous run.

## Discover recent blockers

```bash
/absolute/path/to/instance/bin/hkrc discover \
  --config /absolute/path/to/instance/config/hkrc/config.toml
```

This command opens native board SQLite files read-only and prints exactly one
`board_slug=... task_id=... action=...` resolution for every recent blocked candidate,
including `skipped` and `already_reserved`. It writes only to the configured
controller-owned state database. A successful `reserved` line is not a recovery handoff;
the later mutation operation owns that boundary.

The effective recency window is `[discovery] recency_window_seconds` (default 3600
seconds). A blocked task whose latest event is older than that window is never silently
omitted: `discover` and `run` print a visible informational line for it, e.g.
`note board_slug=... task_id=... status=blocked blocked_seconds_ago=18000 hint="blocked 5h ago — outside recency window, use --backfill"`,
and do not reserve it. To include those older blockers, pass a duration on the one-shot
path — `hkrc discover --backfill 5h`, `hkrc run --backfill 90m`, or bare `--backfill`
for every blocked task; `--since` is an alias with the same semantics. The flag wins
over the configured window for that single invocation and the default behavior
(unflagged) is unchanged.

## Backfill and the Telegram fallback

The `/blocker-recovery run` Telegram entry point always invokes the installed wrapper as
`hkrc run --config <instance>/config/hkrc/config.toml` with no extra flags, so it honors
the instance's effective recency window through configuration: set
`[discovery] recency_window_seconds` in the instance TOML (for example `86400` for 24h)
so the fallback no longer silently misses stale blockers after HKRC was down. Even at
the default window, a stale blocked task now produces the visible
`outside recency window, use --backfill` note line in the run output instead of
disappearing silently; when the operator wants those older blockers handled, they run
`hkrc run --backfill <duration>` once and the note lines become real resolutions.

## Optional continuous stream mode

Continuous mode is disabled by default and is not a migration of the manual contract. The
`[stream]` config gate may be enabled only after an operator has approved an authenticated,
board-scoped WebSocket adapter and a current-state reader for the target Hermes version:

```toml
[stream]
enabled = true
adapter = "approved_websocket"
endpoint = "wss://dashboard.example.test/api/plugins/kanban/events"
boards = ["main"]
credential_env = "HKRC_STREAM_TICKET"
current_state_reader = "approved-dashboard-snapshot"
alert_after_consecutive_failures = 3
```

This config stores the credential variable name, never the token/ticket. It does not acquire
credentials or construct a connector. Without all approved runtime wiring, `hkrc daemon`
fails closed before reservation. It never opens native SQLite, consumes native WAL/SHM files,
or parses CLI watch/tail text. Transport/auth/current-state failures never become recovery
candidates and no fallback is permitted.

Migration is reversible: keep the optional service disabled while installing/upgrading and
validating the adapter; render and review the unit, then explicitly install/enable/start it.
For rollback, stop/disable the optional unit, run the release helper's `rollback`, and resume
with the manual one-shot `run`. Release and rollback operations never modify Hermes source,
config, native boards, or native databases.

## Perform one controlled handoff

Configure the native CLI/profile and Telegram destination in the instance TOML. A chat
ID is a destination, not a bot secret. Prefer an environment reference for deployment-
specific values; never put a bot token, API key, or credential in this config or skill:

```toml
[native]
cli = "hermes"
profile = "target-instance"

[telegram]
chat_id = ""
chat_id_env = "HKRC_TELEGRAM_CHAT_ID"
chat_type = "group"
thread_id = "42"
user_id = "andre"
notifier_profile = "default"
```

Then run the manual one-shot:

```bash
/absolute/path/to/instance/bin/hkrc run \
  --config /absolute/path/to/instance/config/hkrc/config.toml
```

For every newly reserved eligible task, `run` first creates the native task-specific
Telegram subscription, then appends the fixed controller comment, reassigns to
`lead-orchestrator`, and calls native `unblock`, in that exact order. It trusts successful
native return codes without re-reading native state. The reservation is consumed when the
sequence starts; any partial failure records the phase/error and stops without retry or
rollback. Raw native stdout and a count summary are returned; failed handoffs exit nonzero.

## Telegram response contract

For `/blocker-recovery run`, reply with:

- the exact instance and config path used;
- the exact wrapper command executed once;
- the real exit status;
- raw combined stdout/stderr, preserving native error text;
- the short `HKRC counts: reserved=N started=N completed=N failed=N skipped=N` summary
  copied from the actual final summary.

If the request is ambiguous about the instance, paths, or whether a config replacement is
intended, do not execute. If a command would touch a native path or use a non-controller
state DB, refuse the operation and explain the boundary.

## Common pitfalls

1. **Reporting future behavior as current.** `status` does not scan; `discover` reserves
   candidates but does not recover or mutate tasks.
2. **Using native Hermes state as controller state.** The controller DB must be a separate
   SQLite file owned by this repository's configured instance.
3. **Cross-instance reuse.** A controller state DB is bound to its first instance name;
   configure a new state DB for a different instance.
4. **Unattended execution.** This skill is a manual Telegram-operable contract, not a
   cron or daemon authorization.
5. **Fabricating outcomes.** Only report output from the command that actually ran.
6. **Missing destination.** `run` requires a configured Telegram `chat_id` or `chat_id_env`
   whose variable is set; it fails before reserving anything when absent.
7. **Missing lead orchestrator.** A native reassign failure is returned verbatim and stops
   that task; never retry or substitute another assignee.
8. **Inline secrets.** Bot tokens and credentials belong in the Hermes instance `.env` or
   process environment. This controller accepts only variable names, not secret values.

## Outcome Guard (Gate 2) commands

The deterministic outcome guard adds operator-facing admission and Git
enforcement commands. Full docs ship in the release (`docs/outcome-guard.md`);
quick reference:

- `hkrc outcome-guard register --contract-file <json>` — register a validated
  immutable contract into controller-owned state (rewrites are rejected;
  re-registration is idempotent).
- `hkrc outcome-guard admit-child --parent-task-id <p> --contract-ref <c>
  --effect <e> --board <b> --title <t> --assignee <a>` — creates the child via
  the native `hermes kanban` CLI in the non-dispatchable `blocked` state,
  validates the effect against the governing contract and all ancestors,
  records durable admission evidence, then promotes only after validation.
  Denied effects never reach a native call; CLI failures leave the child
  blocked (never dispatched) and fail closed.
- `hkrc outcome-guard authorize-merge --task-id <t> --contract-ref <c>
  [--evidence-file <json>] [--ref refs/heads/main]` — binds a task, contract,
  and review evidence to a protected ref for the git hook.
- `hkrc outcome-guard git-hook install/status/uninstall --repo <path>` —
  idempotent hook management; an existing unmanaged hook is chained, never
  overwritten, and `core.hooksPath` redirection refuses with an actionable
  error. Enforcement is active only after an explicit install.

Pitfalls specific to Gate 2:

9. **Ambient kanban identity.** Admission subprocesses scrub `HERMES_KANBAN_*`
   variables; never rely on them leaking into a native CLI call.
10. **Protected refs are denied without policy state.** A missing config or
    state DB makes the git hook fail closed (`enforcement_unavailable`) —
    restore state before expecting protected updates to succeed.
11. **Commit messages are not authority.** Merge authorization lives in
    controller-owned state (task -> contract -> evidence); never inspect commit
    text in policy decisions.

## Verification checklist

- [ ] The operator named the Hermes instance.
- [ ] The config path and state DB path are absolute and instance-specific.
- [ ] `/blocker-recovery run` used the selected instance's wrapper exactly once.
- [ ] The output was checked for the read-only native boundary.
- [ ] Any `discover` output has one verified line per candidate and only controller state
      was changed.
- [ ] Any `run` output contains the real native phase order, stdout, errors, and summary.
- [ ] No native source/config/database was written and no service was started.
- [ ] The Telegram response contains the raw command output, real exit status, and the
      copied short count summary.
