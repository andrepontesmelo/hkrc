# Development

## Layout

```
src/hkrc/            controller package (stdlib only at runtime)
  cli.py             `hkrc` entry point (init/status/discover/run/daemon/…/flag)
  watcher.py         decision-latency watcher H1-H4 + deadlock archive
  needs_input_watcher.py / stale_block_watch.py / review_gap.py
                     cron watchdogs (silent when nothing new)
  harness_loop.py    daily 03:00 self-review loop (dry-run default)
  outcome_guard.py / admission.py / git_enforce.py
                     contracts, child admission, reference-transaction hook
  config.py / state.py / crons.py
                     instance-scoped config, controller-owned SQLite, cron manifest
  discovery.py / handoff.py / simulation.py / live.py / event_stream.py
                     discovery, recovery handoff, simulation, daemon wiring
  persona_matrix.py / persona_drift.py / classifier.py / assist*.py …
                     drift detection, classification, assist surface
config/              instance config samples
references/          harness-loop prompt, escalation rule, persona-matrix contract
scripts/             release, gates + cron entrypoints (hkrc_release.py, green.sh,
                     *-cron.py, e2e_*.py)
systemd/             opt-in unit samples (operator installs, never the release)
tests/               pytest suite (48 files)
docs/                this index + architecture + outcome-guard +
                     persona-matrix-runbook
```

## The local gate

Everything must pass before a commit is considered done:

```bash
uv run pytest        # full suite — currently 1062 tests
```

CI (`.github/workflows/ci.yml`) runs exactly this gate on Python 3.11 for
pushes to `main` and every pull request. CI must equal the local gate — drift
is a defect.

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/). Zero runtime
third-party dependencies; `uv run` manages its own environment.

## Conventions

- Deterministic state machine, never an autonomous agent: no LLM calls outside
  the harness loop's single daily reflection; no network in the gate.
- Fail-closed: ambiguous board state means skip, never act. Guards live in the
  shared tick path once — not one per caller.
- Verification uses merge state (`git merge-base --is-ancestor`), never
  claimed SHAs or version strings.
- Native Hermes source, config, and task databases are never opened or
  mutated; board reads go through the native CLI with ambient
  `HERMES_KANBAN_*` variables scrubbed.
- Tests are deterministic (`--now` overrides, replay from cursor zero);
  new behavior ships with a test that pins it.

## Before you push

- [ ] `uv run pytest` green
- [ ] New behavior covered by a test in `tests/`
- [ ] Docs updated if the config surface or a contract changed
- [ ] No absolute home-dir paths, IPs/hostnames, session/chat IDs, tokens, or
      credentials in the diff
