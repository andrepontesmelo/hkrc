# Contributing

Thanks for looking at HKRC. PRs welcome.

## Workflow

1. Fork / branch from `main`.
2. Make the change with a test that pins it (`tests/`, `uv run pytest`).
3. Run the local gate:

   ```bash
   uv run pytest        # full suite — currently 1062 tests
   ```

4. Open a PR describing what changed and why.

CI runs the same gate on Python 3.11; a PR is mergeable when it is green.

## Ground rules

- Python 3.11+, stdlib only at runtime, no new runtime dependencies without
  discussion. `uv run` manages the dev environment.
- Deterministic and fail-closed — see
  [docs/architecture.md](docs/architecture.md). Ambiguous board state means
  skip, never act.
- Verification uses merge state (`git merge-base --is-ancestor`), never
  claimed SHAs.
- Never open, mutate, or depend on native Hermes source, config, or task
  databases; board reads go through the native CLI with ambient
  `HERMES_KANBAN_*` scrubbed.
- Update [docs/](docs/index.md) when the config surface or a contract changes.

## Reporting bugs

Open an issue with: HKRC version (`hkrc --version`), the command and flags
used, and the tick/watchdog output (redact board contents that are private —
never paste tokens or credentials). For the watcher, note `--dry-run` vs live
and which H-handler misbehaved.

## Security

See [SECURITY.md](SECURITY.md) — please do not open public issues for security
reports.
