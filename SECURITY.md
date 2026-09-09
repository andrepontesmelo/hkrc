# Security Policy

## Scope

HKRC is a board-observing daemon: it reads task state through the native
`hermes kanban` CLI and takes automated recovery actions on catalogued corner
cases (fix-card creation, review completion on verified merge, pick-gate
advance, missing-event guard, evidence-backed deadlock archive). It never
opens or mutates a Hermes/Kanban SQLite database, and never edits Hermes Agent
source, config, or runtime files.

## Supported versions

Only the latest tag on `main` receives security fixes.

## Reporting a vulnerability

Email the owner via the contact on the GitHub profile (andrepontesmelo)
rather than opening a public issue. Include: affected version/commit, the
command and flags that reproduce the issue (redact anything private), and
expected vs actual behavior. You will get an acknowledgement within 7 days and
a fix or a documented mitigation for anything confirmed.

## What counts as a security issue here

- The watcher acting outside its catalogued corner cases (e.g. archiving or
  completing a card without the required evidence).
- The Outcome Guard hook admitting a canonical-ref update without a bound
  task, an allowing contract, and the required review/terminal evidence.
- Credential or token exposure through HKRC-owned state, logs, or reports.

## Where config and credentials live

Provider keys and Hermes credentials live in the Hermes profile and instance
config — never in this repo. HKRC owns only its instance-scoped controller
state (SQLite under the instance root). Do not paste tokens, credentials, or
private board contents into issues, PRs, or card comments.

## What is NOT a vulnerability

- A watchdog acting exactly as catalogued (fix cards, pick-gate advances,
  deadlock archives with evidence) — that is the product's purpose. Audit
  unfamiliar controller configs before deploying them.
- `--dry-run` output describing would-have actions — those mutate nothing.
- Skipped ticks or silent watchdogs when nothing is new — that is the designed
  cron surface.
