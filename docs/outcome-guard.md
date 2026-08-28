# HKRC Outcome Guard (Gate 2): admission and Git enforcement

The Outcome Guard is a portable, self-contained enforcement package. It has no
runtime third-party dependencies (stdlib only), never opens or mutates a
Hermes/Kanban SQLite database, and never edits Hermes Agent source, config, or
runtime files. Everything it knows about the board it learns through the native
`hermes kanban` CLI as argv subprocesses with `HERMES_KANBAN_*` ambient
variables scrubbed.

Three surfaces, one policy core (`src/hkrc/outcome_guard.py`, the immutable
contracts from Gate 1):

1. **Contract registration** — an operator registers a validated, immutable
   JSON contract into controller-owned state.
2. **Child admission** — HKRC-mediated admission creates a child via the native
   CLI in the non-dispatchable `blocked` state, validates the requested effect
   against the governing contract and every ancestor, records durable
   authorization evidence, and promotes the child only after validation.
3. **Git enforcement** — a portable `reference-transaction` hook (Git 2.36+)
   denies updates to protected canonical refs (`refs/heads/main` by default)
   unless policy state binds a task, a contract that allows `merge_main`, and
   the contract's required review/terminal evidence.

Staging, hook install, audit, and deny are distinct operations (below).

---

## 1. Install on a fresh machine (executable path)

Every command below was executed against this repository. Replace
`<instance-root>`, `<repo-url>`, and the placeholder ids with your own values.

### 1.1 Prerequisites

- Python 3.11+ (stdlib only at runtime).
- Git 2.36+ for the `reference-transaction` hook (the enforcement hook). Older
  git simply never invokes the hook, so enforcement silently absent; verify with
  `git --version` before relying on the gate.
- `uv` for the development/test environment only (not needed at runtime).
- The native `hermes` CLI on `PATH` for admission (admission runs
  `hermes kanban create/promote`).

### 1.2 Clone, verify the dev gate, build

```bash
git clone <repo-url> hermes-kanban-recovery-controller
cd hermes-kanban-recovery-controller
uv sync --dev
uv run pytest        # full suite green (includes the Gate 2 asset manifest gate)
uv build
```

### 1.3 Install the instance release

```bash
python3 scripts/hkrc_release.py install --instance-root <instance-root>
```

The release materializes `src/`, `skills/`, `systemd/`, `config/`, and `docs/`
under `<instance-root>/releases/<version>/` and points `current` at it. The
installer validates the checked asset manifest
(`config/hkrc/outcome-guard-assets.json`) first: a missing runtime file fails
the install, never ships. `docs/` ships inside the release, so the installed
package needs no path back into the source checkout.

### 1.4 Initialize config and state

```bash
<instance-root>/bin/hkrc init \
  --config <instance-root>/config/hkrc/config.toml \
  --instance-name default \
  --native-boards-root /path/to/native/boards \
  --state-db <instance-root>/state/hkrc/state.sqlite3 \
  --workspace <instance-root>/workspace/hkrc
```

`init` writes the config (including `[outcome_guard]`) and the controller-owned
SQLite state. The `[outcome_guard]` section:

```toml
[outcome_guard]
protected_refs = ["refs/heads/main"]   # canonical refs the hook protects
```

### 1.5 Verify the installed package (no source checkout)

```bash
<instance-root>/bin/hkrc outcome-guard check-effect --help        # adapter present
<instance-root>/bin/hkrc outcome-guard git-hook status --repo <repo>  # hook state
ls <instance-root>/releases/<version>/docs/outcome-guard.md       # docs shipped
```

Or run the non-interactive clean-root E2E:

```bash
python3 scripts/e2e_outcome_guard_gate2.py
```

It installs into a temp root, registers contracts, denies an unauthorized
protected-main update, admits a valid child, allows an authorized/reviewed
update, restores normal git on uninstall, and asserts no Hermes source was
modified.

### 1.6 Install the hook into a repository

```bash
<instance-root>/bin/hkrc outcome-guard git-hook install \
  --repo /path/to/repo \
  --config <instance-root>/config/hkrc/config.toml
```

- Idempotent: re-installing replaces the managed hook in place.
- Never overwrites a foreign hook: an existing unmanaged hook is chained
  (renamed to `reference-transaction.hkrc-orig` and executed first).
- Refuses with an actionable error when `core.hooksPath` is set (git would not
  run hooks from `.git/hooks`), or when a foreign hook and a saved hkrc
  original both exist.
- Uninstall restores the original:

```bash
<instance-root>/bin/hkrc outcome-guard git-hook uninstall --repo /path/to/repo
```

### 1.7 Register contracts and authorize merges

```bash
# Register the prototype contract (example ships in the release):
<instance-root>/bin/hkrc outcome-guard register \
  --config <instance-root>/config/hkrc/config.toml \
  --contract-file <instance-root>/config/hkrc/outcome-guard-example-contract.json

# Bind a task + contract + review evidence to the protected ref:
<instance-root>/bin/hkrc outcome-guard authorize-merge \
  --config <instance-root>/config/hkrc/config.toml \
  --task-id <task-id> \
  --contract-ref example-prototype-selection \
  --ref refs/heads/main \
  --evidence-file /path/to/evidence.json
```

---

## 2. Operation

### 2.1 Contract registration

`outcome-guard register --contract-file <json>` validates the document against
`hkrc.outcome-contract.v1` (schema version, non-empty ids, known effects,
non-empty terminal evidence requirements, operator authority, parent refs) and
stores it immutably in controller-owned state. A rewrite of a registered
contract is rejected (`contract_conflict`); registering the same document again
is idempotent (`contract_already_registered`). A child contract may only narrow
its ancestors' effects (`effect_broadens_ancestor` otherwise), and an explicit
successor requires a fresh operator authority.

### 2.2 Child admission

`outcome-guard admit-child --parent-task-id <p> --contract-ref <c> --effect <e>
--board <b> --title <t> --assignee <a> [--body <body>]`

1. Policy pre-check: `check_effect` against the governing contract and all
   ancestors. Denied -> recorded as `denied` evidence, exit 3, no child, no
   native CLI call.
2. Create via the native CLI in the non-dispatchable `blocked` state with the
   deterministic idempotency key `hkrc-admit:<sha256(parent|contract|effect)>`
   (no duplicate child possible).
3. Re-validate after creation; record durable admission evidence
   (`outcome_admissions` row, status `admitted`).
4. Promote via the native CLI only after validation passes. Any CLI failure
   leaves the child blocked (never dispatched) and records `failed`/`held`
   audit evidence; the command fails closed with exit 2.

Re-running the same admission returns the existing child id
(`duplicate: true`) — no duplicate child, no duplicate lease.

### 2.3 Authorize a merge

`outcome-guard authorize-merge --ref refs/heads/main --task-id <t>
--contract-ref <c> [--evidence-file <json>]` upserts a binding in
`outcome_merge_authorizations`. The hook consults the most recent binding for
the ref. Task identity is bound in policy state to the contract and the
evidence snapshot — commit messages are never parsed as authority.

- Authorizing a contract that does not allow `merge_main` is rejected (exit 3).
- Authorizing without evidence records the binding with empty evidence: the
  hook denies until the operator re-authorizes with the evidence file
  (the "merge_main waits for bound independent-review evidence" flow).

### 2.4 Git hook (deny mode, default)

The installed `.git/hooks/reference-transaction` script chains any pre-existing
hook, then runs:

```bash
hkrc outcome-guard git-hook --state <prepared|committed|aborted> --config <config>
```

- Non-`prepared` states return 0 immediately (git ignores the exit status
  there anyway).
- `prepared`: stdin tuples `<old> <new> <ref>` are parsed; malformed input
  denies the whole transaction (fail closed). Non-protected refs (including
  pseudo-refs like `HEAD`) pass. Each protected ref is allowed only when the
  bound authorization's contract allows `merge_main` and its terminal evidence
  requirements are met. Unavailable config/state denies (fail closed). Allowed
  transactions are silent; denials print one stderr line per ref and exit 1,
  which aborts the git transaction.

### 2.5 Audit mode

`outcome-guard git-hook --audit-only --state prepared` evaluates the same policy
but prints the JSON decision to stdout and always exits 0 — git proceeds.
Use it to stage a new protected ref or observe denials without enforcing.

### 2.6 Staging vs enforcement

- `git-hook status --repo <repo>` reports whether the hook is installed,
  managed, chained, or redirected by `core.hooksPath` — no mutation.
- Enforcement is only active after an explicit `git-hook install`. A release
  operation never enables enforcement silently; the `[outcome_guard]` config
  section only declares which refs WOULD be protected.

---

## 3. Upgrade, rollback, uninstall

```bash
# Upgrade: keeps current release for rollback, re-seeds manifests.
python3 scripts/hkrc_release.py upgrade --instance-root <instance-root> \
  --source-root <repo> --version <new-version>

# Rollback: swap current <-> previous.
python3 scripts/hkrc_release.py rollback --instance-root <instance-root>

# Uninstall enforcement for one repo (restores any chained original):
<instance-root>/bin/hkrc outcome-guard git-hook uninstall --repo /path/to/repo
```

Config, state, workspace, the systemd unit, and operator-customized seeded
files survive upgrade and rollback unchanged (existing behavior, covered by
`tests/test_release.py`). The asset manifest and cron manifest are refreshed
unconditionally; the example contract and prompt template are seeded only when
missing.

## 4. Service wiring and permissions

- The shipped systemd unit (`systemd/hkrc.service.in`) is an opt-in artifact;
  the release never installs/enables/starts it. It runs the instance wrapper
  with hardened sandbox settings and no stream credentials.
- The git hook needs read access to the controller state DB (the
  `reference-transaction` process runs as the committing user) and the wrapper
  must be executable. Hook scripts are written mode `0755`.
- The controller never needs write access to the native boards root.

## 5. Recovery

- Denied commit: the transaction was aborted; nothing changed. Record the
  authorization/evidence per section 2.3 and retry.
- Create succeeded but promote failed: the child is `blocked` (never
  dispatched) and the admission row shows `held`. Fix the cause (e.g. parent
  dependency, assignee) and promote manually via `hermes kanban promote <id>`.
- Deleted/renamed state DB: the hook fails closed (`enforcement_unavailable`)
  until state is restored — protected refs cannot be updated while policy state
  is unavailable.
- Hook file accidentally removed: re-run `git-hook install` (idempotent).

## 6. Verification checklist

```bash
uv run pytest -q                     # I: full suite green
uv build                             # wheel + sdist build
git diff --check                     # no whitespace errors
python3 scripts/e2e_outcome_guard_gate2.py   # H: fresh-root E2E
```
