# hermes-kanban-recovery-controller

Standalone controller for Hermes Kanban blocker recovery: watcher, stream
daemon, needs-input sidecar, outcome guard, and the nightly harness learning
loop (see `README.md`, `docs/architecture.md`, `references/`).

This file is the vocabulary map. It names the load-bearing terms so a reader
(and an agent) uses one word per concept. Add a term here when a new one
starts carrying weight in code, cards, or reports.

## Language

**Process finding**:
A harness-loop finding whose remediation is an ARTIFACT amendment — a
working-agreement skill or the supervisor mission file — rather than a change
to HKRC source. It carries `apply_kind = "process"` plus the key of the
remediation pack that owns its text.
_Avoid_: skill finding, non-code finding, soft finding

**Amendment proposal**:
The routing of one process finding: an implementation card plus a
parent-linked review card on board `hkrc` (`process-amendment: <pack key>` /
`review: process-amendment: <pack key> (<impl>)`), keyed
`harness-proc-impl:{fp}` / `harness-proc-review:{fp}`, with workspace
`dir:<artifact directory>`. The proposal carries the target path, the ADD or
AMEND marker, and the verbatim before-text or full content; the paired review
card gates it exactly like a source fix.
_Avoid_: suggestion, patch, edit request

**Artifact home**:
An allowlisted location outside the HKRC repo where an amendment proposal may
land (`process_allowlist`: the supervisor mission file plus
`~/.hermes/dist-skills/*`). ADD proposals must land in an allowlisted
DIRECTORY, AMEND targets must match a glob themselves, and any target inside
the HKRC repo is rejected — repo changes route through the hkrc code channel.
_Avoid_: target, destination, home dir

**Remediation pack**:
A human-authored, version-controlled directory under
`config/hkrc/process_remediations/<key>/` holding `manifest.json` (key,
target_path, kind ADD|AMEND, before, content_file) and the content file. The
pack owns the target, the kind, and the amendment text; the analyzer may rank
or select a pack but never writes amendment prose.
_Avoid_: template, recipe, snippet

## See also

- `README.md` — CLI surface and the harness-loop section.
- `references/harness-loop-prompt.md` — the loop's contract (apply policy,
  process route, deploy policy).
- `docs/architecture.md` — module map and boundaries.
