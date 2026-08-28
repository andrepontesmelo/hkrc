# Persona model + reasoning matrix contract

Source of truth: `src/hkrc/persona_matrix.py` (this doc is asserted against
it by `tests/test_persona_matrix_docs.py`). Settled 2026-08-13 by Andre —
t_49ba1035 FINAL RESOLUTION across the t_e5a5d7c1 grilling thread plus the
persona elimination. The one-time profile edits that make live configs match
this matrix are documented in `docs/persona-matrix-runbook.md` and are
OPERATOR-applied via the skills-dist repository distribution-sync, never
by the HKRC loop.

## Matrix

| persona | model | reasoning | status | notes |
|---|---|---|---|---|
| developer | flash | high | active | fix/DEF + implementation |
| reviewer | flash | high | active | review/merge gate |
| senior-dev | pro | not enforced | active | rescue lane |
| frontend-dev | luna-high | not enforced | active | never touches minified single-line files |
| lead-orchestrator | flash | not enforced | active | routing/decomposition |
| explore | flash | not enforced | active | breadth research |
| griller | -- | -- | eliminated | grilling is a chat conversation; cards capture decisions only; no future griller assignment |
| authoritative | pro | not enforced | active | meta-analysis; unchanged |

Reasoning is "high" only where the matrix requires it (developer, reviewer).
For every other active persona the matrix does not enforce a reasoning
level; the drift flagger reports reasoning problems there only when
reasoning is disabled (`none`/`off`).

## Contract rules

- NO model/reasoning overrides at any level: escalation is persona
  reassignment only, the override ladder is dead. The single documented
  exception is authoritative's standing `pro -> max` reasoning override
  (meta-analysis lane, unchanged).
- HIGH scope = matrix personas only. Profiles outside the matrix (e.g.
  main, default, adversary) are never checked or reported.
- griller is ELIMINATED: grilling is a chat conversation, cards capture
  decisions only, no future griller assignment. Any configured griller
  profile is drift until the operator retires it (runbook).

## Drift flagging (report-only)

`src/hkrc/persona_drift.py` READS live profile configs
(`~/.hermes/profiles/*/config.yaml` — `model.default`,
`agent.reasoning_effort`, `agent.reasoning_overrides`) and FLAGS drift from
the matrix as report-only findings. HKRC never writes `~/.hermes` profile
state (effect boundary, t_7dca44ce batch-1 #3).

Finding codes:

| code | meaning |
|---|---|
| `wrong-model` | profile model differs from the matrix (including unset) |
| `reasoning-unset` | reasoning_effort missing where the matrix requires `high` |
| `reasoning-none` | reasoning_effort is `none`/`off` (disabled) on a matrix persona |
| `reasoning-mismatch` | reasoning_effort set to a level other than the matrix `high` |
| `override-present` | a model/reasoning override on an active persona (authoritative's `pro -> max` exception aside) |
| `eliminated-persona` | a configured profile still exists for an eliminated persona (griller) |

The sweep is pure read: findings are returned, nothing is written, and
`check_snapshot` never mutates its input snapshot. The live read path is
exercised in tests against fixture files under `tmp_path`, never against
`~/.hermes`.

## Change log

- 2026-08-16 (t_545a638a): frontend-dev model flash -> luna-high
  (`cx/gpt-5.6-luna-high`), decided by Andre (selfcheck session 7). The
  live profile was already aligned; only the matrix, drift flagger, and
  docs changed.
