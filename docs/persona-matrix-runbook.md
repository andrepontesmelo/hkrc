# Persona matrix: operator runbook (one-time profile edits)

Applies the t_49ba1035 FINAL RESOLUTION matrix (contract:
`references/persona-matrix.md`, data: `src/hkrc/persona_matrix.py`).

WHO: operator only. The HKRC loop and its drift flagger never write
`~/.hermes` profile state (effect boundary, t_7dca44ce batch-1 #3) — the
flagger READS and reports; it cannot fix. All edits below go through the
skills-dist repository distribution-sync (the operator path), NOT through
HKRC and NOT through a kanban worker.

WHAT: the one-time edits that make the live profiles match the matrix.
Verified live state at 2026-08-13 (pre-edit): developer/reviewer model
flash with `agent.reasoning_effort` UNSET; senior-dev model flash; griller
profile still present with `reasoning_effort: high` and a
`reasoning_overrides` entry.

## 1. developer — set reasoning_effort = high

`~/.hermes/profiles/developer/config.yaml` (dist repo: hermes-skills-dist
profile tree): add under `agent:`:

```yaml
agent:
  reasoning_effort: high
```

Model stays `opencode-go/deepseek-v4-flash` (no change).

## 2. reviewer — set reasoning_effort = high

`~/.hermes/profiles/reviewer/config.yaml`: add under `agent:`:

```yaml
agent:
  reasoning_effort: high
```

Model stays `opencode-go/deepseek-v4-flash` (no change).

## 3. senior-dev — model to pro

`~/.hermes/profiles/senior-dev/config.yaml`:

```yaml
model:
  default: zai/glm-5.3
```

(rescue lane: senior-dev is the pro persona. Provider unchanged.)

## 4. griller — retirement

griller is ELIMINATED (grilling = chat conversation; cards capture decisions
only). Remove the griller profile from the dist-sync tree
(`~/.hermes/profiles/griller/`): this also removes its
`reasoning_overrides` entry (`{"zai/glm-5.3": "high"}`) and
its `reasoning_effort: high`. No future kanban card may assign griller; the
four historic pro pins on map cards (t_49ba1035, t_7dca44ce, t_f4f9ccc2,
t_d2fb8917) are moot.

## 5. Do not touch

- authoritative — stays pro with its standing `pro -> max` reasoning
  override (meta-analysis; unchanged).
- frontend-dev — luna-high (`cx/gpt-5.6-luna-high`; live profile already
  aligned 2026-08-16, t_545a638a). lead-orchestrator and explore — stay
  flash; the matrix does not enforce a reasoning level there.
- No model/reasoning overrides may be added at any level (escalation =
  persona reassignment only; the override ladder is dead).

## Verify after sync

1. `hkrc` drift sweep (report-only): no findings for developer, reviewer,
   senior-dev, frontend-dev, or griller.
2. `git diff --check` on the dist repo; push via the normal distribution-sync
   workflow.
