"""Persona model + reasoning matrix contract (t_49ba1035 final resolution).

Settled 2026-08-13 by Andre across the t_e5a5d7c1 grilling thread and the
persona elimination. Contract summary:

- developer = flash @ high; reviewer = flash @ high.
- senior-dev = pro (rescue lane).
- frontend-dev = luna-high (never touches minified single-line files).
- lead-orchestrator = flash; explore = flash.
- griller = ELIMINATED: grilling is a chat conversation, cards capture
  decisions only; no future griller assignment.
- authoritative = pro (meta-analysis; unchanged).
- NO model/reasoning overrides at any level (escalation = persona
  reassignment only, the override ladder is dead). HIGH scope = the matrix
  personas listed here and nothing else.

This module is the single source of truth for the matrix values. The
human-facing reference (``references/persona-matrix.md``) and the operator
runbook (``docs/persona-matrix-runbook.md``) are kept consistent with it by
``tests/test_persona_matrix_docs.py``.
"""

from __future__ import annotations

from dataclasses import dataclass

MODEL_FLASH = "flash"
MODEL_PRO = "pro"
MODEL_LUNA_HIGH = "luna-high"
REASONING_HIGH = "high"
STATUS_ACTIVE = "active"
STATUS_ELIMINATED = "eliminated"

#: Pro-tier model ids recognized by the drift flagger (t_a832a269): the
#: bare direct-zai id (live since the 2026-09-01 direct-zai migration),
#: the legacy OmniRoute provider-qualified form, and the 2026-09-01
#: OmniRoute combo form. ``-flash`` variants never appear here — the
#: flash tier wins first in ``persona_drift._model_tier``.
PRO_MODEL_IDS: frozenset[str] = frozenset(
    {"glm-5.3", "zai/glm-5.3", "virtual/glm-5.3"}
)

#: The single documented override exception: authoritative's standing
#: ``pro -> max`` reasoning override is part of the unchanged meta-analysis
#: lane (t_49ba1035 Q6). Accepted under every recognized pro id form
#: (PRO_MODEL_IDS, t_a832a269 — the live profile carries the bare id).
#: Every other reasoning/model override on a matrix persona is drift
#: ("NO overrides at any level").
AUTHORITATIVE_ALLOWED_OVERRIDE: frozenset[tuple[str, str]] = frozenset(
    (model_id, "max") for model_id in PRO_MODEL_IDS
)


@dataclass(frozen=True, slots=True)
class PersonaSpec:
    """One row of the persona matrix.

    ``model`` is MODEL_FLASH or MODEL_PRO; ``reasoning`` is REASONING_HIGH
    for the two reasoning-required personas and None (not enforced by the
    matrix) for everyone else. An eliminated persona carries
    model=None/reasoning=None and status STATUS_ELIMINATED.
    """

    persona: str
    model: str | None
    reasoning: str | None
    status: str
    notes: str = ""


PERSONA_MATRIX: tuple[PersonaSpec, ...] = (
    PersonaSpec("developer", MODEL_FLASH, REASONING_HIGH, STATUS_ACTIVE),
    PersonaSpec("reviewer", MODEL_FLASH, REASONING_HIGH, STATUS_ACTIVE),
    PersonaSpec("senior-dev", MODEL_PRO, None, STATUS_ACTIVE, "rescue lane"),
    PersonaSpec(
        "frontend-dev",
        MODEL_LUNA_HIGH,
        None,
        STATUS_ACTIVE,
        "never touches minified single-line files",
    ),
    PersonaSpec("lead-orchestrator", MODEL_FLASH, None, STATUS_ACTIVE),
    PersonaSpec("explore", MODEL_FLASH, None, STATUS_ACTIVE),
    PersonaSpec(
        "griller",
        None,
        None,
        STATUS_ELIMINATED,
        "grilling is a chat conversation; cards capture decisions only; "
        "no future griller assignment",
    ),
    PersonaSpec(
        "authoritative",
        MODEL_PRO,
        None,
        STATUS_ACTIVE,
        "meta-analysis; unchanged",
    ),
)


def persona_spec(persona: str) -> PersonaSpec | None:
    """Return the matrix row for ``persona``, or None when out of scope.

    Profiles outside the matrix (e.g. main, default, adversary) are out of
    scope and are never checked by the drift flagger.
    """
    for spec in PERSONA_MATRIX:
        if spec.persona == persona:
            return spec
    return None


#: Contract rules the drift flagger applies but does not need to re-parse
#: from the matrix rows. Kept as data so the reference doc and the runbook
#: can be asserted against them.
CONTRACT_RULES: dict[str, str] = {
    "overrides": (
        "NO model/reasoning overrides at any level (escalation = persona "
        "reassignment only, ladder dead); the single documented exception is "
        "authoritative's standing pro -> max reasoning override"
    ),
    "scope": "HIGH scope = matrix personas only; profiles outside the matrix "
    "are never checked",
    "griller": "griller ELIMINATED - grilling is a chat conversation, cards "
    "capture decisions only; no future griller assignment",
}
