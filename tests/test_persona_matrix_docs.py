"""Contract tests: persona-matrix reference doc + operator runbook match the
code matrix (t_f7cc2828).

Follows the same import-free-on-purpose pattern as test_harness_loop_docs.py:
these tests assert the shipped docs carry the matrix values from
``src/hkrc/persona_matrix.py`` and the t_49ba1035 FINAL RESOLUTION exactly.
"""

from __future__ import annotations

from pathlib import Path

from hkrc.persona_matrix import (
    PERSONA_MATRIX,
    STATUS_ACTIVE,
    STATUS_ELIMINATED,
    CONTRACT_RULES,
    persona_spec,
)

ROOT = Path(__file__).resolve().parents[1]

REF = ROOT / "references" / "persona-matrix.md"
RUNBOOK = ROOT / "docs" / "persona-matrix-runbook.md"
README = ROOT / "README.md"

#: Exact matrix values from t_49ba1035 FINAL RESOLUTION: persona ->
#: (model, reasoning, status).
EXPECTED = {
    "developer": ("flash", "high", STATUS_ACTIVE),
    "reviewer": ("flash", "high", STATUS_ACTIVE),
    "senior-dev": ("pro", None, STATUS_ACTIVE),
    "frontend-dev": ("luna-high", None, STATUS_ACTIVE),
    "lead-orchestrator": ("flash", None, STATUS_ACTIVE),
    "explore": ("flash", None, STATUS_ACTIVE),
    "griller": (None, None, STATUS_ELIMINATED),
    "authoritative": ("pro", None, STATUS_ACTIVE),
}


def _norm(text: str) -> str:
    """Collapse all whitespace so substring assertions survive line wraps."""
    return " ".join(text.split())


def _table_rows(text: str) -> list[tuple[str, str, str, str]]:
    """Parse the reference doc matrix table into (persona, model, reasoning,
    status) rows. A ``--`` or ``not enforced`` cell maps to None."""
    rows: list[tuple[str, str, str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if cells and cells[0] == "persona":
            continue
        if cells and all(c and set(c) <= {"-"} for c in cells[:4]):
            continue  # separator row
        if len(cells) < 4 or all(not c or c == "-" for c in cells[:4]):
            continue
        rows.append((cells[0], cells[1], cells[2], cells[3]))
    return rows


# --- matrix contract --------------------------------------------------------


def test_matrix_values_match_final_resolution_exactly() -> None:
    assert {s.persona: (s.model, s.reasoning, s.status) for s in PERSONA_MATRIX} == EXPECTED


def test_persona_spec_lookup() -> None:
    assert persona_spec("developer") is not None
    assert persona_spec("adversary") is None  # out of matrix scope


def test_contract_rules_carried() -> None:
    rules = " ".join(CONTRACT_RULES.values()).lower()
    assert "no model/reasoning overrides at any level" in rules
    assert "persona reassignment only" in rules
    assert "matrix personas only" in rules
    assert "griller" in rules


def test_reference_doc_exists() -> None:
    assert REF.is_file(), f"missing {REF}"


def test_reference_doc_table_matches_code_matrix() -> None:
    text = REF.read_text(encoding="utf-8")
    rows = {persona: (model, reasoning, status) for persona, model, reasoning, status in _table_rows(text)}

    def norm(value: str) -> str | None:
        if value in ("--", "not enforced", ""):
            return None
        return value

    for spec in PERSONA_MATRIX:
        assert spec.persona in rows, f"doc table missing {spec.persona}"
        model, reasoning, status = rows[spec.persona]
        assert norm(model) == spec.model, spec.persona
        assert norm(reasoning) == spec.reasoning, spec.persona
        assert status == spec.status, spec.persona
    assert set(rows) == {s.persona for s in PERSONA_MATRIX}


def test_reference_doc_carries_contract_rules() -> None:
    text = _norm(REF.read_text(encoding="utf-8")).lower()
    assert "no model/reasoning overrides at any level" in text
    assert "persona reassignment only" in text
    assert "matrix personas only" in text
    assert "griller" in text
    assert "eliminated" in text
    assert "rescue lane" in text
    assert "minified single-line files" in text
    assert "meta-analysis; unchanged" in text
    assert "report-only" in text
    assert "never writes" in text


# --- operator runbook -------------------------------------------------------


def test_runbook_exists() -> None:
    assert RUNBOOK.is_file(), f"missing {RUNBOOK}"


def test_runbook_is_operator_applied_via_dist_sync() -> None:
    text = _norm(RUNBOOK.read_text(encoding="utf-8"))
    assert "hermes-skills-dist" in text
    assert "distribution-sync" in text
    assert "operator" in text.lower()
    assert "NOT through HKRC" in text
    assert "NOT through a kanban worker" in text


def test_runbook_covers_all_one_time_edits() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    # developer + reviewer reasoning_effort = high
    assert text.count("reasoning_effort: high") >= 2
    assert "developer" in text
    assert "reviewer" in text
    # senior-dev model to pro
    assert "zai/glm-5.3" in text
    assert "senior-dev" in text
    # griller retirement
    assert "griller" in text
    assert "retirement" in text or "retire" in text
    # authoritative untouched
    assert "authoritative" in text
    assert "Do not touch" in text


def test_runbook_never_lets_the_loop_write_profiles() -> None:
    text = _norm(RUNBOOK.read_text(encoding="utf-8")).lower()
    assert "never write" in text
    assert "read" in text and "reports" in text


# --- README -----------------------------------------------------------------


def test_readme_lists_matrix_files() -> None:
    text = README.read_text(encoding="utf-8")
    assert "references/persona-matrix.md" in text
    assert "docs/persona-matrix-runbook.md" in text
    assert "persona_matrix" in text or "persona-matrix" in text
