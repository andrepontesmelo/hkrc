"""Deterministic outcome contracts and fail-closed policy checks."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
from typing import Any, cast

from .state import ControllerState


CONTRACT_SCHEMA_VERSION = "hkrc.outcome-contract.v1"
RESULT_SCHEMA_VERSION = "hkrc.outcome-result.v1"
KNOWN_EFFECTS = frozenset(
    {"isolated_prototype", "repository_modify", "merge_main", "deploy"}
)
CONTINUATION_POLICIES = frozenset(
    {"stop", "ask", "explicitly-authorized-successor"}
)


class OutcomeGuardError(ValueError):
    """Raised when a contract or check input cannot be trusted."""


@dataclass(frozen=True, slots=True)
class PolicyResult:
    """Machine-readable result shared by every future enforcement adapter."""

    allowed: bool
    reason_code: str
    contract_ref: str | None = None
    governing_contract_ref: str | None = None
    outcome_reached: bool | None = None
    missing_evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESULT_SCHEMA_VERSION,
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "contract_ref": self.contract_ref,
            "governing_contract_ref": self.governing_contract_ref,
            "outcome_reached": self.outcome_reached,
            "missing_evidence_refs": list(self.missing_evidence_refs),
        }


class OutcomeGuard:
    """Small facade over immutable contracts in controller-owned state."""

    def __init__(self, state: ControllerState) -> None:
        self.state = state

    def register_contract(self, document: Mapping[str, object]) -> PolicyResult:
        normalized = _normalize_contract(document)
        contract_ref = str(normalized["contract_id"])
        canonical = _canonical_json(normalized)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        existing = self.state.connection.execute(
            "SELECT document_sha256 FROM outcome_contracts WHERE contract_ref = ?",
            (contract_ref,),
        ).fetchone()
        if existing is not None:
            if str(existing["document_sha256"]) == digest:
                return PolicyResult(True, "contract_already_registered", contract_ref)
            return PolicyResult(
                False,
                "contract_conflict",
                contract_ref,
                governing_contract_ref=contract_ref,
            )

        authority = normalized["authority_source"]
        assert isinstance(authority, dict)
        authority_ref = str(authority["authority_id"])
        authority_json = _canonical_json(authority)
        authority_digest = hashlib.sha256(authority_json.encode("utf-8")).hexdigest()
        authority_row = self.state.connection.execute(
            "SELECT document_sha256 FROM outcome_authorities WHERE authority_ref = ?",
            (authority_ref,),
        ).fetchone()
        if authority_row is not None and str(authority_row["document_sha256"]) != authority_digest:
            return PolicyResult(False, "authority_conflict", contract_ref)

        parents = tuple(
            str(item)
            for item in cast(list[str], normalized["parent_contract_refs"])
        )
        requested = frozenset(
            str(item) for item in cast(list[str], normalized["allowed_effects"])
        )
        for parent_ref in parents:
            parent = self._load_contract(parent_ref)
            if parent is None:
                return PolicyResult(False, "governing_contract_missing", contract_ref, parent_ref)
            governing = self._first_effect_violation(parent, requested)
            if governing is not None:
                return PolicyResult(False, "effect_broadens_ancestor", contract_ref, governing)

        successor_of = normalized.get("successor_of")
        if successor_of is not None:
            predecessor = self._load_contract(str(successor_of))
            if predecessor is None:
                return PolicyResult(
                    False, "predecessor_contract_missing", contract_ref, str(successor_of)
                )
            if predecessor["continuation_policy"] != "explicitly-authorized-successor":
                return PolicyResult(
                    False, "successor_not_authorized", contract_ref, str(successor_of)
                )
            predecessor_authority = predecessor["authority_source"]
            assert isinstance(predecessor_authority, dict)
            if authority_ref == predecessor_authority["authority_id"]:
                return PolicyResult(
                    False, "successor_requires_fresh_authority", contract_ref, str(successor_of)
                )

        try:
            self.state.connection.execute("BEGIN IMMEDIATE")
            self.state.connection.execute(
                """
                INSERT OR IGNORE INTO outcome_authorities
                    (authority_ref, document_json, document_sha256)
                VALUES (?, ?, ?)
                """,
                (authority_ref, authority_json, authority_digest),
            )
            self.state.connection.execute(
                """
                INSERT INTO outcome_contracts
                    (contract_ref, document_json, document_sha256, authority_ref)
                VALUES (?, ?, ?, ?)
                """,
                (contract_ref, canonical, digest, authority_ref),
            )
            self.state.connection.commit()
        except Exception:
            self.state.connection.rollback()
            raise
        return PolicyResult(True, "contract_registered", contract_ref)

    def get_contract(self, contract_ref: str) -> dict[str, object] | None:
        """Return a detached persisted contract document."""

        contract = self._load_contract(contract_ref)
        return dict(contract) if contract is not None else None

    def check_effect(self, contract_ref: str, effect: str) -> PolicyResult:
        contract = self._load_contract(contract_ref)
        if contract is None:
            return PolicyResult(False, "contract_missing", contract_ref)
        if effect not in KNOWN_EFFECTS:
            return PolicyResult(False, "unknown_effect", contract_ref, contract_ref)
        governing = self._first_effect_violation(contract, frozenset({effect}))
        if governing is not None:
            return PolicyResult(False, "effect_not_allowed", contract_ref, governing)
        return PolicyResult(True, "effect_allowed", contract_ref, contract_ref)

    def check_outcome(
        self,
        contract_ref: str,
        *,
        evidence: Sequence[Mapping[str, object]],
        task_status: str | None = None,
    ) -> PolicyResult:
        """Evaluate terminal evidence; task status is deliberately non-authoritative."""

        del task_status
        contract = self._load_contract(contract_ref)
        if contract is None:
            return PolicyResult(False, "contract_missing", contract_ref, outcome_reached=False)
        observed: set[tuple[str, str]] = set()
        for item in evidence:
            if not isinstance(item, Mapping):
                return PolicyResult(False, "malformed_evidence", contract_ref, outcome_reached=False)
            evidence_type = item.get("evidence_type")
            evidence_ref = item.get("evidence_ref")
            if not _non_empty(evidence_type) or not _non_empty(evidence_ref):
                return PolicyResult(False, "malformed_evidence", contract_ref, outcome_reached=False)
            observed.add((str(evidence_type), str(evidence_ref)))
        required = contract["terminal_evidence"]
        assert isinstance(required, list)
        missing = tuple(
            str(item["evidence_ref"])
            for item in required
            if (str(item["evidence_type"]), str(item["evidence_ref"])) not in observed
        )
        if missing:
            return PolicyResult(
                False,
                "terminal_evidence_missing",
                contract_ref,
                contract_ref,
                outcome_reached=False,
                missing_evidence_refs=missing,
            )
        return PolicyResult(
            True, "outcome_reached", contract_ref, contract_ref, outcome_reached=True
        )

    def _load_contract(self, contract_ref: str) -> dict[str, object] | None:
        row = self.state.connection.execute(
            "SELECT document_json FROM outcome_contracts WHERE contract_ref = ?",
            (contract_ref,),
        ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row["document_json"]))
        if not isinstance(value, dict):
            raise OutcomeGuardError("persisted outcome contract is malformed")
        return value

    def _first_effect_violation(
        self, contract: Mapping[str, object], requested: frozenset[str]
    ) -> str | None:
        allowed = frozenset(
            str(item) for item in cast(list[str], contract["allowed_effects"])
        )
        if not requested.issubset(allowed):
            return str(contract["contract_id"])
        for parent_ref in cast(list[str], contract["parent_contract_refs"]):
            parent = self._load_contract(str(parent_ref))
            if parent is None:
                return str(parent_ref)
            violation = self._first_effect_violation(parent, requested)
            if violation is not None:
                return violation
        return None


def _normalize_contract(document: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(document, Mapping):
        raise OutcomeGuardError("contract must be an object")
    if document.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        raise OutcomeGuardError(f"schema_version must be {CONTRACT_SCHEMA_VERSION!r}")
    for field in ("contract_id", "declared_outcome"):
        if not _non_empty(document.get(field)):
            raise OutcomeGuardError(f"{field} must be a non-empty string")
    continuation = document.get("continuation_policy")
    if continuation not in CONTINUATION_POLICIES:
        raise OutcomeGuardError("continuation_policy is invalid")
    effects = document.get("allowed_effects")
    if not isinstance(effects, list) or not effects:
        raise OutcomeGuardError("allowed_effects must be a non-empty list")
    if any(not _non_empty(item) for item in effects):
        raise OutcomeGuardError("allowed_effects must contain non-empty strings")
    unknown = sorted(set(str(item) for item in effects) - KNOWN_EFFECTS)
    if unknown:
        raise OutcomeGuardError(f"unknown effects: {', '.join(unknown)}")
    terminal = document.get("terminal_evidence")
    if not isinstance(terminal, list) or not terminal:
        raise OutcomeGuardError("terminal_evidence must be a non-empty list")
    normalized_terminal: list[dict[str, str]] = []
    for requirement in terminal:
        if not isinstance(requirement, Mapping):
            raise OutcomeGuardError("terminal_evidence requirements must be objects")
        evidence_type = requirement.get("evidence_type")
        evidence_ref = requirement.get("evidence_ref")
        if not _non_empty(evidence_type) or not _non_empty(evidence_ref):
            raise OutcomeGuardError(
                "terminal_evidence requires non-empty evidence_type and evidence_ref"
            )
        normalized_terminal.append(
            {"evidence_type": str(evidence_type), "evidence_ref": str(evidence_ref)}
        )
    authority = document.get("authority_source")
    if not isinstance(authority, Mapping):
        raise OutcomeGuardError("authority_source must be an object")
    required_authority = ("authority_id", "kind", "actor", "authorized_at", "statement")
    if any(not _non_empty(authority.get(field)) for field in required_authority):
        raise OutcomeGuardError(
            "authority_source requires authority_id, kind, actor, authorized_at, and statement"
        )
    if authority.get("kind") != "operator":
        raise OutcomeGuardError("authority_source kind must be 'operator'")
    parents = document.get("parent_contract_refs", [])
    if not isinstance(parents, list) or any(not _non_empty(item) for item in parents):
        raise OutcomeGuardError("parent_contract_refs must be a list of non-empty strings")
    successor = document.get("successor_of")
    if successor is not None and not _non_empty(successor):
        raise OutcomeGuardError("successor_of must be a non-empty string")
    normalized: dict[str, object] = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "contract_id": str(document["contract_id"]).strip(),
        "declared_outcome": str(document["declared_outcome"]).strip(),
        "terminal_evidence": normalized_terminal,
        "allowed_effects": sorted(set(str(item) for item in effects)),
        "continuation_policy": str(continuation),
        "authority_source": {
            field: str(authority[field]).strip() for field in required_authority
        },
        "parent_contract_refs": list(dict.fromkeys(str(item) for item in parents)),
    }
    if successor is not None:
        normalized["successor_of"] = str(successor)
    return normalized


def _non_empty(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
