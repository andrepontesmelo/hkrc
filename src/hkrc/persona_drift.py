"""Report-only drift flagging against the persona matrix (t_f7cc2828).

Effect boundary (t_7dca44ce batch-1 #3): HKRC never writes ``~/.hermes``
profile state. This module only READS live profile configs
(``~/.hermes/profiles/*/config.yaml`` — model, reasoning_effort, overrides)
and returns report-only findings. Nothing here opens a file for writing;
the read path is exercised in tests against fixture files under ``tmp_path``.

Findings follow the t_f7cc2828 contract — flag ``unset`` (reasoning missing
where the matrix requires high), ``none`` (reasoning disabled on a matrix
persona), and ``wrong model`` per matrix — plus a matrix-mismatched
reasoning level on the reasoning-required personas, a standing
model/reasoning override on an active persona ("NO overrides at any level";
authoritative's documented ``pro -> max`` exception aside), and the presence
of a configured eliminated persona (griller) pending operator retirement.

The pure entry point ``check_snapshot`` never mutates its input. The live
sweep ``check_profiles`` only opens files for reading.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
import re

from .persona_matrix import (
    AUTHORITATIVE_ALLOWED_OVERRIDE,
    MODEL_FLASH,
    MODEL_LUNA_HIGH,
    MODEL_PRO,
    STATUS_ELIMINATED,
    persona_spec,
)

WRONG_MODEL = "wrong-model"
REASONING_UNSET = "reasoning-unset"
REASONING_NONE = "reasoning-none"
REASONING_MISMATCH = "reasoning-mismatch"
OVERRIDE_PRESENT = "override-present"
ELIMINATED_PERSONA = "eliminated-persona"

#: Reasoning levels that count as "reasoning disabled" regardless of the
#: matrix row, matching the t_7dca44ce decision that removed the
#: ``--reasoning none`` path from the loop.
_DISABLED_REASONING = {"none", "off"}


@dataclass(frozen=True, slots=True)
class ProfileSnapshot:
    """Parsed view of one profile ``config.yaml`` (the flagger's read surface).

    ``model`` is the profile's default model id (e.g.
    ``opencode-go/deepseek-v4-flash``), ``reasoning_effort`` is the
    ``agent.reasoning_effort`` value or None when the key is absent, and
    ``reasoning_overrides`` are the ``agent.reasoning_overrides`` pairs.
    """

    persona: str
    model: str | None = None
    reasoning_effort: str | None = None
    reasoning_overrides: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class DriftFinding:
    """One report-only finding: persona, stable code, human message."""

    persona: str
    code: str
    message: str


def check_snapshot(snapshot: ProfileSnapshot) -> list[DriftFinding]:
    """Compare one profile snapshot against the matrix.

    Pure and read-only: ``snapshot`` is never mutated and a fresh list of
    findings is returned. Profiles not in the matrix are out of scope and
    yield no findings.
    """
    spec = persona_spec(snapshot.persona)
    if spec is None:
        return []
    if spec.status == STATUS_ELIMINATED:
        if snapshot.model or snapshot.reasoning_effort or snapshot.reasoning_overrides:
            return [
                DriftFinding(
                    snapshot.persona,
                    ELIMINATED_PERSONA,
                    f"persona {snapshot.persona} is eliminated by the matrix "
                    "(grilling is a chat conversation; cards capture decisions "
                    "only); the configured profile must be retired by the "
                    "operator runbook (docs/persona-matrix-runbook.md)",
                )
            ]
        return []
    findings: list[DriftFinding] = []
    if spec.model is not None:
        if snapshot.model is None:
            findings.append(
                DriftFinding(
                    snapshot.persona,
                    WRONG_MODEL,
                    f"model unset; matrix requires {spec.model!r}",
                )
            )
        else:
            tier = _model_tier(snapshot.model)
            if tier != spec.model:
                findings.append(
                    DriftFinding(
                        snapshot.persona,
                        WRONG_MODEL,
                        f"model {snapshot.model!r} (tier {tier!r}) != "
                        f"matrix {spec.model!r}",
                    )
                )
    if snapshot.reasoning_effort is not None and (
        snapshot.reasoning_effort.lower() in _DISABLED_REASONING
    ):
        findings.append(
            DriftFinding(
                snapshot.persona,
                REASONING_NONE,
                f"reasoning_effort is {snapshot.reasoning_effort!r} "
                "(reasoning disabled)",
            )
        )
    elif spec.reasoning is not None:
        if snapshot.reasoning_effort is None:
            findings.append(
                DriftFinding(
                    snapshot.persona,
                    REASONING_UNSET,
                    f"reasoning_effort unset; matrix requires {spec.reasoning!r}",
                )
            )
        elif snapshot.reasoning_effort != spec.reasoning:
            findings.append(
                DriftFinding(
                    snapshot.persona,
                    REASONING_MISMATCH,
                    f"reasoning_effort {snapshot.reasoning_effort!r} != "
                    f"matrix {spec.reasoning!r}",
                )
            )
    for override_model, level in snapshot.reasoning_overrides:
        if (
            snapshot.persona == "authoritative"
            and (override_model, level) == AUTHORITATIVE_ALLOWED_OVERRIDE
        ):
            continue
        findings.append(
            DriftFinding(
                snapshot.persona,
                OVERRIDE_PRESENT,
                f"reasoning override {override_model} -> {level} "
                "(no overrides at any level)",
            )
        )
    return findings


def check_snapshots(snapshots: Iterable[ProfileSnapshot]) -> list[DriftFinding]:
    """Aggregate findings across snapshots without mutating the input."""
    findings: list[DriftFinding] = []
    for snapshot in snapshots:
        findings.extend(check_snapshot(snapshot))
    return findings


def default_profiles_root() -> Path:
    """Return the native profiles root the sweep reads (never writes).

    ``HKRC_PROFILES_ROOT`` overrides the default ``~/.hermes/profiles`` (the
    same override pattern as ``HKRC_INSTANCE_ROOT``); without it the sweep
    resolves via ``Path.home()``, which is profile-redirected inside a
    Hermes worker session.
    """
    configured = os.environ.get("HKRC_PROFILES_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".hermes" / "profiles"


def read_profile_config(path: Path) -> ProfileSnapshot:
    """Parse the matrix-relevant keys out of one Hermes profile config.yaml.

    Deliberately a narrow YAML-subset scanner (HKRC has no third-party
    dependencies): it extracts top-level ``model.default`` and the
    ``agent.reasoning_effort`` / ``agent.reasoning_overrides`` keys, and
    ignores everything else. ``reasoning_overrides`` is accepted either as a
    single-line JSON string (``'{"model": "level"}'``) or as an indented
    mapping of ``model: level`` lines. Read-only: the file is only opened
    for reading, never written.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    persona = path.parent.name
    model: str | None = None
    reasoning_effort: str | None = None
    overrides: list[tuple[str, str]] = []
    section: str | None = None
    in_overrides = False
    for raw in lines:
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0:
            in_overrides = False
            key = _scalar(line).rstrip(":")
            if key == "model" and _inline_mapping_default(line):
                model = _inline_mapping_default(line)
                section = None
                continue
            section = key if key in ("model", "agent") else None
            continue
        if section == "model" and indent == 2:
            key, value = _kv(line)
            if key == "default":
                model = value
        elif section == "agent" and indent == 2:
            key, value = _kv(line)
            in_overrides = False
            if key == "reasoning_effort":
                reasoning_effort = value
            elif key == "reasoning_overrides":
                if value is not None:
                    overrides.extend(_parse_overrides(value))
                else:
                    in_overrides = True
        elif section == "agent" and indent == 4 and in_overrides:
            key, value = _kv(line)
            if key is not None and value is not None:
                overrides.append((key, value))
    return ProfileSnapshot(
        persona=persona,
        model=model,
        reasoning_effort=reasoning_effort,
        reasoning_overrides=tuple(overrides),
    )


def check_profiles(profiles_root: Path | None = None) -> list[DriftFinding]:
    """Sweep every matrix-persona profile under the profiles root.

    Read-only sweep: only ``config.yaml`` files of matrix personas are
    opened, and only for reading. Profiles outside the matrix are skipped
    (HIGH scope = matrix personas only). A missing root or a persona
    directory without a ``config.yaml`` yields no findings — an operator
    that retires an eliminated persona by removing its config closes the
    finding.
    """
    root = profiles_root if profiles_root is not None else default_profiles_root()
    if not root.is_dir():
        return []
    findings: list[DriftFinding] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if persona_spec(entry.name) is None:
            continue
        config = entry / "config.yaml"
        if not config.is_file():
            continue
        findings.extend(check_snapshot(read_profile_config(config)))
    return findings


def _scalar(text: str) -> str:
    """Strip an inline comment, surrounding quotes, and whitespace."""
    value = text
    if " #" in value:
        value = value.split(" #", 1)[0]
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    return value


def _model_tier(model_id: str | None) -> str | None:
    """Map a live model id to its matrix tier (flash, pro, or luna-high).

    The matrix is expressed in tiers (t_49ba1035: developer = flash, senior
    dev = pro; frontend-dev = luna-high, t_545a638a); live configs carry
    full ids such as ``opencode-go/deepseek-v4-flash``, ``zai/glm-5.3`` or
    ``cx/gpt-5.6-luna-high``. An id naming neither tier nor the explicit
    pro id is unrecognized and therefore drift — the matrix allows flash,
    pro, or luna-high only.
    """
    if model_id is None:
        return None
    lowered = model_id.lower()
    if MODEL_FLASH in lowered:
        return MODEL_FLASH
    if MODEL_PRO in lowered:
        return MODEL_PRO
    if MODEL_LUNA_HIGH in lowered:
        return MODEL_LUNA_HIGH
    if lowered == "zai/glm-5.3":
        return MODEL_PRO
    return None


def _kv(line: str) -> tuple[str, str | None]:
    """Split a YAML ``key: value`` line; bare keys yield value None."""
    if ":" not in line:
        return (line.strip(), None)
    key, _, rest = line.partition(":")
    if not rest.strip():
        return (_scalar(key), None)
    return (_scalar(key), _scalar(rest))


def _inline_mapping_default(line: str) -> str | None:
    """Extract ``default`` from an inline ``model: {default: <name>}`` map."""
    if "{" not in line or "}" not in line:
        return None
    match = re.search(r"default\s*:\s*([^,}]+)", line)
    if match is None:
        return None
    return _scalar(match.group(1))


def _parse_overrides(value: str) -> list[tuple[str, str]]:
    """Parse a reasoning_overrides value into (model, level) pairs.

    Accepts a single-line JSON object string (the shape Hermes writes for
    scalar overrides) or a bare scalar (no pairs). Nested mapping blocks
    are handled by the line scanner, not here.
    """
    stripped = value.strip()
    if not stripped:
        return []
    if stripped.startswith("{"):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, dict):
            return [(str(key), str(level)) for key, level in parsed.items()]
    return []
