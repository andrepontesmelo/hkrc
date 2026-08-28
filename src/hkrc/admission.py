"""HKRC-mediated child admission through the native kanban CLI.

A child task is admitted into dispatch only after its requested effect passes
the governing contract (and every ancestor contract) policy check. The native
CLI is the only interface to the board: the child is created in the
non-dispatchable ``blocked`` state with a deterministic idempotency key,
validation is re-run, durable authorization evidence is recorded in
controller-owned state, and only then is the child promoted. Every failure
path leaves the child non-dispatchable (or never creates it) and is recorded
as audit evidence; the controller never opens or mutates a Hermes/Kanban
SQLite database.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
import subprocess

from .config import ControllerConfig
from .outcome_guard import OutcomeGuard, PolicyResult
from .state import ControllerState


ADMISSION_KEY_PREFIX = "hkrc-admit:"


class AdmissionError(RuntimeError):
    """Raised when the native CLI cannot be driven safely (fail closed)."""


@dataclass(frozen=True, slots=True)
class NativeResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


NativeRunner = Callable[[Sequence[str]], NativeResult]


@dataclass(frozen=True, slots=True)
class AdmissionReport:
    """Machine-readable admission outcome, including denied/duplicate paths."""

    allowed: bool
    reason_code: str
    child_task_id: str | None
    duplicate: bool
    policy: PolicyResult | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hkrc.admission-result.v1",
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "child_task_id": self.child_task_id,
            "duplicate": self.duplicate,
            "policy": self.policy.to_dict() if self.policy is not None else None,
        }


def scrubbed_env() -> dict[str, str]:
    """Return the ambient environment without Hermes/Kanban dispatch context.

    Child admission must not inherit the caller's kanban ambient identity
    (``HERMES_KANBAN_TASK``, ``HERMES_KANBAN_BOARD``, and friends), so the
    native subprocess cannot be steered by leaked dispatch state.
    """

    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("HERMES_KANBAN_")
    }


def admit_child(
    config: ControllerConfig,
    state: ControllerState,
    *,
    parent_task_id: str,
    contract_ref: str,
    effect: str,
    board_slug: str,
    title: str,
    assignee: str,
    body: str | None = None,
    runner: NativeRunner | None = None,
) -> AdmissionReport:
    """Validate, create (blocked), record, and promote one admitted child.

    ``runner`` exists only for deterministic tests; production uses
    ``subprocess.run`` with an argv list and the scrubbed environment. The
    native CLI's ``--idempotency-key`` guarantees no duplicate child, and the
    controller-owned admission row is the durable lease guaranteeing no
    duplicate admission evidence.

    Raises :class:`AdmissionError` when the native CLI itself fails (create or
    promote); the child, if created, is left in the non-dispatchable blocked
    state and the failure is recorded as audit evidence.
    """

    guard = OutcomeGuard(state)
    native_runner = runner or _run_native
    admission_key = _admission_key(parent_task_id, contract_ref, effect)

    existing = state.connection.execute(
        "SELECT child_task_id, status FROM outcome_admissions "
        "WHERE admission_key = ?",
        (admission_key,),
    ).fetchone()
    if existing is not None:
        return AdmissionReport(
            allowed=existing["status"] == "admitted",
            reason_code="admission_already_recorded",
            child_task_id=existing["child_task_id"],
            duplicate=True,
        )

    policy = guard.check_effect(contract_ref, effect)
    if not policy.allowed:
        _record_admission(
            state,
            admission_key=admission_key,
            parent_task_id=parent_task_id,
            child_task_id=None,
            board_slug=board_slug,
            contract_ref=contract_ref,
            effect=effect,
            status="denied",
            policy=policy,
        )
        return AdmissionReport(False, policy.reason_code, None, duplicate=False, policy=policy)

    create_command = [
        config.native_cli,
        "kanban",
        "--board",
        board_slug,
        "create",
        "--title",
        title,
        "--assignee",
        assignee,
        "--parent",
        parent_task_id,
        "--initial-status",
        "blocked",
        "--idempotency-key",
        f"{ADMISSION_KEY_PREFIX}{admission_key}",
        "--created-by",
        "hkrc-outcome-guard",
        "--json",
    ]
    if body is not None:
        create_command.extend(["--body", body])
    created = native_runner(create_command)
    if created.returncode != 0:
        detail = (created.stderr or created.stdout or f"exit {created.returncode}").strip()
        _record_admission(
            state,
            admission_key=admission_key,
            parent_task_id=parent_task_id,
            child_task_id=None,
            board_slug=board_slug,
            contract_ref=contract_ref,
            effect=effect,
            status="failed",
            policy=policy,
        )
        raise AdmissionError(f"native kanban create failed: {detail}")
    child_task_id = _parse_task_id(created.stdout, created.stderr)
    if child_task_id is None:
        _record_admission(
            state,
            admission_key=admission_key,
            parent_task_id=parent_task_id,
            child_task_id=None,
            board_slug=board_slug,
            contract_ref=contract_ref,
            effect=effect,
            status="failed",
            policy=policy,
        )
        raise AdmissionError("native kanban create returned no task id")

    # Re-validate after creation: promote only when policy still allows it.
    revalidated = guard.check_effect(contract_ref, effect)
    if not revalidated.allowed:
        _record_admission(
            state,
            admission_key=admission_key,
            parent_task_id=parent_task_id,
            child_task_id=child_task_id,
            board_slug=board_slug,
            contract_ref=contract_ref,
            effect=effect,
            status="held",
            policy=revalidated,
        )
        return AdmissionReport(
            False, revalidated.reason_code, child_task_id, duplicate=False, policy=revalidated
        )

    _record_admission(
        state,
        admission_key=admission_key,
        parent_task_id=parent_task_id,
        child_task_id=child_task_id,
        board_slug=board_slug,
        contract_ref=contract_ref,
        effect=effect,
        status="admitted",
        policy=revalidated,
    )

    promote_command = [
        config.native_cli,
        "kanban",
        "promote",
        "--force",
        "--json",
        child_task_id,
        "hkrc-outcome-guard admission",
    ]
    promoted = native_runner(promote_command)
    if promoted.returncode != 0:
        detail = (promoted.stderr or promoted.stdout or f"exit {promoted.returncode}").strip()
        _update_admission_status(state, admission_key, "held")
        raise AdmissionError(
            f"native kanban promote failed for {child_task_id}: {detail} "
            "(child left blocked; no dispatch happened)"
        )
    return AdmissionReport(True, "admitted", child_task_id, duplicate=False, policy=revalidated)


def _admission_key(parent_task_id: str, contract_ref: str, effect: str) -> str:
    material = "\x00".join((parent_task_id, contract_ref, effect))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _record_admission(
    state: ControllerState,
    *,
    admission_key: str,
    parent_task_id: str,
    child_task_id: str | None,
    board_slug: str,
    contract_ref: str,
    effect: str,
    status: str,
    policy: PolicyResult,
) -> None:
    state.connection.execute(
        """
        INSERT OR IGNORE INTO outcome_admissions
            (admission_key, parent_task_id, child_task_id, board_slug,
             contract_ref, effect, status, policy_json, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            admission_key,
            parent_task_id,
            child_task_id,
            board_slug,
            contract_ref,
            effect,
            status,
            json.dumps(policy.to_dict(), sort_keys=True, separators=(",", ":")),
            _utc_now(),
        ),
    )
    state.connection.commit()


def _update_admission_status(state: ControllerState, admission_key: str, status: str) -> None:
    state.connection.execute(
        "UPDATE outcome_admissions SET status = ? WHERE admission_key = ?",
        (status, admission_key),
    )
    state.connection.commit()


def _parse_task_id(stdout: str, stderr: str) -> str | None:
    """Extract the created task id from the native CLI JSON result."""
    for blob in (stdout, stderr):
        for line in blob.splitlines():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            if payload.get("ok") is False:
                return None
            task_id = payload.get("task_id")
            if isinstance(task_id, str) and task_id.strip():
                return task_id.strip()
    return None


def _run_native(command: Sequence[str]) -> NativeResult:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
            env=scrubbed_env(),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _decode(exc.stdout)
        stderr = _decode(exc.stderr)
        return NativeResult(124, stdout, stderr or "native CLI timed out after 120 seconds")
    except OSError as exc:
        return NativeResult(127, "", str(exc))
    return NativeResult(completed.returncode, completed.stdout, completed.stderr)


def _decode(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = [
    "ADMISSION_KEY_PREFIX",
    "AdmissionError",
    "AdmissionReport",
    "NativeResult",
    "admit_child",
    "scrubbed_env",
]
