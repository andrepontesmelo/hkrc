"""Pure, fail-closed classification for normalized Kanban stream events.

The classifier is deliberately side-effect free.  It does not read native task
state, reserve a controller row, or invoke a native mutation.  Callers provide
an already-confirmed current task/run snapshot and use ``reserve`` plus the
composite ``reservation_key`` to perform those separate operations.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any


class PayloadState(str, Enum):
    """How the adapter decoded an event payload."""

    OBJECT = "object"
    NULL = "null"
    # ``malformed`` is the adapter's normalized value for valid event identity
    # with a non-object JSON payload.  The more specific values are accepted
    # for callers that distinguish malformed JSON from a wrong JSON shape.
    MALFORMED = "malformed"
    MALFORMED_JSON = "malformed_json"
    WRONG_JSON_SHAPE = "wrong_json_shape"


class ClassificationKind(str, Enum):
    """Stable classifier outcomes consumed by reservation/runtime code."""

    ACTIONABLE_TYPED_CAPABILITY = "actionable_typed_capability"
    ACTIONABLE_CIRCUIT_BREAKER = "actionable_circuit_breaker"
    ACTIONABLE_FAILURE_EVIDENCE = "actionable_failure_evidence"
    HUMAN_INPUT_REQUIRED = "human_input_required"
    DEPENDENCY_WAIT = "dependency_wait"
    TRANSIENT_RETRYABLE = "transient_retryable"
    LEGACY_OR_UNKNOWN_BLOCK = "legacy_or_unknown_block"
    HUMAN_TRIAGE_REQUIRED = "human_triage_required"
    TERMINAL_SUCCESS = "terminal_success"
    LIFECYCLE_CLEARING = "lifecycle_clearing"
    RUNNING_ALIVE = "running_alive"
    DISPATCHER_RECOVERY_OR_DEFER = "dispatcher_recovery_or_defer"
    LIFECYCLE_METADATA = "lifecycle_metadata"
    PROTOCOL_OR_INTEGRITY_WARNING = "protocol_or_integrity_warning"
    AUDIT_ONLY_FAILURE = "audit_only_failure"
    UNKNOWN_EVENT = "unknown_event"
    STALE_EVENT = "stale_event"
    CURRENT_STATE_UNCONFIRMED = "current_state_unconfirmed"
    TRANSPORT_ERROR = "transport_error"


class ClassifierInputError(ValueError):
    """Raised when a normalized event/current-state identity is unsafe to use."""


@dataclass(frozen=True, slots=True)
class NormalizedEvent:
    """The adapter's normalized event contract.

    ``payload`` is the decoded JSON object when ``payload_state`` is
    :attr:`PayloadState.OBJECT`; for all other states it is retained only as an
    opaque value for diagnostics and is never interpreted as a blocker kind.
    """

    board_slug: str
    event_id: int
    task_id: str
    run_id: int | str | None
    kind: str
    payload: object | None
    payload_state: PayloadState
    created_at: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> NormalizedEvent:
        required = (
            "board_slug",
            "event_id",
            "task_id",
            "run_id",
            "kind",
            "payload",
            "payload_state",
            "created_at",
        )
        missing = [name for name in required if name not in value]
        if missing:
            raise ClassifierInputError(
                "normalized event missing required field(s): " + ", ".join(missing)
            )
        raw_payload_state = value["payload_state"]
        try:
            raw_payload_state = getattr(raw_payload_state, "value", raw_payload_state)
            payload_state = (
                raw_payload_state
                if isinstance(raw_payload_state, PayloadState)
                else PayloadState(str(raw_payload_state))
            )
        except (TypeError, ValueError) as exc:
            raise ClassifierInputError("payload_state is invalid") from exc
        event = cls(
            board_slug=value["board_slug"],
            event_id=value["event_id"],
            task_id=value["task_id"],
            run_id=value["run_id"],
            kind=value["kind"],
            payload=value["payload"],
            payload_state=payload_state,
            created_at=value["created_at"],
        )
        return _validate_event(event)


@dataclass(frozen=True, slots=True)
class CurrentTaskState:
    """A read-only current task/run confirmation supplied by the caller."""

    task_id: str
    status: str
    block_kind: str | None = None
    latest_run_id: int | str | None = None
    run_outcome: str | None = None
    run_error: str | None = None
    run_summary: str | None = None
    latest_event_kind: str | None = None
    latest_event_id: int | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CurrentTaskState:
        required = ("task_id", "status", "block_kind")
        missing = [name for name in required if name not in value]
        if missing:
            raise ClassifierInputError(
                "current task state missing required field(s): " + ", ".join(missing)
            )
        state = cls(
            task_id=value["task_id"],
            status=value["status"],
            block_kind=value["block_kind"],
            latest_run_id=value.get("latest_run_id"),
            run_outcome=value.get("run_outcome"),
            run_error=value.get("run_error"),
            run_summary=value.get("run_summary"),
            latest_event_kind=value.get("latest_event_kind"),
            latest_event_id=value.get("latest_event_id"),
        )
        return _validate_current_state(state)


@dataclass(frozen=True, slots=True)
class StreamError:
    """An adapter/transport error, never a blocker cause."""

    code: str
    message: str
    cursor: int | None = None


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Pure classification output; reservation/native mutation are caller-owned."""

    classification: ClassificationKind
    reason: str
    actionable: bool = False
    reserve: bool = False
    board_slug: str | None = None
    task_id: str | None = None
    event_key: tuple[str, int] | None = None
    reservation_key: tuple[str, str] | None = None

    @property
    def kind(self) -> ClassificationKind:
        """Convenient alias for consumers that call the result's kind."""

        return self.classification


_KNOWN_BLOCK_KINDS = frozenset({"dependency", "needs_input", "capability", "transient"})
_HARD_FAILURE_EVENTS = frozenset({"crashed", "timed_out", "protocol_violation"})
_TERMINAL_STATUSES = frozenset({"done", "completed", "archived"})
_TRIAGE_STATUSES = frozenset({"triage"})
_LIFECYCLE_CLEARING_EVENTS = frozenset({"unblocked"})
_LIFECYCLE_TERMINAL_EVENTS = frozenset({"completed", "archived"})
_LIVENESS_EVENTS = frozenset({"claimed", "spawned", "claim_extended", "heartbeat"})
_DISPATCHER_EVENTS = frozenset(
    {"reclaimed", "stale", "reclaim_deferred", "rate_limited", "respawn_guarded"}
)
_METADATA_EVENTS = frozenset(
    {
        "created",
        "assigned",
        "linked",
        "unlinked",
        "commented",
        "edited",
        "specified",
        "decomposed",
        "scheduled",
        "promoted",
        "promoted_manual",
        "reprioritized",
        "attachment_added",
        "model_changed",
        "status_changed",
    }
)
_INTEGRITY_EVENTS = frozenset(
    {"completion_blocked_hallucination", "suspected_hallucinated_references"}
)
_SUPERSEDING_CURRENT_EVENTS = frozenset(
    {
        "gave_up",
        "reclaimed",
        "stale",
        "reclaim_deferred",
        "promoted",
        "promoted_manual",
        "status_changed",
        "claim_rejected",
        "claimed",
        "spawned",
        "claim_extended",
    }
)


def classify_event(
    event: NormalizedEvent | Mapping[str, Any] | object,
    current_state: CurrentTaskState | Mapping[str, Any],
    *,
    board_slug: str | None = None,
) -> ClassificationResult:
    """Classify one normalized event against a caller-confirmed current state.

    Current terminal/triage state is considered before event history.  For a
    ``blocked`` event, a typed object payload and matching current
    ``block_kind`` are both required for an actionable capability result.
    Prose is diagnostic only.  Unknown, untyped, malformed, and null data
    always fail closed.
    """

    normalized = _coerce_event(event, board_slug=board_slug)
    current = _coerce_current_state(current_state)
    if normalized.task_id != current.task_id:
        raise ClassifierInputError(
            f"event task_id {normalized.task_id!r} does not match current task "
            f"{current.task_id!r}"
        )
    event_key = (normalized.board_slug, normalized.event_id)
    reservation_key = (normalized.board_slug, normalized.task_id)
    base = {
        "board_slug": normalized.board_slug,
        "task_id": normalized.task_id,
        "event_key": event_key,
        "reservation_key": reservation_key,
    }

    # Current state has precedence over stale event history.
    if current.status in _TERMINAL_STATUSES:
        return _result(ClassificationKind.TERMINAL_SUCCESS, "current_state_terminal", **base)
    if current.status in _TRIAGE_STATUSES or normalized.kind == "block_loop_detected":
        return _result(ClassificationKind.HUMAN_TRIAGE_REQUIRED, "current_state_triage", **base)

    # A current snapshot can lag the event's task status in an integration seam,
    # but a newer lifecycle event in that snapshot is still authoritative for
    # this candidate.  Treat equality as newer-or-equal: the observed event is
    # already represented by the current state and must not be reserved again.
    if (
        current.latest_event_id is not None
        and current.latest_event_id >= normalized.event_id
        and current.latest_event_kind in _LIFECYCLE_CLEARING_EVENTS
    ):
        return _result(ClassificationKind.STALE_EVENT, "current_state_supersedes_event", **base)
    if (
        current.latest_event_id is not None
        and current.latest_event_id >= normalized.event_id
        and current.latest_event_kind in _LIFECYCLE_TERMINAL_EVENTS
    ):
        return _result(ClassificationKind.TERMINAL_SUCCESS, "current_state_terminal_event", **base)
    if (
        current.latest_event_id is not None
        and current.latest_event_id >= normalized.event_id
        and current.latest_event_kind == "block_loop_detected"
    ):
        return _result(ClassificationKind.HUMAN_TRIAGE_REQUIRED, "current_state_triage_event", **base)
    if (
        current.latest_event_id is not None
        and current.latest_event_id > normalized.event_id
        and current.latest_event_kind in _SUPERSEDING_CURRENT_EVENTS
    ):
        return _result(ClassificationKind.STALE_EVENT, "current_state_supersedes_event", **base)

    # A run mismatch means the event belongs to an older/replayed attempt.
    if (
        normalized.run_id is not None
        and current.latest_run_id is not None
        and normalized.run_id != current.latest_run_id
    ):
        return _result(ClassificationKind.STALE_EVENT, "run_id_not_current", **base)
    if current.latest_event_id is not None and normalized.event_id < current.latest_event_id:
        return _result(ClassificationKind.STALE_EVENT, "older_than_current_event", **base)

    if normalized.kind in _LIFECYCLE_TERMINAL_EVENTS:
        return _result(ClassificationKind.TERMINAL_SUCCESS, "terminal_event", **base)
    if normalized.kind in _LIFECYCLE_CLEARING_EVENTS:
        return _result(ClassificationKind.LIFECYCLE_CLEARING, "unblocked_lifecycle_event", **base)
    if normalized.kind in _LIVENESS_EVENTS:
        return _result(ClassificationKind.RUNNING_ALIVE, "liveness_event", **base)
    if normalized.kind in _DISPATCHER_EVENTS:
        return _result(ClassificationKind.DISPATCHER_RECOVERY_OR_DEFER, "dispatcher_lifecycle_event", **base)
    if normalized.kind in _METADATA_EVENTS:
        return _result(ClassificationKind.LIFECYCLE_METADATA, "lifecycle_metadata_event", **base)
    if normalized.kind in _INTEGRITY_EVENTS:
        return _result(ClassificationKind.PROTOCOL_OR_INTEGRITY_WARNING, "integrity_warning_event", **base)

    if (
        normalized.kind == "gave_up" or normalized.kind in _HARD_FAILURE_EVENTS
    ) and normalized.payload_state is not PayloadState.OBJECT:
        return _result(
            ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK,
            f"{normalized.kind}_payload_{normalized.payload_state.value}",
            **base,
        )

    if normalized.kind == "gave_up":
        return _classify_gave_up(normalized, current, base)
    if normalized.kind in _HARD_FAILURE_EVENTS:
        if current.status == "blocked":
            if current.block_kind in {"needs_input", "dependency", "transient"}:
                return _result(
                    _skip_kind(current.block_kind),
                    f"current_block_kind_{current.block_kind}",
                    **base,
                )
            if current.latest_event_kind == "gave_up":
                return _result(ClassificationKind.STALE_EVENT, "superseded_by_gave_up", **base)
            return _result(
                ClassificationKind.ACTIONABLE_FAILURE_EVIDENCE,
                f"current_blocked_{normalized.kind}",
                actionable=True,
                reserve=True,
                **base,
            )
        return _result(ClassificationKind.AUDIT_ONLY_FAILURE, "current_state_not_blocked", **base)

    if normalized.kind == "dependency_wait":
        return _result(ClassificationKind.DEPENDENCY_WAIT, "dependency_wait_event", **base)
    if normalized.kind != "blocked":
        return _result(ClassificationKind.UNKNOWN_EVENT, "unknown_event_kind", **base)

    if current.status != "blocked":
        return _result(ClassificationKind.STALE_EVENT, "current_state_not_blocked", **base)
    return _classify_blocked(normalized, current, base)


def classify_stream_error(
    error: StreamError | Mapping[str, Any] | object,
) -> ClassificationResult:
    """Return a non-actionable result for every adapter/transport error."""

    if isinstance(error, Mapping):
        missing = [name for name in ("code", "message") if name not in error]
        if missing:
            raise ClassifierInputError(
                "stream error missing required field(s): " + ", ".join(missing)
            )
        error = StreamError(
            code=getattr(error["code"], "value", error["code"]),
            message=error["message"],
            cursor=error.get("cursor"),
        )
    if not isinstance(error, StreamError):
        code = getattr(error, "code", None)
        message = getattr(error, "message", None)
        if code is None or message is None:
            raise ClassifierInputError("stream error must be a StreamError or mapping")
        error = StreamError(
            code=getattr(code, "value", code),
            message=message,
            cursor=getattr(error, "cursor", None),
        )
    error_code = getattr(error.code, "value", error.code)
    if not isinstance(error_code, str) or not error_code:
        raise ClassifierInputError("stream error code must be a non-empty string")
    if not isinstance(error.message, str):
        raise ClassifierInputError("stream error message must be a string")
    return ClassificationResult(
        classification=ClassificationKind.TRANSPORT_ERROR,
        reason=f"transport_error:{error_code}",
    )


_RUNTIME_CAP_ZERO_RE = re.compile(
    r"limit\s*0s|limit_seconds.{0,4}0\b|elapsed \d+s > limit 0s"
)


def _is_runtime_cap_zero_defect(
    payload: Mapping[str, Any] | None,
    run_error: str | None,
) -> bool:
    """True when a gave_up carries the per-task runtime-cap-zero signature.

    The dispatcher writes ``elapsed 61s > limit 0s`` (payload ``error`` and/or
    the run's ``error``) when a task created with ``--max-runtime 0`` is
    SIGTERM'd at the ~60s check on every run.  Both surfaces are checked so a
    payload-only or run-only sighting still escalates.
    """
    if run_error and _RUNTIME_CAP_ZERO_RE.search(run_error):
        return True
    if payload is not None:
        error = payload.get("error")
        if isinstance(error, str) and _RUNTIME_CAP_ZERO_RE.search(error):
            return True
    return False


def _classify_gave_up(
    event: NormalizedEvent,
    current: CurrentTaskState,
    base: dict[str, Any],
) -> ClassificationResult:
    if current.status != "blocked":
        return _result(ClassificationKind.STALE_EVENT, "current_state_not_blocked", **base)
    if current.block_kind in {"needs_input", "dependency", "transient"}:
        return _result(
            _skip_kind(current.block_kind),
            f"current_block_kind_{current.block_kind}",
            **base,
        )
    if current.latest_event_kind == "gave_up" and current.latest_event_id not in {
        None,
        event.event_id,
    }:
        return _result(ClassificationKind.STALE_EVENT, "superseded_by_newer_gave_up", **base)
    payload = _object_payload(event)
    details: list[str] = []
    if payload is not None:
        for key in ("error", "trigger_outcome", "failures"):
            if key in payload and payload[key] is not None:
                details.append(f"{key}={payload[key]}")
    if current.run_error:
        details.append(f"error={current.run_error}")
    if current.run_outcome and current.run_outcome != "gave_up":
        details.append(f"run_outcome={current.run_outcome}")
    reason = "gave_up_current_blocked"
    if details:
        reason += "; " + ", ".join(details)
    if _is_runtime_cap_zero_defect(payload, current.run_error):
        # A task created with --max-runtime 0 (or an equivalent cap) is
        # SIGTERM'd at ~60s on EVERY run ("elapsed Ns > limit 0s"). Unblocking
        # it is a no-op loop: the next run dies identically. Escalate to a
        # human with the config defect named instead of consuming the one-ever
        # reservation on a blind unblock (verified 2026-08-06: one live-board
        # task re-died twice after the daemon's unblock until the operator
        # set max_runtime_seconds to NULL).
        return _result(
            ClassificationKind.HUMAN_TRIAGE_REQUIRED,
            reason + "; config_defect=per_task_runtime_cap_zero",
            actionable=True,
            reserve=False,
            **base,
        )
    return _result(
        ClassificationKind.ACTIONABLE_CIRCUIT_BREAKER,
        reason,
        actionable=True,
        reserve=True,
        **base,
    )


def _classify_blocked(
    event: NormalizedEvent,
    current: CurrentTaskState,
    base: dict[str, Any],
) -> ClassificationResult:
    if event.payload_state is not PayloadState.OBJECT:
        return _result(
            ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK,
            f"blocked_payload_{event.payload_state.value}",
            **base,
        )
    payload = _object_payload(event)
    if payload is None:
        return _result(ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK, "blocked_payload_not_object", **base)
    payload_kind = payload.get("kind")
    if not isinstance(payload_kind, str) or payload_kind not in _KNOWN_BLOCK_KINDS:
        return _result(ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK, "blocked_kind_unknown_or_missing", **base)

    # Current typed state is authoritative for a presently blocked task.  A
    # mismatch is never promoted from prose or a stale event into recovery.
    if current.block_kind not in _KNOWN_BLOCK_KINDS or current.block_kind != payload_kind:
        if current.block_kind in _KNOWN_BLOCK_KINDS:
            return _result(
                _skip_kind(current.block_kind),
                f"current_block_kind_{current.block_kind}",
                **base,
            ) if current.block_kind != "capability" else _result(
                ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK,
                "payload_kind_does_not_match_current_block_kind",
                **base,
            )
        return _result(ClassificationKind.LEGACY_OR_UNKNOWN_BLOCK, "current_block_kind_unknown_or_missing", **base)

    if payload_kind == "capability":
        reason = _diagnostic_reason(payload, current, fallback="capability_block_current")
        return _result(
            ClassificationKind.ACTIONABLE_TYPED_CAPABILITY,
            reason,
            actionable=True,
            reserve=True,
            **base,
        )
    return _result(_skip_kind(payload_kind), f"typed_block_kind_{payload_kind}", **base)


def _skip_kind(kind: str) -> ClassificationKind:
    return {
        "needs_input": ClassificationKind.HUMAN_INPUT_REQUIRED,
        "dependency": ClassificationKind.DEPENDENCY_WAIT,
        "transient": ClassificationKind.TRANSIENT_RETRYABLE,
    }[kind]


def _diagnostic_reason(
    payload: Mapping[str, Any], current: CurrentTaskState, *, fallback: str
) -> str:
    for value in (payload.get("reason"), current.run_error, current.run_summary):
        if isinstance(value, str) and value:
            return value
    return fallback


def _object_payload(event: NormalizedEvent) -> Mapping[str, Any] | None:
    if event.payload_state is not PayloadState.OBJECT or not isinstance(event.payload, Mapping):
        return None
    return event.payload


def _result(
    classification: ClassificationKind,
    reason: str,
    *,
    actionable: bool = False,
    reserve: bool = False,
    **keys: Any,
) -> ClassificationResult:
    return ClassificationResult(
        classification=classification,
        reason=reason,
        actionable=actionable,
        reserve=reserve,
        **keys,
    )


def _coerce_event(
    value: NormalizedEvent | Mapping[str, Any] | object,
    *,
    board_slug: str | None,
) -> NormalizedEvent:
    if isinstance(value, NormalizedEvent):
        if board_slug is not None and value.board_slug != board_slug:
            raise ClassifierInputError(
                f"event board_slug {value.board_slug!r} does not match "
                f"requested board {board_slug!r}"
            )
        return _validate_event(value)
    if isinstance(value, Mapping):
        if board_slug is not None:
            if "board_slug" in value and value["board_slug"] != board_slug:
                raise ClassifierInputError(
                    f"event board_slug {value['board_slug']!r} does not match "
                    f"requested board {board_slug!r}"
                )
            if "board_slug" not in value:
                value = {**value, "board_slug": board_slug}
        return NormalizedEvent.from_mapping(value)
    # StreamAdapter's event object intentionally keeps board identity on its
    # enclosing EventBatch.  Accept that normalized object directly when the
    # caller supplies the batch's board slug, without importing or coupling the
    # classifier to the sibling transport module.
    event_id = getattr(value, "event_id", getattr(value, "id", None))
    fields = {
        "board_slug": board_slug,
        "event_id": event_id,
        "task_id": getattr(value, "task_id", None),
        "run_id": getattr(value, "run_id", None),
        "kind": getattr(value, "kind", None),
        "payload": getattr(value, "payload", None),
        "payload_state": getattr(value, "payload_state", None),
        "created_at": getattr(value, "created_at", None),
    }
    if any(
        fields[name] is None
        for name in ("board_slug", "event_id", "task_id", "kind", "payload_state", "created_at")
    ):
        raise ClassifierInputError(
            "event must be a NormalizedEvent/mapping or adapter event with board_slug"
        )
    raw_payload_state = getattr(fields["payload_state"], "value", fields["payload_state"])
    fields["payload_state"] = raw_payload_state
    return NormalizedEvent.from_mapping(fields)


def _coerce_current_state(
    value: CurrentTaskState | Mapping[str, Any],
) -> CurrentTaskState:
    if isinstance(value, CurrentTaskState):
        return _validate_current_state(value)
    if isinstance(value, Mapping):
        return CurrentTaskState.from_mapping(value)
    raise ClassifierInputError("current state must be a CurrentTaskState or mapping")


def _validate_event(event: NormalizedEvent) -> NormalizedEvent:
    if not isinstance(event.board_slug, str) or not event.board_slug:
        raise ClassifierInputError("board_slug must be a non-empty string")
    if isinstance(event.event_id, bool) or not isinstance(event.event_id, int) or event.event_id <= 0:
        raise ClassifierInputError("event_id must be a positive integer")
    if not isinstance(event.task_id, str) or not event.task_id:
        raise ClassifierInputError("task_id must be a non-empty string")
    if event.run_id is not None and (
        isinstance(event.run_id, bool)
        or not isinstance(event.run_id, (int, str))
        or (isinstance(event.run_id, str) and not event.run_id)
    ):
        raise ClassifierInputError("run_id must be null, a non-empty string, or an integer")
    if not isinstance(event.kind, str) or not event.kind:
        raise ClassifierInputError("kind must be a non-empty string")
    if (
        isinstance(event.created_at, bool)
        or not isinstance(event.created_at, int)
        or event.created_at < 0
    ):
        raise ClassifierInputError("created_at must be a nonnegative integer")
    if not isinstance(event.payload_state, PayloadState):
        try:
            raw_state = getattr(event.payload_state, "value", event.payload_state)
            event = replace(event, payload_state=PayloadState(str(raw_state)))
        except (TypeError, ValueError) as exc:
            raise ClassifierInputError("payload_state is invalid") from exc
    if event.payload_state is PayloadState.OBJECT and not isinstance(event.payload, Mapping):
        raise ClassifierInputError("object payload must be a mapping")
    if event.payload_state is PayloadState.NULL and event.payload is not None:
        raise ClassifierInputError("null payload state must carry a null payload")
    return event


def _validate_current_state(state: CurrentTaskState) -> CurrentTaskState:
    if not isinstance(state.task_id, str) or not state.task_id:
        raise ClassifierInputError("current task_id must be a non-empty string")
    if not isinstance(state.status, str) or not state.status:
        raise ClassifierInputError("current status must be a non-empty string")
    if state.block_kind is not None and (
        not isinstance(state.block_kind, str) or not state.block_kind
    ):
        raise ClassifierInputError("current block_kind must be null or a non-empty string")
    if state.latest_run_id is not None and (
        isinstance(state.latest_run_id, bool)
        or not isinstance(state.latest_run_id, (int, str))
        or (isinstance(state.latest_run_id, str) and not state.latest_run_id)
    ):
        raise ClassifierInputError("latest_run_id must be null, a non-empty string, or an integer")
    if state.latest_event_id is not None and (
        isinstance(state.latest_event_id, bool)
        or not isinstance(state.latest_event_id, int)
        or state.latest_event_id <= 0
    ):
        raise ClassifierInputError("latest_event_id must be null or a positive integer")
    return state


__all__ = [
    "ClassificationKind",
    "ClassificationResult",
    "ClassifierInputError",
    "CurrentTaskState",
    "NormalizedEvent",
    "PayloadState",
    "StreamError",
    "classify_event",
    "classify_stream_error",
]
