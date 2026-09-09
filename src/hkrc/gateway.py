"""Create-time gateway checks G1-G13 for advisory enforcement.

Deterministic validation of card-creation (and completion) requests so bad
cards are rejected before they burn dispatch runs. Every check is a pure
function over an explicit request; the only subprocess call is the G6
``git check-ref-format`` probe, which is injectable for deterministic tests
(same pattern as ``admission.NativeRunner``). This is validation, not
detection: nothing here reads a live board or mutates state.

Severity contract per check (documented decisions):

- G1  reject: goal-mode card with ``body=None``; warn: body missing Done
      criteria or terminal-instruction markers.
- G2  reject: missing/blank idempotency key.
- G3  reject: ``--branch`` with scratch workspace; sim/prototype card whose
      workspace is not an absolute-path worktree.
- G4  reject: ``--project``/``--workspace`` on a plain review card.
- G5  reject: per-card model/provider override flags (dispatch crashes on
      them).
- G6  reject: title yields an empty slug or the slug fails
      ``git check-ref-format --branch``.
- G7  reject: self-parent cycle, unknown parent, or unverifiable parent
      existence (fail closed).
- G8  reject: impl body stating completion requires its parent reviewer.
- G9  reject: review body missing the "leave card N blocked" line or the
      seeded-corpus probe requirement.
- G10 reject (complete-time): ``complete`` on a todo card with zero runs
      (use archive instead).
- G11 warn (document-only): flags placed before the positional title; the
      native parser already hard-rejects that syntax.
- G12 reject: ``max_runtime_seconds`` neither None nor a positive int.
- G13 reject: prompt template that does not render ``{board_slug}`` (the
      ``{task_id}``-only bug class) or fails to render at all.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
import re
import subprocess


REJECT = "reject"
WARN = "warn"

SimKinds = frozenset({"sim", "simulation", "prototype"})

_G1_DONE_CRITERIA = re.compile(
    r"done criteria|acceptance criteria|definition of done|done when",
    re.IGNORECASE,
)
_G1_TERMINAL = re.compile(
    r"kanban_(?:complete|block)|terminal instruction",
    re.IGNORECASE,
)
_G8_DEADLOCK = re.compile(
    r"(?:parent\s+(?:reviewer|review|approval)"
    r"[^\n]{0,80}?(?:must|before|only|required)[^\n]{0,80}?complet\w*"
    r"|complet\w*[^\n]{0,80}?(?:requires?|needs?|after|once|until|must|before)"
    r"[^\n]{0,80}?parent\s+(?:reviewer|review|approval))",
    re.IGNORECASE,
)
_G9_LEAVE_BLOCKED = re.compile(
    r"leave\s+card\s+\S+\s+blocked",
    re.IGNORECASE,
)
_G9_SEEDED_CORPUS = re.compile(r"seeded[- ]corpus", re.IGNORECASE)


class GatewayError(ValueError):
    """Raised when a gateway request itself cannot be interpreted."""


@dataclass(frozen=True, slots=True)
class GatewayViolation:
    """One failed (or warned) gateway check, machine-readable."""

    code: str
    message: str
    severity: str


@dataclass(frozen=True, slots=True)
class GatewayRequest:
    """Explicit create-time inputs the gateway checks judge.

    Only ``title`` is required; every other field defaults to "not
    requested", which is exactly the state the checks must police. ``parents``
    holds ``(parent_task_id, parent_board_slug_or_None)`` pairs; a None board
    means "the board this card is created on".
    """

    title: str
    board_slug: str = ""
    body: str | None = None
    card_kind: str = "impl"
    goal_mode: bool = False
    idempotency_key: str | None = None
    branch: str | None = None
    workspace_kind: str | None = None
    workspace_path: str | None = None
    project: str | None = None
    workspace: str | None = None
    model: str | None = None
    provider: str | None = None
    parents: tuple[tuple[str, str | None], ...] = ()
    task_id: str | None = None
    max_runtime_seconds: int | None = None
    prompt_template: str | None = None
    command: Sequence[str] | None = None
    parent_exists: Callable[[str, str], bool] | None = None
    ref_format_check: Callable[[str], bool] | None = None


@dataclass(frozen=True, slots=True)
class GatewayResult:
    """Aggregated gateway outcome; ``ok`` is False only on reject severity."""

    violations: tuple[GatewayViolation, ...]

    @property
    def ok(self) -> bool:
        return not any(violation.severity == REJECT for violation in self.violations)

    @property
    def reason_code(self) -> str | None:
        for violation in self.violations:
            if violation.severity == REJECT:
                return violation.code
        return None


def run_gateway_checks(request: GatewayRequest) -> GatewayResult:
    """Run every create-time gateway check (G1-G9, G11-G13) on one request.

    G10 is a completion-time check (:func:`check_g10`) and never runs here.
    All checks run; the result aggregates every violation so callers see the
    full picture instead of only the first failure.
    """

    violations: list[GatewayViolation] = []
    for check in (
        check_g1,
        check_g2,
        check_g3,
        check_g4,
        check_g5,
        check_g6,
        check_g7,
        check_g8,
        check_g9,
        check_g11,
        check_g12,
        check_g13,
    ):
        violations.extend(check(request))
    return GatewayResult(tuple(violations))


def check_g1(request: GatewayRequest) -> list[GatewayViolation]:
    """G1: body carries Done criteria + terminal instruction.

    ``body=None`` on a goal-mode card is rejected outright (the career-next
    trio crash-looped seven times without one); any other missing marker is a
    warning so non-goal cards keep flowing while the bias is applied.
    """

    violations: list[GatewayViolation] = []
    if request.body is None:
        if request.goal_mode:
            violations.append(
                GatewayViolation(
                    "g1_goal_body_required",
                    "goal-mode card must carry a body with Done criteria and "
                    "a terminal instruction",
                    REJECT,
                )
            )
        else:
            violations.append(
                GatewayViolation(
                    "g1_body_missing",
                    "card body is None; Done criteria and terminal "
                    "instruction cannot be verified",
                    WARN,
                )
            )
        return violations
    if not _G1_DONE_CRITERIA.search(request.body):
        violations.append(
            GatewayViolation(
                "g1_done_criteria_missing",
                "body does not state Done criteria (expected a marker such "
                "as 'Done criteria', 'Acceptance criteria', 'done when')",
                WARN,
            )
        )
    if not _G1_TERMINAL.search(request.body):
        violations.append(
            GatewayViolation(
                "g1_terminal_instruction_missing",
                "body has no terminal instruction (kanban_complete / "
                "kanban_block)",
                WARN,
            )
        )
    return violations


def check_g2(request: GatewayRequest) -> list[GatewayViolation]:
    """G2: idempotency key required on every create."""

    if request.idempotency_key is None or not request.idempotency_key.strip():
        return [
            GatewayViolation(
                "g2_idempotency_key_required",
                "create carries no idempotency key; native dedup is bypassed",
                REJECT,
            )
        ]
    return []


def check_g3(request: GatewayRequest) -> list[GatewayViolation]:
    """G3: never ``--branch`` with scratch; sim/prototype must be abs worktree."""

    violations: list[GatewayViolation] = []
    if request.branch is not None and request.workspace_kind == "scratch":
        violations.append(
            GatewayViolation(
                "g3_branch_with_scratch",
                "--branch is never valid with a scratch workspace",
                REJECT,
            )
        )
    if request.card_kind in SimKinds:
        if request.workspace_kind != "worktree":
            violations.append(
                GatewayViolation(
                    "g3_sim_requires_worktree",
                    f"sim/prototype card workspace_kind must be 'worktree', "
                    f"got {request.workspace_kind!r} (never scratch or the "
                    "canonical checkout)",
                    REJECT,
                )
            )
        elif request.workspace_path is None or not _is_absolute(
            request.workspace_path
        ):
            violations.append(
                GatewayViolation(
                    "g3_sim_worktree_path_not_absolute",
                    "sim/prototype worktree path must be absolute, never "
                    "relative or missing",
                    REJECT,
                )
            )
    return violations


def check_g4(request: GatewayRequest) -> list[GatewayViolation]:
    """G4: no ``--project``/``--workspace`` on plain review cards."""

    if request.card_kind == "review" and (
        request.project is not None or request.workspace is not None
    ):
        return [
            GatewayViolation(
                "g4_review_card_project_workspace",
                "plain review cards must not carry --project/--workspace",
                REJECT,
            )
        ]
    return []


def check_g5(request: GatewayRequest) -> list[GatewayViolation]:
    """G5: never per-card model/provider override flags (dispatch crashes)."""

    if request.model is not None or request.provider is not None:
        return [
            GatewayViolation(
                "g5_model_provider_override",
                "per-card model/provider overrides crash dispatch; pin "
                "models at profile level instead",
                REJECT,
            )
        ]
    return []


def check_g6(request: GatewayRequest) -> list[GatewayViolation]:
    """G6: AGENTS.md title/branch-format check before create.

    The canonical slug is derived from the title (lowercase, non-alphanumeric
    runs collapsed to one hyphen) and probed with
    ``git check-ref-format --branch``; the probe is injectable via
    ``GatewayRequest.ref_format_check`` for deterministic tests.
    """

    slug = _title_slug(request.title)
    if not slug:
        return [
            GatewayViolation(
                "g6_title_unsluggable",
                f"title {request.title!r} yields an empty branch slug",
                REJECT,
            )
        ]
    ref_format_ok = request.ref_format_check or _git_ref_format_ok
    if not ref_format_ok(slug):
        return [
            GatewayViolation(
                "g6_branch_ref_format",
                f"branch slug {slug!r} fails git check-ref-format",
                REJECT,
            )
        ]
    return []


def check_g7(request: GatewayRequest) -> list[GatewayViolation]:
    """G7: no link cycles; parents must exist on their (cross-)board.

    Existence is delegated to ``GatewayRequest.parent_exists(board, task_id)``;
    when parents are declared but the probe is missing the check fails closed.
    A card naming itself as its own parent is rejected unconditionally.
    """

    violations: list[GatewayViolation] = []
    if request.task_id is not None:
        for parent_id, _ in request.parents:
            if parent_id == request.task_id:
                violations.append(
                    GatewayViolation(
                        "g7_self_parent_cycle",
                        f"card {request.task_id} lists itself as parent",
                        REJECT,
                    )
                )
    if not request.parents:
        return violations
    if request.parent_exists is None:
        violations.append(
            GatewayViolation(
                "g7_parent_existence_unverifiable",
                "parents declared but no parent_exists probe supplied; "
                "failing closed",
                REJECT,
            )
        )
        return violations
    for parent_id, parent_board in request.parents:
        board = parent_board or request.board_slug
        if not request.parent_exists(board, parent_id):
            violations.append(
                GatewayViolation(
                    "g7_unknown_parent",
                    f"parent {parent_id} does not exist on board {board!r}",
                    REJECT,
                )
            )
    return violations


def check_g8(request: GatewayRequest) -> list[GatewayViolation]:
    """G8: impl body must not gate completion on its parent reviewer.

    One regex over the body text; the pattern catches both orders ("complete
    ... after your parent reviewer ..." and "parent reviewer ... before you
    complete ..."), including inflections (completion, completing) via the
    ``complet`` stem, the until/must/before connectors, and
    ``kanban_complete`` spellings.
    """

    if request.card_kind != "impl" or request.body is None:
        return []
    if _G8_DEADLOCK.search(request.body):
        return [
            GatewayViolation(
                "g8_goal_judge_deadlock",
                "impl body states completion requires its parent reviewer; "
                "that deadlocks the goal judge",
                REJECT,
            )
        ]
    return []


def check_g9(request: GatewayRequest) -> list[GatewayViolation]:
    """G9: review body carries the leave-blocked line + seeded-corpus probes."""

    if request.card_kind != "review":
        return []
    violations: list[GatewayViolation] = []
    if request.body is None or not _G9_LEAVE_BLOCKED.search(request.body):
        violations.append(
            GatewayViolation(
                "g9_leave_blocked_line_missing",
                "review body must instruct the reviewer to 'leave card N "
                "blocked' with the verified outcome",
                REJECT,
            )
        )
    if request.body is None or not _G9_SEEDED_CORPUS.search(request.body):
        violations.append(
            GatewayViolation(
                "g9_seeded_corpus_missing",
                "review body must state seeded-corpus probe requirements "
                "(analysis alone is not acceptance)",
                REJECT,
            )
        )
    return violations


def check_g10(card_status: str, run_count: int) -> GatewayViolation | None:
    """G10 (complete-time): never ``complete`` a never-started todo.

    A todo card with zero runs was never started; archive is the correct
    terminal action. Any started state (or any run history) is allowed.
    """

    if card_status == "todo" and run_count < 1:
        return GatewayViolation(
            "g10_complete_never_started",
            "cannot complete a todo card with zero runs; use archive",
            REJECT,
        )
    return None


def check_g11(request: GatewayRequest) -> list[GatewayViolation]:
    """G11 (document-only): positional title first, flags after.

    The native parser already hard-rejects the bad order, so this is a
    warning for humans reading gateway output; it applies only when a raw
    create ``command`` is supplied.
    """

    command = request.command
    if command is None or "create" not in command:
        return []
    index = list(command).index("create") + 1
    if index < len(command) and str(command[index]).startswith("--"):
        return [
            GatewayViolation(
                "g11_flags_before_title",
                "positional title must come before flags; the native parser "
                "swallows flags-first forms",
                WARN,
            )
        ]
    return []


def check_g12(request: GatewayRequest) -> list[GatewayViolation]:
    """G12: max-runtime must be None or a positive int."""

    value = request.max_runtime_seconds
    if value is None:
        return []
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return [
            GatewayViolation(
                "g12_max_runtime_invalid",
                f"max_runtime_seconds must be None or a positive int, got "
                f"{value!r}",
                REJECT,
            )
        ]
    return []


def check_g13(request: GatewayRequest) -> list[GatewayViolation]:
    """G13: prompt templates must render BOARD SLUG, not bare task_id.

    The ``{task_id}``-only bug class (t_f87e17df) sent workers hunting
    whatever board happened to be current; a template that never mentions
    ``{board_slug}`` is rejected, and templates that fail to render at all
    are rejected with the render error in the message.
    """

    template = request.prompt_template
    if template is None:
        return []
    if "{board_slug}" not in template:
        return [
            GatewayViolation(
                "g13_template_missing_board_slug",
                "prompt template must render {board_slug} (a bare {task_id} "
                "lookup hits the wrong board)",
                REJECT,
            )
        ]
    try:
        template.format(task_id="t_probe", board_slug="probe-board")
    except (KeyError, ValueError, IndexError) as exc:
        return [
            GatewayViolation(
                "g13_template_unrenderable",
                f"prompt template does not render: {exc}",
                REJECT,
            )
        ]
    return []


def _title_slug(title: str) -> str:
    """Derive the canonical branch slug from a card title."""

    return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")


def _is_absolute(path: str) -> bool:
    from pathlib import Path

    return Path(path).is_absolute()


def _git_ref_format_ok(slug: str) -> bool:
    """Probe a branch slug with ``git check-ref-format --branch``."""

    try:
        completed = subprocess.run(
            ["git", "check-ref-format", "--branch", slug],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def gateway_request_for_admission(
    request: GatewayRequest, idempotency_key: str
) -> GatewayRequest:
    """Return the request with the admission-generated idempotency key set.

    ``admit_child`` always generates a deterministic key, so G2 is satisfied
    by construction on that path; this helper keeps the mirror honest without
    asking callers to pre-compute the key.
    """

    return replace(request, idempotency_key=idempotency_key)


__all__ = [
    "REJECT",
    "WARN",
    "GatewayError",
    "GatewayRequest",
    "GatewayResult",
    "GatewayViolation",
    "check_g1",
    "check_g10",
    "check_g11",
    "check_g12",
    "check_g13",
    "check_g2",
    "check_g3",
    "check_g4",
    "check_g5",
    "check_g6",
    "check_g7",
    "check_g8",
    "check_g9",
    "gateway_request_for_admission",
    "run_gateway_checks",
]
