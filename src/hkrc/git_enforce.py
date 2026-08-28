"""Portable Git ``reference-transaction`` adapter for the outcome guard.

The ``reference-transaction`` hook (Git 2.36+) is invoked once per reference
transaction with the state as its single argument (``prepared``,
``committed``, or ``aborted``) and one line per reference update on stdin:
``<old-oid> SP <new-oid> SP <ref-name> LF``. Only the ``prepared`` state
consults the exit status: a non-zero exit aborts the whole transaction.

This module implements that contract without ever parsing commit messages as
authority. A protected canonical ref (``refs/heads/main`` by default) is
denied unless an operator has recorded a merge authorization in
controller-owned state that binds a task id, a contract ref, and the typed
terminal/review evidence required by that contract. Enforcement is only
active after an explicit, idempotent hook install; the installer chains an
existing hook instead of overwriting it and refuses to act when
``core.hooksPath`` redirects git away from ``.git/hooks``.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from .config import ControllerConfig, ConfigError, load_config
from .outcome_guard import OutcomeGuard
from .state import ControllerState, StateError


PROTECTED_REF_DEFAULT = "refs/heads/main"
HOOK_MARKER = "# hkrc-managed outcome-guard reference-transaction hook"
ORIG_SUFFIX = ".hkrc-orig"
_OID = re.compile(r"^[0-9a-fA-F]{40}(?:[0-9a-fA-F]{24})?$")


class GitEnforceError(RuntimeError):
    """Raised when the hook cannot be installed/evaluated safely."""


@dataclass(frozen=True, slots=True)
class RefUpdate:
    old: str
    new: str
    ref: str


@dataclass(frozen=True, slots=True)
class HookDecision:
    """Machine-readable decision for one reference transaction."""

    allowed: bool
    reason_code: str
    denied_refs: tuple[str, ...] = ()
    missing_evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "hkrc.git-hook-result.v1",
            "allowed": self.allowed,
            "reason_code": self.reason_code,
            "denied_refs": list(self.denied_refs),
            "missing_evidence_refs": list(self.missing_evidence_refs),
        }


def parse_reference_transaction(lines: list[str]) -> tuple[RefUpdate, ...]:
    """Parse stdin tuples; any malformed line fails closed for the transaction.

    Real Git 2.43 transactions may also carry pseudo-ref lines (``HEAD``,
    ``ORIG_HEAD``, ``FETCH_HEAD``, ...) whose names never start with ``refs/``
    and therefore can never match a configured protected ref; they are carried
    through and simply never deny. Object-id format and field count are still
    validated strictly so garbage input fails closed.
    """

    updates: list[RefUpdate] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        fields = line.split()
        if len(fields) != 3:
            raise GitEnforceError(f"expected '<old> <new> <ref>' tuple, got: {line!r}")
        old, new, ref = fields
        if not _OID.fullmatch(old) or not _OID.fullmatch(new):
            raise GitEnforceError(f"non-object-id in reference tuple: {line!r}")
        updates.append(RefUpdate(old=old, new=new, ref=ref))
    return tuple(updates)


def evaluate_transaction(
    state: ControllerState, config: ControllerConfig, updates: tuple[RefUpdate, ...]
) -> HookDecision:
    """Decide a prepared transaction against policy state.

    Non-protected refs are never consulted. A protected ref passes only when
    the most recent merge authorization bound to that ref references a
    contract that allows ``merge_main`` and whose terminal evidence
    requirements are satisfied by the evidence snapshot bound at
    authorization time.
    """

    protected = frozenset(config.outcome_guard.protected_refs)
    guarded = tuple(update for update in updates if update.ref in protected)
    if not guarded:
        return HookDecision(True, "no_protected_refs")

    guard = OutcomeGuard(state)
    for update in guarded:
        denial = _first_denial(guard, state, update.ref)
        if denial is not None:
            reason, missing = denial
            return HookDecision(
                False, reason, tuple(u.ref for u in guarded), tuple(missing)
            )
    return HookDecision(True, "authorized")


def _first_denial(
    guard: OutcomeGuard, state: ControllerState, ref: str
) -> tuple[str, tuple[str, ...]] | None:
    row = state.connection.execute(
        "SELECT contract_ref, evidence_json FROM outcome_merge_authorizations "
        "WHERE ref = ? ORDER BY authorized_at DESC, task_id ASC LIMIT 1",
        (ref,),
    ).fetchone()
    if row is None:
        return ("no_merge_authorization", ())
    contract_ref = str(row["contract_ref"])
    if guard.get_contract(contract_ref) is None:
        return ("authorization_contract_missing", ())
    effect = guard.check_effect(contract_ref, "merge_main")
    if not effect.allowed:
        return ("merge_main_not_allowed", ())
    try:
        evidence = json.loads(str(row["evidence_json"]))
    except json.JSONDecodeError:
        return ("authorization_evidence_malformed", ())
    if not isinstance(evidence, list):
        return ("authorization_evidence_malformed", ())
    outcome = guard.check_outcome(contract_ref, evidence=evidence)
    if not outcome.outcome_reached:
        return ("merge_evidence_missing", outcome.missing_evidence_refs)
    return None


def run_hook_command(
    config_path: Path,
    *,
    hook_state: str,
    audit_only: bool = False,
    stdin_lines: list[str] | None = None,
) -> int:
    """Execute the git-hook adapter; return the process exit code.

    ``stdin_lines`` exists only for deterministic tests; production reads
    ``sys.stdin``. Fail closed: unavailable config/state or malformed input
    denies a non-empty transaction. ``audit_only`` reports the decision on
    stdout without ever denying.
    """

    if hook_state != "prepared":
        return 0
    try:
        updates = parse_reference_transaction(stdin_lines if stdin_lines is not None else _read_stdin())
    except GitEnforceError as exc:
        print(f"hkrc outcome-guard: denied malformed_reference_transaction: {exc}", file=sys.stderr)
        return 1
    if not updates:
        return 0
    try:
        config = load_config(config_path)
        with ControllerState.open_read_only(config.state_db) as state:
            if state.instance_name != config.instance_name:
                raise StateError(
                    f"state instance {state.instance_name!r} does not match config "
                    f"{config.instance_name!r}"
                )
            decision = evaluate_transaction(state, config, updates)
    except (ConfigError, StateError) as exc:
        # Without config/state we cannot identify or authorize protected refs,
        # so the only sound behavior is denying the whole transaction.
        print(
            f"hkrc outcome-guard: denied enforcement_unavailable: {exc}",
            file=sys.stderr,
        )
        return 1
    if decision.allowed:
        if audit_only:
            print(json.dumps(decision.to_dict(), sort_keys=True, separators=(",", ":")))
        return 0
    for ref in decision.denied_refs:
        print(
            f"hkrc outcome-guard: denied {decision.reason_code} ref={ref} "
            f"missing_evidence={','.join(decision.missing_evidence_refs)}",
            file=sys.stderr,
        )
    if audit_only:
        print(json.dumps(decision.to_dict(), sort_keys=True, separators=(",", ":")))
        return 0
    return 1


def install_hook(
    repo: Path,
    *,
    wrapper: Path,
    config_path: Path,
    config: ControllerConfig | None = None,
) -> tuple[str, ...]:
    """Install the managed hook into one repository (idempotent, chaining).

    Refuses when ``core.hooksPath`` is set (git would not run this hook), and
    when an unmanaged hook and a saved hkrc original both exist. An existing
    unmanaged hook is preserved by chaining: it is renamed to
    ``reference-transaction.hkrc-orig`` and executed before the managed hook
    on every transaction.
    """

    git_dir = _resolve_git_dir(repo)
    hooks_dir = git_dir / "hooks"
    redirected = _hooks_path_redirect(repo)
    if redirected is not None:
        raise GitEnforceError(
            f"core.hooksPath is set to {redirected!r}; git will not run hooks "
            f"from {hooks_dir}. Unset it (git config --unset core.hooksPath) or "
            "install the hook into the configured hooks directory."
        )
    wrapper = Path(wrapper).expanduser().resolve()
    if not wrapper.is_file() or not os.access(wrapper, os.X_OK):
        raise GitEnforceError(f"hkrc wrapper is not an executable file: {wrapper}")
    config_path = Path(config_path).expanduser().resolve()
    effective_config = config or load_config(config_path)

    hook_path = hooks_dir / "reference-transaction"
    orig_path = hooks_dir / f"reference-transaction{ORIG_SUFFIX}"
    messages: list[str] = []
    if hook_path.is_file():
        if _is_managed(hook_path.read_text(encoding="utf-8", errors="replace")):
            messages.append(f"replaced managed hook: {hook_path}")
        else:
            if orig_path.exists():
                raise GitEnforceError(
                    f"conflict in {hooks_dir}: {hook_path.name} is not hkrc-managed "
                    f"but {orig_path.name} already exists. Move the unmanaged hook "
                    "aside or uninstall the hkrc hook first."
                )
            orig_path.write_text(hook_path.read_text(encoding="utf-8"), encoding="utf-8")
            orig_path.chmod(hook_path.stat().st_mode | 0o111)
            messages.append(f"chained existing hook to: {orig_path}")
    hooks_dir.mkdir(parents=True, exist_ok=True)
    temporary = hook_path.with_name(f".{hook_path.name}.tmp")
    temporary.write_text(
        _hook_script(wrapper, config_path), encoding="utf-8"
    )
    temporary.chmod(0o755)
    temporary.replace(hook_path)
    messages.append(f"installed managed hook: {hook_path}")
    messages.append(
        f"protected_refs={','.join(effective_config.outcome_guard.protected_refs)} "
        f"config={config_path}"
    )
    return tuple(messages)


def uninstall_hook(repo: Path) -> tuple[str, ...]:
    """Remove the managed hook and restore any chained original (idempotent)."""

    git_dir = _resolve_git_dir(repo)
    hooks_dir = git_dir / "hooks"
    hook_path = hooks_dir / "reference-transaction"
    orig_path = hooks_dir / f"reference-transaction{ORIG_SUFFIX}"
    managed = hook_path.is_file() and _is_managed(
        hook_path.read_text(encoding="utf-8", errors="replace")
    )
    messages: list[str] = []
    if managed:
        hook_path.unlink()
        messages.append(f"removed managed hook: {hook_path}")
    if orig_path.is_file():
        orig_path.replace(hook_path)
        messages.append(f"restored original hook: {hook_path}")
    if not managed and not orig_path.is_file():
        if hook_path.is_file():
            messages.append(
                "reference-transaction hook exists but is not hkrc-managed; left untouched"
            )
        else:
            messages.append("no hkrc-managed hook installed; nothing to do")
    return tuple(messages)


def hook_status(repo: Path) -> tuple[str, ...]:
    """Report installation state without mutating anything."""

    git_dir = _resolve_git_dir(repo)
    hooks_dir = git_dir / "hooks"
    hook_path = hooks_dir / "reference-transaction"
    orig_path = hooks_dir / f"reference-transaction{ORIG_SUFFIX}"
    redirected = _hooks_path_redirect(repo)
    lines: list[str] = [f"repo={repo} git_dir={git_dir}"]
    if redirected is not None:
        lines.append(f"warning: core.hooksPath={redirected} (hooks here are ignored)")
    if hook_path.is_file():
        managed = _is_managed(hook_path.read_text(encoding="utf-8", errors="replace"))
        lines.append(f"hook={hook_path} managed={'yes' if managed else 'no'}")
    else:
        lines.append("hook=not installed")
    if orig_path.is_file():
        lines.append(f"chained_original={orig_path}")
    return tuple(lines)


def _is_managed(content: str) -> bool:
    return HOOK_MARKER in content


def _hook_script(wrapper: Path, config_path: Path) -> str:
    return (
        "#!/bin/sh\n"
        f"{HOOK_MARKER}\n"
        "# Installed by: hkrc outcome-guard git-hook install\n"
        "# Uninstall with: hkrc outcome-guard git-hook uninstall --repo <repo>\n"
        "set -u\n"
        'HOOK_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\n'
        f'if [ -x "$HOOK_DIR/reference-transaction{ORIG_SUFFIX}" ]; then\n'
        f'    "$HOOK_DIR/reference-transaction{ORIG_SUFFIX}" "$@" || exit $?\n'
        "fi\n"
        'HOOK_STATE=${1:-}\n'
        "shift 2>/dev/null || true\n"
        f"exec {_shell_quote(str(wrapper))} outcome-guard git-hook "
        f"--state \"$HOOK_STATE\" --config {_shell_quote(str(config_path))} \"$@\"\n"
    )


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def _resolve_git_dir(repo: Path) -> Path:
    repo = Path(repo).expanduser().resolve()
    completed = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--git-dir"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "not a git repository").strip()
        raise GitEnforceError(f"cannot resolve git dir for {repo}: {detail}")
    git_dir = Path(completed.stdout.strip())
    return git_dir if git_dir.is_absolute() else repo / git_dir


def _hooks_path_redirect(repo: Path) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(repo), "config", "--get", "core.hooksPath"],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def _read_stdin() -> list[str]:
    return sys.stdin.read().splitlines()


__all__ = [
    "GitEnforceError",
    "HOOK_MARKER",
    "HookDecision",
    "ORIG_SUFFIX",
    "PROTECTED_REF_DEFAULT",
    "RefUpdate",
    "evaluate_transaction",
    "hook_status",
    "install_hook",
    "parse_reference_transaction",
    "run_hook_command",
    "uninstall_hook",
]
