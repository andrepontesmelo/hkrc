"""Self-health alerting for the continuous stream runtime.

If the controller's own stream transport breaks (auth token rotated, dashboard
down, endpoint unreachable), the daemon records per-board transport failures
and would otherwise stay silent.  This module renders the human-facing alert
and recovery messages that surface an outage episode to the operator's
Telegram destination.

Alerting is additive and fail-closed: a failed alert send never suppresses a
recovery.  Thresholding and episode dedupe live in the observer/state layers;
this module only formats text.
"""

from __future__ import annotations

import re

# Board slugs that reach operator-facing alert text must never be able to
# forge extra lines.  The safe grammar mirrors config validation; anything
# outside it is replaced at render time so a hostile slug (for example a
# discovery-sourced directory name, which bypasses config validation) cannot
# inject a newline or other markup into the alert.
_BOARD_SLUG_SAFE = re.compile(r"[^A-Za-z0-9._-]")


def _safe_board_slug(board_slug: str) -> str:
    """Return ``board_slug`` rendered with every unsafe character replaced.

    Characters outside ``[A-Za-z0-9._-]`` become ``?`` so the operator sees a
    clearly sanitized slug and no hidden line breaks survive.
    """

    return _BOARD_SLUG_SAFE.sub("?", board_slug)


def format_stream_alert(
    board_slug: str,
    *,
    failure_count: int,
    error_code: str,
    first_failure_at: str,
    last_failure_at: str,
) -> str:
    """Render one outage alert; the board has been blind for N failures.

    ``error_code`` is the stable stream error category (``auth_failed``,
    ``transport``, ``disconnected``, ...); the two timestamps anchor the
    outage episode.
    """

    return (
        f"HKRC stream alert: board {_safe_board_slug(board_slug)} is blind\n"
        f"- consecutive stream failures: {failure_count}\n"
        f"- error code: {error_code}\n"
        f"- first failure: {first_failure_at}\n"
        f"- last failure: {last_failure_at}"
    )


def format_stream_recovery(
    board_slug: str,
    *,
    failure_count: int,
    first_failure_at: str,
    last_failure_at: str | None,
) -> str:
    """Render the recovery notice sent when a board's stream resumes."""

    window = first_failure_at
    if last_failure_at is not None and last_failure_at != first_failure_at:
        window = f"{first_failure_at} to {last_failure_at}"
    return (
        f"HKRC stream recovered: board {_safe_board_slug(board_slug)}\n"
        f"- stream resumed after {failure_count} consecutive failures\n"
        f"- outage window: {window}"
    )


__all__ = ["format_stream_alert", "format_stream_recovery"]
