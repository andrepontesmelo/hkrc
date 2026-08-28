# HKRC Assist offline replay fixture

This directory is a committed, deterministic fixture for the HKRC Assist Phase-1 demo.

## Safety and provenance

- Fixture namespace: `synthetic`.
- The sequence is a sanitized transcription of a validated internal `deferred_review_no_merge` incident. Internal provenance is intentionally not included here.
- It preserves only the causal sequence: implementation complete -> review rejected/deferred -> canonical gate blocked -> no merge in the observation window.
- Values are symbolic (`window-001`, `event-001`, `evidence-001`) and timestamps use a fixed synthetic UTC offset. The fixture contains no live board, task, profile, branch, commit, chat, host, username, path, credential, or secret-shaped value.
- The replay is offline and recommendation-only. It does not invoke the Hermes CLI, open native Hermes state, or mutate a board/task.

## One-command replay

From a clean checkout:

```bash
uv run python tests/fixtures/assist_replay/replay.py --json
```

The output is one canonical JSON object labeled `SYNTHETIC_OFFLINE_FIXTURE`. It includes the fixture hash, two windows, evidence references, classifier recurrence transition, malformed and unavailable model fail-closed results, pending candidate, HTML report, zero-mutation proof, append-only defer ledger, and dedupe signature.

To write the candidate card and self-contained HTML report to an operator-selected local directory:

```bash
uv run python tests/fixtures/assist_replay/replay.py --demo --output-dir /tmp/hkrc-assist-demo
```

The output directory is operator-selected and is not part of the committed fixture or its data.

## Acceptance properties

- Window 1 classifies as `first_seen`; window 2 transitions to `recurs_in_2_windows`.
- Malformed and unavailable model outputs preserve evidence and return `ai_status=error` with `recommendation=not_actionable`.
- The candidate remains `state=pending`, `action=not_applied`, `intent=prevention_only`, and `human_in_loop=yes`.
- The proof records zero native CLI calls and zero unblock, reassign, comment, create, merge, deploy, or systemd operations.
- The operator defer is represented as an append-only ledger event; replay duplicates are ignored by symbolic event ID.
- Replaying the fixture twice produces identical hash, signatures, evidence references, and ordering.

Run focused checks:

```bash
uv run pytest tests/test_assist_replay.py -q
```
