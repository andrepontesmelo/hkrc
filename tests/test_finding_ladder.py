"""Operator escalation ladder + next-action dedupe (t_c9da2f07).

Covers the four approved behaviours end to end from the render-time inputs:

1. age rungs (nightly line < digest_after_nights <= digest/rollup),
2. needs_input findings first inside the digest,
3. deterministic disposition proposals for the weekly rollup,
4. identical next actions annotated (and the ledger streak persisted),
5. the operator ``disposition`` command in both directions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hkrc.harness_loop import (
    HarnessLoopError,
    HarnessReport,
    LadderContext,
    _action_fingerprint,
    _entry_to_finding,
    _escalation_map,
    _next_action_dedupe_map,
    _proposed_disposition,
    apply_disposition,
    fingerprint,
    load_state,
    render_report,
    run,
    save_state,
)
from test_harness_loop import (  # noqa: F401  (helper module, same test dir)
    make_config,
    make_hkrc_repo,
    make_ticket_runner,
    queue_entry,
)

DAY = 86400
# 2026-09-13 03:00 UTC is a Sunday (the default digest weekday); +1 day is a
# Monday, so the same ages render two different cadences.
SUNDAY = 1789268400
MONDAY = SUNDAY + DAY
ACTION = "Nothing to do; the next audit runs on the shipped cron schedule."
DIGEST_HEADER = "Digest — open >=3 nights"
ROLLUP_HEADER = "Weekly rollup — open >=14 nights"


def ladder_entry(
    now: int, age: int, *, pattern: str, key: str = "", severity: str = "medium", **overrides
) -> dict:
    """One working-set queue entry aged ``age`` nights relative to ``now``."""
    entry = queue_entry(
        f"{pattern}:{key}",
        pattern=pattern,
        key=key,
        severity=severity,
        first_seen=now - age * DAY,
        last_seen=now,
        **overrides,
    )
    # render_report looks entries up by their canonical fingerprint, exactly
    # as the ledger stores them.
    entry["fingerprint"] = fingerprint(_entry_to_finding(entry))
    return entry


def ladder_report(
    entries: list[dict],
    *,
    now: int,
    escalation: dict | None = None,
    repeat: int = 1,
) -> HarnessReport:
    return HarnessReport(
        story="story",
        wrong=tuple(_entry_to_finding(entry) for entry in entries),
        skipped=(),
        applied=(),
        deploy_ready="none",
        right=(),
        next_action=ACTION,
        escalation=escalation or {},
        ladder=LadderContext(
            now=now,
            entries={entry["fingerprint"]: entry for entry in entries},
            next_action_repeat=repeat,
        ),
    )


def state_with(entries: list[dict], **extra) -> dict:
    return {
        "created": "2026-09-01",
        "last_run": None,
        "resolved_topics": [],
        "suggested_fingerprints": [],
        "open_findings": entries,
        **extra,
    }


def test_age_rungs_nightly_line_then_summary_then_digest() -> None:
    night = render_report(ladder_report([ladder_entry(SUNDAY, 2, pattern="p2")], now=SUNDAY))
    assert "MEDIUM — P2 (open 2 nights)" in night
    assert DIGEST_HEADER not in night
    assert "full digest" not in night

    aged_monday = [
        ladder_entry(MONDAY, 3, pattern="p3", key="k"),
        ladder_entry(MONDAY, 9, pattern="p9"),
    ]
    collapsed = render_report(ladder_report(aged_monday, now=MONDAY))
    # non-digest night: ONE summary line, no per-finding aged line
    assert "2 findings open >=3 nights — full digest Sunday" in collapsed
    assert "(open 3 nights)" not in collapsed
    assert "(open 9 nights)" not in collapsed
    assert DIGEST_HEADER not in collapsed

    aged_sunday = [
        ladder_entry(SUNDAY, 3, pattern="p3", key="k"),
        ladder_entry(SUNDAY, 9, pattern="p9"),
    ]
    digest = render_report(ladder_report(aged_sunday, now=SUNDAY))
    assert digest.count(DIGEST_HEADER) == 1
    assert "MEDIUM — P3 [k] (open 3 nights)" in digest
    assert "MEDIUM — P9 (open 9 nights)" in digest
    assert "findings open >=3 nights" not in digest

    singular = render_report(ladder_report([ladder_entry(MONDAY, 4, pattern="p4")], now=MONDAY))
    assert "1 finding open >=3 nights — full digest Sunday" in singular


def test_needs_input_findings_render_first_in_the_digest() -> None:
    entries = [
        ladder_entry(SUNDAY, 5, pattern="p5", key="machine"),
        ladder_entry(SUNDAY, 5, pattern="p5", key="hkrc:needs_input"),
    ]
    digest = render_report(ladder_report(entries, now=SUNDAY)).split(DIGEST_HEADER, 1)[1]
    assert digest.index("[hkrc:needs_input]") < digest.index("[machine]")


def test_proposed_disposition_is_deterministic_and_prioritised() -> None:
    assert (
        _proposed_disposition(
            {"apply_kind": "hkrc", "fix_status": "deferred", "occurrence_count": 30}
        )
        == "promote-to-ticket"
    )
    assert (
        _proposed_disposition(
            {"apply_kind": "none", "fix_status": "deferred", "occurrence_count": 30}
        )
        == "resolve"
    )
    assert (
        _proposed_disposition(
            {"apply_kind": "none", "fix_status": "open", "occurrence_count": 21}
        )
        == "mark-stale"
    )
    assert (
        _proposed_disposition(
            {"apply_kind": "none", "fix_status": "open", "occurrence_count": 20}
        )
        == "defer"
    )


def test_rollup_lists_aged_entries_with_their_operator_command() -> None:
    entries = [
        ladder_entry(SUNDAY, 14, pattern="p14", key="a"),
        ladder_entry(SUNDAY, 20, pattern="p20", key="b", apply_kind="hkrc"),
        ladder_entry(SUNDAY, 3, pattern="p3", key="c"),
    ]
    text = render_report(ladder_report(entries, now=SUNDAY))
    rollup = text.split(ROLLUP_HEADER, 1)[1]
    assert "MEDIUM — P14 (open 14 nights) — proposed: defer — " in rollup
    assert "hkrc harness-loop disposition p14:a defer" in rollup
    assert "proposed: promote-to-ticket" in rollup
    assert "hkrc harness-loop disposition p20:b promote" in rollup
    assert "p3:c" not in rollup
    # report-only: the rollup never claims to have executed anything
    assert "never auto-run" in text


def test_next_action_dedupe_annotates_only_repeats() -> None:
    entries = [ladder_entry(SUNDAY, 5, pattern="p5")]
    assert "(same as last night" not in render_report(ladder_report(entries, now=SUNDAY))
    assert "(same as last night, x2)" in render_report(
        ladder_report(entries, now=SUNDAY, repeat=2)
    )
    assert "(same as last night, x3)" in render_report(
        ladder_report(entries, now=SUNDAY, repeat=3)
    )
    # a long identical streak is named in the digest section too
    assert "Next action unchanged 3 nights (same as last night, x3)." in render_report(
        ladder_report(entries, now=SUNDAY, repeat=3)
    )


def test_next_action_dedupe_map_bumps_repeats_and_resets_on_change() -> None:
    fp = _action_fingerprint(ACTION)
    fresh, count = _next_action_dedupe_map({}, ACTION)
    assert (fresh, count) == ({fp: {"hash": fp, "count": 1}}, 1)

    prior = {"next_action_dedupe": {fp: {"hash": fp, "count": 2}}}
    bumped, count = _next_action_dedupe_map(prior, ACTION)
    assert (bumped, count) == ({fp: {"hash": fp, "count": 3}}, 3)

    # whitespace-only differences are the SAME action
    _spaced, count = _next_action_dedupe_map(prior, f"  {ACTION}\n")
    assert count == 3

    # a changed action text starts a fresh streak and prunes the stale entry
    changed, count = _next_action_dedupe_map(prior, "Fix the flaky test first.")
    assert count == 1
    assert list(changed) == [_action_fingerprint("Fix the flaky test first.")]


def test_identical_next_action_persists_live_and_never_on_dry_run(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_path = tmp_path / "harness-loop-state.json"
    save_state(state_path, state_with([]))
    before = state_path.read_bytes()
    fp = _action_fingerprint(ACTION)

    preview = run(config, now=MONDAY, dry_run=True, state_path=state_path)
    assert state_path.read_bytes() == before
    assert load_state(state_path).get("next_action_dedupe") is None
    assert "(same as last night" not in preview

    run(config, now=MONDAY, dry_run=False, state_path=state_path)
    assert load_state(state_path)["next_action_dedupe"] == {fp: {"hash": fp, "count": 1}}

    second = run(config, now=MONDAY, dry_run=False, state_path=state_path)
    assert load_state(state_path)["next_action_dedupe"] == {fp: {"hash": fp, "count": 2}}
    assert "(same as last night, x2)" in second


def test_apply_disposition_flips_status_for_one_unique_prefix(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    state_path = tmp_path / "state.json"
    entry = ladder_entry(SUNDAY, 4, pattern="p4", key="only")
    other = ladder_entry(SUNDAY, 1, pattern="p1", key="x")
    save_state(state_path, state_with([entry, other]))

    message = apply_disposition("p4:", "defer", config, state_path=state_path, now=SUNDAY)
    assert message == f"{entry['fingerprint']} -> defer (fix_status=deferred)"
    stored = {item["fingerprint"]: item for item in load_state(state_path)["open_findings"]}
    assert stored[entry["fingerprint"]]["fix_status"] == "deferred"
    assert stored[other["fingerprint"]]["fix_status"] == "open"

    with pytest.raises(HarnessLoopError, match="no working-set finding matches"):
        apply_disposition("nope", "defer", config, state_path=state_path, now=SUNDAY)
    with pytest.raises(HarnessLoopError, match="ambiguous prefix"):
        apply_disposition("p", "defer", config, state_path=state_path, now=SUNDAY)
    with pytest.raises(HarnessLoopError, match="unknown disposition"):
        apply_disposition("p4:", "squash", config, state_path=state_path, now=SUNDAY)


def test_apply_disposition_promote_uses_the_existing_pair_budget(tmp_path: Path) -> None:
    repo = make_hkrc_repo(tmp_path)
    config = make_config(tmp_path, hkrc_repo=repo)
    state_path = tmp_path / "state.json"
    target = repo / "src" / "hkrc" / "thing.py"
    entry = ladder_entry(
        SUNDAY,
        9,
        pattern="p9",
        key="a",
        apply_kind="hkrc",
        before="OLD_WORD",
        after="NEW_WORD",
        target_path=str(target),
        verify_path=str(target),
        verify_text="OLD_WORD",
    )
    runner, calls, _flags = make_ticket_runner()

    # a fingerprint the 30d cooldown already consumed never reroutes
    save_state(
        state_path,
        state_with(
            [entry],
            suggested_fingerprints=[
                {"fingerprint": entry["fingerprint"], "suggested_date": SUNDAY - DAY}
            ],
        ),
    )
    before = state_path.read_bytes()
    with pytest.raises(HarnessLoopError, match="promote budget exhausted"):
        apply_disposition(
            "p9:", "promote", config, state_path=state_path, now=SUNDAY, runner=runner
        )
    assert calls == []
    assert state_path.read_bytes() == before

    # report-only entries have nothing to promote
    save_state(state_path, state_with([ladder_entry(SUNDAY, 9, pattern="p9", key="a")]))
    with pytest.raises(HarnessLoopError, match="report-only"):
        apply_disposition(
            "p9:", "promote", config, state_path=state_path, now=SUNDAY, runner=runner
        )
    assert calls == []

    # happy path: the impl+review pair is created and the entry is closed
    save_state(state_path, state_with([entry]))
    message = apply_disposition(
        "p9:", "promote", config, state_path=state_path, now=SUNDAY, runner=runner
    )
    assert len(calls) == 2
    assert message.startswith(f"{entry['fingerprint']} -> promote (promoted, ")
    stored = load_state(state_path)
    assert stored["open_findings"][0]["fix_status"] == "applied"
    assert stored["resolved_topics"]
    # the promote joined the cooldown ledger: reopening the same finding the
    # same day cannot route a second pair
    assert [
        item for item in stored["suggested_fingerprints"] if item["fingerprint"] == entry["fingerprint"]
    ] == [{"fingerprint": entry["fingerprint"], "suggested_date": SUNDAY}]
    reopened = dict(entry, fix_status="open")
    save_state(
        state_path,
        state_with(
            [reopened],
            resolved_topics=stored["resolved_topics"],
            suggested_fingerprints=stored["suggested_fingerprints"],
        ),
    )
    with pytest.raises(HarnessLoopError, match="promote budget exhausted"):
        apply_disposition(
            "p9:", "promote", config, state_path=state_path, now=SUNDAY, runner=runner
        )
    assert len(calls) == 2


def test_escalation_composes_with_the_ladder() -> None:
    monday = ladder_entry(MONDAY, 5, pattern="p5", key="a", occurrence_count=7)
    config = make_config(Path("/tmp/ladder-unused"))
    escalation = _escalation_map([monday], config=config)
    assert escalation[monday["fingerprint"]] == ("medium", "high", 7, False)

    # an ESCALATED aged entry is never batched away: it keeps its nightly line
    collapsed = render_report(ladder_report([monday], now=MONDAY, escalation=escalation))
    assert "MEDIUM→HIGH (7 nights)" in collapsed
    assert "open >=3 nights" not in collapsed

    # ... while the plain aged backlog still collapses to the one summary line
    plain = ladder_entry(MONDAY, 5, pattern="p9", key="b")
    both = render_report(ladder_report([monday, plain], now=MONDAY, escalation=escalation))
    assert "MEDIUM→HIGH (7 nights)" in both
    assert "1 finding open >=3 nights — full digest Sunday" in both

    digest = render_report(
        ladder_report(
            [ladder_entry(SUNDAY, 5, pattern="p5", key="a", occurrence_count=7)],
            now=SUNDAY,
            escalation=escalation,
        )
    )
    assert "MEDIUM→HIGH (7 nights)" in digest
    assert "(open 5 nights)" in digest
    assert monday["severity"] == "medium"


def test_full_working_set_renders_both_cadences() -> None:
    ages = (0, 2, 3, 5, 14, 20)

    def entries(anchor: int) -> list[dict]:
        return [
            ladder_entry(anchor, age, pattern=f"p{age}", key=f"k{age}") for age in ages
        ]

    night = render_report(ladder_report(entries(MONDAY), now=MONDAY))
    assert "MEDIUM — P0 (open 0 nights)" in night
    assert "(open 2 nights)" in night
    assert "(open 3 nights)" not in night
    assert night.count("open >=3 nights — full digest Sunday") == 1
    assert "4 findings open >=3 nights — full digest Sunday" in night
    assert ROLLUP_HEADER not in night

    digest = render_report(ladder_report(entries(SUNDAY), now=SUNDAY))
    assert digest.count(DIGEST_HEADER) == 1
    assert digest.count(ROLLUP_HEADER) == 1
    assert digest.count("hkrc harness-loop disposition") == 2
    assert "4 findings open >=3 nights" not in digest
    assert "(open 14 nights)" in digest
    assert "(open 20 nights)" in digest
