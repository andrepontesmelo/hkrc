"""Process-amendment channel (t_ba158b41) — routing, gate, packs, report.

The process channel gives findings whose remediation is an ARTIFACT amendment
(a working-agreement skill or the supervisor mission file) a routing path that
mirrors the code-ticket path: an implementation card plus a parent-linked
review card on board ``hkrc``, budgeted separately from ``max_applies``, and
fed only by human-authored remediation packs.

Coverage:

- promotion rule (Q5): HIGH + non-empty remediation key, ranked
  ``occurrence_count`` desc then ``first_seen`` asc, capped at
  ``process_budget``, independent of ``hkrc_budget``/``max_applies``;
- emission payload: ADD carries the pack's full content, AMEND requires
  verbatim before-text grounded in the live artifact, both idempotency keys
  and the parent link match the hkrc pair shape;
- allowlist scope: repo targets rejected, unlisted absolute targets rejected,
  ADD outside an allowlisted directory rejected, production homes accepted;
- loader fail-closed: missing/malformed manifest, key mismatch, unknown kind,
  AMEND without before-text, content_file escaping the pack, missing or empty
  content -> no route at all;
- cooldown eligibility: process findings enter ``suggested_fingerprints``,
  ``none`` findings stay out unchanged;
- regression guards: the code-route candidate list and ``apply_policy_gate``
  leave process findings to this channel;
- report + ledger: a routed proposal renders its own section line and leaves
  the working set without a forever-resolved topic.
"""

from __future__ import annotations

import json
from pathlib import Path

from hkrc.harness_loop import (
    DEFAULT_PROCESS_ALLOWLIST,
    PROCESS_APPLY_KIND,
    PROCESS_REASK_REMEDIATION_KEY,
    Finding,
    _apply_candidates,
    _entry_to_finding,
    _process_allowlisted,
    _process_candidates,
    _process_card_body,
    _process_impl_card_title,
    _process_review_card_title,
    _process_scope_gate,
    _process_section_lines,
    _process_target,
    _open_sessions_read_only,
    apply_policy_gate,
    collect_sessions,
    dedupe,
    default_state_path,
    detect_reask,
    fingerprint,
    load_process_remediation,
    load_state,
    render_report,
    route_process_findings,
    run,
)

from test_harness_loop import (
    DAY,
    NOW,
    make_config,
    make_hkrc_repo,
    make_sessions_db,
    make_ticket_runner,
    queue_entry,
    session_row,
)

ROOT = Path(__file__).resolve().parents[1]
KEY = PROCESS_REASK_REMEDIATION_KEY


def process_finding(
    key: str = "k1",
    *,
    severity: str = "high",
    remediation_key: str = KEY,
) -> Finding:
    return Finding(
        pattern="reask",
        key=key,
        severity=severity,
        evidence=(f"2 fresh sessions asked the same first question ({key})",),
        suggestion="one thread per incident; use session_search handoff",
        apply_kind=PROCESS_APPLY_KIND,
        remediation_key=remediation_key,
    )


def process_entry(
    key: str = "k1",
    *,
    severity: str = "high",
    remediation_key: str = KEY,
    occurrence_count: int = 1,
    first_seen: int = NOW - DAY,
    fix_status: str = "open",
    apply_kind: str = PROCESS_APPLY_KIND,
) -> dict:
    """One persisted queue entry as ``dedupe`` writes it."""
    entry = queue_entry(
        f"reask:{key}",
        pattern="reask",
        key=key,
        severity=severity,
        apply_kind=apply_kind,
        occurrence_count=occurrence_count,
        first_seen=first_seen,
        last_seen=NOW - DAY,
        fix_status=fix_status,
        remediation_key=remediation_key,
    )
    # The persisted fingerprint is fingerprint() of the stored payload (the
    # real pipeline stores exactly that), so route-time lookups match.
    entry["fingerprint"] = fingerprint(_entry_to_finding(entry))
    return entry


def write_pack(
    repo: Path,
    *,
    key: str = KEY,
    target: str,
    kind: str = "ADD",
    content: str = "PACK CONTENT ONE THREAD PER INCIDENT\n",
    before: str | None = None,
    content_file: str = "SKILL.md",
    manifest_override: str | None = None,
    write_content: bool = True,
) -> Path:
    """Write one canned remediation pack into a fixture repo."""
    pack = repo / "config" / "hkrc" / "process_remediations" / key
    pack.mkdir(parents=True, exist_ok=True)
    if manifest_override is None:
        manifest: dict = {
            "version": 1,
            "key": key,
            "target_path": target,
            "kind": kind,
            "content_file": content_file,
        }
        if before is not None:
            manifest["before"] = before
        pack.joinpath("manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
    else:
        pack.joinpath("manifest.json").write_text(manifest_override, encoding="utf-8")
    if write_content:
        pack.joinpath(content_file).write_text(content, encoding="utf-8")
    return pack


def add_pack(repo: Path, tmp_path: Path, **kwargs) -> tuple[Path, str]:
    """Write an ADD pack into an allowlisted tmp artifact home; return its
    directory and the absolute target path."""
    artifact_dir = tmp_path / "dist" / KEY
    artifact_dir.mkdir(parents=True, exist_ok=True)
    target = str(artifact_dir / "SKILL.md")
    write_pack(repo, target=target, **kwargs)
    return artifact_dir, target


# --- promotion rule (Q5) ----------------------------------------------------


def test_promotion_requires_high_severity_and_pack_key(tmp_path: Path) -> None:
    """HIGH + non-empty remediation key; medium or keyless entries never be."""
    config = make_config(tmp_path)
    entries = [
        process_entry("high_ok"),
        process_entry("medium", severity="medium"),
        process_entry("no_key", remediation_key=""),
        process_entry("report_only", apply_kind="none", remediation_key=""),
    ]
    promoted = _process_candidates(
        entries, config=config, now=NOW, cooldown_seconds=30 * DAY
    )
    assert [finding.key for finding in promoted] == ["high_ok"]
    assert promoted[0].apply_kind == PROCESS_APPLY_KIND
    assert promoted[0].remediation_key == KEY


def test_promotion_ranks_occurrence_then_first_seen(tmp_path: Path) -> None:
    """occurrence_count desc, then first_seen asc (most-repeated, then oldest)."""
    config = make_config(tmp_path, process_budget=3)
    entries = [
        process_entry("young_once", occurrence_count=1, first_seen=NOW - 5 * DAY),
        process_entry("old_once", occurrence_count=1, first_seen=NOW - 40 * DAY),
        process_entry("thrice", occurrence_count=3, first_seen=NOW - 2 * DAY),
    ]
    promoted = _process_candidates(
        entries, config=config, now=NOW, cooldown_seconds=30 * DAY
    )
    assert [finding.key for finding in promoted] == ["thrice", "old_once", "young_once"]


def test_promotion_caps_at_process_budget(tmp_path: Path) -> None:
    """At most ``process_budget`` proposals a night; 0 disables the channel."""
    entries = [
        process_entry("a", occurrence_count=3),
        process_entry("b", occurrence_count=2),
        process_entry("c", occurrence_count=1),
    ]
    one = make_config(tmp_path, process_budget=1)
    assert [f.key for f in _process_candidates(entries, config=one, now=NOW, cooldown_seconds=30 * DAY)] == ["a"]
    two = make_config(tmp_path, process_budget=2)
    assert [f.key for f in _process_candidates(entries, config=two, now=NOW, cooldown_seconds=30 * DAY)] == ["a", "b"]
    off = make_config(tmp_path, process_budget=0)
    assert _process_candidates(entries, config=off, now=NOW, cooldown_seconds=30 * DAY) == []


def test_promotion_skips_closed_and_cooldowned_entries(tmp_path: Path) -> None:
    """A cooldowned (recently routed) or closed entry is not re-proposed."""
    config = make_config(tmp_path, process_budget=2)
    entries = [
        process_entry("stale", fix_status="stale"),
        process_entry("applied", fix_status="applied"),
        process_entry("resolved", fix_status="resolved"),
        process_entry("deferred_ok", fix_status="deferred"),
        process_entry("cooled"),
    ]
    suggested = [{"fingerprint": "reask:cooled", "suggested_date": NOW - DAY}]
    promoted = _process_candidates(
        entries,
        config=config,
        now=NOW,
        cooldown_seconds=30 * DAY,
        suggested_fingerprints=suggested,
    )
    assert [finding.key for finding in promoted] == ["deferred_ok"]
    # Cooldown is not removal: past the window the entry is a candidate again.
    promoted_later = _process_candidates(
        entries,
        config=config,
        now=NOW + 31 * DAY,
        cooldown_seconds=30 * DAY,
        suggested_fingerprints=suggested,
    )
    assert [finding.key for finding in promoted_later] == ["deferred_ok", "cooled"]


def test_process_budget_independent_of_max_applies(tmp_path: Path) -> None:
    """A full code-fix night never starves the amendment channel."""
    repo = make_hkrc_repo(tmp_path)
    artifact_dir, target = add_pack(repo, tmp_path)
    config = make_config(tmp_path, max_applies=2, process_budget=1, hkrc_repo=repo)
    runner, calls, _flags = make_ticket_runner()
    # max_applies=2 would allow 2 code pairs; the process budget is its own
    # slot and is consumed even when the code channel already routed.
    hkrc = Finding(
        pattern="skill-contradiction",
        key="s1",
        severity="high",
        evidence=("e",),
        suggestion="s",
        apply_kind="hkrc",
        before="OLD_WORD",
        after="NEW_WORD",
        target_path=str(repo / "src" / "hkrc" / "thing.py"),
        verify_path=str(repo / "src" / "hkrc" / "thing.py"),
        verify_text="OLD_WORD",
    )
    applied, deferrals = apply_policy_gate(
        [hkrc], config, now=NOW, dry_run=False, runner=runner
    )
    assert [change.kind for change in applied] == ["hkrc"]
    assert deferrals == ()
    assert len(calls) == 2
    process_applied, process_deferrals = route_process_findings(
        [process_finding("p1")], config, dry_run=False, runner=runner
    )
    assert [change.kind for change in process_applied] == [PROCESS_APPLY_KIND]
    assert process_deferrals == ()
    assert len(calls) == 4  # code pair + amendment pair, separate budgets
    assert (artifact_dir / "SKILL.md").is_file() is False  # never applied here
    assert target.startswith(str(tmp_path))


# --- emission payload -------------------------------------------------------


def test_route_add_emits_full_pack_content_and_idempotency_keys(
    tmp_path: Path,
) -> None:
    """The ADD card carries target, marker, full content, fingerprint, keys."""
    repo = make_hkrc_repo(tmp_path)
    artifact_dir, target = add_pack(repo, tmp_path, content="SKILL BODY LINE\n")
    config = make_config(tmp_path, hkrc_repo=repo, reviewer_profiles=("reviewer",))
    runner, calls, _flags = make_ticket_runner()
    finding = process_finding("fp1")
    fp = fingerprint(finding)

    applied, deferrals = route_process_findings(
        [finding], config, dry_run=False, runner=runner
    )

    assert deferrals == ()
    assert len(applied) == 1
    change = applied[0]
    assert change.kind == PROCESS_APPLY_KIND
    assert change.fingerprint == fp
    assert change.before == "ADD"
    assert change.after == KEY
    assert change.path == target
    assert "tickets impl=t_impl0001 review=t_review0001" in change.note

    assert len(calls) == 2
    impl_call, review_call = calls
    assert _process_impl_card_title(
        _require_pack(repo, config)
    ) in impl_call
    assert f"harness-proc-impl:{fp}" in impl_call
    assert f"harness-proc-review:{fp}" in review_call
    assert f"--workspace dir:{artifact_dir}" in impl_call
    assert f"--workspace dir:{artifact_dir}" in review_call
    assert "--parent t_impl0001" in review_call
    assert "--assignee reviewer" in review_call

    body = _process_card_body(finding, _require_pack(repo, config), reviewer=False)
    assert f"fingerprint {fp}" in body
    assert f"Artifact home (absolute): {target}" in body
    assert "ADD: create the artifact" in body
    assert "SKILL BODY LINE" in body
    assert body.index("---8<--- begin content") < body.index("SKILL BODY LINE")


def test_route_review_card_body_verifies_before_text(tmp_path: Path) -> None:
    """The review body carries the verification contract (substring, backup)."""
    repo = make_hkrc_repo(tmp_path)
    add_pack(repo, tmp_path)
    config = make_config(tmp_path, hkrc_repo=repo)
    pack = _require_pack(repo, config)
    review_body = _process_card_body(
        process_finding("fp2"), pack, reviewer=True, impl_id="t_impl0007"
    )
    assert "Review implementation t_impl0007" in review_body
    assert "SUBSTRING match against the LIVE artifact" in review_body
    assert "never fuzzy-match" in review_body
    assert ".bak-<YYYYMMDD>" in review_body
    assert "quote the applied after-text" in review_body
    assert "do not rewrite it" in review_body


def test_route_amend_is_grounded_on_verbatim_before_text(tmp_path: Path) -> None:
    """AMEND routes only when the artifact exists and still holds before-text."""
    repo = make_hkrc_repo(tmp_path)
    skill_dir = tmp_path / "dist" / "working-agreement"
    skill_dir.mkdir(parents=True, exist_ok=True)
    skill = skill_dir / "SKILL.md"
    skill.write_text("intro\nOLD AGREEMENT LINE\noutro\n", encoding="utf-8")
    write_pack(
        repo,
        key="amend-demo",
        target=str(skill),
        kind="AMEND",
        before="OLD AGREEMENT LINE",
        content="NEW AGREEMENT LINE\n",
    )
    config = make_config(tmp_path, hkrc_repo=repo, reviewer_profiles=("reviewer",))
    runner, calls, _flags = make_ticket_runner()
    finding = process_finding("amend1", remediation_key="amend-demo")

    applied, deferrals = route_process_findings(
        [finding], config, dry_run=False, runner=runner
    )
    assert deferrals == ()
    assert len(applied) == 1
    assert applied[0].before == "AMEND"
    assert len(calls) == 2
    body = _process_card_body(
        finding, _require_pack(repo, config, "amend-demo"), reviewer=False
    )
    assert "AMEND: the artifact above currently contains this exact text" in body
    assert "OLD AGREEMENT LINE" in body
    assert "NEW AGREEMENT LINE" in body
    # The harness itself never edits the artifact.
    assert skill.read_text(encoding="utf-8").startswith("intro\nOLD AGREEMENT LINE")

    # before-text no longer present -> fail closed, no card.
    skill.write_text("intro\nREPLACED ALREADY\noutro\n", encoding="utf-8")
    runner2, calls2, _flags2 = make_ticket_runner()
    applied2, deferrals2 = route_process_findings(
        [finding], config, dry_run=False, runner=runner2
    )
    assert applied2 == ()
    assert calls2 == []
    assert any("before-text not found in target" in reason for reason in deferrals2)

    # target missing entirely -> fail closed, no card.
    skill.unlink()
    runner3, calls3, _flags3 = make_ticket_runner()
    applied3, deferrals3 = route_process_findings(
        [finding], config, dry_run=False, runner=runner3
    )
    assert applied3 == ()
    assert calls3 == []
    assert any("AMEND target missing" in reason for reason in deferrals3)


def test_dry_run_routes_nothing(tmp_path: Path) -> None:
    """The default is report-only: an operator preview creates no card."""
    repo = make_hkrc_repo(tmp_path)
    add_pack(repo, tmp_path)
    config = make_config(tmp_path, hkrc_repo=repo)
    runner, calls, _flags = make_ticket_runner()
    applied, deferrals = route_process_findings(
        [process_finding("dry")], config, dry_run=True, runner=runner
    )
    assert applied == ()
    assert deferrals == ()
    assert calls == []


# --- scope gate / allowlist -------------------------------------------------


def test_scope_gate_rejects_target_inside_hkrc_repo(tmp_path: Path) -> None:
    """Repo targets route via the hkrc channel — always, fail closed."""
    repo = make_hkrc_repo(tmp_path)
    inside = repo / "docs" / "working-agreement.md"
    write_pack(repo, key="repo-target", target=str(inside))
    config = make_config(tmp_path, hkrc_repo=repo)
    pack = load_process_remediation("repo-target", config)
    assert not isinstance(pack, str)
    reason = _process_scope_gate(process_finding("r", remediation_key="repo-target"), pack, config)
    assert reason == "repo targets route via hkrc channel"


def test_scope_gate_rejects_unlisted_absolute_target(tmp_path: Path) -> None:
    """An absolute path outside the allowlist never routes."""
    repo = make_hkrc_repo(tmp_path)
    outside = tmp_path / "elsewhere" / "SKILL.md"
    write_pack(repo, key="outside", target=str(outside))
    config = make_config(tmp_path, hkrc_repo=repo)
    pack = load_process_remediation("outside", config)
    assert not isinstance(pack, str)
    reason = _process_scope_gate(process_finding("o", remediation_key="outside"), pack, config)
    assert reason is not None and "outside the process allowlist" in reason


def test_scope_gate_add_must_land_in_allowlisted_directory(tmp_path: Path) -> None:
    """ADD is confined to allowlisted DIRECTORIES (Q6 creation constraint)."""
    repo = make_hkrc_repo(tmp_path)
    config = make_config(tmp_path, hkrc_repo=repo)  # allowlist: <tmp>/dist/*
    allowed = tmp_path / "dist" / "new-skill" / "SKILL.md"
    deeper = tmp_path / "dist" / "a" / "b" / "SKILL.md"
    blocked = tmp_path / "other" / "new-skill" / "SKILL.md"
    write_pack(repo, key="allowed", target=str(allowed))
    write_pack(repo, key="deeper", target=str(deeper))
    write_pack(repo, key="blocked", target=str(blocked))
    allowlist = config.harness_loop.process_allowlist
    assert _process_allowlisted(_process_target(_require_pack(repo, config, "allowed")), allowlist, is_add=True)
    assert _process_allowlisted(_process_target(_require_pack(repo, config, "deeper")), allowlist, is_add=True)
    assert not _process_allowlisted(_process_target(_require_pack(repo, config, "blocked")), allowlist, is_add=True)


def test_scope_gate_accepts_production_artifact_homes(tmp_path: Path) -> None:
    """The Q1 homes pass the allowlist; the ~ expansion uses the real home."""
    repo = make_hkrc_repo(tmp_path)
    config = make_config(
        tmp_path, hkrc_repo=repo, process_allowlist=DEFAULT_PROCESS_ALLOWLIST
    )
    for key, target in (
        ("dist-skill", "~/.hermes/dist-skills/one-thread-per-incident/SKILL.md"),
        ("mission", "~/.hermes/hkrc/config/hkrc/supervisor-mission.md"),
    ):
        kind = "ADD" if key == "dist-skill" else "AMEND"
        write_pack(repo, key=key, target=target, kind=kind, before="x")
        pack = _require_pack(repo, config, key)
        reason = _process_scope_gate(
            process_finding(key, remediation_key=key), pack, config
        )
        assert reason is None, reason
        assert _process_target(pack).is_absolute()
    # "~" resolves to an absolute real-home path under the artifact home.
    resolved = str(_process_target(_require_pack(repo, config, "dist-skill")))
    assert resolved.startswith("/") and "/.hermes/dist-skills/" in resolved
    assert "profiles" not in resolved


# --- loader fail-closed -----------------------------------------------------


def test_loader_fail_closed_cases(tmp_path: Path) -> None:
    """Every broken pack routes NOTHING (reason string, zero cards)."""
    repo = make_hkrc_repo(tmp_path)
    target = str(tmp_path / "dist" / "x" / "SKILL.md")
    config = make_config(tmp_path, hkrc_repo=repo)
    runner, calls, _flags = make_ticket_runner()

    # (a) missing manifest entirely
    (repo / "config" / "hkrc" / "process_remediations" / "absent").mkdir(
        parents=True, exist_ok=True
    )
    cases: list[tuple[str, str]] = [
        ("absent", "remediation pack missing"),
        ("", "no remediation key"),
        ("   ", "no remediation key"),
    ]
    for key, expected in cases:
        reason = load_process_remediation(key, config)
        assert isinstance(reason, str) and expected in reason, (key, reason)

    # (b) malformed manifest
    write_pack(repo, key="bad_json", target=target, manifest_override="{not json")
    assert "unreadable" in str(load_process_remediation("bad_json", config))
    write_pack(repo, key="not_object", target=target, manifest_override='["a"]')
    assert "must be an object" in str(load_process_remediation("not_object", config))
    write_pack(repo, key="key_mismatch", target=target, manifest_override=json.dumps(
        {"key": "someone-else", "target_path": target, "kind": "ADD", "content_file": "SKILL.md"}
    ))
    assert "key mismatch" in str(load_process_remediation("key_mismatch", config))
    write_pack(repo, key="no_target", target=target, manifest_override=json.dumps(
        {"key": "no_target", "kind": "ADD", "content_file": "SKILL.md"}
    ))
    assert "no target_path" in str(load_process_remediation("no_target", config))
    write_pack(repo, key="bad_kind", target=target, manifest_override=json.dumps(
        {"key": "bad_kind", "target_path": target, "kind": "DELETE", "content_file": "SKILL.md"}
    ))
    assert "kind must be ADD or AMEND" in str(load_process_remediation("bad_kind", config))
    write_pack(repo, key="amend_no_before", target=target, kind="AMEND")
    assert "requires verbatim before text" in str(
        load_process_remediation("amend_no_before", config)
    )
    write_pack(repo, key="no_content_file", target=target, manifest_override=json.dumps(
        {"key": "no_content_file", "target_path": target, "kind": "ADD"}
    ))
    assert "no content_file" in str(load_process_remediation("no_content_file", config))
    write_pack(repo, key="escape", target=target, content_file="../escape.md", write_content=False)
    assert "must stay inside the pack" in str(load_process_remediation("escape", config))
    write_pack(repo, key="missing_content", target=target, write_content=False)
    assert "content missing" in str(load_process_remediation("missing_content", config))
    write_pack(repo, key="empty_content", target=target, content="   \n")
    assert "content is empty" in str(load_process_remediation("empty_content", config))

    # None of the broken packs may route: one deferral each, zero cards.
    for key in (
        "bad_json",
        "not_object",
        "key_mismatch",
        "no_target",
        "bad_kind",
        "amend_no_before",
        "no_content_file",
        "escape",
        "missing_content",
        "empty_content",
    ):
        applied, deferrals = route_process_findings(
            [process_finding(key, remediation_key=key)],
            config,
            dry_run=False,
            runner=runner,
        )
        assert applied == (), key
        assert len(deferrals) == 1 and deferrals[0].startswith("[reask:"), key
    assert calls == []


def test_shipped_pack_is_valid_and_gates_clean(tmp_path: Path) -> None:
    """The repo's own pack loads, is ADD, and passes the production gate."""
    config = make_config(
        tmp_path, hkrc_repo=ROOT, process_allowlist=DEFAULT_PROCESS_ALLOWLIST
    )
    pack = load_process_remediation(KEY, config)
    assert not isinstance(pack, str), pack
    assert pack.kind == "ADD"
    assert pack.target_path == f"~/.hermes/dist-skills/{KEY}/SKILL.md"
    assert "One thread per incident" in pack.content
    assert 'session_search(query=' in pack.content
    assert pack.content.startswith("---\nname: one-thread-per-incident")

    finding = process_finding("shipped", remediation_key=KEY)
    assert _process_scope_gate(finding, pack, config) is None

    # Byte-for-byte: the card body carries the pack content unmodified, and the
    # content file on disk matches what the loader read.
    body = _process_card_body(finding, pack, reviewer=False)
    content_on_disk = (
        ROOT / "config" / "hkrc" / "process_remediations" / KEY / "SKILL.md"
    ).read_text(encoding="utf-8")
    assert content_on_disk.rstrip("\n") in body

    # Manifest shape follows the documented contract.
    manifest = json.loads(
        (ROOT / "config" / "hkrc" / "process_remediations" / KEY / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["key"] == KEY
    assert manifest["kind"] == "ADD"
    assert manifest["content_file"] == "SKILL.md"
    assert "before" not in manifest  # ADD packs carry no before-text


# --- wiring / regression guards --------------------------------------------


def test_apply_candidates_excludes_process_findings(tmp_path: Path) -> None:
    """The code-ticket candidate list never offers a process finding."""
    entry = process_entry("excluded")
    code_entry = queue_entry(
        "hkrc-fix:t_a",
        pattern="hkrc-fix",
        key="t_a",
        severity="high",
        apply_kind="hkrc",
    )
    candidates = _apply_candidates(
        [entry, code_entry], [], now=NOW, cooldown_seconds=30 * DAY
    )
    assert [finding.key for finding in candidates] == ["t_a"]


def test_apply_policy_gate_skips_process_findings_without_deferral(
    tmp_path: Path,
) -> None:
    """The code router neither routes nor defers a process finding."""
    repo = make_hkrc_repo(tmp_path)
    config = make_config(tmp_path, hkrc_repo=repo)
    runner, calls, _flags = make_ticket_runner()
    applied, deferrals = apply_policy_gate(
        [process_finding("p")], config, now=NOW, dry_run=False, runner=runner
    )
    assert applied == ()
    assert deferrals == ()
    assert calls == []


def test_dedupe_process_finding_enters_cooldown_none_stays_out() -> None:
    """Routed candidates cool down; report-only findings keep firing."""
    process = process_finding("p1")
    report_only = Finding(
        pattern="bloat",
        key="b1",
        severity="medium",
        evidence=("e",),
        suggestion="s",
    )
    fresh, updated = dedupe([process, report_only], {"created": "2026-08-01"}, now=NOW)
    recorded = {entry["fingerprint"] for entry in updated["suggested_fingerprints"]}
    assert fingerprint(process) in recorded
    assert fingerprint(report_only) not in recorded
    assert len(fresh) == 2


def test_dedupe_upgrades_persisted_none_entry_to_process() -> None:
    """A pre-t_ba158b41 reask entry (apply_kind none) becomes routable."""
    stored = process_entry("legacy", apply_kind="none", remediation_key="")
    stored["fix_status"] = "applied"
    stored["remediation_key"] = ""
    state = {
        "created": "2026-08-01",
        "last_run": NOW - DAY,
        "resolved_topics": [],
        "suggested_fingerprints": [],
        "open_findings": [stored],
    }
    fresh, updated = dedupe([process_finding("legacy")], state, now=NOW)
    assert len(fresh) == 1
    entry = updated["open_findings"][0]
    assert entry["apply_kind"] == PROCESS_APPLY_KIND
    assert entry["remediation_key"] == KEY
    assert entry["fix_status"] == "open"  # reopened for the current verdict
    assert entry["occurrence_count"] == 2


def test_detect_reask_emits_process_kind_and_pack_key() -> None:
    """The reask detector is the first process-channel producer."""
    sessions = [
        session_row(
            "s1",
            started_at=NOW - 3600,
            ended_at=NOW - 3000,
            input_tokens=1200,
            first_message="rebuild the hkrc release and then rerun the probe",
        ),
        session_row(
            "s2",
            started_at=NOW - 1800,
            ended_at=NOW - 1200,
            input_tokens=900,
            first_message="rebuild the hkrc release and then rerun the probe",
        ),
    ]
    findings = detect_reask(sessions)
    assert len(findings) == 1
    finding = findings[0]
    assert finding.severity == "high"
    assert finding.apply_kind == PROCESS_APPLY_KIND
    assert finding.remediation_key == PROCESS_REASK_REMEDIATION_KEY


def test_report_renders_process_section(tmp_path: Path) -> None:
    """Routed proposals and the budget left render; empty renders 'none'."""
    config = make_config(tmp_path, process_budget=2)
    from hkrc.harness_loop import AppliedChange, HarnessReport

    routed = AppliedChange(
        kind=PROCESS_APPLY_KIND,
        fingerprint="reask:abc",
        before="ADD",
        after=KEY,
        sha="",
        path="/tmp/SKILL.md",
        note="tickets impl=t_a review=t_b",
    )
    lines = _process_section_lines([routed], config=config)
    assert lines == (f"routed 1/2 amendment proposal(s): {KEY}; process budget remaining 1",)
    assert _process_section_lines([], config=config) == (
        "routed 0/2 amendment proposal(s); process budget remaining 2",
    )

    report = render_report(
        HarnessReport(
            story="story",
            wrong=(),
            skipped=(),
            applied=(),
            deploy_ready="none",
            right=(),
            next_action="none",
            process=lines,
        )
    )
    assert "Process proposals (amendment channel)" in report
    assert f"routed 1/2 amendment proposal(s): {KEY}" in report
    assert "Deploy-ready" in report
    empty = render_report(
        HarnessReport(
            story="story",
            wrong=(),
            skipped=(),
            applied=(),
            deploy_ready="none",
            right=(),
            next_action="none",
        )
    )
    section = empty.split("Process proposals (amendment channel)", 1)[1].split(
        "Deploy-ready", 1
    )[0]
    assert "• none" in section


# --- end to end -------------------------------------------------------------


def test_run_routes_persisted_process_item_and_marks_applied(tmp_path: Path) -> None:
    """AC: a persisted process item promotes, routes a pair, and is reported.

    The ledger transition is deliberate: the entry leaves the working set but
    is NOT recorded as a forever-resolved topic — routing an amendment is not
    evidence that the finding is resolved.
    """
    repo = make_hkrc_repo(tmp_path)
    artifact_dir, target = add_pack(repo, tmp_path)
    message = "rebuild the hkrc release and rerun the nightly harness loop audit"
    sessions_db = make_sessions_db(
        tmp_path / "profiles" / "main" / "state.db",
        [
            {
                "id": "s1",
                "started_at": NOW - 7200,
                "ended_at": NOW - 3600,
                "input_tokens": 1200,
                "message_count": 4,
                "first_messages": [message],
            },
            {
                "id": "s2",
                "started_at": NOW - 1800,
                "ended_at": NOW - 600,
                "input_tokens": 900,
                "message_count": 3,
                "first_messages": [message],
            },
        ],
    )
    config = make_config(
        tmp_path,
        sessions_db=sessions_db,
        hkrc_repo=repo,
        process_budget=1,
        reviewer_profiles=("reviewer",),
    )
    # Drive the detection stage for real: the persisted entry must match the
    # fingerprint the detector produces NOW, so revalidation keeps it open
    # (reask is revalidated by "still detected in the current-state scan").
    connection = _open_sessions_read_only(sessions_db)
    detected = detect_reask(
        collect_sessions(connection, config.harness_loop.window_hours, now=NOW)
    )
    connection.close()
    assert len(detected) == 1
    detected_fp = fingerprint(detected[0])
    assert detected[0].apply_kind == PROCESS_APPLY_KIND
    assert detected[0].remediation_key == KEY
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW - DAY,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": [
                    process_entry(
                        detected[0].key,
                        severity="high",
                        occurrence_count=5,
                        first_seen=NOW - 9 * DAY,
                    )
                ],
            }
        ),
        encoding="utf-8",
    )
    runner, calls, _flags = make_ticket_runner()
    report = run(config, now=NOW, dry_run=False, runner=runner)

    assert len(calls) == 2  # implementation card + parent-linked review card
    assert f"--workspace dir:{artifact_dir}" in calls[0]
    loaded = load_state(state_file)
    entry = loaded["open_findings"][0]
    assert entry["fix_status"] == "applied"
    assert loaded["resolved_topics"] == []  # never a forever-resolved topic
    assert entry["fingerprint"] == detected_fp
    assert any(
        suggested["fingerprint"] == detected_fp
        for suggested in loaded["suggested_fingerprints"]
    )
    assert f"routed 1/1 amendment proposal(s): {KEY}" in report
    assert "amendment pair routed; artifact home, no repo deploy" in report
    # The harness routes; the card pair applies. Nothing was written here.
    assert not (artifact_dir / "SKILL.md").exists()
    assert target.endswith(f"{KEY}/SKILL.md")


def test_missing_pack_surfaces_routing_blocker(tmp_path: Path) -> None:
    """A promoted proposal with no usable pack is loud, never silent.

    Fail-closed means the finding is not routed AND the operator is told why:
    the deferral reaches the report as a routing blocker, and the queue entry
    is marked ``deferred`` (still open, so the next eligible night retries).
    """
    repo = make_hkrc_repo(tmp_path)  # deliberately no remediation packs
    message = "rebuild the hkrc release and rerun the nightly harness loop audit"
    sessions_db = make_sessions_db(
        tmp_path / "profiles" / "main" / "state.db",
        [
            {
                "id": "s1",
                "started_at": NOW - 7200,
                "ended_at": NOW - 3600,
                "input_tokens": 1200,
                "first_messages": [message],
            },
            {
                "id": "s2",
                "started_at": NOW - 1800,
                "ended_at": NOW - 600,
                "input_tokens": 900,
                "first_messages": [message],
            },
        ],
    )
    config = make_config(tmp_path, sessions_db=sessions_db, hkrc_repo=repo)
    state_file = default_state_path(config.state_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "created": "2026-08-01",
                "last_run": NOW - DAY,
                "resolved_topics": [],
                "suggested_fingerprints": [],
                "open_findings": [],
            }
        ),
        encoding="utf-8",
    )
    runner, calls, _flags = make_ticket_runner()
    report = run(config, now=NOW, dry_run=False, runner=runner)

    lowered = report.casefold()
    assert "routing blocker" in lowered
    assert "remediation pack missing" in lowered
    assert KEY in report
    assert calls == []  # fail closed: no card without a usable pack
    entry = next(
        item
        for item in load_state(state_file)["open_findings"]
        if item["pattern"] == "reask"
    )
    assert entry["apply_kind"] == PROCESS_APPLY_KIND
    assert entry["remediation_key"] == KEY
    assert entry["fix_status"] == "deferred"


def _require_pack(repo: Path, config, key: str = KEY):
    """Loader assertion helper: the pack must load, else fail the test loud."""
    pack = load_process_remediation(key, config)
    assert not isinstance(pack, str), f"pack {key} failed to load: {pack}"
    return pack
