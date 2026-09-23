#!/usr/bin/env python3
"""Probe the process-amendment channel against the REAL ledger (t_ba158b41).

Evidence for the acceptance criterion "the reask finding promotes and the
emitted payload matches the pack".  It runs the promotion + emission flow
in-process against the live instance state and a STUB card creator, so:

- no real kanban card is created,
- the live ledger (``harness-loop-state.json``) and the artifact home are
  never written,
- the payload the loop WOULD send is dumped for inspection.

Flow: load the live config -> collect the live sessions in the window ->
``detect_reask`` -> ``dedupe`` into an in-memory copy of the live ledger ->
``_process_candidates`` (the Q5 promotion rule) -> ``route_process_findings``
with an argv-capturing runner.  Exits non-zero when the promotion or the
payload does not match the contract.

The detection window matters: reask groups are groups of sessions inside one
window, so ``--window-hours 24`` (the configured live window) can legitimately
hold none.  The default is ``auto``: widen 24 -> 72 -> 168 -> 336 hours and
use the first window that yields a reask finding, printing which one was
used.  Pass an explicit ``--window-hours`` to pin it.

Remediation packs ship with the change under review, so ``--repo`` (default:
this script's repo root) is what the pack root is resolved against; the live
config and ledger are only ever read.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import sys
import time
from pathlib import Path
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from hkrc.config import ControllerConfig, load_config  # noqa: E402
from hkrc.harness_loop import (  # noqa: E402
    HKRC_IMPL_ASSIGNEE,
    PROCESS_APPLY_KIND,
    PROCESS_REASK_REMEDIATION_KEY,
    ProcessResult,
    _open_sessions_read_only,
    _process_candidates,
    _process_card_body,
    _process_target,
    collect_sessions,
    default_state_path,
    detect_reask,
    dedupe,
    fingerprint,
    load_process_remediation,
    load_state,
    route_process_findings,
)

DEFAULT_CONFIG = Path.home() / ".hermes" / "hkrc" / "config" / "hkrc" / "config.toml"
AUTO_WINDOWS = (24, 72, 168, 336)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--repo",
        default=str(REPO),
        help="repo root holding config/hkrc/process_remediations (pack root)",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=0,
        help="0 = auto (24/72/168/336 until a reask group is found)",
    )
    parser.add_argument(
        "--out",
        default="/tmp/hkrc-process-probe-payload.json",
        help="payload dump path",
    )
    args = parser.parse_args()

    config: ControllerConfig = load_config(Path(args.config))
    # The packs live with the change under review; everything else (ledger,
    # sessions db, budgets, allowlist) stays the live instance's.
    config = dataclasses.replace(
        config,
        harness_loop=dataclasses.replace(
            config.harness_loop, hkrc_repo=Path(args.repo)
        ),
    )
    loop = config.harness_loop
    state_file = default_state_path(config.state_db)
    state = load_state(state_file)
    now = int(time.time())

    sessions_db = loop.sessions_db
    assert sessions_db is not None, "harness_loop.sessions_db is unset"
    windows = (
        (float(loop.window_hours),) if args.window_hours > 0 else AUTO_WINDOWS
    )
    window_used = 0.0
    findings = ()
    sessions_count = 0
    for hours in windows:
        connection = _open_sessions_read_only(sessions_db)
        try:
            sessions = collect_sessions(connection, hours, now=now)
        finally:
            connection.close()
        found = detect_reask(sessions)
        print(f"window {hours:g}h: {len(sessions)} sessions -> {len(found)} reask group(s)")
        if found:
            findings, sessions_count, window_used = found, len(sessions), float(hours)
            break
    if not findings:
        print("no reask finding in any probed window: nothing to promote")
        return 2

    # dedupe works on an in-memory copy: the live ledger stays untouched.
    fresh, updated = dedupe(
        list(findings), state, now=now, cooldown_days=loop.cooldown_days
    )
    del fresh
    candidates = _process_candidates(
        updated.get("open_findings", []),
        config=config,
        now=now,
        cooldown_seconds=int(loop.cooldown_days * 86400),
        # The PRE-dedupe ledger copy, exactly as run() does it: dedupe has just
        # recorded this run's suggestions, and feeding those back would
        # cooldown-suppress the finding the loop is about to route.
        suggested_fingerprints=state.get("suggested_fingerprints", []),
    )
    if not candidates:
        print("reask detected but NOT promoted (see queue entry state below)")
        for entry in updated.get("open_findings", []):
            if str(entry.get("pattern", "")) == "reask":
                print(
                    json.dumps(
                        {
                            "fingerprint": entry.get("fingerprint"),
                            "severity": entry.get("severity"),
                            "apply_kind": entry.get("apply_kind"),
                            "remediation_key": entry.get("remediation_key"),
                            "fix_status": entry.get("fix_status"),
                            "occurrence_count": entry.get("occurrence_count"),
                        },
                        indent=1,
                    )
                )
        return 3

    calls: list[list[str]] = []

    def stub_runner(argv, env, timeout):  # noqa: ANN001, ARG001
        argv_list = [str(part) for part in argv]
        calls.append(argv_list)
        is_review = "--parent" in argv_list
        task_id = "t_review0001" if is_review else "t_impl0001"
        return ProcessResult(0, json.dumps({"id": task_id}), "")

    applied, deferrals = route_process_findings(
        candidates, config, dry_run=False, runner=stub_runner
    )

    finding = candidates[0]
    fp = fingerprint(finding)
    pack = load_process_remediation(finding.remediation_key, config)
    if isinstance(pack, str):
        print(f"FAIL: pack failed to load: {pack}")
        return 4
    target = _process_target(pack)
    body = _process_card_body(finding, pack, reviewer=False)
    payload = {
        "fingerprint": fp,
        "promoted": [candidate.key for candidate in candidates],
        "applied": [
            {
                "kind": change.kind,
                "fingerprint": change.fingerprint,
                "before": change.before,
                "after": change.after,
                "path": change.path,
                "note": change.note,
            }
            for change in applied
        ],
        "deferrals": list(deferrals),
        "pack": {
            "key": pack.key,
            "kind": pack.kind,
            "target_path": pack.target_path,
            "resolved_target": str(target),
            "content_sha256": hashlib.sha256(pack.content.encode("utf-8")).hexdigest(),
        },
        "card_argv": calls,
        "impl_body_excerpt": [
            line
            for line in body.splitlines()
            if line.startswith(("Nightly", "Pattern:", "Remediation pack:", "ADD", "Artifact home"))
        ],
    }
    Path(args.out).write_text(json.dumps(payload, indent=1), encoding="utf-8")

    problems: list[str] = []
    if len(applied) != 1:
        problems.append(f"expected exactly 1 applied proposal, got {len(applied)}")
    if deferrals:
        problems.append(f"unexpected deferrals: {deferrals}")
    if finding.severity != "high":
        problems.append(f"severity is {finding.severity!r}, expected 'high'")
    if finding.remediation_key != PROCESS_REASK_REMEDIATION_KEY:
        problems.append(
            f"remediation_key is {finding.remediation_key!r}, "
            f"expected {PROCESS_REASK_REMEDIATION_KEY!r}"
        )
    if finding.apply_kind != PROCESS_APPLY_KIND:
        problems.append(f"apply_kind is {finding.apply_kind!r}, expected 'process'")
    if len(calls) != 2:
        problems.append(f"expected 2 card creations, got {len(calls)}")
    else:
        impl, review = calls
        workspace = f"dir:{target.parent}"
        if workspace not in impl:
            problems.append(f"impl card workspace missing {workspace}: {impl}")
        if f"harness-proc-impl:{fp}" not in impl:
            problems.append("impl idempotency key missing")
        if "--parent" not in review:
            problems.append("review card is not parent-linked")
        if impl[impl.index("--assignee") + 1] != HKRC_IMPL_ASSIGNEE:
            problems.append("impl card assignee mismatch")
        if pack.content.rstrip("\n") not in body:
            problems.append("card body does not carry the pack content verbatim")
        if str(target) not in body:
            problems.append("card body does not carry the absolute target path")

    print(f"config:            {args.config}")
    print(f"pack root:         {args.repo}")
    print(f"ledger:            {state_file}")
    print(f"window used:       {window_used:g}h ({sessions_count} sessions)")
    print(f"reask findings:    {len(findings)} -> promoted {[c.key for c in candidates]}")
    print(f"fingerprint:       {fp}")
    print(f"pack:              {pack.key} ({pack.kind}) -> {target}")
    print(f"applied:           {[change.kind for change in applied]}")
    print(f"cards:             {len(calls)} (impl + review)")
    print(f"payload dump:      {args.out}")
    if problems:
        print("PROBE FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("PROBE OK: reask promoted, payload matches the shipped pack")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
