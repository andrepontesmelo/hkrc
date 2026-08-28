# QA Defects

## DEF-001 — Exact threshold is treated as unclaimed too early

- **Status:** CLOSED
- **Severity:** HIGH
- **Found by:** QA reviewer (t_b4ef6948)
- **Requirement:** A child is unclaimed for **more than** N seconds before it is flagged. The original ticket explicitly uses `> N`.

### Steps to reproduce

1. Run from the HKRC implementation worktree:
   ```bash
   cd <repo-root>/.worktrees/t_5fb9b223
   uv run python - <<'PY'
   from pathlib import Path
   import sqlite3, sys
   sys.path.insert(0, "src")
   from hkrc.discovery import discover_unclaimed_children

   root = Path("/tmp/hkrc-threshold-repro")
   board = root / "alpha"
   board.mkdir(parents=True, exist_ok=True)
   (board / "board.json").write_text('{"slug":"alpha"}', encoding="utf-8")
   db = board / "kanban.db"
   if db.exists():
       db.unlink()
   con = sqlite3.connect(db)
   con.executescript("""
   CREATE TABLE tasks (
       id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
       block_kind TEXT, assignee TEXT, created_at INTEGER,
       claim_lock TEXT, claim_expires INTEGER, current_run_id INTEGER
   );
   CREATE TABLE task_events (
       id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id INTEGER,
       kind TEXT NOT NULL, payload TEXT, created_at INTEGER NOT NULL
   );
   CREATE TABLE task_links (
       parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
       PRIMARY KEY(parent_id, child_id)
   );
   INSERT INTO tasks VALUES ('p','parent','done',NULL,NULL,0,NULL,NULL,NULL);
   INSERT INTO tasks VALUES ('c_exact','exact','todo',NULL,'reviewer',0,NULL,NULL,NULL);
   INSERT INTO tasks VALUES ('c_inside','inside','todo',NULL,'reviewer',0,NULL,NULL,NULL);
   INSERT INTO task_links VALUES ('p','c_exact');
   INSERT INTO task_links VALUES ('p','c_inside');
   INSERT INTO task_events VALUES (1,'p',NULL,'completed','{}',9000);
   INSERT INTO task_events VALUES (2,'c_exact',NULL,'created','{}',8200);
   INSERT INTO task_events VALUES (3,'c_inside',NULL,'created','{}',8201);
   """)
   con.commit()
   con.close()
   print([(x.task_id, x.latest_event_at)
          for x in discover_unclaimed_children(root, now=10000,
                                               unclaimed_after=1800)])
   PY
   ```
2. Observe the output.

### Expected outcome

No child is flagged: `c_exact` has been unclaimed for exactly 1,800 seconds and `c_inside` for 1,799 seconds. Neither satisfies the strict `> 1,800` requirement.

### Actual outcome

The output is:

```text
[('c_exact', 8200)]
```

`src/hkrc/discovery.py` uses `latest.created_at <= cutoff`, which implements `>= N` elapsed time, not `> N`.

### Evidence

The implementation's own boundary test (`tests/test_discovery.py::test_unclaimed_child_threshold_boundary_exactly_at_and_just_inside`) expects the exact-boundary child to be flagged. The design note says `<=` as well, but that contradicts the original ticket wording and acceptance requirement that the child be unclaimed for `> N` minutes.

### History

- 2026-08-03: Reproduced independently against commit `f3fe0b9`. Full unit suite passes, but this boundary behavior violates the ticket's strict threshold semantics. Awaiting developer correction and QA retest.
- 2026-08-03: CLOSED by QA after rerunning the exact threshold reproduction against fix commit `82930a7`: output was `[('c_outside', 8199)]`, with exact-boundary and younger children omitted. Regression checks passed: 9 unclaimed-child discovery tests, 2 child-handoff tests, 4 native-read-only/WAL/legacy-table tests, and the full suite (`178 passed`). `uv build` passed. A snapshot of the real campcli board detected the reconstructed historical `t_7dca377e` pattern, while the live native DB remained byte/stat unchanged; no product files were changed by QA.

## DEF-002 — Malformed/non-expiring claim locks are treated as expired

- **Status:** CLOSED
- **Severity:** HIGH
- **Found by:** Reviewer (t_0d78a817, review of implementation t_bcc99cc1)
- **Requirement:** Native Hermes claim semantics: a task is claimable only while `claim_lock IS NULL`. The native reclaim path clears lock and expiry together, so a non-null lock with a null expiry is malformed/active and must be skipped, never guessed as expired.

### Steps to reproduce

1. Create a board with a done parent and a todo child whose latest event is old
   (beyond the unclaimed window), with `claim_lock='worker'` and
   `claim_expires=NULL`.
2. Run `discover_unclaimed_children(root, now=now)`.

### Expected outcome

The child is not flagged: any non-null `claim_lock` means the task is claimed,
regardless of `claim_expires`.

### Actual outcome (reviewed branch `wt/t_bcc99cc1`)

The predicate `(c.claim_lock IS NULL OR c.claim_expires IS NULL OR
c.claim_expires < ?)` flags the child, treating the malformed lock as expired
and risking a double alert on an actively claimed task.

### Evidence

Merged fix (main, via merge `21fd9ed`): the query uses `c.claim_lock IS NULL`
only. Regression coverage: `test_unclaimed_child_already_claimed_is_not_flagged`,
`test_unclaimed_child_claim_lock_with_null_expiry_is_skipped`, and
`test_unclaimed_child_review_repro_exact_threshold_and_malformed_lock`
(kanban t_f789b4ab).

### History

- 2026-08-05: CLOSED by developer fix task t_f789b4ab after reproducing against
  main: the strict `claim_lock IS NULL` predicate and the null-expiry regression
  tests are present and passing.
