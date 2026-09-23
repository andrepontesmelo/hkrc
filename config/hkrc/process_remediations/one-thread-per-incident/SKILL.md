---
name: one-thread-per-incident
description: Use when a new session continues an incident, task, or question that an earlier session already worked on. Keep one thread per incident and hand off context with session_search instead of re-deriving it from scratch.
---

# One thread per incident

## Rule

When a new session is about the SAME incident, task, or question as an earlier
session, continue that thread instead of re-deriving the answer from scratch.

1. Search the session history first: `session_search(query="<the incident>")`.
2. Reuse the earlier thread's findings, file paths, decisions, and open
   questions as given.
3. Re-ask only what the earlier thread genuinely did not answer.
4. If the earlier thread is stale or wrong, say so explicitly and state what
   changed - never silently restart.

## Why

A fresh session that re-derives a settled answer pays the whole context cost
again (input tokens) and risks drifting from a decision that was already made.
The nightly harness loop's reask detector flags exactly this pattern as its
number one token saver.

## Anti-pattern

Treating "same question, new session" as "new question". That is the pattern
this agreement exists to stop; the fix is a handoff, not a re-derivation.
