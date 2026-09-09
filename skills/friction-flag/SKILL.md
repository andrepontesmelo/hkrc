---
name: friction-flag
description: Use when a session hits friction worth recording - blocked twice on the same thing, a repeated re-ask, a missed skill trigger, a silent failure, or an ADHD-contract miss - to append one flag to the HKRC queue for the daily harness-learning-loop review.
version: 0.15.15
author: Hermes Agent
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [hermes, kanban, hkrc, friction, operations]
    related_skills: []
---

# Friction flag

## When to flag

Append one flag the moment you notice any of:

- blocked twice on the same thing;
- the user re-asks for something already answered;
- a skill trigger you should have caught and did not;
- a silent failure you spotted that nothing else detects;
- an ADHD-contract miss (answer not first, no single next action, unbounded list, drama over fix).

## Severity

- low: rubbed once, no block (a missed skill trigger, a rephrase, minor token waste).
- medium: workaround or repeated friction (a tool failed and you routed around it, manual
  steering of orchestration; a crash loop caught on the third try is a textbook medium).
- high: work blocked, or a silent failure nothing detects (card crash-looping, supervisor
  dark, wrong data nearly acted on).

Kind: orchestration | tooling | working-agreement | other.

## How

One line, fail-closed, never raises:

```bash
HKRC="$HOME/.hermes/hkrc/bin/hkrc"; [ -x "$HKRC" ] && "$HKRC" flag --severity <s> --kind <k> --note "<one line>" || true
```

The note is one line, max 500 characters, no secrets (scrubbed on write anyway). If the
binary is missing or not executable the line is inert: flagging must never fail a session.

## What happens next

Nothing, immediately. There is no detection mechanism: the agent adds flags, and the
daily harness-learning-loop revisits new flags as report lines and stats only. Repeating
the same note is fine - the reader collapses identical repeats to xN.
