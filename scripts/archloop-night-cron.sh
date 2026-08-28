#!/usr/bin/env bash
# archloop-night-cron.sh — nightly archloop launcher (hkrc project).
# Canonical logic lives in this repo; the cron runs a thin shim at
# ~/.hermes/profiles/main/scripts/archloop-night-cron.sh that execs this file.
#
# What it does, per night:
#   1. Resolve "active repos" = git repos with .archloop/config (archloop-ready,
#      has TEST/LINT cmds) AND (no kanban board OR board not archived).
#      Board archived = explicitly retired project -> excluded.
#   2. Per repo, gate on new commits: run only if the last commit on HEAD is
#      NEWER than the last archloop run (.archloop/ledger.md mtime). No ledger
#      yet = first run = run.
#   3. Preflight skips (cheap git checks, avoids burning a session on a
#      guaranteed run.sh abort): dirty canonical checkout, or not on main.
#   4. Launch archloop-loop.sh per eligible repo, detached (setsid nohup) so
#      loops survive this cron script's exit. No max repos per night (Andre).
#   5. Print a digest; stdout is delivered verbatim by the no_agent cron.
#
# Env: DRY_RUN=1 prints decisions without launching (for testing).
set -uo pipefail

export HOME="$(getent passwd "$(id -un)" | cut -d: -f6)"   # real user home (sandboxed-HOME fix)
HERMES="${HERMES:-$HOME/.local/bin/hermes}"
LOOP_DRIVER="${LOOP_DRIVER:-$HOME/git/andre-archloop/archloop-loop.sh}"
NIGHT_LOG="${NIGHT_LOG:-$HOME/.hermes/logs/archloop-night.log}"
REPO_ROOT="${ARCHLOOP_REPO_ROOT:-$HOME/git}"
ARCHIVED_BOARDS_DIR="${ARCHIVED_BOARDS_DIR:-$HOME/.hermes/kanban/boards/_archived}"
DRY="${DRY_RUN:-0}"

mkdir -p "$(dirname "$NIGHT_LOG")"
log() { printf '%s %s\n' "$(date '+%F %T')" "$*" | tee -a "$NIGHT_LOG"; }

[ -f "$LOOP_DRIVER" ] || { log "FATAL: loop driver not found at $LOOP_DRIVER"; echo "archloop-night: FATAL loop driver missing"; exit 1; }

# --- 1. boards: slug | archived | workdir ---
BOARDS_JSON="$("$HERMES" kanban boards list --all --json 2>/dev/null)"
if [ -z "$BOARDS_JSON" ]; then
  log "FATAL: hermes kanban boards list returned nothing"
  echo "archloop-night: FATAL kanban boards list failed"
  exit 1
fi

declare -A BOARD_ARCHIVED BOARD_WORKDIR
export BOARDS_JSON ARCHIVED_BOARDS_DIR
while IFS='|' read -r slug archived wd; do
  [ -n "$slug" ] || continue
  BOARD_ARCHIVED["$slug"]="$archived"
  [ -n "$wd" ] && BOARD_WORKDIR["$slug"]="$wd"
done < <(python3 - <<'PY'
import json, os, sys
try:
    boards = json.loads(os.environ.get("BOARDS_JSON", ""))
except Exception:
    sys.exit(1)

for b in boards:
    slug = b.get("slug", "")
    wd = b.get("default_workdir") or ""
    print(f"{slug}|{str(bool(b.get('archived'))).lower()}|{wd}")

# Archived boards are moved out of the live board listing. Their board.json
# currently retains archived:false, so the archive directory is authoritative.
from pathlib import Path

archive_root = os.environ.get("ARCHIVED_BOARDS_DIR", "")
if archive_root:
    for metadata_path in sorted(Path(archive_root).glob("*/board.json")):
        try:
            board = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        slug = board.get("slug", "")
        if not slug:
            continue
        wd = board.get("default_workdir") or ""
        print(f"{slug}|true|{wd}")
PY
)

started=()
skipped_no_new=()
skipped_retired=()
skipped_dirty=()
skipped_not_main=()
skipped_running=()

# --- 2-4. walk archloop-ready repos ---
for cfg in "$REPO_ROOT"/*/.archloop/config; do
  [ -f "$cfg" ] || continue
  repo="$(dirname "$(dirname "$cfg")")"
  name="$(basename "$repo")"

  # board lookup: by workdir match first, then slug==repo name
  board_slug=""
  for slug in "${!BOARD_WORKDIR[@]}"; do
    if [ "${BOARD_WORKDIR[$slug]}" = "$repo" ]; then board_slug="$slug"; break; fi
  done
  [ -z "$board_slug" ] && [ -n "${BOARD_ARCHIVED[$name]+x}" ] && board_slug="$name"

  if [ -n "$board_slug" ] && [ "${BOARD_ARCHIVED[$board_slug]}" = "true" ]; then
    log "SKIP $name: board '$board_slug' archived (retired)"
    skipped_retired+=("$name")
    continue
  fi

  # already running a loop for this repo?
  if pgrep -f "archloop-loop.sh $repo" >/dev/null 2>&1 || pgrep -f "andre-archloop/run.sh $repo" >/dev/null 2>&1; then
    log "SKIP $name: loop already running"
    skipped_running+=("$name")
    continue
  fi

  # preflight: clean checkout on main
  dirty="$(git -C "$repo" status --porcelain 2>/dev/null | head -1)"
  branch="$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  if [ -n "$dirty" ]; then
    log "SKIP $name: dirty canonical checkout"
    skipped_dirty+=("$name")
    continue
  fi
  if [ "$branch" != "main" ]; then
    log "SKIP $name: on '$branch' not main"
    skipped_not_main+=("$name")
    continue
  fi

  # gate: new commits since last run (ledger mtime); no ledger = first run
  ledger="$repo/.archloop/ledger.md"
  if [ -f "$ledger" ]; then
    last_run="$(stat -c %Y "$ledger")"
    last_commit="$(git -C "$repo" log -1 --format=%ct 2>/dev/null || echo 0)"
    if [ "$last_commit" -le "$last_run" ]; then
      log "SKIP $name: no new commits since last run ($(date -d @"$last_run" '+%F %T'))"
      skipped_no_new+=("$name")
      continue
    fi
  fi

  started+=("$name")
  log "START $name: launching loop driver"
  if [ "$DRY" != "1" ]; then
    setsid nohup env HOME="$HOME" bash "$LOOP_DRIVER" "$repo" >>"$NIGHT_LOG" 2>&1 &
    sleep 15   # stagger launches so the proxy isn't thundering-herded
  fi
done

# --- 5. digest ---
out="archloop-night $(date '+%F %T')"
[ ${#started[@]} -gt 0 ] && out+="
STARTED (${#started[@]}): ${started[*]}"
[ ${#skipped_no_new[@]} -gt 0 ] && out+="
SKIPPED no-new-commits (${#skipped_no_new[@]}): ${skipped_no_new[*]}"
[ ${#skipped_dirty[@]} -gt 0 ] && out+="
SKIPPED dirty (${#skipped_dirty[@]}): ${skipped_dirty[*]}"
[ ${#skipped_not_main[@]} -gt 0 ] && out+="
SKIPPED not-on-main (${#skipped_not_main[@]}): ${skipped_not_main[*]}"
[ ${#skipped_retired[@]} -gt 0 ] && out+="
SKIPPED board-archived (${#skipped_retired[@]}): ${skipped_retired[*]}"
[ ${#skipped_running[@]} -gt 0 ] && out+="
SKIPPED already-running (${#skipped_running[@]}): ${skipped_running[*]}"
[ ${#started[@]} -eq 0 ] && out+="
(nothing started this night)"

echo "$out"
exit 0
