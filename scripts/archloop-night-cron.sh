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
#   6. Exception: `plan` / `apply` subcommands (operator UX, t_495f8ac7) run the
#      per-repo preflight for ONE repo instead of the nightly walk. `plan` prints
#      the exact unblock steps and stores them in <repo>/.archloop/preflight-plan.txt;
#      `apply` re-derives the plan, refuses on any drift since it was written, and
#      otherwise executes only the classified steps.
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

# --- preflight subcommands: plan / apply (operator UX, t_495f8ac7) -----------
# The nightly walk only says "SKIP <repo>: dirty canonical checkout"; these two
# subcommands turn that into exact steps for ONE repo.  Classification is
# conservative: known tool-junk dirs get excluded (cheap, reversible), everything
# else is stashed as one unit.  No command that discards work is ever emitted or
# run -- nothing here deletes a file, rewrites history or throws a stash away.
PREFLIGHT_JUNK_DIRS=(.claude .scratch .dex .horizon .archloop .worktrees)
PREFLIGHT_DETAIL_MAX="${ARCHLOOP_PREFLIGHT_DETAIL_MAX:-20}"
PLAN_FILE=".archloop/preflight-plan.txt"

preflight_usage() {
  printf '%s\n' \
    "usage: archloop-night-cron.sh                       # nightly run (default)" \
    "       archloop-night-cron.sh plan  --repo <path>    # print + store the steps" \
    "       archloop-night-cron.sh apply --repo <path> [--yes]  # run the stored steps"
}

# Porcelain of one checkout.  `raw` skips the plan-artifact filter (the nightly
# listing should show exactly what its own dirty probe saw); the filtered form is
# what the digest and the classification use, so that writing a plan can never
# change the digest it recorded.
preflight_porcelain() {
  local repo="$1" raw="${2:-}"
  if [ "$raw" = "raw" ]; then
    git -C "$repo" status --porcelain 2>/dev/null
  else
    git -C "$repo" status --porcelain 2>/dev/null | grep -v -F "$PLAN_FILE" || true
  fi
}

# sha256 of the full porcelain output (minus the plan artifact above).
preflight_digest() { preflight_porcelain "$1" | sha256sum | cut -d' ' -f1; }

# Indented (2-space) porcelain listing: the nightly evidence for a dirty skip.
# Every line carries the indent, so a detail line can never match the frozen
# ^SKIPPED detector shape; the listing is capped to keep the cron report small.
preflight_detail() {
  local repo="$1" line total shown=0
  total="$(preflight_porcelain "$repo" raw | wc -l)"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    printf '  %s\n' "$line"
    shown=$((shown + 1))
  done < <(preflight_porcelain "$repo" raw | head -n "$PREFLIGHT_DETAIL_MAX")
  [ "$total" -gt "$shown" ] && printf '  ... (%s more)\n' "$((total - shown))"
  return 0
}

# Base branch resolved exactly like andre-archloop/run.sh does it:
# ARCHLOOP_BASE_BRANCH from <repo>/.archloop/config, default main.  Sourced in a
# subshell so the config's TEST/LINT/UI keys never leak into this launcher.
preflight_base_branch() {
  local repo="$1"
  (
    ARCHLOOP_BASE_BRANCH=""
    if [ -f "$repo/.archloop/config" ]; then
      . "$repo/.archloop/config" >/dev/null 2>&1 || true
    fi
    printf '%s\n' "${ARCHLOOP_BASE_BRANCH:-main}"
  )
}

preflight_exclude_path() {
  git -C "$1" rev-parse --path-format=absolute --git-path info/exclude
}

# Validate --repo: it must resolve under the configured repo root (default ~/git)
# and be a real git checkout.  Prints the absolute path.
preflight_repo_path() {
  local repo root
  repo="$(realpath -m "$1" 2>/dev/null)" || return 1
  root="$(realpath -m "$REPO_ROOT" 2>/dev/null)"
  case "$repo/" in
    "$root"/*) ;;
    *)
      printf 'refusing: --repo must live under %s (got %s)\n' "$root" "$1" >&2
      return 1
      ;;
  esac
  if ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
    printf 'refusing: not a git checkout: %s\n' "$repo" >&2
    return 1
  fi
  printf '%s\n' "$repo"
}

# The exclude pattern for an untracked tool-junk entry: the dir itself when git
# reports one ("?? .claude/"), the top-level tool dir for a nested file
# ("?? .claude/x" -> ".claude/"), or the file when it carries the tool dir's name.
preflight_junk_pattern() {
  local path="$1"
  case "$path" in
    */) printf '%s' "$path" ;;
    */*) printf '%s/' "${path%%/*}" ;;
    *) printf '%s' "$path" ;;
  esac
}

# One "<class>\t<value>" line per dirty entry: `junk` = untracked path whose first
# component is a known tool-junk dir (value = exclude pattern; appending it is
# cheap and reversible), `stash` = tracked modification or loose untracked file
# (value = path; all of them go into the single stash).
preflight_classify() {
  local repo="$1" line path first pattern seen=" "
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    path="${line:3}"
    case "$path" in *" -> "*) path="${path##* -> }" ;; esac
    if [ "${line:0:2}" != "??" ]; then
      printf 'stash\t%s\n' "$path"
      continue
    fi
    first="${path%%/*}"
    case " ${PREFLIGHT_JUNK_DIRS[*]} " in
      *" $first "*) ;;
      *) printf 'stash\t%s\n' "$path"; continue ;;
    esac
    pattern="$(preflight_junk_pattern "$path")"
    case "$seen" in *" $pattern "*) continue ;; esac
    seen="$seen$pattern "
    printf 'junk\t%s\n' "$pattern"
  done < <(preflight_porcelain "$repo")
}

preflight_stash_message() { printf 'archloop preflight %s %s' "$(date +%F)" "$(basename "$1")"; }

# The plan artifact lives in .archloop/ — a known tool dir.  Real archloop repos
# already exclude it (the loop driver creates that entry); where they do not,
# this is emitted/appended first so that writing a plan can never leave the
# checkout dirty for the nightly walk.  Prints the pattern, or nothing at all.
preflight_self_exclude_pattern() {
  local repo="$1" exclude
  exclude="$(preflight_exclude_path "$repo")"
  grep -qxF ".archloop/" "$exclude" 2>/dev/null && return 0
  printf '%s' ".archloop/"
}

# The classified command block: exclude appends first (so the stash below cannot
# swallow tool state), then at most one stash, then the return to base branch.
preflight_commands() {
  local repo="$1" base="$2" branch="$3" cls value exclude cmds=0 self has_stash=0
  local -a junk=()
  exclude="$(preflight_exclude_path "$repo")"
  while IFS=$'\t' read -r cls value; do
    case "$cls" in
      junk) junk+=("$value") ;;
      stash) has_stash=1 ;;
    esac
  done < <(preflight_classify "$repo")
  self="$(preflight_self_exclude_pattern "$repo")"
  if [ -n "$self" ]; then
    printf '%s\n' "printf '%s\\n' '$self' >> $exclude"
    cmds=$((cmds + 1))
  fi
  for value in "${junk[@]}"; do
    printf '%s\n' "printf '%s\\n' '$value' >> $exclude"
    cmds=$((cmds + 1))
  done
  if [ "$has_stash" = 1 ]; then
    printf 'git -C %s stash push -u -m "%s"\n' "$repo" "$(preflight_stash_message "$repo")"
    cmds=$((cmds + 1))
  fi
  if [ "$branch" != "$base" ]; then
    printf 'git -C %s checkout %s\n' "$repo" "$base"
    cmds=$((cmds + 1))
  fi
  [ "$cmds" -gt 0 ] || printf '# (nothing to do: clean checkout on base)\n'
}

# Plan-only advice: SHIPPED ledger shas that never landed on the base branch.
# apply ignores this section -- it never merges.
preflight_stranded() {
  local repo="$1" base="$2" ledger="$1/.archloop/ledger.md"
  local line branch sha shown=0
  if ! git -C "$repo" rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
    printf '#   base %s not found here - skipped\n' "$base"
    return 0
  fi
  if [ ! -f "$ledger" ]; then
    printf '#   no ledger\n'
    return 0
  fi
  while IFS= read -r line; do
    case "$line" in *"SHIPPED on "*) ;; *) continue ;; esac
    branch="${line##*SHIPPED on }"
    branch="${branch%% (*}"
    sha="${line##*\(}"
    sha="${sha%%\)*}"
    [ -n "$branch" ] && [ -n "$sha" ] || continue
    git -C "$repo" cat-file -e "$sha^{commit}" >/dev/null 2>&1 || continue
    git -C "$repo" merge-base --is-ancestor "$sha" "$base" >/dev/null 2>&1 && continue
    shown=$((shown + 1))
    [ "$shown" -le 10 ] || continue
    printf '#   %s (%s) not on %s:\n' "$sha" "$branch" "$base"
    printf '#     git -C %s merge --no-ff %s\n' "$repo" "$branch"
  done < "$ledger"
  [ "$shown" -gt 10 ] && printf '#   (+%s more)\n' "$((shown - 10))"
  [ "$shown" -gt 0 ] || printf '#   none\n'
  return 0
}

# Full plan body for one repo: identity + branch + digest + dirty listing +
# command block (apply --yes runs exactly the block) + plan-only advice.
preflight_plan_text() {
  local repo="$1" name base branch
  name="$(basename "$repo")"
  base="$(preflight_base_branch "$repo")"
  branch="$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  printf '# archloop preflight plan -- %s\n' "$name"
  printf 'repo: %s\n' "$repo"
  printf 'name: %s\n' "$name"
  printf 'date: %s\n' "$(date +%F)"
  printf 'branch: %s\n' "$branch"
  printf 'base: %s\n' "$base"
  printf 'digest: %s\n' "$(preflight_digest "$repo")"
  printf '# dirty set (%s entry/entries):\n' "$(preflight_porcelain "$repo" | wc -l)"
  preflight_porcelain "$repo" | sed 's/^/#   /'
  if [ "$branch" = "$base" ]; then
    printf '# branch: ON-INTENDED (%s) -- no action\n' "$base"
  else
    printf '# branch: not on %s -- step below returns the checkout to base\n' "$base"
  fi
  printf '# commands (apply --yes executes exactly these; nothing else)\n'
  preflight_commands "$repo" "$base" "$branch"
  printf '# stranded ship (plan-only advice; apply never merges)\n'
  preflight_stranded "$repo" "$base"
}

preflight_write_plan() {
  local repo="$1" plan="$1/.archloop/preflight-plan.txt" text
  mkdir -p "$repo/.archloop"
  text="$(preflight_plan_text "$repo")" || return 1
  printf '%s\n' "$text" > "$plan"
  printf '%s\n' "$text"
  printf 'plan written: %s\n' "$plan"
  printf 'next: bash scripts/archloop-night-cron.sh apply --repo %s --yes\n' "$repo"
}

# Re-derive, compare against the stored plan, then act.  Any drift since the plan
# was written (branch switched, files added/removed/changed) refuses and runs
# nothing -- the operator re-plans and re-approves.
preflight_apply() {
  local repo="$1" yes="$2" plan="$1/.archloop/preflight-plan.txt"
  local p_repo p_branch p_digest branch digest base cls path exclude self
  if [ ! -f "$plan" ]; then
    printf 'no plan at %s -- run: bash scripts/archloop-night-cron.sh plan --repo %s\n' \
      "$plan" "$repo"
    return 1
  fi
  p_repo="$(sed -n 's/^repo: //p' "$plan" | head -1)"
  p_branch="$(sed -n 's/^branch: //p' "$plan" | head -1)"
  p_digest="$(sed -n 's/^digest: //p' "$plan" | head -1)"
  branch="$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null)"
  digest="$(preflight_digest "$repo")"
  if [ "$p_repo" != "$repo" ] || [ "$p_branch" != "$branch" ] || [ "$p_digest" != "$digest" ]; then
    printf 'REFUSED: the checkout drifted since the plan was written\n'
    printf '  plan: repo=%s branch=%s digest=%s\n' "$p_repo" "$p_branch" "$p_digest"
    printf '  now:  repo=%s branch=%s digest=%s\n' "$repo" "$branch" "$digest"
    printf 'nothing executed. re-run: bash scripts/archloop-night-cron.sh plan --repo %s\n' "$repo"
    return 1
  fi
  base="$(preflight_base_branch "$repo")"
  exclude="$(preflight_exclude_path "$repo")"
  local -a junk_patterns=() stash_paths=()
  while IFS=$'\t' read -r cls path; do
    case "$cls" in
      junk) junk_patterns+=("$path") ;;
      stash) stash_paths+=("$path") ;;
    esac
  done < <(preflight_classify "$repo")

  printf 'archloop preflight apply -- %s\n' "$(basename "$repo")"
  printf '  branch: %s (base %s)\n' "$branch" "$base"
  printf '  digest: %s (matches plan)\n' "$digest"
  printf '  steps:\n'
  self="$(preflight_self_exclude_pattern "$repo")"
  if [ -n "$self" ]; then
    printf '%s\n' "    printf '%s\\n' '$self' >> $exclude"
  fi
  for pattern in "${junk_patterns[@]}"; do
    printf '%s\n' "    printf '%s\\n' '$pattern' >> $exclude"
  done
  if [ ${#stash_paths[@]} -gt 0 ]; then
    printf '    git -C %s stash push -u -m "%s"\n' "$repo" "$(preflight_stash_message "$repo")"
  fi
  if [ "$branch" != "$base" ]; then
    printf '    git -C %s checkout %s\n' "$repo" "$base"
  fi
  if [ "$yes" != "1" ]; then
    printf 'print-only (no --yes): nothing executed -- re-run with --yes to apply\n'
    return 0
  fi

  # Execute exactly the classified steps, in order: exclude appends, one stash,
  # then the checkout back to base.  Nothing else, ever.
  if [ -n "$self" ]; then
    if printf '%s\n' "$self" >> "$exclude"; then
      printf 'excluded: %s\n' "$self"
    else
      printf 'exclude append failed (%s) -- aborted, nothing else executed\n' "$exclude"
      return 1
    fi
  fi
  for pattern in "${junk_patterns[@]}"; do
    if grep -qxF "$pattern" "$exclude" 2>/dev/null; then
      printf 'already excluded: %s\n' "$pattern"
      continue
    fi
    if printf '%s\n' "$pattern" >> "$exclude"; then
      printf 'excluded: %s\n' "$pattern"
    else
      printf 'exclude append failed (%s) -- aborted, nothing else executed\n' "$exclude"
      return 1
    fi
  done
  if [ ${#stash_paths[@]} -gt 0 ]; then
    if git -C "$repo" stash push -u -m "$(preflight_stash_message "$repo")" >/dev/null; then
      printf 'stashed: %s\n' "$(preflight_stash_message "$repo")"
    else
      printf 'stash failed -- aborted, nothing else executed\n'
      return 1
    fi
  fi
  if [ "$branch" != "$base" ]; then
    if git -C "$repo" checkout "$base" >/dev/null 2>&1; then
      printf 'checked out: %s\n' "$base"
    else
      printf 'checkout %s failed -- stash left in place\n' "$base"
      return 1
    fi
  fi
  printf 'dirty now: %s entry/entries\n' "$(preflight_porcelain "$repo" raw | wc -l)"
  return 0
}

preflight_main() {
  local mode="$1"
  shift
  local repo="" yes=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --repo)
        [ $# -ge 2 ] || { printf 'plan/apply need a value for --repo\n' >&2; return 1; }
        repo="$2"
        shift 2
        ;;
      --yes) yes=1; shift ;;
      -h|--help) preflight_usage; return 0 ;;
      *) printf 'unknown argument: %s\n' "$1" >&2; preflight_usage >&2; return 1 ;;
    esac
  done
  [ -n "$repo" ] || { printf 'plan/apply need --repo <path>\n' >&2; preflight_usage >&2; return 1; }
  repo="$(preflight_repo_path "$repo")" || return 1
  if [ "$mode" = "plan" ]; then
    preflight_write_plan "$repo"
  else
    preflight_apply "$repo" "$yes"
  fi
}

if [ "${1:-}" = "plan" ] || [ "${1:-}" = "apply" ]; then
  preflight_main "$@"
  exit $?
fi

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
    # Evidence: WHICH files blocked the night.  Indented, so no detail line can
    # ever match the frozen ^SKIPPED detector shape.
    preflight_detail "$repo" | tee -a "$NIGHT_LOG"
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
if [ ${#skipped_dirty[@]} -gt 0 ]; then
  # The digest line above stays byte-stable (the HKRC detector matches it); the
  # porcelain listing is appended beneath it as indented evidence lines.
  out+="
SKIPPED dirty (${#skipped_dirty[@]}): ${skipped_dirty[*]}"
  for name in "${skipped_dirty[@]}"; do
    detail="$(preflight_detail "$REPO_ROOT/$name")"
    [ -n "$detail" ] && out+="
$detail"
  done
fi
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
