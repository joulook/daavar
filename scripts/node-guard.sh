#!/usr/bin/env bash
# Node guard (owner, 2026-11-07: "the node protects itself; nobody intervenes by hand"). One detached loop per node,
# started by start-node-guard.sh, every INTERVAL s (default 30):
#  1. log cap: a running row's stdout.log / stderr.log above LOG_CAP_GB (2) of real disk use is truncated in place.
#     The writer's offset continues, so the file becomes sparse: the first 2 MB are kept in <log>.head, the tail with
#     the EVALROW record and the measurement block is what the writer prints last, so the record is not lost.
#  2. disk: below DISK_TRIM_GB (300) GB free, finished rows' logs above 1 GB are cut to head + tail 2 MB, largest
#     first (their excerpts are in brain-data already; exports and summaries are never touched); below DISK_KILL_GB (40)
#     GB free after that, the running row with the largest directory is killed (marker killed=disk-guard: recorded
#     "invalid: disk guard" by run-queue.sh, so a dispatcher can rerun it under a new id).
#  3. memory: below MEM_KILL_PCT (6) % of the machine available, the running row with the largest resident process
#     tree is killed (killed=memory-guard).
#  4. brain-data clone: .git above GC_GB (40) GB -> git gc --prune=now under the publish lock, at most once a day;
#     tmp_pack_* garbage older than a day (dead fetches) is removed.
# Every action is one line in $EVAL/guard.log and in the row's guard.log; a heartbeat line every hour.
# stop: touch $EVAL/.stop-guard
. "$(dirname "$0")/env.sh"
EVAL=$(dirname "$EVAL_OUT"); INTERVAL="${1:-30}"
: "${LOG_CAP_GB:=2}" "${DISK_TRIM_GB:=300}" "${DISK_KILL_GB:=40}" "${MEM_KILL_PCT:=6}" "${GC_GB:=40}"
LOG="$EVAL/guard.log"; HEAD_BYTES=2097152
say() { echo "$(date -Is) $*" >> "$LOG"; }
real_bytes() { local b; b=$(stat -c %b "$1" 2>/dev/null || echo 0); echo $((b * 512)); }
free_gb() { df -BG --output=avail "$EVAL" 2>/dev/null | tail -1 | tr -dc 0-9; }
avail_pct() { free -g | awk '/Mem:/ {if ($2 > 0) print int($7*100/$2); else print 100}'; }
tree_pids() {  # a process and all its descendants
  ps -e -o pid=,ppid= | awk -v root="$1" '{pp[$1]=$2} END {for (p in pp) {q=p; while (q && q!=root && (q in pp)) q=pp[q]; if (q==root) print p}}'
}
tree_rss_kb() { local p s=0; for p in $(tree_pids "$1"); do s=$((s + $(ps -o rss= -p "$p" 2>/dev/null || echo 0))); done; echo $s; }
running_rows() {  # "<rowdir> <pid>" for every row whose status says running and whose pid is alive
  local s p; for s in "$EVAL"/out-*/*/status; do
    grep -q '^state=running' "$s" 2>/dev/null || continue
    p=$(sed -n 's/^pid=//p' "$s" | head -1); [ -n "$p" ] && kill -0 "$p" 2>/dev/null && echo "$(dirname "$s") $p"
  done
}
kill_row() {  # rowdir pid marker
  local d="$1" p="$2" why="$3" q
  echo "$why" > "$d/killed"; echo "$(date -Is) killed: $why" >> "$d/guard.log"
  for q in $(tree_pids "$p"); do kill -KILL "$q" 2>/dev/null; done
  say "KILL $(basename "$d") ($why): pid $p and its tree"
}
cap_running_logs() {
  local d p f sz
  while read -r d p; do [ -n "$d" ] || continue
    for f in "$d/stdout.log" "$d/stderr.log"; do
      [ -f "$f" ] || continue; sz=$(real_bytes "$f")
      [ "$sz" -gt $((LOG_CAP_GB * 1073741824)) ] || continue
      [ -f "$f.head" ] || head -c $HEAD_BYTES "$f" > "$f.head" 2>/dev/null
      : > "$f"; echo "$(date -Is) truncated $(basename "$f") at $sz bytes of real use (cap ${LOG_CAP_GB} GB)" >> "$d/guard.log"
      say "TRUNCATE $(basename "$d")/$(basename "$f") at $sz bytes"
    done
  done < <(running_rows)
}
trim_done_logs() {  # cut finished rows' big logs, largest first, until free disk >= DISK_TRIM_GB
  local f d sz t
  while read -r sz f; do
    [ "$(free_gb)" -lt "$DISK_TRIM_GB" ] || break
    d=$(dirname "$f"); [ -e "$d/done" ] || continue; grep -q '^state=running' "$d/status" 2>/dev/null && continue
    t="$f.guard-tmp"
    { head -c $HEAD_BYTES "$f"; printf '\n... [node guard: %s bytes cut on %s, excerpt in brain-data] ...\n' "$sz" "$(date -Is)"; tail -c $HEAD_BYTES "$f"; } > "$t" 2>/dev/null && mv "$t" "$f" || { rm -f "$t"; continue; }
    echo "$(date -Is) cut $(basename "$f") from $sz bytes to head+tail" >> "$d/guard.log"; say "CUT $(basename "$d")/$(basename "$f") from $sz bytes (free $(free_gb) GB)"
  done < <(for f in "$EVAL"/out-*/*/stdout.log "$EVAL"/out-*/*/stderr.log; do [ -f "$f" ] || continue; sz=$(real_bytes "$f"); [ "$sz" -gt 1073741824 ] && echo "$sz $f"; done | sort -n -r)
}
disk_guard() {
  local fr d p big=0 bigd="" bigp="" u
  fr=$(free_gb); [ -n "$fr" ] || return
  [ "$fr" -lt "$DISK_TRIM_GB" ] && trim_done_logs
  fr=$(free_gb); [ "$fr" -lt "$DISK_KILL_GB" ] || return
  while read -r d p; do [ -n "$d" ] || continue; u=$(du -s "$d" 2>/dev/null | cut -f1); [ "${u:-0}" -gt "$big" ] && { big=$u; bigd=$d; bigp=$p; }; done < <(running_rows)
  [ -n "$bigd" ] && kill_row "$bigd" "$bigp" "disk-guard" || say "DISK ${fr} GB free and no running row to kill"
}
memory_guard() {
  local a d p big=0 bigd="" bigp="" r
  a=$(avail_pct); [ "${a:-100}" -lt "$MEM_KILL_PCT" ] || return
  while read -r d p; do [ -n "$d" ] || continue; r=$(tree_rss_kb "$p"); [ "$r" -gt "$big" ] && { big=$r; bigd=$d; bigp=$p; }; done < <(running_rows)
  [ -n "$bigd" ] && kill_row "$bigd" "$bigp" "memory-guard" || say "MEMORY ${a}% available and no running row to kill"
}
clone_guard() {
  local g="$BRAIN_DATA/.git" sz stamp="$EVAL/.guard-gc"
  [ -d "$g" ] || return
  find "$g/objects/pack" -maxdepth 1 -name 'tmp_pack_*' -mtime +1 -delete 2>/dev/null
  sz=$(du -sBG "$g" 2>/dev/null | cut -f1 | tr -dc 0-9); [ "${sz:-0}" -gt "$GC_GB" ] || return
  [ -f "$stamp" ] && [ "$(( $(date +%s) - $(stat -c %Y "$stamp") ))" -lt 86400 ] && return
  touch "$stamp"; say "GC brain-data clone .git ${sz} GB"
  ( cd "$BRAIN_DATA" && flock -w 60 9 && git gc -q --prune=now ) 9>"$BRAIN_DATA/.publish.lock" 2>>"$LOG"
  say "GC done: .git now $(du -sBG "$g" 2>/dev/null | cut -f1)"
}
say "guard started: LOG_CAP_GB=$LOG_CAP_GB DISK_TRIM_GB=$DISK_TRIM_GB DISK_KILL_GB=$DISK_KILL_GB MEM_KILL_PCT=$MEM_KILL_PCT GC_GB=$GC_GB every ${INTERVAL}s"
i=0
while [ ! -e "$EVAL/.stop-guard" ]; do
  cap_running_logs; disk_guard; memory_guard
  [ $((i % 20)) -eq 0 ] && clone_guard
  [ $((i % 120)) -eq 0 ] && say "heartbeat: free $(free_gb) GB, mem $(avail_pct)% available, running $(running_rows | wc -l)"
  i=$((i + 1)); sleep "$INTERVAL"
done
say "guard stopped"
