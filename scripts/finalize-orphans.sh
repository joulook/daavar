#!/usr/bin/env bash
# Finish the bookkeeping of orphaned rows (2026-11-05, brain04: the paper queues' dispatchers and their run_row
# wrappers were killed while the rows' /usr/bin/time + binary kept running). A row is orphaned when its status says
# state=running, its pid is dead, and /usr/bin/time has written time-v.txt (the row ended). This script appends the
# done block exactly as run_row does (exit status from time-v.txt, the same reason rules), records and publishes the
# row, and marks it done, so that no dispatcher relaunches it and its result reaches brain-data.
# usage: finalize-orphans.sh [loop-seconds]   (no argument: one pass; with one: a loop, e.g. detached with nohup)
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"
pass() {
  local s d id p st start to reason now
  for s in "$EVAL_OUT"/*/status; do
    grep -q '^state=running' "$s" 2>/dev/null || continue
    d=$(dirname "$s"); id=$(basename "$d"); p=$(sed -n 's/^pid=//p' "$s" | head -1)
    [ -n "$p" ] && kill -0 "$p" 2>/dev/null && continue          # still running
    [ -f "$d/time-v.txt" ] && grep -q "Elapsed (wall clock)" "$d/time-v.txt" || continue   # not ended (or never started)
    [ -e "$d/done" ] && continue
    st=$(sed -n 's/^.*Exit status: \([0-9]*\).*$/\1/p' "$d/time-v.txt" | head -1); [ -n "$st" ] || st=0
    grep -q "Command terminated by signal 9" "$d/time-v.txt" && st=137
    start=$(sed -n 's/^start_epoch=//p' "$s" | head -1); to=$(sed -n 's/^timeout_s=//p' "$s" | head -1); now=$(date +%s)
    reason=ok
    if [ "$st" -eq 1 ]; then case "$(cat "$d/cmd.txt")" in *" conf "*|*conf-join*|*" online "*|*--spec*) reason=ok;; *) reason="exit-1";; esac; fi
    if [ "$st" -eq 137 ]; then
      local wall; wall=$(sed -n 's/.*Elapsed (wall clock) time (h:mm:ss or m:ss): //p' "$d/time-v.txt" | awk -F: '{if (NF==3) print $1*3600+$2*60+$3; else print $1*60+$2}' | cut -d. -f1)
      if [ -n "$wall" ] && [ -n "$to" ] && [ "$wall" -ge $((to - 5)) ]; then reason=timeout; else reason="invalid: killed after ${wall:-?} s of $to"; fi
    fi
    { [ "$st" -ne 0 ] && [ "$st" -ne 1 ] && [ "$reason" = ok ]; } && reason="exit-$st"
    grep -qi "out of memory\|memory allocation of\|Cannot allocate\|OutOfMemoryError" "$d/stderr.log" "$d/stdout.log" 2>/dev/null && reason=memory
    { echo "state=done"; echo "end=$(date -Is)"; echo "exit=$st"; echo "reason=$reason"; echo "wall_s=$(( now - ${start:-now} ))"; echo "finalized=orphan $(date -Is)"; } >> "$s"
    sed -i 's/^state=running/state=finished/' "$s"
    python3 -I "$S/mkrecord.py" "$d" >> "$EVAL_OUT/results.tsv" 2>>"$d/mkrecord.err" || echo "mkrecord failed for $id"
    touch "$d/done"
    "$S/publish.sh" "$d" >/dev/null 2>&1
    echo "$(date -Is) finalized orphan $id exit=$st reason=$reason"
  done
}
if [ -n "${1:-}" ]; then while :; do pass; sleep "$1"; done; else pass; fi
