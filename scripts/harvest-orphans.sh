#!/usr/bin/env bash
# Record rows whose harvester shell died (2026-11-02: the coordinator's drain killed reparented run_row shells).
# A row directory with state=running, no `done`, and a dead pid gets what run_row would have written: the exit
# status from /usr/bin/time's report, the reason, the record, the publish. usage: harvest-orphans.sh <outdir>
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; O="$1"; n=0; left=0
for f in "$O"/*/status; do
  d=$(dirname "$f"); id=$(basename "$d")
  grep -q "^state=running" "$f" || continue; [ -e "$d/done" ] && continue
  p=$(sed -n 's/^pid=//p' "$f" | head -1); [ -n "$p" ] && kill -0 "$p" 2>/dev/null && { left=$((left+1)); continue; }
  [ -s "$d/time-v.txt" ] || { left=$((left+1)); continue; }   # the row's process is gone but time's report is not there yet
  st=$(sed -n 's/.*Exit status: \([0-9]*\).*/\1/p' "$d/time-v.txt" | tail -1)
  grep -q "terminated by signal 9" "$d/time-v.txt" && st=137; [ -z "$st" ] && st=1
  cmd=$(cat "$d/cmd.txt"); start=$(sed -n 's/^start_epoch=//p' "$f" | head -1); end=$(stat -c %Y "$d/time-v.txt")
  reason=ok
  if [ "$st" -eq 1 ]; then case "$cmd" in *" conf "*|*conf-join*|*" online "*|*--spec*) reason=ok;; *) reason="exit-1";; esac; fi
  [ "$st" -eq 137 ] && reason=timeout
  { [ "$st" -ne 0 ] && [ "$st" -ne 1 ] && [ "$reason" = ok ]; } && reason="exit-$st"
  grep -qi "out of memory\|memory allocation of\|Cannot allocate\|OutOfMemoryError" "$d/stderr.log" "$d/stdout.log" 2>/dev/null && reason=memory
  { echo "state=done"; echo "end=$(date -Is -d @$end)"; echo "exit=$st"; echo "reason=$reason"; echo "wall_s=$(( end - ${start:-end} ))"; echo "harvested=orphan $(date -Is)"; } >> "$f"
  sed -i 's/^state=running/state=finished/' "$f"
  python3 -I "$S/mkrecord.py" "$d" >> "$O/results.tsv" 2>>"$d/mkrecord.err" || echo "mkrecord failed for $id" >&2
  touch "$d/done"; "$S/publish.sh" "$d" >/dev/null 2>&1; n=$((n+1)); echo "harvested $id exit=$st reason=$reason"
done
echo "$(hostname): $n orphan rows harvested, $left still running"
