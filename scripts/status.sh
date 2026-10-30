#!/usr/bin/env bash
# What is running and what finished on this node (R3). usage: status.sh [queue-name]
. "$(dirname "$0")/env.sh"
echo "== $(hostname) $(date -Is) load $(cut -d' ' -f1-3 /proc/loadavg)"
for q in "$EVAL_OUT"/queues/*/; do
  [ -d "$q" ] || continue; n=$(basename "$q"); [ -n "${1:-}" ] && [ "$n" != "$1" ] && continue
  if [ -f "$q/pid" ] && kill -0 "$(cat "$q/pid")" 2>/dev/null; then st=RUNNING; else st=stopped; fi
  echo "queue $n: $st; $(grep -c '^row .* done' "$q/queue.log" 2>/dev/null) rows done; last: $(tail -1 "$q/queue.log" 2>/dev/null)"
done
echo "-- running rows"
for s in "$EVAL_OUT"/*/status; do
  [ -f "$s" ] || continue
  if grep -q '^state=running' "$s"; then d=$(dirname "$s"); e=$(( $(date +%s) - $(sed -n 's/^start_epoch=//p' "$s") ));
    printf '%s  %ds  %s  pid=%s\n' "$(basename "$d")" "$e" "$(sed -n 's/^cores=//p' "$s")" "$(sed -n 's/^pid=//p' "$s")"; fi
done
echo "-- results: $(wc -l < "$EVAL_OUT/results.tsv" 2>/dev/null) rows; by reason:"
[ -f "$EVAL_OUT/results.tsv" ] && cut -f40 "$EVAL_OUT/results.tsv" | sort | uniq -c | sort -rn | head
