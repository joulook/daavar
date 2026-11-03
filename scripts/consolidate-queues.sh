#!/usr/bin/env bash
# One dispatcher per machine (owner's timing fidelity, 2026-11-03 20:00): every live queue's remaining rows go into one
# queue that owns every NUMA node (minus the reserved ones), so no two rows ever share a node. The live dispatchers are
# stopped (their running rows and harvesters continue and record as usual); rows already done are skipped by the
# runner, rows running elsewhere are deferred. Priority: the paper's curated rows first, then reruns, then the campaign,
# then the filler. usage: consolidate-queues.sh [reserved nodes, comma list]
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; RES="${1:-}"
files=""; stopped=0
for q in /local/mkhoshechin/eval/out-*/queues/*/; do
  [ -f "$q/queue.log" ] || continue
  pid=$(head -1 "$q/queue.log" | sed -n 's/.*pid=\([0-9]*\).*/\1/p'); [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || continue
  f=$(head -1 "$q/queue.log" | sed -n 's/.*rows=\([^ ]*\).*/\1/p'); [ -f "$f" ] && files="$files $f"
  kill "$pid" && stopped=$((stopped+1))
done
sleep 2
rank() {  # lower runs first
  case "$1" in
    */reruns/table2-*|*/reruns/naive-table3*|*/reruns/reset-table3*|*/reruns/twopc-*|*/reruns/twopcab*|*/reruns/twopcds*|*/reruns/chain-*|*/reruns/commit1212*|*/reruns/hermes-spec2*) echo 0;;
    */reruns/pinfix-*|*/reruns/join-merge-*|*/reruns/join-1h-*|*/reruns/jmc-pin2-*|*/reruns/pin-rerun-*) echo 1;;
    */reruns/synthetic-48h*|*/scaling*|*/cf*|*/twins*|*/phase*) echo 2;;
    *) echo 3;;
  esac
}
out="$EVAL_OUT/queues/all-$(date +%m%d-%H%M%S).rows"   # seconds: a second consolidation in the same minute must not truncate the first one's file (2026-11-03 19:30); mkdir -p "$(dirname "$out")"; : > "$out"
for f in $(for f in $files; do echo "$(rank "$f") $f"; done | sort -n -k1,1 -s | awk '{print $2}' | awk '!seen[$0]++'); do cat "$f" >> "$out"; echo >> "$out"; done
awk 'NF && !seen[$1]++' "$out" > "$out.dedup" && mv "$out.dedup" "$out"
free=""; for k in 0 1 2 3 4 5 6 7; do case ",$RES," in *",$k,"*) continue;; esac; free="$free${free:+,}$k"; done
echo "stopped $stopped dispatchers; $(grep -c . "$out") rows in $out; numa $free"
FOREIGN_LOAD_POLICY=ignore NUMA_SET="$free" bash "$S/start-queue.sh" "$out" "all-$(date +%H%M%S)" 8 | tail -1
