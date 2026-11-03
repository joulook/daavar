#!/usr/bin/env bash
# NUMA nodes of this machine held by no live queue dispatcher (one queue per NUMA node: two queues on one node contend
# and inflate every measurement on it; owner's timing fidelity, 2026-11-03). usage: numa-free.sh [reserved nodes, comma list]
. "$(dirname "$0")/env.sh"
held=""
for q in /local/mkhoshechin/eval/out-*/queues/*/; do
  [ -f "$q/queue.log" ] || continue
  pid=$(head -1 "$q/queue.log" | sed -n 's/.*pid=\([0-9]*\).*/\1/p'); [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || continue
  held="$held,$(head -1 "$q/queue.log" | sed -n 's/.*numa=\([0-9,]*\).*/\1/p')"
done
res=",${1:-},"; free=""
for k in 0 1 2 3 4 5 6 7; do case ",$held," in *",$k,"*) continue;; esac; case "$res" in *",$k,"*) continue;; esac; free="$free${free:+,}$k"; done
echo "$free"
