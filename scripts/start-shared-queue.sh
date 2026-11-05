#!/usr/bin/env bash
# Start (or restart) the queue of a shared node on its half of the machine (owner's rules, plan/EVAL-RULES-shared.md).
# Run ON the node: bash scripts/start-shared-queue.sh [rows-file]   (default cluster/campaign/shared/<hostname>.rows)
set -uo pipefail
. "$(dirname "$0")/env.sh"
h=$(hostname); rows=${1:-$DAAVAR/cluster/campaign/shared/$h.rows}; [ -f "$rows" ] || { echo "no rows file $rows"; exit 1; }
case $h in
  brain*) NUMA=0,1,2,3; SLOTS=4; CAP=250; JTO="-Xmx12g";;   # 17 JVMs x 12 GB = 204 GB per JMC export, four at a time under 2 TB (brain01 thrashed uncapped, 2026-11-05)
  volta*) NUMA=0; SLOTS=1; CAP=200; JTO="-Xmx24g";;   # a JMC export runs 17 JVMs; the default heap (1/4 of RAM each) would let one row take the whole machine
  pinky*) NUMA=0; SLOTS=1; CAP=80;  JTO="-Xmx8g";;    # pinky: 251 GB, 17 x 8 GB stays under half of it
  *) echo "unknown host class $h"; exit 1;;
esac
# one dispatcher per node: stop a live one first (its running rows continue)
for q in /local/mkhoshechin/eval/out-*/queues/*/queue.log; do [ -f "$q" ] || continue; p=$(head -1 "$q" | grep -o "pid=[0-9]*" | cut -d= -f2); [ -n "$p" ] && kill -0 "$p" 2>/dev/null && kill "$p" 2>/dev/null; done
cd "$DAAVAR" && git pull -q --ff-only origin main 2>/dev/null
JAVA_TOOL_OPTIONS="$JTO" MEM_CAP=$CAP FOREIGN_LOAD_POLICY=ignore NUMA_SET=$NUMA bash "$DAAVAR/scripts/start-queue.sh" "$rows" "shared-$(date +%m%d-%H%M)" "$SLOTS" | tail -1
