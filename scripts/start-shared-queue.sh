#!/usr/bin/env bash
# Start (or restart) the queue of a shared node on its half of the machine (owner's rules, plan/EVAL-RULES-shared.md).
# Run ON the node: bash scripts/start-shared-queue.sh [rows-file]   (default cluster/campaign/shared/<hostname>.rows)
set -uo pipefail
. "$(dirname "$0")/env.sh"
h=$(hostname); rows=${1:-$DAAVAR/cluster/campaign/shared/$h.rows}; [ -f "$rows" ] || { echo "no rows file $rows"; exit 1; }
case $h in
  brain*) NUMA=0,1,2,3; SLOTS=4; CAP=250;;
  volta*) NUMA=0; SLOTS=1; CAP=200;;
  pinky*) NUMA=0; SLOTS=1; CAP=80;;
  *) echo "unknown host class $h"; exit 1;;
esac
# one dispatcher per node: stop a live one first (its running rows continue)
for q in /local/mkhoshechin/eval/out-*/queues/*/queue.log; do [ -f "$q" ] || continue; p=$(head -1 "$q" | grep -o "pid=[0-9]*" | cut -d= -f2); [ -n "$p" ] && kill -0 "$p" 2>/dev/null && kill "$p" 2>/dev/null; done
cd "$DAAVAR" && git pull -q --ff-only origin main 2>/dev/null
MEM_CAP=$CAP FOREIGN_LOAD_POLICY=ignore NUMA_SET=$NUMA bash "$DAAVAR/scripts/start-queue.sh" "$rows" "shared-$(date +%m%d-%H%M)" "$SLOTS" | tail -1
