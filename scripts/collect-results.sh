#!/usr/bin/env bash
# The single writer of the results repository (owner, 2026-11-02): the nodes only commit into their own clones
# (flush-results.sh with .no-push present); this script, run on the coordinator's workstation, fetches every node's
# main, merges them (each node touches only runs/<node>/ and results/<node>.*.tsv, so merges never conflict), pushes
# once, and then brings every node's clone up to date. usage: collect-results.sh <brain-data-clone> [intervalS=1800] [nodes...]
set -uo pipefail
BD="$1"; IV="${2:-1800}"; shift 2 2>/dev/null; NODES=("${@:-brain01 brain02 brain03 brain04 brain05}"); [ ${#NODES[@]} -eq 1 ] && NODES=($NODES)
S='ssh -o BatchMode=yes -o ForwardX11=no -o ConnectTimeout=60'
cd "$BD" || exit 1
while true; do
  git pull -q --rebase origin main 2>/dev/null
  for n in "${NODES[@]}"; do
    if timeout 900 git fetch -q "ssh://$n/local/mkhoshechin/eval/brain-data" main 2>/dev/null; then
      git merge -q --no-edit -X theirs FETCH_HEAD >/dev/null 2>&1 && echo "$(date '+%m-%d %H:%M') $n: merged $(git log --oneline -1 FETCH_HEAD | cut -c1-60)" || { git merge --abort 2>/dev/null; echo "$(date '+%m-%d %H:%M') $n: merge failed"; }
    else echo "$(date '+%m-%d %H:%M') $n: fetch failed (node not reachable)"; fi
  done
  for i in 1 2 3; do timeout 1200 git push -q origin main 2>/dev/null && break; git pull -q --rebase origin main 2>/dev/null; done
  echo "$(date '+%m-%d %H:%M') pushed: $(git log --oneline -1 | cut -c1-60)"
  for n in "${NODES[@]}"; do timeout 300 $S $n 'cd /local/mkhoshechin/eval/brain-data && flock -w 120 .publish.lock git pull -q --rebase --autostash origin main 2>/dev/null' </dev/null >/dev/null 2>&1 & done; wait
  sleep "$IV"
done
