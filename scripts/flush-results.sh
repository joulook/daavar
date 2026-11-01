#!/usr/bin/env bash
# Commit and push the brain-data clone in batches (R2b): every INTERVAL seconds (default 600), one
# commit "node: N rows" if anything changed, push with fetch+rebase retries, never leaving a rebase
# behind. Start it detached once per node with start-flusher.sh; it exits when $BRAIN_DATA/.stop-flusher exists.
# usage: flush-results.sh [intervalS]
set -uo pipefail
. "$(dirname "$0")/env.sh"
INTERVAL="${1:-600}"; node=$(hostname)
cd "$BRAIN_DATA" || exit 1
while [ ! -e .stop-flusher ]; do
  (
    flock -w 120 9 || exit 0
    [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ] && git rebase --abort >/dev/null 2>&1
    git symbolic-ref -q HEAD >/dev/null || git checkout -q -B main
    if [ -n "$(git status --porcelain -- runs/"$node" results/"$node".*tsv 2>/dev/null)" ]; then
      n=$(git status --porcelain -- runs/"$node" | grep -c '^??\|^ M\|^A ' || true)
      git add -A runs/"$node" results/"$node".*tsv >/dev/null 2>&1
      git commit -q -m "$node: $n row dir(s) updated, $(date -Is)" >/dev/null 2>&1 || true
    fi
    if [ -n "$(git log origin/main..HEAD --oneline 2>/dev/null)" ]; then
      for i in 1 2 3; do
        git push -q origin main 2>/dev/null && break
        git fetch -q origin && { git rebase -q --autostash origin/main >/dev/null 2>&1 || git rebase --abort >/dev/null 2>&1; }
        sleep $((i*15))
      done
    fi
  ) 9>"$BRAIN_DATA/.publish.lock"
  sleep "$INTERVAL"
done
