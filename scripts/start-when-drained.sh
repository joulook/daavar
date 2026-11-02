#!/usr/bin/env bash
# Node-side chain (owner, 2026-11-02 15:00): wait until no row runs on this node, then run the given queues in
# order, each after the previous one finished and the node drained. Survives the coordinator's pause.
# usage: start-when-drained.sh <rows-file>:<queue-name>:<slots> [...]   (paths relative to the Daavar tree)
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; cd "$DAAVAR"
drained() { [ "$(grep -l '^state=running' "$EVAL_OUT"/../out-*/*/status 2>/dev/null | wc -l)" = 0 ]; }
log="$EVAL_OUT/start-when-drained.log"
for step in "$@"; do
  IFS=: read -r rows q slots <<< "$step"
  until drained; do sleep 60; done
  git pull -q 2>>"$log" || true
  echo "$(date -Is) starting $q ($rows, $slots slots)" >> "$log"
  bash "$S/start-queue.sh" "$rows" "$q" "$slots" >> "$log" 2>&1
  sleep 30
  until grep -q finished "$EVAL_OUT/queues/$q/queue.log" 2>/dev/null && drained; do sleep 60; done
  echo "$(date -Is) $q finished" >> "$log"
done
echo "$(date -Is) chain done" >> "$log"
