#!/bin/bash
# Move this node's JMC to the pinned submodule commit (owner, 2026-11-04): only when no JMC JVM is running here, with the
# dispatcher stopped meanwhile (a JVM started during the rebuild would load half-built classes). Restarts the dispatcher.
set -u; . "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; RES="${1:-}"
cd "$DAAVAR" && git pull -q 2>&1 | tail -1; git submodule update --init -q jmc; want=$(git -C "$DAAVAR" ls-tree HEAD jmc | awk '{print $3}')
have=$(cat "$EVAL_OUT/jmc-pin" 2>/dev/null); [ "$have" = "$want" ] && { echo "$(hostname): JMC already at $want"; exit 0; }
if pgrep -f "org.mpi_sws.jmc.mp.launch.MpLauncher" >/dev/null; then echo "$(hostname): JMC runs active, not now"; exit 2; fi
for q in $EVAL_OUT/queues/*/queue.log; do p=$(head -1 $q | sed -n 's/.*pid=\([0-9]*\).*/\1/p'); [ -n "$p" ] && kill -0 $p 2>/dev/null && kill $p; done; sleep 2
if pgrep -f "org.mpi_sws.jmc.mp.launch.MpLauncher" >/dev/null; then echo "$(hostname): a JMC run started, aborting"; bash "$S/consolidate-queues.sh" "$RES" | tail -1; exit 2; fi
cd "$DAAVAR/jmc" && ./gradlew -q --no-daemon --offline :core:testClasses </dev/null || ./gradlew -q --no-daemon :core:testClasses </dev/null
./gradlew -q --no-daemon -I "$DAAVAR/scripts/print-classpath.gradle" :core:printTestClasspath </dev/null && cp cp.txt "$JMC_CP_FILE" && echo "$want" > "$EVAL_OUT/jmc-pin" && echo "$(hostname): JMC rebuilt at $want"
bash "$S/consolidate-queues.sh" "$RES" | tail -1
