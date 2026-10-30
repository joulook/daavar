#!/usr/bin/env bash
# Start a queue detached from the ssh session (R3): survives VPN/ssh drops.
# usage: start-queue.sh <rows-file> <queue-name> [slots=8]
set -euo pipefail
. "$(dirname "$0")/env.sh"
S="$(cd "$(dirname "$0")" && pwd)"
ROWS="$(readlink -f "$1")"; QNAME="$2"; SLOTS="${3:-8}"
QDIR="$EVAL_OUT/queues/$QNAME"; mkdir -p "$QDIR"
if [ -f "$QDIR/pid" ] && kill -0 "$(cat "$QDIR/pid")" 2>/dev/null; then echo "queue $QNAME already running (pid $(cat "$QDIR/pid"))"; exit 1; fi
setsid nohup "$S/run-queue.sh" "$ROWS" "$QNAME" "$SLOTS" </dev/null >"$QDIR/nohup.log" 2>&1 &
echo $! > "$QDIR/pid"
echo "queue $QNAME started on $(hostname), pid $(cat "$QDIR/pid"), log $QDIR/queue.log"
