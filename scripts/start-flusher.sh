#!/usr/bin/env bash
# Start the results flusher detached (once per node). usage: start-flusher.sh [intervalS]
set -euo pipefail
. "$(dirname "$0")/env.sh"
S="$(cd "$(dirname "$0")" && pwd)"
PF="$EVAL_OUT/flusher.pid"
if [ -f "$PF" ] && kill -0 "$(cat "$PF")" 2>/dev/null; then echo "flusher already running (pid $(cat "$PF"))"; exit 0; fi
rm -f "$BRAIN_DATA/.stop-flusher"
setsid nohup "$S/flush-results.sh" "${1:-600}" </dev/null >"$EVAL_OUT/flusher.log" 2>&1 &
echo $! > "$PF"; echo "flusher started on $(hostname), pid $(cat "$PF"), every ${1:-600} s"
