#!/usr/bin/env bash
# Start the node guard detached (once per node). usage: start-node-guard.sh [intervalS]
set -euo pipefail
. "$(dirname "$0")/env.sh"
S="$(cd "$(dirname "$0")" && pwd)"
EVAL=$(dirname "$EVAL_OUT"); PF="$EVAL/.guard.pid"
if [ -f "$PF" ] && kill -0 "$(cat "$PF")" 2>/dev/null; then echo "node guard already running (pid $(cat "$PF"))"; exit 0; fi
if pgrep -f "scripts/node-guard[.]sh" >/dev/null; then echo "node guard already running (pid $(pgrep -f 'scripts/node-guard[.]sh' | head -1))"; exit 0; fi
rm -f "$EVAL/.stop-guard"
setsid nohup "$S/node-guard.sh" "${1:-30}" </dev/null >"$EVAL/guard.nohup.log" 2>&1 &
echo $! > "$PF"; echo "node guard started on $(hostname), pid $(cat "$PF"), every ${1:-30} s"
