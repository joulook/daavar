#!/usr/bin/env bash
# Run JMC's launcher on one benchmark program, in the current (row) directory: fresh report dir,
# classpath from setup-node.sh, stdin closed. (EVAL-J1-J2.md)
# usage: jmc-run.sh <benchdir-name> <Class[:a,b]> <strategy> <iterations> <seed> <export|-> <timeoutS> [--spec <file>]
set -uo pipefail
. "$(dirname "$0")/env.sh"
B="$1"; CLS="$2"; STRAT="$3"; IT="$4"; SEED="$5"; EXP="$6"; TO="$7"; shift 7
CP="$(cat "$JMC_CP_FILE")"; OUT="$EVAL_OUT/javaout/$B"
REPORT="$PWD/report"; rm -rf "$REPORT"; mkdir -p "$REPORT"
[ "$EXP" != "-" ] && EXP="$PWD/$EXP"
exec java -Xss4m -cp "$OUT:$CP" org.mpi_sws.jmc.mp.launch.MpLauncher "$CLS" "$STRAT" "$IT" "$SEED" "$REPORT" "$EXP" "$TO" "$@" </dev/null
