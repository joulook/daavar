#!/usr/bin/env bash
# Compile a benchmark's Java sources against the private JMC copy and run one program under
# JMC's Must strategy (or mp-random), printing graph counts.
# usage: run-java.sh <benchdir> <Class[:arg,arg]> [must|mp-random] [iterations] [timeoutSeconds]
set -euo pipefail
B="$1"; CLS="$2"; STRAT="${3:-must}"; IT="${4:-100000}"; TO="${5:-600}"
J=/local/home/mkhoshechin/tmp/jmc-bench
CP="$(cat $J/cp.txt)"
OUT="$B/java/out"; mkdir -p "$OUT"
javac -d "$OUT" -cp "$CP" $(find "$B/java" -name '*.java' -not -path '*/out/*') 
REPORT=$(mktemp -d /local/home/mkhoshechin/tmp/mp-launch.XXXXXX)
java -cp "$OUT:$CP" org.mpi_sws.jmc.mp.launch.MpLauncher "$CLS" "$STRAT" "$IT" 1 "$REPORT" - "$TO"
