#!/usr/bin/env bash
# Re-run the record builder and the publisher on every finished row of an output tree (after a
# parser change). usage: rerecord.sh <outdir> [id-prefix]
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; O="$1"; P="${2:-}"; n=0
for d in "$O"/${P}*/; do [ -e "$d/done" ] || continue; python3 -I "$S/mkrecord.py" "$d" >/dev/null 2>>"$d/mkrecord.err" && "$S/publish.sh" "$d" >/dev/null 2>&1 && n=$((n+1)); done
echo "$(hostname): $n rows re-recorded"
