#!/usr/bin/env bash
# One-off repair after the exit-1 rule: rows of conformance commands recorded as exit-1 become ok and
# are re-recorded and re-published; rows skipped because of them are removed so the queue reruns them.
# usage: fix-exit1.sh <outdir>
. "$(dirname "$0")/env.sh"; S="$(cd "$(dirname "$0")" && pwd)"; O="$1"; n=0; k=0
for s in "$O"/*/status; do
  d=$(dirname "$s")
  if grep -q '^reason=exit-1' "$s" && grep -qE ' conf | online |--spec|conf-join' "$d/cmd.txt" 2>/dev/null; then
    sed -i 's/^reason=exit-1/reason=ok/' "$s"; python3 -I "$S/mkrecord.py" "$d" >/dev/null 2>&1; "$S/publish.sh" "$d" >/dev/null 2>&1; n=$((n+1))
  elif grep -q '^reason=skipped' "$s"; then rm -rf "$d"; k=$((k+1)); fi
done
echo "$(hostname): $n rows re-recorded as ok, $k skipped rows removed"
