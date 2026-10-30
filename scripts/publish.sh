#!/usr/bin/env bash
# Copy one finished row into the brain-data clone, append its results.tsv line, commit and push (R2b).
# usage: publish.sh <rowdir>    env: PUBLISH_PUSH=1 (0 = commit only)
set -uo pipefail
. "$(dirname "$0")/env.sh"
d="$1"; id=$(basename "$d"); node=$(hostname); : "${PUBLISH_PUSH:=1}"
[ -d "$BRAIN_DATA/.git" ] || { echo "no brain-data clone at $BRAIN_DATA"; exit 1; }
dest="$BRAIN_DATA/runs/$node/$id"; mkdir -p "$dest"
# copy the row; in the copy, compress summaries above 1 MB (the originals stay in $EVAL_OUT: later join
# and online rows read them from there)
rsync -a --exclude 'done' "$d/" "$dest/"
find "$dest" -maxdepth 3 -name '*.jsonl' -size +1M -exec zstd -q -19 --rm {} \; 2>/dev/null
(
  flock -w 600 9 || exit 1
  cd "$BRAIN_DATA"
  if ! grep -q -P "^$(printf '%s' "$id" | sed 's/[.[\*^$]/\\&/g')\t" results.tsv 2>/dev/null; then
    line=$(python3 -I "$DAAVAR/scripts/mkrecord.py" "$d" 2>/dev/null) && printf '%s\n' "$line" >> results.tsv
  fi
  git add -A runs/"$node"/"$id" results.tsv >/dev/null 2>&1
  git commit -q -m "$node: $id ($(sed -n 's/^reason=//p' "$d/status" | head -1))" >/dev/null 2>&1 || true
  if [ "$PUBLISH_PUSH" = 1 ]; then
    for i in 1 2 3; do git push -q 2>/dev/null && break; git pull -q --rebase 2>/dev/null; sleep $((i*20)); done
  fi
) 9>"$BRAIN_DATA/.publish.lock"
echo "published $id $(date -Is)"
