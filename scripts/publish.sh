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
  flock -w 120 9 || { echo "publish: lock timeout for $id"; exit 1; }
  cd "$BRAIN_DATA"
  # never leave a rebase behind; always be on main
  [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ] && git rebase --abort >/dev/null 2>&1
  git symbolic-ref -q HEAD >/dev/null || git checkout -q -B main
  # one results file per node (results/<node>.tsv) so nodes never edit the same file
  mkdir -p results; hdr=$(head -1 results.tsv)
  [ -f "results/$node.tsv" ] || printf '%s\n' "$hdr" > "results/$node.tsv"
  if ! grep -q -P "^$(printf '%s' "$id" | sed 's/[.[\*^$]/\\&/g')\t" "results/$node.tsv" 2>/dev/null; then
    line=$(python3 -I "$DAAVAR/scripts/mkrecord.py" "$d" 2>/dev/null) && printf '%s\n' "$line" >> "results/$node.tsv"
  fi
  git add -A runs/"$node"/"$id" "results/$node.tsv" >/dev/null 2>&1
  git commit -q -m "$node: $id ($(sed -n 's/^reason=//p' "$d/status" | head -1))" >/dev/null 2>&1 || true
  if [ "$PUBLISH_PUSH" = 1 ]; then
    for i in 1 2 3; do
      git push -q origin main 2>/dev/null && break
      git fetch -q origin && { git rebase -q origin/main >/dev/null 2>&1 || git rebase --abort >/dev/null 2>&1; }
      sleep $((i*15))
    done
  fi
) 9>"$BRAIN_DATA/.publish.lock"
echo "published $id $(date -Is)"
