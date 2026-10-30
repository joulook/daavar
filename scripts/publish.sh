#!/usr/bin/env bash
# Copy one finished row into the brain-data clone and append its results line (R2b). The commit
# and push are done in batches by flush-results.sh. usage: publish.sh <rowdir>
set -uo pipefail
. "$(dirname "$0")/env.sh"
d="$1"; id=$(basename "$d"); node=$(hostname); : "${PUBLISH_PUSH:=1}"
pin=$(sed -n 's/^pin=//p' "$d/status" | head -1); pin=${pin:-a3a7e1b}   # the pin the row ran on (rows launched before the field existed: a3a7e1b)
TF_PIN=$pin
[ -d "$BRAIN_DATA/.git" ] || { echo "no brain-data clone at $BRAIN_DATA"; exit 1; }
dest="$BRAIN_DATA/runs/$node/$TF_PIN/$id"; mkdir -p "$dest"
# copy the row; in the copy, compress summaries above 1 MB (the originals stay in $EVAL_OUT: later join
# and online rows read them from there)
rsync -a --exclude 'done' "$d/" "$dest/"
find "$dest" -maxdepth 3 -name '*.jsonl' -size +1M -exec zstd -q -19 --rm {} \; 2>/dev/null
(
  flock -w 120 9 || { echo "publish: lock timeout for $id"; exit 1; }
  cd "$BRAIN_DATA"
  mkdir -p results; hdr=$(head -1 results.tsv)
  [ -f "results/$node.$TF_PIN.tsv" ] || printf '%s\n' "$hdr" > "results/$node.$TF_PIN.tsv"
  # the schema may gain columns (appended at the end): rewrite the node file's header, pad old rows
  if [ "$(head -1 "results/$node.$TF_PIN.tsv")" != "$hdr" ]; then
    python3 -I -c '
import sys; hdr=sys.argv[1].split("\t"); f=sys.argv[2]; lines=open(f).read().splitlines()
old=lines[0].split("\t"); pos={c:i for i,c in enumerate(old)}
out=["\t".join(hdr)]
for l in lines[1:]:
    cells=l.split("\t"); out.append("\t".join(cells[pos[c]] if c in pos and pos[c]<len(cells) else "" for c in hdr))
open(f,"w").write("\n".join(out)+"\n")' "$hdr" "results/$node.$TF_PIN.tsv"
  fi
  if ! grep -q -P "^$(printf '%s' "$id" | sed 's/[.[\*^$]/\\&/g')\t" "results/$node.$TF_PIN.tsv" 2>/dev/null; then
    line=$(python3 -I "$DAAVAR/scripts/mkrecord.py" "$d" 2>/dev/null) && printf '%s\n' "$line" >> "results/$node.$TF_PIN.tsv"
  fi
  # no git here: flush-results.sh commits and pushes every few minutes (one commit per batch)
) 9>"$BRAIN_DATA/.publish.lock"
echo "published $id $(date -Is)"
