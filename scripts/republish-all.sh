#!/usr/bin/env bash
# Rebuild this node's part of the brain-data clone from its finished rows (after a publish-policy
# change): resets the clone to origin/main, drops runs/<node>, republishes every finished row of every
# output tree, commits once. The flusher pushes. usage: republish-all.sh
set -uo pipefail
. "$(dirname "$0")/env.sh"
S="$(cd "$(dirname "$0")" && pwd)"; node=$(hostname)
cd "$BRAIN_DATA" || exit 1
( flock -w 600 9 || exit 1
  git rebase --abort >/dev/null 2>&1; git symbolic-ref -q HEAD >/dev/null || git checkout -q -B main
  git fetch -q origin; git reset -q --hard origin/main
  rm -rf "runs/$node"; rm -f results/"$node".*.tsv
) 9>"$BRAIN_DATA/.publish.lock"
n=0
for d in /local/mkhoshechin/eval/out*/*/; do
  [ -e "$d/done" ] || continue
  PUBLISH_PUSH=0 "$S/publish.sh" "$d" >/dev/null 2>&1 && n=$((n+1))
done
( flock -w 600 9 || exit 1
  git add -A "runs/$node" results/ >/dev/null 2>&1
  git commit -q -m "$node: republished $n rows under the bulk-log policy" >/dev/null 2>&1 || true
  echo "$node: $n rows republished; $(git log origin/main..HEAD --oneline | wc -l) commit(s) to push; size $(du -sh runs/$node | cut -f1)"
) 9>"$BRAIN_DATA/.publish.lock"
