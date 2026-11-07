#!/usr/bin/env bash
# The single writer of the results repository (owner, 2026-11-02): the nodes only commit into their own clones
# (flush-results.sh with .no-push present); this script, run on the coordinator's workstation, fetches every node's
# main, merges them (each node touches only runs/<node>/ and results/<node>.*.tsv, so merges never conflict), pushes
# once, and then brings every node's clone up to date. usage: collect-results.sh <brain-data-clone> [intervalS=1800] [nodes...]
set -uo pipefail
BD="$1"; IV="${2:-1800}"; shift 2 2>/dev/null; NODES=("${@:-brain01 brain02 brain03 brain04 brain05}"); [ ${#NODES[@]} -eq 1 ] && NODES=($NODES)
S='ssh -o BatchMode=yes -o ForwardX11=no -o ConnectTimeout=60'
cd "$BD" || exit 1
while true; do
  git pull -q --rebase origin main 2>/dev/null
  for n in "${NODES[@]}"; do
    if timeout 900 git fetch -q "ssh://$n/local/mkhoshechin/eval/brain-data" main 2>/dev/null; then
      # guard (2026-11-05): a node commit may only add or change files under runs/<node>/ and results/<node>.*; a commit that
      # deletes anything else (a broken clone) is never merged (it would delete the repository's content)
      bad=$(git diff --name-only --diff-filter=D "$(git merge-base HEAD FETCH_HEAD)" FETCH_HEAD 2>/dev/null | grep -v "^runs/$n/\|^results/$n\." | head -3)
      if [ -n "$bad" ]; then echo "$(date '+%m-%d %H:%M') $n: REFUSED (deletes outside its own paths: $(echo $bad | cut -c1-120))"; continue; fi
      big=$(git rev-list --objects "$(git merge-base HEAD FETCH_HEAD)..FETCH_HEAD" 2>/dev/null | git cat-file --batch-check='%(objecttype) %(objectsize) %(rest)' | awk '$1=="blob" && $2>94371840 {print $3}' | head -2)
      if [ -n "$big" ]; then echo "$(date '+%m-%d %H:%M') $n: NOT merged, a file above GitHub's 100 MB limit: $big"; continue; fi   # 2026-11-07: a 239 GB headtail from brain02 blocked every push for hours
      git merge -q --no-edit -X theirs FETCH_HEAD >/dev/null 2>&1 && echo "$(date '+%m-%d %H:%M') $n: merged $(git log --oneline -1 FETCH_HEAD | cut -c1-60)" || { git merge --abort 2>/dev/null; echo "$(date '+%m-%d %H:%M') $n: merge failed"; }
    else echo "$(date '+%m-%d %H:%M') $n: fetch failed (node not reachable)"; fi
  done
  for i in 1 2 3; do timeout 1200 git push -q origin main 2>/dev/null && break; git pull -q --rebase origin main 2>/dev/null; done
  echo "$(date '+%m-%d %H:%M') pushed: $(git log --oneline -1 | cut -c1-60)"
  # 2026-11-07: the node-side `git pull --rebase` was removed. Nodes publish under .no-push and only ever add their own files; the
  # pull brought them nothing they need, took minutes (an hour on brain05 under load) while holding the node's publish lock, and
  # every publish that timed out on that lock lost its record line. The collector merges each node's clone as it is.
  [ "$IV" = 0 ] && break   # one pass (the 30-min pipeline of 2026-11-05 runs the collector as its first step)
  sleep "$IV"
done
