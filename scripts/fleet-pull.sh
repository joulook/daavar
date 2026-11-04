#!/usr/bin/env bash
# Fleet audit of the daavar checkouts (owner, 2026-11-05): for every node, report the daavar commit and the two pins,
# pull origin/main when behind (the scripts change without a rebuild), and flag a pin change (a rebuild is needed:
# pin-jmc.sh / setup-node.sh when no row of that tool runs). usage: fleet-pull.sh [pull] node...
mode=$1; shift; S='ssh -o BatchMode=yes -o ForwardX11=no -o ConnectTimeout=30'
want=$(git -C /local/home/mkhoshechin/Documents/github-jmc/daavar rev-parse --short origin/main 2>/dev/null)
for n in "$@"; do
  r=$(timeout 120 $S $n "cd /local/mkhoshechin/eval/daavar 2>/dev/null || { echo no-checkout; exit 0; }; h=\$(git rev-parse --short HEAD); j=\$(git -C jmc rev-parse --short HEAD); t=\$(git -C traceforge rev-parse --short=7 HEAD); jb=\$(cat /local/mkhoshechin/eval/out-\$t/jmc-pin 2>/dev/null | sed 's/jmc=//'); echo \"head=\$h jmc=\$j tf=\$t built-jmc=\${jb:-?}\"; if [ '$mode' = pull ] && [ \"\$h\" != '$want' ]; then git pull -q --ff-only origin main 2>&1 | tail -1; git submodule update --init -q 2>&1 | tail -1; echo \"pulled -> \$(git rev-parse --short HEAD) jmc=\$(git -C jmc rev-parse --short HEAD) tf=\$(git -C traceforge rev-parse --short=7 HEAD)\"; fi" 2>&1 | grep -v X11 | tr '\n' ' ')
  echo "$n: $r"
done | sed "s/^/$(date '+%m-%d %H:%M') /"
