#!/bin/bash
# Owner's rule (2026-11-03 21:50): before queuing a requested row, make sure no equivalent row is queued or running on any
# node. This prints every live queue's pending rows and every running row of every node as "node queue state id base",
# where base is the id without its tier/rerun suffixes (.t16 .c48h .c1h .j2 .m .r2 .p .xN .fN). usage:
#   cluster-pending.sh [pattern]   # pattern filters the base (grep -E); no pattern lists everything
S='ssh -o BatchMode=yes -o ForwardX11=no -o ConnectTimeout=30'; pat="${1:-.}"
for n in brain01 brain02 brain03 brain04 brain05; do
  timeout 120 $S $n 'cd /local/mkhoshechin/eval/daavar && . scripts/env.sh
    for q in $EVAL_OUT/queues/*/queue.log; do
      p=$(head -1 $q | sed -n "s/.*pid=\([0-9]*\).*/\1/p"); [ -n "$p" ] && kill -0 $p 2>/dev/null || continue
      f=$(head -1 $q | sed -n "s/.*rows=\([^ ]*\).*/\1/p"); qn=$(basename $(dirname $q))
      while read -r id rest; do case "$id" in ""|\#*) continue;; esac; [ -e $EVAL_OUT/$id/done ] && continue
        if grep -q "^state=running" $EVAL_OUT/$id/status 2>/dev/null && kill -0 $(sed -n "s/^pid=//p" $EVAL_OUT/$id/status | head -1) 2>/dev/null; then continue; fi
        echo "'$n' $qn pending $id"; done < $f
    done
    for s in /local/mkhoshechin/eval/out-9a0629e/*/status; do
      grep -q "^state=running" $s 2>/dev/null && kill -0 $(sed -n "s/^pid=//p" $s | head -1) 2>/dev/null && echo "'$n' $(sed -n "s/^queue=//p" $s | head -1) running $(basename $(dirname $s))"
    done' 2>/dev/null
done | awk '{b=$4; gsub(/\.(t16|c48h|c1h|j2|m|r2|p|x[0-9]+|f[0-9]+|w[0-9]+)/,"",b); print $1, $2, $3, $4, b}' | grep -E -- "$pat" | sort -k5,5 -k3,3
