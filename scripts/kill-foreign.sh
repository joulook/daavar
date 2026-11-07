#!/usr/bin/env bash
# Kill every process of other (non-system) users on the reserved nodes brain01..brain05 (rule R7: the nodes are
# ours during the reservation). Only real user accounts are touched (uid >= 1000), never system accounts, never
# mkhoshechin. Needs root on the nodes: the ssh target is root@<node> unless SSH_USER says otherwise.
#
# usage: kill-foreign.sh            list the foreign processes per node (dry run, nothing killed)
#        kill-foreign.sh --kill     list, then kill them (SIGTERM, then SIGKILL after 5 s for survivors)
#        NODES="brain01 brain03" kill-foreign.sh --kill     only these nodes
set -u
OURS="${OURS:-mkhoshechin}"
NODES="${NODES:-brain01 brain02 brain03 brain04 brain05}"
SSH_USER="${SSH_USER:-root}"
SSH_OPTS="${SSH_OPTS:-}"          # e.g. SSH_OPTS="-i ~/.ssh/brain-root" if the root key is not the default one
MODE="${1:-list}"
total=0
for n in $NODES; do
  out=$(ssh $SSH_OPTS -o ConnectTimeout=10 -o BatchMode=yes "$SSH_USER@$n" \
    "echo CONNECTED; ps -eo user:20,uid,pid,etime,args --no-headers | awk -v ours='$OURS' '\$2 >= 1000 && \$1 != ours && \$1 != \"nobody\"'" 2>&1)
  if ! printf '%s\n' "$out" | grep -qx CONNECTED; then echo "== $n: CANNOT CONNECT as $SSH_USER@$n: $(printf '%s' "$out" | tail -1)"; continue; fi
  list=$(printf '%s\n' "$out" | grep -vx CONNECTED)
  if [ -z "$list" ]; then echo "== $n: no foreign processes"; continue; fi
  c=$(printf '%s\n' "$list" | wc -l); total=$((total + c))
  echo "== $n: $c foreign process(es) of $(printf '%s\n' "$list" | awk '{print $1}' | sort -u | tr '\n' ' ')"
  printf '%s\n' "$list" | awk '{printf "   %-12s pid %-8s up %-12s %s\n", $1, $3, $4, substr($0, index($0,$5), 90)}'
  if [ "$MODE" = "--kill" ]; then
    pids=$(printf '%s\n' "$list" | awk '{print $3}' | tr '\n' ' ')
    ssh $SSH_OPTS -o BatchMode=yes "$SSH_USER@$n" "kill $pids 2>/dev/null; sleep 5; left=''; for p in $pids; do kill -0 \$p 2>/dev/null && left=\"\$left \$p\"; done; [ -n \"\$left\" ] && kill -KILL \$left 2>/dev/null; sleep 1; echo \"   killed; still alive: \$(for p in $pids; do kill -0 \$p 2>/dev/null && echo \$p; done | wc -l)\""
  fi
done
[ "$MODE" = "--kill" ] || echo "(dry run: $total foreign process(es); rerun with --kill to terminate them)"
