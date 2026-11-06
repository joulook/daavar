#!/usr/bin/env bash
# One-node queue runner (EVAL-RULES R2, R3, R7). Rows come from a text file, one per line:
#   <row-id> <memGB> <timeoutS> <command ...>      (# starts a comment)
# Each row runs in its own directory $EVAL_OUT/<row-id>/ (cwd of the command; JMC's launcher
# gets a fresh report dir there), pinned with taskset to W NUMA nodes of 16 physical cores
# (W = 1, or 2 for ids containing -t32, 4 for -t64), under ulimit -v, timeout --signal=KILL and
# /usr/bin/time -v, stdin </dev/null, stdout/stderr to files, a `status` file (R3), a load log
# sampled every 60 s (R7), then mkrecord.py writes record.json + the results.tsv row and
# publish.sh pushes the row to brain-data (R2b). Rows with a `done` marker are skipped
# (restartable). Start it detached with start-queue.sh, never in a foreground ssh session.
# usage: run-queue.sh <rows-file> <queue-name> [slots=8]
# env: ROWS_TSV (attributes sidecar, default <rows-file>.tsv), FOREIGN_LOAD_POLICY=wait|kill|ignore, MEM_CAP (GB, shared nodes)
#      (default wait: do not launch while foreign load is present; kill: also kill a running row
#      and mark it invalid), FOREIGN_LOAD_MARGIN (load-average units, default 8), PUBLISH=1.
set -uo pipefail
. "$(dirname "$0")/env.sh"
S="$(cd "$(dirname "$0")" && pwd)"
ROWS="$1"; QNAME="$2"; SLOTS="${3:-8}"
: "${NUMA_SET:=$(seq -s, 0 $((SLOTS-1)))}"   # NUMA nodes this queue may use, e.g. "3,4,5,6,7" when another queue holds 0-2
IFS=, read -r -a ALLOWED <<< "$NUMA_SET"
: "${ROWS_TSV:=$ROWS.tsv}"; [ -f "$ROWS_TSV" ] || ROWS_TSV="$(dirname "$ROWS")/rows.tsv"; : "${PIN:=1}"; : "${FOREIGN_LOAD_POLICY:=wait}"; : "${FOREIGN_LOAD_MARGIN:=8}"; : "${PUBLISH:=1}"
export ROWS_TSV S
QDIR="$EVAL_OUT/queues/$QNAME"; mkdir -p "$QDIR"
echo "queue $QNAME start $(date -Is) rows=$ROWS slots=$SLOTS numa=$NUMA_SET pin=$TF_PIN host=$(hostname) pid=$$" >> "$QDIR/queue.log"

cores_of() {  # $1: comma list of numa nodes -> the cpus of those nodes from lscpu (brain: 16k..16k+15; pinky: the even or odd cpus; volta: 0-7,16-23 / 8-15,24-31)
  local out="" n c; for n in ${1//,/ }; do c=$(lscpu -p=cpu,node | awk -F, -v n="$n" '!/^#/ && $2==n {print $1}' | paste -sd,); [ -n "$c" ] || c="$((16*n))-$((16*n+15))"; out+="${out:+,}$c"; done; echo "$out"; }
width_of() { case "$1" in *-t64*) echo 4;; *-t32*) echo 2;; *) echo 1;; esac; }
busy_threads() {  # physical cores held by every running row on this node, across all output trees and queues
  local t=0 s nw w
  for s in /local/mkhoshechin/eval/out*/*/status "$EVAL_OUT"/*/status; do
    [ -f "$s" ] && grep -q '^state=running' "$s" 2>/dev/null || continue
    nw=$(sed -n 's/^numa=//p' "$s" | head -1); w=${nw#*+}; t=$((t + 16*${w:-1}))
  done; echo $t; }
foreign_load() {  # 1 if the node's 1-min load exceeds our own busy cores by the margin
  local la; la=$(cut -d' ' -f1 /proc/loadavg); awk -v la="$la" -v ours="$(busy_threads)" -v m="$FOREIGN_LOAD_MARGIN" 'BEGIN{exit !(la > ours+m)}'
}
tree_rss_kb() {  # resident memory (kB) of a process and all its descendants, summed
  ps -e -o pid=,ppid=,rss= | awk -v root="$1" '{pp[$1]=$2; rss[$1]=$3} END {for (p in pp) {q=p; while (q && q!=root && (q in pp)) q=pp[q]; if (q==root) s+=rss[p]} print s+0}'
}
sampler() {  # $1 rowdir $2 pid-of-row: every 10 s append the process tree's resident memory (mem.log: epoch kB), every
  # 60 s the loadavg and our busy count (load.log); kill on foreign load if asked. The memory samples give a timed-out
  # or killed row its peak (a lower bound at 10 s granularity), which /usr/bin/time cannot report for a killed child.
  local d="$1" rp="$2" strikes=0 i=0
  while kill -0 "$rp" 2>/dev/null; do
    sleep 10; kill -0 "$rp" 2>/dev/null || break
    echo "$(date +%s) $(tree_rss_kb "$rp")" >> "$d/mem.log"
    i=$((i+1)); [ $((i % 6)) -eq 0 ] || continue
    echo "$(date +%s) $(cut -d' ' -f1-3 /proc/loadavg) ours=$(busy_threads)" >> "$d/load.log"
    if foreign_load; then strikes=$((strikes+1)); else strikes=0; fi
    if [ "$FOREIGN_LOAD_POLICY" = kill ] && [ $strikes -ge 3 ]; then echo "foreign-load" > "$d/killed"; pkill -KILL -P "$rp" 2>/dev/null; kill -KILL "$rp" 2>/dev/null; break; fi
  done
}
run_row() {  # id memGB timeoutS numa-list width cmd...
  local id="$1" mem="$2" to="$3" first="$4" w="$5"; shift 5
  [ -n "${MEM_CAP:-}" ] && [ "$mem" -gt "$MEM_CAP" ] && mem=$MEM_CAP   # shared nodes (2026-11-05): never more than MEM_CAP GB per row
  local d="$EVAL_OUT/$id"; mkdir -p "$d"
  local cores; cores=$(cores_of "$first")
  printf '%s\n' "$*" > "$d/cmd.txt"
  local start; start=$(date +%s)
  { echo "state=running"; echo "id=$id"; echo "host=$(hostname)"; echo "queue=$QNAME"; echo "start=$(date -Is -d @$start)"; echo "start_epoch=$start"
    echo "numa=$first+$w"; echo "pin=$TF_PIN"; echo "cores=$cores"; echo "mem_gb=$mem"; echo "timeout_s=$to"; echo "load_before=$(cut -d' ' -f1 /proc/loadavg)"; } > "$d/status"
  local ts=(taskset -c "$cores"); [ "$PIN" = 1 ] || ts=()
  ( ulimit -v $((mem*1024*1024)); cd "$d" && exec /usr/bin/time -v -o "$d/time-v.txt" \
      "${ts[@]}" timeout --signal=KILL "$to" "$@" </dev/null >"$d/stdout.log" 2>"$d/stderr.log" ) &
  local rp=$!; echo "pid=$rp" >> "$d/status"
  sampler "$d" "$rp" & local sp=$!
  wait "$rp"; local st=$?; kill "$sp" 2>/dev/null; wait "$sp" 2>/dev/null
  local reason=ok
  # the conformance tools exit 1 to report a violation (conf, conf-join, online, --spec): that is a result, not a failure
  if [ $st -eq 1 ]; then case "$*" in *" conf "*|*conf-join*|*" online "*|*--spec*) reason=ok;; *) reason="exit-1";; esac; fi
  if [ $st -eq 137 ]; then   # SIGKILL: the row's own `timeout` (a timeout) or a kill from outside (not a measurement; 2026-11-03: 19 killed rows were published as 48 h timeouts)
    if [ $(( $(date +%s) - start )) -ge $((to - 5)) ]; then reason=timeout; else reason="invalid: killed after $(( $(date +%s) - start )) s of $to"; fi
  fi
  [ -f "$d/killed" ] && reason="invalid: foreign load"
  { [ $st -ne 0 ] && [ $st -ne 1 ] && [ "$reason" = ok ]; } && reason="exit-$st"
  grep -qi "out of memory\|memory allocation of\|Cannot allocate\|OutOfMemoryError" "$d/stderr.log" "$d/stdout.log" 2>/dev/null && reason=memory
  { echo "state=done"; echo "end=$(date -Is)"; echo "exit=$st"; echo "reason=$reason"; echo "wall_s=$(( $(date +%s) - start ))"; } >> "$d/status"
  sed -i 's/^state=running/state=finished/' "$d/status"
  python3 -I "$S/mkrecord.py" "$d" >> "$EVAL_OUT/results.tsv" 2>>"$d/mkrecord.err" || echo "mkrecord failed for $id" >> "$QDIR/queue.log"
  touch "$d/done"
  [ "$PUBLISH" = 1 ] && "$S/publish.sh" "$d" >> "$QDIR/publish.log" 2>&1
  echo "row $id done exit=$st reason=$reason $(date -Is)" >> "$QDIR/queue.log"
}

declare -A BUSY=()   # numa-node -> pid
declare -A ROWPID=() # pid -> "first width"
free_finished() { local n p; for p in "${!ROWPID[@]}"; do if ! kill -0 "$p" 2>/dev/null; then wait "$p" 2>/dev/null; for n in ${ROWPID[$p]//,/ }; do unset "BUSY[$n]"; done; unset "ROWPID[$p]"; fi; done; }
allowed() { local k; for k in "${ALLOWED[@]}"; do [ "$k" = "$1" ] && return 0; done; return 1; }
ext_busy() {  # $1 numa node -> 0 if a row of another queue runs on it (state=running, live pid, numa_node "k+…"; 2026-11-03: one row per node across queues)
  local k=$1 s p
  for s in $(grep -l "^numa=$k+" "$EVAL_OUT"/*/status 2>/dev/null); do
    grep -q '^state=running' "$s" || continue; p=$(sed -n 's/^pid=//p' "$s" | head -1); [ -n "$p" ] && kill -0 "$p" 2>/dev/null && return 0
  done; return 1
}
find_free() {  # $1 width -> a comma list of width free allowed numa nodes (not necessarily consecutive), or empty
  # over the ALLOWED nodes, not 0..SLOTS-1 (2026-11-06: brain04's one-slot queue on node 4 never launched, because the scan stopped at node 0)
  local w=$1 k out="" c=0; for k in "${ALLOWED[@]}"; do { [ -z "${BUSY[$k]:-}" ] && ! ext_busy $k; } && { out="$out${out:+,}$k"; c=$((c+1)); [ $c -eq $w ] && { echo "$out"; return; }; }; done
}
# dispatch: rows are launched in file order as slots free up; a row whose needok dependency has not
# finished is deferred (not blocking the rows behind it) and re-checked on every pass
mapfile -t PENDING < <(grep -v '^[[:space:]]*#' "$ROWS" | grep -v '^[[:space:]]*$')
skip_row() {  # id mem to cmd dep
  local id="$1" mem="$2" to="$3" cmd="$4" dep="$5" d="$EVAL_OUT/$1"; mkdir -p "$d"; printf '%s\n' "$cmd" > "$d/cmd.txt"
  { echo "state=done"; echo "id=$id"; echo "host=$(hostname)"; echo "queue=$QNAME"; echo "pin=$TF_PIN"; echo "start=$(date -Is)"; echo "end=$(date -Is)"; echo "exit=0"; echo "reason=skipped: $dep $(sed -n 's/^reason=//p' "$EVAL_OUT/$dep/status" | head -1)"; echo "mem_gb=$mem"; echo "timeout_s=$to"; } > "$d/status"
  : > "$d/stdout.log"; : > "$d/stderr.log"
  python3 -I "$S/mkrecord.py" "$d" >> "$EVAL_OUT/results.tsv" 2>>"$d/mkrecord.err"; touch "$d/done"
  [ "$PUBLISH" = 1 ] && "$S/publish.sh" "$d" >> "$QDIR/publish.log" 2>&1
  echo "row $id skipped ($dep not ok) $(date -Is)" >> "$QDIR/queue.log"
}
while [ ${#PENDING[@]} -gt 0 ]; do
  free_finished
  NEXT=(); launched_any=0
  for line in "${PENDING[@]}"; do
    read -r id mem to cmd <<< "$line"
    [ -e "$EVAL_OUT/$id/done" ] && continue
    # a row whose status says state=running is never launched again, live pid or not (filler queues share a rows file, 2026-11-03;
    # 2026-11-05 23:45 brain05: C1.omnipaxos 2,1,1 was relaunched one second after its 48 h timeout killed the binary, while its wrapper
    # was still writing the done block, and the duplicate's header overwrote the record). A dead pid is an orphan: finalize-orphans.sh
    # finishes its bookkeeping (done marker); a row is requeued only by a human after that.
    if [ -f "$EVAL_OUT/$id/status" ] && grep -q '^state=running' "$EVAL_OUT/$id/status" 2>/dev/null; then
      if ! kill -0 "$(sed -n 's/^pid=//p' "$EVAL_OUT/$id/status" | head -1)" 2>/dev/null; then echo "row $id left alone: state=running with a dead pid (orphan; finalize-orphans.sh) $(date -Is)" >> "$QDIR/queue.log"; fi
      NEXT+=("$line"); continue
    fi
    dep=""; case "$cmd" in needok:*) dep=${cmd%% *}; dep=${dep#needok:}; cmd=${cmd#* };; esac
    if [ -n "$dep" ]; then   # needok:a,b,... (2026-11-04): every listed row must be done; any not ok skips this row
      wait_dep=0; bad_dep=""
      for d1 in ${dep//,/ }; do
        if [ ! -e "$EVAL_OUT/$d1/done" ]; then wait_dep=1; break; fi
        if ! grep -q '^reason=ok' "$EVAL_OUT/$d1/status" 2>/dev/null; then bad_dep=$d1; fi
      done
      if [ $wait_dep -eq 1 ]; then NEXT+=("$line"); continue; fi
      if [ -n "$bad_dep" ]; then skip_row "$id" "$mem" "$to" "$cmd" "$bad_dep"; continue; fi
    fi
    w=$(width_of "$id"); first=$(find_free "$w")
    if [ -z "$first" ]; then NEXT+=("$line"); continue; fi
    # memory guard (owner, 2026-11-05, after brain01 and two pinkys thrashed under uncapped JMC exports): never launch a row
    # while less than MIN_AVAIL_PCT (default 25) % of the machine's memory is available; the node keeps room for ssh and for us
    _ma=$(free -g | awk '/Mem:/ {print $7}'); _mt=$(free -g | awk '/Mem:/ {print $2}')
    if [ -n "$_ma" ] && [ "$_mt" -gt 0 ] && [ $((_ma*100/_mt)) -lt "${MIN_AVAIL_PCT:-25}" ]; then echo "$(date -Is) waiting: memory ${_ma} of ${_mt} GB available" >> "$QDIR/queue.log"; sleep 60; continue; fi
    if [ "$FOREIGN_LOAD_POLICY" != ignore ] && foreign_load; then echo "$(date -Is) waiting: foreign load $(cut -d' ' -f1 /proc/loadavg) ours=$(busy_threads)" >> "$QDIR/queue.log"; NEXT+=("$line"); continue; fi
    ( eval "run_row '$id' '$mem' '$to' '$first' '$w' $cmd" ) & p=$!
    ROWPID[$p]="$first"; for n in ${first//,/ }; do BUSY[$n]=$p; done; launched_any=1
    echo "$(date -Is) launched $id on numa $first (w=$w) pid $p" >> "$QDIR/queue.log"
  done
  PENDING=("${NEXT[@]}")
  [ ${#PENDING[@]} -gt 0 ] && [ $launched_any -eq 0 ] && sleep 10
done
wait
echo "queue $QNAME finished $(date -Is): $(wc -l < "$EVAL_OUT/results.tsv" 2>/dev/null) results so far on $(hostname)" >> "$QDIR/queue.log"
