#!/usr/bin/env bash
# One-node queue runner for the evaluation. Rows come from a text file, one per line:
#   <row-id> <memGB> <timeoutS> <command ...>
# The runner keeps S slots busy (default 4), pins slot k to NUMA node k (cores 16k..16k+15),
# gives each row its own working directory under $OUT/<row-id>/ (JMC's launcher must get a
# fresh report dir), limits memory with ulimit -v and time with timeout, feeds </dev/null
# (gradle/java hang on an ssh stdin otherwise), and appends one result line per row to
# $OUT/results.tsv: id, exit status, stop reason, wall seconds, peak RSS KB. Rows whose
# $OUT/<row-id>/done marker exists are skipped, so the runner is restartable.
# usage: run-queue.sh <rows.txt> <outdir> [slots]
set -uo pipefail
ROWS="$1"; OUT="$2"; SLOTS="${3:-4}"
mkdir -p "$OUT"
run_row() {  # id memGB timeoutS slot cmd...
  local id="$1" mem="$2" to="$3" slot="$4"; shift 4
  local d="$OUT/$id"; mkdir -p "$d"
  [ -e "$d/done" ] && return 0
  local c0=$((16*slot)) c1=$((16*slot+15))
  local start=$(date +%s)
  ( ulimit -v $((mem*1024*1024)); cd "$d" && \
    /usr/bin/time -f '%e %M' -o "$d/time.txt" \
    taskset -c "$c0-$c1" timeout --signal=KILL "$to" "$@" </dev/null >"$d/stdout.log" 2>"$d/stderr.log" )
  local st=$?
  local reason=ok; [ $st -eq 137 ] && reason=timeout; { [ $st -ne 0 ] && [ $st -ne 137 ]; } && reason="exit-$st"
  grep -qi "out of memory\|memory allocation of\|Cannot allocate" "$d/stderr.log" 2>/dev/null && reason=memory
  local wall peak; read -r wall peak < <(tail -1 "$d/time.txt" 2>/dev/null || echo "? ?")
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$st" "$reason" "$wall" "$peak" "$(hostname) slot=$slot cores=$c0-$c1 mem=${mem}G t=${to}s" >> "$OUT/results.tsv"
  touch "$d/done"
}
export -f run_row; export OUT
# dispatch: slot k runs every (k mod SLOTS)-th row in file order
for ((k=0; k<SLOTS; k++)); do
  ( n=0; while read -r id mem to cmd; do [ -z "$id" ] && continue; case "$id" in \#*) continue;; esac
      if [ $((n % SLOTS)) -eq $k ]; then eval "run_row '$id' '$mem' '$to' '$k' $cmd"; fi; n=$((n+1)); done < "$ROWS" ) &
done
wait
echo "queue $ROWS finished on $(hostname): $(wc -l < "$OUT/results.tsv") results"
