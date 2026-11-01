#!/usr/bin/env bash
# Generate queue rows for the benchmark directories. Rust cases come from the old-form run-rust.sh lines of
# benchmarks/<dir>/out/runs.log, else the case files beside this script (README tables). a2-store, a3-lock and
# s1 to s6 get no B rows: the T rows cover them. Java cases here come from the log's run-java.sh lines only;
# B-java-cases.tsv beside this script lists every Java case, keyed as the Rust rows (it is not read here).
# Writes into <outdir>: B-count-rust.rows, B-export-rust.rows, B-long-rust.rows, B-count-java.rows,
# B-export-java.rows, B-join-java.rows, B-online-java.rows (+ a .tsv sidecar each, read by mkrecord.py). Run the export queue before the
# join and online queues (they read the exports from $EVAL_OUT/<export-row-id>/export.jsonl).
# usage: gen-rows-benchmarks.sh <outdir> [timeoutS=3600] [memGB=250]
set -euo pipefail
. "$(dirname "$0")/env.sh"
O="$1"; TO="${2:-3600}"; MEM="${3:-250}"; mkdir -p "$O"
HDR=$'id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark'
for f in count-rust export-rust long-rust count-java export-java join-java online-java; do : > "$O/B-$f.rows"; printf '%s\n' "$HDR" > "$O/B-$f.rows.tsv"; done
san() { printf '%s' "$1" | tr ':,/ ' '____'; }
row() { printf '%s %s %s %s\n' "$1" "$MEM" "$TO" "$2" >> "$O/B-$3.rows"; }
tsv() { printf '%s\n' "$1" >> "$O/B-$2.rows.tsv"; }
tier=c1h; [ "$TO" -ge 86400 ] && tier=c24h
# The Rust cases of the crates whose log has no usable line. A bad list must not pass as a short queue.
CASES="$(dirname "$0")/B-rust-readme-cases.tsv"
[ -s "$CASES" ] || { echo "gen-rows-benchmarks: $CASES is missing or empty" >&2; exit 1; }
cut -f1 "$CASES" | sort -u | while read -r d; do
  [ -d "$DAAVAR/benchmarks/$d" ] || { echo "gen-rows-benchmarks: $CASES names no directory: $d" >&2; exit 1; }
done
# The crates whose ARGS are not the old positional form: dir, side, model, ARGS, source (readme), options the crate's header says every row spells.
ARGCASES="$(dirname "$0")/B-rust-args-cases.tsv"
# The cases the builders' runs could not exhaust, a 24 h count row and a 24 h export row each: count row id, evidence.
CAPPED="$(dirname "$0")/B-rust-capped-cases.tsv"
[ -s "$ARGCASES" ] || { echo "gen-rows-benchmarks: $ARGCASES is missing or empty" >&2; exit 1; }
[ -s "$CAPPED" ] || { echo "gen-rows-benchmarks: $CAPPED is missing or empty" >&2; exit 1; }
cut -f1 "$ARGCASES" | sort -u | while read -r d; do
  [ -d "$DAAVAR/benchmarks/$d" ] || { echo "gen-rows-benchmarks: $ARGCASES names no directory: $d" >&2; exit 1; }
  ! cut -f1 "$CASES" | grep -qx "$d" || { echo "gen-rows-benchmarks: $d is in both $CASES and $ARGCASES" >&2; exit 1; }
done
# One case: a count row and an export row. A capped case also has a 24 h count row, and its export row is 24 h,
# in B-long-rust: no 1 h row that cannot finish, and one path for the join.
rustcase() {  # sid side model ARGS size variant note [options]; every row has a seed, or the shared main draws a random one
  local sid="$1" side="$2" model="$3" a="$4" size="$5" var="$6" note="$7" opts=" ${8:---seed 0}"
  local id="BR.count.$dir.$sid" xid="BR.export.$dir.$sid" cmd="\"\$DAAVAR\"/scripts/rust-run.sh $dir"
  row "$id" "$cmd count $side --model $model$opts -- $a" count-rust
  tsv "$id	B	$dir	$size	$model	$var	must	1	n/a	$note	$tier	must-standalone	traceforge	traceforge	rust	rust	n/a	full	$dir" count-rust
  if cut -f1 "$CAPPED" | grep -qxF "$id"; then
    printf '%s %s %s %s\n' "$id.c24h" "$MEM" 86400 "$cmd count $side --model $model$opts -- $a" >> "$O/B-long-rust.rows"
    tsv "$id.c24h	B	$dir	$size	$model	$var	must	1	n/a	plain count, 24 h tier (capped in the builders' run)${note#plain count}	c24h	must-standalone	traceforge	traceforge	rust	rust	n/a	full	$dir" long-rust
    printf '%s %s %s %s\n' "$xid" "$MEM" 86400 "$cmd export $side --out export.jsonl --model $model$opts -- $a" >> "$O/B-long-rust.rows"
    tsv "$xid	B	$dir	$size	$model	$var	must	1	n/a	summary export, 24 h tier (capped in the builders' run)${note#plain count}	c24h	export	traceforge	traceforge	rust	rust	offline-batch	full	$dir" long-rust
  else
    row "$xid" "$cmd export $side --out export.jsonl --model $model$opts -- $a" export-rust
    tsv "$xid	B	$dir	$size	$model	$var	must	1	n/a	summary export${note#plain count}	$tier	export	traceforge	traceforge	rust	rust	offline-batch	full	$dir" export-rust
  fi
}
for b in "$DAAVAR"/benchmarks/*/; do
  dir=$(basename "$b"); log="$b/out/runs.log"
  [ ! -f "$log" ] || [ -r "$log" ] || { echo "gen-rows-benchmarks: cannot read $log" >&2; exit 1; }
  # Rust: args after the benchmark path, drop 'conf' rows (G1) and duplicates
  # (the logs keep the old positional form; a line whose first word is not a side is prose, not a run)
  rust=; [ ! -f "$log" ] || rust=$({ grep -o 'run-rust.sh [^ ]* .*' "$log" | sed 's/^run-rust.sh [^ ]* //; s/ *$//' | grep -E '^(spec|spec-ring|impl)( |$)' || true; } | sort -u)
  # A crate whose log has no usable Rust line: the cases of its README count table, listed by hand.
  note="plain count"
  if [ -z "$rust" ]; then
    rust=$(awk -F'\t' -v d="$dir" '$1 == d { print $2 }' "$CASES" | sort -u)
    note="plain count, from the README count table (no usable Rust line in out/runs.log)"
  elif cut -f1 "$CASES" "$ARGCASES" | sort -u | grep -qx "$dir"; then
    echo "gen-rows-benchmarks: $dir has usable Rust lines in its log; its listed cases are ignored" >&2
  fi
  printf '%s' "${rust:+$rust$'\n'}" | while read -r args; do
    set -- $args; side=$1; size=${2:-}; model=${3:-fifo}; var=${4:-$side}
    # The shared main: `count <side> --model <m> -- <size> [variant]`; ironlock's `spec-ring` is `count spec … <n> correct ring`.
    if [ "$side" = spec-ring ]; then
      rustcase "$(san "$args")" spec "$model" "$size correct ring" "$size" "$var" "$note"
    else
      rustcase "$(san "$args")" "$side" "$model" "$size${4:+ $4}" "$size" "$var" "$note"
    fi
  done
  # A crate with neither: its ARGS as listed; size is the numbers in ARGS, variant the words. tables.py keys a
  # row by size, model and variant, so a spec row's variant starts with spec: it must not meet an impl row's.
  if [ -z "$rust" ]; then
    { awk -F'\t' -v d="$dir" '$1 == d { print $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 }' "$ARGCASES"; } | while IFS=$'\t' read -r side model a src opts; do
      [ "$src" = readme ] || { echo "gen-rows-benchmarks: $ARGCASES: unknown source: $src" >&2; exit 1; }
      note="plain count, from the README count table (no usable Rust line in out/runs.log)"
      size=$(printf '%s\n' $a | awk '/^[0-9]+$/' | paste -sd, -); var=$(printf '%s\n' $a | awk '!/^[0-9]+$/' | paste -sd- -)
      [ "$side" != spec ] || var="spec${var:+-$var}"
      rustcase "$(san "$side $a $model")" "$side" "$model" "$a" "$size" "${var:-$side}" "$note" "$opts"
    done
  fi
  [ -f "$log" ] || continue
  # Java: class:args with strategy must
  { grep -o 'run-java.sh [^ ]* [^ ]* must' "$log" || true; } | awk '{print $3}' | tr -d "'" | sort -u | while read -r cls; do
    args=${cls#*:}; IFS=, read -r side size model var <<< "$args"; model=${model:-fifo}; var=${var:-$side}
    sid=$(san "$cls")
    row "BJ.count.$dir.$sid" "\"\$DAAVAR\"/scripts/jmc-run.sh $dir '$cls' must 0 1 - $TO" count-java
    tsv "BJ.count.$dir.$sid	B	$dir	$size	$model	$var	must	1	n/a	plain count	$tier	must-standalone	jmc	jmc	java	java	n/a	full	$dir" count-java
    row "BJ.export.$dir.$sid" "\"\$DAAVAR\"/scripts/jmc-run.sh $dir '$cls' must 0 1 export.jsonl $TO" export-java
    tsv "BJ.export.$dir.$sid	B	$dir	$size	$model	$var	must	1	n/a	summary export	$tier	export	jmc	jmc	java	java	offline-batch	full	$dir" export-java
  done
  # joins and online: every impl against the spec of the same size and model (Java/Java, until the Rust exports exist)
  { grep -o 'run-java.sh [^ ]* [^ ]* must' "$log" || true; } | awk '{print $3}' | tr -d "'" | sort -u > "$O/.cls.$dir"
  { grep ':spec,' "$O/.cls.$dir" || true; } | while read -r sc; do
    IFS=, read -r _ ssize smodel _ <<< "${sc#*:}"; smodel=${smodel:-fifo}
    { grep ':impl,' "$O/.cls.$dir" || true; } | while read -r ic; do
      IFS=, read -r _ isize imodel ivar <<< "${ic#*:}"; imodel=${imodel:-fifo}
      [ "$isize" = "$ssize" ] && [ "$imodel" = "$smodel" ] || continue
      S="\"\$EVAL_OUT\"/BJ.export.$dir.$(san "$sc")/export.jsonl"; I="\"\$EVAL_OUT\"/BJ.export.$dir.$(san "$ic")/export.jsonl"
      jid="BX.join.$dir.$(san "$ic").jmcspec-jmcimpl"
      row "$jid" "\"\$DAAVAR\"/traceforge/target/release/conf-join $S $I" join-java
      tsv "$jid	B	$dir	$isize	$imodel	$ivar	join	1	readme	Java spec vs Java impl, offline	c600	cross-language	jmc	jmc	java	java	offline-batch	full	$dir" join-java
      oid="BJ.online.$dir.$(san "$ic").jmcspec"
      row "$oid" "\"\$DAAVAR\"/scripts/jmc-run.sh $dir '$ic' must 0 1 - $TO --spec $S" online-java
      tsv "$oid	B	$dir	$isize	$imodel	$ivar	stateful-online	1	readme	Java impl online vs Java spec export	$tier	cross-language	jmc	jmc	java	java	online	stop-at-first	$dir" online-java
    done
  done
  rm -f "$O/.cls.$dir"
done
# Every Rust crate but those the T rows cover has rows; every capped case has its 24 h rows.
for b in "$DAAVAR"/benchmarks/*/; do
  dir=$(basename "$b"); [ -f "$b/rust/Cargo.toml" ] || continue
  case "$dir" in a2-store|a3-lock|s1-naive|s2-share|s3-commit|s4-chain|s5-reset|s6-width) continue ;; esac
  grep -qF "BR.count.$dir." "$O/B-count-rust.rows" || { echo "gen-rows-benchmarks: no rows for the Rust crate $dir" >&2; exit 1; }
done
cut -f1 "$CAPPED" | while read -r id; do
  grep -qF "$id.c24h " "$O/B-long-rust.rows" || { echo "gen-rows-benchmarks: $CAPPED names no case: $id" >&2; exit 1; }
  grep -qF "BR.export.${id#BR.count.} " "$O/B-long-rust.rows" || { echo "gen-rows-benchmarks: no 24 h export row: $id" >&2; exit 1; }
done
for f in "$O"/B-*.rows; do echo "$(basename "$f"): $(grep -c . "$f") rows"; done
