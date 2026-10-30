#!/usr/bin/env bash
# Generate queue rows for the 19 literature benchmarks from the programs and sizes the builders
# ran (each benchmarks/<dir>/out/runs.log records every run-java.sh / run-rust.sh command).
# Writes into <outdir>: B-count-rust.rows, B-count-java.rows, B-export-java.rows, B-join-java.rows,
# B-online-java.rows (+ a .tsv sidecar each, read by mkrecord.py). Run the export queue before the
# join and online queues (they read the exports from $EVAL_OUT/<export-row-id>/export.jsonl).
# usage: gen-rows-benchmarks.sh <outdir> [timeoutS=3600] [memGB=250]
set -euo pipefail
. "$(dirname "$0")/env.sh"
O="$1"; TO="${2:-3600}"; MEM="${3:-250}"; mkdir -p "$O"
HDR=$'id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark'
for f in count-rust count-java export-java join-java online-java; do : > "$O/B-$f.rows"; printf '%s\n' "$HDR" > "$O/B-$f.rows.tsv"; done
san() { printf '%s' "$1" | tr ':,/ ' '____'; }
row() { printf '%s %s %s %s\n' "$1" "$MEM" "$TO" "$2" >> "$O/B-$3.rows"; }
tsv() { printf '%s\n' "$1" >> "$O/B-$2.rows.tsv"; }
tier=c1h; [ "$TO" -ge 86400 ] && tier=c24h
for b in "$DAAVAR"/benchmarks/*/; do
  dir=$(basename "$b"); log="$b/out/runs.log"; [ -f "$log" ] || continue
  # Rust: args after the benchmark path, drop 'conf' rows (G1) and duplicates
  grep -o 'run-rust.sh [^ ]* .*' "$log" | sed 's/^run-rust.sh [^ ]* //; s/ *$//' | grep -v '^conf' | sort -u | while read -r args; do
    set -- $args; side=$1; size=${2:-}; model=${3:-fifo}; var=${4:-$side}
    id="BR.count.$dir.$(san "$args")"
    row "$id" "\"\$DAAVAR\"/scripts/rust-run.sh $dir $args" count-rust
    tsv "$id	B	$dir	$size	$model	$var	must	1	n/a	plain count	$tier	must-standalone	traceforge	traceforge	rust	rust	n/a	full	$dir" count-rust
  done
  # Java: class:args with strategy must
  grep -o 'run-java.sh [^ ]* [^ ]* must' "$log" | awk '{print $3}' | tr -d "'" | sort -u | while read -r cls; do
    args=${cls#*:}; IFS=, read -r side size model var <<< "$args"; model=${model:-fifo}; var=${var:-$side}
    sid=$(san "$cls")
    row "BJ.count.$dir.$sid" "\"\$DAAVAR\"/scripts/jmc-run.sh $dir '$cls' must 0 1 - $TO" count-java
    tsv "BJ.count.$dir.$sid	B	$dir	$size	$model	$var	must	1	n/a	plain count	$tier	must-standalone	jmc	jmc	java	java	n/a	full	$dir" count-java
    row "BJ.export.$dir.$sid" "\"\$DAAVAR\"/scripts/jmc-run.sh $dir '$cls' must 0 1 export.jsonl $TO" export-java
    tsv "BJ.export.$dir.$sid	B	$dir	$size	$model	$var	must	1	n/a	summary export	$tier	export	jmc	jmc	java	java	offline-batch	full	$dir" export-java
  done
  # joins and online: every impl against the spec of the same size and model (Java/Java, until the Rust exports exist)
  grep -o 'run-java.sh [^ ]* [^ ]* must' "$log" | awk '{print $3}' | tr -d "'" | sort -u > "$O/.cls.$dir"
  grep ':spec,' "$O/.cls.$dir" | while read -r sc; do
    IFS=, read -r _ ssize smodel _ <<< "${sc#*:}"; smodel=${smodel:-fifo}
    grep ':impl,' "$O/.cls.$dir" | while read -r ic; do
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
for f in "$O"/B-*.rows; do echo "$(basename "$f"): $(grep -c . "$f") rows"; done
