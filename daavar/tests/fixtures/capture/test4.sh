#!/usr/bin/env bash
# S14 test 4 on brain03 (DRIVER-IMPL-NOTES.md S14): the case studies through the driver against the hand-run joins
# (A2A: RESULTS-4, /local/mkhoshechin/a2a-e4/joins, classify2.py; MCP: RESULTS-2, /local/mkhoshechin/tmp/mcp-joins/e2,
# classify.py). Per direction: uncovered/lines, the sorted uncovered lines' md5 (driver vs hand), the tags.
# usage: test4.sh <staged daavar tree> <work dir>
set -uo pipefail
S=$1; W=$2
. "$(dirname "$S")/env.sh"
export DAAVAR_CACHE=$W/cache
DRV="$T $S/bin/daavar"
[ -d "$W" ] && chmod -R u+w "$W"; rm -rf "$W"; mkdir -p "$W"
cp $S/daavar/tests/fixtures/config/a2a-task.daavar.toml $S/benchmarks/a2a-task/daavar.toml   # Appendix A rev 3.7
cp $S/daavar/tests/fixtures/config/mcp-cancel.daavar.toml $S/benchmarks/mcp-cancel/daavar.toml
H=/local/mkhoshechin/a2a-e4/joins; C2=/local/mkhoshechin/a2a-e4/classify2.py
HM=/local/mkhoshechin/tmp/mcp-joins/e2; CM=/local/mkhoshechin/mcp-1120s/classify.py
unc() { grep -E '^(no-bucket|no-slot|not-found) ' "$1" | sed -E 's/^[a-z-]+ //' | LC_ALL=C sort; }
FAILS=0
cmpj() { # label driver-join.out hand.out classifier: the sorted uncovered lines, and the classifier's whole output
  local a=$(unc "$2" | md5sum | cut -c1-8) b=$(unc "$3" | md5sum | cut -c1-8)
  local na=$(unc "$2" | wc -l) nb=$(unc "$3" | wc -l)
  local ta tb ca cb
  # (the MCP classifier also writes per-class examples to its second argument; both get one)
  ta=$(python3 -I $4 "$2" "$W/cls.driver.json" 2>&1); ca=$?; tb=$(python3 -I $4 "$3" "$W/cls.hand.json" 2>&1); cb=$?
  local le=$([ $a = $b ] && echo EQUAL || echo DIFFERENT) te=$([ "$ta" = "$tb" ] && [ $ca = 0 ] && [ $cb = 0 ] && echo EQUAL || echo DIFFERENT)
  echo "  $1: uncovered driver $na hand $nb; lines $le; classifier exit $ca/$cb, full output $te"
  echo "$ta" | sed 's/^/      driver: /'; [ "$te" = EQUAL ] || echo "$tb" | sed 's/^/      hand:   /'
  [ $le = EQUAL ] && [ $te = EQUAL ] || FAILS=$((FAILS+1))
}
dirs8='rust<=spec,java<=spec,rust-fixed<=spec,java-fixed<=spec,java<=rust,rust<=java,java-fixed<=rust-fixed,rust-fixed<=java-fixed'
declare -A MAP=([rust-in-spec]=rust-in-spec [java-in-spec]=java-in-spec [rust-fixed-in-spec]=rustF-in-spec
                [java-fixed-in-spec]=javaF-in-spec [java-in-rust]=java-in-rust [rust-in-java]=rust-in-java
                [java-fixed-in-rust-fixed]=javaF-in-rustF [rust-fixed-in-java-fixed]=rustF-in-javaF)
for sc in one:n:n deleg:n-local:2n-local gated:n-local:2gn-local; do
  IFS=: read s cell hand <<< "$sc"
  t0=$(date +%s.%N); $DRV check $S/benchmarks/a2a-task --scenario $s --cell $cell --directions "$dirs8" > $W/$s.out 2> $W/$s.err
  echo "== a2a $s.$cell: exit $? $(cut -d' ' -f3-4 $W/$s.out) $(python3 -c "print(round($(date +%s.%N)-$t0,1))") s"
  R=$(awk '{print $NF}' $W/$s.out)
  cut -f5,11-13,22-25 $R/summary.tsv | sed 's/^/    /'
  for j in $R/$s.$cell/joins/*/; do d=$(basename $j); cmpj "$d" $j/join.out $H/$hand.${MAP[$d]}.out $C2; done
done
for p in rust java; do :; done
t0=$(date +%s.%N); $DRV check $S/benchmarks/mcp-cancel --scenario pairs --cell aa.s0 --directions 'rust<=spec,java<=spec' > $W/mcp.out 2> $W/mcp.err
echo "== mcp pairs.aa.s0: exit $? $(cut -d' ' -f3-4 $W/mcp.out) $(python3 -c "print(round($(date +%s.%N)-$t0,1))") s"
R=$(awk '{print $NF}' $W/mcp.out); cut -f5,11-13,22-25 $R/summary.tsv | sed 's/^/    /'
for p in rust java; do cmpj "$p-in-spec" $R/pairs.aa.s0/joins/$p-in-spec/join.out $HM/$p-aa-s0.out $CM; done
md5sum /local/mkhoshechin/mcp-1120/tree/benchmarks/mcp-cancel/rust/src/imp.rs $S/benchmarks/mcp-cancel/rust/src/imp.rs | sed 's/^/  imp.rs /'
t0=$(date +%s.%N); $DRV check $S/benchmarks/mcp-cancel --scenario pairs-c2 --cell aa.s0.c2 --directions 'rust<=spec' > $W/mcpc2.out 2> $W/mcpc2.err
echo "== mcp pairs-c2.aa.s0.c2 (no hand join recorded): exit $? $(cut -d' ' -f3-4 $W/mcpc2.out) $(python3 -c "print(round($(date +%s.%N)-$t0,1))") s"
R=$(awk '{print $NF}' $W/mcpc2.out); cut -f5,11-13,22-25 $R/summary.tsv | sed 's/^/    /'
echo "comparisons that differ: $FAILS"
echo "pairs-c2.aa.s0.c2: no hand-run join exists (RESULTS-2 has no c2 cell); this part of test 4 is recorded, not compared"
echo end
[ $FAILS = 0 ]
