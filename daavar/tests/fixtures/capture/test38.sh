#!/usr/bin/env bash
# S11-S13 evidence on brain03 (DRIVER-IMPL-NOTES.md): test 3 (scaffold), test 8 (online, --stop-at-first), S11
# (render and replay, the run untouched), online equals offline on mcp-cancel pairs.aa.s0.
# usage: test38.sh <staged daavar tree> <work dir>
set -uo pipefail
S=$1; W=$2
. "$(dirname "$S")/env.sh"
export DAAVAR_CACHE=$W/cache
DRV="$T $S/bin/daavar"
[ -d "$W" ] && chmod -R u+w "$W"; rm -rf "$W"; mkdir -p "$W/with space"
treesum() { (cd "$1" && find . -type f -o -type l | LC_ALL=C sort | xargs -d '\n' md5sum | md5sum | cut -c1-12); }
latest() { readlink -f "$1/out/latest"; }
graph1() { find "$1" -path '*/joins/*/graphs/1.graph.json' | head -1; }
echo "== test 3: --spec java"; $DRV new x --spec java --dir "$W/x" 2>&1 | tail -2
for impl in rust java rust,java; do
  n=echo-${impl/,/-}; P="$W/with space/$n"
  echo "== test 3: new $n --impl $impl"
  $DRV new $n --impl $impl --dir "$P" | tail -1
  t0=$(date +%s.%N); $DRV check "$P" > "$W/$n.1.out" 2> "$W/$n.1.err"; echo "  check exit $? $(cut -d' ' -f3-4 "$W/$n.1.out") $(python3 -c "print(round($(date +%s.%N)-$t0,1))") s"
  R1=$(latest "$P")
  # the planted bug: the impl answers Pong{n+1}
  sed -i 's/ToServer::Ping { n } => send_msg(client, ToClient::Pong { n }),/ToServer::Ping { n } => send_msg(client, ToClient::Pong { n: n + 1 }),/' "$P/rust/src/imp.rs"
  [ -d "$P/java" ] && sed -i 's/new Pong(p.n())/new Pong(p.n() + 1)/' "$P"/java/bench/*/*.java
  $DRV check "$P" > "$W/$n.2.out" 2> "$W/$n.2.err"; echo "  planted bug: check exit $? $(cut -d' ' -f3-4 "$W/$n.2.out")"
  R2=$(latest "$P"); G=$(graph1 "$R2"); before=$(treesum "$R2")
  $DRV replay "$G" > "$W/$n.replay.out" 2> "$W/$n.replay.err"; echo "  replay $(basename $(dirname $(dirname $G))): exit $? $(cut -d' ' -f3-4 "$W/$n.replay.out") $(grep -m1 'replay:' "$W/$n.replay.err")"
  $DRV render "$G" > "$W/$n.render.out" 2>&1; echo "  render: $(tail -1 "$W/$n.render.out")"
  echo "  run untouched by replay and render: $([ "$before" = "$(treesum "$R2")" ] && echo yes || echo NO)"
  find "$R2" -path '*/joins/*/graphs/1.*' | sed 's|.*/joins/|  graph file |' | sort
  grep -m1 "replay:" "$R2/VERDICT.md" | sed 's/^/  VERDICT: /' | cut -c1-260
done
echo "== test 3: an uncommitted scaffold run reproduced (snapshot)"
P="$W/with space/echo-rust-java"; R=$(latest "$P")
$DRV check --json --from-run "$R" --into "$W/restore" > "$W/restore.out" 2> "$W/restore.err"; echo "  exit $? $(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['status'])" "$W/restore.out")"
grep -E "artifact differs|rebased" "$W/restore.err" | sed 's/^/  /'
N=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['paths'][0])" "$W/restore.out"); python3 -I -c "import json,sys; r=json.load(open(sys.argv[1])); print('  compared', r['n'], 'equal', r['equal'])" "$N/reproduction.json"
echo "== Java replay of a JMC-made graph (java<=spec of echo-java with the bug)"
R=$(latest "$W/with space/echo-java"); G=$(find "$R" -path '*java-in-spec/graphs/1.graph.json'); head -c 300 "$G" | grep -o '"tool":"[a-z]*"' | head -1
$DRV replay "$G" 2>&1 | tail -2
echo "== kv-cancel timing (tools and artifacts cached, exports not)"
$DRV build $S/examples/kv-cancel > /dev/null 2>&1; chmod -R u+w $W/cache/exports 2>/dev/null; rm -rf $W/cache/exports
t0=$(date +%s.%N); $DRV check $S/examples/kv-cancel 2>/dev/null | cut -d' ' -f3-4; echo "  kv-cancel check $(python3 -c "print(round($(date +%s.%N)-$t0,1))") s"
echo "== test 8"
$DRV check $S/examples/kv-cancel --mode online --stop-at-first 2>/dev/null | cut -d' ' -f3-4
K=$(latest $S/examples/kv-cancel); cut -f5,9,13-16,22 $K/summary.tsv | sed 's/^/  /'
P="$W/with space/echo-java"
$DRV check "$P" --mode online --stop-at-first 2>/dev/null | cut -d' ' -f3-4; cut -f5,9,13-16,22 "$(latest "$P")/summary.tsv" | sed 's/^/  /'
$DRV check $S/examples/kv-cancel --stop-at-first 2>&1 | tail -2 | sed 's/^/  /'
cp -r $S/examples/kv-cancel $W/kv-threads2; rm -rf $W/kv-threads2/out; sed -i 's/^threads = 1/threads = 2/' $W/kv-threads2/daavar.toml
sed -i 's|"../../traceforge"|"'$S'/traceforge"|; s|"../../jmc"|"'$S'/jmc"|' $W/kv-threads2/daavar.toml
$DRV check $W/kv-threads2 --mode online --stop-at-first 2>&1 | tail -2 | sed 's/^/  /'
M=$S/benchmarks/mcp-cancel
$DRV check $M --scenario pairs --cell aa.s0 --mode online --stop-at-first --directions 'mutant1<=spec' 2>/dev/null | cut -d' ' -f3-4
cut -f5,9,13-16,22 $(latest $M)/summary.tsv | sed 's/^/  /'
echo "== online equals offline on mcp-cancel pairs.aa.s0"
$DRV check $M --scenario pairs --cell aa.s0 2>/dev/null | cut -d' ' -f3-4; OFF=$(latest $M)
$DRV check $M --scenario pairs --cell aa.s0 --mode online 2>/dev/null | cut -d' ' -f3-4; ON=$(latest $M)
paste <(cut -f5,13-16,22 $OFF/summary.tsv) <(cut -f13-16,22 $ON/summary.tsv) | sed 's/^/  /'
for j in $OFF/pairs.aa.s0/joins/*/; do d=$(basename $j); a=$(grep -E '^(no-bucket|no-slot|not-found) ' $j/join.out | sort | md5sum | cut -c1-8); b=$(grep -E '^(no-bucket|no-slot|not-found) ' $ON/pairs.aa.s0/joins/$d/join.out | sort | md5sum | cut -c1-8); echo "  $d uncovered lines offline $a online $b $([ $a = $b ] && echo equal || echo DIFFERENT)"; done
echo end
