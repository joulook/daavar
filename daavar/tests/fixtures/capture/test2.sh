#!/usr/bin/env bash
# S6 evidence on brain03 (DRIVER-IMPL-NOTES.md S6): `daavar build` twice per study, the artifacts' manifests
# against scripts/source-manifest.sh, and the second build a no-op.
# usage: test2.sh <staged daavar tree> <work dir>
set -uo pipefail
D=$1; W=$2
export PATH=$HOME/jdk17/bin:$HOME/.cargo/bin:$PATH JAVA_HOME=$HOME/jdk17 CARGO_HOME=/local/mkhoshechin/cargo \
       GRADLE_USER_HOME=/local/mkhoshechin/gradle TMPDIR=$W/tmp CARGO_NET_OFFLINE=true DAAVAR_CACHE=$W/cache
mkdir -p $W/tmp $W/test2
cd $D
echo "tools: tf $(git -C traceforge rev-parse HEAD) ($(git -C traceforge status --porcelain | wc -l) changed); jmc $(git -C jmc rev-parse HEAD) ($(git -C jmc status --porcelain | wc -l) changed)"
for b in a2a-task mcp-cancel; do
  for n in 1 2; do
    t0=$(date +%s.%N)
    bin/daavar build benchmarks/$b --json > $W/test2/$b.build$n.json 2> $W/test2/$b.build$n.err; rc=$?
    t1=$(date +%s.%N)
    echo "$b build $n: exit $rc, $(python3 -c "print(round($t1-$t0,1))") s, $(python3 -I -c "import json,sys; r=json.load(open(sys.argv[1]))['result']; print('built_anything', r.get('built_anything'), [(s['name'], s['built']) for s in r.get('steps', [])])" $W/test2/$b.build$n.json)"
  done
  python3 -I - $W/test2/$b.build2.json $W/test2/$b.build1.json <<'PY'
import json, sys
a, b = (json.load(open(p))["result"]["artifacts"] for p in sys.argv[1:3])
print("  same keys on both builds:", {k: a[k]["key"] == b[k]["key"] for k in a})
PY
  R=$(python3 -I -c "import json,sys; print(json.load(open(sys.argv[1]))['result']['artifacts']['rust']['out'])" $W/test2/$b.build1.json)
  bin=$(ls $R | grep -v SOURCE.md5)
  DAAVAR=$D scripts/source-manifest.sh rust benchmarks/$b/rust $W/test2/$b.rust.script.md5 > /dev/null
  cmp $R/$bin.SOURCE.md5 $W/test2/$b.rust.script.md5 && echo "  rust manifest byte-identical ($(md5sum < $R/$bin.SOURCE.md5 | cut -c1-8), $(($(wc -l < $R/$bin.SOURCE.md5) - 1)) files)"
  J=$(python3 -I -c "import json,sys; print(json.load(open(sys.argv[1]))['result']['artifacts']['java']['classes'])" $W/test2/$b.build1.json)
  rm -rf $W/test2/$b.classes; mkdir -p $W/test2/$b.classes; cp -r $J/. $W/test2/$b.classes/; chmod -R u+w $W/test2/$b.classes; rm $W/test2/$b.classes/SOURCE.md5
  DAAVAR=$D scripts/source-manifest.sh java benchmarks/$b $W/test2/$b.classes > /dev/null
  cmp $J/SOURCE.md5 $W/test2/$b.classes/SOURCE.md5 && echo "  java manifest byte-identical ($(md5sum < $J/SOURCE.md5 | cut -c1-8), $(($(wc -l < $J/SOURCE.md5) - 1)) files)"
  ls -ld $R $R/$bin | awk '{print "  " $1, $NF}'
done
