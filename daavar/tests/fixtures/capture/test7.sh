#!/usr/bin/env bash
# S10 test 7 on brain03 (DRIVER-IMPL-NOTES.md S10): a run, then the original project moved away and the cache
# emptied, then `check --from-run`; four cases: A clean commit, B dirty checkout (one edited + one untracked
# file), C no repository, D --allow-dirty-tools (one edited TraceForge source; the dirty checkout moved away too).
# usage: test7.sh <staged daavar tree> <work dir>
set -uo pipefail
S=$1; W=$2
. "$(dirname "$S")/env.sh"
DRV=$S/bin/daavar
TF=/local/mkhoshechin/tmp/xcheck-pins/tf; JMC=/local/mkhoshechin/driver-dev/jmc
[ -d "$W" ] && chmod -R u+w "$W"; rm -rf "$W"; mkdir -p "$W"
kv() { mkdir -p "$1"; (cd $S/examples/kv-cancel && tar cf - daavar.toml README.md rust/Cargo.toml rust/Cargo.lock rust/src java) | tar xf - -C "$1"; }
empty() { [ -d "$1" ] && chmod -R u+w "$1" && rm -rf "$1"; true; }
run1() { # name project-dir [env/flags]
  local n=$1 p=$2; shift 2
  echo "== $n: check $p $*"
  ( cd "$W" && env DAAVAR_CACHE=$W/$n.cache "$@" $T $DRV check "$p" > $W/$n.check.out 2> $W/$n.check.err ); echo "  exit $? $(cat $W/$n.check.out)"
}
restore() { # name run-dir
  local n=$1 r=$2
  empty $W/$n.cache
  echo "== $n: --from-run $r"
  ( cd "$W" && env -u DAAVAR DAAVAR_CACHE=$W/$n.unused-cache $T $DRV check --from-run "$r" --into $W/$n.restore > $W/$n.restore.out 2> $W/$n.restore.err ); echo "  exit $? $(cat $W/$n.restore.out)"
  grep -E "not reproduced|differs|from-run" $W/$n.restore.err | sed 's/^/  /'
  local N=$(awk '{print $NF}' $W/$n.restore.out)
  [ -f "$N/reproduction.json" ] && python3 -I -c "import json,sys; r=json.load(open(sys.argv[1])); print('  compared', r['n'], 'equal', r['equal'], 'artifacts', [(a['name'], a['equal']) for a in r['artifacts']])" "$N/reproduction.json"
}
# A: clean commit of a repository with an origin
mkdir -p $W/A; git init -q --bare $W/A-origin.git
kv $W/A/work/examples/kv-cancel; ln -s $TF $W/A/work/traceforge; ln -s $JMC $W/A/work/jmc
(cd $W/A/work && git init -q && git add -A && git -c user.email=t@t -c user.name=t commit -qm A && git remote add origin $W/A-origin.git && git push -q origin HEAD:refs/heads/main)
run1 A $W/A/work/examples/kv-cancel DAAVAR=$W/A/work
RA=$(awk '{print $NF}' $W/A.check.out); mv $W/A/work $W/A/work.moved; RA=${RA/\/work\//\/work.moved\/}
restore A $RA
# B: dirty checkout of the same repository
git clone -q -b main $W/A-origin.git $W/B/work; ln -sfn $TF $W/B/work/traceforge; ln -sfn $JMC $W/B/work/jmc
echo "// test 7: an edit" >> $W/B/work/examples/kv-cancel/rust/src/spec.rs
echo "// test 7: an untracked, uncompiled module" > $W/B/work/examples/kv-cancel/rust/src/notes.rs
run1 B $W/B/work/examples/kv-cancel DAAVAR=$W/B/work
RB=$(awk '{print $NF}' $W/B.check.out); mv $W/B/work $W/B/work.moved; RB=${RB/\/work\//\/work.moved\/}
restore B $RB
# C: no repository, outside the daavar checkout
kv $W/C/x/kv-cancel; ln -s $TF $W/C/traceforge; ln -s $JMC $W/C/jmc
run1 C $W/C/x/kv-cancel
RC=$(awk '{print $NF}' $W/C.check.out); mv $W/C/x $W/C/x.moved; RC=${RC/\/C\/x\//\/C\/x.moved\/}
restore C $RC
# D: a dirty TraceForge checkout (one edited source file), --allow-dirty-tools; the dirty checkout moved away too
kv $W/D/x/kv-cancel; git clone -q $TF $W/D/traceforge; ln -s $JMC $W/D/jmc
sed -i '1s|^|// test 7: a dirty TraceForge edit\n|' $W/D/traceforge/traceforge/src/lib.rs
git -C $W/D/traceforge status --short
run1 D $W/D/x/kv-cancel; echo "  (without the flag: $(head -1 $W/D.check.err | cut -c1-120))"
( cd "$W" && DAAVAR_CACHE=$W/D.cache $T $DRV check $W/D/x/kv-cancel --allow-dirty-tools > $W/D.check.out 2> $W/D.check.err ); echo "  exit $? $(cat $W/D.check.out)"
RD=$(awk '{print $NF}' $W/D.check.out); ls $RD/inputs/tools/traceforge; mv $W/D/x $W/D/x.moved; mv $W/D/traceforge $W/D/traceforge.moved; RD=${RD/\/D\/x\//\/D\/x.moved\/}
restore D $RD
