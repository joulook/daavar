#!/usr/bin/env bash
# S10 test 7, case E (reviews S7-S10: Astra 5, Opus F4): absolute [tools] paths and an absolute TraceForge path
# dependency; the project and its tool checkouts moved away; --from-run rebases them into D.
# usage: test7e.sh <staged daavar tree> <work dir>
set -uo pipefail
S=$1; W=$2
. "$(dirname "$S")/env.sh"
DRV=$S/bin/daavar
[ -d "$W" ] && chmod -R u+w "$W"; rm -rf "$W"; mkdir -p "$W/E"
git clone -q /local/mkhoshechin/tmp/xcheck-pins/tf $W/E/tools/tf; git clone -q /local/mkhoshechin/driver-dev/jmc $W/E/tools/jmc
mkdir -p $W/E/p/kv; (cd $S/examples/kv-cancel && tar cf - daavar.toml rust/Cargo.toml rust/Cargo.lock rust/src java) | tar xf - -C $W/E/p/kv
sed -i "s|^traceforge = \"../../traceforge\"|traceforge = \"$W/E/tools/tf\"|; s|^checkout = \"../../jmc\"|checkout = \"$W/E/tools/jmc\"|" $W/E/p/kv/daavar.toml
sed -i "s|path = \"../../../traceforge/traceforge\"|path = \"$W/E/tools/tf/traceforge\"|" $W/E/p/kv/rust/Cargo.toml
grep -n "tools/" $W/E/p/kv/daavar.toml $W/E/p/kv/rust/Cargo.toml | sed 's/^/  /'
( cd $W && DAAVAR_CACHE=$W/E.cache $T $DRV check $W/E/p/kv > $W/E.check.out 2> $W/E.check.err ); echo "== E check: exit $? $(cat $W/E.check.out)"
R=$(awk '{print $NF}' $W/E.check.out)
mv $W/E $W/E.moved; R=${R/\/E\//\/E.moved\/}; chmod -R u+w $W/E.cache; rm -rf $W/E.cache
( cd $W && DAAVAR_CACHE=$W/E.unused $T $DRV check --from-run $R --into $W/D.elsewhere > $W/E.restore.out 2> $W/E.restore.err ); echo "== E --from-run: exit $? $(cat $W/E.restore.out)"
grep -E "rebased|artifact differs|from-run" $W/E.restore.err | sed 's/^/  /'
N=$(awk '{print $NF}' $W/E.restore.out); python3 -I -c "import json,sys; r=json.load(open(sys.argv[1])); print('  compared', r['n'], 'equal', r['equal'], [(a['name'], a['equal'], a['allowed']) for a in r['artifacts']])" "$N/reproduction.json"
grep -rn "tools/" $(dirname $(dirname $N))/daavar.toml $(dirname $(dirname $N))/rust/Cargo.toml | sed 's/^/  restored: /'
echo end
