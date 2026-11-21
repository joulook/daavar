#!/usr/bin/env bash
# S15 evidence on brain03 (DRIVER-IMPL-NOTES.md S15): kv-cancel checked with JMC fc684f21 (0.3.0) under
# cli = "positional", "named" and "auto" (offline, then online), and with JMC c1bbf657 under "auto"
# (positional). usage: test15.sh <staged daavar tree> <work dir> <jmc fc684f21 checkout>
set -uo pipefail
S=$1; W=$2; J15=$3
. "$(dirname "$S")/env.sh"
DRV="$T $S/bin/daavar"
TF=/local/mkhoshechin/tmp/xcheck-pins/tf; J1=/local/mkhoshechin/driver-dev/jmc
[ -d "$W" ] && chmod -R u+w "$W"; rm -rf "$W"; mkdir -p "$W"
kv() { # dir jmc cli
  mkdir -p "$1/examples/kv-cancel"; ln -s $TF "$1/traceforge"; ln -s "$2" "$1/jmc"
  (cd $S/examples/kv-cancel && tar cf - daavar.toml README.md rust/Cargo.toml rust/Cargo.lock rust/src java) | tar xf - -C "$1/examples/kv-cancel"
  sed -i "s/^cli = \"positional\"$/cli = \"$3\"/" "$1/examples/kv-cancel/daavar.toml"
  grep '^cli' "$1/examples/kv-cancel/daavar.toml"
}
run() { # name dir cache [flags]
  local n=$1 d=$2 c=$3; shift 3
  ( cd "$W" && DAAVAR_CACHE=$c $DRV check --json "$d/examples/kv-cancel" "$@" > $W/$n.out 2> $W/$n.err )
  echo "== $n: exit $? $(python3 -I -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['status'], d['paths'][0])" $W/$n.out 2>&1)"
}
for f in positional named auto; do kv $W/$f $J15 $f; done
kv $W/old-auto $J1 auto
for f in positional named auto; do run $f-offline $W/$f $W/cache15; run $f-online $W/$f $W/cache15 --mode online; done
run old-auto-offline $W/old-auto $W/cache-old
ls $W/cache15/tools/ | grep cli; cat $W/cache15/tools/*.cli.json $W/cache-old/tools/*.cli.json
