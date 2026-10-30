#!/usr/bin/env bash
# Run one benchmark's Rust binary (built by setup-node.sh into $EVAL_OUT/bench-target).
# usage: rust-run.sh <benchdir-name> <args...>     (args as the crate's CLI: <spec|impl|conf|export> <size> [model] [variant])
set -uo pipefail
. "$(dirname "$0")/env.sh"
B="$1"; shift
BIN=$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$DAAVAR/benchmarks/$B/rust/Cargo.toml" | head -1 | tr '-' '_')
exec "$EVAL_OUT/bench-target/release/$BIN" "$@" </dev/null
