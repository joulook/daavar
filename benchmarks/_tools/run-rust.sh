#!/usr/bin/env bash
# Build one benchmark's Rust crate against the private TraceForge copy and run it.
# usage: run-rust.sh <benchdir> <spec|impl> <size> [fifo|bag|causal]
set -euo pipefail
B="$1"; shift
export CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target
export TMPDIR=/local/home/mkhoshechin/tmp
cd "$B/rust"
cargo run -q -j 2 --offline --release -- "$@"
