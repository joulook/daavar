#!/usr/bin/env bash
# One-time setup of a node after `git pull && git submodule update` in $DAAVAR (R1).
# Builds TraceForge (lib tests = the eval child, and the bins incl. conf-join), JMC (classes +
# classpath file), every benchmark's Rust crate and Java sources. Idempotent. Logs to $EVAL_OUT/setup.log.
# usage: setup-node.sh
set -euo pipefail
. "$(dirname "$0")/env.sh"
exec > >(tee -a "$EVAL_OUT/setup.log") 2>&1
echo "== setup $(hostname) $(date -Is) daavar=$(git -C "$DAAVAR" rev-parse --short HEAD)"
for t in zstd jq python3 taskset /usr/bin/time git; do command -v "$t" >/dev/null || { echo "missing $t"; exit 1; }; done
# TraceForge
cd "$DAAVAR/traceforge"
cargo build --release -p traceforge --bins -j 16
EXE=$(cargo test --release -p traceforge --lib --no-run --message-format=json -j 16 \
      | jq -r 'select(.profile.test==true and .target.kind[0]=="lib") | .executable' | tail -1)
[ -x "$EXE" ] || { echo "no lib test executable"; exit 1; }
ln -sfn "$EXE" target/release/tf-lib-tests
echo "tf-lib-tests -> $EXE"; ls -la target/release/conf-join
# JMC: compile and record the classpath (the launcher lives in core's test classes)
# Gradle: a per-node user home seeded from the NFS one (avoids the shared lock and the downloads); no daemon
if [ ! -d "$GRADLE_USER_HOME" ] && [ -d "$HOME/.gradle" ]; then rsync -a --exclude '*.lock' --exclude daemon "$HOME/.gradle/" "$GRADLE_USER_HOME/"; fi
mkdir -p "$GRADLE_USER_HOME"
cd "$DAAVAR/jmc"
./gradlew -q --no-daemon --offline :core:testClasses </dev/null || ./gradlew -q --no-daemon :core:testClasses </dev/null
./gradlew -q --no-daemon -I "$DAAVAR/scripts/print-classpath.gradle" :core:printTestClasspath </dev/null   # writes jmc/cp.txt (a build output, not source)
grep -q 'core/build/classes' cp.txt || { echo "classpath not produced"; exit 1; }
cp cp.txt "$JMC_CP_FILE"; echo "jmc classpath: $(tr ':' '\n' < "$JMC_CP_FILE" | wc -l) entries"
# Benchmarks: Java once per directory, Rust once per crate
CP="$(cat "$JMC_CP_FILE")"
for b in "$DAAVAR"/benchmarks/*/; do
  [ -d "$b/java" ] || continue
  mkdir -p "$EVAL_OUT/javaout/$(basename "$b")"
  javac -d "$EVAL_OUT/javaout/$(basename "$b")" -cp "$CP" $(find "$b/java" -name '*.java' -not -path '*/out/*') \
    && echo "javac ok: $(basename "$b")" || echo "javac FAILED: $(basename "$b")"
  if [ -f "$b/rust/Cargo.toml" ]; then
    (cd "$b/rust" && CARGO_TARGET_DIR="$EVAL_OUT/bench-target" cargo build --release -q -j 8) \
      && echo "cargo ok: $(basename "$b")" || echo "cargo FAILED: $(basename "$b")"
  fi
done
echo "== setup done $(date -Is)"
