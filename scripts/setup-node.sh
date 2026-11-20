#!/usr/bin/env bash
# Build a checkout after `git submodule update --init`: TraceForge (its binaries, including conf-join), JMC (classes
# and the classpath file $JMC_CP_FILE), every benchmark's Rust crate and Java sources, each followed by its source
# manifest. Idempotent. Paths from env.sh; JOBS = parallel build jobs (default: nproc). Logs to $EVAL_OUT/setup.log.
# usage: setup-node.sh
set -euo pipefail
. "$(dirname "$0")/env.sh"
: "${JOBS:=$(nproc)}"
exec > >(tee -a "$EVAL_OUT/setup.log") 2>&1
echo "== setup $(hostname) $(date -Is) daavar=$(git -C "$DAAVAR" rev-parse --short HEAD)"
for t in git jq python3 cargo java javac; do command -v "$t" >/dev/null || { echo "missing $t"; exit 1; }; done
# Tool commits (witness contract §1.2.2): node trees may have no .git, so the tools' build scripts read the
# commit from TRACEFORGE_COMMIT / JMC_COMMIT, set here from daavar's submodule pins (an exported value wins).
# A readable submodule checkout that differs from the pin (other HEAD, or modified) gives its own HEAD[-dirty].
pin_commit() {  # <submodule dir>
  local pin head
  pin=$(git -C "$DAAVAR" rev-parse -q --verify "HEAD:$1" 2>/dev/null || true)
  head=$(git -C "$DAAVAR/$1" rev-parse -q --verify HEAD 2>/dev/null || true)
  if [ -n "$head" ]; then
    [ -z "$(git -C "$DAAVAR/$1" status --porcelain --untracked-files=no 2>/dev/null)" ] || head="$head-dirty"
    [ -z "$pin" ] || [ "$head" = "$pin" ] || echo "WARNING: $1 checkout $head is not the pin $pin; using the checkout" >&2
    echo "$head"
  else echo "${pin:-unknown}"; fi
}
: "${TRACEFORGE_COMMIT:=$(pin_commit traceforge)}"; : "${JMC_COMMIT:=$(pin_commit jmc)}"
export TRACEFORGE_COMMIT JMC_COMMIT; echo "TRACEFORGE_COMMIT=$TRACEFORGE_COMMIT JMC_COMMIT=$JMC_COMMIT"
# TraceForge
cd "$DAAVAR/traceforge"
cargo build --release -p traceforge --bins -j "$JOBS"
ls -la target/release/conf-join
# JMC: compile and record the classpath (the launcher lives in core's test classes); no Gradle daemon
cd "$DAAVAR/jmc"
./gradlew -q --no-daemon --offline :core:testClasses </dev/null || ./gradlew -q --no-daemon :core:testClasses </dev/null
./gradlew -q --no-daemon -I "$DAAVAR/scripts/print-classpath.gradle" :core:printTestClasspath </dev/null   # writes jmc/cp.txt (a build output, not source)
grep -q 'core/build/classes' cp.txt || { echo "classpath not produced"; exit 1; }
cp cp.txt "$JMC_CP_FILE"; echo "jmc classpath: $(tr ':' '\n' < "$JMC_CP_FILE" | wc -l) entries"
# Benchmarks: Java once per directory, Rust once per crate
CP="$(cat "$JMC_CP_FILE")"
# A directory may hold java/, rust/ or both; each is built where present.
for b in "$DAAVAR"/benchmarks/*/; do
  if [ -d "$b/java" ]; then
    mkdir -p "$EVAL_OUT/javaout/$(basename "$b")"
    javac -d "$EVAL_OUT/javaout/$(basename "$b")" -cp "$CP" $(find "$b/java" -name '*.java' -not -path '*/out/*') \
      && echo "javac ok: $(basename "$b")" && { "$DAAVAR/scripts/source-manifest.sh" java "$b" "$EVAL_OUT/javaout/$(basename "$b")" || echo "manifest FAILED: $(basename "$b")"; } \
      || echo "javac FAILED: $(basename "$b")"
  fi
  # variants with their own Java sources: context-free/java (context-free rewrites), port-java/java (ports of existing
  # code); compiled into javaout/<bench>@<variant>, the name jmc-run.sh takes
  for v in context-free port-java; do
    [ -d "$b/$v/java" ] || continue
    k="$(basename "$b")@$v"; mkdir -p "$EVAL_OUT/javaout/$k"
    javac -d "$EVAL_OUT/javaout/$k" -cp "$CP" $(find "$b/$v/java" -name '*.java' -not -path '*/out/*') \
      && echo "javac ok: $k" && { "$DAAVAR/scripts/source-manifest.sh" java "$b/$v" "$EVAL_OUT/javaout/$k" || echo "manifest FAILED: $k"; } \
      || echo "javac FAILED: $k"
  done
  if [ -f "$b/rust/Cargo.toml" ]; then
    (cd "$b/rust" && CARGO_TARGET_DIR="$EVAL_OUT/bench-target" cargo build --release -q -j "$JOBS") \
      && echo "cargo ok: $(basename "$b")" && { "$DAAVAR/scripts/source-manifest.sh" rust "$b/rust" "$EVAL_OUT/bench-target/release" || echo "manifest FAILED: $(basename "$b")"; } \
      || echo "cargo FAILED: $(basename "$b")"
  fi
done
echo "== setup done $(date -Is)"
