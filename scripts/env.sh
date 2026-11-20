#!/usr/bin/env bash
# Sourced by the scripts: the paths of this checkout. Every value can be overridden by exporting it first.
: "${DAAVAR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"   # this checkout
: "${TF_PIN:=$(git -C "$DAAVAR/traceforge" rev-parse --short=7 HEAD 2>/dev/null || echo nopin)}"  # the TraceForge pin
: "${EVAL_OUT:=$DAAVAR/out/$TF_PIN}"                  # build outputs (benchmark binaries, Java classes), one tree per pin; out/ is git-ignored
: "${JMC_CP_FILE:=$EVAL_OUT/jmc-cp.txt}"              # JMC's classpath, written by setup-node.sh
export DAAVAR TF_PIN EVAL_OUT JMC_CP_FILE
# JMC needs JDK 17: set JAVA_HOME, or have java and javac on PATH. Cargo, Gradle and TMPDIR are taken from the environment.
if [ -n "${JAVA_HOME:-}" ]; then export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"; fi
mkdir -p "$EVAL_OUT" 2>/dev/null || true
