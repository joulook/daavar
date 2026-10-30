#!/usr/bin/env bash
# Sourced by every script. Paths of the evaluation on a Brain node (override by exporting first).
: "${DAAVAR:=/local/mkhoshechin/eval/daavar}"        # this checkout (R1: never edited in place)
: "${EVAL_OUT:=/local/mkhoshechin/eval/out}"          # run directories, one per row id
: "${BRAIN_DATA:=/local/mkhoshechin/eval/brain-data}" # clone of github.com/joulook/brain-data (R2b)
: "${TFX:=$EVAL_OUT/tfx}"                             # TraceForge exports written on this node
: "${JMCX:=$DAAVAR/format/golden}"                    # JMC's 74 golden exports (+ sampled files)
: "${JAVA_HOME:=$HOME/jdk17}"                         # Temurin 17 for JMC
: "${CARGO_HOME:=/local/mkhoshechin/cargo}"
: "${TMPDIR:=/local/mkhoshechin/tmp}"
: "${JMC_CP_FILE:=$EVAL_OUT/jmc-cp.txt}"              # written by setup-node.sh
# git access: the clones use the ssh aliases gh-daavar (read) and gh-brain-data (write) from ~/.ssh/config
export DAAVAR EVAL_OUT BRAIN_DATA TFX JMCX JAVA_HOME CARGO_HOME TMPDIR JMC_CP_FILE
export PATH="$JAVA_HOME/bin:$CARGO_HOME/bin:$HOME/.cargo/bin:$PATH"
mkdir -p "$EVAL_OUT" "$TFX" "$TMPDIR" 2>/dev/null || true
