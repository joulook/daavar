#!/usr/bin/env bash
# Witness step of a join (witness contract §5): conf-join with --decide and --witness-dir, then the renderer.
# usage: witness-join.sh <spec-export> <impl-export> <outdir> [conf-join args...]
#   1. conf-join --decide --witness-dir <outdir>/graphs [args...] <spec> <impl>  > <outdir>/join.out 2> <outdir>/join.err
#      (stores: <export>.graphs.jsonl beside each export, or --impl-graphs/--spec-graphs in the args);
#   2. witness-render.py --check on every <outdir>/graphs/*.graph.json, then --format tikz,dot --width 13.9 (and
#      --cols-per-row 3 for a graph of more than three threads), which writes <outdir>/graphs/<rank>.tex and <rank>.dot
#      (nearest graphs: <rank>.nearest-<k>.{tex,dot});
#   3. <outdir>/WITNESS.md: verdict line, uncovered count, index.tsv, per-graph file list, check/render results;
#      <outdir>/summary.txt: key=value lines (exit, uncovered, graphs, check_failures, render_failures).
# usage: witness-join.sh --render <outdir> <join-stdout> <join-stderr> <join-exit> <spec-export> <impl-export> [conf-join args...]
#   steps 2-3 only, after a join that already ran with --witness-dir <outdir>/graphs.
# Exit: conf-join's exit code (0 conforms, 1 violation, 2 refused). A check or render failure is a tool defect: it is
# printed on stderr and in WITNESS.md, and does not change the exit code. Nothing is silent: a missing store is
# reported per uncovered line by conf-join (index.tsv reason no-store / no-graph) and summarised here.
# env: WITNESS_CONF_JOIN  conf-join binary (default $DAAVAR/traceforge/target/release/conf-join, env.sh paths)
#      WITNESS_WIDTH      tikz width in cm (default 13.9)
# The online re-run that would produce graphs for an export without a store (contract §2.2) is only printed, never run.
set -uo pipefail
S="$(cd "$(dirname "$0")" && pwd)"
RENDER_ONLY=0; [ "${1:-}" = --render ] && { RENDER_ONLY=1; shift; }
if [ $RENDER_ONLY = 1 ]; then
  if [ $# -lt 6 ]; then echo "usage: witness-join.sh --render <outdir> <join-stdout> <join-stderr> <join-exit> <spec> <impl> [conf-join args...]" >&2; exit 2; fi
  OUT="$1"; JOUT="$2"; JERR="$3"; RC="$4"; SPEC="$5"; IMPL="$6"; shift 6
else
  if [ $# -lt 3 ]; then echo "usage: witness-join.sh <spec-export> <impl-export> <outdir> [conf-join args...]" >&2; exit 2; fi
  SPEC="$1"; IMPL="$2"; OUT="$3"; shift 3; JOUT="$OUT/join.out"; JERR="$OUT/join.err"
fi
W="${WITNESS_WIDTH:-13.9}"; G="$OUT/graphs"
if [ -e "$OUT/WITNESS.md" ] || { [ $RENDER_ONLY = 0 ] && [ -e "$JOUT" ]; }; then echo "witness-join: $OUT already holds a witness step; use a new directory" >&2; exit 2; fi
mkdir -p "$OUT" || { echo "witness-join: cannot create $OUT" >&2; exit 2; }
IG=""; SG=""; prev=""
for a in "$@"; do case "$prev" in --impl-graphs) IG="$a";; --spec-graphs) SG="$a";; esac; prev="$a"; done
: "${IG:=$IMPL.graphs.jsonl}"; : "${SG:=$SPEC.graphs.jsonl}"

if [ $RENDER_ONLY = 0 ]; then
  if [ -z "${WITNESS_CONF_JOIN:-}" ]; then . "$S/env.sh"; WITNESS_CONF_JOIN="$DAAVAR/traceforge/target/release/conf-join"; fi
  CJ="$WITNESS_CONF_JOIN"
  if [ ! -x "$CJ" ]; then echo "witness-join: conf-join not found or not executable: $CJ (set WITNESS_CONF_JOIN)" >&2; exit 2; fi
  # --decide unless the args already ask for it (conf-join takes the flag once; --decide-budget implies it)
  DEC=(--decide); for a in "$@"; do case "$a" in --decide|--decide-budget) DEC=();; esac; done
  CMD=("$CJ" "${DEC[@]}" --witness-dir "$G" "$@" "$SPEC" "$IMPL")
  printf '%q ' "${CMD[@]}" > "$OUT/join.cmd"; echo >> "$OUT/join.cmd"
  "${CMD[@]}" > "$JOUT" 2> "$JERR" </dev/null
  RC=$?
fi

VERDICT=$(grep -m1 -E '^verdict' "$JOUT" || true)
[ -n "$VERDICT" ] || VERDICT="(no verdict line; conf-join exit $RC: $(head -c 300 "$JERR" | tr '\n' ' '))"
UNC=$(sed -n 's/^uncovered: //p' "$JOUT" | head -1); UNC=${UNC:--}
IDX='-'; [ -f "$G/index.tsv" ] && IDX=$(awk 'NR>1 && NF' "$G/index.tsv" | wc -l)
WM=$(grep -E '^witness_(written|missing|refused): ' "$JOUT" | paste -sd' ' || true)

# check, then render, every graph written
NG=0; NCHK=0; NREN=0; RENDER=""
shopt -s nullglob
for f in "$G"/*.graph.json; do
  NG=$((NG+1)); b=$(basename "$f" .graph.json)
  if ! python3 -I "$S/witness-render.py" --check -q "$f" >> "$OUT/render.log" 2>&1; then
    NCHK=$((NCHK+1)); RENDER+="| $b | check FAILED (render.log) | |"$'\n'; echo "witness-join: $f fails --check (tool defect, see $OUT/render.log)" >&2; continue
  fi
  CPR=(); [ "$(python3 -I -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["threads"]))' "$f" 2>/dev/null || echo 0)" -gt 3 ] && CPR=(--cols-per-row 3)
  if python3 -I "$S/witness-render.py" --format tikz,dot --width "$W" "${CPR[@]}" -q "$f" >> "$OUT/render.log" 2>&1 && [ -f "$G/$b.tex" ] && [ -f "$G/$b.dot" ]; then
    RENDER+="| $b | ok | \`graphs/$b.graph.json\` \`graphs/$b.tex\` \`graphs/$b.dot\` |"$'\n'
  else
    NREN=$((NREN+1)); RENDER+="| $b | render FAILED (render.log) | \`graphs/$b.graph.json\` |"$'\n'; echo "witness-join: rendering $f failed (see $OUT/render.log)" >&2
  fi
done

{
  echo "# Witness step"
  echo
  echo "- spec: \`$SPEC\` (store \`$SG\`: $([ -f "$SG" ] && echo present || echo absent))"
  echo "- impl: \`$IMPL\` (store \`$IG\`: $([ -f "$IG" ] && echo present || echo absent))"
  if [ $RENDER_ONLY = 0 ]; then echo "- command: \`$(sed 's/ $//' "$OUT/join.cmd")\`"; else echo "- join: the row's own conf-join (stdout \`$JOUT\`)"; fi
  echo "- conf-join exit: $RC"
  echo "- verdict: \`$VERDICT\`"
  echo "- uncovered lines: $UNC (index.tsv rows: $IDX)"
  [ -n "$WM" ] && echo "- conf-join: $WM"
  echo "- graphs written: $NG; check failures: $NCHK; render failures: $NREN (tikz width $W cm)"
  if [ -f "$G/index.tsv" ]; then
    echo "- not written, by reason: $(awk -F'\t' 'NR>1 && $5!="ok" {c[$5]++} END {for (r in c) printf "%s %d; ", r, c[r]; if (!length(c)) printf "none"}' "$G/index.tsv")"
  fi
  if [ ! -f "$IG" ]; then
    echo "- no implementation store: the graphs need an export with \`--graphs\`, or the online re-run of contract §2.2"
    echo "  (\`--witness <dir>/ --witness-format json\` on the implementation with \`--spec $SPEC\`). Not run here."
  fi
  echo
  echo "## index.tsv"
  echo
  if [ -f "$G/index.tsv" ]; then echo '```'; cat "$G/index.tsv"; [ -z "$(tail -c1 "$G/index.tsv")" ] || echo; echo '```'; else echo "(none: conf-join wrote no index; see join.err)"; fi
  echo
  echo "## Graphs"
  echo
  echo "| graph | check/render | files |"
  echo "|---|---|---|"
  printf '%s' "$RENDER"
  echo
  if [ $RENDER_ONLY = 0 ]; then echo "Files: \`join.cmd\`, \`join.out\` (conf-join stdout), \`join.err\`, \`render.log\`, \`summary.txt\`."; else echo "Files: \`render.log\`, \`summary.txt\`."; fi
} > "$OUT/WITNESS.md"
printf 'exit=%s\nuncovered=%s\ngraphs=%s\ncheck_failures=%s\nrender_failures=%s\n' "$RC" "$UNC" "$NG" "$NCHK" "$NREN" > "$OUT/summary.txt"

echo "witness-join: exit $RC; $VERDICT; uncovered $UNC; graphs $NG; check failures $NCHK; render failures $NREN; $OUT/WITNESS.md"
[ -f "$IG" ] || echo "witness-join: no implementation store at $IG: every uncovered line is reported no-store (see WITNESS.md)" >&2
[ $RC -eq 2 ] && echo "witness-join: conf-join refused (exit 2): $(head -c 300 "$JERR" | tr '\n' ' ')" >&2
exit $RC
