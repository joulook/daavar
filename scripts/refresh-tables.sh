#!/usr/bin/env bash
# Keep the paper's tables current: every INTERVAL seconds pull brain-data, rebuild the tables and the
# raw workbook, recompile the PDF. Runs on the coordinator's workstation, detached.
# usage: refresh-tables.sh <brain-data> <daavar> <pldi-dir> [intervalS=1800]
set -uo pipefail
BD="$1"; DV="$2"; PL="$3"; IV="${4:-1800}"
PY=/local/home/mkhoshechin/tmp/venv-tables/bin/python; [ -x "$PY" ] || PY=python3
while :; do
  (cd "$BD" && git pull -q --rebase >/dev/null 2>&1)
  "$PY" -I "$DV/scripts/tables.py" "$BD" "$DV" "$PL/tex/tables" > "$PL/tex/tables/REFRESH.log" 2>&1
  DT="$PL/../plan/dropped-tables"   # the paper's tables leave out the dropped benchmarks (dropped.keep); the review document keeps only them
  DROP=""; [ -f "$DT/dropped.keep" ] && DROP="--drop $DT/dropped.keep"
  "$PY" -I "$DV/scripts/paper-tables.py" "$PL/tex/tables" $DROP >> "$PL/tex/tables/REFRESH.log" 2>&1
  "$PY" -I "$DV/scripts/paper-tables.py" "$PL/tex/tables" --full $DROP >> "$PL/tex/tables/REFRESH.log" 2>&1
  (cd "$PL" && latexmk -g -pdf -interaction=nonstopmode -halt-on-error main.tex >/dev/null 2>&1 && echo "$(date -Is) tables refreshed, pdf built" >> "$PL/tex/tables/REFRESH.log" || echo "$(date -Is) LATEX FAILED" >> "$PL/tex/tables/REFRESH.log")
  # the benchmark book (owner, 2026-11-03): per-benchmark evaluation documents, the groups that exist
  for g in synthetic application realsw agentic; do "$PY" -I "$DV/scripts/bench-book.py" "$BD" "$DV" "$PL/tex/tables" $g --build >> "$PL/tex/tables/REFRESH.log" 2>&1; done
  # the dropped-benchmarks review document (owner, 2026-11-02 21:25): same records, the families of dropped.keep only
  if [ -f "$DT/dropped.keep" ]; then
    "$PY" -I "$DV/scripts/paper-tables.py" "$PL/tex/tables" --keep "$DT/dropped.keep" --out "$DT/tables" >> "$PL/tex/tables/REFRESH.log" 2>&1
    "$PY" -I "$DV/scripts/paper-tables.py" "$PL/tex/tables" --full --keep "$DT/dropped.keep" --out "$DT/tables" >> "$PL/tex/tables/REFRESH.log" 2>&1
    (cd "$DT" && latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex >/dev/null 2>&1 && latexmk -c >/dev/null 2>&1 && echo "$(date -Is) dropped tables refreshed" >> "$PL/tex/tables/REFRESH.log" || echo "$(date -Is) DROPPED LATEX FAILED" >> "$PL/tex/tables/REFRESH.log")
  fi
  sleep "$IV"
done
