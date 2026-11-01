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
  (cd "$PL" && latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex >/dev/null 2>&1 && echo "$(date -Is) tables refreshed, pdf built" >> "$PL/tex/tables/REFRESH.log" || echo "$(date -Is) LATEX FAILED" >> "$PL/tex/tables/REFRESH.log")
  sleep "$IV"
done
