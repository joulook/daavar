# Witness step

- spec: `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug-bounded/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug-bounded/export.jsonl.graphs.jsonl`: present)
- impl: `/local/mkhoshechin/driver-dev/work/cases/tf-export-impl-ok/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/tf-export-impl-ok/export.jsonl.graphs.jsonl`: present)
- command: `/local/mkhoshechin/tmp/xcheck-pins/tf/target/release/conf-join --witness-dir j/graphs --decide-budget 1000000 --witness-limit 20 --witness-nearest 3 /local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug-bounded/export.jsonl /local/mkhoshechin/driver-dev/work/cases/tf-export-impl-ok/export.jsonl`
- conf-join exit: 2
- verdict: `(no verdict line; conf-join exit 2: conf-join: refused: /local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug-bounded/export.jsonl: the specification is `bounded`, not `complete` )`
- uncovered lines: - (index.tsv rows: -)
- graphs written: 0; check failures: 0; render failures: 0 (tikz width 13.9 cm)

## index.tsv

(none: conf-join wrote no index; see join.err)

## Graphs

| graph | check/render | files |
|---|---|---|

Files: `join.cmd`, `join.out` (conf-join stdout), `join.err`, `render.log`, `summary.txt`.
