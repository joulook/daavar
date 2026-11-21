# Witness step

- spec: `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl.graphs.jsonl`: present)
- impl: `/local/mkhoshechin/driver-dev/work/cases/tf-export-impl-bug-bag/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/tf-export-impl-bug-bag/export.jsonl.graphs.jsonl`: present)
- command: `/local/mkhoshechin/tmp/xcheck-pins/tf/target/release/conf-join --witness-dir j/graphs --decide-budget 1000000 --witness-limit 20 --witness-nearest 3 /local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl /local/mkhoshechin/driver-dev/work/cases/tf-export-impl-bug-bag/export.jsonl`
- conf-join exit: 2
- verdict: `(no verdict line; conf-join exit 2: conf-join: refused: the two models differ: p2p and asyn )`
- uncovered lines: - (index.tsv rows: -)
- graphs written: 0; check failures: 0; render failures: 0 (tikz width 13.9 cm)

## index.tsv

(none: conf-join wrote no index; see join.err)

## Graphs

| graph | check/render | files |
|---|---|---|

Files: `join.cmd`, `join.out` (conf-join stdout), `join.err`, `render.log`, `summary.txt`.
