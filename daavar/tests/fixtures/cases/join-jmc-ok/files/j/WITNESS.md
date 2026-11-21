# Witness step

- spec: `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl.graphs.jsonl`: present)
- impl: `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-ok/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-ok/export.jsonl.graphs.jsonl`: present)
- command: `/local/mkhoshechin/tmp/xcheck-pins/tf/target/release/conf-join --witness-dir j/graphs --decide-budget 1000000 --witness-limit 20 --witness-nearest 3 /local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl /local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-ok/export.jsonl`
- conf-join exit: 0
- verdict: `verdict: conforms`
- uncovered lines: 0 (index.tsv rows: 0)
- conf-join: witness_written: 0 witness_missing: 0 witness_refused: 0
- graphs written: 0; check failures: 0; render failures: 0 (tikz width 13.9 cm)
- not written, by reason: none

## index.tsv

```
n	class	decide	graph	reason	line
```

## Graphs

| graph | check/render | files |
|---|---|---|

Files: `join.cmd`, `join.out` (conf-join stdout), `join.err`, `render.log`, `summary.txt`.
