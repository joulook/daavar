# Witness step

- spec: `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl.graphs.jsonl`: present)
- impl: `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug/export.jsonl` (store `/local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug/export.jsonl.graphs.jsonl`: present)
- command: `/local/mkhoshechin/tmp/xcheck-pins/tf/target/release/conf-join --witness-dir j/graphs --merge --decide-budget 1000000 --witness-limit 20 --witness-nearest 3 /local/mkhoshechin/driver-dev/work/cases/tf-export-spec-ok/export.jsonl /local/mkhoshechin/driver-dev/work/cases/jmc-export-impl-bug/export.jsonl`
- conf-join exit: 1
- verdict: `verdict: violation`
- uncovered lines: 1 (index.tsv rows: 1)
- conf-join: witness_written: 1 witness_missing: 0 witness_refused: 0
- graphs written: 1; check failures: 0; render failures: 0 (tikz width 13.9 cm)
- not written, by reason: none

## index.tsv

```
n	class	decide	graph	reason	line
1	no-slot	violation	1.graph.json	ok	{"statuses":{"client":"done","server":"done"},"words":{"client":[{"s":{"t":"Ping","f":[1]}},{"r":{"t":"Pong","f":[2]}},{"s":{"t":"Shutdown","f":[]}}],"server":[{"r":{"t":"Ping","f":[1]}},{"s":{"t":"Pong","f":[2]}},{"r":{"t":"Shutdown","f":[]}}]},"counts":[0,2,2,1,1,3]}
```

## Graphs

| graph | check/render | files |
|---|---|---|
| 1 | ok | `graphs/1.graph.json` `graphs/1.tex` `graphs/1.dot` |

Files: `join.cmd`, `join.out` (conf-join stdout), `join.err`, `render.log`, `summary.txt`.
