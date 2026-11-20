# Scripts (the evaluation's runner; also the artifact's reproduction scripts)

Rules they implement: `plan/EVAL-RULES.md` R1–R9 of the project. Paths come from `env.sh`.

| script | what |
|---|---|
| `env.sh` | paths on a node (`DAAVAR`, `TF_PIN` = TraceForge short sha, `EVAL_OUT` = `out-<pin>`, `BRAIN_DATA`, `TFX`, `JMCX`, JDK, cargo); sourced by all |
| `setup-node.sh` | once per node after `git pull`: build TraceForge (eval child + `conf-join`), JMC (+ classpath file), every benchmark; exports `TRACEFORGE_COMMIT`/`JMC_COMMIT` from the submodule pins and writes each benchmark's `SOURCE.md5` |
| `source-manifest.sh rust\|java <dir> <dest>` | a benchmark's source manifest (witness contract §1.2.1): `# root <daavar-relative dir>`, then `md5sum` lines sorted by path |
| `start-queue.sh <rows> <name> [slots]` | start a queue detached (`setsid nohup`); survives ssh/VPN drops |
| `run-queue.sh` | the queue: NUMA-pinned slots (1/2/4 nodes by `-t32`/`-t64` in the id), `ulimit -v`, `timeout --signal=KILL`, `/usr/bin/time -v`, status file, load log, record, publish |
| `status.sh [name]` | queues alive, rows running, results by stop reason |
| `mkrecord.py <rowdir>` | `record.json` + the `results.tsv` line from status, time -v, load log and the tool's stdout |
| `publish.sh <rowdir>` | copy the row into the brain-data clone and append its results line |
| `start-flusher.sh [s]`, `flush-results.sh` | one commit + push per node every 10 min (default) instead of per row |
| `jmc-run.sh`, `rust-run.sh` | run one benchmark program (JMC launcher / Rust binary) inside a row directory |
| `gen-rows-benchmarks.sh <outdir>` | rows for the 19 literature benchmarks from the builders' run logs |
| `witness-join.sh <spec-export> <impl-export> <outdir> [conf-join args...]` | witness step (contract §5): `conf-join --decide --witness-dir <outdir>/graphs`, `witness-render.py --check` on every graph, then `<outdir>/graphs/<rank>.{tex,dot}` (tikz `--width 13.9`, `--cols-per-row 3` above three threads), `<outdir>/WITNESS.md` (verdict, uncovered count, `index.tsv`, graph files) and `summary.txt`; exits with conf-join's code; a missing store is reported per line (`no-store`), the online re-run is only named. Binary: `WITNESS_CONF_JOIN`, else `$DAAVAR/traceforge/target/release/conf-join`. `--render <outdir> <stdout> <stderr> <exit> <spec> <impl>`: steps after a join that already ran |
| `run-queue.sh` witness hook | a `conf-join` row runs once with `--witness-dir <rowdir>/witness/graphs` added, then `witness-join.sh --render` (log `<rowdir>/witness.log`, one queue-log line) before the record; `mkrecord.py` adds `witness_{exit,uncovered,graphs,check_failures,render_failures}`; verdict and reason never change. `WITNESS=1`/`0` forces it (0: the row runs as before); unset, on when the impl export has a store (`<impl>.graphs.jsonl` or `--impl-graphs`); rows with their own `--witness-dir` are left alone |
| `gen-rows-benchmarks.sh` stores | export rows get `--graphs export.jsonl.graphs.jsonl` (spec exports also `--graphs-limit 0`), default probed once per tool (the flag's literal in a built benchmark binary / `MpLauncher.class`, else the pinned source; neither CLI has `--help`), logged on stderr, forced with `GRAPHS=1`/`0`; `mkrecord.py` records `graphs_written`/`graphs_refused`/`graphs_ms` from the `graph_store:` line and keeps stores out of `summaries_paths` |
| `print-classpath.gradle` | Gradle init script that writes JMC's test classpath |
| `schema8-header.csv` | the 262 column names of TraceForge's `EVALROW` record |

Row file format: `<id> <memGB> <timeoutS> <command...>`; a sidecar `<rows>.tsv` gives each id's attributes
(`id table family size model variant engine threads expected_verdict note tier [kind spec_tool impl_tool spec_lang impl_lang mode exploration benchmark]`).
TraceForge's rows (T1–T4) come from the lead (`plan/traceForge/cluster/`); benchmark rows from `gen-rows-benchmarks.sh`.

Node day one: `git pull && git submodule update --init`, `scripts/setup-node.sh`, clone brain-data to `$BRAIN_DATA`,
`scripts/start-queue.sh cluster/smoke.rows smoke`, `scripts/status.sh`.

Pins: every output tree, results file (`results/<node>.<pin>.tsv`) and run dir (`runs/<node>/<pin>/`) is per TraceForge pin, so a rerun on a new pin never overwrites or skips an old row. `NUMA_SET=3,4,5,6,7` lets a second queue use the idle NUMA nodes while another queue finishes.

Source manifests: `setup-node.sh` rewrites a benchmark's `SOURCE.md5` right after each successful build of it (Rust: one
`<bin>.SOURCE.md5` per bin beside the binary in `$EVAL_OUT/bench-target/release`; Java: in `$EVAL_OUT/javaout/<bench>`),
so they are rebuilt whenever setup-node runs. A failed build leaves the old manifest; a binary or class file rebuilt after
it makes it stale, and the tools then record `"source": "unknown"`. A benchmark built any other way (a study's own
`cargo build`) needs its own `source-manifest.sh` call after the build, or its graphs carry `"unknown"`.
