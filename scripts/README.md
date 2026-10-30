# Scripts (the evaluation's runner; also the artifact's reproduction scripts)

Rules they implement: `plan/EVAL-RULES.md` R1–R9 of the project. Paths come from `env.sh`.

| script | what |
|---|---|
| `env.sh` | paths on a node (`DAAVAR`, `EVAL_OUT`, `BRAIN_DATA`, `TFX`, `JMCX`, JDK, cargo); sourced by all |
| `setup-node.sh` | once per node after `git pull`: build TraceForge (eval child + `conf-join`), JMC (+ classpath file), every benchmark |
| `start-queue.sh <rows> <name> [slots]` | start a queue detached (`setsid nohup`); survives ssh/VPN drops |
| `run-queue.sh` | the queue: NUMA-pinned slots (1/2/4 nodes by `-t32`/`-t64` in the id), `ulimit -v`, `timeout --signal=KILL`, `/usr/bin/time -v`, status file, load log, record, publish |
| `status.sh [name]` | queues alive, rows running, results by stop reason |
| `mkrecord.py <rowdir>` | `record.json` + the `results.tsv` line from status, time -v, load log and the tool's stdout |
| `publish.sh <rowdir>` | copy the row into the brain-data clone and append its results line |
| `start-flusher.sh [s]`, `flush-results.sh` | one commit + push per node every 10 min (default) instead of per row |
| `jmc-run.sh`, `rust-run.sh` | run one benchmark program (JMC launcher / Rust binary) inside a row directory |
| `gen-rows-benchmarks.sh <outdir>` | rows for the 19 literature benchmarks from the builders' run logs |
| `print-classpath.gradle` | Gradle init script that writes JMC's test classpath |
| `schema8-header.csv` | the 262 column names of TraceForge's `EVALROW` record |

Row file format: `<id> <memGB> <timeoutS> <command...>`; a sidecar `<rows>.tsv` gives each id's attributes
(`id table family size model variant engine threads expected_verdict note tier [kind spec_tool impl_tool spec_lang impl_lang mode exploration benchmark]`).
TraceForge's rows (T1–T4) come from the lead (`plan/traceForge/cluster/`); benchmark rows from `gen-rows-benchmarks.sh`.

Node day one: `git pull && git submodule update --init`, `scripts/setup-node.sh`, clone brain-data to `$BRAIN_DATA`,
`scripts/start-queue.sh cluster/smoke.rows smoke`, `scripts/status.sh`.
