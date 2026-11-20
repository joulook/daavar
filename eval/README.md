# The PLDI 2027 evaluation

The paper's runs were made from the state tagged `pldi27-artifact` (daavar `ceac1e1`, 2026-11-12; submodule pins JMC
`648e9c83`, TraceForge `0722274a`); the records name the pin each run used. Everything named below as `scripts/...`
or `cluster/...` exists at that tag, not on the main branch. The tag preserves the paper's snapshot; files changed
after it and removed later are in this branch's history (last at `d22a22c`).

**Table 1 is rebuilt from the tag, the inputs in this directory, and the pinned records** (verified 2026-11-20,
byte-identical). Tables 2 and 3 are not yet reproduced from this repository: their generator `pairs-table.py`, the
row files `rows-pairs-b.tsv` and `rows-pairs-c.tsv`, the Decide results and the TiKV CSV are not here yet.

## How the data was produced

The records are historical and are not rewritten: their commands name the Java programs of the context-free
rewrites and ports `cf-<dir>` (compiled from `benchmarks-cf/<dir>/java` at the tag). On main those programs are
`benchmarks/<dir>/context-free/java` or `benchmarks/<dir>/port-java/java`, built as `<dir>@context-free` or
`<dir>@port-java`.

1. **Environment.** `scripts/env.sh` at the tag: one checkout per node of the evaluation cluster, outputs in
   `out-<TraceForge pin>`, JDK 17 (Temurin), a clone of the brain-data repository at `BRAIN_DATA`.
   `scripts/setup-node.sh` built TraceForge, JMC (with its classpath file) and every benchmark on the node.
2. **Rows.** A row is one run: `<id> <memGB> <timeoutS> <command...>` in a `cluster/**/*.rows` file, with a sidecar
   `<rows>.tsv` giving each id's attributes (table, family, size, model, variant, engine, expected verdict, ...).
   The row files were written by `scripts/gen-rows-*`, `gen-scaling-*`, `gen-study-rows.py` and by hand.
3. **Runs and records.** `scripts/start-queue.sh` / `run-queue.sh` ran the rows on the nodes (NUMA-pinned slots,
   memory and time limits); `scripts/mkrecord.py` wrote each row's `record.json` and its line of `results.tsv`;
   `scripts/publish.sh` and `flush-results.sh` pushed them to brain-data, one file per node and pin:
   `results/<node>.<pin>.tsv` (columns: `SCHEMA.md` there).
4. **Collector.** `scripts/tables.py <brain-data> <daavar> <out>` reads every record and joins it with the row
   sidecars under `cluster/`; it writes the campaign CSVs.
5. **Generators.** `scripts/paper-tables.py <out> --drop dropped.keep` writes `table2.tex` (conforming pairs) and
   `table3.tex` (seeded faults). Table 1 is written by `engines-table.py` from these two files, the row file
   `rows-engines.tsv` (one line per printed pair, naming its records) and the records.

## Inputs in this directory

| file | md5 | what |
|---|---|---|
| `engines-table.py` | `fc3b99938ac868e582122d684d178730` | Table 1's generator (usage in its docstring) |
| `rows-engines.tsv` | `34887aad66d09efbb14ed07ad2d09f91` | Table 1's rows and the run ids behind each |
| `dropped.keep` | `efb685da257f354243ab44ada02d7fe7` | the benchmarks left out of the paper's tables (`--drop`) |

They are copied unchanged from the paper's working files (their comments name internal planning documents).

## Table 1, step by step

Needs git, Python 3.11 with `openpyxl` (3.1.5 was used), a few tens of MB of scratch space. Variables:
`DV` = this daavar clone, `W` = a scratch worktree of the tag, `BD` = a scratch directory for the records,
`T` = a scratch output directory, `EVAL="$DV/eval"`. The records are brain-data
(`https://github.com/joulook/brain-data`) at commit `a7d5731` (the paper's final data audit, 2026-11-13); its later
commit `238a8a4a6` (2026-11-19) gives the same Table 1.

```
git -C "$DV" worktree add --detach "$W" pldi27-artifact
git clone https://github.com/joulook/brain-data "$BD.git"          # or an existing clone
mkdir -p "$BD" "$T"; git -C "$BD.git" archive a7d5731 results results.tsv | tar -x -C "$BD"
python3 -I "$W/scripts/tables.py" "$BD" "$W" "$T"
python3 -I "$W/scripts/paper-tables.py" "$T" --drop "$EVAL/dropped.keep"
python3 -I "$EVAL/engines-table.py" "$T/table2.tex" "$T/table3.tex" "$EVAL/rows-engines.tsv" 8 \
        "$T/engines.tex" "$BD/results" "$T/engines.check.tsv"
git -C "$DV" worktree remove "$W"
```

`$T/engines.tex` (md5 `16ca8798c15294456f5d4f251bc2591e`) is the paper's `tex/tables/eval2/engines.tex`, byte for
byte; `engines.check.tsv` names the record and the field behind every cell. 8 is the time limit in hours.
