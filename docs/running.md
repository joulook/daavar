# Running a check (draft 3, 2026-11-21)

Everything a check needs is one command of the driver, `bin/daavar`, run from the repository root. Python 3.11 or
newer; the two checkers are built on first use from the pinned submodules (a Rust toolchain and JDK 17 required, both
offline-capable once their dependency caches are present).

Where the driver writes: a check writes under the project's `out/`; `render` of a graph inside a run writes into
that project's `out/renders/<id>/`, and of a graph elsewhere beside the graph; `replay` of a graph inside a run
writes into that project's `out/replays/<id>/`, and of a graph elsewhere into `out/replays/<id>/` under the current
directory. Builds and cached exports go to the cache (`~/.cache/daavar/`, or `DAAVAR_CACHE`). `new` writes the
project it creates, `check --from-run` the directory it restores into, and `render --out-dir` that directory. Cargo
and Gradle keep their own dependency caches (`CARGO_HOME`, `GRADLE_USER_HOME`). The tool checkouts are not written.

## The commands
| Command | What it does |
|---|---|
| `bin/daavar new <name> [--impl rust\|java\|rust,java] [--dir D]` | scaffolds a project: a Rust specification, a Rust and/or Java implementation, `MAP.md`, `SPEC-DESIGN.md`, `README.md` and `daavar.toml` (a Java specification is refused in release 1) |
| `bin/daavar build [dir] [--allow-dirty-tools]` | builds the tools and the project's programs; a second build is a no-op |
| `bin/daavar vocab [dir] [--values]` | compares the message vocabulary of the Rust and Java programs (tags and field arity; `--values` adds payload enum constants); exit 0 identical or one language only, 1 different, 2 unparsable |
| `bin/daavar check [dir] [...]` | exports, joins and judges every selected direction; writes `out/<run>/` |
| `bin/daavar render <graph, store or dir> [--format tikz,dot,text] [--out-dir D]` | checks and renders witness graphs; a graph inside a run is rendered into `out/renders/<id>/`; exit 0 ok, 1 a graph does not match its summary line, 2 invalid |
| `bin/daavar replay <graph.json> [--allow-source-change] [--allow-tool-change]` | replays a witness graph against the program that produced it, in `out/replays/<id>/`; exit 0 reproduced, 1 deviation, 2 refused |

Every command prints one line on stdout, `daavar <command>: exit <n> <STATUS> [path]`, and accepts `--json` (one JSON
envelope instead), `--dry-run` and `--debug` (a traceback on an unexpected error; otherwise one line on stderr and
exit 2). `--dry-run` runs nothing: `build --dry-run` prints the build commands on stderr (in `result.commands` with
`--json`); `check --dry-run` prints every command a check would run (the builds, then per cell the exports, the
online checks and the joins, with placeholders such as `<RUST_ARTIFACT>` and `out/<run>/` for what only a build
or a run can tell); `new --dry-run` validates the name and lists the files it would write; `render` and `replay`
print their commands; `check --from-run R --dry-run` prints the restore steps (the clones, checkouts and patches it
would apply) and the check it would rerun. The commands go to stderr, or to `result.commands` with `--json`.

## `check`
`bin/daavar check examples/kv-cancel` runs the whole matrix of the project's `daavar.toml`: every scenario, every cell,
every configured direction. Narrow it with `--scenario`, `--cell`, `--variant`, `--reading`, `--split` or a general
`--where field=values`; an unknown name lists the valid ones. `--model fifo|bag|causal` overrides the model (`causal`
is Rust-only); `--directions checked<=ref,...` keeps a subset of the cells' directions. `--mode online` checks each
implementation and mutant against the specification's export during its exploration instead of joining two
exports (implementation-against-implementation directions still join two exports); `--stop-at-first` stops at the
first uncovered summary (online only; refused with offline mode and with more than one TraceForge thread);
`--no-stop-at-first` overrides `stop_at_first = true` in `daavar.toml`.

`--no-build` uses the cached build only and refuses if anything is missing. `--strict` refuses the check when the
vocabulary comparison does not exit 0 (without it, a difference is a warning). `--no-wait` refuses instead of
waiting for another check of the same project. `--allow-dirty-tools` accepts uncommitted tool checkouts and keeps
their sources in the run. `--from-run R [--into D]` reproduces the run R from its preserved inputs in a fresh
directory D (by default `./restore-<run>`), rebuilds with an empty cache and compares the results. It uses the run's own selection and options:
with `--from-run`, only `--into`, `--json`, `--dry-run` and `--debug` are accepted; a project directory, any
selection, `--model`, `--mode`, `--stop-at-first`, `--directions`, `--no-build`, `--strict`, `--no-wait` and
`--allow-dirty-tools` are refused (exit 2). `--into` without `--from-run` is refused too.

The order of work inside a check: build; the vocabulary comparison; the reference exports (cached by identity: tool
commit, build key, source manifest, arguments, header metadata, model, budgets and exploration flags); a check of
their headers (model and visible roles); the checked exports with their graph stores (offline, or online against the
reference); the joins with Decide and the witness graphs; the verdict files. A program that refuses its arguments
is reported when its export runs.

## Exit codes and verdicts
Each direction gets one verdict: conforms, violation, incomplete or refused.
- **Conforms**: both exports complete and no uncovered line, or every uncovered line a Decide false alarm.
- **Violation**: a Decide-confirmed uncovered line against a complete reference, or a failure (assertion, panic,
  exception) of the checked program.
- **Incomplete**: a bounded, stopped or timed-out export, an incomplete reference, an unresolved Decide, a timed-out join.
- **Refused**: a shape rule, a usage error, an unsupported value, a model or role mismatch, a failure of the reference
  program, a refused join.

Gating directions are `impl<=spec` and a mutant against its reference; implementation-against-implementation (twin)
directions are reported, never gating. For a mutant, a violation means "rejected" and counts as conforms; a
conforming mutant is "not rejected" and counts as incomplete. Inside one direction the worse of the two sides
decides, in the order refused, violation, incomplete, conforms. Over the gating directions of all cells, a violation
anywhere decides the run:

| Exit | Status |
|---|---|
| 0 | CONFORMS: every gating direction conforms (rejected mutants included) |
| 1 | VIOLATION: at least one gating direction is a violation, whatever the others are |
| 2 | no violation, and a gating direction is refused (REFUSED) or incomplete (INCOMPLETE); or REPORT ONLY (no gating direction selected); or a check refused before any direction (VERDICT.md then gives the reason) |

`--from-run` exits with the restored run's verdict, or 2 `NOT-REPRODUCED` when a compared result differs.

## `out/<run>/`
```
out/<run>/
  VERDICT.md  summary.tsv  verdict.json  provenance.json  config.resolved.toml  commands.sh  driver.log
  inputs/                         # manifests/, the project (snapshot, or commit + patch + untracked), dirty tools
  <scenario>.<cell>/
    exports/                      # <program>[.online-<ref>].jsonl, its graph store, stdout, stderr, command
    work-<program>/               # each export's working directory
    joins/<checked>-in-<ref>/     # join.out, WITNESS.md, nearest.diff
      graphs/                     # index.tsv, <n>.graph.json, <n>.tex, <n>.dot, <n>.witness.txt
out/latest                        # the last finished run
```
`VERDICT.md` has one table per cell (direction, gating, enumerations, uncovered/lines, Decide counts, graphs,
verdict), then each uncovered line with its class (when a classifier is configured), the nearest word of the
direction's reference (searched for the first `witness_limit` uncovered lines, at most 200 000 reference lines and
60 s per direction; the report says when it stopped short) and links to the witness graph and its renderings, then the
refusals and failures. `summary.tsv` has one row per direction; `verdict.json` has the same, machine-readable.
`provenance.json` records the tool commits, build and export keys, artifact digests and every command. A run can be
restored elsewhere with `--from-run`.

## By hand
The driver wraps the tools' own commands: `commands.sh` in every run lists them with their working directories, and
the README of `examples/kv-cancel` shows them typed out (count, export, join). `check --dry-run` prints them before
a run.
