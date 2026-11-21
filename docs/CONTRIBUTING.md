# Contributing (draft 1, 2026-11-20)

## Layout
- `benchmarks/<name>/` one directory per protocol or system checked. The convention for new and migrated
  benchmarks (the case studies and `examples/kv-cancel` follow it): `rust/` (the TraceForge crate: `msgs.rs`,
  `env.rs`, `spec.rs`, `imp.rs`, `main.rs`), `java/` (the JMC program), `README.md`, `MAP.md` and `SPEC-DESIGN.md`
  for case studies, `daavar.toml`. Older corpus entries keep their own layouts and have no `daavar.toml` yet; genuine
  variants of a program (`context-free/`, `port-java/`, mutants) live inside
  the benchmark's directory, never in a parallel tree.
- `examples/` the small, documented entry points (`kv-cancel` first).
- `format/` `SPEC.md` (normative), `HISTORY.md` (the decision record), `golden/` (reference JMC exports of the
  A2Store and A3Lock programs, used as regression files for the exporter; they are not cross-tool tests).
- `docs/` this guide. `daavar/`, `bin/daavar` the driver. `scripts/` the helpers the driver and the documentation
  rely on (witness rendering and joining, source manifests, the two one-line tool runners, node setup).
- `jmc/`, `traceforge/` the two tools, pinned as submodules; `eval/` how the paper's tables are rebuilt from the tag.

## The format is versioned in lock-step
The summary export format is shared by both tools. A change to it is one change in three places: `format/SPEC.md`,
the JMC exporter, the TraceForge exporter, plus updated regression files under `format/golden/`. Cross-tool agreement is checked by
exporting the same program with both tools and comparing the common words, statuses and counts as `SPEC.md` defines
them (the header carries each tool's identity, so whole files are not byte-equal). Minor version bumps add fields; nothing is removed or re-encoded without a major bump.
Open a change with the specification text first; the two tool changes follow and are checked against each other with
the cross-tool check before the pins move (`format/SPEC.md` §§2-5 define the headers and canonical summaries both tools write, §6.3 the pairing restrictions, §11 the interoperability coverage).

## Adding a benchmark
1. Copy `examples/kv-cancel` (`bin/daavar new` is forthcoming), then follow `docs/modelling.md` and `docs/spec-derivation.md`.
2. Keep the message vocabulary in one file and mirror it in Java field for field; run `bin/daavar vocab`.
3. `bin/daavar check` at the smallest size; grow one parameter at a time; record sizes and counts in the README.
4. Commit the crate's `Cargo.lock` (builds use `--locked`).
5. A case study comes with its map and its specification design document; a finding is reported only after the
   verification round described in `docs/interpreting.md`.

## Adding a communication model or a value kind
Both tools must implement it and agree on the encoding; the specification text and a golden file come first. The
model names are `fifo`, `bag` and `causal` (TraceForge) with the JMC spellings `p2p`, `asyn` and `cd`; JMC's Must
strategy supports the first two, `causal` is Rust-only (see `docs/concepts.md`).

## Pins and releases
The tools are pinned by commit in the submodules. A pin moves only when both tools pass the cross-tool check on the
pinned binaries; the daavar commit that moves a pin says why. When the TraceForge pin changes the crate's version, the
`traceforge` entry of every benchmark and example `Cargo.lock` is updated in the same commit (builds run with
`--locked`). A release of the framework tags daavar, names the two
tool versions (the JMC jar on Maven Central, the TraceForge crate tag), and ships the examples checked end to end on
the tagged pins.

## Style
Short sentences; no internal document identifiers or ruling numbers in anything a user reads; every number in a
README or a result is a run output; scripts take their paths from arguments or the configuration, never from a
personal or node path.
