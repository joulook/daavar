# Daavar

*Dāvar* (داور): the judge, the arbiter; from the older *dādvar*, the one who judges by law.

Daavar is one judge for two model checkers. It holds, pinned at exact commits, the two tools
that explore Must-API programs, the summary format both export, the benchmarks written against
both, and the scripts that run the cross-language refinement check between a Java program
explored by JMC and a Rust program explored by TraceForge.

## Layout

| path | what |
|---|---|
| `traceforge/` | submodule: TraceForge with the conformance engines (stateful, stateless, parallel) and `conf-join`; pinned |
| `jmc/` | submodule: JMC with the Must API, the Must DPOR strategy, the summary exporter and the online check; pinned |
| `format/` | the compact-summary interchange format (`SUMMARY-FORMAT.md`) and golden export files |
| `benchmarks/` | one directory per benchmark: the source, the Rust and Java Must-API versions, a README with the expected verdict |
| `scripts/` | run the specification side, the implementation side, the join, and the tables |

## The check, in three commands

1. Run the specification program to exhaustion in tool A and export its summaries.
2. Run the implementation program in tool B and export its summaries.
3. `traceforge/target/release/conf-join spec.jsonl impl.jsonl` prints the implementation
   summaries the specification does not cover; none means the implementation refines the
   specification.

The direction is the order of the two files. A `sampled` file (JMC's random strategy) is
accepted as the implementation side only.

## Pins

The submodules are pinned on purpose; a bump is a commit that names the new commit. The
`bump-submodules` workflow moves them to their branches' newest commits every 30 minutes; tags
mark the states the paper's tables were produced from.

## Status

Skeleton (2026-10-29). Benchmarks and scripts follow.
