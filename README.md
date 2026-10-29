# Daavar

*Dāvar* (داور): the judge, the arbiter; from the older *dādvar*, the one who judges by law.

Daavar judges refinement across languages: given a distributed protocol written in Java and
one written in Rust, it decides whether one refines the other, that is, whether every behaviour
the visible parts of one program can show is a behaviour of the other. The programs are written
against the same message-passing model, the Must API, and the comparison is on execution
graphs: the values the visible threads send and receive, their final status, and the causal
order among them.

Each side is explored exhaustively by a DPOR model checker for its language, JMC for Java and
TraceForge for Rust, each running the Must algorithm. The checkers are the instruments; the
verdict is about the two programs. Both export the same compact summaries of their execution
graphs, and one join over the two exports gives the verdict, in either direction.

## Layout

| path | what |
|---|---|
| `traceforge/` | submodule: TraceForge with the conformance engines (stateful, stateless, parallel) and `conf-join`; pinned |
| `jmc/` | submodule: JMC with the Must API, the Must DPOR strategy, the summary exporter and the online check; pinned |
| `format/` | the compact-summary interchange format (`SUMMARY-FORMAT.md`) and golden export files |
| `benchmarks/` | one directory per benchmark: the source, the Rust and Java Must-API versions, a README with the expected verdict |
| `scripts/` | run the specification side, the implementation side, the join, and the tables |

## The check, in three commands

1. Explore the program taken as the specification to exhaustion with its checker and export
   its summaries.
2. Explore the program taken as the implementation with its checker and export its summaries.
3. `traceforge/target/release/conf-join spec.jsonl impl.jsonl` prints the implementation
   summaries the specification does not cover; none means the implementation refines the
   specification.

Java against Rust or Rust against Java: the direction is the order of the two files. A
`sampled` file (JMC's random strategy) is accepted as the implementation side only.

## Pins

The submodules are pinned on purpose; a bump is a commit that names the new commit. The
`bump-submodules` workflow moves them to their branches' newest commits every 30 minutes; tags
mark the states the paper's tables were produced from.

## Status

Skeleton (2026-10-29). Benchmarks and scripts follow.
