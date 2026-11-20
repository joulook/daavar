# Benchmarks

Every program the conformance check has been evaluated on, one directory per benchmark or case study. A directory
holds the Rust crate (`rust/`), the Java programs (`java/`), or both, and a README (or `rust/NOTES.md`) with the
protocol, the specification and the implementation, the variants and their expected verdicts, and the commands
that were run. Copied source material, where there is any, is in `source/` with its provenance. Variants of a
program (other ports, mutants, twins) live in its directory or in a directory named after it. Two kinds of
variant have their own subdirectory: `context-free/` holds a context-free rewrite (an agent wrote it from the Rust
program, the paper or a specification alone) and `port-java/` a Java port of existing code; each has its `java/`
sources and a NOTES.md that opens with its provenance. The evaluation's records name these programs `cf-<dir>`; the
records are historical and keep that name.

Build everything with `scripts/setup-node.sh`, then run one side with `scripts/rust-run.sh <dir> <args...>` or
`scripts/jmc-run.sh <dir> '<Class:args>' <strategy> <iterations> <seed> <export|-> <timeoutS>`, and join two
exports with `scripts/witness-join.sh` (see `scripts/README.md`). `_tools/README-TEMPLATE.md` is the outline of a
benchmark README.

`a2a-task` and `mcp-cancel` are the two agent-protocol case studies (A2A task lifecycle, MCP stdio cancellation).
