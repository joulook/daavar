# Daavar

*Dāvar* (داور): the judge, the arbiter.

Daavar is a conformance framework for distributed protocols: it checks that an implementation, written in Java or
Rust, conforms to a specification program, written in either language. Both programs are written against the same
message-passing model, the Must API, and the comparison is on execution graphs: the values the visible threads send
and receive, their final status, and the causal order among them.

## The two tools

Each program is explored exhaustively by a DPOR model checker for its language, running the Must algorithm:

- **JMC** (`jmc/`, submodule) for Java: the Must API, the Must strategy, the summary exporter and the online check.
- **TraceForge** (`traceforge/`, submodule) for Rust: the Must API, the conformance engines and `conf-join`, the join
  of two exports.

Both export the same compact summaries of their execution graphs (`format/`). One join over the two exports gives
the verdict, in either direction: `conf-join spec.jsonl impl.jsonl` prints the implementation summaries that the
specification does not cover; none means the implementation conforms. `--decide` classifies each potential
violation and `--witness-dir` writes its witness graphs (`scripts/witness-join.sh` runs both and renders the graphs).

## Quick start
Requirements: Python 3.11 or newer, a Rust toolchain and JDK 17 (the two checkers are built from the pinned
submodules on first use). Clone the repository with its submodules, then run from its root:
```
git clone --recursive https://github.com/joulook/daavar.git
cd daavar
bin/daavar check examples/kv-cancel        # a key-value store with a Put racing a Cancel: two conforming
                                           # implementations and one mutant the check rejects; exit 0
bin/daavar new my-proto --impl rust,java   # scaffold your own project, then follow docs/
```
The first `check` builds both checkers (about a minute); later checks of kv-cancel take seconds. `check` explores
both programs, joins their behaviour sets, confirms every uncovered behaviour with Decide, writes the witness
graphs, and leaves `out/<run>/VERDICT.md`. Exit 0 conforms (a rejected mutant counts as conforming), 1 a violation
in some gating direction, 2 refused or incomplete. `new` writes a Rust specification, a Rust and/or Java
implementation, `MAP.md`, `SPEC-DESIGN.md`, `README.md` and `daavar.toml`. `docs/running.md` lists every command
and flag.

Java programs can also be written against the released library without this repository's checkout: JMC 0.2.0 is on
Maven Central as `org.mpi-sws.jmc:jmc:0.2.0` (the Must API and the exporter; the launcher joins the jar in 0.3.0);
the TraceForge crate is the pinned submodule.

## Two ways people use it
- **Paper to implementation.** Hand an agent a systems paper or a protocol text; it writes the specification program
  (Rust) from the text with every open point as a nondeterministic branch, writes or ports the implementation (Rust or
  Java), runs `check`, reads the verdict and the graphs, and iterates. The A2A and MCP case studies under
  `benchmarks/` were produced this way.
- **An implementation in another language.** Go, Python, TypeScript: the agent first writes the specification program
  from the text (or from the foreign implementation, with its accidental restrictions dropped and listed), then a
  Java or Rust port of the implementation under check, faithful to the source with a map of every abstraction, and
  checks the port. Only Java and Rust are checked directly.
`docs/SKILL.md` is the two-page reference an agent loads before writing either program.

## Documentation
| file | contents |
|---|---|
| `docs/SKILL.md` | the quick reference for agents: API cheat sheet, the rules of a faithful port, how to derive a specification, how to read a result |
| `docs/concepts.md` | programs, processes, messages, models, visible roles, words and statuses, conformance, Offline and Online, witnesses |
| `docs/modelling.md` | what to rewrite, what to keep, what to abstract (intra-process concurrency, timers, model calls, I/O), faithfulness, the map, the harness |
| `docs/spec-derivation.md` | from natural language (MUST/SHOULD, readings as arguments, labelled restrictions, mutants), from TLA+, from P, from a reference implementation; Rust first |
| `docs/running.md` | the driver's commands, flags, exit codes, the run directory |
| `docs/interpreting.md` | verdicts, the four classes of an uncovered behaviour, Decide, twins, the verification round, writing a result |
| `docs/examples.md` | kv-cancel; the two case studies; a new project |
| `docs/CONTRIBUTING.md` | layout, the format versioned in lock-step, adding a benchmark, pins and releases |
| `format/SPEC.md` | the normative summary-format, join and witness specification |

## Layout

| path | what |
|---|---|
| `benchmarks/` | every program the check has been evaluated on, one directory per benchmark or case study (including `a2a-task` and `mcp-cancel`), each with its README |
| `format/` | the summary interchange format and the golden export files |
| `examples/` | the small end-to-end example (`kv-cancel`) |
| `docs/` | the user guide (table above) |
| `daavar/`, `bin/daavar` | the driver |
| `scripts/` | the helpers the driver and the guide rely on: witness joining and rendering, source manifests, the two one-line tool runners, node setup |
| `jmc/`, `traceforge/` | the two checkers, pinned |
