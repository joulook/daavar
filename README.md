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

## Layout

| path | what |
|---|---|
| `benchmarks/` | every program the check has been evaluated on, one directory per benchmark or case study (including `a2a-task` and `mcp-cancel`), each with its README |
| `format/` | the summary interchange format and the golden export files |
| `scripts/` | build the checkout (`setup-node.sh`), run one side (`jmc-run.sh`, `rust-run.sh`), join with witnesses (`witness-join.sh`) |
| `jmc/`, `traceforge/` | the two checkers, pinned |

## Documentation

A user guide (how to write the specification and the implementation, what to abstract, how to read a verdict) and
a driver that runs the whole check in one command are in preparation.

The PLDI 2027 Table 1 is rebuilt from tag `pldi27-artifact`, the inputs in `eval/` and the pinned records, see
`eval/README.md`; Tables 2 and 3 are not yet reproduced from this repository.
