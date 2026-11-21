# Examples (draft 2, 2026-11-21)

## kv-cancel
`examples/kv-cancel/` is a key-value store with one client whose Put races a Cancel of the same request. The
specification allows the store to answer PutOk or Canceled, never both, and never PutOk after it acknowledged the
Cancel. It has a Rust specification (`rust/src/spec.rs`), a correct Rust implementation (`rust/src/imp.rs`), a Java
implementation (`java/bench/kv_cancel/KvCancel.java`) and one mutant that answers PutOk after CancelOk. The README
states the protocol in ten lines and the expected verdicts.
```
bin/daavar check examples/kv-cancel
```
Expected: exit 0, CONFORMS. The Rust and the Java implementation conform (0 uncovered words against the
specification, and 0 against each other); the mutant gives one uncovered word, confirmed by Decide, so it is
rejected, which counts as conforming. Its witness graph is
`out/<run>/one.mutant/joins/mutant1-in-spec/graphs/1.graph.json`, with `.tex`, `.dot` and `.witness.txt` beside it.
The specification has 5 executions, the implementation 4, the mutant 3. The first run builds both checkers and
takes about a minute; once they are cached, a check takes a few seconds. Render and replay the witness with
`bin/daavar render` and `bin/daavar replay` on the graph file that VERDICT.md names.

## The two case studies
`benchmarks/a2a-task/` (the A2A protocol's task lifecycle: a streaming send racing a cancel, with delegation between
two agents) and `benchmarks/mcp-cancel/` (the MCP stdio cancellation race) are full studies: a specification program
derived from the protocol text with its readings as arguments, and rewrites of the Java and Rust SDKs. Their design
and map documents are cited in each README, not copied into the directory. Neither directory has a `daavar.toml`
yet, so the driver cannot run them from there as they stand. With a `daavar.toml` of the schema above, the driver
gave the same uncovered words and the same classes as the studies' hand runs on the cells tried (A2A: one server and
both delegation scenarios with the nondeterministic executor; MCP: one handler pair under the first reading). A
cell takes from half a minute to several minutes; start with `--cell` on one cell.

## What a new project looks like
`bin/daavar new my-proto --impl rust,java` creates the layout the guides assume: `daavar.toml`, `README.md`,
`MAP.md`, `SPEC-DESIGN.md`, `rust/Cargo.toml`, `rust/Cargo.lock`, `rust/src/{msgs,env,spec,imp,main}.rs` and
`java/bench/my_proto/MyProto.java`. Hyphens become underscores in the crate and the Java package and split the class
name (`myproto` gives `Myproto.java`); a name whose Java package would be a Java keyword is refused. Fill the
vocabulary first, then the specification, then the implementation; run `vocab` before the first `check`.
