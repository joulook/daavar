# a2a-task: one A2A task, one stream, a cancel racing completion

**Family:** case study (A2A, Agent2Agent protocol). **Sources (pins):** specification
a2aproject/A2A `5f931b8a25fe66789f987ec4a55fc16ee9fe41ee` (docs/specification.md, specification/a2a.proto,
docs/topics/life-of-a-task.md, docs/topics/streaming-and-async.md); Java core a2aproject/a2a-java
`f40664f85f61d232fc794dfe9b7cdc8557bcd7b8`; Rust core a2aproject/a2a-rs `700a6a28eb001f6fc32bc9446078ff87bdd18c6c`.
Fixed rewrites: the same cores with the patches proposed upstream applied: for a2a-rs, task-first stream and
message-stream close (built on `e9b00bc`, whose `handler.rs` equals `700a6a2`'s, and rebased onto `e7d507b` on 2026-11-24 without a change in the modelled behaviour); for a2a-java, task-first stream
(built on `586fdd5`, equivalent to `f40664f` under the model's abstraction). The results below were measured with
TraceForge `387e30d` (feat/conformance) and JMC `5ad3c7b4`. Each rewrite cites its source lines in its header and
comments.

## Programs

| Program | Where | Source |
|---|---|---|
| spec | `rust/src/spec.rs` | the A2A specification text only |
| impl (Rust) | `rust/src/imp.rs` | a2a-rs server core |
| implf (Rust) | `rust/src/imp_fixed.rs` | a2a-rs server core with both fixes |
| impl (Java) | `java/bench/a2a_task/A2ATask.java` | a2a-java server core |
| impl (Java, fixed) | `java/bench/a2a_task/A2ATaskFixed.java` | a2a-java server core with the fix |
| mutant1-4 | `rust/src/mutants.rs` | deliberately wrong servers, run on the impl side |

Every program is one sequential Must process per node, handling each received message atomically; a step ends at
every external wait. All Rust programs share `rust/src/msgs.rs` (the vocabulary) and `rust/src/env.rs` (client,
executor, launcher); the Java twin mirrors `msgs.rs` field for field.

## Roles and observation

- `client` (observed): `SendStream{1}`; stream items of request 1 until `Status{1,t1,Working,false}` (a `TaskItem`
  before it is consumed); `Cancel{2,t1}`; receive until both an answer with req 2 and `StreamClose{1}`; `Get{3,t1}` and
  its answer; with `recancel`, `Cancel{4,t1}` and its answer; last, `Shutdown` to the server.
- `server` (observed): the program under test. It spawns `exec` and `client`. After the client's `Shutdown`, once
  every `CancelCb` is answered, it sends `Shutdown` to `exec` and ends.
- `exec` (unobserved, not a daemon): on `Exec`: variant A sends `Submit`, then `Working`; then exec branch (a)
  `Completed` at once, or branch (b) nothing more; on every `CancelCb` exactly one `Canceled`; on `Shutdown` it ends.

No visibility annotations: every send and receive of `client` and `server` is in the exported word, including the
server's traffic with `exec`, in both tools. `rust/src/env.rs` is the reference for the client and the executor, and
the Java twin is identical event for event. Model fifo on every link. No timeouts, push, reconnect, interrupted
states, transport faults or authorization errors.

## Vocabulary (exported spelling)

client → server: `SendStream{req}`, `Cancel{req,task}`, `Get{req,task}`. server → client: `TaskItem{req,task,state}`,
`Status{req,task,state,fin}`, `StreamClose{req}`, `CancelOk{req,task,state}`, `CancelErr{req,code}`,
`GetOk{req,task,state}`, `Err{req,code}`. Hidden: `Exec`, `Submit`, `Working`, `Completed`, `CancelCb`, `Canceled`
(each `{task}`); client → server `Shutdown` (last), server → exec `Shutdown`. `task = "t1"`. States `Submitted`, `Working`, `Completed`, `Canceled`; cancel
code `TaskNotCancelable` (unit variants); `fin` = the state is terminal; an `Err`'s code is exported as `"*"` (the
implementation's own name is kept, unobserved, in `ErrCode`).

## ARGS and variants

`<spec|impl|implf|mutant<n>>,<a|b|n>[,recancel]` (one server) and
`<spec2|impl2|impl2f|spec2g|impl2g|impl2gf>,<a|b|n>[,<split>]` (delegation, below):
- `a` = variant A (the executor's first report is `Submit`), `b` = variant B (`Working` first);
- `n` = the executor is the agent's model as a nondeterministic procedure (`env::call_llm`): a plain Message
  (`Reply`), or a task whose first report is `Submit` or `Working`, then `Completed` at once, or `Working` and then
  `Completed` or nothing more;
- `recancel` = the client also sends `Cancel{4,t1}` after the read (one server only);
- `implf`, `impl2f`, `impl2gf` run the fixed a2a-rs rewrite (`imp_fixed.rs`); the fixed Java rewrite is the class
  `A2ATaskFixed` with the same ARGS;
- `mutant<n>` (n = 1..4) runs a deliberately wrong server on the impl side, with `a` or `b` only.

The spec server emits the Task item itself, so its observations are the same in A and B; a server that relays only
the executor's reports lacks the first item in B.

Commands (`$R` = the release binary `tf_a2a_task`):

```
$R count  spec --model fifo -- spec,a
$R export spec --out <file> --program a2a-task --size a -- spec,a
```

Bad ARGS print the reason and a `usage:` line on stderr and exit 2.

## Completion and blocking

Completed: the client stopped after its answers and `StreamClose{1}`. Blocked: some thread waits forever (only the
spec's refused cancel under exec branch (b) does). Execution counts include hidden interleavings
(e.g. a late `Canceled`), so they are reported per program and not compared; observations are compared.

## Results

Spec, brain03, cores 80-127, TraceForge `387e30d`:

| ARGS | count | exported observations |
|---|---|---|
| `spec,a` | execs=444 blocked=3 | 447 (444 completed, 3 blocked: exec branch (b) with a refused cancel) |
| `spec,b` | execs=444 blocked=3 | 447 |
| `spec,a,recancel` | execs=7023 blocked=3 | 7026 |
| `spec,b,recancel` | execs=7023 blocked=3 | 7026 |

## Delegation (two servers)

ARGS `spec2,<a|b|n>` / `impl2,<a|b|n>`. Roles `client`, `serverA`, `serverB` (visible) and `execA`, `execB`. One
server function runs as two instances; execA is a client of serverB. Wiring and execA's contract are in
`rust/src/env.rs` (`launch2`, `exec_a`). `spec2g`/`impl2g`/`impl2gf` use the gated contract: execA sends its cancel
downstream only after serverB's stream has shown `Status{1,t1,Working,false}`. With `n`, a third field splits the
exploration into parts: `local` (A answers by itself), `deleg` (A delegates), `delegmsg` (A delegates and B answers
with a Message), `delegtask[-<k>[<c>]]` (A delegates and B opens a task; `k` = `s` for `Submit` first or `w` for
`Working` first; `c` = `c` for `Completed` at once or `w` for `Working`, then `Completed` or nothing more).
Spec, brain03, with variant A/B: `spec2,a` execs=363489 blocked=1017, 173400 observations; `spec2,b` execs=229821
blocked=729, 102138 observations.

## Joins by hand

From `benchmarks/a2a-task/rust`, after `cargo build --release` there and
`(cd ../../../traceforge && cargo build --release -p traceforge --bin conf-join)`:

```
B=target/release/tf_a2a_task; J=../../../traceforge/target/release/conf-join
$B export spec --out spec.jsonl --model fifo -- spec,n
$B export impl --out rust.jsonl --model fifo -- impl,n
$J --decide spec.jsonl rust.jsonl
```

The Java rewrites export with JMC's `MpLauncher` (class `bench.a2a_task.A2ATask:impl,n` or
`bench.a2a_task.A2ATaskFixed:impl,n`, mode `must`). The join's `uncovered` line counts the words of the second file
that the first does not contain (`count` reports executions, which can exceed the number of distinct words).
Uncovered / all words of the implementation, against the spec:

| Cell | Java | Java fixed | Rust | Rust fixed |
|---|---|---|---|---|
| one server `n` | 11/18 | 0/18 | 10/16 | 1/16 |
| one server `n,recancel` | 11/18 | 0/18 | 10/16 | 1/16 |
| delegation `n,delegmsg` | 0/3 | 0/3 | 4/5 | 2/3 |

`daavar.toml` lists every cell and direction for `bin/daavar check` (scenarios `one`, `deleg`, `gated`).

## Upstream

The findings of this benchmark were reported to the A2A projects on 2026-11-22, each with the reproduction
commands above:

| Finding | Issue | Pull request |
|---|---|---|
| Streaming send begins with a status event instead of the Task (a2a-java) | [a2a-java#1352](https://github.com/a2aproject/a2a-java/issues/1352) | [a2a-java#1353](https://github.com/a2aproject/a2a-java/pull/1353), merged 2026-11-24 (commit `369ee867`) |
| Streaming send begins with a status event instead of the Task (a2a-rs) | [a2a-rs#585](https://github.com/a2aproject/a2a-rs/issues/585) | [a2a-rs#587](https://github.com/a2aproject/a2a-rs/pull/587), merged 2026-11-27 (commit `80dce6d7`) |
| A Message answer is followed by a Canceled status and the task is retained (a2a-rs) | [a2a-rs#586](https://github.com/a2aproject/a2a-rs/issues/586) | [a2a-rs#588](https://github.com/a2aproject/a2a-rs/pull/588), merged 2026-11-25 (commit `74ebd92c`) |
| Neither server core begins the stream with the Task; who carries that requirement | [A2A#2494](https://github.com/a2aproject/A2A/issues/2494) | [A2A#2503](https://github.com/a2aproject/A2A/pull/2503) (test) |
| When a cancellation takes effect | [A2A#2495](https://github.com/a2aproject/A2A/issues/2495) | [A2A#2501](https://github.com/a2aproject/A2A/pull/2501) |
| Ending a task stream with an error after an accepted cancel | [A2A#2496](https://github.com/a2aproject/A2A/issues/2496) | awaiting the maintainers' choice |
| From when a task id is valid | [A2A#2497](https://github.com/a2aproject/A2A/issues/2497) | [A2A#2502](https://github.com/a2aproject/A2A/pull/2502) |
| A task kept after a Message answer ("no task tracking") | [A2A#2498](https://github.com/a2aproject/A2A/issues/2498) | awaiting the maintainers' choice |

The two a2a-rs pull requests, #587 and #588, were rebased onto upstream main `e7d507b`. The `fork tf:N` citations
in `rust/src/imp_fixed.rs` are lines of the rebased task-first patch; a re-check of both Rust rewrites against
`e7d507b` and the rebased patches found no modelled word changed.
