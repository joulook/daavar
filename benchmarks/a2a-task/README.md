# a2a-task: one A2A task, one stream, a cancel racing completion

**Family:** case study (A2A, Agent2Agent protocol). **Sources (pins):** specification
a2aproject/A2A `5f931b8a25fe66789f987ec4a55fc16ee9fe41ee` (docs/specification.md, specification/a2a.proto,
docs/topics/life-of-a-task.md, docs/topics/streaming-and-async.md); Java core a2aproject/a2a-java
`f40664f85f61d232fc794dfe9b7cdc8557bcd7b8`; Rust core a2aproject/a2a-rs `700a6a28eb001f6fc32bc9446078ff87bdd18c6c`.
Tools: TraceForge `387e30d` (feat/conformance), JMC `5ad3c7b4`. Design documents (cited, not copied), in the
github-jmc plan tree: `plan/case-studies/a2a/EXPERIMENT-1.md` (brief), `SPEC-DESIGN.md` (specification program),
`MAP-java.md` and `MAP-rust.md` (source-to-model maps).

## Programs

| Program | Where | Source | Author |
|---|---|---|---|
| spec | `rust/src/spec.rs` | the A2A specification text only | spec author |
| impl (Rust) | `rust/src/imp.rs` (stub until it lands) | a2a-rs server core | TraceForge agent |
| impl (Java) | `java/bench/a2a_task/A2ATask.java` | a2a-java server core | JMC agent |

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
states, transport faults or authorization errors. Conventions: brief, section "Rev 5".

## Vocabulary (exported spelling)

client → server: `SendStream{req}`, `Cancel{req,task}`, `Get{req,task}`. server → client: `TaskItem{req,task,state}`,
`Status{req,task,state,fin}`, `StreamClose{req}`, `CancelOk{req,task,state}`, `CancelErr{req,code}`,
`GetOk{req,task,state}`, `Err{req,code}`. Hidden: `Exec`, `Submit`, `Working`, `Completed`, `CancelCb`, `Canceled`
(each `{task}`); client → server `Shutdown` (last), server → exec `Shutdown`. `task = "t1"`. States `Submitted`, `Working`, `Completed`, `Canceled`; cancel
code `TaskNotCancelable` (unit variants); `fin` = the state is terminal; an `Err`'s code is exported as `"*"` (the
implementation's own name is kept, unobserved, in `ErrCode`).

## ARGS and variants

`<spec|impl>,<a|b>[,recancel]`: `a` = variant A (1a: the executor's first report is `Submit`), `b` = variant B (1c:
`Working` first), `recancel` = 1b (variant A plus `Cancel{4,t1}`). The spec server emits the Task item itself, so its
observations are the same in A and B; a server that relays only the executor's reports lacks the first item in B.

Commands (`$R` = the release binary `tf_a2a_task`):

```
$R count  spec --model fifo -- spec,a
$R export spec --out <file> --program a2a-task --size a -- spec,a
```

## Completion and blocking

Completed: the client stopped after its answers and `StreamClose{1}`. Blocked: some thread waits forever (only the
spec's refused cancel under exec branch (b) does). Execution counts include hidden interleavings
(e.g. a late `Canceled`), so they are reported per program and not compared; observations are compared. (The
definition is `SPEC-DESIGN.md` §5.)

## Results

Spec, brain03, cores 80-127, TraceForge `387e30d`:

| ARGS | count | exported observations |
|---|---|---|
| `spec,a` | execs=444 blocked=3 | 447 (444 completed, 3 blocked: exec branch (b) with a refused cancel) |
| `spec,b` | execs=444 blocked=3 | 447 |
| `spec,a,recancel` | execs=7023 blocked=3 | 7026 |
| `spec,b,recancel` | execs=7023 blocked=3 | 7026 |

Joins: see `plan/case-studies/a2a/RESULTS-1.md` (github-jmc plan tree).

## Experiment 2 (delegation)

ARGS `spec2,<a|b>` / `impl2,<a|b>`. Roles `client`, `serverA`, `serverB` (visible) and `execA`, `execB`. One server
function runs as two instances. Wiring and execA's contract are in `rust/src/env.rs` (`launch2`, `exec_a`); the
design is `SPEC-DESIGN-2.md`. Spec (rev 10), brain03: `spec2,a` execs=363489 blocked=1017, 173400 observations;
`spec2,b` execs=229821 blocked=729, 102138 observations. Joins: `plan/case-studies/a2a/RESULTS-2.md`.

