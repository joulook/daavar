# mcp-cancel: two MCP tool calls over stdio, a cancel racing completion

**Family:** case study (MCP, Model Context Protocol). **Sources (pins):** specification
modelcontextprotocol/modelcontextprotocol `d4b317231161ee96d3eefb2448bda4230c76f4c6`, revision 2026-07-28
(docs/specification/2026-07-28/basic/transports/stdio.mdx, basic/patterns/cancellation.mdx,
basic/patterns/progress.mdx, basic/index.mdx, schema/2026-07-28/schema.ts); Rust core modelcontextprotocol/rust-sdk
`165f8fd514afb8f6d1be3e86260ee9abfce58541` (rmcp); Java core modelcontextprotocol/java-sdk
`779893c0706b01ad565e8768b4a1dbe4367eb058` (modern stack). Fixed rewrites: the same cores with the patches proposed
upstream applied: for rmcp, the fix commit `7d6c30ed332fd99953ebccdb7469e41d6ecd9cf8` in github.com/joulook/rust-sdk
(on upstream main `86c65108`, whose server files equal the pin's); for java-sdk, the fix commit
`69e6cd1e8fa3031b7c478782ed30afc051372a58` in github.com/joulook/java-sdk (on upstream main `779893c0`, the pin).

**Tools:** TraceForge `387e30d` (feat/conformance), JMC `5ad3c7b4`. Each rewrite cites its source lines in its
header and comments; `rust/src/spec.rs` cites the specification text it follows.

## Programs

| Program | Where | Source |
|---|---|---|
| spec | `rust/src/spec.rs` | the MCP specification text only |
| impl (Rust) | `rust/src/imp.rs` | rmcp server core |
| implf (Rust) | `rust/src/imp_fixed.rs` | rmcp server core at the fix commit `7d6c30e` (github.com/joulook/rust-sdk) |
| impl (Java) | `java/bench/mcp_cancel/McpCancel.java` | java-sdk server core |
| impl (Java, fixed) | `java/bench/mcp_cancel/McpCancelFixed.java` | java-sdk server core at the fix commit `69e6cd1e` (github.com/joulook/java-sdk) |
| mutant1-3 | `rust/src/spec.rs` with `mutant > 0`, run on the impl side | deliberately wrong variants of spec |

Every program is one sequential Must process per node, handling each received message atomically. All Rust programs
share `rust/src/msgs.rs` (the vocabulary) and `rust/src/env.rs` (client, both handlers, launcher). The Java twin
mirrors both field for field and event for event.

A cancel is received at its identification: the receive of `Cancelled{r}` is the point where the server has the parsed
notification and starts handling it (rmcp: the serve loop's `select!` arm taking it; java-sdk: the entry into
`handleCancel`, `StdioMcpTransport.java:219` at the baseline pin), not the read of its line. In the Java baseline the
validation and remove that follow the entry, and the flag, are later steps; in the fixed rewrite the receive step also
publishes the cancelled id (the SDK's first shared action); in the first design of the fix (68abe8da) the removal
and flag then followed under a lock, in the final fix (69e6cd1e, the one `McpCancelFixed.java` models) each request is
settled ANSWERED or CANCELLED by one compare-and-set, with no lock. The Java baseline was first measured with the receipt at
the read of the line (`readLine`); those numbers are kept in the study record. The revised baseline has the same
words in every measured pair; only its execution counts differ.

## Roles and observation

- `client` (observed):
  - sends `Call{1,"t1"}` and `Call{2,"t2"}`;
  - on the first `Progress{"t1",_}`, sends `Cancelled{1}` if request 1 has no response yet;
  - receives until request 2 has a response and request 1 has a response or the cancel;
  - then sends `Shutdown` to the server. Nothing for request 1 is awaited after the cancel.
- `server` (observed): the program under test. It spawns `handler1`, `handler2` and `client`. On `Shutdown` it sends
  `Shutdown` to both handlers, writes nothing else, and ends.
- `handler1` (r1) and `handler2` (r2), unobserved, not daemons. On `Work{r,t}` a handler sends `Prog{r,t,1}`, then one
  of:
  - (a) `Done{r}`;
  - (b) `Prog{r,t,2}` and `Done{r}`;
  - (c), handler1 only: nothing more.

  `Stop` does nothing. A handler ends on `Shutdown`.

No visibility annotations: every send and receive of `client` and `server` is in the exported word, including the
server's traffic with the handlers. No non-blocking receives. Model fifo on every link. Messages may be left unread
at the end. "Sent" means written to the client; the spec server queues per request and flushes nondeterministically
after each step (`rust/src/spec.rs`, header).

The flush has one restriction. An item may stay queued across a step only when another message to the server is
certain to arrive; nothing waits past an uncertain receive. Without a non-blocking receive, this is what makes every
execution terminate.

## Vocabulary (exported spelling `{"t":tag,"f":[fields in order]}`)

client → server: `Call{req,token}`, `Cancelled{req}`, `Shutdown`.
server → client: `Progress{token,value}`, `Result{req}`, `Error{req,code}`. An `Error`'s code is exported as `"*"`;
no path produces an `Error`.
handler → server: `Prog{req,token,value}`, `Done{req}`.
server → handler: `Work{req,token}`, `Stop{req}`, `Shutdown`.

Values: `req` ∈ {1,2}, `token` ∈ {"t1","t2"}, `value` ∈ {1,2}.

## ARGS

`<spec|impl|implf|mutant1|mutant2|mutant3>[,<h1><h2>]`:
- `<h1>` ∈ `a b c x` is handler1's branch; `<h2>` ∈ `a b x` is handler2's;
- `x` = nondeterministic; the default is `xx`.
- an optional third field selects the spec's reading of the cancellation rule: `s0` strict (the default), `s1`
  (sent = handed to the writer), `s2` (a request completed before the cancel is exempt), `s3` (both);
- an optional fourth field `c2` lets the client also cancel request 2 (Rust only; `implf` was not measured with it);
- `implf` runs the fixed rmcp rewrite (`imp_fixed.rs`); the fixed Java rewrite is the class `McpCancelFixed` with
  McpCancel's ARGS (`impl[,<h1><h2>]`).

The mutants:
- 1: a `Result{1}` after the cancel was received;
- 2: decreasing progress (value 1 held and written after value 2);
- 3: `Result{2}` dropped when request 1 was cancelled.

```
$R count  spec --model fifo -- spec,xx
$R export spec --out <file> --program mcp-cancel --size spec_xx --model fifo -- spec,xx
$R export impl --out <file> --program mcp-cancel --size mutant1_xx --model fifo -- mutant1,xx
```

## Completion and blocking

An execution is completed when the client has sent `Shutdown` and all four threads have ended. It is blocked when the
client waits forever. In the spec program that happens only with handler1 in (c) and its first progress dropped: the
client never cancels, and no response for request 1 comes.

`daavar.toml` lists every cell and direction for `bin/daavar check` (scenarios `pairs`, `pairs-c2`).

## Joins by hand

From `benchmarks/mcp-cancel/rust`, after `cargo build --release` there and
`(cd ../../../traceforge && cargo build --release -p traceforge --bin conf-join)`:

```
B=target/release/tf_mcp_cancel; J=../../../traceforge/target/release/conf-join
$B export spec --out spec.jsonl --model fifo -- spec,aa,s2
$B export impl --out rust.jsonl --model fifo -- impl,aa
$B export impl --out rustf.jsonl --model fifo -- implf,aa
$J --decide spec.jsonl rust.jsonl
$J --decide spec.jsonl rustf.jsonl
```

The Java rewrites export with JMC's `MpLauncher` (class `bench.mcp_cancel.McpCancel:impl,aa` or
`bench.mcp_cancel.McpCancelFixed:impl,aa`, mode `must`). The join's `uncovered` line counts the words of the second
file that the first does not contain. The tables give, per handler-branch pair, the implementation's words and its
uncovered words against the spec under each reading, before (baseline rewrite) → after (fixed rewrite). Under S0 the
uncovered words are split as T1/T2/T3 by the late write of request 1 they contain (the strongest tier counts):

- T1, the item was produced after the cancel (its `?Prog{1,…}` or `?Done{1}` follows `?Cancelled{1}`);
- T2, it was produced before the cancel and written after it, with request 1 still in progress;
- T3, `?Done{1}` came before `?Cancelled{1}`.

Rust (rmcp):

| Pair | words | S0 (T1/T2/T3) | S1 | S2 | S3 |
|---|---|---|---|---|---|
| aa | 850 → 693 | 302 = 123/78/101 → 101 = 0/0/101 | 123 → 0 | 201 → 0 | 123 → 0 |
| ab | 2 840 → 2 287 | 1 138 = 389/286/463 → 463 = 0/0/463 | 389 → 0 | 675 → 0 | 389 → 0 |
| ba | 3 363 → 2 328 | 1 613 = 944/420/249 → 249 = 0/0/249 | 944 → 0 | 1 364 → 0 | 944 → 0 |
| bb | 13 638 → 9 177 | 6 977 = 3 715/1 884/1 378 → 1 378 = 0/0/1 378 | not run | not run | not run |
| ca | 241 → 152 | 85 = 55/30/0 → 0 | 55 → 0 | 85 → 0 | 55 → 0 |
| cb | 641 → 362 | 265 = 161/104/0 → 0 | 161 → 0 | 265 → 0 | 161 → 0 |
| xx | 19 872 → 14 038 | 9 671 = 4 946/2 534/2 191 → 2 191 = 0/0/2 191 | not run | not run | not run |

Java (java-sdk; before = the revised baseline, whose words equal the first baseline's; after = the rewrite of the
**first design** of the fix, 68abe8da, rev 11 of the rewrite; the final fix 69e6cd1e is re-checked below):

| Pair | words | S0 (T1/T2/T3) | S1 | S2 | S3 |
|---|---|---|---|---|---|
| aa | 7 934 → 4 267 | 4 844 = 3 411/710/723 → 1 219 = 0/656/563 | 3 411 → 0 | 4 121 → 656 | 3 411 → 0 |
| ab | 66 467 → 31 134 | 42 887 = 30 311/5 534/7 042 → 8 412 = 0/4 680/3 732 | 30 311 → 0 | 35 845 → 4 680 | 30 311 → 0 |
| ba | 64 200 → 30 314 | 47 453 = 32 663/9 903/4 887 → 12 402 = 0/8 194/4 208 | 32 663 → 0 | 42 566 → 8 194 | 32 663 → 0 |
| ca | 942 → 780 | 323 = 190/133/0 → 122 = 0/122/0 | 190 → 0 | 323 → 122 | 190 → 0 |
| cb | 5 906 → 4 909 | 2 043 = 1 167/876/0 → 728 = 0/728/0 | 1 167 → 0 | 2 043 → 728 | 1 167 → 0 |

The Java pairs bb and xx were not exported for the revised baseline or the fixed rewrite. Every join of a fixed
rewrite, and every Java join, reports no false alarm and no unresolved word; no uncovered word in the tables is
blocked.

With the final Java fix (69e6cd1e; `McpCancelFixed.java` as committed, with its three labelled reductions) only pair
ca has been re-checked: 323 = 190/133/0 → 122 = 0/122/0 under S0 and S2, 190 → 0 under S1 and S3, no false alarm, the
18 client words unchanged; its export equals the first design's apart from execution counts. aa stopped at a 24-hour
cap, ab and ba on a split-mode defect of our Java checker (a 2 GB array limit), cb was still running when this was
written; none of the four is claimed for the final fix.

The readings, by the tiers they reject:
- S0 (strict) rejects T1, T2 and T3: nothing of request 1 may be written after `?Cancelled{1}`.
- S1 (sent = handed to the writer) rejects T1 only: an item handed to the writer before the cancel counts as sent.
- S2 (a request completed before the cancel is exempt) rejects T1 and T2.
- S3 (both relaxations) rejects T1 only.

Residual after the fix: one accepted item of request 1 per cancel, with one Java exception (last sentence). Rust has none under S1-S3 and only T3 words under
S0, each a response completed before the cancel. Java (first design, the table above) has none under S1 and S3; under
S0 and S2 it has T2 words (aa 656, ab 4 680, ba 8 194, ca 122, cb 728), each with the one item of request 1 that the
writer checked before the cancel's publish and wrote after it; under S0 it also has T3 words. Of those Java T3 words,
aa 78, ab 486 and ba 1 170 carry two late items of request 1: request 1 was answered before the cancel, so the cancel
flags nothing and every item of request 1 still queued is written after it. With the final design, pair ca has the
same 122 T2 words and no T3.

## Threaded models

`java/bench/mcp_cancel/McpCancelThreaded.java` (java-sdk at the pin) and `McpCancelThreadedFixed.java` (at
`68abe8da`, the first design of the fix; the final fix has no thread-level Java model yet) model the SDK's threads and shared objects by message passing, under sequential consistency:
- every SDK thread that touches shared state is a Must process: the reader, the single writer, and one callback
  process per handler invocation;
- every shared object is a hidden process that serialises requests and replies: the in-flight map, the per-request
  cancelled flag, the outgoing queue, and in the fixed SDK the lock and the `cancelling` field;
- an operation the SDK performs atomically is one request; every other access is one request, in program order;
- the visible `server` process is the stdio transport: it relays client lines to the reader and the writer's writes
  to the client.

Selectors `implt` and `impltf` (`bench.mcp_cancel.McpCancelThreaded:implt[,<h1><h2>]`,
`bench.mcp_cancel.McpCancelThreadedFixed:impltf[,<h1><h2>]`). `java/scripts/project_threaded.py` removes the relay
events of `server` from such an export so that it can be joined against the spec. The Rust twins are
`rust/src/imp_threaded.rs` (`implt`) and `imp_threaded_fixed.rs` (`impltf`); TraceForge hides the relay events at the
source, so their exports join as they are. `imp_threaded_pred.rs` (`impltp`) and `imp_threaded_fixed_pred.rs`
(`impltfp`) are the same models with the request pool on `traceforge::sync` atomics, the writer mutex of `impltp` on
`traceforge::sync::Mutex` and that of `impltfp` kept hand-written with a state-dependent receive predicate: the same
observations on every pair run (impltp aa, ba; impltfp aa, ba, bb, xx), about 4-5x fewer graphs for `implt` and about
1.7x for `impltf` on those pairs. The four Rust models take the client's `Shutdown` only while no spawned or gated
write is pending (a labelled restriction standing in for the SDK's drain, which is not modelled); the Java models have
no such rule. None of them is in `daavar.toml`'s scenarios.

Results (Rust, all pairs for `impltf`, aa ab ba ca cb for `implt`; bb and xx of `implt` exceed a one-hour cap): every
observation of a hand rewrite without an end-of-input cut is an observation of its threaded model; the verdicts are
unchanged (the fixed model has no T1 or T2 word under any reading; the baseline keeps T1 under every reading); the
observations only the threaded models produce are absent from the hand exports because of the hand rewrites' flush
restriction; against the specification the uncovered ones are of three kinds (the rest conform: for `impltf` aa 105 of the 706 server
words only in its threaded export): rejected by the specification program's flush
restriction SD-M10 only (for `impltf` aa shown by admission: all of them covered once SD-M10 is lifted), T3 words that
the strict reading rejects, and, for the baseline, T1 and T2 words at interleavings the hand rewrite does not reach.
Java: the baseline threaded model of pair aa exported 763,017 lines (59,661 after the
projection) within a 12-hour cap; the fixed one did not finish within it.

## Upstream

| Finding | Issue | Pull request |
|---|---|---|
| Progress written after `notifications/cancelled` was read (rmcp) | to be filed | to be filed |
| A request's output written after `notifications/cancelled` was read (java-sdk, modern stdio transport) | to be filed | to be filed |
