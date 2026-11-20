# mcp-cancel: two MCP tool calls over stdio, a cancel racing completion

**Family:** case study (MCP, Model Context Protocol). **Sources (pins):** specification
modelcontextprotocol/modelcontextprotocol `d4b317231161ee96d3eefb2448bda4230c76f4c6`, revision 2026-07-28
(docs/specification/2026-07-28/basic/transports/stdio.mdx, basic/patterns/cancellation.mdx,
basic/patterns/progress.mdx, basic/index.mdx, schema/2026-07-28/schema.ts); Rust core modelcontextprotocol/rust-sdk
`165f8fd514afb8f6d1be3e86260ee9abfce58541` (rmcp); Java core modelcontextprotocol/java-sdk
`779893c0706b01ad565e8768b4a1dbe4367eb058` (modern stack).

**Tools:** TraceForge `387e30d` (feat/conformance), JMC `5ad3c7b4`.

**Design documents** (cited, not copied), in the github-jmc plan tree under `plan/case-studies/mcp/`:
- `EXPERIMENT-1.md`: the brief;
- `SPEC-DESIGN.md`: the specification program (rev 5);
- `MAP-java.md` and `MAP-rust.md`: the source-to-model maps.

## Programs

| Program | Where | Source | Author |
|---|---|---|---|
| spec | `rust/src/spec.rs` | the MCP specification text only | spec author |
| impl (Rust) | `rust/src/imp.rs` (stub until it lands) | rmcp server core | TraceForge agent |
| impl (Java) | `java/` | java-sdk server core | JMC agent |
| mutant1-3 | `rust/src/spec.rs` with `mutant > 0`, run on the impl side | deliberately wrong variants of spec | spec author |

Every program is one sequential Must process per node, handling each received message atomically. All Rust programs
share `rust/src/msgs.rs` (the vocabulary) and `rust/src/env.rs` (client, both handlers, launcher). The Java twin
mirrors both field for field and event for event.

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
after each step (SPEC-DESIGN §1.6).

The flush has one labelled restriction, SPEC-DESIGN SD-M10. An item may stay queued across a step only when another
message to the server is certain to arrive; nothing waits past an uncertain receive. Without a non-blocking receive,
this is what makes every execution terminate.

## Vocabulary (exported spelling `{"t":tag,"f":[fields in order]}`)

client → server: `Call{req,token}`, `Cancelled{req}`, `Shutdown`.
server → client: `Progress{token,value}`, `Result{req}`, `Error{req,code}`. An `Error`'s code is exported as `"*"`;
no path produces an `Error`.
handler → server: `Prog{req,token,value}`, `Done{req}`.
server → handler: `Work{req,token}`, `Stop{req}`, `Shutdown`.

Values: `req` ∈ {1,2}, `token` ∈ {"t1","t2"}, `value` ∈ {1,2}.

## ARGS

`<spec|impl|mutant1|mutant2|mutant3>[,<h1><h2>]`:
- `<h1>` ∈ `a b c x` is handler1's branch; `<h2>` ∈ `a b x` is handler2's;
- `x` = nondeterministic; the default is `xx`.
- an optional third field selects the spec's reading of the cancellation rule (SPEC-DESIGN SD-M12): `s0` strict
  (the default), `s1` (sent = handed to the writer), `s2` (a request completed before the cancel is exempt), `s3`
  (both).

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
