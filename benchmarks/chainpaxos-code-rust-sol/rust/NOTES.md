# ChainPaxos port

Read `code-src/README.md` first. All Java references below are to the supplied
`code-src/` cut, with line numbers in that cut. No paper or external repository
was consulted. The executable is one implementation: both `Program.spec` and
`Program.imp` construct it; an external harness supplies the specification.

## Processes and interface

`n0`, `n1`, `n2` are created in that order, followed by `c0`, `c1`, followed by
invisible `init`. The main execution thread only spawns and joins. `init` sends
`Setup { nodes, clients }` with tag `u32::MAX`. A selective blocking receive for
that tag leaves earlier client and protocol traffic buffered in the mailbox.
Each replica then runs forever, including after both clients end. Joining these
replicas blocks; this is the intended harness termination condition, not a
shutdown protocol.

Clients have exactly the `Req`, `Rep`, command generator, and sequential client
function in INTERFACE.md. `c0` uses `n0`; `c1` uses `n2`. Only those client thread
names are visible. The entry requires `q >= 1` and `mixed` or `local`, and accepts
an optional `fifo`/`bag` word. Delivery semantics are selected by the shared
TraceForge runner's `--model` option (default FIFO); the optional program word
is validated, not used to override the runner's model. Use matching values when
supplying both. This follows the required `traceforge::program::main(Program)`
entry, whose model selection is owned by `api/program.rs`.

There is no shared mutable state, address registry, OS synchronization, polling,
or external I/O in the modeled program. Every process exchange uses Must message
sends and blocking receives. Immutable startup arguments are captured when
spawning; replicas obtain all addresses through Setup.

## Roles and application

A replica combines the Java protocol, its front, and its application. In
`ChainPaxosMixedFront.java:86-115,130-137,152-175`, an operation becomes a batch,
is submitted at the writes-to node, and only the issuing front executes with
`local=true`. The writes-to address is the supported leader
(`ChainPaxosDelayedProto.java:850-853`, and the mixed equivalent). The port
immediately wraps one client command as `Operation { origin, sequence, request }`.
A nonleader sends `Redirect` to `n0`; the leader calls `submit` immediately.
This is the batching-free equivalent of PeerBatchMessage -> SubmitBatchRequest
-> `sendNextAccept` (`MixedProto:629-667`, `DelayedProto:648-654,662-691`). The generic front registers peer and execution handlers at
`FrontendProto.java:62-74`; `HashMapApp.java:319-330` records the return channel
and submits the strong operation. Weak reads are excluded by INTERFACE.md. There
are no leader-election waiting queues because n0 is the established leader for
the whole execution. Every handled request is submitted before the next receive;
an undelivered mailbox message is not an application pending queue.

Each replica owns a register x, initially zero. Write sets it and produces Ack;
Read produces Value(x); Rmw increments it by ten and produces Result(new x).
Rmw is ordered as a write, and runs at every replica. Only `origin` sends the
client reply, using Setup's client address. This preserves the front's local
flag and `HashMapApp.java:221-246`: remote writes update state, and only local
operations respond. The Java application has no Rmw, so its atomic register
meaning is supplied entirely by INTERFACE.md. Arithmetic uses u64, as required
by the interface; extremely large executions that overflow u64 are outside the
representable register model (release arithmetic follows Rust's release rules).
The per-origin sequence replaces Java batch IDs (`FrontendProto.java:80-83`); sequential clients and reliable
nonduplicating delivery require no retry/deduplication mechanism.

## Protocol messages and handlers

* `Redirect(Operation)` is the front's peer request to the fixed leader.
* `Accept { instance, counter, value, ack }` retains AcceptMsg.java's instance,
  accumulated node counter, value, and cumulative acknowledgment. A value is
  exactly one App operation or NoOp. The invariant fixed ballot/leader fields
  are omitted. Rust instance 1 corresponds to Java instance 0; prefix length 0
  corresponds to Java's initial -1 (`DelayedProto:80-83`).
* `AcceptAck(instance)` is the tail-to-leader cumulative acknowledgment
  (`AcceptAckMsg.java`). It is not sent backward hop by hop.
* `ReadOpen(client, sequence)` reports a local read to the leader solely for
  the bounded replacement of its no-op timer.
* `ReadReady(client, sequence, value, origin)` retires a ready read's no-op
  credit at the leader. `ExecuteRead(client, value)` returns that permission to
  the origin, whose front sends Value. These two bookkeeping messages do not
  order writes or move register state. Their rationale is below.

`submit` allocates the next instance and directly invokes the local ACCEPT
handler with counter zero and the leader's acknowledged prefix, matching
`MixedProto:638-667` / `DelayedProto:662-691`. There is no self-send for this
same-process call. `accept` increments the received counter: n0 stores 1, n1
stores 2, n2 stores 3 (`MixedProto:497-535`, `DelayedProto:514-552`). The fixed
membership specializes `forward` to n0 -> n1 -> n2 -> ACK n0
(`MixedProto:549-579`, `DelayedProto:566-596`; Membership.java's circular
nextLivingInChain/nextNodesUntil and isAfterLeader). Forwarding happens before
local decision and before processing the incoming piggybacked ack. Each
forwarded ACCEPT carries the forwarding replica's *current* acknowledged prefix,
not simply the incoming ack.

At counter >= 2, n1 and n2 decide and execute in instance order. n0 does not
execute merely because it accepted: the tail ACK calls `ack`, which decides and
executes the entire previously undecided prefix. Then `ack` removes the
acknowledged instances. Old cumulative ACKs do nothing. These are
`MixedProto:582-617,722-740` and `DelayedProto:599-636,747-765`. The current
instance value is retained until acknowledgment, allowing attached reads and
cumulative cleanup. `ack` decides the entire prefix before releasing its reads,
as in the two separate Java loops; reads therefore sample the register after
all executions in that handler.

DECIDED is deliberately absent on the wire in the fixed-leader specialization:
its supplied handler (`MixedProto:462-487`, `DelayedProto:479-504`) is part of
PREPARE/leadership recovery, not the ordinary ACCEPT decision path. All normal
decisions here arise from counter quorum or cumulative ACK. Sending a DECIDED
broadcast on every quorum would change this implementation's behavior.

## Local reads, no-ops, and explicit decisions

`mixed` places reads into ordinary App instances. `local` handles Read at its
connected replica without redirection or a read consensus instance. It attaches
the request to **highestAcceptedInstance + 1**, even if that instance has no
value yet (`DelayedProto:657-659`; InstanceState.java:45-51). A quorum decision
alone does not release it. `DelayedProto:619-635` releases attached reads only
when that instance belongs to the acknowledged prefix. The front then executes
the read locally (`DelayedFront.java:151-157,220-231`). Writes and Rmw retain the
same forwarding and origin-reply path in both variants.

The source timer (`MixedProto:325-329`, `DelayedProto:342-346`) has no finite
budget or read awareness; a non-no-op ACK also forces a future timer send
(`MixedProto:596-597`, `DelayedProto:613-614`). INTERFACE.md replaces this with
bounded nondeterminism **only in local**. At each leader handler boundary,
`maybe_noop` offers `nondet()` for each known, outstanding read whose credit has
not been spent. True starts one ordinary NoOp instance and permanently spends
that read's credit; false waits for the next incoming event. There is no timer,
infinite retry loop, or no-op after credits are exhausted. Simultaneous credits
are considered in client/sequence order; all admitted values are identical
NoOps. Reads not yet reported are not eligible. There is no request queue at
such a boundary: all previously received client/redirect requests have already
been submitted. NoOps follow the complete ACCEPT/ACK path and do not alter x.

The source leaves global read-budget bookkeeping open because its timer has no
such condition. This port uses ReadOpen plus ReadReady/ExecuteRead to ensure
that a spent credit always belongs to a still-outstanding client read, even
with bag delivery. At acknowledgment the origin samples x; it waits for the
leader to mark the read closed before sending that saved Value to the client.
For a read at n0, closure and reply happen directly. Closure tombstones handle
a ReadOpen arriving after ReadReady. This extra message delay is an explicit
simplification of the asynchronous protocol-to-front ExecuteReadReply delivery
(`DelayedProto:631`, `DelayedFront:220-231`); local register sampling is folded
into the acknowledgment handler. It avoids assuming instantaneous global
knowledge or sending a no-op on a stale completed-read notification. It adds no
extra read barrier instance and does not permit a read before acknowledgment.

One no-op per read is a bound, not a progress guarantee. In particular, a tail
read may need a later ACCEPT carrying the ack of its attached instance, and a
single available no-op may not suffice. Declining all no-op opportunities may
also leave reads blocked. This preserves the specified finite nondeterministic
rule without inventing extra no-ops, ACK broadcasts, or immediate local reads.
The harness records blocked executions; replicas always remain receiving.

## Receive/transport and delivery decisions

The Must API's typed receives downcast the chosen message; they do not select
by Rust type (`api/lib.rs:889-901,1254-1285,1565-1573`). Req must remain exactly
the interface enum. To multiplex its mailbox, an internal send consists of a
tagged Internal payload followed by an untagged Req::Write wake frame. The
frame's client field is `usize::MAX - senderReplicaIndex`, outside the client
range, and its value is the sender's increasing transport tag. Replicas select
untagged Req messages, then receive the indicated Internal payload by sender
and tag. These frames are invisible replica-to-replica traffic; real client
messages and replies retain their exact enum names and shapes. Setup uses a
reserved tag. Tags are checked against overflow/reserved-tag collision. A frame
may arrive before its payload under bag; the corresponding receive simply
blocks until that payload arrives. No payload or frame is shared across
processes by memory. Each protocol handler begins only after its frame and
payload have been received; no nondeterministic category selection can block
on the wrong message type. Setup similarly uses a tagged receive rather than
consuming traffic of an unknown type.

The Java ACCEPT handler asserts contiguous acceptance
(`MixedProto:521-524`, `DelayedProto:538-541`) and decision
(`MixedProto:723`, `DelayedProto:748`), consistent with its TCP channels. For
bag delivery the source supplies no out-of-order policy. This port buffers
future ACCEPTs and handles the longest contiguous available prefix, retaining
counter/value/ack together. This is an explicit extension rather than disabling
the assertions or executing operations out of log order. FIFO behavior is
unchanged. Cumulative ACKs can overtake each other safely; all leader instances
were already locally accepted before any tail ACK can be generated. Duplicate
ACCEPTs at/below the accepted prefix and old ACKs are ignored. There are no
retransmissions in this failure-free model.

Babel notifications/requests within a replica become direct function calls.
Decision, application execution, and origin reply are processed synchronously
in that replica's handler. The source's separate front/application scheduling
(`MixedFront:161-175`, `DelayedFront:206-231`) does not prescribe a relative
thread scheduling policy, so this port folds it into the single required
replica role. Cross-replica sends remain Must messages. Membership, prepare,
join, reconnect, transfer, all failure handling, and their timers are removed
as requested. All replicas begin active at the fixed ballot, avoiding the
source's initial membership/leadership setup (`DelayedProto:200-277,444-474`).

## Build and requested single run

Build command (inside this directory): `cargo build --release -j 2`.
The initial attempt could not resolve index.crates.io. Retried with
`CARGO_NET_OFFLINE=true cargo build --release -j 2`, using Cargo's cached packages.

Run command (inside this directory):
`timeout 600 target/release/bench_chainpaxos_code_sol count impl --model fifo -- 1 mixed`

The offline build succeeded (`Finished release profile [optimized]`), followed
by a successful invocation of the exact requested `cargo build --release -j 2`
command (exit 0; cached build, 0.12s).

The requested run was invoked exactly once and exited 0. Its count line was:

```text
execs=0 blocked=4
```

The runner exhausted exploration (`stop_reason: exhausted`, 4 iterations,
`max_graph_events: 65`), with no panic/error output. Blocked executions are
expected because replicas never terminate. This is the requested small mixed
FIFO run, not an independent proof or a run of local/bag configurations.
Full captured output is in `target/chainpaxos-run.log`.

The run's last output line was `cache_hits: ` (the field is empty, including
the trailing space after the colon).
