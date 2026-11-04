# Pineapple single-key implementation

The source is `src/main.rs`. This implements section 3's stable-leader, no-failure
case, Algorithms 1 and 2, with three replicas and majority size two. There are
no ballots, leader elections, reconfiguration, multi-key operations, retries, or
failure simulation. Both `Program.spec` and `Program.imp` construct this same
implementation; an external harness supplies the specification.

## Roles and execution

The main process starts `n0`, `n1`, `n2`, then `c0`, `c1`, then `init`. It only
creates and joins threads; it never sends or receives. `visible` returns precisely
`c0`, `c1`. Every process exchange uses `send_msg` and `recv_msg_block`; process
state and protocol queues are thread-local. The `Arc` in the entry point holds
only the reusable program closure, not shared protocol state.

`init` distributes `Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }` to
each node using the address-frame serialization described below, then exits.
Nodes buffer requests and protocol packets received before all their setup
frames. Clients need only their attached node's address, supplied when spawned.

The client enums and client function are verbatim from INTERFACE.md. Each client
executes its prescribed q requests sequentially and terminates after q replies.
`c0` uses `n0`; `c1` uses `n1`. Nodes run infinite receive loops, including after
all clients terminate. Main eventually blocks joining these nodes; the harness
ends the execution with the nodes blocked on receive.

Each node has the replica pair `(value, pstamp)`, initially
`(0, ((0, 0), 0))`, a coordinator sequence number, one active local operation,
and a FIFO queue of pending jobs. `n0` also has its monotonically increasing RMW
slot counter. Background handlers remain active during both coordinator phases.
The queue order and quorum formation follow message arrival order; no random
choice is needed, so there are no `nondet()` calls.

## Pstamps and messages

`Pstamp { ts, writer, slot }` derives lexicographic ordering in exactly that field
order. This is the paper's `((tag.ts, tag.id), slot)`, not an ordering with slot
first. Replica state changes only for a strictly larger full pstamp. Every PUT,
including an equal or stale PUT, receives an acknowledgment.

Node protocol messages are represented locally by `Packet { kind, from, seq,
data }`, where `data` is a `Stored { ps, value }`. The key field is implicit:
there is only one key. The sender index gives the reply address. `seq` identifies
the coordinator's operation, and replies echo it. Reply kinds distinguish the
get and put phases. Distinct replica indices are counted once per phase;
late replies for an old sequence or phase are ignored.

| Message | Sender, receiver, and effect |
| --- | --- |
| `Write(i,v)` | Client i to node i; schedule an ABD write. |
| `Read(i)` | Client i to node i; schedule an ABD read. |
| `Rmw(i)` | Client i to node i; schedule at n0, or forward there. |
| `Ack` | Proxy to its client, after an ABD write's PUT majority. |
| `Value(v)` | Proxy to its client, after an ABD read's PUT majority; returns the get-phase selection. |
| `Result(v)` | Proxy to its client, after the RMW PUT majority and, for c1, the leader's result message. |
| `GetTag` | Coordinator to all three nodes, including itself; requests the stored pstamp. |
| `AckTag` | Replica to coordinator; current pstamp, with unused value zero. |
| `GetValue` | Read proxy or RMW leader to all three nodes, including itself. |
| `AckValue` | Replica to coordinator; current value and pstamp. |
| `Put` | Coordinator to all three nodes, including itself; conditionally installs the supplied pair. |
| `AckPut` | Replica to coordinator, unconditionally after handling PUT. |
| `Forward` | Non-leader proxy to n0; includes the original client index in `data.value`. |
| `Done` | n0 to originating proxy, after its RMW PUT majority; carries the computed output. The proxy replies `Result(output)` to its waiting client. |

Client messages on the wire retain precisely the required `Req` and `Rep`
names and tuple variants. For internal communication only, `Req::Write` is also
used as a two-integer wire carrier. Its first field is outside the client-index
range, so it is unambiguous and invisible to the client projection. This lets
each node receive every possible incoming message with one blocking receive
and then dispatch locally. There is no polling, shared mailbox, helper receiver
process, or nondeterministic selection of an empty typed inbox.

Address frames are `Req::Write(2 + address_index, thread_id)`, for address indices
0..4 (three nodes, then two clients). They reconstruct the `Setup` vectors.
Each index is explicit, so bag delivery can reorder the setup frames. The
dependency's opaque 32-bit ThreadId is copied to u32 and back with two isolated
`unsafe` bit casts. The compiler checks equal sizes. Only bit patterns obtained
from actual ThreadIds created in the same execution are transmitted and
reconstructed; no address is fabricated, incremented, or inferred from process
creation order. This encoding relies on the pinned dependency's ThreadId
representation being a fully initialized 32-bit identifier; it is not a
portable serialization API for arbitrary future ThreadId representations.

For a protocol packet, let B = 2*q + 1. Its first wire field is:

```text
8 + (((((slot * B + ts) * B + seq) * 3 + writer) * 3 + from) * 8 + kind)
```

The second is the full u64 value. Kind codes 0..7 are, respectively, `GetTag`,
`AckTag`, `GetValue`, `AckValue`, `Put`, `AckPut`, `Forward`, `Done`. Decoding
uses division and remainder in the reverse order. Thus every protocol packet is
one send and one receive, with no multi-frame reassembly. Tags, slots, and
coordinator sequences are bounded by the total number of operations 2*q, so B
is sufficient. Setup needs five separate address frames per node. Startup
buffering preserves the receive order of non-setup messages.

## ABD paths (Algorithm 1)

A write broadcasts `GetTag` to all nodes, waits for the first two distinct
`AckTag` replies, and selects the greatest full pstamp. It constructs
`((max.ts + 1, proxy_index), max.slot)` and broadcasts `Put` with the client's
new value. It sends `Ack` to the client only after two distinct `AckPut` replies.
In particular, the writer does not retain the old writer ID and does not reset
the slot field.

A read broadcasts `GetValue`, waits for two distinct replies, selects the pair
with the greatest full pstamp, and broadcasts exactly that pair in `Put`. It
waits for two distinct `AckPut` replies before returning the selected value.
There is no fast read path, even for matching get responses.

ABD quorums need not contain either the proxy or the leader. Self contacts are
ordinary messages; there is no direct replica read substituted for the get
phase and no direct replica write substituted for its PUT handler.

## RMW path (Algorithm 2)

Only n0 executes RMWs. A request through n1 makes the path
`c1 -> n1 -> n0 -> n1 -> c1`, with the two replication rounds inside n0's work.
The proxy keeps the waiting client index locally; sequential clients and
reliable, nonduplicating messaging allow one outstanding forwarded RMW per
attached client without a separate forwarding request number.

n0 processes its queued jobs sequentially, so it cannot start a later RMW
before finishing the preceding RMW. It broadcasts `GetValue` to all nodes.
Its selected quorum is exactly two nodes: itself and the first other node to
reply. If both others reply before itself, it waits for its own reply and
selects the earlier other response. It applies f(v) = v + 10 to the greatest
pair in this quorum, increments its independent local RMW slot, and assigns
`(selected.tag, leader_slot)` to the result. The slot is never reset for a new
ABD tag. An assertion checks that the new slot exceeds the selected slot.

The leader retains its computed pair locally if newer, before broadcasting
`Put` to all nodes. This is the leader's knowledge of its own decision, not
an acknowledgment or an extra quorum vote. This retention is essential when
the preceding PUT majority did not contain n0: the next get quorum must still
observe that RMW or a higher pstamp. Self GET and PUT exchanges still go through
the message API. n0 waits for any two distinct PUT acknowledgments, then sends
the computed result to c0 directly or sends `Done` to the requesting proxy.
The result is the RMW's computed output, even if a concurrent, later-pstamp
write has since superseded it at a replica.

## Decisions where the paper leaves details open

The quotations below are from the supplied PAPER-EXCERPT.md. The interface's
requirements control naming, workload, initial value, and process lifecycle.

1. **Initial pstamp.** The paper says, "slot is a positive integer that
   represents the slot index of the Multi-Paxos component." It does not assign
   the initial pstamp. Slot zero and tag (0,0) are reserved for the initial
   value; actual RMW slots start at one and actual write timestamps at one.
2. **Replica identities and key.** The paper says, "id is the identifier of the
   writer proxy" and uses messages containing `key`. Node index is the writer
   identity. The unique key is implicit in every packet.
3. **Proxy scheduling.** The paper says, "First, RMWs go through the leader;
   hence the ordering is stable." It does not describe concurrent invocations
   inside a proxy. Each node coordinates at most one job at a time, queues new
   jobs in arrival order, and continues servicing every background message.
   n0's queue includes its local ABD operations and all RMWs. No extra ordering
   between different proxies' ABD operations is imposed.
4. **Choosing quorums.** The paper says, "Contact a quorum Q, which includes
   the leader itself" and "both ABD and Multi-Paxos use a majority quorum."
   It does not specify how replies are selected. ABD uses the first two distinct
   responses; the RMW get uses self plus the first other response. Every PUT
   uses the first two distinct acknowledgments. Broadcasts use fixed node-index
   order, as a local loop implementing "send ... to all"; quorum membership
   follows reply arrival, without an additional choice. Equal pstamps denote
   the same immutable pair, so choosing either equal response needs no choice.
5. **Leader's local knowledge.** The paper says, "the leader has the most recent
   information about prior RMWs" and says to "increment[] the local slot index
   ... directly." It does not show a separate leader decision variable or its
   update. The local counter is incremented once per RMW and the leader retains
   its computed pair before sending PUTs. A newer concurrent ABD value is never
   overwritten by an older retained decision.
6. **Forwarding and returning.** The paper says, "RMW takes 3-RTT if the client
   is not co-located with the leader." It does not define the forwarding or
   reply messages. `Forward` routes the operation to n0; `Done` returns the
   computed value to its proxy, which issues the required client `Result`.
   These messages do not add an extra replication phase.
7. **Request correlation.** The paper says, "wait until receiving ... from
   quorum" but shows no operation identifiers, duplicate counting, or treatment
   of late replies. A per-coordinator increasing sequence is echoed in replies;
   response kinds identify phases; per-node boolean arrays prevent duplicate
   counting. Completed/old-phase replies are consumed and ignored. The transport
   has no retries or duplicate production.
8. **Optional optimizations.** The paper says, "ABD can complete on the fast
   path when the quorum returns the same response." The optimization's complete
   rules are not in the excerpt. This implementation always performs the two
   rounds explicitly required by Algorithm 1 and the task.
9. **Encoding and setup.** The paper says, "send ... to all" and gives logical
   message contents but does not define a Rust wire format, heterogeneous inbox,
   or process bootstrapping. INTERFACE.md requires `Setup` and buffering before
   setup. The uniform Req carrier and indexed setup serialization above implement
   these internal details without altering the prescribed external messages.
   This choice follows the supplied API's `expect_msg` downcast behavior.
   ThreadId has no public integer constructor; the address-only round-trip
   bit casts described above bridge that opaque type for setup serialization.
10. **Finite arithmetic.** The paper calls the slot "a positive integer" but
    gives no overflow rule. Values are the interface's u64; RMW addition is
    checked. Metadata arithmetic is validated before starting threads: q must
    fit the mixed-radix metadata in usize (8*3*3*(2*q+1)^3 + 8). Excessively large
    q is rejected rather than wrapped, and runtime encoding checks its counters.
    This is a representation limit, not an additional protocol round or choice.
11. **Arguments and delivery.** The paper says, "operations may be received in
    an arbitrary order." The optional program argument `fifo|bag` is validated;
    delivery semantics are selected by the harness's `--model`, as required by
    INTERFACE.md. No in-program scheduler overrides the chosen harness model.
12. **Lifetime and completion.** The paper's background handlers say "Upon
    receiving" without a shutdown procedure. Nodes never terminate, as required
    by the task. Client replies wait for the stated PUT quorum; a node keeps
    receiving and draining late/background messages afterward.

## Build and required single run

All commands are executed inside this directory. Only source files in this
directory were read; Cargo compiles the explicitly permitted dependency.

Requested compile command:

```sh
cargo build --release -j 2
```

The first invocation tried to update the registry and encountered DNS failures.
It was interrupted. The cached dependencies were then used with
`CARGO_NET_OFFLINE=true cargo build --release -j 2`. Compilation exposed that
ThreadId is an opaque 32-bit struct rather than a public tuple or constructible
integer wrapper; setup address serialization was adjusted accordingly. The
final unmodified requested command `cargo build --release -j 2` succeeded with
exit status 0 and ended with:

```text
    Finished `release` profile [optimized] target(s) in 1.06s
```

A local text comparison also confirmed that the client function, its constant
and value function, and the external Req/Rep declarations match INTERFACE.md
verbatim. No additional invocation of the implementation binary was made.

Required single run command:

```sh
target/release/bench_pineapple_paper_sol count impl --model fifo -- 1
```

Build and run results will be recorded here after completion. The required q=1
workload exercises c0's write and c1's read; it does not exercise an RMW.
