# Pineapple Go-to-Java port

Implementation: `java/bench/pineapple_code_sol/PineappleCodeSol.java`.
All source references below are line numbers in the supplied `code-src` files.
Only files under this directory were read; no web or external source inspection was used.

## Interface and roles

- `PineappleCodeSol(String... args)` implements `MpProgram` and accepts exactly
  `impl,<q>,<fifo|bag>`, with q >= 1. `fifo` uses `CommunicationModel.P2P` and
  `bag` uses `CommunicationModel.ASYN` for every send and receive. The program
  metadata name is `pineapple`, and its size string is the decimal q (the interface
  specifies the entry and parameters but does not prescribe these metadata strings).
- The main thread creates the five named API `MpThread`s in order `n0`, `n1`,
  `n2`, `c0`, `c1`, then starts them in that order. `MpThread` allocates its Tid
  at construction, so all addresses can be initialized before any thread starts.
  The address array is initialization-only shared data; protocol state is private
  to each node. Main sends and receives nothing. Main awaits clients first and
  then nodes using `join1`; the first blocked node await blocks main, as defined
  by the API. There is no shutdown message or node termination condition.
- `c0` and `c1` are the only visible roles. Each performs q sequential operations,
  sends to its own node, consumes and checks exactly one reply of the expected
  type, then continues. Operation kind and write value are exactly `(i+j)%3`
  and `1+i+2*j`. Their records have the exact interface tags and ordered int
  fields: `Write(client,value)`, `Read(client)`, `Rmw(client)`, `Ack()`,
  `Value(value)`, `Result(value)`.
- `n0` has `IsLeader=true`, represented by `id == 0` in tag comparison; the
  other nodes have it false. This replaces the master RPC assignment in
  `pineapple.go:787-790`. Leadership does not gate proposal handling in Go
  (`610-664`), so each attached node coordinates all its own operations,
  including RMW. No request forwarding to n0 is invented.
- Each node has one key, encoded as key 0 in protocol messages. The data value
  is initially 0. An `exists` flag preserves the distinction between an absent
  Go map entry and an explicitly stored zero payload (`119`, `239-245`,
  `653-660`). Absent lookup returns tag `(0,0)` and value 0; a locally proposed
  first Read initializes `(0,nodeId),0`. Tags are Java int pairs, and payloads
  are immutable records. Integers use the interface's int representation;
  there is no unbounded-integer or overflow repair.

## Generic plumbing and internal message representation

`genericsmr.go:27-30,274-295` wraps each client Propose with its reply writer.
Here each interface request is that proposal, and its `client` field supplies
the reply address. The instance retains the original proposal separately from
the mutable accepted command, just as Go retains `lb.clientProposals` separately
from `cmds`. Sequential clients need no additional client command ID or timestamp.
`SendMsg` and `ReplyProposeTS` (`326-331,347-353`) become API sends. Nodes use a
single blocking API receive and dispatch on record type, replacing the channel
select in `pineapple.go:729-783` and the generic RPC decoding (`genericsmr.go:229-270`).

Internal record fields follow the active definitions in `pineappleproto.go`:

| Message | Fields, in declaration order | Receiver action |
| --- | --- | --- |
| `Get` | replicaID, instance, write, key, payload | Return `GetReply` to replicaID; a Read may first install its payload. |
| `GetReply` | replicaID, instance, ok, write, key, payload | Coordinator counts Get round and may reply or broadcast Set. |
| `Set` | replicaID, instance, write, key, payload | Install only if tag is larger; always send SetReply to replicaID. |
| `SetReply` | instance | Coordinator counts Set round and may reply to its original client. |
| `RMWGet` | leaderId, instance, ballot, command | Accept/update instance, then send RMWGetReply to leaderId. |
| `RMWGetReply` | instance, ballot, key, payload | Coordinator counts RMWGet round, modifies local payload and broadcasts RMWSet. |
| `RMWSet` | leaderId, instance, ballot, command, key, payload | Accept/update instance, conditionally install payload, send RMWSetReply to leaderId. |
| `RMWSetReply` | instance, ok, ballot | Coordinator counts completion round and records pending RMW. |
| `ExecuteRMWs` | none | n0 runs its ready reply-executor loop. |

The Java class named `Put` has annotation `@Tag("Set")` to avoid a name conflict
with `java.util.Set`; its protocol name is Set. `Version` has the internal
encoding tag `PineappleTag`, avoiding a Java name conflict with the annotation.
Single-command Go slices are represented by one immutable `MpMessage command`;
the fixed key is 0. Every transmitted record and nested payload is immutable
and value comparable. Numeric IDs in protocol fields are the original replica
indices, not raw API task identifiers; transport destinations use API Tids.
The unused Prepare/PrepareReply/Commit/CommitShort declarations
(`pineappleproto.go:51-63,94-106`) have no registered or dispatched handlers
(`pineapple.go:130-140,729-783`), so no such rounds are added.

All broadcasts send to the other two nodes in cyclic index order, exactly as
the Go broadcast loops (`216-230,342-363,407-419,510-523`). There are no
self-quorum messages: the coordinator's contribution remains the literal +1
in the counters. The execution scheduling message described below is separate.
There is no message loss, timeout, retry, or deduplication added.

## Tags and ABD

- Tag comparison is the exact receiver-local function `isLargerTag`
  (`pineapple.go:147-165`): larger timestamp wins; equal identical IDs do not;
  when this node is leader and the current tag ID is its ID, a different
  received ID wins; otherwise larger ID wins. This is deliberately not
  replaced with a globally leader-aware comparator.
- Propose allocates the next unoccupied local instance (`610-646`). All
  instance fields retain their zero defaults, including a Write's initialTag
  `(0,0)`. Accepted RMWs and locally coordinated ABD/RMWs share the same
  instance map. Remote instance numbers do not advance `crtInstance` directly.
  Instance-number collisions, command overwrites, and retained local
  bookkeeping follow the Go handlers; the port does not namespace instances
  by coordinator or introduce a global log.
- Write starts a Get with `write=1` and zero payload; Read initializes its
  local payload if absent, captures initialTag, and starts Get with `write=0`
  carrying current data (`197-230,648-663`).
- A read Get installs the incoming data if the key is absent or the tag is
  larger, and returns the resulting data. A write Get returns zero payload,
  regardless of the receiver's stored data (`233-257`). This surprising
  behavior is preserved.
- GetReply ignores late replies after getDone, appends the reply, installs a
  strictly larger payload, and counts OK replies (`260-280`). The literal
  condition is `getOKs + 1 > (3 >> 1)`, so the first positive remote reply
  completes the Get quorum. No sender-distinctness check is added.
- Identical-response counting, the initialTag equality-or-larger test, and
  population of hasMaxTag follow `281-301` exactly. A Read completes directly
  when `identicalCount == receivedDataCount+1` (`303-308`). Otherwise the
  instance becomes PREPARED; a Write increments the current local timestamp
  and sets ID to its coordinator ID, retaining the current local value
  (`310-320`). The proposal's Write value is never assigned by Go
  (`617-621,648-663,314-317`), so it is intentionally not assigned here.
  Adding that assignment would be a protocol repair outside the requested
  value/reply adaptations.
- Set broadcasts omit a peer recorded in hasMaxTag only for Reads
  (`355-359`). Set receivers install only a larger tag and acknowledge
  unconditionally (`368-377`). SetReply increments setOKs even after an
  operation has replied. The exact disjunction from `387-388` is retained:
  `(setOKs+1 > hasMaxTag.size && hasMaxTag.size < (N>>1) && size > 0)`
  OR `setOKs+1 > (N>>1)`. The first branch cannot hold for N=3, but is not
  replaced with a different quorum. The completed flag prevents duplicate
  client replies (`168-178`).

## RMW rounds, execution, and required interface adaptations

- An Rmw Propose at any node allocates a local rmwId, with ballot 0, and
  broadcasts RMWGet (`636-647`). The defaultBallot stays 0; there is no leader
  change or prepare round. Incoming obsolete ballots raise an assertion,
  matching the Go panic branches rather than introducing recovery.
- RMWGet on a missing instance creates an ACCEPTED instance with no local
  proposal and replies with zero payload (`422-439`). On an existing instance
  it checks ballot, overwrites cmds, preserves COMMITTED status if present,
  and replies with local data (`440-450`). It does not update the existing
  ballot in this path. These asymmetries are preserved.
- RMWGetReply ignores late replies, accumulates payloads, and completes with
  `rmwGetOKs+1 > (N>>1)` (`456-478`). Its largest-tag loop deliberately assigns
  the current reply's payload, as Go does at `471-474`, rather than assigning
  the loop variable. It increments timestamp, sets tag ID to the coordinator,
  and modifies the local value. **Required adaptation:** `+10` implements the
  interface f; Go uses `+1` with `TODO: update RMW modify` at `483`.
  Modification happens here, not again in the reply executor. RMWSet carries
  the current instance command and the resulting payload (`480-490,501-507`).
- RMWSet creates an ACCEPTED instance when absent; on a higher ballot it
  updates command, ballot, status; on equal ballot it overwrites command but
  preserves COMMITTED status. It stores receivedRMW and installs it only if
  larger. Its reply always has OK=1 and defaultBallot=0 (`526-565`). No new
  COMMITTED transition is invented; that status is only preserved as in Go.
- RMWSetReply ignores replies with `inst.rmwId <= rmwDoneUpTo`, increments
  the raw counter without checking OK or ballot, and on
  `rmwSetOKs+1 > (N>>1)` assigns pendingRMWs[rmwId] and increments rmwDoneUpTo
  by one (`568-580`). The port does not replace this with maximum-ID or
  contiguous-prefix tracking.
- Replica 0's Go executor is a separate goroutine (`584-608,718-720`). The
  interface requires exactly one process per node. **Scheduling decision:**
  a newly completed RMW quorum at n0 sends `ExecuteRMWs()` to n0 itself.
  Its delivery runs the ready executor loop on n0, allowing intervening
  protocol messages and keeping all node state within one process. It drains
  pending IDs in order using the Go loop bound, replies once, and advances
  its private execution cursor. An empty slot raises an assertion, preserving
  the Go nil-slot failure rather than silently repairing it. No busy polling
  or periodic executor wakeup is needed when no RMW is ready. No extra role
  or shared-state worker is introduced.
- **Required Read reply decision:** `replyClient` returns `state.NIL` in Go
  (`168-176`); instead a Read returns `Value` with the coordinating node's
  current data value precisely when that handler invokes replyClient, either
  the optimized Get completion (`305-306`) or Set quorum (`387-389`). No
  earlier snapshot is substituted. Writes receive Ack at these same Go
  completion points.
- **Required leader RMW reply decision:** the executor also returns
  `state.NIL` (`591-598`). It sends `Result` with n0's current data at the
  point the scheduled executor completes the operation. Intervening data
  changes can therefore affect the returned value; the port does not cache
  the value computed at RMWGet quorum as a substitute.
- **Required missing nonleader reply decision:** Go handles an RMW Propose
  regardless of leadership (`636-647`), but starts executeRMWs only on replica
  ID 0 (`718-720`); consequently an RMW attached to n1 never gets a Go client
  reply. At a nonzero coordinator, the Java port sends Result once at its
  RMWSet quorum, after updating pendingRMWs and rmwDoneUpTo (`577-580`), using
  that coordinator's data at that exact completion point. This adds only the
  missing answer; it neither forwards the operation nor transfers ownership
  to n0. n2 has no attached client in this interface. Replies always go
  directly from the coordinator to the client identified by the original
  local proposal; acceptors do not answer that client.
- Dreply is treated as true so the required client interface has replies
  (`genericsmr.go:54-55,71,85-86`; `pineapple.go:170,591`). Exec is not used
  to add state.Command.Execute calls: the supplied Pineapple code has no such
  execution on these paths. The single command and its original proposal are
  retained even if incoming accepted commands overwrite the instance command.

## Scheduling and omitted machinery

The source handlers are deterministic; no protocol value or quorum-member
choice needs a new `Nondet` event. Blocking API receive delivery order chooses
which pending message is handled next, including n0's executor message. This
replaces Go's ready-channel select by the interface's fifo/bag delivery model;
there is no fixed priority between message types and no custom transport queue.
Every exchange, including executor scheduling, is an API send/receive.
The executor scheduling choice above is explicitly a message-arrival choice.

As requested, networking, serialization/RPC codes, durability
(`pineapple.go:668-701`), failure/Alive tests, master RPC, clock and batching
(`703-708,722-735,760-761`), and generic beacons are omitted. Proposals are
always eligible for reception; there is no batching gate. Initial setup uses
known addresses, so Java needs no extra setup process or setup messages.
Node receive loops are infinite, including after both clients finish.

## Compilation and the single requested run

Both commands were executed from this directory, with no further launcher run.

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Compilation succeeded with exit status 0 and no output.

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.pineapple_code_sol.PineappleCodeSol:impl,1,fifo' must 0 1 report - 600 </dev/null
```

The launcher exited 0: 325 iterations, 325 distinct blocked graphs, 0 complete
graphs, exploration stack exhausted (`stop_reason: exhausted`). Nodes stay
blocked on receive as required, so blocked executions are expected. The run's
q=1 exercises c0's Write and c1's Read; it does not exercise RMW or bag delivery.
The launcher also emitted a JVM perf-file warning and a logging configuration
error at startup; neither prevented exploration or caused a nonzero exit.

The last output line was exactly:

```text
max_porf_count: 0
```
