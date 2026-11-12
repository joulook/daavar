# cassandra-rr-port: Cassandra 4.1 quorum write path and quorum read path with read repair, ported to JMC's Must API

Program `bench.cassandra_rr_port.CassandraReadRepair` (one file, `java/bench/cassandra_rr_port/CassandraReadRepair.java`),
constructed from the launcher's argument string `impl,<q>,<blocking|none>,<fifo|bag>`. It ports the sources of the study's cut
`plan/case-studies/cassandra/read-repair-src` (paths below relative to `src/java/org/apache/cassandra`; line numbers of the cut)
following `plan/case-studies/cassandra/rr/INTERFACE.md`: 3 replicas `n0..n2` (each the coordinator of its client), 2 clients
`c0`, `c1`, one partition with the regular cells `a` and `b`, replication factor 3, QUORUM = 2 for every read and write,
read-repair strategy `blocking` (`ReadRepairStrategy.BLOCKING` -> `BlockingReadRepair`) or `none` (`NONE` ->
`ReadOnlyReadRepair`). Every line that departs from the source is marked `// PORT:` with the reason.

## Processes

| process | kind | role in the source |
|---|---|---|
| `c0`, `c1` | visible (`MpProcess.spawn("c<i>")`), terminate | the interface's client loop, verbatim: `ci` sends to `ni` and waits for each reply |
| `n0`, `n1`, `n2` | invisible daemons, never terminate | one process per node: the node's memtable (`AtomicBTreePartition`, the one row), the coordinator side (`StorageProxy` with its `MessagingService` callbacks, `RequestCallbacks`, `ResponseVerbHandler`) serving the node's one client, and the replica side's verb handlers (`MutationVerbHandler`, `ReadCommandVerbHandler`, `ReadRepairVerbHandler`) on the node's stages (`Node`) |
| `init` | invisible daemon | sends each node its one `Setup` message (the addresses of all processes) before anything else |

Main spawns, in this order: `n0`, `n1`, `n2`, `c0`, `c1`, `init`; then it awaits `c0` and `c1` and nothing else; it sends and
receives nothing. `rolesInSpawnOrder() = visibleRoles() = [c0, c1]` (a daemon carries no role name in this API: the nodes are
`n0`, `n1`, `n2` by spawn order).

How one process stands for the node's threads (the request thread waiting on its callbacks and the MUTATION, READ and
REQUEST_RESPONSE stage workers): the node has one receive loop. Whenever the coordinator side waits on a callback
(`Node.awaitUntil`: `AbstractWriteResponseHandler.get`, `ReadCallback.awaitResults`, `BlockingPartitionRepair.awaitRepairsUntil`)
and whenever it is idle (`Node.run`), it receives every message addressed to it and dispatches it (`Node.dispatch`, the
inbound message handler): a response is delivered to the callback registered under its id, which is removed first
(`ResponseVerbHandler.doVerb` -> `RequestCallbacks.remove`); a response without a callback is dropped; a `MutationReq`
(`Verb.MUTATION_REQ`, stage MUTATION) is handed to the MUTATION stage as a task the node sends to itself (`MutationTask`),
which executes (`MutationVerbHandler.doVerb`) when the node receives it, at any later receive point; a `ReadReq`
(`Verb.READ_REQ`, stage READ) and a `ReadRepairReq` (`Verb.READ_REPAIR_REQ`, stage MUTATION) are executed by their handlers
at receipt. A local mutation (`performLocally`, `Stage.MUTATION.maybeExecuteImmediately(LocalMutationRunnable)`) is a
MUTATION-stage task like a remote one, registered under a fresh id and answering the handler directly; a local read
(`Stage.READ.maybeExecuteImmediately(LocalReadRunnable)`) runs inline and hands its response to the callback directly
(`ReadCallback.response`), as the source does without a message.

What the one-process model has of the source's scheduling, and what it does not. The source's stages are
multi-threaded, so at a node any received task may execute before or after any other, and before or after later-received
messages. The port's node executes its tasks as follows: a MUTATION_REQ (remote or the node's own performLocally) becomes
a stage task (`MutationTask`) that executes at any later receive point of the node, in the order the node's mutation
tasks were dispatched (the self-channel is FIFO under `fifo`; the node's own READ_REPAIR_REQ travels on the same channel
and is received behind them); a READ_REQ, a READ_REPAIR_REQ and a response callback execute at receipt; a local read runs
inline at submission (`maybeExecuteImmediately` with a free worker). Orders of the source the port has: a mutation task
against any later-received request or response of any sender, under `fifo` too; tasks of different senders in any order
the delivery model and the causal order of their sends allow; a read's data and digest responses against the other
nodes' requests. Orders of the source the port does not have, each with the reason it changes no client-visible history
(the clients' words and the causal pasts of their events); none of these arguments is a simulation proof, they are the
cases the reviewers and the author examined (plan/case-studies/java-ports-1112/cassandra/REVIEW-*.md):
- Two mutation tasks of one node in the reverse of their dispatch order (the mutations of two successive writes of one
  coordinator, the first still pending; or two senders' mutations whose dispatch order is fixed by `fifo` or by
  causality), and a node's own repair against its pending mutation tasks. The memtable merge (`Cells.reconcile`) gives
  the same final row either way; the intermediate row with the later mutation applied and the earlier not is a row with
  the larger timestamps (the values grow with the operation index), which a read observes as the state after both (it is
  not reachable at q <= 3 at all: one coordinator has one pending mutation at a time); each acknowledgement goes to its
  own handler; the repair of the node's own value by a read of another coordinator has the same words through the read
  plan that repairs the other contact.
- A read, a repair or a response callback executing after a request received later (the source may queue them; the port
  runs them at receipt), and a read executed after a later-received response. The later request changes what a read
  snapshots only if it is a mutation or a repair; when `fifo` and the causal order allow that mutation to arrive before
  the read, the receive order has that schedule; when they do not (a node answering another coordinator's read while
  its own read is in flight), the same reply values arise through the read plan that contacts the other replicas or
  through the digest mismatch and the second round; a repair's apply commutes with any other apply; a response callback
  only changes the handler's count or accumulator, which the coordinator reads at its next wait point.
- A local read queued behind a later-received mutation (`maybeExecuteImmediately` without a free worker): when that
  mutation is not causally after the read's own sends, the schedule in which it arrives before the read is submitted
  has the same snapshot; when it is, the same reply comes from the second round or from another read plan.
The one-process model orders every task of a node by program order (the interface fixes one process per node); the
causal pasts of the visible events therefore include edges through a node's successive tasks that the source's
separate stage threads do not create (and that the source's worker reuse creates in part). This is the observation
contract of every one-process model in the study (the specifications' objects, port_R's serial replica thread), not a
choice of this port. Within it, the receive order of the strategy places a mutation task at every receive point after
its dispatch and every other task at its receipt; the arguments above are what supports the claim that no client-visible
history is lost, case by case, not a general simulation result.

## Messages

Visible (the interface, `@Tag` = the variant name of the Rust side): `Write(int client, long value)`, `ReadA(int client)`,
`ReadAB(int client)` (client -> its coordinator); `Ack(int client)`, `ValueA(int client, long a)`,
`ValueAB(int client, long a, long b)` (coordinator -> client).

Invisible (net/Verb.java), with the `net/Message` header reduced to the message id (the callback's key in
`net/RequestCallbacks`) and the address to respond to; a response names its replica by index (`Message.from`):

| message | from -> to | source |
|---|---|---|
| `Setup(nodes, clients)` | init -> each node | the interface's setup message |
| `MutationTask(id, respondTo, Mutation, local)` | a node -> itself | a task of the MUTATION stage: a received MUTATION_REQ handed to the stage by the inbound handler, or the local mutation of performLocally |
| `MutationReq(id, respondTo, Mutation)` | coordinator -> another node | MUTATION_REQ (`sendToHintedReplicas`) |
| `MutationRsp(id, from)` | node -> coordinator | MUTATION_RSP (the empty response, `MutationVerbHandler.respond`) |
| `ReadReq(id, respondTo, ReadCommand)` | coordinator -> another node | READ_REQ, a data or a digest query (`AbstractReadExecutor.makeRequests`, `AbstractReadRepair.sendReadCommand`); a local read runs inline (`LocalReadRunnable`) |
| `ReadRsp(id, from, ReadResponse)` | node -> coordinator | READ_RSP (`ReadCommandVerbHandler.doVerb`) |
| `ReadRepairReq(id, respondTo, Mutation)` | coordinator -> a node (itself included: `sendRR` goes through `MessagingService`) | READ_REPAIR_REQ (`BlockingPartitionRepair.sendInitialRepairs`) |
| `ReadRepairRsp(id, from)` | node -> coordinator | READ_REPAIR_RSP (`ReadRepairVerbHandler.applyMutation`) |

Data: `Cell(value, timestamp)` (db/rows/Cell; timestamp 0 = the absent cell; `withSkippedValue` keeps the timestamp and
drops the value, 0 standing for the empty value), a row = one `Cell` per column `[a, b]`; `Mutation(update)` = the
`PartitionUpdate` with one cell per column (absent = not touched); `ReadCommand(queriedColumns, isDigestQuery)` with
`fetchedColumns()` = both columns; `ReadResponse(isDigestResponse, data, digestValue)`.

## The protocol, role by role

- Client `ci`: the interface's loop.
- Node `ni` (`Node.run`): receives its client's request; `Write(c, v)` -> `StorageProxy.mutate` of the mutation
  `a = v, b = v` at timestamp `v`, then `Ack(c)`; `ReadA(c)` -> `StorageProxy.read` of the command querying `a`, then
  `ValueA(c, a)`; `ReadAB(c)` -> the command querying `a, b`, then `ValueAB(c, a, b)`. Every other message (responses,
  requests, stage tasks) is dispatched as described under Processes, both while a handler waits (`Node.awaitUntil`) and
  while the node is idle.
- Write (`StorageProxy.mutate` l.873-905 -> `performWrite` l.1360-1385 -> `sendToHintedReplicas` l.1464-1560): the write plan
  is every replica (`ReplicaPlans.forWrite`, writeNormal); `WriteResponseHandler` with `responses = blockFor = writeQuorum = 2`;
  the local replica applies on its MUTATION stage (`performLocally`, a `MutationTask` of the node itself), then the other
  contacts get `MutationReq` in contact order; `maybeTryAdditionalReplicas` (cheap-quorum upgrade) is time-based and does
  nothing; `get()` waits until two responses arrived (`onResponse` decrements, signal at 0). The third response, arriving
  after `get()` returned, still finds its registration and decrements the handler to -1 (`WriteResponseHandler` l.55-58),
  with no effect.
- Read (`StorageProxy.read` -> `readRegular` -> `fetchRows` l.2068-2133):
  1. `AbstractReadExecutor.getReadExecutor` (l.190-212): `ReplicaPlans.forRead` (candidates sorted by proximity, contacts
     the first `blockFor` = 2), `NeverSpeculatingReadExecutor` (no speculative retry: the interface), with the read repair of
     the strategy (`ReadRepair.create`), a `DigestResolver` and a `ReadCallback` with `blockFor = readQuorum = 2`.
  2. `executeAsync` (l.178-185): `makeFullDataRequests` to the first contact, `makeDigestRequests` to the other; in
     `makeRequests` (l.134-170) a remote replica is sent the command with the callback and the local read, if any, is run
     last on the READ stage (`LocalReadRunnable`, inline).
  3. `maybeTryAdditionalReplicas`: nothing (never speculating).
  4. `awaitResponses` (l.396-440): `ReadCallback.awaitResults` until the data response is present and 2 responses were
     accumulated (`onResponse` l.179-202); `DigestResolver.responsesMatch` (l.105-138) compares the digest of every
     response (the data response's digest is computed from its data, `ReadResponse.digest`); on a match the result is the
     data response (`getData`), else `readRepair.startRepair`.
  5. `startRepair` (`AbstractReadRepair` l.126-153, both strategies): a `DataResolver` with this read repair, a new
     `ReadCallback` (blockFor 2), a full data read to every contact (`sendReadCommand` l.96-120: the local one on the READ
     stage, the other with the callback).
  6. `maybeSendAdditionalDataRequests` -> `maybeSendAdditionalReads` (l.185-203): time-based, nothing.
  7. `awaitReadRepair` -> `awaitReads` (l.156-173): wait for the full data reads, then `DataResolver.resolve` (l.94-135,
     212-225, 269-299): the responses are merged cell by cell with `Cells.reconcile`; with more than one response and a read
     repair that is not `NoopReadRepair` the merge is reported to the strategy's merge listener:
     `BlockingReadRepair.getMergeListener` -> `RowIteratorMergeListener` (`onCell` l.138-142: a merged cell that differs from
     the source's and is queried, `isQueried` l.144-154, goes into that source's repair; `close` l.367-400: one mutation per
     write-plan contact with a repair, `BlockingReadRepairs.createRepairMutation` null for none, then
     `readRepair.repairPartition`); `ReadOnlyReadRepair.getMergeListener` -> NOOP.
  8. `BlockingReadRepair.repairPartition` (l.112-116) -> `BlockingPartitionRepair` (l.69-100: the latch is `writeQuorum`
     minus the write-plan contacts that get no mutation, clamped at 0; the write plan is `ReplicaPlans.forReadRepair`
     l.434-437 + `writeReadRepair` l.551-575: the two contacts already read from, which reach `blockForWrite` = 2) ->
     `sendInitialRepairs` (l.140-158): `ReadRepairReq` to each repaired contact (the local one to its own stage as well).
  9. `getResult`; then, as `concatAndBlockOnRepair` (l.2029-2053) does when the merged result is closed:
     `maybeSendAdditionalWrites` (time-based, nothing) and `awaitWrites` (`BlockingReadRepair` l.83-108 ->
     `awaitRepairsUntil` l.172-184: the latch, decremented by `ack` l.112-117 on each `ReadRepairRsp`); for
     `ReadOnlyReadRepair` both are empty.
  10. The reply: the queried cells of the result row (`a`, or `a` and `b`); an absent cell reads as 0.
- Replica side (`Node.dispatch`): `MutationReq` -> the MUTATION stage's task; `MutationTask` -> `MutationVerbHandler.doVerb`
  (l.45-73): apply (the memtable merge, `Cells.reconcile` per column) then respond (a local task answers its handler
  directly); `ReadReq` -> `ReadCommandVerbHandler.doVerb` (l.50-128): `executeLocally` (the row restricted to the fetched
  columns, the value of a fetched-but-not-queried cell skipped, `BTreeRow.filter`) then `createResponse` (data or digest)
  and respond; `ReadRepairReq` -> `ReadRepairVerbHandler` (l.31-42): apply, respond.

## Cassandra methods and their counterparts

| Cassandra (cut file: method, lines) | here |
|---|---|
| StorageProxy.mutate (873-905), performWrite (1360-1385), sendToHintedReplicas (1464-1560), performLocally (1549, 1646-1655) / LocalMutationRunnable, LocalReadRunnable | `StorageProxy.mutate`, `performWrite`, `sendToHintedReplicas`, `performLocally`, `localReadRunnable` |
| AbstractWriteResponseHandler.get (113-145), blockFor (220-224); WriteResponseHandler.onResponse (40-61) | `AbstractWriteResponseHandler.get`, `blockFor`; `WriteResponseHandler.onResponse` |
| StorageProxy.readOne/read/readRegular (1820-2019), fetchRows (2068-2133), concatAndBlockOnRepair (2029-2053) | `StorageProxy.read`, `fetchRows` (the close of concatAndBlockOnRepair inlined at its end) |
| ReplicaPlans.forWrite (writeNormal), forRead (700-709), contactForRead (660-680), forReadRepair (434-437), writeReadRepair (551-575); ReplicaPlan.ForTokenRead.readQuorum (99), ForWrite.writeQuorum | `ReplicaPlans.forWrite`, `forRead`, `forReadRepair`; `ReplicaPlan.ForTokenRead`, `ForWrite` |
| AbstractReadExecutor.getReadExecutor (190-212), makeFullDataRequests/makeDigestRequests/makeRequests (116-170), executeAsync (178-185), awaitResponses (396-440), awaitReadRepair (442-456), maybeSendAdditionalDataRequests (465-471), getResult (473-477); NeverSpeculatingReadExecutor (258-283) | the same names in `AbstractReadExecutor` / `NeverSpeculatingReadExecutor` |
| ReadCallback: constructor (83), awaitResults (126-151), onResponse (179-202), response (219-224) | `ReadCallback.awaitResults`, `onResponse`, `response` |
| ResponseResolver.preprocess (60-68); DigestResolver.preprocess (56-62), getData (77-101), responsesMatch (105-138), isDataPresent; DataResolver.resolve (94-135) -> resolveWithReadRepair (212-225) -> resolveInternal (269-299) | `ResponseResolver.preprocess`; `DigestResolver.*`; `DataResolver.resolve` |
| ReadRepair.create / ReadRepairStrategy (BLOCKING, NONE) | `ReadRepair.create`, `ReadRepairStrategy` |
| AbstractReadRepair.sendReadCommand (96-120), startRepair (126-153), awaitReads (156-173), maybeSendAdditionalReads (185-203) | the same names in `AbstractReadRepair` |
| BlockingReadRepair.getMergeListener (62-65), maybeSendAdditionalWrites (74-80), awaitWrites (83-108), repairPartition (112-116) | the same names in `BlockingReadRepair` |
| ReadOnlyReadRepair (34-73) | `ReadOnlyReadRepair` |
| NoopReadRepair | `NoopReadRepair` (identity only) |
| BlockingPartitionRepair: constructor (69-100), ack (112-117), sendInitialRepairs (140-158), awaitRepairsUntil (172-184), maybeSendAdditionalWrites (198-) | the same names in `BlockingPartitionRepair` |
| PartitionIteratorMergeListener (47-49); RowIteratorMergeListener: constructor (85-115), onCell (138-142), isQueried (144-154), applyToPartition (180-193), onMergedRows (207-215), close (367-400); BlockingReadRepairs.createRepairMutation (49-52) | `RowIteratorMergeListener` (the partition listener reduced to its construction; createRepairMutation inlined in `close`) |
| net/InboundMessageHandler (the verb's stage executes the handler), Verb.MUTATION_REQ/READ_REQ/READ_REPAIR_REQ stages (114, 118, 138) | `Node.dispatch`, `MutationTask` |
| MutationVerbHandler.doVerb (45-66), applyMutation (69-73); Mutation.apply / the memtable merge | `MutationVerbHandler.doVerb`, `Node.apply` |
| ReadCommandVerbHandler.doVerb (50-128); ReadCommand.executeLocally, createResponse; BTreeRow.filter (full tree db/rows/BTreeRow.java 344-371) | `ReadCommandVerbHandler.doVerb`, `ReadCommand.executeLocally`, `createResponse` |
| ReadRepairVerbHandler.doVerb / applyMutation (31-42) | `ReadRepairVerbHandler.doVerb` |
| ReadResponse.createDataResponse/createDigestResponse, digest (283-287), makeDigest (128) | `ReadResponse.*` |
| Cells.reconcile (68-77), resolveRegular (79-96) | `Cells.reconcile`, `resolveRegular` |
| ConsistencyLevel.quorumFor / blockFor (91-100, 133-148) | `QUORUM = 2` |
| net/RequestCallbacks, ResponseVerbHandler.doVerb (36-58), MessagingService.sendWithCallback | `Node.sendWithCallback`, `Node.awaitUntil`, `Node.dispatch` |

## Decisions and simplifications (the interface's replacements and what the source leaves open)

1. **One process per node** (the interface: one process each for `n0`, `n1`, `n2`), standing for the node's threads as
   described under Processes: the stage execution order of the source is the receive order of the node's messages, with
   mutations as stage tasks that execute at any later receive point; reads, repairs and response callbacks execute at
   receipt, local reads inline. The orders of the source the port does not have are listed there with the reason each
   changes no client-visible history; no `Nondet` is spent on scheduling (an exhaustive exploration of every interleaving
   of task execution against message receipt is outside any exploration budget: the study's earlier transliteration with
   one process per task did not finish size 2 in 48 hours).
2. **The read plan's order.** `ReplicaPlans.forRead` sorts the candidates by the snitch's proximity (`sortedByProximity`:
   `DynamicEndpointSnitch` by its latency scores, `SimpleSnitch` in ring order), which the source leaves to the snitch and
   the interface leaves nondeterministic; the port chooses one of the six orders of the three replicas
   (`Nondet.chooseIndex(6)`), the contacts are the first two, the data request goes to the first and the digest request to
   the second (`executeAsync` with `initialDataRequestCount` = 1). Plans without the local replica and plans with the
   digest read local are included. (The study's earlier prompts to the Rust and Java agents fixed the local replica first;
   port_R has that restriction.)
3. **No speculation, no timeouts** (the interface): `NeverSpeculatingReadExecutor`; `maybeTryAdditionalReplicas`,
   `maybeSendAdditionalReads`, `maybeSendAdditionalWrites` and the cheap-quorum upgrade are time-based and never fire; every
   wait is unbounded; no failures (every reply arrives).
4. **Fetched vs queried columns.** The command of `SELECT a` queries `a` but fetches both regular columns
   (`ColumnFilterFactory.fromColumns` builds the filter with `ColumnFilter.allRegularColumnsBuilder`, full tree
   cql3/selection/ColumnFilterFactory.java l.46-57). The replica's row therefore holds `b` with its timestamp and a skipped
   value (`BTreeRow.filter` l.344-371: the writes are UPDATEs without primary-key liveness, so a skippable cell is never
   dropped); the digest covers both cells; the merge reconciles both; the repair holds only the queried cells
   (`RowIteratorMergeListener.isQueried`). A `ReadA` thus repairs `a` only, as the interface requires, while a difference in
   `b`'s timestamp alone makes its digests mismatch, as in the source.
5. **The digest.** `ReadResponse.makeDigest` is an MD5 (`Digest.java`) of the serialized filtered partition; the port
   abstracts it by a text that is injective over the modelled rows (which cells are present, their values or skipped
   values, their timestamps): equal rows give equal digests and different rows different ones. MD5 collisions between
   different rows (the "logically identical data, different digests" case of `BlockingPartitionRepair` l.80-84 is its
   converse: equal data, different digests, from serialization differences) are not modelled.
6. **Data model.** A cell is `(value, timestamp)`, absent = timestamp 0; a write sets both cells to `(v, v)`; the memtable
   merges by `Cells.reconcile` (newer timestamp wins; absent loses; equal timestamps: the bigger value, which only separates a
   cell from its skipped copy); no row liveness, deletions, tombstones, TTL (the interface's writes are UPDATEs).
7. **Message ids and late responses.** Each request to each destination gets a fresh id (a `long`) and its callback is
   registered under it (`MessagingService.sendWithCallback`; the source keys by message id and peer). A response removes
   its registration and is delivered to the callback (`ResponseVerbHandler.doVerb` -> `RequestCallbacks.remove`);
   registrations of outstanding requests stay after their handler's wait returned (the source keeps them until expiry,
   which never comes), so the third acknowledgement of a write still reaches its `WriteResponseHandler` (responses -1, no
   effect). A response without a registration is dropped. The node receives late responses also while idle (the launcher
   lets the daemons run on after the clients are done until every node blocks on an empty mailbox).
8. **The merge is eager.** `DataResolver.resolve` merges the responses and runs the merge listener (whose `close` sends the
   repair mutations) when it is called, in `awaitReads`; the source merges lazily when the merged iterator is consumed and
   runs the listener's `close` when it is closed, inside `concatAndBlockOnRepair.close` before `awaitWrites`. Nothing
   observable happens between the two points (no sends), so the order of sends and waits is the source's.
9. **The repair's write plan and order.** `forReadRepair` = the two read contacts (they already reach `blockForWrite` = 2),
   so `buildFullDiff` is false and `writeBackTo` is all true; `sendInitialRepairs` iterates the source's `ConcurrentHashMap`
   (order open): the port sends in write-plan contact order. The two sends go to different nodes and both are awaited
   before the reply, so their order is not observable.
10. **One client, one request at a time** per coordinator (the interface): the coordinator side is sequential; the
    node serves the other nodes' requests meanwhile (decision 1).
11. **Not ported** (the interface replaces them): hints, forwarding, multiple data centres, transient replicas, range reads,
    short-read protection, replica filtering, repaired-data tracking, Paxos reads, tracing, metrics, warnings, monitoring
    time, expiry checks.
12. **Arguments.** `impl,<q>,<blocking|none>,<fifo|bag>`: `fifo` = `CommunicationModel.P2P`, `bag` = `CommunicationModel.ASYN`
    for every send and receive.

## Compile and runs

Compile, inside this directory:
```
javac -d java/out -cp "$(cat ../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```
Runs (2026-11-12, this machine, inside `tmp/`; the Must strategy, exhaustive; revision 2 after the round-1 reviews):
```
java -Xss4m -Xmx8g -cp ../java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr_port.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report-blocking - 900 </dev/null
  -> iterations: 8202, stop_reason: exhausted  (every execution ends with the nodes blocked on receive)
java ... 'bench.cassandra_rr_port.CassandraReadRepair:impl,1,none,fifo' must 0 1 report-none - 900 </dev/null
  -> iterations: 5368, stop_reason: exhausted
java ... 'bench.cassandra_rr_port.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report export-q1-blocking.jsonl 900 --split 2 --k 1 </dev/null
  -> the export of the size-1 summaries (see LOG.md of the task for the join results against port_R and S_rr)
```
