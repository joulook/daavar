# Cassandra 4.1 quorum read with read repair: Java reference port (JMC message-passing API)

Source: `java/bench/cassandra_rr/CassandraReadRepair.java` (one file). Transliterated from the 4.1 cut in
`plan/case-studies/cassandra/read-repair-src` (paths below are relative to `src/java/org/apache/cassandra`),
with lookups in the full tree `src-4.1` where the cut calls out of itself. Every changed line carries `// PORT:`.

## Roles (since fix round 2)

| Role | Process | What it runs |
|---|---|---|
| `n0`, `n1`, `n2` | `Node.run` | The replica's coordinator thread (native-transport request thread) and the replica's address. It runs StorageProxy for its own client, the read executor, inline local work, and the callbacks of its own requests. It never terminates: it waits for the next client request. |
| `n<r>-mutation` | `MutationStage.run` | The replica's MUTATION stage and its memtable. It runs MutationVerbHandler, ReadRepairVerbHandler and queued local mutations, and publishes every new memtable state (`Version`) to the replica's readers. It never terminates. |
| `n<r>-read-c<c>-<s>` | `ReadStageTask.run` | One READ stage task: the s-th read request that coordinator `n<c>` sends replica r (ReadCommandVerbHandler, or a queued local read). Single-use. Coordinator c can send r at most two requests per read of client c (the initial one and the repair's full data read), so there are 2·reads(c) such tasks. A task that is never used stays blocked on receive. |
| `c0`, `c1` | `client` | The INTERFACE.md loop: operation j has kind (i+j) mod 3: 0 is `Write(i, 1+i+2j)` and waits for `Ack`; 1 is `ReadA(i)` and waits for `ValueA`; 2 is `ReadAB(i)` and waits for `ValueAB`. |
| main | `run` | Spawns every replica's threads, replica by replica (coordinator, mutation stage, read tasks), then c0 and c1. It checks the task ids are consecutive, awaits the clients and then the rest. It sends and receives nothing. Every execution ends blocked, with replica threads waiting on receive. |

At q=1 a replica has 4 threads (c1 has one read), so there are 14 processes in all.

Addresses: process k in spawn order (`Layout`) has task id `base + k`. Each process computes `base` from its
own `MpProcess.self()` and its index, so no setup message is needed. Main checks the ids are consecutive.

Visible roles: `c0`, `c1`. Declared model (`model()`, the header only): `fifo` is `CommunicationModel.P2P`,
`bag` is `ASYN`. Every send and receive uses `STAGES = ASYN`, whatever the argument (F1, G2).
`programName()` is `cassandra-rr`. `size()` is `q=<q>,rr=<blocking|none>,<fifo|bag>`.

## Messages

Visible (INTERFACE.md; `@Tag` equals the record name. That is an assumption: INTERFACE.md does not give the
tags the exporter expects): `Write(int client, long value)`, `ReadA(int client)`, `ReadAB(int client)`,
`Ack(int client)`, `ValueA(int client, long a)`, `ValueAB(int client, long a, long b)`.

Internal (net/Verb.java). `id` is the callback correlation of net/RequestCallbacks. `from` is the requester's
address (its coordinator thread), the `Message.from()` of the header. `known` is the sender's causal
knowledge of memtable versions (G1).

| Record | Verb | Sent by, to | Handled by |
|---|---|---|---|
| `MutationReq(id, from, Mutation, known)` | MUTATION_REQ (or a queued/inline local mutation) | coordinator (sendToHintedReplicas, performLocally) to `n<r>-mutation` | MutationVerbHandler.doVerb: apply, publish, reply |
| `MutationRsp(id, from, known)` | MUTATION_RSP | mutation stage to the coordinator | WriteResponseHandler.onResponse |
| `ReadReq(id, from, ReadCommand, known)` | READ_REQ (or a queued local read) | coordinator (makeRequests, sendReadCommand) to a read task | ReadCommandVerbHandler.doVerb |
| `ReadRsp(id, from, ReadResponse, known)` | READ_RSP | read task to the coordinator | ReadCallback.onResponse, then the resolver's preprocess |
| `ReadRepairReq(id, from, Mutation, known)` | READ_REPAIR_REQ | BlockingPartitionRepair.sendRR to `n<r>-mutation` (own replica included) | ReadRepairVerbHandler.doVerb |
| `ReadRepairRsp(id, from, known)` | READ_REPAIR_RSP | mutation stage to the coordinator | BlockingPartitionRepair.onResponse |
| `Version(replica, index, slot, memtable, known)` | none: the publication of a memtable state (G1) | mutation stage to each read task (slot 0) and to its coordinator thread (one copy per inline-read slot) | a read observing that state |

Data: `Cell(column, value, timestamp)` (a skipped value is 0, `Cell.withSkippedValue`). `PartitionUpdate(List<Cell>)` holds the one row; an empty list means
no row. `Mutation(PartitionUpdate)`. `ReadCommand(queriedColumns, isDigestQuery)`.
`ReadResponse(isDigestResponse, data, digestValue)`.

## Protocol, step by step

Write (`Write{i,v}` to `ni`). StorageProxy.mutate calls performWrite. performWrite builds the write plan with
`ReplicaPlans.forWrite` and the `writeNormal` selector, which contacts all three replicas. It builds a
`WriteResponseHandler` with `responses = blockFor = writeQuorum = 2` and calls sendToHintedReplicas.
sendToHintedReplicas walks the contacts in order. The local mutation goes to performLocally (`Node.performLocally`).
Queued (a Nondet choice), it is a `MutationReq` to the replica's own mutation stage, counted when its
`MutationRsp` arrives. Inline, the coordinator waits for exactly that acknowledgement before going on. Each other replica
is sent a `MutationReq`. Next, `maybeTryAdditionalReplicas`
returns at once, because nothing is left uncontacted. `get()` then waits until two responses have arrived,
and the coordinator replies `Ack{i}`. The third `MutationRsp` arrives later, and its callback has no effect.
A replica applies a mutation cell by cell with `Cells.reconcile` (as the memtable merges cells): the newer
timestamp wins.

Read (`ReadA`, columns [a]; `ReadAB`, columns [a,b]). fetchRows runs these phases in order:
1. `getReadExecutor` calls `ReplicaPlans.forRead`. The candidate order is chosen: the first candidate by
   `Nondet.chooseIndex(3)` among all three replicas, the second by `Nondet.chooseIndex(2)` among the rest.
   The contacts are the first 2, so the local replica may or may not be read, and either contact may get the
   data read. The executor is a
   `NeverSpeculatingReadExecutor` (initialDataRequestCount = 1). `ReadRepair.create` builds a
   `BlockingReadRepair` for `blocking` and a `ReadOnlyReadRepair` for `none`.
2. `executeAsync`. The data read goes to contacts[0], the digest read to contacts[1]. A remote one is a
   `ReadReq`. A local one is `Node.localRead`: queued, a `ReadReq` to one of the replica's
   own read tasks; inline (a Nondet choice), the coordinator thread receives a `Version` itself.
   makeRequests runs the local read last.
   Each command fetches both regular columns. A non-queried one keeps its timestamp, and its value is
   skipped. Data and digest cover what is fetched.
3. `maybeTryAdditionalReplicas` does nothing.
4. `awaitResponses`. The ReadCallback (blockFor 2) is signalled once the data is present and 2 responses
   have arrived. If `DigestResolver.responsesMatch` holds, the result is the data response
   (`setResult(getData())`). Otherwise `readRepair.startRepair` runs. It creates a new DataResolver and
   ReadCallback (blockFor 2) and sends a full data read to each contact: `Node.localRead` for the local replica, a
   `ReadReq` otherwise.
5. `maybeSendAdditionalDataRequests` returns if the result is set. Otherwise it calls
   `maybeSendAdditionalReads`, which sends nothing (no speculation).
6. `awaitReadRepair` calls `awaitReads`. This waits for the 2 data responses, then runs
   `DataResolver.resolve()` and passes its output to `setResult`. resolve merges the two rows column by column
   with `Cells.reconcile`. Under `blocking` it feeds a `RowIteratorMergeListener`:
   - The write plan is `forReadRepair`, whose contacts are the two read sources.
   - For each source i and column, `onCell(i, merged, original)` adds `merged` to source i's repair when
     `merged != original` and the column was queried. A `ReadA` therefore never repairs `b`.
   - `close()` builds one Mutation per write-plan contact that has a repair and calls
     `BlockingReadRepair.repairPartition`.
   - repairPartition creates a `BlockingPartitionRepair`. Its latch is `writeQuorum (2)` minus the number of
     contacts without a mutation. `sendInitialRepairs` then sends a `ReadRepairReq` to each stale source,
     the coordinator itself included (a message to itself, as `MessagingService.sendWithCallback` does in
     Cassandra).

   Under `none`, `ReadOnlyReadRepair` returns the NOOP merge listener: the reconciled data and no repair.
7. `getResult` and `concatAndBlockOnRepair`, which runs the close of the returned iterator:
   `maybeSendAdditionalWrites` and then `awaitWrites`. For `blocking`, this waits until the latch reaches 0.
   One acknowledgement is needed per repair mutation sent. For `none`, both calls do nothing.
8. The reply carries the per-column value of the result, `ValueA{i,a}` or `ValueAB{i,a,b}`. An absent cell
   is 0.

While waiting in steps 4, 6 and 7 and in the write's `get()`, the coordinator thread receives only the
responses its live callbacks await, and runs each callback (`Node.awaitUntil`). The replica's mutation stage
and read tasks are separate processes, so they serve other coordinators meanwhile (fix round 2).

## Method map

| Cassandra (4.1) | Port |
|---|---|
| StorageProxy.mutate (StorageProxy.java l.873) | `StorageProxy.mutate` |
| StorageProxy.performWrite (l.1360) | `StorageProxy.performWrite` |
| StorageProxy.sendToHintedReplicas (l.1464) | `StorageProxy.sendToHintedReplicas` |
| StorageProxy.performLocally (l.1646) | `Node.performLocally` |
| Stage.maybeExecuteImmediately / SEPExecutor.maybeExecuteImmediately (F l.218) | `Node.inline` (the choice), `Node.performLocally`, `Node.localRead` |
| InboundMessageHandler.dispatch -> verb.stage.execute (F l.388-399) | the stage processes: `MutationStage.run`, `ReadStageTask.run`; responses in `Node.awaitUntil` |
| Memtable apply / AtomicBTreePartition | `MutationStage.apply` + `publish` |
| AbstractWriteResponseHandler.get / blockFor / maybeTryAdditionalReplicas / signal | `WriteResponseHandler.get / blockFor / maybeTryAdditionalReplicas / signal` |
| WriteResponseHandler.onResponse / ackCount | `WriteResponseHandler.onResponse / ackCount` |
| StorageProxy.readRegular (l.1967) | `StorageProxy.readRegular` |
| StorageProxy.fetchRows (l.2068) | `StorageProxy.fetchRows` |
| StorageProxy.concatAndBlockOnRepair (l.2029) | `StorageProxy.concatAndBlockOnRepair` |
| StorageProxy.SinglePartitionReadLifecycle | does not exist in this 4.1 tree (see decision D1) |
| StorageProxy.LocalReadRunnable | `Node.localRead` (queued: a read task; inline: the coordinator thread) |
| ReplicaPlans.forRead / contactForRead (l.706, l.660) | `ReplicaPlans.forRead` |
| ReplicaPlans.forWrite + writeNormal (l.439, l.505) | `ReplicaPlans.forWrite` |
| ReplicaPlans.forReadRepair + writeReadRepair (l.434, l.551) | `ReplicaPlans.forReadRepair` |
| ReplicaPlan.ForTokenRead / ForWrite (readQuorum, writeQuorum, lookup, withContacts, liveUncontacted) | `ReplicaPlan.ForTokenRead / ForWrite` |
| AbstractReadExecutor.getReadExecutor (l.190) | `AbstractReadExecutor.getReadExecutor` |
| AbstractReadExecutor.makeFullDataRequests / makeDigestRequests / makeRequests (l.112-170) | same names |
| AbstractReadExecutor.executeAsync (l.178) | `executeAsync` |
| AbstractReadExecutor.awaitResponses (l.396-440) | `awaitResponses` |
| AbstractReadExecutor.awaitReadRepair (l.442) | `awaitReadRepair` |
| AbstractReadExecutor.maybeSendAdditionalDataRequests (l.465), isDone, setResult | same names |
| AbstractReadExecutor.getResult (l.473) | `getResult` |
| NeverSpeculatingReadExecutor (l.258), maybeTryAdditionalReplicas | `NeverSpeculatingReadExecutor` |
| ReadCallback.awaitResults (l.126), onResponse (l.179), response | `ReadCallback.awaitResults / onResponse / response` |
| ResponseResolver.preprocess | `ResponseResolver.preprocess` |
| DigestResolver.preprocess / getData (l.77) / responsesMatch (l.105) / isDataPresent | same names |
| DataResolver.resolve (l.94) / resolveWithReadRepair / resolveInternal / isDataPresent | `DataResolver.resolve` (with resolveWithReadRepair's listener choice) / `resolveInternal` |
| Cells.reconcile / resolveRegular (Cells.java l.68, l.79) | `Cells.reconcile / resolveRegular` |
| Rows.diff (Rows.java l.127) | `RowIteratorMergeListener.onMergedRows` (the per-column, per-source diff loop) |
| ReadRepair (interface) | `ReadRepair` |
| ReadRepairStrategy.NONE / BLOCKING / fromString | `ReadRepairStrategy` |
| AbstractReadRepair.startRepair (l.126) / awaitReads (l.156) / maybeSendAdditionalReads (l.185) / sendReadCommand (l.93) | same names |
| BlockingReadRepair.repairPartition (l.112) / awaitWrites (l.83) / maybeSendAdditionalWrites (l.74) / getMergeListener | same names |
| BlockingPartitionRepair (ctor latch l.97, ack l.112, onResponse, sendRR, sendInitialRepairs l.145, awaitRepairsUntil l.172, maybeSendAdditionalWrites l.198) | same names |
| ReadOnlyReadRepair (the NONE strategy) | `ReadOnlyReadRepair` |
| NoopReadRepair | `NoopReadRepair` (ported; never selected, see D2) |
| PartitionIteratorMergeListener.getRowMergeListener | `BlockingReadRepair.getMergeListener` (constructs the row listener directly) |
| RowIteratorMergeListener (ctor l.85, onCell/isQueried l.138-155, applyToPartition, onMergedRows l.207, close l.367) | `RowIteratorMergeListener` |
| BlockingReadRepairs.createRepairMutation | `BlockingReadRepairs.createRepairMutation` |
| MutationVerbHandler.doVerb / applyMutation / respond | `MutationVerbHandler.doVerb / applyMutation / respond` |
| ReadCommandVerbHandler.doVerb (l.50) | `ReadCommandVerbHandler.doVerb` |
| ReadRepairVerbHandler.doVerb (l.31) / applyMutation | `ReadRepairVerbHandler.doVerb / applyMutation` |
| ResponseVerbHandler.doVerb + RequestCallbacks | `ResponseVerbHandler.doVerb`, `Node.callbacks`, `Node.sendWithCallback` |
| ReadCommand.executeLocally + createResponse; ReadResponse digest/data | `Node.executeLocally`, `ReadResponse.createDataResponse / createDigestResponse / digest` |

## Decisions and simplifications (with the source lines that left them open)

D1. **SinglePartitionReadLifecycle.** The README and the prompt name an inner class
SinglePartitionReadLifecycle (doInitialQueries, awaitResultsAndRetryOnDigestMismatch, ...). This 4.1 tree
has no such class: a grep of both the cut and src-4.1 finds no match. In 4.1, StorageProxy.fetchRows
(l.2068-2134) runs the phases as loops over the executors: getReadExecutor, executeAsync,
maybeTryAdditionalReplicas, awaitResponses, maybeSendAdditionalDataRequests, awaitReadRepair, getResult and
getReadRepair, then concatAndBlockOnRepair. The port follows fetchRows. No class was invented.

D2. **What `none` is.** ReadRepairStrategy.NONE (ReadRepairStrategy.java l.28-34) creates a
**ReadOnlyReadRepair**, not a NoopReadRepair. So under `none`, a digest mismatch still triggers the full data
reads (AbstractReadRepair.startRepair) and the reconciliation. Its merge listener is NOOP and it never
repairs. This is what the port does, and the README describes it the same way (ReadOnlyReadRepair is "the
NONE strategy, as ReadRepairStrategy.java maps it"). The round-0 note here claimed the README calls
NoopReadRepair the NONE strategy. That claim is stale and is withdrawn. NoopReadRepair is ported as written and is never selected (in 4.1 only DigestResolver.getData
with transient replicas uses it).

D3. **Which replicas are read; data versus digest.** ReplicaPlans.forRead (l.706) takes the candidates
`sortedByProximity` from the snitch (ReplicaLayout.forTokenReadLiveSorted, ReplicaLayout.java l.331) and
contacts the first blockFor = 2 (contactForRead l.660-678). Since fix round 1, the snitch order is a free
nondeterministic order (INTERFACE.md: "the coordinator's fastest replicas are a nondeterministic choice").
The first two candidates are chosen among all three replicas, with no local-first restriction. The data read
goes to contacts[0] and the digest read to contacts[1] (executeAsync l.178, initialDataRequestCount = 1).
The original prompt's wording, "the local replica and one other", described round 0 and is not followed any
more; see F2. SimpleSnitch
(SimpleSnitch.java l.41) keeps the natural order, and DynamicEndpointSnitch could rank another replica first,
so the snitch is what leaves this open.

D4. **Executor.** getReadExecutor (l.190-212) picks by speculative-retry policy. With no clock there is no
speculation, so it is always NeverSpeculatingReadExecutor (l.258). Its maybeTryAdditionalReplicas only waits
and records a metric (shouldSpeculateAndMaybeWait, l.226), so it does nothing here.
AbstractReadRepair.maybeSendAdditionalReads (l.185-204) sends a speculative read only after a timed wait
expires, so it never sends.

D5. **Local execution.** Superseded by F3 (fix round 1). Local work is queued. The inline branch is
ported behind `INLINE_LOCAL_EXECUTION` (false). Repair mutations to the coordinator itself are
`ReadRepairReq` messages, as BlockingPartitionRepair.sendRR does.

D6. **Concurrency in one process.** Superseded by F1 (fix round 1).

D7. **Timeouts.** Every timed wait returns only once its condition holds, so no timeout path is taken (no
`ReadTimeoutException`, `WriteTimeoutException`, hints, or `onFailure`). In particular,
BlockingPartitionRepair.maybeSendAdditionalWrites (l.198) begins with `awaitRepairsUntil(...)`, which here
waits until the latch is 0 and returns true. So the speculative repair writes to uncontacted replicas are
never sent, and the wait for the repair acknowledgements happens in that call. The `awaitWrites` that follows
returns at once. The visible behaviour is the source's "wait for the acknowledgements, then reply".
Callbacks never expire (RequestCallbacks), and late responses are delivered to their callbacks with no effect.

D8. **Lazy iterators.** The resolved PartitionIterator is lazy. The merge listener produces the repair
mutations while the partition is consumed, and `concatAndBlockOnRepair`'s close() then waits for the repairs.
The CQL layer consumes and closes the iterator before it answers the client. The port evaluates eagerly
instead: `DataResolver.resolve` merges and closes the listener (repairPartition sends the repairs) inside
`awaitReads`, and the close() body runs in `concatAndBlockOnRepair` before the reply. The order of message
events is the same.

D9. **What is fetched and digested.** Superseded by F4 (fix round 1). Both columns are fetched, a
non-queried cell has its value skipped, and only queried columns are repaired.

D10. **Data model.** The interface replaces the partition: one row, simple live regular cells `a` and `b`. A
write is `UPDATE … SET a = v, b = v USING TIMESTAMP v`: two cells with timestamp v and no row marker (an
UPDATE writes no primary-key liveness). So Rows.diff's liveness and deletion callbacks never fire, and only
`onCell` is ported. An absent cell is null in the merge and 0 in the reply. A replica that has no cell
returns an empty partition (no row). Cells.reconcile keeps the timestamp rule and the value tie-break. There
are no tombstones, TTL or counters.

D11. **Order inside maps.** RowIteratorMergeListener.close builds a HashMap, and BlockingPartitionRepair
copies it into a ConcurrentHashMap (l.71), so the source fixes no order for the repair sends. The port uses
the write-plan contact order (n0, n1, n2). Sends never block, so this only orders sends on distinct channels.

D12. **DataResolver.resolve's replica list.** `readCandidates().select(froms, false)` (DataResolver.java
l.105) is taken in response order. The order only indexes the per-source repair builders. The
writeBackTo/buildFullDiff logic is ported, but buildFullDiff is always false here because the read-repair
write plan's contacts are the two sources (writeReadRepair l.562-580 adds none).

D13. **Write plan and handler.** writeNormal (l.505) contacts all live replicas (no transient replicas).
maybeTryAdditionalReplicas (AbstractWriteResponseHandler l.334) returns at once because nothing is
uncontacted. The local mutation goes to performLocally (inline or queued, G5) before the remote MutationReqs
are sent. Ideal-CL delegates,
metrics, tracing and hints are dropped.

D14. **Addresses.** The prompt says every process knows every address from the start, and main must not send.
So each process computes the task ids from its own id and the spawn order. Main checks that the ids are
consecutive before waiting.

D15. **Observation.** `toString()` (printed by the launcher as `observed:`) reports how many client runs
finished, the distinct reply sequences per client, the number of distinct reply-string pairs (renamed in
fix round 2: they are reply strings only, with no causal order, and are not joint histories), and (for q=2)
whether the REVIEW.md finding-1 history was seen. These are collected outside the message-passing API, for
the reader only. They are not part of the protocol and raise no event.

## Compile (inside OUT_DIR)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

## Fix round 1 (review: plan/case-studies/cassandra/rr/translit-review/REVIEW.md)

Kept as a record. Fix round 2 supersedes F1's single replica process and F3's self-messages and disabled
inline branch (see G1-G4). F2, F4 and F6 still hold. F5's history is rechecked under round 2.

The review judged round 0 NOT FAITHFUL. Round 0 ran each received verb to completion at receipt, read the
local replica first, executed local work only inline, and digested the queried cells only. Changes:

F1. **Receipt separated from stage execution (finding 1).** Cassandra hands every received verb to its
stage (InboundMessageHandler.dispatch -> header.verb.stage.execute, F/net/InboundMessageHandler.java
l.388-399; MUTATION_REQ and READ_REPAIR_REQ on MUTATION, READ_REQ on READ, the *_RSP on REQUEST_RESPONSE,
S/net/Verb.java l.113-138). Those stages are multi-threaded pools (F/concurrent/Stage.java l.45-50). So the
tasks of one replica run in any order, relative to each other and to the coordinator thread, whatever the
transport's receipt order. In round 1:

- **Explicit queue, tried first.** A replica's step was a non-blocking receive that only queued the task,
  or ⊥ followed by a Nondet choice of a pending task. q=1 blocking fifo did not finish in 600 s
  (101 786 graphs, stack not exhausted), because ⊥ and the task order repeat the same interleavings.
- **Equivalent cheap form, adopted.** If a task can be received early and executed at any later point, its
  execution order is any order consistent with its send. That is exactly unordered delivery. So the verbs
  between replicas are sent and received under `STAGES = ASYN`, and a received verb runs at once
  (`Node.step`). The delay between receipt and execution is the delivery delay.
- **Client channels.** These also use `STAGES`. A client has at most one message in flight per direction,
  so P2P and ASYN coincide there. A JMC receive reads only sends of its own model (tested: a P2P client
  send was never read by an ASYN replica receive, and the run blocked at once). The replica's single
  receive has to read both kinds, so the client channels cannot keep P2P.
- **Consequence.** The `fifo`/`bag` argument now changes only the declared `model()`. It cannot change a
  Cassandra history of this study, because stage concurrency removes every transport-order constraint on
  execution. The runs below use `fifo` as prescribed. `bag` gives the same exploration.
- **Late write responses are not received.** The guard in `Node.step` leaves a `MutationRsp` unread once its
  handler has signalled. Only the third write acknowledgement is affected, and its task has no effect
  (WriteResponseHandler.onResponse only decrements). Leaving it unread removes executions that differ only
  in where it was read.

The coordinator thread's waits (`awaitUntil`) keep stepping, as before.

F2. **Replica order (finding 2).** ReplicaPlans.forRead chooses the first candidate among all three
replicas and the second among the remaining two (`Nondet`). The contacts are the first two, so the local
replica need not be read. The data read goes to contacts[0], the digest read to contacts[1].
- This departs from the original prompt's "(the local replica and one other ...)". The review and the
  coordinator required it, and INTERFACE.md only says the fastest replicas are a nondeterministic choice.
- If the prompt's local-first reading is wanted after all, the old order is one line (round 0, D3).

F3. **Local execution (finding 2).** `Stage.maybeExecuteImmediately` (SEPExecutor.maybeExecuteImmediately,
F/concurrent/SEPExecutor.java l.218-239) is `Node.maybeExecuteImmediately`:
- **Queued branch (taken).** A queued local task is a message of the replica to itself
  (`LocalMutationRunnable`, `LocalReadRunnable`). It runs when received, at any later step (F1). Its
  callback is registered under an id.
- **Inline branch (disabled).** It is ported, but behind `INLINE_LOCAL_EXECUTION = false`. The queued branch
  already yields every history the inline branch does. Between the inline point and the replica's next
  step, the coordinator only sends messages: in makeRequests the local read is last, and in
  sendToHintedReplicas the remote sends follow. A queued task may be received at that very next step.
  Enabling the inline branch adds a Nondet choice whose inline side only duplicates histories (q=1 runs
  doubled in round-1 trials).
- This is a reduction I chose. Flip the constant to explore both branches.

F4. **Fetched metadata in digests (finding 3).** A command fetches every regular column
(ColumnFilter.allRegularColumnsBuilder, F/cql3/selection/ColumnFilterFactory.java l.46-57) and queries the
selected ones. A fetched, non-queried cell is returned with its timestamp and a skipped value
(BTreeRow.filter, F/db/rows/BTreeRow.java l.344-371).
- An UPDATE writes no row liveness, so such a cell is never dropped. PORT: the skipped value is 0
  (`Cell.withSkippedValue`).
- Data and digest responses cover the fetched cells (ReadResponse.makeDigest over the filtered iterator).
  So a `ReadA` whose replicas agree on `a` but differ in `b`'s timestamp sees a digest mismatch and does
  the full data round, as in Cassandra.
- Repairs still contain only queried cells (RowIteratorMergeListener.isQueried). Replies use only the
  queried columns.

F5. **Reference check (finding 1's history).** The history, with q=2, none, fifo:
- c0: `Write(0,1)` -> `Ack`, then `ReadA` -> 1.
- c1: `ReadA` -> 1, then `ReadAB` -> (0,0).

`toString()` reports `review history (REVIEW.md finding 1) PRESENT|ABSENT` for q=2.

The unrestricted q=2 exploration does not reach it within its bound. The bounded must runs below are DFS
corners. 1.2 M `mp-random` samples (4 seeds × 300 000) did not hit it either; mp-random appears not to vary
the Nondet replica choice. So reachability is shown with a test-only restriction:
- **The restriction.** The fifth argument `review1` removes choices only, so every execution it allows is
  an execution of the unrestricted program:
  - c1's reads use the fixed plans [n1, n0] and [n1, n2];
  - n1 receives MutationReq only after its client's two operations;
  - n2 receives MutationReq only after serving a read;
  - n0 serves reads only after applying its queued local mutation.
- **Command:** `...MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,2,none,fifo,review1' must 0 1 report - 1200 </dev/null`
- **Result:** 1600 graphs, stack exhausted; every run finished (3200 client runs). `observed: ... c1:ValueA[client=1, a=1] ValueAB[client=1, a=0, b=0] ...; review history (REVIEW.md finding 1) PRESENT`.
- **Hand trace of one such execution:**
  1. n0 queues its local mutation and sends MutationReq to n1 and n2.
  2. n1 queues a local data read and sends a digest ReadReq to n0.
  3. n0 applies its local mutation, then answers the digest ("a:1@1;b:0@1;").
  4. n1's local data is empty, so the digests mismatch. ReadOnlyReadRepair re-reads n1 (empty) and n0
     (a=1), and the reply is `ValueA(1,1)`.
  5. n1 reads [n1, n2]. Both are empty and match, so the reply is `ValueAB(1,0,0)`.
  6. The mutations are then applied, `Ack(0)` follows, and `ValueA(0,1)`.

  Round 0 could not produce this history (REVIEW.md finding 1).

F6. **Stale remark corrected.** D2 no longer claims that the README calls NoopReadRepair the NONE strategy.
The current README names ReadOnlyReadRepair.

## Runs of the round 1 binary (superseded; kept as a record)

Compile as above (no warnings, no errors).

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. `ran: 8376 iterations`. `graphs: 0 distinct complete, 8376 distinct blocked; stack exhausted`.
`observed: finished client runs 16752; distinct replies per client [c0:Ack[client=0], c1:ValueA[client=1, a=0], c1:ValueA[client=1, a=1]]; distinct reply-string pairs 2`.
Last output line: `max_porf_count: 0`.

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,none,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. `ran: 4608 iterations`. `graphs: 0 distinct complete, 4608 distinct blocked; stack exhausted`.
`observed: finished client runs 9216; ... distinct reply-string pairs 2`.
Last output line: `max_porf_count: 0`.

Every graph is blocked because the replicas never terminate. Every client run finished. For comparison,
round 0 gave 31 (blocking) and 25 (none) graphs.

q=2 checks (same commands with `impl,2,<s>,fifo`, timeout 1200 s; not of record):
- `none`: 134 929 graphs, stack not exhausted (timeout). Every client run finished.
- `blocking`: 113 095 graphs, stack not exhausted (timeout). Every client run finished.

In both, the DFS stayed in one corner within the bound: one distinct reply-string pair, and the review history
was ABSENT there. Its reachability is established by the `review1` run (F5), not by these. A complete q=2
enumeration needs a much larger budget or the split mode. Round 0's q=2 runs exhausted at 2282 and 1635
graphs, over its much smaller behaviour set.


## Fix round 2 (review: plan/case-studies/cassandra/rr/translit-review/REVIEW-r2.md)

The review accepted the round-1 value-history fix. It found that the exported observation includes causal
order (SummaryExporter: per visible event, the count of the other client's visible events porf-before it,
through invisible events), and that round 1 put every task of a replica on one thread. Round 1 therefore
had program-order edges that Cassandra's separate stages do not impose. Its witness, a Cassandra history
that round 1 cannot produce, is (q=1, none):

- c0: `Write(0,1)` -> `Ack(0)`
- c1: `ReadA(1)` -> `ValueA(1,0)`
- no causal edge from c1's `ReadA` send to c0's `Ack` receive.

Causal model assumed (as the review states it, and stated here so it can be checked). The source's causal
order is:
- program order of each Cassandra thread;
- message send -> receive;
- the memory happens-before the source's synchronisation creates:
  - the stage executor's submit -> task;
  - a callback's accumulate or count -> the waiting coordinator's continuation;
  - a memtable write -> a read that observes it (the volatile read of AtomicBTreePartition.ref);
  - a memtable write -> the next write to the partition (its CAS reads the tree the previous write
    installed).

A read does **not** order a later write: there is no read -> write edge.

G1. **One process per Cassandra thread.** A replica is now three kinds of processes (Roles): the coordinator
thread, the MUTATION stage, and one READ stage task per read request.
- **Memtable state flows only from write to read.** The memtable lives in the MUTATION stage process. After
  each write it publishes the new state as a `Version` message to every reader of that replica:
  - each READ stage task gets one copy;
  - the coordinator thread gets one copy per possible inline read.
- **What a read observes.** A reader observes a state by receiving one `Version`. So it is causally after
  the write that produced that state, and through the stage's program order after all earlier writes, and
  after nothing else. It sends nothing to the mutation stage, so there is no read -> write edge.
- **Which states a read may observe.** It must observe a state no older than the newest write in its causal
  past. This is the guard `index >= known[r]`, where `known` is a per-replica vector of the highest version
  in the sender's causal past, carried on every internal message and merged at every receipt. In the source
  such a write completed earlier in real time, so the read sees it. The read may also observe any newer
  state already published, which is a concurrent write.
- **Every such choice is a source behaviour.** The causal order is a partial order with no edge from the
  read to the writes after the observed one (except through the read's own later messages). So a real-time
  schedule exists that puts the read after the observed write and before the next one.
- **The witness.** Its zero read takes version 0 at two replicas whose mutation stage applies the write
  afterwards, with no edge back.

G2. **ASYN delivery, argued for the causal observation.** ASYN and P2P differ only in which receive orders
are admissible, not in the edges a receive creates (send -> receive, plus the receiver's program order). So
the question is only whether some admissible order is missing or extra.
- **Mutation stage.** It stands for a multi-threaded pool (F/concurrent/Stage.java l.45-50), whose tasks run
  in any order even when FIFO delivered them in order (F1). So ASYN is the source's admissible set. P2P would
  forbid orders the source has.
- **Read tasks.** Each receives one request, then one `Version` chosen by its guard. Order is moot.
- **Coordinator thread.** It receives only the responses its live callbacks await, and they come from
  different stage tasks (REQUEST_RESPONSE is multi-threaded too). It receives the one in-flight client
  request.
- **Clients.** They have at most one message in flight per direction, so ASYN and P2P coincide (REVIEW-r2
  finding 2 accepts this part).

G3. **Remaining program-order edges, argued for the exported counts.** A count changes only if some visible
event enters or leaves a visible event's causal past. An added edge whose source has no visible event in its
past beyond those the target already has changes nothing.
- **Responses on the coordinator thread.** REQUEST_RESPONSE tasks run on the coordinator thread at receipt
  (`Node.awaitUntil`). In the source each awaited response's task is ordered before the coordinator's
  continuation by the signal it contributes to: the accumulator or counter the coordinator reads, and the
  Condition or latch. So receiving it on the coordinator thread adds no edge into the coordinator's future
  that the source lacks. Two awaited responses on one thread may be ordered either way (ASYN). The edge
  between them adds no visible predecessor beyond each sender's past, and both pasts reach the continuation
  anyway.
- **One mutation-stage process.** Between consecutive mutation tasks i then j, the model adds task i's
  events before task j's receipt. In the source, task j's apply is after task i's apply (CAS). Task j's
  receipt has no successors other than task j's later events, which follow its apply. Task i's events after
  its apply are sends whose own pasts lie within task i's prefix. So the model adds no visible predecessor
  to anything after task j's apply that the source does not already have. The other order (j before i) is
  explored too.
- **Inline local mutation.** The inline write is a synchronous request to the replica's own mutation stage.
  The coordinator waits for exactly that acknowledgement. This gives coordinator-past -> write -> later
  writes, and earlier writes -> write -> the coordinator's continuation. Those are the source's edges for a
  CAS performed on the coordinator thread.

G4. **The unreceived third write acknowledgement, argued for the exported counts.** In the source the third
`MUTATION_RSP` runs a REQUEST_RESPONSE task that only decrements `responses`, which nothing reads after the
signal. So that task has no successor in the causal order, and its receive event is invisible. Receiving it
on the coordinator thread would add a spurious edge (that response's sender -> the coordinator's later
events), so leaving it unread is the faithful choice.
- **Delivery.** It is an omitted completion with no successors, not a claim that the modelled execution
  delivers every message. The source's delivery of it adds an event with no outgoing edges and no effect on
  any summary (words, statuses or counts).
- **Unread `Version` copies.** These are modelling devices. Each stands for a read that did not happen, not
  for a source message.

G5. **Local execution: both branches.** `INLINE_LOCAL_EXECUTION = true`, so `Node.inline` is a Nondet choice
at every local mutation and local read.
- **Inline.** The work runs on the coordinator thread: a read receives a `Version` there, and a mutation
  waits for its acknowledgement. Inline work orders the coordinator's later sends after it.
- **Queued.** The work is a mutation-stage task or a read task.
- **Why both are needed.** Round 1's argument that queued subsumes inline covered values only. The two
  branches give different causal orders, and both are source behaviours (SEPExecutor.maybeExecuteImmediately
  l.218-239), so both are explored.

G6. **Evidence, and what each piece shows.**
- **Exhaustive q=1 runs (below).** Both exhaust the stack.
- **Exported summaries.** These are the causal observation: words plus counts. They are written to
  `checks/q1-none.jsonl` and `checks/q1-blocking.jsonl` with the same command and an export file in place
  of `-`.
  - **none:** 2 distinct summaries.
    - `ValueA(1,0)` with counts `[0,0,0,0]`: no c1 event is before c0's `Ack` and no c0 event is before
      c1's reply. This is Astra's witness: reached in the exhaustive run, unrestricted.
    - `ValueA(1,1)` with counts `[0,0,0,1]`.
  - **blocking:** 3 distinct summaries. The two above, plus `ValueA(1,1)` with counts `[0,1,0,1]`: c0's
    `Ack` after c1's `ReadA`, which needs a read-repair write applied at a replica before its write
    acknowledgement.
- **`review1`: a reachability aid only.** It removes choices: c1's read plans are fixed, all local work is
  queued, and the read tasks for c1's reads observe the oldest admissible state at n1 and n2 and a state
  with a write at n0. Every execution it allows is an execution of the unrestricted program. It shows only
  that the restricted search reaches the round-1 history (q=2, none): exhausted at 2112 graphs, `review reply
  strings (REVIEW.md finding 1) PRESENT`, summary in `checks/q2-none-review1.jsonl`. It is not evidence of
  equivalence.
- **The `observed:` monitor.** It checks reply strings only (`distinct reply-string pairs`). It records no
  causal counts. The exported summaries carry those.
- **The bounded q=2 runs.** These are incomplete evidence: neither a counterexample nor a validation (below).

## Runs (inside OUT_DIR, round 2 binary)

Compile as above (no warnings, no errors).

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. `ran: 2258 iterations`. `graphs: 0 distinct complete, 2258 distinct blocked; stack exhausted`.
`observed: finished client runs 4516; distinct replies per client [c0:Ack[client=0], c1:ValueA[client=1, a=0], c1:ValueA[client=1, a=1]]; distinct reply-string pairs 2`.
Last output line: `max_porf_count: 0`.

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,none,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. `ran: 1606 iterations`. `graphs: 0 distinct complete, 1606 distinct blocked; stack exhausted`.
`observed: finished client runs 3212; ... distinct reply-string pairs 2`.
Last output line: `max_porf_count: 0`.

Every graph is blocked because replica threads never terminate. Every client run finished.

The graph counts fell from round 1 (8376 and 4608). Independent threads need fewer interleavings in Must's
graph enumeration than one thread per replica, whose receive orders all differ.

Causal check (same command, export file instead of `-`):
- `... 'bench.cassandra_rr.CassandraReadRepair:impl,1,none,fifo' must 0 1 report checks/q1-none.jsonl 600 </dev/null`:
  same 1606 graphs, `summaries_distinct: 2`, Astra's witness present (G6).
- `... 'bench.cassandra_rr.CassandraReadRepair:impl,2,none,fifo,review1' must 0 1 report checks/q2-none-review1.jsonl 1200 </dev/null`:
  2112 graphs, exhausted, round-1 reply strings PRESENT.

q=2 bounded runs: incomplete evidence, neither a counterexample nor a validation. Same command with
`impl,2,<s>,fifo`, export to `checks/q2-<s>.jsonl`, 1200 s.
- **none:** 45 547 graphs, stack not exhausted (timeout). Every client run finished. 3 distinct summaries, 3
  reply-string pairs. The round-1 reply strings were not met within the bound; `review1` reaches them.
- **blocking:** 32 622 graphs, stack not exhausted (timeout). Every client run finished. 5 distinct
  summaries, 2 reply-string pairs.

All round-2 runs were repeated after the machine pause, on the final binary, with the same numbers.
