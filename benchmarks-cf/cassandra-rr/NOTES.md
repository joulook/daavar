# Cassandra 4.1 quorum read with read repair: Java reference port (JMC message-passing API)

Source: `java/bench/cassandra_rr/CassandraReadRepair.java` (one file). Transliterated from the 4.1 cut in
`plan/case-studies/cassandra/read-repair-src` (paths below are relative to `src/java/org/apache/cassandra`),
with lookups in the full tree `src-4.1` where the cut calls out of itself. Every changed line carries `// PORT:`.

## Roles

| Role | Process | What it runs |
|---|---|---|
| `n0`, `n1`, `n2` | `Node.run` | Replica side (MutationVerbHandler, ReadCommandVerbHandler, ReadRepairVerbHandler, ResponseVerbHandler) and the coordinator for its own client (StorageProxy.mutate, StorageProxy.readRegular). It never terminates: it loops on `Channel.recv`. |
| `c0`, `c1` | `client` | The INTERFACE.md loop: operation j has kind (i+j) mod 3: 0 is `Write(i, 1+i+2j)` and waits for `Ack`; 1 is `ReadA(i)` and waits for `ValueA`; 2 is `ReadAB(i)` and waits for `ValueAB`. |
| main | `run` | Spawns n0, n1, n2, then c0, c1 (named roles), checks the task ids are consecutive, awaits the clients and then the replicas. It sends and receives nothing. Every execution ends blocked, with the replicas waiting on receive. |

Addresses: process k (in spawn order) has task id `base + k`. Each process computes `base` from its own
`MpProcess.self()` and its index, so no setup message is needed. Main checks the ids are consecutive before
it waits on anyone.

Visible roles: `c0`, `c1`. Model: `fifo` is `CommunicationModel.P2P`, `bag` is `ASYN`. `programName()` is
`cassandra-rr`. `size()` is `q=<q>,rr=<blocking|none>,<fifo|bag>`.

## Messages

Visible (INTERFACE.md; `@Tag` equals the record name. That is an assumption: INTERFACE.md does not give the tags the exporter expects): `Write(int client, long value)`, `ReadA(int client)`,
`ReadAB(int client)`, `Ack(int client)`, `ValueA(int client, long a)`, `ValueAB(int client, long a, long b)`.

Internal (net/Verb.java). `id` is the callback correlation of net/RequestCallbacks. `from` is the sender, the
`Message.from()` of the header.

| Record | Verb | Sent by | Handled by |
|---|---|---|---|
| `MutationReq(id, from, Mutation)` | MUTATION_REQ | coordinator, sendToHintedReplicas, to each non-local replica | MutationVerbHandler.doVerb: apply, reply |
| `MutationRsp(id, from)` | MUTATION_RSP | replica | WriteResponseHandler.onResponse |
| `ReadReq(id, from, ReadCommand)` | READ_REQ (data or digest, by `isDigestQuery`) | AbstractReadExecutor.makeRequests (initial data and digest reads); AbstractReadRepair.sendReadCommand (full data reads after a mismatch) | ReadCommandVerbHandler.doVerb: data or digest response |
| `ReadRsp(id, from, ReadResponse)` | READ_RSP | replica | ReadCallback.onResponse, then DigestResolver or DataResolver.preprocess |
| `ReadRepairReq(id, from, Mutation)` | READ_REPAIR_REQ | BlockingPartitionRepair.sendRR, to each stale source (the coordinator itself included) | ReadRepairVerbHandler.doVerb: apply, reply |
| `ReadRepairRsp(id, from)` | READ_REPAIR_RSP | replica | BlockingPartitionRepair.onResponse, then ack |

Data: `Cell(column, value, timestamp)`. `PartitionUpdate(List<Cell>)` holds the one row; an empty list means
no row. `Mutation(PartitionUpdate)`. `ReadCommand(queriedColumns, isDigestQuery)`.
`ReadResponse(isDigestResponse, data, digestValue)`.

## Protocol, step by step

Write (`Write{i,v}` to `ni`). StorageProxy.mutate calls performWrite. performWrite builds the write plan with
`ReplicaPlans.forWrite` and the `writeNormal` selector, which contacts all three replicas. It builds a
`WriteResponseHandler` with `responses = blockFor = writeQuorum = 2` and calls sendToHintedReplicas.
sendToHintedReplicas walks the contacts in order. The local replica is applied directly (performLocally) and
counts as one response. Each other replica is sent a `MutationReq`. Next, `maybeTryAdditionalReplicas`
returns at once, because nothing is left uncontacted. `get()` then waits until two responses have arrived,
and the coordinator replies `Ack{i}`. The third `MutationRsp` arrives later, and its callback has no effect.
A replica applies a mutation cell by cell with `Cells.reconcile` (as the memtable merges cells): the newer
timestamp wins.

Read (`ReadA`, columns [a]; `ReadAB`, columns [a,b]). fetchRows runs these phases in order:
1. `getReadExecutor` calls `ReplicaPlans.forRead`. The candidates are [local, F, other], where F is chosen by
   `Nondet.chooseIndex(2)` among the two other replicas. The contacts are the first 2. The executor is a
   `NeverSpeculatingReadExecutor` (initialDataRequestCount = 1). `ReadRepair.create` builds a
   `BlockingReadRepair` for `blocking` and a `ReadOnlyReadRepair` for `none`.
2. `executeAsync`. The data read goes to contacts[0], the local replica. The local read is delayed to the end
   of makeRequests, but it is the only request in that call, so it runs immediately. The digest read
   (`ReadReq` with isDigestQuery) then goes to F.
3. `maybeTryAdditionalReplicas` does nothing.
4. `awaitResponses`. The ReadCallback (blockFor 2) is signalled once the data is present and 2 responses
   have arrived. If `DigestResolver.responsesMatch` holds, the result is the data response
   (`setResult(getData())`). Otherwise `readRepair.startRepair` runs. It creates a new DataResolver and
   ReadCallback (blockFor 2) and sends a full data read to each contact: locally for the local replica, by
   `ReadReq` to F.
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

While waiting in steps 4, 6 and 7 and in the write's `get()`, the coordinator keeps receiving messages of any
kind and handles them as the inbound path would (`Node.awaitUntil`). So a replica blocked as a coordinator
still serves other coordinators' mutations, reads and repairs, including its own repair message.

## Method map

| Cassandra (4.1) | Port |
|---|---|
| StorageProxy.mutate (StorageProxy.java l.873) | `StorageProxy.mutate` |
| StorageProxy.performWrite (l.1360) | `StorageProxy.performWrite` |
| StorageProxy.sendToHintedReplicas (l.1464) | `StorageProxy.sendToHintedReplicas` |
| StorageProxy.performLocally (l.1646) | inline in `sendToHintedReplicas` (apply + `onResponse`) |
| AbstractWriteResponseHandler.get / blockFor / maybeTryAdditionalReplicas / signal | `WriteResponseHandler.get / blockFor / maybeTryAdditionalReplicas / signal` |
| WriteResponseHandler.onResponse / ackCount | `WriteResponseHandler.onResponse / ackCount` |
| StorageProxy.readRegular (l.1967) | `StorageProxy.readRegular` |
| StorageProxy.fetchRows (l.2068) | `StorageProxy.fetchRows` |
| StorageProxy.concatAndBlockOnRepair (l.2029) | `StorageProxy.concatAndBlockOnRepair` |
| StorageProxy.SinglePartitionReadLifecycle | does not exist in this 4.1 tree (see decision D1) |
| StorageProxy.LocalReadRunnable | inline `handler.response(node.executeLocally(cmd))` |
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
repairs. This is what the port does. The README calls NoopReadRepair "the NONE strategy", but the source
says otherwise. NoopReadRepair is ported as written and is never selected (in 4.1 only DigestResolver.getData
with transient replicas uses it).

D3. **Which replicas are read; data versus digest.** ReplicaPlans.forRead (l.706) takes the candidates
`sortedByProximity` from the snitch (ReplicaLayout.forTokenReadLiveSorted, ReplicaLayout.java l.331) and
contacts the first blockFor = 2 (contactForRead l.660-678). The snitch order is replaced as INTERFACE.md and
the prompt require: the local replica first, then the "fastest" other replica, chosen with
`Nondet.chooseIndex(2)`, then the third. The data read goes to contacts[0], the local replica
(executeAsync l.178, initialDataRequestCount = 1). The digest read goes to the chosen replica. SimpleSnitch
(SimpleSnitch.java l.41) keeps the natural order, and DynamicEndpointSnitch could rank another replica first,
so the snitch is what leaves this open.

D4. **Executor.** getReadExecutor (l.190-212) picks by speculative-retry policy. With no clock there is no
speculation, so it is always NeverSpeculatingReadExecutor (l.258). Its maybeTryAdditionalReplicas only waits
and records a metric (shouldSpeculateAndMaybeWait, l.226), so it does nothing here.
AbstractReadRepair.maybeSendAdditionalReads (l.185-204) sends a speculative read only after a timed wait
expires, so it never sends.

D5. **Local execution.** Local work goes through `Stage.X.maybeExecuteImmediately`: the local data or digest
read (AbstractReadExecutor.makeRequests l.165; AbstractReadRepair.sendReadCommand) and the local mutation
(StorageProxy.performLocally l.1646). SEPExecutor.maybeExecuteImmediately (src-4.1
concurrent/SEPExecutor.java l.218) runs the task inline when it gets a work permit and queues it otherwise.
The port takes the inline branch: the local read or apply runs at once on the coordinator's process, and its
response reaches the callback directly (`handler.response`, `onResponse`) without a message. The queued
branch, where a local read or mutation is overtaken by inbound verbs, is not modelled. Repair mutations
behave differently: BlockingPartitionRepair.sendRR uses `MessagingService.sendWithCallback` even for the
local replica, so a repair of the coordinator's own data is a `ReadRepairReq` to itself, as in the source.

D6. **Concurrency in one process.** Cassandra handles inbound verbs on stages while the coordinator thread is
blocked on a Condition or latch (ReadCallback.awaitResults l.126, AbstractWriteResponseHandler.get l.113,
BlockingPartitionRepair.awaitRepairsUntil l.172). A replica here is one process (INTERFACE.md), so each wait
is a loop: receive any message, dispatch it to its verb handler or callback, and recheck the condition
(`Node.awaitUntil`). Without this, two coordinators waiting on each other would deadlock, which the source
cannot do. Messages are received in the order the communication model delivers them. No guard is used.

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

D9. **What is fetched and digested.** In 4.1, `SELECT a` uses ColumnFilter.allRegularColumnsBuilder (src-4.1
cql3/selection/ColumnFilterFactory.java l.46-57). It fetches every regular column, b included (value
skipped, timestamp kept), and repairs only queried columns (RowIteratorMergeListener.isQueried l.144). As
INTERFACE.md requires ("a digest of the cells read"), a replica's response here contains only the queried
columns, and the digest is computed over them. The digest is a deterministic, injective string of
(column, value, timestamp) and stands in for the MD5 of ReadResponse.makeDigest. With the full fetch, a
`ReadA` would also see a digest mismatch when only `b` differs. That mismatch would trigger a data round that
repairs nothing for `b`. The port excludes it.

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
uncontacted. The local mutation is applied before the remote MutationReqs are sent (insertLocal is handled
before localDc in sendToHintedReplicas l.1547-1555, with D5's inline execution). Ideal-CL delegates,
metrics, tracing and hints are dropped.

D14. **Addresses.** The prompt says every process knows every address from the start, and main must not send.
So each process computes the task ids from its own id and the spawn order. Main checks that the ids are
consecutive before waiting.

D15. **Observation.** `toString()` (printed by the launcher as `observed:`) reports how many client runs
finished and the distinct reply sequences per client. These are collected outside the message-passing API,
for the reader only. They are not part of the protocol and raise no event.

## Compile (inside OUT_DIR)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

## Runs (inside OUT_DIR)

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. Key lines:
```
ran:        31 iterations
blocked:    31 (counted by the strategy)
observed:   finished client runs 62; distinct replies per client [c0:Ack[client=0], c1:ValueA[client=1, a=0], c1:ValueA[client=1, a=1]]
graphs:     0 distinct complete, 31 distinct blocked; stack exhausted
```
Last output line: `max_porf_count: 0`

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,none,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. Key lines:
```
ran:        25 iterations
blocked:    25 (counted by the strategy)
observed:   finished client runs 50; distinct replies per client [c0:Ack[client=0], c1:ValueA[client=1, a=0], c1:ValueA[client=1, a=1]]
graphs:     0 distinct complete, 25 distinct blocked; stack exhausted
```
Last output line: `max_porf_count: 0`

Every graph is "blocked" because the replicas never terminate (they end blocked on receive, as intended).
In every graph both clients finished: 2 per iteration. `blocking` has more graphs than `none` (31 against 25)
because of the repair mutations and their acknowledgements. Extra checks run during development, not of
record:
- q=2 fifo: 2282 graphs (blocking) and 1635 (none), stack exhausted.
- q=1 bag, blocking: 64 graphs, exhausted.
- q=3 blocking: timed out at 120 s.
In all of them every client run finished.
