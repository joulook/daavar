# Cassandra quorum read repair port

Implementation: `java/bench/cassandra_rr/CassandraReadRepair.java`.
All Cassandra paths and line numbers below are relative to `read-repair-src/`.
The requested first read of `read-repair-src/README.md` failed because that file is absent from the supplied directory. The supplied Java sources and `INTERFACE.md` were used directly. No web or other repository files were consulted.

## Roles and execution

`c0` and `c1` are the only visible roles. Client i sends to `ni`, performs exactly q sequential operations, with `(i+j)%3` selecting Write, ReadA, or ReadAB, and waits for the corresponding reply. Write values are `1L+i+2L*j`. The six visible records have exactly the interface's names, field order, and primitive types, and matching `@Tag` annotations.

`n0`, `n1`, `n2` each own independent cells a and b and a coordinator state machine. The receive loop services replica requests even while the coordinator is waiting. Each cell stores an explicit value and timestamp. Both start at (0,0). The coordinator phases are IDLE, WRITE, DIGEST, FULL_DATA, and REPAIR_ACK. Its client has at most one outstanding operation, but other coordinators' replica work can arrive at any phase.

Main constructs all five `MpThread`s, installs immutable address lists before starting them, starts replicas in index order then clients in index order, and awaits clients then replicas. It sends and receives nothing. Constructor-time address installation shares no mutable protocol state. Replicas receive forever; the harness ends executions with them blocked on receive. No daemon processes or extra worker processes are introduced.

Every exchange, including local requests and their replies, uses `Channel.send` and `Channel.recv`. All receives accept the next message; there is no private mailbox, polling, shared replica storage, or selective receive that skips an earlier message. FIFO selects P2P and bag selects ASYN. The sole protocol choice is `Nondet.choose` between the two nonlocal replicas for each read. All other competition is resolved by message arrival order.

## Messages and protocol

| Message | Sender → receiver | Action |
| --- | --- | --- |
| Write(client,value) | ci → ni | Begin write; broadcast both cells at timestamp=value to all three replicas. |
| ReadA(client), ReadAB(client) | ci → ni | Begin read with mask 1 (a) or 3 (a,b); choose the other contact. |
| Ack(client) | ni → ci | Sent after two distinct MutationRsp messages for this write. |
| ValueA(client,a), ValueAB(client,a,b) | ni → ci | Return the selected snapshot on digest agreement, or fresh reconciled cells on mismatch; blocking waits for repairs first. |
| MutationReq(coordinator,id,mask,data) | coordinator → every replica | Reconcile each masked cell into local storage, then acknowledge. Ordinary writes always use mask 3. |
| MutationRsp(id,replica) | replica → coordinator | Count toward the write quorum. A third/late acknowledgement is consumed and ignored. |
| ReadReq(coordinator,id,phase,mask,digest) | coordinator → read contact | Take an immutable snapshot of selected cells and answer with data or digest. Phase 0 is initial; phase 1 is repair data. |
| DataRsp(id,replica,phase,data) | replica → coordinator | Supply data to the appropriate read round. |
| DigestRsp(id,replica,digest) | replica → coordinator | Supply the initial digest, compared with the digest calculated from initial data. |
| RepairReq(coordinator,id,mask,data) | coordinator → stale contact | Apply only differing selected cells by timestamp, then acknowledge, even if concurrent newer data makes the repair a no-op. |
| RepairRsp(id,replica) | repaired replica → coordinator | Decrement the repair acknowledgement threshold. |

The initial read sends one data request to self and one digest request to the chosen other replica. Both replies are required. A match returns the initial data without further messages. A mismatch sends NEW full data requests to BOTH contacts; it does not reuse the initial data response and does not query the third replica. Reconciliation merges these fresh responses independently per cell by maximum timestamp.

For `none`, reconciliation immediately produces the reply with no repair writes. For `blocking`, the coordinator compares each fresh response with the merged result and builds a separate masked delta for each contact. No unselected or unchanged cell is included. Repair write contacts are the two read contacts. The threshold is `max(2 - number_of_contacts_without_mutations, 0)`, following BlockingPartitionRepair. With exactly two contacts it equals the number of nonempty repair mutations, so every such mutation must be acknowledged. It can be zero if the fresh responses agree after an initial digest mismatch. Disjoint newer cells can require repairs to both contacts. The reply retains the reconciled snapshot even if later writes arrive while repair acknowledgements are pending.

Operation ids increase per coordinator; requests carry the coordinator's Tid, and responses carry the id, replica index, and read phase when applicable. Separate response classes distinguish ordinary writes and repairs. Late replies cannot count toward another operation.

## Cassandra method mapping

Methods combined into one state-machine transition are listed together. This maps the behavior ported, rather than claiming to reproduce unused Cassandra features.

| Cassandra method (source lines) | Port method/transition |
| --- | --- |
| StorageProxy.mutate (873–901), performWrite (1360–1383), sendToHintedReplicas (1464–1562) | Node.performWrite; broadcast MutationReq |
| WriteResponseHandler constructor (43–50), onResponse (58–65) | Node.begin, Node.onWriteResponse |
| ConsistencyLevel.quorumFor (91–94), blockFor (133–148) | Fixed quorum 2 in onWriteResponse and repairPartition |
| StorageProxy.readRegular (1967–1981), fetchRows (2068–2133) | Node.executeRead and subsequent receive-loop transitions |
| ReplicaPlans.forRead (706–713), contactForRead (660–675) | Node.executeRead; local plus Nondet-selected other |
| AbstractReadExecutor.executeAsync (178–185), makeFullDataRequests (116–120), makeDigestRequests (127–132), makeRequests (134–166) | Node.executeRead, ReadReq |
| ReadCallback.onResponse (179–203), awaitResults (126–171) | Node.onDataResponse, onDigestResponse, awaitInitialResponses; FULL_DATA response counter |
| DigestResolver.preprocess (56–63), getData (77–103), responsesMatch (105–134) | initialData/remoteDigest accumulation, digest, awaitInitialResponses |
| AbstractReadExecutor.awaitResponses (404–439), awaitReadRepair (442–446) | Node.awaitInitialResponses, startRepair, resolve |
| ReadRepairStrategy.NONE.create (28–34), BLOCKING.create (37–43) | Constructor strategy argument; Node.resolve branch |
| AbstractReadRepair.startRepair (125–153), sendReadCommand (93–120), awaitReads (156–174) | Node.startRepair, phase-1 ReadReq, onDataResponse |
| DataResolver.resolve (94–130), resolveWithReadRepair (209–221), resolveInternal (270–299) | Node.resolve |
| Cells.reconcile / resolveRegular (68–84) | reconcile; also used by Node.apply |
| BlockingReadRepair.getMergeListener (62–65), PartitionIteratorMergeListener.getRowMergeListener (47–50) | Node.repairPartition / diff |
| Rows.diff (127–214), RowIteratorMergeListener.onCell / isQueried (138–154), close (367–400) | Node.diff, per-target delta construction in repairPartition |
| ReplicaPlans.writeReadRepair selector (551–578) | The two selected read contacts in repairPartition |
| BlockingReadRepair.repairPartition (112–116) | Node.repairPartition |
| BlockingPartitionRepair constructor (69–97), sendInitialRepairs (151–171) | Node.repairPartition threshold and RepairReq sends |
| BlockingPartitionRepair.ack / onResponse (112–121), awaitRepairsUntil (181–196) | Node.onRepairResponse, REPAIR_ACK phase |
| BlockingReadRepair.awaitWrites (83–109), StorageProxy.concatAndBlockOnRepair.close (2038–2042) | Node.onRepairResponse → replyRead |
| ReadOnlyReadRepair.getMergeListener (47–50), awaitWrites (71–74) | Node.resolve → replyRead without repair |
| MutationVerbHandler.doVerb / applyMutation / respond (33–75) | Node.mutationVerb |
| ReadCommandVerbHandler.doVerb (50–129), ReadCommand.createResponse (343–351) | Node.readVerb |
| ReadResponse.createDigestResponse (70–74), digest construction (127–132) | digest (deterministic tuple string) |
| ReadRepairVerbHandler.applyMutation (39–42) | Node.repairVerb |
| Mutation.applyFuture (225–228), apply (250–253) | Node.apply (in-memory timestamp merge) |

## Decisions and simplifications

1. **Fixed topology and data**, required by INTERFACE.md: replace tokens, partition iterators, storage, keyspace schema, pending/transient replicas, and replication strategies with three full replicas, one partition, two live cells, and quorum 2. Source generality: ReplicaPlans.forRead (706–713), writeReadRepair (551–578), ConsistencyLevel.quorumFor (91–94), DataResolver.resolveInternal (270–299), Mutation.apply (250–253). Treat all nodes as one datacenter, with no forwarding needed (StorageProxy.sendToHintedReplicas 1505–1561).
2. **Snitch ordering**: local is first and supplies data; the second contact is an API choice over the two remote replicas. The interface fixes local inclusion, while source delegates proximity ordering to candidates (ReplicaPlans 700–713; StorageProxy 2075–2088; AbstractReadExecutor 180–184). No latency measurements are modeled.
3. **Local scheduling**: local operations use self-messages instead of Cassandra stage runnables/direct callbacks (StorageProxy 1546–1549; AbstractReadExecutor 161–165; AbstractReadRepair 97–100). Local and remote request execution can therefore interleave through the same receive loop. All mutation sends are issued before processing their acknowledgements; all read-round requests are issued before processing round responses. Broadcast/repair targets use stable index/contact order rather than adding a choice for collection iteration order. This is the selected sequential message-passing abstraction of Cassandra's stage execution and send loops (StorageProxy 1546–1561; AbstractReadRepair 148–150; BlockingPartitionRepair 155–171).
4. **Cell values and timestamps**: each ordinary write constructs two identical (value,value) cells. Missing/initial cells are (0,0); omitted fields in snapshots/deltas are canonical zero and ignored using the mask. No tombstones, TTL, counters, deletion metadata, or unequal values at equal timestamps are reachable. The source's additional equal-timestamp rules (Cells.resolveRegular 86 onward) reduce to either equal cell; this port chooses the left. Incoming ordinary writes and repairs merge rather than overwrite newer cells.
5. **Digest representation**: use a collision-free string tuple of selected values AND timestamps, not a cryptographic digest (ReadCommand.createResponse 349–351; ReadResponse 127–132). ReadA always canonicalizes b before digesting. A digest carries no usable data for reconciliation: mismatch still triggers both fresh reads (AbstractReadRepair 125, 141–150).
6. **Eager merge instead of iterator lifetime**: consume the fixed cells, calculate differences, send repairs, and gate the client reply in explicit phases. This replaces lazy iterator merge/close and futures/latches while retaining ordering (DataResolver 294–299; RowIteratorMergeListener 367–400; StorageProxy 2038–2042). A replica applies both masked cells atomically within a receive handler before sending its acknowledgement; snapshots likewise capture both cells together. The source delegates this work to storage execution controllers and mutation application (ReadCommandVerbHandler 92–96; MutationVerbHandler 73–75).
7. **No speculative operations/timeouts**, as required: omit initial read retries (AbstractReadExecutor 169–173, 226–248), additional repair reads (AbstractReadRepair 185 onward), additional repair writes (BlockingPartitionRepair 216 onward), and timeout/error branches (ReadCallback.awaitResults 126–171; BlockingReadRepair 83–109). No message loss, failure detector, hints, tracing, metrics, size limits, bootstrap state, range filtering, short reads, paging, or repaired-SSTable tracking are represented. These do not occur in the prescribed fixed successful-exchange model.
8. **Callback bookkeeping**: coordinator-local ids and phase tags replace Cassandra callback registration/message correlation. Only one own-client operation is active, as required by the client loop. Late write acknowledgements are drained, not awaited after quorum (WriteResponseHandler 58–65). Separate protocol message types and duplicate guards make acknowledgement counting explicit; there are no retransmissions.
9. **Initialization and termination**: the Java interface's known-address assumption is implemented by pre-start address initialization, with no setup messages from main. Replicas never terminate. Main joins clients first so client failures are observable before it awaits the permanently receiving replicas. All five processes are ordinary named MpThreads; no extra process or communication is hidden outside the API.

## Compilation and runs

Run from OUT_DIR (`.`). Compilation succeeded, exit code 0, with no output:

```sh
javac -d java/out -cp "$(cat ./cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Requested exhaustive runs (both exit code 0, stack exhausted):

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,blocking,fifo' must 0 1 report - 600 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,1,none,fifo' must 0 1 report - 600 </dev/null
```

Blocking output includes:

```text
ran:        3090 iterations
blocked:    3090 (counted by the strategy)
completed:  0
graphs:     0 distinct complete, 3090 distinct blocked; stack exhausted
```

Its last output lines:

```text
iterations: 3090
stop_reason: exhausted
peak_rss_kb: 382948
max_message_events: 40
max_porf_pairs: 0
max_graph_events: 45
mean_graph_events: 41.816
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

None output includes:

```text
ran:        2313 iterations
blocked:    2313 (counted by the strategy)
completed:  0
graphs:     0 distinct complete, 2313 distinct blocked; stack exhausted
```

Its last output lines:

```text
iterations: 2313
stop_reason: exhausted
peak_rss_kb: 196604
max_message_events: 36
max_porf_pairs: 0
max_graph_events: 41
mean_graph_events: 39.599
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

The runs emitted a logging-configuration diagnostic (`Console contains an invalid element or attribute "level"`) at startup, but both checked successfully. These commands request no export, so zero visible-event/export measurements are not assertions about absence of client traffic. Terminal blocked executions are expected because replicas continue receiving forever; this is not a claim of specification equivalence (no separate specification was supplied).

Supplementary runs cover ReadAB and each client's write with bag delivery (200 random executions each, not exhaustive):

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,3,blocking,bag' mp-random 200 1 report-blocking-bag - 60 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_rr.CassandraReadRepair:impl,3,none,bag' mp-random 200 1 report-none-bag - 60 </dev/null
```

Both exited 0. Last output lines, blocking:

```text
ran:        200 iterations
blocked:    200 (counted by the strategy)
completed:  0
rate:       131.6 iterations/s
elapsed:    1.520 s
observed:   bench.cassandra_rr.CassandraReadRepair@5524cca1
```

None:

```text
blocked:    200 (counted by the strategy)
completed:  0
rate:       128.2 iterations/s
elapsed:    1.560 s
observed:   bench.cassandra_rr.CassandraReadRepair@5524cca1
```
