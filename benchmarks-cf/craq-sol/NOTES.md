# 3FS one-chunk port

The implementation is in `java/bench/craq_sol/ChainReplication.java`, package
`bench.craq_sol`. The requested dotted basename `java/bench.craq_sol.ChainReplication.java`
was tried and rejected by javac: `class ChainReplication is public, should be declared
in a file named ChainReplication.java`. This standard Java package path is the one
necessary filename adjustment. There is one source file. The launcher reflects a
public `String[]` constructor (api/api/launch/MpLauncher.java:243–249), so the
constructor accepts the launcher's comma-split arguments.

## Roles, state and messages

Main constructs all addresses before starting any process, then starts n0, n1, n2,
c0, c1, retry0, retry1, in that order, and only joins. There are no setup messages
and no messages sent or received by main. The five prescribed role names are exact;
the two additional invisible roles are fixed retry coroutines, needed because this
API exports every send/receive on a visible client (SummaryExporter.java:79–99).
No process is created after startup. Each node owns its own ChunkReplica and
StorageTarget; no mutable metadata is shared between processes.

* c0 and c1 execute q sequential operations, kind `(i+j)%2`, write value
  `1+i+2*j`. A write routes to n0 and waits for Ack. A read nondeterministically
  selects n0/n1/n2, sends Read, and waits for Value. Only these clients are visible.
* n0 is head, n1 is middle, n2 is tail. Each is the single chunk-store process
  authorized by the coordinator's November 5 resolution. Its receive loop processes
  atomic local chunk operations, while a suspended update retains its chunk lock
  and continuation. Waiting for a successor does not block the loop: reads and
  second write arrivals can still be received. A second update queues behind the
  chunk lock and cannot apply until the previous commit. All nodes receive forever.
* retry0/retry1 are internal pieces of the corresponding storage client coroutine.
  They receive failed attempts, remember every failed version pair per target for
  the same read, select a target with Nondet, and send the next attempt immediately. A
  successful node reply goes straight to c0/c1. These internal roles receive
  forever; an unproductive retry blocks the retry process and consequently the
  visible client's outstanding Value receive.

| Message | Sender → receiver | Meaning |
|---|---|---|
| Write(client,value) | ci → n0 | Exact visible full-chunk write |
| Ack(client) | n0 → ci | Exact visible reply, after local commit |
| Read(client) | ci → selected node | Exact visible initial read |
| Value(client,value) | selected node → ci | Exact visible successful read reply |
| UpdateReq(client,value,updateVer,chainVer,fromClient) | node → successor | Full update, assigned version, constant chain version 1, fromClient=false |
| UpdateRsp(updateVer,commitVer,chainVer,checksum) | successor → predecessor | Successor has committed; checksum is content |
| NotCommit(client,target,updateVer,commitVer,first) | node → retryi | Internal kChunkNotCommit; first identifies the initial attempt of a new read |
| RetryRead(client) | retryi → selected node | Internal read attempt; successful result still goes to ci |

The head converts Write into a local UpdateReq with updateVer=0 and fromClient=true.
This is a local call, not a process exchange. The tail's no-successor result is also
a local continuation call, not a fabricated network message. Every exchange between
processes uses Channel.send/recv with the selected communication model. The sole
protocol choice is TargetSelectionStrategy.selectTarget using Nondet.chooseIndex(3).
All other arbitration follows receive order or the chunk-lock waiter queue.

## Method correspondence

Line numbers below refer to the supplied craq-src snapshot (README.md identifies
3FS commit 22fca04). The Java classes are nested under ChainReplication, preserving
the requested class and method names. Each nonblank Java line carries `// PORT:`
because even unchanged protocol expressions require Java syntax or API scaffolding.

| 3FS method | Java method | Source |
|---|---|---|
| StorageClientImpl.batchWriteWithoutRetry | StorageClientImpl.batchWriteWithoutRetry | client/storage/StorageClientImpl.cc:1799–1858 |
| StorageClientImpl.sendWriteRequest | StorageClientImpl.sendWriteRequest | same:1860–1927 |
| StorageClientImpl.batchReadWithoutRetry | StorageClientImpl.batchReadWithoutRetry | same:1642–1747 |
| StorageClientImpl.batchReadWithRetry | StorageClientImpl.batchReadWithRetry | same:1577–1640; retry loop 1144–1300; fast errors 166–170 |
| selectRoutingTargetForOps | selectRoutingTargetForOps | same:490 onward |
| TargetSelectionStrategy / selectTarget | TargetSelectionStrategy / selectTarget | client/storage/TargetSelection.cc:16–55,110–119 |
| StorageOperator.write | StorageOperator.write | storage/service/StorageOperator.cc:233–282 |
| StorageOperator.update | StorageOperator.update | same:284–331 |
| StorageOperator.handleUpdate | StorageOperator.handleUpdate(UpdateReq), handleUpdate(UpdateRsp) | same:333–519 |
| StorageOperator.doUpdate | StorageOperator.doUpdate | same:521–614 |
| StorageOperator.doCommit | StorageOperator.doCommit | same:616–638 |
| StorageOperator.batchRead | StorageOperator.batchRead | same:82–231 |
| ReliableUpdate.update | ReliableUpdate.update, cache | storage/service/ReliableUpdate.cc:16–128 |
| ReliableForwarding.forwardWithRetry | ReliableForwarding.forwardWithRetry | storage/service/ReliableForwarding.cc:33–104 |
| ReliableForwarding.forward | ReliableForwarding.forward | same:106–136 |
| ReliableForwarding.doForward | ReliableForwarding.doForward | same:138–280 |
| ChunkReplica.update | ChunkReplica.update | storage/store/ChunkReplica.cc:132–318 |
| ChunkReplica.commit | ChunkReplica.commit | same:397–469 |
| ChunkReplica.aioPrepareRead | ChunkReplica.aioPrepareRead | same:38–78 |
| ChunkReplica.aioFinishRead | ChunkReplica.aioFinishRead | same:79–102 |
| TargetMap head/successor/tail construction | TargetMap.isHead, successor | storage/service/TargetMap.cc:193,221–254 |
| StorageTarget.lockChunk | StorageTarget.lockChunk | storage/store/StorageTarget.h:61–64 |

## Decisions and reductions

1. **Concurrency and suspension.** The source coroutine suspends at the chunk lock
   and successor RPC (StorageOperator.cc:380–390,449–450). The node loop implements
   explicit continuations rather than a blocking Java call. UpdateReq starts the
   continuation; UpdateRsp resumes it after the successor commits. This is one
   chunk-store process per node, exactly the coordinator's authorized synchronization
   point. There are no independent chunks in this study. Node program order includes
   local request processing as allowed by that resolution. No global coordinator or
   shared metadata object imposes cross-node order.
2. **Lock arbitration.** StorageTarget.h:61–64 exposes lockChunk but does not specify
   fairness or waiter scheduling. Waiters use FIFO arrival order. The lock is held
   through update, forwarding and commit, while reads ignore that update lock and
   inspect the chunk metadata. Lock ownership is released before replying; queued
   continuation processing follows the reply send in the same store process.
3. **Atomic memory operations.** The coordinator explicitly permits serialization of
   update, commit and read requests at the one-chunk store. doUpdate/doCommit call
   the replica directly, replacing UpdateWorker enqueue/complete
   (StorageOperator.cc:598–600,629–630). Successful update ends CLEAN, although its
   versions still differ; DIRTY is only the transient physical-write state
   (ChunkReplica.cc:240–318). Reads prepare and finish within one memory operation.
   Both version checks remain, in source order, but no disk I/O window remains for
   the finish check to fail (original window ChunkReplica.cc:79–102). This is the
   chosen in-memory whole-read granularity under the coordinator's resolution.
4. **Version rules and errors.** ChunkReplica.update retains DIRTY rejection,
   committed/stale/missing update conditions, and allocation of the next head
   version in source order (ChunkReplica.cc:179–240). commit retains future-version
   rejection, DIRTY rejection, advancing commit, stale commit in source order
   (same:419–453). The service's success/missing/committed/stale/advance branch order
   remains (StorageOperator.cc:402–440). Unreachable failures in this failure-free
   fixed-chain workload raise AssertionError rather than introducing unspecified
   client error messages. Successful successor version and checksum validation
   precede commit (same:451–510). With a locked chain and no duplicates, all actual
   updates take the success branch.
5. **Reliable channels.** ReliableUpdate.cc:29–119 validates tags, locks a client
   channel, checks sequence numbers, and caches results. Here each client issues
   sequential requests, every RPC is delivered once, no request is retried, and
   only the head forwards. Thus duplicate, concurrent-same-channel, and cached
   retry branches cannot arise. ReliableUpdate.update calls handleUpdate directly;
   cache records the last result per client, without adding tag fields to visible
   Write. No cross-client channel dependency is added.
6. **Routing and chain.** TargetMap is the fixed 0→1→2 chain and chain version 1.
   The interface fixes all targets serving, and maps source Default HeadTarget
   writes and LoadBalance reads to head and nondeterministic choice. Source
   TargetSelection.cc:20–42 includes random preference and historical load counters;
   those heuristics are replaced by the interface's unrestricted choice. There
   are no routing failures, syncing, leases, resyncs, read-only mode or validation
   failures for the fixed valid chunk and full-chunk I/O (StorageOperator.cc:338–357).
7. **Forwarding.** ReliableForwarding.forwardWithRetry sends once and awaits one
   reply by continuation. Source retry and back-off paths (ReliableForwarding.cc:45–98)
   require errors/timeouts excluded here. At the tail, the source's kNoSuccessorTarget
   leaves the proposed commit version intact and sets chain version; the Java local
   continuation carries exactly that version (same:113–117). Successor commit
   version is the local commit version at other targets (same:121–123).
8. **Read retry visibility.** Source batchReadWithRetry calls batchReadWithoutRetry
   for each retry and immediately retries kChunkNotCommit
   (StorageClientImpl.cc:166–170,1144–1300,1577–1608). The API has no invisible-event
   flag, and SummaryExporter exports every message event of a visible role.
   Therefore the failed attempt answer and subsequent attempts use one fixed
   internal coroutine per client. Their causal edges still reach the eventual
   Value. Initial Read and final Value retain the exact required visible shape.
   Successful replies bypass the helper; the next first=true failure resets its
   remembered attempt, so different reads are never compared.
9. **Study cut (third resumption).** There is no assume in the supplied
   MpThread/MpProcess/Channel/Nondet/Monitors API. For each read, the retry coroutine
   keeps a set of `(target, updateVer, commitVer)` answers. A NotCommit whose key
   is already present blocks on `Channel.recv(model, m -> false)`; no further
   retry is sent, so the visible client's outstanding Value receive remains
   blocked too. This implements INTERFACE.md's revised last paragraph, including
   alternating unchanged targets. The first=true answer clears the set before
   insertion, so answers from different reads are never compared. All previously
   seen pairs are retained, including after a different answer from the same target.
   This finite-exploration cut is a study addition, not a 3FS behavior; the source
   fast retry and retry call sites are StorageClientImpl.cc:166–170,1144–1300,
   1577–1605. Every changed Java line carries its PORT reason.
10. **Loop audit.** Each retry follows a received NotCommit; no re-selection runs
    without a receive. Each of the finitely many writes advances a target version
    at most once, so there are finitely many distinct failed version pairs per
    target. The per-read set therefore bounds active retries even when targets
    alternate. Forwarding sends once and awaits its successor by continuation;
    there is no forwarding or write retry. Startup creates exactly seven processes
    and never creates more. Node and retry service loops block on receive when
    idle, as required; clients perform exactly q sequential operations unless cut.
11. **Data and excluded mechanisms.** The integer content is also the checksum;
    metadata starts content=updateVer=commitVer=0 at all replicas. RDMA, allocation,
    persistence, partial ranges, truncate/extend/remove, physical failure handling,
    tracing, metrics and timers are removed as requested. ChunkReplica.cc:198–208,
    245–318 and StorageOperator.cc:521–638 supply the replaced data/store operations.
    All local checksum consistency and version checks relevant to reachable full
    writes remain. fifo maps to P2P, bag maps to ASYN (CommunicationModel.java).

## Validation commands and results

Compile (executed successfully after the filename and constructor adaptation):

```sh
javac -d java/out -cp "$(cat ./cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Successful compilation produced no output.

The q=1 1200-second command supersedes the original 600-second q=1 request:

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq_sol.ChainReplication:impl,1,fifo' must 0 1 report - 1200 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq_sol.ChainReplication:impl,2,fifo' must 0 1 report - 600 </dev/null
```

Both commands were launched. The historical results below used the old
consecutive-answer cut; current per-target-cut results follow them. The launcher emits a logging configuration diagnostic,
`main ERROR Console contains an invalid element or attribute "level"`, before
starting exploration; this is not a protocol assertion failure.

### Historical q=2 output (superseded consecutive-answer cut)

The 600-second q=2 search did **not** exhaust. The launcher exited with status 0,
but explicitly reported timeout and a nonempty exploration stack. Its final lines:

```text
ran:        395 iterations
blocked:    395 (counted by the strategy)
completed:  0
rate:       0.7 iterations/s
error:      Halting execution: 395 due to timeout
elapsed:    603.314 s
observed:   bench.craq_sol.ChainReplication@68ad99fe
graphs:     0 distinct complete, 395 distinct blocked; stack not exhausted
--- measurements
wall_ms: 605326
explore_ms: 603313
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 395
summaries_distinct: 0
iterations: 395
stop_reason: timeout
peak_rss_kb: 1434872
max_message_events: 824
max_porf_pairs: 0
max_graph_events: 1030
mean_graph_events: 535.261
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

The complete/blocked numbers above are the launcher's graph classification, not a
count of client operations. Nodes intentionally never terminate and main awaits
them, so their terminal receive is blocked. The supplied MpThread.join1 also
propagates blocked status to its awaiter. The requested report-only command does
not export summaries (`-`); no summary counts are inferred from client completion.

### Historical q=1 output (superseded consecutive-answer cut, 1200-second limit)

The q=1 search did **not** exhaust. The launcher completed its timed run and
printed final counts: **515 iterations, 0 distinct complete graphs, 515 distinct
blocked graphs, stack not exhausted**. It exited with status 0 while reporting
`stop_reason: timeout`. No protocol assertion failure was reported. Its final
output lines were:

```text
ran:        515 iterations
blocked:    515 (counted by the strategy)
completed:  0
rate:       0.4 iterations/s
error:      Halting execution: 515 due to timeout
elapsed:    1205.707 s
observed:   bench.craq_sol.ChainReplication@533b266e
2026-11-05 21:37:35,814 [Thread-0]  INFO o.m.j.r.s.Scheduler$SchedulerThread - Shutting down scheduler thread.
2026-11-05 21:37:35,818 [Thread-0]  INFO o.m.j.r.s.Scheduler$SchedulerThread - Scheduler thread finished.
graphs:     0 distinct complete, 515 distinct blocked; stack not exhausted
--- measurements
wall_ms: 1208252
explore_ms: 1205706
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 515
summaries_distinct: 0
iterations: 515
stop_reason: timeout
peak_rss_kb: 2531912
max_message_events: 1044
max_porf_pairs: 0
max_graph_events: 1309
mean_graph_events: 668.250
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

These are measured timeout results, not an exhaustive verification claim. The
consecutive-answer cut is implemented exactly, but the alternating-target family
described above still has unbounded finite prefixes. No stronger cut was added.
Both searches used the same compiled source; q=2 ran concurrently with part of q=1.
The rates and timeout-prefix counts therefore reflect that resource sharing.


### Current q=1 output (per-target cut, 1200-second limit)

Recompiled successfully with the compile command above (exit 0, no output), then ran:

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq_sol.ChainReplication:impl,1,fifo' must 0 1 report - 1200 </dev/null
```

The launcher exited 0 and explicitly printed **stack exhausted**: **29 iterations,
0 distinct complete graphs, 29 distinct blocked graphs**, `stop_reason: exhausted`.
The intentional terminal receives and joins explain the blocked classification;
these counts do not imply that every read was cut. No assertion failure occurred.
Final output:

```text
ran:        29 iterations
blocked:    29 (counted by the strategy)
completed:  0
rate:       22.0 iterations/s
elapsed:    1.321 s
observed:   bench.craq_sol.ChainReplication@17f7cd29
graphs:     0 distinct complete, 29 distinct blocked; stack exhausted
--- measurements
wall_ms: 2305
explore_ms: 1320
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 29
summaries_distinct: 0
iterations: 29
stop_reason: exhausted
peak_rss_kb: 120172
max_message_events: 24
max_porf_pairs: 0
max_graph_events: 34
mean_graph_events: 29.241
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```


### Current q=2 output (per-target cut, 600-second limit)

Reran against the same rebuilt source:

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq_sol.ChainReplication:impl,2,fifo' must 0 1 report - 600 </dev/null
```

Exit 0; **stack exhausted**, **1222 iterations, 0 distinct complete graphs,
1222 distinct blocked graphs**. No assertion failure occurred. Final output
(scheduler shutdown logging interleaved with the counts is omitted):

```text
ran:        1222 iterations
blocked:    1222 (counted by the strategy)
completed:  0
rate:       117.9 iterations/s
elapsed:    10.366 s
observed:   bench.craq_sol.ChainReplication@44536de4
graphs:     0 distinct complete, 1222 distinct blocked; stack exhausted
--- measurements
wall_ms: 10910
explore_ms: 10365
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 1222
summaries_distinct: 0
iterations: 1222
stop_reason: exhausted
peak_rss_kb: 376004
max_message_events: 48
max_porf_pairs: 0
max_graph_events: 61
mean_graph_events: 51.250
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```
