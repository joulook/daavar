# BookKeeper main port

Target: the supplied main cut, commit `7b66b5d` identified by `bk-src/README.md`. No other BookKeeper version or external material was consulted. Source references below are relative to `bk-src/main` and use the supplied files' line numbers.

## Entry and files

`bench.bookkeeper_sol.BookKeeper` implements `MpProgram` and has the launcher's `String...` constructor. Arguments are exactly `impl,<e>,<fifo|bag>`; `e >= 1`. `fifo` uses `CommunicationModel.P2P`, `bag` uses `ASYN`. Program name is `bookkeeper`; size is the decimal entry count; only `w1` and `w2` are visible.

The single source file is `java/bench/bookkeeper_sol/BookKeeper.java`. The prompt's dotted deliverable `java/bench.bookkeeper_sol.BookKeeper.java` is interpreted as its qualified Java name: `javac` requires a public `BookKeeper` to be in a file named `BookKeeper.java`. The package follows the explicitly requested launcher command (`bench.bookkeeper_sol`), specializing the interface's generic package template.

Every nonblank Java line has a `// PORT:` reason. This includes declarations and scaffolding, since all such lines adapt the original implementation to this API. Comments identify the relevant source component and replacement; the table and decisions below provide precise source locations.

## Roles, tasks, and messages

Main starts `b1`, `b2`, `b3`, `zk`, `w1h`, `w2h`, `w1`, `w2`, then invisible `init`. Main only starts and awaits processes. `init` sends one immutable `Setup` containing all eight addresses to each of the six internal roles. Clients capture their handle addresses at construction; they do not receive extra setup messages. Each internal role selectively receives setup before handling any requests. All named roles are declared; `init` is invisible.

The clients implement the required loops. `w1` sends `Add(long entry)` for 1 through e, waiting for `Acked(long entry)` or `Rejected(long entry)` each time. `w2` sends `Recover()` and waits for `Closed(long last)`. Visible replies are sent by the named handle, including replies dispatched through its callback task. These five records have the corresponding verbatim `@Tag` names.

The handles, bookies, and zk loop forever. Every in-flight `PendingAddOp`, each `RecoveryReadOp`, `ReadLastConfirmedOp`, each `LedgerRecoveryOp`, each bookie request processor, and each metadata close operation is an anonymous logical daemon task. Each handle also has a persistent callback task. Daemon tasks express source asynchronous activities, not reusable pool workers. Main awaits the persistent named processes after both clients; consequently a quiescent execution is classified as blocked by this harness. That is intentional: the services do not terminate.

Each handle exclusively owns its pending queue, callback-triggered projection, LAC, pushed entry ID, completion sequence head, close state, and delayed-failure marker. Each bookie exclusively owns its map, fence bit, and stored LAC. Only messages cross these ownership boundaries. A request processor's fencing and subsequent read are separate storage messages: their dependency is maintained, while another request may access storage between them. The fenced check and normal add are one storage-owner action, corresponding to `synchronized (handle)`.

| Messages | Purpose |
| --- | --- |
| `Setup` | Immutable startup addresses, sent by init only |
| `AddRequest`, `AddResponse` | Entry ID, packaged LAC, flags, bookie index, callback; write status |
| `ReadRequest`, `ReadResponse` | Entry ID (`-1` for LAC), flags, index, callback; status and LAC |
| `FenceLedger`, `FenceResult`, `AddEntry`, `ReadEntry` | Request-processor calls to sequential bookie storage |
| `ReadLedgerMetadata`, `WriteLedgerMetadata`, `VersionedLedgerMetadata` | Metadata snapshot and version-checked update/result |
| `Initiate`, `RecoveryData`, `InitiateComplete` | Add task obtains the handle's LAC and callback-triggered state at initiation; signal synchronous initiation return after all write-set sends |
| `WriteComplete`, `NotifyWriteFailed` | Add task reports quorum completion, failure, or a late failure |
| `SubmitCallback`, `DeliverCallback`, `CallbackDelivered` | Ordered callback task and handle delivery dependency; close continuation waits for drained-add callbacks |
| `InitializeRecovery`, `RecoveryInitialized` | Initialize the recovery handle from fencing LAC |
| `RecoveryAddEntry`, `RecoveryAddStarted`, `AddComplete` | Start recovery add, observe synchronous initiation return after all three sends, asynchronous quorum completion |
| `LastConfirmedDataComplete`, `OnEntryComplete` | LAC and single-entry read callbacks to recovery coordinator |
| `AsyncCloseInternal`, `CloseComplete`, `RecoveryComplete` | Close the recovery handle/metadata and relay final client reply |

All wire values are immutable tagged records, enums, primitives, `Tid`, or immutable lists. `Pending` is exclusively local queue state and is never sent. Operation IDs are entry IDs; there is one attempt per entry and no timers or replacement bookies.

## Method mapping

| Original source and lines | Port component/method | Specialization |
| --- | --- | --- |
| `client/LedgerHandle.java:217-230,550-551` | `LedgerHandle` state, `isHandleWritable` | Local handle state; no implicit metadata watch |
| `client/LedgerHandle.java:1716-1774` | `LedgerHandle.doAsyncAddEntry` | Check writable, allocate next entry ID, enqueue, closed callback or initiate task, in that order |
| `client/LedgerHandle.java:1621-1627` | `LedgerHandle.asyncRecoveryAddEntry` | Same pending-add path with recovery flag |
| `client/PendingAddOp.java:146-154,246-275` | `PendingAddOp.initiate/sendWriteRequest` | Check callback triggered, package current LAC, handle delayed failure, send round-robin write set |
| `client/PendingAddOp.java:278-424` | `PendingAddOp.writeComplete` | Record ack before completed check; completed late-error handling precedes error switch; mark quorum completion last |
| `client/RoundRobinDistributionSchedule.java:53-59,104-109,306-319` | Round-robin arithmetic and `AckSetImpl` | Full write set of three, ack quorum of two, distinct-bookie bit sets |
| `client/LedgerHandle.java:2180-2190,2205-2221` | `handleUnrecoverableErrorDuringAdd`, `errorOutPendingAdds`, `drainPendingAddsAndAdjustLength` | Recovery guard; close original writer; drain pending queue before error callbacks |
| `client/LedgerHandle.java:2225-2256` | `LedgerHandle.sendAddSuccessCallbacks` | Head must be completed and next in sequence; remove, advance LAC, submit callback |
| `client/LedgerHandle.java:2266-2290,2293-2420` | Delayed-failure marker, `maybeHandleDelayedWriteBookieFailure`, `handleBookieFailure` | No replacement possible, so unrecoverable error rather than an ensemble change |
| `client/LedgerHandle.java:576-665,2533-2539` | `doAsyncCloseInternal`, `closeMetadata`, `executeOrdered` | Drain, capture confirmed last, close handle, callbacks, versioned metadata close; dedicated logical callback task |
| `client/LedgerRecoveryOp.java:56-70,98-155` | `LedgerRecoveryOp.initiate`, `RecoveryReadOp` | Fixed first ensemble starts at zero; initialize handle from fencing-LAC result |
| `client/ReadLastConfirmedOp.java:122-132,135-211` | `ReadLastConfirmedOp.initiateWithFencing/readEntryComplete` | Fence all bookies, maximum valid packaged LAC, callback after coverage, drain third reply |
| `client/RoundRobinDistributionSchedule.java:373-401` | Fencing coverage bit set | With N=Qw=3 and Qa=2, coverage is two valid distinct responses |
| `client/LedgerRecoveryOp.java:166-171,176-242,246-259` | `doRecoveryRead`, `onEntryComplete`, `addComplete`, `submitCallback` | Batch one; pipeline next read after recovery-add initiation sends all three write requests; read/write counters gate close |
| `client/PendingReadOp.java:48,66-90,94-122,175-190` | `PendingReadOp.initiate/sendReadTo/readEntryComplete` | Default sequential read, recovery flag = HIGH_PRIORITY OR DO_FENCING |
| `client/PendingReadOp.java:213-246,371-428` | `SequenceReadRequest.read/sendNextRead/logErrorAndReattemptRead/complete` | First valid entry completes; after two missing responses report missing; no speculative reads |
| `client/ListenerBasedPendingReadOp.java:52-84` | `ListenerBasedPendingReadOp.submitCallback` | One completed request, one listener message |
| `proto/WriteEntryProcessorV3.java:45-166` | `WriteEntryProcessorV3.run/getAddResponse` and storage-owner dispatch | Recovery flag chooses `recoveryAddEntry`; normal add checks fencing |
| `proto/ReadEntryProcessorV3.java:157-185,196-258` | `ReadEntryProcessorV3.getReadResponse/readEntry` | Fence first; then read; missing-entry status preserved |
| `bookie/BookieImpl.java:1024-1035,1112-1129,1153-1189` | `BookieImpl.recoveryAddEntry/addEntry/fenceLedger/readEntry/readLastAddConfirmed` | Sequential in-memory storage; normal adds reject fenced, recovery adds never do |
| `bookie/LedgerDescriptorImpl.java:87-119,167-187` | `setFenced/isFenced`, stored-entry map | Monotonic fence, no journal, packaged LAC retained per stored entry |
| LedgerManager/ZooKeeper wrapper absent from cut; interface metadata section | `LedgerManager`, recovery metadata preparation, `closeMetadata` | Versioned Open/InRecovery/Closed store and CAS loop |

## Decisions and boundaries

1. **Source reads override the descriptive quorum sentence.** `INTERFACE.md` and the README describe a recovery read succeeding at Qa responses. The supplied implementation does not do that: `PendingReadOp.java:48,76-80` selects sequential reads by default; `SingleLedgerEntryRequest.complete` at 213-246 accepts the first valid entry. `SequenceReadRequest.logErrorAndReattemptRead` at 387-425 stops after Qw-Qa+1 missing responses. `LedgerRecoveryOp.java:56-70,166-171` does not enable parallel reads. This port uses exactly those source conditions: one success, or two missing responses. No artificial successful-read quorum is added.

2. **Entry numbering.** `LedgerHandle.java:226,1727` starts pushed and confirmed IDs at -1 and allocates entry zero first; the interface exposes entries 1..e. Internal source IDs remain 0..e-1. External Add IDs are decremented, Acked/Rejected IDs incremented, and Closed converts a nonnegative ID by +1 while retaining -1 for empty. This preserves the source's special entry-zero condition in `sendAddSuccessCallbacks` at 2239.

3. **Fencing flags are main's flags.** `PendingReadOp.java:180-185` uses `FLAG_HIGH_PRIORITY | FLAG_DO_FENCING`; `ReadLastConfirmedOp.java:122-130` uses only `FLAG_DO_FENCING`. Recovery writes use RECOVERY_ADD|HIGH_PRIORITY (`PendingAddOp.java:147`). Normal writes have FLAG_NONE. Priority has no scheduling policy in this cut; the bit is still carried. Every recovery read fences the bookie it reaches, even if the requested entry is missing.

4. **Read order.** Use the supplied round-robin write set (`RoundRobinDistributionSchedule.java:58-59,104-109`) for sequential reads, without sticky-read configuration, placement-policy reorder, speculation, or timers. `LedgerHandle.java:2508-2529` permits the nonsticky branch; no sticky configuration is supplied. There is at most one outstanding replica request per recovery read. A later read may coexist with prior recovery writes.

5. **Write configuration.** No DEFERRED_SYNC, fault-domain enforcement, client closure, authorization errors, read-only bookies, or physical I/O failures are introduced. Fenced is the only generated write error. The source default error branch is retained as a projection of failure reporting and the ack set includes `failBookieAndCheck`; no failure injector makes it reachable. Completed operations record all acks, ignore late errors for their own outcome, and notify a delayed failure before returning (`PendingAddOp.java:299-339`); a subsequent initiate handles that failure (`246-275`, `LedgerHandle.java:2272-2290`). With no replacement bookies, failure closes the handle. A late fenced response does not revoke an already completed add.

6. **Callback and queue ordering.** `LedgerHandle.java:2230-2256` is specialized to `changingEnsemble=false` and ordinary sync writes. Each callback submission receives a monotonically increasing local sequence number. The callback task selectively receives the next sequence, requests delivery from the named handle, and waits for delivery before processing another callback. This is the ordered callback dependency (`LedgerHandle.java:2533-2539`), even under bag delivery, without modelling executor worker reuse. A local `CloseAfterCallbacks` continuation delays metadata close until every error callback drained by that close has actually been delivered, preserving the synchronous callback-return edge at `LedgerHandle.java:605-614` before `MetadataUpdateLoop.run` at 623-655. The handle records callback-triggered state when it commits the callback submission. Source locking, queue mutation and op projections are serialized at the handle owner; no queue state is concurrently accessed by an add task.

7. **In-flight add lifetime.** Each initiated add sends all three requests before processing responses, and sends `InitiateComplete` after the third send. The coordinator waits for `RecoveryAddStarted` relayed from that completion before starting the next read; it does not wait for the add quorum callback. This preserves the synchronous call-return edge from `asyncRecoveryAddEntry` through `doAsyncAddEntry` to `PendingAddOp.initiate` (`LedgerHandle.java:1621-1627,1774`), reflecting the synchronized `PendingAddOp.initiate/writeComplete` methods (`246,278`). It consumes all three replies, including after its client callback, preserving the source's late-error path and avoiding orphan replies. If the handle has already triggered that op's callback before initiation, initiation sends nothing (`249-255`). An error can drain an incomplete op while its requests remain in flight; later responses cannot deliver a second client callback because it is no longer in the queue.

8. **Storage and LAC.** The interface replaces the journal, digests, master keys, lengths and physical storage. `BookieImpl.addEntry` at 1120-1127 checks the fence and inserts atomically. Recovery insertion at 1031-1035 ignores the fence. Each stored entry retains its packaged LAC; a LAST_ADD_CONFIRMED read reads the highest stored entry and returns that entry's packaged LAC, as `ReadLastConfirmedOp.java:145-154` extracts it from entry data. `readLastAddConfirmed` returns the maximum carried LAC, never the greatest stored entry ID. Ordinary recovery-read responses do not advance the recovery handle LAC (`PendingReadOp.java:109-114`). Length and payload bytes cannot distinguish these entries in the interface, so only IDs and carried LAC are stored. Recovery overwrites the same entry with its current recovery-handle LAC.

9. **Empty ledgers.** Fencing uses a writable descriptor (`BookieImpl.java:1155`), creating the local ledger if necessary. A subsequent read with no stored entries reports NoSuchEntry, still a valid fencing-LAC response (`ReadLastConfirmedOp.java:169-172`). NoSuchLedger is also represented and accepted, but is not generated after successful descriptor creation/fencing in this failure-free instance.

10. **Metadata wrapper.** The cut explicitly omits the LedgerManager and recovery/open wrapper; `LedgerRecoveryOp.java:98-155` assumes it already has recovery metadata and completes its promise without itself closing metadata. Implement the interface's missing wrapper: CAS Open to InRecovery, use Closed immediately if already present, run recovery if InRecovery, then close. Both handles have independent local metadata/close state; marking zk InRecovery does not notify the writer. A writer unrecoverable error closes its local handle first and fails its pending adds before attempting metadata close (`LedgerHandle.java:593-614`). The metadata close CAS retries version conflicts. Existing Closed metadata is immutable and authoritative to the recovery wrapper; it returns that last ID. For the writer's no-op close callback, a conflicting Closed value has no further visible effect, corresponding to the failed close promise at `LedgerHandle.java:627-644`. The supplied cut does not define the recovery wrapper's response to another closer winning the CAS; returning the existing final metadata is the explicit decision here. No metadata update reopens or overwrites Closed.

11. **Start moment.** The scheduler may delay w2's first send by any finite interleaving, including until after all writer replies. This implements the interface's nondeterministic start without adding a visible message, timer, or bounded delay loop. There is no source-prescribed finite set of start instants requiring an additional `Nondet` value. Requests are never abandoned.

12. **Lifetime and bookkeeping.** Dynamic tasks are anonymous daemons; the eight service/client roles and init remain stable across executions. Bookie dispatch creates a separate processor for each request, rather than making the whole request atomic at the bookie loop. Setup messages, flags, metadata, request callbacks, and queue projections are internal. No assertion purports to prove conformance; source invariants such as expected recovery entry ID remain checked. The requested launcher runs are exploration runs without an attached specification file.

## Compilation and runs

Compile, from OUT_DIR:

```sh
javac -d java/out -cp "$(cat ./cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Result: exit 0, no compiler output. Final compilation includes the explicit recovery-add initiation-return and error-callback-before-metadata dependencies.

Requested runs (stdout/stderr captured for the final lines):

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper_sol.BookKeeper:impl,1,fifo' must 0 1 report - 900 </dev/null > run-impl-1.log 2>&1
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper_sol.BookKeeper:impl,2,fifo' must 0 1 report - 900 </dev/null > run-impl-2.log 2>&1
```

Preliminary e=1 explorations used earlier snapshots while reviewing initiation-return and close-callback dependencies; their output is superseded by the final runs of the corrected binary below. Final run results and last output lines follow. A timeout is a bounded exploration result, not a conformance proof or stack exhaustion.

### Final impl,1,fifo

Exit status: 0. Stopped at the requested 900-second limit; 855 distinct blocked graphs, stack not exhausted. No program assertion was reported. Persistent services account for blocked rather than completed graphs. Last output lines:

```text
ran:        855 iterations
blocked:    855 (counted by the strategy)
completed:  0
rate:       0.9 iterations/s
error:      Halting execution: 855 due to timeout
elapsed:    900.126 s
observed:   bench.bookkeeper_sol.BookKeeper@7e2c64
graphs:     0 distinct complete, 855 distinct blocked; stack not exhausted
--- measurements
wall_ms: 904446
explore_ms: 900125
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 855
summaries_distinct: 0
iterations: 855
stop_reason: timeout
peak_rss_kb: 618704
max_message_events: 210
max_porf_pairs: 0
max_graph_events: 219
mean_graph_events: 219.000
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

### Final impl,2,fifo

Exit status: 0. Stopped at the requested 900-second limit (checker elapsed 903.873 seconds); 910 distinct blocked graphs, stack not exhausted. No program assertion was reported. Last output lines:

```text
2026-11-05 22:53:26,387 [Thread-0]  INFO o.m.j.r.s.Scheduler$SchedulerThread - Shutting down scheduler thread.
2026-11-05 22:53:26,387 [Thread-0]  INFO o.m.j.r.s.Scheduler$SchedulerThread - Scheduler thread finished.
completed:  0
rate:       1.0 iterations/s
error:      Halting execution: 910 due to timeout
elapsed:    903.873 s
observed:   bench.bookkeeper_sol.BookKeeper@2e8a1ab4
graphs:     0 distinct complete, 910 distinct blocked; stack not exhausted
--- measurements
wall_ms: 912255
explore_ms: 903873
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 910
summaries_distinct: 0
iterations: 910
stop_reason: timeout
peak_rss_kb: 824748
max_message_events: 264
max_porf_pairs: 0
max_graph_events: 273
mean_graph_events: 271.673
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

Both final runs used the final compiled source. Both stopped on the time limit with exploration work remaining. They do not establish exhaustive conformance; no specification was supplied to these requested counting-mode commands.
