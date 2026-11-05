# BookKeeper 4.13 ledger replication with fencing: Java transliteration (agent `java-4.13`)

Source: `plan/case-studies/bookkeeper/bk-src/4.13` (tag release-4.13.0) and the 4.13 tree
`code-4.13` (for `ReadOnlyLedgerHandle`, `LedgerOpenOp`, `MetadataUpdateLoop`). Interface:
`INTERFACE-agents.md` (as corrected on 2026-11-05: a recovery read succeeds on the first positive
response). Program: `java/bench/bookkeeper413/BookKeeper413.java`, class
`bench.bookkeeper413.BookKeeper413` (`MpProgram`, argument string `impl,<e>,<fifo|bag>`; `fifo` is
`P2P`, `bag` is `ASYN`). Every changed line is marked `// PORT:` in the source.

## Roles and processes

| Process | Kind | Source |
|---|---|---|
| `b1`, `b2`, `b3` | daemon, never ends | `BookieRequestProcessor` + `WriteEntryProcessorV3` + `ReadEntryProcessorV3` + `Bookie` + `HandleFactoryImpl` + `LedgerDescriptorImpl` + in-memory `LedgerStorage`. One sequential process per bookie; the storage's program order is the bookie's. |
| `zk` | daemon, never ends | `LedgerManager` over ZooKeeper: read, versioned conditional write. |
| `w1h` | daemon, never ends | w1's `LedgerHandle`: its state, the `pendingAddOps` queue, and the ledger's ordered executor (callback thread). |
| `w2h` | daemon, never ends | w2's BookKeeper client: `LedgerOpenOp`, `ReadOnlyLedgerHandle` (`recover`, `closeRecovered`), `LedgerRecoveryOp`, same queue/executor. |
| one per add in flight | daemon, ends after its last response | `PendingAddOp` (`safeRun`, `sendWriteRequest`, `writeComplete`), for w1's adds and w2's recovery adds. |
| one per recovery | daemon, ends after its last response | `ReadLastConfirmedOp` (`initiateWithFencing`, `readEntryComplete`). |
| one per recovery read | daemon, ends | `RecoveryReadOp` = `ListenerBasedPendingReadOp` with `SequenceReadRequest`s (`parallelRead` false). |
| `init` | daemon, ends | sends each of b1, b2, b3, zk, w1h, w2h one `Setup` with every address. |
| `w1`, `w2` | visible (roles), end | the interface's client loops. |

Main spawns b1, b2, b3, zk, w1h, w2h, w1, w2, then init, awaits w1 and w2, and sends nothing. It
runs under `MpProcess.runMain` (the launcher), so the daemons may stay blocked on a receive.

## Messages

Visible: `Add(long entry)`, `Recover()`, `Acked(long entry)`, `Rejected(long entry)`,
`Closed(long last)` (all `@Tag`ged with their names).

Invisible:
- `Setup(b1, b2, b3, zk, w1, w1h, w2, w2h)`.
- Bookie protocol (V3): `AddRequest(from, ledgerId, entryId, recoveryAddFlag, priority, body, ctx)`,
  `AddResponse(ledgerId, entryId, status, ctx, addr)`, `ReadRequest(from, ledgerId, entryId,
  fenceLedgerFlag, hasMasterKey, priority, ctx)`, `ReadResponse(ledgerId, entryId, status, hasBody,
  body, hasMaxLAC, maxLAC, ctx)`; `body` is `StoredEntry(ledgerId, entryId, lastAddConfirmed,
  length, data)`, the packaged entry without a digest. `ctx` is the completion's ctx (the bookie
  index), `addr` the bookie (as `PerChannelBookieClient`'s completion key supplies them).
- Metadata: `ReadLedgerMetadata(from, ledgerId, reqId)`, `WriteLedgerMetadata(from, ledgerId,
  metadata, expectedVersion, reqId)`, `MetadataResult(reqId, rc, Versioned(LedgerMetadata(state,
  lastEntryId, length), version))`.
- Op -> handle calls (each answered before the op continues): `SafeRunCall(opId)` /
  `SafeRunReturn(callbackTriggered, lastAddConfirmed)`; `SendAddSuccessCallbacksCall(opId,
  completed)`, `HandleUnrecoverableErrorDuringAddCall(from, rc)`, `ErrorOutPendingAddsCall(from,
  rc)`, `NotifyWriteFailedCall(from, index, addr)`, `HandleBookieFailureCall(from, index, addr)`,
  each answered by `Ret()`.
- Recovery callbacks: `ReadLastConfirmedDataComplete(cbId, rc, lac, length)` (RLC op -> w2h),
  `OnEntryComplete(cbId, rc, hasEntry, entryId, length, data)` (read op -> w2h).
- `Task(id)`: a handle sends itself one per task submitted to the ledger's ordered executor
  (`executeOrdered`, `whenCompleteAsync(..., chooseThread(ledgerId))`, `OrderedGenericCallback`).

## Method map

| Source (4.13) | Here |
|---|---|
| `LedgerHandle.addEntry` + `SyncAddCallback` | `ClientProcess.dispatch(Add)`: callback replies `Acked`/`Rejected` |
| `LedgerHandle.asyncAddEntry`, `asyncRecoveryAddEntry` (l.1206), `doAsyncAddEntry` (l.1296) | same names in `LedgerHandle` |
| `PendingAddOp.create`, `enableRecoveryAdd`, `setEntryId`, `setLedgerLength`, `submitCallback` (l.418) | `PendingAddOpState` (handle side) |
| `PendingAddOp.safeRun` (l.249), `sendWriteRequest` (l.149), `writeComplete` (l.286) | `PendingAddOp` process |
| `LedgerHandle.sendAddSuccessCallbacks` (l.1803), `handleUnrecoverableErrorDuringAdd` (l.1760), `errorOutPendingAdds` (l.1783/1797), `drainPendingAddsAndAdjustLength` (l.1787), `notifyWriteFailed`, `maybeHandleDelayedWriteBookieFailure`, `handleBookieFailure` (l.1864), `ensembleChangeLoop` (l.1906), `asyncCloseInternal`, `doAsyncCloseInternal` (l.541), `isHandleWritable`, `setLedgerMetadata` (l.364), `addToLength` | same names in `LedgerHandle` |
| `ReadOnlyLedgerHandle.handleUnrecoverableErrorDuringAdd`, `handleBookieFailure`, `recover`, `closeRecovered` | same names in `ReadOnlyLedgerHandle` |
| `LedgerOpenOp.initiate`, `openWithMetadata`, `openComplete` | `LedgerOpenOp` |
| `MetadataUpdateLoop.run`, `writeLoop`, `updateLocalValueFromStore`, `readLoop` | `MetadataUpdateLoop` |
| `LedgerRecoveryOp.initiate`, the RLC callback, `submitCallback`, `doRecoveryRead`, `onEntryComplete`, `addComplete` | `LedgerRecoveryOp` (`readLastConfirmedDataComplete` is the anonymous callback) |
| `ReadLastConfirmedOp.initiateWithFencing`, `readEntryComplete` | `ReadLastConfirmedOp` process |
| `PendingReadOp.initiate` (l.510), `sendReadTo` (l.566), `readEntryComplete` (l.577); `LedgerEntryRequest.complete`/`fail`/`logErrorAndReattemptRead`; `SequenceReadRequest.read`/`sendNextRead`/`logErrorAndReattemptRead`/`readsOutstanding`; `ListenerBasedPendingReadOp.submitCallback` | `RecoveryReadOp` process and its inner `SequenceReadRequest` |
| `RoundRobinDistributionSchedule` write set, `AckSetImpl`, `RRQuorumCoverageSet` | `getWriteSet`, `AckSet`, `RRQuorumCoverageSet` |
| `PerChannelBookieClient.addEntry`/`readEntry` (V3 request building), `statusCodeToExceptionCode` (l.2274) | `bookieClientAddEntry`, `bookieClientReadEntry`, `statusCodeToExceptionCode` |
| `BookieRequestProcessor.processRequest`, `processAddRequestV3`, `processReadRequestV3` | `Bookie` methods of those names |
| `WriteEntryProcessorV3.getAddResponse`, `safeRun` (addEntry vs recoveryAddEntry, l.112-115) | `Bookie.WriteEntryProcessorV3` |
| `ReadEntryProcessorV3.getReadResponse`, `readEntry`, `handleReadResultForFenceRead`, `sendFenceResponse`, `buildResponse`, `executeOp` | `Bookie.ReadEntryProcessorV3` |
| `Bookie.addEntry` (l.1389, LedgerFencedException), `recoveryAddEntry` (l.1306), `addEntryInternal`, `getLedgerForEntry`, `fenceLedger` (l.1429), `readEntry` (l.1435), `readLastAddConfirmed` (l.1461) | same names in `Bookie` |
| `HandleFactoryImpl.getHandle`, `getReadOnlyHandle`; `LedgerDescriptor.create`/`createReadOnly`; `LedgerDescriptorImpl.setFenced`, `isFenced`, `fenceAndLogInJournal`, `logFenceEntryInJournal`, `addEntry`, `readEntry`, `getLastAddConfirmed` | same names |
| `LedgerStorage` (interface) | `InMemoryLedgerStorage` |

## Decisions (where the source or the interface leaves something open)

1. **Logical tasks.** In 4.13 everything a client does for one ledger runs on the ledger's ordered
   worker thread (`getMainWorkerPool().executeOrdered(ledgerId, ...)`), the bookie client's
   completions included. Per the owner's contract, each `PendingAddOp`, the `ReadLastConfirmedOp`
   and each `RecoveryReadOp` are separate processes; the handle (its synchronized state and
   `pendingAddOps`) is the sequential process w1h/w2h. A call from an op into `lh` is a request
   followed by a blocking receive of its return, so the op's program order is kept under `bag` too.
   The terminal callbacks of the read ops (`readLastConfirmedDataComplete`, `onEntryComplete`) are
   plain messages: nothing in their process follows that the handle could observe.
2. **`PendingAddOp.safeRun` (l.249-283).** Its reads of handle state (`callbackTriggered`,
   `lh.lastAddConfirmed`) and the call `lh.maybeHandleDelayedWriteBookieFailure()` are done in one
   `SafeRunCall` at the handle, atomically, as on the ordered thread. `hasRun` is set on both sides.
3. **Ordered-executor tasks are `Task` messages to self**, received among all other messages:
   `doAsyncCloseInternal`'s task (so op responses already delivered can still be acknowledged
   before the close task drains the queue), the closed-ledger callback in `doAsyncAddEntry`, the
   `whenCompleteAsync` of `ensembleChangeLoop`, and `LedgerOpenOp`'s `OrderedGenericCallback`.
   Self-messages keep the submission order of these tasks only under `fifo` (P2P, one channel
   from the handle to itself). Under `bag` they may be received in another order, so this is not a
   general implementation of an ordered executor. In this bounded program the tasks that can
   compete are the writer's close tasks, and they perform the same drain/close (the second finds
   the handle CLOSED); no distinct visible outcome from their permutation was found (Astra review,
   finding 11). No general claim of executor-order preservation is made.
4. **`completed`** is a field of the op process; the handle's queue needs it in
   `sendAddSuccessCallbacks`. The op sends it with every `SendAddSuccessCallbacksCall` (the op calls
   `lh.sendAddSuccessCallbacks()` only right after `completed` became or was true, l.330, l.409).
5. **No ensemble change** (interface): `EnsembleUtils.replaceBookiesInEnsemble` finds no spare
   bookie, so `ensembleChangeLoop`'s transform throws `BKNotEnoughBookiesException` and its
   completion calls `handleUnrecoverableErrorDuringAdd`. `changingEnsemble` stays true afterwards,
   as in the source (it is reset only on success). This path is reachable: a `Fenced` answer that
   arrives after an op `completed` goes to `notifyWriteFailed` (l.312, `delayEnsembleChange` false
   by default), and the next op's `safeRun` then starts this loop (and still sends its writes).
   The success branch would need a new bookie; it throws `AssertionError` (unreachable).
   `ReadOnlyLedgerHandle.handleBookieFailure` likewise ends in `handleUnrecoverableErrorDuringAdd`.
6. **Recovery reads, 4.13 behaviour.** `sendReadTo` (l.571) sends `FLAG_HIGH_PRIORITY` only, no
   `FLAG_DO_FENCING`: recovery reads do not fence. `RecoveryReadOp` does not set `parallelRead`, so
   reads are `SequenceReadRequest`s: one bookie at a time in write-set order; the read **completes
   on the first positive response** (`LedgerEntryRequest.complete`, l.133); it fails with
   NoSuchEntry once `Qw - Qa + 1 = 2` bookies answered NoSuchEntry/NoSuchLedger (l.428). The
   coordinator's correction (INTERFACE.md had said two responses) did not affect this code: it was
   written from the source from the start.
7. **Configuration.** Checked against the 4.13 configuration sources fetched to
   `plan/case-studies/bookkeeper/bk-src/conf-4.13` (`ClientConfiguration.java`,
   `AbstractConfiguration.java`). Each setting the program fixes:
   - `recoveryReadBatchSize` = 1: verified at ClientConfiguration.java l.1104 (default 1; the
     interface also fixes batch size 1).
   - `reorderReadSequenceEnabled` = false: verified at ClientConfiguration.java l.1125.
   - `stickyReadsEnabled` = false: verified at ClientConfiguration.java l.1155.
   - `delayEnsembleChange` = false: verified at ClientConfiguration.java l.1781 (getter l.1780).
   - The ensemble-change feature is not disabled: the feature name defaults at
     ClientConfiguration.java l.1870, and its availability comes from the feature provider, which
     `BookKeeper` sets to `SettableFeatureProvider.DISABLE_ALL` when none is given (code-4.13
     `client/BookKeeper.java` l.421-422). That DISABLE_ALL reports the feature unavailable is read
     off its name: `SettableFeatureProvider` is not in the available sources. So: verified as far
     as the default provider goes.
   - `maxAllowedEnsembleChanges` = `Integer.MAX_VALUE`: verified at ClientConfiguration.java l.1891.
   - `enforceMinNumFaultDomainsForWrite` = false: verified at AbstractConfiguration.java l.953.
   - `waitTimeoutOnBackpressureMillis` (`waitForWriteSetMs`) = -1, so no `waitForWritable`:
     verified at ClientConfiguration.java l.820.
   - `addEntryQuorumTimeout` = -1 (no quorum timeout): verified at ClientConfiguration.java l.636.
     The per-request add/read timeouts (l.612-613, l.660-661, default the read timeout) are
     dropped: benchmark configuration (no timeouts, interface), not a default.
   - No speculative reads: benchmark configuration, not a default. The default
     `speculativeReadTimeout` is 2000 ms (l.912) and `firstSpeculativeReadTimeout` defaults to it
     (l.937), so 4.13 does install a speculative read policy by default (code-4.13
     `ClientInternalConf.java` l.86-90). Speculative reads are timer-driven and the interface has no
     timers.
   - `enableParallelRecoveryRead` = false: verified at ClientConfiguration.java l.1083 (in 4.13
     `RecoveryReadOp` does not call `parallelRead` either).
   - `enableBookieFailureTracking` defaults to true (l.1828); not modelled (benchmark
     configuration). With read reordering off it only feeds bookie-health bookkeeping, not the
     replica order.
   - High-priority bookie threads present (a fence read answers through `fenceThreadPool`):
     benchmark configuration, not verified: it is a server setting
     (`ServerConfiguration`), not in the fetched files. With the in-memory journal both paths of
     `handleReadResultForFenceRead` send the same response.
8. **In-memory bookie storage.** `getHandle` creates the ledger (`setMasterKey`); `readEntry` of
   `LAST_ADD_CONFIRMED` (-1) returns the stored entry with the highest id (as
   `InterleavedLedgerStorage` via `getLastEntry`), else NoEntry; `getLastAddConfirmed` is the
   largest LAC piggybacked on a stored entry. The journal completes at once: an add's response is
   sent inside `addEntryInternal`, the fence future is done when `fenceLedger` returns. Fencing a
   ledger the bookie has not seen creates it, so a fence read there answers ENOENTRY (not
   ENOLEDGER). A fence read with entries answers EOK with the last entry: the client's LAC is the
   LAC that entry carried (`verifyDigestAndReturnLastConfirmed`), as in the source.
9. **Bookie threads.** The write, read and high-priority pools are collapsed into the bookie's
   one sequential process. This is the owner's contract (a bookie's request handling and its
   ledger storage are a sequential process), not a property of the source: in the source only
   parts are synchronized (`addEntry`'s fence check and store under `synchronized (handle)`,
   `setFenced`); the fence/read/reply path of `ReadEntryProcessorV3` is not one synchronized
   operation and can interleave with other requests on other pool threads. Every channel to a
   bookie carries at most one request at a time (each op/read process sends one request per
   bookie at a time), so the collapse loses no reordering within a channel.
10. **zk.** The ledger exists from the start, `Open`, last entry -1, length 0, version 0, and w1h
    starts with that `Versioned` as `asyncCreateLedger` returns it. A write succeeds iff its
    expected version is the current one (then version + 1); otherwise
    `MetadataVersionException`. `LongVersion.compare` of equal versions is taken as
    `CONCURRENTLY` (used in `readLoop`). The ensemble is not in the metadata (fixed).
11. **w1 can close the ledger.** On a fenced write w1h's local metadata is still `Open`, so
    `handleUnrecoverableErrorDuringAdd` closes: the close's `MetadataUpdateLoop` conflicts with w2's
    `InRecovery` write, re-reads, and, if w2 has not closed yet, writes `Closed` with w1's LAC; w2's
    `closeRecovered` then conflicts, re-reads `Closed` and accepts it (`setLedgerMetadata` sets
    w2h's LAC to it). Kept as in the source.
12. **Entry numbers.** BookKeeper's entry ids start at 0 (`++lastAddPushed` from -1); w1's entries
    are 1..e. The payload of entry id k is w1's number k + 1; `Acked`/`Rejected` carry w1's number
    (the add's ctx). `Closed { last }` carries `lh.getLastAddConfirmed()` mapped to w1's numbering:
    `last = lastEntryId + 1`, and -1 when the ledger is empty. This is the convention CONTRACT.md
    fixes (Closed = id + 1, -1 when empty).
13. **w2's "nondeterministic moment".** No `Nondet` choice: w2 sends `Recover` at once; when the
    recovery happens relative to w1's writes is the exploration's choice of the order in which
    bookies, zk and handles receive their messages, which is the only thing a moment can change in
    this program.
14. **Failed open.** If the recovery failed, `LedgerOpenOp.openComplete` would report an error,
    which the interface has no message for: w2h sends nothing and w2 stays blocked (counted as
    blocked). Not expected to be reachable (no failures, recovery adds are never fenced).
15. **Not modelled** (interface): digests and master keys (always valid), timeouts
    (`maybeTimeout`), speculative reads, bookie health and slow-bookie tracking, explicit LAC,
    stats, recycling (`maybeRecycle`), the `RejectedExecutionException` paths, read-only bookies,
    `isVersionCompatible`. The entry payload length is 1.
16. **Observation only.** w1 and w2 record locally the largest `Acked` entry and the `Closed`
    value; main counts iterations where an acknowledged entry lies above the closed last entry
    (Defect 1) and the launcher prints the count through `toString()`. No message is added.
17. `size()` is `"e=<e>"`, `programName()` is `"bookkeeper"`.

Defect 1 is not fixed: recovery reads go without fencing, so a bookie not fenced by the
`Qw - Qa + 1` LAC responses can still accept w1's write after the recovery read passed it.

### Recorded witness of Defect 1 at e = 1 (Astra's schedule, hand-derived, not replayed)

My hand derivation said b2 answers the recovery read with NoSuchEntry; that was wrong: b2 has
never created the ledger at that point (no add, no fence reached it), so it answers
**NoSuchLedger** (ENOLEDGER), which counts as missing in the same way. The schedule:

1. Admit `Add(1)` and initiate its three ordinary writes, carrying entry id 0, LAC -1.
2. Mark the metadata `IN_RECOVERY` and issue all fencing LAC requests.
3. Fence empty b1. Store entry 0 at b3, then fence b3. Their LAC responses both yield -1.
4. Keep b2's fencing request and ordinary add pending.
5. Read entry 0 from b1, receiving NoSuchEntry; then read b2, receiving **NoSuchLedger** because
   b2 has not yet created the ledger.
6. Those two misses finish recovery; commit the empty closed metadata and deliver `Closed(-1)`.
7. Deliver the ordinary add to the still unfenced b2. Process its success and b3's success at the
   add operation, then deliver `Acked(1)`.
8. Deliver the remaining requests and responses afterwards; in particular, delay b1's fencing
   error response to the add until after the acknowledgement.

Visible result: `Acked(1)` with `Closed(-1)`. The schedule respects `fifo`, which is FIFO per
process pair: b2's fence (from the ReadLastConfirmedOp), its recovery read (from the
RecoveryReadOp) and its add (from the PendingAddOp) come from three different sender processes,
so `fifo` does not order b2's fence ahead of its recovery read or its add. The two 900 s runs did
not reach this schedule (their explorations were partial, see below).

## Compile (inside this directory)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Result: compiles with no errors or warnings.

## Runs (inside this directory)

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper413.BookKeeper413:impl,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper413.BookKeeper413:impl,2,fifo' must 0 1 report - 900 </dev/null
```

Last lines, `impl,1,fifo` (2026-11-05, shared machine, load average about 45 on 16 cores):

```
ran:        3131 iterations
blocked:    0 (counted by the strategy)
completed:  3131
rate:       3.5 iterations/s
error:      Halting execution: 3131 due to timeout
elapsed:    900.768 s
observed:   BookKeeper413(impl,1,fifo): 3131 iterations observed, 0 with an entry Acked above the Closed last (Defect 1)
graphs:     3131 distinct complete, 0 distinct blocked; stack not exhausted
graphs_complete: 3131
graphs_blocked: 0
stop_reason: timeout
exit 0
```

Last lines, `impl,2,fifo` (2026-11-05, shared machine, load average about 45 on 16 cores):

```
ran:        3516 iterations
blocked:    0 (counted by the strategy)
completed:  3516
rate:       3.9 iterations/s
error:      Halting execution: 3516 due to timeout
elapsed:    900.677 s
observed:   BookKeeper413(impl,2,fifo): 3516 iterations observed, 0 with an entry Acked above the Closed last (Defect 1)
graphs:     3516 distinct complete, 0 distinct blocked; stack not exhausted
graphs_complete: 3516
graphs_blocked: 0
stop_reason: timeout
exit 0
```

Neither run exhausted the exploration stack within 900 s: they are partial (timeout), and no explored graph witnessed Defect 1 among the first ~3 100 / ~3 500 graphs. No exception or assertion in either log. A process fault would show as FAILED.
