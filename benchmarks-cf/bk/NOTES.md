# BookKeeper (main 7b66b5d) ledger replication with fencing: Java transliteration over JMC's message-passing API

Source: `plan/case-studies/bookkeeper/bk-src/main` (the cut) and the version's full tree `plan/case-studies/bookkeeper/code`
(read for LedgerOpenOp, ReadOnlyLedgerHandle, MetadataUpdateLoop, ReadOpBase, CompletionValue, InterleavedLedgerStorage,
EnsembleUtils, LedgerMetadataImpl, BKException codes). Program: `java/bench/bookkeeper/BookKeeper.java`, class
`bench.bookkeeper.BookKeeper`, constructed from `impl,<e>,<fifo|bag>`. Every changed line is marked `// PORT:`.

## Roles and processes

| process | kind | source |
|---|---|---|
| `w1` | visible, spawned with role `w1`, awaited | the interface's writer loop: `Add(i)` for i = 1..e, waits for `Acked`/`Rejected` |
| `w2` | visible, role `w2`, awaited | the interface's recoverer: `Recover`, waits for `Closed` |
| `b1`,`b2`,`b3` | daemon | `BookieRequestProcessor` → `WriteEntryProcessorV3` / `ReadEntryProcessorV3` over `BookieImpl` and an in-memory `LedgerStorage` |
| `zk` | daemon | `LedgerManager` over ZooKeeper, as a versioned store of the ledger's metadata |
| `w1h` | daemon | `LedgerHandle` of w1: the handle state, its pending-add queue, and its ordered executor (lh.executor) |
| `w2h` | daemon | `ReadOnlyLedgerHandle` of w2 opened with recovery (`LedgerOpenOp`), with its `LedgerRecoveryOp` |
| PendingAddOpTask × e (w1h), × e (w2h) | daemon | a `PendingAddOp` in flight: receives the bookies' answers and queues each on its handle's executor (`WriteCompleteCallback`), one per entry id; no handle effects of its own (fix round 1) |
| ReadLastConfirmedOp × 1 (w2h) | daemon | the fencing LAC read (`initiateWithFencing`, `readEntryComplete`) |
| RecoveryReadOp × (e+1) (w2h) | daemon | `LedgerRecoveryOp.RecoveryReadOp` = `ListenerBasedPendingReadOp`/`PendingReadOp` with one `SequenceReadRequest`, one per entry id |
| init | daemon | sends `Setup(client)` to w1h and w2h (the clients' addresses) |

Main spawns, in this order: b1, b2, b3, zk, w1h's add ops, the LAC-read op, w2h's read ops, w2h's add ops, w1h, w2h, w1,
w2, init. Then it awaits w1 and w2 and sends or receives nothing. `MpLauncher` runs `run()` under `MpProcess.runMain`,
so the daemons run until none can move. `rolesInSpawnOrder()` = `visibleRoles()` = `[w1, w2]`: daemons carry no role.

## Messages

Visible, with their shapes fixed by the interface (`@Tag` = the variant name): `Add(long entry)`, `Recover()`, `Acked(long entry)`,
`Rejected(long entry)`, `Closed(long last)`.

Invisible:
- client ↔ bookie: `AddRequest(replyTo, ledgerId, entryId, flags, body, ctx=bookieIndex)`, `AddResponse(status, ledgerId, entryId, addr, ctx)`,
  `ReadRequest(replyTo, ledgerId, entryId, flags, hasMasterKey, ctx)`, `ReadResponse(status, ledgerId, entryId, body|null, hasMaxLAC, maxLAC, addr, ctx)`.
  `body` = `EntryBody(ledgerId, entryId, lastAddConfirmed, length, data)`, the packaged entry (computeDigestAndPackageForSending without a digest).
  Statuses are the V3 `StatusCode`; the client maps them with `CompletionValue.convertStatus` (default WriteException for adds and ReadException for reads).
- handle ↔ zk: `ReadLedgerMetadata`/`ReadLedgerMetadataResult`, `WriteLedgerMetadata(metadata, currentVersion)`/`WriteLedgerMetadataResult(OK | MetadataVersionException)`.
- handle → op: `PendingAddOpCreate(lh, entryId, isRecoveryAdd, pendingWriteRequests)`, `ReadLastConfirmedOpCreate(lh)`, `RecoveryReadOpCreate(lh, start, end)`.
- op → handle: `WriteCompleteCallback(opEntryId, status, ledgerId, entryId, addr, ctx)`, one per bookie answer (fix round 1),
  `ReadLastConfirmedDataComplete(rc, lac, length)`, `OnEntryComplete(rc, hasEntry, entryId, length, data)`.
- handle → itself: `OrderedTask(seq)`, a task on the handle's ordered executor. The guarded receive accepts only the next seq, so tasks run in queue order under `bag` too.
- init → handle: `Setup(client)`.

## Method mapping

| BookKeeper (main) | here |
|---|---|
| LedgerHandle.asyncAddEntry / asyncRecoveryAddEntry / doAsyncAddEntry (l.1716) | `LedgerHandle.asyncAddEntry`, `asyncRecoveryAddEntry`, `doAsyncAddEntry` |
| PendingAddOp.create / initiate / sendWriteRequest (l.146) | `PendingAddOpCreate` + `LedgerHandle.initiate` (on the handle) |
| PendingAddOp.writeComplete (l.278), completeBookieAndCheck | `PendingAddOp.writeComplete`, `completeBookieAndCheck` (the handle-side `PendingAddOp`, one ordered executor turn per answer) |
| PendingAddOp.submitCallback | `PendingAddOp.submitCallback` (on the handle) |
| LedgerHandle.sendAddSuccessCallbacks (l.2225) | `LedgerHandle.sendAddSuccessCallbacks` |
| handleUnrecoverableErrorDuringAdd (l.2180), errorOutPendingAdds (l.2205), drainPendingAddsAndAdjustLength | same names |
| asyncCloseInternal / doAsyncCloseInternal + MetadataUpdateLoop | same names; `LedgerHandle.MetadataUpdateLoop` (writeLoop, readLoop) |
| notifyWriteFailed, maybeHandleDelayedWriteBookieFailure, handleBookieFailure (l.2293), ensembleChangeLoop (l.2339) | same names |
| LedgerOpenOp.initiate / openWithMetadata, ReadOnlyLedgerHandle.recover / closeRecovered / handleUnrecoverableErrorDuringAdd / handleBookieFailure | `ReadOnlyLedgerHandle.LedgerOpenOp.initiate`, `openWithMetadata`, `recover`, `closeRecovered`, `recoveryComplete`, overrides |
| LedgerRecoveryOp.initiate / readLastConfirmedDataComplete / doRecoveryRead / onEntryComplete / addComplete / submitCallback | `LedgerRecoveryOp` (same names; `addComplete` is `addCompleteWithLatency`) |
| ReadLastConfirmedOp.initiateWithFencing / readEntryComplete, RRQuorumCoverageSet.checkCovered | `ReadLastConfirmedOp` (same names) |
| PendingReadOp.initiate / readEntryComplete / sendReadTo, ListenerBasedPendingReadOp.submitCallback, SequenceReadRequest.read / sendNextRead / logErrorAndReattemptRead / complete, LedgerEntryRequest.fail | `RecoveryReadOp` and `RecoveryReadOp.SequenceReadRequest` (same names) |
| BookieRequestProcessor.processAddRequestV3 / processReadRequestV3 | `BookieRequestProcessor` (same names) |
| WriteEntryProcessorV3.getAddResponse / run (l.112-115 addEntry vs recoveryAddEntry) | `WriteEntryProcessorV3` |
| ReadEntryProcessorV3.getReadResponse / readEntry / handleReadResultForFenceRead / sendFenceResponse / buildResponse | `ReadEntryProcessorV3` |
| BookieImpl.addEntry (l.1112) / recoveryAddEntry (l.1024) / fenceLedger (l.1153) / readEntry (l.1159) / readLastAddConfirmed (l.1187) | `BookieImpl` (same names) |
| LedgerStorage (InterleavedLedgerStorage semantics: getEntry(-1) = last entry; getLastAddConfirmed) | `LedgerStorage` (in memory) |
| LedgerManager (ZooKeeper) | `LedgerManager` process `zk` |

## Decisions (where the source leaves something open or the interface replaces it)

1. **Logical tasks.** Each PendingAddOp in flight, each recovery read, and the fencing LAC read is its own process. Each bookie is one process: its request handling over its ledger storage. Each handle is one process: the handle's state, its pending-add queue and its ordered executor. In the source (LedgerHandle constructor, `executor = mainWorkerPool.chooseThread(ledgerId)`), that executor runs every callback of the handle. Initiation runs where it reads or changes state:
   - `PendingAddOp.initiate` runs on the handle, because it reads `lh.lastAddConfirmed` and calls `lh.maybeHandleDelayedWriteBookieFailure()`. The source calls it synchronously from `doAsyncAddEntry`.
   - `ReadLastConfirmedOp.initiateWithFencing` and `PendingReadOp.initiate` only send, so they run on the op's process after its creation message. The source calls them on the creator's thread right after construction.
2. **`completed` flag** (superseded by decision 18, fix round 1). The `completed` flag is the PendingAddOp's own field on the handle. `writeComplete` sets it and calls `sendAddSuccessCallbacks` in the same executor turn, as the source does. The earlier travelling flag (`SendAddSuccessCallbacks`) is removed.
3. **The zk client's callbacks** (MetadataUpdateLoop's write and read completions) run on the handle's process. The handle state they touch is synchronized in the source (`setLedgerMetadata`), so running them there serialises them, as the source does.
4. **executeOrdered** queues the task locally and sends the handle an `OrderedTask(seq)`. The handle's receive admits only the next seq, so tasks run in FIFO order whatever the delivery model. Since fix round 1, every callback the source runs on lh.executor goes through it (decision 18).
5. **Addresses.** Bookies and zk answer the `replyTo` carried by each request. Ops learn their handle from their creation message. The handles learn their client from `Setup`, sent by an invisible `init` daemon (as the Rust entry's `init`). The spawn order cannot break the handle↔client cycle any other way.
6. **Entry ids.** BookKeeper's ids start at 0: `Add(i)` is entry id `i-1` (`++lastAddPushed` from -1). The payload is `i` and the callback context is `i`, so `Acked(i)`/`Rejected(i)` echo the Add. `Closed(last)` carries the interface's number of the source's last entry id: `lh.getLastAddConfirmed()` after `setLedgerMetadata` of the closed metadata, plus one, and -1 when empty (the specification and the ports number entries from 1; coordinator's fix of 2026-11-06 at `openComplete`). So with e = 1 and the entry recovered, `last = 1`.
7. **Ledger length.** Every payload is 1 unit, so the ledger length counts entries. It is kept because the close path compares it (`doAsyncCloseInternal`'s conflict check), although no visible message depends on it.
8. **Fixed ensemble, no spare bookie.**
   - `EnsembleUtils.replaceBookiesInEnsemble` always throws BKNotEnoughBookiesException, so the ensemble-change MetadataUpdateLoop's transform throws. Its `whenCompleteAsync(executor)` then runs `handleUnrecoverableErrorDuringAdd(NotEnoughBookiesException)`, which closes the handle.
   - Before that, `handleBookieFailure` sets `changingEnsemble = true`, and nothing resets it on failure, so `sendAddSuccessCallbacks` stops for good (source l.2321-2324, l.2231).
   - This path is reached when an entry is completed and a later answer is an error (Fenced): `notifyWriteFailed`, then the next add's `initiate`.
   - Defaults assumed: `delayEnsembleChange = false`, `disableEnsembleChangeFeature` unavailable, `maxAllowedEnsembleChanges` not reached. ClientConfiguration is not in the tree.
9. **The writer's close is the source's.**
   - `doAsyncCloseInternal` runs a MetadataUpdateLoop that writes `Closed(lac)` with w1h's local version. On a conflict it re-reads zk.
   - If zk shows InRecovery, the predicate `!metadata.isClosed()` holds and w1h writes `Closed(its LAC)` over the recovery's InRecovery.
   - w2h's `closeRecovered` then conflicts, re-reads, finds Closed (needsUpdate is `state == IN_RECOVERY`, false), and completes with w1h's last entry id. `Closed{last}` is then w1h's LAC.
   - All of this is the code's behaviour (LedgerHandle l.576-668, ReadOnlyLedgerHandle.closeRecovered, MetadataUpdateLoop). Nothing is added.
   - w1h's `handleUnrecoverableErrorDuringAdd` checks its *local* metadata state. That state becomes IN_RECOVERY only if w1h's own read loop read it.
10. **Recovery reads complete on the first positive answer.** `SequenceReadRequest.complete`, called from `PendingReadOp.readEntryComplete`, completes the entry on the first EOK. The read is sequential: replica 0 of the write set (entry id mod 3) first; after an error, the next replica (`logErrorAndReattemptRead`, `sendNextRead`). It ends with NoSuchEntry once Qw - Qa + 1 = 2 bookies answered NoSuchEntry/NoSuchLedger (l.411). At main the read is sent with `FLAG_HIGH_PRIORITY | FLAG_DO_FENCING` (`sendReadTo`, l.181-182), so it fences the bookie that answers it. The earlier INTERFACE text ("succeeds when Qa = 2 bookies return the entry") was never followed. The coordinator's correction of 2026-11-05 confirms the code's rule, which is what is implemented.
11. **No speculative reads, timeouts, reordered or sticky reads, throttling, or explicit LAC.** They are timer-driven or off by default. `waitForWriteSetMs`: every channel is writable.
12. **Bookie storage** is in memory, following InterleavedLedgerStorage:
   - `getEntry(-1)` returns the highest stored entry. NoEntryException when there is none; NoLedgerException when the ledger was never written or fenced (fencing creates it, as `getHandle`/`setFenced` do).
   - The bookie LAC is the maximum LAC carried by the entries added (`updateLastAddConfirmed`).
   - The journal (the add and fence journal writes) completes at once. The fence future is therefore complete when `handleReadResultForFenceRead` runs, so the fence pool and the 1 s wait collapse to the value.
   - Authorisation always succeeds. No master-key cache. The request thread pools are not modelled: a bookie handles requests one at a time, in receive order.
13. **zk** holds `Versioned(LedgerMetadata(state, lastEntryId, length), version)`, starting `Open, -1, 0` at version 0 (LedgerCreateOp is not in the cut; w1h starts with that version). A write succeeds iff its version is current (new version + 1); otherwise MetadataVersionException. `Version.compare == CONCURRENTLY` is read as equal versions (LongVersion; not in the tree).
14. **Open failure.** If the open/recovery fails (unreachable here), the application's open callback gets an error. The interface defines no message for that, so w2h sends none and w2 stays blocked, which would show as a blocked graph. The client loops are the interface's, verbatim. w2's "nondeterministic moment" is the scheduler's: nothing is chosen explicitly.
15. **Op pools.** Op processes are pre-spawned per entry id, never reused:
   - w1h: e add ops (ids 0..e-1);
   - w2h: e+1 read ops (ids LAC+1..e, LAC ≥ -1) and e recovery-add ops.
   An unused op stays blocked on its creation message. A daemon blocked on a receive does not make the execution blocked.
16. **Program name** `bookkeeper`; size = `e` as text. These must match the Rust side for a summary comparison; to be checked.
17. **Delivery model**: `fifo` = `CommunicationModel.P2P`, `bag` = `ASYN`.

18. **The ordered executor carries every executor callback (fix round 1, Astra REVIEW-main findings 1-2).**
   Source: PendingAddOp l.146-152 gives the bookie client `lh.executor`, and PerChannelBookieClient l.1399-1403 and l.1542-1546 queue the response handling on it. The fencing LAC read (`callbackExecutor = lh.executor`), the recovery reads (`sendReadTo(..., lh.executor)`) and the close tasks (`executeOrdered`, LedgerHandle l.2538-2539) use the same executor.
   Now:
   - The op's process only receives the bookie answers and forwards each one as a `WriteCompleteCallback`. It has no handle effects.
   - On arrival, the handle queues `PendingAddOp.writeComplete` for that answer as one `OrderedTask`. That turn includes its synchronous effects: `sendAddSuccessCallbacks`, `notifyWriteFailed`, `handleBookieFailure`, and `handleUnrecoverableErrorDuringAdd` together with the close task it queues.
   - The completions of the LAC-read op and the read ops (`readLastConfirmedDataComplete`, `onEntryComplete`, with the recovery adds and reads they issue) are queued the same way.
   - The handle drains the queue strictly in arrival order (decision 4).
   What this guarantees: a callback runs only after every task queued before it. A close task queued by a Fenced answer therefore runs before any callback of an add issued later. In Astra's e=2 counterexample, Acked(2) can no longer precede the queued close.
   The application's calls (Add, Recover) and the zk completions (decision 3) are not executor tasks in the source, so they are handled on arrival.
   The op-local and read-op-local processing that precedes the forward touches only that op's state. Queueing its effect when the forward arrives is the same as the bookie's answer arriving at that moment.

## Compile and run (inside OUT_DIR = daavar/benchmarks-cf2/bk/java)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper.BookKeeper:impl,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper.BookKeeper:impl,2,fifo' must 0 1 report - 900 </dev/null
```

Compile: no errors, no warnings (JDK 17.0.18).

## Runs (last output lines)

2026-11-05, machine: local workstation, JDK 17.0.18, seed 1, timeout 900 s. Neither run exhausted the stack.
They hit the timeout with no failure and no blocked graph.

`impl,1,fifo`:
```
ran:        3643 iterations
blocked:    0 (counted by the strategy)
completed:  3643
rate:       4.0 iterations/s
error:      Halting execution: 3643 due to timeout
elapsed:    900.878 s
graphs:     3643 distinct complete, 0 distinct blocked; stack not exhausted
stop_reason: timeout
peak_rss_kb: 661064
max_graph_events: 120
mean_graph_events: 107.808
```

`impl,2,fifo`:
```
ran:        2700 iterations
blocked:    0 (counted by the strategy)
completed:  2700
rate:       3.0 iterations/s
error:      Halting execution: 2700 due to timeout
elapsed:    900.078 s
graphs:     2700 distinct complete, 0 distinct blocked; stack not exhausted
stop_reason: timeout
peak_rss_kb: 679984
max_graph_events: 150
mean_graph_events: 147.999
```


### After fix round 1 (decision 18), 2026-11-06, seed 1

The earlier runs above are of the pre-fix program. Logs: `run-e1-fix1.log` and `run-e2-fix1.log` in OUT_DIR. The launcher deletes `report/`, so the logs are kept outside it.

`impl,1,fifo`, `must 0 1 report - 900`:
```
ran:        20748 iterations
blocked:    0 (counted by the strategy)
completed:  20748
rate:       23.1 iterations/s
error:      Halting execution: 20748 due to timeout
elapsed:    900.059 s
graphs:     20748 distinct complete, 0 distinct blocked; stack not exhausted
stop_reason: timeout
peak_rss_kb: 918448
max_graph_events: 124
mean_graph_events: 124.000
```

`impl,2,fifo`, `must 0 1 report - 300` (bounded at 300 s):
```
ran:        3595 iterations
blocked:    0 (counted by the strategy)
completed:  3595
rate:       12.0 iterations/s
error:      Halting execution: 3595 due to timeout
elapsed:    300.100 s
graphs:     3595 distinct complete, 0 distinct blocked; stack not exhausted
stop_reason: timeout
peak_rss_kb: 568124
max_graph_events: 178
mean_graph_events: 176.053
```
