# bk-port: BookKeeper ledger replication with fencing, ported to JMC's message-passing API

Source: apache/bookkeeper main 7b66b5d (cut in plan/case-studies/bookkeeper/bk-src/main; paths below relative to
it). Interface: plan/case-studies/bookkeeper/INTERFACE-agents.md. Program: `java/bench/bookkeeper_port/BookKeeperPort.java`
(class `bench.bookkeeper_port.BookKeeperPort`, args `impl,<e>,<fifo|bag>`, programName `bk-port`, size `<e>`, visible
roles `w1`, `w2`). Lines that differ from the source carry `// PORT:`.

## Processes (spawn order)

| process | kind | what it is |
|---|---|---|
| b1, b2, b3 | daemon | `BookieImpl` (bookie index 0, 1, 2) with `WriteEntryProcessorV3` and `ReadEntryProcessorV3` over in-memory ledger storage |
| zk | daemon | `MetadataStore`: the ledger's metadata (state OPEN / IN_RECOVERY / CLOSED, last entry id) with a version |
| w1h | daemon | `LedgerHandle` of the writer (doAsyncAddEntry, PendingAddOp, sendAddSuccessCallbacks, close on fence) |
| w2h | daemon | `LedgerHandle` of the recovery client (open, mark IN_RECOVERY, LedgerRecoveryOp, close) |
| w1 | visible `spawn("w1")` | sends `Add(i)`, i = 1..e, to w1h, waits for `Acked`/`Rejected` before the next |
| w2 | visible `spawn("w2")` | sends `Recover` to w2h, waits for `Closed` |
| init | daemon | sends one `Setup` (the Tids of b1..b3, zk, w1h, w2h, w1, w2) to each of b1, b2, b3, zk, w1h, w2h |

Main spawns these in this order, then awaits w1 and w2; it sends and receives nothing. Every internal process first
receives its `Setup` (guarded receive), then loops forever on guarded receives.

## Messages

Visible (fixed): `Add(long entry)`, `Recover()`, `Acked(long entry)`, `Rejected(long entry)`, `Closed(long last)`.
`Add(i)` is entry id i-1; `Acked`/`Rejected` carry the i of the `Add` they answer; `Closed(last)` = last entry id + 1,
-1 when the ledger is closed empty.

Invisible (all tagged records):
- `Setup(Tid b1, b2, b3, zk, w1h, w2h, w1, w2)`: init -> internal processes.
- `AddRequest(Tid from, long entryId, long lastAddConfirmed, long data, boolean recoveryAdd)`: handle -> bookie
  (`PendingAddOp.sendWriteRequest`; `recoveryAdd` = FLAG_RECOVERY_ADD; `lastAddConfirmed` = the LAC piggybacked by
  `computeDigestAndPackageForSending`; `data` = the entry's payload, the writer uses the entry number i).
- `AddResponse(int bookieIndex, long entryId, StatusCode status)`: bookie -> handle, EOK or EFENCED.
- `ReadRequest(Tid from, long entryId, boolean doFencing)`: handle -> bookie; entryId -1 = LAST_ADD_CONFIRMED (the
  fencing LAC read of ReadLastConfirmedOp), entryId >= 0 = a recovery read (FLAG_DO_FENCING set, as at main).
- `ReadResponse(int bookieIndex, long entryId, StatusCode status, long bodyEntryId, long bodyLac, long data, long maxLAC)`:
  bookie -> handle; status EOK / ENOENTRY / ENOLEDGER; the body is the stored entry (id, piggybacked LAC, data); -1 where absent.
- `ReadMeta(Tid from)` / `MetaValue(LedgerMetadata, long version)`; `WriteMeta(Tid from, LedgerMetadata, long
  expectedVersion)` / `MetaWritten(long version)` or `MetaBadVersion()`: handle <-> zk.
- `LedgerMetadata(State state, long lastEntryId)` (a field of the zk messages).
`from` is the Tid of the handle a bookie or zk answers to (the source's netty channel; MpMessage's contract: process
references are Tids; review round 2). Bookie indices stay ensemble positions.
- `ExecutorTask(long seq, String kind)`: a handle -> itself; appends task `seq` to the handle's ordered executor (D10).

## Protocol, role by role

- **w1h**: one receive loop that takes `Add`, every `AddResponse`, every zk answer and its own `ExecutorTask`s (D10, D11); response callbacks and the close run as executor tasks. `Add` -> `doAsyncAddEntry`:
  if `isHandleWritable()`, entry id = ++lastAddPushed, queue, `PendingAddOp.initiate` (AddRequest to the write set
  (id+k)%3, k = 0..2, carrying lastAddConfirmed); else `Rejected` via the ordered executor. `writeComplete`: EOK ->
  ack set, completed at 2, `sendAddSuccessCallbacks` (in entry order, LAC advances, `Acked`); EFENCED ->
  `handleUnrecoverableErrorDuringAdd` -> (local state not IN_RECOVERY) `asyncCloseInternal` -> `doAsyncCloseInternal`
  (ordered): drain pending adds, lastEntry = LAC, handle CLOSED, `Rejected` for every drained add, then (first close
  only) the metadata update loop at zk: closed with the same last entry -> nothing, closed otherwise -> conflict (the
  close fails silently, NoopCloseCallback), not closed -> write CLOSED(lastEntry) conditioned on the version; on a
  bad version re-read and test again. The update is started and not awaited: while it is outstanding w1h keeps
  receiving, rejects later `Add`s at once (`doAsyncAddEntry`, handle CLOSED) and consumes late add responses.
- **w2h**: one receive loop that takes `Recover` when idle, every `AddResponse`, `ReadResponse` and zk answer, and its
  own `ExecutorTask`s; each
  step below continues from it. On `Recover`: read the metadata (open), metadata update loop OPEN -> IN_RECOVERY; if the ledger is
  closed, reply `Closed` at once; else `LedgerRecoveryOp.initiate`: `ReadLastConfirmedOp.initiateWithFencing` (fencing
  LAC read to all three); `readEntryComplete` until the RR coverage set is covered (2 valid responses), LAC = max;
  `doRecoveryRead` reads LAC+1, LAC+2, ... one `SequenceReadRequest` each: one bookie at a time in write-set order,
  first EOK completes it (-> `onEntryComplete` -> `asyncRecoveryAddEntry` + next `doRecoveryRead`), 2 NoSuchEntry
  answers fail it with NoSuchEntry (-> readDone). Recovery adds are `PendingAddOp`s with FLAG_RECOVERY_ADD acked at 2;
  `addComplete` counts writes; when readDone and reads == writes the promise completes; then `closeRecovered`
  (metadata update loop IN_RECOVERY -> CLOSED(lastAddConfirmed)); reply `Closed(number(metadata.lastEntryId))`.
- **b1..b3**: `AddRequest` -> `WriteEntryProcessorV3`: `recoveryAddEntry` (never fenced) or `addEntry`
  (LedgerFencedException when fenced -> EFENCED); store, answer EOK. `ReadRequest` -> `ReadEntryProcessorV3`: if
  fence flag, `fenceLedger` first; `readEntry` (LAC read = the highest stored entry); EOK with the body, or
  ENOENTRY / ENOLEDGER.
- **zk**: `ReadMeta` -> `MetaValue`; `WriteMeta` with the current version -> store, version+1, `MetaWritten`; else
  `MetaBadVersion`.

## Method map

| BookKeeper (main) | port |
|---|---|
| LedgerHandle.doAsyncAddEntry (client/LedgerHandle.java l.1716) | `LedgerHandle.doAsyncAddEntry` |
| LedgerHandle.isHandleWritable (l.550) | `LedgerHandle.isHandleWritable` |
| LedgerHandle.handleUnrecoverableErrorDuringAdd (l.2180) | `LedgerHandle.handleUnrecoverableErrorDuringAdd` |
| LedgerHandle.errorOutPendingAdds (l.2205, l.2219), drainPendingAddsAndAdjustLength (l.2209) | same names |
| LedgerHandle.sendAddSuccessCallbacks (l.2225) | `LedgerHandle.sendAddSuccessCallbacks` |
| LedgerHandle.asyncCloseInternal (l.554), doAsyncCloseInternal (l.576) | same names |
| LedgerHandle.asyncRecoveryAddEntry (l.1621) | `LedgerHandle.asyncRecoveryAddEntry` |
| LedgerHandle.executeOrdered (l.2538), PerChannelBookieClient's callback dispatch to it (l.1399-1404, l.1530-1555) | `LedgerHandle.executeOrdered` (task registry `tasks` + `ExecutorTask` to self), run in `LedgerHandle.run` on receipt |
| MetadataUpdateLoop (not in the cut) | `LedgerHandle.MetadataUpdateLoop` (run, onWritten, onBadVersion), `readLedgerMetadata`, `setLedgerMetadata` |
| PerChannelBookieClient's per-request completion (callback to the op that sent the request) | `LedgerHandle.dispatchAddResponse`, `dispatchReadResponse`, registries `addOps`, `readOps` |
| ReadOnlyLedgerHandle.recover / closeRecovered, LedgerOpenOp (not in the cut) | `LedgerHandle.recover`, `onRecoveryPromise`, `closeRecovered`, `recoveryComplete` |
| PendingAddOp.initiate (client/PendingAddOp.java l.246) | `PendingAddOp.initiate` |
| PendingAddOp.sendWriteRequest (l.146) | `PendingAddOp.sendWriteRequest` |
| PendingAddOp.writeComplete (l.278) | `PendingAddOp.writeComplete` |
| PendingAddOp.sendAddSuccessCallbacks (l.428), submitCallback (l.432) | same names |
| AckSetImpl.completeBookieAndCheck (RoundRobinDistributionSchedule l.307) | `PendingAddOp.completeBookieAndCheck` |
| RoundRobinDistributionSchedule.getWriteSetBookieIndex (l.58) | `getWriteSetBookieIndex` |
| RRQuorumCoverageSet.addBookie / checkCovered (l.373, l.378) | `ReadLastConfirmedOp.covered[]`, `checkCovered` |
| LedgerRecoveryOp.initiate (client/LedgerRecoveryOp.java l.92) | `LedgerRecoveryOp.initiate` |
| LedgerRecoveryOp.submitCallback (l.151), doRecoveryRead (l.166) | same names |
| LedgerRecoveryOp.onEntryComplete (l.176), addComplete (l.246) | same names |
| ReadLastConfirmedOp.initiateWithFencing (l.103), readEntryComplete (l.114) | same names |
| PendingReadOp.initiate (l.65), readEntryComplete (l.95), sendReadTo (l.176) | `RecoveryReadOp.initiate`, `readEntryComplete`, `sendReadTo` |
| ListenerBasedPendingReadOp.submitCallback (l.54) | `RecoveryReadOp.submitCallback` |
| PendingReadOp.SequenceReadRequest (l.295): read, sendNextRead (l.358), logErrorAndReattemptRead (l.391), complete (l.424), readsOutstanding | `RecoveryReadOp.SequenceReadRequest`, same method names |
| SingleLedgerEntryRequest.complete (l.204), LedgerEntryRequest.fail (ReadOpBase, not in the cut) | `SequenceReadRequest.complete`, `fail` |
| WriteEntryProcessorV3.getAddResponse / run (proto/WriteEntryProcessorV3.java l.45, l.169) | `BookieImpl.writeEntryProcessorV3` |
| ReadEntryProcessorV3.getReadResponse / readEntry / sendFenceResponse (proto/ReadEntryProcessorV3.java l.194, l.165, l.343) | `BookieImpl.readEntryProcessorV3` |
| BookieImpl.addEntry (bookie/BookieImpl.java l.1112) | `BookieImpl.addEntry` |
| BookieImpl.recoveryAddEntry (l.1024) | `BookieImpl.recoveryAddEntry` |
| BookieImpl.getLedgerForEntry (l.955), addEntryInternal (l.980) | same names |
| BookieImpl.fenceLedger (l.1153) | `BookieImpl.fenceLedger` |
| BookieImpl.readEntry (l.1159) | `BookieImpl.readEntry` |
| BookieImpl.readLastAddConfirmed (l.1187) | `BookieImpl.readLastAddConfirmed` |

## Decisions and simplifications

- D1 (interface) Fixed ensemble [b1, b2, b3], Qw = 3, Qa = 2; no ensemble change. Every error branch of
  `writeComplete` other than OK / LedgerFencedException is unreachable and throws an AssertionError (PendingAddOp
  l.338-401); the "ensemble has already changed" test (l.282) is always false and dropped; `waitForWriteSetMs`,
  `enforceMinNumFaultDomainsForWrite`, throttling, digests, master keys, lengths, metrics are dropped. The delayed
  write failure is ported (review round 3, Astra finding 2): an error after the ack quorum calls `notifyWriteFailed`
  (PendingAddOp l.300-308, LedgerHandle l.2266) into `delayedWriteFailedBookies`; the next `initiate` (l.269) calls
  `maybeHandleDelayedWriteBookieFailure` (l.2272) -> `handleBookieFailure` (l.2293-2336), which sets
  `changingEnsemble` (the gate of `sendAddSuccessCallbacks`, l.2230-2236) and calls `ensembleChangeLoop`. Only the
  ensemble change itself is replaced (`// PORT:`): with no spare bookie, the MetadataUpdateLoop of
  `ensembleChangeLoop` (l.2339-2453) evaluated on the local copy either throws in its transform
  (EnsembleUtils.replaceBookiesInEnsemble, BKNotEnoughBookiesException) when the copy is OPEN, or returns a CLOSED
  or IN_RECOVERY copy; its `whenCompleteAsync` callback (l.2397-2420) runs on the ordered executor as an
  `EnsembleChangeFailed` task and calls `handleUnrecoverableErrorDuringAdd` with WriteException,
  LedgerClosedException or LedgerFencedException; `changingEnsemble` stays set (reset only after a successful
  change, l.2442). The handle then closes as for an active fenced add.
- D2 (interface) zk is a process: one `LedgerMetadata(state, lastEntryId)` with a version starting at 0, OPEN, -1.
  The ledger was created by w1 before the study: w1h starts with that metadata at version 0 (the creation is not
  modelled). The LedgerManager is not in the cut.
- D3 MetadataUpdateLoop is not in the cut (used by LedgerHandle l.616-650): ported as the class
  `LedgerHandle.MetadataUpdateLoop`, an asynchronous state machine driven by the handle's one receive loop. `run()`:
  if the local copy needs no transformation, complete with it; else send the transformed copy to zk conditioned on
  the local version and return (the handle keeps receiving). `MetaWritten`: the local copy becomes the written one,
  complete. `MetaBadVersion`: re-read zk (`ReadMeta`), and on `MetaValue` set the local copy and `run()` again. A
  predicate that throws (doAsyncCloseInternal's BKMetadataVersionException, l.629-640) completes the loop
  exceptionally; the writer's close callback is NoopCloseCallback (l.2191), invisible. Review round 1 (Astra): the
  first submitted version ran the loop to completion with a blocking receive of zk answers only, so w1h could not
  receive and reject a later `Add` while its close was outstanding, which removed causal histories (`Add(2)` ->
  w1h -> metadata retry -> zk -> recovery's close -> `Closed`); fixed. The same version answered a bad version
  with zk's current value instead of a re-read; replaced by a separate re-read, which is what MetadataUpdateLoop does (reconstructed: not in the cut), so no
  causal-history argument is needed.
- D4 The recovery client's open and close (BookKeeper.asyncOpenLedger with recovery, LedgerOpenOp,
  ReadOnlyLedgerHandle.recover / closeRecovered) are not in the cut. Ported as: read the metadata from zk; update
  loop "state == OPEN -> IN_RECOVERY"; if the result is CLOSED, reply Closed with its last entry without recovery;
  else LedgerRecoveryOp; on success update loop "state == IN_RECOVERY -> CLOSED(lastAddConfirmed of the recovery
  handle)"; reply `Closed` with the last entry id of the resulting metadata (so if the writer's close got there
  first, w2 reports the writer's last entry). A failed recovery promise cannot occur here and raises an AssertionError.
- D5 Consequence of D3 kept from the source: the writer's close predicate (LedgerHandle l.624-645) tests only
  `isClosed()`, so the writer, fenced, may close the ledger at zk while it is IN_RECOVERY (after a bad version and a
  re-read), with its own LAC; the recovery's close then finds it CLOSED and returns that metadata. No IN_RECOVERY
  guard was added.
- D6 ReadOpBase (LedgerEntryRequest: fail, firstError, numBookiesMissingEntry, writeSet) is not in the cut:
  NoSuchEntry / NoSuchLedger answers increment numBookiesMissingEntry, requiredBookiesMissingEntryForRecovery =
  Qw - Qa + 1 = 2 (bk-src README; PendingReadOp l.411); `fail` sets the request complete with the code and calls
  submitCallback; the read order is the write set (entryId + k) % 3, no reorderReadSequence, no speculative reads
  (readSpeculativeRequestPolicy absent, PendingReadOp l.86), batch size 1 (interface).
- D7 LedgerStorage is an interface in the cut (getEntry, getLastAddConfirmed, LedgerStorage l.145, l.154): in-memory
  entries; `getEntry(LAST_ADD_CONFIRMED)` returns the stored entry with the highest id (NoEntryException if none);
  the LAC returned by the fencing LAC read is the LAC piggybacked in that entry's body (what
  `verifyDigestAndReturnLastConfirmed` reads, ReadLastConfirmedOp l.124); the storage's `getLastAddConfirmed` is the
  maximum piggybacked LAC stored (only used by the non-fence read branch, unreachable at main since every read
  fences). A ledger exists at a bookie after its first add or fence (handles.getHandle creates it, BookieImpl
  l.959, l.1155); a read before that answers ENOLEDGER, which the client treats as NoSuchEntry.
- D8 fenceLedger has no journal: the fence future completes at once with true (BookieImpl l.1153-1157,
  LedgerDescriptorImpl l.93); so a fence read answers EOK with the body or ENOENTRY / ENOLEDGER; EIO is unreachable.
- D9 The bookie is one process handling one request at a time (the request processors' thread pools and the
  journal's group commit are not modelled); the write callback (WriteEntryProcessorV3 l.68-97) answers at once.
- D10 The handle's ordered executor (LedgerHandle l.233-240, executeOrdered l.2538) is modelled with the Must API:
  appending a task = keeping the closure in the handle's process-local registry `tasks` under a sequence number and
  sending the handle itself `ExecutorTask(seq, kind)`; running the executor's next task = receiving that message and
  running the closure. Receiving a bookie response does not run its callback: it appends the callback
  (PerChannelBookieClient l.1399-1404, l.1530-1555 hand responses to the ledger's ordered executor). Tasks appended
  by a running task (the close task of doAsyncCloseInternal l.577, the closed-ledger rejection of doAsyncAddEntry
  l.1739, the recovery continuation after the LedgerRecoveryOp promise) are appended the same way. Under `fifo`
  the self-channel is FIFO, so the tasks run in append order, and the checker explores every interleaving of the
  executor's steps with the handle's network receipts: a callback already appended runs before a close task
  appended after it, as in the source. The order is enforced in both models (review round 3, Astra finding 3):
  the handle keeps `nextTaskToRun`, captures it in a final local before each receive, and its guard accepts an
  `ExecutorTask` only when its `seq` equals that value (network messages are accepted independently); running a
  task increments it. So under `bag` too the executor runs its tasks in append order, while the checker still
  chooses when the next task runs relative to network receipts. A received `Add` runs `doAsyncAddEntry` and
  `initiate` atomically with respect to executor tasks, whereas in the source a close task could drain the op
  between the push and `initiate` (then `initiate` returns without sending, PendingAddOp l.248-253); the reply is
  `Rejected` in both, and the port's extra write requests give only outcomes the source shows when `initiate` ran
  before the close (Opus r3 R3-2). Run directly on receipt, as in the source on the receiving thread: the
  client's `Add` (asyncAddEntry runs on the caller's thread) and `Recover`, and every zk answer (MetadataUpdateLoop's
  completions run on the metadata client's thread under the handle's lock, not on the ordered executor; Opus r2
  N3). Nothing on the executor blocks: a task that talks to zk sends and returns, and the answers come back through
  the receive loop (D3). Review round 2 (Astra finding 1, Opus r1 F1): the earlier version ran a queued task
  right after the callback that queued it, so a close appended by an EFENCED callback overtook OK callbacks the
  source already had queued; the source history Recover -> fencing LAC at b3 -> EFENCED(b3) run -> queued OK(b1),
  OK(b2) run -> Acked(1) -> close with LAC 0 -> Closed(1) was unreachable; fixed.
- D11 Receive guards: every message sent to a handle is eventually received. w1h receives `Add`, every
  `AddResponse`, every zk answer and its own `ExecutorTask`s; w2h receives `Recover` while idle, every
  `AddResponse`, every `ReadResponse`, every zk answer and its own `ExecutorTask`s; both loop forever. A response is dispatched to the op that sent the request, as the bookie
  client's per-request callback does: registries `addOps` (entry id -> PendingAddOp, kept after completion or
  drain) and `readOps` (entry id -> RecoveryReadOp); LAC responses go to the one ReadLastConfirmedOp. The ops'
  own source logic consumes late responses without a second callback: `writeComplete`'s `completed` branch calls
  `sendAddSuccessCallbacks` and returns (PendingAddOp l.293-329). A late EFENCED after the ack quorum does occur
  (witness at e = 1: b3 fenced by the LAC read, entry 0 EOK at b1 and b2, `Acked(1)`, then b3's EFENCED received);
  it records the delayed write failure that the next `initiate` consumes (D1). An OK for a drained op sets `completed` and finds it no longer queued; a fence for
  a drained op closes an already CLOSED handle (no zk write, nothing pending, LedgerHandle l.595-615) or, if the
  local copy is IN_RECOVERY after a re-read, errors out an empty queue; a third LAC answer updates the coverage set
  and finds `completed` (ReadLastConfirmedOp l.114-179). A recovery read never gets a late answer: with batch size
  1 and no speculative reads it has at most one read outstanding (Opus r2 N2); the `complete` / `isComplete` guards
  are kept as in the source. Review round 1 (Astra): the first version
  excluded these responses from the guards and left them unreceived; fixed.
- D12 The add callback of w1h replies with the number i of the `Add` (the op's ctx), `Acked(i)` on OK and
  `Rejected(i)` on any error (LedgerFencedException via errorOutPendingAdds, LedgerClosedException for an add on a
  closed handle, LedgerHandle l.1736-1745, where the source passes INVALID_ENTRY_ID).
- D13 The interface's "nondeterministic moment" of w2 is the scheduling of w2 itself (no Nondet): the client loops
  are the interface's Rust loops verbatim, which take no choice.

- D14 setLedgerMetadata (LedgerHandle l.400-415): when the new metadata is closed, lastAddConfirmed = lastAddPushed =
  its last entry id (review round 1, Opus F3, optional, applied). No expected-object check (each handle is one process)
  and no length.

## Compile and runs, review round 3 (2026-11-12, after the delayed write failure and the executor order in both models)

Compile (inside this directory, no warnings or errors):
`javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')`

The runs were made inside `tmp/` with `-cp ../java/out:...`; all outputs are under `tmp/`. First submission: `tmp/r0/`
and `tmp/BookKeeperPort.java.r0`; round 1: `tmp/r1/` and `tmp/BookKeeperPort.java.r1`; round 2: `tmp/r2/` and
`tmp/BookKeeperPort.java.r2`.

- e = 1: `java -Xss4m -Xmx8g -cp ../java/out:$(cat .../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.bookkeeper_port.BookKeeperPort:impl,1,fifo' must 0 1 report - 600 </dev/null`
  -> `graphs_complete: 40946`, `graphs_blocked: 0`, `iterations: 40946`, **`stop_reason: timeout`** at 600 s, no
  error (tmp/run-e1.log). **Not exhaustive.**
- export: `... 'bench.bookkeeper_port.BookKeeperPort:impl,1,fifo' must 0 1 export.jsonl 600 --split 2 --k 1 </dev/null`
  -> `FAILED: incomplete: timed out with 14 of 22 items committed`, `stop_reason: error`; the cause is the time
  limit, no assertion or exception (tmp/run-export.log). The 14 committed items hold 52379 executions.

Round 2 (tmp/r2/, source tmp/BookKeeperPort.java.r2): e = 1 35877 executions, timeout; export 13 of 22 items
(38823 executions), timeout.
Round 1 (tmp/r1/): e = 1 36743 executions, timeout; export 10 of 16 items committed, timeout; a 4-worker 1800 s
attempt committed 14 of 16 items holding 83543 executions. The growth over the first submission (15912,
exhausted) comes from the round-1 fixes: every late response is received, so its position among the handle's other
receives is explored, and each bad version costs a separate re-read; round 2 adds the executor's steps as
receives of self-messages.
- e = 2: not rerun. It was already not exhaustive before the fixes (24916 executions in 600 s; a 4-worker 1800 s
  attempt committed 19 of 25 items).

First submission, for reference (tmp/r0/): e = 1 15912 executions, exhausted; export 15912 iterations as 4 distinct
summaries, exhausted; outcomes (w1; w2): Add 1 Acked 1; Closed 1 | Add 1 Rejected 1; Closed 1 | Add 1 Rejected 1;
Closed -1. No outcome had an acknowledged entry missing from the closed ledger.
