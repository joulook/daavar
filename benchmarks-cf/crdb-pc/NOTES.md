# crdb-pc, Java transliteration (JMC message-passing API)

Source: cockroachdb/cockroach at d30c905, the cut in `plan/case-studies/cockroach/cr-src` (README.md), with
lookups in the full checkout `plan/case-studies/cockroach/code`. Interface: `INTERFACE-agents.md`.
One file: `java/bench/cockroach/ParallelCommits.java`, class `bench.cockroach.ParallelCommits` (implements
`org.mpi_sws.jmc.mp.launch.MpProgram`), argument string `impl,<n>,<p>,<m>,<a>,<fifo|bag>`.
Lines changed for a reason of the target language or of the interface carry `// PORT:` and the reason.

## Roles and messages

| process | role | spawn | what it stands for |
|---|---|---|---|
| `r1`, `r2` (r2 only when n = 2) | invisible | daemon | one range each: leaseholder batch evaluation, store (per-key cells), timestamp cache; r1 also T's record and the folded-in async intent resolver |
| `tcx` | invisible | daemon | goroutine of `txnCommitter.makeTxnCommitExplicitAsync` |
| `tc` | invisible | daemon | TxnCoordSender: `txnPipeliner` -> `txnCommitter` -> `txnSpanRefresher` -> DistSender |
| `h1..hm` | invisible | daemon | preventer i's push path: PushTxn at r1, `Replica.handleIndeterminateCommitError`, txnrecovery probe + RecoverTxn, retried push, resolution of T's intents |
| `c` | visible | `spawn("c", ...)`, awaited | T's client (interface loop) |
| `p1..pm` | visible | `spawn("pi", ...)`, awaited | preventers' clients (interface loop) |
| `init` | invisible | daemon | sends `Setup` to r1, r2, tcx, tc, h1..hm (in that order) |

Spawn order: r1, r2, tcx, tc, h1..hm, c, p1..pm, init. Main only spawns and awaits c, p1..pm.
`rolesInSpawnOrder()` = `visibleRoles()` = `c, p1..pm` (the daemons carry no role).
Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`; every send and receive uses it.

Visible messages (records, `@Tag` = the Rust variant name): `Put(int key)`, `Commit(List<Integer> keys)`,
`Recover()`, `Written(int key)`, `Committed()`, `Retry()`, `Aborted()`, `Recovered(boolean committed)`.

Invisible messages:
- `Setup(ranges, tc, tcx, handles, c, preventers, TxnMeta txn)`: init -> every invisible process (T's meta: key k1,
  epoch 0, write timestamp 1, min timestamp 1, sequence 0).
- `BatchRequest(from, id, Transaction txn, timestamp, canForwardReadTimestamp, noReply, List<Request>)`: tc / tcx /
  hi / r1 (intent resolver) -> a range. Requests: `PutRequest`, `QueryIntentRequest`, `EndTxnRequest`,
  `QueryTxnRequest`, `PushTxnRequest`, `RecoverTxnRequest`, `ResolveIntentRequest`.
- `BatchResponse(id, Transaction txn, List<Response>, KvError error)`: range -> sender (not for `noReply`).
  `KvError` kinds: INTENT_MISSING, TRANSACTION_RETRY(reason), TRANSACTION_ABORTED(reason),
  TRANSACTION_STATUS(reason), INDETERMINATE_COMMIT(StagingTxn), AMBIGUOUS_RESULT, NO_1PC_TXN (errNo1PCTxn).
- `MakeTxnCommitExplicit(Transaction txn, List<Integer> lockSpans)`: tc -> tcx.

## Function mapping

| Go (file, lines) | Java |
|---|---|
| TxnCoordSender.Send / updateStateLocked / handleRetryableErrLocked (code: txn_coord_sender.go l.510-600, 864-1106) | `Coordinator.send`, `handleRetryableErrLocked`, `transactionRestart` |
| txnPipeliner.SendLocked (pipeliner l.311-361) | `Coordinator.pipelinerSendLocked` |
| attachLocksToEndTxn (l.437-501) | `Coordinator.attachLocksToEndTxn` |
| canUseAsyncConsensus (l.503-590) | `Coordinator.canUseAsyncConsensus` |
| chainToInFlightWrites (l.608-718) | `Coordinator.chainToInFlightWrites` |
| updateLockTrackingInner (l.796-899) | `Coordinator.updateLockTrackingInner` |
| stripQueryIntents, adjustError (l.904-954) | `Coordinator.stripQueryIntents`, `adjustError` |
| txnPipeliner.epochBumpedLocked | `Coordinator.pipelinerEpochBumpedLocked` |
| txnCommitter.SendLocked (committer l.138-261) | `Coordinator.committerSendLocked` |
| canCommitInParallel (l.349-396), mergeIntoSpans | `Coordinator.canCommitInParallel`, `mergeIntoSpans` |
| isTxnCommitImplicit (l.413-441) | `Coordinator.isTxnCommitImplicit` |
| retryTxnCommitAfterFailedParallelCommit (l.443-486) | `Coordinator.retryTxnCommitAfterFailedParallelCommit` |
| maybeRemoveStagingStatusInErr | `Coordinator.maybeRemoveStagingStatusInErr` |
| makeTxnCommitExplicitAsync (l.492-527) | `Coordinator.makeTxnCommitExplicitAsync` + process `tcx` (`ExplicitCommitTask.run`) |
| makeTxnCommitExplicitLocked (l.529-594) | `ExplicitCommitTask.makeTxnCommitExplicitLocked` |
| txnSpanRefresher.SendLocked (refresher l.150-188) | `refresherSendLocked` |
| sendLockedWithRefreshAttempts (l.226-245) | `sendLockedWithRefreshAttempts` |
| maybeRefreshAndRetrySend (l.249-324), maxTxnRefreshAttempts (l.30) | `maybeRefreshAndRetrySend`, `MAX_TXN_REFRESH_ATTEMPTS` |
| splitEndTxnAndRetrySend (l.367-410) | `splitEndTxnAndRetrySend` |
| maybeRefreshPreemptively (l.411-498) | `maybeRefreshPreemptively` |
| DistSender.Send (dist_sender l.1310-1420) | `DistSender.send`, `split` |
| divideAndSendParallelCommit (l.1567-1723) | `DistSender.divideAndSendParallelCommit` |
| detectIntentMissingDueToIntentResolution (l.1746-1798) | `DistSender.detectIntentMissingDueToIntentResolution` |
| mergeErrors (l.1819-1831) | `DistSender.mergeErrors` |
| divideAndSendBatchToRanges (l.1847-2160), sendPartialBatch(Async) | `DistSender.dispatch` (sends) + `collect` (its defer) + `sendPartialBatch` |
| Replica.Send -> executeWriteBatch / evaluateWriteBatch / evaluateBatch (code) | `Range.executeBatch` |
| maybeStripInFlightWrites (replica_batch_updates l.37-170) | `Range.maybeStripInFlightWrites` |
| maybeBumpReadTimestampToWriteTimestamp (l.183-202), called at replica_write.go l.440 | inline in `Range.executeBatch` |
| applyTimestampCache (replica_tscache l.397-464) | `Range.applyTimestampCache` |
| updateTimestampCache (l.90-369: EndTxn, RecoverTxn, PushTxn, QueryIntent cases) | `Range.updateTimestampCache` |
| Replica.CanCreateTxnRecord (l.598-656), MinTxnCommitTS | `Range.canCreateTxnRecord`, `minTxnCommitTS` |
| EndTxn (cmd_end_transaction l.248-605), resolveLocalLocks, updateStagingTxn, updateFinalizedTxn, IsEndTxnTriggeringRetryError | `Range.endTxn` |
| QueryIntent (cmd_query_intent l.60-167) | `Range.queryIntent` |
| QueryTxn (cmd_query_txn l.48-89) | `Range.queryTxn` |
| RecoverTxn (cmd_recover_txn l.52-234) | `Range.recoverTxn` |
| PushTxn (cmd_push_txn l.111-350) | `Range.pushTxn` |
| CanCreateTxnRecord, BumpToMinTxnCommitTS, SynthesizeTxnFromMeta (transaction.go l.119-203) | `Range.canCreateTxnRecordOrError`, `bumpToMinTxnCommitTS`, `synthesizeTxnFromMeta` |
| storage MVCC Put / GetIntent / MVCCResolveWriteIntent (replaced) | `Range.put`, `Range.mvccResolveWriteIntent`, `Range.resolveIntent` |
| Replica.handleIndeterminateCommitError (replica_send l.818-838), retry at l.593-600 | `Handle.handleIndeterminateCommitError`, the loop in `Handle.run` |
| ResolveIndeterminateCommit (manager l.82-110) | `Handle.resolveIndeterminateCommit` |
| resolveIndeterminateCommitForTxn (l.120-151) | `Handle.resolveIndeterminateCommitForTxn` |
| resolveIndeterminateCommitForTxnProbe (l.158-267), defaultBatchSize | `Handle.resolveIndeterminateCommitForTxnProbe`, `DEFAULT_BATCH_SIZE` |
| resolveIndeterminateCommitForTxnRecover (l.279-299), batchTimestamp (l.306-310) | `Handle.resolveIndeterminateCommitForTxnRecover`, `batchTimestamp` |
| roachpb.Transaction.Update / BumpReadTimestamp / AsRecord, kvpb.Error.UpdateTxn, BatchRequest.UpdateTxn | `Transaction.update`, `bumpReadTimestamp`, `asRecord`, `KvError.updateTxn`, `Ba.updateTxn` |

## Decisions (where the source or the interface leaves something open)

1. Invisible processes as daemons. The interface says tc, tcx, the handles and the ranges never terminate;
   `MpProcess.spawnDaemon` (MpProcess.java) is the API's never-awaited process, and a daemon blocked on a receive
   does not make the execution blocked. Daemons have no role, so the declared roles are `c, p1..pm` only.
2. Setup carries the clients' addresses too (`c`, `p1..pm`): the interface's setup has "the addresses of all
   processes"; tc and hi need them to reply (JMC does not expose a message's sender).
3. Program name `crdb_pc` (the Rust crate's name), size `n,p,m,a`, tags = variant names (`Put`, `Commit`,
   `Recover`, `Written`, `Committed`, `Retry`, `Aborted`, `Recovered`). The Rust encoding was not available to me
   (other studies may not be read); to be checked against the Rust side before any export comparison.
4. DistSender sends and replies (dist_sender l.1946-1990, l.2066-2093): every partial batch of a divided batch is
   sent before any reply is awaited (the async goroutines and the synchronous last range are in flight together);
   the replies are then received in range order. The receive order is local to tc and observed by no one; the
   combined result is computed only after all replies, as the defer does.
5. The pre-commit QueryIntent goroutine (dist_sender l.1650-1660, `go func` l.1655) races with the synchronous
   EndTxn batch (l.1670). The README says it is not a process. Its sends and those of the EndTxn batch to the same
   range (r1, whenever p >= 1) reach r1 in either order in the source. Under `fifo` (P2P) tc's send order would fix
   the delivery order, so tc makes `Nondet.chooseBoolean()` for which group it sends first; under `bag` delivery
   already covers both orders and no choice is made. Without this, under fifo the #37866 interleaving (a recovery
   resolving k1 between T's EndTxn and T's pre-commit QueryIntent at r1) would be unreachable.
6. The swap of the EndTxn with the first pre-commit QueryIntent (l.1594-1600, positions l.1626-1648,
   maybeSwapErrorIndex) is replaced by direct original positions for both groups; same responses and error
   indices.
7. `mergeErrors` (l.1819-1831) without `kvpb.ErrPriority` (not supplied): only errors of one kind meet (two
   IntentMissingErrors of the pre-commit QueryIntents when p = 2), so the lower index wins; meeting two kinds is an
   assertion failure.
8. `BatchRequest.Split(canSplitET)` (Send l.1331-1343; kvpb not supplied): with the split (epoch > 0) the parts
   are "everything before the EndTxn" and "the EndTxn"; otherwise one part. Epoch > 0 is unreachable (README).
9. Refresher: T reads nothing, so the refresh footprint is empty and valid; `canForwardReadTimestampWithoutRefresh`
   is true (l.153), `tryRefreshTxnSpans` succeeds without a message. `maybeRefreshPreemptively` (l.156, l.411-498)
   is kept: it is on the SendLocked path that the retried EndTxn (committer l.464) and the explicit commit
   (committer l.543) take, and it is the "refreshing first" of the interface.
10. Server side: `maybeBumpReadTimestampToWriteTimestamp` (replica_batch_updates l.183-202, called at
    replica_write.go l.440) is kept, since it is code on the evaluation path of every write batch with an EndTxn.
    `evaluateWriteBatchWithServersideRefreshes` (replica_write.go l.690-740) is not modelled: its retry needs an
    evaluation error refreshable by `TransactionRefreshTimestamp`; the only candidate here is EndTxn's
    RETRY_SERIALIZABLE, which needs WriteTimestamp != ReadTimestamp after the bump, i.e. a STAGING record with a
    higher timestamp than the EndTxn's own txn, which this program does not produce. One-phase-commit evaluation is
    cut (README).
11. Not supplied, written from their behaviour (each marked PORT): `roachpb.Transaction.Update` (epoch-scoped
    replace / forward rules, timestamps forwarded), `AsRecord` (no ReadTimestamp), `kvpb.TransactionRefreshTimestamp`
    (RETRY_SERIALIZABLE, RETRY_WRITE_TOO_OLD), `PrepareTransactionForRetry` + `Txn.Restart` (epoch + 1, read := write,
    PENDING; RETRY_SERIALIZABLE forwards to `clock.Now()` = the timestamp in hand), `TransactionRestart` (retry and
    aborted errors), request flags: AppliesTimestampCache = Put; UpdatesTimestampCache = EndTxn, RecoverTxn, PushTxn,
    QueryIntent, ResolveIntent; on error only QueryIntent; QueryTxn not isAlone (the manager batches it with the
    QueryIntents and says they are "issued in parallel", manager l.220-223).
12. Errors from evaluation carry the batch header's txn at the failing request, with its index
    (replica_evaluate.go l.385-431 in the checkout); the command's reply txn is absorbed first.
13. A batch is evaluated on a copy of the store and applied only on success (a storage.Batch that is discarded on
    error); the timestamp cache is updated after evaluation (for errors only at the failing QueryIntent); the
    result of EndTxn finding an ABORTED record (l.354-363, alwaysReturn) is still processed.
14. Intent resolver folded into r1 (interface): after a successful batch, `result.FromEndTxn` external locks
    (l.568) get one fire-and-forget ResolveIntent batch each, sent after the reply. For the alwaysReturn case
    (l.361) the local keys are resolved by r1 right after the batch (no message to itself), the external ones by
    message.
15. No record GC (interface): `updateFinalizedTxn` (l.839-861) always writes the record (the auto-GC branch l.851
    is cut).
16. Handle: the push is a non-transactional batch [PushTxn(k1, pushee = setup meta, PUSH_ABORT)] with header
    timestamp and PushTo = the pushee's write timestamp (the timestamp in hand); the pusher has its own txn id for
    the tombstone marker. `pusherWins` is the "pushee is expired" case (cmd_push_txn l.224-229). The replica's
    retry after a recovery (replica_send l.593-600) is the handle's loop. After the push returns a finalized
    record, the handle sends one ResolveIntent batch for k1..kn with that status (split per range by the
    DistSender), waits for its replies (the pusher resolves synchronously), then replies `Recovered`.
17. `LastActive()` forwarding in PushTxn (l.316) is a no-op: no heartbeats, LastHeartbeat <= MinTimestamp <=
    WriteTimestamp.
18. Timestamp cache: per-key entries are only added by QueryIntents with uuid.Nil (replica_tscache l.335); the
    tombstone and push markers keep (timestamp, txn id); an equal timestamp from another txn sets the id to Nil.
    The push marker stays 0 (PUSH_ABORT only). EndTxn's tombstone uses the batch's read timestamp
    (`br.Timestamp = br.Txn.ReadTimestamp`, replica_evaluate.go l.491).
19. Cells: Put lays or rewrites T's intent (epoch, bumped write timestamp, sequence); ResolveIntent COMMITTED ->
    value at the intent's timestamp, ABORTED -> intent removed, no intent -> nothing (interface). No WriteTooOld.
20. Sequence numbers: each Put gets the next write sequence (txnSeqNumAllocator); EndTxn's sequence is not
    tracked (no use in the cut).
21. `updateLockTrackingInner`'s stripping of the failed request (pipeliner l.802-819) needs `ErrPriority`; the
    conservative branch (all of the batch's lock spans to the footprint) is taken. The footprint only matters at
    epoch > 0.
22. tc replies `Committed` for a successful commit, `Written(k)` for a successful Put, `Aborted` for a
    TransactionAbortedError (the client's restart with a new txn is cut), `Retry` for a TransactionRetryError
    (epoch bump, `txnPipeliner.epochBumpedLocked`). A non-retryable error reaching the client (TransactionStatusError,
    AmbiguousResultError) has no reply in the interface and is an AssertionError in tc.
23. `errors.AssertionFailedf` / `log.Fatalf` become Java AssertionErrors (a failed process fails the execution).
24. tcx is started by a message sent after tc replies `Committed` (interface), not before the committer returns
    (committer l.254 then l.259); `tc.mu` is implicit (tc is idle then: its client has ended).
25. tcx logs and ignores errors as the source does (l.512); "already committed" is tolerated (l.565-571).
26. The batch timestamp of a transactional batch is its txn's ReadTimestamp; `ds.clock.Now()` in
    detectIntentMissingDueToIntentResolution (l.1750) is the txn's write timestamp.

## Compile (inside OUT_DIR)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit 0 (only `bad path element` classpath warnings from cp.txt; with `-Xlint:all` also one `[serial]`
warning for `CmdError` and the unclaimed `@Tag` annotation notice).

## Runs (inside OUT_DIR)

    java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach.ParallelCommits:<args>' must 0 1 report - 900 </dev/null

`impl,1,1,1,1,fifo` (exit 0):

    ran:        43 iterations
    blocked:    0 (counted by the strategy)
    completed:  43
    elapsed:    2.842 s
    graphs:     43 distinct complete, 0 distinct blocked; stack exhausted
    wall_ms: 3789
    stop_reason: exhausted
    peak_rss_kb: 153852

`impl,2,1,1,1,fifo` (exit 0):

    ran:        137 iterations
    blocked:    0 (counted by the strategy)
    completed:  137
    elapsed:    7.384 s
    graphs:     137 distinct complete, 0 distinct blocked; stack exhausted
    wall_ms: 8449
    stop_reason: exhausted
    peak_rss_kb: 263532

`impl,2,2,1,1,fifo` (exit 0):

    ran:        140 iterations
    blocked:    0 (counted by the strategy)
    completed:  140
    elapsed:    12.232 s
    graphs:     140 distinct complete, 0 distinct blocked; stack exhausted
    wall_ms: 13466
    stop_reason: exhausted
    peak_rss_kb: 339848

`impl,2,0,1,1,fifo` (exit 0):

    ran:        27 iterations
    blocked:    0 (counted by the strategy)
    completed:  27
    elapsed:    2.784 s
    graphs:     27 distinct complete, 0 distinct blocked; stack exhausted
    wall_ms: 4481
    stop_reason: exhausted
    peak_rss_kb: 151048

`impl,2,1,2,1,fifo` (exit 0):

    ran:        12150 iterations
    blocked:    0 (counted by the strategy)
    completed:  12150
    elapsed:    900.107 s
    graphs:     12150 distinct complete, 0 distinct blocked; stack not exhausted
    wall_ms: 901337
    stop_reason: timeout
    peak_rss_kb: 665048

All five ended with exit 0 and no FAILED line. The four m = 1 instances exhausted (complete enumeration, 0 blocked
graphs); `impl,2,1,2,1,fifo` (m = 2) hit the 900 s timeout before exhausting (12150 graphs, 0 blocked).

Sanity check (a scratch copy outside OUT_DIR that counts outcomes per execution; not a deliverable): for
`1,1,1,1`, `2,0,1,1`, `2,1,1,1`, `2,2,1,1` under fifo, every execution has c = Committed with p1 seeing Committed,
or c = Aborted with p1 seeing Aborted; no `Retry` occurred (as the README's reachability notes predict).

