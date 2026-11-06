# ScalarDB Consensus Commit, Java transliteration on JMC's message-passing API

Source: scalar-labs/scalardb at 416cdc2, cut in plan/case-studies/scalardb/cc-src (`C/` =
core/src/main/java/com/scalar/db/transaction/consensuscommit/). Interface:
plan/case-studies/scalardb/INTERFACE-agents.md. One source file:
`java/bench/scalardb/ConsensusCommit.java` (class `bench.scalardb.ConsensusCommit implements MpProgram`,
constructed from `impl,<pattern>,<k>,<iso>,<fifo|bag>`). Every changed line carries `// PORT:`.

## Roles and messages

| process | started as | what it is |
|---|---|---|
| `s1..sk` | daemon | storage partition of key `ki`: sequential conditional store (replaced part 1) |
| `coord` | daemon | coordinator table: map tx_id -> State, GetState, put-if-not-exists PutState (part 2) |
| `xik` | daemon | RecoveryExecutor's background task for hi's read of key k (part 5) |
| `hi` | daemon | ConsensusCommit (as `ConsensusCommitTransaction`), ConsensusCommitManager.begin, CrudHandler, Snapshot, RecoveryExecutor (foreground part), CommitHandler, ParticipantCommitHandler, CoordinatorCommitHandler, CoordinatorStateAccessor, ParallelExecutor for transaction `ti` |
| `c1, c2 (c3)` | `MpProcess.spawn("ci", ...)`, awaited by main | the clients (INTERFACE client loop) |
| `init` | daemon | sends `Setup` to every invisible process |

Start order: s1..sk, coord, x11..xnk, h1..hn, c1..cn, init. Main sends and receives nothing; it
awaits the clients; `MpProcess.runMain` (the launcher) then drains the daemons.

Visible (`@Tag` = record name): `Begin()`, `Read(key)`, `Write(key, value)`, `Commit()`, `Begun()`,
`Value(key, value)`, `Written(key)`, `Committed()`, `Aborted()`, `Unknown()`.

Invisible:
- `Setup(s, coord, x, h, c, txIds, initialTxId, isolation, readOnly)` init -> all invisible.
- `StorageGet(from, reqId)` -> `StorageGetReply(reqId, record|null)`; `StorageMutate(from, reqId,
  Mutation)` -> `StorageMutateReply(reqId, applied)`. `Mutation(key, delete, columns, Condition)`,
  `Condition(NONE|PUT_IF_NOT_EXISTS|PUT_IF|DELETE_IF, [Conjunct(column, value|null=IS NULL)])`.
- `CoordGetState(from, reqId, id)` -> `CoordGetStateReply(reqId, State|null)`;
  `CoordPutState(from, reqId, State)` -> `CoordPutStateReply(reqId, applied)`.
- `RecoveryJob(jobId, task, selection, result, state|null, transactionId)` hi -> xik, task in
  {TRY_RECOVER (RecoveryExecutor l.223-226), ROLLBACK_RECORD (l.326-329),
  COMMITTED_RESULT_AND_RECOVER (l.139-160)}; `RecoveryDone(jobId, key, outcome)` xik -> hi, outcome
  DONE or the exception the Future would hold.

## Method mapping

| ScalarDB (C/) | here | change |
|---|---|---|
| ConsensusCommit get l.79-81, upsert l.198-204, commit l.227-237, commitInternal l.239-270, rollback l.272-275, releaseResources l.277-295 | `ConsensusCommitTransaction` same methods | renamed class (D7); upsert builds createPutForUpsert's Put inline; releaseResources empty (no slot, no scanners) |
| ConsensusCommitManager.begin l.303-330 | `ConsensusCommitManager.begin` | no group commit slot; read-only wrapper dropped (D22) |
| TransactionContext l.69-100 | `TransactionContext` | scan/scanner sets absent |
| Snapshot putIntoReadSet/GetSet/WriteSet, getResult, mergeResult, to, toSerializable (Get part), validateGetResult, isChanged, throwExceptionDueToAntiDependency | `Snapshot` same | validateGetResult becomes a task (request + code on reply); ordered maps (D6) |
| CrudHandler get, readUnread, read, recoverAndFilter, executeRecovery, putIntoReadSetInSnapshot, putIntoGetSetInSnapshot, put, readIfImplicitPreReadEnabled, waitForRecoveryCompletionIfNecessary, waitForRecoveryCompletion, getFromStorage | `CrudHandler` same | index/conjunction branches dropped |
| RecoveryExecutor execute, resolveLatestResultAndRecover, checkReadRetryLimit, getCoordinatorState, createRecoveredResult, throwUncommittedRecordExceptionIfTransactionNotExpired, createRecordFromBeforeImage, createResultFromAfterImage, getCommittedAt, addNullBeforeImageColumns, Result | `RecoveryExecutor` same, `RecoveryResult`, `RecoveryFuture`; lambda bodies in `runSubmitted` (run on xik) | submit -> RecoveryJob; Future -> RecoveryDone |
| RecoveryHandler tryRecover, rollRecordToState, rollbackRecord, createRollbackMutationComposer, rollforwardRecord, createCommitMutationComposer, tryAbortExpiredTransaction, abortIfExpired, mutate, isTransactionExpired | `RecoveryHandler` same | isTransactionExpired = Nondet (part 6) |
| CommitHandler commit, abortStateAndRollbackRecordsIfNeeded, isCoordinatorStateWriteRequired, phase delegations, commitState, abortState | `CommitHandler` same | hook and onFailure callback dropped (D23) |
| ParticipantCommitHandler prepareRecords, validateRecords, commitRecords/doCommitRecords, rollbackRecords/doRollbackRecords, tryRecoverRecordsBlockingWrites, getKeysOfWritesNeverRead, getKeysOfWritesAndDeletes, toReadTasks, toTasks, canOnePhaseCommit | `ParticipantCommitHandler` same | AsyncExecutor inline; tasks are requests |
| CoordinatorCommitHandler commitState, handleCommitConflict, abortState, readCoordinatorStateAfterAbortConflict | `CoordinatorCommitHandler` same | timestamps 0 |
| CoordinatorStateAccessor getState, getStateInternal, putState, forceAbort, createGetWith, get, createPutWith, put, State | `CoordinatorStateAccessor`, `State` | non-group branches only; storage calls -> messages to coord; retry loops dropped (D13) |
| ParallelExecutor prepareRecords, validateRecords, commitRecords, rollbackRecords, readRecordsForRollback, executeTasks, executeTasksInParallel, executeTasksSerially | `ParallelExecutor` same | no pool: send all requests, collect replies in any order (part 4) |
| AbstractMutationComposer, PrepareMutationComposer add(Put)/add(Delete)/createBeforeColumns/isBeforeRequired, CommitMutationComposer add/composePut/composeDelete/getPartitionKey, RollbackMutationComposer add/getLatestRecord/composePut/composeDelete | same names | Put/Delete are `Mutation` records |
| ConsensusCommitUtils getNextTxVersion, createAfterImageColumnsFromBeforeImage, rereadRecord | static methods, same names | |
| MutationsGrouper.groupMutations | `groupMutations` | one mutation per group (every key its own partition) |
| TransactionResult getId, getState, getVersion, getPreparedAt, isCommitted, isDeemedAsCommitted, isMergedResult, hasBeforeImage | record `TransactionResult(key, columns, mergedResult)` | carries the partition key (D19) |
| api/DistributedStorage get/mutate | `DistributedStorage` (per process) | request/reply to sk (part 3) |

## Decisions (where the source or the interface leaves something open)

- D1 Invisible processes are daemons (`MpProcess.spawnDaemon`): they never terminate (prompt;
  INTERFACE "never do"), and MpProcess.Handle.await requires every non-daemon to be awaited
  (MpProcess.java, Handle.await doc). A daemon has no role, so `rolesInSpawnOrder()` =
  `visibleRoles()` = the clients. `init` is a daemon too (it ends after its sends).
- D2 `fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN` (CommunicationModel.java l.10-12). All sends
  and receives use the instance's model.
- D3 Visible tags are the record names (`Begin`, ..., `Unknown`); program name `scalardb_cc` (the
  Rust crate name), size `<pattern>,<k>,<iso>`.
- D4 Request ids are per-process counters; every reply is received with a guard on its id. A reply
  of a phase that ended early (validation, `stopOnError = true`, ParallelExecutor l.95 and l.299-301)
  is never received: that is the "recognised and dropped" of replaced part 3.
- D5 ParallelExecutor's one-task shortcut (l.255-259) sends the one request and waits for it, which is
  message-for-message what the parallel branch does with one task; kept as a separate branch.
- D6 Snapshot's readSet/getSet are ConcurrentHashMap and writeSet/deleteSet HashMap (Snapshot
  l.97-101): iteration order unspecified. Chosen: readSet, writeSet, deleteSet ascending by key
  (TreeMap); getSet in insertion order, which is the read order k1..kk. This fixes the order in which
  a phase sends its requests (prepare, commit, rollback, validation).
- D7 ScalarDB's `ConsensusCommit` is `ConsensusCommitTransaction`: Java forbids a nested class with
  the name of its enclosing class, and the interface fixes the program class name
  `bench.scalardb.ConsensusCommit`.
- D8 A read that throws (CrudConflictException / CrudException, incl. UncommittedRecordException,
  RecoveryExecutor l.376-397) is answered `Aborted` after the handle calls `rollback()`
  (ConsensusCommit l.272-275: releaseResources only, no storage or coordinator effect). The handle
  then waits for further messages forever. Nothing is written to the coordinator in that case: the
  source writes nothing either.
- D9 A read that yields no record or a null value fails the run (AssertionError). Unreachable: every
  record has a before image (t0 is initial, PrepareMutationComposer l.61-64 copies it), so
  createRecordFromBeforeImage (l.399-402) never returns empty and rollback never deletes
  (RollbackMutationComposer l.84-88).
- D10 `Futures.immediateFuture(null)` (RecoveryExecutor l.175, 252, 256, 299, 303) sends no job; its
  `get()` returns at once. Every `executorService.submit` (l.139, l.223, l.326) sends one job to
  `x{i}{key}`; `recoveryFuture.get()` (CrudHandler l.808) waits for that job's `RecoveryDone`.
- D11 `isTransactionExpired` (RecoveryHandler l.429-432) is `Nondet.chooseBoolean()` in the process
  that calls it: hi in throwUncommittedRecordExceptionIfTransactionNotExpired (RecoveryExecutor
  l.384), xik in abortIfExpired (RecoveryHandler l.300) and in the COMMITTED_RESULT_AND_RECOVER job.
  In the instance only hi calls it: TRY_RECOVER is submitted only with a present state (l.211-228),
  so abortIfExpired is not reached on xik.
- D12 readIfImplicitPreReadEnabled (CrudHandler l.721-755): its reads would go through
  executeImplicitPreRead (parallel, stopOnError); here they run one after another. Not reached: every
  written key is read first (the client loop reads all keys), so the task list is empty.
- D13 CoordinatorStateAccessor.get / put retry loops (l.344-376, l.400-437) retry only on storage
  failures, which coord never produces: one attempt; a NoMutation reply is
  CoordinatorConflictException with numAttempts = counter + 1 = 1, so handleCommitConflict's
  `mayHaveConflictedWithOwnWrite` (CoordinatorCommitHandler l.81-83) is always false.
- D14 Exceptions are checked Java classes with ScalarDB's names and subclassing
  (UncommittedRecordException extends CrudConflictException, l.9; NoMutationException and
  RetriableExecutionException extend ExecutionException, DistributedStorage l.30-41;
  CoordinatorConflictException extends CoordinatorException). Every `catch (Exception e)` of the
  source (ConsensusCommit l.231, ParticipantCommitHandler l.136, l.167, l.227) is narrowed to the
  checked exceptions its body can throw, so that JMC's control-flow RuntimeExceptions
  (HaltTaskException, HaltExecutionException; Channel.java class doc) are never swallowed.
- D15 The exception a recovery Future would hold crosses from xik to hi as `RecoveryOutcome` and is
  rebuilt as an exception of the same class in `RecoveryFuture.get`; waitForRecoveryCompletion
  (CrudHandler l.805-836) then classifies it as the source does.
- D16 `TransactionResult.getVersion` of a null column is 0, as ScalarDB's `Result.getInt` of a null
  INT column (not reached: versions are never null in the instance).
- D17 Records: the store keeps every column of the table (null where unset); a Put sets the columns
  it carries, a Put on no record creates it with the others null. The partition key is the
  partition, not a column, so `TransactionResult` carries it as a field and isBeforeRequired
  (PrepareMutationComposer l.143-148) needs no primary-key test.
- D18 commitRecords / rollbackRecords run inline (AsyncExecutor l.85-106, async off), before the
  handle replies (INTERFACE configuration).
- D19 ConsensusCommit.get's FilteredResult (projections) is dropped: the handle replies with the
  `value` column of the result.
- D20 The before-preparation hook (CommitHandler l.170-172, waitBeforePreparationHookFuture) and
  `safelyCallOnFailureBeforeCommit` (l.99-107, a no-op callback outside group commit) are dropped;
  canOnePhaseCommit returns false (one-phase commit off).
- D21 tryRecoverRecordsBlockingWrites (ParticipantCommitHandler l.202-235) is transliterated (its job
  goes to `x{i}{key}`), but not reached: all written keys are read, so getKeysOfWritesNeverRead is
  empty.
- D22 ReadOnlyDistributedTransaction (ConsensusCommitManager l.326-328) only rejects writes; c3 sends
  none, so it is not modelled. The read-only flag still reaches TransactionContext (no read set, no
  coordinator write with write omission on, CommitHandler l.256-258).
- D23 The client receives with an unguarded receive: only its handle sends to it.

## Compile

Inside OUT_DIR (`daavar/benchmarks-cf2/scalardb-cc/java`):

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Result: no errors, no warnings (2026-11-06).

## Runs

Inside OUT_DIR, one after another (2026-11-06; the machine was at load 16 on 16 cores), for each
`<args>` in `impl,lost,1,snapshot,fifo`, `impl,lost,2,snapshot,fifo`, `impl,skew,2,snapshot,fifo`,
`impl,skew,2,serializable,fifo`, `impl,mixed,2,snapshot,fifo`:

```
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb.ConsensusCommit:<args>' must 0 1 report - 900 </dev/null
```

Full logs: `runs/<pattern>-<k>-<iso>.log`. All exit 0, none timed out, none FAILED. Last lines:

| args | graphs line | wall_ms | peak_rss_kb |
|---|---|---|---|
| lost,1,snapshot,fifo | 74 distinct complete, 0 distinct blocked; stack exhausted | 5382 | 335244 |
| lost,2,snapshot,fifo | 94 distinct complete, 0 distinct blocked; stack exhausted | 6752 | 375252 |
| skew,2,snapshot,fifo | 65 distinct complete, 0 distinct blocked; stack exhausted | 5311 | 334624 |
| skew,2,serializable,fifo | 156 distinct complete, 0 distinct blocked; stack exhausted | 9956 | 416036 |
| mixed,2,snapshot,fifo | 1834 distinct complete, 0 distinct blocked; stack exhausted | 105152 | 686980 |

Every run: `stop_reason: exhausted`, `graphs_blocked: 0`.

Sanity check (not part of the deliverable; an instrumented copy outside OUT_DIR recorded each
client's outcomes, lost,1,snapshot,fifo): c1 saw {r1=0 Aborted, r1=0 Committed, r1=2 Committed,
read Aborted}, c2 symmetrically; no Unknown.
