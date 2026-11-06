# ScalarDB Consensus Commit: Rust port (TraceForge Must API)

Source: scalar-labs/scalardb at 416cdc2, the cut in plan/case-studies/scalardb/cc-src (`C/` =
core/src/main/java/com/scalar/db/transaction/consensuscommit/). Interface: INTERFACE-agents.md.
Code: src/main.rs. Crate: `bench_scalardb_port` (see decision D1).

## Roles (threads, started in this order)

| thread | role | lifetime |
|---|---|---|
| `s1..sk` | storage partition of key `ki` (replaced part 1): `Option<Rec>`, initially value 0, tx_id t0, COMMITTED, version 1, timestamps 0, before image null | never ends |
| `coord` | coordinator table (replaced part 2): map tx_id -> (state, created_at), put-if-not-exists | never ends |
| `xik` | recovery task of handle `hi` for key `k` (replaced part 5): one job, one completion | never ends (idles after its job) |
| `hi` | transaction handle of `ci`, transaction `ti`: ConsensusCommit, CrudHandler, RecoveryExecutor (synchronous part), Snapshot, CommitHandler, ParticipantCommitHandler, CoordinatorCommitHandler, CoordinatorStateAccessor | never ends |
| `ci` | client (verbatim loop of the interface); visible | ends |
| `init` | sends every invisible process one `Setup` | ends |

The main thread spawns them, then joins `init` and the clients; it sends and receives nothing.

## Messages

Visible (verbatim): `Req { Begin, Read(k), Write(k, v), Commit }`, `Rep { Begun, Value(k, v), Written(k),
Committed, Aborted, Unknown }`; clients send untagged, handles receive with a typed receive on
(sender == own client, no tag).

Invisible (`enum Msg`); requests carry the sender and a request id `rid` (per-process counter from 1) and
are sent with tag 0; each reply is sent with tag `rid` and received by a typed receive on that tag, so a
reply nobody waits for any more stays unreceived; the setup is tagged `u32::MAX`.

| message | from -> to | meaning (source) |
|---|---|---|
| `Setup(Setup)` | init -> s, coord, x, h | addresses of all processes, tx ids t1..t3, isolation, read-only flags |
| `Get{from,rid}` -> `Record{rid,rec}` | h, x -> sk | `storage.get` (api/DistributedStorage l.146) |
| `Mutate{from,rid,m}` -> `Applied{rid,ok}` | h, x -> sk | `storage.mutate` with one Put or Delete (l.203-217); `ok=false` = NoMutationException |
| `GetState{from,rid,id}` -> `State{rid,st}` | h, x -> coord | CoordinatorStateAccessor.get (l.344-376) |
| `PutState{from,rid,id,st}` -> `Applied{rid,ok}` | h, x -> coord | CoordinatorStateAccessor.put (l.400-437), put-if-not-exists (l.389); `ok=false` = CoordinatorConflictException with 1 attempt |
| `Recover{from,rid,job}` -> `Done{rid}` | hi -> xik -> hi | executorService.submit (RecoveryExecutor l.223, l.326) and the future's completion (awaited at CrudHandler l.808) |

`Job::TryRecover{rec, state}` = `recovery.tryRecover(selection, recovered, state)` (RecoveryExecutor
l.223-228); `Job::Rollback{rec}` = `recovery.rollbackRecord(selection, recovered)` (l.326-331).

Mutations: `Put{cols, cond}` sets exactly the listed columns (creating the record if absent);
`Delete{cond}`; `cond` is `PutIfNotExists`, `If([(col, Some(c) | None)])` (`== c` / `IS NULL`), or
`Unconditional` (never used by the cut).

## Protocol, role by role

**Handle `hi`** loops on client requests.
- `Begin`: new workspace (read set, get set, write set, recovery results).
- `Read(k)`: if k is in the get set, the cached result (merged with the write set). Else `Get` from `sk`;
  committed -> the record; uncommitted -> `resolveLatestResultAndRecover`: `GetState(writer)`;
  present -> ABORTED: the before image, COMMITTED: the after image (state COMMITTED, committed_at 0,
  before image null), and a `TryRecover` job to `xik` (roll back / roll forward); absent -> `nondet()`
  (isTransactionExpired): false -> `Aborted` (UncommittedRecordException); true -> re-`Get`: gone ->
  empty, committed -> it, other writer -> retry against it (at most 5 passes), same writer ->
  `PutState(writer, ABORTED, 0)`: Ok -> re-`Get` once more (same three checks), then the before image
  and a `Rollback` job to `xik`; NoMutation -> retry from the top (limit 5). The result goes into the
  read set (not for a read-only handle) and the get set; reply `Value(k, value)`.
- `Write(k, v)`: into the write set; `Written(k)`.
- `Commit`: wait for the `Done` of every recovery job of a written key (every key under serializable);
  then CommitHandler.commit: prepare (one conditional Put per written key: value, tx_id = own,
  PREPARED, prepared_at 0, before_* = the read result's after-image columns, version = read version + 1,
  condition `tx_id == read tx_id`; all sent, all replies collected); any NoMutation -> abortState
  (PutState ABORTED; on conflict GetState), rollbackRecords, `Aborted`. Serializable: re-`Get` every
  read-but-not-written key; a changed tx_id (or presence) -> abortState + rollback (if writes),
  `Aborted`, the remaining replies left unreceived. Then (if writes) `PutState(own, COMMITTED, 0)`:
  Ok -> commitRecords (Put COMMITTED, committed_at, before_* null, condition `tx_id == own AND
  tx_state == PREPARED`, results ignored), `Committed`; NoMutation -> `GetState(own)`: ABORTED or
  absent -> rollbackRecords, `Aborted`; COMMITTED -> commit records, `Committed`. No writes ->
  `Committed`.
- rollbackRecords: `Get` every written key, then for each record still own: restore the before image
  (Put, before_* null) or delete it when it has none, condition `tx_id == own AND tx_state == latest
  state`; results ignored.

**Recovery task `xik`**: waits for one `Recover`; TryRecover with COMMITTED -> rollforwardRecord
(CommitMutationComposer on the record, committed_at = state's created_at), ABORTED -> rollbackRecord,
absent -> abortIfExpired (best-effort); Rollback -> rollbackRecord; NoMutation ignored; then `Done`.

**`sk`**, **`coord`**: evaluate one request at a time and reply to the sender.

## ScalarDB method -> port function

| ScalarDB (C/ unless noted) | lines | port |
|---|---|---|
| ConsensusCommitManager.begin | 303-330 | `Handle::begin`; read-only flag from the setup |
| ConsensusCommit.get | 79-81 | `Handle::get` |
| ConsensusCommit.upsert, CrudHandler.put, Snapshot.putIntoWriteSet | 198-204; 682-703; 157-226 | `Handle::upsert` |
| ConsensusCommit.commit / commitInternal | 227-270 | `Handle::commit` |
| ConsensusCommit.rollback / releaseResources | 272-295 | not reached (clients never roll back; nothing to release: no scanners, no group commit) |
| TransactionContext.isSnapshotReadRequired / isValidationPossiblyRequired / isValidationRequired | 69-100 | `Handle::get` (always snapshot read), `commit` (`serializable`), `validation_keys` |
| Snapshot.putIntoReadSet / putIntoGetSet, getResult, mergeResult | 129-137, 307-354 | `Handle::read`, `Handle::get`, `merge_result` |
| Snapshot.to | 371-382 | the loops of `prepare_records`, `commit_records`, `rollback_records` |
| Snapshot.toSerializable (Get part), validateGetResult, isChanged, throwExceptionDueToAntiDependency | 640-696, 872-898, 1004-1022 | `validate_records` |
| CrudHandler.get / readUnread / read | 97-124, 204-303 | `Handle::get`, `Handle::read` |
| CrudHandler.recoverAndFilter / executeRecovery | 396-483 | `recover_and_filter`, `execute_recovery` |
| CrudHandler.putIntoReadSetInSnapshot / getFromStorage | 598-604, 838-851 | `Handle::read`, `Io::storage_get` |
| CrudHandler.waitForRecoveryCompletionIfNecessary / waitForRecoveryCompletion | 787-836 | start of `Handle::commit` |
| RecoveryExecutor.execute (RETURN_LATEST_RESULT_AND_RECOVER) | 123-180 | `execute_recovery` |
| RecoveryExecutor.resolveLatestResultAndRecover | 205-338 | `resolve_latest_result_and_recover` |
| RecoveryExecutor.checkReadRetryLimit / getCoordinatorState | 340-361 | `check_read_retry_limit`, `Io::get_state` |
| RecoveryExecutor.createRecoveredResult / createRecordFromBeforeImage / createResultFromAfterImage | 363-374, 399-461 | inline choice, `record_from_before_image`, `result_from_after_image` |
| RecoveryExecutor.throwUncommittedRecordExceptionIfTransactionNotExpired | 376-397 | `if !nondet() { Err(Conflict) }` |
| RecoveryExecutor submissions | 139, 223, 326 | `Handle::submit` (l.223, l.326; l.139 is not used) |
| RecoveryHandler.tryRecover / rollRecordToState | 73-81, 151-160 | `Recovery::try_recover`, `roll_record_to_state` |
| RecoveryHandler.rollbackRecord / rollforwardRecord | 162-241 | `Recovery::rollback_record`, `rollforward_record` |
| RecoveryHandler.tryAbortExpiredTransaction | 268-280 | `Io::put_state(ABORTED)` in `resolve_latest_result_and_recover` |
| RecoveryHandler.abortIfExpired (ensureRecordResolved = false) / isTransactionExpired | 295-378, 429-432 | `Recovery::abort_if_expired`, `nondet()` |
| RecoveryHandler.mutate | 399-427 | `Io::storage_mutate` (no retriable error exists) |
| CommitHandler.commit, abortStateAndRollbackRecordsIfNeeded, isCoordinatorStateWriteRequired | 155-258 | `commit_handler_commit` |
| CommitHandler.commitState / abortState | 303-313 | `commit_state`, `abort_state` |
| ParticipantCommitHandler.prepareRecords | 76-102 | `prepare_records` |
| ParticipantCommitHandler.validateRecords | 104-118 | `validate_records` |
| ParticipantCommitHandler.commitRecords / doCommitRecords | 120-140 | `commit_records` |
| ParticipantCommitHandler.rollbackRecords / doRollbackRecords, toReadTasks | 142-173, 274-285 | `rollback_records` |
| ParticipantCommitHandler.tryRecoverRecordsBlockingWrites / getKeysOfWritesNeverRead | 202-252 | assertion that the key list is empty (D9) |
| CoordinatorCommitHandler.commitState / handleCommitConflict | 50-167 | `commit_state` |
| CoordinatorCommitHandler.abortState / readCoordinatorStateAfterAbortConflict | 173-195, 241-252 | `abort_state` |
| CoordinatorStateAccessor.getState / putState / forceAbort / get / put | 163-169, 214-228, 344-437 | `Io::get_state`, `Io::put_state`; the store in `coordinator` |
| PrepareMutationComposer.add(Put), createBeforeColumns, isBeforeRequired | 47-84, 123-148 | `prepare_records` |
| CommitMutationComposer.add / add(Selection) / composePut / composeDelete | 39-125 | `commit_mutation`, `rollforward_record` |
| RollbackMutationComposer.add / getLatestRecord / composePut / composeDelete | 62-165 | `rollback_records` (commit path), `rollback_record` (recovery path), `rollback_mutation` |
| TransactionResult getState / isCommitted / hasBeforeImage | 167-232 | `state_of`, `is_committed`, `has_before_image` |
| ParallelExecutor.executeTasks / executeTasksInParallel | 248-316 | `run_all_mutations` (stopOnError false), `validate_records` (stopOnError true, one-task shortcut) |
| AsyncExecutor.commitRecords / rollbackRecords | 85-106 | inline calls |
| ConsensusCommitUtils.getNextTxVersion / rereadRecord | 416-422, 471-484 | `+ 1` / `Some(1)`, `Io::storage_get` |

## Decisions and simplifications

- D1. Crate name: the task fixes `[package] name = "bench_scalardb_port"`; INTERFACE-agents.md says crate
  `scalardb_cc`. The task's Cargo.toml wins.
- D2. The delivery model is the shared main's `--model` option; a fourth program argument (`fifo|bag`) is
  accepted and ignored. Both `Program` entries return the implementation.
- D3. Reply collection order (ParallelExecutor l.268-316 collects in completion order). For phases with
  stopOnError = false (prepare l.76, commit, rollback) and for readRecordsForRollback (no read can fail),
  every reply is collected and the outcome does not depend on the order, so the port collects in send
  order (fewer explored orders, same outcomes). Validation (stopOnError = true) collects in arrival
  order (`wait_any`) and stops at the first anti-dependency, leaving the other replies unreceived; with a
  single task it runs the task directly (l.255-259).
- D4. Recovery futures hold no exception: the shared store never fails and never returns a retriable
  error (INTERFACE), NoMutation is absorbed in rollbackRecord / rollforwardRecord (RecoveryHandler
  l.183-194, l.229-240), and a forceAbort conflict in abortIfExpired returns false (l.328ff). So
  `Done` carries nothing and CrudHandler l.805-836 never throws. Immediate futures
  (`Futures.immediateFuture`, RecoveryExecutor l.252, l.256, l.299, l.303) send no job and wait for nothing.
- D5. `Unknown` is never produced: UnknownTransactionStatusException comes only from a CoordinatorException
  (a failing coordinator) or from `getNumAttempts() > 1` (CoordinatorCommitHandler l.80-82), and every
  put is applied or not on its first attempt (CoordinatorStateAccessor l.412: `counter + 1` = 1).
- D6. The CommitHandler ignores abortState's returned state (l.179, l.245); the port does the same.
  The handle's own COMMITTED row cannot be written by anyone else, so handleCommitConflict's COMMITTED
  branch (l.120-128) is ported but unreachable; its absent branch (l.129-165) is ported (Aborted) and
  unreachable without cleanup.
- D7. Timestamps: every `System.currentTimeMillis()` is 0, including RecoveryExecutor.getCommittedAt in
  createResultFromAfterImage (l.453; getCommittedAt l.463-467 returns System.currentTimeMillis()).
- D8. createAfterImageColumnsFromBeforeImage (ConsensusCommitUtils; used by RecoveryExecutor l.413 and
  RollbackMutationComposer l.133) is not in the listed lines and was not read; the port maps `before_X -> X` for the six after-image columns and
  nulls the before image (setBeforeImageColumnsToNull, AbstractMutationComposer l.33ff for the composer;
  RecoveryExecutor addNullBeforeImageColumns l.421, l.456). The before_version = 0 special case
  (PrepareMutationComposer l.128-134) never arises: every record carries a version.
- D9. tryRecoverRecordsBlockingWrites (ParticipantCommitHandler l.202-235) only concerns written keys never
  read; every client reads every key before writing, so the list is empty; the port asserts that.
- D10. A Read answered `Aborted` leaves the transaction as it is (the client ends without rollback;
  ConsensusCommit.rollback l.272-275 only releases resources). A read result that is absent cannot
  occur (every key has a record and every prepared record has a before image); the handle panics on it.
- D11. A read-only handle (c3, mixedro) is supported through the setup flag (no read set, no prepare, no
  state writes: CommitHandler l.157-158, l.256-258, CrudHandler l.600). It is not exercised by the runs.
- D12. `rid` tags: one counter per requesting process; replies are matched only by tag, so late replies
  (validation after the first conflict) stay unreceived.
- D13. Threads are spawned with `thread::Builder::new().name(..).spawn(..)` (not daemon threads), so the
  never-ending processes end every execution blocked in a receive: `count` reports every execution as
  blocked (`execs=0 blocked=N`). A throw-away check (not kept) that spawned s, coord, x and h as daemon
  threads gave the same totals as complete executions (lost 1: 74, skew 2 serializable: 156, mixed 2:
  361), i.e. in every explored execution all clients finished.
- D14. Expiry: `nondet()` at each call of isTransactionExpired (handle: RecoveryExecutor l.384; task:
  RecoveryHandler l.300, reached only for a TryRecover with no state, which this cut's read path never
  submits).

## Compile

```
cd /local/home/mkhoshechin/Documents/github-jmc/daavar/benchmarks-cf2/scalardb-cc/rust
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
```
Result: Finished `release` profile; 2 warnings (variants `Rep::Unknown` and `Cond::Unconditional` never
constructed: D5, and no unconditional mutation in the cut).

## Runs (2026-11-06, inside the crate directory)

| command | result line | last output line |
|---|---|---|
| `timeout 600 target/release/bench_scalardb_port count impl --model fifo -- lost 1 snapshot` | `execs=0 blocked=74` | `cache_hits: ` |
| `... -- lost 2 snapshot` | `execs=0 blocked=94` | `cache_hits: ` |
| `... -- skew 2 snapshot` | `execs=0 blocked=65` | `cache_hits: ` |
| `... -- skew 2 serializable` | `execs=0 blocked=156` | `cache_hits: ` |
| `... -- mixed 2 snapshot` | `execs=0 blocked=361` | `cache_hits: ` |

All exit 0, each well under a second. The last line printed is the measurement block's final key;
the run's own result is the `execs=` line before the block.
