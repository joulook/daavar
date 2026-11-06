# ScalarDB Consensus Commit port

The executable ports the Java cut at ScalarDB `416cdc2`. Both `Program` entries return the same implementation. Inputs are `pattern k iso [fifo|bag]`; the optional trailing model selects the runner default, and the shared runner's explicit `--model fifo|bag` overrides it. Visible thread names and the `Req`, `Rep`, and `client` definitions follow `INTERFACE.md`. The Cargo package name follows the explicit task instruction (`bench_scalardb_sol`), overriding the interface's illustrative `scalardb_cc` crate name.

All Java paths below are relative to `cc-src/core/src/main/java/com/scalar/db/transaction/consensuscommit/`, abbreviated `C/` in the cut README. Only the permitted inputs were consulted, excluding `cc-src/docs`.

## Roles and messages

`run` starts `s1..sk`, `coord`, `x11..x1k`, `x21..x2k` (also `x31..x3k` in mixedro), `h1..hn`, `c1..cn`, then `init`, in exactly that order. The main thread only creates and joins threads. `init` sends one `Setup` to each protocol worker; clients need no setup message because their handle address is captured by their prescribed loop. Workers receive setup before any protocol request. Setup includes every storage, coordinator, recovery, handle and client address, logical transaction IDs t0..tn, isolation, and read-only flags. `init` is the bootstrap sender, not a protocol service, and terminates after sending setup. Clients run once and terminate. Handles, recovery workers, storage and coordinator remain alive forever, waiting on typed receives. A recovery worker that is never used waits for its first job forever.

| Sender → receiver | Message | Processing / reply |
|---|---|---|
| init → each protocol worker | `Setup` | Populate addresses and immutable transaction configuration; initial storage record belongs to t0. |
| ci → hi | `Req::Begin` | Allocate a fresh `Snapshot`; `Rep::Begun`. |
| ci → hi | `Req::Read(key)` | Read cached Get result, or physically read and resolve; merge local writes; `Rep::Value(key,value)` or `Rep::Aborted`. |
| ci → hi | `Req::Write(key,value)` | Upsert the local write set only; `Rep::Written(key)`. |
| ci → hi | `Req::Commit` | Await relevant recovery jobs, prepare, validate, commit coordinator state, commit records; `Committed` or `Aborted`. `Unknown` is part of the required reply enum but unreachable under the prescribed stores. |
| hi / xik → sk | `StorageRequest::Get { reply,id,key }` | Return `GetReply { id,key,record }` tagged with request ID. |
| hi / xik → sk | `StorageRequest::Mutate { reply,id,key,mutation }` | Atomically test condition, apply the column patch or Delete, return tagged `MutateReply { id,key,applied }`. |
| hi / xik → coord | `CoordinatorRequest::Get { reply,id,tx }` | Return tagged `StateReply { state: Option<(Decision,created_at)> }`. |
| hi / xik → coord | `CoordinatorRequest::Put { reply,id,tx,state,created_at }` | Put if absent; tagged `StatePutReply { applied }`. Existing state is immutable. |
| hi → xik | `RecoveryJob { record,state }` | Physically roll a known outcome; `None` supports best-effort abort-if-expired. Each fixed worker receives at most one job. |
| xik → hi | `RecoveryDone { key,outcome }` | Completion of the source's Future, tagged by key. Awaited in read order only for written keys or all recovery keys under SERIALIZABLE. |

Requests carry their reply address, avoiding a receive-sender side channel. IDs increase locally per handle/recovery worker; replies can use the same number at different receivers. Receives are typed and match only outstanding IDs for the current step. Completion messages have their own Rust type. Storage serves Get and Mutate using one request enum, so they share the same sequential store ordering; coordinator Get and Put likewise share one request enum. Request arrival determines which operation linearizes first. Parallel phases send all requests before receiving any response, and collect replies in arrival order. Preparation collects all responses even after a conflict. Validation returns at the first conflicting response and leaves remaining replies unreceived. Finished handles wait on `Never`, leaving both abandoned validation replies and unawaited recovery completions queued. Recovery workers also wait on `Never` after their single job.

## Record representation and protocol

`Record` contains twelve independently nullable columns. Indices 0..5 are value, transaction ID, state, version, prepared_at, committed_at; indices 6..11 are their before-image counterparts. t0 is represented by 0, and ti by i. The numeric state constants are local encodings, not serialized Java enum values. Initial after-image is `(0,t0,COMMITTED,1,0,0)`; all before columns are null.

`Condition::Always` implements unconditional mutation (unused by these clients); `Condition::Absent` implements put-if-not-exists. `Condition::All` implements a conjunction of equalities, with `None` meaning IS NULL. It requires a record to exist. A Put patches exactly the listed columns and preserves all others; Delete removes the whole record. A failed condition changes nothing. No extra rejection, storage failure, retry, or nondeterministic failure is invented.

Preparation writes the user's value, own ID, PREPARED, prepared_at=0, read version+1, and all six before columns copied from the resolved read. Its sole predicate for existing records is the read transaction ID, not version or state. It deliberately leaves committed_at unchanged. An absent read produces version 1 and put-if-not-exists. Commit changes state to COMMITTED, committed_at=0 and nulls the before columns, under own-ID AND PREPARED. Rollback restores all six columns from the before image and nulls its before columns; if no before image exists it deletes. Its predicates use the observed writer ID AND observed record state. Conditional recovery and finalization failures are absorbed.

A physical committed read is cached directly. A physical prepared/deleted read asks the coordinator. COMMITTED produces the committed after image and starts rollforward; ABORTED produces the before image and starts rollback. An absent state invokes a fresh `nondet()` expiry check. False aborts this read without aborting the writer. True re-reads the record: absent/committed returns directly, a different uncommitted writer restarts resolution. The same writer is synchronously force-aborted with coordinator put-if-absent. A conflicting Put restarts resolution. A successful abort is followed by the source's second physical re-read, with the same absent/committed/different-writer checks; otherwise the original before image is returned and a rollback job is submitted. The retry limit is five passes, checked only at retry points (not on immediate success).

Commit first waits for required recovery completions. It prepares all writes, waiting for every mutation. Preparation failure writes ABORTED (reading the persisted state on conflict), reads all written keys, and conditionally rolls back only records still owned by this transaction. SERIALIZABLE then physically re-reads Get-set keys outside the write set; presence or transaction ID change conflicts, irrespective of record state/version/value. Validation failure follows the same abort path. A read-only transaction validates under SERIALIZABLE but writes no coordinator state and prepares/finalizes no records. A successful COMMITTED put-if-absent commits all records inline; a conflicting state write re-reads state, accepting COMMITTED and rolling back on ABORTED. No cleanup or group commit occurs.

## Method mapping

Multiple Java methods are intentionally combined where they merely delegate, construct data, or transform checked exceptions. Entries describe the reachable branches of this cut, not unrelated methods in the same file.

| Java method and source lines | Rust function / representation |
|---|---|
| `ConsensusCommitManager.begin` 303–330 | `handle`, `Snapshot::begin`, `Setup::ids/readonly` |
| `ConsensusCommit.get` 79–81 | `handle`, `get` |
| `ConsensusCommit.upsert` 198–204 | `handle` Write branch, `Snapshot::writes` |
| `ConsensusCommit.commit/commitInternal` 227–270 | `handle`, `commit`, `wait_for_recovery_completion` |
| `ConsensusCommit.rollback/releaseResources` 272–295 | End of `handle`; no resources remain to release in this cut |
| `TransactionContext.isSnapshotReadRequired` 69–71 | `get` always caches under both supported isolation levels |
| `TransactionContext.isValidationPossiblyRequired` 73–75 | `Setup::serializable`, `wait_for_recovery_completion` |
| `TransactionContext.isValidationRequired` 77–100 | `validate_records` checks isolation and selects read-but-not-written keys |
| `Snapshot.putIntoReadSet/putIntoGetSet` 129–137 | `get`, `Snapshot::reads/gets` |
| `Snapshot.putIntoWriteSet` 157–226 | `handle` Write branch; replacing the one value column implements merge |
| `Snapshot.getResult` 307–315 | `get`, `Snapshot::merge_result` |
| `Snapshot.mergeResult` 324–354 | `Snapshot::merge_result` overlays writes on cached reads |
| `Snapshot.to` 371–382 | `prepare_records`, `commit_records`, `rollback_records` iterate writes |
| `Snapshot.toSerializable` 640–696, especially 670–693 | `validate_records` sends all applicable Gets |
| `Snapshot.validateGetResult` 872–898 | `validate_records` receives raw physical records |
| `Snapshot.isChanged` 1004–1017 | Presence and ID comparison in `validate_records` |
| `Snapshot.throwExceptionDueToAntiDependency` 1019–1022 | `validate_records` returns false |
| `CrudHandler.get` 97–124 | `get` |
| `CrudHandler.readUnread` 204–213 | `Snapshot::gets` check in `get` |
| `CrudHandler.read` 215–303, single-row 241–253 | `get`, `Io::get`, snapshot registration |
| `CrudHandler.recoverAndFilter` 396–436 | `get` dispatches uncommitted records to resolution; no predicates/index filters in this instance |
| `CrudHandler.executeRecovery` 458–483 | `resolve_latest_result_and_recover`, `submit_recovery` |
| `CrudHandler.putIntoReadSetInSnapshot` 598–604 | `get` updates `Snapshot::reads` |
| `CrudHandler.put` 682–703 | `handle` Write branch |
| `CrudHandler.waitForRecoveryCompletionIfNecessary` 787–797 | `wait_for_recovery_completion` |
| `CrudHandler.waitForRecoveryCompletion` 805–836 | Typed `RecoveryDone` receive and Result propagation |
| `CrudHandler.getFromStorage` 838–851 | `Io::get` |
| `RecoveryExecutor.execute` 123–180, latest-result branch 133–134 | `get`, `resolve_latest_result_and_recover` |
| `RecoveryExecutor.resolveLatestResultAndRecover` 205–338 | `resolve_latest_result_and_recover` |
| `RecoveryExecutor.checkReadRetryLimit` 340–349 | Five-pass loop and retry-point checks |
| `RecoveryExecutor.getCoordinatorState` 351–361 | `Io::get_state` |
| `RecoveryExecutor.createRecoveredResult` 363–374 | State match in `resolve_latest_result_and_recover` |
| `RecoveryExecutor.throwUncommittedRecordExceptionIfTransactionNotExpired` 376–397 | `is_transaction_expired`, `ReadError::Conflict` |
| `RecoveryExecutor.createRecordFromBeforeImage` 399–424 | `Record::before` |
| `RecoveryExecutor.createResultFromAfterImage` 426–461 | `Record::after` |
| `RecoveryExecutor` background submissions 139,223,326; `Result` 578–600 | `submit_recovery`, `RecoveryJob`, `Snapshot::jobs`, `RecoveryDone` |
| `RecoveryHandler.tryRecover` 73–81 | `try_recover` |
| `RecoveryHandler.rollRecordToState` 151–160 | Decision match in `try_recover` |
| `RecoveryHandler.rollbackRecord` 162–195 | `rollback_mutation`, `Io::mutate` with failure ignored |
| `RecoveryHandler.rollforwardRecord` 207–241 | `commit_mutation`, `Io::mutate` with failure ignored |
| `RecoveryHandler.tryAbortExpiredTransaction` 268–280 | `Io::put_state(tx, Aborted)` in read resolution |
| `RecoveryHandler.abortIfExpired` 295–378 | `abort_if_expired` (best-effort branch) |
| `RecoveryHandler.mutate` 399–427 | `Io::mutate`; no retriable failures in the interface |
| `RecoveryHandler.isTransactionExpired` 429–432 | `is_transaction_expired` uses fresh `nondet()` |
| `CommitHandler.commit` 155–231 | `commit` |
| `CommitHandler.abortStateAndRollbackRecordsIfNeeded` 241–250 | Failure branches in `commit`, gated by `has_writes` |
| `CommitHandler.isCoordinatorStateWriteRequired` 256–258 | `has_writes` gates state writes |
| `CommitHandler` phase delegations 265–283; state delegations 303–313 | `prepare_records`, `validate_records`, `commit_records`, `rollback_records`, `commit_state`, `abort_state` |
| `ParticipantCommitHandler.prepareRecords` 76–102 | `prepare_records` |
| `ParticipantCommitHandler.validateRecords` 104–118 | `validate_records` |
| `ParticipantCommitHandler.commitRecords/doCommitRecords` 120–140 | `commit_records` |
| `ParticipantCommitHandler.rollbackRecords/doRollbackRecords` 142–173 | `rollback_records` |
| `ParticipantCommitHandler.tryRecoverRecordsBlockingWrites` 202–235; `getKeysOfWritesNeverRead` 244–252 | Empty set by prescribed client loops; documented at end of `rollback_records` |
| `ParticipantCommitHandler.toReadTasks/toTasks` 274–293 | `Io::send_get/send_mutation` phase loops |
| `CoordinatorCommitHandler.commitState` 50–67 | `commit_state` |
| `CoordinatorCommitHandler.handleCommitConflict` 76–167 | `commit_state` state read after failed Put |
| `CoordinatorCommitHandler.abortState` 173–195; `readCoordinatorStateAfterAbortConflict` 241–252 | `abort_state` |
| `CoordinatorStateAccessor.getState` 163–169 | `Io::get_state` |
| `CoordinatorStateAccessor.putState` 214–217 | `Io::put_state` |
| `CoordinatorStateAccessor.forceAbort` 219–228 | `Io::put_state(tx, Aborted)` |
| `CoordinatorStateAccessor.createGetWith/get` 335–376 | `CoordinatorRequest::Get`, `Io::get_state`, `coordinator` |
| `CoordinatorStateAccessor.createPutWith/put` 379–437 | `CoordinatorRequest::Put`, `Io::put_state`, `coordinator` |
| `CoordinatorStateAccessor.State` 489ff | `(Decision,u64)` table entries keyed by logical ID |
| `AbstractMutationComposer` 20–33ff | `Mutation`, patch vectors; before-null patches in commit/rollback composers |
| `PrepareMutationComposer.add(Put)` 47–84 | `prepare_mutation` |
| `PrepareMutationComposer.createBeforeColumns/isBeforeRequired` 123–148 | Six-column copy in `prepare_mutation` |
| `CommitMutationComposer.add` 39–50, selection add 60–77, `composePut` 79–108 | `commit_mutation`, caller's record state dispatch |
| `RollbackMutationComposer.add/getLatestRecord` 62–104 | `rollback_records` physical read / ownership check, or direct `try_recover` record |
| `RollbackMutationComposer.composePut/composeDelete` 106–165 | `rollback_mutation` |
| `TransactionResult` ID/state/version/prepared_at accessors 153,167,176,180 | `Record` column indexes |
| `TransactionResult.isCommitted/isDeemedAsCommitted/hasBeforeImage` 200–232 | `Record::committed/before`; deemed-committed input excluded by initialization |
| `Attribute` 4–17 | Twelve-column layout and index constants |
| `ConsensusCommitUtils.createPutForUpsert` 365–375 | Write branch stores the supplied value |
| `ConsensusCommitUtils.rereadRecord` 471–484 | `Io::get` in read resolution / expiry recovery |
| `ConsensusCommitUtils.createGet/prepareGetForStorage` 494–500,537–549 | `StorageRequest::Get` with fixed key |
| `ParallelExecutor.prepareRecords` 67–89 | `prepare_records`, `collect_mutations` waits for all |
| `ParallelExecutor.validateRecords` 91–100 | `validate_records` stops on first conflict |
| `ParallelExecutor.commitRecords/rollbackRecords` 102–122 | `commit_records/rollback_records`, collect all replies |
| `ParallelExecutor.readRecordsForRollback` 125–140 | `collect_get`; no read exceptions are possible |
| `ParallelExecutor.executeTasks/executeTasksInParallel` 248–316 | Phase send loops and typed tagged collect loops; one request naturally gives one receive |
| `AsyncExecutor.commitRecords/rollbackRecords` 85–106 | Inline `commit_records/rollback_records` |
| `MutationsGrouper.groupMutations` 29–52 | One request per key/partition |
| `api/DistributedStorage.get/mutate` 146,203–217 | `storage`, `StorageRequest`, `Condition::matches` |
| `api/ConditionBuilder.putIf/putIfNotExists/deleteIf/column` 33–83,161 | `Condition`, `Mutation` |

## Decisions, replacements, and limitations

1. Apply all ten substitutions in `cc-src/README.md`: message-passing in-memory stores and coordinator; no executor pools; messages for recovery Futures; logical IDs and zero timestamps; fresh nondeterministic expiry; one fixed table/value column; Result errors and prescribed boundary replies; excluded features stay excluded. These are interface requirements, not new behavior inferred from the implementation.
2. Container iteration order is unspecified by the source's map/set abstractions (`Snapshot` 129–137,157–226,371–382,670–693; `ParallelExecutor` 268–316). Phase request issuance uses increasing key order except rollback mutation issuance follows completion order of its preceding reads. Completion order is message arrival. No extra scheduler nondeterminism is added. Recovery waits follow registration/read order (`CrudHandler` 789–796). This implements the interface's send-all abstraction; individual Java task-thread scheduling is replaced, not separately modeled.
3. Read and Get sets share one record vector and a separate presence/cached bit because this instance has exactly one plain Get per key, no predicates or indexes (`CrudHandler` 241–253,280–300; `Snapshot` 129–137). Stored absence remains distinct from an unread key via `gets`.
4. Upsert merging reduces to last value wins for the only user column (`Snapshot` 191–223,324–354); workspace reads overlay it. No deletes, inserts, operation conditions or implicit pre-read occur, as excluded by the interface. `Record::before`, rollback Delete and commit DELETED handling remain represented, though these patterns never make records absent: all keys start present and all prepared writes have before images. `merge_result` consequently asserts that a visible Value exists rather than inventing a nonexistent-key value; the fixed `Rep` has no absent-value reply.
5. All reachable transaction metadata is nonnull; independent nulls are still representable in storage. The source's deemed-committed NULL-ID/version branch (`PrepareMutationComposer` 61–71,123–139; `TransactionResult` 219–232) is unreachable from the required initialization. The null-version-to-before-version-0 special case is therefore omitted. Generic IS NULL comparison is implemented.
6. Coordinator state access has exactly one attempt because no transient/unknown storage failures exist (`CoordinatorStateAccessor` 344–376,400–437). Retry backoff and the multiple-attempt ambiguity branches (`CoordinatorCommitHandler` 76–167) are unreachable. `Unknown` remains in the exact required message enum; no branch invents it. Coordinator COMMITTED after a conflict is handled, even though only the owner can write COMMITTED and it writes once here. Coordinator rows are never removed.
7. Self-abort reads state on a conflicting Put (`CoordinatorCommitHandler` 173–195,241–252). The returned state is unused by `CommitHandler` on the precommit failure path (181–184,241–250), so `abort_state` discards it after the exchange. A foreign COMMITTED state cannot arise before the owner's commit-state phase in this interface. Rollback then restores only records owned by the transaction.
8. Preserve both expiry-path physical re-reads (`RecoveryExecutor` 243–313), even though the comments motivate them partly by excluded cleanup. Preserve restart on a different prepared writer and five-pass retry bounds (`RecoveryExecutor` 205–349). A successful synchronous expired-writer abort starts a direct rollback job; it does not check expiry again (`RecoveryExecutor` 319–330).
9. `try_recover` includes the absent-state best-effort `abort_if_expired` path (`RecoveryHandler` 73–81,295–378), with its fresh expiry choice, physical re-read, put-if-absent, conflict-absorbed return and rollback. Normal latest-result read jobs always carry a resolved state (`RecoveryExecutor` 219–228,319–330), so this optional path is unreachable in the prescribed workloads. The guaranteed `recover(...ensureRecordResolved=true)` public path is outside this cut and omitted.
10. No mutation retry loop is necessary (`RecoveryHandler` 399–427): only NoMutation is possible and the rollforward/rollback methods swallow it (162–195,207–241). `RecoveryDone` still carries a Result, but every reachable recovery job completes with Ok. No nondeterminism is introduced for nonexistent failures.
11. Blocking-write recovery's never-read key set is empty (`ParticipantCommitHandler` 202–252): every client reads all keys before writing. The implementation records this proof at the end of rollback instead of creating extra workers or jobs. Read failures end clients before writes.
12. Transaction `rollback()` releases resources, not prepared records (`ConsensusCommit` 272–295). Its no-op meaning is represented by finishing the handle's transaction and waiting forever. Participant rollback is a separate physical protocol, implemented explicitly. Scanners, hooks, group slots, logging/metrics and resource wrappers do not exist in this instance.
13. Initialization and numeric encoding are fixed choices (`ConsensusCommitManager` 303–330 leaves UUID/context construction to the replaced setup). Versions increment normally; no overflow is reachable with at most three transactions. Every source clock value is 0. No wall-clock timer, failure, timeout, cleanup process, coordinator write-set, one-phase commit or group-commit branch is modeled.
14. The optional trailing delivery-model spelling is accepted and validated. `main` uses the shared runner's `main_with_model` for the trailing `bag` default, and otherwise uses `main`; explicit `--model` overrides the default (`api/program.rs` 77–98, `parse_opts`, `config_of`). All requested commands use explicit `--model fifo`. Both entries intentionally port the implementation, as explicitly requested; conformance of implementation against itself is not an independent correctness check.
15. The source cut does not provide exception class definitions and several API carriers (`cc-src/README.md`, Known limitations). Only distinctions observable under the supplied successful-or-NoMutation storage abstraction are retained: read conflict, failed prepare/validation, coordinator conflict, and required boundary replies. No exception behavior was guessed for excluded backend failures.

## Build and runs

Required command (run from OUT_DIR):

```sh
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
```

The initial attempt failed because crates.io DNS was unavailable. Cached dependencies were used with:

```sh
TMPDIR=/local/home/mkhoshechin/tmp cargo build --offline --release -j 2
```

The results of the five requested commands appear below. Infinite protocol services are intentional; TraceForge's exploration termination/block statistics are reported unchanged by the shared runner, rather than treating client termination as service termination.

Message-selection correction from the first smoke run: Must's generic receive type checks the selected payload but does not itself filter the mailbox by Rust type. The initial lost/1 invocation therefore stopped with `wrong message return type; expecting ...::Setup but got ...::Req`. All setup receives now filter reserved `SETUP_TAG`, operation responses filter monotonically allocated IDs, recovery completions filter a separate `RECOVERY_TAG + key` range, and finished processes filter reserved `NEVER_TAG`. Client requests are untagged, and handle request receives select untagged messages. This keeps late completions and responses unreceived even when the handle has finished. Setup is sent with its reserved tag. The corrected runs below supersede that unsuccessful smoke run.

Final build after the message-selection correction and interface checks:

```text
$ TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
    Finished `release` profile [optimized] target(s) in 3.88s
```

The final build exited 0 without warnings. The cache populated by the offline build allowed the exact required build command to succeed.

The following runs use the final binary; each exited 0, and each measurement block reported `stop_reason: exhausted`. All executions are blocked because the required persistent service threads wait forever; these are counts of the explored blocked executions, not counts of normally terminating client histories. No timeout fired.

```sh
timeout 600 target/release/bench_scalardb_sol count impl --model fifo -- lost 1 snapshot
```

Exit status: 0. Count and selected measurements:

```text
execs=0 blocked=74
stop_reason: exhausted
max_graph_events: 141
```

Last output lines (blank values are the runner's output):

```text
uncovered_distinct: 0
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```

```sh
timeout 600 target/release/bench_scalardb_sol count impl --model fifo -- lost 2 snapshot
```

Exit status: 0. Count and selected measurements:

```text
execs=0 blocked=94
stop_reason: exhausted
max_graph_events: 172
```

Last output lines (blank values are the runner's output):

```text
uncovered_distinct: 0
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```

```sh
timeout 600 target/release/bench_scalardb_sol count impl --model fifo -- skew 2 snapshot
```

Exit status: 0. Count and selected measurements:

```text
execs=0 blocked=65
stop_reason: exhausted
max_graph_events: 171
```

Last output lines (blank values are the runner's output):

```text
uncovered_distinct: 0
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```

```sh
timeout 600 target/release/bench_scalardb_sol count impl --model fifo -- skew 2 serializable
```

Exit status: 0. Count and selected measurements:

```text
execs=0 blocked=156
stop_reason: exhausted
max_graph_events: 180
```

Last output lines (blank values are the runner's output):

```text
uncovered_distinct: 0
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```

```sh
timeout 600 target/release/bench_scalardb_sol count impl --model fifo -- mixed 2 snapshot
```

Exit status: 0. Count and selected measurements:

```text
execs=0 blocked=1834
stop_reason: exhausted
max_graph_events: 199
```

Last output lines (blank values are the runner's output):

```text
uncovered_distinct: 0
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```
