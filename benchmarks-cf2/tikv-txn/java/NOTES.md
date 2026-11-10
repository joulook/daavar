# TiKV transactions, Java port (bench.tikv.TxnKv)

Source: client tikv/client-rust 1b883bf, server tikv/tikv 96033a2, read through the cut
(plan/case-studies/tikv/cut/README.md) and the agents' interface (INTERFACE-agents.md). Every line that
differs from the source is marked `// PORT:` with the rule (A1-A6, T1-T6, P1-P2 of the README section 3)
or the reason; the comments name the ported source function and its lines.

Files: `java/bench/tikv/TxnKv.java` (the whole program), this file.

## Compile and run

Compile (in OUT_DIR, `TMPDIR=/local/home/mkhoshechin/tmp`):

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Run (in OUT_DIR), one size per invocation:

    java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,S1,0,0,1,fifo' must 0 1 report - 900 </dev/null

The launcher writes its report directory `report/` in the working directory; it is deleted after the
runs (not a deliverable).

## Program shape (INTERFACE.md sections 1-2, 9)

- `TxnKv(String...)` takes `impl,<scenario>,<async>,<onepc>,<a>,<fifo|bag>[,<variant>]`; refuses `async = 1`
  with `onepc = 1` (IllegalArgumentException), an unknown scenario, variant or model.
- `programName()` = `tikv_txn`, `size()` = `<scenario>,<async>,<onepc>,<a>`, `model()` P2P (fifo) / ASYN (bag),
  `rolesInSpawnOrder()` = `visibleRoles()` = `c1..cm`.
- `run()`: spawns `r1..rR`, `pd`, `x1..xm`, `t1..tm` with `MpProcess.spawnDaemon`, `c1..cm` with
  `MpProcess.spawn("ci", ...)`, then `init` (daemon), then awaits `c1..cm`. Main sends and receives nothing.
  Layout: `onepc = 1` -> `single` (R = 1), else `split` (R = n, key kj on rj).
- `init` sends one `Setup` (all addresses, the layout, the kinds, async, onepc, a) to every region, pd, every
  xi and ti. Each invisible process receives its `Setup` first (typed receive), then loops forever.
- Visible records: verbatim from INTERFACE.md section 4 with their `@Tag`. Client code: verbatim (5.1).
- Invisible messages: `TsoReq(from, id)` / `TsoResp(id, ts)` (pd), `KvReq(from, id, body)` / `KvReply(id, body)`
  (regions; body = one of the kvrpcpb request / response records), `XTrigger` (ti -> xi). The sender puts
  its own address (`MpProcess.self()`, taken outside any guard) and a per-process request id in each
  request; every receive of a reply is guarded by the id, so ti and xi take exactly the reply they wait for.

## Function mapping

| Source (file:lines) | Java |
|---|---|
| client.rs `begin_with_options` 226-230, `current_timestamp` 250-252 | `tMain` (Begin branch), `Lib.getTimestamp` |
| transaction.rs `Transaction::new` 100-123 | `Transaction(..)` |
| transaction.rs `get` 143-164 | `Transaction.get` |
| transaction.rs `get_for_update` 208-224 | `Transaction.getForUpdate` |
| transaction.rs `put` 470-480 | `Transaction.put` |
| (PORT P2) ti's WriteConflict retry | `Transaction.pessimisticLockWithRetry`, `isWriteConflict` |
| transaction.rs `commit` 653-706 | `Transaction.commit` |
| transaction.rs `pessimistic_lock` 859-940 | `Transaction.pessimisticLock` |
| transaction.rs `pessimistic_lock_rollback` 943-980 | `Transaction.pessimisticLockRollback` |
| transaction.rs `check_allow_operation` 983-992 | `Transaction.checkAllowOperation` |
| transaction.rs `TransactionOptions` 1160-1270 | `TransactionOptions` |
| transaction.rs `Committer::commit` 1340-1407 | `Committer.commit` |
| transaction.rs `Committer::prewrite` 1409-1483 | `Committer.prewrite` |
| transaction.rs `Committer::commit_primary` 1486-1542 | `Committer.commitPrimary` |
| transaction.rs `commit_primary_with_retry` 1544-1584 | `Committer.commitPrimaryWithRetry` |
| transaction.rs `commit_secondary` 1586-1648 | `commitSecondary` (run by `xMain`) |
| transaction.rs `calc_txn_lock_ttl` 1711-1720 | `Committer.calcTxnLockTtl` |
| lock.rs `resolve_locks` 86-151 | `Lib.resolveLocks` |
| lock.rs `LockAction`, `from_status` 153-187 | `Lib.LockAction` |
| lock.rs `pessimistic_rollback_lock` 189-217 | `Lib.pessimisticRollbackLock` |
| lock.rs `resolve_lock_with_retry` 223-317 | `Lib.resolveLockWithRetry` |
| lock.rs `ResolveLocksContext` (resolved cache) | `Lib.LockResolver` |
| lock.rs `action_for_lock` 381-482 | `Lib.actionForLock` |
| lock.rs `check_txn_status` 615-689 | `Lib.checkTxnStatus` |
| lock.rs `check_all_secondaries` 691-708 | `Lib.checkAllSecondaries` |
| lock.rs `get_txn_status_from_lock` 729-794 | `Lib.getTxnStatusFromLock` |
| lock.rs `lock_until_expired_ms` 805-807 | `lockUntilExpiredMs` (T2) |
| requests.rs constructors 95-420, 631-652, 794-802 | record constructors at the call sites (`GetReq`, `PrewriteReq`, ...) |
| requests.rs `Shardable for PrewriteRequest` 254-294, `shardable_key(s)!` (shard.rs 184-243), CommitRequest 323-351, PessimisticLockRequest 425-444, CheckTxnStatus 655-676 | `Lib.shards`, `Lib.shardsOf`, `Lib.applyShard` |
| requests.rs `CollectWithShard` merge 448-510 | `Transaction.collectWithShard` |
| requests.rs `TransactionStatus`, `check_ttl`, `is_determined`, `From<..>` 701-795 | `TransactionStatus` |
| requests.rs `CollectSecondaryLocks` 813-880, `SecondaryLocksStatus` 880-924 | `Lib.collectSecondaryLocks`, `SecondaryLocksStatus` |
| requests.rs `HasLocks` impls 925-966 | `Lib.takeLocks` |
| plan.rs `Dispatch` 50-84 | `Lib.send`, `Lib.dispatchOne` |
| plan.rs `single_plan_handler` 375-451 (`join_set.spawn` 398), `collect_join_set_results` 108-153, `RetryableMultiRegion::execute` 1042-1060 | `Lib.retryMultiRegion` (one daemon per shard, `ShardDone`), `Lib.nonPreserve` |
| plan.rs `single_shard_handler` -> `handle_candidate_response` 785-812 | `Lib.singleShardHandler`, `Lib.handleCandidateResponse` |
| plan.rs `ResolveLock::execute` 1267-1308 | `Lib.singleShardHandler` (the body of one shard task) |
| plan.rs `CollectSingle` 1183-1197, `CollectError` 1208-1214, `ExtractError` 1498-1545, `PreserveShard` 1555-1585 | `Lib.collectSingle`, loop in `Committer.prewrite`, `Lib.extractError`, `ShardOut.shard` |
| buffer.rs `get_primary_key`, `primary_key_or`, `get_or_else`, `lock`, `unlock`, `put`, `to_proto_mutations`, `update_cache`, `insert_entry` | `Buffer.*` (same names) |
| backoff.rs 10-13, `next_delay_duration`, `is_none` | `Backoff`, `defaultRegionBackoff`, `optimisticBackoff`, `pessimisticBackoff` |
| storage/mod.rs `get_entry` 634-826, `prepare_snap_ctx` 3573-3634, point_getter.rs 173-421 | `Region.kvGet` |
| txn_types lock.rs `check_ts_conflict_si` 363-422, `into_lock_info` 622-645 | `Region.checkTsConflictSi`, `Lock.intoLockInfo` |
| commands/mod.rs 164-384 (request -> command) | `Region.handle`, `Region.kvPrewrite` (for_update_ts == 0 test) |
| commands/prewrite.rs `Prewriter::process_write` 501-562, `prewrite` 585-719, `write_result` 722-806, `can_skip_constraint_check` 841-865, `one_pc_commit` 951-967, `handle_1pc_locks` 971-991, `fallback_1pc_locks` 994-1000 | `Region.Prewriter.*`, `Region.onePcCommit`, `handle1pcLocks`, `fallback1pcLocks` |
| actions/common.rs `check_committed_record_on_err` 70-91 | `Region.checkCommittedRecordOnErr` (see D5) |
| actions/prewrite.rs `prewrite_with_generation` 58-286 | `Region.prewriteAction` |
| actions/prewrite.rs `PrewriteMutation` (`from_mutation`, `lock_info`, `check_lock`, `check_for_newer_version`, `write_lock`, `write_conflict_error`, `skip_constraint_check`, `try_one_pc`) | `Region.PrewriteMutation.*` |
| actions/prewrite.rs `async_commit_timestamps` 928-979, `amend_pessimistic_lock` 984-1035 | `Region.asyncCommitTimestamps`, `Region.amendPessimisticLock` |
| commands/acquire_pessimistic_lock.rs 82-220; actions 44-333, 358-464 | `Region.kvPessimisticLock`, `acquirePessimisticLock`, `handleExistingExclusiveLock` |
| commands/commit.rs 53-97; actions/commit.rs 19-62, 64-245 | `Region.kvCommit`, `handleLockNotFound`, `commit` |
| commands/check_txn_status.rs 87-189 | `Region.kvCheckTxnStatus` |
| actions/check_txn_status.rs 17-68, 92-217, 223-239, 241-297, 299-357, 423-434, 438-455, 464-496 | `checkTxnStatusFromPessimisticPrimaryLock`, `checkTxnStatusLockExists`, `checkDeterminedTxnStatus`, `checkTxnStatusMissingLock`, `rollbackLock`, `collapsePrevRollback`, `makeRollback`, `MissingLockAction` |
| commands/check_secondary_locks.rs 67-92, 94-145, 148-254 | `checkDeterminedTxnStatusSec`, `checkStatusFromLock`, `kvCheckSecondaryLocks` |
| commands/resolve_lock_readphase.rs 51-113 | `Region.kvResolveLockReadPhase` (D2) |
| commands/resolve_lock.rs 82-173 | `Region.kvResolveLockWrite` (D2), run when `regionMain` takes the `ResolveLockNext` |
| actions/cleanup.rs 24-135 | `Region.cleanup` |
| commands/pessimistic_rollback.rs 60-156 | `Region.kvPessimisticRollback` |
| mvcc/txn.rs `MvccTxn` 66-274 | `MvccTxn` |
| mvcc/reader `TxnCommitRecord`, `seek_write`, `get`, `get_write(_with_commit_ts)`, `load_data`, `load_lock`, `get_txn_commit_record` | `TxnCommitRecord`, `Region.Reader.*` |
| concurrency_manager lib.rs `max_ts` 221-223, `update_max_ts` 235-272 | `Region.maxTs`, `Region.updateMaxTs` |

## Decisions and simplifications

D1 (shard fan-out; revised in review round 1, finding 1). `RetryableMultiRegion::single_plan_handler`
spawns one task per shard (plan.rs:398), and each task runs the cloned plan `ResolveLock<Dispatch>`
(`resolve_lock(..)` inside `retry_multi_region(..)`, plan_builder.rs:74-95, 173-264; `ResolveLock::execute`
plan.rs:1267-1308) to its end, so in the source the lock resolutions of two shards interleave. The port
now does the same: for a plan with `.resolve_lock(..)` and more than one shard, `Lib.retryMultiRegion`
spawns one invisible daemon per shard (`MpProcess.spawnDaemon`, its own address and request ids, a
`Lib` sharing pd and the regions); the task runs `Lib.singleShardHandler` (dispatch, `resolve_locks`,
re-dispatch, backoff bound, `handle_candidate_response`) and sends `ShardDone(plan, idx, out)` to its
spawner, which joins the tasks. Nested plans inside a task (CheckTxnStatus, CheckSecondaryLocks,
PessimisticRollback, ResolveLock) run in that task, as the source's awaits do. Three schedule-preserving
choices, each marked `// PORT:`:
- a plan with one shard runs inline in the caller: the caller does nothing between spawn and join
  (plan.rs:398-420), and a single shard has no sibling to interleave with;
- the caller joins the tasks in shard order, not in `join_next` order: `collect_join_set_results` only
  stores `results[idx]` until every task has ended (plan.rs:117-133), so the join order is not observable;
- a plan without `.resolve_lock(..)` (check_all_secondaries, pessimistic_rollback_lock,
  check_txn_status's plan) keeps the caller-side fan-out: its task is one Dispatch and one reply, the
  shard requests go to distinct regions, so "send all, receive in any order" is the tasks' schedule.
The shard tasks are unnamed invisible processes (no role), next to the fixed roles of INTERFACE.md
section 2; that section and section 6 describe ti as "one sequential process" that "sends ... then
receives in any order"; the coordinator's instruction for this round (shard scheduling as the source
does it) takes precedence over that wording here (see "Review round 1").

D2 (ResolveLock read and write phase; revised in review round 1, finding 2). A ResolveLock without
keys becomes `ResolveLockReadPhase` (commands/mod.rs:375), whose `process_read` scans the region's locks
of the given start_ts and returns `ProcessResult::NextCommand { ResolveLock }`
(resolve_lock_readphase.rs:51-113, 101-111); the scheduler later runs that write command on a snapshot
of its own (resolve_lock.rs:82-173). Two commands, so two steps of the region now: `regionMain` runs
`kvResolveLockReadPhase`; if it finds nothing it replies at once (`ProcessResult::Res`, :92-93), else it
sends the `ResolveLockNext` command (the scanned key/lock pairs, only the lock's ts and type are kept:
the write phase reads nothing else of `current_lock`) to its own mailbox, and the loop takes the next
message, request or `ResolveLockNext`, so any number of other commands may run between the phases;
`kvResolveLockWrite` evaluates the write phase on the cell state of its own start and replies to the
original sender. `// PORT: A1, the region's scheduler queue is its own mailbox`. The scan's
RESOLVE_LOCK_BATCH_SIZE and the MAX_TXN_WRITE_SIZE re-scan are cut (small regions: `has_remain` false,
`scan_key` None); latches are cut (A1).

D3 (wire mappings not in the cut). service/kv.rs is not in the cut; the following mappings were mine,
from the field names the client reads. Since review round 1 the pinned kv.rs and storage/errors.rs are on disk and every mapping
here was checked against them: see "Response shapes against kv.rs and store/errors.rs" under Review
round 1 (all agree; no code change).
- storage error -> `KeyError`: KeyIsLocked -> `locked`, WriteConflict -> `conflict`, TxnNotFound ->
  `txn_not_found`, PrimaryMismatch -> `primary_mismatch` (its lock info in `locked`), CommitTsExpired ->
  `commit_ts_expired` (key, min_commit_ts, attempted_commit_ts); all other kinds carry only their name.
  An error of a command is the response's `error` / `errors = [e]`, and nothing of that command is written.
- `TxnStatus` -> `CheckTxnStatusResponse` (`txnStatusResp`): RolledBack -> NoAction; TtlExpire ->
  TtlExpireRollback; LockNotExist -> LockNotExistRollback; Committed{ts} -> commit_version = ts;
  PessimisticRollBack -> TtlExpirePessimisticRollback; LockNotExistDoNothing -> LockNotExistDoNothing;
  Uncommitted{lock, pushed} -> lock_ttl = lock.ttl, lock_info = lock, action MinCommitTsPushed if pushed.
- `SecondaryLocksStatus` -> `CheckSecondaryLocksResponse`: Locked(locks) -> (locks, 0), Committed(ts) -> ([], ts),
  RolledBack -> ([], 0) (the client's merge, requests.rs:853-875, reads a short lock list as "decided").
- PessimisticLock: `values` / `not_founds` filled only when `return_values`; a missing value is
  `not_found = true` with value 0.
- Prewrite: the collected KeyIsLocked errors are `errors`; `min_commit_ts` and `one_pc_commit_ts` as in
  `PrewriteResult`.

D4 (HasKeyErrors). The `HasKeyErrors` impls live in client-src/src/store/errors.rs (fetched by the
coordinator during review round 1; checked, see Review round 1). A response's key
errors become `Err(MultipleKeyErrors([KeyError..]))` in `handle_candidate_response` (plan.rs:806-808), which
is in the cut; `ExtractError` / `CollectError` over a `Vec<Result<_>>` are ported as "the first Err item is
the plan's error, unchanged". `commit_primary_with_retry` and `check_txn_status` then see
`MultipleKeyErrors` and pop its last element, as the source does.

D5 (a file outside the cut list). `check_committed_record_on_err` (actions/common.rs:70-91) is called from
the listed lines commands/prewrite.rs:665-704 but common.rs is not listed in the README. I read those 22
lines to port the call; nothing else of common.rs. Review round 1 (finding 3) confirmed the helper and
its port; the cut README should list common.rs:70-91 (the coordinator's file, not changed here).

D6 (source not ported because no path of this round reaches it). `Transaction::rollback` and
`Committer::rollback` (P1: the application never rolls back; the README lists `Transaction::rollback` "for
`Committer::rollback` only"); the `BatchRollback` and `Cleanup` commands (only `Committer::rollback` /
`cleanup_locks` send them; the `cleanup` action is ported, it is used by ResolveLock); `ResolveLockLite`
(client-rust sends ResolveLock without keys); `PessimisticRollbackReadPhase` (keys are never empty);
TxnHeartBeat (T3); `cleanup_locks` / `batch_resolve_locks` (GC, not in the cut). Region-error handling,
`retry_after_error`, leader fallback, store / region cache invalidation: no region error is produced (A1).

D7 (GetForUpdate). Pessimistic `GetForUpdate` is ported (`pessimistic_lock(.., need_value = true)`, the
value of the last pair). Two cases are outside the cell model and throw `UnsupportedOperationException`
(no scenario of this round reaches them, INTERFACE.md section 3): an optimistic GetForUpdate (`lock_keys`
is not in the cut), and a commit whose buffer holds a key that was only locked (an `Op::Lock` mutation,
A2 admits only Put; `Buffer.toProtoMutations`). Completeness limit (review round 1, finding 8): INTERFACE.md
section 3 says an implementation "must still support" GetForUpdate, and section 6 admits only Put
mutations; a pessimistic script `GetForUpdate(k)` with no later `Put(k)` therefore cannot commit in this
port (it throws instead of sending an `Op::Lock` prewrite, buffer.rs:277-283). The Rust port emits
`Op::Lock` and rejects it at the region; both stop at the same point of the cut, neither runs such a
transaction. No scenario of this round has GetForUpdate.

D8 (timestamps). `long`; `TimeStamp::max()` / `u64::MAX` is `Long.MAX_VALUE` (`TS_MAX`); `next()` / `prev()`
are `+1` / `-1`. T1: pd answers 1, 2, ...

D9 (T2 sites). Server: `ttlExpired()` returns true at check_txn_status.rs:55 and :176 and in cleanup.rs:58-62
(the cleanup check is never reached: Rollback and ResolveLock pass current_ts 0). Client:
`lockUntilExpiredMs` returns -1 (lock.rs:769, 805-807), `TransactionStatus.checkTtl` sets `is_expired`
(lock.rs:682-683, requests.rs:734). The TSO requests that feed those predicates (lock.rs:768, 682) are kept,
so the message sequence is the source's. The TTL fields (DEFAULT_LOCK_TTL, MAX_TTL, `calc_txn_lock_ttl`,
the TTL-0 protocol of lock.rs:739-746 and actions/prewrite.rs:426-432) are kept.

D10 (T4 / backoff). `Backoff.nextDelayDuration` keeps the attempt bound (10) and never sleeps. The bound is
only consumed where the source consumes it (live locks in `ResolveLock::execute`, TxnNotFound of a live
optimistic lock in `get_txn_status_from_lock`); with every TTL expired neither path is taken, and
`ResolveLock::execute` re-dispatches without consuming a bound when `live_locks` is empty (plan.rs:1294-1295),
as the source does.

D11 (P2 retry). `Transaction.pessimisticLockWithRetry` calls `pessimistic_lock` again on a WriteConflict,
at most `a` attempts in total, for Put and GetForUpdate (INTERFACE.md 5.4). A WriteConflict reaches ti
wrapped: `CollectWithShard` makes `PessimisticLockError{inner, success_keys = []}` and `pessimistic_lock`
returns it unchanged when no key succeeded (transaction.rs:926, `_ => Err(err)`), so `isWriteConflict`
unwraps `PessimisticLockError` -> `MultipleKeyErrors` -> `KeyError.conflict`.

D12 (ti and xi). ti handles one client request at a time and replies `Aborted` for every `KvError` the
library returns (INTERFACE.md 5.2). The `tokio::spawn` of transaction.rs:1401-1405 is one `XTrigger`
(commit_ts, start_ts, every mutation key, the primary, the async flag) to the static xi; ti replies
`Committed` without waiting. xi runs `commit_secondary` and drops its error. xi used `optimisticBackoff()` as its lock backoff; since review round 1 (finding 7) the trigger carries the
transaction's `retry_options.lock_backoff` bound (`lockBackoffMaxAttempts`) and xi uses it, as
`commit_secondary` does (transaction.rs:1638-1642). Same bound as before (backoff.rs:12-13, both 10).

D13 (server state). A command reads the cell state as it was at the command's start (the snapshot) and
writes into an `MvccTxn` whose modifies are applied at the end only when the command succeeds
(`Region.apply`); `commit` and `cleanup` first look at the pending lock modifications
(`get_pending_lock_bytes`), as the source does. `max_ts` is the concurrency manager's memory, updated at
once and not undone when the command fails (as the source: prepare_snap_ctx and CheckTxnStatus update it
before evaluating). The in-memory lock table and key guards are the cell's lock (A3). Short values, gc_fence,
last_change, old values, assertions, generations, shared locks, `is_txn_file`, `txn_source`: cut (A2, A6);
a Rollback write is skipped by the readers (the `_` arm of the last_change match).

D14 (constants). `write_size` counts 2 bytes per Put (key and value are one byte each); it only feeds
`calc_txn_lock_ttl`, whose large-transaction branch is unreachable (16 KB). `FORWARD_MIN_MUTATIONS_NUM` = 12
(prewrite.rs:44): the skip-constraint-check branch is ported and unreachable (at most 2 mutations).

D15 (begin). Both kinds use `begin_with_options` (client.rs:226-230) with `new_optimistic` /
`new_pessimistic` plus `use_async_commit` / `try_one_pc`; `begin_optimistic` (174-181) is the same TSO call
with `new_optimistic()`.

D16 (source behaviour kept, noted). (a) `Shardable::apply_shard` for PrewriteRequest tests the unsharded
mutations for the primary (requests.rs:279), so every shard keeps the secondaries; a non-primary shard's
locks get `use_async_commit` with empty secondaries on the server (prewrite.rs:611-634). (b) Under async
commit `commit_ts = min_commit_ts.unwrap()` (transaction.rs:1387-1388, FIXME "min_commit_ts == 0 => fallback"),
where `min_commit_ts` is the maximum over the shards' replies (transaction.rs:1473-1480); corrected in
review round 1 (finding 6): one shard's 0 does not make it 0, only every shard answering 0 does, and
then the trigger carries commit_ts 0 (kept as the source has it, no fallback added). A shard answers 0
only after a fallback from async commit (commands/prewrite.rs:659-671 a lock already prewritten with
min_commit_ts 0, 682-697 CommitTsTooLarge), and the first fallback needs CommitTsTooLarge, which
`max_commit_ts = 0` disables (actions/prewrite.rs:957; the client never sets it, transaction.rs:1442
FIXME). So I find no zero-maximum path in this round's programs (an argument from the source, not a run
result). (c)
`commit_primary_with_retry`'s commit_ts_expired retry is kept (T5).

## Open questions

Q1. D1 (answered in review round 1: shard tasks run concurrently; done with one daemon per shard). Open
for the coordinator: INTERFACE.md sections 2 and 6 still describe ti as one sequential process with a
"send all, receive in any order" fan-out; the wording should admit the shard tasks.

Q2. D5 (answered: keep the helper; the README should list actions/common.rs:70-91).

Q3. D3 (answered: checked against kv.rs at the pin, Review round 1).

Q4. D4 (answered: checked against store/errors.rs at the pin, Review round 1).

## Runs (round 0, before the review; superseded by "Runs, before and after review round 1")

Local machine, 2026-11-09, two runs at a time, counting mode (`must`, no export, no `--spec`), so there is no
verdict line: each run ends with the graph counts. No run printed an exception or `FAILED:`; every graph
is complete, none blocked. `timeout` = the 900 s bound was hit with work left (the count is a lower bound).

| Size | graphs complete | blocked | end | wall |
|---|---|---|---|---|
| `impl,S1,0,0,1,fifo` | 431 | 0 | exhausted | 13.797 s |
| `impl,X2,0,0,1,fifo` | 10412 | 0 | exhausted | 326.578 s |
| `impl,X2r,0,0,1,fifo` | 19875 | 0 | timeout | 900.045 s |
| `impl,P2,0,0,1,fifo` | 20226 | 0 | timeout | 900.105 s |
| `impl,M2,0,0,1,fifo` | 14399 | 0 | timeout | 900.052 s |
| `impl,X2,1,0,1,fifo` | 32129 | 0 | timeout | 900.024 s |
| `impl,X2,0,1,1,fifo` | 4 | 0 | exhausted | 0.206 s |
| `impl,L1,0,0,1,fifo` | 196 | 0 | exhausted | 5.387 s |
| `impl,A3,0,0,1,fifo` | 20070 | 0 | timeout | 900.033 s |
| `impl,X2,0,0,1,bag` | 16064 | 0 | exhausted | 404.882 s |
| `impl,A3,0,0,1,fifo,f1` | 18155 | 0 | timeout | 900.048 s |
| `impl,L1,0,0,1,fifo,f2` | 226 | 0 | exhausted | 5.409 s |

Last output lines per run:

`impl,S1,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,S1,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        431 iterations
    blocked:    0 (counted by the strategy)
    completed:  431
    rate:       31.2 iterations/s
    elapsed:    13.797 s
    graphs:     431 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 212680

`impl,X2,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,X2,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        10412 iterations
    blocked:    0 (counted by the strategy)
    completed:  10412
    rate:       31.9 iterations/s
    elapsed:    326.578 s
    graphs:     10412 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 391452

`impl,X2r,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,X2r,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        19875 iterations
    blocked:    0 (counted by the strategy)
    completed:  19875
    rate:       22.1 iterations/s
    elapsed:    900.045 s
    graphs:     19875 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 1145112

`impl,P2,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,P2,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        20226 iterations
    blocked:    0 (counted by the strategy)
    completed:  20226
    rate:       22.5 iterations/s
    elapsed:    900.105 s
    graphs:     20226 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 805544

`impl,M2,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,M2,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        14399 iterations
    blocked:    0 (counted by the strategy)
    completed:  14399
    rate:       16.0 iterations/s
    elapsed:    900.052 s
    graphs:     14399 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 598328

`impl,X2,1,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,X2,1,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        32129 iterations
    blocked:    0 (counted by the strategy)
    completed:  32129
    rate:       35.7 iterations/s
    elapsed:    900.024 s
    graphs:     32129 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 573488

`impl,X2,0,1,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,X2,0,1,1,fifo' must 0 1 report - 900 </dev/null

    ran:        4 iterations
    blocked:    0 (counted by the strategy)
    completed:  4
    rate:       19.4 iterations/s
    elapsed:    0.206 s
    graphs:     4 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 127492

`impl,L1,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,L1,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        196 iterations
    blocked:    0 (counted by the strategy)
    completed:  196
    rate:       36.4 iterations/s
    elapsed:    5.387 s
    graphs:     196 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 235812

`impl,A3,0,0,1,fifo`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,A3,0,0,1,fifo' must 0 1 report - 900 </dev/null

    ran:        20070 iterations
    blocked:    0 (counted by the strategy)
    completed:  20070
    rate:       22.3 iterations/s
    elapsed:    900.033 s
    graphs:     20070 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 624384

`impl,X2,0,0,1,bag`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,X2,0,0,1,bag' must 0 1 report - 900 </dev/null

    ran:        16064 iterations
    blocked:    0 (counted by the strategy)
    completed:  16064
    rate:       39.7 iterations/s
    elapsed:    404.882 s
    graphs:     16064 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 779344

`impl,A3,0,0,1,fifo,f1`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,A3,0,0,1,fifo,f1' must 0 1 report - 900 </dev/null

    ran:        18155 iterations
    blocked:    0 (counted by the strategy)
    completed:  18155
    rate:       20.2 iterations/s
    elapsed:    900.048 s
    graphs:     18155 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 542520

`impl,L1,0,0,1,fifo,f2`:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:impl,L1,0,0,1,fifo,f2' must 0 1 report - 900 </dev/null

    ran:        226 iterations
    blocked:    0 (counted by the strategy)
    completed:  226
    rate:       41.8 iterations/s
    elapsed:    5.409 s
    graphs:     226 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 218116

After the runs a comment-only edit was made to TxnKv.java; it was recompiled and `impl,S1,0,0,1,fifo` re-run:
431 complete, 0 blocked, exhausted (unchanged). The `report/` directory was removed.

Sanity check (not part of the deliverable). Before the runs above, a temporary print of ti's replies was
added, run for S1 and L1 under fifo, and removed (the file was restored from a copy; `grep DBG` finds
nothing). Per-iteration outcomes:
- S1 (431): c1=Aborted, c2=V0,Aborted 72; c1=Aborted, c2=V0,Committed 232; c1=Committed, c2=V0,Aborted 119;
  c1=Committed, c2=V0,Committed 6 (c2 serialised before c1's blind write); c1=Committed, c2=V1,Committed 2.
- L1 (196): both Aborted 54; exactly one Committed 69 + 69; both Committed only with one reading the
  other's value (V1 2, V2 2); never both Committed with V0, V0.

## Review round 1 (plan/case-studies/tikv/review/REVIEW-astra-java.md, 2026-11-09)

Line numbers J:… below are those of the revised TxnKv.java.

| # | Finding | Action |
|---|---|---|
| 1 | MUST-FIX shard scheduling | Applied (D1): one daemon per shard for every plan with `.resolve_lock(..)` and more than one shard (`Lib.retryMultiRegion`, `Lib.singleShardHandler`, `ShardDone`), as `join_set.spawn` (plan.rs:398) runs `ResolveLock::execute` (plan.rs:1267-1308) per shard; shard resolutions now interleave. |
| 2 | MUST-FIX ResolveLock phases fused | Applied (D2): read phase and write phase are two region steps (`kvResolveLockReadPhase`, `ResolveLockNext` to the region's own mailbox, `kvResolveLockWrite`), as `ProcessResult::NextCommand` (resolve_lock_readphase.rs:101-111) and resolve_lock.rs:82-89. |
| 3 | SHOULD-FIX keep common.rs helper, fix the cut | Helper kept (D5). The cut README is outside this program's directory: for the coordinator. |
| 4 | NOTE kv.rs mappings | Checked against tikv-src/src/server/service/kv.rs (now on disk); all agree, see below. |
| 5 | SHOULD-FIX store/errors.rs unverified | Checked against client-src/src/store/errors.rs (now on disk); the port's behaviour is the source's on every path this round reaches, see below. No code change. |
| 6 | SHOULD-FIX D16 wording | Applied: D16(b) corrected (maximum over shards; one 0 does not make it 0) with the source argument that no zero-maximum path exists in this round. No code change. |
| 7 | NOTE cross-port decisions | xi's lock backoff now comes from the transaction's options through the trigger (`XTrigger.lockBackoffMaxAttempts`, transaction.rs:1638-1642) instead of `optimisticBackoff()`; same bound 10. `Committer::rollback` / BatchRollback stay unported: their only caller is `Transaction::rollback` (transaction.rs:724-773), which P1 / D-6 never call, and `Committer::commit` does not roll back on failure (transaction.rs:1346-1406). |
| 8 | SHOULD-FIX status decoding; GetForUpdate-only limit | Applied: `TransactionStatus.from` accepts `(0, ttl, Some(info))` for any ttl (requests.rs:788). The `Op::Lock` limit is documented in D7. |
| 9 | NOTE run limits | Agreed; the runs below are counting runs only (no verdict, no conformance check). |

Disputed findings: none. One point where I depart from the review's wording, not its substance: finding
1 offers "a sequential ti state machine" or a justified restricted abstraction; I took neither and ran
the shard tasks as separate invisible processes, because that is the source's structure (one tokio task
per shard, plan.rs:398, with no state shared between the tasks: each `resolve_locks` call has its own
`LockResolver` and `clean_regions`, lock.rs:101-104), whereas a state machine inside ti would be a
hand-written scheduler with the same schedules and far more code to get wrong. The cost is the
interface's wording (sections 2 and 6, "one sequential process"): Q1.

### Response shapes against kv.rs and store/errors.rs (pins 96033a2 / 1b883bf)

Server side: tikv-src/src/server/service/kv.rs (response builders) and src/storage/errors.rs
`extract_key_error` 396-563, `extract_key_errors` 585-596 (storage/errors.rs is outside the cut, read for
this check only).

| Response | Source | Port (`Region.*`) | Verdict |
|---|---|---|---|
| Get | kv.rs future_get 1645-1700: Ok(Some) -> value, Ok(None) -> not_found, Err -> error | `GetResp(error, value, notFound)` | same |
| Prewrite | kv.rs:2496-2502: Ok -> min_commit_ts, one_pc_commit_ts, errors = the collected locks; Err -> errors = [e], timestamps 0 | `writeResult` (min_commit_ts also with locks, prewrite.rs:776-783), command error -> `PrewriteResp([e], 0, 0)` | same |
| PessimisticLock (normal wake-up mode) | kv.rs:2503-2534: Ok(Ok) -> values, not_founds; Err / Ok(Err) -> errors = [e], no values | `kvPessimisticLock` | same |
| PessimisticRollback | kv.rs:2535-2537: errors = extract_key_errors(v) | errors empty (the command cannot fail here) | same |
| ResolveLock | kv.rs:2543-2547: Err -> error | `ResolveLockResp(error)` | same |
| Commit | kv.rs:2548-2556: Ok(Committed{ts}) -> commit_version, Err -> error | `CommitResp(error, commitVersion)` | same |
| CheckTxnStatus | kv.rs:2578-2601: RolledBack NoAction, TtlExpire TtlExpireRollback, LockNotExist LockNotExistRollback, Committed -> commit_version, Uncommitted -> lock_ttl + lock_info(primary) (+ MinCommitTsPushed), PessimisticRollBack TtlExpirePessimisticRollback, LockNotExistDoNothing; Err -> error | `txnStatusResp` | same |
| CheckSecondaryLocks | kv.rs:2602-2612: Locked -> locks, Committed -> commit_ts, RolledBack -> defaults; Err -> error | `kvCheckSecondaryLocks` | same |

KeyError (storage/errors.rs:396-563): KeyIsLocked -> `locked`; WriteConflict -> `conflict` (+ `retryable`
text); TxnLockNotFound -> `txn_lock_not_found` (+ `retryable`); TxnNotFound -> `txn_not_found`;
CommitTsExpired -> `commit_ts_expired`; CommitTsTooLarge -> `commit_ts_too_large`; PrimaryMismatch ->
`primary_mismatch.lock_info`; everything else -> `abort` text. The port's `KeyError.kind` discriminator
carries the same partition; the client reads only `locked` (HasLocks, requests.rs), `primary_mismatch`
(lock.rs:403), `txn_not_found` (lock.rs:668), `commit_ts_expired` (transaction.rs:1550) and, in the
D-7 retry, `conflict`. The port keeps PrimaryMismatch's lock info in the `locked` slot but `takeLocks`
takes only KEY_IS_LOCKED, so no PrimaryMismatch lock is ever resolved as a KeyIsLocked lock: the same
behaviour as the source, where `locked` is unset.

Client side (client-src/src/store/errors.rs): `has_key_error!` (Get, Commit, ResolveLock, CheckTxnStatus,
CheckSecondaryLocks: the single `error`) and `extract_errors` over `errors` (Prewrite, PessimisticLock,
PessimisticRollback) are `Lib.keyErrors`. `HasKeyErrors for Vec<T>` (errors.rs:176-185) returns the key
errors of the first element that has any; `for Result` (163-174) gives `Err(MultipleKeyErrors(es))` ->
`es`, any other `Err(e)` -> `[e]`; ExtractError (plan.rs:1505-1531) then makes `MultipleKeyErrors(errors)`
if all are KeyError/KvError, else returns the first other error. The port's `extractError` throws the
first Err item unchanged. Equal on every reachable path: an Err item is `MultipleKeyErrors` of KeyErrors
from `handle_candidate_response`, which the source unwraps and rewraps to the same value; a shard-level
Err (ResolveLockError, a bare KeyError from `resolve_lock_with_retry`) ends a non-preserving plan in
`single_plan_handler` before ExtractError looks at the vector (plan.rs:415-448 then ExtractError's
`self.inner.execute().await?`), as `nonPreserve` throws it; the preserving plan (pessimistic_lock) has no
ExtractError. The Rust port's extra wrapping of a bare response error (review finding 5) is the
`Err(e)` arm with a KeyError, which this round's plans do not reach. No code change.

### Runs, before and after review round 1

Local machine, 2026-11-09, JMC classes from daavar/jmc/cp.txt (same build for both columns), counting mode
(`must`, no export, no `--spec`, no verdict). "before" = the round-0 program (a copy of TxnKv.java as it
was before this revision, compiled and rerun today), "after" = this revision. Each pair ran side by side;
the S1 and X2-fifo pairs ran with four JVMs at once (before a machine pause), all others with two. Report
directories were per run (`report-<size>`) and removed. No run printed an exception or `FAILED:`; every
graph is complete, none blocked. A timeout count is a lower bound.

| Size | before: complete / blocked, end, wall | after: complete / blocked, end, wall |
|---|---|---|
| `impl,S1,0,0,1,fifo` | 431 / 0, exhausted, 13.226 s | 431 / 0, exhausted, 13.321 s |
| `impl,X2,0,0,1,fifo` | 10412 / 0, exhausted, 269.119 s | 7224 / 0, exhausted, 272.772 s |
| `impl,X2r,0,0,1,fifo` | 19103 / 0, timeout, 900.044 s | 13002 / 0, timeout, 900.048 s |
| `impl,P2,0,0,1,fifo` | 23984 / 0, timeout, 900.038 s | 16812 / 0, exhausted, 896.474 s |
| `impl,M2,0,0,1,fifo` | 15402 / 0, timeout, 900.034 s | 11675 / 0, timeout, 900.019 s |
| `impl,X2,1,0,1,fifo` | 32244 / 0, exhausted, 821.141 s | 15317 / 0, timeout, 900.105 s |
| `impl,X2,0,1,1,fifo` | 4 / 0, exhausted, 0.312 s | 4 / 0, exhausted, 0.432 s |
| `impl,L1,0,0,1,fifo` | 196 / 0, exhausted, 5.212 s | 196 / 0, exhausted, 6.483 s |
| `impl,A3,0,0,1,fifo` | 21579 / 0, timeout, 900.032 s | 19179 / 0, timeout, 900.026 s |
| `impl,X2,0,0,1,bag` | 16064 / 0, exhausted, 389.203 s | 7224 / 0, exhausted, 278.214 s |
| `impl,A3,0,0,1,fifo,f1` | 21628 / 0, timeout, 900.023 s | 20157 / 0, timeout, 900.036 s |
| `impl,L1,0,0,1,fifo,f2` | 226 / 0, exhausted, 6.125 s | 226 / 0, exhausted, 6.143 s |

Command per run (after; before the same with the copy's `java/out`), in OUT_DIR:

    java -Xss4m -Xmx8g -cp java/out:$(cat .../daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.tikv.TxnKv:<size>' must 0 1 report-<size> - 900 </dev/null

Last lines of each after run:

`impl,S1,0,0,1,fifo`:

    ran:        431 iterations
    blocked:    0 (counted by the strategy)
    completed:  431
    elapsed:    13.321 s
    graphs:     431 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 348124

`impl,X2,0,0,1,fifo`:

    ran:        7224 iterations
    blocked:    0 (counted by the strategy)
    completed:  7224
    elapsed:    272.772 s
    graphs:     7224 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 422660

`impl,X2r,0,0,1,fifo`:

    ran:        13002 iterations
    blocked:    0 (counted by the strategy)
    completed:  13002
    elapsed:    900.048 s
    graphs:     13002 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 516872

`impl,P2,0,0,1,fifo`:

    ran:        16812 iterations
    blocked:    0 (counted by the strategy)
    completed:  16812
    elapsed:    896.474 s
    graphs:     16812 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 546588

`impl,M2,0,0,1,fifo`:

    ran:        11675 iterations
    blocked:    0 (counted by the strategy)
    completed:  11675
    elapsed:    900.019 s
    graphs:     11675 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 532936

`impl,X2,1,0,1,fifo`:

    ran:        15317 iterations
    blocked:    0 (counted by the strategy)
    completed:  15317
    elapsed:    900.105 s
    graphs:     15317 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 577760

`impl,X2,0,1,1,fifo`:

    ran:        4 iterations
    blocked:    0 (counted by the strategy)
    completed:  4
    elapsed:    0.432 s
    graphs:     4 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 126976

`impl,L1,0,0,1,fifo`:

    ran:        196 iterations
    blocked:    0 (counted by the strategy)
    completed:  196
    elapsed:    6.483 s
    graphs:     196 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 212868

`impl,A3,0,0,1,fifo`:

    ran:        19179 iterations
    blocked:    0 (counted by the strategy)
    completed:  19179
    elapsed:    900.026 s
    graphs:     19179 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 566524

`impl,X2,0,0,1,bag`:

    ran:        7224 iterations
    blocked:    0 (counted by the strategy)
    completed:  7224
    elapsed:    278.214 s
    graphs:     7224 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 432348

`impl,A3,0,0,1,fifo,f1`:

    ran:        20157 iterations
    blocked:    0 (counted by the strategy)
    completed:  20157
    elapsed:    900.036 s
    graphs:     20157 distinct complete, 0 distinct blocked; stack not exhausted
    stop_reason: timeout
    peak_rss_kb: 800452

`impl,L1,0,0,1,fifo,f2`:

    ran:        226 iterations
    blocked:    0 (counted by the strategy)
    completed:  226
    elapsed:    6.143 s
    graphs:     226 distinct complete, 0 distinct blocked; stack exhausted
    stop_reason: exhausted
    peak_rss_kb: 230676

Reading of the counts (my explanation from the program's structure, not a measured attribution):
- Single-region sizes (S1, L1, L1/f2, X2 onepc) are unchanged: no plan has two shards, and a ResolveLock
  read phase there finds no lock (CheckTxnStatus at the primary already removed it), so no
  `ResolveLockNext` is sent.
- X2 fifo falls from 10412 to 7224 and X2 bag from 16064 to 7224, now equal. Two effects of D1: (i) the
  shard tasks are joined in shard order, whereas round 0 let ti take the two shard replies in either
  order even when nothing depended on it (graphs differing only in that order are gone); (ii) a shard's
  resolution messages now come from the shard's own task, so no two messages of one sender to one region
  are ever in flight at once in X2, and fifo and bag admit the same graphs. In round 0 ti could have a
  shard's Prewrite to r2 in flight while sending r2 the CheckTxnStatus of the other shard's resolution,
  which fifo ordered and bag did not. New schedules (the two shards' resolutions interleaved, and
  commands between a ResolveLock's two phases) are in the after counts; the before counts contain
  schedules that differ only in join order.
- P2 has fewer graphs and now exhausts (16812), by the same two effects as X2. X2 async goes the other
  way: it no longer exhausts within 900 s (15317, against 32244 exhausted before); there the
  CheckSecondaryLocks fan-outs inside concurrent shard resolutions and the two-phase ResolveLock add
  schedules.
- The class files of the S1 and X2-fifo after runs were compiled before a comment-only edit of
  TxnKv.java; every other after run used the final file.

## Round 3: spawn structure for JMC export (2026-11-10, owner decision 2026-11-10 08:40)

Why: at the pin 648e9c8, JMC's export mode needs main to spawn every process. In round 2, ti and xi spawned the
shard tasks of `Lib.retryMultiRegion` (21:10 amendment), so every instance with a two-shard plan was refused with
"must: process 12 was spawned by process 7". This round makes one structural change, and nothing else changes.

Change (TxnKv.java):
- `run()` spawns the shard tasks up front with `MpProcess.spawnDaemon(() -> shardTaskMain(..))`. There is one task
  per (ti or xi, region), so 2·m·R tasks, and only when R >= 2. They are spawned after c1..cm and before init, so
  the tids of r, pd, x, t and c are unchanged and only init's moves. `Setup` carries them in `tTasks` and `xTasks`
  (per client index, per region), and tMain/xMain put their own list into `Lib.shardTasks`.
- Where `retryMultiRegion` spawned a daemon for shard i, it now sends one `ShardWork(parent, plan, idx, sh,
  shardReq, timestamp, backoffMax, backoffCurrent)` to the task of the shard's region. The shards of one plan are
  distinct regions (`shardsOf` groups sorted keys by region, and the region of a key is monotone). This is the
  payload the round-2 closure captured. The `ResolveCfg` travels as its values because JMC compares message
  payloads by `equals` on replay, and `Backoff` has no value equality. Sending the object gave "must: replay drift
  ... ShardWork[... backoff=TxnKv$Backoff@61105c1d]" on the first X2 smoke. The task rebuilds
  `ResolveCfg(timestamp, backoff.copy())`, the same value round 2 passed.
- `shardTaskMain` loops: receive a `ShardWork`, build a fresh Lib at the task's address, run `singleShardHandler`,
  send `ShardDone(plan, idx, out)` to the parent. The parent's join (`ShardDone` received in shard order) and the
  inline one-shard path are unchanged. The constructor `Lib(Lib parent, Tid self)` became
  `Lib(md, self, pd, regions, regionOfKey)`, with the same fields.

Design points (for the round-3 review):
1. No Stop message. The order said to end the tasks "exactly as the other daemon processes of the port are ended".
   The port's daemons (r, pd, x, t, init) are never sent a Stop: each blocks on its next receive when the clients
   are done, and JMC ends the daemons with main. The shard tasks do the same.
2. Fixed task count 2·m·R. Round 2 spawned a new task per shard per plan, so the count varied by execution. Now
   each of ti and xi has one task per region, and it serves that region's shard of every plan the process runs.
   ti and xi run plans concurrently, so they do not share tasks. Within one process, plans run one after another:
   the caller waits for every `ShardDone` before it continues. So a task has at most one job at a time. The shards
   of one plan go to distinct tasks, and there is no shared queue between shards.
3. Fresh Lib per job. Each job starts with request ids at 0, as round 2's freshly spawned task did. The ids are
   reused at the same address across jobs. That is safe because every request a job sends (TSO, KvReq) is answered
   before the job ends: each region sends exactly one reply per request, and `ResolveLockNext` goes to the region
   itself. So no reply is ever left over for a later job to match.

Why the visible behaviour is preserved: spawn edges are invisible, and no observation, status or message tag of
c1..cm changes. Each former spawn is now one invisible `ShardWork` message, sent at the same program point with the
same payload. Its receive orders the task's job after the caller's earlier events, as the spawn edge did. The task
then runs the same code as before and sends the same `ShardDone`. The difference is that a task's job may start
later than the send, and a task's address is reused across jobs. Neither difference is visible, and the parent's
behaviour does not change: it waits for the `ShardDone` messages in shard order whatever the schedule. Graph counts
may still change, because the invisible events change (ShardWork send and receive, the earlier daemon spawns).

Smokes (workstation; JMC classes of 10-31, the stale local build noted in STAGING.md, which contains the export
refusal; javac into ~/tmp/claude-build/tikv-java/out):
- `impl,S1,0,0,1,fifo`, export: exit 0, `graphs_complete: 431`, `graphs_blocked: 0`, `summaries_distinct: 58`,
  `stop_reason: exhausted`, 14.5 s, 59 export lines. Same 431 as rounds 1-2: S1 has one key (n = 1), so R = 1
  and no shard task is spawned. It checks only that the export still works.
- `impl,X2,0,0,1,fifo`, export under `timeout 600` (a round-2 refusal): exit 0, `graphs_complete: 7224`,
  `graphs_blocked: 0`, `summaries_distinct: 16`, `stop_reason: exhausted`, 354 s, 17 export lines. No
  "spawned by process" refusal, and 7224 is the round-2 count for X2 fifo (a non-export run).
- The first X2 smoke, before the ResolveCfg-as-values fix, stopped at once with the replay drift quoted above. S1
  and X2 were then run again on the final file. Those two reruns are the results above.
