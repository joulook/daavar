# bench_tikv_port: notes

A Rust/Must transliteration of TiKV's transaction protocol: the client from tikv/client-rust 1b883bf and the
server actions from tikv/tikv 96033a2. Interface: `plan/case-studies/tikv/INTERFACE-agents.md` (called INTERFACE.md
below). Source cut and rules A1-A6, T1-T6, P1-P2: `plan/case-studies/tikv/cut/README.md` (called README below).
Source paths below are relative to `plan/case-studies/tikv/client-src/src/` (prefix `C:`) and
`plan/case-studies/tikv/tikv-src/` (prefix `S:`). Each changed line in `src/main.rs` carries `// PORT:` and its
reason (94 markers after review round 1).

## Files

- `Cargo.toml`: as the prompt gives it (`traceforge = { path = "../../../traceforge/traceforge" }`, `[workspace]`).
- `src/main.rs`: the program (one file). Sections, in order: the interface (verbatim), the shared messages, the
  client (backoff, plans, requests.rs, lock.rs, buffer.rs, transaction.rs), the client processes (`ti`, `xi`,
  `pd`), the server (txn_types, cell, errors, concurrency manager, MvccTxn, reader, actions, commands), the region
  process, the entry.
- `NOTES.md`: this file.

## Compile and run

```
cd daavar/benchmarks-cf2/tikv-txn/rust
export TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/tikv-rust
cargo build --release -j 2
timeout 600 $CARGO_TARGET_DIR/release/bench_tikv_port count impl --model fifo -- S1 0 0 1
```
With the default target directory, the binary is at `target/release/bench_tikv_port`, as the prompt's run command
has it. The build has no warnings.

## Roles and messages

| Process | Spawn | Source | Function in main.rs |
|---|---|---|---|
| `r1..rR` | `spawn_daemon`, never ends | a TiKV region: scheduler and storage (rule A1) | `region_main`, `Region::handle` |
| `pd` | `spawn_daemon` | the TSO: 1, 2, 3, ... (rule T1) | `pd_main` |
| `x1..xm` | `spawn_daemon`, at program start | `Committer::commit_secondary` spawned at C:transaction/transaction.rs:1401-1405 | `secondary_committer` |
| `t1..tm` | `spawn_daemon` | `Transaction`, `Committer`, `LockResolver`, plans | `library` |
| `c1..cm` | `spawn` (visible) | INTERFACE.md 5.1, verbatim | `client` |
| `init` | `spawn` | the setup message | closure in `run` |
| shard tasks (unnamed) | `thread::spawn` by `ti`, `xi` or another shard task, joined by it | `join_set.spawn` per shard, C:request/plan.rs:398 (only when a request has two or more shards, D1) | closure in `retry_multi_region`, `single_shard_handler` |

The start order is the one INTERFACE.md 2 gives: regions, `pd`, `x*`, `t*`, `c*`, `init`. The main thread joins
`c*` and `init`. Layout: `onepc = 1` gives `single` (R = 1), otherwise `split` (R = n, `kj` on `rj`).

Visible messages: `Req` and `Rep`, verbatim from INTERFACE.md 4. `ci` sends untagged messages; `ti` receives
them with the guard `tag == None`.

Invisible messages: the single type `Net`, always tagged:
- `TAG_SETUP`: `Net::Setup(Setup)` from `init`. It carries the thread ids of all processes, the layout `layout[k-1]`,
  the clients' kinds, `async`, `onepc`, `a` and the variant.
- `TAG_NET`: `Net::Tso(from)` / `Net::TsoReply(ts)`, `Net::Req(from, Request)` / `Net::Rep(region, Response)`.
  `Request` and `Response` are the kvrpcpb messages the cut uses: Get, PessimisticLock, Prewrite, Commit,
  BatchRollback, PessimisticRollback, CheckTxnStatus, CheckSecondaryLocks and ResolveLock. Each message keeps only
  the fields the cut reads or writes. `KeyError` keeps the fields that `S:src/storage/errors.rs:396-541` sets.
  `Net::NextCommand(from, NextCommand::ResolveLock{..})` is sent by a region to itself: the scheduler's
  `ProcessResult::NextCommand` of ResolveLockReadPhase (D5); the region answers `from` after evaluating it.
- `TAG_TRIGGER`: `Net::Trigger` from `ti` to `xi`. It carries what `commit_secondary` needs: the primary key, the
  mutations (whose keys are the keys to commit), start_ts, the options (`async_commit`, backoffs), the write size
  and commit_ts.

`ti`, `xi` and the shard tasks receive a reply with the guard `tag == TAG_NET && sender = the region of the
request` (or `pd`). This works because each of these processes has at most one request in flight: a request with
two or more shards sends each shard from its own shard task (D1).

## Function mapping (source order)

Client (`C:`):
| Source | Lines | main.rs |
|---|---|---|
| transaction/transaction.rs `Transaction::new` | 101-122 | `Transaction::new` |
| `Transaction::get` | 143-164 | `Transaction::get` |
| `Transaction::get_for_update` | 208-224 | `Transaction::get_for_update` (+ P2 loop) |
| `Transaction::put` | 470-480 | `Transaction::put` (+ P2 loop) |
| `Transaction::commit` | 653-706 | `Transaction::commit` |
| `Transaction::pessimistic_lock` | 859-940 | `Transaction::pessimistic_lock` |
| `Transaction::pessimistic_lock_rollback` | 943-980 | `Transaction::pessimistic_lock_rollback` |
| `check_allow_operation`, `start_auto_heartbeat`, `transit_status` | 983-1093 | same names (heartbeat empty, T3) |
| `TransactionOptions` (new_optimistic/pessimistic, use_async_commit, try_one_pc, push_for_update_ts) | 1160-1270 | `TransactionOptions` |
| `Committer::commit` | 1340-1407 | `Committer::commit` (spawn -> trigger) |
| `Committer::prewrite` | 1409-1483 | `Committer::prewrite` |
| `Committer::commit_primary` | 1486-1542 | `Committer::commit_primary` |
| `Committer::commit_primary_with_retry` | 1544-1584 | `Committer::commit_primary_with_retry` |
| `Committer::commit_secondary` | 1586-1648 | `Committer::commit_secondary` (run by `xi`) |
| `Committer::rollback` | 1662-1709 | `Committer::rollback` (never called, P1) |
| `calc_txn_lock_ttl` | 1711-1720 | `Committer::calc_txn_lock_ttl` |
| transaction/lock.rs `reject_shared_locks` | 47-67 | `reject_shared_locks` (always Ok, A2) |
| `resolve_locks` | 86-151 | `resolve_locks` |
| `LockAction` | 153-180 | `LockAction` |
| `pessimistic_rollback_lock` | 189-217 | `pessimistic_rollback_lock` |
| `resolve_lock_with_retry` | 223-317 | `resolve_lock_with_retry` |
| `ResolveLocksContext` (resolved cache) | 320-367 | `ResolveLocksContext` |
| `LockResolver::action_for_lock` | 381-482 | same |
| `LockResolver::check_txn_status` | 615-689 | same |
| `LockResolver::check_all_secondaries` | 691-708 | same |
| `LockResolver::get_txn_status_from_lock` | 729-794 | same |
| `lock_until_expired_ms` | 805-807 | same (T1 `physical`) |
| transaction/requests.rs constructors | 95-100, 192-202, 218-248, 306-317, 362-420, 631-652, 794-802 | `new_*_request` |
| `Shardable for PrewriteRequest` | 254-294 | `impl KvRequest for PrewriteRequest` |
| `Shardable for CommitRequest`, `shardable_keys!` | 319-346, shard.rs 184-243 | `shard_keys`, `impl KvRequest` |
| `Shardable for PessimisticLockRequest` | 426-444 | `impl KvRequest for PessimisticLockRequest` |
| `Merge<..PessimisticLockResponse..> for CollectWithShard` | 448-500 | `collect_with_shard` |
| `TransactionStatus`, `TransactionStatusKind`, `check_ttl`, `is_determined`, `is_cacheable` | 701-791 | same |
| `CollectSecondaryLocks` merge, `SecondaryLocksStatus::resolved_commit_version` | 813-924 | `collect_secondary_locks`, same |
| `HasLocks` impls | 926-962 | `impl HasLocks for *Response` |
| transaction/lowering.rs | 24-161 | folded into the callers (`Key`/`Timestamp` -> integers) |
| transaction/buffer.rs `get_primary_key`, `primary_key_or`, `get`, `get_or_else`, `lock`, `unlock`, `put`, `to_proto_mutations`, `update_cache`, `insert_entry`, `BufferEntry` | 35-73, 201-241, 277-330, 367-433 | `Buffer`, `BufferEntry` |
| transaction/client.rs `begin_optimistic`/`begin_with_options`/`current_timestamp` | 174-181, 226-230, 250-252 | `library` (Req::Begin) |
| backoff.rs constants, `next_delay_duration`, `is_none` | 10-13, 30-80 | `Backoff` |
| request/mod.rs `RetryOptions` | 59-88 | `RetryOptions` |
| request/plan.rs `Dispatch::execute` | 69-84 | `PdClient::{send, recv_any, dispatch}` |
| `collect_join_set_results`, `RetryableMultiRegion::single_plan_handler`/`execute` | 108-153, 375-451, 1042-1060 | `retry_multi_region` (one thread per shard task, joined) |
| `single_shard_handler`, `handle_candidate_response` key-error check | 679ff, 806-812 | `single_shard_handler` |
| `MergeResponse`, `CollectSingle`, `CollectError` | 1154-1222 | `collect_single`, `collect_error` |
| `ResolveLock::execute` | 1267-1308 | `resolve_lock_execute` |
| `ExtractError::execute` | 1505-1540 | `extract_error`, `single_region_extract_error` |
| `PreserveShard`, `ResponseWithShard` | 1546-1604 | `ResponseWithShard` (shard carried by every plan) |

Server (`S:`):
| Source | Lines | main.rs |
|---|---|---|
| src/storage/txn/commands/mod.rs `From<PrewriteRequest>` | 164-213 | `prewriter_from_request` |
| mod.rs `From<PessimisticLockRequest>`, Commit, BatchRollback, PessimisticRollback, CheckTxnStatus, CheckSecondaryLocks, ResolveLock | 214-384 | `Region::handle` arms |
| commands/prewrite.rs `Prewriter::process_write`, `prewrite`, `write_result` | 501-806 | `Prewriter::*` |
| prewrite.rs `Optimistic::can_skip_constraint_check` | 840-862 | `Prewriter::can_skip_constraint_check` |
| prewrite.rs `one_pc_commit`, `handle_1pc_locks`, `fallback_1pc_locks` | 953-1000 | same |
| commands/commit.rs `process_write` | 53-97 | `process_commit` |
| commands/check_txn_status.rs `process_write` | 87-189 | `process_check_txn_status` |
| commands/rollback.rs `process_write` | 50-81 | `process_rollback` |
| commands/resolve_lock_readphase.rs `process_read` | 51-113 | `process_resolve_lock_readphase` (returns the NextCommand) |
| commands/resolve_lock.rs `process_write` | 82-173 | `process_resolve_lock` (a separate command, `Region::handle_next`) |
| storage/txn/mod.rs `ProcessResult::{Res, NextCommand}`; the scheduler re-scheduling a NextCommand | | `Handled`, `Net::NextCommand`, `region_main` |
| commands/check_secondary_locks.rs | 67-254 | `csl_check_determined_txn_status`, `check_status_from_lock`, `process_check_secondary_locks` |
| commands/acquire_pessimistic_lock.rs `process_write` | 82-220 | `process_acquire_pessimistic_lock` |
| commands/pessimistic_rollback.rs `process_write` | 60-156 | `process_pessimistic_rollback` |
| txn/actions/prewrite.rs `prewrite`/`prewrite_with_generation` | 36-286 | `prewrite` |
| prewrite.rs `TransactionProperties`, `CommitKind`, `TransactionKind`, `LockStatus`, `PrewriteMutation::{from_mutation, lock_info, check_lock, check_for_newer_version, write_lock, write_conflict_error, skip_constraint_check, need_min_commit_ts, try_one_pc}` | 288-787, 896-927 | same names (`SrvTransactionKind`) |
| prewrite.rs `async_commit_timestamps`, `amend_pessimistic_lock` | 928-1035 | same |
| txn/actions/common.rs `check_committed_record_on_err` | 70-91 | same |
| txn/actions/commit.rs `handle_lock_not_found`, `commit` | 19-245 | same |
| txn/actions/check_txn_status.rs (all functions of the cut) | 17-496 | same names |
| txn/actions/cleanup.rs `cleanup` | 24-135 | `cleanup` |
| txn/actions/acquire_pessimistic_lock.rs `acquire_pessimistic_lock`, `handle_existing_exclusive_lock` | 44-333, 358-464 | same |
| src/storage/mod.rs `get_entry` + `prepare_snap_ctx`; mvcc/reader/point_getter.rs | 634-826, 3573-3634; 173-421 | `Region::get` |
| src/storage/errors.rs `extract_key_error` | 396-541 | `extract_key_error` |
| src/storage/types.rs `TxnStatus` | 108-147 | `TxnStatus` |
| mvcc/txn.rs `MvccTxn` writes, `mark_rollback_on_mismatching_lock`, `clear`, `get_pending_lock_bytes` | 66-274 | `MvccTxn` |
| mvcc/reader/mod.rs `TxnCommitRecord`, `OverlappedWrite` | 30-96 | same |
| mvcc/reader/reader.rs `load_lock`, `seek_write`, `load_data`, `get`, `get_write`, `get_write_with_commit_ts`, `get_txn_commit_record` | 198-256, 476-631 | `SnapshotReader::*` |
| components/txn_types/src/lock.rs `Lock`, `into_lock_info`, `check_ts_conflict_si` | 87ff, 363-422, 622-656 | `Lock`, `check_ts_conflict_si` |
| components/txn_types/src/write.rs `WriteType`, `Write`, `new_rollback`, `is_protected` | 16ff, 71ff, 204-219, 459-466 | `WriteType`, `Write` |
| components/concurrency_manager/src/lib.rs `max_ts`, `update_max_ts` | 221-272 | `ConcurrencyManager` |

## Decisions and deviations (each with the source lines it replaces)

Every rule of README section 3 is applied where the source has it, and each such line is marked `// PORT:`. The
list below gives what the README does not fix, or fixes only in general.

- D1 Shard tasks (C:request/plan.rs:375-451, `join_set.spawn` 398; `single_shard_handler` 679ff; `ResolveLock`
  1273-1308). Revised in review round 1. The plans are `RetryableMultiRegion(ResolveLock(Dispatch))`
  (plan_builder.rs: `.resolve_lock(..)` wraps the dispatch, `.retry_multi_region(..)` wraps that), so each shard
  task runs the first dispatch, the whole lock-resolution loop with its re-dispatches, and the key-error check of
  806-812, independently of the other shards. The port spawns one invisible thread per shard task
  (`thread::spawn`), with its own `PdClient` (its own thread id, so the regions and `pd` answer it directly), and
  joins them in shard order; `collect_join_set_results` (108-153) also returns the results in shard order, and
  the caller does nothing between the joins, so the join order is not observable. When a request has exactly one
  shard, the task runs in the caller's process: the source's caller only awaits that one task, so no interleaving
  differs. Before round 1, the port sent every shard's first dispatch, received the replies, and then ran the
  rest of each shard task one after the other in shard order; that lost the interleavings of overlapping shard
  continuations and also gave `ti` a receive with two possible senders, whose two orders were counted as two
  executions without a difference in behaviour (this is why some counts went down, see the runs).
- D2 Region errors, leader and candidate rounds, `retry_same_region`, semaphores and gRPC actions
  (C:request/plan.rs:452-1027) are cut (A1). The undetermined handling (`terminal_on_undetermined`,
  `is_undetermined_region_error` 141-143, `is_commit_outcome_unknown`) is kept as a function that returns
  `false` (A6).
- D3 `HasKeyErrors` (C:store/errors.rs, fetched by the coordinator after round 1 and compared line by line): a
  response with `error: Option<KeyError>` reports `error.take()` as one error (`has_key_error!` 66-84: Get,
  Commit, BatchRollback, ResolveLock, CheckTxnStatus, CheckSecondaryLocks); Prewrite, PessimisticLock and
  PessimisticRollback report their `errors` when non-empty (145-161, `extract_errors` 205-215). `Result<T, Error>`
  reports `x.key_errors()` for Ok, the vector of `MultipleKeyErrors`, or the error itself (163-174); `Vec<T>`
  reports the first element that has errors (176-186). With `ExtractError::execute` (C:request/plan.rs:1505-1540:
  all response errors -> `MultipleKeyErrors`, else the first non-response error) this is what `extract_error` and
  `single_region_extract_error` do: the Ok shard results carry no key errors any more (they were moved into the
  shard's `Err(MultipleKeyErrors)` by `handle_candidate_response`, plan.rs:806-812), so the first `Err` wins. The
  reconstruction agreed; only comments changed. `From<kvrpcpb::KeyError> for Error` (C:common/errors.rs:161-164,
  fetched last) is `Error::KeyError(Box::new(e))`, as the port has it (`has_key_error`, `extract_errors`).
- D4 The kv service glue (S:server/service/kv.rs, fetched after round 1 and compared line by line; region errors
  cannot occur, rule A1):
  - Prewrite (2496-2502): on Ok `min_commit_ts` and `one_pc_commit_ts`; `errors` = `extract_key_errors` of the
    locks, or `[extract_key_error(e)]` (S:storage/errors.rs:585-596). Agreed.
  - PessimisticLock (2503-2533, WakeUpModeNormal): `into_legacy_values_and_not_founds` (S:storage/types.rs:371-403)
    of the results, always (not "only when `return_values` is set"); `Err(e) | Ok(Err(e))` -> `errors = [e]`. The
    old code gave the same response (the results are `Value` exactly when `need_value`, else `Empty`, which gives
    empty vectors) and pushed 0 for a missing value where the source pushes an empty value; the client drops both
    by `not_found`. Rewritten literally as `into_legacy_values_and_not_founds`; no behaviour change.
  - PessimisticRollback (2535-2537): `extract_key_errors(v)`; BatchRollback (2538-2542) and ResolveLock (2543-2547):
    `error` only on Err. Agreed.
  - Commit (2548-2556): `commit_version` from `TxnStatus::Committed { commit_ts }`, which is the request's
    commit_ts (S:commands/commit.rs:79-81). Agreed.
  - CheckTxnStatus (2578-2601): as before (RolledBack -> NoAction, TtlExpire -> TtlExpireRollback, LockNotExist ->
    LockNotExistRollback, Committed -> commit_version, Uncommitted -> lock_ttl, lock_info, MinCommitTsPushed if
    pushed, PessimisticRollBack -> TtlExpirePessimisticRollback, LockNotExistDoNothing -> LockNotExistDoNothing),
    except that `lock_info` is built with the lock's own primary (`lock.primary`), not the request's primary key.
    The two are equal whenever a lock is returned (a mismatching primary returns PrimaryMismatch first,
    S:actions/check_txn_status.rs:92-117), so no behaviour change; rewritten literally.
  - CheckSecondaryLocks (2602-2613): Locked -> `locks`, Committed(ts) -> `commit_ts`, RolledBack -> nothing
    (commit_ts 0). Agreed.
  - Get (`future_get` 1645-1697): Some(value) -> `value`, None -> `not_found`, Err -> `error`. Agreed.
- D5 ResolveLockReadPhase and ResolveLock are two commands, as in the source. Revised in review round 1.
  `process_read` (S:commands/resolve_lock_readphase.rs:51-113) scans the region's locks of the given start_ts and
  returns `ProcessResult::Res` when there is none (the reply), else `ProcessResult::NextCommand(ResolveLock {
  txn_status, scan_key: None, key_locks })` (100-110). The scheduler schedules that as a new command; the port's
  region sends it to itself as `Net::NextCommand(from, ..)`, so other commands to the region can be evaluated
  between the two. `ResolveLock::process_write` (S:commands/resolve_lock.rs:82-173) then runs on a new snapshot;
  it uses the read phase's `current_lock` for the ts and the `is_pessimistic_lock` test and lets `cleanup` /
  `commit` load the lock again, as the source does. Its result (or error) is the reply to `from`. Before round 1,
  both ran in one atomic step, which removed, e.g., the schedule of the review in which the transaction commits
  between the read phase and the cleanup, so that cleanup returns `Committed` (S:actions/cleanup.rs:108-120).
- D6 `get_for_update` on an optimistic transaction calls `lock_keys` (C:transaction.rs:211-214), which is not in
  the cut. The port has `unreachable!` there; INTERFACE.md 3 gives GetForUpdate to pessimistic clients only.
- D7 A key that is GetForUpdate-locked and never Put becomes an `Op::Lock` mutation (C:transaction/buffer.rs:403;
  `Transaction::commit` tests for empty mutations, not for the absence of Puts, transaction.rs:675-679).
  INTERFACE.md 3 says an implementation must support GetForUpdate; INTERFACE.md 6 (and README rule A2) allows
  only Put mutations. Reconciled in review round 1 by narrowing, not by adding the Lock path: GetForUpdate is
  supported (pessimistic_lock with need_value, the value returned, the key's buffer entry `Locked(Some(v))`), and a
  script in which a GetForUpdate(k) is not followed by a Put(k) of the same client is refused at argument time
  (`parse_instance`), since it would commit a Lock mutation that the interface excludes. The region still panics
  on a Lock mutation (`prewriter_from_request`, `WriteType::from_lock_type`) as a second guard. No scenario of
  INTERFACE.md 3 contains a GetForUpdate, so no run is affected. Supporting GetForUpdate-only keys would need the
  server's Lock mutation path (prewrite of `Op::Lock`, `WriteType::Lock` records, readers skipping them).
  Decision (coordinator ruling on Q5, 2026-11-09): keep the narrowed interface; GetForUpdate-only keys are refused
  at argument time; no scenario needs the Lock-mutation path.
- D8 Variant f1 is applied at the line: check_txn_status.rs:289-291 (`put_write` of `construct_write`) is skipped in
  `check_txn_status_missing_lock` for every caller. The callers are CheckTxnStatus (commands/check_txn_status.rs
  151), the primary-mismatch branch of `check_txn_status_lock_exists` (actions/check_txn_status.rs:117), and `cleanup`
  (actions/cleanup.rs:108, reached from BatchRollback and from ResolveLock with commit_ts 0). The README
  names the lines; INTERFACE.md 9 names CheckTxnStatus. See open question Q1.
- D9 P2: the WriteConflict retry wraps the `pessimistic_lock` call inside `Transaction::put` (C:transaction.rs:
  474-477) and inside `get_for_update` (215). Each retry is a new call, so it takes a new for_update_ts (887). It
  stops after `a` attempts in total. "WriteConflict" means a `KeyError` with `conflict` set (errors.rs:409-429)
  inside `PessimisticLockError.inner` or `MultipleKeyErrors`. With one key per lock request, success_keys is always
  empty, so the error the caller sees is the `PessimisticLockError` wrapper (C:transaction.rs:926 `_ => Err(err)`).
- D10 `tokio::spawn` at C:transaction.rs:1401-1405 becomes `Net::Trigger` to the static `xi`. `xi` builds its own
  `PdClient` (its own thread id), runs `commit_secondary` and drops the result. `ti` replies `Committed` without
  waiting. The heartbeat (1053) becomes nothing (T3).
- D11 Short values are cut (A2). `write_lock` always calls `put_value` (S:actions/prewrite.rs:688-695), and
  `rollback_lock` always deletes the value of a Put lock (check_txn_status.rs:328-330). Commit and 1PC write
  records carry no value (commit.rs:218-222, commands/prewrite.rs:977-983). Readers take the value from `data` at
  the write's start_ts.
- D12 `last_change` is cut (A2). `get_write_with_commit_ts` skips a Rollback record by seeking below it, which is the
  `_` arm of S:mvcc/reader/reader.rs:575-596. gc_fence is cut.
- D13 Get (S:src/storage/mod.rs:3585-3596; point_getter.rs:205-243): the in-memory check `read_key_check` and the
  persisted check `load_and_check_lock` read the same cell lock (A3), so the port checks it once.
- D14 MvccTxn: commands evaluate on a snapshot (a copy of the region's cells). Their writes are collected as
  `Modify`s and applied at the end, only when the command succeeds (A1/A2). `get_pending_lock` mirrors
  `get_pending_lock_bytes` (txn.rs:260-273), and `max_ts` updates take effect at once, as the concurrency manager
  does (A3).
- D15 The concurrency manager's limit checks (S:components/concurrency_manager/src/lib.rs:242-262, 274-350) are cut,
  so `update_max_ts` always succeeds. `Timestamp::max()` is ignored (lib.rs:240-242).
- D16 TTLs: every predicate is kept and or-ed with `EVERY_TTL_EXPIRED` (T2). This applies to the client's
  check_ttl (C:requests.rs:732-741) and lock_until_expired_ms (lock.rs:769), and to the server's
  check_txn_status.rs:55 and 176. cleanup.rs:58-62 becomes `expire_time >= current && !EVERY_TTL_EXPIRED`.
  `physical(ts) = ts >> 18`, which is 0 for every timestamp (T1). The TTL-0 protocol (C:lock.rs:739-746) and
  `lock_info` setting TTL 0 for pessimistic transactions (S:actions/prewrite.rs:427-434) are kept.
- D17 Backoff (C:backoff.rs:30-50) keeps its counter and returns `Some(())` instead of a Duration. Nobody sleeps
  (T4). In `resolve_lock_with_retry` the backoff is never consumed, because its retry branches (region, leader,
  gRPC errors) cannot occur.
- D18 Cut per A6, marked in place: txn_status_cache (S:commands/prewrite.rs:507-533), check_max_ts_synced (568-579),
  fail points, metrics, logging, assertions (actions/prewrite.rs:196-208 return at once for `Assertion::None`), old
  values (CDC), shared locks, pipelined generations, `allow_lock_with_conflict` (the client's wake-up mode is
  Normal), lite resolve (ResolveLockLite), PessimisticRollbackReadPhase (the client never sends an empty key list;
  the port asserts this).
- D19 `Transaction.status` (an `AtomicU8` with a compare-exchange loop, C:transaction.rs:1064-1093) becomes a plain
  field. The drop check (1095-1123) is cut (P1).
- D20 Arguments (INTERFACE.md 1, 9), revised in review round 1: `parse_instance` takes exactly
  `<scenario> <async> <onepc> <a> [<fifo|bag> [<variant>]]` by position (4 to 6 arguments; the fifth must be
  `fifo` or `bag`, the sixth `correct`, `f1` or `f2`; anything else, a duplicate or a reordering panics). A
  variant therefore needs the model before it, as the prompt's runs give it (`A3 0 0 1 fifo f1`). The entry closure
  cannot see `--model` (program.rs `parse_opts`), so `main` reads `--model` from the command line as `parse_opts`
  does (default `fifo`, the default of `program::main`) and exits with code 2 and a message when the positional
  model differs, before anything runs.
- D21 Buffer `get_write_size` counts 2 bytes per Put (key and value one byte each, C:buffer.rs:284-293). It is only
  compared with the 16 KB TXN_COMMIT_BATCH_SIZE (transaction.rs:1711-1720), so the lock TTL is always
  DEFAULT_LOCK_TTL.
- D22 `retry_multi_region` carries the shard with every reply (`PreserveShard`). Only the plans that preserve shards
  read it (pessimistic lock, CheckSecondaryLocks).

Behaviour of the source under the shared abstractions. These are observations, not changes:
- Every TTL is expired, so a client that meets another client's pessimistic lock rolls it back at once
  (CheckTxnStatus with `resolving_pessimistic_lock`, check_txn_status.rs:55-59). In P2 and M2 the pessimistic
  clients take each other's locks. The loser's prewrite then fails with PessimisticLockNotFound, or, when no lock is
  left, succeeds through `amend_pessimistic_lock` (prewrite.rs:984-1035).
- A newer Rollback record also makes AcquirePessimisticLock fail: S:actions/acquire_pessimistic_lock.rs:171-194
  compares `commit_ts > for_update_ts` for any record type. So S1's pessimistic client can be `Aborted` (a = 1)
  when the other client's rollback record is newer than its for_update_ts.

## Runs (local machine, release build, `count impl`, `timeout 600`)

Before = the program as first delivered (the agent's runs, one at a time). After = review round 1 (binary md5
11c98428..., run three at a time on the 16-core machine, so the wall times are not comparable with Before; the
same twelve runs, also made before a machine pause, gave identical counts).

| Arguments | execs before | execs after | blocked after | wall after (s) | peak RSS after |
|---|---|---|---|---|---|
| `--model fifo -- S1 0 0 1` | 431 | 431 | 0 | 0.13 | 6.3 MB |
| `--model fifo -- X2 0 0 1` | 4480 | 7224 | 0 | 2.99 | 6.9 MB |
| `--model fifo -- X2r 0 0 1` | 19616 | 27892 | 0 | 13.5 | 7.6 MB |
| `--model fifo -- P2 0 0 1` | 59320 | 16812 | 0 | 4.95 | 7.9 MB |
| `--model fifo -- M2 0 0 1` | 34312 | timeout (> 900,000; > 1,000,000 in the pre-pause run) | 0 so far | 600 | 10.1 MB |
| `--model fifo -- X2 1 0 1` | 14016 | 350514 | 0 | 166.8 | 8.0 MB |
| `--model fifo -- X2 0 1 1` | 4 | 4 | 0 | 0.00 | 5.1 MB |
| `--model fifo -- L1 0 0 1` | 196 | 196 | 0 | 0.05 | 6.0 MB |
| `--model fifo -- A3 0 0 1` | 419884 | 489510 | 0 | 273.3 | 8.2 MB |
| `--model bag -- X2 0 0 1 bag` | 4480 | 7224 | 0 | 2.76 | 7.0 MB |
| `--model fifo -- A3 0 0 1 fifo f1` | 597098 | 666090 | 0 | 461.1 | 8.4 MB |
| `--model fifo -- L1 0 0 1 fifo f2` | 226 | 226 | 0 | 0.07 | 6.1 MB |

Every run except M2 printed `execs=<c> blocked=0`, then the measurement block with `stop_reason: exhausted`, and
exited 0; M2 was killed by `timeout` (exit 124) with 0 blocked executions so far. No run panicked.

- Why counts changed: X2, X2r, X2 1 0 1, A3 and M2 have requests with two shards (prewrite on `split`, and the
  async-commit secondaries of `xi`), whose shard tasks now interleave (D1); A3 and the others also have the
  ResolveLock read phase and write phase as two commands (D5). P2 went down because the old fan-out gave `ti` a
  receive from two regions whose two orders were two executions without a difference in behaviour (D1); now each
  shard task receives from its own region. S1, L1 and X2 0 1 1 have one key or one region per request and no
  ResolveLock that finds a lock after CheckTxnStatus, so they are unchanged.
- M2 does not finish in 600 s now: c2's two prewrite shards both meet c1's pessimistic locks and both resolve
  them (TSO, CheckTxnStatus at k1, PessimisticRollback / ResolveLock) concurrently. It needs a longer budget or a
  bigger machine; it was not run on a cluster node (not allowed in this round).
- The review's A3/f1 schedule (item 1) now occurs. `export` of `A3 0 0 1 fifo f1` (the visible words of c1..c3,
  ignoring the per-summary counts): before round 1, 6 distinct word tuples and none with a reader `Aborted`;
  after, 12, a superset of the 6, with 3 in which c2 and 4 in which c3 is answered `Aborted`. Under `correct`, A3
  has 5 distinct word tuples after round 1 and no reader `Aborted`.
- fifo and bag give the same count for X2 (7224 both). This is expected (INTERFACE.md 7): no channel has two
  messages in flight here: every process waits for each reply, each shard task sends to one region, and `init`
  sends one message per process.
- X2 0 1 1 (1PC, one region) has 4 graphs: 2 TSO orders times 2 orders at `r1`.
- Sanity check of f2 (`export`, L1; from before round 1, L1's count is unchanged): the summaries in which both clients read `Value(0)` and both commit (a lost
  update) number 0 under `correct` and 22 under `f2`.

## Open questions

- Q1 f1 scope (D8): should the skipped `put_write` (check_txn_status.rs:289-291) apply only when the caller is
  CheckTxnStatus (INTERFACE.md 9's wording), or to every caller of `check_txn_status_missing_lock`, including
  `cleanup` from BatchRollback and ResolveLock (the README's line-level wording)? The port does the latter. The
  CheckTxnStatus-only reading needs a caller flag, which is one changed line.
- Q2 D1: closed in review round 1 (the shard tasks run concurrently, D1).
- Q3 closed after round 1: the two files were fetched at the pins and compared (D3, D4).
- Q4 Daemon threads: `ti`, `xi`, `pd` and the regions use `spawn_daemon`, as INTERFACE.md 2's "never end" suggests
  and as Java's `MpProcess.spawnDaemon` does. With ordinary threads, every execution would end blocked.
- Q5 closed: coordinator ruling, keep the narrowed interface (D7).

## Review round 1 (REVIEW-astra-rust.md, 2026-11-09)

Applied:
1. MUST-FIX D5: ResolveLockReadPhase and ResolveLock are now two commands; the read phase's NextCommand is a
   message from the region to itself (D5; `process_resolve_lock_readphase`, `process_resolve_lock`,
   `Region::handle_next`, `region_main`). The review's A3/f1 schedule (a commit between selection and cleanup,
   cleanup returning `Committed`, S:actions/cleanup.rs:108-120) is now possible, and it is reached: under f1 the
   readers c2 and c3 can now be answered `Aborted`, which the program before round 1 never did (see the runs).
2. MUST-FIX D1: no initial barrier and no fixed continuation order. Each shard task of a request with two or more
   shards is its own thread running dispatch, lock resolution and re-dispatch (D1; `retry_multi_region`,
   `single_shard_handler`, `resolve_lock_execute`). A single-shard request runs in the caller (the caller only
   awaits it, C:request/plan.rs:398-411). Cost: M2 no longer finishes within 600 s (see the runs).
3. MUST-FIX arguments: positions and cardinality validated, positional `fifo|bag` compared with `--model`
   before `program::main` (D20). Checked: `--model fifo -- X2 0 0 1 bag`, `--model bag -- X2 0 0 1 fifo`,
   `-- X2 0 0 1 bag` and `--model fifo -- X2 0 0 1 f1` exit 2 with a message; `... fifo f1 x` panics on the
   argument count.
4. MUST-FIX GetForUpdate: reconciled by narrowing (D7): GetForUpdate stays supported for keys later Put; a
   GetForUpdate-only key, which would need the Lock mutation that INTERFACE.md 6 excludes, is refused at argument
   time; the citation is corrected to C:transaction/buffer.rs:403. The other reconciliation (authorising the
   source's Lock-mutation path) changes the interface; the coordinator ruled to keep the narrowing (Q5, D7).
5. NOTE Q1: kept as it is (f1 at the line, every caller, D8).
7. NOTE: kept: the `resolving_pessimistic_lock` unlock without a rollback record
   (S:actions/check_txn_status.rs:46-63), the conditional amend (S:actions/prewrite.rs:988-1017), and
   AcquirePessimisticLock's `commit_ts > for_update_ts` before the record type
   (S:actions/acquire_pessimistic_lock.rs:161-194).
8. NOTE: nothing to change.

6. SHOULD-FIX Q3 (D3, D4), done after round 1 once the coordinator had fetched C:store/errors.rs (client-rust
   1b883bf) and S:server/service/kv.rs (tikv 96033a2). Compared line by line (D3, D4): every response field and
   every error classification the cut reaches agreed with the reconstruction. Two places were rewritten to follow
   the source literally without a change in behaviour: the PessimisticLock response through
   `into_legacy_values_and_not_founds` (kv.rs:2503-2533, storage/types.rs:371-403), and the CheckTxnStatus
   `lock_info` built with `lock.primary` (kv.rs:2593-2595). Comments citing "not on disk" now cite the files.
   `From<kvrpcpb::KeyError> for Error` (C:common/errors.rs:161-164, fetched last) is `Error::KeyError(Box::new(e))`,
   the port's assumption; no change, no rerun. Rerun after the change
   (binary md5 32e378b4...; counts): S1 431, X2 7224, bag X2 7224, X2r 27892, P2 16812,
   X2 1 0 1 350514, X2 0 1 1 4, L1 196, L1 f2 226, all `blocked=0`, identical to the table above. A3, A3 f1 and M2
   were not rerun: their requests reach CheckTxnStatus `Uncommitted` (the only `lock_info` path) only for a live
   async-commit lock, which A3 and M2 (`async = 0`, every TTL expired) never produce, and none of them sends a
   PessimisticLock with `return_values`; M2 times out in any case.

No finding of the review is disputed.
