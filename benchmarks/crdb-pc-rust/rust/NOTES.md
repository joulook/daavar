# crdb-pc / rust: CockroachDB parallel commits with transaction recovery (Must API port)

Source: cockroachdb/cockroach at d30c905 (the cut in `plan/case-studies/cockroach/cr-src`, README there).
Interface: `plan/case-studies/cockroach/INTERFACE-agents.md`. The code is `src/main.rs` (one file).
Arguments: `<n> <p> <m> <a> [<fifo|bag>]`. Both `Program` entries (`spec`, `imp`) return the implementation `imp`.

## Processes and messages

Threads, started in this order by the entry closure: `r1`, `r2` (n = 2), `tcx`, `tc`, `h1..hm` (all daemons,
never terminate), `c`, `p1..pm` (visible, the interface's client loops verbatim), then `init`. The main thread
only spawns and joins `c`, `p1..pm`, `init`.

Every invisible message is one Rust type `Msg`, sent tagged; the tag tells the receiver which variant it is,
so every receive is a typed, selective receive of what the current step expects:

| tag | variant | from -> to |
|---|---|---|
| `T_SETUP` = 1 | `Setup { c, ps, tc, tcx, hs, rs, meta }` | `init` -> every invisible process, once; each process's first receive |
| `T_BATCH` = 2 | `Batch { id, from, txn, ts, reqs, reply }` | tc, tcx, h_i -> r_k (a BatchRequest sub-batch for one range); r1 -> r2 (async ResolveIntent, `reply = false`) |
| `T_RESP + id` | `Resp(BatchResp { id, txn, result })` | r_k -> the batch's sender; the tag names the one request it answers |
| `T_EXPLICIT` = 3 | `Explicit { txn, lock_spans }` | tc -> tcx (start the async explicit commit) |
| untagged | `Req::Put/Commit/Recover`, `Rep::*` | c <-> tc, p_i <-> h_i (the visible messages, verbatim) |

`tc` receives client requests only untagged and from `c`; `h_i` only untagged from `p_i`; a range only `T_BATCH`;
`tcx` only `T_EXPLICIT`; a sender of a batch only the `T_RESP + id` of an outstanding id. Requests in a batch
(`Request`): `Put{key,seq}`, `QueryIntent{key,txn,error_if_missing}`, `QueryTxn{txn}`, `EndTxn{commit,lock_spans,
in_flight}`, `PushTxn{pushee}`, `RecoverTxn{txn,implicitly_committed}`, `ResolveIntent{key,txn,status}`;
responses `Response::*`; errors `ErrKind::{IntentMissing, TransactionRetry(Serializable|AsyncWriteFailure),
TransactionAborted(reason), StatusCommitted, IndeterminateCommit(record)}` with the error's request index and txn.

### Role by role

- `init`: sends `Setup` (all addresses; T's meta: epoch 0, write ts 1, min ts 1; id and anchor key k1 implicit).
- `c`, `p_i`: the interface's `committer` and `preventer`, verbatim.
- `tc` (`tc_proc`, struct `Tc`): the interceptor stack pipeliner -> committer -> span refresher -> DistSender.
  - `Put(k)`: one Put with the next sequence number, async consensus; the reply's txn is merged; the write is
    recorded as in-flight; reply `Written(k)`.
  - `Commit(keys)`: batch [Put(k) for the final keys, EndTxn]. The pipeliner attaches the lock footprint and the
    in-flight writes (pipelined + final Puts, by sequence), and chains a pre-commit QueryIntent
    (ErrorIfMissing) for every pipelined in-flight write before the EndTxn. The committer keeps the in-flight writes
    (canCommitInParallel). The span refresher refreshes preemptively when write ts > read ts. The DistSender splits
    off the pre-commit QueryIntents and sends, all at once, the EndTxn batch per range (r1: [Put k1?, EndTxn];
    r2: [Put k2]) and the QueryIntent batch per range; it waits for all responses. If the EndTxn batch failed, its
    error wins. If a QueryIntent reported IntentMissing, tc sends QueryTxn to r1: COMMITTED -> ignore the error;
    ABORTED with a record -> TransactionAborted; otherwise the IntentMissing error, which the pipeliner turns into
    TransactionRetry(ASYNC_WRITE_FAILURE). On success with status STAGING: if the staging ts < the combined write
    ts the commit is not implicit and tc retries the EndTxn alone (in-flight writes moved to lock spans, txn
    PENDING, refreshed first), which commits explicitly or fails; else T is implicitly committed: tc sends
    `Explicit` to `tcx` and replies `Committed`. Errors: TransactionAborted -> `Aborted`; TransactionRetry ->
    `Retry` and an epoch restart.
- `tcx` (`tcx_proc`): on `Explicit`, sends EndTxn(commit, lock spans, no in-flight writes) to r1 and waits; an
  "already committed" error is tolerated.
- `h_i` (`handle_proc`): on `Recover`: PushTxn(PUSH_ABORT, pusher always wins) at r1 with the setup meta. On
  IndeterminateCommit(staging record): recovery (`resolve_indeterminate_commit`): one batch QueryTxn + one
  QueryIntent (no ErrorIfMissing) per in-flight write, sorted by key, at the record's epoch and write ts (r1:
  [QueryTxn, QI k1?], r2: [QI k2?]); if the record changed (finalized, higher epoch or ts) stop, else RecoverTxn
  with ImplicitlyCommitted = no QueryIntent missed an unpushed intent; then the push is retried. With the finalized
  record: ResolveIntent(k, status) for every key k1..kn (acknowledged), then `Recovered(fate)` to `p_i`.
- `r1`, `r2` (`range_proc`, `RangeState`): evaluate one batch at a time, atomically (on an error nothing is
  applied except the timestamp-cache bump of a QueryIntent at the error index), reply when `reply` is set. State:
  the key's cell (T's intent (epoch, ts, seq) or none; resolved value ts or none), the timestamp-cache entry of the
  key; on r1 also T's record and its tombstone and push markers. After a batch that finalizes T's record by
  EndTxn, r1 resolves k1 inside the batch and sends `ResolveIntent` for k2 to r2 (no reply).

## Go function -> Rust function

| Go (file: function, lines) | Rust |
|---|---|
| txn_interceptor_committer.go `SendLocked` l.138-261 | `Tc::committer_send` |
| committer `canCommitInParallel` l.349-396 | `Tc::committer_send` (the `parallel` test) |
| committer `isTxnCommitImplicit` l.413-441 | `Tc::committer_send` (`implicit`) |
| committer `retryTxnCommitAfterFailedParallelCommit` l.443-486 | `Tc::committer_send` (`!implicit` branch) |
| committer `makeTxnCommitExplicitAsync` l.492-527 | `Tc::committer_send` (send `Explicit`) + `tcx_proc` |
| committer `makeTxnCommitExplicitLocked` l.529-594 | `tcx_proc` |
| committer `maybeRemoveStagingStatusInErr` | `Tc::committer_send` (error branches) |
| txn_interceptor_pipeliner.go `SendLocked` l.311-361 | `Tc::pipeliner_send` |
| pipeliner `attachLocksToEndTxn` l.437-501 | `Tc::pipeliner_send` (EndTxn branch) |
| pipeliner `canUseAsyncConsensus` | `Tc::pipeliner_send` (`async_consensus`) |
| pipeliner `chainToInFlightWrites` l.608-718 | `Tc::chain_to_in_flight_writes` |
| pipeliner `updateLockTrackingInner` l.796-899 | `Tc::update_lock_tracking` |
| pipeliner `adjustError` l.929-954 | `Tc::pipeliner_send` (error branch) |
| dist_sender.go `divideAndSendParallelCommit` l.1567-1723 | `Ds::send_txn` |
| dist_sender `divideAndSendBatchToRanges` (division per range, combine) | `Ds::dispatch`, `Ds::send` |
| dist_sender `detectIntentMissingDueToIntentResolution` l.1746-1798 | `Ds::detect_intent_missing_due_to_intent_resolution` |
| dist_sender `mergeErrors` l.1819-1831 | `Ds::dispatch` (lowest index; never two errors) |
| txn_interceptor_span_refresher.go `SendLocked`, `maybeRefreshPreemptively` | `Tc::refresher_send` |
| span refresher `sendLockedWithRefreshAttempts` | `Tc::send_with_refresh_attempts` |
| span refresher `maybeRefreshAndRetrySend` l.249-324, `maxTxnRefreshAttempts` l.30 | `Tc::maybe_refresh_and_retry_send`, `MAX_TXN_REFRESH_ATTEMPTS` |
| span refresher `splitEndTxnAndRetrySend` | `Tc::split_end_txn_and_retry_send` |
| TxnCoordSender result handling / epoch restart (not in the cut) | `Tc::finish` |
| batcheval/cmd_end_transaction.go `EndTxn` l.248-605 | `RangeState::end_txn` |
| batcheval/cmd_query_intent.go `QueryIntent` l.60-167 | `RangeState::query_intent` |
| batcheval/cmd_query_txn.go `QueryTxn` l.48-89 | `RangeState::eval_one` (QueryTxn arm) |
| batcheval/cmd_recover_txn.go `RecoverTxn` l.52-234 | `RangeState::recover_txn` |
| batcheval/cmd_push_txn.go `PushTxn` l.111-350 | `RangeState::push_txn` |
| batcheval/transaction.go `CanCreateTxnRecord` l.119-134 | `RangeState::can_create` (+ the PENDING assertion in `end_txn`) |
| transaction.go `BumpToMinTxnCommitTS` l.139-147 | `RangeState::end_txn` (forward to the push marker) |
| transaction.go `SynthesizeTxnFromMeta` l.167-203 | `RangeState::synthesize` |
| replica_tscache.go `updateTimestampCache` l.90-369 (EndTxn, RecoverTxn, PushTxn, QueryIntent) | `RangeState::eval_batch` (after the loop; QI on error in the error branch) |
| replica_tscache.go `applyTimestampCache` l.397-464 | `RangeState::eval_batch` (before the loop) |
| replica_tscache.go `CanCreateTxnRecord` l.598-656 | `RangeState::can_create` |
| replica_batch_updates.go `maybeStripInFlightWrites` l.37-170 | `RangeState::strip_in_flight_writes` |
| replica_send.go `handleIndeterminateCommitError` l.818-838, call site l.593-600 | `handle_proc` (push loop) |
| txnrecovery/manager.go `ResolveIndeterminateCommit` l.82-110, `resolveIndeterminateCommitForTxn` l.120-151 | `resolve_indeterminate_commit` |
| manager `resolveIndeterminateCommitForTxnProbe` l.158-267 | `resolve_indeterminate_commit` (probe) |
| manager `resolveIndeterminateCommitForTxnRecover` l.279-299 | `resolve_indeterminate_commit` (RecoverTxn) |
| manager `batchTimestamp` l.306-310 | `resolve_indeterminate_commit` (`ts`) |
| storage `MVCCResolveWriteIntent` (replaced by the cell) | `RangeState::resolve` |
| `resolveLocalLocks` + intent resolver (replaced) | `RangeState::end_txn` (local) + `range_proc` (ResolveIntent to r2) |

## Decisions and simplifications

- D1 Crate name. The interface names the crate `crdb_pc`; the prompt fixes `[package] name = "bench_crdb_port"` and
  the run command uses `target/release/bench_crdb_port`. The prompt wins.
- D2 Typed receives. Message kinds are tags (table above); each process receives exactly the kind its current
  step expects, so a client request waits in tc's mailbox until tc is idle, and the setup is always received first.
- D3 Order of receiving responses. The source waits for all sub-batch responses of a divided batch
  (divideAndSendBatchToRanges) and for the pre-commit QueryIntent batch after the EndTxn batch
  (dist_sender l.1670-1675, `qiReply := <-qiResponseCh`). README: "tc sends both sub-batches and receives both
  replies in either order". Nothing is sent between these receives and the combination does not depend on their
  order, so the port receives them in a fixed order (group, then range); receiving in arrival order would only
  repeat identical outcomes (2 1 1 1 went from 1428 to 120 executions when D3 and D9 were applied together).
- D4 Order of sending. The pre-commit QueryIntent batch is sent by a goroutine racing the EndTxn batch
  (dist_sender l.1643-1660); when both go to the same range (r1) and channels are ordered, the order is a `nondet()`
  choice. Sub-batches to different ranges are independent. With the optional 5th argument `bag` the choice is
  skipped (the channel itself is unordered).
- D5 Batch timestamp. `updateTimestampCache` uses `ba.Timestamp` (l.106); for a transactional batch that is the
  read timestamp; the tombstone is bumped to it (EndTxn l.163-180). For non-transactional batches it is the header
  ts: the push uses the pushee meta's write ts, the recovery `batchTimestamp` (record write ts), detectIntentMissing's
  `clock.Now()` (l.1750) is T's write ts, a handle's ResolveIntent uses the record's write ts.
- D6 Timestamp-cache identities. The interface's cache has no txn ids. Key entries come only from QueryIntents,
  which add with `uuid.UUID{}` (l.336), so the `ba.Txn.ID != rTxnID` test of applyTimestampCache (l.421) always
  holds. The tombstone's txn id only selects the abort reason in CanCreateTxnRecord (l.613-652, every case fails);
  the port reports `TimestampCacheRejected`.
- D7 No server-side refresh. Server-side retries/refreshes (replica_evaluate, not in the cut) are not modelled: if
  the write ts of a batch with EndTxn is bumped on r1, EndTxn fails with RETRY_SERIALIZABLE
  (IsEndTxnTriggeringRetryError, l.439) and the client-side span refresher refreshes and retries with the EndTxn
  split off (`splitEndTxnAndRetrySend`). Unreachable here (README reachability notes), kept as code path.
- D8 Refresh. T has no read spans: `tryRefreshTxnSpans` always succeeds and `maybeRefreshPreemptively` (l.412) is
  free, so it is done whenever write ts > read ts (read ts := write ts).
- D9 EndTxn on an ABORTED record. The code returns T's lock spans for resolution with the error (alwaysReturn,
  cmd_end_transaction l.347-364, `reply.Txn.LockSpans = args.LockSpans` l.361). The interface: r1 resolves "after
  a batch that finalizes T's record". This batch does not finalize it, so no resolution is sent; the record became
  ABORTED by a handle, which resolves every key itself.
- D10 Folded intent resolver. r1's ResolveIntents to r2 are fire-and-forget (`reply = false`); the resolver's
  outcome is never consulted in the source. A handle's ResolveIntents are acknowledged and awaited before
  `Recovered` (the pusher resolves before it proceeds).
- D11 ResolveIntent COMMITTED makes a value at the intent's timestamp (interface), not at the commit timestamp;
  an intent of an older epoch is removed (MVCCResolveWriteIntent behaviour; never reached, epoch is always 0).
- D12 Push. Pusher meta = the setup meta (interface). `pusherWins` always with PUSH_ABORT (l.224-229); the
  `LastActive()` forward (l.316) is a no-op without heartbeats; `knownHigherTimestamp/Epoch` kept (l.207-215).
  Missing record: synthesized; ABORTED if the tombstone forbids a record, else aborted without writing a record;
  either way the tombstone is bumped to the pushee's min ts (replica_tscache l.202-266).
- D13 Unreachable paths panic instead of inventing a client outcome: an ambiguous result in
  detectIntentMissingDueToIntentResolution (l.1763, l.1789; a record exists once the EndTxn batch succeeded, no GC),
  a TransactionStatusError at tc, a failed pipelined Put, tcx errors other than "already committed" (the source
  only logs them, l.512, l.557), and the source's assertion failures. None was reached in any run (a panic aborts
  the run with a message; checked by injecting one).
- D14 Epoch restart (TxnCoordSender, not in the cut; unreachable here): epoch + 1, write and read ts := the error
  txn's write ts, in-flight writes moved to the lock footprint, sequence numbers keep increasing.
- D15 `Transaction.Update` reduced to: max epoch, max timestamps, min min-ts, status moves forward only
  (PENDING < STAGING < finalized). A record carries no read ts (read ts 0, forwarded by the header in Update).
- D16 `tc.mu` (makeTxnCommitExplicitAsync l.509): tcx runs independently of tc; T's client has finished once tc
  answered `Committed`, so nothing contends for the lock.
- D17 Recovery without in-flight writes skips the probe loop (manager l.228) and recovers with
  ImplicitlyCommitted = true (cannot happen: a STAGING record always has in-flight writes here).
- D18 Each handle serves `Recover` requests in a loop (it never terminates); its client sends one.
- D19 Errors: at most one sub-batch fails (README "Errors"); the combined error keeps the lowest request index,
  the error's txn is merged with the other sub-batches' txns (`pErr.UpdateTxn`).
- D20 QueryTxn's WaitingTxns, the AbortSpan, heartbeats, GC, latches, Raft are cut as the README says.

## Build and runs

Compile (inside OUT_DIR): `TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2` — finished, no warnings.

Runs (inside OUT_DIR), each `timeout 600 target/release/bench_crdb_port count impl --model fifo -- <args>`.
The program's result line is `execs=… blocked=…`; the last line printed is the last line of the measurement block
(`cache_hits: `, empty).

| args | result line | last output line | wall |
|---|---|---|---|
| `1 1 1 1` | `execs=43 blocked=0` | `cache_hits: ` | < 1 s |
| `2 1 1 1` | `execs=120 blocked=0` | `cache_hits: ` | < 1 s |
| `2 2 1 1` | `execs=140 blocked=0` | `cache_hits: ` | < 1 s |
| `2 0 1 1` | `execs=24 blocked=0` | `cache_hits: ` | < 1 s |
| `2 1 2 1` | `execs=274146 blocked=0` | `cache_hits: ` | 266 s |

All five exit 0, `stop_reason: exhausted`, no panic. A separate check build (a copy outside OUT_DIR, not a
deliverable) asserted that tc's `Committed`/`Aborted` and every handle's `Recovered(fate)` agree within each
execution: no disagreement in any of the five configurations, or in `1 1 2 1` (12864 executions).
