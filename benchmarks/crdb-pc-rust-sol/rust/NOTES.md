# CockroachDB parallel commits port

Source: `cockroachdb/cockroach` at `d30c905fff79ef825adc96bcc647f1872a90f2ff`, supplied in `cr-src`. The supplied README was read first. `INTERFACE.md` takes precedence over the source wherever it specifies a replacement. Both `Program` entries run this implementation. The package name follows the task's `bench_crdb_sol`, overriding the interface's older `crdb_pc` name.

## Roles and messages

All protocol state is thread-local. Every interprocess exchange uses Must messages. The entry starts `r1`, optional `r2`, `tcx`, `tc`, `h1..hm`, `c`, `p1..pm`, then `init`, in that order, and only joins threads. The visible set is exactly `c,p1..pm`. Clients and their message declarations are copied from the interface.

`init` sends one `Setup` to each range, `tcx`, `tc`, and handle. Setup includes every process address, including init's, and T's initial metadata. Clients know their destinations from their creation arguments, as in the supplied client loops. Init itself constructs setup and exits. It does not need to message itself. Invisible service threads block forever after completing their work; they never return.

`Batch` contains the sender address, a reply phase, a recovery round, transaction metadata, and an ordered vector of operations. A range consumes an entire Batch atomically, evaluating its operations in vector order. There is no shared transaction object between processes.

| Message | Sender and receiver | Meaning |
|---|---|---|
| `Setup` | init → invisible service | Addresses and initial T metadata |
| `Req::Put(k)` | c → tc | One pipelined write |
| `WriteReply` | rk → tc | The async-consensus Put was applied; carries bumped timestamp |
| `Rep::Written(k)` | tc → c | Put acknowledged and entered in coordinator in-flight set |
| `Req::Commit(keys)` | c → tc | Final writes and commit |
| `ParallelReply` | ranges → tc | One range sub-batch of the final writes/EndTxn or pre-commit queries |
| `CheckReply` | r1 → tc | QueryTxn after a pre-commit IntentMissing |
| `ExplicitReply` | r1 → tc or tcx | EndTxn alone, for retry or async explicit commit |
| `MakeExplicit(txn,locks)` | tc → tcx | Start the asynchronous explicit commit after replying Committed |
| `Rep::{Committed,Aborted,Retry}` | tc → c | Commit result, preserving the interface client loop |
| `Req::Recover` | pi → hi | Push and recover T once |
| `PushReply` | r1 → hi | Finalized status, or IndeterminateCommit carrying the staging record |
| tagged `ProbeReply` | ranges → hi | QueryTxn plus QueryIntents; tag identifies recovery attempt |
| `RecoveryReply` | r1 → hi | RecoverTxn result, after which hi retries its original push |
| `ResolveReply` | ranges → hi or r1 | Completion of an intent resolution |
| `Rep::Recovered(fate)` | hi → pi | Final fate observed by the retried push, after handle resolutions |

Range operations inside Batch:

* `Put(Write)`: first apply the key's timestamp cache to the entire range batch; lay T's intent with epoch, bumped write timestamp, sequence. `Phase::Write` identifies the pipeliner's async-consensus operation; its reply follows application.
* `QueryIntent { write, own, error_if_missing }`: compare epoch and sequence (`stored sequence >= queried sequence`), then timestamp. An own-transaction query forwards the returned transaction timestamp when it finds a pushed intent. Missing or pushed intents bump the key cache to the **queried** timestamp. Only a missing matching intent raises IntentMissing, and only when error_if_missing is true. A committed value is not an intent.
* `EndTxn { flight, locks }`: strip same-batch Puts from flight and add them to locks. Consult an existing record or check the tombstone before creating one. A committed record returns AlreadyCommitted; an aborted record returns TransactionAborted. Merge epoch/timestamps, check serializability, then stage if flight remains or explicitly commit otherwise. Successful EndTxn bumps the record tombstone, including staging EndTxn. Explicit finalization resolves local locks and sends remote resolutions.
* `QueryTxn`: read the record or synthesize Pending/Aborted from metadata and markers; return whether an actual record exists.
* `PushTxn`: always-winning abort push. A missing record is synthesized and tombstoned without creating a record. Pending is aborted. Finalized status is returned. Staging requires recovery unless the push already knows a greater timestamp or epoch. The actual handles retain their initial metadata on retried pushes.
* `RecoverTxn { implicitly_committed }`: validate the current record against the observed staging metadata. A changed live record stops this recovery; a finalized record stays finalized. In particular, a committed record is accepted even if the probe reported a missing intent. Otherwise merge flight into locks, clear flight, and write Committed or Aborted. Bump the tombstone for a finalized reply. RecoverTxn itself does **not** resolve intents.
* `ResolveIntent(fate)`: commit intent → timestamp-only value, or remove aborted intent; no-op if no intent. No AbortSpan and no acknowledgement consumption at r1 for its asynchronous resolutions.

The coordinator tracks pipelined writes after their replies. Commit adds final point writes to EndTxn's in-flight list and sends final sub-batches concurrently with pre-commit QueryIntents. Each query is own-transaction and ErrorIfMissing. EndTxn-half errors win. After a successful EndTxn, a missing pre-commit intent requires QueryTxn: Committed suppresses the error; a stored Aborted record produces TransactionAborted; a live record retains IntentMissing, which `adjust_error` turns into RETRY_ASYNC_WRITE_FAILURE.

For a staging reply, implicit commit means the staging timestamp is at least the maximum returned write timestamp. On success tc replies Committed, then starts tcx. Otherwise all writes have returned, so tc moves their keys to locks and retries **EndTxn alone**. Its first serialization retry causes a blind-write refresh (read := write), followed by the same EndTxn alone. It never reissues the successfully pushed write on this path. T's client sees Aborted if recovery wins that race. Tcx tolerates AlreadyCommitted if another recovery finalized the record first.

Each handle independently pushes. On IndeterminateCommit it sends one probe batch containing QueryTxn and one QueryIntent per recorded in-flight write, split atomically by range. QueryTxn and QueryIntent(k1), when present, share a single r1 batch. Probe metadata is the record's epoch/write timestamp. A changed/finalized record short-circuits recovery. Otherwise RecoverTxn's ImplicitlyCommitted is true exactly when every probe found an unpushed intent. After recovery the handle retries the original push, resolves all keys with its finalized result, waits for those resolution replies, and replies to pi.

## Function mapping

Paths below are relative to `cr-src/pkg/kv/`. Rust functions are in `src/main.rs`; inline cases are named explicitly rather than pretending there is a separate interceptor framework.

| Go file and function (source lines) | Rust function / case |
|---|---|
| kvclient/kvcoord/txn_interceptor_committer.go `SendLocked` 138–261 | `coordinator`, `divide_and_send_parallel_commit`, `outcome` |
| same, `canCommitInParallel` 349–396 | `coordinator` Commit construction: only eligible point Puts and QueryIntents, no triggers/prepare |
| same, `isTxnCommitImplicit` 413–441 | `coordinator`: `end.staging >= ts` |
| same, `retryTxnCommitAfterFailedParallelCommit` 443–486 | `retry_txn_commit_after_failed_parallel_commit`, `explicit_end` |
| same, `makeTxnCommitExplicitAsync` 492–527 | `MakeExplicit` send, `explicit_task` |
| same, `makeTxnCommitExplicitLocked` 529–594 | `explicit_end`, `explicit_task` AlreadyCommitted handling |
| kvclient/kvcoord/txn_interceptor_pipeliner.go `SendLocked` 311–361 | `coordinator` Put and Commit cases |
| same, `attachLocksToEndTxn` 437–501 | `coordinator`: tracked flight plus final writes; `Op::EndTxn` locks/flight |
| same, `chainToInFlightWrites` 608–718 | `divide_and_send_parallel_commit` own/ErrorIfMissing queries |
| same, `updateLockTrackingInner` 796–899 | `coordinator`: insert flight on write reply; clear on commit; merge into locks after proven writes |
| same, `adjustError` 929–954 | `adjust_error` |
| kvclient/kvcoord/dist_sender.go `divideAndSendParallelCommit` 1567–1723 | `divide_and_send_parallel_commit`, `coordinator` error precedence |
| same, `detectIntentMissingDueToIntentResolution` 1746–1798 | `detect_intent_missing_due_to_intent_resolution` |
| kvclient/kvcoord/txn_interceptor_span_refresher.go `maybeRefreshAndRetrySend` 249–324 | `maybe_refresh_and_retry_send`, retry helper and coordinator serialization-retry branch |
| kvserver/batcheval/cmd_end_transaction.go `EndTxn` 248–605 | `Range::end_txn`, `Range::evaluate` EndTxn case |
| same, `IsEndTxnTriggeringRetryError` 617–635 (called helper) | `Range::end_txn`: read/write timestamp comparison |
| kvserver/batcheval/cmd_query_intent.go `QueryIntent` 60–167 | `Range::query_intent` |
| kvserver/batcheval/cmd_query_txn.go `QueryTxn` 48–89 | `Range::query_txn` |
| kvserver/batcheval/cmd_recover_txn.go `RecoverTxn` 52–234 | `Range::recover_txn` |
| kvserver/batcheval/cmd_push_txn.go `PushTxn` 111–350 | `Range::push_txn` |
| kvserver/batcheval/transaction.go `CanCreateTxnRecord` 119–134 | `Range::can_create_txn_record`, `Range::end_txn` rejection |
| same, `BumpToMinTxnCommitTS` 139–147 | `Range::end_txn` push-marker forwarding |
| same, `SynthesizeTxnFromMeta` 167–203 | `Range::synthesize_txn_from_meta` |
| kvserver/replica_tscache.go `updateTimestampCache` EndTxn 163–180 | `Range::end_txn` successful tombstone update |
| same, RecoverTxn 187–201 | `Range::recover_txn` finalized tombstone update |
| same, PushTxn 202–266 | `Range::push_txn` minimum-timestamp tombstone update |
| same, QueryIntent 307–338 | `Range::query_intent` missing/unpushed cache update |
| same, `applyTimestampCache` 397–464 | `Range::evaluate` batch timestamp forwarding before operations |
| same, `CanCreateTxnRecord` 598–656 | `Range::can_create_txn_record` |
| kvserver/replica_batch_updates.go `maybeStripInFlightWrites` 37–170 | `Range::evaluate` EndTxn case: remove same-batch Put sequences |
| kvserver/replica_send.go `handleIndeterminateCommitError` 818–838, retry call 593–600 | `handle`: recover, then retry original PushTxn |
| kvserver/txnrecovery/manager.go `ResolveIndeterminateCommit` 82–110 | `handle` IndeterminateCommit branch |
| same, `resolveIndeterminateCommitForTxn` 120–151 | `resolve_indeterminate_commit` |
| same, `resolveIndeterminateCommitForTxnProbe` 158–267 | `resolve_indeterminate_commit` probe construction and reply checks |
| same, `resolveIndeterminateCommitForTxnRecover` 279–299 | `resolve_indeterminate_commit` RecoverTxn send/receive |
| same, `batchTimestamp` 306–310 | recovery Batch metadata uses the staging transaction's write timestamp |

## Decisions and simplifications

1. T's identity and anchor k1 are structural constants: the only cells/record belong to T, and all record operations address r1. Metadata stores the variable fields (epoch, timestamps, sequence in Write). This is the interface's one-T abstraction, replacing general IDs/keys in transaction.go 167–203 and QueryIntent 112–119. Initial epoch/read/write/min are 0/1/1/1.
2. Each range has one cell and therefore a scalar key cache; its key is fixed by its range number. R1 additionally has separate tombstone and push scalars. This is exactly the interface's map specialized to one key per range; the push marker stays zero because no timestamp push is generated. No lease/watermark/owner heuristics from replica_tscache.go 598–656 remain.
3. Serializable isolation is chosen. The source leaves isolation configurable (cmd_end_transaction.go 617–635, span refresher 269–277). T has no read spans, so refresh always succeeds and simply forwards its read timestamp. The bounded automatic-refresh loop is specialized to the single necessary refresh: once all writes are acknowledged, no actor can push a staging record's timestamp; the second explicit EndTxn can only succeed or find a finalized record. The source's refresh-attempt budget is at span_refresher.go 30 and 249–324.
4. Put acknowledgements represent the supplied async-consensus abstraction: application is complete when the reply is sent. There is no invented failed replication or intent loss. Source pipeliner 332–360, 877–892, and README reachability notes. Consequently RETRY_ASYNC_WRITE_FAILURE and epoch restart paths remain represented but have no manufactured trigger.
5. The single-flight set is a sorted vector of `(key, sequence)` rather than a tree. Sequences increment in client request order and remain monotonic if an unreachable Retry advances the epoch. A retry clears flight, preserves all keys as locks, increments epoch, and advances logical time. General overlapping/ranged operations and ignored sequence numbers are explicitly excluded by the interface (pipeliner 437–501, 796–899).
6. Because this client never makes another write after a query proves a pipeline write, proven flight need not be individually migrated through a reusable interceptor object. Successful commits clear flight; failed parallel commits send all keys as lock spans. Error handling retains all attempted keys. This specializes pipeliner 796–899 without changing any request in this cut.
7. DistSender dispatches r1/r2 messages from tc. Its source goroutine has no specified order relative to the EndTxn sender (dist_sender.go 1626–1660). A `nondet()` chooses whether query-group sends or final-group sends happen first. Within a group, increasing range order is canonical; different ranges can receive independently. The only competing same-channel sub-batches are pre-commit QueryIntent(k1) and EndTxn. No independent artificial nondeterminism is introduced for responses: tc accepts ParallelReply messages in arrival order and collects both source halves (1663–1723).
8. Recovery sends one probe to each involved range. QueryTxn and QueryIntent(k1) are in the same atomic r1 batch, preserving batching, rather than separate messages. At most two in-flight writes fit the source's 128-request batch (manager.go 230–267). If a QueryTxn response already proves change/finalization, the logical recovery ends immediately; late responses are left unreceived, tagged by attempt. Source checks QueryTxn before examining intent results after db.Run (242–258); discarding remaining replies early only omits unused observations. Sent batches still execute and update caches. This follows the task's explicit finite-exploration requirement.
9. A handle waits for all its ResolveReply messages before replying Recovered. The supplied replacement requires resolution of every key but does not prescribe acknowledgement timing (README replacement of the absent intent resolver; RecoverTxn 226–229 explicitly has no resolver). R1's own asynchronous remote resolutions have replies that r1 never receives; it waits only for Batch, as requested for late/unneeded responses.
10. `resolve` follows the interface literally: a committed resolution stores the intent's timestamp and discards transaction identity; aborted resolution removes only the intent. A resolution with no intent is a no-op. General MVCC history, AbortSpan, and resolver retries are replaced, not simulated. Atomic failed EndTxn batches restore their local Put effects; the successful remote batches remain applied. Source EndTxn 347–350 also returns cleanup information for an already-aborted record.
11. A successful RecoverTxn only finalizes the record and updates its marker; it does not itself initiate intent resolution (cmd_recover_txn.go 226–229). EndTxn with lock spans performs local resolution and r1's remote resolution. A PushTxn changing Pending to Aborted does not invent known lock spans; the handle supplies resolution of all keys afterward (interface replacement).
12. Error values retain IntentMissing, serializable/async-write retry reasons, aborted-record/tombstone reasons, AlreadyCommitted and IndeterminateCommit. Storage/network/lease failures are absent under the interface. The missing-record ambiguous-result branch after a *successful* staging EndTxn is unreachable with no GC, and is an assertion rather than an arbitrary fate (dist_sender.go 1771–1798). Exact log strings, error indices, and tombstone-owner-specific abort diagnostics are omitted, since only outcome messages are visible.
13. Both historical fixes remain: QueryTxn checks a pre-commit missing intent (dist_sender.go 1746–1798, #37866); RecoverTxn accepts Committed even after prevention (cmd_recover_txn.go 156–173, #37784). A resolved timestamp-only value is never treated as positive proof that T's intent exists.
14. Tcx is started only after tc sends Committed, as the interface prescribes; source committer.go 254 queues its task before returning the response. Tcx always runs here (no stopper shutdown, failures, timeouts), and accepts AlreadyCommitted exactly as committer.go 565–571. Clients terminate; all service loops remain waiting forever. Thus Must runs are expected to end as blocked executions, not as all-threads-terminated executions.
15. No heartbeats, expiry checks, priorities, waiting, GC, coordinator rollback, 1PC, prepare, triggers, read spans, replication, recovery coalescing, semaphore or external resolver process are added. These are the explicit interface/README replacements, with original source sites PushTxn 224–229, manager.go 93–101 and 127–133, and EndTxn 248–605. No failure or timestamp choice is synthesized to make unreachable branches reachable.
16. The optional fifth positional fifo/bag argument selects the runner's default; an explicit `--model` flag takes precedence through the shared Program runner. Both entries are the same implementation, so `conf` would not be an independent correctness check.

## Compilation and requested runs

Compilation and run results are recorded below after execution. Commands are run from OUT_DIR (`.`).

Initial command:

```sh
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
```

This first attempt exited 101 because crates.io DNS was unavailable; its final cause was `Could not resolve host: index.crates.io`. The cached dependencies were then built with the additional `--offline` flag. The exact requested build command is retried after the lockfile is generated.

The first five run attempts all exited 101 at setup with `wrong message return type; expecting bench_crdb_sol::Setup but got bench_crdb_sol::Req`. This revealed that Must's generic receive validates the selected payload type rather than selecting by that type. The final implementation uses explicit tags for Setup, Batch, tasks and every response phase; `receive<T>` filters on the expected type's tag. Client messages remain verbatim and untagged, and their internal receivers filter for an absent tag. Recovery probes use `1000 + round`, disjoint from the phase tags 1–10. No catch-all receive consumes and discards mismatching payloads. The untagged client loops still receive only their specified Rep messages.

The otherwise unreachable initial-EndTxn serialization-error branch also retains the source's general split-and-refresh behavior: `refresh_failed_final_batch` reissues the prefix, then EndTxn alone (span_refresher.go 310–312). This differs from the reachable successful-but-pushed-write branch, which retries **only** EndTxn. This function is part of the span refresher mapping above.

Final compilation command:

```sh
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
```

Last build output line (exit 0):

```text
    Finished `release` profile [optimized] target(s) in 6.42s
```

Final-binary runs (each command has the requested 600-second timeout):

```sh
timeout 600 target/release/bench_crdb_sol count impl --model fifo -- 1 1 1 1
timeout 600 target/release/bench_crdb_sol count impl --model fifo -- 2 1 1 1
timeout 600 target/release/bench_crdb_sol count impl --model fifo -- 2 2 1 1
timeout 600 target/release/bench_crdb_sol count impl --model fifo -- 2 0 1 1
timeout 600 target/release/bench_crdb_sol count impl --model fifo -- 2 1 2 1
```

| Arguments | Result line | Exit | Stop reason | wall_ms | max_graph_events |
|---|---|---|---|---|---|
| `1 1 1 1` | `execs=0 blocked=43` | 0 | exhausted | 43 | 89 |
| `2 1 1 1` | `execs=0 blocked=2004` | 0 | exhausted | 1334 | 107 |
| `2 2 1 1` | `execs=0 blocked=1380` | 0 | exhausted | 875 | 114 |
| `2 0 1 1` | `execs=0 blocked=304` | 0 | exhausted | 190 | 94 |
| `2 1 2 1` | no final summary; at least 1,000,000 attempted | 124 | external timeout (600 s) | — | — |

All exhausted runs have a measurement block following the result line. Their literal final output lines are:

```text
spec_graphs_explored: 
sweeps: 
cache_tests: 
cache_hits: 
```

The zero normally completed executions are expected: required server threads are immortal. These are full count explorations of this implementation, with its source-derived assertions; they are not a conformance proof against an independent specification.


The final two-preventer command exited **124** at the requested 600-second timeout. Its literal last output line was:

```text
Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (1694.92/sec).
```

It did not exhaust and did not print a final `execs=` result or measurement block. No assertion or protocol panic was reported before the timeout; this partial run is not reported as exhaustive. An earlier two-preventer run before the final unreachable-path cleanup also timed out after reporting 1,000,000 blocked schedules (1686.34/sec). The four smaller cases were rerun against the final binary and exhausted with the table's counts.
