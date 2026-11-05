# Parallel commits port

Source: CockroachDB d30c905fff79ef825adc96bcc647f1872a90f2ff, the cut in `cr-src/README.md`.
Implementation: `java/bench/cockroach_sol/ParallelCommits.java`, class
`bench.cockroach_sol.ParallelCommits`. The explicitly requested package overrides
INTERFACE.md's generic `bench.cockroach.<Name>` example. Only implementation mode
is provided. Parameters are `impl,n,p,m,a,fifo|bag`, validated against the interface.

## Roles and messages

Spawn order: r1, optional r2, tcx, tc, h1..hm, c, p1..pm, init.
Only c and p1..pm are visible. The clients execute the prescribed loops and stop;
tc, tcx, each handle, and each range remain in receive loops. Main only spawns and
awaits; its first await can remain blocked forever while the checker explores all
other runnable processes. These permanent services deliberately make terminal
executions **blocked**, rather than complete, in JMC's terminology.

init sends each invisible service one immutable Setup containing every address (including init itself) and
T's initial metadata (T, k1, epoch 0, write/read/minimum timestamp 1). Client
addresses are supplied by their spawn closures; clients have no setup messages.
A service selectively receives Setup before accepting work, including in bag mode.
No protocol state is shared across processes; immutable configuration is shared.

Visible messages, with matching @Tag annotations:

| Direction | Messages |
| --- | --- |
| c → tc | Put(int key), Commit(List<Integer> keys) |
| tc → c | Written(int key), Committed(), Retry(), Aborted() |
| pi → hi | Recover() |
| hi → pi | Recovered(boolean committed) |

Internal messages are Setup, Explicit(meta, locks), Batch, Answer. Batch carries
its reply address and correlation token; its kind selects PIPE (async-consensus
Put), FINAL (final Put and, on r1, EndTxn), PRECOMMIT (QueryIntent with
ErrorIfMissing), QUERY (QueryTxn), PROBE (QueryTxn on r1 plus the range's
QueryIntents without ErrorIfMissing), PUSH (PUSH_ABORT), RECOVER
(RecoverTxn with ImplicitlyCommitted), EXPLICIT (EndTxn alone), or RESOLVE.
Each Answer carries the correlation token, range, timestamp, staging timestamp,
error, record/synthetic transaction, record-existence flag and intent-query flags.
Token zero identifies fire-and-forget resolution from r1: no reply is sent.
Messages and nested collections are immutable value records. fifo maps to P2P;
bag maps to ASYN. No nondeterministic fault or invented timestamp push is added.

## Function mapping

Paths below omit the common `pkg/kv/` prefix under cr-src. Line numbers refer to unchanged Go sources.

| Source function and lines | Java location / behavior |
| --- | --- |
| kvclient/kvcoord/txn_interceptor_committer.go SendLocked 138–261 | coordinator: dispatch status, implicit decision, async task |
| same, canCommitInParallel 349–396 | coordinator: enabled for all admitted Put/QueryIntent batches; excluded request types cannot be constructed |
| same, isTxnCommitImplicit 413–441 | coordinator: merged response timestamp <= staging timestamp |
| same, retryTxnCommitAfterFailedParallelCommit 443–486 | coordinator: EndTxn-only EXPLICIT with all writes moved to locks |
| same, makeTxnCommitExplicitAsync 492–527, Locked 529–594 | Explicit message after client acknowledgement; explicitTask tolerates ALREADY_COMMITTED |
| kvclient/kvcoord/txn_interceptor_pipeliner.go SendLocked 311–361, attachLocksToEndTxn 437–501 | coordinator: PIPE requests, attach pipelined and final writes |
| same, chainToInFlightWrites 608–718 | coordinator: PRECOMMIT queries for every tracked pipeline write |
| same, updateLockTrackingInner 796–899 | coordinator: insert answered async writes; clear on success; retain all lock keys for retry/error paths |
| same, adjustError 929–954 | coordinator: uncorrected IntentMissing → RETRY_ASYNC_WRITE_FAILURE → Retry |
| kvclient/kvcoord/dist_sender.go divideAndSendParallelCommit 1567–1723 | coordinator: send all per-range query and final sub-batches, collect in either order; EndTxn error wins |
| same, detectIntentMissingDueToIntentResolution 1746–1798 | coordinator: QueryTxn after missing intent; accept COMMITTED; ABORTED → Aborted |
| kvclient/kvcoord/txn_interceptor_span_refresher.go maybeRefreshAndRetrySend 249–324 | Meta.refresh and coordinator EndTxn-only retry loop |
| kvserver/batcheval/cmd_end_transaction.go EndTxn 248–605 | Store.evaluate FINAL/EXPLICIT, stripping, retry checks, stage/finalize, resolution |
| kvserver/batcheval/cmd_query_intent.go QueryIntent 60–167 | Store.evaluate queries: epoch/sequence match, unpushed test, missing error |
| kvserver/batcheval/cmd_query_txn.go QueryTxn 48–89 | Store.evaluate QUERY/PROBE: snapshot or synth |
| kvserver/batcheval/cmd_recover_txn.go RecoverTxn 52–234 | Store.evaluate RECOVER: strict implicit validation; accept changed/finalized records; finalize STAGING |
| kvserver/batcheval/cmd_push_txn.go PushTxn 111–350 | Store.evaluate PUSH: missing/PENDING abort, STAGING indeterminate, finalized no-op; higher-meta branch retained |
| kvserver/batcheval/transaction.go CanCreateTxnRecord 119–134, BumpToMinTxnCommitTS 139–147, SynthesizeTxnFromMeta 167–203 | Store.synth, tombstone creation guard, pushMarker forwarding |
| kvserver/replica_tscache.go updateTimestampCache 90–369 (selected cases) | Store.evaluate: missing/pushed query key protection; EndTxn/RecoverTxn/PushTxn markers |
| same, applyTimestampCache 397–464 | Store.evaluate before Puts: max(write, cache + 1) |
| same, CanCreateTxnRecord 598–656 | Store.evaluate: tombstone < minimum timestamp |
| kvserver/replica_batch_updates.go maybeStripInFlightWrites 37–170 | Store.evaluate FINAL: same-batch local Put removed from flight, added to locks |
| kvserver/replica_send.go handleIndeterminateCommitError 818–838, retry call 593–600 | handle: recovery followed by retry of original PUSH |
| kvserver/txnrecovery/manager.go ResolveIndeterminateCommit 82–110, resolve 120–151 | handle: independent recovery per preventer |
| same, probe 158–267, recover 279–299, batchTimestamp 306–310 | handle: concurrent per-range PROBE batches; changed-record test precedes prevented-intent result; RECOVER(!prevented) |

## Decisions and replacements

1. Storage and timestamp caches use the interface replacements. Each Store has one
   key's optional intent (Meta plus sequence), optional committed value (zero means
   none), monotonic key cache, and on r1 a separate optional record and tombstone
   and push markers. The constant positive initial time makes zero unambiguous.
   There is no transaction identity on the value. Record creation occurs only in
   successful EndTxn. Push of a missing record installs only a tombstone, never a
   record (cmd_push_txn.go 151–180, 307–322). The push marker is represented but
   stays zero because PUSH_TIMESTAMP is excluded.

2. A range executes Store.evaluate without message operations. Replies and external
   resolution sends follow the atomic application. On failed EndTxn, a local Put's
   storage mutations are discarded; its pushed timestamp is preserved in the
   error. QueryIntent's error-side cache update remains (replica_tscache.go
   123–133, 307–338). Resolution work from an aborted EndTxn is emitted as separate
   messages, including to self, because EndTxn cannot write storage on error
   (cmd_end_transaction.go 359–372). EndTxn success and local resolution are atomic.

3. DistSender's Go goroutine scheduling is open (dist_sender.go 1650–1660).
   Following the interface, tc sends every query sub-batch first and then each
   final sub-batch in increasing range order, receiving all replies without
   imposing range order. Recovery likewise sends r1's probe first, then r2's.
   Delivery and other actors can interleave between any sends. This is a concrete
   dispatch order, not an extra actor or failure. Queries on r1 in one recovery
   share QueryTxn's atomic range batch (manager.go 222–232). With n=1,p=1 the
   pre-commit query remains split from EndTxn as in dist_sender.go 1374–1375,
   1567–1614; therefore that instance can stage and use tcx.

4. EndTxn's attached flight initially contains every key, but the local final Put
   is stripped before storing the record (replica_batch_updates.go 37–170).
   The implementation uses exact Write equality; sequences equal key numbers,
   ordered 1..n and reset per epoch, since each key is written once per epoch.
   Locality plus one write per key eliminates the general stripping search and
   overlapping-span cases. Lock sets are key lists; union order is irrelevant
   because every key has its own range. No ranged spans are represented.

5. Isolation is SERIALIZABLE (cmd_end_transaction.go 620–623 leaves an isolation
   choice; the interface only specifies blind writes). Read and write timestamps
   start at 1. A successful staging response whose remote Put was bumped causes
   EndTxn alone to be retried; refresh forwards read to write. The source first
   attempts EndTxn and gets RETRY_SERIALIZABLE, then refreshes it; the model folds
   that first inevitable failing EndTxn into the refresh, as the interface says
   “refreshing first”. It has no storage effects and no reads can fail refresh.
   EndTxn's own retry error similarly refreshes before the retry. A five-retry
   budget follows maxTxnRefreshAttempts (span_refresher.go 30); exhaustion emits
   Retry. No loop here needs that bound under the admitted actors.

6. The code retains Retry, epoch advance, async-write-failure adjustment, serializable
   retry, PENDING pushes, and known-higher-meta pushes. No triggers are added for
   the unreachable branches (README reachability notes). A retry reuses T's id,
   anchor and minimum timestamp, increments the epoch, and retains its lock
   footprint. All spans can be represented by the full key list after an error:
   by Commit time each key has been attempted (pipeliner.go 796–824).
   In this model a successful commit clears flight, and all attached flight keys
   are folded into the lock footprint for the explicit request. Per-query
   intermediate removals are coalesced at response collection: tc receives no
   next client operation while collecting this commit, so this does not change
   messages or subsequent lock tracking (pipeliner.go 825–899).

7. Source resolution's storage behavior is replaced by the specified cell operation,
   not by an MVCC engine. Epoch/timestamp guards prevent stale resolution from
   deleting a newer epoch or committing a write above the final record timestamp;
   these guards never affect a successful initial-epoch commit. Committing moves
   the intent's own timestamp into value, exactly as INTERFACE.md prescribes.
   A Put after an abort is allowed to leave an intent: no AbortSpan and no
   additional periodic cleanup are invented. A later EndTxn reports the abort.

8. r1's folded asynchronous resolver sends external LockSpans after EndTxn finalizes
   a record. RecoverTxn itself launches no resolution (cmd_recover_txn.go 226–229),
   matching the cut's explicit replacement rule; each handle retries its push and
   then resolves all keys, awaiting their responses before Recovered. The source
   does not define a visible preventer response; that ordering is this port's
   choice for the interface's “then resolution … replying Recovered”. Independent
   handles can both recover; there is no coalescing or waiting on another handle.

9. Both historical fixes are included. The coordinator's missing pre-commit intent
   is checked against QueryTxn (dist_sender.go 1746–1798), and RECOVER(false)
   accepts a COMMITTED record (cmd_recover_txn.go 156–173). A resolved value is
   never treated as proof of a matching intent. An absent ABORTED record after a
   successful staging EndTxn is unreachable without GC: the source's ambiguous
   GC-result branch is therefore mapped to Aborted, rather than adding an external
   Ambiguous message that the interface does not contain.

10. Network correlation tokens are local monotonically increasing integers; no
    cancellation/timeouts, duplicate delivery, failed replication, epochs chosen
    by nondeterminism, or speculative losses are added. Internal unused fields use
    empty immutable lists and a PENDING fate instead of null so values remain
    summary-encodable. The checker explores interleavings through Channel only;
    no monitor processes or shared-state assertions alter the role set.

11. Source areas explicitly replaced by README/INTERFACE are omitted: replication,
    Raft, latches, lock table, real clock, expiry/priority, heartbeats, GC, rollback,
    restart with a fresh transaction id, 1PC, Prepare, triggers, ignored sequences,
    admission control, tracing, budgets, context cancellation and stopper behavior.
    Async-consensus Put is stored before its reply; the coordinator still tracks
    it as in-flight until proven. tcx is permanent and receives Explicit only after
    tc sends Committed, as the interface requires (committer.go 492–527 leaves the
    goroutine's actual scheduling open).

## Compilation and requested runs

Commands are executed from OUT_DIR (`.`). cp.txt is consumed only by the commands
explicitly prescribed in the task, not inspected for implementation context.

```sh
javac -d java/out -cp "$(cat ./cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Compilation succeeded, exit 0, with no output.

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach_sol.ParallelCommits:impl,1,1,1,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach_sol.ParallelCommits:impl,2,1,1,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach_sol.ParallelCommits:impl,2,2,1,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach_sol.ParallelCommits:impl,2,0,1,1,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cockroach_sol.ParallelCommits:impl,2,1,2,1,fifo' must 0 1 report - 900 </dev/null
```

Final-version run results and final output lines follow. All five launcher processes exited 0; a timeout is still a partial exploration.

| Instance n,p,m,a | Exit | Iterations / blocked graphs | Stop |
| --- | --- | --- | --- |
| 1,1,1,1 | 0 | 15 | exhausted |
| 2,1,1,1 | 0 | 456 | exhausted |
| 2,2,1,1 | 0 | 390 | exhausted |
| 2,0,1,1 | 0 | 248 | exhausted |
| 2,1,2,1 | 0 | 15925 | timeout (900 seconds; stack not exhausted) |

The first four runs' measurement tails include, respectively:

```text
# impl,1,1,1,1,fifo
graphs_complete: 0
graphs_blocked: 15
summaries_distinct: 0
iterations: 15
stop_reason: exhausted
peak_rss_kb: 122964
max_message_events: 58
max_porf_pairs: 0
max_graph_events: 63
mean_graph_events: 51.133
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0

# impl,2,1,1,1,fifo
graphs_complete: 0
graphs_blocked: 456
summaries_distinct: 0
iterations: 456
stop_reason: exhausted
peak_rss_kb: 386320
max_message_events: 74
max_porf_pairs: 0
max_graph_events: 80
mean_graph_events: 73.000
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0

# impl,2,2,1,1,fifo
graphs_complete: 0
graphs_blocked: 390
summaries_distinct: 0
iterations: 390
stop_reason: exhausted
peak_rss_kb: 391340
max_message_events: 84
max_porf_pairs: 0
max_graph_events: 90
mean_graph_events: 77.477
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0

# impl,2,0,1,1,fifo
graphs_complete: 0
graphs_blocked: 248
summaries_distinct: 0
iterations: 248
stop_reason: exhausted
peak_rss_kb: 312396
max_message_events: 62
max_porf_pairs: 0
max_graph_events: 68
mean_graph_events: 64.839
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

The launcher emits a Log4j Console configuration diagnostic at startup; the runs
continue. Concurrent launches can also emit a JVM perf-data lock warning. The
commands use the required counting mode (`-` for export), so summary and visible
event measurement counters are zero; visible roles are nevertheless declared by
the program for exports. No specification comparison was requested or performed.
The prescribed `report` destination is shared by these invocations; the results
above are from each invocation's own standard output. A preliminary run's output
was captured in `run-1-1-1.log`; the final runs were collected directly from the
tool. After adding init's own address to Setup, compilation and all five commands
were repeated; the measurement tails above are from the final version.

Further representation details: abort reasons (record found aborted, creation
rejected by a tombstone, etc.) share TRANSACTION_ABORTED because no retained
branch distinguishes those reasons; retry reasons remain distinct, and
ALREADY_COMMITTED remains distinct for tcx (committer.go 565–582). Transaction id
and anchor are fixed as T and k1, so intent matching compares epoch and sequence
without a redundant id comparison (cmd_query_intent.go 109–111). The per-range
cache map is represented by one integer because each range owns exactly one data
key; record markers occupy separate fields, not that data-key entry
(replica_tscache.go 163–266, 307–338). Empty/probe-only range batches have no Put
and therefore do not apply the data-key write timestamp bump.


Final two-preventer run, final output lines:

```text
ran:        15925 iterations
blocked:    15925 (counted by the strategy)
completed:  0
rate:       17.7 iterations/s
error:      Halting execution: 15925 due to timeout
elapsed:    900.457 s
observed:   ParallelCommits(2,1,2,1,fifo)
graphs:     0 distinct complete, 15925 distinct blocked; stack not exhausted
--- measurements
wall_ms: 902671
explore_ms: 900456
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 15925
summaries_distinct: 0
iterations: 15925
stop_reason: timeout
peak_rss_kb: 724304
max_message_events: 108
max_porf_pairs: 0
max_graph_events: 115
mean_graph_events: 105.409
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

No assertion or protocol error was reported in these runs. Four exploration
stacks exhausted; the two-preventer result is partial because of the requested
900-second bound. No conformance or exhaustive-safety claim is inferred from the
counting-mode runs. The preliminary two-preventer run (before including init's
own address in Setup) also timed out at 900 seconds, at 16660 blocked graphs;
its result is superseded by the final-version run above.
