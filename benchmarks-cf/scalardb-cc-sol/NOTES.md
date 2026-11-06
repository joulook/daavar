# ScalarDB Consensus Commit port

Source: scalar-labs/scalardb at the supplied revision `416cdc2`; only the supplied code cut and its README were used, not its docs. `C/` below means `cc-src/core/src/main/java/com/scalar/db/transaction/consensuscommit/`. The implementation is `java/bench/scalardb_sol/ConsensusCommit.java`, class `bench.scalardb_sol.ConsensusCommit` (the deliverable's explicit package takes precedence over INTERFACE.md's generic `bench.scalardb.<Name>` example).

## Entry, roles, and messages

The public `String...` constructor accepts exactly `impl,<pattern>,<k>,<iso>,<fifo|bag>`. Supported patterns: lost, skew, both, mixed, mixedro. One key is accepted only for lost; otherwise two keys. SNAPSHOT and SERIALIZABLE are supported. `fifo` uses `CommunicationModel.P2P`; `bag` uses `ASYN`. The summary program name is `scalardb_cc`; size is `pattern,k,iso`. All processes have their required role names; only `c1`, `c2`, and (mixedro) `c3` are visible.

Main spawns `s1..sk`, `coord`, `x11..x1k`, `x21..x2k` (and `x31..x3k`), `h1`, `h2` (and `h3`), clients, and `init`, in that order, then awaits the processes. Main sends and receives nothing. `init` sends every invisible process one immutable Setup containing every address (including init), logical transaction IDs, isolation, and read-only flags. Each invisible process waits for Setup before handling anything else. Clients know their own handle from their spawn argument, as in the interface client loop. All roles except clients and init run forever. No daemons or shared mutable protocol state are used.

Visible messages, each a public record implementing MpMessage and annotated with its verbatim summary Tag:

| Direction | Messages |
| --- | --- |
| client to handle | `Begin()`, `Read(int key)`, `Write(int key, int value)`, `Commit()` |
| handle to client | `Begun()`, `Value(int key, int value)`, `Written(int key)`, `Committed()`, `Aborted()`, `Unknown()` |

Client loops implement the interface literally: begin; read each key in ascending order, ending on Aborted; write each designated key in order with the client's number; commit; accept one terminal reply. No retries or transaction restart.

Internal messages are immutable records:

| Message | Meaning |
| --- | --- |
| `Setup(addresses, ids, isolation, readOnly)` | Initialization, sent by init |
| `Get(request, replyTo, key)` | Physical storage Get |
| `Mutate(request, replyTo, key, mutation)` | One atomic conditional Put/Delete |
| `GetState(request, replyTo, id)` | Coordinator lookup |
| `PutState(request, replyTo, id, state, createdAt)` | Coordinator put-if-not-exists |
| `Reply(request, key, row, state, ok)` | Correlated storage/coordinator result; unused row/state fields are null |
| `RecoveryJob(key, handle, row, state)` | Apply the already-resolved outcome to the observed record |
| `RecoveryDone(key, outcome)` | Future completion; DONE, CRUD_CONFLICT, or CRUD_ERROR |

Each actor numbers requests locally; replies go only to that actor, so `(replyTo, request)` uniquely identifies a request. Recovery completions use the key because each fixed recovery process is used at most once. The RPC receive loop records early recovery completions and discards replies whose IDs are no longer in its phase. The idle handle also consumes late replies. Validation can therefore stop at its first conflict while requests already sent still execute.

## Method mapping

| Source method(s), source line numbers | Port |
| --- | --- |
| ConsensusCommitManager.begin 303–330 | Handle's Begin branch and per-instance workspace; setup supplies ID/isolation/read-only |
| ConsensusCommit.get 79–81; upsert 198–204 | Handle.get; Write branch and writeSet |
| ConsensusCommit.commit/commitInternal 227–270 | Handle.commit and Commit branch |
| ConsensusCommit.rollback/releaseResources 272–295 | Terminal workspace ownership: no active scanners, group reservations, or public rollback message exist; resource release has no protocol operation |
| TransactionContext.isSnapshotReadRequired/isValidationPossiblyRequired/isValidationRequired 69–100 | Snapshot caching in Handle.get; serializable flag; read-but-not-written key selection in validateRecords |
| Snapshot.putIntoReadSet/putIntoGetSet 129–137; putIntoWriteSet 157–226 | Handle.readSet, getSet, read flags, writeSet; later Put replaces the sole user column |
| Snapshot.getResult/mergeResult 307–354 | Handle.get's cached result and local-value overlay |
| Snapshot.to 371–382 | prepareRecords, commit mutation phase, rollbackRecords: mutation composers use original readSet images |
| Snapshot.toSerializable 640–696; validateGetResult 872–898; isChanged 1004–1017 | Handle.validateRecords: physical Gets, presence and tx_id comparison only |
| CrudHandler.get 97–124; readUnread 204–213; read 215–303 | Handle.get and readStorage, with per-key snapshot caching |
| CrudHandler.recoverAndFilter 396–436; executeRecovery 458–483 | Handle.resolve and submitRecovery; no predicates/index filters in this instance |
| CrudHandler.putIntoReadSetInSnapshot 598–604; put 682–703 | Conditional readSet population (omitted in read-only mode); Write branch |
| CrudHandler.waitForRecoveryCompletionIfNecessary/waitForRecoveryCompletion 787–836 | Handle.waitForRecoveryCompletion and Actor.completed |
| CrudHandler.getFromStorage 838–851 | Actor.readStorage/requestGet, Get/Reply |
| RecoveryExecutor.execute 123–180; resolveLatestResultAndRecover 205–338 | Handle.resolve, with the RETURN_LATEST_RESULT_AND_RECOVER strategy |
| RecoveryExecutor.checkReadRetryLimit 340–349 | Five passes in resolve; retry after pass five throws CrudConflict |
| RecoveryExecutor.getCoordinatorState 351–361 | Actor.getState |
| RecoveryExecutor.createRecoveredResult/createRecordFromBeforeImage/createResultFromAfterImage 363–374, 399–461 | Handle.submitRecovery: before image or committed after image with null before columns |
| RecoveryExecutor.throwUncommittedRecordExceptionIfTransactionNotExpired 376–397 | Fresh Nondet.chooseBoolean in resolve; false throws CrudConflict |
| RecoveryExecutor submissions 139, 223, 326; Result 578–600 | RecoveryJob/RecoveryDone, recovery set, completed map; line 139's alternative strategy is not selected |
| RecoveryHandler.tryRecover/rollRecordToState 73–81, 151–160 | Recovery.run dispatches the job's resolved outcome |
| RecoveryHandler.rollbackRecord 162–195; rollforwardRecord 207–241; mutate 399–427 | Recovery.run, rollback/rollforward composers, atomic Mutate; NoMutation absorbed |
| RecoveryHandler.tryAbortExpiredTransaction 268–280 | resolve's writeState(writer, ABORTED) |
| RecoveryHandler.abortIfExpired 295–378 | Recovery's absent-state branch retains best-effort expiry/re-read/force-abort/rollback; the selected RETURN_LATEST path performs expiry resolution synchronously in Handle.resolve |
| RecoveryHandler.isTransactionExpired 429–432 | Nondet.chooseBoolean, freshly called at each expiry check |
| CommitHandler.commit 155–231; failure handling 241–258; phase delegations 265–313 | Handle.commit, abort, abortState, prepareRecords, validateRecords, rollbackRecords |
| ParticipantCommitHandler.prepareRecords 76–102; validateRecords 104–118 | Handle.prepareRecords and validateRecords |
| ParticipantCommitHandler.commitRecords 120–140; rollbackRecords 142–173 | Final mutation phase in commit; rollbackRecords with parallel re-read followed by conditional restoration |
| ParticipantCommitHandler.tryRecoverRecordsBlockingWrites/getKeysOfWritesNeverRead 202–252 | Empty for these clients: every written key was read before Write |
| ParticipantCommitHandler.toReadTasks/toTasks 274–293 | getPhase and mutation phase send loops |
| CoordinatorCommitHandler.commitState/handleCommitConflict 50–167 | writeState(COMMITTED), then getState on conflict; COMMITTED succeeds, ABORTED/absent conflicts and rolls records back |
| CoordinatorCommitHandler.abortState 173–195; readCoordinatorStateAfterAbortConflict 241–252 | Handle.abortState: put ABORTED, read persisted outcome on conflict |
| CoordinatorStateAccessor.getState 163–169; putState/forceAbort 214–228; get/put 344–437 | Actor.state/getState/putState/writeState and coordinator process |
| PrepareMutationComposer.add(Put) 47–84; createBeforeColumns/isBeforeRequired 123–148 | prepare: new value/id/state/version/prepared_at, original after image copied into before columns, conditional tx_id match or absence |
| CommitMutationComposer.add/composePut 39–108 | rollforward: id-and-state condition, state COMMITTED, committed_at 0, before columns null; DELETED would conditionally delete |
| RollbackMutationComposer.add/getLatestRecord/composePut/composeDelete 62–165 | rollbackRecords and rollback: restore before image or delete, conditional on observed id and state |
| AbstractMutationComposer.setBeforeImageColumnsToNull 33ff | beforeFields(null) |
| ParallelExecutor phase policies 67–140; executeTasks 248–340 | Send all phase requests before awaiting; prepare/finalize/rollback collect all; validation stops on first change; singleton phase naturally sends one request and awaits it |
| AsyncExecutor.commitRecords/rollbackRecords 85–106 | Inline phases in handle before its terminal reply |
| MutationsGrouper.groupMutations 29–52 | One key per partition, one mutation per message |

## Decisions, representation, and limits

1. The ten replacements in cc-src/README.md and INTERFACE.md are applied. No storage/backend faults, lost messages, retries after transport failures, one-phase commit, group commit, coordinator cleanup, scans, indexes, deletes/inserts from clients, implicit pre-read, hooks, write-set encoding, or transaction retries are introduced. The fixed defaults choose parallel phases, inline commit/rollback and read-only coordinator omission (ConsensusCommitConfig 118–162, 177; CommitHandler 155–258). This is a finite workload over a protocol with permanent server loops.

2. Record representation is two immutable six-column Images: value, tx_id, tx_state, tx_version, tx_prepared_at, tx_committed_at. A null before Image means all six before columns are null. `Col` names all twelve physical columns. The storage process evaluates ALWAYS, ABSENT, or the conjunction of equality/IS-NULL tests, then applies only the explicit Field assignments, atomically. Delete removes the whole row. An unsuccessful condition returns ok=false and changes nothing. This directly implements replacement 1 and the missing API data carriers described in the README. Null-equality in a test is IS NULL. Images snapshot values rather than aliasing storage.

3. Preparation deliberately omits the after-image committed_at assignment, preserving the current physical field, as PrepareMutationComposer 47–84 does. It copies all six original after columns into before columns, increments the original version, and checks only original tx_id, not version or state (61–81). Finalization changes only state, committed_at, and the before columns (CommitMutationComposer 79–108). Rollback restores all six original columns and clears all before columns (RollbackMutationComposer 106–165). Conditions include tx_id AND tx_state for finalization and rollback. Null transaction IDs and absent rows are representable in the generic store; imported/deemed-committed data are not generated by this workload (TransactionResult 200–232).

4. Initial t0 records are installed locally on receipt of Setup. They are COMMITTED, value 0, version 1, both timestamps 0, with no before image, exactly as the interface requires. Transaction IDs come from Setup. All later timestamps are 0. Coordinator created_at is retained locally; Reply omits it because its only possible value is 0, also used by all rollforwards (CoordinatorCommitHandler 50–67; CoordinatorStateAccessor 219–228). No coordinator row is deleted. States and created_at are installed together in the sequential process.

5. Source map/collection iteration order and executor scheduling are not prescribed for independent tasks (Snapshot 371–382, 670–693; ParallelExecutor 268–316). The port sends writes in client insertion order and validation Gets in key order. Replies are collected in any order. Rollback composes restored records in the order their re-read replies were received. All requests of a phase are posted before collecting, per the required executor replacement. No nondeterministic send-order permutation is added. Preparation waits for every reply even after NoMutation so that rollback cannot race a still-unsent/unfinished prepare (ParallelExecutor 67–89). Rollback re-reads all written keys before posting restoration mutations (ParticipantCommitHandler 148–167).

6. The workspace separates immutable original readSet/getSet results from pending user writes. Writes only change the local writeSet, with the latest value winning for the sole user column (Snapshot 157–226, 324–354). The client never reads after Write, but get still overlays a pending value. Read-only handles cache getSet results and omit readSet entries (CrudHandler 598–604). SERIALIZABLE validates read-only transactions too, checking precisely presence and tx_id, including prepared physical rows; it does not resolve those validation reads or compare versions, values or states (TransactionContext 77–100; Snapshot 872–898, 1004–1017).

7. The expiry decision is a fresh checker Nondet.chooseBoolean per source call, not a clock or a cached property. False aborts the reader immediately; true permits aborting the writer. The full latest-result resolver is retained, including the physical read before force-abort AND the physical read after a successful abort, writer-change retries, and exactly five resolution passes (RecoveryExecutor 205–349). Successful force-abort does not alone justify returning an old before image: the post-abort read can find a committed record or another prepared writer. These reads remain even though coordinator cleanup is disabled. The boolean option order is the API's true, false; neither choice is preferred semantically.

8. A resolved prepared read returns a synthesized committed after image or restored before image immediately and sends a job to its fixed recovery task, without awaiting it. The job carries the observed record and resolved state and applies a conditional rollforward/rollback, absorbing NoMutation (RecoveryExecutor 205–338, 363–461; RecoveryHandler 162–241). Early completions are retained. Commit waits for jobs of written keys under SNAPSHOT and every read key under SERIALIZABLE (CrudHandler 787–836). Recovery outcome enum includes the source future's failure categories, but only DONE is reachable under the required store, which raises no execution/retriable errors. No simulated exception is added. The null-state best-effort branch of Recovery.run corresponds to RecoveryHandler.abortIfExpired(false); the selected strategy submits only decided states, so that branch is not exercised (RecoveryExecutor 123–180).

9. Every workload key always has a row: it starts with one; every update has a before image; rollback restores it; no client deletes. Thus Value can always carry the integer required by the interface. Generic absent-row storage and rollback-delete operations remain implemented, but returning absence to a client is guarded as an invariant instead of inventing a visible None message (INTERFACE.md fixes Value; PrepareMutationComposer 61–81; RollbackMutationComposer 62–89). No externally visible behavior for other workloads is claimed.

10. Non-failing coordinator writes have exactly one attempt (CoordinatorStateAccessor 400–437). On COMMITTED put conflict, the port reads persisted state and follows it; the ABORTED branch rolls records back and replies Aborted (CoordinatorCommitHandler 76–167; CommitHandler 208–231). On self-abort conflict it also reads persisted state (173–195); the returned state is unused by the source orchestrator, which still rolls back. COMMITTED cannot have been installed on that self-abort path in these one-shot handles. Unknown is an accepted, tagged boundary reply but is unreachable: its source paths require coordinator failures, multiple write attempts, or cleanup (CoordinatorCommitHandler 50–167, 173–195). They are excluded by the interface, so no nondeterministic Unknown is added.

11. `ConsensusCommit.rollback` releases resources, not storage records (272–295). There are no scanners or group reservations here, so it has no message effect; ending a handle's one transaction prevents further requests while the process remains alive. Storage rollback on commit failure is implemented separately by rollbackRecords. A read conflict ends the client without inventing a Rollback request or coordinator write. The source's extra recovery for keys written without having been read is vacuous: every designated key is read successfully before any Write (ParticipantCommitHandler 202–252; fixed client loop). Omitted exception classes are reduced only where the store guarantees they are unreachable, per README limitation 1; an actual read-resolution conflict uses the local checked CrudConflict exception. JMC control-flow exceptions are never caught.

12. The implementation uses ordinary named, awaitable processes, not daemons. Servers/handles/recovery tasks block on receive forever after their work, as required. The checker therefore reports terminal graphs as blocked and main blocks awaiting s1. This is intentional; it is not a protocol deadlock among unfinished clients. No monitor messages are added because the requested program has no monitor role. All interaction uses Channel, all expiry choices use Nondet, and clients are the only visible roles. The API's MpProcess and Channel wrappers provide the runtime events; no instrumentation or direct runtime calls are used.

## Compilation and required runs

Working directory: OUT_DIR (`.`). Compilation command, successful (exit 0, no output):

```sh
javac -d java/out -cp "$(cat ./cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

The five required commands (each includes the 900-second timeout, seed 1, unbounded must enumeration, and no export):

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,lost,1,snapshot,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,lost,2,snapshot,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,skew,2,snapshot,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,skew,2,serializable,fifo' must 0 1 report - 900 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,mixed,2,snapshot,fifo' must 0 1 report - 900 </dev/null
```

Run results and last output lines are recorded below. Counting mode (`-`, no specification) checks executions and process assertions; it is not a refinement/conformance check against an independent specification. No such specification was supplied.

### Finished runs

All four completed required configurations below exited 0 and exhausted the must stack. All graphs are blocked because of the required permanent process loops. `max_visible_events: 0` is the launcher's counting mode statistic (no summary export), not a declaration that clients are invisible.

| Configuration | Iterations / distinct blocked graphs | Distinct complete graphs | Wall ms | Stop reason |
| --- | ---: | ---: | ---: | --- |
| lost,1,snapshot,fifo (final-build repeat) | 166 | 0 | 20400 | exhausted |
| lost,2,snapshot,fifo | 278 | 0 | 18205 | exhausted |
| skew,2,snapshot,fifo | 276 | 0 | 25535 | exhausted |
| skew,2,serializable,fifo | 381 | 0 | 37571 | exhausted |

The initial lost/1 run also exited 0 and exhausted at 166 graphs (`wall_ms: 9774`). It was repeated after aligning the read-only workspace population; the four two-client patterns do not exercise read-only mode. The other required runs used the same protocol behavior. Independent JVM invocations overlapped, so wall times include resource contention; these are validation runs, not controlled performance measurements.

Last output lines of the final-build lost/1 run:

```text
observed:   ConsensusCommit(lost,1,snapshot,fifo)
graphs:     0 distinct complete, 166 distinct blocked; stack exhausted
--- measurements
wall_ms: 20400
explore_ms: 17935
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 166
summaries_distinct: 0
iterations: 166
stop_reason: exhausted
peak_rss_kb: 331700
max_message_events: 108
max_porf_pairs: 0
max_graph_events: 116
mean_graph_events: 105.458
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

Last output lines of lost/2:

```text
observed:   ConsensusCommit(lost,2,snapshot,fifo)
graphs:     0 distinct complete, 278 distinct blocked; stack exhausted
--- measurements
wall_ms: 18205
explore_ms: 17144
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 278
summaries_distinct: 0
iterations: 278
stop_reason: exhausted
peak_rss_kb: 437208
max_message_events: 130
max_porf_pairs: 0
max_graph_events: 141
mean_graph_events: 130.201
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

Last output lines of skew/snapshot:

```text
graphs:     0 distinct complete, 276 distinct blocked; stack exhausted
--- measurements
wall_ms: 25535
explore_ms: 23962
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 276
summaries_distinct: 0
iterations: 276
stop_reason: exhausted
peak_rss_kb: 452280
max_message_events: 130
max_porf_pairs: 0
max_graph_events: 141
mean_graph_events: 133.714
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

Last output lines of skew/serializable:

```text
observed:   ConsensusCommit(skew,2,serializable,fifo)
graphs:     0 distinct complete, 381 distinct blocked; stack exhausted
--- measurements
wall_ms: 37571
explore_ms: 36051
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 381
summaries_distinct: 0
iterations: 381
stop_reason: exhausted
peak_rss_kb: 472540
max_message_events: 138
max_porf_pairs: 0
max_graph_events: 149
mean_graph_events: 139.236
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```

The runtime printed `ERROR Console contains an invalid element or attribute "level"` at startup, and some overlapping JVMs printed a locked hsperfdata warning. These diagnostics did not prevent checking or change the successful exit statuses.

### Additional third-client / bag smoke check

To exercise the read-only handle and non-FIFO delivery, also ran:

```sh
java -Xss4m -Xmx8g -cp java/out:$(cat ./cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.scalardb_sol.ConsensusCommit:impl,mixedro,2,serializable,bag' mp-random 100 1 report - 60 </dev/null
```

Exit 0; 100 requested iterations completed without protocol assertion failure. This was sampling, not exhaustive enumeration. Last output lines (scheduler log lines omitted):

```text
ran:        100 iterations
blocked:    100 (counted by the strategy)
completed:  0
rate:       8.6 iterations/s
elapsed:    11.678 s
observed:   ConsensusCommit(mixedro,2,serializable,bag)
```

### Required mixed/snapshot run

The fifth required command ran to its specified 900-second timeout. The launcher exited 0, but the exploration was **not exhaustive**: 12,078 iterations / distinct blocked graphs, zero distinct complete graphs, stack not exhausted. No protocol assertion failure was reported. This is a partial counting result, not a successful exhaustive verification. The timeout error and last output lines were:

```text
ran:        12078 iterations
blocked:    12078 (counted by the strategy)
completed:  0
rate:       13.4 iterations/s
error:      Halting execution: 12078 due to timeout
elapsed:    900.120 s
observed:   ConsensusCommit(mixed,2,snapshot,fifo)
graphs:     0 distinct complete, 12078 distinct blocked; stack not exhausted
--- measurements
wall_ms: 901682
explore_ms: 900119
summarise_ms: 0
index_ms: 0
lookup_ms: 0
export_ms: 0
graphs_complete: 0
graphs_blocked: 12078
summaries_distinct: 0
iterations: 12078
stop_reason: timeout
peak_rss_kb: 855096
max_message_events: 158
max_porf_pairs: 0
max_graph_events: 169
mean_graph_events: 162.034
max_visible_events: 0
mean_visible_events: 0.000
max_summary_bytes: 0
max_porf_count: 0
```
