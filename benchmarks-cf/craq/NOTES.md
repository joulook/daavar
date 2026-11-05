# 3FS chain replication (CRAQ), one chunk, one chain of three targets: Java transliteration (JMC message passing)

Source: DeepSeek 3FS main 22fca04, the cut in `plan/case-studies/3fs/craq-src` (paths below are relative to it),
full tree in `plan/case-studies/3fs/code`. Program: `java/bench/craq/ChainReplication.java`, class
`bench.craq.ChainReplication implements MpProgram`, argument string `impl,<q>,<fifo|bag>[,consecutive]`
(`fifo` = `CommunicationModel.P2P`, `bag` = `ASYN`; every send and receive uses that model). Every line that
differs from the source carries `// PORT:` and the reason.

## Observation contract (owner's ruling for this evaluation)

The full contract, including the treatment of shared memory as a sequential process, is
`daavar/benchmarks-cf/craq/CONTRACT.md`.

The concurrency inside a storage node is modelled as **logical tasks**: one process per concurrent activity of the
source (an update in flight that waits for its successor, a read, the chunk lock as explicit synchronisation), the
source's dependencies as messages, and no other program order between tasks. The reuse of pool workers
(coroutines multiplexed on folly thread pools, the AIO read worker's threads) is **not** modelled: two requests
that the source may happen to run on one pool thread are two independent processes here.

## Processes

Spawned by `main` in this order; `main` sends and receives nothing and awaits `c0`, `c1` only.

| process | 3FS counterpart | lifetime |
|---|---|---|
| `nk` (k = 0,1,2) | `StorageTarget` tk with its chunk store metadata (`ChunkStore::get`/`set`) and its `UpdateWorker` (runs `updateChunk` = `ChunkReplica::update`/`commit` jobs); serves the read path's metadata lookups | daemon |
| `nk.lock` | `StorageTarget::chunkLocks_` (`lockChunk`, StorageTarget.h l.61) for the one chunk | daemon |
| `nk.upd.ci` (i = 0,1) | the update coroutine of client i's channel at tk: `StorageOperator::write`/`update` → `ReliableUpdate::update` → `handleUpdate` → `ReliableForwarding` | daemon |
| `nk.read.ci` | the read coroutine of client i's reads at tk: `StorageOperator::batchRead` + the AIO job (`aioPrepareRead`, the read, `aioFinishRead`) | daemon |
| `c0.retry`, `c1.retry` | the retry part of `sendOpsWithRetry` for client i's reads (re-selection on `kChunkNotCommit`) | daemon |
| `c0`, `c1` (visible) | `StorageClientImpl` for one user: the client loop of INTERFACE.md | terminates |

Order of spawn: for k = 0..2: `nk`, `nk.lock`, `nk.upd.c0`, `nk.upd.c1`, `nk.read.c0`, `nk.read.c1`; then
`c0.retry`, `c1.retry`, `c0`, `c1` (nodes in index order, then clients in index order, as INTERFACE.md fixes).
`rolesInSpawnOrder()` = `visibleRoles()` = `[c0, c1]`; every other process is an unnamed daemon.

Why one update task per (target, client channel) and one read task per (target, client): a client has one
request outstanding at a time, so its requests at a target are already causally ordered (each next request is
porf-after the reply to the previous one); one task per such sequence therefore adds no program order that the
source does not have. For updates this is also the source's own serialisation: `ReliableUpdate::update` takes the
client's channel lock (`tryLockChannel`, ReliableUpdate.cc l.53-60, the lock l.56), so updates of one channel never run
concurrently at a target. Requests of different clients go to different tasks and are unordered except by the
chunk lock (updates) and the chunk metadata held by `nk` (decision 2).

## Messages

Visible (INTERFACE.md, `@Tag` = the record name): `Write(int client, long value)` ci → `n0.upd.ci`;
`Ack(int client)` `n0.upd.ci` → ci; `Read(int client)` ci → `nk.read.ci` (k chosen by `Nondet`);
`Value(int client, long value)` `nk.read.ci` → ci.

Invisible:

| message | from → to | 3FS |
|---|---|---|
| `UpdateReq(UpdateIO payload, UpdateOptions options, MessageTag tag, int retryCount)` | `nk.upd.ci` → `n(k+1).upd.ci` | the forwarded `UpdateReq` (`doForward`, fromClient = false, updateVer set by the head) |
| `UpdateRsp(MessageTag tag, IOResult result)` | `n(k+1).upd.ci` → `nk.upd.ci` | the successor's `UpdateRsp` (result with commitVer, checksum, commitChainVer) |
| `LockChunk(channel)`, `LockGranted()`, `UnlockChunk(channel)` | `nk.upd.ci` ↔ `nk.lock` | `lockChunk` / `lockGuard.lock()` / the guard's release |
| `UpdateJob(channel, UpdateIO, UpdateOptions)`, `CommitJob(channel, commitVer, commitChainVer)`, `JobDone(IOResult)` | `nk.upd.ci` ↔ `nk` | `updateWorker_.enqueue(&job); co_await job.complete()` in `doUpdate`/`doCommit` |
| `MetaGet(reader)`, `MetaRsp(ChunkMetadata)` | `nk.read.ci` ↔ `nk` | `store.get` of the chunk metadata in `aioPrepareRead` and again in `aioFinishRead` |
| `ReadRetry(client, retryCount)` | `ci.retry` → `nk.read.ci` | the retried `BatchReadReq` of the same ReadIO |
| `ReadRsp(client, retryCount, code, target, updateVer, commitVer)` | `nk.read.ci` → `ci.retry` | a `BatchReadRsp` whose IOResult is `kChunkNotCommit` (with the versions read) |

## The protocol, role by role

- **Client ci** (`StorageClientImpl`). Operation j, kind (i+j) mod 2. Write: `batchWriteWithRetry` →
  `batchWriteWithoutRetry` (Default → `HeadTarget`, l.1805) → `selectRoutingTargetForOps` → t0 →
  `sendWriteRequest`: `Write(i, v)` to `n0.upd.ci`, wait for `Ack`. Read: `batchReadWithRetry` →
  `batchReadWithoutRetry` (Default → `LoadBalance`, l.1645) → `selectRoutingTargetForOps` → `Nondet.chooseIndex(3)`
  → `Read(i)` to `nk.read.ci`, then wait for `Value`.
- **Retry loop ci.retry**: on `ReadRsp` with `kChunkNotCommit` (`isFastRetryError`, l.166) select again
  (`Nondet`) and send `ReadRetry` at once; the final `Value` goes from the answering read task to ci directly.
  The unproductive-retry cut (below) is applied here.
- **Head update task n0.upd.ci**: `Write` → `StorageOperator::write` (UpdateReq with fromClient, updateVer 0) →
  `ReliableUpdate::update` (channel cache) → `handleUpdate`: head check; lock the chunk (`LockChunk`, wait
  `LockGranted`); `doUpdate` (`UpdateJob` to n0; `ChunkReplica::update` with updateVer 0 advances the update
  version by one); version rules (l.400-440); `forwardWithRetry` → `forward` → `doForward`: `UpdateReq` with the
  allocated updateVer to `n1.upd.ci`, wait for `UpdateRsp`, `commitIO.commitVer` := its commitVer; commit-version
  check; checksum check (content = checksum); `doCommit` (`CommitJob`); unlock; `Ack(i)` to ci.
- **Middle/tail update task nk.upd.ci** (k = 1, 2): `UpdateReq` → `StorageOperator::update` →
  `ReliableUpdate::update` → `handleUpdate` as above with the head's updateVer (rules: committed / stale /
  missing / advance update). The tail's `forward` has no successor: `kNoSuccessorTarget`, commitIO keeps the
  local updateVer, so it commits at once. Reply `UpdateRsp(result)` to `n(k-1).upd.ci` after the unlock.
- **Chunk lock nk.lock**: receive any `LockChunk`, grant, wait for that holder's `UnlockChunk`; repeat. A second
  write to the chunk waits until the first has committed at this target.
- **StorageTarget nk**: owns the metadata (updateVer, commitVer, chainVer, chunkState, content). It receives any
  of `UpdateJob`, `CommitJob`, `MetaGet`. A job runs `ChunkReplica_update` / `ChunkReplica_commit` (control flow and
  conditions in the source's order), sets the metadata (`store.set`) and answers `JobDone`; it never waits for a
  reader. A `MetaGet` is answered at once with the current metadata (`store.get`).
- **Read task nk.read.ci**: on `Read`/`ReadRetry`: `batchRead` → `aioPrepareRead` (`MetaGet`/`MetaRsp`;
  commitVer != updateVer and not readUncommitted → `kChunkNotCommit`, l.65); the data read; `aioFinishRead` (a
  second `MetaGet`/`MetaRsp`; updateVer changed → `kChunkNotCommit`, l.97). Success: `Value(i, content)` to ci;
  otherwise `ReadRsp(kChunkNotCommit, k, versions)` to ci.retry.

## Method map

| 3FS | here |
|---|---|
| `StorageClientImpl::batchWriteWithRetry` (l.1771) / `sendOpsWithRetry` (l.1143) | `StorageClientImpl.batchWriteWithRetry` (one attempt) |
| `StorageClientImpl::batchWriteWithoutRetry` (l.1799) | `StorageClientImpl.batchWriteWithoutRetry` |
| `StorageClientImpl::sendWriteRequest` (l.1860) | `StorageClientImpl.sendWriteRequest` |
| `StorageClientImpl::batchReadWithRetry` (l.1577) / `sendOpsWithRetry` | `StorageClientImpl.batchReadWithRetry` (first attempt + wait for `Value`) and `StorageClientImpl.runRetryLoop` (process ci.retry) |
| `StorageClientImpl::batchReadWithoutRetry` (l.1642) | `StorageClientImpl.batchReadWithoutRetry` |
| `selectServingTargets` (l.419), `selectRoutingTargetForOps` (l.490) | `StorageClientImpl.selectServingTargets`, `.selectRoutingTargetForOps` |
| `TargetSelectionStrategy::create`, `LoadBalanceStrategy`/`HeadTargetStrategy::selectTarget` | `TargetSelectionStrategy.create`, `.selectTarget` |
| `TargetMap::getByChainId`, `isHead` l.193, `successor` l.221-250, `isTail` l.254 | `TargetMap.getByChainId` → `Target` |
| `StorageOperator::write` (l.233) | `StorageOperator.write` |
| `StorageOperator::update` (l.284) | `StorageOperator.update` |
| `StorageOperator::handleUpdate` (l.333-520) | `StorageOperator.handleUpdate` (pre-lock checks, lock, unlock) + `handleUpdateLocked` (the rest) |
| `StorageOperator::doUpdate` (l.521), `doCommit` (l.616) | `StorageOperator.doUpdate`, `.doCommit` |
| `ReliableUpdate::update` (l.16-128) | `StorageOperator.ReliableUpdate_update` |
| `ReliableForwarding::forwardWithRetry` (l.33), `forward` (l.106), `doForward` (l.138-280) | `StorageOperator.ReliableForwarding_forwardWithRetry`, `_forward`, `_doForward` |
| `StorageTarget::lockChunk` (StorageTarget.h l.61), `CoLockManager` | process `nk.lock` (`ChunkLock.run`) |
| `UpdateWorker::run` (UpdateWorker.cc l.33) + `StorageTarget::updateChunk` | process `nk` (`StorageTarget.run`) |
| `ChunkReplica::update` (ChunkReplica.cc l.132-318) | `StorageTarget.ChunkReplica_update` |
| `ChunkReplica::commit` (l.397-469) | `StorageTarget.ChunkReplica_commit` |
| `StorageOperator::batchRead` (l.82-233) | `StorageOperator.batchRead` (in a read task) |
| `ChunkReplica::aioPrepareRead` (l.38-78), `aioFinishRead` (l.79-131) | `StorageOperator.aioPrepareRead`, `.aioFinishRead` (on the read task's metadata view) |

(`ReliableUpdate.update` etc. are written `ReliableUpdate_update` because they are methods of the per-task
`StorageOperator` object, which holds the task's state; `ChunkReplica_update`/`_commit` likewise in
`StorageTarget`.)

## Decisions and simplifications

1. **Logical tasks** (owner's contract above). Requests never pass through a per-node dispatcher, which would
   order every two requests at a node; the client sends directly to the task of its channel/its reads.
2. **Shared metadata (fix round 1, replaces the earlier publish-and-acknowledge scheme).** In the source the read
   path and the update worker share the chunk metadata: `ChunkStore::get` in `aioPrepareRead` (ChunkReplica.cc
   l.46) and again in `aioFinishRead` (l.85), `ChunkStore::set` in `ChunkReplica::update` (l.248-249, l.307) and
   `ChunkReplica::commit` (l.458); updates and commits additionally run under `StorageTarget::lockChunk`
   (StorageOperator.cc l.368-380). Here the metadata is held by the target process `nk`: update and commit jobs set
   it there (no push, no acknowledgement; the writer never waits for a reader), and a read task asks `nk` for it
   at each of its two version checks, two separate request/response exchanges, so an update landing between them
   is observed by `aioFinishRead` as in the source. The earlier version pushed every `store.set` to the read tasks
   and waited for their acknowledgements; Astra's review (translit-review/REVIEW.md, finding 1) showed that this
   made a read that returned the old value porf-before the write's `Ack` (a reader-to-writer dependency the source
   does not have), already at q = 1. Consequence of the new scheme: two reads at a node are ordered through
   `nk`'s service of their lookups. Note for the record: `ChunkStore::Map` is a `folly::ConcurrentHashMap`
   (code/src/storage/store/ChunkStore.h l.38, ChunkStore.cc l.54-73, l.92-106), whose lookups do not take a lock, so
   this read-read order through `nk` is the model's synchronisation point as directed by the coordinator, not a
   lock visible in the source.
3. **The two store.set calls of ChunkReplica::update** (DIRTY with the new updateVer, l.241-249; CLEAN after the
   write, l.303-307) are merged into one set: a read sees updateVer != commitVer in both and answers
   `kChunkNotCommit` either way; the content of a DIRTY chunk is never returned.
4. **The data read** between the two checks returns the content of the metadata that `aioPrepareRead` obtained.
   An update changes the content only together with updateVer, so whenever the content could differ,
   `aioFinishRead` rejects the read. The earlier non-blocking receive window is dropped: the second lookup replaces it.
5. **Unproductive read retries (interface rule, INTERFACE-agents.md, last paragraph).** The retry on
   `kChunkNotCommit` is immediate and unbounded (`isFastRetryError` l.166, `sendOpsWithRetry` l.1144-1300). An
   attempt whose not-committed answer (same update and commit versions) equals an earlier answer of the same read
   from the same target is cut (`previousAt[target]` in `runRetryLoop`; one entry per target suffices because a
   target's answers are monotone in its versions). The API has no `assume(false)`, so `ci.retry` blocks on a
   receive of `Never`, which nobody sends; ci stays blocked on its `Value`, and the execution counts as blocked.
   The fourth argument `consecutive` (`impl,<q>,fifo,consecutive`) compares only with the immediately preceding
   attempt. It is an **unbounded diagnostic mode**: a read can alternate between two unchanged uncommitted targets
   indefinitely, so the exploration does not end. It is **never used in study runs**.
6. **Addresses**: no init message (INTERFACE.md's Java entry has none; main sends nothing). The k-th spawned process
   (k = 1, 2, ...) is task k + 1 (MpProgram's contract); the program computes `Tid.of(index + 2, role)` and main
   checks every handle against it, failing loudly otherwise.
7. **Daemons**: the nodes' tasks and the retry loops never terminate; they are spawned with
   `MpProcess.spawnDaemon`, so an execution that ends with them blocked on receive is complete (MpProcess:
   "Its ending blocked on a receive does not make the execution blocked"). Only c0, c1 are awaited.
8. **LoadBalance** (TargetSelection.cc l.14-48) picks a random target, then the least-loaded node by process-wide
   counters; INTERFACE.md fixes it as a nondeterministic choice among the three targets (`Nondet.chooseIndex(3)`).
9. **Chunk lock discipline**: `CoLockManager` (common/utils/CoLockManager.h) is not in the cut and not in the
   tree under `code/src`; the lock process grants to any waiting requester. Since the arrival order of lock
   requests is itself nondeterministic, a FIFO lock yields the same set of orders.
10. **ReliableUpdate**: the per-client cache is kept (seqnum, result, succUpdateVer) per update task; its branches
    are never taken (no write is retried). The channel lock (`tryLockChannel`) always succeeds, since the update
    task is the only user of the channel. The client's channel seqnum is not in the fixed `Write` message: the
    head counts the client's writes instead (PORT in `StorageOperator.write`).
11. **Errors**: none can occur under the interface (fixed chain, every reply arrives). An error result at the
    head would make the client retry with back-off in the source; here it fails the run (`AssertionError`), as
    does an unexpected read error in ci.retry.
12. Dropped as the interface says: chain/routing changes, syncing (`isSyncing` is always false; the syncing
    branches are kept as conditions or assertions), resync, leases, RDMA buffers and inline data, checksums
    (the content stands for its checksum; `updateChecksum` reduces to the full-chunk case), truncate/extend/remove,
    chunk creation (the chunk exists at version 0, CLEAN), timeouts, back-off, fault injection, tracing, metrics.
    The chain version is the constant 1.

## Fix rounds

- Round 1 (2026-11-05, Astra's review finding 1): metadata publication with reader acknowledgements replaced by
  lookups at `nk` (decision 2); the receive window dropped (decision 4); decision 5 reworded as the adopted
  interface rule. Messages `MetaSet`/`MetaSetAck` removed, `MetaGet`/`MetaRsp` added. Logs of the first version:
  `logs-r0/`.
- Round 2 (Astra REVIEW-r2.md: faithful under the contract): the two retry-cut comments in the source updated (no
  behaviour change); the remaining read-before-update order documented as the owner-accepted abstraction of
  CONTRACT.md; artifact locations listed; the complete q = 1 bag export added.

## Compile and run (inside this directory)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq.ChainReplication:impl,1,fifo' must 0 1 report - 600 </dev/null
java -Xss4m -Xmx8g -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.craq.ChainReplication:impl,2,fifo' must 0 1 report - 600 </dev/null
```

### Artifact locations

All logs and exports are in OUT_DIR = `/local/home/mkhoshechin/Documents/github-jmc/daavar/benchmarks-cf2/craq/java`
(the staged copy under `daavar/benchmarks-cf/craq` holds only the source, NOTES.md and CONTRACT.md):
- `logs-r0/run-impl1.log`, `logs-r0/run-impl2.log`: the first version (publish-and-acknowledge metadata), q = 1 and
  q = 2, 600 s, both timed out (superseded).
- `run-r1-impl1-fifo-1200.log`: fix round 1, q = 1, fifo, counting mode.
- `run-r1-export-impl1-fifo-30.log` + `export-r1-impl1-fifo.jsonl`, `run-r1-export-impl1-bag-30.log` +
  `export-r1-impl1-bag.jsonl`: the 30 s export checks (aborted by the time limit).
- `run-r1-export-impl1-fifo-1200.log` + `export-r1-impl1-fifo-full.jsonl`: the complete q = 1 fifo export.
- `run-r2-export-impl1-bag-1200.log` + `export-r2-impl1-bag-full.jsonl`: the complete q = 1 bag export (round 2).
- `report*/`: the launcher's report directories of these runs.

Fix round 1 runs (2026-11-05; the machine carried foreign load, load average about 25 on 16 cores).

Compile: no errors, no warnings (javac 17.0.18).

`run-r1-impl1-fifo-1200.log`: `impl,1,fifo`, `must 0 1 report - 1200`, counting mode:
```
ran:        1348 iterations
blocked:    524 (counted by the strategy)
completed:  824
rate:       6.3 iterations/s
elapsed:    215.127 s
graphs:     824 distinct complete, 524 distinct blocked; stack exhausted
stop_reason: exhausted
max_graph_events: 153
mean_graph_events: 127.876
```

Export checks, 30 s each (`must 0 1 report-export-<m> export-r1-impl1-<m>.jsonl 30`):
- `run-r1-export-impl1-fifo-30.log`: ran 164, blocked 65, completed 99, `graphs: 99 distinct complete, 65 distinct
  blocked; stack not exhausted`, `summaries_distinct: 4`, stop_reason timeout; file `export-r1-impl1-fifo.jsonl`
  (header `"model":"p2p","enumeration":"aborted"`, 4 summary lines).
- `run-r1-export-impl1-bag-30.log`: ran 351, blocked 140, completed 211, `graphs: 211 distinct complete, 140
  distinct blocked; stack not exhausted`, `summaries_distinct: 4`, stop_reason timeout; file
  `export-r1-impl1-bag.jsonl` (header `"model":"asyn","enumeration":"aborted"`, 4 summary lines).

Complete export of q = 1, fifo (`run-r1-export-impl1-fifo-1200.log`, file `export-r1-impl1-fifo-full.jsonl`):
ran 1348, blocked 524, completed 824, elapsed 74.9 s, stack exhausted, `"enumeration":"complete"`, 5 summaries.
c0's word is always `[s Write(0,1), r Ack(0)]`; c1's word, counts and multiplicity:

| c1 | status | counts | n |
|---|---|---|---|
| `s Read(1), r Value(1,1)` | done | [0,1,0,1] | 792 |
| `s Read(1)` | blocked (retry cut) | [0,1,0] | 524 |
| `s Read(1), r Value(1,0)` | done | [0,1,0,1] | 26 |
| `s Read(1), r Value(1,1)` | done | [0,0,0,1] | 3 |
| `s Read(1), r Value(1,0)` | done | [0,1,0,0] | 3 |

(Counts in the format's order: for c0's Write and Ack, the c1 events porf-before; for c1's Read and Value, the c0
events porf-before.)

Complete export of q = 1, bag (round 2; `run-r2-export-impl1-bag-1200.log`, file `export-r2-impl1-bag-full.jsonl`,
compiled from the round-2 source): ran 1348, blocked 524, completed 824, elapsed 243.6 s, `graphs: 824 distinct
complete, 524 distinct blocked; stack exhausted`, `"model":"asyn","enumeration":"complete"`, 5 summaries: the same
five patterns as fifo, with the same multiplicities (792, 524, 26, 3, 3).

**Remaining order: the owner-accepted causal abstraction** (`daavar/benchmarks-cf/craq/CONTRACT.md`, item 2; Astra
round 2, REVIEW-r2.md). Every `Value(1,0)` summary has count 1 at c0's Ack: a read that returns 0 makes both lookups
at some target before that target's update job, and `nk`'s program order then places the read before `JobDone` and
the head's Ack. In 3FS the metadata lives in a lock-free concurrent map (ChunkStore.h l.38), so there such a read is
not causally before the update. The contract makes this explicit: a shared memory location is modelled as a
sequential process, and its program order is part of the observation. The specifications are one sequential object
with the same edge, and the Rust ports route the metadata through the node process too, so the refinement checks
compare like with like. What is not claimed is that the port's causal order equals the source's at the memory
level. This is an accepted abstraction of the contract, not an unresolved defect of the reference.

