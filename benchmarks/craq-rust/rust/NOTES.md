# bench_craq_port: 3FS chain replication (CRAQ) of one chunk, ported to TraceForge's Must API

Sources: `plan/case-studies/3fs/craq-src` (3FS main, commit 22fca04); interface: `plan/case-studies/3fs/craq/INTERFACE-agents.md`.
Line numbers below refer to the files in craq-src.

## Roles (threads)

| Thread | Role |
|---|---|
| `n0`, `n1`, `n2` | The storage service of target `t0` (head), `t1`, `t2` (tail): write path and read path (`StorageOperator`), the chunk lock, the chunk replica (`ChunkReplica`). Never terminates. |
| `n0.rx`, `n1.rx`, `n2.rx` | The node's receive side: the address the clients send to. It receives a `Req` and hands it to its node's service thread as `NodeMsg::Client(req)`. Never terminates. See D1. |
| `c0`, `c1` | The storage client (`StorageClientImpl`), the client loop verbatim from INTERFACE.md. Visible. |
| `init` | Sends every service thread one `Setup` (tag 1) with all addresses, then ends. |
| main | Spawns `n0 n1 n2`, `n0.rx n1.rx n2.rx`, `c0 c1`, `init`, in that order; joins `init` and the clients; sends and receives nothing. |

## Messages

| Message | From -> to | Source counterpart |
|---|---|---|
| `Req::Write(i, v)` | `ci` -> `n0.rx` (head) | `WriteReq` (full-chunk `WriteIO`), routed by `HeadTarget` |
| `Req::Read(i)` | `ci` -> `nk.rx` (selected) | `BatchReadReq` with one `ReadIO`, routed by `LoadBalance`. First send visible; resends invisible (`send_msg_as(.., Invisible, ..)`) |
| `NodeMsg::Client(req)` | `nk.rx` -> `nk` | (none; D1) |
| `NodeMsg::Setup{nodes, clients}` (tag 1) | `init` -> `nk` | (none; the addresses are known in the source) |
| `NodeMsg::Update(UpdateReq{client, value, update_ver, chain_ver, from_client:false})` | `nk` -> `n(k+1)` | `UpdateReq` sent by `doForward` (ReliableForwarding.cc l.138-280) |
| `NodeMsg::UpdateRsp(UpdateRsp{commit_ver, commit_chain_ver, checksum})` | `n(k+1)` -> `nk` | `UpdateRsp` / `IOResult` read by `forward` (l.106: `commitIO.commitVer = ioResult.commitVer`) |
| `ReadRsp::Ok` / `ReadRsp::NotCommit{commit_ver, update_ver}` (tag 2, received invisibly) | `nk` -> `ci` | the status of the read's `IOResult` (`kChunkNotCommit`, ChunkReplica.cc l.65 / l.97, with the versions set at l.56-58) |
| `Rep::Value(i, v)` | `nk` -> `ci` | the read's data, after `ReadRsp::Ok` |
| `Rep::Ack(i)` | `n0` -> `ci` | `WriteRsp` once the head committed |

## Protocol, role by role

- Client, write: `send_msg(head, Write(i, v))`, wait for `Ack`.
- Client, read: select a target (`nondet`: t0, else t1, else t2), send `Read(i)`, receive the tagged `ReadRsp` invisibly; on `NotCommit` select again and resend at once (isFastRetryError, StorageClientImpl.cc l.166); on `Ok` receive the visible `Value`.
- Node, `Client(Write)` (head only; a non-head panics with kRoutingError, StorageOperator.cc l.338): an `UpdateReq` with `update_ver = 0`, `from_client = true` (`write`, l.233), into `handle_update`.
- Node, `Update` from the predecessor (`update`, l.284): into `handle_update`.
- `handle_update`: if the chunk lock is held, the update waits in a FIFO queue; otherwise `locked_update`.
- `locked_update` (lock held): `ChunkReplica::update`: at the head (`update_ver == 0`) `updateVer += 1` (advance-update check); elsewhere the committed/stale/missing rules (ChunkReplica.cc l.215-240). Content := value. The chunk is now updated, not committed (`updateVer = commitVer + 1`). With a successor: send `Update` (with the head's version) and keep the update in flight. At the tail (kNoSuccessorTarget): commit at once with `commitVer = updateVer`.
- `UpdateRsp` from the successor: check commit version == local update version and checksum == local content (StorageOperator.cc l.452-495), then `ChunkReplica::commit` (commitVer := commitIO.commitVer; state COMMIT when commitVer == updateVer).
- After the commit: reply (`Ack` to the client at the head, `UpdateRsp{commit_ver, CHAIN_VER, content}` to the predecessor elsewhere), release the lock, start the next waiting update if there is one.
- `Client(Read)` at any node (`batchRead`, `aioPrepareRead`): if `commitVer != updateVer` send `NotCommit{commitVer, updateVer}`; otherwise send `Ok` then `Value(i, content)`.

## Function map

| 3FS function (file:lines) | Port (src/main.rs) |
|---|---|
| `StorageClientImpl::write` l.1992 / `batchWriteWithRetry` l.1771 / `batchWriteWithoutRetry` l.1799 / `sendWriteRequest` l.1860 (HeadTarget) | `client`, arm `0` (verbatim interface code) |
| `StorageClientImpl::read` l.1748 / `batchReadWithRetry` l.1577 / `batchReadWithoutRetry` l.1642, retry loop l.1140-1300, `isFastRetryError` l.166 | `client`, arm `_` (read loop) |
| `selectRoutingTargetForOps` l.490, `LoadBalanceStrategy::selectTarget` (TargetSelection.cc l.20) | `select_target` |
| `StorageOperator::write` l.233 | `Node::write` |
| `StorageOperator::update` l.284 | `Node::update` |
| `StorageOperator::handleUpdate` l.333-390 (head check, lock) | `Node::handle_update` |
| `handleUpdate` l.392-460 (doUpdate, version rules, forwardWithRetry) | `Node::locked_update` |
| `handleUpdate` l.460-495 (successor's reply, version/checksum checks) | `Node::on_successor_reply` |
| `handleUpdate` l.496-520, `StorageOperator::doCommit` l.616 | `Node::commit_and_reply` |
| return of `handleUpdate` / `ReliableUpdate::update` / the lock guard's release | `Node::reply_and_unlock` |
| `StorageOperator::doUpdate` l.521 + `UpdateJob`/`UpdateWorker` | direct call `Chunk::update` in `locked_update` |
| `ReliableForwarding::forwardWithRetry` l.33, `forward` l.106, `doForward` l.138 | the `send_msg(succ, Update)` in `locked_update` + `on_successor_reply` |
| `TargetMap::isHead` l.193, `successor` l.221-250, `isTail` l.254 | `Node::is_head`, `Node::successor`, `Node::predecessor` |
| `StorageTarget::lockChunk` (StorageTarget.h l.61) | `Node::in_flight` (lock holder) + `Node::waiting` |
| `ChunkReplica::update` l.132-318 | `Chunk::update` |
| `ChunkReplica::commit` l.397-469 | `Chunk::commit` |
| `StorageOperator::batchRead` l.82-233, `ChunkReplica::aioPrepareRead` l.38-78, `aioFinishRead` l.79-131 | `Node::batch_read` |
| `ChunkMetadata` (updateVer, commitVer, chainVer, chunkState) | `Chunk` |

## Decisions and simplifications

- D1, receive thread per node. A TraceForge receive is typed (`expect_msg` panics on another type, lib.rs l.890), and the client messages have the fixed type `Req`, while a node also receives the chain's messages. One service thread cannot wait for "a `Req` or a chain message". Each node therefore has a receive thread `nk.rx` that turns `Req` into `NodeMsg::Client`. The service thread receives only `NodeMsg`. `head` and `targets` in the client are the `nk.rx` thread ids, not the `nk` ids. INTERFACE.md says they are the node threads, so this is a deviation. It adds one hop per client request. I did not use `inbox` because it has no visibility-annotated variant, so I could not tell whether the harness supports it.
- D2, read status as a separate message. The visible reply type `Rep` has no not-committed variant, and the interface's client code ends a read with `recv_msg_block::<Rep>()`. The node therefore answers every read first with an invisible tagged `ReadRsp` (`Ok` or `NotCommit{versions}`), and on `Ok` then sends `Rep::Value`. The source has one `BatchReadRsp`.
- D3, visibility. The first `Read` of an operation is the visible request. Resends use `send_msg_as(.., Visibility::Invisible, ..)`, and the `ReadRsp` is received with `recv_tagged_msg_block_as(Visibility::Invisible, ..)`. These are lib.rs's annotated twins of `send_msg`/`recv_tagged_msg_block`. They were used because the interface requires the retries to be invisible.
- D4, atomic steps. `doUpdate`/`doCommit` go through the update worker in the source (awaited coroutines). Here each is one step of the node's thread. The DIRTY window inside `ChunkReplica::update` (l.244 to l.300) is therefore never observed. `aioFinishRead`'s "update version changed during the read" (l.97) cannot happen either.
- D5, lock order. `lockChunk`/`lockGuard.lock()` (StorageOperator.cc l.380-390, StorageTarget.h l.61) does not say in which order waiters get the lock. I use FIFO by arrival. With two clients there is at most one waiter.
- D6, ReliableUpdate. ReliableUpdate.cc l.16-128 caches results per client channel for duplicated or retried requests. No write is ever retried here (every reply arrives), so the cache is never hit, and it is omitted. The `ChunkCommittedUpdate` and `ChunkStaleUpdate` branches of handleUpdate are ported anyway. All other error codes (missing/advance update, version mismatch, checksum mismatch, routing error, not clean) are unreachable in this instance and `panic!`.
- D7, retry stutter cut. The source retries a `kChunkNotCommit` read at once and without bound (isFastRetryError, l.166; no back-off by the interface). Under Must, a node could always pick the client's next retry before the chain message, which gives infinitely many executions. The client records the `(target, commitVer, updateVer)` of each `NotCommit` answer in the current read. A repeated answer means no node state changed in between, and that execution is cut with `assume!`. The cut execution is the same as one where the retry was served later, so no visible behaviour is lost, and the versions are bounded, so the retries are bounded. Cut executions count as `blocked`.
- D8, initial state. The chunk starts with `content = 0`, `updateVer = commitVer = 0`, state `COMMIT`. The interface's "clean" means not DIRTY. Chain version is constant 1 (`CHAIN_VER`), and the chain-version checks are kept but never fail.
- D9, the update version is allocated by the head. At the head (`update_ver == 0`, fromClient) `updateVer += 1`. The forwarded `UpdateReq` carries it. Successors apply the committed/stale/missing rules (ChunkReplica.cc l.215-240). The successor's reply carries `commitVer` and the content as checksum (StorageOperator.cc l.452-495 compares them).
- D10, termination. Nodes and receive threads loop on `recv_msg_block` forever. main joins only `init` and the clients. So every execution ends blocked on receive (`execs=0`): the harness counts these as `blocked`, and they include the D7 cuts.
- D11, target selection. LoadBalance (TargetSelection.cc l.20-41) picks by I/O counts and a random index. Following the interface, it is a nondeterministic choice among three targets (`nondet()`: t0; else `nondet()`: t1; else t2), made again at each retry.
- Not ported (interface): syncing/resync, routing changes, leases, truncate/extend/remove, RDMA/inline buffers, timeouts and back-off, fault injection, tracing and metrics.

## Compile and runs

```
cd /local/home/mkhoshechin/Documents/github-jmc/daavar/benchmarks-cf2/craq/rust
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
    Finished `release` profile [optimized] target(s) in 2m 01s      (no warnings)

timeout 600 target/release/bench_craq_port count impl --model fifo -- 1
execs=0 blocked=29
--- measurements
wall_ms: 16
explore_ms: 16

timeout 600 target/release/bench_craq_port count impl --model fifo -- 2
execs=0 blocked=1222
--- measurements
wall_ms: 457
explore_ms: 457
```

Check (temporary, removed): logging every `NotCommit` at q=1 showed retries from `t0` (versions 0/1, 21 times) and `t1` (20 times), and never from the tail, as expected: the tail commits in the same step as its update.
