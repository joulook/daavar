# bench_cassandra_rr_port: notes on the port

This crate ports the Apache Cassandra 4.1 quorum write path and the quorum read path with read repair to TraceForge's
Must API. It covers both strategies, `blocking` (ReadRepairStrategy.BLOCKING) and `none` (ReadRepairStrategy.NONE).
The setting is fixed: 3 replicas, 2 clients, one partition with cells `a` and `b`, and QUORUM (2 of 3) everywhere.
Source paths below are relative to `read-repair-src/` (Cassandra's `src/java/org/apache/cassandra`).

## Processes (threads)

| thread | role |
|---|---|
| main (entry closure) | Spawns `n0`, `n1`, `n2` and then `c0`, `c1`. Sends each replica one `Setup` (tag 1) and joins all five. |
| `c0`, `c1` | The interface's client, verbatim. `ci` sends to `ni`. |
| `n0`, `n1`, `n2` | The coordinator side for its client: the QUORUM write, the QUORUM read, read repair. Spawns its own replica-side thread `ni.replica`. |
| `n0.replica`, `n1.replica`, `n2.replica` | The replica side (the verb handlers) and the replica's in-memory cells. Never terminates. |

Why there are two threads per replica: a Must receive has one message type. In its idle loop a replica has to accept
both the client's `Req` and the internal requests from other coordinators. With one thread it would deadlock or need
a heterogeneous receive, and the API has none. So process `ni` is split into two threads:
- `ni` receives only `Req` (untagged) and responses (`Net`, tag 3).
- `ni.replica` receives only internal requests (`Net`, untagged).

The two threads share no memory. The local replica is reached by a message, the same as a remote one. This matches
the source, where the local apply and read run asynchronously on a stage (`performLocally` in StorageProxy l.1464-1560;
`Stage.READ.maybeExecuteImmediately` in AbstractReadExecutor.makeRequests and AbstractReadRepair.sendReadCommand).

## Messages

| message | from → to | source counterpart |
|---|---|---|
| `Setup { replicas, clients }` (tag 1) | main → `ni` | the interface's one invisible setup message |
| `Net::Peer { replica, store }` (tag 2) | `ni` → `nj` (j≠i), once at start | none: tells the other coordinators the address of `ni.replica` |
| `Req::Write/ReadA/ReadAB` (untagged) | `ci` → `ni` | client request (visible) |
| `Rep::Ack/ValueA/ValueAB` | `ni` → `ci` | client reply (visible) |
| `Net::Mutation { rid, from, ts }` | `ni` → every `nj.replica` | MUTATION_REQ |
| `Net::MutationAck { rid, replica }` (tag 3) | `nj.replica` → `ni` | MUTATION_RSP |
| `Net::Read { rid, from, cols, digest }` | `ni` → `nj.replica` | READ_REQ (a data read or a digest read) |
| `Net::DataRsp { rid, replica, cells }` / `Net::DigestRsp { rid, replica, digest }` (tag 3) | `nj.replica` → `ni` | READ_RSP (data or digest) |
| `Net::Repair { rid, from, cells }` | `ni` → `nj.replica` | READ_REPAIR_REQ (only the cells to repair) |
| `Net::RepairAck { rid, replica }` (tag 3) | `nj.replica` → `ni` | READ_REPAIR_RSP |

`rid` is a per-coordinator callback id: the message id together with its RequestCallbacks entry. A coordinator
drops a response whose `rid` is not the callback it is waiting on. For example, the third write acknowledgement can
arrive after the write already returned at QUORUM; RequestCallbacks ignores such a response the same way once the
handler has completed.

## The protocol, role by role

- **Client `ci`**: the interface's loop, verbatim.
- **Coordinator `ni`, at startup**:
  1. Receive `Setup`.
  2. Spawn `ni.replica`.
  3. Send `Peer` to the two other coordinators.
  4. Receive their two `Peer` messages, in index order, filtered by sender.
  5. Loop on client requests.
- **Write `Write(c, v)`**:
  1. Send `Mutation{ts=v}` to all 3 replica threads, the local one first and then the others in index order.
  2. Await 2 `MutationAck`s.
  3. Reply `Ack(c)`.
- **Read `ReadA` (cols `[a]`) or `ReadAB` (cols `[a,b]`)**:
  1. Choose the other contact: `nondet()` between the two non-local replicas. The contacts are `[local, other]`.
  2. Send a digest read to `other` and a data read to the local replica. The local read goes last, as in
     makeRequests.
  3. Await both responses.
  4. Compare the digest of the data response's cells with the digest response.
  5. **If they match**, reply with the data response's values.
  6. **If they do not match** (both strategies; NONE is ReadOnlyReadRepair, which extends AbstractReadRepair):
     1. Send full data reads to both contacts and await both.
     2. Reconcile cell by cell: the larger timestamp wins (`Cells.reconcile`).
     3. **`blocking`**: for each contact, collect the queried cells whose merged cell differs from that contact's cell.
        Send one `Repair` to each contact that has such cells. Await one `RepairAck` per `Repair` sent, then reply
        with the merged values.
     4. **`none`**: reply with the merged values and send no repair.
- **Replica side `ni.replica`**:
  - `Mutation`: merge the cells, then reply `MutationAck`.
  - `Read`: reply `DataRsp` with the cells of `cols`, or `DigestRsp` with their digest.
  - `Repair`: merge the cells, then reply `RepairAck`.
  - Merging always uses reconcile, so an older cell never overwrites a newer one.

## Cassandra methods and their Rust counterparts

| Cassandra (file:method) | Rust |
|---|---|
| StorageProxy.mutate, performWrite, sendToHintedReplicas (l.873-905, 1360-1385, 1464-1560) | `Coordinator::write` (the sends) |
| AbstractWriteResponseHandler.get / WriteResponseHandler.onResponse, blockFor | `Coordinator::write` (the loop until `QUORUM` acks) |
| db/MutationVerbHandler.doVerb / applyMutation, Mutation.apply | `replica_store`, arm `Net::Mutation` |
| StorageProxy.readOne/read/readRegular/fetchRows (l.1820-2134), concatAndBlockOnRepair | `Coordinator::read` (the sequence of steps) |
| ReplicaPlans.forRead / contactForRead (l.660-715) | `Coordinator::read` (the `nondet()` choice of `other`; contacts `[me, other]`) |
| AbstractReadExecutor.getReadExecutor → NeverSpeculatingReadExecutor (l.190, 258), executeAsync, makeDataRequests/makeDigestRequests (l.100-178) | `Coordinator::read` (one data read to the local replica, one digest read to `other`) |
| ReadCallback.awaitResults / onResponse (l.126, 179) | `Coordinator::read` (the wait for data and digest), `Coordinator::await_rsp` |
| DigestResolver.preprocess, responsesMatch (l.105), getData (l.77) | `Coordinator::read` (`digest_of(&data) == digest`; return `data`) |
| AbstractReadExecutor.awaitResponses (l.396-440) → ReadRepair.startRepair | `Coordinator::read` (the mismatch branch) |
| AbstractReadRepair.startRepair (l.126), sendReadCommand, awaitReads (l.156) | `Coordinator::data_reads` |
| DataResolver.resolve (l.94-) with Cells.reconcile | `Coordinator::read` (the `merged` computation), `reconcile` |
| RowIteratorMergeListener (onCell, isQueried, close), PartitionIteratorMergeListener | `Coordinator::read` (the `repairs` computation) |
| ReplicaPlans.forReadRepair / writeReadRepair (l.434, 551) | the write plan is the 2 read contacts (they already reach QUORUM) |
| BlockingReadRepair.repairPartition (l.112), BlockingPartitionRepair constructor (latch), sendInitialRepairs, ack | `Coordinator::read` (sending `Repair`, `block_for`) |
| BlockingReadRepair.awaitWrites (l.83) → BlockingPartitionRepair.awaitRepairsUntil (l.172) | `Coordinator::read` (the loop until `block_for` `RepairAck`s) |
| ReadOnlyReadRepair (NONE): NOOP merge listener, no repairPartition | `Strategy::None` (repair branch skipped) |
| db/ReadCommandVerbHandler.doVerb (l.50), ReadResponse (data vs digest) | `replica_store`, arm `Net::Read` |
| db/ReadRepairVerbHandler.doVerb / applyMutation (l.31) | `replica_store`, arm `Net::Repair` |
| ReadRepairStrategy (BLOCKING, NONE) | `Strategy`, from the program argument |
| ConsistencyLevel.QUORUM blockFor for RF 3 | `QUORUM = 2` |

## Decisions and simplifications

1. **Two threads per replica process.** See "Processes" above. The extra `Peer` exchange gives each coordinator the
   addresses of the other replica threads. Without it a coordinator only knows the addresses from `Setup`.
2. **The setup message is sent by the entry closure.** The interface says both that the main thread "sends and
   receives nothing" and that "the replicas receive the addresses of all processes in one invisible setup message
   before anything else". Only the entry closure knows every address: replicas are spawned before clients, and a
   replica must know its client's address to reply. So the entry closure sends exactly one `Setup` per replica and
   nothing else.
3. **Contacts and which replica gets the data read.** contactForRead takes the first `blockFor` candidates in the
   order of `sortedByProximity` (ReplicaPlans l.660-680, l.425). That order depends on the snitch, so the source
   leaves it open. As the prompt says, I take the local replica plus one nondeterministically chosen other replica.
   The first contact gets the data read. I put the local replica first: a snitch ranks the local node closest, and
   the prompt names "the local replica and one other". So the data read is local and the digest read is remote.
4. **Speculation and timeouts never fire.** The executor is NeverSpeculatingReadExecutor. Its maybeTryAdditionalReplicas
   is time-based and is omitted. The following are also time-based and also omitted:
   - AbstractReadRepair.maybeSendAdditionalReads (l.185).
   - BlockingPartitionRepair.maybeSendAdditionalWrites (l.198). With no timeouts it returns before sending.
   - the `cheap quorum` upgrade in mutate (maybeTryAdditionalReplicas).

   Every await has no deadline.
5. **Read repair awaits.** BlockingPartitionRepair's latch is `writeQuorum` minus the write-plan contacts that get no
   mutation (l.75-90, clamped at 0). The write plan is the two read contacts. So the coordinator waits for every
   repair it sent: 1 or 2.

   The order in which repairs are sent iterates a ConcurrentHashMap (`pendingRepairs`), which leaves the order open.
   I use contact order: local, then other.
6. **Only the queried cells are read, digested and repaired.** A `ReadA` data or digest response holds only cell `a`.
   The source may fetch more columns than it queries (RowIteratorMergeListener.isQueried, CASSANDRA-10655 comment),
   but it repairs only queried cells and the digest has no observable effect beyond matching. A `ReadA` therefore
   repairs `a` only, as the interface requires.
7. **The digest.** It is `[v, ts]` per cell read, which is deterministic and injective. Equal data always gives equal
   digests, so the "logically identical data, different digests" case (BlockingPartitionRepair l.80 comment) never
   occurs.
8. **The data model.**
   - Every cell is `(v, ts)`, and an absent cell is `(0, 0)`. Written timestamps are at least 1, so an absent cell
     loses every reconcile. An absent cell reads as 0, as the interface says.
   - A write sets both cells to `(v, v)`.
   - The replica applies mutations and repairs with reconcile, as the memtable merge does. Equal timestamps carry
     equal values here, so tie-breaking is moot.
   - Row liveness info, deletions, tombstones and TTL are absent: the write is a CQL `UPDATE`.
9. **Write order.** sendToHintedReplicas hands the local apply to the stage before sending the remote mutations. I
   send to the local replica thread first, then the others in index order. All three are sent before any ack is
   awaited.
10. **Late responses.** A response for a completed callback is dropped when the coordinator next waits for responses
    (`await_rsp`). Responses that are still pending when the run ends stay unread, which is harmless.
11. **Not ported** (the interface replaces them): hints, tracing, metrics, diagnostics, repaired-data tracking, Paxos
    reads, transient replicas, multiple DCs, range reads, short-read protection and replica filtering.
12. **Arguments.** `<q> <blocking|none> [<fifo|bag>]`. The optional third argument is ignored here because the
    harness's `--model` sets the delivery model. `spec` and `imp` both return this implementation.
13. **Termination.** Clients end. `ni` and `ni.replica` never end. The entry closure joins the clients and then the
    replicas, so every execution ends blocked on receive, and `count` reports them as `blocked`.

## Compile

Inside this directory:

```
cargo build --release -j 2
```

## Runs

Both runs are inside this directory.

```
timeout 600 target/release/bench_cassandra_rr_port count impl --model fifo -- 1 blocking
```
Result line `execs=0 blocked=336`. The measurement block that follows ends with the line `cache_hits: `. Wall time
about 37 ms.

```
timeout 600 target/release/bench_cassandra_rr_port count impl --model fifo -- 1 none
```
Result line `execs=0 blocked=240`. The block that follows ends with `cache_hits: `.

Extra sanity check, not requested: `-- 2 blocking` gave `execs=0 blocked=241152` and `-- 2 none` gave
`execs=0 blocked=133584`. Neither run panicked.
