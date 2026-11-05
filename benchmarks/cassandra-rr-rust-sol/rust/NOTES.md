# Cassandra quorum read repair port

## Scope and source availability

`src/main.rs` implements both coordinator and replica roles in one program. Both
`Program.spec` and `Program.imp` return `entry`; the harness supplies the actual
specification. Only `c0` and `c1` are visible.

The requested `read-repair-src/README.md` is absent. The first attempted source
read reported `cat: read-repair-src/README.md: No such file or directory`.
The methods below were located directly in the supplied Java files. All Java
paths and line numbers below are relative to `read-repair-src/`.

**Validation is blocked:** the required dependency directory
`api/traceforge` is absent. Cargo fails before compiling this program. No
successful build, exploration counts, or conformance result is claimed. In
particular the available `api/lib.rs` references thread and visibility types
whose definitions are outside the permitted input files; their use still needs
confirmation by compilation against the full dependency.

## Roles and startup

The entry closure starts `n0`, `n1`, `n2`, then `c0`, `c1`, and joins the clients
and replicas. It never sends or receives messages. No extra process is created.

The `c1` startup wrapper knows the already spawned processes' addresses and
obtains its own with `thread::current().id()`. It sends one explicitly invisible,
SETUP-tagged `Setup { nodes, clients }` to `n2`. Before entering its replica loop,
`n2` forwards this address book to `n0` and `n1`. Each replica's first receive
selects SETUP, so an early client request cannot be mistaken for setup. This
implements the interface's invisible initialization without main-thread messages
or shared mutable state. The supplied `client` function itself is verbatim.

Each replica owns two `Cell { value, timestamp }` entries, initially `(0, 0)`.
It also owns an optional coordinator operation, a monotonically increasing local
request ID, and the immutable addresses. All communication between processes is
through Must messages. There are no locks, shared protocol state, clocks, or
host networking. A replica can handle another coordinator's request while its
own operation is awaiting replies. After clients finish, all three replica loops
continue receiving forever; main remains in joins and the harness ends the
blocked execution.

## Messages and protocol

The externally visible `Req` and `Rep` enums and client loop match INTERFACE.md.
Client `ci` issues its `q` operations sequentially to `ni`; it waits for the
specified response variant before issuing another request.

Internal messages carry the initiating coordinator and/or local operation ID.
An ID is interpreted in its destination coordinator's namespace. Replica index
`from` identifies the reply source. There are no duplicated or lost messages.

| Message | Sender → receiver | Action |
| --- | --- | --- |
| `Setup` | `c1` → `n2`, then `n2` → `n0`, `n1` | Invisible immutable initialization, before each replica's protocol loop. |
| `Req::Write(client, value)` | client → its coordinator | Allocate an operation ID; send `Mutation` to all three replicas, including self. |
| `Mutation { coordinator, id, patch }` | coordinator → each replica | Reconcile both supplied cells into local storage, then send `MutationAck`. Each cell's value and timestamp equal the written value. |
| `MutationAck { id, from }` | replica → coordinator | Mark that replica acknowledged; on two distinct acknowledgements send `Rep::Ack(client)`. Keep serving and consume a late third acknowledgement without crediting it to another operation. |
| `Req::ReadA(client)` / `Req::ReadAB(client)` | client → its coordinator | Choose one other replica with `nondet()`; contacts are the local replica and that other replica. Send one local data `Read` and one remote digest `Read`. |
| `Read { coordinator, id, ab, digest_only, full }` | coordinator → contacted replica | Snapshot the requested cells; send `Data` or `Digest`. `ab=false` projects away cell `b`. `full` distinguishes mismatch-stage data from initial data. |
| `Data { id, from, full, cells }` | replica → coordinator | Save the initial data, or a fresh full response indexed by replica. |
| `Digest { id, from, digest }` | replica → coordinator | Save the digest. Wait for both initial responses including data. Compare it with the digest computed from initial data. |
| Fresh full `Read` to both contacts | coordinator → the same two replicas | On mismatch, in **both strategies**, re-read both participants. Discard the initial data for reconciliation; never read the third replica. |
| `Repair { coordinator, id, patch }` | coordinator → older participant | **blocking only:** after both fresh responses arrive, merge cell by cell by timestamp. For each participant, send only queried cells where its response differs from the merge. Apply with timestamp reconciliation, then acknowledge. |
| `RepairAck { id, from }` | replica → coordinator | **blocking only:** clear that participant's pending repair bit; reply after the repair threshold is met. |
| `Rep::ValueA` / `Rep::ValueAB` | coordinator → its client | On matching initial digests, return initial data. Otherwise return the merge of fresh responses; blocking waits for repair, none sends no repairs and replies immediately. |

A `ReadA` never repairs `b`. A replica may have acquired a newer cell since its
response; applying an older repair does not overwrite that newer cell. Responses
are immutable snapshots rather than references to replica storage.

### Repair threshold

`BlockingPartitionRepair`'s constructor at
`service/reads/repair/BlockingPartitionRepair.java:69–103` starts with the repair
write quorum and subtracts participants whose mutations are empty, clamping at
zero. `ReplicaPlans.writeReadRepair` at `locator/ReplicaPlans.java:551–579`
keeps the two contacted live replicas; they already satisfy QUORUM. Thus if `r`
of these two participants need nonempty repairs, the threshold is
`max(0, 2 - (2 - r)) = r`. Waiting for all pending repair bits is exactly this
threshold for this fixed instance, including when both participants need repairs
for different cells. A participant requiring no repair contributes implicitly;
no empty repair request or extra acknowledgement is sent.

### Typed mailbox transport

The permitted API's `recv_msg_block<T>` performs a concrete type downcast
(`api/lib.rs:890–900,1254–1269`). A replica must receive the fixed `Req` type
and a different internal payload type through a single event loop.

`exchange` therefore sends an `Internal` payload tagged PAYLOAD, followed by a
NOTICE-tagged `Req::ReadAB(3 + sender_index)`. The replica's outer receive accepts
untagged client requests or NOTICE. A notice triggers a selective receive of one
PAYLOAD from the indicated sender. These reserved `Req` indices are used only
between internal replicas, never sent to or by the client loop. Each internal
message in the table has this two-message transport; `Setup` uses its own tag.

A notice is just a wakeup token, not an operation identifier. Under FIFO, both
payload and notice streams preserve sender order. Under bag, tokens from a
sender are interchangeable and payload arrival chooses which of that sender's
messages is processed. Every notice has a preceding payload send, so this
transport does not introduce a receive waiting for an unsent payload. Operation
IDs and the phase bit on the payload preserve callback routing. These extra
transport events may increase execution counts relative to a heterogeneous
mailbox; they do not represent additional Cassandra operations.

## Cassandra method mapping

Multiple methods collapse into event-loop branches rather than additional
threads or blocking calls. Accessors, metrics, serializers, and unreachable
failure/speculation branches are not claimed as ported methods.

| Cassandra method (source lines) | Rust function or state transition |
| --- | --- |
| `StorageProxy.mutate`, `service/StorageProxy.java:873–905` | `node`, `Req::Write` branch, `Pending::Write` |
| `StorageProxy.performWrite`, same file `1360–1383` | `node`, write-plan initialization and dispatch |
| `StorageProxy.sendToHintedReplicas`, same file `1464–1572` | `node`, three `Mutation` sends via `exchange` |
| `AbstractWriteResponseHandler.get`, `service/AbstractWriteResponseHandler.java:113–142`; `blockFor`, `220`; `signal`, `268` | `Pending::Write` stays pending until the quorum; `node` sends `Ack` at the threshold |
| `WriteResponseHandler.onResponse`, `service/WriteResponseHandler.java:60–67` | `node`, `MutationAck` branch |
| `ConsistencyLevel.quorumFor`, `db/ConsistencyLevel.java:91–94`; `blockFor`, `133–148` | Fixed count two in `node` and `advance_read` |
| `ReplicaPlans.writeNormal.select`, `locator/ReplicaPlans.java:505–536` | All-three write contact loop in `node` |
| `StorageProxy.readRegular`, `service/StorageProxy.java:1967–1973`; `fetchRows`, `2068–2133` | `node` read branches, `start_read`, `advance_read`, `reply_read` |
| `ReplicaPlans.contactForRead`, `locator/ReplicaPlans.java:660–676`; `forRead`, `706–713` | `start_read`, choose local plus one nondeterministic remote |
| `AbstractReadExecutor.getReadExecutor`, `service/reads/AbstractReadExecutor.java:185–213`; `NeverSpeculatingReadExecutor` constructor, `264–276` | `start_read`, fixed one-data-request/no-speculation plan |
| `AbstractReadExecutor.executeAsync`, same file `172–179`; `makeFullDataRequests`, `116–120`; `makeDigestRequests`, `127–132`; `makeRequests`, `135–160` | `start_read` and `exchange` |
| `ReadCallback.onResponse`, `service/reads/ReadCallback.java:213–245`; `awaitResults`, `138–194` | `node` data/digest response branches, `advance_read` readiness checks |
| `ResponseResolver.preprocess`, `service/reads/ResponseResolver.java:60–76`; `DigestResolver.preprocess`, `service/reads/DigestResolver.java:62–68`; `isDataPresent`, `165–168` | `ReadState.data`, `digest`, `responses`, populated by `node` |
| `DigestResolver.responsesMatch`, `service/reads/DigestResolver.java:128–163`; `getData`, `85–126` | `digest` equality and matching branch in `advance_read` |
| `AbstractReadExecutor.awaitResponses`, `service/reads/AbstractReadExecutor.java:404–440`; `awaitReadRepair`, `442–460`; `getResult`, `473–477` | `advance_read` phases and `reply_read` |
| `ReadRepairStrategy.NONE.create` / `.BLOCKING.create`, `service/reads/repair/ReadRepairStrategy.java:31–48` | `entry` strategy parsing; `blocking` branch in `advance_read` |
| `AbstractReadRepair.startRepair`, `service/reads/repair/AbstractReadRepair.java:126–154`; `sendReadCommand`, `94–120`; `awaitReads`, `156–174` | `advance_read`, new full reads to both contacts, collect both fresh responses |
| `DataResolver.resolve`, `service/reads/DataResolver.java:94–133`; `resolveWithReadRepair`, `209–222` | `advance_read`, merge the two fresh snapshots and optionally build diffs |
| `Cells.reconcile`, `db/rows/Cells.java:69–79`; `resolveRegular`, `81–86` | `reconcile`, also used by `apply` |
| `BlockingReadRepair.getMergeListener`, `service/reads/repair/BlockingReadRepair.java:62–65`; `PartitionIteratorMergeListener.getRowMergeListener`, `service/reads/repair/PartitionIteratorMergeListener.java:49–52` | `advance_read`, per-cell diff generation |
| `RowIteratorMergeListener.onCell` / `isQueried`, `service/reads/repair/RowIteratorMergeListener.java:138–155`; `onMergedRows`, `207–226`; `close`, `365–406` | `advance_read`, build per-participant `Patch`, skip empty patches |
| `ReplicaPlans.forReadRepair`, `locator/ReplicaPlans.java:434–437`; `writeReadRepair.select`, `551–579` | Preserve `ReadState.contacts` as the repair write plan |
| `BlockingReadRepair.repairPartition`, `service/reads/repair/BlockingReadRepair.java:112–117` | `advance_read`, dispatch nonempty `Repair` messages |
| `BlockingPartitionRepair` constructor, `service/reads/repair/BlockingPartitionRepair.java:69–103`; `sendInitialRepairs`, `145–164`; `sendRR`, `140–143` | `advance_read`, pending repair bits and `exchange` |
| `BlockingPartitionRepair.onResponse`, same file `119–122`; `ack`, `112–116`; `awaitRepairsUntil`, `172–184` | `node`, `RepairAck` branch and pending-bit check in `advance_read` |
| `BlockingReadRepair.awaitWrites`, `service/reads/repair/BlockingReadRepair.java:83–109`; `StorageProxy.concatAndBlockOnRepair`, `service/StorageProxy.java:2029–2043` | Final reply gated on zero pending repair bits in `advance_read` |
| `ReadOnlyReadRepair.getMergeListener`, `service/reads/repair/ReadOnlyReadRepair.java:47–50`; `awaitWrites`, `71–74` | `advance_read` with `blocking=false`: merge without patches or repair wait |
| `MutationVerbHandler.doVerb`, `db/MutationVerbHandler.java:47–77`; `applyMutation`, `80–83`; `respond`, `34–38` | `node`, `Mutation` branch, `apply`, then `MutationAck` |
| `ReadCommandVerbHandler.doVerb`, `db/ReadCommandVerbHandler.java:61–167`; `StorageProxy.LocalReadRunnable.runMayThrow`, `service/StorageProxy.java:2157–2178` | `node`, `Internal::Read` branch for local and remote requests |
| `ReadCommand.createResponse`, `db/ReadCommand.java:343–351`; `ReadResponse.createDataResponse`, `db/ReadResponse.java:46–49`; `createDigestResponse`, `70–73` | `project`, `digest`, `node` data/digest response construction |
| `ReadRepairVerbHandler.applyMutation`, `db/ReadRepairVerbHandler.java:46–50` | `node`, `Repair` branch, `apply`, then `RepairAck` |

## Decisions and simplifications

1. **Fixed topology and selection.** No token/DC/failure detector logic. All
   three replicas are full, live, stable, in one DC. `ReplicaPlans` leaves
   candidate ordering to the snitch (`locator/ReplicaPlans.java:700–713`), and
   `StorageProxy.fetchRows` describes fastest/closest selection (`2068–2090`).
   Per INTERFACE.md, local is first (data) and a `nondet()` boolean selects one
   of the other two (digest) on every read. Other protocol scheduling choices
   come from message delivery, with no extra nondeterministic sleeps or ordering.
2. **Cell representation.** Partition keys, rows, clustering, tombstones, TTLs,
   counters and storage engine machinery reduce to two live cells. This is the
   requested instance of `Cells.reconcile/resolveRegular`
   (`db/rows/Cells.java:69–142`). Equal timestamps necessarily have equal values
   in this workload; retain the left cell on ties. Mutations and repair use the
   same reconciliation, so reordered older writes cannot roll back newer cells.
   Initial absence is represented as value/timestamp zero.
3. **Atomic handler snapshots and applications.** Each replica handler reads or
   applies its two-cell state in one local transition. The source delegates
   storage execution to `command.executeLocally` and response creation
   (`db/ReadCommandVerbHandler.java:108–114`), and asynchronous mutation apply
   (`db/MutationVerbHandler.java:80–83`); these do not prescribe storage-engine
   interleavings for this in-memory replacement. Source stage callbacks become
   event-loop transitions. Local operations and their acknowledgements use self
   messages rather than invoking Cassandra's local stage directly
   (`service/StorageProxy.java:1540–1544`,
   `service/reads/AbstractReadExecutor.java:155–160`,
   `service/reads/repair/AbstractReadRepair.java:98–102`).
4. **Digest.** Use the ordered pair of requested `(value,timestamp)` cells as
   an identity digest, with a constant zero placeholder for unrequested `b`.
   The interface permits any deterministic digest. Cassandra delegates hashing
   to `ReadResponse.makeDigest` (`db/ReadResponse.java:126–132`); no hash/version
   compatibility machinery is needed. A digest response is used only for
   equality and is never used as full data for reconciliation.
5. **Mismatch stages.** Always take fresh full responses from both original
   contacts, including the initial data replica. This preserves
   `AbstractReadRepair.startRepair`'s explicit resend-to-all behavior
   (`service/reads/repair/AbstractReadRepair.java:125–151`), even for NONE.
   The third replica does not participate without speculation. Initial data
   and full data are distinguished by operation ID plus `full` phase bit.
6. **Repair scope.** Only read cells with differing full-response data generate
   mutations, following `RowIteratorMergeListener.onCell/isQueried`
   (`service/reads/repair/RowIteratorMergeListener.java:138–155`). Do not upgrade
   a read of `a` into a partition-wide repair. Diff against the saved response,
   not against a later view of replica state. Empty patches are omitted, as in
   the listener's `close` (`365–406`). NONE still merges but uses no listener,
   following `ReadOnlyReadRepair.java:33–50,59–74`.
7. **Repair threshold specialization.** The pending-bit representation is exact
   for the two-contact repair plan, as derived above. No acknowledgement is
   required from a replica that already returned the merged cells. The third
   replica is not silently repaired. `BlockingPartitionRepair.java:69–103`
   and `ReplicaPlans.java:551–579` define this behavior; it is not a generic
   requirement to wait for two repair acknowledgements.
8. **Reliable success-only instance.** No speculative reads, writes, retries,
   timeouts, request expiration, failures, hinting, tracing or metrics, as
   requested. Relevant omitted branches are
   `AbstractReadExecutor.java:185–213,233–255`,
   `AbstractReadRepair.java:176–211`,
   `BlockingPartitionRepair.java:198–260`,
   `StorageProxy.java:1490–1538`,
   `MutationVerbHandler.java:47–77`, and
   `ReadCommandVerbHandler.java:118–166`. The three write targets are sent
   immediately, with no forwarding/DC relay (`StorageProxy.java:1546–1558`).
   Late third write acknowledgements are consumed rather than awaited by the
   client. No background repair is invented for NONE.
9. **One outstanding local operation.** The interface's client loop guarantees
   one pending coordinator operation per node; other clients coordinate at other
   nodes. An `Option<Pending>` suffices instead of Cassandra's unbounded callback
   registry. IDs prevent a late third write acknowledgement from completing a
   later write. Read/full/repair callbacks are similarly scoped. Cassandra
   accumulates responses (`service/reads/ResponseResolver.java:60–76`) and checks
   quorum plus data availability (`service/reads/ReadCallback.java:213–245`);
   this instance explicitly records those slots.
10. **Eager reconciliation.** Cassandra merges iterators lazily and gates
    completion in iterator close (`StorageProxy.java:2029–2043`). With two cells,
    merge, patch generation, and reply gating are eager state-machine steps.
    There are no limits, short reads, replica-filtering protection, range reads,
    schema versions or repaired-SSTable tracking in this fixed instance
    (`DataResolver.java:99–133,209–222`).
11. **API transport and startup.** The extra notice/payload transport and the
    invisible `c1` → `n2` setup are implementation choices for the fixed role
    set and typed Must receives, explained above. Cassandra has separate verb
    handlers (`MutationVerbHandler.java:47`,
    `ReadCommandVerbHandler.java:61`, `ReadRepairVerbHandler.java:38–50`) rather
    than a typed single mailbox; its source does not prescribe this encoding.
    No protocol state is communicated through captured mutable variables.
12. **Entry arguments and delivery.** `q` must be positive; strategy must be
    exactly `blocking` or `none`. The optional trailing `fifo|bag` is accepted
    and validated; delivery semantics are controlled by the harness's `--model`
    option in `api/program.rs:165–173`, as stipulated by INTERFACE.md. It does
    not introduce a second protocol-level delivery policy. Both entries use
    the implementation closure; visible names are always `c0`, `c1`.

## Commands and observed last output lines

Run from OUT_DIR (`.`):

```sh
cargo build --release -j 2
```

Exit status **101**. Last output lines:

```text
  failed to read `/local/home/mkhoshechin/Documents/github-jmc/plan/case-studies/cassandra/rr/codex/rust-sol/api/traceforge/Cargo.toml`

Caused by:
  No such file or directory (os error 2)
```

Both requested run commands were attempted, with the following results:

```sh
timeout 600 target/release/bench_cassandra_rr_port count impl --model fifo -- 1 blocking
```

Exit status **127**, last output line:

```text
timeout: failed to run command ‘target/release/bench_cassandra_rr_port’: No such file or directory
```

```sh
timeout 600 target/release/bench_cassandra_rr_port count impl --model fifo -- 1 none
```

Exit status **127**, last output line:

```text
timeout: failed to run command ‘target/release/bench_cassandra_rr_port’: No such file or directory
```

`rustfmt --edition 2021 src/main.rs` formatted the implementation; formatting
is skipped for the fixed visible enums and verbatim client/value functions.
`rustfmt --edition 2021 --check src/main.rs` then exited **0**, with no output.
This parser/format check does not type-check the program and does not replace
the required Cargo build or either exploration run. Resume validation with the
three commands above once the full TraceForge dependency is available at the
required path.
