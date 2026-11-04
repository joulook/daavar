# Cassandra 4.1 Paxos v1 port

Sources below are relative to `paxos-v1-src/`; line numbers refer to the supplied cut, not another Cassandra revision. `README.md` was read first. The implementation follows the Java methods where `service/paxos/Paxos.md` gives a more general description or describes v2.

## Processes and entry

`implementation` starts `n0`, `n1`, `n2` in that order, then `c0`, `c1`, then the invisible `init`. Every replica is one thread and one event loop containing both a coordinator and replica state. No helper coordinator threads or shared protocol memory are used. The main program thread only creates and joins threads. `init` sends each replica `Setup { nodes, clients }` using tag 2. A selective setup receive leaves any earlier client requests or protocol headers buffered in the mailbox. Clients use the interface's verbatim `Req`, `Rep`, `value`, and `client` definitions and behavior. Each client has at most one outstanding request, so its coordinator needs one active operation slot. Replicas loop forever, even after clients terminate; main's final joins remain blocked as intended.

The program entry is `traceforge::program::main(Program { visible, spec, imp })`. Visible threads are exactly `c0` and `c1`. Both entries build this implementation. Arguments are `<q> <v1|v1nolin> [fifo|bag]`, with `q >= 1`. The optional trailing delivery-model argument is validated; the harness's `--model` selects delivery (default FIFO). Use `--model bag` when supplying a trailing `bag`; the trailing argument does not override the harness. This follows `api/program.rs`'s separation between harness options and program arguments.

## Replica state and coordinator progression

`Replica` stores `promised`, optional `accepted`, `committed`, and a separately timestamped integer `data`, initially zero. The initial ballot is `(0, 0)` and an empty commit has no value. A real proposal carries `Some(value)`; `None` is an empty update, distinct from writing zero. `latest()` is the maximum of promised, accepted and committed ballots. V1 always writes both kinds of promise together, so a separate `promisedWrite` register would duplicate `promised`.

`legacy_prepare` promises only for a ballot strictly greater than `latest()`. Success stores the new promise and returns the prior accepted and committed proposals. Rejection returns the promised ballot in the progress field with an empty update, plus the committed proposal. `legacy_propose` accepts a ballot equal to or greater than `latest()`; it stores accepted unless that ballot is already committed. Neither proposing nor committing advances `promised`. `apply_commit` unconditionally merges the data mutation by timestamp, keeps the greatest committed ballot, and clears accepted only when accepted is no newer than committed.

A coordinator starts at attempt 1 and uses one budget of three attempts across the entire client request. `prepare_paxos` generates `(1 + highest_counter_seen, own_id)`, allocates a new RPC identifier, and broadcasts prepare. A prepare rejection restarts immediately; otherwise the first two delivered successful promises form the summary. The greatest accepted and greatest committed entries are computed independently.

`begin_and_repair_paxos` first refreshes a nonempty accepted value newer than the summary's committed value to the current ballot. It proposes that value, commits it at quorum if successful, then retries the original operation with a fresh ballot. If instead some promise responders lack the latest committed ballot, it sends the commit only to those responders without waiting and retries. Both paths consume the current attempt, including successful replay or repair. The third such retry cause returns `Failed` after issuing/completing the repair required by that attempt.

Once promises are consistent, a CAS performs a separate QUORUM read, evaluates equality against `expected`, and proposes either the requested integer or an empty update with the saved rejected-CAS result. This happens in **both variants**. Normal proposal success needs two accepts; failure waits for all three responses. Replay proposal success also needs two accepts, but failure returns as soon as two rejections make a quorum impossible. A nonempty successful proposal is committed to all replicas and waits for two commit acknowledgements before reporting success. Empty proposals are neither committed nor replayed.

For a serial read, `v1` proposes an empty value, waits for two accepts, then performs the separate QUORUM read and returns its value. `v1nolin` performs that read immediately after begin/repair succeeds, with no empty proposal. The Java cut changes only serial-read behavior between the two v1 variants: the rejected-CAS proposer in `StorageProxy.java:350-354` is unconditional; the variant test is at `StorageProxy.java:1891-1894`. The longer enum name must not be used to infer an additional CAS optimization.

Every read broadcasts full-data requests and resolves the first two replies by greatest data timestamp. If they differ, it sends a data-only repair to the stale respondent and waits for its acknowledgement before using the saved result. A read repair never changes Paxos registers. Serial reads occur after the empty proposal in v1; the CAS precondition read occurs before the CAS proposal. There is no combined v2 prepare/read and no extra read during an unfinished-proposal replay: `beginAndRepairPaxos` itself contains no `readOne`/`fetchRows` call in the supplied Java lines 578-680.

## Messages

All interprocess exchanges use Must sends and blocking receives; local requests to self also traverse the mailbox. Real client messages are untagged `Req::Cas(i, expected, value)` / `Req::Read(i)` with `i < 2`, and untagged `Rep::CasResult(i, applied, value)`, `Rep::Value(i, value)`, or `Rep::Failed(i)`.

The Must API shown in `api/lib.rs:890-901,1254-1285` downcasts receives to one concrete type and exposes no public heterogeneous blocking receive. To multiplex the required, unchanged client enum with internal payloads on the same replica thread, every internal logical message is an untagged `Req::Read(2 + sender_replica_id)` header followed by a tag-1 `Wire` payload. These reserved headers are only replica-to-replica and are invisible. The dispatcher receives an untagged header/request, then selectively receives its payload from that exact sender. Each sender emits payload immediately after its header; FIFO preserves pairing, while bag delivery may pair any already-sent same-sender payload with a header. Headers contain no operation information: their only function is to count and demultiplex payloads. Thus payload delivery order remains the logical message order under bag. There are no fabricated client requests or replies. Setup uses its own tag and needs no header.

| Logical message | Fields and recipient action |
| --- | --- |
| `Prepare` | Coordinator id, RPC id, ballot; run `legacy_prepare`. |
| `Promise` | Replica id, RPC id, success flag, accepted-or-rejecting-promise `progress`, `committed`; observe ballots, update the current prepare summary or retry. |
| `Propose` | Coordinator id, RPC id, proposal; run `legacy_propose`. |
| `Accepted` | Replica id, RPC id, boolean; update the propose callback counts. The source returns a boolean, not a rejecting ballot. |
| `Commit` | Coordinator id, RPC id, proposal, acknowledgement flag; run `apply_commit`; acknowledge only for blocking commits. |
| `CommitAck` | Replica id, RPC id; count toward commit quorum. |
| `Read` | Coordinator id, RPC id; snapshot the current base-register data. |
| `ReadResult` | Replica id, RPC id, timestamped integer; reconcile a read quorum. |
| `ReadRepair` | Coordinator id, RPC id, timestamped integer; merge base-register data without touching Paxos state. |
| `RepairAck` | Replica id, RPC id; finish the blocking repair of the read quorum. |

An RPC identifier is local to its coordinator, monotonically increasing across operations and phases, and separate from ballots. Every response is sent to that coordinator. Late responses are still received; carried ballots update the node's highest observed counter, but counts affect a phase only when its RPC id and expected response kind match. There are no duplicate logical sends per recipient/RPC, so quorum counts are from distinct replicas.

## Method mapping

| Cassandra source method (file and lines) | Rust function / phase |
| --- | --- |
| `StorageProxy.cas` (306-329), v1 branch | `Node::start` |
| `StorageProxy.legacyCas` (329-437), update-proposer lambda (344-389) | `Node::quorum_read`, `Node::read_complete` |
| `CASRequest.readCommand`, `appliesTo`, `makeUpdates` (`service/CASRequest.java:30-46`) | `Node::quorum_read`, equality comparison and `Commit` construction in `Node::read_complete` |
| `StorageProxy.doPaxos` (478-568) | `Node::{start,retry,prepare_paxos,propose_paxos,commit_paxos}`, response arms of `Node::handle` |
| `StorageProxy.beginAndRepairPaxos` (572-680) | `Node::begin_and_repair_paxos`, `Node::prepare_paxos`, `Node::retry` |
| `StorageProxy.sendCommit` (683-688) | `Node::send` with `Wire::Commit { ack: false }` |
| `StorageProxy.preparePaxos` (690-727) | `Node::prepare_paxos`, `Node::broadcast` |
| `StorageProxy.proposePaxos` (734-769) | `Node::propose_paxos`, `Wire::Accepted` handling |
| `StorageProxy.commitPaxos` (771-820), `commitPaxosLocal` (829-858) | `Node::commit_paxos`, `Wire::Commit` / `CommitAck` handling (including self messages) |
| `StorageProxy.readOne` (1820-1825), `read` (1830-1858), `readWithPaxos` (1860-1866), `legacyReadWithPaxos` (1868-1966) | `Node::begin_and_repair_paxos`, empty `Node::propose_paxos`, `Node::quorum_read`, `Node::read_complete` |
| `StorageProxy.readRegular` (1967-2011), `fetchRows` (2068-2134), `concatAndBlockOnRepair` (2029-2051) | `Node::quorum_read`, `Wire::ReadResult` / `RepairAck` handling |
| `PrepareCallback` constructor, `onResponse`, `replicasMissingMostRecentCommit` (`service/paxos/v1/PrepareCallback.java:55-110`) | `Phase::Prepare`, `Wire::Promise` handling, `Node::begin_and_repair_paxos` |
| `ProposeCallback.onResponse`, `getAcceptCount`, `isSuccessful`, `isFullyRefused` (`service/paxos/v1/ProposeCallback.java:53-91`) | `Phase::Propose`, `Wire::Accepted` handling |
| `AbstractPaxosCallback.await`, `getResponseCount` (`service/paxos/v1/AbstractPaxosCallback.java:54-77`) | Counts in `Phase`; event loop remains receptive while awaiting replies |
| `PrepareVerbHandler.doPrepare`, `processMessage` (`service/paxos/v1/PrepareVerbHandler.java:30-40`) | `Wire::Prepare` handling and `Replica::legacy_prepare` |
| `ProposeVerbHandler.doPropose`, `processMessage` (`service/paxos/v1/ProposeVerbHandler.java:30-41`) | `Wire::Propose` handling and `Replica::legacy_propose` |
| `PaxosCommit.RequestHandler.doVerb`, `execute` (`service/paxos/PaxosCommit.java:301-323`) | `Wire::Commit` handling |
| `PaxosState.Snapshot.latestWitnessedOrLowBound` (187-198) | `Replica::latest` |
| `PaxosState.Snapshot.merge` (213-247) | Monotonic committed merge and accepted clearing in `Replica::apply_commit` |
| `PaxosState.commit`, `commitDirect`, `applyCommit` (664-717) | `Replica::apply_commit`, `Replica::apply_data` |
| `PaxosState.legacyPrepare` (719-761) | `Replica::legacy_prepare` |
| `PaxosState.legacyPropose` (763-805) | `Replica::legacy_propose` |
| `Commit.newPrepare`, `emptyCommit`, `newProposal`, `isAfter`, `isSameOrAfter`, `hasSameBallot` (`service/paxos/Commit.java:262-312`) | `Commit`/`Ballot` construction, derived lexicographic comparison, `Replica` methods |
| `Ballot.none`, `atUnixMicrosWithLsb` (`service/paxos/Ballot.java:48-73`), `BallotGenerator.Default.next` (`BallotGenerator.java:49-53`) | `Ballot::default`, `Node::observe`, `Node::prepare_paxos` |
| `PrepareResponse` fields/constructor (`service/paxos/PrepareResponse.java:33-53`) | `Wire::Promise` |
| `Paxos.isLinearizable` (`service/paxos/Paxos.java:1215-1230`), `Config.PaxosVariant` (`config/Config.java:889-930`) | `entry` variant parsing, `Node::linearizable` branch |

## Decisions, replacements, and limits

1. **Membership and quorums.** Replace replica plans, liveness checks, local SERIAL distinctions and snitch selection with all three fixed nodes and quorum two, as required by the interface. Source: `StorageProxy.java:503-514,694-734,782-832,2065-2114`. Every Paxos broadcast contacts all three in index order; response arrival determines which quorum wins. There is no need for a nondeterministic choice among equal proposals: globally unique totally ordered ballots give a unique maximum.
2. **Ballots and data timestamps.** Replace UUID/time generation with interface-mandated lexicographic `(counter, replica_id)`, incrementing one beyond every counter observed by the node, including its replica-handler traffic and late replies. Source: `StorageProxy.java:597-604`, `Commit.java:283-301`, `BallotGenerator.java:44-53`. The same pair timestamps the one data register, including refreshed replay proposals; `Commit.newProposal` explicitly updates mutation timestamps (`Commit.java:278-281,472-475`). No independent random UUID bits are needed. Propose rejections carry only a boolean, as in the source; they do not reveal an unobserved remote ballot.
3. **No time/expiration.** Use in-memory state with no expiry, truncation, schema change, repair low bound, purging configuration or mixed-version compatibility. Sources: `PaxosState.java:187-198,249-267,683-717,734-750,779-792`; `PrepareCallback.java:90-107`; `Paxos.md` section “Paxos system table expiration”. The low bound is the initial ballot. V1's separate `promisedWrite` is redundant because `legacyPrepare` always stores the same ballot in both promise registers (`PaxosState.java:738-742`).
4. **Attempt budget.** Replace the outer and inner contention deadlines/backoff with one shared A=3 budget. Sources: `StorageProxy.java:500-504,545-550,587-588,615-620,645-660,672-680`. A rejected prepare, failed proposal, replay that must finish before the client's request, or missing-commit repair consumes an attempt. Normal and replay commits/reads have no spontaneous timeout: all messages are reliable and every replica keeps serving, so their quorums eventually arrive. Backoff disappears; competing message arrivals supply all scheduling variation. There is no arbitrary random failure choice.
5. **Partial proposal failure.** Java distinguishes a fully refused normal proposal (retry) from a partially accepted one (terminal unknown-result exception): `StorageProxy.java:760-768`, `ProposeCallback.java:32-43,86-91`. The interface explicitly puts rejected proposals/quorum failures under the attempt budget and provides only `Failed`. This port uses the budget for both rejection cases, retaining the source's normal callback rule of waiting for all three before deciding failure and its replay fail-fast rule. A later replay may therefore make an ultimately failed request visible; `Failed` has unknown outcome and does not roll back accepted state. This is an interface-driven replacement of the exception/timeout distinction.
6. **Read abstraction and repair choice.** `fetchRows` delegates snitch selection, digests, speculation, and read-repair policy to classes outside the supplied cut (`StorageProxy.java:2052-2134`). Choose eager full-data requests to all three, resolve first two by greatest timestamp, repair any stale member of those two and wait for that repair. No digest round, additional replicas on mismatch, or background repair is modeled. This is a concrete quorum-read abstraction, not a port of the absent read executor. The read result is fixed at quorum resolution even if a concurrent newer update arrives during repair. The single-register timestamp reconciliation follows the timestamp requirement in `Paxos.md` under “Basic scheme” and the supplied `Commit.newProposal` code. Base-table repair does not update Paxos metadata.
7. **Scheduling and callback boundaries.** Source callbacks and local stages are concurrent (`StorageProxy.java:704-721,748-762`, synchronized callbacks in `PrepareCallback.java:63-85`). A node instead handles one delivered logical message atomically, including a local reply send, and advances its coordinator immediately when a callback reaches its threshold. The summary uses the first successful quorum; late callbacks cannot enlarge an already consumed summary. The source does not prescribe a unique callback-versus-coordinator schedule. No additional choice is needed in this actor representation; arrivals determine order. RPC ids prevent old responses from contaminating a new round.
8. **Internal envelope transport.** The two-message header/payload convention described above is a transport accommodation for the exact required client enum and the supplied typed blocking receive API. Waiting on a payload cannot form a protocol deadlock: its sender sends it before its next receive. Every process exchange is still a Must message; no shared-memory channel or inbox inspection is used.
9. **Retained empty-update optimization and exact variant scope.** No commit or replay of empty proposals, per `StorageProxy.java:525-532,627-643`. Only serial reads omit empty proposals in `v1nolin`, per `StorageProxy.java:1891-1894`; rejected CAS operations still propose empties in both variants (`350-354`). No v2 skip-propose optimization, read-only promise, repair protocol, or combined prepare/read is imported.
10. **Removed facilities.** Failure detection, unavailable replicas, hints, tracing, metrics, token ownership, denial lists, triggers, partitions/rows and serialization are removed. Sources: `StorageProxy.java:316-327,335-343,369-385,789-828`; `AbstractPaxosVerbHandler.java:47-97`; `PaxosCommit.java:314-318`; serializer portions of `Commit`, `Ballot`, `PrepareResponse`. There are no out-of-range requests because all nodes own this sole partition. Replies remain exactly the interface's success/read/failure shapes.

## Build and one requested run

Requested compile command, from this directory:

```sh
cargo build --release -j 2
```

The first invocation failed because crates.io DNS was unavailable. Retried using the existing Cargo cache with the same build flags:

```sh
CARGO_NET_OFFLINE=true cargo build --release -j 2
```

Build succeeded from the cache. A subsequent invocation of the exact requested `cargo build --release -j 2` also succeeded (exit 0), with last output line:

```text
    Finished `release` profile [optimized] target(s) in 0.06s
```

Requested run command, from this directory:

```sh
timeout 600 target/release/bench_cassandra_paxos_v1_sol count impl --model fifo -- 1 v1
```

Executed exactly once. Exit status: **124** (`timeout` reached its 600-second limit). Last output line:

```text
Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (2444.99/sec).
```

The exploration did not finish and printed no final `execs=... blocked=...` summary. The progress line is not an exhaustive verification result. No panic or assertion failure was reported in the captured output. Receive-blocked paths are expected because replicas never terminate. Raw output is retained in `run-output.txt`; Cargo also generated `Cargo.lock` and `target/`. No second program run was performed.
