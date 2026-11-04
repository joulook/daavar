# Cassandra 4.1 LWT Paxos v1 — TraceForge (Must API) port

Crate `bench_cassandra_paxos_v1`, `src/main.rs`. Arguments `<q> <v1|v1nolin> [<fifo|bag>]` (the third is accepted
and ignored; the harness's `--model` selects the delivery model). `visible` = `c0`, `c1`; `spec` and `imp` both
return the implementation program. Source paths below are relative to `paxos-v1-src/` (= `src/java/org/apache/cassandra`).

## Processes

| thread | role |
|---|---|
| `n0`, `n1`, `n2` | coordinator side of node i: receives Setup (tag 1), then its client's `Req` (untagged), runs the Paxos rounds, replies `Rep` to the client. |
| `n0.replica`, `n1.replica`, `n2.replica` | replica side of node i: PaxosState registers and the base-table cell, the PREPARE / PROPOSE / COMMIT / READ handlers; never terminates. |
| `c0`, `c1` | the interface's client code, verbatim; `ci` talks to `ni`. |
| `init` | sends `Setup { nodes: [replica-side thread ids], clients: [c0, c1] }` to `n0`, `n1`, `n2` (tag 1). |
| main | spawns `n0, n0.replica, n1, n1.replica, n2, n2.replica, c0, c1, init`, joins `c0`, `c1`, `init`; sends/receives nothing. |

## Messages

Client/replica messages are the interface's `Req`/`Rep`, verbatim. Internal ones:

| message | source verb | sender -> receiver | contents |
|---|---|---|---|
| `NodeReq::Prepare` | PAXOS_PREPARE_REQ | coordinator -> all 3 replica sides | ballot (Commit.newPrepare: empty update), reply tag |
| `NodeRsp::Prepare` | PAXOS_PREPARE_RSP | replica side -> coordinator (tagged) | promised, inProgressCommit, mostRecentCommit, replica index (= Message.from()) |
| `NodeReq::Propose` | PAXOS_PROPOSE_REQ | coordinator -> all 3 | Commit(ballot, update or empty), reply tag |
| `NodeRsp::Propose(bool)` | PAXOS_PROPOSE_RSP | replica side -> coordinator | accepted |
| `NodeReq::Commit` | PAXOS_COMMIT_REQ | coordinator -> all 3 (commitPaxos, ack tag) or to the replicas missing the MRC (sendCommit, no ack) | Commit |
| `NodeRsp::CommitAck` | PAXOS_COMMIT_RSP | replica side -> coordinator, only when an ack tag was given | — |
| `NodeReq::Read` / `NodeRsp::Read` | READ_REQ/RSP (QUORUM read) | coordinator -> all 3 / back | x and its cell timestamp |

Every round the coordinator waits on gets a fresh tag (`2 + seq`); the coordinator receives only messages with
that tag, so a response that arrives after the round has completed is never consumed (it stays in the mailbox).

## Protocol, role by role

**Replica side** (one per node; state `promised`, `accepted` (option), `committed`, and the cell `(x, ts)`,
initially `NONE`, none, `Committed.none`, `(0, NONE)`):
- Prepare(b): `latest = max(latest(accepted, committed).ballot, promised)`; if `b > latest`: `promised := b`,
  reply `(true, accepted or empty commit, committed)`; else reply `(false, Commit(promised, empty), committed)`.
- Propose(p): if `p.ballot == latest || p.ballot > latest`: unless `p.ballot == committed.ballot`, `accepted := p`;
  reply true. Else reply false. (`promised` is not changed.)
- Commit(c): if `c` has an update and `c.ballot > ts`, write the cell `(v, c.ballot)`; if `c.ballot > committed.ballot`:
  `committed := c` and drop `accepted` unless `accepted.ballot > c.ballot`. Ack if asked.
- Read: reply the cell `(x, ts)`.

**Coordinator side**, per request, at most A = 3 attempts; an attempt = one prepare round:
1. `b := (1 + highest counter seen, i)`; Prepare(b) to all 3; wait for 2 responses (stop at the first rejection).
   Track the latest inProgress (also of rejections), the latest MRC and the MRC per promising replica.
2. Rejected -> next attempt.
3. In-progress non-empty and after the MRC -> propose `Commit(b, inProgress.update)` fail-fast; if accepted by 2,
   commit at QUORUM (wait 2 acks); next attempt either way.
4. A promising replica's MRC differs from the latest MRC -> send that commit to it (no wait); next attempt.
5. Otherwise, the request's proposal:
   - Cas(c, e, v): QUORUM read of x; `x != e` -> empty proposal, reply `CasResult(c, 0, x)`; else proposal `v`,
     reply `CasResult(c, 1, v)`.
   - Read(c), `v1nolin`: no proposal; QUORUM read; reply `Value(c, x)`.
   - Read(c), `v1`: empty proposal.
6. Propose to all 3, wait until 2 accept or all 3 answered. Accepted -> commit at QUORUM if non-empty, then reply
   (a `v1` read does its QUORUM read now and replies `Value`). Accepted by 1 of 3 -> reply `Failed`
   (CasWriteUnknownResultException). Accepted by none -> next attempt.
7. After the third failed attempt reply `Failed`.

## Method map

| Cassandra (file: lines) | here |
|---|---|
| StorageProxy.cas / legacyCas (306-437), the updateProposer lambda | `Coordinator::do_paxos`, arm `Req::Cas` |
| StorageProxy.doPaxos (478-570) | `Coordinator::do_paxos` (loop over `ATTEMPTS`) |
| StorageProxy.beginAndRepairPaxos (572-680) | `Coordinator::begin_and_repair_once` (one loop iteration) |
| StorageProxy.sendCommit (683-690) | `Coordinator::send_commit` |
| StorageProxy.preparePaxos (690-728) | `Coordinator::prepare_paxos` |
| StorageProxy.proposePaxos (734-769) | `Coordinator::propose_paxos` |
| StorageProxy.commitPaxos, commitPaxosLocal (771-870) | `Coordinator::commit_paxos` (local commit as a message to the own replica side) |
| StorageProxy.read / readWithPaxos / legacyReadWithPaxos (1820-1966) | `Coordinator::do_paxos`, arm `Req::Read` |
| StorageProxy.readOne / readRegular / fetchRows (as the QUORUM read) | `Coordinator::quorum_read` |
| BallotGenerator / nextBallot | `Coordinator::next_ballot`, `Coordinator::see` |
| v1/PrepareCallback.onResponse, replicasMissingMostRecentCommit | `prepare_paxos` loop, `PrepareSummary::replicas_missing_most_recent_commit` |
| v1/ProposeCallback.onResponse, isSuccessful, isFullyRefused | `propose_paxos` loop and outcome |
| v1/AbstractPaxosCallback.await | the bounded receive loops (no timeout) |
| v1/PrepareVerbHandler, PaxosState.legacyPrepare (719-761) | `replica`, `ReplicaState::legacy_prepare` |
| v1/ProposeVerbHandler, PaxosState.legacyPropose (763-805) | `replica`, `ReplicaState::legacy_propose` |
| PaxosCommit.RequestHandler (301-323), PaxosState.commitDirect / applyCommit (664-717), Snapshot.merge (210-243) | `replica`, `ReplicaState::commit_direct` |
| PaxosState.Snapshot.latestWitnessedOrLowBound (187-196) | `ReplicaState::latest_witnessed` |
| Commit.isAfter / isSameOrAfter / hasSameBallot, Commit.emptyCommit, Committed.latestCommitted | `Commit::is_after`, ballot `==`/`>`, `Commit::empty` |
| Paxos.isLinearizable (Paxos.java 1215), Config.PaxosVariant (889-930) | `Variant` |
| v1/AbstractPaxosVerbHandler (out-of-range checks) | omitted (single partition, all replicas own it) |

## Decisions and simplifications

1. **Two threads per node.** The node's role is split into the named coordinator thread `ni` and an internal thread
   `ni.replica`. Reason: the Must API's blocking receives return one Rust type, and a node must at any time accept
   either a client `Req` (fixed type) or a replica-side request; with one thread it would have to block on two types.
   Split this way, `ni` waits on `Req` only when idle and on `NodeRsp` only when busy (a client has at most one
   request outstanding), and the replica side waits on `NodeReq` only. This also models Cassandra's local requests,
   which run asynchronously on the PAXOS stage (StorageProxy preparePaxos/proposePaxos `stage.execute`,
   commitPaxosLocal), as messages to the own replica side. Deviation from "one process each" in INTERFACE.md.
2. **Ballot = (counter, node id), lexicographic**, `NONE = (0, 0)`; Commit.isAfter / isSameOrAfter (Commit.java
   284-302) compare `uuidTimestamp()` only; here they compare the whole pair, so distinct ballots never tie.
3. **Fresh ballot** = 1 + the highest counter in this coordinator's own ballots and in every ballot of every prepare
   response it consumed (inProgress, including a rejection's promised ballot, and MRC). Cassandra uses only
   `1 + summary.mostRecentInProgressCommit` plus its clock (StorageProxy l.592); the interface says "highest seen".
   Ballots in read responses (cell timestamps) and anything the node's replica side saw are not counted.
4. **Attempt = one prepare round.** Every `continue` of beginAndRepairPaxos (rejected prepare, in-progress repaired
   or failed, MRC repair) and every rejected own proposal in doPaxos consumes one of the 3 attempts; the sleeps
   (`sleepUninterruptibly(nextInt(100))`) and deadlines are removed.
5. **Partially accepted proposal -> `Failed` immediately**, without using the remaining attempts, as proposePaxos
   throws CasWriteUnknownResultException when `backoffIfPartial && !isFullyRefused()` (StorageProxy proposePaxos,
   l.763-766). Applies to CAS proposals and to the empty proposal of a `v1` read (legacyReadWithPaxos does not catch it).
6. **Variant difference only for reads.** The source's only v1 behavioural difference is legacyReadWithPaxos
   l.1891-1894: `v1nolin` proposes nothing after beginAndRepairPaxos (updateProposer returns null, doPaxos returns
   at once). A rejected CAS proposes an empty update in both variants (legacyCas's updateProposer has no variant
   test). INTERFACE.md's gloss ("rejected CAS operations propose an empty update" only under `v1`) is not followed;
   the code is.
7. **Late responses are ignored.** PrepareCallback keeps receiving after `await()` returns (its fields can still change
   concurrently with beginAndRepairPaxos reading them); here the summary is frozen at the 2nd response (or the first
   rejection). Same for propose responses after success/fail-fast, commit acks beyond 2, and the 3rd read response.
   Implemented with a fresh tag per round; stale messages stay unconsumed.
8. **QUORUM read** = send READ to all 3 replica sides, take the first 2 responses, the value with the greatest cell
   timestamp wins (absent row = `(0, NONE)`). Which 2 replicas answer is left to message arrival. Not modelled:
   data/digest split, speculative retry, and (blocking) read repair (fetchRows/AbstractReadExecutor are outside the
   cut, l.1967-2134 used only as the interface).
9. **sendCommit sends no ack** (StorageProxy 683-690 uses `send` without a callback; the PAXOS_COMMIT_REQ handler
   would still respond, and the response would be dropped). Commits from commitPaxos carry an ack tag and the
   coordinator waits for 2 acks (QUORUM, writeAll, PaxosCommit 301-323 responds after commitDirect).
10. **Commit consistency**: QUORUM for the CAS's own commit and for replayed in-progress commits; a `v1` read's commit
    consistency (ANY) is irrelevant since its proposal is empty and never committed (doPaxos skips empty commits).
11. **Cell write**: Keyspace.apply of the commit's mutation is modelled as last-write-wins by the commit ballot
    (Commit.newProposal stamps the update with the ballot's micros); equal timestamps keep the existing cell.
12. **State purging/TTL, truncation, expiry**: removed (no time): Snapshot.removeExpired is the identity,
    applyCommit's truncation check always passes, PrepareCallback's legacy-TTL escape in
    replicasMissingMostRecentCommit (PrepareCallback l.94-104) never applies.
13. **No failures**: AbstractPaxosVerbHandler's out-of-range rejection, onFailure, timeouts (await), hints,
    unavailable checks, denylist, tracing and metrics are omitted; every replica is alive, so `contacts()` = all 3 and
    `requiredParticipants()` = 2.
14. **promisedWrite** is not kept: legacyPrepare always sets it equal to `promised`, and v1 reads only
    latestWitnessedOrLowBound, which ignores it.
15. **Committed.latestCommitted ties** keep the current committed (with no TTL subclasses, `a` wins on equal ballots);
    then Snapshot.merge returns the old snapshot unchanged, so `accepted` is kept.
16. **No `nondet()`**: every open choice in this cut is a message-arrival order (which 2 of 3 answer first, which
    request a replica serves first), so the program uses none.
17. **Main joins only `c0`, `c1`, `init`**; replicas never end, so joining them would block main.
18. **Arguments**: `q` defaults to 1 and is at least 1; the variant defaults to `v1`; an unknown variant panics.

## Build and run

Compile (in this directory): `cargo build --release -j 2` — succeeds with no warnings ("Finished release profile [optimized]").

Run once (in this directory): `timeout 600 target/release/bench_cassandra_paxos_v1 count impl --model fifo -- 1 v1`

Result: killed by `timeout` at 600 s (wall 10m0.002s), no `execs=` line printed. Last output line:
`Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (3577.82/sec).`
Every execution counts as blocked because the replica threads (`n*` and `n*.replica`) never terminate, as the
interface requires; the instance at q = 1 was not exhausted within the budget.
