# Cassandra 4.1 LWT Paxos v1: Java port (JMC message-passing API)

Source file: `java/bench/cassandra_paxos_v1/CassandraPaxosV1.java` (one file, package `bench.cassandra_paxos_v1`,
class `CassandraPaxosV1`, implementing `org.mpi_sws.jmc.mp.launch.MpProgram`, constructed from
`impl,<q>,<v1|v1nolin>,<fifo|bag>`).
Line numbers below refer to the cut in `plan/case-studies/cassandra/paxos-v1-src` (paths relative to
`src/java/org/apache/cassandra`), except where `src-4.1` is named.
Every line of the Java file that differs from the source is marked `// PORT:` with the reason.

## 1. Processes

| Role | Process | Behaviour |
|---|---|---|
| `c0`, `c1` (visible) | client | INTERFACE.md client, unchanged: q operations, kind `(i+j) mod 2`; kind 0 sends `Cas(i, known, 1+i+2j)`, kind 1 sends `Read(i)`, to `n_i`; waits for one of `CasResult`/`Value`/`Failed`; `known` updated from the first two. |
| `n0`, `n1`, `n2` (internal) | replica + coordinator | one message loop (`Node.run`) that never ends. It serves the replica verbs (prepare, propose, commit, read) and, for a request of its own client, runs the coordinator (`clientCas` -> `legacyCas`, `clientRead` -> `legacyReadWithPaxos`). While the coordinator waits for responses (`Node.await`) it keeps serving the replica verbs, so two coordinators waiting on each other cannot deadlock. |
| main | | creates the five `MpThread`s (n0, n1, n2, c0, c1; the task ids are minted at creation), records their `Tid`s, starts replicas in index order, then clients, then awaits c0, c1, n0, n1, n2. It sends and receives nothing. Executions end with the replicas (and thus main) blocked on receive. |

Every send and receive uses the program's communication model: `fifo` -> `CommunicationModel.P2P`,
`bag` -> `CommunicationModel.ASYN`. No `Nondet` choice is used: every choice the protocol leaves open
(which responses form the quorum, the order of concurrent requests at a replica) is made by the order of
message arrival.

## 2. Messages

Client messages (fixed by INTERFACE.md): `Cas{client,expected,value}`, `Read{client}`,
`CasResult{client,applied,value}`, `Value{client,value}`, `Failed{client}`.

Internal messages (all `@Tag` records; `id` = the coordinator's callback id, `from` = sender replica index;
they stand for `Message.id()`/`Message.from()` of `net/Message`):

| Message | Source verb (net/Verb.java l.124-129) | Payload | Sent by | Answered by |
|---|---|---|---|---|
| `PaxosPrepareReq(id, from, Commit)` | PAXOS_PREPARE_REQ | `Commit.newPrepare(ballot)` (empty update) | `preparePaxos`, to all 3 replicas (itself included) | `PaxosPrepareRsp` |
| `PaxosPrepareRsp(id, from, PrepareResponse)` | PAXOS_PREPARE_RSP | `PrepareResponse(promised, inProgressCommit, mostRecentCommit)` | `PrepareVerbHandler` | - |
| `PaxosProposeReq(id, from, Commit)` | PAXOS_PROPOSE_REQ | the proposal | `proposePaxos`, to all 3 | `PaxosProposeRsp` |
| `PaxosProposeRsp(id, from, boolean)` | PAXOS_PROPOSE_RSP | accepted? | `ProposeVerbHandler` | - |
| `PaxosCommitReq(id, from, Commit)` | PAXOS_COMMIT_REQ | the agreed commit | `commitPaxos` (to all 3), `sendCommit` (to the replicas missing the MRC, `id = -1`) | `PaxosCommitRsp` (always, as `PaxosCommit.RequestHandler.doVerb` responds; with `id = -1` nobody consumes it) |
| `PaxosCommitRsp(id, from)` | PAXOS_COMMIT_RSP | NoPayload | `PaxosCommit.RequestHandler` | - |
| `ReadReq(id, from)` | READ_REQ (QUORUM read) | read x | `fetchRows`, to all 3 | `ReadRsp` |
| `ReadRsp(id, from, x, timestamp)` | READ_RSP | the replica's x and its write timestamp (`Ballot.none()` for the absent row) | `ReadCommandVerbHandler` | - |

Data records: `Ballot(counter, node)`, `PartitionUpdate(isEmpty, x)`, `Commit(ballot, update)`,
`PrepareResponse(promised, inProgressCommit, mostRecentCommit)`.

## 3. The protocol, role by role

Coordinator, CAS (`legacyCas` l.329-437 -> `doPaxos` l.478-555):
1. `beginAndRepairPaxos` (l.572-680), loop: fresh ballot (`nextBallot`), `preparePaxos` to all three,
   await 2 promises (or the first rejection, which ends the wait: `PrepareCallback.onResponse`).
   - not promised -> retry with a fresh ballot (`continue`);
   - `inProgress` non-empty and after the most recent commit -> re-propose it with the new ballot
     (`proposePaxos(..., backoffIfPartial=false)`), on success commit it at QUORUM, then retry (`continue`);
   - some promising replica's MRC differs from the highest MRC -> `sendCommit` of the MRC to those replicas
     (no wait), retry (`continue`);
   - else return the ballot.
2. `createUpdateProposal(ballot)`: QUORUM read of x (`readOne` -> `fetchRows`), `IF x = expected`:
   proposal `SET x = value` and result `null`, else an empty proposal and result = the read value.
3. `proposePaxos(proposal, backoffIfPartial=true)` to all three, awaiting 3 responses or 2 accepts.
   Accepted by a quorum -> commit (`commitPaxos` at QUORUM: to all three, await 2 acks) unless the
   proposal is empty; reply `CasResult(applied=1, value)` or `CasResult(applied=0, current)`.
   Refused by all three -> retry from 1. Accepted by 1 -> `CasWriteUnknownResultException` -> `Failed`.

Coordinator, SERIAL read (`legacyReadWithPaxos` l.1868-1966): `doPaxos` with replay commits at QUORUM
and own commit at ANY; the proposer is `ballot -> null` for `v1nolin` (stop after the prepare/repair
phase) and `ballot -> (empty update, null)` for `v1` (propose an empty update; it is never committed);
then `fetchRows` at QUORUM; reply `Value(x)`.

Replica (`PaxosState`):
- `legacyPrepare` (l.719-761): promise iff `toPrepare.isAfter(latestWitnessedOrLowBound())`, where
  latestWitnessed = max(accepted-or-committed ballot, promised); a promise sets `promised` and `promisedWrite`
  and returns `(true, accepted or Accepted.none, committed)`; a rejection returns
  `(false, Commit(promised, empty), committed)`.
- `legacyPropose` (l.763-805): accept iff `proposal.isSameOrAfter(latestWitnessed)`; on accept store it as
  `accepted` unless its ballot equals `committed`'s (then accept without storing).
- `commitDirect`/`applyCommit` (l.671-717): apply the update to the base table (last write wins by ballot),
  then `Snapshot.merge(current, UnsafeSnapshot(commit))`: the committed register takes the later commit and
  `accepted` is cleared unless it is after the new committed.
- The QUORUM read handler returns the table's x and its timestamp.

Ballot comparison (`Commit.isAfter`, `isSameOrAfter`, `isBefore`, `latest`, `Snapshot.latestWitnessedOrLowBound`)
is kept as in the source, on `uuidTimestamp()`, which here is the (counter, node) pair encoded
lexicographically as `counter*4 + node + 1`.

## 4. Method map

| Cassandra | Port (`CassandraPaxosV1`) |
|---|---|
| `StorageProxy.cas` / CQL layer | `Node.clientCas` (calls `legacyCas`, maps result/exception to `CasResult`/`Failed`) |
| `StorageProxy.legacyCas` (+ its `updateProposer` lambda) | `Node.legacyCas` |
| `StorageProxy.doPaxos` | `Node.doPaxos` |
| `StorageProxy.beginAndRepairPaxos` | `Node.beginAndRepairPaxos` |
| `PaxosBallotAndContention` | record `PaxosBallotAndContention` |
| `StorageProxy.sendCommit` | `Node.sendCommit` |
| `StorageProxy.preparePaxos` | `Node.preparePaxos` |
| `StorageProxy.proposePaxos` | `Node.proposePaxos` |
| `StorageProxy.commitPaxos` | `Node.commitPaxos` |
| `StorageProxy.commitPaxosLocal` | folded into `commitPaxos` (send to self) |
| `StorageProxy.read` / `readWithPaxos` / CQL layer | `Node.clientRead` |
| `StorageProxy.legacyReadWithPaxos` | `Node.legacyReadWithPaxos` |
| `StorageProxy.readOne` -> `read` -> `readRegular` | `Node.readOne` |
| `StorageProxy.fetchRows` (+ ReadCallback/DataResolver) | `Node.fetchRows`, class `ReadCallback` |
| `ReadCommandVerbHandler` | `Node.readCommandVerbHandlerDoVerb` |
| `BallotGenerator.Global.nextBallot` / `Default.next` | `Node.nextBallot` |
| `ClientState.getTimestampForPaxos` | `Node.getTimestampForPaxos` |
| `AbstractPaxosCallback` (+ `await`) | class `AbstractPaxosCallback`, `Node.await` |
| `PrepareCallback` (`onResponse`, `replicasMissingMostRecentCommit`) | class `PrepareCallback` |
| `ProposeCallback` (`onResponse`, `isSuccessful`, `isFullyRefused`, `getAcceptCount`) | class `ProposeCallback` |
| `AbstractWriteResponseHandler` of `commitPaxos` | class `WriteResponseHandler` |
| `AbstractPaxosVerbHandler.doVerb` | `Node.doVerb` |
| `PrepareVerbHandler.processMessage` / `doPrepare` | `Node.prepareVerbHandlerProcessMessage` / `Node.doPrepare` |
| `ProposeVerbHandler.processMessage` / `doPropose` | `Node.proposeVerbHandlerProcessMessage` / `Node.doPropose` |
| `PaxosCommit.RequestHandler.doVerb` / `execute` | `Node.paxosCommitRequestHandlerDoVerb` |
| `PaxosState.legacyPrepare` | `Node.legacyPrepare` |
| `PaxosState.legacyPropose` | `Node.legacyPropose` |
| `PaxosState.commitDirect` | `Node.commitDirect` |
| `PaxosState.applyCommit` (+ `Keyspace.apply(mutation)`) | `Node.applyCommit` (+ `Node.applyMutation`) |
| `currentUpdater.compareAndSet` | `Node.compareAndSet` |
| `PaxosState.Snapshot` (`latestWitnessedOrLowBound`, `merge`, `removeExpired`) / `UnsafeSnapshot` | classes `Snapshot` / `UnsafeSnapshot` |
| `Commit` (+ `Proposal`, `Accepted`, `Agreed`, `Committed`; `newPrepare`, `newProposal`, `emptyCommit`, `isAfter`, `isSameOrAfter`, `isBefore`, `hasBallot`, `hasSameBallot`, `compare`, `latest`, `latestCommitted`, `latestAccepted`) | record `Commit` with the same methods |
| `Ballot` (`none`, `atUnixMicrosWithLsb`, `uuidTimestamp`, `unixMicros`) | record `Ballot` |
| `PartitionUpdate` (`emptyUpdate`, `isEmpty`) | record `PartitionUpdate` |
| `PrepareResponse` | record `PrepareResponse` |
| `CASRequest.appliesTo` / `makeUpdates` | inline in `legacyCas`'s `updateProposer` (`current == expected`; `PartitionUpdate(false, value)`) |
| `Paxos.isLinearizable()` | field `isLinearizable` (`v1` -> true, `v1nolin` -> false) |

## 5. Decisions and simplifications

D1. Attempt budget instead of the contention deadline (StorageProxy l.499 and l.586, `while (nanoTime() < deadline)`).
Both loops test `attempts < A` (A = 3), and `attempts` (reset per client request) is incremented once per fresh
ballot, at the top of the `beginAndRepairPaxos` loop. So every retry the source makes (rejected prepare l.603-610,
in-progress repair l.638-650, missing MRC l.656-664, refused proposal in `doPaxos` l.538-542) consumes one
attempt, and a 4th ballot is never generated: the loop exits and `CasWriteTimeoutException` is thrown ->
`Failed`. The random sleeps (l.541, 608, 647) are dropped.

D2. Ballots (Ballot.java; BallotGenerator l.40, 45; ClientState `getTimestampForPaxos`, src-4.1 ClientState.java l.285-299).
`Ballot(counter, node)` with `uuidTimestamp() = counter*4 + node + 1` (lexicographic), `unixMicros() = counter`,
`Ballot.none() = (0, -1)`. `lastTimestampMicros` of the coordinator is the highest counter it has seen: its own
ballots and every ballot in a prepare response it consumed (promises: inProgress and MRC; rejections: the
promised ballot and the MRC). `getTimestampForPaxos(min)` keeps the source's arithmetic without the clock
(`current = min`, `tstamp = last >= current ? last+1 : current`), so a fresh ballot is
`(1 + highest seen, own id)`, as INTERFACE.md requires. The source's `minTimestampMicrosToUse`
(`1 + summary.mostRecentInProgressCommit.ballot.unixMicros()`, l.592) is kept; it never exceeds `last+1`.
Changed: the source does not record `tstamp` when it equals `min` (CASSANDRA-11991) and relies on the random
lsb for uniqueness; here that would repeat a ballot, so `tstamp` is always recorded.
Not counted as seen: ballots a replica receives in requests on its replica side, and read timestamps (the source
does not advance `ClientState` from them either).

D3. Local requests (StorageProxy l.700, 741, 797, `if (replica.isSelf())`). The source runs the local prepare,
propose and commit on a local stage, asynchronously. The port sends the request to the process itself; the
process serves it from its message loop. This keeps every interleaving of the local handler with remote
requests, at the cost of extra events.

D4. Late responses (PrepareCallback.onResponse l.63-86, ProposeCallback.onResponse l.60-74). In the source, a
response that arrives after the latch reached zero still runs `onResponse` (it may add to `commitsByReplica`,
raise `mostRecentCommit` or `accepts`) concurrently with the coordinator reading the callback. The port reads
the callback exactly when the latch reaches zero and never consumes late responses (they carry an old `id`, which
no receive guard matches). This removes the race on the callback's fields.

D5. Quorum read (StorageProxy l.352 `readOne(readCommand, QUORUM)`, l.1918 `fetchRows(group.queries, QUORUM)`;
readRegular/fetchRows l.1967-2134 used only as the interface). The port sends one `ReadReq` to all three replicas
and resolves the first two responses by write timestamp (last write wins; the absent row is x = 0 with
`Ballot.none()`). Dropped: the choice of 2 contacts by the snitch plus speculative retry (replaced by "first two
of three to arrive"), digest requests, and blocking read repair. With no failures, read repair only rewrites
values with their original timestamps; it is a real message exchange that the port omits.

D6. Base table (PaxosState.applyCommit l.693-701; Commit.newProposal `withTimestamp`). The register x carries the
ballot of the commit that wrote it as its timestamp; `applyMutation` writes only if the commit's ballot is later
(cell reconciliation by timestamp). An empty update writes nothing. Equal timestamps cannot occur with
distinct values (one ballot per proposal). The truncation check (l.693) is always true.

D7. Replica state (PaxosState l.719-805; Snapshot l.158-268). In memory, one `Snapshot` per replica initialised to
`(none, none, null, Committed.none)`. `removeExpired` is the identity (no time, no TTL, no gc_grace).
`ballotTracker().getLowBound()` is `Ballot.none()` (only v2 repair raises it). The `while(true)` +
`compareAndSet` loops are kept; on one thread the CAS always succeeds. `commitDirect`'s `tryGetUnsafe(...)`
(l.674) is treated as always present (the in-memory state is always loaded), so every commit merges into the
snapshot. The `AcceptedWithTTL`/`CommittedWithTTL` wrapping (l.685, l.766; `paxos_state_purging` defaults to
`legacy`, src-4.1 DatabaseDescriptor l.927-928) is dropped; `latestCommitted`/`latestAccepted` therefore keep the
first argument on equal ballots (no `lastDeleted`).

D8. `replicasMissingMostRecentCommit` (PrepareCallback l.88-107). The `legacy` purging branch (an MRC older than
the paxos TTL counts as nobody missing it) needs wall-clock time; with counter ballots no MRC is ever too old, so
the branch is dropped. `commitsByReplica` is ordered by replica index (the order only fixes the order of the
`sendCommit` sends to different replicas).

D9. Commit consistency. CAS: `consistencyForCommit` and the replay consistency are both QUORUM (INTERFACE.md:
"QUORUM for the commit"). SERIAL read: replay QUORUM, own commit ANY (l.1897-1904), never used since the
proposal is empty. `commitPaxos` at QUORUM sends to all three and awaits 2 acks; hints, `checkHintOverload`,
the not-alive branch (l.786-815) are dropped (no failures). `sendCommit` and ANY commits register no callback
(`id = -1`); the replica still answers (as `PaxosCommit.RequestHandler` does), and the answer is never consumed.

D10. Client-visible outcomes. Applied CAS -> `CasResult(1, value)`; condition false -> `CasResult(0, x read)`;
read -> `Value(x)` from the final QUORUM read. `Failed` for: the attempt budget exhausted
(`CasWriteTimeoutException`, from either loop; for reads re-thrown as `ReadTimeoutException` as in l.1909-1912),
and `CasWriteUnknownResultException` (l.765-766: client proposal accepted by some but fewer than a quorum),
which the source reports to the client at once without retry; INTERFACE.md's `Failed` is "unknown outcome",
which this is.

D11. CAS condition (CASRequest.appliesTo). `IF x = expected` with the absent row counting as 0 (INTERFACE.md);
real CQL would treat an absent row as `null != 0`.

D12. Variants (Config.java l.889-930; StorageProxy l.1891-1894). The only difference: for a SERIAL read, `v1`
proposes an empty update with the promised ballot, `v1nolin` proposes nothing (`ballot -> null`, `doPaxos`
returns after `beginAndRepairPaxos`). A failing CAS proposes an empty update in both variants (legacyCas
l.357-362 does not consult `Paxos.isLinearizable()`), despite the variant's name ("or_rejected_writes").

D13. Dropped as not behavioural here: metrics, tracing, the partition denylist, the out-of-range-token checks
(AbstractPaxosVerbHandler l.50-65, PaxosCommit l.316), triggers, schema validation, `validateForCas`, the
`synchronized`/`maybeLoad` of PaxosState, system-table persistence (`savePaxos*`), RPC timeouts (no failures:
`await` never times out, so `WriteTimeoutException` from `callback.await` cannot occur).

D14. Program shell. `programName() = "cassandra_paxos_v1"`, `size() = "q=<q>,variant=<variant>"`, roles
`n0,n1,n2,c0,c1`, visible `c0,c1`. Main constructs the five `MpThread`s first (so every process knows every
`Tid` before any process runs, without a setup message), starts them in the order of the interface, and awaits
the clients and then the replicas (a replica never ends, so main ends blocked).

## 6. Compile and run

Compile (inside this directory):

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit status 0, no output (no warnings, no errors).

Run (inside this directory):

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_paxos_v1.CassandraPaxosV1:impl,1,v1,fifo' must 0 1 report - 600 </dev/null

Last output line: `max_porf_count: 0`. The run hit the 600 s timeout and did not finish (exit status 0): `ran: 18396 iterations`, `blocked: 18396`, `completed: 0` (every execution ends with the replicas blocked on receive, as intended), `graphs: 0 distinct complete, 18396 distinct blocked; stack not exhausted`, `stop_reason: timeout`, `max_graph_events: 123`. It ran in counting mode (export `-`, so no roles), which is why `max_visible_events` is 0.
