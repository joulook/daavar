# ZooKeeper 3.9 Zab, Java port against JMC's message-passing API

File: `java/bench/zookeeper_zab/ZookeeperZab.java`, one source file, class
`bench.zookeeper_zab.ZookeeperZab implements org.mpi_sws.jmc.mp.launch.MpProgram`, built from the
argument string `impl,<e>,<late>,<fifo|bag>` (`fifo` = `CommunicationModel.P2P`, `bag` = `ASYN`).

Sources: `S/` = `plan/case-studies/zookeeper/zab-src/` (the cut; paths below are relative to it).
Resolved against `src-3.9/zookeeper-server/src/main/java` where the cut calls out of itself
(`ZooKeeperServer.processTxn` / `getNextZxid` / `loadData`, `FileTxnLog.FileTxnIterator`,
`QuorumPeer.getLastLoggedZxid`). Every changed line in the Java file carries `// PORT:`.

## 1. Processes

| role | process body | sends | receives |
|---|---|---|---|
| `n0` | `QuorumPeer.run` → `Leader.lead()` then `Leader.serve()` (receive loop) | LEADERINFO, DIFF/TRUNC/SNAP, PROPOSAL, COMMIT, NEWLEADER, UPTODATE to followers; `Committed` to `c0`; `Start` to `n2` (late = 1) | FOLLOWERINFO, ACKEPOCH, ACK, REQUEST from followers; `Propose` from `c0` |
| `n1`, `n2` | `QuorumPeer.run` → `Follower.followLeader()` | FOLLOWERINFO, ACKEPOCH, ACK, REQUEST to `n0`; `Committed` to `c1` (n1 only) | packets from `n0`; `Propose` from `c1` (n1); `Start` (n2, late = 1) |
| `c0`, `c1` | `client(i, e, server)`, the Rust client of INTERFACE.md verbatim | `Propose{v(i,j)}`, v(i,j) = 1 + i + 2j | `Committed{value}` until each own value seen once |
| main | spawns n0, n1, n2, c0, c1 in this order, awaits c0, c1, n0, n1, n2 | nothing | nothing |

Servers never return; an execution ends with every server blocked on a receive, so every execution
is counted blocked by the strategy. `toString()` (printed by the launcher as `observed:`) reports the
executions started and the client completions; 2 completions per execution means no execution
blocked before both clients had seen their values.

Packets between servers are one record `QuorumPacket(int sid, int type, long zxid, PacketData data)`
with ZooKeeper's type codes; `sid` is the sender (the socket it arrives on). Payload records:
`NoData`, `LearnerInfo(serverid, protocolVersion, configVersion)`, `IntData(int)` (LEADERINFO's
protocol version 0x10000, ACKEPOCH's current epoch), `TxnLogEntry(TxnHeader(clientId, cxid, zxid,
type), int txn)` (PROPOSAL), `RequestData(sessionId, cxid, type, value)` (REQUEST), `Snapshot(List<TxnLogEntry>)`
(SNAP). Every message record carries `@Tag` with its own name.

## 2. The protocol, role by role, packet by packet

Initial state of every server: acceptedEpoch = currentEpoch = 0, empty txn log, empty data tree,
lastProcessedZxid = 0 (INTERFACE.md: as if the election had just elected n0).

### Leader n0

1. `lead()` (`S/quorum/Leader.java` 632): `zk.loadData()`, `leaderStateSummary = (0, 0)`,
   `getEpochToPropose(0, 0)`: `epoch := 0 + 1 = 1`, connectingFollowers = {0}, no quorum → waits.
2. On FOLLOWERINFO(zxid = makeZxid(acceptedEpoch, 0), LearnerInfo) from follower s, a new
   LearnerHandler runs (`LearnerHandler.run`, 463): `getEpochToPropose(s, 0)`; with {0, s} a quorum,
   waitingForNewEpoch := false, acceptedEpoch := 1, the waiters are notified. The handler writes
   LEADERINFO(makeZxid(1, 0), 0x10000) directly to the socket (538) and reads ACKEPOCH (542).
3. ACKEPOCH(lastLoggedZxid, currentEpoch) → `waitForEpochAck(s, ss)` (1508); a quorum {0, s} sets
   electionFinished. lead() continues: currentEpoch := 1, `waitForNewLeaderAck(0, makeZxid(1,0))`.
4. The handler runs `syncFollower(peerLastZxid)` (780), which queues DIFF / TRUNC / committed
   proposals + COMMITs, then `startForwarding` (1360) queues committed-not-applied and outstanding
   proposals and adds the handler to forwardingFollowers. If a snapshot is needed, SNAP(lastProcessedZxid,
   data tree) is written directly (569). NEWLEADER(makeZxid(1,0)) is queued (605); the sender thread
   starts (610) and sends the queue in order.
5. ACK(makeZxid(1,0)) (the NEWLEADER ack) → `waitForNewLeaderAck(s, zxid)` (1613); with {0, s} a
   quorum, quorumFormed. lead() runs `startZkServer()` (1563): lastCommitted := makeZxid(1,0),
   `zk.startup()`, lastProcessedZxid := makeZxid(1,0); the leader serves. The handler passes
   `waitForStartup()` and queues UPTODATE(-1) (653), then loops on packets (657).
6. Broadcast. A request (client `Propose` → `LeaderRequestProcessor` → `PrepRequestProcessor`, or a
   learner's REQUEST → `submitLearnerRequest` → `PrepRequestProcessor`) gets zxid = ++hzxid
   (makeZxid(1, 1), makeZxid(1, 2), ...). `ProposalRequestProcessor.processRequest`: the
   CommitProcessor queues it, `propose` sends PROPOSAL(zxid, TxnLogEntry) to every forwarding
   follower and records it in outstandingProposals, then the SyncRequestProcessor appends it to the
   txn log and `AckRequestProcessor` calls `processAck(0, zxid)`.
7. ACK(zxid) from s → `processAck(s, zxid)` (1047) → `tryToCommit` (963): not committed while
   zxid - 1 is outstanding; with a quorum of acks: lastCommitted := zxid, COMMIT(zxid) to every
   forwarding follower, then `commitProcessor.commit` → `ToBeAppliedRequestProcessor` →
   `FinalRequestProcessor` → `processTxn` applies it to the data tree, appends it to committedLog,
   and sends `Committed{value}` to c0. With late = 1 the first application is followed by `Start` to n2.
8. ACKs with a zero counter (the ACK after UPTODATE) and ACKs of committed zxids are ignored.

### Follower n1, n2

1. (late = 1, n2 only) receive `Start`.
2. `followLeader()` (`S/quorum/Follower.java` 71) → `registerWithLeader(FOLLOWERINFO)`
   (`S/quorum/Learner.java` 486): send FOLLOWERINFO(makeZxid(0,0), LearnerInfo(myid, 0x10000, 0)),
   read LEADERINFO; newEpoch 1 > acceptedEpoch 0 → acceptedEpoch := 1, send ACKEPOCH(lastLoggedZxid,
   currentEpoch = 0).
3. `syncWithLeader(makeZxid(1,0))` (570): the first packet is DIFF (no snapshot needed, txns go to
   the log), SNAP (data tree := the leader's, lastProcessedZxid := SNAP's zxid) or TRUNC. Then until
   UPTODATE: PROPOSAL → packetsNotLogged (with `enforceContinuousProposal`); COMMIT → applied at once
   if the txns are not to be logged (before NEWLEADER after a SNAP/TRUNC), else packetsCommitted;
   NEWLEADER → from now on log; the committed ones in packetsCommitted are appended and applied
   (ZOOKEEPER-3911); currentEpoch := 1; send ACK(makeZxid(1,0)). UPTODATE ends the loop.
4. Send ACK(makeZxid(1,0)), `zk.startup()`, `logRequest` every packet still in packetsNotLogged
   (each is appended and ACKed), `commit` every zxid in packetsCommitted.
5. Broadcast loop: PROPOSAL → `processPacket` → `logRequest`: pendingTxns += request, append to the
   log, send ACK(zxid). COMMIT → `FollowerZooKeeperServer.commit(zxid)`: the head of pendingTxns must
   have this zxid (else `requestSystemExit`), then CommitProcessor → FinalRequestProcessor →
   `processTxn` applies it and (n1) sends `Committed{value}` to c1. A client `Propose` →
   `FollowerRequestProcessor`: the CommitProcessor queues it as a local write, `Learner.request`
   sends REQUEST(-1, RequestData) to n0.

## 3. Method map

| ZooKeeper (file:line in `S/`) | port |
|---|---|
| `QuorumPeer.run` (quorum/QuorumPeer.java 1427; FOLLOWING 1551, LEADING 1564) | `QuorumPeer.run` |
| `QuorumPeer.getLastLoggedZxid`, epoch accessors | `QuorumPeer.getLastLoggedZxid`, `get/setAcceptedEpoch`, `get/setCurrentEpoch` |
| `QuorumPeer.makeFollower`, `makeLeader` | same names |
| `Leader.lead` (quorum/Leader.java 632) | `Leader.lead`, `lead2`, `lead3`, `lead4` (cut at its three waits) |
| `Leader.getEpochToPropose` (1462) | `Leader.getEpochToPropose(sid, lastAcceptedEpoch, k)` |
| `Leader.waitForEpochAck` (1508) | `Leader.waitForEpochAck(id, ss, k)` |
| `Leader.waitForNewLeaderAck` (1613) | `Leader.waitForNewLeaderAck(sid, zxid, k)` |
| `Leader.startZkServer` (1563) | `Leader.startZkServer` |
| `Leader.waitForStartup` (1396) | `Leader.waitForStartup(k)` |
| `Leader.propose` (1288) | `Leader.propose` |
| `Leader.processAck` (1047) | `Leader.processAck` |
| `Leader.tryToCommit` (963) | `Leader.tryToCommit` |
| `Leader.commit` (1212), `inform` (1238), `sendPacket` (1188) | same names |
| `Leader.startForwarding` (1360) | `Leader.startForwarding` |
| `Leader.addLearnerHandler`, `removeLearnerHandler`, `addForwardingFollower` | same names |
| `Leader.Proposal`, `Leader.PureRequestProposal` | `Proposal`, `PureRequestProposal` |
| `Leader.ToBeAppliedRequestProcessor` | `Leader.ToBeAppliedRequestProcessor` |
| `Leader.LearnerCnxAcceptor` | the `handler == null` branch of `Leader.serve` |
| `LearnerHandler.run` (quorum/LearnerHandler.java 463) | `LearnerHandler.run`, `run2` .. `run7`, `runLoop` |
| `LearnerHandler.syncFollower` (780) | `LearnerHandler.syncFollower` |
| `LearnerHandler.queueCommittedProposals` (951) | `LearnerHandler.queueCommittedProposals` |
| `LearnerHandler.startSendingPackets` (745), `sendPackets` (329) | `startSendingPackets`, `enqueue`, `writeRecord` |
| `LearnerHandler.queuePacket`, `queueOpPacket`, `shutdown` | same names |
| `LearnerMaster` (quorum/LearnerMaster.java) | `Leader` itself (the only LearnerMaster) |
| `Learner.registerWithLeader` (quorum/Learner.java 486) | `Learner.registerWithLeader` |
| `Learner.enforceContinuousProposal` (542) | `Learner.enforceContinuousProposal` |
| `Learner.syncWithLeader` (570) | `Learner.syncWithLeader` |
| `Learner.writePacket`, `readPacket`, `request`, `findLeader`, `connectToLeader` | same names |
| `Learner.PacketInFlight` | `PacketInFlight` |
| `Follower.followLeader` (quorum/Follower.java 71) | `Follower.followLeader` |
| `Follower.processPacket` (156) | `Follower.processPacket` |
| `FollowerZooKeeperServer.logRequest` (79), `commit` (99), `appendRequest`, `pendingTxns`, `setupRequestProcessors` | same names |
| `LeaderZooKeeperServer.setupRequestProcessors`, `submitLearnerRequest` | same names |
| `ZooKeeperServer.processTxn(Request)`, `getNextZxid`, `getZxid`, `setZxid`, `loadData`, `startup`, `submitRequest` | same names in `ZooKeeperServer` |
| `LeaderRequestProcessor`, `PrepRequestProcessor.pRequest` / `pRequest2Txn` | same names |
| `ProposalRequestProcessor.processRequest` | same |
| `SyncRequestProcessor.processRequest` / `run` / `flush` | `SyncRequestProcessor.processRequest`, `flush` |
| `AckRequestProcessor`, `SendAckRequestProcessor` | same |
| `CommitProcessor.processRequest`, `commit`, `run` (the commit branch) | `CommitProcessor.processRequest`, `commit`, `processWrite` |
| `FollowerRequestProcessor.run` | `FollowerRequestProcessor.processRequest` |
| `FinalRequestProcessor.processRequest` / `applyRequest` | same |
| `ZKDatabase` (ZKDatabase.java): `committedLog`, `addCommittedProposal` (322), `getCommittedLog`, `getmin/maxCommittedLog`, `isTxnLogSyncEnabled`, `calculateTxnLogSizeLimit` (365), `getProposalsFromTxnLog` (386), `truncateLog` (611), `getDataTreeLastProcessedZxid`, `setlastProcessedZxid`, `processTxn`, `append`, `commit`, `clear`, `loadDataBase`, `serializeSnapshot`, `deserializeSnapshot` | same names |
| `DataTree.processTxn`, `lastProcessedZxid` | `DataTree` |
| `StateSummary`, `SyncedLearnerTracker`, `QuorumMaj`, `ZxidUtils`, `Request`, `TxnLogEntry`, `TxnHeader` | same names |

## 4. Decisions and simplifications

**Fix round 1 (section 6) revises D1, D2, D5, D6, D12 and D13; where they disagree, section 6 holds.**

Each item names the source lines that left the point open or that were replaced.

**D1. Election.** `QuorumPeer.run`'s LOOKING case (quorum/QuorumPeer.java 1427 ff.) is replaced:
n0 is LEADING, n1 and n2 FOLLOWING, from the start. Epoch 1 is not hard-coded: it results from
`getEpochToPropose` with every acceptedEpoch 0 (`lastAcceptedEpoch >= epoch` with epoch = -1).
An exception out of `followLeader` (the source logs it and goes back to LOOKING, QuorumPeer.java 1551-1563) is an
`AssertionError`: with a fixed leader and no failures it cannot occur legitimately.

**D2. Threads become continuations.** A server is one process. The leader's blocking waits
(`connectingFollowers.wait` Leader.java 1486, `electingFollowers.wait` 1534,
`newLeaderProposal.qvAcksetPairs.wait` 1645, `zk.wait(20)` 1399) park the rest of the waiting
method on a monitor; `notifyAll` makes the parked continuations ready. The source leaves the order
of the woken threads to the JVM; the port fixes it: the notifying thread continues to its next
blocking point first, then the woken continuations run in park order, all before the next receive.
Consequence: after the NEWLEADER-ack quorum, lead() (`startZkServer`) runs, then the waiting handlers
queue UPTODATE, before any request is proposed. In ZooKeeper a PROPOSAL can be queued to a quorum
handler between `startZkServer` and that handler's UPTODATE (LearnerHandler.java 646-653), which a
follower handles in `syncWithLeader` (packetsNotLogged); the port does not produce that interleaving
for the handlers that formed the quorum. It still does for a follower whose sync runs later (late
= 1: n2 is synced while proposals are in flight). The 20 ms polling of `waitForStartup` becomes a
notification from `startZkServer`.

**D3. Sockets.** A LearnerHandler reads its socket only at its read points (473, 542, 618, 657). The
leader's receive guard accepts a packet from server s only when s's handler is at a read point (or
has no handler yet: the LearnerCnxAcceptor); otherwise the packet stays in the channel, as unread
socket data. The guard is a pure predicate over a captured immutable set.

**D4. The per-learner sender thread.** Packets queued before `startSendingPackets` (610) are held
and sent in order when it runs; afterwards `queuePacket` sends at once (the sender thread's queue is
FIFO, so order is kept). LEADERINFO (538) and SNAP (569) are written directly, so SNAP precedes the
packets `startForwarding` queued, as on the wire. The snapshot body (`serializeSnapshot`, 583) travels
in SNAP's data instead of as a separate stream; the "BenWasHere" signature is dropped.

**D5. Request-processor threads run inline.** PrepRequestProcessor, SyncRequestProcessor,
CommitProcessor, FollowerRequestProcessor and the LearnerHandler/Learner sender threads are
separate threads with queues in the source; the port runs each hand-off at once, in the source's
order (ProposalRequestProcessor.java 80, 85, 89: CommitProcessor queue, propose, log). Consequences:
(a) the leader logs and acks its own proposal right after sending PROPOSAL, so a commit needs exactly
one follower ACK; in ZooKeeper the leader's own ack may come after two follower ACKs; (b) a follower
appends and ACKs a PROPOSAL before it reads its next packet, so its own ACK always precedes its
COMMIT; (c) a commit is applied (and `Committed` sent) in the step that commits it, right after the
COMMIT packets are sent; (d) SyncRequestProcessor flushes every request at once (no batching).
These remove interleavings in which a server's logging or applying lags behind its packet reads.
I believe they do not change what the clients can observe (every client-visible order still arises
through the order of packet arrivals), but I have not proved it.

**D6. When the leader serves.** Client requests to n0 that arrive before the leader serves
(`self.setZooKeeperServer(zk)` after `startZkServer`, Leader.java 762) are held
in the PrepRequestProcessor's queue (the source refuses the client connection until then). With
late = 0, INTERFACE.md requires that all three servers join before any proposal; the source has no
such rule (a quorum suffices). The port opens the PrepRequestProcessor (`Leader.servingGate`) only
when the leader serves and, for late = 0, both handlers have queued UPTODATE. With late = 1 it opens
when the leader serves (n0 and n1 form the quorum).

**D7. The late joiner.** With late = 1, n2 receives `Start` before `registerWithLeader`; n0 sends it
right after its first application to its data tree (after the `Committed` to c0), from
`LeaderZooKeeperServer.delivered`.

**D8. DIFF / TRUNC / SNAP: the code's choice, which differs from INTERFACE.md for late = 1.**
`syncFollower` runs unchanged on the in-memory database. Results:
- late = 0, a follower whose `syncFollower` runs before `startZkServer`: lastProcessedZxid 0 =
  peerLastZxid 0 → empty DIFF(0).
- late = 0, a follower whose `syncFollower` runs after `startZkServer` (it set lastProcessedZxid to
  makeZxid(1,0), Leader.java 1602): committedLog empty → min = max = makeZxid(1,0) > 0, so
  the txn-log branch; the log is empty, the iterator has no element → SNAP(makeZxid(1,0)) with an
  empty tree.
- late = 1, n2: committedLog holds makeZxid(1,1) .. → peerLastZxid 0 < minCommittedLog, txn-log
  branch; `getProposalsFromTxnLog(0, ·)` returns the empty iterator because the first logged txn
  makeZxid(1,1) > 0 (ZKDatabase.java 399, as `FileTxnIterator` without fast-forward positions at the
  first txn of the oldest log file) → **SNAP**, not the DIFF that INTERFACE.md's late = 1 paragraph
  names. I kept the code's behaviour (the prompt: "DIFF (or TRUNC/SNAP when the code would choose
  them)"); n2 has no client, so the choice is invisible to the clients either way. TRUNC is ported
  but unreachable.

**D9. Disk.** The txn log is an in-memory list per server (one log "file"); `append` adds, `commit`
(fsync) is a no-op, `truncateLog` drops later entries and replays the log, `loadDataBase` replays the
log on an empty tree. The snapshot is the data tree (INTERFACE.md); `takeSnapshot` is a no-op.
`calculateTxnLogSizeLimit` returns 0 (no snapshot file, ZKDatabase.java 365-376), i.e. no size
limit; the txn-log storage size is counted in entries. The data tree is the list of applied
`TxnLogEntry`s plus lastProcessedZxid (one znode, setData).

**D10. Payloads.** byte[] payloads are typed records. NEWLEADER's QuorumVerifier bytes
(LearnerHandler.java 604) and the lastSeenQuorumVerifier version bump in `lead()` (ZOOKEEPER-1783, Leader.java 694)
are dropped, so each Proposal tracks one QuorumMaj (no reconfiguration, README). The txn header's
time is dropped.

**D11. Sessions and cxids.** No sessions; a client's requests carry sessionId = myid + 1 of its
server and a cxid counted by that server from 1 (the interface's `Propose` has no xid). The
CommitProcessor matches a commit against its earliest local write by (sessionId, cxid), as at
CommitProcessor.java 325-327.

**D12. Replies.** INTERFACE.md: every server with a client sends `Committed{value}` for every
transaction it applies, in order, including the other client's. The source replies only to the
request's own session (`if (request.cnxn == null) return;`, FinalRequestProcessor.java 160). The
port sends from `ZooKeeperServer.processTxn` (`delivered`), so applications during
`syncWithLeader` (direct `processTxn` before NEWLEADER, the ZOOKEEPER-3911 path) also count; a SNAP
installs a tree without per-transaction replies. (Corrected in fix round 1: under bag, and after a
learner reconnects, n1 can apply transactions on these paths too; the notification is sent there as
well, which is what the interface requires.)

**D13. Fatal paths.** `ServiceUtils.requestSystemExit` (an unmatched COMMIT in
`FollowerZooKeeperServer.commit`, a failed TRUNC) and unreachable features (reconfig,
COMMITANDACTIVATE, INFORM, sync requests, xid rollover) throw `AssertionError`. An IOException in a
LearnerHandler ends that handler (`shutdown`: removed from learners and forwardingFollowers; later
data from that socket is discarded).

**D14. Dropped (README / INTERFACE.md).** Ticks, timeouts, PING and the leader's ping loop (hence no
shutdown on a lost quorum), sessions, REVALIDATE, SYNC, observers and INFORM, reconfiguration,
throttling, metrics, JMX, logging (kept as no-op calls), SASL/SSL, the ObserverMaster.

**D15. Addresses.** INTERFACE.md: every process knows every address from the start. Main computes
the Tids before spawning: the k-th spawned process is task (main's task id + k), as the MpProgram
contract states (the k-th process main spawns is task k + 1), and checks each handle against it.
Main itself sends and receives nothing. If a strategy numbered tasks otherwise, main fails loudly.

**D16. Packet sender.** `QuorumPacket.sid` names the sending server, standing for the TCP socket;
the leader dispatches on it to the LearnerHandler of that socket.

**D17. bag.** Under `bag` (ASYN) the source's TCP assumption fails and the port does what the code
does: e.g. a COMMIT read before its PROPOSAL logs "Committing ... without seeing txn" and is lost,
an out-of-order COMMIT reaches `requestSystemExit`, an ACK for zxid 2 before zxid 1 does not commit
2 (`tryToCommit` requires zxid - 1 committed and is retried only on a later ACK). Such executions
block or fail; that is the code's behaviour over a non-FIFO network.

**D18. Names in the MpProgram header.** `programName()` = `zookeeper-zab` and `size()` =
`e=<e>,late=<late>`: my choice; the summary header must match the Rust side's, which I could not see.

**D19. Diagnostics.** Main's object counts executions and client completions in two AtomicLongs
(written by main and the clients, never read by a process); they only feed `toString()`.

## 5. Build and run (inside this directory)

Compile:

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Result: no output, exit status 0 (JDK 17.0.18).

Run:

```
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.zookeeper_zab.ZookeeperZab:impl,1,0,fifo' must 0 1 report - 600 </dev/null
```

Result (2026-11-04): stopped on the 600 s timeout, stack not exhausted. 23739 executions, all
blocked (the servers wait on a receive at the end, as intended), 0 complete; `observed:` reports
47478 client completions = 2 x 23739, i.e. in every explored execution both clients saw their
value. 23739 distinct blocked graphs, max_graph_events 73, peak RSS 449896 kB. The `visible`
measurements are 0 because without an export file the launcher runs `must` in counting mode
(no roles). Last output line:

```
max_porf_count: 0
```

The exhaustive enumeration of e = 1, late = 0 does not finish in 600 s; D2/D5 already remove
the pipeline-thread interleavings, so most of the remaining branching is the order of packet
arrivals at n0 from n1, n2 and c0.

## 6. Fix round 1

Input: `plan/case-studies/zookeeper/translit-review/REVIEW.md` (findings 1-8). Line numbers below are
those of `java/bench/zookeeper_zab/ZookeeperZab.java` after this round. Every changed line carries
`// PORT:` as before. Section 5 records the round-0 run; the round-1 run is at the end of this
section.

### 6.1 Scheduler (used by findings 2, 3, 4)

Each server is still one process, but it now schedules its threads instead of running them inline.
`nextEvent` (429): if no internal step is enabled, a blocking receive; otherwise a non-blocking
`Channel.tryRecv`, which reads a pending message or ⊥; on ⊥ one enabled step, chosen by
`Nondet.chooseIndex`, runs. A *step* is one thread's run from a blocking point to its next blocking
point (a socket read, a queue take, a monitor wait, leaving a monitor it notified). Inside a step
nothing else runs, so no preemption inside a source `synchronized` block is introduced. Steps:

| server | step (enabling condition) | lines |
|---|---|---|
| n0 | each woken wait continuation, and the notifier's rest after it leaves the monitor (`Threads.readySteps`, `resume`) | 405-420, 1918, 1946, 1970 |
| n0 | PrepRequestProcessor: take one request, pRequest (zxid, CommitProcessor queue, propose, Sync queue) (request waiting and serving gate open) | 1393-1420 |
| n0, n1, n2 | SyncRequestProcessor: take and append one request (queue non-empty), or flush and pass all appended requests on (queue empty, toFlush non-empty) | 1261-1330 |
| n0, n1, n2 | CommitProcessor: one iteration of its `run` loop (a commit waiting) | 1135-1258 |
| n0 | each LearnerHandler sender thread: write the head of queuedPackets (sending started) | 2216-2224 |
| n1, n2 | FollowerRequestProcessor: take a client request, queue it in the CommitProcessor, send REQUEST (queue non-empty) | 2454-2490 |

The leader's enabled steps are collected in `Leader.enabledSteps` (1658), the follower's in
`FollowerZooKeeperServer.enabledSteps` (2397). The follower's reader thread (readPacket +
processPacket) and the client connection are the receive side of the follower's loop
(`Follower.followLeader`, 2803).

### 6.2 Finding 1 (learner exceptions): fixed

What the source does: `Follower.followLeader` catches every Exception, closes the socket and
returns (`quorum/Follower.java` 123-131); `QuorumPeer.run` then runs `follower.shutdown()` and
`updateServerState()` (LOOKING) (`quorum/QuorumPeer.java` 1556-1562); `Learner.shutdown` closes the
socket and calls `zk.shutdown(syncMode == SNAP)` (`quorum/Learner.java` 910-925), which
fast-forwards the database over the logged txns, or clears it after an unfinished SNAP sync
(`ZooKeeperServer.java` 943-970 in the full tree). The next LOOKING round elects n0 again.

Port:
- `QuorumPeer.run`/`runLoop` (932-976) is now the source's state loop: LOOKING → (PORT) the fixed
  vote → FOLLOWING → `makeFollower`, `followLeader`, `follower.shutdown()`, `updateServerState()`
  (978) → LOOKING → … . The election is still replaced by the interface's fixed vote; with no failure
  and no time, n0 never leaves LEADING (its only way out is the ping loop's lost-quorum check, which
  is time-driven and dropped), so every later round has the same outcome. PORT: the `finally` block
  runs on normal return only, since it sends and must not run while JMC unwinds a blocked process.
- `followLeader` (2803) catches the exceptions the port can raise on the reader thread:
  `ZabException` (the source's IOException/Exception: a discontinuous proposal in
  `enforceContinuousProposal`, an EOF, a bad epoch), `NullPointerException` (a sync COMMIT before
  any PROPOSAL, `Learner.java` 671-672), `ZabStateException` (`addCommittedProposal`'s
  IllegalStateException) and `ClassCastException`; then `closeSocket()` (2835-2840).
- Connection identity: `QuorumPacket` carries `cnx` (228); `connectToLeader` takes a new connection
  number (2598); `readPacket` (2552) and the follower's loop read only the current connection, so a
  packet of an older connection never enters a new session; the leader keys its LearnerHandlers by
  (sid, cnx) (`socketKey`, `serve` 1627) and gives a new connection a new handler.
- The end of a connection is a packet `CLOSE` (291), sent after the connection's data
  (`Learner.closeSocket` 2562, `LearnerHandler.shutdown` 2347-2356). Read by the leader it is the
  handler's EOF/IOException path (2030: `shutdown()`, removed from forwardingFollowers); read by a
  follower it is an EOFException (`ZabException`).
- `Learner.shutdown` (2574) and `ZooKeeperServer.shutdown(fullyShutDown)` (1062) as in the source:
  fast-forward (`ZKDatabase.fastForwardDataBase`, 778) or clear. The fast-forward applies logged
  txns to the data tree and so (interface adapter, D12) notifies the client.
- Process-exit branches are separated from caught exceptions: `requestSystemExit` and an exception
  in a request-processor thread (`critical`, 2416; the source's critical-thread exit) throw
  `SystemExit` (361), which `QuorumPeer.run` (932-939) turns into a normal end of the process
  (the server's JVM is gone). No AssertionError is used for a reachable source path any more.
- The reviewer's schedule (e = 2, late = 1, bag, two PROPOSALs reordered before UPTODATE) now
  closes n1's session, n1 rejoins n0 on a new connection and is resynchronised (DIFF from its last
  logged zxid), as the source does.

### 6.3 Finding 2 (commit hand-off): fixed

`CommitProcessor` (1135-1258) now has the source's queuedRequests, queuedWriteRequests,
per-session pendingRequests and committedRequests; `commit` and `processRequest` only queue
(as `CommitProcessor.java` 590-617); `run` is one iteration of the source's loop (200-380:
move queued requests to pendingRequests until a commit is waiting, then one commit,
maxCommitBatchSize = 1, matched against the earliest local write, then drain the session queue).
It is a separate step. On the leader `tryToCommit` (1728-1755) adds the proposal to toBeApplied,
sends COMMIT and hands the request to the CommitProcessor; the proposal stays in toBeApplied until
the CommitProcessor step applies it (`ToBeAppliedRequestProcessor`, 1794), so `startForwarding`
can again forward PROPOSAL + COMMIT of committed-unapplied proposals to a syncing learner. The
application and the `Committed` notification happen in that step, not in the committing step.
Collapsed: an iteration that only moves queued requests into pendingRequests (no commit waiting) is
not a step of its own: nothing reads pendingRequests except a commit-handling iteration, which does
the same moves first, so the states such an iteration produces are not observable by any other
step. No worker pool (there are no reads; the source processes writes on the CommitProcessor
thread too).

### 6.4 Finding 3 (logging, ACKs, forwarding, sender queues): fixed except where listed

Now separate steps (6.1): the leader's PrepRequestProcessor; the SyncRequestProcessor of every
server (append, and flush when its queue is empty: batching as in the source with maxBatchSize
1000 and no flush delay), so the leader's own ACK can come after both followers' ACKs and a
follower can read and apply a COMMIT before its own ACK leaves; the FollowerRequestProcessor; the
CommitProcessor; the LearnerHandler sender threads (LEADERINFO and SNAP are still written directly
by the handler thread, as in the source). Interleavings still lost, and why the clients cannot see
them:

1. **The client reply leaves when the CommitProcessor step applies the txn.** In the source
   `FinalRequestProcessor` hands the reply to the client connection, whose NIO thread writes it
   later. Lost: a server receiving other messages between applying a txn and writing its reply.
   Clients cannot see it: the replies of one server leave in application order either way, a
   reply's later write is matched by a later delivery, and a client sends all its Proposes before
   it reads any reply, so no client action depends on when a reply was written. (A summary that
   records cross-process causal order could see it: a later write has more causal predecessors.)
2. **Receiving a client Propose submits it at once** to the server's first processor queue
   (LeaderRequestProcessor → Prep queue on n0, FollowerRequestProcessor queue on n1). The source's
   NIO and RequestThrottler threads add more FIFO hand-offs before that queue. Lost: delays between
   the receive and the queue insertion. Clients cannot see it: the next thread (Prep / FRP) is a
   separate step already, so every position of the request's processing relative to other events
   is still reachable; only the time at which an invisible queue was entered differs.
3. **A step is atomic.** In particular `LearnerHandler.run4` (syncFollower + startForwarding +
   the SNAP write + NEWLEADER queueing) is one step; in the source the leader can propose between
   `startForwarding` (the handler is then a forwarding follower) and queueing NEWLEADER, so a
   PROPOSAL can precede NEWLEADER on that connection. And the snapshot is copied in that step,
   whereas the source serializes it while other threads commit ("fuzzy" snapshot). Clients cannot
   see either: with late = 0 no request is processed before the barrier (both followers have
   acknowledged UPTODATE, finding 5), so no proposal exists during these syncs; with late = 1 they
   concern n2, which has no client; n2's ACKs for outstanding proposals are unchanged (startForwarding
   queues every outstanding proposal in either case, and a proposal seen before NEWLEADER is logged
   and ACKed after UPTODATE as one seen after it); a COMMIT applied directly before NEWLEADER is of a
   proposal the leader has already committed, so a missing n2 ACK for it has no effect. After a
   reconnection (finding 1), n1 (which has a client) can be resynchronised by SNAP (e.g. when its
   last zxid is makeZxid(1,0) and the leader has committed txns: the txn-log refusal of D8) while
   proposals are in flight. There the source's fuzzy snapshot can contain a txn that is also queued
   as PROPOSAL + COMMIT; the port's atomic copy cannot. I have not shown this case invisible to c1;
   it needs a reconnection, which needs bag.
4. **The follower's Learner writes are synchronous** (asyncSending is false by default, so this is
   the source's behaviour, not a loss).

### 6.5 Finding 4 (fixed priority of continuations): fixed except where listed

Woken continuations are now enabled steps in any order with every other step and with the
receives; the notifier's own rest after its notifyAll is a step too (`threads.resume`, 1918, 1946,
1970), so after the NEWLEADER-ack quorum the leader can start its server and propose before a
quorum handler queues UPTODATE (the follower then buffers the PROPOSAL in packetsNotLogged, as in
the source). Still lost: preemption inside a source `synchronized` method and inside a step
(6.4 item 3); the review asks for exactly this boundary. A thread that returns from a wait without
waiting (e.g. `getEpochToPropose` after `waitingForNewEpoch` became false) continues in the same
step: it did not block, and other threads' steps can still precede or follow that step as a whole.
`waitForStartup`'s 20 ms poll is a notification from `startZkServer` plus a re-check, which reaches
every state the poll reaches.

### 6.6 Finding 5 (late = 0 barrier): fixed

The barrier now opens when both followers have *received* UPTODATE: a LearnerHandler records
`upToDateAcked` when its packet loop reads the first zero-counter ACK (2176), which the learner sends
right after UPTODATE (`Learner.java` 825-826). `servingGate` (1694-1725) opens when the leader serves
and both followers have such a handler, and then stays open (a later reconnection does not close
it again). The flag only observes; `processAck` still ignores the ACK as in the source.

Disagreement (minor): the review warns that under bag the NEWLEADER ACK and the UPTODATE ACK, which
carry the same zxid, could be confused. They cannot: the learner sends the UPTODATE ACK only after
reading UPTODATE, which the leader sends only after its handler has read the NEWLEADER ACK, so the
NEWLEADER ACK has been received before the UPTODATE ACK is sent, under every delivery model. The
NEWLEADER ACK is read at the handler's NEWLEADER read point; the first zero-counter ACK read in the
packet loop is therefore the UPTODATE ACK.

### 6.7 Findings 6, 7, 8

6: no change (INTERFACE.md now admits DIFF, TRUNC or SNAP as the source selects). 7: no change to
the code; the reachability claim in D12 is corrected. 8: no further change; the numeric domain is
int (values 1 + i + 2j fit for e ≤ 2^30 - 1).

### 6.8 Build and run, round 1

Compile (same command): no output, exit status 0. (The run below used the build made before a
comment-only fix of the class header; the recompile after that fix also printed nothing and exited 0.)

Before the run I ran three 200-iteration `mp-random` smoke runs (impl,1,0,fifo; impl,1,1,fifo;
impl,2,1,bag; report directory deleted afterwards) to check that the port does not crash: all
executions ended blocked; 400, 400 and 358 client completions of 400 (bag stalls, as the source
does over a non-FIFO network).

Run (same command, once; output in `run-round1.log`): stopped on the 600 s timeout, stack not
exhausted. 16879 executions, all blocked (servers wait at the end), 0 complete; `observed:` 33758
client completions = 2 x 16879, so in every explored execution both clients saw their value.
16879 distinct blocked graphs, max_graph_events 130 (round 0: 73; the added events are the
non-blocking receives and Nondet choices of the scheduler), peak RSS 434888 kB. Last output line:

```
max_porf_count: 0
```
