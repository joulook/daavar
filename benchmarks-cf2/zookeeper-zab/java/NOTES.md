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
installs a tree without per-transaction replies. Neither case carries a transaction for n1 in these
instances (D6), so only n2 (no client) can apply that way.

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

Result: RESULT_COMPILE

Run:

```
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.zookeeper_zab.ZookeeperZab:impl,1,0,fifo' must 0 1 report - 600 </dev/null
```

RESULT_RUN
