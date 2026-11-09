# Zab (ZooKeeper 3.9) on JMC's message-passing API — NOTES

Source: `java/bench/zookeeper_zab_opus/ZookeeperZabOpus.java` (one file, class
`bench.zookeeper_zab_opus.ZookeeperZabOpus`, an `MpProgram`, constructed from `impl,<e>,<late>,<fifo|bag>`).
Line references are to `zab-src/` (paths relative to it).

## Processes

| role | body | notes |
|---|---|---|
| `n0` | `LeaderServer.run` | QuorumPeer LEADING: `Leader.lead`, both `LearnerHandler`s and the leader's request pipeline, in one process |
| `n1`, `n2` | `FollowerServer.run` | QuorumPeer FOLLOWING: `Follower.followLeader`, `Learner`, `FollowerZooKeeperServer` |
| `c0`, `c1` | `client(i)` | exactly the interface's client: send `Propose(v(i,j))`, j = 0..e-1, then receive `Committed` until each own value has been seen once |

Main spawns n0, n1, n2, c0, c1 (in this order, with these roles), awaits the clients, then the servers;
it sends and receives nothing. Servers never end: an execution ends with them blocked on receive.
The process ids are computed before spawning as `main's task id + 1 + k` for the k-th spawn and checked
against the spawn handles afterwards (they are needed as addresses before every process exists).
Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN`; every send and receive uses it.
`programName()` = `zookeeper_zab`, `size()` = `e=<e>,late=<late>`, visible roles = `c0`, `c1`.

## Messages (records, all `@Tag`ged; the receiver never sees the sender, so learner->leader packets carry the sid)

| record | source packet | direction | fields |
|---|---|---|---|
| `Propose` | client write | client -> its server | `value` (interface) |
| `Committed` | client reply | server -> its client | `value` (interface) |
| `Start` | (interface only) | n0 -> n2, late = 1 | — |
| `FollowerInfo` | FOLLOWERINFO | learner -> n0 | sid, zxid = makeZxid(acceptedEpoch, 0) |
| `LeaderInfo` | LEADERINFO | n0 -> learner | zxid = makeZxid(newEpoch, 0) (protocol version 0x10000 implied) |
| `AckEpoch` | ACKEPOCH | learner -> n0 | sid, epoch (currentEpoch or -1), zxid = lastLoggedZxid |
| `Diff`, `Trunc` | DIFF, TRUNC | n0 -> learner | zxid |
| `Snap` | SNAP + serialized snapshot | n0 -> learner | zxid, data tree (list of applied values) |
| `NewLeader` | NEWLEADER | n0 -> learner | zxid = makeZxid(newEpoch, 0) (no quorum-verifier bytes) |
| `UpToDate` | UPTODATE | n0 -> learner | — |
| `Proposal` | PROPOSAL | n0 -> learner | zxid, value (the txn) |
| `Ack` | ACK | learner -> n0 | sid, zxid |
| `Commit` | COMMIT | n0 -> learner | zxid |
| `Request` | REQUEST | follower -> n0 | sid, value |

Dropped: PING, REVALIDATE, SYNC, INFORM / OBSERVERINFO (observers), COMMITANDACTIVATE / INFORMANDACTIVATE (reconfig),
sessions, marker packets.

## The protocol, role by role, packet by packet

### Leader n0
1. Start = `Leader.lead` (Leader.java:632): `leaderStateSummary = (currentEpoch 0, lastProcessedZxid 0)`;
   the acceptor is "started" (receiving begins); `getEpochToPropose(0, acceptedEpoch 0)` (1462): epoch = 1,
   connectingFollowers = {0}, no quorum -> the lead thread waits.
2. `FollowerInfo(sid, z)` (LearnerHandler.run, 462): `getEpochToPropose(sid, epoch(z))`; the first follower
   completes the quorum {0, sid} -> `waitingForNewEpoch = false`, acceptedEpoch = 1. The handler (and every
   handler waiting there) sends `LeaderInfo(makeZxid(1,0))` and waits for ACKEPOCH. The lead thread resumes:
   zxid = makeZxid(1,0), lastProposed, NEWLEADER zxid, `waitForEpochAck(0, leaderSS)` (adds itself).
3. `AckEpoch(sid, ep, z)`: `waitForEpochAck(sid, (ep, z))` (1508): returns at once if `electionFinished`;
   follower ahead of the leader -> IOException -> the handler dies (`H_DEAD`); otherwise adds sid and, with
   {0, sid}, sets `electionFinished`. Then `syncFollower(z)` (779) and on the wire, in this order:
   `Snap` (only if needSnap; written directly to the socket, LearnerHandler.java:569) , the queued packets
   (DIFF/TRUNC, committed PROPOSAL+COMMIT pairs, then from `startForwarding` (1360) the outstanding
   proposals above the follower's zxid), then `NewLeader` (604). The follower is now a forwarding follower.
4. First `Ack(sid, z)` after NEWLEADER: `waitForNewLeaderAck(sid, z)` (1613). The lead thread already called it
   with its own id, so the first follower ack forms the quorum -> `startZkServer` (1563): lastCommitted =
   makeZxid(1,0), lastProcessedZxid = makeZxid(1,0), running. Every handler past its NEWLEADER ack
   then passes `waitForStartup` and sends `UpToDate` (653).
5. Broadcast: `Propose(v)` from c0, or `Request(sid, v)` from a running handler (711, submitLearnerRequest):
   PrepRequestProcessor assigns zxid = ++hzxid; `Leader.propose` (1288) records the outstanding proposal,
   `sendPacket(Proposal)` to every forwarding follower (1188); SyncRequestProcessor logs it, then
   AckRequestProcessor calls `processAck(0, zxid)` (AckRequestProcessor.java:47).
6. `Ack(sid, z)` from a running handler: `processAck` (1047): ignore epoch-zxids (the ack after UPTODATE),
   empty outstanding, already committed, unknown zxid; else add sid and `tryToCommit` (963): not if zxid-1 is
   outstanding, not without a quorum (2 of 3, QuorumMaj.containsQuorum); else remove, `commit(zxid)` (1212):
   lastCommitted, `Commit` to every forwarding follower; then the CommitProcessor / FinalRequestProcessor
   apply: data tree, lastProcessedZxid, `addCommittedProposal` (ZKDatabase.java:322), `Committed(v)` to c0.
   With late = 1 the first applied transaction is followed by `Start` to n2.

### Followers n1, n2
1. (n2 with late = 1: wait for `Start`.) `registerWithLeader` (Learner.java:486): `FollowerInfo(sid, makeZxid(0,0))`.
2. `LeaderInfo(z)`: new epoch 1 > accepted 0 -> epoch field = currentEpoch (0), accepted = 1 (equal -> -1;
   lower -> IOException -> halt); `AckEpoch(sid, 0, lastLoggedZxid)`; followLeader's epoch check (Follower.java:97).
3. `syncWithLeader` (Learner.java:570), first packet: `Diff` -> snapshotNeeded = false; `Snap` -> db cleared,
   tree and lastProcessedZxid from the packet; `Trunc` -> truncate the in-memory log, reload the tree from it;
   anything else -> exit. writeToTxnLog = !snapshotNeeded.
4. Loop until UPTODATE: `Proposal` -> enforceContinuousProposal (542), packetsNotLogged; `Commit` -> applied at
   once if !writeToTxnLog and it matches the head of packetsNotLogged, else queued in packetsCommitted;
   `NewLeader` -> writeToTxnLog = true, the packetsCommitted are appended to the log and applied (ZOOKEEPER-3911),
   currentEpoch = 1, `Ack(sid, makeZxid(1,0))`; `UpToDate` -> `Ack(sid, makeZxid(1,0))`, zk.startup (the
   follower now takes its client's writes), `logRequest` for each remaining packetsNotLogged (each sends an
   `Ack`), `commit` for each remaining packetsCommitted.
5. Running (`Follower.processPacket`, 156): `Proposal` -> `logRequest` (FollowerZooKeeperServer.java:79):
   pendingTxns, log, `Ack(sid, zxid)` (SendAckRequestProcessor); `Commit` -> `commit` (99): empty pendingTxns ->
   ignored, head mismatch -> requestSystemExit (halt), else apply: tree, lastProcessedZxid, committedLog, and
   `Committed(v)` to the client (n1 -> c1; n2 has none). Client `Propose(v)` -> FollowerRequestProcessor ->
   `Learner.request`: `Request(sid, v)` to n0.

## Method map

| ZooKeeper | port |
|---|---|
| QuorumPeer.run LEADING / FOLLOWING (fixed roles) | `LeaderServer.run` / `FollowerServer.run` |
| Leader.lead | `LeaderServer.run` + `progress()` (lead-thread states L_EPOCH .. L_RUNNING) |
| Leader.getEpochToPropose | `getEpochToPropose` |
| Leader.waitForEpochAck | `waitForEpochAck` |
| Leader.waitForNewLeaderAck | `waitForNewLeaderAck`, `onNewLeaderAck` |
| Leader.startZkServer | `progress()` L_NLACK -> L_RUNNING |
| Leader.startForwarding | `startForwarding` |
| Leader.propose | `submitRequest` |
| Leader.processAck | `processAck` |
| Leader.tryToCommit | `tryToCommit` |
| Leader.commit | `tryToCommit` (lastCommitted, `sendPacket(Commit)`) |
| Leader.inform | dropped (no observers) |
| Leader.sendPacket | `sendPacket` |
| Leader.Proposal / SyncedLearnerTracker | `outstanding` (zxid -> ack set), `outstandingValue` |
| LearnerHandler.run | `onFollowerInfo`, `sendLeaderInfo`, `onAckEpoch`, `syncAndSendNewLeader`, `onNewLeaderAck`, `progress()` (UPTODATE), `handle` (ACK / REQUEST loop) |
| LearnerHandler.syncFollower | `syncFollower` |
| LearnerHandler.queueCommittedProposals | `queueCommittedProposals` |
| LearnerHandler.queuePacket / sendPackets | direct `Channel.send` (FIFO queue = FIFO channel) |
| Learner.registerWithLeader | `registerWithLeader`, `onLeaderInfo` |
| Learner.syncWithLeader | `onFirstSyncPacket`, `onSyncPacket` |
| Learner.enforceContinuousProposal | `enforceContinuousProposal` |
| Learner.request / FollowerRequestProcessor | the `Propose` branch of `FollowerServer.run` |
| Follower.followLeader | `FollowerServer.run` |
| Follower.processPacket | `processPacket` |
| FollowerZooKeeperServer.logRequest / commit / pendingTxns | `logRequest` / `commit` / `pendingTxns` |
| FollowerZooKeeperServer.appendRequest | `db.append` in the NEWLEADER branch |
| PrepRequestProcessor (assign zxid) | `++hzxid` in `submitRequest` |
| ProposalRequestProcessor, SyncRequestProcessor, AckRequestProcessor | `submitRequest` (propose, `db.append`, `processAck(0, ..)`) |
| SyncRequestProcessor + SendAckRequestProcessor (follower) | `logRequest` |
| CommitProcessor, ToBeAppliedRequestProcessor, FinalRequestProcessor | `apply` (leader and follower) |
| ZKDatabase (committedLog, addCommittedProposal, truncateLog, getDataTreeLastProcessedZxid, getProposalsFromTxnLog, clear) | `Db` |
| StateSummary.isMoreRecentThan | inline in `waitForEpochAck` |
| QuorumMaj.containsQuorum | `containsQuorum` (size > 1) |
| ZxidUtils.makeZxid / getEpochFromZxid | `makeZxid` / `epochOf` |
| ServiceUtils.requestSystemExit, end of followLeader / LearnerHandler | `halt()` (block on a receive that accepts nothing) |

## Decisions and simplifications (with the source that leaves them open)

1. **One process per server; threads become states.** The leader's lead thread, its LearnerHandler threads
   (and sender threads) and the request-processor threads are one process. Each blocking wait
   (`connectingFollowers.wait`, Leader.java:1486; `electingFollowers.wait`, 1534; `qvAcksetPairs.wait`, 1645;
   `waitForStartup`, 1396) becomes a state; after every received message `progress()` resumes every waiter whose
   condition holds (the `notifyAll`s at 1477, 1528, 1639), lead thread first, then handlers by sid. A handler
   reads only the packet its code reads next (receive guards on its state), so the order across followers is
   the order in which messages arrive. The resumption order among threads woken together is not fixed by the
   source; the fixed order is harmless here because the woken handlers only send to distinct followers on
   distinct channels and the lead thread sends nothing.
2. **The lead thread runs eagerly.** Its steps after each wait (Leader.java:655-716, startZkServer 1563) are
   local and run as soon as their condition holds, so the leader's own NEWLEADER ack precedes any follower's
   and the first follower NEWLEADER ack forms the quorum. A slower lead thread only delays UPTODATE.
3. **Handler steps are atomic.** syncFollower + startForwarding + SNAP + NEWLEADER run in one step
   (syncFollower holds the db read lock and startForwarding the leader monitor, LearnerHandler.java:801-927;
   the SNAP serialisation and the NEWLEADER enqueue that follow are outside them, 563-605, so in ZooKeeper a
   proposal could be queued between them). No proposal exists while a follower synchronises with late = 0
   (decision 6), and with late = 1 the joiner n2 has no client, so the difference is invisible.
4. **The leader pipeline is atomic per request.** Assign zxid, propose, log, self-ack in one step; commit and
   apply in one step, so `toBeApplied` is always empty at startForwarding (Leader.java:1363-1372) and the
   leader's own ACK always precedes the followers' (SyncRequestProcessor/AckRequestProcessor are separate
   threads in the source, AckRequestProcessor.java:47).
5. **Follower pipeline.** `logRequest` appends and ACKs at once (SyncRequestProcessor batching and flush,
   SyncRequestProcessor.java:160-210, dropped); `commit` applies at once (CommitProcessor ordering for writes
   is zxid order).
6. **"With late = 0 all three servers join before any proposal" (INTERFACE.md).** ZooKeeper would propose as
   soon as the leader runs (after a NEWLEADER quorum). Decision: with late = 0 the leader's pipeline does not
   take a write (c0's `Propose` or a `REQUEST`) until it runs **and** both followers are forwarding followers
   (have passed startForwarding). Deferred writes stay in the channel (receive guard), not in a queue.
7. **Clients connect when the server runs.** A server takes its client's `Propose` only after zk.startup
   (leader: startZkServer; follower: UPTODATE) — the source only accepts client connections then
   (`self.setZooKeeperServer`, Leader.java:762, Learner.java:743).
8. **Delivery = Committed.** Every apply (`ZooKeeperServer.processTxn`, not in the cut) appends the value to
   the data tree, sets lastProcessedZxid and calls `addCommittedProposal`, and the server with a client sends
   `Committed(v)` for every transaction it applies, including the other client's (the interface; the source's
   FinalRequestProcessor replies only when `request.cnxn != null`, FinalRequestProcessor.java:160). Values
   received in a SNAP are installed, not delivered (the only learner that can receive a non-empty SNAP is n2
   with late = 1, which has no client).
9. **DIFF/TRUNC/SNAP selection** is `syncFollower` verbatim over the in-memory database. txnLogSyncEnabled is
   true (default snapshotSizeFactor 0.33, ZKDatabase.java:92, 355); `calculateTxnLogSizeLimit` (365) has no
   snapshot file, so the limit is 0 = unlimited; the "on-disk" txn log is the in-memory log of logged
   proposals and `getProposalsFromTxnLog` (386) returns nothing when the first logged zxid is above the
   start zxid (or the log is empty). Consequences: a fresh follower (peerLastZxid 0) gets an empty `Diff(0)`
   while the leader's lastProcessedZxid is 0 (before startZkServer), and a `Snap` afterwards
   (lastProcessedZxid = makeZxid(1,0), or the first transactions with late = 1). TRUNC is ported but
   unreachable here (no follower has history).
10. **Snapshot = data tree.** `Snap` carries the leader's list of applied values; `takeSnapshot` and
    `ZKDatabase.commit` (disk) are no-ops; `truncateLog` truncates the in-memory log and rebuilds the tree from
    it (loadDataBase replays the log, ZKDatabase.java:611-623).
11. **Process exit.** `requestSystemExit` (FollowerZooKeeperServer.java:108), an IOException ending
    followLeader / a LearnerHandler, and the NullPointerException in syncWithLeader's COMMIT branch when no
    proposal is pending (Learner.java:672) make the process stop: it blocks forever on a receive that accepts
    nothing (no re-election: the leader is fixed). A dead handler (`H_DEAD`) takes no further packets.
12. **Dropped**: the election (n0 is leader of epoch 1, every server starts with acceptedEpoch =
    currentEpoch = 0, so getEpochToPropose yields epoch 1), observers, reconfiguration and quorum-verifier
    bytes in NEWLEADER, the protocol-version < 0x10000 branch (LearnerHandler.java:528, 600), ticks, timeouts,
    syncLimit checks, PING, sessions, throttling, metrics, JMX, marker packets, the commitLogCount bound (500,
    never reached), XidRollover.
13. **No nondeterministic choice is needed**: every choice the protocol leaves open is an arrival order.
14. **Program name** `zookeeper_zab` (the interface does not name it).

## Compile (inside this directory)

    javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Compiles without errors or warnings.

## Run (inside this directory)

    java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.zookeeper_zab_opus.ZookeeperZabOpus:impl,1,0,fifo' must 0 1 report - 600 </dev/null

Result (exit 0, stack exhausted after 192 s): 13530 iterations, all 13530 blocked (as intended: the servers end
blocked on receive), 0 complete, 13530 distinct blocked graphs, every graph 73 events. Last output line:

    max_porf_count: 0

(An earlier attempt of the same command failed at once, `unexpected task id for process 0: Tid(2:n0)`: the
first version assumed the k-th spawn is task k + 1, while main itself is task 1. Fixed by computing the ids
from `MpProcess.self()`; the run above is the one run of the fixed program.)
