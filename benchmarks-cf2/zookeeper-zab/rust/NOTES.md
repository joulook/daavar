# bench_zookeeper_zab: notes on the Rust port of ZooKeeper 3.9 Zab

Sources: `plan/case-studies/zookeeper/zab-src` (cited below as `File.java:line`), the interface
`plan/case-studies/zookeeper/INTERFACE.md`, and TraceForge's Must API (`lib.rs`, `program.rs`).
The code is `src/main.rs`.

## 1. Processes

| thread | role | what it runs |
|---|---|---|
| `n0` | leader of epoch 1 (fixed) | `LeaderPeer::lead`: Leader.lead, one LearnerHandler per follower, the leader's request pipeline |
| `n1`, `n2` | followers | `FollowerPeer::follow_leader`: Follower.followLeader, Learner, FollowerZooKeeperServer |
| `c0`, `c1` | clients (visible) | the client of INTERFACE.md, verbatim; `ci` talks to `ni` |
| `init` | invisible | sends every server `Setup { nodes, clients }` tagged `SETUP_TAG` |
| main | | spawns n0, n1, n2, c0, c1, init (in that order), joins the clients, init, then the servers; it sends and receives nothing |

Each server is one thread. The leader's helper threads in ZooKeeper (one LearnerHandler and one
sender thread per follower, the processor threads) are folded into n0's single event loop: each
packet that n0 receives is handled to completion before the next receive. Section 5 lists the
resulting decisions.

## 2. Messages

Client messages, verbatim: `Req::Propose(u64)` (client -> its server, untagged, `send_msg`) and
`Rep::Committed(u64)` (server -> its client, `send_msg`).

Server messages: `Wire { from, pkt: Packet }`, sent with `send_tagged_msg(dst, seq, ..)`. `seq` is
the per-channel sequence number, so each ordered pair of servers is an in-order channel (the
TCP connection between learner and leader). The receiver takes, from each sender, only the packet
whose tag is that channel's next sequence number (`Net::recv`). Under `--model fifo` this filter
removes no choice. Under `bag` it gives the TCP order that the source assumes.

| Packet | source constant | fields kept |
|---|---|---|
| `FollowerInfo` | FOLLOWERINFO = 11 | zxid = makeZxid(acceptedEpoch, 0), sid, protocol version 0x10000 (Learner.java:490-504) |
| `LeaderInfo` | LEADERINFO = 17 | zxid = makeZxid(newEpoch, 0), version (LearnerHandler.java:535-538) |
| `AckEpoch` | ACKEPOCH = 18 | zxid = lastLoggedZxid, epoch = currentEpoch or -1 (Learner.java:510-528) |
| `Diff`, `Trunc` | DIFF = 13, TRUNC = 14 | zxid |
| `Snap` | SNAP = 15 | zxid, and the serialized data tree in the same packet (LearnerHandler.java:568-585) |
| `NewLeader` | NEWLEADER = 10 | zxid = makeZxid(newEpoch, 0); the quorum-verifier bytes are dropped |
| `UpToDate` | UPTODATE = 12 | none (zxid -1) |
| `Proposal` | PROPOSAL = 2 | zxid, value (the txn body) |
| `Ack` | ACK = 3 | zxid |
| `Commit` | COMMIT = 4 | zxid |
| `Request` | REQUEST = 1 | value (sessionId, cxid, type and auth are dropped) |
| `Start` | INTERFACE.md | none; n0 -> n2 when late = 1 |

Dropped: PING, REVALIDATE, SYNC, INFORM, OBSERVERINFO, COMMITANDACTIVATE, INFORMANDACTIVATE.

## 3. The protocol, role by role and packet by packet

zxid = (epoch << 32) | counter (ZxidUtils). Every server starts with acceptedEpoch = currentEpoch
= 0, an empty log, an empty data tree, and lastProcessedZxid = 0. Every server knows that n0 is the
leader, which replaces the election (QuorumPeer.run's LOOKING case). The quorum is 2 of 3
(QuorumMaj), and the leader counts itself.

### Leader n0 (Leader.lead 632-765, LearnerHandler.run 463-718)

1. Discovery. `leaderStateSummary = (currentEpoch 0, lastProcessedZxid 0)` (648). The leader calls
   `getEpochToPropose(self, 0)` (655): `epoch` goes from -1 to 1 and `connectingFollowers = {n0}`.
2. FOLLOWERINFO from f (the first packet of a LearnerHandler, 473-480). Its handler calls
   `getEpochToPropose(f, epochOf(zxid))` (520-525, 1462-1495). The first follower completes the
   quorum {n0, f}: the epoch is decided (acceptedEpoch = 1). lead() continues: `zk.setZxid(1<<32)`,
   `lastProposed`, the NEWLEADER proposal, and `waitForEpochAck(self)` (657-710). Then every handler
   that waits in getEpochToPropose sends `LEADERINFO(1<<32, 0x10000)` (535-540). A FOLLOWERINFO that
   arrives after the decision gets LEADERINFO at once.
3. ACKEPOCH from f (541-551). The handler calls `waitForEpochAck(f, ss)` (1508-1542): if the
   election has not finished, it checks `isMoreRecentThan` and adds f to `electingFollowers`. When
   the quorum {n0, f} is reached, lead() continues: `setCurrentEpoch(1)` and
   `waitForNewLeaderAck(self)` (711-716). Then every handler waiting in waitForEpochAck syncs its
   follower.
4. Sync (552-610). `syncFollower(peerLastZxid)` (780-938) chooses DIFF, TRUNC or SNAP from the
   committedLog and the in-memory txn log. `queueCommittedProposals` (951-1042) queues a PROPOSAL
   and a COMMIT for each committed txn that the peer lacks. `startForwarding` (1360-1393) queues the
   outstanding proposals above the sync point and adds f to `forwardingFollowers`. Send order: SNAP
   first if needed (it is written to the socket directly, 568-585), then the queue, then NEWLEADER
   (600-606).
5. NEWLEADER ACK from f (617-628). `waitForNewLeaderAck(f, zxid)` (1613-1653). When the acks reach
   the required count (the quorum when late = 1; all of n0, n1, n2 when late = 0, see D3), lead()
   runs `startZkServer` (741, 1563-1603): lastCommitted = 1<<32, lastProcessedZxid = 1<<32, the
   server runs. Every handler waiting here (in waitForNewLeaderAck or waitForStartup) sends UPTODATE
   (646-653). A follower that acks NEWLEADER after the start gets UPTODATE at once.
6. Broadcast loop (655-718):
   - ACK -> `processAck` (1047-1115): ignored if its counter is 0 (the ack sent after UPTODATE), if
     nothing is outstanding, or if the proposal is already committed. Otherwise the ack is added and
     `tryToCommit` (963-1036) runs: the proposal must not follow an outstanding one and must have a
     quorum. Then `commit` sends COMMIT to every forwarding follower (1212-1219), and the
     CommitProcessor / FinalRequestProcessor apply the txn (data tree, committedLog) and reply
     `Committed(v)` to c0. After n0's first delivery with late = 1, n0 sends `Start` to n2.
   - REQUEST -> `submitLearnerRequest` -> the request pipeline below.
7. Client requests (once the server runs). `Propose(v)` from c0 and REQUEST(v) from a follower both
   go through PrepRequestProcessor, which assigns zxid = ++hzxid, then ProposalRequestProcessor
   (`Leader.propose`, 1288-1328): the proposal is added to outstandingProposals, and PROPOSAL goes to
   every forwarding follower. Then SyncRequestProcessor appends to the leader's log, and
   AckRequestProcessor (AckRequestProcessor.java:47-50) calls `processAck(self)`.

### Follower n1 / n2 (Follower.followLeader 71-149, Learner, FollowerZooKeeperServer)

0. If late = 1, n2 first waits for `Start` from n0.
1. `registerWithLeader` (Learner.java:486-540): send `FOLLOWERINFO(makeZxid(acceptedEpoch,0), sid)`,
   receive LEADERINFO. newEpoch > acceptedEpoch, so the follower sends
   `ACKEPOCH(lastLoggedZxid = lastProcessedZxid, currentEpoch)` and sets acceptedEpoch = 1. If the
   epochs are equal it sends -1. The NEWLEADER-first branch (pre-Zab-1.0) is kept and never taken.
2. `syncWithLeader` (570-848). The first packet is DIFF (no snapshot needed, so writeToTxnLog =
   true), SNAP (deserialize the tree, set lastProcessedZxid; writeToTxnLog = false) or TRUNC
   (truncate the log, reload; writeToTxnLog = false). Then, until UPTODATE:
   - PROPOSAL: `enforceContinuousProposal`, then push onto packetsNotLogged.
   - COMMIT: if writeToTxnLog is false, apply the head of packetsNotLogged when it matches.
     Otherwise push onto packetsCommitted.
   - NEWLEADER: writeToTxnLog = true. Each packetsCommitted entry that matches the head of
     packetsNotLogged is appended to the log and applied, without an ACK (769-800).
     currentEpoch = 1. Send `ACK(newLeaderZxid)` (819).
   - UPTODATE: leave the loop.
   After the loop: send `ACK(makeZxid(newEpoch,0))` (825-826) and start the server. `logRequest`
   each packetsNotLogged entry (pendingTxns, append, ACK), then `commit` each packetsCommitted entry
   (838-848).
3. Broadcast (`processPacket`, Follower.java:156-245):
   - PROPOSAL: lastQueued (the mismatch is only a warning), then `logRequest`
     (FollowerZooKeeperServer.java:79-84). It pushes onto pendingTxns. SyncRequestProcessor appends
     to the log, and SendAckRequestProcessor (SendAckRequestProcessor.java:40-52) sends ACK(zxid).
   - COMMIT: `FollowerZooKeeperServer.commit` (99-113). If the zxid is not the head of pendingTxns,
     the server exits (UNMATCHED_TXN_COMMIT; a panic here). Otherwise the CommitProcessor applies
     the txn, and `Committed(v)` goes to the client if the server has one.
4. Client requests (n1 only, once the server runs). `Propose(v)` from c1 goes to
   FollowerRequestProcessor -> `Learner.request` (Learner.java:251-268) and is sent as REQUEST(v)
   to n0 (FollowerRequestProcessor.java:99-118).

### Delivery

A server delivers (applies, and replies `Committed` to its client) in zxid order. On the leader,
tryToCommit commits only the lowest outstanding zxid. On a follower, the pendingTxns head check
enforces it.

## 4. Method map

| ZooKeeper | this port |
|---|---|
| QuorumPeer.run (LOOKING/LEADING/FOLLOWING) | `server` (fixed roles) |
| QuorumPeer.getLastLoggedZxid (1327-1332) | `db.last_processed` in `register_with_leader` |
| Leader.lead | `LeaderPeer::lead` |
| Leader.getEpochToPropose | `get_epoch_to_propose` (+ `release_epoch_waiters`) |
| Leader.waitForEpochAck | `wait_for_epoch_ack` (+ `release_epoch_ack_waiters`) |
| Leader.waitForNewLeaderAck | `wait_for_new_leader_ack` (+ `release_startup_waiters`) |
| Leader.startZkServer | `start_zk_server` |
| Leader.waitForStartup | the `running` check in `release_startup_waiters` |
| Leader.startForwarding / addForwardingFollower | `start_forwarding` |
| Leader.propose / sendPacket | `propose` |
| Leader.processAck | `process_ack` |
| Leader.tryToCommit / commit | `try_to_commit` |
| Leader.inform | dropped (observers) |
| Leader.Proposal, SyncedLearnerTracker, QuorumMaj | `outstanding: BTreeMap<zxid, (value, ackset)>`, `QUORUM` |
| LearnerHandler.run | `on_packet` (per-handler `HState`), `sync_and_newleader` |
| LearnerHandler.syncFollower | `sync_follower` |
| LearnerHandler.queueCommittedProposals | `queue_committed_proposals` |
| LearnerHandler.queuePacket / sendPackets | `Net::send` |
| Learner.registerWithLeader | `register_with_leader` |
| Learner.syncWithLeader | `sync_with_leader` |
| Learner.enforceContinuousProposal | `enforce_continuous_proposal` |
| Learner.request / FollowerRequestProcessor | the request branch of `follow_leader` |
| Follower.followLeader | `follow_leader` |
| Follower.processPacket | `process_packet` |
| FollowerZooKeeperServer.logRequest (+ SyncRequestProcessor, SendAckRequestProcessor) | `log_request` |
| FollowerZooKeeperServer.commit / pendingTxns | `commit` / `pending_txns` |
| FollowerZooKeeperServer.appendRequest | `db.append` in the NEWLEADER case |
| LeaderRequestProcessor, PrepRequestProcessor (getNextZxid) | the request branch of `lead`, `hzxid += 1` in `propose` |
| ProposalRequestProcessor, SyncRequestProcessor, AckRequestProcessor (leader) | `propose` (send, append, self-ack) |
| CommitProcessor, FinalRequestProcessor (apply, reply) | `Db::process_txn` + `Net::reply` (`apply` on followers) |
| ZKDatabase.addCommittedProposal | `Db::add_committed_proposal` |
| ZKDatabase.commit | nothing (the log is in memory) |
| ZKDatabase.truncateLog | `Db::truncate_log` |
| ZKDatabase.deserializeSnapshot / clear | `Db::deserialize_snapshot` / `Db::clear` |
| ZKDatabase.getProposalsFromTxnLog | `Db::proposals_from_txn_log` |
| ZKDatabase.getDataTreeLastProcessedZxid / setlastProcessedZxid | `Db::last_processed` |
| StateSummary.isMoreRecentThan | inline in `wait_for_epoch_ack` |
| ZxidUtils.makeZxid / getEpochFromZxid | `make_zxid`, `epoch_of`, `counter_of` |
| TxnLog / TxnLogEntry / Request | `Db::log`, `Txn = (zxid, value)` |

## 5. Decisions and simplifications

- **D1, the election.** Replaced by the fixed vote for n0 (INTERFACE.md). The epoch is still
  computed by getEpochToPropose from the FOLLOWERINFO epochs, which gives 1, as the interface states.
- **D2, time.** No ticks, timeouts, pings or sessions. Every wait with a timeout in the source
  (getEpochToPropose 1479-1491, waitForEpochAck 1530-1539, waitForNewLeaderAck 1641-1650,
  waitForStartup 1396-1399) becomes "the handler stays in its state until the condition holds". The
  timeout branches cannot fire.
- **D3, late = 0.** INTERFACE.md requires that "all three servers join the leader before any
  proposal is made". The source starts the server on a quorum of NEWLEADER acks (1637), which does
  not guarantee this. So with late = 0, `quorum_formed` needs the NEWLEADER acks of all of n0, n1 and
  n2 (`new_leader_needed = N`). With late = 1 it is the source's quorum.
- **D4, how a late n2 is synchronised: SNAP, not DIFF.** INTERFACE.md expects a DIFF. But the code,
  run against the in-memory database, chooses SNAP. n2 sends peerLastZxid = 0. When n2 joins
  (after n0's first delivery), the committedLog is non-empty with minCommittedLog = 0x100000001.
  So `lastProcessedZxid == peerLastZxid` fails (849), TRUNC is excluded (`isPeerNewEpochZxid`, 858),
  and the committedLog range test fails (868). The txn-log path (874) calls
  `getProposalsFromTxnLog(0)`, which returns an empty iterator because the first logged txn's zxid
  is above 0 (ZKDatabase.java:399-404). needSnap stays true (920), and SNAP is sent (563-585). I
  followed the code, as the prompt says ("or TRUNC/SNAP when the code would choose them"). The
  queued outstanding proposals and NEWLEADER follow the snapshot. The DIFF, TRUNC and
  queueCommittedProposals paths are ported and reachable in general, but with late = 0 only the
  empty DIFF (849-857) is used, and with late = 1 only SNAP. Visible behaviour does not depend on
  this choice, because n2 has no client.
- **D5, the in-memory txn log.** `getProposalsFromTxnLog(start)` returns the whole log when the log
  is empty or its first entry is at or below `start`, and nothing otherwise. `sizeLimit` is
  unlimited: there is no snapshot file, so the size is 0, and 0 means unlimited (365-376, 386-414).
  `snapshotSizeFactor` keeps its default 0.33, so txn-log sync is enabled (355-363). commitLogCount
  (500) is never reached.
- **D6, the leader is one thread.** In the source, the handler threads, the sender threads, the
  lead() thread and the processor threads interleave freely. Here each received packet is handled
  atomically. Consequences:
  - When a quorum condition becomes true, the lead() continuation runs first, then the waiting
    handlers in sid order (n1 before n2). The relative order of sends to different followers is not
    observable.
  - No live PROPOSAL/COMMIT can slip between a handler's startForwarding and its NEWLEADER (source
    925 vs. 605). NEWLEADER always directly follows the synced proposals.
  - The leader's own log append and self-ACK happen inside `propose`. In the source,
    SyncRequestProcessor may lag, so a txn could be committed by the two followers' acks before
    the leader's self-ack. Here the self-ack is immediate. This only lets a commit happen earlier,
    and the leader can still delay its receive of a follower's ACK.
  - Commit and apply on the leader are one step (CommitProcessor and FinalRequestProcessor run
    synchronously after tryToCommit). So `toBeApplied` (993-995, 1364-1373) is always empty when
    startForwarding reads it, and it is not modelled.
- **D7, client requests before start.** A `Propose` that reaches a server before that server runs
  (n0 before startZkServer; n1 before UPTODATE) stays in the mailbox. The server takes client
  requests only once it runs, as a real server accepts client connections only after
  `zk.startup()` (Learner.java:827, Leader.java:1592).
- **D8, receiving from two kinds of sender.** A server thread has to receive both its client's
  `Req` and the servers' `Wire`. Must's receives are typed (`expect_msg` panics on a mismatch), so a
  server with a client decides each step with `nondet()` whether its next receive takes the client's
  next `Propose` or the next server packet. The choice is offered only while the server runs, still
  has client requests, and a server packet is certain to arrive. When no packet is certain, the
  client request is taken without a choice. This "certain to arrive" test is a liveness guard that
  stops the nondet from selecting a receive that blocks forever:
  - Leader: some follower still owes a packet for what n0 sent it (FOLLOWERINFO from a follower that
    joins at once or was sent `Start`, ACKEPOCH after LEADERINFO, the NEWLEADER ACK, the ACK after
    UPTODATE, one ACK per PROPOSAL except a synced proposal whose COMMIT precedes NEWLEADER), or n1
    has forwarded fewer than e REQUESTs.
  - Follower n1: fewer COMMITs seen than e (c0's transactions) plus the requests n1 has forwarded.
  The guard uses e, the instance parameter. It is model knowledge, not protocol state.
  Check: a temporary build (not shipped) in which every server returns once it has delivered all
  2e transactions and is owed nothing.
  - No accounting assertion fired for e = 1 (late 0/1, fifo/bag) or for e = 2, late = 1, fifo.
  - TraceForge still counted a few executions as blocked: fifo/bag e = 1 gave 40/40 (late 0) and
    1/1 (late 1) out of 5540/22040 and 119/473; e = 2, late = 1 gave 122 of 252964.
  - I traced the one blocked execution of fifo e = 1 late = 1. Its log shows n1, n2, both clients
    and n0 all reaching their end, so I take these counts to be Must's own bookkeeping and not a
    receive that the guard sent wrong. I have not shown this for every such execution, so it
    stays an open point.
- **D9, in-order channels.** ZooKeeper's quorum connections are TCP. The per-channel sequence-number
  tag (section 2) keeps this order under `bag`. The client <-> server messages are untagged
  (`send_msg`, verbatim client), so under `bag` they may be reordered.
- **D10, replies.** Every applied transaction is replied to the server's client, including txns
  applied during a sync (INTERFACE.md: "for every transaction the server delivers"). In ZooKeeper,
  a txn applied in syncWithLeader is not replied through FinalRequestProcessor. With these
  instances, n1 never applies during a sync, and n2 has no client.
- **D11, errors.** The source's error exits are panics: "Follower is ahead", "Epoch of leader is
  lower", UNMATCHED_TXN_COMMIT, a non-continuous DIFF proposal, an unexpected first packet, and an
  unexpected packet in a handler state. None of them is reachable here. The source's warnings
  (lastQueued gap, "Committing ... but next proposal is", processAck of a future zxid) are silent
  no-ops.
- **D12, packet contents.** A request carries only its value. sessionId, cxid, type, auth, digests,
  the quorum-verifier bytes and LearnerInfo's config version are dropped. A snapshot is the list of
  applied (zxid, value) pairs.
- **D13, the processing of an ACK in AwaitStartup / AwaitEpoch / AwaitEpochAckQuorum.** In the
  source the handler does not read the socket while it is blocked there. The follower sends nothing
  in those states, so n0 does not mask those senders. A packet in a wrong state panics.
- **D14, the third argument** (`fifo|bag`) of `<e> <late> [<fifo|bag>]` is accepted and ignored. The
  delivery model comes from `--model`. `spec` and `imp` both return this implementation.
- **D15, `TRUNC` reload.** `truncateLog` is ported as: clear, cut the log at the zxid, replay the log
  into the tree and the committedLog (the in-memory `loadDataBase`). It is unreachable here.

## 6. Build and run

Compile, inside this directory:

```
cargo build --release -j 2
```

Result: `Finished `release` profile [optimized]` (no warnings, no errors).

The one run, inside this directory:

```
timeout 600 target/release/bench_zookeeper_zab count impl --model fifo -- 1 0
```

The run exits 0. Its result line is `execs=0 blocked=5540`: every execution ends with the
servers blocked on receive, as the interface intends. The measurement block (`--- measurements`
... `cache_hits:`) follows that line, so the literal last output line is `cache_hits: ` (an empty
value). From the block: `graphs_blocked: 5540`, `max_graph_events: 104`, `explore_ms: 994`.
