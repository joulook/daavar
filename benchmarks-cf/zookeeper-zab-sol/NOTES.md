# ZooKeeper 3.9 Zab port

The implementation is [ZookeeperZabSol.java](java/bench/zookeeper_zab_sol/ZookeeperZabSol.java), package `bench.zookeeper_zab_sol`. Source citations below are line numbers in the supplied `zab-src`, not another ZooKeeper checkout. `zab-src/README.md` was read first. No network sources or files outside this directory were inspected.

## Interface and processes

`ZookeeperZabSol(String[])` accepts exactly `impl,<e>,<late>,<fifo|bag>`. The program declares `zookeeper_zab`, size `<e>,<late>`, roles `n0,n1,n2,c0,c1`, and only `c0,c1` as visible. FIFO uses `CommunicationModel.P2P`; bag uses `ASYN`. Every send and receive, including client traffic and Start, uses that selected model.

`run` constructs five named `MpThread`s and installs all addresses before starting any thread, then starts servers in index order followed by clients in index order. The addresses are immutable configuration; no protocol state is shared between threads. There is no setup process and main sends/receives nothing. Main awaits clients first, then servers, using `join1`: awaiting a permanently receiving server blocks as documented by the API. There are no daemon threads. Each server ends its finite work in the infinite `Server.loop`, blocked on `Channel.recv`; there is no server shutdown after the clients end.

Client `ci` constructs its values `1+i+2*j`, sends all `e` tagged `Propose(int value)` records to `ni`, then receives tagged `Committed(int value)` records. It counts only values in its own list and stops when the count is `e`, exactly as the supplied client algorithm (no additional duplicate filter). Each apply at n0 or n1 sends its value to the attached client, including the other client's transactions. n2 has no client. These two record tags and single integer fields are the only visible message shapes.

The constructor rejects nonpositive e and invalid late/model/mode. Values use Java int arithmetic, as required by the message field type; mathematical values beyond the signed int range wrap in that representation. No additional upper bound on positive int e is imposed. The specified run uses e=1.

## Roles, packets, and ordering

### Leader n0

All nodes know the fixed leader n0 and proposed epoch 1. Initially accepted/current epochs and logs are zero/empty, representing the state before the new election epoch is accepted. Knowledge of the elected epoch does not bypass Zab discovery. n0 contributes its own member to each of three distinct sets: connecting, epoch acknowledgements, and NEWLEADER acknowledgements. A majority is two distinct server ids.

1. `getEpochToPropose` receives FOLLOWERINFO. A quorum of connecting participants, including n0, releases LEADERINFO `(1<<32, 0x10000)` to connected learners. A later connection gets the same fixed epoch.
2. `waitForEpochAck` records ACKEPOCH with the old current epoch and last logged zxid. A current epoch of -1 does not vote. Before the epoch barrier opens, a follower more recent than the leader's original `(0,0)` state summary is rejected. A quorum opens the epoch barrier and sets the leader's current epoch to 1. Each handler waiting at that barrier can now synchronise. Later learners can synchronise after this already-open barrier without voting again.
3. `syncFollower` selects DIFF, TRUNC, or SNAP and builds the initial outgoing packet queue. `queueCommittedProposals` appends each missing historical PROPOSAL immediately followed by its COMMIT. `startForwarding` additionally appends committed transactions waiting for local application as PROPOSAL/COMMIT pairs, then outstanding proposals in sorted zxid order. The learner becomes a forwarding follower at this point, before its NEWLEADER ACK.
4. NEWLEADER `(1<<32)` is appended **after** that initial synchronisation/forwarding queue. The queue is sent in order. This detail follows the supplied implementation: LearnerHandler.java:554-556 calls syncFollower, :604-605 appends NEWLEADER, and :610 starts the sender. Historical committed proposals are queued ahead of NEWLEADER, not moved after it. They are deferred at the learner until NEWLEADER in the DIFF case. Any interpretation of "behind NEWLEADER" as reversing this outgoing order would disagree with these source lines.
5. `waitForNewLeaderAck` waits for a separate quorum of ACK `(1<<32)` votes. Only then does the leader start its server and set the data tree's last processed marker to `(1<<32)`, without applying or reporting a transaction. Each learner whose NEWLEADER ACK has arrived receives UPTODATE. A learner that ACKs later receives UPTODATE then.
6. With late=0, the interface requires both followers to join before any proposal. The leader therefore delays request preparation until both post-UPTODATE ACKs arrive. With late=1 the usual majority startup is sufficient. This extra gate uses the source's existing second epoch-marker ACK, not an extra protocol message.
7. Propose values from c0 and forwarded REQUEST values enter one preparation FIFO in receive order. Preparation assigns the next zxid, inserts the outstanding proposal, broadcasts PROPOSAL to forwarding followers, and queues its own logging. Logging appends to the leader's in-memory transaction log before its own `processAck(0,zxid)` call.
8. `processAck` ignores epoch-marker ACKs, already committed zxids, and unknown/future proposals. Distinct ids acknowledge individual proposals; acknowledgements are not cumulative. `tryToCommit` requires a quorum and absence of the immediately preceding outstanding proposal. It removes the proposal, adds it to `toBeApplied`, updates lastCommitted, broadcasts COMMIT, then queues local delivery. It does not wait for all three servers.
9. Application updates the data tree and committed cache, sends Committed to c0, and removes the transaction from `toBeApplied`. With late=1, delivery of the first transaction also sends Start to n2, once. The first delivery can occur while further transactions are already proposed or committed.

`inform` is omitted because it only sends to observers, which are excluded. There is no INFORM packet in this participant-only program.

### Followers n1 and n2

n1 starts discovery immediately. n2 does so immediately with late=0; with late=1 it first blocks specifically waiting for Start from n0. In the latter case no FOLLOWERINFO or other packet is sent by n2 before Start is received.

1. `registerWithLeader` sends FOLLOWERINFO `(sid,acceptedEpoch=0,version=0x10000)` and waits for LEADERINFO. It accepts the new epoch and sends ACKEPOCH containing the old current epoch and last logged zxid. An already accepted equal epoch would use currentEpoch=-1, matching the source's non-voting reply. A lower leader epoch is an error.
2. The next leader packet must be DIFF, TRUNC, or SNAP. DIFF preserves the tree and enables writing buffered committed history at NEWLEADER. TRUNC truncates and rebuilds the in-memory history. SNAP installs a copied in-memory data tree and its last processed zxid. TRUNC/SNAP need no disk snapshot in this program.
3. During synchronisation PROPOSALs enter `packetsNotLogged`, with `enforceContinuousProposal`: the first proposal is allowed at any zxid, later proposals must be consecutive unless the epoch increases. There is no sorting on receipt.
4. For DIFF, COMMITs enter `packetsCommitted`; for the initial snapshot/TRUNC mode, matching COMMITs apply buffered proposals directly, as in Learner.java:682-695. At NEWLEADER the DIFF committed prefix is appended and applied before currentEpoch changes and ACK is sent. Historical committed proposals do not receive individual ACKs. Outstanding proposals remain buffered. A missing/mismatched historical head while flushing the commit list is skipped as in Learner.java:787-795.
5. After NEWLEADER, further proposals and commits may arrive before UPTODATE; these stay in the synchronisation buffers. UPTODATE sends the source's second ACK `(1<<32)`, enables client request processing, queues all remaining proposals through `logRequest`, and then processes the buffered COMMITs. It enters ordinary broadcast only after transferring those buffers. Ordinary logging ACKs these proposals.
6. Client proposals received before startup remain in the server's preparation FIFO. Once serving, the follower forwards each one as REQUEST to n0; it does not assign a zxid or deliver on forwarding.
7. During broadcast PROPOSAL appends the transaction to `pendingTxns` immediately and queues its transaction-log append. The log worker appends before sending ACK. The source's broadcast continuity warning is not promoted to a reorder mechanism: `lastQueued` is updated, with no packet sorting or repair.
8. COMMIT with an empty pendingTxns queue is discarded, as in FollowerZooKeeperServer.java:100-103. Otherwise its zxid must equal the head's zxid; a mismatch is a protocol assertion representing the source's fatal unmatched-commit path (:104-108). The matching head is removed and queued for application. The application FIFO applies transactions in delivery order and sends Committed to c1 at n1. A later stray UPTODATE is ignored, corresponding to the source's error log followed by continuation.

### Packet reference

All internal records are also tagged, to keep them encodable by JMC; their tag names are the source packet names. sid identifies the sender where the source obtains it from the TCP handler. There are no fabricated protocol packets to acknowledge local jobs.

| Packet | Direction and fields | Processing/order |
| --- | --- | --- |
| Propose | ci -> ni; int value | Client sends its entire input before receiving replies; server buffers until serving. |
| Committed | ni -> ci; int value | Sent once per server application, in application order. |
| Start | n0 -> n2; no fields | Sent immediately after first n0 delivery, only with late=1. |
| FOLLOWERINFO | follower -> n0; sid, acceptedEpoch, protocol version | First learner packet; contributes to connecting quorum. acceptedEpoch is carried directly instead of encoded in a marker zxid. |
| LEADERINFO | n0 -> follower; marker zxid, version | After connecting quorum; proposes epoch 1. |
| ACKEPOCH | follower -> n0; sid, old currentEpoch, last logged zxid | Before sync; quorum barrier distinct from NEWLEADER ACKs. |
| DIFF | n0 -> follower; last committed zxid | Initial sync control, followed by missing proposal/commit pairs. Its zxid is not assigned to lastProcessed on receipt, following actual Learner.java:587-599. |
| TRUNC | n0 -> follower; truncate zxid | Initial sync control for a divergent non-marker history. |
| SNAP | n0 -> follower; processed zxid, copied List<Txn> tree | Initial sync fallback; data are part of the message rather than an adjacent byte stream. |
| NEWLEADER | n0 -> follower; epoch marker zxid | After the initial queued history; follower persists/applies committed history before ACK. |
| UPTODATE | n0 -> follower; no fields | After NEWLEADER quorum and this learner's ACK; ends sync, followed by second marker ACK. |
| REQUEST | follower -> n0; sid, value | Forwarded only after follower startup; enters leader's request FIFO. |
| PROPOSAL | n0 -> follower; Txn(zxid,value) | Logged then ACKed in broadcast; buffered during sync. |
| ACK | follower -> n0; sid,zxid | First marker ACK is NEWLEADER ACK; second marker ACK is post-UPTODATE; other ACKs vote on transactions. |
| COMMIT | n0 -> follower; zxid | Sent on quorum before leader application; follower matches pending head. |

## Method mapping

Nested implementation names below refer to the single Java file. Source methods that are combined into one state-machine transition are listed explicitly. Data-only accessor mappings are grouped where they have no separate transition.

| Supplied ZooKeeper method/type (source lines) | Port method/state |
| --- | --- |
| QuorumPeer.run, LOOKING/FOLLOWING/LEADING dispatch (:1468-1584); Vote leader/epoch | `run`, `Leader.lead`, `Follower.followLeader`; fixed n0/EPOCH=1, no LOOKING loop |
| QuorumPeer.get/setCurrentEpoch, get/setAcceptedEpoch (:2246-2270) | `Leader.currentEpoch`, `Follower.currentEpoch/acceptedEpoch`, handshake transitions |
| Leader.lead (:632-741) | `Leader.lead`, `getEpochToPropose`, `waitForEpochAck`, `waitForNewLeaderAck` |
| Leader.getEpochToPropose (:1462-1497) | `Leader.getEpochToPropose`, `connecting`, `epochProposed` |
| Leader.waitForEpochAck (:1508-1546) | `Leader.waitForEpochAck`, `epochAcks`, `electionFinished` |
| StateSummary.isMoreRecentThan (:42-44) | Summary guard inside `Leader.waitForEpochAck` (initial leader summary is `(0,0)`) |
| Leader.waitForNewLeaderAck (:1613-1655) | `Leader.waitForNewLeaderAck`, `newAcks`, `quorumFormed` |
| Leader.startZkServer (:1563-1603), waitForStartup (:1392-1400) | `Leader.waitForNewLeaderAck` startup transition, `started`, UPTODATE gate |
| Leader.Proposal/PureRequestProposal (:89-146) | `Outstanding`, immutable `Txn`, `Proposal` record |
| SyncedLearnerTracker.addAck, hasAllQuorums (:33-61); QuorumMaj.containsQuorum (:140-142) | Id sets in `Outstanding` and discovery barriers, size >= 2 |
| LearnerMaster handshake/ack/forwarding/database methods | Implemented directly by corresponding `Leader` methods; no separate facade |
| LearnerHandler.run discovery (:462-552) | `Leader.packet`, `getEpochToPropose`, `waitForEpochAck`, per-follower `Peer` |
| LearnerHandler.syncFollower (:780-940) | `Leader.syncFollower` |
| LearnerHandler.queueCommittedProposals (:951-1042) | `Leader.queueCommittedProposals` |
| Leader.startForwarding (:1360-1390) | `Leader.startForwarding`, `Peer.synced`, `Leader.broadcast` |
| LearnerHandler.run NEWLEADER/send/startup (:596-653) | `Leader.syncFollower`, `waitForNewLeaderAck` |
| LearnerHandler.startSendingPackets / queuePacket / queueOpPacket (used :604-610, :1024-1025) | Ordered `List<MpMessage>` in `syncFollower`, API sends in list order, then `broadcast` |
| LearnerHandler.run ACK/REQUEST packet cases (:675-710) | `Leader.packet`, `Leader.processAck`, `requests` FIFO |
| LeaderRequestProcessor.processRequest (:48-74) | `Leader.packet` + `Leader.processRequest`; only valid integer writes |
| PrepRequestProcessor.pRequest (assigning getNextZxid, e.g. :845) | `Leader.processRequest`, increment of `lastProposed` |
| ProposalRequestProcessor.processRequest (:68-90) | `Leader.processRequest`: outstanding insert + broadcast, then `toLog` enqueue |
| Leader.propose (:1288-1337) | `Leader.processRequest` |
| SyncRequestProcessor.run (:173-217), processRequest (:273), flush (:227-251) | `Server.toLog`, `Server.loop` logging branch, `Database.append`, then `logged` |
| AckRequestProcessor.processRequest (:43-47) | `Leader.logged` calling `processAck(0,zxid)` |
| SendAckRequestProcessor.processRequest/flush (:40-57) | `Follower.logged`, `send` |
| Leader.processAck (:1047-1115) | `Leader.processAck` |
| Leader.tryToCommit (:963-1035) | `Leader.tryToCommit` |
| Leader.sendPacket (:1188-1194) | `Leader.broadcast` |
| Leader.commit (:1212-1219) | `Leader.tryToCommit`: update lastCommitted then `broadcast(new Commit)` |
| Leader.inform (:1238 ff.) | Omitted observer-only send; no participants affected |
| Leader.ToBeAppliedRequestProcessor.processRequest (:1149-1163) | `Leader.toBeApplied`, `Leader.delivered` |
| Follower.followLeader (:71-149) | `Follower.followLeader`, `packet` stages, permanent `Server.loop` |
| Learner.registerWithLeader (:486-540) | `Follower.registerWithLeader`, stage 0 of `Follower.packet` |
| Learner.syncWithLeader (:570-846) | Stages 1/2 of `Follower.packet`, `Follower.syncWithLeader` |
| Learner.enforceContinuousProposal (:542-563) | `Follower.enforceContinuousProposal` |
| Follower.processPacket (:156-244) | `Follower.processPacket` |
| FollowerZooKeeperServer.logRequest (:79-84), appendRequest (:89-91) | `Follower.logRequest`, sync-path `Database.append` |
| FollowerZooKeeperServer.commit (:99-113), pendingTxns (:77) | `Follower.commit`, `pendingTxns` |
| FollowerRequestProcessor.processRequest/run (:64-120, :139 ff.) | `Follower.packet` buffers inputs; `Follower.processRequest` forwards them |
| Learner.request (:251-272) | `Follower.processRequest` emits REQUEST |
| CommitProcessor.processRequest/commit/run (:197 ff., :590-610) | Server request queues plus `toApply` FIFO; no reads/session matching needed |
| FinalRequestProcessor.applyRequest/processRequest (:111, :146 ff.) | `Server.apply`, `Database.apply`, client Committed send |
| ZKDatabase.append/commit (:687-708); TxnLog.append/commit (:57, :100) | `Database.append` is in-memory append + successful flush before ACK |
| ZKDatabase.addCommittedProposal (:322-351) | `Database.addCommittedProposal`, 500-entry cache |
| ZKDatabase.getProposalsFromTxnLog (:386-432) | Retained history selection inside `Leader.syncFollower`, then `queueCommittedProposals` |
| ZKDatabase.truncateLog (:611-624) | `Database.truncateLog` |
| ZKDatabase.deserializeSnapshot (:630-632) | `Database.install`, immutable `Snap.tree` |
| ZKDatabase.getDataTreeLastProcessedZxid (:249-250), setlastProcessedZxid (:482-483), processTxn (:494-495) | `Database.lastProcessed`, `Database.apply`, startup/TRUNC/SNAP assignments |
| ZxidUtils.makeZxid/getEpochFromZxid/getCounterFromZxid (:23-33) | `zxid`, `epoch`, `counter` |
| Request/TxnLogEntry transaction header and body | `Txn(zxid,value)`; `Request(sid,value)` before zxid assignment |

## Decisions and simplifications, with their source basis

1. **Fixed configuration and epoch.** All three servers are voting participants with equal weight. The only quorum is 2/3. QuorumMaj.java:140-142 and SyncedLearnerTracker.java:33-61 support this specialisation. The interface replaces election; QuorumPeer.java:1468-1584 otherwise dispatches state changes and elections. Accepted/current epochs start at zero and become 1 through the handshake; Leader.java:1467-1479 normally computes this next epoch dynamically. The empty initial state is chosen because the interface supplies no prior transactions or durable epochs.
2. **Modern protocol only.** Version 0x10000 is explicit in FOLLOWERINFO/LEADERINFO. LearnerHandler.java:528-550 and Learner.java:503-538 contain both old and modern handshakes; the old pre-Zab-1.0 branch is omitted because the task requests the explicit LEADERINFO/ACKEPOCH handshake.
3. **Three participant processes replace source helper threads.** Handler, sender, prep, sync and commit processors are local state machines/queues inside each server. Leader.java:646-653 and LearnerHandler.java:609-610 create helper threads; ProposalRequestProcessor.java:53-65 and FollowerZooKeeperServer.java:67-74 create asynchronous request processors. The interface fixes one process per server, so no extra processes are spawned for those helpers. Every exchange between distinct servers/clients uses Channel.send/recv; local queues never communicate between processes.
4. **Scheduling choices are explicit.** When local work exists, `Channel.tryRecv` nondeterministically receives an available packet or returns bottom. A received packet is handled in arrival order. On bottom, `Nondet.choose` selects an enabled local preparation, logging or application queue; each queue retains FIFO order. Singleton choices use the API's no-event singleton behaviour. If no local work exists, the server uses blocking recv. Thus local work does not spuriously block, and there is no polling loop that can spin without processing a packet or a job. The source does not order separate processor threads (references in decision 3); these are the remaining scheduling choices. Client code has no extra choices.
5. **Atomic local transitions.** A single proposal preparation/broadcast, log+ACK, commit broadcast, application+client notification, and each initial synchronisation queue construction/sending is one local transition, with send events at the indicated positions. Other processes can run at those events, but that server does not receive another packet in the middle of a transition. Source synchronized methods Leader.propose (:1311-1330), startForwarding (:1360), processAck (:1047), and tryToCommit (:963) motivate preserving those boundaries. We contract lock acquisition, object serialisation, stream flushes, and byte-by-byte snapshot transmission. We do not enumerate interleavings inside those transitions or between separate handler threads on the same leader.
6. **Flush size one.** Database.append succeeds, and in-memory persistence is complete before the log worker invokes the ACK callback. SyncRequestProcessor.java:181, :214-217, :227-251 permits batching/flush policy but always commits before its ACK processor. The port chooses an immediate single-record flush, removes disk failure and fsync, and does not model batch size, log rotation, background snapshots or delays. Application can happen before a non-quorum member has logged locally, just as its pendingTxns is populated before its independent sync worker runs (FollowerZooKeeperServer.java:79-84).
7. **Source queue order takes precedence over an abstract NEWLEADER-first description.** LearnerHandler.java:554-556 calls syncFollower/startForwarding before :604-605 queues NEWLEADER; queueCommittedProposals :1023-1025 queues each historical pair. The port sends control, historical pairs, pending committed pairs, outstanding proposals, NEWLEADER, and only after quorum/startup UPTODATE. For DIFF, Learner.java:648-695 buffers the history and :781-820 persists/applies it before NEWLEADER ACK. NEWLEADER is a barrier, not a transaction delivery.
8. **Known empty baseline guarantees the required late DIFF.** There is a genuine tension between the interface's required DIFF for n2 and literal disk selection in the supplied code. A fresh n2 reports lastZxid=0. After the first real transaction, committedLog.min is `(1<<32)|1`; LearnerHandler.java:874-904 then tries the disk log, but ZKDatabase.java:398-405 rejects an iterator whose first header exceeds startZxid=0, potentially falling back to SNAP. The in-memory replacement retains all applied history and treats the initial empty tree at zxid 0 as a known baseline. It allows queueCommittedProposals to use that history. Its marker handling (LearnerHandler.java:991-998) then selects DIFF and all missing committed pairs. No fake transaction at zero is delivered or logged. This is a deliberate storage-selection change to meet the interface's explicit late-join contract.
9. **History selection and fallback.** The committed cache retains the source default 500 transactions (ZKDatabase.java:94-98, :344-348). Full applied history is retained in memory to serve as the recoverable committed prefix of the transaction log; `toBeApplied` supplies the committed tail not yet in that history, and `outstanding` supplies the uncommitted tail (Leader.java:1363-1382). There is no snapshot-size threshold for memory history: ZKDatabase.java:364-378 defines that threshold in terms of disk snapshot file sizes, which the interface removes. Ahead non-marker zxids use TRUNC; in-cache gaps use the source same-epoch TRUNC logic; unknown/incompatible history falls back to SNAP (LearnerHandler.java:849-925, :999-1012). TRUNC and SNAP are implemented, but are unreachable with these fresh, failure-free, fixed-leader inputs and retained history. They are not claimed as separately tested recovery mechanisms.
10. **Snapshot and truncation representation.** SNAP copies the tree into an immutable message instead of serialising a live snapshot stream and signature (LearnerHandler.java:568-585; Learner.java:600-618). Installing it clears local log/cache, as there is no previous disk to retain. TRUNC removes entries above its zxid and rebuilds cache/tree prefix, replacing disk truncate+reload (ZKDatabase.java:611-624). Their in-memory transitions cannot fail. Snapshot application does not replay client notifications for old snapshot entries; reachable snapshots do not occur at either attached client server.
11. **Initial all-joined gate.** ZooKeeper only requires a startup majority (Leader.java:710-741). The interface with late=0 requires all three to join first. `updateServing` waits for both followers' second marker ACKs, which Learner.java:825-827 already sends after UPTODATE. n1 can forward a REQUEST as soon as it starts; n0 buffers it until this gate opens. No proposal is assigned a zxid before the gate. late=1 uses the source majority condition and Start as specified.
12. **Post-UPTODATE marker ACK.** Source Leader.processAck (:1060-1067) ignores marker ACKs. The port also ignores them for transaction voting, but uses them for the interface's all-joined gate. For a peer's first ACK, the handler still requires the NEWLEADER marker exactly (LearnerHandler.java:617-628; Leader.java:1621-1630). Equal marker ACK values do not merge the two quorum sets or become transaction acknowledgements.
13. **No repair for unordered transport.** ZooKeeper reads packets from ordered TCP streams (LearnerHandler.java:474-476, :656-658; Follower.java:122-124). The harness can substitute bag delivery; the port uses that model directly, with no sequence-number envelope, selective phase buffering, or reordered-proposal sorting. Unexpected phase packets assert. Broadcast discontinuity merely updates lastQueued, as Follower.java:166-172 only warns. Empty pending COMMIT is discarded and mismatched pending COMMIT asserts, preserving FollowerZooKeeperServer.java:100-108. Thus the program does not claim Zab's TCP-dependent guarantees under arbitrary bag reordering. Assertions represent source fatal protocol errors without calling System.exit on the checker JVM. Correctly progressing servers never terminate.
14. **Do not drain later quorum proposals automatically.** Leader.java:1094-1115 only retries subsequent proposals after committing a reconfiguration. The port omits that reconfiguration-only loop. It retries a proposal when another ACK for that proposal arrives. In particular bag reordering can leave a later proposal with a quorum but no subsequent ACK to retry it after its predecessor commits. This source behaviour is preserved; there is no invented cumulative-ACK rule or unconditional commit scan.
15. **Transaction/value model and reply replacement.** PrepRequestProcessor.java:829-857 assigns zxids to concrete writes; FinalRequestProcessor.java:111 applies them and :146 ff. processes responses. Per the README/interface, a write here is just `(zxid,int value)`, and the data tree is an append-only list of those transactions. Request session/cxid/opcode/auth/digest fields are unnecessary because each client value is unique. Every applied value is reported to the attached client rather than replying only to the originating request. CommitProcessor.java:319-361 matches local session/cxid and otherwise processes remote writes; with no sessions or reads the relevant invariant is the commit FIFO, represented by `toApply`. ACLs, validation, ephemeral nodes and read workers are outside this integer-broadcast interface.
16. **Cache update at delivery.** Database.apply updates the tree and committed cache as one transition. ZKDatabase.java:322-351 defines cache insertion and ordering, :494-495 delegates tree application; FinalRequestProcessor.java:111 delegates to ZooKeeperServer.processTxn, whose implementation is outside the supplied cut. The missing wrapper leaves the exact cache insertion boundary unspecified here. Leader.toBeApplied covers the gap between COMMIT and delivery, and no delivery/cache insertion is split across receives.
17. **Zxids and rollover.** Marker `(1L<<32)` has counter zero. Real transactions start at `(1L<<32)|1` and increase the low 32-bit counter. ZxidUtils.java:23-33 supplies the encoding. Leader.java:1293-1305 forces election on counter 0xffffffff; the port asserts there because the interface forbids a new leader/epoch. For positive int e, at most `2*Integer.MAX_VALUE = 0xfffffffe` proposals exist, so the reserved rollover counter is unreachable. currentEpoch changes before the learner's NEWLEADER ACK, after the history is applied (Learner.java:811-820).
18. **Warnings and unreachable paths.** Diagnostic logs, metrics, JMX, authentication/version negotiation beyond modern version validation, observers/INFORM, dynamic quorum configuration and activation packets, session revalidation, SYNC reads, PINGs, ticks/timeouts and failures/reconnects are omitted as requested by the README/interface. Source examples are LearnerHandler.java:678-710 (observer/session/SYNC dispatch), Follower.java:158-160 and :233-238 (PING/revalidation), Leader.java:1508-1546 and :1613-1655 (timed waits). Untimed barriers are state flags and message receives. No timer, sleep, ping or session event is introduced. No optional source warning is turned into successful reordering/recovery.

19. **Delivery guard.** `Database.apply` asserts strictly increasing delivered zxids, including the initial leader marker as a lower bound. The ordered commit/application queues enforce this in normal FIFO executions. The supplied cut delegates the detailed data-tree implementation through ZKDatabase.java:494-495; the port makes the requested zxid-order invariant explicit rather than silently applying a reversed history under bag. The cache also retains the source's same-epoch continuity check (ZKDatabase.java:333-341). These assertions diagnose invalid executions; they do not buffer or repair packets.

20. **Recipient iteration order.** Source Leader.sendPacket (:1188-1194) iterates the forwarding-follower collection without specifying a protocol order between recipients. The port serialises outgoing broadcasts in server-index order, and releases eligible handshake waiters in that order too. Packet order on each individual connection is preserved; subsequent reception is chosen by the API. This is a fixed implementation iteration order, not a protocol leader or quorum choice. Snapshots of outgoing queues are immutable after construction.

## Build and the required single run

Working directory: this directory. Compilation command, executed successfully (exit 0, no compiler output), and repeated after removing an unnecessary constructor upper bound on e:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Exactly one launcher invocation was made, with the required arguments and stdin:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.zookeeper_zab_sol.ZookeeperZabSol:impl,1,0,fifo' must 0 1 report - 600 </dev/null
```

Its stdout/stderr were captured in `run-output.txt`; the configured report path was `report`. No other launcher runs or ad hoc protocol test runs were made. The specified run covers e=1, late=0, fifo only; it does not validate late=1, larger e, or bag. The final constructor validation change does not change this e=1 execution.

The process exited with status 0 after 600.047 seconds of checking. The checker stopped because of its configured timeout, with **43,186 iterations, all 43,186 blocked**, **0 distinct complete graphs**, **43,186 distinct blocked graphs**, and **stack not exhausted**. Servers intentionally block on receive, so a blocked final execution is expected; the timeout means exploration was not exhaustive. No protocol assertion was reported. The launcher also printed an initial logging-configuration diagnostic, `Console contains an invalid element or attribute "level"`, before checking began.

The exact last output line was:

```text
max_porf_count: 0
```
