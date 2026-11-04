# ZooKeeper 3.9 Zab port

All source references below are to the supplied `zab-src/` cut, with its original
line numbers. Only this directory was inspected. Cargo alone reads the specified
external dependency and its build dependencies. This is one implementation;
`Program.spec` and `Program.imp` both construct it. The harness supplies the spec.

## Processes and entry

`implementation` starts `n0`, `n1`, `n2`, then `c0`, `c1`, then invisible `init`.
The entry thread only starts and joins threads. `init` sends each server a tagged
`Setup { nodes, clients }`. Servers select Setup first; messages already sent to
them remain buffered in their Must mailboxes. There are no shared protocol
objects, locks, atomics, helper server processes, or out-of-band exchanges.

The exact `Req::Propose(u64)` and `Rep::Committed(u64)` types and the interface's
client function are used. Each client sends all its values first, counts its own
committed values, and ignores the other client's values for termination. Every
application at n0/n1 sends Committed to its connected client, including applications
of the other client's transactions. Messages can remain for an exited client.
Server loops never terminate; joining them blocks the entry thread, and Must ends
the execution when all remaining threads are blocked. `n2` has no client.
Arguments are `<e> <late> [fifo|bag]`, with e positive and late 0/1. The optional
third argument is validated for interface compatibility; the actual exploration
model is controlled by the program runner's `--model`, as in `api/program.rs`.

## Roles and packets

### Leader n0

`Leader::run` multiplexes learner handlers in a single message loop. `phase[sid]`
records each handler's position. n0 counts itself in three distinct sets: connected
participants, epoch acknowledgers, and NEWLEADER acknowledgers. A majority is 2/3.

1. **FOLLOWERINFO** carries the follower sid and accepted epoch (initially zero).
   n0 waits for itself and one follower to connect, then sends **LEADERINFO** with
   epoch 1 to each connected follower. A subsequent follower gets the same epoch.
2. **ACKEPOCH** carries current epoch and last logged zxid. Before the epoch quorum,
   a nonnegative current epoch participates, after checking the follower is not
   ahead of the leader's initial state summary. The `-1` case does not count.
   Synchronisation begins only after the epoch quorum. Later ACKEPOCHs still supply
   the joining follower's last zxid without reopening that barrier.
3. `sync_follower` selects **DIFF**, **TRUNC**, or **SNAP** and queues the history.
   Committed history is sent as separate **PROPOSAL**, **COMMIT** pairs in zxid
   order. Outstanding proposals are then sent in zxid order and the follower is
   included in subsequent broadcasts (`startForwarding`). Finally **NEWLEADER**
   is queued with zxid `(1,0)`.
4. The first **ACK** from that learner is its NEWLEADER ACK. n0 waits for a quorum
   of these ACKs before starting the leader database at zxid `(1,0)`. It then sends
   **UPTODATE** to each learner whose NEWLEADER ACK has arrived. Subsequent ACKs
   of the epoch marker, including the ACK after UPTODATE, are ignored for broadcast.
5. **REQUEST** from n1 or the local preparation queue is assigned the next zxid.
   A **PROPOSAL** goes to every forwarding follower, including followers still
   synchronising. n0 appends the transaction to its log and counts its own ACK.
6. **ACK** adds a distinct server to that proposal's set. `process_ack` keeps
   `tryToCommit`'s predecessor guard and majority test. On success n0 removes the
   outstanding proposal, sends **COMMIT** to forwarding followers, then applies
   locally and sends Committed to c0. This preserves Leader.tryToCommit's
   COMMIT-before-local-commit ordering. `inform` has no recipients: observers are
   excluded by the interface.

For `late=0`, proposal admission additionally waits until both followers' NEWLEADER
ACKs have been processed and both UPTODATE packets have been queued. For `late=1`,
n0 starts with n1 and sends **Start** to n2 immediately after its first local
application/Committed send, before processing another packet. n2's first learner
packet is sent only after it receives Start. Thus its synchronization sees at least
one committed transaction; its initial zxid zero selects DIFF from retained history.

### Followers n1 and n2

`Follower::run` implements registration, synchronization, and broadcast consecutively.
The late n2 first receives Start. A follower sends FOLLOWERINFO, receives LEADERINFO,
updates accepted epoch, and sends ACKEPOCH containing its previous current epoch
(or -1 if already accepted) and last logged zxid. It then reads exactly one initial
DIFF/TRUNC/SNAP packet.

**DIFF** retains the existing tree. **TRUNC** removes log/tree/cache entries above
the supplied zxid and sets last processed zxid. **SNAP** replaces the tree and
last processed zxid with the transmitted in-memory snapshot. During synchronization,
PROPOSALs enter `not_logged` after `enforce_continuous_proposal`: the first proposal
is unrestricted, thereafter zxids must be consecutive or enter a greater epoch.
COMMIT zxids enter `commits`.

On NEWLEADER, the follower appends and applies the committed prefix without
individual proposal ACKs, retaining outstanding proposals. Only after those writes
and applications does it set current epoch and ACK NEWLEADER. Proposals/commits
arriving between NEWLEADER and UPTODATE remain in the synchronization buffers.
On UPTODATE it sends the extra epoch-marker ACK, logs the remaining proposals
in order, sends their ACKs, and processes buffered commits in order. It then admits
its buffered client requests as REQUESTs to n0.

In broadcast, a PROPOSAL is appended to `pending_txns` and the log before its ACK.
A COMMIT with an empty pending queue is ignored, matching the Java warning/return.
Otherwise its zxid must equal the pending head; mismatch is an assertion failure,
corresponding to Java's fatal `UNMATCHED_TXN_COMMIT` exit. The matched transaction
is removed, applied, and reported to c1 when this is n1. No search, sorting,
speculative delivery, or repair bypasses the pendingTxns head check.

## Source-to-function mapping

Ranges identify the relevant method body or the ordering slice within it.

| ZooKeeper method (source lines) | Rust function/state |
| --- | --- |
| QuorumPeer.run, 1427 onward (LEADING/FOLLOWING dispatch) | `server`, fixed sid dispatch |
| QuorumPeer.get/setCurrentEpoch, get/setAcceptedEpoch, 2246–2270 | `Follower::{current_epoch,accepted_epoch}`, `Follower::run`; leader epoch constants/barriers |
| Leader.lead, 632–718 | `Leader::run`, `advance_barriers`, initial self acknowledgements |
| Leader.getEpochToPropose, 1462–1497 | `advance_barriers` connected-follower gate and fixed `EPOCH` |
| Leader.waitForEpochAck, 1508–1545 | `run` ACKEPOCH branch, `epoch_acks`, `election_finished` |
| StateSummary.isMoreRecentThan, 42–47 | `run` ACKEPOCH initial-summary check |
| Leader.waitForNewLeaderAck, 1613–1653 | `run` first-ACK branch, `newleader_acks`, `advance_barriers` |
| Leader.startZkServer, 1560–1603 | `advance_barriers` database/last-committed epoch initialization |
| LearnerHandler.run, 463–655, 677–714 | `Leader::run`, `advance_barriers`, `sync_follower` |
| LearnerHandler.syncFollower, 780–942 | `Leader::sync_follower` |
| LearnerHandler.queueCommittedProposals, 951–1042 | `sync_follower` mode selection and proposal/commit replay loop |
| Leader.startForwarding, 1360–1393 | `sync_follower` replay, outstanding loop, phase 4 membership |
| Leader.propose, 1288–1337 | `Leader::propose`, `Proposal`, `outstanding` |
| Leader.processAck, 1047–1116 | `Leader::process_ack` |
| Leader.tryToCommit, 963–1038 | `process_ack` predecessor/quorum guard and commit path |
| Leader.commit, 1212–1220 | `process_ack` COMMIT broadcast |
| Leader.inform, 1238–1241 | omitted send: empty observer set |
| SyncedLearnerTracker.addAck/hasAllQuorums, 33–60; QuorumMaj.containsQuorum, 140–142 | boolean ACK sets, `quorum` |
| Learner.registerWithLeader, 486–540 | initial registration in `Follower::run` |
| Learner.syncWithLeader, 570–849 | synchronization loop in `Follower::run` |
| Learner.enforceContinuousProposal, 542–562 | `Follower::enforce_continuous_proposal` |
| Follower.followLeader, 69–132 | `Follower::run` registration/sync/broadcast sequence |
| Follower.processPacket, 153–205 | broadcast loop in `Follower::run` |
| FollowerZooKeeperServer.logRequest, 79–85 | `Follower::log_request`, `pending_txns` |
| FollowerZooKeeperServer.appendRequest, 91–93 | `Database::append` during NEWLEADER replay |
| FollowerZooKeeperServer.commit, 99–113 | `Follower::commit` |
| FollowerRequestProcessor.run, 83–170 | buffered local requests, UPTODATE REQUEST forwarding |
| LeaderRequestProcessor.processRequest, 51–89 | local preparation queue and `Leader::propose` |
| PrepRequestProcessor.pRequestHelper assignment sites, 829–857 | increment in `Leader::propose` |
| ProposalRequestProcessor.processRequest, 73–108 | `propose` broadcast followed by append/self ACK |
| SyncRequestProcessor.run/flush, 156–248 | `Database::append` before immediate ACK |
| AckRequestProcessor.processRequest, 45–55 | `propose` -> `process_ack(0, zxid)` |
| SendAckRequestProcessor.processRequest, 45–58 | `Follower::log_request` -> `ack` |
| CommitProcessor.commit/run, 590–600, 197 onward | `process_ack` and `Follower::commit` ordered application |
| FinalRequestProcessor.applyRequest/processRequest, 110–146 | `Database::apply` and Committed reply |
| ZKDatabase.addCommittedProposal, 322–353 | `Database::apply`, `committed_log` |
| ZKDatabase.truncateLog, 611–625 | `Database::truncate` |
| ZKDatabase.append/commit, 687–709 | `Database::append`; memory write completes persistence |
| ZKDatabase.getDataTreeLastProcessedZxid, 249–251 | `Database::last` |
| ZxidUtils.makeZxid/getEpochFromZxid/getCounterFromZxid, 23–31 | `NEW_ZXID`, increment, shifts and masks |
| Learner.writePacket/readPacket (ordered socket operations), 195–234; LearnerHandler.run, 473–475, 612–619 | `Wire::send`, `Wire::recv` |

## Decisions and simplifications

* **Epoch state:** Knowing the election result does not mean already having accepted
  the epoch. Initially current/accepted epoch and last logged zxid are zero; the
  fixed election selects epoch 1. Otherwise Learner.java:515–521 would send -1
  ACKEPOCHs and the first epoch barrier could not form. No election, epoch change,
  crashes, reconnects, or recovery workloads are introduced.
* **Transport ordering:** The Java uses ordered socket input/output archives
  (LearnerHandler.java:468–475, 535–545, 612–619). `Wire` sends each private
  `(sender, Packet)` envelope with a monotonically increasing per-destination tag;
  receives select the next tag per sender. This models that socket contract even
  on a bag transport; it is not a new Zab ACK protocol. Different connections
  still compete in arrival order. Client sends/replies remain untagged and use
  the exact prescribed code; under bag their arrival order is the bag model's.
* **Client batch collection:** Must's public blocking receive is typed
  (`api/lib.rs:1254–1268`), and the required client cannot wrap Req in a server
  envelope. After Setup, n0/n1 collect exactly e Req messages from their own
  clients before running the learner state machines. Those clients unconditionally
  send the entire finite batch before waiting. Requests are retained, not forwarded
  or proposed before readiness. The leader releases its local preparation queue
  through self REQUEST messages; local and forwarded REQUEST streams then compete
  by message arrival. This moves client socket intake ahead of protocol startup
  and collapses the independent intake thread; it is a deliberate scheduling
  abstraction of queuedRequests (FollowerRequestProcessor.java:158–170,
  PrepRequestProcessor.java:1086 onward), not an additional client protocol.
* **One process per server:** Java's sender, handler, preparation, log, and commit
  threads are sequential local transitions here. Sending a proposal precedes
  appending/self ACK; appending precedes a follower ACK; sending COMMIT precedes
  leader application. Source ordering is ProposalRequestProcessor.java:92–106,
  SyncRequestProcessor.java:181,227–247, Leader.java:1025–1028. No independent
  local pipeline scheduling or disk batching is modeled. Batch size is one and
  persistence completion is immediate. Source asynchronous delays can be observed
  through message arrival delays, but this is not an exhaustive model of all Java
  thread interleavings. Commit/apply are one local transition, so `toBeApplied`
  needs no separate queue; `committed_log` supplies its synchronization prefix.
* **NEWLEADER position:** In this modern protocol version, the source does NOT
  place already committed replay proposals after NEWLEADER: syncFollower queues
  them (LearnerHandler.java:556,872–899,951–1030), startForwarding queues outstanding
  proposals (925, Leader.java:1360–1387), then run queues NEWLEADER (596–607).
  This exact send order is retained. New broadcasts can be queued behind
  NEWLEADER while its ACK is in flight. Learner.java:778–821 requires committed
  replay to persist before NEWLEADER ACK, and 837–849 handles later buffers.
* **In-memory retention:** Keep all transaction log and committed-log entries;
  there is no eviction threshold or snapshot-size heuristic. Java's bounded cache
  (ZKDatabase.java:346–349) can fall back to the transaction log
  (LearnerHandler.java:874–906). The complete in-memory log stands in for that
  available history, enabling the required late join from zxid zero to use DIFF.
  Equality selects empty DIFF; a peer beyond committed history with a nonzero
  counter selects TRUNC; a known prefix or epoch marker selects DIFF. A missing
  non-marker zxid within an epoch truncates to the preceding entry; an epoch-crossing
  hole selects SNAP (LearnerHandler.java:987–1014). TRUNC and SNAP are implemented
  but unreachable from the specified fresh, failure-free initial states. No
  arbitrary preexisting/corrupted histories or storage failures are generated.
* **Tree abstraction:** A tree is the ordered vector of applied `(zxid,value)`
  transactions. Requests have no path, operation, session, ACL, or error result
  in this interface. Snapshots clone that vector and last processed zxid rather
  than serializing a disk image/signature (Learner.java:595–617). Truncation filters
  both in-memory log and tree; applying a snapshot emits no historical client
  replies. In reachable DIFF replay every newly applied transaction does emit the
  interface's notification, including during synchronization.
* **Initial zxid / continuity:** `(epoch << 32) | counter` is used; the epoch marker
  is not a transaction. Counter increments start at one. The total number of
  proposals is bounded below `0xffffffff`, where ZooKeeper forces reelection
  (Leader.java:1299–1307), incompatible with the fixed-leader interface.
  Synchronization deliberately starts `last_queued=0`, matching the source's TODO
  about its uncertain baseline (Learner.java:639–641); normal broadcast retains
  the source's warning-only gap behavior (Follower.java:165–172). The pending
  head and monotone application checks remain enforced.
* **Commit quorum progression:** Preserve the predecessor guard and only try the
  acknowledged proposal, as Leader.java:963–978,1094–1116 does without reconfiguration.
  Ordered follower ACK streams and immediate ordered self ACKs ensure a later
  proposal cannot acquire its first quorum before its predecessor. No synthetic
  quorum ACK, prefix-ACK interpretation, or commit sweep is added.
* **Startup gates:** ZooKeeper only requires a quorum (Leader.java:707–718);
  `late=0` additionally waits for all three learners to join, as INTERFACE.md
  requires. Joining means NEWLEADER acknowledged and UPTODATE queued; requests may
  be proposed while that UPTODATE is in transit, and the follower buffers them.
  For `late=1`, Start is the interface's additional trigger, not a ZooKeeper packet.
* **Choice:** All reachable protocol branches follow packet contents and local
  state. Order among eligible connections is selected by Must receives; ordered
  traversals model the source's sorted proposal queue. No free algorithmic choice
  requires `nondet()`. No random choices, ticks, timeouts, sleeps, heartbeat/ping,
  sessions, observers, reconfiguration, throttling, metrics, JMX, TLS, SASL, or disk
  snapshot threads are retained, per zab-src/README.md. Invalid source protocol
  states use assertions instead of Java's process-exit exceptions; empty pending
  commits keep the source's return behavior.

## Build and one requested run

Build command, from this directory:

```sh
cargo build --release -j 2
```

The first build attempted the unreachable crates.io index. The same build was
retried with `CARGO_NET_OFFLINE=true`, using Cargo's cached dependencies. After
formatting, the exact unprefixed `cargo build --release -j 2` also succeeded
(exit 0, release profile). The required run's results follow.

The release build succeeded (exit 0). The final formatting pass does not change
behavior. Build diagnostics are retained in `build.log`.

Exactly one program execution was invoked, from this directory:

```sh
timeout 600 target/release/bench_zookeeper_zab_sol count impl --model fifo -- 1 0
```

Exit status: **0**, before the timeout. Output is retained in `run.log`.
The result line was `execs=0 blocked=5415`; `stop_reason: exhausted` and
`graphs_blocked: 5415` were reported, with no assertion failures. All remaining
servers deliberately wait for messages, so executions are classified as blocked,
not as normally terminated. This count run is not a conformance check and does not
exercise late=1, larger e, or the bag model.

The actual last output line (the measurement block ends with an empty value) was:

```text
cache_hits: 
```
