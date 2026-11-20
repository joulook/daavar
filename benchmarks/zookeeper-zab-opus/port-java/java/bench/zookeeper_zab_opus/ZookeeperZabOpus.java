package bench.zookeeper_zab_opus;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Port of Apache ZooKeeper 3.9's atomic broadcast (Zab) with the fixed leader n0 of epoch 1, three
 * servers n0..n2 and two clients c0, c1 (INTERFACE.md). See NOTES.md for the mapping to the source.
 *
 * <p>Argument string: {@code impl,<e>,<late>,<fifo|bag>}.
 */
public final class ZookeeperZabOpus implements MpProgram {

    // ------------------------------------------------------------------ messages
    /** client -> its server. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** server -> its client. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** n0 -> n2 (late = 1): start the learner protocol. */
    @Tag("Start")
    public record Start() implements MpMessage {}

    /** Learner -> leader: FOLLOWERINFO (zxid = makeZxid(acceptedEpoch, 0)). */
    @Tag("FollowerInfo")
    public record FollowerInfo(int sid, long zxid) implements MpMessage {}

    /** Leader -> learner: LEADERINFO (zxid = makeZxid(newEpoch, 0)). */
    @Tag("LeaderInfo")
    public record LeaderInfo(long zxid) implements MpMessage {}

    /** Learner -> leader: ACKEPOCH (currentEpoch or -1, lastLoggedZxid). */
    @Tag("AckEpoch")
    public record AckEpoch(int sid, int epoch, long zxid) implements MpMessage {}

    /** Leader -> learner: DIFF. */
    @Tag("Diff")
    public record Diff(long zxid) implements MpMessage {}

    /** Leader -> learner: TRUNC. */
    @Tag("Trunc")
    public record Trunc(long zxid) implements MpMessage {}

    /** Leader -> learner: SNAP followed by the serialized data tree (here: in the same packet). */
    @Tag("Snap")
    public record Snap(long zxid, List<Integer> tree) implements MpMessage {}

    /** Leader -> learner: NEWLEADER. */
    @Tag("NewLeader")
    public record NewLeader(long zxid) implements MpMessage {}

    /** Leader -> learner: UPTODATE. */
    @Tag("UpToDate")
    public record UpToDate() implements MpMessage {}

    /** Leader -> learner: PROPOSAL (the txn is the client's value). */
    @Tag("Proposal")
    public record Proposal(long zxid, int value) implements MpMessage {}

    /** Learner -> leader: ACK. */
    @Tag("Ack")
    public record Ack(int sid, long zxid) implements MpMessage {}

    /** Leader -> learner: COMMIT. */
    @Tag("Commit")
    public record Commit(long zxid) implements MpMessage {}

    /** Learner -> leader: REQUEST (a forwarded client write). */
    @Tag("Request")
    public record Request(int sid, int value) implements MpMessage {}

    // ------------------------------------------------------------------ configuration
    private static final int N = 3;
    private static final int CLIENTS = 2;
    private static final int LEADER = 0;

    private final int e;
    private final int late;
    private final CommunicationModel model;
    private final String modelName;

    /** The process ids, from the spawn order: the k-th process main spawns is main's task id + 1 + k. */
    private final Tid[] servers = new Tid[N];
    private final Tid[] clients = new Tid[CLIENTS];

    public ZookeeperZabOpus(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<late>,<fifo|bag>");
        }
        e = Integer.parseInt(args[1]);
        late = Integer.parseInt(args[2]);
        modelName = args[3];
        if (e < 1 || (late != 0 && late != 1)) {
            throw new IllegalArgumentException("e >= 1 and late in {0,1}");
        }
        model = switch (modelName) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag");
        };
    }

    @Override
    public String programName() {
        return "zookeeper_zab";
    }

    @Override
    public String size() {
        return "e=" + e + ",late=" + late;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public void run() {
        long base = MpProcess.self().raw() + 1;
        for (int i = 0; i < N; i++) {
            servers[i] = Tid.of(base + i, "n" + i);
        }
        for (int i = 0; i < CLIENTS; i++) {
            clients[i] = Tid.of(base + N + i, "c" + i);
        }
        List<MpProcess.Handle> hs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int id = i;
            hs.add(MpProcess.spawn("n" + i, () -> {
                if (id == LEADER) {
                    new LeaderServer().run();
                } else {
                    new FollowerServer(id).run();
                }
            }));
        }
        for (int i = 0; i < CLIENTS; i++) {
            final int id = i;
            hs.add(MpProcess.spawn("c" + i, () -> client(id)));
        }
        for (int i = 0; i < N + CLIENTS; i++) {
            if (hs.get(i).tid().raw() != base + i) {
                throw new IllegalStateException("unexpected task id for process " + i + ": " + hs.get(i).tid());
            }
        }
        // Clients end; servers never do (an execution ends with them blocked on receive).
        for (int i = N; i < N + CLIENTS; i++) {
            hs.get(i).await();
        }
        for (int i = 0; i < N; i++) {
            hs.get(i).await();
        }
    }

    @Override
    public String toString() {
        return "ZookeeperZabOpus(e=" + e + ", late=" + late + ", " + modelName + ")";
    }

    // ------------------------------------------------------------------ client
    private static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    private void client(int i) {
        List<Integer> mine = new ArrayList<>();
        for (int j = 0; j < e; j++) {
            mine.add(value(i, j));
        }
        for (int v : mine) {
            Channel.send(model, servers[i], new Propose(v));
        }
        int seen = 0;
        while (seen < e) {
            Committed c = (Committed) Channel.recv(model, m -> m instanceof Committed);
            if (mine.contains(c.value())) {
                seen++;
            }
        }
    }

    // ------------------------------------------------------------------ helpers
    /** ZxidUtils.makeZxid. */
    static long makeZxid(long epoch, long counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
    }

    /** ZxidUtils.getEpochFromZxid. */
    static long epochOf(long zxid) {
        return zxid >> 32L;
    }

    /** QuorumMaj.containsQuorum over the three voting members. */
    static boolean containsQuorum(Set<Integer> s) {
        return s.size() > N / 2;
    }

    /** A logged / committed transaction: (zxid, value). */
    record Txn(long zxid, int value) {}

    /**
     * ServiceUtils.requestSystemExit / the end of followLeader or of a LearnerHandler: the process
     * stops taking part. A server never terminates, so it blocks on a receive that accepts
     * nothing.
     */
    private void halt() {
        Channel.recv(model, m -> false);
        throw new IllegalStateException("unreachable");
    }

    /** The in-memory ZKDatabase: data tree, committedLog and the "on-disk" transaction log. */
    static final class Db {
        final List<Integer> tree = new ArrayList<>();   // DataTree (applied values)
        long lastProcessedZxid = 0;                      // DataTree.lastProcessedZxid
        final List<Txn> committedLog = new ArrayList<>(); // ZKDatabase.committedLog
        long minCommittedLog = 0;
        long maxCommittedLog = 0;
        final List<Txn> txnLog = new ArrayList<>();      // TxnLog (SyncRequestProcessor / append)

        /** ZKDatabase.addCommittedProposal (commitLogCount = 500 is never reached). */
        void addCommittedProposal(Txn t) {
            if (committedLog.isEmpty()) {
                minCommittedLog = t.zxid();
                maxCommittedLog = t.zxid();
            } else if (t.zxid() <= maxCommittedLog) {
                return;
            } else if (t.zxid() != maxCommittedLog + 1 && epochOf(t.zxid()) <= epochOf(maxCommittedLog)) {
                throw new IllegalStateException("Committed proposal cached out of order");
            }
            committedLog.add(t);
            maxCommittedLog = t.zxid();
        }

        /** ZooKeeperServer.processTxn as far as the log goes: apply to the tree, cache in committedLog. */
        void processTxn(Txn t) {
            tree.add(t.value());
            lastProcessedZxid = t.zxid();
            addCommittedProposal(t);
        }

        /** ZKDatabase.clear. */
        void clear() {
            tree.clear();
            lastProcessedZxid = 0;
            committedLog.clear();
            minCommittedLog = 0;
            maxCommittedLog = 0;
        }

        /** TxnLog.append (SyncRequestProcessor / appendRequest). */
        void append(Txn t) {
            txnLog.add(t);
        }

        /**
         * ZKDatabase.getProposalsFromTxnLog(startZxid, sizeLimit = 0): the iterator starts at the
         * first logged txn with zxid >= startZxid; if that txn's zxid is greater than startZxid the
         * result is empty; an empty log gives an empty iterator.
         */
        List<Txn> proposalsFromTxnLog(long startZxid) {
            int i = 0;
            while (i < txnLog.size() && txnLog.get(i).zxid() < startZxid) {
                i++;
            }
            if (i < txnLog.size() && txnLog.get(i).zxid() > startZxid) {
                return List.of();
            }
            return new ArrayList<>(txnLog.subList(i, txnLog.size()));
        }

        /** ZKDatabase.truncateLog: truncate the txn log to zxid, reload the tree from it. */
        void truncateLog(long zxid) {
            clear();
            txnLog.removeIf(t -> t.zxid() > zxid);
            for (Txn t : txnLog) {
                processTxn(t);
            }
        }
    }

    // ------------------------------------------------------------------ leader (n0)
    /** Leader.lead, the LearnerHandlers and the leader's request pipeline in one process. */
    private final class LeaderServer {
        // lead() thread states
        static final int L_EPOCH = 0, L_EPOCHACK = 1, L_NLACK = 2, L_RUNNING = 3;
        // LearnerHandler states
        static final int H_NONE = 0, H_WAIT_EPOCH = 1, H_WAIT_ACKEPOCH = 2, H_WAIT_EPOCHACKQ = 3,
                H_WAIT_NLACK = 4, H_WAIT_NLQ = 5, H_WAIT_STARTUP = 6, H_RUNNING = 7, H_DEAD = 8;

        final Db db = new Db();
        final int myId = LEADER;
        long acceptedEpoch = 0;
        long currentEpoch = 0;
        long hzxid = 0;                     // ZooKeeperServer.hzxid (zk.getZxid / setZxid)
        int leadState = L_EPOCH;
        long[] leaderSS;                    // StateSummary(currentEpoch, lastProcessedZxid)

        // Leader fields
        long epoch = -1;
        boolean waitingForNewEpoch = true;
        final Set<Integer> connectingFollowers = new HashSet<>();
        final Set<Integer> electingFollowers = new HashSet<>();
        boolean electionFinished = false;
        long newLeaderZxid;                 // newLeaderProposal.packet.zxid
        final Set<Integer> newLeaderAcks = new HashSet<>();
        boolean quorumFormed = false;
        boolean running = false;            // zk.isRunning()
        long lastProposed;
        long lastCommitted = -1;
        final TreeMap<Long, Set<Integer>> outstanding = new TreeMap<>(); // outstandingProposals: acks
        final TreeMap<Long, Integer> outstandingValue = new TreeMap<>();
        final boolean[] forwarding = new boolean[N]; // forwardingFollowers

        // per-LearnerHandler state
        final int[] hState = new int[N];
        final long[] hNewEpoch = new long[N];
        boolean startSent = false;

        void run() {
            // lead(): leaderStateSummary, then getEpochToPropose(myId, acceptedEpoch).
            leaderSS = new long[] {currentEpoch, db.lastProcessedZxid};
            getEpochToPropose(myId, acceptedEpoch);
            progress();
            while (true) {
                MpMessage m = Channel.recv(model, guard());
                handle(m);
                progress();
            }
        }

        /** What the leader can read now: each LearnerHandler reads its own socket in its state. */
        Predicate<MpMessage> guard() {
            final int[] hs = hState.clone();
            final boolean accept = acceptsWrites();
            return m -> {
                if (m instanceof FollowerInfo fi) {
                    return fi.sid() > 0 && fi.sid() < N && hs[fi.sid()] == H_NONE;
                } else if (m instanceof AckEpoch ae) {
                    return hs[ae.sid()] == H_WAIT_ACKEPOCH;
                } else if (m instanceof Ack a) {
                    return hs[a.sid()] == H_WAIT_NLACK || hs[a.sid()] == H_RUNNING;
                } else if (m instanceof Request r) {
                    return hs[r.sid()] == H_RUNNING && accept;
                } else if (m instanceof Propose) {
                    return accept;
                }
                return false;
            };
        }

        /**
         * Whether the request pipeline takes writes: the server is running (startZkServer), and
         * with late = 0 every follower has joined (is a forwarding follower) - INTERFACE.md.
         */
        boolean acceptsWrites() {
            if (!running) {
                return false;
            }
            if (late == 0) {
                for (int s = 1; s < N; s++) {
                    if (!forwarding[s]) {
                        return false;
                    }
                }
            }
            return true;
        }

        void handle(MpMessage m) {
            if (m instanceof FollowerInfo fi) {
                onFollowerInfo(fi);
            } else if (m instanceof AckEpoch ae) {
                onAckEpoch(ae);
            } else if (m instanceof Ack a) {
                if (hState[a.sid()] == H_WAIT_NLACK) {
                    onNewLeaderAck(a);
                } else {
                    processAck(a.sid(), a.zxid());
                }
            } else if (m instanceof Request r) {
                // LearnerHandler REQUEST -> submitLearnerRequest -> the leader's pipeline.
                submitRequest(r.value());
            } else if (m instanceof Propose p) {
                submitRequest(p.value());
            }
        }

        // ---------------------------------------------------------- discovery
        /** Leader.getEpochToPropose; returns true when the caller does not wait. */
        boolean getEpochToPropose(int sid, long lastAcceptedEpoch) {
            if (!waitingForNewEpoch) {
                return true;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch + 1;
            }
            connectingFollowers.add(sid);
            if (connectingFollowers.contains(myId) && containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                acceptedEpoch = epoch;
                return true;
            }
            return false;
        }

        /** Leader.waitForEpochAck; returns 1 = go on, 0 = wait, -1 = IOException. */
        int waitForEpochAck(int id, long ssEpoch, long ssZxid) {
            if (electionFinished) {
                return 1;
            }
            if (ssEpoch != -1) {
                boolean moreRecent = ssEpoch > leaderSS[0] || (ssEpoch == leaderSS[0] && ssZxid > leaderSS[1]);
                if (moreRecent) {
                    return -1;
                }
                if (ssZxid != -1) {
                    electingFollowers.add(id);
                }
            }
            if (electingFollowers.contains(myId) && containsQuorum(electingFollowers)) {
                electionFinished = true;
                return 1;
            }
            return 0;
        }

        /** Leader.waitForNewLeaderAck; returns true when the caller does not wait. */
        boolean waitForNewLeaderAck(int sid, long zxid) {
            if (quorumFormed) {
                return true;
            }
            if (zxid != newLeaderZxid) {
                return true; // logged as an error; the method returns
            }
            newLeaderAcks.add(sid);
            if (containsQuorum(newLeaderAcks)) {
                quorumFormed = true;
                return true;
            }
            return false;
        }

        /** LearnerHandler.run up to getEpochToPropose. */
        void onFollowerInfo(FollowerInfo fi) {
            int sid = fi.sid();
            long lastAcceptedEpoch = epochOf(fi.zxid());
            if (getEpochToPropose(sid, lastAcceptedEpoch)) {
                sendLeaderInfo(sid);
            } else {
                hState[sid] = H_WAIT_EPOCH;
            }
        }

        void sendLeaderInfo(int sid) {
            hNewEpoch[sid] = epoch;
            Channel.send(model, servers[sid], new LeaderInfo(makeZxid(epoch, 0)));
            hState[sid] = H_WAIT_ACKEPOCH;
        }

        /** LearnerHandler.run: ACKEPOCH -> waitForEpochAck. */
        void onAckEpoch(AckEpoch ae) {
            int sid = ae.sid();
            hPeerLastZxid[sid] = ae.zxid();
            int r = waitForEpochAck(sid, ae.epoch(), ae.zxid());
            if (r < 0) {
                hState[sid] = H_DEAD;
            } else if (r == 0) {
                hState[sid] = H_WAIT_EPOCHACKQ;
            } else {
                syncAndSendNewLeader(sid);
            }
        }

        final long[] hPeerLastZxid = new long[N];

        /** LearnerHandler.run: syncFollower, SNAP if needed, NEWLEADER, start sending. */
        void syncAndSendNewLeader(int sid) {
            List<MpMessage> queued = new ArrayList<>();
            boolean needSnap = syncFollower(sid, hPeerLastZxid[sid], queued);
            List<MpMessage> wire = new ArrayList<>();
            if (needSnap) {
                // written directly to the socket, before the sender thread drains the queue
                wire.add(new Snap(db.lastProcessedZxid, List.copyOf(db.tree)));
            }
            wire.addAll(queued);
            wire.add(new NewLeader(makeZxid(hNewEpoch[sid], 0)));
            for (MpMessage p : wire) {
                Channel.send(model, servers[sid], p);
            }
            hState[sid] = H_WAIT_NLACK;
        }

        boolean needOpPacket;

        /** LearnerHandler.syncFollower. */
        boolean syncFollower(int sid, long peerLastZxid, List<MpMessage> queued) {
            boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
            long currentZxid = peerLastZxid;
            boolean needSnap = true;
            needOpPacket = true;
            long maxCommittedLog = db.maxCommittedLog;
            long minCommittedLog = db.minCommittedLog;
            long lastProcessedZxid = db.lastProcessedZxid;
            if (db.committedLog.isEmpty()) {
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }
            if (lastProcessedZxid == peerLastZxid) {
                queued.add(new Diff(peerLastZxid));
                needOpPacket = false;
                needSnap = false;
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                queued.add(new Trunc(maxCommittedLog));
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            } else if (maxCommittedLog >= peerLastZxid && minCommittedLog <= peerLastZxid) {
                currentZxid = queueCommittedProposals(queued, db.committedLog, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            } else if (peerLastZxid < minCommittedLog) { // txnLogSyncEnabled: snapshotSizeFactor 0.33 >= 0
                List<Txn> txnLogItr = db.proposalsFromTxnLog(peerLastZxid);
                if (!txnLogItr.isEmpty()) {
                    currentZxid = queueCommittedProposals(queued, txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);
                    if (currentZxid < minCommittedLog) {
                        currentZxid = peerLastZxid;
                        queued.clear();
                        needOpPacket = true;
                    } else {
                        currentZxid = queueCommittedProposals(queued, db.committedLog, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                }
            }
            if (needSnap) {
                currentZxid = db.lastProcessedZxid;
            }
            startForwarding(sid, currentZxid, queued);
            if (needOpPacket && !needSnap) {
                needSnap = true;
            }
            return needSnap;
        }

        /** LearnerHandler.queueCommittedProposals. */
        long queueCommittedProposals(List<MpMessage> queued, List<Txn> itr, long peerLastZxid, Long maxZxid,
                                     long lastCommittedZxid) {
            boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
            long queuedZxid = peerLastZxid;
            long prevProposalZxid = -1;
            for (Txn propose : itr) {
                long packetZxid = propose.zxid();
                if (maxZxid != null && packetZxid > maxZxid) {
                    break;
                }
                if (packetZxid < peerLastZxid) {
                    prevProposalZxid = packetZxid;
                    continue;
                }
                if (needOpPacket) {
                    if (packetZxid == peerLastZxid) {
                        queued.add(new Diff(lastCommittedZxid));
                        needOpPacket = false;
                        continue;
                    }
                    if (isPeerNewEpochZxid) {
                        queued.add(new Diff(lastCommittedZxid));
                        needOpPacket = false;
                    } else if (packetZxid > peerLastZxid) {
                        if (epochOf(packetZxid) != epochOf(peerLastZxid)) {
                            return queuedZxid;
                        }
                        queued.add(new Trunc(prevProposalZxid));
                        needOpPacket = false;
                    }
                }
                if (packetZxid <= queuedZxid) {
                    continue;
                }
                queued.add(new Proposal(packetZxid, propose.value()));
                queued.add(new Commit(packetZxid));
                queuedZxid = packetZxid;
            }
            if (needOpPacket && isPeerNewEpochZxid) {
                queued.add(new Diff(lastCommittedZxid));
                needOpPacket = false;
            }
            return queuedZxid;
        }

        /** Leader.startForwarding (toBeApplied is always empty here: commit and apply are one step). */
        void startForwarding(int sid, long lastSeenZxid, List<MpMessage> queued) {
            if (lastProposed > lastSeenZxid) {
                for (var en : outstanding.entrySet()) {
                    long zxid = en.getKey();
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    queued.add(new Proposal(zxid, outstandingValue.get(zxid)));
                }
            }
            forwarding[sid] = true;
        }

        /** LearnerHandler.run: the first ACK after NEWLEADER -> waitForNewLeaderAck. */
        void onNewLeaderAck(Ack a) {
            int sid = a.sid();
            if (waitForNewLeaderAck(sid, a.zxid())) {
                hState[sid] = H_WAIT_STARTUP;
            } else {
                hState[sid] = H_WAIT_NLQ;
            }
        }

        /** Resumes every waiting thread whose condition holds (notifyAll), lead thread first. */
        void progress() {
            boolean changed = true;
            while (changed) {
                changed = false;
                if (leadState == L_EPOCH && !waitingForNewEpoch) {
                    hzxid = makeZxid(epoch, 0);
                    lastProposed = hzxid;
                    newLeaderZxid = hzxid;
                    leadState = L_EPOCHACK;
                    waitForEpochAck(myId, leaderSS[0], leaderSS[1]);
                    changed = true;
                }
                if (leadState == L_EPOCHACK && electionFinished) {
                    currentEpoch = epoch;
                    leadState = L_NLACK;
                    waitForNewLeaderAck(myId, hzxid);
                    changed = true;
                }
                if (leadState == L_NLACK && quorumFormed) {
                    // startZkServer
                    lastCommitted = hzxid;
                    running = true;
                    db.lastProcessedZxid = hzxid;
                    leadState = L_RUNNING;
                    changed = true;
                }
                for (int sid = 1; sid < N; sid++) {
                    switch (hState[sid]) {
                        case H_WAIT_EPOCH -> {
                            if (!waitingForNewEpoch) {
                                sendLeaderInfo(sid);
                                changed = true;
                            }
                        }
                        case H_WAIT_EPOCHACKQ -> {
                            if (electionFinished) {
                                syncAndSendNewLeader(sid);
                                changed = true;
                            }
                        }
                        case H_WAIT_NLQ -> {
                            if (quorumFormed) {
                                hState[sid] = H_WAIT_STARTUP;
                                changed = true;
                            }
                        }
                        case H_WAIT_STARTUP -> {
                            if (running) {
                                Channel.send(model, servers[sid], new UpToDate());
                                hState[sid] = H_RUNNING;
                                changed = true;
                            }
                        }
                        default -> { }
                    }
                }
            }
        }

        // ---------------------------------------------------------- broadcast
        /** PrepRequestProcessor (assign zxid), ProposalRequestProcessor, SyncRequestProcessor, AckRequestProcessor. */
        void submitRequest(int value) {
            long zxid = ++hzxid;
            // Leader.propose
            outstanding.put(zxid, new HashSet<>());
            outstandingValue.put(zxid, value);
            lastProposed = zxid;
            sendPacket(new Proposal(zxid, value));
            // SyncRequestProcessor: log, then AckRequestProcessor: processAck(self)
            db.append(new Txn(zxid, value));
            processAck(myId, zxid);
        }

        /** Leader.sendPacket: to every forwarding follower. */
        void sendPacket(MpMessage p) {
            for (int s = 1; s < N; s++) {
                if (forwarding[s]) {
                    Channel.send(model, servers[s], p);
                }
            }
        }

        /** Leader.processAck. */
        void processAck(int sid, long zxid) {
            if ((zxid & 0xffffffffL) == 0) {
                return;
            }
            if (outstanding.isEmpty()) {
                return;
            }
            if (lastCommitted >= zxid) {
                return;
            }
            Set<Integer> p = outstanding.get(zxid);
            if (p == null) {
                return;
            }
            p.add(sid);
            tryToCommit(zxid);
        }

        /** Leader.tryToCommit. */
        boolean tryToCommit(long zxid) {
            if (outstanding.containsKey(zxid - 1)) {
                return false;
            }
            if (!containsQuorum(outstanding.get(zxid))) {
                return false;
            }
            int value = outstandingValue.get(zxid);
            outstanding.remove(zxid);
            outstandingValue.remove(zxid);
            // Leader.commit
            lastCommitted = zxid;
            sendPacket(new Commit(zxid));
            // zk.commitProcessor.commit -> ToBeApplied -> FinalRequestProcessor
            apply(new Txn(zxid, value));
            return true;
        }

        /** FinalRequestProcessor: apply, then reply to the client. */
        void apply(Txn t) {
            db.processTxn(t);
            Channel.send(model, clients[LEADER], new Committed(t.value()));
            if (late == 1 && !startSent) {
                startSent = true;
                Channel.send(model, servers[2], new Start());
            }
        }
    }

    // ------------------------------------------------------------------ followers (n1, n2)
    /** QuorumPeer FOLLOWING: Follower.followLeader, Learner, FollowerZooKeeperServer. */
    private final class FollowerServer {
        static final int F_WAIT_START = 0, F_WAIT_LEADERINFO = 1, F_SYNC_FIRST = 2, F_SYNC = 3, F_RUNNING = 4;

        final int myId;
        final Db db = new Db();
        long acceptedEpoch = 0;
        long currentEpoch = 0;
        int state;
        long newEpochZxid;
        long newEpoch;

        // syncWithLeader locals
        boolean snapshotNeeded = true;
        boolean writeToTxnLog;
        long lastQueuedSync = 0;
        final Deque<Long> packetsCommitted = new ArrayDeque<>();
        final Deque<Txn> packetsNotLogged = new ArrayDeque<>();

        // FollowerZooKeeperServer
        final Deque<Txn> pendingTxns = new ArrayDeque<>();
        long lastQueued = 0; // Follower.lastQueued

        FollowerServer(int id) {
            this.myId = id;
        }

        boolean hasClient() {
            return myId < CLIENTS;
        }

        void run() {
            if (late == 1 && myId == 2) {
                state = F_WAIT_START;
                Channel.recv(model, m -> m instanceof Start);
            }
            registerWithLeader();
            while (true) {
                final boolean acceptClient = state == F_RUNNING;
                MpMessage m = Channel.recv(model, x -> !(x instanceof Start)
                        && (!(x instanceof Propose) || acceptClient));
                if (m instanceof Propose p) {
                    // FollowerRequestProcessor -> Learner.request: forward REQUEST to the leader
                    Channel.send(model, servers[LEADER], new Request(myId, p.value()));
                    continue;
                }
                switch (state) {
                    case F_WAIT_LEADERINFO -> onLeaderInfo(m);
                    case F_SYNC_FIRST -> onFirstSyncPacket(m);
                    case F_SYNC -> onSyncPacket(m);
                    case F_RUNNING -> processPacket(m);
                    default -> halt();
                }
            }
        }

        /** Learner.registerWithLeader: FOLLOWERINFO. */
        void registerWithLeader() {
            Channel.send(model, servers[LEADER], new FollowerInfo(myId, makeZxid(acceptedEpoch, 0)));
            state = F_WAIT_LEADERINFO;
        }

        /** Learner.registerWithLeader after LEADERINFO, then Follower.followLeader's epoch check. */
        void onLeaderInfo(MpMessage m) {
            if (!(m instanceof LeaderInfo li)) {
                halt();
                return;
            }
            long lastLoggedZxid = db.lastProcessedZxid; // QuorumPeer.getLastLoggedZxid
            long ne = epochOf(li.zxid());
            int epochBytes;
            if (ne > acceptedEpoch) {
                epochBytes = (int) currentEpoch;
                acceptedEpoch = ne;
            } else if (ne == acceptedEpoch) {
                epochBytes = -1;
            } else {
                halt();
                return;
            }
            Channel.send(model, servers[LEADER], new AckEpoch(myId, epochBytes, lastLoggedZxid));
            newEpochZxid = makeZxid(ne, 0);
            if (epochOf(newEpochZxid) < acceptedEpoch) {
                halt();
            }
            newEpoch = epochOf(newEpochZxid);
            state = F_SYNC_FIRST;
        }

        /** Learner.syncWithLeader: the first packet (DIFF / SNAP / TRUNC). */
        void onFirstSyncPacket(MpMessage m) {
            if (m instanceof Diff) {
                snapshotNeeded = false;
            } else if (m instanceof Snap s) {
                db.clear(); // deserializeSnapshot clears the db
                db.tree.addAll(s.tree());
                db.lastProcessedZxid = s.zxid();
            } else if (m instanceof Trunc t) {
                db.truncateLog(t.zxid());
                db.lastProcessedZxid = t.zxid();
            } else {
                halt();
                return;
            }
            lastQueuedSync = 0;
            writeToTxnLog = !snapshotNeeded;
            state = F_SYNC;
        }

        /** Learner.syncWithLeader: the loop up to UPTODATE. */
        void onSyncPacket(MpMessage m) {
            if (m instanceof Proposal p) {
                lastQueuedSync = enforceContinuousProposal(lastQueuedSync, p.zxid());
                packetsNotLogged.add(new Txn(p.zxid(), p.value()));
            } else if (m instanceof Commit c) {
                Txn pif = packetsNotLogged.peekFirst();
                if (!writeToTxnLog) {
                    if (pif == null) {
                        halt(); // NullPointerException in the source
                        return;
                    }
                    if (pif.zxid() == c.zxid()) {
                        apply(pif);
                        packetsNotLogged.remove();
                    }
                } else {
                    packetsCommitted.add(c.zxid());
                }
            } else if (m instanceof NewLeader nl) {
                writeToTxnLog = true;
                if (!packetsCommitted.isEmpty()) {
                    while (!packetsCommitted.isEmpty()) {
                        long zxid = packetsCommitted.removeFirst();
                        Txn pif = packetsNotLogged.peekFirst();
                        if (pif == null || pif.zxid() != zxid) {
                            continue;
                        }
                        packetsNotLogged.removeFirst();
                        db.append(pif);   // fzk.appendRequest
                        apply(pif);       // fzk.processTxn
                    }
                }
                currentEpoch = newEpoch;
                Channel.send(model, servers[LEADER], new Ack(myId, newEpochZxid));
            } else if (m instanceof UpToDate) {
                Channel.send(model, servers[LEADER], new Ack(myId, makeZxid(newEpoch, 0)));
                state = F_RUNNING; // zk.startup
                for (Txn p : packetsNotLogged) {
                    logRequest(p);
                }
                packetsNotLogged.clear();
                for (long zxid : packetsCommitted) {
                    commit(zxid);
                }
                packetsCommitted.clear();
            }
            // other packets: ignored by the loop
        }

        /** Learner.enforceContinuousProposal. */
        long enforceContinuousProposal(long lq, long zxid) {
            if (lq != 0 && zxid != lq + 1 && epochOf(zxid) <= epochOf(lq)) {
                halt();
            }
            return zxid;
        }

        /** Follower.processPacket. */
        void processPacket(MpMessage m) {
            if (m instanceof Proposal p) {
                lastQueued = p.zxid();
                logRequest(new Txn(p.zxid(), p.value()));
            } else if (m instanceof Commit c) {
                commit(c.zxid());
            }
            // UPTODATE after start: logged as an error and ignored
        }

        /** FollowerZooKeeperServer.logRequest -> SyncRequestProcessor -> SendAckRequestProcessor. */
        void logRequest(Txn t) {
            if ((t.zxid() & 0xffffffffL) != 0) {
                pendingTxns.add(t);
            }
            db.append(t);
            Channel.send(model, servers[LEADER], new Ack(myId, t.zxid()));
        }

        /** FollowerZooKeeperServer.commit -> CommitProcessor -> FinalRequestProcessor. */
        void commit(long zxid) {
            if (pendingTxns.isEmpty()) {
                return;
            }
            if (pendingTxns.peekFirst().zxid() != zxid) {
                halt(); // requestSystemExit(UNMATCHED_TXN_COMMIT)
                return;
            }
            apply(pendingTxns.removeFirst());
        }

        /** ZooKeeperServer.processTxn + the reply to the client. */
        void apply(Txn t) {
            db.processTxn(t);
            if (hasClient()) {
                Channel.send(model, clients[myId], new Committed(t.value()));
            }
        }
    }
}
