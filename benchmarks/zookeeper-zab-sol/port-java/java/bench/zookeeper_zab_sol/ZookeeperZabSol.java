package bench.zookeeper_zab_sol;

import java.util.*;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Apache ZooKeeper 3.9 Zab, with the replacements specified in INTERFACE.md. */
public final class ZookeeperZabSol implements MpProgram {
    private static final long EPOCH = 1, NEW_EPOCH = zxid(EPOCH, 0);
    private final int e, late;
    private final CommunicationModel communication;

    public ZookeeperZabSol(String[] args) {
        if (args.length != 4 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<e>,<late>,<fifo|bag>");
        e = Integer.parseInt(args[1]);
        late = Integer.parseInt(args[2]);
        if (e < 1 || (late != 0 && late != 1))
            throw new IllegalArgumentException("invalid e or late");
        communication = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }
    public String programName() { return "zookeeper_zab"; }
    public String size() { return e + "," + late; }
    public CommunicationModel model() { return communication; }
    public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    public List<String> visibleRoles() { return List.of("c0", "c1"); }

    @Tag("Propose") public record Propose(int value) implements MpMessage {}
    @Tag("Committed") public record Committed(int value) implements MpMessage {}
    @Tag("Start") public record Start() implements MpMessage {}
    @Tag("FOLLOWERINFO") public record FollowerInfo(int sid, long acceptedEpoch, int version) implements MpMessage {}
    @Tag("LEADERINFO") public record LeaderInfo(long zxid, int version) implements MpMessage {}
    @Tag("ACKEPOCH") public record AckEpoch(int sid, long currentEpoch, long lastZxid) implements MpMessage {}
    @Tag("NEWLEADER") public record NewLeader(long zxid) implements MpMessage {}
    @Tag("DIFF") public record Diff(long zxid) implements MpMessage {}
    @Tag("TRUNC") public record Trunc(long zxid) implements MpMessage {}
    @Tag("SNAP") public record Snap(long zxid, List<Txn> tree) implements MpMessage {
        public Snap { tree = List.copyOf(tree); }
    }
    @Tag("UPTODATE") public record Uptodate() implements MpMessage {}
    @Tag("REQUEST") public record Request(int sid, int value) implements MpMessage {}
    @Tag("Txn") public record Txn(long zxid, int value) {}
    @Tag("PROPOSAL") public record Proposal(Txn txn) implements MpMessage {}
    @Tag("ACK") public record Ack(int sid, long zxid) implements MpMessage {}
    @Tag("COMMIT") public record Commit(long zxid) implements MpMessage {}

    static long zxid(long epoch, long counter) { return (epoch << 32) | (counter & 0xffffffffL); }
    static long epoch(long z) { return z >> 32; }
    static long counter(long z) { return z & 0xffffffffL; }
    static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    void send(Tid dst, MpMessage m) { Channel.send(communication, dst, m); }

    public void run() {
        // All addresses are installed before any process is started. No mutable protocol
        // state is shared: the arrays below are immutable configuration after construction.
        Tid[] nodes = new Tid[3], clients = new Tid[2];
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < 3; i++) {
            final int sid = i;
            threads[i] = new MpThread(() -> {
                if (sid == 0) new Leader(nodes.clone(), clients.clone()).lead();
                else new Follower(sid, nodes.clone(), clients.clone()).followLeader();
            }, "n" + i);
            nodes[i] = Tid.of(threads[i].getTaskId(), "n" + i);
        }
        for (int i = 0; i < 2; i++) {
            final int ci = i;
            threads[3 + i] = new MpThread(() -> client(ci, nodes[ci]), "c" + i);
            clients[i] = Tid.of(threads[3 + i].getTaskId(), "c" + i);
        }
        for (MpThread thread : threads) thread.start();
        try {
            // Clients first, since a join on a permanently receiving server blocks main.
            threads[3].join1();
            threads[4].join1();
            for (int i = 0; i < 3; i++) threads[i].join1();
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }
    void client(int ci, Tid server) {
        List<Integer> mine = new ArrayList<>();
        for (int j = 0; j < e; j++) mine.add(1 + ci + 2 * j);
        for (int v : mine) send(server, new Propose(v));
        int seen = 0;
        while (seen < e) {
            Committed reply = Channel.recv(communication, Committed.class);
            if (mine.contains(reply.value())) seen++;
        }
    }

    /** In-memory replacement of transaction files, the committed cache and the data tree. */
    static final class Database {
        final List<Txn> log = new ArrayList<>(), tree = new ArrayList<>();
        final Deque<Txn> committedLog = new ArrayDeque<>();
        long lastProcessed;
        void append(Txn t) { log.add(t); }
        long lastLogged() { return log.isEmpty() ? 0 : log.get(log.size() - 1).zxid(); }
        void addCommittedProposal(Txn t) {
            if (!committedLog.isEmpty()) {
                long prev = committedLog.getLast().zxid();
                if (t.zxid() <= prev) return;
                require(t.zxid() == prev + 1 || epoch(t.zxid()) > epoch(prev), "discontinuous committed log");
            }
            committedLog.addLast(t);
            if (committedLog.size() > 500) committedLog.removeFirst();
        }
        void apply(Txn t) {
            require(t.zxid() > lastProcessed, "data tree delivery out of order");
            tree.add(t);
            lastProcessed = t.zxid();
            addCommittedProposal(t);
        }
        void truncateLog(long z) {
            log.removeIf(t -> t.zxid() > z);
            tree.removeIf(t -> t.zxid() > z);
            committedLog.clear();
            for (Txn t : tree) addCommittedProposal(t);
            lastProcessed = z;
        }
        void install(Snap s) {
            tree.clear(); tree.addAll(s.tree());
            log.clear(); committedLog.clear();
            lastProcessed = s.zxid();
        }
    }

    abstract class Server {
        final int sid;
        final Tid[] nodes, clients;
        final Database db = new Database();
        final Deque<Integer> requests = new ArrayDeque<>();
        final Deque<Txn> toLog = new ArrayDeque<>(), toApply = new ArrayDeque<>();
        boolean serving;
        Server(int sid, Tid[] nodes, Tid[] clients) {
            this.sid = sid; this.nodes = nodes; this.clients = clients;
        }
        abstract void packet(MpMessage m);
        abstract void processRequest(int value);
        abstract void logged(Txn t);
        void delivered(Txn t) {}
        void apply(Txn t) {
            db.apply(t);
            if (sid < 2) send(clients[sid], new Committed(t.value()));
            delivered(t);
        }
        void loop() {
            for (;;) {
                List<Integer> work = new ArrayList<>();
                if (serving && !requests.isEmpty()) work.add(0);
                if (!toLog.isEmpty()) work.add(1);
                if (!toApply.isEmpty()) work.add(2);
                if (work.isEmpty()) {
                    packet(Channel.recv(communication));
                } else {
                    // A bottom result schedules an enabled local processor instead of
                    // parking a process that still has work. No timing or busy waiting.
                    Optional<MpMessage> incoming = Channel.tryRecv(communication);
                    if (incoming.isPresent()) packet(incoming.get());
                    else switch (Nondet.choose(work)) {
                        case 0 -> processRequest(requests.removeFirst());
                        case 1 -> {
                            Txn t = toLog.removeFirst();
                            db.append(t); // SyncRequestProcessor append + flush before ACK
                            logged(t);
                        }
                        case 2 -> apply(toApply.removeFirst());
                        default -> throw new AssertionError();
                    }
                }
            }
        }
    }

    final class Outstanding {
        final Txn txn;
        final Set<Integer> acks = new HashSet<>();
        Outstanding(Txn txn) { this.txn = txn; }
    }
    final class Peer {
        boolean connected, leaderInfoSent, epochAckReceived, synced, newAck, uptodate, startupAck;
        long lastZxid;
    }

    final class Leader extends Server {
        final Peer[] peers = {new Peer(), new Peer(), new Peer()};
        final Set<Integer> connecting = new HashSet<>(), epochAcks = new HashSet<>(), newAcks = new HashSet<>();
        final NavigableMap<Long, Outstanding> outstanding = new TreeMap<>();
        // Committed but not yet applied, as in Leader.toBeApplied.
        final Deque<Txn> toBeApplied = new ArrayDeque<>();
        boolean epochProposed, electionFinished, quorumFormed, started, sentStart;
        long currentEpoch, lastProposed = NEW_EPOCH, lastCommitted = NEW_EPOCH;
        Leader(Tid[] nodes, Tid[] clients) { super(0, nodes, clients); }
        void lead() {
            connecting.add(0); epochAcks.add(0); newAcks.add(0);
            loop();
        }
        void packet(MpMessage m) {
            if (m instanceof Propose p) requests.addLast(p.value());
            else if (m instanceof FollowerInfo f) getEpochToPropose(f);
            else if (m instanceof AckEpoch a) waitForEpochAck(a);
            else if (m instanceof Ack a) {
                Peer p = peers[a.sid()];
                require(p.synced, "ACK before synchronisation was sent");
                if (!p.newAck) waitForNewLeaderAck(a);
                else {
                    if (counter(a.zxid()) == 0 && p.uptodate) {
                        p.startupAck = true;
                        updateServing();
                    }
                    processAck(a.sid(), a.zxid());
                }
            } else if (m instanceof Request r) {
                require(peers[r.sid()].uptodate, "REQUEST before handler startup");
                requests.addLast(r.value());
            } else require(false, "unexpected leader packet " + m);
        }
        void getEpochToPropose(FollowerInfo f) {
            require(f.sid() == 1 || f.sid() == 2, "invalid sid");
            require(f.version() == 0x10000 && f.acceptedEpoch() < EPOCH, "unsupported epoch/protocol");
            Peer p = peers[f.sid()];
            require(!p.connected, "duplicate connection");
            p.connected = true; connecting.add(f.sid());
            if (connecting.size() >= 2) epochProposed = true;
            if (epochProposed) for (int i = 1; i < 3; i++) {
                Peer q = peers[i];
                if (q.connected && !q.leaderInfoSent) {
                    send(nodes[i], new LeaderInfo(NEW_EPOCH, 0x10000));
                    q.leaderInfoSent = true;
                }
            }
        }
        void waitForEpochAck(AckEpoch a) {
            Peer p = peers[a.sid()];
            require(p.leaderInfoSent && !p.epochAckReceived, "unexpected ACKEPOCH");
            if (!electionFinished && a.currentEpoch() != -1) {
                require(a.currentEpoch() <= 0 && (a.currentEpoch() < 0 || a.lastZxid() <= 0), "follower ahead of leader summary");
                if (a.lastZxid() != -1) epochAcks.add(a.sid());
            }
            p.epochAckReceived = true; p.lastZxid = a.lastZxid();
            if (epochAcks.size() >= 2) {
                electionFinished = true; currentEpoch = EPOCH;
            }
            if (electionFinished) for (int i = 1; i < 3; i++)
                if (peers[i].epochAckReceived && !peers[i].synced) syncFollower(i, peers[i].lastZxid);
        }
        void syncFollower(int follower, long peerZxid) {
            long max = db.committedLog.isEmpty() ? db.lastProcessed : db.committedLog.getLast().zxid();
            long min = db.committedLog.isEmpty() ? db.lastProcessed : db.committedLog.getFirst().zxid();
            List<MpMessage> queue = new ArrayList<>();
            boolean snapshot = false;
            long current = peerZxid;
            if (peerZxid == db.lastProcessed) queue.add(new Diff(peerZxid));
            else if (peerZxid > max && counter(peerZxid) != 0) {
                queue.add(new Trunc(max)); current = max;
            } else if (peerZxid >= min && peerZxid <= max) {
                current = queueCommittedProposals(queue, new ArrayList<>(db.committedLog), peerZxid, max);
                snapshot = queue.isEmpty();
            } else if (peerZxid < min) {
                // Full memory history has the known empty baseline 0; see NOTES.md.
                List<Txn> history = new ArrayList<>(db.tree);
                boolean covered = peerZxid == 0 || history.stream().anyMatch(t -> t.zxid() <= peerZxid);
                if (covered) current = queueCommittedProposals(queue, history, peerZxid, max);
                snapshot = queue.isEmpty();
            } else snapshot = true;
            if (snapshot) {
                queue.clear();
                queue.add(new Snap(db.lastProcessed, db.tree));
                current = db.lastProcessed;
            }
            startForwarding(follower, current, queue);
            // LearnerHandler queues NEWLEADER AFTER syncFollower's queued history.
            queue.add(new NewLeader(NEW_EPOCH));
            peers[follower].synced = true;
            for (MpMessage m : queue) send(nodes[follower], m);
        }
        long queueCommittedProposals(List<MpMessage> queue, List<Txn> history, long peerZxid, long max) {
            boolean needOp = true;
            long previous = -1, queued = peerZxid;
            for (Txn t : history) {
                if (t.zxid() < peerZxid) { previous = t.zxid(); continue; }
                if (needOp) {
                    if (t.zxid() == peerZxid) {
                        queue.add(new Diff(max)); needOp = false; continue;
                    }
                    if (counter(peerZxid) == 0) queue.add(new Diff(max));
                    else {
                        if (epoch(t.zxid()) != epoch(peerZxid)) { queue.clear(); return peerZxid; }
                        queue.add(new Trunc(previous));
                    }
                    needOp = false;
                }
                if (t.zxid() <= queued) continue;
                queue.add(new Proposal(t)); queue.add(new Commit(t.zxid())); queued = t.zxid();
            }
            if (needOp && counter(peerZxid) == 0) queue.add(new Diff(max));
            return queued;
        }
        void startForwarding(int follower, long lastSeen, List<MpMessage> queue) {
            for (Txn t : toBeApplied) if (t.zxid() > lastSeen) {
                queue.add(new Proposal(t)); queue.add(new Commit(t.zxid()));
            }
            for (Outstanding p : outstanding.values()) if (p.txn.zxid() > lastSeen)
                queue.add(new Proposal(p.txn));
            // peers[follower].synced is installed atomically with the initial queue.
        }
        void waitForNewLeaderAck(Ack a) {
            require(a.zxid() == NEW_EPOCH, "wrong NEWLEADER ACK");
            peers[a.sid()].newAck = true; newAcks.add(a.sid());
            if (newAcks.size() >= 2 && !quorumFormed) {
                quorumFormed = true; started = true;
                db.lastProcessed = NEW_EPOCH; // Leader.startZkServer
            }
            if (started) for (int i = 1; i < 3; i++) {
                Peer p = peers[i];
                if (p.newAck && !p.uptodate) {
                    send(nodes[i], new Uptodate()); p.uptodate = true;
                }
            }
            updateServing();
        }
        void updateServing() {
            serving = started && (late == 1 || (peers[1].startupAck && peers[2].startupAck));
        }
        void processRequest(int value) {
            long z = ++lastProposed;
            require(counter(z) != 0xffffffffL && epoch(z) == EPOCH, "zxid rollover requires election");
            Txn t = new Txn(z, value);
            outstanding.put(z, new Outstanding(t));
            broadcast(new Proposal(t));
            toLog.addLast(t);
        }
        void broadcast(MpMessage m) {
            for (int i = 1; i < 3; i++) if (peers[i].synced) send(nodes[i], m);
        }
        void logged(Txn t) { processAck(0, t.zxid()); }
        void processAck(int from, long z) {
            if (counter(z) == 0 || z <= lastCommitted) return;
            Outstanding p = outstanding.get(z);
            if (p == null) return;
            p.acks.add(from);
            tryToCommit(p);
            // Source only retries subsequent proposals for reconfiguration, which is dropped.
        }
        void tryToCommit(Outstanding p) {
            long z = p.txn.zxid();
            if (outstanding.containsKey(z - 1) || p.acks.size() < 2) return;
            outstanding.remove(z);
            toBeApplied.addLast(p.txn);
            lastCommitted = z;
            broadcast(new Commit(z));
            toApply.addLast(p.txn);
        }
        void delivered(Txn t) {
            require(toBeApplied.removeFirst().equals(t), "leader apply queue mismatch");
            if (late == 1 && !sentStart) {
                sentStart = true;
                send(nodes[2], new Start());
            }
        }
    }

    final class Follower extends Server {
        final Deque<Txn> pendingTxns = new ArrayDeque<>(), packetsNotLogged = new ArrayDeque<>();
        final Deque<Long> packetsCommitted = new ArrayDeque<>();
        long acceptedEpoch, currentEpoch, syncLastQueued, lastQueued;
        int stage; // 0: LEADERINFO; 1: DIFF/TRUNC/SNAP; 2: sync; 3: broadcast
        boolean snapshotNeeded, writeToTxnLog, newLeaderSeen;
        Follower(int sid, Tid[] nodes, Tid[] clients) { super(sid, nodes, clients); }
        void followLeader() {
            if (late == 1 && sid == 2) Channel.recv(communication, Start.class);
            registerWithLeader();
            loop();
        }
        void registerWithLeader() {
            send(nodes[0], new FollowerInfo(sid, acceptedEpoch, 0x10000));
        }
        void packet(MpMessage m) {
            if (m instanceof Propose p) { requests.addLast(p.value()); return; }
            if (stage == 0) {
                require(m instanceof LeaderInfo, "expected LEADERINFO, received " + m);
                LeaderInfo l = (LeaderInfo) m;
                require(epoch(l.zxid()) >= acceptedEpoch && l.version() == 0x10000, "leader epoch/version");
                long ackEpoch = epoch(l.zxid()) > acceptedEpoch ? currentEpoch : -1;
                acceptedEpoch = epoch(l.zxid());
                send(nodes[0], new AckEpoch(sid, ackEpoch, db.lastLogged()));
                stage = 1;
            } else if (stage == 1) {
                if (m instanceof Diff) snapshotNeeded = false;
                else if (m instanceof Trunc t) { snapshotNeeded = true; db.truncateLog(t.zxid()); }
                else if (m instanceof Snap s) { snapshotNeeded = true; db.install(s); }
                else require(false, "expected DIFF/TRUNC/SNAP, received " + m);
                writeToTxnLog = !snapshotNeeded;
                stage = 2;
            } else if (stage == 2) syncWithLeader(m);
            else processPacket(m);
        }
        long enforceContinuousProposal(long previous, Txn t) {
            require(previous == 0 || t.zxid() == previous + 1 || epoch(t.zxid()) > epoch(previous),
                    "noncontinuous sync proposal");
            return t.zxid();
        }
        void syncWithLeader(MpMessage m) {
            if (m instanceof Proposal p) {
                syncLastQueued = enforceContinuousProposal(syncLastQueued, p.txn());
                packetsNotLogged.addLast(p.txn());
            } else if (m instanceof Commit c) {
                require(!packetsNotLogged.isEmpty(), "sync COMMIT without proposal");
                if (writeToTxnLog) packetsCommitted.addLast(c.zxid());
                else if (packetsNotLogged.getFirst().zxid() == c.zxid()) apply(packetsNotLogged.removeFirst());
            } else if (m instanceof NewLeader n) {
                require(!newLeaderSeen && n.zxid() == NEW_EPOCH, "bad NEWLEADER");
                // Persist/apply historical committed proposals before NEWLEADER ACK;
                // do not ACK each historical proposal. Keep outstanding proposals.
                while (!packetsCommitted.isEmpty()) {
                    long z = packetsCommitted.removeFirst();
                    if (packetsNotLogged.isEmpty() || packetsNotLogged.getFirst().zxid() != z) continue;
                    Txn t = packetsNotLogged.removeFirst();
                    db.append(t); apply(t);
                }
                writeToTxnLog = true; currentEpoch = epoch(n.zxid()); newLeaderSeen = true;
                send(nodes[0], new Ack(sid, n.zxid()));
            } else if (m instanceof Uptodate) {
                require(newLeaderSeen, "UPTODATE before NEWLEADER");
                send(nodes[0], new Ack(sid, NEW_EPOCH));
                serving = true;
                while (!packetsNotLogged.isEmpty()) logRequest(packetsNotLogged.removeFirst());
                while (!packetsCommitted.isEmpty()) commit(packetsCommitted.removeFirst());
                stage = 3;
            } else require(false, "unexpected sync packet " + m);
        }
        void processPacket(MpMessage m) {
            if (m instanceof Proposal p) {
                // Follower.processPacket only WARNS on discontinuity; no reorder buffer.
                lastQueued = p.txn().zxid();
                logRequest(p.txn());
            } else if (m instanceof Commit c) commit(c.zxid());
            else if (m instanceof Uptodate) { /* source logs an error and continues */ }
            else require(false, "unexpected broadcast packet " + m);
        }
        void logRequest(Txn t) {
            if (counter(t.zxid()) != 0) pendingTxns.addLast(t);
            toLog.addLast(t);
        }
        void logged(Txn t) { send(nodes[0], new Ack(sid, t.zxid())); }
        void commit(long z) {
            if (pendingTxns.isEmpty()) return; // source warns and discards this COMMIT
            require(pendingTxns.getFirst().zxid() == z, "COMMIT does not match pendingTxns head");
            toApply.addLast(pendingTxns.removeFirst());
        }
        void processRequest(int value) { send(nodes[0], new Request(sid, value)); }
    }
}
