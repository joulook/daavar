package bench.zookeeper_zab;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

/**
 * ZooKeeper 3.9's atomic broadcast (Zab), transliterated to JMC's message-passing API under the
 * fixed-leader program interface (plan/case-studies/zookeeper/INTERFACE.md).
 *
 * <p>Class and method names are ZooKeeper's (org.apache.zookeeper.server[.quorum]); each method
 * keeps the source's control flow and conditions in the source's order. A line changed from the
 * source carries a {@code // PORT:} comment with the reason. Dropped without a per-line mark, as the
 * interface and the cut's README direct: metrics (ServerMetrics), JMX beans, tracing, SASL/SSL,
 * observers, reconfiguration, sessions and PING, and every timer or timeout. Log statements that
 * mark a branch are kept as calls of the no-op {@link LOG}. NOTES.md lists every decision.
 *
 * <p>Threads. A ZooKeeper server is many threads (QuorumPeer, the LearnerCnxAcceptor, one
 * LearnerHandler and one sender thread per learner, and the request-processor threads); here each
 * server is one JMC process that schedules them itself ({@link #nextEvent}): a thread runs as a
 * step from one blocking point to the next, and the process chooses, with a non-blocking receive
 * and Nondet, between reading a pending message and running one enabled step. The leader's
 * blocking waits (getEpochToPropose, waitForEpochAck, waitForNewLeaderAck, waitForStartup) are
 * written in continuation-passing style over {@link Threads}: a parked continuation becomes an
 * enabled step when notified, and so does the notifier's own rest after it leaves the monitor.
 * NOTES.md lists what is still collapsed ("Fix round 1").
 */
public final class ZookeeperZab implements MpProgram {

    /** N = 3 servers n0, n1, n2. */
    static final int N = 3;
    /** K = 2 clients c0, c1; ci is connected to server ni. */
    static final int CLIENTS = 2;
    /** The fixed leader n0 (INTERFACE.md). */
    static final long LEADER_ID = 0;

    private final int e;
    private final int late;
    private final CommunicationModel model;
    private final String modelName;

    /** Diagnostics only (printed as "observed:"): never read by a process. */
    private final AtomicLong runs = new AtomicLong();
    private final AtomicLong clientsDone = new AtomicLong();

    /**
     * Constructed from the launcher's argument string {@code impl,<e>,<late>,<fifo|bag>}.
     *
     * @param args the four comma-separated arguments
     */
    public ZookeeperZab(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<late>,<fifo|bag>, got " + String.join(",", args));
        }
        this.e = Integer.parseInt(args[1]);
        this.late = Integer.parseInt(args[2]);
        if (e < 1) {
            throw new IllegalArgumentException("e must be >= 1, was " + e);
        }
        if (late != 0 && late != 1) {
            throw new IllegalArgumentException("late must be 0 or 1, was " + late);
        }
        this.modelName = args[3];
        if (args[3].equals("fifo")) {
            this.model = CommunicationModel.P2P;
        } else if (args[3].equals("bag")) {
            this.model = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("model must be fifo or bag, was " + args[3]);
        }
    }

    @Override
    public String programName() {
        return "zookeeper-zab";
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
    public String toString() {
        return "ZookeeperZab(e=" + e + ", late=" + late + ", " + modelName + "): " + runs.get()
                + " executions started, " + clientsDone.get() + " client completions (" + CLIENTS
                + " per execution when every client saw its values)";
    }

    /**
     * Main: starts the servers in index order, then the clients in index order, and waits for them;
     * it sends and receives nothing. Servers never end, so the execution ends blocked.
     */
    @Override
    public void run() {
        runs.incrementAndGet();
        // Every process knows every address from the start (INTERFACE.md). The k-th process main
        // spawns is task k + 1 (MpProgram), i.e. main's own task id + k; checked after each spawn.
        long first = MpProcess.self().raw() + 1;
        Tid[] nodes = new Tid[N];
        for (int i = 0; i < N; i++) {
            nodes[i] = Tid.of(first + i, "n" + i);
        }
        Tid[] clients = new Tid[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            clients[i] = Tid.of(first + N + i, "c" + i);
        }
        List<MpProcess.Handle> servers = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int id = i;
            final Tid client = id < CLIENTS ? clients[id] : null;
            MpProcess.Handle h = MpProcess.spawn("n" + i,
                    () -> new QuorumPeer(id, nodes, client, model, late).run());
            expect(h, nodes[i]);
            servers.add(h);
        }
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            final int id = i;
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> {
                client(id, e, nodes[id], model);
                clientsDone.incrementAndGet();
            });
            expect(h, clients[i]);
            cs.add(h);
        }
        for (MpProcess.Handle h : cs) {
            h.await();
        }
        for (MpProcess.Handle h : servers) {
            h.await();
        }
    }

    private static void expect(MpProcess.Handle h, Tid expected) {
        if (h.tid().raw() != expected.raw()) {
            throw new IllegalStateException("process " + expected.role() + " got task " + h.tid().raw()
                    + ", expected " + expected.raw() + ": the precomputed addresses are wrong");
        }
    }

    // ------------------------------------------------------------------------------------------
    // Clients (INTERFACE.md, the Rust client verbatim)
    // ------------------------------------------------------------------------------------------

    static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    static void client(int i, int e, Tid server, CommunicationModel model) {
        List<Integer> mine = new ArrayList<>();
        for (int j = 0; j < e; j++) {
            mine.add(value(i, j));
        }
        for (int v : mine) {
            Channel.send(model, server, new Propose(v));
        }
        int seen = 0;
        while (seen < e) {
            Committed c = Channel.recv(model, Committed.class);
            if (mine.contains(c.value())) {
                seen += 1;
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Messages
    // ------------------------------------------------------------------------------------------

    /** Client to its server. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** Server to its client, for every transaction the server applies, in order. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** n0 to n2 after n0 delivered its first transaction, when late = 1 (INTERFACE.md). */
    @Tag("Start")
    public record Start() implements MpMessage {}

    /**
     * org.apache.zookeeper.server.quorum.QuorumPacket(type, zxid, data, authinfo). PORT: sid and
     * cnx name the TCP connection a packet travels on (the learner's server id and the learner's
     * connection number); authinfo is dropped.
     */
    @Tag("QuorumPacket")
    public record QuorumPacket(int sid, int cnx, int type, long zxid, PacketData data) implements MpMessage {
        /** The same packet on connection cnx (a leader-side packet is stamped when it is queued). */
        QuorumPacket on(int cnx) {
            return new QuorumPacket(sid, cnx, type, zxid, data);
        }
    }

    /** The byte[] payload of a QuorumPacket, as typed records (PORT: no serialization). */
    public sealed interface PacketData permits NoData, LearnerInfo, IntData, TxnLogEntry, RequestData, Snapshot {}

    /** data == null. */
    @Tag("NoData")
    public record NoData() implements PacketData {}

    /** org.apache.zookeeper.server.quorum.LearnerInfo (FOLLOWERINFO's payload). */
    @Tag("LearnerInfo")
    public record LearnerInfo(long serverid, int protocolVersion, long configVersion) implements PacketData {}

    /** A 4-byte int payload: LEADERINFO's protocol version, ACKEPOCH's current epoch. */
    @Tag("IntData")
    public record IntData(int value) implements PacketData {}

    /** org.apache.zookeeper.txn.TxnHeader(clientId, cxid, zxid, time, type); PORT: no time. */
    @Tag("TxnHeader")
    public record TxnHeader(long clientId, int cxid, long zxid, int type) {}

    /** org.apache.zookeeper.server.TxnLogEntry (PROPOSAL's payload); txn is the SetDataTxn's value. */
    @Tag("TxnLogEntry")
    public record TxnLogEntry(TxnHeader header, int txn) implements PacketData {}

    /** REQUEST's payload: sessionId, cxid, type, then the request record (the value). */
    @Tag("RequestData")
    public record RequestData(long sessionId, int cxid, int type, int value) implements PacketData {}

    /** The snapshot streamed after SNAP: the in-memory data tree (PORT: carried in SNAP's data). */
    @Tag("Snapshot")
    public record Snapshot(List<TxnLogEntry> nodes) implements PacketData {}

    // ------------------------------------------------------------------------------------------
    // Constants and helpers
    // ------------------------------------------------------------------------------------------

    /** Packet types, from Leader. */
    static final int REQUEST = 1;
    static final int PROPOSAL = 2;
    static final int ACK = 3;
    static final int COMMIT = 4;
    static final int PING = 5;
    static final int REVALIDATE = 6;
    static final int SYNC = 7;
    static final int INFORM = 8;
    static final int COMMITANDACTIVATE = 9;
    static final int NEWLEADER = 10;
    static final int FOLLOWERINFO = 11;
    static final int UPTODATE = 12;
    static final int DIFF = 13;
    static final int TRUNC = 14;
    static final int SNAP = 15;
    static final int OBSERVERINFO = 16;
    static final int LEADERINFO = 17;
    static final int ACKEPOCH = 18;
    static final int INFORMANDACTIVATE = 19;
    /** PORT: not a ZooKeeper packet: the end of a TCP connection (socket.close() / EOF). */
    static final int CLOSE = -1;

    /** org.apache.zookeeper.ZooDefs.OpCode, the codes used here. */
    static final class OpCode {
        static final int setData = 5;
        static final int sync = 9;
        static final int reconfig = 16;
    }

    /** org.apache.zookeeper.server.util.ZxidUtils. */
    static final class ZxidUtils {
        static long getEpochFromZxid(long zxid) {
            return zxid >> 32L;
        }

        static long getCounterFromZxid(long zxid) {
            return zxid & 0xffffffffL;
        }

        static long makeZxid(long epoch, long counter) {
            return (epoch << 32L) | (counter & 0xffffffffL);
        }
    }

    /** PORT: logging is a no-op; the calls are kept to mark the source's branches. */
    static final class LOG {
        static void info(String s) {}

        static void warn(String s) {}

        static void error(String s) {}

        static void debug(String s) {}
    }

    /** PORT: java.io.IOException and friends on the follower side, as a checked exception. */
    static final class ZabException extends Exception {
        private static final long serialVersionUID = 1L;

        ZabException(String m) {
            super(m);
        }
    }

    /** PORT: an IOException thrown inside a LearnerHandler thread; ends that handler only. */
    static final class ZabIOException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ZabIOException(String m) {
            super(m);
        }
    }

    /**
     * PORT: an exception the source throws as a RuntimeException that the follower's catch
     * (Exception) in followLeader handles (IllegalStateException of addCommittedProposal).
     */
    static final class ZabStateException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ZabStateException(String m) {
            super(m);
        }
    }

    /**
     * PORT: ServiceUtils.requestSystemExit (and the critical-thread exit of a request processor
     * that throws): the server's JVM exits. Thrown to QuorumPeer.run, which ends the process
     * normally (the server is gone; nothing else runs on it).
     */
    static final class SystemExit extends Error {
        private static final long serialVersionUID = 1L;

        SystemExit(String m) {
            super(m, null, false, false);
        }
    }

    static SystemExit requestSystemExit(String why) {
        return new SystemExit("ServiceUtils.requestSystemExit: " + why);
    }

    /**
     * PORT: the threads of one server process. A blocked Java thread becomes a continuation parked
     * on a monitor; notifyAll() makes it ready; each ready continuation is a separately scheduled
     * step (see {@link #nextEvent}), as is the notifier's own continuation after it leaves the
     * monitor ({@link #resume}).
     */
    static final class Threads {
        final List<Runnable> ready = new ArrayList<>();

        final class Monitor {
            final List<Runnable> waiters = new ArrayList<>();

            void notifyAllWaiters() {
                ready.addAll(waiters);
                waiters.clear();
            }
        }

        Monitor monitor() {
            return new Monitor();
        }

        /** {@code while (cond) monitor.wait(); k} with the timeouts dropped. */
        void waitWhile(Monitor m, BooleanSupplier cond, Runnable k) {
            if (cond.getAsBoolean()) {
                m.waiters.add(() -> waitWhile(m, cond, k));
            } else {
                k.run();
            }
        }

        /** PORT: the calling thread leaves a synchronized block after notifyAll(): its rest is a step. */
        void resume(Runnable k) {
            ready.add(k);
        }

        /** The ready continuations, each as a step that removes itself before it runs. */
        List<Runnable> readySteps() {
            List<Runnable> steps = new ArrayList<>();
            for (Runnable r : ready) {
                steps.add(() -> {
                    ready.remove(r);
                    r.run();
                });
            }
            return steps;
        }
    }

    /**
     * PORT: one scheduling decision of a server process. With no enabled internal step, the
     * process blocks on a receive. Otherwise a non-blocking receive either reads a pending
     * message (returned) or reads nothing, and then one of the enabled steps, chosen with
     * Nondet, runs (null is returned). A step is a thread's run from one blocking point (a socket
     * read, a queue take, a monitor wait, leaving a monitor it notified) to its next one.
     */
    static MpMessage nextEvent(CommunicationModel model, Predicate<MpMessage> guard, List<Runnable> enabled) {
        if (enabled.isEmpty()) {
            return Channel.recv(model, guard);
        }
        Optional<MpMessage> m = Channel.tryRecv(model, guard);
        if (m.isPresent()) {
            return m.get();
        }
        enabled.get(Nondet.chooseIndex(enabled.size())).run();
        return null;
    }

    // ------------------------------------------------------------------------------------------
    // Quorum helpers: StateSummary, QuorumMaj, SyncedLearnerTracker, Proposal
    // ------------------------------------------------------------------------------------------

    /** org.apache.zookeeper.server.quorum.StateSummary. */
    static final class StateSummary {
        private final long currentEpoch;
        private final long lastZxid;

        StateSummary(long currentEpoch, long lastZxid) {
            this.currentEpoch = currentEpoch;
            this.lastZxid = lastZxid;
        }

        long getCurrentEpoch() {
            return currentEpoch;
        }

        long getLastZxid() {
            return lastZxid;
        }

        boolean isMoreRecentThan(StateSummary ss) {
            return (currentEpoch > ss.currentEpoch) || (currentEpoch == ss.currentEpoch && lastZxid > ss.lastZxid);
        }
    }

    /** org.apache.zookeeper.server.quorum.flexible.QuorumMaj over the voters 0 .. n-1. */
    static final class QuorumMaj {
        final Set<Long> votingMembers = new HashSet<>();
        final int half;

        QuorumMaj(int n) {
            for (long i = 0; i < n; i++) {
                votingMembers.add(i);
            }
            half = votingMembers.size() / 2;
        }

        boolean containsQuorum(Set<Long> ackSet) {
            return (ackSet.size() > half);
        }

        long getVersion() {
            return 0; // PORT: no dynamic reconfiguration, the configuration has version 0
        }
    }

    /** org.apache.zookeeper.server.quorum.SyncedLearnerTracker. */
    static class SyncedLearnerTracker {
        protected final ArrayList<QuorumVerifierAcksetPair> qvAcksetPairs = new ArrayList<>();

        void addQuorumVerifier(QuorumMaj qv) {
            qvAcksetPairs.add(new QuorumVerifierAcksetPair(qv, new HashSet<>(qv.votingMembers.size())));
        }

        boolean addAck(Long sid) {
            boolean change = false;
            for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
                if (qvAckset.qv.votingMembers.contains(sid)) {
                    qvAckset.ackset.add(sid);
                    change = true;
                }
            }
            return change;
        }

        boolean hasAllQuorums() {
            for (QuorumVerifierAcksetPair qvAckset : qvAcksetPairs) {
                if (!qvAckset.qv.containsQuorum(qvAckset.ackset)) {
                    return false;
                }
            }
            return true;
        }

        static final class QuorumVerifierAcksetPair {
            final QuorumMaj qv;
            final HashSet<Long> ackset;

            QuorumVerifierAcksetPair(QuorumMaj qv, HashSet<Long> ackset) {
                this.qv = qv;
                this.ackset = ackset;
            }
        }
    }

    /** Leader.Proposal. */
    static class Proposal extends SyncedLearnerTracker {
        QuorumPacket packet;
        Request request;

        Proposal() {}

        Proposal(QuorumPacket packet) {
            this.packet = packet;
        }

        Proposal(Request request, QuorumPacket packet) {
            this.request = request;
            this.packet = packet;
        }

        QuorumPacket getQuorumPacket() {
            return packet;
        }

        long getZxid() {
            return packet.zxid();
        }
    }

    /** Leader.PureRequestProposal (the entries of ZKDatabase.committedLog). */
    static final class PureRequestProposal extends Proposal {
        private final int sid;

        PureRequestProposal(Request request, int sid) {
            this.request = request;
            this.sid = sid;
        }

        @Override
        QuorumPacket getQuorumPacket() {
            TxnLogEntry data = request.getSerializeData();
            return new QuorumPacket(sid, 0, PROPOSAL, request.zxid, data);
        }

        @Override
        long getZxid() {
            return request.zxid;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Request, DataTree, ZKDatabase
    // ------------------------------------------------------------------------------------------

    /** org.apache.zookeeper.server.Request, the fields used here. */
    static final class Request {
        final long sessionId;
        final int cxid;
        final int type;
        /** The request record (a SetDataRequest's value). */
        final int requestValue;
        long zxid = -1;
        private TxnHeader hdr;
        /** The txn (a SetDataTxn's value); meaningful when hdr != null. */
        private int txn;
        Object owner;
        /** PORT: request.cnxn != null, i.e. the client is connected to this server. */
        boolean hasCnxn;

        Request(long sessionId, int cxid, int type, int requestValue) {
            this.sessionId = sessionId;
            this.cxid = cxid;
            this.type = type;
            this.requestValue = requestValue;
        }

        Request(TxnHeader hdr, int txn) {
            this.sessionId = hdr.clientId();
            this.cxid = hdr.cxid();
            this.type = hdr.type();
            this.requestValue = txn;
            this.hdr = hdr;
            this.txn = txn;
            this.zxid = hdr.zxid();
        }

        TxnHeader getHdr() {
            return hdr;
        }

        void setHdr(TxnHeader hdr) {
            this.hdr = hdr;
        }

        int getTxn() {
            return txn;
        }

        void setTxn(int txn) {
            this.txn = txn;
        }

        void setOwner(Object owner) {
            this.owner = owner;
        }

        TxnLogEntry getSerializeData() {
            return new TxnLogEntry(hdr, txn);
        }

        boolean isQuorum() {
            return type == OpCode.setData; // PORT: the only write opcode used
        }
    }

    /** org.apache.zookeeper.server.DataTree, as the list of applied txns. */
    static final class DataTree {
        final List<TxnLogEntry> nodes = new ArrayList<>();
        volatile long lastProcessedZxid = 0;

        void processTxn(TxnHeader header, int txn) {
            nodes.add(new TxnLogEntry(header, txn)); // PORT: setData on the one znode, recorded as a list
            if (header.zxid() > lastProcessedZxid) {
                lastProcessedZxid = header.zxid();
            }
        }
    }

    /** org.apache.zookeeper.server.ZKDatabase over an in-memory txn log and data tree. */
    static final class ZKDatabase {
        private final int sid;
        DataTree dataTree = new DataTree();
        long minCommittedLog;
        long maxCommittedLog;
        static final double DEFAULT_SNAPSHOT_SIZE_FACTOR = 0.33;
        final double snapshotSizeFactor = DEFAULT_SNAPSHOT_SIZE_FACTOR;
        static final int DEFAULT_COMMIT_LOG_COUNT = 500;
        final int commitLogCount = DEFAULT_COMMIT_LOG_COUNT;
        final ArrayDeque<Proposal> committedLog = new ArrayDeque<>();
        private final boolean allowDiscontinuousProposals = false;
        private boolean initialized = false;
        /** PORT: FileTxnSnapLog's transaction log, in memory (one log "file"). */
        final List<TxnLogEntry> txnLog = new ArrayList<>();

        ZKDatabase(int sid) {
            this.sid = sid;
        }

        boolean isInitialized() {
            return initialized;
        }

        void clear() {
            dataTree = new DataTree();
            committedLog.clear();
            minCommittedLog = 0;
            maxCommittedLog = 0;
            initialized = false;
        }

        long getmaxCommittedLog() {
            return maxCommittedLog;
        }

        long getminCommittedLog() {
            return minCommittedLog;
        }

        Collection<Proposal> getCommittedLog() {
            return Collections.unmodifiableCollection(new ArrayList<>(this.committedLog));
        }

        long getDataTreeLastProcessedZxid() {
            return dataTree.lastProcessedZxid;
        }

        /** PORT: snapLog.restore replays the in-memory txn log on an empty tree (no snapshot files). */
        long loadDataBase() {
            for (TxnLogEntry t : txnLog) {
                dataTree.processTxn(t.header(), t.txn());
                addCommittedProposal(new Request(t.header(), t.txn()));
            }
            initialized = true;
            return dataTree.lastProcessedZxid;
        }

        void addCommittedProposal(Request request) {
            if (committedLog.isEmpty()) {
                minCommittedLog = request.zxid;
                maxCommittedLog = request.zxid;
            } else if (request.zxid <= maxCommittedLog) {
                return;
            } else if (!allowDiscontinuousProposals
                    && request.zxid != maxCommittedLog + 1
                    && ZxidUtils.getEpochFromZxid(request.zxid) <= ZxidUtils.getEpochFromZxid(maxCommittedLog)) {
                throw new ZabStateException("Committed proposal cached out of order: 0x"
                        + Long.toHexString(request.zxid) + " is not the next proposal of 0x"
                        + Long.toHexString(maxCommittedLog));
            }
            PureRequestProposal p = new PureRequestProposal(request, sid);
            committedLog.add(p);
            maxCommittedLog = p.getZxid();
            if (committedLog.size() > commitLogCount) {
                committedLog.remove();
                minCommittedLog = committedLog.peek().getZxid();
            }
        }

        boolean isTxnLogSyncEnabled() {
            boolean enabled = snapshotSizeFactor >= 0;
            return enabled;
        }

        long calculateTxnLogSizeLimit() {
            long snapSize = 0; // PORT: no snapshot file on disk, so the limit is 0 (unlimited)
            return (long) (snapSize * snapshotSizeFactor);
        }

        /**
         * PORT: snapLog.readTxnLog(startZxid, false) over the in-memory log: the iterator starts at
         * the first logged txn (no fast-forward), and is refused when that txn is later than
         * startZxid, as FileTxnIterator is.
         */
        Iterator<Proposal> getProposalsFromTxnLog(long startZxid, long sizeLimit) {
            if (sizeLimit < 0) {
                return Collections.emptyIterator();
            }
            TxnHeader header = txnLog.isEmpty() ? null : txnLog.get(0).header();
            if ((header != null) && (header.zxid() > startZxid)) {
                LOG.warn("Unable to find proposals from txnlog for zxid: 0x" + Long.toHexString(startZxid));
                return Collections.emptyIterator();
            }
            if (sizeLimit > 0) {
                long txnSize = txnLog.size(); // PORT: storage size counted in entries
                if (txnSize > sizeLimit) {
                    return Collections.emptyIterator();
                }
            }
            List<Proposal> ps = new ArrayList<>();
            for (TxnLogEntry t : txnLog) {
                ps.add(new Proposal(new QuorumPacket(sid, 0, PROPOSAL, t.header().zxid(), t)));
            }
            return ps.iterator();
        }

        void setlastProcessedZxid(long zxid) {
            dataTree.lastProcessedZxid = zxid;
        }

        /**
         * ZKDatabase.fastForwardDataBase → FileTxnSnapLog.fastForwardFromEdits: apply the logged
         * txns after lastProcessedZxid to the data tree and the committedLog. PORT: zks is told of
         * each application (the interface's Committed notification, see ZooKeeperServer.delivered).
         */
        long fastForwardDataBase(ZooKeeperServer zks) {
            for (TxnLogEntry t : new ArrayList<>(txnLog)) {
                if (t.header().zxid() <= dataTree.lastProcessedZxid) {
                    continue;
                }
                dataTree.processTxn(t.header(), t.txn());
                Request r = new Request(t.header(), t.txn());
                addCommittedProposal(r);
                zks.delivered(r);
            }
            initialized = true;
            return dataTree.lastProcessedZxid;
        }

        void processTxn(TxnHeader hdr, int txn) {
            dataTree.processTxn(hdr, txn);
        }

        /** PORT: snapLog.truncateLog drops the in-memory log entries after zxid. */
        boolean truncateLog(long zxid) {
            clear();
            txnLog.removeIf(t -> t.header().zxid() > zxid);
            loadDataBase();
            return true;
        }

        /** PORT: the snapshot is the in-memory data tree. */
        void deserializeSnapshot(Snapshot snapshot) {
            clear();
            for (TxnLogEntry t : snapshot.nodes()) {
                dataTree.nodes.add(t);
            }
            initialized = true;
        }

        Snapshot serializeSnapshot() {
            return new Snapshot(List.copyOf(dataTree.nodes));
        }

        boolean append(Request si) {
            if (si.getHdr() == null) {
                return false; // FileTxnLog.append: no header, nothing logged
            }
            txnLog.add(new TxnLogEntry(si.getHdr(), si.getTxn()));
            return true;
        }

        void commit() {
            // PORT: the in-memory log needs no flush
        }
    }

    // ------------------------------------------------------------------------------------------
    // QuorumPeer
    // ------------------------------------------------------------------------------------------

    enum ServerState { LOOKING, FOLLOWING, LEADING }

    enum ZabState { ELECTION, DISCOVERY, SYNCHRONIZATION, BROADCAST }

    enum LearnerType { PARTICIPANT, OBSERVER }

    enum SyncMode { NONE, DIFF, SNAP, TRUNC }

    /** org.apache.zookeeper.server.quorum.QuorumPeer: the run() state loop and the epochs. */
    static final class QuorumPeer {
        final long myid;
        final Tid[] nodes;
        /** PORT: the client connected to this server, or null (n2). */
        final Tid client;
        final CommunicationModel model;
        final int late;
        private long acceptedEpoch = 0;
        private long currentEpoch = 0;
        final ZKDatabase zkDb;
        final QuorumMaj quorumVerifier = new QuorumMaj(N);
        ServerState state = ServerState.LOOKING;
        ZabState zabState = ZabState.ELECTION;
        Follower follower;
        Leader leader;
        private SyncMode syncMode = SyncMode.NONE;
        /** PORT: the number of this learner's next connection to the leader (connection identity). */
        int nextCnx = 1;
        /** PORT: the cxid of the client's next request (assigned by the server, see NOTES.md D11). */
        int nextCxid = 1;
        /** PORT: the late joiner has received Start. */
        private boolean started = false;

        QuorumPeer(int myid, Tid[] nodes, Tid client, CommunicationModel model, int late) {
            this.myid = myid;
            this.nodes = nodes;
            this.client = client;
            this.model = model;
            this.late = late;
            this.zkDb = new ZKDatabase(myid);
        }

        long getMyId() {
            return myid;
        }

        long getAcceptedEpoch() {
            return acceptedEpoch;
        }

        void setAcceptedEpoch(long e) {
            acceptedEpoch = e;
        }

        long getCurrentEpoch() {
            return currentEpoch;
        }

        void setCurrentEpoch(long e) {
            currentEpoch = e;
        }

        QuorumMaj getQuorumVerifier() {
            return quorumVerifier;
        }

        void setZabState(ZabState s) {
            zabState = s;
        }

        boolean isRunning() {
            return true; // PORT: no shutdown
        }

        SyncMode getSyncMode() {
            return syncMode;
        }

        void setSyncMode(SyncMode m) {
            syncMode = m;
        }

        long getLastLoggedZxid() {
            if (!zkDb.isInitialized()) {
                zkDb.loadDataBase();
            }
            return zkDb.getDataTreeLastProcessedZxid();
        }

        void send(Tid dst, MpMessage m) {
            Channel.send(model, dst, m);
        }

        /** A leader-side packet; its connection (0 here) is stamped when it is queued or written. */
        QuorumPacket packet(int type, long zxid, PacketData data) {
            return new QuorumPacket((int) myid, 0, type, zxid, data);
        }

        /** QuorumPeer.run. */
        void run() {
            try {
                runLoop();
            } catch (SystemExit exit) {
                // PORT: the server's JVM exited; the process ends (main's await returns)
                LOG.error(exit.getMessage());
            }
        }

        private void runLoop() {
            zkDb.loadDataBase(); // QuorumPeer.start → loadDataBase (empty)
            while (isRunning()) {
                switch (state) {
                    case LOOKING:
                        // PORT: the election is replaced by the interface's fixed vote: n0 leads
                        // epoch 1. It is also the outcome of every later LOOKING round: no server
                        // fails, so n0 keeps leading and a follower that left goes back to it.
                        state = myid == LEADER_ID ? ServerState.LEADING : ServerState.FOLLOWING;
                        break;
                    case FOLLOWING:
                        if (late == 1 && myid == N - 1 && !started) {
                            // PORT: the late joiner starts its learner protocol only after Start from n0.
                            Channel.recv(model, Start.class);
                            started = true;
                        }
                        // try { ... } catch (Exception e) { LOG.warn } finally { ... }: followLeader
                        // catches its own exceptions; PORT: the finally block runs on normal return
                        // only (it sends, and must not run while JMC unwinds a blocked process)
                        follower = makeFollower();
                        follower.followLeader();
                        follower.shutdown();
                        follower = null;
                        updateServerState();
                        break;
                    case LEADING:
                        leader = makeLeader();
                        leader.lead();
                        leader.serve(); // PORT: the leader process's receive loop; never returns
                        throw new AssertionError("lead returned");
                    default:
                        throw new AssertionError(state.toString());
                }
            }
        }

        /** QuorumPeer.updateServerState (reconfigFlag is never set). */
        private void updateServerState() {
            state = ServerState.LOOKING;
        }

        Follower makeFollower() {
            return new Follower(this, new FollowerZooKeeperServer(this, this.zkDb));
        }

        Leader makeLeader() {
            return new Leader(this, new LeaderZooKeeperServer(this, this.zkDb));
        }
    }

    // ------------------------------------------------------------------------------------------
    // Request processors
    // ------------------------------------------------------------------------------------------

    interface RequestProcessor {
        void processRequest(Request request);
    }

    /** org.apache.zookeeper.server.ZooKeeperServer, the part both server kinds share. */
    abstract static class ZooKeeperServer {
        final QuorumPeer self;
        final ZKDatabase zkDb;
        private long hzxid = 0;
        boolean running = false;
        CommitProcessor commitProcessor;
        RequestProcessor firstProcessor;

        ZooKeeperServer(QuorumPeer self, ZKDatabase zkDb) {
            this.self = self;
            this.zkDb = zkDb;
        }

        ZKDatabase getZKDatabase() {
            return zkDb;
        }

        long getZxid() {
            return hzxid;
        }

        void setZxid(long zxid) {
            hzxid = zxid;
        }

        long getNextZxid() {
            return ++hzxid;
        }

        void loadData() {
            if (zkDb.isInitialized()) {
                setZxid(zkDb.getDataTreeLastProcessedZxid());
            } else {
                setZxid(zkDb.loadDataBase());
            }
        }

        boolean isRunning() {
            return running;
        }

        /** PORT: no upgrade from a pre-3.4 data directory. */
        boolean shouldForceWriteInitialSnapshotAfterLeaderElection() {
            return false;
        }

        /** PORT: the snapshot is the in-memory data tree; nothing to write. */
        void takeSnapshot(boolean syncSnap) {
        }

        abstract void setupRequestProcessors();

        void startup() {
            setupRequestProcessors();
            running = true;
        }

        /**
         * ZooKeeperServer.shutdown(fullyShutDown). PORT: the request processors stop with this
         * object (its queues are no longer scheduled); fastForwardDataBase applies the logged
         * txns beyond lastProcessedZxid.
         */
        void shutdown(boolean fullyShutDown) {
            if (running) { // canShutdown()
                running = false; // shutdownComponents()
                if (!fullyShutDown) {
                    zkDb.fastForwardDataBase(this);
                }
            }
            if (fullyShutDown) {
                zkDb.clear();
            }
        }

        /** PORT: a client's Propose{value} as a setData request of its session (sessionId = myid + 1). */
        void submitRequest(int value) {
            Request si = new Request(self.getMyId() + 1, self.nextCxid++, OpCode.setData, value);
            si.hasCnxn = true;
            firstProcessor.processRequest(si);
        }

        /** ZooKeeperServer.processTxn(Request). */
        void processTxn(Request request) {
            TxnHeader hdr = request.getHdr();
            final boolean writeRequest = (hdr != null);
            final boolean quorumRequest = request.isQuorum();
            if (!writeRequest && !quorumRequest) {
                return;
            }
            zkDb.processTxn(hdr, request.getTxn());
            if (quorumRequest) {
                getZKDatabase().addCommittedProposal(request);
            }
            delivered(request); // PORT: the interface's reply (see delivered)
        }

        /**
         * PORT: INTERFACE.md: a server with a client sends it Committed{value} for every
         * transaction it applies to its data tree, in order, including the other client's values
         * (the source replies only to the request's own session, in FinalRequestProcessor).
         */
        void delivered(Request request) {
            if (self.client != null) {
                self.send(self.client, new Committed(request.getTxn()));
            }
        }
    }

    /** org.apache.zookeeper.server.FinalRequestProcessor: apply, then reply (in processTxn). */
    static final class FinalRequestProcessor implements RequestProcessor {
        final ZooKeeperServer zks;

        FinalRequestProcessor(ZooKeeperServer zks) {
            this.zks = zks;
        }

        @Override
        public void processRequest(Request request) {
            applyRequest(request);
            // PORT: if (request.cnxn == null) return; ... the reply is sent by processTxn
        }

        private void applyRequest(Request request) {
            zks.processTxn(request);
        }
    }

    /**
     * org.apache.zookeeper.server.quorum.CommitProcessor: queuedRequests, queuedWriteRequests,
     * the per-session pendingRequests and committedRequests, as in the source. PORT: its thread
     * is a step ({@link #run}, one iteration of the source's do-while loop), enabled when a
     * commit is waiting (an iteration with no commit waiting only moves requests into
     * pendingRequests, which the next commit-handling iteration does first in any case); no
     * worker pool (writes are processed inline by the source too; there are no reads).
     */
    static final class CommitProcessor implements RequestProcessor {
        final RequestProcessor nextProcessor;
        final boolean matchSyncs;
        final ArrayDeque<Request> queuedRequests = new ArrayDeque<>();
        final ArrayDeque<Request> queuedWriteRequests = new ArrayDeque<>();
        final Map<Long, Deque<Request>> pendingRequests = new HashMap<>();
        final ArrayDeque<Request> committedRequests = new ArrayDeque<>();
        /** maxCommitBatchSize: zookeeper.commitProcessor.maxCommitBatchSize, default 1. */
        final int maxCommitBatchSize = 1;
        boolean stopped = false;

        CommitProcessor(RequestProcessor nextProcessor, boolean matchSyncs) {
            this.nextProcessor = nextProcessor;
            this.matchSyncs = matchSyncs;
        }

        boolean needCommit(Request request) {
            switch (request.type) {
                case OpCode.setData:
                    return true;
                case OpCode.sync:
                    return matchSyncs;
                default:
                    return false;
            }
        }

        /** PORT: the scheduler's enabling condition for {@link #run}. */
        boolean hasWork() {
            return !stopped && !committedRequests.isEmpty();
        }

        /** One iteration of CommitProcessor.run's do-while loop. */
        void run() {
            boolean commitIsWaiting = !committedRequests.isEmpty();
            int requestsToProcess = queuedRequests.size();
            Request request;
            while (!stopped && requestsToProcess > 0 && (request = queuedRequests.poll()) != null) {
                requestsToProcess--;
                if (needCommit(request) || pendingRequests.containsKey(request.sessionId)) {
                    Deque<Request> requests = pendingRequests.computeIfAbsent(request.sessionId, sid -> new ArrayDeque<>());
                    requests.addLast(request);
                } else {
                    sendToNextProcessor(request);
                }
                if (!pendingRequests.isEmpty() && !committedRequests.isEmpty()) {
                    commitIsWaiting = true;
                    break;
                }
            }
            if (!commitIsWaiting) {
                commitIsWaiting = !committedRequests.isEmpty();
            }
            if (commitIsWaiting && !stopped) {
                // waitForEmptyPool(): PORT: no worker pool
                int commitsToProcess = maxCommitBatchSize;
                Set<Long> queuesToDrain = new HashSet<>();
                while (commitIsWaiting && !stopped && commitsToProcess > 0) {
                    request = committedRequests.peek();
                    if (!queuedWriteRequests.isEmpty()
                            && queuedWriteRequests.peek().sessionId == request.sessionId
                            && queuedWriteRequests.peek().cxid == request.cxid) {
                        Deque<Request> sessionQueue = pendingRequests.get(request.sessionId);
                        if (sessionQueue == null || sessionQueue.isEmpty() || !needCommit(sessionQueue.peek())) {
                            break;
                        } else {
                            Request topPending = sessionQueue.poll();
                            topPending.setHdr(request.getHdr());
                            topPending.setTxn(request.getTxn());
                            topPending.zxid = request.zxid;
                            request = topPending;
                            queuedWriteRequests.poll();
                            queuesToDrain.add(request.sessionId);
                        }
                    }
                    committedRequests.remove();
                    commitsToProcess--;
                    processWrite(request);
                    commitIsWaiting = !committedRequests.isEmpty();
                }
                for (Long sessionId : queuesToDrain) {
                    Deque<Request> sessionQueue = pendingRequests.get(sessionId);
                    while (!stopped && !sessionQueue.isEmpty() && !needCommit(sessionQueue.peek())) {
                        sendToNextProcessor(sessionQueue.poll());
                    }
                    if (sessionQueue.isEmpty()) {
                        pendingRequests.remove(sessionId);
                    }
                }
            }
        }

        private void sendToNextProcessor(Request request) {
            nextProcessor.processRequest(request); // PORT: no worker pool
        }

        private void processWrite(Request request) {
            nextProcessor.processRequest(request);
        }

        void commit(Request request) {
            if (stopped || request == null) {
                return;
            }
            committedRequests.add(request);
        }

        @Override
        public void processRequest(Request request) {
            if (stopped) {
                return;
            }
            queuedRequests.add(request);
            if (needCommit(request)) {
                queuedWriteRequests.add(request);
            }
        }
    }

    /**
     * org.apache.zookeeper.server.SyncRequestProcessor: append to the log, flush, then pass on.
     * PORT: its thread is a step ({@link #nextStep}): while queuedRequests is non-empty the
     * thread's next action is to take and append one request; when it is empty and appended
     * requests wait, the poll times out and the thread flushes (shouldFlush() is false below
     * maxBatchSize = 1000 with flushDelay 0). No snapshots.
     */
    static final class SyncRequestProcessor implements RequestProcessor {
        final ZooKeeperServer zks;
        final RequestProcessor nextProcessor;
        final ArrayDeque<Request> queuedRequests = new ArrayDeque<>();
        final ArrayDeque<Request> toFlush = new ArrayDeque<>();
        final int maxBatchSize = 1000;

        SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
            this.zks = zks;
            this.nextProcessor = nextProcessor;
        }

        /** PORT: the thread's next step, or null when it waits in queuedRequests.take(). */
        Runnable nextStep() {
            if (!queuedRequests.isEmpty()) {
                return this::runOne;
            }
            if (!toFlush.isEmpty()) {
                return this::flush; // queuedRequests.poll(pollTime) returned null
            }
            return null;
        }

        /** SyncRequestProcessor.run, one request taken from queuedRequests. */
        private void runOne() {
            Request si = queuedRequests.poll();
            if (zks.getZKDatabase().append(si)) {
                // shouldSnapshot(): PORT: no snapshots
            } else if (toFlush.isEmpty()) {
                if (nextProcessor != null) {
                    nextProcessor.processRequest(si);
                }
                return;
            }
            toFlush.add(si);
            if (shouldFlush()) {
                flush();
            }
        }

        private boolean shouldFlush() {
            return (maxBatchSize > 0) && (toFlush.size() >= maxBatchSize);
        }

        private void flush() {
            if (this.toFlush.isEmpty()) {
                return;
            }
            zks.getZKDatabase().commit();
            if (this.nextProcessor == null) {
                this.toFlush.clear();
            } else {
                while (!this.toFlush.isEmpty()) {
                    final Request i = this.toFlush.remove();
                    this.nextProcessor.processRequest(i);
                }
            }
        }

        @Override
        public void processRequest(final Request request) {
            queuedRequests.add(request);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Leader side
    // ------------------------------------------------------------------------------------------

    /** org.apache.zookeeper.server.quorum.LeaderZooKeeperServer. */
    static final class LeaderZooKeeperServer extends ZooKeeperServer {
        Leader leader;
        PrepRequestProcessor prepRequestProcessor;
        ProposalRequestProcessor proposalProcessor;
        private boolean startSent = false;

        LeaderZooKeeperServer(QuorumPeer self, ZKDatabase zkDb) {
            super(self, zkDb);
            // PORT: the pipeline exists from the start so that the PrepRequestProcessor queue can
            // hold requests that arrive before the leader serves (see Leader.servingGate).
            setupRequestProcessors();
        }

        Leader getLeader() {
            return leader;
        }

        @Override
        void setupRequestProcessors() {
            if (firstProcessor != null) {
                return; // PORT: built once, in the constructor
            }
            RequestProcessor finalProcessor = new FinalRequestProcessor(this);
            RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(finalProcessor, this);
            commitProcessor = new CommitProcessor(toBeAppliedProcessor, false);
            proposalProcessor = new ProposalRequestProcessor(this, commitProcessor);
            prepRequestProcessor = new PrepRequestProcessor(this, proposalProcessor);
            firstProcessor = new LeaderRequestProcessor(this, prepRequestProcessor);
        }

        void submitLearnerRequest(Request request) {
            prepRequestProcessor.processRequest(request);
        }

        @Override
        void delivered(Request request) {
            super.delivered(request);
            if (self.late == 1 && !startSent) {
                // PORT: INTERFACE.md: n0 starts the late joiner n2 right after its first delivery.
                startSent = true;
                self.send(self.nodes[N - 1], new Start());
            }
        }
    }

    /** org.apache.zookeeper.server.quorum.LeaderRequestProcessor (session upgrade dropped). */
    static final class LeaderRequestProcessor implements RequestProcessor {
        final LeaderZooKeeperServer lzks;
        final RequestProcessor nextProcessor;

        LeaderRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
            this.lzks = zks;
            this.nextProcessor = nextProcessor;
        }

        @Override
        public void processRequest(Request request) {
            nextProcessor.processRequest(request);
        }
    }

    /** org.apache.zookeeper.server.PrepRequestProcessor: assigns the zxid. */
    static final class PrepRequestProcessor implements RequestProcessor {
        final LeaderZooKeeperServer zks;
        final RequestProcessor nextProcessor;
        final ArrayDeque<Request> submittedRequests = new ArrayDeque<>();

        PrepRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
            this.zks = zks;
            this.nextProcessor = nextProcessor;
        }

        @Override
        public void processRequest(Request request) {
            submittedRequests.add(request);
        }

        /**
         * PORT: the scheduler's enabling condition for {@link #run}: a request waits and the
         * leader's serving gate is open (NOTES.md D6).
         */
        boolean hasWork() {
            return zks.getLeader().servingGate() && !submittedRequests.isEmpty();
        }

        /** One iteration of PrepRequestProcessor.run's loop: take a request and prepare it. */
        void run() {
            Request request = submittedRequests.poll();
            pRequest(request);
        }

        void pRequest(Request request) {
            request.setHdr(null);
            request.setTxn(0);
            pRequestHelper(request);
            request.zxid = zks.getZxid();
            nextProcessor.processRequest(request);
        }

        private void pRequestHelper(Request request) {
            switch (request.type) {
                case OpCode.setData:
                    pRequest2Txn(request.type, zks.getNextZxid(), request, request.requestValue);
                    break;
                default:
                    LOG.warn("unknown type " + request.type);
                    break;
            }
        }

        void pRequest2Txn(int type, long zxid, Request request, int record) {
            request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid, type));
            request.setTxn(record); // PORT: SetDataTxn(path, data, version) is its value
        }
    }

    /** org.apache.zookeeper.server.quorum.ProposalRequestProcessor. */
    static final class ProposalRequestProcessor implements RequestProcessor {
        final LeaderZooKeeperServer zks;
        final RequestProcessor nextProcessor;
        final SyncRequestProcessor syncProcessor;

        ProposalRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
            this.zks = zks;
            this.nextProcessor = nextProcessor;
            AckRequestProcessor ackProcessor = new AckRequestProcessor(zks);
            syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
        }

        @Override
        public void processRequest(Request request) {
            // LearnerSyncRequest: PORT: no sync requests
            if (shouldForwardToNextProcessor(request)) {
                nextProcessor.processRequest(request);
            }
            if (request.getHdr() != null) {
                zks.getLeader().propose(request);
                syncProcessor.processRequest(request);
            }
        }

        private boolean shouldForwardToNextProcessor(Request request) {
            return true; // forwardLearnerRequestsToCommitProcessorDisabled is false by default
        }
    }

    /** org.apache.zookeeper.server.quorum.AckRequestProcessor: the leader acks its own log. */
    static final class AckRequestProcessor implements RequestProcessor {
        final LeaderZooKeeperServer zks;

        AckRequestProcessor(LeaderZooKeeperServer zks) {
            this.zks = zks; // PORT: the Leader is reached through its server (built before it)
        }

        @Override
        public void processRequest(Request request) {
            QuorumPeer self = zks.self;
            if (self != null) {
                zks.getLeader().processAck(self.getMyId(), request.zxid);
            } else {
                LOG.error("Null QuorumPeer");
            }
        }
    }

    /** org.apache.zookeeper.server.quorum.Leader. */
    static final class Leader {
        final QuorumPeer self;
        final LeaderZooKeeperServer zk;
        final Threads threads = new Threads();

        protected boolean quorumFormed = false;
        private final LinkedHashSet<LearnerHandler> learners = new LinkedHashSet<>();
        private final LinkedHashSet<LearnerHandler> forwardingFollowers = new LinkedHashSet<>();
        /** PORT: the learner sockets, by connection: {@link #socketKey}(sid, cnx). */
        private final Map<Long, LearnerHandler> sockets = new TreeMap<>();
        /** PORT: the late = 0 barrier has opened (it stays open). */
        private boolean allJoined = false;
        final Map<Long, Proposal> outstandingProposals = new HashMap<>();
        private final ArrayDeque<Proposal> toBeApplied = new ArrayDeque<>();
        protected final Proposal newLeaderProposal = new Proposal();
        final long followerCounter = -1;

        StateSummary leaderStateSummary;
        long epoch = -1;
        boolean waitingForNewEpoch = true;
        boolean allowedToCommit = true;
        long lastCommitted = -1;
        long lastProposed;
        /** PORT: self.setZooKeeperServer(zk) has run: the leader serves clients. */
        boolean leaderServes = false;

        protected final Set<Long> connectingFollowers = new HashSet<>();
        protected final Set<Long> electingFollowers = new HashSet<>();
        protected boolean electionFinished = false;

        // PORT: the objects the source waits on, as monitors of Threads
        final Threads.Monitor connectingFollowersMonitor = threads.monitor();
        final Threads.Monitor electingFollowersMonitor = threads.monitor();
        final Threads.Monitor newLeaderProposalMonitor = threads.monitor();
        final Threads.Monitor zkMonitor = threads.monitor();

        Leader(QuorumPeer self, LeaderZooKeeperServer zk) {
            this.self = self;
            this.zk = zk;
            zk.leader = this;
        }

        void addLearnerHandler(LearnerHandler learner) {
            learners.add(learner);
        }

        void removeLearnerHandler(LearnerHandler peer) {
            forwardingFollowers.remove(peer);
            learners.remove(peer);
        }

        void addForwardingFollower(LearnerHandler lh) {
            forwardingFollowers.add(lh);
        }

        private boolean isParticipant(long sid) {
            return self.getQuorumVerifier().votingMembers.contains(sid);
        }

        ZKDatabase getZKDatabase() {
            return zk.getZKDatabase();
        }

        long getQuorumVerifierVersion() {
            return self.getQuorumVerifier().getVersion();
        }

        long getAndDecrementFollowerCounter() {
            return followerCounter; // PORT: unused (every learner sends its sid)
        }

        /** Leader.lead(), up to its first wait. */
        void lead() {
            self.setZabState(ZabState.DISCOVERY);
            zk.loadData();
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getZKDatabase().getDataTreeLastProcessedZxid());
            // cnxAcceptor = new LearnerCnxAcceptor(); cnxAcceptor.start(); PORT: serve() accepts
            getEpochToPropose(self.getMyId(), self.getAcceptedEpoch(), this::lead2);
        }

        /** Leader.lead(), after getEpochToPropose returned. */
        private void lead2(long epoch) {
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));
            lastProposed = zk.getZxid();
            newLeaderProposal.packet = self.packet(NEWLEADER, zk.getZxid(), new NoData());
            if ((newLeaderProposal.packet.zxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of " + Long.toHexString(newLeaderProposal.packet.zxid()));
            }
            // PORT: the lastSeenQuorumVerifier version bump (ZOOKEEPER-1783) is dropped: one
            // majority quorum, no reconfiguration
            newLeaderProposal.addQuorumVerifier(self.getQuorumVerifier());
            waitForEpochAckOrFail(self.getMyId(), leaderStateSummary, () -> lead3(epoch));
        }

        /** Leader.lead(), after waitForEpochAck returned. */
        private void lead3(long epoch) {
            self.setCurrentEpoch(epoch);
            self.setZabState(ZabState.SYNCHRONIZATION);
            waitForNewLeaderAck(self.getMyId(), zk.getZxid(), this::lead4);
        }

        /** Leader.lead(), after waitForNewLeaderAck returned. */
        private void lead4() {
            startZkServer();
            leaderServes = true; // self.setZooKeeperServer(zk) (zookeeper.leaderServes = yes)
            self.setZabState(ZabState.BROADCAST);
            // PORT: the ping loop (ticks, syncedAckSet, shutdown on a lost quorum) is dropped: no time
        }

        private void waitForEpochAckOrFail(long id, StateSummary ss, Runnable k) {
            try {
                waitForEpochAck(id, ss, k);
            } catch (ZabIOException ex) {
                // PORT: lead() would shut down and the peer go LOOKING; impossible here
                throw new AssertionError("lead: " + ex.getMessage(), ex);
            }
        }

        static long socketKey(int sid, int cnx) {
            return ((long) cnx << 8) | sid;
        }

        /**
         * The leader process's scheduler. PORT: one loop multiplexes the leader's threads: the
         * learner sockets (LearnerCnxAcceptor and the LearnerHandler reads), the client
         * connection, the woken waits, the PrepRequestProcessor, SyncRequestProcessor and
         * CommitProcessor threads and the LearnerHandler sender threads (see {@link #nextEvent}).
         * A socket is read only while its handler is at a read point; otherwise its packets stay
         * in the channel.
         */
        void serve() {
            while (true) {
                List<Runnable> enabled = enabledSteps();
                final Set<Long> known = Set.copyOf(sockets.keySet());
                final Set<Long> readable = readableSockets();
                MpMessage m = nextEvent(self.model,
                        msg -> msg instanceof Propose
                                || (msg instanceof QuorumPacket q
                                        && (!known.contains(socketKey(q.sid(), q.cnx()))
                                                || readable.contains(socketKey(q.sid(), q.cnx())))),
                        enabled);
                if (m == null) {
                    continue; // a step ran
                }
                if (m instanceof Propose) {
                    zk.submitRequest(((Propose) m).value());
                } else {
                    QuorumPacket qp = (QuorumPacket) m;
                    long key = socketKey(qp.sid(), qp.cnx());
                    LearnerHandler handler = sockets.get(key);
                    if (handler == null) {
                        // LearnerCnxAcceptor: a new connection gets a LearnerHandler
                        handler = new LearnerHandler(qp.sid(), qp.cnx(), this);
                        sockets.put(key, handler);
                    }
                    handler.receive(qp);
                }
            }
        }

        /** PORT: the leader's enabled thread steps, in a fixed order (index 0 is Nondet's min). */
        private List<Runnable> enabledSteps() {
            List<Runnable> steps = threads.readySteps();
            if (zk.prepRequestProcessor.hasWork()) {
                steps.add(zk.prepRequestProcessor::run);
            }
            Runnable sync = zk.proposalProcessor.syncProcessor.nextStep();
            if (sync != null) {
                steps.add(sync);
            }
            if (zk.commitProcessor.hasWork()) {
                steps.add(zk.commitProcessor::run);
            }
            for (LearnerHandler h : sockets.values()) {
                if (h.senderHasWork()) {
                    steps.add(h::sendPacket);
                }
            }
            return steps;
        }

        private Set<Long> readableSockets() {
            Set<Long> r = new HashSet<>();
            for (Map.Entry<Long, LearnerHandler> en : sockets.entrySet()) {
                if (en.getValue().isReading()) {
                    r.add(en.getKey());
                }
            }
            return Set.copyOf(r);
        }

        /**
         * PORT: when the PrepRequestProcessor may take requests: the leader serves (after
         * startZkServer), and with late = 0 both followers have joined: each has received UPTODATE
         * and acknowledged it (INTERFACE.md: all three servers join the leader before any proposal
         * is made). Once open the barrier stays open.
         */
        boolean servingGate() {
            if (!zk.isRunning() || !leaderServes) {
                return false;
            }
            if (self.late == 1 || allJoined) {
                return true;
            }
            for (int sid = 0; sid < N; sid++) {
                if (sid == self.getMyId()) {
                    continue;
                }
                boolean joined = false;
                for (LearnerHandler h : sockets.values()) {
                    if (h.socketSid == sid && h.upToDateAcked) {
                        joined = true;
                    }
                }
                if (!joined) {
                    return false;
                }
            }
            allJoined = true;
            return true;
        }

        /** Leader.startZkServer(). */
        private void startZkServer() {
            lastCommitted = zk.getZxid();
            // self.isReconfigEnabled() is false: PORT: no designated-leader computation
            zk.startup();
            zkMonitor.notifyAllWaiters(); // PORT: replaces waitForStartup's 20 ms polling of zk.isRunning()
            zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
        }

        /** Leader.tryToCommit. */
        boolean tryToCommit(Proposal p, long zxid) {
            if (outstandingProposals.containsKey(zxid - 1)) {
                return false;
            }
            if (!p.hasAllQuorums()) {
                return false;
            }
            if (zxid != lastCommitted + 1) {
                LOG.warn("Commiting zxid 0x" + Long.toHexString(zxid) + " not first!");
                LOG.warn("First is 0x" + Long.toHexString(lastCommitted + 1));
            }
            outstandingProposals.remove(zxid);
            if (p.request != null) {
                toBeApplied.add(p);
            }
            if (p.request == null) {
                LOG.warn("Going to commit null: " + p);
            } else if (p.request.getHdr().type() == OpCode.reconfig) {
                throw new AssertionError("reconfig"); // PORT: no reconfiguration
            } else {
                commit(zxid);
                inform(p);
            }
            zk.commitProcessor.commit(p.request);
            // pendingSyncs: PORT: no sync requests
            return true;
        }

        /** Leader.processAck. */
        void processAck(long sid, long zxid) {
            if (!allowedToCommit) {
                return;
            }
            if ((zxid & 0xffffffffL) == 0) {
                // the ACK of NEWLEADER/UPTODATE: ignored here
                return;
            }
            if (outstandingProposals.size() == 0) {
                LOG.debug("outstanding is 0");
                return;
            }
            if (lastCommitted >= zxid) {
                LOG.debug("proposal has already been committed");
                return;
            }
            Proposal p = outstandingProposals.get(zxid);
            if (p == null) {
                LOG.warn("Trying to commit future proposal: zxid 0x" + Long.toHexString(zxid));
                return;
            }
            p.addAck(sid);
            boolean hasCommitted = tryToCommit(p, zxid);
            if (hasCommitted && p.request != null && p.request.getHdr().type() == OpCode.reconfig) {
                long curZxid = zxid;
                while (allowedToCommit && hasCommitted && p != null) {
                    curZxid++;
                    p = outstandingProposals.get(curZxid);
                    if (p != null) {
                        hasCommitted = tryToCommit(p, curZxid);
                    }
                }
            }
        }

        /** Leader.ToBeAppliedRequestProcessor. */
        static final class ToBeAppliedRequestProcessor implements RequestProcessor {
            private final RequestProcessor next;
            private final LeaderZooKeeperServer zks;

            ToBeAppliedRequestProcessor(RequestProcessor next, LeaderZooKeeperServer zks) {
                this.next = next;
                this.zks = zks; // PORT: the Leader is reached through its server
            }

            @Override
            public void processRequest(Request request) {
                next.processRequest(request);
                if (request.getHdr() != null) {
                    long zxid = request.getHdr().zxid();
                    Iterator<Proposal> iter = zks.getLeader().toBeApplied.iterator();
                    if (iter.hasNext()) {
                        Proposal p = iter.next();
                        if (p.request != null && p.request.zxid == zxid) {
                            iter.remove();
                            return;
                        }
                    }
                    LOG.error("Committed request not found on toBeApplied: " + request);
                }
            }
        }

        /** Leader.sendPacket: to every forwarding follower. */
        void sendPacket(QuorumPacket qp) {
            for (LearnerHandler f : forwardingFollowers) {
                f.queuePacket(qp);
            }
        }

        /** Leader.commit. */
        void commit(long zxid) {
            lastCommitted = zxid;
            QuorumPacket qp = self.packet(COMMIT, zxid, new NoData());
            sendPacket(qp);
        }

        /** Leader.inform: to the observers. PORT: there are none. */
        void inform(Proposal proposal) {
        }

        long getLastProposed() {
            return lastProposed;
        }

        long getEpoch() {
            return ZxidUtils.getEpochFromZxid(lastProposed);
        }

        /** Leader.propose. */
        Proposal propose(Request request) {
            if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
                throw new AssertionError("zxid lower 32 bits have rolled over"); // PORT: shutdown + re-election
            }
            TxnLogEntry data = request.getSerializeData();
            QuorumPacket pp = self.packet(PROPOSAL, request.zxid, data);
            Proposal p = new Proposal(request, pp);
            p.addQuorumVerifier(self.getQuorumVerifier());
            // reconfig / lastSeenQuorumVerifier: PORT: no reconfiguration
            lastProposed = p.packet.zxid();
            outstandingProposals.put(lastProposed, p);
            sendPacket(pp);
            return p;
        }

        /** Leader.startForwarding. */
        long startForwarding(LearnerHandler handler, long lastSeenZxid) {
            if (lastProposed > lastSeenZxid) {
                for (Proposal p : toBeApplied) {
                    if (p.packet.zxid() <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(p.packet);
                    QuorumPacket qp = self.packet(COMMIT, p.packet.zxid(), new NoData());
                    handler.queuePacket(qp);
                }
                if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                    List<Long> zxids = new ArrayList<>(outstandingProposals.keySet());
                    Collections.sort(zxids);
                    for (Long zxid : zxids) {
                        if (zxid <= lastSeenZxid) {
                            continue;
                        }
                        handler.queuePacket(outstandingProposals.get(zxid).packet);
                    }
                }
            }
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                addForwardingFollower(handler);
            }
            return lastProposed;
        }

        /** Leader.waitForStartup. PORT: the 20 ms poll becomes a wait notified by startZkServer. */
        void waitForStartup(Runnable k) {
            threads.waitWhile(zkMonitor, () -> !zk.isRunning(), k);
        }

        /** Leader.getEpochToPropose; the returned epoch is passed to k. */
        void getEpochToPropose(long sid, long lastAcceptedEpoch, LongConsumer k) {
            if (!waitingForNewEpoch) {
                k.accept(epoch);
                return;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch + 1;
            }
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            QuorumMaj verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getMyId()) && verifier.containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowersMonitor.notifyAllWaiters();
            } else {
                // PORT: wait without the initLimit timeout (and without quitWaitForEpoch)
                threads.waitWhile(connectingFollowersMonitor, () -> waitingForNewEpoch, () -> k.accept(epoch));
                return;
            }
            threads.resume(() -> k.accept(epoch)); // PORT: return epoch, as a step after leaving the monitor
        }

        /** Leader.waitForEpochAck. */
        void waitForEpochAck(long id, StateSummary ss, Runnable k) {
            if (electionFinished) {
                k.run();
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new ZabIOException("Follower is ahead of the leader, leader summary: "
                            + leaderStateSummary.getCurrentEpoch() + " (current epoch), "
                            + leaderStateSummary.getLastZxid() + " (last zxid)");
                }
                if (ss.getLastZxid() != -1 && isParticipant(id)) {
                    electingFollowers.add(id);
                }
            }
            QuorumMaj verifier = self.getQuorumVerifier();
            if (electingFollowers.contains(self.getMyId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                electingFollowersMonitor.notifyAllWaiters();
            } else {
                // PORT: wait without the initLimit timeout
                threads.waitWhile(electingFollowersMonitor, () -> !electionFinished, k);
                return;
            }
            threads.resume(k); // PORT: return, as a step after leaving the monitor
        }

        /** Leader.waitForNewLeaderAck. */
        void waitForNewLeaderAck(long sid, long zxid, Runnable k) {
            if (quorumFormed) {
                k.run();
                return;
            }
            long currentZxid = newLeaderProposal.packet.zxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid + " is from a different epoch");
                k.run();
                return;
            }
            newLeaderProposal.addAck(sid);
            if (newLeaderProposal.hasAllQuorums()) {
                quorumFormed = true;
                newLeaderProposalMonitor.notifyAllWaiters();
            } else {
                // PORT: wait without the initLimit timeout
                threads.waitWhile(newLeaderProposalMonitor, () -> !quorumFormed, k);
                return;
            }
            threads.resume(k); // PORT: return, as a step after leaving the monitor
        }
    }

    /**
     * org.apache.zookeeper.server.quorum.LearnerHandler. PORT: run() is cut at its blocking
     * points (socket reads and the leader's waits) into run, run2 .. run7 and the packet loop;
     * the locals that live across a cut are fields.
     */
    static final class LearnerHandler {
        enum Phase { READ_FOLLOWERINFO, BLOCKED, READ_ACKEPOCH, READ_NEWLEADER_ACK, LOOP, CLOSED }

        /** PORT: the socket: the sid of the server at the other end and its connection number. */
        final int socketSid;
        final int cnx;
        final Leader learnerMaster;
        protected long sid = 0;
        protected int version = 0x1;
        LearnerType learnerType = LearnerType.PARTICIPANT;
        final ArrayDeque<QuorumPacket> queuedPackets = new ArrayDeque<>();
        private boolean sendingThreadStarted = false;
        private boolean needOpPacket = true;
        private long leaderLastZxid;
        protected final boolean forceSnapSync = false;
        Phase phase = Phase.READ_FOLLOWERINFO;
        /**
         * PORT: the learner acknowledged UPTODATE (the first zero-counter ACK read in the packet
         * loop; the NEWLEADER ACK was read before UPTODATE was sent, so the two cannot be
         * confused even on bag). Feeds the late = 0 barrier only.
         */
        boolean upToDateAcked = false;

        // run()'s locals across the cuts
        private long newLeaderZxid;
        private boolean needSnap;

        LearnerHandler(int socketSid, int cnx, Leader learnerMaster) {
            this.socketSid = socketSid;
            this.cnx = cnx;
            this.learnerMaster = learnerMaster;
        }

        long getSid() {
            return sid;
        }

        int getVersion() {
            return version;
        }

        LearnerType getLearnerType() {
            return learnerType;
        }

        boolean isReading() {
            return phase != Phase.BLOCKED;
        }

        /** A packet read from this handler's socket. */
        void receive(QuorumPacket qp) {
            if (qp.type() == CLOSE && phase != Phase.CLOSED) {
                // PORT: ia.readRecord throws EOFException: catch (IOException) { closeSocket(); }
                // finally { shutdown(); }
                LOG.error("Unexpected exception in LearnerHandler: EOF");
                shutdown();
                return;
            }
            switch (phase) {
                case READ_FOLLOWERINFO:
                    guarded(() -> run(qp));
                    break;
                case READ_ACKEPOCH:
                    guarded(() -> run3(qp));
                    break;
                case READ_NEWLEADER_ACK:
                    guarded(() -> run5(qp));
                    break;
                case LOOP:
                    guarded(() -> runLoop(qp));
                    break;
                case CLOSED:
                    break; // PORT: a closed socket's data is discarded
                default:
                    throw new AssertionError("read while blocked");
            }
        }

        /** PORT: the catch (IOException) of run(): the handler closes its socket and ends. */
        private void guarded(Runnable r) {
            try {
                r.run();
            } catch (ZabIOException ex) {
                LOG.error("Unexpected exception in LearnerHandler: " + ex.getMessage());
                shutdown();
            }
        }

        /** LearnerHandler.run(), from the first read up to getEpochToPropose. */
        private void run(QuorumPacket qp) {
            learnerMaster.addLearnerHandler(this);
            if (qp.type() != FOLLOWERINFO && qp.type() != OBSERVERINFO) {
                LOG.error("First packet " + qp + " is not FOLLOWERINFO or OBSERVERINFO!");
                shutdown(); // return; → finally { shutdown(); }
                return;
            }
            if (qp.data() instanceof LearnerInfo) {
                LearnerInfo learnerInfoData = (LearnerInfo) qp.data();
                this.sid = learnerInfoData.serverid();
                this.version = learnerInfoData.protocolVersion();
                long configVersion = learnerInfoData.configVersion();
                if (configVersion > learnerMaster.getQuorumVerifierVersion()) {
                    throw new ZabIOException("Follower is ahead of the leader (has a later activated configuration)");
                }
            } else {
                this.sid = learnerMaster.getAndDecrementFollowerCounter();
            }
            if (qp.type() == OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.zxid());
            long zxid = qp.zxid();
            phase = Phase.BLOCKED;
            learnerMaster.getEpochToPropose(this.getSid(), lastAcceptedEpoch,
                    newEpoch -> guarded(() -> run2(newEpoch, zxid)));
        }

        /** LearnerHandler.run(), after getEpochToPropose returned. */
        private void run2(long newEpoch, long zxid) {
            newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);
            if (this.getVersion() < 0x10000) {
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                StateSummary ss = new StateSummary(epoch, zxid);
                phase = Phase.BLOCKED;
                learnerMaster.waitForEpochAck(this.getSid(), ss, () -> guarded(() -> run4(ss)));
            } else {
                QuorumPacket newEpochPacket = learnerMaster.self.packet(LEADERINFO, newLeaderZxid, new IntData(0x10000));
                writeRecord(newEpochPacket);
                phase = Phase.READ_ACKEPOCH; // ia.readRecord(ackEpochPacket, "packet")
            }
        }

        /** LearnerHandler.run(), the ACKEPOCH read, up to waitForEpochAck. */
        private void run3(QuorumPacket ackEpochPacket) {
            if (ackEpochPacket.type() != ACKEPOCH) {
                LOG.error(ackEpochPacket + " is not ACKEPOCH");
                shutdown();
                return;
            }
            StateSummary ss = new StateSummary(((IntData) ackEpochPacket.data()).value(), ackEpochPacket.zxid());
            phase = Phase.BLOCKED;
            learnerMaster.waitForEpochAck(this.getSid(), ss, () -> guarded(() -> run4(ss)));
        }

        /** LearnerHandler.run(), after waitForEpochAck: the sync, NEWLEADER, the sender thread. */
        private void run4(StateSummary ss) {
            long peerLastZxid = ss.getLastZxid();
            needSnap = syncFollower(peerLastZxid, learnerMaster);
            if (needSnap) {
                // syncThrottler: PORT: no throttling
                long zxidToSend = learnerMaster.getZKDatabase().getDataTreeLastProcessedZxid();
                // PORT: the serialized snapshot (and the "BenWasHere" signature) travel in SNAP's data
                writeRecord(learnerMaster.self.packet(SNAP, zxidToSend, learnerMaster.getZKDatabase().serializeSnapshot()));
            }
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = learnerMaster.self.packet(NEWLEADER, newLeaderZxid, new NoData());
                writeRecord(newLeaderQP);
            } else {
                // PORT: getQuorumVerifierBytes() is dropped (no reconfiguration)
                QuorumPacket newLeaderQP = learnerMaster.self.packet(NEWLEADER, newLeaderZxid, new NoData());
                enqueue(newLeaderQP); // queuedPackets.add(newLeaderQP)
            }
            startSendingPackets();
            phase = Phase.READ_NEWLEADER_ACK; // ia.readRecord(qp, "packet")
        }

        /** LearnerHandler.run(), the NEWLEADER ACK read, up to waitForNewLeaderAck. */
        private void run5(QuorumPacket qp) {
            if (qp.type() != ACK) {
                LOG.error("Next packet was supposed to be an ACK, but received packet: " + qp);
                shutdown();
                return;
            }
            phase = Phase.BLOCKED;
            learnerMaster.waitForNewLeaderAck(getSid(), qp.zxid(), () -> guarded(this::run6));
        }

        /** LearnerHandler.run(), after waitForNewLeaderAck. */
        private void run6() {
            // syncLimitCheck.start(); syncThrottler.endSync(); sock.setSoTimeout(...): PORT: no time
            learnerMaster.waitForStartup(() -> guarded(this::run7));
        }

        /** LearnerHandler.run(), after waitForStartup: UPTODATE, then the packet loop. */
        private void run7() {
            enqueue(learnerMaster.self.packet(UPTODATE, -1, new NoData())); // queuedPackets.add(...)
            phase = Phase.LOOP;
        }

        /** LearnerHandler.run(), one iteration of the while (true) packet loop. */
        private void runLoop(QuorumPacket qp) {
            switch (qp.type()) {
                case ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        LOG.debug("Received ACK from Observer " + this.sid);
                    }
                    if ((qp.zxid() & 0xffffffffL) == 0) {
                        upToDateAcked = true; // PORT: observation for the late = 0 barrier only
                    }
                    learnerMaster.processAck(this.sid, qp.zxid());
                    break;
                case PING:
                    break; // PORT: no sessions
                case REVALIDATE:
                    break; // PORT: no sessions
                case REQUEST:
                    RequestData bb = (RequestData) qp.data();
                    long sessionId = bb.sessionId();
                    int cxid = bb.cxid();
                    int type = bb.type();
                    Request si;
                    if (type == OpCode.sync) {
                        throw new AssertionError("sync"); // PORT: no sync requests
                    } else {
                        si = new Request(sessionId, cxid, type, bb.value());
                    }
                    si.setOwner(this);
                    learnerMaster.zk.submitLearnerRequest(si);
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: " + qp);
                    break;
            }
        }

        /**
         * LearnerHandler.startSendingPackets. PORT: the sender thread is the step
         * {@link #sendPacket}, enabled while queuedPackets is non-empty.
         */
        protected void startSendingPackets() {
            if (!sendingThreadStarted) {
                sendingThreadStarted = true;
            } else {
                LOG.error("Attempting to start sending thread after it already started");
            }
        }

        boolean senderHasWork() {
            return sendingThreadStarted && phase != Phase.CLOSED && !queuedPackets.isEmpty();
        }

        /** One iteration of LearnerHandler.sendPackets: take the head of the queue and write it. */
        void sendPacket() {
            QuorumPacket p = queuedPackets.poll();
            writeRecord(p);
        }

        /** oa.writeRecord(p, "packet") + flush: onto this connection. */
        private void writeRecord(QuorumPacket p) {
            learnerMaster.self.send(learnerMaster.self.nodes[socketSid], p.on(cnx));
        }

        /** queuedPackets.add(p). */
        private void enqueue(QuorumPacket p) {
            queuedPackets.add(p);
        }

        /** LearnerHandler.syncFollower. */
        boolean syncFollower(long peerLastZxid, Leader learnerMaster) {
            boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
            long currentZxid = peerLastZxid;
            boolean needSnap = true;
            ZKDatabase db = learnerMaster.getZKDatabase();
            boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
            // rl.lock(): PORT: one thread per process
            long maxCommittedLog = db.getmaxCommittedLog();
            long minCommittedLog = db.getminCommittedLog();
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();
            if (db.getCommittedLog().isEmpty()) {
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }
            if (forceSnapSync) {
                LOG.warn("Forcing snapshot sync - should not see this in production");
            } else if (lastProcessedZxid == peerLastZxid) {
                LOG.info("Sending DIFF zxid=0x" + Long.toHexString(peerLastZxid) + " for peer sid: " + getSid());
                queueOpPacket(DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                queueOpPacket(TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            } else if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                LOG.info("Using committedLog for peer sid: " + getSid());
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                currentZxid = queueCommittedProposals(itr, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            } else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                long sizeLimit = db.calculateTxnLogSizeLimit();
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: " + getSid());
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);
                    if (currentZxid < minCommittedLog) {
                        currentZxid = peerLastZxid;
                        queuedPackets.clear();
                        needOpPacket = true;
                    } else {
                        Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                        currentZxid = queueCommittedProposals(committedLogItr, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                }
                // txnProposalItr.close(): PORT: nothing to close
            } else {
                LOG.warn("Unhandled scenario for peer sid: " + getSid());
            }
            if (needSnap) {
                currentZxid = db.getDataTreeLastProcessedZxid();
            }
            leaderLastZxid = learnerMaster.startForwarding(this, currentZxid);
            if (needOpPacket && !needSnap) {
                LOG.error("Unhandled scenario for peer sid: " + getSid() + " fall back to use snapshot");
                needSnap = true;
            }
            return needSnap;
        }

        /** LearnerHandler.queueCommittedProposals: queues (peerLastZxid, maxZxid]. */
        protected long queueCommittedProposals(Iterator<Proposal> itr, long peerLastZxid, Long maxZxid, Long lastCommittedZxid) {
            boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
            long queuedZxid = peerLastZxid;
            long prevProposalZxid = -1;
            while (itr.hasNext()) {
                Proposal propose = itr.next();
                long packetZxid = propose.getZxid();
                if ((maxZxid != null) && (packetZxid > maxZxid)) {
                    break;
                }
                if (packetZxid < peerLastZxid) {
                    prevProposalZxid = packetZxid;
                    continue;
                }
                if (needOpPacket) {
                    if (packetZxid == peerLastZxid) {
                        queueOpPacket(DIFF, lastCommittedZxid);
                        needOpPacket = false;
                        continue;
                    }
                    if (isPeerNewEpochZxid) {
                        queueOpPacket(DIFF, lastCommittedZxid);
                        needOpPacket = false;
                    } else if (packetZxid > peerLastZxid) {
                        if (ZxidUtils.getEpochFromZxid(packetZxid) != ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                            LOG.warn("Cannot send TRUNC to peer sid: " + getSid() + " peer zxid is from different epoch");
                            return queuedZxid;
                        }
                        queueOpPacket(TRUNC, prevProposalZxid);
                        needOpPacket = false;
                    }
                }
                if (packetZxid <= queuedZxid) {
                    continue;
                }
                queuePacket(propose.getQuorumPacket());
                queueOpPacket(COMMIT, packetZxid);
                queuedZxid = packetZxid;
            }
            if (needOpPacket && isPeerNewEpochZxid) {
                queueOpPacket(DIFF, lastCommittedZxid);
                needOpPacket = false;
            }
            return queuedZxid;
        }

        /** LearnerHandler.shutdown. */
        void shutdown() {
            queuedPackets.clear(); // queuedPackets.put(proposalOfDeath)
            if (phase != Phase.CLOSED) {
                writeRecord(learnerMaster.self.packet(CLOSE, -1, new NoData())); // closeSocket(): PORT: FIN
            }
            phase = Phase.CLOSED;
            learnerMaster.removeLearnerHandler(this);
        }

        private void queueOpPacket(int type, long zxid) {
            QuorumPacket packet = learnerMaster.self.packet(type, zxid, new NoData());
            queuePacket(packet);
        }

        void queuePacket(QuorumPacket p) {
            enqueue(p);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Follower side
    // ------------------------------------------------------------------------------------------

    /** org.apache.zookeeper.server.quorum.FollowerZooKeeperServer. */
    static final class FollowerZooKeeperServer extends ZooKeeperServer {
        Follower follower;
        SyncRequestProcessor syncProcessor;
        final ArrayDeque<Request> pendingTxns = new ArrayDeque<>();

        FollowerZooKeeperServer(QuorumPeer self, ZKDatabase zkDb) {
            super(self, zkDb);
        }

        Follower getFollower() {
            return follower;
        }

        @Override
        void setupRequestProcessors() {
            RequestProcessor finalProcessor = new FinalRequestProcessor(this);
            commitProcessor = new CommitProcessor(finalProcessor, true);
            firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
            syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor(getFollower()));
        }

        /**
         * PORT: the enabled steps of this server's request-processor threads (FollowerRequest-,
         * Sync- and CommitProcessor), in a fixed order. An exception in one of them is the
         * critical-thread exit of the source (ZooKeeperCriticalThread.handleException).
         */
        List<Runnable> enabledSteps() {
            List<Runnable> steps = new ArrayList<>();
            if (!isRunning()) {
                return steps;
            }
            FollowerRequestProcessor frp = (FollowerRequestProcessor) firstProcessor;
            if (frp.hasWork()) {
                steps.add(critical(frp::run));
            }
            Runnable sync = syncProcessor.nextStep();
            if (sync != null) {
                steps.add(critical(sync));
            }
            if (commitProcessor.hasWork()) {
                steps.add(critical(commitProcessor::run));
            }
            return steps;
        }

        private static Runnable critical(Runnable r) {
            return () -> {
                try {
                    r.run();
                } catch (ZabStateException | NullPointerException ex) {
                    throw new SystemExit("critical thread failed: " + ex); // PORT: handleException → exit
                }
            };
        }

        void logRequest(Request request) {
            if ((request.zxid & 0xffffffffL) != 0) {
                pendingTxns.add(request);
            }
            syncProcessor.processRequest(request);
        }

        void appendRequest(Request request) {
            getZKDatabase().append(request);
        }

        void commit(long zxid) {
            if (pendingTxns.size() == 0) {
                LOG.warn("Committing " + Long.toHexString(zxid) + " without seeing txn");
                return;
            }
            long firstElementZxid = pendingTxns.element().zxid;
            if (firstElementZxid != zxid) {
                LOG.error("Committing zxid 0x" + Long.toHexString(zxid) + " but next pending txn 0x"
                        + Long.toHexString(firstElementZxid));
                throw requestSystemExit("UNMATCHED_TXN_COMMIT");
            }
            Request request = pendingTxns.remove();
            commitProcessor.commit(request);
        }
    }

    /** org.apache.zookeeper.server.quorum.FollowerRequestProcessor: forward writes as REQUEST. */
    static final class FollowerRequestProcessor implements RequestProcessor {
        final FollowerZooKeeperServer zks;
        final RequestProcessor nextProcessor;

        FollowerRequestProcessor(FollowerZooKeeperServer zks, RequestProcessor nextProcessor) {
            this.zks = zks;
            this.nextProcessor = nextProcessor;
        }

        final ArrayDeque<Request> queuedRequests = new ArrayDeque<>();

        @Override
        public void processRequest(Request request) {
            queuedRequests.add(request);
        }

        /** PORT: the scheduler's enabling condition for {@link #run}. */
        boolean hasWork() {
            return !queuedRequests.isEmpty();
        }

        /** One iteration of FollowerRequestProcessor.run: take a request, queue it locally, forward it. */
        void run() {
            Request request = queuedRequests.poll();
            maybeSendRequestToNextProcessor(request);
            switch (request.type) {
                case OpCode.sync:
                    throw new AssertionError("sync"); // PORT: no sync requests
                case OpCode.setData:
                    zks.getFollower().request(request);
                    break;
                default:
                    break;
            }
        }

        private void maybeSendRequestToNextProcessor(Request request) {
            nextProcessor.processRequest(request);
        }
    }

    /** org.apache.zookeeper.server.quorum.SendAckRequestProcessor. */
    static final class SendAckRequestProcessor implements RequestProcessor {
        final Learner learner;

        SendAckRequestProcessor(Learner peer) {
            this.learner = peer;
        }

        @Override
        public void processRequest(Request si) {
            if (si.type != OpCode.sync) {
                QuorumPacket qp = learner.packet(ACK, si.getHdr().zxid(), new NoData());
                learner.writePacket(qp, false);
            }
        }
    }

    /** Learner.PacketInFlight. */
    static final class PacketInFlight {
        TxnHeader hdr;
        int rec;

        Request toRequest() {
            return new Request(hdr, rec);
        }
    }

    /** org.apache.zookeeper.server.quorum.Learner. */
    static class Learner {
        final QuorumPeer self;
        final FollowerZooKeeperServer zk;
        /** PORT: the leader's address (findLeader: the fixed leader n0). */
        Tid leaderAddr;
        protected int leaderProtocolVersion = 0x01;
        /** PORT: this learner's connection to the leader (0 before connectToLeader). */
        int cnx = 0;
        /** sockBeingClosed. */
        private boolean sockBeingClosed = false;

        Learner(QuorumPeer self, FollowerZooKeeperServer zk) {
            this.self = self;
            this.zk = zk;
        }

        /** A packet on this learner's connection. */
        QuorumPacket packet(int type, long zxid, PacketData data) {
            return new QuorumPacket((int) self.getMyId(), cnx, type, zxid, data);
        }

        /** Learner.writePacket; asyncSending is false by default, so writePacketNow. */
        void writePacket(QuorumPacket pp, boolean flush) {
            if (pp != null && !sockBeingClosed) { // PORT: a write on a closed socket goes nowhere
                self.send(leaderAddr, pp);
            }
        }

        /** Learner.readPacket: the next packet on this connection (older connections' data is dead). */
        QuorumPacket readPacket() throws ZabException {
            final int c = cnx;
            QuorumPacket pp = Channel.recv(self.model, QuorumPacket.class, q -> q.cnx() == c);
            if (pp.type() == CLOSE) {
                throw new ZabException("EOF: the leader closed the connection"); // PORT: EOFException
            }
            return pp;
        }

        /** Learner.closeSocket. PORT: the close reaches the leader as a CLOSE after the data sent. */
        void closeSocket() {
            if (!sockBeingClosed) {
                if (leaderAddr == null) {
                    sockBeingClosed = true;
                    return; // closing before establishing the connection is a noop
                }
                self.send(leaderAddr, packet(CLOSE, -1, new NoData()));
                sockBeingClosed = true;
            }
        }

        /** Learner.shutdown. */
        void shutdown() {
            // self.setZooKeeperServer(null); self.closeAllConnections(): PORT: the client's
            // messages are read only while following (Follower.followLeader's loop)
            closeSocket();
            if (zk != null) {
                zk.shutdown(self.getSyncMode().equals(SyncMode.SNAP));
            }
        }

        /** Learner.request: forward a client request to the leader. */
        void request(Request request) {
            QuorumPacket qp = packet(REQUEST, -1,
                    new RequestData(request.sessionId, request.cxid, request.type, request.requestValue));
            writePacket(qp, true);
        }

        /** Learner.findLeader. PORT: the vote is the fixed leader n0. */
        protected Tid findLeader() {
            return self.nodes[(int) LEADER_ID];
        }

        /** Learner.connectToLeader. PORT: every address is known; a new connection number. */
        protected void connectToLeader(Tid addr) {
            this.leaderAddr = addr;
            this.cnx = self.nextCnx++;
        }

        /** Learner.registerWithLeader. */
        protected long registerWithLeader(int pktType) throws ZabException {
            long lastLoggedZxid = self.getLastLoggedZxid();
            QuorumPacket qp = packet(pktType, ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0),
                    new LearnerInfo(self.getMyId(), 0x10000, self.getQuorumVerifier().getVersion()));
            writePacket(qp, true);
            qp = readPacket();
            final long newEpoch = ZxidUtils.getEpochFromZxid(qp.zxid());
            if (qp.type() == LEADERINFO) {
                leaderProtocolVersion = ((IntData) qp.data()).value();
                int epochBytes;
                if (newEpoch > self.getAcceptedEpoch()) {
                    epochBytes = (int) self.getCurrentEpoch();
                    self.setAcceptedEpoch(newEpoch);
                } else if (newEpoch == self.getAcceptedEpoch()) {
                    epochBytes = -1;
                } else {
                    throw new ZabException("Leaders epoch, " + newEpoch + " is less than accepted epoch, "
                            + self.getAcceptedEpoch());
                }
                QuorumPacket ackNewEpoch = packet(ACKEPOCH, lastLoggedZxid, new IntData(epochBytes));
                writePacket(ackNewEpoch, true);
                return ZxidUtils.makeZxid(newEpoch, 0);
            } else {
                if (newEpoch > self.getAcceptedEpoch()) {
                    self.setAcceptedEpoch(newEpoch);
                }
                if (qp.type() != NEWLEADER) {
                    LOG.error("First packet should have been NEWLEADER");
                    throw new ZabException("First packet should have been NEWLEADER");
                }
                return qp.zxid();
            }
        }

        /** Learner.enforceContinuousProposal. */
        long enforceContinuousProposal(long lastQueued, PacketInFlight pif) throws ZabException {
            if (lastQueued == 0) {
                LOG.info("DIFF sync got first proposal 0x" + Long.toHexString(pif.hdr.zxid()));
            } else if (pif.hdr.zxid() != lastQueued + 1) {
                if (ZxidUtils.getEpochFromZxid(pif.hdr.zxid()) <= ZxidUtils.getEpochFromZxid(lastQueued)) {
                    String msg = "DIFF sync got proposal 0x" + Long.toHexString(pif.hdr.zxid()) + ", last queued 0x"
                            + Long.toHexString(lastQueued) + ", expected 0x" + Long.toHexString(lastQueued + 1);
                    LOG.error(msg);
                    throw new ZabException(msg);
                }
                LOG.warn("DIFF sync got new epoch proposal");
            }
            return pif.hdr.zxid();
        }

        /** Learner.syncWithLeader. */
        protected void syncWithLeader(long newLeaderZxid) throws ZabException {
            long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid);
            boolean snapshotNeeded = true;
            boolean syncSnapshot = false;
            QuorumPacket qp = readPacket();
            Deque<Long> packetsCommitted = new ArrayDeque<>();
            Deque<PacketInFlight> packetsNotLogged = new ArrayDeque<>();
            // synchronized (zk): PORT: one thread per process
            if (qp.type() == DIFF) {
                LOG.info("Getting a diff from the leader 0x" + Long.toHexString(qp.zxid()));
                self.setSyncMode(SyncMode.DIFF);
                if (zk.shouldForceWriteInitialSnapshotAfterLeaderElection()) {
                    snapshotNeeded = true;
                    syncSnapshot = true;
                } else {
                    snapshotNeeded = false;
                }
            } else if (qp.type() == SNAP) {
                self.setSyncMode(SyncMode.SNAP);
                LOG.info("Getting a snapshot from leader 0x" + Long.toHexString(qp.zxid()));
                zk.getZKDatabase().deserializeSnapshot((Snapshot) qp.data()); // PORT: from SNAP's data
                // initConfigInZKDatabase, the signature check: PORT: dropped
                zk.getZKDatabase().setlastProcessedZxid(qp.zxid());
                syncSnapshot = true;
            } else if (qp.type() == TRUNC) {
                self.setSyncMode(SyncMode.TRUNC);
                LOG.warn("Truncating log to get in sync with the leader 0x" + Long.toHexString(qp.zxid()));
                boolean truncated = zk.getZKDatabase().truncateLog(qp.zxid());
                if (!truncated) {
                    LOG.error("Not able to truncate the log 0x" + Long.toHexString(qp.zxid()));
                    throw requestSystemExit("QUORUM_PACKET_ERROR");
                }
                zk.getZKDatabase().setlastProcessedZxid(qp.zxid());
            } else {
                LOG.error("Got unexpected packet from leader: " + qp + ", exiting ... ");
                throw requestSystemExit("QUORUM_PACKET_ERROR");
            }
            // initConfigInZKDatabase, createSessionTracker: PORT: dropped
            long lastQueued = 0;
            boolean isPreZAB1_0 = true;
            boolean writeToTxnLog = !snapshotNeeded;
            outerLoop:
            while (self.isRunning()) {
                qp = readPacket();
                switch (qp.type()) {
                    case PROPOSAL: {
                        PacketInFlight pif = new PacketInFlight();
                        TxnLogEntry logEntry = (TxnLogEntry) qp.data();
                        pif.hdr = logEntry.header();
                        pif.rec = logEntry.txn();
                        lastQueued = enforceContinuousProposal(lastQueued, pif);
                        // reconfig: PORT: no reconfiguration
                        packetsNotLogged.add(pif);
                        break;
                    }
                    case COMMIT:
                    case COMMITANDACTIVATE: {
                        PacketInFlight pif = packetsNotLogged.peekFirst();
                        if (pif.hdr.zxid() == qp.zxid() && qp.type() == COMMITANDACTIVATE) {
                            throw new AssertionError("COMMITANDACTIVATE"); // PORT: no reconfiguration
                        }
                        if (!writeToTxnLog) {
                            if (pif.hdr.zxid() != qp.zxid()) {
                                LOG.warn("Committing 0x" + Long.toHexString(qp.zxid()) + ", but next proposal is 0x"
                                        + Long.toHexString(pif.hdr.zxid()));
                            } else {
                                zk.processTxn(pif.toRequest());
                                packetsNotLogged.remove();
                            }
                        } else {
                            packetsCommitted.add(qp.zxid());
                        }
                        break;
                    }
                    case INFORM:
                    case INFORMANDACTIVATE:
                        throw new AssertionError("INFORM"); // PORT: observers only
                    case UPTODATE:
                        LOG.info("Learner received UPTODATE message");
                        // newLeaderQV / processReconfig: PORT: no reconfiguration
                        if (isPreZAB1_0) {
                            zk.takeSnapshot(syncSnapshot);
                            self.setCurrentEpoch(newEpoch);
                        }
                        // self.setZooKeeperServer(zk): the client connection is served after the loop
                        break outerLoop;
                    case NEWLEADER: {
                        LOG.info("Learner received NEWLEADER message");
                        // qp.getData() (the leader's QuorumVerifier): PORT: no reconfiguration
                        if (snapshotNeeded) {
                            zk.takeSnapshot(syncSnapshot);
                        }
                        writeToTxnLog = true;
                        isPreZAB1_0 = false;
                        if (!packetsCommitted.isEmpty()) { // zk instanceof FollowerZooKeeperServer
                            FollowerZooKeeperServer fzk = zk;
                            while (!packetsCommitted.isEmpty()) {
                                long zxid = packetsCommitted.removeFirst();
                                PacketInFlight pif = packetsNotLogged.peekFirst();
                                if (pif == null) {
                                    LOG.warn("Committing 0x" + Long.toHexString(zxid) + ", but got no proposal");
                                    continue;
                                } else if (pif.hdr.zxid() != zxid) {
                                    LOG.warn("Committing 0x" + Long.toHexString(zxid) + ", but next proposal is 0x"
                                            + Long.toHexString(pif.hdr.zxid()));
                                    continue;
                                }
                                packetsNotLogged.removeFirst();
                                Request request = pif.toRequest();
                                fzk.appendRequest(request);
                                fzk.processTxn(request);
                            }
                            fzk.getZKDatabase().commit();
                        }
                        self.setCurrentEpoch(newEpoch);
                        // sock.setSoTimeout(...): PORT: no time
                        self.setSyncMode(SyncMode.NONE);
                        writePacket(packet(ACK, newLeaderZxid, new NoData()), true);
                        break;
                    }
                    default:
                        break;
                }
            }
            QuorumPacket ack = packet(ACK, ZxidUtils.makeZxid(newEpoch, 0), new NoData());
            writePacket(ack, true);
            zk.startup();
            // self.updateElectionVote(newEpoch): PORT: no election
            FollowerZooKeeperServer fzk = zk; // zk instanceof FollowerZooKeeperServer
            for (PacketInFlight p : packetsNotLogged) {
                fzk.logRequest(p.toRequest());
            }
            for (Long zxid : packetsCommitted) {
                fzk.commit(zxid);
            }
        }
    }

    /** org.apache.zookeeper.server.quorum.Follower. */
    static final class Follower extends Learner {
        private long lastQueued;
        final FollowerZooKeeperServer fzk;

        Follower(QuorumPeer self, FollowerZooKeeperServer zk) {
            super(self, zk);
            this.fzk = zk;
            zk.follower = this;
        }

        /** Follower.followLeader. */
        void followLeader() {
            self.setZabState(ZabState.DISCOVERY);
            Tid leaderServer = findLeader();
            try {
                connectToLeader(leaderServer);
                long newEpochZxid = registerWithLeader(FOLLOWERINFO);
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch is less than our accepted epoch");
                    throw new ZabException("Error: Epoch of leader is lower");
                }
                self.setZabState(ZabState.SYNCHRONIZATION);
                syncWithLeader(newEpochZxid);
                self.setZabState(ZabState.BROADCAST);
                // ObserverMaster: PORT: no observers
                while (this.isRunning()) {
                    // PORT: the reader thread (readPacket + processPacket), the client connection
                    // and the request-processor threads, scheduled by nextEvent
                    final int c = cnx;
                    MpMessage m = nextEvent(self.model,
                            msg -> msg instanceof Propose || (msg instanceof QuorumPacket q && q.cnx() == c),
                            fzk.enabledSteps());
                    if (m instanceof QuorumPacket) {
                        QuorumPacket qp = (QuorumPacket) m;
                        if (qp.type() == CLOSE) {
                            throw new ZabException("EOF: the leader closed the connection"); // PORT: EOFException
                        }
                        processPacket(qp);
                    } else if (m instanceof Propose) {
                        fzk.submitRequest(((Propose) m).value());
                    }
                }
            } catch (ZabException | ZabStateException | NullPointerException | ClassCastException e) {
                // catch (Exception e): PORT: the exceptions this port can raise on the reader thread
                LOG.warn("Exception when following the leader " + e);
                closeSocket();
                // pendingRevalidations.clear(): PORT: no sessions
            }
        }

        boolean isRunning() {
            return true; // PORT: no shutdown
        }

        /** Follower.processPacket. */
        protected void processPacket(QuorumPacket qp) {
            switch (qp.type()) {
                case PING:
                    break; // PORT: no sessions
                case PROPOSAL: {
                    TxnLogEntry logEntry = (TxnLogEntry) qp.data();
                    TxnHeader hdr = logEntry.header();
                    if (hdr.zxid() != lastQueued + 1) {
                        LOG.warn("Got zxid 0x" + Long.toHexString(hdr.zxid()) + " expected 0x"
                                + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = hdr.zxid();
                    // reconfig: PORT: no reconfiguration
                    fzk.logRequest(new Request(logEntry.header(), logEntry.txn())); // logEntry.toRequest()
                    break;
                }
                case COMMIT:
                    fzk.commit(qp.zxid());
                    break;
                case COMMITANDACTIVATE:
                    throw new AssertionError("COMMITANDACTIVATE"); // PORT: no reconfiguration
                case UPTODATE:
                    LOG.error("Received an UPTODATE message after Follower started");
                    break;
                case REVALIDATE:
                case SYNC:
                    break; // PORT: no sessions, no sync requests
                default:
                    LOG.warn("Unknown packet type: " + qp);
                    break;
            }
        }
    }

}
