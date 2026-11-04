package bench.cassandra_paxos_v1;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Apache Cassandra 4.1 lightweight transactions, Paxos v1 (variants {@code v1} and
 * {@code v1_without_linearizable_reads_or_rejected_writes}), transliterated to JMC's
 * message-passing API. One partition, one integer register {@code x}; three replicas
 * {@code n0..n2} (each also the coordinator of its client) and two clients {@code c0, c1}.
 *
 * <p>Source: StorageProxy.java (legacyCas, doPaxos, beginAndRepairPaxos, sendCommit, preparePaxos,
 * proposePaxos, commitPaxos, commitPaxosLocal, readOne, legacyReadWithPaxos), service/paxos/v1
 * (PrepareCallback, ProposeCallback, PrepareVerbHandler, ProposeVerbHandler), PaxosState
 * (Snapshot, legacyPrepare, legacyPropose, commitDirect, applyCommit), PaxosCommit.RequestHandler,
 * Commit, Ballot, PrepareResponse. Every changed line is marked {@code // PORT:}. See NOTES.md.
 *
 * <p>Launcher argument string: {@code impl,<q>,<v1|v1nolin>,<fifo|bag>}.
 */
public final class CassandraPaxosV1 implements MpProgram {

    // PORT: the interface's fixed instance: N = 3 replicas, K = 2 clients, attempt budget A = 3.
    static final int N = 3;
    static final int K = 2;
    static final int A = 3;

    private final int q;
    private final String variant;
    // PORT: Config.PaxosVariant / Paxos.isLinearizable() (Paxos.java l.1215-1221): true for v1.
    private final boolean isLinearizable;
    private final String delivery;
    private final CommunicationModel model;

    public CassandraPaxosV1(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<v1|v1nolin>,<fifo|bag>");
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1");
        }
        this.variant = args[2];
        if (variant.equals("v1")) {
            isLinearizable = true;
        } else if (variant.equals("v1nolin")) {
            isLinearizable = false;
        } else {
            throw new IllegalArgumentException("variant must be v1 or v1nolin, was " + variant);
        }
        this.delivery = args[3];
        if (delivery.equals("fifo")) {
            model = CommunicationModel.P2P;
        } else if (delivery.equals("bag")) {
            model = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("delivery must be fifo or bag, was " + delivery);
        }
    }

    // ------------------------------------------------------------------------------------------
    // MpProgram
    // ------------------------------------------------------------------------------------------

    @Override
    public String programName() {
        return "cassandra_paxos_v1";
    }

    @Override
    public String size() {
        return "q=" + q + ",variant=" + variant;
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

    // ------------------------------------------------------------------------------------------
    // Client messages (fixed by INTERFACE.md)
    // ------------------------------------------------------------------------------------------

    @Tag("Cas")
    public record Cas(int client, int expected, int value) implements MpMessage {}

    @Tag("Read")
    public record Read(int client) implements MpMessage {}

    @Tag("CasResult")
    public record CasResult(int client, int applied, int value) implements MpMessage {}

    @Tag("Value")
    public record Value(int client, int value) implements MpMessage {}

    @Tag("Failed")
    public record Failed(int client) implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Data types: Ballot, PartitionUpdate, Commit, PrepareResponse
    // ------------------------------------------------------------------------------------------

    /**
     * Ballot.java. PORT: a (counter, replica id) pair instead of a time UUID with a random lsb
     * (INTERFACE.md); ordered lexicographically.
     */
    @Tag("Ballot")
    public record Ballot(int counter, int node) {
        // PORT: Ballot.none() is the epoch ballot (unixMicros 0); here counter 0 with node -1, below
        // every generated ballot (counters start at 1).
        private static final Ballot EPOCH = new Ballot(0, -1);

        public static Ballot none() {
            return EPOCH;
        }

        // PORT: BallotGenerator.Default.atUnixMicros(unixMicros, flag) -> atUnixMicrosWithLsb(unixMicros,
        // secureRandom.nextLong(), flag); the random lsb is replaced by the replica id, the flag dropped.
        public static Ballot atUnixMicrosWithLsb(long unixMicros, int uniqueLsb) {
            return new Ballot((int) unixMicros, uniqueLsb);
        }

        /** PORT: TimeUUID.uuidTimestamp(); a single number ordered as (counter, node) lexicographically. */
        public long uuidTimestamp() {
            return (long) counter * 4 + (node + 1);
        }

        /** PORT: TimeUUID.unixMicros(); the counter. */
        public long unixMicros() {
            return counter;
        }
    }

    /**
     * PORT: PartitionUpdate of the one partition reduced to the single register x: either empty
     * (PartitionUpdate.emptyUpdate) or a write of x with its cell timestamp (Fix round 1, finding 7:
     * the cell timestamp is kept in the update, as the source keeps it in the cells, and is
     * ballot.unixMicros() = the ballot counter, set by Commit.newProposal's withTimestamp).
     */
    @Tag("PartitionUpdate")
    public record PartitionUpdate(boolean isEmpty, int x, long timestamp) {
        public static PartitionUpdate emptyUpdate() {
            return new PartitionUpdate(true, 0, 0); // PORT: no cells, so no timestamp
        }

        /** Commit.withTimestamp (Commit.java l.472-475): every cell gets the given timestamp. */
        static PartitionUpdate withTimestamp(PartitionUpdate update, long timestamp) {
            return update.isEmpty ? update : new PartitionUpdate(false, update.x, timestamp); // PORT: one cell
        }
    }

    /**
     * PORT: Cells.reconcile / resolveRegular (src-4.1 db/rows/Cells.java l.68-127) for the one live
     * int cell: the higher timestamp wins; on equal timestamps compareValues(left, right) >= 0 ? left
     * : right, i.e. the larger value (non-negative int32 bytes compare as the numbers). Returns
     * whether the right cell (ts2, x2) wins over the left (ts1, x1).
     */
    static boolean rightWins(long ts1, int x1, long ts2, int x2) {
        if (ts1 != ts2)
            return ts2 > ts1;
        return !(Integer.compare(x1, x2) >= 0);
    }

    /**
     * Commit.java. PORT: one record stands for Commit and its subclasses Proposal, Accepted, Agreed,
     * Committed (and their WithTTL forms); the partition key and table metadata are dropped.
     */
    @Tag("Commit")
    public record Commit(Ballot ballot, PartitionUpdate update) {

        public static Commit newPrepare(Ballot ballot) {
            return new Commit(ballot, PartitionUpdate.emptyUpdate());
        }

        public static Commit emptyCommit() {
            return new Commit(Ballot.none(), PartitionUpdate.emptyUpdate());
        }

        public static Commit newProposal(Ballot ballot, PartitionUpdate update) {
            update = PartitionUpdate.withTimestamp(update, ballot.unixMicros());
            return new Commit(ballot, update);
        }

        public boolean isAfter(Commit other) {
            return other == null || ballot.uuidTimestamp() > other.ballot.uuidTimestamp();
        }

        public boolean isSameOrAfter(Ballot otherBallot) {
            return otherBallot == null || otherBallot.equals(ballot) || ballot.uuidTimestamp() > otherBallot.uuidTimestamp();
        }

        public boolean isAfter(Ballot otherBallot) {
            return otherBallot == null || ballot.uuidTimestamp() > otherBallot.uuidTimestamp();
        }

        public boolean isBefore(Ballot otherBallot) {
            return otherBallot != null && ballot.uuidTimestamp() < otherBallot.uuidTimestamp();
        }

        public boolean hasBallot(Ballot ballot) {
            return this.ballot.equals(ballot);
        }

        public boolean hasSameBallot(Commit other) {
            return this.ballot.equals(other.ballot);
        }

        public static int compare(Commit a, Commit b) {
            return Long.compare(a.ballot.uuidTimestamp(), b.ballot.uuidTimestamp());
        }

        public static boolean isAfter(Commit testIsAfter, Commit testIsBefore) {
            return testIsAfter != null && testIsAfter.isAfter(testIsBefore);
        }

        public static boolean isAfter(Ballot testIsAfter, Ballot testIsBefore) {
            return testIsAfter != null && (testIsBefore == null || testIsAfter.uuidTimestamp() > testIsBefore.uuidTimestamp());
        }

        /** the latest of two ballots, or the first ballot if equal timestamps */
        public static Commit latest(Commit a, Commit b) {
            return (a == null | b == null) ? (a == null ? b : a) : a.ballot.uuidTimestamp() >= b.ballot.uuidTimestamp() ? a : b;
        }

        /** the latest of two ballots, or the first ballot if equal timestamps */
        public static Ballot latest(Ballot a, Ballot b) {
            return (a == null | b == null) ? (a == null ? b : a) : a.uuidTimestamp() >= b.uuidTimestamp() ? a : b;
        }

        /** Commit.Committed.latestCommitted. PORT: no TTL, so equal ballots keep a (no lastDeleted). */
        public static Commit latestCommitted(Commit a, Commit b) {
            int c = compare(a, b);
            if (c != 0)
                return c > 0 ? a : b;
            return a; // PORT: a instanceof CommittedWithTTL ? ((CommittedWithTTL)a).lastDeleted(b) : a
        }

        /** Commit.Accepted.latestAccepted. PORT: as latestCommitted; nulls handled as by Snapshot.merge's callers. */
        public static Commit latestAccepted(Commit a, Commit b) {
            // PORT: the source calls compare(a, b) on possibly-null Accepted values (Snapshot.merge, both
            // sides full snapshots); unreachable here (merge always meets an UnsafeSnapshot), guarded anyway.
            if (a == null || b == null)
                return a == null ? b : a;
            int c = compare(a, b);
            if (c != 0)
                return c > 0 ? a : b;
            return a;
        }
    }

    /** PrepareResponse.java. */
    @Tag("PrepareResponse")
    public record PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit) {}

    // ------------------------------------------------------------------------------------------
    // Messages among the replicas (net/Verb.java l.124-129, plus the QUORUM read)
    // PORT: Message<T> carries the sender (message.from()) and a callback id; here they are the
    // fields `from` (replica index) and `id` (the coordinator's callback id, -1 for "no callback").
    // ------------------------------------------------------------------------------------------

    /** PAXOS_PREPARE_REQ, payload Commit. */
    @Tag("PaxosPrepareReq")
    public record PaxosPrepareReq(int id, int from, Commit payload) implements MpMessage {}

    /** PAXOS_PREPARE_RSP, payload PrepareResponse. */
    @Tag("PaxosPrepareRsp")
    public record PaxosPrepareRsp(int id, int from, PrepareResponse payload) implements MpMessage {}

    /** PAXOS_PROPOSE_REQ, payload Commit. */
    @Tag("PaxosProposeReq")
    public record PaxosProposeReq(int id, int from, Commit payload) implements MpMessage {}

    /** PAXOS_PROPOSE_RSP, payload Boolean. */
    @Tag("PaxosProposeRsp")
    public record PaxosProposeRsp(int id, int from, boolean payload) implements MpMessage {}

    /** PAXOS_COMMIT_REQ, payload Agreed. */
    @Tag("PaxosCommitReq")
    public record PaxosCommitReq(int id, int from, Commit payload) implements MpMessage {}

    /** PAXOS_COMMIT_RSP, NoPayload. */
    @Tag("PaxosCommitRsp")
    public record PaxosCommitRsp(int id, int from) implements MpMessage {}

    /** PORT: READ_REQ of the QUORUM read (readRegular/fetchRows), reduced to "read x". */
    @Tag("ReadReq")
    public record ReadReq(int id, int from) implements MpMessage {}

    /** PORT: READ_RSP, reduced to the register's cell: present?, value, cell timestamp (Fix round 1, finding 7). */
    @Tag("ReadRsp")
    public record ReadRsp(int id, int from, boolean present, int x, long timestamp) implements MpMessage {}

    static boolean isVerb(MpMessage m) {
        return m instanceof PaxosPrepareReq || m instanceof PaxosProposeReq || m instanceof PaxosCommitReq
                || m instanceof ReadReq;
    }

    static int responseId(MpMessage m) {
        if (m instanceof PaxosPrepareRsp r) return r.id();
        if (m instanceof PaxosProposeRsp r) return r.id();
        if (m instanceof PaxosCommitRsp r) return r.id();
        if (m instanceof ReadRsp r) return r.id();
        return Integer.MIN_VALUE;
    }

    static boolean isResponse(MpMessage m) {
        return m instanceof PaxosPrepareRsp || m instanceof PaxosProposeRsp || m instanceof PaxosCommitRsp
                || m instanceof ReadRsp;
    }

    // PORT (Fix round 1, finding 1): the idle loop receives every message a replica can be sent,
    // responses included (they are dispatched to their callback or discarded).
    static boolean isNodeInput(MpMessage m) {
        return isVerb(m) || isResponse(m) || m instanceof Cas || m instanceof Read;
    }

    static boolean isReply(MpMessage m) {
        return m instanceof CasResult || m instanceof Value || m instanceof Failed;
    }

    // ------------------------------------------------------------------------------------------
    // Exceptions the coordinator gives up with (all answered with Failed)
    // ------------------------------------------------------------------------------------------

    static class WriteTimeoutException extends RuntimeException {
        WriteTimeoutException(String m) {
            super(m, null, false, false);
        }
    }

    static final class CasWriteTimeoutException extends WriteTimeoutException {
        CasWriteTimeoutException(String m) {
            super(m);
        }
    }

    static final class CasWriteUnknownResultException extends RuntimeException {
        CasWriteUnknownResultException(String m) {
            super(m, null, false, false);
        }
    }

    static final class ReadTimeoutException extends RuntimeException {
        ReadTimeoutException(String m) {
            super(m, null, false, false);
        }
    }

    /** PORT: the three consistency levels used; blockFor for RF = 3. */
    enum ConsistencyLevel {
        SERIAL, QUORUM, ANY;

        int blockFor() {
            return this == ANY ? 1 : 2;
        }
    }

    record Pair<L, R>(L left, R right) {
        static <L, R> Pair<L, R> create(L l, R r) {
            return new Pair<>(l, r);
        }
    }

    record PaxosBallotAndContention(Ballot ballot, int contentions) {}

    // ------------------------------------------------------------------------------------------
    // Replica state: PaxosState.Snapshot (l.158-268)
    // ------------------------------------------------------------------------------------------

    static class Snapshot {
        final Ballot promised;
        final Ballot promisedWrite; // <= promised
        final Commit accepted; // if already committed, this will be null
        final Commit committed;

        Snapshot(Ballot promised, Ballot promisedWrite, Commit accepted, Commit committed) {
            // PORT: `promised == promisedWrite` (identity) as equals
            assert Commit.isAfter(promised, promisedWrite) || promised.equals(promisedWrite);
            assert accepted == null || committed.isBefore(accepted.ballot());
            this.promised = promised;
            this.promisedWrite = promisedWrite;
            this.accepted = accepted;
            this.committed = committed;
        }

        Ballot latestWitnessedOrLowBound() {
            // warn: if proposal has same timestamp as promised, we should prefer accepted
            // since (if different) it reached a quorum of promises; this means providing it as first argument
            Ballot latest;
            latest = Commit.latest(accepted, committed).ballot();
            latest = Commit.latest(latest, promised);
            latest = Commit.latest(latest, Ballot.none()); // PORT: ballotTracker().getLowBound() is Ballot.none() (no v2 repair)
            return latest;
        }

        static Snapshot merge(Snapshot a, Snapshot b) {
            if (a == null || b == null)
                return a == null ? b : a;

            Commit committed = Commit.latestCommitted(a.committed, b.committed);
            if (a instanceof UnsafeSnapshot && b instanceof UnsafeSnapshot)
                return new UnsafeSnapshot(committed);

            Commit accepted;
            Ballot promised, promisedWrite;
            if (a instanceof UnsafeSnapshot || b instanceof UnsafeSnapshot) {
                if (a instanceof UnsafeSnapshot)
                    a = b; // we already have the winning Committed saved above, so just want the full snapshot (if either)

                if (committed == a.committed)
                    return a;

                promised = a.promised;
                promisedWrite = a.promisedWrite;
                accepted = Commit.isAfter(a.accepted, committed) ? a.accepted : null;
            } else {
                accepted = Commit.latestAccepted(a.accepted, b.accepted);
                accepted = Commit.isAfter(accepted, committed) ? accepted : null;
                promised = Commit.latest(a.promised, b.promised);
                promisedWrite = Commit.latest(a.promisedWrite, b.promisedWrite);
            }

            return new Snapshot(promised, promisedWrite, accepted, committed);
        }

        // PORT: removeExpired(nowInSec) is the identity: no time, no TTL, no gc_grace purging.
        Snapshot removeExpired() {
            return this;
        }
    }

    // used to permit recording Committed outcomes without waiting for initial read
    static final class UnsafeSnapshot extends Snapshot {
        UnsafeSnapshot(Commit committed) {
            super(Ballot.none(), Ballot.none(), null, committed);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Coordinator callbacks
    // ------------------------------------------------------------------------------------------

    /** PORT: what a coordinator awaits; latchCount() is the CountDownLatch's count. */
    interface Callback {
        int latchCount();

        void onResponse(MpMessage message);
    }

    /** AbstractPaxosCallback. PORT: the CountDownLatch is an int; await() is Node.await (no timeout). */
    abstract static class AbstractPaxosCallback implements Callback {
        protected int latch;
        protected final int targets;

        AbstractPaxosCallback(int targets) {
            this.targets = targets;
            latch = targets;
        }

        public int latchCount() {
            return latch;
        }

        public int getResponseCount() {
            return targets - latch;
        }

        protected void latchDecrement() {
            if (latch > 0)
                latch--;
        }
    }

    /** service/paxos/v1/PrepareCallback.java. */
    static final class PrepareCallback extends AbstractPaxosCallback {
        boolean promised = true;
        Commit mostRecentCommit;
        Commit mostRecentInProgressCommit;
        // PORT: ConcurrentHashMap<InetAddressAndPort, Commit> as a map ordered by replica index.
        private final Map<Integer, Commit> commitsByReplica = new TreeMap<>();
        int requestId; // PORT: the callback's message id, for Node.deliverLate

        PrepareCallback(int targets) {
            super(targets);
            // need to inject the right key in the empty commit so comparing with empty commits in the response works as expected
            mostRecentCommit = Commit.emptyCommit();
            mostRecentInProgressCommit = Commit.emptyCommit();
        }

        public void onResponse(MpMessage msg) {
            PaxosPrepareRsp message = (PaxosPrepareRsp) msg;
            PrepareResponse response = message.payload();

            // We set the mostRecentInProgressCommit even if we're not promised as, in that case, the ballot of that commit
            // will be used to avoid generating a ballot that has not chance to win on retry (think clock skew).
            if (response.inProgressCommit().isAfter(mostRecentInProgressCommit))
                mostRecentInProgressCommit = response.inProgressCommit();

            if (!response.promised()) {
                promised = false;
                while (latch > 0)
                    latchDecrement();
                return;
            }

            commitsByReplica.put(message.from(), response.mostRecentCommit());
            if (response.mostRecentCommit().isAfter(mostRecentCommit))
                mostRecentCommit = response.mostRecentCommit();

            latchDecrement();
        }

        public MissingMostRecentCommit replicasMissingMostRecentCommit() {
            // PORT: the paxosStatePurging() == legacy branch (MRC older than the paxos TTL is ignored) needs
            // wall-clock time; with counter ballots no MRC is ever too old, so the branch is dropped.
            // PORT (Fix round 2, finding 3): the source returns a live Iterables.filter view over
            // commitsByReplica (PrepareCallback l.110), not a materialised list; so does this.
            return new MissingMostRecentCommit(this);
        }
    }

    /**
     * PORT (Fix round 2, finding 3): Iterables.filter(commitsByReplica.keySet(), inetAddress ->
     * !commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot)) as a live view: each
     * traversal re-reads the callback's map and mostRecentCommit, element by element. The traversal
     * visits the replicas in index order and tests each key when it reaches it, as a weakly consistent
     * ConcurrentHashMap iterator may see or miss an entry added during the traversal.
     */
    static final class MissingMostRecentCommit {
        final PrepareCallback callback;

        MissingMostRecentCommit(PrepareCallback callback) {
            this.callback = callback;
        }

        boolean contains(int replica) {
            Commit c = callback.commitsByReplica.get(replica);
            return c != null && !c.ballot().equals(callback.mostRecentCommit.ballot());
        }
    }

    /** service/paxos/v1/ProposeCallback.java. */
    static final class ProposeCallback extends AbstractPaxosCallback {
        private int accepts = 0;
        private final int requiredAccepts;
        private final boolean failFast;

        ProposeCallback(int totalTargets, int requiredTargets, boolean failFast) {
            super(totalTargets);
            this.requiredAccepts = requiredTargets;
            this.failFast = failFast;
        }

        public void onResponse(MpMessage m) {
            PaxosProposeRsp msg = (PaxosProposeRsp) m;
            if (msg.payload())
                accepts++;
            latchDecrement();

            if (isSuccessful() || (failFast && (latch + accepts < requiredAccepts))) {
                while (latch > 0)
                    latchDecrement();
            }
        }

        public int getAcceptCount() {
            return accepts;
        }

        public boolean isSuccessful() {
            return accepts >= requiredAccepts;
        }

        // Note: this is only reliable if !failFast
        public boolean isFullyRefused() {
            // We need to check the latch first to avoid racing with a late arrival
            // between the latch check and the accepts one
            return latch == 0 && accepts == 0;
        }
    }

    /** PORT: AbstractWriteResponseHandler (WriteResponseHandler) of commitPaxos, reduced to counting blockFor acks. */
    static final class WriteResponseHandler implements Callback {
        private int pending;

        WriteResponseHandler(int blockFor) {
            pending = blockFor;
        }

        public int latchCount() {
            return pending;
        }

        public void onResponse(MpMessage m) {
            if (pending > 0)
                pending--;
        }
    }

    /**
     * PORT: ReadCallback + DataResolver of the QUORUM read (readRegular/fetchRows), reduced to:
     * wait for blockFor responses and resolve the register by its write timestamp (last write wins).
     */
    static final class ReadCallback implements Callback {
        private int pending;
        private boolean present = false;
        private int x = 0; // the row absent counts as 0 (INTERFACE.md)
        private long timestamp = 0;

        ReadCallback(int blockFor) {
            pending = blockFor;
        }

        public int latchCount() {
            return pending;
        }

        public void onResponse(MpMessage m) {
            ReadRsp r = (ReadRsp) m;
            // PORT: DataResolver merge = Cells.reconcile of the responses' cells (Fix round 1, finding 7)
            if (r.present() && (!present || rightWins(timestamp, x, r.timestamp(), r.x()))) {
                present = true;
                timestamp = r.timestamp();
                x = r.x();
            }
            if (pending > 0)
                pending--;
        }

        int resolve() {
            return x;
        }
    }

    // ------------------------------------------------------------------------------------------
    // One replica process: PaxosState + the table + verb handlers + StorageProxy (coordinator)
    // ------------------------------------------------------------------------------------------

    final class Node implements Runnable {
        final int id;
        final Tid[] nodes;
        final Tid[] clients;

        // PaxosState: PORT: one partition, in memory (no system.paxos table, no ACTIVE/RECENT caches).
        Snapshot current = new Snapshot(Ballot.none(), Ballot.none(), null, Commit.emptyCommit());

        // PORT: the base table of the partition: one int cell x with its cell timestamp (absent: row absent).
        boolean xPresent = false;
        int x = 0;
        long xTimestamp = 0;

        // PORT: ClientState.lastTimestampMicros; here the highest ballot counter this process has seen
        // (Fix round 1, finding 2: every ballot of every message it receives, and its own ballots).
        long lastTimestampMicros = 0;
        // PORT: the coordinator's callback ids (Message ids).
        int nextRequestId = 0;
        // PORT (Fix round 1, finding 1): MessagingService's callback map. A callback stays registered
        // after its await returns (the source keeps it until its RPC timeout expires; here there is no
        // time, so it stays), and a late response still runs its onResponse, as in the source.
        final Map<Integer, Callback> callbacks = new TreeMap<>();
        // PORT: the attempt budget replaces the contention deadline; ballots generated for the current request.
        int attempts = 0;

        Node(int id, Tid[] nodes, Tid[] clients) {
            this.id = id;
            this.nodes = nodes;
            this.clients = clients;
        }

        @Override
        public void run() {
            // PORT: the replica's message loop (MessagingService + stages); a replica never terminates.
            while (true) {
                MpMessage m = Channel.recv(model, CassandraPaxosV1::isNodeInput);
                if (m instanceof Cas c)
                    clientCas(c);
                else if (m instanceof Read r)
                    clientRead(r);
                else
                    dispatch(m);
            }
        }

        /**
         * PORT (Fix round 1, findings 1 and 2): one received replica message: its ballots are
         * observed, then a verb runs its handler, a response runs its callback's onResponse
         * (ResponseVerbHandler) or, with no callback registered (id -1: sendCommit, ANY commits), is
         * discarded.
         */
        void dispatch(MpMessage m) {
            observeAll(m);
            if (isVerb(m)) {
                doVerb(m);
            } else {
                Callback callback = callbacks.get(responseId(m));
                if (callback != null)
                    callback.onResponse(m);
                // else: PORT: no callback for this id: the response is received and dropped
            }
        }

        /** PORT (Fix round 1, finding 2): the interface's "seen": every ballot a received message carries. */
        void observeAll(MpMessage m) {
            if (m instanceof PaxosPrepareReq r) {
                observe(r.payload().ballot());
            } else if (m instanceof PaxosProposeReq r) {
                observe(r.payload().ballot());
            } else if (m instanceof PaxosCommitReq r) {
                observe(r.payload().ballot());
            } else if (m instanceof PaxosPrepareRsp r) {
                observe(r.payload().inProgressCommit().ballot());
                observe(r.payload().mostRecentCommit().ballot());
            } else if (m instanceof ReadRsp r) {
                if (r.present() && r.timestamp() > lastTimestampMicros)
                    lastTimestampMicros = r.timestamp(); // a cell timestamp is a ballot counter
            }
        }

        /** PORT: register a callback under a fresh id (MessagingService.sendWithCallback). */
        int register(Callback callback) {
            int requestId = nextRequestId++;
            callbacks.put(requestId, callback);
            return requestId;
        }

        /**
         * PORT (Fix round 1, finding 3): between the latch reaching zero and a read of the callback's
         * fields, the source may still run onResponse for late responses on other threads. Here the
         * coordinator may receive any number (zero included) of pending responses for that callback
         * first: non-blocking receives, the strategy choosing ⊥ or a pending response.
         */
        void deliverLate(int requestId) {
            final int late = requestId;
            final int self = id;
            while (true) {
                // PORT (Fix round 2, finding 3): also this node's own pending prepare request of that callback,
                // so that the local prepare (a local stage task in the source) can run and answer at these points.
                java.util.Optional<MpMessage> m = Channel.tryRecv(model, msg -> responseId(msg) == late
                        || (msg instanceof PaxosPrepareReq r && r.id() == late && r.from() == self));
                if (m.isEmpty())
                    return;
                dispatch(m.get());
            }
        }

        void send(int replica, MpMessage m) {
            Channel.send(model, nodes[replica], m);
        }

        void observe(Ballot b) {
            if (b.unixMicros() > lastTimestampMicros)
                lastTimestampMicros = b.unixMicros();
        }

        /**
         * PORT: callback.await() (AbstractPaxosCallback l.60-68 without the RPC timeout: the waits are
         * timeout-free by the common model's "no time"). While waiting, the process keeps receiving
         * every replica message (verbs and all responses, Fix round 1, finding 1).
         */
        void await(int requestId, Callback callback) {
            while (callback.latchCount() > 0) {
                MpMessage m = Channel.recv(model, msg -> isVerb(msg) || isResponse(msg));
                dispatch(m);
            }
        }

        // ---------------------------------------------------------------- client entry points

        /** PORT: the CQL layer (ModificationStatement/CQL3CasRequest) calling StorageProxy.cas. */
        void clientCas(Cas request) {
            attempts = 0;
            MpMessage reply;
            try {
                Integer result = legacyCas(request);
                // null if the operation succeeds in updating the row, or the current values corresponding to conditions.
                reply = result == null ? new CasResult(request.client(), 1, request.value())
                                       : new CasResult(request.client(), 0, result);
            } catch (WriteTimeoutException | CasWriteUnknownResultException | ReadTimeoutException e) {
                // PORT: a CAS timeout / unknown-result error to the client is the interface's Failed.
                reply = new Failed(request.client());
            }
            Channel.send(model, clients[request.client()], reply);
        }

        /** PORT: the CQL layer (SelectStatement at SERIAL) calling StorageProxy.read -> readWithPaxos. */
        void clientRead(Read request) {
            attempts = 0;
            MpMessage reply;
            try {
                reply = new Value(request.client(), legacyReadWithPaxos());
            } catch (WriteTimeoutException | CasWriteUnknownResultException | ReadTimeoutException e) {
                reply = new Failed(request.client());
            }
            Channel.send(model, clients[request.client()], reply);
        }

        // ---------------------------------------------------------------- StorageProxy (coordinator)

        /** StorageProxy.legacyCas (l.329-437). PORT: returns null when applied, else the current x. */
        Integer legacyCas(Cas request) {
            Function<Ballot, Pair<PartitionUpdate, Integer>> updateProposer = ballot ->
            {
                // read the current values and check they validate the conditions
                ConsistencyLevel readConsistency = ConsistencyLevel.QUORUM; // PORT: consistencyForPaxos is SERIAL

                int current = readOne(readConsistency);

                // PORT: request.appliesTo(current): IF x = expected, the absent row counting as 0
                if (!(current == request.expected())) {
                    return Pair.create(PartitionUpdate.emptyUpdate(), current);
                }

                // Create the desired updates
                // PORT: request.makeUpdates(current, clientState, ballot): SET x = value; no triggers
                PartitionUpdate updates = new PartitionUpdate(false, request.value(), ballot.unixMicros());

                return Pair.create(updates, null);
            };

            // PORT: metrics and the exception re-wrapping of legacyCas are dropped; the exceptions propagate.
            return doPaxos(ConsistencyLevel.SERIAL,
                           ConsistencyLevel.QUORUM,
                           ConsistencyLevel.QUORUM,
                           updateProposer);
        }

        /** StorageProxy.doPaxos (l.478-555). */
        Integer doPaxos(ConsistencyLevel consistencyForPaxos,
                        ConsistencyLevel consistencyForReplayCommits,
                        ConsistencyLevel consistencyForCommit,
                        Function<Ballot, Pair<PartitionUpdate, Integer>> createUpdateProposal) {
            int contentions = 0;
            try {
                // PORT: validateForCas/validateForCasCommit dropped (fixed levels).
                while (attempts < A) // PORT: was while (nanoTime() < deadline)
                {
                    // PORT: ReplicaPlans.forPaxos: all three replicas, quorum 2 (no liveness check).
                    PaxosBallotAndContention pair = beginAndRepairPaxos(consistencyForPaxos,
                                                                        consistencyForReplayCommits);

                    final Ballot ballot = pair.ballot();
                    contentions += pair.contentions();

                    Pair<PartitionUpdate, Integer> proposalPair = createUpdateProposal.apply(ballot);
                    // See method javadoc: null here is code for "stop here and return null".
                    if (proposalPair == null)
                        return null;

                    Commit proposal = Commit.newProposal(ballot, proposalPair.left());
                    if (proposePaxos(proposal, true)) {
                        // We skip committing accepted updates when they are empty. This is an optimization which works
                        // because we also skip replaying those same empty update in beginAndRepairPaxos (see the longer
                        // comment there). As empty update are somewhat common (serial reads and non-applying CAS propose
                        // them), this is worth bothering.
                        if (!proposal.update().isEmpty())
                            commitPaxos(proposal, consistencyForCommit, true);
                        Integer result = proposalPair.right();
                        return result;
                    }

                    contentions++;

                    // PORT: Uninterruptibles.sleepUninterruptibly(random 0-99 ms) dropped (no time)
                    // continue to retry
                }
            } catch (CasWriteTimeoutException e) {
                // PORT: contentions += e.contentions (metrics only) dropped
                throw e;
            }
            // PORT: catch (WriteTimeoutException e) -> CasWriteTimeoutException: unreachable, because the common model
            // assumes timeout-free RPC waits ("no time", INTERFACE.md); not because there are no failures (Fix round 1, finding 8).
            // PORT: finally recordCasContention(...) (metrics) dropped.

            throw new CasWriteTimeoutException("doPaxos: attempt budget exhausted");
        }

        /** StorageProxy.beginAndRepairPaxos (l.557-680). */
        PaxosBallotAndContention beginAndRepairPaxos(ConsistencyLevel consistencyForPaxos,
                                                     ConsistencyLevel consistencyForCommit) {
            PrepareCallback summary = null;
            int contentions = 0;
            while (attempts < A) // PORT: was while (nanoTime() < deadline)
            {
                attempts++; // PORT: one attempt of the budget per fresh ballot
                // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
                // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
                // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
                // out-of-order (#7801).
                if (summary != null)
                    deliverLate(summary.requestId); // PORT (Fix round 1, finding 3): late responses may still land before this read
                long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + summary.mostRecentInProgressCommit.ballot().unixMicros();
                Ballot ballot = nextBallot(minTimestampMicrosToUse); // PORT: flag GLOBAL/LOCAL dropped

                // prepare
                Commit toPrepare = Commit.newPrepare(ballot);
                summary = preparePaxos(toPrepare);
                deliverLate(summary.requestId); // PORT (Fix round 1, finding 3)
                if (!summary.promised) {
                    contentions++;
                    // PORT: sleep a random amount to give the other proposer a chance to finish -- dropped
                    continue;
                }

                deliverLate(summary.requestId); // PORT (Fix round 1, finding 3)
                Commit inProgress = summary.mostRecentInProgressCommit;
                deliverLate(summary.requestId); // PORT (Fix round 1, finding 3)
                Commit mostRecent = summary.mostRecentCommit;

                // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
                // needs to be completed, so do it.
                // One special case we make is for update that are empty (which are proposed by serial reads and
                // non-applying CAS). [...] it is safe to skip committing empty updates _as long as_ we also skip
                // replying them below. (comment abridged; StorageProxy.java l.623-637)
                if (!inProgress.update().isEmpty() && inProgress.isAfter(mostRecent)) {
                    Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update());
                    if (proposePaxos(refreshedInProgress, false)) {
                        commitPaxos(refreshedInProgress, consistencyForCommit, false);
                    } else {
                        // sleep a random amount to give the other proposer a chance to finish
                        contentions++;
                        // PORT: sleep dropped
                    }
                    continue;
                }

                // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
                // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
                // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
                // mean we lost messages), we pro-actively "repair" those nodes, and retry.
                deliverLate(summary.requestId); // PORT (Fix round 1, finding 3)
                MissingMostRecentCommit missingMRC = summary.replicasMissingMostRecentCommit();
                if (size(missingMRC) > 0) { // Iterables.size(missingMRC)
                    deliverLate(summary.requestId); // PORT (Fix round 2, finding 3): between the size test and the send traversal
                    sendCommit(mostRecent, missingMRC);
                    // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                    // for all the missingMRC to acknowledge this commit and then move on with proposing our value. [...]
                    continue;
                }

                return new PaxosBallotAndContention(ballot, contentions);
            }

            throw new CasWriteTimeoutException("beginAndRepairPaxos: attempt budget exhausted");
        }

        /** BallotGenerator.Global.nextBallot(minUnixMicros, flag) -> Default.next. */
        Ballot nextBallot(long minUnixMicros) {
            long unixMicros = getTimestampForPaxos(minUnixMicros);
            return Ballot.atUnixMicrosWithLsb(unixMicros, id);
        }

        /** ClientState.getTimestampForPaxos (src-4.1 ClientState.java l.285-299). */
        long getTimestampForPaxos(long minUnixMicros) {
            long current = Math.max(Long.MIN_VALUE, minUnixMicros); // PORT: no clock (currentTimeMillis() * 1000)
            long last = lastTimestampMicros;
            long tstamp = last >= current ? last + 1 : current;
            // PORT: the source keeps lastTimestampMicros unchanged when tstamp == minUnixMicros (CASSANDRA-11991)
            // and relies on the random lsb for uniqueness; with (counter, id) ballots that would repeat a ballot,
            // so the own ballot is always recorded as seen (the interface's "1 + the highest ballot seen").
            lastTimestampMicros = tstamp;
            return tstamp;
        }

        /**
         * PORT (Fix round 2, finding 3): Iterables.size over the live view; late prepare responses may
         * run before each element is tested.
         */
        int size(MissingMostRecentCommit view) {
            int count = 0;
            for (int replica = 0; replica < N; replica++) {
                deliverLate(view.callback.requestId);
                if (view.contains(replica))
                    count++;
            }
            return count;
        }

        /** Unlike commitPaxos, this does not wait for replies */
        void sendCommit(Commit commit, MissingMostRecentCommit replicas) {
            PaxosCommitReq message = new PaxosCommitReq(-1, id, commit); // PORT: Message.out without callback: id -1
            for (int target = 0; target < N; target++) // for (InetAddressAndPort target : replicas)
            {
                deliverLate(replicas.callback.requestId); // PORT (Fix round 2, finding 3): the view is traversed again, live
                if (replicas.contains(target))
                    send(target, message);
            }
        }

        PrepareCallback preparePaxos(Commit toPrepare) {
            // PORT: requiredParticipants() = 2 (SERIAL, RF 3)
            PrepareCallback callback = new PrepareCallback(ConsistencyLevel.SERIAL.blockFor());
            int requestId = register(callback); // PORT: sendWithCallback registers the callback
            callback.requestId = requestId; // PORT: for deliverLate
            PaxosPrepareReq message = new PaxosPrepareReq(requestId, id, toPrepare);

            for (int replica = 0; replica < N; replica++) // contacts(): all three replicas
            {
                // PORT: replica.isSelf(): the source runs doPrepare on the local PAXOS_PREPARE_REQ stage
                // (asynchronously); here the request is sent to this process itself and served by its loop.
                send(replica, message);
            }

            await(requestId, callback);
            return callback;
        }

        /**
         * Propose the {@param proposal} accoding to the {@param replicaPlan}.
         * When {@param backoffIfPartial} is true, the proposer backs off when seeing the proposal being accepted by some but not a quorum.
         * The result of the cooresponding CAS in uncertain as the accepted proposal may or may not be spread to other nodes in later rounds.
         */
        boolean proposePaxos(Commit proposal, boolean backoffIfPartial) {
            // PORT: contacts().size() = 3, requiredParticipants() = 2
            ProposeCallback callback = new ProposeCallback(N, ConsistencyLevel.SERIAL.blockFor(), !backoffIfPartial);
            int requestId = register(callback); // PORT: sendWithCallback registers the callback
            PaxosProposeReq message = new PaxosProposeReq(requestId, id, proposal);
            for (int replica = 0; replica < N; replica++) {
                // PORT: replica.isSelf(): local stage execution replaced by a send to itself (as in preparePaxos)
                send(replica, message);
            }
            await(requestId, callback);

            if (callback.isSuccessful())
                return true;

            if (backoffIfPartial && !callback.isFullyRefused())
                throw new CasWriteUnknownResultException("accepted by " + callback.getAcceptCount() + " of 2 required");

            return false;
        }

        void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints) {
            boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;

            WriteResponseHandler responseHandler = null;
            int requestId = -1; // PORT: no callback registered unless the commit blocks
            // PORT: ReplicaPlans.forWrite(..., writeAll): all three replicas, all alive
            if (shouldBlock) {
                responseHandler = new WriteResponseHandler(consistencyLevel.blockFor());
                requestId = register(responseHandler); // PORT: sendWriteWithCallback registers the handler
            }

            PaxosCommitReq message = new PaxosCommitReq(requestId, id, proposal);
            for (int replica = 0; replica < N; replica++) // replicaPlan.liveAndDown()
            {
                // PORT: checkHintOverload, the not-alive branch and hints dropped (no failures)
                if (shouldBlock) {
                    // PORT: replica.isSelf() -> commitPaxosLocal(...): replaced by a send to itself
                    send(replica, message);
                } else {
                    send(replica, message);
                }
            }

            if (shouldBlock)
                await(requestId, responseHandler);
        }

        /**
         * StorageProxy.readOne(command, QUORUM) -> read -> readRegular -> fetchRows. PORT: the QUORUM
         * read is reduced to fetchRows: one request to every replica, the first blockFor responses
         * resolved by write timestamp; no digest requests, no speculative retry, no read repair.
         */
        int readOne(ConsistencyLevel consistencyLevel) {
            return fetchRows(consistencyLevel);
        }

        int fetchRows(ConsistencyLevel consistencyLevel) {
            ReadCallback handler = new ReadCallback(consistencyLevel.blockFor());
            int requestId = register(handler); // PORT: the read executor registers its callback
            ReadReq message = new ReadReq(requestId, id);
            for (int replica = 0; replica < N; replica++)
                send(replica, message);
            await(requestId, handler);
            return handler.resolve();
        }

        /** StorageProxy.legacyReadWithPaxos (l.1876-1966). PORT: returns x. */
        int legacyReadWithPaxos() {
            ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
            int result;
            final ConsistencyLevel consistencyForReplayCommitsOrFetch = ConsistencyLevel.QUORUM; // consistencyLevel is SERIAL

            try {
                // Commit an empty update to make sure all in-progress updates that should be finished first is, _and_
                // that no other in-progress can get resurrected.
                Function<Ballot, Pair<PartitionUpdate, Integer>> updateProposer =
                    !isLinearizable
                    ? ballot -> null
                    : ballot -> Pair.create(PartitionUpdate.emptyUpdate(), null);
                // When replaying, we commit at quorum/local quorum, as we want to be sure the following read (done at
                // quorum/local_quorum) sees any replayed updates. Our own update is however empty, and those don't even
                // get committed due to an optimiation described in doPaxos/beingRepairAndPaxos, so the commit
                // consistency is irrelevant (we use ANY just to emphasis that we don't wait on our commit).
                doPaxos(consistencyLevel,
                        consistencyForReplayCommitsOrFetch,
                        ConsistencyLevel.ANY,
                        updateProposer);
            } catch (WriteTimeoutException e) {
                throw new ReadTimeoutException("legacyReadWithPaxos: " + e.getMessage());
            }
            // PORT: catch (WriteFailureException e) -> ReadFailureException: no failures here.

            result = fetchRows(consistencyForReplayCommitsOrFetch);
            // PORT: the metric-marking catch blocks and the latency finally are dropped.
            return result;
        }

        // ---------------------------------------------------------------- verb handlers (replica side)

        void doVerb(MpMessage m) {
            // PORT: AbstractPaxosVerbHandler.doVerb's out-of-range-token check dropped (one partition, owned by all).
            if (m instanceof PaxosPrepareReq r)
                prepareVerbHandlerProcessMessage(r);
            else if (m instanceof PaxosProposeReq r)
                proposeVerbHandlerProcessMessage(r);
            else if (m instanceof PaxosCommitReq r)
                paxosCommitRequestHandlerDoVerb(r);
            else if (m instanceof ReadReq r)
                readCommandVerbHandlerDoVerb(r);
            else
                throw new IllegalStateException("not a verb: " + m);
        }

        /** PrepareVerbHandler.processMessage. */
        void prepareVerbHandlerProcessMessage(PaxosPrepareReq message) {
            PaxosPrepareRsp reply = new PaxosPrepareRsp(message.id(), id, doPrepare(message.payload()));
            send(message.from(), reply);
        }

        /** PrepareVerbHandler.doPrepare. */
        PrepareResponse doPrepare(Commit toPrepare) {
            return legacyPrepare(toPrepare);
        }

        /** ProposeVerbHandler.processMessage. */
        void proposeVerbHandlerProcessMessage(PaxosProposeReq message) {
            Boolean response = doPropose(message.payload());
            PaxosProposeRsp reply = new PaxosProposeRsp(message.id(), id, response);
            send(message.from(), reply);
        }

        /** ProposeVerbHandler.doPropose. */
        Boolean doPropose(Commit proposal) {
            return legacyPropose(proposal);
        }

        /** PaxosCommit.RequestHandler.doVerb / execute (PaxosCommit.java l.301-323). */
        void paxosCommitRequestHandlerDoVerb(PaxosCommitReq message) {
            // PORT: Paxos.isInRangeAndShouldProcess(...) is always true (one partition, owned by all).
            commitDirect(message.payload());
            // NOTE: for correctness, this must be our last action, so that we cannot throw an error and send both a response and a failure response
            send(message.from(), new PaxosCommitRsp(message.id(), id));
        }

        /** PORT: ReadCommandVerbHandler, reduced to returning the local row of x with its timestamp. */
        void readCommandVerbHandlerDoVerb(ReadReq message) {
            send(message.from(), new ReadRsp(message.id(), id, xPresent, x, xTimestamp));
        }

        // ---------------------------------------------------------------- PaxosState (replica side)

        /** PaxosState.legacyPrepare (l.719-761). */
        PrepareResponse legacyPrepare(Commit toPrepare) {
            // PORT: getUnsafe/synchronized/maybeLoad dropped: one in-memory state, one thread.
            while (true) {
                // ignore nowInSec when merging as this can only be an issue during the transition period, so the unbounded
                // problem of CASSANDRA-12043 is not an issue
                Snapshot realBefore = current;
                Snapshot before = realBefore.removeExpired();
                Ballot latest = before.latestWitnessedOrLowBound();
                if (toPrepare.isAfter(latest)) {
                    Snapshot after = new Snapshot(toPrepare.ballot(), toPrepare.ballot(), realBefore.accepted, realBefore.committed);
                    if (compareAndSet(realBefore, after)) {
                        // PORT: SystemKeyspace.savePaxosWritePromise dropped (in-memory state)
                        return new PrepareResponse(true, before.accepted == null ? Commit.emptyCommit() /* PORT: Accepted.none */ : before.accepted, before.committed);
                    }
                } else {
                    // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                    return new PrepareResponse(false, new Commit(before.promised, toPrepare.update()), before.committed);
                }
            }
        }

        /** PaxosState.legacyPropose (l.763-805). */
        Boolean legacyPropose(Commit proposal) {
            // PORT: the AcceptedWithTTL wrapping (paxosStatePurging legacy) dropped: no TTL.
            while (true) {
                Snapshot realBefore = current;
                Snapshot before = realBefore.removeExpired();
                boolean accept = proposal.isSameOrAfter(before.latestWitnessedOrLowBound());
                if (accept) {
                    if (proposal.hasSameBallot(before.committed) ||
                        compareAndSet(realBefore,
                                      new Snapshot(realBefore.promised, realBefore.promisedWrite,
                                                   proposal /* PORT: new Accepted(proposal) */, realBefore.committed))) {
                        // PORT: SystemKeyspace.savePaxosProposal dropped (in-memory state)
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }

        /** PORT: currentUpdater.compareAndSet on the one thread of this process: always succeeds. */
        boolean compareAndSet(Snapshot expect, Snapshot update) {
            if (current != expect)
                return false;
            current = update;
            return true;
        }

        /** PaxosState.commitDirect (l.677-686). */
        void commitDirect(Commit commit) {
            applyCommit(commit, apply -> {
                // PORT: tryGetUnsafe(...) != null always: the in-memory state is always present
                current = Snapshot.merge(current, new UnsafeSnapshot(apply)); // currentUpdater.accumulateAndGet(state, new UnsafeSnapshot(apply), Snapshot::merge)
            });
        }

        /** PaxosState.applyCommit (l.688-717). */
        void applyCommit(Commit commit, java.util.function.Consumer<Commit> postCommit) {
            // PORT: CommittedWithTTL.withDefaultTTL (legacy purging) dropped: no TTL.
            // PORT: the truncation check (ballot >= truncatedAt) is always true: no truncation.
            {
                // Mutation mutation = commit.makeMutation(); Keyspace.open(...).apply(mutation, true);
                applyMutation(commit);
            }
            // PORT: SystemKeyspace.savePaxosCommit(commit) dropped (in-memory state)
            postCommit.accept(commit);
        }

        /**
         * PORT: Keyspace.apply of the commit's mutation on the base table: an empty update writes
         * nothing; a write of x carries its cell timestamp (the proposing ballot's counter) and is reconciled by Cells.reconcile.
         */
        void applyMutation(Commit commit) {
            if (commit.update().isEmpty())
                return;
            // PORT (Fix round 1, finding 7): reconcile by the update's cell timestamp (Cells.reconcile), not by ballot
            if (!xPresent || rightWins(xTimestamp, x, commit.update().timestamp(), commit.update().x())) {
                xPresent = true;
                x = commit.update().x();
                xTimestamp = commit.update().timestamp();
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Clients (INTERFACE.md, verbatim behaviour of the Rust client)
    // ------------------------------------------------------------------------------------------

    static int value(int i, int j) {
        return 1 + i + K * j;
    }

    void client(int i, int q, Tid node) {
        int known = 0;
        for (int j = 0; j < q; j++) {
            if ((i + j) % 2 == 0) {
                Channel.send(model, node, new Cas(i, known, value(i, j)));
            } else {
                Channel.send(model, node, new Read(i));
            }
            MpMessage rep = Channel.recv(model, CassandraPaxosV1::isReply);
            if (rep instanceof CasResult r) {
                known = r.value();
            } else if (rep instanceof Value v) {
                known = v.value();
            } else if (rep instanceof Failed) {
                // keeps known
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Program entry
    // ------------------------------------------------------------------------------------------

    @Override
    public void run() {
        Tid[] nodes = new Tid[N];
        Tid[] clients = new Tid[K];
        MpThread[] replicaThreads = new MpThread[N];
        MpThread[] clientThreads = new MpThread[K];
        // Create every process first (the task id is minted at creation), so that each knows every
        // address before any of them runs; then start replicas in index order, then clients.
        for (int i = 0; i < N; i++) {
            Node node = new Node(i, nodes, clients);
            replicaThreads[i] = new MpThread(node, "n" + i);
            nodes[i] = Tid.of(replicaThreads[i].getTaskId(), "n" + i);
        }
        for (int i = 0; i < K; i++) {
            final int ci = i;
            clientThreads[i] = new MpThread(() -> client(ci, q, nodes[ci]), "c" + i);
            clients[i] = Tid.of(clientThreads[i].getTaskId(), "c" + i);
        }
        for (MpThread t : replicaThreads)
            t.start();
        for (MpThread t : clientThreads)
            t.start();
        try {
            for (MpThread t : clientThreads)
                t.join1();
            for (MpThread t : replicaThreads)
                t.join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException("interrupted while awaiting a process", e);
        }
    }

    @Override
    public String toString() {
        return "CassandraPaxosV1(q=" + q + ", " + variant + ", " + delivery + ")";
    }
}
