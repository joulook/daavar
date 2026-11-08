package bench.cassandra_rr;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Reference transliteration of the quorum write path and the quorum read path with read repair of
 * Apache Cassandra 4.1 (src/java/org/apache/cassandra: service/StorageProxy.java,
 * service/AbstractWriteResponseHandler.java, service/WriteResponseHandler.java,
 * service/reads/*, service/reads/repair/*, db/MutationVerbHandler.java,
 * db/ReadCommandVerbHandler.java, db/ReadRepairVerbHandler.java) to JMC's message-passing API.
 *
 * <p>Three replicas n0, n1, n2 (each a coordinator for its client and a replica for everyone),
 * two clients c0, c1; one partition with two regular cells a and b. Every line that departs from
 * the source is marked {@code // PORT:} with the reason; NOTES.md lists the decisions.
 */
public final class CassandraReadRepair implements MpProgram {

    // ------------------------------------------------------------------------------------------
    // Visible messages (INTERFACE.md, fixed shapes)
    // ------------------------------------------------------------------------------------------

    @Tag("Write") public record Write(int client, long value) implements MpMessage {}
    @Tag("ReadA") public record ReadA(int client) implements MpMessage {}
    @Tag("ReadAB") public record ReadAB(int client) implements MpMessage {}
    @Tag("Ack") public record Ack(int client) implements MpMessage {}
    @Tag("ValueA") public record ValueA(int client, long a) implements MpMessage {}
    @Tag("ValueAB") public record ValueAB(int client, long a, long b) implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Data (bounded replacements for db/rows, db/partitions, db/Mutation, db/ReadCommand, db/ReadResponse)
    // ------------------------------------------------------------------------------------------

    /** db/rows/Cell: one regular, live, simple cell. PORT: column name, value and timestamp only (no TTL, no tombstone, no path). */
    @Tag("Cell") public record Cell(String column, long value, long timestamp) implements MpMessage {
        /** Cell.withSkippedValue. PORT: the empty value is 0. */
        Cell withSkippedValue() {
            return new Cell(column, 0, timestamp);
        }
    }

    /** db/partitions/PartitionUpdate: the one row of the one partition. PORT: a list of cells sorted by column; an empty list is no row. */
    @Tag("PartitionUpdate") public record PartitionUpdate(List<Cell> cells) implements MpMessage {}

    /** db/Mutation: PORT: one partition update of the one table. */
    @Tag("Mutation") public record Mutation(PartitionUpdate update) implements MpMessage {}

    /**
     * db/SinglePartitionReadCommand: PORT: the queried columns (ColumnFilter.queriedColumns) and the
     * digest flag only; the fetched columns are always every regular column.
     */
    @Tag("ReadCommand") public record ReadCommand(List<String> queriedColumns, boolean isDigestQuery) implements MpMessage {
        /** ColumnFilter.fetchedColumns of allRegularColumnsBuilder: every regular column. */
        List<String> fetchedColumns() {
            return List.of(COLUMNS);
        }

        ReadCommand copyAsDigestQuery() {
            return new ReadCommand(queriedColumns, true);
        }
    }

    /**
     * db/ReadResponse: a data response (the cells read) or a digest response.
     * PORT: the digest is a String (any deterministic, injective function of the cells returned, INTERFACE.md).
     */
    @Tag("ReadResponse") public record ReadResponse(boolean isDigestResponse, List<Cell> data, String digestValue) implements MpMessage {
        static ReadResponse createDataResponse(List<Cell> data) {
            return new ReadResponse(false, List.copyOf(data), "");
        }

        static ReadResponse createDigestResponse(List<Cell> data) {
            return new ReadResponse(true, List.of(), makeDigest(data));
        }

        /** ReadResponse.digest(command): for a data response the digest is computed from the data. */
        String digest(ReadCommand command) {
            return isDigestResponse ? digestValue : makeDigest(data);
        }

        // PORT: ReadResponse.makeDigest (an MD5 over the serialized partition) replaced by the text
        // of the cells, which is injective over (column, value, timestamp).
        static String makeDigest(List<Cell> data) {
            StringBuilder sb = new StringBuilder();
            for (Cell c : data) {
                sb.append(c.column()).append(':').append(c.value()).append('@').append(c.timestamp()).append(';');
            }
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Internal messages (net/Verb.java: MUTATION_REQ/RSP, READ_REQ/RSP, READ_REPAIR_REQ/RSP).
    // PORT: net/Message's header reduced to the id (the callback correlation of
    // net/RequestCallbacks) and the sender's endpoint (the replica's address, i.e. the Tid of its
    // coordinator thread, where responses go). PORT (fix round 2): every internal message also
    // carries {@code known}: per replica, the highest memtable version in the sender's causal past
    // (see MutationStage and ReadStageTask: it is what a read must at least observe).
    // ------------------------------------------------------------------------------------------

    @Tag("MutationReq") public record MutationReq(int id, Tid from, Mutation payload, List<Integer> known) implements MpMessage {}
    @Tag("MutationRsp") public record MutationRsp(int id, Tid from, List<Integer> known) implements MpMessage {}
    @Tag("ReadReq") public record ReadReq(int id, Tid from, ReadCommand payload, List<Integer> known) implements MpMessage {}
    @Tag("ReadRsp") public record ReadRsp(int id, Tid from, ReadResponse payload, List<Integer> known) implements MpMessage {}
    @Tag("ReadRepairReq") public record ReadRepairReq(int id, Tid from, Mutation payload, List<Integer> known) implements MpMessage {}
    @Tag("ReadRepairRsp") public record ReadRepairRsp(int id, Tid from, List<Integer> known) implements MpMessage {}

    /**
     * PORT (fix round 2): the publication of memtable state {@code index} of replica
     * {@code replica} (the memtable after its index-th write), sent by the replica's MUTATION
     * stage after each write to every thread of that replica that may read the memtable (its READ
     * stage tasks and its coordinator thread, for inline local reads). Receiving it models the
     * volatile read of the memtable's partition reference (AtomicBTreePartition.ref): a reader is
     * causally after the write it observes, and nothing else. A READ stage task is single-use and
     * gets one copy of each state ({@code slot} 0); the coordinator thread may read inline several
     * times and gets one copy per inline-read slot (a message is read once).
     */
    @Tag("Version") public record Version(int replica, int index, int slot, List<Cell> memtable, List<Integer> known) implements MpMessage {}

    /**
     * The model of every message (see NOTES.md F1/G2): delivery to a stage thread pool, whose
     * tasks run in any order; the client channels carry at most one message per direction.
     */
    static final CommunicationModel STAGES = CommunicationModel.ASYN;

    /**
     * Whether Stage.maybeExecuteImmediately may take its inline branch (a free work permit); the
     * queued branch is always possible. PORT (fix round 2): true: both branches are explored (a
     * Nondet choice), since they differ in causal order (inline work is on the coordinator thread).
     */
    static final boolean INLINE_LOCAL_EXECUTION = true;

    static List<Integer> known(int[] k) {
        return List.of(k[0], k[1], k[2]);
    }

    static void merge(int[] k, List<Integer> other) {
        for (int i = 0; i < k.length; i++)
            k[i] = Math.max(k[i], other.get(i));
    }

    /**
     * PORT (fix round 2): the threads of one replica, in spawn order: its coordinator thread
     * (native-transport request thread, role {@code n<r>}, the replica's address), its MUTATION
     * stage ({@code n<r>-mutation}), and its READ stage tasks ({@code n<r>-read-c<c>-<s>}), one
     * single-use thread per read request coordinator {@code c} can send it (two per read of client
     * {@code c}: the initial request and the full data read of a repair). Then the clients.
     */
    static final class Layout {
        final int q;
        final int perReplica;

        Layout(int q) {
            this.q = q;
            int p = 2;
            for (int c = 0; c < CLIENTS; c++) p += slots(c);
            this.perReplica = p;
        }

        int reads(int c) {
            int n = 0;
            for (int j = 0; j < q; j++) if ((c + j) % 3 != 0) n++;
            return n;
        }

        int slots(int c) { return 2 * reads(c); }
        int coordinator(int r) { return r * perReplica; }
        int mutation(int r) { return r * perReplica + 1; }

        int worker(int r, int c, int s) {
            if (s >= slots(c)) throw new IllegalStateException("no read slot " + s + " for c" + c + " at n" + r);
            int off = 2;
            for (int k = 0; k < c; k++) off += slots(k);
            return r * perReplica + off + s;
        }

        int client(int i) { return REPLICAS * perReplica + i; }
        int total() { return REPLICAS * perReplica + CLIENTS; }

        String role(int idx) {
            if (idx >= REPLICAS * perReplica) return "c" + (idx - REPLICAS * perReplica);
            int r = idx / perReplica, o = idx % perReplica;
            if (o == 0) return "n" + r;
            if (o == 1) return "n" + r + "-mutation";
            int off = 2;
            for (int c = 0; c < CLIENTS; c++) {
                if (o < off + slots(c)) return "n" + r + "-read-c" + c + "-" + (o - off);
                off += slots(c);
            }
            throw new IllegalStateException();
        }

        Tid tid(long base, int idx) { return Tid.of(base + idx, role(idx)); }

        List<Tid> workers(long base, int r) {
            List<Tid> l = new ArrayList<>();
            for (int c = 0; c < CLIENTS; c++)
                for (int s = 0; s < slots(c); s++) l.add(tid(base, worker(r, c, s)));
            return l;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Program parameters and entry
    // ------------------------------------------------------------------------------------------

    static final int REPLICAS = 3;
    static final int CLIENTS = 2;
    static final String[] COLUMNS = {"a", "b"};

    private final int q;
    private final ReadRepairStrategy strategy;
    private final CommunicationModel model;
    private final String modelName;
    private final Layout layout;
    /**
     * Test only (not a protocol option): a reachability aid. {@code review1} restricts the
     * schedules towards the REVIEW.md finding-1 history (ReplicaPlans.forRead, Node.inline,
     * ReadStageTask.run). It only removes choices: every execution it allows is an execution of
     * the unrestricted program.
     */
    private final boolean review1;

    /** Launcher entry: {@code impl,<q>,<blocking|none>,<fifo|bag>[,review1]}. */
    public CassandraReadRepair(String... args) {
        if (args.length < 4 || args.length > 5 || !args[0].equals("impl")
                || (args.length == 5 && !args[4].equals("review1"))) {
            throw new IllegalArgumentException("expected impl,<q>,<blocking|none>,<fifo|bag>[,review1]");
        }
        this.review1 = args.length == 5;
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1");
        }
        this.strategy = ReadRepairStrategy.fromString(args[2]);
        this.modelName = args[3];
        this.model = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag");
        };
        this.layout = new Layout(q);
    }

    @Override public String programName() { return "cassandra-rr"; }
    @Override public String size() { return "q=" + q + ",rr=" + strategy.name().toLowerCase() + "," + modelName; }
    @Override public CommunicationModel model() { return model; }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int k = 0; k < layout.total(); k++) roles.add(layout.role(k));
        return roles;
    }

    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }

    /**
     * Observation only (printed by the launcher through toString): the distinct reply sequences
     * of each client and the distinct pairs of them. These are reply strings only: they carry no
     * causal order (the exported summaries do; NOTES.md G6). Not part of the protocol; it touches
     * no message-passing state.
     */
    private final Map<String, Boolean> observedReplies = new ConcurrentHashMap<>();
    private final Map<String, Boolean> observedReplyPairs = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong finishedClients = new java.util.concurrent.atomic.AtomicLong();

    /**
     * The reply strings of the fix-round-1 reference check (translit-review/REVIEW.md, finding 1;
     * q=2, none): c0 Write(0,1) -> Ack, ReadA -> 1; c1 ReadA -> 1, then ReadAB -> (0,0).
     */
    static final String REVIEW_HISTORY = "c0=[Ack[client=0] ValueA[client=0, a=1]] c1=[ValueA[client=1, a=1] ValueAB[client=1, a=0, b=0]]";

    @Override
    public String toString() {
        return "finished client runs " + finishedClients.get() + "; distinct replies per client "
                + new TreeSet<>(observedReplies.keySet()) + "; distinct reply-string pairs " + observedReplyPairs.size()
                + (q == 2 ? "; review reply strings (REVIEW.md finding 1) " + (observedReplyPairs.containsKey(REVIEW_HISTORY) ? "PRESENT" : "ABSENT") : "");
    }

    static long value(int i, int j) {
        return 1 + i + (long) CLIENTS * j;
    }

    /**
     * Main: starts the replicas' threads in replica index order (each replica's coordinator
     * thread, MUTATION stage and READ stage tasks), then the clients in index order, and waits for
     * them; it sends and receives nothing.
     */
    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        String[] replies = new String[CLIENTS]; // observation only, read by main after the clients' await
        for (int k = 0; k < layout.total(); k++) {
            final int idx = k;
            String role = layout.role(k);
            Runnable body;
            if (idx >= layout.client(0)) {
                int i = idx - layout.client(0);
                body = () -> {
                    replies[i] = client(i, layout, model);
                    observedReplies.put("c" + i + ":" + replies[i], Boolean.TRUE);
                    finishedClients.incrementAndGet();
                };
            } else {
                int r = idx / layout.perReplica, o = idx % layout.perReplica;
                if (o == 0) body = () -> new Node(r, layout, strategy, review1).run();
                else if (o == 1) body = () -> new MutationStage(r, layout).run();
                else body = () -> new ReadStageTask(r, idx, layout, review1).run();
            }
            handles.add(MpProcess.spawn(role, body));
        }
        // Every process derives the others' addresses from its own task id: the processes are
        // created consecutively (checked here, before any wait).
        long base = handles.get(0).tid().raw();
        for (int k = 0; k < handles.size(); k++) {
            if (handles.get(k).tid().raw() != base + k) {
                throw new IllegalStateException("task ids are not consecutive: " + handles);
            }
        }
        for (int i = 0; i < CLIENTS; i++) {
            handles.get(layout.client(i)).await();
        }
        observedReplyPairs.put("c0=[" + replies[0] + "] c1=[" + replies[1] + "]", Boolean.TRUE);
        for (int k = 0; k < layout.client(0); k++) {
            handles.get(k).await();
        }
    }

    /** The client loop of INTERFACE.md, verbatim in meaning. */
    static String client(int i, Layout layout, CommunicationModel model) {
        long base = MpProcess.self().raw() - layout.client(i);
        Tid node = layout.tid(base, layout.coordinator(i));
        StringBuilder replies = new StringBuilder();
        for (int j = 0; j < layout.q; j++) {
            MpMessage rep;
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(STAGES, node, new Write(i, value(i, j)));
                    if (!((rep = Channel.recv(STAGES)) instanceof Ack)) throw new AssertionError("Ack expected");
                }
                case 1 -> {
                    Channel.send(STAGES, node, new ReadA(i));
                    if (!((rep = Channel.recv(STAGES)) instanceof ValueA)) throw new AssertionError("ValueA expected");
                }
                default -> {
                    Channel.send(STAGES, node, new ReadAB(i));
                    if (!((rep = Channel.recv(STAGES)) instanceof ValueAB)) throw new AssertionError("ValueAB expected");
                }
            }
            replies.append(rep).append(' ');
        }
        return replies.toString().trim();
    }

    // ------------------------------------------------------------------------------------------
    // db/rows/Cells.java
    // ------------------------------------------------------------------------------------------

    static final class Cells {
        /** Cells.reconcile (l.68): the cell with the newer timestamp wins. */
        static Cell reconcile(Cell c1, Cell c2) {
            if (c1 == null || c2 == null)
                return c2 == null ? c1 : c2;
            // PORT: no counter cells (Cells.resolveCounter is never taken).
            return resolveRegular(c1, c2);
        }

        static Cell resolveRegular(Cell left, Cell right) {
            long leftTimestamp = left.timestamp();
            long rightTimestamp = right.timestamp();
            if (leftTimestamp != rightTimestamp)
                return leftTimestamp > rightTimestamp ? left : right;
            // PORT: no tombstones or expiring cells (the localDeletionTime/TTL branches are never taken).
            return compareValues(left, right) >= 0 ? left : right;
        }

        static int compareValues(Cell left, Cell right) {
            return Long.compare(left.value(), right.value());
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/repair/ReadRepairStrategy.java
    // ------------------------------------------------------------------------------------------

    enum ReadRepairStrategy {
        NONE {
            ReadRepair create(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
                return new ReadOnlyReadRepair(node, command, replicaPlan);
            }
        },
        BLOCKING {
            ReadRepair create(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
                return new BlockingReadRepair(node, command, replicaPlan);
            }
        };

        abstract ReadRepair create(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan);

        static ReadRepairStrategy fromString(String s) {
            return valueOf(s.toUpperCase());
        }
    }

    // ------------------------------------------------------------------------------------------
    // locator/Replica.java, locator/ReplicaPlan.java, locator/ReplicaPlans.java
    // ------------------------------------------------------------------------------------------

    /** locator/Replica: PORT: a full replica of the one token range, identified by index and address. */
    record Replica(int index, Tid endpoint, boolean isSelf) {}

    static final class ReplicaPlan {
        /** ReplicaPlan.ForTokenRead: candidates sorted by the snitch, contacts the first blockFor of them. */
        record ForTokenRead(List<Replica> readCandidates, List<Replica> contacts) {
            int readQuorum() { return ConsistencyLevel.QUORUM_BLOCK_FOR; }

            Replica lookup(Tid endpoint) {
                for (Replica r : readCandidates)
                    if (r.endpoint().equals(endpoint)) return r;
                return null;
            }

            ForTokenRead withContacts(List<Replica> newContact) {
                return new ForTokenRead(readCandidates, newContact);
            }
        }

        /** ReplicaPlan.ForWrite. PORT: no pending replicas, every replica alive (live == liveAndDown). */
        record ForWrite(List<Replica> live, List<Replica> contacts) {
            int writeQuorum() { return ConsistencyLevel.QUORUM_BLOCK_FOR; }

            List<Replica> liveUncontacted() {
                List<Replica> r = new ArrayList<>();
                for (Replica x : live)
                    if (!contacts.contains(x)) r.add(x);
                return r;
            }

            Replica lookup(Tid endpoint) {
                for (Replica r : live)
                    if (r.endpoint().equals(endpoint)) return r;
                return null;
            }
        }
    }

    /** db/ConsistencyLevel.java: QUORUM over RF = 3 is 3 / 2 + 1 = 2. */
    static final class ConsistencyLevel {
        static final int QUORUM_BLOCK_FOR = REPLICAS / 2 + 1;
    }

    // ------------------------------------------------------------------------------------------
    // net/RequestCallback
    // ------------------------------------------------------------------------------------------

    interface RequestCallback {
        void onResponse(Tid from, Object payload);
    }

    // ------------------------------------------------------------------------------------------
    // One replica process: storage, verb handlers, messaging, and the coordinator (StorageProxy)
    // ------------------------------------------------------------------------------------------

    /**
     * A replica's coordinator thread (role {@code n<r>}): the native-transport request thread that
     * runs StorageProxy for its client, and the thread its response callbacks signal.
     *
     * <p>PORT (fix round 2, REVIEW-r2.md finding 1): a replica is several processes, one per
     * Cassandra thread whose work it runs, so that program order exists only where the source has
     * a dependency:
     * <ul>
     *   <li>this coordinator thread: the client request, StorageProxy and the read executor, and
     *       the callbacks of its requests. The REQUEST_RESPONSE stage task of a response
     *       (ResponseVerbHandler -> callback.onResponse) is run here, at the receipt of the
     *       response: its only effect is on the coordinator's handler (the Accumulator, the
     *       counter, the latch), which the waiting coordinator thread then reads, so the task is
     *       ordered before the coordinator's continuation in the source as well (NOTES.md G3);</li>
     *   <li>{@link MutationStage}: the MUTATION stage (MUTATION_REQ, READ_REPAIR_REQ and queued
     *       local mutations) and the memtable;</li>
     *   <li>{@link ReadStageTask}: one READ stage task per read request (READ_REQ and queued
     *       local reads).</li>
     * </ul>
     */
    static final class Node {
        final int index;
        final Layout layout;
        final ReadRepairStrategy readRepairStrategy;
        Tid self;
        long base;
        final List<Replica> naturalReplicas = new ArrayList<>();
        /** net/RequestCallbacks: id -> callback. PORT: entries never expire (no timeouts). */
        final Map<Integer, RequestCallback> callbacks = new HashMap<>();
        int nextId = 0;
        /** The causal knowledge of memtable versions (see the internal messages). */
        final int[] known = new int[REPLICAS];
        /** The next READ stage task slot of each target replica for this coordinator. */
        final int[] slotUsed = new int[REPLICAS];
        /** The next inline-read slot (a copy of each published state per slot). */
        int inlineReads = 0;

        final boolean review1;
        int readsPlanned = 0; // review1 bookkeeping only

        Node(int index, Layout layout, ReadRepairStrategy readRepairStrategy, boolean review1) {
            this.index = index;
            this.layout = layout;
            this.readRepairStrategy = readRepairStrategy;
            this.review1 = review1;
        }

        void run() {
            self = MpProcess.self();
            base = self.raw() - layout.coordinator(index);
            for (int k = 0; k < REPLICAS; k++)
                naturalReplicas.add(new Replica(k, layout.tid(base, layout.coordinator(k)), k == index));
            // PORT: the process never terminates: it keeps receiving client requests (the harness
            // ends an execution blocked on receive).
            while (true) {
                MpMessage m = Channel.recv(STAGES, msg -> msg instanceof Write || msg instanceof ReadA || msg instanceof ReadAB);
                coordinate(m);
            }
        }

        /**
         * The native-transport request: PORT: the CQL layer (UpdateStatement/SelectStatement.execute
         * -> StorageProxy.mutateWithTriggers / StorageProxy.read) reduced to the calls of mutate and
         * readRegular. A SELECT a or SELECT a, b fetches every regular column
         * (ColumnFilter.allRegularColumnsBuilder) and queries the selected ones.
         */
        void coordinate(MpMessage m) {
            if (m instanceof Write w) {
                StorageProxy.mutate(this, new Mutation(new PartitionUpdate(List.of(
                        new Cell("a", w.value(), w.value()), new Cell("b", w.value(), w.value())))));
                Channel.send(STAGES, layout.tid(base, layout.client(w.client())), new Ack(w.client()));
            } else if (m instanceof ReadA r) {
                Map<String, Cell> row = StorageProxy.readRegular(this, new ReadCommand(List.of("a"), false));
                Channel.send(STAGES, layout.tid(base, layout.client(r.client())), new ValueA(r.client(), valueOf(row, "a")));
            } else if (m instanceof ReadAB r) {
                Map<String, Cell> row = StorageProxy.readRegular(this, new ReadCommand(List.of("a", "b"), false));
                Channel.send(STAGES, layout.tid(base, layout.client(r.client())),
                        new ValueAB(r.client(), valueOf(row, "a"), valueOf(row, "b")));
            }
        }

        static long valueOf(Map<String, Cell> row, String column) {
            Cell c = row.get(column);
            return c == null ? 0 : c.value(); // an absent cell counts as 0 (INTERFACE.md)
        }

        /**
         * A wait of the coordinator thread on a Condition or latch. PORT: the responses its
         * callbacks are waiting for are received here, and their REQUEST_RESPONSE task
         * (ResponseVerbHandler.doVerb) runs at receipt (see the class comment).
         *
         * <p>PORT: a response whose callback can no longer change anything (the third
         * MUTATION_RSP of a write, after its handler signalled) is not received: in the source its
         * task only decrements a counter that nothing reads any more, so the task has no causal
         * successor (NOTES.md G4). The guard reads only the captured set.
         */
        void awaitUntil(java.util.function.BooleanSupplier condition) {
            while (!condition.getAsBoolean()) {
                java.util.Set<Integer> live = liveCallbacks();
                MpMessage m = Channel.recv(STAGES, msg -> responseId(msg) >= 0 && live.contains(responseId(msg)));
                if (m instanceof MutationRsp r) {
                    merge(known, r.known());
                    ResponseVerbHandler.doVerb(this, r.id(), r.from(), null);
                } else if (m instanceof ReadRsp r) {
                    merge(known, r.known());
                    ResponseVerbHandler.doVerb(this, r.id(), r.from(), r.payload());
                } else if (m instanceof ReadRepairRsp r) {
                    merge(known, r.known());
                    ResponseVerbHandler.doVerb(this, r.id(), r.from(), null);
                }
            }
        }

        static int responseId(MpMessage msg) {
            if (msg instanceof MutationRsp r) return r.id();
            if (msg instanceof ReadRsp r) return r.id();
            if (msg instanceof ReadRepairRsp r) return r.id();
            return -1;
        }

        java.util.Set<Integer> liveCallbacks() {
            java.util.Set<Integer> live = new java.util.HashSet<>();
            for (Map.Entry<Integer, RequestCallback> e : callbacks.entrySet())
                if (!(e.getValue() instanceof WriteResponseHandler w && w.condition))
                    live.add(e.getKey());
            return java.util.Set.copyOf(live);
        }

        /**
         * Stage.maybeExecuteImmediately -> SEPExecutor.maybeExecuteImmediately (F/concurrent/SEPExecutor.java
         * l.218): runs the task on the calling thread if a work permit is free, else queues it on the
         * stage. PORT: whether a permit is free is a Nondet choice.
         */
        boolean inline() {
            if (review1) return false; // review1: queued only
            return INLINE_LOCAL_EXECUTION && Nondet.chooseBoolean();
        }

        // ---- net/MessagingService --------------------------------------------------------------

        int register(RequestCallback callback) {
            int id = nextId++;
            callbacks.put(id, callback);
            return id;
        }

        /** MessagingService.sendWriteWithCallback(MUTATION_REQ): to the destination's MUTATION stage. */
        void sendMutation(Replica destination, RequestCallback callback, Mutation mutation) {
            int id = register(callback);
            Channel.send(STAGES, layout.tid(base, layout.mutation(destination.index())),
                    new MutationReq(id, self, mutation, known(known)));
        }

        /** MessagingService.sendWithCallback(READ_REQ): to a READ stage task of the destination. */
        void sendRead(Replica destination, RequestCallback callback, ReadCommand command) {
            int id = register(callback);
            int slot = slotUsed[destination.index()]++;
            Channel.send(STAGES, layout.tid(base, layout.worker(destination.index(), index, slot)),
                    new ReadReq(id, self, command, known(known)));
        }

        /** MessagingService.sendWithCallback(READ_REPAIR_REQ): to the destination's MUTATION stage. */
        void sendReadRepair(Replica destination, RequestCallback callback, Mutation mutation) {
            int id = register(callback);
            Channel.send(STAGES, layout.tid(base, layout.mutation(destination.index())),
                    new ReadRepairReq(id, self, mutation, known(known)));
        }

        // ---- local execution (StorageProxy.performLocally, LocalReadRunnable) -------------------

        /**
         * performLocally(Stage.MUTATION, localReplica, mutation::apply, handler) (StorageProxy l.1646).
         * Queued: a LocalMutationRunnable on the MUTATION stage, i.e. a mutation request to this
         * replica's MutationStage, which applies and answers the handler. Inline: the same apply
         * runs on this coordinator thread before it goes on. PORT: the memtable is owned by the
         * MutationStage process, so the inline apply is a synchronous call to it: the request,
         * then the wait for exactly its acknowledgement (handler.onResponse runs at that receipt).
         */
        void performLocally(Mutation mutation, RequestCallback handler) {
            if (!inline()) {
                sendMutation(naturalReplicas.get(index), handler, mutation);
                return;
            }
            boolean[] done = {false};
            sendMutation(naturalReplicas.get(index), (from, payload) -> {
                done[0] = true;
                handler.onResponse(from, payload);
            }, mutation);
            awaitUntil(() -> done[0]);
        }

        /**
         * Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(command, handler)). Queued: a
         * READ stage task of this replica. Inline: the read runs on this coordinator thread: it
         * observes a published memtable state not older than its causal knowledge (the guard), then
         * handler.response.
         */
        void localRead(ReadCommand command, ReadCallback handler) {
            if (!inline()) {
                sendRead(naturalReplicas.get(index), handler, command);
                return;
            }
            final int r = index, need = known[index], slot = inlineReads++;
            Version v = (Version) Channel.recv(STAGES, msg -> msg instanceof Version x && x.replica() == r
                    && x.slot() == slot && x.index() >= need);
            merge(known, v.known());
            handler.response(executeLocally(toRow(v.memtable()), command));
        }
    }

    /**
     * ReadCommand.executeLocally + createResponse over a memtable state. Every fetched column (all
     * regular columns, ColumnFilter.allRegularColumnsBuilder, F/cql3/selection/ColumnFilterFactory.java
     * l.46-57) is returned; a fetched but not queried cell keeps its timestamp and has its value
     * skipped (BTreeRow.filter, F/db/rows/BTreeRow.java l.344-371: an UPDATE writes no row
     * liveness, so the cell is never dropped). The digest covers what is returned.
     * PORT: a skipped value is represented as 0 (Cassandra: an empty value).
     */
    static ReadResponse executeLocally(Map<String, Cell> memtable, ReadCommand command) {
        List<Cell> data = new ArrayList<>();
        for (String column : command.fetchedColumns()) {
            Cell c = memtable.get(column);
            if (c == null) continue;
            boolean isSkippable = !command.queriedColumns().contains(column);
            data.add(isSkippable ? c.withSkippedValue() : c);
        }
        return command.isDigestQuery()
               ? ReadResponse.createDigestResponse(data)
               : ReadResponse.createDataResponse(data);
    }

    /**
     * A replica's MUTATION stage (role {@code n<r>-mutation}) and its memtable. PORT (fix round
     * 2): the stage is multi-threaded in the source, but every write to the partition goes through
     * the memtable's compare-and-swap (AtomicBTreePartition: a volatile read of the current tree,
     * then a CAS), so each write happens after the write it replaced; one process for the stage
     * keeps exactly that write-to-write order and the order of each task's apply and response
     * (NOTES.md G3). Messages are received in any order (ASYN), as the pool's tasks may run.
     * After each write it publishes the new state ({@link Version}) to the replica's readers.
     */
    static final class MutationStage {
        final int index;
        final Layout layout;
        Tid self; // the replica's address (its coordinator thread): the 'from' of responses
        long base;
        final TreeMap<String, Cell> memtable = new TreeMap<>();
        int version = 0;
        final int[] known = new int[REPLICAS];

        MutationStage(int index, Layout layout) {
            this.index = index;
            this.layout = layout;
        }

        void run() {
            base = MpProcess.self().raw() - layout.mutation(index);
            self = layout.tid(base, layout.coordinator(index));
            publish();
            while (true) {
                MpMessage m = Channel.recv(STAGES, msg -> msg instanceof MutationReq || msg instanceof ReadRepairReq);
                if (m instanceof MutationReq r) {
                    merge(known, r.known());
                    MutationVerbHandler.doVerb(this, r);
                } else if (m instanceof ReadRepairReq r) {
                    merge(known, r.known());
                    ReadRepairVerbHandler.doVerb(this, r);
                }
            }
        }

        /** Mutation.apply -> Keyspace.apply -> memtable put: cells are merged with Cells.reconcile. */
        void apply(Mutation mutation) {
            for (Cell c : mutation.update().cells())
                memtable.put(c.column(), Cells.reconcile(memtable.get(c.column()), c));
            version++;
            known[index] = version;
            publish();
        }

        void publish() {
            List<Cell> state = List.copyOf(memtable.values());
            for (int s = 0; s < layout.slots(index); s++) // the coordinator thread's inline-read slots
                Channel.send(STAGES, layout.tid(base, layout.coordinator(index)), new Version(index, version, s, state, known(known)));
            for (Tid reader : layout.workers(base, index))
                Channel.send(STAGES, reader, new Version(index, version, 0, state, known(known)));
        }
    }

    /**
     * One READ stage task of a replica (role {@code n<r>-read-c<c>-<s>}): it serves exactly one
     * read request, the s-th that coordinator c sends this replica. PORT (fix round 2): a task of
     * its own, so two reads at one replica, or a read and a write, are not ordered unless the
     * source orders them: the read observes a published memtable state (a {@link Version}) that
     * is not older than the request's causal knowledge, and is causally after that state's write
     * and nothing else (NOTES.md G1).
     */
    static final class ReadStageTask {
        final int index;
        final int idx;
        final Layout layout;
        final boolean review1;
        Tid self;
        final int[] known = new int[REPLICAS];

        ReadStageTask(int index, int idx, Layout layout, boolean review1) {
            this.index = index;
            this.idx = idx;
            this.layout = layout;
            this.review1 = review1;
        }

        void run() {
            long base = MpProcess.self().raw() - idx;
            self = layout.tid(base, layout.coordinator(index));
            ReadReq message = (ReadReq) Channel.recv(STAGES, msg -> msg instanceof ReadReq);
            merge(known, message.known());
            final int r = index, need = known[index];
            // review1 (test only): c1's reads at n1 and n2 observe the oldest admissible state, at n0
            // a state that contains a write.
            final boolean servesC1 = (idx % layout.perReplica) - 2 >= layout.slots(0);
            final int mode = !review1 || !servesC1 ? 0 : (r == 0 ? 2 : 1);
            Version v = (Version) Channel.recv(STAGES, msg -> msg instanceof Version x && x.replica() == r
                    && (mode == 1 ? x.index() == need : x.index() >= (mode == 2 ? Math.max(need, 1) : need)));
            merge(known, v.known());
            ReadCommandVerbHandler.doVerb(this, message, toRow(v.memtable()));
        }
    }

    // ------------------------------------------------------------------------------------------
    // Replica side: db/MutationVerbHandler.java, db/ReadCommandVerbHandler.java,
    // db/ReadRepairVerbHandler.java, net/ResponseVerbHandler.java
    // ------------------------------------------------------------------------------------------

    static final class MutationVerbHandler {
        static void doVerb(MutationStage stage, MutationReq message) {
            // PORT: no expiry check (no timeouts), no size validation, no forwarding (one DC).
            Tid respondToAddress = message.from();
            applyMutation(stage, message, respondToAddress);
        }

        static void applyMutation(MutationStage stage, MutationReq message, Tid respondToAddress) {
            // PORT: applyFuture().addCallback(respond) collapsed: apply, then respond.
            stage.apply(message.payload());
            respond(stage, message, respondToAddress);
        }

        static void respond(MutationStage stage, MutationReq respondTo, Tid respondToAddress) {
            Channel.send(STAGES, respondToAddress, new MutationRsp(respondTo.id(), stage.self, known(stage.known)));
        }
    }

    static final class ReadCommandVerbHandler {
        static void doVerb(ReadStageTask task, ReadReq message, Map<String, Cell> memtable) {
            ReadCommand command = message.payload();
            // PORT: no bootstrap, out-of-range, transient, monitoring or warning checks.
            ReadResponse response = executeLocally(memtable, command);
            Channel.send(STAGES, message.from(), new ReadRsp(message.id(), task.self, response, known(task.known)));
        }
    }

    static final class ReadRepairVerbHandler {
        static void doVerb(MutationStage stage, ReadRepairReq message) {
            // AbstractMutationVerbHandler.doVerb -> applyMutation (PORT: no token-ownership checks).
            applyMutation(stage, message, message.from());
        }

        static void applyMutation(MutationStage stage, ReadRepairReq message, Tid respondToAddress) {
            stage.apply(message.payload());
            Channel.send(STAGES, respondToAddress, new ReadRepairRsp(message.id(), stage.self, known(stage.known)));
        }
    }

    static final class ResponseVerbHandler {
        static void doVerb(Node node, int id, Tid from, Object payload) {
            RequestCallback callback = node.callbacks.get(id);
            if (callback == null)
                return; // PORT: an expired callback; cannot happen (callbacks never expire here).
            callback.onResponse(from, payload);
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/StorageProxy.java: the write path and the read path of the coordinator
    // ------------------------------------------------------------------------------------------

    static final class StorageProxy {

        /** StorageProxy.mutate (l.873). PORT: one non-counter mutation; no exceptions (no timeouts, no failures). */
        static void mutate(Node node, Mutation mutation) {
            List<WriteResponseHandler> responseHandlers = new ArrayList<>(1);
            responseHandlers.add(performWrite(node, mutation));

            // upgrade to full quorum any failed cheap quorums
            for (WriteResponseHandler responseHandler : responseHandlers)
                responseHandler.maybeTryAdditionalReplicas();

            // wait for writes.
            for (WriteResponseHandler responseHandler : responseHandlers)
                responseHandler.get();
        }

        /** StorageProxy.performWrite (l.1360). */
        static WriteResponseHandler performWrite(Node node, Mutation mutation) {
            ReplicaPlan.ForWrite replicaPlan = ReplicaPlans.forWrite(node);
            // PORT: metrics dropped. rs.getWriteResponseHandler at QUORUM is a WriteResponseHandler.
            WriteResponseHandler responseHandler = new WriteResponseHandler(node, replicaPlan);
            // standardWritePerformer -> sendToHintedReplicas
            sendToHintedReplicas(node, mutation, replicaPlan, responseHandler);
            return responseHandler;
        }

        /** StorageProxy.sendToHintedReplicas (l.1464). */
        static void sendToHintedReplicas(Node node, Mutation mutation, ReplicaPlan.ForWrite plan, WriteResponseHandler responseHandler) {
            List<Replica> localDc = null;
            boolean insertLocal = false;
            Replica localReplica = null;

            for (Replica destination : plan.contacts()) {
                // PORT: every replica is alive (no hints, no expiry); one data centre (no dcGroups).
                if (destination.isSelf()) {
                    insertLocal = true;
                    localReplica = destination;
                } else {
                    if (localDc == null)
                        localDc = new ArrayList<>(plan.contacts().size());
                    localDc.add(destination);
                }
            }

            if (insertLocal) {
                // performLocally(stage, localReplica, mutation::apply, responseHandler, ...) (l.1646)
                node.performLocally(mutation, responseHandler);
            }

            if (localDc != null) {
                for (Replica destination : localDc)
                    node.sendMutation(destination, responseHandler, mutation);
            }
        }

        /** StorageProxy.readRegular (l.1967). PORT: one command per group; no metrics; no exceptions. */
        static Map<String, Cell> readRegular(Node node, ReadCommand command) {
            return fetchRows(node, List.of(command));
        }

        /**
         * StorageProxy.fetchRows (l.2077). PORT: in 4.1 the phases are a loop in fetchRows (there is
         * no SinglePartitionReadLifecycle in this tree); kept in the source's order.
         */
        static Map<String, Cell> fetchRows(Node node, List<ReadCommand> commands) {
            int cmdCount = commands.size();

            AbstractReadExecutor[] reads = new AbstractReadExecutor[cmdCount];

            for (int i = 0; i < cmdCount; i++)
                reads[i] = AbstractReadExecutor.getReadExecutor(node, commands.get(i));

            for (int i = 0; i < cmdCount; i++)
                reads[i].executeAsync();

            for (int i = 0; i < cmdCount; i++)
                reads[i].maybeTryAdditionalReplicas();

            for (int i = 0; i < cmdCount; i++)
                reads[i].awaitResponses();

            for (int i = 0; i < cmdCount; i++)
                reads[i].maybeSendAdditionalDataRequests();

            for (int i = 0; i < cmdCount; i++)
                reads[i].awaitReadRepair();

            List<Map<String, Cell>> results = new ArrayList<>(cmdCount);
            List<ReadRepair> repairs = new ArrayList<>(cmdCount);
            for (int i = 0; i < cmdCount; i++) {
                results.add(reads[i].getResult());
                repairs.add(reads[i].getReadRepair());
            }

            return concatAndBlockOnRepair(results, repairs);
        }

        /**
         * StorageProxy.concatAndBlockOnRepair (l.2045). PORT: the returned iterator's close()
         * (maybeSendAdditionalWrites, then awaitWrites) is run here, before the reply: the CQL
         * layer consumes and closes the iterator before it answers the client.
         */
        static Map<String, Cell> concatAndBlockOnRepair(List<Map<String, Cell>> iterators, List<ReadRepair> repairs) {
            Map<String, Cell> concatenated = iterators.get(0); // PORT: one partition.
            if (repairs.isEmpty())
                return concatenated;
            repairs.forEach(ReadRepair::maybeSendAdditionalWrites);
            repairs.forEach(ReadRepair::awaitWrites);
            return concatenated;
        }
    }

    // ------------------------------------------------------------------------------------------
    // locator/ReplicaPlans.java
    // ------------------------------------------------------------------------------------------

    static final class ReplicaPlans {
        /**
         * ReplicaPlans.forRead (l.706): candidates = the live natural replicas sorted by the
         * snitch (ReplicaLayout.forTokenReadLiveSorted); contacts = the first blockFor (2) of them.
         * PORT: the snitch's proximity order is a nondeterministic choice (INTERFACE.md: the
         * "fastest replicas" are a nondeterministic choice): the first and the second candidate are
         * chosen among all replicas, the local one included or not, and the third is the rest.
         */
        static ReplicaPlan.ForTokenRead forRead(Node node) {
            List<Replica> remaining = new ArrayList<>(node.naturalReplicas);
            List<Replica> candidates = new ArrayList<>();
            if (node.review1 && node.index == 1) {
                // review1 (test only): c1's first read [n1, n0], its second read [n1, n2]
                int other = node.readsPlanned++ == 0 ? 0 : 2;
                candidates.add(remaining.remove(1));
                candidates.add(remaining.remove(other == 0 ? 0 : 1));
            } else {
                candidates.add(remaining.remove(Nondet.chooseIndex(remaining.size())));
                candidates.add(remaining.remove(Nondet.chooseIndex(remaining.size())));
            }
            candidates.add(remaining.remove(0));
            // contactForRead: blockFor + (alwaysSpeculate ? 1 : 0), alwaysSpeculate = false
            int count = ConsistencyLevel.QUORUM_BLOCK_FOR;
            List<Replica> contacts = List.copyOf(candidates.subList(0, Math.min(count, candidates.size())));
            return new ReplicaPlan.ForTokenRead(List.copyOf(candidates), contacts);
        }

        /** ReplicaPlans.forWrite with writeNormal (l.505): no transient replicas -> liveAndDown.all(). */
        static ReplicaPlan.ForWrite forWrite(Node node) {
            List<Replica> all = List.copyOf(node.naturalReplicas);
            return new ReplicaPlan.ForWrite(all, all);
        }

        /** ReplicaPlans.forReadRepair (l.434) with writeReadRepair (l.551). */
        static ReplicaPlan.ForWrite forReadRepair(Node node, ReplicaPlan.ForTokenRead readPlan) {
            List<Replica> live = List.copyOf(node.naturalReplicas);
            List<Replica> contacts = new ArrayList<>();
            // add all live nodes we might write to that we have already contacted on read
            for (Replica r : live)
                if (containsEndpoint(readPlan.contacts(), r.endpoint())) contacts.add(r);
            // finally, add sufficient nodes to achieve our consistency level
            int add = ConsistencyLevel.QUORUM_BLOCK_FOR - contacts.size();
            if (add > 0) {
                for (Replica replica : live) {
                    if (contacts.contains(replica)) continue;
                    contacts.add(replica);
                    if (--add == 0)
                        break;
                }
            }
            return new ReplicaPlan.ForWrite(live, List.copyOf(contacts));
        }

        static boolean containsEndpoint(List<Replica> replicas, Tid endpoint) {
            for (Replica r : replicas)
                if (r.endpoint().equals(endpoint)) return true;
            return false;
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/AbstractWriteResponseHandler.java + service/WriteResponseHandler.java
    // ------------------------------------------------------------------------------------------

    static final class WriteResponseHandler implements RequestCallback {
        final Node node;
        final ReplicaPlan.ForWrite replicaPlan;
        boolean condition = false; // the one-time Condition
        int responses;

        WriteResponseHandler(Node node, ReplicaPlan.ForWrite replicaPlan) {
            this.node = node;
            this.replicaPlan = replicaPlan;
            this.responses = blockFor();
        }

        int blockFor() {
            return replicaPlan.writeQuorum();
        }

        int ackCount() {
            return blockFor() - responses;
        }

        @Override
        public void onResponse(Tid from, Object payload) {
            if (--responses == 0)
                signal();
            // PORT: logResponseToIdealCLDelegate dropped (no ideal CL).
        }

        void signal() {
            condition = true;
        }

        /** AbstractWriteResponseHandler.get (l.113). PORT: condition.await without a timeout; no failures. */
        void get() {
            node.awaitUntil(() -> condition);
        }

        /** AbstractWriteResponseHandler.maybeTryAdditionalReplicas (l.334). */
        void maybeTryAdditionalReplicas() {
            List<Replica> uncontacted = replicaPlan.liveUncontacted();
            if (uncontacted.isEmpty())
                return;
            // PORT: unreachable (writeNormal contacts every replica); the timed wait is not modelled.
            throw new IllegalStateException("uncontacted replicas on a write");
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/ResponseResolver.java, DigestResolver.java, DataResolver.java
    // ------------------------------------------------------------------------------------------

    /** A received ReadResponse with its sender: net/Message<ReadResponse>. */
    record ResponseMessage(Tid from, ReadResponse payload) {}

    abstract static class ResponseResolver {
        final ReadCommand command;
        ReplicaPlan.ForTokenRead replicaPlan;
        final List<ResponseMessage> responses = new ArrayList<>(); // the Accumulator

        ResponseResolver(ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            this.command = command;
            this.replicaPlan = replicaPlan;
        }

        abstract boolean isDataPresent();

        void preprocess(ResponseMessage message) {
            // PORT: no transient replicas (the transient digest check is never taken).
            responses.add(message);
        }
    }

    static final class DigestResolver extends ResponseResolver {
        ResponseMessage dataResponse;

        DigestResolver(ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(command, replicaPlan);
        }

        @Override
        void preprocess(ResponseMessage message) {
            super.preprocess(message);
            Replica replica = replicaPlan.lookup(message.from());
            if (dataResponse == null && !message.payload().isDigestResponse() && replica != null)
                dataResponse = message;
        }

        /** DigestResolver.getData (l.77). PORT: no transient responses, so the data response itself. */
        Map<String, Cell> getData() {
            return toRow(dataResponse.payload().data());
        }

        /** DigestResolver.responsesMatch (l.105). */
        boolean responsesMatch() {
            String digest = null;
            List<ResponseMessage> snapshot = List.copyOf(responses);
            assert snapshot.size() > 0 : "Attempted response match comparison while no responses have been received.";
            if (snapshot.size() == 1)
                return true;

            for (ResponseMessage message : snapshot) {
                String newDigest = message.payload().digest(command);
                if (digest == null)
                    digest = newDigest;
                else if (!digest.equals(newDigest))
                    return false;
            }
            return true;
        }

        @Override
        boolean isDataPresent() {
            return dataResponse != null;
        }
    }

    static final class DataResolver extends ResponseResolver {
        final ReadRepair readRepair;

        DataResolver(ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, ReadRepair readRepair) {
            super(command, replicaPlan);
            this.readRepair = readRepair;
        }

        @Override
        boolean isDataPresent() {
            return !responses.isEmpty();
        }

        /**
         * DataResolver.resolve (l.94) -> resolveWithReadRepair -> resolveInternal: merge the data
         * responses cell by cell with Cells.reconcile, feeding the merge listener.
         * PORT: the lazy merge iterator is evaluated eagerly here (one partition, one row); no
         * short-read protection, no replica filtering protection, no repaired-data tracking.
         */
        Map<String, Cell> resolve() {
            List<ResponseMessage> messages = List.copyOf(responses);
            assert messages.stream().noneMatch(m -> m.payload().isDigestResponse());

            // replicaPlan().readCandidates().select(transform(messages, Message::from), false)
            List<Replica> replicas = new ArrayList<>();
            for (ResponseMessage m : messages)
                replicas.add(replicaPlan.lookup(m.from()));

            // ResolveContext.needsReadRepair: replicas.size() > 1
            RowIteratorMergeListener listener = null;
            if (replicas.size() > 1 && readRepair != NoopReadRepair.instance) {
                ReplicaPlan.ForTokenRead sources = replicaPlan.withContacts(List.copyOf(replicas));
                listener = readRepair.getMergeListener(sources);
            }

            return resolveInternal(messages, listener);
        }

        /** UnfilteredPartitionIterators.merge -> UnfilteredRowIterators.merge -> Rows.merge, with the listener. */
        Map<String, Cell> resolveInternal(List<ResponseMessage> messages, RowIteratorMergeListener listener) {
            int n = messages.size();
            List<Map<String, Cell>> versions = new ArrayList<>(n);
            boolean anyRow = false;
            for (ResponseMessage m : messages) {
                Map<String, Cell> row = toRow(m.payload().data());
                versions.add(row.isEmpty() ? null : row); // an empty partition has no row
                anyRow |= !row.isEmpty();
            }
            TreeMap<String, Cell> merged = new TreeMap<>();
            if (anyRow) {
                // Rows.merge: per column, reconcile every version's cell
                for (String column : COLUMNS) {
                    Cell m = null;
                    for (Map<String, Cell> v : versions)
                        if (v != null) m = Cells.reconcile(m, v.get(column));
                    if (m != null) merged.put(column, m);
                }
                if (listener != null)
                    listener.onMergedRows(merged, versions);
            }
            if (listener != null)
                listener.close();
            return merged;
        }
    }

    static Map<String, Cell> toRow(List<Cell> cells) {
        TreeMap<String, Cell> row = new TreeMap<>();
        for (Cell c : cells)
            row.put(c.column(), c);
        return row;
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/ReadCallback.java
    // ------------------------------------------------------------------------------------------

    static final class ReadCallback implements RequestCallback {
        final Node node;
        final ResponseResolver resolver;
        boolean condition = false; // the one-time Condition
        final int blockFor;

        ReadCallback(Node node, ResponseResolver resolver, ReplicaPlan.ForTokenRead replicaPlan) {
            this.node = node;
            this.resolver = resolver;
            this.blockFor = replicaPlan.readQuorum();
        }

        /** ReadCallback.awaitResults (l.126). PORT: await without timeout; no failures, no warnings. */
        void awaitResults() {
            node.awaitUntil(() -> condition);
        }

        /** ReadCallback.onResponse (l.179). */
        @Override
        public void onResponse(Tid from, Object payload) {
            resolver.preprocess(new ResponseMessage(from, (ReadResponse) payload));
            if (resolver.isDataPresent() && resolver.responses.size() >= blockFor)
                condition = true; // condition.signalAll()
        }

        /** ReadCallback.response: the local read's result. */
        void response(ReadResponse result) {
            onResponse(node.self, result);
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/AbstractReadExecutor.java (NeverSpeculatingReadExecutor)
    // ------------------------------------------------------------------------------------------

    abstract static class AbstractReadExecutor {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        final ReadRepair readRepair;
        final DigestResolver digestResolver;
        final ReadCallback handler;
        final int initialDataRequestCount;
        Map<String, Cell> result = null;

        AbstractReadExecutor(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, int initialDataRequestCount) {
            this.node = node;
            this.command = command;
            this.replicaPlan = replicaPlan;
            this.initialDataRequestCount = initialDataRequestCount;
            this.readRepair = node.readRepairStrategy.create(node, command, replicaPlan); // ReadRepair.create
            this.digestResolver = new DigestResolver(command, replicaPlan);
            this.handler = new ReadCallback(node, digestResolver, replicaPlan);
            // PORT: digest version negotiation dropped (one version).
        }

        ReadRepair getReadRepair() {
            return readRepair;
        }

        void makeFullDataRequests(List<Replica> replicas) {
            makeRequests(command, replicas);
        }

        void makeDigestRequests(List<Replica> replicas) {
            makeRequests(command.copyAsDigestQuery(), replicas);
        }

        /** AbstractReadExecutor.makeRequests: remote requests first, the local read last. */
        void makeRequests(ReadCommand readCommand, List<Replica> replicas) {
            boolean hasLocalEndpoint = false;
            for (Replica replica : replicas) {
                if (replica.isSelf()) {
                    hasLocalEndpoint = true;
                    continue;
                }
                node.sendRead(replica, handler, readCommand);
            }
            // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
            if (hasLocalEndpoint) {
                // Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(readCommand, handler))
                node.localRead(readCommand, handler);
            }
        }

        abstract void maybeTryAdditionalReplicas();

        /** AbstractReadExecutor.executeAsync (l.178). PORT: no transient replicas (makeTransientDataRequests is empty). */
        void executeAsync() {
            List<Replica> selected = replicaPlan.contacts();
            List<Replica> fullDataRequests = List.copyOf(selected.subList(0, Math.min(initialDataRequestCount, selected.size())));
            makeFullDataRequests(fullDataRequests);
            List<Replica> digests = new ArrayList<>();
            for (Replica r : selected)
                if (!fullDataRequests.contains(r)) digests.add(r);
            makeDigestRequests(digests);
        }

        /**
         * AbstractReadExecutor.getReadExecutor (l.190). PORT: with no clock there is no
         * speculation: always a NeverSpeculatingReadExecutor.
         */
        static AbstractReadExecutor getReadExecutor(Node node, ReadCommand command) {
            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(node);
            return new NeverSpeculatingReadExecutor(node, command, replicaPlan);
        }

        void setResult(Map<String, Cell> result) {
            if (this.result != null) throw new IllegalStateException("Result can only be set once");
            this.result = result;
        }

        /** AbstractReadExecutor.awaitResponses (l.396). */
        void awaitResponses() {
            handler.awaitResults();
            assert digestResolver.isDataPresent() : "awaitResults returned with no data present.";

            // return immediately, or begin a read repair
            if (digestResolver.responsesMatch()) {
                setResult(digestResolver.getData());
            } else {
                readRepair.startRepair(digestResolver, this::setResult);
            }
        }

        /** AbstractReadExecutor.awaitReadRepair (l.442). */
        void awaitReadRepair() {
            readRepair.awaitReads();
        }

        boolean isDone() {
            return result != null;
        }

        /** AbstractReadExecutor.maybeSendAdditionalDataRequests (l.465). */
        void maybeSendAdditionalDataRequests() {
            if (isDone())
                return;
            readRepair.maybeSendAdditionalReads();
        }

        /** AbstractReadExecutor.getResult (l.473). */
        Map<String, Cell> getResult() {
            if (result == null) throw new IllegalStateException("Result must be set first");
            return result;
        }
    }

    static final class NeverSpeculatingReadExecutor extends AbstractReadExecutor {
        NeverSpeculatingReadExecutor(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan, 1);
        }

        @Override
        void maybeTryAdditionalReplicas() {
            // PORT: shouldSpeculateAndMaybeWait only records a metric here; no clock, nothing to do.
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/repair: ReadRepair, AbstractReadRepair, BlockingReadRepair,
    // ReadOnlyReadRepair, NoopReadRepair, BlockingPartitionRepair, the merge listeners
    // ------------------------------------------------------------------------------------------

    interface ReadRepair {
        RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicaPlan);
        void startRepair(DigestResolver digestResolver, Consumer<Map<String, Cell>> resultConsumer);
        void awaitReads();
        void maybeSendAdditionalReads();
        void maybeSendAdditionalWrites();
        void awaitWrites();
        void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan);
    }

    abstract static class AbstractReadRepair implements ReadRepair {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        DigestRepair digestRepair = null;

        record DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<Map<String, Cell>> resultConsumer) {}

        AbstractReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            this.node = node;
            this.command = command;
            this.replicaPlan = replicaPlan;
        }

        /** AbstractReadRepair.sendReadCommand. */
        void sendReadCommand(Replica to, ReadCallback readCallback) {
            if (to.isSelf()) {
                // Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(command, readCallback, ...))
                node.localRead(command, readCallback);
                return;
            }
            node.sendRead(to, readCallback, command);
        }

        /** AbstractReadRepair.startRepair (l.126): full data reads to every contact. */
        @Override
        public void startRepair(DigestResolver digestResolver, Consumer<Map<String, Cell>> resultConsumer) {
            // PORT: no metrics; repaired-data tracking off (its default).
            DataResolver resolver = new DataResolver(command, replicaPlan, this);
            ReadCallback readCallback = new ReadCallback(node, resolver, replicaPlan);

            digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);

            for (Replica replica : replicaPlan.contacts())
                sendReadCommand(replica, readCallback);
        }

        /** AbstractReadRepair.awaitReads (l.156). */
        @Override
        public void awaitReads() {
            DigestRepair repair = digestRepair;
            if (repair == null)
                return;
            repair.readCallback().awaitResults();
            repair.resultConsumer().accept(digestRepair.dataResolver().resolve());
        }

        /** AbstractReadRepair.maybeSendAdditionalReads (l.185). */
        @Override
        public void maybeSendAdditionalReads() {
            DigestRepair repair = digestRepair;
            if (repair == null)
                return;
            // PORT: shouldSpeculate && !awaitUntil(sampleLatency): no clock, the timed wait never
            // expires, so no speculative read is sent (awaitReads waits next).
        }
    }

    static final class BlockingReadRepair extends AbstractReadRepair {
        final List<BlockingPartitionRepair> repairs = new ArrayList<>();

        BlockingReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan);
        }

        @Override
        public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicaPlan) {
            // PartitionIteratorMergeListener.getRowMergeListener -> RowIteratorMergeListener
            return new RowIteratorMergeListener(node, replicaPlan, command, this);
        }

        /** BlockingReadRepair.maybeSendAdditionalWrites (l.74). */
        @Override
        public void maybeSendAdditionalWrites() {
            for (BlockingPartitionRepair repair : repairs)
                repair.maybeSendAdditionalWrites();
        }

        /** BlockingReadRepair.awaitWrites (l.83). */
        @Override
        public void awaitWrites() {
            BlockingPartitionRepair timedOut = null;
            for (BlockingPartitionRepair repair : repairs) {
                if (!repair.awaitRepairsUntil()) {
                    timedOut = repair;
                    break;
                }
            }
            if (timedOut != null)
                throw new IllegalStateException("read repair timed out"); // PORT: unreachable (no timeouts)
        }

        /** BlockingReadRepair.repairPartition (l.112). */
        @Override
        public void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan) {
            BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(node, mutations, writePlan);
            blockingRepair.sendInitialRepairs();
            repairs.add(blockingRepair);
        }
    }

    /** ReadOnlyReadRepair: the NONE strategy: full data reads and reconciliation, no repair mutations. */
    static final class ReadOnlyReadRepair extends AbstractReadRepair {
        ReadOnlyReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan);
        }

        @Override
        public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicaPlan) {
            return null; // MergeListener.NOOP
        }

        @Override
        public void maybeSendAdditionalWrites() {}

        @Override
        public void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan) {
            throw new UnsupportedOperationException("ReadOnlyReadRepair shouldn't be trying to repair partitions");
        }

        @Override
        public void awaitWrites() {}
    }

    /** NoopReadRepair: bypasses read repair (used by DigestResolver.getData on transient responses only; never selected here). */
    static final class NoopReadRepair implements ReadRepair {
        static final NoopReadRepair instance = new NoopReadRepair();

        private NoopReadRepair() {}

        @Override public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicas) { return null; }

        @Override
        public void startRepair(DigestResolver digestResolver, Consumer<Map<String, Cell>> resultConsumer) {
            resultConsumer.accept(digestResolver.getData());
        }

        @Override public void awaitReads() {}
        @Override public void maybeSendAdditionalReads() {}
        @Override public void maybeSendAdditionalWrites() {}
        @Override public void awaitWrites() {}
        @Override public void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan) {}
    }

    /** service/reads/repair/BlockingPartitionRepair.java */
    static final class BlockingPartitionRepair implements RequestCallback {
        final Node node;
        final ReplicaPlan.ForWrite writePlan;
        final Map<Replica, Mutation> pendingRepairs;
        int latch; // the CountDownLatch

        BlockingPartitionRepair(Node node, Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan) {
            this.node = node;
            this.pendingRepairs = new LinkedHashMap<>(repairs); // PORT: ConcurrentHashMap -> insertion order (writePlan.contacts order)
            this.writePlan = writePlan;

            for (Replica r : repairs.keySet())
                if (!writePlan.contacts().contains(r))
                    throw new IllegalStateException("All repair targets should be part of contacts of read repair write plan.");

            int blockFor = writePlan.writeQuorum();
            // here we remove empty repair mutations from the block for total, since
            // we're not sending them mutations
            for (Replica participant : writePlan.contacts()) {
                if (!repairs.containsKey(participant))
                    blockFor--;
            }
            latch = Math.max(blockFor, 0);
        }

        int blockFor() {
            return writePlan.writeQuorum();
        }

        int waitingOn() {
            return latch;
        }

        /** BlockingPartitionRepair.ack (l.112). */
        void ack(Tid from) {
            pendingRepairs.remove(writePlan.lookup(from));
            if (latch > 0) latch--; // latch.decrement()
        }

        @Override
        public void onResponse(Tid from, Object payload) {
            ack(from);
        }

        void sendRR(Mutation mutation, Replica endpoint) {
            // MessagingService.sendWithCallback(Message.out(READ_REPAIR_REQ, mutation), endpoint, this):
            // a repair of the coordinator's own replica also goes to its MUTATION stage as a message.
            node.sendReadRepair(endpoint, this, mutation);
        }

        /** BlockingPartitionRepair.sendInitialRepairs. */
        void sendInitialRepairs() {
            for (Map.Entry<Replica, Mutation> entry : List.copyOf(pendingRepairs.entrySet())) {
                Replica destination = entry.getKey();
                Mutation mutation = entry.getValue();
                // use a separate verb here to avoid writing hints on timeouts
                sendRR(mutation, destination);
            }
        }

        /**
         * BlockingPartitionRepair.awaitRepairsUntil (l.172). PORT: no deadline (a timeout never
         * fires): waits until the latch reaches zero and returns true.
         */
        boolean awaitRepairsUntil() {
            node.awaitUntil(() -> latch == 0);
            return true;
        }

        /**
         * BlockingPartitionRepair.maybeSendAdditionalWrites (l.198). The first statement waits
         * (awaitRepairsUntil) and returns when the repair completed in time; PORT: with no
         * timeouts it always completes, so the additional writes to uncontacted replicas are
         * never sent.
         */
        void maybeSendAdditionalWrites() {
            if (awaitRepairsUntil())
                return;
            throw new IllegalStateException("unreachable: no timeouts");
        }
    }

    /**
     * service/reads/repair/RowIteratorMergeListener.java (with PartitionIteratorMergeListener):
     * which cells of which source are stale, and the repair mutation per source.
     * PORT: one row, simple live cells only: no partition deletions, range tombstones, row
     * deletions or primary-key liveness (an UPDATE writes no row marker).
     */
    static final class RowIteratorMergeListener {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead readPlan;
        final ReplicaPlan.ForWrite writePlan;
        final boolean[] writeBackTo;
        final boolean buildFullDiff;
        final List<Cell>[] repairs; // the PartitionUpdate.Builders
        final ReadRepair readRepair;

        @SuppressWarnings("unchecked")
        RowIteratorMergeListener(Node node, ReplicaPlan.ForTokenRead readPlan, ReadCommand command, ReadRepair readRepair) {
            this.node = node;
            this.readPlan = readPlan;
            this.writePlan = ReplicaPlans.forReadRepair(node, readPlan);

            int size = readPlan.contacts().size();
            this.writeBackTo = new boolean[size];
            {
                int i = 0;
                for (Replica replica : readPlan.contacts()) {
                    if (ReplicaPlans.containsEndpoint(writePlan.contacts(), replica.endpoint()))
                        writeBackTo[i] = true;
                    ++i;
                }
            }
            boolean full = false;
            for (Replica r : writePlan.contacts())
                if (!ReplicaPlans.containsEndpoint(readPlan.contacts(), r.endpoint())) full = true;
            this.buildFullDiff = full;
            this.repairs = new List[size + (buildFullDiff ? 1 : 0)];
            this.command = command;
            this.readRepair = readRepair;
        }

        /** diffListener.onCell. */
        void onCell(int i, Cell merged, Cell original) {
            if (merged != null && !merged.equals(original) && isQueried(merged))
                applyToPartition(i, merged);
        }

        boolean isQueried(Cell cell) {
            return command.queriedColumns().contains(cell.column());
        }

        // PORT: currentRow(i).addCell + onMergedRows' applyToPartition(i, p -> p.add(row)) folded
        // into one step (one row, so the row builder per source is the partition builder's row).
        void applyToPartition(int i, Cell cell) {
            if (writeBackTo[i]) {
                if (repairs[i] == null)
                    repairs[i] = new ArrayList<>();
                repairs[i].add(cell);
            }
            if (buildFullDiff) {
                if (repairs[repairs.length - 1] == null)
                    repairs[repairs.length - 1] = new ArrayList<>();
                repairs[repairs.length - 1].add(cell);
            }
        }

        /** onMergedRows -> Rows.diff: per column of the merged row, per source. */
        void onMergedRows(Map<String, Cell> merged, List<Map<String, Cell>> versions) {
            if (merged.isEmpty())
                return;
            for (String column : COLUMNS) { // ColumnData.comparator order
                Cell mergedData = merged.get(column);
                for (int i = 0; i != versions.size(); i++) {
                    Cell input = versions.get(i) == null ? null : versions.get(i).get(column);
                    if (mergedData != null || input != null)
                        onCell(i, mergedData, input);
                }
            }
        }

        /** RowIteratorMergeListener.close: build the mutations per write-plan contact and hand them to repairPartition. */
        void close() {
            boolean hasRepairs = false;
            for (int i = 0; !hasRepairs && i < repairs.length; ++i)
                hasRepairs = repairs[i] != null;
            if (!hasRepairs)
                return;

            List<Cell> fullDiffRepair = null;
            if (buildFullDiff && repairs[repairs.length - 1] != null)
                fullDiffRepair = repairs[repairs.length - 1];

            Map<Replica, Mutation> mutations = new LinkedHashMap<>(); // PORT: HashMap -> writePlan order
            Map<Tid, Integer> sourceIds = new HashMap<>();
            for (int i = 0; i < readPlan.contacts().size(); ++i)
                sourceIds.put(readPlan.contacts().get(i).endpoint(), 1 + i);

            for (Replica replica : writePlan.contacts()) {
                List<Cell> update = null;
                int i = -1 + sourceIds.getOrDefault(replica.endpoint(), 0);
                if (i < 0)
                    update = fullDiffRepair;
                else if (repairs[i] != null)
                    update = repairs[i];

                Mutation mutation = BlockingReadRepairs.createRepairMutation(update);
                if (mutation == null)
                    continue;

                mutations.put(replica, mutation);
            }

            readRepair.repairPartition(mutations, writePlan);
        }
    }

    static final class BlockingReadRepairs {
        /** BlockingReadRepairs.createRepairMutation. PORT: no size limit (a mutation is never oversized). */
        static Mutation createRepairMutation(List<Cell> update) {
            if (update == null)
                return null;
            List<Cell> sorted = new ArrayList<>(update);
            sorted.sort((x, y) -> x.column().compareTo(y.column()));
            return new Mutation(new PartitionUpdate(List.copyOf(sorted)));
        }
    }
}
