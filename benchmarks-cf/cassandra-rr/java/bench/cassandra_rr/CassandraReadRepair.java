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
    @Tag("Cell") public record Cell(String column, long value, long timestamp) implements MpMessage {}

    /** db/partitions/PartitionUpdate: the one row of the one partition. PORT: a list of cells sorted by column; an empty list is no row. */
    @Tag("PartitionUpdate") public record PartitionUpdate(List<Cell> cells) implements MpMessage {}

    /** db/Mutation: PORT: one partition update of the one table. */
    @Tag("Mutation") public record Mutation(PartitionUpdate update) implements MpMessage {}

    /** db/SinglePartitionReadCommand: PORT: the queried columns and the digest flag only. */
    @Tag("ReadCommand") public record ReadCommand(List<String> queriedColumns, boolean isDigestQuery) implements MpMessage {
        ReadCommand copyAsDigestQuery() {
            return new ReadCommand(queriedColumns, true);
        }
    }

    /**
     * db/ReadResponse: a data response (the cells read) or a digest response.
     * PORT: the digest is a String (any deterministic, injective function of the cells read, INTERFACE.md).
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
    // net/RequestCallbacks) and the sender.
    // ------------------------------------------------------------------------------------------

    @Tag("MutationReq") public record MutationReq(int id, Tid from, Mutation payload) implements MpMessage {}
    @Tag("MutationRsp") public record MutationRsp(int id, Tid from) implements MpMessage {}
    @Tag("ReadReq") public record ReadReq(int id, Tid from, ReadCommand payload) implements MpMessage {}
    @Tag("ReadRsp") public record ReadRsp(int id, Tid from, ReadResponse payload) implements MpMessage {}
    @Tag("ReadRepairReq") public record ReadRepairReq(int id, Tid from, Mutation payload) implements MpMessage {}
    @Tag("ReadRepairRsp") public record ReadRepairRsp(int id, Tid from) implements MpMessage {}

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

    /** Launcher entry: {@code impl,<q>,<blocking|none>,<fifo|bag>}. */
    public CassandraReadRepair(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<blocking|none>,<fifo|bag>");
        }
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
    }

    @Override public String programName() { return "cassandra-rr"; }
    @Override public String size() { return "q=" + q + ",rr=" + strategy.name().toLowerCase() + "," + modelName; }
    @Override public CommunicationModel model() { return model; }
    @Override public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }

    /**
     * Observation only (printed by the launcher through toString): the distinct sequences of
     * replies each client received, and how many client runs finished. Not part of the protocol;
     * it touches no message-passing state.
     */
    private final Map<String, Boolean> observedReplies = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong finishedClients = new java.util.concurrent.atomic.AtomicLong();

    @Override
    public String toString() {
        return "finished client runs " + finishedClients.get() + "; distinct replies per client "
                + new TreeSet<>(observedReplies.keySet());
    }

    static long value(int i, int j) {
        return 1 + i + (long) CLIENTS * j;
    }

    /**
     * Main: starts the replicas in index order, then the clients in index order, and waits for
     * them; it sends and receives nothing.
     */
    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int k = 0; k < REPLICAS; k++) {
            final int idx = k;
            handles.add(MpProcess.spawn("n" + k, () -> new Node(idx, q, strategy, model).run()));
        }
        for (int i = 0; i < CLIENTS; i++) {
            final int idx = i;
            handles.add(MpProcess.spawn("c" + i, () -> {
                String replies = client(idx, q, model);
                observedReplies.put("c" + idx + ":" + replies, Boolean.TRUE);
                finishedClients.incrementAndGet();
            }));
        }
        // Every process derives the others' addresses from its own task id: the processes are
        // created consecutively (checked here, before any wait).
        long base = handles.get(0).tid().raw();
        for (int k = 0; k < handles.size(); k++) {
            if (handles.get(k).tid().raw() != base + k) {
                throw new IllegalStateException("task ids are not consecutive: " + handles);
            }
        }
        for (int k = REPLICAS; k < handles.size(); k++) {
            handles.get(k).await();
        }
        for (int k = 0; k < REPLICAS; k++) {
            handles.get(k).await();
        }
    }

    static Tid replicaTid(long base, int k) {
        return Tid.of(base + k, "n" + k);
    }

    static Tid clientTid(long base, int i) {
        return Tid.of(base + REPLICAS + i, "c" + i);
    }

    /** The client loop of INTERFACE.md, verbatim in meaning. */
    static String client(int i, int q, CommunicationModel model) {
        long base = MpProcess.self().raw() - REPLICAS - i;
        Tid node = replicaTid(base, i);
        StringBuilder replies = new StringBuilder();
        for (int j = 0; j < q; j++) {
            MpMessage rep;
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(model, node, new Write(i, value(i, j)));
                    if (!((rep = Channel.recv(model)) instanceof Ack)) throw new AssertionError("Ack expected");
                }
                case 1 -> {
                    Channel.send(model, node, new ReadA(i));
                    if (!((rep = Channel.recv(model)) instanceof ValueA)) throw new AssertionError("ValueA expected");
                }
                default -> {
                    Channel.send(model, node, new ReadAB(i));
                    if (!((rep = Channel.recv(model)) instanceof ValueAB)) throw new AssertionError("ValueAB expected");
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

    static final class Node {
        final int index;
        final int q;
        final ReadRepairStrategy readRepairStrategy;
        final CommunicationModel model;
        Tid self;
        long base;
        final List<Replica> naturalReplicas = new ArrayList<>();
        /** PORT: in-memory state instead of the storage engine (memtable of the one row). */
        final TreeMap<String, Cell> memtable = new TreeMap<>();
        /** net/RequestCallbacks: id -> callback. PORT: entries never expire (no timeouts). */
        final Map<Integer, RequestCallback> callbacks = new HashMap<>();
        int nextId = 0;

        Node(int index, int q, ReadRepairStrategy readRepairStrategy, CommunicationModel model) {
            this.index = index;
            this.q = q;
            this.readRepairStrategy = readRepairStrategy;
            this.model = model;
        }

        void run() {
            self = MpProcess.self();
            base = self.raw() - index;
            for (int k = 0; k < REPLICAS; k++)
                naturalReplicas.add(new Replica(k, replicaTid(base, k), k == index));
            // PORT: the process never terminates: a replica keeps receiving (the harness ends an
            // execution blocked on receive).
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write w) {
                    // PORT: the native-transport/CQL layer (UpdateStatement.execute ->
                    // StorageProxy.mutateWithTriggers) reduced to the call of mutate.
                    StorageProxy.mutate(this, new Mutation(new PartitionUpdate(List.of(
                            new Cell("a", w.value(), w.value()), new Cell("b", w.value(), w.value())))));
                    Channel.send(model, clientTid(base, w.client()), new Ack(w.client()));
                } else if (m instanceof ReadA r) {
                    // PORT: SelectStatement.execute -> StorageProxy.read reduced to readRegular.
                    Map<String, Cell> row = StorageProxy.readRegular(this, new ReadCommand(List.of("a"), false));
                    Channel.send(model, clientTid(base, r.client()), new ValueA(r.client(), valueOf(row, "a")));
                } else if (m instanceof ReadAB r) {
                    Map<String, Cell> row = StorageProxy.readRegular(this, new ReadCommand(List.of("a", "b"), false));
                    Channel.send(model, clientTid(base, r.client()),
                            new ValueAB(r.client(), valueOf(row, "a"), valueOf(row, "b")));
                } else {
                    dispatch(m);
                }
            }
        }

        static long valueOf(Map<String, Cell> row, String column) {
            Cell c = row.get(column);
            return c == null ? 0 : c.value(); // an absent cell counts as 0 (INTERFACE.md)
        }

        /**
         * PORT: Cassandra's stages (READ, MUTATION, the inbound message handlers, the
         * callback executor) run concurrently with a coordinator thread blocked on a condition. A
         * process here is single-threaded, so every wait of the coordinator is a loop that
         * receives the next message of any kind and handles it as the inbound path would, until
         * the waited-for condition holds. A client request cannot arrive during a wait (the client
         * waits for its reply).
         */
        void awaitUntil(java.util.function.BooleanSupplier condition) {
            while (!condition.getAsBoolean()) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write || m instanceof ReadA || m instanceof ReadAB)
                    throw new IllegalStateException("client request during a coordinator wait: " + m);
                dispatch(m);
            }
        }

        /** net/InboundMessageHandler -> IVerbHandler.doVerb, and net/ResponseVerbHandler for responses. */
        void dispatch(MpMessage m) {
            if (m instanceof MutationReq r) {
                MutationVerbHandler.doVerb(this, r);
            } else if (m instanceof ReadReq r) {
                ReadCommandVerbHandler.doVerb(this, r);
            } else if (m instanceof ReadRepairReq r) {
                ReadRepairVerbHandler.doVerb(this, r);
            } else if (m instanceof MutationRsp r) {
                ResponseVerbHandler.doVerb(this, r.id(), r.from(), null);
            } else if (m instanceof ReadRsp r) {
                ResponseVerbHandler.doVerb(this, r.id(), r.from(), r.payload());
            } else if (m instanceof ReadRepairRsp r) {
                ResponseVerbHandler.doVerb(this, r.id(), r.from(), null);
            } else {
                throw new IllegalStateException("unexpected message " + m);
            }
        }

        // ---- net/MessagingService --------------------------------------------------------------

        int sendWithCallback(Tid to, RequestCallback callback, java.util.function.IntFunction<MpMessage> message) {
            int id = nextId++;
            callbacks.put(id, callback);
            Channel.send(model, to, message.apply(id));
            return id;
        }

        // ---- the storage engine ----------------------------------------------------------------

        /** Mutation.apply -> Keyspace.apply -> memtable put: cells are merged with Cells.reconcile. */
        void apply(Mutation mutation) {
            for (Cell c : mutation.update().cells())
                memtable.put(c.column(), Cells.reconcile(memtable.get(c.column()), c));
        }

        /** ReadCommand.executeLocally + createResponse. PORT: only the queried columns are fetched (INTERFACE.md: a digest of the cells read). */
        ReadResponse executeLocally(ReadCommand command) {
            List<Cell> data = new ArrayList<>();
            for (String column : command.queriedColumns()) {
                Cell c = memtable.get(column);
                if (c != null) data.add(c);
            }
            return command.isDigestQuery()
                   ? ReadResponse.createDigestResponse(data)
                   : ReadResponse.createDataResponse(data);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Replica side: db/MutationVerbHandler.java, db/ReadCommandVerbHandler.java,
    // db/ReadRepairVerbHandler.java, net/ResponseVerbHandler.java
    // ------------------------------------------------------------------------------------------

    static final class MutationVerbHandler {
        static void doVerb(Node node, MutationReq message) {
            // PORT: no expiry check (no timeouts), no size validation, no forwarding (one DC).
            Tid respondToAddress = message.from();
            applyMutation(node, message, respondToAddress);
        }

        static void applyMutation(Node node, MutationReq message, Tid respondToAddress) {
            // PORT: applyFuture().addCallback(respond) collapsed: apply, then respond.
            node.apply(message.payload());
            respond(node, message, respondToAddress);
        }

        static void respond(Node node, MutationReq respondTo, Tid respondToAddress) {
            Channel.send(node.model, respondToAddress, new MutationRsp(respondTo.id(), node.self));
        }
    }

    static final class ReadCommandVerbHandler {
        static void doVerb(Node node, ReadReq message) {
            ReadCommand command = message.payload();
            // PORT: no bootstrap, out-of-range, transient, monitoring or warning checks.
            ReadResponse response = node.executeLocally(command);
            Channel.send(node.model, message.from(), new ReadRsp(message.id(), node.self, response));
        }
    }

    static final class ReadRepairVerbHandler {
        static void doVerb(Node node, ReadRepairReq message) {
            // AbstractMutationVerbHandler.doVerb -> applyMutation (PORT: no token-ownership checks).
            applyMutation(node, message, message.from());
        }

        static void applyMutation(Node node, ReadRepairReq message, Tid respondToAddress) {
            node.apply(message.payload());
            Channel.send(node.model, respondToAddress, new ReadRepairRsp(message.id(), node.self));
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
                // performLocally(stage, localReplica, mutation::apply, responseHandler, ...)
                // PORT: Stage.MUTATION.maybeExecuteImmediately taken as executing immediately, on
                // the coordinator's process (the only thread of this replica).
                node.apply(mutation);
                responseHandler.onResponse(localReplica.endpoint(), null);
            }

            if (localDc != null) {
                for (Replica destination : localDc)
                    node.sendWithCallback(destination.endpoint(), responseHandler,
                            id -> new MutationReq(id, node.self, mutation));
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
         * snitch; contacts = the first blockFor (2) of them.
         * PORT: the snitch order is replaced by a nondeterministic choice of which of the two
         * other replicas is "fastest": the local replica first, then the chosen one, then the third.
         */
        static ReplicaPlan.ForTokenRead forRead(Node node) {
            Replica local = node.naturalReplicas.get(node.index);
            List<Replica> others = new ArrayList<>();
            for (Replica r : node.naturalReplicas)
                if (!r.isSelf()) others.add(r);
            int fastest = Nondet.chooseIndex(others.size());
            List<Replica> candidates = new ArrayList<>();
            candidates.add(local);
            candidates.add(others.get(fastest));
            candidates.add(others.get(1 - fastest));
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
                node.sendWithCallback(replica.endpoint(), handler, id -> new ReadReq(id, node.self, readCommand));
            }
            // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
            if (hasLocalEndpoint) {
                // Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(readCommand, handler))
                // PORT: executed immediately on this process.
                handler.response(node.executeLocally(readCommand));
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
                // PORT: executed immediately on this process.
                readCallback.response(node.executeLocally(command));
                return;
            }
            node.sendWithCallback(to.endpoint(), readCallback, id -> new ReadReq(id, node.self, command));
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

        void sendRR(Mutation mutation, Tid endpoint) {
            // MessagingService.sendWithCallback(Message.out(READ_REPAIR_REQ, mutation), endpoint, this):
            // a repair to the coordinator itself is a message to itself, as in Cassandra.
            node.sendWithCallback(endpoint, this, id -> new ReadRepairReq(id, node.self, mutation));
        }

        /** BlockingPartitionRepair.sendInitialRepairs. */
        void sendInitialRepairs() {
            for (Map.Entry<Replica, Mutation> entry : List.copyOf(pendingRepairs.entrySet())) {
                Replica destination = entry.getKey();
                Mutation mutation = entry.getValue();
                // use a separate verb here to avoid writing hints on timeouts
                sendRR(mutation, destination.endpoint());
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
