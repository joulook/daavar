package bench.cassandra_rr_port;

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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Port of Apache Cassandra 4.1's quorum write path and quorum read path with read repair
 * ({@code ReadRepairStrategy.BLOCKING} and {@code ReadRepairStrategy.NONE}) to JMC's message-passing
 * (Must) API. One partition with two regular cells {@code a} and {@code b}, replication factor 3, every
 * operation at QUORUM; three replicas n0, n1, n2 (one process each: the node's coordinator side, its verb handlers on
 * their stages and its memtable), each the coordinator of its client, two clients c0, c1.
 *
 * <p>Source paths in the comments are relative to {@code src/java/org/apache/cassandra} of the study's cut
 * ({@code plan/case-studies/cassandra/read-repair-src}). Every departure from the source is marked
 * {@code // PORT:} with its reason; NOTES.md lists the decisions.
 *
 * <p>Launcher argument string: {@code impl,<q>,<blocking|none>,<fifo|bag>}.
 */
public final class CassandraReadRepair implements MpProgram {

    // ------------------------------------------------------------------------------------------
    // The interface (INTERFACE.md): visible messages and the client loop, fixed.
    // ------------------------------------------------------------------------------------------

    @Tag("Write") public record Write(int client, long value) implements MpMessage {}
    @Tag("ReadA") public record ReadA(int client) implements MpMessage {}
    @Tag("ReadAB") public record ReadAB(int client) implements MpMessage {}
    @Tag("Ack") public record Ack(int client) implements MpMessage {}
    @Tag("ValueA") public record ValueA(int client, long a) implements MpMessage {}
    @Tag("ValueAB") public record ValueAB(int client, long a, long b) implements MpMessage {}

    static final int CLIENTS = 2;
    static final int REPLICAS = 3;

    static long value(int i, int j) {
        return 1L + i + (long) CLIENTS * j;
    }

    /** The client loop of INTERFACE.md, verbatim: ci sends its requests to ni and waits for each reply. */
    static void client(CommunicationModel md, int i, int q, Tid node) {
        for (int j = 0; j < q; j++) {
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(md, node, new Write(i, value(i, j)));
                    if (!(Channel.recv(md) instanceof Ack)) throw new AssertionError("Ack expected");
                }
                case 1 -> {
                    Channel.send(md, node, new ReadA(i));
                    if (!(Channel.recv(md) instanceof ValueA)) throw new AssertionError("ValueA expected");
                }
                default -> {
                    Channel.send(md, node, new ReadAB(i));
                    if (!(Channel.recv(md) instanceof ValueAB)) throw new AssertionError("ValueAB expected");
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Data: bounded replacements for db/rows/Cell, db/rows/Cells, db/rows/Row, db/partitions/PartitionUpdate,
    // db/ReadCommand, db/ReadResponse and db/filter/ColumnFilter.
    // ------------------------------------------------------------------------------------------

    /** Column indices: a = 0, b = 1 (the two regular columns of the table). */
    static final int COL_A = 0;
    static final int COL_B = 1;
    static final List<Integer> ALL_COLUMNS = List.of(COL_A, COL_B);

    /** ConsistencyLevel.QUORUM.blockFor for replication factor 3: quorumFor = 3 / 2 + 1 = 2 (db/ConsistencyLevel.java l.91-100, 133-148). */
    static final int QUORUM = 2;

    /** service/reads/repair/ReadRepairStrategy.java: BLOCKING -> BlockingReadRepair, NONE -> ReadOnlyReadRepair. */
    enum ReadRepairStrategy { BLOCKING, NONE }

    /**
     * db/rows/Cell: a regular live cell with its value and timestamp. PORT: a timestamp of 0 is the absent cell
     * (written timestamps are the written values, at least 1); no TTL, no tombstones, no deletion time (the
     * interface's writes are plain UPDATEs). {@link #withSkippedValue()} is Cell.withSkippedValue: the value is
     * dropped (PORT: 0 stands for the empty value), the timestamp kept.
     */
    @Tag("Cell") public record Cell(long value, long timestamp) implements MpMessage {
        static final Cell ABSENT = new Cell(0, 0);

        boolean isAbsent() {
            return timestamp == 0;
        }

        Cell withSkippedValue() {
            return new Cell(0, timestamp);
        }
    }

    /** db/rows/Cells.java: reconcile (l.68-77) and resolveRegular (l.79-96) for two live regular cells. */
    static final class Cells {
        static Cell reconcile(Cell c1, Cell c2) {
            // l.70-71: a missing cell loses against a present one
            if (c1.isAbsent() || c2.isAbsent()) return c2.isAbsent() ? c1 : c2;
            return resolveRegular(c1, c2);
        }

        private static Cell resolveRegular(Cell left, Cell right) {
            long leftTimestamp = left.timestamp();
            long rightTimestamp = right.timestamp();
            if (leftTimestamp != rightTimestamp) return leftTimestamp > rightTimestamp ? left : right;
            // PORT: neither cell is expiring or a tombstone (no TTL, no deletions); equal timestamps are
            // resolved by the value as the source does last (the bigger value wins), which only matters between
            // a full cell and its skipped-value copy.
            return left.value() >= right.value() ? left : right;
        }
    }

    /**
     * db/rows/Row of the one partition: one cell per column in column order, {@link Cell#ABSENT} where the row
     * has no cell for the column. An empty row (both absent) is "no row". PORT: a List for the message
     * encoder (no arrays).
     */
    static List<Cell> emptyRow() {
        return List.of(Cell.ABSENT, Cell.ABSENT);
    }

    /**
     * db/partitions/PartitionUpdate as carried by a db/Mutation: the cells of the one row to merge in, one per
     * column, {@link Cell#ABSENT} for a column the update does not touch.
     */
    @Tag("Mutation") public record Mutation(List<Cell> update) implements MpMessage {
        /** The interface's write: UPDATE ... SET a = v, b = v with timestamp v. */
        static Mutation write(long v) {
            return new Mutation(List.of(new Cell(v, v), new Cell(v, v)));
        }
    }

    /**
     * db/SinglePartitionReadCommand: the column filter and the digest flag. PORT: the column filter is reduced to
     * the queried columns; the fetched columns are always every regular column, as ColumnFilterFactory.fromColumns
     * builds the filter with ColumnFilter.allRegularColumnsBuilder (full tree cql3/selection/ColumnFilterFactory.java
     * l.46-57).
     */
    @Tag("ReadCommand") public record ReadCommand(List<Integer> queriedColumns, boolean isDigestQuery) implements MpMessage {
        /** ColumnFilter.fetchedColumns(): every regular column. */
        List<Integer> fetchedColumns() {
            return ALL_COLUMNS;
        }

        /** ColumnFilter.fetchedColumnIsQueried. */
        boolean fetchedColumnIsQueried(int column) {
            return queriedColumns.contains(column);
        }

        /** ReadCommand.copyAsDigestQuery. */
        ReadCommand copyAsDigestQuery() {
            return new ReadCommand(queriedColumns, true);
        }

        /**
         * ReadCommand.executeLocally + BTreeRow.filter (full tree db/rows/BTreeRow.java l.344-371): the row of the
         * partition restricted to the fetched columns, with the value of a fetched-but-not-queried cell skipped
         * (the row has no primary-key liveness since the writes are UPDATEs, so a skippable cell is never dropped).
         */
        List<Cell> executeLocally(List<Cell> partition) {
            List<Cell> row = new ArrayList<>();
            for (int column : fetchedColumns()) {
                Cell cell = partition.get(column);
                row.add(fetchedColumnIsQueried(column) || cell.isAbsent() ? cell : cell.withSkippedValue());
            }
            return List.copyOf(row);
        }

        /** ReadCommand.createResponse: a digest response for a digest query, else a data response. */
        ReadResponse createResponse(List<Cell> row) {
            return isDigestQuery ? ReadResponse.createDigestResponse(row) : ReadResponse.createDataResponse(row);
        }
    }

    /**
     * db/ReadResponse: a data response carrying the (filtered) row, or a digest response carrying its digest.
     * PORT: ReadResponse.makeDigest (an MD5 of the serialized partition) is replaced by a text that is injective
     * over the row's cells (presence, value or skipped value, timestamp); equal rows give equal digests and
     * different rows different ones. The data field of a digest response is the empty row (no nulls).
     */
    @Tag("ReadResponse") public record ReadResponse(boolean isDigestResponse, List<Cell> data, String digestValue) implements MpMessage {
        static ReadResponse createDataResponse(List<Cell> row) {
            return new ReadResponse(false, row, "");
        }

        static ReadResponse createDigestResponse(List<Cell> row) {
            return new ReadResponse(true, List.of(), makeDigest(row));
        }

        /** ReadResponse.digest(command): the digest of a data response is computed from its data (l.283-287). */
        String digest() {
            return isDigestResponse ? digestValue : makeDigest(data);
        }

        static String makeDigest(List<Cell> row) {
            StringBuilder sb = new StringBuilder();
            for (int column = 0; column < row.size(); column++) {
                Cell c = row.get(column);
                if (c.isAbsent()) continue; // an absent cell is not serialized
                sb.append(column).append(':').append(c.value()).append('@').append(c.timestamp()).append(';');
            }
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------------------------------
    // Messages among the nodes (net/Verb.java MUTATION_REQ/RSP, READ_REQ/RSP, READ_REPAIR_REQ/RSP). PORT: the
    // net/Message header is reduced to the message id (net/RequestCallbacks correlates the response by it) and the
    // address to respond to; a response names the replica it comes from (Message.from) by its index.
    // ------------------------------------------------------------------------------------------

    @Tag("MutationReq") public record MutationReq(long id, Tid respondTo, Mutation payload) implements MpMessage {}
    @Tag("MutationRsp") public record MutationRsp(long id, int from) implements MpMessage {}
    @Tag("ReadReq") public record ReadReq(long id, Tid respondTo, ReadCommand payload) implements MpMessage {}
    @Tag("ReadRsp") public record ReadRsp(long id, int from, ReadResponse payload) implements MpMessage {}
    @Tag("ReadRepairReq") public record ReadRepairReq(long id, Tid respondTo, Mutation payload) implements MpMessage {}
    @Tag("ReadRepairRsp") public record ReadRepairRsp(long id, int from) implements MpMessage {}

    /**
     * A task of the node's MUTATION stage: a MUTATION_REQ handed to the stage by the inbound message handler
     * (net/InboundMessageHandler: the verb's stage executes the handler, Verb.MUTATION_REQ l.114 -> Stage.MUTATION), or
     * the local mutation of performLocally (StorageProxy l.1549, l.1646-1655: Stage.MUTATION.maybeExecuteImmediately). PORT: the
     * stage's queue is a message from the node to itself; the task executes when the node receives it, at any later
     * receive point, so that a later request may be handled before the mutation is applied, as the source's concurrent
     * stages allow. A local task carries {@code local = true}: its response goes to the handler directly (no message).
     */
    @Tag("MutationTask") public record MutationTask(long id, Tid respondTo, Mutation payload, boolean local) implements MpMessage {}

    /** The one invisible setup message (INTERFACE.md): the addresses of every process, sent by init to each node. */
    @Tag("Setup") public record Setup(List<Tid> nodes, List<Tid> clients) implements MpMessage {}

    static boolean isRequest(MpMessage m) {
        return m instanceof MutationReq || m instanceof ReadReq || m instanceof ReadRepairReq || m instanceof MutationTask;
    }

    static boolean isResponse(MpMessage m) {
        return m instanceof MutationRsp || m instanceof ReadRsp || m instanceof ReadRepairRsp;
    }

    // ------------------------------------------------------------------------------------------
    // The node n<i>: one process. It holds the node's memtable (db/partitions/AtomicBTreePartition: the one row), runs the
    // coordinator side (service/StorageProxy with the MessagingService callbacks, net/RequestCallbacks,
    // net/ResponseVerbHandler) for its one client, and runs the replica side's verb handlers (db/MutationVerbHandler,
    // db/ReadCommandVerbHandler, db/ReadRepairVerbHandler) on the node's stages. PORT: the source's threads of one node
    // (the request thread that waits on its callbacks, the MUTATION, READ and REQUEST_RESPONSE stages) are one process:
    // whenever the coordinator side waits (Node.awaitUntil) and whenever it is idle (Node.run), the node receives and
    // dispatches every message addressed to it: a response goes to its callback (ResponseVerbHandler.doVerb), a read or
    // repair request is executed by its handler at receipt, a mutation request is handed to the MUTATION stage (a task
    // the node sends to itself, executed when received), so that the open execution order of the source's concurrent
    // stages is explored by the receive order. A local read runs inline (Stage.READ.maybeExecuteImmediately); a local
    // mutation is a MUTATION-stage task like a remote one (performLocally).
    // ------------------------------------------------------------------------------------------

    /** net/RequestCallback. */
    interface RequestCallback {
        void onResponse(MpMessage message);
    }

    /** locator/Replica: a replica of the one token, by index; isSelf as in the source. */
    record Replica(int index) {
        boolean isSelf(Node node) {
            return index == node.me;
        }
    }

    static final class Node {
        final CommunicationModel md;
        final int me;
        final Tid self;
        final Setup setup;
        final ReadRepairStrategy strategy;
        /** db/partitions/AtomicBTreePartition: the row of the one partition, one cell per column (the memtable). */
        List<Cell> partition = emptyRow();
        /** net/MessagingService's next message id. */
        private long nextId = 0;
        /** net/RequestCallbacks: the callbacks waiting for responses, by message id (one id per request and destination). */
        private final Map<Long, RequestCallback> callbacks = new HashMap<>();

        Node(CommunicationModel md, int me, Tid self, Setup setup, ReadRepairStrategy strategy) {
            this.md = md;
            this.me = me;
            this.self = self;
            this.setup = setup;
            this.strategy = strategy;
        }

        /** MessagingService.sendWithCallback: register the callback under a fresh message id, send to the replica. */
        void sendWithCallback(Replica to, RequestCallback callback, java.util.function.LongFunction<MpMessage> message) {
            long id = ++nextId;
            callbacks.put(id, callback);
            Channel.send(md, setup.nodes().get(to.index()), message.apply(id));
        }

        /**
         * The wait of a handler (a Condition.await / latch.await of the source) until its condition holds: the node keeps
         * receiving and dispatching its messages (responses, requests, stage tasks) meanwhile.
         */
        void awaitUntil(BooleanSupplier signalled) {
            while (!signalled.getAsBoolean()) {
                dispatch(Channel.recv(md, m -> isResponse(m) || isRequest(m)));
            }
        }

        /**
         * net/InboundMessageHandler + net/ResponseVerbHandler: one received message. A response is delivered to its
         * callback, which is removed first (RequestCallbacks.remove, ResponseVerbHandler.doVerb l.36-58); a response whose
         * callback is unknown is dropped. A MUTATION_REQ is handed to the MUTATION stage (MutationTask); a stage task,
         * a READ_REQ and a READ_REPAIR_REQ are executed by their verb handlers.
         */
        void dispatch(MpMessage m) {
            if (m instanceof MutationRsp r) {
                RequestCallback callback = callbacks.remove(r.id());
                if (callback != null) callback.onResponse(m);
            } else if (m instanceof ReadRsp r) {
                RequestCallback callback = callbacks.remove(r.id());
                if (callback != null) callback.onResponse(m);
            } else if (m instanceof ReadRepairRsp r) {
                RequestCallback callback = callbacks.remove(r.id());
                if (callback != null) callback.onResponse(m);
            } else if (m instanceof MutationReq r) {
                // Verb.MUTATION_REQ: stage MUTATION; the handler runs when the stage executes the task
                Channel.send(md, self, new MutationTask(r.id(), r.respondTo(), r.payload(), false));
            } else if (m instanceof MutationTask t) {
                MutationVerbHandler.doVerb(this, t);
            } else if (m instanceof ReadReq r) {
                // Verb.READ_REQ: stage READ. PORT: executed at receipt; the source may queue it behind later-received
                // tasks (NOTES.md, Processes: the orders not modelled and why they change no client-visible history)
                ReadCommandVerbHandler.doVerb(this, r);
            } else if (m instanceof ReadRepairReq r) {
                // Verb.READ_REPAIR_REQ: stage MUTATION. PORT: executed at receipt (same remark as for READ_REQ; the
                // node's own repair arrives on its self-channel, behind its pending mutation tasks under fifo)
                ReadRepairVerbHandler.doVerb(this, r);
            }
        }

        /** Mutation.apply -> Keyspace.apply -> the memtable merge: Rows.merge with Cells.reconcile per column. */
        void apply(Mutation mutation) {
            List<Cell> merged = new ArrayList<>();
            for (int column : ALL_COLUMNS) {
                merged.add(Cells.reconcile(partition.get(column), mutation.update().get(column)));
            }
            partition = List.copyOf(merged);
        }

        /** The node's loop: its client's requests one at a time (the interface), and every other message meanwhile. */
        void run() {
            while (true) {
                MpMessage req = Channel.recv(md);
                if (req instanceof Write w) {
                    StorageProxy.mutate(this, Mutation.write(w.value()));
                    Channel.send(md, setup.clients().get(w.client()), new Ack(w.client()));
                } else if (req instanceof ReadA r) {
                    List<Cell> row = StorageProxy.read(this, new ReadCommand(List.of(COL_A), false));
                    Channel.send(md, setup.clients().get(r.client()), new ValueA(r.client(), row.get(COL_A).value()));
                } else if (req instanceof ReadAB r) {
                    List<Cell> row = StorageProxy.read(this, new ReadCommand(List.of(COL_A, COL_B), false));
                    Channel.send(md, setup.clients().get(r.client()),
                            new ValueAB(r.client(), row.get(COL_A).value(), row.get(COL_B).value()));
                } else {
                    dispatch(req);
                }
            }
        }
    }

    /**
     * db/MutationVerbHandler.java: doVerb (l.45-66) -> applyMutation (l.69-73): apply, then respond with the empty
     * response; for the local task of performLocally (StorageProxy.LocalMutationRunnable) the handler's onResponse is
     * called directly.
     */
    static final class MutationVerbHandler {
        static void doVerb(Node node, MutationTask task) {
            // PORT: no expiry check (no timeouts), no forwarding headers (one data centre)
            node.apply(task.payload());
            if (task.local()) {
                RequestCallback callback = node.callbacks.remove(task.id());
                if (callback != null) callback.onResponse(new MutationRsp(task.id(), node.me));
            } else {
                Channel.send(node.md, task.respondTo(), new MutationRsp(task.id(), node.me));
            }
        }
    }

    /** db/ReadCommandVerbHandler.java: doVerb (l.50-128): execute the command locally, respond with data or digest. */
    static final class ReadCommandVerbHandler {
        static void doVerb(Node node, ReadReq message) {
            ReadCommand command = message.payload();
            // PORT: no token-range check, no monitoring time, no warnings
            List<Cell> row = command.executeLocally(node.partition);
            ReadResponse response = command.createResponse(row);
            Channel.send(node.md, message.respondTo(), new ReadRsp(message.id(), node.me, response));
        }
    }

    /** db/ReadRepairVerbHandler.java: doVerb (l.31-42) -> applyMutation: apply the repair mutation, respond. */
    static final class ReadRepairVerbHandler {
        static void doVerb(Node node, ReadRepairReq message) {
            node.apply(message.payload());
            Channel.send(node.md, message.respondTo(), new ReadRepairRsp(message.id(), node.me));
        }
    }

    // ------------------------------------------------------------------------------------------
    // locator/ReplicaPlans.java, locator/ReplicaPlan.java: the replicas contacted by a write, a read and a repair.
    // ------------------------------------------------------------------------------------------

    static final class ReplicaPlan {
        /** ReplicaPlan.ForWrite: the live replicas and the contacts; writeQuorum = QUORUM.blockForWrite = 2. */
        record ForWrite(List<Replica> contacts) {
            int writeQuorum() {
                return QUORUM;
            }

            Replica lookup(int from) {
                for (Replica r : contacts) if (r.index() == from) return r;
                throw new AssertionError("response from a replica not in the plan: " + from);
            }
        }

        /** ReplicaPlan.ForTokenRead: the candidates sorted by proximity and the contacts; readQuorum = QUORUM.blockFor = 2 (l.99). */
        record ForTokenRead(List<Replica> readCandidates, List<Replica> contacts) {
            int readQuorum() {
                return QUORUM;
            }

            Replica lookup(int from) {
                for (Replica r : contacts) if (r.index() == from) return r;
                throw new AssertionError("response from a replica not in the plan: " + from);
            }
        }
    }

    static final class ReplicaPlans {
        /** ReplicaPlans.forWrite with the writeNormal selector: every live replica of the token is a contact (all three). */
        static ReplicaPlan.ForWrite forWrite(Node node) {
            List<Replica> contacts = new ArrayList<>();
            for (int i = 0; i < REPLICAS; i++) contacts.add(new Replica(i));
            return new ReplicaPlan.ForWrite(List.copyOf(contacts));
        }

        /** The six proximity orders of three replicas (the snitch's sortedByProximity leaves the order open). */
        static final List<List<Integer>> PROXIMITY_ORDERS = List.of(
                List.of(0, 1, 2), List.of(0, 2, 1), List.of(1, 0, 2), List.of(1, 2, 0), List.of(2, 0, 1), List.of(2, 1, 0));

        /**
         * ReplicaPlans.forRead (l.700-709): the candidates are the live replicas sorted by the snitch's proximity
         * (ReplicaLayout.forTokenReadLiveSorted -> the snitch's sortedByProximity: DynamicEndpointSnitch orders by its
         * latency scores, SimpleSnitch keeps the ring order; the local replica is not necessarily first), the contacts
         * the first blockFor = 2 of them (contactForRead, l.660-680; no speculation). PORT: the order is a
         * nondeterministic choice among the six orders; the first contact gets the data request, the second the digest.
         */
        static ReplicaPlan.ForTokenRead forRead(Node node) {
            List<Integer> order = PROXIMITY_ORDERS.get(Nondet.chooseIndex(PROXIMITY_ORDERS.size()));
            List<Replica> candidates = new ArrayList<>();
            for (int i : order) candidates.add(new Replica(i));
            List<Replica> contacts = candidates.subList(0, Math.min(QUORUM, candidates.size()));
            return new ReplicaPlan.ForTokenRead(List.copyOf(candidates), List.copyOf(contacts));
        }

        /**
         * ReplicaPlans.forReadRepair (l.434-437) with the writeReadRepair selector (l.551-575): the live replicas
         * already contacted on read, plus enough others to reach blockForWrite = 2 (none: two were contacted).
         */
        static ReplicaPlan.ForWrite forReadRepair(ReplicaPlan.ForTokenRead readPlan) {
            List<Replica> contacts = new ArrayList<>(readPlan.contacts());
            int add = QUORUM - contacts.size();
            if (add > 0) {
                for (Replica r : readPlan.readCandidates()) {
                    if (add == 0) break;
                    if (!contacts.contains(r)) {
                        contacts.add(r);
                        add--;
                    }
                }
            }
            return new ReplicaPlan.ForWrite(List.copyOf(contacts));
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/StorageProxy.java: the write path and the read path of the coordinator.
    // ------------------------------------------------------------------------------------------

    static final class StorageProxy {
        /** StorageProxy.mutate (l.873-905) for one non-counter mutation at QUORUM. */
        static void mutate(Node node, Mutation mutation) {
            AbstractWriteResponseHandler responseHandler = performWrite(node, mutation);
            // PORT: maybeTryAdditionalReplicas (the cheap-quorum upgrade, l.894-898) waits on a timeout that never
            // fires here (AbstractWriteResponseHandler l.334-...): no additional replicas.
            // wait for writes (l.901-902)
            responseHandler.get();
        }

        /** StorageProxy.performWrite (l.1360-1385): the write plan, the response handler, the performer. */
        static AbstractWriteResponseHandler performWrite(Node node, Mutation mutation) {
            ReplicaPlan.ForWrite replicaPlan = ReplicaPlans.forWrite(node);
            AbstractWriteResponseHandler responseHandler = new WriteResponseHandler(node, replicaPlan);
            // standardWritePerformer -> sendToHintedReplicas on Stage.MUTATION
            sendToHintedReplicas(node, mutation, replicaPlan, responseHandler);
            return responseHandler;
        }

        /**
         * StorageProxy.sendToHintedReplicas (l.1464-1560): the local replica applies the mutation on the MUTATION
         * stage (performLocally), the other contacts of this data centre get a MUTATION_REQ with the callback.
         * PORT: every replica is alive and in the local data centre; no hints, no forwarding.
         */
        static void sendToHintedReplicas(Node node, Mutation mutation, ReplicaPlan.ForWrite plan,
                AbstractWriteResponseHandler responseHandler) {
            List<Replica> localDc = new ArrayList<>();
            boolean insertLocal = false;
            Replica localReplica = null;
            for (Replica destination : plan.contacts()) {
                if (destination.isSelf(node)) {
                    insertLocal = true;
                    localReplica = destination;
                } else {
                    localDc.add(destination);
                }
            }
            if (insertLocal) {
                // performLocally (l.1549, l.1646-1655): mutation.apply on the MUTATION stage of this node, then the
                // handler's onResponse (LocalMutationRunnable)
                performLocally(node, localReplica, mutation, responseHandler);
            }
            for (Replica destination : localDc) {
                // MessagingService.sendWriteWithCallback (l.1555)
                node.sendWithCallback(destination, responseHandler, id -> new MutationReq(id, node.self, mutation));
            }
        }

        /**
         * StorageProxy.performLocally: Stage.MUTATION.maybeExecuteImmediately(LocalMutationRunnable): the task applies the
         * mutation and calls the handler's onResponse. PORT: a MUTATION-stage task of this node (a message to itself,
         * executed at a later receive point: the source's stage may run it at once or queue it), registered under a fresh
         * id like a remote request so that the handler counts it.
         */
        static void performLocally(Node node, Replica local, Mutation mutation, AbstractWriteResponseHandler handler) {
            node.sendWithCallback(local, handler, id -> new MutationTask(id, node.self, mutation, true));
        }

        /**
         * StorageProxy.LocalReadRunnable (run on Stage.READ by maybeExecuteImmediately): execute the command locally and
         * hand the response to the callback (ReadCallback.response, l.219-224), no message. PORT: run inline at the point
         * the source submits it: the source's maybeExecuteImmediately runs it at once when a READ worker is free and
         * queues it otherwise; the port models only the first. A queued local read that runs after a mutation the node
         * receives later is not an order of this port; its reply is reached by the schedule in which that mutation
         * arrives before the read is submitted (NOTES.md, Processes).
         */
        static void localReadRunnable(Node node, ReadCommand command, ReadCallback handler) {
            List<Cell> row = command.executeLocally(node.partition);
            handler.response(command.createResponse(row));
        }

        /** StorageProxy.readOne -> read -> readRegular (l.1820-1966, l.1967-2019): one single-partition read at QUORUM. */
        static List<Cell> read(Node node, ReadCommand command) {
            // PORT: readRegular's metrics and the row-count check are omitted; fetchRows returns the one row.
            return fetchRows(node, command);
        }

        /**
         * StorageProxy.fetchRows (l.2068-2133) for one command: the read executor, the initial requests, the wait for
         * the consistency level, the read repair on a digest mismatch, the full-data wait, the result, and the
         * repair writes blocked on before the result is returned (concatAndBlockOnRepair, l.2029-2053).
         */
        static List<Cell> fetchRows(Node node, ReadCommand command) {
            AbstractReadExecutor read = AbstractReadExecutor.getReadExecutor(node, command);
            // sends a data request to the closest replica, and a digest request to the others
            read.executeAsync();
            // speculative retry (l.2097-2100): NeverSpeculatingReadExecutor does nothing
            read.maybeTryAdditionalReplicas();
            // wait for enough responses to meet the consistency level; on a digest mismatch begin the read repair
            read.awaitResponses();
            // read repair: additional data requests (l.2113-2116), time-based: nothing
            read.maybeSendAdditionalDataRequests();
            // read repair - block on full data responses
            read.awaitReadRepair();
            // the result and the repair of this read
            List<Cell> result = read.getResult();
            ReadRepair repairs = read.getReadRepair();
            // concatAndBlockOnRepair (l.2029-2053): the merged result is consumed, then, on close, the repair
            // mutations are sent (the merge listener's close) and waited for before the result is returned.
            // PORT: the merge was done eagerly in DataResolver.resolve (the repairs are already sent); the close
            // waits for them here.
            repairs.maybeSendAdditionalWrites();
            repairs.awaitWrites();
            return result;
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/AbstractWriteResponseHandler.java, service/WriteResponseHandler.java.
    // ------------------------------------------------------------------------------------------

    abstract static class AbstractWriteResponseHandler implements RequestCallback {
        final Node node;
        final ReplicaPlan.ForWrite replicaPlan;
        boolean signalled = false;

        AbstractWriteResponseHandler(Node node, ReplicaPlan.ForWrite replicaPlan) {
            this.node = node;
            this.replicaPlan = replicaPlan;
        }

        /**
         * AbstractWriteResponseHandler.get (l.113-145): wait on the condition. PORT: the timeout never fires. The
         * registrations of the outstanding requests stay (RequestCallbacks keeps a callback until its response or its
         * expiry): the third acknowledgement still reaches this handler (WriteResponseHandler l.55-58, responses -1).
         */
        void get() {
            node.awaitUntil(() -> signalled);
        }

        /** AbstractWriteResponseHandler.blockFor (l.220-224): the write quorum of the plan. */
        int blockFor() {
            return replicaPlan.writeQuorum();
        }

        void signal() {
            signalled = true;
        }
    }

    /** WriteResponseHandler: responses counts down from blockFor; at 0 the waiter is signalled (l.40-61). */
    static final class WriteResponseHandler extends AbstractWriteResponseHandler {
        int responses;

        WriteResponseHandler(Node node, ReplicaPlan.ForWrite replicaPlan) {
            super(node, replicaPlan);
            responses = blockFor();
        }

        @Override
        public void onResponse(MpMessage m) {
            if (--responses == 0) signal();
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/AbstractReadExecutor.java (NeverSpeculatingReadExecutor).
    // ------------------------------------------------------------------------------------------

    abstract static class AbstractReadExecutor {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        final int initialDataRequestCount;
        final ReadRepair readRepair;
        final DigestResolver digestResolver;
        final ReadCallback handler;
        private List<Cell> result = null;

        AbstractReadExecutor(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, int initialDataRequestCount) {
            this.node = node;
            this.command = command;
            this.replicaPlan = replicaPlan;
            this.initialDataRequestCount = initialDataRequestCount;
            // ReadRepair.create (l.86): the strategy of the table
            this.readRepair = ReadRepair.create(node, command, replicaPlan);
            this.digestResolver = new DigestResolver(node, command, replicaPlan, readRepair);
            this.handler = new ReadCallback(node, digestResolver, command, replicaPlan);
            // PORT: the digest version negotiation (l.88-93) is omitted
        }

        /**
         * AbstractReadExecutor.getReadExecutor (l.190-212): the read plan, then the executor by the table's
         * speculative retry policy. PORT: no speculative retries (the interface): NeverSpeculatingReadExecutor.
         */
        static AbstractReadExecutor getReadExecutor(Node node, ReadCommand command) {
            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(node);
            return new NeverSpeculatingReadExecutor(node, command, replicaPlan);
        }

        ReadRepair getReadRepair() {
            return readRepair;
        }

        /** makeFullDataRequests (l.116-120) / makeDigestRequests (l.127-132) -> makeRequests (l.134-170). */
        void makeFullDataRequests(List<Replica> replicas) {
            makeRequests(command, replicas);
        }

        void makeDigestRequests(List<Replica> replicas) {
            makeRequests(command.copyAsDigestQuery(), replicas);
        }

        private void makeRequests(ReadCommand readCommand, List<Replica> replicas) {
            boolean hasLocalEndpoint = false;
            for (Replica replica : replicas) {
                if (replica.isSelf(node)) {
                    hasLocalEndpoint = true;
                    continue;
                }
                node.sendWithCallback(replica, handler, id -> new ReadReq(id, node.self, readCommand));
            }
            // We delay the local (potentially blocking) read till the end to avoid stalling remote requests (l.163-169):
            // Stage.READ.maybeExecuteImmediately(new LocalReadRunnable(readCommand, handler))
            if (hasLocalEndpoint) {
                StorageProxy.localReadRunnable(node, readCommand, handler);
            }
        }

        /** Perform additional requests if it looks like the original will time out (l.172-176). */
        abstract void maybeTryAdditionalReplicas();

        /** executeAsync (l.178-185): data requests to the first initialDataRequestCount contacts, digest requests to the rest. */
        void executeAsync() {
            List<Replica> selected = replicaPlan.contacts();
            List<Replica> fullDataRequests = selected.subList(0, Math.min(initialDataRequestCount, selected.size()));
            makeFullDataRequests(fullDataRequests);
            // PORT: no transient replicas
            List<Replica> digests = new ArrayList<>();
            for (Replica r : selected) if (!fullDataRequests.contains(r)) digests.add(r);
            makeDigestRequests(digests);
        }

        void setResult(List<Cell> result) {
            this.result = result;
        }

        /** awaitResponses (l.396-440): wait for the CL, then return the data or start the read repair. PORT: no timeout. */
        void awaitResponses() {
            handler.awaitResults();
            if (!digestResolver.isDataPresent()) throw new AssertionError("awaitResults returned with no data present.");
            // return immediately, or begin a read repair
            if (digestResolver.responsesMatch()) {
                setResult(digestResolver.getData());
            } else {
                readRepair.startRepair(digestResolver, this::setResult);
            }
        }

        /** awaitReadRepair (l.442-456). PORT: no timeout. */
        void awaitReadRepair() {
            readRepair.awaitReads();
        }

        boolean isDone() {
            return result != null;
        }

        /** maybeSendAdditionalDataRequests (l.465-471). */
        void maybeSendAdditionalDataRequests() {
            if (isDone()) return;
            readRepair.maybeSendAdditionalReads();
        }

        /** getResult (l.473-477). */
        List<Cell> getResult() {
            if (result == null) throw new IllegalStateException("Result must be set first");
            return result;
        }
    }

    /** NeverSpeculatingReadExecutor (l.258-283): one initial data request, never any additional replicas. */
    static final class NeverSpeculatingReadExecutor extends AbstractReadExecutor {
        NeverSpeculatingReadExecutor(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan, 1);
        }

        @Override
        void maybeTryAdditionalReplicas() {
            // PORT: shouldSpeculateAndMaybeWait is time-based; only a metric would be updated
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/ResponseResolver.java, DigestResolver.java, DataResolver.java, ReadCallback.java.
    // ------------------------------------------------------------------------------------------

    /** Message<ReadResponse> as the resolvers see it: the sender and the payload. */
    record ResponseMessage(int from, ReadResponse payload) {}

    abstract static class ResponseResolver {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        /** ResponseResolver.responses: the accumulator (Accumulator), in arrival order. */
        final List<ResponseMessage> responses = new ArrayList<>();

        ResponseResolver(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            this.node = node;
            this.command = command;
            this.replicaPlan = replicaPlan;
        }

        /** ResponseResolver.preprocess (l.60-68): accumulate the response. */
        void preprocess(ResponseMessage message) {
            responses.add(message);
        }

        abstract boolean isDataPresent();
    }

    /** DigestResolver (l.56-138). */
    static final class DigestResolver extends ResponseResolver {
        final ReadRepair readRepair;
        private ResponseMessage dataResponse = null;

        DigestResolver(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, ReadRepair readRepair) {
            super(node, command, replicaPlan);
            this.readRepair = readRepair;
        }

        /** preprocess (l.56-62): the first data response from a full replica is the data response. */
        @Override
        void preprocess(ResponseMessage message) {
            super.preprocess(message);
            if (dataResponse == null && !message.payload().isDigestResponse()) dataResponse = message;
        }

        /** getData (l.77-101): the data response's partition. PORT: no transient replicas, so no reconciliation here. */
        List<Cell> getData() {
            return dataResponse.payload().data();
        }

        /** responsesMatch (l.105-138): every digest equals the first one; one response always matches. */
        boolean responsesMatch() {
            String digest = null;
            List<ResponseMessage> snapshot = List.copyOf(responses);
            if (snapshot.isEmpty()) throw new AssertionError("Attempted response match comparison while no responses have been received.");
            if (snapshot.size() == 1) return true;
            for (ResponseMessage message : snapshot) {
                String newDigest = message.payload().digest();
                if (digest == null) digest = newDigest;
                else if (!digest.equals(newDigest)) return false;
            }
            return true;
        }

        @Override
        boolean isDataPresent() {
            return dataResponse != null;
        }
    }

    /** DataResolver (l.94-135, 212-225, 269-299): the merge of the data responses with the read repair's merge listener. */
    static final class DataResolver extends ResponseResolver {
        final ReadRepair readRepair;

        DataResolver(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, ReadRepair readRepair) {
            super(node, command, replicaPlan);
            this.readRepair = readRepair;
        }

        @Override
        boolean isDataPresent() {
            return !responses.isEmpty();
        }

        /**
         * resolve (l.94-135) -> resolveWithReadRepair (l.212-225) -> resolveInternal (l.269-299): the responses
         * present now are merged (UnfilteredPartitionIterators.merge with the listener); a merge listener is
         * installed when more than one replica answered and the read repair is not NoopReadRepair.
         * PORT: the merge is computed eagerly, row by row is one row; the listener's close (which builds and sends
         * the repair mutations) runs at the end of the merge, where the source runs it when the merged iterator
         * is closed, before the result is returned to the client (fetchRows / concatAndBlockOnRepair).
         */
        List<Cell> resolve() {
            List<ResponseMessage> messages = List.copyOf(responses);
            for (ResponseMessage m : messages) if (m.payload().isDigestResponse()) throw new AssertionError("digest response in a data resolver");
            // the replicas that answered, in response order (readCandidates.select(from))
            List<Replica> replicas = new ArrayList<>();
            for (ResponseMessage m : messages) replicas.add(replicaPlan.lookup(m.from()));
            ReplicaPlan.ForTokenRead sources = new ReplicaPlan.ForTokenRead(replicaPlan.readCandidates(), List.copyOf(replicas));
            // ResolveContext.needsReadRepair: replicas.size() > 1
            RowIteratorMergeListener listener = null;
            if (replicas.size() > 1 && !(readRepair instanceof NoopReadRepair)) {
                listener = readRepair.getMergeListener(sources);
            }
            // resolveInternal: the merge of the versions, cell by cell (Rows.merge -> Cells.reconcile), reported to
            // the listener (RowDiffListener.onCell per source), then the listener's close.
            List<List<Cell>> versions = new ArrayList<>();
            for (ResponseMessage m : messages) versions.add(m.payload().data());
            List<Cell> merged = new ArrayList<>();
            for (int column : ALL_COLUMNS) {
                Cell m = Cell.ABSENT;
                for (List<Cell> version : versions) m = Cells.reconcile(m, version.get(column));
                merged.add(m);
            }
            List<Cell> mergedRow = List.copyOf(merged);
            if (listener != null) {
                listener.onMergedRows(mergedRow, versions);
                listener.close();
            }
            return mergedRow;
        }
    }

    /** ReadCallback (l.126-151, 179-202): the response correlation and the count towards the consistency level. */
    static final class ReadCallback implements RequestCallback {
        final Node node;
        final ResponseResolver resolver;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        final int blockFor;
        boolean signalled = false;

        ReadCallback(Node node, ResponseResolver resolver, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            this.node = node;
            this.resolver = resolver;
            this.command = command;
            this.replicaPlan = replicaPlan;
            // l.83: blockFor = replicaPlan.readQuorum()
            this.blockFor = replicaPlan.readQuorum();
        }

        /** awaitResults (l.126-151): wait until signalled. PORT: no timeout, no failures. */
        void awaitResults() {
            node.awaitUntil(() -> signalled);
        }

        /** response (l.219-224): a local response (no message): an internal READ_RSP from this node. */
        void response(ReadResponse result) {
            onResponse(new ReadRsp(-1, node.me, result));
        }

        /**
         * onResponse (l.179-202): preprocess, then signal once the data is present and blockFor responses were
         * accumulated (CASSANDRA-16807).
         */
        @Override
        public void onResponse(MpMessage m) {
            ReadRsp message = (ReadRsp) m;
            replicaPlan.lookup(message.from()); // assertWaitingFor
            resolver.preprocess(new ResponseMessage(message.from(), message.payload()));
            if (resolver.isDataPresent() && resolver.responses.size() >= blockFor) signalled = true;
        }
    }

    // ------------------------------------------------------------------------------------------
    // service/reads/repair: ReadRepair, AbstractReadRepair, BlockingReadRepair, ReadOnlyReadRepair, NoopReadRepair,
    // BlockingPartitionRepair, the merge listeners.
    // ------------------------------------------------------------------------------------------

    interface ReadRepair {
        /** ReadRepair.create (ReadRepairStrategy.create): BLOCKING -> BlockingReadRepair, NONE -> ReadOnlyReadRepair. */
        static ReadRepair create(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            return switch (node.strategy) {
                case BLOCKING -> new BlockingReadRepair(node, command, replicaPlan);
                case NONE -> new ReadOnlyReadRepair(node, command, replicaPlan);
            };
        }

        RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicaPlan);

        void startRepair(DigestResolver digestResolver, Consumer<List<Cell>> resultConsumer);

        void awaitReads();

        void maybeSendAdditionalReads();

        void maybeSendAdditionalWrites();

        void awaitWrites();

        void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan);
    }

    /** AbstractReadRepair (l.93-203). */
    abstract static class AbstractReadRepair implements ReadRepair {
        final Node node;
        final ReadCommand command;
        final ReplicaPlan.ForTokenRead replicaPlan;
        /** DigestRepair: the data resolver, its callback and the result consumer of a started repair. */
        record DigestRepair(DataResolver dataResolver, ReadCallback readCallback, Consumer<List<Cell>> resultConsumer) {}
        private DigestRepair digestRepair = null;

        AbstractReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            this.node = node;
            this.command = command;
            this.replicaPlan = replicaPlan;
        }

        /** sendReadCommand (l.96-120): the local replica reads on Stage.READ, the others get the command with the callback. */
        void sendReadCommand(Replica to, ReadCallback readCallback) {
            if (to.isSelf(node)) {
                // Stage.READ.maybeExecuteImmediately(new StorageProxy.LocalReadRunnable(command, readCallback))
                StorageProxy.localReadRunnable(node, command, readCallback);
                return;
            }
            // PORT: no transient replicas, no tracing
            node.sendWithCallback(to, readCallback, id -> new ReadReq(id, node.self, command));
        }

        /** startRepair (l.126-153): a full data read to every contact, resolved by a DataResolver with this read repair. */
        @Override
        public void startRepair(DigestResolver digestResolver, Consumer<List<Cell>> resultConsumer) {
            // PORT: no repaired-data tracking
            DataResolver resolver = new DataResolver(node, command, replicaPlan, this);
            ReadCallback readCallback = new ReadCallback(node, resolver, command, replicaPlan);
            digestRepair = new DigestRepair(resolver, readCallback, resultConsumer);
            for (Replica replica : replicaPlan.contacts()) {
                sendReadCommand(replica, readCallback);
            }
        }

        /** awaitReads (l.156-173): wait for the full data reads, then resolve. PORT: no timeout. */
        @Override
        public void awaitReads() {
            DigestRepair repair = digestRepair;
            if (repair == null) return;
            repair.readCallback().awaitResults();
            repair.resultConsumer().accept(digestRepair.dataResolver().resolve());
        }

        /** maybeSendAdditionalReads (l.185-203): PORT: time-based (awaitUntil the sampled latency), never fires. */
        @Override
        public void maybeSendAdditionalReads() {
            if (digestRepair == null) return;
        }
    }

    /** BlockingReadRepair (l.62-116). */
    static final class BlockingReadRepair extends AbstractReadRepair {
        final List<BlockingPartitionRepair> repairs = new ArrayList<>();

        BlockingReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan);
        }

        /** getMergeListener (l.62-65): PartitionIteratorMergeListener -> RowIteratorMergeListener for the one partition. */
        @Override
        public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead sources) {
            return new RowIteratorMergeListener(sources, command, this);
        }

        /** maybeSendAdditionalWrites (l.74-80): PORT: BlockingPartitionRepair.maybeSendAdditionalWrites waits on a timeout that never fires. */
        @Override
        public void maybeSendAdditionalWrites() {
            for (BlockingPartitionRepair repair : repairs) repair.maybeSendAdditionalWrites();
        }

        /** awaitWrites (l.83-108): wait for every partition repair. PORT: no timeout. */
        @Override
        public void awaitWrites() {
            for (BlockingPartitionRepair repair : repairs) repair.awaitRepairsUntil();
        }

        /** repairPartition (l.112-116). */
        @Override
        public void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan) {
            BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(node, mutations, writePlan);
            blockingRepair.sendInitialRepairs();
            repairs.add(blockingRepair);
        }
    }

    /** ReadOnlyReadRepair (NONE, l.34-73): reconcile with no merge listener, never repair, never wait for writes. */
    static final class ReadOnlyReadRepair extends AbstractReadRepair {
        ReadOnlyReadRepair(Node node, ReadCommand command, ReplicaPlan.ForTokenRead replicaPlan) {
            super(node, command, replicaPlan);
        }

        @Override
        public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead sources) {
            return RowIteratorMergeListener.NOOP;
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

    /** NoopReadRepair (a testing stub): only its identity matters here (DataResolver.resolveWithReadRepair). */
    static final class NoopReadRepair implements ReadRepair {
        @Override public RowIteratorMergeListener getMergeListener(ReplicaPlan.ForTokenRead replicaPlan) { return RowIteratorMergeListener.NOOP; }
        @Override public void startRepair(DigestResolver digestResolver, Consumer<List<Cell>> resultConsumer) { resultConsumer.accept(digestResolver.getData()); }
        @Override public void awaitReads() {}
        @Override public void maybeSendAdditionalReads() {}
        @Override public void maybeSendAdditionalWrites() {}
        @Override public void awaitWrites() {}
        @Override public void repairPartition(Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan) {}
    }

    /** BlockingPartitionRepair (l.69-201): the repair mutations of one partition, sent and acknowledged. */
    static final class BlockingPartitionRepair implements RequestCallback {
        final Node node;
        final ReplicaPlan.ForWrite writePlan;
        /** pendingRepairs: PORT: a LinkedHashMap (the source's ConcurrentHashMap has no fixed order) in write-plan contact order. */
        final Map<Replica, Mutation> pendingRepairs;
        /** the latch's count. */
        int latch;

        BlockingPartitionRepair(Node node, Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan) {
            this.node = node;
            this.pendingRepairs = new LinkedHashMap<>(repairs);
            this.writePlan = writePlan;
            for (Replica r : repairs.keySet()) {
                if (!writePlan.contacts().contains(r)) throw new IllegalStateException("All repair targets should be part of contacts of read repair write plan.");
            }
            int blockFor = writePlan.writeQuorum();
            // here we remove empty repair mutations from the block for total, since we're not sending them mutations
            for (Replica participant : writePlan.contacts()) {
                if (!repairs.containsKey(participant)) blockFor--;
            }
            // clamped at 0 (l.95-100)
            latch = Math.max(blockFor, 0);
        }

        int blockFor() {
            return writePlan.writeQuorum();
        }

        /** ack (l.112-117). */
        void ack(int from) {
            pendingRepairs.remove(writePlan.lookup(from));
            latch--;
        }

        @Override
        public void onResponse(MpMessage m) {
            ack(((ReadRepairRsp) m).from());
        }

        /** sendInitialRepairs (l.140-158): READ_REPAIR_REQ with this callback to every repair's replica. */
        void sendInitialRepairs() {
            for (Map.Entry<Replica, Mutation> entry : List.copyOf(pendingRepairs.entrySet())) {
                Replica destination = entry.getKey();
                Mutation mutation = entry.getValue();
                // sendRR -> MessagingService.sendWithCallback(READ_REPAIR_REQ, destination, this); a repair to the
                // local replica is a message to its own mutation stage as well
                node.sendWithCallback(destination, this, id -> new ReadRepairReq(id, node.self, mutation));
            }
        }

        /** awaitRepairsUntil (l.172-184): wait on the latch. PORT: no deadline. */
        void awaitRepairsUntil() {
            node.awaitUntil(() -> latch <= 0);
        }

        /** maybeSendAdditionalWrites (l.198-...): PORT: awaitRepairsUntil(timeout) with a timeout that never fires returns at once. */
        void maybeSendAdditionalWrites() {}
    }

    /**
     * RowIteratorMergeListener (l.85-115, 138-154, 180-215, 367-400) for the one row of the one partition, with
     * PartitionIteratorMergeListener (l.47-49) reduced to its construction: per source, the queried cells whose
     * merged version differs from the source's are collected into the source's repair update; close builds one
     * mutation per write-plan contact that has an update and hands them to readRepair.repairPartition.
     * PORT: NOOP is UnfilteredPartitionIterators.MergeListener.NOOP (ReadOnlyReadRepair): it records nothing.
     */
    static final class RowIteratorMergeListener {
        static final RowIteratorMergeListener NOOP = new RowIteratorMergeListener();

        final ReplicaPlan.ForTokenRead readPlan;
        final ReplicaPlan.ForWrite writePlan;
        final ReadCommand command;
        final ReadRepair readRepair;
        /** repairs[i]: the update for source i (a PartitionUpdate.Builder), or null when source i needs none. */
        final List<Map<Integer, Cell>> repairs;
        final List<Boolean> writeBackTo;
        final boolean buildFullDiff;
        final Map<Integer, Cell> fullDiff;

        private RowIteratorMergeListener() {
            this.readPlan = null;
            this.writePlan = null;
            this.command = null;
            this.readRepair = null;
            this.repairs = List.of();
            this.writeBackTo = List.of();
            this.buildFullDiff = false;
            this.fullDiff = Map.of();
        }

        RowIteratorMergeListener(ReplicaPlan.ForTokenRead readPlan, ReadCommand command, ReadRepair readRepair) {
            this.readPlan = readPlan;
            this.writePlan = ReplicaPlans.forReadRepair(readPlan);
            this.command = command;
            this.readRepair = readRepair;
            int size = readPlan.contacts().size();
            List<Boolean> wb = new ArrayList<>();
            for (Replica replica : readPlan.contacts()) wb.add(writePlan.contacts().contains(replica));
            this.writeBackTo = wb;
            boolean full = false;
            for (Replica r : writePlan.contacts()) if (!readPlan.contacts().contains(r)) full = true;
            this.buildFullDiff = full;
            this.repairs = new ArrayList<>();
            for (int i = 0; i < size; i++) repairs.add(null);
            this.fullDiff = new LinkedHashMap<>();
        }

        /** RowDiffListener.onCell (l.138-142): a merged cell that differs from the source's and is queried is repaired at the source. */
        void onCell(int i, Cell merged, Cell original, int column) {
            if (!merged.isAbsent() && !merged.equals(original) && isQueried(column)) {
                applyToPartition(i, column, merged);
            }
        }

        /** isQueried (l.144-154): fetched cells that the user did not query are not repaired. */
        private boolean isQueried(int column) {
            return command.fetchedColumnIsQueried(column);
        }

        /** applyToPartition (l.180-193). */
        private void applyToPartition(int i, int column, Cell merged) {
            if (writeBackTo.get(i)) {
                if (repairs.get(i) == null) repairs.set(i, new LinkedHashMap<>());
                repairs.get(i).put(column, merged);
            }
            if (buildFullDiff) fullDiff.put(column, merged);
        }

        /** onMergedRows (l.207-215) -> Rows.diff: the diff listener's onCell for every source and column. */
        void onMergedRows(List<Cell> merged, List<List<Cell>> versions) {
            if (this == NOOP) return;
            boolean empty = true;
            for (Cell c : merged) if (!c.isAbsent()) empty = false;
            if (empty) return;
            for (int i = 0; i < versions.size(); i++) {
                for (int column : ALL_COLUMNS) {
                    onCell(i, merged.get(column), versions.get(i).get(column), column);
                }
            }
        }

        /** close (l.367-400): one mutation per write-plan contact with a repair; then readRepair.repairPartition. */
        void close() {
            if (this == NOOP) return;
            boolean hasRepairs = false;
            for (Map<Integer, Cell> r : repairs) if (r != null) hasRepairs = true;
            if (!hasRepairs && !(buildFullDiff && !fullDiff.isEmpty())) return;
            Map<Replica, Mutation> mutations = new LinkedHashMap<>();
            for (Replica replica : writePlan.contacts()) {
                Map<Integer, Cell> update = null;
                int i = readPlan.contacts().indexOf(replica);
                if (i < 0) update = buildFullDiff && !fullDiff.isEmpty() ? fullDiff : null;
                else if (repairs.get(i) != null) update = repairs.get(i);
                // BlockingReadRepairs.createRepairMutation: null for a null update (l.49-52); PORT: never oversized
                if (update == null) continue;
                List<Cell> cells = new ArrayList<>();
                for (int column : ALL_COLUMNS) cells.add(update.getOrDefault(column, Cell.ABSENT));
                mutations.put(replica, new Mutation(List.copyOf(cells)));
            }
            readRepair.repairPartition(mutations, writePlan);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Program parameters and entry (MpProgram).
    // ------------------------------------------------------------------------------------------

    private final int q;
    private final ReadRepairStrategy strategy;
    private final CommunicationModel md;
    private final String modelName;

    /**
     * Constructed from the launcher's argument string {@code impl,<q>,<blocking|none>,<fifo|bag>}.
     *
     * @param args the launcher's comma-separated arguments
     */
    public CassandraReadRepair(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<blocking|none>,<fifo|bag>");
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be >= 1, was " + q);
        this.strategy = switch (args[2]) {
            case "blocking" -> ReadRepairStrategy.BLOCKING;
            case "none" -> ReadRepairStrategy.NONE;
            default -> throw new IllegalArgumentException("blocking or none expected, got " + args[2]);
        };
        this.modelName = args[3];
        this.md = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("fifo or bag expected, got " + args[3]);
        };
    }

    @Override
    public String programName() {
        return "cassandra-rr-port";
    }

    @Override
    public String size() {
        return "q=" + q + "," + strategy.name().toLowerCase() + "," + modelName;
    }

    @Override
    public CommunicationModel model() {
        return md;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) roles.add("c" + i);
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return rolesInSpawnOrder();
    }

    /**
     * The entry: main spawns the nodes n0, n1, n2 (invisible daemons, never terminating), the clients c0, c1 (visible,
     * terminating) and init (invisible), which sends each node its one setup message; main then awaits the clients.
     * Main sends and receives nothing. (A daemon has no role name in this API; the nodes are n0, n1, n2 in spawn order.)
     */
    @Override
    public void run() {
        final CommunicationModel model = md;
        final ReadRepairStrategy rr = strategy;
        final int ops = q;
        List<Tid> nodes = new ArrayList<>();
        for (int i = 0; i < REPLICAS; i++) {
            final int me = i;
            nodes.add(MpProcess.spawnDaemon(() -> {
                Tid self = MpProcess.self();
                Setup setup = Channel.recv(model, Setup.class);
                new Node(model, me, self, setup, rr).run();
            }).tid());
        }
        List<MpProcess.Handle> clientHandles = new ArrayList<>();
        List<Tid> clients = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            final int idx = i;
            final Tid node = nodes.get(i);
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> client(model, idx, ops, node));
            clientHandles.add(h);
            clients.add(h.tid());
        }
        final Setup setup = new Setup(List.copyOf(nodes), List.copyOf(clients));
        final List<Tid> targets = List.copyOf(nodes);
        MpProcess.spawnDaemon(() -> {
            for (Tid t : targets) Channel.send(model, t, setup);
        });
        for (MpProcess.Handle h : clientHandles) h.await();
    }
}
