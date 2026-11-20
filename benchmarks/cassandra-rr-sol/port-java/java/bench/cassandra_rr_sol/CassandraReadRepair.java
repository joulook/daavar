package bench.cassandra_rr_sol;

import java.util.List;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Cassandra 4.1's non-speculating quorum paths for the fixed INTERFACE.md model. */
public final class CassandraReadRepair implements MpProgram {
    private final int q;
    private final boolean blocking;
    private final CommunicationModel communication;

    public CassandraReadRepair(String[] args) {
        if (args.length != 4 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<q>,<blocking|none>,<fifo|bag>");
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be positive");
        blocking = switch (args[2]) {
            case "blocking" -> true;
            case "none" -> false;
            default -> throw new IllegalArgumentException("unknown read repair strategy");
        };
        communication = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown delivery model");
        };
    }

    public String programName() { return "cassandra_rr"; }
    public String size() { return q + "," + (blocking ? "blocking" : "none"); }
    public CommunicationModel model() { return communication; }
    public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    public List<String> visibleRoles() { return List.of("c0", "c1"); }

    @Tag("Write") public record Write(int client, long value) implements MpMessage {}
    @Tag("ReadA") public record ReadA(int client) implements MpMessage {}
    @Tag("ReadAB") public record ReadAB(int client) implements MpMessage {}
    @Tag("Ack") public record Ack(int client) implements MpMessage {}
    @Tag("ValueA") public record ValueA(int client, long a) implements MpMessage {}
    @Tag("ValueAB") public record ValueAB(int client, long a, long b) implements MpMessage {}

    @Tag("Cell") public record Cell(long value, long timestamp) implements MpMessage {}
    @Tag("Data") public record Data(Cell a, Cell b) implements MpMessage {}
    @Tag("MutationReq") public record MutationReq(Tid coordinator, long id, int mask, Data data)
            implements MpMessage {}
    @Tag("MutationRsp") public record MutationRsp(long id, int replica) implements MpMessage {}
    // phase 0 is the digest round; phase 1 is the fresh full-data repair round.
    @Tag("ReadReq") public record ReadReq(Tid coordinator, long id, int phase, int mask, boolean digest)
            implements MpMessage {}
    @Tag("DataRsp") public record DataRsp(long id, int replica, int phase, Data data) implements MpMessage {}
    @Tag("DigestRsp") public record DigestRsp(long id, int replica, String digest) implements MpMessage {}
    @Tag("RepairReq") public record RepairReq(Tid coordinator, long id, int mask, Data data)
            implements MpMessage {}
    @Tag("RepairRsp") public record RepairRsp(long id, int replica) implements MpMessage {}

    private static final Cell ZERO = new Cell(0, 0);
    private static Cell reconcile(Cell left, Cell right) {
        return left.timestamp() >= right.timestamp() ? left : right;
    }
    private static String digest(Data data) {
        // Injective tuple encoding, including timestamps; unrequested cells are canonical zero.
        return data.a().value() + ":" + data.a().timestamp() + ":"
                + data.b().value() + ":" + data.b().timestamp();
    }
    private void send(Tid to, MpMessage message) { Channel.send(communication, to, message); }

    @Override public void run() {
        Node[] nodes = { new Node(0), new Node(1), new Node(2) };
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < 3; i++) threads[i] = new MpThread(nodes[i]::run, "n" + i);
        List<Tid> replicas = List.of(Tid.of(threads[0].getTaskId(), "n0"),
                Tid.of(threads[1].getTaskId(), "n1"), Tid.of(threads[2].getTaskId(), "n2"));
        for (int i = 0; i < 2; i++) {
            final int client = i;
            threads[3 + i] = new MpThread(() -> client(client, replicas.get(client)), "c" + i);
        }
        List<Tid> clients = List.of(Tid.of(threads[3].getTaskId(), "c0"),
                Tid.of(threads[4].getTaskId(), "c1"));
        // Address-only initialization, completed before any process starts. No protocol state is shared.
        for (Node node : nodes) { node.replicas = replicas; node.clients = clients; }
        for (MpThread thread : threads) thread.start();
        try {
            threads[3].join1();
            threads[4].join1();
            for (int i = 0; i < 3; i++) threads[i].join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void client(int i, Tid coordinator) {
        for (int j = 0; j < q; j++) {
            int kind = (i + j) % 3;
            switch (kind) {
                case 0 -> send(coordinator, new Write(i, 1L + i + 2L * j));
                case 1 -> send(coordinator, new ReadA(i));
                case 2 -> send(coordinator, new ReadAB(i));
                default -> throw new AssertionError();
            }
            MpMessage reply = Channel.recv(communication);
            if (!(switch (kind) {
                case 0 -> reply instanceof Ack a && a.client() == i;
                case 1 -> reply instanceof ValueA a && a.client() == i;
                case 2 -> reply instanceof ValueAB a && a.client() == i;
                default -> false;
            })) throw new AssertionError("unexpected client reply " + reply);
        }
    }

    private enum Stage { IDLE, WRITE, DIGEST, FULL_DATA, REPAIR_ACK }
    private final class Node {
        private final int index;
        private List<Tid> replicas;
        private List<Tid> clients;
        private Cell a = ZERO, b = ZERO;
        private Stage stage = Stage.IDLE;
        private long id;
        private int client, mask, other, responses, waiting;
        private Data initialData, result;
        private String remoteDigest;
        private final Data[] full = new Data[3];
        private final boolean[] acked = new boolean[3];
        private final boolean[] repairTarget = new boolean[3];

        Node(int index) { this.index = index; }
        private Tid self() { return replicas.get(index); }

        void run() {
            while (true) {
                MpMessage message = Channel.recv(communication);
                if (message instanceof Write m) performWrite(m);
                else if (message instanceof ReadA m) executeRead(m.client(), 1);
                else if (message instanceof ReadAB m) executeRead(m.client(), 3);
                else if (message instanceof MutationReq m) mutationVerb(m);
                else if (message instanceof ReadReq m) readVerb(m);
                else if (message instanceof RepairReq m) repairVerb(m);
                else if (message instanceof MutationRsp m) onWriteResponse(m);
                else if (message instanceof DataRsp m) onDataResponse(m);
                else if (message instanceof DigestRsp m) onDigestResponse(m);
                else if (message instanceof RepairRsp m) onRepairResponse(m);
                else throw new AssertionError("unknown message " + message);
            }
        }

        private void begin(int who, int selectedMask, Stage next) {
            if (stage != Stage.IDLE || who != index) throw new AssertionError("overlapping/wrong client");
            ++id;
            client = who;
            mask = selectedMask;
            stage = next;
            responses = 0;
            waiting = 0;
            initialData = null;
            remoteDigest = null;
            for (int i = 0; i < 3; i++) {
                full[i] = null;
                acked[i] = false;
                repairTarget[i] = false;
            }
        }

        private void performWrite(Write request) {
            begin(request.client(), 3, Stage.WRITE);
            Cell cell = new Cell(request.value(), request.value());
            Data update = new Data(cell, cell);
            for (Tid replica : replicas) send(replica, new MutationReq(self(), id, 3, update));
        }
        private void onWriteResponse(MutationRsp response) {
            if (stage != Stage.WRITE || response.id() != id || acked[response.replica()]) return;
            acked[response.replica()] = true;
            if (++responses == 2) {
                stage = Stage.IDLE;
                send(clients.get(client), new Ack(client));
            }
        }

        private void executeRead(int who, int selectedMask) {
            begin(who, selectedMask, Stage.DIGEST);
            other = Nondet.choose(List.of((index + 1) % 3, (index + 2) % 3));
            send(self(), new ReadReq(self(), id, 0, mask, false));
            send(replicas.get(other), new ReadReq(self(), id, 0, mask, true));
        }
        private void onDataResponse(DataRsp response) {
            if (response.id() != id) return;
            if (stage == Stage.DIGEST && response.phase() == 0 && response.replica() == index) {
                initialData = response.data();
                awaitInitialResponses();
            } else if (stage == Stage.FULL_DATA && response.phase() == 1
                    && (response.replica() == index || response.replica() == other)) {
                if (full[response.replica()] != null) return;
                full[response.replica()] = response.data();
                if (++responses == 2) resolve();
            }
        }
        private void onDigestResponse(DigestRsp response) {
            if (stage != Stage.DIGEST || response.id() != id || response.replica() != other) return;
            remoteDigest = response.digest();
            awaitInitialResponses();
        }
        private void awaitInitialResponses() {
            if (initialData == null || remoteDigest == null) return;
            if (digest(initialData).equals(remoteDigest)) {
                result = initialData;
                replyRead();
            } else startRepair();
        }
        private void startRepair() {
            stage = Stage.FULL_DATA;
            responses = 0;
            // Source explicitly re-reads every contact, including the original data source.
            send(self(), new ReadReq(self(), id, 1, mask, false));
            send(replicas.get(other), new ReadReq(self(), id, 1, mask, false));
        }
        private void resolve() {
            Data local = full[index], remote = full[other];
            result = new Data(reconcile(local.a(), remote.a()), reconcile(local.b(), remote.b()));
            if (!blocking) { replyRead(); return; }
            repairPartition();
        }
        private int diff(Data original) {
            int changed = 0;
            if ((mask & 1) != 0 && !result.a().equals(original.a())) changed |= 1;
            if ((mask & 2) != 0 && !result.b().equals(original.b())) changed |= 2;
            return changed;
        }
        private void repairPartition() {
            stage = Stage.REPAIR_ACK;
            // BlockingPartitionRepair: quorum minus contacts with no mutation (clamped at zero).
            waiting = 2;
            for (int target : new int[] { index, other }) {
                int changed = diff(full[target]);
                repairTarget[target] = changed != 0;
                if (changed == 0) --waiting;
            }
            for (int target : new int[] { index, other }) {
                int changed = diff(full[target]);
                if (changed != 0) {
                    Data delta = new Data((changed & 1) != 0 ? result.a() : ZERO,
                            (changed & 2) != 0 ? result.b() : ZERO);
                    send(replicas.get(target), new RepairReq(self(), id, changed, delta));
                }
            }
            if (waiting == 0) replyRead();
        }
        private void onRepairResponse(RepairRsp response) {
            if (stage != Stage.REPAIR_ACK || response.id() != id
                    || !repairTarget[response.replica()] || acked[response.replica()]) return;
            acked[response.replica()] = true;
            if (--waiting == 0) replyRead();
        }
        private void replyRead() {
            stage = Stage.IDLE;
            send(clients.get(client), mask == 1 ? new ValueA(client, result.a().value())
                    : new ValueAB(client, result.a().value(), result.b().value()));
        }

        private void apply(int selectedMask, Data update) {
            if ((selectedMask & 1) != 0) a = reconcile(a, update.a());
            if ((selectedMask & 2) != 0) b = reconcile(b, update.b());
        }
        private void mutationVerb(MutationReq request) {
            apply(request.mask(), request.data());
            send(request.coordinator(), new MutationRsp(request.id(), index));
        }
        private void readVerb(ReadReq request) {
            Data snapshot = new Data((request.mask() & 1) != 0 ? a : ZERO,
                    (request.mask() & 2) != 0 ? b : ZERO);
            if (request.digest()) send(request.coordinator(), new DigestRsp(request.id(), index, digest(snapshot)));
            else send(request.coordinator(), new DataRsp(request.id(), index, request.phase(), snapshot));
        }
        private void repairVerb(RepairReq request) {
            apply(request.mask(), request.data());
            send(request.coordinator(), new RepairRsp(request.id(), index));
        }
    }
}
