package bench.pineapple_code;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Port of the single-key Pineapple replica of the Go sources (pineapple.go, pineappleproto.go,
 * genericsmr.go) to JMC's message-passing API, against the program interface INTERFACE.md.
 * Three replicas n0, n1, n2 (n0 IsLeader), two clients c0 -> n0, c1 -> n1. See NOTES.md.
 */
public final class PineappleCode implements MpProgram {

    // ---- client <-> node messages (shape fixed by INTERFACE.md) ----

    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}

    // ---- node <-> node messages (pineappleproto.go) ----

    /** pineappleproto.Tag. */
    @Tag("Tag") public record PTag(int timestamp, int id) {}
    /** pineappleproto.Payload. */
    @Tag("Payload") public record Payload(PTag tag, int value) {}

    @Tag("Get") public record Get(int replicaId, int instance, int write, int key, Payload payload)
            implements MpMessage {}
    @Tag("GetReply") public record GetReply(int replicaId, int instance, int ok, int write, int key,
            Payload payload) implements MpMessage {}
    @Tag("Set") public record Set(int replicaId, int instance, int write, int key, Payload payload)
            implements MpMessage {}
    @Tag("SetReply") public record SetReply(int instance) implements MpMessage {}
    /** Command []state.Command is carried as its only used field, the key Command[0].K. */
    @Tag("RMWGet") public record RMWGet(int leaderId, int instance, int ballot, int cmdKey)
            implements MpMessage {}
    @Tag("RMWGetReply") public record RMWGetReply(int instance, int ballot, int key, Payload payload)
            implements MpMessage {}
    @Tag("RMWSet") public record RMWSet(int leaderId, int instance, int ballot, int cmdKey, int key,
            Payload payload) implements MpMessage {}
    @Tag("RMWSetReply") public record RMWSetReply(int instance, int ok, int ballot)
            implements MpMessage {}

    static final int N = 3;
    static final int CLIENTS = 2;
    static final int KEY = 0;
    static final int TRUE = 1;
    static final int FALSE = 0;
    static final Payload EMPTY = new Payload(new PTag(0, 0), 0);

    private final int q;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * @param args {@code impl,<q>,<fifo|bag>}
     */
    public PineappleCode(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>");
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1");
        }
        this.modelName = args[2];
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag");
        };
    }

    @Override public String programName() { return "pineapple"; }
    @Override public String size() { return String.valueOf(q); }
    @Override public CommunicationModel model() { return model; }
    @Override public List<String> rolesInSpawnOrder() { return List.of("c0", "c1"); }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }

    @Override
    public void run() {
        Tid[] nodes = new Tid[N];
        Tid[] clients = new Tid[CLIENTS];
        // Nodes in index order; each Tid is recorded before its thread starts.
        for (int i = 0; i < N; i++) {
            Node node = new Node(i, i == 0, nodes, clients, model);
            MpThread t = new MpThread(node::loop, true); // daemon: never terminates
            nodes[i] = Tid.of(t.getTaskId());
            t.start();
        }
        MpThread[] cs = new MpThread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int ci = i;
            MpThread t = new MpThread(() -> client(ci, q, nodes[ci], model), "c" + i);
            clients[i] = Tid.of(t.getTaskId(), "c" + i);
            cs[i] = t;
            t.start();
        }
        for (MpThread t : cs) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    static void client(int i, int q, Tid node, CommunicationModel model) {
        for (int j = 0; j < q; j++) {
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(model, node, new Write(i, value(i, j)));
                    MpMessage r = Channel.recv(model);
                    if (!(r instanceof Ack)) throw new AssertionError("Ack expected, got " + r);
                }
                case 1 -> {
                    Channel.send(model, node, new Read(i));
                    MpMessage r = Channel.recv(model);
                    if (!(r instanceof Value)) throw new AssertionError("Value expected, got " + r);
                }
                default -> {
                    Channel.send(model, node, new Rmw(i));
                    MpMessage r = Channel.recv(model);
                    if (!(r instanceof Result)) throw new AssertionError("Result expected, got " + r);
                }
            }
        }
    }

    // ---- the replica (pineapple.go Replica) ----

    enum Op { PUT, GET, RMW }

    enum InstanceStatus { PREPARING, PREPARED, ACCEPTED, COMMITTED }

    /** genericsmr.Propose: the command and the client to answer (the Reply writer). */
    record Propose(Op op, int key, int client) {}

    static final class LeaderBookkeeping {
        List<Propose> clientProposals;
        Map<Integer, Boolean> hasMaxTag;
        int getOKs;
        int setOKs;
        boolean getDone;
        int rmwGetOKs;
        int rmwSetOKs;
        boolean rmwGetDone;
        int nacks;
        boolean completed;
    }

    static final class Instance {
        int cmdKey;
        PTag initialTag = new PTag(0, 0);
        int rmwId;
        Payload receivedRMW = EMPTY;
        List<GetReply> receivedData = new ArrayList<>();
        List<Payload> receivedRMWData = new ArrayList<>();
        int ballot;
        InstanceStatus status;
        LeaderBookkeeping lb;
    }

    static final class Node {
        final int id;
        final boolean isLeader;
        final Tid[] nodes;
        final Tid[] clients;
        final CommunicationModel model;
        final boolean dreply = true;

        // data map[int]Payload, one key: present flag + payload (zero Payload when absent)
        boolean dataExists = false;
        Payload data = EMPTY;

        final Map<Integer, Instance> instanceSpace = new HashMap<>();
        final int defaultBallot = 0;
        int crtInstance = 0;
        int crtRmwId = 0;
        int rmwDoneUpTo = -1;
        final Map<Integer, Instance> pendingRMWs = new HashMap<>();
        int executeCursor = 0; // executeRMWs' i (replica 0 only)

        Node(int id, boolean isLeader, Tid[] nodes, Tid[] clients, CommunicationModel model) {
            this.id = id;
            this.isLeader = isLeader;
            this.nodes = nodes;
            this.clients = clients;
            this.model = model;
        }

        void setData(Payload p) {
            data = p;
            dataExists = true;
        }

        /** Run: the main processing loop; every case of the select is one message type. */
        void loop() {
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write w) {
                    handlePropose(new Propose(Op.PUT, KEY, w.client()));
                } else if (m instanceof Read r) {
                    handlePropose(new Propose(Op.GET, KEY, r.client()));
                } else if (m instanceof Rmw r) {
                    handlePropose(new Propose(Op.RMW, KEY, r.client()));
                } else if (m instanceof Set s) {
                    handleSet(s);
                } else if (m instanceof Get g) {
                    handleGet(g);
                } else if (m instanceof SetReply s) {
                    handleSetReply(s);
                } else if (m instanceof GetReply g) {
                    handleGetReply(g);
                } else if (m instanceof RMWGet g) {
                    handleRMWGet(g);
                } else if (m instanceof RMWGetReply g) {
                    handleRMWGetReply(g);
                } else if (m instanceof RMWSet s) {
                    handleRMWSet(s);
                } else if (m instanceof RMWSetReply s) {
                    handleRMWSetReply(s);
                } else {
                    throw new AssertionError("unexpected message " + m);
                }
            }
        }

        void send(int replica, MpMessage msg) {
            Channel.send(model, nodes[replica], msg);
        }

        boolean isLargerTag(PTag currentTag, PTag receivedTag) {
            if (receivedTag.timestamp() > currentTag.timestamp()) {
                return true;
            } else if (receivedTag.timestamp() == currentTag.timestamp()) {
                if (currentTag.id() == receivedTag.id()) {
                    return false;
                } else if (isLeader && currentTag.id() == id) {
                    return true;
                } else {
                    return currentTag.id() < receivedTag.id();
                }
            }
            return false;
        }

        /** replyClient (ABD); the reply value is the data at this point (NOTES.md D3). */
        void replyClient(int instance) {
            Instance inst = instanceSpace.get(instance);
            if (inst.lb.clientProposals != null && dreply && !inst.lb.completed) {
                Propose p = inst.lb.clientProposals.get(0);
                MpMessage reply = p.op() == Op.PUT ? new Ack() : new Value(data.value());
                Channel.send(model, clients[p.client()], reply);
                inst.lb.completed = true;
            }
        }

        void bcastGet(int instance, boolean write, int key) {
            int wr = FALSE;
            Payload payload = EMPTY;
            if (write) {
                wr = TRUE;
            } else {
                payload = data;
            }
            Get args = new Get(id, instance, wr, key, payload);
            int replicaCount = N - 1;
            int qq = id;
            for (int sentCount = 0; sentCount < replicaCount; sentCount++) {
                qq = (qq + 1) % N;
                if (qq == id) break;
                send(qq, args);
            }
        }

        void handleGet(Get get) {
            GetReply getReply;
            int ok = TRUE;
            boolean doesExist = dataExists;
            if (get.write() == 0) {
                if (!doesExist || isLargerTag(data.tag(), get.payload().tag())) {
                    setData(get.payload());
                    getReply = new GetReply(id, get.instance(), ok, get.write(), get.key(), get.payload());
                } else {
                    getReply = new GetReply(id, get.instance(), ok, get.write(), get.key(), data);
                }
            } else {
                getReply = new GetReply(id, get.instance(), ok, get.write(), get.key(), EMPTY);
            }
            send(get.replicaId(), getReply);
        }

        void handleGetReply(GetReply getReply) {
            Instance inst = instanceSpace.get(getReply.instance());
            int key = getReply.key();
            if (inst.lb.getDone) {
                return;
            }
            inst.receivedData.add(getReply);
            if (isLargerTag(data.tag(), getReply.payload().tag())) {
                setData(getReply.payload());
            }
            if (getReply.ok() == TRUE) {
                inst.lb.getOKs++;
                if (inst.lb.getOKs + 1 > (N >> 1)) {
                    int identicalCount = 0;
                    PTag ownTag = data.tag();
                    PTag firstReceivedTag = inst.receivedData.get(0).payload().tag();
                    for (GetReply reply : inst.receivedData) {
                        if (reply.payload().tag().equals(firstReceivedTag)) {
                            identicalCount++;
                        }
                        if (reply.payload().tag().equals(ownTag) && getReply.write() == 0) {
                            inst.lb.hasMaxTag.put(reply.replicaId(), true);
                        }
                    }
                    if (inst.initialTag.equals(firstReceivedTag) || isLargerTag(inst.initialTag, firstReceivedTag)) {
                        identicalCount++;
                    }
                    int receivedDataCount = inst.receivedData.size();
                    inst.receivedData = new ArrayList<>();
                    inst.lb.getDone = true;

                    if (getReply.write() == 0 && identicalCount == receivedDataCount + 1) {
                        replyClient(getReply.instance());
                        return;
                    }

                    boolean write = false;
                    inst.status = InstanceStatus.PREPARED;
                    inst.lb.nacks = 0;
                    if (getReply.write() == 1) {
                        write = true;
                        PTag newTag = new PTag(data.tag().timestamp() + 1, id);
                        setData(new Payload(newTag, data.value()));
                    }
                    bcastSet(getReply.instance(), write, key, data);
                }
            }
        }

        void bcastSet(int instance, boolean write, int key, Payload payload) {
            int wr = write ? TRUE : FALSE;
            Set args = new Set(id, instance, wr, key, payload);
            int replicaCount = N - 1;
            int qq = id;
            Instance inst = instanceSpace.get(instance);
            for (int sentCount = 0; sentCount < replicaCount; sentCount++) {
                qq = (qq + 1) % N;
                if (qq == id) break;
                if (!write) {
                    if (Boolean.TRUE.equals(inst.lb.hasMaxTag.get(qq))) {
                        continue;
                    }
                }
                send(qq, args);
            }
        }

        void handleSet(Set set) {
            if (isLargerTag(data.tag(), set.payload().tag())) {
                setData(set.payload());
            }
            send(set.replicaId(), new SetReply(set.instance()));
        }

        void handleSetReply(SetReply setReply) {
            Instance inst = instanceSpace.get(setReply.instance());
            inst.lb.setOKs++;
            int h = inst.lb.hasMaxTag == null ? 0 : inst.lb.hasMaxTag.size();
            if ((inst.lb.setOKs + 1 > h && h < (N >> 1) && h > 0) || inst.lb.setOKs + 1 > (N >> 1)) {
                replyClient(setReply.instance());
            }
        }

        void bcastRMWGet(int instance, int ballot, int cmdKey) {
            RMWGet args = new RMWGet(id, instance, ballot, cmdKey);
            int n = N - 1;
            int qq = id;
            for (int sent = 0; sent < n; ) {
                qq = (qq + 1) % N;
                if (qq == id) break;
                sent++;
                send(qq, args);
            }
        }

        void handleRMWGet(RMWGet rmwGet) {
            Instance inst = instanceSpace.get(rmwGet.instance());
            int key = rmwGet.cmdKey();
            RMWGetReply rmwGetReply;
            if (inst == null) {
                if (rmwGet.ballot() < defaultBallot) {
                    throw new AssertionError("outdated ballot received");
                }
                Instance ni = new Instance();
                ni.cmdKey = rmwGet.cmdKey();
                ni.ballot = rmwGet.ballot();
                ni.status = InstanceStatus.ACCEPTED;
                ni.lb = null;
                instanceSpace.put(rmwGet.instance(), ni);
                rmwGetReply = new RMWGetReply(rmwGet.instance(), defaultBallot, key, EMPTY);
            } else if (rmwGet.ballot() < inst.ballot) {
                throw new AssertionError("outdated ballot received");
            } else {
                inst.cmdKey = rmwGet.cmdKey();
                if (inst.status != InstanceStatus.COMMITTED) {
                    inst.status = InstanceStatus.ACCEPTED;
                }
                rmwGetReply = new RMWGetReply(rmwGet.instance(), defaultBallot, key, data);
            }
            send(rmwGet.leaderId(), rmwGetReply);
        }

        void handleRMWGetReply(RMWGetReply rmwGetReply) {
            Instance inst = instanceSpace.get(rmwGetReply.instance());
            if (inst.lb.rmwGetDone) {
                return;
            }
            inst.receivedRMWData.add(rmwGetReply.payload());
            inst.lb.rmwGetOKs++;
            if (inst.lb.rmwGetOKs + 1 > (N >> 1)) {
                int key = rmwGetReply.key();
                for (Payload d : inst.receivedRMWData) {
                    if (isLargerTag(data.tag(), d.tag())) {
                        setData(rmwGetReply.payload()); // as in the Go code: the reply's payload, not d
                    }
                }
                inst.receivedRMWData = new ArrayList<>();
                inst.lb.rmwGetDone = true;
                inst.lb.nacks = 0;
                PTag newTag = new PTag(data.tag().timestamp() + 1, id);
                int newValue = data.value() + 10; // interface f(v) = v + 10 (Go: + 1), NOTES.md D2
                setData(new Payload(newTag, newValue));
                bcastRMWSet(rmwGetReply.instance(), rmwGetReply.ballot(), key);
            }
        }

        void bcastRMWSet(int instance, int ballot, int key) {
            RMWSet args = new RMWSet(id, instance, ballot, instanceSpace.get(instance).cmdKey, key, data);
            int n = N - 1;
            int qq = id;
            for (int sent = 0; sent < n; ) {
                qq = (qq + 1) % N;
                if (qq == id) break;
                sent++;
                send(qq, args);
            }
        }

        void handleRMWSet(RMWSet rmwSet) {
            Instance inst = instanceSpace.get(rmwSet.instance());
            RMWSetReply rmwSetReply;
            if (inst == null) {
                if (rmwSet.ballot() < defaultBallot) {
                    throw new AssertionError("outdated ballot received");
                }
                Instance ni = new Instance();
                ni.cmdKey = rmwSet.cmdKey();
                ni.ballot = rmwSet.ballot();
                ni.status = InstanceStatus.ACCEPTED;
                ni.lb = null;
                instanceSpace.put(rmwSet.instance(), ni);
                inst = ni;
                rmwSetReply = new RMWSetReply(rmwSet.instance(), TRUE, defaultBallot);
            } else if (inst.ballot > rmwSet.ballot()) {
                throw new AssertionError("outdated ballot received");
            } else if (inst.ballot < rmwSet.ballot()) {
                inst.cmdKey = rmwSet.cmdKey();
                inst.ballot = rmwSet.ballot();
                inst.status = InstanceStatus.ACCEPTED;
                rmwSetReply = new RMWSetReply(rmwSet.instance(), TRUE, defaultBallot);
            } else {
                inst.cmdKey = rmwSet.cmdKey();
                if (inst.status != InstanceStatus.COMMITTED) {
                    inst.status = InstanceStatus.ACCEPTED;
                }
                rmwSetReply = new RMWSetReply(rmwSet.instance(), TRUE, defaultBallot);
            }
            inst.receivedRMW = rmwSet.payload();
            if (isLargerTag(data.tag(), inst.receivedRMW.tag())) {
                setData(inst.receivedRMW);
            }
            send(rmwSet.leaderId(), rmwSetReply);
        }

        void handleRMWSetReply(RMWSetReply rmwSetReply) {
            Instance inst = instanceSpace.get(rmwSetReply.instance());
            if (inst.rmwId <= rmwDoneUpTo) {
                return;
            }
            inst.lb.rmwSetOKs++;
            if (inst.lb.rmwSetOKs + 1 > (N >> 1)) {
                pendingRMWs.put(inst.rmwId, inst);
                rmwDoneUpTo++;
                if (id == 0) {
                    executeRMWs();
                } else {
                    // The Go code runs executeRMWs only on replica 0 (NOTES.md D4): reply here.
                    replyRmw(inst);
                }
            }
        }

        /** One pass of executeRMWs' inner loop (replica 0). */
        void executeRMWs() {
            while (executeCursor <= rmwDoneUpTo) {
                replyRmw(pendingRMWs.get(executeCursor));
                executeCursor++;
            }
        }

        /** The RMW reply; its value is the data at completion (NOTES.md D2). */
        void replyRmw(Instance inst) {
            if (inst.lb.clientProposals != null && dreply && !inst.lb.completed) {
                Propose p = inst.lb.clientProposals.get(0);
                inst.lb.completed = true;
                Channel.send(model, clients[p.client()], new Result(data.value()));
            }
        }

        void handlePropose(Propose propose) {
            while (instanceSpace.get(crtInstance) != null) {
                crtInstance++;
            }
            int instNo = crtInstance;
            int key = propose.key();
            List<Propose> proposals = List.of(propose);

            Instance inst = new Instance();
            inst.cmdKey = key;
            inst.ballot = 0;
            inst.status = InstanceStatus.PREPARING;
            inst.lb = new LeaderBookkeeping();
            inst.lb.hasMaxTag = new HashMap<>();
            inst.lb.clientProposals = proposals;
            instanceSpace.put(instNo, inst);

            if (propose.op() != Op.PUT && propose.op() != Op.GET) {
                int rmwId = crtRmwId;
                crtRmwId++;
                Instance ri = new Instance();
                ri.rmwId = rmwId;
                ri.cmdKey = key;
                ri.ballot = 0;
                ri.status = InstanceStatus.PREPARING;
                ri.lb = new LeaderBookkeeping();
                ri.lb.clientProposals = proposals;
                instanceSpace.put(instNo, ri);
                bcastRMWGet(instNo, 0, key);
            } else if (propose.op() == Op.PUT) {
                bcastGet(instNo, true, key);
            } else {
                if (!dataExists) {
                    PTag tag = new PTag(0, id);
                    inst.initialTag = tag;
                    setData(new Payload(tag, 0));
                } else {
                    inst.initialTag = data.tag();
                }
                bcastGet(instNo, false, key);
            }
        }
    }

    @Override
    public String toString() {
        return "PineappleCode(q=" + q + ", " + modelName + ")";
    }
}
