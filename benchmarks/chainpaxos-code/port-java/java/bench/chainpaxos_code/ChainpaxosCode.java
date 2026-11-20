package bench.chainpaxos_code;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ChainPaxos (github.com/pfouto/chain, ChainPaxosMixedProto / ChainPaxosDelayedProto with their
 * fronts) ported to JMC's message-passing API, for a fixed chain n0 -> n1 -> n2 with the fixed
 * leader n0, one integer register, no batching, no timers. See NOTES.md next to this file.
 *
 * <p>Arguments: {@code impl,<q>,<mixed|local>,<fifo|bag>}.
 */
public final class ChainpaxosCode implements MpProgram {

    // ------------------------------------------------------------------ instance (INTERFACE.md)
    static final int N = 3;
    static final int CLIENTS = 2;
    static final int QUORUM = 2;
    static final int LEADER = 0;
    /** c0 is connected to n0 (the head and leader), c1 to n2 (the tail). */
    static final int[] CLIENT_NODE = {0, 2};
    static final String[] ROLES = {"n0", "n1", "n2", "c0", "c1"};

    // ------------------------------------------------------------------ client <-> replica messages
    @Tag("Write")
    public record Write(int client, int value) implements MpMessage {}

    @Tag("Read")
    public record Read(int client) implements MpMessage {}

    @Tag("Rmw")
    public record Rmw(int client) implements MpMessage {}

    @Tag("Ack")
    public record Ack() implements MpMessage {}

    @Tag("Value")
    public record Value(int value) implements MpMessage {}

    @Tag("Result")
    public record Result(int value) implements MpMessage {}

    // ------------------------------------------------------------------ replica <-> replica messages
    static final int NOOP = 0;
    static final int WRITE = 1;
    static final int READ = 2;
    static final int RMW = 3;

    /**
     * A Paxos value: NoOpValue (kind NOOP) or a one-operation AppOpBatch. {@code origin}/{@code id}
     * are OpBatch's issuer and batchId; {@code client} replaces HashMapApp's opMapper channel;
     * {@code arg} is the written value.
     */
    @Tag("Op")
    public record Op(int kind, int origin, int id, int client, int arg) {}

    static final Op NOOP_VALUE = new Op(NOOP, -1, -1, -1, 0);

    /** PeerBatchMessage: a front's request forwarded to writesTo (the leader). */
    @Tag("Redirect")
    public record Redirect(Op op) implements MpMessage {}

    /** AcceptMsg without sN (one leader, one ballot for the whole run). */
    @Tag("Accept")
    public record Accept(int iN, int counter, Op value, int ack) implements MpMessage {}

    /** AcceptAckMsg: sent by the last node of the chain to the leader. */
    @Tag("AcceptAck")
    public record AcceptAck(int iN) implements MpMessage {}

    // ------------------------------------------------------------------ parameters
    private final int q;
    private final boolean local;
    private final CommunicationModel model;
    private final String variant;
    private final String delivery;

    public ChainpaxosCode(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<mixed|local>,<fifo|bag>");
        }
        q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1, was " + q);
        }
        variant = args[2];
        if (variant.equals("mixed")) {
            local = false;
        } else if (variant.equals("local")) {
            local = true;
        } else {
            throw new IllegalArgumentException("variant must be mixed or local, was " + variant);
        }
        delivery = args[3];
        if (delivery.equals("fifo")) {
            model = CommunicationModel.P2P;
        } else if (delivery.equals("bag")) {
            model = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("delivery must be fifo or bag, was " + delivery);
        }
    }

    @Override
    public String programName() {
        return "chainpaxos";
    }

    @Override
    public String size() {
        return q + "," + variant;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of(ROLES);
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public String toString() {
        return "ChainpaxosCode(q=" + q + ", " + variant + ", " + delivery + ")";
    }

    // ------------------------------------------------------------------ main
    @Override
    public void run() {
        Tid[] tids = new Tid[ROLES.length];
        MpThread[] threads = new MpThread[ROLES.length];
        // Every process is created (task id minted) before any starts, so that every body sees all ids.
        for (int k = 0; k < N; k++) {
            final int idx = k;
            threads[k] = new MpThread(() -> new Replica(idx, tids).loop(), ROLES[k]);
        }
        for (int i = 0; i < CLIENTS; i++) {
            final int ci = i;
            threads[N + i] = new MpThread(() -> client(ci, tids[CLIENT_NODE[ci]]), ROLES[N + i]);
        }
        for (int k = 0; k < ROLES.length; k++) {
            tids[k] = Tid.of(threads[k].getTaskId(), ROLES[k]);
        }
        for (MpThread t : threads) {
            t.start();
        }
        // Clients first, then the replicas (which end blocked on receive: the execution ends there).
        int[] awaitOrder = {3, 4, 0, 1, 2};
        for (int k : awaitOrder) {
            try {
                threads[k].join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting " + ROLES[k], e);
            }
        }
    }

    // ------------------------------------------------------------------ client (INTERFACE.md, verbatim)
    static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    private void client(int i, Tid node) {
        for (int j = 0; j < q; j++) {
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(model, node, new Write(i, value(i, j)));
                    if (!(Channel.recv(model) instanceof Ack)) {
                        throw new AssertionError("Ack expected");
                    }
                }
                case 1 -> {
                    Channel.send(model, node, new Read(i));
                    if (!(Channel.recv(model) instanceof Value)) {
                        throw new AssertionError("Value expected");
                    }
                }
                default -> {
                    Channel.send(model, node, new Rmw(i));
                    if (!(Channel.recv(model) instanceof Result)) {
                        throw new AssertionError("Result expected");
                    }
                }
            }
        }
    }

    /** The number of Read commands in the whole run: the bound on the leader's no-op instances. */
    private int readsInRun() {
        int n = 0;
        for (int i = 0; i < CLIENTS; i++) {
            for (int j = 0; j < q; j++) {
                if ((i + j) % 3 == 1) {
                    n++;
                }
            }
        }
        return n;
    }

    // ------------------------------------------------------------------ replica
    /** InstanceState: the accepted value, the chain counter, decided, the attached local reads. */
    static final class Inst {
        final int iN;
        Op value;
        int counter;
        boolean decided;
        final List<PendingRead> attachedReads = new ArrayList<>();

        Inst(int iN) {
            this.iN = iN;
        }
    }

    /** A local read waiting at a front (pendingReads entry; one read per "batch"). */
    record PendingRead(int id, int client) {}

    /** One replica: the protocol (ChainPaxos*Proto), its front (ChainPaxos*Front) and the app. */
    final class Replica {
        final int self;
        final Tid[] tids;

        // protocol state (ChainPaxos*Proto)
        final Map<Integer, Inst> instances = new HashMap<>();
        int highestAcknowledgedInstance = -1;
        int highestAcceptedInstance = -1;
        int highestDecidedInstance = -1;
        int lastAcceptSent = -1;
        int noOpsLeft;

        // front state (ChainPaxos*Front)
        int opCounter = 0;
        final ArrayDeque<Integer> pendingWrites = new ArrayDeque<>();
        final ArrayDeque<Integer> pendingReads = new ArrayDeque<>();

        // application state (HashMapApp -> one register)
        int x = 0;

        Replica(int self, Tid[] tids) {
            this.self = self;
            this.tids = tids;
            this.noOpsLeft = (self == LEADER && local) ? readsInRun() : 0;
        }

        void loop() {
            while (true) {
                // local variant: the NoOp timer becomes a nondeterministic no-op at the leader.
                if (noOpsLeft > 0 && Nondet.chooseIndex(2) == 1) {
                    noOpsLeft--;
                    sendNextAccept(NOOP_VALUE);
                    continue;
                }
                final int expected = highestAcceptedInstance + 1;
                // ACCEPTs are taken in instance order (the TCP order the source relies on).
                MpMessage m = Channel.recv(model, msg -> !(msg instanceof Accept a) || a.iN() == expected);
                if (m instanceof Write w) {
                    submitOperation(WRITE, w.client(), w.value());
                } else if (m instanceof Read r) {
                    if (local) {
                        submitLocalRead(r.client());
                    } else {
                        submitOperation(READ, r.client(), 0);
                    }
                } else if (m instanceof Rmw r) {
                    submitOperation(RMW, r.client(), 0);
                } else if (m instanceof Redirect rd) {
                    onPeerBatchMessage(rd.op());
                } else if (m instanceof Accept a) {
                    uponAcceptMsg(a);
                } else if (m instanceof AcceptAck aa) {
                    uponAcceptAckMsg(aa);
                } else {
                    throw new AssertionError("replica n" + self + ": unexpected message " + m);
                }
            }
        }

        // ---------------- front

        /** HashMapApp.channelRead + Front.submitOperation + sendNewBatch + sendBatchToWritesTo. */
        void submitOperation(int kind, int client, int arg) {
            int id = opCounter++;
            Op op = new Op(kind, self, id, client, arg);
            pendingWrites.add(id);
            if (self == LEADER) {
                onPeerBatchMessage(op);
            } else {
                Channel.send(model, tids[LEADER], new Redirect(op));
            }
        }

        /** Front.onPeerBatchMessage -> Proto.onSubmitBatch. */
        void onPeerBatchMessage(Op op) {
            if (self == LEADER) {
                sendNextAccept(op);
            }
            // else: "Received ... without being leader, ignoring." (cannot happen: fixed leader)
        }

        /** DelayedFront.sendNewReadBatch -> DelayedProto.onSubmitRead. */
        void submitLocalRead(int client) {
            int id = opCounter++;
            pendingReads.add(id);
            int readInstance = highestAcceptedInstance + 1;
            Inst inst = instances.computeIfAbsent(readInstance, Inst::new);
            if (inst.decided) {
                throw new IllegalStateException("read attached to a decided instance");
            }
            inst.attachedReads.add(new PendingRead(id, client));
        }

        /** Front.onExecuteBatch + HashMapApp.executeOperation. */
        void onExecuteBatch(Op op) {
            boolean mine = op.origin() == self;
            if (mine) {
                Integer expected = pendingWrites.poll();
                if (expected == null || expected != op.id()) {
                    throw new AssertionError("Expected " + op.id() + ". Got " + expected);
                }
            }
            switch (op.kind()) {
                case WRITE -> {
                    x = op.arg();
                    if (mine) {
                        Channel.send(model, tids[N + op.client()], new Ack());
                    }
                }
                case RMW -> {
                    x = x + 10;
                    if (mine) {
                        Channel.send(model, tids[N + op.client()], new Result(x));
                    }
                }
                case READ -> {
                    if (mine) {
                        Channel.send(model, tids[N + op.client()], new Value(x));
                    }
                }
                default -> throw new AssertionError("unknown operation " + op);
            }
        }

        /** DelayedFront.onExecuteRead + HashMapApp.executeOperation(read, local). */
        void onExecuteRead(PendingRead r) {
            Integer expected = pendingReads.poll();
            if (expected == null || expected != r.id()) {
                throw new AssertionError("Expected " + r.id() + ". Got " + expected);
            }
            Channel.send(model, tids[N + r.client()], new Value(x));
        }

        // ---------------- protocol

        void sendNextAccept(Op val) {
            Inst instance = instances.computeIfAbsent(lastAcceptSent + 1, Inst::new);
            if (instance.value != null) {
                throw new AssertionError("instance " + instance.iN + " already has a value");
            }
            uponAcceptMsg(new Accept(instance.iN, 0, val, highestAcknowledgedInstance));
            lastAcceptSent = instance.iN;
        }

        void uponAcceptMsg(Accept msg) {
            Inst instance = instances.computeIfAbsent(msg.iN(), Inst::new);
            if (instance.decided && instance.value != null) {
                return; // "Discarding decided msg"
            }
            if (instance.value != null && msg.counter() <= instance.counter) {
                return; // "Discarding since same sN & leader, while counter <="
            }
            instance.value = msg.value();
            instance.counter = msg.counter() + 1;
            if (highestAcceptedInstance < instance.iN) {
                highestAcceptedInstance++;
                if (highestAcceptedInstance != instance.iN) {
                    throw new AssertionError("gap: accepted " + instance.iN + " after "
                            + (highestAcceptedInstance - 1));
                }
            }
            forward(instance);
            if (!instance.decided && instance.counter >= QUORUM) {
                decideAndExecute(instance);
            }
            ackInstance(msg.ack());
        }

        void forward(Inst inst) {
            int target = (self + 1) % N; // nextOk: nobody is pending removal
            if (target == LEADER) { // am last
                if (inst.counter < QUORUM) {
                    throw new AssertionError("Last living in chain cannot decide. " + inst.counter);
                }
                Channel.send(model, tids[target], new AcceptAck(inst.iN));
            } else {
                Channel.send(model, tids[target],
                        new Accept(inst.iN, inst.counter, inst.value, highestAcknowledgedInstance));
            }
        }

        void uponAcceptAckMsg(AcceptAck msg) {
            if (msg.iN() <= highestAcknowledgedInstance) {
                return; // "Ignoring acceptAck for old instance"
            }
            Inst inst = instances.get(msg.iN());
            if (self != LEADER || inst == null) {
                throw new AssertionError("Received Ack without being leader...");
            }
            ackInstance(msg.iN());
        }

        void ackInstance(int instanceN) {
            // For nodes in the first half of the chain only
            for (int i = highestDecidedInstance + 1; i <= instanceN; i++) {
                decideAndExecute(instances.get(i));
            }
            // For everyone
            for (int i = highestAcknowledgedInstance + 1; i <= instanceN; i++) {
                Inst ins = instances.remove(i);
                if (ins == null || !ins.decided) {
                    throw new AssertionError("acknowledging undecided instance " + i);
                }
                for (PendingRead r : ins.attachedReads) {
                    onExecuteRead(r);
                }
                highestAcknowledgedInstance++;
            }
        }

        void decideAndExecute(Inst instance) {
            if (instance == null || instance.decided || highestDecidedInstance != instance.iN - 1
                    || instance.value == null) {
                throw new AssertionError("decideAndExecute out of order at n" + self);
            }
            instance.decided = true;
            highestDecidedInstance++;
            if (instance.value.kind() != NOOP) {
                onExecuteBatch(instance.value);
            }
        }
    }
}
