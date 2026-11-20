package bench.pineapple_paper;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Pineapple (Bantikyan et al., NSDI 2025), section 3, on one key, in the common case: a stable
 * leader n0, no failure, no leader change, no reconfiguration, one key. ABD (Algorithm 1) serves
 * writes and reads at every node; the leader's Multi-Paxos component (Algorithm 2) serves RMWs;
 * pstamps ((ts, id), slot) order everything. See NOTES.md next to this file.
 *
 * <p>Constructed from the launcher's argument string {@code impl,<q>,<fifo|bag>}.
 */
public final class PineapplePaper implements MpProgram {

    static final int NODES = 3;
    static final int CLIENTS = 2;
    /** floor(n/2) + 1 (section 3.1, "Analysis"). */
    static final int QUORUM = NODES / 2 + 1;
    /** The Multi-Paxos leader for the whole run. */
    static final int LEADER = 0;

    // ---- client <-> node messages (shapes fixed by INTERFACE.md) ----

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

    // ---- node <-> node messages (Algorithm 1 / Algorithm 2) ----
    // op: the proxy's operation id; from: the sender's node index (a receiver does not see the sender).

    /** GET-TAG, key. */
    @Tag("GetTag")
    public record GetTag(int op, int from) implements MpMessage {}

    /** ACK-TAG, key, ps_j. */
    @Tag("AckTag")
    public record AckTag(int op, int from, int ts, int id, int slot) implements MpMessage {}

    /** PUT, key, ps, value. */
    @Tag("Put")
    public record Put(int op, int from, int ts, int id, int slot, int value) implements MpMessage {}

    /** ACK-PUT, key. */
    @Tag("AckPut")
    public record AckPut(int op, int from) implements MpMessage {}

    /** GET-VALUE, key. */
    @Tag("GetValue")
    public record GetValue(int op, int from) implements MpMessage {}

    /** ACK-VALUE, key, value_j, ps_j. */
    @Tag("AckValue")
    public record AckValue(int op, int from, int value, int ts, int id, int slot) implements MpMessage {}

    /** A client's Rmw received by non-leader node {@code node}, forwarded to the leader. */
    @Tag("RmwFwd")
    public record RmwFwd(int node, int client) implements MpMessage {}

    /** The leader's answer to an RmwFwd: the new value, for the attached node to return to {@code client}. */
    @Tag("RmwDone")
    public record RmwDone(int client, int value) implements MpMessage {}

    /** A pstamp ((ts, id), slot), ordered lexicographically (section 3.1). */
    record PStamp(int ts, int id, int slot) implements Comparable<PStamp> {
        static final PStamp ZERO = new PStamp(0, 0, 0);

        @Override
        public int compareTo(PStamp o) {
            if (ts != o.ts) {
                return Integer.compare(ts, o.ts);
            }
            if (id != o.id) {
                return Integer.compare(id, o.id);
            }
            return Integer.compare(slot, o.slot);
        }
    }

    private final int q;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * @param args {@code impl}, {@code q}, {@code fifo|bag}
     */
    public PineapplePaper(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>, got " + String.join(",", args));
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1, was " + q);
        }
        this.modelName = args[2];
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
    }

    @Override
    public String programName() {
        return "pineapple_paper";
    }

    @Override
    public String size() {
        return Integer.toString(q);
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
        return "PineapplePaper(impl, q=" + q + ", " + modelName + ")";
    }

    /** Main: start the nodes in index order, then the clients, and wait for them; no message event. */
    @Override
    public void run() {
        AtomicReferenceArray<Tid> nodes = new AtomicReferenceArray<>(NODES);
        AtomicReferenceArray<Tid> clients = new AtomicReferenceArray<>(CLIENTS);
        List<MpProcess.Handle> nodeHandles = new ArrayList<>();
        List<MpProcess.Handle> clientHandles = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            final int idx = i;
            MpProcess.Handle h = MpProcess.spawn("n" + i, () -> new Node(idx, nodes, clients).loop());
            nodes.set(i, h.tid());
            nodeHandles.add(h);
        }
        for (int i = 0; i < CLIENTS; i++) {
            final int idx = i;
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> client(idx, nodes, clients));
            clients.compareAndSet(i, null, h.tid());
            clientHandles.add(h);
        }
        for (MpProcess.Handle h : clientHandles) {
            h.await();
        }
        for (MpProcess.Handle h : nodeHandles) {
            h.await(); // nodes never end: the execution ends with them blocked on receive
        }
    }

    static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    private void client(int i, AtomicReferenceArray<Tid> nodes, AtomicReferenceArray<Tid> clients) {
        // Publish the own address before the first request, so the node can reply even if main has
        // not yet stored the handle (no message event).
        clients.compareAndSet(i, null, MpProcess.self().withRole("c" + i));
        Tid node = nodes.get(i);
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

    enum Kind { WRITE, READ, RMW }

    /** A proxy-side operation in progress (node-local state, never sent). */
    static final class Op {
        final int id;
        final Kind kind;
        final int client;
        /** For an RMW: the node the client is attached to. */
        final int origin;
        boolean putPhase;
        /** The write's value; for a read or RMW, the value put in the put phase. */
        int value;
        PStamp ps;
        final List<Integer> replyFrom = new ArrayList<>();
        final List<PStamp> replyPs = new ArrayList<>();
        final List<Integer> replyValue = new ArrayList<>();
        int putAcks;

        Op(int id, Kind kind, int client, int origin, int value) {
            this.id = id;
            this.kind = kind;
            this.client = client;
            this.origin = origin;
            this.value = value;
        }

        /** Index of the reply with the largest pstamp (the first such reply on a tie). */
        int maxReply() {
            int best = 0;
            for (int k = 1; k < replyPs.size(); k++) {
                if (replyPs.get(k).compareTo(replyPs.get(best)) > 0) {
                    best = k;
                }
            }
            return best;
        }
    }

    /** Node n_i: the ABD component (all nodes) and, at n0, the Multi-Paxos component. */
    final class Node {
        final int i;
        final AtomicReferenceArray<Tid> nodes;
        final AtomicReferenceArray<Tid> clients;
        // Storage_i[key]
        int storedValue = 0;
        PStamp storedPs = PStamp.ZERO;
        int nextOp = 0;
        final Map<Integer, Op> ops = new HashMap<>();
        // leader only
        int slot = 0;
        boolean rmwBusy = false;
        final Deque<Op> rmwQueue = new ArrayDeque<>();

        Node(int i, AtomicReferenceArray<Tid> nodes, AtomicReferenceArray<Tid> clients) {
            this.i = i;
            this.nodes = nodes;
            this.clients = clients;
        }

        void loop() {
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write w) {
                    startWrite(w.client(), w.value());
                } else if (m instanceof Read r) {
                    startRead(r.client());
                } else if (m instanceof Rmw r) {
                    if (i == LEADER) {
                        enqueueRmw(i, r.client());
                    } else {
                        Channel.send(model, nodes.get(LEADER), new RmwFwd(i, r.client()));
                    }
                } else if (m instanceof RmwFwd f) {
                    enqueueRmw(f.node(), f.client());
                } else if (m instanceof RmwDone d) {
                    Channel.send(model, clients.get(d.client()), new Result(d.value()));
                } else if (m instanceof GetTag g) {
                    // Alg. 1, lines 18-19
                    Channel.send(model, nodes.get(g.from()),
                            new AckTag(g.op(), i, storedPs.ts(), storedPs.id(), storedPs.slot()));
                } else if (m instanceof Put p) {
                    // Alg. 1, lines 20-24
                    PStamp ps = new PStamp(p.ts(), p.id(), p.slot());
                    if (storedPs.compareTo(ps) < 0) {
                        storedPs = ps;
                        storedValue = p.value();
                    }
                    Channel.send(model, nodes.get(p.from()), new AckPut(p.op(), i));
                } else if (m instanceof GetValue g) {
                    // Alg. 1, lines 25-26
                    Channel.send(model, nodes.get(g.from()), new AckValue(g.op(), i, storedValue,
                            storedPs.ts(), storedPs.id(), storedPs.slot()));
                } else if (m instanceof AckTag a) {
                    onAckTag(a);
                } else if (m instanceof AckValue a) {
                    onAckValue(a);
                } else if (m instanceof AckPut a) {
                    onAckPut(a);
                } else {
                    throw new IllegalStateException("node n" + i + ": unexpected message " + m);
                }
            }
        }

        void broadcast(MpMessage msg) {
            for (int k = 0; k < NODES; k++) {
                Channel.send(model, nodes.get(k), msg);
            }
        }

        Op newOp(Kind kind, int client, int origin, int value) {
            Op op = new Op(nextOp++, kind, client, origin, value);
            ops.put(op.id, op);
            return op;
        }

        // Alg. 1, lines 1-2
        void startWrite(int client, int value) {
            Op op = newOp(Kind.WRITE, client, i, value);
            broadcast(new GetTag(op.id, i));
        }

        // Alg. 1, lines 9-10
        void startRead(int client) {
            Op op = newOp(Kind.READ, client, i, 0);
            broadcast(new GetValue(op.id, i));
        }

        void enqueueRmw(int origin, int client) {
            rmwQueue.addLast(new Op(-1, Kind.RMW, client, origin, 0));
            startNextRmw();
        }

        // Alg. 2, lines 1-2; one RMW at a time (NOTES.md, D4)
        void startNextRmw() {
            if (rmwBusy || rmwQueue.isEmpty()) {
                return;
            }
            rmwBusy = true;
            Op queued = rmwQueue.pollFirst();
            Op op = newOp(Kind.RMW, queued.client, queued.origin, 0);
            broadcast(new GetValue(op.id, i));
        }

        // Alg. 1, lines 3-7
        void onAckTag(AckTag a) {
            Op op = ops.get(a.op());
            if (op == null || op.putPhase || op.kind != Kind.WRITE) {
                return; // a late reply beyond the quorum
            }
            op.replyFrom.add(a.from());
            op.replyPs.add(new PStamp(a.ts(), a.id(), a.slot()));
            op.replyValue.add(0);
            if (op.replyFrom.size() < QUORUM) {
                return;
            }
            PStamp max = op.replyPs.get(op.maxReply());
            op.ps = new PStamp(max.ts() + 1, i, max.slot());
            op.putPhase = true;
            broadcast(new Put(op.id, i, op.ps.ts(), op.ps.id(), op.ps.slot(), op.value));
        }

        // Alg. 1, lines 11-15 (read); Alg. 2, lines 3-8 (RMW)
        void onAckValue(AckValue a) {
            Op op = ops.get(a.op());
            if (op == null || op.putPhase || op.kind == Kind.WRITE) {
                return; // a late reply beyond the quorum
            }
            op.replyFrom.add(a.from());
            op.replyPs.add(new PStamp(a.ts(), a.id(), a.slot()));
            op.replyValue.add(a.value());
            if (op.replyFrom.size() < QUORUM) {
                return;
            }
            if (op.kind == Kind.RMW && !op.replyFrom.contains(i)) {
                return; // Alg. 2, line 3: l in Q
            }
            int best = op.maxReply();
            PStamp max = op.replyPs.get(best);
            int vmax = op.replyValue.get(best);
            if (op.kind == Kind.READ) {
                op.value = vmax;
                op.ps = max;
            } else {
                op.value = vmax + 10; // f(v) = v + 10
                slot = slot + 1;
                op.ps = new PStamp(max.ts(), max.id(), slot); // (ps_max.tag, slot_i + 1)
            }
            op.putPhase = true;
            broadcast(new Put(op.id, i, op.ps.ts(), op.ps.id(), op.ps.slot(), op.value));
        }

        // Alg. 1, lines 8, 16-17; Alg. 2, line 9
        void onAckPut(AckPut a) {
            Op op = ops.get(a.op());
            if (op == null || !op.putPhase) {
                return;
            }
            op.putAcks++;
            if (op.putAcks < QUORUM) {
                return;
            }
            ops.remove(op.id);
            switch (op.kind) {
                case WRITE -> Channel.send(model, clients.get(op.client), new Ack());
                case READ -> Channel.send(model, clients.get(op.client), new Value(op.value));
                case RMW -> {
                    if (op.origin == i) {
                        Channel.send(model, clients.get(op.client), new Result(op.value));
                    } else {
                        Channel.send(model, nodes.get(op.origin), new RmwDone(op.client, op.value));
                    }
                    rmwBusy = false;
                    startNextRmw();
                }
            }
        }
    }
}
