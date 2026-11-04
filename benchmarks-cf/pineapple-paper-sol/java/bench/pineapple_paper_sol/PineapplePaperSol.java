package bench.pineapple_paper_sol;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Single-key Algorithms 1 and 2, with a stable leader n0. */
public final class PineapplePaperSol implements MpProgram {
    private static final int N = 3, QUORUM = N / 2 + 1;
    private final int q;
    private final CommunicationModel communication;

    public PineapplePaperSol(String[] a) {
        if (a.length != 3 || !a[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>");
        }
        q = Integer.parseInt(a[1]);
        if (q < 1) throw new IllegalArgumentException("q must be positive");
        communication = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    @Override public String programName() { return "pineapple"; }
    @Override public String size() { return Integer.toString(q); }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "c0", "c1");
    }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }

    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}

    @Tag("Pstamp") public record Pstamp(long ts, int writer, long slot)
            implements Comparable<Pstamp> {
        @Override public int compareTo(Pstamp p) {
            int c = Long.compare(ts, p.ts);
            if (c == 0) c = Integer.compare(writer, p.writer);
            return c == 0 ? Long.compare(slot, p.slot) : c;
        }
    }
    @Tag("GetTag") public record GetTag(int coordinator, long request) implements MpMessage {}
    @Tag("AckTag") public record AckTag(long request, int replica, Pstamp ps) implements MpMessage {}
    @Tag("GetValue") public record GetValue(int coordinator, long request) implements MpMessage {}
    @Tag("AckValue") public record AckValue(long request, int replica, Pstamp ps, int value)
            implements MpMessage {}
    @Tag("Put") public record Put(int coordinator, long request, Pstamp ps, int value)
            implements MpMessage {}
    @Tag("AckPut") public record AckPut(long request, int replica) implements MpMessage {}
    @Tag("ForwardRmw") public record ForwardRmw(int proxy, int client, long request)
            implements MpMessage {}
    @Tag("RmwDone") public record RmwDone(long request, int client, int value) implements MpMessage {}

    @Override public void run() {
        // Configuration only: filled completely before any process is started, never changed after.
        Tid[] nodes = new Tid[N], clients = new Tid[2];
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < N; i++) {
            final int id = i;
            threads[i] = new MpThread(() -> new Node(id, nodes, clients).run(), "n" + i);
            nodes[i] = Tid.of(threads[i].getTaskId(), "n" + i);
        }
        for (int i = 0; i < 2; i++) {
            final int id = i;
            threads[N + i] = new MpThread(() -> client(id, nodes[id]), "c" + i);
            clients[i] = Tid.of(threads[N + i].getTaskId(), "c" + i);
        }
        for (MpThread t : threads) t.start();
        try {
            for (int i = N; i < threads.length; i++) threads[i].join1();
            for (int i = 0; i < N; i++) threads[i].join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void send(Tid dst, MpMessage m) { Channel.send(communication, dst, m); }

    private void client(int id, Tid node) {
        for (int j = 0; j < q; j++) {
            int kind = (id + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(id, 1 + id + 2 * j);
                case 1 -> new Read(id);
                default -> new Rmw(id);
            };
            send(node, request);
            MpMessage reply = Channel.recv(communication);
            if (!(kind == 0 && reply instanceof Ack
                    || kind == 1 && reply instanceof Value
                    || kind == 2 && reply instanceof Result)) {
                throw new AssertionError("unexpected client reply " + reply);
            }
        }
    }

    private enum Kind { WRITE, READ, RMW }
    private record Work(Kind kind, int client, int value, int proxy, long originRequest) {}

    /** Mutable objects below belong exclusively to one node's process. */
    private static final class Operation {
        final long request;
        final Work work;
        final boolean[] got = new boolean[N], put = new boolean[N];
        final Pstamp[] stamps = new Pstamp[N];
        final int[] values = new int[N];
        int gets, puts, output;
        boolean putting, replied;
        Operation(long request, Work work) { this.request = request; this.work = work; }
    }

    private final class Node {
        final int id;
        final Tid[] nodes, clients;
        Pstamp stored = new Pstamp(0, -1, 0);
        int value;
        long nextRequest, slot;
        final Map<Long, Operation> operations = new HashMap<>();
        final Map<Long, Integer> forwarded = new HashMap<>();
        final ArrayDeque<Work> rmws = new ArrayDeque<>();
        Operation activeRmw;

        Node(int id, Tid[] nodes, Tid[] clients) {
            this.id = id; this.nodes = nodes; this.clients = clients;
        }
        void broadcast(MpMessage m) { for (Tid node : nodes) send(node, m); }

        void run() {
            while (true) {
                MpMessage m = Channel.recv(communication);
                if (m instanceof Write w) {
                    start(new Work(Kind.WRITE, w.client(), w.value(), id, -1));
                } else if (m instanceof Read r) {
                    start(new Work(Kind.READ, r.client(), 0, id, -1));
                } else if (m instanceof Rmw r) {
                    if (id == 0) enqueue(new Work(Kind.RMW, r.client(), 0, id, -1));
                    else {
                        long request = nextRequest++;
                        forwarded.put(request, r.client());
                        send(nodes[0], new ForwardRmw(id, r.client(), request));
                    }
                } else if (m instanceof ForwardRmw r) {
                    if (id != 0) throw new AssertionError("RMW leader expected");
                    enqueue(new Work(Kind.RMW, r.client(), 0, r.proxy(), r.request()));
                } else if (m instanceof RmwDone r) {
                    Integer client = forwarded.remove(r.request());
                    if (client == null || client != r.client()) throw new AssertionError("unknown RMW");
                    send(clients[client], new Result(r.value()));
                } else if (m instanceof GetTag g) {
                    send(nodes[g.coordinator()], new AckTag(g.request(), id, stored));
                } else if (m instanceof GetValue g) {
                    send(nodes[g.coordinator()], new AckValue(g.request(), id, stored, value));
                } else if (m instanceof Put p) {
                    if (stored.compareTo(p.ps()) < 0) { stored = p.ps(); value = p.value(); }
                    send(nodes[p.coordinator()], new AckPut(p.request(), id));
                } else if (m instanceof AckTag a) {
                    got(a.request(), a.replica(), a.ps(), 0, true);
                } else if (m instanceof AckValue a) {
                    got(a.request(), a.replica(), a.ps(), a.value(), false);
                } else if (m instanceof AckPut a) {
                    putAck(a);
                } else throw new AssertionError("unexpected node message " + m);
            }
        }

        void enqueue(Work w) {
            rmws.addLast(w);
            nextRmw();
        }
        void nextRmw() {
            if (activeRmw == null && !rmws.isEmpty()) start(rmws.removeFirst());
        }
        void start(Work w) {
            Operation op = new Operation(nextRequest++, w);
            operations.put(op.request, op);
            if (w.kind() == Kind.RMW) activeRmw = op;
            if (w.kind() == Kind.WRITE) broadcast(new GetTag(id, op.request));
            else broadcast(new GetValue(id, op.request));
        }
        void got(long request, int replica, Pstamp ps, int v, boolean tagOnly) {
            Operation op = operations.get(request);
            if (op == null || op.putting || op.got[replica]) return; // late get response
            if (tagOnly != (op.work.kind() == Kind.WRITE)) throw new AssertionError("wrong get phase");
            // Exactly one remote member plus n0; the first remote response selects it.
            if (op.work.kind() == Kind.RMW && replica != 0
                    && op.gets - (op.got[0] ? 1 : 0) >= QUORUM - 1) return;
            op.got[replica] = true;
            op.stamps[replica] = ps;
            op.values[replica] = v;
            op.gets++;
            // First majority in arrival order; RMW must additionally contain n0.
            if (op.gets < QUORUM || op.work.kind() == Kind.RMW && !op.got[0]) return;
            int best = -1;
            for (int i = 0; i < N; i++) {
                if (op.got[i] && (best < 0 || op.stamps[i].compareTo(op.stamps[best]) > 0)) best = i;
            }
            Pstamp max = op.stamps[best], out = max;
            op.output = op.values[best];
            switch (op.work.kind()) {
                case WRITE -> {
                    out = new Pstamp(max.ts() + 1, id, max.slot());
                    op.output = op.work.value();
                }
                case READ -> { /* Write back the exact observed (value, pstamp). */ }
                case RMW -> {
                    op.output += 10;
                    out = new Pstamp(max.ts(), max.writer(), ++slot);
                    if (out.compareTo(max) <= 0) throw new AssertionError("RMW must advance pstamp");
                }
            }
            op.putting = true;
            broadcast(new Put(id, request, out, op.output));
        }
        void putAck(AckPut a) {
            Operation op = operations.get(a.request());
            if (op == null || !op.putting || op.put[a.replica()]) return;
            op.put[a.replica()] = true;
            op.puts++;
            if (op.puts >= QUORUM && !op.replied) {
                op.replied = true;
                switch (op.work.kind()) {
                    case WRITE -> send(clients[op.work.client()], new Ack());
                    case READ -> send(clients[op.work.client()], new Value(op.output));
                    case RMW -> {
                        if (op.work.proxy() == id) send(clients[op.work.client()], new Result(op.output));
                        else send(nodes[op.work.proxy()], new RmwDone(op.work.originRequest(),
                                op.work.client(), op.output));
                    }
                }
            }
            // Reply at any majority, but retain leader's local acceptance before next RMW.
            if (op.replied && (op.work.kind() != Kind.RMW || op.put[0])) {
                operations.remove(op.request);
                if (op.work.kind() == Kind.RMW) { activeRmw = null; nextRmw(); }
            }
        }
    }
}
