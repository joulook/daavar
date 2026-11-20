package bench.chainpaxos_paper_sol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Fixed-membership, fault-free ChainPaxos. See NOTES.md for paper ambiguities. */
public final class ChainpaxosPaperSol implements MpProgram {
    private final int q;
    private final boolean local;
    private final CommunicationModel communication;
    private static final List<String> ROLES = List.of("n0", "n1", "n2", "c0", "c1");

    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}
    @Tag("NoOp") public record NoOp() implements MpMessage {}
    @Tag("Request") public record Request(int client, int sequence, MpMessage operation)
            implements MpMessage {}
    @Tag("REDIRECT_REQUEST") public record RedirectRequest(Request request) implements MpMessage {}
    @Tag("ACCEPT") public record Accept(int instance, Tid leader, int prepare, Request value,
                                       int accepts, int maxack) implements MpMessage {}
    @Tag("ACCEPT_ACK") public record AcceptAck(int instance) implements MpMessage {}

    public ChainpaxosPaperSol(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<mixed|local>,<fifo|bag>");
        }
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be >= 1");
        local = switch (args[2]) {
            case "mixed" -> false;
            case "local" -> true;
            default -> throw new IllegalArgumentException("expected mixed or local");
        };
        communication = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    @Override public String programName() { return "chainpaxos"; }
    @Override public String size() { return Integer.toString(q); }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() { return ROLES; }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() {
        return "ChainpaxosPaperSol(q=" + q + ",variant=" + (local ? "local" : "mixed")
                + ",model=" + communication + ")";
    }

    private void send(Tid destination, MpMessage message) {
        Channel.send(communication, destination, message);
    }

    // Written only by main before any thread starts; thereafter immutable configuration.
    private static final class Addresses {
        List<Tid> nodes;
        List<Tid> clients;
    }

    @Override public void run() {
        Addresses addresses = new Addresses();
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final int index = i;
            threads.add(new MpThread(() -> new Replica(index, addresses).run(), ROLES.get(i)));
        }
        for (int i = 0; i < 2; i++) {
            final int index = i;
            threads.add(new MpThread(() -> client(index, addresses), ROLES.get(3 + i)));
        }
        addresses.nodes = List.of(Tid.of(threads.get(0).getTaskId(), "n0"),
                Tid.of(threads.get(1).getTaskId(), "n1"), Tid.of(threads.get(2).getTaskId(), "n2"));
        addresses.clients = List.of(Tid.of(threads.get(3).getTaskId(), "c0"),
                Tid.of(threads.get(4).getTaskId(), "c1"));
        for (MpThread thread : threads) thread.start();
        // Await clients first, then the permanent replica receive loops.
        try {
            threads.get(3).join1();
            threads.get(4).join1();
            for (int i = 0; i < 3; i++) threads.get(i).join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void client(int index, Addresses addresses) {
        Tid node = addresses.nodes.get(index == 0 ? 0 : 2);
        for (int j = 0; j < q; j++) {
            int kind = (index + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(index, 1 + index + 2 * j);
                case 1 -> new Read(index);
                default -> new Rmw(index);
            };
            send(node, request);
            MpMessage reply = Channel.recv(communication);
            if (!(kind == 0 && reply instanceof Ack
                    || kind == 1 && reply instanceof Value
                    || kind == 2 && reply instanceof Result)) {
                throw new AssertionError("unexpected reply " + reply);
            }
        }
    }

    private static final class PaxosInst {
        final int na;
        final Request val;
        int nacpts;
        boolean decided;
        PaxosInst(int na, Request val, int nacpts) {
            this.na = na;
            this.val = val;
            this.nacpts = nacpts;
        }
    }

    private record WaitingRead(Request request, int dependency) {}

    private final class Replica {
        // Algorithm 1: fixed chain, supported leader, empty removal set, fixed prepare.
        final List<Tid> chain;
        final Tid self;
        final Tid cnextok;
        final Tid csleader;
        final Set<Tid> marked = Set.of();
        final int npleader = 0;
        final boolean amLeader;
        final Map<Integer, PaxosInst> inst = new TreeMap<>();
        final Set<Request> submitted = new HashSet<>();
        final Set<Request> pending = new LinkedHashSet<>();
        int maxack;
        int maxacpt;
        final int index;
        final List<Tid> clients;
        final int[] nextRequest = new int[2];
        final Map<Integer, Accept> incoming = new TreeMap<>();
        final List<WaitingRead> reads = new ArrayList<>();
        int highestSeen;
        int acceptedThrough;
        int executedThrough;
        int acknowledgedThrough;
        int x;
        int noopsRemaining;

        Replica(int index, Addresses addresses) {
            this.index = index;
            chain = addresses.nodes;
            clients = addresses.clients;
            self = chain.get(index);
            cnextok = chain.get((index + 1) % 3);
            csleader = chain.get(0);
            amLeader = index == 0;
            if (local && amLeader) {
                for (int i = 0; i < 2; i++) {
                    for (int j = 0; j < q; j++) {
                        if ((i + j) % 3 == 1) noopsRemaining++;
                    }
                }
            }
        }

        void run() {
            while (true) {
                if (amLeader && pending.isEmpty() && noopsRemaining > 0
                        && Nondet.chooseBoolean()) {
                    noopsRemaining--;
                    startInstance(new Request(-1, maxacpt + 1, new NoOp()));
                    continue;
                }
                MpMessage message = Channel.recv(communication);
                if (message instanceof Write w) newRequest(w.client(), w);
                else if (message instanceof Read r) newRequest(r.client(), r);
                else if (message instanceof Rmw r) newRequest(r.client(), r);
                else if (message instanceof RedirectRequest r) {
                    if (!amLeader) throw new AssertionError("redirect to non-leader");
                    pending.add(r.request());
                    // Batching is off; the arrival order selects the next request.
                    Request request = pending.iterator().next();
                    pending.remove(request);
                    startInstance(request);
                } else if (message instanceof Accept a) receiveAccept(a);
                else if (message instanceof AcceptAck a) {
                    if (!amLeader) throw new AssertionError("ack to non-leader");
                    decideAndGCUpTo(a.instance());
                } else throw new AssertionError("unexpected replica message " + message);
            }
        }

        void newRequest(int client, MpMessage operation) {
            if (index != (client == 0 ? 0 : 2)) throw new AssertionError("wrong front");
            Request request = new Request(client, nextRequest[client]++, operation);
            submitted.add(request);
            if (local && operation instanceof Read) {
                // Section 4: this includes received but not yet decided instances.
                reads.add(new WaitingRead(request, highestSeen + 1));
            } else send(csleader, new RedirectRequest(request));
        }

        void startInstance(Request value) {
            maxacpt++;
            send(self, new Accept(maxacpt, self, npleader, value, 0, maxack));
        }

        void receiveAccept(Accept accept) {
            highestSeen = Math.max(highestSeen, accept.instance());
            if (accept.prepare() != npleader || !accept.leader().equals(csleader)) {
                throw new AssertionError("fixed leader/prepare violated");
            }
            // The paper uses FIFO channels. Restore instance order on bag channels.
            incoming.put(accept.instance(), accept);
            while (incoming.containsKey(acceptedThrough + 1)) {
                Accept a = incoming.remove(acceptedThrough + 1);
                int ni = a.instance();
                PaxosInst state = new PaxosInst(a.prepare(), a.value(), a.accepts() + 1);
                inst.put(ni, state);
                acceptedThrough = ni;
                if (state.nacpts >= 2) decide(ni);
                decideAndGCUpTo(a.maxack());
                // At the tail this ACCEPT contains all three accept acknowledgments.
                if (index == 2) {
                    serveReads();
                }
                if (cnextok.equals(csleader)) send(csleader, new AcceptAck(ni));
                else send(cnextok, new Accept(ni, csleader, state.na, state.val,
                        state.nacpts, maxack));
            }
            // A reordered ACK may have arrived before the leader's self-ACCEPT.
            decideAndGCUpTo(acknowledgedThrough);
        }

        void decide(int ni) {
            inst.get(ni).decided = true;
            executeDecidedPrefix();
        }

        void executeDecidedPrefix() {
            while (true) {
                PaxosInst state = inst.get(executedThrough + 1);
                if (state == null || !state.decided) return;
                Request request = state.val;
                MpMessage reply = null;
                if (request.operation() instanceof Write w) {
                    x = w.value();
                    reply = new Ack();
                } else if (request.operation() instanceof Rmw) {
                    x += 10;
                    reply = new Result(x);
                } else if (request.operation() instanceof Read) reply = new Value(x);
                else if (!(request.operation() instanceof NoOp)) {
                    throw new AssertionError("unknown operation");
                }
                executedThrough++;
                pending.remove(request);
                // The application/proxy at the client's own replica replies on execution.
                if (submitted.remove(request)) send(clients.get(request.client()), reply);
            }
        }

        void decideAndGCUpTo(int ni) {
            acknowledgedThrough = Math.max(acknowledgedThrough, ni);
            int through = Math.min(acknowledgedThrough, acceptedThrough);
            for (int i = maxack + 1; i <= through; i++) {
                PaxosInst state = inst.get(i);
                if (!state.decided) decide(i);
                if (executedThrough < i) throw new AssertionError("GC before execution");
                inst.remove(i);
            }
            maxack = Math.max(maxack, through);
            serveReads();
        }

        void serveReads() {
            for (int i = 0; i < reads.size();) {
                WaitingRead read = reads.get(i);
                if (read.dependency() <= (index == 2 ? acceptedThrough : maxack)
                        && read.dependency() <= executedThrough) {
                    reads.remove(i);
                    submitted.remove(read.request());
                    send(clients.get(read.request().client()), new Value(x));
                } else i++;
            }
        }
    }
}
