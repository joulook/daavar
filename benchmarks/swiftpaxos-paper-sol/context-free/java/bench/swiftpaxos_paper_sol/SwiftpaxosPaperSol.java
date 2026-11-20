package bench.swiftpaxos_paper_sol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Figures 3 and 4, ballot zero, with the interface's fixed C2 quorums. */
public final class SwiftpaxosPaperSol implements MpProgram {
    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}

    @Tag("Id") public record Id(int client, int sequence) {}
    @Tag("Command") public record Command(Id id, MpMessage request) {}
    @Tag("Propagate") public record Propagate(Command command) implements MpMessage {}
    @Tag("Reply") public record Reply(int ballot, Id id, Set<List<Id>> paths,
                                      MpMessage result) implements MpMessage {
        public Reply { paths = freezePaths(paths); }
    }
    // The sender is explicit because Channel.recv returns only the payload.
    @Tag("FastAck") public record FastAck(int ballot, Tid sender, Id id,
                                          Set<Id> dependencies, Set<List<Id>> paths)
            implements MpMessage {
        public FastAck {
            dependencies = Set.copyOf(dependencies);
            paths = freezePaths(paths);
        }
    }
    @Tag("SlowAck") public record SlowAck(int ballot, Tid sender, Id id)
            implements MpMessage {}

    private final int q;
    private final CommunicationModel communication;
    public SwiftpaxosPaperSol(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>");
        }
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be positive");
        communication = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }
    @Override public String programName() { return "swiftpaxos"; }
    @Override public String size() { return Integer.toString(q); }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "l0", "l1", "c0", "c1");
    }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() {
        return "SwiftpaxosPaperSol(impl," + q + ","
                + (communication == CommunicationModel.P2P ? "fifo" : "bag") + ")";
    }

    private static Set<List<Id>> freezePaths(Set<List<Id>> paths) {
        Set<List<Id>> copy = new LinkedHashSet<>();
        for (List<Id> path : paths) copy.add(List.copyOf(path));
        return Set.copyOf(copy);
    }

    /** Only address wiring is shared; it is completed before any process starts. */
    private final class Network {
        final Tid[] nodes = new Tid[3];
        final Tid[] libraries = new Tid[2];
        final Tid[] clients = new Tid[2];
        void send(Tid to, MpMessage message) { Channel.send(communication, to, message); }
        void broadcast(MpMessage message) {
            for (Tid node : nodes) send(node, message);
        }
        void broadcastAndClient(Id id, MpMessage message) {
            broadcast(message);
            send(libraries[id.client()], message);
        }
    }

    @Override public void run() {
        Network net = new Network();
        List<MpThread> threads = new ArrayList<>();
        // Construction allocates API task identifiers, allowing initial address knowledge
        // without an extra setup process, shared protocol state, or messages from main.
        for (int i = 0; i < 3; i++) {
            int index = i;
            MpThread t = new MpThread(() -> new Replica(index, net).run(), "n" + i);
            net.nodes[i] = Tid.of(t.getTaskId(), "n" + i);
            threads.add(t);
        }
        for (int i = 0; i < 2; i++) {
            int index = i;
            MpThread t = new MpThread(() -> new Library(index, net).run(), "l" + i);
            net.libraries[i] = Tid.of(t.getTaskId(), "l" + i);
            threads.add(t);
        }
        for (int i = 0; i < 2; i++) {
            int index = i;
            MpThread t = new MpThread(() -> client(index, net), "c" + i);
            net.clients[i] = Tid.of(t.getTaskId(), "c" + i);
            threads.add(t);
        }
        for (MpThread t : threads) t.start();
        // Await finite clients first, then the permanently receiving internal processes.
        try {
            threads.get(5).join1();
            threads.get(6).join1();
            for (int i = 0; i < 5; i++) threads.get(i).join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void client(int i, Network net) {
        for (int j = 0; j < q; j++) {
            int kind = (i + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(i, 1 + i + 2 * j);
                case 1 -> new Read(i);
                default -> new Rmw(i);
            };
            net.send(net.libraries[i], request);
            MpMessage answer = Channel.recv(communication);
            if (!(kind == 0 && answer instanceof Ack
                    || kind == 1 && answer instanceof Value
                    || kind == 2 && answer instanceof Result)) {
                throw new AssertionError("unexpected client response: " + answer);
            }
        }
    }

    /** Figure 3, running in li rather than the externally visible ci. */
    private final class Library {
        final int index;
        final Network net;
        int nextSequence;
        Id active;
        Reply reply;
        final Map<Tid, FastAck> fast = new LinkedHashMap<>();
        final Set<Tid> slow = new LinkedHashSet<>();
        Library(int index, Network net) { this.index = index; this.net = net; }
        void run() {
            while (true) {
                MpMessage m = Channel.recv(communication);
                if (m instanceof Write || m instanceof Read || m instanceof Rmw) {
                    if (active != null) throw new AssertionError("concurrent library request");
                    active = new Id(index, nextSequence++);
                    reply = null;
                    fast.clear();
                    slow.clear();
                    net.broadcast(new Propagate(new Command(active, m))); // lines 1-2
                } else if (m instanceof Reply r && r.ballot() == 0 && r.id().equals(active)) {
                    reply = r;
                } else if (m instanceof FastAck a && a.ballot() == 0 && a.id().equals(active)) {
                    fast.put(a.sender(), a);
                } else if (m instanceof SlowAck a && a.ballot() == 0 && a.id().equals(active)) {
                    slow.add(a.sender());
                }
                if (active != null && reply != null) {
                    FastAck a = fast.get(net.nodes[1]);
                    boolean fastQuorum = slow.contains(net.nodes[1])
                            || a != null && a.paths().equals(reply.paths());
                    boolean slowQuorum = slow.contains(net.nodes[1]) || slow.contains(net.nodes[2]);
                    if (fastQuorum || slowQuorum) { // lines 3-5: followers only
                        net.send(net.clients[index], reply.result());
                        active = null;
                    }
                }
                // Late acknowledgements for completed commands are simply consumed.
            }
        }
    }

    private enum Phase { START, PREACCEPT, ACCEPT, COMMIT }
    private static final class Entry {
        Command command;
        Phase phase = Phase.START;
        Set<Id> dependencies = Set.of();
        Set<List<Id>> paths = Set.of(); // immutable original proposal, Figure 4 line 14
        final Map<Tid, FastAck> fast = new LinkedHashMap<>();
        final Set<Tid> slow = new LinkedHashSet<>();
        boolean executed;
    }
    private static boolean conflicts(MpMessage a, MpMessage b) {
        return !(a instanceof Read && b instanceof Read);
    }
    private static int apply(MpMessage command, int state) {
        if (command instanceof Write w) return w.value();
        if (command instanceof Rmw) return state + 10;
        return state;
    }
    private static MpMessage result(MpMessage command, int newState) {
        if (command instanceof Write) return new Ack();
        if (command instanceof Read) return new Value(newState);
        return new Result(newState);
    }

    private final class Replica {
        final int index;
        final Network net;
        final Map<Id, Entry> entries = new LinkedHashMap<>();
        final List<Command> pendingLog = new ArrayList<>();
        int state;
        Replica(int index, Network net) { this.index = index; this.net = net; }
        boolean fastMember() { return index == 0 || index == 1; }
        // Section 3.1's membership definition: all replicas belong to a configured SQ.
        boolean slowMember() { return true; }
        Entry entry(Id id) { return entries.computeIfAbsent(id, ignored -> new Entry()); }
        void run() {
            while (true) {
                MpMessage m = Channel.recv(communication);
                if (m instanceof Propagate p) propagate(p.command());
                else if (m instanceof FastAck a && a.ballot() == 0) {
                    entry(a.id()).fast.put(a.sender(), a);
                } else if (m instanceof SlowAck a && a.ballot() == 0) {
                    entry(a.id()).slow.add(a.sender());
                } else throw new AssertionError("unexpected replica message " + m);
                settle();
            }
        }
        void propagate(Command c) {
            Entry e = entry(c.id());
            if (e.phase != Phase.START) return;
            e.phase = Phase.PREACCEPT;
            e.command = c;
            if (!fastMember()) return;
            Set<Id> deps = new LinkedHashSet<>();
            for (Map.Entry<Id, Entry> other : entries.entrySet()) {
                if (!other.getKey().equals(c.id()) && other.getValue().command != null
                        && conflicts(other.getValue().command.request(), c.request())) {
                    deps.add(other.getKey());
                }
            }
            // Bind the freshly computed D as the root before computing pset(id).
            e.dependencies = Set.copyOf(deps);
            e.paths = paths(c.id());
            if (index == 0) {
                pendingLog.add(c);
                int speculative = state;
                for (Command pending : pendingLog) speculative = apply(pending.request(), speculative);
                net.send(net.libraries[c.id().client()],
                        new Reply(0, c.id(), e.paths, result(c.request(), speculative)));
            }
            FastAck ack = new FastAck(0, net.nodes[index], c.id(), e.dependencies, e.paths);
            if (index == 0) net.broadcast(ack); // includes a real message to self
            else net.broadcastAndClient(c.id(), ack);
        }
        Set<List<Id>> paths(Id id) {
            Entry e = entries.get(id);
            if (e.dependencies.isEmpty()) return Set.of(List.of(id));
            Set<List<Id>> out = new LinkedHashSet<>();
            for (Id dep : e.dependencies) {
                for (List<Id> tail : paths(dep)) {
                    List<Id> path = new ArrayList<>();
                    path.add(id);
                    path.addAll(tail);
                    out.add(List.copyOf(path));
                }
            }
            return Set.copyOf(out);
        }
        boolean allAtLeast(Set<Id> ids, Phase minimum) {
            for (Id id : ids) {
                Entry e = entries.get(id);
                if (e == null || e.phase.ordinal() < minimum.ordinal()) return false;
            }
            return true;
        }
        boolean allExecuted(Set<Id> ids) {
            for (Id id : ids) {
                Entry e = entries.get(id);
                if (e == null || !e.executed) return false;
            }
            return true;
        }
        boolean quorum(Entry e) {
            FastAck follower = e.fast.get(net.nodes[1]);
            boolean fastQuorum = e.slow.contains(net.nodes[1])
                    || follower != null && follower.dependencies().equals(e.dependencies);
            boolean slowQuorum = e.slow.contains(net.nodes[1]) || e.slow.contains(net.nodes[2]);
            return fastQuorum || slowQuorum;
        }
        void accept(Id id, Entry e, FastAck leader) {
            e.phase = Phase.ACCEPT;
            if ((fastMember() && !e.dependencies.equals(leader.dependencies())) || slowMember()) {
                e.dependencies = leader.dependencies(); // lines 25-27
                net.broadcastAndClient(id, new SlowAck(0, net.nodes[index], id));
            } else if (fastMember() && !e.paths.equals(leader.paths())) {
                net.send(net.libraries[id.client()], new SlowAck(0, net.nodes[index], id));
            }
        }
        /** Deferred handlers: never discard an acknowledgement with an unmet guard. */
        void settle() {
            while (true) {
                List<Runnable> enabled = new ArrayList<>();
                for (Map.Entry<Id, Entry> item : entries.entrySet()) {
                    Id id = item.getKey();
                    Entry e = item.getValue();
                    FastAck leader = e.fast.get(net.nodes[0]);
                    if (e.phase == Phase.PREACCEPT && leader != null
                            && allAtLeast(leader.dependencies(), Phase.ACCEPT)) {
                        enabled.add(() -> accept(id, e, leader)); // lines 22-29
                    }
                    if (e.phase == Phase.ACCEPT && allAtLeast(e.dependencies, Phase.COMMIT)
                            && quorum(e)) {
                        enabled.add(() -> e.phase = Phase.COMMIT); // lines 30-32
                    }
                    if (e.phase == Phase.COMMIT && !e.executed && allExecuted(e.dependencies)) {
                        enabled.add(() -> {
                            state = apply(e.command.request(), state); // lines 33-36
                            e.executed = true;
                            if (index == 0) pendingLog.removeIf(c -> c.id().equals(id));
                        });
                    }
                }
                if (enabled.isEmpty()) return;
                enabled.get(Nondet.chooseIndex(enabled.size())).run();
            }
        }
    }
}
