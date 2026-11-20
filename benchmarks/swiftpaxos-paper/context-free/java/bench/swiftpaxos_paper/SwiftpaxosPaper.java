package bench.swiftpaxos_paper;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SwiftPaxos (Ryabinin, Gotsman, Sutra; NSDI 2024), normal operation only (Figures 3 and 4):
 * ballot 0, leader n0, no recovery. N = 3 replicas n0 n1 n2, quorum configuration (C2) with the
 * unique fast quorum {n0, n1}, slow quorums {n0, n1} and {n0, n2}. Two clients c0, c1, each served
 * by its client library l0, l1. One integer register x with Write(v), Read and Rmw (x := x + 10).
 * See NOTES.md for every decision.
 */
public final class SwiftpaxosPaper implements MpProgram {

    // ---------------------------------------------------------------- configuration

    static final int N = 3;
    static final int K = 2;
    static final int LEADER = 0;  // leader(0) = n0
    static final int BALLOT = 0;  // the only ballot of the run

    /** FQ(0) under (C2): the unique fast quorum {n0, n1}. */
    static final List<Set<Integer>> FAST_QUORUMS = List.of(Set.of(0, 1));
    /** SQ(0): every majority containing the leader. */
    static final List<Set<Integer>> SLOW_QUORUMS = List.of(Set.of(0, 1), Set.of(0, 2));

    static boolean fast(int p) {
        for (Set<Integer> q : FAST_QUORUMS) {
            if (q.contains(p)) {
                return true;
            }
        }
        return false;
    }

    /** slow(p, b) of line 25, read as "p is a slow-quorum replica outside every fast quorum" (NOTES D3). */
    static boolean slowOnly(int p) {
        if (fast(p)) {
            return false;
        }
        for (Set<Integer> q : SLOW_QUORUMS) {
            if (q.contains(p)) {
                return true;
            }
        }
        return false;
    }

    static final int KIND_WRITE = 0;
    static final int KIND_READ = 1;
    static final int KIND_RMW = 2;

    static final int START = 0;
    static final int PREACCEPT = 1;
    static final int ACCEPT = 2;
    static final int COMMIT = 3;

    // ---------------------------------------------------------------- visible messages (INTERFACE.md)

    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}

    // ---------------------------------------------------------------- internal messages

    /** Command identifier (client, sequence number). */
    @Tag("Id") public record Id(int client, int seq) implements Comparable<Id> {
        @Override
        public int compareTo(Id o) {
            return client != o.client ? Integer.compare(client, o.client) : Integer.compare(seq, o.seq);
        }
    }

    /** A command: kind 0 Write(value), 1 Read, 2 Rmw. */
    @Tag("Cmd") public record Cmd(Id id, int kind, int value) {
        boolean conflicts(Cmd o) {
            return !(kind == KIND_READ && o.kind == KIND_READ);
        }
    }

    /** Propagate(c), library -> every replica (line 2). */
    @Tag("Propagate") public record Propagate(Cmd cmd) implements MpMessage {}

    /** Reply(b, id, P, r), leader -> library (line 18). */
    @Tag("Reply") public record Reply(int ballot, Id id, Set<List<Id>> paths, int result) implements MpMessage {
        public Reply {
            paths = Set.copyOf(paths);
        }
    }

    /** FastAck(b, id, D, P) from replica {@code from} (lines 19, 21). */
    @Tag("FastAck") public record FastAck(int ballot, Id id, Set<Id> deps, Set<List<Id>> paths, int from)
            implements MpMessage {
        public FastAck {
            deps = Set.copyOf(deps);
            paths = Set.copyOf(paths);
        }
    }

    /** SlowAck(b, id) from replica {@code from} (lines 27, 29). */
    @Tag("SlowAck") public record SlowAck(int ballot, Id id, int from) implements MpMessage {}

    // ---------------------------------------------------------------- program

    private final int q;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * Constructed from the launcher's argument string {@code impl,<q>,<fifo|bag>}.
     *
     * @param args {@code impl}, q, and the delivery model
     */
    public SwiftpaxosPaper(String... args) {
        if (args.length < 2 || args.length > 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>[,<fifo|bag>], got " + String.join(",", args));
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1, was " + q);
        }
        this.modelName = args.length == 3 ? args[2] : "fifo";
        switch (modelName) {
            case "fifo" -> this.model = CommunicationModel.P2P;
            case "bag" -> this.model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + modelName);
        }
    }

    @Override
    public String programName() {
        return "swiftpaxos_paper";
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
        return List.of("n0", "n1", "n2", "l0", "l1", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    /** The main task: spawns n0 n1 n2 l0 l1 c0 c1 in this order and awaits them; no message event. */
    @Override
    public void run() {
        List<String> roles = rolesInSpawnOrder();
        // Every process must know every other's address from the start (INTERFACE.md); main may
        // send nothing, so the addresses are the task ids the runtime mints in spawn order
        // (MpProgram: the k-th process main spawns is the next task), checked after each spawn.
        long base = MpProcess.self().raw();
        List<Tid> tids = new ArrayList<>();
        for (int k = 0; k < roles.size(); k++) {
            tids.add(Tid.of(base + 1 + k, roles.get(k)));
        }
        final List<Tid> replicas = List.copyOf(tids.subList(0, N));
        final List<Tid> libs = List.copyOf(tids.subList(N, N + K));
        final List<Tid> clients = List.copyOf(tids.subList(N + K, N + 2 * K));

        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int p = 0; p < N; p++) {
            final int me = p;
            handles.add(MpProcess.spawn("n" + p, () -> new Replica(me, replicas, libs).loop()));
        }
        for (int i = 0; i < K; i++) {
            final int me = i;
            handles.add(MpProcess.spawn("l" + i, () -> new Library(me, replicas, clients.get(me)).loop()));
        }
        for (int i = 0; i < K; i++) {
            final int me = i;
            handles.add(MpProcess.spawn("c" + i, () -> client(me, libs.get(me))));
        }
        for (int k = 0; k < handles.size(); k++) {
            if (!handles.get(k).tid().equals(tids.get(k))) {
                throw new AssertionError("address of " + roles.get(k) + " is " + handles.get(k).tid()
                        + ", predicted " + tids.get(k));
            }
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    // ---------------------------------------------------------------- client (as the Rust client)

    private void client(int i, Tid node) {
        for (int j = 0; j < q; j++) {
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(model, node, new Write(i, 1 + i + K * j));
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

    // ---------------------------------------------------------------- client library (Figure 3)

    private final class Library {
        final int me;
        final List<Tid> replicas;
        final Tid client;

        Library(int me, List<Tid> replicas, Tid client) {
            this.me = me;
            this.replicas = replicas;
            this.client = client;
        }

        void loop() {
            int seq = 0;
            while (true) {
                MpMessage req = Channel.recv(model, m -> m instanceof Write || m instanceof Read || m instanceof Rmw);
                final Id id = new Id(me, seq++);
                Cmd c;
                if (req instanceof Write w) {
                    c = new Cmd(id, KIND_WRITE, w.value());
                } else if (req instanceof Read) {
                    c = new Cmd(id, KIND_READ, 0);
                } else {
                    c = new Cmd(id, KIND_RMW, 0);
                }
                // submit(c): line 2
                for (Tid r : replicas) {
                    Channel.send(model, r, new Propagate(c));
                }
                Reply reply = null;
                Map<Integer, Set<List<Id>>> fastPaths = new HashMap<>();
                Set<Integer> slow = new HashSet<>();
                while (!accepted(reply, fastPaths, slow)) {
                    MpMessage m = Channel.recv(model, x ->
                            (x instanceof Reply r && r.id().equals(id))
                                    || (x instanceof FastAck f && f.id().equals(id))
                                    || (x instanceof SlowAck s && s.id().equals(id)));
                    if (m instanceof Reply r) {
                        reply = r;
                    } else if (m instanceof FastAck f) {
                        fastPaths.put(f.from(), f.paths());
                    } else if (m instanceof SlowAck s) {
                        slow.add(s.from());
                    }
                }
                // response(id, r): line 5
                switch (c.kind()) {
                    case KIND_WRITE -> Channel.send(model, client, new Ack());
                    case KIND_READ -> Channel.send(model, client, new Value(reply.result()));
                    default -> Channel.send(model, client, new Result(reply.result()));
                }
            }
        }

        /** Lines 3-4. */
        boolean accepted(Reply reply, Map<Integer, Set<List<Id>>> fastPaths, Set<Integer> slow) {
            if (reply == null) {
                return false;
            }
            for (Set<Integer> qf : FAST_QUORUMS) {               // line 3
                boolean all = true;
                for (int f : qf) {
                    if (f == LEADER) {
                        continue;
                    }
                    if (!(reply.paths().equals(fastPaths.get(f)) || slow.contains(f))) {
                        all = false;
                    }
                }
                if (all) {
                    return true;
                }
            }
            for (Set<Integer> qs : SLOW_QUORUMS) {               // line 4
                boolean all = true;
                for (int f : qs) {
                    if (f != LEADER && !slow.contains(f)) {
                        all = false;
                    }
                }
                if (all) {
                    return true;
                }
            }
            return false;
        }
    }

    // ---------------------------------------------------------------- replica (Figure 4)

    private final class Replica {
        final int me;
        final List<Tid> replicas;
        final List<Tid> libs;

        final int bal = BALLOT;
        final Map<Id, Integer> phase = new HashMap<>();
        final Map<Id, Cmd> cmd = new HashMap<>();
        final Map<Id, Set<Id>> dep = new HashMap<>();
        final Map<Id, Set<List<Id>>> paths = new HashMap<>();
        /** Ids in the order they were committed; scanned for line 33. */
        final Set<Id> committed = new LinkedHashSet<>();
        final Set<Id> exec = new HashSet<>();
        final List<Cmd> pendingLog = new ArrayList<>();
        int state = 0;

        /** FastAcks of followers: id -> (follower -> D). */
        final Map<Id, Map<Integer, Set<Id>>> fastVotes = new HashMap<>();
        /** SlowAcks: id -> senders. */
        final Map<Id, Set<Integer>> slowVotes = new HashMap<>();
        /** Propagate and leader-FastAck messages whose precondition does not hold yet, in arrival order. */
        final List<MpMessage> waiting = new ArrayList<>();
        /** Ids in ACCEPT, in the order they were accepted. */
        final Set<Id> accepted = new LinkedHashSet<>();

        Replica(int me, List<Tid> replicas, List<Tid> libs) {
            this.me = me;
            this.replicas = replicas;
            this.libs = libs;
        }

        int phaseOf(Id id) {
            return phase.getOrDefault(id, START);
        }

        void loop() {
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof FastAck f && f.from() != LEADER) {
                    fastVotes.computeIfAbsent(f.id(), x -> new HashMap<>()).put(f.from(), f.deps());
                } else if (m instanceof SlowAck s) {
                    slowVotes.computeIfAbsent(s.id(), x -> new HashSet<>()).add(s.from());
                } else {
                    waiting.add(m);
                }
                fixpoint();
            }
        }

        /** Runs every enabled handler until none is enabled. */
        void fixpoint() {
            boolean progress = true;
            while (progress) {
                progress = tryCommit() || tryExec() || tryWaiting();
            }
        }

        boolean tryWaiting() {
            for (Iterator<MpMessage> it = waiting.iterator(); it.hasNext(); ) {
                MpMessage m = it.next();
                if (m instanceof Propagate pr) {
                    if (phaseOf(pr.cmd().id()) != START) {     // line 7 never true again: drop
                        it.remove();
                        continue;
                    }
                    it.remove();
                    onPropagate(pr.cmd());
                    return true;
                } else if (m instanceof FastAck f) {          // from the leader
                    int ph = phaseOf(f.id());
                    if (ph == ACCEPT || ph == COMMIT) {       // line 23 never true again: drop
                        it.remove();
                        continue;
                    }
                    if (f.ballot() != bal || ph != PREACCEPT) {
                        continue;
                    }
                    boolean depsOk = true;
                    for (Id d : f.deps()) {
                        int pd = phaseOf(d);
                        if (pd != ACCEPT && pd != COMMIT) {
                            depsOk = false;
                        }
                    }
                    if (!depsOk) {
                        continue;
                    }
                    it.remove();
                    onLeaderFastAck(f);
                    return true;
                } else {
                    it.remove();
                    throw new AssertionError("replica n" + me + " received an unexpected message " + m);
                }
            }
            return false;
        }

        /** Lines 6-21. */
        void onPropagate(Cmd c) {
            Id id = c.id();
            phase.put(id, PREACCEPT);
            cmd.put(id, c);
            if (fast(me)) {
                Set<Id> d = new HashSet<>();
                for (Map.Entry<Id, Cmd> e : cmd.entrySet()) {
                    if (!e.getKey().equals(id) && e.getValue().conflicts(c)) {
                        d.add(e.getKey());
                    }
                }
                Set<Id> dd = Set.copyOf(d);
                dep.put(id, dd);
                Set<List<Id>> p = pset(id);
                paths.put(id, p);
                Tid lib = libs.get(id.client());
                if (me == LEADER) {
                    pendingLog.add(c);
                    int r = optExec();
                    Channel.send(model, lib, new Reply(bal, id, p, r));
                    FastAck fa = new FastAck(bal, id, dd, p, me);
                    for (Tid t : replicas) {
                        Channel.send(model, t, fa);
                    }
                } else {
                    FastAck fa = new FastAck(bal, id, dd, p, me);
                    for (Tid t : replicas) {
                        Channel.send(model, t, fa);
                    }
                    Channel.send(model, lib, fa);
                }
            }
        }

        /** Lines 22-29. */
        void onLeaderFastAck(FastAck f) {
            Id id = f.id();
            phase.put(id, ACCEPT);
            accepted.add(id);
            if ((fast(me) && !f.deps().equals(dep.get(id))) || slowOnly(me)) {
                dep.put(id, f.deps());
                SlowAck s = new SlowAck(bal, id, me);
                for (Tid t : replicas) {
                    Channel.send(model, t, s);
                }
                Channel.send(model, libs.get(id.client()), s);
            } else if (fast(me) && !f.paths().equals(paths.get(id))) {
                Channel.send(model, libs.get(id.client()), new SlowAck(bal, id, me));
            }
        }

        /** Lines 30-32. */
        boolean tryCommit() {
            for (Iterator<Id> it = accepted.iterator(); it.hasNext(); ) {
                Id id = it.next();
                Set<Id> d = dep.get(id);
                boolean depsCommitted = true;
                for (Id x : d) {
                    if (phaseOf(x) != COMMIT) {
                        depsCommitted = false;
                    }
                }
                if (!depsCommitted) {
                    continue;
                }
                Map<Integer, Set<Id>> fv = fastVotes.getOrDefault(id, Map.of());
                Set<Integer> sv = slowVotes.getOrDefault(id, Set.of());
                if (quorumAcked(d, fv, sv)) {
                    it.remove();
                    phase.put(id, COMMIT);
                    committed.add(id);
                    return true;
                }
            }
            return false;
        }

        boolean quorumAcked(Set<Id> d, Map<Integer, Set<Id>> fv, Set<Integer> sv) {
            for (Set<Integer> qf : FAST_QUORUMS) {
                boolean all = true;
                for (int f : qf) {
                    if (f != LEADER && !(d.equals(fv.get(f)) || sv.contains(f))) {
                        all = false;
                    }
                }
                if (all) {
                    return true;
                }
            }
            for (Set<Integer> qs : SLOW_QUORUMS) {
                boolean all = true;
                for (int f : qs) {
                    if (f != LEADER && !sv.contains(f)) {
                        all = false;
                    }
                }
                if (all) {
                    return true;
                }
            }
            return false;
        }

        /** Lines 33-36. */
        boolean tryExec() {
            for (Id id : committed) {
                if (exec.contains(id) || !exec.containsAll(dep.get(id))) {
                    continue;
                }
                state = apply(cmd.get(id), state)[0];
                exec.add(id);
                if (me == LEADER) {
                    pendingLog.removeIf(c -> c.id().equals(id));
                }
                return true;
            }
            return false;
        }

        /** opt_exec(pending_log, state) (line 17, §4.4): the last command's result over a copy of state. */
        int optExec() {
            int x = state;
            int r = 0;
            for (Cmd c : pendingLog) {
                int[] xr = apply(c, x);
                x = xr[0];
                r = xr[1];
            }
            return r;
        }

        /** pset (lines 37-39) over the current dep array. */
        Set<List<Id>> pset(Id id) {
            Set<List<Id>> out = new LinkedHashSet<>();
            walk(id, new ArrayList<>(), out);
            return Set.copyOf(out);
        }

        void walk(Id id, List<Id> prefix, Set<List<Id>> out) {
            if (prefix.contains(id)) {
                throw new AssertionError("dependency cycle at n" + me + " through " + id);
            }
            prefix.add(id);
            Set<Id> d = dep.get(id);
            if (d == null) {
                throw new AssertionError("pset over an unknown dependency " + id + " at n" + me);
            }
            if (d.isEmpty()) {
                out.add(List.copyOf(prefix));
            } else {
                List<Id> sorted = new ArrayList<>(d);
                sorted.sort(null);
                for (Id x : sorted) {
                    walk(x, prefix, out);
                }
            }
            prefix.remove(prefix.size() - 1);
        }
    }

    /** The state machine: returns {new x, result}. */
    static int[] apply(Cmd c, int x) {
        switch (c.kind()) {
            case KIND_WRITE:
                return new int[] {c.value(), 0};
            case KIND_READ:
                return new int[] {x, x};
            default:
                return new int[] {x + 10, x + 10};
        }
    }

    @Override
    public String toString() {
        return "SwiftpaxosPaper(q=" + q + ", " + modelName + ")";
    }
}
