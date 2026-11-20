package bench.omnipaxos_derived_impl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * A replicated log over three nodes: leader-based, Paxos-style (ballot election by prepare/promise,
 * replication by accept/accepted of the leader's whole log, decision by majority, decided prefix
 * broadcast). See NOTES.md for the protocol and the decisions.
 *
 * <p>Argument string: {@code impl,<e>,<t>,<cand>,<fifo|bag>}.
 */
public final class OmniPaxosDerivedImpl implements MpProgram {

    static final int N = 3;
    static final int K = 2;
    static final String[] ROLES = {"n0", "n1", "n2", "c0", "c1"};

    // ---- messages (records: immutable, equality by value) ----

    /** client -> node. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** node -> client. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** A ballot: ordered by round, then by node index. */
    @Tag("Ballot")
    public record Ballot(int round, int node) {
        boolean gt(Ballot o) {
            return round > o.round || (round == o.round && node > o.node);
        }
    }

    /** node -> current leader: a value of the sender's own client. */
    @Tag("Forward")
    public record Forward(int value) implements MpMessage {}

    /** candidate -> other nodes. */
    @Tag("Prepare")
    public record Prepare(Ballot b) implements MpMessage {}

    /** node -> candidate of b. */
    @Tag("Promise")
    public record Promise(Ballot b, Ballot accRnd, List<Integer> log, int decidedLen) implements MpMessage {}

    /** leader of b -> other nodes: the leader's whole log under b. */
    @Tag("Accept")
    public record Accept(Ballot b, List<Integer> log) implements MpMessage {}

    /** node -> leader of b: the length of b's log the sender holds. */
    @Tag("Accepted")
    public record Accepted(Ballot b, int from, int len) implements MpMessage {}

    /** leader -> other nodes: the decided prefix. */
    @Tag("Decide")
    public record Decide(List<Integer> decided) implements MpMessage {}

    // ---- instance ----

    private final int e;
    private final int t;
    private final int cand;
    private final CommunicationModel model;
    private final String modelName;

    public OmniPaxosDerivedImpl(String... args) {
        if (args.length != 5 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<t>,<cand>,<fifo|bag>");
        }
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = Integer.parseInt(args[3]);
        modelName = args[4];
        model = switch (args[4]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[4]);
        };
        if (e < 1 || t < 1 || cand < 0 || cand > N) {
            throw new IllegalArgumentException("need e >= 1, t >= 1, 0 <= cand <= 3");
        }
    }

    @Override
    public String programName() {
        return "omnipaxos-derived-impl";
    }

    @Override
    public String size() {
        return "e=" + e + ",t=" + t + ",cand=" + cand;
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
        return "OmniPaxosDerivedImpl(impl," + e + "," + t + "," + cand + "," + modelName + ")";
    }

    /**
     * The address of process {@code k} (spawn position, 0..4), computed from the caller's own id
     * and spawn position: main spawns the five processes consecutively, so their task ids are
     * consecutive (checked by main after spawning).
     */
    static Tid addr(int myPos, int k) {
        long base = MpProcess.self().raw() - myPos;
        return Tid.of(base + k, ROLES[k]);
    }

    @Override
    public void run() {
        List<MpProcess.Handle> nodes = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int me = i;
            nodes.add(MpProcess.spawn(ROLES[i], () -> new Node(me).loop()));
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < K; i++) {
            final int me = i;
            clients.add(MpProcess.spawn(ROLES[N + i], () -> client(me)));
        }
        long base = nodes.get(0).tid().raw();
        for (int k = 0; k < N + K; k++) {
            MpProcess.Handle h = k < N ? nodes.get(k) : clients.get(k - N);
            if (h.tid().raw() != base + k) {
                throw new IllegalStateException("process task ids are not consecutive: " + ROLES[k]
                        + " is " + h.tid() + ", expected " + (base + k));
            }
        }
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        // The nodes never end: awaiting them ends the execution blocked (as the interface intends).
        for (MpProcess.Handle n : nodes) {
            n.await();
        }
    }

    // ---- client ----

    private void client(int i) {
        Tid node = addr(N + i, i);
        List<Integer> mine = new ArrayList<>();
        for (int j = 0; j < e; j++) {
            mine.add(1 + i + 2 * j);
        }
        for (int v : mine) {
            Channel.send(model, node, new Propose(v));
        }
        int seen = 0;
        while (seen < e) {
            Committed c = Channel.recv(model, Committed.class);
            if (mine.contains(c.value())) {
                seen++;
            }
        }
    }

    // ---- node ----

    private static final Ballot ZERO = new Ballot(0, -1);

    private final class Node {
        final int me;
        final Tid[] peers = new Tid[N];
        final Tid client;
        int budget;
        /** Highest ballot promised (or accepted, or started). */
        Ballot promised = ZERO;
        /** Ballot under which {@link #log} was accepted. */
        Ballot accRnd = ZERO;
        List<Integer> log = new ArrayList<>();
        /** The decided prefix learnt, delivered to the client in order. */
        List<Integer> decided = new ArrayList<>();
        /** Index of the known leader, or -1 for none. */
        int leader = -1;
        /** Whether this node is a candidate for {@link #promised} that has not yet won. */
        boolean candidate;
        /** Leader only: the longest b-log length each node is known to hold. */
        int[] accLen = new int[N];
        /** Own client's proposals received while no leader is known, in arrival order. */
        final List<Integer> pending = new ArrayList<>();

        Node(int me) {
            this.me = me;
            for (int k = 0; k < N; k++) {
                peers[k] = addr(me, k);
            }
            client = me < K ? addr(me, N + me) : null;
            budget = me < cand ? t : 0;
        }

        void toOthers(MpMessage m) {
            for (int k = 0; k < N; k++) {
                if (k != me) {
                    Channel.send(model, peers[k], m);
                }
            }
        }

        void loop() {
            while (true) {
                if (leader < 0 && budget > 0 && Nondet.chooseIndex(2) == 1) {
                    timeout();
                    continue;
                }
                MpMessage m = Channel.recv(model);
                if (m instanceof Propose p) {
                    onPropose(p.value());
                } else if (m instanceof Forward f) {
                    if (leader == me) {
                        append(List.of(f.value()));
                    }
                    // not the leader: the value is lost (re-forwarding could reorder a client's values)
                } else if (m instanceof Prepare p) {
                    onPrepare(p.b());
                } else if (m instanceof Promise p) {
                    onPromise(p);
                } else if (m instanceof Accept a) {
                    onAccept(a);
                } else if (m instanceof Accepted a) {
                    onAccepted(a);
                } else if (m instanceof Decide d) {
                    learn(d.decided());
                } else {
                    throw new IllegalStateException("unexpected message " + m);
                }
            }
        }

        void timeout() {
            budget--;
            promised = new Ballot(promised.round() + 1, me);
            candidate = true;
            leader = -1;
            toOthers(new Prepare(promised));
        }

        void onPropose(int v) {
            if (leader == me) {
                append(List.of(v));
            } else if (leader >= 0) {
                Channel.send(model, peers[leader], new Forward(v));
            } else {
                pending.add(v);
            }
        }

        /** Called when a leader becomes known: hand the buffered own values to it, in order. */
        void flushPending() {
            if (pending.isEmpty()) {
                return;
            }
            List<Integer> vs = List.copyOf(pending);
            pending.clear();
            if (leader == me) {
                append(vs);
            } else {
                for (int v : vs) {
                    Channel.send(model, peers[leader], new Forward(v));
                }
            }
        }

        void onPrepare(Ballot b) {
            if (!b.gt(promised)) {
                return; // stale or duplicate: ignored, no nack
            }
            promised = b;
            candidate = false;
            leader = -1;
            Channel.send(model, peers[b.node()], new Promise(b, accRnd, List.copyOf(log), decided.size()));
        }

        void onPromise(Promise p) {
            if (!candidate || !p.b().equals(promised)) {
                return; // late promise (already leader) or for an abandoned ballot
            }
            // Majority = this node + one promiser: adopt the log with the highest (accRnd, length).
            if (p.accRnd().gt(accRnd) || (p.accRnd().equals(accRnd) && p.log().size() > log.size())) {
                log = new ArrayList<>(p.log());
            }
            candidate = false;
            leader = me;
            accRnd = promised;
            accLen = new int[N];
            int commit = Math.min(Math.max(decided.size(), p.decidedLen()), log.size());
            accLen[me] = log.size();
            leaderCommit = commit;
            List<Integer> vs = List.copyOf(pending);
            pending.clear();
            log.addAll(vs);
            accLen[me] = log.size();
            toOthers(new Accept(promised, List.copyOf(log)));
        }

        /** Leader only: the largest prefix length known decided by this leader. */
        int leaderCommit;

        void append(List<Integer> vs) {
            log.addAll(vs);
            accLen[me] = log.size();
            toOthers(new Accept(promised, List.copyOf(log)));
        }

        void onAccept(Accept a) {
            Ballot b = a.b();
            if (b.gt(promised)) {
                promised = b;
                candidate = false;
            } else if (!b.equals(promised)) {
                return; // from an older ballot
            }
            if (leader == me) {
                return; // cannot happen: only the owner of promised sends Accept(promised)
            }
            if (!accRnd.equals(b) || a.log().size() > log.size()) {
                log = new ArrayList<>(a.log());
                accRnd = b;
            }
            boolean learnt = leader != b.node();
            leader = b.node();
            Channel.send(model, peers[b.node()], new Accepted(b, me, log.size()));
            if (learnt) {
                flushPending();
            }
        }

        void onAccepted(Accepted a) {
            if (leader != me || !a.b().equals(promised)) {
                return;
            }
            if (a.len() > accLen[a.from()]) {
                accLen[a.from()] = a.len();
            }
            // majority of 3: the leader plus any one other node
            int best = 0;
            for (int k = 0; k < N; k++) {
                if (k != me && accLen[k] > best) {
                    best = accLen[k];
                }
            }
            if (best > leaderCommit) {
                leaderCommit = best;
                List<Integer> d = List.copyOf(log.subList(0, best));
                learn(d);
                toOthers(new Decide(d));
            }
        }

        /** Merges a decided prefix and delivers the new entries to the client in order. */
        void learn(List<Integer> d) {
            if (d.size() <= decided.size()) {
                return;
            }
            for (int i = 0; i < decided.size(); i++) {
                if (!decided.get(i).equals(d.get(i))) {
                    throw new AssertionError("safety violation at node n" + me + ": decided " + decided
                            + " is not a prefix of " + d);
                }
            }
            for (int i = decided.size(); i < d.size(); i++) {
                decided.add(d.get(i));
                if (client != null) {
                    Channel.send(model, client, new Committed(d.get(i)));
                }
            }
        }
    }
}
