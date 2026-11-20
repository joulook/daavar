package bench.omnipaxos_spec;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * A replicated log over three nodes (n0, n1, n2) with two clients (c0, c1): a leader-based
 * Paxos-style protocol for a log (Sequence Paxos / Multi-Paxos). See NOTES.md for the protocol.
 *
 * <p>Arguments: {@code impl,<e>,<t>,<cand>,<fifo|bag>}.
 */
public final class OmniPaxosSpec implements MpProgram {

    private static final int N = 3;
    private static final int K = 2;

    private final int e;
    private final int t;
    private final int cand;
    private final CommunicationModel model;
    private final String modelName;

    // ---------------------------------------------------------------- messages

    /** client -> node: a value to append. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** node -> client: a value learned as decided, in decided order. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** candidate -> others: phase 1a for ballot b. */
    @Tag("Prepare")
    public record Prepare(int ballot) implements MpMessage {}

    /** node -> candidate: phase 1b; the promiser's accepted ballot and accepted log. */
    @Tag("Promise")
    public record Promise(int ballot, int accBallot, List<Integer> log) implements MpMessage {}

    /** leader -> followers: phase 2a; the leader's whole log at ballot b and its decided length. */
    @Tag("Accept")
    public record Accept(int ballot, List<Integer> log, int decidedLen) implements MpMessage {}

    /** follower -> leader: phase 2b; the follower accepted a log of length len at ballot b. */
    @Tag("Accepted")
    public record Accepted(int ballot, int from, int len) implements MpMessage {}

    /** leader -> followers: the decided prefix. */
    @Tag("Decide")
    public record Decide(List<Integer> prefix) implements MpMessage {}

    /** node -> leader of ballot b: a client value to be proposed by that leader. */
    @Tag("Forward")
    public record Forward(int ballot, int value) implements MpMessage {}

    // ---------------------------------------------------------------- construction

    /**
     * @param args {@code impl, e, t, cand, fifo|bag}
     */
    public OmniPaxosSpec(String... args) {
        if (args.length != 5 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<t>,<cand>,<fifo|bag>");
        }
        this.e = Integer.parseInt(args[1]);
        this.t = Integer.parseInt(args[2]);
        this.cand = Integer.parseInt(args[3]);
        this.modelName = args[4];
        if (e < 1 || t < 1 || cand < 0 || cand > N) {
            throw new IllegalArgumentException("need e >= 1, t >= 1, 0 <= cand <= 3");
        }
        switch (modelName) {
            case "fifo" -> this.model = CommunicationModel.P2P;
            case "bag" -> this.model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + modelName);
        }
    }

    @Override
    public String programName() {
        return "omnipaxos-spec";
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
        return List.of("n0", "n1", "n2", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    // ---------------------------------------------------------------- main

    @Override
    public void run() {
        // All addresses are fixed before any process starts: the threads are created (which mints
        // their task ids) first, and started afterwards, nodes in index order then clients.
        final Tid[] nodes = new Tid[N];
        final Tid[] clients = new Tid[K];
        MpThread[] nodeThreads = new MpThread[N];
        MpThread[] clientThreads = new MpThread[K];
        for (int i = 0; i < N; i++) {
            final int id = i;
            nodeThreads[i] = new MpThread(() -> new Node(id, nodes, clients).loop(), "n" + i);
            nodes[i] = Tid.of(nodeThreads[i].getTaskId(), "n" + i);
        }
        for (int i = 0; i < K; i++) {
            final int id = i;
            clientThreads[i] = new MpThread(() -> client(id, nodes[id]), "c" + i);
            clients[i] = Tid.of(clientThreads[i].getTaskId(), "c" + i);
        }
        for (MpThread th : nodeThreads) {
            th.start();
        }
        for (MpThread th : clientThreads) {
            th.start();
        }
        try {
            for (MpThread th : clientThreads) {
                th.join1();
            }
            // The nodes never end; awaiting them leaves the execution blocked once they all are.
            for (MpThread th : nodeThreads) {
                th.join1();
            }
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    // ---------------------------------------------------------------- client

    private void client(int i, Tid node) {
        for (int j = 0; j < e; j++) {
            Channel.send(model, node, new Propose(1 + i + 2 * j));
        }
        int seen = 0;
        while (seen < e) {
            Committed c = (Committed) Channel.recv(model);
            int v = c.value();
            // mine_i = { 1 + i + 2j : 0 <= j < e }
            if (v >= 1 && (v - 1 - i) % 2 == 0 && (v - 1 - i) / 2 < e && v - 1 - i >= 0) {
                seen++;
            }
        }
    }

    // ---------------------------------------------------------------- node

    private enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final class Node {
        final int id;
        final Tid[] nodes;
        final Tid[] clients;
        /** Remaining election timeouts. */
        int budget;
        /** Highest ballot promised (0 = none). Ballot b belongs to node b % 3. */
        int prom = 0;
        /** Ballot at which {@link #log} was accepted (0 = none). */
        int acc = 0;
        /** Accepted log. */
        List<Integer> log = new ArrayList<>();
        /** Decided prefix learned so far. */
        final List<Integer> decided = new ArrayList<>();
        /** Ballot of the leader this node knows of (0 = no leader). */
        int leaderBallot = 0;
        Role role = Role.FOLLOWER;
        /** Client values held while no leader is known. */
        final List<Integer> pending = new ArrayList<>();
        /** Leader only: per follower, the longest log length it accepted at the current ballot. */
        final int[] acceptedLen = new int[N];

        Node(int id, Tid[] nodes, Tid[] clients) {
            this.id = id;
            this.nodes = nodes;
            this.clients = clients;
            this.budget = id < cand ? t : 0;
        }

        void loop() {
            while (true) {
                if (leaderBallot == 0 && budget > 0 && Nondet.chooseBoolean()) {
                    budget--;
                    timeout();
                    continue;
                }
                handle(Channel.recv(model));
            }
        }

        void sendOthers(MpMessage m) {
            for (int j = 0; j < N; j++) {
                if (j != id) {
                    Channel.send(model, nodes[j], m);
                }
            }
        }

        void timeout() {
            int b = (prom / N + 1) * N + id;
            prom = b;
            role = Role.CANDIDATE;
            leaderBallot = 0;
            sendOthers(new Prepare(b));
        }

        void handle(MpMessage m) {
            if (m instanceof Propose p) {
                onClientValue(p.value());
            } else if (m instanceof Prepare p) {
                if (p.ballot() > prom) {
                    prom = p.ballot();
                    role = Role.FOLLOWER;
                    leaderBallot = 0;
                    Channel.send(model, nodes[p.ballot() % N], new Promise(p.ballot(), acc, List.copyOf(log)));
                }
            } else if (m instanceof Promise p) {
                if (role == Role.CANDIDATE && p.ballot() == prom) {
                    becomeLeader(p);
                }
            } else if (m instanceof Accept a) {
                onAccept(a);
            } else if (m instanceof Accepted a) {
                onAccepted(a);
            } else if (m instanceof Decide d) {
                learn(d.prefix());
            } else if (m instanceof Forward f) {
                if (role == Role.LEADER && f.ballot() == prom) {
                    append(f.value());
                }
                // otherwise dropped: re-routing it could reorder a client's values
            } else {
                throw new IllegalStateException("node n" + id + ": unexpected message " + m);
            }
        }

        void onClientValue(int v) {
            if (role == Role.LEADER) {
                append(v);
            } else if (leaderBallot != 0) {
                Channel.send(model, nodes[leaderBallot % N], new Forward(leaderBallot, v));
            } else {
                pending.add(v);
            }
        }

        void becomeLeader(Promise p) {
            // Majority = this node + one promiser: adopt the log of the higher accepted ballot,
            // the longer one on a tie (logs accepted at one ballot are prefixes of each other).
            if (p.accBallot() > acc || (p.accBallot() == acc && p.log().size() > log.size())) {
                log = new ArrayList<>(p.log());
            }
            checkExtendsDecided(log);
            role = Role.LEADER;
            acc = prom;
            leaderBallot = prom;
            java.util.Arrays.fill(acceptedLen, 0);
            log.addAll(pending);
            pending.clear();
            sendOthers(new Accept(prom, List.copyOf(log), decided.size()));
        }

        void append(int v) {
            log.add(v);
            sendOthers(new Accept(prom, List.copyOf(log), decided.size()));
        }

        void onAccept(Accept a) {
            int b = a.ballot();
            if (b < prom || (b == prom && role != Role.FOLLOWER)) {
                // stale; or b is this node's own ballot (it is its candidate or leader), which
                // only this node sends
                return;
            }
            boolean newLeader = leaderBallot != b;
            prom = b;
            role = Role.FOLLOWER;
            leaderBallot = b;
            if (acc != b || a.log().size() > log.size()) {
                log = new ArrayList<>(a.log());
                acc = b;
            }
            Channel.send(model, nodes[b % N], new Accepted(b, id, log.size()));
            learn(log.subList(0, a.decidedLen()));
            if (newLeader) {
                for (int v : pending) {
                    Channel.send(model, nodes[b % N], new Forward(b, v));
                }
                pending.clear();
            }
        }

        void onAccepted(Accepted a) {
            if (role != Role.LEADER || a.ballot() != prom) {
                return;
            }
            acceptedLen[a.from()] = Math.max(acceptedLen[a.from()], a.len());
            int dec = 0;
            for (int j = 0; j < N; j++) {
                if (j != id) {
                    dec = Math.max(dec, acceptedLen[j]);
                }
            }
            if (dec > decided.size()) {
                List<Integer> prefix = List.copyOf(log.subList(0, dec));
                learn(prefix);
                sendOthers(new Decide(prefix));
            }
        }

        void learn(List<Integer> prefix) {
            if (prefix.size() <= decided.size()) {
                checkConsistent(prefix);
                return;
            }
            checkExtendsDecided(prefix);
            for (int i = decided.size(); i < prefix.size(); i++) {
                int v = prefix.get(i);
                decided.add(v);
                if (id < K) {
                    Channel.send(model, clients[id], new Committed(v));
                }
            }
        }

        void checkExtendsDecided(List<Integer> l) {
            if (l.size() < decided.size() || !l.subList(0, decided.size()).equals(decided)) {
                throw new AssertionError("n" + id + ": log " + l + " does not extend decided " + decided);
            }
        }

        void checkConsistent(List<Integer> prefix) {
            if (!decided.subList(0, prefix.size()).equals(prefix)) {
                throw new AssertionError("n" + id + ": decided " + decided + " conflicts with " + prefix);
            }
        }
    }

    @Override
    public String toString() {
        return "OmniPaxosSpec(impl," + e + "," + t + "," + cand + "," + modelName + ")";
    }
}
