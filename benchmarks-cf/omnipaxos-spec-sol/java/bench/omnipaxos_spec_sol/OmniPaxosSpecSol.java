package bench.omnipaxos_spec_sol;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Full-sequence, majority-quorum Paxos with one outstanding append per leader. */
public final class OmniPaxosSpecSol implements MpProgram {
    private static final CommunicationModel FIFO = CommunicationModel.P2P;
    private final int e, t, cand;
    private final CommunicationModel transport;

    public OmniPaxosSpecSol(String... args) {
        if (args.length < 3 || args.length > 5 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,e,t[,cand[,fifo|bag]]");
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = args.length >= 4 ? Integer.parseInt(args[3]) : 3;
        String delivery = args.length == 5 ? args[4] : "fifo";
        if (e < 1 || e > Integer.MAX_VALUE / 2 || t < 1 || cand < 0 || cand > 3
                || !(delivery.equals("fifo") || delivery.equals("bag")))
            throw new IllegalArgumentException("invalid instance parameters");
        transport = delivery.equals("fifo") ? FIFO : CommunicationModel.ASYN;
    }

    @Override public String programName() { return "omnipaxos_spec_sol"; }
    @Override public String size() { return "e=" + e + ",t=" + t + ",cand=" + cand; }
    @Override public CommunicationModel model() { return transport; }
    @Override public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "c0", "c1");
    }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() { return programName() + "(" + size() + ")"; }

    @Tag("Propose") public record Propose(int value) implements MpMessage {}
    @Tag("Committed") public record Committed(int value) implements MpMessage {}
    @Tag("Prepare") public record Prepare(Tid from, long ballot) implements MpMessage {}
    @Tag("Promise") public record Promise(Tid from, long ballot, long acceptedBallot,
                                          List<Integer> log) implements MpMessage {
        public Promise { log = List.copyOf(log); }
    }
    @Tag("Accept") public record Accept(Tid from, long ballot, List<Integer> log)
            implements MpMessage {
        public Accept { log = List.copyOf(log); }
    }
    @Tag("Accepted") public record Accepted(Tid from, long ballot, int length)
            implements MpMessage {}
    @Tag("Reject") public record Reject(long ballot) implements MpMessage {}
    @Tag("Forward") public record Forward(int value) implements MpMessage {}
    @Tag("Decide") public record Decide(List<Integer> log) implements MpMessage {
        public Decide { log = List.copyOf(log); }
    }

    @Override public void run() {
        // Construct every address before starting any process. Only this immutable directory
        // is shared; each Node's mutable state belongs exclusively to its own thread.
        Tid[] addresses = new Tid[5];
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < 3; i++) {
            Node node = new Node(i, addresses);
            threads[i] = new MpThread(node, "n" + i);
            addresses[i] = Tid.of(threads[i].getTaskId(), "n" + i);
        }
        for (int i = 0; i < 2; i++) {
            final int client = i;
            threads[3 + i] = new MpThread(() -> client(client, addresses), "c" + i);
            addresses[3 + i] = Tid.of(threads[3 + i].getTaskId(), "c" + i);
        }
        for (MpThread thread : threads) thread.start();
        try {
            for (MpThread thread : threads) thread.join1();
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void client(int id, Tid[] addresses) {
        for (int j = 0; j < e; j++)
            Channel.send(transport, addresses[id], new Propose(1 + id + 2 * j));
        Set<Integer> seen = new LinkedHashSet<>();
        while (seen.size() < e) {
            Committed m = Channel.recv(FIFO, Committed.class);
            if (m.value() >= 1 + id && (m.value() - 1 - id) % 2 == 0
                    && (m.value() - 1 - id) / 2 < e)
                seen.add(m.value());
        }
    }

    private final class Node implements Runnable {
        final int id;
        final Tid[] addresses;
        int budget;
        long promised = 0, acceptedBallot = 0, campaign = 0;
        Tid leader;
        boolean preparing, active, outstanding;
        List<Integer> accepted = List.of(), proposed = List.of(), learned = List.of();
        List<Integer> recovery = List.of();
        long recoveryBallot;
        final Set<Tid> promises = new LinkedHashSet<>(), acknowledgments = new LinkedHashSet<>();
        final Set<Integer> pending = new LinkedHashSet<>();

        Node(int id, Tid[] addresses) {
            this.id = id;
            this.addresses = addresses;
            budget = id < cand ? t : 0;
        }
        Tid self() { return addresses[id]; }
        void send(Tid to, MpMessage m) { Channel.send(transport, to, m); }
        void broadcast(MpMessage m) {
            for (int i = 0; i < 3; i++) if (i != id) send(addresses[i], m);
        }
        void stepDown(long ballot) {
            promised = ballot;
            preparing = active = outstanding = false;
            leader = null;
        }
        void election() {
            budget--;
            long ballot = (promised / 3 + 1) * 3 + id + 1;
            stepDown(ballot);
            campaign = ballot;
            preparing = true;
            promises.clear();
            promises.add(self());
            recoveryBallot = acceptedBallot;
            recovery = accepted;
            broadcast(new Prepare(self(), ballot));
        }
        @Override public void run() {
            while (true) {
                if (leader == null && budget > 0 && Nondet.chooseBoolean()) {
                    election();
                    continue;
                }
                MpMessage m = Channel.recv(transport);
                handle(m);
            }
        }
        void handle(MpMessage m) {
            if (m instanceof Propose p) {
                request(p.value());
            } else if (m instanceof Forward f) {
                // Retain locally, but never bounce stale forwards between former leaders.
                pending.add(f.value());
                pump();
            } else if (m instanceof Prepare p) {
                if (p.ballot() < promised) send(p.from(), new Reject(promised));
                else {
                    if (p.ballot() > promised) stepDown(p.ballot());
                    send(p.from(), new Promise(self(), p.ballot(), acceptedBallot, accepted));
                }
            } else if (m instanceof Promise p) {
                if (!preparing || p.ballot() != campaign || promised != campaign
                        || !promises.add(p.from())) return;
                if (p.acceptedBallot() > recoveryBallot
                        || (p.acceptedBallot() == recoveryBallot && p.log().size() > recovery.size())) {
                    recoveryBallot = p.acceptedBallot();
                    recovery = p.log();
                }
                if (promises.size() >= 2) {
                    preparing = false;
                    active = true;
                    leader = self();
                    proposed = recovery;
                    phaseTwo();
                }
            } else if (m instanceof Accept a) {
                if (a.ballot() < promised) { send(a.from(), new Reject(promised)); return; }
                if (a.ballot() > promised) stepDown(a.ballot());
                // A reordered, shorter Accept in the same ballot cannot truncate the log.
                if (acceptedBallot == a.ballot() && a.log().size() < accepted.size()) {
                    requirePrefix(a.log(), accepted);
                } else {
                    if (acceptedBallot == a.ballot()) requirePrefix(accepted, a.log());
                    acceptedBallot = a.ballot();
                    accepted = a.log();
                }
                boolean discovered = !a.from().equals(leader);
                leader = a.from();
                send(a.from(), new Accepted(self(), a.ballot(), accepted.size()));
                if (discovered) forwardPending();
            } else if (m instanceof Accepted a) {
                if (!active || !outstanding || campaign != promised || a.ballot() != campaign
                        || a.length() != proposed.size()) return;
                acknowledgments.add(a.from());
                if (acknowledgments.size() >= 2) {
                    outstanding = false;
                    learn(proposed);
                    broadcast(new Decide(proposed));
                    pump();
                }
            } else if (m instanceof Reject r) {
                if (r.ballot() > promised) stepDown(r.ballot());
            } else if (m instanceof Decide d) {
                // A valid decision remains valid after a higher promise; full prefix carries data.
                learn(d.log());
            } else throw new AssertionError("unexpected message " + m);
        }
        void request(int value) {
            pending.add(value);
            if (leader != null && !leader.equals(self())) send(leader, new Forward(value));
            pump();
        }
        void forwardPending() {
            for (int value : pending)
                if (!learned.contains(value)) send(leader, new Forward(value));
        }
        void pump() {
            if (!active || outstanding || campaign != promised) return;
            for (int value : pending) {
                // Values encode their client and ordinal. This also handles reordered forwards.
                if (!proposed.contains(value) && (value <= 2 || proposed.contains(value - 2))) {
                    ArrayList<Integer> next = new ArrayList<>(proposed);
                    next.add(value);
                    proposed = List.copyOf(next);
                    phaseTwo();
                    return;
                }
            }
        }
        void phaseTwo() {
            acceptedBallot = campaign;
            accepted = proposed;
            outstanding = true;
            acknowledgments.clear();
            acknowledgments.add(self());
            broadcast(new Accept(self(), campaign, proposed));
        }
        void learn(List<Integer> log) {
            requirePrefix(log.size() < learned.size() ? log : learned,
                          log.size() < learned.size() ? learned : log);
            if (log.size() <= learned.size()) return;
            int oldLength = learned.size();
            learned = log;
            for (int i = oldLength; i < log.size(); i++) {
                int value = log.get(i);
                if (value > 2 && !log.subList(0, i).contains(value - 2))
                    throw new AssertionError("client proposal order violated");
                if (log.subList(0, i).contains(value)) throw new AssertionError("duplicate value");
                if (id < 2) Channel.send(FIFO, addresses[3 + id], new Committed(value));
            }
        }
    }
    private static void requirePrefix(List<Integer> prefix, List<Integer> log) {
        if (prefix.size() > log.size() || !log.subList(0, prefix.size()).equals(prefix))
            throw new AssertionError("incompatible log prefixes");
    }
}
