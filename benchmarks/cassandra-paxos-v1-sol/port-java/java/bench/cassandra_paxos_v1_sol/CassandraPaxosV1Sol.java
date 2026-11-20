package bench.cassandra_paxos_v1_sol;

import java.util.ArrayList;
import java.util.List;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Cassandra 4.1 legacy Paxos on a single integer register. See NOTES.md. */
public final class CassandraPaxosV1Sol implements MpProgram {
    private final int q;
    private final boolean linearizable;
    private final String variant;
    private final CommunicationModel communication;

    public CassandraPaxosV1Sol(String... args) {
        if (args.length != 4 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<q>,<v1|v1nolin>,<fifo|bag>");
        q = Integer.parseInt(args[1]);
        if (q < 1 || !(args[2].equals("v1") || args[2].equals("v1nolin")))
            throw new IllegalArgumentException("invalid size or variant");
        variant = args[2];
        linearizable = variant.equals("v1");
        communication = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("invalid delivery model");
        };
    }
    public String programName() { return "cassandra_paxos_v1"; }
    public String size() { return "q=" + q + ",variant=" + variant; }
    public CommunicationModel model() { return communication; }
    public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    public List<String> visibleRoles() { return List.of("c0", "c1"); }
    public String toString() { return "CassandraPaxosV1Sol(impl," + q + "," + variant + "," + communication + ")"; }

    @Tag("Cas") public record Cas(int client, int expected, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("CasResult") public record CasResult(int client, int applied, int value) implements MpMessage {}
    @Tag("Value") public record Value(int client, int value) implements MpMessage {}
    @Tag("Failed") public record Failed(int client) implements MpMessage {}
    @Tag("Ballot") public record Ballot(int counter, int replica) implements MpMessage, Comparable<Ballot> {
        public int compareTo(Ballot b) {
            int c = Integer.compare(counter, b.counter);
            return c != 0 ? c : Integer.compare(replica, b.replica);
        }
    }
    private static final Ballot ZERO = new Ballot(0, -1);
    @Tag("Commit") public record Commit(Ballot ballot, boolean empty, int value) implements MpMessage {}
    private static final Commit NONE = new Commit(ZERO, true, 0);
    @Tag("Prepare") public record Prepare(int from, int round, Ballot ballot) implements MpMessage {}
    @Tag("Promise") public record Promise(int from, int round, boolean promised, Commit accepted, Commit committed) implements MpMessage {}
    @Tag("Propose") public record Propose(int from, int round, Commit proposal) implements MpMessage {}
    // The witness supplements Cassandra's Boolean response for the interface's seen-ballot clock.
    @Tag("Accepted") public record Accepted(int from, int round, boolean ok, Ballot witness) implements MpMessage {}
    @Tag("Learn") public record Learn(int from, int round, Commit proposal, boolean acknowledge) implements MpMessage {}
    @Tag("Ack") public record Ack(int from, int round) implements MpMessage {}
    @Tag("Fetch") public record Fetch(int from, int round) implements MpMessage {}
    @Tag("Data") public record Data(int from, int round, Commit stored) implements MpMessage {}

    public void run() {
        // Allocate all identifiers before starting any process. Only immutable addresses are shared.
        Node[] nodes = { new Node(0), new Node(1), new Node(2) };
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < 3; i++) threads[i] = new MpThread(nodes[i]::run, "n" + i);
        for (int i = 0; i < 2; i++) {
            final int client = i;
            threads[3 + i] = new MpThread(() -> client(client, q, nodes[client].nodes.get(client)), "c" + i);
        }
        List<Tid> replicas = List.of(Tid.of(threads[0].getTaskId(), "n0"),
                Tid.of(threads[1].getTaskId(), "n1"), Tid.of(threads[2].getTaskId(), "n2"));
        List<Tid> clients = List.of(Tid.of(threads[3].getTaskId(), "c0"), Tid.of(threads[4].getTaskId(), "c1"));
        for (Node n : nodes) { n.nodes = replicas; n.clients = clients; }
        for (MpThread t : threads) t.start();
        // Await clients first; replica joins block forever after all messages drain.
        try {
            threads[3].join1(); threads[4].join1();
            for (int i = 0; i < 3; i++) threads[i].join1();
        } catch (InterruptedException e) { throw new IllegalStateException(e); }
    }
    private void send(Tid to, MpMessage m) { Channel.send(communication, to, m); }
    private void client(int i, int count, Tid node) {
        int known = 0;
        for (int j = 0; j < count; j++) {
            send(node, (i + j) % 2 == 0 ? new Cas(i, known, 1 + i + 2 * j) : new Read(i));
            MpMessage reply = Channel.recv(communication);
            if (reply instanceof CasResult r) known = r.value;
            else if (reply instanceof Value r) known = r.value;
            else if (!(reply instanceof Failed)) throw new AssertionError("invalid client reply");
        }
    }
    private static Ballot max(Ballot a, Ballot b) { return a.compareTo(b) >= 0 ? a : b; }
    private static Commit latest(Commit a, Commit b) { return a.ballot.compareTo(b.ballot) >= 0 ? a : b; }

    private final class Node {
        final int id;
        List<Tid> nodes, clients;
        Ballot promised = ZERO;
        Commit accepted = NONE, committed = NONE, stored = NONE;
        int seen, sequence;
        Node(int id) { this.id = id; }
        void observe(Ballot b) { seen = Math.max(seen, b.counter); }
        Ballot latestWitnessed() { return max(promised, max(accepted.ballot, committed.ballot)); }
        void broadcast(MpMessage m) { for (Tid to : nodes) send(to, m); }
        void run() {
            while (true) {
                MpMessage m = Channel.recv(communication);
                if (m instanceof Cas || m instanceof Read) coordinate(m);
                else handle(m);
            }
        }
        // Also used while the coordinator awaits a callback: replica service never stops.
        void handle(MpMessage m) {
            if (m instanceof Prepare p) {
                observe(p.ballot);
                boolean ok = p.ballot.compareTo(latestWitnessed()) > 0;
                Commit previous = ok ? accepted : new Commit(promised, true, 0);
                if (ok) promised = p.ballot;
                send(nodes.get(p.from), new Promise(id, p.round, ok, previous, committed));
            } else if (m instanceof Propose p) {
                observe(p.proposal.ballot);
                boolean ok = p.proposal.ballot.compareTo(latestWitnessed()) >= 0;
                if (ok && !p.proposal.ballot.equals(committed.ballot)) accepted = p.proposal;
                send(nodes.get(p.from), new Accepted(id, p.round, ok, latestWitnessed()));
            } else if (m instanceof Learn l) {
                observe(l.proposal.ballot);
                // Mutation timestamps and Paxos metadata both resist delayed older commits.
                if (!l.proposal.empty) stored = latest(stored, l.proposal);
                committed = latest(committed, l.proposal);
                if (accepted.ballot.compareTo(committed.ballot) <= 0) accepted = NONE;
                if (l.acknowledge) send(nodes.get(l.from), new Ack(id, l.round));
            } else if (m instanceof Fetch f) {
                send(nodes.get(f.from), new Data(id, f.round, stored));
            } else if (m instanceof Promise p) {
                observe(p.accepted.ballot); observe(p.committed.ballot);
            } else if (m instanceof Accepted a) observe(a.witness);
            else if (m instanceof Data d) observe(d.stored.ballot);
            // Unmatched replies (including Ack) belong to callbacks already completed.
        }
        MpMessage response(int round, Class<? extends MpMessage> type) {
            while (true) {
                MpMessage m = Channel.recv(communication);
                // Observe even replies that finish the current callback.
                if (m instanceof Promise p) { observe(p.accepted.ballot); observe(p.committed.ballot); }
                if (m instanceof Accepted a) observe(a.witness);
                if (m instanceof Data d) observe(d.stored.ballot);
                int r = m instanceof Promise p ? p.round : m instanceof Accepted a ? a.round
                        : m instanceof Ack a ? a.round : m instanceof Data d ? d.round : -1;
                if (r == round && type.isInstance(m)) return m;
                handle(m);
            }
        }
        List<Promise> preparePaxos(Ballot ballot) {
            int r = ++sequence;
            broadcast(new Prepare(id, r, ballot));
            List<Promise> promises = new ArrayList<>();
            while (promises.size() < 2) {
                Promise p = (Promise) response(r, Promise.class);
                if (!p.promised) return List.of(); // first rejection releases PrepareCallback
                promises.add(p);
            }
            return promises;
        }
        // 1 = quorum; 0 = retry; -1 = partial acceptance / unknown client outcome.
        int proposePaxos(Commit proposal, boolean repair) {
            int r = ++sequence, accepts = 0;
            broadcast(new Propose(id, r, proposal));
            for (int received = 1; received <= 3; received++) {
                if (((Accepted) response(r, Accepted.class)).ok) accepts++;
                if (accepts >= 2) return 1;
                if (repair && accepts + 3 - received < 2) return 0;
            }
            return accepts == 0 ? 0 : -1;
        }
        void commitPaxos(Commit proposal) {
            int r = ++sequence;
            broadcast(new Learn(id, r, proposal, true));
            response(r, Ack.class); response(r, Ack.class);
        }
        boolean beginAndRepairPaxos(Ballot ballot) {
            List<Promise> ps = preparePaxos(ballot);
            if (ps.isEmpty()) return false;
            Commit progress = NONE, recent = NONE;
            for (Promise p : ps) { progress = latest(progress, p.accepted); recent = latest(recent, p.committed); }
            if (!progress.empty && progress.ballot.compareTo(recent.ballot) > 0) {
                Commit refreshed = new Commit(ballot, false, progress.value);
                if (proposePaxos(refreshed, true) == 1) commitPaxos(refreshed);
                return false; // completion consumes an attempt even when successful
            }
            boolean missing = false;
            for (Promise p : ps) if (!p.committed.ballot.equals(recent.ballot)) {
                send(nodes.get(p.from), new Learn(id, ++sequence, recent, false));
                missing = true;
            }
            return !missing; // fire-and-forget repair, then fresh prepare
        }
        int quorumRead() {
            int r = ++sequence;
            // No snitch in this model: choose any pair as the two closest replicas.
            int omitted = Nondet.chooseIndex(3);
            for (int i = 0; i < 3; i++) if (i != omitted) send(nodes.get(i), new Fetch(id, r));
            Commit a = ((Data) response(r, Data.class)).stored;
            Commit b = ((Data) response(r, Data.class)).stored;
            return latest(a, b).value;
        }
        void coordinate(MpMessage request) {
            int client = request instanceof Cas c ? c.client : ((Read) request).client;
            for (int attempt = 0; attempt < 3; attempt++) {
                Ballot ballot = new Ballot(++seen, id);
                if (!beginAndRepairPaxos(ballot)) continue;
                if (request instanceof Read) {
                    if (linearizable) {
                        int result = proposePaxos(new Commit(ballot, true, 0), false);
                        if (result < 0) break;
                        if (result == 0) continue;
                    }
                    send(clients.get(client), new Value(client, quorumRead()));
                    return;
                }
                Cas cas = (Cas) request;
                int current = quorumRead();
                boolean applied = current == cas.expected;
                Commit proposal = new Commit(ballot, !applied, applied ? cas.value : 0);
                int result = proposePaxos(proposal, false);
                if (result < 0) break;
                if (result == 0) continue;
                if (applied) commitPaxos(proposal);
                send(clients.get(client), new CasResult(client, applied ? 1 : 0, applied ? cas.value : current));
                return;
            }
            send(clients.get(client), new Failed(client));
        }
    }
}
