package bench.omnipaxos_paper_sol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
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

/** Fixed-membership Sequence Paxos and Ballot Leader Election (paper Figures 3 and 4). */
public final class OmniPaxosPaperSol implements MpProgram {
    private static final int N = 3;
    private final int e, t, cand;
    private final CommunicationModel communication;
    private final String delivery;

    public OmniPaxosPaperSol(String... args) {
        if (args.length != 4 && args.length != 5)
            throw new IllegalArgumentException("impl,<e>,<t>,[<cand>,]<fifo|bag>");
        if (!args[0].equals("impl")) throw new IllegalArgumentException("expected impl");
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = args.length == 5 ? Integer.parseInt(args[3]) : N;
        delivery = args[args.length - 1];
        if (e < 1 || e > Integer.MAX_VALUE / 2 || t < 1 || cand < 0 || cand > N)
            throw new IllegalArgumentException("invalid size parameters");
        communication = switch (delivery) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    @Override public String programName() { return "omnipaxos_paper_sol"; }
    @Override public String size() { return "e=" + e + ",t=" + t + ",cand=" + cand; }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "c0", "c1");
    }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() {
        return "OmniPaxosPaperSol(impl," + e + "," + t + "," + cand + "," + delivery + ")";
    }

    @Tag("Propose") public record Propose(int value) implements MpMessage {}
    @Tag("Committed") public record Committed(int value) implements MpMessage {}
    @Tag("Ballot") public record Ballot(long sequence, int pid) implements MpMessage,
            Comparable<Ballot> {
        @Override public int compareTo(Ballot b) {
            int c = Long.compare(sequence, b.sequence);
            return c != 0 ? c : Integer.compare(pid, b.pid);
        }
    }
    private static final Ballot ZERO = new Ballot(-1, 0);
    @Tag("HBRequest") public record HBRequest(long round) implements MpMessage {}
    @Tag("HBReply") public record HBReply(long round, Ballot ballot, boolean qc)
            implements MpMessage {}
    @Tag("Prepare") public record Prepare(Ballot ballot) implements MpMessage {}
    @Tag("Promise") public record Promise(Ballot ballot, Ballot acceptedRound,
            List<Integer> log, int decided) implements MpMessage {
        public Promise { log = List.copyOf(log); }
    }
    @Tag("AcceptSync") public record AcceptSync(Ballot ballot, List<Integer> log)
            implements MpMessage {
        public AcceptSync { log = List.copyOf(log); }
    }
    @Tag("Accept") public record Accept(Ballot ballot, int value) implements MpMessage {}
    @Tag("Accepted") public record Accepted(Ballot ballot, int length) implements MpMessage {}
    @Tag("Decide") public record Decide(Ballot ballot, int length) implements MpMessage {}
    @Tag("Forward") public record Forward(int value) implements MpMessage {}
    @Tag("Wire") public record Wire(Tid from, long sequence, MpMessage body)
            implements MpMessage {}

    @Override public void run() {
        // Construct every address before starting any body. Only addresses are shared.
        Tid[] addresses = new Tid[5];
        MpThread[] processes = new MpThread[5];
        for (int i = 0; i < 5; i++) {
            final int role = i;
            String name = i < N ? "n" + i : "c" + (i - N);
            processes[i] = new MpThread(() -> {
                if (role < N) new Node(role, addresses).run();
                else client(role - N, addresses);
            }, name);
            addresses[i] = Tid.of(processes[i].getTaskId(), name);
        }
        for (MpThread process : processes) process.start();
        for (MpThread process : processes) {
            try { process.join1(); }
            catch (InterruptedException ex) { throw new IllegalStateException(ex); }
        }
    }

    private void client(int id, Tid[] addresses) {
        for (int j = 0; j < e; j++)
            Channel.send(communication, addresses[id], new Propose(1 + id + 2 * j));
        Set<Integer> seen = new LinkedHashSet<>();
        while (seen.size() < e) {
            int value = Channel.recv(communication, Committed.class).value();
            if (value >= 1 + id && (value - 1 - id) % 2 == 0
                    && (value - 1 - id) / 2 < e) seen.add(value);
        }
    }

    private final class Node {
        final int id;
        final Tid[] addresses;
        final long[] sent = new long[N], received = new long[N];
        final List<TreeMap<Long, MpMessage>> inbox = new ArrayList<>();
        int budget;
        // BLE state, separate from the replication promise and accepted ballot.
        long heartbeatRound;
        Ballot ballot, elected = ZERO;
        boolean qc = true;
        final HBReply[] heartbeats = new HBReply[N];
        // Sequence Paxos state; indices are prefix lengths, initially zero.
        Ballot promised = ZERO, acceptedRound = ZERO, current = ZERO;
        int leader = -1, decided;
        boolean leading, acceptPhase;
        List<Integer> log = new ArrayList<>();
        final Promise[] promises = new Promise[N];
        final int[] accepted = new int[N];
        final Set<Integer> pending = new LinkedHashSet<>();
        final Set<Integer> buffer = new LinkedHashSet<>();

        Node(int id, Tid[] addresses) {
            this.id = id;
            this.addresses = addresses;
            budget = id < cand ? t : 0;
            // Stable unique process identifiers; n0 has highest initial priority.
            ballot = new Ballot(0, N - id);
            for (int i = 0; i < N; i++) inbox.add(new TreeMap<>());
        }

        void send(int to, MpMessage body) {
            Channel.send(communication, addresses[to],
                    new Wire(addresses[id], sent[to]++, body));
        }

        void heartbeatRequests() {
            for (int p = 0; p < N; p++) if (p != id)
                send(p, new HBRequest(heartbeatRound));
        }

        void run() {
            heartbeatRequests();
            while (true) {
                if (leader < 0 && budget > 0 && Nondet.chooseBoolean()) {
                    timeout();
                    continue;
                }
                MpMessage message = Channel.recv(communication);
                if (message instanceof Propose p) proposal(p.value());
                else if (message instanceof Wire w) {
                    int from = nodeIndex(w.from());
                    inbox.get(from).put(w.sequence(), w.body());
                    while (inbox.get(from).containsKey(received[from])) {
                        MpMessage next = inbox.get(from).remove(received[from]++);
                        handle(from, next);
                    }
                } else throw new IllegalStateException("unexpected node message " + message);
            }
        }

        int nodeIndex(Tid tid) {
            for (int p = 0; p < N; p++) if (addresses[p].equals(tid)) return p;
            throw new IllegalArgumentException("unknown node");
        }

        void timeout() {
            budget--;
            heartbeats[id] = new HBReply(heartbeatRound, ballot, qc);
            int replies = 0;
            Ballot maximum = ZERO;
            for (HBReply hb : heartbeats) if (hb != null) {
                replies++;
                if (hb.qc() && hb.ballot().compareTo(maximum) > 0) maximum = hb.ballot();
            }
            if (replies >= 2) {
                if (maximum.compareTo(elected) < 0 || maximum.equals(ZERO)) {
                    ballot = new Ballot(Math.max(ballot.sequence(), elected.sequence()) + 1,
                            ballot.pid());
                    qc = true;
                } else if (maximum.compareTo(elected) > 0) {
                    elected = maximum;
                    observeLeader(N - maximum.pid());
                    if (leader == id && elected.compareTo(promised) > 0) startPrepare(elected);
                }
            } else qc = false;
            Arrays.fill(heartbeats, null);
            heartbeatRound++;
            heartbeatRequests();
        }

        void observeLeader(int next) {
            if (leader == next) return;
            leader = next;
            if (next != id) {
                leading = false;
                buffer.clear();
                for (int value : pending) send(next, new Forward(value));
            }
        }

        void startPrepare(Ballot round) {
            leading = true;
            acceptPhase = false;
            current = promised = round;
            Arrays.fill(promises, null);
            Arrays.fill(accepted, 0);
            buffer.clear();
            buffer.addAll(pending);
            promises[id] = new Promise(round, acceptedRound, log, decided);
            for (int p = 0; p < N; p++) if (p != id) send(p, new Prepare(round));
        }

        void proposal(int value) {
            pending.add(value);
            if (leading) {
                if (log.contains(value)) return;
                if (!acceptPhase) buffer.add(value);
                else {
                    log.add(value);
                    accepted[id] = log.size();
                    for (int p = 0; p < N; p++) if (p != id && promises[p] != null)
                        send(p, new Accept(current, value));
                }
            } else if (leader >= 0) send(leader, new Forward(value));
        }

        void handle(int from, MpMessage message) {
            if (message instanceof HBRequest h) {
                send(from, new HBReply(h.round(), ballot, qc));
            } else if (message instanceof HBReply h) {
                if (h.round() == heartbeatRound) heartbeats[from] = h;
            } else if (message instanceof Prepare p) {
                if (p.ballot().compareTo(promised) < 0) return;
                promised = p.ballot();
                leading = false;
                acceptPhase = false;
                observeLeader(from);
                send(from, new Promise(promised, acceptedRound, log, decided));
            } else if (message instanceof Promise p) {
                if (!leading || !p.ballot().equals(current)) return;
                promises[from] = p;
                if (acceptPhase) sync(from);
                else {
                    int count = 0;
                    Promise best = null;
                    for (Promise candidate : promises) if (candidate != null) {
                        count++;
                        if (best == null || candidate.acceptedRound().compareTo(best.acceptedRound()) > 0
                                || (candidate.acceptedRound().equals(best.acceptedRound())
                                    && candidate.log().size() > best.log().size())) best = candidate;
                    }
                    if (count < 2) return;
                    replaceLog(best.log());
                    for (int value : buffer) if (!log.contains(value)) log.add(value);
                    buffer.clear();
                    acceptedRound = current;
                    accepted[id] = log.size();
                    acceptPhase = true;
                    for (int peer = 0; peer < N; peer++)
                        if (peer != id && promises[peer] != null) sync(peer);
                }
            } else if (message instanceof AcceptSync s) {
                if (leading || acceptPhase || !s.ballot().equals(promised)) return;
                replaceLog(s.log());
                acceptedRound = s.ballot();
                acceptPhase = true;
                send(from, new Accepted(promised, log.size()));
            } else if (message instanceof Accept a) {
                if (leading || !acceptPhase || !a.ballot().equals(promised)) return;
                log.add(a.value());
                send(from, new Accepted(promised, log.size()));
            } else if (message instanceof Accepted a) {
                if (!leading || !acceptPhase || !a.ballot().equals(current)) return;
                accepted[from] = Math.max(accepted[from], a.length());
                int[] lengths = accepted.clone();
                Arrays.sort(lengths);
                int chosen = lengths[1];
                if (chosen > decided) {
                    learn(chosen);
                    for (int p = 0; p < N; p++) if (p != id && promises[p] != null)
                        send(p, new Decide(current, decided));
                }
            } else if (message instanceof Decide d) {
                if (!leading && acceptPhase && d.ballot().equals(promised)) learn(d.length());
            } else if (message instanceof Forward f) {
                // A former leader retains an in-flight request; its ingress also
                // resubmits on a leader change. Do not bounce it around stale routes.
                if (leading) proposal(f.value());
                else pending.add(f.value());
            } else throw new IllegalStateException("unexpected protocol message " + message);
        }

        void sync(int peer) {
            send(peer, new AcceptSync(current, log));
            if (decided > promises[peer].decided()) send(peer, new Decide(current, decided));
        }

        void replaceLog(List<Integer> replacement) {
            if (replacement.size() < decided
                    || !replacement.subList(0, decided).equals(log.subList(0, decided)))
                throw new AssertionError("changed decided prefix at n" + id);
            log = new ArrayList<>(replacement);
        }

        void learn(int length) {
            if (length > log.size()) throw new AssertionError("decision beyond accepted log");
            while (decided < length) {
                int value = log.get(decided++);
                pending.remove(value);
                if (id < 2) Channel.send(communication, addresses[N + id], new Committed(value));
            }
        }
    }
}
