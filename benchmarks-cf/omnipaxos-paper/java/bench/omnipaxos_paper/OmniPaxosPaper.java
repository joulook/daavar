package bench.omnipaxos_paper;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * Omni-Paxos log replication (Ng, Haridi, Carbone, EuroSys 2023): Sequence Paxos (section 4,
 * Figure 3) driven by Ballot Leader Election (section 5, Figure 4), no reconfiguration, over three
 * nodes n0..n2 and two clients c0, c1 (see INTERFACE.md). The circled numbers in the comments are
 * the handler numbers of Figure 3b (SP) and Figure 4 (BLE); NOTES.md maps them one by one.
 *
 * <p>Arguments: {@code impl,<e>,<t>,<cand>,<fifo|bag>}.
 */
public final class OmniPaxosPaper implements MpProgram {

    static final int N = 3;
    static final int MAJORITY = N / 2 + 1;
    static final String[] NODE_ROLES = {"n0", "n1", "n2"};
    static final String[] CLIENT_ROLES = {"c0", "c1"};
    static final int K = CLIENT_ROLES.length;

    private final int e;
    private final int t;
    private final int cand;
    private final String delivery;
    private final CommunicationModel model;

    public OmniPaxosPaper(String... args) {
        if (args.length != 5 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<t>,<cand>,<fifo|bag>");
        }
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = Integer.parseInt(args[3]);
        delivery = args[4];
        if (e < 1 || t < 1 || cand < 0 || cand > N) {
            throw new IllegalArgumentException("need e >= 1, t >= 1, 0 <= cand <= 3");
        }
        if (delivery.equals("fifo")) {
            model = CommunicationModel.P2P;
        } else if (delivery.equals("bag")) {
            model = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("delivery must be fifo or bag, was " + delivery);
        }
    }

    // ------------------------------------------------------------------ MpProgram

    @Override
    public String programName() {
        return "omnipaxos_paper";
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

    @Override
    public String toString() {
        return "OmniPaxosPaper(" + size() + "," + delivery + ")";
    }

    // ------------------------------------------------------------------ addresses

    /**
     * Addresses: main spawns the five processes back to back, so their task ids are consecutive
     * (main checks this after spawning). Each process derives the first id, {@code base}, from its
     * own id and its spawn position (D12).
     */
    static Tid nodeTid(long base, int i) {
        return Tid.of(base + i, NODE_ROLES[i]);
    }

    static Tid clientTid(long base, int j) {
        return Tid.of(base + N + j, CLIENT_ROLES[j]);
    }

    /** BLE pid of node n_i (NOTES.md, decision D1): n0 has the highest pid. */
    static int pidOf(int node) {
        return N - node;
    }

    static int nodeOfPid(int pid) {
        return N - pid;
    }

    // ------------------------------------------------------------------ program entry

    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int idx = i;
            handles.add(MpProcess.spawn(NODE_ROLES[i], () -> new Node(idx).run()));
        }
        for (int j = 0; j < K; j++) {
            final int idx = j;
            handles.add(MpProcess.spawn(CLIENT_ROLES[j], () -> client(idx)));
        }
        long base = handles.get(0).tid().raw();
        for (int k = 0; k < handles.size(); k++) {
            Tid expected = k < N ? nodeTid(base, k) : clientTid(base, k - N);
            if (!handles.get(k).tid().equals(expected)) {
                throw new IllegalStateException("process " + k + " is " + handles.get(k).tid()
                        + ", expected " + expected);
            }
        }
        // Clients first (they end), then the nodes (they never end: the execution ends blocked).
        for (int k = N; k < handles.size(); k++) {
            handles.get(k).await();
        }
        for (int k = 0; k < N; k++) {
            handles.get(k).await();
        }
    }

    private void client(int i) {
        final long base = MpProcess.self().raw() - (N + i);
        Set<Integer> own = new HashSet<>();
        for (int j = 0; j < e; j++) {
            int v = 1 + i + 2 * j;
            own.add(v);
            Channel.send(model, nodeTid(base, i), new Propose(v));
        }
        while (!own.isEmpty()) {
            Committed c = Channel.recv(model, Committed.class);
            own.remove(c.value());
        }
    }

    // ------------------------------------------------------------------ messages

    /** Client to node. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** Node to client. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** BLE ballot (n, pid), ordered lexicographically; also the Sequence Paxos round. */
    @Tag("Ballot")
    public record Ballot(int n, int pid) implements Comparable<Ballot> {
        static final Ballot BOTTOM = new Ballot(0, 0);

        @Override
        public int compareTo(Ballot o) {
            return n != o.n ? Integer.compare(n, o.n) : Integer.compare(pid, o.pid);
        }

        boolean gt(Ballot o) {
            return compareTo(o) > 0;
        }

        boolean lt(Ballot o) {
            return compareTo(o) < 0;
        }
    }

    /**
     * A node-to-node message: sender index and a per-link sequence number (the paper's FIFO
     * perfect link, decision D7).
     */
    public sealed interface NodeMsg extends MpMessage
            permits HBRequest, HBReply, Prepare, Promise, AcceptSync, Accept, Accepted, Decide, Forward {
        int from();

        int seq();
    }

    /** BLE (3). */
    @Tag("HBRequest")
    public record HBRequest(int from, int seq, int rnd) implements NodeMsg {}

    /** BLE (4). */
    @Tag("HBReply")
    public record HBReply(int from, int seq, int rnd, Ballot ballot, boolean qc) implements NodeMsg {}

    /** SP (3). */
    @Tag("Prepare")
    public record Prepare(int from, int seq, Ballot n, Ballot accRnd, int logIdx, int decIdx) implements NodeMsg {}

    /** SP (4). */
    @Tag("Promise")
    public record Promise(int from, int seq, Ballot n, Ballot accRnd, int logIdx, int decIdx, List<Integer> sfx)
            implements NodeMsg {}

    /** SP (5). */
    @Tag("AcceptSync")
    public record AcceptSync(int from, int seq, Ballot n, List<Integer> sfx, int syncIdx) implements NodeMsg {}

    /** SP (7). */
    @Tag("Accept")
    public record Accept(int from, int seq, Ballot n, int c) implements NodeMsg {}

    /** SP (8). */
    @Tag("Accepted")
    public record Accepted(int from, int seq, Ballot n, int logIdx) implements NodeMsg {}

    /** SP (9). */
    @Tag("Decide")
    public record Decide(int from, int seq, Ballot n, int decIdx) implements NodeMsg {}

    /** A follower's client proposal forwarded towards the leader (SP (6) at a follower, decision D9). */
    @Tag("Forward")
    public record Forward(int from, int seq, int value) implements NodeMsg {}

    // ------------------------------------------------------------------ nodes

    enum Role { FOLLOWER, LEADER }

    enum Phase { PREPARE, ACCEPT }

    /** A promise as the leader stores it: (accRnd, logIdx, f, decIdx, sfx). */
    record PromiseData(Ballot accRnd, int logIdx, int f, int decIdx, List<Integer> sfx) {}

    /** A ballot with its quorum-connected flag, as stored in BLE's ballots{}. */
    record Hb(Ballot ballot, boolean qc) {}

    private final class Node {
        final int idx;
        final int pid;
        long base;
        int budget;

        // FIFO link layer (D7)
        final int[] sendSeq = new int[N];
        final int[] expected = new int[N];

        // BLE (Figure 4)
        Ballot l = Ballot.BOTTOM;
        int r = 0;
        Ballot b;
        boolean qc = true;
        final TreeMap<Integer, Hb> ballots = new TreeMap<>();
        boolean inRound = false;

        // Sequence Paxos persistent state
        List<Integer> log = new ArrayList<>();
        Ballot promisedRnd = Ballot.BOTTOM;
        Ballot acceptedRnd = Ballot.BOTTOM;
        // volatile state
        int decidedIdx = 0;
        Role role = Role.FOLLOWER;
        Phase phase = Phase.PREPARE;
        // volatile state of leader
        Ballot currentRnd = Ballot.BOTTOM;
        final TreeMap<Integer, PromiseData> promises = new TreeMap<>();
        PromiseData maxProm = null;
        final List<Integer> buffer = new ArrayList<>();
        int[] accepted = new int[N];

        /** Client proposals this node holds while it knows no leader (D9). */
        final List<Integer> pending = new ArrayList<>();

        Node(int idx) {
            this.idx = idx;
            this.pid = pidOf(idx);
            this.b = new Ballot(0, pid);
            this.budget = idx < cand ? t : 0;
        }

        void run() {
            base = MpProcess.self().raw() - idx;
            while (true) {
                // The interface's election timeout = BLE's heartbeat timeout (D2): only when this
                // node has no leader, has budget, and no heartbeat round is running.
                if (budget > 0 && l.equals(Ballot.BOTTOM) && !inRound && Nondet.chooseBoolean()) {
                    startRound();
                    continue;
                }
                final int x0 = expected[0];
                final int x1 = expected[1];
                final int x2 = expected[2];
                Predicate<MpMessage> guard = m -> m instanceof Propose
                        || (m instanceof NodeMsg nm
                                && nm.seq() == (nm.from() == 0 ? x0 : nm.from() == 1 ? x1 : x2));
                MpMessage m = Channel.recv(model, guard);
                if (m instanceof NodeMsg nm) {
                    expected[nm.from()]++;
                }
                handle(m);
            }
        }

        void send(int to, NodeMsgFactory f) {
            NodeMsg msg = f.make(idx, sendSeq[to]++);
            Channel.send(model, nodeTid(base, to), msg);
        }

        void handle(MpMessage m) {
            if (m instanceof Propose p) {
                onProposal(p.value());
            } else if (m instanceof Forward f) {
                onProposal(f.value());
            } else if (m instanceof HBRequest q) {
                onHBRequest(q);
            } else if (m instanceof HBReply q) {
                onHBReply(q);
            } else if (m instanceof Prepare q) {
                onPrepare(q);
            } else if (m instanceof Promise q) {
                onPromise(q);
            } else if (m instanceof AcceptSync q) {
                onAcceptSync(q);
            } else if (m instanceof Accept q) {
                onAccept(q);
            } else if (m instanceof Accepted q) {
                onAccepted(q);
            } else if (m instanceof Decide q) {
                onDecide(q);
            } else {
                throw new IllegalStateException("unexpected message " + m);
            }
        }

        // ---------------------------------------------------------------- BLE (Figure 4)

        /** One unit of timeout budget: the start of a heartbeat round (Figure 4, steps 3-4 / start). */
        void startRound() {
            budget--;
            inRound = true;
            ballots.clear();
            ballots.put(idx, new Hb(b, qc)); // D3: own ballot counts towards the majority
            final int rnd = r;
            for (int p = 0; p < N; p++) {
                if (p != idx) {
                    send(p, (from, seq) -> new HBRequest(from, seq, rnd));
                }
            }
        }

        /** BLE (3): reply with ballot and qc flag. */
        void onHBRequest(HBRequest q) {
            final Ballot bb = b;
            final boolean qq = qc;
            send(q.from(), (from, seq) -> new HBReply(from, seq, q.rnd(), bb, qq));
        }

        /** BLE (4); the round's timer fires once a majority of ballots is in (D4). */
        void onHBReply(HBReply q) {
            if (q.rnd() == r && inRound) {
                ballots.put(q.from(), new Hb(q.ballot(), q.qc()));
                if (ballots.size() >= MAJORITY) {
                    timerFires();
                }
            }
        }

        /** BLE (2) "Upon timeout of startTimer", steps 1-3; step 4 is the next startRound (D2). */
        void timerFires() {
            if (ballots.size() >= MAJORITY) {
                checkLeader();
            } else {
                qc = false;
            }
            ballots.clear();
            r = r + 1;
            inRound = false;
        }

        /** BLE (1). */
        void checkLeader() {
            Ballot max = null;
            for (Hb h : ballots.values()) {
                if (h.qc() && (max == null || h.ballot().gt(max))) {
                    max = h.ballot();
                }
            }
            if (max == null) {
                return;
            }
            if (max.lt(l)) {
                b = new Ballot(l.n() + 1, pid); // increment(b) s.t. b > l
                qc = true;
            } else if (max.gt(l)) {
                l = max;
                onLeader(nodeOfPid(max.pid()), max); // trigger <Leader, max.pid, max>
            }
        }

        // ---------------------------------------------------------------- Sequence Paxos (Figure 3b)

        List<Integer> suffix(int i) {
            return i > log.size() ? List.of() : List.copyOf(log.subList(i, log.size()));
        }

        List<Integer> prefix(int i) {
            return new ArrayList<>(log.subList(0, i));
        }

        /** SP (2): <Leader> from BLE. */
        void onLeader(int s, Ballot n) {
            if (s != idx || !n.gt(promisedRnd)) {
                return;
            }
            // 1. reset all volatile state of leader
            promises.clear();
            maxProm = null;
            buffer.clear();
            accepted = new int[N];
            buffer.addAll(pending); // D9: proposals held without a leader join the buffer
            pending.clear();
            // 2.
            role = Role.LEADER;
            phase = Phase.PREPARE;
            currentRnd = n;
            promisedRnd = n;
            // 3. own promise
            promises.put(idx, new PromiseData(acceptedRnd, log.size(), idx, decidedIdx, suffix(decidedIdx)));
            // 4. Prepare to all peers
            final Ballot cr = currentRnd;
            final Ballot ar = acceptedRnd;
            final int li = log.size();
            final int di = decidedIdx;
            for (int p = 0; p < N; p++) {
                if (p != idx) {
                    send(p, (from, seq) -> new Prepare(from, seq, cr, ar, li, di));
                }
            }
        }

        /** SP (3): <Prepare> from leader l. */
        void onPrepare(Prepare q) {
            if (promisedRnd.gt(q.n())) {
                return;
            }
            boolean wasLeader = role == Role.LEADER;
            role = Role.FOLLOWER;
            phase = Phase.PREPARE;
            promisedRnd = q.n();
            final List<Integer> sfx;
            if (acceptedRnd.gt(q.accRnd())) {
                sfx = suffix(q.decIdx());
            } else if (acceptedRnd.equals(q.accRnd())) {
                sfx = suffix(q.logIdx());
            } else {
                sfx = List.of();
            }
            final Ballot ar = acceptedRnd;
            final int li = log.size();
            final int di = decidedIdx;
            send(q.from(), (from, seq) -> new Promise(from, seq, q.n(), ar, li, di, sfx));
            // D10: a demoted leader hands its buffered proposals to the new leader.
            if (wasLeader) {
                pending.addAll(buffer);
                buffer.clear();
            }
            forwardPending();
        }

        /** SP (4): <Promise> from follower f. */
        void onPromise(Promise q) {
            if (!q.n().equals(currentRnd)) {
                return;
            }
            PromiseData p = new PromiseData(q.accRnd(), q.logIdx(), q.from(), q.decIdx(), q.sfx());
            promises.put(q.from(), p);
            if (role == Role.LEADER && phase == Phase.PREPARE) {
                // P1
                if (promises.size() < MAJORITY) {
                    return;
                }
                // P2 (D5: ties beyond (accRnd, logIdx) go to the leader itself, then the lowest index)
                maxProm = null;
                if (promises.containsKey(idx)) {
                    maxProm = promises.get(idx);
                }
                for (PromiseData d : promises.values()) {
                    if (maxProm == null || better(d, maxProm)) {
                        maxProm = d;
                    }
                }
                // P3
                if (!maxProm.accRnd().equals(acceptedRnd)) {
                    log = prefix(decidedIdx);
                }
                // P4 (D6: the leader's own promise adds nothing to its own log)
                if (maxProm.f() != idx) {
                    log.addAll(maxProm.sfx());
                }
                // P5: stopped() is always false (no reconfiguration)
                log.addAll(buffer);
                buffer.clear();
                // P6
                acceptedRnd = currentRnd;
                accepted[idx] = log.size();
                phase = Phase.ACCEPT;
                // P7
                for (PromiseData d : promises.values()) {
                    if (d.f() == idx) {
                        continue;
                    }
                    int syncIdx = d.accRnd().equals(maxProm.accRnd()) ? d.logIdx() : d.decIdx();
                    sendAcceptSync(d.f(), syncIdx);
                }
            } else if (role == Role.LEADER && phase == Phase.ACCEPT) {
                // A1 (D8: min with the follower's own length)
                int syncIdx = q.accRnd().equals(maxProm.accRnd())
                        ? Math.min(q.logIdx(), maxProm.logIdx()) : q.decIdx();
                // A2
                sendAcceptSync(q.from(), syncIdx);
                // A3
                if (decidedIdx > q.decIdx()) {
                    final Ballot cr = currentRnd;
                    final int di = decidedIdx;
                    send(q.from(), (from, seq) -> new Decide(from, seq, cr, di));
                }
            }
        }

        boolean better(PromiseData a, PromiseData m) {
            int c = a.accRnd().compareTo(m.accRnd());
            return c > 0 || (c == 0 && a.logIdx() > m.logIdx());
        }

        void sendAcceptSync(int f, int syncIdx) {
            final Ballot cr = currentRnd;
            final List<Integer> sfx = suffix(syncIdx);
            send(f, (from, seq) -> new AcceptSync(from, seq, cr, sfx, syncIdx));
        }

        /** SP (5): <AcceptSync> from leader l. */
        void onAcceptSync(AcceptSync q) {
            if (!promisedRnd.equals(q.n()) || role != Role.FOLLOWER || phase != Phase.PREPARE) {
                return;
            }
            acceptedRnd = q.n();
            phase = Phase.ACCEPT;
            log = prefix(q.syncIdx());
            log.addAll(q.sfx());
            final int li = log.size();
            send(q.from(), (from, seq) -> new Accepted(from, seq, q.n(), li));
        }

        /** SP (6): proposal C from a client (or forwarded by a follower). */
        void onProposal(int c) {
            // 1. return if stopped(): never (no reconfiguration)
            if (role == Role.LEADER && phase == Phase.PREPARE) {
                buffer.add(c); // P1
            } else if (role == Role.LEADER && phase == Phase.ACCEPT) {
                log.add(c); // A1
                accepted[idx] = log.size();
                final Ballot cr = currentRnd;
                for (int f : promises.keySet()) { // A2
                    if (f != idx) {
                        send(f, (from, seq) -> new Accept(from, seq, cr, c));
                    }
                }
            } else {
                pending.add(c); // D9: a follower forwards to the leader it promised
                forwardPending();
            }
        }

        /** D9: forward held proposals to the leader of promisedRnd, if one is known. */
        void forwardPending() {
            if (role != Role.FOLLOWER || promisedRnd.equals(Ballot.BOTTOM) || pending.isEmpty()) {
                return;
            }
            int leader = nodeOfPid(promisedRnd.pid());
            if (leader == idx) {
                return;
            }
            for (int v : pending) {
                send(leader, (from, seq) -> new Forward(from, seq, v));
            }
            pending.clear();
        }

        /** SP (7): <Accept> from leader l. */
        void onAccept(Accept q) {
            if (!promisedRnd.equals(q.n()) || role != Role.FOLLOWER || phase != Phase.ACCEPT) {
                return;
            }
            log.add(q.c());
            final int li = log.size();
            send(q.from(), (from, seq) -> new Accepted(from, seq, q.n(), li));
        }

        /** SP (8): <Accepted> from follower f. */
        void onAccepted(Accepted q) {
            if (!currentRnd.equals(q.n()) || role != Role.LEADER || phase != Phase.ACCEPT) {
                return;
            }
            accepted[q.from()] = q.logIdx();
            if (q.logIdx() > decidedIdx) {
                int count = 0;
                for (int a : accepted) {
                    if (a >= q.logIdx()) {
                        count++;
                    }
                }
                if (count >= MAJORITY) {
                    learn(q.logIdx());
                    final Ballot cr = currentRnd;
                    final int di = decidedIdx;
                    for (int f : promises.keySet()) {
                        if (f != idx) {
                            send(f, (from, seq) -> new Decide(from, seq, cr, di));
                        }
                    }
                }
            }
        }

        /** SP (9): <Decide> from leader l. */
        void onDecide(Decide q) {
            if (promisedRnd.equals(q.n()) && role == Role.FOLLOWER && phase == Phase.ACCEPT) {
                learn(q.decIdx());
            }
        }

        /** decidedIdx <- idx; a node with a client reports every newly decided entry in order (D11). */
        void learn(int newIdx) {
            if (newIdx <= decidedIdx) {
                return;
            }
            int old = decidedIdx;
            decidedIdx = newIdx;
            if (idx < K) {
                for (int i = old; i < newIdx; i++) {
                    Channel.send(model, clientTid(base, idx), new Committed(log.get(i)));
                }
            }
        }
    }

    @FunctionalInterface
    interface NodeMsgFactory {
        NodeMsg make(int from, int seq);
    }
}
