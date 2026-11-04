package bench.omnipaxos_rust_sol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Fixed-membership, unbatched Sequence Paxos and Ballot Leader Election. */
public final class OmniPaxosRustSol implements MpProgram {
    private final int e, t, cand;
    private final CommunicationModel communication;

    public OmniPaxosRustSol(String... args) {
        if (args.length != 5 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<e>,<t>,<cand>,<fifo|bag>");
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = Integer.parseInt(args[3]);
        if (e < 1 || t < 1 || cand < 1 || cand > 3)
            throw new IllegalArgumentException("e,t >= 1; 1 <= cand <= 3");
        communication = switch (args[4]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    @Override public String programName() { return "omnipaxos"; }
    @Override public String size() { return "e=" + e + ",t=" + t + ",cand=" + cand; }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() { return "OmniPaxosRustSol(" + size() + "," + communication + ")"; }

    @Tag("Propose") public record Propose(int value) implements MpMessage {}
    @Tag("Committed") public record Committed(int value) implements MpMessage {}
    @Tag("Ballot") public record Ballot(int configId, long n, long priority, int pid)
            implements Comparable<Ballot> {
        @Override public int compareTo(Ballot b) {
            int c = Long.compare(n, b.n);
            if (c == 0) c = Long.compare(priority, b.priority);
            return c == 0 ? Integer.compare(pid, b.pid) : c;
        }
    }
    private static final Ballot ZERO = new Ballot(0, 0, 0, 0);
    @Tag("SequenceNumber") public record SequenceNumber(long session, long counter)
            implements Comparable<SequenceNumber> {
        @Override public int compareTo(SequenceNumber b) {
            int c = Long.compare(session, b.session);
            return c == 0 ? Long.compare(counter, b.counter) : c;
        }
    }
    @Tag("LogSync") public record LogSync(int syncIdx, List<Integer> suffix) {
        public LogSync { suffix = List.copyOf(suffix); }
    }
    @Tag("HeartbeatRequest") public record HeartbeatRequest(int from, long round) implements MpMessage {}
    @Tag("HeartbeatReply") public record HeartbeatReply(long round, Ballot ballot, Ballot leader,
                                                        boolean happy) implements MpMessage {}
    @Tag("PrepareReq") public record PrepareReq(int from, Ballot n) implements MpMessage {}
    @Tag("Prepare") public record Prepare(Ballot n, int decidedIdx, Ballot nAccepted,
                                           int acceptedIdx) implements MpMessage {}
    // A zero/one element list represents Rust's Option<LogSync>, without null payloads.
    @Tag("Promise") public record Promise(int from, Ballot n, Ballot nAccepted, int decidedIdx,
                                           int acceptedIdx, List<LogSync> logSync) implements MpMessage {
        public Promise { logSync = List.copyOf(logSync); }
    }
    @Tag("AcceptSync") public record AcceptSync(Ballot n, SequenceNumber seqNum, int decidedIdx,
                                                 LogSync logSync) implements MpMessage {}
    @Tag("AcceptDecide") public record AcceptDecide(Ballot n, SequenceNumber seqNum, int decidedIdx,
                                                     List<Integer> entries) implements MpMessage {
        public AcceptDecide { entries = List.copyOf(entries); }
    }
    @Tag("Accepted") public record Accepted(int from, Ballot n, int acceptedIdx) implements MpMessage {}
    @Tag("Decide") public record Decide(Ballot n, SequenceNumber seqNum, int decidedIdx) implements MpMessage {}
    @Tag("NotAccepted") public record NotAccepted(int from, Ballot n) implements MpMessage {}
    @Tag("ProposalForward") public record ProposalForward(List<Integer> entries) implements MpMessage {
        public ProposalForward { entries = List.copyOf(entries); }
    }

    @Override public void run() {
        // All addresses are filled before starting any process; thereafter this table is read only.
        Tid[] addresses = new Tid[5];
        MpThread[] processes = new MpThread[5];
        for (int i = 0; i < 5; i++) {
            final int index = i;
            String role = i < 3 ? "n" + i : "c" + (i - 3);
            processes[i] = new MpThread(() -> {
                if (index < 3) new Node(index + 1, addresses).serve();
                else client(index - 3, addresses);
            }, role);
            addresses[i] = Tid.of(processes[i].getTaskId(), role);
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
        boolean[] seen = new boolean[e];
        int remaining = e;
        while (remaining > 0) {
            int value = Channel.recv(communication, Committed.class).value;
            int delta = value - 1 - id;
            if (delta >= 0 && delta % 2 == 0 && delta / 2 < e && !seen[delta / 2]) {
                seen[delta / 2] = true;
                remaining--;
            }
        }
    }

    private enum Phase { NONE, PREPARE, ACCEPT, RECOVER }

    private final class Node {
        final int pid;
        final Tid[] addresses;
        int budget;
        Ballot ballot, bleLeader;
        boolean happy = true;
        long hbRound;
        final List<HeartbeatReply> replies = new ArrayList<>();
        Ballot promise = ZERO, acceptedRound = ZERO, leaderRound = ZERO;
        boolean leader;
        Phase phase = Phase.NONE;
        final List<Integer> log = new ArrayList<>(), buffered = new ArrayList<>();
        int decided, delivered;
        SequenceNumber currentSeq = new SequenceNumber(0, 0);
        final Promise[] promises = new Promise[3];
        final int[] accepted = new int[3];
        final long[] sessions = new long[3], counters = new long[3];
        Promise maxPromise;

        Node(int pid, Tid[] addresses) {
            this.pid = pid;
            this.addresses = addresses;
            budget = pid <= cand ? t : 0;
            ballot = new Ballot(1, 1, 0, pid);
            bleLeader = ballot;
        }
        void send(int to, MpMessage m) { Channel.send(communication, addresses[to - 1], m); }
        void newHeartbeatRound() {
            replies.clear();
            hbRound++;
            for (int p = 1; p <= 3; p++) if (p != pid)
                send(p, new HeartbeatRequest(pid, hbRound));
        }
        void serve() {
            newHeartbeatRound();
            while (true) {
                if (budget > 0 && promise.pid == 0 && Nondet.chooseBoolean()) {
                    budget--;
                    timeout();
                } else handle(Channel.recv(communication));
                // Equivalent to polling read_decided_suffix(delivered) after each library call.
                while (delivered < decided) {
                    int value = log.get(delivered++);
                    if (pid <= 2) Channel.send(communication, addresses[pid + 2], new Committed(value));
                }
            }
        }
        void timeout() {
            for (HeartbeatReply r : replies)
                if (r.ballot.compareTo(bleLeader) > 0) bleLeader = r.ballot;
            if (bleLeader.equals(ballot)) {
                int followers = 1;
                boolean largerHappy = false;
                for (HeartbeatReply r : replies) {
                    if (r.leader.compareTo(ballot) <= 0) followers++;
                    if (r.leader.compareTo(ballot) > 0 && r.happy) largerHappy = true;
                }
                happy = followers >= 2 || largerHappy;
            } else {
                happy = false;
                for (HeartbeatReply r : replies)
                    if (r.ballot.equals(bleLeader) && r.happy) happy = true;
            }
            if (!happy && replies.size() + 1 >= 2 && replies.stream().allMatch(r -> !r.happy)) {
                ballot = new Ballot(1, bleLeader.n + 1, ballot.priority, pid);
                bleLeader = ballot;
                happy = true;
            }
            newHeartbeatRound();
            if (promise.compareTo(bleLeader) > 0) {
                bleLeader = promise;
                if (promise.pid == pid) ballot = promise;
                happy = true;
            }
            if (bleLeader.equals(ballot)) elect(ballot);
        }
        void elect(Ballot n) {
            if (n.compareTo(leaderRound) <= 0 || n.compareTo(promise) <= 0) return;
            if (n.pid != pid) { leader = false; return; }
            leaderRound = n;
            Arrays.fill(promises, null);
            Arrays.fill(accepted, 0);
            Arrays.fill(sessions, 0);
            Arrays.fill(counters, 0);
            maxPromise = null;
            promise = n;
            putPromise(new Promise(pid, n, acceptedRound, decided, log.size(), List.of()), true);
            leader = true;
            phase = Phase.PREPARE;
            for (int p = 1; p <= 3; p++) if (p != pid) sendPrepare(p);
        }
        void sendPrepare(int to) {
            send(to, new Prepare(leaderRound, decided, acceptedRound, log.size()));
        }
        void propose(List<Integer> entries) {
            if (leader && phase == Phase.PREPARE) buffered.addAll(entries);
            else if (leader && phase == Phase.ACCEPT) {
                log.addAll(entries);
                accepted[pid - 1] = log.size();
                for (int p = 1; p <= 3; p++) if (isFollower(p))
                    send(p, new AcceptDecide(leaderRound, nextSeq(p), decided, entries));
            } else forward(entries);
        }
        void forward(List<Integer> entries) {
            if (promise.pid > 0 && promise.pid != pid) send(promise.pid, new ProposalForward(entries));
            else buffered.addAll(entries);
        }
        boolean isFollower(int p) { return p != pid && promises[p - 1] != null; }
        SequenceNumber nextSeq(int p) { return new SequenceNumber(sessions[p - 1], ++counters[p - 1]); }
        LogSync suffix(int start) { return new LogSync(start, log.subList(start, log.size())); }
        void sync(Ballot n, int d, List<LogSync> updates) {
            if (!updates.isEmpty()) {
                LogSync update = updates.get(0);
                log.subList(update.syncIdx, log.size()).clear();
                log.addAll(update.suffix);
            }
            acceptedRound = n;
            decided = d;
            if (decided > log.size() || decided < delivered)
                throw new AssertionError("invalid decided prefix");
        }
        void putPromise(Promise m, boolean checkMax) {
            if (checkMax && (maxPromise == null || m.nAccepted.compareTo(maxPromise.nAccepted) > 0
                    || (m.nAccepted.equals(maxPromise.nAccepted) && m.acceptedIdx > maxPromise.acceptedIdx)))
                maxPromise = m;
            promises[m.from - 1] = m;
        }
        void acceptSync(int to) {
            Promise follower = promises[to - 1];
            int valid = follower.nAccepted.equals(leaderRound) ? follower.acceptedIdx
                    : follower.nAccepted.equals(maxPromise.nAccepted)
                        ? Math.min(maxPromise.acceptedIdx, follower.acceptedIdx) : follower.decidedIdx;
            sessions[to - 1]++;
            counters[to - 1] = 0;
            send(to, new AcceptSync(leaderRound, nextSeq(to), decided, suffix(valid)));
        }
        void onPromise(Promise m) {
            if (!leader || !m.n.equals(leaderRound)) return;
            if (phase == Phase.PREPARE) {
                putPromise(m, true);
                int count = 0, maxDecided = 0;
                for (Promise p : promises) if (p != null) { count++; maxDecided = Math.max(maxDecided, p.decidedIdx); }
                if (count >= 2) {
                    sync(leaderRound, maxDecided, maxPromise.logSync);
                    log.addAll(buffered);
                    buffered.clear();
                    phase = Phase.ACCEPT;
                    accepted[pid - 1] = log.size();
                    for (int p = 1; p <= 3; p++) if (isFollower(p)) acceptSync(p);
                }
            } else if (phase == Phase.ACCEPT) {
                putPromise(m, false);
                acceptSync(m.from);
            }
        }
        void reconnect(int p) {
            if (p == pid) return;
            if (p == promise.pid) { leader = false; phase = Phase.RECOVER; }
            send(p, new PrepareReq(pid, promise));
        }
        boolean validBallot(Ballot n) {
            int c = promise.compareTo(n);
            if (c == 0) return true;
            if (c > 0) send(n.pid, new NotAccepted(pid, promise));
            else reconnect(n.pid);
            return false;
        }
        boolean expected(SequenceNumber seq, int from) {
            if (seq.session == currentSeq.session && seq.counter == currentSeq.counter + 1) {
                currentSeq = seq;
                return true;
            }
            if (seq.compareTo(currentSeq) > 0) reconnect(from);
            return false;
        }
        void learn(int d) { if (d > decided) decided = Math.min(d, log.size()); }
        void handle(MpMessage message) {
            if (message instanceof Propose m) propose(List.of(m.value));
            else if (message instanceof ProposalForward m) propose(m.entries);
            else if (message instanceof HeartbeatRequest m)
                send(m.from, new HeartbeatReply(m.round, ballot, bleLeader, happy));
            else if (message instanceof HeartbeatReply m) {
                if (m.round == hbRound && m.ballot.configId == 1) replies.add(m);
            } else if (message instanceof Prepare m) {
                if (promise.compareTo(m.n) < 0 || (promise.equals(m.n) && phase == Phase.RECOVER)) {
                    promise = m.n;
                    leader = false;
                    phase = Phase.PREPARE;
                    currentSeq = new SequenceNumber(0, 0);
                    List<LogSync> update = acceptedRound.compareTo(m.nAccepted) > 0 ? List.of(suffix(m.decidedIdx))
                            : acceptedRound.equals(m.nAccepted) && log.size() > m.acceptedIdx
                                ? List.of(suffix(m.acceptedIdx)) : List.of();
                    send(m.n.pid, new Promise(pid, m.n, acceptedRound, decided, log.size(), update));
                }
            } else if (message instanceof Promise m) onPromise(m);
            else if (message instanceof AcceptSync m) {
                if (validBallot(m.n) && !leader && phase == Phase.PREPARE) {
                    sync(m.n, m.decidedIdx, List.of(m.logSync));
                    if (!buffered.isEmpty()) {
                        List<Integer> entries = List.copyOf(buffered);
                        buffered.clear();
                        forward(entries);
                    }
                    phase = Phase.ACCEPT;
                    currentSeq = m.seqNum;
                    send(m.n.pid, new Accepted(pid, m.n, log.size()));
                }
            } else if (message instanceof AcceptDecide m) {
                if (validBallot(m.n) && !leader && phase == Phase.ACCEPT && expected(m.seqNum, m.n.pid)) {
                    log.addAll(m.entries);
                    learn(m.decidedIdx);
                    send(m.n.pid, new Accepted(pid, m.n, log.size()));
                }
            } else if (message instanceof Decide m) {
                if (validBallot(m.n) && phase == Phase.ACCEPT && expected(m.seqNum, m.n.pid)) learn(m.decidedIdx);
            } else if (message instanceof Accepted m) {
                if (leader && phase == Phase.ACCEPT && m.n.equals(leaderRound)) {
                    accepted[m.from - 1] = m.acceptedIdx;
                    int count = 0;
                    for (int a : accepted) if (a >= m.acceptedIdx) count++;
                    if (m.acceptedIdx > decided && count >= 2) {
                        decided = m.acceptedIdx;
                        for (int p = 1; p <= 3; p++) if (isFollower(p))
                            send(p, new Decide(leaderRound, nextSeq(p), decided));
                    }
                }
            } else if (message instanceof PrepareReq m) {
                if (leader && m.n.compareTo(leaderRound) <= 0) {
                    promises[m.from - 1] = null;
                    sendPrepare(m.from);
                }
            } else if (message instanceof NotAccepted m) {
                if (leader && leaderRound.compareTo(m.n) < 0) promises[m.from - 1] = null;
            } else throw new IllegalArgumentException("unexpected message " + message);
        }
    }
}
