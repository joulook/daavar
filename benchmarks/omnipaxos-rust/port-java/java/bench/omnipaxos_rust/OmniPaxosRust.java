package bench.omnipaxos_rust;

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

/**
 * OmniPaxos (Sequence Paxos + Ballot Leader Election) over three nodes, ported from the Rust
 * library (omnipaxos-src). See NOTES.md next to this source for the mapping and every decision.
 *
 * <p>Argument string: {@code impl,<e>,<t>,<cand>,<fifo|bag>}.
 */
public final class OmniPaxosRust implements MpProgram {

    // ------------------------------------------------------------------ parameters

    private static final int N = 3;
    private static final int K = 2;

    private final int e;
    private final int t;
    private final int cand;
    private final CommunicationModel model;
    private final String delivery;

    public OmniPaxosRust(String... args) {
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
        model = switch (delivery) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("delivery must be fifo or bag: " + delivery);
        };
    }

    @Override
    public String programName() {
        return "omnipaxos_rust";
    }

    @Override
    public String size() {
        return "e=" + e + ",t=" + t + ",cand=" + cand + "," + delivery;
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
        return "OmniPaxosRust(" + size() + ")";
    }

    // ------------------------------------------------------------------ messages

    /** Client -> node. */
    @Tag("Propose")
    public record Propose(int value) implements MpMessage {}

    /** Node -> client. */
    @Tag("Committed")
    public record Committed(int value) implements MpMessage {}

    /** ballot_leader_election::Ballot (config_id is constant 1 and omitted; priority is 0). */
    @Tag("Ballot")
    public record Ballot(int n, int priority, int pid) implements MpMessage, Comparable<Ballot> {
        static final Ballot DEFAULT = new Ballot(0, 0, 0);

        @Override
        public int compareTo(Ballot o) {
            if (n != o.n) {
                return Integer.compare(n, o.n);
            }
            if (priority != o.priority) {
                return Integer.compare(priority, o.priority);
            }
            return Integer.compare(pid, o.pid);
        }

        boolean gt(Ballot o) {
            return compareTo(o) > 0;
        }
    }

    /** util::SequenceNumber. */
    @Tag("SeqNum")
    public record SeqNum(int session, int counter) implements MpMessage {
        static final SeqNum DEFAULT = new SeqNum(0, 0);

        int cmp(SeqNum o) {
            return session != o.session ? Integer.compare(session, o.session) : Integer.compare(counter, o.counter);
        }
    }

    // BLE messages (messages::ballot_leader_election)
    @Tag("HeartbeatRequest")
    public record HeartbeatRequest(int from, int round) implements MpMessage {}

    @Tag("HeartbeatReply")
    public record HeartbeatReply(int from, int round, Ballot ballot, Ballot leader, boolean happy)
            implements MpMessage {}

    // Sequence Paxos messages (messages::sequence_paxos)
    @Tag("PrepareReq")
    public record PrepareReq(int from, Ballot n) implements MpMessage {}

    @Tag("Prepare")
    public record Prepare(int from, Ballot n, int decidedIdx, Ballot nAccepted, int acceptedIdx)
            implements MpMessage {}

    /** Promise; the optional LogSync is (hasLogSync, suffix, syncIdx). */
    @Tag("Promise")
    public record Promise(int from, Ballot n, Ballot nAccepted, int decidedIdx, int acceptedIdx,
            boolean hasLogSync, List<Integer> suffix, int syncIdx) implements MpMessage {}

    @Tag("AcceptSync")
    public record AcceptSync(int from, Ballot n, SeqNum seqNum, int decidedIdx, List<Integer> suffix,
            int syncIdx) implements MpMessage {}

    @Tag("AcceptDecide")
    public record AcceptDecide(int from, Ballot n, SeqNum seqNum, int decidedIdx, List<Integer> entries)
            implements MpMessage {}

    @Tag("Accepted")
    public record Accepted(int from, Ballot n, int acceptedIdx) implements MpMessage {}

    @Tag("NotAccepted")
    public record NotAccepted(int from, Ballot n) implements MpMessage {}

    @Tag("Decide")
    public record Decide(int from, Ballot n, SeqNum seqNum, int decidedIdx) implements MpMessage {}

    @Tag("ProposalForward")
    public record ProposalForward(int from, List<Integer> entries) implements MpMessage {}

    // ------------------------------------------------------------------ main

    @Override
    public void run() {
        // Task ids: the i-th process main spawns gets id base + 1 + i (checked after each spawn), so
        // every process knows every address from the start without any message from main.
        long base = MpProcess.self().raw();
        Tid[] nodeTid = new Tid[N];
        Tid[] clientTid = new Tid[K];
        for (int i = 0; i < N; i++) {
            nodeTid[i] = Tid.of(base + 1 + i, "n" + i);
        }
        for (int i = 0; i < K; i++) {
            clientTid[i] = Tid.of(base + 1 + N + i, "c" + i);
        }
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            int budget = i < cand ? t : 0;
            Node node = new Node(i + 1, budget, nodeTid, i < K ? clientTid[i] : null);
            MpProcess.Handle h = MpProcess.spawn("n" + i, node::run);
            check(h, nodeTid[i]);
            handles.add(h);
        }
        for (int i = 0; i < K; i++) {
            int ci = i;
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> client(ci, nodeTid[ci]));
            check(h, clientTid[i]);
            handles.add(h);
        }
        // Clients first, then nodes; the nodes never end, so main ends blocked on its await.
        for (int i = N; i < N + K; i++) {
            handles.get(i).await();
        }
        for (int i = 0; i < N; i++) {
            handles.get(i).await();
        }
    }

    private static void check(MpProcess.Handle h, Tid expected) {
        if (!h.tid().equals(expected)) {
            throw new IllegalStateException("spawned " + h.tid() + ", expected task id " + expected.raw());
        }
    }

    // ------------------------------------------------------------------ client

    private void client(int i, Tid node) {
        Set<Integer> own = new HashSet<>();
        for (int j = 0; j < e; j++) {
            int v = 1 + i + 2 * j;
            own.add(v);
            Channel.send(model, node, new Propose(v));
        }
        Set<Integer> seen = new HashSet<>();
        while (seen.size() < own.size()) {
            Committed c = Channel.recv(model, Committed.class);
            if (own.contains(c.value())) {
                seen.add(c.value());
            }
        }
    }

    // ------------------------------------------------------------------ node

    private enum Role { FOLLOWER, LEADER }

    private enum Phase { PREPARE, ACCEPT, RECOVER, NONE }

    /** An outgoing message to a node pid (1..3); records are immutable, so coalescing replaces them. */
    private static final class Out {
        final int to;
        MpMessage msg;

        Out(int to, MpMessage msg) {
            this.to = to;
            this.msg = msg;
        }
    }

    /** util::PromiseMetaData. */
    private record PromiseMeta(Ballot nAccepted, int acceptedIdx, int decidedIdx, int pid) {
        static final PromiseMeta DEFAULT = new PromiseMeta(Ballot.DEFAULT, 0, 0, 0);

        /** PartialOrd::gt as written in util.rs. */
        boolean gt(PromiseMeta o) {
            if (nAccepted.equals(o.nAccepted) && acceptedIdx == o.acceptedIdx && pid == o.pid) {
                return false;
            }
            return nAccepted.gt(o.nAccepted) || (nAccepted.equals(o.nAccepted) && acceptedIdx > o.acceptedIdx);
        }
    }

    /** util::LogSync without snapshot and stopsign. */
    private record LogSync(List<Integer> suffix, int syncIdx) {}

    private static final int NOT_PROMISED = 0;
    private static final int PROMISED = 1;
    private static final int PROMISED_HIGHER = 2;

    /** util::LeaderState. Index = pid - 1. */
    private static final class LeaderState {
        final Ballot nLeader;
        final int[] promiseState = new int[N];
        final PromiseMeta[] promiseMeta = new PromiseMeta[N];
        final SeqNum[] followerSeqNums = new SeqNum[N];
        final int[] acceptedIndexes = new int[N];
        PromiseMeta maxPromiseMeta = PromiseMeta.DEFAULT;
        LogSync maxPromiseSync = null;
        /** latest_accept_meta: index in the outgoing buffer, or -1; the ballot is always nLeader. */
        final int[] latestAcceptMeta = new int[N];

        LeaderState(Ballot n) {
            nLeader = n;
            for (int i = 0; i < N; i++) {
                followerSeqNums[i] = SeqNum.DEFAULT;
                latestAcceptMeta[i] = -1;
            }
        }

        void incrementSeqNumSession(int pid) {
            SeqNum s = followerSeqNums[pid - 1];
            followerSeqNums[pid - 1] = new SeqNum(s.session() + 1, 0);
        }

        SeqNum nextSeqNum(int pid) {
            SeqNum s = followerSeqNums[pid - 1];
            followerSeqNums[pid - 1] = new SeqNum(s.session(), s.counter() + 1);
            return followerSeqNums[pid - 1];
        }

        boolean setPromise(Promise prom, LogSync sync, int from, boolean checkMaxProm) {
            PromiseMeta meta = new PromiseMeta(prom.nAccepted(), prom.acceptedIdx(), prom.decidedIdx(), from);
            if (checkMaxProm && meta.gt(maxPromiseMeta)) {
                maxPromiseMeta = meta;
                maxPromiseSync = sync;
            }
            promiseState[from - 1] = PROMISED;
            promiseMeta[from - 1] = meta;
            int num = 0;
            for (int s : promiseState) {
                if (s == PROMISED) {
                    num++;
                }
            }
            return num >= N / 2 + 1;
        }

        void resetPromise(int pid) {
            promiseState[pid - 1] = NOT_PROMISED;
        }

        void lostPromise(int pid) {
            promiseState[pid - 1] = PROMISED_HIGHER;
        }

        LogSync takeMaxPromiseSync() {
            LogSync s = maxPromiseSync;
            maxPromiseSync = null;
            return s;
        }

        int maxDecidedIdx() {
            int m = 0;
            for (int i = 0; i < N; i++) {
                if (promiseState[i] == PROMISED) {
                    m = Math.max(m, promiseMeta[i].decidedIdx());
                }
            }
            return m;
        }

        List<Integer> promisedFollowers() {
            List<Integer> out = new ArrayList<>();
            for (int i = 0; i < N; i++) {
                if (promiseState[i] == PROMISED && i != nLeader.pid() - 1) {
                    out.add(i + 1);
                }
            }
            return out;
        }

        void resetLatestAcceptMeta() {
            for (int i = 0; i < N; i++) {
                latestAcceptMeta[i] = -1;
            }
        }

        boolean isChosen(int idx) {
            int num = 0;
            for (int a : acceptedIndexes) {
                if (a >= idx) {
                    num++;
                }
            }
            return num >= N / 2 + 1;
        }
    }

    private final class Node {
        final int pid;
        final int[] peers;
        int budget;
        final Tid[] nodeTid;
        final Tid client;

        // ---- BallotLeaderElection
        int hbRound = 0;
        List<HeartbeatReply> heartbeatReplies = new ArrayList<>();
        Ballot currentBallot;
        Ballot bleLeader;
        boolean happy = true;
        final List<Out> bleOutgoing = new ArrayList<>();

        // ---- SequencePaxos + internal storage (batch_size 1)
        Ballot promise = Ballot.DEFAULT;
        Ballot acceptedRound = Ballot.DEFAULT;
        final List<Integer> log = new ArrayList<>();
        int decidedIdx = 0;
        Role role = Role.FOLLOWER;
        Phase phase = Phase.NONE;
        final List<Integer> bufferedProposals = new ArrayList<>();
        final List<Out> outgoing = new ArrayList<>();
        LeaderState leaderState = new LeaderState(Ballot.DEFAULT);
        /** latest_accepted_meta: (ballot, index in outgoing) or null. */
        Ballot latestAcceptedBallot = null;
        int latestAcceptedIdx = -1;
        SeqNum currentSeqNum = SeqNum.DEFAULT;

        // ---- application: decided entries already delivered to the client
        int delivered = 0;

        Node(int pid, int budget, Tid[] nodeTid, Tid client) {
            this.pid = pid;
            this.budget = budget;
            this.nodeTid = nodeTid;
            this.client = client;
            int[] p = new int[N - 1];
            int k = 0;
            for (int q = 1; q <= N; q++) {
                if (q != pid) {
                    p[k++] = q;
                }
            }
            this.peers = p;
            // BallotLeaderElection::with (no recovered leader)
            currentBallot = new Ballot(1, 0, pid);
            bleLeader = currentBallot;
            newHbRound();
        }

        void run() {
            takeAndSendOutgoing();
            while (true) {
                // Election timeout: only when this node has no leader (get_current_leader() == None).
                if (budget > 0 && promise.pid() == 0 && Nondet.chooseBoolean()) {
                    budget--;
                    electionTimeout();
                } else {
                    MpMessage m = Channel.recv(model);
                    handleIncoming(m);
                }
                takeAndSendOutgoing();
                deliverDecided();
            }
        }

        // ---------------------------------------------------------- OmniPaxos glue

        void electionTimeout() {
            Ballot leader = hbTimeout();
            if (leader != null) {
                handleLeader(leader);
            }
        }

        void handleIncoming(MpMessage m) {
            if (m instanceof Propose p) {
                append(p.value());
            } else if (m instanceof HeartbeatRequest r) {
                handleRequest(r.from(), r.round());
            } else if (m instanceof HeartbeatReply r) {
                handleReply(r);
            } else if (m instanceof PrepareReq r) {
                handlePrepareReq(r, r.from());
            } else if (m instanceof Prepare r) {
                handlePrepare(r, r.from());
            } else if (m instanceof Promise r) {
                if (role == Role.LEADER && phase == Phase.PREPARE) {
                    handlePromisePrepare(r, r.from());
                } else if (role == Role.LEADER && phase == Phase.ACCEPT) {
                    handlePromiseAccept(r, r.from());
                }
            } else if (m instanceof AcceptSync r) {
                handleAcceptSync(r, r.from());
            } else if (m instanceof AcceptDecide r) {
                handleAcceptDecide(r);
            } else if (m instanceof NotAccepted r) {
                handleNotAccepted(r, r.from());
            } else if (m instanceof Accepted r) {
                handleAccepted(r, r.from());
            } else if (m instanceof Decide r) {
                handleDecide(r);
            } else if (m instanceof ProposalForward r) {
                handleForwardedProposal(new ArrayList<>(r.entries()));
            } else {
                throw new IllegalStateException("unexpected message " + m);
            }
        }

        /** take_outgoing_messages: Sequence Paxos messages first, then BLE; resets the caches. */
        void takeAndSendOutgoing() {
            for (Out o : outgoing) {
                Channel.send(model, nodeTid[o.to - 1], o.msg);
            }
            outgoing.clear();
            leaderState.resetLatestAcceptMeta();
            latestAcceptedBallot = null;
            latestAcceptedIdx = -1;
            for (Out o : bleOutgoing) {
                Channel.send(model, nodeTid[o.to - 1], o.msg);
            }
            bleOutgoing.clear();
        }

        /** The application reads the decided suffix and reports each new entry to its client. */
        void deliverDecided() {
            while (delivered < decidedIdx) {
                int v = log.get(delivered);
                delivered++;
                if (client != null) {
                    Channel.send(model, client, new Committed(v));
                }
            }
        }

        // ---------------------------------------------------------- BLE

        void newHbRound() {
            heartbeatReplies = new ArrayList<>();
            hbRound++;
            for (int peer : peers) {
                bleOutgoing.add(new Out(peer, new HeartbeatRequest(pid, hbRound)));
            }
        }

        Ballot hbTimeout() {
            updateLeader();
            updateHappiness();
            checkTakeover();
            newHbRoundKeepingReplies();
            if (promise.gt(bleLeader)) {
                bleLeader = promise;
                if (promise.pid() == pid) {
                    currentBallot = promise;
                }
                happy = true;
            }
            return bleLeader.equals(currentBallot) ? currentBallot : null;
        }

        /** new_hb_round inside hb_timeout (prev_replies is UI-only and not kept). */
        void newHbRoundKeepingReplies() {
            newHbRound();
        }

        void updateLeader() {
            Ballot max = null;
            for (HeartbeatReply r : heartbeatReplies) {
                if (max == null || r.ballot().gt(max)) {
                    max = r.ballot();
                }
            }
            if (max != null && max.gt(bleLeader)) {
                bleLeader = max;
            }
        }

        void updateHappiness() {
            if (bleLeader.equals(currentBallot)) {
                int potentialFollowers = 0;
                for (HeartbeatReply r : heartbeatReplies) {
                    if (r.leader().compareTo(currentBallot) <= 0) {
                        potentialFollowers++;
                    }
                }
                // accept and prepare quorums are both the majority (no flexible quorum)
                boolean canFormQuorum = potentialFollowers + 1 >= N / 2 + 1;
                if (canFormQuorum) {
                    happy = true;
                } else {
                    boolean seeLargerHappyLeader = false;
                    for (HeartbeatReply r : heartbeatReplies) {
                        if (r.leader().gt(currentBallot) && r.happy()) {
                            seeLargerHappyLeader = true;
                        }
                    }
                    happy = seeLargerHappyLeader;
                }
            } else {
                boolean h = false;
                for (HeartbeatReply r : heartbeatReplies) {
                    if (r.ballot().equals(bleLeader) && r.happy()) {
                        h = true;
                    }
                }
                happy = h;
            }
        }

        void checkTakeover() {
            if (!happy) {
                boolean allNeighborsUnhappy = true;
                for (HeartbeatReply r : heartbeatReplies) {
                    if (r.happy()) {
                        allNeighborsUnhappy = false;
                    }
                }
                boolean imQuorumConnected = heartbeatReplies.size() + 1 >= N / 2 + 1;
                if (allNeighborsUnhappy && imQuorumConnected) {
                    currentBallot = new Ballot(bleLeader.n() + 1, currentBallot.priority(), currentBallot.pid());
                    bleLeader = currentBallot;
                    happy = true;
                }
            }
        }

        void handleRequest(int from, int round) {
            bleOutgoing.add(new Out(from, new HeartbeatReply(pid, round, currentBallot, bleLeader, happy)));
        }

        void handleReply(HeartbeatReply rep) {
            if (rep.round() == hbRound) {
                heartbeatReplies.add(rep);
            }
        }

        // ---------------------------------------------------------- Sequence Paxos: storage helpers

        LogSync createLogSync(int commonPrefixIdx) {
            return new LogSync(List.copyOf(log.subList(commonPrefixIdx, log.size())), commonPrefixIdx);
        }

        /** InternalStorage::sync_log: set the accepted round, apply the log sync, set the decided index. */
        int syncLog(Ballot n, int newDecidedIdx, LogSync sync) {
            acceptedRound = n;
            if (sync != null) {
                while (log.size() > sync.syncIdx()) {
                    log.remove(log.size() - 1);
                }
                log.addAll(sync.suffix());
            }
            decidedIdx = Math.min(newDecidedIdx, log.size());
            return log.size();
        }

        Ballot currentLeaderBallot() {
            return promise;
        }

        // ---------------------------------------------------------- Sequence Paxos: user functions

        void append(int entry) {
            proposeEntry(entry);
        }

        void proposeEntry(int entry) {
            if (role == Role.LEADER && phase == Phase.PREPARE) {
                bufferedProposals.add(entry);
            } else if (role == Role.LEADER && phase == Phase.ACCEPT) {
                acceptEntriesLeader(List.of(entry));
            } else {
                forwardProposals(new ArrayList<>(List.of(entry)));
            }
        }

        void forwardProposals(List<Integer> entries) {
            int leader = promise.pid();
            if (leader > 0 && leader != pid) {
                outgoing.add(new Out(leader, new ProposalForward(pid, List.copyOf(entries))));
            } else {
                bufferedProposals.addAll(entries);
            }
        }

        void reconnected(int p) {
            if (p == pid) {
                return;
            } else if (p == promise.pid()) {
                role = Role.FOLLOWER;
                phase = Phase.RECOVER;
            }
            outgoing.add(new Out(p, new PrepareReq(pid, promise)));
        }

        // ---------------------------------------------------------- Sequence Paxos: leader

        void handleLeader(Ballot n) {
            if (n.compareTo(leaderState.nLeader) <= 0 || n.compareTo(promise) <= 0) {
                return;
            }
            if (pid == n.pid()) {
                leaderState = new LeaderState(n);
                promise = n;
                Ballot na = acceptedRound;
                int accIdx = log.size();
                Promise myPromise = new Promise(pid, n, na, decidedIdx, accIdx, false, List.of(), 0);
                leaderState.setPromise(myPromise, null, pid, true);
                role = Role.LEADER;
                phase = Phase.PREPARE;
                Prepare prep = new Prepare(pid, n, decidedIdx, na, accIdx);
                for (int p : peers) {
                    outgoing.add(new Out(p, prep));
                }
            } else {
                role = Role.FOLLOWER;
            }
        }

        void handlePrepareReq(PrepareReq req, int from) {
            if (role == Role.LEADER && req.n().compareTo(leaderState.nLeader) <= 0) {
                leaderState.resetPromise(from);
                leaderState.latestAcceptMeta[from - 1] = -1;
                sendPrepare(from);
            }
        }

        void handleForwardedProposal(List<Integer> entries) {
            if (role == Role.LEADER && phase == Phase.PREPARE) {
                bufferedProposals.addAll(entries);
            } else if (role == Role.LEADER && phase == Phase.ACCEPT) {
                acceptEntriesLeader(entries);
            } else {
                forwardProposals(entries);
            }
        }

        void sendPrepare(int to) {
            outgoing.add(new Out(to, new Prepare(pid, leaderState.nLeader, decidedIdx, acceptedRound, log.size())));
        }

        void acceptEntriesLeader(List<Integer> entries) {
            log.addAll(entries);
            leaderState.acceptedIndexes[pid - 1] = log.size();
            sendAcceptDecide(entries);
        }

        void sendAccSync(int to) {
            Ballot currentN = leaderState.nLeader;
            PromiseMeta max = leaderState.maxPromiseMeta;
            PromiseMeta f = leaderState.promiseMeta[to - 1];
            if (leaderState.promiseState[to - 1] != PROMISED) {
                throw new IllegalStateException("No Metadata found for promised follower");
            }
            int followersDecidedIdx = f.decidedIdx();
            int validIdx;
            if (f.nAccepted().equals(currentN)) {
                validIdx = f.acceptedIdx();
            } else if (f.nAccepted().equals(max.nAccepted())) {
                validIdx = Math.min(max.acceptedIdx(), f.acceptedIdx());
            } else {
                validIdx = followersDecidedIdx;
            }
            LogSync sync = createLogSync(validIdx);
            leaderState.incrementSeqNumSession(to);
            AcceptSync acc = new AcceptSync(pid, currentN, leaderState.nextSeqNum(to), decidedIdx,
                    sync.suffix(), sync.syncIdx());
            outgoing.add(new Out(to, acc));
        }

        void sendAcceptDecide(List<Integer> entries) {
            for (int p : leaderState.promisedFollowers()) {
                int idx = latestAccDecIndex(p);
                if (idx >= 0) {
                    AcceptDecide old = (AcceptDecide) outgoing.get(idx).msg;
                    List<Integer> ext = new ArrayList<>(old.entries());
                    ext.addAll(entries);
                    outgoing.get(idx).msg =
                            new AcceptDecide(pid, old.n(), old.seqNum(), decidedIdx, List.copyOf(ext));
                } else {
                    leaderState.latestAcceptMeta[p - 1] = outgoing.size();
                    outgoing.add(new Out(p, new AcceptDecide(pid, leaderState.nLeader, leaderState.nextSeqNum(p),
                            decidedIdx, List.copyOf(entries))));
                }
            }
        }

        void sendDecide(int to, int dIdx) {
            outgoing.add(new Out(to, new Decide(pid, leaderState.nLeader, leaderState.nextSeqNum(to), dIdx)));
        }

        void handleMajorityPromises() {
            LogSync maxSync = leaderState.takeMaxPromiseSync();
            int d = leaderState.maxDecidedIdx();
            int newAcceptedIdx = syncLog(leaderState.nLeader, d, maxSync);
            if (!bufferedProposals.isEmpty()) {
                log.addAll(bufferedProposals);
                bufferedProposals.clear();
                newAcceptedIdx = log.size();
            }
            role = Role.LEADER;
            phase = Phase.ACCEPT;
            leaderState.acceptedIndexes[pid - 1] = newAcceptedIdx;
            for (int p : leaderState.promisedFollowers()) {
                sendAccSync(p);
            }
        }

        LogSync logSyncOf(Promise prom) {
            return prom.hasLogSync() ? new LogSync(prom.suffix(), prom.syncIdx()) : null;
        }

        void handlePromisePrepare(Promise prom, int from) {
            if (prom.n().equals(leaderState.nLeader)) {
                boolean majority = leaderState.setPromise(prom, logSyncOf(prom), from, true);
                if (majority) {
                    handleMajorityPromises();
                }
            }
        }

        void handlePromiseAccept(Promise prom, int from) {
            if (prom.n().equals(leaderState.nLeader)) {
                leaderState.setPromise(prom, logSyncOf(prom), from, false);
                sendAccSync(from);
            }
        }

        void handleAccepted(Accepted acc, int from) {
            if (acc.n().equals(leaderState.nLeader) && role == Role.LEADER && phase == Phase.ACCEPT) {
                leaderState.acceptedIndexes[from - 1] = acc.acceptedIdx();
                if (acc.acceptedIdx() > decidedIdx && leaderState.isChosen(acc.acceptedIdx())) {
                    int d = acc.acceptedIdx();
                    decidedIdx = d;
                    for (int p : leaderState.promisedFollowers()) {
                        int idx = latestAccDecIndex(p);
                        if (idx >= 0) {
                            AcceptDecide old = (AcceptDecide) outgoing.get(idx).msg;
                            outgoing.get(idx).msg = new AcceptDecide(pid, old.n(), old.seqNum(), d, old.entries());
                        } else {
                            sendDecide(p, d);
                        }
                    }
                }
            }
        }

        int latestAccDecIndex(int to) {
            int idx = leaderState.latestAcceptMeta[to - 1];
            if (idx >= 0 && idx < outgoing.size() && outgoing.get(idx).msg instanceof AcceptDecide) {
                return idx;
            }
            return -1;
        }

        void handleNotAccepted(NotAccepted na, int from) {
            if (role == Role.LEADER && leaderState.nLeader.compareTo(na.n()) < 0) {
                leaderState.lostPromise(from);
            }
        }

        // ---------------------------------------------------------- Sequence Paxos: follower

        void handlePrepare(Prepare prep, int from) {
            Ballot oldPromise = promise;
            if (oldPromise.compareTo(prep.n()) < 0 || (oldPromise.equals(prep.n()) && phase == Phase.RECOVER)) {
                promise = prep.n();
                role = Role.FOLLOWER;
                phase = Phase.PREPARE;
                currentSeqNum = SeqNum.DEFAULT;
                Ballot na = acceptedRound;
                int accIdx = log.size();
                LogSync sync;
                if (na.gt(prep.nAccepted())) {
                    sync = createLogSync(prep.decidedIdx());
                } else if (na.equals(prep.nAccepted()) && accIdx > prep.acceptedIdx()) {
                    sync = createLogSync(prep.acceptedIdx());
                } else {
                    sync = null;
                }
                Promise p = sync == null
                        ? new Promise(pid, prep.n(), na, decidedIdx, accIdx, false, List.of(), 0)
                        : new Promise(pid, prep.n(), na, decidedIdx, accIdx, true, sync.suffix(), sync.syncIdx());
                outgoing.add(new Out(from, p));
            }
        }

        void handleAcceptSync(AcceptSync acc, int from) {
            if (checkValidBallot(acc.n()) && role == Role.FOLLOWER && phase == Phase.PREPARE) {
                int newAcceptedIdx = syncLog(acc.n(), acc.decidedIdx(), new LogSync(acc.suffix(), acc.syncIdx()));
                forwardBufferedProposals();
                phase = Phase.ACCEPT;
                currentSeqNum = acc.seqNum();
                latestAcceptedBallot = acc.n();
                latestAcceptedIdx = outgoing.size();
                outgoing.add(new Out(from, new Accepted(pid, acc.n(), newAcceptedIdx)));
            }
        }

        void forwardBufferedProposals() {
            if (!bufferedProposals.isEmpty()) {
                List<Integer> props = new ArrayList<>(bufferedProposals);
                bufferedProposals.clear();
                forwardProposals(props);
            }
        }

        void handleAcceptDecide(AcceptDecide acc) {
            if (checkValidBallot(acc.n()) && role == Role.FOLLOWER && phase == Phase.ACCEPT
                    && handleSequenceNum(acc.seqNum(), acc.n().pid()) == Status.EXPECTED) {
                Integer newAcceptedIdx = null;
                if (!acc.entries().isEmpty()) {
                    log.addAll(acc.entries());
                    newAcceptedIdx = log.size();
                }
                Integer flushed = updateDecidedIdxAndGetAcceptedIdx(acc.decidedIdx());
                if (flushed != null) {
                    newAcceptedIdx = flushed;
                }
                if (newAcceptedIdx != null) {
                    replyAccepted(acc.n(), newAcceptedIdx);
                }
            }
        }

        void handleDecide(Decide dec) {
            if (checkValidBallot(dec.n()) && phase == Phase.ACCEPT
                    && handleSequenceNum(dec.seqNum(), dec.n().pid()) == Status.EXPECTED) {
                Integer idx = updateDecidedIdxAndGetAcceptedIdx(dec.decidedIdx());
                if (idx != null) {
                    replyAccepted(dec.n(), idx);
                }
            }
        }

        Integer updateDecidedIdxAndGetAcceptedIdx(int newDecidedIdx) {
            if (newDecidedIdx <= decidedIdx) {
                return null;
            }
            if (newDecidedIdx > log.size()) {
                // flush_batch is a no-op with batch_size 1
                int newAcceptedIdx = log.size();
                decidedIdx = Math.min(newDecidedIdx, newAcceptedIdx);
                return newAcceptedIdx;
            } else {
                decidedIdx = newDecidedIdx;
                return null;
            }
        }

        void replyAccepted(Ballot n, int accIdx) {
            if (latestAcceptedBallot != null && latestAcceptedBallot.equals(n) && latestAcceptedIdx >= 0
                    && outgoing.get(latestAcceptedIdx).msg instanceof Accepted) {
                outgoing.get(latestAcceptedIdx).msg = new Accepted(pid, n, accIdx);
            } else {
                latestAcceptedBallot = n;
                latestAcceptedIdx = outgoing.size();
                outgoing.add(new Out(n.pid(), new Accepted(pid, n, accIdx)));
            }
        }

        boolean checkValidBallot(Ballot messageBallot) {
            int c = promise.compareTo(messageBallot);
            if (c == 0) {
                return true;
            } else if (c > 0) {
                outgoing.add(new Out(messageBallot.pid(), new NotAccepted(pid, promise)));
                return false;
            } else {
                reconnected(messageBallot.pid());
                return false;
            }
        }

        Status handleSequenceNum(SeqNum seq, int from) {
            Status st;
            if (seq.session() == currentSeqNum.session() && seq.counter() == currentSeqNum.counter() + 1) {
                st = Status.EXPECTED;
            } else if (seq.cmp(currentSeqNum) <= 0) {
                st = Status.OUTDATED;
            } else {
                st = Status.DROPPED_PRECEDING;
            }
            switch (st) {
                case EXPECTED -> currentSeqNum = seq;
                case DROPPED_PRECEDING -> reconnected(from);
                default -> { }
            }
            return st;
        }
    }

    private enum Status { EXPECTED, DROPPED_PRECEDING, OUTDATED }
}
