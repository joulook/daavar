package bench.swiftpaxos_code_sol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.BiPredicate;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Ballot-zero port. Source correspondence and fixed options are in NOTES.md. */
public final class SwiftpaxosCodeSol implements MpProgram {
    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}
    @Tag("Id") public record Id(int client, int sequence) {}
    @Tag("Propagate") public record Propagate(Id id, MpMessage command) implements MpMessage {}
    @Tag("FastAck") public record FastAck(int replica, int ballot, Id id,
                                         List<Id> dep, String checksum, int seqnum) implements MpMessage {
        public FastAck { dep = List.copyOf(dep); }
    }
    @Tag("FastAckClient") public record FastAckClient(int replica, int ballot, Id id,
                                                     String checksum) implements MpMessage {}
    @Tag("LightSlowAck") public record LightSlowAck(int replica, int ballot, Id id) implements MpMessage {}
    @Tag("Reply") public record Reply(int replica, int ballot, Id id,
                                     String checksum, int value) implements MpMessage {}

    private final int q;
    private final CommunicationModel model;
    private static final List<String> ROLES = List.of("n0", "n1", "n2", "l0", "l1", "c0", "c1");
    public SwiftpaxosCodeSol(String... args) {
        if (args.length != 3 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>");
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be positive");
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }
    public String programName() { return "swiftpaxos"; }
    public String size() { return Integer.toString(q); }
    public CommunicationModel model() { return model; }
    public List<String> rolesInSpawnOrder() { return ROLES; }
    public List<String> visibleRoles() { return List.of("c0", "c1"); }
    public String toString() { return "SwiftpaxosCodeSol impl q=" + q + " model=" + model; }

    public void run() {
        // Only addresses are shared, installed before any process is started.
        Tid[] addresses = new Tid[7];
        MpThread[] threads = new MpThread[7];
        for (int k = 0; k < 7; k++) {
            final int role = k;
            threads[k] = new MpThread(() -> {
                List<Tid> peers = List.copyOf(Arrays.asList(addresses));
                if (role < 3) new Replica(role, peers).run();
                else if (role < 5) new Library(role - 3, peers).run();
                else client(role - 5, peers);
            }, ROLES.get(k));
            addresses[k] = Tid.of(threads[k].getTaskId(), ROLES.get(k));
        }
        for (MpThread thread : threads) thread.start();
        try {
            for (MpThread thread : threads) thread.join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
    private void send(Tid to, MpMessage message) { Channel.send(model, to, message); }
    private void client(int i, List<Tid> peers) {
        for (int j = 0; j < q; j++) {
            int kind = (i + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(i, 1 + i + 2 * j);
                case 1 -> new Read(i);
                default -> new Rmw(i);
            };
            send(peers.get(3 + i), request);
            MpMessage response = Channel.recv(model);
            if (!(kind == 0 ? response instanceof Ack : kind == 1 ? response instanceof Value
                                                                                : response instanceof Result))
                throw new AssertionError("wrong client response: " + response);
        }
    }

    // A value used only inside a process: null dep/checksum denotes an unconditional slow vote.
    private record Vote(int replica, List<Id> dep, String checksum) {}
    /** replica/mset.go:45-86, including replacement before the leader and duplicate filtering after it. */
    private static final class MsgSet {
        final boolean fast;
        final BiPredicate<Vote, Vote> accept;
        final Map<Integer, Vote> votes = new LinkedHashMap<>();
        Vote leader;
        MsgSet(boolean fast, BiPredicate<Vote, Vote> accept) { this.fast = fast; this.accept = accept; }
        void add(Vote vote, boolean isLeader) {
            int r = vote.replica;
            if (fast && r > 1) return;
            if (leader != null && votes.containsKey(r)) return;
            if (isLeader) {
                leader = vote;
                votes.values().removeIf(v -> !accept.test(v, leader));
            } else if (leader == null || accept.test(vote, leader)) votes.put(r, vote);
        }
        boolean ready() { return leader != null && !votes.isEmpty(); }
    }
    private static boolean depsEqual(List<Id> a, List<Id> b) {
        return a.size() == b.size() && new HashSet<>(a).equals(new HashSet<>(b));
    }

    /** Exact single-key SHA-256 chain, recomputed after each leader update (dpath.go). */
    private static final class HashLog {
        int synced = -1;
        String syncedHash = "00".repeat(32);
        String hash = syncedHash;
        final List<Id> pending = new ArrayList<>();
        String append(Id id) {
            pending.add(id);
            hash = extend(hash, id);
            return hash;
        }
        void update(FastAck leader) {
            boolean existed = pending.remove(leader.id);
            if (!existed && leader.seqnum <= synced) return;
            if (leader.seqnum > synced) { synced = leader.seqnum; syncedHash = leader.checksum; }
            hash = syncedHash;
            for (Id id : pending) hash = extend(hash, id);
        }
        static String extend(String initial, Id id) {
            byte[] data = ByteBuffer.allocate(40).order(ByteOrder.LITTLE_ENDIAN)
                .put(HexFormat.of().parseHex(initial)).putInt(id.client).putInt(id.sequence).array();
            try {
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
            } catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
        }
    }
    private static final int START = 0, PRE_ACCEPT = 1, ACCEPT = 2, COMMIT = 3;
    private static final class Desc {
        final Id id;
        int phase = START;
        MpMessage command;
        List<Id> dep = List.of();
        String checksum;
        boolean slowPath, delivered;
        FastAck deferredLeader;
        final MsgSet fast = new MsgSet(true, (v, l) -> v.dep == null || depsEqual(v.dep, l.dep));
        final MsgSet slow = new MsgSet(false, (v, l) -> v.dep == null || depsEqual(v.dep, l.dep));
        Desc(Id id) { this.id = id; }
    }
    private final class Replica {
        final int index;
        final List<Tid> peers;
        final Map<Id, Desc> descriptions = new LinkedHashMap<>();
        final Map<Id, FastAck> pendingUpdates = new HashMap<>();
        final HashLog log = new HashLog();
        Id lastCmd, lastWrite;
        int x, seqnum;
        Replica(int index, List<Tid> peers) { this.index = index; this.peers = peers; }
        Desc desc(Id id) { return descriptions.computeIfAbsent(id, Desc::new); }
        void broadcast(MpMessage message) {
            for (int r = 0; r < 3; r++) if (r != index) send(peers.get(r), message);
        }
        void run() {
            while (true) {
                MpMessage message = Channel.recv(model);
                if (message instanceof Propagate p) propose(p);
                else if (message instanceof FastAck f) {
                    if (f.ballot != 0) continue;
                    if (f.replica == 0 && index != 0) {
                        if (desc(f.id).command == null) pendingUpdates.put(f.id, f);
                        else log.update(f);
                    }
                    fastAck(f, desc(f.id));
                } else if (message instanceof LightSlowAck s && s.ballot == 0) {
                    Desc d = desc(s.id);
                    if (!d.delivered) slowAck(s, d);
                } else throw new AssertionError("unexpected replica message " + message);
                executeReady();
            }
        }
        void propose(Propagate p) {
            Desc d = desc(p.id);
            if (d.command != null) return;
            // lightKeyInfo: a read depends on last write; a mutator on last command.
            Id prior = p.command instanceof Read ? lastWrite : lastCmd;
            List<Id> localDep = prior == null ? List.of() : List.of(prior);
            lastCmd = p.id;
            if (!(p.command instanceof Read)) lastWrite = p.id;
            d.checksum = log.append(p.id);
            FastAck update = pendingUpdates.remove(p.id);
            if (update != null) log.update(update); // snapshot above intentionally predates update
            d.command = p.command;
            if (index < 2) { d.dep = localDep; d.phase = PRE_ACCEPT; }
            if (d.deferredLeader != null) {
                FastAck leader = d.deferredLeader;
                d.deferredLeader = null;
                leaderAck(leader, d);
            }
            if (index == 2 || d.slowPath || d.delivered) return;
            FastAck own = new FastAck(index, 0, p.id, d.dep, d.checksum, index == 0 ? seqnum++ : 0);
            if (index != 0) send(peers.get(3 + p.id.client),
                                    new FastAckClient(index, 0, p.id, d.checksum));
            broadcast(own);
            // optExec=true, fastRead=false: leader executes even before COMMIT.
            if (index == 0) execute(d);
            fastAck(own, d);
        }
        void fastAck(FastAck f, Desc d) {
            if (d.delivered) return;
            if (f.replica == 0) {
                if (d.command == null) d.deferredLeader = f;
                else leaderAck(f, d);
            } else {
                d.fast.add(new Vote(f.replica, f.dep, f.checksum), false);
                commit(d);
            }
        }
        void leaderAck(FastAck f, Desc d) {
            if (d.delivered) return;
            d.phase = ACCEPT;
            boolean neq = !depsEqual(d.dep, f.dep);
            // SQ is any majority: both followers belong to it, even when deps agree.
            boolean sendSlow = index != 0;
            Vote leader = new Vote(0, f.dep, f.checksum);
            d.slow.add(leader, true);
            commit(d);
            if (!d.delivered) { d.fast.add(leader, true); commit(d); }
            if (sendSlow) {
                if (neq && !d.delivered) d.dep = f.dep;
                d.slowPath = true;
                LightSlowAck slow = new LightSlowAck(index, 0, d.id);
                send(peers.get(3 + d.id.client), slow);
                broadcast(slow);
                if (!d.delivered) slowAck(slow, d);
            }
        }
        void slowAck(LightSlowAck s, Desc d) {
            Vote slow = new Vote(s.replica, null, null);
            d.slow.add(slow, false);
            commit(d);
            if (!d.delivered) { d.fast.add(slow, false); commit(d); }
        }
        void commit(Desc d) {
            if (d.delivered || !(d.slow.ready() || d.fast.ready())) return;
            d.phase = COMMIT;
            execute(d);
        }
        void execute(Desc d) {
            if (d.command == null || d.delivered || (d.phase != COMMIT && index != 0)) return;
            for (Id dependency : d.dep) if (!desc(dependency).delivered) return;
            d.delivered = true;
            if (d.command instanceof Write w) x = w.value;
            else if (d.command instanceof Rmw) x += 10;
            if (index == 0) send(peers.get(3 + d.id.client), new Reply(0, 0, d.id, d.checksum, x));
        }
        void executeReady() {
            // Successor notifications are local work in the replica process.
            boolean changed;
            do {
                changed = false;
                for (Desc d : new ArrayList<>(descriptions.values())) {
                    boolean before = d.delivered;
                    execute(d);
                    changed |= !before && d.delivered;
                }
            } while (changed);
        }
    }

    private static final class ResponseState {
        final MsgSet fast = new MsgSet(true, (v, l) -> Objects.equals(v.checksum, l.checksum));
        final MsgSet slow = new MsgSet(false, (v, l) -> Objects.equals(v.checksum, l.checksum));
        MpMessage command;
        int value;
        boolean delivered;
    }
    private final class Library {
        final int index;
        final List<Tid> peers;
        final Map<Id, ResponseState> responses = new HashMap<>();
        int sequence;
        Library(int index, List<Tid> peers) { this.index = index; this.peers = peers; }
        ResponseState response(Id id) { return responses.computeIfAbsent(id, k -> new ResponseState()); }
        void run() {
            while (true) {
                MpMessage message = Channel.recv(model);
                if (message instanceof Write || message instanceof Read || message instanceof Rmw) {
                    Id id = new Id(index, sequence++);
                    response(id).command = message;
                    for (int r = 0; r < 3; r++) send(peers.get(r), new Propagate(id, message));
                } else if (message instanceof FastAckClient f && f.ballot == 0) {
                    ResponseState s = response(f.id);
                    if (!s.delivered) {
                        s.fast.add(new Vote(f.replica, null, f.checksum), false);
                        finish(f.id, s);
                    }
                } else if (message instanceof LightSlowAck a && a.ballot == 0) {
                    ResponseState s = response(a.id);
                    if (!s.delivered) {
                        Vote vote = new Vote(a.replica, null, null);
                        s.fast.add(vote, false);
                        finish(a.id, s);
                        if (!s.delivered) { s.slow.add(vote, false); finish(a.id, s); }
                    }
                } else if (message instanceof Reply r && r.ballot == 0 && r.replica == 0) {
                    ResponseState s = response(r.id);
                    if (!s.delivered) {
                        s.value = r.value;
                        s.fast.add(new Vote(0, null, r.checksum), true);
                        finish(r.id, s);
                        if (!s.delivered) { s.slow.add(new Vote(0, null, null), true); finish(r.id, s); }
                    }
                } else throw new AssertionError("unexpected library message " + message);
            }
        }
        void finish(Id id, ResponseState s) {
            if (s.delivered || !(s.fast.ready() || s.slow.ready())) return;
            s.delivered = true;
            MpMessage reply = s.command instanceof Write ? new Ack()
                : s.command instanceof Read ? new Value(s.value) : new Result(s.value);
            send(peers.get(5 + index), reply);
        }
    }
}
