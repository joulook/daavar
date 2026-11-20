package bench.swiftpaxos_code;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

/**
 * SwiftPaxos (github.com/imdea-software/swiftpaxos, swift/), ported to JMC's message-passing API:
 * ballot 0, leader n0, fast quorum {n0, n1}, slow quorums = majorities, one integer register.
 * See NOTES.md next to this file for the protocol, message by message, and every decision.
 */
public final class SwiftpaxosCode implements MpProgram {

    // ------------------------------------------------------------------ instance parameters

    static final int N = 3;
    static final int CLIENTS = 2;
    static final int LEADER = 0; // replica.Leader(ballot 0, N) = 0
    static final int BALLOT = 0;

    // Command kinds (the interface's Write / Read / Rmw).
    static final int WRITE = 0;
    static final int READ = 1;
    static final int RMW = 2;

    // Phases (swift/defs.go: START, PRE_ACCEPT, ACCEPT, COMMIT).
    static final int START = 0;
    static final int PRE_ACCEPT = 1;
    static final int ACCEPT = 2;
    static final int COMMIT = 3;

    private final int q;
    private final CommunicationModel model;
    private final String delivery;

    /**
     * Constructed from the launcher's argument string {@code impl,<q>,<fifo|bag>}.
     *
     * @param args {@code impl}, the number of commands per client, the delivery model
     */
    public SwiftpaxosCode(String... args) {
        if (args.length < 2 || args.length > 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<q>[,<fifo|bag>], got " + String.join(",", args));
        }
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q must be >= 1, was " + q);
        }
        this.delivery = args.length == 3 ? args[2] : "fifo";
        switch (delivery) {
            case "fifo" -> this.model = CommunicationModel.P2P;
            case "bag" -> this.model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("delivery must be fifo or bag, was " + delivery);
        }
    }

    @Override
    public String programName() {
        return "swiftpaxos_code";
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
        return List.of("c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public String toString() {
        return "swiftpaxos_code impl q=" + q + " " + delivery;
    }

    // ------------------------------------------------------------------ messages

    // client -> its library (shape fixed by the interface)
    @Tag("Write")
    record Write(int client, int value) implements MpMessage {}

    @Tag("Read")
    record Read(int client) implements MpMessage {}

    @Tag("Rmw")
    record Rmw(int client) implements MpMessage {}

    // library -> client (shape fixed by the interface)
    @Tag("Ack")
    record Ack() implements MpMessage {}

    @Tag("Value")
    record Value(int value) implements MpMessage {}

    @Tag("Result")
    record Result(int value) implements MpMessage {}

    /** CommandId {ClientId, SeqNum} (swift/defs.go). */
    @Tag("CmdId")
    record CmdId(int client, int seq) implements MpMessage {}

    /**
     * SHash: what dpath.go's sha256 chain computes, kept as the chain itself: the command ids
     * hashed in order, starting from the zero hash (the empty chain).
     */
    @Tag("Hash")
    record Hash(List<CmdId> chain) implements MpMessage {
        static final Hash ZERO = new Hash(List.of());

        Hash then(CmdId id) {
            List<CmdId> c = new ArrayList<>(chain);
            c.add(id);
            return new Hash(List.copyOf(c));
        }
    }

    /** GPropose / Propose: library -> every replica. kind is WRITE/READ/RMW, value used by WRITE. */
    @Tag("Propagate")
    record Propagate(int client, int seq, int kind, int value) implements MpMessage {
        CmdId cmdId() {
            return new CmdId(client, seq);
        }
    }

    /** MFastAck (sent as a one-element MOptAcks by the batcher): replica -> the other replicas. */
    @Tag("FastAck")
    record FastAck(int replica, int ballot, CmdId cmdId, List<CmdId> dep, List<Hash> checksum, int seqnum)
            implements MpMessage {}

    /** MLightSlowAck: follower -> the other replicas and -> the command's library. */
    @Tag("LightSlowAck")
    record LightSlowAck(int replica, int ballot, CmdId cmdId) implements MpMessage {}

    /** MFastAckClient: fast-quorum follower -> the command's library. */
    @Tag("FastAckClient")
    record FastAckClient(int replica, int ballot, CmdId cmdId, List<Hash> checksum) implements MpMessage {}

    /** MReply: leader -> the command's library, after its (optimistic) execution. */
    @Tag("Reply")
    record Reply(int replica, int ballot, CmdId cmdId, List<Hash> checksum, int rep) implements MpMessage {}

    // ------------------------------------------------------------------ quorums and message sets

    /** replica.QuorumI. */
    interface Quorum {
        int size();

        boolean contains(int rep);
    }

    /** replica.Majority: size N/2+1, contains every replica. */
    static Quorum majority(int n) {
        int s = n / 2 + 1;
        return new Quorum() {
            public int size() {
                return s;
            }

            public boolean contains(int rep) {
                return true;
            }
        };
    }

    /** replica.Quorum: an explicit set. */
    static Quorum quorumOf(int... reps) {
        Set<Integer> set = new HashSet<>();
        for (int r : reps) {
            set.add(r);
        }
        return new Quorum() {
            public int size() {
                return set.size();
            }

            public boolean contains(int rep) {
                return set.contains(rep);
            }
        };
    }

    /** The fast quorum of configuration (C2): {n0, n1} (r.FQ = r.qs.AQ(ballot 0)). */
    static final Quorum FQ = quorumOf(0, 1);
    /** r.SQ = replica.NewMajorityOf(N). */
    static final Quorum SQ = majority(N);

    /** replica.MsgSet (replica/mset.go), verbatim in behaviour. */
    static final class MsgSet<T> {
        private final Quorum q;
        private Map<Integer, T> msgs = new LinkedHashMap<>();
        private T leaderMsg;
        private final BiPredicate<T, T> accept;
        private final BiConsumer<T, List<T>> handler;

        MsgSet(Quorum q, BiPredicate<T, T> accept, BiConsumer<T, List<T>> handler) {
            this.q = q;
            this.accept = accept;
            this.handler = handler;
        }

        boolean add(int repId, boolean isLeader, T msg) {
            if (!q.contains(repId)) {
                return false;
            }
            if (leaderMsg != null && msgs.containsKey(repId)) {
                return false;
            }
            boolean added = false;
            if (isLeader) {
                leaderMsg = msg;
                Map<Integer, T> newMsgs = new LinkedHashMap<>();
                for (Map.Entry<Integer, T> e : msgs.entrySet()) {
                    if (!newMsgs.containsKey(e.getKey()) && accept.test(e.getValue(), leaderMsg)) {
                        newMsgs.put(e.getKey(), e.getValue());
                    }
                }
                msgs = newMsgs;
                added = true;
            } else if (accept.test(msg, leaderMsg)) {
                msgs.put(repId, msg);
                added = true;
            }
            if (msgs.size() >= q.size() || (msgs.size() >= q.size() - 1 && leaderMsg != null)) {
                handler.accept(leaderMsg, new ArrayList<>(msgs.values()));
            }
            return added;
        }
    }

    // ------------------------------------------------------------------ helpers from defs.go / dpath.go

    /** Dep.Equals (swift/defs.go); a null dep is Go's nil slice (length 0). */
    static boolean depEquals(List<CmdId> d1, List<CmdId> d2) {
        List<CmdId> a = d1 == null ? List.of() : d1;
        List<CmdId> b = d2 == null ? List.of() : d2;
        if (a.size() != b.size()) {
            return false;
        }
        Set<CmdId> seen1 = new HashSet<>();
        Set<CmdId> seen2 = new HashSet<>();
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i).equals(b.get(i))) {
                continue;
            }
            if (seen2.contains(a.get(i))) {
                seen2.remove(a.get(i));
            } else {
                seen1.add(a.get(i));
            }
            if (seen1.contains(b.get(i))) {
                seen1.remove(b.get(i));
            } else {
                seen2.add(b.get(i));
            }
        }
        return seen1.size() == seen2.size() && seen1.isEmpty();
    }

    /** SHashesEq (swift/dpath.go); a null checksum is Go's nil slice (length 0). */
    static boolean shashesEq(List<Hash> h1, List<Hash> h2) {
        List<Hash> a = h1 == null ? List.of() : h1;
        List<Hash> b = h2 == null ? List.of() : h2;
        if (a.size() != b.size()) {
            return false;
        }
        for (Hash h : a) {
            if (!b.contains(h)) {
                return false;
            }
        }
        return true;
    }

    /** HashLog (swift/dpath.go) for the one key; the pending linked list as a list. */
    static final class HashLog {
        private int synced = -1;
        private Hash syncedHash = Hash.ZERO;
        private final List<CmdId> pending = new ArrayList<>();
        private Hash hash = Hash.ZERO;
        private final Map<CmdId, int[]> pendingUpdSeq = new HashMap<>();
        private final Map<CmdId, Hash> pendingUpdHash = new HashMap<>();

        Hash append(CmdId cmdId) {
            if (pendingUpdHash.containsKey(cmdId)) {
                Hash h = pendingUpdHash.remove(cmdId);
                int s = pendingUpdSeq.remove(cmdId)[0];
                update(cmdId, s, h, false);
                return h;
            }
            if (pending.isEmpty()) {
                pending.add(cmdId);
                hash = syncedHash.then(cmdId);
                return hash;
            }
            pending.add(cmdId);
            hash = hash.then(cmdId);
            return hash;
        }

        void update(CmdId cmdId, int s, Hash h, boolean save) {
            boolean exists = pending.contains(cmdId);
            if (!exists && save) {
                pendingUpdHash.put(cmdId, h);
                pendingUpdSeq.put(cmdId, new int[] {s});
                return;
            }
            if (exists) {
                pending.remove(cmdId);
            }
            if (s <= synced && !exists) {
                return;
            }
            if (s > synced) {
                synced = s;
                syncedHash = h;
            }
            Hash initial = syncedHash;
            for (CmdId id : pending) {
                initial = initial.then(id);
            }
            hash = initial;
        }
    }

    // ------------------------------------------------------------------ replica (swift/swift.go)

    /** A received or locally made MFastAck; dep == null is a light slow ack (Go's nil Dep). */
    record RAck(int replica, int ballot, CmdId cmdId, List<CmdId> dep, List<Hash> checksum, int seqnum) {}

    private static final Object DELIVER = new Object();

    static final class Replica {
        final int id;
        final Tid[] replicas;
        final Tid[] libs;
        final CommunicationModel model;
        final boolean optExec = true;
        final boolean fastRead = false;
        final boolean proxy = false; // GPropose.Proxy (Config.Proxy.IsProxy): no proxy configuration

        int x = 0; // the register (r.State)
        final Map<CmdId, Desc> cmdDescs = new HashMap<>();
        final Set<CmdId> delivered = new HashSet<>();
        // lightKeyInfo of the one key (null until the key is first used, as r.keys)
        List<CmdId> lastWrite = null;
        List<CmdId> lastCmd = null;
        HashLog hlog = null;
        int seqnum = 0;
        final Map<CmdId, Hash> pendingHashUpdHash = new HashMap<>();
        final Map<CmdId, Integer> pendingHashUpdSeq = new HashMap<>();
        final Map<CmdId, Propagate> proposes = new HashMap<>();
        final ArrayDeque<CmdId> deliverChan = new ArrayDeque<>();

        final class Desc {
            int phase = START;
            Propagate cmd;
            List<CmdId> dep = null;
            List<Hash> hs = null;
            Propagate propose = null;
            List<CmdId> proposeDep = null;
            MsgSet<RAck> slowPathH;
            MsgSet<RAck> fastPathH;
            Runnable afterPropagatePending = null;
            boolean slowPath = false;
            final List<CmdId> successors = new ArrayList<>();

            /** hook.OptCondF.Call with condition desc.propose != nil. */
            void afterPropagateCall(Runnable f) {
                if (propose != null) {
                    f.run();
                } else {
                    afterPropagatePending = f;
                }
            }

            /** hook.OptCondF.Recall: runs the stored function if the condition now holds. */
            boolean afterPropagateRecall() {
                if (propose != null && afterPropagatePending != null) {
                    Runnable f = afterPropagatePending;
                    afterPropagatePending = null;
                    f.run();
                    return true;
                }
                return false;
            }
        }

        Replica(int id, Tid[] replicas, Tid[] libs, CommunicationModel model) {
            this.id = id;
            this.replicas = replicas;
            this.libs = libs;
            this.model = model;
        }

        int leader() {
            return LEADER;
        }

        void sendToAll(MpMessage m) {
            // sender.sendToAll: every peer that is Alive, i.e. every replica but this one
            for (int p = 0; p < N; p++) {
                if (p != id) {
                    Channel.send(model, replicas[p], m);
                }
            }
        }

        void sendToClient(int clientId, MpMessage m) {
            Channel.send(model, libs[clientId], m);
        }

        /** Replica.run: the event loop; never returns. */
        void run() {
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Propagate propose) {
                    CmdId cmdId = propose.cmdId();
                    proposes.put(cmdId, propose);
                    List<CmdId> dep = new ArrayList<>();
                    List<Hash> hs = new ArrayList<>();
                    getDepAndHashes(propose, cmdId, dep, hs);
                    if (pendingHashUpdHash.containsKey(cmdId) && leader() != id) {
                        Hash h = pendingHashUpdHash.remove(cmdId);
                        int s = pendingHashUpdSeq.remove(cmdId);
                        recordLeaderHash(cmdId, s, List.of(h));
                    }
                    Desc desc = getCmdDescSeq(cmdId, propose, List.copyOf(dep), List.copyOf(hs));
                    if (desc == null) {
                        throw new IllegalStateException("Got proposal for the delivered command " + cmdId);
                    }
                } else if (m instanceof FastAck f) {
                    if (f.replica() == leader() && leader() != id) {
                        recordLeaderHash(f.cmdId(), f.seqnum(), f.checksum());
                    }
                    getCmdDescSeq(f.cmdId(),
                            new RAck(f.replica(), f.ballot(), f.cmdId(), f.dep(), f.checksum(), f.seqnum()),
                            null, null);
                } else if (m instanceof LightSlowAck ls) {
                    getCmdDescSeq(ls.cmdId(), ls, null, null);
                } else {
                    throw new IllegalStateException("replica n" + id + ": unexpected message " + m);
                }
                // deliverChan: successors of executed commands, handled by the loop after the message
                while (!deliverChan.isEmpty()) {
                    getCmdDescSeq(deliverChan.poll(), DELIVER, null, null);
                }
            }
        }

        void handlePropose(Propagate msg, Desc desc, CmdId cmdId) {
            if (desc.propose != null) {
                return;
            }
            desc.propose = msg;
            desc.cmd = msg;
            if (!FQ.contains(id)) {
                desc.afterPropagateRecall();
                return;
            }
            desc.dep = desc.proposeDep;
            desc.phase = PRE_ACCEPT;
            if ((desc.afterPropagateRecall() && desc.slowPath) || delivered.contains(cmdId)) {
                // a slow ack was already sent, no FastAck
                return;
            }
            int s = 0;
            if (id == leader()) {
                s = seqnum;
                seqnum++;
            }
            RAck fastAck = new RAck(id, BALLOT, cmdId, desc.dep, desc.hs, s);
            boolean isRead = fastRead && msg.kind() != WRITE;
            FastAck wire = new FastAck(id, BALLOT, cmdId, desc.dep, desc.hs, s);
            if (!optExec) {
                sendToAll(wire);
            } else if ((id == leader() && !isRead) || (isRead && proxy)) {
                sendToAll(wire);
                deliver(desc, cmdId);
            } else {
                // batcher.SendFastAckClient: MFastAckClient to the client, then the acks to all
                sendToClient(cmdId.client(), new FastAckClient(id, BALLOT, cmdId, desc.hs));
                sendToAll(wire);
            }
            handleFastAck(fastAck, desc);
        }

        void handleFastAck(RAck msg, Desc desc) {
            if (msg.replica() == leader()) {
                fastAckFromLeader(msg, desc);
            } else {
                commonCaseFastAck(msg, desc);
            }
        }

        void fastAckFromLeader(RAck msg, Desc desc) {
            desc.afterPropagateCall(() -> {
                if (BALLOT != msg.ballot()) {
                    return;
                }
                desc.phase = ACCEPT;
                List<CmdId> dep = msg.dep();
                List<Hash> hs = desc.hs;
                boolean neq = !depEquals(desc.dep, dep);
                boolean fast = FQ.contains(id);
                boolean slow = SQ.contains(id);
                boolean sendSlowAck = leader() != id && (slow || (fast && neq));
                CmdId msgCmdId = msg.cmdId();
                List<Hash> msgChecksum = msg.checksum();

                desc.slowPathH.add(msg.replica(), true, msg);
                boolean isDelivered = delivered.contains(msgCmdId);
                if (!isDelivered) {
                    desc.fastPathH.add(msg.replica(), true, msg);
                    isDelivered = delivered.contains(msgCmdId);
                }
                if (sendSlowAck) {
                    if (neq && !isDelivered) {
                        desc.dep = dep;
                    }
                    desc.slowPath = true;
                    LightSlowAck lightSlowAck = new LightSlowAck(id, BALLOT, msgCmdId);
                    if (!optExec) {
                        sendToAll(lightSlowAck);
                    } else {
                        // batcher.SendLightSlowAckClient: to the client, then to all
                        sendToClient(msgCmdId.client(), lightSlowAck);
                        sendToAll(lightSlowAck);
                    }
                    if (!isDelivered) {
                        handleLightSlowAck(lightSlowAck, desc);
                    }
                }
                // the deferred function
                if (leader() == id || delivered.contains(msgCmdId)) {
                    return;
                }
                if (!sendSlowAck && optExec && !shashesEq(hs, msgChecksum)) {
                    sendToClient(msgCmdId.client(), new LightSlowAck(id, BALLOT, msgCmdId));
                }
            });
        }

        void commonCaseFastAck(RAck msg, Desc desc) {
            if (BALLOT != msg.ballot()) {
                return;
            }
            CmdId msgCmdId = msg.cmdId();
            if (msg.dep() == null) {
                desc.slowPathH.add(msg.replica(), msg.replica() == leader(), msg);
                if (delivered.contains(msgCmdId)) {
                    return;
                }
            }
            desc.fastPathH.add(msg.replica(), msg.replica() == leader(), msg);
        }

        void handleLightSlowAck(LightSlowAck msg, Desc desc) {
            commonCaseFastAck(new RAck(msg.replica(), msg.ballot(), msg.cmdId(), null, null, 0), desc);
        }

        /** getFastAndSlowAcksHandler. */
        void fastAndSlowAcks(Desc desc, RAck leaderMsg) {
            if (leaderMsg == null) {
                return;
            }
            desc.phase = COMMIT;
            if (desc.dep != null) {
                for (CmdId depCmdId : desc.dep) {
                    Desc depDesc = getCmdDescSeq(depCmdId, null, null, null);
                    if (depDesc == null) {
                        continue;
                    }
                    depDesc.successors.add(leaderMsg.cmdId());
                }
            }
            deliver(desc, leaderMsg.cmdId());
        }

        void deliver(Desc desc, CmdId cmdId) {
            if (desc.propose == null || delivered.contains(cmdId)) {
                return;
            }
            if (desc.phase != COMMIT && (!optExec || id != leader())) {
                return;
            }
            if (desc.dep != null) {
                for (CmdId d : desc.dep) {
                    if (!delivered.contains(d)) {
                        return;
                    }
                }
            }
            delivered.add(cmdId);
            int v = execute(desc.cmd);
            deliverChan.addAll(desc.successors);
            // repchan.reply
            if (optExec && id == leader()) {
                List<Hash> hs = desc.hs == null ? List.of() : List.copyOf(desc.hs);
                sendToClient(cmdId.client(), new Reply(id, BALLOT, cmdId, hs, v));
            }
            // (!optExec && Proxy: MAccept; optExec && Proxy at a follower: nothing)
        }

        /** Command.Execute on the register. Write returns NIL, here 0. */
        int execute(Propagate c) {
            switch (c.kind()) {
                case WRITE -> {
                    x = c.value();
                    return 0;
                }
                case READ -> {
                    return x;
                }
                case RMW -> {
                    x = x + 10;
                    return x;
                }
                default -> throw new IllegalStateException("unknown command kind " + c.kind());
            }
        }

        Desc getCmdDescSeq(CmdId cmdId, Object msg, List<CmdId> dep, List<Hash> hs) {
            if (delivered.contains(cmdId)) {
                return null;
            }
            Desc desc = cmdDescs.get(cmdId);
            if (desc == null) {
                desc = newDesc();
                cmdDescs.put(cmdId, desc);
            }
            if (dep != null && desc.proposeDep == null) {
                desc.proposeDep = dep;
                if (hs != null) {
                    desc.hs = hs;
                }
            }
            if (msg != null) {
                handleMsg(msg, desc, cmdId);
            }
            return desc;
        }

        Desc newDesc() {
            Desc desc = new Desc();
            BiPredicate<RAck, RAck> accept = (msg, leaderMsg) ->
                    leaderMsg == null || msg.dep() == null || depEquals(leaderMsg.dep(), msg.dep());
            BiConsumer<RAck, List<RAck>> h = (leaderMsg, msgs) -> fastAndSlowAcks(desc, leaderMsg);
            desc.slowPathH = new MsgSet<>(SQ, accept, h);
            desc.fastPathH = new MsgSet<>(FQ, accept, h);
            return desc;
        }

        void handleMsg(Object m, Desc desc, CmdId cmdId) {
            if (m instanceof Propagate p) {
                handlePropose(p, desc, cmdId);
            } else if (m instanceof RAck f) {
                if (f.cmdId().equals(cmdId)) {
                    handleFastAck(f, desc);
                }
            } else if (m instanceof LightSlowAck ls) {
                if (ls.cmdId().equals(cmdId)) {
                    handleLightSlowAck(ls, desc);
                }
            } else if (m == DELIVER) {
                deliver(desc, cmdId);
            }
        }

        /** getDepAndHashes for the one key, with lightKeyInfo (key.go). */
        void getDepAndHashes(Propagate cmd, CmdId cmdId, List<CmdId> dep, List<Hash> hashes) {
            boolean isGet = cmd.kind() == READ; // Write and Rmw are PUT-class
            if (lastCmd != null) {
                dep.addAll(isGet ? lastWrite : lastCmd);
            } else {
                lastWrite = List.of();
                lastCmd = List.of();
            }
            lastCmd = List.of(cmdId);
            if (!isGet) {
                lastWrite = List.of(cmdId);
            }
            if (hlog == null) {
                hlog = new HashLog();
            }
            hashes.add(hlog.append(cmdId));
        }

        void recordLeaderHash(CmdId cmdId, int s, List<Hash> hs) {
            Propagate p = proposes.get(cmdId);
            if (p == null) {
                pendingHashUpdHash.put(cmdId, hs.get(0));
                pendingHashUpdSeq.put(cmdId, s);
                return;
            }
            // updateLogs: one key, so one hash
            if (hs.size() != 1) {
                throw new IllegalStateException("the number of hashes does not match number of objects");
            }
            if (hlog == null) {
                hlog = new HashLog();
            }
            hlog.update(cmdId, s, hs.get(0), true);
        }
    }

    // ------------------------------------------------------------------ client library (swift/client.go)

    /** An MFastAck as the client side builds it from Reply / FastAckClient / LightSlowAck. */
    record CAck(int replica, int ballot, CmdId cmdId, List<Hash> checksum) {}

    static final class Library {
        final int i;
        final Tid client;
        final Tid[] replicas;
        final CommunicationModel model;

        int seqnum = -1;
        int kind = -1;
        int val = 0;
        int ballot = -1;
        final Set<CmdId> delivered = new HashSet<>();
        final Map<CmdId, MsgSet<CAck>> slowPathH = new HashMap<>();
        final Map<CmdId, MsgSet<CAck>> fastPathH = new HashMap<>();
        // client side: c.SQ = Majority(N); c.FQ: the fast quorum {n0, n1} (see NOTES.md)
        final Quorum cSQ = majority(N);
        final Quorum cFQ = FQ;

        Library(int i, Tid client, Tid[] replicas, CommunicationModel model) {
            this.i = i;
            this.client = client;
            this.replicas = replicas;
            this.model = model;
        }

        void run() {
            while (true) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write w) {
                    propose(WRITE, w.value());
                } else if (m instanceof Read) {
                    propose(READ, 0);
                } else if (m instanceof Rmw) {
                    propose(RMW, 0);
                } else if (m instanceof Reply r) {
                    handleReply(r);
                } else if (m instanceof FastAckClient f) {
                    handleFastAck(new CAck(f.replica(), f.ballot(), f.cmdId(), f.checksum()), false);
                } else if (m instanceof LightSlowAck ls) {
                    handleLightSlowAck(ls);
                } else {
                    throw new IllegalStateException("library l" + i + ": unexpected message " + m);
                }
            }
        }

        /** SendWrite / SendRead with Fast = true: the proposal goes to every replica. */
        void propose(int k, int value) {
            seqnum++;
            kind = k;
            Propagate p = new Propagate(i, seqnum, k, value);
            for (int rep = 0; rep < N; rep++) {
                Channel.send(model, replicas[rep], p);
            }
        }

        void initMsgSets(CmdId cmdId) {
            BiPredicate<CAck, CAck> accept = (msg, leaderMsg) -> {
                if (leaderMsg == null) {
                    return true;
                }
                return shashesEq(leaderMsg.checksum(), msg.checksum());
            };
            BiConsumer<CAck, List<CAck>> h = (leaderMsg, msgs) -> handleFastAndSlowAcks(leaderMsg);
            slowPathH.computeIfAbsent(cmdId, c -> new MsgSet<>(cSQ, accept, h));
            fastPathH.computeIfAbsent(cmdId, c -> new MsgSet<>(cFQ, accept, h));
        }

        boolean handleFastAck(CAck f, boolean fromLeader) {
            if (ballot == -1) {
                ballot = f.ballot();
            } else if (ballot < f.ballot()) {
                ballot = f.ballot();
            } else if (ballot > f.ballot()) {
                return false;
            }
            if (delivered.contains(f.cmdId())) {
                return false;
            }
            initMsgSets(f.cmdId());
            fastPathH.get(f.cmdId()).add(f.replica(), fromLeader, f);
            return true;
        }

        void handleLightSlowAck(LightSlowAck ls) {
            if (delivered.contains(ls.cmdId())) {
                return;
            }
            CAck f = new CAck(ls.replica(), ls.ballot(), ls.cmdId(), null);
            handleFastAck(f, false);
            if (!delivered.contains(f.cmdId())) {
                initMsgSets(f.cmdId());
                slowPathH.get(f.cmdId()).add(f.replica(), false, f);
            }
        }

        void handleFastAndSlowAcks(CAck leaderMsg) {
            if (leaderMsg == null) {
                return;
            }
            CmdId cmdId = leaderMsg.cmdId();
            if (delivered.contains(cmdId)) {
                return;
            }
            delivered.add(cmdId);
            registerReply(val, cmdId.seq());
        }

        void handleReply(Reply r) {
            if (delivered.contains(r.cmdId())) {
                return;
            }
            CAck f = new CAck(r.replica(), r.ballot(), r.cmdId(), r.checksum());
            val = r.rep();
            handleFastAck(f, true);
            if (!delivered.contains(f.cmdId())) {
                CAck g = new CAck(f.replica(), f.ballot(), f.cmdId(), null);
                initMsgSets(g.cmdId());
                slowPathH.get(g.cmdId()).add(g.replica(), true, g);
            }
        }

        /** BufferClient.RegisterReply, then the answer to the client process. */
        void registerReply(int v, int seq) {
            if (seq != seqnum) {
                throw new IllegalStateException("library l" + i + ": reply for " + seq + " while at " + seqnum);
            }
            MpMessage answer = switch (kind) {
                case WRITE -> new Ack();
                case READ -> new Value(v);
                case RMW -> new Result(v);
                default -> throw new IllegalStateException("no command pending at l" + i);
            };
            kind = -1;
            Channel.send(model, client, answer);
        }
    }

    // ------------------------------------------------------------------ client (the interface's code)

    static int value(int i, int j) {
        return 1 + i + CLIENTS * j;
    }

    static void client(int i, int q, Tid node, CommunicationModel model) {
        for (int j = 0; j < q; j++) {
            switch ((i + j) % 3) {
                case 0 -> {
                    Channel.send(model, node, new Write(i, value(i, j)));
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

    // ------------------------------------------------------------------ main

    @Override
    public void run() {
        Tid[] reps = new Tid[N];
        Tid[] libs = new Tid[CLIENTS];
        Tid[] clients = new Tid[CLIENTS];
        CommunicationModel m = model;
        int cmds = q;
        // Every process is created (its Tid minted) before any is started, so every body reads
        // complete address arrays; nothing runs before start().
        List<MpThread> order = new ArrayList<>();
        for (int k = 0; k < N; k++) {
            int id = k;
            MpThread t = new MpThread(() -> new Replica(id, reps, libs, m).run(), true);
            reps[k] = Tid.of(t.getTaskId());
            order.add(t);
        }
        for (int k = 0; k < CLIENTS; k++) {
            int id = k;
            MpThread t = new MpThread(() -> new Library(id, clients[id], reps, m).run(), true);
            libs[k] = Tid.of(t.getTaskId());
            order.add(t);
        }
        List<MpThread> clientThreads = new ArrayList<>();
        for (int k = 0; k < CLIENTS; k++) {
            int id = k;
            String role = "c" + k;
            MpThread t = new MpThread(() -> client(id, cmds, libs[id], m), role);
            clients[k] = Tid.of(t.getTaskId(), role);
            order.add(t);
            clientThreads.add(t);
        }
        for (MpThread t : order) {
            t.start();
        }
        for (MpThread t : clientThreads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting a client", e);
            }
        }
    }
}
