package bench.pineapple_code_sol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Single-key translation of the supplied Go handlers; see NOTES.md for adaptations. */
public final class PineappleCodeSol implements MpProgram {
    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}
    @Tag("PineappleTag") public record Version(int timestamp, int id) {}
    @Tag("Payload") public record Payload(Version tag, int value) {}
    @Tag("Get") public record Get(int replicaID, int instance, int write, int key,
                                   Payload payload) implements MpMessage {}
    @Tag("GetReply") public record GetReply(int replicaID, int instance, int ok, int write,
                                             int key, Payload payload) implements MpMessage {}
    @Tag("Set") public record Put(int replicaID, int instance, int write, int key,
                                   Payload payload) implements MpMessage {}
    @Tag("SetReply") public record SetReply(int instance) implements MpMessage {}
    @Tag("RMWGet") public record RMWGet(int leaderId, int instance, int ballot,
                                         MpMessage command) implements MpMessage {}
    @Tag("RMWGetReply") public record RMWGetReply(int instance, int ballot, int key,
                                                  Payload payload) implements MpMessage {}
    @Tag("RMWSet") public record RMWSet(int leaderId, int instance, int ballot,
                                         MpMessage command, int key, Payload payload) implements MpMessage {}
    @Tag("RMWSetReply") public record RMWSetReply(int instance, int ok, int ballot) implements MpMessage {}
    // Schedules replica 0's executor within its one process (pineapple.go:584-608,718-720).
    @Tag("ExecuteRMWs") public record ExecuteRMWs() implements MpMessage {}

    private static final Payload ZERO = new Payload(new Version(0, 0), 0);
    private static final int PREPARING = 0, PREPARED = 1, ACCEPTED = 2, COMMITTED = 3;
    private final int q;
    private final CommunicationModel communication;

    public PineappleCodeSol(String... args) {
        if (args.length != 3 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>");
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be >= 1");
        communication = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    public String programName() { return "pineapple"; }
    public String size() { return Integer.toString(q); }
    public CommunicationModel model() { return communication; }
    public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    public List<String> visibleRoles() { return List.of("c0", "c1"); }
    public String toString() { return "PineappleCodeSol impl," + q + "," + (communication == CommunicationModel.P2P ? "fifo" : "bag"); }

    public void run() {
        // Only initialization is shared. All five addresses are set before any thread starts.
        Tid[] addresses = new Tid[5];
        MpThread[] threads = new MpThread[5];
        for (int i = 0; i < 3; i++) {
            Node node = new Node(i, addresses);
            threads[i] = new MpThread(node, "n" + i);
        }
        for (int i = 0; i < 2; i++) {
            final int client = i;
            threads[3 + i] = new MpThread(() -> client(client, addresses), "c" + i);
        }
        for (int i = 0; i < 5; i++) addresses[i] = Tid.of(threads[i].getTaskId(), rolesInSpawnOrder().get(i));
        for (MpThread thread : threads) thread.start();
        try {
            // Await clients first; then await the perpetually receiving nodes.
            for (int i : new int[] {3, 4, 0, 1, 2}) threads[i].join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void client(int i, Tid[] addresses) {
        for (int j = 0; j < q; j++) {
            int kind = (i + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(i, 1 + i + 2 * j);
                case 1 -> new Read(i);
                default -> new Rmw(i);
            };
            Channel.send(communication, addresses[i], request);
            MpMessage reply = Channel.recv(communication);
            if (!(kind == 0 && reply instanceof Ack || kind == 1 && reply instanceof Value
                    || kind == 2 && reply instanceof Result)) throw new AssertionError("unexpected reply " + reply);
        }
    }

    private static final class Instance {
        MpMessage command;
        MpMessage proposal; // null is the Go nil leader bookkeeping on an acceptor.
        Version initialTag = ZERO.tag();
        Payload receivedRMW = ZERO;
        int ballot, status = PREPARING, rmwId;
        int getOKs, setOKs, rmwGetOKs, rmwSetOKs;
        boolean getDone, rmwGetDone, completed;
        final Set<Integer> hasMaxTag = new HashSet<>();
        final List<GetReply> receivedData = new ArrayList<>();
        final List<Payload> receivedRMWData = new ArrayList<>();
        Instance(MpMessage command, int ballot, MpMessage proposal) {
            this.command = command;
            this.ballot = ballot;
            this.proposal = proposal;
        }
    }

    private final class Node implements Runnable {
        final int id;
        final Tid[] addresses;
        final Map<Integer, Instance> instances = new HashMap<>();
        final Map<Integer, Instance> pendingRMWs = new HashMap<>();
        Payload data = ZERO;
        boolean exists; // Distinguishes a Go absent map entry from its zero-value lookup.
        int crtInstance, crtRmwId, rmwDoneUpTo = -1, nextExecute;
        Node(int id, Tid[] addresses) { this.id = id; this.addresses = addresses; }
        void send(int destination, MpMessage message) { Channel.send(communication, addresses[destination], message); }
        void broadcast(MpMessage message) {
            for (int offset = 1; offset < 3; offset++) send((id + offset) % 3, message);
        }
        boolean isLargerTag(Version current, Version received) {
            if (received.timestamp() > current.timestamp()) return true;
            if (received.timestamp() == current.timestamp()) {
                if (current.id() == received.id()) return false;
                if (id == 0 && current.id() == id) return true;
                return current.id() < received.id();
            }
            return false;
        }
        void install(Payload payload) { data = payload; exists = true; }
        void installIfLarger(Payload payload) {
            if (isLargerTag(data.tag(), payload.tag())) install(payload);
        }
        void replyClient(Instance inst) {
            if (inst.proposal == null || inst.completed) return;
            int client;
            MpMessage reply;
            if (inst.proposal instanceof Write w) { client = w.client(); reply = new Ack(); }
            else if (inst.proposal instanceof Read r) { client = r.client(); reply = new Value(data.value()); }
            else { client = ((Rmw) inst.proposal).client(); reply = new Result(data.value()); }
            inst.completed = true;
            send(3 + client, reply);
        }
        void propose(MpMessage proposal) {
            while (instances.containsKey(crtInstance)) crtInstance++;
            int number = crtInstance;
            Instance inst = new Instance(proposal, 0, proposal);
            instances.put(number, inst);
            if (proposal instanceof Rmw) {
                inst.rmwId = crtRmwId++;
                broadcast(new RMWGet(id, number, 0, proposal));
            } else if (proposal instanceof Write) {
                // Go never assigns the proposed write value (610-663,314-317).
                broadcast(new Get(id, number, 1, 0, ZERO));
            } else {
                if (!exists) install(new Payload(new Version(0, id), 0));
                inst.initialTag = data.tag();
                broadcast(new Get(id, number, 0, 0, data));
            }
        }
        void get(Get g) {
            Payload result = ZERO;
            if (g.write() == 0) {
                if (!exists || isLargerTag(data.tag(), g.payload().tag())) install(g.payload());
                result = data;
            }
            send(g.replicaID(), new GetReply(id, g.instance(), 1, g.write(), g.key(), result));
        }
        void getReply(GetReply reply) {
            Instance inst = instances.get(reply.instance());
            if (inst.getDone) return;
            inst.receivedData.add(reply);
            installIfLarger(reply.payload());
            if (reply.ok() != 1 || ++inst.getOKs + 1 <= (3 >> 1)) return;
            int identical = 0;
            Version own = data.tag(), first = inst.receivedData.get(0).payload().tag();
            for (GetReply r : inst.receivedData) {
                if (r.payload().tag().equals(first)) identical++;
                if (r.payload().tag().equals(own) && reply.write() == 0) inst.hasMaxTag.add(r.replicaID());
            }
            if (inst.initialTag.equals(first) || isLargerTag(inst.initialTag, first)) identical++;
            int count = inst.receivedData.size();
            inst.receivedData.clear();
            inst.getDone = true;
            if (reply.write() == 0 && identical == count + 1) { replyClient(inst); return; }
            inst.status = PREPARED;
            if (reply.write() == 1) install(new Payload(new Version(data.tag().timestamp() + 1, id), data.value()));
            Put set = new Put(id, reply.instance(), reply.write(), 0, data);
            for (int offset = 1; offset < 3; offset++) {
                int peer = (id + offset) % 3;
                if (reply.write() == 0 && inst.hasMaxTag.contains(peer)) continue;
                send(peer, set);
            }
        }
        void setReply(SetReply reply) {
            Instance inst = instances.get(reply.instance());
            inst.setOKs++;
            int max = inst.hasMaxTag.size();
            if (inst.setOKs + 1 > max && max < (3 >> 1) && max > 0
                    || inst.setOKs + 1 > (3 >> 1)) replyClient(inst);
        }
        void rmwGet(RMWGet g) {
            Instance inst = instances.get(g.instance());
            Payload result = ZERO;
            if (inst == null) {
                if (g.ballot() < 0) throw new AssertionError("outdated ballot received");
                inst = new Instance(g.command(), g.ballot(), null);
                inst.status = ACCEPTED;
                instances.put(g.instance(), inst);
            } else {
                if (g.ballot() < inst.ballot) throw new AssertionError("outdated ballot received");
                inst.command = g.command();
                if (inst.status != COMMITTED) inst.status = ACCEPTED;
                result = data;
            }
            send(g.leaderId(), new RMWGetReply(g.instance(), 0, 0, result));
        }
        void rmwGetReply(RMWGetReply reply) {
            Instance inst = instances.get(reply.instance());
            if (inst.rmwGetDone) return;
            inst.receivedRMWData.add(reply.payload());
            if (++inst.rmwGetOKs + 1 <= (3 >> 1)) return;
            for (Payload p : inst.receivedRMWData) {
                // Preserve the Go assignment of the current reply, rather than loop variable.
                if (isLargerTag(data.tag(), p.tag())) install(reply.payload());
            }
            inst.receivedRMWData.clear();
            inst.rmwGetDone = true;
            // Interface f replaces the +1 TODO at pineapple.go:483.
            install(new Payload(new Version(data.tag().timestamp() + 1, id), data.value() + 10));
            broadcast(new RMWSet(id, reply.instance(), reply.ballot(), inst.command, 0, data));
        }
        void rmwSet(RMWSet set) {
            Instance inst = instances.get(set.instance());
            if (inst == null) {
                if (set.ballot() < 0) throw new AssertionError("outdated ballot received");
                inst = new Instance(set.command(), set.ballot(), null);
                inst.status = ACCEPTED;
                instances.put(set.instance(), inst);
            } else if (inst.ballot > set.ballot()) throw new AssertionError("outdated ballot received");
            else if (inst.ballot < set.ballot()) {
                inst.command = set.command(); inst.ballot = set.ballot(); inst.status = ACCEPTED;
            } else {
                inst.command = set.command();
                if (inst.status != COMMITTED) inst.status = ACCEPTED;
            }
            inst.receivedRMW = set.payload();
            installIfLarger(inst.receivedRMW);
            send(set.leaderId(), new RMWSetReply(set.instance(), 1, 0));
        }
        void rmwSetReply(RMWSetReply reply) {
            Instance inst = instances.get(reply.instance());
            if (inst.rmwId <= rmwDoneUpTo) return;
            if (++inst.rmwSetOKs + 1 > (3 >> 1)) {
                pendingRMWs.put(inst.rmwId, inst);
                rmwDoneUpTo++;
                if (id == 0) send(id, new ExecuteRMWs());
                else replyClient(inst); // Missing nonzero-replica executor: NOTES.md.
            }
        }
        void executeRMWs() {
            while (nextExecute <= rmwDoneUpTo) {
                Instance inst = pendingRMWs.get(nextExecute);
                if (inst == null) throw new AssertionError("Go executor reached an empty pending RMW slot");
                replyClient(inst);
                nextExecute++;
            }
        }
        public void run() {
            for (;;) {
                MpMessage message = Channel.recv(communication);
                if (message instanceof Write || message instanceof Read || message instanceof Rmw) propose(message);
                else if (message instanceof Get g) get(g);
                else if (message instanceof GetReply r) getReply(r);
                else if (message instanceof Put s) { installIfLarger(s.payload()); send(s.replicaID(), new SetReply(s.instance())); }
                else if (message instanceof SetReply r) setReply(r);
                else if (message instanceof RMWGet g) rmwGet(g);
                else if (message instanceof RMWGetReply r) rmwGetReply(r);
                else if (message instanceof RMWSet s) rmwSet(s);
                else if (message instanceof RMWSetReply r) rmwSetReply(r);
                else if (message instanceof ExecuteRMWs) executeRMWs();
                else throw new AssertionError("unexpected node message " + message);
            }
        }
    }
}
