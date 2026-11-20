package bench.chainpaxos_code_sol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Fixed-leader, fixed-membership port; see NOTES.md for the source mapping. */
public final class ChainpaxosCodeSol implements MpProgram {
    @Tag("Write") public record Write(int client, int value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Rmw") public record Rmw(int client) implements MpMessage {}
    @Tag("Ack") public record Ack() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Result") public record Result(int value) implements MpMessage {}

    @Tag("Request") public record Request(int client, int sequence, MpMessage command)
            implements MpMessage {}
    @Tag("RedirectRequest") public record RedirectRequest(Request request) implements MpMessage {}
    @Tag("NoOp") public record NoOp() implements MpMessage {}
    @Tag("Accept") public record Accept(int instance, int counter, MpMessage value, int ack)
            implements MpMessage {}
    @Tag("AcceptAck") public record AcceptAck(int instance) implements MpMessage {}
    @Tag("ReadStarted") public record ReadStarted(int token) implements MpMessage {}
    @Tag("ReadReady") public record ReadReady(int token, int value) implements MpMessage {}
    @Tag("ReadRelease") public record ReadRelease(int token, int value) implements MpMessage {}

    private final int q;
    private final boolean local;
    private final CommunicationModel communication;

    public ChainpaxosCodeSol(String... args) {
        if (args.length != 4 || !args[0].equals("impl"))
            throw new IllegalArgumentException("expected impl,<q>,<mixed|local>,<fifo|bag>");
        q = Integer.parseInt(args[1]);
        if (q < 1) throw new IllegalArgumentException("q must be positive");
        local = switch (args[2]) {
            case "mixed" -> false;
            case "local" -> true;
            default -> throw new IllegalArgumentException("unknown variant: " + args[2]);
        };
        communication = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown delivery model: " + args[3]);
        };
    }

    @Override public String programName() { return "chainpaxos"; }
    @Override public String size() { return q + "," + (local ? "local" : "mixed"); }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1"); }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() { return "ChainpaxosCodeSol:impl," + size(); }

    @Override public void run() {
        // Mint every address before starting any body: main performs no message events.
        List<Tid> addresses = new ArrayList<>();
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int index = i;
            String role = rolesInSpawnOrder().get(i);
            MpThread thread = new MpThread(() -> {
                if (index < 3) new Replica(index, List.copyOf(addresses)).run();
                else client(index - 3, List.copyOf(addresses));
            }, role);
            threads.add(thread);
            addresses.add(Tid.of(thread.getTaskId(), role));
        }
        for (MpThread thread : threads) thread.start();
        // Await terminating clients first, then the permanently receiving replicas.
        for (int index : new int[] {3, 4, 0, 1, 2}) {
            try { threads.get(index).join1(); }
            catch (InterruptedException e) { throw new IllegalStateException(e); }
        }
    }

    private void client(int i, List<Tid> addresses) {
        Tid node = addresses.get(i == 0 ? 0 : 2);
        for (int j = 0; j < q; j++) {
            int kind = (i + j) % 3;
            MpMessage request = switch (kind) {
                case 0 -> new Write(i, 1 + i + 2 * j);
                case 1 -> new Read(i);
                default -> new Rmw(i);
            };
            Channel.send(communication, node, request);
            MpMessage reply = Channel.recv(communication);
            if (!(kind == 0 && reply instanceof Ack
                    || kind == 1 && reply instanceof Value
                    || kind == 2 && reply instanceof Result))
                throw new AssertionError("unexpected reply " + reply);
        }
    }

    private static final class Instance {
        final int number;
        MpMessage value;
        int counter;
        boolean decided;
        final List<Integer> reads = new ArrayList<>();
        Instance(int number) { this.number = number; }
    }

    private final class Replica {
        final int index;
        final List<Tid> addresses;
        final Map<Integer, Instance> instances = new TreeMap<>();
        final Map<Integer, Accept> reordered = new TreeMap<>();
        final int[] nextSequence = new int[2];
        // Leader-only bookkeeping for the finite replacement of the no-op timer.
        final TreeSet<Integer> readCredits = new TreeSet<>();
        final TreeSet<Integer> completedReads = new TreeSet<>();
        int highestAccepted = -1, highestDecided = -1, highestAcknowledged = -1;
        int lastAcceptSent = -1, x;

        Replica(int index, List<Tid> addresses) { this.index = index; this.addresses = addresses; }
        void send(int destination, MpMessage message) {
            Channel.send(communication, addresses.get(destination), message);
        }
        Instance instance(int number) { return instances.computeIfAbsent(number, Instance::new); }

        void run() {
            while (true) {
                MpMessage message;
                if (index == 0 && local && !readCredits.isEmpty()) {
                    // Service already available input before offering an idle no-op.
                    var available = Channel.tryRecv(communication);
                    if (available.isPresent()) message = available.get();
                    else if (Nondet.chooseBoolean()) {
                        readCredits.pollFirst();
                        sendNextAccept(new NoOp());
                        continue;
                    } else message = Channel.recv(communication);
                } else message = Channel.recv(communication);
                receive(message);
            }
        }

        void receive(MpMessage message) {
            if (message instanceof Write write) submit(write.client(), write);
            else if (message instanceof Read read) submit(read.client(), read);
            else if (message instanceof Rmw rmw) submit(rmw.client(), rmw);
            else if (message instanceof RedirectRequest redirect) {
                require(index == 0, "redirect destination");
                sendNextAccept(redirect.request());
            } else if (message instanceof Accept accept) {
                if (accept.instance() <= highestAccepted) return;
                reordered.put(accept.instance(), accept);
                Accept next;
                while ((next = reordered.remove(highestAccepted + 1)) != null) uponAccept(next);
            } else if (message instanceof AcceptAck ack) {
                require(index == 0, "ack destination");
                ackInstance(ack.instance());
            } else if (message instanceof ReadStarted started) {
                require(index == 0, "read registration destination");
                if (!completedReads.contains(started.token())) readCredits.add(started.token());
            } else if (message instanceof ReadReady ready) {
                require(index == 0, "read completion destination");
                readCredits.remove(ready.token());
                completedReads.add(ready.token());
                int origin = ready.token() % 2 == 0 ? 0 : 2;
                if (origin == 0) releaseRead(ready.token(), ready.value());
                else send(origin, new ReadRelease(ready.token(), ready.value()));
            } else if (message instanceof ReadRelease release) {
                releaseRead(release.token(), release.value());
            } else throw new AssertionError("unexpected message " + message);
        }

        void submit(int client, MpMessage command) {
            require(index == (client == 0 ? 0 : 2), "client destination");
            int sequence = nextSequence[client]++;
            if (local && command instanceof Read) {
                int token = 2 * sequence + client;
                instance(highestAccepted + 1).reads.add(token);
                if (index == 0) readCredits.add(token);
                else send(0, new ReadStarted(token));
            } else {
                Request request = new Request(client, sequence, command);
                if (index == 0) sendNextAccept(request);
                else send(0, new RedirectRequest(request));
            }
        }

        void sendNextAccept(MpMessage value) {
            require(index == 0, "only leader proposes");
            // Source invokes its own ACCEPT handler directly; no process exchange here.
            int number = ++lastAcceptSent;
            uponAccept(new Accept(number, 0, value, highestAcknowledged));
        }

        void uponAccept(Accept accept) {
            require(accept.instance() == highestAccepted + 1, "ACCEPT gap");
            Instance inst = instance(accept.instance());
            inst.value = accept.value();
            inst.counter = accept.counter() + 1;
            highestAccepted = inst.number;
            // Forward BEFORE deciding and BEFORE consuming the piggybacked ack.
            if (index == 2) send(0, new AcceptAck(inst.number));
            else send(index + 1, new Accept(inst.number, inst.counter, inst.value, highestAcknowledged));
            if (inst.counter >= 2) decideAndExecute(inst);
            ackInstance(accept.ack());
        }

        void decideAndExecute(Instance inst) {
            require(!inst.decided && inst.number == highestDecided + 1, "decision gap");
            inst.decided = true;
            highestDecided = inst.number;
            if (inst.value instanceof NoOp) return;
            Request request = (Request) inst.value;
            MpMessage reply;
            if (request.command() instanceof Write write) {
                x = write.value();
                reply = new Ack();
            } else if (request.command() instanceof Rmw) {
                x += 10;
                reply = new Result(x);
            } else reply = new Value(x);
            // Every replica executes; only the issuing front responds to its client.
            if (index == (request.client() == 0 ? 0 : 2)) send(3 + request.client(), reply);
        }

        void ackInstance(int number) {
            for (int i = highestDecided + 1; i <= number; i++) decideAndExecute(instance(i));
            for (int i = highestAcknowledged + 1; i <= number; i++) {
                Instance inst = instances.remove(i);
                require(inst != null && inst.decided, "retiring undecided instance");
                // Reads see the application at retirement, not at attachment/decision.
                for (int token : inst.reads) {
                    ReadReady ready = new ReadReady(token, x);
                    if (index == 0) receive(ready);
                    else send(0, ready);
                }
                highestAcknowledged = i;
            }
        }

        void releaseRead(int token, int value) { send(3 + token % 2, new Value(value)); }
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError(reason);
    }
}
