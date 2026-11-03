package bench.ring_election_igloo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/**
 * Context-free Java twin of the Rust benchmark ring-election-igloo (Igloo's Chang-Roberts leader
 * election over UDP with explicit duplication). Arguments: {@code <spec|impl>,<n>,<fifo|bag>[,<variant>]},
 * n >= 2, variant {@code correct} (default) or {@code no-compare}.
 */
public final class RingElectionIgloo implements MpProgram {

    /** Iterations of mainLoop (receive, then resend) after the initial send. */
    private static final int R = 1;
    /** Duplicated sends per node. */
    private static final int D = 1;

    /** The Rust enum Msg. */
    public sealed interface Msg extends MpMessage permits Id, Status {}

    /** node -> successor: send_int(to_send). */
    @Tag("Id")
    public record Id(int value) implements Msg {}

    /** node -> observer: the node's final leader flag. */
    @Tag("Status")
    public record Status(int node, boolean leader) implements Msg {}

    private static final String PROGRAM = "ring-election-igloo";
    private static final String USAGE = "`<spec|impl>,<n>,<fifo|bag>[,correct|no-compare]`, n >= 2";

    private final boolean isSpec;
    private final int n;
    private final CommunicationModel model;
    private final boolean compare;

    public RingElectionIgloo(String... a) {
        if (a.length < 3 || a.length > 4) {
            throw new IllegalArgumentException(PROGRAM + ": ARGS are " + USAGE + ", got " + List.of(a));
        }
        switch (a[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException(PROGRAM + ": first argument must be spec or impl");
        }
        try {
            n = Integer.parseInt(a[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PROGRAM + ": <n> must be a number; ARGS are " + USAGE);
        }
        if (n < 2) {
            throw new IllegalArgumentException(PROGRAM + ": n must be at least 2");
        }
        model = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException(PROGRAM + ": model must be fifo or bag");
        };
        String variant = a.length == 4 ? a[3] : "correct";
        compare = switch (variant) {
            case "correct" -> true;
            case "no-compare" -> false;
            default -> throw new IllegalArgumentException(
                    PROGRAM + ": unknown variant `" + variant + "`; ARGS are " + USAGE);
        };
    }

    @Override
    public String programName() {
        return PROGRAM;
    }

    @Override
    public String size() {
        return Integer.toString(n);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("observer");
        if (isSpec) {
            roles.add("oracle");
        } else {
            for (int i = 0; i < n; i++) {
                roles.add("node" + i);
            }
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("observer");
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    // ---- threads ----

    private void observer() {
        for (int k = 0; k < n; k++) {
            Channel.recv(model);
        }
    }

    /** try_send_int, with the environment's duplication. budget is a one-element counter. */
    private void udpSend(Tid next, int v, int[] budget) {
        Channel.send(model, next, new Id(v));
        if (budget[0] > 0 && Nondet.chooseBoolean()) {
            budget[0] -= 1;
            Channel.send(model, next, new Id(v));
        }
    }

    private void node(int i, Tid next, Tid obs) {
        int myId = i;
        int[] budget = {D};
        int toSend = myId;
        boolean leader = false;
        udpSend(next, toSend, budget);
        for (int r = 0; r < R; r++) {
            Optional<MpMessage> m = Channel.tryRecv(model);
            if (m.isPresent() && m.get() instanceof Id id) {
                int msg = id.value();
                if (msg == myId) {
                    leader = true;
                    break;
                } else if (!compare || msg > toSend) {
                    toSend = msg;
                }
            }
            udpSend(next, toSend, budget);
        }
        Channel.send(model, obs, new Status(myId, leader));
    }

    // ---- main ----

    private static Tid tidOf(MpThread t) {
        return Tid.of(t.getTaskId(), t.role());
    }

    private static void await(MpThread t) {
        try {
            t.join1();
        } catch (InterruptedException e) {
            throw new IllegalStateException("interrupted while joining " + t, e);
        }
    }

    private void spec() {
        MpThread o = new MpThread(this::observer, "observer");
        Tid otx = tidOf(o);
        MpThread s = new MpThread(() -> {
            boolean elected = Nondet.chooseBoolean();
            for (int i = 0; i < n; i++) {
                Channel.send(model, otx, new Status(i, elected && i == n - 1));
            }
        }, "oracle");
        o.start();
        s.start();
        await(o);
        await(s);
    }

    private void imp() {
        // Every process is created (its Tid fixed) before any is started, as the Rust program builds
        // every channel before spawning: node i sends to node i+1, which is created after it.
        MpThread o = new MpThread(this::observer, "observer");
        Tid otx = tidOf(o);
        Tid[] tids = new Tid[n];
        MpThread[] nodes = new MpThread[n];
        for (int i = 0; i < n; i++) {
            final int me = i;
            nodes[i] = new MpThread(() -> node(me, tids[(me + 1) % n], otx), "node" + i);
            tids[i] = tidOf(nodes[i]);
        }
        List<MpThread> hs = new ArrayList<>();
        hs.add(o);
        for (MpThread t : nodes) {
            hs.add(t);
        }
        for (MpThread t : hs) {
            t.start();
        }
        for (MpThread t : hs) {
            await(t);
        }
    }

    @Override
    public String toString() {
        return PROGRAM + (isSpec ? " spec" : " impl") + " n=" + n + " model=" + model
                + (isSpec ? "" : (compare ? " correct" : " no-compare"));
    }
}
