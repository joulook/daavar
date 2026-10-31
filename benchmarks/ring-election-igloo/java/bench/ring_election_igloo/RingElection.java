package bench.ring_election_igloo;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Igloo's ring leader election with retransmission, loss and duplication (OOPSLA 2020; candidate
 * N23): the Java twin of {@code rust/src/main.rs} (same threads, spawn order, messages and choices;
 * Rust's per-thread inbox channels are thread mailboxes here, the ids minted before any thread
 * starts; the non-blocking {@code recv_msg} is {@code Channel.tryRecv}).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>,<variant>}; {@code n >= 2} nodes, R = 1
 * iteration, D = 1 duplicated send per node. Variants: {@code correct}, {@code no-compare}.
 * Visible: {@code observer}.
 */
public final class RingElection implements MpProgram {

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Id, Status {}

    /** node -> successor. */
    @Tag("Id")
    public record Id(int v) implements Msg {}

    /** node -> observer. */
    @Tag("Status")
    public record Status(int node, boolean leader) implements Msg {}

    private static final int R = 1;
    private static final int D = 1;

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean compare;

    /**
     * @param args side, n, model, variant
     */
    public RingElection(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("RingElection takes <spec|impl>,<n>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        if (n < 2) {
            throw new IllegalArgumentException("n must be at least 2");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.compare = switch (v) {
            case "correct" -> true;
            case "no-compare" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "ring-election-igloo";
    }

    @Override
    public String size() {
        return "n" + n + "r" + R + "d" + D;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        r.add("observer");
        if (!impl) {
            r.add("oracle");
        } else {
            for (int i = 0; i < n; i++) {
                r.add("node" + i);
            }
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("observer");
    }

    /** Mailboxes: n observer, 0..n-1 the nodes. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[n + 1];
        t[n] = Tid.of(ts.add("observer", this::observer).getTaskId());
        if (!impl) {
            ts.add("oracle", () -> oracle(t));
        } else {
            for (int i = 0; i < n; i++) {
                final int ii = i;
                t[i] = Tid.of(ts.add("node" + i, () -> node(ii, t)).getTaskId());
            }
        }
        ts.startAndAwaitAll();
    }

    private void observer() {
        for (int k = 0; k < n; k++) {
            Channel.recv(m, Msg.class);
        }
    }

    private void oracle(Tid[] t) {
        boolean elected = Nondet.chooseBoolean();
        for (int i = 0; i < n; i++) {
            Channel.send(m, t[n], new Status(i, elected && i == n - 1));
        }
    }

    /** try_send_int with the environment's duplication; returns the budget left. */
    private int udpSend(Tid to, int v, int budget) {
        Channel.send(m, to, new Id(v));
        if (budget > 0 && Nondet.chooseBoolean()) {
            Channel.send(m, to, new Id(v));
            return budget - 1;
        }
        return budget;
    }

    private void node(int i, Tid[] t) {
        Tid next = t[(i + 1) % n];
        int budget = D;
        int toSend = i;
        boolean leader = false;
        budget = udpSend(next, toSend, budget);
        for (int k = 0; k < R; k++) {
            Optional<Msg> q = Channel.tryRecv(m, Msg.class);
            if (q.isPresent() && q.get() instanceof Id id) {
                int msg = id.v();
                if (msg == i) {
                    leader = true;
                    break;
                } else if (!compare || msg > toSend) {
                    toSend = msg;
                }
            }
            budget = udpSend(next, toSend, budget);
        }
        Channel.send(m, t[n], new Status(i, leader));
    }
}
