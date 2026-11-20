package bench.broadcast_consensus_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Java twin of the Rust program broadcast-consensus-civl (CIVL's BroadcastConsensus): n processes,
 * each chooses a value in {0, 1}, broadcasts it, collects values and decides the max.
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>[,<correct|decide-early>]}.
 */
public final class BroadcastConsensus implements MpProgram {

    /** The messages: one record per Rust message type, tag = Rust type name. */
    public sealed interface Msg extends MpMessage permits Input, Val, Decide {}

    /** {@code c_i -> oracle} (spec) or {@code c_i -> node_i} (impl): the process's value. */
    @Tag("Input")
    public record Input(int v) implements Msg {}

    /** {@code node_j -> node_i}: the broadcast value. */
    @Tag("Val")
    public record Val(int v) implements Msg {}

    /** {@code oracle -> c_i} (spec) or {@code node_i -> c_i} (impl): the decision. */
    @Tag("Decide")
    public record Decide(int d) implements Msg {}

    private final boolean spec;
    private final int n;
    private final CommunicationModel model;
    private final boolean decideEarly;

    /**
     * @param args {@code <spec|impl>, <n>, <fifo|bag> [, <correct|decide-early>]}
     */
    public BroadcastConsensus(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "expected <spec|impl>,<n>,<fifo|bag>[,<correct|decide-early>], got " + String.join(",", args));
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("kind must be spec or impl, was " + args[0]);
        }
        n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("n must be at least 1, was " + n);
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
        String variant = args.length == 4 ? args[3] : "correct";
        decideEarly = switch (variant) {
            case "correct" -> false;
            case "decide-early" -> true;
            default -> throw new IllegalArgumentException(
                    "unknown variant " + variant + "; one of correct, decide-early");
        };
    }

    @Override
    public String programName() {
        return "broadcast-consensus-civl";
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
        if (spec) {
            roles.add("oracle");
        } else {
            for (int i = 0; i < n; i++) {
                roles.add("node" + i);
            }
        }
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        if (spec) {
            runSpec();
        } else {
            runImpl();
        }
    }

    /** {@code client}: choose v in {0, 1}, send Input(v), block for Decide. */
    private void client(Tid to) {
        int v = Nondet.chooseBoolean() ? 1 : 0;
        Channel.send(model, to, new Input(v));
        Channel.recv(model, Decide.class);
    }

    private void runSpec() {
        // Rust creates every channel before spawning; here every process (and so its Tid) is
        // created before any is started, then started in the Rust spawn order.
        Tid[] clients = new Tid[n];
        Tid[] oracleHolder = new Tid[1];
        List<MpThread> threads = new ArrayList<>();
        MpThread oracle = new MpThread(() -> {
            int d = Integer.MIN_VALUE;
            for (int k = 0; k < n; k++) {
                Input in = Channel.recv(model, Input.class);
                d = Math.max(d, in.v());
            }
            for (Tid c : clients) {
                Channel.send(model, c, new Decide(d));
            }
        }, "oracle");
        oracleHolder[0] = Tid.of(oracle.getTaskId(), "oracle");
        threads.add(oracle);
        for (int i = 0; i < n; i++) {
            MpThread c = new MpThread(() -> client(oracleHolder[0]), "c" + i);
            clients[i] = Tid.of(c.getTaskId(), "c" + i);
            threads.add(c);
        }
        startAndAwait(threads);
    }

    private void runImpl() {
        Tid[] nodes = new Tid[n];
        Tid[] clients = new Tid[n];
        int collect = decideEarly ? n - 1 : n;
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int me = i;
            MpThread node = new MpThread(() -> {
                Input in = Channel.recv(model, Input.class);
                for (Tid p : nodes) {
                    Channel.send(model, p, new Val(in.v()));
                }
                int d = Integer.MIN_VALUE;
                for (int k = 0; k < collect; k++) {
                    Val w = Channel.recv(model, Val.class);
                    d = Math.max(d, w.v());
                }
                Channel.send(model, clients[me], new Decide(d));
            }, "node" + i);
            nodes[i] = Tid.of(node.getTaskId(), "node" + i);
            threads.add(node);
        }
        for (int i = 0; i < n; i++) {
            final int me = i;
            MpThread c = new MpThread(() -> client(nodes[me]), "c" + i);
            clients[i] = Tid.of(c.getTaskId(), "c" + i);
            threads.add(c);
        }
        startAndAwait(threads);
    }

    /** Starts every process in creation (= Rust spawn) order, then awaits all of them. */
    private static void startAndAwait(List<MpThread> threads) {
        for (MpThread t : threads) {
            t.start();
        }
        for (MpThread t : threads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting " + t, e);
            }
        }
    }
}
