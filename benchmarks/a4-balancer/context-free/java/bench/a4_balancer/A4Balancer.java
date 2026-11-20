package bench.a4_balancer;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
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
 * Java twin of TraceForge's A4 load-balancing server (a4-balancer/rust/src/main.rs).
 *
 * <p>Arguments (launcher): {@code <spec|impl>,<impl>,<spec>,<k>,<q>,<m>,<fifo|bag>}, i.e. the Rust
 * command line {@code {spec|impl} --model {fifo|bag} -- <impl> <spec> <k> <q> <m>}, with the side
 * first, the Rust positional arguments in their order, and the model last.
 * impl: hash, rr, pair-swap-hash, pair-swap-rr, drop, hash-drop; spec: a4a, a4b.
 *
 * <p>Visible roles: c0 .. c{k-1}.
 */
public final class A4Balancer implements MpProgram {

    /** The messages: Rust's {@code A4Req(usize, usize)} and {@code A4Reply(usize, usize)}. */
    public sealed interface A4Msg extends MpMessage permits A4Req, A4Reply {}

    /** {@code A4Req(i, j)}: client i's j-th request. */
    @Tag("A4Req")
    public record A4Req(int i, int j) implements A4Msg {}

    /** {@code A4Reply(x, j)}: server x's reply to request j. */
    @Tag("A4Reply")
    public record A4Reply(int x, int j) implements A4Msg {}

    private enum Policy { ROUND_ROBIN, HASH }

    private enum Fault { NONE, DROP, PAIR_SWAP }

    private enum Spec { A4A, A4B }

    private static final String PROGRAM = "a4-balancer";
    private static final String USAGE = "<spec|impl>,<impl>,<spec>,<k>,<q>,<m>,<fifo|bag>; impl one of hash, rr,"
            + " pair-swap-hash, pair-swap-rr, drop, hash-drop; spec one of a4a, a4b";

    private final boolean specSide;
    private final String implName;
    private final String specName;
    private final Policy policy;
    private final Fault fault;
    private final Spec spec;
    private final int k;
    private final int q;
    private final int m;
    private final CommunicationModel model;

    /**
     * @param args {@code <spec|impl>,<impl>,<spec>,<k>,<q>,<m>,<fifo|bag>}
     */
    public A4Balancer(String... args) {
        if (args.length != 7) {
            throw new IllegalArgumentException(PROGRAM + ": expected 7 arguments, got " + args.length
                    + " (" + String.join(",", args) + "); ARGS are " + USAGE);
        }
        switch (args[0]) {
            case "spec" -> specSide = true;
            case "impl" -> specSide = false;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown side `" + args[0] + "`; " + USAGE);
        }
        implName = args[1];
        switch (args[1]) {
            case "hash" -> { policy = Policy.HASH; fault = Fault.NONE; }
            case "rr" -> { policy = Policy.ROUND_ROBIN; fault = Fault.NONE; }
            case "pair-swap-hash" -> { policy = Policy.HASH; fault = Fault.PAIR_SWAP; }
            case "pair-swap-rr" -> { policy = Policy.ROUND_ROBIN; fault = Fault.PAIR_SWAP; }
            case "drop" -> { policy = Policy.ROUND_ROBIN; fault = Fault.DROP; }
            case "hash-drop" -> { policy = Policy.HASH; fault = Fault.DROP; }
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown impl `" + args[1] + "`; " + USAGE);
        }
        specName = args[2];
        switch (args[2]) {
            case "a4a" -> spec = Spec.A4A;
            case "a4b" -> spec = Spec.A4B;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown spec `" + args[2] + "`; " + USAGE);
        }
        k = num(args[3], "k");
        q = num(args[4], "q");
        m = num(args[5], "m");
        switch (args[6]) {
            case "fifo" -> model = CommunicationModel.P2P;
            case "bag" -> model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown model `" + args[6] + "`; " + USAGE);
        }
    }

    private static int num(String s, String what) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PROGRAM + ": <" + what + "> must be a number, got `" + s + "`; " + USAGE);
        }
    }

    @Override
    public String programName() {
        return PROGRAM;
    }

    @Override
    public String size() {
        return implName + " " + specName + " " + k + " " + q + " " + m;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add(specSide ? "dispatcher" : "balancer");
        for (int x = 0; x < m; x++) {
            roles.add("s" + x);
        }
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    /**
     * Main: spawns the dispatcher (spec) or balancer (impl), then the servers s0..s{m-1}, then the
     * clients c0..c{k-1} (the Rust spawn order), then awaits every process.
     */
    @Override
    public void run() {
        // Process identities (Rust's channel endpoints). The servers' are written by main right
        // after each spawn, before any client exists; each client writes its own as its first
        // statement, before its first send, so it is set before any server can reply to it.
        final Tid[] servers = new Tid[m];
        final Tid[] clients = new Tid[k];
        List<MpProcess.Handle> handles = new ArrayList<>();

        MpProcess.Handle front = specSide
                ? MpProcess.spawn("dispatcher", () -> dispatcher(servers))
                : MpProcess.spawn("balancer", () -> balancer(servers));
        handles.add(front);
        final Tid frontTid = front.tid();

        for (int x = 0; x < m; x++) {
            final int sx = x;
            MpProcess.Handle h = MpProcess.spawn("s" + x, () -> server(sx, clients));
            servers[x] = h.tid();
            handles.add(h);
        }
        for (int i = 0; i < k; i++) {
            final int ci = i;
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> {
                clients[ci] = MpProcess.self();
                client(ci, frontTid);
            });
            handles.add(h);
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    /** {@code a4_spawn_servers}: loop { Req(i, j) <- own channel; reply_tx[i] ! Reply(x, j) }. */
    private void server(int x, Tid[] clients) {
        while (true) {
            A4Req r = Channel.recv(model, A4Req.class);
            Channel.send(model, clients[r.i()], new A4Reply(x, r.j()));
        }
    }

    /** {@code a4_spawn_clients}: for j in 0..q { tx_req ! Req(i, j); _ <- own reply channel }. */
    private void client(int i, Tid front) {
        for (int j = 0; j < q; j++) {
            Channel.send(model, front, new A4Req(i, j));
            Channel.recv(model, A4Reply.class);
        }
    }

    /** {@code a4_spec}'s dispatcher. */
    private void dispatcher(Tid[] servers) {
        List<Optional<Integer>> affinity = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            affinity.add(Optional.empty());
        }
        for (int n = 0; n < k * q; n++) {
            A4Req r = Channel.recv(model, A4Req.class);
            int x;
            if (spec == Spec.A4B) {
                x = Nondet.chooseIndex(m);
            } else {
                Optional<Integer> a = affinity.get(r.i());
                if (a.isPresent()) {
                    x = a.get();
                } else {
                    x = Nondet.chooseIndex(m);
                    affinity.set(r.i(), Optional.of(x));
                }
            }
            Channel.send(model, servers[x], r);
        }
    }

    /** {@code a4_impl}'s balancer. */
    private void balancer(Tid[] servers) {
        int[] p = {0};
        java.util.function.Consumer<A4Req> forward = r -> {
            int x = switch (policy) {
                case ROUND_ROBIN -> p[0] % m;
                case HASH -> r.i() % m;
            };
            p[0]++;
            Channel.send(model, servers[x], r);
        };
        if (fault == Fault.PAIR_SWAP) {
            for (int n = 0; n < q; n++) {
                A4Req a = Channel.recv(model, A4Req.class);
                A4Req b = Channel.recv(model, A4Req.class);
                forward.accept(b);
                forward.accept(a);
            }
        } else {
            for (int n = 0; n < k * q; n++) {
                A4Req r = Channel.recv(model, A4Req.class);
                boolean dropped = fault == Fault.DROP && r.i() == k - 1 && r.j() == q - 1;
                if (!dropped) {
                    forward.accept(r);
                }
            }
        }
    }

    @Override
    public String toString() {
        return PROGRAM + "(" + (specSide ? "spec" : "impl") + ", " + size() + ", " + model + ")";
    }
}
