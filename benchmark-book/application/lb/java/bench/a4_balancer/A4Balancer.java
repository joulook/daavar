package bench.a4_balancer;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * TraceForge's A4, a load-balancing server with two specifications: the Java twin of
 * {@code rust/src/main.rs} (same threads, spawn order, messages, choices and variants).
 *
 * <p>Arguments: {@code spec,<k>,<q>,<m>,fifo,<a4a|a4b>} or
 * {@code impl,<k>,<q>,<m>,fifo,<hash|rr|pair-swap-hash|pair-swap-rr|drop|hash-drop>}. Visible:
 * {@code c0..c(k-1)}.
 *
 * <p>Rust's typed channels (one request channel read by the dispatcher or balancer, one per
 * server, one reply channel per client, all {@code LocalOrder}) become the reading thread's
 * mailbox: every thread reads exactly one of those channels, so a mailbox per thread under
 * {@code P2P} (per-sender FIFO) is the same relation. The channels ignore the shared main's
 * {@code --model}, so the model here is {@code fifo} only.
 */
public final class A4Balancer implements MpProgram {

    /** {@code A4Req(client, request)}. */
    @Tag("A4Req")
    public record A4Req(int i, int j) implements MpMessage {}

    /** {@code A4Reply(server, request)}. */
    @Tag("A4Reply")
    public record A4Reply(int x, int j) implements MpMessage {}

    enum Policy { ROUND_ROBIN, HASH }

    enum Fault { NONE, DROP, PAIR_SWAP }

    private static final CommunicationModel M = CommunicationModel.P2P;

    private final boolean impl;
    private final int k;
    private final int q;
    private final int m;
    private final String variant;
    private final boolean a4a;
    private final Policy policy;
    private final Fault fault;

    /**
     * @param args side, k, q, m, model ({@code fifo}), variant
     */
    public A4Balancer(String... args) {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "A4Balancer takes spec,<k>,<q>,<m>,fifo,<a4a|a4b> or impl,<k>,<q>,<m>,fifo,<impl variant>");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.q = Integer.parseInt(args[2].trim());
        this.m = Integer.parseInt(args[3].trim());
        if (Support.model(args[4]) != CommunicationModel.P2P) {
            throw new IllegalArgumentException("the Rust channels are LocalOrder whatever --model: fifo only");
        }
        this.variant = args[5].trim();
        boolean a = false;
        Policy p = Policy.HASH;
        Fault f = Fault.NONE;
        if (!impl) {
            a = switch (variant) {
                case "a4a" -> true;
                case "a4b" -> false;
                default -> throw new IllegalArgumentException("spec variant is a4a or a4b, was " + variant);
            };
        } else {
            switch (variant) {
                case "hash" -> { p = Policy.HASH; f = Fault.NONE; }
                case "rr" -> { p = Policy.ROUND_ROBIN; f = Fault.NONE; }
                case "pair-swap-hash" -> { p = Policy.HASH; f = Fault.PAIR_SWAP; }
                case "pair-swap-rr" -> { p = Policy.ROUND_ROBIN; f = Fault.PAIR_SWAP; }
                case "drop" -> { p = Policy.ROUND_ROBIN; f = Fault.DROP; }
                case "hash-drop" -> { p = Policy.HASH; f = Fault.DROP; }
                default -> throw new IllegalArgumentException("unknown impl variant " + variant
                        + "; one of hash, rr, pair-swap-hash, pair-swap-rr, drop, hash-drop");
            }
        }
        this.a4a = a;
        this.policy = p;
        this.fault = f;
        if (k < 1 || q < 1 || m < 1) {
            throw new IllegalArgumentException("k, q, m must be at least 1");
        }
    }

    @Override
    public String programName() {
        return "a4-balancer-" + (impl ? "impl-" : "spec-") + variant;
    }

    @Override
    public String size() {
        return "k" + k + "q" + q + "m" + m;
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        r.add(impl ? "balancer" : "dispatcher");
        for (int x = 0; x < m; x++) {
            r.add("s" + x);
        }
        r.addAll(visibleRoles());
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            r.add("c" + i);
        }
        return r;
    }

    /** Spawn order as in Rust: dispatcher or balancer, servers, clients; ids minted first. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] servers = new Tid[m];
        Tid[] clients = new Tid[k];
        Tid front = Tid.of(ts.add(impl ? "balancer" : "dispatcher",
                impl ? () -> balancer(servers) : () -> dispatcher(servers)).getTaskId());
        for (int x = 0; x < m; x++) {
            final int xx = x;
            servers[x] = Tid.of(ts.add("s" + x, () -> server(xx, clients)).getTaskId());
        }
        for (int i = 0; i < k; i++) {
            final int ii = i;
            clients[i] = Tid.of(ts.add("c" + i, () -> client(ii, front)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    /** {@code a4_spawn_servers}: loop until the run ends, replying {@code Reply(x, j)}. */
    private void server(int x, Tid[] clients) {
        while (true) {
            A4Req r = Channel.recv(M, A4Req.class);
            Channel.send(M, clients[r.i()], new A4Reply(x, r.j()));
        }
    }

    /** {@code a4_spawn_clients}. */
    private void client(int i, Tid front) {
        for (int j = 0; j < q; j++) {
            Channel.send(M, front, new A4Req(i, j));
            Channel.recv(M, A4Reply.class);
        }
    }

    /** {@code a4_spec}'s dispatcher. */
    private void dispatcher(Tid[] servers) {
        int[] affinity = new int[k];
        java.util.Arrays.fill(affinity, -1);
        for (int n = 0; n < k * q; n++) {
            A4Req r = Channel.recv(M, A4Req.class);
            int x;
            if (!a4a) {
                x = Nondet.chooseIndex(m);
            } else if (affinity[r.i()] >= 0) {
                x = affinity[r.i()];
            } else {
                x = Nondet.chooseIndex(m);
                affinity[r.i()] = x;
            }
            Channel.send(M, servers[x], r);
        }
    }

    private int p;

    private void forward(A4Req r, Tid[] servers) {
        int x = policy == Policy.ROUND_ROBIN ? p % m : r.i() % m;
        p++;
        Channel.send(M, servers[x], r);
    }

    /** {@code a4_impl}'s balancer. */
    private void balancer(Tid[] servers) {
        p = 0;
        if (fault == Fault.PAIR_SWAP) {
            for (int n = 0; n < q; n++) {
                A4Req a = Channel.recv(M, A4Req.class);
                A4Req b = Channel.recv(M, A4Req.class);
                forward(b, servers);
                forward(a, servers);
            }
        } else {
            for (int n = 0; n < k * q; n++) {
                A4Req r = Channel.recv(M, A4Req.class);
                boolean dropped = fault == Fault.DROP && r.i() == k - 1 && r.j() == q - 1;
                if (!dropped) {
                    forward(r, servers);
                }
            }
        }
    }
}
