package bench.balancer_load_aware;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The load-aware load balancers L1, L2 and L2-stale against the A4 relay specification: the Java
 * twin of {@code rust/src/main.rs} (same threads, spawn order, messages and choices; Rust's
 * channels are thread mailboxes here, the balancer's two channels its mailbox read with a type
 * guard, the thread ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<variant>,<k>,<q>,<m>[,<c>]}; variant {@code l1}, {@code l2} or
 * {@code l2-stale} ({@code c} for the last two); {@code b} = 2 balancers / dispatchers for
 * {@code l2-stale}, 1 otherwise; client {@code c_i} talks to balancer {@code i mod b}. Model p2p.
 * Visible: {@code c0 .. c<k-1>}.
 */
public final class Balancer implements MpProgram {

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Req, Poll, Load, Reply, Reject, Stop {}

    /** Request {@code j} of client {@code c}. */
    @Tag("Req")
    public record Req(int c, int j) implements Msg {}

    /** Balancer {@code b} asks for the queue length; {@code again} on a re-poll. */
    @Tag("Poll")
    public record Poll(int b, boolean again) implements Msg {}

    /** Server {@code s}'s queue length {@code n}. */
    @Tag("Load")
    public record Load(int s, int n) implements Msg {}

    /** Server {@code s} served request {@code j}. */
    @Tag("Reply")
    public record Reply(int s, int j) implements Msg {}

    /** Server {@code s} rejected request {@code j}: its queue was full. */
    @Tag("Reject")
    public record Reject(int s, int j) implements Msg {}

    /** No more messages from this balancer / dispatcher. */
    @Tag("Stop")
    public record Stop() implements Msg {}

    private static final CommunicationModel M = CommunicationModel.P2P;

    private final boolean impl;
    private final String variant;
    private final int k;
    private final int q;
    private final int m;
    /** -1: unbounded (l1). */
    private final int cap;
    private final int b;

    /**
     * @param args side, variant, k, q, m and (l2, l2-stale) c
     */
    public Balancer(String... args) {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Balancer takes <spec|impl>,<l1|l2|l2-stale>,<k>,<q>,<m>[,<c>]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.variant = args[1].trim();
        int n = switch (variant) {
            case "l1" -> 5;
            case "l2", "l2-stale" -> 6;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
        if (args.length != n) {
            throw new IllegalArgumentException("variant " + variant + " takes " + n + " arguments");
        }
        this.k = Integer.parseInt(args[2].trim());
        this.q = Integer.parseInt(args[3].trim());
        this.m = Integer.parseInt(args[4].trim());
        this.cap = variant.equals("l1") ? -1 : Integer.parseInt(args[5].trim());
        this.b = variant.equals("l2-stale") ? 2 : 1;
    }

    @Override
    public String programName() {
        return "balancer-load-aware";
    }

    @Override
    public String size() {
        return variant + "-k" + k + "q" + q + "m" + m + (cap < 0 ? "" : "c" + cap);
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        for (int d = 0; d < b; d++) {
            r.add((impl ? "balancer" : "dispatcher") + d);
        }
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

    private int requestsOf(int d) {
        int n = 0;
        for (int i = 0; i < k; i++) {
            if (i % b == d) {
                n++;
            }
        }
        return n * q;
    }

    /** Thread ids: balancers / dispatchers 0..b, servers b..b+m, clients b+m..b+m+k. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[b + m + k];
        for (int d = 0; d < b; d++) {
            final int dd = d;
            Runnable body = impl ? () -> balancer(dd, t) : () -> dispatcher(dd, t);
            t[d] = Tid.of(ts.add((impl ? "balancer" : "dispatcher") + d, body).getTaskId());
        }
        for (int x = 0; x < m; x++) {
            final int xx = x;
            Runnable body = impl ? () -> server(xx, t) : () -> specServer(xx, t);
            t[b + x] = Tid.of(ts.add("s" + x, body).getTaskId());
        }
        for (int i = 0; i < k; i++) {
            final int ii = i;
            t[b + m + i] = Tid.of(ts.add("c" + i, () -> client(ii, t)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void client(int i, Tid[] t) {
        for (int j = 0; j < q; j++) {
            Channel.send(M, t[i % b], new Req(i, j));
            Channel.recv(M);
        }
    }

    // ------------------------------------------------------------ spec ----

    private void dispatcher(int d, Tid[] t) {
        int n = requestsOf(d);
        for (int r = 0; r < n; r++) {
            Req req = (Req) Channel.recv(M);
            int x = m > 1 ? Nondet.chooseIndex(m) : 0;
            Channel.send(M, t[b + x], req);
        }
        for (int x = 0; x < m; x++) {
            Channel.send(M, t[b + x], new Stop());
        }
    }

    private void specServer(int x, Tid[] t) {
        int stops = 0;
        while (stops < b) {
            MpMessage msg = Channel.recv(M);
            if (msg instanceof Req r) {
                Channel.send(M, t[b + m + r.c()], new Reply(x, r.j()));
            } else if (msg instanceof Stop) {
                stops++;
            } else {
                throw new IllegalStateException("spec s" + x + " got " + msg);
            }
        }
    }

    // ------------------------------------------------------------ impl ----

    private void balancer(int d, Tid[] t) {
        int n = requestsOf(d);
        for (int r = 0; r < n; r++) {
            Req req = Channel.recv(M, Req.class);
            boolean again = false;
            int target;
            // At most two rounds: a re-polled full server serves once before it answers.
            while (true) {
                for (int x = 0; x < m; x++) {
                    Channel.send(M, t[b + x], new Poll(d, again));
                }
                int[] load = new int[m];
                for (int y = 0; y < m; y++) {
                    Load l = Channel.recv(M, Load.class);
                    load[l.s()] = l.n();
                }
                List<Integer> space = new ArrayList<>();
                for (int x = 0; x < m; x++) {
                    if (cap < 0 || load[x] < cap) {
                        space.add(x);
                    }
                }
                if (space.isEmpty()) {
                    again = true;
                    continue;
                }
                int min = Integer.MAX_VALUE;
                for (int x : space) {
                    min = Math.min(min, load[x]);
                }
                List<Integer> best = new ArrayList<>();
                for (int x : space) {
                    if (load[x] == min) {
                        best.add(x);
                    }
                }
                target = best.size() == 1 ? best.get(0) : best.get(Nondet.chooseIndex(best.size()));
                break;
            }
            Channel.send(M, t[b + target], req);
        }
        for (int x = 0; x < m; x++) {
            Channel.send(M, t[b + x], new Stop());
        }
    }

    private boolean full(ArrayDeque<Req> queue) {
        return cap >= 0 && queue.size() >= cap;
    }

    private void serve(int x, ArrayDeque<Req> queue, Tid[] t) {
        Req r = queue.pollFirst();
        Channel.send(M, t[b + m + r.c()], new Reply(x, r.j()));
    }

    private void server(int x, Tid[] t) {
        ArrayDeque<Req> queue = new ArrayDeque<>();
        int stops = 0;
        while (true) {
            if (stops == b) {
                while (!queue.isEmpty()) {
                    serve(x, queue, t);
                }
                return;
            }
            // Serve now or receive: with work queued the receive is non-blocking, its ⊥ the serve.
            Optional<MpMessage> got = queue.isEmpty() ? Optional.of(Channel.recv(M)) : Channel.tryRecv(M);
            if (got.isEmpty()) {
                serve(x, queue, t);
                continue;
            }
            MpMessage msg = got.get();
            if (msg instanceof Poll p) {
                if (p.again() && full(queue)) {
                    serve(x, queue, t);
                }
                Channel.send(M, t[p.b()], new Load(x, queue.size()));
            } else if (msg instanceof Req r) {
                if (full(queue)) {
                    Channel.send(M, t[b + m + r.c()], new Reject(x, r.j()));
                } else {
                    queue.addLast(r);
                }
            } else if (msg instanceof Stop) {
                stops++;
            } else {
                throw new IllegalStateException("s" + x + " got " + msg);
            }
        }
    }
}
