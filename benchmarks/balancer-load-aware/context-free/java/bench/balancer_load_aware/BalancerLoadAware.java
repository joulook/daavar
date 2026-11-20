package bench.balancer_load_aware;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
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
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Java twin of the TraceForge benchmark balancer-load-aware (rust/src/main.rs): load-aware load
 * balancers L1, L2, L2-stale (impl) against a dispatcher that picks a server by nondet (spec).
 *
 * <p>Arguments (launcher string): {@code <spec|impl>,<k>,<q>,<m>[,<c>],<fifo|bag>,<variant>} with
 * variant {@code l1 | l2 | l2-stale}; {@code c} present exactly for {@code l2} and {@code l2-stale}.
 * The Rust order {@code <spec|impl>,<variant>,<k>,<q>,<m>[,<c>],<fifo|bag>} is accepted too.
 *
 * <p>Rust channels map to per-process mailboxes: a client's, a server's and a dispatcher's
 * inbox are their mailbox; a balancer's two channels (requests, loads) are its one mailbox read
 * with the type guards {@code Req} and {@code Load} (the two channels carry disjoint types).
 */
public final class BalancerLoadAware implements MpProgram {

    /** The messages; one record per Rust {@code Msg} variant, same tags and field order. */
    public sealed interface Msg extends MpMessage permits Req, Poll, Load, Reply, Reject, Stop {}

    /** client -> balancer / dispatcher -> server: request {@code j} of client {@code c}. */
    @Tag("Req")
    public record Req(int c, int j) implements Msg {}

    /** balancer {@code b} -> server: report your queue length; {@code again} on a re-poll. */
    @Tag("Poll")
    public record Poll(int b, boolean again) implements Msg {}

    /** server {@code s} -> balancer: its queue length {@code n}. */
    @Tag("Load")
    public record Load(int s, int n) implements Msg {}

    /** server {@code s} -> client: request {@code j} served. */
    @Tag("Reply")
    public record Reply(int s, int j) implements Msg {}

    /** server {@code s} -> client: request {@code j} arrived while the queue was full. */
    @Tag("Reject")
    public record Reject(int s, int j) implements Msg {}

    /** balancer / dispatcher -> server: no more messages from me. */
    @Tag("Stop")
    public record Stop() implements Msg {}

    private enum Variant { L1, L2, L2_STALE }

    private static final String USAGE =
            "<spec|impl>,<k>,<q>,<m>[,<c>],<fifo|bag>,<l1|l2|l2-stale>; c for l2, l2-stale";

    private final boolean isSpec;
    private final CommunicationModel model;
    private final Variant variant;
    private final int k;
    private final int q;
    private final int m;
    /** -1 for l1 (unbounded queues). */
    private final int cap;
    private final int b;
    private final String sizeText;

    /**
     * Constructs from the launcher's argument string, split at commas.
     *
     * @param args see the class comment
     */
    public BalancerLoadAware(String... args) {
        if (args.length < 5) {
            throw new IllegalArgumentException("balancer-load-aware: too few arguments; ARGS are " + USAGE);
        }
        isSpec = switch (args[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException("first argument must be spec or impl: " + args[0]);
        };
        List<String> rest = new ArrayList<>(List.of(args).subList(1, args.length));
        String v;
        String mod;
        if (parseVariant(rest.get(0)) != null) {
            // Rust order: variant, sizes, model
            v = rest.remove(0);
            mod = rest.remove(rest.size() - 1);
        } else {
            // brief order: sizes, model, variant
            v = rest.remove(rest.size() - 1);
            mod = rest.remove(rest.size() - 1);
        }
        variant = parseVariant(v);
        if (variant == null) {
            throw new IllegalArgumentException("balancer-load-aware: unknown variant " + v + "; ARGS are " + USAGE);
        }
        model = switch (mod) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + mod);
        };
        int n = variant == Variant.L1 ? 3 : 4;
        if (rest.size() != n) {
            throw new IllegalArgumentException("balancer-load-aware: expected " + n + " sizes for " + v
                    + ", got " + rest + "; ARGS are " + USAGE);
        }
        k = num(rest.get(0), "k");
        q = num(rest.get(1), "q");
        m = num(rest.get(2), "m");
        cap = variant == Variant.L1 ? -1 : num(rest.get(3), "c");
        b = variant == Variant.L2_STALE ? 2 : 1;
        sizeText = v + " " + String.join(" ", rest);
    }

    private static Variant parseVariant(String s) {
        return switch (s) {
            case "l1" -> Variant.L1;
            case "l2" -> Variant.L2;
            case "l2-stale" -> Variant.L2_STALE;
            default -> null;
        };
    }

    private static int num(String s, String what) {
        int v;
        try {
            v = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("balancer-load-aware: <" + what + "> not a number: " + s);
        }
        if (v < 1 || v > 100) {
            throw new IllegalArgumentException("balancer-load-aware: <" + what + "> must be in 1..=100");
        }
        return v;
    }

    /** Requests balancer / dispatcher {@code d} receives: {@code q} from every client {@code i} with i mod b = d. */
    private int requestsOf(int d) {
        int count = 0;
        for (int i = 0; i < k; i++) {
            if (i % b == d) {
                count++;
            }
        }
        return q * count;
    }

    // ------------------------------------------------------------ MpProgram ----

    @Override
    public String programName() {
        return "balancer-load-aware";
    }

    @Override
    public String size() {
        return sizeText;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int d = 0; d < b; d++) {
            roles.add((isSpec ? "dispatcher" : "balancer") + d);
        }
        for (int x = 0; x < m; x++) {
            roles.add("s" + x);
        }
        for (int i = 0; i < k; i++) {
            roles.add("c" + i);
        }
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

    // ------------------------------------------------------------ main ----

    /**
     * The process identities, filled by main right after each spawn. A process reads an entry only
     * after a receive that causally follows the spawn of the process it names (rust: the channels
     * exist before any thread; here a mailbox's address exists only after its owner's spawn).
     */
    private static final class Directory {
        final AtomicReferenceArray<Tid> fronts; // dispatchers or balancers
        final AtomicReferenceArray<Tid> servers;
        final AtomicReferenceArray<Tid> clients;

        Directory(int b, int m, int k) {
            fronts = new AtomicReferenceArray<>(b);
            servers = new AtomicReferenceArray<>(m);
            clients = new AtomicReferenceArray<>(k);
        }
    }

    @Override
    public void run() {
        Directory dir = new Directory(b, m, k);
        List<MpProcess.Handle> hs = new ArrayList<>();
        for (int d = 0; d < b; d++) {
            final int dd = d;
            final int n = requestsOf(d);
            MpProcess.Handle h = isSpec
                    ? MpProcess.spawn("dispatcher" + d, () -> dispatcher(n, dir))
                    : MpProcess.spawn("balancer" + d, () -> balancer(dd, n, dir));
            dir.fronts.set(d, h.tid());
            hs.add(h);
        }
        for (int x = 0; x < m; x++) {
            final int xx = x;
            MpProcess.Handle h = isSpec
                    ? MpProcess.spawn("s" + x, () -> specServer(xx, dir))
                    : MpProcess.spawn("s" + x, () -> server(xx, dir));
            dir.servers.set(x, h.tid());
            hs.add(h);
        }
        for (int i = 0; i < k; i++) {
            final int ii = i;
            final Tid to = dir.fronts.get(i % b);
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> client(ii, to));
            dir.clients.set(i, h.tid());
            hs.add(h);
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    private void client(int i, Tid to) {
        for (int j = 0; j < q; j++) {
            Channel.send(model, to, new Req(i, j));
            Channel.recv(model); // Reply or Reject, ignored
        }
    }

    // ------------------------------------------------------------ spec ----

    private void dispatcher(int n, Directory dir) {
        for (int r = 0; r < n; r++) {
            MpMessage req = Channel.recv(model);
            int x = m > 1 ? Nondet.chooseIndex(m) : 0;
            Channel.send(model, dir.servers.get(x), req);
        }
        for (int x = 0; x < m; x++) {
            Channel.send(model, dir.servers.get(x), new Stop());
        }
    }

    private void specServer(int x, Directory dir) {
        int stops = 0;
        while (stops < b) {
            MpMessage msg = Channel.recv(model);
            if (msg instanceof Req r) {
                Channel.send(model, dir.clients.get(r.c()), new Reply(x, r.j()));
            } else if (msg instanceof Stop) {
                stops++;
            } else {
                throw new IllegalStateException("spec s" + x + " got " + msg);
            }
        }
    }

    // ------------------------------------------------------------ impl ----

    private void balancer(int d, int n, Directory dir) {
        for (int r = 0; r < n; r++) {
            Req req = Channel.recv(model, Req.class); // the requests channel
            boolean again = false;
            int x;
            // At most two rounds: a re-polled full server serves once before it answers.
            while (true) {
                for (int s = 0; s < m; s++) {
                    Channel.send(model, dir.servers.get(s), new Poll(d, again));
                }
                int[] load = new int[m];
                for (int i = 0; i < m; i++) {
                    Load l = Channel.recv(model, Load.class); // the loads channel
                    load[l.s()] = l.n();
                }
                List<Integer> space = new ArrayList<>();
                for (int s = 0; s < m; s++) {
                    if (cap < 0 || load[s] < cap) {
                        space.add(s);
                    }
                }
                if (space.isEmpty()) {
                    again = true;
                    continue;
                }
                int min = Integer.MAX_VALUE;
                for (int s : space) {
                    min = Math.min(min, load[s]);
                }
                List<Integer> best = new ArrayList<>();
                for (int s : space) {
                    if (load[s] == min) {
                        best.add(s);
                    }
                }
                x = best.size() == 1 ? best.get(0) : best.get(Nondet.chooseIndex(best.size()));
                break;
            }
            Channel.send(model, dir.servers.get(x), req);
        }
        for (int s = 0; s < m; s++) {
            Channel.send(model, dir.servers.get(s), new Stop());
        }
    }

    private void server(int x, Directory dir) {
        ArrayDeque<Req> queue = new ArrayDeque<>();
        int stops = 0;
        while (true) {
            if (stops == b) {
                while (!queue.isEmpty()) {
                    serve(x, queue, dir);
                }
                return;
            }
            // Serve now or receive: with work queued the receive is non-blocking and its ⊥ is the serve.
            Optional<MpMessage> got = queue.isEmpty()
                    ? Optional.of(Channel.recv(model))
                    : Channel.tryRecv(model);
            if (got.isEmpty()) {
                serve(x, queue, dir);
                continue;
            }
            MpMessage msg = got.get();
            if (msg instanceof Poll p) {
                if (p.again() && full(queue)) {
                    serve(x, queue, dir);
                }
                Channel.send(model, dir.fronts.get(p.b()), new Load(x, queue.size()));
            } else if (msg instanceof Req r) {
                if (full(queue)) {
                    Channel.send(model, dir.clients.get(r.c()), new Reject(x, r.j()));
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

    private boolean full(ArrayDeque<Req> queue) {
        return cap >= 0 && queue.size() >= cap;
    }

    private void serve(int x, ArrayDeque<Req> queue, Directory dir) {
        Req r = queue.removeFirst();
        Channel.send(model, dir.clients.get(r.c()), new Reply(x, r.j()));
    }

    @Override
    public String toString() {
        return "balancer-load-aware " + (isSpec ? "spec" : "impl") + " " + sizeText + " " + model;
    }
}
