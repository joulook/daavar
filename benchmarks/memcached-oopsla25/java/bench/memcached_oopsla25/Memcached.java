package bench.memcached_oopsla25;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * The Memcached case study of OOPSLA 2025 (§5.1), the Java twin of {@code rust/src/main.rs}.
 * Specification: the model's one cache served per key ({@code k0}, {@code k1}). Implementation:
 * v3, one connection handler per client ({@code w0}, {@code w1}) and one storage thread per bucket
 * ({@code b0}, {@code b1}; {@code b0} alone for {@code shared-bucket}).
 *
 * <p>Arguments: {@code <spec|impl>,<Q>[,<fifo|bag>[,<variant>]]}; {@code Q} commands per client
 * (1..3), clients {@code c0}, {@code c1}, keys 0 and 1. Variants: {@code v3},
 * {@code shared-bucket}, {@code lost-set} (ignored on the spec side). Every receive is untyped, as
 * in Rust (each mailbox holds one kind of message at a time).
 */
public final class Memcached implements MpProgram {

    /** {@code AbsCommand} without expiry (Rust {@code Op}). */
    public sealed interface Op permits Get, Set {}

    /** {@code AbsCommand::Get}. */
    @Tag("Get")
    public record Get(int key) implements Op {}

    /** {@code AbsCommand::Set}. */
    @Tag("Set")
    public record Set(int key, int value) implements Op {}

    /** A command of connection {@code c} (Rust {@code Msg::Cmd}). */
    @Tag("Cmd")
    public record Cmd(int c, Op op) implements MpMessage {}

    /** {@code AbsResponse} (Rust {@code Res}). */
    public sealed interface Reply permits Stored, Got {}

    /** {@code AbsResponse::Set}. */
    @Tag("Stored")
    public record Stored() implements Reply {}

    /** {@code AbsResponse::Get}; {@code value == null} is {@code None}. */
    @Tag("Got")
    public record Got(int key, Integer value) implements Reply {}

    /** A response (Rust {@code Msg::Res}). */
    @Tag("Res")
    public record Res(Reply res) implements MpMessage {}

    private static final int N = 2;
    private static final int KEYS = 2;
    private static final Op[][] SCRIPT = {
        {new Set(0, 1), new Get(1), new Get(0)},
        {new Set(1, 2), new Get(1), new Set(0, 2)},
    };

    private final boolean impl;
    private final int q;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, Q, model, variant
     */
    public Memcached(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Memcached takes <spec|impl>,<Q>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.q = Integer.parseInt(args[1].trim());
        if (q < 1 || q > 3) {
            throw new IllegalArgumentException("Q must be 1..3");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        this.variant = args.length > 3 ? args[3].trim() : "v3";
        if (!List.of("v3", "shared-bucket", "lost-set").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "memcached-oopsla25-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "n" + N + "q" + q;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    private int buckets() {
        return variant.equals("shared-bucket") ? 1 : KEYS;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (!impl) {
            r.add("k0");
            r.add("k1");
        } else {
            for (int b = 0; b < buckets(); b++) {
                r.add("b" + b);
            }
            r.add("w0");
            r.add("w1");
        }
        r.add("c0");
        r.add("c1");
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    private static int keyOf(Op op) {
        return op instanceof Get g ? g.key() : ((Set) op).key();
    }

    /** The bucket of a command (Rust {@code route}). */
    private int route(Op op) {
        if (variant.equals("shared-bucket")) {
            return 0;
        }
        if (variant.equals("lost-set") && op instanceof Set s) {
            return (s.key() + 1) % KEYS;
        }
        return keyOf(op);
    }

    private interface OpPred {
        boolean test(Op op);
    }

    private int count(OpPred f) {
        int n = 0;
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < q; j++) {
                if (f.test(SCRIPT[i][j])) {
                    n++;
                }
            }
        }
        return n;
    }

    @Override
    public void run() {
        Support.Procs procs = new Support.Procs();
        Tid[] servers = new Tid[impl ? buckets() : KEYS];
        Tid[] workers = new Tid[N];
        Tid[] clients = new Tid[N];
        if (!impl) {
            for (int k = 0; k < KEYS; k++) {
                final int key = k;
                int n = count(op -> keyOf(op) == key);
                servers[k] = procs.add("k" + k, () -> serve(n, clients));
            }
        } else {
            for (int b = 0; b < buckets(); b++) {
                final int bucket = b;
                int n = count(op -> route(op) == bucket);
                servers[b] = procs.add("b" + b, () -> serve(n, workers));
            }
            for (int w = 0; w < N; w++) {
                final int me = w;
                workers[w] = procs.add("w" + w, () -> worker(me, servers, clients));
            }
        }
        for (int i = 0; i < N; i++) {
            final int me = i;
            clients[i] = procs.add("c" + i, () -> client(me, servers, workers));
        }
        procs.startAndAwait();
    }

    /** {@code cache_process} for Set and Get, {@code n} commands, answered to {@code replyTo[c]}. */
    private void serve(int n, Tid[] replyTo) {
        Integer[] store = new Integer[KEYS];
        for (int i = 0; i < n; i++) {
            Cmd cmd = (Cmd) Channel.recv(m);
            Reply res;
            if (cmd.op() instanceof Set s) {
                store[s.key()] = s.value();
                res = new Stored();
            } else {
                Get g = (Get) cmd.op();
                res = new Got(g.key(), store[g.key()]);
            }
            Channel.send(m, replyTo[cmd.c()], new Res(res));
        }
    }

    /** {@code handle_client}: read, {@code Storage::process} on the command's bucket, write. */
    private void worker(int w, Tid[] buckets, Tid[] clients) {
        for (int j = 0; j < q; j++) {
            Cmd cmd = (Cmd) Channel.recv(m);
            Channel.send(m, buckets[route(cmd.op())], new Cmd(cmd.c(), cmd.op()));
            Res res = (Res) Channel.recv(m);
            Channel.send(m, clients[w], new Res(res.res()));
        }
    }

    /** {@code SendCommand} then {@code ReceiveResponse}, {@code q} times. */
    private void client(int i, Tid[] servers, Tid[] workers) {
        for (int j = 0; j < q; j++) {
            Op op = SCRIPT[i][j];
            Tid to = impl ? workers[i] : servers[keyOf(op)];
            Channel.send(m, to, new Cmd(i, op));
            Res res = (Res) Channel.recv(m);
        }
    }
}
