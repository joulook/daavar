package bench.memcached_oopsla25;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Java twin of the Rust benchmark memcached-oopsla25 (rust/src/main.rs), written context-free
 * from that file and the JMC Must API only.
 *
 * <p>Arguments: {@code <spec|impl>,<Q>,<fifo|bag>[,<variant>]}, Q = commands per client (1..=3),
 * variant one of {@code v3} (default), {@code shared-bucket}, {@code lost-set}. 2 clients, 2 keys.
 *
 * <p>Processes (all spawned by main, each named as in the Rust program): spec {@code k0 k1 c0 c1};
 * impl {@code b0 [b1] w0 w1 c0 c1}. Visible: {@code c0}, {@code c1}. Every process receives only from
 * its own mailbox (the Rust program's per-thread named channel).
 */
public final class Memcached implements MpProgram {

    static final int N = 2; // clients (connections)
    static final int KEYS = 2;

    // ---- Messages (Rust: enum Op, enum Res, enum Msg) ----

    /** Rust {@code enum Op}. */
    public sealed interface Op permits Op.Get, Op.Set {
        /** {@code Op::Get { key }}. */
        @Tag("Get")
        record Get(int key) implements Op {}

        /** {@code Op::Set { key, value }}. */
        @Tag("Set")
        record Set(int key, int value) implements Op {}
    }

    /** Rust {@code enum Res} (the response value; the Msg variant of the same name wraps it). */
    public sealed interface Response permits Response.Stored, Response.Got {
        /** {@code Res::Stored}. */
        @Tag("Stored")
        record Stored() implements Response {}

        /** {@code Res::Got { key, value: Option<u8> }}. */
        @Tag("Got")
        record Got(int key, Optional<Integer> value) implements Response {}
    }

    /** Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage permits Msg.Cmd, Msg.Res {
        /** {@code Msg::Cmd { c, op }}. */
        @Tag("Cmd")
        record Cmd(int c, Op op) implements Msg {}

        /** {@code Msg::Res(Res)}. */
        @Tag("Res")
        record Res(Response res) implements Msg {}
    }

    /** Client i's commands; the first Q are issued. */
    static final Op[][] SCRIPT = {
        {new Op.Set(0, 1), new Op.Get(1), new Op.Get(0)},
        {new Op.Set(1, 2), new Op.Get(1), new Op.Set(0, 2)},
    };

    static int keyOf(Op op) {
        return op instanceof Op.Get g ? g.key() : ((Op.Set) op).key();
    }

    // ---- Configuration ----

    private final boolean spec;
    private final int q;
    private final String modelName;
    private final CommunicationModel model;
    private final String variant;
    /** Role names in spawn order. */
    private final List<String> roles;
    /**
     * Raw task id of the first spawned process: main's id + 1, read in run() before any spawn
     * (MpProcess.self raises no event). Process k in spawn order is base + k, checked at spawn.
     */
    private volatile long base;

    /**
     * @param args {@code <spec|impl>, <Q>, <fifo|bag> [, <variant>]}
     */
    public Memcached(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<Q>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("first argument must be spec or impl: " + args[0]);
        }
        q = Integer.parseInt(args[1]);
        if (q < 1 || q > 3) {
            throw new IllegalArgumentException("Q must be 1..=3");
        }
        modelName = args[2];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        };
        String v = args.length > 3 ? args[3] : "v3";
        variant = switch (v) {
            case "v3", "shared-bucket", "lost-set" -> v;
            default -> throw new IllegalArgumentException(
                    "unknown variant " + v + "; one of v3, shared-bucket, lost-set");
        };
        List<String> r = new ArrayList<>();
        if (spec) {
            for (int k = 0; k < KEYS; k++) {
                r.add("k" + k);
            }
        } else {
            for (int b = 0; b < buckets(); b++) {
                r.add("b" + b);
            }
            for (int w = 0; w < N; w++) {
                r.add("w" + w);
            }
        }
        for (int i = 0; i < N; i++) {
            r.add("c" + i);
        }
        roles = List.copyOf(r);
    }

    /**
     * The process of the Rust channel {@code (role, i)}: named {@code role + i}. Its Tid is fixed
     * by spawn order (task base + k for the k-th spawned process), checked at spawn.
     */
    private Tid inbox(String role, int i) {
        String name = role + i;
        int idx = roles.indexOf(name);
        if (idx < 0) {
            throw new IllegalStateException("no process " + name);
        }
        return Tid.of(base + idx, name);
    }

    private Msg recv() {
        return (Msg) Channel.recv(model);
    }

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    /** cache_process for Set and Get; no expiry, so Get never removes. */
    private static Response process(Optional<Integer>[] store, Op op) {
        if (op instanceof Op.Set s) {
            store[s.key()] = Optional.of(s.value());
            return new Response.Stored();
        }
        Op.Get g = (Op.Get) op;
        return new Response.Got(g.key(), store[g.key()]);
    }

    /** A server: n commands, each read blocking from its own inbox, answered to (replyTo, c). */
    @SuppressWarnings("unchecked")
    private void serve(String replyTo, int n) {
        Optional<Integer>[] store = new Optional[KEYS];
        for (int k = 0; k < KEYS; k++) {
            store[k] = Optional.empty();
        }
        for (int j = 0; j < n; j++) {
            Msg m = recv();
            if (!(m instanceof Msg.Cmd cmd)) {
                throw new IllegalStateException("unreachable: server got " + m);
            }
            Response res = process(store, cmd.op());
            send(inbox(replyTo, cmd.c()), new Msg.Res(res));
        }
    }

    /** A client: send command, receive response, q times. */
    private void client(int i) {
        for (int j = 0; j < q; j++) {
            Op op = SCRIPT[i][j];
            Tid to = spec ? inbox("k", keyOf(op)) : inbox("w", i);
            send(to, new Msg.Cmd(i, op));
            Msg m = recv();
            if (!(m instanceof Msg.Res)) {
                throw new IllegalStateException("unreachable: client got " + m);
            }
        }
    }

    private int count(java.util.function.Predicate<Op> f) {
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

    private int route(Op op) {
        if (variant.equals("shared-bucket")) {
            return 0;
        }
        if (variant.equals("lost-set") && op instanceof Op.Set s) {
            return (s.key() + 1) % KEYS;
        }
        return keyOf(op);
    }

    private int buckets() {
        return variant.equals("shared-bucket") ? 1 : KEYS;
    }

    /** handle_client: read a command, forward to its bucket, read the response, answer the client. */
    private void handler(int w) {
        for (int j = 0; j < q; j++) {
            Msg m = recv();
            if (!(m instanceof Msg.Cmd cmd)) {
                throw new IllegalStateException("unreachable: handler got " + m);
            }
            send(inbox("b", route(cmd.op())), new Msg.Cmd(cmd.c(), cmd.op()));
            Msg r = recv();
            if (!(r instanceof Msg.Res res)) {
                throw new IllegalStateException("unreachable: handler got " + r);
            }
            send(inbox("c", w), new Msg.Res(res.res()));
        }
    }

    private MpProcess.Handle spawn(String name, Runnable body) {
        MpProcess.Handle h = MpProcess.spawn(name, body);
        Tid expected = Tid.of(base + roles.indexOf(name));
        if (!h.tid().equals(expected)) {
            throw new IllegalStateException("process " + name + " is " + h.tid() + ", expected " + expected);
        }
        return h;
    }

    @Override
    public void run() {
        base = MpProcess.self().raw() + 1;
        List<MpProcess.Handle> hs = new ArrayList<>();
        if (spec) {
            for (int k = 0; k < KEYS; k++) {
                final int kk = k;
                int n = count(op -> keyOf(op) == kk);
                hs.add(spawn("k" + k, () -> serve("c", n)));
            }
        } else {
            for (int b = 0; b < buckets(); b++) {
                final int bb = b;
                int n = count(op -> route(op) == bb);
                hs.add(spawn("b" + b, () -> serve("w", n)));
            }
            for (int w = 0; w < N; w++) {
                final int ww = w;
                hs.add(spawn("w" + w, () -> handler(ww)));
            }
        }
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int ii = i;
            cs.add(spawn("c" + i, () -> client(ii)));
        }
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    @Override
    public String programName() {
        return "memcached-oopsla25";
    }

    @Override
    public String size() {
        return Integer.toString(q);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public String toString() {
        return "memcached-oopsla25 " + (spec ? "spec" : "impl") + " Q=" + q + " model=" + modelName
                + " variant=" + variant;
    }
}
