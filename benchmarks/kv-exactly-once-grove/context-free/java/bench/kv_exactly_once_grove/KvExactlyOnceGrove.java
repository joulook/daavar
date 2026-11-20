package bench.kv_exactly_once_grove;

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
 * Grove's tutorial exactly-once KV service (gokv/tutorial/kvservice), written from the Rust
 * (TraceForge) program of the same benchmark. Arguments: {@code <spec|impl>,<keys>,<fifo|bag>[,<variant>]},
 * variants (impl side) {@code exactly-once} (default) and {@code no-request-id}.
 * Two clients: c0 runs ConditionalPut(k, 0, 1) and c1 runs Get(k) for every key k in order.
 * One drop in all: clerk p0 may lose one reply (nondet) and retry. Values: 0 is "" and 1 is "1".
 */
public final class KvExactlyOnceGrove implements MpProgram {

    static final int N = 2; // clients
    static final int DROPS = 1; // lost replies in the whole run (all at p0)

    /** Rust {@code KvOp}. */
    public sealed interface KvOp permits CondPut, Get {}

    /** {@code conditionalPut{key, expectedVal, newVal}}. */
    @Tag("CondPut")
    public record CondPut(int key, int expected, int newVal) implements KvOp {}

    /** {@code get{key}}. */
    @Tag("Get")
    public record Get(int key) implements KvOp {}

    /** Rust {@code KvRes}. */
    public sealed interface KvRes permits Ok, Val {}

    /** ConditionalPut's result: {@code reply == "ok"}. */
    @Tag("Ok")
    public record Ok(boolean ok) implements KvRes {}

    /** Get's result. */
    @Tag("Val")
    public record Val(int val) implements KvRes {}

    /** Rust {@code Msg}. */
    public sealed interface Msg extends MpMessage permits Op, Res, Fresh, FreshId, Rpc, Reply {}

    /** A client's operation; {@code from} is the client. */
    @Tag("Op")
    public record Op(Tid from, KvOp op) implements Msg {}

    /** The answer to a client. */
    @Tag("Res")
    public record Res(KvRes res) implements Msg {}

    /** {@code getFreshNumRpc}. */
    @Tag("Fresh")
    public record Fresh(Tid from) implements Msg {}

    /** Its reply. */
    @Tag("FreshId")
    public record FreshId(int id) implements Msg {}

    /** put/conditionalPut/get RPC with an op id (empty: the no-request-id clerk). */
    @Tag("Rpc")
    public record Rpc(Tid from, Optional<Integer> opId, KvOp op) implements Msg {}

    /** Its reply. */
    @Tag("Reply")
    public record Reply(KvRes res) implements Msg {}

    private final boolean spec;
    private final int keys;
    private final CommunicationModel model;
    private final String modelName;
    private final boolean withIds;

    /**
     * @param args {@code <spec|impl>, <keys>, <fifo|bag> [, <variant>]}
     */
    public KvExactlyOnceGrove(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<keys>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("unknown kind " + args[0]);
        }
        keys = Integer.parseInt(args[1]);
        modelName = args[2];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown model " + args[2]);
        };
        String variant = args.length > 3 ? args[3] : "exactly-once";
        withIds = switch (variant) {
            case "exactly-once" -> true;
            case "no-request-id" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
    }

    static KvOp script(int i, int key) {
        return i == 0 ? new CondPut(key, 0, 1) : new Get(key);
    }

    /** The server's operation semantics ({@code kvs[key]}, default ""). */
    static KvRes execute(int[] kvs, KvOp op) {
        if (op instanceof CondPut c) {
            if (kvs[c.key()] == c.expected()) {
                kvs[c.key()] = c.newVal();
                return new Ok(true);
            }
            return new Ok(false);
        }
        Get g = (Get) op;
        return new Val(kvs[g.key()]);
    }

    private Msg recv() {
        return Channel.recv(model, Msg.class);
    }

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    private List<MpProcess.Handle> spawnClients(Tid[] front) {
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int ci = i;
            final Tid to = front[i];
            cs.add(MpProcess.spawn("c" + i, () -> {
                Tid me = MpProcess.self();
                for (int key = 0; key < keys; key++) {
                    send(to, new Op(me, script(ci, key)));
                    recv();
                }
            }));
        }
        return cs;
    }

    private void runSpec() {
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            int[] kvs = new int[keys];
            for (int n = 0; n < N * keys; n++) {
                Op op = (Op) recv();
                KvRes r = execute(kvs, op.op());
                send(op.from(), new Res(r));
            }
        });
        Tid sid = server.tid();
        List<MpProcess.Handle> cs = spawnClients(new Tid[] {sid, sid});
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        server.await();
    }

    private record Entry(int id, KvRes res) {}

    private void runImpl() {
        // the Server: getFreshNum, conditionalPut, get; ends blocked when the clerks are done
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            int[] kvs = new int[keys];
            int nextFreshId = 0;
            List<Entry> lastReplies = new ArrayList<>();
            while (true) {
                Msg m = recv();
                if (m instanceof Fresh f) {
                    int n = nextFreshId;
                    nextFreshId++;
                    send(f.from(), new FreshId(n));
                } else if (m instanceof Rpc rpc) {
                    KvRes cached = null;
                    if (rpc.opId().isPresent()) {
                        int id = rpc.opId().get();
                        for (Entry e : lastReplies) {
                            if (e.id() == id) {
                                cached = e.res();
                                break;
                            }
                        }
                    }
                    KvRes r;
                    if (cached != null) {
                        r = cached;
                    } else {
                        r = execute(kvs, rpc.op());
                        if (rpc.opId().isPresent()) {
                            lastReplies.add(new Entry(rpc.opId().get(), r));
                        }
                    }
                    send(rpc.from(), new Reply(r));
                } else {
                    throw new IllegalStateException("unreachable: server got " + m);
                }
            }
        });
        Tid sid = server.tid();
        // the Clerks: one per client, invisible
        List<MpProcess.Handle> clerks = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int pi = i;
            clerks.add(MpProcess.spawn("p" + i, () -> {
                Tid me = MpProcess.self();
                int drops = pi == 0 ? DROPS : 0;
                for (int k = 0; k < keys; k++) {
                    Op o = (Op) recv();
                    Tid client = o.from();
                    KvOp op = o.op();
                    Optional<Integer> opId;
                    if (withIds) {
                        send(sid, new Fresh(me));
                        FreshId fid = (FreshId) recv();
                        opId = Optional.of(fid.id());
                    } else {
                        opId = Optional.empty();
                    }
                    send(sid, new Rpc(me, opId, op));
                    KvRes r = ((Reply) recv()).res();
                    if (drops > 0 && Nondet.chooseBoolean()) {
                        // the reply is lost (urpc error): retry the same request
                        drops--;
                        send(sid, new Rpc(me, opId, op));
                        r = ((Reply) recv()).res();
                    }
                    send(client, new Res(r));
                }
            }));
        }
        Tid[] front = new Tid[N];
        for (int i = 0; i < N; i++) {
            front[i] = clerks.get(i).tid();
        }
        List<MpProcess.Handle> cs = spawnClients(front);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle p : clerks) {
            p.await();
        }
        server.await();
    }

    @Override
    public void run() {
        if (spec) {
            runSpec();
        } else {
            runImpl();
        }
    }

    @Override
    public String programName() {
        return "kv-exactly-once-grove";
    }

    @Override
    public String size() {
        return Integer.toString(keys);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return spec ? List.of("server", "c0", "c1") : List.of("server", "p0", "p1", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public String toString() {
        return "KvExactlyOnceGrove(" + (spec ? "spec" : "impl") + ", keys=" + keys + ", " + modelName
                + (spec ? "" : ", " + (withIds ? "exactly-once" : "no-request-id")) + ")";
    }
}
