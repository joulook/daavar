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

/**
 * Grove's tutorial exactly-once KV service (candidate N6), the Java twin of
 * {@code rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,<keys>,<fifo|bag>,<variant>}; two clients: {@code c0} runs
 * {@code ConditionalPut(k, "", "1")} and {@code c1} runs {@code Get(k)} for every key in order.
 * Variants: {@code exactly-once}, {@code no-request-id}. One drop in all, at clerk {@code p0}.
 * Values: 0 is "" and 1 is "1". Every receive is unguarded, as in Rust (one message type per
 * mailbox: the sealed {@link Msg}). Every message is a {@link Tag}ged record mirroring the Rust
 * variant (same tag, same field order); Rust's {@code op_id: Option<u8>} is {@code opId}, -1 for
 * {@code None}.
 */
public final class ExactlyOnceKv implements MpProgram {

    /** Rust {@code KvOp}. */
    public sealed interface KvOp permits CondPut, Get {}

    /** {@code conditionalPut{key, expectedVal, newVal}}. */
    @Tag("CondPut")
    public record CondPut(int key, int expected, int nw) implements KvOp {}

    /** {@code get{key}}. */
    @Tag("Get")
    public record Get(int key) implements KvOp {}

    /** Rust {@code KvRes}. */
    public sealed interface KvRes permits Ok, Val {}

    /** {@code ConditionalPut}'s result: {@code reply == "ok"}. */
    @Tag("Ok")
    public record Ok(boolean ok) implements KvRes {}

    /** {@code Get}'s result. */
    @Tag("Val")
    public record Val(int v) implements KvRes {}

    /** Every message of the program (the Rust {@code Msg} enum). */
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
    public record FreshId(int n) implements Msg {}

    /** An operation RPC; {@code opId} -1 is {@code None} (the no-request-id clerk). */
    @Tag("Rpc")
    public record Rpc(Tid from, int opId, KvOp op) implements Msg {}

    /** Its reply. */
    @Tag("Reply")
    public record Reply(KvRes res) implements Msg {}

    private static final int N = 2;
    private static final int DROPS = 1;

    private final boolean impl;
    private final int keys;
    private final CommunicationModel m;
    private final boolean withIds;

    /**
     * @param args side, keys, model, variant
     */
    public ExactlyOnceKv(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("ExactlyOnceKv takes <spec|impl>,<keys>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.keys = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "exactly-once";
        this.withIds = switch (v) {
            case "exactly-once" -> true;
            case "no-request-id" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "kv-exactly-once-grove-" + (impl ? (withIds ? "exactly-once" : "no-request-id") : "spec");
    }

    @Override
    public String size() {
        return "n" + N + "keys" + keys + "d" + DROPS;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return impl ? List.of("server", "p0", "p1", "c0", "c1") : List.of("server", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    private Msg recv() {
        return (Msg) Channel.recv(m);
    }

    private static KvOp script(int i, int key) {
        return i == 0 ? new CondPut(key, 0, 1) : new Get(key);
    }

    /** The server's operation semantics ({@code kvs[key]}, default ""). */
    private static KvRes execute(int[] kvs, KvOp op) {
        if (op instanceof CondPut c) {
            if (kvs[c.key()] == c.expected()) {
                kvs[c.key()] = c.nw();
                return new Ok(true);
            }
            return new Ok(false);
        }
        return new Val(kvs[((Get) op).key()]);
    }

    @Override
    public void run() {
        List<MpProcess.Handle> infra = new ArrayList<>();
        Tid[] front = new Tid[N];
        if (!impl) {
            MpProcess.Handle s = MpProcess.spawn("server", () -> {
                int[] kvs = new int[keys];
                for (int n = 0; n < N * keys; n++) {
                    Op o = (Op) recv();
                    Channel.send(m, o.from(), new Res(execute(kvs, o.op())));
                }
            });
            infra.add(s);
            for (int i = 0; i < N; i++) {
                front[i] = s.tid();
            }
        } else {
            MpProcess.Handle s = MpProcess.spawn("server", this::server);
            infra.add(s);
            Tid sid = s.tid();
            for (int i = 0; i < N; i++) {
                final int me = i;
                MpProcess.Handle p = MpProcess.spawn("p" + i, () -> clerk(me, sid));
                infra.add(p);
                front[i] = p.tid();
            }
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int me = i;
            final Tid to = front[i];
            clients.add(MpProcess.spawn("c" + i, () -> client(me, to)));
        }
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        // as in Rust: the clerks, then the server
        for (int i = 1; i < infra.size(); i++) {
            infra.get(i).await();
        }
        infra.get(0).await();
    }

    private void client(int i, Tid to) {
        Tid me = MpProcess.self();
        for (int key = 0; key < keys; key++) {
            Channel.send(m, to, new Op(me, script(i, key)));
            recv();
        }
    }

    /** The Server: getFreshNum, conditionalPut, get; ends blocked when the clerks are done. */
    private void server() {
        int[] kvs = new int[keys];
        int nextFreshId = 0;
        List<Integer> ids = new ArrayList<>(); // lastReplies
        List<KvRes> replies = new ArrayList<>();
        while (true) {
            Msg q = recv();
            if (q instanceof Fresh fr) {
                Channel.send(m, fr.from(), new FreshId(nextFreshId++));
            } else if (q instanceof Rpc r) {
                int at = r.opId() >= 0 ? ids.indexOf(r.opId()) : -1;
                KvRes res;
                if (at >= 0) {
                    res = replies.get(at);
                } else {
                    res = execute(kvs, r.op());
                    if (r.opId() >= 0) {
                        ids.add(r.opId());
                        replies.add(res);
                    }
                }
                Channel.send(m, r.from(), new Reply(res));
            } else {
                throw new IllegalStateException("unreachable");
            }
        }
    }

    /** The Clerk: fresh id, then the RPC, retried once after a lost reply. */
    private void clerk(int i, Tid sid) {
        Tid me = MpProcess.self();
        int drops = i == 0 ? DROPS : 0;
        for (int key = 0; key < keys; key++) {
            Op o = (Op) recv();
            int opId = -1;
            if (withIds) {
                Channel.send(m, sid, new Fresh(me));
                opId = ((FreshId) recv()).n();
            }
            Rpc rpc = new Rpc(me, opId, o.op());
            Channel.send(m, sid, rpc);
            Reply r = (Reply) recv();
            if (drops > 0 && Nondet.chooseBoolean()) {
                // the reply is lost (urpc error): retry the same request
                drops--;
                Channel.send(m, sid, rpc);
                r = (Reply) recv();
            }
            Channel.send(m, o.from(), new Res(r.res()));
        }
    }
}
