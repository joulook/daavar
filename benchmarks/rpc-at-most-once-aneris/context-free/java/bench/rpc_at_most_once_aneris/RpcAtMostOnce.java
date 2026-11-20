package bench.rpc_at_most_once_aneris;

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
 * rpc-at-most-once-aneris: the Aneris dscm one-server memory with request ids, retransmission and
 * duplicate suppression (ctable), against a sequentially consistent memory server. Java twin of
 * the Rust program; arguments {@code <spec|impl>,<K>,<fifo|bag>[,<dedup|no-dedup>]}.
 */
public final class RpcAtMostOnce implements MpProgram {

    private static final int N = 2; // clients
    private static final int DUPS = 1; // retransmissions in the whole run (all at p0)

    // ---- values ----

    /** Rust {@code enum Op}. */
    public sealed interface Op permits Wr, Rd {}

    /** {@code InjL (k, v)}. */
    @Tag("Wr")
    public record Wr(int key, int val) implements Op {}

    /** {@code InjR k}. */
    @Tag("Rd")
    public record Rd(int key) implements Op {}

    /** Rust {@code enum Res}. */
    public sealed interface Res permits Ack, Val {}

    /** {@code InjL ()}. */
    @Tag("Ack")
    public record Ack() implements Res {}

    /** {@code InjR (map_lookup k db)}; 0 is None. */
    @Tag("Val")
    public record Val(int v) implements Res {}

    /** Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage permits OpMsg, ResMsg, Req, Rep {}

    /** A client's operation; {@code from} is the client. */
    @Tag("Op")
    public record OpMsg(Tid from, Op op) implements Msg {}

    /** The answer to a client. */
    @Tag("Res")
    public record ResMsg(Res res) implements Msg {}

    /** The proxy's request. */
    @Tag("Req")
    public record Req(Op op, int rid, Tid from) implements Msg {}

    /** The server's reply. */
    @Tag("Rep")
    public record Rep(Res res, int rid) implements Msg {}

    // ---- configuration ----

    private final boolean isSpec;
    private final int k;
    private final CommunicationModel model;
    private final boolean dedup;

    public RpcAtMostOnce(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("args: <spec|impl>,<K>,<fifo|bag>[,<dedup|no-dedup>]");
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("spec or impl, not " + args[0]);
        }
        k = Integer.parseInt(args[1]);
        switch (args[2]) {
            case "fifo" -> model = CommunicationModel.P2P;
            case "bag" -> model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("fifo or bag, not " + args[2]);
        }
        String variant = args.length == 4 ? args[3] : "dedup";
        switch (variant) {
            case "dedup" -> dedup = true;
            case "no-dedup" -> dedup = false;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "rpc-at-most-once-aneris";
    }

    @Override
    public String size() {
        return Integer.toString(k);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return isSpec ? List.of("server", "c0", "c1") : List.of("server", "p0", "p1", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    // ---- program ----

    private static Op script(int i, int j) {
        return j % 2 == 0 ? new Wr(0, i + 1) : new Rd(0);
    }

    /** {@code commit_op} on a one-key-per-index map. */
    private static Res apply(int[] db, Op op) {
        if (op instanceof Wr w) {
            db[w.key()] = w.val();
            return new Ack();
        }
        Rd r = (Rd) op;
        return new Val(db[r.key()]);
    }

    private static void check(boolean cond, String what) {
        if (!cond) {
            throw new AssertionError(what);
        }
    }

    private List<MpProcess.Handle> spawnClients(List<Tid> front) {
        final CommunicationModel m = model;
        final int kk = k;
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int ci = i;
            final Tid to = front.get(i);
            cs.add(MpProcess.spawn("c" + i, () -> {
                Tid me = MpProcess.self();
                for (int j = 0; j < kk; j++) {
                    Channel.send(m, to, new OpMsg(me, script(ci, j)));
                    Channel.recv(m, ResMsg.class);
                }
            }));
        }
        return cs;
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    private void spec() {
        final CommunicationModel m = model;
        final int kk = k;
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            int[] db = new int[1];
            for (int n = 0; n < N * kk; n++) {
                OpMsg o = Channel.recv(m, OpMsg.class);
                Res r = apply(db, o.op());
                Channel.send(m, o.from(), new ResMsg(r));
            }
        });
        Tid sid = server.tid();
        List<MpProcess.Handle> cs = spawnClients(List.of(sid, sid));
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        server.await();
    }

    /** One ctable entry: caddr -> (rid, result). */
    private record Entry(Tid from, int rid, Res res) {}

    private void imp() {
        final CommunicationModel m = model;
        final int kk = k;
        final boolean dd = dedup;
        // the server: request_loop over process_request; ends blocked when the clients are done
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            int[] db = new int[1];
            List<Entry> ctable = new ArrayList<>();
            while (true) {
                Req q = Channel.recv(m, Req.class);
                Op op = q.op();
                int rid = q.rid();
                Tid from = q.from();
                if (dd) {
                    int e = -1;
                    for (int x = 0; x < ctable.size(); x++) {
                        if (ctable.get(x).from().equals(from)) {
                            e = x;
                            break;
                        }
                    }
                    if (e >= 0) {
                        Entry en = ctable.get(e);
                        int t = en.rid();
                        if (rid < t) {
                            continue; // ignore stale requests
                        }
                        if (rid == t) {
                            Channel.send(m, from, new Rep(en.res(), rid));
                            continue; // resend the cached reply
                        }
                        check(rid == t + 1, "rid == t + 1");
                        ctable.remove(e);
                    }
                    Res res = apply(db, op);
                    ctable.add(new Entry(from, rid, res));
                    Channel.send(m, from, new Rep(res, rid));
                } else {
                    Res res = apply(db, op);
                    Channel.send(m, from, new Rep(res, rid));
                }
            }
        });
        final Tid sid = server.tid();
        List<MpProcess.Handle> proxies = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int pi = i;
            proxies.add(MpProcess.spawn("p" + i, () -> {
                Tid me = MpProcess.self();
                int dups = pi == 0 ? DUPS : 0;
                int sentDups = 0;
                for (int rid = 0; rid < kk; rid++) {
                    OpMsg o = Channel.recv(m, OpMsg.class);
                    Tid client = o.from();
                    Op op = o.op();
                    Channel.send(m, sid, new Req(op, rid, me));
                    if (dups > 0 && Nondet.chooseBoolean()) {
                        // receive timeout: retransmit the same request
                        dups -= 1;
                        sentDups += 1;
                        Channel.send(m, sid, new Req(op, rid, me));
                    }
                    // accept only resId = rid; at most sentDups stale replies precede it
                    Res res = null;
                    for (int x = 0; x <= sentDups; x++) {
                        Rep p = Channel.recv(m, Rep.class);
                        check(p.rid() <= rid, "id <= rid");
                        if (p.rid() == rid) {
                            res = p.res();
                            break;
                        }
                    }
                    check(res != null, "res.unwrap()");
                    Channel.send(m, client, new ResMsg(res));
                }
            }));
        }
        List<Tid> front = new ArrayList<>();
        for (MpProcess.Handle p : proxies) {
            front.add(p.tid());
        }
        List<MpProcess.Handle> cs = spawnClients(front);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle p : proxies) {
            p.await();
        }
        server.await();
    }

    @Override
    public String toString() {
        return "rpc-at-most-once-aneris(" + (isSpec ? "spec" : "impl") + ", K=" + k + ", " + model
                + (isSpec ? "" : ", " + (dedup ? "dedup" : "no-dedup")) + ")";
    }
}
