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
 * The Aneris dscm one-server memory (candidate N5), the Java twin of {@code rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>,<variant>}; {@code K} operations per client,
 * two clients {@code c0}, {@code c1}; client {@code i}'s operation {@code j} is
 * {@code Wr(x, i+1)} for even {@code j}, {@code Rd(x)} for odd {@code j}. Variants:
 * {@code dedup}, {@code no-dedup}. One duplicate in all, at proxy {@code p0}. Rust's tagged
 * receives are type-test receives here. Every message is a {@link Tag}ged record mirroring the
 * Rust variant (same tag, same field order); {@code Val(0)} is {@code None}.
 */
public final class AtMostOnceRpc implements MpProgram {

    /** Rust {@code Op}. */
    public sealed interface Op permits Wr, Rd {}

    /** {@code InjL (k, v)}. */
    @Tag("Wr")
    public record Wr(int key, int val) implements Op {}

    /** {@code InjR k}. */
    @Tag("Rd")
    public record Rd(int key) implements Op {}

    /** Rust {@code Res}. */
    public sealed interface Res permits Ack, Val {}

    /** {@code InjL ()}. */
    @Tag("Ack")
    public record Ack() implements Res {}

    /** {@code InjR (map_lookup k db)}; 0 is {@code None}. */
    @Tag("Val")
    public record Val(int v) implements Res {}

    /** {@code Msg::Op}: a client's operation; {@code from} is the client. */
    @Tag("Op")
    public record OpMsg(Tid from, Op op) implements MpMessage {}

    /** {@code Msg::Res}: the answer to a client. */
    @Tag("Res")
    public record ResMsg(Res res) implements MpMessage {}

    /** {@code Msg::Req}: {@code ((op, rid), caddr)}. */
    @Tag("Req")
    public record Req(Op op, int rid, Tid from) implements MpMessage {}

    /** {@code Msg::Rep}: {@code ((res, rid), caddr)}. */
    @Tag("Rep")
    public record Rep(Res res, int rid) implements MpMessage {}

    private static final int N = 2;
    private static final int DUPS = 1;

    private final boolean impl;
    private final int k;
    private final CommunicationModel m;
    private final boolean dedup;

    /**
     * @param args side, K, model, variant
     */
    public AtMostOnceRpc(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("AtMostOnceRpc takes <spec|impl>,<K>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "dedup";
        this.dedup = switch (v) {
            case "dedup" -> true;
            case "no-dedup" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "rpc-at-most-once-aneris-" + (impl ? (dedup ? "dedup" : "no-dedup") : "spec");
    }

    @Override
    public String size() {
        return "n" + N + "k" + k + "d" + DUPS;
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

    private static Op script(int i, int j) {
        return j % 2 == 0 ? new Wr(0, i + 1) : new Rd(0);
    }

    /** {@code commit_op} on the one-key map. */
    private static Res apply(int[] db, Op op) {
        if (op instanceof Wr w) {
            db[w.key()] = w.val();
            return new Ack();
        }
        return new Val(db[((Rd) op).key()]);
    }

    @Override
    public void run() {
        List<MpProcess.Handle> infra = new ArrayList<>();
        Tid[] front = new Tid[N];
        if (!impl) {
            MpProcess.Handle s = MpProcess.spawn("server", () -> {
                int[] db = {0};
                for (int n = 0; n < N * k; n++) {
                    OpMsg o = Channel.recv(m, OpMsg.class);
                    Channel.send(m, o.from(), new ResMsg(apply(db, o.op())));
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
                MpProcess.Handle p = MpProcess.spawn("p" + i, () -> proxy(me, sid));
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
        // as in Rust: the proxies, then the server
        for (int i = 1; i < infra.size(); i++) {
            infra.get(i).await();
        }
        infra.get(0).await();
    }

    private void client(int i, Tid to) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            Channel.send(m, to, new OpMsg(me, script(i, j)));
            Channel.recv(m, ResMsg.class);
        }
    }

    /** {@code request_loop} over {@code process_request}; ends blocked when the clients are done. */
    private void server() {
        int[] db = {0};
        List<Tid> cAddr = new ArrayList<>(); // ctable: caddr -> (rid, result)
        List<Integer> cRid = new ArrayList<>();
        List<Res> cRes = new ArrayList<>();
        while (true) {
            Req q = Channel.recv(m, Req.class);
            if (dedup) {
                int e = cAddr.indexOf(q.from());
                if (e >= 0) {
                    int t = cRid.get(e);
                    if (q.rid() < t) {
                        continue; // ignore stale requests
                    }
                    if (q.rid() == t) {
                        Channel.send(m, q.from(), new Rep(cRes.get(e), q.rid()));
                        continue; // resend the cached reply
                    }
                    if (q.rid() != t + 1) {
                        throw new AssertionError("rid = t + 1");
                    }
                    cAddr.remove(e);
                    cRid.remove(e);
                    cRes.remove(e);
                }
                Res r = apply(db, q.op());
                cAddr.add(q.from());
                cRid.add(q.rid());
                cRes.add(r);
                Channel.send(m, q.from(), new Rep(r, q.rid()));
            } else {
                Channel.send(m, q.from(), new Rep(apply(db, q.op()), q.rid()));
            }
        }
    }

    /** {@code install_proxy} / {@code request} / {@code wait_for_reply}. */
    private void proxy(int i, Tid sid) {
        Tid me = MpProcess.self();
        int dups = i == 0 ? DUPS : 0;
        int sentDups = 0;
        for (int rid = 0; rid < k; rid++) {
            OpMsg o = Channel.recv(m, OpMsg.class);
            Req q = new Req(o.op(), rid, me);
            Channel.send(m, sid, q);
            if (dups > 0 && Nondet.chooseBoolean()) {
                // receive timeout: retransmit the same request
                dups--;
                sentDups++;
                Channel.send(m, sid, q);
            }
            // accept only resId = rid; at most sentDups stale replies precede it
            Res res = null;
            for (int a = 0; a <= sentDups; a++) {
                Rep r = Channel.recv(m, Rep.class);
                if (r.rid() > rid) {
                    throw new AssertionError("resId <= rid");
                }
                if (r.rid() == rid) {
                    res = r.res();
                    break;
                }
            }
            Channel.send(m, o.from(), new ResMsg(res));
        }
    }
}
