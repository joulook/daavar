package bench.fencing_grove;

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
 * fencing-grove: gokv's fencing-token front ends over a counter server, written against the Java
 * Must API from the Rust (TraceForge) program alone.
 *
 * <p>Argument string: {@code <spec|impl>,<K>,<fifo|bag>[,<variant>]}; {@code K} =
 * {@code FetchAndIncrement(0)} calls per client; variants (impl side) {@code fenced} (default) and
 * {@code stale-frontend}. Visible roles: {@code c0}, {@code c1}.
 */
public final class FencingGrove implements MpProgram {

    /** The messages: one record per variant of the Rust {@code Msg} enum, same tags, same field order. */
    public sealed interface Msg extends MpMessage {
        /** client -> server / clerk: {@code FetchAndIncrement(key)}. */
        @Tag("Fai")
        record Fai(Tid from, int key) implements Msg {}

        /** -> client: the value fetched. */
        @Tag("Val")
        record Val(int value) implements Msg {}

        /** clerk -> config: {@code Get()}. */
        @Tag("CfgGet")
        record CfgGet(Tid from) implements Msg {}

        /** config -> clerk: the current front end. */
        @Tag("Current")
        record Current(Tid fe) implements Msg {}

        /** front end -> config: {@code AcquireEpoch(me)}. */
        @Tag("Acquire")
        record Acquire(Tid from) implements Msg {}

        /** config -> front end: the epoch. */
        @Tag("Epoch")
        record Epoch(int epoch) implements Msg {}

        /** clerk -> front end: {@code RPC_FAI(key)}. */
        @Tag("FeFai")
        record FeFai(Tid from, int key) implements Msg {}

        /** front end -> clerk: the value. */
        @Tag("FeVal")
        record FeVal(int value) implements Msg {}

        /** front end -> clerk: the call failed (the front end has exited). */
        @Tag("FeErr")
        record FeErr() implements Msg {}

        /** front end -> ctr: {@code Get(epoch)}. */
        @Tag("CtrGet")
        record CtrGet(Tid from, int epoch) implements Msg {}

        /** ctr -> front end: {@code GetReply { err, val }}. */
        @Tag("GetRep")
        record GetRep(boolean stale, int val) implements Msg {}

        /** front end -> ctr: {@code Put(v, epoch)}. */
        @Tag("CtrPut")
        record CtrPut(Tid from, int v, int epoch) implements Msg {}

        /** ctr -> front end: the error code. */
        @Tag("PutRep")
        record PutRep(boolean stale) implements Msg {}
    }

    private static final List<String> SPEC_ROLES = List.of("server", "c0", "c1");
    private static final List<String> IMPL_ROLES = List.of("config", "ctr", "fe1", "fe2", "p0", "p1", "c0", "c1");

    private final boolean isSpec;
    private final int k;
    private final CommunicationModel model;
    private final boolean fenced;

    /**
     * @param args {@code <spec|impl>,<K>,<fifo|bag>[,<variant>]}
     */
    public FencingGrove(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<K>,<fifo|bag>[,<variant>], got " + String.join(",", args));
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("spec or impl, not " + args[0]);
        }
        k = Integer.parseInt(args[1]);
        if (k < 0) {
            throw new IllegalArgumentException("K must be >= 0, was " + k);
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("fifo or bag, not " + args[2]);
        };
        String variant = args.length > 3 ? args[3] : "fenced";
        if (isSpec) {
            fenced = true; // the spec side ignores the variant, as the Rust parse does
        } else {
            fenced = switch (variant) {
                case "fenced" -> true;
                case "stale-frontend" -> false;
                default -> throw new IllegalArgumentException("unknown variant " + variant);
            };
        }
    }

    @Override
    public String programName() {
        return "fencing-grove";
    }

    @Override
    public String size() {
        return String.valueOf(k);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return isSpec ? SPEC_ROLES : IMPL_ROLES;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    // ---------------------------------------------------------------- shared

    /** Blocking receive of one message type (Rust {@code recv_tag}). */
    private <T extends Msg> T recv(Class<T> type) {
        return Channel.recv(model, type);
    }

    /** Blocking receive of either of two message types (Rust {@code recv_tags}). */
    private Msg recv2(Class<? extends Msg> a, Class<? extends Msg> b) {
        return (Msg) Channel.recv(model, m -> a.isInstance(m) || b.isInstance(m));
    }

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    /** {@code c0}, {@code c1}: K times {@code FetchAndIncrement(0)}. */
    private List<MpProcess.Handle> spawnClients(Tid to0, Tid to1) {
        Tid[] to = {to0, to1};
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Tid t = to[i];
            cs.add(MpProcess.spawn("c" + i, () -> client(t)));
        }
        return cs;
    }

    private void client(Tid t) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            send(t, new Msg.Fai(me, 0));
            recv(Msg.Val.class);
        }
    }

    // ---------------------------------------------------------------- spec

    private void spec() {
        MpProcess.Handle server = MpProcess.spawn("server", this::server);
        Tid s = server.tid();
        List<MpProcess.Handle> cs = spawnClients(s, s);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        server.await();
    }

    private void server() {
        int v = 0;
        for (int j = 0; j < 2 * k; j++) {
            Msg.Fai f = recv(Msg.Fai.class);
            send(f.from(), new Msg.Val(v));
            v += 1;
        }
    }

    // ---------------------------------------------------------------- impl

    private void imp() {
        MpProcess.Handle config = MpProcess.spawn("config", this::config);
        Tid cfg = config.tid();
        MpProcess.Handle ctr = MpProcess.spawn("ctr", this::ctr);
        Tid cid = ctr.tid();
        List<MpProcess.Handle> fes = new ArrayList<>();
        for (String name : List.of("fe1", "fe2")) {
            fes.add(MpProcess.spawn(name, () -> frontEnd(cfg, cid)));
        }
        List<MpProcess.Handle> clerks = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            clerks.add(MpProcess.spawn("p" + i, () -> clerk(cfg)));
        }
        List<MpProcess.Handle> cs = spawnClients(clerks.get(0).tid(), clerks.get(1).tid());
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle p : clerks) {
            p.await();
        }
        for (MpProcess.Handle f : fes) {
            f.await();
        }
        ctr.await();
        config.await();
    }

    /** {@code config}: a blocking server loop (A4 idiom); it ends blocked. */
    private void config() {
        int epoch = 0;
        boolean active = false;
        Optional<Tid> data = Optional.empty();
        List<Tid> acquirers = new ArrayList<>();
        List<Tid> getters = new ArrayList<>();
        while (true) {
            Tid grant = null;
            Msg m = recv2(Msg.Acquire.class, Msg.CfgGet.class);
            if (m instanceof Msg.Acquire a) {
                if (active) {
                    acquirers.add(a.from()); // AcquireEpoch waits while currHolderActive
                } else {
                    grant = a.from();
                }
            } else if (m instanceof Msg.CfgGet g) {
                if (data.isPresent()) {
                    send(g.from(), new Msg.Current(data.get()));
                } else {
                    getters.add(g.from()); // no front end yet
                }
            } else {
                throw new IllegalStateException("unreachable: " + m);
            }
            // HeartbeatListener: the holder's heartbeat may expire while another front end waits
            if (grant == null && active && !acquirers.isEmpty() && Nondet.chooseBoolean()) {
                active = false;
                grant = acquirers.remove(0);
            }
            if (grant != null) {
                active = true;
                data = Optional.of(grant);
                epoch += 1;
                send(grant, new Msg.Epoch(epoch));
                for (Tid g : getters) {
                    send(g, new Msg.Current(grant));
                }
                getters.clear();
            }
        }
    }

    /** {@code ctr}: the counter server; rejects {@code epoch < lastEpoch}. A blocking loop. */
    private void ctr() {
        int v = 0;
        int last = 0;
        while (true) {
            Msg m = recv2(Msg.CtrGet.class, Msg.CtrPut.class);
            if (m instanceof Msg.CtrGet g) {
                if (g.epoch() < last) {
                    send(g.from(), new Msg.GetRep(true, 0));
                } else {
                    last = g.epoch();
                    send(g.from(), new Msg.GetRep(false, v));
                }
            } else if (m instanceof Msg.CtrPut p) {
                if (p.epoch() < last) {
                    send(p.from(), new Msg.PutRep(true));
                } else {
                    last = p.epoch();
                    v = p.v();
                    send(p.from(), new Msg.PutRep(false));
                }
            } else {
                throw new IllegalStateException("unreachable: " + m);
            }
        }
    }

    /** {@code fe1}, {@code fe2}: acquire an epoch, then serve {@code FetchAndIncrement} forever. */
    private void frontEnd(Tid cfg, Tid cid) {
        Tid me = MpProcess.self();
        send(cfg, new Msg.Acquire(me));
        int e = recv(Msg.Epoch.class).epoch();
        boolean exited = false;
        while (true) {
            Msg.FeFai req = recv(Msg.FeFai.class);
            Tid from = req.from();
            if (exited) {
                send(from, new Msg.FeErr());
                continue;
            }
            send(cid, new Msg.CtrGet(me, e));
            Msg.GetRep gr = recv(Msg.GetRep.class);
            if (gr.stale() && fenced) {
                exited = true; // "ctr: get() stale epoch number"; primitive.Exit(1)
                send(from, new Msg.FeErr());
                continue;
            }
            int val = gr.val();
            send(cid, new Msg.CtrPut(me, val + 1, e));
            Msg.PutRep pr = recv(Msg.PutRep.class);
            if (pr.stale() && fenced) {
                exited = true;
                send(from, new Msg.FeErr());
                continue;
            }
            send(from, new Msg.FeVal(val));
        }
    }

    /** {@code p0}, {@code p1}: the clerk of one client. */
    private void clerk(Tid cfg) {
        Tid me = MpProcess.self();
        // MakeClerk: the current front end
        send(cfg, new Msg.CfgGet(me));
        Tid fe = recv(Msg.Current.class).fe();
        for (int j = 0; j < k; j++) {
            Msg.Fai req = recv(Msg.Fai.class);
            boolean done = false;
            for (int attempt = 0; attempt < 2; attempt++) {
                send(fe, new Msg.FeFai(me, req.key()));
                Msg r = recv2(Msg.FeVal.class, Msg.FeErr.class);
                if (r instanceof Msg.FeVal v) {
                    send(req.from(), new Msg.Val(v.value()));
                    done = true;
                    break;
                } else if (r instanceof Msg.FeErr) {
                    send(cfg, new Msg.CfgGet(me));
                    fe = recv(Msg.Current.class).fe();
                } else {
                    throw new IllegalStateException("unreachable: " + r);
                }
            }
            if (!done) {
                throw new AssertionError("the second front end failed too");
            }
        }
    }
}
