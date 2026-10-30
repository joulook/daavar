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

/**
 * gokv's fencing-token front ends (candidate N16), the Java twin of {@code rust/src/main.rs}.
 * Specification: one {@code server} with the counter of key 0. Implementation: {@code config},
 * {@code ctr}, front ends {@code fe1}, {@code fe2}, client clerks {@code p0}, {@code p1}.
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>,<variant>}; {@code K} calls of
 * {@code FetchAndIncrement(0)} per client {@code c0}, {@code c1}. Variants: {@code fenced} (the
 * source), {@code stale-frontend} (a front end ignores {@code EStale}).
 */
public final class Fencing implements MpProgram {

    @Tag("Fai")
    public record Fai(Tid from, int key) implements MpMessage {}

    @Tag("Val")
    public record Val(int v) implements MpMessage {}

    @Tag("CfgGet")
    public record CfgGet(Tid from) implements MpMessage {}

    @Tag("Current")
    public record Current(Tid fe) implements MpMessage {}

    @Tag("Acquire")
    public record Acquire(Tid from) implements MpMessage {}

    @Tag("Epoch")
    public record Epoch(int e) implements MpMessage {}

    @Tag("FeFai")
    public record FeFai(Tid from, int key) implements MpMessage {}

    @Tag("FeVal")
    public record FeVal(int v) implements MpMessage {}

    @Tag("FeErr")
    public record FeErr() implements MpMessage {}

    @Tag("CtrGet")
    public record CtrGet(Tid from, int epoch) implements MpMessage {}

    @Tag("GetRep")
    public record GetRep(boolean stale, int val) implements MpMessage {}

    @Tag("CtrPut")
    public record CtrPut(Tid from, int v, int epoch) implements MpMessage {}

    @Tag("PutRep")
    public record PutRep(boolean stale) implements MpMessage {}

    private final boolean impl;
    private final int k;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, K, model, variant
     */
    public Fencing(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Fencing takes <spec|impl>,<K>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        this.variant = args.length > 3 ? args[3].trim() : "fenced";
        if (impl && !List.of("fenced", "stale-frontend").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "fencing-grove-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "k" + k;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        if (!impl) {
            return List.of("server", "c0", "c1");
        }
        return List.of("config", "ctr", "fe1", "fe2", "p0", "p1", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public void run() {
        List<MpProcess.Handle> servers = new ArrayList<>();
        Tid[] to = new Tid[2];
        if (!impl) {
            MpProcess.Handle s = MpProcess.spawn("server", this::specServer);
            servers.add(s);
            to[0] = to[1] = s.tid();
        } else {
            boolean fenced = variant.equals("fenced");
            MpProcess.Handle config = MpProcess.spawn("config", this::config);
            Tid cfg = config.tid();
            MpProcess.Handle ctr = MpProcess.spawn("ctr", this::ctr);
            Tid cid = ctr.tid();
            List<MpProcess.Handle> fes = new ArrayList<>();
            for (String name : List.of("fe1", "fe2")) {
                fes.add(MpProcess.spawn(name, () -> frontEnd(cfg, cid, fenced)));
            }
            List<MpProcess.Handle> clerks = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                clerks.add(MpProcess.spawn("p" + i, () -> clerk(cfg)));
            }
            to[0] = clerks.get(0).tid();
            to[1] = clerks.get(1).tid();
            servers.addAll(clerks);
            servers.addAll(fes);
            servers.add(ctr);
            servers.add(config);
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final Tid t = to[i];
            clients.add(MpProcess.spawn("c" + i, () -> client(t)));
        }
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        for (MpProcess.Handle s : servers) {
            s.await();
        }
    }

    private void client(Tid t) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            Channel.send(m, t, new Fai(me, 0));
            Channel.recv(m, Val.class);
        }
    }

    private void specServer() {
        int v = 0;
        for (int c = 0; c < 2 * k; c++) {
            Fai f = Channel.recv(m, Fai.class);
            Channel.send(m, f.from(), new Val(v));
            v++;
        }
    }

    private void config() {
        int epoch = 0;
        boolean active = false;
        Tid data = null;
        List<Tid> acquirers = new ArrayList<>();
        List<Tid> getters = new ArrayList<>();
        // a blocking server loop (A4 idiom): it ends blocked
        while (true) {
            Tid grant = null;
            MpMessage q = Channel.recv(m, x -> x instanceof Acquire || x instanceof CfgGet);
            if (q instanceof Acquire a) {
                if (active) {
                    acquirers.add(a.from()); // AcquireEpoch waits while currHolderActive
                } else {
                    grant = a.from();
                }
            } else {
                CfgGet g = (CfgGet) q;
                if (data != null) {
                    Channel.send(m, g.from(), new Current(data));
                } else {
                    getters.add(g.from()); // no front end yet
                }
            }
            // HeartbeatListener: the holder's heartbeat may expire while another front end waits
            if (grant == null && active && !acquirers.isEmpty() && Nondet.chooseBoolean()) {
                active = false;
                grant = acquirers.remove(0);
            }
            if (grant != null) {
                active = true;
                data = grant;
                epoch++;
                Channel.send(m, grant, new Epoch(epoch));
                for (Tid g : getters) {
                    Channel.send(m, g, new Current(grant));
                }
                getters.clear();
            }
        }
    }

    private void ctr() {
        int v = 0;
        int last = 0;
        while (true) {
            MpMessage q = Channel.recv(m, x -> x instanceof CtrGet || x instanceof CtrPut);
            if (q instanceof CtrGet g) {
                if (g.epoch() < last) {
                    Channel.send(m, g.from(), new GetRep(true, 0));
                } else {
                    last = g.epoch();
                    Channel.send(m, g.from(), new GetRep(false, v));
                }
            } else {
                CtrPut p = (CtrPut) q;
                if (p.epoch() < last) {
                    Channel.send(m, p.from(), new PutRep(true));
                } else {
                    last = p.epoch();
                    v = p.v();
                    Channel.send(m, p.from(), new PutRep(false));
                }
            }
        }
    }

    private void frontEnd(Tid cfg, Tid cid, boolean fenced) {
        Tid me = MpProcess.self();
        Channel.send(m, cfg, new Acquire(me));
        int e = Channel.recv(m, Epoch.class).e();
        boolean exited = false;
        while (true) {
            FeFai f = Channel.recv(m, FeFai.class);
            if (exited) {
                Channel.send(m, f.from(), new FeErr());
                continue;
            }
            Channel.send(m, cid, new CtrGet(me, e));
            GetRep g = Channel.recv(m, GetRep.class);
            if (g.stale() && fenced) {
                exited = true; // "ctr: get() stale epoch number"; primitive.Exit(1)
                Channel.send(m, f.from(), new FeErr());
                continue;
            }
            Channel.send(m, cid, new CtrPut(me, g.val() + 1, e));
            PutRep p = Channel.recv(m, PutRep.class);
            if (p.stale() && fenced) {
                exited = true;
                Channel.send(m, f.from(), new FeErr());
                continue;
            }
            Channel.send(m, f.from(), new FeVal(g.val()));
        }
    }

    private void clerk(Tid cfg) {
        Tid me = MpProcess.self();
        // MakeClerk: the current front end
        Channel.send(m, cfg, new CfgGet(me));
        Tid fe = Channel.recv(m, Current.class).fe();
        for (int j = 0; j < k; j++) {
            Fai f = Channel.recv(m, Fai.class);
            boolean done = false;
            for (int attempt = 0; attempt < 2; attempt++) {
                Channel.send(m, fe, new FeFai(me, f.key()));
                MpMessage r = Channel.recv(m, x -> x instanceof FeVal || x instanceof FeErr);
                if (r instanceof FeVal v) {
                    Channel.send(m, f.from(), new Val(v.v()));
                    done = true;
                    break;
                }
                Channel.send(m, cfg, new CfgGet(me));
                fe = Channel.recv(m, Current.class).fe();
            }
            if (!done) {
                throw new AssertionError("the second front end failed too");
            }
        }
    }
}
