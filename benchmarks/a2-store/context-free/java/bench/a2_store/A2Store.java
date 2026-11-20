package bench.a2_store;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Java twin of the Rust program {@code a2-store} (TraceForge's A2 key-value store): one Spec and
 * the Impls {@code pb}, {@code pb-br}, {@code sh} with their mutants and controls.
 *
 * <p>Arguments: {@code <spec|impl>,<k>,<q>[,<s>],<fifo|bag>,<variant>}; {@code s} present exactly
 * for {@code sh} and {@code misroute}. The order of the variant, size and model tokens is not
 * significant (the variant is the one non-numeric token other than the model), so the Rust order
 * {@code <spec|impl>,<variant>,<k>,<q>[,<s>],<fifo|bag>} is accepted too.
 *
 * <p>Every Rust channel is {@code typed_chan}, i.e. {@code CommunicationModel::LocalOrder}
 * whatever {@code --model} says; here every send and receive uses {@link CommunicationModel#P2P}.
 * A Rust channel is mapped to its receiving thread plus a type guard: no thread receives the same
 * message type from two channels.
 */
public final class A2Store implements MpProgram {

    // ------------------------------------------------------------ messages ----

    /** Rust {@code enum Req}. */
    public sealed interface Req extends MpMessage permits Put, Get {}

    /** Rust {@code Req::Put { c, k, v }}. */
    @Tag("Put")
    public record Put(int c, int k, int v) implements Req {}

    /** Rust {@code Req::Get { c, k }}. */
    @Tag("Get")
    public record Get(int c, int k) implements Req {}

    /** Rust {@code enum Rep}. */
    public sealed interface Rep extends MpMessage permits Ack, Val, RepNone {}

    /** Rust {@code Rep::Ack}. */
    @Tag("Ack")
    public record Ack() implements Rep {}

    /** Rust {@code Rep::Val(i32)}. */
    @Tag("Val")
    public record Val(int v) implements Rep {}

    /** Rust {@code Rep::None}. */
    @Tag("None")
    public record RepNone() implements Rep {}

    /** Rust {@code enum Internal}. */
    public sealed interface Internal extends MpMessage permits Fwd, AckPut {}

    /** Rust {@code Internal::Fwd(Req)}. */
    @Tag("Fwd")
    public record Fwd(Req r) implements Internal {}

    /** Rust {@code Internal::AckPut}. */
    @Tag("AckPut")
    public record AckPut() implements Internal {}

    // ------------------------------------------------------------ variants ----

    private enum Pb { CORRECT, EARLY_ACK, STALE_GET, SILENT_PUT }

    private enum Br { CORRECT, EARLY_ACK, CONTROL_FORWARDED_EARLY_ACK }

    private static final CommunicationModel M = CommunicationModel.P2P;

    private final boolean spec;
    private final String variant;
    private final int k;
    private final int q;
    private final int s;
    private final String modelArg;
    private final Pb pb;
    private final Br br;
    private final boolean sharded;
    private final boolean misroute;

    /**
     * @param args {@code <spec|impl>,<k>,<q>[,<s>],<fifo|bag>,<variant>} (see the class comment)
     */
    public A2Store(String... args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("a2-store: no arguments");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("a2-store: first argument must be spec|impl, got " + args[0]);
        }
        String model = null;
        String var = null;
        List<Integer> nums = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String a = args[i].trim();
            if (a.equals("fifo") || a.equals("bag")) {
                if (model != null) {
                    throw new IllegalArgumentException("a2-store: model given twice");
                }
                model = a;
            } else if (a.matches("\\d+")) {
                nums.add(Integer.parseInt(a));
            } else {
                if (var != null) {
                    throw new IllegalArgumentException("a2-store: two variants: " + var + ", " + a);
                }
                var = a;
            }
        }
        if (model == null) {
            throw new IllegalArgumentException("a2-store: missing <fifo|bag>");
        }
        if (var == null) {
            throw new IllegalArgumentException("a2-store: missing <variant>");
        }
        Pb p = null;
        Br b = null;
        boolean sh = false;
        boolean mis = false;
        switch (var) {
            case "pb" -> p = Pb.CORRECT;
            case "pb-br" -> b = Br.CORRECT;
            case "early-ack" -> b = Br.EARLY_ACK;
            case "stale-get" -> p = Pb.STALE_GET;
            case "silent-put" -> p = Pb.SILENT_PUT;
            case "control-early-ack-primary" -> p = Pb.EARLY_ACK;
            case "control-early-ack-fwd" -> b = Br.CONTROL_FORWARDED_EARLY_ACK;
            case "sh" -> sh = true;
            case "misroute" -> {
                sh = true;
                mis = true;
            }
            default -> throw new IllegalArgumentException("a2-store: unknown variant " + var);
        }
        int want = sh ? 3 : 2;
        if (nums.size() != want) {
            throw new IllegalArgumentException(
                    "a2-store: expected " + want + " numbers (k q" + (sh ? " s" : "") + "), got " + nums);
        }
        this.variant = var;
        this.modelArg = model;
        this.pb = p;
        this.br = b;
        this.sharded = sh;
        this.misroute = mis;
        this.k = nums.get(0);
        this.q = nums.get(1);
        this.s = sh ? nums.get(2) : 0;
    }

    // ------------------------------------------------------------- helpers ----

    /** Client {@code c_i}'s op {@code j}: a put iff {@code i+j} is even, key {@code (j/2 + i/2) mod 2}, value {@code 10i+j}. */
    private static Req op(int i, int j) {
        int key = (j / 2 + i / 2) % 2;
        if ((i + j) % 2 == 0) {
            return new Put(i, key, 10 * i + j);
        }
        return new Get(i, key);
    }

    private static boolean isPut(Req r) {
        return r instanceof Put;
    }

    private static int key(Req r) {
        return r instanceof Put p ? p.k() : ((Get) r).k();
    }

    private static Rep lookup(Map<Integer, Integer> map, int key) {
        Integer v = map.get(key);
        return v == null ? new RepNone() : new Val(v);
    }

    private static Req recvReq() {
        return Channel.recv(M, Req.class);
    }

    private static Internal recvInternal() {
        return Channel.recv(M, Internal.class);
    }

    /**
     * The process ids of one execution. A client registers its own id before its first send; a
     * server reads a client's id only after receiving a request causally after that send. Server
     * ids are written by main before any client is spawned.
     */
    private static final class Ids {
        final AtomicReferenceArray<Tid> clients;
        final AtomicReference<Tid> store = new AtomicReference<>();
        final AtomicReference<Tid> primary = new AtomicReference<>();
        final AtomicReference<Tid> backup = new AtomicReference<>();
        final AtomicReference<Tid> router = new AtomicReference<>();
        final AtomicReferenceArray<Tid> shards;

        Ids(int k, int s) {
            clients = new AtomicReferenceArray<>(k);
            shards = new AtomicReferenceArray<>(Math.max(s, 0));
        }

        Tid client(int c) {
            Tid t = clients.get(c);
            if (t == null) {
                throw new IllegalStateException("client c" + c + " not registered");
            }
            return t;
        }
    }

    private interface Route {
        Tid to(Req r);
    }

    /** The closed-loop clients: {@code c_i} sends its script and blocks on its reply after each request. */
    private void spawnClients(Ids ids, Route route, List<MpProcess.Handle> handles) {
        for (int i = 0; i < k; i++) {
            final int ci = i;
            handles.add(MpProcess.spawn("c" + i, () -> {
                ids.clients.set(ci, MpProcess.self());
                for (int j = 0; j < q; j++) {
                    Req r = op(ci, j);
                    Channel.send(M, route.to(r), r);
                    Channel.recv(M, Rep.class);
                }
            }));
        }
    }

    // ------------------------------------------------------------ programs ----

    @Override
    public void run() {
        Ids ids = new Ids(k, s);
        List<MpProcess.Handle> handles = new ArrayList<>();
        if (spec) {
            runSpec(ids, handles);
        } else if (pb != null) {
            runPb(ids, handles);
        } else if (br != null) {
            runPbBr(ids, handles);
        } else {
            runSh(ids, handles);
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    /** The Spec: one invisible {@code store}, serving in arrival order. */
    private void runSpec(Ids ids, List<MpProcess.Handle> handles) {
        MpProcess.Handle store = MpProcess.spawn("store", () -> {
            Map<Integer, Integer> map = new HashMap<>();
            for (int n = 0; n < k * q; n++) {
                Req r = recvReq();
                if (r instanceof Put p) {
                    map.put(p.k(), p.v());
                    Channel.send(M, ids.client(p.c()), new Ack());
                } else {
                    Get g = (Get) r;
                    Channel.send(M, ids.client(g.c()), lookup(map, g.k()));
                }
            }
        });
        handles.add(store);
        ids.store.set(store.tid());
        spawnClients(ids, r -> ids.store.get(), handles);
    }

    /** {@code pb}: primary-backup, gets served by the primary. */
    private void runPb(Ids ids, List<MpProcess.Handle> handles) {
        final Pb kind = pb;
        int nPuts = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < q; j++) {
                if (isPut(op(i, j))) {
                    nPuts++;
                }
            }
        }
        final int puts = nPuts;
        MpProcess.Handle primary = MpProcess.spawn("primary", () -> {
            Map<Integer, Integer> map = new HashMap<>();
            boolean firstPut = true;
            for (int n = 0; n < k * q; n++) {
                Req r = recvReq();
                if (r instanceof Put p) {
                    if (kind != Pb.STALE_GET) {
                        map.put(p.k(), p.v());
                    }
                    Channel.send(M, ids.backup.get(), new Fwd(r));
                    if (kind == Pb.EARLY_ACK) {
                        Channel.send(M, ids.client(p.c()), new Ack());
                        recvInternal();
                    } else {
                        recvInternal();
                        boolean silent = kind == Pb.SILENT_PUT && firstPut;
                        if (!silent) {
                            Channel.send(M, ids.client(p.c()), new Ack());
                        }
                    }
                    firstPut = false;
                } else {
                    Get g = (Get) r;
                    Channel.send(M, ids.client(g.c()), lookup(map, g.k()));
                }
            }
        });
        handles.add(primary);
        ids.primary.set(primary.tid());
        MpProcess.Handle backup = MpProcess.spawn("backup", () -> {
            Map<Integer, Integer> map = new HashMap<>();
            for (int n = 0; n < puts; n++) {
                Internal m = recvInternal();
                if (m instanceof Fwd f && f.r() instanceof Put p) {
                    map.put(p.k(), p.v());
                }
                Channel.send(M, ids.primary.get(), new AckPut());
            }
        });
        handles.add(backup);
        ids.backup.set(backup.tid());
        spawnClients(ids, r -> ids.primary.get(), handles);
    }

    /** {@code pb-br}: gets go to the backup directly (except in the forwarded control). */
    private void runPbBr(Ids ids, List<MpProcess.Handle> handles) {
        final Br kind = br;
        MpProcess.Handle primary = MpProcess.spawn("primary", () -> {
            int total = 0;
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < q; j++) {
                    if (isPut(op(i, j)) || kind == Br.CONTROL_FORWARDED_EARLY_ACK) {
                        total++;
                    }
                }
            }
            for (int n = 0; n < total; n++) {
                Req r = recvReq();
                if (r instanceof Put p) {
                    Channel.send(M, ids.backup.get(), r);
                    if (kind == Br.CORRECT) {
                        recvInternal();
                        Channel.send(M, ids.client(p.c()), new Ack());
                    } else {
                        Channel.send(M, ids.client(p.c()), new Ack());
                        recvInternal();
                    }
                } else {
                    Channel.send(M, ids.backup.get(), r);
                }
            }
        });
        handles.add(primary);
        ids.primary.set(primary.tid());
        MpProcess.Handle backup = MpProcess.spawn("backup", () -> {
            Map<Integer, Integer> map = new HashMap<>();
            for (int n = 0; n < k * q; n++) {
                Req r = recvReq();
                if (r instanceof Put p) {
                    map.put(p.k(), p.v());
                    Channel.send(M, ids.primary.get(), new AckPut());
                } else {
                    Get g = (Get) r;
                    Channel.send(M, ids.client(g.c()), lookup(map, g.k()));
                }
            }
        });
        handles.add(backup);
        ids.backup.set(backup.tid());
        spawnClients(ids, r -> (isPut(r) || kind == Br.CONTROL_FORWARDED_EARLY_ACK)
                ? ids.primary.get() : ids.backup.get(), handles);
    }

    /** {@code sh}: a router forwards each request to shard {@code key mod s} ({@code misroute}: a get to {@code (key+1) mod s}). */
    private void runSh(Ids ids, List<MpProcess.Handle> handles) {
        final boolean mis = misroute;
        final int shards = s;
        java.util.function.ToIntFunction<Req> route = r -> {
            int key = key(r);
            return (mis && !isPut(r)) ? (key + 1) % shards : key % shards;
        };
        int[] perShard = new int[shards];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < q; j++) {
                perShard[route.applyAsInt(op(i, j))]++;
            }
        }
        MpProcess.Handle router = MpProcess.spawn("router", () -> {
            for (int n = 0; n < k * q; n++) {
                Req r = recvReq();
                Channel.send(M, ids.shards.get(route.applyAsInt(r)), r);
            }
        });
        handles.add(router);
        ids.router.set(router.tid());
        for (int x = 0; x < shards; x++) {
            final int count = perShard[x];
            MpProcess.Handle h = MpProcess.spawn("shard" + x, () -> {
                Map<Integer, Integer> map = new HashMap<>();
                for (int n = 0; n < count; n++) {
                    Req r = recvReq();
                    if (r instanceof Put p) {
                        map.put(p.k(), p.v());
                        Channel.send(M, ids.client(p.c()), new Ack());
                    } else {
                        Get g = (Get) r;
                        Channel.send(M, ids.client(g.c()), lookup(map, g.k()));
                    }
                }
            });
            handles.add(h);
            ids.shards.set(x, h.tid());
        }
        spawnClients(ids, r -> ids.router.get(), handles);
    }

    // ------------------------------------------------------------ MpProgram ----

    @Override
    public String programName() {
        return "a2-store";
    }

    @Override
    public String size() {
        return variant + " " + k + " " + q + (sharded ? " " + s : "");
    }

    @Override
    public CommunicationModel model() {
        // typed_chan: LocalOrder for every channel, whatever --model says.
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        if (spec) {
            roles.add("store");
        } else if (sharded) {
            roles.add("router");
            for (int x = 0; x < s; x++) {
                roles.add("shard" + x);
            }
        } else {
            roles.add("primary");
            roles.add("backup");
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

    @Override
    public String toString() {
        return "a2-store " + (spec ? "spec" : "impl") + " " + size() + " (--model " + modelArg + ")";
    }
}
