package bench.kv_snapshot_isolation_tla;

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
 * The TLA+ examples' key-value store (candidate N9), the Java twin of {@code rust/src/main.rs}.
 * Specification: a serializable {@code server} executing each transaction atomically in receive
 * order, or rolling it back. Implementation: KVsnap's snapshot-isolation {@code store} and one
 * transaction process {@code t<i>} per client (START, READ, UPDATE, COMMIT).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>,<variant>}; {@code n} clients = keys; client
 * {@code i} reads key {@code (i+1) mod n} and writes key {@code i} := {@code i+1}. Variants:
 * {@code si} (KVsnap), {@code serializable} (the commit also validates the read key).
 */
public final class KvSnap implements MpProgram {

    /** client -> server / transaction process (Rust {@code Msg::Txn}). */
    @Tag("Txn")
    public record Txn(Tid from, int rk, int wk, int v) implements MpMessage {}

    /** -> client: committed and the value read, 0 when aborted (Rust {@code Msg::Result}). */
    @Tag("Result")
    public record Result(boolean ok, int read) implements MpMessage {}

    /** transaction process -> store: START (Rust {@code Msg::Begin}). */
    @Tag("Begin")
    public record Begin(Tid from) implements MpMessage {}

    /** store -> transaction process: the snapshot (Rust {@code Msg::Snap}). */
    @Tag("Snap")
    public record Snap(List<Integer> store) implements MpMessage {}

    /** transaction process -> store: COMMIT (Rust {@code Msg::Commit}). */
    @Tag("Commit")
    public record Commit(Tid from, int rk, int wk, int v) implements MpMessage {}

    /** store -> transaction process: committed or aborted (Rust {@code Msg::CRes}). */
    @Tag("CRes")
    public record CRes(boolean ok) implements MpMessage {}

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, n, model, variant
     */
    public KvSnap(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("KvSnap takes <spec|impl>,<n>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        this.variant = args.length > 3 ? args[3].trim() : "si";
        if (impl && !List.of("si", "serializable").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "kv-snapshot-isolation-tla-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "n" + n;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (!impl) {
            r.add("server");
        } else {
            r.add("store");
            for (int i = 0; i < n; i++) {
                r.add("t" + i);
            }
        }
        for (int i = 0; i < n; i++) {
            r.add("c" + i);
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("c" + i);
        }
        return r;
    }

    @Override
    public void run() {
        List<MpProcess.Handle> servers = new ArrayList<>();
        List<Tid> front = new ArrayList<>();
        if (!impl) {
            MpProcess.Handle s = MpProcess.spawn("server", this::specServer);
            servers.add(s);
            for (int i = 0; i < n; i++) {
                front.add(s.tid());
            }
        } else {
            boolean validateReads = variant.equals("serializable");
            MpProcess.Handle st = MpProcess.spawn("store", () -> store(validateReads));
            servers.add(st);
            Tid sid = st.tid();
            for (int i = 0; i < n; i++) {
                MpProcess.Handle t = MpProcess.spawn("t" + i, () -> txProcess(sid));
                servers.add(t);
                front.add(t.tid());
            }
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int me = i;
            final Tid to = front.get(i);
            clients.add(MpProcess.spawn("c" + i, () -> client(me, to)));
        }
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        // as in Rust: the transaction processes, then the store / server
        for (int i = 1; i < servers.size(); i++) {
            servers.get(i).await();
        }
        servers.get(0).await();
    }

    private void client(int i, Tid to) {
        Tid me = MpProcess.self();
        Channel.send(m, to, new Txn(me, (i + 1) % n, i, i + 1));
        Channel.recv(m, Result.class);
    }

    private void specServer() {
        int[] store = new int[n];
        for (int c = 0; c < n; c++) {
            Txn t = Channel.recv(m, Txn.class);
            // RollbackTx is always enabled for an open transaction
            if (Nondet.chooseBoolean()) {
                Channel.send(m, t.from(), new Result(false, 0));
            } else {
                int r = store[t.rk()];
                store[t.wk()] = t.v();
                Channel.send(m, t.from(), new Result(true, r));
            }
        }
    }

    private void store(boolean validateReads) {
        int[] store = new int[n];
        List<Tid> tx = new ArrayList<>();
        List<Tid> missedOf = new ArrayList<>();
        List<boolean[]> missed = new ArrayList<>();
        for (int c = 0; c < 2 * n; c++) {
            MpMessage msg = Channel.recv(m, x -> x instanceof Begin || x instanceof Commit);
            if (msg instanceof Begin b) {
                tx.add(b.from());
                missedOf.add(b.from());
                missed.add(new boolean[n]);
                List<Integer> snap = new ArrayList<>();
                for (int v : store) {
                    snap.add(v);
                }
                Channel.send(m, b.from(), new Snap(List.copyOf(snap)));
            } else {
                Commit cm = (Commit) msg;
                int idx = missedOf.indexOf(cm.from());
                boolean[] mine = missed.get(idx);
                boolean conflict = mine[cm.wk()] || (validateReads && mine[cm.rk()]);
                tx.remove(cm.from());
                missedOf.remove(idx);
                missed.remove(idx);
                if (!conflict) {
                    for (int e = 0; e < missedOf.size(); e++) {
                        if (tx.contains(missedOf.get(e))) {
                            missed.get(e)[cm.wk()] = true;
                        }
                    }
                    store[cm.wk()] = cm.v();
                }
                Channel.send(m, cm.from(), new CRes(!conflict));
            }
        }
    }

    private void txProcess(Tid sid) {
        Tid me = MpProcess.self();
        Txn t = Channel.recv(m, Txn.class);
        // START
        Channel.send(m, sid, new Begin(me));
        List<Integer> snap = new ArrayList<>(Channel.recv(m, Snap.class).store());
        // READ
        int r = snap.get(t.rk());
        // UPDATE (local to the snapshot)
        snap.set(t.wk(), t.v());
        // COMMIT
        Channel.send(m, sid, new Commit(me, t.rk(), t.wk(), t.v()));
        boolean ok = Channel.recv(m, CRes.class).ok();
        Channel.send(m, t.from(), new Result(ok, ok ? r : 0));
    }
}
