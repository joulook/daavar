package bench.kv_snapshot_isolation_tla;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * kv-snapshot-isolation-tla: the TLA+ examples' key-value store. Arguments:
 * {@code <spec|impl>,<n>,<fifo|bag>[,<si|serializable>]}; n = clients = keys; variant default si.
 * Spec: a serializable server. Impl: KVsnap's snapshot-isolation store and one transaction
 * process per client.
 */
public final class KvSnapshotIsolation implements MpProgram {

    private final boolean spec;
    private final int n;
    private final CommunicationModel model;
    private final String variant;

    public KvSnapshotIsolation(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<n>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("kind must be spec or impl, was " + args[0]);
        }
        n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1");
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
        variant = args.length == 4 ? args[3] : "si";
        if (!spec && !variant.equals("si") && !variant.equals("serializable")) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "kv-snapshot-isolation-tla";
    }

    @Override
    public String size() {
        return Integer.toString(n);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        if (spec) {
            roles.add("server");
        } else {
            roles.add("store");
            for (int i = 0; i < n; i++) {
                roles.add("t" + i);
            }
        }
        for (int i = 0; i < n; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        if (spec) {
            runSpec();
        } else {
            runImpl();
        }
    }

    /** Client i: one transaction, read key (i+1) mod n, write key i := i+1. */
    private List<MpProcess.Handle> spawnClients(List<Tid> front) {
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final Tid to = front.get(i);
            final int rk = (i + 1) % n;
            final int wk = i;
            final int v = i + 1;
            cs.add(MpProcess.spawn("c" + i, () -> {
                Tid me = MpProcess.self();
                Channel.send(model, to, new Msg.Txn(me, rk, wk, v));
                Channel.recv(model, Msg.Result.class);
            }));
        }
        return cs;
    }

    private void runSpec() {
        final int n = this.n;
        final CommunicationModel model = this.model;
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            int[] store = new int[n];
            for (int k = 0; k < n; k++) {
                Msg.Txn t = Channel.recv(model, Msg.Txn.class);
                // RollbackTx is always enabled for an open transaction
                if (Nondet.chooseBoolean()) {
                    Channel.send(model, t.from(), new Msg.Result(false, 0));
                } else {
                    int r = store[t.rk()];
                    store[t.wk()] = t.v();
                    Channel.send(model, t.from(), new Msg.Result(true, r));
                }
            }
        });
        List<Tid> front = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            front.add(server.tid());
        }
        List<MpProcess.Handle> cs = spawnClients(front);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        server.await();
    }

    private void runImpl() {
        final int n = this.n;
        final CommunicationModel model = this.model;
        final boolean validateReads = variant.equals("serializable");
        MpProcess.Handle storeH = MpProcess.spawn("store", () -> {
            int[] store = new int[n];
            List<Tid> tx = new ArrayList<>();
            List<Tid> missedId = new ArrayList<>();
            List<boolean[]> missedSet = new ArrayList<>();
            for (int k = 0; k < 2 * n; k++) {
                Msg m = (Msg) Channel.recv(model, x -> x instanceof Msg.Begin || x instanceof Msg.Commit);
                if (m instanceof Msg.Begin b) {
                    tx.add(b.from());
                    missedId.add(b.from());
                    missedSet.add(new boolean[n]);
                    List<Integer> snap = new ArrayList<>();
                    for (int s : store) {
                        snap.add(s);
                    }
                    Channel.send(model, b.from(), new Msg.Snap(snap));
                } else if (m instanceof Msg.Commit c) {
                    int idx = missedId.indexOf(c.from());
                    if (idx < 0) {
                        throw new IllegalStateException("commit of an unopened transaction");
                    }
                    boolean[] mine = missedSet.get(idx);
                    boolean conflict = mine[c.wk()] || (validateReads && mine[c.rk()]);
                    tx.remove(c.from());
                    missedId.remove(idx);
                    missedSet.remove(idx);
                    if (!conflict) {
                        for (int e = 0; e < missedId.size(); e++) {
                            if (tx.contains(missedId.get(e))) {
                                missedSet.get(e)[c.wk()] = true;
                            }
                        }
                        store[c.wk()] = c.v();
                    }
                    Channel.send(model, c.from(), new Msg.CRes(!conflict));
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        final Tid sid = storeH.tid();
        List<MpProcess.Handle> ts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ts.add(MpProcess.spawn("t" + i, () -> {
                Tid me = MpProcess.self();
                Msg.Txn t = Channel.recv(model, Msg.Txn.class);
                // START
                Channel.send(model, sid, new Msg.Begin(me));
                Msg.Snap s = Channel.recv(model, Msg.Snap.class);
                int[] snap = new int[s.store().size()];
                for (int j = 0; j < snap.length; j++) {
                    snap[j] = s.store().get(j);
                }
                // READ
                int r = snap[t.rk()];
                // UPDATE (local to the snapshot)
                snap[t.wk()] = t.v();
                // COMMIT
                Channel.send(model, sid, new Msg.Commit(me, t.rk(), t.wk(), t.v()));
                Msg.CRes cr = Channel.recv(model, Msg.CRes.class);
                boolean ok = cr.ok();
                Channel.send(model, t.from(), new Msg.Result(ok, ok ? r : 0));
            }));
        }
        List<Tid> front = new ArrayList<>();
        for (MpProcess.Handle h : ts) {
            front.add(h.tid());
        }
        List<MpProcess.Handle> cs = spawnClients(front);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle t : ts) {
            t.await();
        }
        storeH.await();
    }

    @Override
    public String toString() {
        return "KvSnapshotIsolation(" + (spec ? "spec" : "impl") + "," + n + "," + model + "," + variant + ")";
    }
}
