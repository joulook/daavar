package bench.bank_grove;

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
 * bank-grove: gokv's bank over a lock service and a KV store, written from the Rust (TraceForge)
 * program alone. Arguments: {@code <spec|impl>,<K>,<fifo|bag>[,<ordered|unordered>]}; K = transfers
 * per client; the variant (default {@code ordered}) is used by the impl side only.
 */
public final class BankGrove implements MpProgram {

    private static final int ACCTS = 2;
    private static final int BAL_TOTAL = 1;
    private static final int AMOUNT = 1;

    /** The messages: one record per variant of the Rust {@code Msg} enum, same tags and field order. */
    public sealed interface Msg extends MpMessage
            permits Transfer, Done, Audit, Total, CondPut, CondOk, Unlock, Get, Val, Put, PutOk {}

    /** client -> bank / clerk: transfer_internal(src, dst, amt). */
    @Tag("Transfer")
    public record Transfer(Tid from, int src, int dst, int amt) implements Msg {}

    /** -> client: the transfer returned. */
    @Tag("Done")
    public record Done() implements Msg {}

    /** auditor -> bank / auditor's clerk: get_total(). */
    @Tag("Audit")
    public record Audit(Tid from) implements Msg {}

    /** -> auditor: the total. */
    @Tag("Total")
    public record Total(int value) implements Msg {}

    /** clerk -> lock: ConditionalPut(key, "", "1"), answered once it succeeds. */
    @Tag("CondPut")
    public record CondPut(Tid from, int key) implements Msg {}

    /** lock -> clerk: "ok". */
    @Tag("CondOk")
    public record CondOk() implements Msg {}

    /** clerk -> lock: Put(key, ""). */
    @Tag("Unlock")
    public record Unlock(Tid from, int key) implements Msg {}

    /** clerk -> kv: Get(key). */
    @Tag("Get")
    public record Get(Tid from, int key) implements Msg {}

    /** kv -> clerk: the value. */
    @Tag("Val")
    public record Val(int value) implements Msg {}

    /** clerk -> kv: Put(key, val). */
    @Tag("Put")
    public record Put(Tid from, int key, int val) implements Msg {}

    /** kv / lock -> clerk: the Put returned. */
    @Tag("PutOk")
    public record PutOk() implements Msg {}

    private final boolean isSpec;
    private final int k;
    private final CommunicationModel model;
    private final String modelName;
    private final boolean ordered;

    /**
     * @param args {@code <spec|impl>, <K>, <fifo|bag> [, <ordered|unordered>]}
     */
    public BankGrove(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<K>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("side must be spec or impl, was " + args[0]);
        }
        k = Integer.parseInt(args[1]);
        if (k < 0) {
            throw new IllegalArgumentException("K must be >= 0, was " + k);
        }
        modelName = args[2];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
        String variant = args.length == 4 ? args[3] : "ordered";
        ordered = switch (variant) {
            case "ordered" -> true;
            case "unordered" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
    }

    @Override
    public String programName() {
        return "bank-grove";
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
        return isSpec
                ? List.of("bank", "c0", "c1", "auditor")
                : List.of("lock", "kv", "clerk0", "clerk1", "aclerk", "c0", "c1", "auditor");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1", "auditor");
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    /** The visible clients c0, c1 (K transfers each) and auditor (one audit). */
    private List<MpProcess.Handle> spawnClients(Tid[] to) {
        List<MpProcess.Handle> hs = new ArrayList<>();
        final CommunicationModel m = model;
        final int kk = k;
        for (int i = 0; i < 2; i++) {
            final Tid t = to[i];
            final int src = i;
            final int dst = 1 - i;
            hs.add(MpProcess.spawn("c" + i, () -> {
                Tid me = MpProcess.self();
                for (int n = 0; n < kk; n++) {
                    Channel.send(m, t, new Transfer(me, src, dst, AMOUNT));
                    Channel.recv(m, Done.class);
                }
            }));
        }
        final Tid t = to[2];
        hs.add(MpProcess.spawn("auditor", () -> {
            Tid me = MpProcess.self();
            Channel.send(m, t, new Audit(me));
            Channel.recv(m, Total.class);
        }));
        return hs;
    }

    private void spec() {
        final CommunicationModel m = model;
        final int kk = k;
        MpProcess.Handle bank = MpProcess.spawn("bank", () -> {
            int[] bal = {BAL_TOTAL, 0};
            for (int n = 0; n < 2 * kk + 1; n++) {
                Msg msg = (Msg) Channel.recv(m, x -> x instanceof Transfer || x instanceof Audit);
                if (msg instanceof Transfer tr) {
                    if (bal[tr.src()] >= tr.amt()) {
                        bal[tr.src()] -= tr.amt();
                        bal[tr.dst()] += tr.amt();
                    }
                    Channel.send(m, tr.from(), new Done());
                } else if (msg instanceof Audit a) {
                    Channel.send(m, a.from(), new Total(bal[0] + bal[1]));
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        Tid b = bank.tid();
        List<MpProcess.Handle> cs = spawnClients(new Tid[] {b, b, b});
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        bank.await();
    }

    /** The lock clerk's RPCs. */
    private static void lock(CommunicationModel m, Tid l, Tid me, int key) {
        Channel.send(m, l, new CondPut(me, key));
        Channel.recv(m, CondOk.class);
    }

    private static void unlock(CommunicationModel m, Tid l, Tid me, int key) {
        Channel.send(m, l, new Unlock(me, key));
        Channel.recv(m, PutOk.class);
    }

    /** The KV clerk's RPCs. */
    private static int get(CommunicationModel m, Tid kv, Tid me, int key) {
        Channel.send(m, kv, new Get(me, key));
        return Channel.recv(m, Val.class).value();
    }

    private static void put(CommunicationModel m, Tid kv, Tid me, int key, int val) {
        Channel.send(m, kv, new Put(me, key, val));
        Channel.recv(m, PutOk.class);
    }

    private void imp() {
        final CommunicationModel m = model;
        final int kk = k;
        final boolean ord = ordered;
        // Lock and unlock requests: 4 per transfer (2 locks, 2 unlocks) and 4 for the audit.
        final int lockMsgs = 4 * (2 * kk) + 4;
        MpProcess.Handle lk = MpProcess.spawn("lock", () -> {
            boolean[] held = new boolean[ACCTS];
            List<List<Tid>> waiting = new ArrayList<>();
            for (int a = 0; a < ACCTS; a++) {
                waiting.add(new ArrayList<>());
            }
            for (int n = 0; n < lockMsgs; n++) {
                Msg msg = (Msg) Channel.recv(m, x -> x instanceof CondPut || x instanceof Unlock);
                if (msg instanceof CondPut cp) {
                    int key = cp.key();
                    if (!held[key]) {
                        held[key] = true;
                        Channel.send(m, cp.from(), new CondOk());
                    } else {
                        // the clerk keeps retrying; it succeeds once the key is free
                        waiting.get(key).add(cp.from());
                    }
                } else if (msg instanceof Unlock u) {
                    int key = u.key();
                    Channel.send(m, u.from(), new PutOk());
                    int w = waiting.get(key).size();
                    if (w == 0) {
                        held[key] = false;
                    } else {
                        // any retrying clerk may win the next ConditionalPut
                        int j = w == 1 ? 0 : Nondet.chooseIndex(w);
                        Tid next = waiting.get(key).remove(j);
                        Channel.send(m, next, new CondOk());
                    }
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        MpProcess.Handle kvh = MpProcess.spawn("kv", () -> {
            int[] store = {BAL_TOTAL, 0};
            // a blocking server loop: it ends blocked once the clerks are done
            while (true) {
                Msg msg = (Msg) Channel.recv(m, x -> x instanceof Get || x instanceof Put);
                if (msg instanceof Get g) {
                    Channel.send(m, g.from(), new Val(store[g.key()]));
                } else if (msg instanceof Put p) {
                    store[p.key()] = p.val();
                    Channel.send(m, p.from(), new PutOk());
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        final Tid l = lk.tid();
        final Tid kv = kvh.tid();
        List<MpProcess.Handle> clerks = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            clerks.add(MpProcess.spawn("clerk" + i, () -> {
                Tid me = MpProcess.self();
                for (int n = 0; n < kk; n++) {
                    Transfer tr = Channel.recv(m, Transfer.class);
                    int src = tr.src();
                    int dst = tr.dst();
                    int amt = tr.amt();
                    // acquire_two / acquire_two_good
                    if (ord && dst < src) {
                        lock(m, l, me, dst);
                        lock(m, l, me, src);
                    } else {
                        lock(m, l, me, src);
                        lock(m, l, me, dst);
                    }
                    int old = get(m, kv, me, src);
                    if (old >= amt) {
                        put(m, kv, me, src, old - amt);
                        int t = get(m, kv, me, dst);
                        put(m, kv, me, dst, t + amt);
                    }
                    // release_two
                    unlock(m, l, me, src);
                    unlock(m, l, me, dst);
                    Channel.send(m, tr.from(), new Done());
                }
            }));
        }
        clerks.add(MpProcess.spawn("aclerk", () -> {
            Tid me = MpProcess.self();
            Audit au = Channel.recv(m, Audit.class);
            // get_total: the accounts in sorted order
            int sum = 0;
            for (int a = 0; a < ACCTS; a++) {
                lock(m, l, me, a);
                sum += get(m, kv, me, a);
            }
            for (int a = 0; a < ACCTS; a++) {
                unlock(m, l, me, a);
            }
            Channel.send(m, au.from(), new Total(sum));
        }));
        Tid[] to = {clerks.get(0).tid(), clerks.get(1).tid(), clerks.get(2).tid()};
        List<MpProcess.Handle> cs = spawnClients(to);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle c : clerks) {
            c.await();
        }
        lk.await();
        kvh.await();
    }

    @Override
    public String toString() {
        return "bank-grove(" + (isSpec ? "spec" : "impl") + ", K=" + k + ", " + modelName
                + (isSpec ? "" : ", " + (ordered ? "ordered" : "unordered")) + ")";
    }
}
