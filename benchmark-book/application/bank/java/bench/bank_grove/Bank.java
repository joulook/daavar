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
 * gokv's bank over a lock service and a KV store (candidate N10), the Java twin of
 * {@code rust/src/main.rs}. Specification: one {@code bank} thread serving each transfer
 * atomically and the audit with the sum. Implementation: clerks {@code clerk0}, {@code clerk1},
 * {@code aclerk} over {@code lock} (blocking {@code ConditionalPut}, {@code Put(key, "")}) and
 * {@code kv} ({@code Get}, {@code Put}).
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>,<variant>}; {@code K} transfers per client
 * ({@code c0}: account 0 to 1, {@code c1}: 1 to 0, amount 1, {@code BAL_TOTAL} = 1), one audit by
 * {@code auditor}. Variants: {@code ordered} ({@code acquire_two_good}), {@code unordered}
 * ({@code acquire_two}, the source's).
 */
public final class Bank implements MpProgram {

    @Tag("Transfer")
    public record Transfer(Tid from, int src, int dst, int amt) implements MpMessage {}

    @Tag("Done")
    public record Done() implements MpMessage {}

    @Tag("Audit")
    public record Audit(Tid from) implements MpMessage {}

    @Tag("Total")
    public record Total(int sum) implements MpMessage {}

    @Tag("CondPut")
    public record CondPut(Tid from, int key) implements MpMessage {}

    @Tag("CondOk")
    public record CondOk() implements MpMessage {}

    @Tag("Unlock")
    public record Unlock(Tid from, int key) implements MpMessage {}

    @Tag("Get")
    public record Get(Tid from, int key) implements MpMessage {}

    @Tag("Val")
    public record Val(int v) implements MpMessage {}

    @Tag("Put")
    public record Put(Tid from, int key, int val) implements MpMessage {}

    @Tag("PutOk")
    public record PutOk() implements MpMessage {}

    private static final int ACCTS = 2;
    private static final int BAL_TOTAL = 1;
    private static final int AMOUNT = 1;

    private final boolean impl;
    private final int k;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, K, model, variant
     */
    public Bank(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Bank takes <spec|impl>,<K>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        this.variant = args.length > 3 ? args[3].trim() : "ordered";
        if (impl && !List.of("ordered", "unordered").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "bank-grove-" + (impl ? variant : "spec");
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
            return List.of("bank", "c0", "c1", "auditor");
        }
        return List.of("lock", "kv", "clerk0", "clerk1", "aclerk", "c0", "c1", "auditor");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1", "auditor");
    }

    @Override
    public void run() {
        List<MpProcess.Handle> servers = new ArrayList<>();
        Tid[] to = new Tid[3];
        if (!impl) {
            MpProcess.Handle b = MpProcess.spawn("bank", this::bank);
            servers.add(b);
            to[0] = to[1] = to[2] = b.tid();
        } else {
            boolean ordered = variant.equals("ordered");
            MpProcess.Handle lk = MpProcess.spawn("lock", this::lockServer);
            MpProcess.Handle kv = MpProcess.spawn("kv", this::kvServer);
            Tid l = lk.tid();
            Tid kvt = kv.tid();
            List<MpProcess.Handle> clerks = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                clerks.add(MpProcess.spawn("clerk" + i, () -> clerk(l, kvt, ordered)));
            }
            clerks.add(MpProcess.spawn("aclerk", () -> auditClerk(l, kvt)));
            for (int i = 0; i < 3; i++) {
                to[i] = clerks.get(i).tid();
            }
            servers.addAll(clerks);
            servers.add(lk);
            servers.add(kv);
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final int me = i;
            final Tid t = to[i];
            clients.add(MpProcess.spawn("c" + i, () -> client(me, t)));
        }
        final Tid ta = to[2];
        clients.add(MpProcess.spawn("auditor", () -> auditor(ta)));
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        for (MpProcess.Handle s : servers) {
            s.await();
        }
    }

    private void client(int i, Tid t) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            Channel.send(m, t, new Transfer(me, i, 1 - i, AMOUNT));
            Channel.recv(m, Done.class);
        }
    }

    private void auditor(Tid t) {
        Tid me = MpProcess.self();
        Channel.send(m, t, new Audit(me));
        Channel.recv(m, Total.class);
    }

    private void bank() {
        int[] bal = {BAL_TOTAL, 0};
        for (int c = 0; c < 2 * k + 1; c++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Transfer || x instanceof Audit);
            if (q instanceof Transfer t) {
                if (bal[t.src()] >= t.amt()) {
                    bal[t.src()] -= t.amt();
                    bal[t.dst()] += t.amt();
                }
                Channel.send(m, t.from(), new Done());
            } else {
                Audit a = (Audit) q;
                Channel.send(m, a.from(), new Total(bal[0] + bal[1]));
            }
        }
    }

    private void lockServer() {
        int lockMsgs = 4 * (2 * k) + 4;
        boolean[] held = new boolean[ACCTS];
        List<List<Tid>> waiting = new ArrayList<>();
        for (int a = 0; a < ACCTS; a++) {
            waiting.add(new ArrayList<>());
        }
        for (int c = 0; c < lockMsgs; c++) {
            MpMessage q = Channel.recv(m, x -> x instanceof CondPut || x instanceof Unlock);
            if (q instanceof CondPut cp) {
                if (!held[cp.key()]) {
                    held[cp.key()] = true;
                    Channel.send(m, cp.from(), new CondOk());
                } else {
                    // the clerk keeps retrying; it succeeds once the key is free
                    waiting.get(cp.key()).add(cp.from());
                }
            } else {
                Unlock u = (Unlock) q;
                Channel.send(m, u.from(), new PutOk());
                List<Tid> w = waiting.get(u.key());
                if (w.isEmpty()) {
                    held[u.key()] = false;
                } else {
                    // any retrying clerk may win the next ConditionalPut
                    int j = w.size() == 1 ? 0 : Nondet.chooseIndex(w.size());
                    Tid next = w.remove(j);
                    Channel.send(m, next, new CondOk());
                }
            }
        }
    }

    private void kvServer() {
        int[] store = {BAL_TOTAL, 0};
        // a blocking server loop (A4 idiom): it ends blocked once the clerks are done
        while (true) {
            MpMessage q = Channel.recv(m, x -> x instanceof Get || x instanceof Put);
            if (q instanceof Get g) {
                Channel.send(m, g.from(), new Val(store[g.key()]));
            } else {
                Put p = (Put) q;
                store[p.key()] = p.val();
                Channel.send(m, p.from(), new PutOk());
            }
        }
    }

    private void lock(Tid l, Tid me, int key) {
        Channel.send(m, l, new CondPut(me, key));
        Channel.recv(m, CondOk.class);
    }

    private void unlock(Tid l, Tid me, int key) {
        Channel.send(m, l, new Unlock(me, key));
        Channel.recv(m, PutOk.class);
    }

    private int get(Tid kv, Tid me, int key) {
        Channel.send(m, kv, new Get(me, key));
        return Channel.recv(m, Val.class).v();
    }

    private void put(Tid kv, Tid me, int key, int val) {
        Channel.send(m, kv, new Put(me, key, val));
        Channel.recv(m, PutOk.class);
    }

    /** {@code transfer_internal}, K times. */
    private void clerk(Tid l, Tid kv, boolean ordered) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            Transfer t = Channel.recv(m, Transfer.class);
            // acquire_two / acquire_two_good
            if (ordered && t.dst() < t.src()) {
                lock(l, me, t.dst());
                lock(l, me, t.src());
            } else {
                lock(l, me, t.src());
                lock(l, me, t.dst());
            }
            int old = get(kv, me, t.src());
            if (old >= t.amt()) {
                put(kv, me, t.src(), old - t.amt());
                int v = get(kv, me, t.dst());
                put(kv, me, t.dst(), v + t.amt());
            }
            // release_two
            unlock(l, me, t.src());
            unlock(l, me, t.dst());
            Channel.send(m, t.from(), new Done());
        }
    }

    /** {@code get_total}: the accounts in sorted order. */
    private void auditClerk(Tid l, Tid kv) {
        Tid me = MpProcess.self();
        Audit a = Channel.recv(m, Audit.class);
        int sum = 0;
        for (int acct = 0; acct < ACCTS; acct++) {
            lock(l, me, acct);
            sum += get(kv, me, acct);
        }
        for (int acct = 0; acct < ACCTS; acct++) {
            unlock(l, me, acct);
        }
        Channel.send(m, a.from(), new Total(sum));
    }
}
