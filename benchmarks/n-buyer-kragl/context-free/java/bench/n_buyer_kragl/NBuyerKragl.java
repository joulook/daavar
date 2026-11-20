package bench.n_buyer_kragl;

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
 * Java twin of the Rust n-buyer-kragl program (Kragl et al., PLDI 2020, NBuyer.bpl).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>[,<correct|no-subtract>]}, n >= 2.
 * Visible role: {@code ledger}. PRICE = 2, WALLET = 1.
 */
public final class NBuyerKragl implements MpProgram {

    static final int PRICE = 2;
    static final int WALLET = 1;

    /** The messages: one record per Rust {@code Msg} variant, same tag, same field order. */
    public sealed interface Msg extends MpMessage
            permits Req, Quote, Rem, Dec, Contrib, Final {}

    /** b1 -> seller. */
    @Tag("Req")
    public record Req() implements Msg {}

    /** seller -> buyer. */
    @Tag("Quote")
    public record Quote(int price) implements Msg {}

    /** buyer -> next buyer. */
    @Tag("Rem")
    public record Rem(int rem) implements Msg {}

    /** last buyer -> seller. */
    @Tag("Dec")
    public record Dec(boolean d) implements Msg {}

    /** buyer (or oracle) -> ledger. */
    @Tag("Contrib")
    public record Contrib(int i, int a) implements Msg {}

    /** seller (or oracle) -> ledger. */
    @Tag("Final")
    public record Final(boolean d) implements Msg {}

    private final boolean spec;
    private final int n;
    private final String sizeText;
    private final CommunicationModel model;
    private final boolean subtract;

    /**
     * @param args {@code <spec|impl>, <n>, <fifo|bag> [, <correct|no-subtract>]}
     */
    public NBuyerKragl(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "n-buyer-kragl: arguments are <spec|impl>,<n>,<fifo|bag>[,<correct|no-subtract>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("n-buyer-kragl: unknown side " + args[0]);
        }
        try {
            n = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("n-buyer-kragl: <n> must be a number, got " + args[1]);
        }
        if (n < 2) {
            throw new IllegalArgumentException("n-buyer-kragl: n must be at least 2");
        }
        sizeText = args[1];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("n-buyer-kragl: unknown model " + args[2]);
        };
        String variant = args.length == 4 ? args[3] : "correct";
        subtract = switch (variant) {
            case "correct" -> true;
            case "no-subtract" -> false;
            default -> throw new IllegalArgumentException("n-buyer-kragl: unknown variant " + variant);
        };
    }

    @Override
    public String programName() {
        return "n-buyer-kragl";
    }

    @Override
    public String size() {
        return sizeText;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("ledger");
        if (spec) {
            roles.add("oracle");
        } else {
            roles.add("seller");
            for (int i = 1; i <= n; i++) {
                roles.add("b" + i);
            }
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("ledger");
    }

    /** {@code if (*) amount := min(wallet, rem) else amount := 0}; min(S) is the first branch. */
    private static int choose(int rem) {
        return Nondet.chooseBoolean() ? Math.min(WALLET, rem) : 0;
    }

    /** The visible observer: n + 1 blocking receives of anything. */
    private static void ledger(CommunicationModel m, int n) {
        for (int k = 0; k <= n; k++) {
            Channel.recv(m);
        }
    }

    @Override
    public void run() {
        if (spec) {
            runSpec();
        } else {
            runImpl();
        }
    }

    /**
     * The identifier the k-th spawned process (k >= 1) will get: task ids are minted
     * sequentially at construction, and only main creates tasks. Checked against every handle.
     */
    private static Tid expected(long base, int k) {
        return Tid.of(base + k);
    }

    private static void check(MpProcess.Handle h, Tid want) {
        if (!h.tid().equals(want)) {
            throw new IllegalStateException("spawned " + h.tid() + ", expected task " + want.raw());
        }
    }

    private void runSpec() {
        final CommunicationModel m = model;
        final int nn = n;
        long base = MpProcess.self().raw();
        final Tid ledgerTid = expected(base, 1);
        MpProcess.Handle l = MpProcess.spawn("ledger", () -> ledger(m, nn));
        check(l, ledgerTid);
        MpProcess.Handle o = MpProcess.spawn("oracle", () -> {
            int rem = PRICE;
            int sum = 0;
            for (int i = 1; i <= nn; i++) {
                int a = choose(rem);
                rem -= a;
                sum += a;
                Channel.send(m, ledgerTid, new Contrib(i, a));
            }
            Channel.send(m, ledgerTid, new Final(sum == PRICE));
        });
        check(o, expected(base, 2));
        l.await();
        o.await();
    }

    private void runImpl() {
        final CommunicationModel m = model;
        final int nn = n;
        final boolean sub = subtract;
        long base = MpProcess.self().raw();
        // inboxes, as the Rust Vec<Tx>: 0 seller, 1..=n buyers, n + 1 ledger.
        // spawn order: ledger (1), seller (2), b1..bn (3..n+2).
        final Tid[] t = new Tid[nn + 2];
        t[nn + 1] = expected(base, 1);
        t[0] = expected(base, 2);
        for (int i = 1; i <= nn; i++) {
            t[i] = expected(base, 2 + i);
        }
        List<MpProcess.Handle> hs = new ArrayList<>();
        hs.add(MpProcess.spawn("ledger", () -> ledger(m, nn)));
        hs.add(MpProcess.spawn("seller", () -> {
            // sellerInit
            Channel.recv(m, Req.class);
            for (int i = 1; i <= nn; i++) {
                Channel.send(m, t[i], new Quote(PRICE));
            }
            // sellerFinish
            Dec d = Channel.recv(m, Dec.class);
            Channel.send(m, t[nn + 1], new Final(d.d()));
        }));
        for (int idx = 1; idx <= nn; idx++) {
            final int i = idx;
            hs.add(MpProcess.spawn("b" + i, () -> {
                if (i == 1) {
                    // firstBuyerInit, firstBuyer
                    Channel.send(m, t[0], new Req());
                }
                int q = Channel.recv(m, Quote.class).price();
                int r = i == 1 ? q : Channel.recv(m, Rem.class).rem();
                int a = choose(r);
                Channel.send(m, t[nn + 1], new Contrib(i, a));
                if (i < nn) {
                    int fwd = sub ? r - a : r;
                    Channel.send(m, t[i + 1], new Rem(fwd));
                } else {
                    // lastBuyer
                    Channel.send(m, t[0], new Dec(a == r));
                }
            }));
        }
        check(hs.get(0), t[nn + 1]);
        check(hs.get(1), t[0]);
        for (int i = 1; i <= nn; i++) {
            check(hs.get(1 + i), t[i]);
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    @Override
    public String toString() {
        return "n-buyer-kragl " + (spec ? "spec" : "impl") + " n=" + n + " " + model
                + (spec ? "" : (subtract ? " correct" : " no-subtract"));
    }
}
