package bench.n_buyer_kragl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * The N-buyer protocol of Kragl et al. (PLDI 2020, {@code NBuyer.bpl}; candidate N20): the Java
 * twin of {@code rust/src/main.rs} (same threads, spawn order, messages and choices; Rust's
 * per-thread inbox channels with tag-selected receives are thread mailboxes with type-test
 * receives here, the ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>,<variant>}; {@code n >= 2} buyers, price 2,
 * wallet 1. Variants: {@code correct}, {@code no-subtract}. Visible: {@code ledger}.
 */
public final class NBuyer implements MpProgram {

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Req, Quote, Rem, Dec, Contrib, Final {}

    /** b1 -> seller. */
    @Tag("Req")
    public record Req() implements Msg {}

    /** seller -> buyer: the price. */
    @Tag("Quote")
    public record Quote(int q) implements Msg {}

    /** buyer -> next buyer: the remaining amount. */
    @Tag("Rem")
    public record Rem(int r) implements Msg {}

    /** last buyer -> seller: the decision. */
    @Tag("Dec")
    public record Dec(boolean d) implements Msg {}

    /** buyer -> ledger: its contribution. */
    @Tag("Contrib")
    public record Contrib(int i, int a) implements Msg {}

    /** seller -> ledger: the decision received. */
    @Tag("Final")
    public record Final(boolean d) implements Msg {}

    private static final int PRICE = 2;
    private static final int WALLET = 1;

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean subtract;

    /**
     * @param args side, n, model, variant
     */
    public NBuyer(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("NBuyer takes <spec|impl>,<n>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        if (n < 2) {
            throw new IllegalArgumentException("n must be at least 2");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.subtract = switch (v) {
            case "correct" -> true;
            case "no-subtract" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "n-buyer-kragl";
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
        r.add("ledger");
        if (!impl) {
            r.add("oracle");
        } else {
            r.add("seller");
            for (int i = 1; i <= n; i++) {
                r.add("b" + i);
            }
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("ledger");
    }

    private int choose(int rem) {
        return Nondet.chooseBoolean() ? Math.min(WALLET, rem) : 0;
    }

    private void ledger() {
        for (int k = 0; k <= n; k++) {
            Channel.recv(m, Msg.class);
        }
    }

    /** Mailboxes, as the Rust inboxes: 0 seller (or oracle), 1..n buyers, n + 1 ledger. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[n + 2];
        t[n + 1] = Tid.of(ts.add("ledger", this::ledger).getTaskId());
        if (!impl) {
            t[0] = Tid.of(ts.add("oracle", () -> oracle(t)).getTaskId());
        } else {
            t[0] = Tid.of(ts.add("seller", () -> seller(t)).getTaskId());
            for (int i = 1; i <= n; i++) {
                final int ii = i;
                t[i] = Tid.of(ts.add("b" + i, () -> buyer(ii, t)).getTaskId());
            }
        }
        ts.startAndAwaitAll();
    }

    private void oracle(Tid[] t) {
        int rem = PRICE;
        int sum = 0;
        for (int i = 1; i <= n; i++) {
            int a = choose(rem);
            rem -= a;
            sum += a;
            Channel.send(m, t[n + 1], new Contrib(i, a));
        }
        Channel.send(m, t[n + 1], new Final(sum == PRICE));
    }

    private void seller(Tid[] t) {
        Channel.recv(m, Req.class);
        for (int i = 1; i <= n; i++) {
            Channel.send(m, t[i], new Quote(PRICE));
        }
        Dec d = Channel.recv(m, Dec.class);
        Channel.send(m, t[n + 1], new Final(d.d()));
    }

    private void buyer(int i, Tid[] t) {
        if (i == 1) {
            Channel.send(m, t[0], new Req());
        }
        int q = Channel.recv(m, Quote.class).q();
        int r = i == 1 ? q : Channel.recv(m, Rem.class).r();
        int a = choose(r);
        Channel.send(m, t[n + 1], new Contrib(i, a));
        if (i < n) {
            Channel.send(m, t[i + 1], new Rem(subtract ? r - a : r));
        } else {
            Channel.send(m, t[0], new Dec(a == r));
        }
    }
}
