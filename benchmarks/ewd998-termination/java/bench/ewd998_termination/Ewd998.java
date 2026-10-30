package bench.ewd998_termination;

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
 * Safra's termination detection (EWD 998, candidate N14) against a termination oracle: the Java
 * twin of {@code rust/src/main.rs} (same threads, spawn order, messages and choices; Rust's
 * per-thread inbox channels are thread mailboxes here, the ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<N>,<fifo|bag>,<variant>}; N nodes, K = 1 payload per node,
 * R = 2 probes. Variants: {@code correct}, {@code no-black-on-receive}. Visible: {@code app0..}.
 * Rust's {@code sent: Option<u8>} is {@code sent}, -1 for {@code None}.
 */
public final class Ewd998 implements MpProgram {

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Pl, Idle, Tok, Trm, Stop {}

    /** A payload; {@code from} is the sending node. */
    @Tag("Pl")
    public record Pl(int from) implements Msg {}

    /** An app's report on going idle. */
    @Tag("Idle")
    public record Idle(int node, int sent, int got) implements Msg {}

    /** The token. */
    @Tag("Tok")
    public record Tok(int q, boolean black) implements Msg {}

    /** The termination announcement. */
    @Tag("Trm")
    public record Trm() implements Msg {}

    /** Ends a detector or a stub. */
    @Tag("Stop")
    public record Stop() implements Msg {}

    private static final int R = 2;

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean blackOnReceive;

    /**
     * @param args side, N, model, variant
     */
    public Ewd998(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Ewd998 takes <spec|impl>,<N>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.blackOnReceive = switch (v) {
            case "correct" -> true;
            case "no-black-on-receive" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "ewd998-termination-" + (impl ? (blackOnReceive ? "correct" : "no-black-on-receive") : "spec");
    }

    @Override
    public String size() {
        return "n" + n + "k1r" + R;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (!impl) {
            r.add("oracle");
        }
        for (int i = 0; i < n; i++) {
            r.add("det" + i);
        }
        r.addAll(visibleRoles());
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("app" + i);
        }
        return r;
    }

    private Msg recv() {
        return (Msg) Channel.recv(m);
    }

    /** Mailboxes, as the Rust inboxes: apps 0..n, detectors / stubs n..2n, oracle 2n. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[2 * n + 1];
        if (!impl) {
            t[2 * n] = Tid.of(ts.add("oracle", () -> oracle(t)).getTaskId());
            for (int j = 0; j < n; j++) {
                final int jj = j;
                t[n + j] = Tid.of(ts.add("det" + j, () -> stub(jj, t)).getTaskId());
            }
        } else {
            for (int j = 0; j < n; j++) {
                final int jj = j;
                t[n + j] = Tid.of(ts.add("det" + j, () -> detector(jj, t)).getTaskId());
            }
        }
        for (int i = 0; i < n; i++) {
            final int ii = i;
            final int report = impl ? n + i : 2 * n;
            t[i] = Tid.of(ts.add("app" + i, () -> app(ii, t, report)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    /** One active phase: SendMsg (once per node) then Deactivate; returns the new {@code sent}. */
    private int phase(int i, int sent, int got, Tid[] t, int report) {
        if (sent < 0) {
            int c = Nondet.chooseIndex(n);
            if (c > 0) {
                int j = (i + c) % n;
                Channel.send(m, t[n + j], new Pl(i));
                sent = j;
            }
        }
        Channel.send(m, t[report], new Idle(i, sent, got));
        return sent;
    }

    private void app(int i, Tid[] t, int report) {
        int sent = -1;
        int got = 0;
        sent = phase(i, sent, got, t, report);
        for (int s = 0; s < n; s++) {
            Msg x = recv();
            if (x instanceof Trm) {
                return;
            } else if (x instanceof Pl p) {
                got |= 1 << p.from();
                sent = phase(i, sent, got, t, report);
            } else {
                throw new IllegalStateException("app" + i + " got " + x);
            }
        }
    }

    /** The oracle's test (Rust {@code terminated}); {@code view[i] = {sent, got}} or null. */
    private boolean terminated(int[][] view) {
        for (int[] v : view) {
            if (v == null) {
                return false;
            }
        }
        for (int i = 0; i < n; i++) {
            int sent = view[i][0];
            int got = view[i][1];
            if (sent >= 0 && (view[sent][1] & (1 << i)) == 0) {
                return false;
            }
            for (int k = 0; k < n; k++) {
                if ((got & (1 << k)) != 0 && view[k][0] != i) {
                    return false;
                }
            }
        }
        return true;
    }

    private void oracle(Tid[] t) {
        int[][] view = new int[n][];
        while (true) {
            Msg x = recv();
            if (!(x instanceof Idle r)) {
                throw new IllegalStateException("oracle got " + x);
            }
            int[] old = view[r.node()];
            if (old == null || Integer.bitCount(r.got()) > Integer.bitCount(old[1])) {
                view[r.node()] = new int[] {r.sent(), r.got()};
            }
            if (terminated(view)) {
                if (Nondet.chooseBoolean()) {
                    for (int a = 0; a < n; a++) {
                        Channel.send(m, t[a], new Trm());
                    }
                    for (int d = 0; d < n; d++) {
                        Channel.send(m, t[n + d], new Stop());
                    }
                }
                return;
            }
        }
    }

    private void stub(int j, Tid[] t) {
        while (true) {
            Msg x = recv();
            if (x instanceof Pl p) {
                Channel.send(m, t[j], new Pl(p.from()));
            } else if (x instanceof Stop) {
                return;
            } else {
                throw new IllegalStateException("det" + j + " (stub) got " + x);
            }
        }
    }

    private void detector(int i, Tid[] t) {
        int counter = 0;
        boolean black = false;
        int outstanding = 1;
        boolean sentCounted = false;
        boolean hasToken = false;
        int tq = 0;
        boolean tb = false;
        int rounds = 0;
        if (i == 0) {
            Channel.send(m, t[n + n - 1], new Tok(0, false));
            rounds = 1;
        }
        while (true) {
            Msg x = recv();
            if (x instanceof Pl p) {
                counter--;
                if (blackOnReceive) {
                    black = true;
                }
                outstanding++;
                Channel.send(m, t[i], new Pl(p.from()));
            } else if (x instanceof Idle r) {
                if (r.sent() >= 0 && !sentCounted) {
                    counter++;
                    sentCounted = true;
                }
                outstanding--;
            } else if (x instanceof Tok k) {
                hasToken = true;
                tq = k.q();
                tb = k.black();
            } else if (x instanceof Stop) {
                return;
            } else {
                throw new IllegalStateException("det" + i + " got " + x);
            }
            if (hasToken) {
                if (i != 0) {
                    if (outstanding == 0) {
                        Channel.send(m, t[n + i - 1], new Tok(tq + counter, tb || black));
                        black = false;
                        hasToken = false;
                    }
                } else {
                    boolean conclusive = !tb && !black && tq + counter == 0;
                    if (!conclusive) {
                        if (rounds < R) {
                            Channel.send(m, t[n + n - 1], new Tok(0, false));
                            black = false;
                            rounds++;
                        }
                        hasToken = false;
                    } else if (outstanding == 0) {
                        for (int a = 0; a < n; a++) {
                            Channel.send(m, t[a], new Trm());
                        }
                        for (int d = 1; d < n; d++) {
                            Channel.send(m, t[n + d], new Stop());
                        }
                        return;
                    }
                }
            }
        }
    }
}
