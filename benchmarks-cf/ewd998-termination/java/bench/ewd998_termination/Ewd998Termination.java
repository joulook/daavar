package bench.ewd998_termination;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
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
 * Safra's termination detection (EWD 998) on a ring of N nodes; Java twin of the Rust program
 * {@code ewd998-termination/rust/src/main.rs}. Arguments: {@code <spec|impl>,<N>,<fifo|bag>[,<variant>]},
 * variant {@code correct} (default) or {@code no-black-on-receive}.
 *
 * <p>Every thread has one inbox: its own process id. The threads address each other circularly, so
 * main mints every process (and so its id) before starting any of them, in the Rust spawn order.
 */
public final class Ewd998Termination implements MpProgram {

    /** Probes det0 may initiate. */
    private static final int R = 2;

    /** The messages: one record per variant of the Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage permits Pl, Idle, Tok, Trm, Stop {}

    /** A payload; {@code from} is the sending node. */
    @Tag("Pl")
    public record Pl(int from) implements Msg {}

    /** An app's report on going idle. */
    @Tag("Idle")
    public record Idle(int node, Optional<Integer> sent, int got) implements Msg {}

    /** Impl only: the token. */
    @Tag("Tok")
    public record Tok(int q, boolean black) implements Msg {}

    /** The termination announcement. */
    @Tag("Trm")
    public record Trm() implements Msg {}

    /** Ends a detector or a stub. */
    @Tag("Stop")
    public record Stop() implements Msg {}

    private final boolean spec;
    private final int n;
    private final CommunicationModel model;
    private final boolean blackOnReceive;

    /**
     * @param args {@code <spec|impl>, <N>, <fifo|bag> [, <variant>]}
     */
    public Ewd998Termination(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<N>,<fifo|bag>[,<variant>]");
        }
        this.spec = switch (args[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException("unknown side " + args[0]);
        };
        this.n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("N must be at least 1, was " + n);
        }
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown model " + args[2]);
        };
        String variant = args.length == 4 ? args[3] : "correct";
        this.blackOnReceive = switch (variant) {
            case "correct" -> true;
            case "no-black-on-receive" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
    }

    @Override
    public String programName() {
        return "ewd998-termination";
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
            roles.add("oracle");
        }
        for (int j = 0; j < n; j++) {
            roles.add("det" + j);
        }
        for (int i = 0; i < n; i++) {
            roles.add("app" + i);
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("app" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        if (spec) {
            spec();
        } else {
            imp();
        }
    }

    // ------------------------------------------------------------------------------------------
    // the application of node i (the same on both sides)

    private Msg recv() {
        return (Msg) Channel.recv(model);
    }

    private void send(Tid dst, Msg m) {
        Channel.send(model, dst, m);
    }

    /** One active phase: SendMsg (at most once per node), then Deactivate (the Idle report). */
    private Optional<Integer> phase(int i, Optional<Integer> sent, int got, Tid[] det, Tid report) {
        if (sent.isEmpty()) {
            // 0: no send; c in 1..n: send to node (i + c) mod n
            int c = Nondet.chooseIndex(n);
            if (c > 0) {
                int j = (i + c) % n;
                send(det[j], new Pl(i));
                sent = Optional.of(j);
            }
        }
        send(report, new Idle(i, sent, got));
        return sent;
    }

    private void app(int i, Tid[] det, Tid report) {
        Optional<Integer> sent = Optional.empty();
        int got = 0;
        sent = phase(i, sent, got, det, report);
        for (int k = 0; k < n; k++) {
            Msg m = recv();
            if (m instanceof Trm) {
                return;
            } else if (m instanceof Pl pl) {
                got |= 1 << pl.from();
                sent = phase(i, sent, got, det, report);
            } else {
                throw new IllegalStateException("app" + i + " got " + m);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // spec

    /** view[i] == null: no report yet; otherwise the latest kept report of node i. */
    private record View(Optional<Integer> sent, int got) {}

    private static boolean terminated(View[] view) {
        for (View v : view) {
            if (v == null) {
                return false;
            }
        }
        int n = view.length;
        for (int i = 0; i < n; i++) {
            Optional<Integer> sent = view[i].sent();
            int got = view[i].got();
            if (sent.isPresent() && (view[sent.get()].got() & (1 << i)) == 0) {
                return false;
            }
            for (int k = 0; k < n; k++) {
                if ((got & (1 << k)) != 0 && !view[k].sent().equals(Optional.of(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    private void oracle(Tid[] apps, Tid[] stubs) {
        View[] view = new View[n];
        while (true) {
            Msg m = recv();
            if (m instanceof Idle idle) {
                View old = view[idle.node()];
                boolean newer = old == null || Integer.bitCount(idle.got()) > Integer.bitCount(old.got());
                if (newer) {
                    view[idle.node()] = new View(idle.sent(), idle.got());
                }
                if (terminated(view)) {
                    if (Nondet.chooseBoolean()) {
                        for (int a = 0; a < n; a++) {
                            send(apps[a], new Trm());
                        }
                        for (int d = 0; d < n; d++) {
                            send(stubs[d], new Stop());
                        }
                    }
                    return;
                }
            } else {
                throw new IllegalStateException("oracle got " + m);
            }
        }
    }

    private void stub(int j, Tid app) {
        while (true) {
            Msg m = recv();
            if (m instanceof Pl pl) {
                send(app, new Pl(pl.from()));
            } else if (m instanceof Stop) {
                return;
            } else {
                throw new IllegalStateException("det" + j + " (stub) got " + m);
            }
        }
    }

    /** Spawn order: oracle, det0.., app0... */
    private void spec() {
        Tid[] apps = new Tid[n];
        Tid[] stubs = new Tid[n];
        Tid[] oracle = new Tid[1];
        List<MpThread> threads = new ArrayList<>();
        threads.add(new MpThread(() -> oracle(apps, stubs), "oracle"));
        for (int j = 0; j < n; j++) {
            final int jj = j;
            threads.add(new MpThread(() -> stub(jj, apps[jj]), "det" + j));
        }
        for (int i = 0; i < n; i++) {
            final int ii = i;
            threads.add(new MpThread(() -> app(ii, stubs, oracle[0]), "app" + i));
        }
        oracle[0] = tidOf(threads.get(0));
        for (int j = 0; j < n; j++) {
            stubs[j] = tidOf(threads.get(1 + j));
        }
        for (int i = 0; i < n; i++) {
            apps[i] = tidOf(threads.get(1 + n + i));
        }
        startAndAwait(threads);
    }

    // ------------------------------------------------------------------------------------------
    // impl

    /** Spawn order: det0.., app0... */
    private void imp() {
        Tid[] apps = new Tid[n];
        Tid[] dets = new Tid[n];
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int ii = i;
            threads.add(new MpThread(() -> detector(ii, apps, dets), "det" + i));
        }
        for (int i = 0; i < n; i++) {
            final int ii = i;
            threads.add(new MpThread(() -> app(ii, dets, dets[ii]), "app" + i));
        }
        for (int i = 0; i < n; i++) {
            dets[i] = tidOf(threads.get(i));
            apps[i] = tidOf(threads.get(n + i));
        }
        startAndAwait(threads);
    }

    /** Safra's detector of node i. */
    private void detector(int i, Tid[] apps, Tid[] dets) {
        int counter = 0; // sends minus receives
        boolean black = false;
        int outstanding = 1; // the app is active until it reports Idle for every Pl it got
        boolean sentCounted = false;
        Tok token = null; // None
        int rounds = 0;
        if (i == 0) {
            // the initial token at node 0 is black, so the first round is initiated at once
            send(dets[n - 1], new Tok(0, false));
            rounds = 1;
        }
        while (true) {
            Msg m = recv();
            if (m instanceof Pl pl) {
                counter -= 1;
                if (blackOnReceive) {
                    black = true;
                }
                outstanding += 1;
                send(apps[i], new Pl(pl.from()));
            } else if (m instanceof Idle idle) {
                if (idle.sent().isPresent() && !sentCounted) {
                    counter += 1;
                    sentCounted = true;
                }
                outstanding -= 1;
            } else if (m instanceof Tok tok) {
                token = tok;
            } else if (m instanceof Stop) {
                return;
            } else {
                throw new IllegalStateException("det" + i + " got " + m);
            }
            if (token != null) {
                int q = token.q();
                boolean tb = token.black();
                if (i != 0) {
                    // PassToken: only when inactive
                    if (outstanding == 0) {
                        send(dets[i - 1], new Tok(q + counter, tb || black));
                        black = false;
                        token = null;
                    }
                } else {
                    boolean conclusive = !tb && !black && q + counter == 0;
                    if (!conclusive) {
                        // InitiateProbe; after R probes, no more
                        if (rounds < R) {
                            send(dets[n - 1], new Tok(0, false));
                            black = false;
                            rounds += 1;
                        }
                        token = null;
                    } else if (outstanding == 0) {
                        for (int a = 0; a < n; a++) {
                            send(apps[a], new Trm());
                        }
                        for (int d = 1; d < n; d++) {
                            send(dets[d], new Stop());
                        }
                        return;
                    }
                    // conclusive but active: keep the token
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------

    private static Tid tidOf(MpThread t) {
        return Tid.of(t.getTaskId(), t.role());
    }

    /** Starts every process in order, then awaits each in the same order (Rust: join in spawn order). */
    private static void startAndAwait(List<MpThread> threads) {
        for (MpThread t : threads) {
            t.start();
        }
        for (MpThread t : threads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting " + t, e);
            }
        }
    }

    @Override
    public String toString() {
        return "Ewd998Termination(" + (spec ? "spec" : "impl") + ", N=" + n + ", " + model
                + (spec ? "" : ", blackOnReceive=" + blackOnReceive) + ")";
    }
}
