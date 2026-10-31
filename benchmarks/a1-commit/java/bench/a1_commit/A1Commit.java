package bench.a1_commit;

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
 * TraceForge's A1, two-phase commit deepened to {@code r} rounds: the Java twin of
 * {@code rust/src/main.rs} (same threads, messages, choices and variants).
 *
 * <p>Arguments: {@code spec,<n>,<r>,<fifo|bag>[,<variant>]} or {@code impl,<n>,<r>,<fifo|bag>,<variant>},
 * variant one of {@code correct}, {@code eager}, {@code early-abort}, {@code silent},
 * {@code control-reverse}. The spec has two participants whatever {@code n} (as {@code a1_spec(r)}).
 * Visible: {@code p0}, {@code p1}.
 *
 * <p>The one departure of form: Rust's {@code main} sends {@code Init(ids)} to the coordinator after
 * spawning; a JMC main may perform no message-passing event, so an invisible thread {@code init},
 * spawned last, sends the same {@code Init} (same value, same single receive by {@code coord}).
 */
public final class A1Commit implements MpProgram {

    /** {@code A1ToCoord}. */
    public sealed interface ToCoord extends MpMessage permits Init, Yes, No {}

    /** {@code Init(Vec<ThreadId>)}. */
    @Tag("Init")
    public record Init(List<Tid> ps) implements ToCoord {}

    /** {@code Yes}. */
    @Tag("Yes")
    public record Yes() implements ToCoord {}

    /** {@code No}. */
    @Tag("No")
    public record No() implements ToCoord {}

    /** {@code A1ToPart}. */
    public sealed interface ToPart extends MpMessage permits Prepare, Commit, Abort {}

    /** {@code Prepare(ThreadId)}: the coordinator's id. */
    @Tag("Prepare")
    public record Prepare(Tid coord) implements ToPart {}

    /** {@code Commit}. */
    @Tag("Commit")
    public record Commit() implements ToPart {}

    /** {@code Abort}. */
    @Tag("Abort")
    public record Abort() implements ToPart {}

    /** {@code A1Coord}. */
    enum Kind {
        CORRECT("correct"),
        EAGER("eager"),
        EARLY_ABORT("early-abort"),
        SILENT("silent"),
        CONTROL_REVERSE("control-reverse");

        final String label;

        Kind(String label) {
            this.label = label;
        }

        static Kind byLabel(String s) {
            for (Kind k : values()) {
                if (k.label.equals(s.trim())) {
                    return k;
                }
            }
            throw new IllegalArgumentException(
                    "unknown variant " + s + "; one of correct, eager, early-abort, silent, control-reverse");
        }
    }

    private final boolean impl;
    private final int n;
    private final int r;
    private final CommunicationModel m;
    private final Kind kind;

    /**
     * @param args side, n, r, model, variant (optional on the spec side)
     */
    public A1Commit(String... args) {
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException(
                    "A1Commit takes spec,<n>,<r>,<fifo|bag>[,<variant>] or impl,<n>,<r>,<fifo|bag>,<variant>");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.r = Integer.parseInt(args[2].trim());
        this.m = Support.model(args[3]);
        if (impl && args.length != 5) {
            throw new IllegalArgumentException("the impl side needs a variant");
        }
        this.kind = args.length == 5 ? Kind.byLabel(args[4]) : Kind.CORRECT;
        if (n < 2 || r < 1) {
            throw new IllegalArgumentException("n >= 2 (p0 and p1 are visible) and r >= 1");
        }
    }

    @Override
    public String programName() {
        return "a1-commit-" + (impl ? kind.label : "spec");
    }

    @Override
    public String size() {
        return "n" + n + "r" + r;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    private int participants() {
        return impl ? n : 2;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("coord");
        for (int i = 0; i < participants(); i++) {
            roles.add("p" + i);
        }
        roles.add("init");
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("p0", "p1");
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] coord = new Tid[1];
        coord[0] = Tid.of(ts.add("coord", impl ? () -> coordinator(coord[0]) : () -> oracle(coord[0])).getTaskId());
        List<Tid> ids = new ArrayList<>();
        for (int i = 0; i < participants(); i++) {
            ids.add(Tid.of(ts.add("p" + i, this::participant).getTaskId()));
        }
        List<Tid> init = List.copyOf(ids);
        ts.add("init", () -> Channel.send(m, coord[0], new Init(init)));
        ts.startAndAwaitAll();
    }

    /** {@code a1_participant}: {@code r} rounds of recv Prepare; vote; send; recv decision. */
    private void participant() {
        for (int i = 0; i < r; i++) {
            ToPart p = Channel.recv(m, ToPart.class);
            if (!(p instanceof Prepare prep)) {
                return;
            }
            boolean vote = Nondet.chooseBoolean();
            Channel.send(m, prep.coord(), vote ? new Yes() : new No());
            Channel.recv(m, ToPart.class);
        }
    }

    private List<Tid> readInit() {
        ToCoord x = Channel.recv(m, ToCoord.class);
        return x instanceof Init init ? init.ps() : null;
    }

    /** {@code a1_coordinator(kind, r)}. */
    private void coordinator(Tid me) {
        List<Tid> ps = readInit();
        if (ps == null) {
            return;
        }
        for (int round = 0; round < r; round++) {
            for (Tid p : ps) {
                Channel.send(m, p, new Prepare(me));
            }
            switch (kind) {
                case CORRECT, SILENT, CONTROL_REVERSE -> {
                    int yes = 0;
                    for (int i = 0; i < ps.size(); i++) {
                        if (Channel.recv(m, ToCoord.class) instanceof Yes) {
                            yes++;
                        }
                    }
                    boolean commit = yes == ps.size();
                    List<Tid> order = new ArrayList<>(ps);
                    if (kind == Kind.CONTROL_REVERSE) {
                        java.util.Collections.reverse(order);
                    }
                    for (int i = 0; i < order.size(); i++) {
                        boolean silent = kind == Kind.SILENT && round + 1 == r && i == 1;
                        if (!silent) {
                            Channel.send(m, order.get(i), commit ? new Commit() : new Abort());
                        }
                    }
                }
                case EAGER -> {
                    boolean seenNo = false;
                    for (Tid p : ps) {
                        if (Channel.recv(m, ToCoord.class) instanceof No) {
                            seenNo = true;
                        }
                        Channel.send(m, p, seenNo ? new Abort() : new Commit());
                    }
                }
                case EARLY_ABORT -> {
                    int read = 0;
                    boolean aborted = false;
                    while (read < ps.size()) {
                        ToCoord v = Channel.recv(m, ToCoord.class);
                        read++;
                        if (!aborted && v instanceof No) {
                            aborted = true;
                            for (Tid p : ps) {
                                Channel.send(m, p, new Abort());
                            }
                        }
                    }
                    if (!aborted) {
                        for (Tid p : ps) {
                            Channel.send(m, p, new Commit());
                        }
                    }
                }
                default -> throw new IllegalStateException();
            }
        }
    }

    /** The spec's coordinator: reads every vote, then a free choice, the same decision to both. */
    private void oracle(Tid me) {
        List<Tid> ps = readInit();
        if (ps == null) {
            return;
        }
        for (int round = 0; round < r; round++) {
            for (Tid p : ps) {
                Channel.send(m, p, new Prepare(me));
            }
            for (int i = 0; i < ps.size(); i++) {
                Channel.recv(m, ToCoord.class);
            }
            boolean commit = Nondet.chooseBoolean();
            for (Tid p : ps) {
                Channel.send(m, p, commit ? new Commit() : new Abort());
            }
        }
    }
}
