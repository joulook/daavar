package bench.a1_commit;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A1: two-phase commit deepened to {@code r} rounds, the Java twin of the Rust program
 * {@code a1-commit} (TraceForge Must API).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<r>,<fifo|bag>,<variant>}, variant one of {@code correct},
 * {@code eager}, {@code early-abort}, {@code silent}, {@code control-reverse}. The Rust order
 * {@code <spec|impl>,<variant>,<n>,<r>,<fifo|bag>} is accepted too (a non-numeric second argument
 * is read as the variant). The spec reads only {@code r} (two participants whatever {@code n}), but
 * every argument is checked on both sides.
 *
 * <p>Visible roles: {@code p0}, {@code p1}, whatever the arguments.
 */
public final class A1Commit implements MpProgram {

    // ------------------------------------------------------------ messages ----

    /** {@code A1ToCoord}. */
    public sealed interface ToCoord extends MpMessage permits Init, Yes, No {}

    /** {@code A1ToCoord::Init(Vec<ThreadId>)}; declared for the mapping, never sent (main sends nothing). */
    @Tag("Init")
    public record Init(List<Tid> ps) implements ToCoord {
        public Init {
            ps = List.copyOf(ps);
        }
    }

    /** {@code A1ToCoord::Yes}. */
    @Tag("Yes")
    public record Yes() implements ToCoord {}

    /** {@code A1ToCoord::No}. */
    @Tag("No")
    public record No() implements ToCoord {}

    /** {@code A1ToPart}. */
    public sealed interface ToPart extends MpMessage permits Prepare, Commit, Abort {}

    /** {@code A1ToPart::Prepare(ThreadId)}. */
    @Tag("Prepare")
    public record Prepare(Tid coord) implements ToPart {}

    /** {@code A1ToPart::Commit}. */
    @Tag("Commit")
    public record Commit() implements ToPart {}

    /** {@code A1ToPart::Abort}. */
    @Tag("Abort")
    public record Abort() implements ToPart {}

    /** {@code A1Coord}. */
    public enum Kind {
        CORRECT("correct"),
        EAGER("eager"),
        EARLY_ABORT("early-abort"),
        SILENT("silent"),
        CONTROL_REVERSE("control-reverse");

        final String arg;

        Kind(String arg) {
            this.arg = arg;
        }

        static Kind parse(String s) {
            for (Kind k : values()) {
                if (k.arg.equals(s)) {
                    return k;
                }
            }
            throw new IllegalArgumentException(PROGRAM + ": unknown variant `" + s + "`; " + USAGE);
        }
    }

    // ----------------------------------------------------------- arguments ----

    private static final String PROGRAM = "a1-commit";
    private static final String USAGE = "arguments are <spec|impl>,<n>,<r>,<fifo|bag>,<variant>,"
            + " variant one of correct, eager, early-abort, silent, control-reverse";

    private final boolean spec;
    private final int n;
    private final int r;
    private final CommunicationModel model;
    private final String modelArg;
    private final Kind kind;

    /**
     * The launcher's constructor.
     *
     * @param args {@code <spec|impl>,<n>,<r>,<fifo|bag>,<variant>}
     */
    public A1Commit(String... args) {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    PROGRAM + ": expected 5 arguments, got " + args.length + " " + List.of(args) + "; " + USAGE);
        }
        spec = switch (args[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException(PROGRAM + ": first argument must be spec or impl; " + USAGE);
        };
        String nArg;
        String rArg;
        String variantArg;
        if (isNumber(args[1])) {
            nArg = args[1];
            rArg = args[2];
            modelArg = args[3];
            variantArg = args[4];
        } else {
            // The Rust order: <variant> <n> <r>.
            variantArg = args[1];
            nArg = args[2];
            rArg = args[3];
            modelArg = args[4];
        }
        n = num(nArg, "n");
        r = num(rArg, "r");
        model = switch (modelArg) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException(PROGRAM + ": model must be fifo or bag, got `" + modelArg + "`; " + USAGE);
        };
        kind = Kind.parse(variantArg);
    }

    private static boolean isNumber(String s) {
        return !s.isEmpty() && s.chars().allMatch(Character::isDigit);
    }

    private static int num(String s, String what) {
        try {
            int v = Integer.parseInt(s);
            if (v < 0) {
                throw new NumberFormatException();
            }
            return v;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PROGRAM + ": <" + what + "> must be a number, got `" + s + "`; " + USAGE);
        }
    }

    // ------------------------------------------------------------ the roles ----

    /** Participants spawned: {@code n} in the impl, always 2 in the spec. */
    private int participants() {
        return spec ? 2 : n;
    }

    /**
     * The participants' ids, built by the coordinator instead of received in {@code Init} (main
     * performs no message-passing event): the k-th process main spawns is task {@code k + 1}
     * ({@link MpProgram}, k counted from 1): coord (k = 1) is task 2, so {@code p_i} (k = i + 2) is task
     * {@code i + 3}. Main checks this at every spawn.
     */
    private List<Tid> participantIds() {
        List<Tid> ids = new ArrayList<>();
        for (int i = 0; i < participants(); i++) {
            ids.add(Tid.of(i + 3, "p" + i));
        }
        return ids;
    }

    // ------------------------------------------------------------ processes ----

    /** A participant: {@code r} rounds of {@code recv Prepare; vote; send; recv decision}. */
    private void participant() {
        for (int round = 0; round < r; round++) {
            ToPart m = Channel.recv(model, ToPart.class);
            if (!(m instanceof Prepare p)) {
                return;
            }
            Tid cid = p.coord();
            boolean vote = Nondet.chooseBoolean();
            Channel.send(model, cid, vote ? new Yes() : new No());
            Channel.recv(model, ToPart.class); // the decision, ignored
        }
    }

    /** The implementation's coordinator. */
    private void coordinator() {
        List<Tid> ps = participantIds();
        Tid me = MpProcess.self().withRole("coord");
        for (int round = 0; round < r; round++) {
            for (Tid p : ps) {
                Channel.send(model, p, new Prepare(me));
            }
            switch (kind) {
                case CORRECT, SILENT, CONTROL_REVERSE -> {
                    int yes = 0;
                    for (int i = 0; i < ps.size(); i++) {
                        if (Channel.recv(model, ToCoord.class) instanceof Yes) {
                            yes++;
                        }
                    }
                    ToPart d = yes == ps.size() ? new Commit() : new Abort();
                    List<Tid> order = new ArrayList<>(ps);
                    if (kind == Kind.CONTROL_REVERSE) {
                        Collections.reverse(order);
                    }
                    for (int i = 0; i < order.size(); i++) {
                        boolean silent = kind == Kind.SILENT && round + 1 == r && i == 1;
                        if (!silent) {
                            Channel.send(model, order.get(i), d);
                        }
                    }
                }
                case EAGER -> {
                    boolean seenNo = false;
                    for (Tid p : ps) {
                        if (Channel.recv(model, ToCoord.class) instanceof No) {
                            seenNo = true;
                        }
                        Channel.send(model, p, seenNo ? new Abort() : new Commit());
                    }
                }
                case EARLY_ABORT -> {
                    int read = 0;
                    boolean aborted = false;
                    while (read < ps.size()) {
                        ToCoord v = Channel.recv(model, ToCoord.class);
                        read++;
                        if (!aborted && v instanceof No) {
                            aborted = true;
                            for (Tid p : ps) {
                                Channel.send(model, p, new Abort());
                            }
                        }
                    }
                    if (!aborted) {
                        for (Tid p : ps) {
                            Channel.send(model, p, new Commit());
                        }
                    }
                }
                default -> throw new IllegalStateException("unreachable: " + kind);
            }
        }
    }

    /** The specification's coordinator: reads every vote, then chooses the decision by nondet. */
    private void specCoordinator() {
        List<Tid> ps = participantIds();
        Tid me = MpProcess.self().withRole("coord");
        for (int round = 0; round < r; round++) {
            for (Tid p : ps) {
                Channel.send(model, p, new Prepare(me));
            }
            for (int i = 0; i < ps.size(); i++) {
                Channel.recv(model, ToCoord.class); // the vote, ignored
            }
            ToPart d = Nondet.chooseBoolean() ? new Commit() : new Abort();
            for (Tid p : ps) {
                Channel.send(model, p, d);
            }
        }
    }

    // ----------------------------------------------------------------- main ----

    /** Main: coordinator first, then {@code p0..}; no message (the coordinator builds the list); then awaits all. */
    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        MpProcess.Handle coord = MpProcess.spawn("coord", spec ? this::specCoordinator : this::coordinator);
        handles.add(coord);
        List<Tid> expected = participantIds();
        for (int i = 0; i < participants(); i++) {
            MpProcess.Handle h = MpProcess.spawn("p" + i, this::participant);
            handles.add(h);
            if (!h.tid().equals(expected.get(i))) {
                throw new IllegalStateException("spawn-order rule broken: p" + i + " is " + h.tid()
                        + ", expected " + expected.get(i));
            }
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    // ---------------------------------------------------------- MpProgram ----

    @Override
    public String programName() {
        return PROGRAM;
    }

    @Override
    public String size() {
        return n + "," + r;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("coord");
        for (int i = 0; i < participants(); i++) {
            roles.add("p" + i);
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("p0", "p1");
    }

    @Override
    public String toString() {
        return PROGRAM + "(" + (spec ? "spec" : "impl") + ", n=" + n + ", r=" + r + ", " + modelArg + ", " + kind.arg + ")";
    }
}
