package bench.two_pc_abstain_kragl;

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

/**
 * Java twin of the Rust benchmark two-pc-abstain-kragl (one-transaction 2PC with early abort and
 * abstaining participants). Arguments: {@code <spec|impl>,<n>,<fifo|bag>[,<correct|off-by-one>]}.
 * Visible roles {@code p1..pn}; {@code coord} is spawned first and is invisible.
 */
public final class TwoPcAbstainKragl implements MpProgram {

    /** Rust {@code enum Ballot { Yes, No }}. */
    public sealed interface Ballot permits Yes, No {}

    @Tag("Yes")
    public record Yes() implements Ballot {}

    @Tag("No")
    public record No() implements Ballot {}

    /** Rust {@code enum Decision { Commit, Abort }}. */
    public sealed interface Decision permits Commit, Abort {}

    @Tag("Commit")
    public record Commit() implements Decision {}

    @Tag("Abort")
    public record Abort() implements Decision {}

    /** Rust {@code enum Msg { Req, Vote(Ballot), Dec(Decision) }}. */
    public sealed interface Msg extends MpMessage permits Req, Vote, Dec {}

    @Tag("Req")
    public record Req() implements Msg {}

    @Tag("Vote")
    public record Vote(Ballot ballot) implements Msg {}

    @Tag("Dec")
    public record Dec(Decision decision) implements Msg {}

    private static final String PROGRAM = "two-pc-abstain-kragl";
    private static final String USAGE = "<spec|impl>,<n>,<fifo|bag>[,<correct|off-by-one>], n >= 1";

    private final boolean spec;
    private final int n;
    private final boolean offByOne;
    private final CommunicationModel model;

    public TwoPcAbstainKragl(String... a) {
        if (a.length < 3 || a.length > 4) {
            throw new IllegalArgumentException(PROGRAM + ": ARGS are " + USAGE + ", got " + List.of(a));
        }
        spec = switch (a[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown side `" + a[0] + "`; " + USAGE);
        };
        try {
            n = Integer.parseInt(a[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PROGRAM + ": <n> must be a number; ARGS are " + USAGE);
        }
        if (n < 1) {
            throw new IllegalArgumentException(PROGRAM + ": n must be at least 1 (the source's axiom n > 0)");
        }
        model = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown model `" + a[2] + "`; " + USAGE);
        };
        String variant = a.length == 4 ? a[3] : "correct";
        offByOne = switch (variant) {
            case "correct" -> false;
            case "off-by-one" -> true;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown variant `" + variant + "`; " + USAGE);
        };
    }

    @Override
    public String programName() {
        return PROGRAM;
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
        List<String> r = new ArrayList<>();
        r.add("coord");
        r.addAll(visibleRoles());
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            r.add("p" + i);
        }
        return r;
    }

    private void send(Tid dst, Msg m) {
        Channel.send(model, dst, m);
    }

    /** {@code participant1} then {@code participant2}; {@code t[0]} is the coordinator. */
    private void participant(Tid[] t) {
        if (Nondet.chooseBoolean()) {
            Channel.recv(model, Req.class);
            Ballot v = Nondet.chooseBoolean() ? new Yes() : new No();
            send(t[0], new Vote(v));
        }
        Channel.recv(model, Dec.class);
    }

    private void broadcast(Tid[] t, Decision d) {
        for (int i = 1; i <= n; i++) {
            send(t[i], new Dec(d));
        }
    }

    /** {@code MAIN5}. */
    private void specCoord(Tid[] t) {
        for (int i = 1; i <= n; i++) {
            send(t[i], new Req());
        }
        boolean allYes = true;
        for (int k = 0; k < n; k++) {
            if (Nondet.chooseBoolean()) {
                broadcast(t, new Abort());
                return;
            }
            Vote v = Channel.recv(model, Vote.class);
            if (v.ballot() instanceof No) {
                allYes = false;
            }
        }
        Decision d = (allYes && Nondet.chooseBoolean()) ? new Commit() : new Abort();
        broadcast(t, d);
    }

    /** {@code coordinator1} + {@code coordinator2}. */
    private void implCoord(int quorum, Tid[] t) {
        for (int i = 1; i <= n; i++) {
            send(t[i], new Req());
        }
        Decision d = new Commit();
        for (int k = 0; k < quorum; k++) {
            Vote v = Channel.recv(model, Vote.class);
            if (v.ballot() instanceof No) {
                d = new Abort();
                break;
            }
        }
        broadcast(t, d);
    }

    @Override
    public void run() {
        // Inboxes 0 = coord, 1..n = participants. As in the Rust program, every address exists
        // before any process is spawned: the threads are created first (minting their ids), then
        // started in the Rust spawn order (coord, p1, ..., pn), then awaited.
        Tid[] t = new Tid[n + 1];
        MpThread[] th = new MpThread[n + 1];
        int quorum = offByOne ? n - 1 : n;
        th[0] = new MpThread(() -> {
            if (spec) {
                specCoord(t);
            } else {
                implCoord(quorum, t);
            }
        }, "coord");
        t[0] = Tid.of(th[0].getTaskId(), "coord");
        for (int i = 1; i <= n; i++) {
            String role = "p" + i;
            th[i] = new MpThread(() -> participant(t), role);
            t[i] = Tid.of(th[i].getTaskId(), role);
        }
        for (MpThread x : th) {
            x.start();
        }
        for (MpThread x : th) {
            try {
                x.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting " + x, e);
            }
        }
    }

    @Override
    public String toString() {
        return PROGRAM + "(" + (spec ? "spec" : "impl") + ", n=" + n + ", " + model
                + (offByOne ? ", off-by-one" : "") + ")";
    }
}
