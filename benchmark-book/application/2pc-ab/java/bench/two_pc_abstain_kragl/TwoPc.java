package bench.two_pc_abstain_kragl;

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
 * One-transaction 2PC with early abort and abstaining participants (Kragl et al., PLDI 2020,
 * {@code 2PC.bpl}; candidate N21): the Java twin of {@code rust/src/main.rs} (same threads, spawn
 * order, messages and choices; Rust's per-thread inbox channels with tag-selected receives are
 * thread mailboxes with type-test receives here, the ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>,<variant>}; {@code n >= 1} participants.
 * Variants: {@code correct}, {@code off-by-one}. Visible: {@code p1 .. pn}.
 */
public final class TwoPc implements MpProgram {

    /** The vote ({@code vote = YES | NO}). */
    public sealed interface Ballot permits Yes, No {}

    /** {@code YES()}. */
    @Tag("Yes")
    public record Yes() implements Ballot {}

    /** {@code NO()}. */
    @Tag("No")
    public record No() implements Ballot {}

    /** The decision ({@code COMMIT | ABORT}). */
    public sealed interface Decision permits Commit, Abort {}

    /** {@code COMMIT()}. */
    @Tag("Commit")
    public record Commit() implements Decision {}

    /** {@code ABORT()}. */
    @Tag("Abort")
    public record Abort() implements Decision {}

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Req, Vote, Dec {}

    /** coordinator -> participant. */
    @Tag("Req")
    public record Req() implements Msg {}

    /** participant -> coordinator. */
    @Tag("Vote")
    public record Vote(Ballot v) implements Msg {}

    /** coordinator -> participant. */
    @Tag("Dec")
    public record Dec(Decision d) implements Msg {}

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean offByOne;

    /**
     * @param args side, n, model, variant
     */
    public TwoPc(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("TwoPc takes <spec|impl>,<n>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        if (n < 1) {
            throw new IllegalArgumentException("n must be at least 1");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.offByOne = switch (v) {
            case "correct" -> false;
            case "off-by-one" -> true;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "two-pc-abstain-kragl";
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

    /** Mailboxes, as the Rust inboxes: 0 coord, 1..n participants. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[n + 1];
        t[0] = Tid.of(ts.add("coord", () -> {
            if (impl) {
                implCoord(t);
            } else {
                specCoord(t);
            }
        }).getTaskId());
        for (int i = 1; i <= n; i++) {
            t[i] = Tid.of(ts.add("p" + i, () -> participant(t)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void participant(Tid[] t) {
        if (Nondet.chooseBoolean()) {
            Channel.recv(m, Req.class);
            Ballot v = Nondet.chooseBoolean() ? new Yes() : new No();
            Channel.send(m, t[0], new Vote(v));
        }
        Channel.recv(m, Dec.class);
    }

    private void broadcast(Tid[] t, Decision d) {
        for (int i = 1; i <= n; i++) {
            Channel.send(m, t[i], new Dec(d));
        }
    }

    /** MAIN5: any decision, Commit only if every participant voted Yes. */
    private void specCoord(Tid[] t) {
        for (int i = 1; i <= n; i++) {
            Channel.send(m, t[i], new Req());
        }
        boolean allYes = true;
        for (int k = 0; k < n; k++) {
            if (Nondet.chooseBoolean()) {
                broadcast(t, new Abort());
                return;
            }
            if (Channel.recv(m, Vote.class).v() instanceof No) {
                allYes = false;
            }
        }
        broadcast(t, allYes && Nondet.chooseBoolean() ? new Commit() : new Abort());
    }

    /** coordinator1 + coordinator2, the vote loop to n (or n - 1, the fault). */
    private void implCoord(Tid[] t) {
        for (int i = 1; i <= n; i++) {
            Channel.send(m, t[i], new Req());
        }
        Decision d = new Commit();
        int quorum = offByOne ? n - 1 : n;
        for (int k = 0; k < quorum; k++) {
            if (Channel.recv(m, Vote.class).v() instanceof No) {
                d = new Abort();
                break;
            }
        }
        broadcast(t, d);
    }
}
