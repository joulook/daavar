package bench.two_pc_disel;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark two-pc-disel (candidate N8): Disel's multi-round two-phase commit with
 * acknowledgements ({@code TwoPhaseCoordinator.v}, {@code TwoPhaseParticipant.v}); a
 * line-for-line port of {@code rust/src/main.rs}. Disel's tag filters (Rust: message tags) are
 * type guards here: {@link Prep}, {@link Dec}, {@link Vote}, {@link Ack}.
 *
 * <p>Arguments: {@code <spec|impl>,N[,fifo|bag[,correct|skip-ack-loop]]}; {@code N} participants,
 * {@code K = 2} rounds; default model {@code bag} (asyn), default variant {@code correct}. Visible
 * roles {@code p0..p(N-1)}; invisible {@code cn}.
 */
public final class TwoPcDisel implements MpProgram {

    static final int K = 2;
    static final int[][] DATA = {{1, 2}, {3, 4}};

    /** {@code prep_req}, contents {@code e :: data}. */
    record Prep(int e, int d0, int d1) implements MpMessage {}

    /** {@code commit_req} / {@code abort_req}, contents {@code [e]}. */
    record Dec(boolean commit, int e) implements MpMessage {}

    /** {@code prep_yes} / {@code prep_no} from participant {@code from}. */
    record Vote(int from, int e, boolean yes) implements MpMessage {}

    /** {@code commit_ack} / {@code abort_ack} from participant {@code from}. */
    record Ack(int from, int e, boolean commit) implements MpMessage {}

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean skipAcks;

    /**
     * @param args side, N, model, variant
     */
    public TwoPcDisel(String... args) {
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.skipAcks = switch (v) {
            case "correct" -> false;
            case "skip-ack-loop" -> true;
            default -> throw new IllegalArgumentException("variant must be correct or skip-ack-loop");
        };
    }

    @Override
    public String programName() {
        return "two-pc-disel-" + (impl ? "impl-" + (skipAcks ? "skip-ack-loop" : "correct") : "spec");
    }

    @Override
    public String size() {
        return "N" + n + "K" + K;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("cn");
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("p" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        Support.Procs procs = new Support.Procs();
        Tid[] ps = new Tid[n];
        boolean freeAbort = !impl;
        boolean skip = impl && skipAcks;
        Tid cn = procs.add("cn", () -> coordinator(ps, freeAbort, skip));
        for (int i = 0; i < n; i++) {
            final int me = i;
            ps[i] = procs.add("p" + i, () -> participant(me, cn));
        }
        procs.startAndAwait();
    }

    /** {@code participant_with_choices}, the vote a free choice. */
    private void participant(int i, Tid cn) {
        for (int e = 0; e < K; e++) {
            // receive_prep_req_loop e: the source's round filter never discards (see the Rust
            // port), so one receive and an assertion.
            check(Channel.recv(m, Prep.class).e() == e, "prep_req of another round");
            boolean doCommit = Nondet.chooseBoolean();
            Channel.send(m, cn, new Vote(i, e, doCommit));
            Dec d = Channel.recv(m, Dec.class);
            check(d.e() == e, "decision of another round");
            Channel.send(m, cn, new Ack(i, e, d.commit()));
        }
    }

    /** {@code coordinator_loop}; the spec's decision is TCommit's; {@code skip} is fault F1. */
    private void coordinator(Tid[] ps, boolean freeAbort, boolean skip) {
        for (int e = 0; e < K; e++) {
            for (Tid p : ps) {
                Channel.send(m, p, new Prep(e, DATA[e][0], DATA[e][1]));
            }
            List<Integer> from = new ArrayList<>();
            boolean allYes = true;
            for (int k = 0; k < n; k++) {
                Vote v = Channel.recv(m, Vote.class);
                check(v.e() == e && !from.contains(v.from()), "vote of another round or duplicate");
                from.add(v.from());
                allYes &= v.yes();
            }
            boolean commit = freeAbort ? allYes && Nondet.chooseBoolean() : allYes;
            for (Tid p : ps) {
                Channel.send(m, p, new Dec(commit, e));
            }
            if (skip) {
                continue;
            }
            List<Integer> acks = new ArrayList<>();
            for (int k = 0; k < n; k++) {
                Ack a = Channel.recv(m, Ack.class);
                check(a.commit() == commit && a.e() == e && !acks.contains(a.from()),
                        "ack of another round, kind or duplicate");
                acks.add(a.from());
            }
        }
    }

    private static void check(boolean cond, String what) {
        if (!cond) {
            throw new AssertionError(what);
        }
    }
}
