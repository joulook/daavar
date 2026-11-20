package bench.two_pc_disel;

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
 * two-pc-disel: Disel's multi-round two-phase commit with acknowledgements, written from the
 * Rust (TraceForge) program. Arguments: {@code <spec|impl>,<N>,<fifo|bag>[,<variant>]}, variant
 * {@code correct} (default) or {@code skip-ack-loop}. Visible roles {@code p0..p{N-1}}; the
 * coordinator {@code cn} is invisible.
 */
public final class TwoPcDisel implements MpProgram {

    /** Rounds. */
    static final int K = 2;
    /** data_seq, first K items. */
    static final List<List<Integer>> DATA = List.of(List.of(1, 2), List.of(3, 4));

    /** Coordinator to participant (Rust enum ToP). */
    public sealed interface ToP extends MpMessage permits Prep, Commit, Abort {}

    /** Participant to coordinator (Rust enum ToCn): sender index, round. */
    public sealed interface ToCn extends MpMessage permits Yes, No, CommitAck, AbortAck {}

    @Tag("Prep") public record Prep(int e, List<Integer> d) implements ToP {}
    @Tag("Commit") public record Commit(int e) implements ToP {}
    @Tag("Abort") public record Abort(int e) implements ToP {}

    @Tag("Yes") public record Yes(int from, int e) implements ToCn {}
    @Tag("No") public record No(int from, int e) implements ToCn {}
    @Tag("CommitAck") public record CommitAck(int from, int e) implements ToCn {}
    @Tag("AbortAck") public record AbortAck(int from, int e) implements ToCn {}

    private final boolean spec;
    private final int n;
    private final CommunicationModel model;
    private final String variant;
    private final boolean skipAcks;

    public TwoPcDisel(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<N>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("side must be spec or impl: " + args[0]);
        }
        n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("N must be at least 1");
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        };
        variant = args.length == 4 ? args[3] : "correct";
        skipAcks = switch (variant) {
            case "correct" -> false;
            case "skip-ack-loop" -> true;
            default -> throw new IllegalArgumentException(
                    "unknown variant " + variant + "; one of correct, skip-ack-loop");
        };
    }

    private static void check(boolean cond, String what) {
        if (!cond) {
            throw new AssertionError(what);
        }
    }

    /**
     * Participant i's address, computed from the coordinator's own task id: cn is spawned first
     * and the participants right after it, so p_i is task cn + 1 + i (main checks this).
     */
    private static Tid participantTid(Tid cn, int i) {
        return Tid.of(cn.raw() + 1 + i, "p" + i);
    }

    private void participant(int i, Tid cn) {
        for (int e = 0; e < K; e++) {
            Prep prep = Channel.recv(model, Prep.class);
            check(prep.e() == e, "prep_req of another round");
            boolean doCommit = Nondet.chooseBoolean();
            if (doCommit) {
                Channel.send(model, cn, new Yes(i, e));
            } else {
                Channel.send(model, cn, new No(i, e));
            }
            MpMessage dec = Channel.recv(model, m -> m instanceof Commit || m instanceof Abort);
            boolean committed;
            if (dec instanceof Commit c) {
                check(c.e() == e, "commit_req of another round");
                committed = true;
            } else {
                check(((Abort) dec).e() == e, "abort_req of another round");
                committed = false;
            }
            if (committed) {
                Channel.send(model, cn, new CommitAck(i, e));
            } else {
                Channel.send(model, cn, new AbortAck(i, e));
            }
        }
    }

    private void sendToAll(Tid self, ToP m) {
        for (int i = 0; i < n; i++) {
            Channel.send(model, participantTid(self, i), m);
        }
    }

    private void coordinator(boolean freeAbort, boolean skip) {
        Tid self = MpProcess.self();
        for (int e = 0; e < K; e++) {
            sendToAll(self, new Prep(e, DATA.get(e)));
            List<Integer> from = new ArrayList<>();
            boolean allYes = true;
            for (int j = 0; j < n; j++) {
                MpMessage v = Channel.recv(model, m -> m instanceof Yes || m instanceof No);
                int f;
                int e2;
                if (v instanceof Yes y) {
                    f = y.from();
                    e2 = y.e();
                } else {
                    No no = (No) v;
                    f = no.from();
                    e2 = no.e();
                    allYes = false;
                }
                check(e2 == e && !from.contains(f), "vote of another round or duplicate");
                from.add(f);
            }
            boolean commit = freeAbort ? allYes && Nondet.chooseBoolean() : allYes;
            if (commit) {
                sendToAll(self, new Commit(e));
            } else {
                sendToAll(self, new Abort(e));
            }
            if (skip) {
                continue;
            }
            List<Integer> acks = new ArrayList<>();
            for (int j = 0; j < n; j++) {
                MpMessage a = Channel.recv(model, m -> m instanceof CommitAck || m instanceof AbortAck);
                int f;
                int e2;
                boolean ok;
                if (a instanceof CommitAck c) {
                    f = c.from();
                    e2 = c.e();
                    ok = commit;
                } else {
                    AbortAck ab = (AbortAck) a;
                    f = ab.from();
                    e2 = ab.e();
                    ok = !commit;
                }
                check(ok && e2 == e && !acks.contains(f), "ack of another round, kind or duplicate");
                acks.add(f);
            }
        }
    }

    @Override
    public void run() {
        boolean freeAbort = spec;
        boolean skip = !spec && skipAcks;
        MpProcess.Handle cn = MpProcess.spawn("cn", () -> coordinator(freeAbort, skip));
        Tid cid = cn.tid();
        List<MpProcess.Handle> ps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            MpProcess.Handle h = MpProcess.spawn("p" + i, () -> participant(idx, cid));
            if (h.tid().raw() != cid.raw() + 1 + i) {
                throw new IllegalStateException("participant p" + i + " is task " + h.tid().raw()
                        + ", expected " + (cid.raw() + 1 + i));
            }
            ps.add(h);
        }
        for (MpProcess.Handle h : ps) {
            h.await();
        }
        cn.await();
    }

    @Override
    public String programName() {
        return "two-pc-disel";
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
        r.add("cn");
        for (int i = 0; i < n; i++) {
            r.add("p" + i);
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("p" + i);
        }
        return r;
    }

    @Override
    public String toString() {
        return "two-pc-disel " + (spec ? "spec" : "impl " + variant) + " N=" + n + " " + model;
    }
}
