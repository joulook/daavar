package bench.two_pc_lbinria;

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
 * Two-phase commit (lbinria), Java twin of the Rust/TraceForge program.
 *
 * <p>Arguments (launcher): {@code <spec|impl>,<N>,<fifo|bag>[,<correct|counting-bug>]}. {@code N} =
 * resource managers; resend bound {@code R = 2}; default variant {@code correct} (ignored by spec).
 *
 * <p>Visible roles: {@code rm0..rm{N-1}}. Invisible: {@code tm} (both sides), {@code p0..p{N-1}}
 * (impl only). Spawn order: spec {@code tm, rm0..}; impl {@code tm, p0.., rm0..}.
 */
public final class TwoPcLbinria implements MpProgram {

    /** Resends per RM: the proxy forwards {@code Prepared} {@code 1..=R} times. */
    static final int R = 2;

    /** Every message of the program. */
    public sealed interface Msg extends MpMessage permits Prepared, Decision {}

    /** {@code Prepared(usize)}: the sending RM's index. */
    @Tag("Prepared")
    public record Prepared(long from) implements Msg {}

    /** {@code enum Decision { Commit, Abort }}, TM to RM. */
    public sealed interface Decision extends Msg permits Commit, Abort {}

    /** {@code Decision::Commit}. */
    @Tag("Commit")
    public record Commit() implements Decision {}

    /** {@code Decision::Abort}. */
    @Tag("Abort")
    public record Abort() implements Decision {}

    private final boolean isSpec;
    private final int n;
    private final CommunicationModel model;
    private final boolean countingBug;

    /**
     * @param args {@code <spec|impl>, <N>, <fifo|bag> [, <correct|counting-bug>]}
     */
    public TwoPcLbinria(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<N>,<fifo|bag>[,<variant>], got "
                    + String.join(",", args));
        }
        isSpec = switch (args[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException("side must be spec or impl, was " + args[0]);
        };
        n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("N must be at least 1, was " + n);
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
        String variant = args.length > 3 ? args[3] : "correct";
        if (isSpec) {
            countingBug = false; // the Rust spec ignores the variant
        } else {
            countingBug = switch (variant) {
                case "correct" -> false;
                case "counting-bug" -> true;
                default -> throw new IllegalArgumentException(
                        "unknown variant " + variant + "; one of correct, counting-bug");
            };
        }
    }

    // ---- MpProgram ----

    @Override
    public String programName() {
        return "two-pc-lbinria";
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
        roles.add("tm");
        if (!isSpec) {
            for (int i = 0; i < n; i++) {
                roles.add("p" + i);
            }
        }
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("rm" + i);
        }
        return roles;
    }

    // ---- the processes ----

    /**
     * The task ids of the RMs, predicted from the spawn order (the process main spawns at
     * 0-based position s is task main + 1 + s; checked empirically), because {@code tm} is
     * spawned before the RMs and must address them; the Rust side uses the named channel
     * {@code ("rm", i)} for this.
     */
    private List<Tid> rmTids(int firstRmSpawnIndex) {
        long mainTask = MpProcess.self().raw(); // no event: main's own task id
        List<Tid> tids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            tids.add(Tid.of(mainTask + 1L + firstRmSpawnIndex + i, "rm" + i));
        }
        return List.copyOf(tids);
    }

    private void sendAll(List<Tid> rms, Decision d) {
        for (Tid rm : rms) {
            Channel.send(model, rm, d);
        }
    }

    /** ResourceManager.run, identical on both sides: crash, or send Prepared once and await the decision. */
    private void rm(int i, Tid server) {
        if (Nondet.chooseBoolean()) {
            return; // crash: no message at all
        }
        Channel.send(model, server, new Prepared(i));
        Channel.recv(model, Decision.class); // RMRcvCommitMsg / RMRcvAbortMsg
    }

    /** The specification's TM oracle (TwoPhase.tla). */
    private void specTm(List<Tid> rms) {
        for (int k = 0; k < n; k++) {
            if (Nondet.chooseBoolean()) {
                sendAll(rms, new Abort());
                return;
            }
            Channel.recv(model, Prepared.class);
        }
        Decision d = Nondet.chooseBoolean() ? new Commit() : new Abort();
        sendAll(rms, d);
    }

    /** TransactionManager.run; counting-bug counts a resent Prepared twice. */
    private void implTm(List<Tid> rms) {
        List<Long> prepared = new ArrayList<>();
        for (int k = 0; k < n * R; k++) {
            if (Nondet.chooseBoolean()) {
                sendAll(rms, new Abort()); // TMAbort (timeout)
                return;
            }
            long from = Channel.recv(model, Prepared.class).from();
            if (countingBug || !prepared.contains(from)) {
                prepared.add(from);
            }
            if (prepared.size() >= n) {
                sendAll(rms, new Commit()); // TMCommit
                return;
            }
        }
    }

    /** The transport of one RM: receive its one Prepared and forward it 1..=R times. */
    private void proxy(Tid tm) {
        Prepared m = Channel.recv(model, Prepared.class);
        List<Integer> ks = new ArrayList<>();
        for (int k = 1; k <= R; k++) {
            ks.add(k);
        }
        int k = Nondet.choose(ks);
        for (int j = 0; j < k; j++) {
            Channel.send(model, tm, m);
        }
    }

    @Override
    public void run() {
        if (isSpec) {
            List<Tid> rms = rmTids(1);
            MpProcess.Handle tm = MpProcess.spawn("tm", () -> specTm(rms));
            Tid tmTid = tm.tid();
            List<MpProcess.Handle> rmHandles = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int idx = i;
                rmHandles.add(MpProcess.spawn("rm" + i, () -> rm(idx, tmTid)));
            }
            for (MpProcess.Handle h : rmHandles) {
                h.await();
            }
            tm.await();
        } else {
            List<Tid> rms = rmTids(1 + n);
            MpProcess.Handle tm = MpProcess.spawn("tm", () -> implTm(rms));
            Tid tmTid = tm.tid();
            List<MpProcess.Handle> ps = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ps.add(MpProcess.spawn("p" + i, () -> proxy(tmTid)));
            }
            List<MpProcess.Handle> rmHandles = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int idx = i;
                Tid p = ps.get(i).tid();
                rmHandles.add(MpProcess.spawn("rm" + i, () -> rm(idx, p)));
            }
            for (MpProcess.Handle h : rmHandles) {
                h.await();
            }
            for (MpProcess.Handle h : ps) {
                h.await();
            }
            tm.await();
        }
    }

    @Override
    public String toString() {
        return "two-pc-lbinria " + (isSpec ? "spec" : "impl") + " N=" + n + " " + model
                + (isSpec ? "" : (countingBug ? " counting-bug" : " correct"));
    }
}
