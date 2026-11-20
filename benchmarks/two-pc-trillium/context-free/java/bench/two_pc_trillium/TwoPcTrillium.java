package bench.two_pc_trillium;

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
 * Two-phase commit of Trillium (AnerisLang), Java twin of the Rust TraceForge program
 * {@code daavar/benchmarks/two-pc-trillium/rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,<N>,<fifo|bag>[,<correct|ignore-abort-vote>]}; N = resource
 * managers. Visible roles {@code obs0..obs{N-1}}; invisible {@code tc} (spec), {@code tm} and
 * {@code rm0..rm{N-1}} (impl).
 */
public final class TwoPcTrillium implements MpProgram {

    /** TM to RM. */
    public sealed interface ToRm extends MpMessage permits ToRm.Prepare, ToRm.Commit, ToRm.Abort {
        @Tag("Prepare")
        record Prepare() implements ToRm {}

        @Tag("Commit")
        record Commit() implements ToRm {}

        @Tag("Abort")
        record Abort() implements ToRm {}
    }

    /** RM to TM, with the sender's RM index. */
    public sealed interface ToTm extends MpMessage permits ToTm.Prepared, ToTm.Aborted, ToTm.Committed {
        int rm();

        @Tag("Prepared")
        record Prepared(int rm) implements ToTm {}

        @Tag("Aborted")
        record Aborted(int rm) implements ToTm {}

        @Tag("Committed")
        record Committed(int rm) implements ToTm {}
    }

    /** RM i's final TC_model state, reported to obs{i}. */
    public sealed interface Outcome extends MpMessage permits Outcome.Committed, Outcome.Aborted {
        @Tag("Committed")
        record Committed() implements Outcome {}

        @Tag("Aborted")
        record Aborted() implements Outcome {}
    }

    private final boolean isSpec;
    private final int n;
    private final CommunicationModel model;
    private final String modelName;
    private final String variant;

    /**
     * @param args {@code <spec|impl>, <N>, <fifo|bag> [, <variant>]}
     */
    public TwoPcTrillium(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "expected <spec|impl>,<N>,<fifo|bag>[,<correct|ignore-abort-vote>]");
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("kind must be spec or impl, was " + args[0]);
        }
        n = Integer.parseInt(args[1]);
        if (n < 0) {
            throw new IllegalArgumentException("N must be >= 0, was " + n);
        }
        modelName = args[2];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
        variant = args.length == 4 ? args[3] : "correct";
        if (!isSpec && !variant.equals("correct") && !variant.equals("ignore-abort-vote")) {
            throw new IllegalArgumentException(
                    "unknown variant " + variant + "; one of correct, ignore-abort-vote");
        }
    }

    @Override
    public String programName() {
        return "two-pc-trillium";
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
        List<String> r = new ArrayList<>(observerRoles());
        if (isSpec) {
            r.add("tc");
        } else {
            r.add("tm");
            for (int i = 0; i < n; i++) {
                r.add("rm" + i);
            }
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return observerRoles();
    }

    private List<String> observerRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("obs" + i);
        }
        return r;
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp(variant.equals("ignore-abort-vote"));
        }
    }

    /** obs{i}: one blocking receive of RM i's outcome (identical on both sides). */
    private List<MpProcess.Handle> spawnObservers() {
        CommunicationModel m = model;
        List<MpProcess.Handle> hs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            hs.add(MpProcess.spawn("obs" + i, () -> Channel.recv(m, Outcome.class)));
        }
        return hs;
    }

    /** The specification: one oracle tc draws every RM's coin and reports one outcome to all. */
    private void spec() {
        List<MpProcess.Handle> obs = spawnObservers();
        List<Tid> oids = obs.stream().map(MpProcess.Handle::tid).toList();
        CommunicationModel m = model;
        int count = n;
        MpProcess.Handle tc = MpProcess.spawn("tc", () -> {
            boolean allPrepared = true;
            for (int i = 0; i < count; i++) {
                if (Nondet.chooseBoolean()) {
                    allPrepared = false;
                }
            }
            Outcome o = allPrepared ? new Outcome.Committed() : new Outcome.Aborted();
            for (Tid id : oids) {
                Channel.send(m, id, o);
            }
        });
        for (MpProcess.Handle h : obs) {
            h.await();
        }
        tc.await();
    }

    private void imp(boolean ignoreAbortVote) {
        List<MpProcess.Handle> obs = spawnObservers();
        List<Tid> oids = obs.stream().map(MpProcess.Handle::tid).toList();
        // The Rust RM inbox is the named channel ("rm", i), reachable by tm although rm i is spawned
        // after tm. The Java API has only per-process mailboxes, so tm addresses rm i by its task
        // id, which the spawn order fixes: task ids are minted consecutively at spawn, so with
        // b = obs0's id, obs i is b + i, tm is b + N and rm i is b + N + 1 + i. Checked below.
        long base = n > 0 ? oids.get(0).raw() : -1;
        List<Tid> rmIds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rmIds.add(Tid.of(base + n + 1L + i, "rm" + i));
        }
        List<Tid> rmTids = List.copyOf(rmIds);
        CommunicationModel m = model;
        int count = n;
        MpProcess.Handle tm = MpProcess.spawn("tm", () -> transactionManager(m, count, rmTids, ignoreAbortVote));
        Tid tmId = tm.tid();
        List<MpProcess.Handle> rms = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int idx = i;
            Tid o = oids.get(i);
            rms.add(MpProcess.spawn("rm" + i, () -> resourceManager(m, idx, tmId, o)));
        }
        for (int i = 0; i < n; i++) {
            if (oids.get(i).raw() != base + i) {
                throw new IllegalStateException("obs" + i + " is " + oids.get(i) + ", expected " + (base + i));
            }
        }
        if (n > 0 && tmId.raw() != base + n) {
            throw new IllegalStateException("tm is " + tmId + ", expected " + (base + n));
        }
        for (int i = 0; i < n; i++) {
            if (!rms.get(i).tid().equals(rmTids.get(i))) {
                throw new IllegalStateException(
                        "rm" + i + " is " + rms.get(i).tid() + ", expected " + rmTids.get(i));
            }
        }
        for (MpProcess.Handle h : obs) {
            h.await();
        }
        for (MpProcess.Handle h : rms) {
            h.await();
        }
        tm.await();
    }

    /** resource_manager rm tm, plus the report of its final state to obs{i}. */
    private static void resourceManager(CommunicationModel m, int i, Tid tm, Tid obs) {
        ToRm value = Channel.recv(m, ToRm.class);
        if (value instanceof ToRm.Abort) {
            // someone already aborted
            Channel.send(m, tm, new ToTm.Aborted(i));
            Channel.send(m, obs, new Outcome.Aborted());
        } else {
            boolean localAbort = Nondet.chooseBoolean(); // coin_flip ()
            if (localAbort) {
                Channel.send(m, tm, new ToTm.Aborted(i));
                Channel.send(m, obs, new Outcome.Aborted());
            } else {
                Channel.send(m, tm, new ToTm.Prepared(i));
                ToRm decision = Channel.recv(m, ToRm.class);
                if (decision instanceof ToRm.Commit) {
                    Channel.send(m, tm, new ToTm.Committed(i));
                    Channel.send(m, obs, new Outcome.Committed());
                } else {
                    Channel.send(m, tm, new ToTm.Aborted(i));
                    Channel.send(m, obs, new Outcome.Aborted());
                }
            }
        }
    }

    /** transaction_manager tm rms; ignore-abort-vote counts ABORTED votes as PREPARED. */
    private static void transactionManager(
            CommunicationModel m, int n, List<Tid> rms, boolean ignoreAbortVote) {
        for (int i = 0; i < n; i++) {
            Channel.send(m, rms.get(i), new ToRm.Prepare());
        }
        List<Integer> prepared = new ArrayList<>();
        boolean allPrepared = true;
        while (prepared.size() != n) {
            ToTm msg = Channel.recv(m, ToTm.class);
            if (msg instanceof ToTm.Prepared p) {
                if (!prepared.contains(p.rm())) {
                    prepared.add(p.rm());
                }
            } else if (msg instanceof ToTm.Aborted a && ignoreAbortVote) {
                if (!prepared.contains(a.rm())) {
                    prepared.add(a.rm());
                }
            } else {
                allPrepared = false;
                break;
            }
        }
        if (allPrepared) {
            for (int i = 0; i < n; i++) {
                Channel.send(m, rms.get(i), new ToRm.Commit());
            }
            List<Integer> from = new ArrayList<>();
            while (from.size() != n) {
                ToTm msg = Channel.recv(m, ToTm.class);
                int j = msg.rm();
                if (!from.contains(j)) {
                    from.add(j);
                    if (!msg.equals(new ToTm.Committed(j))) {
                        throw new AssertionError("assert (m = \"COMMITTED\"): got " + msg);
                    }
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                Channel.send(m, rms.get(i), new ToRm.Abort());
            }
        }
    }

    @Override
    public String toString() {
        return "TwoPcTrillium(" + (isSpec ? "spec" : "impl") + "," + n + "," + modelName
                + (isSpec ? "" : "," + variant) + ")";
    }
}
