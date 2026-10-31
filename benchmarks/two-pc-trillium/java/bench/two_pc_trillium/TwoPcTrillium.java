package bench.two_pc_trillium;

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
 * Benchmark two-pc-trillium (candidate N7): the AnerisLang two-phase commit
 * ({@code two_phase_code.ml}) against the Coq {@code TC_model} ({@code tc_model.v}); a
 * line-for-line port of {@code rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,N[,fifo|bag[,correct|ignore-abort-vote]]}; default model
 * {@code bag} (asyn), default variant {@code correct}. Visible roles {@code obs0..obs(N-1)} (the
 * observer of RM {@code i}'s final state); invisible {@code tc} (spec), {@code tm} and
 * {@code rm0..rm(N-1)} (impl).
 */
public final class TwoPcTrillium implements MpProgram {

    /** TM to RM: {@code "PREPARE" | "COMMIT" | "ABORT"} (Rust {@code enum ToRm}). */
    sealed interface ToRm extends MpMessage {
        @Tag("Prepare")
        record Prepare() implements ToRm {}

        @Tag("Commit")
        record Commit() implements ToRm {}

        @Tag("Abort")
        record Abort() implements ToRm {}
    }

    /**
     * RM to TM: {@code "PREPARED" | "ABORTED" | "COMMITTED"}, with the sender index (Rust
     * {@code enum ToTm}).
     */
    sealed interface ToTm extends MpMessage {
        /** @return the sender RM's index */
        int from();

        @Tag("Prepared")
        record Prepared(int from) implements ToTm {}

        @Tag("Aborted")
        record Aborted(int from) implements ToTm {}

        @Tag("Committed")
        record Committed(int from) implements ToTm {}
    }

    /** RM {@code i}'s final {@code TC_model} state, to its observer (Rust {@code enum Outcome}). */
    sealed interface Outcome extends MpMessage {
        @Tag("Committed")
        record Committed() implements Outcome {}

        @Tag("Aborted")
        record Aborted() implements Outcome {}
    }

    /** @return {@code Committed} if {@code committed}, else {@code Aborted} */
    private static Outcome outcome(boolean committed) {
        return committed ? new Outcome.Committed() : new Outcome.Aborted();
    }

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean ignoreAbortVote;

    /**
     * @param args side, N, model, variant
     */
    public TwoPcTrillium(String... args) {
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.ignoreAbortVote = switch (v) {
            case "correct" -> false;
            case "ignore-abort-vote" -> true;
            default -> throw new IllegalArgumentException("variant must be correct or ignore-abort-vote");
        };
    }

    @Override
    public String programName() {
        return "two-pc-trillium-" + (impl ? "impl-" + (ignoreAbortVote ? "ignore-abort-vote" : "correct") : "spec");
    }

    @Override
    public String size() {
        return "N" + n;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>(visibleRoles());
        if (impl) {
            roles.add("tm");
            for (int i = 0; i < n; i++) {
                roles.add("rm" + i);
            }
        } else {
            roles.add("tc");
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("obs" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        Support.Procs procs = new Support.Procs();
        Tid[] obs = new Tid[n];
        for (int i = 0; i < n; i++) {
            obs[i] = procs.add("obs" + i, () -> Channel.recv(m, Outcome.class));
        }
        if (impl) {
            Tid[] rms = new Tid[n];
            Tid tm = procs.add("tm", () -> transactionManager(rms));
            for (int i = 0; i < n; i++) {
                final int me = i;
                rms[i] = procs.add("rm" + i, () -> resourceManager(me, tm, obs[me]));
            }
        } else {
            procs.add("tc", () -> tc(obs));
        }
        procs.startAndAwait();
    }

    /** The {@code TC_model} oracle: RM coins, commit iff none aborted, one outcome to all. */
    private void tc(Tid[] obs) {
        boolean allPrepared = true;
        for (int i = 0; i < n; i++) {
            if (Nondet.chooseBoolean()) {
                allPrepared = false;
            }
        }
        for (Tid o : obs) {
            Channel.send(m, o, outcome(allPrepared));
        }
    }

    /** {@code resource_manager rm tm}, plus the report of its final state. */
    private void resourceManager(int i, Tid tm, Tid obs) {
        ToRm value = Channel.recv(m, ToRm.class);
        if (value instanceof ToRm.Abort) {
            Channel.send(m, tm, new ToTm.Aborted(i));
            Channel.send(m, obs, outcome(false));
        } else {
            boolean localAbort = Nondet.chooseBoolean();
            if (localAbort) {
                Channel.send(m, tm, new ToTm.Aborted(i));
                Channel.send(m, obs, outcome(false));
            } else {
                Channel.send(m, tm, new ToTm.Prepared(i));
                ToRm decision = Channel.recv(m, ToRm.class);
                if (decision instanceof ToRm.Commit) {
                    Channel.send(m, tm, new ToTm.Committed(i));
                    Channel.send(m, obs, outcome(true));
                } else {
                    Channel.send(m, tm, new ToTm.Aborted(i));
                    Channel.send(m, obs, outcome(false));
                }
            }
        }
    }

    /** {@code transaction_manager tm rms}; {@code ignore-abort-vote} counts ABORTED as prepared. */
    private void transactionManager(Tid[] rms) {
        for (Tid r : rms) {
            Channel.send(m, r, new ToRm.Prepare());
        }
        List<Integer> prepared = new ArrayList<>();
        boolean allPrepared = true;
        while (prepared.size() != n) {
            ToTm msg = Channel.recv(m, ToTm.class);
            if (msg instanceof ToTm.Prepared || (ignoreAbortVote && msg instanceof ToTm.Aborted)) {
                if (!prepared.contains(msg.from())) {
                    prepared.add(msg.from());
                }
            } else {
                allPrepared = false;
                break;
            }
        }
        if (allPrepared) {
            for (Tid r : rms) {
                Channel.send(m, r, new ToRm.Commit());
            }
            List<Integer> from = new ArrayList<>();
            while (from.size() != n) {
                ToTm msg = Channel.recv(m, ToTm.class);
                if (!from.contains(msg.from())) {
                    from.add(msg.from());
                    if (!(msg instanceof ToTm.Committed)) {
                        throw new AssertionError("assert (m = \"COMMITTED\")");
                    }
                }
            }
        } else {
            for (Tid r : rms) {
                Channel.send(m, r, new ToRm.Abort());
            }
        }
    }
}
