package bench.two_pc_trillium;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

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

    /** TM to RM: {@code "PREPARE" | "COMMIT" | "ABORT"}. */
    enum Kind { PREPARE, COMMIT, ABORT }

    /** A TM-to-RM message. */
    record ToRm(Kind kind) implements MpMessage {}

    /** RM to TM: {@code "PREPARED" | "ABORTED" | "COMMITTED"}, with the sender index. */
    enum Resp { PREPARED, ABORTED, COMMITTED }

    /** An RM-to-TM message. */
    record ToTm(Resp resp, int from) implements MpMessage {}

    /** RM {@code i}'s final {@code TC_model} state, to its observer. */
    record Outcome(boolean committed) implements MpMessage {}

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
            Channel.send(m, o, new Outcome(allPrepared));
        }
    }

    /** {@code resource_manager rm tm}, plus the report of its final state. */
    private void resourceManager(int i, Tid tm, Tid obs) {
        ToRm value = Channel.recv(m, ToRm.class);
        if (value.kind() == Kind.ABORT) {
            Channel.send(m, tm, new ToTm(Resp.ABORTED, i));
            Channel.send(m, obs, new Outcome(false));
        } else {
            boolean localAbort = Nondet.chooseBoolean();
            if (localAbort) {
                Channel.send(m, tm, new ToTm(Resp.ABORTED, i));
                Channel.send(m, obs, new Outcome(false));
            } else {
                Channel.send(m, tm, new ToTm(Resp.PREPARED, i));
                ToRm decision = Channel.recv(m, ToRm.class);
                if (decision.kind() == Kind.COMMIT) {
                    Channel.send(m, tm, new ToTm(Resp.COMMITTED, i));
                    Channel.send(m, obs, new Outcome(true));
                } else {
                    Channel.send(m, tm, new ToTm(Resp.ABORTED, i));
                    Channel.send(m, obs, new Outcome(false));
                }
            }
        }
    }

    /** {@code transaction_manager tm rms}; {@code ignore-abort-vote} counts ABORTED as prepared. */
    private void transactionManager(Tid[] rms) {
        for (Tid r : rms) {
            Channel.send(m, r, new ToRm(Kind.PREPARE));
        }
        List<Integer> prepared = new ArrayList<>();
        boolean allPrepared = true;
        while (prepared.size() != n) {
            ToTm msg = Channel.recv(m, ToTm.class);
            if (msg.resp() == Resp.PREPARED || (ignoreAbortVote && msg.resp() == Resp.ABORTED)) {
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
                Channel.send(m, r, new ToRm(Kind.COMMIT));
            }
            List<Integer> from = new ArrayList<>();
            while (from.size() != n) {
                ToTm msg = Channel.recv(m, ToTm.class);
                if (!from.contains(msg.from())) {
                    from.add(msg.from());
                    if (msg.resp() != Resp.COMMITTED) {
                        throw new AssertionError("assert (m = \"COMMITTED\")");
                    }
                }
            }
        } else {
            for (Tid r : rms) {
                Channel.send(m, r, new ToRm(Kind.ABORT));
            }
        }
    }
}
