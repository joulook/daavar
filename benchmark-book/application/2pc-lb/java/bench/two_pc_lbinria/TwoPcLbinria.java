package bench.two_pc_lbinria;

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
 * Benchmark two-pc-lbinria (candidate N1): the Java two-phase commit of Cirstea et al.
 * ({@code lbinria-twophase/}, {@code TransactionManager.java}, {@code ResourceManager.java}, spec
 * {@code spec/TwoPhase.tla}); a line-for-line port of {@code rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,N[,fifo|bag[,correct|counting-bug]]}; {@code N} resource
 * managers, resend bound {@code R = 2}; default model {@code bag} (asyn), default variant
 * {@code correct}. Visible roles {@code rm0..rm(N-1)}; invisible {@code tm} (both sides) and
 * {@code p0..p(N-1)} (impl: the retransmitting transport of each RM).
 */
public final class TwoPcLbinria implements MpProgram {

    /** Resends per RM: the proxy forwards {@code Prepared} {@code 1..=R} times. */
    static final int R = 2;

    /** {@code TwoPhaseMessage.Prepared} with the sender index (Rust {@code struct Prepared(usize)}). */
    @Tag("Prepared")
    record Prepared(int from) implements MpMessage {}

    /** {@code TwoPhaseMessage.Commit} / {@code Abort}, TM to RM (Rust {@code enum Decision}). */
    sealed interface Decision extends MpMessage {
        static Decision of(boolean commit) {
            return commit ? new Commit() : new Abort();
        }

        @Tag("Commit")
        record Commit() implements Decision {}

        @Tag("Abort")
        record Abort() implements Decision {}
    }

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final boolean countingBug;

    /**
     * @param args side, N, model, variant
     */
    public TwoPcLbinria(String... args) {
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.countingBug = switch (v) {
            case "correct" -> false;
            case "counting-bug" -> true;
            default -> throw new IllegalArgumentException("variant must be correct or counting-bug");
        };
    }

    @Override
    public String programName() {
        return "two-pc-lbinria-" + (impl ? "impl-" + (countingBug ? "counting-bug" : "correct") : "spec");
    }

    @Override
    public String size() {
        return "N" + n + "R" + R;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("tm");
        if (impl) {
            for (int i = 0; i < n; i++) {
                roles.add("p" + i);
            }
        }
        for (int i = 0; i < n; i++) {
            roles.add("rm" + i);
        }
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

    @Override
    public void run() {
        Support.Procs procs = new Support.Procs();
        Tid[] rms = new Tid[n];
        Tid tm = procs.add("tm", impl ? () -> tm(rms) : () -> specTm(rms));
        Tid[] servers = new Tid[n];
        if (impl) {
            for (int i = 0; i < n; i++) {
                servers[i] = procs.add("p" + i, () -> proxy(tm));
            }
        } else {
            for (int i = 0; i < n; i++) {
                servers[i] = tm;
            }
        }
        for (int i = 0; i < n; i++) {
            final int me = i;
            rms[i] = procs.add("rm" + i, () -> rm(me, servers[me]));
        }
        procs.startAndAwait();
    }

    private void sendAll(Tid[] rms, boolean commit) {
        for (Tid r : rms) {
            Channel.send(m, r, Decision.of(commit));
        }
    }

    /** {@code ResourceManager.run}: crash, or one {@code Prepared} and the decision. */
    private void rm(int i, Tid server) {
        if (Nondet.chooseBoolean()) {
            return;
        }
        Channel.send(m, server, new Prepared(i));
        Channel.recv(m, Decision.class);
    }

    /** The specification's TM oracle ({@code TwoPhase.tla}). */
    private void specTm(Tid[] rms) {
        for (int k = 0; k < n; k++) {
            if (Nondet.chooseBoolean()) {
                sendAll(rms, false);
                return;
            }
            Channel.recv(m, Prepared.class);
        }
        sendAll(rms, Nondet.chooseBoolean());
    }

    /** {@code TransactionManager.run}: set ({@code correct}) or list ({@code counting-bug}). */
    private void tm(Tid[] rms) {
        List<Integer> prepared = new ArrayList<>();
        for (int k = 0; k < n * R; k++) {
            if (Nondet.chooseBoolean()) {
                sendAll(rms, false);
                return;
            }
            Prepared p = Channel.recv(m, Prepared.class);
            if (countingBug || !prepared.contains(p.from())) {
                prepared.add(p.from());
            }
            if (prepared.size() >= n) {
                sendAll(rms, true);
                return;
            }
        }
    }

    /** The RM's transport: forward its one {@code Prepared} {@code 1..=R} times. */
    private void proxy(Tid tm) {
        Prepared p = Channel.recv(m, Prepared.class);
        int k = 1 + Nondet.chooseIndex(R);
        for (int j = 0; j < k; j++) {
            Channel.send(m, tm, p);
        }
    }
}
