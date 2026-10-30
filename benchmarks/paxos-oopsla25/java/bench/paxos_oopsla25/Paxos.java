package bench.paxos_oopsla25;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Single-decree Paxos of OOPSLA 2025 (§5.3, {@code ex05_paxos.rs}) against a consensus oracle,
 * the Java twin of {@code rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,<L>[,<bag|fifo>[,<variant>]]}; {@code L} learners (1..3), two
 * clients {@code c0}, {@code c1}, two proposers {@code p1}, {@code p2} (ballots 1, 2), three
 * acceptors {@code a0..a2}. Variants: {@code correct}, {@code ignore-max-vote},
 * {@code skip-2a-check}, {@code half-majority}, {@code 1b-none} (ignored on the spec side). Every
 * receive is untyped, as in Rust.
 */
public final class Paxos implements MpProgram {

    /** A vote {@code (Ballot, Value)} (Rust tuple). */
    @Tag("Vote")
    public record Vote(int ballot, int value) {}

    /** client to proposer (spec: to the oracle). */
    @Tag("Propose")
    public record Propose(int v) implements MpMessage {}

    /** {@code Msg1a(Ballot)}. */
    @Tag("Msg1a")
    public record Msg1a(int b) implements MpMessage {}

    /** {@code Msg1b(Acceptor, Ballot, Option<(Ballot, Value)>)}; {@code vote == null} is None. */
    @Tag("Msg1b")
    public record Msg1b(int a, int b, Vote vote) implements MpMessage {}

    /** {@code Msg2a(Ballot, Value)}. */
    @Tag("Msg2a")
    public record Msg2a(int b, int v) implements MpMessage {}

    /** {@code Msg2b(Acceptor, Ballot, Value)}. */
    @Tag("Msg2b")
    public record Msg2b(int a, int b, int v) implements MpMessage {}

    /** proposer (spec: oracle) to learner. */
    @Tag("Decide")
    public record Decide(int v) implements MpMessage {}

    private static final int ACCEPTORS = 3;
    private static final int PROPOSERS = 2;

    private final boolean impl;
    private final int l;
    private final CommunicationModel m;
    private final String variant;
    private final boolean ignoreMaxVote;
    private final boolean skip2aCheck;
    private final boolean halfMajority;
    private final boolean reportNone;

    /**
     * @param args side, L, model, variant
     */
    public Paxos(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Paxos takes <spec|impl>,<L>[,<bag|fifo>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.l = Integer.parseInt(args[1].trim());
        if (l < 1 || l > 3) {
            throw new IllegalArgumentException("L must be 1..3");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        this.variant = args.length > 3 ? args[3].trim() : "correct";
        if (!List.of("correct", "ignore-max-vote", "skip-2a-check", "half-majority", "1b-none")
                .contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
        this.ignoreMaxVote = variant.equals("ignore-max-vote");
        this.skip2aCheck = variant.equals("skip-2a-check");
        this.halfMajority = variant.equals("half-majority");
        this.reportNone = variant.equals("1b-none");
    }

    @Override
    public String programName() {
        return "paxos-oopsla25-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "a" + ACCEPTORS + "p" + PROPOSERS + "l" + l;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (!impl) {
            r.add("oracle");
        } else {
            for (int b = 1; b <= PROPOSERS; b++) {
                r.add("p" + b);
            }
            for (int a = 0; a < ACCEPTORS; a++) {
                r.add("a" + a);
            }
        }
        for (int j = 0; j < l; j++) {
            r.add("l" + j);
        }
        r.add("c0");
        r.add("c1");
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>(List.of("c0", "c1"));
        for (int j = 0; j < l; j++) {
            r.add("l" + j);
        }
        return r;
    }

    @Override
    public void run() {
        Support.Procs procs = new Support.Procs();
        Tid[] learners = new Tid[l];
        Tid[] proposers = new Tid[PROPOSERS + 1]; // index = ballot
        Tid[] acceptors = new Tid[ACCEPTORS];
        Tid[] front = new Tid[2]; // client i's destination
        if (!impl) {
            Tid o = procs.add("oracle", () -> oracle(learners));
            front[0] = o;
            front[1] = o;
        } else {
            for (int b = 1; b <= PROPOSERS; b++) {
                final int ballot = b;
                proposers[b] = procs.add("p" + b, () -> proposer(ballot, acceptors, learners));
            }
            for (int a = 0; a < ACCEPTORS; a++) {
                final int me = a;
                acceptors[a] = procs.add("a" + a, () -> acceptor(me, proposers));
            }
            front[0] = proposers[1];
            front[1] = proposers[2];
        }
        for (int j = 0; j < l; j++) {
            learners[j] = procs.add("l" + j, () -> {
                Decide d = (Decide) Channel.recv(m);
            });
        }
        for (int i = 0; i < 2; i++) {
            final int me = i;
            procs.add("c" + i, () -> Channel.send(m, front[me], new Propose(me + 1)));
        }
        procs.startAndAwait();
    }

    /** The consensus oracle: non-blocking reads, then one value read, or nothing, to all. */
    private void oracle(Tid[] learners) {
        List<Integer> vals = new ArrayList<>();
        for (int n = 0; n < PROPOSERS; n++) {
            Optional<MpMessage> r = Channel.tryRecv(m);
            if (r.isPresent() && r.get() instanceof Propose p) {
                vals.add(p.v());
            }
        }
        if (!vals.isEmpty() && Nondet.chooseBoolean()) {
            int v = vals.get(Nondet.chooseIndex(vals.size()));
            for (Tid t : learners) {
                Channel.send(m, t, new Decide(v));
            }
        }
    }

    /** {@code run_leader_ballot} plus the announcement (see the Rust {@code proposer}). */
    private void proposer(int b, Tid[] acceptors, Tid[] learners) {
        int majority = halfMajority ? ACCEPTORS / 2 : ACCEPTORS / 2 + 1;
        int mine = ((Propose) Channel.recv(m)).v();
        for (Tid a : acceptors) {
            Channel.send(m, a, new Msg1a(b));
        }
        List<Integer> q = new ArrayList<>();
        Vote maxVote = null;
        for (int n = 0; n < ACCEPTORS; n++) {
            if (q.size() >= majority) {
                break;
            }
            MpMessage r = Channel.recv(m);
            if (r instanceof Msg1b p && p.b() == b && !q.contains(p.a())) {
                q.add(p.a());
                if (p.vote() != null && (maxVote == null || p.vote().ballot() > maxVote.ballot())) {
                    maxVote = p.vote();
                }
            }
        }
        int v = (maxVote != null && !ignoreMaxVote) ? maxVote.value() : mine;
        for (Tid a : acceptors) {
            Channel.send(m, a, new Msg2a(b, v));
        }
        List<Integer> acc = new ArrayList<>();
        for (int n = 0; n < ACCEPTORS; n++) {
            if (acc.size() >= majority) {
                break;
            }
            MpMessage r = Channel.recv(m);
            if (r instanceof Msg2b p && p.b() == b && !acc.contains(p.a())) {
                acc.add(p.a());
            }
        }
        if (acc.size() >= majority) {
            for (Tid t : learners) {
                Channel.send(m, t, new Decide(v));
            }
        }
    }

    /** {@code run_acceptor}, 4 blocking receives. */
    private void acceptor(int me, Tid[] proposers) {
        Integer maxBal = null;
        Vote maxVote = null;
        for (int n = 0; n < 2 * PROPOSERS; n++) {
            MpMessage r = Channel.recv(m);
            if (r instanceof Msg1a p) {
                if (maxBal == null || p.b() > maxBal) {
                    maxBal = p.b();
                    Channel.send(m, proposers[p.b()], new Msg1b(me, p.b(), reportNone ? null : maxVote));
                }
            } else if (r instanceof Msg2a p) {
                if (skip2aCheck || maxBal == null || p.b() >= maxBal) {
                    maxBal = p.b();
                    maxVote = new Vote(p.b(), p.v());
                    Channel.send(m, proposers[p.b()], new Msg2b(me, p.b(), p.v()));
                }
            } else {
                throw new IllegalStateException("acceptor received " + r);
            }
        }
    }
}
