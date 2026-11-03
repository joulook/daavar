package bench.paxos_oopsla25;

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
import java.util.Optional;

/**
 * Java twin of the Rust benchmark paxos-oopsla25 (single-decree Paxos against a consensus oracle).
 *
 * <p>Argument string: {@code <spec|impl>,<L>,<fifo|bag>[,<variant>]}, L = learners (1..=3), variant
 * one of {@code correct} (default), {@code ignore-max-vote}, {@code skip-2a-check},
 * {@code half-majority}, {@code 1b-none}. {@code fifo} is P2P, {@code bag} is ASYN.
 *
 * <p>Every Rust named channel has exactly one receiving thread, so each channel is that thread's
 * mailbox here: a send to channel ("p", b) is a send to the process p{b}, and so on.
 */
public final class PaxosOopsla25 implements MpProgram {

    static final int ACCEPTORS = 3;
    static final int PROPOSERS = 2; // ballots 1..=PROPOSERS; proposer b serves client b-1

    /** Rust {@code struct Vote(Ballot, Value)}. */
    @Tag("Vote")
    public record Vote(int ballot, int value) {}

    /** Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage
            permits Propose, Msg1a, Msg1b, Msg2a, Msg2b, Decide {}

    @Tag("Propose")
    public record Propose(int value) implements Msg {}

    @Tag("Msg1a")
    public record Msg1a(int ballot) implements Msg {}

    @Tag("Msg1b")
    public record Msg1b(int acceptor, int ballot, Optional<Vote> vote) implements Msg {}

    @Tag("Msg2a")
    public record Msg2a(int ballot, int value) implements Msg {}

    @Tag("Msg2b")
    public record Msg2b(int acceptor, int ballot, int value) implements Msg {}

    @Tag("Decide")
    public record Decide(int value) implements Msg {}

    record Faults(boolean ignoreMaxVote, boolean skip2aCheck, boolean halfMajority, boolean reportNone) {
        static Faults of(String variant) {
            return switch (variant) {
                case "correct" -> new Faults(false, false, false, false);
                case "ignore-max-vote" -> new Faults(true, false, false, false);
                case "skip-2a-check" -> new Faults(false, true, false, false);
                case "half-majority" -> new Faults(false, false, true, false);
                case "1b-none" -> new Faults(false, false, false, true);
                default -> throw new IllegalArgumentException("unknown variant " + variant
                        + "; one of correct, ignore-max-vote, skip-2a-check, half-majority, 1b-none");
            };
        }
    }

    private final boolean spec;
    private final int l;
    private final CommunicationModel model;
    private final Faults f;

    /**
     * @param args {@code <spec|impl>, <L>, <fifo|bag> [, <variant>]}
     */
    public PaxosOopsla25(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<L>,<fifo|bag>[,<variant>]");
        }
        this.spec = switch (args[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException("first argument must be spec or impl: " + args[0]);
        };
        this.l = Integer.parseInt(args[1]);
        if (l < 1 || l > 3) {
            throw new IllegalArgumentException("L must be 1..=3");
        }
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        };
        this.f = Faults.of(args.length > 3 ? args[3] : "correct");
    }

    // ---- MpProgram ----

    @Override
    public String programName() {
        return "paxos-oopsla25";
    }

    @Override
    public String size() {
        return Integer.toString(l);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (spec) {
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
        List<String> v = new ArrayList<>(List.of("c0", "c1"));
        for (int j = 0; j < l; j++) {
            v.add("l" + j);
        }
        return v;
    }

    // ---- main ----

    /**
     * The process ids of one execution. Processes address processes spawned after them (the oracle
     * and the proposers send to learners, proposers to acceptors), so every id is fixed before any
     * process is spawned: the k-th process main spawns gets task id main + 1 + k (MpProgram's
     * contract); {@link #spawn} checks each prediction against the handle and fails loudly otherwise.
     */
    private static final class Ids {
        final long base;
        final List<String> roles;

        Ids(long base, List<String> roles) {
            this.base = base;
            this.roles = roles;
        }

        Tid of(String role) {
            int k = roles.indexOf(role);
            if (k < 0) {
                throw new IllegalStateException("no process " + role);
            }
            return Tid.of(base + 1 + k, role);
        }
    }

    private static MpProcess.Handle spawn(Ids ids, String role, Runnable body) {
        MpProcess.Handle h = MpProcess.spawn(role, body);
        if (!h.tid().equals(ids.of(role))) {
            throw new IllegalStateException("process " + role + " got " + h.tid() + ", expected " + ids.of(role));
        }
        return h;
    }

    @Override
    public void run() {
        Ids ids = new Ids(MpProcess.self().raw(), rolesInSpawnOrder());
        if (spec) {
            runSpec(ids);
        } else {
            runImpl(ids);
        }
    }

    // ---- shared roles ----

    private void learner(int j) {
        MpMessage m = Channel.recv(model);
        if (!(m instanceof Decide)) {
            throw new IllegalStateException("unreachable: learner l" + j + " got " + m);
        }
    }

    /** Client i proposes value i + 1 to {@code to}. */
    private void client(int i, Tid to) {
        Channel.send(model, to, new Propose(i + 1));
    }

    // ---- spec ----

    /**
     * The consensus oracle: reads each proposal non-blocking, then decides one value read, or
     * nothing, and tells every learner the same value.
     */
    private void oracle(Ids ids) {
        List<Integer> vals = new ArrayList<>();
        for (int k = 0; k < PROPOSERS; k++) {
            Optional<MpMessage> m = Channel.tryRecv(model);
            if (m.isPresent() && m.get() instanceof Propose p) {
                vals.add(p.value());
            }
        }
        if (!vals.isEmpty() && Nondet.chooseBoolean()) {
            int v = vals.get(Nondet.chooseIndex(vals.size()));
            for (int j = 0; j < l; j++) {
                Channel.send(model, ids.of("l" + j), new Decide(v));
            }
        }
    }

    private void runSpec(Ids ids) {
        MpProcess.Handle o = spawn(ids, "oracle", () -> oracle(ids));
        List<MpProcess.Handle> ls = new ArrayList<>();
        for (int j = 0; j < l; j++) {
            int jj = j;
            ls.add(spawn(ids, "l" + j, () -> learner(jj)));
        }
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            int ii = i;
            cs.add(spawn(ids, "c" + i, () -> client(ii, ids.of("oracle"))));
        }
        for (MpProcess.Handle h : cs) {
            h.await();
        }
        for (MpProcess.Handle h : ls) {
            h.await();
        }
        o.await();
    }

    // ---- impl ----

    private void proposer(Ids ids, int b) {
        int majority = f.halfMajority() ? ACCEPTORS / 2 : ACCEPTORS / 2 + 1;
        MpMessage first = Channel.recv(model);
        if (!(first instanceof Propose prop)) {
            throw new IllegalStateException("unreachable: proposer p" + b + " got " + first);
        }
        int mine = prop.value();
        for (int a = 0; a < ACCEPTORS; a++) {
            Channel.send(model, ids.of("a" + a), new Msg1a(b));
        }
        List<Integer> q = new ArrayList<>();
        Vote maxVote = null; // Rust Option<Vote>; local only, never sent as null
        for (int k = 0; k < ACCEPTORS; k++) {
            if (q.size() >= majority) {
                break;
            }
            if (Channel.recv(model) instanceof Msg1b m1b) {
                if (m1b.ballot() == b && !q.contains(m1b.acceptor())) {
                    q.add(m1b.acceptor());
                    if (m1b.vote().isPresent()) {
                        Vote vt = m1b.vote().get();
                        if (maxVote == null || vt.ballot() > maxVote.ballot()) {
                            maxVote = new Vote(vt.ballot(), vt.value());
                        }
                    }
                }
            }
        }
        int v = (maxVote != null && !f.ignoreMaxVote()) ? maxVote.value() : mine;
        for (int a = 0; a < ACCEPTORS; a++) {
            Channel.send(model, ids.of("a" + a), new Msg2a(b, v));
        }
        List<Integer> acc = new ArrayList<>();
        for (int k = 0; k < ACCEPTORS; k++) {
            if (acc.size() >= majority) {
                break;
            }
            if (Channel.recv(model) instanceof Msg2b m2b) {
                if (m2b.ballot() == b && !acc.contains(m2b.acceptor())) {
                    acc.add(m2b.acceptor());
                }
            }
        }
        if (acc.size() >= majority) {
            for (int j = 0; j < l; j++) {
                Channel.send(model, ids.of("l" + j), new Decide(v));
            }
        }
    }

    /** {@code run_acceptor}, bounded to 2 prepares + 2 accepts: 4 blocking receives. */
    private void acceptor(Ids ids, int me) {
        Integer maxBal = null; // Rust Option<Ballot>
        Optional<Vote> maxVote = Optional.empty();
        for (int k = 0; k < 2 * PROPOSERS; k++) {
            MpMessage m = Channel.recv(model);
            if (m instanceof Msg1a m1a) {
                int b = m1a.ballot();
                if (maxBal == null || b > maxBal) {
                    maxBal = b;
                    Optional<Vote> vote = f.reportNone() ? Optional.empty() : maxVote;
                    Channel.send(model, ids.of("p" + b), new Msg1b(me, b, vote));
                }
            } else if (m instanceof Msg2a m2a) {
                int b = m2a.ballot();
                int v = m2a.value();
                if (f.skip2aCheck() || maxBal == null || b >= maxBal) {
                    maxBal = b;
                    maxVote = Optional.of(new Vote(b, v));
                    Channel.send(model, ids.of("p" + b), new Msg2b(me, b, v));
                }
            } else {
                throw new IllegalStateException("unreachable: acceptor a" + me + " got " + m);
            }
        }
    }

    private void runImpl(Ids ids) {
        List<MpProcess.Handle> hs = new ArrayList<>();
        for (int b = 1; b <= PROPOSERS; b++) {
            int bb = b;
            hs.add(spawn(ids, "p" + b, () -> proposer(ids, bb)));
        }
        for (int a = 0; a < ACCEPTORS; a++) {
            int aa = a;
            hs.add(spawn(ids, "a" + a, () -> acceptor(ids, aa)));
        }
        List<MpProcess.Handle> ls = new ArrayList<>();
        for (int j = 0; j < l; j++) {
            int jj = j;
            ls.add(spawn(ids, "l" + j, () -> learner(jj)));
        }
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            int ii = i;
            cs.add(spawn(ids, "c" + i, () -> client(ii, ids.of("p" + (ii + 1)))));
        }
        for (MpProcess.Handle h : cs) {
            h.await();
        }
        for (MpProcess.Handle h : ls) {
            h.await();
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    @Override
    public String toString() {
        return "paxos-oopsla25 " + (spec ? "spec" : "impl") + " L=" + l + " " + model
                + (spec ? "" : " " + f);
    }
}
