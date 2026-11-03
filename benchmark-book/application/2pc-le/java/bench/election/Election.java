package bench.election;

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
 * TraceForge's leader election + two-phase commit (all-to-all and ring): the Java twin of
 * {@code rust/src/main.rs} (same threads, spawn order, messages, choices and variants).
 *
 * <p>Arguments: {@code spec,<n>,<fifo|bag>[,<variant>]} or {@code impl,<n>,<fifo|bag>,<variant>},
 * variant one of {@code leader-conf}, {@code leader-split}, {@code leader-silent},
 * {@code ring-conf}, {@code ring-split} (Rust's two words {@code leader conf} etc. joined by
 * {@code -}). Visible: {@code p0..p(n-1)}.
 *
 * <p>The one departure of form: Rust's {@code main} sends each elector its {@code Setup} (or the
 * oracle its bare {@code Setup}) after spawning; a JMC main may perform no message-passing event,
 * so an invisible thread {@code init}, spawned last, sends the same messages in the same order.
 */
public final class Election implements MpProgram {

    /** {@code struct Setup { me, electors, participants }}. */
    @Tag("Setup")
    public record Setup(int me, List<Tid> electors, List<Tid> participants) implements MpMessage {}

    /** {@code EIn}: what an all-to-all elector receives. */
    public sealed interface EIn extends MpMessage permits ESetup, Elect {}

    /** {@code EIn::Setup(Setup)}. */
    @Tag("Setup")
    public record ESetup(Setup s) implements EIn {}

    /** {@code EIn::Elect(usize)}. */
    @Tag("Elect")
    public record Elect(int j) implements EIn {}

    /** {@code PIn}: everything a participant receives. */
    public sealed interface PIn extends MpMessage permits BeLeader, BeFollower, Vote, Decision {}

    /** {@code PIn::BeLeader(Vec<ThreadId>)}. */
    @Tag("BeLeader")
    public record BeLeader(List<Tid> peers) implements PIn {}

    /** {@code PIn::BeFollower(ThreadId)}. */
    @Tag("BeFollower")
    public record BeFollower(Tid leader) implements PIn {}

    /** {@code PIn::Vote(bool)}. */
    @Tag("Vote")
    public record Vote(boolean v) implements PIn {}

    /** {@code PIn::Decision(bool)}. */
    @Tag("Decision")
    public record Decision(boolean d) implements PIn {}

    /** {@code RIn}: what a ring elector receives. */
    public sealed interface RIn extends MpMessage permits RSetup, Tok, Ann {}

    /** {@code RIn::Setup(Setup)}. */
    @Tag("Setup")
    public record RSetup(Setup s) implements RIn {}

    /** {@code RIn::Tok(usize)}. */
    @Tag("Tok")
    public record Tok(int max) implements RIn {}

    /** {@code RIn::Ann(usize)}. */
    @Tag("Ann")
    public record Ann(int leader) implements RIn {}

    enum Variant {
        LEADER_CONF("leader-conf"),
        LEADER_SPLIT("leader-split"),
        LEADER_SILENT("leader-silent"),
        RING_CONF("ring-conf"),
        RING_SPLIT("ring-split");

        final String label;

        Variant(String label) {
            this.label = label;
        }

        static Variant byLabel(String s) {
            for (Variant v : values()) {
                if (v.label.equals(s.trim())) {
                    return v;
                }
            }
            throw new IllegalArgumentException("unknown variant " + s
                    + "; one of leader-conf, leader-split, leader-silent, ring-conf, ring-split");
        }

        boolean ring() {
            return this == RING_CONF || this == RING_SPLIT;
        }
    }

    private final boolean impl;
    private final int n;
    private final CommunicationModel m;
    private final Variant variant;

    /**
     * @param args side, n, model, variant (optional on the spec side)
     */
    public Election(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "Election takes spec,<n>,<fifo|bag>[,<variant>] or impl,<n>,<fifo|bag>,<variant>");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.n = Integer.parseInt(args[1].trim());
        this.m = Support.model(args[2]);
        if (impl && args.length != 4) {
            throw new IllegalArgumentException("the impl side needs a variant");
        }
        this.variant = args.length == 4 ? Variant.byLabel(args[3]) : Variant.LEADER_CONF;
        if (n < 1) {
            throw new IllegalArgumentException("n must be at least 1");
        }
    }

    @Override
    public String programName() {
        return "election-" + (impl ? variant.label : "spec");
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
        List<String> r = new ArrayList<>(visibleRoles());
        if (impl) {
            for (int i = 0; i < n; i++) {
                r.add("e" + i);
            }
        } else {
            r.add("oracle");
        }
        r.add("init");
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

    /** Participants first (F41), then the electors or the oracle, then {@code init}. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        List<Tid> ps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ps.add(Tid.of(ts.add("p" + i, this::participant).getTaskId()));
        }
        List<Tid> participants = List.copyOf(ps);
        if (impl) {
            List<Tid> es = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Runnable body = switch (variant) {
                    case LEADER_CONF -> () -> elector(false, false);
                    case LEADER_SPLIT -> () -> elector(true, false);
                    case LEADER_SILENT -> () -> elector(false, true);
                    case RING_CONF -> () -> ringElector(false);
                    case RING_SPLIT -> () -> ringElector(true);
                };
                es.add(Tid.of(ts.add("e" + i, body).getTaskId()));
            }
            List<Tid> electors = List.copyOf(es);
            boolean ring = variant.ring();
            ts.add("init", () -> {
                for (int i = 0; i < n; i++) {
                    Setup s = new Setup(i, electors, participants);
                    Channel.send(m, electors.get(i), ring ? new RSetup(s) : new ESetup(s));
                }
            });
        } else {
            Tid oracle = Tid.of(ts.add("oracle", this::oracle).getTaskId());
            ts.add("init", () -> Channel.send(m, oracle, new Setup(0, List.of(), participants)));
        }
        ts.startAndAwaitAll();
    }

    /** {@code le_participant(n)}, shared by both sides. */
    private void participant() {
        List<Tid> peers = null;
        boolean voted = false;
        int votes = 0;
        boolean allYes = true;
        for (int i = 0; i < n; i++) {
            PIn x = Channel.recv(m, PIn.class);
            if (x instanceof BeLeader b) {
                peers = b.peers();
            } else if (x instanceof BeFollower f) {
                if (!voted) {
                    voted = true;
                    Channel.send(m, f.leader(), new Vote(true));
                }
            } else if (x instanceof Vote v) {
                votes++;
                allYes &= v.v();
            } else {
                return;
            }
            if (peers != null && votes == peers.size()) {
                for (Tid p : peers) {
                    Channel.send(m, p, new Decision(allYes));
                }
                return;
            }
        }
    }

    /** {@code le_announce(i, leader, ps)}. */
    private void announce(int i, int leader, List<Tid> ps) {
        if (i == leader) {
            List<Tid> peers = new ArrayList<>();
            for (int j = 0; j < ps.size(); j++) {
                if (j != leader) {
                    peers.add(ps.get(j));
                }
            }
            Channel.send(m, ps.get(i), new BeLeader(List.copyOf(peers)));
        } else {
            Channel.send(m, ps.get(i), new BeFollower(ps.get(leader)));
        }
    }

    /** {@code le_elector_with(buggy, silent_winner)}. */
    private void elector(boolean buggy, boolean silentWinner) {
        Setup setup = null;
        List<Integer> heard = new ArrayList<>();
        boolean announced = false;
        while (true) {
            EIn x = Channel.recv(m, EIn.class);
            if (x instanceof ESetup s) {
                setup = s.s();
            } else {
                heard.add(((Elect) x).j());
            }
            if (setup == null) {
                continue;
            }
            if (!announced) {
                announced = true;
                for (int i = 0; i < setup.electors().size(); i++) {
                    if (i != setup.me()) {
                        Channel.send(m, setup.electors().get(i), new Elect(setup.me()));
                    }
                }
            }
            if (heard.size() == setup.electors().size() - 1) {
                Integer best = buggy ? null : setup.me();
                for (int j : heard) {
                    best = best == null ? j : Math.max(best, j);
                }
                int leader = best == null ? setup.me() : best;
                if (silentWinner && leader == setup.me()) {
                    return;
                }
                announce(setup.me(), leader, setup.participants());
                return;
            }
        }
    }

    /** {@code le_oracle}: a ladder of boolean choices picks the leader. */
    private void oracle() {
        Setup s = Channel.recv(m, Setup.class);
        int k = s.participants().size();
        int leader = k - 1;
        for (int i = 0; i < k - 1; i++) {
            if (Nondet.chooseBoolean()) {
                leader = i;
                break;
            }
        }
        for (int i = 0; i < k; i++) {
            announce(i, leader, s.participants());
        }
    }

    /** {@code ring_elector(buggy)}. */
    private void ringElector(boolean buggy) {
        Setup setup = null;
        Integer tok = null;
        Integer ann = null;
        boolean started = false;
        for (int round = 0; round < 3; round++) {
            RIn x = Channel.recv(m, RIn.class);
            if (x instanceof RSetup s) {
                setup = s.s();
            } else if (x instanceof Tok t) {
                tok = t.max();
            } else {
                ann = ((Ann) x).leader();
            }
            if (setup == null) {
                continue;
            }
            int size = setup.electors().size();
            int me = setup.me();
            Tid succ = setup.electors().get((me + 1) % size);
            if (!started) {
                started = true;
                if (me == 0) {
                    Channel.send(m, succ, new Tok(0));
                }
            }
            if (tok != null) {
                int t = tok;
                tok = null;
                if (buggy) {
                    announce(me, t, setup.participants());
                    if (me != 0) {
                        Channel.send(m, succ, new Tok(Math.max(t, me)));
                    }
                    return;
                }
                if (me == 0) {
                    announce(0, t, setup.participants());
                    if (size > 1) {
                        Channel.send(m, succ, new Ann(t));
                    }
                    return;
                }
                Channel.send(m, succ, new Tok(Math.max(t, me)));
            }
            if (ann != null) {
                int l = ann;
                ann = null;
                announce(me, l, setup.participants());
                if (me + 1 < size) {
                    Channel.send(m, succ, new Ann(l));
                }
                return;
            }
        }
    }
}
