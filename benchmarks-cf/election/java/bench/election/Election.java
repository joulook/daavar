package bench.election;

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
 * Leader election + two-phase commit ("leader" all-to-all election and "ring" election), the Java
 * twin of {@code daavar/benchmarks/election/rust/src/main.rs}, written against the Must API.
 *
 * <p>Arguments (launcher string): {@code <spec|impl>,<n>,<fifo|bag>,<leader|ring>,<conf|split|silent>}.
 * The variant may also be given as one token {@code leader-conf} / {@code "leader conf"}.
 * {@code fifo} is {@link CommunicationModel#P2P}, {@code bag} is {@link CommunicationModel#ASYN}.
 *
 * <p>Visible roles: {@code p0 .. p{n-1}}. Invisible: {@code e0 .. e{n-1}} (impl) or {@code oracle}
 * (spec), and {@code boot}, which sends the setup messages the Rust main sends (a JMC main may
 * perform no message-passing event).
 */
public final class Election implements MpProgram {

    // ------------------------------------------------------------ messages ----

    /** Rust {@code struct Setup}: sent bare to the spec's oracle, and wrapped in EIn/RIn. */
    @Tag("Setup")
    public record Setup(int me, List<Tid> electors, List<Tid> participants) implements MpMessage {
        public Setup {
            electors = List.copyOf(electors);
            participants = List.copyOf(participants);
        }
    }

    /** Rust {@code enum EIn}: what an all-to-all elector receives. */
    public sealed interface EIn extends MpMessage permits EIn.SetupE, EIn.Elect {
        @Tag("Setup")
        record SetupE(Setup setup) implements EIn {}

        @Tag("Elect")
        record Elect(int index) implements EIn {}
    }

    /** Rust {@code enum PIn}: what a participant receives. */
    public sealed interface PIn extends MpMessage
            permits PIn.BeLeader, PIn.BeFollower, PIn.Vote, PIn.Decision {
        @Tag("BeLeader")
        record BeLeader(List<Tid> peers) implements PIn {
            public BeLeader {
                peers = List.copyOf(peers);
            }
        }

        @Tag("BeFollower")
        record BeFollower(Tid leader) implements PIn {}

        @Tag("Vote")
        record Vote(boolean yes) implements PIn {}

        @Tag("Decision")
        record Decision(boolean commit) implements PIn {}
    }

    /** Rust {@code enum RIn}: what a ring elector receives. */
    public sealed interface RIn extends MpMessage permits RIn.SetupR, RIn.Tok, RIn.Ann {
        @Tag("Setup")
        record SetupR(Setup setup) implements RIn {}

        @Tag("Tok")
        record Tok(int max) implements RIn {}

        @Tag("Ann")
        record Ann(int leader) implements RIn {}
    }

    // ------------------------------------------------------------ arguments ----

    private enum Variant { LEADER_CONF, LEADER_SPLIT, LEADER_SILENT, RING_CONF, RING_SPLIT }

    private final boolean spec;
    private final int n;
    private final CommunicationModel model;
    private final Variant variant;
    private final String variantWords;

    public Election(String... args) {
        if (args.length != 4 && args.length != 5) {
            throw new IllegalArgumentException(
                    "election: expected <spec|impl>,<n>,<fifo|bag>,<leader|ring>,<conf|split|silent>, got "
                            + String.join(",", args));
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("election: expected spec or impl, got " + args[0]);
        }
        try {
            n = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("election: <n> must be a number, got " + args[1]);
        }
        if (n < 1) {
            throw new IllegalArgumentException("election: <n> must be at least 1, got " + n);
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("election: expected fifo or bag, got " + args[2]);
        };
        String topo;
        String v;
        if (args.length == 5) {
            topo = args[3];
            v = args[4];
        } else {
            String[] w = args[3].split("[ \\-_/]");
            if (w.length != 2) {
                throw new IllegalArgumentException("election: unknown variant " + args[3]);
            }
            topo = w[0];
            v = w[1];
        }
        variant = switch (topo + " " + v) {
            case "leader conf" -> Variant.LEADER_CONF;
            case "leader split" -> Variant.LEADER_SPLIT;
            case "leader silent" -> Variant.LEADER_SILENT;
            case "ring conf" -> Variant.RING_CONF;
            case "ring split" -> Variant.RING_SPLIT;
            default -> throw new IllegalArgumentException("election: unknown variant `" + topo + " " + v + "`");
        };
        variantWords = topo + " " + v;
    }

    @Override
    public String programName() {
        return "election";
    }

    @Override
    public String size() {
        return variantWords + " " + n;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>(visibleRoles());
        if (spec) {
            roles.add("oracle");
        } else {
            for (int i = 0; i < n; i++) {
                roles.add("e" + i);
            }
        }
        roles.add("boot");
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

    // ------------------------------------------------------------ main ----

    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        // Participants first (F41), on both sides.
        List<Tid> ps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MpProcess.Handle h = MpProcess.spawn("p" + i, this::participant);
            handles.add(h);
            ps.add(h.tid());
        }
        if (spec) {
            MpProcess.Handle oracle = MpProcess.spawn("oracle", this::oracle);
            handles.add(oracle);
            Tid o = oracle.tid();
            List<Tid> psCopy = List.copyOf(ps);
            handles.add(MpProcess.spawn("boot",
                    () -> Channel.send(model, o, new Setup(0, List.of(), psCopy))));
        } else {
            List<Tid> es = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Runnable body = switch (variant) {
                    case LEADER_CONF -> () -> elector(false, false);
                    case LEADER_SPLIT -> () -> elector(true, false);
                    case LEADER_SILENT -> () -> elector(false, true);
                    case RING_CONF -> () -> ringElector(false);
                    case RING_SPLIT -> () -> ringElector(true);
                };
                MpProcess.Handle h = MpProcess.spawn("e" + i, body);
                handles.add(h);
                es.add(h.tid());
            }
            List<Tid> esCopy = List.copyOf(es);
            List<Tid> psCopy = List.copyOf(ps);
            boolean ring = variant == Variant.RING_CONF || variant == Variant.RING_SPLIT;
            handles.add(MpProcess.spawn("boot", () -> {
                for (int i = 0; i < esCopy.size(); i++) {
                    Setup s = new Setup(i, esCopy, psCopy);
                    Channel.send(model, esCopy.get(i), ring ? new RIn.SetupR(s) : new EIn.SetupE(s));
                }
            }));
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    // ------------------------------------------------------------ participant ----

    /** Rust {@code le_participant(n)}, shared by both sides. */
    private void participant() {
        List<Tid> peers = null;
        boolean voted = false;
        int votes = 0;
        boolean allYes = true;
        for (int k = 0; k < n; k++) {
            PIn m = Channel.recv(model, PIn.class);
            if (m instanceof PIn.BeLeader bl) {
                peers = bl.peers();
            } else if (m instanceof PIn.BeFollower bf) {
                if (!voted) {
                    voted = true;
                    Channel.send(model, bf.leader(), new PIn.Vote(true));
                }
            } else if (m instanceof PIn.Vote v) {
                votes += 1;
                allYes &= v.yes();
            } else if (m instanceof PIn.Decision) {
                return;
            }
            if (peers != null && votes == peers.size()) {
                for (Tid p : peers) {
                    Channel.send(model, p, new PIn.Decision(allYes));
                }
                return;
            }
        }
    }

    /** Rust {@code le_announce(i, leader, ps)}. */
    private void announce(int i, int leader, List<Tid> ps) {
        if (i == leader) {
            List<Tid> peers = new ArrayList<>();
            for (int j = 0; j < ps.size(); j++) {
                if (j != leader) {
                    peers.add(ps.get(j));
                }
            }
            Channel.send(model, ps.get(i), new PIn.BeLeader(peers));
        } else {
            Channel.send(model, ps.get(i), new PIn.BeFollower(ps.get(leader)));
        }
    }

    // ------------------------------------------------------------ spec ----

    /** Rust {@code le_oracle()}: a ladder of boolean choices picks the leader. */
    private void oracle() {
        Setup s = Channel.recv(model, Setup.class);
        int np = s.participants().size();
        int leader = np - 1;
        for (int i = 0; i < np - 1; i++) {
            if (Nondet.chooseBoolean()) {
                leader = i;
                break;
            }
        }
        for (int i = 0; i < np; i++) {
            announce(i, leader, s.participants());
        }
    }

    // ------------------------------------------------------------ impl: all-to-all ----

    /** Rust {@code le_elector_with(buggy, silent_winner)}. */
    private void elector(boolean buggy, boolean silentWinner) {
        Setup setup = null;
        List<Integer> heard = new ArrayList<>();
        boolean announced = false;
        while (true) {
            EIn m = Channel.recv(model, EIn.class);
            if (m instanceof EIn.SetupE se) {
                setup = se.setup();
            } else if (m instanceof EIn.Elect el) {
                heard.add(el.index());
            }
            if (setup == null) {
                continue;
            }
            Setup s = setup;
            if (!announced) {
                announced = true;
                for (int i = 0; i < s.electors().size(); i++) {
                    if (i != s.me()) {
                        Channel.send(model, s.electors().get(i), new EIn.Elect(s.me()));
                    }
                }
            }
            if (heard.size() == s.electors().size() - 1) {
                Integer best = buggy ? null : Integer.valueOf(s.me());
                for (int j : heard) {
                    best = best == null ? j : Math.max(best, j);
                }
                int leader = best == null ? s.me() : best;
                if (silentWinner && leader == s.me()) {
                    return;
                }
                announce(s.me(), leader, s.participants());
                return;
            }
        }
    }

    // ------------------------------------------------------------ impl: ring ----

    /** Rust {@code ring_elector(buggy)}. */
    private void ringElector(boolean buggy) {
        Setup setup = null;
        Integer tok = null;
        Integer ann = null;
        boolean started = false;
        for (int k = 0; k < 3; k++) {
            RIn m = Channel.recv(model, RIn.class);
            if (m instanceof RIn.SetupR sr) {
                setup = sr.setup();
            } else if (m instanceof RIn.Tok t) {
                tok = t.max();
            } else if (m instanceof RIn.Ann a) {
                ann = a.leader();
            }
            if (setup == null) {
                continue;
            }
            Setup s = setup;
            int ne = s.electors().size();
            Tid succ = s.electors().get((s.me() + 1) % ne);

            if (!started) {
                started = true;
                if (s.me() == 0) {
                    Channel.send(model, succ, new RIn.Tok(0));
                }
            }

            if (tok != null) {
                int t = tok;
                tok = null;
                if (buggy) {
                    announce(s.me(), t, s.participants());
                    if (s.me() != 0) {
                        Channel.send(model, succ, new RIn.Tok(Math.max(t, s.me())));
                    }
                    return;
                }
                if (s.me() == 0) {
                    announce(0, t, s.participants());
                    if (ne > 1) {
                        Channel.send(model, succ, new RIn.Ann(t));
                    }
                    return;
                }
                Channel.send(model, succ, new RIn.Tok(Math.max(t, s.me())));
            }

            if (ann != null) {
                int l = ann;
                ann = null;
                announce(s.me(), l, s.participants());
                if (s.me() + 1 < ne) {
                    Channel.send(model, succ, new RIn.Ann(l));
                }
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "election(" + (spec ? "spec" : "impl") + ", " + variantWords + ", n=" + n + ", " + model + ")";
    }
}
