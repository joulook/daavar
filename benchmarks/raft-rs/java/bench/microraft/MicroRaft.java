package bench.microraft;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * MicroRaft (unmodified core, {@code java/vendor}) under JMC's Must API: the Java side of the
 * raft-rs case study ({@code benchmarks/raft-rs/rust/src/main.rs}), with the same visible
 * interface and a twin of raft-rs's specification.
 *
 * <p>impl: three invisible node threads {@code n0 n1 n2} ({@link Node}), each owning one MicroRaft
 * {@code RaftNodeImpl}; two visible clients {@code c0 c1}, {@code c_i} attached to {@code n_i}.
 * spec: raft-rs's {@code spec_log} twin: one invisible {@code log} thread; the clients send their
 * proposals to it; each is either delivered as {@code Committed(v)} to every client in log order,
 * or lost; an invisible {@code init} sends it the clients' ids ({@code Setup}), as in raft-rs.
 *
 * <p>Arguments: {@code <side>,<variant>,<e>,<t>[,<cand>]}: side {@code spec|impl}, variant
 * {@code correct|reorder} (a fault of the harness's delivery: a batch of committed entries is
 * handed to the client in reverse log order), e entries per client, t election timeouts each
 * candidate node may fire, cand the number of nodes allowed to time out ({@code n0..n(cand-1)},
 * default 3). The spec reads only e. Model: {@code fifo} (JMC p2p) only, as raft-rs's README.
 */
public final class MicroRaft implements MpProgram {

    /** visible: client -> its node (raft-rs {@code NIn::Propose(u64)}). */
    @Tag("Propose")
    public record Propose(long v) implements MpMessage {}

    /** visible: node -> its client (raft-rs {@code CIn::Committed(u64)}). */
    @Tag("Committed")
    public record Committed(long v) implements MpMessage {}

    /** spec only, invisible: init -> log (raft-rs {@code NIn::Setup(Setup)}). */
    @Tag("Setup")
    public record Setup(long me, List<Tid> nodes, Optional<Tid> client) implements MpMessage {}

    /**
     * impl only, invisible: a proposal forwarded by a follower to the node it believes leader;
     * raft-rs forwards {@code MsgPropose} inside its own wire messages, MicroRaft has no
     * forwarding ({@code replicate} on a follower fails with {@code NotLeaderException}), so the
     * harness does it.
     */
    @Tag("Fwd")
    public record Fwd(long v) implements MpMessage {}

    static final CommunicationModel M = CommunicationModel.P2P;
    static final int NODES = 3;
    static final int CLIENTS = 2;

    private final boolean impl;
    private final String variant;
    private final boolean reorder;
    private final int e;
    private final int t;
    private final int cand;

    /** @param args side, variant, e, t[, cand] */
    public MicroRaft(String... args) {
        if (args.length != 4 && args.length != 5) {
            throw new IllegalArgumentException(
                    "MicroRaft takes <spec|impl>,<correct|reorder>,<e>,<t>[,<cand>], got " + List.of(args));
        }
        impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        variant = args[1].trim();
        reorder = switch (variant) {
            case "correct" -> false;
            case "reorder" -> true;
            default -> throw new IllegalArgumentException("variant must be correct or reorder");
        };
        e = Integer.parseInt(args[2].trim());
        t = Integer.parseInt(args[3].trim());
        cand = args.length == 5 ? Integer.parseInt(args[4].trim()) : NODES;
        if (e < 0 || t < 0 || cand < 0 || cand > NODES) {
            throw new IllegalArgumentException("e, t >= 0 and 0 <= cand <= 3");
        }
    }

    @Override
    public String programName() {
        return impl ? "microraft-impl-" + variant : "microraft-spec";
    }

    @Override
    public String size() {
        return impl ? "e" + e + "t" + t + "c" + cand : "e" + e;
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return impl ? List.of("n0", "n1", "n2", "c0", "c1") : List.of("log", "c0", "c1", "init");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    static long value(int client, int j) {
        return 1 + client + (long) CLIENTS * j;
    }

    /** raft-rs's {@code client}: propose e values, then receive until all own values were seen. */
    private void client(int i, Tid node) {
        List<Long> mine = new ArrayList<>();
        for (int j = 0; j < e; j++) {
            mine.add(value(i, j));
        }
        for (long v : mine) {
            Channel.send(M, node, new Propose(v));
        }
        int seen = 0;
        while (seen < e) {
            Committed c = Channel.recv(M, Committed.class);
            if (mine.contains(c.v())) {
                seen++;
            }
        }
    }

    /** raft-rs's {@code spec_log}. */
    private void specLog() {
        List<Tid> clients = null;
        ArrayDeque<MpMessage> early = new ArrayDeque<>();
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Setup s) {
                clients = s.nodes();
            } else {
                early.add(m);
            }
            if (clients != null) {
                while (!early.isEmpty()) {
                    if (early.poll() instanceof Propose p) {
                        if (Nondet.chooseBoolean()) {
                            for (Tid c : clients) {
                                Channel.send(M, c, new Committed(p.v()));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Spawn order as in raft-rs: spec {@code log, c0, c1, init}; impl {@code n0, n1, n2, c0, c1}
     * (raft-rs's impl also spawns an {@code init} that sends each node its peers' ids, because a
     * TraceForge thread id exists only after its spawn; here the ids are minted before any thread
     * starts, so the nodes are given them directly).
     */
    @Override
    public void run() {
        List<MpThread> all = new ArrayList<>();
        Tid[] clients = new Tid[CLIENTS];
        if (impl) {
            Tid[] nodes = new Tid[NODES];
            for (int i = 0; i < NODES; i++) {
                final int ii = i;
                MpThread th = new MpThread(() -> new Node(ii, nodes, ii < CLIENTS ? clients[ii] : null,
                        ii < cand ? t : 0, reorder).run(), "n" + i);
                all.add(th);
                nodes[i] = Tid.of(th.getTaskId());
            }
            for (int i = 0; i < CLIENTS; i++) {
                final int ii = i;
                MpThread th = new MpThread(() -> client(ii, nodes[ii]), "c" + i);
                all.add(th);
                clients[i] = Tid.of(th.getTaskId());
            }
        } else {
            MpThread lt = new MpThread(this::specLog, "log");
            all.add(lt);
            Tid log = Tid.of(lt.getTaskId());
            for (int i = 0; i < CLIENTS; i++) {
                final int ii = i;
                MpThread th = new MpThread(() -> client(ii, log), "c" + i);
                all.add(th);
                clients[i] = Tid.of(th.getTaskId());
            }
            all.add(new MpThread(() -> Channel.send(M, log,
                    new Setup(0, List.of(clients), Optional.empty())), "init"));
        }
        for (MpThread th : all) {
            th.start();
        }
        try {
            for (MpThread th : all) {
                th.join1();
            }
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
