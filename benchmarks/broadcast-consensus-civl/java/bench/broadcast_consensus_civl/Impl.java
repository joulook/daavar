package bench.broadcast_consensus_civl;

import bench.broadcast_consensus_civl.Msgs.Decide;
import bench.broadcast_consensus_civl.Msgs.Input;
import bench.broadcast_consensus_civl.Msgs.Val;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * N3 implementation (twin of {@code imp} in {@code rust/src/main.rs}): invisible
 * {@code node0..node(n-1)}; {@code node_i} receives {@code Input(v)} from {@code c_i}, sends
 * {@code Val(v)} to every node (self included), receives n {@code Val}s ({@code n - 1} under
 * {@code decide-early}) and sends {@code Decide(max)} to {@code c_i}. Arguments
 * {@code n,model,variant}. Spawn order: node0..node(n-1), c0..c(n-1).
 */
public final class Impl implements MpProgram {

    private final int n;
    private final CommunicationModel m;
    private final boolean decideEarly;

    public Impl(String... args) {
        this.n = Integer.parseInt(args[0].trim());
        this.m = Support.model(args.length > 1 ? args[1] : "bag");
        String v = args.length > 2 ? args[2].trim() : "correct";
        this.decideEarly = switch (v) {
            case "correct" -> false;
            case "decide-early" -> true;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "broadcast-consensus-civl-impl-" + (decideEarly ? "decide-early" : "correct");
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
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("node" + i);
        }
        for (int i = 0; i < n; i++) {
            r.add("c" + i);
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return rolesInSpawnOrder().subList(n, 2 * n);
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] nodes = new Tid[n];
        Tid[] cs = new Tid[n];
        for (int i = 0; i < n; i++) {
            final int me = i;
            nodes[i] = Tid.of(ts.add("node" + i, () -> node(me, nodes, cs)).getTaskId());
        }
        for (int i = 0; i < n; i++) {
            final Tid to = nodes[i];
            cs[i] = Tid.of(ts.add("c" + i, () -> Spec.client(m, to)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void node(int i, Tid[] nodes, Tid[] cs) {
        int v = Channel.recv(m, Input.class).v();
        for (Tid p : nodes) {
            Channel.send(m, p, new Val(v));
        }
        int d = Integer.MIN_VALUE;
        int collect = decideEarly ? n - 1 : n;
        for (int j = 0; j < collect; j++) {
            d = Math.max(d, Channel.recv(m, Val.class).v());
        }
        Channel.send(m, cs[i], new Decide(d));
    }
}
