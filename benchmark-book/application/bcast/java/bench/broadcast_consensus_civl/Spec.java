package bench.broadcast_consensus_civl;

import bench.broadcast_consensus_civl.Msgs.Decide;
import bench.broadcast_consensus_civl.Msgs.Input;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * N3 specification (twin of {@code spec} in {@code rust/src/main.rs}): an invisible {@code oracle}
 * receives the n inputs and sends {@code Decide(max)} to every visible {@code c_i}. Arguments
 * {@code n,model}. Spawn order: oracle, c0..c(n-1).
 */
public final class Spec implements MpProgram {

    private final int n;
    private final CommunicationModel m;

    public Spec(String... args) {
        this.n = Integer.parseInt(args[0].trim());
        this.m = Support.model(args.length > 1 ? args[1] : "bag");
    }

    @Override
    public String programName() {
        return "broadcast-consensus-civl-spec";
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
        r.add("oracle");
        for (int i = 0; i < n; i++) {
            r.add("c" + i);
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return rolesInSpawnOrder().subList(1, n + 1);
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] cs = new Tid[n];
        MpThread o = ts.add("oracle", () -> oracle(cs));
        Tid oid = Tid.of(o.getTaskId());
        for (int i = 0; i < n; i++) {
            cs[i] = Tid.of(ts.add("c" + i, () -> client(m, oid)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void oracle(Tid[] cs) {
        int d = Integer.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            d = Math.max(d, Channel.recv(m, Input.class).v());
        }
        for (Tid c : cs) {
            Channel.send(m, c, new Decide(d));
        }
    }

    /** The visible client, identical on both sides. */
    static void client(CommunicationModel m, Tid to) {
        int v = Nondet.chooseBoolean() ? 1 : 0;
        Channel.send(m, to, new Input(v));
        Channel.recv(m, Decide.class);
    }
}
