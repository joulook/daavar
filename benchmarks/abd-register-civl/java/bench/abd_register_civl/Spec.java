package bench.abd_register_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.List;

/**
 * ABD specification: one invisible {@code register} serving the three operations atomically.
 * Arguments {@code replicas,model} (the replica count does not change the specification).
 * Spawn order: register, w, r1, r2.
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
        return "abd-register-civl-spec";
    }

    @Override
    public String size() {
        return "replicas" + n;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("register", "w", "r1", "r2");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("w", "r1", "r2");
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] c = new Tid[3];
        Tid reg = Tid.of(ts.add("register", () -> register(c)).getTaskId());
        Tid[] r2 = new Tid[1];
        c[0] = Tid.of(ts.add("w", () -> Abd.w(m, reg)).getTaskId());
        c[1] = Tid.of(ts.add("r1", () -> Abd.r1(m, reg, r2)).getTaskId());
        c[2] = Tid.of(ts.add("r2", () -> Abd.r2(m, reg)).getTaskId());
        r2[0] = c[2];
        ts.startAndAwaitAll();
    }

    private void register(Tid[] c) {
        int value = 0;
        for (int k = 0; k < 3; k++) {
            Abd.Op op = Channel.recv(m, Abd.Op.class);
            if (op instanceof Abd.Write wr) {
                value = wr.v();
                Channel.send(m, c[0], new Abd.WriteAck());
            } else {
                Channel.send(m, c[((Abd.Read) op).i()], new Abd.ReadReply(value));
            }
        }
    }
}
