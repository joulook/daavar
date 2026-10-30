package bench.ironlock;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * IronLock specification {@code spec-ring}, stricter than IronFleet's (twin of
 * {@code spec(n, true)} in {@code rust/src/main.rs}): an invisible {@code oracle} holding the
 * history in ring order sends {@code Locked(e)} to {@code obs_{(e-1) mod n}} for
 * {@code e = 2..K+1}, then {@code End} to every observer. Arguments {@code n,model}. Spawn order:
 * oracle, obs0..obs(n-1).
 */
public final class SpecRing implements MpProgram {

    private final int n;
    private final CommunicationModel m;

    public SpecRing(String... args) {
        this.n = Integer.parseInt(args[0].trim());
        this.m = Support.model(args.length > 1 ? args[1] : "bag");
    }

    @Override
    public String programName() {
        return "ironlock-spec-ring";
    }

    @Override
    public String size() {
        return "n" + n + "K" + Lock.K;
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
            r.add("obs" + i);
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
        Tid[] obs = new Tid[n];
        ts.add("oracle", () -> {
            for (int e = 2; e <= Lock.K + 1; e++) {
                Channel.send(m, obs[(e - 1) % n], new Lock.Locked(e));
            }
            for (int i = 0; i < n; i++) {
                Channel.send(m, obs[i], new Lock.End());
            }
        });
        for (int i = 0; i < n; i++) {
            obs[i] = Tid.of(ts.add("obs" + i, () -> Lock.observer(m)).getTaskId());
        }
        ts.startAndAwaitAll();
    }
}
