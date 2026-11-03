package bench.abd_register_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * ABD implementation (twin of {@code imp} in {@code rust/src/main.rs}): invisible replicas
 * {@code rep0..rep(n-1)} and proxies {@code wp}, {@code rp1}, {@code rp2} running the source's
 * {@code WriteClient}/{@code ReadClient} ({@code QueryPhase}, then {@code UpdatePhase}; majority
 * {@code n/2 + 1} of the replies, first come). Variant {@code read-no-writeback}: readers skip
 * {@code UpdatePhase}. Arguments {@code replicas,model,variant}. Spawn order: rep0..rep(n-1), wp,
 * rp1, rp2, w, r1, r2.
 */
public final class Impl implements MpProgram {

    private static final int WPID = 1;

    private final int n;
    private final int maj;
    private final CommunicationModel m;
    private final boolean writeback;

    public Impl(String... args) {
        this.n = Integer.parseInt(args[0].trim());
        this.maj = n / 2 + 1;
        this.m = Support.model(args.length > 1 ? args[1] : "bag");
        String v = args.length > 2 ? args[2].trim() : "correct";
        this.writeback = switch (v) {
            case "correct" -> true;
            case "read-no-writeback" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "abd-register-civl-impl-" + (writeback ? "correct" : "read-no-writeback");
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
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("rep" + i);
        }
        r.addAll(List.of("wp", "rp1", "rp2", "w", "r1", "r2"));
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("w", "r1", "r2");
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] reps = new Tid[n];
        Tid[] proxies = new Tid[3];
        Tid[] clients = new Tid[3];
        int perReplica = writeback ? 6 : 4;
        for (int i = 0; i < n; i++) {
            reps[i] = Tid.of(ts.add("rep" + i, () -> replica(perReplica, proxies)).getTaskId());
        }
        String[] names = {"wp", "rp1", "rp2"};
        for (int p = 0; p < 3; p++) {
            final int me = p;
            proxies[p] = Tid.of(ts.add(names[p], () -> proxy(me, reps, clients)).getTaskId());
        }
        Tid[] r2 = new Tid[1];
        clients[0] = Tid.of(ts.add("w", () -> Abd.w(m, proxies[0])).getTaskId());
        clients[1] = Tid.of(ts.add("r1", () -> Abd.r1(m, proxies[1], r2)).getTaskId());
        clients[2] = Tid.of(ts.add("r2", () -> Abd.r2(m, proxies[2])).getTaskId());
        r2[0] = clients[2];
        ts.startAndAwaitAll();
    }

    /** The source's {@code Query#0} / {@code Update#0}, one reply per request. */
    private void replica(int requests, Tid[] proxies) {
        int t = 0;
        int pid = 0;
        int val = 0;
        for (int k = 0; k < requests; k++) {
            Abd.ToReplica msg = Channel.recv(m, Abd.ToReplica.class);
            if (msg instanceof Abd.Query q) {
                Channel.send(m, proxies[q.p()], new Abd.QueryReply(new Abd.Ts(t, pid), val));
            } else {
                Abd.Update u = (Abd.Update) msg;
                if (Abd.lt(t, pid, u.ts().t(), u.ts().pid())) {
                    t = u.ts().t();
                    pid = u.ts().pid();
                    val = u.v();
                }
                Channel.send(m, proxies[u.p()], new Abd.UpdateAck());
            }
        }
    }

    private void proxy(int me, Tid[] reps, Tid[] clients) {
        Abd.Op op = Channel.recv(m, Abd.Op.class);
        // QueryPhase
        for (Tid r : reps) {
            Channel.send(m, r, new Abd.Query(me));
        }
        int bt = 0;
        int bp = 0;
        int bv = 0;
        for (int k = 0; k < maj; k++) {
            Abd.QueryReply q = Channel.recv(m, Abd.QueryReply.class);
            if (Abd.lt(bt, bp, q.ts().t(), q.ts().pid())) {
                bt = q.ts().t();
                bp = q.ts().pid();
                bv = q.v();
            }
        }
        if (op instanceof Abd.Write wr) {
            updatePhase(me, reps, bt + 1, WPID, wr.v());
            Channel.send(m, clients[me], new Abd.WriteAck());
        } else {
            if (writeback) {
                updatePhase(me, reps, bt, bp, bv);
            }
            Channel.send(m, clients[me], new Abd.ReadReply(bv));
        }
    }

    private void updatePhase(int me, Tid[] reps, int t, int pid, int v) {
        for (Tid r : reps) {
            Channel.send(m, r, new Abd.Update(me, new Abd.Ts(t, pid), v));
        }
        for (int k = 0; k < maj; k++) {
            Channel.recv(m, Abd.UpdateAck.class);
        }
    }
}
