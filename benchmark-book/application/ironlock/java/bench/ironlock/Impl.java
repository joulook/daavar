package bench.ironlock;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;

import java.util.ArrayList;
import java.util.List;

/**
 * IronLock implementation (twin of {@code imp} in {@code rust/src/main.rs}): invisible
 * {@code node0..node(n-1)} running {@code NodeInit}/{@code NodeGrant}/{@code NodeAccept}. Stop
 * rule: a node holding epoch K+1 does not grant, it sends {@code Stop} to every node (itself
 * included) and ends; a waiting non-holder ends at the first {@code Stop} it reads. Each node
 * sends {@code End} to its observer when it ends. Variant {@code double-hold}: node 1, right
 * after granting {@code Transfer(e+1)}, may also take epoch e+1 itself and emit {@code Locked(e+1)}.
 * Variant {@code grant-without-token}: node 1 may run {@code NodeGrant} once at start without the
 * token.
 * Arguments {@code n,model,variant}. Spawn order: node0..node(n-1), obs0..obs(n-1).
 */
public final class Impl implements MpProgram {

    private final int n;
    private final CommunicationModel m;
    private final String variant;
    private final boolean doubleHold;
    private final boolean grantWithoutToken;

    public Impl(String... args) {
        this.n = Integer.parseInt(args[0].trim());
        this.m = Support.model(args.length > 1 ? args[1] : "bag");
        String v = args.length > 2 ? args[2].trim() : "correct";
        switch (v) {
            case "correct", "double-hold", "grant-without-token" -> { }
            default -> throw new IllegalArgumentException("unknown variant " + v);
        }
        this.variant = v;
        this.doubleHold = v.equals("double-hold");
        this.grantWithoutToken = v.equals("grant-without-token");
    }

    @Override
    public String programName() {
        return "ironlock-impl-" + variant;
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
        for (int i = 0; i < n; i++) {
            r.add("node" + i);
        }
        for (int i = 0; i < n; i++) {
            r.add("obs" + i);
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
        Tid[] obs = new Tid[n];
        for (int i = 0; i < n; i++) {
            final int me = i;
            nodes[i] = Tid.of(ts.add("node" + i, () -> node(me, nodes, obs)).getTaskId());
        }
        for (int i = 0; i < n; i++) {
            obs[i] = Tid.of(ts.add("obs" + i, () -> Lock.observer(m)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void node(int i, Tid[] nodes, Tid[] obs) {
        Tid next = nodes[(i + 1) % n];
        boolean held = i == 0;
        int epoch = i == 0 ? 1 : 0;
        if (grantWithoutToken && i == 1 && Nondet.chooseBoolean()) {
            Channel.send(m, next, new Lock.Transfer(epoch + 1));
        }
        for (int step = 0; step < 2 * Lock.K + 2; step++) {
            if (held) {
                if (epoch <= Lock.K) {
                    Channel.send(m, next, new Lock.Transfer(epoch + 1));
                    held = false;
                    if (doubleHold && i == 1 && Nondet.chooseBoolean()) {
                        held = true;
                        epoch++;
                        Channel.send(m, obs[i], new Lock.Locked(epoch));
                    }
                } else {
                    for (Tid t : nodes) {
                        Channel.send(m, t, new Lock.Stop());
                    }
                    break;
                }
            } else {
                Lock.NodeMsg msg = Channel.recv(m, Lock.NodeMsg.class);
                if (msg instanceof Lock.Stop) {
                    break;
                }
                int e = ((Lock.Transfer) msg).e();
                if (e > epoch) {
                    held = true;
                    epoch = e;
                    Channel.send(m, obs[i], new Lock.Locked(e));
                }
            }
        }
        Channel.send(m, obs[i], new Lock.End());
    }
}
