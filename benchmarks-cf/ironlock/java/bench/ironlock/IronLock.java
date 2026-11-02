package bench.ironlock;

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
 * IronLock token ring (Java twin of the Rust Must-API program ironlock/rust/src/main.rs).
 *
 * <p>Arguments: {@code <spec|impl>,<n>,<fifo|bag>[,<variant>[,<spec-kind>]]}; variant
 * {@code correct} (default), {@code double-hold} or {@code grant-without-token}; spec-kind
 * {@code any} (default) or {@code ring}. The spec ignores the variant, the impl ignores spec-kind.
 */
public final class IronLock implements MpProgram {

    static final int K = 2;

    /** Node to node: {@code NodeMsg}. */
    sealed interface NodeMsg extends MpMessage permits Transfer, Stop {}

    @Tag("Transfer")
    record Transfer(int epoch) implements NodeMsg {}

    @Tag("Stop")
    record Stop() implements NodeMsg {}

    /** Wire out of node i to obs_i: {@code Wire}. */
    sealed interface Wire extends MpMessage permits Locked, End {}

    @Tag("Locked")
    record Locked(int epoch) implements Wire {}

    @Tag("End")
    record End() implements Wire {}

    enum Variant { CORRECT, DOUBLE_HOLD, GRANT_WITHOUT_TOKEN }

    private final boolean isSpec;
    private final int n;
    private final CommunicationModel model;
    private final Variant variant;
    private final boolean ring;

    public IronLock(String... a) {
        if (a.length < 3 || a.length > 5) {
            throw new IllegalArgumentException(
                    "expected <spec|impl>,<n>,<fifo|bag>[,<variant>[,<spec-kind>]]");
        }
        isSpec = switch (a[0]) {
            case "spec" -> true;
            case "impl" -> false;
            default -> throw new IllegalArgumentException("unknown side " + a[0]);
        };
        n = Integer.parseInt(a[1]);
        if (n < 1) {
            throw new IllegalArgumentException("n must be at least 1");
        }
        model = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown model " + a[2]);
        };
        String v = a.length > 3 ? a[3] : "correct";
        variant = switch (v) {
            case "correct" -> Variant.CORRECT;
            case "double-hold" -> Variant.DOUBLE_HOLD;
            case "grant-without-token" -> Variant.GRANT_WITHOUT_TOKEN;
            default -> throw new IllegalArgumentException("unknown variant " + v
                    + "; one of correct, double-hold, grant-without-token");
        };
        String s = a.length > 4 ? a[4] : "any";
        ring = switch (s) {
            case "any" -> false;
            case "ring" -> true;
            default -> throw new IllegalArgumentException("unknown spec-kind " + s + "; one of any, ring");
        };
    }

    @Override
    public String programName() {
        return "ironlock";
    }

    @Override
    public String size() {
        return Integer.toString(n);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (isSpec) {
            r.add("oracle");
        } else {
            for (int i = 0; i < n; i++) {
                r.add("node" + i);
            }
        }
        r.addAll(visibleRoles());
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add("obs" + i);
        }
        return r;
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    /** The visible obs_i: at most K+2 blocking receives, stop at End. */
    private void observer() {
        for (int k = 0; k < K + 2; k++) {
            if (Channel.recv(model, Wire.class) instanceof End) {
                return;
            }
        }
    }

    private void spawnObservers(Tid[] obs, List<MpProcess.Handle> hs) {
        for (int i = 0; i < n; i++) {
            MpProcess.Handle h = MpProcess.spawn("obs" + i, this::observer);
            obs[i] = h.tid();
            hs.add(h);
        }
    }

    private void spec() {
        // Filled by main before any process runs a message-passing event (main spawns everything
        // first); the processes read it only at their sends.
        final Tid[] obs = new Tid[n];
        List<MpProcess.Handle> hs = new ArrayList<>();
        hs.add(MpProcess.spawn("oracle", () -> {
            if (ring) {
                for (int e = 2; e <= K + 1; e++) {
                    Channel.send(model, obs[(e - 1) % n], new Locked(e));
                }
            } else {
                for (int e = 1; e <= K + 1; e++) {
                    int c = Nondet.chooseIndex(n + 1);
                    if (c < n) {
                        Channel.send(model, obs[c], new Locked(e));
                    }
                }
            }
            for (Tid t : obs) {
                Channel.send(model, t, new End());
            }
        }));
        spawnObservers(obs, hs);
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    private void node(int i, Tid[] all, Tid[] obsAll) {
        // The Tids are read at each send, never before the first event (see spec()).
        final int next = (i + 1) % n;
        boolean held = i == 0;
        int epoch = i == 0 ? 1 : 0;
        if (variant == Variant.GRANT_WITHOUT_TOKEN && i == 1 && Nondet.chooseBoolean()) {
            Channel.send(model, all[next], new Transfer(epoch + 1));
        }
        for (int k = 0; k < 2 * K + 2; k++) {
            if (held) {
                if (epoch <= K) {
                    Channel.send(model, all[next], new Transfer(epoch + 1));
                    held = false;
                    if (variant == Variant.DOUBLE_HOLD && i == 1 && Nondet.chooseBoolean()) {
                        held = true;
                        epoch += 1;
                        Channel.send(model, obsAll[i], new Locked(epoch));
                    }
                } else {
                    for (Tid t : all) {
                        Channel.send(model, t, new Stop());
                    }
                    break;
                }
            } else {
                NodeMsg m = Channel.recv(model, NodeMsg.class);
                if (m instanceof Stop) {
                    break;
                }
                int e = ((Transfer) m).epoch();
                if (e > epoch) {
                    held = true;
                    epoch = e;
                    Channel.send(model, obsAll[i], new Locked(e));
                }
            }
        }
        Channel.send(model, obsAll[i], new End());
    }

    private void imp() {
        final Tid[] nodes = new Tid[n];
        final Tid[] obs = new Tid[n];
        List<MpProcess.Handle> hs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int id = i;
            MpProcess.Handle h = MpProcess.spawn("node" + i, () -> node(id, nodes, obs));
            nodes[i] = h.tid();
            hs.add(h);
        }
        spawnObservers(obs, hs);
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }
}
