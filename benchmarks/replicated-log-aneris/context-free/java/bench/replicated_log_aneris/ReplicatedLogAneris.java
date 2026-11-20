package bench.replicated_log_aneris;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Java twin of the Rust benchmark replicated-log-aneris: Aneris's leader-followers replicated
 * database driven by the causality example. Arguments: {@code <spec|impl>,<W>,<fifo|bag>[,<variant>]},
 * W writes by {@code writer} at the leader (key i := val(i)), then W reads by {@code reader} at the
 * follower in reverse key order. Variants (impl side): {@code correct} (default) and
 * {@code asyn-fault}.
 */
public final class ReplicatedLogAneris implements MpProgram {

    /** The messages; one record per Rust {@code Msg} variant, same tag and field order. */
    public sealed interface Msg extends MpMessage
            permits Write, Ack, Read, Val, Sync, Entry {}

    /** client -> leader: {@code InjL (k, v)}. */
    @Tag("Write")
    public record Write(Tid from, int k, int v) implements Msg {}

    /** leader -> client: {@code InjL ()}. */
    @Tag("Ack")
    public record Ack() implements Msg {}

    /** client -> follower: the key. */
    @Tag("Read")
    public record Read(Tid from, int k) implements Msg {}

    /** follower -> client: {@code val option} (0 = None), as the Rust {@code Val(u8)}. */
    @Tag("Val")
    public record Val(int v) implements Msg {}

    /** follower -> leader: the requested log index. */
    @Tag("Sync")
    public record Sync(Tid from, int i) implements Msg {}

    /** leader -> follower: {@code ((k, v), j)}. */
    @Tag("Entry")
    public record Entry(int k, int v, int j) implements Msg {}

    private final boolean isSpec;
    private final int w;
    private final CommunicationModel model;
    private final String variant;

    /**
     * @param args {@code <spec|impl>, <W>, <fifo|bag> [, <variant>]}
     */
    public ReplicatedLogAneris(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<W>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("kind must be spec or impl: " + args[0]);
        }
        w = Integer.parseInt(args[1]);
        switch (args[2]) {
            case "fifo" -> model = CommunicationModel.P2P;
            case "bag" -> model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        }
        variant = args.length == 4 ? args[3] : "correct";
        if (!isSpec && !variant.equals("correct") && !variant.equals("asyn-fault")) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    /** The value written to key i: x = 37, y = 1, then i. */
    private static int val(int i) {
        return switch (i) {
            case 0 -> 37;
            case 1 -> 1;
            default -> i;
        };
    }

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    private <T extends Msg> T recvTag(Class<T> type) {
        return Channel.recv(model, type);
    }

    private Msg recvTags(Class<? extends Msg> a, Class<? extends Msg> b) {
        return (Msg) Channel.recv(model, m -> a.isInstance(m) || b.isInstance(m));
    }

    /** {@code writer} and {@code reader}, spawned in that order. */
    private List<MpProcess.Handle> spawnClients(Tid wto, Tid rto) {
        final int n = w;
        MpProcess.Handle wr = MpProcess.spawn("writer", () -> {
            Tid me = MpProcess.self();
            for (int i = 0; i < n; i++) {
                send(wto, new Write(me, i, val(i)));
                recvTag(Ack.class);
            }
        });
        MpProcess.Handle rd = MpProcess.spawn("reader", () -> {
            Tid me = MpProcess.self();
            for (int i = n - 1; i >= 0; i--) {
                send(rto, new Read(me, i));
                recvTag(Val.class);
            }
        });
        return List.of(wr, rd);
    }

    private void spec() {
        final int n = w;
        MpProcess.Handle follower = MpProcess.spawn("follower", () -> {
            int[] db = new int[n];
            int next = 0;
            List<Entry> early = new ArrayList<>();
            for (int s = 0; s < 2 * n; s++) {
                Msg m = recvTags(Entry.class, Read.class);
                if (m instanceof Entry e) {
                    early.add(e);
                    while (true) {
                        int p = -1;
                        for (int q = 0; q < early.size(); q++) {
                            if (early.get(q).j() == next) {
                                p = q;
                                break;
                            }
                        }
                        if (p < 0) {
                            break;
                        }
                        Entry x = early.remove(p);
                        db[x.k()] = x.v();
                        next++;
                    }
                } else if (m instanceof Read r) {
                    send(r.from(), new Val(db[r.k()]));
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        Tid fid = follower.tid();
        MpProcess.Handle leader = MpProcess.spawn("leader", () -> {
            for (int j = 0; j < n; j++) {
                Write wm = recvTag(Write.class);
                send(wm.from(), new Ack());
                send(fid, new Entry(wm.k(), wm.v(), j));
            }
        });
        Tid lid = leader.tid();
        for (MpProcess.Handle c : spawnClients(lid, fid)) {
            c.await();
        }
        leader.await();
        follower.await();
    }

    private record Waiting(Tid f, int i) {}

    private void imp() {
        final int n = w;
        final boolean pipelined = variant.equals("asyn-fault");
        MpProcess.Handle leader = MpProcess.spawn("leader", () -> {
            List<int[]> log = new ArrayList<>();
            int[] db = new int[n];
            List<Waiting> waiting = new ArrayList<>();
            for (int s = 0; s < 2 * n; s++) {
                Msg m = recvTags(Write.class, Sync.class);
                if (m instanceof Write wm) {
                    db[wm.k()] = wm.v();
                    log.add(new int[] {wm.k(), wm.v()});
                    send(wm.from(), new Ack());
                    // monitor_signal: answer the waiting requests the new entry satisfies
                    waiting.sort(Comparator.comparingInt(Waiting::i)); // stable, as sort_by_key
                    while (!waiting.isEmpty()) {
                        Waiting first = waiting.get(0);
                        if (first.i() < log.size()) {
                            int[] e = log.get(first.i());
                            send(first.f(), new Entry(e[0], e[1], first.i()));
                            waiting.remove(0);
                        } else {
                            break;
                        }
                    }
                } else if (m instanceof Sync sy) {
                    if (sy.i() < log.size()) {
                        int[] e = log.get(sy.i());
                        send(sy.from(), new Entry(e[0], e[1], sy.i()));
                    } else {
                        waiting.add(new Waiting(sy.from(), sy.i()));
                    }
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        Tid lid = leader.tid();
        MpProcess.Handle follower = MpProcess.spawn("follower", () -> {
            Tid me = MpProcess.self();
            int[] db = new int[n];
            int next = 0; // sync_loop's i
            if (pipelined) {
                for (int i = 0; i < n; i++) {
                    send(lid, new Sync(me, i));
                }
            } else {
                send(lid, new Sync(me, 0));
            }
            for (int s = 0; s < 2 * n; s++) {
                Msg m = recvTags(Entry.class, Read.class);
                if (m instanceof Entry e) {
                    if (!pipelined && next != e.j()) {
                        throw new AssertionError("assertion failed: next == j (next=" + next + ", j=" + e.j() + ")");
                    }
                    db[e.k()] = e.v();
                    next++;
                    if (!pipelined && next < n) {
                        send(lid, new Sync(me, next));
                    }
                } else if (m instanceof Read r) {
                    send(r.from(), new Val(db[r.k()]));
                } else {
                    throw new IllegalStateException("unreachable");
                }
            }
        });
        Tid fid = follower.tid();
        for (MpProcess.Handle c : spawnClients(lid, fid)) {
            c.await();
        }
        leader.await();
        follower.await();
    }

    @Override
    public void run() {
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    @Override
    public String programName() {
        return "replicated-log-aneris";
    }

    @Override
    public String size() {
        return Integer.toString(w);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return isSpec
                ? List.of("follower", "leader", "writer", "reader")
                : List.of("leader", "follower", "writer", "reader");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("writer", "reader");
    }

    @Override
    public String toString() {
        return "ReplicatedLogAneris(" + (isSpec ? "spec" : "impl") + ", W=" + w + ", " + model
                + (isSpec ? "" : ", " + variant) + ")";
    }
}
