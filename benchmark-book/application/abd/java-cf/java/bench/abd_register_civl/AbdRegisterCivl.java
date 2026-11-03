package bench.abd_register_civl;

import java.util.ArrayList;
import java.util.List;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/**
 * abd-register-civl: the ABD atomic register, written from the Rust Must-API program alone.
 * Argument string: {@code <spec|impl>,<replicas>,<fifo|bag>[,<correct|read-no-writeback>]}.
 * Visible roles w, r1, r2; spec adds the invisible register, impl the invisible
 * rep0..rep{n-1}, wp, rp1, rp2. Every Rust channel becomes the receiving thread's mailbox, and
 * a receive on a channel becomes a receive guarded by the channel's message type.
 */
public final class AbdRegisterCivl implements MpProgram {

    /** The writer's pid in {@code TimeStamp(t, pid)}. */
    static final int WPID = 1;

    /** Every message type of the program. */
    public sealed interface Msg extends MpMessage permits Op, Reply, ToReplica, Done, QueryReply, UpdateAck {}

    /** Client -> register (spec) or client -> its proxy (impl). */
    public sealed interface Op extends Msg permits Write, Read {}

    @Tag("Write")
    public record Write(int v) implements Op {}

    /** {@code Read(i)} names the reader r<i>. */
    @Tag("Read")
    public record Read(int i) implements Op {}

    /** Register or proxy -> client. */
    public sealed interface Reply extends Msg permits WriteAck, ReadReply {}

    @Tag("WriteAck")
    public record WriteAck() implements Reply {}

    @Tag("ReadReply")
    public record ReadReply(int v) implements Reply {}

    /** r1 -> r2: r1's read has returned. */
    @Tag("Done")
    public record Done() implements Msg {}

    /** A timestamp (t, pid), ordered lexicographically. */
    @Tag("Ts")
    public record Ts(int t, int pid) {
        boolean lessThan(Ts o) {
            return t < o.t || (t == o.t && pid < o.pid);
        }
    }

    /** Proxy -> replica; the int is the proxy (0 = wp, 1 = rp1, 2 = rp2). */
    public sealed interface ToReplica extends Msg permits Query, Update {}

    @Tag("Query")
    public record Query(int p) implements ToReplica {}

    @Tag("Update")
    public record Update(int p, Ts ts, int v) implements ToReplica {}

    /** Replica -> proxy: the Query result. */
    @Tag("QueryReply")
    public record QueryReply(Ts ts, int v) implements Msg {}

    /** Replica -> proxy: Update done. */
    @Tag("UpdateAck")
    public record UpdateAck() implements Msg {}

    private final boolean isSpec;
    private final int n;
    private final CommunicationModel model;
    private final boolean correct;
    private final String sizeText;

    /**
     * @param args {@code spec|impl}, the replica count, {@code fifo|bag}, optionally the variant
     */
    public AbdRegisterCivl(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "expected <spec|impl>,<replicas>,<fifo|bag>[,<variant>], got " + String.join(",", args));
        }
        switch (args[0]) {
            case "spec" -> isSpec = true;
            case "impl" -> isSpec = false;
            default -> throw new IllegalArgumentException("unknown kind " + args[0] + "; spec or impl");
        }
        n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("replicas must be at least 1, was " + n);
        }
        sizeText = args[1];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown model " + args[2] + "; fifo or bag");
        };
        String variant = args.length == 4 ? args[3] : "correct";
        correct = switch (variant) {
            case "correct" -> true;
            case "read-no-writeback" -> false;
            default -> throw new IllegalArgumentException(
                    "unknown variant " + variant + "; one of correct, read-no-writeback");
        };
    }

    @Override
    public String programName() {
        return "abd-register-civl";
    }

    @Override
    public String size() {
        return sizeText;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        if (isSpec) {
            roles.add("register");
        } else {
            for (int i = 0; i < n; i++) {
                roles.add("rep" + i);
            }
            roles.addAll(List.of("wp", "rp1", "rp2"));
        }
        roles.addAll(List.of("w", "r1", "r2"));
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("w", "r1", "r2");
    }

    @Override
    public void run() {
        // An iteration that ended blocked or failed may have left the list filled.
        threads.clear();
        if (isSpec) {
            spec();
        } else {
            imp();
        }
    }

    // ---- processes: created first (task ids minted, so every body can address every process, as
    // the Rust channels are all created before any spawn), then started in the Rust spawn order,
    // then all awaited.

    private final List<MpThread> threads = new ArrayList<>();

    private Tid create(String role, Runnable body) {
        MpThread t = new MpThread(body, role);
        threads.add(t);
        return Tid.of(t.getTaskId(), role);
    }

    private void startAndAwaitAll() {
        for (MpThread t : threads) {
            t.start();
        }
        for (MpThread t : threads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while joining " + t, e);
            }
        }
        threads.clear();
    }

    /** The three visible clients. {@code to[k]} is client k's front end, {@code clients[k]} client k. */
    private void createClients(Tid[] to, Tid[] clients) {
        CommunicationModel m = model;
        clients[0] = create("w", () -> {
            Channel.send(m, to[0], new Write(1));
            Channel.recv(m, Reply.class);
        });
        clients[1] = create("r1", () -> {
            Channel.send(m, to[1], new Read(1));
            Channel.recv(m, Reply.class);
            Channel.send(m, clients[2], new Done());
        });
        clients[2] = create("r2", () -> {
            Channel.recv(m, Done.class);
            Channel.send(m, to[2], new Read(2));
            Channel.recv(m, Reply.class);
        });
    }

    private void spec() {
        CommunicationModel m = model;
        Tid[] clients = new Tid[3];
        Tid register = create("register", () -> {
            int value = 0;
            for (int k = 0; k < 3; k++) {
                Op op = Channel.recv(m, Op.class);
                if (op instanceof Write w) {
                    value = w.v();
                    Channel.send(m, clients[0], new WriteAck());
                } else if (op instanceof Read r) {
                    Channel.send(m, clients[r.i()], new ReadReply(value));
                }
            }
        });
        createClients(new Tid[] {register, register, register}, clients);
        startAndAwaitAll();
    }

    /** QueryPhase: query every replica, keep the max of the first maj replies. */
    private static QueryReply queryPhase(CommunicationModel m, int me, Tid[] reps, int maj) {
        for (Tid r : reps) {
            Channel.send(m, r, new Query(me));
        }
        QueryReply best = new QueryReply(new Ts(0, 0), 0);
        for (int k = 0; k < maj; k++) {
            QueryReply q = Channel.recv(m, QueryReply.class);
            if (best.ts().lessThan(q.ts())) {
                best = q;
            }
        }
        return best;
    }

    /** UpdatePhase: send (ts, v) to every replica, wait for maj acks. */
    private static void updatePhase(CommunicationModel m, int me, Tid[] reps, int maj, Ts ts, int v) {
        for (Tid r : reps) {
            Channel.send(m, r, new Update(me, ts, v));
        }
        for (int k = 0; k < maj; k++) {
            Channel.recv(m, UpdateAck.class);
        }
    }

    private void imp() {
        CommunicationModel m = model;
        int maj = n / 2 + 1;
        int perReplica = correct ? 6 : 4;
        boolean writeback = correct;
        Tid[] reps = new Tid[n];
        Tid[] proxies = new Tid[3];
        Tid[] clients = new Tid[3];
        for (int i = 0; i < n; i++) {
            reps[i] = create("rep" + i, () -> {
                Ts storeTs = new Ts(0, 0);
                int storeV = 0;
                for (int k = 0; k < perReplica; k++) {
                    ToReplica req = Channel.recv(m, ToReplica.class);
                    if (req instanceof Query q) {
                        Channel.send(m, proxies[q.p()], new QueryReply(storeTs, storeV));
                    } else if (req instanceof Update u) {
                        if (storeTs.lessThan(u.ts())) {
                            storeTs = u.ts();
                            storeV = u.v();
                        }
                        Channel.send(m, proxies[u.p()], new UpdateAck());
                    }
                }
            });
        }
        String[] names = {"wp", "rp1", "rp2"};
        for (int p = 0; p < 3; p++) {
            final int me = p;
            proxies[p] = create(names[p], () -> {
                Op op = Channel.recv(m, Op.class);
                if (op instanceof Write w) {
                    QueryReply q = queryPhase(m, me, reps, maj);
                    updatePhase(m, me, reps, maj, new Ts(q.ts().t() + 1, WPID), w.v());
                    Channel.send(m, clients[me], new WriteAck());
                } else if (op instanceof Read) {
                    QueryReply q = queryPhase(m, me, reps, maj);
                    if (writeback) {
                        updatePhase(m, me, reps, maj, q.ts(), q.v());
                    }
                    Channel.send(m, clients[me], new ReadReply(q.v()));
                }
            });
        }
        createClients(proxies, clients);
        startAndAwaitAll();
    }

    @Override
    public String toString() {
        return "AbdRegisterCivl(" + (isSpec ? "spec" : "impl") + ", n=" + n + ", " + model
                + (isSpec ? "" : correct ? ", correct" : ", read-no-writeback") + ")";
    }
}
