package bench.calculator_disel;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Disel's calculator (twin of the Rust program calculator-disel/rust/src/main.rs). One
 * specification (a server answering {@code Resp(f(args), args)} to each {@code Req(args)},
 * {@code f [x; y] = x + y}) and three implementations (batched, memoising, delegating), each with
 * a seeded fault variant.
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>[,<variant>]}; {@code K} = requests per client
 * (2 clients); variant (impl side, default {@code batched}): {@code batched}, {@code batched-swap},
 * {@code memo}, {@code memo-ignore-args}, {@code delegating}, {@code delegating-prev}.
 */
public final class Calculator implements MpProgram {

    /** Number of clients. */
    private static final int N = 2;

    /** The argument domain: client i's j-th request has ARGS[(i + j) % 2]. */
    private static final int[][] ARGS = {{1, 2}, {2, 2}};

    /** The messages (Rust enum Msg). */
    public sealed interface Msg extends MpMessage permits Req, Resp {}

    /** Disel {@code req}: the requester and {@code args = [x; y]}. */
    @Tag("Req")
    public record Req(Tid from, int x, int y) implements Msg {}

    /** Disel {@code resp}: {@code ans :: args}. */
    @Tag("Resp")
    public record Resp(int ans, int x, int y) implements Msg {}

    private final boolean spec;
    private final int k;
    private final CommunicationModel model;
    private final String variant;

    /**
     * @param a {@code <spec|impl>, <K>, <fifo|bag> [, <variant>]}
     */
    public Calculator(String... a) {
        if (a.length < 3 || a.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<K>,<fifo|bag>[,<variant>]");
        }
        switch (a[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("kind must be spec or impl: " + a[0]);
        }
        k = Integer.parseInt(a[1]);
        if (k < 1) {
            throw new IllegalArgumentException("K must be >= 1");
        }
        model = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + a[2]);
        };
        variant = a.length == 4 ? a[3] : "batched";
        switch (variant) {
            case "batched", "batched-swap", "memo", "memo-ignore-args", "delegating", "delegating-prev" -> { }
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    private static int f(int x, int y) {
        return x + y;
    }

    private Req recvReq() {
        return Channel.recv(model, Req.class);
    }

    private Resp recvResp() {
        return Channel.recv(model, Resp.class);
    }

    private void sendAnswer(Tid to, int ans, int x, int y) {
        Channel.send(model, to, new Resp(ans, x, y));
    }

    /** {@code compute_list_f}: K times send Req, blocking receive Resp. */
    private List<MpProcess.Handle> spawnClients(Tid server) {
        List<MpProcess.Handle> cs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int ci = i;
            cs.add(MpProcess.spawn("c" + i, () -> {
                Tid me = MpProcess.self();
                for (int j = 0; j < k; j++) {
                    int[] args = ARGS[(ci + j) % 2];
                    Channel.send(model, server, new Req(me, args[0], args[1]));
                    recvResp();
                }
            }));
        }
        return cs;
    }

    /** The memoising server body, {@code n} requests; {@code ignoreArgs} is fault F1. */
    private void memoLoop(int n, boolean ignoreArgs) {
        // entries: {x, y, value}
        List<int[]> table = new ArrayList<>();
        for (int r = 0; r < n; r++) {
            Req q = recvReq();
            Optional<Integer> hit = Optional.empty();
            if (ignoreArgs) {
                if (!table.isEmpty()) {
                    hit = Optional.of(table.get(0)[2]);
                }
            } else {
                for (int[] e : table) {
                    if (e[0] == q.x() && e[1] == q.y()) {
                        hit = Optional.of(e[2]);
                        break;
                    }
                }
            }
            int ans;
            if (hit.isPresent()) {
                ans = hit.get();
            } else {
                ans = f(q.x(), q.y());
                table.add(new int[] {q.x(), q.y(), ans});
            }
            sendAnswer(q.from(), ans, q.x(), q.y());
        }
    }

    private void runSpec() {
        MpProcess.Handle server = MpProcess.spawn("server", () -> {
            for (int r = 0; r < N * k; r++) {
                Req q = recvReq();
                sendAnswer(q.from(), f(q.x(), q.y()), q.x(), q.y());
            }
        });
        List<MpProcess.Handle> cs = spawnClients(server.tid());
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        server.await();
    }

    private void runImpl() {
        final int total = N * k;
        List<MpProcess.Handle> servers = new ArrayList<>();
        Tid front;
        switch (variant) {
            case "batched", "batched-swap" -> {
                final boolean swap = variant.equals("batched-swap");
                MpProcess.Handle h = MpProcess.spawn("batcher", () -> {
                    final int bsize = 2;
                    for (int b = 0; b < total / bsize; b++) {
                        // receive_req_loop: prepend each request (Disel's acc)
                        List<Req> acc = new ArrayList<>();
                        for (int r = 0; r < bsize; r++) {
                            acc.add(0, recvReq());
                        }
                        // send_ans_loop; fault F2 sends entry e's answer to entry e+1's requester
                        for (int e = 0; e < bsize; e++) {
                            Req q = acc.get(e);
                            Tid to = swap ? acc.get((e + 1) % bsize).from() : q.from();
                            sendAnswer(to, f(q.x(), q.y()), q.x(), q.y());
                        }
                    }
                });
                servers.add(h);
                front = h.tid();
            }
            case "memo", "memo-ignore-args" -> {
                final boolean bad = variant.equals("memo-ignore-args");
                MpProcess.Handle h = MpProcess.spawn("memo", () -> memoLoop(total, bad));
                servers.add(h);
                front = h.tid();
            }
            case "delegating", "delegating-prev" -> {
                final boolean prev = variant.equals("delegating-prev");
                MpProcess.Handle m = MpProcess.spawn("memo", () -> memoLoop(total, false));
                final Tid mid = m.tid();
                MpProcess.Handle d = MpProcess.spawn("delegator", () -> {
                    Tid me = MpProcess.self();
                    Optional<Integer> last = Optional.empty();
                    for (int r = 0; r < total; r++) {
                        Req q = recvReq();
                        // compute_f on the delegate (instance l2)
                        Channel.send(model, mid, new Req(me, q.x(), q.y()));
                        int ans = recvResp().ans();
                        // fault F3: a one-slot cache answers with the previous request's result
                        int out = prev ? last.orElse(ans) : ans;
                        last = Optional.of(ans);
                        sendAnswer(q.from(), out, q.x(), q.y());
                    }
                });
                servers.add(m);
                servers.add(d);
                front = d.tid();
            }
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        }
        List<MpProcess.Handle> cs = spawnClients(front);
        for (MpProcess.Handle c : cs) {
            c.await();
        }
        for (MpProcess.Handle s : servers) {
            s.await();
        }
    }

    @Override
    public void run() {
        if (spec) {
            runSpec();
        } else {
            runImpl();
        }
    }

    @Override
    public String programName() {
        return "calculator-disel";
    }

    @Override
    public String size() {
        return Integer.toString(k);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        if (spec) {
            return List.of("server", "c0", "c1");
        }
        return switch (variant) {
            case "batched", "batched-swap" -> List.of("batcher", "c0", "c1");
            case "memo", "memo-ignore-args" -> List.of("memo", "c0", "c1");
            default -> List.of("memo", "delegator", "c0", "c1");
        };
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    @Override
    public String toString() {
        return "Calculator(" + (spec ? "spec" : "impl") + ", K=" + k + ", " + model
                + (spec ? "" : ", " + variant) + ")";
    }
}
