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

/**
 * Disel's calculator (candidate N2), the Java twin of {@code rust/src/main.rs}: one specification
 * (a server answering {@code Resp(f(args), args)}, {@code f [x; y] = x + y}) and three
 * implementations (batched, memoising, delegating) with a seeded fault each.
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>,<variant>}; {@code K} requests per client, two
 * clients {@code c0}, {@code c1}. Variants: {@code batched}, {@code batched-swap}, {@code memo},
 * {@code memo-ignore-args}, {@code delegating}, {@code delegating-prev} (ignored on the spec side).
 * Rust's tagged receives ({@code TAG_REQ}, {@code TAG_RESP}) are type-test receives here. Every
 * message is a {@link Tag}ged record mirroring the Rust variant (same tag, same field order).
 */
public final class Calculator implements MpProgram {

    /** Disel {@code req}: the requester and {@code args = [x; y]} (Rust {@code Msg::Req}). */
    @Tag("Req")
    public record Req(Tid from, int x, int y) implements MpMessage {}

    /** Disel {@code resp}: {@code ans :: args} (Rust {@code Msg::Resp}). */
    @Tag("Resp")
    public record Resp(int ans, int x, int y) implements MpMessage {}

    private static final int N = 2;
    private static final int[][] ARGS = {{1, 2}, {2, 2}};

    private final boolean impl;
    private final int k;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, K, model, variant
     */
    public Calculator(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Calculator takes <spec|impl>,<K>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        this.variant = args.length > 3 ? args[3].trim() : "batched";
        if (impl && !List.of("batched", "batched-swap", "memo", "memo-ignore-args", "delegating",
                "delegating-prev").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "calculator-disel-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "n" + N + "k" + k;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (!impl) {
            r.add("server");
        } else if (variant.startsWith("batched")) {
            r.add("batcher");
        } else if (variant.startsWith("memo")) {
            r.add("memo");
        } else {
            r.add("memo");
            r.add("delegator");
        }
        r.add("c0");
        r.add("c1");
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    private static int f(int x, int y) {
        return x + y;
    }

    private Req recvReq() {
        return Channel.recv(m, Req.class);
    }

    private Resp recvResp() {
        return Channel.recv(m, Resp.class);
    }

    @Override
    public void run() {
        int total = N * k;
        List<MpProcess.Handle> servers = new ArrayList<>();
        Tid front;
        if (!impl) {
            MpProcess.Handle s = MpProcess.spawn("server", () -> {
                for (int n = 0; n < total; n++) {
                    Req r = recvReq();
                    Channel.send(m, r.from(), new Resp(f(r.x(), r.y()), r.x(), r.y()));
                }
            });
            servers.add(s);
            front = s.tid();
        } else if (variant.startsWith("batched")) {
            boolean swap = variant.equals("batched-swap");
            MpProcess.Handle s = MpProcess.spawn("batcher", () -> {
                final int bsize = 2;
                for (int b = 0; b < total / bsize; b++) {
                    List<Req> acc = new ArrayList<>();
                    for (int n = 0; n < bsize; n++) {
                        acc.add(0, recvReq());
                    }
                    for (int e = 0; e < bsize; e++) {
                        Req r = acc.get(e);
                        Tid to = swap ? acc.get((e + 1) % bsize).from() : r.from();
                        Channel.send(m, to, new Resp(f(r.x(), r.y()), r.x(), r.y()));
                    }
                }
            });
            servers.add(s);
            front = s.tid();
        } else if (variant.startsWith("memo")) {
            boolean bad = variant.equals("memo-ignore-args");
            MpProcess.Handle s = MpProcess.spawn("memo", () -> memoLoop(total, bad));
            servers.add(s);
            front = s.tid();
        } else {
            boolean prev = variant.equals("delegating-prev");
            MpProcess.Handle mm = MpProcess.spawn("memo", () -> memoLoop(total, false));
            Tid mid = mm.tid();
            MpProcess.Handle d = MpProcess.spawn("delegator", () -> {
                Tid me = MpProcess.self();
                Integer last = null;
                for (int n = 0; n < total; n++) {
                    Req r = recvReq();
                    Channel.send(m, mid, new Req(me, r.x(), r.y()));
                    Resp a = recvResp();
                    int out = prev && last != null ? last : a.ans();
                    last = a.ans();
                    Channel.send(m, r.from(), new Resp(out, r.x(), r.y()));
                }
            });
            servers.add(mm);
            servers.add(d);
            front = d.tid();
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int me = i;
            clients.add(MpProcess.spawn("c" + i, () -> client(me, front)));
        }
        for (MpProcess.Handle c : clients) {
            c.await();
        }
        for (MpProcess.Handle s : servers) {
            s.await();
        }
    }

    /** {@code compute_list_f}: K times send {@code Req}, blocking receive {@code Resp}. */
    private void client(int i, Tid server) {
        Tid me = MpProcess.self();
        for (int j = 0; j < k; j++) {
            int[] a = ARGS[(i + j) % 2];
            Channel.send(m, server, new Req(me, a[0], a[1]));
            recvResp();
        }
    }

    /** {@code memoizing_server}, {@code n} requests; {@code ignoreArgs} is fault F1. */
    private void memoLoop(int n, boolean ignoreArgs) {
        List<int[]> table = new ArrayList<>(); // {x, y, ans}
        for (int c = 0; c < n; c++) {
            Req r = recvReq();
            Integer hit = null;
            if (ignoreArgs) {
                if (!table.isEmpty()) {
                    hit = table.get(0)[2];
                }
            } else {
                for (int[] e : table) {
                    if (e[0] == r.x() && e[1] == r.y()) {
                        hit = e[2];
                        break;
                    }
                }
            }
            int ans;
            if (hit != null) {
                ans = hit;
            } else {
                ans = f(r.x(), r.y());
                table.add(new int[] {r.x(), r.y(), ans});
            }
            Channel.send(m, r.from(), new Resp(ans, r.x(), r.y()));
        }
    }
}
