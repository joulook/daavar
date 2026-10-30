package bench.replicated_log_aneris;

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
 * Aneris's leader-followers replicated database (candidate N11), the Java twin of
 * {@code rust/src/main.rs}. Specification: a {@code leader} server that appends, acknowledges and
 * pushes each write to a {@code follower} server, which serves reads from the longest prefix of
 * the log it has received. Implementation:
 * {@code leader} and one {@code follower} running {@code sync_loop}.
 *
 * <p>Arguments: {@code <spec|impl>,<W>,<fifo|bag>,<variant>}; {@code W} writes by {@code writer}
 * (key {@code i} := 37, 1, 2, ...), then {@code W} reads by {@code reader} in reverse key order.
 * Variants: {@code correct} (the source's sync), {@code asyn-fault} (pipelined sync, entries
 * applied in arrival order).
 */
public final class RepDb implements MpProgram {

    /** client -> leader: {@code InjL (k, v)}. */
    @Tag("Write")
    public record Write(Tid from, int k, int v) implements MpMessage {}

    /** leader -> client: {@code InjL ()}. */
    @Tag("Ack")
    public record Ack() implements MpMessage {}

    /** client -> follower: the key. */
    @Tag("Read")
    public record Read(Tid from, int k) implements MpMessage {}

    /** follower -> client: {@code val option} (0 = {@code None}). */
    @Tag("Val")
    public record Val(int v) implements MpMessage {}

    /** follower -> leader: the requested log index. */
    @Tag("Sync")
    public record Sync(Tid from, int i) implements MpMessage {}

    /** leader -> follower: {@code ((k, v), j)}. */
    @Tag("Entry")
    public record Entry(int k, int v, int j) implements MpMessage {}

    private final boolean impl;
    private final int w;
    private final CommunicationModel m;
    private final String variant;

    /**
     * @param args side, W, model, variant
     */
    public RepDb(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("RepDb takes <spec|impl>,<W>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.w = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        this.variant = args.length > 3 ? args[3].trim() : "correct";
        if (impl && !List.of("correct", "asyn-fault").contains(variant)) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    @Override
    public String programName() {
        return "replicated-log-aneris-" + (impl ? variant : "spec");
    }

    @Override
    public String size() {
        return "w" + w;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        if (!impl) {
            return List.of("follower", "leader", "writer", "reader");
        }
        return List.of("leader", "follower", "writer", "reader");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("writer", "reader");
    }

    private static int val(int i) {
        return switch (i) {
            case 0 -> 37;
            case 1 -> 1;
            default -> i;
        };
    }

    @Override
    public void run() {
        List<MpProcess.Handle> servers = new ArrayList<>();
        Tid wto;
        Tid rto;
        if (!impl) {
            MpProcess.Handle f = MpProcess.spawn("follower", this::specFollower);
            Tid fid = f.tid();
            MpProcess.Handle l = MpProcess.spawn("leader", () -> specLeader(fid));
            servers.add(l);
            servers.add(f);
            wto = l.tid();
            rto = fid;
        } else {
            boolean pipelined = variant.equals("asyn-fault");
            MpProcess.Handle l = MpProcess.spawn("leader", this::leader);
            Tid lid = l.tid();
            MpProcess.Handle f = MpProcess.spawn("follower", () -> follower(lid, pipelined));
            servers.add(l);
            servers.add(f);
            wto = lid;
            rto = f.tid();
        }
        final Tid wt = wto;
        final Tid rt = rto;
        MpProcess.Handle wr = MpProcess.spawn("writer", () -> writer(wt));
        MpProcess.Handle rd = MpProcess.spawn("reader", () -> reader(rt));
        wr.await();
        rd.await();
        for (MpProcess.Handle s : servers) {
            s.await();
        }
    }

    /** node0, {@code do_writes}. */
    private void writer(Tid to) {
        Tid me = MpProcess.self();
        for (int i = 0; i < w; i++) {
            Channel.send(m, to, new Write(me, i, val(i)));
            Channel.recv(m, Ack.class);
        }
    }

    /** node1, {@code do_reads} with one read per key. */
    private void reader(Tid to) {
        Tid me = MpProcess.self();
        for (int i = w - 1; i >= 0; i--) {
            Channel.send(m, to, new Read(me, i));
            Channel.recv(m, Val.class);
        }
    }

    /** Spec leader: append, acknowledge, and push each entry to the follower. */
    private void specLeader(Tid fid) {
        for (int j = 0; j < w; j++) {
            Write wr = Channel.recv(m, Write.class);
            Channel.send(m, wr.from(), new Ack());
            Channel.send(m, fid, new Entry(wr.k(), wr.v(), j));
        }
    }

    /** Spec follower: serves reads from the longest prefix of the log received contiguously. */
    private void specFollower() {
        int[] db = new int[w];
        int next = 0;
        List<Entry> early = new ArrayList<>(); // entries received ahead of next
        for (int c = 0; c < 2 * w; c++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Entry || x instanceof Read);
            if (q instanceof Entry e) {
                early.add(e);
                boolean moved = true;
                while (moved) {
                    moved = false;
                    for (int i = 0; i < early.size(); i++) {
                        if (early.get(i).j() == next) {
                            Entry a = early.remove(i);
                            db[a.k()] = a.v();
                            next++;
                            moved = true;
                            break;
                        }
                    }
                }
            } else {
                Read rd = (Read) q;
                Channel.send(m, rd.from(), new Val(db[rd.k()]));
            }
        }
    }

    private void leader() {
        List<int[]> log = new ArrayList<>();
        int[] db = new int[w];
        List<Sync> waiting = new ArrayList<>(); // requests in log_wait_until
        for (int c = 0; c < 2 * w; c++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Write || x instanceof Sync);
            if (q instanceof Write wr) {
                db[wr.k()] = wr.v();
                log.add(new int[] {wr.k(), wr.v()});
                Channel.send(m, wr.from(), new Ack());
                // monitor_signal: answer the waiting requests the new entry satisfies
                waiting.sort((a, b) -> Integer.compare(a.i(), b.i()));
                while (!waiting.isEmpty() && waiting.get(0).i() < log.size()) {
                    Sync s = waiting.remove(0);
                    int[] e = log.get(s.i());
                    Channel.send(m, s.from(), new Entry(e[0], e[1], s.i()));
                }
            } else {
                Sync s = (Sync) q;
                if (s.i() < log.size()) {
                    int[] e = log.get(s.i());
                    Channel.send(m, s.from(), new Entry(e[0], e[1], s.i()));
                } else {
                    waiting.add(s);
                }
            }
        }
    }

    private void follower(Tid lid, boolean pipelined) {
        Tid me = MpProcess.self();
        int[] db = new int[w];
        int next = 0; // sync_loop's i
        if (pipelined) {
            for (int i = 0; i < w; i++) {
                Channel.send(m, lid, new Sync(me, i));
            }
        } else {
            Channel.send(m, lid, new Sync(me, 0));
        }
        for (int c = 0; c < 2 * w; c++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Entry || x instanceof Read);
            if (q instanceof Entry e) {
                if (!pipelined && next != e.j()) {
                    throw new AssertionError("sync_loop: i <> j");
                }
                db[e.k()] = e.v();
                next++;
                if (!pipelined && next < w) {
                    Channel.send(m, lid, new Sync(me, next));
                }
            } else {
                Read rd = (Read) q;
                Channel.send(m, rd.from(), new Val(db[rd.k()]));
            }
        }
    }
}
