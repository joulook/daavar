package bench.zab_broadcast_remix;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * The broadcast phase of Zab with a one-shot late join (candidate N18), against one sequencer
 * server: the Java twin of {@code rust/src/main.rs} (same threads, spawn order, messages; Rust's
 * per-thread inbox channels are thread mailboxes here, the ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<Q>,<fifo|bag>,<variant>}; Q writes per client, clients
 * {@code c0} (at the leader) and {@code c1} (at the late follower {@code s2}). Variants:
 * {@code correct}, {@code issue1-propose-to-ackld}. Internal histories are {@code int[]}
 * arrays ({@code zxid, client, value[, acks]}); {@code NEWLEADER} carries {@link Entry} records.
 */
public final class ZabBroadcast implements MpProgram {

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage
            permits Write, Committed, AckEpoch, NewLeader, AckLd, CommitLd, Propose, Ack, Commit {}

    /** A client write (also a follower's forward of it). */
    @Tag("Write")
    public record Write(int c, int v) implements Msg {}

    /** The answer to a client. */
    @Tag("Committed")
    public record Committed(int zxid) implements Msg {}

    /** {@code ACKEPOCH}. */
    @Tag("AckEpoch")
    public record AckEpoch() implements Msg {}

    /** A history entry {@code (zxid, client, value)} (Rust's tuple). */
    public record Entry(int zxid, int c, int v) {}

    /** {@code NEWLEADER} with the history. */
    @Tag("NewLeader")
    public record NewLeader(List<Entry> hist) implements Msg {}

    /** {@code ACKLD}. */
    @Tag("AckLd")
    public record AckLd(int zxid) implements Msg {}

    /** {@code COMMITLD}. */
    @Tag("CommitLd")
    public record CommitLd(int zxid) implements Msg {}

    /** {@code PROPOSE}. */
    @Tag("Propose")
    public record Propose(int zxid, int c, int v) implements Msg {}

    /** {@code ACK} from follower {@code from}. */
    @Tag("Ack")
    public record Ack(int zxid, int from) implements Msg {}

    /** {@code COMMIT}. */
    @Tag("Commit")
    public record Commit(int zxid) implements Msg {}

    private static final int QUORUM = 2;
    /** Mailbox indices, as the Rust inboxes: 0 c0, 1 c1, 2 server / s0, 3 s1, 4 s2. */
    private static final int C0 = 0;
    private static final int S0 = 2;

    private final boolean impl;
    private final int q;
    private final CommunicationModel m;
    private final boolean proposeToAckld;

    /**
     * @param args side, Q, model, variant
     */
    public ZabBroadcast(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("ZabBroadcast takes <spec|impl>,<Q>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.q = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "fifo");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.proposeToAckld = switch (v) {
            case "correct" -> false;
            case "issue1-propose-to-ackld" -> true;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "zab-broadcast-remix-" + (impl ? (proposeToAckld ? "issue1-propose-to-ackld" : "correct") : "spec");
    }

    @Override
    public String size() {
        return "s3c2q" + q;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return impl ? List.of("s0", "s1", "s2", "c0", "c1") : List.of("server", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    private Msg recv() {
        return (Msg) Channel.recv(m);
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[5];
        if (!impl) {
            t[S0] = Tid.of(ts.add("server", () -> server(t)).getTaskId());
            t[0] = Tid.of(ts.add("c0", () -> client(0, t[S0], t)).getTaskId());
            t[1] = Tid.of(ts.add("c1", () -> client(1, t[S0], t)).getTaskId());
        } else {
            t[S0] = Tid.of(ts.add("s0", () -> leader(t)).getTaskId());
            t[S0 + 1] = Tid.of(ts.add("s1", () -> follower(1, -1, false, t)).getTaskId());
            t[S0 + 2] = Tid.of(ts.add("s2", () -> follower(2, 1, true, t)).getTaskId());
            t[0] = Tid.of(ts.add("c0", () -> client(0, t[S0], t)).getTaskId());
            t[1] = Tid.of(ts.add("c1", () -> client(1, t[S0 + 2], t)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void client(int c, Tid to, Tid[] t) {
        for (int j = 0; j < q; j++) {
            Channel.send(m, to, new Write(c, 10 * c + j + 1));
            if (!(recv() instanceof Committed)) {
                throw new IllegalStateException("c" + c);
            }
        }
    }

    private void server(Tid[] t) {
        for (int n = 1; n <= 2 * q; n++) {
            Write w = (Write) recv();
            Channel.send(m, t[w.c()], new Committed(n));
        }
    }

    private void leader(Tid[] t) {
        List<int[]> history = new ArrayList<>(); // {zxid, c, v, acks}
        int lastCommitted = 0;
        int ackerecv = 1 << 1;
        int ackldrecv = 1 << 1;
        while (true) {
            Msg x = recv();
            if (x instanceof Write w) {
                int zxid = history.size() + 1;
                history.add(new int[] {zxid, w.c(), w.v(), 1});
                int qs = proposeToAckld ? ackldrecv : ackerecv;
                for (int f = 1; f < 3; f++) {
                    if ((qs & (1 << f)) != 0) {
                        Channel.send(m, t[S0 + f], new Propose(zxid, w.c(), w.v()));
                    }
                }
            } else if (x instanceof Ack a) {
                int index = a.zxid();
                int[] e = history.get(index - 1);
                e[3] |= 1 << a.from();
                if (lastCommitted + 1 == index && Integer.bitCount(e[3]) >= QUORUM) {
                    lastCommitted = index;
                    for (int f = 1; f < 3; f++) {
                        if ((ackldrecv & (1 << f)) != 0) {
                            Channel.send(m, t[S0 + f], new Commit(a.zxid()));
                        }
                    }
                    if (e[1] == C0) {
                        Channel.send(m, t[C0], new Committed(a.zxid()));
                    }
                }
            } else if (x instanceof AckEpoch) {
                ackerecv |= 1 << 2;
                List<Entry> hist = new ArrayList<>();
                for (int[] e : history) {
                    hist.add(new Entry(e[0], e[1], e[2]));
                }
                Channel.send(m, t[S0 + 2], new NewLeader(hist));
            } else if (x instanceof AckLd a) {
                ackldrecv |= 1 << 2;
                for (int[] e : history) {
                    if (e[0] <= a.zxid()) {
                        e[3] |= 1 << 2;
                    }
                }
                Channel.send(m, t[S0 + 2], new CommitLd(lastCommitted));
            } else {
                throw new IllegalStateException("s0 got " + x);
            }
        }
    }

    /** {@code client}: the client this follower serves, -1 for none. */
    private void follower(int sid, int client, boolean late, Tid[] t) {
        List<int[]> log = new ArrayList<>();
        int committed = 0;
        boolean synced = !late;
        List<Write> waiting = new ArrayList<>();
        if (late) {
            Channel.send(m, t[S0], new AckEpoch());
        }
        while (true) {
            Msg x = recv();
            if (x instanceof Write w) {
                if (synced) {
                    Channel.send(m, t[S0], w);
                } else {
                    waiting.add(w);
                }
            } else if (x instanceof NewLeader nl) {
                log = new ArrayList<>();
                for (Entry e : nl.hist()) {
                    log.add(new int[] {e.zxid(), e.c(), e.v()});
                }
                int last = log.isEmpty() ? 0 : log.get(log.size() - 1)[0];
                Channel.send(m, t[S0], new AckLd(last));
            } else if (x instanceof CommitLd cl) {
                while (committed < log.size() && log.get(committed)[0] <= cl.zxid()) {
                    reply(log.get(committed), client, t);
                    committed++;
                }
                synced = true;
                for (Write w : waiting) {
                    Channel.send(m, t[S0], w);
                }
                waiting.clear();
            } else if (x instanceof Propose p) {
                int last = log.isEmpty() ? 0 : log.get(log.size() - 1)[0];
                if (p.zxid() == last + 1) {
                    log.add(new int[] {p.zxid(), p.c(), p.v()});
                    Channel.send(m, t[S0], new Ack(p.zxid(), sid));
                }
            } else if (x instanceof Commit c) {
                if (committed < log.size() && log.get(committed)[0] == c.zxid()) {
                    reply(log.get(committed), client, t);
                    committed++;
                }
            } else {
                throw new IllegalStateException("s" + sid + " got " + x);
            }
        }
    }

    private void reply(int[] e, int client, Tid[] t) {
        if (e[1] == client) {
            Channel.send(m, t[e[1]], new Committed(e[0]));
        }
    }
}
