package bench.zab_broadcast_remix;

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
 * Java twin of the Rust/TraceForge benchmark zab-broadcast-remix (broadcast phase of Zab with the
 * one-shot join of a late follower). Argument string: {@code <spec|impl>,<Q>,<fifo|bag>[,<variant>]},
 * variant {@code correct} (default) or {@code issue1-propose-to-ackld}.
 */
public final class ZabBroadcastRemix implements MpProgram {

    /** One entry of a history: zxid, client, value (the Rust tuple struct {@code Entry}). */
    @Tag("Entry")
    public record Entry(int zxid, int c, int v) {}

    /** The Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage
            permits Write, Committed, AckEpoch, NewLeader, AckLd, CommitLd, Propose, Ack, Commit {}

    @Tag("Write") public record Write(int c, int v) implements Msg {}
    @Tag("Committed") public record Committed(int zxid) implements Msg {}
    @Tag("AckEpoch") public record AckEpoch() implements Msg {}
    @Tag("NewLeader") public record NewLeader(List<Entry> hist) implements Msg {
        public NewLeader { hist = List.copyOf(hist); }
    }
    @Tag("AckLd") public record AckLd(int zxid) implements Msg {}
    @Tag("CommitLd") public record CommitLd(int zxid) implements Msg {}
    @Tag("Propose") public record Propose(int zxid, int c, int v) implements Msg {}
    @Tag("Ack") public record Ack(int zxid, int from) implements Msg {}
    @Tag("Commit") public record Commit(int zxid) implements Msg {}

    private static final int QUORUM = 2;
    /** Inboxes: 0 c0, 1 c1, 2 server (spec) / s0 (impl), 3 s1, 4 s2. */
    private static final int C0 = 0;
    private static final int S0 = 2;

    private final boolean impl;
    private final int q;
    private final CommunicationModel model;
    private final boolean proposeToAckld;

    public ZabBroadcastRemix(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<Q>,<fifo|bag>[,<variant>]");
        }
        impl = switch (args[0]) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("kind must be spec or impl: " + args[0]);
        };
        q = Integer.parseInt(args[1]);
        if (q < 0 || 10 + q > 255) {
            throw new IllegalArgumentException("Q out of range (values are u8 on the Rust side): " + q);
        }
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        };
        String variant = args.length > 3 ? args[3] : "correct";
        proposeToAckld = switch (variant) {
            case "correct" -> false;
            case "issue1-propose-to-ackld" -> true;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
    }

    @Override
    public String programName() {
        return "zab-broadcast-remix";
    }

    @Override
    public String size() {
        return Integer.toString(q);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return impl ? List.of("s0", "s1", "s2", "c0", "c1") : List.of("server", "c0", "c1");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("c0", "c1");
    }

    // ---------------------------------------------------------------------------------------
    // Addressing. The Rust program creates every channel before spawning; here an inbox is the
    // process itself, whose Tid exists only once it is spawned. A process may run before main has
    // spawned the rest, so the Tids are computed up front from the launcher's documented rule
    // (MpProgram: "the k-th process main spawns is task k + 1") and checked against every handle.
    // ---------------------------------------------------------------------------------------

    /** Inbox index -> Tid, for this iteration. */
    private Tid[] addressBook(int nProcs, int[] spawnOrderToInbox, String[] roles, long firstRaw) {
        Tid[] t = new Tid[nProcs];
        for (int k = 0; k < spawnOrderToInbox.length; k++) {
            t[spawnOrderToInbox[k]] = Tid.of(firstRaw + k, roles[k]);
        }
        return t;
    }

    private static void check(MpProcess.Handle h, Tid expected) {
        if (!h.tid().equals(expected)) {
            throw new IllegalStateException("spawned " + h.tid() + ", expected " + expected
                    + ": the task numbering assumption does not hold");
        }
    }

    /** First spawned process's raw task id (main is task 1, so k-th spawned, k from 1, is k + 1). */
    private static final long FIRST_RAW = 2L;

    private Msg recv() {
        MpMessage m = Channel.recv(model);
        if (!(m instanceof Msg msg)) {
            throw new IllegalStateException("unexpected message " + m);
        }
        return msg;
    }

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    private void client(int c, Tid to) {
        for (int j = 0; j < q; j++) {
            send(to, new Write(c, 10 * c + j + 1));
            if (!(recv() instanceof Committed)) {
                throw new IllegalStateException("c" + c);
            }
        }
    }

    @Override
    public void run() {
        if (impl) {
            imp();
        } else {
            spec();
        }
    }

    private void spec() {
        // spawn order: server (inbox 2), c0 (inbox 0), c1 (inbox 1)
        int[] inbox = {S0, 0, 1};
        String[] roles = {"server", "c0", "c1"};
        Tid[] t = addressBook(3, inbox, roles, FIRST_RAW);
        List<MpProcess.Handle> hs = new ArrayList<>();
        hs.add(MpProcess.spawn("server", () -> {
            for (int n = 1; n <= 2 * q; n++) {
                if (!(recv() instanceof Write w)) {
                    throw new IllegalStateException("server");
                }
                send(t[w.c()], new Committed(n));
            }
        }));
        for (int c = 0; c < 2; c++) {
            int cc = c;
            hs.add(MpProcess.spawn("c" + c, () -> client(cc, t[S0])));
        }
        for (int k = 0; k < hs.size(); k++) {
            check(hs.get(k), t[inbox[k]]);
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    private void imp() {
        // spawn order: s0 (2), s1 (3), s2 (4), c0 (0), c1 (1)
        int[] inbox = {S0, S0 + 1, S0 + 2, 0, 1};
        String[] roles = {"s0", "s1", "s2", "c0", "c1"};
        Tid[] t = addressBook(5, inbox, roles, FIRST_RAW);
        List<MpProcess.Handle> hs = new ArrayList<>();
        hs.add(MpProcess.spawn("s0", () -> leader(t)));
        hs.add(MpProcess.spawn("s1", () -> follower(1, -1, false, t)));
        hs.add(MpProcess.spawn("s2", () -> follower(2, 1, true, t)));
        // c0 writes to the leader, c1 to the late follower
        hs.add(MpProcess.spawn("c0", () -> client(0, t[S0])));
        hs.add(MpProcess.spawn("c1", () -> client(1, t[S0 + 2])));
        for (int k = 0; k < hs.size(); k++) {
            check(hs.get(k), t[inbox[k]]);
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    /** A leader-local history entry; {@code acks} has one bit per server (ackSid). */
    private static final class Txn {
        final int zxid;
        final int c;
        final int v;
        int acks;

        Txn(int zxid, int c, int v, int acks) {
            this.zxid = zxid;
            this.c = c;
            this.v = v;
            this.acks = acks;
        }
    }

    /** The leader s0. Servers are numbered 0, 1, 2 (inbox 2 + sid). */
    private void leader(Tid[] t) {
        List<Txn> history = new ArrayList<>();
        int lastCommitted = 0;
        int ackerecv = 1 << 1;
        int ackldrecv = 1 << 1;
        while (true) {
            Msg m = recv();
            if (m instanceof Write w) {
                int zxid = history.size() + 1;
                history.add(new Txn(zxid, w.c(), w.v(), 1 << 0));
                int qs = proposeToAckld ? ackldrecv : ackerecv;
                for (int f = 1; f < 3; f++) {
                    if ((qs & (1 << f)) != 0) {
                        send(t[S0 + f], new Propose(zxid, w.c(), w.v()));
                    }
                }
            } else if (m instanceof Ack a) {
                int index = a.zxid();
                Txn x = history.get(index - 1);
                x.acks |= 1 << a.from();
                if (lastCommitted + 1 == index && Integer.bitCount(x.acks) >= QUORUM) {
                    lastCommitted = index;
                    for (int f = 1; f < 3; f++) {
                        if ((ackldrecv & (1 << f)) != 0) {
                            send(t[S0 + f], new Commit(a.zxid()));
                        }
                    }
                    if (x.c == C0) {
                        send(t[C0], new Committed(a.zxid()));
                    }
                }
            } else if (m instanceof AckEpoch) {
                ackerecv |= 1 << 2;
                List<Entry> hist = new ArrayList<>();
                for (Txn x : history) {
                    hist.add(new Entry(x.zxid, x.c, x.v));
                }
                send(t[S0 + 2], new NewLeader(hist));
            } else if (m instanceof AckLd a) {
                ackldrecv |= 1 << 2;
                for (Txn x : history) {
                    if (x.zxid <= a.zxid()) {
                        x.acks |= 1 << 2;
                    }
                }
                send(t[S0 + 2], new CommitLd(lastCommitted));
            } else {
                throw new IllegalStateException("s0 got " + m);
            }
        }
    }

    /** A follower s{sid}; {@code client}: the client it serves (-1 for none); {@code late}: joins late. */
    private void follower(int sid, int client, boolean late, Tid[] t) {
        List<Entry> log = new ArrayList<>();
        int committed = 0;
        boolean synced = !late;
        List<Write> waiting = new ArrayList<>();
        if (late) {
            send(t[S0], new AckEpoch());
        }
        while (true) {
            Msg m = recv();
            if (m instanceof Write w) {
                if (synced) {
                    send(t[S0], w);
                } else {
                    waiting.add(w);
                }
            } else if (m instanceof NewLeader nl) {
                log = new ArrayList<>(nl.hist());
                int last = log.isEmpty() ? 0 : log.get(log.size() - 1).zxid();
                send(t[S0], new AckLd(last));
            } else if (m instanceof CommitLd cl) {
                while (committed < log.size() && log.get(committed).zxid() <= cl.zxid()) {
                    reply(log.get(committed), client, t);
                    committed++;
                }
                synced = true;
                for (Write w : waiting) {
                    send(t[S0], w);
                }
                waiting.clear();
            } else if (m instanceof Propose p) {
                int last = log.isEmpty() ? 0 : log.get(log.size() - 1).zxid();
                if (p.zxid() == last + 1) {
                    log.add(new Entry(p.zxid(), p.c(), p.v()));
                    send(t[S0], new Ack(p.zxid(), sid));
                }
            } else if (m instanceof Commit cm) {
                if (committed < log.size() && log.get(committed).zxid() == cm.zxid()) {
                    reply(log.get(committed), client, t);
                    committed++;
                }
            } else {
                throw new IllegalStateException("s" + sid + " got " + m);
            }
        }
    }

    private void reply(Entry e, int client, Tid[] t) {
        if (e.c() == client) {
            send(t[e.c()], new Committed(e.zxid()));
        }
    }

    @Override
    public String toString() {
        return "ZabBroadcastRemix(" + (impl ? "impl" : "spec") + ", Q=" + q + ", " + model
                + (proposeToAckld ? ", issue1-propose-to-ackld" : ", correct") + ")";
    }
}
