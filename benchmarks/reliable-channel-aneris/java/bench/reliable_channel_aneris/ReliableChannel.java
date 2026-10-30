package bench.reliable_channel_aneris;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Aneris's reliable FIFO channel over a lossy, duplicating, reordering link (candidate N12),
 * against a FIFO relay: the Java twin of {@code rust/src/main.rs} (same threads, spawn order,
 * messages and choices; Rust's per-thread inbox channels are thread mailboxes here, the ids
 * minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>,<variant>}; K messages. Variants:
 * {@code correct}, {@code no-seqno}. Visible: {@code sender}, {@code receiver}.
 */
public final class ReliableChannel implements MpProgram {

    /** The base of every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage permits Send, Queued, Recv, Deliver, Data, Ack {}

    /** {@code send c v}. */
    @Tag("Send")
    public record Send(int v) implements Msg {}

    /** Its return. */
    @Tag("Queued")
    public record Queued() implements Msg {}

    /** {@code recv c}. */
    @Tag("Recv")
    public record Recv() implements Msg {}

    /** Its return. */
    @Tag("Deliver")
    public record Deliver(int v) implements Msg {}

    /** A data packet. */
    @Tag("Data")
    public record Data(int seq, int v) implements Msg {}

    /** An acknowledgement: the next id expected. */
    @Tag("Ack")
    public record Ack(int ack) implements Msg {}

    private static final int L = 1;
    private static final int D = 1;

    private final boolean impl;
    private final int k;
    private final CommunicationModel m;
    private final boolean seqno;

    /**
     * @param args side, K, model, variant
     */
    public ReliableChannel(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("ReliableChannel takes <spec|impl>,<K>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.k = Integer.parseInt(args[1].trim());
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.seqno = switch (v) {
            case "correct" -> true;
            case "no-seqno" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "reliable-channel-aneris-" + (impl ? (seqno ? "correct" : "no-seqno") : "spec");
    }

    @Override
    public String size() {
        return "k" + k + "l" + L + "d" + D;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("ep_s", "ep_r", "sender", "receiver");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("sender", "receiver");
    }

    private Msg recv() {
        return (Msg) Channel.recv(m);
    }

    /** Mailbox indices, as the Rust inboxes: 0 sender, 1 receiver, 2 ep_s, 3 ep_r. */
    private static final int SND = 0;
    private static final int RCV = 1;
    private static final int EPS = 2;
    private static final int EPR = 3;

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[4];
        t[EPS] = Tid.of(ts.add("ep_s", () -> {
            if (impl) {
                implEpS(t);
            } else {
                specEpS(t);
            }
        }).getTaskId());
        t[EPR] = Tid.of(ts.add("ep_r", () -> {
            if (impl) {
                implEpR(t);
            } else {
                specEpR(t);
            }
        }).getTaskId());
        t[SND] = Tid.of(ts.add("sender", () -> sender(t)).getTaskId());
        t[RCV] = Tid.of(ts.add("receiver", () -> receiver(t)).getTaskId());
        ts.startAndAwaitAll();
    }

    private void sender(Tid[] t) {
        for (int v = 1; v <= k; v++) {
            Channel.send(m, t[EPS], new Send(v));
            if (!(recv() instanceof Queued)) {
                throw new IllegalStateException("sender");
            }
        }
    }

    private void receiver(Tid[] t) {
        for (int i = 0; i < k; i++) {
            Channel.send(m, t[EPR], new Recv());
            if (!(recv() instanceof Deliver)) {
                throw new IllegalStateException("receiver");
            }
        }
    }

    private void specEpS(Tid[] t) {
        for (int seq = 0; seq < k; seq++) {
            Send s = (Send) recv();
            Channel.send(m, t[SND], new Queued());
            Channel.send(m, t[EPR], new Data(seq, s.v()));
        }
    }

    private void specEpR(Tid[] t) {
        Integer[] buf = new Integer[k];
        int next = 0;
        int calls = 0;
        while (next < k) {
            Msg x = recv();
            if (x instanceof Data d) {
                buf[d.seq()] = d.v();
            } else if (x instanceof Recv) {
                calls++;
            } else {
                throw new IllegalStateException("spec ep_r got " + x);
            }
            while (calls > 0 && next < k && buf[next] != null) {
                Channel.send(m, t[RCV], new Deliver(buf[next]));
                next++;
                calls--;
            }
        }
    }

    /** A transmission; {@code budget[0]} losses and {@code budget[1]} duplicates left. */
    private void xmit(Tid[] t, int seq, int v, int[] budget) {
        while (budget[0] > 0 && Nondet.chooseBoolean()) {
            budget[0]--;
        }
        Channel.send(m, t[EPR], new Data(seq, v));
        if (budget[1] > 0 && Nondet.chooseBoolean()) {
            budget[1]--;
            Channel.send(m, t[EPR], new Data(seq, v));
        }
    }

    private void implEpS(Tid[] t) {
        List<Integer> sbuf = new ArrayList<>();
        int sidLb = 0;
        int queued = 0;
        boolean inFlight = false;
        int[] budget = {L, D};
        while (queued < k || !sbuf.isEmpty()) {
            Msg x = recv();
            if (x instanceof Send s) {
                Channel.send(m, t[SND], new Queued());
                sbuf.add(s.v());
                queued++;
            } else if (x instanceof Ack a) {
                if (a.ack() > sidLb) {
                    int drop = Math.min(a.ack() - sidLb, sbuf.size());
                    sbuf.subList(0, drop).clear();
                    sidLb = a.ack();
                    inFlight = false;
                }
            } else {
                throw new IllegalStateException("ep_s got " + x);
            }
            if (!inFlight && !sbuf.isEmpty()) {
                xmit(t, sidLb, sbuf.get(0), budget);
                inFlight = true;
            }
        }
    }

    private void implEpR(Tid[] t) {
        ArrayDeque<Integer> rbuf = new ArrayDeque<>();
        int ackId = 0;
        int calls = 0;
        int delivered = 0;
        while (delivered < k) {
            Msg x = recv();
            if (x instanceof Data d) {
                if (!seqno || d.seq() == ackId) {
                    rbuf.addLast(d.v());
                    ackId++;
                }
                Channel.send(m, t[EPS], new Ack(ackId));
            } else if (x instanceof Recv) {
                calls++;
            } else {
                throw new IllegalStateException("ep_r got " + x);
            }
            while (calls > 0 && !rbuf.isEmpty() && delivered < k) {
                Channel.send(m, t[RCV], new Deliver(rbuf.pollFirst()));
                calls--;
                delivered++;
            }
        }
    }
}
