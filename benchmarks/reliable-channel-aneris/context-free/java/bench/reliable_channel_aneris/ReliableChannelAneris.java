package bench.reliable_channel_aneris;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
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
 * Aneris's reliable FIFO channel over UDP, twin of the Rust (TraceForge) program
 * {@code reliable-channel-aneris/rust/src/main.rs}.
 *
 * <p>Arguments: {@code <spec|impl>,<K>,<fifo|bag>[,<variant>]}; variants {@code correct} (default)
 * and {@code no-seqno} (impl side only; the spec side accepts and ignores it, as the Rust parse does).
 * Visible roles: {@code sender}, {@code receiver}. Invisible roles: {@code ep_s}, {@code ep_r}.
 */
public final class ReliableChannelAneris implements MpProgram {

    /** Losses in all. */
    private static final int L = 1;
    /** Duplications in all. */
    private static final int D = 1;

    /** The messages, one record per Rust {@code Msg} variant. */
    public sealed interface Msg extends MpMessage
            permits Send, Queued, Recv, Deliver, Data, Ack {}

    /** {@code send c v}: the sender's call. */
    @Tag("Send")
    public record Send(int v) implements Msg {}

    /** Its return. */
    @Tag("Queued")
    public record Queued() implements Msg {}

    /** {@code recv c}: the receiver's call. */
    @Tag("Recv")
    public record Recv() implements Msg {}

    /** Its return. */
    @Tag("Deliver")
    public record Deliver(int v) implements Msg {}

    /** A data packet. */
    @Tag("Data")
    public record Data(int seq, int v) implements Msg {}

    /** An acknowledgement: the next sequence id expected. */
    @Tag("Ack")
    public record Ack(int ack) implements Msg {}

    /** Inboxes (process indices): 0 sender, 1 receiver, 2 ep_s, 3 ep_r. */
    private static final int SND = 0;
    private static final int RCV = 1;
    private static final int EPS = 2;
    private static final int EPR = 3;

    private final boolean spec;
    private final int k;
    private final CommunicationModel model;
    private final String modelName;
    private final boolean seqno;

    /**
     * @param args {@code <spec|impl>, <K>, <fifo|bag> [, <variant>]}
     */
    public ReliableChannelAneris(String... args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("expected <spec|impl>,<K>,<fifo|bag>[,<variant>]");
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("unknown side " + args[0]);
        }
        k = Integer.parseInt(args[1]);
        modelName = args[2];
        model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("unknown model " + args[2]);
        };
        String variant = args.length == 4 ? args[3] : "correct";
        seqno = switch (variant) {
            case "correct" -> true;
            case "no-seqno" -> false;
            default -> throw new IllegalArgumentException("unknown variant " + variant);
        };
    }

    @Override
    public String programName() {
        return "reliable-channel-aneris";
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
        return List.of("ep_s", "ep_r", "sender", "receiver");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("sender", "receiver");
    }

    /** A process body given the table of process ids (indexed by SND, RCV, EPS, EPR). */
    private interface Body {
        void run(Tid[] t);
    }

    /** Spawns ep_s, ep_r, sender, receiver (in this order) and awaits them all. */
    @Override
    public void run() {
        Body epS = spec ? this::specEpS : this::implEpS;
        Body epR = spec ? this::specEpR : this::implEpR;
        Tid[] t = new Tid[4];
        String[] roles = {"ep_s", "ep_r", "sender", "receiver"};
        int[] slot = {EPS, EPR, SND, RCV};
        Body[] bodies = {epS, epR, this::sender, this::receiver};
        // Create all four first (task ids are minted at creation), so that every body holds
        // every process id before any of them runs, as the Rust program builds all four
        // channels before spawning; then start them in the Rust spawn order.
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Body b = bodies[i];
            MpThread th = new MpThread(() -> b.run(t), roles[i]);
            t[slot[i]] = Tid.of(th.getTaskId(), roles[i]);
            threads.add(th);
        }
        for (MpThread th : threads) {
            th.start();
        }
        for (MpThread th : threads) {
            try {
                th.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted", e);
            }
        }
    }

    private void send(Tid dst, Msg m) {
        Channel.send(model, dst, m);
    }

    private Msg recv() {
        return (Msg) Channel.recv(model);
    }

    private void sender(Tid[] t) {
        for (int v = 1; v <= k; v++) {
            send(t[EPS], new Send(v));
            if (!(recv() instanceof Queued)) {
                throw new IllegalStateException("sender");
            }
        }
    }

    private void receiver(Tid[] t) {
        for (int i = 0; i < k; i++) {
            send(t[EPR], new Recv());
            if (!(recv() instanceof Deliver)) {
                throw new IllegalStateException("receiver");
            }
        }
    }

    /** Spec ep_s: answer each call, forward the value once with its position. */
    private void specEpS(Tid[] t) {
        for (int seq = 0; seq < k; seq++) {
            if (!(recv() instanceof Send s)) {
                throw new IllegalStateException("spec ep_s");
            }
            send(t[SND], new Queued());
            send(t[EPR], new Data(seq, s.v()));
        }
    }

    /** Spec ep_r: deliver the values in position order, one per call. */
    private void specEpR(Tid[] t) {
        Integer[] buf = new Integer[k];
        int next = 0;
        int calls = 0;
        while (next < k) {
            Msg m = recv();
            if (m instanceof Data d) {
                buf[d.seq()] = d.v();
            } else if (m instanceof Recv) {
                calls++;
            } else {
                throw new IllegalStateException("spec ep_r got " + m);
            }
            while (calls > 0 && next < k && buf[next] != null) {
                send(t[RCV], new Deliver(buf[next]));
                next++;
                calls--;
            }
        }
    }

    /** Impl ep_s: send buffer, window 1, lossy and duplicating transmission. */
    private void implEpS(Tid[] t) {
        List<Integer> sbuf = new ArrayList<>();
        int sidLb = 0;
        int queued = 0;
        boolean inFlight = false;
        int[] budget = {L, D}; // losses, dups
        while (queued < k || !sbuf.isEmpty()) {
            Msg m = recv();
            if (m instanceof Send s) {
                send(t[SND], new Queued());
                sbuf.add(s.v());
                queued++;
            } else if (m instanceof Ack a) {
                int ack = a.ack();
                if (ack > sidLb) {
                    int n = Math.min(ack - sidLb, sbuf.size());
                    sbuf.subList(0, n).clear();
                    sidLb = ack;
                    inFlight = false;
                }
            } else {
                throw new IllegalStateException("ep_s got " + m);
            }
            if (!inFlight && !sbuf.isEmpty()) {
                xmit(t, sidLb, sbuf.get(0), budget);
                inFlight = true;
            }
        }
    }

    private void xmit(Tid[] t, int seq, int v, int[] budget) {
        // a lost transmission is retransmitted when the timer fires (at most L losses)
        while (budget[0] > 0 && Nondet.chooseBoolean()) {
            budget[0]--;
        }
        send(t[EPR], new Data(seq, v));
        if (budget[1] > 0 && Nondet.chooseBoolean()) {
            budget[1]--;
            send(t[EPR], new Data(seq, v));
        }
    }

    /** Impl ep_r (process_data_on_chan + recv); without seqno the id is ignored. */
    private void implEpR(Tid[] t) {
        ArrayDeque<Integer> rbuf = new ArrayDeque<>();
        int ackId = 0;
        int calls = 0;
        int delivered = 0;
        while (delivered < k) {
            Msg m = recv();
            if (m instanceof Data d) {
                if (!seqno || d.seq() == ackId) {
                    rbuf.addLast(d.v());
                    ackId++;
                }
                send(t[EPS], new Ack(ackId));
            } else if (m instanceof Recv) {
                calls++;
            } else {
                throw new IllegalStateException("ep_r got " + m);
            }
            while (calls > 0 && !rbuf.isEmpty() && delivered < k) {
                send(t[RCV], new Deliver(rbuf.pollFirst()));
                calls--;
                delivered++;
            }
        }
    }

    @Override
    public String toString() {
        return "reliable-channel-aneris(" + (spec ? "spec" : "impl") + ", K=" + k + ", " + modelName
                + (spec ? "" : seqno ? ", correct" : ", no-seqno") + ")";
    }
}
