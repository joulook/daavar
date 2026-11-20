package bench.hermes;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hermes (Katsarakis et al., ASPLOS 2020, section 3.2) for one replicated integer key: reads and
 * writes only, no faults, no RMWs, no replays. N replicas r0..r{N-1}, K clients c0..c{K-1}.
 * Constructed from {@code impl,<N>,<K>,<fifo|bag>}. See NOTES.md for the mapping to the paper.
 */
public final class Hermes implements MpProgram {

    // ---- client <-> replica messages (shape fixed by INTERFACE.md) ----

    @Tag("Write")
    public record Write(int from, int value) implements MpMessage {}

    @Tag("WriteAck")
    public record WriteAck() implements MpMessage {}

    @Tag("Read")
    public record Read(int from) implements MpMessage {}

    @Tag("ReadReply")
    public record ReadReply(int value) implements MpMessage {}

    // ---- replica <-> replica messages (paper, Figure 3) ----

    /** The logical timestamp [v, cid], ordered lexicographically (paper, footnote 5). */
    @Tag("TS")
    public record Ts(int version, int cid) {
        boolean higherThan(Ts o) {
            return version > o.version || (version == o.version && cid > o.cid);
        }
    }

    /** INV: the key (implicit, one key), the new timestamp and the value; {@code from} = sender. */
    @Tag("INV")
    public record Inv(Ts ts, int value, int from) implements MpMessage {}

    /** ACK: the same timestamp as the INV it answers; {@code from} = the acknowledging follower. */
    @Tag("ACK")
    public record Ack(Ts ts, int from) implements MpMessage {}

    /** VAL: the key (implicit) and the timestamp of the completed write. */
    @Tag("VAL")
    public record Val(Ts ts) implements MpMessage {}

    /** The paper's key states (section 3.2); REPLAY is never entered (no replays, see NOTES.md). */
    enum KeyState { VALID, INVALID, WRITE, REPLAY, TRANS }

    private final int n;
    private final int k;
    private final CommunicationModel model;
    private final String modelName;

    public Hermes(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<N>,<K>,<fifo|bag>");
        }
        n = Integer.parseInt(args[1]);
        k = Integer.parseInt(args[2]);
        if (n < 1 || k < 1) {
            throw new IllegalArgumentException("N and K must be at least 1");
        }
        modelName = args[3];
        switch (modelName) {
            case "fifo" -> model = CommunicationModel.P2P;
            case "bag" -> model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + modelName);
        }
    }

    @Override
    public String programName() {
        return "hermes";
    }

    @Override
    public String size() {
        return n + "," + k;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int j = 0; j < n; j++) {
            roles.add("r" + j);
        }
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    /** Main: creates every process (so all addresses are known before any runs), starts, awaits. */
    @Override
    public void run() {
        Tid[] replicas = new Tid[n];
        Tid[] clients = new Tid[k];
        List<MpThread> threads = new ArrayList<>();
        for (int j = 0; j < n; j++) {
            final int id = j;
            MpThread t = new MpThread(() -> new Replica(id, replicas, clients).run(), "r" + j);
            replicas[j] = Tid.of(t.getTaskId(), "r" + j);
            threads.add(t);
        }
        for (int i = 0; i < k; i++) {
            final int id = i;
            MpThread t = new MpThread(() -> client(id, replicas), "c" + i);
            clients[i] = Tid.of(t.getTaskId(), "c" + i);
            threads.add(t);
        }
        for (MpThread t : threads) {
            t.start();
        }
        for (MpThread t : threads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** Client ci: Write{i+1} to r(i mod N), await WriteAck; Read{} to r((i+1) mod N), await ReadReply. */
    private void client(int i, Tid[] replicas) {
        Channel.send(model, replicas[i % n], new Write(i, i + 1));
        Channel.recv(model, WriteAck.class);
        Channel.send(model, replicas[(i + 1) % n], new Read(i));
        Channel.recv(model, ReadReply.class);
    }

    /** A client request that the replica has received but not yet served (stalled). */
    private record Request(boolean isWrite, int from, int value) {}

    /** One replica's state for the key and its event loop. Fresh per iteration. */
    private final class Replica {
        final int id;
        final Tid[] replicas;
        final Tid[] clients;

        KeyState state = KeyState.VALID;
        Ts ts = new Ts(0, 0);
        int value = 0;

        /** The timestamp of this replica's own write in flight (CINV sent, not all ACKs), or null. */
        Ts pending = null;
        /** The client whose write is in flight. */
        int pendingClient = -1;
        final Set<Integer> acks = new HashSet<>();

        /** Requests stalled because the key is not Valid (or a write of ours is in flight). */
        final Deque<Request> stalled = new ArrayDeque<>();

        Replica(int id, Tid[] replicas, Tid[] clients) {
            this.id = id;
            this.replicas = replicas;
            this.clients = clients;
        }

        /** How many messages this replica receives in every run (no faults, no replays). */
        int expectedMessages() {
            int ownWrites = 0;
            int reads = 0;
            for (int i = 0; i < k; i++) {
                if (i % n == id) {
                    ownWrites++;
                }
                if ((i + 1) % n == id) {
                    reads++;
                }
            }
            // Writes + Reads from clients, an ACK per follower per own write, INV + VAL per other write.
            return ownWrites + reads + ownWrites * (n - 1) + 2 * (k - ownWrites);
        }

        void run() {
            int total = expectedMessages();
            for (int received = 0; received < total; received++) {
                MpMessage m = Channel.recv(model);
                if (m instanceof Write w) {
                    stalled.addLast(new Request(true, w.from(), w.value()));
                } else if (m instanceof Read rd) {
                    stalled.addLast(new Request(false, rd.from(), 0));
                } else if (m instanceof Inv inv) {
                    onInv(inv);
                } else if (m instanceof Ack ack) {
                    onAck(ack);
                } else if (m instanceof Val val) {
                    onVal(val);
                } else {
                    throw new IllegalStateException("r" + id + ": unexpected message " + m);
                }
                serveStalled();
            }
            if (!stalled.isEmpty() || pending != null || state != KeyState.VALID) {
                throw new IllegalStateException("r" + id + " ended with state " + state + ", pending "
                        + pending + ", stalled " + stalled);
            }
        }

        /** Serves stalled requests in arrival order while the key is Valid and no own write is in flight. */
        void serveStalled() {
            while (!stalled.isEmpty() && state == KeyState.VALID && pending == null) {
                Request r = stalled.pollFirst();
                if (r.isWrite()) {
                    coordinate(r.from(), r.value());
                } else {
                    // Reads: served locally, only in the Valid state.
                    Channel.send(model, clients[r.from()], new ReadReply(value));
                }
            }
        }

        /** CTS and CINV. */
        void coordinate(int client, int newValue) {
            // CTS: increment the version, append our node id as cid.
            ts = new Ts(ts.version() + 1, id);
            // CINV: apply the value locally, go to Write, broadcast INV to the followers.
            value = newValue;
            state = KeyState.WRITE;
            pending = ts;
            pendingClient = client;
            acks.clear();
            for (int j = 0; j < n; j++) {
                if (j != id) {
                    Channel.send(model, replicas[j], new Inv(ts, newValue, id));
                }
            }
            if (n == 1) {
                complete();
            }
        }

        /** FINV and FACK. */
        void onInv(Inv inv) {
            if (inv.ts().higherThan(ts)) {
                // Invalid, or Trans if this replica has a write of its own in flight.
                state = pending != null ? KeyState.TRANS : KeyState.INVALID;
                ts = inv.ts();
                value = inv.value();
            }
            Channel.send(model, replicas[inv.from()], new Ack(inv.ts(), id));
        }

        /** CACK: counts ACKs carrying the timestamp of the write in flight. */
        void onAck(Ack ack) {
            if (pending != null && ack.ts().equals(pending)) {
                acks.add(ack.from());
                if (acks.size() == n - 1) {
                    complete();
                }
            }
        }

        /** CACK completion, the client's acknowledgement, then CVAL. */
        void complete() {
            Ts done = pending;
            pending = null;
            if (state == KeyState.TRANS) {
                state = KeyState.INVALID;
            } else {
                // Write -> Valid; Valid (validated by a higher write's VAL meanwhile) stays Valid.
                state = KeyState.VALID;
            }
            Channel.send(model, clients[pendingClient], new WriteAck());
            for (int j = 0; j < n; j++) {
                if (j != id) {
                    Channel.send(model, replicas[j], new Val(done));
                }
            }
        }

        /** FVAL: Valid iff the timestamp equals the local one; otherwise ignored. */
        void onVal(Val val) {
            if (val.ts().equals(ts)) {
                state = KeyState.VALID;
            }
        }
    }

    @Override
    public String toString() {
        return "Hermes(impl, N=" + n + ", K=" + k + ", " + modelName + ")";
    }
}
