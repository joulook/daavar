package bench.a3_lock;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A3 lock service (context-free twin of the Rust a3-lock program).
 *
 * <p>Arguments: {@code <spec|impl>,<k>,<q>,<fifo|bag>[,<variant>]}; variant one of fifo,
 * double-grant, never-grant, wrong-round, control-lifo (required for impl, validated if given
 * for spec, where it has no effect).
 *
 * <p>Visible roles: {@code c0 .. c{k-1}}. Invisible: {@code lock} (spec) or {@code coordinator}
 * (impl), spawned first; main is invisible, spawns every process and awaits them all.
 */
public final class A3Lock implements MpProgram {

    /** Rust {@code enum Msg}. */
    public sealed interface Msg extends MpMessage permits Acq, Rel {}

    /** Rust {@code Msg::Acq(usize)}. */
    @Tag("Acq")
    public record Acq(long i) implements Msg {}

    /** Rust {@code Msg::Rel(usize)}. */
    @Tag("Rel")
    public record Rel(long i) implements Msg {}

    /** Rust {@code struct Grant(usize, usize)}. */
    @Tag("Grant")
    public record Grant(long i, long j) implements MpMessage {}

    /** Rust {@code A3Coord}. */
    enum Kind { FIFO, DOUBLE_GRANT, NEVER_GRANT, WRONG_ROUND, LIFO }

    private final boolean spec;
    private final int k;
    private final int q;
    private final CommunicationModel model;
    private final Kind kind;
    private final String args;

    public A3Lock(String... a) {
        if (a.length != 4 && a.length != 5) {
            throw new IllegalArgumentException(
                    "a3-lock: expected <spec|impl>,<k>,<q>,<fifo|bag>[,<variant>], got " + String.join(",", a));
        }
        switch (a[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("a3-lock: first argument must be spec or impl, got " + a[0]);
        }
        k = Integer.parseInt(a[1]);
        q = Integer.parseInt(a[2]);
        if (k < 1 || q < 0) {
            throw new IllegalArgumentException("a3-lock: need k >= 1 and q >= 0");
        }
        model = switch (a[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("a3-lock: model must be fifo or bag, got " + a[3]);
        };
        if (a.length == 5) {
            kind = switch (a[4]) {
                case "fifo" -> Kind.FIFO;
                case "double-grant" -> Kind.DOUBLE_GRANT;
                case "never-grant" -> Kind.NEVER_GRANT;
                case "wrong-round" -> Kind.WRONG_ROUND;
                case "control-lifo" -> Kind.LIFO;
                default -> throw new IllegalArgumentException("a3-lock: unknown variant " + a[4]);
            };
        } else if (spec) {
            kind = null;
        } else {
            throw new IllegalArgumentException("a3-lock: impl needs a variant");
        }
        args = String.join(",", a);
    }

    @Override
    public String programName() {
        return "a3-lock";
    }

    @Override
    public String size() {
        return k + " " + q;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    private String server() {
        return spec ? "lock" : "coordinator";
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        r.add(server());
        r.addAll(visibleRoles());
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            r.add("c" + i);
        }
        return r;
    }

    @Override
    public void run() {
        // Per-iteration addressing: client i writes its own Tid here before its first send, so
        // the server reads entry i only after receiving a message from client i.
        AtomicReferenceArray<Tid> clientTid = new AtomicReferenceArray<>(k);
        List<MpProcess.Handle> handles = new ArrayList<>();
        MpProcess.Handle server = MpProcess.spawn(server(), spec ? lockBody(clientTid) : coordinatorBody(clientTid));
        handles.add(server);
        Tid serverTid = server.tid();
        for (int i = 0; i < k; i++) {
            final int me = i;
            handles.add(MpProcess.spawn("c" + i, () -> {
                clientTid.set(me, MpProcess.self());
                for (int r = 0; r < q; r++) {
                    Channel.send(model, serverTid, new Acq(me));
                    Channel.recv(model, Grant.class);
                    Channel.send(model, serverTid, new Rel(me));
                }
            }));
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    /** Rust {@code a3_spec}'s {@code lock}: acquire channel and release channel. */
    private Runnable lockBody(AtomicReferenceArray<Tid> clientTid) {
        return () -> {
            long[] round = new long[k];
            for (int n = 0; n < k * q; n++) {
                Acq a = Channel.recv(model, Acq.class);
                int i = (int) a.i();
                Channel.send(model, clientTid.get(i), new Grant(i, round[i]));
                round[i] += 1;
                Channel.recv(model, Rel.class);
            }
        };
    }

    /** Rust {@code a3_impl}'s {@code coordinator}: one mailbox and a waiter queue. */
    private Runnable coordinatorBody(AtomicReferenceArray<Tid> clientTid) {
        return () -> {
            long[] round = new long[k];
            boolean held = false;
            ArrayDeque<Integer> queue = new ArrayDeque<>();
            for (int n = 0; n < 2 * k * q; n++) {
                Msg m = Channel.recv(model, Msg.class);
                if (m instanceof Acq a) {
                    int i = (int) a.i();
                    if (!held || kind == Kind.DOUBLE_GRANT) {
                        held = true;
                        grant(i, round, clientTid);
                    } else {
                        queue.addLast(i);
                    }
                } else {
                    Integer next = kind == Kind.LIFO ? queue.pollLast() : queue.pollFirst();
                    if (next != null && kind == Kind.NEVER_GRANT && next == k - 1) {
                        held = false;
                    } else if (next != null) {
                        grant(next, round, clientTid);
                    } else {
                        held = false;
                    }
                }
            }
        };
    }

    private void grant(int i, long[] round, AtomicReferenceArray<Tid> clientTid) {
        long j = (kind == Kind.WRONG_ROUND && i == k - 1) ? round[i] + 1 : round[i];
        Channel.send(model, clientTid.get(i), new Grant(i, j));
        round[i] += 1;
    }

    @Override
    public String toString() {
        return "A3Lock(" + args + ")";
    }
}
