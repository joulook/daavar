package bench.kv_cancel;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * kv-cancel, Java side: the correct store server of {@code rust/src/imp.rs} (a request table; the
 * cancel of an in-flight Put is always accepted), with the scripted client and disk of
 * {@code rust/src/env.rs}, message for message and choice for choice.
 *
 * <p>Arguments: {@code impl}. Visible: {@code client} and {@code server}, every send and receive.
 * Model: fifo ({@code P2P}). Bad arguments: the constructor throws
 * {@link IllegalArgumentException} with the usage text, which MpLauncher reports with exit 2.
 *
 * <p>Termination (no daemons, no messages from main, nothing left unread): the client's last send
 * is {@code Shutdown}; the server then waits for an outstanding {@code Written}, sends
 * {@code Shutdown} to the disk and ends; the disk ends on {@code Shutdown}.
 */
public final class KvCancel implements MpProgram {

    /** The usage text of the constructor's arguments. */
    public static final String USAGE = "usage: bench.kv_cancel.KvCancel:impl";

    // ---- the vocabulary (msgs.rs; exported as {"t":tag,"f":[fields in order]}) ----

    /** Client to server: store {@code val} under {@code key}; request id {@code req}. */
    @Tag("Put")
    public record Put(int req, String key, int val) implements MpMessage {}

    /** Client to server: cancel request {@code req}. */
    @Tag("Cancel")
    public record Cancel(int req) implements MpMessage {}

    /** Disk to server: the write of request {@code req} is durable (internal). */
    @Tag("Written")
    public record Written(int req) implements MpMessage {}

    /** Client to server, last; server to disk, at the end. */
    @Tag("Shutdown")
    public record Shutdown() implements MpMessage {}

    /** The Put {@code req} took effect. */
    @Tag("PutOk")
    public record PutOk(int req) implements MpMessage {}

    /** The Put {@code req} was cancelled: it gets no {@code PutOk} (the server suppresses the reply). */
    @Tag("Canceled")
    public record Canceled(int req) implements MpMessage {}

    /** The cancel of request {@code req} is acknowledged. */
    @Tag("CancelOk")
    public record CancelOk(int req) implements MpMessage {}

    /** The cancel of request {@code req} is refused. */
    @Tag("CancelErr")
    public record CancelErr(int req) implements MpMessage {}

    /** Server to disk: make {@code key = val} of request {@code req} durable. */
    @Tag("Write")
    public record Write(int req, String key, int val) implements MpMessage {}

    private static final CommunicationModel M = CommunicationModel.P2P;

    /** @param args {@code impl} */
    public KvCancel(String... args) {
        if (args.length != 1 || !args[0].trim().equals("impl")) {
            throw new IllegalArgumentException("KvCancel: expected impl, got " + List.of(args) + "; " + USAGE);
        }
    }

    @Override
    public String programName() {
        return "kv-cancel";
    }

    @Override
    public String size() {
        return "impl";
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("server", "disk", "client");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("client", "server");
    }

    /**
     * The Rust twin's main spawns the server, whose thread spawns the disk and the client. JMC's
     * export requires main to create every process, so main creates all three here, starts them and
     * waits for them; each Rust spawn precedes the spawning thread's first message event, so the
     * exported words are the same.
     */
    @Override
    public void run() {
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending disk = MpProcess.create("disk");
        MpProcess.Pending client = MpProcess.create("client");
        Tid s = server.tid();
        Tid d = disk.tid();
        Tid c = client.tid();
        List<MpProcess.Handle> hs = List.of(
                server.start(() -> server(c, d)),
                disk.start(() -> disk(s)),
                client.start(() -> client(s)));
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    // ---------------------------------------------------------------- environment (env.rs)

    /**
     * The client: {@code Put{1,"k",1}}; then, by a nondeterministic choice, {@code Cancel{1}} at once
     * or no cancel. It receives until the Put has its answer and, if it cancelled, the cancel has
     * its answer; then it sends {@code Shutdown}.
     */
    private static void client(Tid server) {
        Channel.send(M, server, new Put(1, "k", 1));
        boolean cancel = Nondet.chooseBoolean();
        if (cancel) {
            Channel.send(M, server, new Cancel(1));
        }
        boolean putAnswered = false;
        boolean cancelAnswered = !cancel;
        while (!(putAnswered && cancelAnswered)) {
            MpMessage m = Channel.recv(M);
            if (m instanceof PutOk || m instanceof Canceled) {
                putAnswered = true;
            } else if (m instanceof CancelOk || m instanceof CancelErr) {
                cancelAnswered = true;
            }
        }
        Channel.send(M, server, new Shutdown());
    }

    /** The disk: on {@code Write{r,..}} it answers {@code Written{r}}; on {@code Shutdown} it ends. */
    private static void disk(Tid server) {
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Write w) {
                Channel.send(M, server, new Written(w.req()));
            } else if (m instanceof Shutdown) {
                return;
            } else {
                throw new IllegalStateException("disk: unexpected " + m);
            }
        }
    }

    // ---------------------------------------------------------------- the server (imp.rs)

    /** The server: a table of in-flight request ids, an id removed when it is answered. */
    private static void server(Tid client, Tid disk) {
        List<Integer> table = new ArrayList<>();
        int outstandingWrites = 0;
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Put p) {
                table.add(p.req());
                Channel.send(M, disk, new Write(p.req(), p.key(), p.val()));
                outstandingWrites++;
            } else if (m instanceof Written w) {
                outstandingWrites--;
                if (table.remove(Integer.valueOf(w.req()))) {
                    Channel.send(M, client, new PutOk(w.req()));
                }
                // not in the table: cancelled, no reply for the write
            } else if (m instanceof Cancel x) {
                if (table.remove(Integer.valueOf(x.req()))) {
                    Channel.send(M, client, new Canceled(x.req()));
                    Channel.send(M, client, new CancelOk(x.req()));
                } else {
                    Channel.send(M, client, new CancelErr(x.req()));
                }
            } else if (m instanceof Shutdown) {
                while (outstandingWrites > 0) {
                    if (Channel.recv(M) instanceof Written) {
                        outstandingWrites--;
                    }
                }
                Channel.send(M, disk, new Shutdown());
                return;
            } else {
                throw new IllegalStateException("server: unexpected " + m);
            }
        }
    }
}
