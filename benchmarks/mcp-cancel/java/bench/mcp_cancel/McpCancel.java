package bench.mcp_cancel;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
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
 * MCP cancel benchmark, Java side: the server core of modelcontextprotocol/java-sdk at
 * 779893c0706b01ad565e8768b4a1dbe4367eb058 (modern stack, stdio), rewritten as one sequential
 * Must process, with the scripted client and two tool handlers of the shared environment.
 *
 * <p>{@code S/} = {@code mcp-core/src/main/java/io/modelcontextprotocol/modern/server/}; SMT =
 * {@code S/transport/StdioMcpTransport.java}, MS = {@code S/McpServer.java}, DAN =
 * {@code S/DefaultAsyncNotifier.java}. On stdio the cancellation lives in SMT: {@code McpServer}
 * ignores notifications (MS 112-117).
 *
 * <p>Arguments: {@code impl[,<h1><h2>]}: handler1's branch {@code a}, {@code b}, {@code c} or
 * {@code x} (any of the three), handler2's {@code a}, {@code b} or {@code x} (either); default
 * {@code xx}. Visible: {@code client} and {@code server}, every send and receive. Model: fifo.
 * The messages, the client and the handlers mirror the Rust twin's ({@code msgs.rs},
 * {@code env.rs}) message for message and choice for choice. The client's option to cancel
 * request 2 ({@code cancel2}) is not offered: the server writes request 2's items without a
 * separate check, which holds only while request 2 is never cancelled.
 *
 * <p>Termination (no daemons, no messages from main): the client's last send is {@code Shutdown};
 * on it the server sends {@code Shutdown} to both handlers, writes nothing else and ends; each
 * handler ends on {@code Shutdown}. The SDK instead ends on stdin EOF, after which its writer may
 * still write queued items; the harness does not model those writes.
 */
public final class McpCancel implements MpProgram {

    // ---- the vocabulary (msgs.rs; exported as {"t":tag,"f":[fields in order]}) ----

    /** Client to server, last (stdin closed); server to each handler, at the end. */
    @Tag("Shutdown")
    public record Shutdown() implements MpMessage {}

    /** {@code tools/call} with id {@code req} and {@code _meta.progressToken = token}. */
    @Tag("Call")
    public record Call(int req, String token) implements MpMessage {}

    /** {@code notifications/cancelled} with {@code requestId = req}. */
    @Tag("Cancelled")
    public record Cancelled(int req) implements MpMessage {}

    /** Handler to server: progress {@code value} of request {@code req} (internal). */
    @Tag("Prog")
    public record Prog(int req, String token, int value) implements MpMessage {}

    /** Handler to server: the tool of request {@code req} returned successfully (internal). */
    @Tag("Done")
    public record Done(int req) implements MpMessage {}

    /** {@code notifications/progress} with {@code progressToken = token}, {@code progress = value}. */
    @Tag("Progress")
    public record Progress(String token, int value) implements MpMessage {}

    /** The result response to request {@code req}. */
    @Tag("Result")
    public record Result(int req) implements MpMessage {}

    /** An error response to request {@code req}; the code is exported as "*". No path here sends one. */
    @Tag("Error")
    public record Error(int req, String code) implements MpMessage {}

    /** Server to handler: run the tool of request {@code req}. */
    @Tag("Work")
    public record Work(int req, String token) implements MpMessage {}

    /** Server to handler: the cancellation signal of request {@code req}. */
    @Tag("Stop")
    public record Stop(int req) implements MpMessage {}

    /** A handler's branch; {@code ANY} = chosen nondeterministically. */
    public enum Branch { A, B, C, ANY }

    private static final CommunicationModel M = CommunicationModel.P2P;

    /** The progress token of request {@code r}: the client names it {@code t<r>}. */
    static String token(int r) {
        return "t" + r;
    }

    private final Branch h1Branch;
    private final Branch h2Branch;
    private final String variant;

    /** @param args {@code impl}, optionally {@code <h1><h2>} */
    public McpCancel(String... args) {
        if (args.length < 1 || args.length > 2 || !args[0].trim().equals("impl")) {
            throw new IllegalArgumentException("McpCancel takes impl[,<a|b|c|x><a|b|x>]");
        }
        this.variant = args.length == 2 ? args[1].trim() : "xx";
        if (variant.length() != 2) {
            throw new IllegalArgumentException("the variant must be two letters, was " + variant);
        }
        this.h1Branch = branch(variant.charAt(0), true);
        this.h2Branch = branch(variant.charAt(1), false);
    }

    private static Branch branch(char c, boolean third) {
        return switch (c) {
            case 'a' -> Branch.A;
            case 'b' -> Branch.B;
            case 'c' -> {
                if (!third) {
                    throw new IllegalArgumentException("handler2 has no branch c");
                }
                yield Branch.C;
            }
            case 'x' -> Branch.ANY;
            default -> throw new IllegalArgumentException("bad branch " + c);
        };
    }

    @Override
    public String programName() {
        return "mcp-cancel";
    }

    @Override
    public String size() {
        return variant;
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("server", "handler1", "handler2", "client");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("client", "server");
    }

    /**
     * The Rust twin's main spawns the server, whose thread spawns handler1, handler2 and the client.
     * JMC's export requires main to create every process, so main creates all four here, starts
     * them and waits for them; each Rust spawn comes before the spawning thread's first message
     * event, so the exported words are the same.
     */
    @Override
    public void run() {
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending handler1 = MpProcess.create("handler1");
        MpProcess.Pending handler2 = MpProcess.create("handler2");
        MpProcess.Pending client = MpProcess.create("client");
        Tid s = server.tid();
        Tid h1 = handler1.tid();
        Tid h2 = handler2.tid();
        Tid c = client.tid();
        List<MpProcess.Handle> hs = List.of(
                server.start(() -> new Server(c, h1, h2).run()),
                handler1.start(() -> handler(s, true, h1Branch)),
                handler2.start(() -> handler(s, false, h2Branch)),
                client.start(() -> client(s, false)));
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    // ---------------------------------------------------------------- environment (env.rs)

    /** The request a client-bound message belongs to (progress: the request of its token). */
    private static int reqOf(MpMessage m) {
        if (m instanceof Progress p) {
            return Integer.parseInt(p.token().substring(1));
        }
        if (m instanceof Result r) {
            return r.req();
        }
        return ((Error) m).req();
    }

    /**
     * The client: the host whose model decides, as a nondeterministic procedure. It sends
     * {@code Call{1,"t1"}}, then chooses whether to send {@code Call{2,"t2"}}. Before reading anything,
     * and after each message it reads while something is outstanding, it may cancel request 1 while
     * that request has no response, at most once; with {@code cancel2}, likewise request 2 if it was
     * sent. It stops once every request it sent has a response or its cancel, then sends
     * {@code Shutdown}. A message read before the stop has no effect beyond marking a response.
     */
    private static void client(Tid server, boolean cancel2) {
        Channel.send(M, server, new Call(1, token(1)));
        boolean sent2 = Nondet.chooseBoolean();
        if (sent2) {
            Channel.send(M, server, new Call(2, token(2)));
        }
        boolean[] sent = {true, sent2};
        boolean[] cancellable = {true, sent2 && cancel2};
        boolean[] answered = {false, false};
        boolean[] cancelled = {false, false};
        maybeCancel(server, cancellable, answered, cancelled);
        while (outstanding(sent, answered, cancelled)) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Result || m instanceof Error) {
                answered[reqOf(m) - 1] = true;
            }
            if (outstanding(sent, answered, cancelled)) {
                maybeCancel(server, cancellable, answered, cancelled);
            }
        }
        Channel.send(M, server, new Shutdown());
    }

    private static boolean outstanding(boolean[] sent, boolean[] answered, boolean[] cancelled) {
        for (int i = 0; i < 2; i++) {
            if (sent[i] && !answered[i] && !cancelled[i]) {
                return true;
            }
        }
        return false;
    }

    private static void maybeCancel(Tid server, boolean[] cancellable, boolean[] answered, boolean[] cancelled) {
        for (int i = 0; i < 2; i++) {
            if (cancellable[i] && !answered[i] && !cancelled[i] && Nondet.chooseBoolean()) {
                Channel.send(M, server, new Cancelled(i + 1));
                cancelled[i] = true;
            }
        }
    }

    /**
     * A tool handler. On {@code Work{r,t}}: {@code Prog{r,t,1}}, then branch (a) {@code Done{r}},
     * (b) {@code Prog{r,t,2}} and {@code Done{r}}, or (c, handler1 only) nothing more. {@code Stop}:
     * nothing. {@code Shutdown}: it ends.
     */
    private static void handler(Tid server, boolean first, Branch branch) {
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Work w) {
                Channel.send(M, server, new Prog(w.req(), w.token(), 1));
                Branch b = branch;
                if (b == Branch.ANY) {
                    b = first ? new Branch[] {Branch.A, Branch.B, Branch.C}[Nondet.chooseIndex(3)]
                            : (Nondet.chooseBoolean() ? Branch.A : Branch.B);
                }
                if (b == Branch.A) {
                    Channel.send(M, server, new Done(w.req()));
                } else if (b == Branch.B) {
                    Channel.send(M, server, new Prog(w.req(), w.token(), 2));
                    Channel.send(M, server, new Done(w.req()));
                }
            } else if (m instanceof Shutdown) {
                return;
            }
            // Stop: nothing
        }
    }

    // ---------------------------------------------------------------- the server

    /** The state of a request id. */
    private enum St {
        /** Not in the in-flight map, never seen. */
        ABSENT,
        /** In the map (SMT 233-237). */
        ACTIVE,
        /** Removed by its response before the response was queued (SMT 270-274). */
        ANSWERED,
        /** Removed by a cancel whose flag is not yet set (between SMT 224 and SMT 227). */
        CANCELLING,
        /** Removed by a cancel, flag set and subscription disposed (SMT 227-228). */
        CANCELLED
    }

    /** A queued write: the message and the request whose cancelled flag the writer checks. */
    private record Item(int req, MpMessage msg) {}

    /** The micro-actions a flush may take inside a step. */
    private enum Act { HALT, WRITER, FLAG, HANDLE, ENQ1, ENQ2 }

    /**
     * The SDK's server side on stdio: the reader ({@code handleLine}, SMT 187-217), the in-flight
     * map and cancel (SMT 219-277), the per-request subscriber callbacks (SMT 269-276), the
     * notifier (DAN) and the single FIFO writer (SMT 72-76, 294-322), in one process.
     *
     * <p>One step = one blocking receive, then a flush. In the SDK the reader, the request workers
     * and the writer run concurrently; here their actions that are not receives happen at
     * nondeterministic points of a flush:
     * <ul>
     *   <li>HANDLE: the reader's handling of the client line it has read (a request is registered
     *       and subscribed, SMT 232-268; a cancel removes the entry, SMT 224). Until then the
     *       reader reads no further line, while handler output still arrives.</li>
     *   <li>FLAG: the cancel's second half, flag (SMT 227) and dispose (SMT 228). The reader reads
     *       no further line until it has happened.</li>
     *   <li>ENQ1, ENQ2: a response's callback removes its entry (SMT 273) when the response
     *       arrives; queueing the response for the writer (SMT 275, 296) follows later.</li>
     *   <li>WRITER: the writer's check of the head (SMT 299-302) or its write (SMT 303).</li>
     * </ul>
     * No non-blocking receive is used. A flush may stop with work left only when a further message
     * to the server is certain to arrive (see {@link #certain}); otherwise it continues, so no
     * execution blocks for want of a writer or a reader action.
     */
    private final class Server {
        private final Tid client;
        private final Tid[] handler;

        private final St[] st = {St.ABSENT, St.ABSENT};
        private final String[] tok = new String[2];
        /** DAN 29: the last progress value per request, initially negative infinity. */
        private final double[] last = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
        /** The writer's queue (SMT 76, 296): FIFO over all requests. */
        private final ArrayDeque<Item> queue = new ArrayDeque<>();
        /** The item the writer has checked (SMT 299) but not yet written (SMT 303), or null. */
        private Item checked;
        /** The client line the reader has read and not yet handled, or null. */
        private MpMessage pending;
        /** A response whose entry is removed (SMT 273) and that is not yet queued (SMT 275). */
        private final boolean[] enq = {false, false};

        // What the server has seen and written, for the certainty of a further message.
        private int calls;
        private final boolean[] gotCall = {false, false};
        private final boolean[] gotCancel = {false, false};
        private final boolean[] wroteResult = {false, false};
        private boolean gotProg1;
        private boolean gotDone2;

        Server(Tid client, Tid h1, Tid h2) {
            this.client = client;
            this.handler = new Tid[] {h1, h2};
        }

        private int cancelling() {
            for (int i = 0; i < 2; i++) {
                if (st[i] == St.CANCELLING) {
                    return i + 1;
                }
            }
            return 0;
        }

        /** The reader is inside a line: read and not yet handled, or a cancel not yet flagged. */
        private boolean readerBusy() {
            return pending != null || cancelling() != 0;
        }

        void run() {
            while (true) {
                MpMessage m = readerBusy()
                        ? Channel.recv(M, x -> x instanceof Prog || x instanceof Done)
                        : Channel.recv(M);
                if (m instanceof Shutdown) {
                    // stdin EOF: the server ends; nothing further is written (harness rule).
                    Channel.send(M, handler[0], new Shutdown());
                    Channel.send(M, handler[1], new Shutdown());
                    return;
                }
                if (m instanceof Call || m instanceof Cancelled) {
                    pending = m; // read by the reader; handled at a later point (HANDLE)
                    if (m instanceof Call c) {
                        calls++;
                        gotCall[c.req() - 1] = true;
                    } else {
                        gotCancel[((Cancelled) m).req() - 1] = true;
                    }
                } else {
                    callback(m);
                }
                flush();
            }
        }

        /** The reader's handling of a client line (SMT 202-215). */
        private void handleLine(MpMessage m) {
            if (m instanceof Call c) {
                int i = c.req() - 1;
                if (st[i] == St.ACTIVE) {
                    return; // SMT 237-242: a duplicate in-flight id is ignored, unanswered
                }
                if (st[i] != St.ABSENT) {
                    throw new IllegalStateException("id reuse is outside the model");
                }
                st[i] = St.ACTIVE; // SMT 236-237
                tok[i] = c.token(); // MS 174: the context's progress token
                Channel.send(M, handler[i], new Work(c.req(), c.token()));
            } else if (m instanceof Cancelled c) {
                int i = c.req() - 1;
                if (st[i] == St.ACTIVE) {
                    st[i] = St.CANCELLING; // SMT 224: removed; the flag and dispose follow
                }
                // not in the map (absent, answered, cancelled): ignored, nothing written (SMT 224-225)
            } else {
                throw new IllegalStateException("server: unexpected " + m);
            }
        }

        /** A request's subscriber callback on handler output (SMT 269-276). */
        private void callback(MpMessage m) {
            if (m instanceof Prog p) {
                int i = p.req() - 1;
                if (i == 0) {
                    gotProg1 = true;
                }
                // DAN 37-49: stamped with the request's own token; non-increasing values dropped.
                // After the response or the flag the sink refuses it (DAN 58-63).
                if ((st[i] == St.ACTIVE || st[i] == St.CANCELLING) && p.value() > last[i]) {
                    last[i] = p.value();
                    queue.add(new Item(p.req(), new Progress(tok[i], p.value())));
                }
            } else if (m instanceof Done d) {
                int i = d.req() - 1;
                if (i == 1) {
                    gotDone2 = true;
                }
                if (st[i] == St.ACTIVE) {
                    st[i] = St.ANSWERED; // SMT 273: removed before the response is queued
                    enq[i] = true;
                } else if (st[i] == St.CANCELLING) {
                    // SMT 273 remove(key, entry) is a no-op (already removed); emit still queues it.
                    enq[i] = true;
                }
                // CANCELLED: the disposed pipeline delivers nothing (SMT 263-268)
            } else {
                throw new IllegalStateException("server: unexpected " + m);
            }
        }

        /**
         * Whether a further message to the server is certain, and receivable at the next step
         * (while the reader is busy, only handler output is). Certain: the first {@code Call} not
         * yet read; handler1's first {@code Prog} once it has its {@code Work} (it sends one in
         * every branch); handler2's {@code Done} once it has its {@code Work}; a client message once
         * every request read so far has its {@code Result} written or its cancel read (the client
         * then has sent, or will send, its second call or {@code Shutdown}). The host's cancels and
         * its choice of a second call are not certain.
         */
        private boolean certain() {
            boolean fromHandler = (st[0] != St.ABSENT && !gotProg1) || (st[1] != St.ABSENT && !gotDone2);
            boolean settled = calls > 0;
            for (int i = 0; i < 2; i++) {
                if (gotCall[i] && !wroteResult[i] && !gotCancel[i]) {
                    settled = false;
                }
            }
            boolean fromClient = calls < 1 || settled;
            return fromHandler || (!readerBusy() && fromClient);
        }

        private boolean idle() {
            return queue.isEmpty() && checked == null && pending == null && !enq[0] && !enq[1]
                    && cancelling() == 0;
        }

        /**
         * A nondeterministic sequence of micro-actions, ending at HALT. HALT is offered when
         * nothing is left to do, or when a further message is certain; otherwise the flush goes
         * on. The actions without a visible event (ENQ, the handling of a cancel) commute with the
         * writer's and with each other's, except that two ENQs give the queue order; they are
         * offered only before the flush's first visible action, unless HALT is not available.
         */
        private void flush() {
            boolean visible = false;
            while (true) {
                boolean halt = idle() || certain();
                List<Act> acts = new ArrayList<>(6);
                if (halt) {
                    acts.add(Act.HALT);
                }
                if (checked != null || !queue.isEmpty()) {
                    acts.add(Act.WRITER);
                }
                if (cancelling() != 0) {
                    acts.add(Act.FLAG);
                }
                boolean silent = !visible || !halt;
                if (pending != null && (pending instanceof Call || silent)) {
                    acts.add(Act.HANDLE);
                }
                if (enq[0] && silent) {
                    acts.add(Act.ENQ1);
                }
                if (enq[1] && silent) {
                    acts.add(Act.ENQ2);
                }
                Act a = acts.size() == 1 ? acts.get(0) : acts.get(Nondet.chooseIndex(acts.size()));
                switch (a) {
                    case HALT -> {
                        return;
                    }
                    case WRITER -> visible |= writer();
                    case FLAG -> {
                        int r = cancelling();
                        st[r - 1] = St.CANCELLED;
                        // Items of the request still queued can only be dropped by the writer's
                        // check (SMT 299-302); removing them now gives the same writes.
                        queue.removeIf(it -> it.req() == r);
                        Channel.send(M, handler[r - 1], new Stop(r));
                        visible = true;
                    }
                    case HANDLE -> {
                        MpMessage m = pending;
                        pending = null;
                        handleLine(m);
                        visible |= m instanceof Call;
                    }
                    case ENQ1 -> enqueue(0);
                    case ENQ2 -> enqueue(1);
                }
            }
        }

        private void enqueue(int i) {
            enq[i] = false;
            if (st[i] != St.CANCELLED) {
                queue.add(new Item(i + 1, new Result(i + 1))); // SMT 275, 296
            }
            // flag set: the writer's check would drop it (SMT 299-302)
        }

        /** One writer action; returns whether it wrote. */
        private boolean writer() {
            if (checked != null) {
                write(checked); // SMT 303
                checked = null;
                return true;
            }
            Item it = queue.poll();
            // Request 2 is never cancelled (the client runs without cancel2), and an answered
            // request's flag can never be set (its entry was removed before the item was queued):
            // for them the check cannot fail, and holding the item checked is indistinguishable
            // from leaving it queued.
            if (it.req() == 2 || st[it.req() - 1] == St.ANSWERED) {
                write(it);
                return true;
            }
            checked = it; // passed the check; the write may come after a flag is set (SMT 299-303)
            return false;
        }

        private void write(Item it) {
            if (it.msg() instanceof Result r) {
                wroteResult[r.req() - 1] = true;
            }
            Channel.send(M, client, it.msg());
        }
    }
}
