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
 * MCP cancel benchmark, Java side, fixed: the server core of modelcontextprotocol/java-sdk with the
 * stdio cancellation fix 69e6cd1e8fa3031b7c478782ed30afc051372a58 ("Stop stdio output of a request
 * once its cancellation is seen", on 779893c0706b01ad565e8768b4a1dbe4367eb058; modern stack,
 * stdio), rewritten as one sequential Must process, with the scripted client and two tool
 * handlers of the shared environment. The baseline is {@link McpCancel}; everything outside the
 * server is the same.
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
 * request 2 ({@code cancel2}) is not offered, as in the baseline, whose server writes request 2's
 * items without a separate check; the runs compare the two with the same client.
 *
 * <p>Line numbers are those of SMT at 69e6cd1e. The fix adds a volatile field {@code cancelling}
 * (SMT 85), the id of the request whose cancellation is being handled, and gives each request a
 * state (SMT 87-96, 368) that leaves IN_PROGRESS once, by compareAndSet, to ANSWERED or CANCELLED.
 * No lock. {@code handleCancel} publishes the id (SMT 243), looks the request up (245), settles it
 * CANCELLED if still in progress (247), clears the id (250) and, if it found the request and it is
 * CANCELLED, removes and disposes it (251-253). A response's callback reads {@code cancelling}
 * (304; the response is "taken" there), then, if still IN_PROGRESS, settles it (304) CANCELLED if
 * its id was published, else ANSWERED, removes the entry (305) and queues the response (306).
 * Progress is queued unchecked (296). The writer's check reads {@code cancelling} (349), settles an
 * in-progress request with that id CANCELLED (351), reads the state (353), drops iff CANCELLED;
 * the write follows (358). One action per access, every CAS one step, except two merges (MAP 9.2)
 * and three labelled reductions that keep every completed export line (MAP 9.4): F14, a CANCELLED
 * request's queued items are dropped at its settle and its later items are not queued; F15, a
 * response's queueing is offered only before the flush's first visible action, unless HALT is not
 * available or the response reached the queueing after that action; F16, an item of request 2 or
 * of an ANSWERED request is taken, checked and written in one action. F15 and F16 rest on
 * {@code cancel2} being off (a {@code Cancelled{2}} stops the run), on the FIFO receive and on
 * {@link Server#certain} as it is.
 *
 * <p>Receipt of a cancel (owner ruling R-A, 2026-11-23): the receive of {@code Cancelled} is the
 * entry into {@code handleCancel} with the parsed cancel; the validation and the key before the
 * publish are local, so the receive is the publish. The later steps follow, each at a later point;
 * the reader reads no further line until the cancel is done.
 *
 * <p>Termination (no daemons, no messages from main): the client's last send is {@code Shutdown};
 * on it the server sends {@code Shutdown} to both handlers, writes nothing else and ends; each
 * handler ends on {@code Shutdown}. The SDK instead ends on stdin EOF, after which its writer may
 * still write queued items; the harness does not model those writes.
 */
public final class McpCancelFixed implements MpProgram {

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

    /** The usage text of the constructor's arguments (MpLauncher's {@code Class:args} form). */
    public static final String USAGE = "usage: bench.mcp_cancel.McpCancelFixed:impl[,<a|b|c|x><a|b|x>]";

    /** A bad argument: the reason and the usage text; MpLauncher reports it with exit 2. */
    private static IllegalArgumentException bad(String why) {
        return new IllegalArgumentException("McpCancelFixed: " + why + "; " + USAGE);
    }

    /** @param args {@code impl}, optionally {@code <h1><h2>} */
    public McpCancelFixed(String... args) {
        if (args.length < 1 || args.length > 2 || !args[0].trim().equals("impl")) {
            throw bad("expected impl and at most one variant, got " + java.util.Arrays.toString(args));
        }
        this.variant = args.length == 2 ? args[1].trim() : "xx";
        if (variant.length() != 2) {
            throw bad("the variant must be two letters, was " + variant);
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
                    throw bad("handler2 has no branch c");
                }
                yield Branch.C;
            }
            case 'x' -> Branch.ANY;
            default -> throw bad("bad branch " + c);
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

    /** A request's {@code InFlight.state} (SMT 87-96, 368): it leaves IN_PROGRESS once, by CAS. */
    private enum Rs { IN_PROGRESS, ANSWERED, CANCELLED }

    /** The cancel being handled by the reader (SMT 237-255): the step it has done last. */
    private enum Cx {
        /** No cancel in progress. */
        NONE,
        /** {@code cancelling = key} (SMT 243), at the receive. */
        PUBLISHED,
        /** {@code inFlight.get(key)} (SMT 245) done; {@code found} holds the result. */
        LOOKED_UP,
        /** The CAS IN_PROGRESS to CANCELLED (SMT 247) done (only if found). */
        SETTLED,
        /** {@code cancelling = null} (SMT 250) done. */
        CLEARED
    }

    /** A response's callback (SMT 294-307), between its read of {@code cancelling} and its queueing. */
    private enum Resp {
        /** No response callback in progress. */
        NONE,
        /** {@code key.equals(this.cancelling)} read (SMT 304), at the receive of {@code Done}. */
        READ,
        /** The CAS (SMT 304) done. */
        SETTLED,
        /** {@code inFlight.remove(key, entry)} (SMT 305) done. */
        REMOVED
    }

    /** The writer's task for its current item (SMT 327-335, 348-354): the step it has done last. */
    private enum Wr {
        /** No item taken. */
        IDLE,
        /** Item taken and {@code this.cancelling} read (SMT 349); {@code wEq} holds the comparison. */
        READ,
        /** The CAS IN_PROGRESS to CANCELLED (SMT 351) done (only if {@code wEq}). */
        SETTLED,
        /** The state read (SMT 353) found it not CANCELLED: the item will be written (SMT 334). */
        CHECKED
    }

    /** A queued write: the message and the request whose state the writer checks. */
    private record Item(int req, MpMessage msg) {}

    /** The micro-actions a flush may take inside a step. */
    private enum Act { HALT, WRITER, CANCEL, RESPONSE1, RESPONSE2, HANDLE }

    /**
     * The fixed SDK's server side on stdio: the reader ({@code readLoop}, {@code handleLine},
     * SMT 175-235), the in-flight map and cancel (SMT 237-308), the per-request subscriber callbacks
     * (SMT 294-307), the notifier (DAN) and the single FIFO writer (SMT 73-77, 325-366), in one
     * process.
     *
     * <p>One step = one blocking receive, then a flush. No lock. Every shared access of the fix is
     * its own action (each CAS one step), except where noted:
     * <ul>
     *   <li>HANDLE: the reader's handling of the request line it has read (registered and
     *       subscribed, SMT 257-308). Until then the reader reads no further line, while handler
     *       output still arrives.</li>
     *   <li>CANCEL: the cancel's next step after its publish (the receive): the look-up (SMT 245),
     *       the CAS if found (247), the clear (250), then the end (251-253: if found and CANCELLED,
     *       the remove and the dispose, {@code Stop}). The reader reads no further line until the
     *       end.</li>
     *   <li>RESPONSE1, RESPONSE2: request r's response callback's next step after its read of
     *       {@code cancelling} (the receive of {@code Done}): the CAS (SMT 304), the remove (305),
     *       the queueing (306). The two requests' callbacks run on different threads, so they
     *       interleave freely.</li>
     *   <li>WRITER: the writer's next step: take the head and read {@code cancelling} (SMT 349),
     *       the CAS if it holds the item's id (351), the state read (353: dropped iff CANCELLED),
     *       the write (358). An item of request 2 or of an ANSWERED request is taken, checked and
     *       written in one action (F16).</li>
     * </ul>
     * Reductions (MAP 9.4): F14 at every settle to CANCELLED and at the queueing of a
     * CANCELLED request's items; F15 in {@link #flush}; F16 in {@link #writer}.
     * No non-blocking receive is used. A flush may stop with work left only when a further message
     * to the server is certain to arrive (see {@link #certain}); otherwise it continues, so no
     * execution blocks for want of a writer, reader or callback action.
     */
    private final class Server {
        private final Tid client;
        private final Tid[] handler;

        /** Whether request r was registered (SMT 261-262) and subscribed. */
        private final boolean[] started = {false, false};
        /** Whether request r's entry is in {@code inFlight} (SMT 262 put; 252, 305 remove). */
        private final boolean[] inMap = {false, false};
        /** {@code InFlight.state} (SMT 261, 368). */
        private final Rs[] state = {Rs.IN_PROGRESS, Rs.IN_PROGRESS};
        /** Whether request r's subscription was disposed (SMT 253): its pipeline delivers nothing more. */
        private final boolean[] disposed = {false, false};
        private final String[] tok = new String[2];
        /** DAN 29: the last progress value per request, initially negative infinity. */
        private final double[] last = {Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
        /** The writer's queue (SMT 77, 327): FIFO over all requests. */
        private final ArrayDeque<Item> queue = new ArrayDeque<>();
        /** The writer's current item (taken from the queue, not yet dropped or written), or null. */
        private Item cur;
        private Wr w = Wr.IDLE;
        /** The writer's read of {@code cancelling} (SMT 349-350) equalled its item's id. */
        private boolean wEq;
        /** The request line the reader has read and not yet handled, or null (never a cancel: R-A). */
        private MpMessage pending;
        /** {@code cancelling} (SMT 85): the request whose id is published (SMT 243) and not yet cleared (250), or 0. */
        private int cancelling;
        /** The request whose cancel the reader is handling, or 0; its step, and its look-up's result. */
        private int cxReq;
        private Cx cx = Cx.NONE;
        private boolean found;
        /** Per request: its response callback's step, and the state it settles to (SMT 304). */
        private final Resp[] resp = {Resp.NONE, Resp.NONE};
        private final Rs[] respTarget = {null, null};

        // What the server has seen and written, for the certainty of a further message.
        private int calls;
        private final boolean[] gotCall = {false, false};
        private final boolean[] gotCancel = {false, false};
        private final boolean[] wroteResult = {false, false};
        private boolean gotProg1;
        private boolean gotProg2;
        private boolean gotDone2;

        Server(Tid client, Tid h1, Tid h2) {
            this.client = client;
            this.handler = new Tid[] {h1, h2};
        }

        /** The reader is inside a line: a request read and not yet handled, or a cancel not yet done. */
        private boolean readerBusy() {
            return pending != null || cx != Cx.NONE;
        }

        void run() {
            while (true) {
                // While the reader is busy, no further line; handler output is taken at any time.
                MpMessage m = !readerBusy()
                        ? Channel.recv(M)
                        : Channel.recv(M, x -> x instanceof Prog || x instanceof Done);
                if (m instanceof Shutdown) {
                    // stdin EOF: the server ends; nothing further is written (harness rule).
                    Channel.send(M, handler[0], new Shutdown());
                    Channel.send(M, handler[1], new Shutdown());
                    return;
                }
                if (m instanceof Call c) {
                    pending = m; // read by the reader; handled at a later point (HANDLE)
                    calls++;
                    gotCall[c.req() - 1] = true;
                } else if (m instanceof Cancelled c) {
                    if (c.req() != 1) {
                        // F15 and F16 hold only while request 2 is never cancelled (cancel2 off).
                        throw new IllegalStateException("Cancelled{" + c.req() + "}: cancel2 is outside the model");
                    }
                    // R-A: received at the entry into handleCancel; the validation and the key are
                    // local, the publish (SMT 243) is this step. The rest follows at CANCEL.
                    gotCancel[c.req() - 1] = true;
                    cancelling = c.req();
                    cxReq = c.req();
                    cx = Cx.PUBLISHED;
                } else {
                    callback(m);
                }
                flush();
            }
        }

        /** The reader's handling of a request line (SMT 231-232, 257-308); whether it sent {@code Work}. */
        private boolean handleCall(Call c) {
            int i = c.req() - 1;
            if (inMap[i]) {
                return false; // SMT 262-266: a duplicate in-flight id is ignored, unanswered
            }
            if (started[i]) {
                throw new IllegalStateException("id reuse is outside the model");
            }
            started[i] = true;
            inMap[i] = true; // SMT 261-262, state IN_PROGRESS
            tok[i] = c.token(); // MS 174: the context's progress token
            Channel.send(M, handler[i], new Work(c.req(), c.token()));
            return true;
        }

        /**
         * F14 (as baseline V8 a): request i settled CANCELLED; its queued items would all be
         * dropped by the writer's state read (SMT 353, 330-332), so they are dropped now. The
         * writer's current item is left to its own steps.
         */
        private void dropQueued(int i) {
            queue.removeIf(it -> it.req() == i + 1);
        }

        /** The cancel's next step (SMT 245-253); whether it sent {@code Stop}. */
        private boolean cancelStep() {
            int i = cxReq - 1;
            switch (cx) {
                case PUBLISHED -> {
                    found = inMap[i]; // SMT 245
                    cx = Cx.LOOKED_UP;
                }
                case LOOKED_UP -> {
                    if (found) {
                        if (state[i] == Rs.IN_PROGRESS) { // SMT 247
                            state[i] = Rs.CANCELLED;
                            dropQueued(i); // F14
                        }
                        cx = Cx.SETTLED;
                    } else {
                        cancelling = 0; // SMT 250
                        cx = Cx.CLEARED;
                    }
                }
                case SETTLED -> {
                    cancelling = 0; // SMT 250
                    cx = Cx.CLEARED;
                }
                case CLEARED -> {
                    // SMT 251-253: the state is settled (the look-up found the entry and the CAS
                    // ran, or the entry was gone, removed only after a settle), so this read is
                    // the state's final value; the remove has no later reader in the scenario.
                    int r = cxReq;
                    cx = Cx.NONE;
                    cxReq = 0;
                    if (found && state[i] == Rs.CANCELLED) {
                        inMap[i] = false; // SMT 252
                        disposed[i] = true; // SMT 253
                        Channel.send(M, handler[i], new Stop(r));
                        return true;
                    }
                }
                case NONE -> throw new IllegalStateException("no cancel");
            }
            return false;
        }

        /** A request's subscriber callback on handler output (SMT 294-307). */
        private void callback(MpMessage m) {
            if (m instanceof Prog p) {
                int i = p.req() - 1;
                if (i == 0) {
                    gotProg1 = true;
                } else {
                    gotProg2 = true;
                }
                // After the dispose the pipeline delivers nothing (SMT 253). Otherwise DAN 37-49:
                // stamped with the request's own token, non-increasing values dropped, and
                // SMT 295-296: queued, whatever the state; the writer checks it. F14: an item of a
                // CANCELLED request is not queued (the writer would drop it); last[i] is updated
                // either way, as the notifier does.
                if (!disposed[i] && p.value() > last[i]) {
                    last[i] = p.value();
                    if (state[i] != Rs.CANCELLED) {
                        queue.add(new Item(p.req(), new Progress(tok[i], p.value())));
                    }
                }
            } else if (m instanceof Done d) {
                int i = d.req() - 1;
                if (i == 1) {
                    gotDone2 = true;
                }
                if (!disposed[i]) {
                    // SMT 304: the response is taken here, at the read of cancelling; the CAS,
                    // the remove and the queueing follow at RESPONSE.
                    respTarget[i] = cancelling == d.req() ? Rs.CANCELLED : Rs.ANSWERED;
                    resp[i] = Resp.READ;
                }
            } else {
                throw new IllegalStateException("server: unexpected " + m);
            }
        }

        /** Request r's response callback's next step (SMT 304-306). */
        private void responseStep(int r) {
            int i = r - 1;
            switch (resp[i]) {
                case READ -> {
                    if (state[i] == Rs.IN_PROGRESS) { // SMT 304, the CAS
                        state[i] = respTarget[i];
                        if (state[i] == Rs.CANCELLED) {
                            dropQueued(i); // F14
                        }
                    }
                    resp[i] = Resp.SETTLED;
                }
                case SETTLED -> {
                    inMap[i] = false; // SMT 305
                    resp[i] = Resp.REMOVED;
                }
                case REMOVED -> {
                    // SMT 306: emit schedules the write (offered as F15 says, in flush). F14: not
                    // queued if CANCELLED (the writer would drop it).
                    if (state[i] != Rs.CANCELLED) {
                        queue.add(new Item(r, new Result(r)));
                    }
                    resp[i] = Resp.NONE;
                    respTarget[i] = null;
                }
                case NONE -> throw new IllegalStateException("no response");
            }
        }

        /** The writer's next step (SMT 327-335, 348-354, 358); whether it wrote. */
        private boolean writer() {
            switch (w) {
                case IDLE -> {
                    Item head = queue.peek();
                    if (head.req() == 2 || state[head.req() - 1] == Rs.ANSWERED) {
                        // F16 (as baseline V3): request 2 is never cancelled and ANSWERED is final,
                        // so SMT 349-353 cannot drop it or change a state; taken, checked and
                        // written (334, 358) in one action.
                        queue.poll();
                        write(head);
                        return true;
                    }
                    // The task taken from the queue, and its first statement, SMT 349.
                    cur = queue.poll();
                    wEq = cancelling == cur.req();
                    w = Wr.READ;
                }
                case READ -> {
                    if (wEq) {
                        if (state[cur.req() - 1] == Rs.IN_PROGRESS) { // SMT 351
                            state[cur.req() - 1] = Rs.CANCELLED;
                            dropQueued(cur.req() - 1); // F14
                        }
                        w = Wr.SETTLED;
                    } else {
                        stateCheck();
                    }
                }
                case SETTLED -> stateCheck();
                case CHECKED -> {
                    write(cur); // SMT 334, 358
                    cur = null;
                    w = Wr.IDLE;
                    return true;
                }
            }
            return false;
        }

        /** SMT 353, 330-332: the state read; a CANCELLED request's item is dropped. */
        private void stateCheck() {
            if (state[cur.req() - 1] == Rs.CANCELLED) {
                cur = null;
                w = Wr.IDLE;
            } else {
                w = Wr.CHECKED;
            }
        }

        /**
         * Whether a further message to the server is certain, and receivable at the next step
         * (while the reader is busy, only handler output is). Certain: the first {@code Call} not
         * yet read; each handler's first {@code Prog} once it has its {@code Work} (it sends one in
         * every branch); handler2's {@code Done} once it has its {@code Work}; a client message once
         * every request read so far has its {@code Result} written or its cancel read (the client
         * then has sent, or will send, its second call or {@code Shutdown}). The host's cancels and
         * its choice of a second call are not certain.
         */
        private boolean certain() {
            boolean fromHandler = (started[0] && !gotProg1) || (started[1] && !gotProg2)
                    || (started[1] && !gotDone2);
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
            return queue.isEmpty() && cur == null && pending == null && cx == Cx.NONE
                    && resp[0] == Resp.NONE && resp[1] == Resp.NONE;
        }

        /**
         * A nondeterministic sequence of micro-actions, ending at HALT. HALT is offered when
         * nothing is left to do, or when a further message is certain; otherwise the flush goes
         * on. Every action is offered at every point, except F15 (as baseline V8 b): a response's
         * queueing (SMT 306) is offered only while no visible action (a send) has run in this
         * flush, or when HALT is not available, or when the response reached the queueing after
         * that visible action.
         */
        private void flush() {
            boolean visible = false;
            boolean[] late = {false, false};
            while (true) {
                boolean halt = idle() || certain();
                List<Act> acts = new ArrayList<>(6);
                if (halt) {
                    acts.add(Act.HALT);
                }
                if (cur != null || !queue.isEmpty()) {
                    acts.add(Act.WRITER);
                }
                if (cx != Cx.NONE) {
                    acts.add(Act.CANCEL);
                }
                for (int i = 0; i < 2; i++) {
                    boolean offered = resp[i] != Resp.NONE
                            && (resp[i] != Resp.REMOVED || !visible || !halt || late[i]); // F15
                    if (offered) {
                        acts.add(i == 0 ? Act.RESPONSE1 : Act.RESPONSE2);
                    }
                }
                if (pending != null) {
                    acts.add(Act.HANDLE);
                }
                Act a = acts.size() == 1 ? acts.get(0) : acts.get(Nondet.chooseIndex(acts.size()));
                switch (a) {
                    case HALT -> {
                        return;
                    }
                    case WRITER -> visible |= writer();
                    case CANCEL -> visible |= cancelStep();
                    case RESPONSE1, RESPONSE2 -> {
                        int i = a == Act.RESPONSE1 ? 0 : 1;
                        boolean toRemoved = resp[i] == Resp.SETTLED;
                        responseStep(i + 1);
                        if (toRemoved && visible) {
                            late[i] = true; // F15: reached the queueing after a visible action
                        }
                    }
                    case HANDLE -> {
                        Call c = (Call) pending;
                        pending = null;
                        visible |= handleCall(c);
                    }
                }
            }
        }

        private void write(Item it) {
            if (it.msg() instanceof Result r) {
                wroteResult[r.req() - 1] = true;
            }
            Channel.send(M, client, it.msg());
        }
    }
}
