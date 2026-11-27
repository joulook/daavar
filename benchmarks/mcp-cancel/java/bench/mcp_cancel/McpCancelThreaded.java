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
import java.util.List;

/**
 * MCP cancel benchmark, Java side, threaded: the server core of modelcontextprotocol/java-sdk at
 * 779893c0706b01ad565e8768b4a1dbe4367eb058 (modern stack, stdio) with its threads and shared objects
 * as Must processes, so that their interleavings are enumerated instead of chosen by hand.
 *
 * <p>{@code S/} = {@code mcp-core/src/main/java/io/modelcontextprotocol/modern/server/}; SMT =
 * {@code S/transport/StdioMcpTransport.java}, DAN = {@code S/DefaultAsyncNotifier.java}, SA =
 * {@code S/feature/SyncAdapters.java}.
 *
 * <p>Processes. Visible: {@code client} and {@code server}. {@code server} is the stdio wire: it
 * reads client lines and writes client lines, and it carries the tool invocations' visible events
 * ({@code Work}, {@code Stop}, {@code Prog}, {@code Done}, {@code Shutdown} to the handlers). The
 * hidden handlers are those of the hand rewrite {@link McpCancel}, except that each waits for its
 * callback's {@code JNext} before a signal (V7). Daemons, one per SDK thread:
 * {@code reader} (SMT 157-185, the reader thread), {@code writer} (SMT 72-76, 296-304, the writer
 * worker), {@code callback1}, {@code callback2} (the thread on which request r's tool runs, the
 * boundedElastic thread of SA 37, 49, as stdio calls {@code handle(..., false)}, MS 103-105; it also
 * runs the subscriber callback, SMT 269-276). Daemons, one per shared object: {@code map1},
 * {@code map2} (the in-flight ConcurrentHashMap, SMT 78, one process per key: operations on
 * different keys commute), {@code flag1}, {@code flag2} (each entry's AtomicBoolean, SMT 324),
 * {@code sub1}, {@code sub2} (the cancelled state of each request's subscriber, set by its dispose,
 * SMT 228, read when a signal is delivered), {@code queue} (the writer worker's task queue, SMT 296).
 * Every shared access is one request to the object's process, answered by one reply; the caller waits
 * for the reply, so each thread's accesses take effect in program order (sequential consistency).
 *
 * <p>The {@code server} process's messages with the daemons have Java-only tags beginning with
 * {@code J}; they are in its exported word and are removed from it (with the counts recomputed)
 * before any join, so that the word compared with the specification is the wire word.
 *
 * <p>Arguments: {@code implt[,<h1><h2>]}, as {@link McpCancel}'s {@code impl}. Model: fifo.
 * Termination: the client's last send is {@code Shutdown}; the server reads it as the reader's next
 * line (stdin EOF), sends {@code Shutdown} to both handlers, writes nothing else and ends (harness
 * rule, as in {@link McpCancel}); the daemons are left blocked.
 */
public final class McpCancelThreaded implements MpProgram {

    // ---- the visible vocabulary is McpCancel's (msgs.rs) ----

    // ---- Java-only tags, server <-> daemons (removed from the server's word before a join) ----

    /** reader -> server: readLine (SMT 161); the server reads the next client line. */
    @Tag("JRead")
    public record JRead() implements MpMessage {}

    /** server -> reader: the line read is a request. */
    @Tag("JLineCall")
    public record JLineCall(int req, String token) implements MpMessage {}

    /** server -> reader: the line read is a cancellation. */
    @Tag("JLineCancelled")
    public record JLineCancelled(int req) implements MpMessage {}

    /**
     * callback r -> handler r: request r's thread is ready for its tool's next signal; the handler
     * sends its next {@code Prog} or {@code Done} only after it (V7).
     */
    @Tag("JNext")
    public record JNext(int req) implements MpMessage {}

    /** server -> callback r: the tool reported progress. */
    @Tag("JProg")
    public record JProg(int req, String token, int value) implements MpMessage {}

    /** server -> callback r: the tool returned. */
    @Tag("JDone")
    public record JDone(int req) implements MpMessage {}

    /** reader -> server: the request's subscription starts its tool (SMT 256, 267-269). */
    @Tag("JWork")
    public record JWork(int req, String token) implements MpMessage {}

    /** reader -> server: the dispose cancels the request's tool (SMT 228). */
    @Tag("JStop")
    public record JStop(int req) implements MpMessage {}

    /** server -> reader: the tool's task is cancelled ({@code Stop} sent); the dispose goes on. */
    @Tag("JStopped")
    public record JStopped() implements MpMessage {}

    /** writer -> server: writeLine of a progress notification (SMT 303, 312-322). */
    @Tag("JWriteProgress")
    public record JWriteProgress(String token, int value) implements MpMessage {}

    /** writer -> server: writeLine of a result response (SMT 303, 312-322). */
    @Tag("JWriteResult")
    public record JWriteResult(int req) implements MpMessage {}

    /** server -> writer: writeLine returned. */
    @Tag("JWritten")
    public record JWritten() implements MpMessage {}

    // ---- object requests and replies (daemons only; never in a visible word) ----

    /** Callers of the objects, the index of the reply's destination. */
    static final int READER = 0;
    static final int WRITER = 1;
    static final int CB1 = 2;
    static final int CB2 = 3;

    /** map: {@code putIfAbsent(idKey(r), entry)} (SMT 237); reply: whether it was absent. */
    @Tag("JPutIfAbsent")
    public record JPutIfAbsent(int caller, int req) implements MpMessage {}

    /** map: {@code remove(idKey(r))} (SMT 224); reply: whether an entry was removed. */
    @Tag("JRemove")
    public record JRemove(int caller, int req) implements MpMessage {}

    /** map: {@code remove(key, entry)} (SMT 268, 273); reply: whether it was removed. */
    @Tag("JRemoveIf")
    public record JRemoveIf(int caller, int req) implements MpMessage {}

    /** flag r: {@code cancelled().get()} (SMT 299); reply: the value. */
    @Tag("JFlagGet")
    public record JFlagGet(int caller) implements MpMessage {}

    /** flag r: {@code cancelled().set(true)} (SMT 227); reply: an acknowledgement. */
    @Tag("JFlagSet")
    public record JFlagSet(int caller) implements MpMessage {}

    /** sub r: the dispose cancels the subscriber (SMT 228); reply: whether it was still active. */
    @Tag("JSubCancel")
    public record JSubCancel(int caller) implements MpMessage {}

    /**
     * sub r: the pipeline completes after the response's callback returned; reply: whether the
     * subscriber was still active (then doFinally runs on this thread, SMT 267-268).
     */
    @Tag("JSubComplete")
    public record JSubComplete(int caller) implements MpMessage {}

    /** sub r: a signal's delivery reads whether the subscriber is cancelled; reply: the value. */
    @Tag("JSubCheck")
    public record JSubCheck(int caller) implements MpMessage {}

    /**
     * queue: {@code writer.schedule(task)} (SMT 296), the task of the write of one line of request
     * {@code req} (a result, or progress {@code value} with {@code token}); reply: an acknowledgement.
     */
    @Tag("JOffer")
    public record JOffer(int caller, int req, boolean result, String token, int value) implements MpMessage {}

    /** queue: the worker takes the next task; answered only when the queue is not empty. */
    @Tag("JPoll")
    public record JPoll(int caller) implements MpMessage {}

    @Tag("JAck")
    public record JAck() implements MpMessage {}

    @Tag("JBool")
    public record JBool(boolean value) implements MpMessage {}

    /** The task taken: the fields of its {@link JOffer}. */
    @Tag("JTask")
    public record JTask(int req, boolean result, String token, int value) implements MpMessage {}

    private static final CommunicationModel M = CommunicationModel.P2P;

    private final McpCancel.Branch h1Branch;
    private final McpCancel.Branch h2Branch;
    private final String variant;

    public static final String USAGE = "usage: bench.mcp_cancel.McpCancelThreaded:implt[,<a|b|c|x><a|b|x>]";

    private static IllegalArgumentException bad(String why) {
        return new IllegalArgumentException("McpCancelThreaded: " + why + "; " + USAGE);
    }

    /** @param args {@code implt}, optionally {@code <h1><h2>} */
    public McpCancelThreaded(String... args) {
        if (args.length < 1 || args.length > 2 || !args[0].trim().equals("implt")) {
            throw bad("expected implt and at most one variant, got " + java.util.Arrays.toString(args));
        }
        this.variant = args.length == 2 ? args[1].trim() : "xx";
        if (variant.length() != 2) {
            throw bad("the variant must be two letters, was " + variant);
        }
        this.h1Branch = branch(variant.charAt(0), true);
        this.h2Branch = branch(variant.charAt(1), false);
    }

    private static McpCancel.Branch branch(char c, boolean third) {
        return switch (c) {
            case 'a' -> McpCancel.Branch.A;
            case 'b' -> McpCancel.Branch.B;
            case 'c' -> {
                if (!third) {
                    throw bad("handler2 has no branch c");
                }
                yield McpCancel.Branch.C;
            }
            case 'x' -> McpCancel.Branch.ANY;
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

    @Override
    public void run() {
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending handler1 = MpProcess.create("handler1");
        MpProcess.Pending handler2 = MpProcess.create("handler2");
        MpProcess.Pending client = MpProcess.create("client");
        MpProcess.Pending reader = MpProcess.createDaemon();
        MpProcess.Pending writer = MpProcess.createDaemon();
        MpProcess.Pending cb1 = MpProcess.createDaemon();
        MpProcess.Pending cb2 = MpProcess.createDaemon();
        MpProcess.Pending map1 = MpProcess.createDaemon();
        MpProcess.Pending map2 = MpProcess.createDaemon();
        MpProcess.Pending flag1 = MpProcess.createDaemon();
        MpProcess.Pending flag2 = MpProcess.createDaemon();
        MpProcess.Pending sub1 = MpProcess.createDaemon();
        MpProcess.Pending sub2 = MpProcess.createDaemon();
        MpProcess.Pending queue = MpProcess.createDaemon();
        Tid s = server.tid();
        Tid[] callers = {reader.tid(), writer.tid(), cb1.tid(), cb2.tid()};
        Objects o = new Objects(new Tid[] {map1.tid(), map2.tid()}, new Tid[] {flag1.tid(), flag2.tid()},
                new Tid[] {sub1.tid(), sub2.tid()}, queue.tid());
        Tid[] h = {handler1.tid(), handler2.tid()};
        Tid[] cb = {cb1.tid(), cb2.tid()};
        reader.start(() -> reader(s, o));
        writer.start(() -> writer(s, o));
        cb1.start(() -> callback(1, s, h[0], o, CB1));
        cb2.start(() -> callback(2, s, h[1], o, CB2));
        map1.start(() -> mapObject(callers));
        map2.start(() -> mapObject(callers));
        flag1.start(() -> flagObject(callers));
        flag2.start(() -> flagObject(callers));
        sub1.start(() -> subscriberObject(callers));
        sub2.start(() -> subscriberObject(callers));
        queue.start(() -> queueObject(callers));
        List<MpProcess.Handle> hs = List.of(
                server.start(() -> server(client.tid(), h, reader.tid(), writer.tid(), cb)),
                handler1.start(() -> handler(s, true, h1Branch)),
                handler2.start(() -> handler(s, false, h2Branch)),
                client.start(() -> client(s, false)));
        for (MpProcess.Handle hd : hs) {
            hd.await();
        }
    }

    /** The objects' processes, as seen by a thread. */
    record Objects(Tid[] map, Tid[] flag, Tid[] sub, Tid queue) {}

    // ---------------------------------------------------------------- environment (env.rs), as McpCancel 192-280

    /** The request a client-bound message belongs to (progress: the request of its token). */
    private static int reqOf(MpMessage m) {
        if (m instanceof McpCancel.Progress p) {
            return Integer.parseInt(p.token().substring(1));
        }
        if (m instanceof McpCancel.Result r) {
            return r.req();
        }
        return ((McpCancel.Error) m).req();
    }

    /**
     * The client: the host whose model decides, as a nondeterministic procedure. It sends
     * {@code Call{1,"t1"}}, then chooses whether to send {@code Call{2,"t2"}}. Before reading
     * anything, and after each message it reads while something is outstanding, it may cancel request 1 while
     * that request has no response, at most once; with {@code cancel2}, likewise request 2 if it was
     * sent. It stops once every request it sent has a response or its cancel, then sends
     * {@code Shutdown}. A message read before the stop has no effect beyond marking a response.
     */
    private static void client(Tid server, boolean cancel2) {
        Channel.send(M, server, new McpCancel.Call(1, McpCancel.token(1)));
        boolean sent2 = Nondet.chooseBoolean();
        if (sent2) {
            Channel.send(M, server, new McpCancel.Call(2, McpCancel.token(2)));
        }
        boolean[] sent = {true, sent2};
        boolean[] cancellable = {true, sent2 && cancel2};
        boolean[] answered = {false, false};
        boolean[] cancelled = {false, false};
        maybeCancel(server, cancellable, answered, cancelled);
        while (outstanding(sent, answered, cancelled)) {
            MpMessage m = Channel.recv(M);
            if (m instanceof McpCancel.Result || m instanceof McpCancel.Error) {
                answered[reqOf(m) - 1] = true;
            }
            if (outstanding(sent, answered, cancelled)) {
                maybeCancel(server, cancellable, answered, cancelled);
            }
        }
        Channel.send(M, server, new McpCancel.Shutdown());
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
                Channel.send(M, server, new McpCancel.Cancelled(i + 1));
                cancelled[i] = true;
            }
        }
    }

    /**
     * A tool handler. On {@code Work{r,t}}: {@code Prog{r,t,1}}, then branch (a) {@code Done{r}},
     * (b) {@code Prog{r,t,2}} and {@code Done{r}}, or (c, handler1 only) nothing more. {@code Stop}:
     * nothing. {@code Shutdown}: it ends. Unlike McpCancel's handler (V7), each {@code Prog} and
     * {@code Done} is sent only after its callback's {@code JNext}: the tool's thread runs the
     * subscriber callback before the tool goes on (SA 52-72, the sync notifier blocks until the sink
     * has taken the item). A {@code Shutdown} while it waits ends it: the wire has ended and would not
     * take the signal.
     */
    private static void handler(Tid server, boolean first, McpCancel.Branch branch) {
        while (true) {
            MpMessage m = Channel.recv(M, x -> !(x instanceof JNext));
            if (m instanceof McpCancel.Work w) {
                if (!next()) {
                    return;
                }
                Channel.send(M, server, new McpCancel.Prog(w.req(), w.token(), 1));
                McpCancel.Branch b = branch;
                if (b == McpCancel.Branch.ANY) {
                    b = first
                            ? new McpCancel.Branch[] {McpCancel.Branch.A, McpCancel.Branch.B, McpCancel.Branch.C}
                                    [Nondet.chooseIndex(3)]
                            : (Nondet.chooseBoolean() ? McpCancel.Branch.A : McpCancel.Branch.B);
                }
                if (b == McpCancel.Branch.A) {
                    if (!next()) {
                        return;
                    }
                    Channel.send(M, server, new McpCancel.Done(w.req()));
                } else if (b == McpCancel.Branch.B) {
                    if (!next()) {
                        return;
                    }
                    Channel.send(M, server, new McpCancel.Prog(w.req(), w.token(), 2));
                    if (!next()) {
                        return;
                    }
                    Channel.send(M, server, new McpCancel.Done(w.req()));
                }
            } else if (m instanceof McpCancel.Shutdown) {
                return;
            }
            // Stop: nothing
        }
    }

    /** The handler waits for its callback's {@code JNext}; false if {@code Shutdown} comes first. */
    private static boolean next() {
        return Channel.recv(M, x -> x instanceof JNext || x instanceof McpCancel.Shutdown) instanceof JNext;
    }

    // ---------------------------------------------------------------- the wire (visible)

    /**
     * The stdio wire. A client line is read only when the reader has called readLine (SMT 161), one
     * line per call. Every other message, the tools' signals included (their handler sends each only
     * when the tool's thread is ready for it, V7), is taken at any time.
     */
    private static void server(Tid client, Tid[] handler, Tid reader, Tid writer, Tid[] cb) {
        boolean[] lineWanted = {false};
        while (true) {
            boolean line = lineWanted[0];
            MpMessage m = Channel.recv(M, x -> line || !(x instanceof McpCancel.Call
                    || x instanceof McpCancel.Cancelled || x instanceof McpCancel.Shutdown));
            if (m instanceof JRead) {
                lineWanted[0] = true;
            } else if (m instanceof McpCancel.Call c) {
                lineWanted[0] = false;
                Channel.send(M, reader, new JLineCall(c.req(), c.token()));
            } else if (m instanceof McpCancel.Cancelled c) {
                lineWanted[0] = false;
                Channel.send(M, reader, new JLineCancelled(c.req()));
            } else if (m instanceof McpCancel.Shutdown) {
                // stdin EOF: the server ends; nothing further is written (harness rule).
                Channel.send(M, handler[0], new McpCancel.Shutdown());
                Channel.send(M, handler[1], new McpCancel.Shutdown());
                return;
            } else if (m instanceof McpCancel.Prog p) {
                Channel.send(M, cb[p.req() - 1], new JProg(p.req(), p.token(), p.value()));
            } else if (m instanceof McpCancel.Done d) {
                Channel.send(M, cb[d.req() - 1], new JDone(d.req()));
            } else if (m instanceof JWork w) {
                Channel.send(M, handler[w.req() - 1], new McpCancel.Work(w.req(), w.token()));
            } else if (m instanceof JStop st) {
                Channel.send(M, handler[st.req() - 1], new McpCancel.Stop(st.req()));
                Channel.send(M, reader, new JStopped());
            } else if (m instanceof JWriteProgress wp) {
                Channel.send(M, client, new McpCancel.Progress(wp.token(), wp.value()));
                Channel.send(M, writer, new JWritten());
            } else if (m instanceof JWriteResult wr) {
                Channel.send(M, client, new McpCancel.Result(wr.req()));
                Channel.send(M, writer, new JWritten());
            } else {
                throw new IllegalStateException("server: unexpected " + m);
            }
        }
    }

    // ---------------------------------------------------------------- the SDK's threads

    /** The reader thread: readLoop (SMT 157-185) and handleLine (SMT 187-217). */
    private static void reader(Tid server, Objects o) {
        while (true) {
            Channel.send(M, server, new JRead()); // SMT 161 readLine
            MpMessage line = Channel.recv(M);
            // SMT 161 closing.get(): false at every read here (written only at EOF, by this thread).
            if (line instanceof JLineCall c) {
                dispatch(server, o, c.req(), c.token());
            } else if (line instanceof JLineCancelled c) {
                handleCancel(server, o, c.req());
            } else {
                throw new IllegalStateException("reader: unexpected " + line);
            }
        }
    }

    /** SMT 232-277. */
    private static void dispatch(Tid server, Objects o, int r, String token) {
        if (!call(o.map()[r - 1], new JPutIfAbsent(READER, r))) { // SMT 237
            return; // SMT 238-241: a duplicate in-flight id is ignored, unanswered
        }
        // SMT 245 closing.get(): false at every read here (written only at EOF, by this thread).
        // SMT 253-268: subscribe; subscribeOn starts the tool on its own thread (SMT 256).
        Channel.send(M, server, new JWork(r, token));
        // SMT 267 update(...): the Swap is touched by this thread only.
    }

    /** SMT 219-230. */
    private static void handleCancel(Tid server, Objects o, int r) {
        // SMT 220-223: the params are well formed (scope).
        boolean found = call(o.map()[r - 1], new JRemove(READER, r)); // SMT 224
        if (found) {
            act(o.flag()[r - 1], new JFlagSet(READER)); // SMT 227
            dispose(server, o, r); // SMT 228
        }
    }

    /**
     * SMT 228 {@code subscription().dispose()}: the subscriber is cancelled, the cancellation goes
     * up to the tool's task, and doFinally runs (SMT 267-268) on this thread.
     */
    private static void dispose(Tid server, Objects o, int r) {
        if (!call(o.sub()[r - 1], new JSubCancel(READER))) {
            return; // the pipeline has completed: dispose cancels nothing, doFinally has run
        }
        Channel.send(M, server, new JStop(r));
        Channel.recv(M, JStopped.class);
        call(o.map()[r - 1], new JRemoveIf(READER, r)); // SMT 268, doFinally on cancel
    }

    /**
     * The thread of request r's tool (SA 37, 49): it runs the tool and, for each signal of the tool,
     * the subscriber callback (SMT 269-276); then doFinally (SMT 267-268).
     */
    private static void callback(int r, Tid server, Tid handler, Objects o, int self) {
        double last = Double.NEGATIVE_INFINITY; // DAN 29: read and written by this thread only
        while (true) {
            Channel.send(M, handler, new JNext(r));
            MpMessage m = Channel.recv(M);
            if (m instanceof JProg p) {
                if (p.value() <= last) {
                    continue; // DAN 43-47
                }
                last = p.value(); // DAN 48
                if (call(o.sub()[r - 1], new JSubCheck(self))) {
                    continue; // the cancelled subscriber takes no further signal
                }
                // SMT 275: emit; DAN 39: the context's token, the request's own
                act(o.queue(), new JOffer(self, r, false, p.token(), p.value()));
            } else if (m instanceof JDone) {
                if (call(o.sub()[r - 1], new JSubCheck(self))) {
                    return;
                }
                call(o.map()[r - 1], new JRemoveIf(self, r)); // SMT 273
                act(o.queue(), new JOffer(self, r, true, "", 0)); // SMT 275
                if (call(o.sub()[r - 1], new JSubComplete(self))) { // completion: doFinally once
                    call(o.map()[r - 1], new JRemoveIf(self, r)); // SMT 268, on completion
                }
                return;
            } else {
                throw new IllegalStateException("callback: unexpected " + m);
            }
        }
    }

    /** The writer worker (SMT 72-76): its tasks in FIFO order, each the task of SMT 296-304. */
    private static void writer(Tid server, Objects o) {
        while (true) {
            JTask it = (JTask) act(o.queue(), new JPoll(WRITER));
            if (call(o.flag()[it.req() - 1], new JFlagGet(WRITER))) { // SMT 299
                continue; // SMT 300-301
            }
            // SMT 303 writeLine
            Channel.send(M, server, it.result()
                    ? new JWriteResult(it.req()) : new JWriteProgress(it.token(), it.value()));
            Channel.recv(M, JWritten.class);
        }
    }

    // ---------------------------------------------------------------- the shared objects

    /** A request whose reply is a value ({@link JBool}): sent, then the reply awaited. */
    private static boolean call(Tid object, MpMessage request) {
        Channel.send(M, object, request);
        return Channel.recv(M, JBool.class).value();
    }

    /** A request whose reply is an acknowledgement or a task: sent, then the reply awaited. */
    private static MpMessage act(Tid object, MpMessage request) {
        Channel.send(M, object, request);
        return Channel.recv(M, x -> x instanceof JAck || x instanceof JTask);
    }

    /**
     * The in-flight map's entry for one id (one process per id: operations on different keys commute;
     * one entry per id, as each id is used once, scope).
     */
    private static void mapObject(Tid[] callers) {
        boolean[] present = {false};
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof JPutIfAbsent p) {
                boolean absent = !present[0];
                present[0] = true;
                Channel.send(M, callers[p.caller()], new JBool(absent));
            } else if (m instanceof JRemove p) {
                boolean was = present[0];
                present[0] = false;
                Channel.send(M, callers[p.caller()], new JBool(was));
            } else if (m instanceof JRemoveIf p) {
                boolean was = present[0];
                present[0] = false;
                Channel.send(M, callers[p.caller()], new JBool(was));
            } else {
                throw new IllegalStateException("map: unexpected " + m);
            }
        }
    }

    /** An entry's AtomicBoolean {@code cancelled}, initially false: {@code get}, {@code set(true)}. */
    private static void flagObject(Tid[] callers) {
        boolean value = false;
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof JFlagGet g) {
                Channel.send(M, callers[g.caller()], new JBool(value));
            } else if (m instanceof JFlagSet g) {
                value = true;
                Channel.send(M, callers[g.caller()], new JAck());
            } else {
                throw new IllegalStateException("flag: unexpected " + m);
            }
        }
    }

    /**
     * Request r's subscriber (the pipeline's terminal state, as LambdaSubscriber's subscription
     * field): active, cancelled by dispose (SMT 228) or completed after the response. A delivery is
     * dropped once it is cancelled; cancel and completion each act only on an active subscriber, so
     * doFinally runs once, on the thread that ended it.
     */
    private static void subscriberObject(Tid[] callers) {
        int state = 0; // 0 active, 1 cancelled, 2 completed
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof JSubCheck g) {
                Channel.send(M, callers[g.caller()], new JBool(state == 1));
            } else if (m instanceof JSubCancel g) {
                boolean active = state == 0;
                if (active) {
                    state = 1;
                }
                Channel.send(M, callers[g.caller()], new JBool(active));
            } else if (m instanceof JSubComplete g) {
                boolean active = state == 0;
                if (active) {
                    state = 2;
                }
                Channel.send(M, callers[g.caller()], new JBool(active));
            } else {
                throw new IllegalStateException("subscriber: unexpected " + m);
            }
        }
    }

    /** The worker's task queue: FIFO; a take waits while it is empty. */
    private static void queueObject(Tid[] callers) {
        ArrayDeque<JOffer> q = new ArrayDeque<>();
        while (true) {
            boolean nonEmpty = !q.isEmpty();
            MpMessage m = Channel.recv(M, x -> x instanceof JOffer || nonEmpty);
            if (m instanceof JOffer f) {
                q.add(f);
                Channel.send(M, callers[f.caller()], new JAck());
            } else if (m instanceof JPoll p) {
                JOffer f = q.poll();
                Channel.send(M, callers[p.caller()], new JTask(f.req(), f.result(), f.token(), f.value()));
            } else {
                throw new IllegalStateException("queue: unexpected " + m);
            }
        }
    }
}
