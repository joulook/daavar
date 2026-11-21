package bench.a2a_task;

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
 * One A2A task, one stream, a cancel racing the task's completion: the server core of a2a-java
 * rewritten as one sequential process.
 *
 * <p>Source: a2aproject/a2a-java at {@code f40664f85f61d232fc794dfe9b7cdc8557bcd7b8},
 * {@code server-common/src/main/java/org/a2aproject/sdk/server/}: {@code requesthandlers/DefaultRequestHandler}
 * (DRH), {@code events/MainEventBusProcessor} (MEBP), {@code events/EventQueue} (EQ),
 * {@code events/EventConsumer} (EC), {@code tasks/TaskManager} (TM), {@code tasks/ResultAggregator}
 * (RA), {@code tasks/InMemoryTaskStore}; the JSON-RPC transport's
 * {@code transport/jsonrpc/src/main/java/org/a2aproject/sdk/transport/jsonrpc/handler/JSONRPCHandler}
 * (JH). Line numbers below refer to that commit.
 *
 * <p>Abstraction: the server handles each incoming message in one atomic step, and a step ends at
 * every external wait. The agent executor is the process {@code exec}, which is not visible. The bus processor's
 * commit and distribution of an executor event (MEBP 217-312) run in the step that receives that
 * event. The cancel's synchronous call of the executor's {@code cancel} (DRH 761) is an external
 * wait: the server sends {@code CancelCb} and returns to its mailbox; the call returns when the
 * executor's {@code Canceled} arrives (the scripted {@code cancel} emits exactly that one event and
 * returns). The subscribers' consumers are independent threads (EC 101), so the outputs one step
 * produces for different subscribers are sent in any interleaving that keeps each subscriber's own
 * order.
 *
 * <p>Arguments: the Rust twin's ARGS, {@code <impl|impl2|impl2g>,<a|b|n>[,<option>]}. In {@code n}
 * the executor is the agent's model as a nondeterministic procedure ({@code call_llm}): it answers
 * a plain Message ({@code Reply}, relayed as one {@code MessageItem} after which the stream closes),
 * or opens a task, reporting {@code Submit} or {@code Working} first, then {@code Completed} at once
 * or {@code Working} and then {@code Completed} or nothing more. {@code a} and {@code b} are its two
 * former fixed scripts ({@code Submit} first, or {@code Working} first, then {@code Completed} or
 * nothing). {@code recancel} (one server only) adds a second cancel after the {@code Get} unless the
 * stream carried a Message. For the delegation scenario with {@code n} the option is a part of the
 * exploration: {@code local}, {@code deleg}, {@code delegmsg}, {@code delegtask[-<s|w>[<c|w>]]}.
 * Visible: {@code client} and {@code server}, every send and receive; {@code exec} is not visible.
 * Model: fifo. The messages, the client and the executor mirror the Rust twin's ({@code msgs.rs},
 * {@code env.rs}) event for event.
 *
 * <p>Delegation, {@code impl2,<a|b|n>}: two instances of the same server. Instance A
 * ({@code serverA}) serves {@code client} and its executor {@code execA}. Instance B ({@code serverB})
 * serves {@code execA}, which delegates the task to it as an A2A client, and the single-server
 * executor {@code execB}. Visible: {@code client}, {@code serverA}, {@code serverB}.
 * {@code impl2g,<a|b|n>} is the same with the gated delegation contract: execA sends its cancel to B
 * only after B's stream has shown {@code Status{Working}}.
 *
 * <p>Termination (no daemons, no messages from main): the client's last send is {@code Shutdown};
 * the server records it and, once every cancel callback it sent has been answered by a
 * {@code Canceled}, sends {@code Shutdown} to the executor and ends; the executor ends on
 * {@code Shutdown}. This harness protocol is not part of a2a-java.
 */
public final class A2ATask implements MpProgram {

    /** Task states; {@code Completed} and {@code Canceled} are terminal. */
    public enum State { Submitted, Working, Completed, Canceled }

    /** The code of a refused cancel. */
    public enum Code { TaskNotCancelable }

    /** The visible code of every other error: an observation says only "an error here". */
    private static final String ANY_ERROR = "*";

    /** The end of the run: client to server, then server to executor. */
    @Tag("Shutdown")
    public record Shutdown() implements MpMessage {}

    // ---- client -> server ----

    /** {@code message/stream}: a new task, streamed. */
    @Tag("SendStream")
    public record SendStream(int req) implements MpMessage {}

    /** {@code tasks/cancel}. */
    @Tag("Cancel")
    public record Cancel(int req, String task) implements MpMessage {}

    /** {@code tasks/get}. */
    @Tag("Get")
    public record Get(int req, String task) implements MpMessage {}

    // ---- server -> client ----

    /** A stream item carrying the Task object. */
    @Tag("TaskItem")
    public record TaskItem(int req, String task, State state) implements MpMessage {}

    /** A stream item carrying a status-update event; {@code fin} is true exactly on a terminal state. */
    @Tag("Status")
    public record Status(int req, String task, State state, boolean fin) implements MpMessage {}

    /** The end of the stream of request {@code req}. */
    @Tag("StreamClose")
    public record StreamClose(int req) implements MpMessage {}

    /** A successful cancel answer: the task as it is after the request. */
    @Tag("CancelOk")
    public record CancelOk(int req, String task, State state) implements MpMessage {}

    /** A cancel refused because the task is not cancelable. */
    @Tag("CancelErr")
    public record CancelErr(int req, Code code) implements MpMessage {}

    /** The answer to a read of the task. */
    @Tag("GetOk")
    public record GetOk(int req, String task, State state) implements MpMessage {}

    /** A stream item carrying a Message (the stream of a request answered without a task). */
    @Tag("MessageItem")
    public record MessageItem(int req) implements MpMessage {}

    /** Any other error, for any request; {@code code} is always {@code "*"}. */
    @Tag("Err")
    public record Err(int req, String code) implements MpMessage {}

    // ---- server <-> exec (observed at the server; the exec role is not visible) ----

    /** The executor has the task: a task snapshot in state submitted (variant a only). */
    @Tag("Submit")
    public record Submit(String task) implements MpMessage {}

    /** The executor is working on the task: a status update. */
    @Tag("Working")
    public record Working(String task) implements MpMessage {}

    /** The executor finished the task: a status update. */
    @Tag("Completed")
    public record Completed(String task) implements MpMessage {}

    /** The executor's {@code cancel} stopped the task: a status update. */
    @Tag("Canceled")
    public record Canceled(String task) implements MpMessage {}

    /** The executor answered with a plain Message instead of opening a task (internal). */
    @Tag("Reply")
    public record Reply(String task) implements MpMessage {}

    /** Start executing the task. */
    @Tag("Exec")
    public record Exec(String task) implements MpMessage {}

    /** Cancel callback: stop the task. */
    @Tag("CancelCb")
    public record CancelCb(String task) implements MpMessage {}

    private static final CommunicationModel M = CommunicationModel.P2P;
    private static final String T1 = "t1";

    private final boolean submitFirst;
    private final boolean recancel;
    /** The delegation scenario: two server instances, the first one's executor a client of the second. */
    private final boolean delegation;
    /**
     * Delegation only: execA sends its cancel downstream only after the downstream stream has shown
     * {@code Status{Working}} (gated contract).
     */
    private final boolean gated;
    /** Variant {@code n}: the executor is the agent's model as a nondeterministic procedure. */
    private final boolean llm;
    /**
     * Delegation with {@code n} only, a part of the exploration: 0 every choice; 1 A answers
     * locally; 2 A delegates; 3 A delegates and B answers with a Message; 4 A delegates and B opens a
     * task.
     */
    private final int split;
    /** With split 4: B's first report, 0 any, 1 {@code Submit}, 2 {@code Working}. */
    private final int bKind;
    /** With split 4: B's continuation, 0 any, 1 {@code Completed} at once, 2 {@code Working} first. */
    private final int bCont;
    /** The variant and option fields, for the export header's size. */
    private final String sizeName;

    /** The usage text of the constructor's arguments (MpLauncher's {@code Class:args} form). */
    public static final String USAGE = "usage: bench.a2a_task.A2ATask:<impl|impl2|impl2g>,<a|b|n>[,recancel|local|deleg|delegmsg|delegtask[-<s|w>[<c|w>]]]";

    /** A bad argument: the reason and the usage text; MpLauncher reports it with exit 2. */
    private static IllegalArgumentException bad(String why) {
        return new IllegalArgumentException("A2ATask: " + why + "; " + USAGE);
    }

    /**
     * @param args the Rust twin's ARGS fields: {@code impl}, {@code impl2} or {@code impl2g}; the
     *     variant {@code a}, {@code b} or {@code n}; optionally {@code recancel} (one server only) or,
     *     for the delegation scenario with {@code n}, a part: {@code local}, {@code deleg},
     *     {@code delegmsg}, {@code delegtask[-<s|w>[<c|w>]]}
     */
    public A2ATask(String... args) {
        if (args.length < 2 || args.length > 3) {
            throw bad("expected 2 or 3 arguments, got " + args.length);
        }
        String side = args[0].trim();
        this.delegation = switch (side) {
            case "impl" -> false;
            case "impl2", "impl2g" -> true;
            default -> throw bad("side must be impl, impl2 or impl2g, was " + args[0]);
        };
        this.gated = side.equals("impl2g");
        String variant = args[1].trim();
        this.llm = variant.equals("n");
        this.submitFirst = switch (variant) {
            case "a", "n" -> true;
            case "b" -> false;
            default -> throw bad("variant must be a, b or n, was " + args[1]);
        };
        String opt = args.length == 3 ? args[2].trim() : null;
        boolean rc = false;
        int sp = 0;
        if (opt != null) {
            if (opt.equals("recancel")) {
                rc = true;
            } else if (opt.equals("local")) {
                sp = 1;
            } else if (opt.equals("deleg")) {
                sp = 2;
            } else if (opt.equals("delegmsg")) {
                sp = 3;
            } else if (opt.startsWith("delegtask")) {
                sp = 4;
            } else {
                throw bad("unknown option " + opt);
            }
        }
        if (sp > 0 && !(delegation && llm)) {
            throw bad(opt + " splits the delegation scenario with n only");
        }
        this.recancel = rc;
        if (delegation && recancel) {
            throw bad("no recancel in the delegation scenario");
        }
        this.split = sp;
        int kind = 0;
        int cont = 0;
        if (sp == 4) {
            String x = opt.substring("delegtask".length());
            String r = x.startsWith("-") ? x.substring(1) : x;
            if (!x.isEmpty() && (r.isEmpty() || r.length() > 2 || !x.startsWith("-"))) {
                throw bad("bad split " + opt);
            }
            if (r.length() >= 1) {
                if (r.charAt(0) == 'c') {
                    throw bad("bad split " + opt);
                }
                kind = pick(r.charAt(0), opt);
            }
            if (r.length() == 2) {
                if (r.charAt(1) == 's') {
                    throw bad("bad split " + opt);
                }
                cont = pick(r.charAt(1), opt);
            }
        }
        this.bKind = kind;
        this.bCont = cont;
        this.sizeName = (delegation ? "2" : "") + (gated ? "g" : "") + variant + (opt == null ? "" : "-" + opt);
    }

    private static int pick(char c, String opt) {
        return switch (c) {
            case 's', 'c' -> 1;
            case 'w' -> 2;
            default -> throw bad("bad split " + opt);
        };
    }

    @Override
    public String programName() {
        return "a2a-task";
    }

    @Override
    public String size() {
        return sizeName;
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return delegation
                ? List.of("serverA", "execA", "client", "serverB", "execB")
                : List.of("server", "exec", "client");
    }

    @Override
    public List<String> visibleRoles() {
        return delegation ? List.of("client", "serverA", "serverB") : List.of("client", "server");
    }

    /**
     * Creates the three processes first, so each can name the others, then starts them and waits
     * for all three (the Rust twin's server thread spawns the other two; spawn order is not observed).
     */
    @Override
    public void run() {
        if (delegation) {
            launch2();
            return;
        }
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending exec = MpProcess.create("exec");
        MpProcess.Pending client = MpProcess.create("client");
        Tid s = server.tid();
        Tid e = exec.tid();
        Tid c = client.tid();
        MpProcess.Handle hs = server.start(() -> new Server(c, e).run());
        MpProcess.Handle he = exec.start(() -> exec(s, Fixed.NONE));
        MpProcess.Handle hc = client.start(() -> client(s));
        hs.await();
        he.await();
        hc.await();
    }

    // ---------------------------------------------------------------- delegation wiring

    /**
     * The delegation scenario. The Rust twin's spawn tree is: main spawns {@code serverA}; serverA
     * spawns {@code execA}, then {@code client}; execA spawns {@code serverB}; serverB spawns
     * {@code execB}. JMC's export requires main to create every process, so main creates all five
     * here, then starts them and waits for them. In the tree, each spawn comes before the spawning
     * process's first message event, so the tree orders no two message events. The exported words
     * and their causal order are therefore the same. serverA runs the server for (client, execA),
     * execA the delegating executor, serverB the same server for (execA, execB), and execB the
     * single-server executor. Shutdown chain: client, serverA, execA, serverB, execB.
     */
    private void launch2() {
        MpProcess.Pending serverA = MpProcess.create("serverA");
        MpProcess.Pending execA = MpProcess.create("execA");
        MpProcess.Pending client = MpProcess.create("client");
        MpProcess.Pending serverB = MpProcess.create("serverB");
        MpProcess.Pending execB = MpProcess.create("execB");
        Tid a = serverA.tid();
        Tid ea = execA.tid();
        Tid c = client.tid();
        Tid b = serverB.tid();
        Tid eb = execB.tid();
        List<MpProcess.Handle> hs = List.of(
                serverA.start(() -> new Server(c, ea).run()),
                execA.start(() -> execA(a, b)),
                client.start(() -> client(a)),
                serverB.start(() -> new Server(ea, eb).run()),
                execB.start(() -> exec(b, bFixed())));
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    /**
     * The delegating executor of server A. Towards B it is a client with requests 1 (streaming send)
     * and 2 (cancel). On {@code Exec}: with {@code n}, first a choice (fixed by the part): answer
     * locally ({@link #callLlm} towards A; every {@code CancelCb} from A is then answered by one
     * {@code Canceled} to A, and B is never asked), or delegate. Delegating: in variant a (and
     * {@code n}) {@code Submit} to A; {@code Working} to A; {@code SendStream{1}} to B. From B's
     * stream: a final {@code Status} remembers its state; a {@code MessageItem} is remembered (B
     * answered without a task); a {@code TaskItem}, a non-final {@code Status} or an {@code Err} does
     * nothing (not relayed); {@code StreamClose{1}}: if the remembered state is {@code Completed} or B
     * answered a Message, {@code Completed} to A (once). {@code CancelCb} from A (at most one):
     * {@code Cancel{2,t1}} to B; when gated, only once B's {@code Status{1,t1,Working,false}} has
     * arrived: a callback received earlier is remembered and the cancel goes out when that status
     * arrives; if B's stream closes first (before or after the callback), no downstream cancel is
     * sent and the callback counts as answered. Any message for request 2 is B's answer. After every message: once a callback
     * arrived, B answered and B's stream closed, exactly one {@code Canceled} to A. {@code Shutdown}
     * from A: {@code Shutdown} to B, and it ends.
     */
    private void execA(Tid a, Tid b) {
        State bFinal = null;
        boolean bClosed = false;
        boolean bWorking = false;
        boolean cancelDue = false;
        boolean bMessage = false;
        boolean local = false;
        boolean cb = false;
        boolean answered = false;
        boolean completedSent = false;
        boolean canceledSent = false;
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Exec x && llm && (split == 1 || (split == 0 && Nondet.chooseBoolean()))) {
                // The agent answers by itself, without delegating; B is never asked.
                local = true;
                callLlm(a, x.task(), Fixed.NONE);
            } else if (m instanceof Exec x) {
                if (submitFirst) {
                    Channel.send(M, a, new Submit(x.task()));
                }
                Channel.send(M, a, new Working(x.task()));
                Channel.send(M, b, new SendStream(1));
            } else if (m instanceof CancelCb x && local) {
                Channel.send(M, a, new Canceled(x.task()));
            } else if (m instanceof CancelCb) {
                if (cb) {
                    throw new AssertionError("execA: one cancel callback");
                }
                cb = true;
                if (!gated || bWorking) {
                    Channel.send(M, b, new Cancel(2, T1));
                } else if (bClosed) {
                    // Gated, and B's stream already closed without a working status: nothing is left
                    // to cancel downstream; the callback is settled without a downstream cancel.
                    answered = true;
                } else {
                    cancelDue = true;
                }
            } else if (m instanceof Shutdown) {
                Channel.send(M, b, new Shutdown());
                return;
            } else if (m instanceof Status st && st.req() == 1 && st.fin()) {
                bFinal = st.state();
            } else if (m instanceof Status st && st.req() == 1 && st.state() == State.Working && !st.fin()) {
                bWorking = true;
                if (cancelDue) {
                    cancelDue = false;
                    Channel.send(M, b, new Cancel(2, T1));
                }
            } else if (m instanceof TaskItem ti && ti.req() == 1
                    || m instanceof Status st && st.req() == 1
                    || m instanceof Err er && er.req() == 1) {
                // nothing: B's other stream items are not relayed
            } else if (m instanceof MessageItem mi && mi.req() == 1) {
                bMessage = true;
            } else if (m instanceof StreamClose sc && sc.req() == 1) {
                bClosed = true;
                if (cancelDue) {
                    // Gated: B's stream closed before any working status (B answered with a Message,
                    // or finished without publishing one); nothing is left to cancel downstream, so
                    // the callback is settled without a downstream cancel.
                    cancelDue = false;
                    answered = true;
                }
                if ((bFinal == State.Completed || bMessage) && !completedSent) {
                    completedSent = true;
                    Channel.send(M, a, new Completed(T1));
                }
            } else if (reqOf(m) == 2) {
                answered = true;
            } else {
                throw new IllegalStateException("execA: unexpected " + m);
            }
            if (cb && answered && bClosed && !canceledSent) {
                canceledSent = true;
                Channel.send(M, a, new Canceled(T1));
            }
        }
    }

    // ---------------------------------------------------------------- client

    /** The request a client-bound message answers or belongs to (executor messages: 0). */
    private static int reqOf(MpMessage m) {
        if (m instanceof TaskItem x) {
            return x.req();
        } else if (m instanceof Status x) {
            return x.req();
        } else if (m instanceof StreamClose x) {
            return x.req();
        } else if (m instanceof MessageItem x) {
            return x.req();
        } else if (m instanceof CancelOk x) {
            return x.req();
        } else if (m instanceof CancelErr x) {
            return x.req();
        } else if (m instanceof GetOk x) {
            return x.req();
        } else if (m instanceof Err x) {
            return x.req();
        } else if (m instanceof Exec || m instanceof CancelCb || m instanceof Shutdown) {
            return 0;
        }
        throw new IllegalStateException("unexpected " + m);
    }

    /**
     * The client: a streaming send, read until the stream closes; on the first
     * {@code Status{1,t1,Working,false}} while the stream is open it cancels (request 2) and then also
     * waits for the cancel's answer (any message for request 2). Then a read ({@code Get}, request 3).
     * A stream that carried a Message (no task) is followed by the read only; otherwise, with
     * {@code recancel}, a second cancel (request 4). Then {@code Shutdown} to the server.
     */
    private void client(Tid server) {
        Channel.send(M, server, new SendStream(1));
        boolean canc = false;
        boolean answered = false;
        boolean closed = false;
        boolean message = false;
        while (!(closed && (!canc || answered))) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Status st && st.req() == 1 && st.state() == State.Working && !st.fin()
                    && !canc && !closed) {
                Channel.send(M, server, new Cancel(2, T1));
                canc = true;
            } else if (m instanceof MessageItem mi && mi.req() == 1) {
                message = true;
            } else if (m instanceof StreamClose sc && sc.req() == 1) {
                closed = true;
            } else if (reqOf(m) == 2) {
                answered = true;
            }
        }
        Channel.send(M, server, new Get(3, T1));
        while (reqOf(Channel.recv(M)) != 3) {
            // other requests' messages are consumed
        }
        if (recancel && !message) {
            Channel.send(M, server, new Cancel(4, T1));
            while (reqOf(Channel.recv(M)) != 4) {
                // other requests' messages are consumed
            }
        }
        Channel.send(M, server, new Shutdown());
    }

    // ---------------------------------------------------------------- exec

    /**
     * Choices of {@link #callLlm} fixed in advance, used only to split an exploration into parts:
     * {@code message} a Message (1) or a task (2); {@code submit} the first report {@code Submit} (1)
     * or {@code Working} (2); {@code complete} {@code Completed} at once (1) or {@code Working} first
     * (2); 0 leaves the choice free.
     */
    private record Fixed(int message, int submit, int complete) {
        static final Fixed NONE = new Fixed(0, 0, 0);
    }

    /** B's fixed choices for the delegation part (the Rust twin's launcher). */
    private Fixed bFixed() {
        int message = split == 3 ? 1 : split == 4 ? 2 : 0;
        return new Fixed(message, bKind, bCont);
    }

    private static boolean choose(int fixed) {
        return fixed == 0 ? Nondet.chooseBoolean() : fixed == 1;
    }

    /**
     * The agent's answer to {@code Exec}, as a nondeterministic procedure ({@code call_llm}): a plain
     * Message ({@code Reply}), or a task: first report {@code Submit} (the Task object first) or
     * {@code Working} (a status first); then {@code Completed} at once, or {@code Working} and then
     * {@code Completed} or nothing more (it keeps working).
     */
    private static void callLlm(Tid server, String task, Fixed fx) {
        if (choose(fx.message())) {
            Channel.send(M, server, new Reply(task));
            return;
        }
        if (choose(fx.submit())) {
            Channel.send(M, server, new Submit(task));
        } else {
            Channel.send(M, server, new Working(task));
        }
        if (choose(fx.complete())) {
            Channel.send(M, server, new Completed(task));
        } else {
            Channel.send(M, server, new Working(task));
            if (Nondet.chooseBoolean()) {
                Channel.send(M, server, new Completed(task));
            }
        }
    }

    /**
     * The executor. With {@code n}, on {@code Exec} it runs {@link #callLlm}. Otherwise (the former
     * fixed scripts): in variant a a snapshot first, then {@code Working}, then either
     * {@code Completed} at once, or nothing more (an {@code execute()} that has not returned). On
     * every {@code CancelCb}: exactly one {@code Canceled}, whatever it sent before (its
     * {@code cancel} runs on a fresh emitter, DRH 754, so the executing emitter's terminal flag does
     * not stop it). On {@code Shutdown}: it ends.
     */
    private void exec(Tid server, Fixed fx) {
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Exec x && llm) {
                callLlm(server, x.task(), fx);
            } else if (m instanceof Exec x) {
                if (submitFirst) {
                    Channel.send(M, server, new Submit(x.task()));
                }
                Channel.send(M, server, new Working(x.task()));
                if (Nondet.chooseBoolean()) {
                    Channel.send(M, server, new Completed(x.task()));
                }
            } else if (m instanceof CancelCb cb) {
                Channel.send(M, server, new Canceled(cb.task()));
            } else if (m instanceof Shutdown) {
                return;
            } else {
                throw new IllegalStateException("exec: unexpected " + m);
            }
        }
    }

    // ---------------------------------------------------------------- server

    /** {@code TaskState.isFinal} (a2a-java {@code TaskState.java} 43-67) for the states used here. */
    private static boolean isFinal(State state) {
        return state == State.Completed || state == State.Canceled;
    }

    /**
     * An event distributed to the children: a task snapshot, a status update, an agent Message, or
     * the {@code InternalError} that replaces an event the store rejected.
     */
    private record Event(boolean task, State state, boolean error, boolean message) {
        static Event snapshot(State state) {
            return new Event(true, state, false, false);
        }

        static Event status(State state) {
            return new Event(false, state, false, false);
        }

        static Event internalError() {
            return new Event(false, null, true, false);
        }

        /** AE 533-536, 560-569: the executor's Message, enqueued (EQ 721: the queue does not check its task id). */
        static Event agentMessage() {
            return new Event(false, null, false, true);
        }
    }

    /**
     * A child queue tapped on the task's main queue (EQ 567-578, 934-1165) and the consumer that reads
     * it: the stream's {@code EventConsumer} (EC 94-305) or the cancel's blocking
     * {@code consumeAndBreakOnInterrupt} (RA 72-266).
     */
    private static final class Child {
        final boolean stream;
        final int req;
        /** The child queue was closed and left its main queue (EC 262-264; EQ 1157-1161, 766-801). */
        boolean closed;
        /** The consumer stopped reading (an error, EC 224-228, or after a close). */
        boolean consumerDone;
        /** Cancel only: events queued in the child before consumption starts (DRH 743 before 785). */
        final List<Event> queued = new ArrayList<>();
        /** Cancel only: the executor's {@code cancel} has returned, so consumption has started. */
        boolean consuming;
        /** Cancel only: the waiter has been woken and the cancel answered. */
        boolean answered;
        /** Outputs of the current step for this subscriber, in its own order. */
        final List<MpMessage> out = new ArrayList<>();

        Child(boolean stream, int req) {
            this.stream = stream;
            this.req = req;
        }
    }

    private final class Server {
        private final Tid client;
        private final Tid exec;

        /** {@code InMemoryTaskStore}: the stored state of {@code t1}, or null when absent. */
        private State stored;

        /** The children of the task's main queue, in tap order (EQ 513). */
        private final List<Child> children = new ArrayList<>();

        /** The client's {@code Shutdown} has been received (harness protocol). */
        private boolean shutdown;

        /** Cancel callbacks sent and not yet answered by a {@code Canceled}. */
        private int callbacksOutstanding;

        Server(Tid client, Tid exec) {
            this.client = client;
            this.exec = exec;
        }

        void run() {
            while (!(shutdown && callbacksOutstanding == 0)) {
                MpMessage m = Channel.recv(M);
                if (m instanceof Shutdown) {
                    shutdown = true;
                } else if (m instanceof SendStream r) {
                    onMessageSendStream(r);
                } else if (m instanceof Cancel r) {
                    onCancelTask(r);
                } else if (m instanceof Get r) {
                    onGetTask(r);
                } else if (m instanceof Submit) {
                    processEvent(Event.snapshot(State.Submitted));
                } else if (m instanceof Working) {
                    processEvent(Event.status(State.Working));
                } else if (m instanceof Completed) {
                    processEvent(Event.status(State.Completed));
                } else if (m instanceof Reply) {
                    processEvent(Event.agentMessage());
                } else if (m instanceof Canceled) {
                    processEvent(Event.status(State.Canceled));
                    cancelCallbackReturned();
                    callbacksOutstanding--;
                }
                flush();
            }
            Channel.send(M, exec, new Shutdown());
        }

        /**
         * Sends this step's outputs. Each subscriber's outputs keep their order; different subscribers'
         * outputs interleave in any way, as their consumer threads do (EC 101; RA 90).
         */
        private void flush() {
            List<List<MpMessage>> lanes = new ArrayList<>();
            for (Child ch : children) {
                if (!ch.out.isEmpty()) {
                    lanes.add(new ArrayList<>(ch.out));
                    ch.out.clear();
                }
            }
            for (Child ch : retired) {
                if (!ch.out.isEmpty()) {
                    lanes.add(new ArrayList<>(ch.out));
                    ch.out.clear();
                }
            }
            retired.clear();
            while (!lanes.isEmpty()) {
                int i = lanes.size() == 1 ? 0 : Nondet.chooseIndex(lanes.size());
                List<MpMessage> lane = lanes.get(i);
                Channel.send(M, client, lane.remove(0));
                if (lane.isEmpty()) {
                    lanes.remove(i);
                }
            }
        }

        /** Closed children whose last outputs are still to be sent in this step. */
        private final List<Child> retired = new ArrayList<>();

        /**
         * DRH 1022-1185: no task is stored; TM creates it from the executor's first event (TM 70-85
         * for a snapshot, 241-256 for a status update). The stream's child is tapped (DRH 1045) before
         * the executor starts (DRH 1072, 1360-1429). Nothing is sent to the client: the stream carries
         * only what the executor emits.
         */
        private void onMessageSendStream(SendStream r) {
            children.add(new Child(true, r.req()));
            Channel.send(M, exec, new Exec(T1));
        }

        /**
         * DRH 702-797. The store is read (723); absent: task not found (724-726); final: not cancelable
         * (729-732), before any tap. Otherwise the cancel's child is tapped (743) and the executor's
         * {@code cancel} is called (761): the external wait. Consumption starts when it returns (785).
         */
        private void onCancelTask(Cancel r) {
            // No task stored, also while a Message-only stream is open: not found at once, without
            // calling the executor's cancel (DRH 723-726).
            if (stored == null) {
                // a2a-java: TaskNotFoundError
                Channel.send(M, client, new Err(r.req(), ANY_ERROR));
                return;
            }
            if (isFinal(stored)) {
                Channel.send(M, client, new CancelErr(r.req(), Code.TaskNotCancelable));
                return;
            }
            children.add(new Child(false, r.req()));
            Channel.send(M, exec, new CancelCb(T1));
            callbacksOutstanding++;
            // DRH 778-779: the stream's running-agent entry was removed when the stream was set up
            // (DRH 1174), so there is nothing to cancel there.
        }

        /**
         * The executor's {@code cancel} returned (it emitted its one {@code Canceled}): the cancel
         * whose callback was outstanding starts consuming its child (DRH 785), oldest event first.
         */
        private void cancelCallbackReturned() {
            for (Child ch : List.copyOf(children)) {
                if (!ch.stream && !ch.consuming) {
                    ch.consuming = true;
                    List<Event> backlog = new ArrayList<>(ch.queued);
                    ch.queued.clear();
                    for (Event ev : backlog) {
                        consumeAtCancel(ch, ev);
                    }
                    retireIfClosed(ch);
                    return;
                }
            }
        }

        /** DRH 561-573. */
        private void onGetTask(Get r) {
            // After a Message-only stream nothing was stored (TM 154-207): not found (DRH 565-568).
            if (stored == null) {
                // a2a-java: TaskNotFoundError
                Channel.send(M, client, new Err(r.req(), ANY_ERROR));
            } else {
                Channel.send(M, client, new GetOk(r.req(), T1, stored));
            }
        }

        /**
         * MEBP 217-312: commit first (234, through TM.process), then distribute to the children in
         * tap order (277; EQ 807-829: a closed child is skipped). A commit the transition guard
         * rejects distributes an {@code InternalError} instead of the event (MEBP 251-259, 364-368).
         */
        private void processEvent(Event ev) {
            Event toDistribute = commit(ev) ? ev : Event.internalError();
            for (Child child : List.copyOf(children)) {
                if (child.closed) {
                    continue;
                }
                if (child.stream) {
                    consumeAtStream(child, toDistribute);
                } else if (child.consuming) {
                    consumeAtCancel(child, toDistribute);
                } else {
                    child.queued.add(toDistribute);
                }
                retireIfClosed(child);
            }
        }

        private void retireIfClosed(Child child) {
            if (child.closed && children.remove(child)) {
                retired.add(child);
            }
        }

        /**
         * TM.process (TM 152-208): a snapshot is saved as given (70-85), a status update is applied to
         * the stored task or to a new submitted one (91-125, ensureTask 241-256). Both pass the
         * transition guard first (275-286): from a final state only the same state is accepted.
         *
         * @return false if the guard rejected the event; nothing was stored then
         */
        private boolean commit(Event ev) {
            if (ev.message()) {
                // TM 154-207 has no branch for a Message: nothing is created or stored, and the
                // event is distributed as it is (MEBP 237, 277).
                return true;
            }
            State current = stored;
            if (!ev.task() && current == null) {
                current = State.Submitted;
                stored = current;
            }
            if (current != null && isFinal(current) && current != ev.state()) {
                return false;
            }
            stored = ev.state();
            return true;
        }

        /**
         * EC 101-302 for the stream, with the transport's conversion. An error fails the core publisher
         * and the consumer stops without closing its child (EC 224-228, 293-297); the transport sends it
         * as one error item and completes (JH 780-818). A final status or a final task is forwarded,
         * then the child closes and the stream completes (EC 232-297). Anything else is forwarded.
         */
        private void consumeAtStream(Child child, Event ev) {
            if (child.consumerDone) {
                return;
            }
            if (ev.error()) {
                // a2a-java: InternalError
                child.out.add(new Err(child.req, ANY_ERROR));
                child.out.add(new StreamClose(child.req));
                child.consumerDone = true;
                return;
            }
            if (ev.message()) {
                // EC 235-238: a Message is the complete response; it is sent, then the child closes
                // (EC 258-264) and the stream completes (EC 296). Nothing is sent before it.
                child.out.add(new MessageItem(child.req));
                child.out.add(new StreamClose(child.req));
                child.closed = true;
                child.consumerDone = true;
                return;
            }
            if (ev.task()) {
                child.out.add(new TaskItem(child.req, T1, ev.state()));
            } else {
                child.out.add(new Status(child.req, T1, ev.state(), isFinal(ev.state())));
            }
            if (isFinal(ev.state())) {
                child.out.add(new StreamClose(child.req));
                child.closed = true;
                child.consumerDone = true;
            }
        }

        /**
         * RA 72-266 and DRH 785-797 for the cancel. The first event of any kind wakes the waiter
         * (RA 129-171). An error is thrown as the answer (EC 224-228, RA 197-202, 223-240). Otherwise the
         * result is the captured task snapshot if the event was one (RA 116-124, 245-251), else the
         * stored task (RA 252-257, 268-272); it is the answer if canceled, else the cancel is refused
         * (DRH 792-795). The consumer keeps reading in the background (RA 184-191), without output,
         * until an error stops it (EC 224-228) or a final event closes its child (EC 262-264).
         */
        private void consumeAtCancel(Child child, Event ev) {
            if (child.consumerDone) {
                return;
            }
            if (ev.message()) {
                // A cancel's child exists only once a task is stored (DRH 723-726), and the scripted
                // executor answers a Message only as its first and only event.
                throw new IllegalStateException("a Message reached a cancel's consumer: outside the scenario");
            }
            if (!child.answered) {
                child.answered = true;
                if (ev.error()) {
                    // a2a-java: InternalError
                    child.out.add(new Err(child.req, ANY_ERROR));
                } else {
                    State state = ev.task() ? ev.state() : stored;
                    if (state == State.Canceled) {
                        child.out.add(new CancelOk(child.req, T1, state));
                    } else {
                        child.out.add(new CancelErr(child.req, Code.TaskNotCancelable));
                    }
                }
            }
            if (ev.error()) {
                child.consumerDone = true;
            } else if (isFinal(ev.state())) {
                child.closed = true;
                child.consumerDone = true;
            }
        }
    }
}
