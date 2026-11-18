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
 * (JH). Line numbers below refer to that commit. The source-to-model map {@code MAP-java.md} of the
 * a2a case study lists every collapsed thread, queue, lock and timed wait, every kept decision and
 * every deviation forced by the abstraction.
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
 * <p>Arguments: {@code impl,<a|b>[,recancel]}. In {@code a} the executor emits a task snapshot
 * first ({@code Submit}), which the stream relays as {@code TaskItem}; in {@code b} it emits
 * {@code Working} first. {@code recancel} adds a second cancel after the {@code Get}. Visible:
 * {@code client} and {@code server}, every send and receive; {@code exec} is not visible. Model:
 * fifo. The messages, the client and the executor mirror the Rust twin's ({@code msgs.rs},
 * {@code env.rs}) event for event.
 *
 * <p>Delegation, {@code impl2,<a|b>}: two instances of the same server. Instance A
 * ({@code serverA}) serves {@code client} and its executor {@code execA}. Instance B ({@code serverB})
 * serves {@code execA}, which delegates the task to it as an A2A client, and the single-server
 * executor {@code execB}. Visible: {@code client}, {@code serverA}, {@code serverB}.
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
     * @param args {@code impl} or {@code impl2}, {@code a} or {@code b}, optionally (not with
     *     {@code impl2}) {@code recancel}
     */
    public A2ATask(String... args) {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("A2ATask takes impl,<a|b>[,recancel] or impl2,<a|b>");
        }
        this.delegation = switch (args[0].trim()) {
            case "impl" -> false;
            case "impl2" -> true;
            default -> throw new IllegalArgumentException("side must be impl or impl2, was " + args[0]);
        };
        this.submitFirst = switch (args[1].trim()) {
            case "a" -> true;
            case "b" -> false;
            default -> throw new IllegalArgumentException("variant must be a or b, was " + args[1]);
        };
        if (args.length == 3 && !args[2].trim().equals("recancel")) {
            throw new IllegalArgumentException("third argument must be recancel, was " + args[2]);
        }
        this.recancel = args.length == 3;
        if (delegation && recancel) {
            throw new IllegalArgumentException("no recancel in the delegation scenario");
        }
    }

    @Override
    public String programName() {
        return "a2a-task";
    }

    @Override
    public String size() {
        return (delegation ? "2" : "") + (submitFirst ? "a" : "b") + (recancel ? "-recancel" : "");
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
        MpProcess.Handle he = exec.start(() -> exec(s));
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
                execB.start(() -> exec(b)));
        for (MpProcess.Handle h : hs) {
            h.await();
        }
    }

    /**
     * The delegating executor of server A. Towards B it is a client with requests 1 (streaming send)
     * and 2 (cancel). On {@code Exec}: in variant a {@code Submit} to A; {@code Working} to A;
     * {@code SendStream{1}} to B. From B's stream: a final {@code Status} remembers its state; a
     * {@code TaskItem}, a non-final {@code Status} or an {@code Err} does nothing (not relayed);
     * {@code StreamClose{1}}: if the remembered state is {@code Completed}, {@code Completed} to A
     * (once). {@code CancelCb} from A (at most one): {@code Cancel{2,t1}} to B. Any message for
     * request 2 is B's answer. After every message: once a callback arrived, B answered and B's stream
     * closed, exactly one {@code Canceled} to A. {@code Shutdown} from A: {@code Shutdown} to B, and
     * it ends.
     */
    private void execA(Tid a, Tid b) {
        State bFinal = null;
        boolean bClosed = false;
        boolean cb = false;
        boolean answered = false;
        boolean completedSent = false;
        boolean canceledSent = false;
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Exec x) {
                if (submitFirst) {
                    Channel.send(M, a, new Submit(x.task()));
                }
                Channel.send(M, a, new Working(x.task()));
                Channel.send(M, b, new SendStream(1));
            } else if (m instanceof CancelCb) {
                if (cb) {
                    throw new AssertionError("execA: one cancel callback");
                }
                cb = true;
                Channel.send(M, b, new Cancel(2, T1));
            } else if (m instanceof Shutdown) {
                Channel.send(M, b, new Shutdown());
                return;
            } else if (m instanceof Status st && st.req() == 1 && st.fin()) {
                bFinal = st.state();
            } else if (m instanceof TaskItem ti && ti.req() == 1
                    || m instanceof Status st && st.req() == 1
                    || m instanceof Err er && er.req() == 1) {
                // nothing: B's other stream items are not relayed
            } else if (m instanceof StreamClose sc && sc.req() == 1) {
                bClosed = true;
                if (bFinal == State.Completed && !completedSent) {
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

    /** The request a client-bound message answers or belongs to. */
    private static int reqOf(MpMessage m) {
        if (m instanceof TaskItem x) {
            return x.req();
        } else if (m instanceof Status x) {
            return x.req();
        } else if (m instanceof StreamClose x) {
            return x.req();
        } else if (m instanceof CancelOk x) {
            return x.req();
        } else if (m instanceof CancelErr x) {
            return x.req();
        } else if (m instanceof GetOk x) {
            return x.req();
        } else if (m instanceof Err x) {
            return x.req();
        }
        throw new IllegalStateException("client: unexpected " + m);
    }

    /**
     * The scripted client: a streaming send, a cancel while the stream is open, a read, with
     * {@code recancel} a second cancel of the finished task, then {@code Shutdown} to the server.
     */
    private void client(Tid server) {
        Channel.send(M, server, new SendStream(1));
        // Stream items of request 1 until Status{1,t1,Working,false}; a task item before it is consumed.
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Status st && st.req() == 1 && st.state() == State.Working && !st.fin()) {
                break;
            }
        }
        // Cancel while the stream is open; then consume everything until both the cancel's answer
        // (any message for request 2) and the end of the stream have arrived.
        Channel.send(M, server, new Cancel(2, T1));
        boolean answered = false;
        boolean closed = false;
        while (!(answered && closed)) {
            MpMessage m = Channel.recv(M);
            if (m instanceof StreamClose sc && sc.req() == 1) {
                closed = true;
            } else if (reqOf(m) == 2) {
                answered = true;
            }
        }
        Channel.send(M, server, new Get(3, T1));
        while (reqOf(Channel.recv(M)) != 3) {
            // other requests' messages are consumed
        }
        if (recancel) {
            Channel.send(M, server, new Cancel(4, T1));
            while (reqOf(Channel.recv(M)) != 4) {
                // other requests' messages are consumed
            }
        }
        Channel.send(M, server, new Shutdown());
    }

    // ---------------------------------------------------------------- exec

    /**
     * The scripted executor. On {@code Exec}: in variant a a snapshot first, then {@code Working},
     * then either {@code Completed} at once, or nothing more (an {@code execute()} that has not
     * returned: the agent is still working). On every {@code CancelCb}: exactly one {@code Canceled},
     * whatever it sent before (its {@code cancel} runs on a fresh emitter, DRH 754, so the executing
     * emitter's terminal flag does not stop it). On {@code Shutdown}: it ends.
     */
    private void exec(Tid server) {
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Exec x) {
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
            }
        }
    }

    // ---------------------------------------------------------------- server

    /** {@code TaskState.isFinal} (a2a-java {@code TaskState.java} 43-67) for the states used here. */
    private static boolean isFinal(State state) {
        return state == State.Completed || state == State.Canceled;
    }

    /**
     * An event distributed to the children: a task snapshot, a status update, or the
     * {@code InternalError} that replaces an event the store rejected.
     */
    private record Event(boolean task, State state, boolean error) {
        static Event snapshot(State state) {
            return new Event(true, state, false);
        }

        static Event status(State state) {
            return new Event(false, state, false);
        }

        static Event internalError() {
            return new Event(false, null, true);
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
