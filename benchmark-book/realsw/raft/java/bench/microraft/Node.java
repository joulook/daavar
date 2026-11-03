package bench.microraft;

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.RaftRole;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.impl.MustRaftNodes;
import io.microraft.impl.RaftNodeImpl;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.impl.DefaultRaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.model.log.SnapshotChunk;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.RaftMessage;
import io.microraft.model.persistence.RaftEndpointPersistentState;
import io.microraft.model.persistence.RaftTermPersistentState;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * One invisible node thread {@code n<i>}: one unmodified MicroRaft {@link RaftNodeImpl} and the
 * four pieces MicroRaft leaves to its user (executor, transport, store, state machine), all owned
 * by this thread. The loop is raft-rs's harness loop: either fire one bounded election timeout
 * (an invisible {@code Nondet} choice, only while no leader is known) or receive one message;
 * then run every task the core queued, and after each task send what it produced.
 *
 * <p>No Must event is raised inside a frame of the core: the transport, the state machine and the
 * executor only record, and this loop sends after the core's task has returned. The core wraps
 * {@code Transport.send} in {@code catch (Throwable)} ({@code RaftNodeImpl.send}) and every task in
 * another ({@code RaftNodeStatusAwareTask.run}); a JMC control exception raised in there would be
 * swallowed. For the same reason a harness fault found inside a core frame is recorded and thrown
 * by this loop after the task.
 */
final class Node implements RaftNodeExecutor, Transport, StateMachine, RaftStore {

    /** The core's configuration: defaults, except a log small enough to allocate per execution. */
    private static final RaftConfig CONFIG = RaftConfig.newBuilder().setCommitCountToTakeSnapshot(100)
            .setMaxPendingLogEntryCount(16).build();

    /**
     * A fixed clock: time is never read to decide a timeout here (timeouts are the loop's choices);
     * the core reads it to stamp leader heartbeats and follower responses. 3600 s after the epoch is
     * past the core's leader-heartbeat timeout (10 s) from its initial stamp 0, so a node that has
     * heard no leader considers the leader timed out (and grants pre-votes), and a node that has heard
     * one never does (a live leader's heartbeats, which this harness does not send, would keep it so).
     */
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(3600), ZoneOffset.UTC);

    private final int me;
    private final Tid[] nodes;
    private final Tid client; // null for n2
    private final boolean reorder;
    private int budget;

    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    private final List<Runnable> timers = new ArrayList<>();
    private final List<Object[]> out = new ArrayList<>(); // {Tid, Wire.Msg}
    private final List<Long> applied = new ArrayList<>();
    private final List<Object> persisted = new ArrayList<>(); // the in-memory store's contents
    private boolean starting;
    private boolean firing;
    private String fault;
    private final RaftModelFactory factory = new DefaultRaftModelFactory();
    private RaftNodeImpl raft;

    Node(int me, Tid[] nodes, Tid client, int timeouts, boolean reorder) {
        this.me = me;
        this.nodes = nodes;
        this.client = client;
        this.budget = timeouts;
        this.reorder = reorder;
    }

    /** The thread body. Never returns: like raft-rs's node, it waits for messages forever. */
    void run() {
        List<RaftEndpoint> members = Arrays.asList(Wire.ENDPOINTS);
        RaftNodeReportListener none = r -> {};
        raft = MustRaftNodes.create(Wire.GROUP, Wire.ENDPOINTS[me], members, CONFIG, this, this, this, factory,
                this, none, new Random(me), CLOCK);
        // start(): the core runs a pre-vote round at once (RaftNodeImpl.runStart -> runPreVote). Its
        // requests are not delivered: in this harness a node campaigns only through a firing, as a
        // raft-rs node does only through its election ticks (see the README, section 1).
        starting = true;
        raft.start();
        runTasks();
        starting = false;
        ArrayDeque<Long> pending = new ArrayDeque<>();
        while (true) {
            if (budget > 0 && raft.getLeaderEndpoint() == null && raft.state().role() != RaftRole.LEADER
                    && Nondet.chooseBoolean()) {
                budget--;
                fire();
            } else {
                MpMessage m = Channel.recv(MicroRaft.M);
                if (m instanceof Wire.Msg w) {
                    raft.handle(Wire.decode(w, factory));
                    runTasks();
                } else if (m instanceof MicroRaft.Propose p) {
                    pending.add(p.v());
                } else if (m instanceof MicroRaft.Fwd f) {
                    forwarded(f.v());
                } else {
                    throw new IllegalStateException("node: unexpected " + m);
                }
            }
            // Proposals wait at the node until it knows a leader (a client library's retry), as in
            // raft-rs's harness; then the leader replicates, a follower forwards to the leader.
            while (!pending.isEmpty()) {
                RaftEndpoint leader = raft.getLeaderEndpoint();
                if (leader == null) {
                    break;
                }
                propose(leader, pending.poll());
            }
        }
    }

    /** raft-rs's MsgPropose at a node: the leader appends, a follower forwards, else dropped. */
    private void forwarded(long v) {
        RaftEndpoint leader = raft.getLeaderEndpoint();
        if (leader != null) {
            propose(leader, v);
        }
    }

    private void propose(RaftEndpoint leader, long v) {
        if (Wire.id(leader) == me) {
            raft.replicate(Long.valueOf(v)); // a failed replication loses the value (the spec may)
            runTasks();
        } else {
            Channel.send(MicroRaft.M, nodes[Wire.id(leader)], new MicroRaft.Fwd(v));
        }
    }

    /**
     * One election timeout: this node's time passes every deadline scheduled so far, so each task
     * scheduled so far runs once, in scheduling order (tasks they schedule wait for the next
     * firing). Pre-vote requests are delivered only from these tasks.
     */
    private void fire() {
        List<Runnable> due = new ArrayList<>(timers);
        timers.clear();
        firing = true;
        for (Runnable t : due) {
            t.run();
            afterTask();
        }
        firing = false;
        runTasks();
    }

    private void runTasks() {
        Runnable t;
        while ((t = tasks.poll()) != null) {
            t.run();
            afterTask();
        }
    }

    /** Sends what the task produced: its messages in order, then its applied values (one batch). */
    private void afterTask() {
        if (fault != null) {
            throw new IllegalStateException("n" + me + ": " + fault);
        }
        for (Object[] o : out) {
            Channel.send(MicroRaft.M, (Tid) o[0], (MpMessage) o[1]);
        }
        out.clear();
        if (reorder) {
            Collections.reverse(applied); // the fault: a batch delivered in reverse log order
        }
        for (long v : applied) {
            if (client != null) {
                Channel.send(MicroRaft.M, client, new MicroRaft.Committed(v));
            }
        }
        applied.clear();
    }

    private void fault(String f) {
        if (fault == null) {
            fault = f;
        }
    }

    // ------------------------------------------------------------ RaftNodeExecutor ----

    @Override
    public void execute(Runnable task) {
        tasks.add(task);
    }

    @Override
    public void submit(Runnable task) {
        tasks.add(task);
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit timeUnit) {
        timers.add(task);
    }

    // ------------------------------------------------------------------- Transport ----

    @Override
    public void send(RaftEndpoint target, RaftMessage message) {
        if (message instanceof PreVoteRequest && !firing) {
            if (!starting) {
                fault("a PreVoteRequest outside a firing and outside start()");
            }
            return; // the start-up round, not delivered
        }
        try {
            out.add(new Object[] {nodes[Wire.id(target)], Wire.encode(message)});
        } catch (RuntimeException e) {
            fault(e.getMessage());
        }
    }

    @Override
    public boolean isReachable(RaftEndpoint endpoint) {
        return true;
    }

    // ---------------------------------------------------------------- StateMachine ----

    @Override
    public Object runOperation(long commitIndex, Object operation) {
        if (operation instanceof Long v) {
            applied.add(v);
            return v;
        } else if (!(operation instanceof Wire.NewTerm)) {
            fault("applied an unknown operation " + operation);
        }
        return null;
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        fault("takeSnapshot: this harness takes no snapshot");
    }

    @Override
    public void installSnapshot(long commitIndex, List<Object> snapshotChunks) {
        fault("installSnapshot: this harness takes no snapshot");
    }

    @Override
    public Object getNewTermOperation() {
        return Wire.NEW_TERM;
    }

    // ------------------------------------------------- RaftStore (in memory) ----

    @Override
    public void persistAndFlushLocalEndpoint(RaftEndpointPersistentState s) {
        persisted.add(s);
    }

    @Override
    public void persistAndFlushInitialGroupMembers(RaftGroupMembersView m) {
        persisted.add(m);
    }

    @Override
    public void persistAndFlushTerm(RaftTermPersistentState s) {
        persisted.add(s);
    }

    @Override
    public void persistLogEntries(List<LogEntry> entries) {
        persisted.addAll(entries);
    }

    @Override
    public void persistSnapshotChunk(SnapshotChunk chunk) {
        fault("persistSnapshotChunk: this harness takes no snapshot");
    }

    @Override
    public void truncateLogEntriesFrom(long logIndexInclusive) {
        persisted.removeIf(o -> o instanceof LogEntry e && e.getIndex() >= logIndexInclusive);
    }

    @Override
    public void truncateLogEntriesUntil(long logIndexInclusive) {
        persisted.removeIf(o -> o instanceof LogEntry e && e.getIndex() <= logIndexInclusive);
    }

    @Override
    public void deleteSnapshotChunks(long logIndex, int snapshotChunkCount) {
        fault("deleteSnapshotChunks: this harness takes no snapshot");
    }

    @Override
    public void flush() {}
}
