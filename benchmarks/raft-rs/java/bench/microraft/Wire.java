package bench.microraft;

import io.microraft.RaftEndpoint;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.log.LogEntry;
import io.microraft.model.message.AppendEntriesFailureResponse;
import io.microraft.model.message.AppendEntriesRequest;
import io.microraft.model.message.AppendEntriesSuccessResponse;
import io.microraft.model.message.PreVoteRequest;
import io.microraft.model.message.PreVoteResponse;
import io.microraft.model.message.RaftMessage;
import io.microraft.model.message.VoteRequest;
import io.microraft.model.message.VoteResponse;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * The harness transport's wire format: a MicroRaft message serialised into a value record (as a
 * real deployment serialises it, e.g. with protobuf) and rebuilt with the core's
 * {@link RaftModelFactory} at the receiver. Needed because JMC compares sent values by
 * {@code equals} when it replays an execution, and the core's message objects have identity
 * equality; it also keeps the node threads from sharing any mutable object. Only node-to-node
 * (invisible) messages use it. The snapshot and leadership-transfer messages are not encoded:
 * this harness takes no snapshot and transfers no leadership, and the encoder refuses them (a
 * harness fault, reported by the node loop), so their absence is checked on every run.
 */
final class Wire {

    private Wire() {}

    /** A serialised MicroRaft message. */
    sealed interface Msg extends MpMessage {}

    @Tag("PreVoteReq")
    record PreVoteReq(int from, int term, int lastLogTerm, long lastLogIndex) implements Msg {}

    @Tag("PreVoteResp")
    record PreVoteResp(int from, int term, boolean granted) implements Msg {}

    @Tag("VoteReq")
    record VoteReq(int from, int term, int lastLogTerm, long lastLogIndex, boolean sticky) implements Msg {}

    @Tag("VoteResp")
    record VoteResp(int from, int term, boolean granted) implements Msg {}

    @Tag("AppendReq")
    record AppendReq(int from, int term, int prevLogTerm, long prevLogIndex, long commitIndex,
            List<Entry> entries, long querySeq, long flowSeq) implements Msg {}

    @Tag("AppendOk")
    record AppendOk(int from, int term, long lastLogIndex, long querySeq, long flowSeq) implements Msg {}

    @Tag("AppendFail")
    record AppendFail(int from, int term, long expectedNextIndex, long querySeq, long flowSeq) implements Msg {}

    /** A log entry; its operation is a client value or the leader's new-term marker. */
    @Tag("Entry")
    record Entry(long index, int term, Op op) {}

    sealed interface Op {}

    /** A client's value, {@code Propose(v)} replicated. */
    @Tag("Value")
    record Value(long v) implements Op {}

    /**
     * The operation the state machine gives the core for a new leader's first entry
     * ({@code StateMachine.getNewTermOperation}); raft-rs's empty entry. Used as the log
     * operation itself and on the wire.
     */
    @Tag("NewTerm")
    record NewTerm() implements Op {}

    static final NewTerm NEW_TERM = new NewTerm();

    /** The group id every node is built with. */
    static final String GROUP = "microraft";

    /** The endpoint of node {@code n<id>}. */
    @Tag("Endpoint")
    record Endpoint(int id) implements RaftEndpoint {
        @Override
        public Object getId() {
            return id;
        }
    }

    static final Endpoint[] ENDPOINTS = {new Endpoint(0), new Endpoint(1), new Endpoint(2)};

    static int id(RaftEndpoint e) {
        return ((Endpoint) e).id();
    }

    /** @throws IllegalArgumentException for a message or operation this harness never sends */
    static Msg encode(RaftMessage m) {
        int from = id(m.getSender());
        if (m instanceof PreVoteRequest r) {
            return new PreVoteReq(from, r.getTerm(), r.getLastLogTerm(), r.getLastLogIndex());
        } else if (m instanceof PreVoteResponse r) {
            return new PreVoteResp(from, r.getTerm(), r.isGranted());
        } else if (m instanceof VoteRequest r) {
            return new VoteReq(from, r.getTerm(), r.getLastLogTerm(), r.getLastLogIndex(), r.isSticky());
        } else if (m instanceof VoteResponse r) {
            return new VoteResp(from, r.getTerm(), r.isGranted());
        } else if (m instanceof AppendEntriesRequest r) {
            List<Entry> es = new ArrayList<>();
            for (LogEntry e : r.getLogEntries()) {
                es.add(new Entry(e.getIndex(), e.getTerm(), op(e.getOperation())));
            }
            return new AppendReq(from, r.getTerm(), r.getPreviousLogTerm(), r.getPreviousLogIndex(),
                    r.getCommitIndex(), List.copyOf(es), r.getQuerySequenceNumber(),
                    r.getFlowControlSequenceNumber());
        } else if (m instanceof AppendEntriesSuccessResponse r) {
            return new AppendOk(from, r.getTerm(), r.getLastLogIndex(), r.getQuerySequenceNumber(),
                    r.getFlowControlSequenceNumber());
        } else if (m instanceof AppendEntriesFailureResponse r) {
            return new AppendFail(from, r.getTerm(), r.getExpectedNextIndex(), r.getQuerySequenceNumber(),
                    r.getFlowControlSequenceNumber());
        }
        throw new IllegalArgumentException("harness sends no " + m.getClass().getSimpleName());
    }

    private static Op op(Object o) {
        if (o instanceof Long v) {
            return new Value(v);
        } else if (o instanceof NewTerm n) {
            return n;
        }
        throw new IllegalArgumentException("harness replicates no operation " + o);
    }

    static RaftMessage decode(Msg w, RaftModelFactory f) {
        if (w instanceof PreVoteReq r) {
            return f.createPreVoteRequestBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setLastLogTerm(r.lastLogTerm()).setLastLogIndex(r.lastLogIndex()).build();
        } else if (w instanceof PreVoteResp r) {
            return f.createPreVoteResponseBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setGranted(r.granted()).build();
        } else if (w instanceof VoteReq r) {
            return f.createVoteRequestBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setLastLogTerm(r.lastLogTerm()).setLastLogIndex(r.lastLogIndex())
                    .setSticky(r.sticky()).build();
        } else if (w instanceof VoteResp r) {
            return f.createVoteResponseBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setGranted(r.granted()).build();
        } else if (w instanceof AppendReq r) {
            List<LogEntry> es = new ArrayList<>();
            for (Entry e : r.entries()) {
                Object op = e.op() instanceof Value v ? (Object) Long.valueOf(v.v()) : NEW_TERM;
                es.add(f.createLogEntryBuilder().setIndex(e.index()).setTerm(e.term()).setOperation(op).build());
            }
            return f.createAppendEntriesRequestBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setPreviousLogTerm(r.prevLogTerm()).setPreviousLogIndex(r.prevLogIndex())
                    .setCommitIndex(r.commitIndex()).setLogEntries(es).setQuerySequenceNumber(r.querySeq())
                    .setFlowControlSequenceNumber(r.flowSeq()).build();
        } else if (w instanceof AppendOk r) {
            return f.createAppendEntriesSuccessResponseBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setLastLogIndex(r.lastLogIndex()).setQuerySequenceNumber(r.querySeq())
                    .setFlowControlSequenceNumber(r.flowSeq()).build();
        } else if (w instanceof AppendFail r) {
            return f.createAppendEntriesFailureResponseBuilder().setGroupId(GROUP).setSender(ENDPOINTS[r.from()])
                    .setTerm(r.term()).setExpectedNextIndex(r.expectedNextIndex())
                    .setQuerySequenceNumber(r.querySeq()).setFlowControlSequenceNumber(r.flowSeq()).build();
        }
        throw new IllegalArgumentException("unknown wire message " + w);
    }
}
