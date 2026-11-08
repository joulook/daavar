package bench.cockroach;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Transliteration of CockroachDB's parallel commits with transaction recovery (cockroachdb/cockroach at d30c905,
 * the cut listed in plan/case-studies/cockroach/cr-src/README.md) to JMC's message-passing API, following the
 * program interface INTERFACE-agents.md: one committing transaction T over n keys (p of them pipelined), m
 * preventers, a epochs, delivery model fifo or bag.
 *
 * <p>Processes: {@code c}, {@code p1..pm} (visible clients, the interface's loops); {@code tc} (the
 * TxnCoordSender interceptor stack txnPipeliner -> txnCommitter -> txnSpanRefresher and the DistSender); {@code tcx}
 * (the goroutine of txnCommitter.makeTxnCommitExplicitAsync); {@code h1..hm} (each preventer's push path: PushTxn,
 * Replica.handleIndeterminateCommitError, the txnrecovery manager, the retried push, the intent resolution);
 * {@code r1}, {@code r2} (one range each: its batch evaluation and its store; r1 holds T's record); {@code init}.
 *
 * <p>Every line that differs from the Go source for a reason of the target language or the interface is marked
 * {@code // PORT:} with the reason. Go function and type names are kept as method and record names.
 */
public final class ParallelCommits implements MpProgram {

    // =====================================================================================================
    // Visible messages (INTERFACE-agents.md, "Program entries", Java)
    // =====================================================================================================

    /** c -> tc. */
    @Tag("Put")
    public record Put(int key) implements MpMessage {}

    /** c -> tc: the final-batch keys, in increasing order. */
    @Tag("Commit")
    public record Commit(List<Integer> keys) implements MpMessage {}

    /** pi -> hi. */
    @Tag("Recover")
    public record Recover() implements MpMessage {}

    /** tc -> c. */
    @Tag("Written")
    public record Written(int key) implements MpMessage {}

    /** tc -> c. */
    @Tag("Committed")
    public record Committed() implements MpMessage, Fate {}

    /** tc -> c. */
    @Tag("Retry")
    public record Retry() implements MpMessage {}

    /** tc -> c. */
    @Tag("Aborted")
    public record Aborted() implements MpMessage, Fate {}

    /** hi -> pi: committed == true for Fate Committed. */
    /** The fate of a recovered transaction: the zero-field records Committed and Aborted, written as the Rust
     *  unit variants Fate::Committed and Fate::Aborted (2026-11-08: a boolean here never matched the Rust twin). */
    sealed interface Fate permits Committed, Aborted {}
    @Tag("Recovered")
    public record Recovered(Fate fate) implements MpMessage {}

    // =====================================================================================================
    // Invisible vocabulary: roachpb / kvpb / enginepb types (only the fields the cut uses)
    // =====================================================================================================

    // PORT: uuid.UUID -> int. Only T has a record and intents; the pushers are identified for the tscache only.
    static final int NIL_TXN = 0;
    static final int TXN_T = 1;

    static int pusherTxnId(int handle) {
        return 100 + handle;
    }

    enum TransactionStatus {
        PENDING, STAGING, COMMITTED, ABORTED;

        boolean isFinalized() {
            return this == COMMITTED || this == ABORTED;
        }
    }

    enum PushTxnType { PUSH_TIMESTAMP, PUSH_ABORT }

    /** enginepb.TxnMeta (ID implicit: always T); key is the anchor key k1. Timestamps: 0 is the empty timestamp. */
    record TxnMeta(int key, int epoch, long writeTimestamp, long minTimestamp, int sequence) {
        TxnMeta withSequence(int s) {
            return new TxnMeta(key, epoch, writeTimestamp, minTimestamp, s);
        }
    }

    /** roachpb.SequencedWrite (strength always lock.Intent). */
    record SequencedWrite(int key, int sequence) {}

    /**
     * roachpb.Transaction (and TransactionRecord through {@link #asRecord}). lockSpans are point keys (no ranged
     * writes, interface).
     */
    record Transaction(int key, int epoch, long writeTimestamp, long readTimestamp, long minTimestamp,
                       int sequence, TransactionStatus status, List<Integer> lockSpans,
                       List<SequencedWrite> inFlightWrites) {

        TxnMeta txnMeta() {
            return new TxnMeta(key, epoch, writeTimestamp, minTimestamp, sequence);
        }

        Transaction withStatus(TransactionStatus s) {
            return new Transaction(key, epoch, writeTimestamp, readTimestamp, minTimestamp, sequence, s, lockSpans,
                    inFlightWrites);
        }

        Transaction withWriteTimestamp(long ts) {
            return new Transaction(key, epoch, ts, readTimestamp, minTimestamp, sequence, status, lockSpans,
                    inFlightWrites);
        }

        Transaction withSequence(int s) {
            return new Transaction(key, epoch, writeTimestamp, readTimestamp, minTimestamp, s, status, lockSpans,
                    inFlightWrites);
        }

        Transaction withLockSpans(List<Integer> ls) {
            return new Transaction(key, epoch, writeTimestamp, readTimestamp, minTimestamp, sequence, status,
                    List.copyOf(ls), inFlightWrites);
        }

        Transaction withInFlightWrites(List<SequencedWrite> w) {
            return new Transaction(key, epoch, writeTimestamp, readTimestamp, minTimestamp, sequence, status,
                    lockSpans, List.copyOf(w));
        }

        /** roachpb.Transaction.BumpReadTimestamp. */
        Transaction bumpReadTimestamp(long ts) {
            long read = Math.max(readTimestamp, ts);
            long write = Math.max(writeTimestamp, read);
            return new Transaction(key, epoch, write, read, minTimestamp, sequence, status, lockSpans,
                    inFlightWrites);
        }

        /**
         * roachpb.Transaction.AsRecord followed by AsTransaction: a TransactionRecord has no ReadTimestamp.
         * PORT: the record proto is not supplied; the fields kept are TxnMeta, Status, LockSpans, InFlightWrites.
         */
        Transaction asRecord() {
            return new Transaction(key, epoch, writeTimestamp, 0L, minTimestamp, sequence, status, lockSpans,
                    inFlightWrites);
        }

        /**
         * roachpb.Transaction.Update. PORT: pkg/roachpb/data.go is not in the cut or the checkout; this follows its
         * epoch-scoped rules (replace on a higher epoch, forward on the same epoch, only ABORTED from a lower one)
         * and forwards the timestamps.
         */
        Transaction update(Transaction o) {
            if (o == null) {
                return this;
            }
            int ep = epoch;
            TransactionStatus st = status;
            int seq = sequence;
            List<Integer> ls = lockSpans;
            List<SequencedWrite> ifw = inFlightWrites;
            if (epoch < o.epoch) {
                ep = o.epoch;
                st = o.status;
                seq = o.sequence;
                ls = o.lockSpans;
                ifw = o.inFlightWrites;
            } else if (epoch == o.epoch) {
                switch (status) {
                    case PENDING -> st = o.status;
                    case STAGING -> {
                        if (o.status != TransactionStatus.PENDING) {
                            st = o.status;
                        }
                    }
                    case ABORTED, COMMITTED -> { /* nothing to do */ }
                }
                if (sequence < o.sequence) {
                    seq = o.sequence;
                }
                if (!o.lockSpans.isEmpty()) {
                    ls = o.lockSpans;
                }
                if (!o.inFlightWrites.isEmpty()) {
                    ifw = o.inFlightWrites;
                }
            } else {
                if (o.status == TransactionStatus.ABORTED) {
                    st = TransactionStatus.ABORTED;
                }
            }
            long min = minTimestamp;
            if (o.minTimestamp != 0 && (min == 0 || o.minTimestamp < min)) {
                min = o.minTimestamp; // MinTimestamp.Backward
            }
            return new Transaction(key, ep, Math.max(writeTimestamp, o.writeTimestamp),
                    Math.max(readTimestamp, o.readTimestamp), min, seq, st, ls, ifw);
        }

        static Transaction fromMeta(TxnMeta m, TransactionStatus status) {
            return new Transaction(m.key(), m.epoch(), m.writeTimestamp(), 0L, m.minTimestamp(), m.sequence(),
                    status, List.of(), List.of());
        }
    }

    // ----- requests (kvpb.*Request) -----

    sealed interface Request permits PutRequest, QueryIntentRequest, EndTxnRequest, QueryTxnRequest,
            PushTxnRequest, RecoverTxnRequest, ResolveIntentRequest {
        int key();
    }

    record PutRequest(int key, int sequence) implements Request {}

    record QueryIntentRequest(int key, TxnMeta txn, boolean errorIfMissing) implements Request {}

    record EndTxnRequest(int key, boolean commit, List<Integer> lockSpans, List<SequencedWrite> inFlightWrites)
            implements Request {
        boolean isParallelCommit() {
            return commit && !inFlightWrites.isEmpty();
        }

        EndTxnRequest withKey(int k) {
            return new EndTxnRequest(k, commit, lockSpans, inFlightWrites);
        }

        EndTxnRequest withLocks(List<Integer> ls, List<SequencedWrite> ifw) {
            return new EndTxnRequest(key, commit, List.copyOf(ls), List.copyOf(ifw));
        }
    }

    record QueryTxnRequest(int key, TxnMeta txn) implements Request {}

    record PushTxnRequest(int key, TxnMeta pusheeTxn, long pushTo, PushTxnType pushType, int pusherTxnId)
            implements Request {}

    record RecoverTxnRequest(int key, TxnMeta txn, boolean implicitlyCommitted) implements Request {}

    record ResolveIntentRequest(int key, TxnMeta intentTxn, TransactionStatus status) implements Request {}

    // ----- responses (kvpb.*Response) -----

    sealed interface Response permits PutResponse, QueryIntentResponse, EndTxnResponse, QueryTxnResponse,
            PushTxnResponse, RecoverTxnResponse, ResolveIntentResponse {}

    record PutResponse() implements Response {}

    /** txn: the response header's Txn (set only for an own pushed intent). */
    record QueryIntentResponse(boolean foundIntent, boolean foundUnpushedIntent, Transaction txn)
            implements Response {}

    record EndTxnResponse(long stagingTimestamp) implements Response {}

    record QueryTxnResponse(Transaction queriedTxn, boolean txnRecordExists) implements Response {}

    record PushTxnResponse(Transaction pusheeTxn, boolean ambiguousAbort) implements Response {}

    record RecoverTxnResponse(Transaction recoveredTxn) implements Response {}

    record ResolveIntentResponse() implements Response {}

    // ----- errors (kvpb.Error with the details that occur, README "Errors") -----

    enum ErrKind {
        INTENT_MISSING, TRANSACTION_RETRY, TRANSACTION_ABORTED, TRANSACTION_STATUS, INDETERMINATE_COMMIT,
        AMBIGUOUS_RESULT, NO_1PC_TXN
    }

    /**
     * kvpb.Error: detail kind, reason (TransactionRetryReason, TransactionAbortedReason or
     * TransactionStatusError reason), index (-1 = nil), the error's txn, and for IndeterminateCommitError the
     * StagingTxn.
     */
    record KvError(ErrKind kind, String reason, int index, Transaction txn, Transaction stagingTxn) {
        KvError withIndex(int i) {
            return new KvError(kind, reason, i, txn, stagingTxn);
        }

        KvError withTxn(Transaction t) {
            return new KvError(kind, reason, index, t, stagingTxn);
        }

        /** kvpb.Error.UpdateTxn. */
        KvError updateTxn(Transaction o) {
            if (o == null) {
                return this;
            }
            return withTxn(txn == null ? o : txn.update(o));
        }

        static KvError of(ErrKind kind, String reason) {
            return new KvError(kind, reason, -1, null, null);
        }
    }

    // ----- the wire: batches -----

    /**
     * kvpb.BatchRequest as sent to one range. PORT: the reply address (a Go RPC's return path) is the field
     * {@code from}; {@code id} matches the reply; {@code noReply} marks the intent resolver's fire-and-forget
     * resolution (nobody waits for it).
     */
    record BatchRequest(Tid from, int id, Transaction txn, long timestamp, boolean canForwardReadTimestamp,
                        boolean noReply, List<Request> requests) implements MpMessage {}

    /** kvpb.BatchResponse, or its error. */
    record BatchResponse(int id, Transaction txn, List<Response> responses, KvError error) implements MpMessage {}

    /** init -> every invisible process. */
    record Setup(List<Tid> ranges, Tid tc, Tid tcx, List<Tid> handles, Tid c, List<Tid> preventers, TxnMeta txn)
            implements MpMessage {}

    /** tc -> tcx: the arguments of makeTxnCommitExplicitAsync (PORT: the goroutine's start). */
    record MakeTxnCommitExplicit(Transaction txn, List<Integer> lockSpans) implements MpMessage {}

    // ----- client-side batch values -----

    /** kvpb.BatchRequest before the DistSender splits it (Header: Txn, Timestamp, CanForwardReadTimestamp). */
    record Ba(Transaction txn, long timestamp, boolean canForwardReadTimestamp, List<Request> requests) {
        Ba withRequests(List<Request> rs) {
            return new Ba(txn, timestamp, canForwardReadTimestamp, List.copyOf(rs));
        }

        Ba withTxn(Transaction t) {
            return new Ba(t, timestamp, canForwardReadTimestamp, requests);
        }

        Ba withCanForwardReadTimestamp(boolean b) {
            return new Ba(txn, timestamp, b, requests);
        }

        /** BatchRequest.UpdateTxn. */
        Ba updateTxn(Transaction o) {
            return withTxn(txn == null ? o : txn.update(o));
        }

        /** BatchRequest.GetArg(EndTxn): the EndTxn, always last. */
        EndTxnRequest endTxn() {
            if (!requests.isEmpty() && requests.get(requests.size() - 1) instanceof EndTxnRequest et) {
                return et;
            }
            return null;
        }

        Ba withEndTxn(EndTxnRequest et) {
            List<Request> rs = new ArrayList<>(requests);
            rs.set(rs.size() - 1, et);
            return withRequests(rs);
        }
    }

    record Br(Transaction txn, List<Response> responses) {}

    /** PORT: Go's (*BatchResponse, *kvpb.Error) pair. */
    record Result(Br br, KvError err) {
        static Result ok(Br br) {
            return new Result(br, null);
        }

        static Result err(KvError e) {
            return new Result(null, e);
        }
    }

    // =====================================================================================================
    // Program parameters and entry
    // =====================================================================================================

    private final int n;
    private final int p;
    private final int m;
    private final int a;
    private final String modelName;
    private final CommunicationModel model;

    /**
     * Constructed from {@code impl,<n>,<p>,<m>,<a>,<fifo|bag>}.
     *
     * @param args the launcher's arguments
     */
    public ParallelCommits(String... args) {
        if (args.length != 6 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<n>,<p>,<m>,<a>,<fifo|bag>");
        }
        n = Integer.parseInt(args[1]);
        p = Integer.parseInt(args[2]);
        m = Integer.parseInt(args[3]);
        a = Integer.parseInt(args[4]);
        modelName = args[5];
        model = switch (modelName) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + modelName);
        };
        if (n < 1 || n > 2 || p < 0 || p > n || (n == 1 && p == 0) || m < 1 || m > 2 || a < 1) {
            throw new IllegalArgumentException("not an instance: n=" + n + " p=" + p + " m=" + m + " a=" + a);
        }
    }

    @Override
    public String programName() {
        return "crdb_pc";
    }

    @Override
    public String size() {
        return n + "," + p + "," + m + "," + a;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        roles.add("c");
        for (int i = 1; i <= m; i++) {
            roles.add("p" + i);
        }
        return roles;
    }

    @Override
    public void run() {
        final CommunicationModel md = model;
        // PORT: the invisible processes never terminate, so they are daemons (no role, not awaited); r1, r2,
        // tcx, tc, h1..hm, c, p1..pm, init are started in the interface's order.
        List<Tid> ranges = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            final int idx = i;
            ranges.add(MpProcess.spawnDaemon(() -> new Range(idx, md, n).run()).tid());
        }
        Tid tcx = MpProcess.spawnDaemon(() -> new ExplicitCommitTask(md, n).run()).tid();
        Tid tc = MpProcess.spawnDaemon(() -> new Coordinator(md, n).run()).tid();
        List<Tid> handles = new ArrayList<>();
        for (int i = 1; i <= m; i++) {
            final int idx = i;
            handles.add(MpProcess.spawnDaemon(() -> new Handle(idx, md, n).run()).tid());
        }
        List<MpProcess.Handle> clients = new ArrayList<>();
        final int nn = n;
        final int pp = p;
        final int aa = a;
        clients.add(MpProcess.spawn("c", () -> committer(md, nn, pp, aa, tc)));
        List<Tid> preventers = new ArrayList<>();
        for (int i = 1; i <= m; i++) {
            final Tid h = handles.get(i - 1);
            MpProcess.Handle ph = MpProcess.spawn("p" + i, () -> preventer(md, h));
            clients.add(ph);
            preventers.add(ph.tid());
        }
        // T's meta: id T, anchor key k1, epoch 0, write timestamp 1, minimum timestamp 1 (interface).
        final Setup setup = new Setup(List.copyOf(ranges), tc, tcx, List.copyOf(handles), clients.get(0).tid(),
                List.copyOf(preventers), new TxnMeta(1, 0, 1L, 1L, 0));
        MpProcess.spawnDaemon(() -> {
            // init: one setup message to every invisible process, before anything else.
            for (Tid r : setup.ranges()) {
                Channel.send(md, r, setup);
            }
            Channel.send(md, setup.tcx(), setup);
            Channel.send(md, setup.tc(), setup);
            for (Tid h : setup.handles()) {
                Channel.send(md, h, setup);
            }
        });
        for (MpProcess.Handle c : clients) {
            c.await();
        }
    }

    // ----- the clients (interface, verbatim loops) -----

    static void committer(CommunicationModel md, int n, int p, int a, Tid tc) {
        for (int e = 1; e <= a; e++) {
            for (int k = 1; k <= p; k++) {
                Channel.send(md, tc, new Put(k));
                if (!(Channel.recv(md) instanceof Written)) {
                    throw new AssertionError("Written expected");
                }
            }
            List<Integer> keys = new ArrayList<>();
            for (int k = p + 1; k <= n; k++) {
                keys.add(k);
            }
            Channel.send(md, tc, new Commit(List.copyOf(keys)));
            MpMessage rep = Channel.recv(md);
            if (rep instanceof Committed || rep instanceof Aborted) {
                return;
            } else if (rep instanceof Retry) {
                continue;
            } else {
                throw new AssertionError("Committed, Retry or Aborted expected");
            }
        }
    }

    static void preventer(CommunicationModel md, Tid h) {
        Channel.send(md, h, new Recover());
        if (!(Channel.recv(md) instanceof Recovered)) {
            throw new AssertionError("Recovered expected");
        }
    }

    // =====================================================================================================
    // Shared helpers
    // =====================================================================================================

    /** errors.AssertionFailedf and log.Fatalf. PORT: a programming error of the source becomes an AssertionError. */
    static AssertionError assertionFailed(String msg) {
        return new AssertionError("assertion failed: " + msg);
    }

    /** roachpb.MergeSpans over point keys: sorted, deduplicated. */
    static List<Integer> mergeSpans(List<Integer> spans) {
        return List.copyOf(new TreeSet<>(spans));
    }

    /** txn_interceptor_committer.go mergeIntoSpans l.~337. */
    static List<Integer> mergeIntoSpans(List<Integer> s, List<SequencedWrite> ws) {
        List<Integer> mm = new ArrayList<>(s);
        for (SequencedWrite w : ws) {
            mm.add(w.key());
        }
        return mergeSpans(mm);
    }

    /** cloneWithStatus. */
    static Transaction cloneWithStatus(Transaction txn, TransactionStatus s) {
        return txn.withStatus(s);
    }

    // =====================================================================================================
    // DistSender (dist_sender.go), shared by tc, tcx and the handles (one instance per process)
    // =====================================================================================================

    /** One partial batch in flight: the range it went to, its id, and its positions in the divided batch. */
    record Inflight(int range, int id, int[] positions) {}

    static final class DistSender {
        final CommunicationModel model;
        final List<Tid> ranges;
        final Tid self;
        int nextBatchId = 1;

        DistSender(CommunicationModel model, List<Tid> ranges, Tid self) {
            this.model = model;
            this.ranges = ranges;
            this.self = self;
        }

        /** PORT: the range descriptor cache: key ki is on range ri. */
        int rangeOf(int key) {
            return key - 1;
        }

        /** DistSender.Send l.1310-1420 (the parts loop). */
        Result send(Ba ba) {
            boolean splitET = false;
            // To ensure that we lay down intents to prevent starvation, always split the end transaction
            // request into its own batch on retries. (no Require1PC: interface)
            if (ba.txn() != null && ba.txn().epoch() > 0) {
                splitET = true;
            }
            List<List<Request>> parts = split(ba, splitET);
            List<Br> rplChunks = new ArrayList<>();
            boolean onePart = parts.size() == 1;
            int errIdxOffset = 0;
            while (!parts.isEmpty()) {
                if (!onePart) {
                    ba = ba.withRequests(parts.get(0));
                }
                EndTxnRequest et = ba.endTxn();
                boolean withCommit = et != null && et.commit();
                boolean withParallelCommit = et != null && et.isParallelCommit();
                Result r;
                if (withParallelCommit) {
                    r = divideAndSendParallelCommit(ba);
                } else {
                    r = divideAndSendBatchToRanges(ba, withCommit);
                }
                if (r.err() != null && r.err().kind() == ErrKind.NO_1PC_TXN) {
                    // If we tried to send a single round-trip EndTxn but it looks like it's going to hit
                    // multiple ranges, split it here and try again.
                    if (parts.size() != 1) {
                        throw assertionFailed("EndTxn not in last chunk of batch");
                    }
                    parts = split(ba, true);
                    onePart = false;
                    continue;
                }
                if (r.err() != null) {
                    KvError pErr = r.err();
                    if (pErr.index() != -1) {
                        pErr = pErr.withIndex(pErr.index() + errIdxOffset);
                    }
                    return Result.err(pErr);
                }
                errIdxOffset += ba.requests().size();
                rplChunks.add(r.br());
                parts = parts.subList(1, parts.size());
                if (!parts.isEmpty()) {
                    ba = ba.updateTxn(r.br().txn());
                }
            }
            List<Response> all = new ArrayList<>();
            for (Br b : rplChunks) {
                all.addAll(b.responses());
            }
            // The final update is taken and put into the response's main header.
            return Result.ok(new Br(rplChunks.get(rplChunks.size() - 1).txn(), all));
        }

        /**
         * BatchRequest.Split(canSplitET). PORT: pkg/kv/kvpb is not supplied; with only Puts, QueryIntents and
         * EndTxn in T's batches the split is: with canSplitET, everything before the EndTxn, then the EndTxn
         * alone; otherwise one part. The handles' batches are never split.
         */
        static List<List<Request>> split(Ba ba, boolean canSplitET) {
            List<List<Request>> parts = new ArrayList<>();
            if (canSplitET && ba.endTxn() != null && ba.requests().size() > 1) {
                parts.add(ba.requests().subList(0, ba.requests().size() - 1));
                parts.add(ba.requests().subList(ba.requests().size() - 1, ba.requests().size()));
            } else {
                parts.add(ba.requests());
            }
            return parts;
        }

        /** divideAndSendParallelCommit l.1567-1723. */
        Result divideAndSendParallelCommit(Ba ba) {
            // Search backwards, looking for the first pre-commit QueryIntent.
            int swapIdx = -1;
            int lastIdx = ba.requests().size() - 1;
            for (int i = lastIdx - 1; i >= 0; i--) {
                if (ba.requests().get(i) instanceof QueryIntentRequest) {
                    swapIdx = i;
                } else {
                    break;
                }
            }
            if (swapIdx == -1) {
                // No pre-commit QueryIntents. Nothing to split.
                return divideAndSendBatchToRanges(ba, true /* withCommit */);
            }
            // PORT: the swap of the EndTxn and the first pre-commit QueryIntent only arranges the two groups
            // and their positions; the groups are taken directly here and the responses are put back at their
            // original positions (l.1594-1600, l.1611-1627, l.1702-1706).
            List<Request> qiReqs = new ArrayList<>(ba.requests().subList(swapIdx, lastIdx));
            int[] qiPositions = new int[qiReqs.size()];
            for (int i = 0; i < qiPositions.length; i++) {
                qiPositions[i] = swapIdx + i;
            }
            Ba qiBa = ba.withRequests(qiReqs);
            List<Request> mainReqs = new ArrayList<>(ba.requests().subList(0, swapIdx));
            mainReqs.add(ba.requests().get(lastIdx));
            int[] mainPositions = new int[mainReqs.size()];
            for (int i = 0; i < swapIdx; i++) {
                mainPositions[i] = i;
            }
            mainPositions[swapIdx] = lastIdx;
            Ba mainBa = ba.withRequests(mainReqs);

            // PORT: `go sendPreCommit(ctx)` (l.1650-1660) and the synchronous divideAndSendBatchToRanges of the
            // EndTxn batch (l.1670) race; their sends to a common range (r1) reach it in either order. Under the
            // fifo model the order of tc's sends fixes the order of delivery, so the race is a nondeterministic
            // choice; under bag delivery already covers both orders.
            boolean preCommitFirst = true;
            if (model == CommunicationModel.P2P && sharesRange(qiBa, mainBa)) {
                preCommitFirst = Nondet.chooseBoolean();
            }
            Object qiSent;
            Object mainSent;
            if (preCommitFirst) {
                qiSent = dispatch(qiBa, true /* withCommit */);
                mainSent = dispatch(mainBa, true /* withCommit */);
            } else {
                mainSent = dispatch(mainBa, true /* withCommit */);
                qiSent = dispatch(qiBa, true /* withCommit */);
            }
            Result main = collect(mainBa, mainSent);
            // Wait for the QueryIntent-only batch to complete and stitch the responses together.
            Result qiReply = collect(qiBa, qiSent);

            // Handle error conditions.
            if (main.err() != null) {
                // The batch with the EndTxn returned an error. Ignore errors from the pre-commit QueryIntent
                // requests because that request is read-only and will produce the same errors next time.
                KvError pErr = main.err();
                if (qiReply.br() != null) {
                    pErr = pErr.updateTxn(qiReply.br().txn());
                }
                pErr = remapIndex(pErr, mainPositions); // maybeSwapErrorIndex
                return Result.err(pErr);
            }
            Br br = main.br();
            if (qiReply.err() != null) {
                KvError qiPErr = qiReply.err();
                // The batch with the pre-commit QueryIntent requests returned an error.
                boolean ignoreMissing = false;
                if (qiPErr.kind() == ErrKind.INTENT_MISSING) {
                    // If the error is an IntentMissingError, detect whether this is due to intent resolution
                    // and can be safely ignored.
                    Object detected = detectIntentMissingDueToIntentResolution(br.txn());
                    if (detected instanceof KvError err) {
                        return Result.err(err.withTxn(br.txn())); // NewErrorWithTxn(err, br.Txn)
                    }
                    ignoreMissing = (Boolean) detected;
                }
                if (!ignoreMissing) {
                    qiPErr = qiPErr.updateTxn(br.txn());
                    qiPErr = remapIndex(qiPErr, qiPositions); // maybeSwapErrorIndex
                    return Result.err(qiPErr);
                }
                // Populate the pre-commit QueryIntent batch response. If we made it here then we know we can
                // ignore intent missing errors.
                List<Response> filled = new ArrayList<>();
                for (int i = 0; i < qiReqs.size(); i++) {
                    filled.add(new QueryIntentResponse(true, true, null));
                }
                // PORT: qiBa.CreateReply() carries no Txn.
                qiReply = Result.ok(new Br(null, filled));
            }
            // Both halves of the split batch succeeded. Piece them back together.
            Response[] resps = new Response[ba.requests().size()];
            for (int i = 0; i < mainPositions.length; i++) {
                resps[mainPositions[i]] = br.responses().get(i);
            }
            for (int i = 0; i < qiPositions.length; i++) {
                resps[qiPositions[i]] = qiReply.br().responses().get(i);
            }
            Transaction txn = br.txn();
            if (qiReply.br().txn() != null) {
                txn = txn == null ? qiReply.br().txn() : txn.update(qiReply.br().txn()); // br.Combine header
            }
            List<Response> list = new ArrayList<>();
            Collections.addAll(list, resps);
            return Result.ok(new Br(txn, list));
        }

        boolean sharesRange(Ba x, Ba y) {
            for (Request r : x.requests()) {
                for (Request s : y.requests()) {
                    if (rangeOf(r.key()) == rangeOf(s.key())) {
                        return true;
                    }
                }
            }
            return false;
        }

        static KvError remapIndex(KvError e, int[] positions) {
            if (e.index() == -1) {
                return e;
            }
            return e.withIndex(positions[e.index()]);
        }

        /**
         * detectIntentMissingDueToIntentResolution l.1746-1798. PORT: (bool, error) is returned as a Boolean or
         * a KvError.
         */
        Object detectIntentMissingDueToIntentResolution(Transaction txn) {
            // PORT: ba.Timestamp = ds.clock.Now() -> the timestamp in hand, the transaction's write timestamp.
            Ba ba = new Ba(null, txn.writeTimestamp(), false,
                    List.of(new QueryTxnRequest(txn.key(), txn.txnMeta())));
            Result r = send(ba);
            if (r.err() != null) {
                // We weren't able to determine whether the intent missing error is due to intent resolution or
                // not, so it is still ambiguous whether the commit succeeded.
                return KvError.of(ErrKind.AMBIGUOUS_RESULT, "error=" + r.err().kind() + " [intent missing]");
            }
            QueryTxnResponse resp = (QueryTxnResponse) r.br().responses().get(0);
            Transaction respTxn = resp.queriedTxn();
            switch (respTxn.status()) {
                case COMMITTED:
                    // The transaction has already been finalized as committed. The missing intent error must
                    // have been a result of a concurrent transaction recovery ... so ignore the error.
                    return Boolean.TRUE;
                case ABORTED:
                    if (resp.txnRecordExists()) {
                        return KvError.of(ErrKind.TRANSACTION_ABORTED, "ABORT_REASON_ABORTED_RECORD_FOUND");
                    }
                    return KvError.of(ErrKind.AMBIGUOUS_RESULT, "intent missing and record aborted");
                default:
                    // The transaction has not been finalized yet, so the missing intent error must have been
                    // caused by a real missing intent. Propagate the missing intent error.
                    return Boolean.FALSE;
            }
        }

        /** divideAndSendBatchToRanges l.1847-2160: sends every partial batch, then combines (its defer). */
        Result divideAndSendBatchToRanges(Ba ba, boolean withCommit) {
            return collect(ba, dispatch(ba, withCommit));
        }

        /**
         * The sending half of divideAndSendBatchToRanges: one partial batch per range in key order (the
         * truncation helper keeps the requests' relative order). All but the last range are sent by
         * sendPartialBatchAsync goroutines, the last synchronously; PORT: all of them are messages in flight
         * together, and their replies are collected afterwards in range order (a local reordering of tc's
         * receives, which no other process observes). Returns the in-flight partial batches or the
         * errNo1PCTxn error.
         */
        Object dispatch(Ba ba, boolean withCommit) {
            TreeMap<Integer, List<Integer>> byRange = new TreeMap<>();
            for (int i = 0; i < ba.requests().size(); i++) {
                byRange.computeIfAbsent(rangeOf(ba.requests().get(i).key()), x -> new ArrayList<>()).add(i);
            }
            if (byRange.size() > 1) {
                // If the batch contains a non-parallel commit EndTxn and spans ranges then we want the caller to
                // come again with the EndTxn in a separate (non-concurrent) batch.
                if (withCommit) {
                    EndTxnRequest et = ba.endTxn();
                    if (et != null && !et.isParallelCommit()) {
                        return KvError.of(ErrKind.NO_1PC_TXN, "cannot send 1PC txn to multiple ranges");
                    }
                }
                // unsetCanForwardReadTimestampFlag: no request of these batches NeedsRefresh (no reads).
            }
            List<Inflight> sent = new ArrayList<>();
            for (Map.Entry<Integer, List<Integer>> e : byRange.entrySet()) {
                List<Request> sub = new ArrayList<>();
                int[] positions = new int[e.getValue().size()];
                for (int j = 0; j < positions.length; j++) {
                    positions[j] = e.getValue().get(j);
                    sub.add(ba.requests().get(positions[j]));
                }
                int id = sendPartialBatch(e.getKey(), ba, sub);
                sent.add(new Inflight(e.getKey(), id, positions));
            }
            return sent;
        }

        /** sendPartialBatch -> sendToReplicas: PORT: one message to the range's leaseholder process. */
        int sendPartialBatch(int range, Ba ba, List<Request> sub) {
            int id = nextBatchId++;
            // PORT: a transactional batch's Timestamp is its txn's ReadTimestamp (BatchRequest.SetActiveTimestamp).
            long ts = ba.txn() != null ? ba.txn().readTimestamp() : ba.timestamp();
            Channel.send(model, ranges.get(range),
                    new BatchRequest(self, id, ba.txn(), ts, ba.canForwardReadTimestamp(), false, List.copyOf(sub)));
            return id;
        }

        /** The defer of divideAndSendBatchToRanges l.1946-1990: wait for every partial batch, combine. */
        @SuppressWarnings("unchecked")
        Result collect(Ba ba, Object sentOrErr) {
            if (sentOrErr instanceof KvError e) {
                return Result.err(e);
            }
            List<Inflight> sent = (List<Inflight>) sentOrErr;
            Response[] responses = new Response[ba.requests().size()];
            Transaction brTxn = null;
            KvError pErr = null;
            for (Inflight f : sent) {
                final int id = f.id();
                BatchResponse resp = Channel.recv(model, BatchResponse.class, r -> r.id() == id);
                if (resp.error() != null) {
                    KvError re = resp.error();
                    // Re-map the error index within this partial batch back to its position in the
                    // encompassing batch.
                    if (re.index() != -1) {
                        re = re.withIndex(f.positions()[re.index()]);
                    }
                    if (pErr == null) {
                        pErr = re;
                        // Update the error's transaction with any new information from the batch response.
                        pErr = pErr.updateTxn(brTxn);
                    } else {
                        // The batch was split and saw (at least) two different errors.
                        pErr = mergeErrors(pErr, re);
                    }
                    continue;
                }
                if (pErr == null) {
                    // br.Combine(ctx, resp.reply, resp.positions, ba)
                    for (int j = 0; j < f.positions().length; j++) {
                        responses[f.positions()[j]] = resp.responses().get(j);
                    }
                    brTxn = brTxn == null ? resp.txn() : (resp.txn() == null ? brTxn : brTxn.update(resp.txn()));
                } else {
                    pErr = pErr.updateTxn(resp.txn());
                }
            }
            if (pErr != null) {
                return Result.err(pErr);
            }
            List<Response> list = new ArrayList<>();
            Collections.addAll(list, responses);
            return Result.ok(new Br(brTxn, list));
        }

        /**
         * mergeErrors l.1819-1831. PORT: kvpb.ErrPriority is not supplied; only errors of one kind meet here
         * (two IntentMissingErrors of the pre-commit QueryIntents), so the priorities are equal and the lower
         * index wins.
         */
        static KvError mergeErrors(KvError pErr1, KvError pErr2) {
            KvError ret = pErr1;
            KvError drop = pErr2;
            boolean hasIndex = ret.index() != -1 && drop.index() != -1;
            if (hasIndex && drop.index() < ret.index()) {
                ret = pErr2;
                drop = pErr1;
            }
            if (ret.kind() != drop.kind()) {
                throw assertionFailed("mergeErrors of different kinds without ErrPriority: " + ret + " " + drop);
            }
            return ret.updateTxn(drop.txn());
        }
    }

    // =====================================================================================================
    // txnSpanRefresher (txn_interceptor_span_refresher.go), the layer below the committer, shared by tc and tcx.
    // PORT: T performs no reads, so the refresh footprint is always empty and valid; the interceptor has no
    // state left and its methods take the DistSender (its wrapped sender, via the txnLockGatekeeper).
    // =====================================================================================================

    static final int MAX_TXN_REFRESH_ATTEMPTS = 5; // maxTxnRefreshAttempts l.30

    /** txnSpanRefresher.SendLocked l.150-188. */
    static Result refresherSendLocked(DistSender ds, Ba ba) {
        // Set the batch's CanForwardReadTimestamp flag.
        // canForwardReadTimestampWithoutRefresh: no refresh spans, refresh valid, commit timestamp not fixed.
        ba = ba.withCanForwardReadTimestamp(true);
        // Attempt a refresh before sending the batch.
        Preemptive pre = maybeRefreshPreemptively(ba);
        if (pre.err() != null) {
            return Result.err(pre.err());
        }
        ba = pre.ba();
        // Send through wrapped lockedSender.
        Result r = sendLockedWithRefreshAttempts(ds, ba, MAX_TXN_REFRESH_ATTEMPTS);
        if (r.err() != null) {
            return r;
        }
        // If the transaction is no longer pending, just return without attempting to record its refresh spans.
        // (appendRefreshSpans: Puts, QueryIntents and EndTxn need no refresh.)
        return r;
    }

    /** PORT: Go's (*BatchRequest, *kvpb.Error) pair. */
    record Preemptive(Ba ba, KvError err) {}

    /** maybeRefreshPreemptively l.411-498. */
    static Preemptive maybeRefreshPreemptively(Ba ba) {
        // If the transaction has yet to be pushed, no refresh is necessary.
        if (ba.txn().readTimestamp() == ba.txn().writeTimestamp()) {
            return new Preemptive(ba, null);
        }
        // If true, tryRefreshTxnSpans will trivially succeed.
        boolean refreshFree = ba.canForwardReadTimestamp();
        // If true, this batch is guaranteed to fail without a refresh.
        EndTxnRequest et = ba.endTxn();
        boolean refreshInevitable = et != null && et.commit()
                && !TOLERATES_WRITE_SKEW; // serializable
        // If neither condition is true, defer the refresh.
        if (!refreshFree && !refreshInevitable) {
            return new Preemptive(ba, null);
        }
        // If the transaction cannot change its read timestamp, no refresh is possible.
        // (canForwardReadTimestamp: the commit timestamp is never fixed here.)
        Transaction refreshToTxn = ba.txn().bumpReadTimestamp(ba.txn().writeTimestamp());
        // Try refreshing the txn spans at a timestamp that will allow us to commit.
        // tryRefreshTxnSpans: refreshFootprint.empty() -> success.
        ba = ba.updateTxn(refreshToTxn);
        return new Preemptive(ba, null);
    }

    static final boolean TOLERATES_WRITE_SKEW = false; // IsoLevel SERIALIZABLE

    /** sendLockedWithRefreshAttempts l.226-245. */
    static Result sendLockedWithRefreshAttempts(DistSender ds, Ba ba, int maxRefreshAttempts) {
        Result r = ds.send(ba);
        // Check for server-side refresh: forwardRefreshTimestampOnResponse (no refresh spans to track).
        if (r.err() != null) {
            if (maxRefreshAttempts > 0) {
                r = maybeRefreshAndRetrySend(ds, ba, r.err(), maxRefreshAttempts);
            }
        }
        return r;
    }

    /**
     * kvpb.TransactionRefreshTimestamp. PORT: pkg/kv/kvpb is not supplied; of its cases only
     * TransactionRetryError with RETRY_SERIALIZABLE or RETRY_WRITE_TOO_OLD can occur here (no WriteTooOld, no
     * uncertainty).
     */
    static long transactionRefreshTimestamp(KvError pErr) {
        Transaction txn = pErr.txn();
        if (txn == null) {
            return -1;
        }
        if (pErr.kind() == ErrKind.TRANSACTION_RETRY
                && (pErr.reason().equals("RETRY_SERIALIZABLE") || pErr.reason().equals("RETRY_WRITE_TOO_OLD"))) {
            return txn.writeTimestamp();
        }
        return -1;
    }

    /** maybeRefreshAndRetrySend l.249-324. */
    static Result maybeRefreshAndRetrySend(DistSender ds, Ba ba, KvError pErr, int maxRefreshAttempts) {
        Transaction txn = pErr.txn();
        if (txn == null /* || !sr.canForwardReadTimestamp(txn): always true */) {
            return Result.err(pErr);
        }
        // Check for an error which can be refreshed away to avoid a client-side transaction restart.
        long refreshTS = transactionRefreshTimestamp(pErr);
        if (refreshTS < 0) {
            return Result.err(pErr);
        }
        EndTxnRequest endTxnArg = ba.endTxn();
        boolean hasET = endTxnArg != null;
        boolean bumpedRefreshTimestampRequired = hasET && txn.status() == TransactionStatus.STAGING
                && TOLERATES_WRITE_SKEW;
        if (bumpedRefreshTimestampRequired) {
            refreshTS = refreshTS + 1; // Next()
        }
        Transaction refreshToTxn = txn.bumpReadTimestamp(refreshTS);
        switch (refreshToTxn.status()) {
            case PENDING:
                break;
            case STAGING:
                // If the batch resulted in an error but the EndTxn request succeeded, staging the transaction
                // record in the process, downgrade the status back to PENDING.
                refreshToTxn = refreshToTxn.withStatus(TransactionStatus.PENDING);
                break;
            default:
                throw assertionFailed("unexpected txn status during refresh: " + refreshToTxn);
        }
        // Try refreshing the txn spans so we can retry. (tryRefreshTxnSpans: empty footprint -> success)
        // We've refreshed all of the read spans successfully and bumped ba.Txn's timestamps. Attempt the
        // request again.
        ba = ba.updateTxn(refreshToTxn);
        // To prevent starvation of batches and to ensure the correctness of parallel commits, split off the
        // EndTxn request into its own batch on auto-retries.
        if (ba.requests().size() > 1 && hasET /* && !Require1PC */) {
            return splitEndTxnAndRetrySend(ds, ba);
        }
        Result retry = sendLockedWithRefreshAttempts(ds, ba, maxRefreshAttempts - 1);
        if (retry.err() != null) {
            return retry;
        }
        return retry;
    }

    /** splitEndTxnAndRetrySend l.367-410. */
    static Result splitEndTxnAndRetrySend(DistSender ds, Ba ba) {
        // Issue a batch up to but not including the EndTxn request.
        int etIdx = ba.requests().size() - 1;
        Ba baPrefix = ba.withRequests(ba.requests().subList(0, etIdx));
        Result prefix = refresherSendLocked(ds, baPrefix);
        if (prefix.err() != null) {
            return prefix;
        }
        // Issue a batch containing only the EndTxn request.
        Ba baSuffix = ba.withRequests(ba.requests().subList(etIdx, etIdx + 1)).updateTxn(prefix.br().txn());
        EndTxnRequest et = baSuffix.endTxn();
        if (et.isParallelCommit()) {
            et = et.withLocks(mergeIntoSpans(et.lockSpans(), et.inFlightWrites()), List.of());
            baSuffix = baSuffix.withEndTxn(et);
        }
        Result suffix = refresherSendLocked(ds, baSuffix);
        if (suffix.err() != null) {
            return suffix;
        }
        // Combine the responses.
        List<Response> resps = new ArrayList<>(prefix.br().responses());
        resps.add(suffix.br().responses().get(0));
        Transaction txn = prefix.br().txn() == null ? suffix.br().txn() : prefix.br().txn().update(suffix.br().txn());
        if (txn == null || !txn.status().isFinalized()) {
            throw assertionFailed("txn status not finalized after successful retried EndTxn: " + txn);
        }
        return Result.ok(new Br(txn, resps));
    }

    // =====================================================================================================
    // tc: TxnCoordSender with txnPipeliner -> txnCommitter -> txnSpanRefresher -> DistSender
    // =====================================================================================================

    /** inFlightWrite of the pipeliner's inFlightWriteSet (a btree by key; one write per key here). */
    static final class InFlightWrite {
        final int key;
        final int sequence;
        int chainIndex;

        InFlightWrite(int key, int sequence) {
            this.key = key;
            this.sequence = sequence;
        }
    }

    static final class Coordinator {
        final CommunicationModel model;
        final int n;
        Setup setup;
        DistSender ds;

        // tc.mu.txn
        Transaction txn;
        // txnSeqNumAllocator.writeSeq
        int writeSeq;
        // txnPipeliner state
        final TreeMap<Integer, InFlightWrite> ifWrites = new TreeMap<>();
        final TreeSet<Integer> lockFootprint = new TreeSet<>();
        int ifWritesChainIndex;
        // PORT: makeTxnCommitExplicitAsync's goroutine is started (a message to tcx) after tc replied Committed.
        MakeTxnCommitExplicit pendingExplicitCommit;

        Coordinator(CommunicationModel model, int n) {
            this.model = model;
            this.n = n;
        }

        void run() {
            Tid self = MpProcess.self();
            setup = Channel.recv(model, Setup.class);
            ds = new DistSender(model, setup.ranges(), self);
            txn = Transaction.fromMeta(setup.txn(), TransactionStatus.PENDING).bumpReadTimestamp(1L);
            while (true) {
                MpMessage req = Channel.recv(model, mm -> mm instanceof Put || mm instanceof Commit);
                if (req instanceof Put put) {
                    // txnSeqNumAllocator: an intent write gets the next write sequence.
                    List<Request> reqs = List.of(new PutRequest(put.key(), ++writeSeq));
                    MpMessage rep = send(new Ba(null, 0L, false, reqs));
                    Channel.send(model, setup.c(), rep == null ? new Written(put.key()) : rep);
                } else {
                    Commit commit = (Commit) req;
                    List<Request> reqs = new ArrayList<>();
                    for (int k : commit.keys()) {
                        reqs.add(new PutRequest(k, ++writeSeq));
                    }
                    reqs.add(new EndTxnRequest(0 /* et.Key assigned by the committer */, true, List.of(), List.of()));
                    MpMessage rep = send(new Ba(null, 0L, false, reqs));
                    Channel.send(model, setup.c(), rep == null ? new Committed() : rep);
                    if (rep == null && pendingExplicitCommit != null) {
                        Channel.send(model, setup.tcx(), pendingExplicitCommit);
                        pendingExplicitCommit = null;
                    }
                }
            }
        }

        /**
         * TxnCoordSender.Send l.510-600 and updateStateLocked l.985-1106, mapped to the client's replies:
         * null for success (Written / Committed), Retry for a retryable error that bumps the epoch, Aborted for
         * a retryable error that aborts the transaction.
         */
        MpMessage send(Ba ba) {
            // Clone the Txn's Proto so that future modifications can be made without worrying about
            // synchronization.
            ba = ba.withTxn(txn);
            // Send the command through the txnInterceptor stack.
            Result r = pipelinerSendLocked(ba);
            KvError pErr = r.err();
            if (pErr == null) {
                txn = txn.update(r.br().txn());
                return null;
            }
            if (transactionRestart(pErr)) {
                return handleRetryableErrLocked(pErr);
            }
            // This is the non-retriable error case.
            // PORT: the client interface has no reply for a non-retryable error (TransactionStatusError,
            // AmbiguousResultError); one reaching the client is reported as a failure.
            throw new AssertionError("non-retryable error returned to the client: " + pErr);
        }

        /** kvpb.Error.TransactionRestart() != NONE. PORT: kvpb not supplied. */
        static boolean transactionRestart(KvError e) {
            return e.kind() == ErrKind.TRANSACTION_RETRY || e.kind() == ErrKind.TRANSACTION_ABORTED;
        }

        /** handleRetryableErrLocked l.864-945 and handleTransactionRetryWithProtoRefreshErrorErrLocked. */
        MpMessage handleRetryableErrLocked(KvError pErr) {
            if (pErr.kind() == ErrKind.TRANSACTION_ABORTED) {
                // If the previous transaction is aborted, it means we had to start a new transaction and the old
                // one is toast. PORT: the client's restart with a new transaction is cut; T's fate is observed.
                txn = txn.withStatus(TransactionStatus.ABORTED);
                return new Aborted();
            }
            // kvpb.PrepareTransactionForRetry. PORT: kvpb not supplied; for a TransactionRetryError the next txn
            // is the error's txn restarted (epoch+1, read := write timestamp, PENDING); for RETRY_SERIALIZABLE it
            // is first forwarded to clock.Now(), here the timestamp in hand (no change).
            Transaction prev = pErr.txn() != null ? pErr.txn() : txn;
            if (prev.status().isFinalized()) {
                throw assertionFailed("unexpected retryable error for finalized txn: " + prev);
            }
            Transaction next = new Transaction(prev.key(), prev.epoch() + 1, prev.writeTimestamp(),
                    prev.writeTimestamp(), prev.minTimestamp(), 0, TransactionStatus.PENDING, List.of(), List.of());
            // This is where we get a new read timestamp or a new epoch.
            txn = txn.update(next);
            // Reset state if this is a retryable txn error that is incrementing the transaction's epoch.
            pipelinerEpochBumpedLocked();
            // txnCommitter.epochBumpedLocked, txnSpanRefresher.epochBumpedLocked: no state here.
            return new Retry();
        }

        /** txnPipeliner.epochBumpedLocked: move the in-flight writes into the lock footprint. */
        void pipelinerEpochBumpedLocked() {
            for (InFlightWrite w : ifWrites.values()) {
                lockFootprint.add(w.key);
            }
            ifWrites.clear();
        }

        // ----- txnPipeliner -----

        /** txnPipeliner.SendLocked l.311-361. */
        Result pipelinerSendLocked(Ba ba) {
            // If an EndTxn request is part of this batch, attach the in-flight writes and the lock footprint.
            ba = attachLocksToEndTxn(ba);
            // maybeRejectOverBudget: budgets are never exceeded.
            boolean asyncConsensus = canUseAsyncConsensus(ba);
            // Adjust the batch so that it doesn't miss any in-flight writes.
            ba = chainToInFlightWrites(ba);
            // Send through wrapped lockedSender.
            Result r = committerSendLocked(ba);
            // Update the in-flight write set and the lock footprint with the results of the request.
            updateLockTrackingInner(ba, asyncConsensus, r.br(), r.err());
            if (r.err() != null) {
                return Result.err(adjustError(ba, r.err()));
            }
            return Result.ok(stripQueryIntents(r.br()));
        }

        /** attachLocksToEndTxn l.437-501. */
        Ba attachLocksToEndTxn(Ba ba) {
            EndTxnRequest et = ba.endTxn();
            if (et == null) {
                return ba;
            }
            if (!et.lockSpans().isEmpty()) {
                throw assertionFailed("client must not pass intents to EndTxn");
            }
            if (!et.inFlightWrites().isEmpty()) {
                throw assertionFailed("client must not pass in-flight writes to EndTxn");
            }
            // Populate et.LockSpans and et.InFlightWrites.
            List<Integer> lockSpans = new ArrayList<>(lockFootprint);
            List<SequencedWrite> inFlight = new ArrayList<>();
            for (InFlightWrite w : ifWrites.values()) {
                inFlight.add(new SequencedWrite(w.key, w.sequence));
            }
            // Augment et.LockSpans and et.InFlightWrites with writes from the current batch.
            for (Request req : ba.requests().subList(0, ba.requests().size() - 1)) {
                if (req instanceof PutRequest put) {
                    // Point writes are added to the in-flight writes set.
                    inFlight.add(new SequencedWrite(put.key(), put.sequence()));
                }
            }
            // Sort both sets and condense the lock spans.
            lockSpans = mergeSpans(lockSpans);
            inFlight.sort((x, y) -> Integer.compare(x.sequence(), y.sequence()));
            return ba.withEndTxn(et.withLocks(lockSpans, inFlight));
        }

        /** canUseAsyncConsensus l.505-590: EndTxn batches never; T's other batches are single Puts. */
        boolean canUseAsyncConsensus(Ba ba) {
            if (ba.endTxn() != null) {
                return false;
            }
            // PipelinedWritesEnabled, not disabled, budget never exceeded; every request is a Put (CanPipeline).
            for (Request req : ba.requests()) {
                if (!(req instanceof PutRequest)) {
                    return false;
                }
            }
            return true;
        }

        /** chainToInFlightWrites l.608-718. */
        Ba chainToInFlightWrites(Ba ba) {
            // If there are no in-flight writes, there's nothing to chain to.
            if (ifWrites.isEmpty()) {
                return ba;
            }
            boolean forked = false;
            List<Request> oldReqs = ba.requests();
            List<Request> newReqs = new ArrayList<>();
            ifWritesChainIndex++;
            int chainIndex = ifWritesChainIndex;
            int chainCount = 0;
            for (int i = 0; i < oldReqs.size(); i++) {
                Request req = oldReqs.get(i);
                if (ifWrites.size() > chainCount) {
                    List<InFlightWrite> toChain = new ArrayList<>();
                    if (req instanceof EndTxnRequest et) {
                        if (et.commit()) {
                            // EndTxns need to prove all in-flight writes before being allowed to succeed.
                            toChain.addAll(ifWrites.values());
                        }
                    } else {
                        // Transactional reads and writes needs to chain on to any overlapping in-flight writes.
                        InFlightWrite w = ifWrites.get(req.key());
                        if (w != null) {
                            toChain.add(w);
                        }
                    }
                    for (InFlightWrite w : toChain) {
                        if (!forked) {
                            newReqs.addAll(oldReqs.subList(0, i));
                            forked = true;
                        }
                        if (w.chainIndex != chainIndex) {
                            TxnMeta meta = ba.txn().txnMeta().withSequence(w.sequence);
                            newReqs.add(new QueryIntentRequest(w.key, meta, true /* ErrorIfMissing */));
                            w.chainIndex = chainIndex;
                            chainCount++;
                        }
                    }
                }
                if (forked) {
                    newReqs.add(req);
                }
            }
            return forked ? ba.withRequests(newReqs) : ba;
        }

        /** updateLockTrackingInner l.796-899 (lock-span budget handling of updateLockTracking: never needed). */
        void updateLockTrackingInner(Ba ba, boolean asyncConsensus, Br br, KvError pErr) {
            // If the request failed, add all lock acquisitions attempts directly to the lock footprint.
            if (pErr != null) {
                // PORT: the ErrPriority-based stripping of the failed request (l.815-819) needs kvpb.ErrPriority,
                // which is not supplied; all of the batch's lock spans are added (the conservative branch).
                for (Request req : ba.requests()) {
                    if (req instanceof PutRequest put) {
                        lockFootprint.add(put.key());
                    }
                }
                return;
            }
            // Similarly, if the transaction is now finalized, we don't need to accurately update the lock tracking.
            if (br.txn().status().isFinalized()) {
                switch (br.txn().status()) {
                    case ABORTED -> {
                        for (Request req : ba.requests()) {
                            if (req instanceof PutRequest put) {
                                lockFootprint.add(put.key());
                            }
                        }
                    }
                    case COMMITTED -> ifWrites.clear();
                    default -> throw new IllegalStateException("unexpected");
                }
                return;
            }
            for (int i = 0; i < ba.requests().size(); i++) {
                Request req = ba.requests().get(i);
                Response resp = br.responses().get(i);
                if (req instanceof QueryIntentRequest qiReq) {
                    // Remove any in-flight writes that were proven to exist.
                    QueryIntentResponse qiResp = (QueryIntentResponse) resp;
                    if (qiResp.foundIntent()) {
                        InFlightWrite w = ifWrites.get(qiReq.key());
                        if (w != null && w.sequence == qiReq.txn().sequence()) {
                            ifWrites.remove(qiReq.key());
                        }
                        // Move to lock footprint.
                        lockFootprint.add(qiReq.key());
                    }
                } else if (req instanceof PutRequest put) {
                    // If the request intended to acquire locks, track its lock spans.
                    if (asyncConsensus) {
                        // Record any writes that were performed asynchronously.
                        ifWrites.put(put.key(), new InFlightWrite(put.key(), put.sequence()));
                    } else {
                        lockFootprint.add(put.key());
                    }
                }
            }
        }

        /** stripQueryIntents. */
        static Br stripQueryIntents(Br br) {
            List<Response> out = new ArrayList<>();
            for (Response r : br.responses()) {
                if (!(r instanceof QueryIntentResponse)) {
                    out.add(r);
                }
            }
            return new Br(br.txn(), out);
        }

        /** adjustError l.929-954. */
        static KvError adjustError(Ba ba, KvError pErr) {
            // Fix the error index to hide the impact of any QueryIntent requests.
            if (pErr.index() != -1) {
                int before = 0;
                for (Request req : ba.requests().subList(0, pErr.index())) {
                    if (req instanceof QueryIntentRequest) {
                        before++;
                    }
                }
                pErr = pErr.withIndex(pErr.index() - before);
            }
            // Turn an IntentMissingError into a transactional retry error.
            if (pErr.kind() == ErrKind.INTENT_MISSING) {
                return new KvError(ErrKind.TRANSACTION_RETRY, "RETRY_ASYNC_WRITE_FAILURE", pErr.index(), pErr.txn(),
                        null);
            }
            return pErr;
        }

        // ----- txnCommitter -----

        /** txnCommitter.SendLocked l.138-261. */
        Result committerSendLocked(Ba ba) {
            // If the batch does not include an EndTxn request, pass it through.
            EndTxnRequest et = ba.endTxn();
            if (et == null) {
                return refresherSendLocked(ds, ba);
            }
            // validateEndTxnBatch: MaxSpanRequestKeys == 0.
            // Determine whether we can elide the EndTxn entirely.
            if (et.lockSpans().isEmpty() && et.inFlightWrites().isEmpty()) {
                throw assertionFailed("read-only EndTxn: T always writes"); // sendLockedWithElidedEndTxn: cut
            }
            // Assign the transaction's key to the Request's header.
            if (et.key() != 0) {
                throw assertionFailed("client must not assign Key to EndTxn");
            }
            et = et.withKey(ba.txn().key());
            // Determine whether the commit request can be run in parallel with the rest of the batch.
            if (!et.inFlightWrites().isEmpty() && !canCommitInParallel(ba, et)) {
                et = et.withLocks(mergeIntoSpans(et.lockSpans(), et.inFlightWrites()), List.of());
            }
            ba = ba.withEndTxn(et);
            // If the EndTxn request is a rollback, pass it through.
            if (!et.commit()) {
                return refresherSendLocked(ds, ba);
            }
            // Send the adjusted batch through the wrapped lockedSender.
            Result r = refresherSendLocked(ds, ba);
            if (r.err() != null) {
                return Result.err(maybeRemoveStagingStatusInErr(r.err()));
            }
            Br br = r.br();
            // Determine next steps based on the status of the transaction.
            switch (br.txn().status()) {
                case STAGING:
                    // Continue with STAGING-specific validation and cleanup.
                    break;
                case COMMITTED:
                    // The transaction is explicitly committed.
                    return Result.ok(br);
                default:
                    throw assertionFailed("unexpected response status without error: " + br.txn());
            }
            // Determine whether the transaction satisfies the implicit commit condition.
            boolean implicitCommit = isTxnCommitImplicit(br);
            // Retry the EndTxn request (and nothing else) if the transaction does not satisfy the implicit
            // commit condition.
            if (!implicitCommit) {
                return retryTxnCommitAfterFailedParallelCommit(ba, br);
            }
            // If the transaction doesn't need to retry then it is implicitly committed!
            List<Integer> mergedLockSpans = mergeIntoSpans(et.lockSpans(), et.inFlightWrites());
            makeTxnCommitExplicitAsync(br.txn(), mergedLockSpans);
            // Switch the status on the batch response's transaction to COMMITTED.
            return Result.ok(new Br(cloneWithStatus(br.txn(), TransactionStatus.COMMITTED), br.responses()));
        }

        /** maybeRemoveStagingStatusInErr. */
        static KvError maybeRemoveStagingStatusInErr(KvError pErr) {
            if (pErr.txn() != null && pErr.txn().status() == TransactionStatus.STAGING) {
                return pErr.withTxn(pErr.txn().withStatus(TransactionStatus.PENDING));
            }
            return pErr;
        }

        /** canCommitInParallel l.349-396. */
        static boolean canCommitInParallel(Ba ba, EndTxnRequest et) {
            // parallelCommitsEnabled: true (interface).
            if (!et.commit()) {
                return false;
            }
            // et.Prepare, et.InternalCommitTrigger: never set (interface).
            for (Request req : ba.requests().subList(0, ba.requests().size() - 1)) {
                if (req instanceof PutRequest) {
                    // The request can be part of a batch that is committed in parallel.
                } else if (req instanceof QueryIntentRequest) {
                    // QueryIntent requests are compatible with parallel commits.
                } else {
                    return false;
                }
            }
            return true;
        }

        /** isTxnCommitImplicit l.413-441. */
        static boolean isTxnCommitImplicit(Br br) {
            if (br.responses().isEmpty()) {
                throw assertionFailed("no responses in BatchResponse");
            }
            Response lastResp = br.responses().get(br.responses().size() - 1);
            if (!(lastResp instanceof EndTxnResponse etResp)) {
                throw assertionFailed("unexpected response in BatchResponse: " + lastResp);
            }
            if (etResp.stagingTimestamp() == 0) {
                throw assertionFailed("empty StagingTimestamp in EndTxnResponse");
            }
            boolean failed = etResp.stagingTimestamp() < br.txn().writeTimestamp();
            return !failed;
        }

        /** retryTxnCommitAfterFailedParallelCommit l.443-486. */
        Result retryTxnCommitAfterFailedParallelCommit(Ba ba, Br br) {
            // Issue a batch containing only the EndTxn request.
            int etIdx = ba.requests().size() - 1;
            Ba baSuffix = ba.withRequests(ba.requests().subList(etIdx, etIdx + 1))
                    .withTxn(cloneWithStatus(br.txn(), TransactionStatus.PENDING));
            EndTxnRequest et = baSuffix.endTxn();
            et = et.withLocks(mergeIntoSpans(et.lockSpans(), et.inFlightWrites()), List.of());
            baSuffix = baSuffix.withEndTxn(et);
            Result suffix = refresherSendLocked(ds, baSuffix);
            if (suffix.err() != null) {
                return Result.err(maybeRemoveStagingStatusInErr(suffix.err()));
            }
            // Combine the responses.
            List<Response> resps = new ArrayList<>(br.responses());
            resps.set(etIdx, suffix.br().responses().get(0));
            Transaction txn2 = br.txn().update(suffix.br().txn());
            if (txn2 == null || !txn2.status().isFinalized()) {
                throw assertionFailed("txn status not finalized after successful retried EndTxn: " + txn2);
            }
            return Result.ok(new Br(txn2, resps));
        }

        /**
         * makeTxnCommitExplicitAsync l.492-527. PORT: the goroutine is the process tcx; the stopper handle is
         * cut; it is started by a message sent after tc's reply to c (interface).
         */
        void makeTxnCommitExplicitAsync(Transaction t, List<Integer> lockSpans) {
            pendingExplicitCommit = new MakeTxnCommitExplicit(t, List.copyOf(lockSpans));
        }
    }

    // =====================================================================================================
    // tcx: the goroutine of makeTxnCommitExplicitAsync
    // =====================================================================================================

    static final class ExplicitCommitTask {
        final CommunicationModel model;
        final int n;

        ExplicitCommitTask(CommunicationModel model, int n) {
            this.model = model;
            this.n = n;
        }

        void run() {
            Tid self = MpProcess.self();
            Setup setup = Channel.recv(model, Setup.class);
            DistSender ds = new DistSender(model, setup.ranges(), self);
            while (true) {
                MakeTxnCommitExplicit work = Channel.recv(model, MakeTxnCommitExplicit.class);
                // tc.mu.Lock(): PORT: tc is idle by then (its client has finished).
                KvError err = makeTxnCommitExplicitLocked(ds, work.txn(), work.lockSpans());
                if (err != null) {
                    // log.Dev.Errorf(ctx, "making txn commit explicit failed for %s: %v", txn, err)
                }
            }
        }

        /** makeTxnCommitExplicitLocked l.529-594; s is tc.wrapped, the span refresher. */
        static KvError makeTxnCommitExplicitLocked(DistSender ds, Transaction txn, List<Integer> lockSpans) {
            // Construct a new batch with just an EndTxn request.
            EndTxnRequest et = new EndTxnRequest(txn.key(), true, List.copyOf(lockSpans), List.of());
            Ba ba = new Ba(txn, 0L, false, List.of(et));
            Result r = refresherSendLocked(ds, ba);
            KvError pErr = r.err();
            if (pErr != null) {
                switch (pErr.kind()) {
                    case TRANSACTION_STATUS:
                        // Detect whether the error indicates that someone else beat us to explicitly committing
                        // the transaction record.
                        if (pErr.reason().equals("REASON_TXN_COMMITTED")) {
                            return null;
                        }
                        break;
                    case TRANSACTION_RETRY:
                        // logFunc(ctx, "unexpected retry error when making commit explicit ...") (not a race build)
                        break;
                    default:
                        break;
                }
                return pErr;
            }
            return null;
        }
    }

    // =====================================================================================================
    // hi: preventer i's push path
    // =====================================================================================================

    static final int DEFAULT_BATCH_SIZE = 128; // txnrecovery manager.go l.52

    static final class Handle {
        final int index;
        final CommunicationModel model;
        final int n;
        DistSender ds;

        Handle(int index, CommunicationModel model, int n) {
            this.index = index;
            this.model = model;
            this.n = n;
        }

        void run() {
            Tid self = MpProcess.self();
            Setup setup = Channel.recv(model, Setup.class);
            ds = new DistSender(model, setup.ranges(), self);
            Recover rec = Channel.recv(model, Recover.class);
            // The pushee's meta as read from one of T's intents (interface: the setup's meta).
            TxnMeta pushee = setup.txn();
            // PORT: the push is issued as a non-transactional batch by the pusher (the lock-table waiter is cut);
            // header timestamp and PushTo: the timestamp in hand, the pushee's write timestamp.
            Ba pushBa = new Ba(null, pushee.writeTimestamp(), false,
                    List.of(new PushTxnRequest(pushee.key(), pushee, pushee.writeTimestamp(),
                            PushTxnType.PUSH_ABORT, pusherTxnId(index))));
            Transaction finalized;
            // Replica.executeBatchWithConcurrencyRetries l.593-600: after a successful recovery the push is
            // retried. PORT: the replica's retry loop runs in the handle (interface).
            while (true) {
                Result r = ds.send(pushBa);
                if (r.err() == null) {
                    finalized = ((PushTxnResponse) r.br().responses().get(0)).pusheeTxn();
                    break;
                }
                if (r.err().kind() == ErrKind.INDETERMINATE_COMMIT) {
                    KvError pErr = handleIndeterminateCommitError(r.err());
                    if (pErr != null) {
                        throw new AssertionError("push failed after indeterminate commit: " + pErr);
                    }
                    continue;
                }
                throw new AssertionError("push failed: " + r.err());
            }
            if (!finalized.status().isFinalized()) {
                throw new AssertionError("push returned a non-finalized pushee: " + finalized);
            }
            // Interface: once its push returned a finalized record, the handle resolves T's intents on every key
            // k1..kn with that status. PORT: one batch of ResolveIntent requests through the DistSender; the
            // handle waits for its replies (the pusher resolves synchronously).
            List<Request> resolves = new ArrayList<>();
            for (int k = 1; k <= n; k++) {
                resolves.add(new ResolveIntentRequest(k, finalized.txnMeta(), finalized.status()));
            }
            Result rr = ds.send(new Ba(null, finalized.writeTimestamp(), false, resolves));
            if (rr.err() != null) {
                throw new AssertionError("intent resolution failed: " + rr.err());
            }
            Channel.send(model, setup.preventers().get(index - 1), new Recovered(finalized.status() == TransactionStatus.COMMITTED ? new Committed() : new Aborted()));
            while (true) {
                Channel.recv(model, mm -> false); // never terminates: waits for a next message that never comes
            }
        }

        /** Replica.handleIndeterminateCommitError replica_send.go l.818-838. */
        KvError handleIndeterminateCommitError(KvError t) {
            // On an indeterminate commit error, attempt to recover and finalize the stuck transaction. Retry
            // immediately if successful.
            Object res = resolveIndeterminateCommit(t);
            if (res instanceof KvError err) {
                // Do not propagate ambiguous results; assume success and retry original op.
                if (err.kind() == ErrKind.AMBIGUOUS_RESULT) {
                    return null;
                }
                // Propagate new error. Preserve the error index.
                return err.withIndex(t.index());
            }
            // We've recovered the transaction that blocked the request; retry.
            return null;
        }

        /** manager.ResolveIndeterminateCommit l.82-110. PORT: no singleflight (README). */
        Object resolveIndeterminateCommit(KvError ice) {
            Transaction txn = ice.stagingTxn();
            if (txn.status() != TransactionStatus.STAGING) {
                throw new AssertionError("IndeterminateCommitError with non-STAGING transaction: " + txn);
            }
            Object res = resolveIndeterminateCommitForTxn(txn);
            if (res instanceof KvError err) {
                return err; // errors.Wrap(res.Err, "failed indeterminate commit recovery")
            }
            return res;
        }

        /** resolveIndeterminateCommitForTxn l.120-151. PORT: no semaphore, no metrics. */
        Object resolveIndeterminateCommitForTxn(Transaction txn) {
            // We probe to determine whether the transaction is implicitly committed or not.
            Object probe = resolveIndeterminateCommitForTxnProbe(txn);
            if (probe instanceof KvError err) {
                return err;
            }
            if (probe instanceof Transaction changedTxn) {
                return changedTxn;
            }
            boolean preventedIntent = (Boolean) probe;
            // Now that we know whether the transaction was implicitly committed or not, we attempt to recover it.
            return resolveIndeterminateCommitForTxnRecover(txn, preventedIntent);
        }

        /**
         * resolveIndeterminateCommitForTxnProbe l.158-267. PORT: (preventedIntent, changedTxn, err) is returned
         * as a Boolean, a Transaction or a KvError.
         */
        Object resolveIndeterminateCommitForTxnProbe(Transaction txn) {
            // Create a QueryTxnRequest that we will periodically send to the transaction's record.
            QueryTxnRequest queryTxnReq = new QueryTxnRequest(txn.key(), txn.txnMeta());
            // Create a QueryIntentRequest for each of the transaction's in-flight writes.
            List<QueryIntentRequest> queryIntentReqs = new ArrayList<>();
            for (SequencedWrite w : txn.inFlightWrites()) {
                TxnMeta meta = txn.txnMeta().withSequence(w.sequence());
                queryIntentReqs.add(new QueryIntentRequest(w.key(), meta, false));
            }
            // Sort the query intent requests to maximize batching by range.
            queryIntentReqs.sort((x, y) -> Integer.compare(x.key(), y.key()));
            // Loop until either the transaction is observed to change, an in-flight write is prevented, or we run
            // out of in-flight writes to query.
            while (!queryIntentReqs.isEmpty()) {
                List<Request> b = new ArrayList<>();
                b.add(queryTxnReq);
                for (int i = 0; i < DEFAULT_BATCH_SIZE && !queryIntentReqs.isEmpty(); i++) {
                    b.add(queryIntentReqs.remove(0));
                }
                Result r = ds.send(new Ba(null, batchTimestamp(txn), false, b));
                if (r.err() != null) {
                    // Bail out on the first error.
                    return r.err();
                }
                // First, check the QueryTxnResponse to determine whether the state of the transaction record
                // has changed since we began the recovery process.
                List<Response> resps = r.br().responses();
                QueryTxnResponse queryTxnResp = (QueryTxnResponse) resps.get(0);
                Transaction queriedTxn = queryTxnResp.queriedTxn();
                if (queriedTxn.status().isFinalized()
                        || txn.epoch() < queriedTxn.epoch()
                        || txn.writeTimestamp() < queriedTxn.writeTimestamp()) {
                    // The transaction was already found to have changed.
                    return queriedTxn;
                }
                // Next, look through the QueryIntentResponses to check whether any of the in-flight writes failed.
                for (Response ru : resps.subList(1, resps.size())) {
                    QueryIntentResponse queryIntentResp = (QueryIntentResponse) ru;
                    if (!queryIntentResp.foundUnpushedIntent()) {
                        return Boolean.TRUE; // preventedIntent
                    }
                }
            }
            return Boolean.FALSE;
        }

        /** resolveIndeterminateCommitForTxnRecover l.279-299. */
        Object resolveIndeterminateCommitForTxnRecover(Transaction txn, boolean preventedIntent) {
            Ba b = new Ba(null, batchTimestamp(txn), false,
                    List.of(new RecoverTxnRequest(txn.key(), txn.txnMeta(), !preventedIntent)));
            Result r = ds.send(b);
            if (r.err() != null) {
                return r.err();
            }
            return ((RecoverTxnResponse) r.br().responses().get(0)).recoveredTxn();
        }

        /** batchTimestamp l.306-310. PORT: clock.Now() -> the timestamp in hand (forwarded to WriteTimestamp). */
        static long batchTimestamp(Transaction txn) {
            long now = 0L;
            return Math.max(now, txn.writeTimestamp());
        }
    }

    // =====================================================================================================
    // ri: a range (its leaseholder replica: batch evaluation, store, timestamp cache; r1 holds T's record)
    // =====================================================================================================

    /** T's intent at a key (MVCC replaced by the per-key cell, interface). */
    record Intent(int epoch, long writeTimestamp, int sequence) {}

    /** The per-key cell: T's intent or none; a resolved value (its timestamp only, 0 = none). */
    record Cell(Intent intent, long value) {}

    /** The evaluation's view of the store (a storage.Batch: applied only if the batch succeeds). */
    static final class EvalStore {
        final TreeMap<Integer, Cell> cells;
        Transaction record;

        EvalStore(TreeMap<Integer, Cell> cells, Transaction record) {
            this.cells = new TreeMap<>(cells);
            this.record = record;
        }

        Cell cell(int key) {
            return cells.getOrDefault(key, new Cell(null, 0L));
        }
    }

    /** batcheval.CommandArgs.Header: the batch's Txn and Timestamp. */
    record Header(Transaction txn, long timestamp) {
        /** Header.WriteTimestamp. */
        long writeTimestamp() {
            return txn != null ? txn.writeTimestamp() : timestamp;
        }
    }

    /**
     * A command's outcome: its response, its response header's Txn, and the result.Result fields used here
     * (EndTxnIntents: the finalized txn whose LockSpans go to the intent resolver).
     */
    record CmdResult(Response resp, Transaction replyTxn, Transaction endTxnIntents) {}

    /** A command's error (with the result that is propagated even on error, l.390-396 of replica_evaluate.go). */
    static final class CmdError extends RuntimeException {
        final KvError err;
        final CmdResult partial;

        CmdError(KvError err, CmdResult partial) {
            super(null, null, false, false);
            this.err = err;
            this.partial = partial;
        }
    }

    static final class Range {
        final int index; // 1 or 2: key ki is on range ri
        final CommunicationModel model;
        final int n;
        Setup setup;
        Tid self;
        int nextBatchId = 1;

        // the store
        TreeMap<Integer, Cell> cells = new TreeMap<>();
        Transaction record; // T's record (only on r1, the anchor's range)
        // the timestamp cache: per key (txn id always uuid.Nil: only QueryIntents add to keys), and T's markers
        final TreeMap<Integer, Long> tsCache = new TreeMap<>();
        long tombstoneTs;
        int tombstoneTxn = NIL_TXN;
        long pushMarkerTs;
        int pushMarkerTxn = NIL_TXN;

        Range(int index, CommunicationModel model, int n) {
            this.index = index;
            this.model = model;
            this.n = n;
        }

        boolean containsKey(int key) {
            return key == index;
        }

        void run() {
            self = MpProcess.self();
            setup = Channel.recv(model, Setup.class);
            while (true) {
                BatchRequest ba = Channel.recv(model, BatchRequest.class);
                List<BatchRequest> resolverBatches = new ArrayList<>();
                BatchResponse br = executeBatch(ba, resolverBatches);
                if (!ba.noReply()) {
                    Channel.send(model, ba.from(), br);
                }
                // The asynchronous intent resolver, folded into r1 (interface): ResolveIntents for T's keys on
                // other ranges, after the batch is applied. PORT: the reply is sent first, then the resolutions.
                for (BatchRequest rb : resolverBatches) {
                    Channel.send(model, setup.ranges().get(rb.requests().get(0).key() - 1), rb);
                }
            }
        }

        // ----- timestamp cache -----

        long tsCacheGetMax(int key) {
            return tsCache.getOrDefault(key, 0L);
        }

        void addToTSCacheKey(int key, long ts) {
            if (ts > tsCacheGetMax(key)) {
                tsCache.put(key, ts);
            }
        }

        /** tsCache.Add on a marker key: a higher timestamp wins; an equal one from another txn loses the id. */
        void addTombstone(long ts, int txnId) {
            if (ts > tombstoneTs) {
                tombstoneTs = ts;
                tombstoneTxn = txnId;
            } else if (ts == tombstoneTs && txnId != tombstoneTxn) {
                tombstoneTxn = NIL_TXN;
            }
        }

        void addPushMarker(long ts, int txnId) {
            if (ts > pushMarkerTs) {
                pushMarkerTs = ts;
                pushMarkerTxn = txnId;
            } else if (ts == pushMarkerTs && txnId != pushMarkerTxn) {
                pushMarkerTxn = NIL_TXN;
            }
        }

        /** Replica.CanCreateTxnRecord replica_tscache.go l.598-656. */
        String canCreateTxnRecord(long txnMinTS) {
            if (txnMinTS <= tombstoneTs) {
                if (tombstoneTxn == TXN_T) {
                    return "ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY";
                } else if (tombstoneTxn == NIL_TXN) {
                    // (no leases: the lease-start case does not occur)
                    return "ABORT_REASON_TIMESTAMP_CACHE_REJECTED";
                } else {
                    return "ABORT_REASON_ABORTED_RECORD_FOUND";
                }
            }
            return null; // ok
        }

        /** Replica.MinTxnCommitTS. */
        long minTxnCommitTS() {
            return pushMarkerTs;
        }

        // ----- batch execution -----

        /**
         * Replica.Send -> executeBatchWithConcurrencyRetries -> executeWriteBatch / executeReadOnlyBatch,
         * reduced to what the cut keeps: maybeStripInFlightWrites (replica_send.go l.174), applyTimestampCache
         * (replica_write.go l.156), maybeBumpReadTimestampToWriteTimestamp (replica_write.go l.440), the
         * evaluation of the requests in order (replica_evaluate.go evaluateBatch), the atomic application of the
         * batch, and updateTimestampCache. PORT: latches, the lock table and Raft are cut: the range evaluates
         * one batch at a time and applies it before replying.
         */
        BatchResponse executeBatch(BatchRequest req, List<BatchRequest> resolverOut) {
            Transaction txn = req.txn();
            long ts = req.timestamp();
            List<Request> reqs = req.requests();
            boolean isReadOnly = true;
            for (Request r : reqs) {
                if (!(r instanceof QueryIntentRequest || r instanceof QueryTxnRequest)) {
                    isReadOnly = false;
                }
            }
            if (!isReadOnly) {
                reqs = maybeStripInFlightWrites(reqs);
                // applyTimestampCache
                long[] tsHolder = {ts};
                txn = applyTimestampCache(reqs, txn, tsHolder);
                ts = tsHolder[0];
                // maybeBumpReadTimestampToWriteTimestamp
                if (txn != null && req.canForwardReadTimestamp() && txn.readTimestamp() != txn.writeTimestamp()
                        && reqs.get(reqs.size() - 1) instanceof EndTxnRequest) {
                    // IsEndTxnExceedingDeadline: no deadline. tryBumpBatchTimestamp: no latches hold it back.
                    txn = txn.bumpReadTimestamp(txn.writeTimestamp());
                    ts = txn.readTimestamp();
                }
            }
            EvalStore store = new EvalStore(cells, record);
            Transaction baTxn = txn; // baHeader.Txn
            List<Response> responses = new ArrayList<>();
            List<CmdResult> results = new ArrayList<>();
            KvError pErr = null;
            CmdResult errResult = null;
            for (int index = 0; index < reqs.size(); index++) {
                Request args = reqs.get(index);
                if (baTxn != null && args instanceof PutRequest put) {
                    // Set the Request's sequence number on the TxnMeta for this request.
                    baTxn = baTxn.withSequence(put.sequence());
                }
                try {
                    CmdResult cr = evaluateCommand(store, new Header(baTxn, ts), args);
                    // Absorb the update into baHeader.Txn.
                    if (baTxn != null && cr.replyTxn() != null) {
                        baTxn = baTxn.update(cr.replyTxn());
                    }
                    responses.add(cr.resp());
                    results.add(cr);
                } catch (CmdError e) {
                    if (baTxn != null && e.partial != null && e.partial.replyTxn() != null) {
                        baTxn = baTxn.update(e.partial.replyTxn());
                    }
                    pErr = e.err.withTxn(baTxn).withIndex(index); // NewErrorWithTxn(err, baHeader.Txn)
                    errResult = e.partial;
                    break;
                }
            }
            if (pErr != null) {
                // The write batch is discarded; the result is still processed (alwaysReturn intents).
                updateTimestampCache(reqs, ts, null, pErr);
                if (errResult != null && errResult.endTxnIntents() != null) {
                    resolveEndTxnIntents(errResult.endTxnIntents(), true, resolverOut);
                }
                return new BatchResponse(req.id(), null, null, pErr);
            }
            // Apply the batch atomically.
            cells = store.cells;
            record = store.record;
            long brTs = baTxn != null ? baTxn.readTimestamp() : ts;
            updateTimestampCache(reqs, brTs, responses, null);
            for (CmdResult cr : results) {
                if (cr.endTxnIntents() != null) {
                    resolveEndTxnIntents(cr.endTxnIntents(), false, resolverOut);
                }
            }
            return new BatchResponse(req.id(), baTxn, List.copyOf(responses), null);
        }

        /**
         * The intent resolver for result.EndTxnIntents. PORT: folded into the range: intents on other ranges
         * get a fire-and-forget ResolveIntent batch; with alwaysReturn (an error result, EndTxn finding an
         * ABORTED record) the local ones are resolved here right after the batch (no message to itself).
         */
        void resolveEndTxnIntents(Transaction txn, boolean local, List<BatchRequest> out) {
            for (int key : txn.lockSpans()) {
                if (containsKey(key)) {
                    if (local) {
                        Cell c = cells.getOrDefault(key, new Cell(null, 0L));
                        cells.put(key, mvccResolveWriteIntent(c, txn.status()));
                    }
                } else {
                    out.add(new BatchRequest(self, nextBatchId++, null, txn.writeTimestamp(), false, true,
                            List.of(new ResolveIntentRequest(key, txn.txnMeta(), txn.status()))));
                }
            }
        }

        /** applyTimestampCache replica_tscache.go l.397-464. */
        Transaction applyTimestampCache(List<Request> reqs, Transaction txn, long[] ts) {
            for (Request args : reqs) {
                // kvpb.AppliesTimestampCache: PORT: kvpb not supplied; of the requests here only Put.
                if (args instanceof PutRequest) {
                    // Forward the timestamp if there's been a more recent read (by someone else).
                    long rTS = tsCacheGetMax(args.key());
                    int rTxnID = NIL_TXN; // only QueryIntents add to keys, with uuid.Nil
                    // minReadTS: no closed timestamps.
                    long nextRTS = rTS + 1;
                    if (txn != null) {
                        if (TXN_T != rTxnID) {
                            if (txn.writeTimestamp() < nextRTS) {
                                txn = txn.withWriteTimestamp(nextRTS);
                            }
                        }
                    } else {
                        if (ts[0] < nextRTS) {
                            ts[0] = nextRTS;
                        }
                    }
                }
            }
            return txn;
        }

        /**
         * updateTimestampCache replica_tscache.go l.90-369 (cases EndTxn, RecoverTxn, PushTxn, QueryIntent).
         * PORT: kvpb.UpdatesTimestampCache(OnError) not supplied: EndTxn, RecoverTxn, PushTxn, QueryIntent and
         * ResolveIntent update it; only QueryIntent on error.
         */
        void updateTimestampCache(List<Request> reqs, long ts, List<Response> resps, KvError pErr) {
            for (int i = 0; i < reqs.size(); i++) {
                Request req = reqs.get(i);
                Response resp = resps != null ? resps.get(i) : null;
                // Skip update if there's an error and it's not for this index or the request doesn't update the
                // timestamp cache on errors.
                if (pErr != null) {
                    if (!(req instanceof QueryIntentRequest) || pErr.index() == -1 || i != pErr.index()) {
                        continue;
                    }
                }
                if (req instanceof EndTxnRequest) {
                    // EndTxn requests record a tombstone in the timestamp cache.
                    addTombstone(ts, TXN_T);
                    // ReplicatedLocalLocksReleasedOnCommit: none (intents only).
                } else if (req instanceof RecoverTxnRequest) {
                    Transaction recovered = ((RecoverTxnResponse) resp).recoveredTxn();
                    if (recovered.status().isFinalized()) {
                        addTombstone(ts, TXN_T);
                    }
                } else if (req instanceof PushTxnRequest t) {
                    Transaction pushee = ((PushTxnResponse) resp).pusheeTxn();
                    boolean tombstone;
                    switch (pushee.status()) {
                        case PENDING -> tombstone = false;
                        case ABORTED -> tombstone = true;
                        case COMMITTED, STAGING -> {
                            continue;
                        }
                        default -> throw assertionFailed("unexpected transaction status: " + pushee.status());
                    }
                    if (tombstone) {
                        addTombstone(pushee.minTimestamp(), t.pusherTxnId());
                    } else {
                        addPushMarker(pushee.writeTimestamp(), t.pusherTxnId());
                    }
                } else if (req instanceof QueryIntentRequest t) {
                    // (strength always lock.Intent)
                    boolean missing;
                    if (pErr != null) {
                        missing = pErr.kind() == ErrKind.INTENT_MISSING;
                    } else {
                        missing = !((QueryIntentResponse) resp).foundUnpushedIntent();
                    }
                    if (missing) {
                        // Update the timestamp cache at the intent's key to the intent's transactional timestamp,
                        // with an empty transaction ID.
                        addToTSCacheKey(t.key(), t.txn().writeTimestamp());
                    }
                } else if (req instanceof ResolveIntentRequest) {
                    // ReplicatedLocksReleasedCommitTimestamp: always empty (intents only).
                }
                // PutRequest, QueryTxnRequest: do not update the timestamp cache.
            }
        }

        /** maybeStripInFlightWrites replica_batch_updates.go l.37-170. */
        static List<Request> maybeStripInFlightWrites(List<Request> reqs) {
            if (!(reqs.get(reqs.size() - 1) instanceof EndTxnRequest)) {
                return reqs;
            }
            EndTxnRequest origET = (EndTxnRequest) reqs.get(reqs.size() - 1);
            List<Request> otherReqs = reqs.subList(0, reqs.size() - 1);
            if (!origET.isParallelCommit() || otherReqs.isEmpty()) {
                return reqs;
            }
            List<Integer> lockSpans = new ArrayList<>(origET.lockSpans());
            List<SequencedWrite> inFlight = new ArrayList<>();
            // Fast-path: If we know that this batch contains all of the transaction's in-flight writes, ...
            if (otherReqs.size() >= origET.inFlightWrites().size()) {
                int writes = 0;
                for (Request req : otherReqs) {
                    if (req instanceof PutRequest || req instanceof QueryIntentRequest) {
                        writes++;
                    }
                }
                if (origET.inFlightWrites().size() < writes) {
                    throw new AssertionError("more write in batch with EndTxn than listed in in-flight writes");
                } else if (origET.inFlightWrites().size() == writes) {
                    lockSpans = mergeIntoSpans(origET.lockSpans(), origET.inFlightWrites());
                    return withET(reqs, origET.withLocks(lockSpans, List.of()));
                }
            }
            // Slow-path: If not then we remove each transaction write in the batch from the in-flight write set
            // and merge it into the lock spans.
            int copiedTo = 0;
            List<SequencedWrite> orig = origET.inFlightWrites();
            for (Request req : otherReqs) {
                if (!(req instanceof PutRequest put)) {
                    // QueryIntent, ranged write or read: skip.
                    continue;
                }
                int seq = put.sequence();
                int match = -1;
                for (int i = copiedTo; i < orig.size(); i++) {
                    if (orig.get(i).sequence() == seq) {
                        match = i;
                        break;
                    }
                }
                if (match == -1) {
                    throw new AssertionError("write in batch with EndTxn missing from in-flight writes");
                }
                SequencedWrite w = orig.get(match);
                inFlight.addAll(orig.subList(copiedTo, match));
                copiedTo = match + 1;
                // Move the write to the lock spans set since it's no longer being tracked in the in-flight set.
                lockSpans.add(w.key());
            }
            inFlight.addAll(orig.subList(copiedTo, orig.size()));
            lockSpans = mergeSpans(lockSpans);
            return withET(reqs, origET.withLocks(lockSpans, inFlight));
        }

        static List<Request> withET(List<Request> reqs, EndTxnRequest et) {
            List<Request> out = new ArrayList<>(reqs);
            out.set(out.size() - 1, et);
            return out;
        }

        /** evaluateCommand: the command dispatch. */
        CmdResult evaluateCommand(EvalStore s, Header h, Request args) {
            if (args instanceof PutRequest put) {
                return put(s, h, put);
            } else if (args instanceof QueryIntentRequest qi) {
                return queryIntent(s, h, qi);
            } else if (args instanceof EndTxnRequest et) {
                return endTxn(s, h, et);
            } else if (args instanceof QueryTxnRequest qt) {
                return queryTxn(s, h, qt);
            } else if (args instanceof PushTxnRequest pt) {
                return pushTxn(s, h, pt);
            } else if (args instanceof RecoverTxnRequest rt) {
                return recoverTxn(s, h, rt);
            } else if (args instanceof ResolveIntentRequest ri) {
                return resolveIntent(s, h, ri);
            }
            throw new IllegalStateException("unknown request " + args);
        }

        /**
         * Put. PORT: MVCC replaced by the cell (interface): T's intent is laid or rewritten at the batch's
         * (bumped) write timestamp, with the request's epoch and sequence.
         */
        CmdResult put(EvalStore s, Header h, PutRequest args) {
            Cell c = s.cell(args.key());
            s.cells.put(args.key(), new Cell(new Intent(h.txn().epoch(), h.txn().writeTimestamp(), args.sequence()),
                    c.value()));
            return new CmdResult(new PutResponse(), null, null);
        }

        /** MVCCResolveWriteIntent (interface): COMMITTED -> value at the intent's timestamp; ABORTED -> removed. */
        static Cell mvccResolveWriteIntent(Cell c, TransactionStatus status) {
            if (c.intent() == null) {
                return c;
            }
            if (status == TransactionStatus.COMMITTED) {
                return new Cell(null, c.intent().writeTimestamp());
            } else if (status == TransactionStatus.ABORTED) {
                return new Cell(null, c.value());
            }
            return c; // PENDING / STAGING: a push update, not used
        }

        CmdResult resolveIntent(EvalStore s, Header h, ResolveIntentRequest args) {
            s.cells.put(args.key(), mvccResolveWriteIntent(s.cell(args.key()), args.status()));
            return new CmdResult(new ResolveIntentResponse(), null, null);
        }

        /** QueryIntent cmd_query_intent.go l.60-167 (intent strength only). */
        CmdResult queryIntent(EvalStore s, Header h, QueryIntentRequest args) {
            boolean ownTxn = false;
            if (h.txn() != null) {
                // Determine if the request is querying an intent in its own transaction (IDs: always T).
                ownTxn = true;
            }
            if (h.writeTimestamp() < args.txn().writeTimestamp()) {
                throw assertionFailed("QueryIntent request timestamp less than txn WriteTimestamp");
            }
            // TxnSeqIsIgnored: no savepoints.
            // Read from the lock table to see if an intent exists.
            Intent intent = s.cell(args.key()).intent();
            boolean foundIntent = false;
            boolean foundUnpushedIntent = false;
            Transaction replyTxn = null;
            if (intent != null) {
                foundIntent = /* (args.Txn.ID == intent.Txn.ID) && */ (args.txn().epoch() == intent.epoch())
                        && (args.txn().sequence() <= intent.sequence());
                if (foundIntent) {
                    // If we found a matching intent, check whether the intent was pushed past its expected
                    // timestamp.
                    long cmpTS = args.txn().writeTimestamp();
                    if (ownTxn) {
                        cmpTS = Math.max(cmpTS, h.txn().writeTimestamp());
                    }
                    foundUnpushedIntent = intent.writeTimestamp() <= cmpTS;
                    if (!foundUnpushedIntent) {
                        // If the request was querying an intent in its own transaction, update the response txn.
                        if (ownTxn) {
                            replyTxn = h.txn().withWriteTimestamp(
                                    Math.max(h.txn().writeTimestamp(), intent.writeTimestamp()));
                        }
                    }
                }
            }
            QueryIntentResponse reply = new QueryIntentResponse(foundIntent, foundUnpushedIntent, replyTxn);
            if (!foundIntent && args.errorIfMissing()) {
                throw new CmdError(KvError.of(ErrKind.INTENT_MISSING, "key " + args.key()),
                        new CmdResult(reply, replyTxn, null));
            }
            return new CmdResult(reply, replyTxn, null);
        }

        /** QueryTxn cmd_query_txn.go l.48-89 (WaitingTxns ignored). */
        CmdResult queryTxn(EvalStore s, Header h, QueryTxnRequest args) {
            if (h.txn() != null) {
                throw assertionFailed("ErrTransactionUnsupported");
            }
            if (h.writeTimestamp() < args.txn().minTimestamp()) {
                throw assertionFailed("QueryTxn request timestamp less than txn MinTimestamp");
            }
            if (args.key() != args.txn().key()) {
                throw assertionFailed("QueryTxn request key does not match txn key");
            }
            // Fetch transaction record; if missing, attempt to synthesize one.
            if (s.record != null) {
                return new CmdResult(new QueryTxnResponse(s.record, true), null, null);
            }
            return new CmdResult(new QueryTxnResponse(synthesizeTxnFromMeta(args.txn()), false), null, null);
        }

        /** SynthesizeTxnFromMeta transaction.go l.167-203. */
        Transaction synthesizeTxnFromMeta(TxnMeta txn) {
            // LastHeartbeat = MinTimestamp (PORT: not kept; no heartbeats, no expiry).
            Transaction synth = Transaction.fromMeta(txn, TransactionStatus.PENDING);
            if (txn.minTimestamp() == 0) {
                return synth.withStatus(TransactionStatus.ABORTED);
            }
            // Determine whether the record could ever be allowed to be written in the future.
            if (canCreateTxnRecord(txn.minTimestamp()) == null) {
                // Forward the provisional commit timestamp by the minimum timestamp that the transaction would be
                // able to commit at.
                synth = synth.withWriteTimestamp(Math.max(synth.writeTimestamp(), minTxnCommitTS()));
            } else {
                // Mark the transaction as ABORTED because it is uncommittable.
                synth = synth.withStatus(TransactionStatus.ABORTED);
            }
            return synth;
        }

        /** batcheval.CanCreateTxnRecord transaction.go l.119-134. */
        void canCreateTxnRecordOrError(Transaction txn) {
            String reason = canCreateTxnRecord(txn.minTimestamp());
            if (reason != null) {
                throw new CmdError(KvError.of(ErrKind.TRANSACTION_ABORTED, reason), null);
            }
            // Verify that if the transaction record is being created, the client thinks it's in the PENDING state.
            if (txn.status() != TransactionStatus.PENDING) {
                throw assertionFailed("cannot create transaction record with non-PENDING transaction: " + txn);
            }
        }

        /** BumpToMinTxnCommitTS transaction.go l.139-147. */
        Transaction bumpToMinTxnCommitTS(Transaction txn) {
            if (txn.status() != TransactionStatus.PENDING) {
                throw assertionFailed("non-pending txn passed to BumpToMinTxnCommitTS: " + txn);
            }
            long minCommitTS = minTxnCommitTS();
            return txn.withWriteTimestamp(Math.max(txn.writeTimestamp(), minCommitTS));
        }

        /** EndTxn cmd_end_transaction.go l.248-605. */
        CmdResult endTxn(EvalStore s, Header h, EndTxnRequest args) {
            // VerifyTransaction(h, args, PENDING, PREPARED, STAGING, ABORTED)
            if (h.txn() == null) {
                throw assertionFailed("EndTxn without transaction");
            }
            if (h.txn().status() == TransactionStatus.COMMITTED) {
                throw new CmdError(KvError.of(ErrKind.TRANSACTION_STATUS, "REASON_UNKNOWN"), null);
            }
            // Require1PC, Poison, Prepare: never set (interface).
            // Fetch existing transaction.
            Transaction existingTxn = s.record;
            boolean recordAlreadyExisted = existingTxn != null;
            Transaction replyTxn;
            if (!recordAlreadyExisted) {
                // No existing transaction record was found - create one by writing it below.
                replyTxn = h.txn();
                // Verify that it is safe to create the transaction record.
                if (args.commit()) {
                    canCreateTxnRecordOrError(replyTxn);
                }
            } else {
                replyTxn = existingTxn;
                switch (replyTxn.status()) {
                    case COMMITTED:
                        // Someone else must have performed the STAGING->COMMITTED transition in the meantime.
                        throw new CmdError(KvError.of(ErrKind.TRANSACTION_STATUS, "REASON_TXN_COMMITTED"), null);
                    case ABORTED:
                        // The transaction has already been aborted by someone else.
                        if (!args.commit()) {
                            // rollback: cut (no rollback by T's coordinator).
                            throw assertionFailed("rollback is cut");
                        }
                        // If the transaction was previously aborted by a concurrent writer's push, any intents
                        // written are still open. ... return them all for asynchronous resolution.
                        replyTxn = replyTxn.withLockSpans(args.lockSpans());
                        throw new CmdError(KvError.of(ErrKind.TRANSACTION_ABORTED, "ABORT_REASON_ABORTED_RECORD_FOUND"),
                                new CmdResult(null, replyTxn, replyTxn /* alwaysReturn */));
                    case PENDING:
                        if (h.txn().epoch() < replyTxn.epoch()) {
                            throw assertionFailed("programming error: epoch regression: " + h.txn().epoch());
                        }
                        break;
                    case STAGING:
                        if (h.txn().epoch() < replyTxn.epoch()) {
                            throw assertionFailed("programming error: epoch regression: " + h.txn().epoch());
                        } else if (h.txn().epoch() == replyTxn.epoch()) {
                            // (args.Prepare: never)
                        } else {
                            // If the EndTxn carries a newer epoch than a STAGING txn record, we do not consider the
                            // transaction to be performing a parallel commit.
                            replyTxn = replyTxn.withStatus(TransactionStatus.PENDING);
                        }
                        break;
                    default:
                        throw assertionFailed("bad txn status: " + replyTxn);
                }
                // Update the existing txn with the supplied txn.
                replyTxn = replyTxn.update(h.txn());
            }
            long stagingTimestamp = 0L;
            // Attempt to commit or abort the transaction per the args.Commit parameter.
            if (args.commit()) {
                // Bump the transaction's provisional commit timestamp to account for any transaction pushes.
                if (!recordAlreadyExisted || existingTxn.status() == TransactionStatus.PENDING) {
                    replyTxn = bumpToMinTxnCommitTS(replyTxn);
                } else if (existingTxn.status() == TransactionStatus.STAGING) {
                    // Don't check timestamp cache.
                } else {
                    throw new IllegalStateException("unreachable");
                }
                // Determine whether the transaction's commit is successful or should trigger a retry error.
                // IsEndTxnTriggeringRetryError (serializable; no deadline)
                if (!TOLERATES_WRITE_SKEW && replyTxn.writeTimestamp() != replyTxn.readTimestamp()) {
                    throw new CmdError(KvError.of(ErrKind.TRANSACTION_RETRY, "RETRY_SERIALIZABLE"), null);
                }
                // If the transaction needs to be staged as part of an implicit commit before being explicitly
                // committed, write the staged transaction record and return.
                if (args.isParallelCommit()) {
                    replyTxn = replyTxn.withStatus(TransactionStatus.STAGING);
                    stagingTimestamp = replyTxn.writeTimestamp();
                    // updateStagingTxn
                    replyTxn = replyTxn.withLockSpans(args.lockSpans()).withInFlightWrites(args.inFlightWrites());
                    s.record = replyTxn.asRecord();
                    return new CmdResult(new EndTxnResponse(stagingTimestamp), replyTxn, null);
                }
                // Else, the transaction can be explicitly committed.
                replyTxn = replyTxn.withStatus(TransactionStatus.COMMITTED);
            } else {
                if (replyTxn.status() == TransactionStatus.STAGING) {
                    if (!recordAlreadyExisted) {
                        throw assertionFailed("programming error: transaction in STAGING without transaction record");
                    }
                    throw new CmdError(new KvError(ErrKind.INDETERMINATE_COMMIT, "", -1, null, existingTxn), null);
                }
                replyTxn = replyTxn.withStatus(TransactionStatus.ABORTED);
            }
            // Resolve locks on the local range synchronously so that their resolution ends up in the same Raft
            // entry.
            List<Integer> externalLocks = new ArrayList<>();
            for (int key : args.lockSpans()) {
                if (!containsKey(key)) {
                    externalLocks.add(key);
                    continue;
                }
                s.cells.put(key, mvccResolveWriteIntent(s.cell(key), replyTxn.status()));
            }
            // WriteAbortSpanOnResolve / UpdateAbortSpan: cut (no AbortSpan).
            // updateFinalizedTxn. PORT: no record GC (interface): the record is always written, never deleted.
            replyTxn = replyTxn.withLockSpans(externalLocks).withInFlightWrites(List.of());
            s.record = replyTxn.asRecord();
            // result.FromEndTxn(reply.Txn, false /* alwaysReturn */): the external locks go to the resolver.
            // RunCommitTrigger: no commit triggers.
            return new CmdResult(new EndTxnResponse(stagingTimestamp), replyTxn,
                    externalLocks.isEmpty() ? null : replyTxn);
        }

        /** PushTxn cmd_push_txn.go l.111-350. */
        CmdResult pushTxn(EvalStore s, Header h, PushTxnRequest args) {
            if (h.txn() != null) {
                throw assertionFailed("ErrTransactionUnsupported");
            }
            if (h.writeTimestamp() < args.pushTo()) {
                throw assertionFailed("request timestamp less than PushTo timestamp");
            }
            if (h.writeTimestamp() < args.pusheeTxn().minTimestamp()) {
                throw assertionFailed("request timestamp less than pushee txn MinTimestamp");
            }
            if (args.key() != args.pusheeTxn().key()) {
                throw assertionFailed("request key should match pushee txn key");
            }
            // Fetch existing transaction; if missing, we're allowed to abort.
            Transaction existTxn = s.record;
            boolean ok = existTxn != null;
            Transaction pushee;
            if (!ok) {
                pushee = synthesizeTxnFromMeta(args.pusheeTxn());
                if (pushee.status() == TransactionStatus.ABORTED) {
                    // The transaction may actually have committed and already removed its intents and txn record,
                    // or it may have aborted and done the same. We can't know, so mark the abort as ambiguous.
                    return new CmdResult(new PushTxnResponse(pushee, true), null, null);
                }
            } else {
                // Start with the persisted transaction record.
                pushee = existTxn;
            }
            // If already committed or aborted, return success.
            if (pushee.status().isFinalized()) {
                return new CmdResult(new PushTxnResponse(pushee, false), null, null);
            }
            PushTxnType pushType = args.pushType();
            if (pushType == PushTxnType.PUSH_TIMESTAMP && args.pushTo() <= pushee.writeTimestamp()) {
                return new CmdResult(new PushTxnResponse(pushee, false), null, null);
            }
            // The pusher might be aware of a newer version of the pushee.
            boolean knownHigherTimestamp = false;
            boolean knownHigherEpoch = false;
            if (pushee.writeTimestamp() < args.pusheeTxn().writeTimestamp()) {
                pushee = pushee.withWriteTimestamp(args.pusheeTxn().writeTimestamp());
                knownHigherTimestamp = true;
            }
            if (pushee.epoch() < args.pusheeTxn().epoch()) {
                pushee = new Transaction(pushee.key(), args.pusheeTxn().epoch(), pushee.writeTimestamp(),
                        pushee.readTimestamp(), pushee.minTimestamp(), pushee.sequence(), pushee.status(),
                        pushee.lockSpans(), pushee.inFlightWrites());
                knownHigherEpoch = true;
            }
            TransactionStatus pusheeStatus = pushee.status();
            boolean pusherWins;
            // case txnwait.IsExpired(...): PORT: a push always wins (interface): the pushee is treated as expired.
            pushType = PushTxnType.PUSH_ABORT;
            pusherWins = true;
            // If the pushed transaction is in the staging state, we can't change its record without first going
            // through the transaction recovery process and attempting to finalize it.
            boolean pusheeStaging = pusheeStatus == TransactionStatus.STAGING;
            boolean pusheeStagingFailed = pusheeStaging && (knownHigherTimestamp || knownHigherEpoch);
            boolean recoverOnFailedPush = false;
            if (pusheeStaging && !pusheeStagingFailed && (pusherWins || recoverOnFailedPush)) {
                throw new CmdError(new KvError(ErrKind.INDETERMINATE_COMMIT, "", -1, null, pushee), null);
            }
            if (pusheeStaging && pusherWins && pushType == PushTxnType.PUSH_TIMESTAMP) {
                pushType = PushTxnType.PUSH_ABORT;
            }
            // if !pusherWins: TransactionPushError: never.
            switch (pushType) {
                case PUSH_ABORT:
                    // If aborting the transaction, set the new status.
                    pushee = pushee.withStatus(TransactionStatus.ABORTED);
                    // Forward the timestamp to accommodate AbortSpan GC: LastActive(). PORT: no heartbeats; the
                    // record's LastHeartbeat is never above its write timestamp, so this is a no-op.
                    // If the transaction was previously staging, clear its in-flight writes.
                    pushee = pushee.withInFlightWrites(List.of());
                    // If the transaction record was already present, persist the updates to it.
                    if (ok) {
                        s.record = pushee.asRecord();
                    }
                    break;
                case PUSH_TIMESTAMP:
                    if (existTxn == null || existTxn.status() != TransactionStatus.PENDING) {
                        throw assertionFailed("PUSH_TIMESTAMP succeeded against non-PENDING txn");
                    }
                    pushee = pushee.withWriteTimestamp(Math.max(pushee.writeTimestamp(), args.pushTo()));
                    break;
                default:
                    throw assertionFailed("unexpected push type");
            }
            return new CmdResult(new PushTxnResponse(pushee, false), null, null);
        }

        /** RecoverTxn cmd_recover_txn.go l.52-234. */
        CmdResult recoverTxn(EvalStore s, Header h, RecoverTxnRequest args) {
            if (h.txn() != null) {
                throw assertionFailed("ErrTransactionUnsupported");
            }
            if (h.writeTimestamp() < args.txn().minTimestamp()) {
                throw assertionFailed("RecoverTxn request timestamp less than txn MinTimestamp");
            }
            if (args.key() != args.txn().key()) {
                throw assertionFailed("RecoverTxn request key does not match txn key");
            }
            // Fetch transaction record; if missing, attempt to synthesize one.
            Transaction recovered = s.record;
            if (recovered == null) {
                Transaction synthTxn = synthesizeTxnFromMeta(args.txn());
                if (synthTxn.status() != TransactionStatus.ABORTED) {
                    throw new AssertionError("txn record synthesized with non-ABORTED status: " + synthTxn);
                }
                return new CmdResult(new RecoverTxnResponse(synthTxn), null, null);
            }
            // Determine whether to continue with recovery.
            if (args.implicitlyCommitted()) {
                switch (recovered.status()) {
                    case PENDING, ABORTED:
                        throw assertionFailed("programming error: found " + recovered.status()
                                + " record for implicitly committed transaction: " + recovered);
                    case STAGING, COMMITTED:
                        if (args.txn().epoch() != recovered.epoch()) {
                            throw assertionFailed("programming error: epoch change by implicitly committed transaction");
                        }
                        if (args.txn().writeTimestamp() != recovered.writeTimestamp()) {
                            throw assertionFailed(
                                    "programming error: timestamp change by implicitly committed transaction");
                        }
                        if (recovered.status() == TransactionStatus.COMMITTED) {
                            // The transaction commit was already made explicit.
                            return new CmdResult(new RecoverTxnResponse(recovered), null, null);
                        }
                        // Continue with recovery.
                        break;
                    default:
                        throw assertionFailed("bad txn status: " + recovered);
                }
            } else {
                // Did the transaction change its epoch or timestamp in such a way that it would be allowed to
                // continue trying to commit?
                boolean legalChange = args.txn().epoch() < recovered.epoch()
                        || args.txn().writeTimestamp() < recovered.writeTimestamp();
                switch (recovered.status()) {
                    case ABORTED:
                        // The transaction was aborted by some other process.
                        return new CmdResult(new RecoverTxnResponse(recovered), null, null);
                    case COMMITTED:
                        // (#37784: a resolved intent can be mistaken for a missing in-flight write.)
                        return new CmdResult(new RecoverTxnResponse(recovered), null, null);
                    case PENDING:
                        if (args.txn().epoch() < recovered.epoch()) {
                            // Recovery not immediately needed because the transaction is still in progress.
                            return new CmdResult(new RecoverTxnResponse(recovered), null, null);
                        }
                        throw assertionFailed("programming error: cannot recover " + recovered.status()
                                + " transaction in same epoch: " + recovered);
                    case STAGING:
                        if (legalChange) {
                            // Recovery not immediately needed because the transaction is still in progress.
                            return new CmdResult(new RecoverTxnResponse(recovered), null, null);
                        }
                        // Continue with recovery.
                        break;
                    default:
                        throw assertionFailed("bad txn status: " + recovered);
                }
            }
            // Merge all of the transaction's in-flight writes into its lock spans set and clear the in-flight
            // write set.
            recovered = recovered.withLockSpans(mergeIntoSpans(recovered.lockSpans(), recovered.inFlightWrites()))
                    .withInFlightWrites(List.of());
            // Recover the transaction based on whether or not all of its writes succeeded.
            if (args.implicitlyCommitted()) {
                recovered = recovered.withStatus(TransactionStatus.COMMITTED);
            } else {
                recovered = recovered.withStatus(TransactionStatus.ABORTED);
            }
            s.record = recovered.asRecord();
            // (no intent resolution triggered: the TODO l.226-229)
            return new CmdResult(new RecoverTxnResponse(recovered), null, null);
        }
    }

    @Override
    public String toString() {
        return "ParallelCommits(impl," + n + "," + p + "," + m + "," + a + "," + modelName + ")";
    }
}
