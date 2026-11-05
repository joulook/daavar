package bench.bookkeeper413;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * Transliteration of Apache BookKeeper 4.13 (tag release-4.13.0) ledger replication with fencing,
 * one ledger, writer w1, recovery client w2, bookies b1..b3, metadata store zk, against JMC's
 * message-passing API. See NOTES.md for the roles, the messages, the method map and every
 * decision. Every line that departs from the source is marked {@code // PORT:} with the reason.
 *
 * <p>Logical tasks (the owner's contract): one process per concurrent activity of the source.
 * Each PendingAddOp in flight, each ReadLastConfirmedOp and each recovery read (PendingReadOp)
 * is its own (daemon) process; a LedgerHandle (with its pending-add queue and its ordered
 * callback executor) is the sequential process w1h / w2h; each bookie (request processor over
 * its in-memory ledger storage) is the sequential process b1 / b2 / b3; zk is the versioned
 * metadata store. Calls from an op into its LedgerHandle are request/return messages.
 */
public final class BookKeeper413 implements MpProgram {

    // ------------------------------------------------------------------------------------------
    // Instance parameters (INTERFACE.md)
    // ------------------------------------------------------------------------------------------

    static final int ENSEMBLE_SIZE = 3;
    static final int WRITE_QUORUM_SIZE = 3;
    static final int ACK_QUORUM_SIZE = 2;
    // PORT: one ledger; its id is fixed (LedgerCreateOp is outside the program).
    static final long LEDGER_ID = 1L;
    // PORT: ClientConfiguration defaults of 4.13 (the conf/ package is not in the cut).
    static final int RECOVERY_READ_BATCH_SIZE = 1;               // recoveryReadBatchSize
    static final boolean DELAY_ENSEMBLE_CHANGE = false;           // delayEnsembleChange
    static final boolean DISABLE_ENSEMBLE_CHANGE_FEATURE = false; // disableEnsembleChangeFeature
    static final boolean ENFORCE_MIN_NUM_FAULT_DOMAINS_FOR_WRITE = false;
    static final int MAX_ALLOWED_ENSEMBLE_CHANGES = Integer.MAX_VALUE;
    // PORT: an entry's payload is the client's entry number; its byte length is taken as 1.
    static final long DATA_LENGTH = 1L;

    private final int e;
    private final CommunicationModel model;
    private final String modelName;
    // Observation across iterations (printed by the launcher through toString; no messages).
    private long iterations = 0;
    private long defect1Witnesses = 0;

    /**
     * Launcher argument string {@code impl,<e>,<fifo|bag>}.
     *
     * @param args kind, e, delivery model
     */
    public BookKeeper413(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<fifo|bag>, got " + List.of(args));
        }
        this.e = Integer.parseInt(args[1]);
        if (e < 1) {
            throw new IllegalArgumentException("e must be >= 1, was " + e);
        }
        this.modelName = args[2];
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        };
    }

    @Override
    public String programName() {
        return "bookkeeper";
    }

    @Override
    public String size() {
        return "e=" + e;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("w1", "w2");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("w1", "w2");
    }

    @Override
    public String toString() {
        return "BookKeeper413(impl," + e + "," + modelName + "): " + iterations
                + " iterations observed, " + defect1Witnesses
                + " with an entry Acked above the Closed last (Defect 1)";
    }

    // ------------------------------------------------------------------------------------------
    // Program entry: start b1, b2, b3, zk, w1h, w2h, w1, w2 (in that order), then init.
    // ------------------------------------------------------------------------------------------

    @Override
    public void run() {
        final CommunicationModel m = model;
        final int entries = e;
        Tid b1 = MpProcess.spawnDaemon(() -> new Bookie(m).run()).tid();
        Tid b2 = MpProcess.spawnDaemon(() -> new Bookie(m).run()).tid();
        Tid b3 = MpProcess.spawnDaemon(() -> new Bookie(m).run()).tid();
        Tid zk = MpProcess.spawnDaemon(() -> new ZooKeeperLedgerManager(m).run()).tid();
        Tid w1h = MpProcess.spawnDaemon(() -> new ClientProcess(m, false).run()).tid();
        Tid w2h = MpProcess.spawnDaemon(() -> new ClientProcess(m, true).run()).tid();
        final long[] maxAcked = {-1};
        final long[] closedLast = {Long.MIN_VALUE};
        MpProcess.Handle w1 = MpProcess.spawn("w1", () -> writer(m, entries, w1h, maxAcked));
        MpProcess.Handle w2 = MpProcess.spawn("w2", () -> recoverer(m, w2h, closedLast));
        // The invisible init process sends every internal process its one setup message.
        final Setup setup = new Setup(b1, b2, b3, zk, w1.tid(), w1h, w2.tid(), w2h);
        MpProcess.spawnDaemon(() -> {
            for (Tid t : List.of(b1, b2, b3, zk, w1h, w2h)) {
                Channel.send(m, t, setup);
            }
        });
        w1.await();
        w2.await();
        iterations++;
        if (closedLast[0] != Long.MIN_VALUE && maxAcked[0] > closedLast[0]) {
            defect1Witnesses++;
        }
    }

    /** The client loop of w1 (INTERFACE.md, verbatim in meaning). */
    static void writer(CommunicationModel m, int e, Tid h, long[] maxAcked) {
        for (long i = 1; i <= e; i++) {
            Channel.send(m, h, new Add(i));
            MpMessage r = Channel.recv(m);
            if (!(r instanceof Acked) && !(r instanceof Rejected)) {
                throw new AssertionError("Acked or Rejected expected, got " + r);
            }
            if (r instanceof Acked a) {
                maxAcked[0] = Math.max(maxAcked[0], a.entry()); // observation only
            }
        }
    }

    /** The client loop of w2 (INTERFACE.md, verbatim in meaning). */
    static void recoverer(CommunicationModel m, Tid h, long[] closedLast) {
        Channel.send(m, h, new Recover());
        MpMessage r = Channel.recv(m);
        if (!(r instanceof Closed)) {
            throw new AssertionError("Closed expected, got " + r);
        }
        closedLast[0] = ((Closed) r).last(); // observation only
    }

    // ------------------------------------------------------------------------------------------
    // Messages
    // ------------------------------------------------------------------------------------------

    /** w1 -> w1h. */
    @Tag("Add")
    public record Add(long entry) implements MpMessage {}

    /** w2 -> w2h. */
    @Tag("Recover")
    public record Recover() implements MpMessage {}

    /** w1h -> w1. */
    @Tag("Acked")
    public record Acked(long entry) implements MpMessage {}

    /** w1h -> w1. */
    @Tag("Rejected")
    public record Rejected(long entry) implements MpMessage {}

    /** w2h -> w2. */
    @Tag("Closed")
    public record Closed(long last) implements MpMessage {}

    /** init -> every internal process: the addresses. */
    @Tag("Setup")
    public record Setup(Tid b1, Tid b2, Tid b3, Tid zk, Tid w1, Tid w1h, Tid w2, Tid w2h) implements MpMessage {}

    /**
     * The packaged entry (DigestManager.computeDigestAndPackageForSending without the digest):
     * ledger id, entry id, the piggybacked last add confirmed, the ledger length, the payload.
     */
    @Tag("Entry")
    public record StoredEntry(long ledgerId, long entryId, long lastAddConfirmed, long length, long data)
            implements MpMessage {}

    /** No body (a response without one). */
    static final StoredEntry NO_BODY = new StoredEntry(-1, -1, -1, -1, -1);

    /** BookkeeperProtocol.AddRequest (V3) as PerChannelBookieClient.addEntry builds it. */
    @Tag("AddRequest")
    public record AddRequest(Tid from, long ledgerId, long entryId, boolean recoveryAddFlag, int priority,
                             StoredEntry body, int ctx) implements MpMessage {}

    /** BookkeeperProtocol.AddResponse, with the completion key's ctx and the bookie address. */
    @Tag("AddResponse")
    public record AddResponse(long ledgerId, long entryId, int status, int ctx, Tid addr) implements MpMessage {}

    /** BookkeeperProtocol.ReadRequest (V3) as PerChannelBookieClient.readEntry builds it. */
    @Tag("ReadRequest")
    public record ReadRequest(Tid from, long ledgerId, long entryId, boolean fenceLedgerFlag, boolean hasMasterKey,
                              int priority, int ctx) implements MpMessage {}

    /** BookkeeperProtocol.ReadResponse, with the completion key's ctx. */
    @Tag("ReadResponse")
    public record ReadResponse(long ledgerId, long entryId, int status, boolean hasBody, StoredEntry body,
                               boolean hasMaxLAC, long maxLAC, int ctx) implements MpMessage {}

    /** LedgerMetadata.State. */
    public enum State { OPEN, IN_RECOVERY, CLOSED }

    /** LedgerMetadata (PORT: the ensemble is fixed and omitted; no digest/password/ctime). */
    @Tag("LedgerMetadata")
    public record LedgerMetadata(State state, long lastEntryId, long length) implements MpMessage {
        boolean isClosed() {
            return state == State.CLOSED;
        }

        State getState() {
            return state;
        }
    }

    /** Versioned&lt;LedgerMetadata&gt; with a LongVersion. */
    @Tag("Versioned")
    public record Versioned(LedgerMetadata value, long version) implements MpMessage {}

    /** LedgerManager.readLedgerMetadata request. */
    @Tag("ReadLedgerMetadata")
    public record ReadLedgerMetadata(Tid from, long ledgerId, long reqId) implements MpMessage {}

    /** LedgerManager.writeLedgerMetadata request (conditional on the version). */
    @Tag("WriteLedgerMetadata")
    public record WriteLedgerMetadata(Tid from, long ledgerId, LedgerMetadata metadata, long expectedVersion,
                                      long reqId) implements MpMessage {}

    /** Completion of a metadata read or write. */
    @Tag("MetadataResult")
    public record MetadataResult(long reqId, int rc, Versioned versioned) implements MpMessage {}

    /** A task submitted to the handle's ordered executor (executeOrdered / whenCompleteAsync). */
    @Tag("Task")
    public record Task(long id) implements MpMessage {}

    // Calls from a PendingAddOp into its LedgerHandle, and their returns.

    /** PendingAddOp.safeRun's reads of the handle: callbackTriggered, lh.lastAddConfirmed,
     *  lh.maybeHandleDelayedWriteBookieFailure(). */
    @Tag("SafeRunCall")
    public record SafeRunCall(long opId) implements MpMessage {}

    @Tag("SafeRunReturn")
    public record SafeRunReturn(boolean callbackTriggered, long lastAddConfirmed) implements MpMessage {}

    /** lh.sendAddSuccessCallbacks(), carrying the op's completed flag. */
    @Tag("SendAddSuccessCallbacksCall")
    public record SendAddSuccessCallbacksCall(long opId, boolean completed) implements MpMessage {}

    /** lh.handleUnrecoverableErrorDuringAdd(rc). */
    @Tag("HandleUnrecoverableErrorDuringAddCall")
    public record HandleUnrecoverableErrorDuringAddCall(Tid from, int rc) implements MpMessage {}

    /** lh.errorOutPendingAdds(rc). */
    @Tag("ErrorOutPendingAddsCall")
    public record ErrorOutPendingAddsCall(Tid from, int rc) implements MpMessage {}

    /** lh.notifyWriteFailed(index, addr). */
    @Tag("NotifyWriteFailedCall")
    public record NotifyWriteFailedCall(Tid from, int index, Tid addr) implements MpMessage {}

    /** lh.handleBookieFailure(ImmutableMap.of(index, addr)). */
    @Tag("HandleBookieFailureCall")
    public record HandleBookieFailureCall(Tid from, int index, Tid addr) implements MpMessage {}

    /** Return of a void call. */
    @Tag("Ret")
    public record Ret() implements MpMessage {}

    /** ReadLastConfirmedOp.LastConfirmedDataCallback.readLastConfirmedDataComplete. */
    @Tag("ReadLastConfirmedDataComplete")
    public record ReadLastConfirmedDataComplete(long cbId, int rc, long lastAddConfirmed, long length)
            implements MpMessage {}

    /** ReadEntryListener.onEntryComplete (entry null encoded as hasEntry = false). */
    @Tag("OnEntryComplete")
    public record OnEntryComplete(long cbId, int rc, boolean hasEntry, long entryId, long length, long data)
            implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Codes (BKException.Code, BookkeeperProtocol.StatusCode, BookieProtocol flags)
    // ------------------------------------------------------------------------------------------

    static final int OK = 0;
    static final int UNINITIALIZED = 1;
    static final int ReadException = -1;
    static final int NotEnoughBookiesException = -6;
    static final int NoSuchLedgerExistsException = -7;
    static final int LedgerClosedException = -11;
    static final int WriteException = -12;
    static final int NoSuchEntryException = -13;
    static final int MetadataVersionException = -17;
    static final int IllegalOpException = -100;
    static final int LedgerFencedException = -101;
    static final int UnauthorizedAccessException = -102;
    static final int ClientClosedException = -24;
    static final int LedgerRecoveryException = -10;
    static final int UnexpectedConditionException = -999;

    // StatusCode
    static final int EOK = 0;
    static final int ENOLEDGER = 402;
    static final int ENOENTRY = 403;
    static final int EBADREQ = 404;
    static final int EIO = 501;
    static final int EUA = 502;
    static final int EBADVERSION = 503;
    static final int EFENCED = 504;
    static final int EREADONLY = 505;

    // BookieProtocol
    static final long INVALID_ENTRY_ID = -1;
    static final long LAST_ADD_CONFIRMED = -1;
    static final int FLAG_NONE = 0x0;
    static final int FLAG_DO_FENCING = 0x0001;
    static final int FLAG_RECOVERY_ADD = 0x0002;
    static final int FLAG_HIGH_PRIORITY = 0x0004;

    /** PerChannelBookieClient.statusCodeToExceptionCode (l.2274). */
    static int statusCodeToExceptionCode(int status) {
        switch (status) {
            case EOK:
                return OK;
            case ENOENTRY:
                return NoSuchEntryException;
            case ENOLEDGER:
                return NoSuchLedgerExistsException;
            case EUA:
                return UnauthorizedAccessException;
            case EFENCED:
                return LedgerFencedException;
            default:
                return UNINITIALIZED;
        }
    }

    /** The client-visible number of a ledger entry id: entry id k carries w1's entry k + 1. */
    static long clientEntryOf(long lastEntryId) {
        // PORT: BookKeeper numbers entries from 0, the interface's clients from 1; -1 stays -1.
        return lastEntryId < 0 ? -1 : lastEntryId + 1;
    }

    // ------------------------------------------------------------------------------------------
    // DistributionSchedule / RoundRobinDistributionSchedule
    // ------------------------------------------------------------------------------------------

    /** RoundRobinDistributionSchedule.WriteSetImpl.reset: (entryId + w) % ensembleSize. */
    static int[] getWriteSet(long entryId) {
        int[] ws = new int[WRITE_QUORUM_SIZE];
        for (int w = 0; w < WRITE_QUORUM_SIZE; w++) {
            ws[w] = (int) ((entryId + w) % ENSEMBLE_SIZE);
        }
        return ws;
    }

    static int indexOf(int[] writeSet, int bookieIndex) {
        for (int i = 0; i < writeSet.length; i++) {
            if (writeSet[i] == bookieIndex) {
                return i;
            }
        }
        return -1;
    }

    /** RoundRobinDistributionSchedule.AckSetImpl. */
    static final class AckSet {
        private final int writeQuorumSize = WRITE_QUORUM_SIZE;
        private final int ackQuorumSize = ACK_QUORUM_SIZE;
        private final BitSet ackSet = new BitSet();
        private final Tid[] failureMap = new Tid[ENSEMBLE_SIZE];

        boolean completeBookieAndCheck(int bookieIndexHeardFrom) {
            failureMap[bookieIndexHeardFrom] = null;
            ackSet.set(bookieIndexHeardFrom);
            return ackSet.cardinality() >= ackQuorumSize;
        }

        boolean failBookieAndCheck(int bookieIndexHeardFrom, Tid address) {
            ackSet.clear(bookieIndexHeardFrom);
            failureMap[bookieIndexHeardFrom] = address;
            return failed() > (writeQuorumSize - ackQuorumSize);
        }

        private int failed() {
            int n = 0;
            for (Tid t : failureMap) {
                if (t != null) {
                    n++;
                }
            }
            return n;
        }
    }

    /** RoundRobinDistributionSchedule.RRQuorumCoverageSet. */
    static final class RRQuorumCoverageSet {
        private final int[] covered = new int[ENSEMBLE_SIZE];

        RRQuorumCoverageSet() {
            for (int i = 0; i < covered.length; i++) {
                covered[i] = UNINITIALIZED;
            }
        }

        void addBookie(int bookieIndexHeardFrom, int rc) {
            covered[bookieIndexHeardFrom] = rc;
        }

        boolean checkCovered() {
            for (int i = 0; i < ENSEMBLE_SIZE; i++) {
                int nodesUnknown = 0;
                for (int j = 0; j < WRITE_QUORUM_SIZE; j++) {
                    int nodeIndex = (i + j) % ENSEMBLE_SIZE;
                    if (covered[nodeIndex] != OK
                        && covered[nodeIndex] != NoSuchEntryException
                        && covered[nodeIndex] != NoSuchLedgerExistsException) {
                        nodesUnknown++;
                    }
                }
                if (nodesUnknown >= ACK_QUORUM_SIZE) {
                    return false;
                }
            }
            return true;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Futures (CompletableFuture as the source uses it: whenComplete, thenCompose)
    // ------------------------------------------------------------------------------------------

    /** A CompletableFuture reduced to what the source uses; an exception is its BKException code. */
    static final class Promise<T> {
        private boolean done;
        private T value;
        private Integer exception;
        private final List<BiConsumer<T, Integer>> callbacks = new ArrayList<>();

        boolean isDone() {
            return done;
        }

        boolean complete(T v) {
            if (done) {
                return false;
            }
            done = true;
            value = v;
            fire();
            return true;
        }

        boolean completeExceptionally(int rc) {
            if (done) {
                return false;
            }
            done = true;
            exception = rc;
            fire();
            return true;
        }

        void whenComplete(BiConsumer<T, Integer> cb) {
            if (done) {
                cb.accept(value, exception);
            } else {
                callbacks.add(cb);
            }
        }

        <U> Promise<U> thenCompose(Function<T, Promise<U>> f) {
            Promise<U> result = new Promise<>();
            whenComplete((v, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                } else {
                    f.apply(v).whenComplete((u, ex2) -> {
                        if (ex2 != null) {
                            result.completeExceptionally(ex2);
                        } else {
                            result.complete(u);
                        }
                    });
                }
            });
            return result;
        }

        static <T> Promise<T> completedFuture(T v) {
            Promise<T> p = new Promise<>();
            p.complete(v);
            return p;
        }

        private void fire() {
            List<BiConsumer<T, Integer>> cbs = new ArrayList<>(callbacks);
            callbacks.clear();
            for (BiConsumer<T, Integer> cb : cbs) {
                cb.accept(value, exception);
            }
        }
    }

    /** BKException (client) as a checked exception carrying its code. */
    static final class BKException extends Exception {
        private static final long serialVersionUID = 1L;
        final int code;

        BKException(int code) {
            super("BKException " + code, null, false, false);
            this.code = code;
        }
    }

    // ------------------------------------------------------------------------------------------
    // zk: the LedgerManager over ZooKeeper, as a versioned store (INTERFACE.md)
    // ------------------------------------------------------------------------------------------

    /** The ledger's metadata as LedgerCreateOp left it: Open, no entries, version 0. */
    static final Versioned INITIAL_METADATA = new Versioned(new LedgerMetadata(State.OPEN, INVALID_ENTRY_ID, 0), 0);

    /** zk: read metadata, write metadata with a version check (ZkLedgerManager setData(version)). */
    static final class ZooKeeperLedgerManager {
        final CommunicationModel model;
        // PORT: the ledger exists from the start (created by w1 before the program).
        Versioned current = INITIAL_METADATA;

        ZooKeeperLedgerManager(CommunicationModel model) {
            this.model = model;
        }

        void run() {
            Channel.recv(model, Setup.class);
            while (true) {
                MpMessage m = Channel.recv(model, x -> x instanceof ReadLedgerMetadata || x instanceof WriteLedgerMetadata);
                if (m instanceof ReadLedgerMetadata r) {
                    Channel.send(model, r.from(), new MetadataResult(r.reqId(), OK, current));
                } else if (m instanceof WriteLedgerMetadata w) {
                    if (w.expectedVersion() == current.version()) {
                        current = new Versioned(w.metadata(), current.version() + 1);
                        Channel.send(model, w.from(), new MetadataResult(w.reqId(), OK, current));
                    } else {
                        // BadVersion -> BKMetadataVersionException
                        Channel.send(model, w.from(), new MetadataResult(w.reqId(), MetadataVersionException, current));
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Bookie: BookieRequestProcessor, WriteEntryProcessorV3, ReadEntryProcessorV3, Bookie,
    // HandleFactoryImpl, LedgerDescriptorImpl, LedgerStorage (in memory)
    // ------------------------------------------------------------------------------------------

    static final class NoLedgerException extends Exception {
        private static final long serialVersionUID = 1L;

        NoLedgerException() {
            super(null, null, false, false);
        }
    }

    static final class NoEntryException extends Exception {
        private static final long serialVersionUID = 1L;

        NoEntryException() {
            super(null, null, false, false);
        }
    }

    static final class BookieLedgerFencedException extends Exception {
        private static final long serialVersionUID = 1L;

        BookieLedgerFencedException() {
            super(null, null, false, false);
        }
    }

    static final class BookieUnauthorizedAccessException extends Exception {
        private static final long serialVersionUID = 1L;

        BookieUnauthorizedAccessException() {
            super(null, null, false, false);
        }
    }

    interface WriteCallback {
        void writeComplete(int rc, long ledgerId, long entryId, Tid addr, Object ctx);
    }

    /** PORT: the in-memory LedgerStorage (InterleavedLedgerStorage's observable behaviour). */
    static final class InMemoryLedgerStorage {
        static final class LedgerState {
            boolean fenced;
            long lastAddConfirmed = INVALID_ENTRY_ID;
            final TreeMap<Long, StoredEntry> entries = new TreeMap<>();
        }

        final TreeMap<Long, LedgerState> ledgers = new TreeMap<>();

        boolean ledgerExists(long ledgerId) {
            return ledgers.containsKey(ledgerId);
        }

        void setMasterKey(long ledgerId) {
            ledgers.putIfAbsent(ledgerId, new LedgerState());
        }

        boolean setFenced(long ledgerId) {
            LedgerState l = ledgers.get(ledgerId);
            if (l.fenced) {
                return false;
            }
            l.fenced = true;
            return true;
        }

        boolean isFenced(long ledgerId) {
            return ledgers.get(ledgerId).fenced;
        }

        long addEntry(StoredEntry entry) {
            LedgerState l = ledgers.get(entry.ledgerId());
            l.entries.put(entry.entryId(), entry);
            // ledgerCache.updateLastAddConfirmed(ledgerId, lac): the piggybacked LAC, if larger
            if (entry.lastAddConfirmed() > l.lastAddConfirmed) {
                l.lastAddConfirmed = entry.lastAddConfirmed();
            }
            return entry.entryId();
        }

        StoredEntry getEntry(long ledgerId, long entryId) throws NoEntryException {
            LedgerState l = ledgers.get(ledgerId);
            if (entryId == LAST_ADD_CONFIRMED) {
                // InterleavedLedgerStorage: entryId = ledgerCache.getLastEntry(ledgerId)
                if (l.entries.isEmpty()) {
                    throw new NoEntryException();
                }
                entryId = l.entries.lastKey();
            }
            StoredEntry entry = l.entries.get(entryId);
            if (entry == null) {
                throw new NoEntryException();
            }
            return entry;
        }

        long getLastAddConfirmed(long ledgerId) {
            return ledgers.get(ledgerId).lastAddConfirmed;
        }
    }

    /** LedgerDescriptorImpl over the in-memory storage. */
    static final class LedgerDescriptorImpl {
        final InMemoryLedgerStorage ledgerStorage;
        final long ledgerId;
        boolean fenceEntryPersisted;
        Boolean logFenceResult; // SettableFuture<Boolean>; null when not created

        LedgerDescriptorImpl(long ledgerId, InMemoryLedgerStorage ledgerStorage) {
            this.ledgerId = ledgerId;
            this.ledgerStorage = ledgerStorage;
        }

        boolean setFenced() {
            return ledgerStorage.setFenced(ledgerId);
        }

        boolean isFenced() {
            return ledgerStorage.isFenced(ledgerId);
        }

        /** Returns the (already completed) fence future's value. */
        boolean fenceAndLogInJournal() {
            boolean success = this.setFenced();
            if (success) {
                return logFenceEntryInJournal();
            } else {
                if (logFenceResult == null || fenceEntryPersisted) {
                    return true;
                } else {
                    // PORT: the in-memory journal never fails, so logFenceResult is done and true.
                    return logFenceResult;
                }
            }
        }

        private boolean logFenceEntryInJournal() {
            // PORT: the in-memory journal logs the fence entry at once with rc 0.
            fenceEntryPersisted = true;
            logFenceResult = true;
            return true;
        }

        long addEntry(StoredEntry entry) {
            return ledgerStorage.addEntry(entry);
        }

        StoredEntry readEntry(long entryId) throws NoEntryException {
            return ledgerStorage.getEntry(ledgerId, entryId);
        }

        long getLastAddConfirmed() {
            return ledgerStorage.getLastAddConfirmed(ledgerId);
        }
    }

    /** A bookie: BookieRequestProcessor over Bookie over its ledger storage; one sequential process. */
    static final class Bookie {
        final CommunicationModel model;
        Tid self;
        final InMemoryLedgerStorage ledgerStorage = new InMemoryLedgerStorage();
        // HandleFactoryImpl
        final TreeMap<Long, LedgerDescriptorImpl> ledgers = new TreeMap<>();
        final TreeMap<Long, LedgerDescriptorImpl> readOnlyLedgers = new TreeMap<>();
        final TreeSet<Long> masterKeyCache = new TreeSet<>();

        Bookie(CommunicationModel model) {
            this.model = model;
        }

        void run() {
            Channel.recv(model, Setup.class);
            self = MpProcess.self();
            while (true) {
                MpMessage m = Channel.recv(model, x -> x instanceof AddRequest || x instanceof ReadRequest);
                processRequest(m);
            }
        }

        /** BookieRequestProcessor.processRequest: dispatch by operation. */
        void processRequest(MpMessage msg) {
            if (msg instanceof AddRequest r) {
                processAddRequestV3(r);
            } else if (msg instanceof ReadRequest r) {
                processReadRequestV3(r);
            }
        }

        void processAddRequestV3(AddRequest r) {
            // PORT: highPriorityThreadPool / writeThreadPool.executeOrdered collapsed into this
            // sequential process (no pool-worker modelling); the processor runs now.
            new WriteEntryProcessorV3(r).safeRun();
        }

        void processReadRequestV3(ReadRequest r) {
            // PORT: highPriorityThreadPool / readThreadPool.executeOrdered collapsed likewise;
            // fenceThreadPool is non-null (numHighPriorityWorkerThreads > 0 by default).
            new ReadEntryProcessorV3(r).safeRun();
        }

        // ---- HandleFactoryImpl

        LedgerDescriptorImpl getHandle(long ledgerId) {
            LedgerDescriptorImpl handle = ledgers.get(ledgerId);
            if (handle == null) {
                // LedgerDescriptor.create: new LedgerDescriptorImpl + ledgerStorage.setMasterKey
                handle = new LedgerDescriptorImpl(ledgerId, ledgerStorage);
                ledgerStorage.setMasterKey(ledgerId);
                ledgers.putIfAbsent(ledgerId, handle);
            }
            // PORT: handle.checkAccess(masterKey) always succeeds (no master keys).
            return handle;
        }

        LedgerDescriptorImpl getReadOnlyHandle(long ledgerId) throws NoLedgerException {
            LedgerDescriptorImpl handle = readOnlyLedgers.get(ledgerId);
            if (handle == null) {
                // LedgerDescriptor.createReadOnly
                if (!ledgerStorage.ledgerExists(ledgerId)) {
                    throw new NoLedgerException();
                }
                handle = new LedgerDescriptorImpl(ledgerId, ledgerStorage);
                readOnlyLedgers.putIfAbsent(ledgerId, handle);
            }
            return handle;
        }

        // ---- Bookie

        LedgerDescriptorImpl getLedgerForEntry(StoredEntry entry) {
            final long ledgerId = entry.ledgerId();
            return getHandle(ledgerId);
        }

        private void addEntryInternal(LedgerDescriptorImpl handle, StoredEntry entry, boolean ackBeforeSync,
                                      WriteCallback cb, Object ctx) {
            long ledgerId = handle.ledgerId;
            long entryId = handle.addEntry(entry);
            if (!masterKeyCache.contains(ledgerId)) {
                // PORT: the master-key journal entry is not modelled (no journal, no keys).
                masterKeyCache.add(ledgerId);
            }
            // getJournal(ledgerId).logAddEntry(entry, ackBeforeSync, cb, ctx)
            // PORT: the in-memory journal completes the write at once: cb runs now with EOK.
            cb.writeComplete(EOK, ledgerId, entryId, self, ctx);
        }

        /** Bookie.recoveryAddEntry (4.13 Bookie.java l.1306): never rejected by fencing. */
        void recoveryAddEntry(StoredEntry entry, WriteCallback cb, Object ctx) {
            LedgerDescriptorImpl handle = getLedgerForEntry(entry);
            addEntryInternal(handle, entry, false, cb, ctx);
        }

        /** Bookie.addEntry (4.13 Bookie.java l.1389): LedgerFencedException when fenced. */
        void addEntry(StoredEntry entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
                throws BookieLedgerFencedException {
            LedgerDescriptorImpl handle = getLedgerForEntry(entry);
            if (handle.isFenced()) {
                throw new BookieLedgerFencedException();
            }
            addEntryInternal(handle, entry, ackBeforeSync, cb, ctx);
        }

        /** Bookie.fenceLedger (l.1429). */
        boolean fenceLedger(long ledgerId) {
            LedgerDescriptorImpl handle = getHandle(ledgerId);
            return handle.fenceAndLogInJournal();
        }

        /** Bookie.readEntry (l.1435). */
        StoredEntry readEntry(long ledgerId, long entryId) throws NoLedgerException, NoEntryException {
            LedgerDescriptorImpl handle = getReadOnlyHandle(ledgerId);
            return handle.readEntry(entryId);
        }

        /** Bookie.readLastAddConfirmed (l.1461). */
        long readLastAddConfirmed(long ledgerId) throws NoLedgerException {
            LedgerDescriptorImpl handle = getReadOnlyHandle(ledgerId);
            return handle.getLastAddConfirmed();
        }

        // ---- WriteEntryProcessorV3

        final class WriteEntryProcessorV3 {
            final AddRequest request;

            WriteEntryProcessorV3(AddRequest request) {
                this.request = request;
            }

            /** getAddResponse: null when the response is sent by the write callback. */
            Integer getAddResponse() {
                final long ledgerId = request.ledgerId();
                final long entryId = request.entryId();
                // PORT: isVersionCompatible() is true; the bookie is never read-only.
                WriteCallback wcb = (rc, lid, eid, addr, ctx) -> {
                    int status;
                    switch (rc) {
                        case EOK:
                            status = EOK;
                            break;
                        case EIO:
                            status = EIO;
                            break;
                        default:
                            status = EUA;
                            break;
                    }
                    sendResponse(status);
                };
                final boolean ackBeforeSync = false; // WriteFlag.NONE
                Integer status = null;
                StoredEntry entryToAdd = request.body();
                try {
                    if (request.recoveryAddFlag()) {
                        recoveryAddEntry(entryToAdd, wcb, null);
                    } else {
                        addEntry(entryToAdd, ackBeforeSync, wcb, null);
                    }
                    status = EOK;
                } catch (BookieLedgerFencedException ex) {
                    status = EFENCED;
                }
                if (status != EOK) {
                    return status;
                }
                return null;
            }

            void safeRun() {
                Integer addResponseStatus = getAddResponse();
                if (null != addResponseStatus) {
                    sendResponse(addResponseStatus);
                }
            }

            void sendResponse(int status) {
                Channel.send(model, request.from(),
                        new AddResponse(request.ledgerId(), request.entryId(), status, request.ctx(), self));
            }
        }

        // ---- ReadEntryProcessorV3

        final class ReadEntryProcessorV3 {
            final ReadRequest readRequest;
            final long ledgerId;
            final long entryId;
            Boolean fenceResult = null; // SettableFuture<Boolean>

            ReadEntryProcessorV3(ReadRequest request) {
                this.readRequest = request;
                this.ledgerId = request.ledgerId();
                this.entryId = request.entryId();
            }

            boolean isFenceRequest() {
                return readRequest.fenceLedgerFlag(); // RequestUtils.isFenceRequest
            }

            void handleReadResultForFenceRead(StoredEntry entryBody) {
                // fenceThreadPool != null: Futures.addCallback(fenceResult, ...)
                // PORT: the fence future is already complete (in-memory journal): callback now.
                sendFenceResponse(entryBody, fenceResult);
            }

            /** readEntry(builder, entryId, readLACPiggyBack = false, sw): null for a fence read. */
            ReadResponse readEntry() throws NoLedgerException, NoEntryException {
                StoredEntry entryBody = Bookie.this.readEntry(ledgerId, entryId);
                if (null != fenceResult) {
                    handleReadResultForFenceRead(entryBody);
                    return null;
                } else {
                    long knownLAC = Bookie.this.readLastAddConfirmed(ledgerId);
                    return new ReadResponse(ledgerId, entryId, EOK, true, entryBody, true, knownLAC, readRequest.ctx());
                }
            }

            ReadResponse getReadResponse() {
                try {
                    if (isFenceRequest()) {
                        if (!readRequest.hasMasterKey()) {
                            throw new BookieUnauthorizedAccessException();
                        } else {
                            fenceResult = Bookie.this.fenceLedger(ledgerId);
                        }
                    }
                    return readEntry();
                } catch (NoLedgerException ex) {
                    return buildResponse(ENOLEDGER);
                } catch (NoEntryException ex) {
                    return buildResponse(ENOENTRY);
                } catch (BookieUnauthorizedAccessException ex) {
                    return buildResponse(EUA);
                }
            }

            void safeRun() {
                // PORT: isVersionCompatible() is true.
                executeOp();
            }

            void executeOp() {
                ReadResponse readResponse = getReadResponse();
                if (null != readResponse) {
                    sendResponse(readResponse);
                }
            }

            void sendFenceResponse(StoredEntry entryBody, boolean fenceResult) {
                // getFenceResponse
                if (!fenceResult) {
                    sendResponse(new ReadResponse(ledgerId, entryId, EIO, false, NO_BODY, false, -1, readRequest.ctx()));
                } else {
                    sendResponse(new ReadResponse(ledgerId, entryId, EOK, true, entryBody, false, -1, readRequest.ctx()));
                }
            }

            ReadResponse buildResponse(int statusCode) {
                return new ReadResponse(ledgerId, entryId, statusCode, false, NO_BODY, false, -1, readRequest.ctx());
            }

            void sendResponse(ReadResponse readResponse) {
                Channel.send(model, readRequest.from(), readResponse);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // BookieClient (PerChannelBookieClient, V3): building requests from the client side
    // ------------------------------------------------------------------------------------------

    static void bookieClientAddEntry(CommunicationModel model, Tid self, Tid addr, long ledgerId, long entryId,
                                     StoredEntry toSend, int ctx, int options) {
        boolean recoveryAdd = (options & FLAG_RECOVERY_ADD) == FLAG_RECOVERY_ADD;
        int priority = (options & FLAG_HIGH_PRIORITY) == FLAG_HIGH_PRIORITY ? 1 : 0;
        Channel.send(model, addr, new AddRequest(self, ledgerId, entryId, recoveryAdd, priority, toSend, ctx));
    }

    static void bookieClientReadEntry(CommunicationModel model, Tid self, Tid addr, long ledgerId, long entryId,
                                      int ctx, int flags, boolean masterKey) {
        boolean fence = (flags & FLAG_DO_FENCING) == FLAG_DO_FENCING;
        int priority = (flags & FLAG_HIGH_PRIORITY) == FLAG_HIGH_PRIORITY ? 1 : 0;
        Channel.send(model, addr, new ReadRequest(self, ledgerId, entryId, fence, fence && masterKey, priority, ctx));
    }

    // ------------------------------------------------------------------------------------------
    // PendingAddOp: one process per op in flight
    // ------------------------------------------------------------------------------------------

    /**
     * PendingAddOp's process part: safeRun, sendWriteRequest, writeComplete. Its handle-side
     * fields (completed as seen by the queue, callbackTriggered, the callback) are kept by the
     * handle in {@link PendingAddOpState}.
     */
    static final class PendingAddOp implements Runnable {
        final CommunicationModel model;
        final Tid lh;
        final long opId;
        final long entryId;
        final boolean isRecoveryAdd;
        final long currentLedgerLength;
        final long payload;
        final List<Tid> ensemble;
        final AckSet ackSet = new AckSet();
        boolean completed = false;
        int pendingWriteRequests = 0;
        boolean hasRun = false;
        final Set<Tid> addEntrySuccessBookies = new LinkedHashSet<>();
        StoredEntry toSend;
        Tid self;

        PendingAddOp(CommunicationModel model, Tid lh, PendingAddOpState state, List<Tid> ensemble) {
            this.model = model;
            this.lh = lh;
            this.opId = state.opId;
            this.entryId = state.entryId;
            this.isRecoveryAdd = state.isRecoveryAdd;
            this.currentLedgerLength = state.currentLedgerLength;
            this.payload = state.payload;
            this.ensemble = ensemble;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            safeRun();
            // writeComplete runs once per response of a write request sent
            while (pendingWriteRequests > 0) {
                AddResponse r = Channel.recv(model, AddResponse.class);
                int rc = statusCodeToExceptionCode(r.status());
                writeComplete(rc, r.ledgerId(), r.entryId(), r.addr(), r.ctx());
            }
        }

        void call(MpMessage m) {
            Channel.send(model, lh, m);
            Channel.recv(model, Ret.class);
        }

        void sendWriteRequest(List<Tid> ensemble, int bookieIndex) {
            int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;
            bookieClientAddEntry(model, self, ensemble.get(bookieIndex), LEDGER_ID, entryId, toSend, bookieIndex, flags);
            ++pendingWriteRequests;
        }

        /** Initiate the add operation (PendingAddOp.safeRun, l.249). */
        void safeRun() {
            hasRun = true;
            // PORT: callbackTriggered, lh.lastAddConfirmed and lh.maybeHandleDelayedWriteBookieFailure()
            // live in the handle: one call reads/executes them there, atomically as on the ordered thread.
            Channel.send(model, lh, new SafeRunCall(opId));
            SafeRunReturn r = Channel.recv(model, SafeRunReturn.class);
            if (r.callbackTriggered()) {
                // failed due to another request ahead in the pending queue: ignore this request
                return;
            }
            // PORT: no digest; the package is (ledgerId, entryId, lac, length, payload).
            this.toSend = new StoredEntry(LEDGER_ID, entryId, r.lastAddConfirmed(), currentLedgerLength, payload);
            // lh.maybeHandleDelayedWriteBookieFailure() ran inside the SafeRunCall.
            int[] writeSet = getWriteSet(entryId);
            for (int i = 0; i < writeSet.length; i++) {
                sendWriteRequest(ensemble, writeSet[i]);
            }
        }

        /** PendingAddOp.writeComplete (l.286). */
        void writeComplete(int rc, long ledgerId, long entryId, Tid addr, int ctx) {
            int bookieIndex = ctx;
            --pendingWriteRequests;

            if (!ensemble.get(bookieIndex).equals(addr)) {
                // ensemble has already changed, failure of this addr is immaterial
                return;
            }

            // must record all acks, even if complete (completion can be undone by an ensemble change)
            boolean ackQuorum = false;
            if (OK == rc) {
                ackQuorum = ackSet.completeBookieAndCheck(bookieIndex);
                addEntrySuccessBookies.add(ensemble.get(bookieIndex));
            }

            if (completed) {
                if (rc != OK) {
                    if (!DISABLE_ENSEMBLE_CHANGE_FEATURE && !DELAY_ENSEMBLE_CHANGE) {
                        call(new NotifyWriteFailedCall(self, bookieIndex, addr));
                    }
                }
                // PORT: the op's completed flag travels with the call (the queue is in the handle).
                call(new SendAddSuccessCallbacksCall(opId, completed));
                return;
            }

            switch (rc) {
                case OK:
                    break;
                case ClientClosedException:
                    call(new ErrorOutPendingAddsCall(self, rc));
                    return;
                case IllegalOpException:
                    call(new HandleUnrecoverableErrorDuringAddCall(self, rc));
                    return;
                case LedgerFencedException:
                    call(new HandleUnrecoverableErrorDuringAddCall(self, rc));
                    return;
                case UnauthorizedAccessException:
                    call(new HandleUnrecoverableErrorDuringAddCall(self, rc));
                    return;
                default:
                    if (DELAY_ENSEMBLE_CHANGE) {
                        if (ackSet.failBookieAndCheck(bookieIndex, addr)) {
                            // PORT: unreachable with delayEnsembleChange = false.
                            call(new HandleBookieFailureCall(self, bookieIndex, addr));
                        }
                    } else {
                        call(new HandleBookieFailureCall(self, bookieIndex, addr));
                    }
                    return;
            }

            if (ackQuorum && !completed) {
                if (ENFORCE_MIN_NUM_FAULT_DOMAINS_FOR_WRITE) {
                    // PORT: placement policy not modelled (enforceMinNumFaultDomainsForWrite = false).
                    return;
                } else {
                    completed = true;
                    // PORT: as above, completed travels with the call.
                    call(new SendAddSuccessCallbacksCall(opId, completed));
                }
            }
        }
    }

    /** AddCallback (addCompleteWithLatency). */
    interface AddCallback {
        void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, Object ctx);
    }

    /** The handle-side fields of a PendingAddOp (PendingAddOp.create, submitCallback). */
    static final class PendingAddOpState {
        final long opId;
        final long payload;
        final long entryLength;
        final AddCallback cb;
        final Object ctx;
        long entryId = INVALID_ENTRY_ID;
        long currentLedgerLength = -1;
        boolean completed = false;
        boolean callbackTriggered = false;
        boolean hasRun = false;
        boolean isRecoveryAdd = false;

        PendingAddOpState(long opId, long payload, AddCallback cb, Object ctx) {
            this.opId = opId;
            this.payload = payload;
            this.entryLength = DATA_LENGTH;
            this.cb = cb;
            this.ctx = ctx;
        }

        PendingAddOpState enableRecoveryAdd() {
            isRecoveryAdd = true;
            return this;
        }

        void setEntryId(long entryId) {
            this.entryId = entryId;
        }

        void setLedgerLength(long ledgerLength) {
            this.currentLedgerLength = ledgerLength;
        }

        /** PendingAddOp.submitCallback (l.418), run on the handle's thread. */
        void submitCallback(LedgerHandle lh, int rc) {
            cb.addCompleteWithLatency(rc, lh, entryId, ctx);
            callbackTriggered = true;
        }
    }

    // ------------------------------------------------------------------------------------------
    // The handle processes w1h / w2h: BookKeeper client + LedgerHandle + ordered executor
    // ------------------------------------------------------------------------------------------

    /**
     * The process of a client's ledger handle: the handle's synchronized state and pending-add
     * queue, the LedgerManager client, and the ledger's ordered executor (its mailbox: a task is a
     * {@link Task} the process sends itself).
     */
    static final class ClientProcess {
        final CommunicationModel model;
        final boolean recoveryClient;
        Tid self;
        Tid zk;
        Tid client;
        List<Tid> ensemble;
        long nextReqId = 0;
        long nextTaskId = 0;
        long nextCbId = 0;
        long nextOpId = 0;
        final Map<Long, Consumer<MetadataResult>> zkCallbacks = new HashMap<>();
        final Map<Long, Runnable> tasks = new HashMap<>();
        final Map<Long, Consumer<MpMessage>> opCallbacks = new HashMap<>();
        LedgerHandle lh;

        ClientProcess(CommunicationModel model, boolean recoveryClient) {
            this.model = model;
            this.recoveryClient = recoveryClient;
        }

        void run() {
            Setup s = Channel.recv(model, Setup.class);
            self = MpProcess.self();
            zk = s.zk();
            ensemble = List.of(s.b1(), s.b2(), s.b3());
            client = recoveryClient ? s.w2() : s.w1();
            if (!recoveryClient) {
                // PORT: w1's handle is the one asyncCreateLedger returned: Open, version 0.
                lh = new LedgerHandle(this, INITIAL_METADATA);
            }
            while (true) {
                MpMessage m = Channel.recv(model);
                dispatch(m);
            }
        }

        void send(Tid dst, MpMessage m) {
            Channel.send(model, dst, m);
        }

        void dispatch(MpMessage m) {
            if (m instanceof Add a) {
                // LedgerHandle.addEntry(data) with a SyncAddCallback: w1 waits for the result
                lh.asyncAddEntry(a.entry(), (rc, h, entryId, ctx) -> {
                    long entry = (Long) ctx;
                    if (rc == OK) {
                        send(client, new Acked(entry));
                    } else {
                        send(client, new Rejected(entry));
                    }
                }, a.entry());
            } else if (m instanceof Recover) {
                // bk.openLedger(ledgerId, ...): LedgerOpenOp with recovery
                new LedgerOpenOp(this).initiate();
            } else if (m instanceof Task t) {
                tasks.remove(t.id()).run();
            } else if (m instanceof MetadataResult r) {
                zkCallbacks.remove(r.reqId()).accept(r);
            } else if (m instanceof SafeRunCall c) {
                PendingAddOpState op = lh.opsById.get(c.opId());
                op.hasRun = true;
                if (op.callbackTriggered) {
                    send(lh.opProcess.get(c.opId()), new SafeRunReturn(true, lh.lastAddConfirmed));
                } else {
                    long lac = lh.lastAddConfirmed;
                    lh.maybeHandleDelayedWriteBookieFailure();
                    send(lh.opProcess.get(c.opId()), new SafeRunReturn(false, lac));
                }
            } else if (m instanceof SendAddSuccessCallbacksCall c) {
                lh.opsById.get(c.opId()).completed = c.completed();
                lh.sendAddSuccessCallbacks();
                send(lh.opProcess.get(c.opId()), new Ret());
            } else if (m instanceof HandleUnrecoverableErrorDuringAddCall c) {
                lh.handleUnrecoverableErrorDuringAdd(c.rc());
                send(c.from(), new Ret());
            } else if (m instanceof ErrorOutPendingAddsCall c) {
                lh.errorOutPendingAdds(c.rc());
                send(c.from(), new Ret());
            } else if (m instanceof NotifyWriteFailedCall c) {
                lh.notifyWriteFailed(c.index(), c.addr());
                send(c.from(), new Ret());
            } else if (m instanceof HandleBookieFailureCall c) {
                TreeMap<Integer, Tid> failed = new TreeMap<>();
                failed.put(c.index(), c.addr());
                lh.handleBookieFailure(failed);
                send(c.from(), new Ret());
            } else if (m instanceof ReadLastConfirmedDataComplete c) {
                opCallbacks.get(c.cbId()).accept(c);
            } else if (m instanceof OnEntryComplete c) {
                opCallbacks.get(c.cbId()).accept(c);
            }
        }

        /** executeOrdered(ledgerId, task) / whenCompleteAsync(..., chooseThread(ledgerId)). */
        void executeOrdered(Runnable r) {
            long id = nextTaskId++;
            tasks.put(id, r);
            send(self, new Task(id));
        }

        long registerOpCallback(Consumer<MpMessage> cb) {
            long id = nextCbId++;
            opCallbacks.put(id, cb);
            return id;
        }

        // ---- LedgerManager client side

        Promise<Versioned> readLedgerMetadata(long ledgerId) {
            Promise<Versioned> p = new Promise<>();
            long id = nextReqId++;
            zkCallbacks.put(id, r -> {
                if (r.rc() == OK) {
                    p.complete(r.versioned());
                } else {
                    p.completeExceptionally(r.rc());
                }
            });
            send(zk, new ReadLedgerMetadata(self, ledgerId, id));
            return p;
        }

        Promise<Versioned> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, long currentVersion) {
            Promise<Versioned> p = new Promise<>();
            long id = nextReqId++;
            zkCallbacks.put(id, r -> {
                if (r.rc() == OK) {
                    p.complete(r.versioned());
                } else {
                    p.completeExceptionally(r.rc());
                }
            });
            send(zk, new WriteLedgerMetadata(self, ledgerId, metadata, currentVersion, id));
            return p;
        }
    }

    // ------------------------------------------------------------------------------------------
    // MetadataUpdateLoop
    // ------------------------------------------------------------------------------------------

    interface NeedsUpdatePredicate {
        boolean needsUpdate(LedgerMetadata metadata) throws BKException;
    }

    interface MetadataTransform {
        LedgerMetadata transform(LedgerMetadata metadata) throws BKException;
    }

    interface LocalValueUpdater {
        boolean updateValue(Versioned oldValue, Versioned newValue);
    }

    static final class MetadataUpdateLoop {
        final ClientProcess lm;
        final long ledgerId;
        final Supplier<Versioned> currentLocalValue;
        final NeedsUpdatePredicate needsTransformation;
        final MetadataTransform transform;
        final LocalValueUpdater updateLocalValue;

        MetadataUpdateLoop(ClientProcess lm, long ledgerId, Supplier<Versioned> currentLocalValue,
                           NeedsUpdatePredicate needsTransformation, MetadataTransform transform,
                           LocalValueUpdater updateLocalValue) {
            this.lm = lm;
            this.ledgerId = ledgerId;
            this.currentLocalValue = currentLocalValue;
            this.needsTransformation = needsTransformation;
            this.transform = transform;
            this.updateLocalValue = updateLocalValue;
        }

        Promise<Versioned> run() {
            Promise<Versioned> promise = new Promise<>();
            writeLoop(currentLocalValue.get(), promise);
            return promise;
        }

        private void writeLoop(Versioned currentLocal, Promise<Versioned> promise) {
            boolean needs;
            LedgerMetadata transformed = null;
            try {
                needs = needsTransformation.needsUpdate(currentLocal.value());
                if (needs) {
                    transformed = transform.transform(currentLocal.value());
                }
            } catch (BKException ex) {
                promise.completeExceptionally(ex.code);
                return;
            }
            if (needs) {
                lm.writeLedgerMetadata(ledgerId, transformed, currentLocal.version())
                    .whenComplete((writtenMetadata, ex) -> {
                        if (ex == null) {
                            if (updateLocalValue.updateValue(currentLocal, writtenMetadata)) {
                                promise.complete(writtenMetadata);
                            } else {
                                writeLoop(currentLocalValue.get(), promise);
                            }
                        } else if (ex == MetadataVersionException) {
                            updateLocalValueFromStore(ledgerId).whenComplete((readMetadata, readEx) -> {
                                if (readEx == null) {
                                    writeLoop(readMetadata, promise);
                                } else {
                                    promise.completeExceptionally(readEx);
                                }
                            });
                        } else {
                            promise.completeExceptionally(ex);
                        }
                    });
            } else {
                promise.complete(currentLocal);
            }
        }

        private Promise<Versioned> updateLocalValueFromStore(long ledgerId) {
            Promise<Versioned> promise = new Promise<>();
            readLoop(ledgerId, promise);
            return promise;
        }

        private void readLoop(long ledgerId, Promise<Versioned> promise) {
            Versioned current = currentLocalValue.get();
            lm.readLedgerMetadata(ledgerId).whenComplete((read, exception) -> {
                if (exception != null) {
                    promise.completeExceptionally(exception);
                } else if (current.version() == read.version()) {
                    // LongVersion.compare: equal versions are CONCURRENTLY: no update needed
                    promise.complete(current);
                } else if (updateLocalValue.updateValue(current, read)) {
                    promise.complete(read);
                } else {
                    readLoop(ledgerId, promise);
                }
            });
        }
    }

    // ------------------------------------------------------------------------------------------
    // LedgerHandle
    // ------------------------------------------------------------------------------------------

    enum HandleState { OPEN, CLOSED }

    static class LedgerHandle {
        final ClientProcess p;
        final long ledgerId = LEDGER_ID;
        private Versioned versionedMetadata;
        long lastAddPushed;
        HandleState handleState = HandleState.OPEN;
        final Promise<Void> closePromise = new Promise<>();
        long lastAddConfirmed;
        long pendingAddsSequenceHead;
        long length;
        boolean changingEnsemble = false;
        int numEnsembleChanges = 0;
        final TreeMap<Integer, Tid> delayedWriteFailedBookies = new TreeMap<>();
        final ArrayDeque<PendingAddOpState> pendingAddOps = new ArrayDeque<>();
        // PORT: the ops by id and their processes, for the calls they make into the handle.
        final Map<Long, PendingAddOpState> opsById = new HashMap<>();
        final Map<Long, Tid> opProcess = new HashMap<>();

        LedgerHandle(ClientProcess p, Versioned versionedMetadata) {
            this.p = p;
            this.versionedMetadata = versionedMetadata;
            LedgerMetadata metadata = versionedMetadata.value();
            if (metadata.isClosed()) {
                lastAddConfirmed = lastAddPushed = metadata.lastEntryId();
                length = metadata.length();
            } else {
                lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
                length = 0;
            }
            this.pendingAddsSequenceHead = lastAddConfirmed;
        }

        LedgerMetadata getLedgerMetadata() {
            return versionedMetadata.value();
        }

        Versioned getVersionedLedgerMetadata() {
            return versionedMetadata;
        }

        boolean setLedgerMetadata(Versioned expected, Versioned newMetadata) {
            if (versionedMetadata == expected) {
                versionedMetadata = newMetadata;
                LedgerMetadata metadata = versionedMetadata.value();
                if (metadata.isClosed()) {
                    lastAddConfirmed = lastAddPushed = metadata.lastEntryId();
                    length = metadata.length();
                }
                return true;
            } else {
                return false;
            }
        }

        long getLastAddConfirmed() {
            return lastAddConfirmed;
        }

        boolean isHandleWritable() {
            return !getLedgerMetadata().isClosed() && handleState == HandleState.OPEN;
        }

        long addToLength(long delta) {
            this.length += delta;
            return this.length;
        }

        PendingAddOpState createOp(long data, AddCallback cb, Object ctx) {
            PendingAddOpState op = new PendingAddOpState(p.nextOpId++, data, cb, ctx);
            opsById.put(op.opId, op);
            return op;
        }

        void asyncAddEntry(long data, AddCallback cb, Object ctx) {
            PendingAddOpState op = createOp(data, cb, ctx);
            doAsyncAddEntry(op);
        }

        /** asyncRecoveryAddEntry (l.1206): recovery adds can add to a fenced ledger. */
        void asyncRecoveryAddEntry(long data, AddCallback cb, Object ctx) {
            PendingAddOpState op = createOp(data, cb, ctx).enableRecoveryAdd();
            doAsyncAddEntry(op);
        }

        /** doAsyncAddEntry (l.1296). */
        void doAsyncAddEntry(final PendingAddOpState op) {
            boolean wasClosed = false;
            if (isHandleWritable()) {
                long entryId = ++lastAddPushed;
                long currentLedgerLength = addToLength(op.entryLength);
                op.setEntryId(entryId);
                op.setLedgerLength(currentLedgerLength);
                pendingAddOps.add(op);
            } else {
                wasClosed = true;
            }

            if (wasClosed) {
                // make sure the callback is triggered in main worker pool
                p.executeOrdered(() -> op.cb.addCompleteWithLatency(LedgerClosedException, this, INVALID_ENTRY_ID, op.ctx));
                return;
            }

            // PORT: waitForWritable is not modelled (waitForWriteSetMs < 0: always writable).
            // clientCtx.getMainWorkerPool().executeOrdered(ledgerId, op): the op's own process.
            PendingAddOp proc = new PendingAddOp(p.model, p.self, op, p.ensemble);
            Tid t = MpProcess.spawnDaemon(proc).tid();
            opProcess.put(op.opId, t);
        }

        void asyncCloseInternal(IntConsumer cb, int rc) {
            doAsyncCloseInternal(cb, rc);
        }

        /** doAsyncCloseInternal (l.541). */
        void doAsyncCloseInternal(final IntConsumer cb, final int rc) {
            p.executeOrdered(() -> {
                final HandleState prevHandleState;
                final List<PendingAddOpState> pendingAdds;
                final long lastEntry;
                final long finalLength;

                closePromise.whenComplete((ignore, ex) -> {
                    if (ex != null) {
                        cb.accept(ex);
                    } else {
                        cb.accept(OK);
                    }
                });

                prevHandleState = handleState;
                // drain pending adds first
                pendingAdds = drainPendingAddsAndAdjustLength();
                // taking the length must occur after draining, as draining changes the length
                lastEntry = lastAddPushed = this.lastAddConfirmed;
                finalLength = this.length;
                handleState = HandleState.CLOSED;

                // error out all pending adds during closing
                errorOutPendingAdds(rc, pendingAdds);

                if (prevHandleState != HandleState.CLOSED) {
                    // tearDownWriteHandleState(): no explicit LAC flush, no timeouts
                    new MetadataUpdateLoop(p, ledgerId, this::getVersionedLedgerMetadata,
                        (metadata) -> {
                            if (metadata.isClosed()) {
                                if (lastEntry == metadata.lastEntryId() && finalLength == metadata.length()) {
                                    return false;
                                } else {
                                    // Metadata conflict when closing ledger
                                    throw new BKException(MetadataVersionException);
                                }
                            } else {
                                return true;
                            }
                        },
                        (metadata) -> new LedgerMetadata(State.CLOSED, lastEntry, finalLength),
                        this::setLedgerMetadata)
                        .run().whenComplete((metadata, ex) -> {
                            if (ex != null) {
                                closePromise.completeExceptionally(ex);
                            } else {
                                closePromise.complete(null);
                            }
                        });
                }
            });
        }

        /** handleUnrecoverableErrorDuringAdd (l.1760): close the ledger and fail the pipeline. */
        void handleUnrecoverableErrorDuringAdd(int rc) {
            if (getLedgerMetadata().getState() == State.IN_RECOVERY) {
                // we should not close ledger if ledger is recovery mode, otherwise we may lose entry.
                errorOutPendingAdds(rc);
                return;
            }
            asyncCloseInternal(ignore -> { }, rc); // NoopCloseCallback
        }

        void errorOutPendingAdds(int rc) {
            errorOutPendingAdds(rc, drainPendingAddsAndAdjustLength());
        }

        List<PendingAddOpState> drainPendingAddsAndAdjustLength() {
            PendingAddOpState pendingAddOp;
            List<PendingAddOpState> opsDrained = new ArrayList<>(pendingAddOps.size());
            while ((pendingAddOp = pendingAddOps.poll()) != null) {
                addToLength(-pendingAddOp.entryLength);
                opsDrained.add(pendingAddOp);
            }
            return opsDrained;
        }

        void errorOutPendingAdds(int rc, List<PendingAddOpState> ops) {
            for (PendingAddOpState op : ops) {
                op.submitCallback(this, rc);
            }
        }

        /** sendAddSuccessCallbacks (l.1803): acknowledgements in entry order. */
        void sendAddSuccessCallbacks() {
            PendingAddOpState pendingAddOp;
            while ((pendingAddOp = pendingAddOps.peek()) != null && !changingEnsemble) {
                if (!pendingAddOp.completed) {
                    return;
                }
                // Check if it is the next entry in the sequence.
                if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != pendingAddsSequenceHead + 1) {
                    return;
                }
                pendingAddOps.remove();
                // explicitLacFlushPolicy.updatePiggyBackedLac: PORT: no explicit LAC
                pendingAddsSequenceHead = pendingAddOp.entryId;
                // writeFlags does not contain DEFERRED_SYNC
                this.lastAddConfirmed = pendingAddsSequenceHead;
                pendingAddOp.submitCallback(this, OK);
            }
        }

        void notifyWriteFailed(int index, Tid addr) {
            delayedWriteFailedBookies.put(index, addr);
        }

        void maybeHandleDelayedWriteBookieFailure() {
            if (delayedWriteFailedBookies.isEmpty()) {
                return;
            }
            TreeMap<Integer, Tid> toReplace = new TreeMap<>(delayedWriteFailedBookies);
            delayedWriteFailedBookies.clear();
            handleBookieFailure(toReplace);
        }

        /** handleBookieFailure (l.1864). */
        void handleBookieFailure(final TreeMap<Integer, Tid> failedBookies) {
            // disableEnsembleChangeFeature not available; writeFlags has no DEFERRED_SYNC
            boolean triggerLoop = false;
            TreeMap<Integer, Tid> toReplace = null;
            List<Tid> origEnsemble = null;
            if (changingEnsemble) {
                delayedWriteFailedBookies.putAll(failedBookies);
            } else {
                changingEnsemble = true;
                triggerLoop = true;
                toReplace = new TreeMap<>(delayedWriteFailedBookies);
                delayedWriteFailedBookies.clear();
                toReplace.putAll(failedBookies);
                origEnsemble = p.ensemble;
            }
            if (triggerLoop) {
                ensembleChangeLoop(origEnsemble, toReplace);
            }
        }

        /** ensembleChangeLoop (l.1906) with no spare bookie (INTERFACE.md). */
        void ensembleChangeLoop(List<Tid> origEnsemble, TreeMap<Integer, Tid> failedBookies) {
            int ensembleChangeId = ++numEnsembleChanges;
            if (ensembleChangeId > MAX_ALLOWED_ENSEMBLE_CHANGES) {
                handleUnrecoverableErrorDuringAdd(WriteException);
                return;
            }
            new MetadataUpdateLoop(p, ledgerId, this::getVersionedLedgerMetadata,
                (metadata) -> metadata.getState() == State.OPEN
                        && failedBookies.entrySet().stream().anyMatch(e -> p.ensemble.get(e.getKey()).equals(e.getValue())),
                (metadata) -> {
                    // PORT: EnsembleUtils.replaceBookiesInEnsemble: no bookie outside the
                    // ensemble exists, so it throws BKNotEnoughBookiesException.
                    throw new BKException(NotEnoughBookiesException);
                },
                this::setLedgerMetadata)
                .run().whenComplete((metadata, ex) -> p.executeOrdered(() -> {
                    if (ex != null) {
                        handleUnrecoverableErrorDuringAdd(ex);
                    } else if (metadata.value().isClosed()) {
                        handleUnrecoverableErrorDuringAdd(LedgerClosedException);
                    } else if (metadata.value().getState() == State.IN_RECOVERY) {
                        handleUnrecoverableErrorDuringAdd(LedgerFencedException);
                    } else {
                        // PORT: the success branch (new ensemble, unsetSuccessAndSendWriteRequest)
                        // needs a replacement bookie; none exists.
                        throw new AssertionError("ensemble change succeeded without a spare bookie");
                    }
                }));
        }
    }

    /** ReadOnlyLedgerHandle: the recovery client's handle. */
    static final class ReadOnlyLedgerHandle extends LedgerHandle {

        ReadOnlyLedgerHandle(ClientProcess p, Versioned metadata) {
            super(p, metadata);
            // watch = !doRecovery = false: no metadata listener
        }

        @Override
        void handleUnrecoverableErrorDuringAdd(int rc) {
            errorOutPendingAdds(rc);
        }

        @Override
        void handleBookieFailure(final TreeMap<Integer, Tid> failedBookies) {
            // PORT: replaceBookiesInEnsemble finds no spare bookie: BKNotEnoughBookiesException
            handleUnrecoverableErrorDuringAdd(NotEnoughBookiesException);
        }

        /** recover(finalCb, listener = null, forceRecovery = false). */
        void recover(IntConsumer cb) {
            NeedsUpdatePredicate needsUpdate = (metadata) -> metadata.getState() == State.OPEN;
            new MetadataUpdateLoop(p, ledgerId, this::getVersionedLedgerMetadata, needsUpdate,
                (metadata) -> new LedgerMetadata(State.IN_RECOVERY, metadata.lastEntryId(), metadata.length()),
                this::setLedgerMetadata)
                .run()
                .<LedgerHandle>thenCompose((metadata) -> {
                    if (metadata.value().isClosed()) {
                        return Promise.completedFuture(ReadOnlyLedgerHandle.this);
                    } else {
                        return new LedgerRecoveryOp(ReadOnlyLedgerHandle.this, p).initiate();
                    }
                })
                .thenCompose((ignore) -> closeRecovered())
                .whenComplete((ignore, ex) -> {
                    if (ex != null) {
                        cb.accept(ex);
                    } else {
                        cb.accept(OK);
                    }
                });
        }

        Promise<Versioned> closeRecovered() {
            final long lac;
            final long len;
            lac = lastAddConfirmed;
            len = length;
            // newEnsemblesFromRecovery is empty (no ensemble change)
            return new MetadataUpdateLoop(p, ledgerId, this::getVersionedLedgerMetadata,
                (metadata) -> metadata.getState() == State.IN_RECOVERY,
                (metadata) -> new LedgerMetadata(State.CLOSED, lac, len),
                this::setLedgerMetadata).run();
        }
    }

    /** LedgerOpenOp (with recovery) in the recovery client's process. */
    static final class LedgerOpenOp {
        final ClientProcess bk;
        final long ledgerId = LEDGER_ID;
        final boolean doRecovery = true;
        ReadOnlyLedgerHandle lh;

        LedgerOpenOp(ClientProcess bk) {
            this.bk = bk;
        }

        void initiate() {
            bk.readLedgerMetadata(ledgerId).whenComplete((metadata, exception) -> {
                if (exception != null) {
                    openComplete(exception, null);
                } else {
                    openWithMetadata(metadata);
                }
            });
        }

        private void openWithMetadata(Versioned versionedMetadata) {
            LedgerMetadata metadata = versionedMetadata.value();
            // PORT: no password / digest checks.
            lh = new ReadOnlyLedgerHandle(bk, versionedMetadata);
            bk.lh = lh;
            if (metadata.isClosed()) {
                openComplete(OK, lh);
                return;
            }
            if (doRecovery) {
                // OrderedGenericCallback: completes on the ledger's ordered executor
                lh.recover(rc -> bk.executeOrdered(() -> {
                    if (rc == OK) {
                        openComplete(OK, lh);
                    } else if (rc == UnauthorizedAccessException) {
                        openComplete(UnauthorizedAccessException, null);
                    } else {
                        openComplete(LedgerRecoveryException, null);
                    }
                }));
            }
        }

        void openComplete(int rc, LedgerHandle lh) {
            // cb.openComplete(rc, lh, ctx): w2's openLedger returns lh; it reports lh's LAC.
            if (rc == OK) {
                bk.send(bk.client, new Closed(clientEntryOf(lh.getLastAddConfirmed())));
            }
            // PORT: a failed open has no reply in the interface: w2 stays waiting (blocked).
        }
    }

    // ------------------------------------------------------------------------------------------
    // LedgerRecoveryOp (in w2h), ReadLastConfirmedOp and recovery reads (own processes)
    // ------------------------------------------------------------------------------------------

    static final class LedgerRecoveryOp implements AddCallback {
        final LedgerHandle lh;
        final ClientProcess clientCtx;
        final Promise<LedgerHandle> promise;
        long readCount = 0;
        long writeCount = 0;
        boolean readDone = false;
        long startEntryToRead;
        long endEntryToRead;
        LedgerMetadata metadataForRecovery;

        LedgerRecoveryOp(LedgerHandle lh, ClientProcess clientCtx) {
            this.promise = new Promise<>();
            this.lh = lh;
            this.clientCtx = clientCtx;
        }

        Promise<LedgerHandle> initiate() {
            long cbId = clientCtx.registerOpCallback(m -> {
                ReadLastConfirmedDataComplete d = (ReadLastConfirmedDataComplete) m;
                readLastConfirmedDataComplete(d.rc(), d.lastAddConfirmed(), d.length());
            });
            // new ReadLastConfirmedOp(...); rlcop.initiateWithFencing(): its own process
            MpProcess.spawnDaemon(new ReadLastConfirmedOp(clientCtx.model, clientCtx.self, cbId, LEDGER_ID,
                    clientCtx.ensemble));
            return promise;
        }

        void readLastConfirmedDataComplete(int rc, long dataLastAddConfirmed, long dataLength) {
            if (rc == OK) {
                lh.lastAddPushed = lh.lastAddConfirmed = dataLastAddConfirmed;
                lh.length = dataLength;
                lh.pendingAddsSequenceHead = lh.lastAddConfirmed;
                startEntryToRead = endEntryToRead = lh.lastAddConfirmed;
                // keep a copy of ledger metadata before proceeding ledger recovery
                metadataForRecovery = lh.getLedgerMetadata();
                doRecoveryRead();
            } else if (rc == UnauthorizedAccessException) {
                submitCallback(rc);
            } else {
                submitCallback(ReadException);
            }
        }

        private void submitCallback(int rc) {
            if (OK == rc) {
                promise.complete(lh);
            } else {
                promise.completeExceptionally(rc);
            }
        }

        /** Try to read past the last confirmed. */
        private void doRecoveryRead() {
            if (!promise.isDone()) {
                startEntryToRead = endEntryToRead + 1;
                endEntryToRead = endEntryToRead + RECOVERY_READ_BATCH_SIZE;
                long cbId = clientCtx.registerOpCallback(m -> {
                    OnEntryComplete c = (OnEntryComplete) m;
                    onEntryComplete(c.rc(), c.hasEntry(), c.entryId(), c.length(), c.data());
                });
                // new RecoveryReadOp(lh, clientCtx, start, end, this, null).initiate(): own process
                MpProcess.spawnDaemon(new RecoveryReadOp(clientCtx.model, clientCtx.self, cbId, LEDGER_ID,
                        startEntryToRead, endEntryToRead, clientCtx.ensemble));
            }
        }

        /** onEntryComplete (ReadEntryListener). */
        void onEntryComplete(int rc, boolean hasEntry, long entryEntryId, long entryLength, long entryData) {
            // entryListener == null
            if (!promise.isDone() && !readDone && rc == OK) {
                readCount++;
                long data = entryData;
                lh.length = entryLength - DATA_LENGTH;
                if (entryEntryId != lh.lastAddPushed + 1) {
                    rc = UnexpectedConditionException;
                }
                if (OK == rc) {
                    lh.asyncRecoveryAddEntry(data, this, null);
                    if (entryEntryId == endEntryToRead) {
                        // trigger next batch read
                        doRecoveryRead();
                    }
                    return;
                }
            }

            // no entry found. stop recovery procedure but wait until recovery add finished.
            if (rc == NoSuchEntryException || rc == NoSuchLedgerExistsException) {
                readDone = true;
                if (readCount == writeCount) {
                    submitCallback(OK);
                }
                return;
            }

            // otherwise, some other error, we can't handle
            if (OK != rc && !promise.isDone()) {
                submitCallback(rc);
            }
            // else: read an entry but readDone was already true (gaps in the tail): logged only
        }

        @Override
        public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, Object ctx) {
            addComplete(rc, lh, entryId, ctx);
        }

        void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            if (rc != OK) {
                submitCallback(rc);
                return;
            }
            long numAdd = ++writeCount;
            if (readDone && readCount == numAdd) {
                submitCallback(rc);
            }
        }
    }

    /** ReadLastConfirmedOp: its own process; initiateWithFencing, readEntryComplete. */
    static final class ReadLastConfirmedOp implements Runnable {
        final CommunicationModel model;
        final Tid lh;
        final long cbId;
        final long ledgerId;
        final List<Tid> currentEnsemble;
        int numResponsesPending;
        long maxLac = INVALID_ENTRY_ID; // maxRecoveredData = new RecoveryData(INVALID_ENTRY_ID, 0)
        long maxLength = 0;
        boolean completed = false;
        int lastSeenError = ReadException;
        final RRQuorumCoverageSet coverageSet = new RRQuorumCoverageSet();
        Tid self;

        ReadLastConfirmedOp(CommunicationModel model, Tid lh, long cbId, long ledgerId, List<Tid> ensemble) {
            this.model = model;
            this.lh = lh;
            this.cbId = cbId;
            this.ledgerId = ledgerId;
            this.currentEnsemble = ensemble;
            this.numResponsesPending = ensemble.size();
        }

        @Override
        public void run() {
            self = MpProcess.self();
            initiateWithFencing();
            // readEntryComplete runs once per response
            while (numResponsesPending > 0) {
                ReadResponse r = Channel.recv(model, ReadResponse.class);
                readEntryComplete(statusCodeToExceptionCode(r.status()), r.ledgerId(), r.entryId(), r.body(), r.ctx());
            }
        }

        void initiateWithFencing() {
            for (int i = 0; i < currentEnsemble.size(); i++) {
                bookieClientReadEntry(model, self, currentEnsemble.get(i), ledgerId, LAST_ADD_CONFIRMED, i,
                        FLAG_DO_FENCING, true);
            }
        }

        void cb(int rc) {
            Channel.send(model, lh, new ReadLastConfirmedDataComplete(cbId, rc, maxLac, maxLength));
        }

        void readEntryComplete(final int rc, final long ledgerId, final long entryId, final StoredEntry buffer,
                               final int ctx) {
            int bookieIndex = ctx;
            coverageSet.addBookie(bookieIndex, rc);
            numResponsesPending--;
            boolean heardValidResponse = false;
            if (rc == OK) {
                // PORT: no digest: verifyDigestAndReturnLastConfirmed reads the packaged LAC and length.
                long recoveredLac = buffer.lastAddConfirmed();
                long recoveredLength = buffer.length();
                if (recoveredLac > maxLac) {
                    maxLac = recoveredLac;
                    maxLength = recoveredLength;
                }
                heardValidResponse = true;
            }

            if (rc == NoSuchLedgerExistsException || rc == NoSuchEntryException) {
                // this still counts as a valid response, e.g., if the client crashed without writing any entry
                heardValidResponse = true;
            }

            if (rc == UnauthorizedAccessException && !completed) {
                cb(rc);
                completed = true;
            }

            if (!heardValidResponse && OK != rc) {
                lastSeenError = rc;
            }

            // other return codes dont count as valid responses
            if (heardValidResponse && coverageSet.checkCovered() && !completed) {
                completed = true;
                cb(OK);
                return;
            }

            if (numResponsesPending == 0 && !completed) {
                cb(lastSeenError);
            }
        }
    }

    /**
     * RecoveryReadOp = ListenerBasedPendingReadOp(isRecoveryRead = true) with SequenceReadRequests
     * (parallelRead = false): its own process.
     */
    static final class RecoveryReadOp implements Runnable {
        final CommunicationModel model;
        final Tid lh;
        final long cbId;
        final long ledgerId;
        final long startEntryId;
        final long endEntryId;
        final List<Tid> ensemble;
        final boolean isRecoveryRead = true;
        final int requiredBookiesMissingEntryForRecovery = WRITE_QUORUM_SIZE - ACK_QUORUM_SIZE + 1;
        final List<SequenceReadRequest> seq = new ArrayList<>();
        final BitSet heardFromHostsBitSet = new BitSet(ENSEMBLE_SIZE);
        long numPendingEntries;
        int outstanding = 0; // PORT: read requests in flight, to end the process
        Tid self;

        RecoveryReadOp(CommunicationModel model, Tid lh, long cbId, long ledgerId, long startEntryId,
                       long endEntryId, List<Tid> ensemble) {
            this.model = model;
            this.lh = lh;
            this.cbId = cbId;
            this.ledgerId = ledgerId;
            this.startEntryId = startEntryId;
            this.endEntryId = endEntryId;
            this.ensemble = ensemble;
            this.numPendingEntries = endEntryId - startEntryId + 1;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            initiate();
            while (outstanding > 0) {
                ReadResponse r = Channel.recv(model, ReadResponse.class);
                outstanding--;
                readEntryComplete(statusCodeToExceptionCode(r.status()), r.ledgerId(), r.entryId(),
                        r.body(), r.ctx());
            }
        }

        final class SequenceReadRequest {
            boolean complete = false;
            int rc = OK;
            int firstError = OK;
            int numBookiesMissingEntry = 0;
            final List<Tid> ensemble;
            final int[] writeSet;
            final long eId;
            long entryLength;
            long entryData;
            int nextReplicaIndexToReadFrom = 0;
            final BitSet sentReplicas = new BitSet(WRITE_QUORUM_SIZE);
            final BitSet erroredReplicas = new BitSet(WRITE_QUORUM_SIZE);

            SequenceReadRequest(List<Tid> ensemble, long lId, long eId) {
                this.ensemble = ensemble;
                this.eId = eId;
                // enableReorderReadSequence = false; sticky reads disabled
                this.writeSet = getWriteSet(eId);
            }

            boolean isComplete() {
                return complete;
            }

            int getRc() {
                return rc;
            }

            void read() {
                sendNextRead();
            }

            boolean readsOutstanding() {
                return (sentReplicas.cardinality() - erroredReplicas.cardinality()) > 0;
            }

            Tid sendNextRead() {
                if (nextReplicaIndexToReadFrom >= WRITE_QUORUM_SIZE) {
                    // we are done, the read has failed from all replicas, just fail the read
                    fail(firstError);
                    return null;
                }
                int replica = nextReplicaIndexToReadFrom;
                int bookieIndex = writeSet[nextReplicaIndexToReadFrom];
                nextReplicaIndexToReadFrom++;
                Tid to = ensemble.get(bookieIndex);
                sendReadTo(bookieIndex, to, this);
                sentReplicas.set(replica);
                return to;
            }

            /** LedgerEntryRequest.complete + SequenceReadRequest.complete. */
            boolean complete(int bookieIndex, Tid host, StoredEntry buffer) {
                if (isComplete()) {
                    return false;
                }
                // PORT: no digest: verifyDigestAndReturnData returns the payload.
                if (!complete) {
                    complete = true;
                    rc = OK;
                    entryLength = buffer.length();
                    entryData = buffer.data();
                    // PORT: registerSlowBookie (placement policy, speculative reads) not modelled.
                    return true;
                } else {
                    return false;
                }
            }

            boolean fail(int rc) {
                if (!complete) {
                    complete = true;
                    this.rc = rc;
                    submitCallback(rc);
                    return true;
                } else {
                    return false;
                }
            }

            /** LedgerEntryRequest.logErrorAndReattemptRead + SequenceReadRequest override. */
            void logErrorAndReattemptRead(int bookieIndex, Tid host, int rc) {
                if (OK == firstError || NoSuchEntryException == firstError || NoSuchLedgerExistsException == firstError) {
                    firstError = rc;
                }
                if (NoSuchEntryException == rc || NoSuchLedgerExistsException == rc) {
                    ++numBookiesMissingEntry;
                }
                // lh.recordReadErrorOnBookie(bookieIndex): PORT: bookie health not modelled.

                int replica = indexOf(writeSet, bookieIndex);
                if (replica == -1) {
                    return;
                }
                erroredReplicas.set(replica);

                if (isRecoveryRead && (numBookiesMissingEntry >= requiredBookiesMissingEntryForRecovery)) {
                    // For recovery, report NoSuchEntry as soon as wQ-aQ+1 bookies report that they do not have the entry
                    fail(NoSuchEntryException);
                    return;
                }

                if (!readsOutstanding()) {
                    sendNextRead();
                }
            }
        }

        /** PendingReadOp.initiate (l.510). */
        void initiate() {
            long i = startEntryId;
            do {
                seq.add(new SequenceReadRequest(ensemble, ledgerId, i));
                i++;
            } while (i <= endEntryId);
            for (SequenceReadRequest entry : seq) {
                entry.read();
                // PORT: no speculative read policy (no timers).
            }
        }

        /** PendingReadOp.sendReadTo (l.566): 4.13 sends recovery reads WITHOUT FLAG_DO_FENCING. */
        void sendReadTo(int bookieIndex, Tid to, SequenceReadRequest entry) {
            int flags = isRecoveryRead ? FLAG_HIGH_PRIORITY : FLAG_NONE;
            bookieClientReadEntry(model, self, to, ledgerId, entry.eId, bookieIndex, flags, false);
            outstanding++;
        }

        /** PendingReadOp.readEntryComplete (l.577). */
        void readEntryComplete(int rc, long ledgerId, final long entryId, final StoredEntry buffer, int ctx) {
            final int bookieIndex = ctx;
            SequenceReadRequest entry = null;
            for (SequenceReadRequest r : seq) {
                if (r.eId == entryId) {
                    entry = r;
                }
            }
            if (entry == null) {
                // PORT: the request was already handed to the listener and removed from seq;
                // with batch size 1 and no speculative reads no response arrives after that.
                return;
            }
            if (rc != OK) {
                entry.logErrorAndReattemptRead(bookieIndex, ensemble.get(bookieIndex), rc);
                return;
            }
            heardFromHostsBitSet.set(bookieIndex, true);
            if (entry.complete(bookieIndex, ensemble.get(bookieIndex), buffer)) {
                // isRecoveryRead: do not advance LastAddConfirmed for recovery reads
                submitCallback(OK);
            }
        }

        /** ListenerBasedPendingReadOp.submitCallback. */
        void submitCallback(int code) {
            SequenceReadRequest request;
            while (!seq.isEmpty() && (request = seq.get(0)) != null) {
                if (!request.isComplete()) {
                    return;
                }
                seq.remove(0);
                if (OK == request.getRc()) {
                    Channel.send(model, lh, new OnEntryComplete(cbId, request.getRc(), true, request.eId,
                            request.entryLength, request.entryData));
                } else {
                    Channel.send(model, lh, new OnEntryComplete(cbId, request.getRc(), false, -1, -1, -1));
                }
            }
        }
    }
}
