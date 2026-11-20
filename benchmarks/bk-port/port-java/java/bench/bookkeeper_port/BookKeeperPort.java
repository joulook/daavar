package bench.bookkeeper_port;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Port of Apache BookKeeper's ledger replication with fencing (apache/bookkeeper main 7b66b5d, cut in
 * plan/case-studies/bookkeeper/bk-src/main) for one ledger, one writer w1, one recovery client w2, three
 * bookies and a metadata store zk, against JMC's message-passing API. The program interface is
 * plan/case-studies/bookkeeper/INTERFACE-agents.md. See NOTES.md next to this source tree for the roles, the
 * messages, the method map and every decision. Lines that differ from the source are marked {@code // PORT:}.
 *
 * <p>Entry numbering: the client's {@code Add(i)} is the entry with id {@code i - 1} (the source's 0-based ids);
 * {@code Closed(last)} reports the number {@code id + 1} of the last entry, -1 for an empty ledger.
 */
public final class BookKeeperPort implements MpProgram {

    // ------------------------------------------------------------------------------------------
    // Instance parameters (INTERFACE-agents.md): ensemble 3, Qw = 3, Qa = 2.
    // ------------------------------------------------------------------------------------------
    static final int ENSEMBLE_SIZE = 3;
    static final int WRITE_QUORUM_SIZE = 3;
    static final int ACK_QUORUM_SIZE = 2;
    static final int RECOVERY_READ_BATCH_SIZE = 1; // PORT: batch size 1 (interface)
    static final long INVALID_ENTRY_ID = -1;       // LedgerHandle.INVALID_ENTRY_ID
    static final long LAST_ADD_CONFIRMED = -1;     // BookieProtocol.LAST_ADD_CONFIRMED

    /** BKException.Code values used by the port (the source's numeric codes are irrelevant here). */
    static final int OK = 0;
    static final int UNINITIALIZED = 1;
    static final int ReadException = -1;
    static final int NoSuchEntryException = -13;
    static final int LedgerClosedException = -11;
    static final int LedgerFencedException = -101;
    static final int NoSuchLedgerExistsException = -7;
    static final int UnexpectedConditionException = -999;
    static final int WriteException = -12; // BKNotEnoughBookiesException is reported as a write failure (D1)
    /** ClientConfiguration defaults: ensemble changes enabled, not delayed (PendingAddOp l.305-306). */
    static final boolean DISABLE_ENSEMBLE_CHANGE_FEATURE = false;
    static final boolean DELAY_ENSEMBLE_CHANGE = false;

    // ------------------------------------------------------------------------------------------
    // Visible messages (INTERFACE-agents.md, fixed shape and tags).
    // ------------------------------------------------------------------------------------------
    @Tag("Add") public record Add(long entry) implements MpMessage {}
    @Tag("Recover") public record Recover() implements MpMessage {}
    @Tag("Acked") public record Acked(long entry) implements MpMessage {}
    @Tag("Rejected") public record Rejected(long entry) implements MpMessage {}
    @Tag("Closed") public record Closed(long last) implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Invisible messages: setup, bookie protocol (AddRequest/AddResponse, ReadRequest/ReadResponse with the
    // fence flag), metadata store (read, versioned write).
    // ------------------------------------------------------------------------------------------
    /** The addresses of every process, sent once by init to every internal process. */
    @Tag("Setup") public record Setup(Tid b1, Tid b2, Tid b3, Tid zk, Tid w1h, Tid w2h, Tid w1, Tid w2)
            implements MpMessage {}

    /** Which handle a LedgerHandle process is (process-local, never sent). */
    enum Client { W1H, W2H }

    /** proto StatusCode subset. */
    public enum StatusCode { EOK, EFENCED, ENOENTRY, ENOLEDGER }

    /** AddRequest (BookieProtocol / proto AddRequest): FLAG_RECOVERY_ADD as recoveryAdd; the body is the
     *  entry id, the piggybacked LAC and the data (PORT: no digest, no master key, no ledger id: one ledger). */
    @Tag("AddRequest") public record AddRequest(Tid from, long entryId, long lastAddConfirmed, long data,
                                                boolean recoveryAdd) implements MpMessage {}
    @Tag("AddResponse") public record AddResponse(int bookieIndex, long entryId, StatusCode status)
            implements MpMessage {}

    /** ReadRequest with FLAG_DO_FENCING as doFencing (entryId LAST_ADD_CONFIRMED for the LAC read). */
    @Tag("ReadRequest") public record ReadRequest(Tid from, long entryId, boolean doFencing) implements MpMessage {}
    /** ReadResponse: the entry body is (entryLac, data); maxLAC as in the non-fence branch; -1 where absent. */
    @Tag("ReadResponse") public record ReadResponse(int bookieIndex, long entryId, StatusCode status,
                                                    long bodyEntryId, long bodyLac, long data, long maxLAC)
            implements MpMessage {}

    public enum State { OPEN, IN_RECOVERY, CLOSED }

    /** LedgerMetadata, reduced to state and last entry id (PORT: fixed ensemble, no length). */
    @Tag("LedgerMetadata") public record LedgerMetadata(State state, long lastEntryId) implements MpMessage {
        boolean isClosed() {
            return state == State.CLOSED;
        }
    }

    @Tag("ReadMeta") public record ReadMeta(Tid from) implements MpMessage {}
    @Tag("MetaValue") public record MetaValue(LedgerMetadata metadata, long version) implements MpMessage {}
    @Tag("WriteMeta") public record WriteMeta(Tid from, LedgerMetadata metadata, long expectedVersion)
            implements MpMessage {}
    @Tag("MetaWritten") public record MetaWritten(long version) implements MpMessage {}
    @Tag("MetaBadVersion") public record MetaBadVersion() implements MpMessage {}

    /**
     * A task appended to a handle's ordered executor (LedgerHandle.executeOrdered / the bookie client's
     * callback executor): a message the handle sends to itself (D10). seq names the task in the handle's
     * process-local task registry; kind says what it is (for reading traces only).
     */
    @Tag("ExecutorTask") public record ExecutorTask(long seq, String kind) implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Program.
    // ------------------------------------------------------------------------------------------
    private final long e;
    private final CommunicationModel model;

    public BookKeeperPort(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<fifo|bag>");
        }
        this.e = Long.parseLong(args[1]);
        if (e < 1) {
            throw new IllegalArgumentException("e >= 1");
        }
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("fifo or bag, not " + args[2]);
        };
    }

    @Override public String programName() { return "bk-port"; }
    @Override public String size() { return Long.toString(e); }
    @Override public CommunicationModel model() { return model; }
    @Override public List<String> rolesInSpawnOrder() { return List.of("w1", "w2"); }
    @Override public List<String> visibleRoles() { return List.of("w1", "w2"); }

    @Override
    public void run() {
        MpProcess.Handle b1 = MpProcess.spawnDaemon(() -> new BookieImpl(0).run());
        MpProcess.Handle b2 = MpProcess.spawnDaemon(() -> new BookieImpl(1).run());
        MpProcess.Handle b3 = MpProcess.spawnDaemon(() -> new BookieImpl(2).run());
        MpProcess.Handle zk = MpProcess.spawnDaemon(() -> new MetadataStore().run());
        MpProcess.Handle w1h = MpProcess.spawnDaemon(() -> new LedgerHandle(Client.W1H).run());
        MpProcess.Handle w2h = MpProcess.spawnDaemon(() -> new LedgerHandle(Client.W2H).run());
        final Tid w1hTid = w1h.tid();
        final Tid w2hTid = w2h.tid();
        MpProcess.Handle w1 = MpProcess.spawn("w1", () -> writer(e, w1hTid));
        MpProcess.Handle w2 = MpProcess.spawn("w2", () -> recoverer(w2hTid));
        final Setup setup = new Setup(b1.tid(), b2.tid(), b3.tid(), zk.tid(), w1hTid, w2hTid, w1.tid(), w2.tid());
        MpProcess.spawnDaemon(() -> {
            for (Tid t : List.of(setup.b1(), setup.b2(), setup.b3(), setup.zk(), setup.w1h(), setup.w2h())) {
                send(t, setup);
            }
        });
        w1.await();
        w2.await();
    }

    private void send(Tid dst, MpMessage m) {
        Channel.send(model, dst, m);
    }

    private MpMessage recv(Predicate<MpMessage> guard) {
        return Channel.recv(model, guard);
    }

    // ------------------------------------------------------------------------------------------
    // Clients (INTERFACE-agents.md, the Rust loops in Java).
    // ------------------------------------------------------------------------------------------
    void writer(long e, Tid h) {
        for (long i = 1; i <= e; i++) {
            send(h, new Add(i));
            MpMessage r = recv(m -> m instanceof Acked || m instanceof Rejected || m instanceof Closed);
            if (!(r instanceof Acked) && !(r instanceof Rejected)) {
                throw new AssertionError("Acked or Rejected expected");
            }
        }
    }

    void recoverer(Tid h) {
        send(h, new Recover());
        MpMessage r = recv(m -> m instanceof Acked || m instanceof Rejected || m instanceof Closed);
        if (!(r instanceof Closed)) {
            throw new AssertionError("Closed expected");
        }
    }

    static Setup awaitSetup(BookKeeperPort p) {
        return (Setup) p.recv(m -> m instanceof Setup);
    }

    static long number(long entryId) {
        // Interface: Closed reports the number id + 1 of the last entry, -1 when the ledger is empty.
        return entryId == INVALID_ENTRY_ID ? -1 : entryId + 1;
    }

    // ------------------------------------------------------------------------------------------
    // zk: the metadata store (PORT: LedgerManager over ZooKeeper is not in the cut; the interface defines it
    // as a versioned store: read, write with a version check).
    // ------------------------------------------------------------------------------------------
    final class MetadataStore {
        LedgerMetadata metadata = new LedgerMetadata(State.OPEN, INVALID_ENTRY_ID); // PORT: created by w1 earlier
        long version = 0;
        Setup addr;

        void run() {
            addr = awaitSetup(BookKeeperPort.this);
            while (true) {
                MpMessage m = recv(x -> x instanceof ReadMeta || x instanceof WriteMeta);
                if (m instanceof ReadMeta r) {
                    send(r.from(), new MetaValue(metadata, version));
                } else if (m instanceof WriteMeta w) {
                    if (w.expectedVersion() == version) {
                        metadata = w.metadata();
                        version++;
                        send(w.from(), new MetaWritten(version));
                    } else {
                        send(w.from(), new MetaBadVersion());
                    }
                }
            }
        }
    }


    // ------------------------------------------------------------------------------------------
    // Bookie: BookieImpl over in-memory ledger storage, WriteEntryProcessorV3, ReadEntryProcessorV3.
    // ------------------------------------------------------------------------------------------
    /** A stored entry: the body's entry id, piggybacked LAC and data. PORT: in-memory, no journal. */
    record StoredEntry(long entryId, long lac, long data) {}

    static final class BookieException extends Exception {
        final String code;
        BookieException(String code) {
            super(code, null, false, false);
            this.code = code;
        }
    }

    final class BookieImpl {
        final int bookieIndex;
        Setup addr;
        // LedgerDescriptor / LedgerStorage of the one ledger (PORT: in memory).
        boolean ledgerExists = false;
        boolean fenced = false;
        final TreeMap<Long, StoredEntry> entries = new TreeMap<>();
        long lastAddConfirmed = INVALID_ENTRY_ID;

        BookieImpl(int bookieIndex) {
            this.bookieIndex = bookieIndex;
        }

        void run() {
            addr = awaitSetup(BookKeeperPort.this);
            while (true) {
                // BookieRequestProcessor dispatch. PORT: one process, requests handled one at a time.
                MpMessage m = recv(x -> x instanceof AddRequest || x instanceof ReadRequest);
                if (m instanceof AddRequest a) {
                    writeEntryProcessorV3(a);
                } else if (m instanceof ReadRequest r) {
                    readEntryProcessorV3(r);
                }
            }
        }

        /** WriteEntryProcessorV3.getAddResponse / run (l.45-185). */
        void writeEntryProcessorV3(AddRequest addRequest) {
            Tid client = addRequest.from();
            StatusCode status;
            try {
                if (addRequest.recoveryAdd()) {
                    recoveryAddEntry(addRequest);
                } else {
                    addEntry(addRequest);
                }
                status = StatusCode.EOK;
            } catch (BookieException ex) {
                // LedgerFencedException -> EFENCED (l.132-139); the other catch clauses cannot occur.
                status = StatusCode.EFENCED;
            }
            // PORT: the write callback (l.68-97, rc EOK after the journal) and the error path (l.158-163)
            // both send one AddResponse; with no journal the callback runs at once.
            send(client, new AddResponse(bookieIndex, addRequest.entryId(), status));
        }

        /** BookieImpl.addEntry (l.1112). */
        void addEntry(AddRequest entry) throws BookieException {
            getLedgerForEntry();
            if (fenced) {
                throw new BookieException("LedgerFencedException");
            }
            addEntryInternal(entry);
        }

        /** BookieImpl.recoveryAddEntry (l.1024): never rejected by fencing. */
        void recoveryAddEntry(AddRequest entry) {
            getLedgerForEntry();
            addEntryInternal(entry);
        }

        /** BookieImpl.getLedgerForEntry (l.955): handles.getHandle creates the ledger. */
        void getLedgerForEntry() {
            ledgerExists = true;
        }

        /** BookieImpl.addEntryInternal (l.980): handle.addEntry into the ledger storage. */
        void addEntryInternal(AddRequest entry) {
            // PORT: LedgerStorage.addEntry in memory; the storage's LAC is the highest LAC piggybacked
            // on a stored entry (bk-src README; the storage implementations are not in the cut).
            entries.put(entry.entryId(), new StoredEntry(entry.entryId(), entry.lastAddConfirmed(), entry.data()));
            lastAddConfirmed = Math.max(lastAddConfirmed, entry.lastAddConfirmed());
        }

        /** BookieImpl.fenceLedger (l.1153): getHandle(create) then setFenced; the future is always true. */
        boolean fenceLedger() {
            ledgerExists = true; // PORT: handles.getHandle(ledgerId, masterKey, false) creates the ledger
            fenced = true;       // PORT: no journal: fenceAndLogInJournal completes at once with true
            return true;
        }

        /** BookieImpl.readEntry (l.1159). */
        StoredEntry readEntry(long entryId) throws BookieException {
            if (!ledgerExists) {
                throw new BookieException("NoLedgerException"); // getReadOnlyHandle
            }
            // PORT: LedgerStorage.getEntry (interface only): LAST_ADD_CONFIRMED reads the entry with the
            // highest id stored; a missing entry is NoEntryException.
            StoredEntry entry;
            if (entryId == LAST_ADD_CONFIRMED) {
                entry = entries.isEmpty() ? null : entries.lastEntry().getValue();
            } else {
                entry = entries.get(entryId);
            }
            if (entry == null) {
                throw new BookieException("NoEntryException");
            }
            return entry;
        }

        /** BookieImpl.readLastAddConfirmed (l.1187). */
        long readLastAddConfirmed() throws BookieException {
            if (!ledgerExists) {
                throw new BookieException("NoLedgerException");
            }
            return lastAddConfirmed;
        }

        /** ReadEntryProcessorV3.getReadResponse (l.194) / readEntry (l.165) / sendFenceResponse. */
        void readEntryProcessorV3(ReadRequest readRequest) {
            Tid client = readRequest.from();
            long entryId = readRequest.entryId();
            boolean fenceResult = false;
            boolean isFenceRequest = readRequest.doFencing();
            ReadResponse resp;
            try {
                // handle fence request
                if (isFenceRequest) {
                    // PORT: no master key check (always present)
                    fenceResult = fenceLedger();
                }
                StoredEntry entryBody = readEntry(entryId);
                if (isFenceRequest) {
                    // handleReadResultForFenceRead / getFenceResponse: EOK with the body when fenced.
                    resp = fenceResult
                            ? new ReadResponse(bookieIndex, entryId, StatusCode.EOK, entryBody.entryId(),
                                               entryBody.lac(), entryBody.data(), -1)
                            : new ReadResponse(bookieIndex, entryId, StatusCode.ENOLEDGER, -1, -1, -1, -1); // PORT: EIO unreachable
                } else {
                    long knownLAC = readLastAddConfirmed();
                    resp = new ReadResponse(bookieIndex, entryId, StatusCode.EOK, entryBody.entryId(),
                                            entryBody.lac(), entryBody.data(), knownLAC);
                }
            } catch (BookieException ex) {
                StatusCode code = ex.code.equals("NoLedgerException") ? StatusCode.ENOLEDGER : StatusCode.ENOENTRY;
                resp = new ReadResponse(bookieIndex, entryId, code, -1, -1, -1, -1);
            }
            send(client, resp);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Ledger handles w1h and w2h: LedgerHandle, PendingAddOp, LedgerRecoveryOp, ReadLastConfirmedOp,
    // PendingReadOp.SequenceReadRequest.
    // ------------------------------------------------------------------------------------------
    enum HandleState { OPEN, CLOSED }

    interface AddCallback {
        void addComplete(int rc, long entryId, PendingAddOp op);
    }

    /** MetadataUpdateLoop: needsTransformation threw (BKMetadataVersionException). */
    static final class MetadataConflict extends Exception {
        MetadataConflict() {
            super("BKMetadataVersionException", null, false, false);
        }
    }

    final class LedgerHandle {
        final Client self;
        Setup addr;
        Tid client;
        LedgerMetadata metadata;
        long metadataVersion;
        HandleState handleState = HandleState.OPEN;
        long lastAddPushed = INVALID_ENTRY_ID;
        long lastAddConfirmed = INVALID_ENTRY_ID;
        long pendingAddsSequenceHead = INVALID_ENTRY_ID;
        final ArrayDeque<PendingAddOp> pendingAddOps = new ArrayDeque<>();
        boolean changingEnsemble = false;
        /** LedgerHandle.delayedWriteFailedBookies (bookie index -> bookie; the bookie is named by its index). */
        final TreeMap<Integer, Integer> delayedWriteFailedBookies = new TreeMap<>();
        /** This handle's address (from Setup): requests carry it as the reply address. */
        Tid selfTid;
        /**
         * PORT: the ordered executor (D10). A task is kept here under a sequence number and appended to the
         * executor by an ExecutorTask message to self; the handle runs it when it receives that message.
         */
        final TreeMap<Long, Runnable> tasks = new TreeMap<>();
        long nextTaskSeq = 0;
        /** The sequence number of the executor's next task: tasks run in append order in both models (D10). */
        long nextTaskToRun = 0;
        /**
         * PORT: the bookie client's per-request callback registry (PerChannelBookieClient keeps the op as the
         * request's callback until its response arrives): every add op by entry id, every recovery read by entry
         * id, so that a response arriving after its op completed or was drained still reaches that op (D11).
         */
        final TreeMap<Long, PendingAddOp> addOps = new TreeMap<>();
        final TreeMap<Long, RecoveryReadOp> readOps = new TreeMap<>();
        /** PORT: the metadata store calls in flight, answered through the same receive loop (D3). */
        MetadataUpdateLoop metadataLoop;
        java.util.function.Consumer<MetaValue> pendingRead;
        // w2h: the recovery client's open-recover-close, as a state machine (D4).
        boolean recovering = false;
        LedgerRecoveryOp recoveryOp;

        LedgerHandle(Client self) {
            this.self = self;
        }

        /** LedgerHandle.executeOrdered (l.2538): append to the ordered executor = send self an ExecutorTask. */
        void executeOrdered(String kind, Runnable r) {
            long seq = nextTaskSeq++;
            tasks.put(seq, r);
            send(selfTid, new ExecutorTask(seq, kind));
        }

        void run() {
            addr = awaitSetup(BookKeeperPort.this);
            client = self == Client.W1H ? addr.w1() : addr.w2();
            selfTid = self == Client.W1H ? addr.w1h() : addr.w2h();
            if (self == Client.W1H) {
                // PORT: w1 created the ledger before the study starts: its handle holds version 0, OPEN.
                metadata = new LedgerMetadata(State.OPEN, INVALID_ENTRY_ID);
                metadataVersion = 0;
            }
            // One receive loop per handle (D10, D11). It receives the client's request (Add for w1h; Recover
            // for an idle w2h), handled at once on the receiving thread as in the source (asyncAddEntry runs on
            // the caller's thread); every bookie response, whose callback is appended to the ordered executor
            // (PerChannelBookieClient l.1399-1404, l.1530-1555); every zk answer, handled at once (the metadata
            // client's own thread); and its own ExecutorTask messages, each of which runs the next executor
            // task. The handle never stops receiving.
            while (true) {
                final boolean acceptRecover = self == Client.W2H && !recovering;
                final boolean acceptAdd = self == Client.W1H;
                final long taskToRun = nextTaskToRun;
                MpMessage m = recv(x -> (acceptAdd && x instanceof Add)
                        || (acceptRecover && x instanceof Recover)
                        || x instanceof AddResponse || x instanceof ReadResponse
                        || x instanceof MetaWritten || x instanceof MetaBadVersion || x instanceof MetaValue
                        || (x instanceof ExecutorTask t && t.seq() == taskToRun));
                if (m instanceof Add a) {
                    // LedgerHandle.asyncAddEntry -> doAsyncAddEntry
                    PendingAddOp op = new PendingAddOp(this, (rc, entryId, o) -> {
                        // the application's AddCallback: reply to w1 with the entry number it asked for
                        send(client, rc == OK ? new Acked(o.ctx) : new Rejected(o.ctx));
                    }, a.entry(), a.entry());
                    doAsyncAddEntry(op);
                } else if (m instanceof Recover) {
                    recover();
                } else if (m instanceof AddResponse r) {
                    executeOrdered("AddResponse", () -> dispatchAddResponse(r));
                } else if (m instanceof ReadResponse r) {
                    executeOrdered("ReadResponse", () -> dispatchReadResponse(r));
                } else if (m instanceof MetaWritten w) {
                    // The metadata client's completions run on its own thread, not on the handle's ordered
                    // executor (MetadataUpdateLoop, under the handle's lock): run at once, atomically (D10).
                    metadataLoop.onWritten(w.version());
                } else if (m instanceof MetaBadVersion) {
                    metadataLoop.onBadVersion();
                } else if (m instanceof MetaValue v) {
                    java.util.function.Consumer<MetaValue> k = pendingRead;
                    pendingRead = null;
                    k.accept(v);
                } else if (m instanceof ExecutorTask t) {
                    nextTaskToRun++;
                    tasks.remove(t.seq()).run();
                }
            }
        }

        /** PerChannelBookieClient: the add response goes to the op that sent the request (completed or not). */
        void dispatchAddResponse(AddResponse r) {
            PendingAddOp op = addOps.get(r.entryId());
            if (op == null) {
                throw new AssertionError("add response for an unknown op " + r);
            }
            int rc = r.status() == StatusCode.EOK ? OK
                    : r.status() == StatusCode.EFENCED ? LedgerFencedException : ReadException;
            op.writeComplete(rc, r.bookieIndex());
        }

        /** PerChannelBookieClient: the read response goes to the op that sent the request (completed or not). */
        void dispatchReadResponse(ReadResponse r) {
            if (r.entryId() == LAST_ADD_CONFIRMED) {
                recoveryOp.rlcop.readEntryComplete(rcOf(r.status()), r.bookieIndex(), r.bodyLac());
            } else {
                RecoveryReadOp op = readOps.get(r.entryId());
                if (op == null) {
                    throw new AssertionError("read response for an unknown op " + r);
                }
                op.readEntryComplete(rcOf(r.status()), r.bookieIndex(), r.data());
            }
        }

        /** LedgerHandle.isHandleWritable (l.550). */
        boolean isHandleWritable() {
            return !metadata.isClosed() && handleState == HandleState.OPEN;
        }

        /** LedgerHandle.doAsyncAddEntry (l.1716). */
        void doAsyncAddEntry(PendingAddOp op) {
            boolean wasClosed = false;
            if (isHandleWritable()) {
                long entryId = ++lastAddPushed;
                op.setEntryId(entryId);
                pendingAddOps.add(op);
                addOps.put(entryId, op); // PORT: the bookie client's callback registry (D11)
            } else {
                wasClosed = true;
            }
            if (wasClosed) {
                // make sure the callback is triggered in main worker pool
                executeOrdered("AddToClosedLedger", () -> op.cb.addComplete(LedgerClosedException, INVALID_ENTRY_ID, op));
                return;
            }
            // PORT: waitForWriteSetMs < 0 (no channel writability wait)
            op.initiate();
        }

        /** LedgerHandle.handleUnrecoverableErrorDuringAdd (l.2180). */
        void handleUnrecoverableErrorDuringAdd(int rc) {
            if (metadata.state() == State.IN_RECOVERY) {
                // we should not close ledger if ledger is recovery mode
                errorOutPendingAdds(rc);
                return;
            }
            asyncCloseInternal(rc);
        }

        /** LedgerHandle.errorOutPendingAdds (l.2205). */
        void errorOutPendingAdds(int rc) {
            errorOutPendingAdds(rc, drainPendingAddsAndAdjustLength());
        }

        List<PendingAddOp> drainPendingAddsAndAdjustLength() {
            List<PendingAddOp> opsDrained = new ArrayList<>(pendingAddOps);
            pendingAddOps.clear(); // PORT: no length
            return opsDrained;
        }

        void errorOutPendingAdds(int rc, List<PendingAddOp> ops) {
            for (PendingAddOp op : ops) {
                op.submitCallback(rc);
            }
        }

        /** LedgerHandle.sendAddSuccessCallbacks (l.2225). */
        void sendAddSuccessCallbacks() {
            PendingAddOp pendingAddOp;
            while ((pendingAddOp = pendingAddOps.peek()) != null && !changingEnsemble) {
                if (!pendingAddOp.completed) {
                    return;
                }
                // Check if it is the next entry in the sequence.
                if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != pendingAddsSequenceHead + 1) {
                    return;
                }
                pendingAddOps.remove();
                pendingAddsSequenceHead = pendingAddOp.entryId;
                this.lastAddConfirmed = pendingAddsSequenceHead; // no DEFERRED_SYNC
                pendingAddOp.submitCallback(OK);
            }
        }

        /** LedgerHandle.notifyWriteFailed (l.2266). */
        void notifyWriteFailed(int index) {
            delayedWriteFailedBookies.put(index, index);
        }

        /** LedgerHandle.maybeHandleDelayedWriteBookieFailure (l.2272). */
        void maybeHandleDelayedWriteBookieFailure() {
            if (delayedWriteFailedBookies.isEmpty()) {
                return;
            }
            TreeMap<Integer, Integer> toReplace = new TreeMap<>(delayedWriteFailedBookies);
            delayedWriteFailedBookies.clear();
            if (toReplace.isEmpty()) {
                return;
            }
            handleBookieFailure(toReplace);
        }

        /** LedgerHandle.handleBookieFailure (l.2293). */
        void handleBookieFailure(TreeMap<Integer, Integer> failedBookies) {
            // disableEnsembleChangeFeature is off; no DEFERRED_SYNC write flag
            boolean triggerLoop = false;
            TreeMap<Integer, Integer> toReplace = null;
            if (changingEnsemble) {
                delayedWriteFailedBookies.putAll(failedBookies);
            } else {
                changingEnsemble = true;
                triggerLoop = true;
                toReplace = new TreeMap<>(delayedWriteFailedBookies);
                delayedWriteFailedBookies.clear();
                toReplace.putAll(failedBookies);
            }
            if (triggerLoop) {
                ensembleChangeLoop(toReplace);
            }
        }

        /**
         * LedgerHandle.ensembleChangeLoop (l.2339). PORT: the ensemble change itself is replaced (D1): there is
         * no spare bookie. The MetadataUpdateLoop is evaluated on the local copy: if it is OPEN (and the failed
         * bookie is in the ensemble) the transform, EnsembleUtils.replaceBookiesInEnsemble, throws
         * BKNotEnoughBookiesException before any write, so the loop completes exceptionally; if it is CLOSED or
         * IN_RECOVERY the predicate is false and the loop returns it. Its whenCompleteAsync callback
         * (l.2397-2420) then runs on the ordered executor: handleUnrecoverableErrorDuringAdd with WriteException,
         * LedgerClosedException or LedgerFencedException. changingEnsemble stays set (it is reset only after a
         * successful change, l.2442).
         */
        void ensembleChangeLoop(TreeMap<Integer, Integer> failedBookies) {
            final LedgerMetadata md = metadata;
            executeOrdered("EnsembleChangeFailed", () -> {
                if (md.state() == State.OPEN) {
                    handleUnrecoverableErrorDuringAdd(WriteException);
                } else if (md.isClosed()) {
                    handleUnrecoverableErrorDuringAdd(LedgerClosedException);
                } else {
                    handleUnrecoverableErrorDuringAdd(LedgerFencedException);
                }
            });
        }

        /** LedgerHandle.asyncCloseInternal (l.554). */
        void asyncCloseInternal(int rc) {
            doAsyncCloseInternal(rc);
        }

        /** LedgerHandle.doAsyncCloseInternal (l.576). */
        void doAsyncCloseInternal(int rc) {
            executeOrdered("Close", () -> {
                final HandleState prevHandleState;
                final List<PendingAddOp> pendingAdds;
                final long lastEntry;
                prevHandleState = handleState;
                // drain pending adds first
                pendingAdds = drainPendingAddsAndAdjustLength();
                lastEntry = lastAddPushed = this.lastAddConfirmed;
                handleState = HandleState.CLOSED;
                // error out all pending adds during closing
                errorOutPendingAdds(rc, pendingAdds);
                if (prevHandleState != HandleState.CLOSED) {
                    // new MetadataUpdateLoop(...).run().whenComplete(...): started, not awaited (D3); the
                    // handle keeps receiving (later Adds are rejected by doAsyncAddEntry meanwhile).
                    new MetadataUpdateLoop(
                            md -> {
                                if (md.isClosed()) {
                                    if (lastEntry == md.lastEntryId()) { // PORT: no length
                                        return false;
                                    } else {
                                        throw new MetadataConflict();
                                    }
                                } else {
                                    return true;
                                }
                            },
                            md -> new LedgerMetadata(State.CLOSED, lastEntry),
                            (md, conflict) -> {
                                // closePromise completes (exceptionally on a conflict); NoopCloseCallback
                            }).run();
                }
            });
        }

        void readLedgerMetadata(java.util.function.Consumer<MetaValue> k) {
            send(addr.zk(), new ReadMeta(selfTid));
            pendingRead = k;
        }

        /** LedgerHandle.setLedgerMetadata (l.400-415). PORT: no expected-object check (one writer per handle
         *  process, D3), no length. */
        void setLedgerMetadata(LedgerMetadata md, long version) {
            this.metadata = md;
            this.metadataVersion = version;
            if (md.isClosed()) {
                lastAddConfirmed = lastAddPushed = md.lastEntryId();
            }
        }

        interface NeedsTransformation {
            boolean test(LedgerMetadata md) throws MetadataConflict;
        }

        interface UpdateDone {
            void done(LedgerMetadata md, boolean conflict);
        }

        /**
         * PORT: MetadataUpdateLoop is not in the cut (D3). run(): if the local copy needs no transformation,
         * complete with it; else write the transformed copy conditioned on the local version. Written: the
         * local copy becomes it, complete. Bad version: re-read zk into the local copy, then run() again. A
         * predicate that throws completes the loop exceptionally. Each zk answer arrives through the handle's
         * receive loop (onWritten, onBadVersion, the pendingRead continuation).
         */
        final class MetadataUpdateLoop {
            final NeedsTransformation needsTransformation;
            final UnaryOperator<LedgerMetadata> transform;
            final UpdateDone whenComplete;
            LedgerMetadata transformed;

            MetadataUpdateLoop(NeedsTransformation needsTransformation, UnaryOperator<LedgerMetadata> transform,
                               UpdateDone whenComplete) {
                this.needsTransformation = needsTransformation;
                this.transform = transform;
                this.whenComplete = whenComplete;
            }

            void run() {
                boolean needs;
                try {
                    needs = needsTransformation.test(metadata);
                } catch (MetadataConflict ex) {
                    finish(metadata, true);
                    return;
                }
                if (!needs) {
                    finish(metadata, false);
                    return;
                }
                transformed = transform.apply(metadata);
                metadataLoop = this;
                send(addr.zk(), new WriteMeta(selfTid, transformed, metadataVersion));
            }

            void onWritten(long version) {
                metadataLoop = null;
                setLedgerMetadata(transformed, version);
                finish(metadata, false);
            }

            void onBadVersion() {
                metadataLoop = null;
                readLedgerMetadata(v -> {
                    setLedgerMetadata(v.metadata(), v.version());
                    run();
                });
            }

            void finish(LedgerMetadata md, boolean conflict) {
                whenComplete.done(md, conflict);
            }
        }

        /** LedgerHandle.asyncRecoveryAddEntry (l.1621). */
        void asyncRecoveryAddEntry(long data, AddCallback cb) {
            PendingAddOp op = new PendingAddOp(this, cb, data, -1).enableRecoveryAdd();
            doAsyncAddEntry(op);
        }

        // ---- w2h: the recovery client's handle -----------------------------------------------
        /**
         * PORT: BookKeeper.asyncOpenLedger(recovery) / LedgerOpenOp / ReadOnlyLedgerHandle.recover are not in
         * the cut (D4): open = read the metadata from zk; mark IN_RECOVERY; LedgerRecoveryOp; closeRecovered;
         * reply Closed. Each step continues from the receive loop.
         */
        void recover() {
            recovering = true;
            readLedgerMetadata(v -> {
                setLedgerMetadata(v.metadata(), v.version());
                new MetadataUpdateLoop(m -> m.state() == State.OPEN,
                                       m -> new LedgerMetadata(State.IN_RECOVERY, m.lastEntryId()),
                                       (md, conflict) -> {
                                           if (md.isClosed()) {
                                               recoveryComplete(md);
                                           } else {
                                               recoveryOp = new LedgerRecoveryOp(this);
                                               recoveryOp.initiate();
                                           }
                                       }).run();
            });
        }

        /** The LedgerRecoveryOp promise completed (LedgerRecoveryOp.submitCallback). */
        void onRecoveryPromise(int rc) {
            if (rc != OK) {
                throw new AssertionError("recovery failed with rc " + rc);
            }
            closeRecovered();
        }

        /** PORT: ReadOnlyLedgerHandle.closeRecovered (not in the cut, D4). */
        void closeRecovered() {
            final long lac = lastAddConfirmed;
            new MetadataUpdateLoop(m -> m.state() == State.IN_RECOVERY,
                                   m -> new LedgerMetadata(State.CLOSED, lac),
                                   (md, conflict) -> recoveryComplete(md)).run();
        }

        void recoveryComplete(LedgerMetadata md) {
            send(client, new Closed(number(md.lastEntryId())));
            recovering = false;
        }

        int rcOf(StatusCode s) {
            return switch (s) {
                case EOK -> OK;
                case ENOENTRY -> NoSuchEntryException;
                case ENOLEDGER -> NoSuchLedgerExistsException;
                case EFENCED -> LedgerFencedException;
            };
        }
    }

    /** RoundRobinDistributionSchedule.getWriteSetBookieIndex (l.58). */
    static int getWriteSetBookieIndex(long entryId, int writeSetIndex) {
        return (int) (entryId + writeSetIndex) % ENSEMBLE_SIZE;
    }

    Tid bookie(Setup addr, int bookieIndex) {
        return switch (bookieIndex) {
            case 0 -> addr.b1();
            case 1 -> addr.b2();
            default -> addr.b3();
        };
    }

    // ------------------------------------------------------------------------------------------
    // PendingAddOp.
    // ------------------------------------------------------------------------------------------
    final class PendingAddOp {
        final LedgerHandle lh;
        final AddCallback cb;
        final long data;
        final long ctx; // the client's entry number (w1) or -1 (recovery)
        long entryId;
        boolean isRecoveryAdd = false;
        boolean completed = false;
        boolean callbackTriggered = false;
        int pendingWriteRequests = 0;
        final boolean[] ackSet = new boolean[ENSEMBLE_SIZE]; // AckSetImpl
        long toSendLac;

        PendingAddOp(LedgerHandle lh, AddCallback cb, long data, long ctx) {
            this.lh = lh;
            this.cb = cb;
            this.data = data;
            this.ctx = ctx;
        }

        PendingAddOp enableRecoveryAdd() {
            isRecoveryAdd = true;
            return this;
        }

        void setEntryId(long entryId) {
            this.entryId = entryId;
        }

        /** PendingAddOp.sendWriteRequest (l.146). */
        private void sendWriteRequest(int bookieIndex) {
            send(bookie(lh.addr, bookieIndex), new AddRequest(lh.selfTid, entryId, toSendLac, data, isRecoveryAdd));
            ++pendingWriteRequests;
        }

        /** PendingAddOp.initiate (l.246). */
        void initiate() {
            if (callbackTriggered) {
                return;
            }
            // computeDigestAndPackageForSending(entryId, lh.lastAddConfirmed, ...): the piggybacked LAC.
            this.toSendLac = lh.lastAddConfirmed;
            // We are about to send. Check if we need to make an ensemble change because of delayed write errors
            lh.maybeHandleDelayedWriteBookieFailure();
            for (int i = 0; i < WRITE_QUORUM_SIZE; i++) {
                sendWriteRequest(getWriteSetBookieIndex(entryId, i));
            }
        }

        /** AckSetImpl.completeBookieAndCheck. */
        boolean completeBookieAndCheck(int bookieIndex) {
            ackSet[bookieIndex] = true;
            int c = 0;
            for (boolean b : ackSet) {
                if (b) {
                    c++;
                }
            }
            return c >= ACK_QUORUM_SIZE;
        }

        /** PendingAddOp.writeComplete (l.278). */
        void writeComplete(int rc, int bookieIndex) {
            --pendingWriteRequests;
            // PORT: the ensemble never changes: the "ensemble has already changed" test is always false
            boolean ackQuorum = false;
            if (OK == rc) {
                ackQuorum = completeBookieAndCheck(bookieIndex);
            }
            if (completed) {
                // A late response after the ack quorum (D11). It can be an EFENCED: b3 fenced by the LAC read,
                // entry 0 EOK at b1 and b2 (Acked(1)), then b3's EFENCED.
                if (rc != OK) {
                    // Got an error after satisfying AQ: under replicated; a delayed ensemble change, consumed
                    // by the next initiate (maybeHandleDelayedWriteBookieFailure).
                    if (!DISABLE_ENSEMBLE_CHANGE_FEATURE && !DELAY_ENSEMBLE_CHANGE) {
                        lh.notifyWriteFailed(bookieIndex);
                    }
                }
                sendAddSuccessCallbacks();
                // I am already finished, ignore incoming responses.
                return;
            }
            switch (rc) {
                case OK:
                    break;
                case LedgerFencedException:
                    lh.handleUnrecoverableErrorDuringAdd(rc);
                    return;
                default:
                    // PORT: no other error can occur (no bookie failure, no ensemble change)
                    throw new AssertionError("unexpected rc " + rc);
            }
            if (ackQuorum && !completed) {
                // PORT: enforceMinNumFaultDomainsForWrite is false
                completed = true;
                sendAddSuccessCallbacks();
            }
        }

        void sendAddSuccessCallbacks() {
            lh.sendAddSuccessCallbacks();
        }

        /** PendingAddOp.submitCallback (l.432). */
        void submitCallback(int rc) {
            cb.addComplete(rc, entryId, this);
            callbackTriggered = true;
        }
    }

    // ------------------------------------------------------------------------------------------
    // LedgerRecoveryOp.
    // ------------------------------------------------------------------------------------------
    final class LedgerRecoveryOp implements AddCallback {
        final LedgerHandle lh;
        long readCount = 0;
        long writeCount = 0;
        boolean readDone = false;
        long startEntryToRead;
        long endEntryToRead;
        boolean promiseDone = false;
        int promiseRc = OK;
        ReadLastConfirmedOp rlcop;

        LedgerRecoveryOp(LedgerHandle lh) {
            this.lh = lh;
        }

        /** LedgerRecoveryOp.initiate (l.92). */
        void initiate() {
            rlcop = new ReadLastConfirmedOp(lh, (rc, lastAddConfirmedRead) -> {
                if (rc == OK) {
                    // lastEnsembleEntryId: the one ensemble starts at entry 0
                    long lastEnsembleEntryId = 0;
                    lh.lastAddPushed = lh.lastAddConfirmed = Math.max(lastAddConfirmedRead, (lastEnsembleEntryId - 1));
                    lh.pendingAddsSequenceHead = lh.lastAddConfirmed;
                    startEntryToRead = endEntryToRead = lh.lastAddConfirmed;
                    // metadataForRecovery = lh.getLedgerMetadata(): fixed ensemble, nothing to keep
                    doRecoveryRead();
                } else {
                    submitCallback(ReadException);
                }
            });
            rlcop.initiateWithFencing();
        }

        /** LedgerRecoveryOp.submitCallback (l.151). */
        void submitCallback(int rc) {
            // promise.complete / completeExceptionally: completes once.
            if (promiseDone) {
                return;
            }
            promiseDone = true;
            promiseRc = rc;
            // PORT: the promise's continuation (ReadOnlyLedgerHandle.recover, not in the cut, D4) runs next
            // on the handle's ordered executor.
            lh.executeOrdered("RecoveryPromise", () -> lh.onRecoveryPromise(rc));
        }

        /** LedgerRecoveryOp.doRecoveryRead (l.166). */
        void doRecoveryRead() {
            if (!promiseDone) {
                startEntryToRead = endEntryToRead + 1;
                endEntryToRead = endEntryToRead + RECOVERY_READ_BATCH_SIZE;
                RecoveryReadOp op = new RecoveryReadOp(lh, startEntryToRead, endEntryToRead, this);
                lh.readOps.put(startEntryToRead, op); // PORT: the bookie client's callback registry (D11)
                op.initiate();
            }
        }

        /** LedgerRecoveryOp.onEntryComplete (l.176); entry is null unless rc == OK. */
        void onEntryComplete(int rc, long entryId, long data) {
            if (!promiseDone && !readDone && rc == OK) {
                readCount++;
                // check whether entry id is expected, so we won't overwritten any entries by mistake
                if (entryId != lh.lastAddPushed + 1) {
                    rc = UnexpectedConditionException;
                }
                if (OK == rc) {
                    lh.asyncRecoveryAddEntry(data, this);
                    if (entryId == endEntryToRead) {
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
            if (OK != rc && !promiseDone) {
                submitCallback(rc);
            }
            // else: read an entry but readDone was already set (log only)
        }

        /** LedgerRecoveryOp.addComplete (l.246). */
        @Override
        public void addComplete(int rc, long entryId, PendingAddOp op) {
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

    // ------------------------------------------------------------------------------------------
    // ReadLastConfirmedOp with RRQuorumCoverageSet.
    // ------------------------------------------------------------------------------------------
    interface LastConfirmedDataCallback {
        void readLastConfirmedDataComplete(int rc, long lastAddConfirmed);
    }

    final class ReadLastConfirmedOp {
        final LedgerHandle lh;
        final LastConfirmedDataCallback cb;
        int numResponsesPending = ENSEMBLE_SIZE;
        long maxRecoveredData = INVALID_ENTRY_ID; // RecoveryData(INVALID_ENTRY_ID, 0).lastAddConfirmed
        boolean completed = false;
        int lastSeenError = ReadException;
        final int[] covered = {UNINITIALIZED, UNINITIALIZED, UNINITIALIZED}; // RRQuorumCoverageSet

        ReadLastConfirmedOp(LedgerHandle lh, LastConfirmedDataCallback cb) {
            this.lh = lh;
            this.cb = cb;
        }

        /** ReadLastConfirmedOp.initiateWithFencing (l.103). */
        void initiateWithFencing() {
            for (int i = 0; i < ENSEMBLE_SIZE; i++) {
                send(bookie(lh.addr, i), new ReadRequest(lh.selfTid, LAST_ADD_CONFIRMED, true));
            }
        }

        /** RRQuorumCoverageSet.checkCovered (RoundRobinDistributionSchedule l.378). */
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

        /** ReadLastConfirmedOp.readEntryComplete (l.114); lac is the LAC in the entry body (OK only). */
        void readEntryComplete(int rc, int bookieIndex, long lac) {
            covered[bookieIndex] = rc; // coverageSet.addBookie
            numResponsesPending--;
            boolean heardValidResponse = false;
            if (rc == OK) {
                // verifyDigestAndReturnLastConfirmed: PORT: no digest, never a mismatch
                if (lac > maxRecoveredData) {
                    maxRecoveredData = lac;
                }
                heardValidResponse = true;
            }
            if (rc == NoSuchLedgerExistsException || rc == NoSuchEntryException) {
                heardValidResponse = true;
            }
            if (!heardValidResponse && OK != rc) {
                lastSeenError = rc;
            }
            if (heardValidResponse && checkCovered() && !completed) {
                completed = true;
                cb.readLastConfirmedDataComplete(OK, maxRecoveredData);
                return;
            }
            if (numResponsesPending == 0 && !completed) {
                cb.readLastConfirmedDataComplete(lastSeenError, maxRecoveredData);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // RecoveryReadOp = ListenerBasedPendingReadOp(isRecoveryRead) with one SequenceReadRequest.
    // ------------------------------------------------------------------------------------------
    final class RecoveryReadOp {
        final LedgerHandle lh;
        final LedgerRecoveryOp listener;
        final SequenceReadRequest entry; // seq has one request: batch size 1
        boolean callbackDone = false;

        RecoveryReadOp(LedgerHandle lh, long startEntryId, long endEntryId, LedgerRecoveryOp listener) {
            this.lh = lh;
            this.listener = listener;
            if (startEntryId != endEntryId) {
                throw new AssertionError("batch size 1"); // PORT: recoveryReadBatchSize = 1
            }
            this.entry = new SequenceReadRequest(startEntryId);
        }

        /** PendingReadOp.initiate (l.65): no speculative reads. */
        void initiate() {
            entry.read();
        }

        /** PendingReadOp.readEntryComplete (l.95). */
        void readEntryComplete(int rc, int bookieIndex, long data) {
            if (rc != OK) {
                entry.logErrorAndReattemptRead(bookieIndex, rc);
                return;
            }
            if (entry.complete(data)) {
                // recovery read: do not advance LastAddConfirmed
                submitCallback(OK);
            }
        }

        /** ListenerBasedPendingReadOp.submitCallback (l.54): one request in seq. */
        void submitCallback(int code) {
            if (!entry.isComplete() || callbackDone) {
                return;
            }
            callbackDone = true; // seq.removeFirst()
            listener.onEntryComplete(entry.rc, entry.eId, entry.data);
        }

        /** PendingReadOp.sendReadTo (l.176): recovery reads carry FLAG_DO_FENCING at main (l.181-182). */
        void sendReadTo(int bookieIndex) {
            send(bookie(lh.addr, bookieIndex), new ReadRequest(lh.selfTid, entry.eId, true));
        }

        /** PendingReadOp.SequenceReadRequest (l.295) with ReadOpBase.LedgerEntryRequest. */
        final class SequenceReadRequest {
            final long eId;
            final int[] writeSet = new int[WRITE_QUORUM_SIZE];
            int nextReplicaIndexToReadFrom = 0;
            final boolean[] sentReplicas = new boolean[WRITE_QUORUM_SIZE];
            final boolean[] erroredReplicas = new boolean[WRITE_QUORUM_SIZE];
            // LedgerEntryRequest (ReadOpBase, not in the cut): PORT, decision D6.
            boolean complete = false;
            int rc = OK;
            int firstError = OK;
            int numBookiesMissingEntry = 0;
            final int requiredBookiesMissingEntryForRecovery = WRITE_QUORUM_SIZE - ACK_QUORUM_SIZE + 1;
            long data = -1;

            SequenceReadRequest(long eId) {
                this.eId = eId;
                for (int w = 0; w < WRITE_QUORUM_SIZE; w++) {
                    writeSet[w] = getWriteSetBookieIndex(eId, w); // PORT: no reorderReadSequence
                }
            }

            boolean isComplete() {
                return complete;
            }

            boolean readsOutstanding() {
                int c = 0;
                for (int i = 0; i < WRITE_QUORUM_SIZE; i++) {
                    if (sentReplicas[i]) {
                        c++;
                    }
                    if (erroredReplicas[i]) {
                        c--;
                    }
                }
                return c > 0;
            }

            void read() {
                sendNextRead();
            }

            /** SequenceReadRequest.sendNextRead (l.358). */
            void sendNextRead() {
                if (nextReplicaIndexToReadFrom >= WRITE_QUORUM_SIZE) {
                    // we are done, the read has failed from all replicas, just fail the read
                    fail(firstError);
                    return;
                }
                int replica = nextReplicaIndexToReadFrom;
                int bookieIndex = writeSet[nextReplicaIndexToReadFrom];
                nextReplicaIndexToReadFrom++;
                sendReadTo(bookieIndex);
                sentReplicas[replica] = true;
            }

            /** ReadOpBase.LedgerEntryRequest.fail (not in the cut). */
            void fail(int code) {
                if (!complete) {
                    complete = true;
                    rc = code;
                    submitCallback(code);
                }
            }

            /** SequenceReadRequest.logErrorAndReattemptRead (l.391). */
            void logErrorAndReattemptRead(int bookieIndex, int rcIn) {
                if (isComplete()) {
                    return;
                }
                // super.logErrorAndReattemptRead (ReadOpBase, not in the cut): first error, missing count.
                if (firstError == OK || firstError == NoSuchEntryException
                        || firstError == NoSuchLedgerExistsException) {
                    firstError = rcIn;
                }
                if (rcIn == NoSuchEntryException || rcIn == NoSuchLedgerExistsException) {
                    ++numBookiesMissingEntry;
                }
                int replica = -1;
                for (int i = 0; i < WRITE_QUORUM_SIZE; i++) {
                    if (writeSet[i] == bookieIndex) {
                        replica = i;
                    }
                }
                if (replica == -1) {
                    return;
                }
                erroredReplicas[replica] = true;
                if (numBookiesMissingEntry >= requiredBookiesMissingEntryForRecovery) {
                    // For recovery, report NoSuchEntry as soon as wQ-aQ+1 bookies report that they do not have it
                    fail(NoSuchEntryException);
                    return;
                }
                if (!readsOutstanding()) {
                    sendNextRead();
                }
            }

            /** SingleLedgerEntryRequest.complete (l.204) via SequenceReadRequest.complete (l.424). */
            boolean complete(long body) {
                if (isComplete()) {
                    return false;
                }
                // verifyDigestAndReturnData: PORT: no digest
                complete = true;
                rc = OK;
                data = body;
                return true;
            }
        }
    }

    @Override
    public String toString() {
        return "BookKeeperPort(e=" + e + ", " + model + ")";
    }
}
