package bench.bookkeeper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/**
 * Apache BookKeeper (main at 7b66b5d), ledger replication with fencing: one ledger, one writer (w1, its handle w1h),
 * one recovery client (w2, its handle w2h), three bookies (b1, b2, b3) and the metadata store (zk), transliterated
 * to JMC's message-passing API. Class and method names are BookKeeper's; every changed line carries {@code PORT:}.
 * Roles, messages, the method mapping and every decision: NOTES.md next to this tree.
 *
 * <p>Concurrency as logical tasks (the owner's contract): one process per PendingAddOp in flight, per recovery read,
 * for the fencing ReadLastConfirmedOp, per bookie (its request handling over its ledger storage), per handle (the
 * handle's state with its pending-add queue, which is also the handle's ordered callback thread), and zk. Dependencies
 * between them are messages.
 */
public final class BookKeeper implements MpProgram {

    // ------------------------------------------------------------------------------------------------------------
    // Instance parameters (INTERFACE.md)
    // ------------------------------------------------------------------------------------------------------------

    private final int e;
    private final String delivery;
    private final CommunicationModel model;

    /**
     * Constructed by the launcher from {@code impl,<e>,<fifo|bag>}.
     *
     * @param args the kind ({@code impl}), the number of entries w1 adds, the delivery model
     */
    public BookKeeper(String... args) {
        if (args.length != 3 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<e>,<fifo|bag>, got " + String.join(",", args));
        }
        this.e = Integer.parseInt(args[1]);
        if (e < 1) {
            throw new IllegalArgumentException("e must be at least 1, was " + e);
        }
        this.delivery = args[2];
        if (delivery.equals("fifo")) {
            this.model = CommunicationModel.P2P;
        } else if (delivery.equals("bag")) {
            this.model = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("delivery must be fifo or bag, was " + delivery);
        }
    }

    @Override
    public String programName() {
        return "bookkeeper";
    }

    @Override
    public String size() {
        return Integer.toString(e);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        // PORT: the internal processes are daemons (they never terminate), which carry no role.
        return List.of("w1", "w2");
    }

    @Override
    public List<String> visibleRoles() {
        return List.of("w1", "w2");
    }

    @Override
    public String toString() {
        return "BookKeeper(impl, e=" + e + ", " + delivery + ")";
    }

    // PORT: the fixed ensemble of the interface: ensemble size 3, write quorum 3, ack quorum 2, no spare bookie.
    static final int ENSEMBLE_SIZE = 3;
    static final int WRITE_QUORUM_SIZE = 3;
    static final int ACK_QUORUM_SIZE = 2;
    // PORT: one ledger.
    static final long LEDGER_ID = 1L;
    // PORT: payloads carry the client's entry number; every payload is one unit long (the ledger length counts entries).
    static final long PAYLOAD_LENGTH = 1L;
    // PORT: ClientConfiguration.recoveryReadBatchSize, fixed to 1 by the interface.
    static final int RECOVERY_READ_BATCH_SIZE = 1;

    /** LedgerHandle.INVALID_ENTRY_ID. */
    static final long INVALID_ENTRY_ID = -1L;

    /** BookieProtocol constants. */
    static final long LAST_ADD_CONFIRMED = -1L;
    static final int FLAG_NONE = 0x0;
    static final int FLAG_DO_FENCING = 0x0001;
    static final int FLAG_RECOVERY_ADD = 0x0002;
    static final int FLAG_HIGH_PRIORITY = 0x0004;

    /** BKException.Code, the values used here. */
    static final class Code {
        static final int OK = 0;
        static final int ReadException = -1;
        static final int NotEnoughBookiesException = -6;
        static final int NoSuchLedgerExistsException = -7;
        static final int LedgerRecoveryException = -10;
        static final int LedgerClosedException = -11;
        static final int WriteException = -12;
        static final int NoSuchEntryException = -13;
        static final int ProtocolVersionException = -16;
        static final int MetadataVersionException = -17;
        static final int ClientClosedException = -19;
        static final int TimeoutException = -23;
        static final int IllegalOpException = -100;
        static final int LedgerFencedException = -101;
        static final int UnauthorizedAccessException = -102;
        static final int WriteOnReadOnlyBookieException = -104;
        static final int UnexpectedConditionException = -999;
        static final int UNINITIALIZED = 1;

        private Code() {}
    }

    /** BookkeeperProtocol.StatusCode (the V3 wire status). */
    enum StatusCode { EOK, ENOLEDGER, ENOENTRY, EBADREQ, EIO, EUA, EBADVERSION, EFENCED, EREADONLY, ETOOMANYREQUESTS,
        EUNKNOWNLEDGERSTATE }

    /** LedgerMetadata.State. */
    enum State { OPEN, IN_RECOVERY, CLOSED }

    /**
     * LedgerMetadata. PORT: the ensemble is fixed (b1, b2, b3 from entry 0) and not stored; no password, digest type
     * or custom metadata.
     */
    record LedgerMetadata(State state, long lastEntryId, long length) {
        boolean isClosed() {
            return state == State.CLOSED;
        }

        State getState() {
            return state;
        }

        long getLastEntryId() {
            return lastEntryId;
        }

        long getLength() {
            return length;
        }
    }

    /** Versioned&lt;LedgerMetadata&gt;. PORT: LongVersion as a long. */
    record Versioned(LedgerMetadata value, long version) {
        LedgerMetadata getValue() {
            return value;
        }

        long getVersion() {
            return version;
        }
    }

    /**
     * The entry as packaged for sending (DigestManager.computeDigestAndPackageForSending): the header ledger id, entry
     * id, last add confirmed, ledger length, then the payload. PORT: no digest.
     */
    record EntryBody(long ledgerId, long entryId, long lastAddConfirmed, long length, long data) {}

    // ------------------------------------------------------------------------------------------------------------
    // Visible messages (INTERFACE.md, verbatim shapes)
    // ------------------------------------------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------------------------------------------
    // Invisible messages
    // ------------------------------------------------------------------------------------------------------------

    /** PORT: the client's address, sent once by init to its handle (the client's callback target). */
    record Setup(Tid client) implements MpMessage {}

    /** AddRequest (V3) with the client's channel as {@code replyTo}; {@code ctx} is the bookie index. */
    record AddRequest(Tid replyTo, long ledgerId, long entryId, int flags, EntryBody body, int ctx)
            implements MpMessage {}

    /** AddResponse (V3); {@code addr} is the answering bookie, {@code ctx} the bookie index of the request. */
    record AddResponse(StatusCode status, long ledgerId, long entryId, Tid addr, int ctx) implements MpMessage {}

    /** ReadRequest (V3): FENCE_LEDGER when {@code flags} has FLAG_DO_FENCING; the master key when fencing. */
    record ReadRequest(Tid replyTo, long ledgerId, long entryId, int flags, boolean hasMasterKey, int ctx)
            implements MpMessage {}

    /** ReadResponse (V3); {@code body} is null when the response has none; {@code maxLAC} when {@code hasMaxLAC}. */
    record ReadResponse(StatusCode status, long ledgerId, long entryId, EntryBody body, boolean hasMaxLAC,
                        long maxLAC, Tid addr, int ctx) implements MpMessage {}

    /** PendingAddOp.create(lh, ...): the handle hands the op its entry; the handle already sent its write requests. */
    record PendingAddOpCreate(Tid lh, long entryId, boolean isRecoveryAdd, int pendingWriteRequests)
            implements MpMessage {}

    /**
     * PORT: the op's process -> LedgerHandle: a bookie's AddResponse for entry {@code opEntryId}, queued on the
     * handle's ordered executor (PerChannelBookieClient queues writeComplete on lh.executor), one message per response.
     */
    record WriteCompleteCallback(long opEntryId, StatusCode status, long ledgerId, long entryId, Tid addr, int ctx)
            implements MpMessage {}

    /** new ReadLastConfirmedOp(...) by LedgerRecoveryOp.initiate, then initiateWithFencing(). */
    record ReadLastConfirmedOpCreate(Tid lh) implements MpMessage {}

    /** ReadLastConfirmedOp -> LedgerRecoveryOp: cb.readLastConfirmedDataComplete(rc, RecoveryData). */
    record ReadLastConfirmedDataComplete(int rc, long lastAddConfirmed, long length) implements MpMessage {}

    /** new RecoveryReadOp(lh, clientCtx, startEntryId, endEntryId, this, null).initiate(). */
    record RecoveryReadOpCreate(Tid lh, long startEntryId, long endEntryId) implements MpMessage {}

    /** RecoveryReadOp -> LedgerRecoveryOp: listener.onEntryComplete(rc, lh, entry, ctx); no entry when !hasEntry. */
    record OnEntryComplete(int rc, boolean hasEntry, long entryId, long length, long data) implements MpMessage {}


    /** LedgerManager.readLedgerMetadata(ledgerId). */
    record ReadLedgerMetadata(Tid replyTo, long ledgerId, int loopId) implements MpMessage {}

    /** Its completion; rc OK or an error. */
    record ReadLedgerMetadataResult(int rc, Versioned read, int loopId) implements MpMessage {}

    /** LedgerManager.writeLedgerMetadata(ledgerId, metadata, currentVersion). */
    record WriteLedgerMetadata(Tid replyTo, long ledgerId, LedgerMetadata metadata, long currentVersion, int loopId)
            implements MpMessage {}

    /** Its completion; rc OK, or MetadataVersionException (BKMetadataVersionException). */
    record WriteLedgerMetadataResult(int rc, Versioned written, int loopId) implements MpMessage {}

    // ------------------------------------------------------------------------------------------------------------
    // Program entry
    // ------------------------------------------------------------------------------------------------------------

    @Override
    public void run() {
        final CommunicationModel m = model;
        // PORT: bookies, zk and the handles never terminate: daemons. Bookies and zk answer the requester's address.
        Tid b1 = MpProcess.spawnDaemon(new BookieRequestProcessor(m)).tid();
        Tid b2 = MpProcess.spawnDaemon(new BookieRequestProcessor(m)).tid();
        Tid b3 = MpProcess.spawnDaemon(new BookieRequestProcessor(m)).tid();
        Tid zk = MpProcess.spawnDaemon(new LedgerManager(m)).tid();
        final List<Tid> ensemble = List.of(b1, b2, b3);
        // PORT: one logical task per PendingAddOp / read op the handles can create, keyed by entry id (no pool reuse).
        // w1h adds entries 0..e-1; w2h reads entries LAC+1..e and writes back entries LAC+1..e-1 (LAC >= -1).
        List<Tid> w1AddOps = new ArrayList<>();
        for (int i = 0; i < e; i++) {
            w1AddOps.add(MpProcess.spawnDaemon(new PendingAddOpTask(m)).tid());
        }
        Tid rlc = MpProcess.spawnDaemon(new ReadLastConfirmedOp(m, ensemble)).tid();
        List<Tid> w2ReadOps = new ArrayList<>();
        for (int i = 0; i <= e; i++) {
            w2ReadOps.add(MpProcess.spawnDaemon(new RecoveryReadOp(m, ensemble)).tid());
        }
        List<Tid> w2AddOps = new ArrayList<>();
        for (int i = 0; i < e; i++) {
            w2AddOps.add(MpProcess.spawnDaemon(new PendingAddOpTask(m)).tid());
        }
        // PORT: w1 created the ledger (LedgerCreateOp, not in the cut): zk holds it Open at version 0 and w1h's
        // local metadata is that version.
        Tid w1h = MpProcess.spawnDaemon(new LedgerHandle(m, ensemble, zk, w1AddOps,
                new Versioned(new LedgerMetadata(State.OPEN, INVALID_ENTRY_ID, 0L), 0L))).tid();
        Tid w2h = MpProcess.spawnDaemon(new ReadOnlyLedgerHandle(m, ensemble, zk, w2AddOps, rlc, w2ReadOps)).tid();
        final int entries = e;
        MpProcess.Handle w1 = MpProcess.spawn("w1", () -> writer(m, entries, w1h));
        MpProcess.Handle w2 = MpProcess.spawn("w2", () -> recoverer(m, w2h));
        // PORT: init gives each handle its client's address (the callback target of the application's calls).
        final Tid w1tid = w1.tid();
        final Tid w2tid = w2.tid();
        MpProcess.spawnDaemon(() -> {
            Channel.send(m, w1h, new Setup(w1tid));
            Channel.send(m, w2h, new Setup(w2tid));
        });
        w1.await();
        w2.await();
    }

    /** The writer client, as INTERFACE.md gives it. */
    static void writer(CommunicationModel m, int e, Tid h) {
        for (long i = 1; i <= e; i++) {
            Channel.send(m, h, new Add(i));
            MpMessage r = Channel.recv(m);
            if (!(r instanceof Acked) && !(r instanceof Rejected)) {
                throw new AssertionError("Acked or Rejected expected, got " + r);
            }
        }
    }

    /** The recovery client, as INTERFACE.md gives it. */
    static void recoverer(CommunicationModel m, Tid h) {
        Channel.send(m, h, new Recover());
        MpMessage r = Channel.recv(m);
        if (!(r instanceof Closed)) {
            throw new AssertionError("Closed expected, got " + r);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // CompletionValue.convertStatus (client side of PerChannelBookieClient)
    // ------------------------------------------------------------------------------------------------------------

    static int convertStatus(StatusCode status, int defaultStatus) {
        // convert to BKException code
        int rcToRet = statusCodeToExceptionCode(status);
        if (rcToRet == Code.UNINITIALIZED) {
            return defaultStatus;
        } else {
            return rcToRet;
        }
    }

    static int statusCodeToExceptionCode(StatusCode status) {
        switch (status) {
            case EOK:
                return Code.OK;
            case ENOENTRY:
                return Code.NoSuchEntryException;
            case ENOLEDGER:
                return Code.NoSuchLedgerExistsException;
            case EBADVERSION:
                return Code.ProtocolVersionException;
            case EUA:
                return Code.UnauthorizedAccessException;
            case EFENCED:
                return Code.LedgerFencedException;
            case EREADONLY:
                return Code.WriteOnReadOnlyBookieException;
            default:
                // PORT: ETOOMANYREQUESTS and EUNKNOWNLEDGERSTATE (never produced here) fall to the default status.
                return Code.UNINITIALIZED;
        }
    }

    // ============================================================================================================
    // Bookie side: BookieRequestProcessor, WriteEntryProcessorV3, ReadEntryProcessorV3, BookieImpl, LedgerStorage
    // ============================================================================================================

    /** BookieImpl/LedgerStorage exceptions used by the request processors. */
    static class BookieException extends Exception {
        BookieException(String what) {
            super(what, null, false, false);
        }
    }

    static final class LedgerFencedException extends BookieException {
        LedgerFencedException() {
            super("LedgerFencedException");
        }
    }

    static final class NoLedgerException extends BookieException {
        NoLedgerException() {
            super("NoLedgerException");
        }
    }

    static final class NoEntryException extends BookieException {
        NoEntryException() {
            super("NoEntryException");
        }
    }

    /**
     * LedgerStorage of one bookie. PORT: in memory (the interface): entries by id, the fenced flag, the LAC (as
     * InterleavedLedgerStorage: the maximum of the LACs carried by the entries added, FileInfo.setLastAddConfirmed).
     */
    static final class LedgerStorage {
        boolean ledgerExists = false;
        boolean fenced = false;
        Long lac = null;
        final Map<Long, EntryBody> entries = new HashMap<>();
        long lastEntry = INVALID_ENTRY_ID;

        boolean setFenced(long ledgerId) {
            ledgerExists = true; // PORT: fencing creates the ledger's index, as getFileInfo does
            boolean changed = !fenced;
            fenced = true;
            return changed;
        }

        boolean isFenced(long ledgerId) {
            return fenced;
        }

        long addEntry(EntryBody entry) {
            long ledgerId = entry.ledgerId();
            long entryId = entry.entryId();
            long lac = entry.lastAddConfirmed();

            ledgerExists = true;
            entries.put(entryId, entry); // processEntry
            if (entryId > lastEntry) {
                lastEntry = entryId;
            }

            updateLastAddConfirmed(ledgerId, lac);
            return entryId;
        }

        private void updateLastAddConfirmed(long ledgerId, long lac) {
            if (null == this.lac || this.lac < lac) {
                this.lac = lac;
            }
        }

        EntryBody getEntry(long ledgerId, long entryId) throws BookieException {
            if (!ledgerExists) {
                throw new NoLedgerException(); // PORT: no index for the ledger
            }
            /*
             * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
             */
            if (entryId == LAST_ADD_CONFIRMED) {
                entryId = lastEntry;
            }
            EntryBody body = entries.get(entryId);
            if (body == null) {
                throw new NoEntryException();
            }
            return body;
        }

        long getLastAddConfirmed(long ledgerId) throws BookieException {
            Long lac = this.lac;
            if (lac == null) {
                EntryBody bb;
                try {
                    bb = getEntry(ledgerId, LAST_ADD_CONFIRMED);
                } catch (NoEntryException | NoLedgerException e) {
                    bb = null; // PORT: the in-memory getEntry throws where the storage returns no buffer
                }
                if (null == bb) {
                    return INVALID_ENTRY_ID;
                } else {
                    lac = bb.lastAddConfirmed();
                    updateLastAddConfirmed(ledgerId, lac);
                }
            }
            return lac;
        }
    }

    /**
     * BookieImpl over LedgerDescriptor(Impl) and HandleFactoryImpl. PORT: one ledger, authorisation always succeeds
     * (checkAccess), the journal completes at once (writeDataToJournal: the callback runs after the in-memory add),
     * the master key cache and its journal entry are not modelled.
     */
    static final class BookieImpl {
        final LedgerStorage ledgerStorage = new LedgerStorage();

        /** The callback of addEntry/recoveryAddEntry: WriteCallback.writeComplete(rc, ledgerId, entryId, addr, ctx). */
        interface WriteCallback {
            void writeComplete(int rc, long ledgerId, long entryId);
        }

        /** recoveryAddEntry: add even if the ledger has been fenced. */
        void recoveryAddEntry(EntryBody entry, WriteCallback cb) throws BookieException {
            // LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);  PORT: the one ledger, access granted
            addEntryInternal(entry, cb);
        }

        /** addEntry: LedgerFencedException when the ledger is fenced. */
        void addEntry(EntryBody entry, WriteCallback cb) throws BookieException {
            // LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);  PORT: the one ledger, access granted
            if (ledgerStorage.isFenced(entry.ledgerId())) { // handle.isFenced()
                throw new LedgerFencedException();
            }
            addEntryInternal(entry, cb);
        }

        private void addEntryInternal(EntryBody entry, WriteCallback cb) {
            long ledgerId = entry.ledgerId();
            long entryId = ledgerStorage.addEntry(entry); // handle.addEntry(entry)
            // PORT: getJournal(ledgerId).logAddEntry(entry, ackBeforeSync, cb, ctx): the journal write completes at once.
            cb.writeComplete(0, ledgerId, entryId);
        }

        /** fenceLedger: handle.fenceAndLogInJournal(journal); the future completes true. */
        boolean fenceLedger(long ledgerId) {
            // LedgerDescriptor handle = handles.getHandle(ledgerId, masterKey, false);  PORT: access granted
            ledgerStorage.setFenced(ledgerId);
            // PORT: logFenceEntryInJournal completes at once with true (fenced for the first time or already fenced).
            return true;
        }

        EntryBody readEntry(long ledgerId, long entryId) throws BookieException {
            // LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
            return ledgerStorage.getEntry(ledgerId, entryId); // handle.readEntry(entryId)
        }

        long readLastAddConfirmed(long ledgerId) throws BookieException {
            return ledgerStorage.getLastAddConfirmed(ledgerId);
        }
    }

    /**
     * One bookie: BookieRequestProcessor dispatching to WriteEntryProcessorV3 / ReadEntryProcessorV3 over its
     * BookieImpl. PORT: the request thread pools are not modelled: the bookie handles one request at a time, in the
     * order it receives them (its ledger storage is the sequential part).
     */
    static final class BookieRequestProcessor implements Runnable {
        final CommunicationModel m;
        final BookieImpl bookie = new BookieImpl();
        Tid self;

        BookieRequestProcessor(CommunicationModel m) {
            this.m = m;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            while (true) {
                MpMessage r = Channel.recv(m, x -> x instanceof AddRequest || x instanceof ReadRequest);
                if (r instanceof AddRequest add) {
                    processAddRequestV3(add);
                } else {
                    processReadRequestV3((ReadRequest) r);
                }
            }
        }

        private void processAddRequestV3(AddRequest r) {
            new WriteEntryProcessorV3(r).run();
        }

        private void processReadRequestV3(ReadRequest r) {
            new ReadEntryProcessorV3(r).run();
        }

        /** WriteEntryProcessorV3. */
        final class WriteEntryProcessorV3 {
            final AddRequest addRequest;

            WriteEntryProcessorV3(AddRequest request) {
                this.addRequest = request;
            }

            // Returns null if there is no exception thrown
            private AddResponse getAddResponse() {
                long ledgerId = addRequest.ledgerId();
                long entryId = addRequest.entryId();
                // PORT: version compatible, not read-only: those checks always pass.

                BookieImpl.WriteCallback wcb = (rc, lid, eid) -> {
                    StatusCode status;
                    switch (rc) {
                        case 0: // BookieProtocol.EOK
                            status = StatusCode.EOK;
                            break;
                        default:
                            status = StatusCode.EUA; // PORT: EIO (never produced) folded into the default
                            break;
                    }
                    sendResponse(new AddResponse(status, ledgerId, entryId, self, addRequest.ctx()));
                };
                StatusCode status = null;
                EntryBody entryToAdd = addRequest.body();
                try {
                    if ((addRequest.flags() & FLAG_RECOVERY_ADD) == FLAG_RECOVERY_ADD) {
                        bookie.recoveryAddEntry(entryToAdd, wcb);
                    } else {
                        bookie.addEntry(entryToAdd, wcb);
                    }
                    status = StatusCode.EOK;
                } catch (LedgerFencedException ex) {
                    status = StatusCode.EFENCED;
                } catch (BookieException ex) {
                    status = StatusCode.EUA;
                }

                // If everything is okay, we return null so that the calling function
                // doesn't return a response back to the caller.
                if (!status.equals(StatusCode.EOK)) {
                    return new AddResponse(status, ledgerId, entryId, self, addRequest.ctx());
                }
                return null;
            }

            void run() {
                AddResponse addResponse = getAddResponse();
                if (null != addResponse) {
                    // This means there was an error and we should send this back.
                    sendResponse(addResponse);
                }
            }

            private void sendResponse(AddResponse response) {
                Channel.send(m, addRequest.replyTo(), response);
            }
        }

        /** ReadEntryProcessorV3. */
        final class ReadEntryProcessorV3 {
            final ReadRequest readRequest;
            final long ledgerId;
            final long entryId;
            private Boolean fenceResult = null; // PORT: CompletableFuture<Boolean>, completed at once

            ReadEntryProcessorV3(ReadRequest request) {
                this.readRequest = request;
                this.ledgerId = request.ledgerId();
                this.entryId = request.entryId();
            }

            private boolean isFenceRequest() { // RequestUtils.isFenceRequest
                return (readRequest.flags() & FLAG_DO_FENCING) == FLAG_DO_FENCING;
            }

            /** Handle read result for fence read. */
            private void handleReadResultForFenceRead(EntryBody entryBody, long entryId) {
                // PORT: fenceResult is already complete: the fence thread pool and the 1 s wait collapse to its value.
                boolean success = fenceResult;
                sendFenceResponse(entryBody, success);
            }

            /** Read a specific entry; returns null for a fence read (its response is sent by the fence path). */
            private ReadResponse readEntry(long entryId) throws BookieException {
                EntryBody entryBody = bookie.readEntry(ledgerId, entryId);
                if (null != fenceResult) {
                    handleReadResultForFenceRead(entryBody, entryId);
                    return null;
                } else {
                    // readLACPiggyBack is false for V3 reads without ENTRY_PIGGYBACK
                    long knownLAC = bookie.readLastAddConfirmed(ledgerId);
                    return new ReadResponse(StatusCode.EOK, ledgerId, entryId, entryBody, true, knownLAC, self,
                            readRequest.ctx());
                }
            }

            private ReadResponse getReadResponse() {
                try {
                    // handle fence request
                    if (isFenceRequest()) {
                        if (!readRequest.hasMasterKey()) {
                            throw new BookieException("UnauthorizedAccessException");
                        } else {
                            fenceResult = bookie.fenceLedger(ledgerId);
                        }
                    }
                    return readEntry(entryId);
                } catch (NoLedgerException ex) {
                    return buildResponse(StatusCode.ENOLEDGER);
                } catch (NoEntryException ex) {
                    return buildResponse(StatusCode.ENOENTRY);
                } catch (BookieException ex) {
                    return buildResponse(StatusCode.EUA);
                }
            }

            void run() {
                // PORT: the channel is open and the version compatible.
                executeOp();
            }

            private void executeOp() {
                ReadResponse readResponse = getReadResponse();
                if (null != readResponse) {
                    sendResponse(readResponse);
                }
            }

            private void sendFenceResponse(EntryBody entryBody, boolean fenceResult) {
                // build the fence read response (getFenceResponse)
                StatusCode status;
                EntryBody body = null;
                if (!fenceResult) {
                    status = StatusCode.EIO;
                } else {
                    status = StatusCode.EOK;
                    body = entryBody;
                }
                // send the fence read response
                sendResponse(new ReadResponse(status, ledgerId, entryId, body, false, 0L, self, readRequest.ctx()));
            }

            private ReadResponse buildResponse(StatusCode statusCode) {
                return new ReadResponse(statusCode, ledgerId, entryId, null, false, 0L, self, readRequest.ctx());
            }

            private void sendResponse(ReadResponse readResponse) {
                Channel.send(m, readRequest.replyTo(), readResponse);
            }
        }
    }

    // ============================================================================================================
    // zk: the LedgerManager over ZooKeeper as a versioned store
    // ============================================================================================================

    /**
     * PORT: the LedgerManager over ZooKeeper (not in the cut) is a process holding the ledger's versioned metadata:
     * readLedgerMetadata returns it, writeLedgerMetadata replaces it when the given version is current (the new
     * version is the next one) and fails with BKMetadataVersionException otherwise.
     */
    static final class LedgerManager implements Runnable {
        final CommunicationModel m;
        Versioned current = new Versioned(new LedgerMetadata(State.OPEN, INVALID_ENTRY_ID, 0L), 0L);

        LedgerManager(CommunicationModel m) {
            this.m = m;
        }

        @Override
        public void run() {
            while (true) {
                MpMessage r = Channel.recv(m, x -> x instanceof ReadLedgerMetadata || x instanceof WriteLedgerMetadata);
                if (r instanceof ReadLedgerMetadata read) {
                    Channel.send(m, read.replyTo(), new ReadLedgerMetadataResult(Code.OK, current, read.loopId()));
                } else {
                    WriteLedgerMetadata w = (WriteLedgerMetadata) r;
                    if (w.currentVersion() == current.getVersion()) {
                        current = new Versioned(w.metadata(), current.getVersion() + 1);
                        Channel.send(m, w.replyTo(), new WriteLedgerMetadataResult(Code.OK, current, w.loopId()));
                    } else {
                        Channel.send(m, w.replyTo(),
                                new WriteLedgerMetadataResult(Code.MetadataVersionException, null, w.loopId()));
                    }
                }
            }
        }
    }

    // ============================================================================================================
    // Client side: PendingAddOp (one process per op in flight)
    // ============================================================================================================

    /**
     * PORT: the logical task of a PendingAddOp in flight: it receives the bookies' answers to the op's write requests
     * and queues each, one message per answer, on its handle's ordered executor, where PendingAddOp.writeComplete runs
     * (PendingAddOp l.146-152 passes lh.executor to the bookie client; PerChannelBookieClient l.1542-1546 queues the
     * response handling on it). The task has no handle effects of its own. Recycling is not modelled: the process ends
     * when every write request it was sent for has been answered (maybeRecycle's pendingWriteRequests == 0).
     */
    static final class PendingAddOpTask implements Runnable {
        final CommunicationModel m;

        PendingAddOpTask(CommunicationModel m) {
            this.m = m;
        }

        @Override
        public void run() {
            PendingAddOpCreate c = Channel.recv(m, PendingAddOpCreate.class);
            int outstanding = c.pendingWriteRequests();
            while (outstanding > 0) {
                AddResponse r = Channel.recv(m, AddResponse.class);
                outstanding--;
                Channel.send(m, c.lh(), new WriteCompleteCallback(c.entryId(), r.status(), r.ledgerId(), r.entryId(),
                        r.addr(), r.ctx()));
            }
        }
    }

    // ============================================================================================================
    // Client side: ReadLastConfirmedOp (the fencing LAC read)
    // ============================================================================================================

    /**
     * ReadLastConfirmedOp with fencing. PORT: the op's process runs initiateWithFencing() after its creation message
     * (the source calls it on the creator's thread right after the constructor; it only sends), and its callbacks
     * (readEntryComplete); the completion is a message to the LedgerRecoveryOp.
     */
    static final class ReadLastConfirmedOp implements Runnable {
        final CommunicationModel m;
        final List<Tid> currentEnsemble;
        Tid lh;
        private int numResponsesPending;
        private long maxLastAddConfirmed = INVALID_ENTRY_ID; // maxRecoveredData = new RecoveryData(INVALID, 0)
        private long maxLength = 0L;
        private boolean completed = false;
        private int lastSeenError = Code.ReadException;
        private final int[] covered = new int[ENSEMBLE_SIZE]; // RRQuorumCoverageSet
        Tid self;

        ReadLastConfirmedOp(CommunicationModel m, List<Tid> ensemble) {
            this.m = m;
            this.currentEnsemble = ensemble;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            ReadLastConfirmedOpCreate c = Channel.recv(m, ReadLastConfirmedOpCreate.class);
            this.lh = c.lh();
            this.numResponsesPending = currentEnsemble.size();
            for (int i = 0; i < covered.length; i++) {
                covered[i] = Code.UNINITIALIZED;
            }
            initiateWithFencing();
            int outstanding = currentEnsemble.size();
            while (outstanding > 0) {
                ReadResponse r = Channel.recv(m, ReadResponse.class);
                outstanding--;
                readEntryComplete(convertStatus(r.status(), Code.ReadException), r.ledgerId(), r.entryId(), r.body(),
                        r.ctx());
            }
        }

        public void initiateWithFencing() {
            for (int i = 0; i < currentEnsemble.size(); i++) {
                // bookieClient.readEntry(currentEnsemble.get(i), ledgerId, LAST_ADD_CONFIRMED, this, i,
                //                        FLAG_DO_FENCING, ledgerKey, false, callbackExecutor)
                Channel.send(m, currentEnsemble.get(i),
                        new ReadRequest(self, LEDGER_ID, LAST_ADD_CONFIRMED, FLAG_DO_FENCING, true, i));
            }
        }

        private boolean checkCovered() {
            // now check if there are any write quorums, with |ackQuorum| nodes available
            for (int i = 0; i < ENSEMBLE_SIZE; i++) {
                int nodesUnknown = 0;
                for (int j = 0; j < WRITE_QUORUM_SIZE; j++) {
                    int nodeIndex = (i + j) % ENSEMBLE_SIZE;
                    if (covered[nodeIndex] != Code.OK
                            && covered[nodeIndex] != Code.NoSuchEntryException
                            && covered[nodeIndex] != Code.NoSuchLedgerExistsException) {
                        nodesUnknown++;
                    }
                }
                if (nodesUnknown >= ACK_QUORUM_SIZE) {
                    return false;
                }
            }
            return true;
        }

        void readEntryComplete(int rc, long ledgerId, long entryId, EntryBody buffer, int ctx) {
            int bookieIndex = ctx;

            // add the response to coverage set
            covered[bookieIndex] = rc; // coverageSet.addBookie(bookieIndex, rc)

            numResponsesPending--;
            boolean heardValidResponse = false;
            if (rc == Code.OK) {
                // RecoveryData recoveryData = digestManager.verifyDigestAndReturnLastConfirmed(buffer);  PORT: no digest
                long recoveredLac = buffer.lastAddConfirmed();
                if (recoveredLac > maxLastAddConfirmed) {
                    maxLastAddConfirmed = recoveredLac;
                    maxLength = buffer.length();
                }
                heardValidResponse = true;
            }

            if (rc == Code.NoSuchLedgerExistsException || rc == Code.NoSuchEntryException) {
                // this still counts as a valid response, e.g., if the client crashed without writing any entry
                heardValidResponse = true;
            }

            if (rc == Code.UnauthorizedAccessException && !completed) {
                readLastConfirmedDataComplete(rc);
                completed = true;
            }

            if (!heardValidResponse && Code.OK != rc) {
                lastSeenError = rc;
            }

            // other return codes dont count as valid responses
            if (heardValidResponse
                    && checkCovered()
                    && !completed) {
                completed = true;
                readLastConfirmedDataComplete(Code.OK);
                return;
            }

            if (numResponsesPending == 0 && !completed) {
                readLastConfirmedDataComplete(lastSeenError);
            }
        }

        private void readLastConfirmedDataComplete(int rc) {
            // cb.readLastConfirmedDataComplete(rc, maxRecoveredData);  PORT: a message to the LedgerRecoveryOp
            Channel.send(m, lh, new ReadLastConfirmedDataComplete(rc, maxLastAddConfirmed, maxLength));
        }
    }

    // ============================================================================================================
    // Client side: RecoveryReadOp = ListenerBasedPendingReadOp over PendingReadOp with a SequenceReadRequest
    // ============================================================================================================

    /**
     * LedgerRecoveryOp.RecoveryReadOp (ListenerBasedPendingReadOp, PendingReadOp, ReadOpBase), recovery read, not
     * parallel (SequenceReadRequest). PORT: the op's process runs initiate() after its creation message (the source
     * calls it on the creator's thread right after the constructor; it only sends), and its callbacks. No speculative
     * reads (they are timer-driven), no reordered read sequence, no sticky reads (the defaults).
     */
    static final class RecoveryReadOp implements Runnable {
        final CommunicationModel m;
        final List<Tid> ensemble; // metadataForRecovery's ensemble at every entry: the fixed ensemble
        Tid lh;
        long startEntryId;
        long endEntryId;
        final boolean isRecoveryRead = true;
        final int requiredBookiesMissingEntryForRecovery = WRITE_QUORUM_SIZE - ACK_QUORUM_SIZE + 1;
        final LinkedList<SequenceReadRequest> seq = new LinkedList<>();
        final Set<Tid> heardFromHosts = new HashSet<>();
        final BitSet heardFromHostsBitSet = new BitSet(ENSEMBLE_SIZE);
        final Set<Tid> sentToHosts = new HashSet<>();
        int sent = 0;      // PORT: requests sent and
        int answered = 0;  // responses received, to end the process when nothing is outstanding
        Tid self;

        RecoveryReadOp(CommunicationModel m, List<Tid> ensemble) {
            this.m = m;
            this.ensemble = ensemble;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            RecoveryReadOpCreate c = Channel.recv(m, RecoveryReadOpCreate.class);
            this.lh = c.lh();
            this.startEntryId = c.startEntryId();
            this.endEntryId = c.endEntryId();
            initiate();
            while (answered < sent) {
                ReadResponse r = Channel.recv(m, ReadResponse.class);
                answered++;
                readEntryComplete(convertStatus(r.status(), Code.ReadException), r.ledgerId(), r.entryId(), r.body(),
                        r.ctx(), r.addr());
            }
        }

        void initiate() {
            long i = startEntryId;
            do {
                // ensemble = getLedgerMetadata().getEnsembleAt(i);  PORT: the fixed ensemble
                SequenceReadRequest entry = new SequenceReadRequest(ensemble, LEDGER_ID, i);
                seq.add(entry);
                i++;
            } while (i <= endEntryId);
            // read the entries.
            for (SequenceReadRequest entry : seq) {
                entry.read();
                // PORT: readSpeculativeRequestPolicy: speculative reads are timer-driven, not modelled
            }
        }

        public void readEntryComplete(int rc, long ledgerId, final long entryId, final EntryBody buffer, int bookieIndex,
                                      Tid to) {
            // final ReadContext rctx = (ReadContext) ctx;  PORT: (bookieIndex, to); one entry per op (batch size 1)
            final SequenceReadRequest entry = findRequest(entryId);

            if (rc != Code.OK) {
                entry.logErrorAndReattemptRead(bookieIndex, to, "Error: " + rc, rc);
                return;
            }

            heardFromHosts.add(to);
            heardFromHostsBitSet.set(bookieIndex, true);

            // if entry has completed don't handle twice
            if (entry.complete(bookieIndex, to, buffer)) {
                // isRecoveryRead: do not advance LastAddConfirmed for recovery reads
                submitCallback(Code.OK);
            }
        }

        private SequenceReadRequest findRequest(long entryId) {
            for (SequenceReadRequest r : seq) {
                if (r.eId == entryId) {
                    return r;
                }
            }
            // PORT: the request was already removed by submitCallback: an answer to a completed entry
            return completedRequests.get(entryId);
        }

        final Map<Long, SequenceReadRequest> completedRequests = new HashMap<>();

        /** ListenerBasedPendingReadOp.submitCallback. */
        protected void submitCallback(int code) {
            SequenceReadRequest request;
            while (!seq.isEmpty() && (request = seq.getFirst()) != null) {
                if (!request.isComplete()) {
                    return;
                }
                seq.removeFirst();
                completedRequests.put(request.eId, request);
                // callback with completed entry, or none
                boolean hasEntry = Code.OK == request.getRc();
                // listener.onEntryComplete(request.getRc(), lh, entry, ctx);  PORT: a message to the LedgerRecoveryOp
                Channel.send(m, lh, new OnEntryComplete(request.getRc(), hasEntry, request.eId,
                        hasEntry ? request.entryLength : 0L, hasEntry ? request.entryData : 0L));
            }
        }

        void sendReadTo(int bookieIndex, Tid to, SequenceReadRequest entry) {
            if (isRecoveryRead) {
                int flags = FLAG_HIGH_PRIORITY | FLAG_DO_FENCING;
                Channel.send(m, to, new ReadRequest(self, LEDGER_ID, entry.eId, flags, true, bookieIndex));
            } else {
                Channel.send(m, to, new ReadRequest(self, LEDGER_ID, entry.eId, FLAG_NONE, false, bookieIndex));
            }
            sent++;
        }

        /** ReadOpBase.LedgerEntryRequest + PendingReadOp.SingleLedgerEntryRequest + SequenceReadRequest. */
        final class SequenceReadRequest {
            static final int NOT_FOUND = -1;
            boolean complete = false;
            int rc = Code.OK;
            int firstError = Code.OK;
            int numBookiesMissingEntry = 0;
            final long eId;
            final List<Tid> ensemble;
            final int[] writeSet; // lh.getWriteSetForReadOperation(eId): RoundRobin, (eId + w) % ensembleSize
            int nextReplicaIndexToReadFrom = 0;
            final BitSet sentReplicas = new BitSet(WRITE_QUORUM_SIZE);
            final BitSet erroredReplicas = new BitSet(WRITE_QUORUM_SIZE);
            long entryLength; // entryImpl
            long entryData;

            SequenceReadRequest(List<Tid> ensemble, long lId, long eId) {
                this.ensemble = ensemble;
                this.eId = eId;
                this.writeSet = new int[WRITE_QUORUM_SIZE];
                for (int w = 0; w < WRITE_QUORUM_SIZE; w++) {
                    writeSet[w] = (int) ((eId + w) % ENSEMBLE_SIZE);
                }
            }

            private int indexOf(int bookieIndex) {
                for (int w = 0; w < writeSet.length; w++) {
                    if (writeSet[w] == bookieIndex) {
                        return w;
                    }
                }
                return NOT_FOUND;
            }

            boolean isComplete() {
                return complete;
            }

            int getRc() {
                return rc;
            }

            boolean fail(int rc) {
                if (!complete) { // complete.compareAndSet(false, true)
                    complete = true;
                    this.rc = rc;
                    submitCallback(rc);
                    return true;
                } else {
                    return false;
                }
            }

            private boolean readsOutstanding() {
                return (sentReplicas.cardinality() - erroredReplicas.cardinality()) > 0;
            }

            void read() {
                sendNextRead();
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
                sentToHosts.add(to);
                sentReplicas.set(replica);
                return to;
            }

            /** ReadOpBase.LedgerEntryRequest.logErrorAndReattemptRead (the part before the override's). */
            private void baseLogErrorAndReattemptRead(int bookieIndex, Tid host, String errMsg, int rc) {
                if (Code.OK == firstError
                        || Code.NoSuchEntryException == firstError
                        || Code.NoSuchLedgerExistsException == firstError) {
                    firstError = rc;
                }
                // PORT: the BookieHandleNotAvailableException case is never produced
                if (Code.NoSuchEntryException == rc
                        || Code.NoSuchLedgerExistsException == rc) {
                    ++numBookiesMissingEntry;
                }
                // lh.recordReadErrorOnBookie(bookieIndex);  PORT: bookie health statistics only
            }

            void logErrorAndReattemptRead(int bookieIndex, Tid host, String errMsg, int rc) {
                // A late error response from a bookie whose request was already superseded ... must not flow through
                if (isComplete()) {
                    return;
                }
                baseLogErrorAndReattemptRead(bookieIndex, host, errMsg, rc);

                int replica = indexOf(bookieIndex);
                if (replica == NOT_FOUND) {
                    return;
                }
                erroredReplicas.set(replica);

                if (isRecoveryRead && (numBookiesMissingEntry >= requiredBookiesMissingEntryForRecovery)) {
                    /* For recovery, report NoSuchEntry as soon as wQ-aQ+1 bookies report that they do not
                     * have the entry */
                    fail(Code.NoSuchEntryException);
                    return;
                }

                if (!readsOutstanding()) {
                    sendNextRead();
                }
            }

            /** SequenceReadRequest.complete, then SingleLedgerEntryRequest.complete. */
            boolean complete(int bookieIndex, Tid host, EntryBody buffer) {
                if (isComplete()) {
                    return false;
                }
                // PORT: registerSlowBookie for replicas tried earlier only feeds the placement policy: omitted
                if (isComplete()) {
                    return false;
                }
                // content = lh.macManager.verifyDigestAndReturnData(eId, buffer);  PORT: no digest
                long content = buffer.data();
                if (!complete) { // !complete.getAndSet(true)
                    complete = true;
                    rc = Code.OK;
                    entryLength = buffer.length();
                    entryData = content;
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    // ============================================================================================================
    // Client side: LedgerHandle (w1h) and ReadOnlyLedgerHandle (w2h)
    // ============================================================================================================

    /** AsyncCallback.AddCallbackWithLatency, as the handle calls it. */
    interface AddCallback {
        void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, Object ctx);
    }

    /**
     * PendingAddOp: the add's state and writeComplete. PORT: it lives on its handle's process and every writeComplete
     * runs as one turn of the handle's ordered executor, with its synchronous effects (sendAddSuccessCallbacks,
     * notifyWriteFailed, handleUnrecoverableErrorDuringAdd and the close task it queues), as in the source. The
     * bookies' answers reach it through its PendingAddOpTask. Recycling is not modelled.
     */
    static final class PendingAddOp {
        final long entryId;
        boolean completed = false;
        boolean callbackTriggered = false;
        final AddCallback cb;
        final Object ctx;
        final long entryLength;
        final LedgerHandle lh;
        final boolean isRecoveryAdd;
        final List<Tid> ensemble;
        final BitSet ackSet = new BitSet(ENSEMBLE_SIZE); // DistributionSchedule.AckSet (RoundRobin)
        int pendingWriteRequests = 0;
        final Set<Tid> addEntrySuccessBookies = new HashSet<>();

        PendingAddOp(LedgerHandle lh, List<Tid> ensemble, long entryId, AddCallback cb, Object ctx, long entryLength,
                     boolean isRecoveryAdd) {
            this.lh = lh;
            this.ensemble = ensemble;
            this.entryId = entryId;
            this.cb = cb;
            this.ctx = ctx;
            this.entryLength = entryLength;
            this.isRecoveryAdd = isRecoveryAdd;
        }

        /** RoundRobinDistributionSchedule.AckSetImpl.completeBookieAndCheck. */
        private boolean completeBookieAndCheck(int bookieIndexHeardFrom) {
            ackSet.set(bookieIndexHeardFrom);
            return ackSet.cardinality() >= ACK_QUORUM_SIZE;
        }

        void writeComplete(int rc, long ledgerId, long entryId, Tid addr, int ctx) {
            int bookieIndex = ctx;
            --pendingWriteRequests;

            if (!ensemble.get(bookieIndex).equals(addr)) {
                // ensemble has already changed, failure of this addr is immaterial
                return;
            }

            // must record all acks, even if complete (completion can be undone by an ensemble change)
            boolean ackQuorum = false;
            if (Code.OK == rc) {
                ackQuorum = completeBookieAndCheck(bookieIndex);
                addEntrySuccessBookies.add(ensemble.get(bookieIndex));
            }

            if (completed) {
                if (rc != Code.OK) {
                    // Got an error after satisfying AQ. This means we are under replicated at the create itself.
                    // PORT: disableEnsembleChangeFeature unavailable and delayEnsembleChange false (the defaults).
                    lh.notifyWriteFailed(bookieIndex, addr);
                }
                // even the add operation is completed, ... so do a check here to complete again.
                sendAddSuccessCallbacks();
                // I am already finished, ignore incoming responses.
                return;
            }

            switch (rc) {
                case Code.OK:
                    // continue
                    break;
                case Code.ClientClosedException:
                    // bookie client is closed.
                    lh.errorOutPendingAdds(rc);
                    return;
                case Code.IllegalOpException:
                    lh.handleUnrecoverableErrorDuringAdd(rc);
                    return;
                case Code.LedgerFencedException:
                    lh.handleUnrecoverableErrorDuringAdd(rc);
                    return;
                case Code.UnauthorizedAccessException:
                    lh.handleUnrecoverableErrorDuringAdd(rc);
                    return;
                default:
                    // delayEnsembleChange false (the default): lh.handleBookieFailure(ImmutableMap.of(bookieIndex, addr))
                    Map<Integer, Tid> failed = new HashMap<>();
                    failed.put(bookieIndex, addr);
                    lh.handleBookieFailure(failed);
                    return;
            }

            if (ackQuorum && !completed) {
                // PORT: enforceMinNumFaultDomainsForWrite false (the default)
                completed = true;
                sendAddSuccessCallbacks();
            }
        }

        void sendAddSuccessCallbacks() {
            lh.sendAddSuccessCallbacks();
        }

        void submitCallback(final int rc) {
            cb.addCompleteWithLatency(rc, lh, entryId, ctx);
            callbackTriggered = true;
        }
    }

    /** MetadataUpdateLoop.NeedsUpdatePredicate / MetadataTransform. */
    interface NeedsUpdatePredicate {
        boolean needsUpdate(LedgerMetadata metadata) throws Exception;
    }

    interface MetadataTransform {
        LedgerMetadata transform(LedgerMetadata metadata) throws Exception;
    }

    /** The completion of a loop's promise: (result, exception code), the code OK when it completed normally. */
    interface LoopCompletion {
        void whenComplete(Versioned metadata, int exRc);
    }

    /** BKException thrown inside a MetadataUpdateLoop, with its code. */
    static final class BKException extends Exception {
        final int code;

        BKException(int code) {
            super("BKException " + code, null, false, false);
            this.code = code;
        }
    }

    /**
     * LedgerHandle: the handle's state with its pending-add queue, and the handle's ordered executor (lh.executor,
     * the main worker pool thread chosen by ledger id), as one sequential process. PORT: the application's calls
     * (asyncAddEntry; for w2, the open) arrive as the client's messages; executeOrdered queues the task on a local
     * deque drained before every receive; the zk client's callbacks run on this process too.
     */
    static class LedgerHandle implements Runnable {
        enum HandleState { OPEN, CLOSED }

        final CommunicationModel m;
        final List<Tid> currentEnsemble;
        final Tid zk;
        final List<Tid> addOps;
        final long ledgerId = LEDGER_ID;
        Tid self;
        Tid client;

        Versioned versionedMetadata;
        long lastAddConfirmed;
        long lastAddPushed;
        long length;
        long pendingAddsSequenceHead;
        HandleState handleState = HandleState.OPEN;
        boolean changingEnsemble = false;
        int numEnsembleChanges = 0;
        final ArrayDeque<PendingAddOp> pendingAddOps = new ArrayDeque<>();
        final Map<Integer, Tid> delayedWriteFailedBookies = new HashMap<>();
        // PORT: every PendingAddOp this handle created, by entry id (an op drained from the queue still gets answers)
        final Map<Long, PendingAddOp> addOpsByEntryId = new HashMap<>();
        final boolean deferredSync = false; // writeFlags.contains(WriteFlag.DEFERRED_SYNC)

        // PORT: the executor's queue
        final ArrayDeque<Runnable> executorQueue = new ArrayDeque<>();
        // PORT: MetadataUpdateLoops waiting for zk, by loop id
        final Map<Integer, MetadataUpdateLoop> loops = new HashMap<>();
        int nextLoopId = 0;
        // closePromise.whenComplete(...) handlers: PORT: NoopCloseCallback for every close here
        boolean closePromiseDone = false;

        LedgerHandle(CommunicationModel m, List<Tid> ensemble, Tid zk, List<Tid> addOps, Versioned versionedMetadata) {
            this.m = m;
            this.currentEnsemble = ensemble;
            this.zk = zk;
            this.addOps = addOps;
            if (versionedMetadata != null) {
                init(versionedMetadata);
            }
        }

        /** The constructor's metadata part. */
        final void init(Versioned versionedMetadata) {
            this.versionedMetadata = versionedMetadata;
            LedgerMetadata metadata = versionedMetadata.getValue();
            if (metadata.isClosed()) {
                lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
                length = metadata.getLength();
            } else {
                lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
                length = 0L;
            }
            this.pendingAddsSequenceHead = lastAddConfirmed;
        }

        @Override
        public void run() {
            self = MpProcess.self();
            client = Channel.recv(m, Setup.class).client();
            while (true) {
                // PORT: the handle's ordered executor (lh.executor). Before every receive it runs the tasks the handle
                // submitted to itself (executeOrdered), in submission order, until none is left; a received callback,
                // application call or zk completion then runs at once and completely: the receive is its turn.
                Runnable task;
                while ((task = executorQueue.poll()) != null) {
                    task.run();
                }
                MpMessage msg = Channel.recv(m);
                dispatch(msg);
            }
        }

        /** The messages the handle answers. */
        void dispatch(MpMessage msg) {
            if (msg instanceof Add a) {
                // the application: lh.asyncAddEntry(data, cb, ctx) with the entry number as data and context
                asyncAddEntry(a.entry(), applicationAddCallback(), a.entry());
            } else if (msg instanceof WriteCompleteCallback w) {
                // PORT: the bookie client submits the response handling to lh.executor: it runs on receipt, as one
                // executor turn, after every task the handle submitted to itself earlier (see run())
                final PendingAddOp op = addOpsByEntryId.get(w.opEntryId());
                op.writeComplete(convertStatus(w.status(), Code.WriteException), w.ledgerId(), w.entryId(), w.addr(),
                        w.ctx());
            } else if (msg instanceof WriteLedgerMetadataResult w) {
                loops.get(w.loopId()).writeComplete(w);
            } else if (msg instanceof ReadLedgerMetadataResult r) {
                loops.get(r.loopId()).readComplete(r);
            } else {
                dispatchMore(msg);
            }
        }

        void dispatchMore(MpMessage msg) {
            throw new IllegalStateException("unexpected message at the handle: " + msg);
        }

        /** The application's add callback: Acked or Rejected to the client. */
        AddCallback applicationAddCallback() {
            return (rc, lh, entryId, ctx) -> {
                long entry = (Long) ctx;
                if (rc == Code.OK) {
                    Channel.send(m, client, new Acked(entry));
                } else {
                    Channel.send(m, client, new Rejected(entry));
                }
            };
        }

        void executeOrdered(Runnable runnable) {
            // PORT: lh.executor's queue, local to the handle's process; drained before its next receive (see run())
            executorQueue.add(runnable);
        }

        LedgerMetadata getLedgerMetadata() {
            return versionedMetadata.getValue();
        }

        Versioned getVersionedLedgerMetadata() {
            return versionedMetadata;
        }

        long getLastAddConfirmed() {
            return lastAddConfirmed;
        }

        List<Tid> getCurrentEnsemble() {
            return currentEnsemble;
        }

        boolean setLedgerMetadata(Versioned expected, Versioned newMetadata) {
            // ensure that we only update the metadata if it is the object we expect it to be
            if (versionedMetadata == expected) {
                versionedMetadata = newMetadata;
                LedgerMetadata metadata = versionedMetadata.getValue();
                if (metadata.isClosed()) {
                    lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
                    length = metadata.getLength();
                }
                return true;
            } else {
                return false;
            }
        }

        long addToLength(long delta) {
            this.length += delta;
            return this.length;
        }

        boolean isHandleWritable() {
            return !getLedgerMetadata().isClosed() && handleState == HandleState.OPEN;
        }

        // ---- close ----

        void asyncCloseInternal(final int rc) {
            // PORT: no RejectedExecutionException: the executor always accepts
            doAsyncCloseInternal(rc);
        }

        void doAsyncCloseInternal(final int rc) {
            executeOrdered(() -> {
                final HandleState prevHandleState;
                final List<PendingAddOp> pendingAdds;
                final long lastEntry;
                final long finalLength;

                // closePromise.whenComplete(cb.closeComplete ...)  PORT: NoopCloseCallback

                prevHandleState = handleState;

                // drain pending adds first
                pendingAdds = drainPendingAddsAndAdjustLength();

                // taking the length must occur after draining, as draining changes the length
                lastEntry = lastAddPushed = LedgerHandle.this.lastAddConfirmed;
                finalLength = LedgerHandle.this.length;
                handleState = HandleState.CLOSED;

                // error out all pending adds during closing
                errorOutPendingAdds(rc, pendingAdds);

                if (prevHandleState != HandleState.CLOSED) {
                    // tearDownWriteHandleState();  PORT: no explicit-LAC flush, no timeout monitor
                    new MetadataUpdateLoop(
                            LedgerHandle.this::getVersionedLedgerMetadata,
                            (metadata) -> {
                                if (metadata.isClosed()) {
                                    /* If the ledger has been closed with the same lastEntry
                                     * and length that we planned to close with, we have nothing to do,
                                     * so just return success */
                                    if (lastEntry == metadata.getLastEntryId()
                                            && finalLength == metadata.getLength()) {
                                        return false;
                                    } else {
                                        throw new BKException(Code.MetadataVersionException);
                                    }
                                } else {
                                    return true;
                                }
                            },
                            (metadata) -> new LedgerMetadata(State.CLOSED, lastEntry, finalLength),
                            LedgerHandle.this::setLedgerMetadata,
                            (metadata, exRc) -> {
                                closePromiseDone = true; // closePromise completed; PORT: no close callback to run
                            })
                            .run();
                }
            });
        }

        // ---- add ----

        void asyncAddEntry(long data, AddCallback cb, Object ctx) {
            // PendingAddOp op = PendingAddOp.create(this, clientCtx, getCurrentEnsemble(), data, writeFlags, cb, ctx);
            doAsyncAddEntry(data, cb, ctx, false);
        }

        void asyncRecoveryAddEntry(long data, AddCallback cb, Object ctx) {
            // PendingAddOp.create(...).enableRecoveryAdd()
            doAsyncAddEntry(data, cb, ctx, true);
        }

        protected void doAsyncAddEntry(long payload, AddCallback cb, Object ctx, boolean isRecoveryAdd) {
            // PORT: no throttler
            boolean wasClosed = false;
            PendingAddOp op = null;
            long currentLedgerLength = -1;
            // synchronized on this to ensure that the ledger isn't closed between checking and updating lastAddPushed
            if (isHandleWritable()) {
                long entryId = ++lastAddPushed;
                currentLedgerLength = addToLength(PAYLOAD_LENGTH);
                op = new PendingAddOp(this, currentEnsemble, entryId, cb, ctx, PAYLOAD_LENGTH,
                        isRecoveryAdd); // PendingAddOp.create(...); op.setEntryId(entryId)
                addOpsByEntryId.put(entryId, op); // PORT: see addOpsByEntryId
                pendingAddOps.add(op);
            } else {
                wasClosed = true;
            }

            if (wasClosed) {
                // make sure the callback is triggered in main worker pool
                executeOrdered(() -> {
                    // log.warn("Attempt to add to closed ledger");
                    cb.addCompleteWithLatency(Code.LedgerClosedException, LedgerHandle.this, INVALID_ENTRY_ID, ctx);
                    // op.recyclePendAddOpObject();
                });
                return;
            }

            // PORT: waitForWriteSetMs: every channel is writable
            initiate(op, payload, currentLedgerLength, isRecoveryAdd);
        }

        /** PendingAddOp.initiate(), on the handle's process (PORT: see PendingAddOp). */
        private void initiate(PendingAddOp op, long payload, long currentLedgerLength, boolean isRecoveryAdd) {
            // hasRun = true; callbackTriggered is false here (the op was queued just now)
            int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE;
            EntryBody toSend = new EntryBody(ledgerId, op.entryId, lastAddConfirmed, currentLedgerLength, payload);
            Tid opProcess = addOpFor(op.entryId);
            // PORT: the op's creation reaches its process before its responses (guarded receive)
            Channel.send(m, opProcess, new PendingAddOpCreate(self, op.entryId, isRecoveryAdd, WRITE_QUORUM_SIZE));

            // We are about to send. Check if we need to make an ensemble change because of delayed write errors
            maybeHandleDelayedWriteBookieFailure();

            // Iterate over set and trigger the sendWriteRequests
            for (int i = 0; i < WRITE_QUORUM_SIZE; i++) {
                int bookieIndex = (int) ((op.entryId + i) % ENSEMBLE_SIZE); // getWriteSetBookieIndex(entryId, i)
                // sendWriteRequest(ensemble, bookieIndex)
                Channel.send(m, currentEnsemble.get(bookieIndex),
                        new AddRequest(opProcess, ledgerId, op.entryId, flags, toSend, bookieIndex));
                ++op.pendingWriteRequests;
            }
        }

        private Tid addOpFor(long entryId) {
            if (entryId < 0 || entryId >= addOps.size()) {
                throw new IllegalStateException("no PendingAddOp process for entry " + entryId);
            }
            return addOps.get((int) entryId);
        }

        // close the ledger and send fails to all the adds in the pipeline
        void handleUnrecoverableErrorDuringAdd(int rc) {
            if (getLedgerMetadata().getState() == State.IN_RECOVERY) {
                // we should not close ledger if ledger is recovery mode
                // otherwise we may lose entry.
                errorOutPendingAdds(rc);
                return;
            }
            asyncCloseInternal(rc); // NoopCloseCallback
        }

        void errorOutPendingAdds(int rc) {
            errorOutPendingAdds(rc, drainPendingAddsAndAdjustLength());
        }

        List<PendingAddOp> drainPendingAddsAndAdjustLength() {
            PendingAddOp pendingAddOp;
            List<PendingAddOp> opsDrained = new ArrayList<>(pendingAddOps.size());
            while ((pendingAddOp = pendingAddOps.poll()) != null) {
                addToLength(-pendingAddOp.entryLength);
                opsDrained.add(pendingAddOp);
            }
            return opsDrained;
        }

        void errorOutPendingAdds(int rc, List<PendingAddOp> ops) {
            for (PendingAddOp op : ops) {
                op.submitCallback(rc);
            }
        }

        void sendAddSuccessCallbacks() {
            // Start from the head of the queue and proceed while there are
            // entries that have had all their responses come back
            PendingAddOp pendingAddOp;

            while ((pendingAddOp = pendingAddOps.peek()) != null
                    && !changingEnsemble) {
                if (!pendingAddOp.completed) {
                    return;
                }
                // Check if it is the next entry in the sequence.
                if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != pendingAddsSequenceHead + 1) {
                    return;
                }

                pendingAddOps.remove();
                // explicitLacFlushPolicy.updatePiggyBackedLac(lastAddConfirmed);  PORT: no explicit LAC
                pendingAddsSequenceHead = pendingAddOp.entryId;
                if (!deferredSync) {
                    this.lastAddConfirmed = pendingAddsSequenceHead;
                }

                pendingAddOp.submitCallback(Code.OK);
            }
        }

        void notifyWriteFailed(int index, Tid addr) {
            delayedWriteFailedBookies.put(index, addr);
        }

        void maybeHandleDelayedWriteBookieFailure() {
            Map<Integer, Tid> toReplace = null;
            if (delayedWriteFailedBookies.isEmpty()) {
                return;
            }
            toReplace = new HashMap<>(delayedWriteFailedBookies);
            delayedWriteFailedBookies.clear();

            if (toReplace.isEmpty()) {
                return;
            }

            handleBookieFailure(toReplace);
        }

        void handleBookieFailure(final Map<Integer, Tid> failedBookies) {
            // PORT: disableEnsembleChangeFeature unavailable (the default); no DEFERRED_SYNC

            boolean triggerLoop = false;
            Map<Integer, Tid> toReplace = null;
            List<Tid> origEnsemble = null;
            if (changingEnsemble) {
                delayedWriteFailedBookies.putAll(failedBookies);
            } else {
                changingEnsemble = true;
                triggerLoop = true;

                toReplace = new HashMap<>(delayedWriteFailedBookies);
                delayedWriteFailedBookies.clear();
                toReplace.putAll(failedBookies);

                origEnsemble = getCurrentEnsemble();
            }
            if (triggerLoop) {
                ensembleChangeLoop(origEnsemble, toReplace);
            }
        }

        void ensembleChangeLoop(List<Tid> origEnsemble, Map<Integer, Tid> failedBookies) {
            int ensembleChangeId = ++numEnsembleChanges;
            // PORT: maxAllowedEnsembleChanges is not reached

            new MetadataUpdateLoop(
                    this::getVersionedLedgerMetadata,
                    (metadata) -> metadata.getState() == State.OPEN
                            && failedBookies.entrySet().stream().anyMatch(
                                    en -> currentEnsemble.get(en.getKey()).equals(en.getValue())),
                    (metadata) -> {
                        // List<BookieId> newEnsemble = EnsembleUtils.replaceBookiesInEnsemble(...)
                        // PORT: no bookie outside the ensemble: replaceBookie throws BKNotEnoughBookiesException
                        throw new BKException(Code.NotEnoughBookiesException);
                    },
                    this::setLedgerMetadata,
                    (metadata, exRc) -> {
                        // .whenCompleteAsync(..., executor)
                        executeOrdered(() -> {
                            if (exRc != Code.OK) {
                                handleUnrecoverableErrorDuringAdd(exRc); // getExceptionCode(ex, WriteException)
                            } else if (metadata.getValue().isClosed()) {
                                // Metadata closed during attempt to replace bookie.
                                handleUnrecoverableErrorDuringAdd(Code.LedgerClosedException);
                            } else if (metadata.getValue().getState() == State.IN_RECOVERY) {
                                // Metadata marked as in-recovery during attempt to replace bookie.
                                handleUnrecoverableErrorDuringAdd(Code.LedgerFencedException);
                            } else {
                                // PORT: success needs a replacement bookie: unreachable (the transform always throws)
                                throw new IllegalStateException("ensemble change succeeded without a spare bookie");
                            }
                        });
                    })
                    .run();
        }

        /**
         * MetadataUpdateLoop over the zk process. PORT: lm.writeLedgerMetadata / lm.readLedgerMetadata are
         * messages to zk; their completions arrive at the handle's process (the source runs them on the zk client's
         * callback thread; the handle state they touch is synchronized there).
         */
        final class MetadataUpdateLoop {
            final int loopId = nextLoopId++;
            final java.util.function.Supplier<Versioned> currentLocalValue;
            final NeedsUpdatePredicate needsTransformation;
            final MetadataTransform transform;
            final java.util.function.BiPredicate<Versioned, Versioned> updateLocalValue;
            final LoopCompletion promise;
            Versioned writing; // the currentLocal of the write in flight
            Versioned reading; // the current of the read in flight

            MetadataUpdateLoop(java.util.function.Supplier<Versioned> currentLocalValue,
                               NeedsUpdatePredicate needsTransformation,
                               MetadataTransform transform,
                               java.util.function.BiPredicate<Versioned, Versioned> updateLocalValue,
                               LoopCompletion promise) {
                this.currentLocalValue = currentLocalValue;
                this.needsTransformation = needsTransformation;
                this.transform = transform;
                this.updateLocalValue = updateLocalValue;
                this.promise = promise;
            }

            void run() {
                loops.put(loopId, this);
                writeLoop(currentLocalValue.get());
            }

            private void complete(Versioned v, int exRc) {
                loops.remove(loopId);
                promise.whenComplete(v, exRc);
            }

            private void writeLoop(Versioned currentLocal) {
                try {
                    if (needsTransformation.needsUpdate(currentLocal.getValue())) {
                        LedgerMetadata transformed = transform.transform(currentLocal.getValue());
                        writing = currentLocal;
                        // lm.writeLedgerMetadata(ledgerId, transformed, currentLocal.getVersion())
                        Channel.send(m, zk, new WriteLedgerMetadata(self, ledgerId, transformed,
                                currentLocal.getVersion(), loopId));
                    } else {
                        // Update not needed, completing
                        complete(currentLocal, Code.OK);
                    }
                } catch (BKException ex) {
                    complete(null, ex.code);
                } catch (Exception ex) {
                    complete(null, Code.UnexpectedConditionException);
                }
            }

            /** .whenComplete((writtenMetadata, ex) -> ...) of the write. */
            void writeComplete(WriteLedgerMetadataResult r) {
                Versioned currentLocal = writing;
                if (r.rc() == Code.OK) {
                    if (updateLocalValue.test(currentLocal, r.written())) {
                        complete(r.written(), Code.OK);
                    } else {
                        // local value changed while we were writing, try again
                        writeLoop(currentLocalValue.get());
                    }
                } else if (r.rc() == Code.MetadataVersionException) {
                    // conflict writing metadata to store, update local value and try again
                    readLoop();
                } else {
                    complete(null, r.rc());
                }
            }

            private void readLoop() {
                reading = currentLocalValue.get();
                Channel.send(m, zk, new ReadLedgerMetadata(self, ledgerId, loopId));
            }

            /** .whenComplete((read, exception) -> ...) of the read; then writeLoop(readMetadata). */
            void readComplete(ReadLedgerMetadataResult r) {
                Versioned current = reading;
                if (r.rc() != Code.OK) {
                    complete(null, r.rc());
                } else if (current.getVersion() == r.read().getVersion()) {
                    // no update needed, these are the same in the immutable world (Version.Occurred.CONCURRENTLY)
                    writeLoop(current);
                } else if (updateLocalValue.test(current, r.read())) {
                    // updated local value successfully
                    writeLoop(r.read());
                } else {
                    // local value changed while we were reading, look at new value, and try to read again
                    readLoop();
                }
            }
        }
    }

    /**
     * ReadOnlyLedgerHandle of the recovery client, opened with recovery (LedgerOpenOp), with its LedgerRecoveryOp.
     * PORT: the application's open (bk.asyncOpenLedger with recovery) is the client's Recover; the open callback
     * replies Closed with the handle's last add confirmed (the closed metadata's last entry id).
     */
    static final class ReadOnlyLedgerHandle extends LedgerHandle {
        final Tid rlcOp;
        final List<Tid> readOps;
        LedgerRecoveryOp recoveryOp;
        final Map<Long, List<Tid>> newEnsemblesFromRecovery = new HashMap<>();

        ReadOnlyLedgerHandle(CommunicationModel m, List<Tid> ensemble, Tid zk, List<Tid> addOps, Tid rlcOp,
                             List<Tid> readOps) {
            super(m, ensemble, zk, addOps, null);
            this.rlcOp = rlcOp;
            this.readOps = readOps;
        }

        @Override
        void dispatchMore(MpMessage msg) {
            if (msg instanceof Recover) {
                new LedgerOpenOp().initiate();
            } else if (msg instanceof ReadLastConfirmedDataComplete d) {
                // PORT: the op's callback runs on lh.executor (its callbackExecutor): executed on receipt, one turn
                recoveryOp.readLastConfirmedDataComplete(d.rc(), d.lastAddConfirmed(), d.length());
            } else if (msg instanceof OnEntryComplete o) {
                // PORT: sendReadTo passes lh.executor: the read's callback is executed on receipt, one turn
                recoveryOp.onEntryComplete(o.rc(), o.hasEntry(), o.entryId(), o.length(), o.data());
            } else {
                super.dispatchMore(msg);
            }
        }

        @Override
        void handleBookieFailure(final Map<Integer, Tid> failedBookies) {
            // EnsembleUtils.replaceBookiesInEnsemble(...)  PORT: no spare bookie: BKNotEnoughBookiesException
            // Could not get additional bookie to remake ensemble, closing ledger
            handleUnrecoverableErrorDuringAdd(Code.NotEnoughBookiesException);
        }

        @Override
        void handleUnrecoverableErrorDuringAdd(int rc) {
            errorOutPendingAdds(rc);
        }

        /** The open callback of the application: Closed to the client. PORT: an error has no message (none exists). */
        void openComplete(int rc, LedgerHandle lh) {
            if (rc == Code.OK) {
                // PORT: the interface numbers w1's entries from 1 and BookKeeper's ids from 0: Closed carries id + 1, -1 when empty.
                long lac = lh.getLastAddConfirmed();
                Channel.send(m, client, new Closed(lac < 0 ? -1 : lac + 1));
            }
        }

        /** LedgerOpenOp (doRecovery). */
        final class LedgerOpenOp {
            void initiate() {
                // Asynchronously read the ledger metadata node.
                // bk.getLedgerManager().readLedgerMetadata(ledgerId).thenAcceptAsync(this::openWithMetadata, ...)
                int loopId = nextLoopId++; // PORT: the read's id among the handle's zk requests
                pendingOpenLoopId = loopId;
                Channel.send(m, zk, new ReadLedgerMetadata(self, ledgerId, loopId));
            }
        }

        int pendingOpenLoopId = -1;

        @Override
        void dispatch(MpMessage msg) {
            if (msg instanceof ReadLedgerMetadataResult r && r.loopId() == pendingOpenLoopId) {
                pendingOpenLoopId = -1;
                if (r.rc() != Code.OK) {
                    openComplete(r.rc(), null);
                } else {
                    openWithMetadata(r.read());
                }
                return;
            }
            super.dispatch(msg);
        }

        private void openWithMetadata(Versioned versionedMetadata) {
            LedgerMetadata metadata = versionedMetadata.getValue();
            // PORT: password and digest checks pass
            // lh = new ReadOnlyLedgerHandle(bk.getClientCtx(), ledgerId, versionedMetadata, ...)
            init(versionedMetadata);

            if (metadata.isClosed()) {
                // Ledger was closed properly
                openComplete(Code.OK, this);
                return;
            }

            // doRecovery
            recover();
        }

        void recover() {
            NeedsUpdatePredicate needsUpdate = (metadata) -> metadata.getState() == State.OPEN; // !forceRecovery
            new MetadataUpdateLoop(
                    this::getVersionedLedgerMetadata,
                    needsUpdate,
                    (metadata) -> new LedgerMetadata(State.IN_RECOVERY, metadata.getLastEntryId(),
                            metadata.getLength()), // withInRecoveryState()
                    this::setLedgerMetadata,
                    (metadata, exRc) -> {
                        if (exRc != Code.OK) {
                            recoveryComplete(exRc);
                        } else if (metadata.getValue().isClosed()) {
                            closeRecovered(); // thenCompose(completed handle) -> closeRecovered()
                        } else {
                            recoveryOp = new LedgerRecoveryOp(this);
                            recoveryOp.initiate(); // its promise -> closeRecovered()
                        }
                    })
                    .run();
        }

        /** .whenComplete((ignore, ex) -> cb.operationComplete(...)) of recover, then LedgerOpenOp.recoveryComplete. */
        void recoveryComplete(int rc) {
            if (rc == Code.OK) {
                openComplete(Code.OK, this);
            } else {
                // closeLedgerHandleAsync() then openComplete(LedgerRecoveryException or rc, null)
                openComplete(rc == Code.UnauthorizedAccessException || rc == Code.TimeoutException
                        ? rc : Code.LedgerRecoveryException, null);
            }
        }

        void closeRecovered() {
            long lac;
            long len;
            lac = lastAddConfirmed;
            len = length;
            new MetadataUpdateLoop(
                    this::getVersionedLedgerMetadata,
                    (metadata) -> metadata.getState() == State.IN_RECOVERY,
                    (metadata) -> {
                        // newEnsemblesFromRecovery is empty (no ensemble change)
                        return new LedgerMetadata(State.CLOSED, lac, len); // withClosedState().withLastEntryId(lac)
                    },
                    this::setLedgerMetadata,
                    (metadata, exRc) -> {
                        newEnsemblesFromRecovery.clear();
                        recoveryComplete(exRc);
                    })
                    .run();
        }
    }

    /**
     * LedgerRecoveryOp, its methods running on the recovery handle's process (the callbacks run on lh.executor).
     */
    static final class LedgerRecoveryOp implements AddCallback {
        final ReadOnlyLedgerHandle lh;
        boolean promiseDone = false;
        long readCount;
        long writeCount;
        boolean readDone;
        long startEntryToRead;
        long endEntryToRead;

        LedgerRecoveryOp(ReadOnlyLedgerHandle lh) {
            readCount = 0;
            writeCount = 0;
            readDone = false;
            this.lh = lh;
        }

        void initiate() {
            // ReadLastConfirmedOp rlcop = new ReadLastConfirmedOp(...); rlcop.initiateWithFencing();
            // PORT: the op's process runs initiateWithFencing() on its creation message
            Channel.send(lh.m, lh.rlcOp, new ReadLastConfirmedOpCreate(lh.self));
        }

        /** The LastConfirmedDataCallback given to the ReadLastConfirmedOp. */
        void readLastConfirmedDataComplete(int rc, long dataLastAddConfirmed, long dataLength) {
            if (rc == Code.OK) {
                /*
                 The lowest an LAC can be for use in recovery is the first entry id
                 of the current ensemble - 1.
                 */
                long lastEnsembleEntryId = 0L; // PORT: the only ensemble starts at entry 0

                lh.lastAddPushed = lh.lastAddConfirmed = Math.max(dataLastAddConfirmed, (lastEnsembleEntryId - 1));

                lh.length = dataLength;
                lh.pendingAddsSequenceHead = lh.lastAddConfirmed;
                startEntryToRead = endEntryToRead = lh.lastAddConfirmed;
                // keep a copy of ledger metadata before proceeding ledger recovery
                // metadataForRecovery = lh.getLedgerMetadata();  PORT: the read ops use the fixed ensemble
                doRecoveryRead();
            } else if (rc == Code.TimeoutException) {
                submitCallback(rc);
            } else if (rc == Code.UnauthorizedAccessException) {
                submitCallback(rc);
            } else {
                submitCallback(Code.ReadException);
            }
        }

        private void submitCallback(int rc) {
            if (promiseDone) {
                return; // a CompletableFuture completes once
            }
            promiseDone = true;
            if (Code.OK == rc) {
                lh.closeRecovered(); // promise.complete(lh) -> thenCompose(closeRecovered)
            } else {
                lh.recoveryComplete(rc); // promise.completeExceptionally(BKException.create(rc))
            }
        }

        /** Try to read past the last confirmed. */
        private void doRecoveryRead() {
            if (!promiseDone) {
                startEntryToRead = endEntryToRead + 1;
                endEntryToRead = endEntryToRead + RECOVERY_READ_BATCH_SIZE;
                // new RecoveryReadOp(lh, clientCtx, startEntryToRead, endEntryToRead, this, null).initiate();
                if (startEntryToRead < 0 || startEntryToRead >= lh.readOps.size()) {
                    throw new IllegalStateException("no RecoveryReadOp process for entry " + startEntryToRead);
                }
                Channel.send(lh.m, lh.readOps.get((int) startEntryToRead),
                        new RecoveryReadOpCreate(lh.self, startEntryToRead, endEntryToRead));
            }
        }

        /** ReadEntryListener.onEntryComplete. */
        public void onEntryComplete(int rc, boolean hasEntry, long entryId, long entryLength, long data) {
            // PORT: no entry listener (test hook)

            // we only trigger recovery add an entry when readDone == false && callbackDone == false
            if (!promiseDone && !readDone && rc == Code.OK) {
                readCount++;

                /*
                 * We will add this entry again to make sure it is written to enough
                 * replicas. We subtract the length of the data itself, since it will
                 * be added again when processing the call to add it.
                 */
                lh.length = entryLength - PAYLOAD_LENGTH;
                // check whether entry id is expected, so we won't overwritten any entries by mistake
                if (entryId != lh.lastAddPushed + 1) {
                    rc = Code.UnexpectedConditionException;
                }
                if (Code.OK == rc) {
                    lh.asyncRecoveryAddEntry(data, this, null);
                    if (entryId == endEntryToRead) {
                        // trigger next batch read
                        doRecoveryRead();
                    }
                    return;
                }
            }

            // no entry found. stop recovery procedure but wait until recovery add finished.
            if (rc == Code.NoSuchEntryException || rc == Code.NoSuchLedgerExistsException) {
                readDone = true;
                if (readCount == writeCount) {
                    submitCallback(Code.OK);
                }
                return;
            }

            // otherwise, some other error, we can't handle
            if (Code.OK != rc && !promiseDone) {
                submitCallback(rc);
            }
            // else: we successfully read an entry but readDone was already set to true.
        }

        @Override
        public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, Object ctx) {
            // addComplete
            if (rc != Code.OK) {
                submitCallback(rc);
                return;
            }
            long numAdd = ++writeCount;
            if (readDone && readCount == numAdd) {
                submitCallback(rc);
            }
        }
    }
}
