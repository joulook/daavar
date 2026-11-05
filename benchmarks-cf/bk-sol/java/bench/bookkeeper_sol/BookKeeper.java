package bench.bookkeeper_sol; // PORT: JMC entry, immutable wire values and interface adaptation.

import java.util.*; // PORT: JMC entry, immutable wire values and interface adaptation.
import org.mpi_sws.jmc.api.mp.*; // PORT: JMC entry, immutable wire values and interface adaptation.
import org.mpi_sws.jmc.mp.events.*; // PORT: JMC entry, immutable wire values and interface adaptation.
import org.mpi_sws.jmc.mp.launch.MpProgram; // PORT: JMC entry, immutable wire values and interface adaptation.
import org.mpi_sws.jmc.mp.summary.Tag; // PORT: JMC entry, immutable wire values and interface adaptation.

public final class BookKeeper implements MpProgram { // PORT: JMC entry, immutable wire values and interface adaptation.
    final int e; // PORT: JMC entry, immutable wire values and interface adaptation.
    final CommunicationModel communicationModel; // PORT: JMC entry, immutable wire values and interface adaptation.
    public BookKeeper(String... args) { // PORT: JMC entry, immutable wire values and interface adaptation.
        if (args.length != 3 || !args[0].equals("impl")) throw new IllegalArgumentException("impl,<e>,<fifo|bag>"); // PORT: JMC entry, immutable wire values and interface adaptation.
        e = Integer.parseInt(args[1]); // PORT: JMC entry, immutable wire values and interface adaptation.
        if (e < 1) throw new IllegalArgumentException("e >= 1"); // PORT: JMC entry, immutable wire values and interface adaptation.
        communicationModel = switch (args[2]) { case "fifo" -> CommunicationModel.P2P; case "bag" -> CommunicationModel.ASYN; default -> throw new IllegalArgumentException("fifo|bag"); }; // PORT: JMC entry, immutable wire values and interface adaptation.
    } // PORT: JMC entry, immutable wire values and interface adaptation.
    public String programName() { return "bookkeeper"; } // PORT: JMC entry, immutable wire values and interface adaptation.
    public String size() { return Integer.toString(e); } // PORT: JMC entry, immutable wire values and interface adaptation.
    public CommunicationModel model() { return communicationModel; } // PORT: JMC entry, immutable wire values and interface adaptation.
    public List<String> rolesInSpawnOrder() { return List.of("b1", "b2", "b3", "zk", "w1h", "w2h", "w1", "w2", "init"); } // PORT: JMC entry, immutable wire values and interface adaptation.
    public List<String> visibleRoles() { return List.of("w1", "w2"); } // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Add") public record Add(long entry) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Recover") public record Recover() implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Acked") public record Acked(long entry) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Rejected") public record Rejected(long entry) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Closed") public record Closed(long last) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Setup") public record Setup(List<Tid> bookies, Tid zk, Tid w1h, Tid w2h, Tid w1, Tid w2) implements MpMessage { // PORT: JMC entry, immutable wire values and interface adaptation.
        public Setup { bookies = List.copyOf(bookies); } // PORT: JMC entry, immutable wire values and interface adaptation.
    } // PORT: JMC entry, immutable wire values and interface adaptation.
    enum Code { OK, LedgerFencedException, NoSuchEntryException, NoSuchLedgerExistsException, WriteException, LedgerClosedException } // PORT: JMC entry, immutable wire values and interface adaptation.
    enum State { Open, InRecovery, Closed } // PORT: JMC entry, immutable wire values and interface adaptation.
    static final int FLAG_NONE = 0, FLAG_DO_FENCING = 1, FLAG_RECOVERY_ADD = 2, FLAG_HIGH_PRIORITY = 4; // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("AddRequest") public record AddRequest(long entryId, long lac, int flags, int bookieIndex, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("AddResponse") public record AddResponse(Code rc, int bookieIndex) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("ReadRequest") public record ReadRequest(long entryId, int flags, int bookieIndex, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("ReadResponse") public record ReadResponse(Code rc, long entryId, long lac, int bookieIndex) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("FenceLedger") public record FenceLedger(Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("FenceResult") public record FenceResult() implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("AddEntry") public record AddEntry(AddRequest request, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("ReadEntry") public record ReadEntry(ReadRequest request, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("ReadLedgerMetadata") public record ReadLedgerMetadata(Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("WriteLedgerMetadata") public record WriteLedgerMetadata(long version, State state, long last, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("VersionedLedgerMetadata") public record VersionedLedgerMetadata(long version, State state, long last, boolean success) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("Initiate") public record Initiate(long entryId, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("InitiateComplete") public record InitiateComplete(long entryId) implements MpMessage {} // PORT: synchronous initiate return follows every sendWriteRequest; carry that dependency to recovery.
    @Tag("RecoveryData") public record RecoveryData(long lastAddConfirmed, boolean callbackTriggered) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("WriteComplete") public record WriteComplete(long entryId, Code rc, boolean completed) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("NotifyWriteFailed") public record NotifyWriteFailed() implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("SubmitCallback") public record SubmitCallback(long sequence, Code rc, long entryId, Tid callback, boolean recovery) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("DeliverCallback") public record DeliverCallback(long sequence, Code rc, long entryId, Tid callback, boolean recovery) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("CallbackDelivered") public record CallbackDelivered() implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("InitializeRecovery") public record InitializeRecovery(long lac, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("RecoveryInitialized") public record RecoveryInitialized() implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("RecoveryAddEntry") public record RecoveryAddEntry(long entryId, Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("RecoveryAddStarted") public record RecoveryAddStarted(long entryId) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("OnEntryComplete") public record OnEntryComplete(Code rc, long entryId) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("AddComplete") public record AddComplete(Code rc, long entryId) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("LastConfirmedDataComplete") public record LastConfirmedDataComplete(long lac) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("AsyncCloseInternal") public record AsyncCloseInternal(Tid callback) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("CloseComplete") public record CloseComplete(long last) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.
    @Tag("RecoveryComplete") public record RecoveryComplete(long last) implements MpMessage {} // PORT: JMC entry, immutable wire values and interface adaptation.

    void send(Tid destination, MpMessage value) { Channel.send(model(), destination, value); } // PORT: JMC entry, immutable wire values and interface adaptation.
    MpMessage recv() { return Channel.recv(model()); } // PORT: JMC entry, immutable wire values and interface adaptation.
    <T extends MpMessage> T recv(Class<T> type) { return Channel.recv(model(), type); } // PORT: JMC entry, immutable wire values and interface adaptation.
    Tid self() { return MpProcess.self(); } // PORT: JMC entry, immutable wire values and interface adaptation.
    void task(Runnable body) { MpProcess.spawnDaemon(body); } // PORT: JMC entry, immutable wire values and interface adaptation.
    long external(long entryId) { return entryId < 0 ? -1 : entryId + 1; } // PORT: JMC entry, immutable wire values and interface adaptation.
    public void run() { // PORT: JMC entry, immutable wire values and interface adaptation.
        List<MpProcess.Handle> processes = new ArrayList<>(); // PORT: JMC entry, immutable wire values and interface adaptation.
        for (int i = 0; i < 3; i++) processes.add(MpProcess.spawn("b" + (i + 1), () -> new BookieImpl().run())); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle zk = MpProcess.spawn("zk", () -> new LedgerManager().run()); processes.add(zk); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle w1h = MpProcess.spawn("w1h", () -> new LedgerHandle(false).run()); processes.add(w1h); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle w2h = MpProcess.spawn("w2h", () -> new LedgerHandle(true).run()); processes.add(w2h); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle w1 = MpProcess.spawn("w1", () -> writer(w1h.tid())); processes.add(w1); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle w2 = MpProcess.spawn("w2", () -> recoverer(w2h.tid())); processes.add(w2); // PORT: JMC entry, immutable wire values and interface adaptation.
        Setup setup = new Setup(List.of(processes.get(0).tid(), processes.get(1).tid(), processes.get(2).tid()), zk.tid(), w1h.tid(), w2h.tid(), w1.tid(), w2.tid()); // PORT: JMC entry, immutable wire values and interface adaptation.
        MpProcess.Handle init = MpProcess.spawn("init", () -> { for (MpProcess.Handle p : processes) if (p != w1 && p != w2) send(p.tid(), setup); }); // PORT: JMC entry, immutable wire values and interface adaptation.
        init.await(); // PORT: JMC entry, immutable wire values and interface adaptation.
        w1.await(); w2.await(); // PORT: JMC entry, immutable wire values and interface adaptation.
        for (MpProcess.Handle p : processes) if (p != w1 && p != w2) p.await(); // PORT: JMC entry, immutable wire values and interface adaptation.
    } // PORT: JMC entry, immutable wire values and interface adaptation.
    void writer(Tid handle) { // PORT: JMC entry, immutable wire values and interface adaptation.
        for (long i = 1; i <= e; i++) { // PORT: JMC entry, immutable wire values and interface adaptation.
            send(handle, new Add(i)); // PORT: JMC entry, immutable wire values and interface adaptation.
            MpMessage reply = recv(); // PORT: JMC entry, immutable wire values and interface adaptation.
            if (!(reply instanceof Acked) && !(reply instanceof Rejected)) throw new AssertionError("Acked or Rejected expected"); // PORT: JMC entry, immutable wire values and interface adaptation.
        } // PORT: JMC entry, immutable wire values and interface adaptation.
    } // PORT: JMC entry, immutable wire values and interface adaptation.
    void recoverer(Tid handle) { // PORT: JMC entry, immutable wire values and interface adaptation.
        send(handle, new Recover()); // PORT: JMC entry, immutable wire values and interface adaptation.
        if (!(recv() instanceof Closed)) throw new AssertionError("Closed expected"); // PORT: JMC entry, immutable wire values and interface adaptation.
    } // PORT: JMC entry, immutable wire values and interface adaptation.

    final class LedgerManager { // PORT: interface replacement: sequential versioned metadata store.
        long version = 0, last = -1; // PORT: interface replacement: sequential versioned metadata store.
        State state = State.Open; // PORT: interface replacement: sequential versioned metadata store.
        void run() { // PORT: interface replacement: sequential versioned metadata store.
            recv(Setup.class); // PORT: interface replacement: sequential versioned metadata store.
            while (true) { // PORT: interface replacement: sequential versioned metadata store.
                MpMessage m = recv(); // PORT: interface replacement: sequential versioned metadata store.
                if (m instanceof ReadLedgerMetadata r) send(r.callback(), new VersionedLedgerMetadata(version, state, last, true)); // PORT: interface replacement: sequential versioned metadata store.
                else if (m instanceof WriteLedgerMetadata w) { // PORT: interface replacement: sequential versioned metadata store.
                    boolean success = w.version() == version; // PORT: interface replacement: sequential versioned metadata store.
                    if (success) { version++; state = w.state(); last = w.last(); } // PORT: interface replacement: sequential versioned metadata store.
                    send(w.callback(), new VersionedLedgerMetadata(version, state, last, success)); // PORT: interface replacement: sequential versioned metadata store.
                } else throw new AssertionError(m); // PORT: interface replacement: sequential versioned metadata store.
            } // PORT: interface replacement: sequential versioned metadata store.
        } // PORT: interface replacement: sequential versioned metadata store.
    } // PORT: interface replacement: sequential versioned metadata store.
    final class BookieImpl { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        boolean fenced; // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        long lac = -1; // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        final Map<Long, Long> entries = new HashMap<>(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        void run() { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            recv(Setup.class); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            Tid storage = self(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            while (true) { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                MpMessage m = recv(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                if (m instanceof AddRequest a) task(() -> new WriteEntryProcessorV3(storage, a).run()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                else if (m instanceof ReadRequest r) task(() -> new ReadEntryProcessorV3(storage, r).run()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                else if (m instanceof FenceLedger f) { fenceLedger(); send(f.callback(), new FenceResult()); } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                else if (m instanceof AddEntry a) { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                    AddRequest r = a.request(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                    Code rc = (r.flags() & FLAG_RECOVERY_ADD) != 0 ? recoveryAddEntry(r) : addEntry(r); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                    send(a.callback(), new AddResponse(rc, r.bookieIndex())); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                } else if (m instanceof ReadEntry r) send(r.callback(), readEntry(r.request())); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
                else throw new AssertionError(m); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        Code addEntry(AddRequest entry) { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            if (isFenced()) return Code.LedgerFencedException; // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            return addEntryInternal(entry); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        Code recoveryAddEntry(AddRequest entry) { return addEntryInternal(entry); } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        Code addEntryInternal(AddRequest entry) { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            entries.put(entry.entryId(), entry.lac()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            lac = Math.max(lac, entry.lac()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            return Code.OK; // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        boolean isFenced() { return fenced; } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        void fenceLedger() { setFenced(); } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        void setFenced() { fenced = true; } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        ReadResponse readEntry(ReadRequest r) { // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            long entryId = r.entryId(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            if (entryId == -1) entryId = entries.keySet().stream().mapToLong(Long::longValue).max().orElse(-1); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            if (!entries.containsKey(entryId)) return new ReadResponse(Code.NoSuchEntryException, r.entryId(), -1, r.bookieIndex()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            long value = r.entryId() == -1 ? entries.get(entryId) : readLastAddConfirmed(); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
            return new ReadResponse(Code.OK, r.entryId(), value, r.bookieIndex()); // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
        long readLastAddConfirmed() { return lac; } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
    } // PORT: BookieImpl/LedgerDescriptorImpl: sequential in-memory ledger owner.
    final class WriteEntryProcessorV3 { // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
        final Tid storage; final AddRequest request; // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
        WriteEntryProcessorV3(Tid storage, AddRequest request) { this.storage = storage; this.request = request; } // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
        void run() { getAddResponse(); } // PORT: preserve source request-processor entry and getAddResponse method boundary.
        void getAddResponse() { // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
            send(storage, new AddEntry(request, self())); // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
            AddResponse result = recv(AddResponse.class); // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
            send(request.callback(), result); // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
        } // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
    } // PORT: WriteEntryProcessorV3: one logical request task; storage RPC replaces direct call.
    final class ReadEntryProcessorV3 { // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
        final Tid storage; final ReadRequest readRequest; // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
        ReadEntryProcessorV3(Tid storage, ReadRequest readRequest) { this.storage = storage; this.readRequest = readRequest; } // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
        void run() { getReadResponse(); } // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
        void getReadResponse() { // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
            if ((readRequest.flags() & FLAG_DO_FENCING) != 0) { // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
                send(storage, new FenceLedger(self())); // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
                recv(FenceResult.class); // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
            } // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
            readEntry(); // PORT: source getReadResponse fences before invoking readEntry.
        } // PORT: preserve source getReadResponse/readEntry method boundary.
        void readEntry() { // PORT: synchronous storage read expressed as RPC on sequential storage owner.
            send(storage, new ReadEntry(readRequest, self())); // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
            ReadResponse response = recv(ReadResponse.class); // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
            send(readRequest.callback(), response); // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
        } // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.
    } // PORT: ReadEntryProcessorV3: fence dependency and read expressed as storage messages.

    record CloseAfterCallbacks(long sequence, long lastEntry, Tid callback) {} // PORT: local close continuation preserves synchronous error-callback-before-metadata dependency.
    final class Pending { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        final long entryId; final Tid callback; final boolean recovery; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        boolean completed; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        Pending(long entryId, Tid callback, boolean recovery) { this.entryId = entryId; this.callback = callback; this.recovery = recovery; } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
    } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
    final class LedgerHandle { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        final boolean recovery; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        final ArrayDeque<Pending> pendingAddOps = new ArrayDeque<>(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        final List<CloseAfterCallbacks> closeAfterCallbacks = new ArrayList<>(); // PORT: only the queue owner holds deferred close continuations.
        final Map<Long, Tid> recoveryStarts = new HashMap<>(); // PORT: retain recovery call return dependencies until PendingAddOp finishes initiation.
        final Set<Long> callbackTriggered = new HashSet<>(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        Setup setup; Tid callbackThread; long callbackSequence; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        long lastAddPushed = -1, lastAddConfirmed = -1, pendingAddsSequenceHead = -1; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        boolean handleClosed, delayedWriteFailedBookies; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        LedgerHandle(boolean recovery) { this.recovery = recovery; } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void run() { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            setup = recv(Setup.class); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            Tid callbackOwner = self(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            callbackThread = MpProcess.spawnDaemon(() -> executeOrdered(callbackOwner)).tid(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            while (true) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                MpMessage m = recv(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                if (m instanceof Add a) doAsyncAddEntry(a.entry() - 1, setup.w1(), false); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                else if (m instanceof Recover) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    Tid owner = self(); Setup addresses = setup; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    task(() -> new LedgerRecoveryOp(owner, addresses).initiate()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof Initiate i) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    boolean triggered = callbackTriggered.contains(i.entryId()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    send(i.callback(), new RecoveryData(lastAddConfirmed, triggered)); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    if (!triggered) maybeHandleDelayedWriteBookieFailure(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof InitiateComplete i) { // PORT: source asyncRecoveryAddEntry returns only after op.initiate has sent the write set.
                    Tid callback = recoveryStarts.remove(i.entryId()); // PORT: queue owner correlates synchronous return with the recovery coordinator.
                    if (callback != null) send(callback, new RecoveryAddStarted(i.entryId())); // PORT: release the next recovery read only after all recovery writes were sent.
                } else if (m instanceof WriteComplete w) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    if (w.rc() != Code.OK) handleUnrecoverableErrorDuringAdd(w.rc()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    else { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                        for (Pending op : pendingAddOps) if (op.entryId == w.entryId()) op.completed = w.completed(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                        sendAddSuccessCallbacks(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof NotifyWriteFailed) delayedWriteFailedBookies = true; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                else if (m instanceof InitializeRecovery i) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    lastAddPushed = lastAddConfirmed = i.lac(); pendingAddsSequenceHead = lastAddConfirmed; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    send(i.callback(), new RecoveryInitialized()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof RecoveryAddEntry r) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    recoveryStarts.put(r.entryId(), r.callback()); // PORT: register return dependency before starting the logical add task.
                    asyncRecoveryAddEntry(r.entryId(), r.callback()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    if (handleClosed) { recoveryStarts.remove(r.entryId()); send(r.callback(), new RecoveryAddStarted(r.entryId())); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof AsyncCloseInternal c) asyncCloseInternal(Code.LedgerClosedException, c.callback()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                else if (m instanceof DeliverCallback c) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    if (c.recovery()) send(c.callback(), new AddComplete(c.rc(), c.entryId())); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    else send(c.callback(), c.rc() == Code.OK ? new Acked(external(c.entryId())) : new Rejected(external(c.entryId()))); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                    send(callbackThread, new CallbackDelivered()); // PORT: preserve synchronous callback-return dependency before metadata close.
                    Iterator<CloseAfterCallbacks> continuations = closeAfterCallbacks.iterator(); // PORT: resume source close only after its final pending-add callback has returned.
                    while (continuations.hasNext()) { // PORT: multiple close attempts share the single winning handle-close transition.
                        CloseAfterCallbacks close = continuations.next(); // PORT: continuation data belongs exclusively to the handle queue owner.
                        if (close.sequence() <= c.sequence()) { continuations.remove(); startMetadataClose(close.lastEntry(), close.callback()); } // PORT: preserve callback delivery before MetadataUpdateLoop.run.
                    } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                } else if (m instanceof RecoveryComplete c) { handleClosed = true; send(setup.w2(), new Closed(external(c.last()))); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                else throw new AssertionError(m); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        boolean isHandleWritable() { return !handleClosed; } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void doAsyncAddEntry(long expectedEntry, Tid callback, boolean isRecoveryAdd) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            boolean wasClosed = false; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            Pending op = null; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            if (isHandleWritable()) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                long entryId = ++lastAddPushed; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                if (entryId != expectedEntry) throw new AssertionError("Unexpected entryId"); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                op = new Pending(entryId, callback, isRecoveryAdd); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                pendingAddOps.add(op); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } else wasClosed = true; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            if (wasClosed) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                send(callbackThread, new SubmitCallback(callbackSequence++, Code.LedgerClosedException, expectedEntry, callback, isRecoveryAdd)); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                return; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            long entryId = op.entryId; Tid owner = self(); Setup addresses = setup; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            task(() -> new PendingAddOp(owner, addresses, entryId, isRecoveryAdd).initiate()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void asyncRecoveryAddEntry(long entryId, Tid callback) { doAsyncAddEntry(entryId, callback, true); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void maybeHandleDelayedWriteBookieFailure() { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            if (!delayedWriteFailedBookies) return; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            delayedWriteFailedBookies = false; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            handleBookieFailure(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void handleBookieFailure() { handleUnrecoverableErrorDuringAdd(Code.WriteException); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void handleUnrecoverableErrorDuringAdd(Code rc) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            if (recovery) { errorOutPendingAdds(rc); return; } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            asyncCloseInternal(rc, null); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void asyncCloseInternal(Code rc, Tid callback) { doAsyncCloseInternal(rc, callback); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void doAsyncCloseInternal(Code rc, Tid callback) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            boolean prevHandleState = handleClosed; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            List<Pending> pendingAdds = drainPendingAddsAndAdjustLength(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            long lastEntry = lastAddPushed = lastAddConfirmed; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            handleClosed = true; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            errorOutPendingAdds(rc, pendingAdds); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            if (!prevHandleState) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                if (pendingAdds.isEmpty()) startMetadataClose(lastEntry, callback); // PORT: source has no error callbacks to await when the drained queue is empty.
                else closeAfterCallbacks.add(new CloseAfterCallbacks(callbackSequence - 1, lastEntry, callback)); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void startMetadataClose(long lastEntry, Tid callback) { // PORT: resume the metadata-update portion of source doAsyncCloseInternal after synchronous callbacks.
            Tid zk = setup.zk(); // PORT: immutable address passed to the logical metadata-update task.
            task(() -> closeMetadata(zk, lastEntry, callback)); // PORT: source asynchronous MetadataUpdateLoop, without pool-worker reuse.
        } // PORT: end metadata close continuation.
        List<Pending> drainPendingAddsAndAdjustLength() { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            List<Pending> drained = new ArrayList<>(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            while (!pendingAddOps.isEmpty()) drained.add(pendingAddOps.remove()); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            return drained; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void errorOutPendingAdds(Code rc) { errorOutPendingAdds(rc, drainPendingAddsAndAdjustLength()); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void errorOutPendingAdds(Code rc, List<Pending> ops) { for (Pending op : ops) submitCallback(op, rc); } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void sendAddSuccessCallbacks() { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            while (!pendingAddOps.isEmpty()) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                Pending pendingAddOp = pendingAddOps.peek(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                if (!pendingAddOp.completed) return; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                if (pendingAddOp.entryId != 0 && pendingAddOp.entryId != pendingAddsSequenceHead + 1) return; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                pendingAddOps.remove(); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                pendingAddsSequenceHead = pendingAddOp.entryId; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                lastAddConfirmed = pendingAddsSequenceHead; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                submitCallback(pendingAddOp, Code.OK); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void submitCallback(Pending op, Code rc) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            callbackTriggered.add(op.entryId); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            send(callbackThread, new SubmitCallback(callbackSequence++, rc, op.entryId, op.callback, op.recovery)); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        void executeOrdered(Tid owner) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            long sequence = 0; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            while (true) { // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                long expected = sequence++; // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                SubmitCallback c = Channel.recv(model(), SubmitCallback.class, v -> v.sequence() == expected); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                send(owner, new DeliverCallback(c.sequence(), c.rc(), c.entryId(), c.callback(), c.recovery())); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
                recv(CallbackDelivered.class); // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
            } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
        } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.
    } // PORT: LedgerHandle: queue-owned PendingAddOp projection; callback task communicates with owner.

    void closeMetadata(Tid zk, long lastEntry, Tid callback) { // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
        send(zk, new ReadLedgerMetadata(self())); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
        VersionedLedgerMetadata metadata = recv(VersionedLedgerMetadata.class); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
        while (true) { // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
            if (metadata.state() == State.Closed) { // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                if (metadata.last() != lastEntry) { // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                    if (callback != null) send(callback, new CloseComplete(metadata.last())); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                    return; // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                } // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                if (callback != null) send(callback, new CloseComplete(lastEntry)); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
                return; // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
            } // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
            send(zk, new WriteLedgerMetadata(metadata.version(), State.Closed, lastEntry, self())); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
            metadata = recv(VersionedLedgerMetadata.class); // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
            if (metadata.success()) { if (callback != null) send(callback, new CloseComplete(lastEntry)); return; } // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
        } // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
    } // PORT: MetadataUpdateLoop: interface versioned CAS; existing Closed result is authoritative.
    final class AckSetImpl { // PORT: RoundRobinDistributionSchedule: fixed N=Qw=3, Qa=2; omit pooling.
        final BitSet ackSet = new BitSet(3), failureMap = new BitSet(3); // PORT: RoundRobinDistributionSchedule: fixed N=Qw=3, Qa=2; omit pooling.
        boolean completeBookieAndCheck(int bookieIndex) { failureMap.clear(bookieIndex); ackSet.set(bookieIndex); return ackSet.cardinality() >= 2; } // PORT: RoundRobinDistributionSchedule: fixed N=Qw=3, Qa=2; omit pooling.
        boolean failBookieAndCheck(int bookieIndex) { ackSet.clear(bookieIndex); failureMap.set(bookieIndex); return failureMap.cardinality() > 1; } // PORT: RoundRobinDistributionSchedule: fixed N=Qw=3, Qa=2; omit pooling.
    } // PORT: RoundRobinDistributionSchedule: fixed N=Qw=3, Qa=2; omit pooling.
    final class PendingAddOp { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        final Tid lh; final Setup setup; final long entryId; final boolean isRecoveryAdd; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        final AckSetImpl ackSet = new AckSetImpl(); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        boolean completed; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        long lac; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        PendingAddOp(Tid lh, Setup setup, long entryId, boolean isRecoveryAdd) { this.lh = lh; this.setup = setup; this.entryId = entryId; this.isRecoveryAdd = isRecoveryAdd; } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        void initiate() { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            send(lh, new Initiate(entryId, self())); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            RecoveryData data = recv(RecoveryData.class); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            if (data.callbackTriggered()) { send(lh, new InitiateComplete(entryId)); return; } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            lac = data.lastAddConfirmed(); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            for (int i = 0; i < 3; i++) sendWriteRequest((int) ((entryId + i) % 3)); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            send(lh, new InitiateComplete(entryId)); // PORT: source synchronous initiate returns after sending all three writes, before recovery starts the next read.
            for (int i = 0; i < 3; i++) writeComplete(recv(AddResponse.class)); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        void sendWriteRequest(int bookieIndex) { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            int flags = isRecoveryAdd ? FLAG_RECOVERY_ADD | FLAG_HIGH_PRIORITY : FLAG_NONE; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            send(setup.bookies().get(bookieIndex), new AddRequest(entryId, lac, flags, bookieIndex, self())); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        void writeComplete(AddResponse response) { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            Code rc = response.rc(); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            boolean ackQuorum = false; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            if (rc == Code.OK) ackQuorum = ackSet.completeBookieAndCheck(response.bookieIndex()); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            if (completed) { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                if (rc != Code.OK) send(lh, new NotifyWriteFailed()); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                sendAddSuccessCallbacks(); // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                return; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            switch (rc) { // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                case OK: break; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                case LedgerFencedException: handleUnrecoverableErrorDuringAdd(rc); return; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
                default: ackSet.failBookieAndCheck(response.bookieIndex()); handleUnrecoverableErrorDuringAdd(rc); return; // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
            if (ackQuorum && !completed) { completed = true; sendAddSuccessCallbacks(); } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        void handleUnrecoverableErrorDuringAdd(Code rc) { send(lh, new WriteComplete(entryId, rc, completed)); } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
        void sendAddSuccessCallbacks() { send(lh, new WriteComplete(entryId, Code.OK, completed)); } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
    } // PORT: PendingAddOp: logical add task; handle calls become messages; omit digests/timers/pooling.
    final class ReadLastConfirmedOp { // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        final Setup setup; final Tid callback; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        final BitSet coverageSet = new BitSet(3); // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        long maxRecoveredData = -1; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        boolean completed; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        ReadLastConfirmedOp(Setup setup, Tid callback) { this.setup = setup; this.callback = callback; } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        void initiateWithFencing() { // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            for (int i = 0; i < 3; i++) send(setup.bookies().get(i), new ReadRequest(-1, FLAG_DO_FENCING, i, self())); // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            for (int i = 0; i < 3; i++) readEntryComplete(recv(ReadResponse.class)); // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        void readEntryComplete(ReadResponse r) { // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            coverageSet.set(r.bookieIndex()); // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            boolean heardValidResponse = false; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            if (r.rc() == Code.OK) { if (r.lac() > maxRecoveredData) maxRecoveredData = r.lac(); heardValidResponse = true; } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            if (r.rc() == Code.NoSuchLedgerExistsException || r.rc() == Code.NoSuchEntryException) heardValidResponse = true; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            if (heardValidResponse && coverageSet.cardinality() >= 2 && !completed) { // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
                completed = true; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
                send(callback, new LastConfirmedDataComplete(maxRecoveredData)); // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
                return; // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
            } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
        } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
    } // PORT: ReadLastConfirmedOp: logical fencing-LAC task; fixed quorum coverage.
    class PendingReadOp { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        final Setup setup; final Tid callback; final long entryId; // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        int nextReplicaIndexToReadFrom, numBookiesMissingEntry; // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        PendingReadOp(Setup setup, Tid callback, long entryId) { this.setup = setup; this.callback = callback; this.entryId = entryId; } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        void initiate() { new SequenceReadRequest().read(); } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        void sendReadTo(int bookieIndex) { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            int flags = FLAG_HIGH_PRIORITY | FLAG_DO_FENCING; // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            send(setup.bookies().get(bookieIndex), new ReadRequest(entryId, flags, bookieIndex, self())); // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        boolean readEntryComplete(ReadResponse response, SequenceReadRequest entry) { // PORT: source PendingReadOp callback projected to one active sequential entry.
            if (response.rc() != Code.OK) return entry.logErrorAndReattemptRead(response); // PORT: preserve source error-before-success callback branch.
            entry.complete(); // PORT: recovery reads do not advance handle LAC, and the first valid entry completes.
            return true; // PORT: signal logical single-entry task completion.
        } // PORT: end source readEntryComplete projection.
        final class SequenceReadRequest { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            void read() { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                while (nextReplicaIndexToReadFrom < 3) { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                    sendNextRead(); // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                    ReadResponse r = recv(ReadResponse.class); // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                    if (readEntryComplete(r, this)) return; // PORT: source read callback dispatches error reattempt or successful completion in that order.
                } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                throw new AssertionError("No failure callback"); // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            void sendNextRead() { int index = (int) ((entryId + nextReplicaIndexToReadFrom++) % 3); sendReadTo(index); } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            boolean logErrorAndReattemptRead(ReadResponse r) { // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                if (r.rc() == Code.NoSuchEntryException || r.rc() == Code.NoSuchLedgerExistsException) numBookiesMissingEntry++; // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                if (numBookiesMissingEntry >= 2) { submitCallback(Code.NoSuchEntryException); return true; } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
                return false; // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
            void complete() { submitCallback(Code.OK); } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
        void submitCallback(Code rc) { send(callback, new OnEntryComplete(rc, entryId)); } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
    } // PORT: PendingReadOp: batch=1, default sequence reads, no speculation; preserve main fencing flags.
    class ListenerBasedPendingReadOp extends PendingReadOp { // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
        ListenerBasedPendingReadOp(Setup setup, Tid callback, long entryId) { super(setup, callback, entryId); } // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
        @Override void submitCallback(Code rc) { send(callback, new OnEntryComplete(rc, entryId)); } // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
    } // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
    final class RecoveryReadOp extends ListenerBasedPendingReadOp { // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
        RecoveryReadOp(Setup setup, Tid callback, long entryId) { super(setup, callback, entryId); } // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
    } // PORT: source ListenerBasedPendingReadOp/RecoveryReadOp names; one-entry listener projection.
    final class LedgerRecoveryOp { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        final Tid lh; final Setup setup; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        long readCount, writeCount, lastAddPushed, endEntryToRead; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        boolean readDone, promiseDone; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        LedgerRecoveryOp(Tid lh, Setup setup) { this.lh = lh; this.setup = setup; } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        void initiate() { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            send(setup.zk(), new ReadLedgerMetadata(self())); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            VersionedLedgerMetadata metadata = recv(VersionedLedgerMetadata.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            while (metadata.state() == State.Open) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                send(setup.zk(), new WriteLedgerMetadata(metadata.version(), State.InRecovery, -1, self())); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                metadata = recv(VersionedLedgerMetadata.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (metadata.state() == State.Closed) { send(lh, new RecoveryComplete(metadata.last())); return; } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            Tid callback = self(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            task(() -> new ReadLastConfirmedOp(setup, callback).initiateWithFencing()); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            LastConfirmedDataComplete data = recv(LastConfirmedDataComplete.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            lastAddPushed = endEntryToRead = Math.max(data.lac(), -1); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            send(lh, new InitializeRecovery(lastAddPushed, self())); recv(RecoveryInitialized.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            doRecoveryRead(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            while (!promiseDone) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                MpMessage m = recv(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                if (m instanceof OnEntryComplete c) onEntryComplete(c); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                else if (m instanceof AddComplete c) addComplete(c); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                else throw new AssertionError(m); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            send(lh, new AsyncCloseInternal(self())); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            CloseComplete result = recv(CloseComplete.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            send(lh, new RecoveryComplete(result.last())); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        void doRecoveryRead() { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (!promiseDone) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                long startEntryToRead = endEntryToRead + 1; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                endEntryToRead = endEntryToRead + 1; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                Tid callback = self(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                task(() -> new RecoveryReadOp(setup, callback, startEntryToRead).initiate()); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        void onEntryComplete(OnEntryComplete result) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            Code rc = result.rc(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (!promiseDone && !readDone && rc == Code.OK) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                readCount++; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                if (result.entryId() != lastAddPushed + 1) throw new AssertionError("Unexpected entryId during recovery add"); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                send(lh, new RecoveryAddEntry(result.entryId(), self())); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                recv(RecoveryAddStarted.class); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                lastAddPushed++; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                if (result.entryId() == endEntryToRead) doRecoveryRead(); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                return; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (rc == Code.NoSuchEntryException || rc == Code.NoSuchLedgerExistsException) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                readDone = true; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                if (readCount == writeCount) submitCallback(Code.OK); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
                return; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (rc != Code.OK && !promiseDone) throw new AssertionError("Recovery read failed: " + rc); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        void addComplete(AddComplete result) { // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (result.rc() != Code.OK) throw new AssertionError("Recovery add failed: " + result.rc()); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            long numAdd = ++writeCount; // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
            if (readDone && readCount == numAdd) submitCallback(result.rc()); // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
        void submitCallback(Code rc) { promiseDone = true; } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
    } // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
} // PORT: LedgerRecoveryOp: asynchronous reads/adds as logical tasks and callback messages.
