package bench.craq;

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
 * Transliteration of DeepSeek 3FS's chain replication (CRAQ) write and read paths for one chunk on one
 * chain of three targets (3FS main 22fca04; cut in plan/case-studies/3fs/craq-src), against JMC's
 * message-passing API. See NOTES.md next to this file for the role/message description, the method
 * map and every decision. Lines that differ from the source are marked {@code // PORT:}.
 *
 * <p>Observation contract (owner): the concurrency inside a storage node is modelled as logical tasks,
 * one process per concurrent activity of the source, with the source's dependencies as messages and no
 * other program order between tasks; the reuse of pool workers is not modelled.
 */
public final class ChainReplication implements MpProgram {

    // ------------------------------------------------------------------------------------------
    // Instance parameters (INTERFACE.md).
    // ------------------------------------------------------------------------------------------
    static final int CLIENTS = 2;
    static final int TARGETS = 3; // t0 = head, t1, t2 = tail; target k on node nk
    static final int CHAIN_VER = 1; // PORT: one fixed chain, constant chain version

    static long value(int i, int j) {
        return 1 + i + (long) CLIENTS * j;
    }

    // ------------------------------------------------------------------------------------------
    // Visible messages (INTERFACE.md, fixed shape and tags).
    // ------------------------------------------------------------------------------------------
    @Tag("Write") public record Write(int client, long value) implements MpMessage {}
    @Tag("Read") public record Read(int client) implements MpMessage {}
    @Tag("Ack") public record Ack(int client) implements MpMessage {}
    @Tag("Value") public record Value(int client, long value) implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Status codes (fbs/ StorageCode, StorageClientCode; numeric values are this port's).
    // ------------------------------------------------------------------------------------------
    static final int kOK = 0;
    static final int kChunkNotCommit = 1;
    static final int kChunkMissingUpdate = 2;
    static final int kChunkCommittedUpdate = 3;
    static final int kChunkStaleUpdate = 4;
    static final int kChunkAdvanceUpdate = 5;
    static final int kChunkNotClean = 6;
    static final int kChunkVersionMismatch = 7;
    static final int kChainVersionMismatch = 8;
    static final int kNoSuccessorTarget = 9;
    static final int kChunkStaleCommit = 10;
    static final int kChecksumMismatch = 11;
    static final int kRoutingError = 12;
    static final int kDuplicateUpdate = 13;
    static final int kFoundBug = 14;

    // ------------------------------------------------------------------------------------------
    // Values (fbs/ Common.h, storage/store ChunkMetadata), as immutable records.
    // ------------------------------------------------------------------------------------------
    public enum ChunkState { COMMIT, DIRTY, CLEAN }

    /** ChunkMetadata: updateVer, commitVer, chainVer, chunkState; content stands for the chunk data and its checksum. */
    @Tag("ChunkMetadata")
    public record ChunkMetadata(long updateVer, long commitVer, int chainVer, ChunkState chunkState, long content)
            implements MpMessage {}

    /**
     * IOResult: lengthInfo (code == kOK means a value, which is the length, 1 = the whole chunk),
     * updateVer, commitVer, checksum (= content), commitChainVer.
     */
    @Tag("IOResult")
    public record IOResult(int code, long updateVer, long commitVer, long checksum, int commitChainVer)
            implements MpMessage {
        boolean ok() {
            return code == kOK;
        }

        IOResult withCode(int c) {
            return new IOResult(c, updateVer, commitVer, checksum, commitChainVer);
        }

        IOResult withVersions(long u, long c) {
            return new IOResult(code, u, c, checksum, commitChainVer);
        }

        IOResult withCommitVer(long c) {
            return new IOResult(code, updateVer, c, checksum, commitChainVer);
        }

        IOResult withChecksum(long s) {
            return new IOResult(code, updateVer, commitVer, s, commitChainVer);
        }
    }

    static IOResult makeError(int code) {
        return new IOResult(code, 0, 0, 0, 0);
    }

    /** UpdateIO: the full-chunk write; offset/length/chunkSize fixed (one chunk, full chunk). */
    @Tag("UpdateIO")
    public record UpdateIO(long updateVer, long data, long checksum, int chainVer) implements MpMessage {}

    /** UpdateOptions: fromClient, isSyncing (always false here). */
    @Tag("UpdateOptions")
    public record UpdateOptions(boolean fromClient, boolean isSyncing) implements MpMessage {}

    /** MessageTag: clientId and the client's channel seqnum (channel id = client). */
    @Tag("MessageTag")
    public record MessageTag(int clientId, long seqnum) implements MpMessage {}

    /** UpdateReq: payload, options, tag, retryCount. */
    @Tag("UpdateReq")
    public record UpdateReq(UpdateIO payload, UpdateOptions options, MessageTag tag, int retryCount)
            implements MpMessage {}

    /** UpdateRsp: the result returned to the predecessor. */
    @Tag("UpdateRsp") public record UpdateRsp(MessageTag tag, IOResult result) implements MpMessage {}

    // Internal messages of a storage node (logical tasks <-> StorageTarget, chunk lock, readers).
    @Tag("LockChunk") public record LockChunk(int channel) implements MpMessage {}
    @Tag("LockGranted") public record LockGranted() implements MpMessage {}
    @Tag("UnlockChunk") public record UnlockChunk(int channel) implements MpMessage {}
    @Tag("UpdateJob") public record UpdateJob(int channel, UpdateIO updateIO, UpdateOptions options) implements MpMessage {}
    @Tag("CommitJob") public record CommitJob(int channel, long commitVer, int commitChainVer) implements MpMessage {}
    @Tag("JobDone") public record JobDone(IOResult result) implements MpMessage {}
    /** ChunkStore::get by a read task (aioPrepareRead / aioFinishRead) and its answer. */
    @Tag("MetaGet") public record MetaGet(int reader) implements MpMessage {}
    @Tag("MetaRsp") public record MetaRsp(ChunkMetadata meta) implements MpMessage {}

    // The client's invisible read retry.
    @Tag("ReadRetry") public record ReadRetry(int client, int retryCount) implements MpMessage {}
    @Tag("ReadRsp") public record ReadRsp(int client, int retryCount, int code, int target, long updateVer, long commitVer)
            implements MpMessage {}
    @Tag("Never") public record Never() implements MpMessage {}

    // ------------------------------------------------------------------------------------------
    // Program: roles, spawn order, addresses.
    // ------------------------------------------------------------------------------------------
    private final int q;
    private final CommunicationModel model;
    private final String modelName;
    private final List<String> roles;
    /**
     * PORT: which unproductive read retries are cut (NOTES.md). false (default): an answer equal to the
     * preceding answer of the same read from the same target; true (fourth argument "consecutive"): only an
     * answer equal to the immediately preceding attempt's, from the same target (the clarification read
     * literally; the exploration is then infinite, since a read can alternate between two targets).
     */
    private final boolean cutConsecutiveOnly;

    public ChainReplication(String... args) {
        if (args.length < 3 || args.length > 4 || !args[0].equals("impl")
                || (args.length == 4 && !args[3].equals("consecutive"))) {
            throw new IllegalArgumentException("expected impl,<q>,<fifo|bag>[,consecutive]");
        }
        this.cutConsecutiveOnly = args.length == 4;
        this.q = Integer.parseInt(args[1]);
        if (q < 1) {
            throw new IllegalArgumentException("q >= 1");
        }
        this.modelName = args[2];
        this.model = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("fifo or bag, not " + args[2]);
        };
        List<String> r = new ArrayList<>();
        for (int k = 0; k < TARGETS; k++) {
            r.add(node(k));
            r.add(lock(k));
            for (int i = 0; i < CLIENTS; i++) {
                r.add(upd(k, i));
            }
            for (int i = 0; i < CLIENTS; i++) {
                r.add(rd(k, i));
            }
        }
        for (int i = 0; i < CLIENTS; i++) {
            r.add(retry(i));
        }
        for (int i = 0; i < CLIENTS; i++) {
            r.add(client(i));
        }
        this.roles = List.copyOf(r);
    }

    static String node(int k) { return "n" + k; }                    // StorageTarget tk (+ update worker)
    static String lock(int k) { return "n" + k + ".lock"; }           // StorageTarget::chunkLocks_
    static String upd(int k, int i) { return "n" + k + ".upd.c" + i; } // update task of client i's channel
    static String rd(int k, int i) { return "n" + k + ".read.c" + i; } // read task of client i
    static String retry(int i) { return "c" + i + ".retry"; }          // client i's read retry loop
    static String client(int i) { return "c" + i; }

    /** The address of a role: the k-th spawned process (k = 1, 2, ...) is task k + 1 (MpProgram); checked in run(). */
    private Tid tid(String role) {
        int idx = roles.indexOf(role);
        if (idx < 0) {
            throw new IllegalArgumentException(role);
        }
        return Tid.of(idx + 2, role); // idx is 0-based: the first spawned process is task 2
    }

    @Override public String programName() { return "craq"; }
    @Override public String size() { return "q=" + q + "," + modelName + (cutConsecutiveOnly ? ",consecutive" : ""); }
    @Override public CommunicationModel model() { return model; }
    /** The spawned roles: the clients; every other process is an (unnamed) daemon. */
    @Override public List<String> rolesInSpawnOrder() { return List.of(client(0), client(1)); }
    @Override public List<String> visibleRoles() { return List.of(client(0), client(1)); }

    @Override
    public void run() {
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (String role : roles) {
            Runnable body = bodyOf(role);
            // PORT: the never-terminating processes (node tasks, client retry loops) are daemons, so that an
            // execution ending with them blocked on receive is complete (INTERFACE.md: the harness ends it).
            MpProcess.Handle h = isClient(role) ? MpProcess.spawn(role, body) : MpProcess.spawnDaemon(body);
            if (h.tid().raw() != tid(role).raw()) {
                // PORT: addresses are fixed by spawn order instead of an init message; fail loudly if not.
                throw new IllegalStateException("task id of " + role + " is " + h.tid() + ", expected " + tid(role));
            }
            handles.add(h);
        }
        // Main awaits the clients; the daemons run on until they block on receive (MpProcess.runMain).
        for (int i = 0; i < CLIENTS; i++) {
            handles.get(roles.indexOf(client(i))).await();
        }
    }

    static boolean isClient(String role) {
        return role.equals(client(0)) || role.equals(client(1));
    }

    private Runnable bodyOf(String role) {
        for (int k = 0; k < TARGETS; k++) {
            final int kk = k;
            if (role.equals(node(k))) return () -> new StorageTarget(kk).run();
            if (role.equals(lock(k))) return () -> new ChunkLock(kk).run();
            for (int i = 0; i < CLIENTS; i++) {
                final int ii = i;
                if (role.equals(upd(k, i))) return () -> new StorageOperator(kk, ii).runUpdateTask();
                if (role.equals(rd(k, i))) return () -> new StorageOperator(kk, ii).runReadTask();
            }
        }
        for (int i = 0; i < CLIENTS; i++) {
            final int ii = i;
            if (role.equals(retry(i))) return () -> new StorageClientImpl(ii).runRetryLoop();
            if (role.equals(client(i))) return () -> new StorageClientImpl(ii).client(q);
        }
        throw new IllegalArgumentException(role);
    }

    private void send(Tid dst, MpMessage m) {
        Channel.send(model, dst, m);
    }

    private <T extends MpMessage> T recv(Class<T> type) {
        return Channel.recv(model, type);
    }

    // ------------------------------------------------------------------------------------------
    // TargetMap (storage/service/TargetMap.cc): the fixed chain t0 -> t1 -> t2.
    // ------------------------------------------------------------------------------------------
    /** Target: isHead (TargetMap.cc l.193), successor (l.221-250), isTail (l.254). */
    record Target(int targetId, boolean isHead, boolean isTail, Integer successor, int chainVer) {}

    static final class TargetMap {
        // PORT: getByChainId of one fixed chain whose version never changes; never fails.
        static Target getByChainId(int targetId) {
            boolean isHead = targetId == 0;
            Integer successor = targetId + 1 < TARGETS ? targetId + 1 : null;
            boolean isTail = successor == null;
            return new Target(targetId, isHead, isTail, successor, CHAIN_VER);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Client: client/storage/StorageClientImpl.cc, TargetSelection.cc.
    // ------------------------------------------------------------------------------------------
    enum TargetSelectionMode { Default, LoadBalance, HeadTarget }

    /** TargetSelectionStrategy (TargetSelection.cc). */
    static final class TargetSelectionStrategy {
        final TargetSelectionMode mode;

        TargetSelectionStrategy(TargetSelectionMode mode) {
            this.mode = mode;
        }

        static TargetSelectionStrategy create(TargetSelectionMode mode) {
            return new TargetSelectionStrategy(mode);
        }

        int selectTarget(int[] servingTargets) {
            if (mode == TargetSelectionMode.HeadTarget) {
                return servingTargets[0]; // HeadTargetStrategy: chain.servingTargets.front()
            }
            // LoadBalanceStrategy: a random preferred target, then the least loaded node.
            // PORT: INTERFACE.md fixes LoadBalance as a nondeterministic choice among the serving targets.
            return servingTargets[Nondet.chooseIndex(servingTargets.length)];
        }
    }

    final class StorageClientImpl {
        final int i;

        StorageClientImpl(int i) {
            this.i = i;
        }

        /** The client loop of INTERFACE.md. */
        void client(int q) {
            for (int j = 0; j < q; j++) {
                if ((i + j) % 2 == 0) {
                    batchWriteWithRetry(value(i, j));
                } else {
                    batchReadWithRetry();
                }
            }
        }

        /** selectServingTargets l.419: every target serving, in chain order. */
        int[] selectServingTargets() {
            int[] t = new int[TARGETS];
            for (int k = 0; k < TARGETS; k++) {
                t[k] = k;
            }
            return t;
        }

        /** selectRoutingTargetForOps l.490: the routing target of the (single) op by the strategy. */
        int selectRoutingTargetForOps(TargetSelectionMode mode) {
            TargetSelectionStrategy targetSelectionStrategy = TargetSelectionStrategy.create(mode);
            int[] servingTargets = selectServingTargets();
            return targetSelectionStrategy.selectTarget(servingTargets);
        }

        /** batchWriteWithRetry l.1771 (sendOpsWithRetry l.1143): no error reaches it here, one attempt. */
        void batchWriteWithRetry(long v) {
            // PORT: sendOpsWithRetry's loop is not needed: every write succeeds at the first attempt
            // (no routing changes, no timeouts); an error is a failure of the run (StorageOperator.write).
            batchWriteWithoutRetry(v);
        }

        /** batchWriteWithoutRetry l.1799: Default mode = HeadTarget (l.1805). */
        void batchWriteWithoutRetry(long v) {
            TargetSelectionMode mode = TargetSelectionMode.Default == TargetSelectionMode.Default
                    ? TargetSelectionMode.HeadTarget : TargetSelectionMode.Default;
            int target = selectRoutingTargetForOps(mode);
            sendWriteRequest(target, v);
        }

        /** sendWriteRequest l.1860: WriteReq with updateVer 0, the full-chunk write; waits for the WriteRsp. */
        void sendWriteRequest(int target, long v) {
            send(tid(upd(target, i)), new Write(i, v)); // PORT: the visible Write{client, value} is the WriteReq
            Ack ack = recv(Ack.class);
            if (ack.client() != i) {
                throw new AssertionError("Ack for another client");
            }
        }

        /**
         * batchReadWithRetry l.1577 / sendOpsWithRetry l.1143: the first attempt here; a kChunkNotCommit
         * answer (isFastRetryError l.166) is retried at once by runRetryLoop, and the final content arrives
         * here.
         */
        void batchReadWithRetry() {
            batchReadWithoutRetry(0);
            // PORT: the retries of sendOpsWithRetry run in the process ci.retry (invisible), see NOTES.md.
            Value v = recv(Value.class);
            if (v.client() != i) {
                throw new AssertionError("Value for another client");
            }
        }

        /** batchReadWithoutRetry l.1642: Default mode = LoadBalance (l.1645); the BatchReadReq to the target's node. */
        void batchReadWithoutRetry(int retryCount) {
            TargetSelectionMode mode = TargetSelectionMode.Default == TargetSelectionMode.Default
                    ? TargetSelectionMode.LoadBalance : TargetSelectionMode.Default;
            int target = selectRoutingTargetForOps(mode);
            if (retryCount == 0) {
                send(tid(rd(target, i)), new Read(i));
            } else {
                send(tid(rd(target, i)), new ReadRetry(i, retryCount)); // PORT: invisible retry of the same ReadIO
            }
        }

        /** The retry part of sendOpsWithRetry for reads: on kChunkNotCommit, select again and resend at once. */
        void runRetryLoop() {
            ReadRsp previous = null;                       // the immediately preceding answer of this read
            ReadRsp[] previousAt = new ReadRsp[TARGETS];   // the preceding answer of this read from each target
            while (true) {
                ReadRsp rsp = recv(ReadRsp.class);
                if (rsp.retryCount() == 0) {
                    previous = null; // the first answer of a new read operation
                    previousAt = new ReadRsp[TARGETS];
                }
                if (rsp.code() == kChunkNotCommit) { // isFastRetryError: waitTime only shortened; PORT: no wait
                    ReadRsp compareWith = cutConsecutiveOnly ? previous : previousAt[rsp.target()];
                    if (compareWith != null && compareWith.target() == rsp.target()
                            && compareWith.updateVer() == rsp.updateVer() && compareWith.commitVer() == rsp.commitVer()) {
                        // PORT (coordinator's clarification, INTERFACE.md): the same not-committed answer from the
                        // same target as the immediately preceding attempt: the execution is cut as unproductive.
                        // The API has no assume(false): this process waits for a message nobody sends, and the
                        // client stays blocked on its Value, so the execution counts as blocked.
                        recv(Never.class);
                    }
                    previous = rsp;
                    previousAt[rsp.target()] = rsp;
                    batchReadWithoutRetry(rsp.retryCount() + 1);
                } else {
                    throw new AssertionError("unexpected read error " + rsp.code());
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // The chunk lock: StorageTarget::lockChunk (StorageTarget.h l.61), CoLockManager chunkLocks_.
    // ------------------------------------------------------------------------------------------
    final class ChunkLock {
        final int k;

        ChunkLock(int k) {
            this.k = k;
        }

        void run() {
            while (true) {
                LockChunk req = recv(LockChunk.class); // any waiting update task (arrival order)
                final int owner = req.channel();
                send(tid(upd(k, owner)), new LockGranted());
                Channel.recv(model, UnlockChunk.class, u -> u.channel() == owner);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // StorageTarget + UpdateWorker + ChunkReplica (storage/store, storage/update): owns the metadata.
    // ------------------------------------------------------------------------------------------
    static ChunkMetadata initialMeta() {
        // PORT: the chunk exists at every target, committed at version 0 (INTERFACE.md).
        return new ChunkMetadata(0, 0, CHAIN_VER, ChunkState.CLEAN, 0);
    }

    /** Mutable working copy of ChunkMetadata (the ChunkInfo copy that ChunkReplica edits before store.set). */
    static final class Meta {
        long updateVer;
        long commitVer;
        int chainVer;
        ChunkState chunkState;
        long content;

        Meta(ChunkMetadata m) {
            updateVer = m.updateVer();
            commitVer = m.commitVer();
            chainVer = m.chainVer();
            chunkState = m.chunkState();
            content = m.content();
        }

        ChunkMetadata freeze() {
            return new ChunkMetadata(updateVer, commitVer, chainVer, chunkState, content);
        }

        long checksum() {
            return content; // PORT: the content stands for its checksum
        }
    }

    final class StorageTarget {
        final int k;
        ChunkMetadata stored = initialMeta(); // the chunk store's metadata of the one chunk

        StorageTarget(int k) {
            this.k = k;
        }

        /**
         * UpdateWorker::run: dequeue a job, StorageTarget::updateChunk, complete the job; and the chunk store's
         * metadata lookups (ChunkStore::get) of the read path, served at once.
         */
        void run() {
            while (true) {
                MpMessage job = Channel.recv(model,
                        m -> m instanceof UpdateJob || m instanceof CommitJob || m instanceof MetaGet);
                if (job instanceof MetaGet g) {
                    // PORT: store.get of aioPrepareRead / aioFinishRead, a request to the metadata's owner.
                    send(tid(rd(k, g.reader())), new MetaRsp(stored));
                    continue;
                }
                IOResult result;
                int channel;
                if (job instanceof UpdateJob u) {
                    channel = u.channel();
                    result = ChunkReplica_update(u);
                } else {
                    CommitJob c = (CommitJob) job;
                    channel = c.channel();
                    result = ChunkReplica_commit(c);
                }
                send(tid(upd(k, channel)), new JobDone(result)); // job.complete()
            }
        }

        /** store.set: the metadata held by this process; read tasks see it at their next MetaGet (NOTES.md). */
        void storeSet(Meta meta) {
            stored = meta.freeze();
        }

        /** ChunkReplica::update l.132-318. */
        IOResult ChunkReplica_update(UpdateJob job) {
            UpdateIO writeIO = job.updateIO();
            UpdateOptions options = job.options();
            int jobCommitChainVer = writeIO.chainVer(); // UpdateJob::commitChainVer (not syncing, not commit)

            // 1. get meta info. PORT: the chunk always exists (no create, no remove).
            Meta meta = new Meta(stored);

            // 2. begin to write.
            long rCommitVer = meta.commitVer;
            long rUpdateVer = meta.updateVer;
            long rChecksum = meta.checksum();
            int rCommitChainVer = meta.chainVer;
            if (meta.chunkState == ChunkState.DIRTY && !options.isSyncing()) {
                return new IOResult(kChunkNotClean, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
            }
            if (jobCommitChainVer < meta.chainVer && meta.chunkState == ChunkState.COMMIT) {
                return new IOResult(kChainVersionMismatch, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
            }
            if (writeIO.data() != writeIO.checksum()) { // ChecksumInfo::create(data) != writeIO.checksum
                return new IOResult(kChecksumMismatch, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
            }

            if (options.isSyncing()) {
                // PORT: no syncing in this study.
                throw new AssertionError("syncing update");
            } else if (writeIO.updateVer() > 0) {
                if (writeIO.updateVer() <= meta.commitVer) {
                    return new IOResult(kChunkCommittedUpdate, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
                } else if (writeIO.updateVer() <= meta.updateVer) {
                    return new IOResult(kChunkStaleUpdate, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
                } else if (writeIO.updateVer() > meta.updateVer + 1) {
                    return new IOResult(kChunkMissingUpdate, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
                }
                meta.updateVer = writeIO.updateVer();
            } else {
                meta.updateVer += 1;
                if (meta.updateVer > meta.commitVer + 1) {
                    return new IOResult(kChunkAdvanceUpdate, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
                }
            }

            meta.chunkState = ChunkState.DIRTY;
            meta.chainVer = jobCommitChainVer;
            // PORT: the first store.set (state DIRTY, new updateVer) is merged with the second below: a read
            // sees updateVer != commitVer in both and answers kChunkNotCommit either way (NOTES.md).
            rCommitChainVer = meta.chainVer;
            rUpdateVer = meta.updateVer;

            // 3. do write operation: the full-chunk write (doRealWrite).
            meta.content = writeIO.data();
            // updateChecksum: offset 0 and length == size: the write's checksum is the chunk's.

            // 4. finish to write.
            meta.chunkState = ChunkState.CLEAN;
            storeSet(meta);
            rChecksum = meta.checksum();
            rCommitVer = meta.commitVer;
            rCommitChainVer = meta.chainVer;
            return new IOResult(kOK, rUpdateVer, rCommitVer, rChecksum, rCommitChainVer);
        }

        /** ChunkReplica::commit l.397-469. */
        IOResult ChunkReplica_commit(CommitJob commitIO) {
            int jobCommitChainVer = commitIO.commitChainVer(); // UpdateJob::commitChainVer for COMMIT
            Meta meta = new Meta(stored);
            long rCommitVer = meta.commitVer;
            long rUpdateVer = meta.updateVer;
            int rCommitChainVer = meta.chainVer;

            if (jobCommitChainVer < meta.chainVer) {
                return new IOResult(kChainVersionMismatch, rUpdateVer, rCommitVer, 0, rCommitChainVer);
            }
            if (commitIO.commitVer() > meta.updateVer) {
                return new IOResult(kChunkVersionMismatch, rUpdateVer, rCommitVer, 0, rCommitChainVer);
            }

            // PORT: commitIO.isForce is never set (no sync).
            if (meta.chunkState == ChunkState.DIRTY) {
                return new IOResult(kChunkNotClean, rUpdateVer, rCommitVer, 0, rCommitChainVer);
            } else if (meta.commitVer < commitIO.commitVer()) {
                meta.commitVer = commitIO.commitVer();
            } else {
                rCommitVer = meta.commitVer;
                rCommitChainVer = meta.chainVer;
                return new IOResult(kChunkStaleCommit, rUpdateVer, rCommitVer, 0, rCommitChainVer);
            }

            if (meta.commitVer == meta.updateVer) {
                meta.chunkState = ChunkState.COMMIT;
                meta.chainVer = jobCommitChainVer;
            }
            storeSet(meta); // PORT: never readyToRemove (no remove)
            rCommitVer = meta.commitVer;
            rCommitChainVer = meta.chainVer;
            return new IOResult(kOK, rUpdateVer, rCommitVer, 0, rCommitChainVer);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Storage service: StorageOperator, ReliableUpdate, ReliableForwarding (storage/service).
    // One instance per logical task: an update task (client i's channel at target k) or a read task.
    // ------------------------------------------------------------------------------------------
    /** CommitIO (fbs): key, commitVer, commitChainVer, isRemove (false), isSyncing (false); filled by forward. */
    static final class CommitIO {
        long commitVer;
        int commitChainVer;
        boolean isRemove = false;
        boolean isSyncing = false;
    }

    /** ReliableUpdate's ReqResult of a client channel (ReliableUpdate.h). */
    static final class ReqResult {
        long channelSeqnum = 0;
        IOResult updateResult = null;
        long succUpdateVer = 0;
    }

    final class StorageOperator {
        final int k;   // the target hosted by this node
        final int ch;  // the client whose channel (update task) or reads (read task) this task serves
        long headSeqnum = 0;
        final ReqResult reqResult = new ReqResult(); // ReliableUpdate: clientStatus->channelMap[(chain, channel)]

        StorageOperator(int k, int ch) {
            this.k = k;
            this.ch = ch;
        }

        // ---------------- update task ----------------

        void runUpdateTask() {
            while (true) {
                MpMessage m = Channel.recv(model, x -> x instanceof Write || x instanceof UpdateReq);
                if (m instanceof Write w) {
                    IOResult result = write(w);
                    if (!result.ok()) {
                        // PORT: the client's retry of a failed write is not modelled; no error can occur here.
                        throw new AssertionError("write failed with code " + result.code());
                    }
                    send(tid(client(w.client())), new Ack(w.client())); // WriteRsp
                } else {
                    UpdateReq req = (UpdateReq) m;
                    UpdateRsp rsp = update(req);
                    send(tid(upd(k - 1, ch)), rsp); // reply to the predecessor's forward (doForward)
                }
            }
        }

        /** StorageOperator::write l.233: the client's WriteReq becomes an UpdateReq with fromClient. */
        IOResult write(Write req) {
            Target target = TargetMap.getByChainId(k);
            // PORT: the client's channel seqnum and request id are not in the fixed Write message; the
            // head counts the client's writes on its channel instead.
            UpdateReq updateReq = new UpdateReq(
                    new UpdateIO(0 /*updateVer*/, req.value(), req.value() /*checksum*/, CHAIN_VER),
                    new UpdateOptions(true /*fromClient*/, false),
                    new MessageTag(req.client(), ++headSeqnum),
                    0);
            return ReliableUpdate_update(updateReq, target);
        }

        /** StorageOperator::update l.284: an UpdateReq forwarded by the predecessor. */
        UpdateRsp update(UpdateReq req) {
            Target target = TargetMap.getByChainId(k);
            // PORT: updateType is always WRITE, so never the REMOVE shortcut to handleUpdate.
            IOResult result = ReliableUpdate_update(req, target);
            return new UpdateRsp(req.tag(), result);
        }

        /** ReliableUpdate::update l.16-128. */
        IOResult ReliableUpdate_update(UpdateReq req, Target target) {
            // 1. channel id valid (the client index + 1 stands for it). 2. get cached: this task's reqResult.
            // 3. lock channel. PORT: tryLockChannel always succeeds: this task is the only user of the channel.
            if (req.tag().seqnum() < reqResult.channelSeqnum) {
                return makeError(kDuplicateUpdate);
            }
            // 4. return cached result.
            if (req.tag().seqnum() == reqResult.channelSeqnum && reqResult.updateResult != null) {
                if (reqResult.updateResult.ok()) {
                    if (req.payload().updateVer() == 0 || req.payload().updateVer() == reqResult.updateResult.updateVer()) {
                        return reqResult.updateResult;
                    }
                } else if (req.payload().updateVer() == 0 && reqResult.succUpdateVer != 0) {
                    req = new UpdateReq(new UpdateIO(reqResult.succUpdateVer, req.payload().data(),
                            req.payload().checksum(), req.payload().chainVer()), req.options(), req.tag(), req.retryCount());
                }
            }
            // 5. start a new task.
            UpdateReq[] reqRef = {req};
            IOResult updateResult = handleUpdate(reqRef, target);
            reqResult.channelSeqnum = req.tag().seqnum();
            reqResult.updateResult = updateResult;
            reqResult.succUpdateVer = reqRef[0].payload().updateVer();
            return updateResult;
        }

        /** StorageOperator::handleUpdate l.333-520; req is in/out (updateVer set by the head). */
        IOResult handleUpdate(UpdateReq[] reqRef, Target target) {
            UpdateReq req = reqRef[0];
            // 1. get target.
            if (req.options().fromClient() && !target.isHead()) {
                return makeError(kRoutingError);
            }
            // PORT: read_only, empty chunk id, syncing truncate/extend checks cannot hold here.

            // 2. lock chunk.
            send(tid(lock(k)), new LockChunk(ch));
            recv(LockGranted.class);
            // PORT: the lock guard's release at every co_return is the UnlockChunk after handleUpdateLocked.
            IOResult result = handleUpdateLocked(reqRef, target);
            send(tid(lock(k)), new UnlockChunk(ch));
            return result;
        }

        IOResult handleUpdateLocked(UpdateReq[] reqRef, Target target) {
            UpdateReq req = reqRef[0];
            // re-check chain version after acquiring the lock.
            target = TargetMap.getByChainId(k);

            // 3. update local target.
            IOResult updateResult = doUpdate(req.payload(), req.options());

            int code = updateResult.code();
            if (code == 0) {
                // 1. write success.
                if (req.payload().updateVer() == 0) {
                    req = new UpdateReq(new UpdateIO(updateResult.updateVer(), req.payload().data(),
                            req.payload().checksum(), req.payload().chainVer()), req.options(), req.tag(), req.retryCount());
                    reqRef[0] = req;
                } else if (req.payload().updateVer() != updateResult.updateVer()) {
                    return makeError(kChunkVersionMismatch);
                }
            } else if (code == kChunkMissingUpdate) {
                return updateResult;
            } else if (code == kChunkCommittedUpdate) {
                return new IOResult(kOK, req.payload().updateVer(), req.payload().updateVer(),
                        updateResult.checksum(), updateResult.commitChainVer());
            } else if (code == kChunkStaleUpdate) {
                updateResult = new IOResult(kOK, req.payload().updateVer(), updateResult.commitVer(),
                        updateResult.checksum(), updateResult.commitChainVer());
            } else if (code == kChunkAdvanceUpdate) {
                return updateResult;
            } else {
                return updateResult;
            }

            // 4. forward to successor.
            CommitIO commitIO = new CommitIO();
            commitIO.commitVer = updateResult.updateVer();
            commitIO.isRemove = false;

            Target[] targetRef = {target};
            IOResult forwardResult = ReliableForwarding_forwardWithRetry(req, targetRef, commitIO);
            target = targetRef[0];
            if (commitIO.commitVer != updateResult.updateVer()) {
                return makeError(kChunkVersionMismatch);
            }

            if (forwardResult.ok()) {
                if (commitIO.isRemove) {
                    // not reached (no remove)
                } else if (forwardResult.checksum() != updateResult.checksum()) {
                    return makeError(kChecksumMismatch);
                }
            } else if (forwardResult.code() != kNoSuccessorTarget) {
                return forwardResult;
            }

            // 5. commit.
            IOResult commitResult = doCommit(commitIO);
            code = commitResult.code();
            if (code == 0) {
                // 1. commit success.
            } else if (code == kChunkStaleCommit) {
                // 2. stale commit, considered as a successful commit.
                commitResult = commitResult.withCommitVer(updateResult.updateVer());
            } else {
                // 3. commit fail.
                return commitResult;
            }

            // commitResult.lengthInfo = updateResult.lengthInfo; commitResult.checksum = updateResult.checksum;
            commitResult = commitResult.withCode(kOK).withChecksum(updateResult.checksum());
            return commitResult;
        }

        /** StorageOperator::doUpdate l.521: an UpdateJob to the update worker, then job.complete(). */
        IOResult doUpdate(UpdateIO updateIO, UpdateOptions updateOptions) {
            // PORT: no RDMA buffers or inline data: the content travels in UpdateIO.
            send(tid(node(k)), new UpdateJob(ch, updateIO, updateOptions));
            return recv(JobDone.class).result();
        }

        /** StorageOperator::doCommit l.616. */
        IOResult doCommit(CommitIO commitIO) {
            send(tid(node(k)), new CommitJob(ch, commitIO.commitVer, commitIO.commitChainVer));
            return recv(JobDone.class).result();
        }

        /** ReliableForwarding::forwardWithRetry l.33. */
        IOResult ReliableForwarding_forwardWithRetry(UpdateReq req, Target[] target, CommitIO commitIO) {
            for (int retryCount = 0; ; ++retryCount) {
                target[0] = TargetMap.getByChainId(k);
                IOResult ioResult = ReliableForwarding_forward(req, retryCount, target[0], commitIO);
                if (ioResult.ok()) {
                    return ioResult;
                } else if (ioResult.code() == kNoSuccessorTarget) {
                    return ioResult;
                }
                // PORT: no timeouts and no back-off; no other error can come back, and one would be returned.
                return ioResult;
            }
        }

        /** ReliableForwarding::forward l.106. */
        IOResult ReliableForwarding_forward(UpdateReq req, int retryCount, Target target, CommitIO commitIO) {
            if (target.successor() == null) {
                // use the latest chain version.
                commitIO.commitChainVer = target.chainVer();
                return makeError(kNoSuccessorTarget);
            }
            IOResult ioResult = ReliableForwarding_doForward(req, retryCount, target, commitIO);
            if (ioResult.ok()) {
                commitIO.commitVer = ioResult.commitVer();
                // use successor's chain version.
                commitIO.commitChainVer = ioResult.commitChainVer();
                if (ioResult.commitChainVer() > target.chainVer()) {
                    return makeError(kChainVersionMismatch);
                }
            }
            return ioResult;
        }

        /** ReliableForwarding::doForward l.138-280: the UpdateReq to the successor with fromClient = false. */
        IOResult ReliableForwarding_doForward(UpdateReq req, int retryCount, Target target, CommitIO commitIO) {
            UpdateReq updateReq = new UpdateReq(
                    new UpdateIO(req.payload().updateVer(), req.payload().data(), req.payload().checksum(), target.chainVer()),
                    new UpdateOptions(false /*fromClient*/, false),
                    req.tag(),
                    retryCount);
            commitIO.isSyncing = false; // PORT: the successor is always SERVING
            send(tid(upd(target.successor(), ch)), updateReq); // messenger.update(successorAddr, updateReq)
            UpdateRsp updateResult = recv(UpdateRsp.class);
            if (updateResult.result().ok()) {
                if (target.chainVer() < updateResult.result().commitChainVer()) {
                    return makeError(kChainVersionMismatch);
                }
            }
            return updateResult.result();
        }

        // ---------------- read task ----------------

        /** store.get: PORT: the metadata is asked of its owner, the target process nk (NOTES.md). */
        ChunkMetadata storeGet() {
            send(tid(node(k)), new MetaGet(ch));
            return recv(MetaRsp.class).meta();
        }

        void runReadTask() {
            while (true) {
                MpMessage m = Channel.recv(model, x -> x instanceof Read || x instanceof ReadRetry);
                int client = m instanceof Read r ? r.client() : ((ReadRetry) m).client();
                int retryCount = m instanceof ReadRetry rr ? rr.retryCount() : 0;
                long[] content = new long[1];
                long[] resultVer = new long[2];
                int code = batchRead(content, resultVer);
                if (code == kOK) {
                    send(tid(client(client)), new Value(client, content[0])); // BatchReadRsp with the data
                } else {
                    // BatchReadRsp with kChunkNotCommit; the IOResult carries the versions read (commitVer, updateVer)
                    send(tid(retry(client)), new ReadRsp(client, retryCount, code, k, resultVer[1], resultVer[0]));
                }
            }
        }

        /** StorageOperator::batchRead l.82-233 for the one ReadIO, with the AIO read worker's job. */
        int batchRead(long[] content, long[] resultVer) {
            Target target = TargetMap.getByChainId(k); // upToDate: always serving
            boolean readUncommitted = false; // ALLOW_READ_UNCOMMITTED is not set by the client (l.129)
            // AioReadWorker: aioPrepareRead, the read of the data, aioFinishRead.
            int code = aioPrepareRead(readUncommitted, resultVer);
            if (code != kOK) {
                return code;
            }
            // The AIO read of the data runs between prepare and finish; an update may land meanwhile, which
            // aioFinishRead's own lookup observes. PORT: the data read is the content of the version prepared
            // (an update changes the content only together with updateVer, which aioFinishRead then rejects).
            content[0] = preparedContent;
            return aioFinishRead(resultVer);
        }

        long preparedContent;

        /** ChunkReplica::aioPrepareRead l.38-78. */
        int aioPrepareRead(boolean readUncommitted, long[] result) {
            // 1. get meta info.
            ChunkMetadata meta = storeGet();
            preparedContent = meta.content();
            result[0] = meta.commitVer();
            result[1] = meta.updateVer();
            if (result[0] != result[1] && !readUncommitted) {
                return kChunkNotCommit;
            }
            return kOK;
        }

        /** ChunkReplica::aioFinishRead l.79-131. */
        int aioFinishRead(long[] result) {
            // 1. get meta info.
            ChunkMetadata meta = storeGet();
            if (result[1] != meta.updateVer()) {
                return kChunkNotCommit;
            }
            return kOK;
        }
    }

    @Override
    public String toString() {
        return "ChainReplication(q=" + q + "," + modelName + ")";
    }
}
