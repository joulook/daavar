package bench.tikv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntFunction;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/**
 * Java port of TiKV's distributed transactions: the client of tikv/client-rust 1b883bf
 * (Transaction, Committer, LockResolver / resolve_locks, the request plans) and the server actions
 * of tikv/tikv 96033a2 (storage/txn/commands, storage/txn/actions, the MVCC reader/writer as the
 * per-key cell of INTERFACE.md section 6). Roles c1..cm (visible), t1..tm, x1..xm, pd, r1..rR, init
 * (invisible daemons). Every line that differs from the source is marked PORT with its reason; the
 * decisions are in NOTES.md.
 */
public final class TxnKv implements MpProgram {

    // ===================================================================================
    // Visible messages (INTERFACE.md section 4, verbatim)
    // ===================================================================================
    @Tag("Begin") public record Begin() implements MpMessage {}
    @Tag("Get") public record Get(int key) implements MpMessage {}
    @Tag("GetForUpdate") public record GetForUpdate(int key) implements MpMessage {}
    @Tag("Put") public record Put(int key, int value) implements MpMessage {}
    @Tag("Commit") public record Commit() implements MpMessage {}
    @Tag("Begun") public record Begun() implements MpMessage {}
    @Tag("Value") public record Value(int value) implements MpMessage {}
    @Tag("Done") public record Done() implements MpMessage {}
    @Tag("Committed") public record Committed() implements MpMessage {}
    @Tag("Aborted") public record Aborted() implements MpMessage {}

    // ===================================================================================
    // Scenarios (INTERFACE.md section 3, the same table as the Rust program)
    // ===================================================================================
    enum Kind { OPTIMISTIC, PESSIMISTIC }

    /** One operation of a script. */
    record Op(int type, int key) {
        static final int GET = 0;
        static final int GET_FOR_UPDATE = 1;
        static final int PUT = 2;

        boolean isPut() {
            return type == PUT;
        }

        boolean isGetForUpdate() {
            return type == GET_FOR_UPDATE;
        }
    }

    record ClientScript(Kind kind, List<Op> ops) {}

    record Scenario(int n, List<ClientScript> clients) {}

    static Op get(int k) {
        return new Op(Op.GET, k);
    }

    static Op put(int k) {
        return new Op(Op.PUT, k);
    }

    static Scenario scenario(String name) {
        Kind o = Kind.OPTIMISTIC;
        Kind p = Kind.PESSIMISTIC;
        switch (name) {
            case "S1":
                return new Scenario(1, List.of(new ClientScript(p, List.of(put(1))),
                        new ClientScript(o, List.of(get(1), put(1)))));
            case "X2":
                return new Scenario(2, List.of(new ClientScript(o, List.of(put(1), put(2))),
                        new ClientScript(o, List.of(put(2), put(1)))));
            case "P2":
                return new Scenario(2, List.of(new ClientScript(p, List.of(put(1), put(2))),
                        new ClientScript(p, List.of(put(2), put(1)))));
            case "X2r":
                return new Scenario(2, List.of(new ClientScript(o, List.of(get(1), get(2), put(1), put(2))),
                        new ClientScript(o, List.of(get(2), get(1), put(2), put(1)))));
            case "M2":
                return new Scenario(2, List.of(new ClientScript(p, List.of(put(1), put(2))),
                        new ClientScript(o, List.of(get(1), get(2), put(1), put(2)))));
            case "L1":
                return new Scenario(1, List.of(new ClientScript(o, List.of(get(1), put(1))),
                        new ClientScript(o, List.of(get(1), put(1)))));
            case "A3":
                return new Scenario(2, List.of(new ClientScript(o, List.of(put(1), put(2))),
                        new ClientScript(o, List.of(get(2))),
                        new ClientScript(o, List.of(get(1), get(2)))));
            default:
                throw new IllegalArgumentException("unknown scenario " + name);
        }
    }

    // ===================================================================================
    // Client code (INTERFACE.md section 5.1, verbatim)
    // ===================================================================================
    static void client(CommunicationModel md, int i, List<Op> script, Tid t) {
        Channel.send(md, t, new Begin());
        if (!(Channel.recv(md) instanceof Begun)) throw new AssertionError("Begun expected");
        for (Op op : script) {
            if (op.isPut()) {
                Channel.send(md, t, new Put(op.key(), i));
                MpMessage r = Channel.recv(md);
                if (r instanceof Aborted) return;
                if (!(r instanceof Done)) throw new AssertionError("Done or Aborted expected, got " + r);
            } else {
                Channel.send(md, t, op.isGetForUpdate() ? new GetForUpdate(op.key()) : new Get(op.key()));
                MpMessage r = Channel.recv(md);
                if (r instanceof Aborted) return;
                if (!(r instanceof Value)) throw new AssertionError("Value or Aborted expected, got " + r);
            }
        }
        Channel.send(md, t, new Commit());
        MpMessage r = Channel.recv(md);
        if (!(r instanceof Committed) && !(r instanceof Aborted)) throw new AssertionError("Committed or Aborted expected, got " + r);
    }

    // ===================================================================================
    // Program parameters and entry
    // ===================================================================================
    private final String scenarioName;
    private final boolean async;
    private final boolean onepc;
    private final int a;
    private final CommunicationModel md;
    private final String fifoBag;
    private final String variant;
    private final Scenario sc;

    /**
     * Constructed from {@code impl,<scenario>,<async>,<onepc>,<a>,<fifo|bag>[,<variant>]}.
     *
     * @param args the launcher's comma-separated arguments
     */
    public TxnKv(String... args) {
        if (args.length < 6 || args.length > 7 || !args[0].equals("impl")) {
            throw new IllegalArgumentException(
                    "expected impl,<scenario>,<async>,<onepc>,<a>,<fifo|bag>[,<variant>]");
        }
        this.scenarioName = args[1];
        this.sc = scenario(args[1]);
        this.async = parse01(args[2], "async");
        this.onepc = parse01(args[3], "onepc");
        this.a = Integer.parseInt(args[4]);
        if (a < 1) throw new IllegalArgumentException("a must be >= 1, was " + a);
        if (async && onepc) throw new IllegalArgumentException("async = 1 with onepc = 1 is not an instance");
        this.fifoBag = args[5];
        if (fifoBag.equals("fifo")) {
            md = CommunicationModel.P2P;
        } else if (fifoBag.equals("bag")) {
            md = CommunicationModel.ASYN;
        } else {
            throw new IllegalArgumentException("fifo or bag expected, got " + fifoBag);
        }
        this.variant = args.length > 6 ? args[6] : "correct";
        if (!variant.equals("correct") && !variant.equals("f1") && !variant.equals("f2")) {
            throw new IllegalArgumentException("unknown variant " + variant);
        }
    }

    private static boolean parse01(String s, String what) {
        if (s.equals("0")) return false;
        if (s.equals("1")) return true;
        throw new IllegalArgumentException(what + " must be 0 or 1, was " + s);
    }

    @Override
    public String programName() {
        return "tikv_txn";
    }

    @Override
    public String size() {
        return scenarioName + "," + (async ? 1 : 0) + "," + (onepc ? 1 : 0) + "," + a;
    }

    @Override
    public CommunicationModel model() {
        return md;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        for (int i = 1; i <= sc.clients().size(); i++) r.add("c" + i);
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        return rolesInSpawnOrder();
    }

    /** The setup message of {@code init} (INTERFACE.md section 2). */
    record Setup(List<Tid> regions, Tid pd, List<Tid> xs, List<Tid> ts, List<Tid> cs, List<Integer> regionOfKey,
            List<Boolean> pessimistic, boolean async, boolean onepc, int a, List<List<Tid>> tTasks,
            List<List<Tid>> xTasks) implements MpMessage {}

    @Override
    public void run() {
        final int m = sc.clients().size();
        final int n = sc.n();
        // Layout: onepc = 1 -> single (R = 1), else split (R = n, key kj on rj) (INTERFACE.md section 1, 2).
        final int regionCount = onepc ? 1 : n;
        List<Integer> regionOfKey = new ArrayList<>();
        regionOfKey.add(-1); // keys are 1..n
        for (int k = 1; k <= n; k++) regionOfKey.add(onepc ? 0 : k - 1);
        final List<Integer> layout = List.copyOf(regionOfKey);
        List<Boolean> pess = new ArrayList<>();
        for (ClientScript cs : sc.clients()) pess.add(cs.kind() == Kind.PESSIMISTIC);
        final List<Boolean> pessimistic = List.copyOf(pess);
        final String var = variant;
        final CommunicationModel model = md;

        List<Tid> regions = new ArrayList<>();
        for (int r = 0; r < regionCount; r++) {
            final int rIdx = r;
            regions.add(MpProcess.spawnDaemon(() -> regionMain(model, rIdx, var)).tid());
        }
        Tid pd = MpProcess.spawnDaemon(() -> pdMain(model)).tid();
        List<Tid> xs = new ArrayList<>();
        for (int i = 1; i <= m; i++) {
            final int idx = i;
            xs.add(MpProcess.spawnDaemon(() -> xMain(model, idx)).tid());
        }
        List<Tid> ts = new ArrayList<>();
        for (int i = 1; i <= m; i++) {
            final int idx = i;
            ts.add(MpProcess.spawnDaemon(() -> tMain(model, idx)).tid());
        }
        List<MpProcess.Handle> cHandles = new ArrayList<>();
        List<Tid> cs = new ArrayList<>();
        for (int i = 1; i <= m; i++) {
            final int idx = i;
            final List<Op> script = sc.clients().get(i - 1).ops();
            final Tid t = ts.get(i - 1);
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> client(model, idx, script, t));
            cHandles.add(h);
            cs.add(h.tid());
        }
        // Round 3 (owner 2026-11-10 08:40, JMC export mode needs main to spawn every process): the shard tasks
        // of Lib.retryMultiRegion are spawned here, one invisible daemon per (ti or xi, region), and each takes
        // its jobs as ShardWork messages. Only a split layout (R >= 2) has plans with two shards; with R = 1
        // every plan has one shard and runs inline, so no task is spawned.
        List<List<Tid>> tTasks = new ArrayList<>();
        List<List<Tid>> xTasks = new ArrayList<>();
        if (regionCount >= 2) {
            final List<Tid> regionTids = List.copyOf(regions);
            for (List<List<Tid>> owners : List.of(tTasks, xTasks)) {
                for (int i = 1; i <= m; i++) {
                    List<Tid> perRegion = new ArrayList<>();
                    for (int r = 0; r < regionCount; r++) {
                        perRegion.add(MpProcess.spawnDaemon(() -> shardTaskMain(model, pd, regionTids, layout)).tid());
                    }
                    owners.add(List.copyOf(perRegion));
                }
            }
        }
        final Setup setup = new Setup(List.copyOf(regions), pd, List.copyOf(xs), List.copyOf(ts), List.copyOf(cs),
                layout, pessimistic, async, onepc, a, List.copyOf(tTasks), List.copyOf(xTasks));
        final List<Tid> targets = new ArrayList<>();
        targets.addAll(regions);
        targets.add(pd);
        targets.addAll(xs);
        targets.addAll(ts);
        final List<Tid> initTargets = List.copyOf(targets);
        MpProcess.spawnDaemon(() -> {
            for (Tid tgt : initTargets) Channel.send(model, tgt, setup);
        });
        for (MpProcess.Handle h : cHandles) h.await();
    }

    // ===================================================================================
    // pd: the timestamp oracle (T1: integer TSO starting at 1)
    // ===================================================================================
    record TsoReq(Tid from, int id) implements MpMessage {}

    record TsoResp(int id, long ts) implements MpMessage {}

    static void pdMain(CommunicationModel md) {
        Channel.recv(md, Setup.class);
        long next = 1;
        while (true) {
            TsoReq q = Channel.recv(md, TsoReq.class);
            Channel.send(md, q.from(), new TsoResp(q.id(), next));
            next++;
        }
    }

    // ===================================================================================
    // Shared constants (client-rust)
    // ===================================================================================
    /** TimeStamp::max() / u64::MAX. */
    static final long TS_MAX = Long.MAX_VALUE; // PORT: u64::MAX as the largest long (timestamps are small)
    static final long MAX_TTL = 20000; // transaction.rs:1125
    static final long DEFAULT_LOCK_TTL = 3000; // transaction.rs:1127
    static final long TXN_COMMIT_BATCH_SIZE = 16 * 1024; // transaction.rs:1132
    static final double TTL_FACTOR = 6000.0; // transaction.rs:1133

    /** backoff.rs: attempt bound kept, delays not used (T4). */
    static final class Backoff {
        final int maxAttempts;
        int currentAttempts;

        Backoff(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        Backoff copy() {
            Backoff b = new Backoff(maxAttempts);
            b.currentAttempts = currentAttempts;
            return b;
        }

        /** next_delay_duration: true for Some(duration) (never slept, T4). */
        boolean nextDelayDuration() {
            if (currentAttempts >= maxAttempts) {
                return false;
            }
            currentAttempts++;
            return true; // PORT: T4, the delay is not computed and never slept
        }

        boolean isNone() {
            return maxAttempts == 0;
        }
    }

    static Backoff defaultRegionBackoff() {
        return new Backoff(10); // backoff.rs:10
    }

    static Backoff optimisticBackoff() {
        return new Backoff(10); // backoff.rs:12
    }

    static Backoff pessimisticBackoff() {
        return new Backoff(10); // backoff.rs:13
    }

    // ===================================================================================
    // kvrpcpb: the invisible request / response messages
    // ===================================================================================
    enum MOp { PUT, PESSIMISTIC_LOCK } // PORT: A2 only Put mutations (and the PessimisticLock op of the lock request)

    enum PessimisticAction { SKIP_PESSIMISTIC_CHECK, DO_PESSIMISTIC_CHECK, DO_CONSTRAINT_CHECK }

    enum Action { NO_ACTION, TTL_EXPIRE_ROLLBACK, LOCK_NOT_EXIST_ROLLBACK, MIN_COMMIT_TS_PUSHED,
        TTL_EXPIRE_PESSIMISTIC_ROLLBACK, LOCK_NOT_EXIST_DO_NOTHING }

    record Mutation(MOp op, int key, int value) {}

    /** kvrpcpb::LockInfo. */
    record LockInfo(int key, int primaryLock, long lockVersion, long lockTtl, long txnSize, MOp lockType,
            long lockForUpdateTs, boolean useAsyncCommit, long minCommitTs, List<Integer> secondaries) {
        LockInfo withTtl(long ttl) {
            return new LockInfo(key, primaryLock, lockVersion, ttl, txnSize, lockType, lockForUpdateTs, useAsyncCommit,
                    minCommitTs, secondaries);
        }
    }

    /** The storage error kinds that reach a response as a kvrpcpb::KeyError. */
    enum ErrKind { KEY_IS_LOCKED, WRITE_CONFLICT, PESSIMISTIC_LOCK_NOT_FOUND, COMMIT_TS_TOO_LARGE, TXN_NOT_FOUND,
        PRIMARY_MISMATCH, COMMIT_TS_EXPIRED, TXN_LOCK_NOT_FOUND, INVALID_TXN_TSO, LOCK_TYPE_NOT_MATCH,
        PESSIMISTIC_LOCK_ROLLED_BACK, COMMITTED, OTHER }

    /**
     * kvrpcpb::KeyError: {@code locked} (KeyIsLocked), {@code conflict} (WriteConflict),
     * {@code txn_not_found}, {@code primary_mismatch} (its lock info in {@code locked}),
     * {@code commit_ts_expired}, otherwise retryable/abort, named by {@code kind}.
     */
    record KeyError(ErrKind kind, LockInfo locked, long startTs, long conflictStartTs, long conflictCommitTs, int key,
            int primary, long minCommitTs, long attemptedCommitTs) {
        static KeyError of(ErrKind kind, long startTs, int key) {
            return new KeyError(kind, null, startTs, 0, 0, key, 0, 0, 0);
        }
    }

    interface KvRequest {}

    interface KvResponse {}

    record GetReq(int key, long version) implements KvRequest {}

    record PrewriteReq(List<Mutation> mutations, int primaryLock, long startVersion, long lockTtl, long txnSize,
            long forUpdateTs, List<PessimisticAction> pessimisticActions, boolean useAsyncCommit,
            List<Integer> secondaries, boolean tryOnePc, long minCommitTs, long maxCommitTs) implements KvRequest {}

    record PessimisticLockReq(List<Mutation> mutations, int primaryLock, long startVersion, long lockTtl,
            long forUpdateTs, boolean isFirstLock, long waitTimeout, boolean returnValues, long minCommitTs)
            implements KvRequest {}

    record CommitReq(List<Integer> keys, long startVersion, long commitVersion) implements KvRequest {}

    record PessimisticRollbackReq(List<Integer> keys, long startVersion, long forUpdateTs) implements KvRequest {}

    record CheckTxnStatusReq(int primaryKey, long lockTs, long callerStartTs, long currentTs,
            boolean rollbackIfNotExist, boolean forceSyncCommit, boolean resolvingPessimisticLock,
            boolean verifyIsPrimary) implements KvRequest {}

    record CheckSecondaryLocksReq(List<Integer> keys, long startVersion) implements KvRequest {}

    record ResolveLockReq(long startVersion, long commitVersion) implements KvRequest {}

    record GetResp(KeyError error, int value, boolean notFound) implements KvResponse {}

    record PrewriteResp(List<KeyError> errors, long minCommitTs, long onePcCommitTs) implements KvResponse {}

    record PessimisticLockResp(List<KeyError> errors, List<Integer> values, List<Boolean> notFounds)
            implements KvResponse {}

    record CommitResp(KeyError error, long commitVersion) implements KvResponse {}

    record PessimisticRollbackResp(List<KeyError> errors) implements KvResponse {}

    record CheckTxnStatusResp(KeyError error, long lockTtl, long commitVersion, Action action, LockInfo lockInfo)
            implements KvResponse {}

    record CheckSecondaryLocksResp(KeyError error, List<LockInfo> locks, long commitTs) implements KvResponse {}

    record ResolveLockResp(KeyError error) implements KvResponse {}

    /** A request to a region, with its reply address and a per-sender id (A5). */
    record KvReq(Tid from, int id, KvRequest body) implements MpMessage {}

    record KvReply(int id, KvResponse body) implements MpMessage {}

    /** The trigger of {@code xi} (transaction.rs:1401-1405 -> README section 4). */
    record XTrigger(long commitTs, long startVersion, List<Integer> mutationKeys, int primaryKey,
            boolean asyncCommit, int lockBackoffMaxAttempts) implements MpMessage {}

    // ===================================================================================
    // Server: a region (A1), its per-key cells (A2) and its max_ts (A3)
    // ===================================================================================
    static void regionMain(CommunicationModel md, int regionIndex, String variant) {
        Setup s = Channel.recv(md, Setup.class);
        Region region = new Region(variant);
        for (int k = 1; k < s.regionOfKey().size(); k++) {
            if (s.regionOfKey().get(k) == regionIndex) region.cells.put(k, new Cell());
        }
        final Tid me = MpProcess.self();
        while (true) {
            // The scheduler's queue: a request, or a NextCommand this region handed to itself.
            MpMessage msg = Channel.recv(md, x -> x instanceof KvReq || x instanceof ResolveLockNext);
            if (msg instanceof ResolveLockNext next) {
                // The write phase, a command of its own (resolve_lock.rs:82-173) on its own snapshot.
                Channel.send(md, next.from(), new KvReply(next.id(), region.kvResolveLockWrite(next)));
                continue;
            }
            KvReq q = (KvReq) msg;
            if (q.body() instanceof ResolveLockReq r) {
                // keys empty -> ResolveLockReadPhase (mod.rs:375), process_read (resolve_lock_readphase.rs:51-113)
                ResolveLockNext next = region.kvResolveLockReadPhase(r, q.from(), q.id());
                if (next == null) {
                    Channel.send(md, q.from(), new KvReply(q.id(), new ResolveLockResp(null))); // ProcessResult::Res
                } else {
                    // ProcessResult::NextCommand (resolve_lock_readphase.rs:109-111): the scheduler runs the write
                    // command later; commands that arrive in between may run first.
                    // PORT: A1, the region's scheduler queue is its own mailbox (a message to itself)
                    Channel.send(md, me, next);
                }
                continue;
            }
            KvResponse resp = region.handle(q.body());
            Channel.send(md, q.from(), new KvReply(q.id(), resp));
        }
    }

    /** One (key, lock) pair a ResolveLockReadPhase scan found (only the lock's ts and type are read later). */
    record KeyLock(int key, long ts, boolean pessimisticLock) {}

    /** The ResolveLock command a read phase returns as NextCommand (resolve_lock_readphase.rs:101-108). */
    record ResolveLockNext(Tid from, int id, long startVersion, long commitVersion, List<KeyLock> keyLocks)
            implements MpMessage {}

    enum LockType { PUT, PESSIMISTIC } // PORT: A2 no Delete, Lock, Shared

    /** txn_types::Lock (the cell's lock); copied on every load and store, as the source parses bytes. */
    static final class Lock {
        LockType lockType;
        int primary;
        long ts;
        long ttl;
        long forUpdateTs;
        long txnSize;
        long minCommitTs;
        boolean useAsyncCommit;
        boolean useOnePc;
        List<Integer> secondaries = List.of();
        List<Long> rollbackTs = List.of();
        boolean isLockedWithConflict;

        /** Lock::new (txn_types lock.rs). */
        static Lock create(LockType lockType, int primary, long ts, long ttl, long forUpdateTs, long txnSize,
                long minCommitTs) {
            Lock l = new Lock();
            l.lockType = lockType;
            l.primary = primary;
            l.ts = ts;
            l.ttl = ttl;
            l.forUpdateTs = forUpdateTs;
            l.txnSize = txnSize;
            l.minCommitTs = minCommitTs;
            return l;
        }

        Lock copy() {
            Lock l = create(lockType, primary, ts, ttl, forUpdateTs, txnSize, minCommitTs);
            l.useAsyncCommit = useAsyncCommit;
            l.useOnePc = useOnePc;
            l.secondaries = secondaries;
            l.rollbackTs = rollbackTs;
            l.isLockedWithConflict = isLockedWithConflict;
            return l;
        }

        boolean isPessimisticLock() {
            return lockType == LockType.PESSIMISTIC;
        }

        boolean isPessimisticTxn() {
            return forUpdateTs != 0;
        }

        boolean isPessimisticLockWithConflict() {
            return isPessimisticLock() && isLockedWithConflict;
        }

        /** lock.rs:622-645. */
        LockInfo intoLockInfo(int rawKey) {
            MOp op = lockType == LockType.PUT ? MOp.PUT : MOp.PESSIMISTIC_LOCK;
            return new LockInfo(rawKey, primary, ts, ttl, txnSize, op, forUpdateTs, useAsyncCommit, minCommitTs,
                    List.copyOf(secondaries));
        }
    }

    enum WriteType { PUT, ROLLBACK } // PORT: A2 no Delete, Lock writes

    /** txn_types::Write; gc_fence, last_change, short value cut (A2). */
    record Write(WriteType writeType, long startTs, boolean isProtected, boolean hasOverlappedRollback) {
        static Write newRollback(long startTs, boolean protectedRollback) {
            return new Write(WriteType.ROLLBACK, startTs, protectedRollback, false);
        }

        Write setOverlappedRollback(boolean v) {
            return new Write(writeType, startTs, isProtected, v); // PORT: gc_fence cut
        }
    }

    /** The per-key cell (A2): lock, writes (commit_ts -> write), data (start_ts -> value). */
    static final class Cell {
        Lock lock;
        final TreeMap<Long, Write> writes = new TreeMap<>();
        final HashMap<Long, Integer> data = new HashMap<>();
    }

    record CommitWrite(long commitTs, Write write) {}

    /** A storage error (mvcc / txn ErrorInner), carried to the response as a KeyError. */
    static final class MvccErr extends RuntimeException {
        final ErrKind kind;
        final KeyError keyError;

        MvccErr(KeyError keyError) {
            super(keyError.kind().name(), null, false, false);
            this.kind = keyError.kind();
            this.keyError = keyError;
        }
    }

    sealed interface Modify permits PutLock, DelLock, PutValue, DelValue, PutWriteM, DelWrite {}

    record PutLock(int key, Lock lock) implements Modify {}

    record DelLock(int key) implements Modify {}

    record PutValue(int key, long ts, int value) implements Modify {}

    record DelValue(int key, long ts) implements Modify {}

    record PutWriteM(int key, long ts, Write write) implements Modify {}

    record DelWrite(int key, long ts) implements Modify {}

    record OnePcLock(int key, Lock lock, boolean deletePessimisticLock) {}

    /** mvcc/txn.rs MvccTxn: the modifies of one command, applied at its end (A2). */
    static final class MvccTxn {
        long startTs;
        final List<Modify> modifies = new ArrayList<>();
        final List<OnePcLock> locksFor1pc = new ArrayList<>();

        MvccTxn(long startTs) {
            this.startTs = startTs;
        }

        List<Modify> intoModifies() {
            if (!locksFor1pc.isEmpty()) throw new AssertionError("locks_for_1pc not empty");
            return modifies;
        }

        void putLock(int key, Lock lock) {
            modifies.add(new PutLock(key, lock.copy())); // PORT: new_locks (lock-manager bookkeeping) cut
        }

        void putLocksFor1pc(int key, Lock lock, boolean removePessimisticLock) {
            locksFor1pc.add(new OnePcLock(key, lock.copy(), removePessimisticLock));
        }

        void putPessimisticLock(int key, Lock lock) {
            modifies.add(new PutLock(key, lock.copy()));
        }

        void unlockKey(int key) {
            modifies.add(new DelLock(key)); // PORT: ReleasedLock (lock-manager wake-ups) cut, A4
        }

        void putValue(int key, long ts, int value) {
            modifies.add(new PutValue(key, ts, value));
        }

        void deleteValue(int key, long ts) {
            modifies.add(new DelValue(key, ts));
        }

        void putWrite(int key, long ts, Write write) {
            modifies.add(new PutWriteM(key, ts, write));
        }

        void deleteWrite(int key, long ts) {
            modifies.add(new DelWrite(key, ts));
        }

        void markRollbackOnMismatchingLock(int key, Lock lock, boolean isProtected) {
            if (lock.ts == startTs) throw new AssertionError("assert_ne!(lock.ts, self.start_ts)");
            if (!isProtected) return;
            if (startTs < lock.minCommitTs) return;
            if (!lock.useAsyncCommit) return;
            Lock l = lock.copy();
            List<Long> rb = new ArrayList<>(l.rollbackTs);
            rb.add(startTs);
            l.rollbackTs = List.copyOf(rb);
            putLock(key, l);
        }

        void clear() {
            modifies.clear();
            locksFor1pc.clear();
        }

        /** get_pending_lock_bytes: null = no pending modification; NONE_PENDING = deleted. */
        Object pendingLock(int key) {
            for (int i = modifies.size() - 1; i >= 0; i--) {
                Modify m = modifies.get(i);
                if (m instanceof PutLock p && p.key() == key) return p.lock().copy();
                if (m instanceof DelLock d && d.key() == key) return NONE_PENDING;
            }
            return null;
        }
    }

    static final Object NONE_PENDING = new Object();

    /** TxnCommitRecord (mvcc/reader/mod.rs:30-47). */
    record TxnCommitRecord(int kind, long commitTs, Write write) {
        static final int NONE = 0;
        static final int SINGLE = 1;
        static final int OVERLAPPED_ROLLBACK = 2;
    }

    /** storage::TxnStatus (the result of CheckTxnStatus actions). */
    record TxnStatus(int kind, long commitTs, Lock lock, boolean minCommitTsPushed) {
        static final int ROLLED_BACK = 0;
        static final int TTL_EXPIRE = 1;
        static final int LOCK_NOT_EXIST = 2;
        static final int COMMITTED = 3;
        static final int PESSIMISTIC_ROLL_BACK = 4;
        static final int LOCK_NOT_EXIST_DO_NOTHING = 5;
        static final int UNCOMMITTED = 6;

        static TxnStatus of(int kind) {
            return new TxnStatus(kind, 0, null, false);
        }

        static TxnStatus committed(long ts) {
            return new TxnStatus(COMMITTED, ts, null, false);
        }

        static TxnStatus uncommitted(Lock lock, boolean pushed) {
            return new TxnStatus(UNCOMMITTED, 0, lock, pushed);
        }
    }

    enum MissingLockAction {
        ROLLBACK, PROTECTED_ROLLBACK, RETURN_ERROR;

        static MissingLockAction rollbackProtect(boolean protectRollback) {
            return protectRollback ? PROTECTED_ROLLBACK : ROLLBACK;
        }

        static MissingLockAction rollback(boolean rollbackIfNotExist) {
            return rollbackIfNotExist ? PROTECTED_ROLLBACK : RETURN_ERROR;
        }

        boolean collapseRollback() {
            switch (this) {
                case ROLLBACK: return true;
                case PROTECTED_ROLLBACK: return false;
                default: throw new AssertionError("unreachable");
            }
        }

        Write constructWrite(long ts, Write overlappedWrite) {
            return makeRollback(ts, !collapseRollback(), overlappedWrite);
        }
    }

    /** make_rollback (actions/check_txn_status.rs:438-455); null = None. */
    static Write makeRollback(long startTs, boolean isProtected, Write overlappedWrite) {
        if (overlappedWrite != null) {
            if (!(startTs > overlappedWrite.startTs())) throw new AssertionError("assert!(start_ts > write.start_ts)");
            if (isProtected) {
                return overlappedWrite.setOverlappedRollback(true);
            } else {
                return null;
            }
        }
        return Write.newRollback(startTs, isProtected);
    }

    static final class Region {
        final Map<Integer, Cell> cells = new TreeMap<>();
        /** ConcurrencyManager max_ts (A3). */
        long maxTs = 0;
        final boolean f1;
        final boolean f2;

        Region(String variant) {
            this.f1 = variant.equals("f1");
            this.f2 = variant.equals("f2");
        }

        /** concurrency_manager lib.rs:235-272 (limit checks cut). */
        void updateMaxTs(long newTs) {
            if (newTs == TS_MAX) return;
            maxTs = Math.max(maxTs, newTs);
        }

        long maxTs() {
            return maxTs;
        }

        Cell cell(int key) {
            Cell c = cells.get(key);
            if (c == null) throw new AssertionError("key " + key + " is not in this region");
            return c;
        }

        /** Applies the modifies of one command at its end (A2: one atomic step). */
        void apply(List<Modify> modifies) {
            for (Modify m : modifies) {
                if (m instanceof PutLock p) cell(p.key()).lock = p.lock().copy();
                else if (m instanceof DelLock d) cell(d.key()).lock = null;
                else if (m instanceof PutValue v) cell(v.key()).data.put(v.ts(), v.value());
                else if (m instanceof DelValue v) cell(v.key()).data.remove(v.ts());
                else if (m instanceof PutWriteM w) cell(w.key()).writes.put(w.ts(), w.write());
                else if (m instanceof DelWrite w) cell(w.key()).writes.remove(w.ts());
            }
        }

        KvResponse handle(KvRequest req) {
            if (req instanceof GetReq r) return kvGet(r);
            if (req instanceof PrewriteReq r) return kvPrewrite(r);
            if (req instanceof PessimisticLockReq r) return kvPessimisticLock(r);
            if (req instanceof CommitReq r) return kvCommit(r);
            if (req instanceof PessimisticRollbackReq r) return kvPessimisticRollback(r);
            if (req instanceof CheckTxnStatusReq r) return kvCheckTxnStatus(r);
            if (req instanceof CheckSecondaryLocksReq r) return kvCheckSecondaryLocks(r);
            // ResolveLockReq: two commands, handled in regionMain (kvResolveLockReadPhase, kvResolveLockWrite)
            throw new AssertionError("unknown request " + req);
        }

        // -------------------------------------------------------------------------------
        // SnapshotReader / MvccReader over the cell (mvcc/reader)
        // -------------------------------------------------------------------------------
        final class Reader {
            long startTs;

            Reader(long startTs) {
                this.startTs = startTs;
            }

            Lock loadLock(int key) {
                Lock l = cell(key).lock;
                return l == null ? null : l.copy();
            }

            /** reader.rs:476-500: the write with the largest commit_ts <= ts. */
            CommitWrite seekWrite(int key, long ts) {
                Map.Entry<Long, Write> e = cell(key).writes.floorEntry(ts);
                if (e == null) return null;
                return new CommitWrite(e.getKey(), e.getValue());
            }

            /** reader.rs:557-614 (last_change cut: a Rollback is skipped, A2). */
            CommitWrite getWriteWithCommitTs(int key, long ts) {
                CommitWrite seek = seekWrite(key, ts);
                while (true) {
                    if (seek == null) return null;
                    // gc_fence check cut (A2)
                    if (seek.write().writeType() == WriteType.PUT) return seek;
                    // WriteType::Rollback: LastChange cut (A2) -> the `_` arm
                    if (ts == 0) return null;
                    ts = seek.commitTs() - 1;
                    seek = seekWrite(key, ts);
                }
            }

            Write getWrite(int key, long ts) {
                CommitWrite cw = getWriteWithCommitTs(key, ts);
                return cw == null ? null : cw.write();
            }

            /** reader.rs get: the value of the latest version <= ts. */
            Integer get(int key, long ts) {
                Write w = getWrite(key, ts);
                return w == null ? null : loadData(key, w);
            }

            /** load_data: the value written by the write's transaction (no short values, A2). */
            int loadData(int key, Write write) {
                Integer v = cell(key).data.get(write.startTs());
                if (v == null) throw new AssertionError("default not found: key " + key + " start_ts " + write.startTs());
                return v;
            }

            /** reader.rs:616-653. */
            TxnCommitRecord getTxnCommitRecord(int key) {
                long seekTs = TS_MAX;
                CommitWrite cw;
                while ((cw = seekWrite(key, seekTs)) != null) {
                    if (cw.write().startTs() == startTs) {
                        return new TxnCommitRecord(TxnCommitRecord.SINGLE, cw.commitTs(), cw.write());
                    }
                    if (cw.commitTs() == startTs) {
                        if (cw.write().hasOverlappedRollback()) {
                            return new TxnCommitRecord(TxnCommitRecord.OVERLAPPED_ROLLBACK, cw.commitTs(), null);
                        }
                        return new TxnCommitRecord(TxnCommitRecord.NONE, 0, cw.write());
                    }
                    // gc_fence cut (A2)
                    if (cw.commitTs() < startTs) break;
                    seekTs = cw.commitTs() - 1;
                }
                return new TxnCommitRecord(TxnCommitRecord.NONE, 0, null);
            }
        }

        // -------------------------------------------------------------------------------
        // Get (storage/mod.rs get_entry 634-826, prepare_snap_ctx 3573-3634, point_getter.rs)
        // -------------------------------------------------------------------------------
        GetResp kvGet(GetReq r) {
            // prepare_snap_ctx: update max_ts (3585), then read_key_check (3596)
            updateMaxTs(r.version());
            Reader reader = new Reader(r.version());
            Lock lock = reader.loadLock(r.key()); // PORT: A3, the memory lock table and the CF_LOCK check are the cell's one lock
            if (lock != null) {
                try {
                    checkTsConflictSi(lock, r.key(), r.version());
                } catch (MvccErr e) {
                    return new GetResp(e.keyError, 0, false); // PORT: kv.rs error -> GetResponse.error (not in the cut)
                }
            }
            // PointGetter::load_data
            Integer v = reader.get(r.key(), r.version());
            return v == null ? new GetResp(null, 0, true) : new GetResp(null, v, false);
        }

        /** txn_types lock.rs check_ts_conflict_si 363-422 (bypass_locks empty: client-rust sets none). */
        void checkTsConflictSi(Lock lock, int key, long ts) {
            if (lock.ts > ts || lock.isPessimisticLock()) { // PORT: LockType::Lock / Shared cut (A2)
                return;
            }
            if (lock.minCommitTs > ts) {
                return;
            }
            // bypass_locks empty; replica read cut (A1)
            if (ts == TS_MAX && key == lock.primary && !lock.useAsyncCommit && !lock.useOnePc) {
                return;
            }
            throw keyIsLocked(lock.intoLockInfo(key));
        }

        static MvccErr keyIsLocked(LockInfo info) {
            return new MvccErr(new KeyError(ErrKind.KEY_IS_LOCKED, info, 0, 0, 0, info.key(), info.primaryLock(), 0, 0));
        }

        // -------------------------------------------------------------------------------
        // Prewrite (commands/prewrite.rs, actions/prewrite.rs)
        // -------------------------------------------------------------------------------
        static final int FORWARD_MIN_MUTATIONS_NUM = 12; // commands/prewrite.rs:44

        enum CommitKindT { TWO_PC, ONE_PC, ASYNC }

        enum LockStatusT { LOCKED, PESSIMISTIC, NONE }

        record LockStatus(LockStatusT kind, long ts) {
            boolean hasPessimisticLock() {
                return kind == LockStatusT.PESSIMISTIC;
            }
        }

        /** actions/prewrite.rs TransactionProperties 288-325. */
        static final class TransactionProperties {
            long startTs;
            boolean pessimistic; // TransactionKind::Pessimistic(for_update_ts) vs Optimistic(skip)
            long kindForUpdateTs;
            boolean skipConstraintCheckOpt;
            CommitKindT commitKind;
            long commitKindMaxCommitTs;
            int primary;
            long txnSize;
            long lockTtl;
            long minCommitTs;
            boolean isRetryRequest;

            long maxCommitTs() {
                if (commitKind == CommitKindT.TWO_PC) throw new AssertionError("unreachable");
                return commitKindMaxCommitTs;
            }

            boolean isPessimistic() {
                return pessimistic;
            }

            long forUpdateTs() {
                return pessimistic ? kindForUpdateTs : 0;
            }
        }

        record PrewriteOut(List<KeyError> locks, long finalMinCommitTs) {}

        PrewriteResp kvPrewrite(PrewriteReq req) {
            // commands/mod.rs:164-213: for_update_ts == 0 -> Prewrite, else PrewritePessimistic
            Prewriter p = new Prewriter(req, req.forUpdateTs() != 0);
            return p.processWrite();
        }

        final class Prewriter {
            final boolean pessimisticKind;
            final long forUpdateTs;
            boolean skipConstraintCheck = false;
            List<Mutation> mutations;
            List<PessimisticAction> pessimisticActions;
            final int primary;
            final long startTs;
            final long lockTtl;
            final long txnSize;
            final long minCommitTs;
            final long maxCommitTs;
            List<Integer> secondaryKeys;
            boolean tryOnePc;

            Prewriter(PrewriteReq req, boolean pessimistic) {
                this.pessimisticKind = pessimistic;
                this.forUpdateTs = req.forUpdateTs();
                this.mutations = new ArrayList<>(req.mutations());
                this.pessimisticActions = pessimistic ? new ArrayList<>(req.pessimisticActions()) : null;
                this.primary = req.primaryLock();
                this.startTs = req.startVersion();
                this.lockTtl = req.lockTtl();
                this.txnSize = req.txnSize();
                this.minCommitTs = req.minCommitTs();
                this.maxCommitTs = req.maxCommitTs();
                this.secondaryKeys = req.useAsyncCommit() ? req.secondaries() : null; // mod.rs:167-171
                this.tryOnePc = req.tryOnePc();
            }

            /** Prewriter::process_write 501-562. */
            PrewriteResp processWrite() {
                // txn_status_cache 507-533 cut (A6)
                canSkipConstraintCheck();
                // check_max_ts_synced 568-579 cut (A6)
                MvccTxn txn = new MvccTxn(startTs);
                Reader reader = new Reader(startTs);
                PrewriteOut res;
                try {
                    res = prewrite(txn, reader);
                } catch (MvccErr e) {
                    return new PrewriteResp(List.of(e.keyError), 0, 0); // PORT: kv.rs error -> PrewriteResponse.errors
                }
                return writeResult(res.locks(), txn, res.finalMinCommitTs());
            }

            /** Optimistic::can_skip_constraint_check 841-865 (pessimistic: nothing). */
            void canSkipConstraintCheck() {
                if (pessimisticKind) return;
                if (mutations.size() > FORWARD_MIN_MUTATIONS_NUM) {
                    mutations.sort((x, y) -> Integer.compare(x.key(), y.key()));
                    int left = mutations.get(0).key();
                    int right = mutations.get(mutations.size() - 1).key();
                    boolean hasData = false; // has_data_in_range over CF_WRITE
                    for (Map.Entry<Integer, Cell> e : cells.entrySet()) {
                        if (e.getKey() >= left && e.getKey() <= right && !e.getValue().writes.isEmpty()) hasData = true;
                    }
                    if (!hasData) skipConstraintCheck = true;
                }
            }

            /** Prewriter::prewrite 585-719. */
            PrewriteOut prewrite(MvccTxn txn, Reader reader) {
                CommitKindT commitKind;
                if (tryOnePc) commitKind = CommitKindT.ONE_PC;
                else if (secondaryKeys != null) commitKind = CommitKindT.ASYNC;
                else commitKind = CommitKindT.TWO_PC;

                TransactionProperties props = new TransactionProperties();
                props.startTs = startTs;
                props.pessimistic = pessimisticKind;
                props.kindForUpdateTs = forUpdateTs;
                props.skipConstraintCheckOpt = skipConstraintCheck;
                props.commitKind = commitKind;
                props.commitKindMaxCommitTs = maxCommitTs;
                props.primary = primary;
                props.txnSize = txnSize;
                props.lockTtl = lockTtl;
                props.minCommitTs = minCommitTs;
                props.isRetryRequest = false; // PORT: no retried requests (A1, section 7)

                Integer asyncCommitPk = (secondaryKeys != null && !secondaryKeys.isEmpty()) ? primary : null;

                long finalMinCommitTs = 0;
                List<KeyError> locks = new ArrayList<>();
                // assertion_failure cut (A2: no assertions)

                List<Mutation> ms = mutations;
                mutations = new ArrayList<>();
                for (int idx = 0; idx < ms.size(); idx++) {
                    Mutation m = ms.get(idx);
                    PessimisticAction pessimisticAction =
                            pessimisticKind ? pessimisticActions.get(idx) : PessimisticAction.SKIP_PESSIMISTIC_CHECK;
                    int key = m.key();

                    List<Integer> secondaries = secondaryKeys != null ? List.of() : null;
                    if (asyncCommitPk != null && key == asyncCommitPk) {
                        secondaries = secondaryKeys;
                    }

                    boolean needMinCommitTs = secondaries != null || tryOnePc;
                    try {
                        long ts = prewriteAction(txn, reader, props, m, secondaries, pessimisticAction);
                        if (!(needMinCommitTs && ts == 0)) {
                            if (needMinCommitTs && finalMinCommitTs < ts) {
                                finalMinCommitTs = ts;
                            }
                            // insert_old_value_if_resolved cut (A6: CDC old values)
                        } else {
                            props.commitKind = CommitKindT.TWO_PC;
                            asyncCommitPk = null;
                            secondaryKeys = null;
                            tryOnePc = false;
                            fallback1pcLocks(txn);
                            // txn.guards cleared: memory locks are the cell's lock (A3)
                            finalMinCommitTs = 0;
                        }
                    } catch (MvccErr e) {
                        if (e.kind == ErrKind.WRITE_CONFLICT && e.keyError.conflictCommitTs() > e.keyError.startTs()) {
                            return checkCommittedRecordOnErr(e, txn, reader, key);
                        } else if (e.kind == ErrKind.PESSIMISTIC_LOCK_NOT_FOUND) {
                            return checkCommittedRecordOnErr(e, txn, reader, key);
                        } else if (e.kind == ErrKind.COMMIT_TS_TOO_LARGE) {
                            try {
                                return checkCommittedRecordOnErr(e, txn, reader, key);
                            } catch (MvccErr ignored) {
                                // fallback to not using async commit or 1pc
                            }
                            props.commitKind = CommitKindT.TWO_PC;
                            asyncCommitPk = null;
                            secondaryKeys = null;
                            tryOnePc = false;
                            fallback1pcLocks(txn);
                            finalMinCommitTs = 0;
                        } else if (e.kind == ErrKind.KEY_IS_LOCKED) {
                            try {
                                return checkCommittedRecordOnErr(e, txn, reader, key);
                            } catch (MvccErr e2) {
                                locks.add(e2.keyError);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
                return new PrewriteOut(locks, finalMinCommitTs);
            }

            /** write_result 722-806. */
            PrewriteResp writeResult(List<KeyError> locks, MvccTxn txn, long finalMinCommitTs) {
                long asyncCommitTs = secondaryKeys != null ? finalMinCommitTs : 0;
                if (locks.isEmpty()) {
                    long onePcCommitTs = onePcCommit(tryOnePc, txn, finalMinCommitTs);
                    apply(txn.intoModifies());
                    return new PrewriteResp(List.of(), asyncCommitTs, onePcCommitTs);
                } else {
                    // Skip write stage if some keys are locked.
                    return new PrewriteResp(List.copyOf(locks), asyncCommitTs, 0);
                }
            }
        }

        /** actions/common.rs check_committed_record_on_err (called from commands/prewrite.rs:665-704). */
        PrewriteOut checkCommittedRecordOnErr(MvccErr err, MvccTxn txn, Reader reader, int key) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(key);
            if (rec.kind() == TxnCommitRecord.SINGLE && rec.write().writeType() != WriteType.ROLLBACK) {
                txn.clear();
                return new PrewriteOut(List.of(), rec.commitTs());
            }
            throw err;
        }

        /** one_pc_commit 951-967. */
        long onePcCommit(boolean tryOnePc, MvccTxn txn, long finalMinCommitTs) {
            if (tryOnePc) {
                if (finalMinCommitTs == 0) throw new AssertionError("assert_ne!(final_min_commit_ts, 0)");
                handle1pcLocks(txn, finalMinCommitTs);
                return finalMinCommitTs;
            } else {
                if (!txn.locksFor1pc.isEmpty()) throw new AssertionError("locks_for_1pc not empty");
                return 0;
            }
        }

        /** handle_1pc_locks 971-991. */
        void handle1pcLocks(MvccTxn txn, long commitTs) {
            List<OnePcLock> ls = new ArrayList<>(txn.locksFor1pc);
            txn.locksFor1pc.clear();
            for (OnePcLock l : ls) {
                Write write = new Write(WriteType.PUT, txn.startTs, false, false); // WriteType::from_lock_type(Put)
                txn.putWrite(l.key(), commitTs, write);
                if (l.deletePessimisticLock()) {
                    txn.unlockKey(l.key());
                }
            }
        }

        /** fallback_1pc_locks 994-1000. */
        void fallback1pcLocks(MvccTxn txn) {
            List<OnePcLock> ls = new ArrayList<>(txn.locksFor1pc);
            txn.locksFor1pc.clear();
            for (OnePcLock l : ls) {
                Lock lock = l.lock().copy();
                lock.useOnePc = false;
                txn.putLock(l.key(), lock);
            }
        }

        /** actions/prewrite.rs PrewriteMutation. */
        final class PrewriteMutation {
            int key;
            Integer value;
            List<Integer> secondaryKeys;
            long minCommitTs;
            PessimisticAction pessimisticAction;
            LockType lockType;
            long lockTtl;
            TransactionProperties txnProps;

            /** from_mutation 373-411. */
            PrewriteMutation(Mutation mutation, List<Integer> secondaryKeys, PessimisticAction pessimisticAction,
                    TransactionProperties txnProps) {
                // should_not_write / should_not_exist / shared lock / assertion: none for Put (A2)
                this.key = mutation.key();
                this.value = mutation.value();
                this.secondaryKeys = secondaryKeys;
                this.minCommitTs = txnProps.minCommitTs;
                this.pessimisticAction = pessimisticAction;
                this.lockType = LockType.PUT; // LockType::from_mutation(Put)
                this.lockTtl = txnProps.lockTtl;
                this.txnProps = txnProps;
            }

            /** lock_info 426-432: TTL 0 for a pessimistic prewrite (the TTL-0 protocol). */
            LockInfo lockInfo(Lock lock) {
                LockInfo info = lock.intoLockInfo(key);
                if (txnProps.isPessimistic()) {
                    info = info.withTtl(0);
                }
                return info;
            }

            /** check_lock 435-561. */
            LockStatus checkLock(Lock lock, PessimisticAction pessimisticAction) {
                if (lock.ts != txnProps.startTs) {
                    if (pessimisticAction == PessimisticAction.DO_PESSIMISTIC_CHECK) {
                        throw new MvccErr(KeyError.of(ErrKind.PESSIMISTIC_LOCK_NOT_FOUND, txnProps.startTs, key));
                    }
                    throw keyIsLocked(lockInfo(lock));
                }
                // last_change cut (A2)
                if (lock.isPessimisticLock()) {
                    if (!txnProps.isPessimistic()) {
                        throw new MvccErr(KeyError.of(ErrKind.LOCK_TYPE_NOT_MATCH, txnProps.startTs, key));
                    }
                    // expected_for_update_ts: None (client-rust sends no for_update_ts_constraints)
                    lockTtl = Math.max(lockTtl, lock.ttl);
                    minCommitTs = Math.max(minCommitTs, lock.minCommitTs);
                    return new LockStatus(LockStatusT.PESSIMISTIC, lock.forUpdateTs);
                }
                // generation checks cut (generation 0: no pipelined DML, A6)
                long mct = lock.useAsyncCommit ? lock.minCommitTs : 0;
                return new LockStatus(LockStatusT.LOCKED, mct);
            }

            /** check_for_newer_version 563-648. */
            CommitWrite checkForNewerVersion(Reader reader) {
                long seekTs = TS_MAX;
                CommitWrite cw;
                while ((cw = reader.seekWrite(key, seekTs)) != null) {
                    long commitTs = cw.commitTs();
                    Write write = cw.write();
                    if (commitTs == txnProps.startTs
                            && (write.writeType() == WriteType.ROLLBACK || write.hasOverlappedRollback())) {
                        writeConflictError(write, commitTs);
                    }
                    // last_change cut (A2)
                    if (!txnProps.pessimistic) {
                        if (!f2 && commitTs > txnProps.startTs) { // PORT: variant f2 skips this conflict check (584-592)
                            writeConflictError(write, commitTs);
                        }
                    } else {
                        long forUpdateTs = txnProps.kindForUpdateTs;
                        if (pessimisticAction == PessimisticAction.DO_CONSTRAINT_CHECK) {
                            if (commitTs > txnProps.startTs) {
                                writeConflictError(write, commitTs);
                            }
                        }
                        if (commitTs > forUpdateTs) {
                            if (write.writeType() == WriteType.ROLLBACK) {
                                seekTs = commitTs - 1;
                                continue;
                            }
                            throw new MvccErr(KeyError.of(ErrKind.PESSIMISTIC_LOCK_NOT_FOUND, txnProps.startTs, key));
                        }
                    }
                    // check_data_constraint: should_not_exist is false for Put (A2)
                    return cw;
                }
                return null;
            }

            /** write_conflict_error 772-787. */
            void writeConflictError(Write write, long commitTs) {
                throw new MvccErr(new KeyError(ErrKind.WRITE_CONFLICT, null, txnProps.startTs, write.startTs(), commitTs,
                        key, txnProps.primary, 0, 0));
            }

            /** skip_constraint_check 896-913. */
            boolean skipConstraintCheck() {
                if (!txnProps.pessimistic) return txnProps.skipConstraintCheckOpt;
                switch (pessimisticAction) {
                    case DO_PESSIMISTIC_CHECK: return true;
                    case SKIP_PESSIMISTIC_CHECK: return !txnProps.isRetryRequest;
                    default: return false;
                }
            }

            boolean tryOnePc() {
                return txnProps.commitKind == CommitKindT.ONE_PC;
            }

            /** write_lock 650-770. */
            long writeLock(LockStatus lockStatus, MvccTxn txn) {
                boolean tryOnePc = tryOnePc();
                long forUpdateTsToWrite = lockStatus.kind() == LockStatusT.PESSIMISTIC
                        ? Math.max(txnProps.forUpdateTs(), lockStatus.ts())
                        : txnProps.forUpdateTs();
                Lock lock = Lock.create(lockType, txnProps.primary, txnProps.startTs, lockTtl, forUpdateTsToWrite,
                        txnProps.txnSize, minCommitTs);
                // last_change cut (A2)
                if (value != null) {
                    txn.putValue(key, txnProps.startTs, value); // PORT: A2 no short values, the value goes to data
                }
                if (secondaryKeys != null) {
                    lock.useAsyncCommit = true;
                    lock.secondaries = List.copyOf(secondaryKeys);
                } else if (tryOnePc) {
                    lock.useOnePc = true;
                }
                long finalMinCommitTs = 0;
                MvccErr deferred = null;
                if (lock.useAsyncCommit || tryOnePc) {
                    try {
                        finalMinCommitTs = asyncCommitTimestamps(key, lock, txnProps.startTs, txnProps.forUpdateTs(),
                                txnProps.maxCommitTs());
                    } catch (MvccErr e) {
                        if (e.kind == ErrKind.COMMIT_TS_TOO_LARGE) {
                            tryOnePc = false;
                            lock.useOnePc = false;
                            lock.useAsyncCommit = false;
                            lock.secondaries = List.of();
                        }
                        deferred = e;
                    }
                }
                // shared locks cut (A2)
                if (tryOnePc) {
                    txn.putLocksFor1pc(key, lock, lockStatus.hasPessimisticLock());
                } else {
                    txn.putLock(key, lock);
                }
                if (deferred != null) throw deferred;
                return finalMinCommitTs;
            }
        }

        /** prewrite / prewrite_with_generation 37-286 (generation 0). */
        long prewriteAction(MvccTxn txn, Reader reader, TransactionProperties txnProps, Mutation mutation,
                List<Integer> secondaryKeys, PessimisticAction pessimisticAction) {
            PrewriteMutation m = new PrewriteMutation(mutation, secondaryKeys, pessimisticAction, txnProps);
            // should_not_exist false for Put: no max_ts update (A2); fail point cut
            boolean lockAmended = false;
            LockStatus lockStatus;
            Lock lock = reader.loadLock(m.key);
            if (lock != null) {
                lockStatus = m.checkLock(lock, pessimisticAction);
            } else if (pessimisticAction == PessimisticAction.DO_PESSIMISTIC_CHECK) {
                amendPessimisticLock(m, reader);
                lockAmended = true;
                lockStatus = new LockStatus(LockStatusT.NONE, 0);
            } else {
                lockStatus = new LockStatus(LockStatusT.NONE, 0);
            }
            if (lockStatus.kind() == LockStatusT.LOCKED) {
                return lockStatus.ts();
            }
            if (!m.skipConstraintCheck()) {
                m.checkForNewerVersion(reader);
            }
            // check_assertion cut (A2: no assertions); should_not_write false; old value cut (A6)
            return m.writeLock(lockStatus, txn);
        }

        /** async_commit_timestamps 928-979. */
        long asyncCommitTimestamps(int key, Lock lock, long startTs, long forUpdateTs, long maxCommitTs) {
            // key_guard / with_lock: the region evaluates one command at a time (A1, A3)
            long maxTs = maxTs();
            long minCommitTs = Math.max(Math.max(maxTs, startTs), forUpdateTs) + 1;
            minCommitTs = Math.max(lock.minCommitTs, minCommitTs);
            if (maxCommitTs != 0 && minCommitTs > maxCommitTs) {
                throw new MvccErr(KeyError.of(ErrKind.COMMIT_TS_TOO_LARGE, startTs, key));
            }
            lock.minCommitTs = minCommitTs;
            // *l = Some(lock.clone()): the memory lock is the cell's lock once the command is applied (A3)
            return minCommitTs;
        }

        /** amend_pessimistic_lock 984-1035. */
        void amendPessimisticLock(PrewriteMutation m, Reader reader) {
            CommitWrite w = reader.seekWrite(m.key, TS_MAX);
            if (w != null) {
                if (w.commitTs() >= reader.startTs) {
                    throw new MvccErr(KeyError.of(ErrKind.PESSIMISTIC_LOCK_NOT_FOUND, reader.startTs, m.key));
                }
                // last_change cut (A2)
            }
            // check_assertion cut (A2)
        }

        // -------------------------------------------------------------------------------
        // AcquirePessimisticLock (commands/acquire_pessimistic_lock.rs 82-220, actions 44-464)
        // -------------------------------------------------------------------------------
        PessimisticLockResp kvPessimisticLock(PessimisticLockReq req) {
            // allow_lock_with_conflict false (WakeUpModeNormal); wait_timeout 0 -> None (A4)
            boolean waitTimeoutIsNone = true; // PORT: A4, WaitTimeout::from_encoded(0) -> no wait
            MvccTxn txn = new MvccTxn(req.startVersion());
            Reader reader = new Reader(req.startVersion());
            List<Integer> values = new ArrayList<>();
            List<Boolean> notFounds = new ArrayList<>();
            KeyError encountered = null;
            for (Mutation mu : req.mutations()) {
                if (mu.op() != MOp.PESSIMISTIC_LOCK) throw new AssertionError("mismatch Op in pessimistic lock mutations");
                try {
                    Integer[] val = acquirePessimisticLock(txn, reader, mu.key(), req.primaryLock(), req.lockTtl(),
                            req.forUpdateTs(), req.returnValues(), req.minCommitTs());
                    values.add(val[0] == null ? 0 : val[0]);
                    notFounds.add(val[0] == null);
                } catch (MvccErr e) {
                    if (e.kind == ErrKind.KEY_IS_LOCKED) {
                        encountered = e.keyError;
                        // Do not lock previously succeeded keys.
                        txn.clear();
                        values.clear();
                        notFounds.clear();
                        break;
                    }
                    return new PessimisticLockResp(List.of(e.keyError), List.of(), List.of()); // PORT: kv.rs mapping
                }
            }
            apply(txn.intoModifies());
            if (encountered != null && waitTimeoutIsNone) {
                return new PessimisticLockResp(List.of(encountered), List.of(), List.of());
            }
            // PORT: kv.rs (not in the cut): values / not_founds only when return_values
            if (!req.returnValues()) return new PessimisticLockResp(List.of(), List.of(), List.of());
            return new PessimisticLockResp(List.of(), List.copyOf(values), List.copyOf(notFounds));
        }

        /** actions/acquire_pessimistic_lock.rs acquire_pessimistic_lock 44-333; returns {val}. */
        Integer[] acquirePessimisticLock(MvccTxn txn, Reader reader, int key, int primary, long lockTtl,
                long forUpdateTs, boolean needValue, long minCommitTs) {
            // should_not_exist, need_check_existence, lock_only_if_exists, allow_lock_with_conflict,
            // shared locks: all false / cut (client-rust sends none of them)
            if (needValue) {
                updateMaxTs(forUpdateTs);
            }
            Integer val = null;
            Lock currentLock = reader.loadLock(key);
            if (currentLock != null) {
                return handleExistingExclusiveLock(txn, reader, key, primary, lockTtl, forUpdateTs, needValue,
                        minCommitTs, currentLock);
            }
            CommitWrite cw = reader.seekWrite(key, TS_MAX);
            if (cw != null) {
                long commitTs = cw.commitTs();
                Write write = cw.write();
                if (commitTs > forUpdateTs) {
                    throw new MvccErr(new KeyError(ErrKind.WRITE_CONFLICT, null, reader.startTs, write.startTs(), commitTs,
                            key, primary, 0, 0));
                }
                if (commitTs == reader.startTs
                        && (write.writeType() == WriteType.ROLLBACK || write.hasOverlappedRollback())) {
                    if (!(write.hasOverlappedRollback() || write.startTs() == commitTs)) throw new AssertionError();
                    throw new MvccErr(KeyError.of(ErrKind.PESSIMISTIC_LOCK_ROLLED_BACK, reader.startTs, key));
                }
                if (commitTs > reader.startTs) {
                    CommitWrite older = reader.seekWrite(key, reader.startTs);
                    if (older != null && older.commitTs() == reader.startTs
                            && (older.write().writeType() == WriteType.ROLLBACK || older.write().hasOverlappedRollback())) {
                        throw new MvccErr(KeyError.of(ErrKind.PESSIMISTIC_LOCK_ROLLED_BACK, reader.startTs, key));
                    }
                }
                // check_data_constraint: should_not_exist false; last_change cut (A2)
                if (needValue) {
                    if (write.writeType() == WriteType.PUT) {
                        val = reader.loadData(key, write); // gc_fence check cut (A2)
                    } else { // Rollback
                        val = reader.get(key, commitTs - 1);
                    }
                }
            }
            // load_old_value cut (A6)
            Lock lock = Lock.create(LockType.PESSIMISTIC, primary, reader.startTs, lockTtl, forUpdateTs, 0, minCommitTs);
            lock.isLockedWithConflict = false;
            txn.putPessimisticLock(key, lock);
            return new Integer[] {val};
        }

        /** handle_existing_exclusive_lock 358-464. */
        Integer[] handleExistingExclusiveLock(MvccTxn txn, Reader reader, int key, int primary, long lockTtl,
                long forUpdateTs, boolean needValue, long minCommitTs, Lock lock) {
            if (lock.ts != reader.startTs) {
                throw keyIsLocked(lock.intoLockInfo(key));
            }
            if (!lock.isPessimisticLock()) {
                throw new MvccErr(KeyError.of(ErrKind.LOCK_TYPE_NOT_MATCH, reader.startTs, key));
            }
            Integer val = null;
            boolean needLoadValue = needValue;
            // allow_lock_with_conflict false: locked_with_conflict_ts None
            if (needLoadValue) {
                CommitWrite w = reader.getWriteWithCommitTs(key, forUpdateTs);
                if (w != null) {
                    val = reader.loadData(key, w.write());
                }
            }
            Lock lockToWrite = Lock.create(LockType.PESSIMISTIC, primary, reader.startTs, lockTtl, forUpdateTs, 0,
                    minCommitTs);
            lockToWrite.isLockedWithConflict = lock.isPessimisticLockWithConflict();
            if (forUpdateTs > lock.forUpdateTs) {
                txn.putPessimisticLock(key, lockToWrite);
            }
            return new Integer[] {val};
        }

        // -------------------------------------------------------------------------------
        // Commit (commands/commit.rs 53-97, actions/commit.rs 19-245)
        // -------------------------------------------------------------------------------
        CommitResp kvCommit(CommitReq r) {
            if (r.commitVersion() <= r.startVersion()) {
                return new CommitResp(KeyError.of(ErrKind.INVALID_TXN_TSO, r.startVersion(), 0), 0);
            }
            MvccTxn txn = new MvccTxn(r.startVersion());
            Reader reader = new Reader(r.startVersion());
            try {
                for (int k : r.keys()) {
                    commit(txn, reader, k, r.commitVersion());
                }
            } catch (MvccErr e) {
                return new CommitResp(e.keyError, 0); // PORT: kv.rs error -> CommitResponse.error
            }
            apply(txn.intoModifies());
            return new CommitResp(null, r.commitVersion());
        }

        /** handle_lock_not_found 19-62. */
        void handleLockNotFound(Reader reader, int key, long commitTs) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(key);
            boolean isRollbackOrNone = rec.kind() == TxnCommitRecord.NONE
                    || rec.kind() == TxnCommitRecord.OVERLAPPED_ROLLBACK
                    || rec.write().writeType() == WriteType.ROLLBACK;
            if (isRollbackOrNone) {
                throw new MvccErr(new KeyError(ErrKind.TXN_LOCK_NOT_FOUND, null, reader.startTs, 0, commitTs, key, 0, 0, 0));
            }
            // Committed by concurrent transaction.
        }

        /** commit 64-245 (commit_role None). */
        void commit(MvccTxn txn, Reader reader, int key, long commitTs) {
            Lock lock;
            Object pending = txn.pendingLock(key);
            if (pending == NONE_PENDING) {
                lock = null;
            } else if (pending != null) {
                lock = (Lock) pending;
            } else {
                lock = reader.loadLock(key);
            }
            boolean doCommit;
            if (lock != null) {
                if (lock.ts != reader.startTs) {
                    handleLockNotFound(reader, key, commitTs);
                    return;
                }
                if (commitTs < lock.minCommitTs) {
                    throw new MvccErr(new KeyError(ErrKind.COMMIT_TS_EXPIRED, null, reader.startTs, 0, 0, key, lock.primary,
                            lock.minCommitTs, commitTs));
                }
                doCommit = !lock.isPessimisticLock();
            } else {
                handleLockNotFound(reader, key, commitTs);
                return;
            }
            if (!doCommit) {
                txn.unlockKey(key);
                return;
            }
            Write write = new Write(WriteType.PUT, reader.startTs, false, false); // from_lock_type(Put); short value cut
            for (long ts : lock.rollbackTs) {
                if (ts == commitTs) {
                    write = write.setOverlappedRollback(true);
                    break;
                }
            }
            txn.putWrite(key, commitTs, write);
            txn.unlockKey(key);
        }

        // -------------------------------------------------------------------------------
        // CheckTxnStatus (commands/check_txn_status.rs 87-189, actions/check_txn_status.rs)
        // -------------------------------------------------------------------------------
        CheckTxnStatusResp kvCheckTxnStatus(CheckTxnStatusReq r) {
            long newMaxTs = r.lockTs();
            if (r.currentTs() != TS_MAX && r.currentTs() > newMaxTs) newMaxTs = r.currentTs();
            if (r.callerStartTs() != TS_MAX && r.callerStartTs() > newMaxTs) newMaxTs = r.callerStartTs();
            updateMaxTs(newMaxTs);
            MvccTxn txn = new MvccTxn(r.lockTs());
            Reader reader = new Reader(r.lockTs());
            TxnStatus st;
            try {
                Lock lock = reader.loadLock(r.primaryKey());
                if (lock != null && lock.ts == r.lockTs()) {
                    st = checkTxnStatusLockExists(txn, reader, r.primaryKey(), lock, r.currentTs(), r.callerStartTs(),
                            r.forceSyncCommit(), r.resolvingPessimisticLock(), r.verifyIsPrimary(),
                            r.rollbackIfNotExist());
                } else {
                    st = checkTxnStatusMissingLock(txn, reader, r.primaryKey(), lock,
                            MissingLockAction.rollback(r.rollbackIfNotExist()), r.resolvingPessimisticLock());
                }
            } catch (MvccErr e) {
                return new CheckTxnStatusResp(e.keyError, 0, 0, Action.NO_ACTION, null);
            }
            apply(txn.intoModifies());
            return txnStatusResp(st, r.primaryKey());
        }

        /** PORT: service/kv.rs TxnStatus -> CheckTxnStatusResponse (not in the cut; see NOTES.md). */
        static CheckTxnStatusResp txnStatusResp(TxnStatus st, int primaryKey) {
            switch (st.kind()) {
                case TxnStatus.ROLLED_BACK: return new CheckTxnStatusResp(null, 0, 0, Action.NO_ACTION, null);
                case TxnStatus.TTL_EXPIRE: return new CheckTxnStatusResp(null, 0, 0, Action.TTL_EXPIRE_ROLLBACK, null);
                case TxnStatus.LOCK_NOT_EXIST:
                    return new CheckTxnStatusResp(null, 0, 0, Action.LOCK_NOT_EXIST_ROLLBACK, null);
                case TxnStatus.COMMITTED: return new CheckTxnStatusResp(null, 0, st.commitTs(), Action.NO_ACTION, null);
                case TxnStatus.PESSIMISTIC_ROLL_BACK:
                    return new CheckTxnStatusResp(null, 0, 0, Action.TTL_EXPIRE_PESSIMISTIC_ROLLBACK, null);
                case TxnStatus.LOCK_NOT_EXIST_DO_NOTHING:
                    return new CheckTxnStatusResp(null, 0, 0, Action.LOCK_NOT_EXIST_DO_NOTHING, null);
                default:
                    return new CheckTxnStatusResp(null, st.lock().ttl, 0,
                            st.minCommitTsPushed() ? Action.MIN_COMMIT_TS_PUSHED : Action.NO_ACTION,
                            st.lock().intoLockInfo(primaryKey));
            }
        }

        /** T2: every TTL counts as expired. */
        static boolean ttlExpired() {
            return true; // PORT: T2, `lock.ts.physical() + lock.ttl < current_ts.physical()` is true
        }

        /** check_txn_status_from_pessimistic_primary_lock 17-68; null = (None, None). */
        TxnStatus checkTxnStatusFromPessimisticPrimaryLock(MvccTxn txn, Reader reader, int primaryKey, Lock lock,
                long currentTs, boolean resolvingPessimisticLock) {
            if (!lock.isPessimisticLock()) throw new AssertionError();
            if (lock.isPessimisticLockWithConflict()) {
                TxnStatus ts = checkDeterminedTxnStatus(reader, primaryKey);
                if (ts != null) {
                    txn.unlockKey(primaryKey);
                    return ts;
                }
            }
            if (ttlExpired()) { // PORT: T2 (check_txn_status.rs:55)
                if (resolvingPessimisticLock) {
                    txn.unlockKey(primaryKey);
                    return TxnStatus.of(TxnStatus.PESSIMISTIC_ROLL_BACK);
                } else {
                    rollbackLock(txn, reader, primaryKey, lock, true, true);
                    return TxnStatus.of(TxnStatus.TTL_EXPIRE);
                }
            }
            return null;
        }

        /** check_txn_status_lock_exists 92-217. */
        TxnStatus checkTxnStatusLockExists(MvccTxn txn, Reader reader, int primaryKey, Lock lock, long currentTs,
                long callerStartTs, boolean forceSyncCommit, boolean resolvingPessimisticLock, boolean verifyIsPrimary,
                boolean rollbackIfNotExist) {
            if (verifyIsPrimary && primaryKey != lock.primary) {
                if (!resolvingPessimisticLock && lock.isPessimisticLock()) {
                    TxnStatus st = checkTxnStatusMissingLock(txn, reader, primaryKey, null,
                            MissingLockAction.rollback(rollbackIfNotExist), resolvingPessimisticLock);
                    txn.unlockKey(primaryKey);
                    return st;
                }
                throw new MvccErr(new KeyError(ErrKind.PRIMARY_MISMATCH, lock.intoLockInfo(primaryKey), reader.startTs, 0,
                        0, primaryKey, lock.primary, 0, 0));
            }
            if (lock.useAsyncCommit) {
                if (!forceSyncCommit) {
                    return TxnStatus.uncommitted(lock, false);
                }
            }
            boolean isPessimisticTxn = lock.forUpdateTs != 0;
            if (lock.isPessimisticLock()) {
                TxnStatus check = checkTxnStatusFromPessimisticPrimaryLock(txn, reader, primaryKey, lock, currentTs,
                        resolvingPessimisticLock);
                if (check != null) {
                    return check;
                }
            } else if (ttlExpired()) { // PORT: T2 (check_txn_status.rs:176)
                // generation > 0 warning cut (A6)
                rollbackLock(txn, reader, primaryKey, lock, isPessimisticTxn, true);
                return TxnStatus.of(TxnStatus.TTL_EXPIRE);
            }
            if (lock.minCommitTs != 0 && callerStartTs != TS_MAX && callerStartTs >= lock.minCommitTs) {
                lock.minCommitTs = callerStartTs + 1;
                if (lock.minCommitTs < currentTs) {
                    lock.minCommitTs = currentTs;
                }
                txn.putLock(primaryKey, lock);
            }
            boolean minCommitTsPushed = (callerStartTs != 0 && lock.minCommitTs > callerStartTs)
                    || callerStartTs == TS_MAX;
            return TxnStatus.uncommitted(lock, minCommitTsPushed);
        }

        /** check_determined_txn_status 223-239; null = None. */
        TxnStatus checkDeterminedTxnStatus(Reader reader, int primaryKey) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(primaryKey);
            if (rec.kind() == TxnCommitRecord.SINGLE) {
                return rec.write().writeType() == WriteType.ROLLBACK ? TxnStatus.of(TxnStatus.ROLLED_BACK)
                        : TxnStatus.committed(rec.commitTs());
            }
            if (rec.kind() == TxnCommitRecord.OVERLAPPED_ROLLBACK) return TxnStatus.of(TxnStatus.ROLLED_BACK);
            return null;
        }

        /** check_txn_status_missing_lock 241-297. */
        TxnStatus checkTxnStatusMissingLock(MvccTxn txn, Reader reader, int primaryKey, Lock mismatchLock,
                MissingLockAction action, boolean resolvingPessimisticLock) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(primaryKey);
            if (rec.kind() == TxnCommitRecord.SINGLE) {
                return rec.write().writeType() == WriteType.ROLLBACK ? TxnStatus.of(TxnStatus.ROLLED_BACK)
                        : TxnStatus.committed(rec.commitTs());
            }
            if (rec.kind() == TxnCommitRecord.OVERLAPPED_ROLLBACK) return TxnStatus.of(TxnStatus.ROLLED_BACK);
            Write overlappedWrite = rec.write();
            if (action == MissingLockAction.RETURN_ERROR) {
                throw new MvccErr(KeyError.of(ErrKind.TXN_NOT_FOUND, reader.startTs, primaryKey));
            }
            if (resolvingPessimisticLock) {
                return TxnStatus.of(TxnStatus.LOCK_NOT_EXIST_DO_NOTHING);
            }
            long ts = reader.startTs;
            if (action.collapseRollback()) {
                collapsePrevRollback(txn, reader, primaryKey);
            }
            if (mismatchLock != null && overlappedWrite == null) {
                txn.markRollbackOnMismatchingLock(primaryKey, mismatchLock,
                        action == MissingLockAction.PROTECTED_ROLLBACK);
            }
            Write write = action.constructWrite(ts, overlappedWrite);
            if (write != null && !f1) { // PORT: variant f1 does not execute this put_write (287-291)
                txn.putWrite(primaryKey, ts, write);
            }
            return TxnStatus.of(TxnStatus.LOCK_NOT_EXIST);
        }

        /** rollback_lock 299-357. */
        void rollbackLock(MvccTxn txn, Reader reader, int key, Lock lock, boolean isPessimisticTxn,
                boolean collapseRollback) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(key);
            Write overlappedWrite;
            if (rec.kind() == TxnCommitRecord.NONE) {
                overlappedWrite = rec.write();
            } else if (rec.kind() == TxnCommitRecord.SINGLE && rec.write().writeType() != WriteType.ROLLBACK) {
                throw new AssertionError("txn record found but not expected: " + rec.write() + " " + rec.commitTs());
            } else {
                txn.unlockKey(key);
                return;
            }
            if (lock.lockType == LockType.PUT) { // short_value is None (A2)
                txn.deleteValue(key, lock.ts);
            }
            boolean isProtected = key == lock.primary; // generation > 0 cut (A6)
            Write write = makeRollback(reader.startTs, isProtected, overlappedWrite);
            if (write != null) {
                txn.putWrite(key, reader.startTs, write);
            }
            if (collapseRollback) {
                collapsePrevRollback(txn, reader, key);
            }
            txn.unlockKey(key);
        }

        /** collapse_prev_rollback 423-434. */
        void collapsePrevRollback(MvccTxn txn, Reader reader, int key) {
            CommitWrite cw = reader.seekWrite(key, reader.startTs);
            if (cw != null) {
                if (cw.write().writeType() == WriteType.ROLLBACK && !cw.write().isProtected()) {
                    txn.deleteWrite(key, cw.commitTs());
                }
            }
        }

        // -------------------------------------------------------------------------------
        // CheckSecondaryLocks (commands/check_secondary_locks.rs 67-254)
        // -------------------------------------------------------------------------------
        record SecStatus(int kind, long commitTs, Lock lock) {
            static final int LOCKED = 0;
            static final int COMMITTED = 1;
            static final int ROLLED_BACK = 2;
        }

        record SecCheck(SecStatus status, boolean needRollback, Write overlappedWrite) {}

        SecCheck checkDeterminedTxnStatusSec(Reader reader, int key) {
            TxnCommitRecord rec = reader.getTxnCommitRecord(key);
            if (rec.kind() == TxnCommitRecord.SINGLE) {
                SecStatus st = rec.write().writeType() != WriteType.ROLLBACK
                        ? new SecStatus(SecStatus.COMMITTED, rec.commitTs(), null)
                        : new SecStatus(SecStatus.ROLLED_BACK, 0, null);
                return new SecCheck(st, false, null);
            }
            if (rec.kind() == TxnCommitRecord.OVERLAPPED_ROLLBACK) {
                return new SecCheck(new SecStatus(SecStatus.ROLLED_BACK, 0, null), false, null);
            }
            return new SecCheck(new SecStatus(SecStatus.ROLLED_BACK, 0, null), true, rec.write());
        }

        SecCheck checkStatusFromLock(MvccTxn txn, Reader reader, Lock lock, int key) {
            Write overlappedWrite = null;
            if (lock.isPessimisticLockWithConflict()) {
                SecCheck c = checkDeterminedTxnStatusSec(reader, key);
                if (!c.needRollback()) {
                    txn.unlockKey(key);
                    return c;
                }
                overlappedWrite = c.overlappedWrite();
            }
            if (lock.isPessimisticLock()) {
                txn.unlockKey(key);
                Write ow;
                if (lock.isPessimisticLockWithConflict()) {
                    ow = overlappedWrite;
                } else {
                    TxnCommitRecord rec = reader.getTxnCommitRecord(key);
                    if (rec.kind() != TxnCommitRecord.NONE) throw new AssertionError("txn record found but not expected");
                    ow = rec.write();
                }
                return new SecCheck(new SecStatus(SecStatus.ROLLED_BACK, 0, null), true, ow);
            } else {
                return new SecCheck(new SecStatus(SecStatus.LOCKED, 0, lock), false, null);
            }
        }

        CheckSecondaryLocksResp kvCheckSecondaryLocks(CheckSecondaryLocksReq r) {
            updateMaxTs(r.startVersion());
            MvccTxn txn = new MvccTxn(r.startVersion());
            Reader reader = new Reader(r.startVersion());
            List<LockInfo> lockedResult = new ArrayList<>();
            int resultKind = SecStatus.LOCKED;
            long resultCommitTs = 0;
            for (int key : r.keys()) {
                Lock mismatchLock = null;
                SecCheck c;
                Lock l = reader.loadLock(key);
                if (l != null && l.ts == r.startVersion()) {
                    c = checkStatusFromLock(txn, reader, l, key);
                } else {
                    mismatchLock = l;
                    c = checkDeterminedTxnStatusSec(reader, key);
                }
                if (c.needRollback()) {
                    if (mismatchLock != null) {
                        txn.markRollbackOnMismatchingLock(key, mismatchLock, true);
                    }
                    Write write = makeRollback(r.startVersion(), true, c.overlappedWrite());
                    if (write != null) {
                        txn.putWrite(key, r.startVersion(), write);
                        collapsePrevRollback(txn, reader, key);
                    }
                }
                SecStatus st = c.status();
                if (st.kind() == SecStatus.LOCKED) {
                    lockedResult.add(st.lock().intoLockInfo(key));
                } else if (st.kind() == SecStatus.COMMITTED) {
                    resultKind = SecStatus.COMMITTED;
                    resultCommitTs = st.commitTs();
                    break;
                } else {
                    resultKind = SecStatus.ROLLED_BACK;
                    break;
                }
            }
            apply(txn.intoModifies());
            // PORT: kv.rs SecondaryLocksStatus -> CheckSecondaryLocksResponse (not in the cut)
            if (resultKind == SecStatus.LOCKED) return new CheckSecondaryLocksResp(null, List.copyOf(lockedResult), 0);
            if (resultKind == SecStatus.COMMITTED) return new CheckSecondaryLocksResp(null, List.of(), resultCommitTs);
            return new CheckSecondaryLocksResp(null, List.of(), 0);
        }

        // -------------------------------------------------------------------------------
        // ResolveLock (mod.rs 354-384 -> resolve_lock_readphase.rs + resolve_lock.rs 82-173)
        // -------------------------------------------------------------------------------
        /**
         * ResolveLockReadPhase::process_read (resolve_lock_readphase.rs:51-113): the region's locks of the
         * transactions in txn_status; null for ProcessResult::Res (nothing found), else the NextCommand.
         */
        ResolveLockNext kvResolveLockReadPhase(ResolveLockReq r, Tid from, int id) {
            // txn_status = {start_version: commit_version} (mod.rs:354-375)
            Map<Long, Long> txnStatus = new HashMap<>();
            txnStatus.put(r.startVersion(), r.commitVersion());
            List<KeyLock> flattenPairs = new ArrayList<>();
            // scan_locks_from_storage over the cell's keys; PORT: RESOLVE_LOCK_BATCH_SIZE cut (small regions),
            // so has_remain is false and scan_key is None; shared locks cut (A2)
            for (Map.Entry<Integer, Cell> e : cells.entrySet()) {
                Lock l = e.getValue().lock;
                if (l != null && txnStatus.containsKey(l.ts)) {
                    flattenPairs.add(new KeyLock(e.getKey(), l.ts, l.isPessimisticLock()));
                }
            }
            if (flattenPairs.isEmpty()) return null;
            return new ResolveLockNext(from, id, r.startVersion(), r.commitVersion(), List.copyOf(flattenPairs));
        }

        /** ResolveLock::process_write (resolve_lock.rs:82-173) on the snapshot of its own start. */
        ResolveLockResp kvResolveLockWrite(ResolveLockNext next) {
            Map<Long, Long> txnStatus = new HashMap<>();
            txnStatus.put(next.startVersion(), next.commitVersion());
            MvccTxn txn = new MvccTxn(0);
            Reader reader = new Reader(0);
            try {
                for (KeyLock kl : next.keyLocks()) {
                    int currentKey = kl.key();
                    KeyLock currentLock = kl;
                    txn.startTs = currentLock.ts();
                    reader.startTs = currentLock.ts();
                    long commitTs = txnStatus.get(currentLock.ts());
                    if (commitTs == 0) {
                        cleanup(txn, reader, currentKey, 0, false);
                    } else if (commitTs > currentLock.ts()) {
                        try {
                            commit(txn, reader, currentKey, commitTs);
                        } catch (MvccErr e) {
                            if (!(e.kind == ErrKind.TXN_LOCK_NOT_FOUND && currentLock.pessimisticLock())) throw e;
                        }
                    } else {
                        throw new MvccErr(KeyError.of(ErrKind.INVALID_TXN_TSO, currentLock.ts(), currentKey));
                    }
                    // MAX_TXN_WRITE_SIZE batching cut
                }
            } catch (MvccErr e) {
                return new ResolveLockResp(e.keyError);
            }
            apply(txn.intoModifies());
            return new ResolveLockResp(null);
        }

        /** actions/cleanup.rs cleanup 24-135. */
        void cleanup(MvccTxn txn, Reader reader, int key, long currentTs, boolean protectRollback) {
            Lock lock;
            Object pending = txn.pendingLock(key);
            if (pending == NONE_PENDING) {
                lock = null;
            } else if (pending != null) {
                lock = (Lock) pending;
            } else {
                lock = reader.loadLock(key);
            }
            if (lock != null && lock.ts == reader.startTs) {
                if (currentTs != 0) {
                    if (!ttlExpired()) { // PORT: T2 (cleanup.rs:58-62), `expire_time >= current_ts.physical()` is false
                        throw keyIsLocked(lock.intoLockInfo(key));
                    }
                }
                rollbackLock(txn, reader, key, lock, lock.isPessimisticTxn(), !protectRollback);
                return;
            }
            TxnStatus st = checkTxnStatusMissingLock(txn, reader, key, lock,
                    MissingLockAction.rollbackProtect(protectRollback), false);
            if (st.kind() == TxnStatus.COMMITTED) {
                throw new MvccErr(new KeyError(ErrKind.COMMITTED, null, reader.startTs, 0, st.commitTs(), key, 0, 0, 0));
            }
            if (st.kind() == TxnStatus.ROLLED_BACK || st.kind() == TxnStatus.LOCK_NOT_EXIST) return;
            throw new AssertionError("unreachable");
        }

        // -------------------------------------------------------------------------------
        // PessimisticRollback (commands/pessimistic_rollback.rs 60-156)
        // -------------------------------------------------------------------------------
        PessimisticRollbackResp kvPessimisticRollback(PessimisticRollbackReq r) {
            // keys non-empty: PessimisticRollback (the read phase variant is not reachable)
            MvccTxn txn = new MvccTxn(r.startVersion());
            Reader reader = new Reader(r.startVersion());
            for (int key : r.keys()) {
                Lock lock = reader.loadLock(key);
                if (lock != null) {
                    if (lock.isPessimisticLock() && lock.ts == r.startVersion() && lock.forUpdateTs <= r.forUpdateTs()) {
                        txn.unlockKey(key);
                    }
                }
            }
            apply(txn.intoModifies());
            return new PessimisticRollbackResp(List.of());
        }
    }

    // ===================================================================================
    // Client library: errors (client-rust common/errors.rs, as far as used)
    // ===================================================================================
    static class KvError extends RuntimeException {
        KvError(String m) {
            super(m, null, false, false);
        }

        // A KvError travels inside a ShardDone message, so it compares as a value (its class and its message,
        // which prints every field), not by identity.
        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == getClass() && java.util.Objects.equals(((KvError) o).getMessage(),
                    getMessage());
        }

        @Override
        public int hashCode() {
            return getClass().getName().hashCode() * 31 + java.util.Objects.hashCode(getMessage());
        }
    }

    /** Error::KeyError. */
    static final class KeyErr extends KvError {
        final KeyError keyErr;

        KeyErr(KeyError k) {
            super("KeyError " + k);
            keyErr = k;
        }
    }

    /** Error::MultipleKeyErrors. */
    static final class MultipleKeyErrors extends KvError {
        final List<KvError> errors;

        MultipleKeyErrors(List<KvError> errors) {
            super("MultipleKeyErrors " + errors);
            this.errors = errors;
        }
    }

    static final class TxnNotFound extends KvError {
        TxnNotFound(KeyError k) {
            super("TxnNotFound " + k);
        }
    }

    static final class PessimisticLockError extends KvError {
        final KvError inner;
        final List<Integer> successKeys;

        PessimisticLockError(KvError inner, List<Integer> successKeys) {
            super("PessimisticLockError " + inner + " " + successKeys);
            this.inner = inner;
            this.successKeys = successKeys;
        }
    }

    static final class ResolveLockError extends KvError {
        ResolveLockError(List<LockInfo> locks) {
            super("ResolveLockError " + locks);
        }
    }

    static final class OnePcFailure extends KvError {
        OnePcFailure() {
            super("OnePcFailure");
        }
    }

    static final class ProtocolViolation extends KvError {
        ProtocolViolation(String m) {
            super("ProtocolViolation " + m);
        }
    }

    static final class StringError extends KvError {
        StringError(String m) {
            super(m);
        }
    }

    // ===================================================================================
    // Client library: the request plans, the lock resolver (ti / xi, and one task per shard of a
    // plan with lock resolution, plan.rs:398)
    // ===================================================================================
    /** One result of the retry layer: Ok(response) or Err(error). */
    record Item(KvResponse resp, KvError err) {}

    /** The result of one shard's handler: a shard-level Err, or Ok(vec![item]); with its shard. */
    record ShardOut(KvError shardErr, Item item, List<Integer> shard) {}

    record Shard(List<Integer> keys, int region) {}

    /** The end of a shard task (plan.rs:398-410: the task's (idx, result)), sent to the task that spawned it. */
    record ShardDone(int plan, int idx, ShardOut out) implements MpMessage {}

    /**
     * One job of a shard task (round 3): what join_set.spawn (plan.rs:398) handed the task, i.e. what the
     * round-2 port captured when it spawned the task (the caller, the plan id, the shard index, the shard,
     * its request and its ResolveCfg). The ResolveCfg travels as its values (timestamp, the backoff's
     * maxAttempts and currentAttempts), because JMC compares message payloads by equals on replay and
     * Backoff is a mutable object without value equality.
     */
    record ShardWork(Tid parent, int plan, int idx, Shard sh, KvRequest shardReq, long timestamp,
            int backoffMax, int backoffCurrent) implements MpMessage {
        ResolveCfg cfg() {
            Backoff b = new Backoff(backoffMax);
            b.currentAttempts = backoffCurrent;
            return new ResolveCfg(timestamp, b);
        }
    }

    /**
     * A shard task (round 3): an invisible daemon spawned by main for one (ti or xi, region). Each job runs
     * as the round-2 spawned task did: a fresh Lib at the task's address, singleShardHandler, ShardDone to
     * the caller. Like the port's other daemons it is never stopped; it blocks on its next ShardWork.
     */
    static void shardTaskMain(CommunicationModel md, Tid pd, List<Tid> regions, List<Integer> regionOfKey) {
        final Tid self = MpProcess.self();
        while (true) {
            ShardWork w = Channel.recv(md, ShardWork.class);
            Lib task = new Lib(md, self, pd, regions, regionOfKey);
            ShardOut o = task.singleShardHandler(w.sh(), w.shardReq(), w.cfg());
            Channel.send(md, w.parent(), new ShardDone(w.plan(), w.idx(), o));
        }
    }

    /** What `.resolve_lock(timestamp, backoff, ..)` adds to a plan. */
    record ResolveCfg(long timestamp, Backoff backoff) {}

    /**
     * The library of one client process (ti or xi): the PdClient (TSO from pd), the region
     * layout (A5), the plans of client-rust's request/ module and the lock resolver.
     */
    static final class Lib {
        final CommunicationModel md;
        final Tid self;
        final Tid pd;
        final List<Tid> regions;
        final List<Integer> regionOfKey;
        int nextId = 0;
        /** ti's xi (the target of the commit_secondary trigger). */
        Tid xi;
        /** This process's shard tasks, indexed by region (round 3; empty when R = 1). */
        List<Tid> shardTasks = List.of();

        void sendTrigger(XTrigger t) {
            Channel.send(md, xi, t); // tokio::spawn (transaction.rs:1401-1405) -> one message to xi
        }

        Lib(CommunicationModel md, Tid self, Setup s) {
            this.md = md;
            this.self = self;
            this.pd = s.pd();
            this.regions = s.regions();
            this.regionOfKey = s.regionOfKey();
        }

        /** The library of one job of a shard task (plan.rs:398): same PD and regions, the task's address. */
        Lib(CommunicationModel md, Tid self, Tid pd, List<Tid> regions, List<Integer> regionOfKey) {
            this.md = md;
            this.self = self;
            this.pd = pd;
            this.regions = regions;
            this.regionOfKey = regionOfKey;
        }

        /** PdClient::get_timestamp. */
        long getTimestamp() {
            final int id = nextId++;
            Channel.send(md, pd, new TsoReq(self, id));
            TsoResp r = Channel.recv(md, TsoResp.class, x -> x.id() == id);
            return r.ts();
        }

        /** Dispatch::execute's send half: one request message to one region (plan.rs:50-84). */
        int send(int region, KvRequest req) {
            int id = nextId++;
            Channel.send(md, regions.get(region), new KvReq(self, id, req));
            return id;
        }

        /** A single-region request and its reply. */
        KvResponse dispatchOne(int region, KvRequest req) {
            final int id = send(region, req);
            return Channel.recv(md, KvReply.class, x -> x.id() == id).body();
        }

        int regionOf(int key) {
            return regionOfKey.get(key); // PORT: A5, the region of a key comes from the setup message
        }

        /** region_stream_for_keys over sorted keys (shard.rs, requests.rs shards); batches never split (keys tiny). */
        List<Shard> shardsOf(List<Integer> keysIn) {
            List<Integer> keys = new ArrayList<>(keysIn);
            keys.sort(Integer::compare);
            List<Shard> out = new ArrayList<>();
            List<Integer> cur = new ArrayList<>();
            int curRegion = -1;
            for (int k : keys) {
                int r = regionOf(k);
                if (r != curRegion && !cur.isEmpty()) {
                    out.add(new Shard(List.copyOf(cur), curRegion));
                    cur.clear();
                }
                curRegion = r;
                cur.add(k);
            }
            if (!cur.isEmpty()) out.add(new Shard(List.copyOf(cur), curRegion));
            return out;
        }

        /** Shardable::shards for each request type. */
        List<Shard> shards(KvRequest req) {
            if (req instanceof GetReq r) return shardsOf(List.of(r.key()));
            if (req instanceof PrewriteReq r) return shardsOf(r.mutations().stream().map(Mutation::key).toList());
            if (req instanceof PessimisticLockReq r) return shardsOf(r.mutations().stream().map(Mutation::key).toList());
            if (req instanceof CommitReq r) return shardsOf(r.keys());
            if (req instanceof PessimisticRollbackReq r) return shardsOf(r.keys());
            if (req instanceof CheckTxnStatusReq r) return shardsOf(List.of(r.primaryKey()));
            if (req instanceof CheckSecondaryLocksReq r) return shardsOf(r.keys());
            throw new AssertionError("not shardable: " + req);
        }

        /** Shardable::apply_shard for each request type. */
        KvRequest applyShard(KvRequest req, List<Integer> shard) {
            if (req instanceof PrewriteReq r) {
                // requests.rs:277-290
                List<Integer> secondaries = r.secondaries();
                boolean anyPrimary = r.mutations().stream().anyMatch(m -> m.key() == r.primaryLock());
                if (r.useAsyncCommit() && !anyPrimary) {
                    secondaries = List.of();
                }
                boolean tryOnePc = r.tryOnePc();
                if (tryOnePc && shard.size() != secondaries.size() + 1) {
                    tryOnePc = false;
                }
                List<Mutation> ms = new ArrayList<>();
                List<PessimisticAction> acts = new ArrayList<>();
                for (int k : shard) {
                    for (int i = 0; i < r.mutations().size(); i++) {
                        if (r.mutations().get(i).key() == k) {
                            ms.add(r.mutations().get(i));
                            if (!r.pessimisticActions().isEmpty()) acts.add(r.pessimisticActions().get(i));
                        }
                    }
                }
                return new PrewriteReq(List.copyOf(ms), r.primaryLock(), r.startVersion(), r.lockTtl(), r.txnSize(),
                        r.forUpdateTs(), List.copyOf(acts), r.useAsyncCommit(), secondaries, tryOnePc,
                        r.minCommitTs(), r.maxCommitTs());
            }
            if (req instanceof PessimisticLockReq r) {
                List<Mutation> ms = new ArrayList<>();
                for (int k : shard) {
                    for (Mutation m : r.mutations()) if (m.key() == k) ms.add(m);
                }
                return new PessimisticLockReq(List.copyOf(ms), r.primaryLock(), r.startVersion(), r.lockTtl(),
                        r.forUpdateTs(), r.isFirstLock(), r.waitTimeout(), r.returnValues(), r.minCommitTs());
            }
            if (req instanceof CommitReq r) return new CommitReq(shard, r.startVersion(), r.commitVersion());
            if (req instanceof PessimisticRollbackReq r) return new PessimisticRollbackReq(shard, r.startVersion(), r.forUpdateTs());
            if (req instanceof CheckSecondaryLocksReq r) return new CheckSecondaryLocksReq(shard, r.startVersion());
            return req; // single-key requests: the shard is the key itself
        }

        /** HasLocks::take_locks for each response (requests.rs:925-966). */
        static List<LockInfo> takeLocks(KvResponse resp) {
            List<LockInfo> out = new ArrayList<>();
            List<KeyError> errs = new ArrayList<>();
            if (resp instanceof GetResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof PrewriteResp r) errs.addAll(r.errors());
            if (resp instanceof PessimisticLockResp r) errs.addAll(r.errors());
            if (resp instanceof PessimisticRollbackResp r) errs.addAll(r.errors());
            if (resp instanceof CommitResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof CheckTxnStatusResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof CheckSecondaryLocksResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof ResolveLockResp r && r.error() != null) errs.add(r.error());
            for (KeyError e : errs) {
                if (e.kind() == ErrKind.KEY_IS_LOCKED && e.locked() != null) out.add(e.locked());
            }
            return out;
        }

        /** HasKeyErrors::key_errors (store module, not in the cut): the response's key errors or null. */
        static List<KeyError> keyErrors(KvResponse resp) {
            List<KeyError> errs = new ArrayList<>();
            if (resp instanceof GetResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof PrewriteResp r) errs.addAll(r.errors());
            if (resp instanceof PessimisticLockResp r) errs.addAll(r.errors());
            if (resp instanceof PessimisticRollbackResp r) errs.addAll(r.errors());
            if (resp instanceof CommitResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof CheckTxnStatusResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof CheckSecondaryLocksResp r && r.error() != null) errs.add(r.error());
            if (resp instanceof ResolveLockResp r && r.error() != null) errs.add(r.error());
            return errs.isEmpty() ? null : errs;
        }

        /**
         * RetryableMultiRegion::single_plan_handler (plan.rs:375-451) over ResolveLock::execute
         * (plan.rs:1267-1308) when {@code rl} is given: one task per shard (plan.rs:398), each running
         * its shard's request, lock resolution and re-dispatch to the end; the caller joins them. No
         * region error occurs (A1), so no region retry.
         */
        List<ShardOut> retryMultiRegion(KvRequest req, ResolveCfg rl) {
            List<Shard> shards = shards(req);
            int n = shards.size();
            ShardOut[] outs = new ShardOut[n];
            if (rl != null) {
                // Review round 1, finding 1: each shard is its own task (join_set.spawn, plan.rs:398), running
                // the cloned plan ResolveLock<Dispatch> (plan.rs:1267-1308) to its end, so the lock resolutions
                // of two shards interleave as in the source.
                if (n == 1) {
                    // PORT: one shard runs inline in the caller; the caller does nothing between the spawn
                    // and the join (plan.rs:398-420), so this is the same schedule as a task.
                    outs[0] = singleShardHandler(shards.get(0), applyShard(req, shards.get(0).keys()),
                            new ResolveCfg(rl.timestamp(), rl.backoff().copy()));
                    return List.of(outs);
                }
                final int plan = nextId++;
                for (int i = 0; i < n; i++) {
                    final Shard sh = shards.get(i);
                    final KvRequest shardReq = applyShard(req, sh.keys()); // clone_then_apply_shard
                    // join_set.spawn (plan.rs:398) -> one job for this shard's task (PORT: a tokio task). Round 3:
                    // the task is a daemon spawned by main (one per region of this process), and the spawn is
                    // one ShardWork message to it; the shards of a plan are distinct regions (shardsOf). The
                    // task rebuilds ResolveCfg(rl.timestamp(), rl.backoff().copy()) (backoff.clone()) from it.
                    Channel.send(md, shardTasks.get(sh.region()), new ShardWork(self, plan, i, sh, shardReq,
                            rl.timestamp(), rl.backoff().maxAttempts, rl.backoff().currentAttempts));
                }
                for (int i = 0; i < n; i++) {
                    // collect_join_set_results (plan.rs:108-153). PORT: joined in shard order, not in
                    // join_next order; the handler only stores results[idx] until every task has ended
                    // (plan.rs:117-133), so the join order is not observable.
                    final int idx = i;
                    ShardDone d = Channel.recv(md, ShardDone.class, x -> x.plan() == plan && x.idx() == idx);
                    outs[i] = d.out();
                }
                return List.of(outs);
            }
            // Without .resolve_lock(..) a shard's task is one Dispatch and its reply; the shard requests
            // go to distinct regions, so sending them all from the caller and taking the replies in any
            // order is the schedule of the spawned tasks.
            KvRequest[] shardReqs = new KvRequest[n];
            Map<Integer, Integer> outstanding = new HashMap<>();
            for (int i = 0; i < n; i++) {
                shardReqs[i] = applyShard(req, shards.get(i).keys());
                outstanding.put(send(shards.get(i).region(), shardReqs[i]), i); // join_set.spawn (plan.rs:398)
            }
            while (!outstanding.isEmpty()) {
                // collect_join_set_results (plan.rs:108-153): the replies in any order
                final Set<Integer> want = Set.copyOf(outstanding.keySet());
                KvReply rep = Channel.recv(md, KvReply.class, x -> want.contains(x.id()));
                int i = outstanding.remove(rep.id());
                outs[i] = handleCandidateResponse(rep.body(), shards.get(i).keys());
            }
            return List.of(outs);
        }

        /** single_shard_handler -> handle_candidate_response (plan.rs:785-812): key errors to Err. */
        static ShardOut handleCandidateResponse(KvResponse resp, List<Integer> shardKeys) {
            List<KeyError> kerrs = keyErrors(resp);
            if (kerrs != null) {
                List<KvError> es = new ArrayList<>();
                for (KeyError k : kerrs) es.add(new KeyErr(k));
                return new ShardOut(null, new Item(null, new MultipleKeyErrors(es)), shardKeys);
            }
            return new ShardOut(null, new Item(resp, null), shardKeys);
        }

        /**
         * One shard's task: single_shard_handler (plan.rs) over ResolveLock::execute (plan.rs:1267-1308)
         * over Dispatch (plan.rs:50-84). {@code rl.backoff()} is the task's own clone.
         */
        ShardOut singleShardHandler(Shard sh, KvRequest shardReq, ResolveCfg rl) {
            List<Integer> shardKeys = sh.keys();
            KvResponse result = dispatchOne(sh.region(), shardReq); // self.inner.execute()
            Backoff cloneBackoff = rl.backoff().copy(); // let mut clone = self.clone()
            while (true) {
                List<LockInfo> locks = takeLocks(result);
                if (locks.isEmpty()) {
                    break;
                }
                if (rl.backoff().isNone()) {
                    return new ShardOut(new ResolveLockError(locks), null, shardKeys);
                }
                try {
                    List<LockInfo> liveLocks = resolveLocks(locks, rl.timestamp());
                    if (liveLocks.isEmpty()) {
                        result = dispatchOne(sh.region(), shardReq);
                    } else if (!cloneBackoff.nextDelayDuration()) {
                        return new ShardOut(new ResolveLockError(liveLocks), null, shardKeys);
                    } else {
                        // sleep(delay_duration) cut (T4)
                        result = dispatchOne(sh.region(), shardReq);
                    }
                } catch (KvError e) {
                    return new ShardOut(e, null, shardKeys);
                }
            }
            return handleCandidateResponse(result, shardKeys);
        }

        /** single_plan_handler, not preserve_region_results: the first shard-level error wins, else the items. */
        static List<Item> nonPreserve(List<ShardOut> outs) {
            List<Item> oks = new ArrayList<>();
            for (ShardOut o : outs) {
                if (o.shardErr() != null) throw o.shardErr(); // undetermined errors cannot occur (A6)
                oks.add(o.item());
            }
            return oks;
        }

        /** ExtractError (plan.rs:1498-1545) over Vec<Result<_>>: any Err item is the plan's error. */
        static List<KvResponse> extractError(List<Item> items) {
            List<KvResponse> out = new ArrayList<>();
            for (Item it : items) {
                if (it.err() != null) throw it.err(); // PORT: the Vec impl of HasKeyErrors is in the store module (not in the cut)
                out.add(it.resp());
            }
            return out;
        }

        /** CollectSingle (plan.rs:1183-1197). */
        static KvResponse collectSingle(List<Item> items) {
            if (items.size() != 1) throw new AssertionError("assert!(input.len() == 1)");
            Item it = items.get(0);
            if (it.err() != null) throw it.err();
            return it.resp();
        }

        // ---------------------------------------------------------------------------------
        // lock.rs
        // ---------------------------------------------------------------------------------
        enum LockActionKind { RESOLVE, LIVE, DONE }

        record LockAction(LockActionKind kind, long commitVersion, List<Integer> keys, LockInfo primaryLock) {
            static LockAction fromStatus(TransactionStatus status) {
                switch (status.kind) {
                    case COMMITTED: return new LockAction(LockActionKind.RESOLVE, status.commitTs, List.of(), null);
                    case ROLLED_BACK: return new LockAction(LockActionKind.RESOLVE, 0, List.of(), null);
                    default: return new LockAction(LockActionKind.LIVE, 0, null, status.primaryLock);
                }
            }
        }

        /** resolve_locks (lock.rs:86-151). */
        List<LockInfo> resolveLocks(List<LockInfo> locks, long timestamp) {
            // reject_shared_locks cut (A2)
            long ts = getTimestamp();
            long callerStartTs = timestamp;
            long currentTs = ts;
            List<LockInfo> liveLocks = new ArrayList<>();
            LockResolver lockResolver = new LockResolver();
            Map<Long, Set<Integer>> cleanRegions = new HashMap<>();
            for (LockInfo lock : locks) {
                LockAction act = actionForLock(lockResolver, lock, callerStartTs, currentTs);
                if (act.kind() == LockActionKind.LIVE) {
                    liveLocks.add(act.primaryLock());
                    continue;
                }
                if (act.kind() == LockActionKind.DONE) continue;
                Set<Integer> regionsCleaned = cleanRegions.computeIfAbsent(lock.lockVersion(), k -> new HashSet<>());
                List<Integer> keys = new ArrayList<>(act.keys());
                keys.add(lock.key());
                for (int key : keys) {
                    resolveLockWithRetry(key, lock.lockVersion(), act.commitVersion(), optimisticBackoff(),
                            regionsCleaned);
                }
            }
            return liveLocks;
        }

        /** pessimistic_rollback_lock (lock.rs:189-217). */
        void pessimisticRollbackLock(LockInfo lock) {
            if (lock.key() == lock.primaryLock()) {
                return;
            }
            long forUpdateTs = lock.lockForUpdateTs() == 0 ? TS_MAX : lock.lockForUpdateTs();
            PessimisticRollbackReq req = new PessimisticRollbackReq(List.of(lock.key()), lock.lockVersion(), forUpdateTs);
            // retry_multi_region(DEFAULT_REGION_BACKOFF).extract_error()
            extractError(nonPreserve(retryMultiRegion(req, null)));
        }

        /** resolve_lock_with_retry (lock.rs:223-317). */
        void resolveLockWithRetry(int key, long startVersion, long commitVersion, Backoff backoff,
                Set<Integer> cleanRegions) {
            int attempt = 0;
            while (true) {
                attempt++;
                int verId = regionOf(key); // store_for_key (A5)
                if (cleanRegions.contains(verId)) {
                    return;
                }
                ResolveLockReq request = new ResolveLockReq(startVersion, commitVersion); // is_txn_file false (A6)
                // single_region_with_store: LeaderNotFound cannot occur (A1)
                ResolveLockResp resp = (ResolveLockResp) dispatchOne(verId, request);
                if (resp.error() == null) {
                    cleanRegions.add(verId);
                    return;
                }
                // region errors and gRPC errors cannot occur (A1, A6); key error:
                throw new KeyErr(resp.error());
            }
        }

        /** ResolveLocksContext (lock.rs:320-370): the cache of one resolve_locks call. */
        static final class LockResolver {
            final Map<Long, TransactionStatus> resolved = new HashMap<>();
        }

        /** LockResolver::action_for_lock (lock.rs:381-482). */
        LockAction actionForLock(LockResolver lr, LockInfo lock, long callerStartTs, long currentTs) {
            TransactionStatus status;
            try {
                status = getTxnStatusFromLock(lr, optimisticBackoff(), lock, callerStartTs, currentTs, false);
            } catch (KeyErr e) {
                if (e.keyErr.kind() == ErrKind.PRIMARY_MISMATCH && lock.lockType() == MOp.PESSIMISTIC_LOCK) {
                    pessimisticRollbackLock(lock);
                    return new LockAction(LockActionKind.DONE, 0, null, null);
                }
                throw e;
            }
            LockInfo primaryLock;
            if (status.kind == TsKind.LOCKED && status.isExpired && status.primaryLock.useAsyncCommit()) {
                primaryLock = status.primaryLock;
            } else if (status.kind == TsKind.LOCKED) {
                return LockAction.fromStatus(status);
            } else if (lock.lockType() == MOp.PESSIMISTIC_LOCK) {
                pessimisticRollbackLock(lock);
                return new LockAction(LockActionKind.DONE, 0, null, null);
            } else {
                return LockAction.fromStatus(status);
            }
            SecondaryLocksStatus secondaryStatus = checkAllSecondaries(primaryLock.secondaries(), lock.lockVersion());
            if (secondaryStatus.fallback2pc) {
                TransactionStatus st2 = getTxnStatusFromLock(lr, optimisticBackoff(), lock, callerStartTs, currentTs, true);
                return LockAction.fromStatus(st2);
            }
            long commitVersion = secondaryStatus.resolvedCommitVersion(primaryLock.minCommitTs());
            lr.resolved.put(lock.lockVersion(), TransactionStatus.fromCommitVersion(commitVersion));
            List<Integer> keys = new ArrayList<>(primaryLock.secondaries());
            keys.add(primaryLock.key());
            return new LockAction(LockActionKind.RESOLVE, commitVersion, keys, null);
        }

        /** LockResolver::check_txn_status (lock.rs:615-689). */
        TransactionStatus checkTxnStatus(LockResolver lr, long txnId, int primary, long callerStartTs, long currentTs,
                boolean rollbackIfNotExist, boolean forceSyncCommit, boolean resolvingPessimisticLock) {
            TransactionStatus cached = lr.resolved.get(txnId);
            if (cached != null) {
                return cached;
            }
            CheckTxnStatusReq req = new CheckTxnStatusReq(primary, txnId, callerStartTs, currentTs, rollbackIfNotExist,
                    forceSyncCommit, resolvingPessimisticLock, true); // verify_is_primary = true; is_txn_file cut
            // retry_multi_region(DEFAULT_REGION_BACKOFF).merge(CollectSingle).extract_error().post_process_default()
            CheckTxnStatusResp resp;
            try {
                resp = (CheckTxnStatusResp) collectSingle(nonPreserve(retryMultiRegion(req, null)));
            } catch (MultipleKeyErrors e) {
                KvError last = e.errors.get(e.errors.size() - 1);
                if (last instanceof KeyErr ke) {
                    if (ke.keyErr.kind() == ErrKind.TXN_NOT_FOUND) {
                        throw new TxnNotFound(ke.keyErr);
                    }
                    throw ke;
                }
                throw last;
            }
            TransactionStatus status = TransactionStatus.from(resp);
            long current = getTimestamp();
            status.checkTtl(current);
            if (status.isCacheable()) {
                lr.resolved.put(txnId, status);
            }
            return status;
        }

        /** LockResolver::check_all_secondaries (lock.rs:691-708). */
        SecondaryLocksStatus checkAllSecondaries(List<Integer> keys, long txnId) {
            CheckSecondaryLocksReq req = new CheckSecondaryLocksReq(keys, txnId);
            // preserve_shard().retry_multi_region().extract_error().merge(CollectSecondaryLocks)
            List<ShardOut> outs = retryMultiRegion(req, null);
            List<Item> items = nonPreserve(outs);
            extractError(items);
            return collectSecondaryLocks(outs, txnId);
        }

        /** CollectSecondaryLocks merge (requests.rs:813-880). */
        static SecondaryLocksStatus collectSecondaryLocks(List<ShardOut> input, long startVersion) {
            SecondaryLocksStatus out = new SecondaryLocksStatus();
            for (ShardOut o : input) {
                CheckSecondaryLocksResp resp = (CheckSecondaryLocksResp) o.item().resp();
                List<Integer> requestedKeys = o.shard();
                if (resp.locks().size() > requestedKeys.size()) {
                    throw new ProtocolViolation("CheckSecondaryLocks returned " + resp.locks().size()
                            + " locks for " + requestedKeys.size() + " requested keys");
                }
                for (LockInfo lock : resp.locks()) {
                    if (lock.lockVersion() != startVersion) {
                        throw new ProtocolViolation("CheckSecondaryLocks returned a lock of transaction "
                                + lock.lockVersion() + " while checking transaction " + startVersion);
                    }
                    if (!lock.useAsyncCommit()) {
                        out.fallback2pc = true;
                    }
                    out.minCommitTs = Math.max(out.minCommitTs, lock.minCommitTs());
                }
                if (resp.locks().size() < requestedKeys.size()) {
                    if (out.commitTs != null && out.commitTs != resp.commitTs()) {
                        throw new ProtocolViolation("CheckSecondaryLocks reported conflicting commit TS");
                    }
                    out.commitTs = resp.commitTs();
                }
            }
            return out;
        }

        /** LockResolver::get_txn_status_from_lock (lock.rs:729-794). */
        TransactionStatus getTxnStatusFromLock(LockResolver lr, Backoff backoff, LockInfo lock, long callerStartTs,
                long currentTs, boolean forceSyncCommit) {
            long cts = lock.lockTtl() == 0 ? TS_MAX : currentTs; // the TTL-0 protocol
            boolean rollbackIfNotExist = false;
            while (true) {
                try {
                    return checkTxnStatus(lr, lock.lockVersion(), lock.primaryLock(), callerStartTs, cts,
                            rollbackIfNotExist, forceSyncCommit, lock.lockType() == MOp.PESSIMISTIC_LOCK);
                } catch (TxnNotFound e) {
                    long current = getTimestamp();
                    if (lockUntilExpiredMs(lock.lockVersion(), lock.lockTtl(), current) <= 0) {
                        rollbackIfNotExist = true;
                        continue;
                    } else if (lock.lockType() == MOp.PESSIMISTIC_LOCK) {
                        TransactionStatus st = new TransactionStatus();
                        st.kind = TsKind.LOCKED;
                        st.ttl = lock.lockTtl();
                        st.primaryLock = lock;
                        st.action = Action.NO_ACTION;
                        st.isExpired = false;
                        return st;
                    }
                    if (backoff.nextDelayDuration()) {
                        continue; // sleep cut (T4)
                    }
                    throw e;
                }
            }
        }
    }

    /** lock_until_expired_ms (lock.rs:805-807). */
    static long lockUntilExpiredMs(long lockVersion, long ttl, long current) {
        return -1; // PORT: T2, every lock counts as expired (a literal port with physical() = 0 would give ttl)
    }

    enum TsKind { COMMITTED, ROLLED_BACK, LOCKED }

    /** requests.rs TransactionStatus 701-795. */
    static final class TransactionStatus {
        TsKind kind;
        long commitTs;
        long ttl;
        LockInfo primaryLock;
        Action action;
        boolean isExpired;

        /** From<CheckTxnStatusResponse> and From<(u64, u64, Option<LockInfo>)>. */
        static TransactionStatus from(CheckTxnStatusResp resp) {
            TransactionStatus s = new TransactionStatus();
            s.action = resp.action();
            s.isExpired = false;
            long ts = resp.commitVersion();
            long ttl = resp.lockTtl();
            LockInfo info = resp.lockInfo();
            if (ts == 0 && ttl == 0 && info == null) {
                s.kind = TsKind.ROLLED_BACK;
            } else if (ttl == 0 && info == null) {
                s.kind = TsKind.COMMITTED;
                s.commitTs = ts;
            } else if (ts == 0 && info != null) { // (0, ttl, Some(info)), any ttl (requests.rs:788)
                s.kind = TsKind.LOCKED;
                s.ttl = ttl;
                s.primaryLock = info;
            } else {
                throw new AssertionError("unreachable: " + resp);
            }
            return s;
        }

        static TransactionStatus fromCommitVersion(long commitVersion) {
            TransactionStatus s = new TransactionStatus();
            if (commitVersion == 0) {
                s.kind = TsKind.ROLLED_BACK;
            } else {
                s.kind = TsKind.COMMITTED;
                s.commitTs = commitVersion;
            }
            s.action = Action.NO_ACTION;
            s.isExpired = false;
            return s;
        }

        void checkTtl(long current) {
            if (kind == TsKind.LOCKED) {
                isExpired = true; // PORT: T2, `current.physical - lock.physical >= ttl` is true (requests.rs:734)
            }
        }

        boolean isDetermined() {
            switch (kind) {
                case COMMITTED: return true;
                case ROLLED_BACK:
                    return action == Action.NO_ACTION || action == Action.LOCK_NOT_EXIST_ROLLBACK
                            || action == Action.TTL_EXPIRE_ROLLBACK;
                default: return false;
            }
        }

        boolean isCacheable() {
            return isDetermined();
        }
    }

    /** requests.rs SecondaryLocksStatus 880-924. */
    static final class SecondaryLocksStatus {
        Long commitTs = null;
        long minCommitTs = 0;
        boolean fallback2pc = false;

        long resolvedCommitVersion(long primaryMinCommitTs) {
            long minCommitTs = Math.max(primaryMinCommitTs, this.minCommitTs);
            long commitVersion = commitTs != null ? commitTs : minCommitTs;
            if (commitVersion != 0 && commitVersion < minCommitTs) {
                throw new ProtocolViolation("CheckSecondaryLocks reported commit TS " + commitVersion
                        + " below a surviving lock's min_commit_ts " + minCommitTs);
            }
            return commitVersion;
        }
    }

    // ===================================================================================
    // Client: Buffer (buffer.rs)
    // ===================================================================================
    enum EntryKind { CACHED, PUT, LOCKED } // PORT: Del, Insert, CheckNotExist cut (A2)

    /** BufferEntry: Cached(Option<Value>), Put(Value), Locked(Option<Option<Value>>). */
    record BufferEntry(EntryKind kind, boolean lockedHasValue, Integer value) {}

    static final class Buffer {
        Integer primaryKey = null;
        final TreeMap<Integer, BufferEntry> entryMap = new TreeMap<>();
        final boolean isPessimistic;

        Buffer(boolean isPessimistic) {
            this.isPessimistic = isPessimistic;
        }

        Integer getPrimaryKey() {
            return primaryKey;
        }

        void primaryKeyOr(int key) {
            if (primaryKey == null) primaryKey = key;
        }

        /** get_or_else 55-73; f is the read of the key. */
        Integer getOrElse(int key, IntFunction<Integer> f) {
            BufferEntry e = entryMap.get(key);
            if (e != null) {
                // get_value 414-424
                if (e.kind() == EntryKind.CACHED) return e.value();
                if (e.kind() == EntryKind.PUT) return e.value();
                if (e.kind() == EntryKind.LOCKED && e.lockedHasValue()) return e.value();
            }
            Integer value = f.apply(key);
            updateCache(key, value);
            return value;
        }

        /** lock 201-214. */
        void lock(int key) {
            if (primaryKey == null) primaryKey = key;
            BufferEntry e = entryMap.get(key);
            if (e == null) {
                entryMap.put(key, new BufferEntry(EntryKind.LOCKED, false, null));
            } else if (e.kind() == EntryKind.CACHED) {
                entryMap.put(key, new BufferEntry(EntryKind.LOCKED, true, e.value()));
            }
        }

        /** unlock 216-226. */
        void unlock(int key) {
            BufferEntry e = entryMap.get(key);
            if (e != null && e.kind() == EntryKind.LOCKED) {
                if (e.lockedHasValue()) {
                    entryMap.put(key, new BufferEntry(EntryKind.CACHED, false, e.value()));
                } else {
                    entryMap.remove(key);
                }
            }
        }

        /** put 228-241 (Insert / CheckNotExist arm cut). */
        void put(int key, int value) {
            insertEntry(key, new BufferEntry(EntryKind.PUT, false, value));
        }

        /** to_proto_mutations 277-283. */
        List<Mutation> toProtoMutations() {
            List<Mutation> out = new ArrayList<>();
            for (Map.Entry<Integer, BufferEntry> e : entryMap.entrySet()) {
                BufferEntry be = e.getValue();
                if (be.kind() == EntryKind.CACHED) continue;
                if (be.kind() == EntryKind.LOCKED) {
                    // PORT: Op::Lock mutations are outside the cell model (A2: only Put); a key only locked
                    // (GetForUpdate without Put) is not in any scenario of this round.
                    throw new UnsupportedOperationException("Op::Lock mutation (GetForUpdate without Put) not modelled");
                }
                out.add(new Mutation(MOp.PUT, e.getKey(), be.value()));
            }
            return out;
        }

        long getWriteSize() {
            long s = 0;
            for (BufferEntry be : entryMap.values()) {
                if (be.kind() == EntryKind.PUT) s += 2; // PORT: a key and a value are one byte each
            }
            return s;
        }

        /** update_cache 297-321. */
        void updateCache(int key, Integer value) {
            BufferEntry e = entryMap.get(key);
            if (e != null && e.kind() == EntryKind.LOCKED && !e.lockedHasValue()) {
                entryMap.put(key, new BufferEntry(EntryKind.LOCKED, true, value));
            } else if (e == null) {
                entryMap.put(key, new BufferEntry(EntryKind.CACHED, false, value));
            } else if (e.kind() == EntryKind.CACHED || e.kind() == EntryKind.LOCKED) {
                if (!java.util.Objects.equals(value, e.value())) throw new AssertionError("assert!(&value == v)");
            } else if (e.kind() == EntryKind.PUT) {
                if (!java.util.Objects.equals(value, e.value())) throw new AssertionError("assert!(value == Some(v))");
            }
        }

        /** insert_entry 324-330 (the first mutation becomes the primary). */
        void insertEntry(int key, BufferEntry entry) {
            if (entry.kind() != EntryKind.CACHED) {
                if (primaryKey == null) primaryKey = key;
            }
            entryMap.put(key, entry);
        }
    }

    // ===================================================================================
    // Client: TransactionOptions, Transaction, Committer (transaction.rs)
    // ===================================================================================
    static final class TransactionOptions {
        boolean pessimistic;
        long pessimisticForUpdateTs;
        boolean tryOnePc;
        boolean asyncCommit;
        Backoff regionBackoff;
        Backoff lockBackoff;

        static TransactionOptions newOptimistic() {
            TransactionOptions o = new TransactionOptions();
            o.pessimistic = false;
            o.regionBackoff = defaultRegionBackoff();
            o.lockBackoff = optimisticBackoff();
            // check_level / heartbeat_option cut (T3, P1)
            return o;
        }

        static TransactionOptions newPessimistic() {
            TransactionOptions o = new TransactionOptions();
            o.pessimistic = true;
            o.pessimisticForUpdateTs = 0;
            o.regionBackoff = defaultRegionBackoff();
            o.lockBackoff = pessimisticBackoff();
            return o;
        }

        TransactionOptions useAsyncCommit() {
            asyncCommit = true;
            return this;
        }

        TransactionOptions tryOnePc() {
            tryOnePc = true;
            return this;
        }

        TransactionOptions copy() {
            TransactionOptions o = new TransactionOptions();
            o.pessimistic = pessimistic;
            o.pessimisticForUpdateTs = pessimisticForUpdateTs;
            o.tryOnePc = tryOnePc;
            o.asyncCommit = asyncCommit;
            o.regionBackoff = regionBackoff.copy();
            o.lockBackoff = lockBackoff.copy();
            return o;
        }

        void pushForUpdateTs(long forUpdateTs) {
            if (!pessimistic) throw new AssertionError("unreachable");
            pessimisticForUpdateTs = Math.max(pessimisticForUpdateTs, forUpdateTs);
        }

        boolean isPessimistic() {
            return pessimistic;
        }
    }

    enum TransactionStatusT { ACTIVE, STARTED_COMMIT, COMMITTED }

    static final class Transaction {
        TransactionStatusT status;
        final long timestamp;
        final Buffer buffer;
        final Lib rpc;
        final TransactionOptions options;
        boolean prewritten = false;
        final int a;

        Transaction(long timestamp, Lib rpc, TransactionOptions options, int a) {
            this.status = TransactionStatusT.ACTIVE; // read_only false
            this.timestamp = timestamp;
            this.buffer = new Buffer(options.isPessimistic());
            this.rpc = rpc;
            this.options = options;
            this.a = a; // PORT: P2, the attempt bound of ti's WriteConflict retry
        }

        /** Transaction::get 143-164. */
        Integer get(int key) {
            checkAllowOperation();
            final long ts = timestamp;
            final Backoff lockBackoff = options.lockBackoff.copy();
            return buffer.getOrElse(key, k -> {
                GetReq request = new GetReq(k, ts);
                // resolve_lock(timestamp, lock_backoff).retry_multi_region(DEFAULT_REGION_BACKOFF)
                //   .merge(CollectSingle).post_process_default()
                GetResp resp = (GetResp) Lib.collectSingle(Lib.nonPreserve(
                        rpc.retryMultiRegion(request, new ResolveCfg(ts, lockBackoff))));
                return resp.notFound() ? null : resp.value();
            });
        }

        /** Transaction::get_for_update 208-224. */
        Integer getForUpdate(int key) {
            checkAllowOperation();
            if (!isPessimistic()) {
                // PORT: lock_keys is not in the cut; optimistic GetForUpdate is in no scenario (INTERFACE 3)
                throw new UnsupportedOperationException("optimistic get_for_update (lock_keys) not in the cut");
            }
            List<int[]> pairs = pessimisticLockWithRetry(key, true);
            if (pairs.isEmpty()) return null;
            return pairs.get(pairs.size() - 1)[1];
        }

        /** Transaction::put 470-480. */
        void put(int key, int value) {
            checkAllowOperation();
            if (isPessimistic()) {
                pessimisticLockWithRetry(key, false);
            }
            buffer.put(key, value);
        }

        /** PORT: P2 / INTERFACE 5.4, ti retries a WriteConflict of pessimistic_lock, at most a attempts in total. */
        List<int[]> pessimisticLockWithRetry(int key, boolean needValue) {
            for (int attempt = 1; ; attempt++) { // PORT: P2
                try { // PORT: P2
                    return pessimisticLock(List.of(key), needValue);
                } catch (KvError e) { // PORT: P2
                    if (attempt < a && isWriteConflict(e)) continue; // PORT: P2
                    throw e; // PORT: P2
                }
            }
        }

        /** PORT: P2, a WriteConflict as pessimistic_lock returns it (PessimisticLockError / MultipleKeyErrors / KeyError). */
        static boolean isWriteConflict(KvError e) {
            if (e instanceof PessimisticLockError p) return isWriteConflict(p.inner);
            if (e instanceof MultipleKeyErrors m) {
                for (KvError x : m.errors) if (isWriteConflict(x)) return true;
                return false;
            }
            return e instanceof KeyErr k && k.keyErr.kind() == ErrKind.WRITE_CONFLICT;
        }

        /** Transaction::commit 653-706. */
        Long commit() {
            if (!(status == TransactionStatusT.STARTED_COMMIT || status == TransactionStatusT.ACTIVE)) {
                throw new StringError("OperationAfterCommitError");
            }
            status = TransactionStatusT.STARTED_COMMIT;
            prewritten = true;
            Integer primaryKey = buffer.getPrimaryKey();
            List<Mutation> mutations = buffer.toProtoMutations();
            if (mutations.isEmpty()) {
                if (primaryKey != null) throw new AssertionError("assert!(primary_key.is_none())");
                return null;
            }
            // start_auto_heartbeat cut (T3)
            Long res = new Committer(primaryKey, mutations, timestamp, rpc, options.copy(), buffer.getWriteSize()).commit();
            status = TransactionStatusT.COMMITTED;
            return res;
        }

        /** Transaction::pessimistic_lock 859-940; the pairs are {key, value}. */
        List<int[]> pessimisticLock(List<Integer> keys, boolean needValue) {
            if (!options.isPessimistic()) {
                throw new AssertionError("`pessimistic_lock` is only valid to use with pessimistic transactions");
            }
            if (keys.isEmpty()) {
                return List.of();
            }
            int firstKey = keys.get(0);
            int primaryLock = buffer.getPrimaryKey() != null ? buffer.getPrimaryKey() : firstKey;
            long forUpdateTs = rpc.getTimestamp();
            options.pushForUpdateTs(forUpdateTs);
            List<Mutation> ms = new ArrayList<>();
            for (int k : keys) ms.add(new Mutation(MOp.PESSIMISTIC_LOCK, k, 0)); // lowering.rs:140-160
            PessimisticLockReq request = new PessimisticLockReq(List.copyOf(ms), primaryLock, timestamp, MAX_TTL,
                    forUpdateTs, false, 0, needValue, 0); // requests.rs:398-420
            // resolve_lock(..).preserve_shard().retry_multi_region_preserve_results(..).merge(CollectWithShard)
            List<ShardOut> outs = rpc.retryMultiRegion(request, new ResolveCfg(timestamp, options.lockBackoff.copy()));
            List<int[]> pairs;
            try {
                pairs = collectWithShard(outs);
            } catch (PessimisticLockError err) {
                if (!err.successKeys.isEmpty()) {
                    pessimisticLockRollback(err.successKeys, timestamp, forUpdateTs);
                    throw err.inner;
                }
                throw err;
            }
            buffer.primaryKeyOr(firstKey);
            // start_auto_heartbeat cut (T3)
            for (int key : keys) {
                buffer.lock(key);
            }
            return pairs;
        }

        /** Merge<ResponseWithShard<PessimisticLockResponse, _>> for CollectWithShard (requests.rs:448-510). */
        static List<int[]> collectWithShard(List<ShardOut> outs) {
            // preserve_region_results: every shard result, shard-level errors included, is an item
            List<KvError> errors = new ArrayList<>();
            List<ShardOut> success = new ArrayList<>();
            for (ShardOut o : outs) {
                KvError e = o.shardErr() != null ? o.shardErr() : o.item().err();
                if (e != null) errors.add(e);
                else success.add(o);
            }
            if (!errors.isEmpty()) {
                KvError firstErr = errors.get(errors.size() - 1); // errors.pop()
                List<Integer> successKeys = new ArrayList<>();
                for (ShardOut o : success) successKeys.addAll(o.shard());
                throw new PessimisticLockError(firstErr, List.copyOf(successKeys));
            }
            List<int[]> pairs = new ArrayList<>();
            for (ShardOut o : success) {
                PessimisticLockResp resp = (PessimisticLockResp) o.item().resp();
                List<Integer> keys = o.shard();
                int valuesLen = resp.values().size();
                List<int[]> kv = new ArrayList<>();
                for (int i = 0; i < Math.min(keys.size(), valuesLen); i++) kv.add(new int[] {keys.get(i), resp.values().get(i)});
                if (kv.size() != valuesLen) throw new AssertionError("assert_eq!(kvpairs.len(), values_len)");
                if (resp.notFounds().isEmpty()) {
                    // legacy: an empty value is a missing key; values are never empty here (ints)
                    pairs.addAll(kv);
                } else {
                    if (kv.size() != resp.notFounds().size()) throw new AssertionError();
                    for (int i = 0; i < kv.size(); i++) if (!resp.notFounds().get(i)) pairs.add(kv.get(i));
                }
            }
            return pairs;
        }

        /** Transaction::pessimistic_lock_rollback 943-980. */
        void pessimisticLockRollback(List<Integer> keys, long startVersion, long forUpdateTs) {
            if (keys.isEmpty()) {
                return;
            }
            PessimisticRollbackReq req = new PessimisticRollbackReq(List.copyOf(keys), startVersion, forUpdateTs);
            // resolve_lock(start_version, lock_backoff).retry_multi_region(region_backoff).extract_error()
            Lib.extractError(Lib.nonPreserve(rpc.retryMultiRegion(req,
                    new ResolveCfg(startVersion, options.lockBackoff.copy()))));
            for (int key : keys) {
                buffer.unlock(key);
            }
        }

        void checkAllowOperation() {
            if (status != TransactionStatusT.ACTIVE) throw new StringError("OperationAfterCommitError");
        }

        boolean isPessimistic() {
            return options.isPessimistic();
        }
    }

    /** Committer (transaction.rs:1320-1720). */
    static final class Committer {
        final Integer primaryKey;
        final List<Mutation> mutations;
        final long startVersion;
        final Lib rpc;
        final TransactionOptions options;
        final long writeSize;

        Committer(Integer primaryKey, List<Mutation> mutations, long startVersion, Lib rpc, TransactionOptions options,
                long writeSize) {
            this.primaryKey = primaryKey;
            this.mutations = mutations;
            this.startVersion = startVersion;
            this.rpc = rpc;
            this.options = options;
            this.writeSize = writeSize;
        }

        /** Committer::commit 1340-1407; returns the commit ts (null = None). */
        Long commit() {
            Long minCommitTs;
            // is_commit_outcome_unknown cannot hold: no gRPC / undetermined errors (A6)
            minCommitTs = prewrite();
            // fail_point after-prewrite cut
            if (options.tryOnePc) {
                return minCommitTs;
            }
            long commitTs;
            if (options.asyncCommit) {
                if (minCommitTs == null) throw new AssertionError("min_commit_ts.unwrap() on None");
                commitTs = minCommitTs;
            } else {
                commitTs = commitPrimaryWithRetry(); // the undetermined flag cannot be set (A6)
            }
            // tokio::spawn(self.commit_secondary(commit_ts)) -> the trigger of xi (README section 4)
            List<Integer> keys = new ArrayList<>();
            for (Mutation m : mutations) keys.add(m.key());
            rpc.sendTrigger(new XTrigger(commitTs, startVersion, List.copyOf(keys), primaryKey, options.asyncCommit,
                    options.lockBackoff.maxAttempts)); // self.options.retry_options.lock_backoff (transaction.rs:1638-1642)
            return commitTs;
        }

        /** Committer::prewrite 1409-1483. */
        Long prewrite() {
            int primaryLock = primaryKey;
            long elapsed = 0; // PORT: T6, start_instant unused
            long lockTtl = calcTxnLockTtl();
            long forUpdateTs = options.pessimistic ? options.pessimisticForUpdateTs : 0;
            List<PessimisticAction> actions = new ArrayList<>();
            if (options.pessimistic) {
                // new_pessimistic_prewrite_request (requests.rs:235-248)
                for (int i = 0; i < mutations.size(); i++) actions.add(PessimisticAction.DO_PESSIMISTIC_CHECK);
            }
            List<Integer> secondaries = new ArrayList<>();
            for (Mutation m : mutations) if (m.key() != primaryKey) secondaries.add(m.key());
            // new_prewrite_request: txn_size = u64::MAX (requests.rs:228); FIXME max/min_commit_ts unset
            PrewriteReq request = new PrewriteReq(List.copyOf(mutations), primaryLock, startVersion, lockTtl + elapsed,
                    TS_MAX, forUpdateTs, List.copyOf(actions), options.asyncCommit, List.copyOf(secondaries),
                    options.tryOnePc, 0, 0);
            // resolve_lock(start_version, lock_backoff); retry_multi_region(_terminal_on_undetermined);
            // merge(CollectError).extract_error()
            List<ShardOut> outs = rpc.retryMultiRegion(request, new ResolveCfg(startVersion, options.lockBackoff.copy()));
            List<Item> items = Lib.nonPreserve(outs);
            List<PrewriteResp> response = new ArrayList<>();
            for (Item it : items) { // CollectError: input.into_iter().collect()
                if (it.err() != null) throw it.err();
                response.add((PrewriteResp) it.resp());
            }
            if (options.tryOnePc && response.size() == 1) {
                if (response.get(0).onePcCommitTs() == 0) {
                    throw new OnePcFailure();
                }
                return response.get(0).onePcCommitTs();
            }
            options.tryOnePc = false;
            Long minCommitTs = null;
            for (PrewriteResp r : response) {
                if (r.onePcCommitTs() != 0) throw new AssertionError("assert_eq!(r.one_pc_commit_ts, 0)");
                minCommitTs = minCommitTs == null ? r.minCommitTs() : Math.max(minCommitTs, r.minCommitTs());
            }
            return minCommitTs;
        }

        /** Committer::commit_primary 1486-1542. */
        long commitPrimary() {
            long commitVersion = rpc.getTimestamp();
            CommitReq req = new CommitReq(List.of(primaryKey), startVersion, commitVersion);
            // resolve_lock(..).retry_multi_region_terminal_on_undetermined(..).extract_error()
            Lib.extractError(Lib.nonPreserve(rpc.retryMultiRegion(req,
                    new ResolveCfg(startVersion, options.lockBackoff.copy()))));
            return commitVersion;
        }

        /** Committer::commit_primary_with_retry 1544-1584. */
        long commitPrimaryWithRetry() {
            while (true) {
                try {
                    return commitPrimary();
                } catch (MultipleKeyErrors e) {
                    if (e.errors.isEmpty()) throw new AssertionError("unreachable");
                    KvError last = e.errors.get(e.errors.size() - 1);
                    if (last instanceof KeyErr ke) {
                        if (ke.keyErr.kind() == ErrKind.COMMIT_TS_EXPIRED) {
                            KeyError expired = ke.keyErr;
                            if (primaryKey != expired.key()) {
                                throw new StringError(
                                        "2PC commitTS rejected by TiKV, but the key is not the primary key");
                            }
                            long diff = expired.minCommitTs() - expired.attemptedCommitTs();
                            if (Math.max(diff, 0) > 943718400000L) {
                                throw new StringError("2PC min_commit_ts is too large");
                            }
                            continue;
                        } else {
                            throw ke;
                        }
                    }
                    throw last;
                }
            }
        }

        long calcTxnLockTtl() {
            long lockTtl = DEFAULT_LOCK_TTL;
            if (writeSize > TXN_COMMIT_BATCH_SIZE) {
                double sizeMb = writeSize / 1024.0 / 1024.0;
                lockTtl = (long) (TTL_FACTOR * Math.sqrt(sizeMb));
                lockTtl = Math.max(DEFAULT_LOCK_TTL, Math.min(lockTtl, MAX_TTL));
            }
            return lockTtl;
        }
    }

    /** Committer::commit_secondary 1586-1648, the body of xi. */
    static void commitSecondary(Lib rpc, XTrigger t) {
        long startVersion = t.startVersion();
        int mutationsLen = t.mutationKeys().size();
        boolean primaryOnly = mutationsLen == 1;
        // integration-tests fail point cut
        CommitReq req;
        if (t.asyncCommit()) {
            req = new CommitReq(t.mutationKeys(), startVersion, t.commitTs());
        } else if (primaryOnly) {
            return;
        } else {
            List<Integer> keys = new ArrayList<>();
            for (int k : t.mutationKeys()) if (k != t.primaryKey()) keys.add(k);
            req = new CommitReq(List.copyOf(keys), startVersion, t.commitTs());
        }
        // resolve_lock(start_version, lock_backoff).retry_multi_region(region_backoff).extract_error()
        // Review round 1, finding 7: the transaction's own lock_backoff (its bound travels in the trigger)
        Lib.extractError(Lib.nonPreserve(rpc.retryMultiRegion(req,
                new ResolveCfg(startVersion, new Backoff(t.lockBackoffMaxAttempts())))));
    }

    // ===================================================================================
    // The processes ti and xi
    // ===================================================================================
    static void tMain(CommunicationModel md, int i) {
        Setup s = Channel.recv(md, Setup.class);
        final Tid self = MpProcess.self();
        Lib lib = new Lib(md, self, s);
        lib.xi = s.xs().get(i - 1);
        if (!s.tTasks().isEmpty()) lib.shardTasks = s.tTasks().get(i - 1);
        Tid c = s.cs().get(i - 1);
        boolean pessimistic = s.pessimistic().get(i - 1);
        Transaction txn = null;
        while (true) {
            MpMessage m = Channel.recv(md, x -> x instanceof Begin || x instanceof Get || x instanceof GetForUpdate
                    || x instanceof Put || x instanceof Commit);
            MpMessage reply;
            try {
                if (m instanceof Begin) {
                    // TransactionClient::begin_with_options (client.rs:226-230), current_timestamp 250-252
                    TransactionOptions options = pessimistic ? TransactionOptions.newPessimistic()
                            : TransactionOptions.newOptimistic();
                    if (s.async()) options = options.useAsyncCommit();
                    if (s.onepc()) options = options.tryOnePc();
                    long timestamp = lib.getTimestamp();
                    txn = new Transaction(timestamp, lib, options, s.a());
                    reply = new Begun();
                } else if (m instanceof Get g) {
                    Integer v = txn.get(g.key());
                    reply = new Value(v == null ? 0 : v);
                } else if (m instanceof GetForUpdate g) {
                    Integer v = txn.getForUpdate(g.key());
                    reply = new Value(v == null ? 0 : v);
                } else if (m instanceof Put p) {
                    txn.put(p.key(), p.value());
                    reply = new Done();
                } else {
                    txn.commit();
                    reply = new Committed();
                }
            } catch (KvError e) {
                reply = new Aborted(); // INTERFACE 5.2: every library error becomes Aborted
            }
            Channel.send(md, c, reply);
        }
    }

    static void xMain(CommunicationModel md, int i) {
        Setup s = Channel.recv(md, Setup.class);
        final Tid self = MpProcess.self();
        Lib lib = new Lib(md, self, s);
        if (!s.xTasks().isEmpty()) lib.shardTasks = s.xTasks().get(i - 1);
        while (true) {
            XTrigger t = Channel.recv(md, XTrigger.class);
            try {
                commitSecondary(lib, t);
            } catch (KvError e) {
                // log::warn!("Failed to commit secondary keys") -> dropped (transaction.rs:1402-1404)
            }
        }
    }
}
