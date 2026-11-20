package bench.scalardb;

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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transliteration of ScalarDB's Consensus Commit (scalar-labs/scalardb at 416cdc2; source cut in
 * plan/case-studies/scalardb/cc-src) to JMC's message-passing API, following
 * plan/case-studies/scalardb/INTERFACE-agents.md. Class, method and record names are ScalarDB's;
 * every changed line carries a {@code // PORT:} comment with the reason. See NOTES.md.
 *
 * <p>Processes (one per concurrent activity of the source): clients {@code c1..}; handles
 * {@code h1..} (the application thread through ConsensusCommit, CrudHandler, Snapshot,
 * CommitHandler, ParticipantCommitHandler, CoordinatorCommitHandler, CoordinatorStateAccessor);
 * recovery tasks {@code xik} (RecoveryExecutor's background task for the read of key k by hi);
 * storage partitions {@code s1..sk}; the coordinator table {@code coord}; {@code init}.
 */
public final class ConsensusCommit implements MpProgram {

  // ===================================================================================
  // Instance parameters (INTERFACE: impl,<pattern>,<k>,<iso>,<fifo|bag>)
  // ===================================================================================

  private final String pattern;
  private final int keys;
  private final Isolation isolation;
  private final CommunicationModel model;
  private final String modelName;
  private final int clients;

  public ConsensusCommit(String... args) {
    if (args.length != 5 || !args[0].equals("impl")) {
      throw new IllegalArgumentException("expected impl,<pattern>,<k>,<iso>,<fifo|bag>");
    }
    this.pattern = args[1];
    this.keys = Integer.parseInt(args[2]);
    switch (args[3]) {
      case "snapshot":
        this.isolation = Isolation.SNAPSHOT;
        break;
      case "serializable":
        this.isolation = Isolation.SERIALIZABLE;
        break;
      default:
        throw new IllegalArgumentException("iso: snapshot or serializable, not " + args[3]);
    }
    switch (args[4]) {
      case "fifo":
        this.model = CommunicationModel.P2P;
        break;
      case "bag":
        this.model = CommunicationModel.ASYN;
        break;
      default:
        throw new IllegalArgumentException("model: fifo or bag, not " + args[4]);
    }
    this.modelName = args[4];
    switch (pattern) {
      case "lost":
        if (keys != 1 && keys != 2) {
          throw new IllegalArgumentException("lost: k in {1, 2}");
        }
        clients = 2;
        break;
      case "skew":
      case "both":
      case "mixed":
        if (keys != 2) {
          throw new IllegalArgumentException(pattern + ": k = 2");
        }
        clients = 2;
        break;
      case "mixedro":
        if (keys != 2) {
          throw new IllegalArgumentException(pattern + ": k = 2");
        }
        clients = 3;
        break;
      default:
        throw new IllegalArgumentException("unknown pattern " + pattern);
    }
  }

  /** The write set of client {@code id} (INTERFACE pattern table). */
  private int[] writesOf(int id) {
    switch (pattern) {
      case "lost":
        return new int[] {1};
      case "skew":
        return id == 1 ? new int[] {1} : new int[] {2};
      case "both":
        return new int[] {1, 2};
      case "mixed":
      case "mixedro":
        return id == 1 ? new int[] {1, 2} : id == 2 ? new int[] {2} : new int[] {};
      default:
        throw new IllegalStateException(pattern);
    }
  }

  @Override
  public String programName() {
    return "scalardb_cc";
  }

  @Override
  public String size() {
    return pattern + "," + keys + "," + (isolation == Isolation.SNAPSHOT ? "snapshot" : "serializable");
  }

  @Override
  public CommunicationModel model() {
    return model;
  }

  @Override
  public List<String> rolesInSpawnOrder() {
    // The invisible processes are daemons (they never terminate), and a daemon has no role.
    List<String> roles = new ArrayList<>();
    for (int i = 1; i <= clients; i++) {
      roles.add("c" + i);
    }
    return roles;
  }

  @Override
  public List<String> visibleRoles() {
    return rolesInSpawnOrder();
  }

  @Override
  public String toString() {
    return "ConsensusCommit(" + size() + "," + modelName + ")";
  }

  // ===================================================================================
  // Visible messages (INTERFACE: client <-> handle)
  // ===================================================================================

  @Tag("Begin")
  public record Begin() implements MpMessage {}

  @Tag("Read")
  public record Read(int key) implements MpMessage {}

  @Tag("Write")
  public record Write(int key, int value) implements MpMessage {}

  @Tag("Commit")
  public record Commit() implements MpMessage {}

  @Tag("Begun")
  public record Begun() implements MpMessage {}

  @Tag("Value")
  public record Value(int key, int value) implements MpMessage {}

  @Tag("Written")
  public record Written(int key) implements MpMessage {}

  @Tag("Committed")
  public record Committed() implements MpMessage {}

  @Tag("Aborted")
  public record Aborted() implements MpMessage {}

  @Tag("Unknown")
  public record Unknown() implements MpMessage {}

  // ===================================================================================
  // Invisible messages
  // ===================================================================================

  /** init -> every invisible process: all addresses, the ids, the isolation, read-only flags. */
  record Setup(
      List<Tid> s,
      Tid coord,
      List<List<Tid>> x,
      List<Tid> h,
      List<Tid> c,
      List<String> txIds,
      String initialTxId,
      Isolation isolation,
      List<Boolean> readOnly)
      implements MpMessage {}

  // PORT: DistributedStorage.get / mutate become request/reply messages to sk (replaced part 3).
  record StorageGet(Tid from, long reqId) implements MpMessage {}

  record StorageGetReply(long reqId, Map<String, Object> record /* null: no record */)
      implements MpMessage {}

  record StorageMutate(Tid from, long reqId, Mutation mutation) implements MpMessage {}

  record StorageMutateReply(long reqId, boolean applied) implements MpMessage {}

  // PORT: the coordinator table's storage.get / storage.put become messages to coord (part 2).
  record CoordGetState(Tid from, long reqId, String id) implements MpMessage {}

  record CoordGetStateReply(long reqId, State state /* null: absent */) implements MpMessage {}

  record CoordPutState(Tid from, long reqId, State state) implements MpMessage {}

  record CoordPutStateReply(long reqId, boolean applied) implements MpMessage {}

  // PORT: RecoveryExecutor's executorService.submit becomes a job to xik; the Future becomes the
  // completion message RecoveryDone (replaced part 5).
  enum RecoveryTask {
    TRY_RECOVER, // l.223-226
    ROLLBACK_RECORD, // l.326-329
    COMMITTED_RESULT_AND_RECOVER // l.139-160
  }

  record RecoveryJob(
      long jobId,
      RecoveryTask task,
      Get selection,
      TransactionResult result,
      State state /* null: Optional.empty() */,
      String transactionId)
      implements MpMessage {}

  /** The exception the source's future would hold, or DONE. */
  enum RecoveryOutcome {
    DONE,
    UNCOMMITTED_RECORD,
    CRUD_CONFLICT,
    CRUD,
    RETRIABLE_EXECUTION,
    EXECUTION,
    COORDINATOR
  }

  record RecoveryDone(long jobId, int key, RecoveryOutcome outcome) implements MpMessage {}

  // ===================================================================================
  // Data carriers (replaced part 8: one table, partition key only, one INT column `value`)
  // ===================================================================================

  enum Isolation {
    READ_COMMITTED,
    SNAPSHOT,
    SERIALIZABLE
  }

  enum TransactionState {
    PREPARED(1),
    DELETED(2),
    COMMITTED(3),
    ABORTED(4),
    UNKNOWN(5);

    private final int id;

    TransactionState(int id) {
      this.id = id;
    }

    public int get() {
      return id;
    }

    public static TransactionState getInstance(int id) {
      for (TransactionState state : TransactionState.values()) {
        if (state.get() == id) {
          return state;
        }
      }
      throw new IllegalArgumentException("invalid transaction state id " + id);
    }
  }

  static final class Attribute {
    static final String ID = "tx_id";
    static final String STATE = "tx_state";
    static final String VERSION = "tx_version";
    static final String PREPARED_AT = "tx_prepared_at";
    static final String COMMITTED_AT = "tx_committed_at";
    static final String BEFORE_PREFIX = "before_";
    static final String BEFORE_ID = BEFORE_PREFIX + ID;
    static final String BEFORE_STATE = BEFORE_PREFIX + STATE;
    static final String BEFORE_VERSION = BEFORE_PREFIX + VERSION;
    static final String BEFORE_PREPARED_AT = BEFORE_PREFIX + PREPARED_AT;
    static final String BEFORE_COMMITTED_AT = BEFORE_PREFIX + COMMITTED_AT;
  }

  // PORT: TransactionTableMetadata of the one table (replaced part 8).
  static final String VALUE = "value";
  static final List<String> AFTER_IMAGE_COLUMN_NAMES =
      List.of(VALUE, Attribute.ID, Attribute.STATE, Attribute.VERSION, Attribute.PREPARED_AT,
          Attribute.COMMITTED_AT);
  static final LinkedHashSet<String> BEFORE_IMAGE_COLUMN_NAMES = new LinkedHashSet<>(List.of(
      Attribute.BEFORE_PREFIX + VALUE, Attribute.BEFORE_ID, Attribute.BEFORE_STATE,
      Attribute.BEFORE_VERSION, Attribute.BEFORE_PREPARED_AT, Attribute.BEFORE_COMMITTED_AT));
  static final List<String> ALL_COLUMN_NAMES;

  static {
    List<String> all = new ArrayList<>(AFTER_IMAGE_COLUMN_NAMES);
    all.addAll(BEFORE_IMAGE_COLUMN_NAMES);
    ALL_COLUMN_NAMES = Collections.unmodifiableList(all);
  }

  /** A record's columns with every column of the table present (null for a null column). */
  static Map<String, Object> columnsOf(Map<String, Object> carried) {
    LinkedHashMap<String, Object> m = new LinkedHashMap<>();
    for (String c : ALL_COLUMN_NAMES) {
      m.put(c, carried.get(c));
    }
    return Collections.unmodifiableMap(m);
  }

  /** Marker for Put, Delete and Get (the source's Operation). */
  interface Operation {
    int key();
  }

  /** A Get on the partition of {@code key} (the source's Get / Selection; also Snapshot.Key). */
  record Get(int key) implements Operation {}

  /** A Put carrying {@code columns} (null values allowed: a column set to null). */
  record Put(int key, Map<String, Object> columns, boolean implicitPreRead, boolean insertMode)
      implements Operation {
    boolean containsColumn(String name) {
      return columns.containsKey(name);
    }
  }

  /** A Delete (no delete is issued by the instance; kept for the composers' shape). */
  record Delete(int key) implements Operation {}

  enum ConditionType {
    NONE,
    PUT_IF_NOT_EXISTS,
    PUT_IF,
    DELETE_IF
  }

  /** {@code column == value}, or {@code column IS NULL} when value is null. */
  record Conjunct(String column, Object value) {}

  record Condition(ConditionType type, List<Conjunct> conjuncts) {}

  /** One conditional Put or Delete sent to a partition (a mutation group holds one mutation). */
  record Mutation(int key, boolean delete, Map<String, Object> columns, Condition condition) {}

  /** The source's TransactionResult over a storage record of the partition {@code key}. */
  record TransactionResult(int key, Map<String, Object> columns, boolean mergedResult) {
    String getId() {
      return (String) columns.get(Attribute.ID);
    }

    TransactionState getState() {
      if (columns.get(Attribute.STATE) == null) {
        return TransactionState.COMMITTED;
      }
      return TransactionState.getInstance((Integer) columns.get(Attribute.STATE));
    }

    int getVersion() {
      Object v = columns.get(Attribute.VERSION);
      // PORT: ScalarDB's Result.getInt returns 0 for a null INT column.
      return v == null ? 0 : (Integer) v;
    }

    long getPreparedAt() {
      Object v = columns.get(Attribute.PREPARED_AT);
      return v == null ? 0L : (Long) v;
    }

    boolean isCommitted() {
      return getState().equals(TransactionState.COMMITTED);
    }

    boolean isDeemedAsCommitted() {
      return getId() == null;
    }

    boolean isMergedResult() {
      return mergedResult;
    }

    boolean hasBeforeImage() {
      return columns.get(Attribute.BEFORE_ID) != null
          || columns.get(Attribute.BEFORE_VERSION) != null;
    }

    Integer getValue() {
      return (Integer) columns.get(VALUE);
    }
  }

  /** CoordinatorStateAccessor.State (no child ids, no write set: replaced part 2). */
  record State(String id, TransactionState state, long createdAt) {
    String getId() {
      return id;
    }

    TransactionState getState() {
      return state;
    }

    long getCreatedAt() {
      return createdAt;
    }
  }

  // ===================================================================================
  // Exceptions (replaced part 9: Java exceptions inside a process). All checked, so that no
  // catch of the source can swallow JMC's control flow (RuntimeExceptions).
  // ===================================================================================

  static class ExecutionException extends Exception {
    ExecutionException(String m) {
      super(m);
    }
  }

  static class NoMutationException extends ExecutionException {
    NoMutationException(String m) {
      super(m);
    }
  }

  static class RetriableExecutionException extends ExecutionException {
    RetriableExecutionException(String m) {
      super(m);
    }
  }

  static class CrudException extends Exception {
    CrudException(String m) {
      super(m);
    }
  }

  static class CrudConflictException extends CrudException {
    CrudConflictException(String m) {
      super(m);
    }
  }

  static class UncommittedRecordException extends CrudConflictException {
    UncommittedRecordException(String m) {
      super(m);
    }
  }

  static class CommitException extends Exception {
    CommitException(String m) {
      super(m);
    }
  }

  static class CommitConflictException extends CommitException {
    CommitConflictException(String m) {
      super(m);
    }
  }

  static class UnknownTransactionStatusException extends Exception {
    UnknownTransactionStatusException(String m) {
      super(m);
    }
  }

  static class PreparationException extends Exception {
    PreparationException(String m) {
      super(m);
    }
  }

  static class PreparationConflictException extends PreparationException {
    PreparationConflictException(String m) {
      super(m);
    }
  }

  static class ValidationException extends Exception {
    ValidationException(String m) {
      super(m);
    }
  }

  static class ValidationConflictException extends ValidationException {
    ValidationConflictException(String m) {
      super(m);
    }
  }

  static class CoordinatorException extends Exception {
    CoordinatorException(String m) {
      super(m);
    }
  }

  static class CoordinatorConflictException extends CoordinatorException {
    private final int numAttempts;

    CoordinatorConflictException(String m, int numAttempts) {
      super(m);
      this.numAttempts = numAttempts;
    }

    int getNumAttempts() {
      return numAttempts;
    }
  }

  /** java.util.concurrent.ExecutionException of a recovery future. */
  static class FutureExecutionException extends Exception {
    FutureExecutionException(Throwable cause) {
      super(cause);
    }
  }

  // ===================================================================================
  // Per-process messaging (the replacement of the DistributedStorage client)
  // ===================================================================================

  /** One process's address book and request counter. */
  final class Io {
    final Tid self;
    final Setup setup;
    long nextId = 0;

    Io(Tid self, Setup setup) {
      this.self = self;
      this.setup = setup;
    }

    void send(Tid dst, MpMessage m) {
      Channel.send(model, dst, m);
    }

    Tid storageOf(int key) {
      return setup.s().get(key - 1);
    }

    /** Waits for the storage reply to one of {@code ids} (any order). */
    MpMessage awaitStorageReply(Set<Long> ids) {
      final Set<Long> want = Set.copyOf(ids);
      return Channel.recv(
          model,
          m -> (m instanceof StorageGetReply r && want.contains(r.reqId()))
              || (m instanceof StorageMutateReply r2 && want.contains(r2.reqId())));
    }
  }

  static long replyId(MpMessage m) {
    if (m instanceof StorageGetReply r) {
      return r.reqId();
    }
    return ((StorageMutateReply) m).reqId();
  }

  static Optional<TransactionResult> resultOf(int key, StorageGetReply r) {
    return r.record() == null
        ? Optional.empty()
        : Optional.of(new TransactionResult(key, r.record(), false));
  }

  /** api/DistributedStorage, as request/reply messages to the partitions. */
  final class DistributedStorage {
    final Io io;

    DistributedStorage(Io io) {
      this.io = io;
    }

    // PORT: storage.get(get) -> StorageGet to sk, wait for its reply.
    Optional<TransactionResult> get(Get get) throws ExecutionException {
      long id = sendGet(get);
      return resultOf(get.key(), (StorageGetReply) io.awaitStorageReply(Set.of(id)));
    }

    // PORT: storage.mutate(mutations) -> StorageMutate to sk, wait; NoMutation reply -> exception.
    void mutate(List<Mutation> mutations) throws ExecutionException {
      if (mutations.size() != 1) {
        throw new IllegalStateException("one mutation per group (one key per partition)");
      }
      long id = sendMutate(mutations.get(0));
      StorageMutateReply r = (StorageMutateReply) io.awaitStorageReply(Set.of(id));
      if (!r.applied()) {
        throw new NoMutationException("no mutation was applied");
      }
    }

    long sendGet(Get get) {
      long id = io.nextId++;
      io.send(io.storageOf(get.key()), new StorageGet(io.self, id));
      return id;
    }

    long sendMutate(Mutation m) {
      long id = io.nextId++;
      io.send(io.storageOf(m.key()), new StorageMutate(io.self, id, m));
      return id;
    }
  }

  // ===================================================================================
  // ParallelExecutor (replaced part 4: no thread pool; a phase sends all its requests and then
  // collects the replies in any order, keeping stopOnError and the one-task shortcut)
  // ===================================================================================

  /** A task: one storage request (start) and the code that runs on its reply (finish). */
  interface ParallelExecutorTask {
    long start();

    void finish(MpMessage reply)
        throws ExecutionException, ValidationConflictException, CrudException;
  }

  final class ParallelExecutor {
    final Io io;
    // PORT: ConsensusCommitConfig defaults (l.144-152): all parallel flags true.
    final boolean parallelPreparationEnabled = true;
    final boolean parallelValidationEnabled = true;
    final boolean parallelCommitEnabled = true;
    final boolean parallelRollbackEnabled = true;

    ParallelExecutor(Io io) {
      this.io = io;
    }

    void prepareRecords(List<ParallelExecutorTask> tasks, String transactionId)
        throws ExecutionException {
      try {
        boolean stopOnError = !parallelPreparationEnabled;
        executeTasks(tasks, parallelPreparationEnabled, stopOnError, "prepareRecords",
            transactionId);
      } catch (ValidationConflictException | CrudException e) {
        throw new AssertionError(
            "Tasks for preparing a transaction should not throw ValidationConflictException and CrudException",
            e);
      }
    }

    void validateRecords(List<ParallelExecutorTask> tasks, String transactionId)
        throws ExecutionException, ValidationConflictException {
      try {
        executeTasks(tasks, parallelValidationEnabled, true, "validateRecords", transactionId);
      } catch (CrudException e) {
        throw new AssertionError("Tasks for validating a transaction should not throw CrudException", e);
      }
    }

    void commitRecords(List<ParallelExecutorTask> tasks, String transactionId)
        throws ExecutionException {
      try {
        executeTasks(tasks, parallelCommitEnabled, false, "commitRecords", transactionId);
      } catch (ValidationConflictException | CrudException e) {
        throw new AssertionError(
            "Tasks for committing a transaction should not throw ValidationConflictException and CrudException",
            e);
      }
    }

    void rollbackRecords(List<ParallelExecutorTask> tasks, String transactionId)
        throws ExecutionException {
      try {
        executeTasks(tasks, parallelRollbackEnabled, false, "rollbackRecords", transactionId);
      } catch (ValidationConflictException | CrudException e) {
        throw new AssertionError(
            "Tasks for rolling back a transaction should not throw ValidationConflictException and CrudException",
            e);
      }
    }

    void readRecordsForRollback(List<ParallelExecutorTask> tasks, String transactionId)
        throws ExecutionException {
      try {
        executeTasks(tasks, parallelRollbackEnabled, true, "readRecordsForRollback", transactionId);
      } catch (ValidationConflictException | CrudException e) {
        throw new AssertionError(
            "Tasks for reading records for a rollback should not throw ValidationConflictException and CrudException",
            e);
      }
    }

    void executeTasks(
        List<ParallelExecutorTask> tasks,
        boolean parallel,
        boolean stopOnError,
        String taskName,
        String transactionId)
        throws ExecutionException, ValidationConflictException, CrudException {
      if (tasks.size() == 1) {
        // If there is only one task, we can run it directly without parallel execution.
        // PORT: run = send the request and wait for its reply.
        ParallelExecutorTask task = tasks.get(0);
        long id = task.start();
        task.finish(io.awaitStorageReply(Set.of(id)));
        return;
      }

      if (parallel) {
        executeTasksInParallel(tasks, stopOnError, taskName, transactionId);
      } else {
        executeTasksSerially(tasks, stopOnError);
      }
    }

    private void executeTasksInParallel(
        List<ParallelExecutorTask> tasks, boolean stopOnError, String taskName, String transactionId)
        throws ExecutionException, ValidationConflictException, CrudException {
      // Submit tasks
      // PORT: the CompletionService's tasks become requests in flight, keyed by request id.
      Map<Long, ParallelExecutorTask> inFlight = new LinkedHashMap<>();
      for (ParallelExecutorTask task : tasks) {
        inFlight.put(task.start(), task);
      }

      Exception throwable = null;

      for (int i = 0; i < tasks.size(); i++) {
        // PORT: completionService.take() -> receive the reply of any request still in flight.
        MpMessage reply = io.awaitStorageReply(inFlight.keySet());
        ParallelExecutorTask task = inFlight.remove(replyId(reply));
        try {
          task.finish(reply);
        } catch (ExecutionException | ValidationConflictException | CrudException cause) {
          if (stopOnError) {
            // PORT: the replies still in flight are never received (dropped; replaced part 3).
            throw cause;
          } else {
            if (throwable == null) {
              throwable = cause;
            } else {
              throwable.addSuppressed(cause);
            }
          }
        }
      }

      // Rethrow exception if necessary
      if (!stopOnError && throwable != null) {
        rethrow(throwable);
      }
    }

    private void executeTasksSerially(List<ParallelExecutorTask> tasks, boolean stopOnError)
        throws ExecutionException, ValidationConflictException, CrudException {
      Exception exception = null;
      for (ParallelExecutorTask task : tasks) {
        try {
          long id = task.start();
          task.finish(io.awaitStorageReply(Set.of(id)));
        } catch (ExecutionException | ValidationConflictException | CrudException e) {
          if (!stopOnError) {
            if (exception == null) {
              exception = e;
            } else {
              exception.addSuppressed(e);
            }
          } else {
            throw e;
          }
        }
      }

      if (!stopOnError && exception != null) {
        rethrow(exception);
      }
    }

    private void rethrow(Exception e)
        throws ExecutionException, ValidationConflictException, CrudException {
      if (e instanceof ExecutionException ee) {
        throw ee;
      }
      if (e instanceof ValidationConflictException ve) {
        throw ve;
      }
      throw (CrudException) e;
    }
  }

  /** A storage mutation task (ParticipantCommitHandler.toTasks l.287-293). */
  ParallelExecutorTask mutateTask(DistributedStorage storage, List<Mutation> mutations) {
    if (mutations.size() != 1) {
      throw new IllegalStateException("one mutation per group (one key per partition)");
    }
    return new ParallelExecutorTask() {
      @Override
      public long start() {
        return storage.sendMutate(mutations.get(0));
      }

      @Override
      public void finish(MpMessage reply) throws ExecutionException {
        if (!((StorageMutateReply) reply).applied()) {
          throw new NoMutationException("no mutation was applied");
        }
      }
    };
  }

  // ===================================================================================
  // Coordinator table: CoordinatorStateAccessor (non-group branches)
  // ===================================================================================

  final class CoordinatorStateAccessor {
    final Io io;

    CoordinatorStateAccessor(Io io) {
      this.io = io;
    }

    Optional<State> getState(String id) throws CoordinatorException {
      // PORT: KEY_MANIPULATOR.isFullKey(id) is false (no group commit, replaced part 10).
      return getStateInternal(id);
    }

    private Optional<State> getStateInternal(String id) throws CoordinatorException {
      String get = createGetWith(id);
      return get(get, id);
    }

    void putState(State state) throws CoordinatorException {
      State put = createPutWith(state);
      put(put, state.getId());
    }

    void forceAbort(String id) throws CoordinatorException {
      // PORT: KEY_MANIPULATOR.isFullKey(id) is false (no group commit, replaced part 10).
      // PORT: System.currentTimeMillis() -> 0 (replaced part 6).
      putState(new State(id, TransactionState.ABORTED, 0L));
    }

    String createGetWith(String id) {
      // PORT: the Get on (coordinator, state) keyed by tx_id is the id itself.
      return id;
    }

    private Optional<State> get(String get, String id) throws CoordinatorException {
      // PORT: storage.get(get) -> GetState to coord; the retry loop on storage failures is dropped
      // since coord never fails (replaced part 2).
      long reqId = io.nextId++;
      io.send(io.setup.coord(), new CoordGetState(io.self, reqId, get));
      CoordGetStateReply r =
          Channel.recv(model, CoordGetStateReply.class, m -> m.reqId() == reqId);
      if (r.state() != null) {
        return Optional.of(r.state());
      } else {
        return Optional.empty();
      }
    }

    State createPutWith(State state) {
      // PORT: the Put with putIfNotExists on tx_id; no child ids, no write set (replaced part 2).
      return state;
    }

    private void put(State put, String id) throws CoordinatorException {
      int counter = 0;
      // PORT: storage.put(put) -> PutState to coord; only NoMutation can come back (part 2).
      long reqId = io.nextId++;
      io.send(io.setup.coord(), new CoordPutState(io.self, reqId, put));
      CoordPutStateReply r =
          Channel.recv(model, CoordPutStateReply.class, m -> m.reqId() == reqId);
      if (!r.applied()) {
        // An earlier attempt may have been applied although it was reported as failed, in which
        // case this attempt conflicted with this writer's own row. Record the number of attempts.
        throw new CoordinatorConflictException("Mutation seems applied already", counter + 1);
      }
    }
  }

  // ===================================================================================
  // Mutation composers
  // ===================================================================================

  interface MutationComposer {
    void add(Operation base, TransactionResult result) throws ExecutionException;

    List<Mutation> get();
  }

  abstract static class AbstractMutationComposer implements MutationComposer {
    protected final String id;
    protected final List<Mutation> mutations;
    protected final long timestamp;

    AbstractMutationComposer(String id, long timestamp) {
      this.id = id;
      this.mutations = new ArrayList<>();
      this.timestamp = timestamp;
    }

    @Override
    public List<Mutation> get() {
      return List.copyOf(mutations);
    }

    static void setBeforeImageColumnsToNull(
        Map<String, Object> putBuilder, Set<String> beforeImageColumnNames) {
      for (String beforeImageColumnName : beforeImageColumnNames) {
        putBuilder.put(beforeImageColumnName, null);
      }
    }
  }

  static Mutation buildPut(int key, Map<String, Object> columns, Condition condition) {
    return new Mutation(key, false, Collections.unmodifiableMap(new LinkedHashMap<>(columns)), condition);
  }

  static Condition putIf(Conjunct... cs) {
    return new Condition(ConditionType.PUT_IF, List.of(cs));
  }

  static Condition deleteIf(Conjunct... cs) {
    return new Condition(ConditionType.DELETE_IF, List.of(cs));
  }

  static final class PrepareMutationComposer extends AbstractMutationComposer {
    PrepareMutationComposer(String id, long timestamp) {
      super(id, timestamp);
    }

    @Override
    public void add(Operation base, TransactionResult result) throws ExecutionException {
      if (base instanceof Put) {
        add((Put) base, result);
      } else if (base instanceof Delete) {
        add((Delete) base, result);
      } else if (base instanceof Get) {
        throw new AssertionError(
            "This path should not be reached since the EXTRA_WRITE strategy is deleted");
      } else {
        throw new AssertionError("PrepareMutationComposer.add only accepts Put, Delete, or Get");
      }
    }

    private void add(Put base, TransactionResult result) throws ExecutionException {
      Map<String, Object> putBuilder = new LinkedHashMap<>();
      putBuilder.putAll(base.columns());

      putBuilder.put(Attribute.ID, id);
      putBuilder.put(Attribute.STATE, TransactionState.PREPARED.get());
      putBuilder.put(Attribute.PREPARED_AT, timestamp);

      Condition condition;
      if (!base.insertMode() && result != null) { // overwrite existing record
        createBeforeColumns(base, result).forEach(putBuilder::put);
        int version = result.getVersion();
        putBuilder.put(Attribute.VERSION, getNextTxVersion(version));

        // check if the record is not interrupted by other conflicting transactions
        if (result.isDeemedAsCommitted()) {
          // record is deemed-commit state
          condition = putIf(new Conjunct(Attribute.ID, null));
        } else {
          condition = putIf(new Conjunct(Attribute.ID, result.getId()));
        }
      } else { // initial record or insert mode enabled
        putBuilder.put(Attribute.VERSION, getNextTxVersion(null));

        // check if the record is not created by other conflicting transactions
        condition = new Condition(ConditionType.PUT_IF_NOT_EXISTS, List.of());
      }

      mutations.add(buildPut(base.key(), putBuilder, condition));
    }

    private void add(Delete base, TransactionResult result) throws ExecutionException {
      Map<String, Object> putBuilder = new LinkedHashMap<>();
      putBuilder.put(Attribute.ID, id);
      putBuilder.put(Attribute.STATE, TransactionState.DELETED.get());
      putBuilder.put(Attribute.PREPARED_AT, timestamp);

      Condition condition;
      if (result != null) {
        createBeforeColumns(base, result).forEach(putBuilder::put);
        int version = result.getVersion();
        putBuilder.put(Attribute.VERSION, getNextTxVersion(version));

        if (result.isDeemedAsCommitted()) {
          condition = putIf(new Conjunct(Attribute.ID, null));
        } else {
          condition = putIf(new Conjunct(Attribute.ID, result.getId()));
        }
      } else {
        putBuilder.put(Attribute.VERSION, getNextTxVersion(null));
        condition = new Condition(ConditionType.PUT_IF_NOT_EXISTS, List.of());
      }

      mutations.add(buildPut(base.key(), putBuilder, condition));
    }

    private Map<String, Object> createBeforeColumns(Operation base, TransactionResult result) {
      Map<String, Object> columns = new LinkedHashMap<>();
      for (Map.Entry<String, Object> column : result.columns().entrySet()) {
        if (isBeforeRequired(base, column.getKey())) {
          if (column.getKey().equals(Attribute.VERSION) && column.getValue() == null) {
            columns.put(Attribute.BEFORE_VERSION, 0);
          } else {
            columns.put(Attribute.BEFORE_PREFIX + column.getKey(), column.getValue());
          }
        }
      }
      return columns;
    }

    private boolean isBeforeRequired(Operation base, String columnName) {
      // PORT: the primary key (the partition) is not a column of the record (replaced part 8).
      return AFTER_IMAGE_COLUMN_NAMES.contains(columnName);
    }
  }

  static int getNextTxVersion(Integer currentTxVersion) {
    if (currentTxVersion == null) {
      return 1;
    } else {
      return currentTxVersion + 1;
    }
  }

  static final class CommitMutationComposer extends AbstractMutationComposer {
    CommitMutationComposer(String id, long timestamp) {
      super(id, timestamp);
    }

    @Override
    public void add(Operation base, TransactionResult result) throws ExecutionException {
      if (base instanceof Put) {
        // for usual commit
        add((Put) base, result);
      } else if (base instanceof Delete) {
        // for usual commit
        add((Delete) base, result);
      } else { // Selection
        assert base instanceof Get;
        add((Get) base, result);
      }
    }

    private void add(Put base, TransactionResult result) {
      mutations.add(composePut(base, result));
    }

    private void add(Delete base, TransactionResult result) {
      mutations.add(composeDelete(base, result));
    }

    private void add(Get base, TransactionResult result) {
      if (result == null) {
        throw new AssertionError(
            "This path should not be reached since the EXTRA_WRITE strategy is deleted");
      } else if (result.getState().equals(TransactionState.PREPARED)) {
        // for rollforward in lazy recovery
        mutations.add(composePut(base, result));
      } else if (result.getState().equals(TransactionState.DELETED)) {
        // for rollforward in lazy recovery
        mutations.add(composeDelete(base, result));
      } else {
        assert result.getState().equals(TransactionState.COMMITTED);
        // The record was committed by the originated one or rolled forward by another transaction
      }
    }

    private Mutation composePut(Operation base, TransactionResult result) {
      Map<String, Object> putBuilder = new LinkedHashMap<>();
      Condition condition =
          putIf(
              new Conjunct(Attribute.ID, id),
              new Conjunct(Attribute.STATE, TransactionState.PREPARED.get()));
      putBuilder.put(Attribute.COMMITTED_AT, timestamp);
      putBuilder.put(Attribute.STATE, TransactionState.COMMITTED.get());

      // Set before image columns to null
      if (result != null) {
        setBeforeImageColumnsToNull(putBuilder, BEFORE_IMAGE_COLUMN_NAMES);
      }

      return buildPut(getPartitionKey(base, result), putBuilder, condition);
    }

    private Mutation composeDelete(Operation base, TransactionResult result) {
      Condition condition =
          deleteIf(
              new Conjunct(Attribute.ID, id),
              new Conjunct(Attribute.STATE, TransactionState.DELETED.get()));
      return new Mutation(getPartitionKey(base, result), true, Map.of(), condition);
    }

    private int getPartitionKey(Operation base, TransactionResult result) {
      if (base instanceof Put || base instanceof Delete) {
        // for usual commit
        return base.key();
      } else {
        assert base instanceof Get;
        if (result != null) {
          // for rollforward in lazy recovery
          return result.key();
        } else {
          throw new AssertionError(
              "This path should not be reached since the EXTRA_WRITE strategy is deleted");
        }
      }
    }
  }

  static final class RollbackMutationComposer extends AbstractMutationComposer {
    private final Map<Integer, TransactionResult> latestRecords;

    RollbackMutationComposer(String id, Map<Integer, TransactionResult> latestRecords) {
      super(id, 0L);
      this.latestRecords = Map.copyOf(latestRecords);
    }

    /** Rollback in either prepare phase in commit or lazy recovery phase in read. */
    @Override
    public void add(Operation base, TransactionResult result) throws ExecutionException {
      TransactionResult latest;
      if (result == null || !Objects.equals(result.getId(), id)) {
        // For rollback in prepare phase, we need to check the latest status of the record.
        latest = getLatestRecord(base).orElse(null);
        if (latest == null) {
          // The record was not prepared (yet) by this transaction or has already been rollback
          // deleted.
          return;
        }
        if (!Objects.equals(latest.getId(), id)) {
          // The record was not prepared (yet) by this transaction or has already been rolled back.
          return;
        }
      } else {
        // For rollback in lazy recovery, we can use the result directly.
        latest = result;
      }

      if (latest.hasBeforeImage()) {
        mutations.add(composePut(base, latest));
      } else {
        // no record to rollback, so it should be deleted
        mutations.add(composeDelete(base, latest));
      }
    }

    private Optional<TransactionResult> getLatestRecord(Operation base) {
      assert base instanceof Put || base instanceof Delete;
      return Optional.ofNullable(latestRecords.get(base.key()));
    }

    private Mutation composePut(Operation base, TransactionResult result) {
      assert result != null
          && (result.getState().equals(TransactionState.PREPARED)
              || result.getState().equals(TransactionState.DELETED));

      int partitionKey = result.key();
      Condition condition =
          putIf(
              new Conjunct(Attribute.ID, id),
              new Conjunct(Attribute.STATE, result.getState().get()));

      Map<String, Object> columns = new LinkedHashMap<>();
      createAfterImageColumnsFromBeforeImage(columns, result, BEFORE_IMAGE_COLUMN_NAMES);
      Map<String, Object> putBuilder = new LinkedHashMap<>(columns);

      // Set before image columns to null
      setBeforeImageColumnsToNull(putBuilder, BEFORE_IMAGE_COLUMN_NAMES);

      return buildPut(partitionKey, putBuilder, condition);
    }

    private Mutation composeDelete(Operation base, TransactionResult result) {
      assert result != null
          && (result.getState().equals(TransactionState.PREPARED)
              || result.getState().equals(TransactionState.DELETED));
      int partitionKey = result.key();
      Condition condition =
          deleteIf(
              new Conjunct(Attribute.ID, id),
              new Conjunct(Attribute.STATE, result.getState().get()));
      return new Mutation(partitionKey, true, Map.of(), condition);
    }
  }

  /** ConsensusCommitUtils.createAfterImageColumnsFromBeforeImage l.424-444. */
  static void createAfterImageColumnsFromBeforeImage(
      Map<String, Object> columns, TransactionResult result, Set<String> beforeImageColumnNames) {
    result
        .columns()
        .forEach(
            (k, v) -> {
              if (beforeImageColumnNames.contains(k)) {
                String columnName = k.substring(Attribute.BEFORE_PREFIX.length());
                if (columnName.equals(Attribute.VERSION) && v != null && (Integer) v == 0) {
                  columns.put(columnName, null);
                } else {
                  columns.put(columnName, v);
                }
              }
            });
  }

  /** ConsensusCommitUtils.rereadRecord l.471-484. */
  static Optional<TransactionResult> rereadRecord(
      DistributedStorage storage, Get selection, TransactionResult observed)
      throws ExecutionException {
    int partitionKey = observed.key();
    return storage.get(new Get(partitionKey));
  }

  // ===================================================================================
  // RecoveryHandler
  // ===================================================================================

  final class RecoveryHandler {
    static final long TRANSACTION_LIFETIME_MILLIS = 15000;
    static final int MAX_CONFLICT_RETRY_COUNT = 10;
    private final DistributedStorage storage;
    private final CoordinatorStateAccessor coordinator;

    RecoveryHandler(DistributedStorage storage, CoordinatorStateAccessor coordinator) {
      this.storage = storage;
      this.coordinator = coordinator;
    }

    void tryRecover(Get selection, TransactionResult result, Optional<State> state)
        throws ExecutionException, CoordinatorException {
      if (state.isPresent()) {
        rollRecordToState(selection, result, state.get());
      } else {
        abortIfExpired(selection, result, false);
      }
    }

    private void rollRecordToState(Get selection, TransactionResult result, State state)
        throws ExecutionException {
      if (state.getState().equals(TransactionState.COMMITTED)) {
        rollforwardRecord(selection, result, state.getCreatedAt());
      } else {
        assert state.getState().equals(TransactionState.ABORTED);
        rollbackRecord(selection, result);
      }
    }

    void rollbackRecord(Get selection, TransactionResult result) throws ExecutionException {
      RollbackMutationComposer composer = createRollbackMutationComposer(selection, result);

      try {
        mutate(composer.get(), result.getId());
      } catch (NoMutationException e) {
        // This can happen when the record has already been rolled back by another transaction. In
        // this case, we just ignore it.
      }
    }

    RollbackMutationComposer createRollbackMutationComposer(Get selection, TransactionResult result)
        throws ExecutionException {
      RollbackMutationComposer composer = new RollbackMutationComposer(result.getId(), Map.of());
      composer.add(selection, result);
      return composer;
    }

    void rollforwardRecord(Get selection, TransactionResult result, long committedAt)
        throws ExecutionException {
      CommitMutationComposer composer = createCommitMutationComposer(selection, result, committedAt);

      try {
        mutate(composer.get(), result.getId());
      } catch (NoMutationException e) {
        // This can happen when the record has already been committed by another transaction. In
        // this case, we just ignore it.
      }
    }

    CommitMutationComposer createCommitMutationComposer(
        Get selection, TransactionResult result, long committedAt) throws ExecutionException {
      CommitMutationComposer composer = new CommitMutationComposer(result.getId(), committedAt);
      composer.add(selection, result);
      return composer;
    }

    boolean tryAbortExpiredTransaction(String id) throws CoordinatorException {
      try {
        coordinator.forceAbort(id);
        return true;
      } catch (CoordinatorConflictException e) {
        return false;
      }
    }

    private boolean abortIfExpired(
        Get selection, TransactionResult result, boolean ensureRecordResolved)
        throws CoordinatorException, ExecutionException {
      if (!isTransactionExpired(result)) {
        return false;
      }

      String txId = result.getId();
      assert txId != null;

      Optional<TransactionResult> latest = rereadRecord(storage, selection, result);
      if (!latest.isPresent() || latest.get().isCommitted() || !txId.equals(latest.get().getId())) {
        return true;
      }

      try {
        coordinator.forceAbort(txId);
      } catch (CoordinatorConflictException e) {
        if (!ensureRecordResolved) {
          return false;
        }

        Optional<State> rereadState = coordinator.getState(txId);

        if (rereadState.isPresent()) {
          rollRecordToState(selection, result, rereadState.get());
          return true;
        }

        return true;
      }

      rollbackRecord(selection, result);
      return true;
    }

    private void mutate(List<Mutation> mutations, String transactionId) throws ExecutionException {
      if (mutations.isEmpty()) {
        return;
      }

      int attempt = 0;
      while (true) {
        try {
          storage.mutate(mutations);
          return;
        } catch (RetriableExecutionException e) {
          // PORT: unreachable, the store never returns a retriable error (replaced part 1).
          if (attempt >= MAX_CONFLICT_RETRY_COUNT) {
            throw e;
          }
          attempt++;
        }
      }
    }

    boolean isTransactionExpired(TransactionResult result) {
      // PORT: current > preparedAt + TRANSACTION_LIFETIME_MILLIS -> a fresh nondeterministic
      // boolean per call (replaced part 6).
      return Nondet.chooseBoolean();
    }
  }

  // ===================================================================================
  // RecoveryExecutor
  // ===================================================================================

  enum RecoveryType {
    RETURN_LATEST_RESULT_AND_RECOVER,
    RETURN_COMMITTED_RESULT_AND_RECOVER,
    RETURN_COMMITTED_RESULT_AND_NOT_RECOVER
  }

  /** The Future of a submitted recovery: the completion message from xik. */
  final class RecoveryFuture {
    final Io io;
    final long jobId; // -1: Futures.immediateFuture(null)
    RecoveryOutcome outcome;

    RecoveryFuture(Io io, long jobId) {
      this.io = io;
      this.jobId = jobId;
      this.outcome = jobId < 0 ? RecoveryOutcome.DONE : null;
    }

    void get() throws FutureExecutionException {
      if (outcome == null) {
        // PORT: recoveryFuture.get() -> wait for xik's RecoveryDone of this job (part 5).
        final long id = jobId;
        RecoveryDone d = Channel.recv(model, RecoveryDone.class, m -> m.jobId() == id);
        outcome = d.outcome();
      }
      switch (outcome) {
        case DONE:
          return;
        case UNCOMMITTED_RECORD:
          throw new FutureExecutionException(new UncommittedRecordException("uncommitted record"));
        case CRUD_CONFLICT:
          throw new FutureExecutionException(new CrudConflictException("recovery conflict"));
        case CRUD:
          throw new FutureExecutionException(new CrudException("recovery failed"));
        case RETRIABLE_EXECUTION:
          throw new FutureExecutionException(new RetriableExecutionException("retriable"));
        case EXECUTION:
          throw new FutureExecutionException(new ExecutionException("execution"));
        default:
          throw new FutureExecutionException(new CoordinatorException("coordinator"));
      }
    }
  }

  static final class RecoveryResult {
    final int key;
    final Optional<TransactionResult> recoveredResult;
    final RecoveryFuture recoveryFuture;
    final boolean rolledBack;

    RecoveryResult(
        int key,
        Optional<TransactionResult> recoveredResult,
        RecoveryFuture recoveryFuture,
        boolean rolledBack) {
      this.key = key;
      this.recoveredResult = recoveredResult;
      this.recoveryFuture = recoveryFuture;
      this.rolledBack = rolledBack;
    }
  }

  final class RecoveryExecutor {
    static final int MAX_RESOLUTION_ATTEMPTS = 5;
    private final Io io;
    private final int handle; // i of hi; the recovery of key k goes to xik
    private final DistributedStorage storage;
    private final CoordinatorStateAccessor coordinator;
    private final RecoveryHandler recovery;

    RecoveryExecutor(
        Io io,
        int handle,
        DistributedStorage storage,
        CoordinatorStateAccessor coordinator,
        RecoveryHandler recovery) {
      this.io = io;
      this.handle = handle;
      this.storage = storage;
      this.coordinator = coordinator;
      this.recovery = recovery;
    }

    // PORT: executorService.submit(...) -> a RecoveryJob to x{handle}{key} (replaced part 5).
    private RecoveryFuture submit(
        RecoveryTask task, Get selection, TransactionResult result, Optional<State> state,
        String transactionId) {
      long jobId = io.nextId++;
      Tid x = io.setup.x().get(handle - 1).get(selection.key() - 1);
      io.send(x, new RecoveryJob(jobId, task, selection, result, state.orElse(null), transactionId));
      return new RecoveryFuture(io, jobId);
    }

    private RecoveryFuture immediateFuture() {
      return new RecoveryFuture(io, -1);
    }

    RecoveryResult execute(
        int key, Get selection, TransactionResult result, String transactionId,
        RecoveryType recoveryType)
        throws CrudException {
      assert !result.isCommitted();

      switch (recoveryType) {
        case RETURN_LATEST_RESULT_AND_RECOVER:
          return resolveLatestResultAndRecover(key, selection, result, transactionId);
        case RETURN_COMMITTED_RESULT_AND_RECOVER:
          RecoveryFuture future =
              submit(RecoveryTask.COMMITTED_RESULT_AND_RECOVER, selection, result,
                  Optional.empty(), transactionId);
          return new RecoveryResult(
              key, createRecordFromBeforeImage(selection, result, transactionId), future, true);
        case RETURN_COMMITTED_RESULT_AND_NOT_RECOVER:
          return new RecoveryResult(
              key, createRecordFromBeforeImage(selection, result, transactionId),
              immediateFuture(), true);
        default:
          throw new AssertionError("Unknown recovery type: " + recoveryType);
      }
    }

    /** The bodies of the submitted lambdas (l.139-160, l.223-226, l.326-329); run on xik. */
    RecoveryOutcome runSubmitted(RecoveryJob job) {
      try {
        switch (job.task()) {
          case COMMITTED_RESULT_AND_RECOVER: {
            Optional<State> s = getCoordinatorState(job.result().getId());
            throwUncommittedRecordExceptionIfTransactionNotExpired(
                s, job.selection(), job.result(), job.transactionId());
            recovery.tryRecover(job.selection(), job.result(), s);
            break;
          }
          case TRY_RECOVER:
            recovery.tryRecover(job.selection(), job.result(), Optional.ofNullable(job.state()));
            break;
          case ROLLBACK_RECORD:
            recovery.rollbackRecord(job.selection(), job.result());
            break;
          default:
            throw new AssertionError(job.task());
        }
        return RecoveryOutcome.DONE;
        // PORT: the exception the Future would hold becomes the completion's outcome (part 9).
      } catch (UncommittedRecordException e) {
        return RecoveryOutcome.UNCOMMITTED_RECORD;
      } catch (CrudConflictException e) {
        return RecoveryOutcome.CRUD_CONFLICT;
      } catch (CrudException e) {
        return RecoveryOutcome.CRUD;
      } catch (RetriableExecutionException e) {
        return RecoveryOutcome.RETRIABLE_EXECUTION;
      } catch (ExecutionException e) {
        return RecoveryOutcome.EXECUTION;
      } catch (CoordinatorException e) {
        return RecoveryOutcome.COORDINATOR;
      }
    }

    private RecoveryResult resolveLatestResultAndRecover(
        int key, Get selection, TransactionResult result, String transactionId)
        throws CrudException {
      TransactionResult current = result;
      for (int attempt = 0; ; attempt++) {
        String txId = current.getId();
        assert txId != null;

        Optional<State> state = getCoordinatorState(txId);

        if (state.isPresent()) {
          boolean rolledBack = state.get().getState() == TransactionState.ABORTED;
          Optional<TransactionResult> recoveredResult =
              createRecoveredResult(state.get(), selection, current, transactionId);
          TransactionResult recovered = current;
          RecoveryFuture future =
              submit(RecoveryTask.TRY_RECOVER, selection, recovered, state, transactionId);
          return new RecoveryResult(key, recoveredResult, future, rolledBack);
        }

        throwUncommittedRecordExceptionIfTransactionNotExpired(
            state, selection, current, transactionId);

        Optional<TransactionResult> fresh;
        try {
          fresh = rereadRecord(storage, selection, current);
        } catch (ExecutionException e) {
          throw new CrudException("recovering records failed: " + e.getMessage());
        }

        if (!fresh.isPresent()) {
          return new RecoveryResult(key, Optional.empty(), immediateFuture(), true);
        }
        if (fresh.get().isCommitted()) {
          return new RecoveryResult(key, fresh, immediateFuture(), false);
        }
        if (!txId.equals(fresh.get().getId())) {
          checkReadRetryLimit(attempt, transactionId);
          current = fresh.get();
          continue;
        }

        boolean aborted;
        try {
          aborted = recovery.tryAbortExpiredTransaction(txId);
        } catch (CoordinatorException e) {
          throw new CrudException("recovering records failed: " + e.getMessage());
        }

        if (aborted) {
          Optional<TransactionResult> afterAbort;
          try {
            afterAbort = rereadRecord(storage, selection, current);
          } catch (ExecutionException e) {
            throw new CrudException("recovering records failed: " + e.getMessage());
          }
          if (!afterAbort.isPresent()) {
            return new RecoveryResult(key, Optional.empty(), immediateFuture(), true);
          }
          if (afterAbort.get().isCommitted()) {
            return new RecoveryResult(key, afterAbort, immediateFuture(), false);
          }
          if (!txId.equals(afterAbort.get().getId())) {
            checkReadRetryLimit(attempt, transactionId);
            current = afterAbort.get();
            continue;
          }

          Optional<TransactionResult> recoveredResult =
              createRecordFromBeforeImage(selection, current, transactionId);
          TransactionResult recovered = current;
          RecoveryFuture future =
              submit(RecoveryTask.ROLLBACK_RECORD, selection, recovered, Optional.empty(),
                  transactionId);
          return new RecoveryResult(key, recoveredResult, future, true);
        }

        checkReadRetryLimit(attempt, transactionId);
      }
    }

    private void checkReadRetryLimit(int attempt, String transactionId)
        throws CrudConflictException {
      if (attempt >= MAX_RESOLUTION_ATTEMPTS - 1) {
        throw new CrudConflictException(
            "resolving an uncommitted record exceeded the retry limit: " + transactionId);
      }
    }

    private Optional<State> getCoordinatorState(String transactionId) throws CrudException {
      try {
        return coordinator.getState(transactionId);
      } catch (CoordinatorException e) {
        throw new CrudException("recovering records failed: " + e.getMessage());
      }
    }

    private Optional<TransactionResult> createRecoveredResult(
        State state, Get selection, TransactionResult result, String transactionId)
        throws CrudException {
      if (state.getState() == TransactionState.ABORTED) {
        return createRecordFromBeforeImage(selection, result, transactionId);
      }
      assert state.getState() == TransactionState.COMMITTED;
      return createResultFromAfterImage(selection, result, transactionId);
    }

    private void throwUncommittedRecordExceptionIfTransactionNotExpired(
        Optional<State> state, Get selection, TransactionResult result, String transactionId)
        throws CrudException {
      if (!state.isPresent() && !recovery.isTransactionExpired(result)) {
        throw new UncommittedRecordException(
            "read uncommitted record of " + result.getId() + " in " + transactionId);
      }
    }

    private Optional<TransactionResult> createRecordFromBeforeImage(
        Get selection, TransactionResult result, String transactionId) throws CrudException {
      if (!result.hasBeforeImage()) {
        return Optional.empty();
      }

      Map<String, Object> columns = new HashMap<>();

      createAfterImageColumnsFromBeforeImage(columns, result, BEFORE_IMAGE_COLUMN_NAMES);

      // PORT: the partition key is the record's partition, not a column (replaced part 8).

      addNullBeforeImageColumns(columns, BEFORE_IMAGE_COLUMN_NAMES);

      return Optional.of(new TransactionResult(result.key(), columnsOf(columns), false));
    }

    private Optional<TransactionResult> createResultFromAfterImage(
        Get selection, TransactionResult result, String transactionId) throws CrudException {
      if (result.getState() == TransactionState.DELETED) {
        return Optional.empty();
      }

      assert result.getState() == TransactionState.PREPARED;

      Map<String, Object> columns = new HashMap<>();
      result
          .columns()
          .forEach(
              (columnName, column) -> {
                if (columnName.equals(Attribute.STATE)) {
                  // Set the state to COMMITTED
                  columns.put(Attribute.STATE, TransactionState.COMMITTED.get());
                } else {
                  columns.put(columnName, column);
                }
              });

      long committedAt = getCommittedAt();
      columns.put(Attribute.COMMITTED_AT, committedAt);

      addNullBeforeImageColumns(columns, BEFORE_IMAGE_COLUMN_NAMES);

      return Optional.of(new TransactionResult(result.key(), columnsOf(columns), false));
    }

    long getCommittedAt() {
      // PORT: System.currentTimeMillis() -> 0 (replaced part 6).
      return 0L;
    }

    private void addNullBeforeImageColumns(
        Map<String, Object> columns, LinkedHashSet<String> beforeImageColumnNames) {
      for (String beforeImageColumnName : beforeImageColumnNames) {
        columns.put(beforeImageColumnName, null);
      }
    }
  }

  // ===================================================================================
  // Snapshot and TransactionContext
  // ===================================================================================

  final class Snapshot {
    private final String id;
    private final ParallelExecutor parallelExecutor;
    // PORT: ConcurrentHashMap / HashMap iteration order is unspecified; ascending key order.
    private final Map<Integer, Optional<TransactionResult>> readSet = new TreeMap<>();
    private final Map<Get, Optional<TransactionResult>> getSet = new LinkedHashMap<>();
    private final Map<Integer, Put> writeSet = new TreeMap<>();
    private final Map<Integer, Delete> deleteSet = new TreeMap<>();

    Snapshot(String id, ParallelExecutor parallelExecutor) {
      this.id = id;
      this.parallelExecutor = parallelExecutor;
    }

    void putIntoReadSet(int key, Optional<TransactionResult> result) {
      readSet.put(key, result);
    }

    void putIntoGetSet(Get get, Optional<TransactionResult> result) {
      getSet.put(get, result);
    }

    void putIntoWriteSet(int key, Put put) throws CrudException {
      if (deleteSet.containsKey(key)) {
        Map<String, Object> putBuilder = new LinkedHashMap<>(put.columns());
        if (!put.containsColumn(VALUE)) {
          putBuilder.put(VALUE, null);
        }
        // Disable insert mode; enable implicit pre-read.
        Put mergedPut = new Put(key, Collections.unmodifiableMap(putBuilder), true, false);
        deleteSet.remove(key);
        writeSet.put(key, mergedPut);
      } else if (writeSet.containsKey(key)) {
        if (put.insertMode()) {
          throw new IllegalArgumentException("inserting already written data is not allowed");
        }

        // merge the previous put in the write set and the new put
        Put originalPut = writeSet.get(key);
        Map<String, Object> putBuilder = new LinkedHashMap<>(originalPut.columns());
        putBuilder.putAll(put.columns());
        boolean implicitPreRead = originalPut.implicitPreRead();

        if (put.implicitPreRead() && !originalPut.insertMode()) {
          implicitPreRead = true;
        }

        writeSet.put(
            key,
            new Put(key, Collections.unmodifiableMap(putBuilder), implicitPreRead,
                originalPut.insertMode()));
      } else {
        writeSet.put(key, put);
      }
    }

    boolean containsKeyInReadSet(int key) {
      return readSet.containsKey(key);
    }

    boolean containsKeyInGetSet(Get get) {
      return getSet.containsKey(get);
    }

    boolean containsKeyInWriteSet(int key) {
      return writeSet.containsKey(key);
    }

    boolean containsKeyInDeleteSet(int key) {
      return deleteSet.containsKey(key);
    }

    boolean hasWritesOrDeletes() {
      return !writeSet.isEmpty() || !deleteSet.isEmpty();
    }

    Map<Get, Optional<TransactionResult>> getGetSet() {
      return getSet;
    }

    Map<Integer, Put> getWriteSet() {
      return writeSet;
    }

    Map<Integer, Delete> getDeleteSet() {
      return deleteSet;
    }

    Optional<TransactionResult> getResult(int key) throws CrudException {
      Optional<TransactionResult> result = readSet.getOrDefault(key, Optional.empty());
      return mergeResult(key, result);
    }

    Optional<TransactionResult> getResult(int key, Get get) throws CrudException {
      Optional<TransactionResult> result = getSet.getOrDefault(get, Optional.empty());
      // PORT: no conjunctions in the instance (Get by partition key only).
      return mergeResult(key, result, true);
    }

    private Optional<TransactionResult> mergeResult(int key, Optional<TransactionResult> result)
        throws CrudException {
      if (deleteSet.containsKey(key)) {
        return Optional.empty();
      } else if (writeSet.containsKey(key)) {
        // merge the result in the read set and the put in the write set
        // PORT: MergedResult: the put's columns over the read result's.
        Map<String, Object> merged = new LinkedHashMap<>();
        result.ifPresent(r -> merged.putAll(r.columns()));
        merged.putAll(writeSet.get(key).columns());
        return Optional.of(new TransactionResult(key, columnsOf(merged), true));
      } else {
        return result;
      }
    }

    Optional<TransactionResult> mergeResult(
        int key, Optional<TransactionResult> result, boolean conjunctionsEmpty)
        throws CrudException {
      Optional<TransactionResult> ret = mergeResult(key, result);

      if (conjunctionsEmpty) {
        // We can just return the result without checking the condition if there is no condition.
        return ret;
      }
      throw new AssertionError("no conjunctions in the instance");
    }

    void to(MutationComposer composer) throws ExecutionException {
      for (Map.Entry<Integer, Put> entry : writeSet.entrySet()) {
        TransactionResult result =
            readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
        composer.add(entry.getValue(), result);
      }
      for (Map.Entry<Integer, Delete> entry : deleteSet.entrySet()) {
        TransactionResult result =
            readSet.containsKey(entry.getKey()) ? readSet.get(entry.getKey()).orElse(null) : null;
        composer.add(entry.getValue(), result);
      }
    }

    void toSerializable(DistributedStorage storage)
        throws ExecutionException, ValidationConflictException {
      List<ParallelExecutorTask> tasks = new ArrayList<>();

      // PORT: no scan set and no scanner set in the instance.

      // Get set is re-validated to check if there is no anti-dependency
      for (Map.Entry<Get, Optional<TransactionResult>> entry : getSet.entrySet()) {
        Get get = entry.getKey();
        // PORT: no Get with index in the instance.
        int key = get.key();
        if (writeSet.containsKey(key) || deleteSet.containsKey(key)) {
          continue;
        }

        tasks.add(validateGetResult(storage, get, entry.getValue()));
      }

      parallelExecutor.validateRecords(tasks, id);
    }

    // PORT: the task's storage.get becomes its request; the rest runs on the reply.
    private ParallelExecutorTask validateGetResult(
        DistributedStorage storage, Get get, Optional<TransactionResult> originalResult) {
      return new ParallelExecutorTask() {
        @Override
        public long start() {
          // Check if a read record is not changed
          return storage.sendGet(get);
        }

        @Override
        public void finish(MpMessage reply) throws ValidationConflictException {
          Optional<TransactionResult> latestResult =
              resultOf(get.key(), (StorageGetReply) reply);

          if (isChanged(latestResult, originalResult)) {
            throwExceptionDueToAntiDependency();
          }
        }
      };
    }

    private boolean isChanged(
        Optional<TransactionResult> latestResult, Optional<TransactionResult> result) {
      if (latestResult.isPresent() != result.isPresent()) {
        return true;
      }
      if (!latestResult.isPresent()) {
        return false;
      }
      return isChanged(latestResult.get(), result.get());
    }

    private boolean isChanged(TransactionResult latestResult, TransactionResult result) {
      return !Objects.equals(latestResult.getId(), result.getId());
    }

    private void throwExceptionDueToAntiDependency() throws ValidationConflictException {
      throw new ValidationConflictException("anti-dependency found in " + id);
    }
  }

  static final class TransactionContext {
    final String transactionId;
    final Snapshot snapshot;
    final Isolation isolation;
    final boolean readOnly;
    final boolean oneOperation;
    final List<RecoveryResult> recoveryResults = new ArrayList<>();

    TransactionContext(
        String transactionId, Snapshot snapshot, Isolation isolation, boolean readOnly,
        boolean oneOperation) {
      this.transactionId = transactionId;
      this.snapshot = snapshot;
      this.isolation = isolation;
      this.readOnly = readOnly;
      this.oneOperation = oneOperation;
    }

    boolean isSnapshotReadRequired() {
      return isolation != Isolation.READ_COMMITTED;
    }

    boolean isValidationPossiblyRequired() {
      return isolation == Isolation.SERIALIZABLE;
    }

    boolean isValidationRequired() {
      // Only SERIALIZABLE isolation level requires validation
      if (isolation != Isolation.SERIALIZABLE) {
        return false;
      }

      // PORT: the scan set and the scanner set are empty in the instance.

      // If all the records in the get set are included in the write set or delete set, no
      // validation is required
      return snapshot.getGetSet().keySet().stream()
          .anyMatch(
              get -> {
                int key = get.key();
                return !snapshot.containsKeyInWriteSet(key) && !snapshot.containsKeyInDeleteSet(key);
              });
    }
  }

  // ===================================================================================
  // CrudHandler
  // ===================================================================================

  final class CrudHandler {
    private final DistributedStorage storage;
    private final RecoveryExecutor recoveryExecutor;

    CrudHandler(DistributedStorage storage, RecoveryExecutor recoveryExecutor) {
      this.storage = storage;
      this.recoveryExecutor = recoveryExecutor;
    }

    Optional<TransactionResult> get(Get get, TransactionContext context) throws CrudException {
      // PORT: no secondary index in the instance.
      int key = get.key();

      if (isSnapshotReadRequired(context)) {
        readUnread(key, get, context);
        // PORT: FilteredResult (projections) dropped: the handle uses the value column.
        return context.snapshot.getResult(key, get);
      } else {
        Optional<TransactionResult> result = read(key, get, context);
        return context.snapshot.mergeResult(key, result, true);
      }
    }

    void readUnread(int key, Get get, TransactionContext context) throws CrudException {
      if (!context.snapshot.containsKeyInGetSet(get)) {
        read(key, get, context);
      }
    }

    Optional<TransactionResult> read(int originalKey, Get get, TransactionContext context)
        throws CrudException {
      // PORT: no before-index check (no index) in the instance; the loop runs once.
      for (int i = 0; ; i++) {
        int key = originalKey;
        Optional<TransactionResult> result;
        boolean indexKeyFilteredOut;

        // PORT: the index branch is not used (single-row Get, l.241-253).
        Optional<TransactionResult> fromStorage = getFromStorage(get, context.transactionId);
        if (fromStorage.isPresent()) {
          RecoveredResult recovered = recoverAndFilter(key, get, fromStorage.get(), context);
          result = recovered.result;
          indexKeyFilteredOut = recovered.indexKeyFilteredOut;
        } else {
          result = Optional.empty();
          indexKeyFilteredOut = false;
        }

        // Put the result in the snapshot.
        if (result.isPresent() || (/* get.getConjunctions().isEmpty() */ true && !indexKeyFilteredOut)) {
          putIntoReadSetInSnapshot(key, result, context);
        }
        putIntoGetSetInSnapshot(get, result, context);

        return result;
      }
    }

    private RecoveredResult recoverAndFilter(
        int key, Get selection, TransactionResult result, TransactionContext context)
        throws CrudException {
      Optional<TransactionResult> ret;
      boolean indexKeyFilteredOut = false;
      if (!result.isCommitted()) {
        // Lazy recovery
        RecoveryResult recoveryResult = executeRecovery(key, selection, result, context);
        // PORT: no secondary index: the index-key filter does not apply.
        ret = recoveryResult.recoveredResult;
      } else {
        ret = Optional.of(result);
      }

      // PORT: no conjunctions in the instance.

      return new RecoveredResult(ret, indexKeyFilteredOut);
    }

    private RecoveryResult executeRecovery(
        int key, Get selection, TransactionResult result, TransactionContext context)
        throws CrudException {
      RecoveryType recoveryType;
      if (context.isolation == Isolation.READ_COMMITTED) {
        if (context.readOnly) {
          recoveryType = RecoveryType.RETURN_COMMITTED_RESULT_AND_NOT_RECOVER;
        } else {
          recoveryType = RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER;
        }
      } else {
        recoveryType = RecoveryType.RETURN_LATEST_RESULT_AND_RECOVER;
      }

      RecoveryResult recoveryResult =
          recoveryExecutor.execute(key, selection, result, context.transactionId, recoveryType);
      context.recoveryResults.add(recoveryResult);
      return recoveryResult;
    }

    private void putIntoReadSetInSnapshot(
        int key, Optional<TransactionResult> result, TransactionContext context) {
      // In read-only mode, we don't need to put the result into the read set
      if (!context.readOnly && !context.snapshot.containsKeyInReadSet(key)) {
        context.snapshot.putIntoReadSet(key, result);
      }
    }

    private boolean isSnapshotReadRequired(TransactionContext context) {
      return !context.oneOperation && context.isSnapshotReadRequired();
    }

    private boolean isValidationOrSnapshotReadRequired(TransactionContext context) {
      return context.isValidationPossiblyRequired() || isSnapshotReadRequired(context);
    }

    private void putIntoGetSetInSnapshot(
        Get get, Optional<TransactionResult> result, TransactionContext context) {
      if (isValidationOrSnapshotReadRequired(context)) {
        context.snapshot.putIntoGetSet(get, result);
      }
    }

    void put(Put put, TransactionContext context) throws CrudException {
      int key = put.key();
      // PORT: the clients' upserts carry no condition (INTERFACE configuration), so the condition
      // checks of l.686-700 never apply.
      context.snapshot.putIntoWriteSet(key, put);
    }

    void readIfImplicitPreReadEnabled(TransactionContext context) throws CrudException {
      List<Integer> tasks = new ArrayList<>();

      for (Map.Entry<Integer, Put> entry : context.snapshot.getWriteSet().entrySet()) {
        Put put = entry.getValue();
        if (put.implicitPreRead()) {
          int key = entry.getKey();
          if (!context.snapshot.containsKeyInReadSet(key)) {
            tasks.add(key);
          }
        }
      }

      for (Map.Entry<Integer, Delete> entry : context.snapshot.getDeleteSet().entrySet()) {
        int key = entry.getKey();
        if (!context.snapshot.containsKeyInReadSet(key)) {
          tasks.add(key);
        }
      }

      if (!tasks.isEmpty()) {
        // PORT: executeImplicitPreRead runs the reads one after another (each read is several
        // round trips); not reached in the instance, where every written key is read first.
        for (int key : tasks) {
          read(key, new Get(key), context);
        }
      }
    }

    void waitForRecoveryCompletionIfNecessary(TransactionContext context) throws CrudException {
      for (RecoveryResult recoveryResult : context.recoveryResults) {
        if (context.snapshot.containsKeyInWriteSet(recoveryResult.key)
            || context.snapshot.containsKeyInDeleteSet(recoveryResult.key)
            || context.isValidationPossiblyRequired()) {
          waitForRecoveryCompletion(recoveryResult, context.transactionId);
        }
      }
    }

    private void waitForRecoveryCompletion(RecoveryResult recoveryResult, String transactionId)
        throws CrudException {
      try {
        recoveryResult.recoveryFuture.get();
      } catch (FutureExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof CrudException) {
          throw (CrudException) cause;
        }
        if (cause instanceof RetriableExecutionException) {
          throw new CrudConflictException(
              "conflict occurred when recovering records: " + cause.getMessage());
        }

        throw new CrudException("recovering records failed: " + cause.getMessage());
      }
    }

    Optional<TransactionResult> getFromStorage(Get get, String transactionId)
        throws CrudException {
      try {
        return storage.get(get);
      } catch (ExecutionException e) {
        throw new CrudException("reading record from storage failed: " + e.getMessage());
      }
    }
  }

  static final class RecoveredResult {
    final Optional<TransactionResult> result;
    final boolean indexKeyFilteredOut;

    RecoveredResult(Optional<TransactionResult> result, boolean indexKeyFilteredOut) {
      this.result = result;
      this.indexKeyFilteredOut = indexKeyFilteredOut;
    }
  }

  // ===================================================================================
  // Commit: CommitHandler, ParticipantCommitHandler, CoordinatorCommitHandler
  // ===================================================================================

  final class ParticipantCommitHandler {
    private final DistributedStorage storage;
    private final RecoveryExecutor recoveryExecutor;
    private final ParallelExecutor parallelExecutor;

    ParticipantCommitHandler(
        DistributedStorage storage, RecoveryExecutor recoveryExecutor,
        ParallelExecutor parallelExecutor) {
      this.storage = storage;
      this.recoveryExecutor = recoveryExecutor;
      this.parallelExecutor = parallelExecutor;
    }

    void prepareRecords(TransactionContext context, long preparedAt) throws PreparationException {
      try {
        PrepareMutationComposer composer =
            new PrepareMutationComposer(context.transactionId, preparedAt);
        context.snapshot.to(composer);
        List<List<Mutation>> groupedMutations = groupMutations(composer.get());

        List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
        parallelExecutor.prepareRecords(tasks, context.transactionId);
      } catch (NoMutationException e) {
        throw new PreparationConflictException("conditional mutation not applied");
      } catch (RetriableExecutionException e) {
        throw new PreparationConflictException("conflict occurred when preparing records");
      } catch (ExecutionException e) {
        throw new PreparationException("preparing records failed");
      }
    }

    void validateRecords(TransactionContext context) throws ValidationException {
      if (!context.isValidationRequired()) {
        return;
      }

      try {
        // validation is executed when SERIALIZABLE is chosen.
        context.snapshot.toSerializable(storage);
      } catch (ExecutionException e) {
        throw new ValidationException("validation failed: " + e.getMessage());
      }
    }

    void commitRecords(TransactionContext context, long committedAt) {
      // PORT: AsyncExecutor inline (asynchronous commit off, replaced part 4).
      doCommitRecords(context, committedAt);
    }

    private void doCommitRecords(TransactionContext context, long committedAt) {
      try {
        CommitMutationComposer composer =
            new CommitMutationComposer(context.transactionId, committedAt);
        context.snapshot.to(composer);
        List<List<Mutation>> groupedMutations = groupMutations(composer.get());

        List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
        parallelExecutor.commitRecords(tasks, context.transactionId);
      } catch (ExecutionException e) { // PORT: catch (Exception) narrowed so JMC control flow passes
        // ignore since records are recovered lazily
      }
    }

    void rollbackRecords(TransactionContext context) {
      // PORT: AsyncExecutor inline (asynchronous rollback off, replaced part 4).
      doRollbackRecords(context);
    }

    private void doRollbackRecords(TransactionContext context) {
      Map<Integer, TransactionResult> latestRecords = new ConcurrentHashMap<>();

      try {
        parallelExecutor.readRecordsForRollback(
            toReadTasks(getKeysOfWritesAndDeletes(context), latestRecords), context.transactionId);

        RollbackMutationComposer composer =
            new RollbackMutationComposer(context.transactionId, latestRecords);
        context.snapshot.to(composer);
        List<List<Mutation>> groupedMutations = groupMutations(composer.get());

        List<ParallelExecutorTask> tasks = toTasks(groupedMutations);
        parallelExecutor.rollbackRecords(tasks, context.transactionId);
      } catch (ExecutionException e) { // PORT: catch (Exception) narrowed so JMC control flow passes
        // ignore since records are recovered lazily
      }

      tryRecoverRecordsBlockingWrites(context, getKeysOfWritesNeverRead(context), latestRecords);
    }

    private void tryRecoverRecordsBlockingWrites(
        TransactionContext context, List<Integer> keys, Map<Integer, TransactionResult> latestRecords) {
      for (int key : keys) {
        TransactionResult latestRecord = latestRecords.get(key);
        if (latestRecord == null
            || latestRecord.isCommitted()
            || context.transactionId.equals(latestRecord.getId())) {
          continue;
        }

        try {
          RecoveryResult recoveryResult =
              recoveryExecutor.execute(
                  key,
                  new Get(key),
                  latestRecord,
                  context.transactionId,
                  RecoveryType.RETURN_COMMITTED_RESULT_AND_RECOVER);
          context.recoveryResults.add(recoveryResult);
        } catch (CrudException e) { // PORT: catch (Exception) narrowed
          // Recovering a record blocking a write failed
        }
      }
    }

    private List<Integer> getKeysOfWritesNeverRead(TransactionContext context) {
      List<Integer> keys = new ArrayList<>();
      for (Map.Entry<Integer, Put> entry : context.snapshot.getWriteSet().entrySet()) {
        if (!context.snapshot.containsKeyInReadSet(entry.getKey())) {
          keys.add(entry.getKey());
        }
      }
      return keys;
    }

    private List<Integer> getKeysOfWritesAndDeletes(TransactionContext context) {
      List<Integer> keys = new ArrayList<>();
      keys.addAll(context.snapshot.getWriteSet().keySet());
      keys.addAll(context.snapshot.getDeleteSet().keySet());
      return keys;
    }

    // PORT: each read task's storage.get becomes its request; the rest runs on the reply.
    private List<ParallelExecutorTask> toReadTasks(
        List<Integer> keys, Map<Integer, TransactionResult> latestRecords) {
      List<ParallelExecutorTask> tasks = new ArrayList<>(keys.size());
      for (int key : keys) {
        tasks.add(
            new ParallelExecutorTask() {
              @Override
              public long start() {
                return storage.sendGet(new Get(key));
              }

              @Override
              public void finish(MpMessage reply) {
                resultOf(key, (StorageGetReply) reply)
                    .ifPresent(result -> latestRecords.put(key, result));
              }
            });
      }
      return tasks;
    }

    private List<ParallelExecutorTask> toTasks(List<List<Mutation>> groupedMutations) {
      List<ParallelExecutorTask> tasks = new ArrayList<>(groupedMutations.size());
      for (List<Mutation> mutations : groupedMutations) {
        tasks.add(mutateTask(storage, mutations));
      }
      return tasks;
    }

    boolean canOnePhaseCommit(TransactionContext context) {
      // PORT: one-phase commit off (ConsensusCommitConfig default, replaced part 10).
      return false;
    }
  }

  /** MutationsGrouper.groupMutations: every key is its own partition, one mutation per group. */
  static List<List<Mutation>> groupMutations(List<Mutation> mutations) {
    List<List<Mutation>> batches = new ArrayList<>();
    for (Mutation m : mutations) {
      batches.add(List.of(m));
    }
    return batches;
  }

  final class CoordinatorCommitHandler {
    private final CoordinatorStateAccessor coordinator;

    CoordinatorCommitHandler(CoordinatorStateAccessor coordinator) {
      this.coordinator = coordinator;
    }

    long commitState(String id) throws CommitConflictException, UnknownTransactionStatusException {
      try {
        // PORT: System.currentTimeMillis() -> 0 (replaced part 6).
        long committedAt = 0L;
        State state = new State(id, TransactionState.COMMITTED, committedAt);
        coordinator.putState(state);
        return committedAt;
      } catch (CoordinatorConflictException e) {
        return handleCommitConflict(id, e);
      } catch (CoordinatorException e) {
        throw new UnknownTransactionStatusException("unknown coordinator status");
      }
    }

    long handleCommitConflict(String id, Exception cause)
        throws CommitConflictException, UnknownTransactionStatusException {
      boolean mayHaveConflictedWithOwnWrite =
          cause instanceof CoordinatorConflictException
              && ((CoordinatorConflictException) cause).getNumAttempts() > 1;
      try {
        Optional<State> s = coordinator.getState(id);
        if (s.isPresent()) {
          State persisted = s.get();
          if (persisted.getState() == TransactionState.ABORTED) {
            if (mayHaveConflictedWithOwnWrite) {
              throw new UnknownTransactionStatusException(
                  "conflict occurred when committing state after retry");
            }
            throw new CommitConflictException("conflict occurred when committing state");
          }
          return persisted.getCreatedAt();
        } else {
          if (mayHaveConflictedWithOwnWrite) {
            throw new UnknownTransactionStatusException(
                "conflict occurred when committing state after retry");
          }
          throw new CommitConflictException("conflict occurred when committing state");
        }
      } catch (CoordinatorException ex) {
        throw new UnknownTransactionStatusException("cannot get coordinator status");
      }
    }

    TransactionState abortState(String id) throws UnknownTransactionStatusException {
      try {
        // PORT: System.currentTimeMillis() -> 0 (replaced part 6).
        State state = new State(id, TransactionState.ABORTED, 0L);
        coordinator.putState(state);
        return TransactionState.ABORTED;
      } catch (CoordinatorConflictException e) {
        return readCoordinatorStateAfterAbortConflict(id, e).orElse(TransactionState.ABORTED);
      } catch (CoordinatorException e) {
        throw new UnknownTransactionStatusException("unknown coordinator status");
      }
    }

    private Optional<TransactionState> readCoordinatorStateAfterAbortConflict(
        String id, CoordinatorConflictException e) throws UnknownTransactionStatusException {
      try {
        return coordinator.getState(id).map(State::getState);
      } catch (CoordinatorException e1) {
        throw new UnknownTransactionStatusException("cannot get coordinator status");
      }
    }
  }

  final class CommitHandler {
    private final ParticipantCommitHandler participantCommitHandler;
    private final CoordinatorCommitHandler coordinatorCommitHandler;
    // PORT: ConsensusCommitConfig default (l.159-162): coordinator write omission on read-only on.
    private final boolean coordinatorWriteOmissionOnReadOnlyEnabled = true;

    CommitHandler(
        ParticipantCommitHandler participantCommitHandler,
        CoordinatorCommitHandler coordinatorCommitHandler) {
      this.participantCommitHandler = participantCommitHandler;
      this.coordinatorCommitHandler = coordinatorCommitHandler;
    }

    void commit(TransactionContext context)
        throws CommitException, UnknownTransactionStatusException {
      boolean hasWritesOrDeletesInSnapshot =
          !context.readOnly && context.snapshot.hasWritesOrDeletes();

      if (canOnePhaseCommit(context)) {
        throw new AssertionError("one-phase commit is off");
      }

      // PORT: no before-preparation hook (never set, replaced part 10). The calls of
      // safelyCallOnFailureBeforeCommit (l.99-107, a no-op callback outside group commit) and the
      // catch (Exception e) { ...; throw e; } clauses around them are dropped.

      if (hasWritesOrDeletesInSnapshot) {
        try {
          // PORT: System.currentTimeMillis() -> 0 (replaced part 6).
          prepareRecords(context, 0L);
        } catch (PreparationException e) {
          abortState(context);
          rollbackRecords(context);
          if (e instanceof PreparationConflictException) {
            throw new CommitConflictException(e.getMessage());
          }
          throw new CommitException(e.getMessage());
        }
      }

      try {
        validateRecords(context);
      } catch (ValidationException e) {
        abortStateAndRollbackRecordsIfNeeded(context, hasWritesOrDeletesInSnapshot);

        if (e instanceof ValidationConflictException) {
          throw new CommitConflictException(e.getMessage());
        }
        throw new CommitException(e.getMessage());
      }

      if (isCoordinatorStateWriteRequired(hasWritesOrDeletesInSnapshot)) {
        long committedAt;
        try {
          committedAt = commitState(context);
        } catch (CommitConflictException e) {
          if (hasWritesOrDeletesInSnapshot) {
            rollbackRecords(context);
          }
          throw e;
        }
        if (hasWritesOrDeletesInSnapshot) {
          commitRecords(context, committedAt);
        }
      }
    }

    private void abortStateAndRollbackRecordsIfNeeded(
        TransactionContext context, boolean hasWritesOrDeletesInSnapshot)
        throws UnknownTransactionStatusException {
      if (isCoordinatorStateWriteRequired(hasWritesOrDeletesInSnapshot)) {
        abortState(context);
      }
      if (hasWritesOrDeletesInSnapshot) {
        rollbackRecords(context);
      }
    }

    private boolean isCoordinatorStateWriteRequired(boolean hasWritesOrDeletes) {
      return hasWritesOrDeletes || !coordinatorWriteOmissionOnReadOnlyEnabled;
    }

    void prepareRecords(TransactionContext context, long preparedAt) throws PreparationException {
      participantCommitHandler.prepareRecords(context, preparedAt);
    }

    void validateRecords(TransactionContext context) throws ValidationException {
      participantCommitHandler.validateRecords(context);
    }

    void commitRecords(TransactionContext context, long committedAt) {
      participantCommitHandler.commitRecords(context, committedAt);
    }

    void rollbackRecords(TransactionContext context) {
      participantCommitHandler.rollbackRecords(context);
    }

    boolean canOnePhaseCommit(TransactionContext context) {
      return participantCommitHandler.canOnePhaseCommit(context);
    }

    long commitState(TransactionContext context)
        throws CommitConflictException, UnknownTransactionStatusException {
      // PORT: no write set is stored (WriteSetEncoder dropped, replaced part 10).
      return coordinatorCommitHandler.commitState(context.transactionId);
    }

    TransactionState abortState(TransactionContext context)
        throws UnknownTransactionStatusException {
      return coordinatorCommitHandler.abortState(context.transactionId);
    }
  }

  // ===================================================================================
  // The transaction object and its manager
  // ===================================================================================

  // PORT: ScalarDB's ConsensusCommit, renamed: a nested class cannot share the name of the
  // program class bench.scalardb.ConsensusCommit that the interface fixes.
  final class ConsensusCommitTransaction {
    private final TransactionContext context;
    private final CrudHandler crud;
    private final CommitHandler commit;

    ConsensusCommitTransaction(TransactionContext context, CrudHandler crud, CommitHandler commit) {
      this.context = context;
      this.crud = crud;
      this.commit = commit;
    }

    String getId() {
      return context.transactionId;
    }

    Optional<TransactionResult> get(Get get) throws CrudException {
      return crud.get(get, context);
    }

    void upsert(int key, int value) throws CrudException {
      // PORT: the Upsert of the client's Write; createPutForUpsert (l.365-375) enables implicit
      // pre-read and carries the value column.
      Map<String, Object> columns = new LinkedHashMap<>();
      columns.put(VALUE, value);
      Put put = new Put(key, Collections.unmodifiableMap(columns), true, false);
      crud.put(put, context);
    }

    void commit() throws CommitException, UnknownTransactionStatusException {
      try {
        commitInternal();
      } catch (CommitException | UnknownTransactionStatusException e) { // PORT: catch (Exception) narrowed
        releaseResources();
        throw e;
      }
    }

    private void commitInternal() throws CommitException, UnknownTransactionStatusException {
      // PORT: no scanners in the instance (areAllScannersClosed is true).

      // Execute implicit pre-read
      try {
        crud.readIfImplicitPreReadEnabled(context);
      } catch (CrudConflictException e) {
        throw new CommitConflictException(
            "conflict occurred while implicit pre-read: " + e.getMessage());
      } catch (CrudException e) {
        throw new CommitException("executing implicit pre-read failed: " + e.getMessage());
      }

      try {
        crud.waitForRecoveryCompletionIfNecessary(context);
      } catch (CrudConflictException e) {
        throw new CommitConflictException(e.getMessage());
      } catch (CrudException e) {
        throw new CommitException(e.getMessage());
      }

      commit.commit(context);
    }

    void rollback() {
      releaseResources();
    }

    private void releaseResources() {
      // PORT: no group commit slot and no scanners in the instance: nothing to release.
    }
  }

  final class ConsensusCommitManager {
    private final CrudHandler crud;
    private final CommitHandler commit;
    private final ParallelExecutor parallelExecutor;

    ConsensusCommitManager(Io io, int handle) {
      DistributedStorage storage = new DistributedStorage(io);
      CoordinatorStateAccessor coordinator = new CoordinatorStateAccessor(io);
      RecoveryHandler recovery = new RecoveryHandler(storage, coordinator);
      RecoveryExecutor recoveryExecutor =
          new RecoveryExecutor(io, handle, storage, coordinator, recovery);
      this.parallelExecutor = new ParallelExecutor(io);
      this.crud = new CrudHandler(storage, recoveryExecutor);
      ParticipantCommitHandler participant =
          new ParticipantCommitHandler(storage, recoveryExecutor, parallelExecutor);
      this.commit = new CommitHandler(participant, new CoordinatorCommitHandler(coordinator));
    }

    ConsensusCommitTransaction begin(String txId, Isolation isolation, boolean readOnly) {
      // PORT: no group commit (replaced part 10): no slot is reserved.
      Snapshot snapshot = new Snapshot(txId, parallelExecutor);
      TransactionContext context = new TransactionContext(txId, snapshot, isolation, readOnly, false);
      ConsensusCommitTransaction transaction = new ConsensusCommitTransaction(context, crud, commit);
      // PORT: the ReadOnlyDistributedTransaction wrapper only rejects writes; c3 issues none.
      return transaction;
    }
  }

  // ===================================================================================
  // Processes
  // ===================================================================================

  /** hi: the application thread of transaction ti. */
  private void handleMain(int i) {
    Tid self = MpProcess.self();
    Setup setup = Channel.recv(model, Setup.class);
    Io io = new Io(self, setup);
    Tid client = setup.c().get(i - 1);
    String txId = setup.txIds().get(i - 1);
    boolean readOnly = setup.readOnly().get(i - 1);
    ConsensusCommitManager manager = new ConsensusCommitManager(io, i);
    ConsensusCommitTransaction transaction = null;
    while (true) {
      MpMessage m =
          Channel.recv(
              model,
              x -> x instanceof Begin || x instanceof Read || x instanceof Write || x instanceof Commit);
      if (m instanceof Begin) {
        transaction = manager.begin(txId, setup.isolation(), readOnly);
        io.send(client, new Begun());
      } else if (m instanceof Read r) {
        Optional<TransactionResult> result;
        try {
          result = transaction.get(new Get(r.key()));
        } catch (CrudException e) {
          // PORT: CrudConflictException / CrudException at a read -> Aborted (replaced part 9);
          // the application rolls the transaction back.
          transaction.rollback();
          io.send(client, new Aborted());
          continue;
        }
        if (!result.isPresent() || result.get().getValue() == null) {
          throw new AssertionError("read of k" + r.key() + " by " + txId + " found no value");
        }
        io.send(client, new Value(r.key(), result.get().getValue()));
      } else if (m instanceof Write w) {
        try {
          transaction.upsert(w.key(), w.value());
        } catch (CrudException e) {
          throw new AssertionError("an upsert without condition cannot fail", e);
        }
        io.send(client, new Written(w.key()));
      } else {
        try {
          transaction.commit();
          io.send(client, new Committed());
        } catch (CommitException e) {
          // PORT: CommitConflictException / CommitException -> Aborted (replaced part 9).
          io.send(client, new Aborted());
        } catch (UnknownTransactionStatusException e) {
          // PORT: UnknownTransactionStatusException -> Unknown (replaced part 9).
          io.send(client, new Unknown());
        }
      }
    }
  }

  /** xik: RecoveryExecutor's background task for the read of key k by hi. */
  private void recoveryTaskMain(int i, int k) {
    Tid self = MpProcess.self();
    Setup setup = Channel.recv(model, Setup.class);
    Io io = new Io(self, setup);
    DistributedStorage storage = new DistributedStorage(io);
    CoordinatorStateAccessor coordinator = new CoordinatorStateAccessor(io);
    RecoveryHandler recovery = new RecoveryHandler(storage, coordinator);
    RecoveryExecutor executor = new RecoveryExecutor(io, i, storage, coordinator, recovery);
    Tid h = setup.h().get(i - 1);
    while (true) {
      RecoveryJob job = Channel.recv(model, RecoveryJob.class);
      RecoveryOutcome outcome = executor.runSubmitted(job);
      io.send(h, new RecoveryDone(job.jobId(), job.selection().key(), outcome));
    }
  }

  /** sk: the conditional store of key k (replaced part 1). */
  private void storageMain(int k) {
    Setup setup = Channel.recv(model, Setup.class);
    Map<String, Object> init = new LinkedHashMap<>();
    init.put(VALUE, 0);
    init.put(Attribute.ID, setup.initialTxId());
    init.put(Attribute.STATE, TransactionState.COMMITTED.get());
    init.put(Attribute.VERSION, 1);
    init.put(Attribute.PREPARED_AT, 0L);
    init.put(Attribute.COMMITTED_AT, 0L);
    Map<String, Object> record = columnsOf(init);
    while (true) {
      MpMessage m = Channel.recv(model, x -> x instanceof StorageGet || x instanceof StorageMutate);
      if (m instanceof StorageGet g) {
        Channel.send(model, g.from(), new StorageGetReply(g.reqId(), record));
      } else {
        StorageMutate mu = (StorageMutate) m;
        Mutation mutation = mu.mutation();
        boolean applies = conditionHolds(mutation.condition(), record);
        if (applies) {
          if (mutation.delete()) {
            record = null;
          } else {
            Map<String, Object> next = new LinkedHashMap<>();
            if (record != null) {
              next.putAll(record);
            }
            next.putAll(mutation.columns());
            record = columnsOf(next);
          }
        }
        Channel.send(model, mu.from(), new StorageMutateReply(mu.reqId(), applies));
      }
    }
  }

  static boolean conditionHolds(Condition condition, Map<String, Object> record) {
    switch (condition.type()) {
      case NONE:
        return true;
      case PUT_IF_NOT_EXISTS:
        return record == null;
      default:
        if (record == null) {
          return false;
        }
        for (Conjunct c : condition.conjuncts()) {
          Object actual = record.get(c.column());
          if (c.value() == null ? actual != null : !Objects.equals(actual, c.value())) {
            return false;
          }
        }
        return true;
    }
  }

  /** coord: the coordinator table (replaced part 2). */
  private void coordinatorMain() {
    Channel.recv(model, Setup.class);
    Map<String, State> states = new HashMap<>();
    while (true) {
      MpMessage m = Channel.recv(model, x -> x instanceof CoordGetState || x instanceof CoordPutState);
      if (m instanceof CoordGetState g) {
        Channel.send(model, g.from(), new CoordGetStateReply(g.reqId(), states.get(g.id())));
      } else {
        CoordPutState p = (CoordPutState) m;
        boolean applied = !states.containsKey(p.state().getId());
        if (applied) {
          states.put(p.state().getId(), p.state());
        }
        Channel.send(model, p.from(), new CoordPutStateReply(p.reqId(), applied));
      }
    }
  }

  /** ci: the client loop (INTERFACE, verbatim in meaning). */
  private void client(int id, int keys, int[] writes, Tid h) {
    Channel.send(model, h, new Begin());
    if (!(Channel.recv(model) instanceof Begun)) {
      throw new AssertionError("Begun expected");
    }
    for (int k = 1; k <= keys; k++) {
      Channel.send(model, h, new Read(k));
      MpMessage rep = Channel.recv(model);
      if (rep instanceof Value) {
        continue;
      } else if (rep instanceof Aborted) {
        return;
      } else {
        throw new AssertionError("Value or Aborted expected");
      }
    }
    for (int k : writes) {
      Channel.send(model, h, new Write(k, id));
      if (!(Channel.recv(model) instanceof Written)) {
        throw new AssertionError("Written expected");
      }
    }
    Channel.send(model, h, new Commit());
    MpMessage rep = Channel.recv(model);
    if (!(rep instanceof Committed || rep instanceof Aborted || rep instanceof Unknown)) {
      throw new AssertionError("Committed, Aborted or Unknown expected");
    }
  }

  @Override
  public void run() {
    // Start order as the interface's: s1..sk, coord, x11..xnk, h1..hn, c1..cn, init.
    List<Tid> s = new ArrayList<>();
    for (int k = 1; k <= keys; k++) {
      final int kk = k;
      s.add(MpProcess.spawnDaemon(() -> storageMain(kk)).tid());
    }
    Tid coord = MpProcess.spawnDaemon(this::coordinatorMain).tid();
    List<List<Tid>> x = new ArrayList<>();
    for (int i = 1; i <= clients; i++) {
      List<Tid> xi = new ArrayList<>();
      for (int k = 1; k <= keys; k++) {
        final int ii = i;
        final int kk = k;
        xi.add(MpProcess.spawnDaemon(() -> recoveryTaskMain(ii, kk)).tid());
      }
      x.add(List.copyOf(xi));
    }
    List<Tid> h = new ArrayList<>();
    for (int i = 1; i <= clients; i++) {
      final int ii = i;
      h.add(MpProcess.spawnDaemon(() -> handleMain(ii)).tid());
    }
    List<Tid> c = new ArrayList<>();
    List<MpProcess.Handle> clientHandles = new ArrayList<>();
    for (int i = 1; i <= clients; i++) {
      final int ii = i;
      final Tid hi = h.get(i - 1);
      final int[] writes = writesOf(i);
      MpProcess.Handle handle = MpProcess.spawn("c" + i, () -> client(ii, keys, writes, hi));
      clientHandles.add(handle);
      c.add(handle.tid());
    }
    List<String> txIds = new ArrayList<>();
    List<Boolean> readOnly = new ArrayList<>();
    for (int i = 1; i <= clients; i++) {
      txIds.add("t" + i);
      readOnly.add(i == 3); // c3 (mixedro) is read-only
    }
    Setup setup =
        new Setup(List.copyOf(s), coord, List.copyOf(x), List.copyOf(h), List.copyOf(c),
            List.copyOf(txIds), "t0", isolation, List.copyOf(readOnly));
    MpProcess.spawnDaemon(
        () -> {
          // init: one setup message to every invisible process.
          for (Tid t : s) {
            Channel.send(model, t, setup);
          }
          Channel.send(model, coord, setup);
          for (List<Tid> xi : x) {
            for (Tid t : xi) {
              Channel.send(model, t, setup);
            }
          }
          for (Tid t : h) {
            Channel.send(model, t, setup);
          }
        });
    for (MpProcess.Handle handle : clientHandles) {
      handle.await();
    }
  }
}
