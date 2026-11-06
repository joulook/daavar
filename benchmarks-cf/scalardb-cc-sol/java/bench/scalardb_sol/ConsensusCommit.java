package bench.scalardb_sol;

import java.util.*;
import org.mpi_sws.jmc.api.mp.*;
import org.mpi_sws.jmc.mp.events.*;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** ScalarDB 416cdc2, the partition-key/upsert cut described in NOTES.md. */
public final class ConsensusCommit implements MpProgram {
  @Tag("Begin") public record Begin() implements MpMessage {}
  @Tag("Read") public record Read(int key) implements MpMessage {}
  @Tag("Write") public record Write(int key, int value) implements MpMessage {}
  @Tag("Commit") public record Commit() implements MpMessage {}
  @Tag("Begun") public record Begun() implements MpMessage {}
  @Tag("Value") public record Value(int key, int value) implements MpMessage {}
  @Tag("Written") public record Written(int key) implements MpMessage {}
  @Tag("Committed") public record Committed() implements MpMessage {}
  @Tag("Aborted") public record Aborted() implements MpMessage {}
  @Tag("Unknown") public record Unknown() implements MpMessage {}

  enum State { PREPARED, DELETED, COMMITTED, ABORTED }
  // The two six-column images are immutable; null means SQL NULL, not an omitted column.
  record Image(Integer value, String id, State state, Integer version,
               Long preparedAt, Long committedAt) {}
  record Row(Image after, Image before) {}
  enum Col { VALUE, ID, STATE, VERSION, PREPARED_AT, COMMITTED_AT,
             BEFORE_VALUE, BEFORE_ID, BEFORE_STATE, BEFORE_VERSION,
             BEFORE_PREPARED_AT, BEFORE_COMMITTED_AT }
  record Field(Col column, Object value) {}
  enum Condition { ALWAYS, ABSENT, MATCH }
  record Mutation(boolean delete, Condition condition, List<Field> tests, List<Field> sets) {}
  record Address(String role, Tid tid) {}
  record Setup(List<Address> addresses, List<String> ids, String isolation,
               List<Boolean> readOnly) implements MpMessage {}
  record Get(int request, Tid replyTo, int key) implements MpMessage {}
  record Mutate(int request, Tid replyTo, int key, Mutation mutation) implements MpMessage {}
  record GetState(int request, Tid replyTo, String id) implements MpMessage {}
  record PutState(int request, Tid replyTo, String id, State state, long createdAt) implements MpMessage {}
  record Reply(int request, int key, Row row, State state, boolean ok) implements MpMessage {}
  enum Outcome { DONE, CRUD_CONFLICT, CRUD_ERROR }
  record RecoveryJob(int key, Tid handle, Row row, State state) implements MpMessage {}
  record RecoveryDone(int key, Outcome outcome) implements MpMessage {}
  static final class CrudConflict extends Exception {}

  private final String pattern, isolation, delivery;
  private final int keys, clients;
  private final CommunicationModel communication;
  public ConsensusCommit(String... args) {
    if (args.length != 5 || !args[0].equals("impl")) throw new IllegalArgumentException("impl,pattern,k,iso,model");
    pattern = args[1]; keys = Integer.parseInt(args[2]); isolation = args[3]; delivery = args[4];
    if (!Set.of("lost", "skew", "both", "mixed", "mixedro").contains(pattern)
        || keys < 1 || keys > 2 || (keys == 1 && !pattern.equals("lost"))
        || !Set.of("snapshot", "serializable").contains(isolation)
        || !Set.of("fifo", "bag").contains(delivery)) throw new IllegalArgumentException("invalid instance");
    clients = pattern.equals("mixedro") ? 3 : 2;
    communication = delivery.equals("fifo") ? CommunicationModel.P2P : CommunicationModel.ASYN;
  }
  public String programName() { return "scalardb_cc"; }
  public String size() { return pattern + "," + keys + "," + isolation; }
  public CommunicationModel model() { return communication; }
  public List<String> visibleRoles() {
    return clients == 3 ? List.of("c1", "c2", "c3") : List.of("c1", "c2");
  }
  public List<String> rolesInSpawnOrder() {
    List<String> roles = new ArrayList<>();
    for (int k = 1; k <= keys; k++) roles.add("s" + k);
    roles.add("coord");
    for (int i = 1; i <= clients; i++) for (int k = 1; k <= keys; k++) roles.add("x" + i + k);
    for (int i = 1; i <= clients; i++) roles.add("h" + i);
    roles.addAll(visibleRoles()); roles.add("init"); return List.copyOf(roles);
  }
  void send(Tid to, MpMessage msg) { Channel.send(communication, to, msg); }
  MpMessage receive() { return Channel.recv(communication); }
  public void run() {
    List<MpProcess.Handle> processes = new ArrayList<>();
    List<Address> addresses = new ArrayList<>();
    for (int k = 1; k <= keys; k++) {
      final int key = k;
      spawn(processes, addresses, "s" + k, () -> storage(key));
    }
    spawn(processes, addresses, "coord", this::coordinator);
    for (int i = 1; i <= clients; i++) for (int k = 1; k <= keys; k++)
      spawn(processes, addresses, "x" + i + k, () -> new Recovery().run());
    for (int i = 1; i <= clients; i++) {
      final int client = i;
      spawn(processes, addresses, "h" + i, () -> new Handle(client).run());
    }
    for (int i = 1; i <= clients; i++) {
      final int client = i; final Tid handle = lookup(addresses, "h" + i);
      spawn(processes, addresses, "c" + i, () -> client(client, handle));
    }
    List<Address> all = List.copyOf(addresses);
    // init's own address is also part of setup; obtain it locally in its body.
    spawn(processes, addresses, "init", () -> {
      List<Address> withInit = new ArrayList<>(all);
      withInit.add(new Address("init", MpProcess.self()));
      Setup setup = new Setup(List.copyOf(withInit), List.of("t0", "t1", "t2", "t3"),
          isolation, List.of(false, false, false, true));
      for (Address a : all) if (!a.role().startsWith("c") || a.role().equals("coord")) send(a.tid(), setup);
    });
    for (MpProcess.Handle p : processes) p.await();
  }
  void spawn(List<MpProcess.Handle> ps, List<Address> as, String role, Runnable body) {
    MpProcess.Handle h = MpProcess.spawn(role, body); ps.add(h); as.add(new Address(role, h.tid()));
  }
  static Tid lookup(List<Address> addresses, String role) {
    for (Address a : addresses) if (a.role().equals(role)) return a.tid();
    throw new IllegalArgumentException(role);
  }
  int[] writes(int i) {
    if (i == 3) return new int[0];
    return switch (pattern) {
      case "lost" -> new int[]{1};
      case "skew" -> new int[]{i};
      case "both" -> new int[]{1, 2};
      default -> i == 1 ? new int[]{1, 2} : new int[]{2};
    };
  }
  void expect(boolean condition) { if (!condition) throw new AssertionError("unexpected protocol reply"); }
  void client(int i, Tid h) {
    send(h, new Begin()); expect(receive() instanceof Begun);
    for (int k = 1; k <= keys; k++) {
      send(h, new Read(k)); MpMessage r = receive();
      if (r instanceof Aborted) return;
      expect(r instanceof Value);
    }
    for (int k : writes(i)) { send(h, new Write(k, i)); expect(receive() instanceof Written); }
    send(h, new Commit()); MpMessage r = receive();
    expect(r instanceof Committed || r instanceof Aborted || r instanceof Unknown);
  }

  static Object[] columns(Row row) {
    Object[] a = new Object[12];
    if (row != null) { copyImage(a, 0, row.after()); copyImage(a, 6, row.before()); }
    return a;
  }
  static void copyImage(Object[] a, int n, Image i) {
    if (i == null) return;
    a[n] = i.value(); a[n+1] = i.id(); a[n+2] = i.state(); a[n+3] = i.version();
    a[n+4] = i.preparedAt(); a[n+5] = i.committedAt();
  }
  static Image image(Object[] a, int n) {
    if (n == 6 && Arrays.stream(a, 6, 12).allMatch(Objects::isNull)) return null;
    return new Image((Integer)a[n], (String)a[n+1], (State)a[n+2], (Integer)a[n+3],
        (Long)a[n+4], (Long)a[n+5]);
  }
  static Row row(Object[] a) { return new Row(image(a, 0), image(a, 6)); }
  void storage(int key) {
    Setup setup = Channel.recv(communication, Setup.class);
    Row record = new Row(new Image(0, setup.ids().get(0), State.COMMITTED, 1, 0L, 0L), null);
    while (true) {
      MpMessage m = receive();
      if (m instanceof Get g) {
        expect(g.key() == key); send(g.replyTo(), new Reply(g.request(), key, record, null, true));
      } else if (m instanceof Mutate q) {
        expect(q.key() == key); Mutation mu = q.mutation(); Object[] a = columns(record);
        boolean ok = switch (mu.condition()) {
          case ALWAYS -> true;
          case ABSENT -> record == null;
          case MATCH -> record != null && mu.tests().stream().allMatch(f -> Objects.equals(a[f.column().ordinal()], f.value()));
        };
        if (ok) {
          if (mu.delete()) record = null;
          else { for (Field f : mu.sets()) a[f.column().ordinal()] = f.value(); record = row(a); }
        }
        send(q.replyTo(), new Reply(q.request(), key, null, null, ok));
      } else throw new AssertionError("storage message");
    }
  }
  void coordinator() {
    Channel.recv(communication, Setup.class);
    Map<String, State> states = new HashMap<>();
    Map<String, Long> createdAt = new HashMap<>();
    while (true) {
      MpMessage m = receive();
      if (m instanceof GetState g) send(g.replyTo(), new Reply(g.request(), 0, null, states.get(g.id()), true));
      else if (m instanceof PutState p) {
        boolean ok = !states.containsKey(p.id());
        if (ok) { states.put(p.id(), p.state()); createdAt.put(p.id(), p.createdAt()); }
        send(p.replyTo(), new Reply(p.request(), 0, null, null, ok));
      } else throw new AssertionError("coordinator message");
    }
  }
  static Field f(Col c, Object v) { return new Field(c, v); }
  static List<Field> beforeFields(Image image) {
    Object[] a = new Object[12]; copyImage(a, 6, image);
    List<Field> fields = new ArrayList<>();
    for (int n = 6; n < 12; n++) fields.add(f(Col.values()[n], a[n]));
    return fields;
  }
  static Mutation prepare(String id, int value, Row original) {
    List<Field> sets = new ArrayList<>(List.of(f(Col.VALUE, value), f(Col.ID, id),
        f(Col.STATE, State.PREPARED), f(Col.VERSION, original == null ? 1 : original.after().version() + 1),
        f(Col.PREPARED_AT, 0L)));
    if (original != null) sets.addAll(beforeFields(original.after()));
    return new Mutation(false, original == null ? Condition.ABSENT : Condition.MATCH,
        original == null ? List.of() : List.of(f(Col.ID, original.after().id())), List.copyOf(sets));
  }
  static Mutation rollforward(String id, State state) {
    List<Field> sets = new ArrayList<>(List.of(f(Col.STATE, State.COMMITTED), f(Col.COMMITTED_AT, 0L)));
    sets.addAll(beforeFields(null));
    return new Mutation(state == State.DELETED, Condition.MATCH,
        List.of(f(Col.ID, id), f(Col.STATE, state)), List.copyOf(sets));
  }
  static Mutation rollback(Row original) {
    List<Field> sets = new ArrayList<>();
    if (original.before() != null) {
      Object[] a = new Object[12]; copyImage(a, 0, original.before());
      for (int n = 0; n < 6; n++) sets.add(f(Col.values()[n], a[n]));
      sets.addAll(beforeFields(null));
    }
    return new Mutation(original.before() == null, Condition.MATCH,
        List.of(f(Col.ID, original.after().id()), f(Col.STATE, original.after().state())), List.copyOf(sets));
  }

  abstract class Actor {
    Setup setup;
    int nextRequest;
    final Map<Integer, Outcome> completed = new HashMap<>();
    void setup() { setup = Channel.recv(communication, Setup.class); }
    Tid address(String name) { return lookup(setup.addresses(), name); }
    int requestGet(int key) { int id = ++nextRequest; send(address("s" + key), new Get(id, MpProcess.self(), key)); return id; }
    int mutate(int key, Mutation m) { int id = ++nextRequest; send(address("s" + key), new Mutate(id, MpProcess.self(), key, m)); return id; }
    int state(String tx) { int id = ++nextRequest; send(address("coord"), new GetState(id, MpProcess.self(), tx)); return id; }
    int putState(String tx, State s) { int id = ++nextRequest; send(address("coord"), new PutState(id, MpProcess.self(), tx, s, 0)); return id; }
    Reply await(Set<Integer> pending) {
      while (true) {
        MpMessage m = receive();
        if (m instanceof RecoveryDone d) completed.put(d.key(), d.outcome());
        else if (m instanceof Reply r) { if (pending.contains(r.request())) return r; /* stale phase reply */ }
        else throw new AssertionError("unexpected RPC message");
      }
    }
    Reply await(int id) { return await(Set.of(id)); }
    Row readStorage(int key) { return await(requestGet(key)).row(); }
    State getState(String tx) { return await(state(tx)).state(); }
    boolean writeState(String tx, State s) { return await(putState(tx, s)).ok(); }
  }

  final class Recovery extends Actor {
    void run() {
      setup();
      while (true) {
        RecoveryJob job = Channel.recv(communication, RecoveryJob.class);
        Row r = job.row();
        if (job.state() == null) {
          // RecoveryHandler.tryRecover -> abortIfExpired(false). This branch is retained
          // for completeness; RETURN_LATEST always submits an already-resolved state.
          if (Nondet.chooseBoolean()) {
            Row latest = readStorage(job.key());
            if (latest != null && latest.after().state() != State.COMMITTED
                && Objects.equals(latest.after().id(), r.after().id())
                && writeState(r.after().id(), State.ABORTED)) await(mutate(job.key(), rollback(r)));
          }
        } else await(mutate(job.key(), job.state() == State.COMMITTED
            ? rollforward(r.after().id(), r.after().state()) : rollback(r)));
        // A conditional NoMutation is absorbed by both recovery composers.
        send(job.handle(), new RecoveryDone(job.key(), Outcome.DONE));
      }
    }
  }

  final class Handle extends Actor {
    final int number;
    String tx;
    Tid client;
    boolean serializable, readOnly, begun, ended;
    Row[] readSet = new Row[keys + 1];
    Row[] getSet = new Row[keys + 1];
    boolean[] read = new boolean[keys + 1];
    final Map<Integer, Integer> writeSet = new LinkedHashMap<>();
    final Set<Integer> recovery = new LinkedHashSet<>();
    Handle(int number) { this.number = number; }
    void run() {
      setup(); tx = setup.ids().get(number); client = address("c" + number);
      serializable = setup.isolation().equals("serializable"); readOnly = setup.readOnly().get(number);
      while (true) {
        MpMessage m = receive();
        if (m instanceof Reply) continue; // validation's late replies
        if (m instanceof RecoveryDone d) { completed.put(d.key(), d.outcome()); continue; }
        if (m instanceof Begin) {
          expect(!begun); begun = true; send(client, new Begun());
        } else if (m instanceof Read r) {
          expect(begun && !ended);
          try { send(client, new Value(r.key(), get(r.key()))); }
          catch (CrudConflict conflict) { ended = true; send(client, new Aborted()); }
        } else if (m instanceof Write w) {
          expect(begun && !ended && !readOnly); writeSet.put(w.key(), w.value()); send(client, new Written(w.key()));
        } else if (m instanceof Commit) {
          expect(begun && !ended); ended = true;
          send(client, commit());
        } else throw new AssertionError("handle message");
      }
    }
    int get(int key) throws CrudConflict {
      if (!read[key]) {
        Row result = readStorage(key);
        if (result != null && result.after().state() != State.COMMITTED) result = resolve(key, result);
        if (!readOnly) readSet[key] = result;
        getSet[key] = result; read[key] = true;
      }
      // Snapshot.mergeResult: the sole user column is overwritten by the local Put.
      if (writeSet.containsKey(key)) return writeSet.get(key);
      expect(getSet[key] != null); // initial rows exist; all writes are updates
      return getSet[key].after().value();
    }
    Row submitRecovery(int key, Row current, State state) {
      recovery.add(key);
      send(address("x" + number + key), new RecoveryJob(key, MpProcess.self(), current, state));
      if (state == State.ABORTED) return current.before() == null ? null : new Row(current.before(), null);
      Image a = current.after();
      return a.state() == State.DELETED ? null
          : new Row(new Image(a.value(), a.id(), State.COMMITTED, a.version(), a.preparedAt(), 0L), null);
    }
    Row resolve(int key, Row initial) throws CrudConflict {
      Row current = initial;
      for (int attempt = 0; attempt < 5; attempt++) {
        String writer = current.after().id();
        State state = getState(writer);
        if (state != null) return submitRecovery(key, current, state);
        if (!Nondet.chooseBoolean()) throw new CrudConflict();
        Row fresh = readStorage(key);
        if (fresh == null || fresh.after().state() == State.COMMITTED) return fresh;
        if (!writer.equals(fresh.after().id())) { current = fresh; continue; }
        if (writeState(writer, State.ABORTED)) {
          Row afterAbort = readStorage(key);
          if (afterAbort == null || afterAbort.after().state() == State.COMMITTED) return afterAbort;
          if (!writer.equals(afterAbort.after().id())) { current = afterAbort; continue; }
          return submitRecovery(key, current, State.ABORTED);
        }
      }
      throw new CrudConflict();
    }
    boolean waitForRecoveryCompletion() {
      for (int key : recovery) if (writeSet.containsKey(key) || serializable) {
        while (!completed.containsKey(key)) {
          MpMessage m = receive();
          if (m instanceof RecoveryDone d) completed.put(d.key(), d.outcome());
          else if (!(m instanceof Reply)) throw new AssertionError("future message");
        }
        if (completed.get(key) != Outcome.DONE) return false;
      }
      return true;
    }
    Map<Integer, Integer> getPhase(Collection<Integer> ks) {
      Map<Integer, Integer> pending = new LinkedHashMap<>();
      for (int key : ks) pending.put(requestGet(key), key);
      return pending;
    }
    boolean collectMutations(Map<Integer, Integer> pending) {
      boolean ok = true;
      while (!pending.isEmpty()) { Reply r = await(pending.keySet()); pending.remove(r.request()); ok &= r.ok(); }
      return ok;
    }
    boolean prepareRecords() {
      Map<Integer, Integer> pending = new LinkedHashMap<>();
      for (var w : writeSet.entrySet()) pending.put(mutate(w.getKey(), prepare(tx, w.getValue(), readSet[w.getKey()])), w.getKey());
      return collectMutations(pending); // preparation always waits for every reply
    }
    boolean validateRecords() {
      if (!serializable) return true;
      List<Integer> ks = new ArrayList<>();
      for (int key = 1; key <= keys; key++) if (read[key] && !writeSet.containsKey(key)) ks.add(key);
      Map<Integer, Integer> pending = getPhase(ks);
      while (!pending.isEmpty()) {
        Reply r = await(pending.keySet()); int key = pending.remove(r.request());
        Row original = getSet[key], latest = r.row();
        if ((original == null) != (latest == null)
            || (original != null && !Objects.equals(original.after().id(), latest.after().id()))) return false;
      }
      return true;
    }
    void rollbackRecords() {
      Map<Integer, Integer> pending = getPhase(writeSet.keySet());
      List<Reply> records = new ArrayList<>();
      while (!pending.isEmpty()) { Reply r = await(pending.keySet()); pending.remove(r.request()); records.add(r); }
      for (Reply r : records) if (r.row() != null && tx.equals(r.row().after().id()))
        pending.put(mutate(r.key(), rollback(r.row())), r.key());
      collectMutations(pending);
      // getKeysOfWritesNeverRead is empty: these clients read every key first.
    }
    void abortState() { if (!writeState(tx, State.ABORTED)) getState(tx); }
    MpMessage abort() {
      if (!writeSet.isEmpty()) { abortState(); rollbackRecords(); }
      return new Aborted();
    }
    MpMessage commit() {
      if (!waitForRecoveryCompletion()) return new Aborted();
      if (!prepareRecords()) return abort();
      if (!validateRecords()) return abort();
      if (writeSet.isEmpty()) return new Committed();
      if (!writeState(tx, State.COMMITTED) && getState(tx) != State.COMMITTED) {
        rollbackRecords(); return new Aborted();
      }
      Map<Integer, Integer> pending = new LinkedHashMap<>();
      for (int key : writeSet.keySet()) pending.put(mutate(key, rollforward(tx, State.PREPARED)), key);
      collectMutations(pending); // NoMutation is intentionally ignored after the decision
      return new Committed();
    }
  }
  public String toString() { return "ConsensusCommit(" + size() + "," + delivery + ")"; }
}
