package bench.cockroach_sol;

import java.util.ArrayList;
import java.util.List;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** CockroachDB d30c905, restricted transaction-coordinator and recovery cut. */
public final class ParallelCommits implements MpProgram {
    @Tag("Put") public record Put(int key) implements MpMessage {}
    @Tag("Commit") public record Commit(List<Integer> keys) implements MpMessage {
        public Commit { keys = List.copyOf(keys); }
    }
    @Tag("Recover") public record Recover() implements MpMessage {}
    @Tag("Written") public record Written(int key) implements MpMessage {}
    @Tag("Committed") public record Committed() implements MpMessage, Fate {}
    @Tag("Retry") public record Retry() implements MpMessage {}
    @Tag("Aborted") public record Aborted() implements MpMessage, Fate {}
    sealed interface Fate permits Committed, Aborted {}   // written as the Rust unit variants Fate::Committed / Fate::Aborted (2026-11-08)
    @Tag("Recovered") public record Recovered(Fate fate) implements MpMessage {}

    enum Status { PENDING, STAGING, COMMITTED, ABORTED }
    enum Error { NONE, INTENT_MISSING, RETRY_SERIALIZABLE, RETRY_ASYNC_WRITE_FAILURE,
                 TRANSACTION_ABORTED, ALREADY_COMMITTED, INDETERMINATE_COMMIT }
    enum Kind { PIPE, FINAL, PRECOMMIT, QUERY, PROBE, PUSH, RECOVER, EXPLICIT, RESOLVE }
    @Tag("Meta") record Meta(String id, int anchor, int epoch, int write, int read, int min) {
        Meta at(int ts) { return new Meta(id, anchor, epoch, ts, read, min); }
        Meta refresh(int ts) { return new Meta(id, anchor, epoch, ts, ts, min); }
        Meta restart() { return new Meta(id, anchor, epoch + 1, write, write, min); }
    }
    @Tag("Write") record Write(int key, int sequence) {}
    @Tag("Txn") record Txn(Meta meta, Status status, List<Write> flight, List<Integer> locks) {
        Txn { flight = List.copyOf(flight); locks = List.copyOf(locks); }
        boolean finalized() { return status == Status.COMMITTED || status == Status.ABORTED; }
    }
    @Tag("Setup") record Setup(List<Tid> ranges, Tid tc, Tid tcx, List<Tid> handles,
                               Tid client, List<Tid> preventers, Tid initializer, Meta meta) implements MpMessage {
        Setup { ranges = List.copyOf(ranges); handles = List.copyOf(handles); preventers = List.copyOf(preventers); }
    }
    @Tag("Batch") record Batch(Tid reply, int token, Kind kind, Meta meta, List<Write> puts,
                                List<Write> queries, List<Write> flight, List<Integer> locks,
                                boolean implicit, Status fate) implements MpMessage {
        Batch { puts = List.copyOf(puts); queries = List.copyOf(queries);
                flight = List.copyOf(flight); locks = List.copyOf(locks); }
    }
    @Tag("Answer") record Answer(int token, Kind kind, int range, int timestamp, int staging,
                                  Error error, Txn txn, boolean exists, boolean found,
                                  boolean unpushed) implements MpMessage {}
    @Tag("Explicit") record Explicit(Meta meta, List<Integer> locks) implements MpMessage {
        Explicit { locks = List.copyOf(locks); }
    }

    final int n, p, m, a;
    final CommunicationModel delivery;
    public ParallelCommits(String... args) {
        if (args.length != 6 || !args[0].equals("impl")) throw new IllegalArgumentException("impl,n,p,m,a,fifo|bag");
        n = Integer.parseInt(args[1]); p = Integer.parseInt(args[2]);
        m = Integer.parseInt(args[3]); a = Integer.parseInt(args[4]);
        if (n < 1 || n > 2 || p < 0 || p > n || (n == 1 && p == 0) || m < 1 || m > 2 || a < 1)
            throw new IllegalArgumentException("invalid instance");
        delivery = switch (args[5]) { case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN; default -> throw new IllegalArgumentException("delivery"); };
    }
    public String programName() { return "crdb_pc"; }
    public String size() { return n + "," + p + "," + m + "," + a; }
    public CommunicationModel model() { return delivery; }
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        for (int k = 1; k <= n; k++) r.add("r" + k);
        r.add("tcx"); r.add("tc");
        for (int i = 1; i <= m; i++) r.add("h" + i);
        r.add("c"); for (int i = 1; i <= m; i++) r.add("p" + i);
        r.add("init"); return List.copyOf(r);
    }
    public List<String> visibleRoles() {
        List<String> r = new ArrayList<>(); r.add("c");
        for (int i = 1; i <= m; i++) r.add("p" + i); return List.copyOf(r);
    }
    void send(Tid to, MpMessage value) { Channel.send(delivery, to, value); }
    <V extends MpMessage> V recv(Class<V> type) { return Channel.recv(delivery, type); }
    List<Integer> keys() { List<Integer> r = new ArrayList<>(); for (int k = 1; k <= n; k++) r.add(k); return List.copyOf(r); }
    Write write(int k) { return new Write(k, k); }
    Batch batch(int token, Kind kind, Meta meta, List<Write> puts, List<Write> queries,
                List<Write> flight, List<Integer> locks, boolean implicit, Status fate) {
        return new Batch(MpProcess.self(), token, kind, meta, puts, queries, flight, locks, implicit, fate);
    }
    Batch simple(int token, Kind kind, Meta meta) {
        return batch(token, kind, meta, List.of(), List.of(), List.of(), List.of(), false, Status.PENDING);
    }
    Answer answer(int token) { return Channel.recv(delivery, Answer.class, x -> x.token() == token); }
    public void run() {
        List<MpProcess.Handle> all = new ArrayList<>(); List<Tid> ranges = new ArrayList<>();
        for (int k = 1; k <= n; k++) { final int key = k;
            MpProcess.Handle h = MpProcess.spawn("r" + k, () -> range(key)); all.add(h); ranges.add(h.tid()); }
        MpProcess.Handle tcx = MpProcess.spawn("tcx", this::explicitTask); all.add(tcx);
        MpProcess.Handle tc = MpProcess.spawn("tc", this::coordinator); all.add(tc);
        List<Tid> handles = new ArrayList<>();
        for (int i = 1; i <= m; i++) { final int index = i - 1;
            MpProcess.Handle h = MpProcess.spawn("h" + i, () -> handle(index)); all.add(h); handles.add(h.tid()); }
        MpProcess.Handle c = MpProcess.spawn("c", () -> client(tc.tid())); all.add(c);
        List<Tid> preventers = new ArrayList<>();
        for (int i = 1; i <= m; i++) { final Tid h = handles.get(i - 1);
            MpProcess.Handle pi = MpProcess.spawn("p" + i, () -> preventer(h)); all.add(pi); preventers.add(pi.tid()); }
        List<Tid> rangeAddresses = List.copyOf(ranges);
        List<Tid> handleAddresses = List.copyOf(handles);
        List<Tid> preventerAddresses = List.copyOf(preventers);
        all.add(MpProcess.spawn("init", () -> {
            Setup setup = new Setup(rangeAddresses, tc.tid(), tcx.tid(), handleAddresses,
                c.tid(), preventerAddresses, MpProcess.self(), new Meta("T", 1, 0, 1, 1, 1));
            for (Tid r : setup.ranges()) send(r, setup);
            send(setup.tcx(), setup); send(setup.tc(), setup);
            for (Tid h : setup.handles()) send(h, setup);
        }));
        for (MpProcess.Handle h : all) h.await();
    }
    void client(Tid tc) {
        for (int epoch = 0; epoch < a; epoch++) {
            for (int k = 1; k <= p; k++) { send(tc, new Put(k));
                MpMessage rep = Channel.recv(delivery); if (!(rep instanceof Written)) throw new AssertionError("Written expected"); }
            List<Integer> finalKeys = new ArrayList<>(); for (int k = p + 1; k <= n; k++) finalKeys.add(k);
            send(tc, new Commit(finalKeys)); MpMessage rep = Channel.recv(delivery);
            if (rep instanceof Committed || rep instanceof Aborted) return;
            if (!(rep instanceof Retry)) throw new AssertionError("outcome expected");
        }
    }
    void preventer(Tid handle) {
        send(handle, new Recover()); if (!(Channel.recv(delivery) instanceof Recovered)) throw new AssertionError("Recovered expected");
    }

    // Client interceptors and DistSender. Only this process owns the pipeliner state.
    void coordinator() {
        Setup s = recv(Setup.class); Meta meta = s.meta(); int token = 0;
        List<Write> flight = new ArrayList<>(); List<Integer> locks = new ArrayList<>();
        while (true) {
            MpMessage request = Channel.recv(delivery, x -> x instanceof Put || x instanceof Commit);
            if (request instanceof Put put) {
                int id = ++token;
                send(s.ranges().get(put.key() - 1), batch(id, Kind.PIPE, meta, List.of(write(put.key())),
                    List.of(), List.of(), List.of(), false, Status.PENDING));
                Answer r = answer(id); meta = meta.at(Math.max(meta.write(), r.timestamp()));
                flight.add(write(put.key())); send(s.client(), new Written(put.key())); continue;
            }
            Commit commit = (Commit) request;
            List<Write> attached = new ArrayList<>(flight);
            for (int k : commit.keys()) attached.add(write(k));
            int id = ++token, count = 0;
            // Split-off pre-commit queries are sent concurrently with the EndTxn half.
            for (Write w : flight) {
                send(s.ranges().get(w.key() - 1), batch(id, Kind.PRECOMMIT, meta, List.of(),
                    List.of(w), List.of(), List.of(), false, Status.PENDING)); count++;
            }
            for (int k = 1; k <= n; k++) {
                List<Write> puts = commit.keys().contains(k) ? List.of(write(k)) : List.of();
                if (k != 1 && puts.isEmpty()) continue;
                send(s.ranges().get(k - 1), batch(id, Kind.FINAL, meta, puts, List.of(),
                    k == 1 ? attached : List.of(), locks, false, Status.PENDING)); count++;
            }
            Answer end = null; boolean missing = false; int maxTS = meta.write();
            for (int i = 0; i < count; i++) {
                Answer r = answer(id); maxTS = Math.max(maxTS, r.timestamp());
                if (r.kind() == Kind.FINAL && r.range() == 1) end = r;
                if (r.error() == Error.INTENT_MISSING) missing = true;
            }
            if (end == null) throw new AssertionError("EndTxn response");
            Error error = end.error();
            if (error == Error.NONE && missing) {
                int qid = ++token; send(s.ranges().get(0), simple(qid, Kind.QUERY, meta.at(maxTS)));
                Answer q = answer(qid);
                if (q.txn().status() == Status.COMMITTED) missing = false;
                else if (q.txn().status() == Status.ABORTED) error = Error.TRANSACTION_ABORTED;
                else error = Error.RETRY_ASYNC_WRITE_FAILURE;
            }
            meta = meta.at(maxTS);
            // Retry only EndTxn, folding every proven write into its lock footprint.
            if (error == Error.RETRY_SERIALIZABLE ||
                (error == Error.NONE && end.txn().status() == Status.STAGING && maxTS > end.staging())) {
                locks = new ArrayList<>(keys()); flight.clear();
                int attempts = 0;
                do {
                    meta = meta.refresh(meta.write()); // no reads: refresh always succeeds
                    int eid = ++token;
                    send(s.ranges().get(0), batch(eid, Kind.EXPLICIT, meta, List.of(), List.of(),
                        List.of(), locks, false, Status.PENDING));
                    end = answer(eid); meta = meta.at(Math.max(meta.write(), end.timestamp())); error = end.error();
                } while (error == Error.RETRY_SERIALIZABLE && ++attempts < 5);
            }
            if (error == Error.NONE) {
                flight.clear(); locks = new ArrayList<>(keys());
                send(s.client(), new Committed());
                if (end.txn().status() == Status.STAGING)
                    send(s.tcx(), new Explicit(meta, keys()));
            } else if (error == Error.TRANSACTION_ABORTED) {
                locks = new ArrayList<>(keys()); send(s.client(), new Aborted());
            } else if (error == Error.RETRY_ASYNC_WRITE_FAILURE || error == Error.RETRY_SERIALIZABLE) {
                send(s.client(), new Retry()); meta = meta.restart(); flight.clear(); locks = new ArrayList<>(keys());
            } else throw new AssertionError("unexpected coordinator error " + error);
        }
    }
    void explicitTask() {
        Setup s = recv(Setup.class); int token = 0;
        while (true) {
            Explicit e = recv(Explicit.class); int id = ++token;
            send(s.ranges().get(0), batch(id, Kind.EXPLICIT, e.meta(), List.of(), List.of(),
                List.of(), e.locks(), false, Status.PENDING));
            Error error = answer(id).error();
            if (error != Error.NONE && error != Error.ALREADY_COMMITTED)
                throw new AssertionError("implicit commit lost: " + error);
        }
    }

    // Push -> probe (one atomic sub-batch per range) -> recover -> retry original push.
    void handle(int index) {
        Setup s = recv(Setup.class); int token = 0;
        while (true) {
            recv(Recover.class); Txn pushed;
            while (true) {
                int id = ++token; send(s.ranges().get(0), simple(id, Kind.PUSH, s.meta()));
                Answer push = answer(id); pushed = push.txn();
                if (push.error() != Error.INDETERMINATE_COMMIT) break;
                Txn staging = pushed; int probeId = ++token, count = 0;
                for (int k = 1; k <= n; k++) {
                    List<Write> qs = new ArrayList<>();
                    for (Write w : staging.flight()) if (w.key() == k) qs.add(w);
                    if (k != 1 && qs.isEmpty()) continue;
                    send(s.ranges().get(k - 1), batch(probeId, Kind.PROBE, staging.meta(), List.of(),
                        qs, List.of(), List.of(), false, Status.PENDING)); count++;
                }
                Txn queried = staging; boolean prevented = false;
                for (int i = 0; i < count; i++) {
                    Answer r = answer(probeId); if (r.range() == 1) queried = r.txn();
                    prevented |= !r.unpushed();
                }
                boolean changed = queried.finalized() || queried.meta().epoch() > staging.meta().epoch()
                    || queried.meta().write() > staging.meta().write();
                if (!changed) {
                    int rid = ++token;
                    send(s.ranges().get(0), batch(rid, Kind.RECOVER, staging.meta(), List.of(), List.of(),
                        List.of(), List.of(), !prevented, Status.PENDING)); answer(rid);
                }
            }
            if (!pushed.finalized()) throw new AssertionError("push did not finalize");
            int resolveId = ++token;
            for (Tid range : s.ranges()) send(range, batch(resolveId, Kind.RESOLVE, pushed.meta(), List.of(),
                List.of(), List.of(), keys(), false, pushed.status()));
            for (int k = 0; k < n; k++) answer(resolveId);
            send(s.preventers().get(index), new Recovered(pushed.status() == Status.COMMITTED ? new Committed() : new Aborted()));
        }
    }

    /** All mutable storage below is owned exclusively by a single range process. */
    final class Store {
        final int key; final Setup setup;
        Meta intent; int sequence; int value; int cache; int tombstone; int pushMarker;
        Txn record;
        Store(int key, Setup setup) { this.key = key; this.setup = setup; }
        Txn synth(Meta meta) {
            return new Txn(meta.at(Math.max(meta.write(), pushMarker)),
                tombstone < meta.min() ? Status.PENDING : Status.ABORTED, List.of(), List.of());
        }
        void resolve(Status fate, Meta meta) {
            if (intent == null || intent.epoch() > meta.epoch()) return;
            if (fate == Status.COMMITTED) {
                if (intent.epoch() != meta.epoch() || intent.write() > meta.write()) return;
                value = intent.write();
            }
            intent = null;
        }
        Answer evaluate(Batch b) {
            Meta oldIntent = intent; int oldSequence = sequence, oldValue = value;
            Meta meta = b.meta(); Error error = Error.NONE; int staging = 0;
            boolean found = true, unpushed = true;
            Txn txn = record == null ? synth(meta) : record;
            // applyTimestampCache happens before any request in the atomic batch.
            if (!b.puts().isEmpty()) meta = meta.at(Math.max(meta.write(), cache + 1));
            for (Write w : b.puts()) { intent = meta; sequence = w.sequence(); }
            for (Write q : b.queries()) {
                boolean match = intent != null && intent.epoch() == b.meta().epoch() && sequence >= q.sequence();
                boolean atTime = match && intent.write() <= b.meta().write();
                found &= match; unpushed &= atTime;
                if (!atTime) cache = Math.max(cache, b.meta().write());
                if (match && b.kind() == Kind.PRECOMMIT) meta = meta.at(Math.max(meta.write(), intent.write()));
                if (!match && b.kind() == Kind.PRECOMMIT) error = Error.INTENT_MISSING;
            }
            if (key == 1) {
                switch (b.kind()) {
                    case FINAL, EXPLICIT -> {
                        if (record != null && record.status() == Status.COMMITTED) error = Error.ALREADY_COMMITTED;
                        else if (record != null && record.status() == Status.ABORTED) error = Error.TRANSACTION_ABORTED;
                        else if (record == null && tombstone >= meta.min()) error = Error.TRANSACTION_ABORTED;
                        else {
                            if (record != null) meta = meta.at(Math.max(meta.write(), record.meta().write()));
                            else meta = meta.at(Math.max(meta.write(), pushMarker));
                            if (meta.write() != meta.read()) error = Error.RETRY_SERIALIZABLE;
                            else {
                                List<Write> remaining = new ArrayList<>(b.flight());
                                List<Integer> locks = new ArrayList<>(b.locks());
                                // maybeStripInFlightWrites: local final-batch Put is proven by atomicity.
                                for (Write w : b.puts()) if (remaining.remove(w)) locks.add(w.key());
                                Status status = remaining.isEmpty() ? Status.COMMITTED : Status.STAGING;
                                record = new Txn(meta, status, remaining, locks); txn = record;
                                tombstone = Math.max(tombstone, meta.write());
                                if (status == Status.STAGING) staging = meta.write();
                                else finalizeLocks(record);
                            }
                        }

                    }
                    case PUSH -> {
                        txn = record == null ? synth(meta) : record;
                        if (!txn.finalized()) {
                            boolean higher = meta.epoch() > txn.meta().epoch() || meta.write() > txn.meta().write();
                            if (txn.status() == Status.STAGING && !higher) error = Error.INDETERMINATE_COMMIT;
                            else {
                                Meta merged = new Meta(meta.id(), meta.anchor(), Math.max(meta.epoch(), txn.meta().epoch()),
                                    Math.max(meta.write(), txn.meta().write()), txn.meta().read(), meta.min());
                                txn = new Txn(merged, Status.ABORTED, List.of(), txn.locks());
                                if (record != null) record = txn;
                            }
                        }
                        if (txn.status() == Status.ABORTED) tombstone = Math.max(tombstone, txn.meta().min());
                    }
                    case RECOVER -> {
                        txn = record == null ? synth(meta) : record;
                        if (b.implicit()) {
                            if ((txn.status() != Status.STAGING && txn.status() != Status.COMMITTED)
                                || txn.meta().epoch() != meta.epoch() || txn.meta().write() != meta.write())
                                throw new AssertionError("invalid implicitly committed recovery");
                        }
                        boolean changed = txn.meta().epoch() > meta.epoch() || txn.meta().write() > meta.write();
                        if (!txn.finalized() && (b.implicit() || !changed)) {
                            if (txn.status() != Status.STAGING) throw new AssertionError("recover non-staging record");
                            List<Integer> locks = new ArrayList<>(txn.locks());
                            for (Write w : txn.flight()) if (!locks.contains(w.key())) locks.add(w.key());
                            record = new Txn(txn.meta(), b.implicit() ? Status.COMMITTED : Status.ABORTED, List.of(), locks);
                            txn = record;
                            // RecoverTxn itself does not launch intent resolution (source l.226-229).
                        }
                        if (txn.finalized()) tombstone = Math.max(tombstone, meta.write());
                    }
                    default -> { }
                }
            }
            if (b.kind() == Kind.RESOLVE && b.locks().contains(key)) resolve(b.fate(), b.meta());
            // A failed atomic write batch applies no storage mutations. Error-side
            // QueryIntent cache protection remains, as replica_tscache specifies.
            if (error != Error.NONE && (b.kind() == Kind.FINAL || b.kind() == Kind.EXPLICIT)) {
                intent = oldIntent; sequence = oldSequence; value = oldValue;
            }
            // Preserve error timestamp (refresh must see the pushed timestamp).
            return new Answer(b.token(), b.kind(), key, meta.write(), staging, error, txn,
                              record != null, found, unpushed);
        }
        void finalizeLocks(Txn txn) {
            if (txn.locks().contains(key)) resolve(txn.status(), txn.meta());
        }
    }
    void range(int key) {
        Setup s = recv(Setup.class); Store store = new Store(key, s);
        while (true) {
            Batch b = recv(Batch.class);
            Txn before = store.record;
            Answer reply = store.evaluate(b); // No message operation inside evaluation.
            Txn after = store.record;
            // Folded intent resolver: only EndTxn supplies its resolution work here.
            boolean end = b.kind() == Kind.FINAL || b.kind() == Kind.EXPLICIT;
            if (key == 1 && end && after != null && after.finalized()
                && (before == null || !before.finalized())) {
                for (int k : after.locks()) if (k != 1)
                    send(s.ranges().get(k - 1), batch(0, Kind.RESOLVE, after.meta(), List.of(), List.of(),
                        List.of(), after.locks(), false, after.status()));
            }
            // An aborted EndTxn returns its known LockSpans for asynchronous cleanup.
            if (key == 1 && end && reply.error() == Error.TRANSACTION_ABORTED && after != null) {
                for (int k : b.locks())
                    send(s.ranges().get(k - 1), batch(0, Kind.RESOLVE, after.meta(), List.of(), List.of(),
                        List.of(), b.locks(), false, Status.ABORTED));
            }
            if (b.token() != 0) send(b.reply(), reply);
        }
    }
    public String toString() { return "ParallelCommits(" + size() + "," + (delivery == CommunicationModel.P2P ? "fifo" : "bag") + ")"; }
}
