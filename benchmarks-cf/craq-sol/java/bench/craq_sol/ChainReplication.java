package bench.craq_sol; // PORT: Java syntax and message-passing representation of the fixed one-chunk study

import java.util.*; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
import org.mpi_sws.jmc.api.mp.*; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
import org.mpi_sws.jmc.mp.events.*; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
import org.mpi_sws.jmc.mp.launch.MpProgram; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
import org.mpi_sws.jmc.mp.summary.Tag; // PORT: Java syntax and message-passing representation of the fixed one-chunk study

public final class ChainReplication implements MpProgram { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("Write") public record Write(int client, long value) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("Read") public record Read(int client) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("Ack") public record Ack(int client) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("Value") public record Value(int client, long value) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("Unavailable") public record Unavailable(int client) implements MpMessage {} // COORDINATOR (owner, 2026-11-06): visible reply that ends a read at the study cut
    @Tag("UpdateReq") public record UpdateReq(int client, long value, long updateVer, long chainVer, boolean fromClient) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("UpdateRsp") public record UpdateRsp(long updateVer, long commitVer, long chainVer, long checksum) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("RetryRead") public record RetryRead(int client) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    @Tag("NotCommit") public record NotCommit(int client, int target, long updateVer, long commitVer, boolean first) implements MpMessage {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    record NotCommitAnswer(int target, long updateVer, long commitVer) {} // PORT: per-target study cut remembers every failed answer within one read
    enum Code { OK, kChunkNotClean, kChunkCommittedUpdate, kChunkStaleUpdate, kChunkMissingUpdate, kChunkAdvanceUpdate, kChunkStaleCommit } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    record IOResult(Code code, long updateVer, long commitVer, long checksum) {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    record CommitIO(long commitVer, long chainVer) {} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final int q; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final CommunicationModel communication; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    Tid[] nodes = new Tid[3], clients = new Tid[2], retries = new Tid[2]; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public ChainReplication(String[] args) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        String[] p = args; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        if (p.length != 3 || !p[0].equals("impl")) throw new IllegalArgumentException("impl,<q>,<fifo|bag>"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        q = Integer.parseInt(p[1]); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        if (q < 1) throw new IllegalArgumentException("q >= 1"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        communication = switch (p[2]) { case "fifo" -> CommunicationModel.P2P; case "bag" -> CommunicationModel.ASYN; default -> throw new IllegalArgumentException("fifo|bag"); }; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public String programName() { return "ChainReplication"; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public String size() { return Integer.toString(q); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public CommunicationModel model() { return communication; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public List<String> rolesInSpawnOrder() { return List.of("n0", "n1", "n2", "c0", "c1", "retry0", "retry1"); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public List<String> visibleRoles() { return List.of("c0", "c1"); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    void send(Tid dst, MpMessage message) { Channel.send(communication, dst, message); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    MpMessage receive() { return Channel.recv(communication); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    static void check(boolean condition, String reason) { if (!condition) throw new AssertionError(reason); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    public void run() { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        List<MpThread> processes = new ArrayList<>(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        for (int n = 0; n < 3; n++) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            final int index = n; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            MpThread t = new MpThread(() -> new StorageOperator(index).serve(), "n" + n); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            nodes[n] = Tid.of(t.getTaskId(), "n" + n); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            processes.add(t); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        for (int c = 0; c < 2; c++) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            final int index = c; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            MpThread t = new MpThread(() -> new StorageClientImpl(index).run(), "c" + c); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            clients[c] = Tid.of(t.getTaskId(), "c" + c); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            processes.add(t); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        for (int c = 0; c < 2; c++) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            final int index = c; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            MpThread t = new MpThread(() -> new StorageClientImpl(index).batchReadWithRetry(), "retry" + c); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            retries[c] = Tid.of(t.getTaskId(), "retry" + c); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            processes.add(t); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        for (MpThread t : processes) t.start(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        for (MpThread t : processes) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            try { t.join1(); } catch (InterruptedException e) { throw new IllegalStateException(e); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    record TargetSelectionStrategy(boolean headTarget) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        int selectTarget() { return headTarget ? 0 : Nondet.chooseIndex(3); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    int selectRoutingTargetForOps(TargetSelectionStrategy strategy) { return strategy.selectTarget(); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final class StorageClientImpl { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final int client; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        StorageClientImpl(int client) { this.client = client; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void run() { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            for (int j = 0; j < q; j++) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if ((client + j) % 2 == 0) batchWriteWithoutRetry(1L + client + 2L * j); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                    batchReadWithoutRetry(true); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                    MpMessage reply = Channel.recv(communication, m -> m instanceof Value || m instanceof Unavailable); // COORDINATOR (owner, 2026-11-06): a read ends with Value or Unavailable
                    check(reply instanceof Value value ? value.client() == client : ((Unavailable) reply).client() == client, "read client"); // COORDINATOR (owner, 2026-11-06): either reply names this client
                } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void batchWriteWithoutRetry(long value) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            int target = selectRoutingTargetForOps(new TargetSelectionStrategy(true)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            sendWriteRequest(target, value); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void sendWriteRequest(int target, long value) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            send(nodes[target], new Write(client, value)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            Ack ack = Channel.recv(communication, Ack.class); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(ack.client() == client, "write client"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void batchReadWithoutRetry(boolean first) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            int target = selectRoutingTargetForOps(new TargetSelectionStrategy(false)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            send(nodes[target], first ? new Read(client) : new RetryRead(client)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void batchReadWithRetry() { // PORT: fixed internal coroutine process hides retry messages from exporter
            Set<NotCommitAnswer> seen = new HashSet<>(); // PORT: per-target study cut keeps all answers of the current read
            while (true) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                NotCommit answer = Channel.recv(communication, NotCommit.class); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if (answer.first()) seen.clear(); // PORT: never compare failed answers from different reads
                if (!seen.add(new NotCommitAnswer(answer.target(), answer.updateVer(), answer.commitVer()))) { // PORT: cut a repeated version pair from any earlier attempt at this target, including alternating targets
                    send(clients[answer.client()], new Unavailable(answer.client())); // COORDINATOR (owner, 2026-11-06): the cut ends the read with a visible Unavailable reply instead of an unmatchable receive
                    continue; // COORDINATOR (owner, 2026-11-06): this read ends; wait for the next read's first answer
                } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                batchReadWithoutRetry(false); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    record TargetMap(int index) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        boolean isHead() { return index == 0; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        int successor() { return index == 2 ? -1 : index + 1; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    static final class StorageTarget { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        UpdateReq owner; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final Deque<UpdateReq> waiting = new ArrayDeque<>(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        boolean lockChunk(UpdateReq req) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (owner != null) { waiting.addLast(req); return false; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            owner = req; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            return true; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    static final class ChunkReplica { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        long content = 0, updateVer = 0, commitVer = 0; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        boolean dirty = false; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult result(Code code) { return new IOResult(code, updateVer, commitVer, content); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult update(UpdateReq req) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (dirty) return result(Code.kChunkNotClean); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(req.chainVer() == 1, "constant chain version"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (req.updateVer() > 0) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if (req.updateVer() <= commitVer) return result(Code.kChunkCommittedUpdate); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (req.updateVer() <= updateVer) return result(Code.kChunkStaleUpdate); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (req.updateVer() > updateVer + 1) return result(Code.kChunkMissingUpdate); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                updateVer = req.updateVer(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } else { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                updateVer += 1; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if (updateVer > commitVer + 1) return result(Code.kChunkAdvanceUpdate); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            dirty = true; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            content = req.value(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            dirty = false; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            return result(Code.OK); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult commit(CommitIO io) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(io.chainVer() == 1, "constant chain version"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(io.commitVer() <= updateVer, "commit version mismatch"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (dirty) return result(Code.kChunkNotClean); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else if (commitVer < io.commitVer()) commitVer = io.commitVer(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else return result(Code.kChunkStaleCommit); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            return result(Code.OK); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        boolean aioPrepareRead() { return commitVer == updateVer; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        boolean aioFinishRead(long preparedUpdateVer) { return preparedUpdateVer == updateVer; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final class StorageOperator { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final int index; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final TargetMap targetMap; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final StorageTarget target = new StorageTarget(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final ChunkReplica chunk = new ChunkReplica(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final ReliableUpdate reliableUpdate = new ReliableUpdate(this); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final ReliableForwarding reliableForwarding = new ReliableForwarding(this); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult updateResult; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        UpdateReq active; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        StorageOperator(int index) { this.index = index; targetMap = new TargetMap(index); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void serve() { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            while (true) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                MpMessage message = receive(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if (message instanceof Write w) write(w); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (message instanceof UpdateReq u) update(u); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (message instanceof UpdateRsp r) handleUpdate(r); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (message instanceof Read r) batchRead(r.client(), true); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else if (message instanceof RetryRead r) batchRead(r.client(), false); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else throw new AssertionError("unexpected message " + message); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void write(Write req) { reliableUpdate.update(new UpdateReq(req.client(), req.value(), 0, 1, true)); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void update(UpdateReq req) { reliableUpdate.update(req); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void handleUpdate(UpdateReq req) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(!req.fromClient() || targetMap.isHead(), "non-head client write"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (!target.lockChunk(req)) return; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            updateResult = doUpdate(req); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            Code code = updateResult.code(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (code == Code.OK) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                if (req.updateVer() == 0) req = new UpdateReq(req.client(), req.value(), updateResult.updateVer(), req.chainVer(), req.fromClient()); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                else check(req.updateVer() == updateResult.updateVer(), "update version mismatch"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } else if (code == Code.kChunkMissingUpdate) throw new AssertionError(code); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else if (code == Code.kChunkCommittedUpdate) { complete(req, new IOResult(Code.OK, req.updateVer(), req.updateVer(), req.value())); return; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else if (code == Code.kChunkStaleUpdate) updateResult = new IOResult(Code.OK, req.updateVer(), updateResult.commitVer(), updateResult.checksum()); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else if (code == Code.kChunkAdvanceUpdate) throw new AssertionError(code); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else throw new AssertionError(code); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            active = req; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            reliableForwarding.forwardWithRetry(req); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void handleUpdate(UpdateRsp forwardResult) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(active != null, "successor reply without active write"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            check(forwardResult.commitVer() == updateResult.updateVer(), "commit version mismatch"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (targetMap.successor() != -1) check(forwardResult.checksum() == updateResult.checksum(), "checksum mismatch"); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            IOResult commitResult = doCommit(new CommitIO(forwardResult.commitVer(), forwardResult.chainVer())); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            Code code = commitResult.code(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (code == Code.OK) { } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else if (code == Code.kChunkStaleCommit) commitResult = new IOResult(Code.OK, updateResult.updateVer(), updateResult.updateVer(), updateResult.checksum()); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else throw new AssertionError(code); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            complete(active, new IOResult(Code.OK, commitResult.updateVer(), commitResult.commitVer(), updateResult.checksum())); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult doUpdate(UpdateReq req) { return chunk.update(req); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        IOResult doCommit(CommitIO io) { return chunk.commit(io); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void complete(UpdateReq req, IOResult result) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            reliableUpdate.cache(req, result); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            target.owner = null; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            active = null; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (req.fromClient()) send(clients[req.client()], new Ack(req.client())); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            else send(nodes[index - 1], new UpdateRsp(result.updateVer(), result.commitVer(), 1, result.checksum())); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (!target.waiting.isEmpty()) handleUpdate(target.waiting.removeFirst()); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void batchRead(int client, boolean first) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            long preparedUpdateVer = chunk.updateVer; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (!chunk.aioPrepareRead()) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                send(retries[client], new NotCommit(client, index, chunk.updateVer, chunk.commitVer, first)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                return; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            long value = chunk.content; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (!chunk.aioFinishRead(preparedUpdateVer)) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                send(retries[client], new NotCommit(client, index, preparedUpdateVer, chunk.commitVer, first)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                return; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            send(clients[client], new Value(client, value)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final class ReliableUpdate { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final StorageOperator operator; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final Map<Integer, IOResult> cached = new HashMap<>(); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        ReliableUpdate(StorageOperator operator) { this.operator = operator; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void update(UpdateReq req) { operator.handleUpdate(req); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void cache(UpdateReq req, IOResult result) { cached.put(req.client(), result); } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    final class ReliableForwarding { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        final StorageOperator operator; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        ReliableForwarding(StorageOperator operator) { this.operator = operator; } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void forwardWithRetry(UpdateReq req) { forward(req); } // PORT: no failures or timeouts; forward once
        void forward(UpdateReq req) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            if (operator.targetMap.successor() == -1) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                operator.handleUpdate(new UpdateRsp(req.updateVer(), req.updateVer(), 1, req.value())); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
                return; // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            doForward(req); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        void doForward(UpdateReq req) { // PORT: Java syntax and message-passing representation of the fixed one-chunk study
            send(nodes[operator.targetMap.successor()], new UpdateReq(req.client(), req.value(), req.updateVer(), 1, false)); // PORT: Java syntax and message-passing representation of the fixed one-chunk study
        } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
    } // PORT: Java syntax and message-passing representation of the fixed one-chunk study
} // PORT: Java syntax and message-passing representation of the fixed one-chunk study
