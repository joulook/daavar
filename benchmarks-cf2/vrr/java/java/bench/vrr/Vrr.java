package bench.vrr;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Viewstamped Replication (Liskov and Cowling 2012), normal operation (section 4.1) only:
 * fixed group of N = 2f + 1 replicas, view-number 0, primary r0, backups r1..r{N-1}; K clients
 * c0..c{K-1}, one request each. The service is a counter whose operation add(n) returns the new value.
 *
 * <p>Launcher argument string: {@code impl,<N>,<K>,<fifo|bag>}.
 */
public final class Vrr implements MpProgram {

    // ---- messages -----------------------------------------------------------------------------

    /** Client to primary: the paper's REQUEST op, c, s (request-number s = 1 is implicit). */
    @Tag("Request")
    public record Request(int from, int op) implements MpMessage {}

    /** Primary to client: the paper's REPLY v, s, x with v and s dropped. */
    @Tag("Reply")
    public record Reply(int result) implements MpMessage {}

    /** Primary to backups: PREPARE v, m, n, k. */
    @Tag("Prepare")
    public record Prepare(int v, Request m, int n, int k) implements MpMessage {}

    /** Backup i to primary: PREPAREOK v, n, i. */
    @Tag("PrepareOk")
    public record PrepareOk(int v, int n, int i) implements MpMessage {}

    /** Primary to backups: COMMIT v, k. */
    @Tag("Commit")
    public record Commit(int v, int k) implements MpMessage {}

    // ---- parameters ---------------------------------------------------------------------------

    private static final int VIEW = 0;
    private static final int REQUEST_NUMBER = 1;

    private final String kind;
    private final int n;
    private final int k;
    private final int f;
    private final CommunicationModel model;

    /**
     * Built from the launcher's argument string {@code impl,<N>,<K>,<fifo|bag>}.
     *
     * @param args kind, N, K, model
     */
    public Vrr(String... args) {
        if (args.length != 4) {
            throw new IllegalArgumentException("expected impl,<N>,<K>,<fifo|bag>, got " + List.of(args));
        }
        if (!args[0].equals("impl")) {
            throw new IllegalArgumentException("unknown kind " + args[0]);
        }
        this.kind = args[0];
        this.n = Integer.parseInt(args[1]);
        this.k = Integer.parseInt(args[2]);
        if (n < 3 || n % 2 == 0 || k < 1) {
            throw new IllegalArgumentException("need N = 2f + 1 >= 3 and K >= 1");
        }
        this.f = (n - 1) / 2;
        switch (args[3]) {
            case "bag" -> this.model = CommunicationModel.ASYN;
            case "fifo" -> this.model = CommunicationModel.P2P;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[3]);
        }
    }

    @Override
    public String programName() {
        return "vrr";
    }

    @Override
    public String size() {
        return n + "," + k;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int j = 0; j < n; j++) {
            roles.add("r" + j);
        }
        for (int i = 0; i < k; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    // ---- main ---------------------------------------------------------------------------------

    /**
     * Creates every process first (minting its address), so that every process knows every other
     * address from the start; then starts replicas in index order, then clients; then awaits all.
     * Main sends and receives nothing.
     */
    @Override
    public void run() {
        Tid[] replicas = new Tid[n];
        Tid[] clients = new Tid[k];
        List<MpThread> threads = new ArrayList<>();
        for (int j = 0; j < n; j++) {
            final int idx = j;
            Runnable body = idx == 0 ? () -> primary(replicas, clients) : () -> backup(idx, replicas);
            MpThread t = new MpThread(body, "r" + j);
            replicas[j] = Tid.of(t.getTaskId(), "r" + j);
            threads.add(t);
        }
        for (int i = 0; i < k; i++) {
            final int idx = i;
            MpThread t = new MpThread(() -> client(idx, replicas), "c" + i);
            clients[i] = Tid.of(t.getTaskId(), "c" + i);
            threads.add(t);
        }
        for (MpThread t : threads) {
            t.start();
        }
        for (MpThread t : threads) {
            try {
                t.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while awaiting " + t, e);
            }
        }
    }

    // ---- client -------------------------------------------------------------------------------

    /** Client ci: one REQUEST add(i + 1) to the primary, then wait for the REPLY. */
    private void client(int i, Tid[] replicas) {
        Channel.send(model, replicas[0], new Request(i, i + 1));
        Channel.recv(model, Reply.class);
    }

    // ---- primary ------------------------------------------------------------------------------

    /** Replica r0, the primary of view 0. */
    private void primary(Tid[] replicas, Tid[] clients) {
        int opNumber = 0;
        int commitNumber = 0;
        int counter = 0;
        List<Request> log = new ArrayList<>();      // log.get(x - 1) is op-number x
        int[] tableRequest = new int[k];             // client-table: most recent request-number
        int[] tableResult = new int[k];              // client-table: result, when executed
        boolean[] tableExecuted = new boolean[k];
        int[] highestOk = new int[n];                // per backup: highest op-number it PREPAREOKed
        int replied = 0;
        int lastCommitSent = 0;

        while (replied < k) {
            MpMessage msg = Channel.recv(model, m -> m instanceof Request || m instanceof PrepareOk);
            if (msg instanceof Request req) {
                // Step 2: compare the request-number with the client-table.
                int c = req.from();
                if (REQUEST_NUMBER <= tableRequest[c]) {
                    if (REQUEST_NUMBER == tableRequest[c] && tableExecuted[c]) {
                        Channel.send(model, clients[c], new Reply(tableResult[c]));
                    }
                    continue;
                }
                // Step 3: advance op-number, append, update client-table, PREPARE to the backups.
                opNumber++;
                log.add(req);
                tableRequest[c] = REQUEST_NUMBER;
                tableExecuted[c] = false;
                for (int j = 1; j < n; j++) {
                    Channel.send(model, replicas[j], new Prepare(VIEW, req, opNumber, commitNumber));
                }
            } else {
                PrepareOk ok = (PrepareOk) msg;
                if (ok.v() != VIEW) {
                    continue;
                }
                // A PREPAREOK for n says that n and all earlier operations prepared at backup i.
                highestOk[ok.i()] = Math.max(highestOk[ok.i()], ok.n());
                // Step 5: an operation is committed once f different backups prepared it; execute
                // committed operations in op-number order, reply, update the client-table.
                while (commitNumber < opNumber && preparedBy(highestOk, commitNumber + 1) >= f) {
                    Request op = log.get(commitNumber);
                    counter += op.op();
                    commitNumber++;
                    Channel.send(model, clients[op.from()], new Reply(counter));
                    tableResult[op.from()] = counter;
                    tableExecuted[op.from()] = true;
                    replied++;
                }
                // Step 6: with nothing in progress (commit-number = op-number), the primary may
                // time out waiting for a new request and send COMMIT. When every client's request
                // has been received no new one can come, so the timeout certainly fires.
                if (commitNumber == opNumber && commitNumber > lastCommitSent) {
                    boolean timeout = opNumber == k || Nondet.chooseBoolean();
                    if (timeout) {
                        for (int j = 1; j < n; j++) {
                            Channel.send(model, replicas[j], new Commit(VIEW, commitNumber));
                        }
                        lastCommitSent = commitNumber;
                    }
                }
            }
        }
    }

    /** The number of backups whose highest PREPAREOK covers op-number x. */
    private int preparedBy(int[] highestOk, int x) {
        int count = 0;
        for (int j = 1; j < n; j++) {
            if (highestOk[j] >= x) {
                count++;
            }
        }
        return count;
    }

    // ---- backup -------------------------------------------------------------------------------

    /** Replica ri, i >= 1, a backup. */
    private void backup(int i, Tid[] replicas) {
        int opNumber = 0;
        int commitNumber = 0;
        int knownCommit = 0;                          // the highest commit-number learned
        int counter = 0;
        List<Request> log = new ArrayList<>();
        int[] tableRequest = new int[k];
        int[] tableResult = new int[k];

        while (commitNumber < k) {
            // Step 4: PREPAREs in op-number order — only the next one is accepted; a COMMIT any time.
            final int next = opNumber + 1;
            MpMessage msg = Channel.recv(model,
                    m -> (m instanceof Prepare p && p.n() == next) || m instanceof Commit);
            if (msg instanceof Prepare p) {
                if (p.v() != VIEW) {
                    continue;
                }
                opNumber++;
                log.add(p.m());
                tableRequest[p.m().from()] = REQUEST_NUMBER;
                Channel.send(model, replicas[0], new PrepareOk(VIEW, opNumber, i));
                knownCommit = Math.max(knownCommit, p.k());
            } else {
                Commit c = (Commit) msg;
                if (c.v() != VIEW) {
                    continue;
                }
                knownCommit = Math.max(knownCommit, c.k());
            }
            // Step 7: execute every committed operation that is in the log, in order; no reply.
            while (commitNumber < knownCommit && commitNumber < opNumber) {
                Request op = log.get(commitNumber);
                counter += op.op();
                commitNumber++;
                tableResult[op.from()] = counter;
            }
        }
    }

    @Override
    public String toString() {
        return "Vrr(" + kind + ", N=" + n + ", K=" + k + ", " + model + ")";
    }
}
