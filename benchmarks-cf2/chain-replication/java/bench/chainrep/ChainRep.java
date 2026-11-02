package bench.chainrep;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Chain replication (van Renesse and Schneider, OSDI 2004, section 3), no failures: servers s0 (head)
 * .. s{N-1} (tail), clients c0 .. c{K-1}. Arguments: impl,N,K,fifo|bag. See NOTES.md.
 */
public final class ChainRep implements MpProgram {

    // ---- client <-> server messages (shape fixed by INTERFACE.md and its addendum) ----

    /** client -> head: update(newVal); {@code from} is the client's index. */
    @Tag("Update")
    public record Update(int from, int value) implements MpMessage {}

    /** tail -> client: the object's value right after the update was applied at the tail. */
    @Tag("UpdateReply")
    public record UpdateReply(int value) implements MpMessage {}

    /** client -> tail: query(); {@code from} is the client's index. */
    @Tag("Query")
    public record Query(int from) implements MpMessage {}

    /** tail -> client: the value of the tail's replica. */
    @Tag("QueryReply")
    public record QueryReply(int value) implements MpMessage {}

    // ---- server <-> server messages (ours) ----

    /** s_i -> s_{i+1}: the state change of update number {@code seq}, made by the head for client {@code from}. */
    @Tag("Forward")
    public record Forward(int seq, int from, int value) implements MpMessage {}

    /** s_{i+1} -> s_i: ack(r) for update number {@code seq}. */
    @Tag("Ack")
    public record Ack(int seq) implements MpMessage {}

    private final int n;
    private final int k;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * Constructed by MpLauncher from {@code impl,<N>,<K>,<fifo|bag>}.
     *
     * @param args the comma-separated launcher arguments
     */
    public ChainRep(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<N>,<K>,<fifo|bag>");
        }
        this.n = Integer.parseInt(args[1]);
        this.k = Integer.parseInt(args[2]);
        if (n < 1 || k < 1) {
            throw new IllegalArgumentException("N and K must be at least 1");
        }
        this.modelName = args[3];
        this.model = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[3]);
        };
    }

    @Override
    public String programName() {
        return "chain-replication";
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
        for (int i = 0; i < n; i++) {
            roles.add("s" + i);
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

    /**
     * Main: creates every process (servers in index order, then clients), fills the address book,
     * starts them in the same order and awaits them. Sends and receives nothing.
     */
    @Override
    public void run() {
        Tid[] servers = new Tid[n];
        Tid[] clients = new Tid[k];
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            MpThread t = new MpThread(() -> server(idx, servers, clients), "s" + i);
            servers[i] = Tid.of(t.getTaskId(), "s" + i);
            threads.add(t);
        }
        for (int i = 0; i < k; i++) {
            final int idx = i;
            MpThread t = new MpThread(() -> client(idx, servers), "c" + i);
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

    /** Client ci: one update to the head, then one query to the tail. */
    private void client(int i, Tid[] servers) {
        Channel.send(model, servers[0], new Update(i, i + 1));
        Channel.recv(model, UpdateReply.class);
        Channel.send(model, servers[n - 1], new Query(i));
        Channel.recv(model, QueryReply.class);
    }

    /** Server s_i; the head when i = 0, the tail when i = N-1 (both when N = 1). */
    private void server(int i, Tid[] servers, Tid[] clients) {
        final boolean head = i == 0;
        final boolean tail = i == n - 1;
        int value = 0;                              // the replica of the object
        List<Integer> hist = new ArrayList<>();     // Hist^i: sequence numbers of the updates applied here
        List<Integer> sent = new ArrayList<>();     // Sent_i: forwarded, not yet acknowledged
        int nextSeq = 1;                            // head: number of the next update; others: next Forward expected
        int nextAck = 1;                            // next ack expected (FIFO)
        int requests = 0;                           // head: Updates received; others: Forwards received
        int closing = 0;                            // tail: Queries received; others: Acks received
        while (requests < k || closing < k) {
            final int wantSeq = nextSeq;
            final int wantAck = nextAck;
            final boolean moreRequests = requests < k;
            final boolean moreClosing = closing < k;
            Predicate<MpMessage> guard = m ->
                    (moreRequests && head && m instanceof Update)
                    || (moreRequests && !head && m instanceof Forward f && f.seq() == wantSeq)
                    || (moreClosing && tail && m instanceof Query)
                    || (moreClosing && !tail && m instanceof Ack a && a.seq() == wantAck);
            MpMessage m = Channel.recv(model, guard);
            if (m instanceof Update u) {
                // Update processing at the head: the new value is computed once, here.
                requests++;
                int seq = nextSeq++;
                value = u.value();
                hist.add(seq);
                if (tail) {
                    Channel.send(model, clients[u.from()], new UpdateReply(value));
                } else {
                    sent.add(seq);
                    Channel.send(model, servers[i + 1], new Forward(seq, u.from(), value));
                }
            } else if (m instanceof Forward f) {
                // A state change forwarded down the chain: each replica only writes it.
                requests++;
                nextSeq++;
                value = f.value();
                hist.add(f.seq());
                if (tail) {
                    Channel.send(model, clients[f.from()], new UpdateReply(value));
                    Channel.send(model, servers[i - 1], new Ack(f.seq()));
                } else {
                    sent.add(f.seq());
                    Channel.send(model, servers[i + 1], f);
                }
            } else if (m instanceof Ack a) {
                closing++;
                nextAck++;
                if (sent.isEmpty() || sent.get(0) != a.seq()) {
                    throw new AssertionError("s" + i + ": ack(" + a.seq() + ") but Sent = " + sent);
                }
                sent.remove(0);
                if (!head) {
                    Channel.send(model, servers[i - 1], a);
                }
            } else if (m instanceof Query q) {
                closing++;
                Channel.send(model, clients[q.from()], new QueryReply(value));
            } else {
                throw new AssertionError("s" + i + ": unexpected message " + m);
            }
        }
        if (!sent.isEmpty() || hist.size() != k) {
            throw new AssertionError("s" + i + " ends with Sent = " + sent + ", |Hist| = " + hist.size());
        }
    }

    @Override
    public String toString() {
        return "ChainRep(impl," + n + "," + k + "," + modelName + ")";
    }
}
