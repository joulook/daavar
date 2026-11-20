package bench.ricart_agrawala;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Ricart and Agrawala, "An Optimal Algorithm for Mutual Exclusion in Computer Networks" (CACM 1981),
 * section 2.2, as a message-passing program: N nodes n0..n{N-1} run the algorithm, one client ci per
 * node asks its node to enter the critical section once (Enter/Granted) and to leave it (Exit/Released).
 *
 * <p>Arguments: {@code impl,<N>,<fifo|bag>}.
 */
public final class RicartAgrawala implements MpProgram {

    // ---- client <-> node messages (fixed by INTERFACE.md) ----

    /** client -> node: invoke mutual exclusion. */
    @Tag("Enter")
    public record Enter(int from) implements MpMessage {}

    /** client -> node: release the critical section. */
    @Tag("Exit")
    public record Exit(int from) implements MpMessage {}

    /** node -> client: the node has entered its critical section. */
    @Tag("Granted")
    public record Granted() implements MpMessage {}

    /** node -> client: the node has left the critical section and sent its deferred replies. */
    @Tag("Released")
    public record Released() implements MpMessage {}

    // ---- node <-> node messages (the paper's REQUEST(k, j) and REPLY) ----

    /** REQUEST(Our_Sequence_Number, me): {@code seq} is k, {@code node} is j. */
    @Tag("Request")
    public record Request(int seq, int node) implements MpMessage {}

    /** REPLY: carries nothing, as in the paper. */
    @Tag("Reply")
    public record Reply() implements MpMessage {}

    private final int n;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * @param args {@code impl}, N, {@code fifo} or {@code bag}
     */
    public RicartAgrawala(String... args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("expected impl,<N>,<fifo|bag>");
        }
        if (!args[0].equals("impl")) {
            throw new IllegalArgumentException("unknown kind " + args[0] + " (only impl)");
        }
        this.n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("N must be at least 1");
        }
        switch (args[2]) {
            case "fifo" -> model = CommunicationModel.P2P;
            case "bag" -> model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        }
        this.modelName = args[2];
    }

    @Override
    public String programName() {
        return "ricart_agrawala";
    }

    @Override
    public String size() {
        return Integer.toString(n);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("n" + i);
        }
        for (int i = 0; i < n; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    /**
     * Main: creates the processes (nodes in index order, then clients in index order), so that every
     * address is known before any process runs, starts them in the same order, and awaits them all.
     * It sends and receives nothing.
     */
    @Override
    public void run() {
        Tid[] nodes = new Tid[n];
        Tid[] clients = new Tid[n];
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int me = i;
            MpThread t = new MpThread(() -> node(me, nodes, clients), "n" + i);
            nodes[i] = Tid.of(t.getTaskId(), "n" + i);
            threads.add(t);
        }
        for (int i = 0; i < n; i++) {
            final int me = i;
            MpThread t = new MpThread(() -> client(me, nodes), "c" + i);
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

    /** Client ci: Enter, wait Granted, Exit, wait Released (exactly once). */
    private void client(int i, Tid[] nodes) {
        Channel.send(model, nodes[i], new Enter(i));
        Channel.recv(model, Granted.class);
        Channel.send(model, nodes[i], new Exit(i));
        Channel.recv(model, Released.class);
    }

    /**
     * Node {@code me}: the paper's three processes (invoke mutual exclusion, receive REQUEST,
     * receive REPLY) as one process that handles its messages in arrival order; each message is
     * handled atomically, which plays the role of the semaphore Shared_vars.
     */
    private void node(int me, Tid[] nodes, Tid[] clients) {
        // SHARED DATABASE
        int ourSequenceNumber = 0;
        int highestSequenceNumber = 0;
        int outstandingReplyCount = 0;
        boolean requestingCriticalSection = false;
        boolean[] replyDeferred = new boolean[n];

        boolean released = false;  // own critical section done (Released sent)
        int requestsAnswered = 0;  // REPLYs sent to other nodes' REQUESTs

        // Ends after its own critical section and after answering every other node's request (K = 1).
        while (!(released && requestsAnswered == n - 1)) {
            MpMessage m = Channel.recv(model);
            if (m instanceof Enter e) {
                // PROCESS WHICH INVOKES MUTUAL EXCLUSION: choose a sequence number
                requestingCriticalSection = true;
                ourSequenceNumber = highestSequenceNumber + 1;
                outstandingReplyCount = n - 1;
                for (int j = 0; j < n; j++) {
                    if (j != me) {
                        Channel.send(model, nodes[j], new Request(ourSequenceNumber, me));
                    }
                }
                // WAITFOR (Outstanding_Reply_Count = 0): immediate when there is no other node
                if (outstandingReplyCount == 0) {
                    Channel.send(model, clients[me], new Granted());
                }
            } else if (m instanceof Request r) {
                // PROCESS WHICH RECEIVES REQUEST(k, j) MESSAGES
                int k = r.seq();
                int j = r.node();
                highestSequenceNumber = Math.max(highestSequenceNumber, k);
                boolean deferIt = requestingCriticalSection
                        && ((k > ourSequenceNumber) || (k == ourSequenceNumber && j > me));
                if (deferIt) {
                    replyDeferred[j] = true;
                } else {
                    Channel.send(model, nodes[j], new Reply());
                    requestsAnswered++;
                }
            } else if (m instanceof Reply) {
                // PROCESS WHICH RECEIVES REPLY MESSAGES
                outstandingReplyCount--;
                if (outstandingReplyCount == 0) {
                    // Critical Section Processing can be performed at this point
                    Channel.send(model, clients[me], new Granted());
                }
            } else if (m instanceof Exit e) {
                // Release the Critical Section
                requestingCriticalSection = false;
                for (int j = 0; j < n; j++) {
                    if (replyDeferred[j]) {
                        replyDeferred[j] = false;
                        Channel.send(model, nodes[j], new Reply());
                        requestsAnswered++;
                    }
                }
                Channel.send(model, clients[me], new Released());
                released = true;
            } else {
                throw new IllegalStateException("node n" + me + ": unexpected message " + m);
            }
        }
    }

    @Override
    public String toString() {
        return "RicartAgrawala(N=" + n + ", " + modelName + ")";
    }
}
