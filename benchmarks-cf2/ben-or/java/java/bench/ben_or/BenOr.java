package bench.ben_or;

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
 * Ben-Or's randomized consensus protocol (PODC 1983, section 3, step 3 in its first variant) as a
 * message-passing program: N processes p0..p{N-1} run the protocol, N clients c0..c{N-1} each give
 * one process its initial value and wait for the outcome. See NOTES.md for the mapping.
 *
 * <p>Constructed from {@code impl,<N>,<fifo|bag>}.
 */
public final class BenOr implements MpProgram {

    // ---- client <-> process messages (shape fixed by INTERFACE.md) ----

    /** client -> process: the process's initial value; the reply goes to c{from}. */
    @Tag("Input")
    public record Input(int from, int value) implements MpMessage {}

    /** process -> client: the process decided {@code value}. */
    @Tag("Decide")
    public record Decide(int value) implements MpMessage {}

    /** process -> client: no decision by the end of round R. */
    @Tag("Undecided")
    public record Undecided() implements MpMessage {}

    // ---- process <-> process messages (the paper's two message types of a round) ----

    /** The paper's {@code (1, r, x_p)}. */
    @Tag("Type1")
    public record Type1(int r, int x) implements MpMessage {}

    /**
     * The paper's {@code (2, r, v, D)} when {@code d} is true (a D-message), and {@code (2, r, ?)}
     * when {@code d} is false (then {@code v} is {@link #NO_VALUE}).
     */
    @Tag("Type2")
    public record Type2(int r, int v, boolean d) implements MpMessage {}

    /** The value field of a {@code (2, r, ?)} message. */
    public static final int NO_VALUE = -1;

    /** The fault bound t (INTERFACE.md: t = 1). */
    public static final int T = 1;

    /** The maximum number of rounds R (INTERFACE.md: R = 2). */
    public static final int R = 2;

    private final String kind;
    private final int n;
    private final CommunicationModel model;

    /**
     * @param args {@code impl}, N, {@code fifo} or {@code bag}
     */
    public BenOr(String... args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("expected impl,<N>,<fifo|bag>, got " + List.of(args));
        }
        if (!args[0].equals("impl")) {
            throw new IllegalArgumentException("unknown kind " + args[0] + " (only impl)");
        }
        this.kind = args[0];
        this.n = Integer.parseInt(args[1]);
        if (n < 1 || n - T < 1) {
            throw new IllegalArgumentException("N must satisfy N - t >= 1, was " + n);
        }
        switch (args[2]) {
            case "fifo" -> this.model = CommunicationModel.P2P;
            case "bag" -> this.model = CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, was " + args[2]);
        }
    }

    @Override
    public void run() {
        // Create every process first (p* in index order, then the clients), so that every address
        // is known before any process runs; then start them in the same order, then wait for all.
        MpThread[] threads = new MpThread[2 * n];
        Tid[] procs = new Tid[n];
        Tid[] clients = new Tid[n];
        for (int i = 0; i < n; i++) {
            final int me = i;
            threads[i] = new MpThread(() -> process(me, procs, clients), "p" + i);
            procs[i] = Tid.of(threads[i].getTaskId(), "p" + i);
        }
        for (int i = 0; i < n; i++) {
            final int me = i;
            threads[n + i] = new MpThread(() -> client(me, procs), "c" + i);
            clients[i] = Tid.of(threads[n + i].getTaskId(), "c" + i);
        }
        for (MpThread th : threads) {
            th.start();
        }
        for (MpThread th : threads) {
            try {
                th.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while waiting for " + th, e);
            }
        }
    }

    /** Client ci: send Input{i, i mod 2} to pi, wait for the reply. */
    private void client(int i, Tid[] procs) {
        Channel.send(model, procs[i], new Input(i, i % 2));
        Channel.recv(model, m -> m instanceof Decide || m instanceof Undecided);
    }

    /** Process pi: the paper's protocol, with the initial value from its client. */
    private void process(int i, Tid[] procs, Tid[] clients) {
        Input in = Channel.recv(model, Input.class);
        Tid client = clients[in.from()];
        int x = in.value();                                   // initial value x_p
        for (int r = 1; r <= R; r++) {                        // step 0 (r := 1) and step 4 (r := r + 1)
            final int round = r;
            // step 1: send (1, r, x_p) to all processes
            for (Tid q : procs) {
                Channel.send(model, q, new Type1(round, x));
            }
            // step 2: wait till N - t messages of type (1, r, *) are received
            int[] count = new int[2];
            for (int k = 0; k < n - T; k++) {
                Type1 m = Channel.recv(model, Type1.class, msg -> msg.r() == round);
                count[m.x()]++;
            }
            Type2 out = new Type2(round, NO_VALUE, false);    // (2, r, ?)
            for (int v = 0; v <= 1; v++) {
                if (2 * count[v] > n) {                       // more than N/2 messages with value v
                    out = new Type2(round, v, true);          // (2, r, v, D)
                }
            }
            for (Tid q : procs) {
                Channel.send(model, q, out);
            }
            // step 3: wait till N - t messages of type (2, r, *) arrive
            int dCount = 0;
            int dValue = NO_VALUE;
            for (int k = 0; k < n - T; k++) {
                Type2 m = Channel.recv(model, Type2.class, msg -> msg.r() == round);
                if (m.d()) {
                    dCount++;
                    dValue = m.v();
                }
            }
            if (dCount >= 1) {                                // (a) a D-message (2, r, v, D): x_p := v
                x = dValue;
                if (dCount > T) {                             // (b) more than t D-messages: decide v
                    Channel.send(model, client, new Decide(dValue));
                    return;                                   // the process ends after replying
                }
            } else {                                          // (c) x_p := 1 or 0, each with prob. 1/2
                x = Nondet.chooseIndex(2);
            }
        }
        Channel.send(model, client, new Undecided());         // not decided after round R
    }

    @Override
    public String programName() {
        return "ben_or";
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
            roles.add("p" + i);
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

    @Override
    public String toString() {
        return "BenOr(" + kind + "," + n + "," + (model == CommunicationModel.P2P ? "fifo" : "bag") + ")";
    }
}
