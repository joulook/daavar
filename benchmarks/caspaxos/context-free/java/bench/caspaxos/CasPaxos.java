package bench.caspaxos;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * CASPaxos (Rystsov, arXiv:1802.07000v9, sections 2.1-2.2) as a message-passing program over JMC's
 * Must API. Roles: clients c0..c{K-1} (visible), proposers p0..p{P-1}, acceptors a0..a{A-1}.
 * Arguments: {@code impl,<A>,<K>,<fifo|bag>}. See NOTES.md for the step-by-step mapping.
 */
public final class CasPaxos implements MpProgram {

    /** Number of proposers (fixed by the interface). */
    static final int P = 2;
    /** Times a proposer may retry one request after a conflict (fixed by the interface). */
    static final int R = 1;

    /** Phase tags carried by a conflict reply. */
    static final int PHASE_PREPARE = 1;
    static final int PHASE_ACCEPT = 2;

    // ---- client <-> proposer (shape fixed by INTERFACE.md) ----

    @Tag("Cas")
    public record Cas(int expected, int value) implements MpMessage {}

    @Tag("Done")
    public record Done(int value) implements MpMessage {}

    @Tag("Fail")
    public record Fail() implements MpMessage {}

    // ---- proposer <-> acceptor; a ballot is the pair (counter, id), id = proposer index ----

    /** Phase 1 request: "prepare" with ballot B. */
    @Tag("Prepare")
    public record Prepare(int counter, int id) implements MpMessage {}

    /**
     * Phase 1 confirmation for ballot B = (counter, id): either empty (nothing accepted yet) or
     * the accepted tuple (accCounter, accId, value).
     */
    @Tag("Promise")
    public record Promise(int counter, int id, boolean empty, int accCounter, int accId, int value)
            implements MpMessage {}

    /** Phase 2 request: "accept" with ballot B and the new state. */
    @Tag("Accept")
    public record Accept(int counter, int id, int value) implements MpMessage {}

    /** Phase 2 confirmation for ballot B. */
    @Tag("Accepted")
    public record Accepted(int counter, int id) implements MpMessage {}

    /**
     * The acceptor already saw a greater ballot than B = (counter, id) in the given phase; it
     * reports the greatest ballot it saw, (seenCounter, seenId).
     */
    @Tag("Conflict")
    public record Conflict(int phase, int counter, int id, int seenCounter, int seenId)
            implements MpMessage {}

    /**
     * Not in the paper (termination only): proposer {@code id} is finished and sent {@code count}
     * messages in total to the receiving acceptor.
     */
    @Tag("Stop")
    public record Stop(int id, int count) implements MpMessage {}

    // ---- instance ----

    private final int acceptors;
    private final int clients;
    private final CommunicationModel model;
    private final String modelName;

    /**
     * Constructed by the launcher from {@code impl,<A>,<K>,<fifo|bag>}.
     *
     * @param args the comma-separated arguments
     */
    public CasPaxos(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<A>,<K>,<fifo|bag>, got " + String.join(",", args));
        }
        this.acceptors = Integer.parseInt(args[1]);
        this.clients = Integer.parseInt(args[2]);
        if (acceptors < 1 || acceptors % 2 == 0 || clients < 1) {
            throw new IllegalArgumentException("A must be odd and positive (2F + 1), K positive");
        }
        this.modelName = args[3];
        this.model = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag, got " + args[3]);
        };
    }

    @Override
    public String programName() {
        return "caspaxos";
    }

    @Override
    public String size() {
        return acceptors + "," + clients;
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int j = 0; j < P; j++) {
            roles.add("p" + j);
        }
        for (int k = 0; k < acceptors; k++) {
            roles.add("a" + k);
        }
        roles.addAll(visibleRoles());
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            roles.add("c" + i);
        }
        return roles;
    }

    /** Process identifiers of one iteration; written before any reader can causally need them. */
    private static final class Topology {
        final AtomicReferenceArray<Tid> proposers = new AtomicReferenceArray<>(P);
        final AtomicReferenceArray<Tid> acceptors;
        final AtomicReferenceArray<Tid> clients;

        Topology(int a, int k) {
            acceptors = new AtomicReferenceArray<>(a);
            clients = new AtomicReferenceArray<>(k);
        }
    }

    /** Main: spawns p*, a*, then c* in index order, then awaits all; sends and receives nothing. */
    @Override
    public void run() {
        Topology topo = new Topology(acceptors, clients);
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int j = 0; j < P; j++) {
            final int id = j;
            MpProcess.Handle h = MpProcess.spawn("p" + j, () -> proposer(id, topo));
            topo.proposers.set(j, h.tid());
            handles.add(h);
        }
        for (int k = 0; k < acceptors; k++) {
            MpProcess.Handle h = MpProcess.spawn("a" + k, () -> acceptor(topo));
            topo.acceptors.set(k, h.tid());
            handles.add(h);
        }
        for (int i = 0; i < clients; i++) {
            final int idx = i;
            handles.add(MpProcess.spawn("c" + i, () -> client(idx, topo)));
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    // ---- client ----

    private void client(int i, Topology topo) {
        // The client's own id is published before its request leaves, so the proposer that
        // receives the request can address the reply (Cas carries no sender).
        topo.clients.set(i, MpProcess.self());
        Channel.send(model, topo.proposers.get(i % P), new Cas(i, i + 1));
        Channel.recv(model, m -> m instanceof Done || m instanceof Fail);
    }

    // ---- proposer ----

    /** Ballot comparison: counter first, id as the tiebreaker. */
    static int compareBallot(int c1, int id1, int c2, int id2) {
        if (c1 != c2) {
            return Integer.compare(c1, c2);
        }
        return Integer.compare(id1, id2);
    }

    private void proposer(int id, Topology topo) {
        int requests = 0;
        for (int i = 0; i < clients; i++) {
            if (i % P == id) {
                requests++;
            }
        }
        int f = (acceptors - 1) / 2;
        int quorum = f + 1;
        int counter = 0;
        int sentPerAcceptor = 0;

        for (int served = 0; served < requests; served++) {
            Cas req = (Cas) Channel.recv(model, m -> m instanceof Cas);
            // Received after c_{req.expected} published its id and after main recorded every
            // acceptor (all acceptors are spawned before any client).
            Tid replyTo = topo.clients.get(req.expected());
            boolean done = false;
            int result = 0;
            for (int attempt = 0; attempt <= R && !done; attempt++) {
                // Generate a fresh ballot B = (counter, id).
                counter++;
                final int bc = counter;
                final int bid = id;

                // Phase 1: prepare to every acceptor.
                for (int k = 0; k < acceptors; k++) {
                    Channel.send(model, topo.acceptors.get(k), new Prepare(bc, bid));
                }
                sentPerAcceptor++;
                int confirmations = 0;
                int conflicts = 0;
                boolean anyValue = false;
                int bestC = 0;
                int bestId = 0;
                int current = 0; // the paper's empty state, represented as 0
                while (confirmations < quorum && conflicts <= acceptors - quorum) {
                    MpMessage m = Channel.recv(model, x ->
                            (x instanceof Promise p && p.counter() == bc && p.id() == bid)
                                    || (x instanceof Conflict c && c.phase() == PHASE_PREPARE
                                            && c.counter() == bc && c.id() == bid));
                    if (m instanceof Promise p) {
                        confirmations++;
                        if (!p.empty() && (!anyValue
                                || compareBallot(p.accCounter(), p.accId(), bestC, bestId) > 0)) {
                            anyValue = true;
                            bestC = p.accCounter();
                            bestId = p.accId();
                            current = p.value();
                        }
                    } else {
                        Conflict c = (Conflict) m;
                        conflicts++;
                        counter = Math.max(counter, c.seenCounter());
                    }
                }
                if (confirmations < quorum) {
                    continue; // phase 1 lost; retry with a new ballot if retries remain
                }

                // Apply the change function: if current == expected then value else current.
                int next = current == req.expected() ? req.value() : current;

                // Phase 2: accept to every acceptor.
                for (int k = 0; k < acceptors; k++) {
                    Channel.send(model, topo.acceptors.get(k), new Accept(bc, bid, next));
                }
                sentPerAcceptor++;
                confirmations = 0;
                conflicts = 0;
                while (confirmations < quorum && conflicts <= acceptors - quorum) {
                    MpMessage m = Channel.recv(model, x ->
                            (x instanceof Accepted a && a.counter() == bc && a.id() == bid)
                                    || (x instanceof Conflict c && c.phase() == PHASE_ACCEPT
                                            && c.counter() == bc && c.id() == bid));
                    if (m instanceof Accepted) {
                        confirmations++;
                    } else {
                        Conflict c = (Conflict) m;
                        conflicts++;
                        counter = Math.max(counter, c.seenCounter());
                    }
                }
                if (confirmations >= quorum) {
                    done = true;
                    result = next;
                }
            }
            Channel.send(model, replyTo, done ? new Done(result) : new Fail());
        }
        // Termination (not in the paper): tell each acceptor how many messages it will get from us.
        for (int k = 0; k < acceptors; k++) {
            Channel.send(model, topo.acceptors.get(k), new Stop(id, sentPerAcceptor));
        }
    }

    // ---- acceptor ----

    private void acceptor(Topology topo) {
        boolean promised = false;
        int promC = 0;
        int promId = 0;
        boolean accepted = false;
        int accC = 0;
        int accId = 0;
        int value = 0;

        int stops = 0;
        int expected = 0;
        int handled = 0;
        while (stops < P || handled < expected) {
            MpMessage m = Channel.recv(model);
            if (m instanceof Stop s) {
                stops++;
                expected += s.count();
                continue;
            }
            handled++;
            int bc;
            int bid;
            int phase;
            if (m instanceof Prepare p) {
                bc = p.counter();
                bid = p.id();
                phase = PHASE_PREPARE;
            } else if (m instanceof Accept a) {
                bc = a.counter();
                bid = a.id();
                phase = PHASE_ACCEPT;
            } else {
                throw new IllegalStateException("acceptor received " + m);
            }
            Tid proposer = topo.proposers.get(bid);

            // The greatest ballot seen: the promise or the accepted ballot.
            boolean seen = false;
            int seenC = 0;
            int seenId = 0;
            if (promised) {
                seen = true;
                seenC = promC;
                seenId = promId;
            }
            if (accepted && (!seen || compareBallot(accC, accId, seenC, seenId) > 0)) {
                seen = true;
                seenC = accC;
                seenId = accId;
            }
            if (seen && compareBallot(seenC, seenId, bc, bid) > 0) {
                Channel.send(model, proposer, new Conflict(phase, bc, bid, seenC, seenId));
                continue;
            }
            if (phase == PHASE_PREPARE) {
                promised = true;
                promC = bc;
                promId = bid;
                Channel.send(model, proposer, accepted
                        ? new Promise(bc, bid, false, accC, accId, value)
                        : new Promise(bc, bid, true, 0, 0, 0));
            } else {
                Accept a = (Accept) m;
                promised = false;
                accepted = true;
                accC = bc;
                accId = bid;
                value = a.value();
                Channel.send(model, proposer, new Accepted(bc, bid));
            }
        }
    }

    @Override
    public String toString() {
        return "CasPaxos(impl,A=" + acceptors + ",K=" + clients + "," + modelName + ",P=" + P + ",R=" + R + ")";
    }
}
