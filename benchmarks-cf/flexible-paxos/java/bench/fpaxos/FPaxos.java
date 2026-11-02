package bench.fpaxos;

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
 * Flexible Paxos (Howard, Malkhi, Spiegelman, OPODIS 2016), single decree, majority quorums of
 * section 4.1: Q1 = A/2 + 1, Q2 = A/2 (A even). Arguments: impl,&lt;A&gt;,&lt;K&gt;,&lt;fifo|bag&gt;; P = K.
 */
public final class FPaxos implements MpProgram {

    /** Retries a proposer may make after a failed phase (interface: R = 1). */
    static final int R = 1;

    // ---- client <-> proposer (externally visible) ----
    @Tag("Propose") public record Propose(int from, int value) implements MpMessage {}
    @Tag("Decided") public record Decided(int value) implements MpMessage {}
    @Tag("Fail") public record Fail() implements MpMessage {}

    // ---- proposer <-> acceptor (internal). A proposal number is (round, proposer). ----
    /** Phase 1 i: prepare(p). */
    @Tag("Prepare") public record Prepare(int round, int proposer) implements MpMessage {}
    /** Phase 1 ii: promise(p, (p', v')); acceptedRound = 0 means no accepted proposal. */
    @Tag("Promise") public record Promise(int acceptor, int round, int proposer,
            int acceptedRound, int acceptedProposer, int acceptedValue) implements MpMessage {}
    /** Phase 2 i: propose(p, v). */
    @Tag("ProposeP2") public record ProposeP2(int round, int proposer, int value) implements MpMessage {}
    /** Phase 2 ii: accept(p). */
    @Tag("Accept") public record Accept(int acceptor, int round, int proposer) implements MpMessage {}
    /** Conflict reply to prepare (phase 1) or propose (phase 2): carries the acceptor's promise. */
    @Tag("Nack") public record Nack(int acceptor, int phase, int round, int proposer,
            int promisedRound, int promisedProposer) implements MpMessage {}
    /** Termination: proposer is finished; an acceptor stops after one Done from every proposer. */
    @Tag("Done") public record Done(int proposer) implements MpMessage {}

    private final int a;
    private final int k;
    private final CommunicationModel model;
    private final String modelName;

    public FPaxos(String... args) {
        if (args.length != 4 || !args[0].equals("impl")) {
            throw new IllegalArgumentException("expected impl,<A>,<K>,<fifo|bag>");
        }
        this.a = Integer.parseInt(args[1]);
        this.k = Integer.parseInt(args[2]);
        this.modelName = args[3];
        this.model = switch (args[3]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[3]);
        };
        if (a < 2 || a % 2 != 0 || k < 1) {
            throw new IllegalArgumentException("A must be even and >= 2, K >= 1");
        }
    }

    int q1() { return a / 2 + 1; }
    int q2() { return a / 2; }

    static int cmp(int r1, int p1, int r2, int p2) {
        return r1 != r2 ? Integer.compare(r1, r2) : Integer.compare(p1, p2);
    }

    @Override
    public void run() {
        final int nP = k;
        final AtomicReferenceArray<Tid> acc = new AtomicReferenceArray<>(a);
        final AtomicReferenceArray<Tid> prop = new AtomicReferenceArray<>(nP);
        final AtomicReferenceArray<Tid> cli = new AtomicReferenceArray<>(k);
        List<MpProcess.Handle> handles = new ArrayList<>();
        for (int i = 0; i < a; i++) {
            final int me = i;
            MpProcess.Handle h = MpProcess.spawn("a" + i, () -> {
                acc.set(me, MpProcess.self().withRole("a" + me));
                acceptor(me, nP, prop);
            });
            acc.set(i, h.tid());
            handles.add(h);
        }
        for (int i = 0; i < nP; i++) {
            final int me = i;
            MpProcess.Handle h = MpProcess.spawn("p" + i, () -> {
                prop.set(me, MpProcess.self().withRole("p" + me));
                proposer(me, acc, cli);
            });
            prop.set(i, h.tid());
            handles.add(h);
        }
        for (int i = 0; i < k; i++) {
            final int me = i;
            MpProcess.Handle h = MpProcess.spawn("c" + i, () -> {
                cli.set(me, MpProcess.self().withRole("c" + me));
                client(me, prop);
            });
            cli.set(i, h.tid());
            handles.add(h);
        }
        for (MpProcess.Handle h : handles) {
            h.await();
        }
    }

    // ---- client ci: send Propose{i, i+1} to pi, wait for the reply ----
    private void client(int me, AtomicReferenceArray<Tid> prop) {
        Channel.send(model, prop.get(me), new Propose(me, me + 1));
        Channel.recv(model, m -> m instanceof Decided || m instanceof Fail);
    }

    // ---- acceptor ----
    private void acceptor(int me, int nP, AtomicReferenceArray<Tid> prop) {
        int promisedRound = 0;
        int promisedProposer = -1;
        int acceptedRound = 0;
        int acceptedProposer = -1;
        int acceptedValue = 0;
        int done = 0;
        while (done < nP) {
            MpMessage m = Channel.recv(model,
                    x -> x instanceof Prepare || x instanceof ProposeP2 || x instanceof Done);
            if (m instanceof Done) {
                done++;
            } else if (m instanceof Prepare p) {
                // Phase 1 ii: promise only if p is higher than every number promised so far.
                if (cmp(p.round(), p.proposer(), promisedRound, promisedProposer) > 0) {
                    promisedRound = p.round();
                    promisedProposer = p.proposer();
                    Channel.send(model, prop.get(p.proposer()), new Promise(me, p.round(), p.proposer(),
                            acceptedRound, acceptedProposer, acceptedValue));
                } else {
                    Channel.send(model, prop.get(p.proposer()), new Nack(me, 1, p.round(), p.proposer(),
                            promisedRound, promisedProposer));
                }
            } else if (m instanceof ProposeP2 p) {
                // Phase 2 ii: accept if p is equal to or greater than the promised number.
                if (cmp(p.round(), p.proposer(), promisedRound, promisedProposer) >= 0) {
                    promisedRound = p.round();
                    promisedProposer = p.proposer();
                    acceptedRound = p.round();
                    acceptedProposer = p.proposer();
                    acceptedValue = p.value();
                    Channel.send(model, prop.get(p.proposer()), new Accept(me, p.round(), p.proposer()));
                } else {
                    Channel.send(model, prop.get(p.proposer()), new Nack(me, 2, p.round(), p.proposer(),
                            promisedRound, promisedProposer));
                }
            }
        }
    }

    // ---- proposer pi (also the learner of its own decision) ----
    private void proposer(int me, AtomicReferenceArray<Tid> acc, AtomicReferenceArray<Tid> cli) {
        Propose req = Channel.recv(model, Propose.class);
        final Tid client = cli.get(req.from());
        int round = 1;
        boolean decided = false;
        int decidedValue = 0;
        for (int attempt = 0; attempt <= R && !decided; attempt++) {
            final int r = round;
            int highestSeen = round;
            // Phase 1 i: prepare(p) to the acceptors.
            for (int i = 0; i < a; i++) {
                Channel.send(model, acc.get(i), new Prepare(r, me));
            }
            // Phase 1 iii: wait for Q1 promises, or until Q1 can no longer be reached.
            int promises = 0;
            int nacks = 0;
            int bestRound = 0;
            int bestProposer = -1;
            int bestValue = 0;
            while (promises < q1() && nacks <= a - q1()) {
                MpMessage m = Channel.recv(model, x ->
                        (x instanceof Promise pr && pr.round() == r && pr.proposer() == me)
                        || (x instanceof Nack n && n.phase() == 1 && n.round() == r && n.proposer() == me));
                if (m instanceof Promise pr) {
                    promises++;
                    if (pr.acceptedRound() > 0
                            && cmp(pr.acceptedRound(), pr.acceptedProposer(), bestRound, bestProposer) > 0) {
                        bestRound = pr.acceptedRound();
                        bestProposer = pr.acceptedProposer();
                        bestValue = pr.acceptedValue();
                    }
                } else {
                    Nack n = (Nack) m;
                    nacks++;
                    highestSeen = Math.max(highestSeen, n.promisedRound());
                }
            }
            if (promises < q1()) {
                round = highestSeen + 1;
                continue;
            }
            // Phase 2 i: value of the highest-numbered accepted proposal, else the client's value.
            final int v = bestRound > 0 ? bestValue : req.value();
            for (int i = 0; i < a; i++) {
                Channel.send(model, acc.get(i), new ProposeP2(r, me, v));
            }
            // Phase 2 iii: wait for Q2 accepts, or until Q2 can no longer be reached.
            int accepts = 0;
            nacks = 0;
            while (accepts < q2() && nacks <= a - q2()) {
                MpMessage m = Channel.recv(model, x ->
                        (x instanceof Accept ac && ac.round() == r && ac.proposer() == me)
                        || (x instanceof Nack n && n.phase() == 2 && n.round() == r && n.proposer() == me));
                if (m instanceof Accept) {
                    accepts++;
                } else {
                    nacks++;
                    highestSeen = Math.max(highestSeen, ((Nack) m).promisedRound());
                }
            }
            if (accepts >= q2()) {
                decided = true;
                decidedValue = v;
            } else {
                round = highestSeen + 1;
            }
        }
        Channel.send(model, client, decided ? new Decided(decidedValue) : new Fail());
        for (int i = 0; i < a; i++) {
            Channel.send(model, acc.get(i), new Done(me));
        }
    }

    // ---- MpProgram ----
    @Override public String programName() { return "fpaxos"; }
    @Override public String size() { return a + "," + k; }
    @Override public CommunicationModel model() { return model; }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < a; i++) roles.add("a" + i);
        for (int i = 0; i < k; i++) roles.add("p" + i);
        for (int i = 0; i < k; i++) roles.add("c" + i);
        return roles;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> roles = new ArrayList<>();
        for (int i = 0; i < k; i++) roles.add("c" + i);
        return roles;
    }

    @Override
    public String toString() {
        return "FPaxos(A=" + a + ", P=K=" + k + ", Q1=" + q1() + ", Q2=" + q2() + ", R=" + R + ", " + modelName + ")";
    }
}
