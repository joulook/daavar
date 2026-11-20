package bench.omnipaxos_derived_sol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

/** A finite-budget election followed by serial, multi-slot Paxos. */
public final class OmniPaxosDerivedSol implements MpProgram {
    @Tag("Propose") public record Propose(int value) implements MpMessage {}
    @Tag("Committed") public record Committed(int value) implements MpMessage {}
    @Tag("Offer") public record Offer(int value) implements MpMessage {}
    @Tag("AcceptedSlot") public record AcceptedSlot(int index, long ballot, int value) {}
    @Tag("Prepare") public record Prepare(long ballot, Tid candidate) implements MpMessage {}
    @Tag("Promise") public record Promise(long ballot, Tid voter, List<AcceptedSlot> slots)
            implements MpMessage {
        public Promise { slots = List.copyOf(slots); }
    }
    @Tag("Elected") public record Elected(long ballot, Tid leader) implements MpMessage {}
    @Tag("Accept") public record Accept(long ballot, Tid leader, int index, int value)
            implements MpMessage {}
    @Tag("Accepted") public record Accepted(long ballot, Tid voter, int index, int value)
            implements MpMessage {}
    @Tag("Nack") public record Nack(long rejected, long promised) implements MpMessage {}
    @Tag("Decide") public record Decide(List<Integer> prefix) implements MpMessage {
        public Decide { prefix = List.copyOf(prefix); }
    }

    private final int e, t, cand;
    private final CommunicationModel communication;

    public OmniPaxosDerivedSol(String... args) {
        if (args.length != 4 && args.length != 5)
            throw new IllegalArgumentException("expected impl,e,t[,cand],fifo|bag");
        if (!args[0].equals("impl")) throw new IllegalArgumentException("expected impl");
        e = Integer.parseInt(args[1]);
        t = Integer.parseInt(args[2]);
        cand = args.length == 5 ? Integer.parseInt(args[3]) : 3;
        if (e < 1 || e > Integer.MAX_VALUE / 2 || t < 1 || cand < 0 || cand > 3)
            throw new IllegalArgumentException("require 1 <= e <= MAX_INT/2, t >= 1, 0 <= cand <= 3");
        communication = switch (args[args.length - 1]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("expected fifo or bag");
        };
    }

    @Override public String programName() { return "omnipaxos"; }
    @Override public String size() { return "e=" + e + ",t=" + t + ",cand=" + cand; }
    @Override public CommunicationModel model() { return communication; }
    @Override public List<String> rolesInSpawnOrder() {
        return List.of("n0", "n1", "n2", "c0", "c1");
    }
    @Override public List<String> visibleRoles() { return List.of("c0", "c1"); }
    @Override public String toString() { return "OmniPaxosDerivedSol(" + size() + "," + model() + ")"; }

    @Override public void run() {
        // Bootstrap only: publish all addresses before starting any process. The list
        // is thereafter read-only; no protocol state is shared between processes.
        List<Tid> addresses = new ArrayList<>();
        List<MpThread> threads = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            final int id = i;
            threads.add(new MpThread(() -> new Node(id, List.copyOf(addresses)).run(), "n" + i));
        }
        for (int i = 0; i < 2; i++) {
            final int id = i;
            threads.add(new MpThread(() -> client(id, List.copyOf(addresses)), "c" + i));
        }
        for (int i = 0; i < threads.size(); i++)
            addresses.add(Tid.of(threads.get(i).getTaskId(), rolesInSpawnOrder().get(i)));
        for (MpThread thread : threads) thread.start();
        for (MpThread thread : threads) {
            try { thread.join1(); }
            catch (InterruptedException ex) { throw new IllegalStateException(ex); }
        }
    }

    private void send(Tid destination, MpMessage message) {
        Channel.send(communication, destination, message);
    }

    private void client(int id, List<Tid> addresses) {
        for (int j = 0; j < e; j++) send(addresses.get(id), new Propose(1 + id + 2 * j));
        int own = 0;
        int next = 1;
        while (own < e) {
            final int expected = next;
            Committed message = Channel.recv(communication, Committed.class,
                    m -> m.value() == expected);
            if ((message.value() - 1) % 2 == id) own++;
            next++;
        }
    }

    private final class Node {
        private final int id;
        private final List<Tid> addresses;
        private final Tid self;
        private int budget;
        private long promised, maxSeen;
        private long campaign = -1;
        private Tid leader;
        private boolean active;
        private final TreeMap<Integer, AcceptedSlot> accepted = new TreeMap<>();
        private final Map<Tid, List<AcceptedSlot>> promises = new HashMap<>();
        private final Set<Integer> offers = new HashSet<>();
        private final List<Integer> recovered = new ArrayList<>();
        private final List<Integer> chosen = new ArrayList<>();
        private final List<Integer> learned = new ArrayList<>();
        private final Set<Tid> votes = new HashSet<>();
        private int inFlight = -1;
        private int inFlightValue;

        Node(int id, List<Tid> addresses) {
            this.id = id;
            this.addresses = addresses;
            self = addresses.get(id);
            budget = id < cand ? t : 0;
        }

        void broadcast(MpMessage message) {
            for (int i = 0; i < 3; i++) send(addresses.get(i), message);
        }

        void run() {
            for (;;) {
                if (leader == null && budget > 0 && Nondet.chooseBoolean()) {
                    elect();
                } else {
                    handle(Channel.recv(communication));
                    pump();
                }
            }
        }

        void stopCampaign() {
            campaign = -1;
            active = false;
            inFlight = -1;
            votes.clear();
            promises.clear();
        }

        void elect() {
            budget--;
            stopCampaign();
            campaign = (maxSeen / 3 + 1) * 3 + id;
            promised = maxSeen = campaign;
            broadcast(new Prepare(campaign, self));
        }

        void observe(long ballot) {
            maxSeen = Math.max(maxSeen, ballot);
            if (ballot > promised) {
                promised = ballot;
                leader = null;
                stopCampaign();
            }
        }

        void handle(MpMessage message) {
            if (message instanceof Propose m) {
                check(id < 2 && (m.value() - 1) % 2 == id, "wrong proposal gateway");
                check(m.value() > 0 && m.value() <= 2 * e, "invalid proposal");
                offers.add(m.value());
                for (int i = 0; i < 3; i++)
                    if (i != id) send(addresses.get(i), new Offer(m.value()));
            } else if (message instanceof Offer m) {
                offers.add(m.value());
            } else if (message instanceof Prepare m) {
                if (m.ballot() < promised) {
                    send(m.candidate(), new Nack(m.ballot(), promised));
                } else {
                    observe(m.ballot());
                    send(m.candidate(), new Promise(m.ballot(), self,
                            List.copyOf(accepted.values())));
                }
            } else if (message instanceof Promise m) {
                if (m.ballot() != campaign || active) return;
                check(addresses.subList(0, 3).contains(m.voter()), "unknown voter");
                promises.putIfAbsent(m.voter(), m.slots());
                if (promises.size() >= 2) {
                    TreeMap<Integer, AcceptedSlot> merged = new TreeMap<>();
                    for (List<AcceptedSlot> snapshot : promises.values()) {
                        for (AcceptedSlot slot : snapshot) {
                            AcceptedSlot old = merged.get(slot.index());
                            if (old == null || slot.ballot() > old.ballot())
                                merged.put(slot.index(), slot);
                            else if (slot.ballot() == old.ballot())
                                check(slot.value() == old.value(), "same-ballot conflict");
                        }
                    }
                    recovered.clear();
                    for (AcceptedSlot slot : merged.values()) {
                        check(slot.index() == recovered.size(), "recovery hole");
                        recovered.add(slot.value());
                    }
                    chosen.clear();
                    active = true;
                    leader = self;
                    broadcast(new Elected(campaign, self));
                }
            } else if (message instanceof Elected m) {
                if (m.ballot() >= promised) {
                    observe(m.ballot());
                    leader = m.leader();
                }
            } else if (message instanceof Accept m) {
                if (m.ballot() < promised) {
                    send(m.leader(), new Nack(m.ballot(), promised));
                } else {
                    observe(m.ballot());
                    leader = m.leader();
                    check(m.index() >= 0 && m.value() == m.index() + 1, "invalid log order");
                    AcceptedSlot old = accepted.get(m.index());
                    check(old == null || old.ballot() != m.ballot() || old.value() == m.value(),
                            "acceptor equivocation");
                    accepted.put(m.index(), new AcceptedSlot(m.index(), m.ballot(), m.value()));
                    send(m.leader(), new Accepted(m.ballot(), self, m.index(), m.value()));
                }
            } else if (message instanceof Accepted m) {
                if (!active || m.ballot() != campaign || m.index() != inFlight
                        || m.value() != inFlightValue) return;
                check(addresses.subList(0, 3).contains(m.voter()), "unknown acceptor");
                votes.add(m.voter());
                if (votes.size() >= 2) {
                    check(inFlight == chosen.size(), "noncontiguous choice");
                    chosen.add(inFlightValue);
                    inFlight = -1;
                    votes.clear();
                    broadcast(new Decide(chosen));
                }
            } else if (message instanceof Nack m) {
                maxSeen = Math.max(maxSeen, m.promised());
                if (m.rejected() == campaign && m.promised() > campaign)
                    observe(m.promised());
            } else if (message instanceof Decide m) {
                for (int i = 0; i < Math.min(learned.size(), m.prefix().size()); i++)
                    check(learned.get(i).equals(m.prefix().get(i)), "conflicting decisions");
                while (learned.size() < m.prefix().size()) {
                    int value = m.prefix().get(learned.size());
                    check(value == learned.size() + 1, "invalid decided sequence");
                    learned.add(value);
                    if (id < 2) send(addresses.get(3 + id), new Committed(value));
                }
            } else {
                throw new AssertionError("unexpected node message: " + message);
            }
        }

        void pump() {
            if (!active || inFlight >= 0) return;
            int index = chosen.size();
            if (index >= recovered.size() && !offers.contains(index + 1)) return;
            int value = index < recovered.size() ? recovered.get(index) : index + 1;
            check(value == index + 1, "recovered order mismatch");
            inFlight = index;
            inFlightValue = value;
            votes.clear();
            broadcast(new Accept(campaign, self, index, value));
        }
    }

    private static void check(boolean condition, String reason) {
        if (!condition) throw new AssertionError(reason);
    }
}
