package bench.paxos_oopsla25;

import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;

import java.util.ArrayList;
import java.util.List;

/** What this package copies from the H1 ports' {@code H1Support}: the model argument, spawning. */
final class Support {

    private Support() {}

    /**
     * Parses the model argument: {@code fifo} (TraceForge {@code ConsType::FIFO}, JMC p2p) or
     * {@code bag} ({@code ConsType::Bag}, JMC asyn).
     *
     * @param s the argument
     * @return the model
     */
    static CommunicationModel model(String s) {
        switch (s.trim()) {
            case "fifo":
                return CommunicationModel.P2P;
            case "bag":
                return CommunicationModel.ASYN;
            default:
                throw new IllegalArgumentException("model must be fifo or bag, was " + s);
        }
    }

    /**
     * Every thread is constructed first (its id minted, so that an earlier thread can address a
     * later one, as Rust's named channels do), then all are started in construction order, and
     * main awaits them all last.
     */
    static final class Procs {
        private final List<MpThread> all = new ArrayList<>();

        /**
         * Constructs a thread with a role name, without starting it.
         *
         * @param role the role name
         * @param body the thread body
         * @return the thread's id
         */
        Tid add(String role, Runnable body) {
            MpThread t = new MpThread(body, role);
            all.add(t);
            return Tid.of(t.getTaskId());
        }

        /** Starts every thread in construction order, then awaits every one. */
        void startAndAwait() {
            for (MpThread t : all) {
                t.start();
            }
            try {
                for (MpThread t : all) {
                    t.join1();
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
