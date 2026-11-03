package bench.two_pc_lbinria;

import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.Tid;

import java.util.ArrayList;
import java.util.List;

/**
 * What this package copies from the H1 ports' {@code H1Support}: the model argument, and
 * construct-first spawning (every {@link MpThread} is constructed, which mints its id, before any
 * is started, so every body can name every thread; main then starts them in spawn order and awaits
 * them all last, performing no communication itself).
 */
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

    /** Threads constructed in spawn order, started together, awaited last. */
    static final class Procs {
        private final List<MpThread> all = new ArrayList<>();

        /**
         * Constructs (does not start) a thread with a role name.
         *
         * @param role the role
         * @param body the body
         * @return its id
         */
        Tid add(String role, Runnable body) {
            MpThread t = new MpThread(body, role);
            all.add(t);
            return Tid.of(t.getTaskId());
        }

        /** Starts every thread in spawn order, then awaits every one. */
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
