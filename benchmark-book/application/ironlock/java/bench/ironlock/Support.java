package bench.ironlock;

import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;

import java.util.ArrayList;
import java.util.List;

/** What this benchmark needs from {@code h1.H1Support}, copied: the model argument and a joiner. */
final class Support {

    private Support() {}

    /**
     * {@code fifo} (TraceForge {@code ConsType::FIFO}, JMC p2p) or {@code bag}
     * ({@code ConsType::Bag}, JMC asyn).
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

    /** Threads constructed first (ids minted), started in order, awaited by main at its end. */
    static final class Threads {
        private final List<MpThread> all = new ArrayList<>();

        MpThread add(String role, Runnable body) {
            MpThread t = new MpThread(body, role);
            all.add(t);
            return t;
        }

        void startAndAwaitAll() {
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
