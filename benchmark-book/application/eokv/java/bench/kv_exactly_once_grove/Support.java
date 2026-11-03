package bench.kv_exactly_once_grove;

import org.mpi_sws.jmc.mp.events.CommunicationModel;

/** What this package copies from the H1 ports' {@code H1Support}: the model argument. */
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
}
