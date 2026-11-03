package bench.producer_consumer_civl;

import org.mpi_sws.jmc.mp.events.CommunicationModel;

/** The specification: the producer-consumer pair over a FIFO channel. Arguments {@code k[,model]}; the model is ignored (always p2p). */
public final class Spec extends ProducerConsumer {
    public Spec(String... args) {
        super("spec", Integer.parseInt(args[0].trim()), CommunicationModel.P2P);
    }
}
