package bench.producer_consumer_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.List;

/**
 * N4, CIVL {@code samples/ProducerConsumer.bpl}; the Java twin of {@code rust/src/main.rs}.
 * Visible roles {@code producer}, {@code consumer}; spawn order consumer, producer. The producer
 * sends {@code Item(1..j)} then {@code Item(0)}, {@code j} chosen by one boolean per step; the
 * consumer receives until {@code Item(0)}, at most {@code k + 1} times. {@link Spec} always uses
 * p2p (the source's FIFO queue); {@link Impl} uses the model argument.
 */
abstract class ProducerConsumer implements org.mpi_sws.jmc.mp.launch.MpProgram {

    /** {@code Msg::Item(x)}. */
    @Tag("Item")
    record Item(int x) implements MpMessage {}

    final int k;
    final CommunicationModel m;
    private final String side;

    ProducerConsumer(String side, int k, CommunicationModel m) {
        this.side = side;
        this.k = k;
        this.m = m;
    }

    @Override
    public String programName() {
        return "producer-consumer-civl-" + side;
    }

    @Override
    public String size() {
        return "k" + k;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("consumer", "producer");
    }

    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        MpThread c = ts.add("consumer", this::consumer);
        Tid cid = Tid.of(c.getTaskId());
        ts.add("producer", () -> producer(cid));
        ts.startAndAwaitAll();
    }

    private void producer(Tid c) {
        for (int x = 1; x <= k; x++) {
            if (Nondet.chooseBoolean()) {
                Channel.send(m, c, new Item(x));
            } else {
                Channel.send(m, c, new Item(0));
                return;
            }
        }
        Channel.send(m, c, new Item(0));
    }

    private void consumer() {
        for (int i = 0; i <= k; i++) {
            Item it = Channel.recv(m, Item.class);
            if (it.x() == 0) {
                return;
            }
        }
    }
}
