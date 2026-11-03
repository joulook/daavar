package bench.producer_consumer_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.api.mp.Nondet;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.List;

/**
 * producer-consumer-civl: CIVL's ProducerConsumer.bpl, Java twin of the Rust Must-API program.
 *
 * <p>Arguments: {@code <spec|impl>,<k>,<fifo|bag>[,<variant>]}; no variant exists, so a variant
 * argument, if present, must be empty.
 *
 * <p>Visible roles: {@code consumer}, {@code producer}, spawned in that order by the invisible main.
 * The producer sends {@code Item(1) .. Item(j)} then {@code Item(0)}, choosing to continue by one
 * boolean nondet per step; the consumer blocking-receives until it reads {@code Item(0)}, at most
 * {@code k + 1} receives. spec: every send/receive is P2P (FIFO from the single sender) whatever
 * the run's model; impl: the run's model (fifo = P2P, bag = ASYN).
 */
public final class ProducerConsumerCivl implements MpProgram {

    /** The message type: one record per Rust {@code Msg} variant. */
    public sealed interface Msg extends MpMessage permits Item {}

    /** {@code Item(x)}: {@code x >= 1} is a payload, {@code x == 0} ends the stream. */
    @Tag("Item")
    public record Item(int x) implements Msg {}

    private final boolean spec;
    private final int k;
    private final CommunicationModel runModel;
    private final CommunicationModel model;

    public ProducerConsumerCivl(String[] args) {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException(
                    "expected <spec|impl>,<k>,<fifo|bag>[,<variant>], got " + String.join(",", args));
        }
        switch (args[0]) {
            case "spec" -> spec = true;
            case "impl" -> spec = false;
            default -> throw new IllegalArgumentException("first argument must be spec or impl: " + args[0]);
        }
        k = Integer.parseInt(args[1]);
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0, was " + k);
        }
        runModel = switch (args[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException("model must be fifo or bag: " + args[2]);
        };
        if (args.length == 4 && !args[3].isEmpty()) {
            throw new IllegalArgumentException("this program has no variants: " + args[3]);
        }
        // spec: the channel is FIFO by construction (Rust LocalOrder), whatever the run's model.
        model = spec ? CommunicationModel.P2P : runModel;
    }

    private void producer(Tid consumer) {
        for (int x = 1; x <= k; x++) {
            if (Nondet.chooseBoolean()) {
                Channel.send(model, consumer, new Item(x));
            } else {
                Channel.send(model, consumer, new Item(0));
                return;
            }
        }
        Channel.send(model, consumer, new Item(0));
    }

    private void consumer() {
        for (int i = 0; i <= k; i++) {
            Item m = Channel.recv(model, Item.class);
            if (m.x() == 0) {
                return;
            }
        }
    }

    @Override
    public void run() {
        MpProcess.Handle c = MpProcess.spawn("consumer", this::consumer);
        Tid ct = c.tid();
        MpProcess.Handle p = MpProcess.spawn("producer", () -> producer(ct));
        c.await();
        p.await();
    }

    @Override
    public String programName() {
        return "producer-consumer-civl";
    }

    @Override
    public String size() {
        return Integer.toString(k);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("consumer", "producer");
    }
}
