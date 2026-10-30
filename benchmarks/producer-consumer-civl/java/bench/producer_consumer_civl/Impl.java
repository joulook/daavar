package bench.producer_consumer_civl;

/** The implementation: the same pair over the run's model. Arguments {@code k,model} ({@code fifo|bag}). */
public final class Impl extends ProducerConsumer {
    public Impl(String... args) {
        super("impl", Integer.parseInt(args[0].trim()), Support.model(args.length > 1 ? args[1] : "fifo"));
    }
}
