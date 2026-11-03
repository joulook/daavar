package bench.broadcast_consensus_civl;

import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.summary.Tag;

/** The three message types of the Rust crate; one Rust channel per type is one type guard here. */
final class Msgs {
    private Msgs() {}

    /** {@code Input(v)}: {@code c_i -> oracle} (spec) or {@code c_i -> node_i} (impl). */
    @Tag("Input")
    record Input(int v) implements MpMessage {}

    /** {@code Val(v)}: {@code node_j -> node_i}, the source's {@code send(v, i)}. */
    @Tag("Val")
    record Val(int v) implements MpMessage {}

    /** {@code Decide(d)}: to {@code c_i}. */
    @Tag("Decide")
    record Decide(int d) implements MpMessage {}
}
