package bench.ironlock;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.summary.Tag;

/** Shared parts of the IronLock programs: messages, K, the visible observer. */
final class Lock {
    private Lock() {}

    /** K = 2 transfers, as in the Rust crate. */
    static final int K = 2;

    /** What goes node to node (Rust {@code enum NodeMsg}). */
    interface NodeMsg extends MpMessage {}

    /** {@code LockMessage::Transfer(transfer_epoch)}, node to node. */
    @Tag("Transfer")
    record Transfer(int e) implements NodeMsg {}

    /** Ours: some node holds epoch K+1, so the bounded run is over. */
    @Tag("Stop")
    record Stop() implements NodeMsg {}

    /** What goes on the wire out of node i to {@code obs_i} (Rust {@code enum Wire}). */
    interface Wire extends MpMessage {}

    /** {@code LockMessage::Locked(locked_epoch)}, node i to {@code obs_i}. */
    @Tag("Locked")
    record Locked(int e) implements Wire {}

    /** Ours: node i has stopped. */
    @Tag("End")
    record End() implements Wire {}

    /** The visible {@code obs_i}, identical on every side: at most K+2 receives, stop at End. */
    static void observer(CommunicationModel m) {
        for (int j = 0; j < K + 2; j++) {
            if (Channel.recv(m, Wire.class) instanceof End) {
                return;
            }
        }
    }
}
