package bench.kv_snapshot_isolation_tla;

import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.List;

/** The Rust {@code enum Msg}: one record per variant, same tag and field order. */
public sealed interface Msg extends MpMessage
        permits Msg.Txn, Msg.Result, Msg.Begin, Msg.Snap, Msg.Commit, Msg.CRes {

    /** client -> server / transaction process: read {@code rk}, write {@code wk := v}. */
    @Tag("Txn")
    record Txn(Tid from, int rk, int wk, int v) implements Msg {}

    /** -> client: committed, and the value read (0 when aborted). */
    @Tag("Result")
    record Result(boolean ok, int read) implements Msg {}

    /** transaction process -> store: START. */
    @Tag("Begin")
    record Begin(Tid from) implements Msg {}

    /** store -> transaction process: the snapshot. */
    @Tag("Snap")
    record Snap(List<Integer> store) implements Msg {
        public Snap {
            store = List.copyOf(store);
        }
    }

    /** transaction process -> store: COMMIT with the read and write sets. */
    @Tag("Commit")
    record Commit(Tid from, int rk, int wk, int v) implements Msg {}

    /** store -> transaction process: committed (true) or aborted (false). */
    @Tag("CRes")
    record CRes(boolean ok) implements Msg {}
}
