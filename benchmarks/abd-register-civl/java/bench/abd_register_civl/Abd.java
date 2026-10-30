package bench.abd_register_civl;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.summary.Tag;

/**
 * Shared parts of the ABD pair (twin of {@code rust/src/main.rs}): the messages and the three
 * visible clients. One Rust channel per message type of a receiver is one type guard here.
 */
final class Abd {
    private Abd() {}

    /** Client to register (spec) or to its proxy (impl): {@code Op::Write(v)} / {@code Op::Read(i)}. */
    sealed interface Op extends MpMessage permits Write, Read {}

    @Tag("Write")
    record Write(int v) implements Op {}

    @Tag("Read")
    record Read(int i) implements Op {}

    /** Register or proxy to client: {@code Reply::WriteAck} / {@code Reply::ReadReply(v)}. */
    sealed interface Reply extends MpMessage permits WriteAck, ReadReply {}

    @Tag("WriteAck")
    record WriteAck() implements Reply {}

    @Tag("ReadReply")
    record ReadReply(int v) implements Reply {}

    /** {@code r1 -> r2}: r1's read has returned. */
    @Tag("Done")
    record Done() implements MpMessage {}

    /** Proxy to replica; {@code p} is the proxy (0 = wp, 1 = rp1, 2 = rp2), ts = (t, pid). */
    sealed interface ToReplica extends MpMessage permits Query, Update {}

    @Tag("Query")
    record Query(int p) implements ToReplica {}

    @Tag("Update")
    record Update(int p, int t, int pid, int v) implements ToReplica {}

    /** Replica to proxy: the {@code Query#0} result. */
    @Tag("QueryReply")
    record QueryReply(int t, int pid, int v) implements MpMessage {}

    /** Replica to proxy: {@code Update#0} done. */
    @Tag("UpdateAck")
    record UpdateAck() implements MpMessage {}

    /** The source's {@code lt} on {@code TimeStamp(t, pid)}. */
    static boolean lt(int t1, int p1, int t2, int p2) {
        return t1 < t2 || (t1 == t2 && p1 < p2);
    }

    /** {@code w}: write 1, await the ack. */
    static void w(CommunicationModel m, Tid front) {
        Channel.send(m, front, new Write(1));
        Channel.recv(m, Reply.class);
    }

    /** {@code r1}: read, then tell r2. */
    static void r1(CommunicationModel m, Tid front, Tid[] r2) {
        Channel.send(m, front, new Read(1));
        Channel.recv(m, Reply.class);
        Channel.send(m, r2[0], new Done());
    }

    /** {@code r2}: wait for r1, then read. */
    static void r2(CommunicationModel m, Tid front) {
        Channel.recv(m, Done.class);
        Channel.send(m, front, new Read(2));
        Channel.recv(m, Reply.class);
    }
}
