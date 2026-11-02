package bench.replicated_log_esop20;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpThread;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * Java twin of the Rust program replicated-log-esop20 (Aneris replicated log over two-phase
 * commit). Arguments: {@code <spec|impl>,<c>,<fifo|bag>[,<correct|early-reply>]}, 1 <= c <= 6.
 */
public final class ReplicatedLog implements MpProgram {

    // ---- Verdict (Rust enum Verdict) ----
    public sealed interface Verdict extends MpMessage permits Commit, Abort {}

    @Tag("Commit")
    public record Commit() implements Verdict {}

    @Tag("Abort")
    public record Abort() implements Verdict {}

    // ---- Msg (Rust enum Msg) ----
    public sealed interface Msg extends MpMessage
            permits Event, Result, Request, Vote, Dec, Ack, Read, Log, Done {}

    @Tag("Event")
    public record Event(int from, String ev) implements Msg {}

    @Tag("Result")
    public record Result(Verdict v) implements Msg {}

    @Tag("Request")
    public record Request(String ev) implements Msg {}

    @Tag("Vote")
    public record Vote(Verdict v) implements Msg {}

    @Tag("Dec")
    public record Dec(Verdict v) implements Msg {}

    @Tag("Ack")
    public record Ack() implements Msg {}

    @Tag("Read")
    public record Read() implements Msg {}

    @Tag("Log")
    public record Log(String log) implements Msg {}

    @Tag("Done")
    public record Done() implements Msg {}

    private static final String PROGRAM = "replicated-log-esop20";
    private static final String USAGE =
            "`<spec|impl>,<c>,<fifo|bag>[,correct|early-reply]`, 1 <= c <= 6";

    // Inboxes: 0 coordinator (spec: log), 1 db1, 2 db2, 3 auditor, 3 + i client i.
    private static final int COORD = 0;
    private static final int AUDITOR = 3;

    private final boolean imp;
    private final int c;
    private final CommunicationModel model;
    private final boolean earlyReply;

    public ReplicatedLog(String... a) {
        if (a.length < 3 || a.length > 4) {
            throw new IllegalArgumentException(PROGRAM + ": args are " + USAGE + ", got " + Arrays.toString(a));
        }
        switch (a[0]) {
            case "spec" -> imp = false;
            case "impl" -> imp = true;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown side " + a[0]);
        }
        try {
            c = Integer.parseInt(a[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(PROGRAM + ": <c> must be a number; args are " + USAGE);
        }
        if (c < 1 || c > 6) {
            throw new IllegalArgumentException(PROGRAM + ": c must be in 1..=6; args are " + USAGE);
        }
        model = switch (a[2]) {
            case "fifo" -> CommunicationModel.P2P;
            case "bag" -> CommunicationModel.ASYN;
            default -> throw new IllegalArgumentException(PROGRAM + ": unknown model " + a[2]);
        };
        String variant = a.length == 4 ? a[3] : "correct";
        earlyReply = switch (variant) {
            case "correct" -> false;
            case "early-reply" -> true;
            default -> throw new IllegalArgumentException(
                    PROGRAM + ": unknown variant `" + variant + "`; args are " + USAGE);
        };
    }

    @Override
    public String programName() {
        return PROGRAM;
    }

    @Override
    public String size() {
        return Integer.toString(c);
    }

    @Override
    public CommunicationModel model() {
        return model;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (imp) {
            r.add("coordinator");
            r.add("db1");
            r.add("db2");
        } else {
            r.add("log");
        }
        r.add("auditor");
        for (int i = 1; i <= c; i++) {
            r.add("client" + i);
        }
        return r;
    }

    @Override
    public List<String> visibleRoles() {
        List<String> v = new ArrayList<>();
        for (int i = 1; i <= c; i++) {
            v.add("client" + i);
        }
        v.add("auditor");
        return v;
    }

    // ---- helpers ----

    private void send(Tid to, Msg m) {
        Channel.send(model, to, m);
    }

    @SafeVarargs
    private Msg recv(Class<? extends Msg>... types) {
        Predicate<MpMessage> guard = m -> {
            for (Class<? extends Msg> t : types) {
                if (t.isInstance(m)) {
                    return true;
                }
            }
            return false;
        };
        return (Msg) Channel.recv(model, guard);
    }

    private static String event(int i) {
        String[] names = {"X", "Y", "Z", "W", "V", "U"};
        return "EVENT " + names[i - 1];
    }

    // ---- threads ----

    /** {@code client} (one event), then {@code Done} to the auditor. */
    private void client(int i, Tid[] t) {
        send(t[COORD], new Event(i, event(i)));
        recv(Result.class);
        send(t[AUDITOR], new Done());
    }

    /** The added reader: after every client is answered, read both replicas' logs. */
    private void auditor(Tid[] dbs) {
        for (int k = 0; k < c; k++) {
            recv(Done.class);
        }
        for (Tid d : dbs) {
            send(d, new Read());
        }
        for (int k = 0; k < 2; k++) {
            recv(Log.class);
        }
    }

    /** The specification: one atomic log. */
    private void specLog(Tid[] t) {
        StringBuilder log = new StringBuilder();
        for (int k = 0; k < c + 2; k++) {
            Msg m = recv(Event.class, Read.class);
            if (m instanceof Event e) {
                log.append(e.ev());
                send(t[AUDITOR + e.from()], new Result(new Commit()));
            } else if (m instanceof Read) {
                send(t[AUDITOR], new Log(log.toString()));
            } else {
                throw new IllegalStateException("unreachable");
            }
        }
    }

    /** {@code logger}: for each client event, two-phase commit over db1, db2, then the reply. */
    private void coordinator(Tid[] t) {
        for (int k = 0; k < c; k++) {
            Event e = (Event) recv(Event.class);
            for (int d = 1; d <= 2; d++) {
                send(t[d], new Request(e.ev()));
            }
            boolean allCommit = true;
            for (int j = 0; j < 2; j++) {
                Msg m = recv(Vote.class, Ack.class);
                if (m instanceof Vote v && v.v() instanceof Abort) {
                    allCommit = false;
                }
            }
            Verdict result = allCommit ? new Commit() : new Abort();
            for (int d = 1; d <= 2; d++) {
                send(t[d], new Dec(result));
            }
            if (earlyReply) {
                send(t[AUDITOR + e.from()], new Result(result));
            }
            for (int j = 0; j < 2; j++) {
                recv(Vote.class, Ack.class);
            }
            if (!earlyReply) {
                send(t[AUDITOR + e.from()], new Result(result));
            }
        }
    }

    /** {@code db}: 2c + 1 receives on its one socket. */
    private void db(Tid[] t) {
        String waiting = "";
        StringBuilder log = new StringBuilder();
        for (int k = 0; k < 2 * c + 1; k++) {
            Msg m = recv(Request.class, Dec.class, Read.class);
            if (m instanceof Request r) {
                waiting = r.ev();
                send(t[COORD], new Vote(new Commit()));
            } else if (m instanceof Dec d) {
                if (d.v() instanceof Commit) {
                    log.append(waiting);
                }
                send(t[COORD], new Ack());
            } else if (m instanceof Read) {
                send(t[AUDITOR], new Log(log.toString()));
            } else {
                throw new IllegalStateException("unreachable");
            }
        }
    }

    /**
     * Main: creates every process (minting its task id) in the Rust spawn order, so that every
     * process can address every other one as the Rust program's pre-built channels allow, then
     * starts them in that order and awaits all of them. Main performs no message-passing event.
     */
    @Override
    public void run() {
        // Inbox index -> Tid; filled before any process starts.
        Tid[] t = new Tid[4 + c];
        List<MpThread> threads = new ArrayList<>();
        if (imp) {
            MpThread co = new MpThread(() -> coordinator(t), "coordinator");
            t[COORD] = Tid.of(co.getTaskId(), "coordinator");
            threads.add(co);
            for (int d = 1; d <= 2; d++) {
                MpThread th = new MpThread(() -> db(t), "db" + d);
                t[d] = Tid.of(th.getTaskId(), "db" + d);
                threads.add(th);
            }
        } else {
            MpThread lg = new MpThread(() -> specLog(t), "log");
            t[COORD] = Tid.of(lg.getTaskId(), "log");
            threads.add(lg);
        }
        Tid[] dbs = imp ? new Tid[] {t[1], t[2]} : new Tid[] {t[COORD], t[COORD]};
        MpThread au = new MpThread(() -> auditor(dbs), "auditor");
        t[AUDITOR] = Tid.of(au.getTaskId(), "auditor");
        threads.add(au);
        for (int i = 1; i <= c; i++) {
            final int ci = i;
            MpThread cl = new MpThread(() -> client(ci, t), "client" + i);
            t[AUDITOR + i] = Tid.of(cl.getTaskId(), "client" + i);
            threads.add(cl);
        }
        for (MpThread th : threads) {
            th.start();
        }
        for (MpThread th : threads) {
            try {
                th.join1();
            } catch (InterruptedException e) {
                throw new IllegalStateException("interrupted while joining " + th, e);
            }
        }
    }

    @Override
    public String toString() {
        return PROGRAM + "(" + (imp ? "impl" : "spec") + ", c=" + c + ", " + model
                + (imp ? ", " + (earlyReply ? "early-reply" : "correct") : "") + ")";
    }
}
