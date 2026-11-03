package bench.replicated_log_esop20;

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import org.mpi_sws.jmc.mp.launch.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Aneris's replicated log over two-phase commit (ESOP 2020, {@code rep_log.v} + {@code tpc.v};
 * candidate N22): the Java twin of {@code rust/src/main.rs} (same threads, spawn order, messages;
 * Rust's per-thread inbox channels with tag-selected receives are thread mailboxes with type-test
 * receives here, the ids minted before any thread starts).
 *
 * <p>Arguments: {@code <spec|impl>,<c>,<fifo|bag>,<variant>}; {@code c} clients (1..6).
 * Variants: {@code correct}, {@code early-reply}. Visible: {@code client1 .. client<c>},
 * {@code auditor}.
 */
public final class RepLog implements MpProgram {

    /** {@code "COMMIT"} or {@code "ABORT"}. */
    public sealed interface Verdict permits Commit, Abort {}

    /** {@code "COMMIT"}. */
    @Tag("Commit")
    public record Commit() implements Verdict {}

    /** {@code "ABORT"}. */
    @Tag("Abort")
    public record Abort() implements Verdict {}

    /** Every message (the Rust {@code Msg} enum). */
    public sealed interface Msg extends MpMessage
            permits Event, Result, Request, Vote, Dec, Ack, Read, Log, Done {}

    /** client -> logger. */
    @Tag("Event")
    public record Event(int from, String ev) implements Msg {}

    /** logger -> client. */
    @Tag("Result")
    public record Result(Verdict v) implements Msg {}

    /** logger -> db. */
    @Tag("Request")
    public record Request(String ev) implements Msg {}

    /** db -> logger. */
    @Tag("Vote")
    public record Vote(Verdict v) implements Msg {}

    /** logger -> db. */
    @Tag("Dec")
    public record Dec(Verdict v) implements Msg {}

    /** db -> logger. */
    @Tag("Ack")
    public record Ack() implements Msg {}

    /** auditor -> db (spec: log). */
    @Tag("Read")
    public record Read() implements Msg {}

    /** db (spec: log) -> auditor. */
    @Tag("Log")
    public record Log(String log) implements Msg {}

    /** client -> auditor. */
    @Tag("Done")
    public record Done() implements Msg {}

    private static final String[] NAMES = {"X", "Y", "Z", "W", "V", "U"};
    private static final int COORD = 0;
    private static final int AUDITOR = 3;

    private final boolean impl;
    private final int c;
    private final CommunicationModel m;
    private final boolean earlyReply;

    /**
     * @param args side, c, model, variant
     */
    public RepLog(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("RepLog takes <spec|impl>,<c>[,<fifo|bag>[,<variant>]]");
        }
        this.impl = switch (args[0].trim()) {
            case "spec" -> false;
            case "impl" -> true;
            default -> throw new IllegalArgumentException("side must be spec or impl");
        };
        this.c = Integer.parseInt(args[1].trim());
        if (c < 1 || c > 6) {
            throw new IllegalArgumentException("c must be in 1..6");
        }
        this.m = Support.model(args.length > 2 ? args[2] : "bag");
        String v = args.length > 3 ? args[3].trim() : "correct";
        this.earlyReply = switch (v) {
            case "correct" -> false;
            case "early-reply" -> true;
            default -> throw new IllegalArgumentException("unknown variant " + v);
        };
    }

    @Override
    public String programName() {
        return "replicated-log-esop20";
    }

    @Override
    public String size() {
        return "c" + c;
    }

    @Override
    public CommunicationModel model() {
        return m;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        List<String> r = new ArrayList<>();
        if (impl) {
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
        List<String> r = new ArrayList<>();
        for (int i = 1; i <= c; i++) {
            r.add("client" + i);
        }
        r.add("auditor");
        return r;
    }

    private static String event(int i) {
        return "EVENT " + NAMES[i - 1];
    }

    /** Mailboxes, as the Rust inboxes: 0 coordinator (spec: log), 1 db1, 2 db2, 3 auditor, 3 + i client i. */
    @Override
    public void run() {
        Support.Threads ts = new Support.Threads();
        Tid[] t = new Tid[4 + c];
        if (impl) {
            t[COORD] = Tid.of(ts.add("coordinator", () -> coordinator(t)).getTaskId());
            t[1] = Tid.of(ts.add("db1", () -> db(t)).getTaskId());
            t[2] = Tid.of(ts.add("db2", () -> db(t)).getTaskId());
        } else {
            t[COORD] = Tid.of(ts.add("log", () -> specLog(t)).getTaskId());
        }
        t[AUDITOR] = Tid.of(ts.add("auditor", () -> auditor(t)).getTaskId());
        for (int i = 1; i <= c; i++) {
            final int ii = i;
            t[AUDITOR + i] = Tid.of(ts.add("client" + i, () -> client(ii, t)).getTaskId());
        }
        ts.startAndAwaitAll();
    }

    private void client(int i, Tid[] t) {
        Channel.send(m, t[COORD], new Event(i, event(i)));
        Channel.recv(m, Result.class);
        Channel.send(m, t[AUDITOR], new Done());
    }

    private void auditor(Tid[] t) {
        for (int k = 0; k < c; k++) {
            Channel.recv(m, Done.class);
        }
        Tid[] dbs = impl ? new Tid[] {t[1], t[2]} : new Tid[] {t[COORD], t[COORD]};
        for (Tid d : dbs) {
            Channel.send(m, d, new Read());
        }
        for (int k = 0; k < 2; k++) {
            Channel.recv(m, Log.class);
        }
    }

    private void specLog(Tid[] t) {
        StringBuilder log = new StringBuilder();
        for (int k = 0; k < c + 2; k++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Event || x instanceof Read);
            if (q instanceof Event e) {
                log.append(e.ev());
                Channel.send(m, t[AUDITOR + e.from()], new Result(new Commit()));
            } else {
                Channel.send(m, t[AUDITOR], new Log(log.toString()));
            }
        }
    }

    private void coordinator(Tid[] t) {
        for (int k = 0; k < c; k++) {
            Event e = Channel.recv(m, Event.class);
            for (int d = 1; d <= 2; d++) {
                Channel.send(m, t[d], new Request(e.ev()));
            }
            boolean allCommit = true;
            for (int j = 0; j < 2; j++) {
                MpMessage q = Channel.recv(m, x -> x instanceof Vote || x instanceof Ack);
                if (q instanceof Vote v && v.v() instanceof Abort) {
                    allCommit = false;
                }
            }
            Verdict result = allCommit ? new Commit() : new Abort();
            for (int d = 1; d <= 2; d++) {
                Channel.send(m, t[d], new Dec(result));
            }
            if (earlyReply) {
                Channel.send(m, t[AUDITOR + e.from()], new Result(result));
            }
            for (int j = 0; j < 2; j++) {
                Channel.recv(m, x -> x instanceof Vote || x instanceof Ack);
            }
            if (!earlyReply) {
                Channel.send(m, t[AUDITOR + e.from()], new Result(result));
            }
        }
    }

    private void db(Tid[] t) {
        String waiting = "";
        StringBuilder log = new StringBuilder();
        for (int k = 0; k < 2 * c + 1; k++) {
            MpMessage q = Channel.recv(m, x -> x instanceof Request || x instanceof Dec || x instanceof Read);
            if (q instanceof Request r) {
                waiting = r.ev();
                Channel.send(m, t[COORD], new Vote(new Commit()));
            } else if (q instanceof Dec d) {
                if (d.v() instanceof Commit) {
                    log.append(waiting);
                }
                Channel.send(m, t[COORD], new Ack());
            } else {
                Channel.send(m, t[AUDITOR], new Log(log.toString()));
            }
        }
    }
}
