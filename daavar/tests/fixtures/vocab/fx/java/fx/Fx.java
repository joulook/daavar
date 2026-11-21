package fx;

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
 * Driver fixture program (daavar/daavar/tests/fixtures), the Java twin of tf_fx: an echo whose
 * arguments {@code <spec|impl>,<mode>} plant each process outcome of DRIVER-DESIGN §4.1.
 *
 * <p>Modes: {@code ok}; {@code bug} (impl only: the server may answer n+1); {@code assert-spawned}
 * (a failed {@code assert} in the server); {@code assert-main} (a failed {@code assert} in main
 * after its awaits); {@code mainsend} (main sends: a shape error); {@code novisible} (a visible role
 * that is not spawned); {@code float} (a payload the encoder refuses); {@code sleep} (the server
 * sleeps an hour: a driver timeout). Anything else: {@code IllegalArgumentException} from the
 * constructor (the usage convention, exit 2).
 */
public final class Fx implements MpProgram {

    @Tag("Ping")
    public record Ping(int n) implements MpMessage {}

    @Tag("Pong")
    public record Pong(int n) implements MpMessage {}

    @Tag("Shutdown")
    public record Shutdown() implements MpMessage {}

    /** Not part of the vocabulary: the payload of mode {@code float}. */
    public record Bad(double x) implements MpMessage {}

    private static final CommunicationModel M = CommunicationModel.P2P;
    private static final List<String> MODES =
            List.of("ok", "bug", "assert-spawned", "assert-main", "mainsend", "novisible", "float", "sleep");

    private final boolean impl;
    private final String mode;

    public Fx(String... args) {
        if (args.length != 2 || !(args[0].equals("spec") || args[0].equals("impl")) || !MODES.contains(args[1])) {
            throw new IllegalArgumentException("usage: fx.Fx:<spec|impl>,<" + String.join("|", MODES) + ">");
        }
        this.impl = args[0].equals("impl");
        this.mode = args[1];
    }

    @Override
    public String programName() {
        return "fx";
    }

    @Override
    public String size() {
        return (impl ? "impl," : "spec,") + mode;
    }

    @Override
    public CommunicationModel model() {
        return M;
    }

    @Override
    public List<String> rolesInSpawnOrder() {
        return List.of("server", "client");
    }

    @Override
    public List<String> visibleRoles() {
        return mode.equals("novisible") ? List.of("client", "nobody") : List.of("client", "server");
    }

    @Override
    public void run() {
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending client = MpProcess.create("client");
        Tid s = server.tid();
        Tid c = client.tid();
        List<MpProcess.Handle> hs = List.of(server.start(() -> server(c)), client.start(() -> client(s)));
        if (mode.equals("mainsend")) {
            Channel.send(M, s, new Ping(0));
        }
        for (MpProcess.Handle h : hs) {
            h.await();
        }
        if (mode.equals("assert-main")) {
            assert false : "fx: planted assert in main";
        }
    }

    private void server(Tid client) {
        while (true) {
            MpMessage m = Channel.recv(M);
            if (m instanceof Ping p) {
                if (mode.equals("assert-spawned")) {
                    assert p.n() == 0 : "fx: planted assert in the server";
                }
                if (mode.equals("sleep")) {
                    try {
                        Thread.sleep(3_600_000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (mode.equals("float")) {
                    Channel.send(M, client, new Bad(0.5));
                    continue;
                }
                int n = impl && mode.equals("bug") && Nondet.chooseBoolean() ? p.n() + 1 : p.n();
                Channel.send(M, client, new Pong(n));
            } else if (m instanceof Shutdown) {
                return;
            }
        }
    }

    private static void client(Tid server) {
        Channel.send(M, server, new Ping(1));
        Channel.recv(M);
        Channel.send(M, server, new Shutdown());
    }
}
