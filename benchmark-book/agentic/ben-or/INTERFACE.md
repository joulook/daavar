# Program interface for Ben-Or's consensus protocol (fixed for every implementation of it)

Instance parameters: N processes (N = 3), t = 1 (the protocol's fault bound: a process waits for N − t
messages of a round; no process actually fails in this exercise), K = N clients (one per process), and R,
the maximum number of rounds (R = 2): a process that has not decided after round R stops and reports so.
Every message sent is eventually delivered, in any order (the harness selects the delivery model). The
paper's coin flip ("each with probability 1/2") is the API's choice operation.

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{N-1}`: the externally visible processes.
- processes `p0` … `p{N-1}`: internal; `pi` runs the protocol with the initial value its client gives it.
- The main thread only starts the processes (`p*` in index order, then the clients in index order) and waits
  for them; it sends and receives nothing.

What each client does, exactly once: `ci` sends `Input { from: i, value: i mod 2 }` to `pi` and waits for
the reply.

Messages between clients and processes (the only externally observable messages; their shape is fixed):
- client → process: `Input { from: int, value: int }` (the process's initial value, 0 or 1; `from` = the
  client's index, the reply goes to `c{from}`).
- process → client: `Decide { value: int }` when the process decides, or `Undecided {}` when it reaches the
  end of round R without deciding.

Messages among the processes (the paper's type-1 and type-2 messages of a round, with their fields)
are yours to define; name them as the paper does. A process ends after replying to its client; messages
of later rounds that arrive after that are ignored. Assume every process knows every other process's
address from the start.

Program entry:
- Java: package `bench.ben_or`, public class `BenOr` implementing `org.mpi_sws.jmc.mp.launch.MpProgram` (see
  its source), constructed from the launcher's argument string `impl,<N>,<fifo|bag>`. The visible roles are
  the clients.
- Rust: crate `bench_ben_or` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the program for the argument `<N>`, and
  `spec` = the same program (the harness uses `imp`); default model Bag. All messages of the program are
  variants of one enum named `Msg`.
