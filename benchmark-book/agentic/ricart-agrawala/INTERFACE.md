# Program interface for the Ricart–Agrawala mutual exclusion (fixed for every implementation of it)

Instance parameters: N nodes (N = 3), each with one client; every client enters the critical section exactly
once (K = 1). No process fails; every message sent is eventually delivered, in any order (the harness selects
the delivery model).

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{N-1}`: the externally visible processes; client `ci` belongs to node `ni`.
- nodes `n0` … `n{N-1}`: internal; they run the paper's algorithm (node numbers 0 … N−1 are the paper's node
  numbers, used as the tie-break).
- The main thread only starts the processes (nodes in index order, then clients in index order) and waits
  for them; it sends and receives nothing.

What each client does, exactly once, in this order: `ci` sends `Enter { from: i }` to `ni` and waits for
`Granted {}` (the node has entered its critical section on the client's behalf); then it sends
`Exit { from: i }` to `ni` and waits for `Released {}` (the node has left the critical section and sent its
deferred replies).

Messages between clients and nodes (the only externally observable messages; their shape is fixed):
- client → node: `Enter { from: int }`, `Exit { from: int }` (`from` = the client's index).
- node → client: `Granted {}`, `Released {}`.

Messages among nodes (the paper's REQUEST with its sequence number and node number, and REPLY) are yours to
define; name them as the paper does. The paper's three processes per node with shared variables may be
one process per node handling its messages in arrival order, as long as the paper's rules (sequence
numbers, the deferral decision, the deferred replies after leaving) are kept. A node ends after its own
critical section and after having answered every other node's request. Assume every process knows every
other process's address from the start.

Program entry:
- Java: package `bench.ricart_agrawala`, public class `RicartAgrawala` implementing
  `org.mpi_sws.jmc.mp.launch.MpProgram` (see its source), constructed from the launcher's argument string
  `impl,<N>,<fifo|bag>`. The visible roles are the clients.
- Rust: crate `bench_ricart_agrawala` using `traceforge::program::Program` and
  `traceforge::program::main_with_model` (see `program.rs`), with `visible` = the client names, `imp` = the
  program for the argument `<N>`, and `spec` = the same program (the harness uses `imp`); default model Bag.
  All messages of the program are variants of one enum named `Msg`.
