# Program interface for Viewstamped Replication, normal operation (fixed for every implementation of it)

Instance parameters: 2f + 1 replicas with f = 1 (N = 3), K clients (K = 2), one request per client. The
group is fixed, view-number 0, replica `r0` is the primary, `r1` and `r2` the backups; no replica fails, so
only the normal-operation protocol (section 4.1) runs; no view change, no recovery. Every message sent is
eventually delivered, in any order (the harness selects the delivery model). The replicated service is a
counter, initially 0, whose only operation `add(n)` adds n and returns the new value.

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{K-1}`: the externally visible processes (each is the paper's client with its proxy).
- replicas `r0` … `r{N-1}`: internal.
- The main thread only starts the processes (replicas in index order, then clients in index order) and
  waits for them; it sends and receives nothing.

What each client does, exactly once: `ci` sends `Request { from: i, op: i + 1 }` to the primary `r0` (the
paper's REQUEST with the client-id `from`, request-number 1, operation `add(op)`) and waits for the reply.

Messages between clients and replicas (the only externally observable messages; their shape is fixed):
- client → primary: `Request { from: int, op: int }`.
- primary → client: `Reply { result: int }` (the counter's value after the client's operation executed;
  the paper's REPLY with the view-number and request-number dropped, since there is one view and one request).

Messages among the replicas (the paper's PREPARE, PREPAREOK, COMMIT with their fields) are yours to
define; name them as the paper does. Backups also execute the operations (the paper's up-call) once they
learn of the commit; a backup ends after executing K operations, the primary after replying to K clients.
Assume every process knows every other process's address from the start.

Program entry:
- Java: package `bench.vrr`, public class `Vrr` implementing `org.mpi_sws.jmc.mp.launch.MpProgram` (see
  its source), constructed from the launcher's argument string `impl,<N>,<K>,<fifo|bag>`. The visible roles
  are the clients.
- Rust: crate `bench_vrr` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the program for the arguments `<N> <K>`,
  and `spec` = the same program (the harness uses `imp`); default model Bag. All messages of the program
  are variants of one enum named `Msg`.
