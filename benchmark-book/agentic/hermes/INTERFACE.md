# Program interface for the Hermes-replicated key (fixed for every implementation of it)

Instance parameters: N replicas (N = 3) and K clients (K = 2). One key, holding an integer, initially 0.
No replica fails in this exercise and no membership changes, so leases, the failure detection and the
recovery of the paper are not needed; messages are reliable and may be delivered in any order (the harness
selects the delivery model). Only reads and writes (no RMWs).

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{K-1}`: the externally visible processes.
- replicas `r0` … `r{N-1}`: internal; every replica can coordinate a write and serve a read.
- The main thread only starts the processes (replicas first in index order, then the clients in index
  order) and waits for them; it sends and receives nothing.

What each client does, exactly once, in this order: client `ci` sends `Write { from: i, value: i + 1 }` to replica
`r(i mod N)` and waits for its reply; then it sends `Read { from: i }` to replica `r((i + 1) mod N)` and waits for
its reply.

Messages between clients and replicas (the only externally observable messages; their shape is fixed):
- client → replica: `Write { from: int, value: int }` (`from` = the client's index, so that the reply can be addressed); reply: `WriteAck {}` when the write has completed at its
  coordinator (the paper's acknowledgement to the client).
- client → replica: `Read { from: int }`; reply: `ReadReply { value: int }`.

Messages among the replicas (the paper's INV, ACK, VAL with their timestamps) are yours to define; name them
as the paper does. Timestamps are the paper's (version, coordinator id) pairs. A read at a replica whose copy
is not valid waits as the paper says (it is served when the copy becomes valid again). Assume every process
knows every other process's address from the start.

Program entry:
- Java: package `bench.hermes`, public class `Hermes` implementing `org.mpi_sws.jmc.mp.launch.MpProgram`
  (see its source), constructed from the launcher's argument string `impl,<N>,<K>,<fifo|bag>`. The visible
  roles are the clients.
- Rust: crate `bench_hermes` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the program for the arguments `<N> <K>`,
  and `spec` = the same program (the harness uses `imp`); default model Bag.

Addendum (18:10, before any program was delivered): the client requests carry `from`, the client's index, because
the receive operations of the APIs do not report a message's sender; the reply to client `ci` goes to the process
named `c{i}`, whose address every process knows.
