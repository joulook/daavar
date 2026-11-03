# Program interface for the chain-replicated object (fixed for every implementation of it)

Instance parameters: N servers (N = 3) forming the chain `s0` (head) → `s1` → `s2` (tail), and K clients
(K = 2). One object, holding an integer, initially 0. No server fails in this exercise, so the master and
the failure handling of the paper are not needed; links are reliable and deliver in order (the harness
selects the delivery model; assume FIFO per link).

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{K-1}`: the externally visible processes.
- servers `s0` … `s{N-1}`: internal; `s0` is the head, `s{N-1}` the tail, the chain follows the indices.
- The main thread only starts the processes (servers first in index order, then the clients in index
  order) and waits for them; it sends and receives nothing.

What each client does, exactly once, in this order: client `ci` sends `Update { from: i, value: i + 1 }` to the head
and waits for the reply; then it sends `Query { from: i }` to the tail and waits for the reply.

Messages between clients and servers (the only externally observable messages; their shape is fixed):
- client → head: `Update { from: int, value: int }` (`from` = the client's index, so that the reply can be addressed); reply, from the tail to the client: `UpdateReply { value: int }`
  carrying the object's value right after this update was applied at the tail.
- client → tail: `Query { from: int }`; reply, from the tail: `QueryReply { value: int }`.
- The paper's reply for an update is sent by the tail; the paper's `objID` is dropped (one object).

Messages among the servers (the forwarding of updates down the chain) are yours to define. The paper
leaves open how a server learns the next server's address and how a client learns the head's and the
tail's; assume every process knows every other process's address from the start.

Program entry:
- Java: package `bench.chainrep`, public class `ChainRep` implementing `org.mpi_sws.jmc.mp.launch.MpProgram`
  (see its source), constructed from the launcher's argument string `impl,<N>,<K>,<fifo|bag>`. The visible
  roles are the clients.
- Rust: crate `bench_chainrep` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the program for the arguments `<N> <K>`,
  and `spec` = the same program (the harness uses `imp`); default model FIFO.

Addendum (18:10, before any program was delivered): the client requests carry `from`, the client's index, because
the receive operations of the APIs do not report a message's sender; the reply to client `ci` goes to the process
named `c{i}`, whose address every process knows.
