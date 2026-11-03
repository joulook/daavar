# Program interface for the Flexible Paxos single-decree consensus (fixed for every implementation of it)

Instance parameters: A acceptors (A = 4), P proposers (P = 2), K clients (K = 2, one per proposer), and R,
the number of times a proposer may retry with a higher proposal number after a failed phase (R = 1). Quorums
as in the paper's section 4.1 for an even number of acceptors: a phase-1 quorum is any Q1 = 3 acceptors, a
phase-2 quorum any Q2 = 2 acceptors. No process fails; every message sent is eventually delivered, in any
order (the harness selects the delivery model).

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{K-1}`: the externally visible processes.
- proposers `p0` … `p{P-1}` and acceptors `a0` … `a{A-1}`: internal. Each proposer is also the learner of
  the value it decides.
- The main thread only starts the processes (acceptors, then proposers, then clients, each in index order)
  and waits for them; it sends and receives nothing.

What each client does, exactly once: client `ci` sends `Propose { from: i, value: i + 1 }` to proposer `pi`
and waits for the reply.

Messages between clients and proposers (the only externally observable messages; their shape is fixed):
- client → proposer: `Propose { from: int, value: int }` (`from` = the client's index; the reply goes to `c{from}`).
- proposer → client: `Decided { value: int }` with the value the proposer learned to be decided (its own
  value, or a value already chosen that it learned and re-proposed), or `Fail {}` when the proposer gave up
  after R retries without learning a decision.

Messages among proposers and acceptors (prepare, promise, propose, accept, and any conflict reply you
need) are yours to define; name them as the paper does. Proposal numbers are pairs (round, proposer index)
compared by round first. Assume every process knows every other process's address from the start.

Program entry:
- Java: package `bench.fpaxos`, public class `FPaxos` implementing `org.mpi_sws.jmc.mp.launch.MpProgram` (see
  its source), constructed from the launcher's argument string `impl,<A>,<K>,<fifo|bag>` (P = K). The visible
  roles are the clients.
- Rust: crate `bench_fpaxos` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the program for the arguments `<A> <K>`, and
  `spec` = the same program (the harness uses `imp`); default model Bag. All messages of the program are
  variants of one enum named `Msg`.
