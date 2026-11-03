# Program interface for the CASPaxos register (fixed for every implementation of it)

Instance parameters: A acceptors (A = 3), K clients (K = 2), and R, the number of times a proposer may retry
one request after a conflict (R = 1). There are P = 2 proposers. All processes are reliable (no crashes); every
message sent is eventually delivered, in any order (the harness selects the delivery model).

Roles and names (one process each, the names exactly as given):
- clients `c0` … `c{K-1}`: the externally visible processes.
- proposers `p0` … `p{P-1}` and acceptors `a0` … `a{A-1}`: internal.
- The main thread only starts the processes (clients last, in index order after `p*` then `a*`) and waits
  for them; it sends and receives nothing.

The register holds an integer, initially 0 (the paper's ∅ is represented as 0 for the purpose of this
exercise; the change functions below never produce a value that would be mistaken for it).

What each client does, exactly once: client `ci` sends one request to proposer `p(i mod P)` and waits for
its reply. The request is a compare-and-set change function: "if the current value is `i` then set it to
`i + 1`, otherwise leave it unchanged". The reply carries the register's new value (the value after the
function was applied, changed or not), or a failure when the proposer gave up after R retries.

Messages between clients and proposers (these two are the only externally observable messages; their
shape is fixed):
- client → proposer: `Cas { expected: int, value: int }`
- proposer → client: `Done { value: int }` on success, `Fail {}` when the proposer gave up.

Messages among proposers and acceptors (prepare, promise/conflict, accept, accepted/conflict) are yours to
define; name them as the paper does. Ballot numbers are the paper's (counter, proposer-id) pairs.

Program entry:
- Java: package `bench.caspaxos`, public class `CasPaxos` implementing `org.mpi_sws.jmc.mp.launch.MpProgram`
  (see its source), constructed from the launcher's argument string `impl,<A>,<K>,<fifo|bag>`; `R` is a constant
  in the program. The visible roles are the clients.
- Rust: crate `bench_caspaxos` using `traceforge::program::Program` and `traceforge::program::main_with_model`
  (see `program.rs`), with `visible` = the client names, `imp` = the register program for the arguments
  `<A> <K>`, and `spec` = the same program (the harness uses `imp`); default model Bag. `R` is a constant.
