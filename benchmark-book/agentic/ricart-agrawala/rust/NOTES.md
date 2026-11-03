# Ricart–Agrawala, Rust (TraceForge Must API) — notes

Crate `bench_ricart_agrawala` (`Cargo.toml`, `src/main.rs`). Entry: `traceforge::program::main_with_model`
with `Program { visible: c0..c{N-1}, spec: run(N), imp: run(N) }`, default model `ConsType::Bag`; `N` is the
first program argument (default 3). All messages are variants of `enum Msg`.

## Processes
- `main` (`run`): spawns `n0..n{N-1}` then `c0..c{N-1}` (named with `thread::Builder::name`), joins all;
  sends and receives nothing.
- `ni` (`node`): the paper's three processes of node i merged into one process that handles its
  messages in arrival order (`recv_msg_block::<Msg>()`), as INTERFACE.md allows. The shared database is
  the process's local variables; the semaphore `Shared_vars` is unnecessary in one process.
- `ci` (`client`): `Enter{from:i}` → wait `Granted{}` → `Exit{from:i}` → wait `Released{}`.

## Paper step → code
| Paper | Code |
|---|---|
| SHARED DATABASE `Our_Sequence_Number`, `Highest_Sequence_Number` initial 0, `Outstanding_Reply_Count`, `Requesting_Critical_Section` initial FALSE, `Reply_Deferred[1:N]` initial FALSE | locals of `node` with the same names (node numbers 0..N−1) |
| Invoke mutual exclusion (process 1): `Requesting_Critical_Section := TRUE; Our_Sequence_Number := Highest+1; Outstanding_Reply_Count := N−1; Send REQUEST(Our_Sequence_Number, me) to all j ≠ me` | arm `Msg::Enter` (the client's request is the paper's "mutual exclusion is invoked") |
| `WAITFOR (Outstanding_Reply_Count = 0)`; critical section | when the count reaches 0 (arm `Msg::Reply`, or at once in `Enter` if N = 1) the node sends `Granted{}` to its client: the node is in its critical section until the client's `Exit` |
| Release: `Requesting_Critical_Section := FALSE; for j: if Reply_Deferred[j] then Reply_Deferred[j] := FALSE; Send REPLY to j` | arm `Msg::Exit`, then `Released{}` to the client |
| Process 2, REQUEST(k, j): `Highest := max(Highest, k)`; `Defer_it := Requesting AND (k > Our OR (k = Our AND j > me))`; defer or send REPLY | arm `Msg::Request { seq: k, from: j }` |
| Process 3, REPLY: `Outstanding_Reply_Count := Outstanding_Reply_Count − 1` | arm `Msg::Reply` |

Node-to-node messages: `Request { seq, from }` (the paper's REQUEST(k, j)) and `Reply {}` (REPLY, no content).

## Decisions where the paper is silent
1. Merging the three processes into one ("The three processes run asynchronously but operate on a set
   of common variables. A semaphore is used to serialize access to the common variables when
   necessary."): one process per node handles messages one at a time in arrival order; every handler
   is then atomic, which is at least as strong as the paper's P/V sections. No `nondet` choice is
   needed: the only open choice is which message to process first, and that is the arrival order.
2. When the node enters ("Comment Critical Section Processing can be performed at this point;"): the
   node enters when the last REPLY is processed while `Requesting_Critical_Section` is TRUE, and signals
   it to the client with `Granted{}`; it leaves on the client's `Exit`, sending its deferred REPLYs
   before `Released{}` (INTERFACE.md: "the node has left the critical section and sent its deferred
   replies").
3. Termination ("A node ends after its own critical section and after having answered every other
   node's request", INTERFACE.md; the paper's processes never end): the node loops until it has left
   its critical section and has sent N−1 REPLYs (K = 1, so each other node sends exactly one REQUEST).
4. Addresses ("Assume every process knows every other process's address from the start"; the paper:
   "refers to its own unique node number"): thread ids are fixed by spawn order (main t0, ni = t(1+i),
   ci = t(1+N+i)) and built with `thread::construct_thread_id`; `run` checks each spawned handle's id
   against them with `traceforge::assert`, so a mismatch would be reported, not silently misrouted.
5. The `from` field of `Enter`/`Exit` is checked (`assert_eq!`) to be the node's own client index; an
   unexpected message kind panics.
6. Integer type: sequence numbers and node numbers are `i32` (the paper's INTEGER).

## Compile
```
cd daavar/benchmarks-cf2/ricart-agrawala/rust
CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-ra-target cargo build --release -j 2
```
Result: `Finished release profile [optimized] target(s) in 2m 21s`.

## The one run
```
timeout 300 /local/home/mkhoshechin/tmp/cf2-ra-target/release/bench_ricart_agrawala count impl --model bag -- 3
```
Exit 0 in 7.7 s. The measurement block after the result reports `impl_complete: 32800`,
`impl_blocked: 0` (so the result line is `execs=32800 blocked=0`); the last output line is
`cache_hits: ` (the block's final, empty field).
