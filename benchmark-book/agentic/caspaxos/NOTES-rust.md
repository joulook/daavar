# CASPaxos register, Rust (TraceForge Must API) — bench_caspaxos

Source: `src/main.rs`; build file: `Cargo.toml` (crate `bench_caspaxos`, `[workspace]`, traceforge by path).
Entry: `traceforge::program::main_with_model` with `visible` = `c0..c{K-1}`, `imp` = `spec` = the register
program for the arguments `<A> <K>`, default model `Bag`. Constants: P = 2 proposers, R = 1 retry.

## Processes (spawned by main in this order, named with `thread::Builder::name`)
`p0..p{P-1}`, then `a0..a{A-1}`, then `c0..c{K-1}`; main then joins all of them and sends/receives nothing.

## Messages (one enum `Msg`)
| Message | From → to | Paper |
|---|---|---|
| `Cas { expected, value }` (tagged with the client index) | client → proposer | "A client submits the f change function to a proposer" |
| `Prepare { ballot }` | proposer → every acceptor | "sends 'prepare' messages containing that number to the acceptors" |
| `Promise { ballot, accepted: Option<(Ballot, i32)> }` | acceptor → proposer | "returns a confirmation either with an empty value ... or with a tuple of an accepted value and its ballot number" |
| `Conflict { ballot, seen }` | acceptor → proposer (both phases) | "Returns a conflict if it already saw a greater ballot number" |
| `Accept { ballot, value }` | proposer → every acceptor | "sends the result, new state, along with the generated ballot number B (an 'accept' message)" |
| `Accepted { ballot }` | acceptor → proposer | "returns a confirmation" (phase 2) |
| `Done { value }` / `Fail {}` | proposer → client | "Returns the new state to the client" / gave up after R retries (INTERFACE.md) |
| `Stop {}` | proposer → every acceptor | not in the paper (termination, see decisions) |

## Roles, step by step
- **Client `ci`** (`client`): sends `Cas { expected: i, value: i + 1 }` to `p(i mod P)`, waits for `Done`/`Fail`.
- **Proposer `pid`** (`proposer`): serves the clients `ci` with `i mod P == id`. Per request, at most 1 + R
  attempts: ballot `(counter + 1, id)` (`Ballot`, derived lexicographic `Ord`: counter first, ID as tiebreaker);
  `Prepare` to all acceptors; `await_quorum` waits for F + 1 confirmations (F = (A - 1) / 2); if none of them
  carries an accepted tuple the current state is 0 (the paper's ∅), otherwise the value of the tuple with the
  highest ballot; applies f (`if current == expected then value else current`); `Accept` to all acceptors;
  waits for F + 1 `Accepted`; replies `Done { value: new }`. After the last attempt fails: `Fail {}`.
- **Acceptor `aj`** (`acceptor`): state `promise` (ZERO = none) and `accepted: Option<(Ballot, i32)>`;
  `seen = max(promise, accepted ballot)`. `Prepare b`: `seen > b` → `Conflict`; else `promise = b`, `Promise`
  with `accepted`. `Accept b v`: `seen > b` → `Conflict`; else erase the promise, `accepted = (b, v)`,
  `Accepted`. Replies go to the proposer named by `b.id`.
- **Conflict reaction**: "When a proposer receives a conflicting message from an acceptor, it should update its
  counter to avoid a conflict in the future." → `counter = max(counter, seen.counter)` (the acceptor returns the
  ballot it saw in the conflict), so the next ballot `(counter + 1, id)` exceeds it.

## Decisions where the paper is silent
1. *When a phase gives up.* "Waits for the F + 1 confirmations" says nothing about conflicts. The proposer keeps
   waiting while F + 1 confirmations are still possible and abandons the attempt as soon as more than
   A − (F + 1) conflicts arrived (no quorum can form). No choice operation is needed: replies arrive in some order.
2. *What a conflict carries.* "Returns a conflict if it already saw a greater ballot number" — the conflict
   carries the request's ballot and the greater ballot seen (`seen`), which the proposer uses for the counter
   update ("it should update its counter").
3. *Stale replies.* The paper does not say what to do with confirmations of an abandoned attempt or late ones
   from a finished phase. Every acceptor reply is tagged with `reply_tag(ballot, phase)`; the proposer receives
   only replies with the tag of its current ballot and phase (`recv_tagged_msg_block`), so stale replies are
   never received (left in the mailbox).
4. *Equal ballots.* "already saw a greater ballot number": a conflict only for strictly greater; equal is
   accepted (only the same proposer's same attempt can send an equal ballot).
5. *"Erases the promise".* After an accept the promise is reset to ZERO; the accepted ballot still counts
   as seen, so a later smaller prepare/accept still gets a conflict.
6. *Several requests at one proposer* (only when K > P; not with K = 2): requests are served one at a time;
   which waiting client is served next is chosen with `nondet()` (`choose`), then its `Cas` is received by
   its tag. With K = 2 each proposer serves one client and makes no choice.
7. *Retry budget.* "R, the number of times a proposer may retry one request after a conflict": 1 + R attempts per
   request, each with a fresh ballot; the counter persists across requests.
8. *Termination of acceptors.* The paper's acceptors run forever. Each proposer, after its last request, sends
   `Stop {}` to every acceptor; an acceptor ends after P `Stop`s (messages still in its mailbox are then
   irrelevant: no proposer waits for them). This lets main join every process.
9. *Learning thread ids (not protocol).* Proposers are spawned before acceptors and clients, and the API's
   receive does not return the sender. Main writes each acceptor's ThreadId into a shared registry before
   spawning any client; each client writes its own ThreadId into a registry before sending its `Cas`. A
   proposer reads acceptor ids only after receiving a `Cas` and the client id only to reply, so every read
   happens after the write. The registries carry no protocol state. Acceptors get proposer ids at spawn.
10. *Values.* The register is an `i32`, initially 0 (∅). Ballot counters start at 1, so `(0, 0)` is a bottom.

## Build and run
Compile (from this directory):
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-caspaxos-target cargo build --release -j 2` — finished, 0 warnings.

One run (smallest parameters of the interface, A = 3, K = 2, bag):
`timeout 580 /local/home/mkhoshechin/tmp/cf2-caspaxos-target/release/bench_caspaxos count impl --model bag -- 3 2`
Result: no output; killed by `timeout` after 580 s (exit 143); last output line: `Terminated`. The exploration
did not finish within that bound; the program was not run again.
