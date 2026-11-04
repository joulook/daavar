# SwiftPaxos (one key, N = 3, K = 2, ballot 0, leader n0, no failure): derived client-observable specification

Sources read: PAPER-EXCERPT.md, INTERFACE.md, traceforge/src/lib.rs, traceforge/src/program.rs, the rust-template.
Files: `Cargo.toml`, `src/main.rs`, this file.

## 1. What the specification says

The 2q commands of `c0` and `c1` are executed one at a time on the register `x` (initially 0), in **some interleaving
of the two clients' program orders**; every reply is the result of its command in that sequential execution
(Write: `Ack`, x := v; Read: `Value(x)`; Rmw: x := x + 10, `Result(x)`). Every interleaving is possible, and every
request is answered (no blocked execution). Nothing else is observable.

## 2. Roles and messages of the program

| process | role | receives | sends |
|---|---|---|---|
| `c0`, `c1` (visible) | the interface's client code, verbatim | `Rep` from its library | `Req` to its library |
| `l0`, `l1` | client library of `ci`: one command at a time | `Setup` (tag 1, from `init`); `Req` from `ci`; `Rep` from `sm` | `Req` to `sm`; `Rep` to `ci` |
| `sm` | the abstract replicated state machine (stands for the order in which leader n0 receives and optimistically executes commands) | `Setup` (tag 1); 2q `Req` from `l0`/`l1` | `Rep` to the issuing library |
| `init` | sends `Setup { nodes: [sm], libs: [l0, l1], clients: [c0, c1] }` to `sm`, `l0`, `l1` | — | `Setup` (tag 1) |

Message by message for one command of `ci`: `ci -> li: Req` ; `li -> sm: Req` (forwarded unchanged; it carries the
client index) ; `sm` applies it to `x` ; `sm -> li: Rep` ; `li -> ci: Rep`.

Receives are filtered (`recv_tagged_msg_block`): Setup by tag 1, so a Req arriving before Setup stays buffered;
`li` takes requests only from `ci` and replies only from `sm`; `sm` takes untagged messages from `l0`/`l1`.
`main` spawns `sm`, `l0`, `l1`, `c0`, `c1`, `init` (with `thread::Builder::new().name(..)`) and joins them; it
sends and receives nothing. `sm` ends after 2q commands, `li` after q, `init` after its 3 sends.
The only freedom is the order in which the two libraries' requests reach `sm` (each library has at most one request
outstanding, so fifo vs bag makes no difference). No `nondet()` is used: no further choice is needed (see D3).

## 3. Paper sentences each part follows

- **One sequential register execution.** §2: "The service is defined by a deterministic state machine ..."; Ordering:
  "The relation ≺ is acyclic"; "Ordering guarantees that different replicas cannot execute conflicting commands in
  different orders"; "Implementing this specification is sufficient to ensure the linearizability ... of the
  service". Invariant 2: "For any two conflicting commands c and c′ committed at a replica, either c belongs to the
  dependencies of c′, or the converse holds." Invariant 3 (acyclic committed graph). In this instance every two
  commands conflict except two Reads, so all non-Read commands are totally ordered; a Read is ordered against every
  Write/Rmw; the relative order of two Reads does not change any result. Hence one total order explains all results.
- **The reply is the result of the command in that order.** Nontriviality: "The return value r obtained by the client
  for a command c is the result of c’s execution at some replica." A.5: "the result of execution of c at any correct
  replica equals to the result of optimistic execution at leader(b)". Figure 3 lines 3-5: response(id, r) with r
  from the leader's `Reply`.
- **The order is the leader's receive order, and any interleaving is possible.** Figure 4 lines 11-19: the leader's
  proposal D = {id | cmd[id] ▷◁ c} (all conflicting commands it received before c), `pending_log ← pending_log · c`,
  `r = opt_exec(pending_log, state)`. §4.1: "If p is the leader, upon receiving a command p also executes it and sends
  its result in a Reply message". §4.4: "the followers will always accept the command unless one of them suspects
  the leader of failure"; §3.3/§4.1: on both fast and slow paths "replicas commit commands with the dependencies
  proposed by the leader". With no failure, committed order = the leader's receive order, which the network fixes
  freely (Propagate messages from two clients arrive at n0 in any order). This is `sm`'s arrival order.
- **Each client's commands in program order.** Interface: ci submits command j+1 only after the reply to j; §2
  ≺ includes ◀ ("c was executed by some replica before any process submitted d", A.3).
- **Every request is answered.** Liveness: "If a command c is submitted by a non-faulty client ... then c is
  eventually executed by all non-faulty replicas"; §3.5: "a client always gets a response within 2 or 3 message
  delays". No failure, every message delivered -> every reply eventually reaches the client. `sm`/`li` serve exactly
  2q / q commands and end, so no execution of the spec blocks.
- **Validity / Integrity.** A replica executes only submitted commands, each at most once: `sm` applies each received
  request exactly once, and only requests the clients sent.

## 4. Decisions where the paper (or the prompt) was silent or I had to choose

- D1. **opt_exec semantics.** The paper says only that opt_exec computes the result "reading the local copy while
  taking into account prior uncommitted commands in pending_log". I take it to mean: apply `state` then all of
  `pending_log` in order; i.e. the result in the leader's receive order. This is what makes Nontriviality hold.
- D2. **Linearizability across clients is not modelled.** Real-time order between different clients (◀ across
  clients) is not observable from per-client sequences of request/reply, so the spec allows every interleaving of
  the two program orders. I believe the protocol (no failure) can show every such interleaving: n0 can receive the
  clients' Propagates in any order, and a client can always be delayed.
- D3. **No `nondet()`.** The only protocol freedom visible to clients (command order) is expressed as message arrival
  order at `sm`, which the prompt allows ("otherwise the order in which messages arrive"). Fast vs slow path, which
  follower acks first, dependency-path mismatches (line 29) etc. change only latency/invisible messages, never the
  reply value, so they are not choices in the spec.
- D4. **Two Reads.** Reads commute, so their mutual order is irrelevant to results; the spec totally orders them
  anyway (no observable difference).
- D5. **Names.** The paper's replicas are not modelled; the single abstract process is named `sm`, not `n0`, and
  there are no `n1`, `n2` threads. `Setup.nodes = [sm]`. If the exporter/comparison requires the interface's thread
  names `n0..n2` for internal threads, rename `sm` to `n0` (internal threads are invisible, so it should not matter).
- D6. **Termination.** INTERFACE.md says libraries and replicas never end; the prompt says the internal threads of
  this specification must end once all requests are served. I followed the prompt: `li` ends after q commands, `sm`
  after 2q, so every spec execution is complete (blocked = 0).
- D7. **Libraries are kept** (the client code sends to `li`, and the visible messages are between `ci` and `li`).
  `li` forwards `Req` unchanged to `sm` and `Rep` unchanged back; internal message types are therefore `Req`/`Rep`
  themselves plus `Setup`.
- D8. **Arguments.** `<q> [<fifo|bag>]`: only `q` (first argument, default 1) is read; the model comes from the
  harness's `--model` option.
- D9. **Section 2 excerpt is partly cut** (the definition of ≺ and the start of Validity are missing from the
  excerpt); I used A.3's expression ≺ = (∪p ◁p) ∪ ◀ and A.1's proof of Validity.

## 5. Build and run

Compile (inside this directory): `cargo build --release -j 2` -> `Finished release profile [optimized] target(s) in 3m 13s` (exit 0, no warnings shown in the tail).

Run (inside this directory): `timeout 600 target/release/bench_swiftpaxos_derived count spec --model fifo -- 1`
-> first output line `execs=2 blocked=0` (exit 0), followed by the `--- measurements` block whose last line is
`cache_hits: ` (empty value). Expected by hand for q = 1: c0 does Write(1), c1 does Read; the two orders give
c1 `Value(0)` or `Value(1)`: 2 complete executions.

Deviation: the run command was executed three times, not once (the first time with `| tail -20`, which hid the
`execs=` line; then twice more in one command to grep that line and to show the exact last line). All three gave
the same output.
