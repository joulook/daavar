# paxos-oopsla25: context-free Java twin

Class: `bench.paxos_oopsla25.PaxosOopsla25` (implements `MpProgram`), constructor `(String...)`.
Source read: `daavar/benchmarks/paxos-oopsla25/rust/src/main.rs` and `Cargo.toml`, the Java API
sources listed in the brief, and the two sections of SUMMARY-FORMAT.md. Nothing else.

## Argument format

`<spec|impl>,<L>,<fifo|bag>[,<variant>]`, e.g. `impl,1,bag,skip-2a-check`.
- `L` = learners, 1..=3 (Rust asserts the same range). 2 clients, 2 proposers, 3 acceptors fixed.
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN` (the Rust program declares
  bag as its default model; I have no default here, the model is mandatory in the argument string).
- variant: `correct` (default), `ignore-max-vote`, `skip-2a-check`, `half-majority`, `1b-none`.
  As in Rust's `parse`, the variant is validated on both sides and only affects `impl`.
- `programName()` = `paxos-oopsla25`, `size()` = `L`.

## Threads (roles), spawn order and awaits

Main is invisible, spawns everything, performs no message event, awaits in the Rust join order.
- spec: `oracle`, `l0..l{L-1}`, `c0`, `c1`; await c0, c1, l*, then oracle.
- impl: `p1`, `p2`, `a0`, `a1`, `a2`, `l0..l{L-1}`, `c0`, `c1`; await c0, c1, l*, then p1, p2, a0..a2.
- visible: `c0`, `c1`, `l0..l{L-1}` (as Rust's `visible`). Invisible threads are still spawned with
  their Rust names (`oracle`, `p1`, ...) and listed in `rolesInSpawnOrder()`, not in `visibleRoles()`.

Channels: every Rust named channel `(role, i)` has exactly one receiving thread (`("o",0)` oracle,
`("p",b)` proposer b, `("a",a)` acceptor a, `("l",j)` learner j), so each channel is mapped to
that thread's own mailbox: a send on `("p",b)` is `Channel.send(model, tid(p{b}), ...)`.

Thread bodies, line by line from Rust:
- `c{i}`: one send `Propose(i+1)` to the oracle (spec) or to `p{i+1}` (impl).
- `l{j}`: one blocking receive, unguarded; anything but `Decide` throws (Rust `unreachable!`).
- `oracle`: 2 non-blocking unguarded receives, keeping the value of each `Propose` read;
  then if the list is non-empty, `Nondet.chooseBoolean()`; if true, `Nondet.chooseIndex(len)` picks
  the value, sent as `Decide(v)` to every learner in order l0.. .
- `p{b}`: blocking receive (expects `Propose`), `Msg1a(b)` to a0,a1,a2; up to 3 blocking unguarded
  receives while fewer than `majority` distinct acceptors with a `Msg1b` of ballot b (non-`Msg1b`
  messages consume an iteration, as in Rust's `if let`); value = highest-ballot vote unless
  `ignore-max-vote` or none, else own; `Msg2a(b,v)` to a0..a2; up to 3 blocking receives for
  `Msg2b` of ballot b likewise; if majority reached, `Decide(v)` to every learner.
  `majority` = 1 under `half-majority`, else 2.
- `a{me}`: 4 blocking unguarded receives; `Msg1a(b)`: if no maxBal or b > maxBal, set maxBal and
  send `Msg1b(me, b, vote)` to p{b} (vote = `Optional.empty()` under `1b-none`, else its maxVote);
  `Msg2a(b,v)`: if `skip-2a-check` or no maxBal or b >= maxBal, set maxBal, maxVote = Vote(b,v),
  send `Msg2b(me,b,v)` to p{b}; any other message throws (Rust `unreachable!`).

## Messages

Sealed interface `Msg extends MpMessage`, one record per Rust variant, same tags and field order:
- `@Tag("Propose") Propose(int value)`
- `@Tag("Msg1a") Msg1a(int ballot)`
- `@Tag("Msg1b") Msg1b(int acceptor, int ballot, Optional<Vote> vote)`
- `@Tag("Msg2a") Msg2a(int ballot, int value)`
- `@Tag("Msg2b") Msg2b(int acceptor, int ballot, int value)`
- `@Tag("Decide") Decide(int value)`
- `@Tag("Vote") Vote(int ballot, int value)` for the tuple struct `Vote(Ballot, Value)` (not an
  `MpMessage`; only carried inside `Msg1b`).
`u8` -> `int`.

## Doubts and assumptions

1. **Process ids before spawn.** The oracle and proposers send to learners/acceptors spawned after
   them, so ids are needed before spawning. I predict them as `main + 1 + k` for the k-th spawned
   process (MpProgram's javadoc: "the k-th process main spawns is task k + 1"), using
   `MpProcess.self()` on main (a runtime read, not a message event), and check every prediction
   against the returned handle; a mismatch throws. Not a race, but it relies on that numbering.
2. **`Optional` in a payload.** The brief prescribes `Optional` for `Option`, but `MpValueEncoder`
   (as I read it) has no case for `Optional` and would refuse it ("not encodable"). This does not
   affect the plain enumeration; an export of `impl` with a `Msg1b` read would fail if the encoder
   is as I read it. How the Rust side encodes `Option<Vote>` (null / tagged `Some`/`None`) is not in
   the sections I was allowed to read. Left as the brief says.
3. **Rust `nondet()` vs `Nondet.chooseBoolean()`.** Same outcome set {true, false}; the order
   (Java min = true) may differ from TraceForge's; I assume it is irrelevant to the graph sets.
4. **`(0..len).nondet()` with len 1.** Java `chooseIndex(1)` emits no ND event; I do not know
   whether TraceForge emits one for a one-element range. Same outcomes either way; an ND event is
   not a message event, so I assume it does not change the counts of distinct graphs (it might if
   TraceForge counts graphs including ND nodes, but the value set is the same).
5. **Guards.** Every Rust receive is unguarded (`recv_msg_block()` / `recv_msg()` then pattern
   match), so every Java receive is unguarded (`Channel.recv(model)` / `Channel.tryRecv(model)`).
6. **`unreachable!()`** is mapped to `IllegalStateException` in the process (a process failure).
   I believe these branches are unreachable in all variants (each mailbox only ever receives the
   message kinds its owner matches), as in Rust.
7. **Model mandatory.** Rust defaults to bag; the Java argument string requires the model.

## Counts measured (plain Must enumeration, `must 0 1 report - 600`, L = 1)

| side | model | result | time |
|---|---|---|---|
| spec, L=1 | bag (ASYN) | `graphs: 8 distinct complete, 7 distinct blocked; stack exhausted` | 0.9 s |
| spec, L=1 | fifo (P2P) | `graphs: 8 distinct complete, 7 distinct blocked; stack exhausted` | 0.5 s |
| impl, L=1, correct | bag (ASYN) | **timeout at 600 s**: 50811 complete, 17952 blocked so far; stack not exhausted (68763 iterations) | 600 s |
| impl, L=1, correct | fifo (P2P) | `graphs: 36288 distinct complete, 23904 distinct blocked; stack exhausted` | 434 s |

The impl side under the Rust program's default model (bag) does not finish within the brief's
600 s bound at the smallest size L = 1; under fifo it does. I did not change the program to make it
finish (no tuning against counts), and did not look up the Rust counts.
