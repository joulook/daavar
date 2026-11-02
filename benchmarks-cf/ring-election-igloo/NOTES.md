# ring-election-igloo: context-free Java twin

Class: `bench.ring_election_igloo.RingElectionIgloo` (implements `MpProgram`), file
`java/bench/ring_election_igloo/RingElectionIgloo.java`.

Argument string (public `(String...)` constructor): `<spec|impl>,<n>,<fifo|bag>[,<variant>]`,
`n >= 2`, variant `correct` (default) or `no-compare`. Example:
`bench.ring_election_igloo.RingElectionIgloo:impl,2,bag`.

Sources read: the Rust crate (`src/main.rs`, `Cargo.toml`), the JMC API files listed in the brief,
and the "Values" and "What both programs must satisfy" sections of SUMMARY-FORMAT.md. Nothing else.

## Mapping

Constants: `R = 1`, `D = 1`, as in Rust. Program name `ring-election-igloo`; `size()` = `n`.
Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.

Messages (sealed interface `Msg extends MpMessage`):
- `Msg::Id(i32)` -> `@Tag("Id") record Id(int value)`
- `Msg::Status { node: i32, leader: bool }` -> `@Tag("Status") record Status(int node, boolean leader)`
  (field order node, leader).

Roles: visible `observer` only. Declared roles: spec `[observer, oracle]`; impl
`[observer, node0, ..., node{n-1}]`. Main is invisible, spawns everything, then awaits all in spawn
order (spec: observer, oracle; impl: observer, node0..node{n-1}), as the Rust joins.

Channels: each Rust `Builder` channel has exactly one receiver thread, so each maps to that
thread's own mailbox: `Channel.send(model, <receiver tid>, msg)` / `Channel.recv(model)`.
Observer's channel <- all nodes (or the oracle); node i's channel <- node (i-1 mod n).

Threads:
- `observer`: `n` blocking `Channel.recv(model)` (no type guard, as `recv_msg_block` has none).
- spec `oracle`: `elected = Nondet.chooseBoolean()`; for i in 0..n: send
  `Status(i, elected && i == n-1)` to observer.
- impl `node i`: `udpSend(next, my_id)`; then R times: `Channel.tryRecv(model)` (non-blocking,
  untyped, then `instanceof Id`, mirroring `if let Some(Msg::Id(msg)) = rx.recv_msg()`);
  `msg == my_id` -> leader, break; else if `!compare || msg > to_send` -> `to_send = msg`;
  `udpSend(next, to_send)`. Then `Status(i, leader)` to the observer.
- `udpSend`: send `Id(v)`; if `budget > 0 && Nondet.chooseBoolean()` (nondet only evaluated when
  the budget is positive, short-circuit as in Rust) then `budget--` and send `Id(v)` again.

## Doubts / assumptions

1. Tid availability before spawn. Node i sends to node i+1, which Rust spawns later but whose
   channel exists beforehand. In JMC a destination is a `Tid`, which exists only once the thread is
   created. I therefore construct all `MpThread`s first (`new MpThread(body, role)`, which fixes the
   task id and reports TASK_CREATED), then `start()` them in the same order, then `join1()` them.
   I did not use `MpProcess.spawn` because it starts immediately and the strategy might run node0
   before node1 exists. Creation order = start order = Rust spawn order. The export run accepted the
   roles, so the recorder did not object, but this create-all-then-start-all pattern is a deviation
   from `MpProcess.spawn` worth checking.
2. `size()` returns just `n` (not the variant). I do not know what the Rust harness writes in its
   header's `size` field.
3. The variant is parsed (and validated) on the spec side too but ignored there, as in Rust's
   `parse`.
4. `nondet()` (Rust bool) -> `Nondet.chooseBoolean()` (options true, false); same outcome set.
5. `i32` -> `int`; `node` id in `Status` is the integer index, not a Tid (as in Rust).

## Counts measured (`must 0 1 report - 600`, counting mode)

| args | complete | blocked |
|---|---|---|
| spec,2,bag | 4 | 0 |
| impl,2,bag | 202 | 0 |
| impl,2,bag,no-compare | 194 | 0 |
| spec,2,fifo | 2 | 0 |
| impl,2,fifo | 72 | 0 |
| spec,3,bag | 12 | 0 |
| impl,3,bag | 7506 | 0 |

All ended "stack exhausted". An export run at n=2 bag (to a scratch file, since deleted) gave 4
distinct observer summaries for both spec (4 graphs) and impl (202 graphs).
