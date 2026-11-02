# ironlock: context-free Java twin

Class: `bench.ironlock.IronLock` (java/bench/ironlock/IronLock.java), implements `MpProgram`,
public `(String...)` constructor.

Arguments: `<spec|impl>,<n>,<fifo|bag>[,<variant>[,<spec-kind>]]`
- `n`: number of nodes / observers (Rust's `<n>`; Rust default 2, here required).
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- variant: `correct` (default), `double-hold`, `grant-without-token` (impl only; spec ignores it, as Rust).
- spec-kind: `any` (default), `ring` (spec only; impl ignores it, as Rust). This fifth field is an
  extension of the brief's format, because the Rust program takes `[variant] [spec-kind]` after `<n>`.

programName `ironlock`, size `"<n>"`. K = 2 (constant, as Rust).

## Thread mapping (spawn order as Rust)
- spec: `oracle` (invisible), then `obs0..obs{n-1}` (visible). Main awaits all.
- impl: `node0..node{n-1}` (invisible), then `obs0..obs{n-1}` (visible). Main awaits all.
- visibleRoles = `obs0..obs{n-1}`; rolesInSpawnOrder = invisible roles then observers.
- Each Rust channel has exactly one receiving thread, so each channel becomes that thread's
  mailbox (send to its Tid). Node mailboxes only receive `NodeMsg`, observer mailboxes only `Wire`.
- observer: up to K+2 blocking `Channel.recv(model, Wire.class)`, returns at the first `End`.
- oracle (any): for e = 1..K+1, `Nondet.chooseIndex(n+1)` (Rust `(0..n+1).nondet()`); c < n sends
  `Locked(e)` to obs_c; then `End` to every observer in index order.
- oracle (ring): for e = 2..K+1 sends `Locked(e)` to obs_{(e-1) mod n}; then `End` to all.
- node i: same code as Rust `node`: optional initial grant (grant-without-token, node 1,
  `Nondet.chooseBoolean()`), loop of 2K+2 steps, holder grants `Transfer(epoch+1)` to node (i+1) mod n
  or (epoch > K) sends `Stop` to all nodes (own included) in index order and stops; non-holder does a
  blocking `Channel.recv(model, NodeMsg.class)`; `Stop` ends, `Transfer(e)` with e > epoch takes the
  lock and sends `Locked(e)` to obs_i; double-hold after a grant at node 1 with `Nondet.chooseBoolean()`.
  On end, `End` to obs_i.

## Messages
- `NodeMsg` sealed: `@Tag("Transfer") record Transfer(int epoch)`, `@Tag("Stop") record Stop()`.
- `Wire` sealed: `@Tag("Locked") record Locked(int epoch)`, `@Tag("End") record End()`.
- Tags = Rust variant names; Rust `i32` -> `int`.

## Doubts / assumptions
1. Forward Tid references: nodes send to nodes and observers spawned after them. Main fills shared
   `Tid[]` arrays as it spawns; processes read them only at send time. This assumes (per the
   MpProgram contract) that main spawns everything before any process runs a message-passing
   event. A node's very first event may be an `ND` choice (grant-without-token) before its first
   send; the arrays are read only at the send. Worked in all runs below (no NPE).
2. Boolean nondet order: Java `chooseBoolean` has min = true; Rust `nondet()` order unknown to me.
   The outcome set is the same {true,false}.
3. Recv guards are class (type) tests `Wire.class` / `NodeMsg.class`; since each mailbox only gets
   one message type they accept everything, as the Rust typed channels do.
4. "Smallest size": I took n = 1 and also ran n = 2 (Rust's default). n = 1 has node 0 sending
   `Transfer` and `Stop` to its own mailbox, as Rust does with `all[(0+1)%1]`.
5. The brief's argument format has no slot for spec-kind; I appended it as a fifth optional field.

## Measured counts (plain enumeration: `must 0 1 report - 600`), all "stack exhausted"
| args | complete | blocked |
|---|---|---|
| spec,1,fifo | 8 | 0 |
| impl,1,fifo | 1 | 0 |
| spec,2,fifo | 27 | 0 |
| impl,2,fifo | 1 | 0 |
| spec,2,bag | 159 | 0 |
| impl,2,bag | 4 | 0 |
| impl,2,fifo,double-hold | 2 | 0 |
| impl,2,fifo,grant-without-token | 2 | 0 |
| spec,2,fifo,correct,ring | 1 | 0 |
| impl,3,fifo | 1 | 0 |
java/out and report/ removed after the runs.
