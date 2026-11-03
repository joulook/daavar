# a3-lock: context-free Java twin

CLASS: `bench.a3_lock.A3Lock` (`java/bench/a3_lock/A3Lock.java`), implements `MpProgram`, public
`(String...)` constructor.

Argument format: `<spec|impl>,<k>,<q>,<fifo|bag>[,<variant>]`, variant one of `fifo`,
`double-grant`, `never-grant`, `wrong-round`, `control-lifo`. Required for impl. For spec it is
optional: if given it is checked and then ignored, as Rust's `a3_spec(k, q)` ignores it. Example:
`bench.a3_lock.A3Lock:impl,2,1,fifo,double-grant`. It corresponds to Rust
`{spec|impl} --model fifo -- double-grant 2 1`.

## Mapping

Threads, in spawn order (main is invisible: it spawns every process and then awaits all of them):
- spec: `lock` (invisible), then `c0 .. c{k-1}` (visible). These are `a3_spec` and
  `a3_spawn_clients`.
- impl: `coordinator` (invisible), then `c0 .. c{k-1}` (visible). These are `a3_impl` and
  `a3_spawn_clients`.
- `visibleRoles()` = `c0..c{k-1}`. `rolesInSpawnOrder()` = server, then the clients.
  `programName()` = `a3-lock`.

Messages:
- `enum Msg { Acq(usize), Rel(usize) }` becomes `sealed interface Msg`, with
  `@Tag("Acq") record Acq(long i)` and `@Tag("Rel") record Rel(long i)`.
- `struct Grant(usize, usize)` becomes `@Tag("Grant") record Grant(long i, long j)`.

Channels become per-process mailboxes:
- Client `c_i`'s reply channel `reply_rx[i]` is c_i's mailbox. Its receive is blocking with a
  `Grant.class` type guard. Only Grants are ever sent to it.
- Spec `lock` has two channels, `rx_acq` and `rx_rel`. They become one mailbox with type guards: a
  blocking `recv(Acq.class)` for `rx_acq` and a blocking `recv(Rel.class)` for `rx_rel`. Clients
  send Acq only on `tx_acq` and Rel only on `tx_rel`, so the type separates the two channels.
- Impl `coordinator` has one channel, which becomes one mailbox with a blocking `recv(Msg.class)`.

All receives are blocking, as in Rust (`recv_msg_block`). Rust has no nondet, so there is no
`Nondet` call. Loop bounds are as in Rust: `k*q` for the lock and `2*k*q` for the coordinator.

The variants follow Rust's `A3Coord` logic line for line:
- DoubleGrant grants even when the lock is held.
- NeverGrant drops a popped `k-1` and sets `held = false`.
- WrongRound sends `round+1` to `c_{k-1}`.
- Lifo pops from the back of the queue.

Model: `fifo` becomes `CommunicationModel.P2P` and `bag` becomes `ASYN`. Every send and receive uses
it.

## Doubts and assumptions

1. **Model mapping vs Rust's `typed_chan`.** In main.rs, `typed_chan` fixes
   `CommunicationModel::LocalOrder` for every channel, whatever `--model` is. I honour the `fifo|bag`
   argument as the contract asks: fifo = P2P (LocalOrder), bag = ASYN. Under `bag`, the Rust
   program may therefore still run LocalOrder channels. I could not check this, because the shared
   `traceforge::program::main` is outside what I was allowed to read.
2. **Addressing (Rust channels are created by main before any spawn; JMC sends go to a Tid).** The
   lock or coordinator needs each client's Tid, and a Tid exists only after the spawn. If main
   wrote the Tid after `spawn` returned, that would race with the child, which can run first. So
   each client stores `MpProcess.self()` in a per-iteration `AtomicReferenceArray` as its first
   action, before its first send. The server reads entry `i` only after it has received an
   `Acq(i)` from `c_i`. This is plain Java memory, not a message event, and it adds no events.
   Clients get the server's Tid from main, which spawns the server before any client.
3. **Two channels merged into one mailbox (spec lock).** Under P2P, per-sender FIFO across the merged
   mailbox could in principle order an Acq against a Rel from the same client. That cannot matter
   here: the lock receives client i's Rel before client i can send its next Acq, because the next
   Acq follows a Grant that only comes after the previous Rel was received. I assumed type-guarded
   selective receive is the intended rendering of "per-thread channels" in this situation.
4. **`size()`** returns `"<k> <q>"`. I do not know the exact size text the Rust shared main writes
   to the header. It might be `k<k>q<q>` (the fixture name) or the raw arguments.
5. **The variant is optional for spec.** Rust's parser always requires 3 arguments
   (`<variant> <k> <q>`, variant first). I put the variant last, following the brief's launcher
   format.
6. `usize` becomes `long` in the records. Indices are `int` locally.

## Counts measured (`must 0 1 report - 600`, plain enumeration)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,1,fifo,fifo (smallest) | 1 | 0 | stack exhausted |
| impl,1,1,fifo,fifo (smallest) | 1 | 0 | stack exhausted |
| spec,2,1,fifo,fifo | 2 | 0 | stack exhausted |
| impl,2,1,fifo,fifo | 4 | 0 | stack exhausted |
| impl,2,1,fifo,double-grant | 6 | 0 | stack exhausted |
| impl,2,1,fifo,never-grant | 3 | 1 | stack exhausted |
| impl,2,1,fifo,wrong-round | 4 | 0 | stack exhausted |
| impl,2,1,fifo,control-lifo | 4 | 0 | stack exhausted |
| spec,2,2,fifo,fifo | 6 | 0 | stack exhausted |
| impl,2,2,fifo,fifo | 28 | 0 | stack exhausted |
| spec,1,1,bag,fifo | 1 | 0 | stack exhausted |
| impl,1,1,bag,fifo | 1 | 0 | stack exhausted |
| spec,2,1,bag,fifo | 2 | 0 | stack exhausted |
| impl,2,1,bag,fifo | 4 | 0 | stack exhausted |

I did not look up the Rust counts and did not tune anything to match them. `java/out` and `report/`
have been removed.
