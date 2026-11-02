# reliable-channel-aneris: context-free Java twin

Written from `daavar/benchmarks/reliable-channel-aneris/rust/src/main.rs` and the JMC Must API
sources only (brief: plan/twins-contextfree/BRIEF.md).

- Class: `bench.reliable_channel_aneris.ReliableChannelAneris` (implements `MpProgram`), file
  `java/bench/reliable_channel_aneris/ReliableChannelAneris.java`.
- Arguments (launcher `(String...)` constructor): `<spec|impl>,<K>,<fifo|bag>[,<variant>]`,
  variant `correct` (default) or `no-seqno`. The spec side accepts and ignores the variant, as the
  Rust `parse` does (it validates it but the spec closure never uses it).
- Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`. Every send and
  receive uses that one model.
- `programName()` = `reliable-channel-aneris`; `size()` = `K` as decimal text.

## Threads

| Rust | Java role | visible | body |
|---|---|---|---|
| main (`run`) | main | no | creates and starts ep_s, ep_r, sender, receiver in that order; awaits them in that order |
| `ep_s` (`spec_ep_s` / `impl_ep_s`) | `ep_s` | no | `specEpS` / `implEpS` (+ `xmit`) |
| `ep_r` (`spec_ep_r` / `impl_ep_r` / `impl_ep_r_no_seqno`) | `ep_r` | no | `specEpR` / `implEpR` (`seqno` flag) |
| `sender` | `sender` | yes | `sender` |
| `receiver` | `receiver` | yes | `receiver` |

`rolesInSpawnOrder()` = [ep_s, ep_r, sender, receiver]; `visibleRoles()` = [sender, receiver].

Per-thread inboxes: Rust has one channel per thread (indices SND 0, RCV 1, EPS 2, EPR 3), each read
only by its owner with `recv_msg_block`. Java: each process receives at itself with a blocking
unguarded `Channel.recv(model)` (no guard; the Rust side takes any `Msg` and then panics on the
wrong variant, which Java mirrors with `IllegalStateException` after the receive). All receives
are blocking; there are no non-blocking receives in the Rust program.

## Messages (sealed interface `Msg`, field order as in Rust)

| Rust variant | Java record | tag |
|---|---|---|
| `Send { v: u8 }` | `Send(int v)` | `Send` |
| `Queued` | `Queued()` | `Queued` |
| `Recv` | `Recv()` | `Recv` |
| `Deliver { v: u8 }` | `Deliver(int v)` | `Deliver` |
| `Data { seq: u8, v: u8 }` | `Data(int seq, int v)` | `Data` |
| `Ack { ack: u8 }` | `Ack(int ack)` | `Ack` |

## Nondeterminism

Only in impl `ep_s`'s `xmit`, at the same two places: the loss loop
`while losses > 0 && nondet()` and the duplicate test `if dups > 0 && nondet()`, each
`Nondet.chooseBoolean()` (outcomes {true, false}), evaluated only when the budget is positive
(short-circuit as in Rust). L = 1, D = 1, shared over the whole run of ep_s.

## Doubts and assumptions

1. **Process ids before spawn.** Rust builds all four channels before spawning, so every thread
   holds every sender. `MpProcess.spawn` returns a Tid only after the process is started, so a
   process spawned earlier could not know a later one's id. I therefore construct all four
   `MpThread(body, role)` first (task id minted at construction, `TASK_CREATED_EVENT` raised
   then), fill a `Tid[]` with `Tid.of(taskId, role)`, and only then `start()` them in the Rust
   spawn order, awaiting with `join1()`. Creation order and start order equal the Rust spawn order,
   but the four creation events all precede the first start (in Rust each spawn creates and starts
   together). I did not use `MpProcess.spawn`/`Handle` for this reason. If the harness requires
   `MpProcess.spawn`, the alternative is relying on "the k-th process main spawns is task k+1"
   (MpProgram javadoc) to compute the ids.
2. **`nondet()` order.** `Nondet.chooseBoolean()` lists {true, false} (min = true). I do not know
   TraceForge's internal order for `nondet()`; the outcome set is the same.
3. **No `ARGS:` comment** in main.rs; sizes/variants taken from the module doc and `parse`
   (K, default 2 in Rust when missing; Java requires K). Model default in Rust is `bag`.
4. **`programName`/`size` strings** for the summary header are my guess (directory name; K as text).
5. `u8` fields as `int`; no overflow at the sizes run.
6. Wrong-variant panics become `IllegalStateException` (not `AssertionError`); they are unreachable
   in this protocol.
7. Invisible ep_s/ep_r are given roles (their Rust thread names), so they appear in
   `rolesInSpawnOrder` but not in `visibleRoles`.

## Counts measured (strategy must, iterations 0, no export; all "stack exhausted")

| args | complete | blocked |
|---|---|---|
| spec,1,bag | 2 | 0 |
| impl,1,bag | 20 | 0 |
| impl,1,bag,no-seqno | 20 | 0 |
| spec,1,fifo | 2 | 0 |
| impl,1,fifo | 10 | 0 |
| impl,1,fifo,no-seqno | 10 | 0 |
| spec,2,bag | 8 | 0 |
| impl,2,bag | 882 | 0 |
| impl,2,bag,no-seqno | 614 | 90 |
| spec,2,fifo | 5 | 0 |
| impl,2,fifo | 165 | 0 |
| impl,2,fifo,no-seqno | 150 | 15 |

Not compared against the Rust counts (brief).
