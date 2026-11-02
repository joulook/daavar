# fencing-grove: context-free Java twin

Written from `daavar/benchmarks/fencing-grove/rust/src/main.rs` (+ Cargo.toml), the JMC Must API
sources, MpProgram/MpLauncher, the summary package, and SUMMARY-FORMAT.md ("Values", "What both
programs must satisfy") only.

- Class: `bench.fencing_grove.FencingGrove` (`java/bench/fencing_grove/FencingGrove.java`),
  implements `MpProgram`, public `(String...)` constructor.
- Argument string: `<spec|impl>,<K>,<fifo|bag>[,<variant>]`; `K` = FetchAndIncrement calls per
  client; variant `fenced` (default) or `stale-frontend` (impl only; ignored on the spec side, as
  the Rust `parse` ignores it there). `fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN`.
- Visible roles: `c0`, `c1`. Main is invisible, spawns every process, then awaits all of them.

## Threads (spawn order = Rust spawn order)

| Rust | Java role | Notes |
|---|---|---|
| spec `server` | `server` | 2K times: blocking recv `Fai`, send `Val(v)` to `from`, `v += 1` |
| `c0`, `c1` (`spawn_clients`) | `c0`, `c1` | K times: send `Fai(self, 0)` to target, blocking recv `Val` |
| impl `config` | `config` | infinite loop; blocking recv `Acquire` or `CfgGet`; `Nondet.chooseBoolean()` exactly at the Rust `nondet()` (evaluated only when `grant` is none, `active`, and `acquirers` non-empty: Java `&&` short-circuits like Rust) |
| impl `ctr` | `ctr` | infinite loop; blocking recv `CtrGet` or `CtrPut`; stale if `epoch < last` |
| `fe1`, `fe2` | `fe1`, `fe2` | send `Acquire`, recv `Epoch`, then infinite loop serving `FeFai` (with `exited` / `fenced` logic as in Rust) |
| `p0`, `p1` | `p0`, `p1` | `CfgGet`/`Current`, then K times: recv `Fai`, up to 2 attempts `FeFai` -> recv `FeVal` or `FeErr`; on `FeErr` re-ask config; `assert!(done)` -> `throw new AssertionError(...)` |

Spec spawn order: server, c0, c1. Impl spawn order: config, ctr, fe1, fe2, p0, p1, c0, c1. Await
order as in Rust: clients, clerks, front ends, ctr, config.

All receives are blocking (the Rust program has only `recv_tagged_msg_block`). Each Rust tag
filter `t == Some(TAG_X)` is a type test on the record class `Msg.X` (`Channel.recv(model, Class)`;
for two tags a guard `a.isInstance(m) || b.isInstance(m)`). Each thread has its one mailbox, as in
Rust (no extra channels).

## Messages (sealed interface `FencingGrove.Msg`, one record per Rust variant)

| Rust variant | Java record (`@Tag`) |
|---|---|
| `Fai { from: ThreadId, key: u8 }` | `Fai(Tid from, int key)` |
| `Val(u8)` | `Val(int value)` |
| `CfgGet { from }` | `CfgGet(Tid from)` |
| `Current(ThreadId)` | `Current(Tid fe)` |
| `Acquire { from }` | `Acquire(Tid from)` |
| `Epoch(u8)` | `Epoch(int epoch)` |
| `FeFai { from, key }` | `FeFai(Tid from, int key)` |
| `FeVal(u8)` | `FeVal(int value)` |
| `FeErr` (unit) | `FeErr()` |
| `CtrGet { from, epoch }` | `CtrGet(Tid from, int epoch)` |
| `GetRep { stale: bool, val: u8 }` | `GetRep(boolean stale, int val)` |
| `CtrPut { from, v, epoch }` | `CtrPut(Tid from, int v, int epoch)` |
| `PutRep { stale: bool }` | `PutRep(boolean stale)` |

`config`'s `data: Option<ThreadId>` is a local `Optional<Tid>` (never sent as such).

## Doubts / assumptions

1. **Tag names**: I used the Rust enum variant names (`Fai`, `Val`, ...) as `@Tag` values. The Rust
   program also has numeric `TAG_*` constants (used only for receive selection); I assumed the
   summary encoder on the Rust side tags values by variant name, not by those numbers. Not verified.
2. **Field names in tuple variants** (`Val(u8)` -> `value`, `Current` -> `fe`, `Epoch` -> `epoch`):
   names are mine; only order matters for the encoding.
3. **u8 -> int**: Rust u8 arithmetic would panic on overflow (debug) at values > 255; Java `int` does
   not. Irrelevant at small K.
4. **The implementation never completes**: `config`, `ctr`, `fe1`, `fe2` are infinite blocking
   loops spawned as ordinary (non-daemon) processes and awaited by main, as in Rust (`named` spawn +
   `join`; the comment says "it ends blocked"). So every impl execution ends blocked. I kept that
   rather than using `spawnDaemon`, since the Rust program does not use `spawn_daemon`.
5. **`programName()` = `fencing-grove`, `size()` = `K` only** (the variant is not in the size). The
   Rust side's header values were not visible to me; guessed.
6. **Invalid arguments** (unknown variant etc.) throw in the constructor; Rust panics inside the
   impl closure instead (at run time).
7. **Main's process spawn**: `MpProcess.spawn(role, body)`; the program does not call
   `MpProcess.runMain` (no daemons).
8. `nondet()` -> `Nondet.chooseBoolean()` (outcomes {true, false}, min = true); I assumed
   TraceForge's `nondet()` is the same boolean choice.

## Measured (must, plain enumeration, K = 1, `must 0 1 report - 600`)

| args | result | exit |
|---|---|---|
| `spec,1,fifo` | `graphs: 2 distinct complete, 0 distinct blocked; stack exhausted` | 0 |
| `impl,1,fifo` | `graphs: 0 distinct complete, 164 distinct blocked; stack exhausted` | 0 |
| `impl,1,fifo,stale-frontend` (extra) | `graphs: 0 distinct complete, 176 distinct blocked; stack exhausted` | 0 |

Not compared with any Rust count (per the brief).
