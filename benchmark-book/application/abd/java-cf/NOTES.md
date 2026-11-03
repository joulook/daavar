# abd-register-civl: context-free Java twin

Written from `daavar/benchmarks/abd-register-civl/rust/src/main.rs` and the JMC Must API sources only.

- Class: `bench.abd_register_civl.AbdRegisterCivl` (one file, `java/bench/abd_register_civl/AbdRegisterCivl.java`).
- Argument string: `<spec|impl>,<replicas>,<fifo|bag>[,<correct|read-no-writeback>]`, e.g.
  `bench.abd_register_civl.AbdRegisterCivl:impl,1,bag,read-no-writeback`. Variant defaults to `correct`.
  `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- `programName()` = `abd-register-civl`; `size()` = the replica count as given (e.g. `"1"`); the variant is not in `size()`.
- Visible roles: `w`, `r1`, `r2`. Roles declared: spec `register, w, r1, r2`; impl `rep0..rep{n-1}, wp, rp1, rp2, w, r1, r2`.

## Threads (Rust spawn order kept)

| Rust thread | Java role | Body |
|---|---|---|
| `register` (spec) | `register` | 3x blocking recv of an `Op`; `Write(v)`: value = v, send `WriteAck` to `w`; `Read(i)`: send `ReadReply(value)` to client i (1 = r1, 2 = r2) |
| `rep{i}` (impl) | `rep{i}` | `per_replica` (6 correct / 4 read-no-writeback) blocking recvs of a `ToReplica`; `Query(p)` -> `QueryReply(store.ts, store.v)` to proxy p; `Update(p, ts, v)` -> store if `store.ts < ts`, `UpdateAck` to proxy p |
| `wp`, `rp1`, `rp2` (impl) | same | one blocking recv of an `Op`; Write: QueryPhase, UpdatePhase with `Ts(max.t + 1, 1)`, `WriteAck` to its client; Read: QueryPhase, UpdatePhase(max) only in `correct`, `ReadReply(max.v)` to its client |
| `w` | `w` | send `Write(1)`; blocking recv of a `Reply` |
| `r1` | `r1` | send `Read(1)`; blocking recv of a `Reply`; send `Done` to `r2` |
| `r2` | `r2` | blocking recv of `Done`; send `Read(2)`; blocking recv of a `Reply` |

QueryPhase: `Query(me)` to every replica in index order, then `maj = n/2+1` blocking recvs of
`QueryReply`, keeping the reply with strictly larger `Ts` (lexicographic), starting from `(Ts(0,0), 0)`.
UpdatePhase: `Update(me, ts, v)` to every replica in index order, then `maj` blocking recvs of `UpdateAck`.
No `Nondet` anywhere (the Rust program makes no `nondet` choice). All receives are blocking, as in Rust.

## Messages

One sealed interface `Msg` (extends `MpMessage`) with sealed sub-interfaces mirroring the Rust types:

| Rust | Java record | Tag | Fields |
|---|---|---|---|
| `Op::Write(i32)` | `Write` (in `Op`) | `Write` | `int v` |
| `Op::Read(usize)` | `Read` (in `Op`) | `Read` | `int i` |
| `Reply::WriteAck` | `WriteAck` (in `Reply`) | `WriteAck` | none |
| `Reply::ReadReply(i32)` | `ReadReply` (in `Reply`) | `ReadReply` | `int v` |
| `struct Done` | `Done` | `Done` | none |
| `ToReplica::Query(usize)` | `Query` (in `ToReplica`) | `Query` | `int p` |
| `ToReplica::Update(usize, Ts, i32)` | `Update` (in `ToReplica`) | `Update` | `int p, Ts ts, int v` |
| `struct QueryReply(Ts, i32)` | `QueryReply` | `QueryReply` | `Ts ts, int v` |
| `struct UpdateAck` | `UpdateAck` | `UpdateAck` | none |
| `struct Ts(i32, i32)` | `Ts` (record, not a message) | `Ts` | `int t, int pid` |

## Channels -> mailboxes

Every Rust `Receiver` is owned by exactly one thread, so each Rust channel becomes the receiving
thread's mailbox, and a receive on a channel becomes a blocking receive guarded by a type test on
that channel's Rust type (`Op`, `Reply`, `Done`, `ToReplica`, `QueryReply`, `UpdateAck`). Threads
with more than one channel: proxies (`Op`, `QueryReply`, `UpdateAck`) and `r2` (`Done`, `Reply`);
the message types on those channels are disjoint, so the guard picks the channel exactly.

## Doubts / assumptions

1. **Addressing before spawn.** The Rust program creates all channels in main before spawning, and
   replicas are spawned before the proxies they reply to, proxies before the clients they reply to.
   JMC addresses by `Tid`, which `MpProcess.spawn` returns only after starting the process. I therefore
   construct every `MpThread(body, role)` first (minting all task ids, `TASK_CREATED` events in spawn
   order), fill the `Tid` arrays the bodies capture, then `start()` all in Rust spawn order, then
   `join1()` all (what `MpProcess.Handle.await` calls). This uses the public `MpThread` class directly
   instead of `MpProcess.spawn`; the creation events thus all precede the first start, rather than
   alternating create/start. An alternative (spawn with `MpProcess.spawn` and fill a shared holder after
   each spawn) was rejected as a possible race if a process runs before main stores a `Tid`.
2. **P2P vs per-channel FIFO.** Rust `fifo` orders per channel; JMC `P2P` orders per sender->receiver
   mailbox. A replica sends a `QueryReply` and later an `UpdateAck` to the same proxy on two Rust
   channels; in Java they share a P2P mailbox. I assumed P2P ordering is guard-aware (an unread
   `QueryReply` from a replica does not stop the proxy's `UpdateAck` receive). Not verified in the
   strategy source (not on the reading list).
3. `usize` fields (`Read`, `Query`, `Update` proxy index) encoded as `int`.
4. `size()` returns only the replica count; the variant is not part of the size text. Spec ignores
   the replica count and the variant (as the Rust spec does), but still parses them.
5. Invisible processes are given role names (the Rust thread names) and listed in
   `rolesInSpawnOrder()`; only `w, r1, r2` are visible. The export at n=1 was accepted with this.
6. Rust defaults the size to 3 when absent; the Java argument string requires it.

## Counts measured (`must 0 1 report - 600`, plain enumeration)

| args | result |
|---|---|
| `spec,1,bag` | 3 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,1,fifo` | 3 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,bag` | 15 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,fifo` | 15 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,bag,read-no-writeback` | 6 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,fifo,read-no-writeback` | 6 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,3,bag` (timeout 280 s) | 3 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,3,bag` (timeout 280 s) | 14384 distinct complete, 0 blocked; stack NOT exhausted |
