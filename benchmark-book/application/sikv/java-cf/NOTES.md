# kv-snapshot-isolation-tla: context-free Java twin

Class: `bench.kv_snapshot_isolation_tla.KvSnapshotIsolation` (implements `MpProgram`, public
`(String...)` constructor). Messages: `bench.kv_snapshot_isolation_tla.Msg` (sealed interface).

Argument string: `<spec|impl>,<n>,<fifo|bag>[,<si|serializable>]`, e.g.
`bench.kv_snapshot_isolation_tla.KvSnapshotIsolation:impl,2,fifo,serializable`.
`n` = clients = keys (Rust `-- <n> [variant]`); variant default `si`; `fifo` -> `P2P`, `bag` -> `ASYN`.

## Mapping

Threads (spawn order as in Rust; main is invisible, spawns everything, then awaits clients, then
the rest in Rust's join order):
- spec: `server`, then `c0..c{n-1}`. Server loops n times: blocking recv of `Txn`, then
  `Nondet.chooseBoolean()` where Rust calls `nondet()`; `true` = rollback (`Result(false,0)`),
  `false` = execute (read `store[rk]`, write `store[wk]=v`, `Result(true,r)`).
- impl: `store`, then `t0..t{n-1}`, then `c0..c{n-1}`. Store loops 2n times on a blocking recv
  whose guard accepts `Begin` or `Commit` (type test, the Rust tag test `TAG_BEGIN || TAG_COMMIT`);
  `tx` and `missed` kept as in Rust (missed as two parallel lists id / boolean[n]). `t_i`: blocking
  recv `Txn`, send `Begin(me)`, blocking recv `Snap`, local read/update, send `Commit(me,rk,wk,v)`,
  blocking recv `CRes`, send `Result(ok, ok ? r : 0)` to the client.
- client `c_i`: send `Txn(me, (i+1) mod n, i, i+1)` to its front (server, or `t_i`), blocking recv
  `Result`.
- Visible roles: `c0..c{n-1}` (Rust `visible`). All other processes get a role name too (the Rust
  thread names `server`, `store`, `t{i}`), because `Begin.from`/`Commit.from` carry a `t_i` Tid
  that the encoder must name, and `rolesInSpawnOrder` lists every spawned role.

Messages (Rust `enum Msg` -> records, tag = variant name, same field order; `u8` -> `int`,
`ThreadId` -> `Tid`, `Vec<u8>` -> `List<Integer>`, `bool` -> `boolean`):
`Txn(Tid from,int rk,int wk,int v)`, `Result(boolean ok,int read)`, `Begin(Tid from)`,
`Snap(List<Integer> store)`, `Commit(Tid from,int rk,int wk,int v)`, `CRes(boolean ok)`.

Receives: all blocking (Rust uses only `recv_tagged_msg_block`); each Rust tag filter becomes a
type guard on the corresponding record. Each process receives on its own (per-thread) mailbox, as
in Rust.

## Doubts / assumptions

- `programName()` = `kv-snapshot-isolation-tla` and `size()` = `"<n>"` (variant not included):
  guessed; I could not see what the Rust side writes in the header.
- Field name of `Snap`'s single tuple field (`store`) is arbitrary; the encoding uses only order.
- The Rust spec ignores the variant; the Java spec accepts and ignores any variant. Impl rejects
  an unknown variant at construction (Rust panics when the program runs).
- Nondet order: `chooseBoolean()` has min = `true` = rollback; I assumed Rust's `nondet()` has
  the same two outcomes (order should not affect the set of graphs).
- The Rust tags are numeric (`TAG_TXN=1`...); the Java `@Tag` uses the variant names, as the brief
  asks; I assumed the summary tags on the Rust side are the variant names too.
- n >= 1 required; Rust's default n=2 when no size is given is not reproduced (size is mandatory
  in the launcher argument).

## Counts measured (`must 0 1 report - 600`)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,fifo | 2 | 0 | stack exhausted |
| impl,1,fifo | 1 | 0 | stack exhausted |
| spec,1,bag | 2 | 0 | stack exhausted |
| impl,1,bag | 1 | 0 | stack exhausted |
| spec,2,fifo | 8 | 0 | stack exhausted |
| impl,2,fifo | 6 | 0 | stack exhausted |
| impl,2,fifo,serializable | 6 | 0 | stack exhausted |
