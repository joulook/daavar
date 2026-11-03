# memcached-oopsla25: context-free Java twin

Source of truth: `daavar/benchmarks/memcached-oopsla25/rust/src/main.rs` (+ Cargo.toml).
Written from that file, the JMC Must API sources listed in the brief, and SUMMARY-FORMAT.md
("Values", "What both programs must satisfy") only.

- Class: `bench.memcached_oopsla25.Memcached` (`java/bench/memcached_oopsla25/Memcached.java`).
- Arguments: `<spec|impl>,<Q>,<fifo|bag>[,<variant>]`; Q in 1..=3; variant `v3` (default),
  `shared-bucket`, `lost-set`. The variant is validated for `spec` too (as Rust `parse` does) and ignored there.
- Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- `programName()` = `memcached-oopsla25`; `size()` = `Q` (e.g. `1`).

## Threads (spawn order, as in Rust)

| Rust | Java role | body |
|---|---|---|
| spec: `k0`, `k1` (`serve("k", k, "c", n)`) | `k0`, `k1` | `serve("c", n)`: n blocking receives of `Cmd`, apply to a private 2-key store, send `Res` to `c<c>` |
| spec: `c0`, `c1` (`client(i, q, true)`) | `c0`, `c1` | send `Cmd{i, SCRIPT[i][j]}` to `k<key>`, blocking receive of `Res`, q times |
| impl: `b0`, `b1` (`b0` only for shared-bucket) (`serve("b", b, "w", n)`) | `b0`[, `b1`] | `serve("w", n)`: answers to `w<c>` |
| impl: `w0`, `w1` (handle_client) | `w0`, `w1` | q times: recv `Cmd`, send `Cmd` to `b<route(op)>`, recv `Res`, send `Res` to `c<w>` |
| impl: `c0`, `c1` (`client(i, q, false)`) | `c0`, `c1` | as spec, but sends to `w<i>` |

n per server = number of the first Q commands of both clients with that key (spec) / that route (impl),
computed as in Rust `count`. Main spawns all (servers/handlers first, then clients), awaits clients
then servers/handlers, and does no message-passing event. Visible roles `c0`, `c1`;
`rolesInSpawnOrder()` lists every spawned role (all are named, as in Rust).

Per-thread channels: each Rust named channel `(role, i)` is the mailbox of process `role+i`. All
receives are blocking and unguarded (`Channel.recv(model)`), as `recv_msg_block()`; the Rust
`let ... else { unreachable!() }` is mirrored by an `IllegalStateException` (never fires).
No `Nondet`: the Rust program makes no nondeterministic choice.

## Messages

| Rust | Java (tag, fields) |
|---|---|
| `Msg::Cmd { c: u8, op: Op }` | `Msg.Cmd(int c, Op op)` `@Tag("Cmd")` |
| `Msg::Res(Res)` | `Msg.Res(Response res)` `@Tag("Res")` |
| `Op::Get { key }` | `Op.Get(int key)` `@Tag("Get")` |
| `Op::Set { key, value }` | `Op.Set(int key, int value)` `@Tag("Set")` |
| `Res::Stored` | `Response.Stored()` `@Tag("Stored")` |
| `Res::Got { key, value: Option<u8> }` | `Response.Got(int key, Optional<Integer> value)` `@Tag("Got")` |

`Msg` is the sealed `MpMessage`; `Op` and `Response` are sealed interfaces of records (payload
values, not messages). The Rust type `Res` is named `Response` in Java to avoid a clash with the
`Msg.Res` record; only the tags matter for the format.

## Doubts / assumptions

1. **Tids before spawn.** Servers reply to clients spawned after them, so a Tid must be known before
   its process exists. I compute it from spawn order: `base = MpProcess.self().raw() + 1` (read by
   main before any spawn; no event), process k = `Tid.of(base + k, name)`, and check at every
   spawn that the returned Tid equals it (fails loudly otherwise). The MpProgram javadoc says
   "the k-th process main spawns is task k + 1", but the runtime gave main id 1 and the first
   process id 2, so I used main's id + 1 rather than a constant.
2. **`Optional` in the encoder.** The brief asks for `Optional` for `Option`, but the
   `MpValueEncoder` source I read has no `Optional` case (it would refuse it as "not encodable").
   Counting mode does not encode, so it does not matter for the check below; an export of a
   graph containing `Got` may be refused unless the exporter unwraps `Optional` elsewhere (not read).
   How Rust encodes `Option` (`null`/value or a tagged `Some`/`None`) is not stated in the
   sections I was allowed to read.
3. **Nested enum encoding.** I assumed `Op`/`Res` inside `Msg` are encoded as tagged records
   (`{"t":"Get","f":[1]}`) and `Msg::Res(Res)` as a one-field record — per "every enum variant".
4. **Integers** `u8` -> `int`.
5. **Role list.** I declared the invisible roles (`k*`, `b*`, `w*`) in `rolesInSpawnOrder()`, since
   the launcher's javadoc says the spawned roles must equal that list; visible roles are `c0`, `c1` only.
6. `size()` is just `Q`; the header's expected size text (whether it includes the variant) is unknown to me.

## Counts measured (must, plain enumeration, `must 0 1 report - 600`)

| args | result |
|---|---|
| `spec,1,fifo` | 1 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,fifo` | 1 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,1,bag` | 1 complete, 0 blocked; exhausted |
| `impl,1,bag` | 1 complete, 0 blocked; exhausted |
| `impl,1,fifo,shared-bucket` | 2 complete, 0 blocked; exhausted |
| `impl,1,fifo,lost-set` | 1 complete, 0 blocked; exhausted |
