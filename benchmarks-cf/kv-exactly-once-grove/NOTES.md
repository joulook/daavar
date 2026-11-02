# kv-exactly-once-grove: context-free Java twin

Class: `bench.kv_exactly_once_grove.KvExactlyOnceGrove` (one file,
`java/bench/kv_exactly_once_grove/KvExactlyOnceGrove.java`), public `(String...)` constructor.

Arguments: `<spec|impl>,<keys>,<fifo|bag>[,<variant>]`, e.g. `impl,1,bag,no-request-id`.
`fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`. Variants (impl only):
`exactly-once` (default), `no-request-id`; anything else throws (Rust panics). The spec side
accepts and ignores a variant, as Rust's `spec` ignores it.

## Threads (spawn order = Rust order)

| Rust | Java role | visible | body |
|---|---|---|---|
| main | (main) | no | spawns, then awaits c0, c1, [p0, p1,] server |
| spec `server` | `server` | no | N*keys times: blocking recv Op, execute, send Res to `from` |
| impl `server` | `server` | no | infinite loop: blocking recv; Fresh -> FreshId(next++); Rpc -> cached lastReplies[opId] or execute (+record if opId), send Reply |
| impl `p0`,`p1` (clerks) | `p0`,`p1` | no | per key: recv Op; if with ids: send Fresh, recv FreshId; send Rpc; recv Reply; if drops>0 && nondet: drops--, resend Rpc, recv Reply (replaces r); send Res to client. drops = 1 at p0, 0 at p1 |
| `c0`,`c1` | `c0`,`c1` | yes | per key k: send Op(me, script(i,k)) to front[i]; blocking recv (any) |

Spec: server, c0, c1 (front = server for both). Impl: server, p0, p1, c0, c1 (front[i] = p_i).
`rolesInSpawnOrder()` = those lists; `visibleRoles()` = [c0, c1].
The impl server never terminates, so it ends blocked and main's await of it blocks: every impl
execution ends blocked, as in Rust (comment: "ends blocked when the clerks are done").

## Messages

Every receive is a blocking `Channel.recv(model, Msg.class)` (type test on the Msg type, the
analogue of `recv_msg_block::<Msg>()`); the Rust `let Msg::X .. = .. else { unreachable!() }`
become Java casts (ClassCastException in place of the panic). No non-blocking receives.

| Rust | Java record (@Tag) | fields |
|---|---|---|
| `KvOp::CondPut{key,expected,new}` | `CondPut` ("CondPut") | int key, int expected, int newVal |
| `KvOp::Get{key}` | `Get` ("Get") | int key |
| `KvRes::Ok(bool)` | `Ok` ("Ok") | boolean ok |
| `KvRes::Val(u8)` | `Val` ("Val") | int val |
| `Msg::Op{from,op}` | `Op` ("Op") | Tid from, KvOp op |
| `Msg::Res(KvRes)` | `Res` ("Res") | KvRes res |
| `Msg::Fresh{from}` | `Fresh` ("Fresh") | Tid from |
| `Msg::FreshId(u8)` | `FreshId` ("FreshId") | int id |
| `Msg::Rpc{from,op_id,op}` | `Rpc` ("Rpc") | Tid from, Optional<Integer> opId, KvOp op |
| `Msg::Reply(KvRes)` | `Reply` ("Reply") | KvRes res |

`KvOp`, `KvRes` and `Msg` are sealed interfaces (only `Msg` extends `MpMessage`).
Nondet: `Nondet.chooseBoolean()` at the one Rust `nondet()` (clerk, after the first Reply,
short-circuited by `drops > 0`), outcomes {true, false}; true = the reply is lost.

## Counts measured (`must 0 1 report - 600`, keys = 1)

| args | result |
|---|---|
| `spec,1,bag` | 2 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,bag` | 0 distinct complete, 16 distinct blocked; stack exhausted |
| `impl,1,bag,no-request-id` | 0 distinct complete, 5 distinct blocked; stack exhausted |
| `spec,1,fifo` | 2 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,fifo` | 0 distinct complete, 16 distinct blocked; stack exhausted |
| `impl,1,fifo,no-request-id` | 0 distinct complete, 5 distinct blocked; stack exhausted |

Not compared with any Rust count (the brief forbids it).

## Doubts and assumptions

1. `Optional<Integer>` for `op_id: Option<u8>` as the brief prescribes, but `MpValueEncoder`
   (as read) has no case for `Optional` and would refuse it in an export; the plain count does not
   encode values, so the runs above are unaffected. Export of `Rpc` messages may fail until the
   encoder handles `Optional` (or the encoding of None/Some must be agreed).
2. Tag names are the bare Rust variant names; `Ok`/`Val` (KvRes) and `CondPut`/`Get` (KvOp) are
   nested enums, encoded as tagged records too. I assumed the Rust encoder tags nested enum
   variants by variant name the same way. Field `new` is renamed `newVal` (Java keyword); position
   is unchanged.
3. Values are `u8` in Rust; `int` here. Values 0/1 stand for "" and "1" as in Rust.
4. `programName()` = `kv-exactly-once-grove` (the benchmark directory name); I do not know what
   the Rust side writes in the header (crate name is `bench_kv_exactly_once_grove`).
5. Rust `parse` defaults keys to 1 when absent; the Java argument string requires keys, so no
   default is implemented.
6. Smallest size taken as keys = 1 (keys = 0 would have no messages).
7. Clerks p0/p1 and the server are given roles (their Rust thread names) although invisible; the
   `MpProgram` contract says every spawned role is declared, so they are in
   `rolesInSpawnOrder()`. A clerk's Tid travels in `Fresh`/`Rpc`, a client's in `Op`.
8. Every impl execution ends blocked (the server's infinite loop); I kept this rather than
   bounding the server, since Rust does the same.
