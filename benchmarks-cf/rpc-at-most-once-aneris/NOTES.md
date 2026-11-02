# rpc-at-most-once-aneris: context-free Java twin

Class: `bench.rpc_at_most_once_aneris.RpcAtMostOnce` (java/bench/rpc_at_most_once_aneris/RpcAtMostOnce.java)

Arguments: `<spec|impl>,<K>,<fifo|bag>[,<dedup|no-dedup>]` — K = operations per client (2 clients);
fifo -> P2P, bag -> ASYN; variant defaults to `dedup` and is only used by impl (spec accepts and
ignores it, as the Rust spec ignores it).

## Mapping

Threads (spawn order as in Rust, all spawned by main with `MpProcess.spawn(role, ...)`, all awaited):
- spec: `server`, `c0`, `c1`; await c0, c1, server.
- impl: `server`, `p0`, `p1`, `c0`, `c1`; await c0, c1, p0, p1, server.
- visible: `c0`, `c1` (Rust `visible`). main invisible, no message events.
- client `ci`: for j < K: send `Op{from=self, op=script(i,j)}` to its front (server in spec, `pi`
  in impl); blocking receive of a `Res`. script: even j -> `Wr(0, i+1)`, odd j -> `Rd(0)`.
- spec server: N*K times: blocking receive `Op`, apply to db[1], send `Res` to `from`.
- impl server: infinite loop: blocking receive `Req`; dedup ctable (list of (from, rid, res))
  with stale-ignore, cached-resend, `rid == t+1` assertion, replace entry; no-dedup applies always.
- proxy `pi`: for rid < K: blocking receive `Op`; send `Req{op,rid,me}` to server; if
  `dups > 0 && Nondet.chooseBoolean()` resend the same Req (only p0 has dups=1); then up to
  sentDups+1 blocking receives of `Rep`, assert `id <= rid`, stop at `id == rid`; send `Res` to the
  client (assert a result was found = Rust `res.unwrap()`).

Messages (sealed `Msg extends MpMessage`, `@Tag` = Rust variant name, Rust field order):
- `Op` -> `OpMsg(Tid from, Op op)`; `Res` -> `ResMsg(Res res)`; `Req` -> `Req(Op op, int rid, Tid from)`;
  `Rep` -> `Rep(Res res, int rid)`.
- nested `Op`: `@Tag("Wr") Wr(int key, int val)`, `@Tag("Rd") Rd(int key)`;
  nested `Res`: `@Tag("Ack") Ack()`, `@Tag("Val") Val(int v)`. u8 -> int.
- Rust receives by numeric tag (TAG_OP/RES/REQ/REP), each tag carrying exactly one Msg variant;
  Java receives by record type (`Channel.recv(model, X.class)`), a type-test guard. All receives
  blocking, any sender, one mailbox per thread, as in Rust.

## Doubts / assumptions

1. The impl server loops forever, so (as the Rust comment says) every impl execution ends with the
   server blocked on a receive and main blocked awaiting it: JMC counts every impl graph as
   blocked (0 complete). I kept the Rust structure (plain spawn + join, not a daemon). If
   TraceForge counts these differently, this is where the counts would diverge.
2. Rust `nondet()` vs `Nondet.chooseBoolean()` (min = true): same outcome set {true,false}; order unknown.
3. Rust `assert!` -> explicit `throw new AssertionError` (Java `assert` is off without -ea).
   Rust `let ... else unreachable!()` -> typed receive (cannot fail).
4. `programName()` = "rpc-at-most-once-aneris" and `size()` = "K" are guesses; the Rust program
   does not show how its header name/size are formed.
5. The `Res::Val(u8)` tuple field is named `v` in Java (the encoding is positional, so the name is irrelevant).
6. "Smallest size" taken as K=1. The Rust has no `ARGS:` line; I used its usage comment.

## Measured (`must 0 1 report - 600`, all "stack exhausted")

| args | complete | blocked |
|---|---|---|
| spec,1,bag | 2 | 0 |
| impl,1,bag (dedup) | 0 | 14 |
| impl,1,bag,no-dedup | 0 | 14 |
| spec,1,fifo | 2 | 0 |
| impl,1,fifo | 0 | 5 |
| spec,2,bag | 6 | 0 |
| impl,2,bag (dedup) | 0 | 146 |
| impl,2,bag,no-dedup | 0 | 166 |

No assertion failure in any of these runs.
