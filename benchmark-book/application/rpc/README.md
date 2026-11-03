# rpc(K) — at-most-once RPC (`rpc-at-most-once-aneris`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: the duplicate must be able to overtake later requests); the program declares
bag as its default `--model`, and the campaign ran bag. **Languages:** Rust (TraceForge's Must API), in `rust/`;
Java twin in `java/` (`bench.rpc_at_most_once_aneris.AtMostOnceRpc`, args
`<spec|impl>,<K>[,<fifo|bag>[,<variant>]]`); context-free Java port in `java-cf/`
(`bench.rpc_at_most_once_aneris.RpcAtMostOnce`). **Visible threads:** `c0`, `c1` (the clients); the `server`
(both sides) and the implementation's proxies `p0`, `p1` are invisible. **Parameters:** `K ≥ 1` = the operations
per client (default 2); fixed: 2 clients, one key `x`, one retransmission in the whole run (at `p0`). ARGS
`<K> [variant]`; the specification ignores the variant. **Source:** the Aneris repository sample `dscm`
(distributed sequentially consistent memory), one-server implementation
`ml_sources/examples/dscm/implementations/one_server/` and `examples/dscm/{spec,clients}/`. The visible values
carry the client's thread id (`Op(from, op)`) and the two sides spawn in different orders, so the sides are
compared by role name (`online`, `conf-join`), not by the in-process `conf` (README.orig §6).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking and takes a message of the kind
written on its left (the Rust receives are tag-filtered, one tag per message kind). Messages are `Op(c, op)`
(client → front), `Res(r)` (front → client), `Req(op, rid, p)` (proxy → server), `Rep(r, rid)` (server → proxy);
`op ∈ {Wr(x, v), Rd(x)}`, `r ∈ {Ack, Val(v)}`, `Val(0)` is "unset" (the written values are 1 and 2). Client `i`'s
operation `j` is `op(i, j) ≜ Wr(x, i + 1)` for even `j` and `Rd(x)` for odd `j`, so at `K = 2` `c0` does
`Wr(x, 1); Rd(x)` and `c1` does `Wr(x, 2); Rd(x)`. The clients are the same on both sides except for the front they
talk to (`F0`, `F1` below), which the comparison does not see:

```
Clients(K, F0, F1) ≜ ‖_{i<2} c_i: for j := 0..K−1:  send^asyn(F_i, Op(c_i, op(i, j)));  Res(_) := recv^asyn()
```

Each client sends its operations one at a time and waits for the answer of each.

## The pairs

The specification, a sequentially consistent memory with one server:

```
Spec(K) ≜ Clients(K, server, server)
        ‖ server: db := 0
                  for n := 1..2K:  Op(c, op) := recv^asyn();  send^asyn(c, Res(apply(op)))

apply(Wr(x, v)) ≜ db := v;  Ack          apply(Rd(x)) ≜ Val(db)
```

`server` reads the `2K` operations of both clients in some order, applies each atomically and answers it: every
visible history is an interleaving of the two program orders in which each read returns the last write before it.
Every specification graph is complete; there are `(2K)!/(K!)²` of them (2, 6, 20, 70 for `K = 1..4`,
README.orig §2).

The implementation, the source's client proxy (`install_proxy` / `request` / `wait_for_reply`) and server
(`request_loop` over `process_request`):

```
Impl_v(K) ≜ Clients(K, p0, p1) ‖ p0 ‖ p1 ‖ server

p_i ≜ dups := (1 if i = 0 else 0);  sd := 0
      for rid := 0..K−1:
         Op(c, op) := recv^asyn()
         send^asyn(server, Req(op, rid, p_i))
         if dups > 0 and nondet({true, false}) then  dups := dups − 1;  sd := sd + 1;  send^asyn(server, Req(op, rid, p_i))
         repeat at most sd + 1 times:  Rep(r, id) := recv^asyn();  if id = rid then res := r;  exit
         send^asyn(c, Res(res))

server ≜ db := 0;  ctable := ∅
         while true:
            Req(op, rid, p) := recv^asyn()
            ⟨dedup⟩
            res := apply(op);  ctable(p) := (rid, res);  send^asyn(p, Rep(res, rid))
```

`p_i` forwards each operation as `Req(op, rid, p_i)` with its request id, and `p0` may, once in the run, send the
same `Req` a second time (the receive-timeout retransmission; the bag does not duplicate, so the duplicate is a
second send). It then reads replies until one carries its `rid` (at most `sd` stale ones precede it) and forwards
the result to the client. `server` is a loop that ends blocked when the proxies are done, so every implementation
graph is blocked in an invisible thread; `ctable(p)` holds the last `(rid, result)` per proxy.

### dedup — expected: **conforms**

```
⟨dedup⟩ ≜ if ctable(p) = (t, r):  if rid < t then  continue                                   (stale request: ignored)
                                  if rid = t then  send^asyn(p, Rep(r, rid));  continue          (cached reply re-sent)
```

Each `(p_i, rid)` is applied once: the first copy the server reads is applied and recorded; the second copy is
either read while `t = rid` and answered with the cached reply (a second `Rep(res, rid)` with the same content,
which `p0` reads instead of the first or leaves pending and skips at its next `rid`, since it accepts only
`id = rid`), or read after `p0`'s next request (`rid < t`) and ignored. A proxy forwards only the reply with its own
`rid`, whose result was computed when the operation was applied. So the visible history is the server's order of
first copies with their results, which is a `server` order of reads in the specification, and the specification's
answer to an operation follows the sends of exactly the operations read before it, which the implementation's
answer also follows (through the first copies). Every client ends done on both sides.

### no-dedup — expected: **violation** if `K ≥ 2`; **conforms** (control) if `K = 1`

```
⟨dedup⟩ ≜ skip        ◀ changed: ctable removed, every Req is applied
```

Schedule at `K = 2`: `p0` sends `Req(Wr(x, 1), 0)` twice; `server` reads the first (`db = 1`, `Ack` to `c0`), then
`p1`'s `Req(Wr(x, 2), 0)` (`db = 2`, `Ack` to `c1`), then `p0`'s `Req(Rd(x), 1)` (`Val(2)` to `c0`), then the
duplicate `Req(Wr(x, 1), 0)` (`db = 1` again), then `p1`'s `Req(Rd(x), 1)` (`Val(1)` to `c1`). The uncovered
behaviour is

```
c0: S(Op(c0, Wr(x, 1))) R(Res(Ack)) S(Op(c0, Rd(x))) R(Res(Val(2)))
c1: S(Op(c1, Wr(x, 2))) R(Res(Ack)) S(Op(c1, Rd(x))) R(Res(Val(1)))
```

In the specification `c0` reading 2 puts `Wr(x, 2)` after `Wr(x, 1)`, and `c1` reading 1 puts `Wr(x, 1)` after
`Wr(x, 2)`; with each write applied once no order has both (value). At `K = 1` the clients only write and every
result is `Ack`, so the double application is not observable and the two specification orders cover every graph.
At `K ≥ 3` these two read values alone are sequentially consistent (README.orig §4: `c0`'s third `Wr(x, 1)` may fall
between `c1`'s write and read), but by hand such a specification order applies a third-operation write before a
second-operation read, i.e. a third `Op` send precedes a second `Res` receive, an order the implementation graph
of the schedule above (where the re-applied write is the duplicate sent before `c0`'s first `Ack`) lacks.

## What it is for

A repository sample with its retransmission layer kept and one fault injected: the duplicate is sent and read
between invisible threads, so the fault surfaces only as a value at `K ≥ 2` and is masked at `K = 1`. Every
implementation graph is blocked in the server loop, so the status component is carried by the clients alone, and
the specification is small against the implementation (6 graphs against 146 at `K = 2`, README.orig §7).
