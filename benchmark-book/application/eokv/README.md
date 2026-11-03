# eokv(K) — exactly-once key-value store (`kv-exactly-once-grove`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: unordered delivery), the default of both the Rust program (`--model` left
out) and the Java twin; the campaign ran bag only (a client has one operation in flight at a time, so its legs carry
at most one message and bag equals fifo there). **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin
in `java/` (`bench.kv_exactly_once_grove.ExactlyOnceKv`); context-free Java port in `java-cf/`
(`bench.kv_exactly_once_grove.KvExactlyOnceGrove`). **Visible threads:** `c0`, `c1` (the clients); the `server`
(both sides) and the clerks `p0`, `p1` (implementation) are invisible. **Parameters:** `1 ≤ K ≤ 127` = the number
of keys (default 1; the Rust program's keys, values and operation ids are bytes, and the `2K` ids of `exactly-once`
fit only for `2K ≤ 255`); fixed: 2 clients, one lost reply in the whole run (at `p0`). ARGS
`<keys> [exactly-once|no-request-id]`, variant `exactly-once` (default) or `no-request-id`; the specification
ignores the variant. **Source:** Grove (Sharma et al., SOSP 2023) / gokv, the tutorial KV service
`tutorial/kvservice/` (`server.go`, `client.go`).

Notation: as in `../../synthetic/naive/README.md`; `while true` is a server loop that ends blocked when nothing more
arrives (the server's status is invisible, so every implementation execution is counted as blocked). Messages are
`Op(c, op)`, `Res(r)`, `Fresh(p)`, `FreshId(n)`, `Rpc(p, id, op)`, `Reply(r)`, with `op ∈ {CondPut(k, e, v), Get(k)}`
and `r ∈ {Ok(b), Val(v)}`; values `0` = `""`, `1` = `"1"`; `id = ⊥` is a request without an id. Every receive takes
any message addressed to the thread (the Rust program's untagged `recv_msg_block`): at a client or clerk only one
kind can be pending, and the server takes `Fresh` and `Rpc` in any order. The clients are the same on both sides
except for the thread they send to (`F0`, `F1`), which the comparison does not see:

```
Clients(K, F0, F1) ≜ c0: for k := 0..K−1:  send^asyn(F0, Op(c0, CondPut(k, 0, 1)));  m := recv^asyn()    (m = Res(r))
                   ‖ c1: for k := 0..K−1:  send^asyn(F1, Op(c1, Get(k)));             m := recv^asyn()    (m = Res(r))
```

`c0` runs `ConditionalPut(k, "", "1")` and `c1` runs `Get(k)` for every key in order, one operation at a time, each
waiting for its result.

The specification (both pairs), one server executing each operation once:

```
Spec(K) ≜ Clients(K, server, server)
        ‖ server: kvs := [0, …, 0]
                  for n := 1..2K:  Op(c, op) := recv^asyn();  send^asyn(c, Res(exec(op)))

exec(CondPut(k, e, v)) ≜ if kvs[k] = e then kvs[k] := v;  Ok(true)  else  Ok(false)
exec(Get(k))           ≜ Val(kvs[k])
```

`server` takes the `2K` operations one at a time in some order and answers each. Only `c0` writes, each key once from
`0`, so `c0` receives `Ok(true)` for every key, and `c1` receives `Val(1)` for key `k` iff its `Get(k)` is served
after `c0`'s `CondPut(k)`. Both clients end done in every graph (`(2K)!/(K!·K!)` graphs: 2, 6, 20 at `K = 1, 2, 3`).

The implementation, the source's `Server` and one `Clerk` per client:

```
Impl_v(K) ≜ Clients(K, p0, p1) ‖ p0 ‖ p1 ‖ server

server ≜ kvs := [0, …, 0];  next := 0;  lastReplies := ∅
         while true:
            m := recv^asyn()
            if m = Fresh(p):        send^asyn(p, FreshId(next));  next := next + 1
            if m = Rpc(p, id, op):  if id ≠ ⊥ and (id, r) ∈ lastReplies:  send^asyn(p, Reply(r))
                                    else:  r := exec(op);  if id ≠ ⊥ then lastReplies := lastReplies ∪ {(id, r)}
                                           send^asyn(p, Reply(r))

p_i (i = 0, 1) ≜ drops := (if i = 0 then 1 else 0)
                 for k := 0..K−1:
                    Op(c, op) := recv^asyn()
                    id := ⟨op id⟩
                    send^asyn(server, Rpc(p_i, id, op));  Reply(r) := recv^asyn()
                    if drops > 0 and nondet({true, false}):                 (the reply is lost: retry the same request)
                       drops := drops − 1
                       send^asyn(server, Rpc(p_i, id, op));  Reply(r) := recv^asyn()
                    send^asyn(c, Res(r))
```

`server` is `server.go`: `getFreshNum` hands out `nextFreshId`; an RPC whose id is in `lastReplies` is answered
from the cache, any other is executed and, when it carries an id, recorded. `p_i` is `c_i`'s clerk (`client.go`):
per operation it takes an id (`⟨op id⟩`, the variant's hole), sends the RPC and takes the reply; `p0` may once in
the run treat the reply as lost (the urpc error) and resend the same RPC, taking the second reply. The loss is
modelled after the reply's receipt, so the server has executed the operation and the clerk discards its reply. The
clerk forwards the reply it kept to the client.

## The pairs

### exactly-once — expected: **conforms**

```
⟨op id⟩ ≜ send^asyn(server, Fresh(p_i));  FreshId(n) := recv^asyn();  n
```

Every operation takes a fresh id from the one `server` (`next` increases at each `Fresh`), so ids are distinct
across operations and the retry carries its operation's id. The server executed the first `Rpc` before replying,
and the retry is sent only after that reply was received, so the retry finds `(id, r)` in `lastReplies` and is
answered with the first execution's `r` without a second execution. So the server executes each of the `2K`
operations exactly once, one at a time; its order of first executions restricted to a client is that client's
program order (a clerk handles `c_i`'s operations in order, and `c_i` sends the next only after its `Res`), and
each execution lies between the client's send of `Op` and its receive of `Res`. Placing the specification
`server`'s `n`-th receive at the implementation's `n`-th first execution gives a specification graph with the same
`kvs` evolution, the same results and the same per-client words; the `Fresh` and `Rpc` exchanges are invisible.
Both clients end done on both sides; the implementation server's blocked loop is invisible.

### no-request-id — expected: **violation** (`K ≥ 1`)

```
⟨op id⟩ ≜ ⊥        ◀ changed: the clerk takes no OpId (no Fresh round trip), so the server cannot recognise the retry
```

Schedule at `K = 1`: `p0` sends `Rpc(p0, ⊥, CondPut(0, 0, 1))`; `server` executes it (`kvs[0] := 1`, `Ok(true)`)
and replies; `p0` takes the reply, the coin says lost, and it resends; `server` executes it again (`kvs[0] = 1 ≠ 0`:
`Ok(false)`); `p0` forwards `Res(Ok(false))`. The uncovered behaviour is

```
c0: S(Op(c0, CondPut(0, 0, 1))) R(Res(Ok(false)))
```

(`c1`'s word is any of the specification's). In every specification graph `c0` receives only `Ok(true)` (signature
value). The same schedule on key `0` exists at every `K ≥ 1`.

## What it is for

A client–server service with the idempotence mechanism of the Grove tutorial (fresh operation ids and a reply cache)
against a server that executes each operation once; the id round trip, the lost reply and the retry stay between the
invisible clerk and server, so the comparison sees only the clients' operations and results. Every implementation
graph ends blocked in the invisible server loop, as in `../zab`, and the implementation's graphs (`exactly-once`:
16, 322, 6072 at `K = 1, 2, 3`) grow much faster than the specification's (2, 6, 20). The fault is a one-value
difference (`Ok(false)`) reached with one lost reply at the smallest size.
