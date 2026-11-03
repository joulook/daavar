# calc(K) — calculator RPC (`calculator-disel`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: Disel's unordered message soup), the default the Rust program declares, the Java
twin's default, and the model of every campaign row. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java
twin in `java/` (`bench.calculator_disel.Calculator`, args `<spec|impl>,<K>,<fifo|bag>,<variant>`); context-free Java
port in `java-cf/` (`bench.calculator_disel.Calculator`, the same class name, args
`<spec|impl>,<K>,<fifo|bag>[,<variant>]`). **Visible threads:** `c0`, `c1` (the clients); the specification's
`server` and the implementation's `batcher`, `memo` and `delegator` are invisible. **Parameters:** `K ≥ 1` = the
number of requests per client (default 1); variant one of `batched`, `batched-swap`, `memo`, `memo-ignore-args`,
`delegating`, `delegating-prev`; fixed: 2 clients, `f(x, y) = x + y`, the argument domain `ARGS = [(1, 2), (2, 2)]`.
ARGS: `-- <K> [variant]` (`K` defaults to 1 and the variant to `batched`; any other variant name is refused by `imp`;
`spec` ignores the variant).
**Source:** Disel (Sergey, Wilcox, Tatlock, POPL 2018), `related-work-papers/sources/disel/theories/Examples/Calculator/`
(`CalculatorProtocol.v`, `SimpleCalculatorApp.v` and the other `.v` files of that directory).

Notation: as in `../../synthetic/naive/README.md`. A receive written with a pattern (`Resp(a, x, y) := recv^asyn()`)
takes a message of that type (tagged receives in Rust, type guards in Java). Messages: `Req(c, x, y)` (Disel's `req`,
`c` the requester, `args = [x; y]`) and `Resp(a, x, y)` (Disel's `resp`, `ans :: args`). The clients are the same on
both sides except for the front end they talk to (`F` below), which the comparison does not see:

```
Clients(K, F) ≜ ‖_{i<2} c_i: for j := 0..K−1:  (x, y) := ARGS[(i + j) mod 2]
                                               send^asyn(F, Req(c_i, x, y));  Resp(a, x', y') := recv^asyn()
```

`compute_list_f`: each client sends `K` requests one at a time, alternating the two argument pairs (`c0` starts with
`(1, 2)`, `c1` with `(2, 2)`), and waits for each answer. The visible values are the answers `(a, x', y')`.

The specification (every pair), one server:

```
Spec(K) ≜ Clients(K, server)
        ‖ server: for 2K times:  Req(c, x, y) := recv^asyn();  send^asyn(c, Resp(f(x, y), x, y))
```

`server` answers each request with `f` of its arguments and the arguments, right after reading it: `c_i`'s `j`-th
answer is `Resp(f(ARGS[(i+j) mod 2]), ARGS[(i+j) mod 2])`, and each answer follows its request and every request the
server read before it. `(2K)!/(K!·K!)` graphs (2, 6, 20, 70 at `K = 1..4`), one per receive order.

## The pairs

### batched — expected: **conforms**

```
Impl(K) ≜ Clients(K, batcher) ‖ batcher

batcher ≜ for K times:                                                       (2K requests in batches of 2)
             acc := []
             for 2 times:  Req(c, x, y) := recv^asyn();  acc := (c, x, y) · acc          (receive_req_loop)
             for e := 0..1:  (c, x, y) := acc[e];  send^asyn(⟨to⟩, Resp(f(x, y), x, y))  (send_ans_loop)

⟨to⟩ ≜ c
```

The source's `batched_server` with batch size 2. A client has at most one request outstanding, so each batch holds
one request of each client, and every answer is `Resp(f(args), args)` to its requester: the values are the
specification's. Both answers of a batch are sent after both requests were read, so the implementation graph's order
contains that of the specification graph reading the batch's two requests in the same order and only adds to it.
`2^K` graphs (2, 4, 8, 16 at `K = 1..4`), each covered.

### batched-swap — expected: **violation** (`K ≥ 1`)

```
⟨to⟩ ≜ acc[(e + 1) mod 2].c        ◀ changed: entry e's answer goes to entry e+1's requester (F2)
```

In every graph the first batch holds `c0`'s `(1, 2)` and `c1`'s `(2, 2)`, and each answer goes to the other entry's
requester. The uncovered behaviour at `K = 1`:

```
c0: S(Req(c0, 1, 2)) R(Resp(4, 2, 2))
c1: S(Req(c1, 2, 2)) R(Resp(3, 1, 2))
```

The specification gives `c0` only `Resp(3, 1, 2)` (signature value). Every implementation graph is uncovered at
every `K ≥ 1`, its first batch being swapped.

### memo — expected: **conforms**

```
Impl(K) ≜ Clients(K, memo) ‖ memo

memo ≜ Memo(2K)

Memo(n) ≜ table := []
          for n times:  Req(c, x, y) := recv^asyn()
                        hit := ⟨lookup⟩
                        if hit = some a then ans := a
                        else ans := f(x, y);  table := table · ((x, y), ans)
                        send^asyn(c, Resp(ans, x, y))

⟨lookup⟩ ≜ the ans of the first entry of table with args (x, y), or none
```

The source's `memoizing_server`. An entry is stored as `((x, y), f(x, y))` and the lookup matches the arguments, so
every answer is `Resp(f(args), args)` to its requester; one serving thread reads and answers one request at a time,
so the graphs correspond one-to-one to the specification's receive orders (2, 6, 20, 70), with the same values and
order.

### memo-ignore-args — expected: **violation** (`K ≥ 1`)

```
⟨lookup⟩ ≜ the ans of the first entry of table, whatever its args, or none        ◀ changed: F1
```

The first served request misses and is stored; the second served request is answered with the first one's value,
and its arguments are the other pair (it is the other client's first request or the same client's second; client
`i`'s `j`-th request has `ARGS[(i + j) mod 2]`). The uncovered behaviour at `K = 1`, by which request is served first:

```
c0 first:   c1: S(Req(c1, 2, 2)) R(Resp(3, 2, 2))
c1 first:   c0: S(Req(c0, 1, 2)) R(Resp(4, 1, 2))
```

The specification answers `(2, 2)` with `4` and `(1, 2)` with `3` (signature value). Every implementation graph is
uncovered at every `K ≥ 1`.

### delegating — expected: **conforms**

```
Impl(K) ≜ Clients(K, delegator) ‖ delegator ‖ memo

memo      ≜ Memo(2K)                                     (the lookup of `memo`, matching the arguments)
delegator ≜ last := none
            for 2K times:  Req(c, x, y) := recv^asyn()
                           send^asyn(memo, Req(delegator, x, y));  Resp(a, _, _) := recv^asyn()      (compute_f on instance l2)
                           out := ⟨out⟩;  last := some a
                           send^asyn(c, Resp(out, x, y))

⟨out⟩ ≜ a
```

The source's `delegating_server` with `SimpleCalculatorApp.v`'s wiring: the delegator forwards each request to the
memoising server and passes its answer on. `memo` is correct, so `a = f(x, y)` and every answer is
`Resp(f(args), args)` to its requester; the round trip to `memo` is invisible and the delegator serves one request at
a time, so the graphs correspond one-to-one to the specification's receive orders (2, 6, 20, 70), with the same values
and order.

### delegating-prev — expected: **violation** (`K ≥ 1`)

```
⟨out⟩ ≜ if last = some a' then a' else a        ◀ changed: a one-slot cache answers with the previous request's result (F3)
```

The first served request is answered correctly (`last` is empty); the second served request is answered with the
first one's result, and its arguments are the other pair, as for `memo-ignore-args`. The uncovered behaviour at
`K = 1` is the same as there: `c1: S(Req(c1, 2, 2)) R(Resp(3, 2, 2))` when `c0` is served first,
`c0: S(Req(c0, 1, 2)) R(Resp(4, 1, 2))` when `c1` is (signature value). Every implementation graph is uncovered at
every `K ≥ 1`.

## What it is for

One specification against the source's three server architectures (batching, memoisation, delegation), each with a
seeded value fault. `memo` and `delegating` have exactly the specification's graphs; `batched` has fewer (`2^K`), each
with more order than the specification graph that covers it. Every fault is a wrong answer value, visible at `K = 1`
in every implementation graph.
