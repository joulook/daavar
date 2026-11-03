# lb(k, q, m) — a load balancer against two specifications (`a4-balancer`)

**In the paper:** Table 2 (conforming): `lb` = variant `rr-a4b`, model fifo, size 3,3,3, 4,4,4. Table 3 (violating): `lb-strict` = variant `rr-a4a`, model fifo, size 3,3,3, 4,4,4. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** fifo (`p2p`: per-channel order); every channel is built per-sender FIFO (`LocalOrder`)
whatever the run's `--model`. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/`;
context-free Java port, in `java-cf/`. **Visible threads:** the clients `c0 … c(k−1)`; the front thread
(`dispatcher` in the specification, `balancer` in the implementation) and the servers `s0 … s(m−1)` are invisible.
**Parameters:** `k` = the number of clients, `q` = the requests per client, `m` = the number of servers (all `≥ 1`);
the `pair-swap-*` implementations are defined for `k = 2` only. ARGS `<impl> <spec> <k> <q> <m>`; the specification
side reads only `<spec>`, the implementation side only `<impl>`. The paper's main rows are `hash`–`a4a` and
`drop`–`a4a`. **Source:** written for this work (TraceForge fixture A4, `traceforge/src/conformance/grid.rs` at
`a3a7e1b`, lines 3748–3912).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking (`recv^fifo()`) and reads the executing
thread's one channel; `front` is the dispatcher or the balancer. `loop` repeats forever.

The clients and the servers, the same in every program (spawn order: `front`, `s0..`, `c0..`):

```
C_i(q) ≜ c_i: for j := 0..q−1: send^fifo(front, Req(i, j)); _ := recv^fifo()

S_x    ≜ s_x: loop: Req(i, j) := recv^fifo(); send^fifo(c_i, Reply(x, j))
```

Client `c_i` sends `q` requests, each followed by a blocking read of its reply. Server `s_x` serves forever,
replying `Reply(x, j)` (its own index and the request number) to the client directly; so the servers end blocked in
every graph of both sides, and only the clients' statuses are compared.

The two specifications (the dispatcher reads `k·q` requests in arrival order and forwards each at once):

```
Spec_a4b(k, q, m) ≜ dispatcher: for t := 1..k·q:
                                   r := recv^fifo()
                                   x := nondet({0, …, m−1})
                                   send^fifo(s_x, r)
                  ‖ ‖_{x<m} S_x ‖ ‖_{i<k} C_i(q)

Spec_a4a(k, q, m) ≜ dispatcher: aff := [⊥]^k
                                for t := 1..k·q:
                                   r := recv^fifo();  Req(i, _) = r
                                   if aff[i] = ⊥ then aff[i] := nondet({0, …, m−1})
                                   send^fifo(s_{aff[i]}, r)
                  ‖ ‖_{x<m} S_x ‖ ‖_{i<k} C_i(q)
```

`a4b` (relay) chooses a server per request; `a4a` (affinity) chooses on a client's first request and sends all its
later requests to the same server.

The implementations (no choice; `p` counts the forwarded requests):

```
Impl_{P,F}(k, q, m) ≜ balancer: B_{P,F} ‖ ‖_{x<m} S_x ‖ ‖_{i<k} C_i(q)

FWD_P(r) ≜ Req(i, _) = r;  x := ROUTE_P;  p := p + 1;  send^fifo(s_x, r)
ROUTE_hash ≜ i mod m          ROUTE_rr ≜ p mod m

B_{P,none} ≜ p := 0; for t := 1..k·q: r := recv^fifo(); FWD_P(r)
```

### hash against a4a — expected: **conforms**

```
Impl ≜ Impl_{hash,none}        Spec ≜ Spec_a4a
```

Every client gets the one server `i mod m`, which the affinity specification can choose on its first request. The
balancer and the dispatcher both read in arrival order and forward each request at once, so the causal order of the
replies after the requests is the same on both sides.

### hash against a4b — expected: **conforms**

```
Impl ≜ Impl_{hash,none}        Spec ≜ Spec_a4b
```

As above: the relay specification may choose `i mod m` for every request.

### rr against a4b — expected: **conforms**

```
Impl ≜ Impl_{rr,none}          Spec ≜ Spec_a4b
```

Request number `p` (in arrival order) goes to `s_{p mod m}`; the relay specification allows any server per request.

### pair-swap-hash against a4a — expected: **conforms** (control; `k = 2`)

```
Impl ≜ balancer: p := 0
                 for t := 1..q:
                    a := recv^fifo(); b := recv^fifo()                    ◀ changed: read two,
                    FWD_hash(b); FWD_hash(a)                              ◀ forward the later-arrived first
       ‖ ‖_{x<m} S_x ‖ C_0(q) ‖ C_1(q)
Spec ≜ Spec_a4a
```

Each round the balancer reads both clients' `j`-th requests (each client waits for its reply, so the two requests of
a round come from different clients) and forwards the later one first. The servers are unchanged (hash), and
forwarding only after both requests arrived adds order (each reply is after both requests), which the
specification's graphs allow. Not defined at `k = 1` (the balancer would wait for a second request that never comes).

### pair-swap-hash against a4b — expected: **conforms** (control; `k = 2`)

```
Impl ≜ the pair-swap-hash implementation above      Spec ≜ Spec_a4b
```

As above.

### pair-swap-rr against a4b — expected: **conforms** (control; `k = 2`)

```
Impl ≜ the pair-swap balancer with FWD_rr in place of FWD_hash      Spec ≜ Spec_a4b
```

The servers are arbitrary in `a4b`, and the swap only adds order.

### rr against a4a — expected: **violation** if `q ≥ 2 ∧ m ≥ 2`; **conforms** (vacuously) if `q = 1 ∨ m = 1`

```
Impl ≜ Impl_{rr,none}          Spec ≜ Spec_a4a
```

Round robin sends a client's consecutive requests to different servers. At `(k, q, m) = (1, 2, 2)`:
`c0: S(Req(0,0)) R(Reply(0,0)) S(Req(0,1)) R(Reply(1,1))`; the affinity specification serves both requests from one
server, so the second reply carries the same `x` as the first (value). With `q = 1` a client has one request, and with
`m = 1` there is one server, so the two policies coincide.

### pair-swap-rr against a4a — expected: **violation** if `q ≥ 2 ∧ m ≥ 2`; **conforms** if `q = 1 ∨ m = 1` (`k = 2`)

```
Impl ≜ the pair-swap balancer with FWD_rr      Spec ≜ Spec_a4a
```

In round `t` the later-arrived request goes to `s_{2(t−1) mod m}` and the earlier one to `s_{(2t−1) mod m}`; which
client arrives later can change between rounds, so a client's requests land on two servers, as with `rr` (value). Not
a row of README.orig; kept by the campaign rules.

### hash-drop against a4a — expected: **violation**

```
Impl ≜ balancer: p := 0
                 for t := 1..k·q:
                    r := recv^fifo()
                    if r = Req(k−1, q−1) then skip else FWD_hash(r)        ◀ changed: c(k−1)'s last request dropped
       ‖ ‖_{x<m} S_x ‖ ‖_{i<k} C_i(q)
Spec ≜ Spec_a4a
```

`c(k−1)`'s last request is never forwarded, so `c(k−1)` ends blocked on its last receive in every graph, while in
every specification graph every client receives all its replies (status). Smallest size `(1, 1, 1)`.

### hash-drop against a4b — expected: **violation**

```
Impl ≜ the hash-drop implementation above      Spec ≜ Spec_a4b
```

The same status difference. Not a row of README.orig; kept by the campaign rules.

### drop against a4a — expected: **violation**

```
Impl ≜ the hash-drop balancer with FWD_rr in place of FWD_hash      ◀ changed: c(k−1)'s last request dropped
Spec ≜ Spec_a4a
```

Round robin with the same drop (the dropped request does not advance `p`): `c(k−1)` ends blocked (status). README.orig
lists this combination as "not a row"; the campaign rules keep it and the paper uses it as a main row.

### drop against a4b — expected: **violation**

```
Impl ≜ the drop implementation above      Spec ≜ Spec_a4b
```

The same status difference.

## What it is for

One implementation family against two specifications that differ in how much freedom the front thread has: the same
`rr` balancer conforms to the relay specification and violates the affinity one, and the violation needs `q ≥ 2` and
`m ≥ 2`, so the verdict depends on the size. The specification's free server choice multiplies its graphs (96 at
`(2, 2, 2)` for `a4b`, 24 for `a4a`) against 6 for the deterministic balancer; every graph of both sides is blocked
(the servers loop), so the status component is carried by the visible clients alone. The drop faults exercise status,
`rr`–`a4a` value, and the pair-swap controls check that an implementation adding order is not reported.
