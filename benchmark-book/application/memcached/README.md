# memcached(Q) — memcached's command handling (`memcached-oopsla25`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** fifo (`p2p`: a connection is a TCP stream, the model's per-connection FIFO queues
`network_cmd[con]` and `network_res[con]`), the Rust program's declared default; the campaign ran fifo only.
**Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.memcached_oopsla25.Memcached`);
context-free Java port in `java-cf/` (`bench.memcached_oopsla25.Memcached`, the same class name in its own tree).
**Visible threads:** `c0`, `c1`, the clients (one connection each); the specification's key servers `k0`, `k1` and
the implementation's connection handlers `w0`, `w1` and bucket threads `b0`, `b1` (`b0` alone for `shared-bucket`)
are invisible. **Parameters:** `Q` = the commands per client, `1 ≤ Q ≤ 3` (the client scripts have three commands and
the Rust program asserts the bound; default 1; smallest interesting size 1 for `shared-bucket`, 2 for `lost-set`);
fixed: 2 clients, 2 keys `{0, 1}`, values `{1, 2}`. ARGS `<Q> [v3|shared-bucket|lost-set]`, default variant `v3`;
the specification side ignores the variant. **Source:** Bílý, Pereira, Müller, "A Refinement Methodology for
Distributed Programs in Rust", OOPSLA 2025 §5.1, the artefact's `refinement-lib/case-study-memcached/`
(`src/bin/memcached.rs` `handle_client`, `src/storage/mod.rs` v3, the model `src/model/`).

Notation: as in `../../synthetic/naive/README.md`. Messages: `Cmd(c, op)` with `op ∈ {Get(k), Set(k, v)}` and `c`
the connection (client) index, and `Res(r)` with `r ∈ {Stored, Got(k, v)}`, `v ∈ {⊥, 1, 2}` (`⊥` = `None`). Every
thread receives on its own channel and every channel holds one kind of message at a time, so each receive below
accepts any message addressed to the thread. The scripts (a client issues the first `Q`):

```
script_0 = [Set(0, 1), Get(1), Get(0)]
script_1 = [Set(1, 2), Get(1), Set(0, 2)]
```

At `Q = 1` the two `Set`s are on different keys; at `Q = 2` `c0`'s `Get(1)` races `c1`'s `Set(1, 2)` and `c1`
reads its own write; at `Q = 3` `c0`'s `Get(0)` races `c1`'s `Set(0, 2)`. The clients (closed loop: one command,
then its response) and a server body are common to both sides; `T(i, op)` is the thread a client sends to:

```
Clients(Q, T) ≜ ‖_{i<2} c_i: for j := 0..Q−1:  op := script_i[j];  send^fifo(T(i, op), Cmd(i, op));  r := recv^fifo()

Serve(reply, m) ≜ store := [⊥, ⊥]
                  for m times:  Cmd(c, op) := recv^fifo();  send^fifo(reply_c, Res(apply(store, op)))

apply(store, Set(k, v)) ≜ store[k] := v;  Stored
apply(store, Get(k))    ≜ Got(k, store[k])
```

`apply` is the model's `cache_process` without expiry (`Get` never removes). A server performs a static number `m`
of receives: the number of commands, among the first `Q` of both scripts, that are routed to it.

The specification, the model's one cache served per key:

```
Spec(Q) ≜ Clients(Q, (i, op) ↦ k_{key(op)}) ‖ ‖_{k<2} k_k: Serve(c, m_k)      (m_k = the commands on key k)
```

A client sends each command straight to the server of its key and blocks on the response. Visible behaviour: per
key, the commands are served in one order consistent with each client's program order, every `Get` returns the
value of the last `Set` before it in that order (or `⊥`), and commands on different keys are unordered. Graphs: 1, 3,
7 at `Q = 1, 2, 3`.

The implementation, v3 (`handle_client` per connection, `Storage::process` locking only the key's bucket, the
bucket thread playing the bucket's lock); the variant chooses the routing `route_v` and the bucket count `B_v`:

```
Impl_v(Q) ≜ Clients(Q, (i, op) ↦ w_i)
          ‖ ‖_{b<B_v} b_b: Serve(w, m_b)                                      (m_b = the commands routed to bucket b)
          ‖ ‖_{i<2} w_i: for Q times:  Cmd(c, op) := recv^fifo();  send^fifo(b_{route_v(op)}, Cmd(c, op))
                                       Res(r) := recv^fifo();        send^fifo(c_i, Res(r))
```

`w_i` reads a command from `c_i`, forwards it to its bucket, waits for the bucket's response and forwards that to
`c_i`. A bucket's store holds every key (the source's bucket item list holds any key that hashes to it).

## The pairs

### v3 — expected: **conforms**

```
B_v3 ≜ 2        route_v3(op) ≜ key(op)          (the source's hash, identity on the two keys)
```

Bucket `b_k` serves exactly the commands on key `k`, as `k_k` does, with the same `apply`; the handlers are relays,
one per closed-loop client (at most one outstanding command each), so they add no choice: the implementation has
the specification's 1, 3, 7 graphs. The implementation graph in which `b_k` serves key `k`'s commands in order
`σ_k` has the same client words as the specification graph in which `k_k` serves them in `σ_k` (same values at every
`Get`), and the relay only adds hops to the order between a client's send and its receive, so the specification's
order is contained in the implementation's and every graph is covered.

### shared-bucket — expected: **conforms**

```
B_shared-bucket ≜ 1        route_shared-bucket(op) ≜ 0        ◀ changed: both keys hash to one bucket (TABLE_SIZE collisions)
```

One bucket serves all `2Q` commands in one order, so a `Set(0)` and a `Set(1)` are ordered by `b0`'s program order,
which no per-key specification graph has. That is extra order in the implementation: `b0`'s order restricted to
each key is a valid `k_k` order with the same values (`apply` touches one key per command), so the specification
graph with those per-key orders has the same signature and an order contained in the implementation's, and the
check accepts it. The implementation has `C(2Q, Q)` = 2, 6, 20 graphs against the specification's 1, 3, 7, so the
cover is many-to-one.

### lost-set — expected: **violation** (`Q ≥ 2`); **conforms** at `Q = 1` (control)

```
B_lost-set ≜ 2        route_lost-set(Set(k, _)) ≜ (k + 1) mod 2        ◀ changed: a Set goes to the other key's bucket
                      route_lost-set(Get(k))    ≜ k
```

Every `Set` lands in the other key's bucket and every `Get` looks in the right one, so every `Get` answers `⊥`. At
`Q = 2`: `c1` sends `Set(1, 2)`, which `w1` routes to `b0` (stored there as key 1), and receives `Stored`; it then
sends `Get(1)`, routed to `b1`, whose `store[1]` is `⊥`. In every implementation graph the uncovered word is

```
c1: S(Cmd(1, Set(1, 2))) R(Res(Stored)) S(Cmd(1, Get(1))) R(Res(Got(1, ⊥)))
```

In every specification graph `k1` applies `c1`'s `Set(1, 2)` before `c1` sends its `Get(1)` (the client waits for
`Stored`), `c0`'s `Get(1)` writes nothing and no command deletes, so the answer is `Got(1, 2)` (signature value).
All 3 graphs are uncovered at `Q = 2`, all 7 at `Q = 3` (where `c0`'s `Get(0)` answers `⊥` too). At `Q = 1` the
scripts issue two `Set`s only, both answered `Stored`: the misrouting is invisible and every graph has `v3`'s word, a
conforming control.

## What it is for

A real server's command handling (the artefact's v3 storage with one lock per bucket) against the per-key cache
model of its own refinement proof, with the clients as the only visible threads. `shared-bucket` checks that an
implementation with more order than the specification (one bucket serialising both keys) is not reported, and
`lost-set` is a routing fault that becomes visible only once a `Get` follows a `Set` on the same key, so its verdict
depends on `Q`.
