# bcast(n) — broadcast consensus: decide the max of n values (`broadcast-consensus-civl`)

**Communication model:** bag (`asyn`: the source's channels are multisets), the model the crate declares.
**Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.broadcast_consensus_civl.Spec`
and `.Impl`); context-free Java port in `java-cf/` (`BroadcastConsensus`). **Visible threads:** `c0 … c(n−1)`, one
client per process; the oracle `oracle` (specification) and the nodes `node0 … node(n−1)` (implementation) are
invisible. **Parameters:** `n` = the number of processes (default 2, smallest interesting size 2); variant `correct`
or `decide-early`.
**Source:** CIVL (Boogie), `Test/civl/samples/BroadcastConsensus.bpl`.

Notation: as in `../../synthetic/naive/README.md`. Messages: `Input(v)` (client → oracle or node), `Val(v)` (node →
node, the source's `send(v, i)`), `Decide(d)` (oracle or node → client). `x := recv^asyn(tag = T)` is a blocking
receive of a message of type `T` (each node has one `Input` and one `Val` channel). The clients are common to both
sides, with `to_i` = `oracle` in the specification and `node_i` in the implementation:

```
C(n) ≜ ‖_{i<n} c_i: v := nondet({0, 1});  send^asyn(to_i, Input(v));  Decide(d) := recv^asyn(tag = Decide)
```

Client `c_i` is process `i`'s environment: it picks the process's value, sends it, and receives the decision (the
source's `value` is an unconstrained global; the choice stands for any value).

The specification (both pairs):

```
Spec(n) ≜ oracle: d := −∞
                  for 1..n:  Input(v) := recv^asyn(tag = Input);  d := max(d, v)
                  for i := 0..n−1: send^asyn(c_i, Decide(d))
        ‖ C(n)
```

The oracle receives the `n` inputs in any order and sends their maximum to every client. Every `Decide` is after
every client's `Input` and carries `max(v)`; `2^n · n!` graphs.

## The pairs

### correct — expected: **conforms**

```
Impl(n) ≜ ‖_{i<n} node_i: Input(v) := recv^asyn(tag = Input)
                          for j := 0..n−1: send^asyn(node_j, Val(v))
                          d := −∞
                          for 1..n:  Val(w) := recv^asyn(tag = Val);  d := max(d, w)
                          send^asyn(c_i, Decide(d))
        ‖ C(n)
```

`node_i` receives its client's value, sends it to every node, itself included (`Broadcast`), receives `n` values in
any order, and sends their maximum to its client (`Collect` and `set_decision`); `2^n · (n!)^n` graphs. The
specification replaces the `n` nodes by one oracle that sees the inputs directly. Node `i` decides only after all
`n` values, and each `Val(v_j)` was sent after `node_j` received `Input(v_j)`, so every client's `Input` happens
before every `Decide`, as in the specification, and every decision is `max(v)`. The oracle's sends are invisible, so
the clients' receives are mutually unordered on both sides.

### decide-early — expected: **violation** (`n ≥ 2`)

```
Impl(n) ≜ the correct implementation with the collection
                          for 1..n−1:  Val(w) := recv^asyn(tag = Val);  d := max(d, w)      ← changed: n − 1 values
```

Node `i` decides after `n − 1` of the `n` values, and may miss the maximum. At `n = 2`, with `v0 = 0`, `v1 = 1` and
`node0` reading its own `Val(0)`: `c0` receives `Decide(0)`, where every specification graph with these inputs gives
`Decide(1)` (signature value). On other graphs the order fails: with `v0 = v1 = 0` and `node0` reading its own value,
`c0`'s `Decide(0)` is not causally after `c1`'s `Input(0)`, which the specification requires (signature order). The
plain-Must counts equal `correct`'s (`n!/1! = n!` read orders per node). At `n = 1` (hand-derived, not run) the node
reads nothing and decides the initial `−∞` (`i32::MIN`), also a wrong value.

## What it is for

A consensus protocol against its one-line property (`decision[j] = max(values)`, the source's `ensures`). The
implementation's graphs grow as `(n!)^n` against the specification's `n!` (16 against 8 at `n = 2`, 1728 against 48
at `n = 3`, 5 308 416 against 384 at `n = 4`), so the cover is strongly many-to-one; the fault is caught by the
value component on some graphs and by the order component on others.
