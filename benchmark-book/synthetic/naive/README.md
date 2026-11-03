# naive(k) — the stateless worst case (`s1-naive`)

**In the paper:** Table 3 (violating): `naive` = variant `naive`, model bag, size 10, 11. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** bag (`asyn`: unordered delivery). **Language:** Rust (TraceForge's Must API), in `rust/`.
**Visible threads:** `A`, `C`, `B1 … Bk`. **Parameter:** `k` = the number of `B` senders. (The engine's test grid
also had a second spawn encoding, `B1..Bk, A, C` instead of `C, B1..Bk, A`; it changes nothing visible and only the
exploration order, and was dropped from the benchmark on 2026-11-03; the campaign's rows are those of the first encoding.)

Notation (the paper's): `T: c1; c2` is thread `T` running the commands in order; `‖` composes threads;
`send^M(to, v)` sends `v` to thread `to` under model `M`; `x := recv^M()` is a blocking receive of any message
addressed to the executing thread; `recv^{M,nb}()` is non-blocking and may return `⊥`; `nondet(S)` picks an
element of `S`; `skip` does nothing.

## The pairs

### naive — expected: **violation**

```
Impl_k ≜ C: send^asyn(A, 0); x1 := recv^asyn(); … ; xk := recv^asyn()
       ‖ B1: send^asyn(C, 1) ‖ … ‖ Bk: send^asyn(C, 1)
       ‖ A: skip

Spec_k ≜ C: send^asyn(A, 1); x1 := recv^asyn(); … ; xk := recv^asyn()
       ‖ B1: send^asyn(C, 1) ‖ … ‖ Bk: send^asyn(C, 1)
       ‖ A: skip
```

The only difference is the value `C` sends to `A` first: `0` in the implementation, `1` in the specification. The
`k` sends to `C` all carry `1` and the bag model orders none of them, so each of `C`'s receives may consume any of
them: each program has `k!` complete graphs. No implementation graph is covered by any specification graph, because
`C`'s first visible event is the send of `0` on one side and of `1` on the other: all `k!` implementation graphs are
uncovered (`G_x = k!`). This is the paper's `ex:naive`.

### self — expected: **conforms**

```
Impl_k = Spec_k ≜ the specification of the pair above (C sends 1)
```

The specification against itself: every graph covers itself.

## What it is for

The stateless engine has no index: for every implementation graph it re-explores the specification until it finds a
cover. On `self` every implementation graph needs a sweep over the specification's `k!` graphs, so the stateless
engine does `(k!)²` work where the stateful engine does `2·k!` (`G_spec^expl` = specification graphs explored in
total). On `naive` the sweep never finds a cover, so the stateless engine explores the whole specification once per
implementation graph and reports every graph uncovered.
