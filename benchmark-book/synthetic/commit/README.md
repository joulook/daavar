# commit(n, j) — the commitment dial (`s3-commit`)

**In the paper:** Table 2 (conforming): `commit` = variant `impl`, model fifo, size 12,12, 16,16, 20,20. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** fifo. **Language:** Rust, in `rust/`. **Visible threads:** `P`, `C`.
**Parameters:** `n` = the number of chosen bits (`2^n` graphs on each side); `j` = how many of them the
specification chooses before its first visible send (`0 ≤ j ≤ n`).

Notation: as in `../naive/README.md`.

## The pair — expected: **conforms**

```
Impl(n)    ≜ P: send^fifo(C, −1);  for i := 1..n: ( b := nondet({0,1}); send^fifo(C, b) )
           ‖ C: for i := 0..n: x_i := recv^fifo()

Spec(n, j) ≜ P: for i := 1..j: b_i := nondet({0,1});
                send^fifo(C, −1);
                for i := 1..j: send^fifo(C, b_i);
                for i := j+1..n: ( b := nondet({0,1}); send^fifo(C, b) )
           ‖ C: for i := 0..n: x_i := recv^fifo()
```

`P` sends a common prefix `−1` to `C`, then `n` bits; `C` receives `n + 1` messages. Both sides produce the same
`2^n` visible behaviours (the bit strings behind the prefix) and differ only in **when** the bits are chosen: the
implementation chooses each bit right before sending it; the specification chooses the first `j` before the prefix
send. The choices are invisible, only the sent values are. Every implementation graph is covered by the
specification graph with the same bit string.

## What it is for

`j` moves the point where the specification commits to a behaviour without changing the behaviours, so the family
tests whether an engine's cost depends on the commitment distance (the stateful and offline engines work per
distinct summary and should not). Every graph of commit has its own signature but all share one *shape*, which
exposed a quadratic slot search in the in-process index: at `n = 16` the stateful check took 1 008 s where the
export of the same graphs took 9 s (plan/traceForge/MESSAGE-STATEFUL-INDEX.md).
