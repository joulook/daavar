# width(w) — pure value branching (`s6-width`)

**Communication model:** fifo. **Language:** Rust, in `rust/`. **Visible threads:** `V`, `C`. **Parameter:** `w` =
the number of values to choose from.

Notation: as in `../naive/README.md`.

## The pair (self-paired) — expected: **conforms**

```
Impl(w) = Spec(w) ≜ V: x := nondet({0, …, w−1}); send^fifo(C, x)
                  ‖ C: y := recv^fifo()
```

`V` chooses one of `w` values and sends it to `C`; the same program on both sides: `w` graphs, `w` signatures, one
graph per signature, each covered by its own counterpart.

## What it is for

The cheapest possible graphs with the widest branching: the per-graph overhead of each engine (summary, index
insertion, lookup, export line) when the graphs are trivial, and the stateless witness cache at `|W| = w`.
