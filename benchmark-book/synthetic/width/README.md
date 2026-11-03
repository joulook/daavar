# width(w) — pure value branching (`s6-width`)

**Communication model:** fifo. **Language:** Rust only. **Visible threads:** `v`, `c`. **Parameter:** `w` = the
number of values to choose from.

## The program

`v` chooses a value `x` from `0 … w − 1` non-deterministically and sends it to `c`, who receives once. The same
program is used on both sides: `w` graphs, `w` signatures, one graph per signature.

## The pair

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| (single, self-paired) | `width(w)` | `width(w)` | **conforms** | the program against itself; every one of the `w` graphs is covered by its own counterpart. |

## What it is for

The cheapest possible graphs with the widest branching: the family measures the per-graph overhead of each engine
(summary, index insertion, lookup, export line) when the graphs themselves are trivial, and the stateless engine's
witness cache at `|W| = w`.
