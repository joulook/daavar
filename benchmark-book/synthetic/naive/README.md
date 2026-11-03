# naive(k, e) — the stateless worst case (`s1-naive`)

**Communication model:** bag (unordered delivery; the `asyn` channel). **Language:** Rust only (TraceForge's Must API).
**Visible threads:** `a`, `c`, `b1 … bk`. **Parameters:** `k` = the number of `b` senders; `e` = the spawn encoding
(1 spawns `c, b1..bk, a`; 2 spawns `b1..bk, a, c`), which changes nothing visible and is kept to show that the result
does not depend on the order of spawning.

## The program

`c` first sends one value `v` to `a` (who never receives it: the send is visible, its value is the only thing that
differs between the sides), then receives `k` messages. Each `b_i` sends the value 1 to `c`. `a` does nothing. The
bag model lets `c` receive the `k` messages in any order, so each side has `k!` complete graphs.

## The pairs

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| `naive` | `c` sends `v = 0` | `c` sends `v = 1` | **violation** | every implementation graph carries the visible value 0 where every specification graph carries 1; no specification graph covers any implementation graph, so all `k!` implementation graphs are uncovered (`G_x = k!`). This is the paper's `ex:naive`. |
| `self` | `c` sends `v = 1` | the same program | **conforms** | the specification checked against itself: every graph covers itself. |

## What it is for

The stateless engine has no index: for every implementation graph it re-explores the specification until it finds a
cover. On `self` every implementation graph needs a sweep over the specification's `k!` graphs, so the stateless
engine does `(k!)^2` work where the stateful engine does `2·k!`; the table shows the quadratic gap directly
(`G_spec^expl` = specification graphs explored in total). On `naive` the sweep never finds a cover, so the
stateless engine explores the whole specification once per implementation graph and reports every graph uncovered.
