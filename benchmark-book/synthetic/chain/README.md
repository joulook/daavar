# chain(d) — a relayed chain against a direct one (`s4-chain`)

**Communication model:** fifo. **Language:** Rust only. **Visible threads:** `v0 … v{d}`.
**Parameter:** `d` = the number of hops.

## The program

A chain of `d` hops: `v0` sends the value 1 along hop 1; `v_i` (for `1 ≤ i < d`) receives on hop `i` and then
sends `i + 1` along hop `i + 1`; `v_d` receives. In the implementation every hop passes through an invisible relay
`r_i` that forwards what it received; in the specification `v_{i-1}` sends to `v_i` directly. Each side has exactly
one complete graph.

## The pair

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| (single) | relayed hops (`4d` events, `2d` of them visible) | direct hops (`2d` events, all visible) | **conforms** | the visible events and their order are the same: a chain of `2d` visible events over `d` hops. The one implementation graph is covered by the one specification graph. |

## What it is for

One graph per side with a long visible order: the family measures the per-graph cost of summarising and comparing
a large visible order (the summary's order counts grow with `d`), separately from the number of graphs. At
`d ≥ 512` the program needs TraceForge's spawn threshold raised (`--threshold 65536`, 1 025 spawns), which the rows
spell; sizes where the tool refused to run are listed as "not run" in the evaluation document.
