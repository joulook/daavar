# share(m, c) — invisible choice behind relays (`s2-share`)

**Communication model:** fifo (per-channel order; the `p2p` channel). **Language:** Rust only.
**Visible threads:** `a0 … a{m-1}` and `c0 … c{m-1}`. **Parameters:** `m` = the number of relay modules;
`c` = how many of them contain the invisible choice (`0 ≤ c ≤ m`).

## The program

`m` independent modules on disjoint FIFO channels. In module `i` the visible `a_i` sends the value 9 to an invisible
relay `r_i`; `r_i` receives it and forwards it to the visible `c_i`, who receives it. The implementation's relay, in
the first `c` modules, first tries a **non-blocking** receive and, if nothing has arrived yet, a blocking one: the
choice between "arrived already" and "not yet" is an invisible branch, so the implementation has `2^c` complete
graphs that all look the same from outside (one visible signature, one visible order). The specification removes
the relays: `a_i` sends 9 straight to `c_i`; it has one graph.

## The pairs

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| `share` | relays with the invisible choice in `c` modules (`2^c` graphs) | direct sends (1 graph) | **conforms** | all `2^c` implementation graphs project to the same visible behaviour as the specification's single graph: one summary covers them all. |
| `ctl` (control) | relays with a blocking receive, and in the first `c` modules a **visible** choice: `r_i` sends `10·i + b` with `b ∈ {0,1}` chosen non-deterministically | the same program | **conforms** (self-paired) | the same shape with the branch made visible in `c_i`'s value: `2^c` graphs on both sides, each with its own signature, so nothing is shared. The control separates the cost of exploration from the benefit of sharing. |

## What it is for

At fixed `m`, raising `c` multiplies the implementation's graphs while the visible behaviour stays the same: the index
of the stateful engine and the summaries of the offline mode stay at one entry, and the witness cache of the stateless
engine hits on every graph after the first. The control `ctl` has the same number of graphs with no sharing at all,
so its cost is the exploration's own. Note for the tables: `ctl` is self-paired, so its offline joins must use the
export of `ctl` as the specification (a join against the `share` specification is not a measurement).
