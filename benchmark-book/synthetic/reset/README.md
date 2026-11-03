# reset(k, s) — certificate resets across backward revisits (`s5-reset`)

**Communication model:** bag. **Language:** Rust only. **Visible threads:** `a_i`, `b_i`, `c_i` for `i < k`.
**Parameters:** `k` = the number of copies of the pair; `s` = the size of the specification's invisible padding
(it multiplies the specification's graphs by `s!` without changing anything visible).

## The program

`k` copies of one pair on disjoint channels. In copy `i`, `a_i` sends the value 1 (tag 1) and `b_i` sends the value 2
(tag 2) to `c_i`, who receives **once**. On the implementation side `c_i` accepts any message, so each copy has two
behaviours (receive 1 or receive 2) and the implementation has `2^k` graphs. On the specification side `c_i` accepts
only tag 2, so the specification has one visible behaviour per copy. The specification additionally spawns, after
every copy, an invisible padding module: `s` invisible senders of distinct values to one invisible receiver `d`,
which gives the specification `s!` graphs with the same visible projection. The spawn order `a, c, b` makes `b_i`'s
send a backward revisit of `c_i`'s receive during exploration; the control uses the order `a, b, c`, where both sends
are deliverable when `c_i` receives.

## The pairs

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| `reset` | `c_i` receives any message, spawn order `a, c, b` | `c_i` receives only tag 2, spawn order `a, c, b`, padding `s` | **violation** | every implementation graph in which some `c_i` received the value 1 has no counterpart in the specification: `2^k − 1` uncovered graphs; only the all-2 graph is covered. |
| `ctl` (control) | the same with spawn order `a, b, c` | the same with spawn order `a, b, c` | **violation** | the same behaviours and the same uncovered graphs, reached without backward revisits; the control separates the exploration pattern from the verdict. It is a violating control by construction. |
| `twin` | the specification program itself (`only tag 2`, padding `s`) | the same program | **conforms** | the specification against itself. |

## What it is for

The family exercises what happens to a witness or a certificate when the exploration backtracks through a backward
revisit (`reset`) against the same behaviours reached by forward exploration (`ctl`), and the knob `s` makes every
failing sweep of the stateless engine cost `s!` specification graphs without adding visible behaviours.
