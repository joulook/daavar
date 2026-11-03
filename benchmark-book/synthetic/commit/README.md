# commit(n, j) — the commitment dial (`s3-commit`)

**Communication model:** fifo. **Language:** Rust only. **Visible threads:** `p`, `c`.
**Parameters:** `n` = the number of chosen bits (`2^n` graphs on each side); `j` = how many of them the
specification chooses before its first visible send (`0 ≤ j ≤ n`).

## The program

`p` sends a common prefix value `−1` to `c`, then `n` further values, each a bit chosen non-deterministically
(`0` or `1`); `c` receives `n + 1` messages. Both sides produce the same `2^n` visible behaviours (the bit strings
behind the prefix). They differ only in **when** the choice is made: the implementation chooses each bit right before
sending it; the specification chooses the first `j` bits before the prefix send and the remaining ones right before
their sends. The choices are invisible; only the sent values are visible.

## The pair

| variant | implementation | specification | expected | why |
|---|---|---|---|---|
| (single) | bits chosen at the send (`j` plays no role) | the first `j` bits chosen before the prefix | **conforms** | the sets of visible behaviours are identical (`2^n` signatures each, one graph per signature); every implementation graph is covered by the specification graph with the same bit string. |

## What it is for

`j` moves the point where the specification commits to a behaviour without changing the behaviours: the family
measures whether the engines' cost depends on the commitment distance (it should not for the stateful and offline
engines, whose work is per distinct summary). Every graph of commit has its own signature but all of them have one
*shape*, which exposed a quadratic slot search in the in-process index (see the campaign notes: at `n = 16` the
in-process stateful check took 1 008 s where the export of the same graphs took 9 s).
