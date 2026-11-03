# share(m, c) — invisible choice behind relays (`s2-share`)

**Communication model:** fifo (`p2p`: per-channel order). **Language:** Rust, in `rust/`.
**Visible threads:** `A0 … A(m−1)` and `C0 … C(m−1)`; the relays `Ri` are invisible. **Parameters:** `m` = the number
of relay modules; `c` = how many of them contain the invisible choice (`0 ≤ c ≤ m`).

Notation: as in `../naive/README.md`. `x := recv^{fifo,nb}()` is the non-blocking receive, which returns `⊥` when
no message has arrived yet.

## The pairs

### share — expected: **conforms**

```
Impl(m, c) ≜ ‖_{i<m} ( Ai: send^fifo(Ri, 9)
                     ‖ Ri: [ if i < c:  x := recv^{fifo,nb}();  if x = ⊥ then x := recv^fifo() ]
                           [ if i ≥ c:  x := recv^fifo() ]
                           send^fifo(Ci, x)
                     ‖ Ci: y := recv^fifo() )

Spec(m)    ≜ ‖_{i<m} ( Ai: send^fifo(Ci, 9) ‖ Ci: y := recv^fifo() )
```

`m` independent modules on disjoint channels. In the implementation, `Ai` sends `9` to the invisible relay `Ri`,
which forwards it to `Ci`. In the first `c` modules the relay first tries a non-blocking receive and falls back to a
blocking one: whether the message "had already arrived" is an invisible branch, so the implementation has `2^c`
complete graphs that all have the same visible events in the same visible order. The specification removes the
relays: `Ai` sends straight to `Ci`; it has one graph. Every implementation graph is covered by that one graph.

### ctl (the matched control, self-paired) — expected: **conforms**

```
Ctl(m, c) ≜ ‖_{i<m} ( Ai: send^fifo(Ri, 9)
                    ‖ Ri: x := recv^fifo();  b := (if i < c then nondet({0,1}) else 0);  send^fifo(Ci, 10·i + b)
                    ‖ Ci: y := recv^fifo() )
```

The same shape with a blocking receive in every relay and, in the first `c` modules, a choice that is **visible**
in the value `Ci` receives (`10·i` or `10·i + 1`). Both sides are this program: `2^c` graphs each, every graph with
its own signature, so nothing is shared. The control is checked against its own export (a join against the `share`
specification is not a measurement).

## What it is for

At fixed `m`, raising `c` multiplies the implementation's graphs while its visible behaviour stays one: the stateful
index and the offline summaries stay at one entry and the stateless witness cache hits on every graph after the
first. `ctl` has the same number of graphs with no sharing, so its cost is the exploration's own.
