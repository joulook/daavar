# reset(k, s) — certificate resets across backward revisits (`s5-reset`)

**In the paper:** Table 3 (violating): `reset` = variant `reset`, model bag, size 20,11. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** bag. **Language:** Rust, in `rust/`. **Visible threads:** `Ai`, `Bi`, `Ci` for `i < k`; the
padding threads `S0 … S(s−1)` and `D` are invisible. **Parameters:** `k` = the number of copies of the pair; `s` = the
size of the specification's invisible padding (it multiplies the specification's graphs by `s!` and changes nothing
visible). All three pairs require `k ≥ 1` and `s ≥ 1`.

Notation: as in `../naive/README.md`; `send^{asyn}_t(to, v)` sends with tag `t`, `recv^{asyn}(tag = 2)` receives
only a message with tag 2. "spawned `A, C, B`" is the order in which the main thread starts the copy's threads.

## The pairs

### reset — expected: **violation**

```
Impl(k)    ≜ ‖_{i<k} ( Ai: send^asyn_1(Ci, 1) ‖ Ci: x := recv^asyn() ‖ Bi: send^asyn_2(Ci, 2) )     spawned A, C, B

Spec(k, s) ≜ ‖_{i<k} ( Ai: send^asyn_1(Ci, 1) ‖ Ci: x := recv^asyn(tag = 2) ‖ Bi: send^asyn_2(Ci, 2) )   spawned A, C, B
           ‖ S0: send^asyn(D, 100) ‖ … ‖ S(s−1): send^asyn(D, 100 + s − 1) ‖ D: for j := 1..s: y := recv^asyn()
```

In every copy, `Ai` sends `1` and `Bi` sends `2` to `Ci`, who receives once. The implementation's `Ci` accepts any
message, so each copy has two behaviours and the implementation has `2^k` graphs. The specification's `Ci` accepts
only the tag-2 message, so its visible behaviour per copy is one; the invisible padding gives it `s!` graphs with the
same visible projection. Every implementation graph in which some `Ci` received `1` has no counterpart: `2^k − 1`
uncovered graphs, only the all-`2` graph is covered. The spawn order `A, C, B` makes `Bi`'s send a backward revisit of
`Ci`'s receive during exploration.

### ctl (the control) — expected: **violation** (by construction)

```
the same two programs, each copy spawned A, B, C
```

The same behaviours and the same `2^k − 1` uncovered graphs, reached without backward revisits: both sends are
already deliverable when `Ci` receives. The control separates the exploration pattern from the verdict.

### twin — expected: **conforms**

```
Impl = Spec ≜ Spec(k, s) of the reset pair
```

The specification against itself.

## What it is for

What happens to a witness or certificate when the exploration backtracks through a backward revisit (`reset`)
against the same behaviours reached by forward exploration (`ctl`); the knob `s` makes every failing sweep of the
stateless engine cost `s!` specification graphs without adding visible behaviours.
