# chain(d) — a relayed chain against a direct one (`s4-chain`)

**In the paper:** Table 2 (conforming): `chain` = variant `impl`, model fifo, size 2048, 4096, 8192, 16384, 32768. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** fifo. **Language:** Rust, in `rust/`. **Visible threads:** `V0 … Vd`; the relays `Ri` are
invisible. **Parameter:** `d` = the number of hops, `d ≥ 1`.

Notation: as in `../naive/README.md`.

## The pair — expected: **conforms**

```
Impl(d) ≜ V0: send^fifo(R1, 1)
        ‖ ‖_{1≤i<d} ( Ri: x := recv^fifo(); send^fifo(Vi, x)
                    ‖ Vi: y := recv^fifo(); send^fifo(R(i+1), i+1) )
        ‖ Rd: x := recv^fifo(); send^fifo(Vd, x)
        ‖ Vd: y := recv^fifo()

Spec(d) ≜ V0: send^fifo(V1, 1)
        ‖ ‖_{1≤i<d} ( Vi: y := recv^fifo(); send^fifo(V(i+1), i+1) )
        ‖ Vd: y := recv^fifo()
```

A chain of `d` hops: `V0` sends `1` along hop 1, `Vi` receives on hop `i` and sends `i + 1` along hop `i + 1`, `Vd`
receives. In the implementation every hop passes through an invisible relay that forwards what it received; in the
specification the senders talk to the receivers directly. Each side has one complete graph; the visible events and
their order are the same chain of `2d` events, so the implementation graph is covered.

## What it is for

One graph per side with a long visible order: the per-graph cost of summarising and comparing a large visible order
(the order counts grow with `d`), separately from the number of graphs. At `d ≥ 512` the program needs TraceForge's
spawn threshold raised (`--threshold 65536`, 1 025 spawns), which the rows spell; sizes where the tool refused to
run are listed as "not run" in EVAL.pdf.
