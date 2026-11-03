# 2pc(n, r) — two-phase commit over r rounds (`a1-commit`)

**In the paper:** Table 2 (conforming): `2pc` = variant `correct`, model fifo, size 5,3. Table 3 (violating): `2pc-ea` = variant `early-abort`, model fifo, size 5,3. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** fifo (`p2p`: per-channel order). **Languages:** Rust (TraceForge's Must API), in `rust/`;
Java twin, in `java/`; context-free Java port, in `java-cf/`. **Visible threads:** `p0`, `p1`; the coordinator
`coord`, the participants `p2 … p(n−1)` and the thread that sends `Init` are invisible. **Parameters:** `n` = the
number of participants of the implementation (`n ≥ 2`; the specification always has two); `r` = the number of
rounds (`r ≥ 1`). ARGS `<variant> <n> <r>`.
**Source:** written for this work (TraceForge fixture A1, `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3106–3266).

Notation: as in `../../synthetic/naive/README.md`. Every receive here is blocking (`recv^fifo()`); a receive
accepts any message addressed to the executing thread, and `match` on its constructor is a plain case split. The
participant list `ps` reaches `coord` in an `Init(ps)` message: in Rust the main thread sends it after spawning, in
the Java twin an invisible thread `init` spawned last sends it (a JMC main performs no message event), in `java-cf/`
`coord` builds `ps` itself and there is no `Init` message. The three forms give the same graph counts.

The participant code, the same in every program:

```
P(r) ≜ for j := 1..r:
          m := recv^fifo();  if m ≠ Prepare(c) then stop
          v := nondet({Yes, No});  send^fifo(c, v)
          d := recv^fifo()                                     -- Commit or Abort
```

Each round, a participant reads `Prepare(coord)`, votes freely to the coordinator named in it, and reads the
round's decision.

## The pairs

The specification of every pair:

```
Spec(r) ≜ coord: Init(ps) := recv^fifo()                       -- ps = [p0, p1]
                 for j := 1..r:
                    for p in ps: send^fifo(p, Prepare(coord))
                    for i := 1..|ps|: _ := recv^fifo()
                    d := nondet({Commit, Abort})
                    for p in ps: send^fifo(p, d)
        ‖ p0: P(r) ‖ p1: P(r)
        ‖ init: send^fifo(coord, Init([p0, p1]))
```

Each round the specification's coordinator sends `Prepare` to both participants, reads both votes, then picks the
decision freely and sends that one decision to both. So in every round both visible participants receive the same
decision, and each receive is causally after both votes; the votes do not constrain the value.

The implementation, `coord` is the only thread that changes between variants:

```
Impl_V(n, r) ≜ coord: C_V(r)  ‖ p0: P(r) ‖ … ‖ p(n−1): P(r)
             ‖ init: send^fifo(coord, Init([p0, …, p(n−1)]))

C_V(r) ≜ Init(ps) := recv^fifo()
         for j := 1..r:
            for p in ps: send^fifo(p, Prepare(coord))
            DECIDE_V
```

### correct — expected: **conforms**

```
DECIDE_correct ≜ yes := 0
                 for i := 1..|ps|: v := recv^fifo(); if v = Yes then yes := yes + 1
                 d := (if yes = |ps| then Commit else Abort)
                 for p in ps: send^fifo(p, d)
```

The real 2PC: read every vote, commit iff all are `Yes`, send the same decision to every participant in order.
The decision is one value, sent to `p0` and `p1` after every vote is read; the specification's free choice can pick
that value. The invisible participants (`n > 2`) only change which value it is, which the free choice covers.

### control-reverse — expected: **conforms** (control)

```
DECIDE_control-reverse ≜ … as DECIDE_correct …
                         for p in reverse(ps): send^fifo(p, d)            ◀ changed: reverse order
```

Decisions go to `p(n−1)` first and `p0` last. The order of two sends to different threads is not observed, so the
pair conforms as `correct` does.

### eager — expected: **violation**

```
DECIDE_eager ≜ seen_no := false
               for p in ps:                                               ◀ changed
                  v := recv^fifo(); if v = No then seen_no := true
                  send^fifo(p, if seen_no then Abort else Commit)
```

The coordinator decides as each vote arrives: it reads one vote (from any participant) and at once sends the
decision so far to the next participant in order. At `n = 2, r = 1`, with `p0`'s `Yes` read first and `p1`'s `No`
second: `p0: R(Prepare(coord)) S(Yes) R(Commit)`, `p1: R(Prepare(coord)) S(No) R(Abort)`. Two different decisions
in one round, which no specification graph has (value); also `p0`'s decision is not after `p1`'s vote (order).

### early-abort — expected: **violation**

```
DECIDE_early-abort ≜ read := 0; aborted := false
                     while read < |ps|:                                   ◀ changed
                        v := recv^fifo(); read := read + 1
                        if ¬aborted ∧ v = No then
                           aborted := true; for p in ps: send^fifo(p, Abort)
                     if ¬aborted then for p in ps: send^fifo(p, Commit)
```

On the first `No` read, `Abort` goes to every participant before the remaining votes are read; the coordinator
then drains them. With `p0`'s `No` read first, `p0: R(Abort)` is not causally after `p1`'s vote; in the
specification every decision is after both votes (order).

### silent — expected: **violation**

```
DECIDE_silent ≜ … as DECIDE_correct, except the last send loop:
                for (i, p) in enumerate(ps):
                   if ¬(j = r ∧ i = 1) then send^fifo(p, d)              ◀ changed: no decision to p1 in round r
```

In the last round the decision to `ps[1] = p1` is never sent, so `p1` ends blocked on its last receive in every
graph (`0` complete, all blocked). Every specification graph is complete (status).

## What it is for

A small application with a free vote per participant and per round. Measured graph counts (README.orig §7): the
specification has 16 at `r = 1` and 256 at `r = 2`; the `correct` implementation has 8 at `(n, r) = (2, 1)`, 64 at
`(2, 2)`, 48 at `(3, 1)`, while the visible threads stay two. The
invisible participants (`n > 2`) multiply the implementation's graphs without adding visible behaviour; the three
faults exercise the three components of a mismatch (value, order, status), and `control-reverse` checks that an
unobservable reordering of sends is not reported.
