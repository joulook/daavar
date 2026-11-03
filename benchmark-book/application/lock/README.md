# lock(k, q) — a lock service with a waiter queue (`a3-lock`)

**Communication model:** fifo (`p2p`: per-channel order); every channel is built per-sender FIFO (`LocalOrder`)
whatever the run's `--model`. **Languages:** Rust (TraceForge's Must API), in `rust/`; context-free Java port, in
`java-cf/` (no Java twin). **Visible threads:** the clients `c0 … c(k−1)`; the specification's `lock` and the
implementation's `coordinator` are invisible. **Parameters:** `k` = the number of clients, `q` = the acquisitions per
client (both `≥ 1`). ARGS `<variant> <k> <q>`; the specification ignores the variant. **Source:** written for this
work (TraceForge fixture A3, `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3565–3573 and 3626–3744).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking. `x := recv^fifo(T)` receives only a
message of kind `T` (the specification's lock has two channels, one for `Acq` and one for `Rel`; `java-cf/` renders
them as one mailbox with a type guard); `recv^fifo()` takes any message on the thread's one channel. `L` is the lock
or the coordinator.

The clients, the same in every program (spawn order: `L`, then `c0..`):

```
C_i(q) ≜ c_i: for j := 1..q:
                 send^fifo(L, Acq(i))
                 _ := recv^fifo()                    -- Grant(i, j−1)
                 send^fifo(L, Rel(i))
```

Each client acquires, waits for its grant, and releases, `q` times.

## The pairs

The specification of every pair:

```
Spec(k, q) ≜ lock: round := [0]^k
                   for t := 1..k·q:
                      Acq(i) := recv^fifo(Acq)
                      send^fifo(c_i, Grant(i, round[i])); round[i] := round[i] + 1
                      _ := recv^fifo(Rel)
           ‖ ‖_{i<k} C_i(q)
```

The lock reads any pending `Acq` (the channel orders only each sender's messages), grants it with the client's round
number, and reads the holder's `Rel` before the next `Acq`. One holder at a time, any grant order: every grant is
causally after the previous holder's release.

The implementation: one mailbox for both kinds of message, and a waiter queue.

```
Impl_V(k, q) ≜ coordinator: round := [0]^k; held := false; Q := ⟨⟩
                            for t := 1..2·k·q:
                               m := recv^fifo()
                               if m = Acq(i) then
                                  if ¬held ∨ DOUBLE_V then held := true; GRANT_V(i) else Q := Q · ⟨i⟩
                               if m = Rel(_) then
                                  if Q = ⟨⟩ then held := false
                                  else n := POP_V(Q); if STRAND_V(n) then held := false else GRANT_V(n)
             ‖ ‖_{i<k} C_i(q)

GRANT_V(i) ≜ send^fifo(c_i, Grant(i, ROUND_V(i))); round[i] := round[i] + 1
```

The default parts: `DOUBLE_V = false`, `POP_V(Q)` = take the front of `Q`, `STRAND_V(n) = false`,
`ROUND_V(i) = round[i]`. A `Rel` hands the lock to the next waiter, or frees it.

### fifo — expected: **conforms**

```
all parts default
```

The coordinator grants a free lock at once and otherwise queues the client; a `Rel` grants the next waiter in queue
order. Every grant is after the `Acq` it answers and after the previous holder's `Rel`, with the right round
number, which is the specification's order; queueing a waiter's `Acq` before the `Rel` only adds order.

### control-lifo — expected: **conforms** (control)

```
POP_control-lifo(Q) ≜ take the back of Q                                 ◀ changed: a waiter stack
```

The waiters are granted last-come first. The specification allows any grant order, and the coordinator still grants
only after the previous holder's `Rel`, so the pair conforms. At `k = 2` at most one client waits, and the program is
the same as `fifo`.

### wrong-round — expected: **violation**

```
ROUND_wrong-round(i) ≜ if i = k−1 then round[i] + 1 else round[i]       ◀ changed
```

Every grant to `c(k−1)` carries `j + 1` instead of `j`: `c(k−1): S(Acq(k−1)) R(Grant(k−1, 1)) …`, where the
specification sends `Grant(k−1, 0)` (value). Holds at every size, `k = 1` included (`c0` is then `c(k−1)`).

### double-grant — expected: **violation** if `k ≥ 2`; **conforms** if `k = 1`

```
DOUBLE_double-grant ≜ true                                               ◀ changed: grant even when held
```

An `Acq` is granted at once even when the lock is held. At `k = 2, q = 1`, with both `Acq`s read before any `Rel`:
`c0: S(Acq(0)) R(Grant(0,0)) S(Rel(0))`, `c1: S(Acq(1)) R(Grant(1,0)) S(Rel(1))`, and neither grant is after the other
client's `Rel`; in every specification graph the second grant is after the first holder's `Rel` (order). With one
client the lock is always free when it acquires, and the program behaves as `fifo`.

### never-grant — expected: **violation** if `k ≥ 2`; **conforms** if `k = 1`

```
STRAND_never-grant(n) ≜ (n = k−1)                                        ◀ changed: a queued c(k−1) is dropped
```

When the waiter popped on a `Rel` is `c(k−1)`, it is dropped and the lock is left free; nothing is granted, so a
waiter queued behind it is stranded too until a new `Acq` finds the lock free. A direct grant to `c(k−1)` on a free
lock still happens. At `k = 2`, in the graph where `c1`'s `Acq` is read while `c0` holds the lock, `c1` ends blocked on
its receive (the coordinator ends blocked too, invisible); every specification graph is complete (status). With one
client nobody is ever queued.

## What it is for

A small service whose implementation has a different internal structure from its specification (one mailbox and a
queue against two channels and a lock): the coordinator reads `Acq`s and `Rel`s in either order, which multiplies its
graphs (28 at `(k, q) = (2, 2)` against the specification's 6, `java-cf/NOTES.md`) without adding visible behaviour.
The three faults exercise the three components of a mismatch (value: `wrong-round`, order: `double-grant`, status:
`never-grant`), the last two only from `k = 2`, and `control-lifo` checks that a different grant order the
specification allows is not reported.
