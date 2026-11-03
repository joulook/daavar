# pbkv(k, q) and pbkv-sh(k, q, s) — a replicated and a sharded key-value store (`a2-store`)

**Communication model:** fifo (`p2p`: per-channel order); every channel is built per-sender FIFO (`LocalOrder`)
whatever the run's `--model`. **Languages:** Rust (TraceForge's Must API), in `rust/`; context-free Java port, in
`java-cf/` (no Java twin). **Visible threads:** the clients `c0 … c(k−1)`; the specification's `store`, the
implementations' `primary`, `backup`, `router` and `shard0 … shard(s−1)` are invisible. **Parameters:** `k` = the
number of clients, `q` = the operations per client, `s` = the number of shards (`sh` and `misroute` only); all `≥ 1`.
ARGS `<variant> <k> <q> [<s>]`; the specification ignores the variant. The paper names the program `pbkv` for the
primary–backup variants (main rows `pb` and `early-ack`) and `pbkv-sh` for `sh` and `misroute`. **Source:** written
for this work (TraceForge fixture A2, `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3270–3561).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking. `x := recv^fifo(T)` receives only a
message of kind `T`, one of `Req` (a `Put` or a `Get`), `Fwd` and `AckPut`: each kind travels on its own Rust
channel, and `java-cf/` renders the channels as one mailbox with a type guard. `LOOKUP(map, x)` is `Val(map[x])` if
`x` is in `map`, otherwise `None`.

The clients, the same in every program, with a fixed script:

```
C_i(q, DEST) ≜ c_i: for j := 0..q−1: r := op(i, j); send^fifo(DEST(r), r); _ := recv^fifo()

op(i, j)  ≜ if i + j even then Put(i, key(i, j), 10·i + j) else Get(i, key(i, j))
key(i, j) ≜ (⌊j/2⌋ + ⌊i/2⌋) mod 2
```

Each client sends its operations in turn and waits for each reply. The scripts for `k, q ≤ 3`:
`c0: Put(0,0,0) Get(0,0) Put(0,1,2)`, `c1: Get(1,0) Put(1,0,11) Get(1,1)`, `c2: Put(2,1,20) Get(2,1) Put(2,0,22)`
(`Put(c, key, value)`, `Get(c, key)`). For `k, q ≤ 2` every operation is on key 0. `P` is the number of puts in all
the scripts.

## The pairs

The specification of every pair (spawn order `store, c0..`):

```
Spec(k, q) ≜ store: map := ∅
                    for t := 1..k·q:
                       r := recv^fifo()
                       if r = Put(c, x, v) then map[x] := v; send^fifo(c_c, Ack)
                       if r = Get(c, x) then send^fifo(c_c, LOOKUP(map, x))
           ‖ ‖_{i<k} C_i(q, r ↦ store)
```

One store serving requests in arrival order: every reply is causally after every request served before it, and a
get returns the last value put on its key in that order (or `None`).

The primary–backup implementations `pb` (spawn order `primary, backup, c0..`); gets are served by the primary:

```
PB_V(k, q) ≜ primary: map := ∅; first := true
                      for t := 1..k·q:
                         r := recv^fifo(Req)
                         if r = Put(c, x, v) then
                            APPLY_V
                            send^fifo(backup, Fwd(r))
                            REPLY_V
                            first := false
                         if r = Get(c, x) then send^fifo(c_c, LOOKUP(map, x))
           ‖ backup: map := ∅
                     for t := 1..P:
                        m := recv^fifo(Fwd); if m = Fwd(Put(_, x, v)) then map[x] := v
                        send^fifo(primary, AckPut)
           ‖ ‖_{i<k} C_i(q, r ↦ primary)

APPLY_V ≜ map[x] := v                       (default)
REPLY_V ≜ _ := recv^fifo(AckPut); send^fifo(c_c, Ack)      (default)
```

The backup-read implementations `pb-br` (spawn order `primary, backup, c0..`); the backup holds the data and
serves gets, the primary forwards puts:

```
BR_V(k, q) ≜ primary: for t := 1..N_V:
                         r := recv^fifo(Req)
                         if r = Put(c, _, _) then send^fifo(backup, r); BRREPLY_V
                         if r = Get(_, _) then send^fifo(backup, r)
           ‖ backup: map := ∅
                     for t := 1..k·q:
                        r := recv^fifo(Req)
                        if r = Put(_, x, v) then map[x] := v; send^fifo(primary, AckPut)
                        if r = Get(c, x) then send^fifo(c_c, LOOKUP(map, x))
           ‖ ‖_{i<k} C_i(q, DEST_V)

N_V        ≜ P                                                  (default)
DEST_V(r)  ≜ if r is a Put then primary else backup             (default)
BRREPLY_V  ≜ _ := recv^fifo(AckPut); send^fifo(c_c, Ack)        (default)
```

The backup's request channel is fed by the primary (puts) and by the clients (gets).

The sharded implementations `sh` (spawn order `router, shard0.., c0..`); `n_x` is the number of operations routed to
shard `x`:

```
SH_V(k, q, s) ≜ router: for t := 1..k·q: r := recv^fifo(); send^fifo(shard_{ROUTE_V(r)}, r)
              ‖ ‖_{x<s} shard_x: map := ∅
                                 for t := 1..n_x:
                                    r := recv^fifo()
                                    if r = Put(c, y, v) then map[y] := v; send^fifo(c_c, Ack)
                                    if r = Get(c, y) then send^fifo(c_c, LOOKUP(map, y))
              ‖ ‖_{i<k} C_i(q, r ↦ router)

ROUTE_V(r) ≜ key(r) mod s                                       (default)
```

### pb — expected: **conforms**

```
Impl ≜ PB_pb, all parts default
```

The primary is the serialization point: it applies a put, forwards it to the backup, waits for the backup's ack and
then replies; gets are answered from the primary's map. Every reply follows every earlier-served request through the
primary, and every get sees the latest put in the primary's order, as the specification's store does.

### control-early-ack-primary — expected: **conforms** (control)

```
Impl ≜ PB_control-early-ack-primary with
       REPLY ≜ send^fifo(c_c, Ack); _ := recv^fifo(AckPut)          ◀ changed: reply before the backup's ack
```

The client's `Ack` comes before the backup has applied the put, but the backup's map is never read (gets are served
by the primary, which applied the put first) and the primary reads the backup's ack before its next request. The
backup's lag is unobservable.

### pb-br — expected: **conforms**

```
Impl ≜ BR_pb-br, all parts default
```

Gets go to the backup directly. The backup reads every request (forwarded puts and direct gets) in one order and
applies it; the primary replies `Ack` only after the backup's ack. So the backup is the serialization point, and every
reply is after every request the backup served before.

### control-early-ack-fwd — expected: **conforms** (control)

```
Impl ≜ BR_control-early-ack-fwd with
       N       ≜ k·q                                                ◀ changed: the primary reads every request
       DEST(r) ≜ primary                                            ◀ changed: gets also go through the primary
       BRREPLY ≜ send^fifo(c_c, Ack); _ := recv^fifo(AckPut)        ◀ changed: early ack
```

Early ack, but every get is forwarded through the primary, so the backup reads in the primary's order (one sender,
per-sender FIFO): a get forwarded after a put is served after it. The early ack is unobservable.

### sh — expected: **conforms** (all `k, q, s`)

```
Impl ≜ SH_sh, ROUTE default
```

The router reads every request in one order and forwards each to the shard of its key; each shard serves its key in
the router's order. Every reply is after every request the router read before it, through the router, which is the
specification's order; a get sees the latest put on its key.

### early-ack — expected: **violation** if `k ≥ 2 ∨ q ≥ 2`; **conforms** if `k = q = 1`

```
Impl ≜ BR_early-ack with
       BRREPLY ≜ send^fifo(c_c, Ack); _ := recv^fifo(AckPut)        ◀ changed: Ack before the backup applied the put
```

The ack-before-commit fault: gets go to the backup directly, and the primary acknowledges a put before the backup
has applied it. At `(k, q) = (1, 2)`: `c0: S(Put(0,0,0)) R(Ack) S(Get(0,0)) R(None)`, the get overtaking the
forwarded put at the backup; the specification returns `Val(0)` (value). At `(2, 1)`: `c0: S(Put(0,0,0)) R(Ack)`,
`c1: S(Get(1,0)) R(None)`, with the backup reading the get first; `c0`'s `Ack` is not after `c1`'s get, while every
specification graph that answers `None` serves the get first and orders `c1`'s `S(Get)` before `c0`'s `R(Ack)`
(order). One operation leaves nothing to read stale.

### stale-get — expected: **violation** if `k ≥ 2 ∨ q ≥ 2`; **conforms** if `k = q = 1`

```
Impl ≜ PB_stale-get with
       APPLY ≜ skip                                                  ◀ changed: the primary never applies a put
```

The primary forwards and acknowledges puts but never updates its own map, so every get returns `None`. At `(1, 2)`:
`c0: S(Put(0,0,0)) R(Ack) S(Get(0,0)) R(None)` against the specification's `Val(0)` (value). At `(2, 1)`: in the
graph where the primary serves `c0`'s put before `c1`'s get, `c1` still reads `None`, and `c1`'s `S(Get)` is not
before `c0`'s `R(Ack)`; the specification answers `None` only when it serves the get first, which orders it so
(order).

### silent-put — expected: **violation**

```
Impl ≜ PB_silent-put with
       REPLY ≜ _ := recv^fifo(AckPut); if ¬first then send^fifo(c_c, Ack)     ◀ changed: first put never answered
```

The first put the primary receives is never answered: its client ends blocked on the receive in every graph
(`c0`'s first operation is a put, so there is one at every size); every specification graph is complete (status).

### misroute — expected: **violation** if `s ≥ 2 ∧ (k ≥ 2 ∨ q ≥ 2)`; **conforms** if `s = 1 ∨ k = q = 1`

```
Impl ≜ SH_misroute with
       ROUTE(r) ≜ if r is a Get then (key(r) + 1) mod s else key(r) mod s     ◀ changed: gets to the wrong shard
```

A get goes to the shard after its key's shard, whose map never holds that key (puts go to `key mod s`), so with
`s ≥ 2` every get returns `None`. At `(k, q, s) = (1, 2, 2)`: `c0: S(Put(0,0,0)) R(Ack) S(Get(0,0)) R(None)` against `Val(0)` (value); at
`(2, 1, 2)` the graph where the router reads the put first gives `c1` `None` without `c1`'s get ordered before `c0`'s
`Ack` (order, as for `stale-get`). With `s = 1` the two routes coincide; with one operation there is nothing to read.

## What it is for

Three implementation structures (primary–backup, backup reads, sharding) against one specification, with the
replication and routing invisible: the implementations' graph counts stay close to the specification's (6 each at
`(k, q) = (2, 2)`, 10 for `early-ack`, `java-cf/NOTES.md`), so the pair measures the cost of the conformance check on
realistic store code rather than a blow-up. The faults are the classic replication bugs (ack before commit, stale
read, lost reply, misrouting), exercising value, order and status, three of them only from two operations on; the two
early-ack controls check that an early acknowledgement nobody can observe is not reported.
