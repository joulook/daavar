# ring-el(n) — ring leader election under loss, duplication and reordering (`ring-election-igloo`)

**Communication model:** bag (`asyn`: UDP reorders; a lost message is one that is never read). **Languages:** Rust
(TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.ring_election_igloo.RingElection`); context-free
Java port in `java-cf/` (`RingElectionIgloo`). **Visible thread:** `observer`; the specification's `oracle` and the
implementation's `node0 … node(n−1)` are invisible. **Parameters:** `n ≥ 2` = the number of nodes, with ids
`0 … n−1` on the ring `i → i+1 mod n` (top `n−1`); fixed: `R = 1` loop iteration after the initial send, `D = 1`
duplicated send per node.
**Source:** Sprenger, Klenze et al., "Igloo", OOPSLA 2020, leader-election case study (`election.py`'s `mainLoop`;
models `Leader_Election_0..4.thy`).

Notation: as in `../../synthetic/naive/README.md`. Messages are `Id(v)` (node → successor) and
`Status(i, b)` (node or oracle → observer: node `i`'s final `leader` flag `b`). The observer is added (the source
prints "I am the leader") and is the same on both sides:

```
Observer(n) ≜ observer: for n times: s := recv^asyn()
```

The specification (both pairs), the abstract models' property `inv_leader_top` with `U_0`: at most the top is
elected, and possibly nobody (the source proves no liveness):

```
Spec(n) ≜ Observer(n)
        ‖ oracle: e := nondet({true, false})
                  for i := 0..n−1: send^asyn(observer, Status(i, e ∧ i = n−1))
```

The implementation, `mainLoop` with its loop bounded:

```
Impl_v(n) ≜ Observer(n) ‖ ‖_{i<n} nodei

nodei ≜ budget := 1;  to_send := i;  leader := false
        usend(to_send)
        for R times:
           m := recv^{asyn,nb}()
           if m = Id(i):  leader := true;  break
           if m = Id(v), v ≠ i, and ⟨adopt v⟩:  to_send := v
           usend(to_send)
        send^asyn(observer, Status(i, leader))

usend(v) ≜ send^asyn(node(i+1 mod n), Id(v))
           if budget > 0 and nondet({true, false}):  budget := 0;  send^asyn(node(i+1 mod n), Id(v))
```

Each node sends its own id to its successor, then (once, `R = 1`) tries a non-blocking receive: `⊥` is the source's
receive timeout; its own id elects it and it stops; another id may be adopted as the value to forward; in every
iteration that did not stop it retransmits `to_send`. `usend` is the environment's duplication: each node may send
one of its messages twice. Finally the node reports its `leader` flag to the observer.

## The pairs

### correct — expected: **conforms**

```
⟨adopt v⟩ ≜ v > to_send
```

The Chang–Roberts filter: `to_send` is the largest id the node has seen, starting at its own, so every id node `j`
forwards is at least `j`. An id `i < n−1` would have to pass node `n−1`, whose `to_send ≥ n−1 > i`, so it never
returns to `i`: no node other than the top receives its own id. Duplication and reordering only repeat or permute
ids under the same bound. Every implementation word is the observer receiving `n` statuses in some order with at
most the top's flag `true`, which is a specification word (`e` = whether the top was elected; the order is free on
both sides; there is one visible thread).

### no-compare — expected: **violation** (`n ≥ 2`)

```
⟨adopt v⟩ ≜ true        ◀ fault: `v > to_send` dropped, every other id is adopted
```

The seeded fault removes the comparison `inv_leader_top` rests on. Schedule at `n = 2`: `node0` sends `Id(0)`;
`node1` sends `Id(1)`, receives `Id(0)`, adopts `0` and sends `Id(0)`; `node0` receives `Id(0)`, its own id, and is
elected. The uncovered word (and its reverse order) is

```
observer: R(Status(0, true)) R(Status(1, false))
```

The specification elects only node `n−1`. At `R = 1` the fault breaks `inv_leader_top` but not `U_0`: two
different leaders would need two loop iterations.

## What it is for

A ring protocol under a hostile environment (non-blocking receives standing for timeouts and loss, nondeterministic
duplication, unordered delivery) against a one-thread oracle: the implementation has orders of magnitude more graphs
than the specification (202 against 4 at `n = 2`) and one visible thread, so almost every implementation graph is
covered by the same few specification graphs.
