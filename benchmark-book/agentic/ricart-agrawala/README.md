# Ricart–Agrawala mutual exclusion, paper-to-twin (`ricart-agrawala`)

**Communication model:** bag (`asyn`), the model both ports declare (Rust default `ConsType::Bag`; Java `bag` →
`CommunicationModel.ASYN`). **Languages:** Rust port (TraceForge's Must API) in `rust/` (crate
`bench_ricart_agrawala`, `rust/src/main.rs`, entry `imp`); Java port (JMC's message-passing API) in `java/`
(`bench.ricart_agrawala.RicartAgrawala`); the specification in `rust/src/spec.rs` (entry `spec`, written by the
coordinator, using the Rust port's `Msg` type). **Visible threads:** the clients `c0 … c{N−1}`; the nodes
`n0 … n{N−1}`, the specification's `lock` and `main` are invisible. **Parameters:** `N` = the number of nodes, one
client each (default 3; sizes run: 3, 4); `K = 1` critical section per client (fixed); no failures.
**Paper:** Glenn Ricart, Ashok K. Agrawala, *An Optimal Algorithm for Mutual Exclusion in Computer Networks*,
CACM 24(1), 1981, section 2 (`PAPER-EXCERPT.md`). **Agents:** two context-free Opus 5.5 agents, one per language,
given the paper excerpt and the program interface only (`PROMPT.md`, `INTERFACE.md`); their decisions are in
`NOTES-rust.md` and `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; in addition `x := recv^M(P)` is a blocking receive of a message
addressed to the executing thread that matches the guard `P` (Java's selective receive by class), and `recv^M()`
accepts any message. Messages: `Enter(i)`, `Exit(i)` (client → node or lock, `i` = the client's index),
`Granted`, `Released` (node or lock → client), and the nodes' `Request(k, j)` (the paper's REQUEST with sequence
number `k` and node number `j`) and `Reply` (REPLY, no content). Addresses are known from the start on both
ports (Rust: thread ids computed from the spawn order and asserted after spawning; Java: all `MpThread`s are
constructed, minting their ids, before any is started); they are not shown.

## The specification

```
Spec(N) ≜ c_i: send^asyn(lock, Enter(i)); g := recv^asyn(); send^asyn(lock, Exit(i)); r := recv^asyn()   (i = 0..N−1)
        ‖ lock: holder := ⊥;  waiting := []
                for 1..2N:
                   m := recv^asyn()
                   if m = Enter(c):  if holder = ⊥:  holder := c;  send^asyn(c_c, Granted)
                                     else            waiting := waiting · c
                   if m = Exit(c):   if waiting ≠ []:
                                        k := 0;  while k + 1 < |waiting| ∧ ¬nondet({true, false}):  k := k + 1
                                        holder := waiting[k];  remove waiting[k];  send^asyn(c_holder, Granted)
                                     else  holder := ⊥
                                     send^asyn(c_c, Released)
```

`c_i` asks for the critical section, waits for `Granted`, leaves it and waits for `Released`. `lock` serves the
`2N` client messages in arrival order: an `Enter` on a free lock is granted at once, otherwise queued; on an `Exit`
any queued client (a chain of binary choices) is granted next, and only then is the leaving client answered (the
paper's deferred replies before the release). `lock` spawns the clients so that it knows their ids (the spawn
edges are invisible).

## The Rust port

```
Impl_rs(N) ≜ c_i: send^asyn(n_i, Enter(i)); g := recv^asyn(); send^asyn(n_i, Exit(i)); r := recv^asyn()
           ‖ n_i: our := 0;  high := 0;  out := 0;  req := false;  deferred := [false]^N;  left := false;  sent := 0
                  while ¬(left ∧ sent = N − 1):
                     m := recv^asyn()
                     if m = Enter(i):      req := true;  our := high + 1;  out := N − 1
                                           for j := 0..N−1, j ≠ i:  send^asyn(n_j, Request(our, i))
                                           if out = 0:  send^asyn(c_i, Granted)
                     if m = Reply:         out := out − 1
                                           if out = 0 ∧ req:  send^asyn(c_i, Granted)
                     if m = Request(k, j): high := max(high, k)
                                           if req ∧ (k > our ∨ (k = our ∧ j > i)):  deferred[j] := true
                                           else  send^asyn(n_j, Reply);  sent := sent + 1
                     if m = Exit(i):       req := false
                                           for j := 0..N−1:  if deferred[j]:  deferred[j] := false;  send^asyn(n_j, Reply);  sent := sent + 1
                                           send^asyn(c_i, Released);  left := true
```

`n_i` is the paper's three processes of node `i` merged into one that handles its messages in arrival order, so
the semaphore `Shared_vars` is not needed. The client's `Enter` is "mutual exclusion is invoked": the node picks
`our = high + 1` and sends `Request` to every other node. A `Request(k, j)` raises `high` and is deferred when
the node is requesting and has priority (smaller sequence number, ties broken by the smaller node number),
otherwise answered at once. The last `Reply` is the paper's `WAITFOR`: the node enters and sends `Granted`; it
is in its critical section until `Exit`, which sends the deferred replies and then `Released`. The node ends
after its own section and after `N − 1` replies (each other node requests once). `high` is updated only on a
received `Request` (the paper's code; its comment says "sent or received"); with `K = 1` a node chooses its
number once, so this is immaterial. Unexpected messages panic (never happen).

## The Java port

```
Impl_java(N) ≜ c_i: send^asyn(n_i, Enter(i)); g := recv^asyn(Granted); send^asyn(n_i, Exit(i)); r := recv^asyn(Released)
             ‖ n_i: as in Impl_rs, except
                     if m = Reply:  out := out − 1
                                    if out = 0:  send^asyn(c_i, Granted)                ← no test of req
```

The same reading of the paper, decision by decision (`NOTES-java.md` 1–6): one process per node, arrival order,
`high` updated on receipt only, sends in increasing node index, the same termination rule. The field of `Request`
is called `node` instead of `from` (internal).

## The pairs

### Rust implementation against the specification — expected: **conforms**

`Impl_rs(N)` against `Spec(N)` (in-process stateful and stateless, and offline join). The specification has one
invisible lock where the port has `N` nodes exchanging `Request`/`Reply`; the visible words are the same,
`c_i: S(Enter(i)) R(Granted) S(Exit(i)) R(Released)`. Ricart–Agrawala guarantees mutual exclusion: if `c_a` enters
before `c_b`, then `n_a` answers `n_b`'s `Request` only at or after its own `Exit` (deferred, or received after it), so `c_b`'s
`R(Granted)` is causally after `c_a`'s `S(Exit(a))`, as in the specification, where `lock` grants `c_b` only on
receiving `c_a`'s `Exit`. Which waiting client goes next is fixed by sequence numbers and arrival order in the
port and is a free choice in the specification, so the cover is many-to-one. Runs (EVAL.pdf, 2026-11-03): at
`N = 3`, 36 specification graphs, 32 800 implementation graphs, conforms (stateful, stateless, offline); `N = 4`
reached the 4 h tier.

### Java implementation against the specification — expected: **conforms**

`Impl_java(N)` against `Spec(N)`: offline (the Java export joined with the Rust specification export) and online
(JMC exploring the Java port against the specification export). The argument is that of the Rust pair: the Java
port differs from the Rust port only in invisible guards (below). Runs: at `N = 3`, conforms offline and online
(36 specification graphs, 32 800 Java graphs); `N = 4` online reached the 4 h tier.

### The two implementations against each other — expected: **conforms** (both directions)

`Impl_java(N)` against `Impl_rs(N)` and `Impl_rs(N)` against `Impl_java(N)` (offline joins both ways, online both
tools). The differences between the ports:

- The `Reply` handler: Rust grants when `out = 0 ∧ req`, Java when `out = 0`. A `Reply` arrives only in answer to
  the node's own `Request`, sent on `Enter` with `req := true`, and `req` becomes false only on `Exit`, which comes
  after `Granted`, i.e. after the last `Reply`; so the test is always true. No effect on the verdict.
- The clients' receives: Java guards them by class (`Granted`, `Released`), Rust accepts any message. A client
  only ever receives `Granted` and then `Released`, and the second is sent after the client's `Exit`, which is
  after the first; so the guard never excludes a message. No effect.
- Names: Java's `Request(seq, node)` against Rust's `Request { seq, from }`; Java's `programName()` is
  `ricart_agrawala`. Internal messages are invisible. No effect.
- Both update `high` only on a received `Request` (the same reading; immaterial at `K = 1`).

Both have 32 800 graphs at `N = 3`, and the joins conform in both directions, offline and online (the online
TraceForge-against-Java row is at a superseded pin, rerun pending).

## What it is for

A classic mutual-exclusion algorithm implemented twice from the paper alone, by agents that never saw each other's
code, checked against a one-thread lock. The specification is a few lines and has 36 graphs at `N = 3` against the
ports' 32 800, so the check is many-to-one; the ports' only differences are invisible guards, so the twin
comparison is the paper-to-twin experiment's baseline: two independent readings of a precisely specified algorithm
give the same visible behaviour.
