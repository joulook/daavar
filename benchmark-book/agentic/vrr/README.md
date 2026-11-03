# Viewstamped Replication, normal operation, paper-to-twin (`vrr`)

**Communication model:** bag (`asyn`), the model both ports declare (Rust default `ConsType::Bag`; Java `bag` →
`CommunicationModel.ASYN`). **Languages:** Rust port (TraceForge's Must API) in `rust/` (crate `bench_vrr`,
`rust/src/main.rs`, entry `imp`); Java port (JMC's message-passing API) in `java/` (`bench.vrr.Vrr`); the
specification in `rust/src/spec.rs` (entry `spec`, written by the coordinator, using the Rust port's `Msg` type).
**Visible threads:** the clients `c0 … c{K−1}`; the replicas `r0 … r{N−1}`, the specification's `svc` and `main`
are invisible. **Parameters:** `N = 2f + 1` replicas (default 3, `f = 1`), `K` clients with one request each
(default 2; sizes run: `3,2` and `3,3`); view 0, primary `r0`, no failures, so only the normal-operation protocol
runs (no view change, no recovery); the service is a counter, initially 0, with `add(n)` returning the new value.
**Paper:** Barbara Liskov, James Cowling, *Viewstamped Replication Revisited*, MIT-CSAIL-TR-2012-021, 2012,
sections 2–4.1 (`PAPER-EXCERPT.md`). **Agents:** two context-free Opus 5.5 agents, one per language, given the
paper excerpt and the program interface only (`PROMPT.md`, `INTERFACE.md`); their decisions are in
`NOTES-rust.md` and `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; in addition `x := recv^M(P)` is a blocking receive of a message
addressed to the executing thread that matches the guard `P` (Rust's tagged receive, Java's selective receive),
and `recv^M()` accepts any message. Messages: `Request(c, op)` (client → primary: the paper's REQUEST with
client-id `c`, request-number 1 implicit, operation `add(op)`), `Reply(x)` (primary → client: the counter after the
operation), and the replicas' `Prepare(v, m, n, k)`, `PrepareOk(v, n, i)`, `Commit(v, k)` (the paper's PREPARE,
PREPAREOK, COMMIT; `m` = the logged request `(c, s, op)`; `log[x]` is the entry of op-number `x`). Every replica message carries `v = 0` and every replica
drops a message with another view; this never happens and is not shown. Addresses are known from the start (Rust:
each process computes the ids from its own id and the spawn order, replicas then clients, and `main` asserts them
after spawning; Java: all `MpThread`s are constructed, minting their ids, before any is started); not shown.

The clients:

```
Clients(K) ≜ c_i: send^asyn(r0, Request(i, i + 1));  x := recv^asyn()          (i = 0..K−1)
```

## The specification

```
Spec(N, K) ≜ c_i: send^asyn(svc, Request(i, i + 1));  x := recv^asyn()
           ‖ svc: ctr := 0
                  for 1..K:
                     Request(c, op) := recv^asyn()
                     ctr := ctr + op;  send^asyn(c_c, Reply(ctr))
```

A sequential counter: `svc` executes the requests in arrival order and answers each with the new value. Every
operation passes through one process on both sides (the primary in the ports), so the causal order exists on both
sides. `svc` spawns the clients so that it knows their ids (spawn edges invisible); `N` is unused.

## The Rust port

```
Impl_rs(N, K) ≜ Clients(K)
  ‖ r0: opn := 0;  log := [];  cn := 0;  table := {};  ctr := 0;  oks := {};  ann := 0;  reqs := 0;  reps := 0
        while reps < K:
           m := recv^asyn()
           if m = Request(c, op):
              if table[c].s ≥ 1:  (if table[c].x ≠ ⊥:  send^asyn(c_c, Reply(table[c].x)));  skip the rest   -- never taken
              reqs := reqs + 1;  opn := opn + 1;  log := log · (c, 1, op);  table[c] := (1, ⊥)
              for j := 1..N−1:  send^asyn(r_j, Prepare(0, (c, 1, op), opn, cn))           -- tag opn
              ann := cn
           if m = PrepareOk(0, n, b):
              for x := 1..n:  oks[x] := oks[x] ∪ {b}
              while cn < opn ∧ |oks[cn + 1]| ≥ f:
                 (c, s, op) := log[cn + 1];  ctr := ctr + op;  cn := cn + 1
                 send^asyn(c_c, Reply(ctr));  table[c] := (s, ctr);  reps := reps + 1
              if cn = opn ∧ cn > ann ∧ reqs < K ∧ nondet({true, false}):                 -- the COMMIT timeout
                 for j := 1..N−1:  send^asyn(r_j, Commit(0, cn))                         -- tag 0
                 ann := cn
        if cn > ann:  for j := 1..N−1:  send^asyn(r_j, Commit(0, cn))
  ‖ r_b: opn := 0;  log := [];  cn := 0;  kc := 0;  table := {};  ctr := 0                  (b = 1..N−1)
        while cn < K:
           m := recv^asyn(Prepare(_, _, opn + 1, _) ∨ Commit(_, _))                       -- tag opn + 1 or tag 0
           if m = Prepare(0, (c, s, op), n, k):
              opn := opn + 1;  log := log · (c, s, op);  table[c] := (s, ⊥)
              send^asyn(r0, PrepareOk(0, opn, b));  kc := max(kc, k)
           if m = Commit(0, k):  kc := max(kc, k)
           while cn < kc ∧ cn < opn:
              (c, s, op) := log[cn + 1];  ctr := ctr + op;  cn := cn + 1;  table[c] := (s, ctr)
```

The primary `r0` (steps 2, 3, 5, 6 of section 4.1) handles messages in arrival order: a new `Request` gets the next
op-number, is logged and sent in a `Prepare` (tagged with its op-number) to the backups with the current
commit-number piggybacked; a `PrepareOk(n)` from backup `b` counts `b` for every op `1..n` ("this operation and all
earlier ones have prepared"), and every op in order with `f` such backups is executed, answered and committed. The
paper's "if the primary does not receive a new client request in a timely way" COMMIT is a choice while requests
may still come (`reqs < K`); after the `K`-th reply no request can come, so the last COMMIT is sent unconditionally
(the backups could not learn the last commit otherwise). A backup (steps 4, 7) takes only the next op-number's
`Prepare` (later ones wait in the network) or any `Commit`, answers `PrepareOk`, and executes committed ops that are
in its log, without replying; it ends after `K` executions, the primary after `K` replies. `PrepareOk`s left in
flight to the ended primary are never received.

## The Java port

```
Impl_java(N, K) ≜ c_i: send^asyn(r0, Request(i, i + 1));  x := recv^asyn(Reply)
  ‖ r0: opn := 0;  log := [];  cn := 0;  ctr := 0;  table;  high := [0]^N;  replied := 0;  last := 0
        while replied < K:
           m := recv^asyn(Request ∨ PrepareOk)
           if m = Request(c, op):
              if table[c].s ≥ 1:  (if executed:  send^asyn(c_c, Reply(table[c].x)));  skip the rest      -- never taken
              opn := opn + 1;  log := log · Request(c, op);  table[c] := (1, not executed)
              for j := 1..N−1:  send^asyn(r_j, Prepare(0, Request(c, op), opn, cn))
           if m = PrepareOk(0, n, b):
              high[b] := max(high[b], n)
              while cn < opn ∧ |{j ∈ 1..N−1 : high[j] ≥ cn + 1}| ≥ f:
                 Request(c, op) := log[cn + 1];  ctr := ctr + op;  cn := cn + 1
                 send^asyn(c_c, Reply(ctr));  table[c] := (1, ctr);  replied := replied + 1
              if cn = opn ∧ cn > last:
                 if opn = K ∨ nondet({true, false}):                                       -- the COMMIT timeout
                    for j := 1..N−1:  send^asyn(r_j, Commit(0, cn))
                    last := cn
  ‖ r_b: as in Impl_rs, with the guard  recv^asyn(Prepare(_, _, opn + 1, _) ∨ Commit(_, _))  as a predicate on the message
```

The same reading of section 4.1: arrival order at the primary, a per-backup highest acknowledged op-number (the
Rust port's per-op sets, in another form), commits in op order, a `Commit` that is a choice while requests may
still come (`opn < K`) and certain once all `K` requests were received, backups that accept `Prepare`s in op-number
order through a selective receive and remember a commit-number that overtook its `Prepare` (`NOTES-java.md`
decisions 1–4). The `Commit` is sent only from the `PrepareOk` branch.

## The pairs

### Rust implementation against the specification — expected: **conforms**

`Impl_rs(N, K)` against `Spec(N, K)`. The primary executes the operations in op-number order, which is the arrival
order of the `Request`s at `r0`, and answers each with the running sum, so `c_i` receives the sum of the operations
of the requests that reached `r0` up to its own; `svc` does the same with the arrival order at `svc`. The
`Prepare`/`PrepareOk`/`Commit` traffic, the backups and the COMMIT choice are invisible and only multiply the
graphs. Runs (EVAL.pdf, 2026-11-03): conforms at `3,2` (2 specification graphs, 868 implementation graphs) and at
`3,3` (6 against 1 194 756), stateful, stateless and offline.

### Java implementation against the specification — expected: **conforms**

`Impl_java(N, K)` against `Spec(N, K)`, offline join and online check. The argument is the Rust pair's. Runs:
conforms at `3,2` (868 Java graphs) and `3,3` (1 194 756), offline and online (these rows are at superseded pins,
reruns pending).

### The two implementations against each other — expected: **conforms** (both directions)

The differences between the ports, each with its effect on the verdict:

- **The COMMIT timeout.** Both ports take it as a choice under the same condition. Rust: `cn = opn ∧ cn > ann ∧
  reqs < K ∧ nondet`, and an unconditional `Commit` after the loop if `cn > ann`; Java: `cn = opn ∧ cn > last ∧
  (opn = K ∨ nondet)` inside the `PrepareOk` branch. `reqs = opn` in the Rust port (both are incremented together),
  so the choice is made in the same states; the Rust port also sets `ann := cn` when it sends a `Prepare`, but that
  piggybacked commit-number is always below `opn` when `cn = opn`, so `cn > ann` and `cn > last` agree there. The
  final `Commit` is sent in the same state (after the `K`-th reply, `cn = opn = K`), by Rust after leaving the loop
  and by Java before. The choice and the `Commit`s are invisible. No effect. (The declared divergence "Java sends
  COMMIT without a choice" is not what the code does: Java's `opn = K ∨ nondet` makes a choice whenever `opn < K`,
  as its `NOTES-java.md` decision 2 says.)
- **Counting `PrepareOk`s.** Rust adds `b` to the set of every op `1..n`; Java keeps `high[b] = max n`. The number of
  backups counted for op `x` is the same. No effect.
- **The receive guards.** The Java primary receives `Request ∨ PrepareOk` and the Java client `Reply`; the Rust
  primary and client receive any message (the primary ignores other kinds). Only those kinds are ever sent to
  them. The backups' guard is the same, expressed as a tag (Rust: `Prepare` tagged with its op-number, `Commit` with
  0) or as a predicate (Java). No effect.
- **Addresses.** Rust computes the ids from the spawn order and asserts them; Java passes the ids minted at
  construction. Invisible, no effect.

Both have 868 graphs at `3,2` and 1 194 756 at `3,3`; the joins conform in both directions, offline and online
(some online rows at superseded pins, reruns pending).

## What it is for

A state-machine-replication protocol implemented twice from the paper alone, against a sequential counter. The
specification has 2 and 6 graphs where the ports have 868 and 1 194 756: the replication is all invisible, so the
cover is strongly many-to-one. The paper leaves the COMMIT timeout ("in a timely way") open; both agents resolved it
the same way (a choice while requests may come, certain after the last), so the twin comparison shows that two
independent readings of the paper agree on everything a client can observe.
