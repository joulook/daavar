# Ben-Or randomized consensus, paper-to-twin (`ben-or`)

**Communication model:** bag (`asyn`), the model both ports declare (Rust default `ConsType::Bag`; Java `bag` →
`CommunicationModel.ASYN`). **Languages:** Rust port (TraceForge's Must API) in `rust/` (crate `bench_ben_or`,
`rust/src/main.rs`, entry `imp`); Java port (JMC's message-passing API) in `java/` (`bench.ben_or.BenOr`); the
specification in `rust/src/spec.rs` (entry `spec`, written by the coordinator, using the Rust port's `Msg` type).
**Visible threads:** the clients `c0 … c{N−1}`; the processes `p0 … p{N−1}`, the specification's `cons` and `main`
are invisible. **Parameters:** `N` processes (default 3, the only size run), `t = 1` (a process waits for `N − t`
messages per step; nobody fails), `R = 2` rounds at most, `K = N` clients, client `c_i` proposing `i mod 2`
(so `0, 1, 0` at `N = 3`); the paper's coin is the API's choice. **Paper:** Michael Ben-Or, *Another Advantage of
Free Choice: Completely Asynchronous Agreement Protocols (Extended Abstract)*, PODC 1983, sections 2–3, step 3 in
its first variant (`PAPER-EXCERPT.md`). **Agents:** two context-free Opus 5.5 agents, one per language, given the
paper excerpt and the program interface only (`PROMPT.md`, `INTERFACE.md`); their decisions are in
`NOTES-rust.md` and `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; in addition `x := recv^M(P)` is a blocking receive of a message
addressed to the executing thread that matches the guard `P` (Rust's tagged receive, Java's selective receive),
and `recv^M()` accepts any message. Messages: `Input(c, v)` (client → process or `cons`: the initial value `v`,
the reply goes to `c_c`), `Decide(v)` and `Undecided` (the replies), and the processes' `Type1(r, x)` (the
paper's `(1, r, x_p)`) and `Type2(r, w)` with `w = v` for the D-message `(2, r, v, D)` and `w = ?` for `(2, r, ?)`.
`for q` ranges over `p0 … p{N−1}`, the sender included. Addresses are known from the start (Rust: `main` writes the
processes' ids, and each client its own id, into shared vectors that a process reads after its `Input`; Java: all
`MpThread`s are constructed, minting their ids, before any is started); not shown.

## The specification

```
Spec(N) ≜ c_i: send^asyn(cons, Input(i, i mod 2));  y := recv^asyn()                       (i = 0..N−1)
        ‖ cons: props := [];  pend := [];  dec := ⊥
                for 1..N:
                   Input(c, v) := recv^asyn();  props := props · v;  pend := pend · c
                   if dec = ⊥ ∧ nondet({true, false}):  dec := props[pick(|props|)]
                   answer(false)
                if dec = ⊥ ∧ pend ≠ [] ∧ nondet({true, false}):  dec := props[pick(|props|)]
                answer(true)

  answer(final) ≜ for c in pend:
                     if dec ≠ ⊥:   if nondet({true, false}):  send^asyn(c_c, Decide(dec))  else  send^asyn(c_c, Undecided)
                                   remove c from pend
                     else:         if final ∨ nondet({true, false}):  send^asyn(c_c, Undecided);  remove c from pend
  pick(n)      ≜ for j := 0..n−1:  if j + 1 = n ∨ nondet({true, false}):  return j
```

Consensus as a one-shot object: `cons` takes the proposals in arrival order; after any proposal it may decide one
of the proposals received so far (agreement: one value; validity: a value proposed before the decision); each
waiting client is answered, now or later, with the decision or with `Undecided` (a process may end undecided after
round `R`, also when others decided); at the end every client is answered. `cons` spawns the clients so that it
knows their ids (spawn edges invisible).

## The Rust port

```
Impl_rs(N) ≜ c_i: send^asyn(p_i, Input(i, i mod 2));  y := recv^asyn()
           ‖ p_i: Input(c, x) := recv^asyn(Input)                         -- the only untagged message
                  dec := ⊥
                  for r := 1..R:
                     for q:  send^asyn(q, Type1(r, x))                       -- tag 2r
                     cnt := [0, 0]
                     for 1..N−t:  Type1(r, v) := recv^asyn(Type1(r, _));  cnt[v] := cnt[v] + 1
                     w := (the v ∈ {0, 1} with 2·cnt[v] > N, else ?)
                     for q:  send^asyn(q, Type2(r, w))                       -- tag 2r + 1
                     d := 0;  dv := ⊥
                     for 1..N−t:  Type2(r, u) := recv^asyn(Type2(r, _));  if u ≠ ?:  d := d + 1;  dv := u
                     if dv ≠ ⊥:  x := dv                                     -- (a)
                                 if d > t ∧ dec = ⊥:  dec := dv;  send^asyn(c_c, Decide(dv))      -- (b)
                     else        x := nondet({1, 0})                         -- (c) the coin
                  if dec = ⊥:  send^asyn(c_c, Undecided)
```

`p_i` takes its initial value from its client, then runs the rounds: send `(1, r, x)` to all, wait for `N − t` of
them (which ones: the arrival order), send a D-message for a strict majority value or `?`, wait for `N − t`
type-2 messages; adopt the value of any D-message (a), decide it on more than `t` D-messages (b), else flip the
coin (c). Messages of a later round wait in the network (the guard on the round); the surplus messages of a round
are never received. A process that decides sends `Decide` at once and **keeps running the rounds up to `R`**
(`NOTES-rust.md` decision 3): the agent found that stopping deadlocks at `N = 3, t = 1` with inputs `0, 1, 0`
(below). A process undecided after round `R` sends `Undecided`.

## The Java port

```
Impl_java(N) ≜ c_i: send^asyn(p_i, Input(i, i mod 2));  y := recv^asyn(Decide ∨ Undecided)
             ‖ p_i: Input(c, x) := recv^asyn(Input)
                    for r := 1..R:
                       for q:  send^asyn(q, Type1(r, x))
                       cnt := [0, 0]
                       for 1..N−t:  Type1(r, v) := recv^asyn(Type1(r, _));  cnt[v] := cnt[v] + 1
                       w := (the v ∈ {0, 1} with 2·cnt[v] > N, else ?)
                       for q:  send^asyn(q, Type2(r, w))
                       d := 0;  dv := ⊥
                       for 1..N−t:  Type2(r, u) := recv^asyn(Type2(r, _));  if u ≠ ?:  d := d + 1;  dv := u
                       if d ≥ 1:  x := dv
                                  if d > t:  send^asyn(c_c, Decide(dv));  end           ← the process ends
                       else       x := nondet({0, 1})
                    send^asyn(c_c, Undecided)
```

The same reading of the paper in every step but one: a process that decides replies and **ends**, as INTERFACE.md
says ("A process ends after replying to its client"); `NOTES-java.md` decision 5 records the consequence that a
process still running may then block. `?` is encoded as `Type2(r, −1, false)`, a D-message as `Type2(r, v, true)`
(Rust: `Option`); internal, invisible.

## The pairs

### Rust implementation against the specification — expected: **conforms**

`Impl_rs(N)` against `Spec(N)`. Every process runs all `R` rounds, and in every round every process sends to all,
so each wait for `N − t` messages is satisfied and no execution blocks; every client gets exactly one reply.
Agreement: a D-message needs a strict majority of one value, so all D-messages of a round carry one value, and a
decision on more than `t` of them forces every process to adopt it. Validity in the specification's sense: a
`Decide(0)` needs a D-message for 0, which needs `Type1(r, 0)` from two processes, one of them `p0` or `p2`, so it
follows an input of 0; a `Decide(1)` can arise only from a coin (only `c1` proposes 1),
which needs `?` messages, which need `p1`'s `Type1`, which follows `c1`'s `Input`. The rounds, coins and surplus
messages are invisible. No run yet: the specification has 1 740 graphs; the agent's single exploration of the port
did not finish in 300 s (rows queued, `cluster/campaign/cf2/ra-vrr-benor.rows`).

### Java implementation against the specification — expected: **violation** (blocked clients)

`Impl_java(N)` against `Spec(N)`, offline join and online check. The divergence: at `N = 3` (inputs `0, 1, 0`)
`p0` and `p2` take each other's `Type1(1, 0)` and send `Type2(1, 0)`; both take the two D-messages, decide 0 in
round 1 and end. `p1` took its own `Type1(1, 1)` and one `0`, sent `Type2(1, ?)`, takes its own `?` and one D-message
(`d = 1`), adopts 0 and enters round 2, where it waits for `N − t = 2` messages `Type1(2, _)` and gets only its own.
`p1` blocks, and `c1` ends blocked on its receive after `S(Input(1, 1))`. Every specification graph is complete and
answers every client, so these graphs are uncovered (status); the complete Java graphs are expected to be covered as
in the Rust pair. No run yet; the agent's single exploration did not finish in 600 s (68 637 complete graphs, none
blocked among those).

### The two implementations against each other — expected: **violation** Java against Rust; **conforms** (not derived) Rust against Java

The differences between the ports, each with its effect on the verdict:

- **A decided process.** Rust keeps it in the rounds up to `R` (its later messages keep the others' waits
  satisfiable); Java ends it after `Decide`. Java has blocked graphs (the run above: `c1` without a reply) that
  Rust has not: `Impl_java` against `Impl_rs` is expected to be uncovered on those (status). In the other direction
  the Rust port has executions where a late process decides with the help of a decided process's later rounds
  (`p1` in the run above decides 0 in round 2); the same visible words (every client `Decide(0)`) exist in Java
  through other executions, but that each of them comes with the same causal order between the clients is not
  derived, so `Impl_rs` against `Impl_java` is expected to conform by the default rule only.
- **The `?` encoding.** Java's `Type2(r, −1, false)` against Rust's `Type2 { r, v: None }`. Internal, invisible. No
  effect.
- **The receive guards.** Java's clients receive `Decide ∨ Undecided`, Rust's any message; a process's first
  receive is `Input` by class (Java) or untagged (Rust; `Input` is the only untagged message); round guards are a
  predicate on `r` (Java) or the tags `2r`, `2r + 1` (Rust). The same messages are accepted. No effect.
- **The coin.** Rust `nondet()` → 1 or 0, Java `chooseIndex(2)` → 0 or 1: the same two branches in another order.
  Both flip also in round `R`, where the value is not used. No effect.
- **Addresses.** Rust passes them through shared memory (written before the reads they depend on); Java passes the
  ids minted at construction. Invisible. No effect.

No run yet (the twin rows `XC1`, `XC2` of the queue wait on both exports).

## What it is for

A randomized consensus protocol implemented twice from the paper alone, where the paper leaves a point open that
matters: what a process does after deciding. The interface fixed it one way ("ends after replying"), the Rust agent
departed from the interface because that reading deadlocks at the smallest size, the Java agent kept it and noted the
risk. The specification (a one-shot consensus object, 1 740 graphs at `N = 3`) has no blocked graph, so the checks
are expected to separate the two readings: the Rust port conforms, the Java port's blocked graphs are uncovered, and
the twin comparison shows the divergence in one direction.
