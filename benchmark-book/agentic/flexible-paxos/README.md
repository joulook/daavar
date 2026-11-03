# fpaxos(A, K) — Flexible Paxos single-decree consensus, implemented from the paper (`flexible-paxos`)

**Communication model:** bag (`asyn`), the model both ports declare (the Rust crate's default `Bag`; the Java port's
`bag` = JMC's `ASYN`). **Languages:** Rust port in `rust/` (crate `bench_fpaxos`, entry `imp`, TraceForge's Must API);
Java port in `java/` (`bench.fpaxos.FPaxos`, JMC's message-passing API); the specification in `rust/src/spec.rs`
(entry `spec` of the same crate, hand-written by the coordinator). **Visible threads:** the clients `c0 … c{K−1}`; the
acceptors `a0 … a{A−1}` and the proposers `p0 … p{K−1}` (each also the learner of its own decision) are invisible (in
the specification: `cons`). **Parameters:** `A` = the number of acceptors, `K` = the number of clients and of
proposers (`P = K`), `R = 1` retry (constant); quorums of §4.1: `Q1 = ⌊A/2⌋ + 1` for phase 1 and `Q2 = A − Q1 + 1` for phase 2 (Rust), which
is `A/2` (Java) for every even `A`: `Q1 = 3`, `Q2 = 2` at `A = 4`; the Java port refuses an odd `A`. Sizes: `(A, K)` = `(4, 2)` (the interface's) and
`(5, 2)`.
**Paper:** Heidi Howard, Dahlia Malkhi, Alexander Spiegelman, *Flexible Paxos: Quorum intersection revisited*,
OPODIS 2016 (arXiv:1608.06696v1), §2, §3, §4.1 (`PAPER-EXCERPT.md`).
**Agents:** two context-free Opus 5.5 agents, one per language, each given the paper excerpt, the program interface
(`INTERFACE.md`) and the API sources only, under the prompt `PROMPT.md`; neither was told what the program is for.
Their own records of the choices the paper leaves open: `NOTES-rust.md`, `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; `send^M_t(to, v)` sends with tag `t` and `recv^M(tag = t)` receives
only a message with tag `t` (as in `../../synthetic/reset/README.md`; `tag = none` = untagged); `recv^M(P)` receives
only a message matching the pattern `P` (`_` = any field). Messages fixed by the interface: `Propose(f, v)` (client
`c_f` proposes `v`), its replies `Decided(v)` and `Fail`. A proposal number is `b = (round, id)`, ordered on `round`
first; `⊥` is below every number. Addresses are not messages (spawn order `a*`, `p*`, `c*`).

The specification (both pairs that have it):

```
Spec(A, K) ≜ cons: props := [];  pend := [];  dec := ⊥
                   for 1..K:
                      Propose(f, v) := recv^asyn()
                      props := props · v;  pend := pend · f
                      if dec = ⊥ ∧ nondet({true, false}):  dec := nondet(props)
                      reply(false)
                   if dec = ⊥ ∧ pend ≠ [] ∧ nondet({true, false}):  dec := nondet(props)
                   reply(true)

             reply(last) ≜ for f in pend, in order:
                              if dec ≠ ⊥:  send^asyn(c_f, nondet({Decided(dec), Fail}));  pend := pend − f
                              else if last ∨ nondet({true, false}):  send^asyn(c_f, Fail);  pend := pend − f

           ‖ c_i: send^asyn(cons, Propose(i, i + 1));  r := recv^asyn()                   (i = 0 … K−1)
```

`cons` receives the proposals in arrival order; after any proposal it may decide (by `nondet`) one of the values
received so far, once. A waiting client is answered with the decision or `Fail` once there is one, and may be
answered `Fail` earlier (a proposer may give up); at the end every waiting client is answered. Agreement: one value
in every `Decided`; validity: a proposed value, decided after it was proposed (causally, through `cons`). `cons`
spawns the clients to know their ids; spawn edges are invisible. `A` is unused.

## The pairs

### Rust implementation against the specification — expected: **conforms**

```
Rust(A, K) ≜ c_i: send^asyn(p_i, Propose(i, i + 1));  r := recv^asyn(on ch_i)                -- ch_i: see below
           ‖ p_j: Propose(f, v0) := recv^asyn(tag = none)
                  rd := 1;  out := Fail
                  for 0..R:
                     b := (rd, j)
                     for k := 0..A−1:  send^asyn(a_k, Prepare(b, p_j))
                     q := collect(b, 1, Q1)
                     if q = Err(h):  rd := h + 1;  continue
                     v := the value of the highest-numbered accepted proposal in q's Promises, else v0
                     for k := 0..A−1:  send^asyn(a_k, Propose2(b, v, p_j))
                     q := collect(b, 2, Q2)
                     if q = Ok(_):  out := Decided(v);  break
                     else Err(h):   rd := h + 1
                  for k := 0..A−1:  send^asyn(a_k, Done)
                  send^asyn(ch_f, out)

             collect(b, ph, Q) ≜ oks := [];  n := 0;  h := b.round
                                 loop:  m := recv^asyn(tag = 2·b.round + ph)
                                        if m = Nack(b, q):  n := n + 1;  h := max(h, q.round)
                                        else:  oks := oks · m
                                        if |oks| ≥ Q:  return Ok(oks)
                                        if n > A − Q:  return Err(h)

           ‖ a_k: prom := ⊥;  acc := ⊥;  done := 0
                  while done < K:
                     m := recv^asyn()
                     if m = Prepare(b, p):      if b > prom:  prom := b;  send^asyn_{2·b.round+1}(p, Promise(b, acc))
                                                else:  send^asyn_{2·b.round+1}(p, Nack(b, prom))
                     if m = Propose2(b, v, p):  if b ≥ prom:  prom := b;  acc := (b, v);  send^asyn_{2·b.round+2}(p, Accept(b))
                                                else:  send^asyn_{2·b.round+2}(p, Nack(b, prom))
                     if m = Done:               done := done + 1
```

Client `ci` proposes `i + 1` to proposer `pi` and waits for the reply, which arrives on a named TraceForge channel
`ch_i = ("fpaxos-client", i)` that both the client and the proposer build from the index, not on the client's thread
inbox (NOTES-rust D1). Proposer `pj` runs at most `1 + R` attempts: phase 1 sends `prepare(b)` to every acceptor and
waits for `Q1` promises; it proposes the value of the highest-numbered accepted proposal among them, else its client's
value; phase 2 waits for `Q2` accepts, and then the value is decided. A phase is given up as soon as more than `A − Q`
nacks arrived, and the next round is one above the highest promised round reported (NOTES-rust D3, D5). Replies are
tagged by (round, phase), so only the current attempt's replies are received. After its last attempt the proposer
sends `Done` to every acceptor and then replies. Acceptor `ak` promises a strictly higher number and accepts a number
at least its promise, otherwise nacks with its promise; it ends after `K` `Done`s (NOTES-rust D4). `Q2 = A − Q1 + 1`.

How the two sides differ: the specification has one decision point; the implementation has proposal numbers,
two quorums, nacks and retries, all invisible. Why it conforms: every two phase-1 quorums and every phase-1 and
phase-2 quorum intersect (`Q1 + Q2 > A`, `2·Q1 > A`), so every proposer that decides learns the value accepted by the
first phase-2 quorum (agreement); a decided value is some client's value carried through promises, so the client that
decides it is causally after that value's `Propose` (validity); a proposer that runs out of retries answers `Fail`,
which the specification allows at any point. Through the intersecting quorums the second operation's reply is causally
after the first operation's proposal, as it is through `cons`.

### Java implementation against the specification — expected: **conforms** (`A` even only)

```
Java(A, K) ≜ c_i: send^asyn(p_i, Propose(i, i + 1));  r := recv^asyn(Decided(_) ∨ Fail)
           ‖ p_j: Propose(f, v0) := recv^asyn(Propose(_, _))
                  rd := 1;  dec := false
                  for 0..R while ¬dec:
                     h := rd
                     for k := 0..A−1:  send^asyn(a_k, Prepare(rd, j))
                     pr := 0;  n := 0;  best := ⊥
                     while pr < Q1 ∧ n ≤ A − Q1:
                        m := recv^asyn(Promise(_, rd, j, _) ∨ Nack(_, 1, rd, j, _))
                        if m = Promise(_, rd, j, t):  pr := pr + 1;  if t ≠ ⊥ ∧ ballot(t) > ballot(best):  best := t
                        if m = Nack(_, 1, rd, j, q):  n := n + 1;  h := max(h, q.round)
                     if pr < Q1:  rd := h + 1;  continue
                     v := (best ≠ ⊥ ? value(best) : v0)
                     for k := 0..A−1:  send^asyn(a_k, ProposeP2(rd, j, v))
                     ac := 0;  n := 0
                     while ac < Q2 ∧ n ≤ A − Q2:
                        m := recv^asyn(Accept(_, rd, j) ∨ Nack(_, 2, rd, j, _))
                        if m = Accept(_, rd, j):      ac := ac + 1
                        if m = Nack(_, 2, rd, j, q):  n := n + 1;  h := max(h, q.round)
                     if ac ≥ Q2:  dec := true  else:  rd := h + 1
                  send^asyn(c_f, dec ? Decided(v) : Fail)
                  for k := 0..A−1:  send^asyn(a_k, Done(j))
           ‖ a_k: prom := ⊥;  acc := ⊥;  done := 0
                  while done < K:
                     m := recv^asyn(Prepare(_, _) ∨ ProposeP2(_, _, _) ∨ Done(_))
                     if m = Prepare(b):        if b > prom:  prom := b;  send^asyn(p_{b.id}, Promise(k, b, acc))
                                               else:  send^asyn(p_{b.id}, Nack(k, 1, b, prom))
                     if m = ProposeP2(b, v):   if b ≥ prom:  prom := b;  acc := (b, v);  send^asyn(p_{b.id}, Accept(k, b))
                                               else:  send^asyn(p_{b.id}, Nack(k, 2, b, prom))
                     if m = Done(_):           done := done + 1
```

The same protocol with the Java agent's choices: the client receives on its thread inbox; acceptors address the
proposer by the number's `id` (no `reply_to`); the receive guards select the current round, the proposer and, for
nacks, the phase (NOTES-java 5); `Q2 = A/2`; the proposer replies to its client before sending `Done` (NOTES-java 7).
The constructor refuses an odd `A` ("A must be even and >= 2"), so this pair exists only at `(4, 2)`. The argument for
conformance is the one of the Rust pair.

### The two implementations against each other — expected: **conforms** (`A` even only)

Both directions. The visible threads and the client messages are the same; the differences, from the code and the two
notes:

1. **Odd `A`.** The Java port refuses it; the Rust port accepts it with `Q2 = A − Q1 + 1` (NOTES-rust D8), so at
   `(5, 2)` Rust has `Q1 = Q2 = 3` and there is no Java program. The interface fixes `A = 4` and §4.1's even case
   (`Q1 = 3`, `Q2 = 2`), where both ports agree; the twin pair and the Java pair run only at `(4, 2)`.
2. **The retry round.** Rust's next round is one above the highest promise reported by the nacks of the phase that
   failed (a successful phase's nacks are dropped by `collect`); Java's `h` accumulates the nacks of both phases of
   the attempt (NOTES-rust D5, NOTES-java 2). The retry number can differ, and with it which schedules end in `Fail`;
   neither note declares a visible effect, and we expect (not shown by a run) the same client outcomes on both sides.
3. **The client's inbox.** Rust's reply travels on the named channel `ch_i`, Java's to the client's thread inbox. The
   Rust agent flagged the risk (NOTES-rust D1: "if the harness observes only thread-mailbox receives on visible
   threads, this differs"); the compared encoding leaves the receive's location out (`traceforge/src/conformance/
   canon.rs`: `RecvLoc.locs` is excluded), so we expect no difference. No twin or specification run has finished to
   confirm it, and the specification's clients use the thread inbox, so the risk applies to the Rust pair too.
4. **Order at the end of a proposer.** Rust sends `Done` to the acceptors and then replies; Java replies and then sends
   `Done`. Both are sends of an invisible proposer; nothing visible depends on their order.
5. **Internal shapes.** Rust's `Prepare`/`Propose2` carry `reply_to`, replies travel tagged; Java's replies carry the
   acceptor's index and the phase. Invisible.

## What it is for

The quorum-flexibility protocol of the set: the safety argument rests on `Q1`/`Q2` intersection rather than
majorities, and the two agents read §4.1's even case differently outside it (Java refuses odd `A`, Rust extends the
formula), which the size `(5, 2)` exposes as an asymmetric benchmark: the Rust pair runs there and the Java pairs do
not. The specification is small (84 graphs at `(4, 2)`, `EVAL.tex`), the implementations are not: the Java agent's run
passed 44 436 graphs in 600 s at `(4, 2)` without finishing, the Rust agent's did not finish in 300 s, and the
in-process Rust checks at `(4, 2)` reached the 4 h limit.
