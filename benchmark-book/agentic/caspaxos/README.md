# caspaxos(A, K) — the CASPaxos register, implemented from the paper (`caspaxos`)

**Communication model:** bag (`asyn`), the model both ports declare (the Rust crate's default `Bag`; the Java port's
`bag` = JMC's `ASYN`). **Languages:** Rust port in `rust/` (crate `bench_caspaxos`, entry `imp`, TraceForge's Must
API); Java port in `java/` (`bench.caspaxos.CasPaxos`, JMC's message-passing API); the specification in
`rust/src/spec.rs` (entry `spec` of the same crate, hand-written by the coordinator). **Visible threads:** the clients
`c0 … c{K−1}`; the proposers `p0, p1` and the acceptors `a0 … a{A−1}` are invisible (in the specification: the
register `reg`). **Parameters:** `A` = the number of acceptors, `K` = the number of clients; `P = 2` proposers and
`R = 1` retry are constants; `F = ⌊(A − 1)/2⌋`, a quorum is `F + 1` acceptors. Sizes: `(A, K)` = `(3, 2)`, `(3, 3)`,
`(5, 2)`.
**Paper:** Denis Rystsov, *CASPaxos: Replicated State Machines without logs*, arXiv:1802.07000v9 (preprint, 2018),
§2.1–2.2 (`PAPER-EXCERPT.md`).
**Agents:** two context-free Opus 5.5 agents, one per language, each given the paper excerpt, the program interface
(`INTERFACE.md`) and the API sources only, under the prompt `PROMPT.md`; neither was told what the program is for.
Their own records of the choices the paper leaves open: `NOTES-rust.md`, `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; `send^M_t(to, v)` sends with tag `t` and `recv^M(tag = t)` receives
only a message with tag `t` (as in `../../synthetic/reset/README.md`); `recv^M(P)` receives only a message matching
the pattern `P` (`_` = any field). Messages fixed by the interface: `Cas(e, v)` ("if the value is `e` then set it to
`v`"), its replies `Done(v)` (the value after the change) and `Fail`. A ballot is `b = (ctr, id)`, ordered on `ctr`
first; `⊥` is below every ballot. `apply(e, v, x) = (x = e ? v : x)`. Addresses are not messages: both ports
publish thread ids in shared tables before any message that leads to their use (spawn order `p*`, `a*`, `c*`).

The specification (both pairs that have it):

```
Spec(A, K) ≜ reg: x := 0
                  for 1..K:
                     Cas(e, v) := recv^asyn()
                     if nondet({true, false}):  x := apply(e, v, x);  send^asyn(c_e, Done(x))
                     else:  if nondet({true, false}):  x := apply(e, v, x)
                            send^asyn(c_e, Fail)
           ‖ c_i: send^asyn(reg, Cas(i, i + 1));  r := recv^asyn()                      (i = 0 … K−1)
```

`reg` is a linearizable register serving the requests in arrival order: it applies each compare-and-set and answers
`Done` with the new value, or answers `Fail` with the change applied or not (a proposer that gives up may still have
had its value accepted by a quorum). Client `ci` asks "if `i` then `i + 1`", so `reg` replies to `c_e`. `reg` spawns
the clients to know their ids; spawn edges are invisible. `A` is unused.

## The pairs

### Rust implementation against the specification — expected: **conforms**

```
Rust(A, K) ≜ c_i: send^asyn_i(p_{i mod 2}, Cas(i, i + 1));  r := recv^asyn()           -- r ∈ {Done(_), Fail}
           ‖ p_j: ctr := 0;  pend := [i < K | i mod 2 = j]
                  while pend ≠ []:
                     i := nondet(pend);  pend := pend − i                 -- a choice only when |pend| > 1 (K > 2)
                     Cas(e, v) := recv^asyn(tag = i)
                     reply := Fail
                     for 0..R:
                        ctr := ctr + 1;  b := (ctr, j)
                        for k := 0..A−1:  send^asyn(a_k, Prepare(b))
                        q := quorum(b, 1)
                        if q = Err(s):  ctr := max(ctr, s.ctr);  continue
                        y := the value of the highest-ballot accepted tuple in q's Promises, else 0
                        for k := 0..A−1:  send^asyn(a_k, Accept(b, apply(e, v, y)))
                        q := quorum(b, 2)
                        if q = Ok(_):  reply := Done(apply(e, v, y));  break
                        else Err(s):   ctr := max(ctr, s.ctr)
                     send^asyn(c_i, reply)
                  for k := 0..A−1:  send^asyn(a_k, Stop)

             quorum(b, ph) ≜ oks := [];  nc := 0;  g := ⊥
                             loop:  m := recv^asyn(tag = (b, ph))
                                    if m = Conflict(b, s):  nc := nc + 1;  g := max(g, s)
                                                            if nc > A − (F + 1):  return Err(g)
                                    else:  oks := oks · m;  if |oks| = F + 1:  return Ok(oks)

           ‖ a_k: prom := ⊥;  acc := ⊥;  stops := 0
                  while stops < 2:
                     seen := max(prom, ballot(acc))
                     m := recv^asyn()
                     if m = Prepare(b):    if seen > b:  send^asyn_{(b,1)}(p_{b.id}, Conflict(b, seen))
                                           else:  prom := b;  send^asyn_{(b,1)}(p_{b.id}, Promise(b, acc))
                     if m = Accept(b, x):  if seen > b:  send^asyn_{(b,2)}(p_{b.id}, Conflict(b, seen))
                                           else:  prom := ⊥;  acc := (b, x);  send^asyn_{(b,2)}(p_{b.id}, Accepted(b))
                     if m = Stop:          stops := stops + 1
```

Client `ci` sends its compare-and-set to proposer `p(i mod 2)` (tagged with its index) and waits for the reply.
Proposer `pj` serves its clients one at a time (which one next is a `nondet` choice, then it receives that client's
`Cas` by tag); per request it makes at most `1 + R` attempts, each with a fresh ballot: phase 1 sends `Prepare` to
every acceptor and waits for `F + 1` promises, takes the value of the highest-ballot accepted tuple among them (0 if
none), applies the change and sends `Accept` to every acceptor; phase 2 waits for `F + 1` `Accepted`. A phase is given
up as soon as more than `A − (F + 1)` conflicts arrived; the counter then jumps to the largest conflicting ballot's
counter (NOTES-rust 1, 2, 7). Replies are tagged by (ballot, phase), so stale replies are never received (NOTES-rust 3).
Acceptor `ak` answers a `Prepare` or `Accept` whose ballot is below the largest it has seen with a `Conflict`,
otherwise promises (with its accepted tuple) or accepts (erasing the promise); it ends after a `Stop` from each
proposer (NOTES-rust 8), leaving any unread request in its mailbox.

How the two sides differ: the specification has one register; the implementation has ballots, quorums, conflicts and
retries, all invisible. Why it conforms: a `Done(y)` is the value of the change applied at the ballot whose
`Accept` reached a quorum, and every later quorum's `Promise` carries it, so the successful changes form one sequence
(the register's); a `Fail` is a proposer giving up, whose `Accept` may or may not have reached a quorum — both are
`reg`'s `Fail` branches. The causal order matches as well: every operation reads through a phase-1 quorum and writes
through a phase-2 quorum, and two quorums share an acceptor, so the reply of the operation ordered second is causally
after the request of the one ordered first, as it is through `reg`. The design note
(`plan/twins-contextfree/README-paper-to-twin.md`, "Specification lessons") keeps the sequential register for this
reason. The tag on the client's `Cas` is not part of the compared encoding (`traceforge/src/conformance/canon.rs`
excludes the send's tag).

### Java implementation against the specification — expected: **conforms**

```
Java(A, K) ≜ c_i: send^asyn(p_{i mod 2}, Cas(i, i + 1));  r := recv^asyn(Done(_) ∨ Fail)
           ‖ p_j: ctr := 0;  sent := 0
                  for 1..|{i < K | i mod 2 = j}|:
                     Cas(e, v) := recv^asyn(Cas(_, _))
                     done := false
                     for 0..R while ¬done:
                        ctr := ctr + 1;  b := (ctr, j)
                        for k := 0..A−1:  send^asyn(a_k, Prepare(b));  sent := sent + 1     -- once per broadcast
                        ok := 0;  nc := 0;  best := ⊥;  y := 0
                        while ok < F + 1 ∧ nc ≤ A − (F + 1):
                           m := recv^asyn(Promise(b, _) ∨ Conflict(1, b, _))
                           if m = Promise(b, t):   ok := ok + 1;  if t = (bt, yt) ≠ ⊥ ∧ bt > best:  best := bt;  y := yt
                           if m = Conflict(1, b, s):  nc := nc + 1;  ctr := max(ctr, s.ctr)
                        if ok < F + 1:  continue
                        for k := 0..A−1:  send^asyn(a_k, Accept(b, apply(e, v, y)));  sent := sent + 1
                        ok := 0;  nc := 0
                        while ok < F + 1 ∧ nc ≤ A − (F + 1):
                           m := recv^asyn(Accepted(b) ∨ Conflict(2, b, _))
                           if m = Accepted(b):        ok := ok + 1
                           if m = Conflict(2, b, s):  nc := nc + 1;  ctr := max(ctr, s.ctr)
                        if ok ≥ F + 1:  done := true
                     send^asyn(c_e, done ? Done(apply(e, v, y)) : Fail)
                  for k := 0..A−1:  send^asyn(a_k, Stop(j, sent))
           ‖ a_k: prom := ⊥;  acc := ⊥;  stops := 0;  expect := 0;  handled := 0
                  while stops < 2 ∨ handled < expect:
                     m := recv^asyn()
                     if m = Stop(_, n):  stops := stops + 1;  expect := expect + n;  continue
                     handled := handled + 1;  (ph, b) := the phase and ballot of m
                     seen := max(prom, ballot(acc))
                     if seen > b:  send^asyn(p_{b.id}, Conflict(ph, b, seen));  continue
                     if m = Prepare(b):    prom := b;  send^asyn(p_{b.id}, Promise(b, acc))
                     if m = Accept(b, x):  prom := ⊥;  acc := (b, x);  send^asyn(p_{b.id}, Accepted(b))
```

The same protocol with the Java agent's choices: the proposer takes any `Cas` in arrival order and replies to
`c_e` (NOTES-java 9, 10); the receive guards select the current ballot and, for conflicts, the phase (NOTES-java 6);
the counter is raised on every conflict received (NOTES-java 5); the acceptor's `Stop` carries how many requests the
proposer sent it, and the acceptor ends only after handling all of them (NOTES-java 8). The constructor refuses an even
`A` ("A must be odd and positive (2F + 1)"). The argument for conformance is the one of the Rust pair: one sequence
of successful changes through intersecting quorums, `Fail` with or without effect.

### The two implementations against each other — expected: **conforms**

Both directions (the Rust port's graphs covered by the Java port's, and back). The visible threads and the client
messages are the same on both sides; the differences, from the code and the two notes:

1. **The retry counter.** Rust raises `ctr` only from the conflicts of the phase that failed (`quorum` returns `Err(g)`;
   conflicts received during a phase that still reached its quorum are dropped); Java raises it on every conflict
   received, including those of a successful phase 1 followed by a failed phase 2 (NOTES-rust 7, NOTES-java 5). So the
   retry ballot can differ, and with it which schedules end in `Fail`. Neither note declares a visible effect, and we
   expect (not shown by a run) that every combination of client outcomes reached by one port is reached by the other
   through another schedule, so the expected verdict stays "conforms".
2. **Choosing the next request.** Rust chooses with `nondet` which of its clients to serve and then waits for that
   client's `Cas` by tag; Java takes whichever `Cas` arrives. Only at `K = 3` (`p0` serves `c0` and `c2`); both give
   every service order, so nothing visible differs.
3. **Acceptor termination.** Rust's acceptor stops after two `Stop`s, and under bag a `Stop` may overtake its sender's
   earlier requests, which then stay unread; Java's acceptor handles every request first. No proposer waits on those
   replies, and acceptors are invisible.
4. **Even `A`.** Java refuses it; Rust accepts it with quorum `F + 1 = A/2`, which for even `A` is not a majority.
   The sizes are `A = 3, 5`, so this is immaterial here.
5. **Internal message shapes.** Rust's `Promise` carries an `Option`, the reply phase travels in the tag; Java
   flattens the tuple and carries the phase in `Conflict`. Invisible.

## What it is for

The first paper-to-twin protocol: the same paper section given twice to an agent with no context yields two programs
that agree on every choice the prompt names (give-up rule, retry with a new ballot, an acceptor-stop message) and
differ in the ones it does not (the counter update, termination bookkeeping), so the twin pair tests whether those
differences are visible. The specification is small (18 graphs at `(3, 2)`, 162 at `(3, 3)`; the design note's
table) and the implementations are not: the Rust port was not exhausted in 10 minutes at `(3, 2)`, the Java port
passed 117 848 graphs in 10 minutes, and the in-process Rust checks at `(3, 2)` reached the 4 h limit (`EVAL.tex`).
