# paxos(L) — Paxos, 2 proposers, 3 acceptors (`paxos-oopsla25`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: the source's message soup, without duplication); the program declares bag
as its default `--model`, and the campaign ran bag. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java
twin in `java/` (`bench.paxos_oopsla25.Paxos`, args `<spec|impl>,<L>[,<bag|fifo>[,<variant>]]`); context-free
Java port in `java-cf/` (`bench.paxos_oopsla25.PaxosOopsla25`). **Visible threads:** `c0`, `c1` (the clients, one
proposal each) and `l0 … l(L−1)` (the learners, one decision each); the specification's `oracle` and the
implementation's proposers `p1`, `p2` (ballots 1, 2; `p(i+1)` serves `c(i)`) and acceptors `a0`, `a1`, `a2` are
invisible. **Parameters:** `1 ≤ L ≤ 3` = the number of learners (default 1; the Rust `parse` asserts the range);
fixed: 2 clients (values 1, 2), 2 proposers, 3 acceptors, majority 2, one ballot per proposer (ballot = proposer
index). ARGS `<L> [variant]`; the specification ignores the variant. **Source:** Bílý, Pereira, Müller, OOPSLA
2025 §5.3, the artifact's `refinement-examples/src/bin/ex05_paxos.rs` (single-decree Paxos after Lamport's TLA+
`Paxos`); the source has no learners and no clients, which the benchmark adds.

Notation: as in `../../synthetic/naive/README.md`; `recv^{asyn,nb}()` is non-blocking and may return `⊥`. Every
thread receives on its own channel; a receive takes any pending message addressed to the executing thread.
Messages are `Propose(v)`, `Msg1a(b)`, `Msg1b(a, b, vote)` with `vote ∈ {⊥, Vote(b', v)}`, `Msg2a(b, v)`,
`Msg2b(a, b, v)`, `Decide(v)`. The clients and the learners are the same on both sides except for the thread a
client proposes to (`T0`, `T1` below), which the comparison does not see:

```
Clients(T0, T1) ≜ c0: send^asyn(T0, Propose(1)) ‖ c1: send^asyn(T1, Propose(2))
Learners(L)     ≜ ‖_{j<L} l_j: m := recv^asyn()                                   (m = Decide(v))
```

Each client proposes once (`c_i` proposes `i + 1`); each learner blocks on one `Decide`.

## The pairs

The specification, a consensus oracle:

```
Spec(L) ≜ Clients(oracle, oracle) ‖ Learners(L)
        ‖ oracle: vals := []
                  for 2 times:  m := recv^{asyn,nb}();  if m = Propose(v) then vals := vals · v
                  if vals ≠ [] and nondet({true, false}) then
                     v := nondet(vals)
                     for j := 0..L−1:  send^asyn(l_j, Decide(v))
```

The oracle reads each proposal non-blocking (it may leave any unread, so the decision depends causally only on the
proposals it read), then decides nothing or one value it read, and tells every learner the same value. When it
decides nothing every learner ends blocked; when it decides, every learner receives `Decide(v)` for the one `v`
(8 complete and 7 blocked graphs at either `L`, README.orig §2).

The implementation, the source's `run_leader_ballot` and `run_acceptor` with the decision announced to the learners:

```
Impl_v(L) ≜ Clients(p1, p2) ‖ Learners(L) ‖ p1 ‖ p2 ‖ a0 ‖ a1 ‖ a2

p_b ≜ Propose(mine) := recv^asyn()
      for a := 0..2:  send^asyn(a_a, Msg1a(b))
      Q := ∅;  maxv := ⊥
      repeat at most 3 times while |Q| < MAJ:
         m := recv^asyn()
         if m = Msg1b(a, b, vote) and a ∉ Q:   Q := Q ∪ {a}
                                              if vote = Vote(vb, vv) and (maxv = ⊥ or vb > ballot(maxv)) then maxv := vote
      v := VALUE
      for a := 0..2:  send^asyn(a_a, Msg2a(b, v))
      A := ∅
      repeat at most 3 times while |A| < MAJ:
         m := recv^asyn()
         if m = Msg2b(a, b, _) and a ∉ A:      A := A ∪ {a}
      if |A| ≥ MAJ then for j := 0..L−1:  send^asyn(l_j, Decide(v))

a_i ≜ maxbal := ⊥;  maxvote := ⊥
      for 4 times:
         m := recv^asyn()
         if m = Msg1a(b):     if maxbal = ⊥ or b > maxbal then  maxbal := b;  send^asyn(p_b, Msg1b(i, b, REPORT))
         if m = Msg2a(b, v):  if VOTE_OK(b) then  maxbal := b;  maxvote := Vote(b, v);  send^asyn(p_b, Msg2b(i, b, v))
```

`p_b` waits for its client's proposal, sends `Msg1a(b)` to every acceptor, reads up to 3 messages until it has
1b's of ballot `b` from `MAJ` distinct acceptors (a message of another kind consumes an iteration), proposes
`VALUE` (`choose_value()`: the value of the highest-ballot vote reported, else its own) in `Msg2a(b, v)`, reads up to
3 messages until `MAJ` distinct 2b's of ballot `b`, and, if it has them, sends `Decide(v)` to every learner.
Every receive is blocking: a ballot that never gets its majority waits for ever (the source's abandonment and the
soup's loss, both invisible). `a_i` reads exactly 4 messages (2 prepares, 2 accepts): a 1a is promised if its ballot
is greater than `maxbal`, reporting `REPORT`; a 2a is voted if `VOTE_OK`. An acceptor sent fewer than 4 messages
ends blocked, unobserved. With two deciding proposers a learner receives two `Decide`s and reads one.

### correct — expected: **conforms**

```
MAJ ≜ 2 (⌊3/2⌋ + 1)       VALUE ≜ vv if maxv = Vote(_, vv), else mine       REPORT ≜ maxvote       VOTE_OK(b) ≜ maxbal = ⊥ or b ≥ maxbal
```

Paxos agreement: if ballot 1 decides `v`, a majority (two acceptors) voted `(1, v)`; ballot 2's majority intersects
it, so `p2` receives a 1b reporting `(1, v)` and proposes `v`. So in every implementation graph either no proposer
decides (every learner blocked, as in the oracle graphs that decide nothing) or every learner receives the same
`v ∈ {1, 2}`, and `c(v−1)`'s `Propose(v)` precedes every learner's receive causally (the value travels from the
client through its proposer, possibly an acceptor's vote and the other proposer). The oracle graph that reads
exactly that proposal and decides `v` has the same words and statuses (both clients done, the learners all done or
all blocked) and an order (that client's send before every learner's receive) contained in the implementation's.

### ignore-max-vote — expected: **violation** if `L ≥ 2`; **conforms** (control) if `L = 1`

```
VALUE ≜ mine        ◀ changed: the highest-ballot vote reported in the 1b's is ignored
```

Schedule at `L = 2`: `p1` gets promises from `a0`, `a1`, proposes 1, `a0`, `a1` vote, `p1` decides 1 and sends
`Decide(1)` to `l0`, `l1`. Then `a1` promises ballot 2 reporting `Vote(1, 1)` and `a2` promises (no vote); `p2`
ignores the vote and proposes its own 2; `a1` (`2 ≥ 2`) and `a2` vote, `p2` decides 2 and sends `Decide(2)` to
both learners. `l0` reads `p1`'s `Decide(1)`, `l1` reads `p2`'s `Decide(2)`. The uncovered behaviour is

```
c0: S(Propose(1))      c1: S(Propose(2))      l0: R(Decide(1))      l1: R(Decide(2))
```

In every specification graph the oracle sends one value to all learners, so no specification word has two learners
with different values (value). At `L = 1` the one learner reads one `Decide(v)`; `v` is a proposed value that
causally descends from `c(v−1)`'s `Propose` (no fault invents a value), so the oracle graph that reads that proposal
and decides `v` covers it, as for `correct`.

### skip-2a-check — expected: **violation** if `L ≥ 2`; **conforms** (control) if `L = 1`

```
VOTE_OK(b) ≜ true        ◀ changed: an acceptor votes for a 2a below its maxbal
```

Schedule at `L = 2`: `p1` gets promises from `a0`, `a1`; its `Msg2a(1, 1)`s are delayed. `p2` gets promises from
`a1` (now `maxbal = 2`, no vote yet) and `a2`, proposes 2, `a1`, `a2` vote, `p2` decides 2. Then `a0` and `a1` read
the delayed `Msg2a(1, 1)` and vote (`a1` despite `maxbal = 2`), `p1` decides 1. The same uncovered word as above
with `l0: R(Decide(1))`, `l1: R(Decide(2))` (value). Control at `L = 1` as above.

### half-majority — expected: **violation** if `L ≥ 2`; **conforms** (control) if `L = 1`

```
MAJ ≜ 1        ◀ changed: ⌊3/2⌋; quorums of one do not intersect
```

Schedule at `L = 2`: `p1` decides 1 with `a0` alone (promise, propose 1, vote), `p2` decides 2 with `a2` alone (no
vote reported). The same uncovered word (value). Control at `L = 1` as above.

### 1b-none — expected: **violation** if `L ≥ 2`; **conforms** (control) if `L = 1`

```
REPORT ≜ ⊥        ◀ changed: a 1b reports no vote instead of maxvote
```

Schedule at `L = 2`: as `ignore-max-vote`; `a1`'s 1b to ballot 2 hides its vote `(1, 1)`, so `p2` proposes 2. The
same uncovered word (value). Control at `L = 1` as above.

## What it is for

A related-work benchmark: the Paxos of an OOPSLA 2025 refinement library with four seeded faults, against a
one-thread oracle. Every fault is a value miss at two learners and invisible at one, so each gives a violating pair
and a conforming control from the same code by the size alone. The state spaces are lopsided: 8 complete oracle
graphs against 186 624 complete implementation graphs at `L = 1` (README.orig §7), with every proposer and acceptor
status invisible.
