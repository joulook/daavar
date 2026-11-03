# 2pc-le(n) — leader election, then two-phase commit (`election`)

**Communication model:** fifo (`p2p`); every row and every verdict below is under fifo (the crate and the twins
also accept `bag`, which was not checked). **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in
`java/` (`bench.election.Election`); context-free Java port in `java-cf/` (`bench.election.Election`).
**Visible threads:** `p0 … p(n−1)`; the oracle `oracle` (specification), the electors `e0 … e(n−1)`
(implementation) and the setup sender (`main` in Rust, the extra thread `init` in `java/`, `boot` in `java-cf/`)
are invisible. **Parameters:** `n` = the number of participants (and of electors); the implementation is `leader`
(all-to-all election) or `ring` (ring election), with a variant (`conf`, `split`, `silent`); ARGS
`leader {conf|split|silent} <n>` or `ring {conf|split} <n>`; smallest interesting size `n = 2`.
**Source:** written for this work: the TraceForge fixtures `2pc/leader` and `2pc/ring`
(`traceforge/src/conformance/demo.rs` at `a3a7e1b`), ported unchanged to `rust/`.

Notation: as in `../../synthetic/naive/README.md`. Messages to a participant are `BeLeader(ps)` (a list of
participants), `BeFollower(p)`, `Vote(b)`, `Decision(b)`; to an elector `Setup(i, es, ps)` (its index, the
electors, the participants), `Elect(j)` (leader) or `Tok(t)`, `Ann(l)` (ring). Every receive below accepts any
message addressed to the thread (one message type per thread, so the arrival order of a role and a peer's vote is
free). Both sides of every pair share the participants:

```
P(n) ≜ ‖_{i<n} p_i: peers := ⊥;  voted := false;  votes := 0;  yes := true
                    for 1..n:
                       m := recv^fifo()
                       if m = BeLeader(ps):   peers := ps
                       if m = BeFollower(l):  if ¬voted: voted := true;  send^fifo(l, Vote(true))
                       if m = Vote(b):        votes := votes + 1;  yes := yes ∧ b
                       if m = Decision(b):    stop
                       if peers ≠ ⊥ ∧ votes = |peers|:  for q ∈ peers: send^fifo(q, Decision(yes));  stop

announce(i, l) ≜ if i = l then send^fifo(p_i, BeLeader([p_j | j ≠ l])) else send^fifo(p_i, BeFollower(p_l))
```

A participant waits for its role; a follower sends one fixed `Vote(true)` to its leader; the leader, once it holds
`n − 1` votes, sends `Decision(true)` to every follower; a follower stops at the decision. The loop is bounded by
`n` receives (the leader's role and `n − 1` votes, or a follower's role, … , decision).

The specification (every pair):

```
Spec(n) ≜ P(n)
        ‖ oracle: Setup(_, _, ps) := recv^fifo()
                  l := n − 1;  for i := 0..n−2: if nondet({true, false}) then l := i; break
                  for i := 0..n−1: announce(i, l)
        ‖ init:   send^fifo(oracle, Setup(0, [], [p0 … p(n−1)]))
```

The oracle picks the leader freely (a ladder of boolean choices: the first `true` picks `p_i`, none picks
`p(n−1)`) and announces every role. Exactly one participant leads, and no role depends on a participant's event.
The participants are spawned first on both sides, so their ids agree.

## The pairs

### leader conf — expected: **conforms** (every `n ≥ 1`)

```
Impl(n) ≜ P(n)
        ‖ ‖_{i<n} e_i: setup := ⊥;  heard := [];  sent := false
                       loop:
                          m := recv^fifo()
                          if m = Setup(...): setup := m
                          if m = Elect(j):   heard := heard · j
                          if setup = ⊥: continue
                          if ¬sent: sent := true;  for j ≠ i: send^fifo(e_j, Elect(i))
                          if |heard| = n − 1:
                             l := max({i} ∪ heard)
                             announce(i, l);  stop
        ‖ init: for i := 0..n−1: send^fifo(e_i, Setup(i, [e0 … e(n−1)], [p0 … p(n−1)]))
```

Each elector, once it has its setup, sends `Elect(i)` to every other elector; having heard the `n − 1` others, it
takes the maximum index, its own included, and announces its own participant's role. Every elector computes
`l = n − 1`. The specification differs only in the invisible part: the election is replaced by a free choice, and
the choice `l = n − 1` (every `nondet` false) gives the same roles. In both programs every role is caused by
invisible events only, so each implementation graph (`p(n−1)` receives `BeLeader` and the votes in any order, then
decides `true`) has a specification graph with the same visible words and no extra visible order.

### ring conf — expected: **conforms** (every `n ≥ 1`)

```
Impl(n) ≜ P(n)
        ‖ ‖_{i<n} e_i: setup, tok, ann := ⊥;  started := false
                       for 1..3:
                          m := recv^fifo()
                          if m = Setup(...): setup := m
                          if m = Tok(t):     tok := t
                          if m = Ann(l):     ann := l
                          if setup = ⊥: continue
                          succ := e_{(i+1) mod n}
                          if ¬started: started := true;  if i = 0: send^fifo(succ, Tok(0))
                          if tok = t ≠ ⊥:
                             tok := ⊥
                             if i = 0:  announce(0, t);  if n > 1: send^fifo(succ, Ann(t));  stop
                             send^fifo(succ, Tok(max(t, i)))
                          if ann = l ≠ ⊥:
                             announce(i, l);  if i + 1 < n: send^fifo(succ, Ann(l));  stop
        ‖ init: for i := 0..n−1: send^fifo(e_i, Setup(i, [e0 … e(n−1)], [p0 … p(n−1)]))
```

`e0` starts a token `Tok(0)`; each elector forwards the running maximum `Tok(max(t, i))` to its successor; when the
token returns to `e0` it carries `n − 1`, and `e0` announces its role and sends `Ann(n − 1)` round the ring, each
elector announcing its participant's role and forwarding until `e(n−1)`. An elector's setup and the ring traffic come
from different senders and may arrive in either order; the loop stores what it receives and acts once the setup is
there. The leader is again `n − 1`, which the oracle can choose: conforms for the reason given for `leader conf`.

### leader split — expected: **violation** (`n ≥ 2`; conforms at `n = 1`)

```
Impl(n) ≜ the leader conf implementation with the line
                             l := max(heard), or i if heard = []           ← changed: own index left out
```

Each elector takes the maximum of what it received only. At `n = 2`, `e0` elects `1` and `e1` elects `0`: `p0`
receives `BeFollower(p1)` and `p1` receives `BeFollower(p0)`; each sends `Vote(true)` to the other and receives
its vote, and the loop ends after `n = 2` receives. No participant receives `BeLeader`, while every specification
graph has exactly one. Every implementation graph is uncovered (signature value). At `n ≥ 3`, `p(n−1)` follows
`p(n−2)` and every other participant follows `p(n−1)`: again no leader, and every graph ends blocked. At `n = 1` the
only elector has heard nothing and elects itself, as in `leader conf`.

### leader silent — expected: **violation** (every `n ≥ 1`)

```
Impl(n) ≜ the leader conf implementation with the lines
                             l := max({i} ∪ heard)
                             if l = i: stop                                 ← added: the winner's elector is silent
                             announce(i, l);  stop
```

The election is correct, but the elector that elects itself (`e(n−1)`) never announces. `p(n−1)` receives the
followers' votes but never its role, so it never decides; the followers wait for a decision. Every graph ends with
every participant blocked, each visible word a proper prefix of a specification word: the specification never
blocks (signature status). At `n = 1`, `p0` receives nothing and blocks.

### ring split — expected: **violation** (`n ≥ 2`; conforms at `n = 1`)

```
Impl(n) ≜ the ring conf implementation with the token step replaced by
                          if tok = t ≠ ⊥:                                   ← changed: decide alone, no Ann
                             announce(i, t)
                             if i ≠ 0: send^fifo(succ, Tok(max(t, i)))
                             stop
```

Each elector announces the leader named by the token it saw, without its own index and without waiting for the
announcement round. At `n = 2`, `e1` sees `Tok(0)` (`p1` follows `p0`) and `e0` sees the returning `Tok(1)` (`p0`
follows `p1`): no leader, every graph uncovered (signature value). At `n ≥ 3`, `p_i` follows `p(i−1)` for `i ≥ 1`
and `p0` follows `p(n−1)`: no leader, every graph blocked. At `n = 1`, `e0` sees its own `Tok(0)` and makes `p0`
the leader, as in `ring conf`.

## What it is for

The abstraction is a whole sub-protocol: the specification replaces an election by a free invisible choice, and the
check asks whether the election can produce an outcome the choice could not. The two implementations have the same
specification and very different costs: the all-to-all election interleaves `n(n − 1)` messages (`leader conf` has
588 graphs at `n = 3` and 1 839 744 at `n = 4` under plain Must), the ring is a chain (24 graphs at `n = 3`, 192 at
`n = 4`). The faults cover the two components a violation is caught by: a wrong value (`split`) and a blocked run
(`silent`).
