# 2pc-ds(N) — multi-round two-phase commit with acknowledgements (`two-pc-disel`)

**Communication models:** bag (`asyn`: Disel's message soup, the default) and fifo (`p2p`: per-channel order); the
verdicts are the same under both. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/`;
context-free Java port, in `java-cf/`. **Visible threads:** the participants `p0 … p(N−1)`; the coordinator `cn` is
invisible. **Parameters:** `N` = the number of participants (`N ≥ 2`; runs at `N = 2, 3`); the number of rounds is
fixed at `K = 2`, with data `d_0 = [1, 2]`, `d_1 = [3, 4]` (the first two items of the app's `data_seq`). ARGS
`<N> [correct|skip-ack-loop]`.
**Source:** Disel's two-phase commit, `disel/theories/Examples/TwoPhaseCommit/` (`TwoPhaseProtocol.v`, `TwoPhaseCoordinator.v`, `TwoPhaseParticipant.v`, `SimpleTPCApp.v`).

Notation: as in `../../synthetic/naive/README.md`; `M` is the model (`bag` or `fifo`). `recv^M(Prep)`,
`recv^M(Dec)`, `recv^M(Vote)`, `recv^M(Ack)` are blocking receives that accept only a message of that kind (Disel's
tag-filtered `tryrecv` spin loops; tags prep 0, vote 1, decision 3, ack 5; type guards in Java). The decision kind
is `Commit(e) | Abort(e)`, the vote kind `Yes(i, e) | No(i, e)`, the ack kind `CommitAck(i, e) | AbortAck(i, e)`.
The source's round and sender filters never discard a message in these programs (each participant votes and acks
once per round, and a round-`e+1` message needs the participant's own round-`e` messages), so each spin loop is
one receive and the filter is an assertion no run violates.

The participant code, the same on both sides (`participant_with_choices`, the vote `doCommit` free):

```
P_i ≜ for e := 0..K−1:
         Prep(e, d) := recv^M(Prep)
         if nondet({true, false}) then send^M(cn, Yes(i, e)) else send^M(cn, No(i, e))
         c := recv^M(Dec)                                     -- Commit(e) or Abort(e)
         if c = Commit(e) then send^M(cn, CommitAck(i, e)) else send^M(cn, AbortAck(i, e))
```

Each round, a participant reads the round's prepare request with its data, votes freely, reads the decision
(appending it to its log in the source) and sends the matching acknowledgement.

## The pairs

The coordinator of every program, with two switches (`free`, `skip`):

```
CN(free, skip) ≜ for e := 0..K−1:
                    for i := 0..N−1: send^M(pi, Prep(e, d_e))
                    all_yes := true
                    for j := 1..N:  v := recv^M(Vote);  if v = No(_, e) then all_yes := false
                    commit := (if free then all_yes ∧ nondet({true, false}) else all_yes)
                    for i := 0..N−1: send^M(pi, if commit then Commit(e) else Abort(e))
                    if ¬skip then
                       for j := 1..N:  _ := recv^M(Ack)
```

The specification is `Spec(N) ≜ cn: CN(true, false) ‖ p0: P_0 ‖ … ‖ p(N−1): P_(N−1)`: per round the coordinator
prepares everyone, collects one vote from each, commits only if every vote is yes and then still by a free choice
(abort is always allowed, `TCommit`'s rule), sends the decision to everyone, and collects one ack from each before
the next round.

### correct — expected: **conforms** (bag and fifo)

```
Impl_correct(N) ≜ cn: CN(false, false) ‖ p0: P_0 ‖ … ‖ p(N−1): P_(N−1)
                                                           -- commit := all_yes (read_resp_result)
```

The source's coordinator: the specification's free choice fixed to "commit iff all yes". Both coordinators collect
every vote and every ack of round `e` before sending `Prep(e+1)`, so every implementation graph is a specification
graph with the same visible events, values and causal order.

### skip-ack-loop — expected: **violation** (bag and fifo)

```
Impl_skip(N) ≜ cn: CN(false, true) ‖ p0: P_0 ‖ … ‖ p(N−1): P_(N−1)
               -- in CN:  if ¬skip then for j := 1..N: _ := recv^M(Ack)          ◀ changed: never run
```

Fault F1 of the catalogue (ours): `receive_commit_loop` / `receive_abort_loop` are dropped, so after sending the
round-`e` decision the coordinator starts round `e+1` at once and never reads an ack. Every participant still sees
the specification's events and values per round, and nothing blocks. But at `N = 2`:

```
p0: R(Prep(0,[1,2])) S(vote) R(Dec(0)) S(Ack(0)) R(Prep(1,[3,4]))
p1: R(Prep(0,[1,2])) S(vote)                         -- p1 has not acked round 0
```

In every specification graph `p1`'s `S(Ack(0))` is causally before `p0`'s `R(Prep(1, …))`, because the coordinator
reads every round-0 ack before sending any `Prep(1)`; in the implementation it need not be. Values and statuses
match, the order does not: every complete implementation graph is uncovered (64 of 64 at `N = 2`, 2304 of 2304 at
`N = 3`). Under fifo too: the tag-filtered receives already keep a `Prep(e+1)` that overtakes `Decision(e)` from
being consumed as the decision, so per-channel order adds nothing, and it does not order another participant's ack
before the next `Prep`.

## What it is for

A literature protocol with two models and an order-only fault: the faulty implementation has the specification's
words and statuses on every participant and differs only in a cross-thread causal edge (ack → next prepare), so a
check that compared words alone would miss it. The specification's free abort makes it larger than the
implementation (400 against 256 graphs at `N = 2`, 104976 against 82944 at `N = 3`).
