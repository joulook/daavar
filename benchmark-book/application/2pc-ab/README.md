# 2pc-ab(n) — two-phase commit with early abort and abstaining participants (`two-pc-abstain-kragl`)

**In the paper:** Table 2 (conforming): `2pc-ab` = variant `correct`, model bag, size 8, 9. Table 3 (violating): `2pc-ab-off1` = variant `off-by-one`, model bag, size 8, 9. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** bag (`asyn`: unordered delivery; the source's channels are multisets; the Rust program's default is fifo, so its runs
of the two pairs below pass `--model bag` explicitly, while the Java twin defaults to bag). **Languages:**
Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/`; context-free Java port, in `java-cf/`.
**Visible threads:** `p1 … pn`; the coordinator `coord` is invisible. **Parameters:** `n` = the number of
participants (`n ≥ 1`, the source's `axiom n > 0`). ARGS `<n> [correct|off-by-one]`.
**Source:** Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive Sequentialization of Asynchronous Programs", PLDI 2020, artifact file `boogie/Test/civl/inductive-sequentialization/2PC.bpl`.

Notation: as in `../../synthetic/naive/README.md`; `recv^asyn(Req)`, `recv^asyn(Vote)`, `recv^asyn(Dec)` are
blocking receives that accept only a message of that kind (the code selects by message tag), so a participant's
request and decision, and the coordinator's votes, are separate kinds as the source's separate channels. A vote
carries no sender identity (the source's `VoteCH`).

The participant code, the same on both sides (`participant1` then `participant2`):

```
P ≜ if nondet({true, false}) then
       _ := recv^asyn(Req)
       v := nondet({Yes, No});  send^asyn(coord, Vote(v))
    d := recv^asyn(Dec)
```

A participant either takes part (reads the request, votes freely) or **abstains** (reads no request, sends no
vote); either way it then reads the decision. `bcast(d) ≜ for i := 1..n: send^asyn(pi, Dec(d))`.

## The pairs

The specification of every pair, `coord` running a message-level form of the source's atomic action `MAIN5`:

```
Spec(n) ≜ coord: for i := 1..n: send^asyn(pi, Req)
                 all_yes := true
                 for j := 1..n:
                    if nondet({true, false}) then bcast(Abort); stop      -- abort at any point
                    Vote(v) := recv^asyn(Vote);  if v = No then all_yes := false
                 d := (if all_yes ∧ nondet({true, false}) then Commit else Abort)
                 bcast(d)
        ‖ p1: P ‖ … ‖ pn: P
```

The coordinator sends a request to everyone; before each of `n` vote receives it may abort and stop; after `n`
votes it commits only if all were `Yes` (and may still abort). One decision goes to everyone. `MAIN5` itself is a
ghost predicate (`dec = COMMIT ⇒ ∀i. votes[i] = YES`); the program makes the commit evidence observable: a
`Commit` is causally after a received `Yes` from every participant, an `Abort` may come at any point.

The implementation, `coordinator1` and `coordinator2` as one thread:

```
Impl_q(n) ≜ coord: for i := 1..n: send^asyn(pi, Req)
                   d := Commit
                   for j := 1..q:
                      Vote(v) := recv^asyn(Vote)
                      if v = No then d := Abort; break               -- early abort, rest not drained
                   bcast(d)
          ‖ p1: P ‖ … ‖ pn: P
```

with `q = n` (`correct`) or `q = n − 1` (`off-by-one`).

### correct — expected: **conforms**

```
Impl_n(n)        -- q = n
```

The source's coordinator: read votes until `n` `Yes` or the first `No`, then send the decision. Every
implementation graph is matched by a specification graph that receives the same votes in the same order, taking
the receive branch each time; then, if the implementation broke at a `No`, the specification aborts at its next
abort choice (or, after the last vote, by `all_yes = false`); if the implementation read `n` `Yes`, it commits; if
the implementation blocks (some participant abstained and no `No` arrived, so a vote never comes), the specification
blocks on the same receive. The participants' events, their statuses and each decision's causal past are the same.
An abstention is the protocol's own behaviour (the source proves safety only), not a fault.

### off-by-one — expected: **violation**

```
Impl_{n−1}(n):   for j := 1..n − 1:  …                               ◀ changed: q = n − 1
```

The seeded fault (the source ships none): a quorum off-by-one, so the coordinator commits after `n − 1` `Yes`
votes. At `n = 1` it reads no vote and always sends `Commit`; two of the implementation's three graphs are
uncovered:

```
p1: R(Req) S(Vote(No)) R(Dec(Commit))      -- voted No, told Commit
p1: R(Dec(Commit))                          -- abstained, told Commit
```

The specification commits only after receiving a `Yes` from every participant, so neither is possible (value);
`p1` voting `Yes` and receiving `Commit` is covered. At `n = 2` (6 of 13 graphs uncovered) the fault also shows in
the order: with both voting `Yes` and only one vote read, a `Commit` has the other participant's `Yes` send outside
its causal past.

## What it is for

A literature protocol whose abstaining participants make blocked graphs an ordinary outcome on both sides
(implementation `correct`: 10 complete / 3 blocked at `n = 2`; specification 31 / 5), so the check compares
statuses as well as words. The specification's abort-at-any-point choice gives it many more graphs than the
implementation (17253 / 2491 against 2085 / 206 at `n = 5`), the fault is reachable at the smallest size, and at
`n ≥ 2` it shows both as a value and as an order mismatch.
