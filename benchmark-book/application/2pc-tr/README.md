# 2pc-tr(N) — two-phase commit refining TCommit (`two-pc-trillium`)

**Communication model:** bag (`asyn`: UDP reorders; duplication is not modelled), the model the crate declares.
**Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.two_pc_trillium.TwoPcTrillium`);
context-free Java port in `java-cf/` (`TwoPcTrillium`). **Visible threads:** `obs0 … obs(N−1)`, the observer of
resource manager `i`'s final state; the oracle `tc` (specification), the transaction manager `tm` and the resource
managers `rm0 … rm(N−1)` (implementation) are invisible. **Parameters:** `N` = the number of resource managers
(default 2, smallest interesting size 2; the source's runner has 3); variant `correct` or `ignore-abort-vote`.
**Source:** Trillium's AnerisLang two-phase commit, `aneris/ml_sources/examples/transaction_commit/two_phase_code.ml`,
refinement target the Coq `TC_model` in `aneris/examples/transaction_commit/tc_model.v` (TLA+ `TCommit`).

Notation: as in `../../synthetic/naive/README.md`. Messages: `Prepare`, `Commit`, `Abort` (to an RM, on its own
inbox); `Prepared(i)`, `Aborted(i)`, `Committed(i)` (to `tm`, `i` the sending RM); `Committed`, `Aborted` (the
outcome, to an observer). Each thread's inbox carries one message type, so every receive below accepts any message
addressed to the thread. The observers are common to both sides:

```
Obs(N) ≜ ‖_{i<N} obs_i: o := recv^asyn()
```

Each observer receives one outcome, the final `TC_model` state (`COMMITTED` or `ABORTED`) of RM `i`.

The specification (both pairs):

```
Spec(N) ≜ Obs(N)
        ‖ tc: all := true
              for i := 0..N−1: if nondet({true, false}) then all := false
              o := (if all then Committed else Aborted)
              for i := 0..N−1: send^asyn(obs_i, o)
```

`tc` draws each RM's coin (`true` = local abort), decides `Committed` iff no coin said abort (`canCommit`), and
sends the one outcome to every observer (`TCConsistent`). It has `2^N` graphs and two visible words: every observer
receives `Committed`, or every observer receives `Aborted`.

## The pairs

### correct — expected: **conforms**

```
Impl(N) ≜ Obs(N) ‖ tm ‖ ‖_{i<N} rm_i

rm_i: m := recv^asyn()
      if m = Abort:                     send^asyn(tm, Aborted(i));  send^asyn(obs_i, Aborted)
      else if nondet({true, false}):    send^asyn(tm, Aborted(i));  send^asyn(obs_i, Aborted)
      else:
         send^asyn(tm, Prepared(i))
         d := recv^asyn()
         if d = Commit:  send^asyn(tm, Committed(i));  send^asyn(obs_i, Committed)
         else:           send^asyn(tm, Aborted(i));    send^asyn(obs_i, Aborted)

tm:   for i := 0..N−1: send^asyn(rm_i, Prepare)
      P := ∅;  all := true
      while |P| ≠ N:
         r := recv^asyn()
         if r = Prepared(j):  P := P ∪ {j}
         else:                all := false;  break
      if all:
         for i := 0..N−1: send^asyn(rm_i, Commit)
         F := ∅
         while |F| ≠ N:
            r := recv^asyn();  j := the sender of r
            if j ∉ F:  F := F ∪ {j};  assert r = Committed(j)
      else:
         for i := 0..N−1: send^asyn(rm_i, Abort)
```

`tm` sends `Prepare` to every RM and collects votes until every RM prepared, stopping at the first other response
(`recv_responses`). If all prepared it sends `Commit` to all and collects one `Committed` per RM (the source's
assertion is kept; it never fails); otherwise it sends `Abort` to all. An RM receives one message: on `Abort` it
aborts; on `Prepare` it flips its coin, aborts on `true`, and otherwise prepares and waits for the decision. Every
RM reports its final state to its observer as its last step (the only addition to the source).

The specification has no TM, no RMs and no votes: one oracle draws the coins and sends the outcome. If every coin
says prepare, the TM commits and every RM reports `Committed`. If some coin says abort, the TM breaks at the first
`Aborted` and sends `Abort` to all; every RM reports `Aborted` (an RM that prepared receives `Abort`; one that has
read nothing yet may, under bag, read `Abort` before `Prepare` and abort without a coin). So every implementation
graph gives all observers one outcome, `Committed` only if every coin said prepare, and nothing blocks; the
observers' receives are mutually unordered on both sides. Each such graph is covered by the specification graph with
the same outcome.

### ignore-abort-vote — expected: **violation**

```
Impl(N) ≜ the correct implementation with the vote collection
         while |P| ≠ N:
            r := recv^asyn()
            if r = Prepared(j) ∨ r = Aborted(j):  P := P ∪ {j}     ← changed: an abort vote counts as prepared
            else:                                 all := false;  break
```

The source's `(fst msg = "PREPARED") &&` test is dropped, so the TM always commits. At `N = 2`: `rm1`'s coin says
abort, it sends `Aborted(1)` and reports `Aborted`; `rm0` prepares; the TM's set is full and it sends `Commit`;
`rm0` reports `Committed`. The uncovered visible words: `obs0` receives `Committed`, `obs1` receives `Aborted` (and
the mirror image). No specification graph gives two observers different outcomes (signature value). The observers
finish; the TM then waits forever for a `Committed` from the aborted RM, which is invisible, so plain Must counts
these graphs blocked (6 of 10 at `N = 2`) while the conformance check treats them as complete (4 uncovered at
`N = 2`, 54 at `N = 3`).

## What it is for

A protocol checked against its abstract model: the specification is TLA+'s `TCommit` decision as one invisible
oracle, and the visible threads see only each RM's final state. The implementation's graphs grow much faster than
the specification's (`correct`: 12, 87, 888 at `N = 2, 3, 4` against `2^N` = 4, 8, 16), all with one of two visible
words, so the cover is many-to-one. The fault is a one-conjunct deletion in the TM that breaks agreement.
