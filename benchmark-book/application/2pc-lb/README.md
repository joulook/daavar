# 2pc-lb(N) — two-phase commit with retransmission and the counting bug (`two-pc-lbinria`)

**Communication models:** bag (`asyn`: the source's mailbox server runs with `"unordered"` boxes; the default of
both the Rust program and the Java twin) and fifo (`p2p`: per-channel order, selected with `--model fifo` in Rust, the model argument `fifo` in Java); each
variant is a pair under each model. **Languages:**
Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/`; context-free Java port, in `java-cf/`.
**Visible threads:** the resource managers `rm0 … rm(N−1)`; the transaction manager `tm` (both sides) and the
transports `p0 … p(N−1)` (implementation only) are invisible. **Parameters:** `N` = the number of resource managers
(`N ≥ 2`; runs at `N = 2, 3, 4`); the resend bound is fixed at `R = 2`. ARGS `<N> [correct|counting-bug]`.
**Source:** the Java 2PC of Cirstea et al. (trace validation), repository `lbinria-twophase/` (`protocol/TransactionManager.java`, `protocol/ResourceManager.java`, spec `spec/TwoPhase.tla`).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking; `tm` and the transports receive
`Prepared` messages on their own mailbox, an RM receives its decision on its own inbox. The source's timeouts are
`nondet` choices: the TM's abort timeout before each receive, and the RM's resend timeout as the number of times its
transport forwards (`1` or `2`). `abort_all ≜ for i := 0..N−1: send^asyn(rm_i, Abort)`, likewise `commit_all`.

The resource manager, the same code on both sides (`ResourceManager.run`); `s` is the thread it sends to (the
transport `pi` in the implementation, `tm` in the specification), both invisible:

```
RM_i(s) ≜ if nondet({true, false}) then stop                -- crash before preparing: no events
          send^asyn(s, Prepared(i))
          d := recv^asyn()                                   -- Commit or Abort
```

An RM either crashes (nothing visible) or sends `Prepared(i)` once and waits for the decision; its resends are not
its own events but the transport's.

## The pairs

The specification of every pair (`spec/TwoPhase.tla` with one TM oracle):

```
Spec(N) ≜ tm: for j := 1..N:
                 if nondet({true, false}) then abort_all; stop         -- TMAbort, enabled in "init"
                 Prepared(_) := recv^asyn()                            -- TMRcvPrepared
              d := nondet({Commit, Abort});  for i := 0..N−1: send^asyn(rm_i, d)    -- TMCommit or TMAbort
        ‖ rm0: RM_0(tm) ‖ … ‖ rm(N−1): RM_(N−1)(tm)
```

Before each of `N` receives the oracle may abort; after receiving `N` `Prepared` messages, one per RM, it commits or
still aborts. So it commits only when every RM's `Prepared` was received, each once. With an RM crashed the oracle
aborts or ends blocked, and the RMs that prepared receive `Abort` or end blocked.

The implementation (`TransactionManager.run` and one transport per RM):

```
Impl_V(N) ≜ tm: prepared := []
                for j := 1..N·R:
                   if nondet({true, false}) then abort_all; stop      -- abort timeout
                   Prepared(f) := recv^asyn()
                   ADD_V
                   if |prepared| ≥ N then commit_all; stop
          ‖ p0: T ‖ … ‖ p(N−1): T
          ‖ rm0: RM_0(p0) ‖ … ‖ rm(N−1): RM_(N−1)(p(N−1))

T ≜ m := recv^asyn();  k := nondet({1, …, R});  for j := 1..k: send^asyn(tm, m)
```

Transport `pi` reads its RM's one `Prepared` and forwards it once or twice (the RM's resend on timeout); a crashed
RM's transport ends blocked (invisible). The bound `N·R` is the number of `Prepared` messages the transports can
send, so it never cuts a run short.

### correct — expected: **conforms**

```
ADD_correct ≜ if f ∉ prepared then prepared := prepared ++ [f]           -- HashSet, as shipped
```

`tm` commits only when `prepared` holds `N` distinct RMs, that is, after receiving a `Prepared` from every RM, which
every RM sent once, visibly. The duplicates are invisible (transport sends, TM receives). The oracle has the same
decision points (abort before any receive, commit after all `N`), so every visible word and order of the
implementation is one of the specification's.

### correct under fifo — expected: **conforms**

```
Spec^fifo(N)          ≜ Spec(N)          with every send^asyn, recv^asyn replaced by send^fifo, recv^fifo
Impl^fifo_correct(N)  ≜ Impl_correct(N)  with the same replacement
```

In the specification every channel carries at most one message (each RM sends one `Prepared` to `tm`, `tm` sends
one decision to each RM), so fifo constrains nothing: its graphs are the bag specification's. The implementation's
fifo graphs are among its bag graphs (fifo only removes reads-from choices: a transport's second copy is read after
its first), so each is covered as under bag.

### counting-bug — expected: **violation**

```
ADD_counting-bug ≜ prepared := prepared ++ [f]                           ◀ changed: ArrayList, duplicates counted
```

The paper's bug, the commented-out line `this.preparedRMs = new ArrayList<>();`: a resent `Prepared` is counted
twice. At `N = 2`: `rm0` crashes, `rm1` sends `Prepared(1)`, `p1` forwards it twice, `tm` receives both, its list
has length `2 = N`, it commits:

```
rm0: ε (done)
rm1: S(Prepared(1)) R(Commit) (done)
```

Every specification graph in which an RM receives `Commit` has both `Prepared` sends; with `rm0` crashed the oracle
can only abort or block (value). Without a crash the same early commit is caught by the order: both RMs prepare,
`p0` forwards twice, `tm` commits on those two receives; both RMs receive `Commit`, the specification's word, but
`rm1`'s `S(Prepared(1))` is not causally before `rm0`'s `R(Commit)`. At `N = 2`, 12 complete implementation graphs
are uncovered (4 with a crashed RM, 8 with both prepared); 324 at `N = 3`.

### counting-bug under fifo — expected: **violation**

```
Spec^fifo(N)               ≜ Spec(N)               with every send^asyn, recv^asyn replaced by send^fifo, recv^fifo
Impl^fifo_counting-bug(N)  ≜ Impl_counting-bug(N)  with the same replacement
```

Fifo neither removes a retransmitted `Prepared` nor stops `tm` from counting one RM twice: the `N = 2` witness above
(`rm0` crashes, `p1` forwards `Prepared(1)` twice, `tm` reads both copies in order and commits) is a fifo graph, and
the fifo specification, whose graphs are the bag specification's, still cannot commit with `rm0` crashed.

## What it is for

A literature bug found by trace validation, reproduced as a conformance violation: the retransmission lives in
invisible threads, so the implementation has many more graphs than the specification (54 / 22 complete / blocked
against 12 / 3 at `N = 2`, 54248 / 18098 against 192 / 41 at `N = 4`, `correct`) while the visible behaviour is
the specification's. The blocked graphs include ones where only an invisible transport is blocked and every visible
RM finishes, so the check must compare visible-thread status, not whole-program completion. The fault shows both as
a value and as an order mismatch at the smallest size.
