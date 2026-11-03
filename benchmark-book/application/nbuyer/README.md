# nbuyer(n) — n buyers coordinate one purchase (`n-buyer-kragl`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: the source's channels are multisets, `[pid][msg]int` counters); the Rust
program declares no default, the campaign passes `--model bag` (the model argument `bag` in Java). **Languages:**
Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.n_buyer_kragl.NBuyer`); context-free Java
port in `java-cf/` (`bench.n_buyer_kragl.NBuyerKragl`). **Visible threads:** `ledger`, added by the benchmark (the
source's property is on the ghost map `contribution` and the seller's `assert_sum`, neither a message); the
specification's `oracle` and the implementation's `seller` and `b1 … bn` are invisible. **Parameters:** `n` = the
number of buyers, `n ≥ 2` (the source's axiom `n > 1`; the Rust program rejects a smaller `n`); fixed:
`price = 2`, `wallet = 1` (the smallest instance in which `Final(true)` needs two contributions). ARGS
`<n> [correct|no-subtract]`, default variant `correct`; the specification side ignores the variant. **Source:**
Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive Sequentialization of Asynchronous Programs", PLDI 2020,
artifact file `boogie/Test/civl/inductive-sequentialization/NBuyer.bpl` (layer 1).

Notation: as in `../../synthetic/naive/README.md`; `x := recv^bag(tag = T)` is a blocking receive of a message of
kind `T` (the Rust program's tagged receives, the Java twin's type tests). Messages: `Req` (b1 to the seller,
`send_req`), `Quote(q)` (seller to a buyer, `send_quote`), `Rem(r)` (buyer to the next buyer, `send_rem`), `Dec(d)`
(last buyer to the seller, `send_dec`), and the two the ledger receives, `Contrib(i, a)` (buyer `i`'s
`set_contribution(i, a)`, made a message) and `Final(d)` (the decision `sellerFinish` received, in place of its
assertion). The ledger and the buyers' choice are common to both sides:

```
Ledger(n) ≜ ledger: for n+1 times:  m := recv^bag()

choose(r) ≜ nondet({min(wallet, r), 0})                     (the source's `if (*)`)
```

The ledger only receives, `n + 1` messages in any order (bag), and sends nothing.

The specification, the sequentialization `MAIN3`/`INV3`: the buyers act in pid order in one invisible thread, the
seller's finish last:

```
Spec(n) ≜ Ledger(n)
        ‖ oracle: rem := price;  sum := 0
                  for i := 1..n:  a := choose(rem);  rem := rem − a;  sum := sum + a;  send^bag(ledger, Contrib(i, a))
                  send^bag(ledger, Final(sum = price))
```

`2^n` choice vectors; each sends `n` contributions and one decision, true iff they sum to the price. Graphs:
`(n+1)! · 2^n` (24, 192, 1920, 23040 at `n = 2..5`).

The implementation, the layer-1 procedures as one thread per role (`sellerInit`/`sellerFinish`,
`firstBuyerInit`/`firstBuyer`, `middleBuyer`, `lastBuyer`), in program order:

```
Impl_v(n) ≜ Ledger(n) ‖ seller ‖ ‖_{1≤i≤n} b_i

seller ≜ _ := recv^bag(tag = Req)
         for i := 1..n: send^bag(b_i, Quote(price))
         Dec(d) := recv^bag(tag = Dec)
         send^bag(ledger, Final(d))

b_i ≜ if i = 1 then send^bag(seller, Req)
      Quote(q) := recv^bag(tag = Quote)
      if i = 1 then r := q  else  Rem(r) := recv^bag(tag = Rem)
      a := choose(r)
      send^bag(ledger, Contrib(i, a))
      if i < n then send^bag(b_{i+1}, Rem(⟨fwd⟩))
      else          send^bag(seller, Dec(a = r))
```

The seller quotes the price to every buyer after b1's request and reports the last buyer's decision. b1 contributes
from the quote, every later buyer from the remainder it receives, and `b_n` decides whether its contribution
clears the remainder. Each inbox holds at most one message of each kind, so the only delivery nondeterminism is the
ledger's receive order.

## The pairs

### correct — expected: **conforms**

```
⟨fwd⟩ ≜ r − a
```

`b_i` receives `r_i = price − a_1 − … − a_{i−1}` (b1: `r_1 = price`), the oracle's `rem` at step `i`, and chooses
`a_i` from the same set `{min(wallet, r_i), 0}`; `b_n` sends `Dec(a_n = r_n)`, and `a_n = r_n` iff
`a_1 + … + a_n = price`. So each choice vector gives the same `n + 1` messages to the ledger on both sides, with the
same `Final`. The ledger is the only visible thread, it sends nothing, and its receives are from invisible senders in
any order on both sides, so the graphs correspond one to one (`(n+1)! · 2^n` on each side) and every implementation
graph is covered by the specification graph with the same vector and the same receive order.

### no-subtract — expected: **violation** (`n ≥ 2`)

```
⟨fwd⟩ ≜ r        ◀ changed: a first or middle buyer forwards the remainder it received, not r − a
```

Every buyer now receives `r_i = price = 2` and `b_n` decides `a_n = 2` with `a_n ≤ wallet = 1`, so the
implementation always reports `Final(false)`. At `n = 2`: b1 chooses `a_1 = min(1, 2) = 1`, sends `Contrib(1, 1)` and
`Rem(2)` instead of `Rem(1)`; b2 chooses `a_2 = min(1, 2) = 1`, sends `Contrib(2, 1)` and `Dec(1 = 2) = Dec(false)`;
the seller sends `Final(false)`. The uncovered word, in each of its `3!` receive orders:

```
ledger: R(Contrib(1, 1)) R(Contrib(2, 1)) R(Final(false))
```

The specification sends `Final(true)` with contributions `(1, 1)`, and no other vector gives these two
contributions (signature value). The other three vectors give `Final(false)` with the same contributions on both
sides, so exactly `3! = 6` of the 24 implementation graphs are uncovered at `n = 2`. At larger `n` every buyer may
contribute 1, and the uncovered graphs are those with at least two contributions of 1: with exactly two the
specification produces the same vector with `Final(true)`, with more it never produces the vector (the oracle's
remainder is 0 after two). This is the run in which `sellerFinish` would skip `assert_sum`: it breaks the invariant
the inductive-sequentialization proof carries (`INV3`: the remainder in flight equals the price minus the
contributions so far).

## What it is for

An asynchronous program checked against its inductive sequentialization, the one proof artefact the source ships:
the specification is one thread acting for every buyer in pid order, the implementation the message-passing
protocol. The only visible thread is a passive ledger, so the comparison is on the values it receives and the
graphs grow as `(n+1)!` from its receive order alone; the fault is the one that breaks the proof's invariant and is
visible at the smallest size.
