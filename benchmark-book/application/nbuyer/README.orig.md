# N-buyer purchase coordination (Kragl et al., PLDI 2020) against its sequentialization

**Family:** related-work  **Source:** Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive
Sequentialization of Asynchronous Programs", PLDI 2020, artifact file
`related-work-papers/sources/kragl-pldi20-artifact/inductive-sequentialization-pldi2020/boogie/Test/civl/inductive-sequentialization/NBuyer.bpl`
(candidate N20, record "KIS/NBuyer.bpl" in `CATALOGUE-artifacts-2.md`)  **Expected verdict:**
`correct`: **CONFORMS** (every contribution vector and decision of the protocol is one the
buyers-in-order specification produces); `no-subtract`: **DOES NOT REFINE** (b1 forwards the
whole price, so with contributions (1, 1) summing to the price the seller reports `Final(false)`).

## 1. The protocol

`NBuyer.bpl`, layer 1: a seller (pid 0) and `n > 1` buyers (pids 1..n) buy one item of `price`;
each buyer has `wallet`. `firstBuyerInit` sends a request to the seller (`send_req`);
`sellerInit` receives it and sends `price` as a quote to every buyer (`send_quote(i, price)`),
then `sellerFinish` receives the decision (`receive_dec`) and, on `true`, asserts
`sum(contribution, 1, n) == price`. `firstBuyer` receives its quote `q`, contributes
`amount := min(wallet, q)` or `0` (`if (*)`), and sends `q - amount` to buyer 2; `middleBuyer`
receives its quote and the remainder `r`, contributes `min(wallet, r)` or `0`, sends
`r - amount` on; `lastBuyer` does the same and sends `amount == r` to the seller. Channels are
multisets (`[pid][msg]int` counters): our `bag`.

Roles:

- Visible: `ledger` (added, see below).
- Invisible: spec `oracle`; impl `seller`, `b1 .. bn`.

Messages (Rust `Msg`, Java records with the same tags): `Req` (b1 → seller), `Quote(q)` (seller →
buyer), `Rem(r)` (buyer → next buyer), `Dec(d)` (last buyer → seller), and the two visible ones,
`Contrib { i, a }` (buyer i → ledger: the source's `set_contribution(i, a)`, made a message) and
`Final(d)` (seller → ledger: the decision `sellerFinish` received).

The source's property is about the ghost map `contribution` and the seller's assertion, neither of
which is a message. The ledger makes them observable: every buyer reports its contribution
before forwarding, and the seller reports the decision it received instead of asserting. The
ledger only receives (`n + 1` blocking receives of any message), so it sends and receives the same
on both sides (rule 3).

Parameters: `n` buyers (the size); `price = 2`, `wallet = 1` fixed (the catalogue's smallest
interesting instance: `Final(true)` needs two contributions of 1). Smallest size `n = 2`. Model
`bag` (the source's multiset channels). Each mailbox holds at most one message of each type except
the ledger's, so the only delivery nondeterminism is the ledger's receive order.

## 2. The specification program

One invisible `oracle`, the sequentialization `MAIN3`/`INV3` (buyers in pid order, with the
seller's finish last): `rem := price`; for i = 1..n: `a_i := min(wallet, rem)` or `0`
(`nondet()`), `rem -= a_i`, send `Contrib { i, a: a_i }` to the ledger; then send
`Final(sum a_i == price)`. One thread, so its sends are ordered by program order (rule 2; in the
implementation they are ordered through the `Rem` chain and the `Dec`).

The visible behaviour: the ledger reads the `n + 1` messages in any order (bag). Graphs:
`(n + 1)! · 2^n` (24, 192, 1920, 23040 for n = 2..5).

## 3. The implementation program

Threads `ledger`, `seller`, `b1 .. bn` (spawn order), each with one inbox; receives select by
message tag (Rust) / type (Java). `seller`: receive `Req`; send `Quote(2)` to b1..bn; receive
`Dec(d)`; send `Final(d)` to the ledger. `b1`: send `Req`; receive `Quote(q)`; `a :=
min(1, q)` or 0; send `Contrib { 1, a }`; send `Rem(q - a)` to b2. `b_i` (1 < i < n): receive
`Quote`, `Rem(r)`; choose `a`; send `Contrib { i, a }`, `Rem(r - a)`. `b_n`: receive `Quote`,
`Rem(r)`; choose `a`; send `Contrib { n, a }`, `Dec(a == r)`. This is the layer-1 code with the
pending-async split (`SellerInit`/`SellerFinish`, `FirstBuyerInit`/`FirstBuyer`) as one thread per
role in program order, which is the only order the source's spawn structure allows.

Departures: the ghost `contribution` and `assert_sum` became the `Contrib` and `Final` messages to
the added `ledger`; `price`, `wallet` are fixed at 2 and 1; `n` is bounded by the run.

## 4. Where they differ, and why the verdict is what it is

**`correct`, CONFORMS.** In the implementation `b_i` receives `Rem(price − a_1 − … − a_{i−1})`
(the chain subtracts every contribution), chooses `a_i` from `{min(1, rem_i), 0}` exactly as the
oracle does, and `b_n` sends `Dec(a_n == rem_n)`, i.e. `Dec(sum == price)`. So each choice vector
gives the same `n + 1` visible messages on both sides, and the ledger's receive order is free on
both (bag; the ledger is the only visible thread, so there are no causal counts to compare). The
graph sets correspond one to one: (n + 1)! · 2^n each.

**`no-subtract`, DOES NOT REFINE**, smallest size `n = 2`. Schedule: b1 sends `Req`; the seller
sends `Quote(2)` to both buyers; b1 chooses `a_1 = min(1, 2) = 1`, sends `Contrib { 1, 1 }` and
(the fault) `Rem(2)` instead of `Rem(1)`; b2 receives `Rem(2)`, chooses `a_2 = min(1, 2) = 1`,
sends `Contrib { 2, 1 }` and `Dec(1 == 2) = Dec(false)`; the seller sends `Final(false)`. Uncovered
word (one of its 3! orders):

```
ledger: R(Contrib { i: 1, a: 1 }) R(Contrib { i: 2, a: 1 }) R(Final(false))
```

The specification, with contributions (1, 1), sends `Final(true)`. The other three choice vectors
((0,0), (0,1), (1,0)) give `Final(false)` on both sides with the same contributions, so exactly
3! = 6 implementation graphs are uncovered. Component: **signature value** (the `Final` payload).
In the source this is the run where `sellerFinish` would skip `assert_sum`, i.e. the protocol
under-reports a completed purchase; with `wallet = 1` the opposite error (a `true` with a wrong
sum) needs a larger wallet and is not exercised.

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `correct` | bag | the source's layer-1 protocol | CONFORMS | §4 |
| `no-subtract` | bag | b1 (and middle buyers) forward the remainder received, not `r − a` | DOES NOT REFINE (n ≥ 2) | contributions (1, 1) with `Final(false)` |

The source ships no fault (all `.expect` files are "0 errors"). The seeded fault is the
catalogue's fault (b): it breaks the one invariant the IS proof carries (`INV3`/`MiddleBuyer'`:
`RemCH[pid][r] > 0 ==> r == price - sum(contribution, 1, pid - 1)`), so it is the fault the
proof is about, and it is visible at the smallest size.

## 6. Files

- `source/`: `NBuyer.bpl`, `NBuyer.bpl.expect`, copied verbatim, provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_n_buyer_kragl` on TraceForge's shared main; ARGS `<n> [correct|no-subtract]`,
  model by `--model bag`. Visible `ledger`.
- `java/`: package `bench.n_buyer_kragl`, class `NBuyer` (args `<spec|impl>,<n>,<fifo|bag>[,<variant>]`),
  `Support` (model argument and thread joiner, copied from `H1Support`). Spawn order: spec
  `ledger, oracle`; impl `ledger, seller, b1 .. bn`; the same as the Rust crate. Compiled to `java/out/`.
- `out/runs.log`: every command and its output; `out/export/`: the five summary files of §7.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=2 | bag | 24 / 0 | 24 / 0 | yes |
| impl `correct` | n=2 | bag | 24 / 0 | 24 / 0 | yes |
| impl `no-subtract` | n=2 | bag | 24 / 0 | 24 / 0 | yes |
| spec | n=3 | bag | 192 / 0 | 192 / 0 | yes |
| impl `correct` | n=3 | bag | 192 / 0 | 192 / 0 | yes |
| impl `no-subtract` | n=3 | bag | 192 / 0 | 192 / 0 | yes |
| spec | n=4 | bag | 1920 / 0 | 1920 / 0 | yes |
| impl `correct` | n=4 | bag | 1920 / 0 | 1920 / 0 | yes |
| impl `no-subtract` | n=4 | bag | 1920 / 0 | 1920 / 0 | yes |
| spec | n=5 | bag | 23040 / 0 | 23040 / 0 | yes |
| impl `correct` | n=5 | bag | 23040 / 0 | 23040 / 0 | yes |
| impl `no-subtract` | n=5 | bag | 23040 / 0 | 23040 / 0 | yes |

All runs exhausted; no disagreement; the counts are the hand count `(n + 1)! · 2^n`. n = 5 is the
first size over a minute (JMC's impl runs), so the sweep stops there.

Conformance and the cross-language join, at n = 2, model bag:

| check | spec | impl | verdict |
|---|---|---|---|
| `conf` (TraceForge) | Rust spec | Rust `correct` | conforms (silence) |
| `conf` (TraceForge) | Rust spec | Rust `no-subtract` | 6 candidate violations (exit 1) |
| `conf-join` | Rust spec export | Java `correct` export | conforms, 0 uncovered |
| `conf-join` | Rust spec export | Java `no-subtract` export | violation, 6 uncovered (`no-slot`, the six orders of the word in §4) |
| `conf-join` | Java spec export | Rust `correct` export | conforms, 0 uncovered |

Every export has 24 distinct summaries (header `enumeration: complete`).

Commands (X = `/local/home/mkhoshechin/tmp/bench-target/release/bench_n_buyer_kragl`, built with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target cargo build --release -j 4` in `rust/`;
J = `java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher`,
Java compiled with `javac -d java/out -cp $(cat daavar/jmc/cp.txt) java/bench/n_buyer_kragl/*.java`),
for `n ∈ {2..5}`, `v ∈ {correct, no-subtract}`, all under `timeout 600`:

```
X count spec --model bag <n>
X count impl --model bag <n> <v>
J 'bench.n_buyer_kragl.NBuyer:spec,<n>,bag' must 0 1 <fresh report dir> - 600 </dev/null
J 'bench.n_buyer_kragl.NBuyer:impl,<n>,bag,<v>' must 0 1 <fresh report dir> - 600 </dev/null
X conf --model bag 2 <v>
X export spec --out out/export/rust-spec-n2.jsonl --model bag --program n-buyer-kragl --size n2 2
X export impl --out out/export/rust-impl-correct-n2.jsonl --model bag --program n-buyer-kragl --size n2 2 correct
J 'bench.n_buyer_kragl.NBuyer:<side>,2,bag[,<v>]' must 0 1 <fresh report dir> out/export/java-<side>[-<v>]-n2.jsonl 600 </dev/null
daavar/traceforge/target/release/conf-join <spec file> <impl file>
```
