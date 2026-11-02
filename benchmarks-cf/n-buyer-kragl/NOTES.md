# n-buyer-kragl: context-free Java twin

Class: `bench.n_buyer_kragl.NBuyerKragl` (file `java/bench/n_buyer_kragl/NBuyerKragl.java`),
implements `MpProgram`, public `(String...)` constructor.

Argument string: `<spec|impl>,<n>,<fifo|bag>[,<correct|no-subtract>]`, n >= 2
(Rust: `{spec|impl} ... --model {fifo|bag} -- <n> [variant]`). Variant defaults to `correct`;
it is validated on both sides and, as in Rust, ignored by spec. `fifo` -> `P2P`, `bag` -> `ASYN`.
`programName()` = `n-buyer-kragl`, `size()` = the `<n>` text as given.

## Threads (spawn order = Rust order; main invisible, spawns all, then awaits all in spawn order)

- spec: `ledger`, `oracle`. Roles declared: [ledger, oracle].
- impl: `ledger`, `seller`, `b1` .. `bn`. Roles declared: [ledger, seller, b1..bn].
- Visible: `ledger` only (Rust `visible: ["ledger"]`).
- ledger: n + 1 blocking receives of any message (`Channel.recv(model)`), as `recv_msg_block`.
- oracle: for i = 1..n, `a = choose(rem)`, rem -= a, sum += a, send `Contrib(i, a)` to ledger;
  then `Final(sum == PRICE)`.
- seller: blocking recv by type `Req`; send `Quote(PRICE)` to b1..bn in order; blocking recv
  `Dec`; send `Final(d)` to ledger.
- b_i: b1 first sends `Req` to seller; recv `Quote`; for i > 1 recv `Rem`; `a = choose(r)`;
  send `Contrib(i, a)` to ledger; i < n: send `Rem(subtract ? r - a : r)` to b_{i+1};
  i = n: send `Dec(a == r)` to seller.
- Rust `recv_tagged_msg_block(tag)` -> `Channel.recv(model, X.class)` (blocking, type-test guard).
  No non-blocking receives (none in Rust).
- Per-thread inboxes: Rust has one channel per thread; JMC has one mailbox per process, addressed
  by Tid. Same mapping.

## Messages (sealed interface `Msg extends MpMessage`)

| Rust | Java record | @Tag |
|---|---|---|
| `Req` | `Req()` | `Req` |
| `Quote(i32)` | `Quote(int price)` | `Quote` |
| `Rem(i32)` | `Rem(int rem)` | `Rem` |
| `Dec(bool)` | `Dec(boolean d)` | `Dec` |
| `Contrib { i, a }` | `Contrib(int i, int a)` | `Contrib` |
| `Final(bool)` | `Final(boolean d)` | `Final` |

## Nondeterminism

`choose(rem)` = `Nondet.chooseBoolean() ? min(WALLET, rem) : 0`, at exactly the Rust `nondet()`
sites (oracle once per i; each buyer once). Outcome set {true, false}; JMC's min(S) is `true`.
The choice is made even when rem = 0 (both branches give 0), as in Rust.

## Doubts / assumptions

1. Tids before spawn. The seller is spawned before the buyers but sends to them, and b_i sends
   to b_{i+1}; Rust creates all channels first. I compute every Tid in advance as
   `MpProcess.self().raw() + k` for the k-th spawn (task ids minted sequentially at
   construction, only main creates tasks) and check each handle's Tid against it (throws on a
   mismatch; none occurred). `self()` in main raises no event. Assumption: ids are sequential
   from main's id.
2. Order of the boolean options: Rust `nondet()`'s internal order is unknown to me; JMC puts
   `true` first. Same set of outcomes; only exploration order could differ.
3. `size()` = the bare `<n>` text; I do not know the exact size string the Rust side writes in
   a summary header.
4. Spec accepts and ignores a variant (Rust `parse` does the same).
5. Record component names (`price`, `rem`, `d`) are mine; only tag and field order are encoded.

## Counts measured (`must 0 1 report - 600`, n = 2)

| args | result |
|---|---|
| `spec,2,bag` | 24 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,2,bag` | 24 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,2,bag,no-subtract` | 24 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,2,fifo` | 4 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,2,fifo` | 24 distinct complete, 0 distinct blocked; stack exhausted |

All exit status 0. (Bag is the model the Rust header names for this program.)
