# bank(K) — two-key bank transfers (`bank-grove`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** fifo (`p2p`: per sender–receiver order, the source's reliable RPCs), the default the Rust
program declares, the Java twin's default, and the model of every campaign row. **Languages:** Rust (TraceForge's Must
API), in `rust/`; Java twin in `java/` (`bench.bank_grove.Bank`, args `<spec|impl>,<K>,<fifo|bag>,<variant>`);
context-free Java port in `java-cf/` (`bench.bank_grove.BankGrove`, args
`<spec|impl>,<K>,<fifo|bag>[,<ordered|unordered>]`). **Visible threads:** `c0`, `c1` (the transfer clients) and
`auditor`; the specification's `bank` and the implementation's clerks `clerk0`, `clerk1`, `aclerk`, lock service
`lock` and account store `kv` are invisible. **Parameters:** `K ≥ 1` = the number of transfers per client (default
1); variant `ordered` or `unordered`; fixed: 2 accounts, total balance 1, all of it in account 0 initially, every
transfer of amount 1, one audit. ARGS: `-- <K> [ordered|unordered]` (`K` defaults to 1 and the variant to `ordered`;
any other variant name is refused by `imp`; `spec` ignores the variant).
**Source:** Grove (Sharma et al., SOSP 2023) / gokv, `related-work-papers/sources/gokv/bank/`, `lockservice/`, `kv/`
(the files `bank/bank.go`, `lockservice/lock_clerk.go`, `kv/interface.go`).

Notation: as in `../../synthetic/naive/README.md`. A receive written with a pattern (`Done := recv^fifo()`) takes a
message of that type (tagged receives in Rust, type guards in Java). Messages: `Transfer(c, src, dst, amt)`, `Done`
(client ↔ bank or clerk), `Audit(a)`, `Total(s)` (auditor ↔ bank or `aclerk`); implementation only:
`CondPut(cl, key)` / `CondOk` (the source's `Lock`: `ConditionalPut(key, "", "1")` retried until `"ok"`),
`Unlock(cl, key)` / `PutOk` (`Unlock`: `Put(key, "")`), `Get(cl, key)` / `Val(v)`, `Put(cl, key, val)` / `PutOk`. The
clients are the same on both sides except for the front end they talk to (`F0`, `F1`, `FA` below), which the
comparison does not see:

```
Clients(K, F0, F1, FA) ≜ c0: for j := 0..K−1:  send^fifo(F0, Transfer(c0, 0, 1, 1));  Done := recv^fifo()
                       ‖ c1: for j := 0..K−1:  send^fifo(F1, Transfer(c1, 1, 0, 1));  Done := recv^fifo()
                       ‖ auditor: send^fifo(FA, Audit(auditor));  Total(s) := recv^fifo()
```

`c0` moves 1 from account 0 to account 1, `K` times, one transfer at a time; `c1` the reverse; `auditor` asks once
for the total. `Done` carries nothing, so the visible values are `s` and the three statuses.

The specification (both pairs), one bank:

```
Spec(K) ≜ Clients(K, bank, bank, bank)
        ‖ bank: bal := [1, 0]
                for 2K + 1 times:  m := recv^fifo()
                   if m = Transfer(c, src, dst, amt):  if bal[src] ≥ amt then bal[src] := bal[src] − amt;  bal[dst] := bal[dst] + amt
                                                       send^fifo(c, Done)
                   if m = Audit(a):                    send^fifo(a, Total(bal[0] + bal[1]))
```

`bank` serves the `2K + 1` operations one at a time in its receive order; a transfer moves the amount only if the
source account holds it, so the total stays 1 and the audit always answers `Total(1)`. `(2K+1)!/(K!·K!)` graphs (6,
30, 140, 630 at `K = 1..4`), every one ending with the three clients done.

The implementation, the source's `BankClerk` (`transfer_internal`, `get_total`) over its lock clerk and KV clerk:

```
Impl_v(K) ≜ Clients(K, clerk0, clerk1, aclerk) ‖ clerk0 ‖ clerk1 ‖ aclerk ‖ lock ‖ kv

clerk_i ≜ for K times:
             Transfer(c, src, dst, amt) := recv^fifo()
             ⟨acquire⟩
             old := Get(src)
             if old ≥ amt:  Put(src, old − amt);  t := Get(dst);  Put(dst, t + amt)
             Unlock(src);  Unlock(dst)                                                    (release_two)
             send^fifo(c, Done)

aclerk  ≜ Audit(a) := recv^fifo()
          sum := 0
          for key := 0..1:  Lock(key);  sum := sum + Get(key)                              (get_total, sorted order)
          for key := 0..1:  Unlock(key)
          send^fifo(a, Total(sum))

Lock(key)      ≜ send^fifo(lock, CondPut(me, key));  CondOk := recv^fifo()
Unlock(key)    ≜ send^fifo(lock, Unlock(me, key));   PutOk := recv^fifo()
Get(key)       ≜ send^fifo(kv, Get(me, key));        Val(v) := recv^fifo();  return v
Put(key, val)  ≜ send^fifo(kv, Put(me, key, val));   PutOk := recv^fifo()

lock ≜ held := [false, false];  waiting := [[], []]
       for 8K + 4 times:  m := recv^fifo()
          if m = CondPut(cl, key):  if ¬held[key] then held[key] := true;  send^fifo(cl, CondOk)
                                    else waiting[key] := waiting[key] · cl
          if m = Unlock(cl, key):   send^fifo(cl, PutOk)
                                    if waiting[key] = [] then held[key] := false
                                    else next := nondet(waiting[key]);  remove next from waiting[key];  send^fifo(next, CondOk)

kv   ≜ store := [1, 0]
       while true:  m := recv^fifo()
          if m = Get(cl, key):       send^fifo(cl, Val(store[key]))
          if m = Put(cl, key, val):  store[key] := val;  send^fifo(cl, PutOk)
```

A clerk takes both accounts' locks, reads the source account, moves the amount if it is there, releases both locks
and answers its client; `aclerk` locks and reads the accounts in sorted order. `lock` answers a `CondPut` at once if
the key is free and otherwise queues the requester, who is granted the key at some `Unlock` (any queued clerk may win
the next `ConditionalPut`); it serves the `8K + 4` lock requests (four per transfer, four for the audit). `kv` is a
server loop that never ends, so every implementation execution is counted blocked by plain Must; its status is
invisible and the comparison sees only the clients' statuses.

## The pairs

### ordered — expected: **conforms**

```
⟨acquire⟩ ≜ Lock(min(src, dst));  Lock(max(src, dst))        (the source's acquire_two_good)
```

Every transfer (both move between accounts 0 and 1) and the audit take lock 0 first and hold it until their last KV
operation, so `lock` holds lock 0 for one operation at a time and the KV operations of different operations never
interleave; no cycle of lock waits exists (all take 0, then 1), so every lock request is eventually granted and no
visible thread blocks. Take the specification graph whose serving order is the order in which `lock` grants lock 0:
each grant lies after the operation's request and before its reply, and `o_i`'s `Unlock(0)` precedes the grant to
`o_j` in `lock`'s order, so `inv(o_i) → resp(o_j)` for `i ≤ j` holds in the implementation; the balances in `kv`
evolve as in that serial execution, and `aclerk`, holding both locks while it reads both accounts, sums to 1. The
values (`Total(1)`) and the statuses (all done) agree, and the specification graph has no order the implementation
lacks.

### unordered — expected: **violation** (`K ≥ 1`)

```
⟨acquire⟩ ≜ Lock(src);  Lock(dst)        ◀ changed: the source's acquire_two, what transfer_internal calls
```

`c1`'s transfers now take lock 1 first. Schedule at the first transfer of each client: `lock` grants `clerk0`'s
`CondPut(0)` and `clerk1`'s `CondPut(1)`; `clerk0`'s `CondPut(1)` and `clerk1`'s `CondPut(0)` are queued behind each
other's lock, neither clerk ever unlocks, and `aclerk`'s `CondPut(0)` is queued too. The uncovered behaviour is

```
c0:      S(Transfer(c0, 0, 1, 1))     with c0 blocked
c1:      S(Transfer(c1, 1, 0, 1))     with c1 blocked
auditor: S(Audit(auditor))            with auditor blocked
```

In every specification graph the three clients end done; the difference is in the statuses. A second deadlock has
the same shape: `aclerk` holds lock 0 and waits for lock 1 while `clerk1` holds 1 and waits for 0.

## What it is for

A lock-ordering bug of the source (its `transfer_internal` calls the unordered `acquire_two`; the ordered
`acquire_two_good` is unused) against a one-thread bank. The violation is a status (three clients that never
complete) and every implementation execution ends blocked in the invisible `kv` loop, so the pair exercises the
comparison of visible statuses when invisible threads block; the conforming pair's cover rests on the lock order
serialising the KV operations.
