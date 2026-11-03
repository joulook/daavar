# bank-grove: context-free Java twin (2026-11-02)

Written from `daavar/benchmarks/bank-grove/rust/src/main.rs` + `Cargo.toml`, the JMC Must API sources
listed in plan/twins-contextfree/BRIEF.md and SUMMARY-FORMAT.md ("Values", "What both programs must
satisfy"). No other benchmark file was read.

- Class: `bench.bank_grove.BankGrove` (file `java/bench/bank_grove/BankGrove.java`), implements `MpProgram`,
  public `(String...)` constructor.
- Arguments: `<spec|impl>,<K>,<fifo|bag>[,<ordered|unordered>]`; K = transfers per client; variant default
  `ordered`, accepted but unused on the spec side (as in Rust, where `spec` ignores it). `fifo` -> `P2P`,
  `bag` -> `ASYN`.

## Threads (spawn order as in Rust)

| Rust | Java role | notes |
|---|---|---|
| spec: `bank` | `bank` (invisible) | 2K+1 blocking receives of Transfer-or-Audit; balances `[1,0]`; Transfer applied only if `bal[src] >= amt`, replies `Done`; Audit replies `Total(sum)` |
| impl: `lock` | `lock` (invisible) | exactly `4*(2K)+4` blocking receives of CondPut-or-Unlock; per-key `held` + waiting list; on Unlock: `PutOk` to the unlocker, then free the key or hand `CondOk` to a waiter chosen with `Nondet.chooseIndex(w)` only when `w > 1` (index 0 when `w == 1`, as Rust) |
| impl: `kv` | `kv` (invisible) | infinite blocking loop over Get-or-Put; store `[1,0]` |
| impl: `clerk0`, `clerk1` | same (invisible) | K times: recv Transfer; lock order `dst,src` iff `ordered && dst < src`, else `src,dst`; get/put as in Rust; unlock src then dst; `Done` to the client |
| impl: `aclerk` | `aclerk` (invisible) | recv Audit; lock+get account 0, lock+get account 1; unlock 0, 1; `Total(sum)` |
| `c0`, `c1` | `c0`, `c1` (visible) | K times: send `Transfer(me, i, 1-i, 1)` to bank / clerk i, blocking recv `Done` |
| `auditor` | `auditor` (visible) | send `Audit(me)` to bank / aclerk, blocking recv `Total` |

Main is invisible, spawns everything (spec: bank, c0, c1, auditor; impl: lock, kv, clerk0, clerk1, aclerk,
c0, c1, auditor), then awaits in Rust's join order (clients, then bank / clerks, lock, kv). Visible roles
`c0, c1, auditor`; `rolesInSpawnOrder` returns the spawned roles of the chosen side.

## Messages

One `@Tag` record per Rust `Msg` variant in a sealed interface `Msg`, same names and field order:
`Transfer(Tid from, int src, int dst, int amt)`, `Done()`, `Audit(Tid from)`, `Total(int value)`,
`CondPut(Tid from, int key)`, `CondOk()`, `Unlock(Tid from, int key)`, `Get(Tid from, int key)`,
`Val(int value)`, `Put(Tid from, int key, int val)`, `PutOk()`. `u8` -> `int`; `ThreadId` -> `Tid`.
Rust's numeric-tag guards become type tests (`recv(m, X.class)` or `x instanceof A || x instanceof B`
for the two-tag receives). Rust's TAG_PUTOK is shared by the lock's Unlock reply and the kv's Put reply;
both are the same `PutOk` record here, and the waiting receive accepts either, as in Rust.
All receives are blocking (Rust uses only `recv_tagged_msg_block`); no non-blocking receives.

## Assumptions / doubts

1. `kv` never terminates and is not a daemon (Rust uses `named`, not `spawn_daemon`, and joins it), so
   main's await of `kv` blocks and **every impl execution ends blocked** (0 complete graphs). I kept that
   faithfully; the Rust comment ("it ends blocked once the clerks are done", "A4 idiom") suggests it is
   intended, but I could not confirm how the harness compares blocked impl graphs against a complete spec.
2. `programName()` = `"bank-grove"` and `size()` = `"<K>"` are guesses; the Rust program delegates the header
   to `traceforge::program`, which I was not to read.
3. The Rust `ARGS:` comment the brief mentions is not present; I used the `Usage:` comment and `parse`
   (K, then optional variant). Rust defaults K to 1 when absent; here K is required by the argument format.
4. Unknown variant: Rust panics inside `imp` at run time; here the constructor throws (refused argument).
5. `Nondet.chooseIndex(w)` order = the waiting list's insertion order, matching Rust's `(0..w).nondet()`
   then `remove(j)`. With 2 clerks + aclerk at most 2 waiters per key, so the choice can occur.
6. u8 arithmetic: values stay in {0,1} (BAL_TOTAL = 1, AMOUNT = 1), so no overflow/underflow difference.
7. The launcher calls `run()`; I do not wrap it in `MpProcess.runMain` (no daemons, so no extra event).

## Measured (MpLauncher, `must 0 1 report - 600`, K = 1)

| args | result |
|---|---|
| `spec,1,fifo` | 6 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,fifo` (ordered) | 0 distinct complete, 366 distinct blocked; stack exhausted |
| `impl,1,fifo,unordered` | 0 distinct complete, 258 distinct blocked; stack exhausted |
| `spec,1,bag` | 6 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,bag` (ordered) | 0 distinct complete, 366 distinct blocked; stack exhausted |
| `impl,1,bag,unordered` | 0 distinct complete, 258 distinct blocked; stack exhausted |

Not compared with any Rust count (per the brief).
