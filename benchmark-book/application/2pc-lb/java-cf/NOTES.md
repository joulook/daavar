# two-pc-lbinria: context-free Java twin

Class: `bench.two_pc_lbinria.TwoPcLbinria` (implements `MpProgram`, public `(String...)` constructor).
Source: `java/bench/two_pc_lbinria/TwoPcLbinria.java`. Written from `rust/src/main.rs` + `Cargo.toml` and the
JMC API sources listed in the brief only.

## Arguments

`<spec|impl>,<N>,<fifo|bag>[,<correct|counting-bug>]`, e.g. `impl,2,bag,counting-bug`.
- `N` = resource managers (required here; Rust defaults it to 2 when absent). `R = 2` fixed, as in Rust.
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- Variant default `correct`. The spec side ignores the variant (as Rust's `spec(n)` does); the impl side
  refuses an unknown variant at construction (Rust panics in `imp` at run time).
- `programName()` = `two-pc-lbinria`, `size()` = `N` as text.

## Messages

| Rust | Java | Tag | Fields |
|---|---|---|---|
| `struct Prepared(usize)` | `record Prepared(long from)` | `Prepared` | `[from]` |
| `Decision::Commit` | `record Commit()` | `Commit` | `[]` |
| `Decision::Abort` | `record Abort()` | `Abort` | `[]` |

`Msg` is a sealed interface over `Prepared` and the sealed interface `Decision` (`Commit`, `Abort`).
Every receive is a typed (type-test) blocking receive: `recv(model, Prepared.class)` or
`recv(model, Decision.class)`. There are no non-blocking receives (none in Rust).

## Threads (spawn order and awaits as in Rust)

- spec: `tm`, then `rm0..rm{N-1}`; main awaits `rm0..`, then `tm`.
- impl: `tm`, then `p0..p{N-1}`, then `rm0..rm{N-1}`; main awaits `rm*`, then `p*`, then `tm`.
- Visible roles: `rm0..rm{N-1}`. `tm` and `p*` are spawned with their Rust thread names as roles
  (non-daemon, awaited, invisible); `rolesInSpawnOrder()` lists every spawned name, `visibleRoles()` the RMs.
- `rm(i, server)`: `Nondet.chooseBoolean()` true -> return (crash); else send `Prepared(i)` to `server`
  (spec: `tm`; impl: `p_i`), then blocking receive of a `Decision`.
- spec `tm`: N times { `chooseBoolean` -> send `Abort` to rm0..rm{N-1}, stop; else blocking receive
  `Prepared` }; then `chooseBoolean ? Commit : Abort` sent to rm0..rm{N-1}.
- impl `tm`: up to N*R times { `chooseBoolean` -> `Abort` to all, stop; receive `Prepared(from)`; add
  `from` to a list if counting-bug or not already present; if size >= N -> `Commit` to all, stop }.
- `p_i`: blocking receive of `Prepared`; `k = Nondet.choose([1, 2])`; send it `k` times to `tm`.

Nondet outcome sets are the same as Rust's (`nondet()` = {true,false}, `(1..=R).nondet()` = {1,2});
Java's option order is `true, false` and `1, 2` (order only affects Must's `min(S)`).

## Places I was unsure / assumptions

1. **RM inboxes (main assumption).** Rust gives each RM a named channel `("rm", i)` so that `tm`,
   spawned before the RMs, can address them. JMC has no named channels: every receive is on the
   process's own mailbox. I map the channel to the RM's own mailbox (it only ever carries
   `Decision`) and give `tm` the RM `Tid`s by **predicting** them from the spawn order:
   the process spawned at 0-based position `s` is task `self(main) + 1 + s` (main's own id read with
   `MpProcess.self()`, which raises no event). The MpProgram javadoc says "the k-th process main
   spawns is task k + 1"; my first reading of it (0-based k) was off by one, so I verified the
   formula empirically: a temporary check in each RM (`self()` equals the predicted Tid) ran through
   the full enumerations of spec/impl N=2 bag/fifo and impl counting-bug without a mismatch, then
   was removed. If JMC's task numbering changes, this breaks (a send to a wrong task).
2. Channel-per-RM vs mailbox: Rust's RM inbox and thread mailbox are distinct, but an RM never
   receives on its thread mailbox and nobody sends to it, so merging them changes nothing observable.
3. `Prepared` field as `long` (Rust `usize`); encodes as a JSON integer either way.
4. `programName`/`size` strings are my guess of what the harness expects (`two-pc-lbinria`, `"N"`).
5. Roles for invisible `tm`/`p*`: I named them (as Rust names its threads) and listed them in
   `rolesInSpawnOrder()`. An export run (impl,2,bag) was accepted with `visible:["rm0","rm1"]`.
6. A crashed RM's proxy ends blocked on its receive and main's await of it blocks the execution,
   as in Rust (Rust joins proxies too). Likewise a spec `tm` waiting for a crashed RM's `Prepared`.

## Counts measured (`must 0 1 report - 600`, plain enumeration)

| args | complete | blocked | stack |
|---|---|---|---|
| spec,1,bag | 4 | 1 | exhausted |
| impl,1,bag | 5 | 2 | exhausted |
| impl,1,bag,counting-bug | 5 | 2 | exhausted |
| spec,1,fifo | 4 | 1 | exhausted |
| impl,1,fifo | 4 | 2 | exhausted |
| spec,2,bag | 12 | 3 | exhausted |
| impl,2,bag | 54 | 22 | exhausted |
| impl,2,bag,counting-bug | 42 | 18 | exhausted |

Not compared with any Rust count (none looked up).
