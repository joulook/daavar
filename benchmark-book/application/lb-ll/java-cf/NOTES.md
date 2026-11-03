# balancer-load-aware: context-free Java twin

Source of truth: `daavar/benchmarks/balancer-load-aware/rust/src/main.rs`.
Java: `java/bench/balancer_load_aware/BalancerLoadAware.java`.

## Class and arguments

- CLASS = `bench.balancer_load_aware.BalancerLoadAware` (implements `MpProgram`, public `(String...)`
  constructor).
- Argument string (brief order, variant last):
  `<spec|impl>,<k>,<q>,<m>[,<c>],<fifo|bag>,<variant>`, variant `l1 | l2 | l2-stale`, `c` given
  exactly for `l2` and `l2-stale` (as in the Rust `ARGS: <variant> <k> <q> <m> [<c>]`).
  The Rust order `<spec|impl>,<variant>,<k>,<q>,<m>[,<c>],<fifo|bag>` is accepted as well (the
  variant is detected by name in second position).
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN`; every send and receive uses that one model.
- Checks as in Rust: each number in 1..=100, exact count of sizes per variant;
  `b` (balancers / dispatchers) = 2 for `l2-stale`, else 1; `cap` absent (-1) for `l1`.
- `programName()` = `balancer-load-aware`; `size()` = the Rust argument list joined by spaces,
  e.g. `l2 1 1 1 1`.

## Threads (spawn order as in Rust: fronts, servers, clients; main awaits in the same order)

| Rust thread | Java role | body |
|---|---|---|
| `dispatcher{d}` (spec) | `dispatcher{d}` | `requests_of(d)` times: blocking recv (any), `Nondet.chooseIndex(m)` if m>1 else 0, forward the received message to that server; then `Stop` to every server in order |
| `s{x}` (spec) | `s{x}` | blocking recv (any) until `b` Stops; `Req{c,j}` -> `Reply{x,j}` to client c |
| `balancer{d}` (impl) | `balancer{d}` | per request: blocking recv guarded `Req` (the requests channel); loop: `Poll{d,again}` to every server, `m` blocking recvs guarded `Load` (the loads channel), keep servers with space (all for l1, `n<c` otherwise), none -> `again=true`, re-poll; else least load, ties by `Nondet.chooseIndex(|best|)` only when >1; send Req; at the end `Stop` to every server |
| `s{x}` (impl) | `s{x}` | as Rust: Stops==b -> drain (Reply each), end; queue empty -> blocking recv, else non-blocking `tryRecv` whose ⊥ serves the oldest; `Poll` -> (again && full: serve once) then `Load{x,|queue|}` to balancer p.b; `Req` -> Reject if full else enqueue; `Stop` -> count |
| `c{i}` | `c{i}` (visible) | `q` times: send `Req{i,j}` to front `i mod b`, blocking recv (any) |

Visible roles: `c0..c{k-1}`. All roles: `dispatcher*|balancer*`, `s*`, `c*`. Main does no
message passing; it spawns all, then awaits all.

## Messages (sealed `Msg extends MpMessage`, one record per Rust variant, `@Tag` = variant name)

`Req(int c, int j)`, `Poll(int b, boolean again)`, `Load(int s, int n)`, `Reply(int s, int j)`,
`Reject(int s, int j)`, `Stop()`. Rust `u8` -> `int`.

## Channels -> mailboxes

- Rust: one channel per client, per server, per dispatcher; two per balancer (requests, loads).
- Java: each process's mailbox. The balancer's two channels become its one mailbox read with the
  type guards `Req` (requests) and `Load` (loads); the two channels carry disjoint types and
  different senders (clients vs servers), so under p2p no FIFO order is lost or added. All other
  receives are unguarded, like the Rust `recv_msg_block()` / `recv_msg()` on a single channel.

## Doubts and assumptions

1. Argument order: the brief template puts the variant last; the Rust ARGS put it first. I made
   variant-last the documented form and accept both. The harness's exact string is unknown to me.
2. Process addressing: Rust channels exist before any thread; a Java mailbox address (Tid) exists
   only after the spawn, and dispatchers/balancers are spawned before the servers they send to.
   I use a directory (`AtomicReferenceArray<Tid>`) filled by main right after each spawn. Every
   read of an entry happens after a receive that causally follows the named process's spawn
   (fronts read server Tids only after receiving a client request; clients are spawned last).
   No extra message events. Not using `Tid.of(k+1)` arithmetic.
3. Unreachable Rust `panic!`s (unexpected message) are `IllegalStateException`s; they never fire.
4. `programName()` and the `size()` text are my guesses at what the Rust side writes in the header.
5. The Rust comment says the twin is meaningful under fifo only; `bag` is supported mechanically.
6. A client ignores the content of the reply/reject, as in Rust (`let _ =`).

## Counts measured (must, iterations 0, seed 1, fifo, smallest sizes k=q=m=c=1)

| args | graphs |
|---|---|
| `spec,1,1,1,fifo,l1` | 1 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,1,1,fifo,l1` | 2 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,1,1,1,1,fifo,l2` | 1 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,1,1,1,fifo,l2` | 2 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,1,1,1,1,fifo,l2-stale` | 3 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,1,1,1,1,fifo,l2-stale` | 10 distinct complete, 0 distinct blocked; stack exhausted |

An export run (`impl,l2-stale,1,1,1,1,fifo`, Rust argument order) was accepted by the recorder
(roles check passed) and produced 1 distinct summary: c0 sends `{"t":"Req","f":[0,0]}`, receives
`{"t":"Reply","f":[0,0]}`.

Extra smoke runs (not required; the re-poll path): `impl,2,1,1,1,fifo,l2` -> 12 distinct complete,
0 blocked, exhausted; `impl,2,1,2,1,fifo,l2-stale` -> timeout at 120 s with 7665 complete, 0 blocked,
no error.
