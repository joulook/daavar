# a2-store: context-free Java twin

Source of truth: `daavar/benchmarks/a2-store/rust/src/main.rs` (and `Cargo.toml`).
Class: `bench.a2_store.A2Store` (`java/bench/a2_store/A2Store.java`), implements `MpProgram`,
public `(String...)` constructor.

## Argument format

`<spec|impl>,<k>,<q>[,<s>],<fifo|bag>,<variant>`, with `s` present exactly for `sh` and `misroute`
(the Rust ARGS: `<variant> <k> <q> [<s>]`).

The variant, the numbers and the model may come in any order after `spec|impl`: the model is the
`fifo`/`bag` token, the variant is the one other non-numeric token, and the numbers are k, q[, s] in
that order. So the Rust order `<spec|impl>,<variant>,<k>,<q>[,<s>],<fifo|bag>` also works.
Variants: `pb`, `pb-br`, `early-ack`, `stale-get`, `silent-put`, `control-early-ack-primary`,
`control-early-ack-fwd`, `sh`, `misroute`. `spec` ignores the variant (Rust: `a2_spec(k, q)` for
every variant). For `spec` the variant is still required, and so is `s` for sh/misroute, as in Rust.

## Threads (spawn order, roles)

Main is invisible. It spawns every process (none spawns another), then awaits all of them in spawn
order. Visible roles: `c0 .. c{k-1}`.

| program | spawn order |
|---|---|
| spec (`a2_spec`) | `store`, `c0..c{k-1}` |
| pb, stale-get, silent-put, control-early-ack-primary (`a2_pb`) | `primary`, `backup`, `c0..` |
| pb-br, early-ack, control-early-ack-fwd (`a2_pb_br`) | `primary`, `backup`, `c0..` |
| sh, misroute (`a2_sh`) | `router`, `shard0..shard{s-1}`, `c0..` |

Thread bodies are line-by-line ports of the Rust closures: the same loop bounds (`k*q`, `n_puts`,
the pb-br primary's `n` computed inside the primary, `per_shard[x]`), the same order of send and
blocking receive in each branch (e.g. EarlyAck: send Ack, then receive the backup's ack; Correct:
the reverse), the same `first_put` / silent logic, and the same map semantics (`HashMap<Integer,Integer>`;
a missing key gives `None`). The client op is `a2_op`: put iff `i+j` even, key `(j/2 + i/2) mod 2`,
value `10i+j`.

## Channels

Java has no channel objects; a Rust channel becomes "send to the receiving thread's Tid" plus a
blocking receive with a type guard (`Channel.recv(M, Req.class)` etc.). This is sound here
because no thread receives the same message type from two channels:

| Rust channel | receiver | Java guard |
|---|---|---|
| `tx_req` (Req) | store / primary / router | `Req` |
| `reply_tx[c]` (Rep) | client `c` | `Rep` |
| pb `tx_b` (Internal) | backup | `Internal` |
| pb, pb-br `tx_ack` (Internal) | primary | `Internal` |
| pb-br `tx_bk` (Req) | backup | `Req` |
| sh `shard_tx[x]` (Req) | `shard{x}` | `Req` |

All receives are blocking (`recv_msg_block` everywhere in Rust); there are no non-blocking receives
and no nondeterministic choices in the Rust program, hence no `Nondet` in Java.

## Messages

| Rust | Java record | `@Tag` | fields |
|---|---|---|---|
| `Req::Put { c, k, v }` | `Put(int c, int k, int v)` | `Put` | c, k, v |
| `Req::Get { c, k }` | `Get(int c, int k)` | `Get` | c, k |
| `Rep::Ack` | `Ack()` | `Ack` | – |
| `Rep::Val(i32)` | `Val(int v)` | `Val` | v |
| `Rep::None` | `RepNone()` | `None` | – |
| `Internal::Fwd(Req)` | `Fwd(Req r)` | `Fwd` | the nested Req record |
| `Internal::AckPut` | `AckPut()` | `AckPut` | – |

Sealed interfaces `Req`, `Rep`, `Internal`, each extending `MpMessage`.

## Measured counts (`must 0 1 report - 600`)

Smallest size, k=1 q=1 (s=1 for sh/misroute):

| args | result |
|---|---|
| `spec,1,1,fifo,pb` | 1 distinct complete, 0 distinct blocked; stack exhausted |
| `spec,1,1,1,fifo,sh` | 1 complete, 0 blocked; exhausted |
| `impl,1,1,fifo,{pb,pb-br,early-ack,stale-get,control-early-ack-primary,control-early-ack-fwd}` | 1 complete, 0 blocked; exhausted (each) |
| `impl,1,1,fifo,silent-put` | 0 complete, 1 blocked; exhausted |
| `impl,1,1,1,fifo,{sh,misroute}` | 1 complete, 0 blocked; exhausted (each) |

Extra sanity run, k=2 q=2 (not tuned to anything):

| args | result |
|---|---|
| `spec,2,2,fifo,pb` | 6 complete, 0 blocked |
| `impl,2,2,fifo,{pb,pb-br,stale-get,control-early-ack-primary,control-early-ack-fwd}` | 6 complete, 0 blocked (each) |
| `impl,2,2,fifo,early-ack` | 10 complete, 0 blocked |
| `impl,2,2,fifo,silent-put` | 0 complete, 3 blocked |
| `impl,2,2,{1,2},fifo,sh`, `impl,2,2,2,fifo,misroute` | 6 complete, 0 blocked (each) |
| `impl,pb,2,2,bag` (Rust token order) | 6 complete, 0 blocked |

All stack exhausted.

## Doubts and assumptions

1. **Model.** Every Rust channel is `typed_chan`, which hard-codes `CommunicationModel::LocalOrder`
   whatever `--model` is. I assumed LocalOrder = JMC `P2P`, and the Java program uses `P2P` for every
   send and receive and returns `P2P` from `model()` under both `fifo` and `bag`. `bag` is accepted
   and ignored, as I read the Rust program. If the harness expects `bag` to mean `ASYN` (or expects
   `model()` to echo the argument for the header), change the constant `M`.
2. **Argument order.** The brief's template puts the variant last; the Rust ARGS put it first. Both
   orders are accepted (see above). The `s` argument is required exactly for sh/misroute, as in Rust.
3. **`size()`.** I do not know the Rust side's header text for the size. I used the Rust ARGS
   verbatim joined by spaces: `<variant> <k> <q>[ <s>]`.
4. **Learning Tids.** Rust creates all channels before spawning; Java addresses threads by Tid,
   which exists only after a spawn, and the servers are spawned before the clients. Server Tids are
   stored by main (in a per-run holder) before any client is spawned. Each client stores its own
   Tid (`MpProcess.self()`) as its first action, before its first send; a server reads client `c`'s
   Tid only after receiving a request carrying `c`, which is causally after that write. Main does not
   store client Tids after `spawn` returns, because a client could send and be answered before main
   resumes. This adds one `self()` call per client (not a message event).
5. **Guards by type.** The channel-to-type mapping above relies on no thread receiving one type from
   two Rust channels. I checked every thread; it holds for all nine variants.
6. **pb backup.** `if let Internal::Fwd(Req::Put{..})` — a non-Put Fwd never occurs (the primary only
   forwards puts), but the backup still acks it, as in Rust.
7. **`silent-put` blocks** at every size I ran: the first put's client waits forever. That matches
   the Rust code (the reply is skipped; the client's `recv_msg_block` has nothing to read).
8. Rust `usize` fields (`c`, `k`) are Java `int`; `i32` values are `int`.
