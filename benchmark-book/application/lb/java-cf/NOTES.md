# a4-balancer: context-free Java twin

Class: `bench.a4_balancer.A4Balancer` (`java/bench/a4_balancer/A4Balancer.java`), implements `MpProgram`,
public `(String...)` constructor.

## Argument format

`<spec|impl>,<impl>,<spec>,<k>,<q>,<m>,<fifo|bag>`

The Rust command line is `{spec|impl} --model {fifo|bag} -- <impl> <spec> <k> <q> <m>`. I put the side
first, then the Rust positional arguments in their Rust order (the two variant names come *before* the
sizes there), then the model last. Example: `spec,hash,a4a,1,1,1,fifo`. Both variant names are
parsed and validated on both sides; the spec side uses only `<spec>`, the impl side only `<impl>`
(as in Rust). impl → (policy, fault): hash=(Hash,None), rr=(RoundRobin,None),
pair-swap-hash=(Hash,PairSwap), pair-swap-rr=(RoundRobin,PairSwap), drop=(RoundRobin,Drop),
hash-drop=(Hash,Drop). Model: fifo → `P2P`, bag → `ASYN`.

## Mapping

Threads (spawn order = Rust order; main is invisible, spawns everything, then awaits every handle
in spawn order):
1. `dispatcher` (spec) / `balancer` (impl), role of that name, invisible.
2. `s0 .. s{m-1}`: servers, invisible. Body: `loop { A4Req(i,j) = recv (blocking, type guard A4Req);
   send to client i: A4Reply(x, j) }`.
3. `c0 .. c{k-1}`: clients, the visible roles. Body: `for j in 0..q { send A4Req(i,j) to the
   front; recv (blocking, type guard A4Reply) }`.

`rolesInSpawnOrder()` = [dispatcher|balancer, s0.., c0..]; `visibleRoles()` = [c0..c{k-1}].

Channels: every Rust channel has exactly one receiving thread and every thread receives on exactly
one channel (front: `rx_req`; server x: its request channel; client i: its reply channel), so each
channel maps to the receiver's own mailbox (`Channel.send(model, tid, msg)`, `Channel.recv(model,
Class)`). All receives are blocking (`recv_msg_block`), none non-blocking.

Messages: sealed interface `A4Msg` permits two records, `@Tag("A4Req") A4Req(int i, int j)` and
`@Tag("A4Reply") A4Reply(int x, int j)` (Rust tuple structs `A4Req(usize, usize)`,
`A4Reply(usize, usize)`, same field order). The forwarded request is the same value received.

Choices: dispatcher only. `A4b`: `Nondet.chooseIndex(m)` per request (Rust `(0..m).nondet()`).
`A4a`: `affinity: List<Optional<Integer>>` of size k; choose `Nondet.chooseIndex(m)` on a client's
first request only, reuse afterwards. Options in order 0..m-1. The balancer (impl) has no choice:
RoundRobin `p % m` with p counting forwarded requests, Hash `i % m`; Drop skips `(k-1, q-1)`;
PairSwap does q rounds of two receives and forwards the second first (no k=2 check, as in Rust).

## Doubts / assumptions

1. **Argument order** (above): the brief's generic form puts the variant last, but this program's Rust
   positional order has the variants first; I followed the Rust order. Change if the harness expects
   `<side>,<k>,<q>,<m>,<fifo|bag>,<impl>,<spec>` or similar.
2. **Model under `bag`**: the Rust `typed_chan` hard-codes `CommunicationModel::LocalOrder` on every
   channel, so the Rust program may stay FIFO whatever `--model` says. I made the Java model follow the
   argument (bag → ASYN). If the Rust side really ignores `--model`, the Java twin should use `P2P`
   always. Under `fifo` (the only model the Rust header comment says rows use) the two agree.
3. **LocalOrder = P2P**: assumed TraceForge's `LocalOrder` is per-sender FIFO, i.e. JMC `P2P`.
4. **Servers never terminate**: they loop on a blocking receive, as in Rust, as ordinary (non-daemon)
   processes, because the Rust program spawns them with `named` (not `spawn_daemon`). Main awaits
   them (the format requires it), so every execution ends blocked. Result: 0 complete graphs in
   every configuration I ran. If TraceForge counts such executions differently (e.g. treats them as
   complete because only receive-blocked threads remain), the counts will differ in the C/B split;
   using `spawnDaemon` for servers would be the alternative, but then they would have no role.
5. **Process identities**: Rust passes channel endpoints created before any spawn; Java needs Tids that
   only exist after spawning, and servers are spawned before the clients they reply to. Server Tids
   are stored by main right after each spawn (before any client exists, hence before the front can
   forward). Each client stores `MpProcess.self()` as its first statement, before its first send, so
   it is set before any server can reply to it. These are plain (uninstrumented) arrays allocated
   fresh in every `run()`; they make no events.
6. **Main does not join in Rust**; Java main awaits every handle in spawn order (required by the API
   and the format).
7. **Integer type**: `usize` mapped to `int`.
8. **size()** string: `"<impl> <spec> <k> <q> <m>"` (not compared by the format).
9. `chooseIndex(1)` raises no event (m = 1); I did not check whether TraceForge's `(0..1).nondet()`
   records a choice.

## Counts measured (must, counting mode, `must 0 1 report - 600`)

| args | complete | blocked | end |
|---|---|---|---|
| spec,hash,a4a,1,1,1,fifo (smallest) | 0 | 1 | stack exhausted |
| impl,hash,a4a,1,1,1,fifo (smallest) | 0 | 1 | stack exhausted |
| spec,hash,a4a,2,1,2,fifo | 0 | 8 | stack exhausted |
| spec,hash,a4b,2,1,2,fifo | 0 | 8 | stack exhausted |
| impl,hash,a4a,2,1,2,fifo | 0 | 2 | stack exhausted |
| impl,rr,a4a,2,1,2,fifo | 0 | 2 | stack exhausted |
| impl,pair-swap-hash,a4a,2,1,2,fifo | 0 | 2 | stack exhausted |
| impl,drop,a4a,2,1,2,fifo | 0 | 2 | stack exhausted |
| spec,hash,a4b,2,2,2,fifo | 0 | 96 | stack exhausted |
| impl,pair-swap-rr,a4b,2,2,2,fifo | 0 | 4 | stack exhausted |
