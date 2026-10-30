# IronLock: IronFleet's token-ring lock against the history of holders

**Family:** related-work  **Source:** IronFleet (SOSP 2015), `ironclad/ironfleet/src/Dafny/Distributed/{Services,Protocol,Impl}/Lock/`
under `related-work-papers/sources/` (CANDIDATES.md N13)  **Expected verdict:** against the
primary specification `spec` (IronFleet's `AbstractService.s.dfy`: any holder, any order, one
holder per epoch): `correct` **CONFORMS**; `double-hold` **DOES NOT REFINE** at n ≥ 2 (two nodes
emit `Locked(3)`, two holders of epoch 3); `grant-without-token` **CONFORMS** at n = 2 (masked)
and **DOES NOT REFINE** at n ≥ 3 (two nodes emit `Locked(2)`, two holders of epoch 2). Against
the stricter `spec-ring`: `correct` **CONFORMS**; `double-hold` **DOES NOT REFINE** at n ≥ 2;
`grant-without-token` **CONFORMS** at n = 2 (masked) and **DOES NOT REFINE** at n ≥ 3 (node 2
emits `Locked(1)`, and two nodes emit `Locked(2)`). Table in §5.

## 1. The protocol

Here n nodes in a ring pass one lock token (`Protocol/Lock/Node.i.dfy`):

- `NodeInit`: node 0 holds the token with epoch 1. Every other node starts with `held = false`
  and `epoch = 0`.
- `NodeGrant`: the holder sends `Transfer(epoch + 1)` to node `(i + 1) mod n` and releases the
  token.
- `NodeAccept`: a non-holder that receives `Transfer(e)` with `e > epoch` takes the token,
  sets `epoch = e`, and emits `Locked(e)`.

Messages (`Types.i.dfy`): `Transfer(transfer_epoch)` and `Locked(locked_epoch)`. The specification
(`Services/Lock/AbstractService.s.dfy`) is a `history` of holders. `Service_Init` sets
`history == [e]` for any host e. `Service_Next` appends any host. `Service_Correspondence` says
that a `Locked(e)` packet from host `src` requires `1 ≤ e ≤ |history|` and `src == history[e-1]`.
On packets this means: each epoch has at most one source (one holder at a time), and the sources
of the epochs may be any hosts in any order.

Roles:

- Visible: `obs0 .. obs{n-1}`. `obs_i` stands for the wire out of node i: it stands for the
  `Locked` packet with source node i as it goes to its host destination (in IronFleet, the
  sender of the accepted transfer, `Impl/Lock/Node.i.dfy` `NodeAcceptImpl`). It receives the
  `Locked(e)` packets node i emits, which are the packets `Service_Correspondence` constrains,
  and then one `End` that marks that node i has stopped. It performs at most K + 2 blocking
  receives and stops at the first `End`. The nodes cannot be visible (rule 3), because they
  exchange `Transfer`s that the specification has no counterpart for.
- Invisible: `node0 .. node{n-1}` on the implementation side, `oracle` on both specification
  sides.

In IronFleet the `Locked` packet goes to the transfer's sender and nobody reads it. Here it goes
to `obs_i`, which makes the constrained packet observable. `End` is ours. It lets an observer
stop without knowing in advance how many packets its node emits, which under the any-holder
specification is not fixed. `Stop` (node to node, §3) is ours too; it ends the bounded run and
is invisible. These are the three changes to the message flow.

Parameters: n nodes (CLI size) and K = 2 transfers (constant `K`). The transfers carry epochs 2
and 3. Epoch 1 is node 0's initial holding, which emits no packet (the source's `NodeInit` has no
I/O). No variant emits an epoch above K + 1 = 3. Smallest interesting size: n = 2 for `correct`
and `double-hold`, n = 3 for `grant-without-token` (§4).

## 2. The specification programs

Both specification programs have one invisible `oracle` and the observers of §1. The oracle sends
every `Locked` packet and then `End` to every observer, in that order.

**`spec` (primary; IronFleet's `AbstractService.s.dfy`).** For every epoch e = 1..K+1 the oracle
picks `c = (0..n+1).nondet()` (Java `Nondet.chooseIndex(n+1)`). If `c == n`, no `Locked(e)`
packet exists. Otherwise `history[e-1] = node c`, and the oracle sends `Locked(e)` to `obs_c`.
So the specification admits every assignment of epochs 1..3 to at most one holder each, in any
order, including epoch 1 (`Service_Init` lets any host be the first holder) and including epochs
with no packet (`Service_Correspondence` only constrains the packets that exist). There are
(n+1)^(K+1) oracle choices. Under `fifo` each observer reads its packets in increasing epoch
order and then `End`. Under `bag` it reads them in any order and stops at `End`, leaving the rest
unread. This gives the counts in §7, which we also checked by hand: 27 / 64 / 125 under `fifo`
and 159 / 412 / 845 under `bag`, for n = 2 / 3 / 4.

Two properties of IronFleet's predicate are not produced by the oracle:

- epochs above K + 1, which no implementation variant emits;
- a duplicate of an identical packet (same source, same epoch), which `Service_Correspondence`
  admits because it ranges over a set of packets. No variant emits one either.

**`spec-ring` (stricter than IronFleet's).** The oracle fixes the history in ring order,
`history[e-1] = (e-1) mod n`, and for e = 2..K+1 sends `Locked(e)` to `obs_{(e-1) mod n}`. This is
the only history the correct ring produces, so it is the smallest specification against which
`correct` conforms. It is not IronFleet's specification: it rejects holder sequences that
`AbstractService.s.dfy` admits (§4, `grant-without-token`). It is a deterministic oracle, so it
has 1 graph under `fifo` and 4 under `bag` (two observers each read `Locked`/`End` in either
order).

## 3. The implementation program

The implementation has one invisible `node_i` per node, running `NodeInit`, then a bounded loop
of `NodeGrant` / `NodeAccept`, as `NodeImpl.HostNextMain` does (`if held then grant else
accept`):

- A holder grants only while `epoch ≤ K`. This is our bound; IronFleet's is `epoch < 2^64 − 1`.
- Stop rule (ours, schedule-independent). A node that holds epoch K + 1 does not grant. It sends
  `Stop` to every node, its own mailbox included, and ends.
- A non-holder blocks on its mailbox, which carries `Transfer` and `Stop` (one message type
  `NodeMsg`, Java `Lock.NodeMsg`). At the first `Stop` it reads it ends; a second `Stop` stays
  unread. On a `Transfer(e)` it applies the epoch check `e > epoch`.
- On acceptance it sends `Locked(e)` to `obs_i`.
- When a node ends it sends `End` to `obs_i`.

No node counts its acceptances: what ends a node does not depend on the ring schedule. (An
earlier version stopped node i after the ring schedule's number of acceptances for it; that bound
encoded the ring and masked `grant-without-token` at n ≥ 3, §4.)

The loop runs at most `2K + 2` iterations, and it never ends a node by running out: a node grants
at most twice (once per held epoch ≤ K), reads at most three `Transfer`s (its predecessor sends at
most one per held epoch ≤ K plus the one `grant-without-token` transfer), and the iteration that
ends it (its `Stop` broadcast or its first `Stop` read) is one more, so at most 2 + 3 + 1 = 6. We
checked this once with a temporary assertion in the Rust crate over all 18 implementation runs; it
did not fire, and the assertion was removed again. Receive timeouts and the network model's
loss/duplication (IronFleet's set network) are not modelled.

## 4. Where they differ, and why the verdict is what it is

**`correct`.** The ring is deterministic. Transfer t goes from node (t−1) mod n to node t mod n
with epoch t + 1, and the acceptor emits `Locked(t+1)` to its observer. Node 2 mod n then holds
epoch 3 = K + 1 and sends `Stop` to every node. Every other node is waiting on its mailbox and
ends at that `Stop`, and every node emits `End`. No mailbox ever offers two messages at once (each
`Stop` is sent after the last `Transfer`, and a node's `Transfer` causally precedes the `Stop`
it could race with), so the nodes add no branching. So `obs_i` receives exactly the packets the `spec-ring` oracle sends it, followed by `End`.
These are also the packets of one `spec` choice (c = n for epoch 1, c = (e−1) mod n for e = 2, 3).
Under `fifo` each observer has one sender on each side (node i, or the oracle) and reads in send
order. Under `bag` each side lets it read its packets and `End` in any order. The observers'
receives are unordered among themselves on every side: each reads from an invisible sender, and
an observer sends nothing. **CONFORMS** to both specifications under `fifo` and `bag`, n = 2, 3, 4.

**`double-hold`.** Node 1 accepts `Transfer(2)`, emits `Locked(2)`, and grants `Transfer(3)` to
node 2 mod n. Right after that grant it may (`nondet()`) also take epoch 3 itself: it sets
`held = true, epoch = 3` and emits `Locked(3)` to `obs1`. This keeps the token it has just given
away. Holding epoch 3 = K + 1, node 1 then sends `Stop` to every node and ends. Node 2 mod n has
`Transfer(3)` and `Stop` from node 1 in its mailbox. Under `fifo` it reads `Transfer(3)` first
(same sender, send order), accepts it, emits `Locked(3)` to its own observer, and as the second
holder of epoch 3 sends `Stop` to every node too. So epoch 3 has two holders at once, node 1 and
node 2 mod n, which differ for every n ≥ 2 (node 0 at n = 2, node 2 at n ≥ 3).

- Visible word, at n = 2 under `fifo`: `obs1` reads `Locked(2), Locked(3), End` and `obs0`
  reads `Locked(3), End`. At n ≥ 3, `obs2` takes `obs0`'s place. The remaining waiting nodes
  (node 0 at n = 3; nodes 0 and 3 at n = 4) each read one of the two `Stop`s, which gives the
  `fifo` counts 2 / 3 / 5 (1 + 1, 1 + 2, 1 + 4).
- Under `bag` the same word is one of the graphs. `bag` also lets node 2 mod n read `Stop`
  before `Transfer(3)` and end with no `Locked(3)`; that word (`obs1` alone reads `Locked(3)`)
  is one `spec` choice, but the two-holder graphs above remain.
- `spec`: an oracle sends `Locked(3)` to at most one observer, so no specification graph has
  `Locked(3)` read by two observers. In IronFleet's terms, `history[2]` would have to be node 1
  and node 2 mod n at once. **DOES NOT REFINE**, n ≥ 2, **signature value**.
- `spec-ring`: `obs1` never receives `Locked(3)`. **DOES NOT REFINE**, n ≥ 2, **signature
  value**.

Smallest exposing size: n = 2.

**`grant-without-token`.** At start node 1 may (`nondet()`) run `NodeGrant` once without holding
the token, which drops the `s.held` guard of `NodeGrant`. It sends `Transfer(0 + 1)` to node
2 mod n. Below, "fault taken" means that `nondet()` returned true; otherwise the run is the
correct ring's.

- n = 2: the transfer goes to node 0, whose epoch is 1 (`NodeInit`). Node 0's mailbox gets
  `Transfer(1)` and `Transfer(3)`, both from node 1. Under `fifo` node 0 reads `Transfer(1)`
  first and the check `1 > 1` rejects it; under `bag` it may read `Transfer(3)` first, accept it,
  and end at epoch 3 with `Transfer(1)` unread. Either way every graph has the correct ring's
  visible word. **CONFORMS** to both specifications, `fifo` and `bag` (a masked fault, a
  conforming control as rule 4 requires us to say).
- n = 3, `fifo`, fault taken. Node 2 (epoch 0) has `Transfer(1)` and `Transfer(3)` from node 1
  and reads them in that order.
  1. Node 2 accepts `Transfer(1)` and emits `Locked(1)` to `obs2`. Epoch 1 now has two holders:
     node 0 holds it from `NodeInit` and node 2 has accepted it. The two holdings are unordered
     (node 1's start-up send does not wait for node 0's grant), so in some interleavings node 0
     has not yet granted when node 2 accepts. The overlap is not visible: node 0's epoch-1
     holding emits no packet.
  2. Node 2 grants `Transfer(2)` to node 0. Node 0 (epoch 1, released) accepts it, since
     `2 > 1`, and emits `Locked(2)` to `obs0`. Node 1 has accepted `Transfer(2)` from node 0
     and emitted `Locked(2)` to `obs1`. So two nodes emit `Locked(2)`: epoch 2 has two holders,
     node 1 and node 0. Node 0's read is forced: its only other messages are `Stop`s, and node 2
     sent its `Stop` after `Transfer(2)`, node 1 only after node 0's next grant.
  3. Node 2 accepts `Transfer(3)`, emits `Locked(3)`, holds K + 1 and sends `Stop` to all.
     Node 0 grants `Transfer(3)` to node 1. Node 1 holds epoch 2 and has `Transfer(3)` from
     node 0 and `Stop` from node 2, from two senders, so it reads either first. If it reads
     `Transfer(3)` it emits `Locked(3)` too (epoch 3 gets two holders) and sends `Stop` to all.
     Node 0 then ends at one of the `Stop`s.

  This gives 3 graphs with the fault taken plus 1 without, the `fifo` count 4. In every one of
  those 3, `obs0` and `obs1` both read `Locked(2)`. `bag` contains these graphs; it also lets
  node 2 read `Transfer(3)` first (the correct ring's word, `Transfer(1)` unread) and lets
  node 0 read node 2's `Stop` before `Transfer(2)` (then only node 1 emits `Locked(2)`).
- n = 4, `fifo`, fault taken. Node 2 accepts `Transfer(1)` (the same unseen epoch-1 overlap
  with node 0), emits `Locked(1)`, and grants `Transfer(2)` to node 3. Node 3 (epoch 0) reads it
  first (node 2's `Stop` comes after it, node 0's only after node 3's own grant), accepts, emits
  `Locked(2)`, and grants `Transfer(3)` to node 0, which may accept it and emit `Locked(3)`.
  So `obs1` and `obs3` both read `Locked(2)` in every fault-taken `fifo` graph.
  - `spec` (n = 3, 4): the oracle sends `Locked(2)` to at most one observer, so no
    specification graph has `Locked(2)` read by two observers. In IronFleet's terms `history[1]`
    would have to be node 1 and node 0 (n = 3) or node 1 and node 3 (n = 4) at once. **DOES NOT
    REFINE**, n ≥ 3, `fifo` and `bag`, **signature value**. (`obs2` reading `Locked(1)` alone
    would be admitted: the history `[node 2, …]` is one `Service_Init`.)
  - `spec-ring` (n = 3, 4): in no `spec-ring` graph does an observer read `Locked(1)`, and
    `Locked(2)` goes only to `obs1`. **DOES NOT REFINE**, n ≥ 3, `fifo` and `bag`, **signature
    value** (`Locked(1)` from node 2; `Locked(2)` from node 0 or node 3).

An earlier version of this benchmark stopped node i after the ring schedule's count of
acceptances for it. That bound stopped node 0 (n = 3) and node 3 (n = 4) before they could read
node 2's `Transfer(2)`, which hid the second `Locked(2)`, and it made `grant-without-token` look
conforming to `spec` at n ≥ 3. With the schedule-independent stop rule the two specifications
agree on every variant and size here.

## 5. Variants and faults

| impl variant | how it differs | vs `spec` (IronFleet, primary) | vs `spec-ring` (stricter) | reason |
|---|---|---|---|---|
| `correct` | — | CONFORMS (n = 2, 3, 4; `fifo`, `bag`) | CONFORMS (n = 2, 3, 4; `fifo`, `bag`) | §4 |
| `double-hold` | node 1, right after granting `Transfer(3)`, may also take epoch 3 and emit `Locked(3)` | DOES NOT REFINE, n ≥ 2 (`fifo`, `bag`) | DOES NOT REFINE, n ≥ 2 (`fifo`, `bag`) | two observers read `Locked(3)` (value) |
| `grant-without-token` | node 1 may send `Transfer(epoch+1)` = `Transfer(1)` once at start while not holding; at n ≥ 3 node 2 accepts it, so epoch 1 has two holders, node 0 (from `NodeInit`, no packet) and node 2 (`Locked(1)`), and node 2's `Transfer(2)` then gives epoch 2 a second holder | n = 2: CONFORMS (masked); n = 3, 4: DOES NOT REFINE (`fifo`, `bag`) | n = 2: CONFORMS (masked); n = 3, 4: DOES NOT REFINE (`fifo`, `bag`) | `spec`: two observers read `Locked(2)` (value); `spec-ring`: `obs2` reads `Locked(1)`, and a second observer reads `Locked(2)` (value) |

The source ships no faults; both are ours. `double-hold` is the fault that IronFleet's
specification rejects. It takes the "grants itself the lock while another node still holds it"
form. `grant-without-token` is the "grants without the token" form; at n = 2 the epoch check
masks it. The catalogue's other form is a duplicated `Transfer` accepted with the epoch check dropped
(the "accepts a stale transfer" form). It is not built here: with the check dropped, a duplicate
of `Transfer(e)` re-accepted by the same node emits a second identical `Locked(e)` from the same
source, which `Service_Correspondence` (a predicate over a set of packets) admits.

## 6. Files

- `source/`: the Lock service, protocol, implementation and proof `.dfy` files, and
  `IronLockServer/Program.cs`, copied verbatim with provenance in `source/PROVENANCE.md`.
- `rust/`: the Must-API Rust crate `bench_ironlock`. CLI:
  `<spec|spec-ring|impl> <n> [fifo|bag|causal] [correct|double-hold|grant-without-token]` (the
  variant is read only for `impl`). See `_tools/run-rust.sh`.
- `java/`: package `bench.ironlock`, with `Spec` (IronFleet's specification, args `n,model`),
  `SpecRing` (`spec-ring`, args `n,model`) and `Impl` (args `n,model,variant`). Shared parts are
  in `Lock.java`: `NodeMsg` = `Transfer` | `Stop`, `Wire` = `Locked` | `End`, and the observer. Spawn order: both
  specs `oracle, obs0..`; impl `node0.., obs0..`. See `_tools/run-java.sh`.
- `out/logs/`: one log per run below, `rust-<side>-<n>-<model>.log` and
  `java-<side>-<n>-<model>.log`, where `<side>` is `spec`, `spec-ring` or `impl-<variant>`.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| `spec` (IronFleet) | n=2 | fifo | 27 / 0 | 27 / 0 | yes |
| `spec` (IronFleet) | n=2 | bag | 159 / 0 | 159 / 0 | yes |
| `spec` (IronFleet) | n=3 | fifo | 64 / 0 | 64 / 0 | yes |
| `spec` (IronFleet) | n=3 | bag | 412 / 0 | 412 / 0 | yes |
| `spec` (IronFleet) | n=4 | fifo | 125 / 0 | 125 / 0 | yes |
| `spec` (IronFleet) | n=4 | bag | 845 / 0 | 845 / 0 | yes |
| `spec-ring` | n=2 | fifo | 1 / 0 | 1 / 0 | yes |
| `spec-ring` | n=2 | bag | 4 / 0 | 4 / 0 | yes |
| `spec-ring` | n=3 | fifo | 1 / 0 | 1 / 0 | yes |
| `spec-ring` | n=3 | bag | 4 / 0 | 4 / 0 | yes |
| `spec-ring` | n=4 | fifo | 1 / 0 | 1 / 0 | yes |
| `spec-ring` | n=4 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | n=2 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `correct` | n=2 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | n=3 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `correct` | n=3 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | n=4 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `correct` | n=4 | bag | 4 / 0 | 4 / 0 | yes |
| impl `double-hold` | n=2 | fifo | 2 / 0 | 2 / 0 | yes |
| impl `double-hold` | n=2 | bag | 19 / 0 | 19 / 0 | yes |
| impl `double-hold` | n=3 | fifo | 3 / 0 | 3 / 0 | yes |
| impl `double-hold` | n=3 | bag | 29 / 0 | 29 / 0 | yes |
| impl `double-hold` | n=4 | fifo | 5 / 0 | 5 / 0 | yes |
| impl `double-hold` | n=4 | bag | 49 / 0 | 49 / 0 | yes |
| impl `grant-without-token` | n=2 | fifo | 2 / 0 | 2 / 0 | yes |
| impl `grant-without-token` | n=2 | bag | 12 / 0 | 12 / 0 | yes |
| impl `grant-without-token` | n=3 | fifo | 4 / 0 | 4 / 0 | yes |
| impl `grant-without-token` | n=3 | bag | 166 / 0 | 166 / 0 | yes |
| impl `grant-without-token` | n=4 | fifo | 7 / 0 | 7 / 0 | yes |
| impl `grant-without-token` | n=4 | bag | 222 / 0 | 222 / 0 | yes |

All 60 runs were redone on 2026-10-30 after the stop rule of §3 replaced the per-node ring
count; the `spec`, `spec-ring` and `correct` rows came out as before, the `double-hold` and
`grant-without-token` rows changed (except `grant-without-token` at n = 2). Every JMC run ended
with "stack exhausted", meaning the exploration finished. The two tools agree on every row. n = 4 was run because every n = 3 run finished in under a minute on both tools.
The branching comes from three sources:

- the `spec` oracle's (n+1)^3 choices;
- the order in which an observer reads its `Locked` packets and `End` under `bag`;
- the faults' `nondet()`, and then the order in which a node reads its mailbox when it offers
  two messages: two `Transfer`s (`bag`), a `Transfer` and a `Stop` (from two senders under
  `fifo`, any two under `bag`), or two `Stop`s (when epoch 3 has two holders).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, one at a
time, for `n ∈ {2, 3, 4}`, `m ∈ {fifo, bag}`, `v ∈ {correct, double-hold, grant-without-token}`
(logs to `$B/ironlock/out/logs/`; the six commands below write, in order,
`rust-spec-<n>-<m>.log`, `rust-spec-ring-<n>-<m>.log`, `rust-impl-<v>-<n>-<m>.log`,
`java-spec-<n>-<m>.log`, `java-spec-ring-<n>-<m>.log` and `java-impl-<v>-<n>-<m>.log`):

```
$B/_tools/run-rust.sh $B/ironlock spec <n> <m>
$B/_tools/run-rust.sh $B/ironlock spec-ring <n> <m>
$B/_tools/run-rust.sh $B/ironlock impl <n> <m> <v>
$B/_tools/run-java.sh $B/ironlock 'bench.ironlock.Spec:<n>,<m>' must 100000 600
$B/_tools/run-java.sh $B/ironlock 'bench.ironlock.SpecRing:<n>,<m>' must 100000 600
$B/_tools/run-java.sh $B/ironlock 'bench.ironlock.Impl:<n>,<m>,<v>' must 100000 600
```
