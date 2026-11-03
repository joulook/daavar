# ironlock(n) — token-ring lock (`ironlock`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication models:** fifo (`p2p`: per-channel order), the Rust program's declared default, and bag (`asyn`:
unordered delivery, `--model bag` in Rust, the model argument `bag` in Java); the campaign ran every variant under
both. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.ironlock.Spec`,
`bench.ironlock.SpecRing` and `bench.ironlock.Impl`, one launcher class per side); context-free Java port in
`java-cf/` (`bench.ironlock.IronLock`). **Visible threads:** `obs0 … obs(n−1)`, where `obs_i` stands for the wire
out of node `i`; the specifications' `oracle` and the implementation's `node0 … node(n−1)` are invisible.
**Parameters:** `n` = the number of nodes (default 2; `n ≥ 2`; smallest interesting size 2 for `correct` and
`double-hold`, 3 for `grant-without-token`); fixed: `K = 2` transfers, so the epochs are `1` (node 0's initial
holding, which emits no packet) and `2, 3` (the two transfers). ARGS `<n> [correct|double-hold|grant-without-token]
[any|ring]`; the implementation side reads the variant (default `correct`) and ignores the spec-kind, the
specification side reads the spec-kind (`any` = `Spec` below, the default; `ring` = `SpecRing`) and ignores the
variant. The campaign's verdict rows pair each variant with `Spec`; the pairs against `SpecRing` are given here
too. **Source:** IronFleet (SOSP 2015),
`ironclad/ironfleet/src/Dafny/Distributed/{Services,Protocol,Impl}/Lock/`: the protocol `Protocol/Lock/Node.i.dfy`,
the implementation `Impl/Lock/NodeImpl.i.dfy`, the specification `Services/Lock/AbstractService.s.dfy`.

Notation: as in `../../synthetic/naive/README.md`. `M` is the model of the run (`fifo` or `bag`); both sides of a
pair run under the same `M`. Messages: `Transfer(e)` and `Stop` (node to node, one inbox per node); `Locked(e)` and
`End` (node `i` to `obs_i`). `Transfer` and `Locked` are IronFleet's `LockMessage`; `Stop` (some node holds epoch
`K + 1`, the bounded run is over) and `End` (node `i` has stopped) are the benchmark's. The observers are common to
every program:

```
Obs(n) ≜ ‖_{i<n} obs_i: for K+2 times:  m := recv^M();  if m = End then break
```

`obs_i` receives the `Locked(e)` packets node `i` emits (the packets `Service_Correspondence` constrains: a
`Locked(e)` from node `i` requires `history[e−1] = i`), then one `End`; it stops at the first `End`, so under bag a
`Locked` read after `End` stays unread.

The two specifications, each one invisible oracle that sends every `Locked` packet and then `End` to every observer:

```
Spec(n)     ≜ Obs(n)
            ‖ oracle: for e := 1..K+1:
                         c := nondet({0, …, n})
                         if c < n then send^M(obs_c, Locked(e))
                      for i := 0..n−1: send^M(obs_i, End)

SpecRing(n) ≜ Obs(n)
            ‖ oracle: for e := 2..K+1: send^M(obs_{(e−1) mod n}, Locked(e))
                      for i := 0..n−1: send^M(obs_i, End)
```

`Spec` is IronFleet's `AbstractService.s.dfy`: the `history` of holders may start at any host (`Service_Init`) and
append any host (`Service_Next`), and `Service_Correspondence` constrains only the packets that exist, so for each
epoch the oracle picks one holder (`c < n`, `history[e−1] = c`) or no packet (`c = n`). Each epoch has at most one
holder; the holders are in any order. `(n+1)^3` oracle choices. `SpecRing` fixes `history[e−1] = (e−1) mod n`, the
one history the correct ring produces; it is stricter than IronFleet's specification (it rejects holder sequences
that `AbstractService.s.dfy` admits) and deterministic: 1 graph under fifo, 4 under bag.

The implementation, one invisible `node_i` running `NodeInit`, then a bounded loop of `NodeGrant` / `NodeAccept`
(`NodeImpl.HostNextMain`: `if held then grant else accept`):

```
Impl_v(n) ≜ Obs(n) ‖ ‖_{i<n} node_i

node_i ≜ held := (i = 0);  epoch := (if i = 0 then 1 else 0);  next := (i + 1) mod n
         ⟨start⟩
         for 2K+2 times:
            if held:
               if epoch ≤ K:  send^M(node_next, Transfer(epoch + 1));  held := false;  ⟨after grant⟩
               else:          for j := 0..n−1: send^M(node_j, Stop);  break
            else:
               m := recv^M()
               if m = Stop:  break
               if m = Transfer(e) and e > epoch:  held := true;  epoch := e;  send^M(obs_i, Locked(e))
         send^M(obs_i, End)
```

Node 0 starts holding epoch 1. A holder grants `Transfer(epoch + 1)` to its successor and releases, but only while
`epoch ≤ K`: the node that holds epoch `K + 1 = 3` sends `Stop` to every node (its own inbox included) and ends.
A non-holder blocks on its inbox: it ends at the first `Stop` it reads (a later `Stop` stays unread) and accepts a
`Transfer(e)` only if `e > epoch` (`NodeAccept`), emitting `Locked(e)`. Every node sends `End` to its observer when it
ends. The loop bound `2K + 2` is never what ends a node. Receive timeouts and IronFleet's loss and duplication are
not modelled.

## The pairs

### correct against Spec — expected: **conforms**

```
⟨start⟩ ≜ skip          ⟨after grant⟩ ≜ skip
```

The ring is deterministic: transfer `t ∈ {1, 2}` goes from node `(t−1) mod n` to node `t mod n` with epoch `t + 1`,
whose acceptance emits `Locked(t+1)` to `obs_{t mod n}`; node `2 mod n` then holds epoch 3 and sends `Stop` to all,
every other node is waiting on its inbox and ends at that `Stop`, and every node emits `End`. No inbox ever offers
two messages at once (a node's `Transfer` causally precedes the `Stop` it could race with), so the nodes add no
branching: `obs_{1 mod n}` receives `Locked(2)` then `End`, `obs_{2 mod n}` receives `Locked(3)` then `End`, every
other observer `End` only. These are the sends of the `Spec` choice `c = n` for `e = 1`, `c = (e−1) mod n` for
`e = 2, 3`. Under fifo each observer has one sender on each side and reads in send order; under bag each side lets it
read its `Locked` and `End` in either order (4 implementation graphs at `n = 2, 3, 4`). The observers' receives are
mutually unordered on both sides (each reads from an invisible sender and sends nothing), so each implementation
graph is covered by the specification graph with the same reads.

### correct against SpecRing — expected: **conforms**

```
the same implementation
```

The packets above are exactly the `SpecRing` oracle's sends (`Locked(e)` to `obs_{(e−1) mod n}` for `e = 2, 3`,
then `End` to all), with the same per-observer read orders under each model, so the cover is one to one (1 graph
against 1 under fifo, 4 against 4 under bag).

### double-hold against Spec — expected: **violation** (`n ≥ 2`)

```
⟨after grant⟩ ≜ if i = 1 and nondet({true, false}) then        ◀ changed: node 1 keeps the token it has just given away
                   held := true;  epoch := epoch + 1;  send^M(obs_1, Locked(epoch))
```

Node 1 accepts `Transfer(2)`, emits `Locked(2)`, grants `Transfer(3)` to node `2 mod n`, and may then also take
epoch 3 itself, emitting `Locked(3)` to `obs1`; holding `K + 1` it sends `Stop` to all and ends. Node `2 mod n` has
`Transfer(3)` and `Stop`, both from node 1; under fifo it reads `Transfer(3)` first, accepts it, emits `Locked(3)`
to its own observer and sends `Stop` to all as a second holder of epoch 3. At `n = 2` the visible word is

```
obs1: R(Locked(2)) R(Locked(3)) R(End)
obs0: R(Locked(3)) R(End)
```

(at `n ≥ 3`, `obs2` in `obs0`'s place). The `Spec` oracle sends `Locked(3)` to at most one observer, so no
specification graph has `Locked(3)` read by two observers (signature value; in IronFleet's terms `history[2]` would be
node 1 and node `2 mod n` at once). This fifo graph is also a bag graph, so the violation holds under both models
(bag additionally lets node `2 mod n` read `Stop` first and end without `Locked(3)`, a word `Spec` admits). The
implementation has 2, 3, 5 fifo graphs at `n = 2, 3, 4` (the choice, then which `Stop` each waiting node reads).

### double-hold against SpecRing — expected: **violation** (`n ≥ 2`)

```
the same implementation
```

The same word: the `SpecRing` oracle never sends `Locked(3)` to `obs1` (it goes to `obs_{2 mod n}`), so `obs1`'s
read of `Locked(3)` is uncovered (signature value).

### grant-without-token against Spec — expected: **conforms** at `n = 2` (masked); **violation** at `n ≥ 3`

```
⟨start⟩ ≜ if i = 1 and nondet({true, false}) then        ◀ changed: NodeGrant without the s.held guard, once at start
             send^M(node_next, Transfer(epoch + 1))         (epoch = 0, so Transfer(1))
```

At `n = 2` the extra `Transfer(1)` goes to node 0, whose epoch is already 1: under fifo node 0 reads it before
node 1's `Transfer(3)` (same sender) and the check `1 > 1` rejects it; under bag it may read `Transfer(3)` first,
accept it, hold `K + 1` and end with `Transfer(1)` unread. Either way every graph has the correct ring's visible
word, which `Spec` covers as above: a masked fault, a conforming control.

At `n ≥ 3` the `Transfer(1)` goes to node 2, whose epoch is 0. Under fifo, fault taken, at `n = 3`: node 2 reads
`Transfer(1)` before `Transfer(3)` (same sender), accepts it and emits `Locked(1)` to `obs2`; now holding epoch 1 it
grants `Transfer(2)` to node 0. Node 0 has granted `Transfer(2)` to node 1 (released, epoch 1) and reads node 2's
`Transfer(2)` (its only other messages are `Stop`s, which node 2 sends after that `Transfer` and node 1 only after
node 0's next grant); `2 > 1`, so it accepts and emits `Locked(2)` to `obs0`, while node 1 has accepted node 0's
`Transfer(2)` and emitted `Locked(2)` to `obs1`. So `obs0` and `obs1` both read `Locked(2)`. The `Spec` oracle
sends `Locked(2)` to at most one observer (signature value; `history[1]` would be node 1 and node 0 at once). At
`n ≥ 4` node 2's `Transfer(2)` reaches node 3 (epoch 0), which accepts it, so `obs1` and `obs3` both read
`Locked(2)`. These fifo graphs are bag graphs too, so the violation holds under both models. (`obs2` reading
`Locked(1)` alone would be admitted by `Spec`: `history = [node 2, …]` is one `Service_Init`.)

### grant-without-token against SpecRing — expected: **conforms** at `n = 2` (masked); **violation** at `n ≥ 3`

```
the same implementation
```

At `n = 2` every graph has the correct ring's word, which `SpecRing` produces. At `n ≥ 3` the fault-taken graphs
above are uncovered twice over: no `SpecRing` graph has an observer read `Locked(1)` (the oracle sends `Locked(2)`
and `Locked(3)` only), and `Locked(2)` goes only to `obs1`, never to `obs0` or `obs3` (signature value).

## What it is for

A verified system's protocol (IronFleet's Lock service) checked against its abstract service, where the visible
threads see only the `Locked` packets `Service_Correspondence` constrains, and against a second, stricter
specification that fixes the ring order. Both faults are signature-value differences (two observers read the same
epoch), and `grant-without-token` is masked at `n = 2` by the epoch check, so its verdict depends on the size. The
`Spec` oracle's `(n+1)^3` choices make the cover many-to-one (27 fifo graphs at `n = 2` against the correct ring's
one).
