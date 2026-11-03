# Ring leader election with loss, duplication and retransmission (Igloo, OOPSLA 2020)

**Family:** related-work  **Source:** Sprenger, Klenze et al., "Igloo", OOPSLA 2020, leader-election
case study: implementation `related-work-papers/sources/igloo-vm/extracted/home/igloo/igloo-python/leader-election/leader-election-python-executable/leader/election.py`
(and the Java `igloo-java/leader-election-sequential/src/impl/Leader.java`), models
`igloo-isabelle/case-studies/leader-election/Leader_Election_0..4.thy` (candidate N23, record "IG
leader election" in `CATALOGUE-artifacts-2.md`)  **Expected verdict:** `correct`: **CONFORMS**
(only the top node can ever be elected, possibly none, under loss, duplication and reordering);
`no-compare`: **DOES NOT REFINE** (a node adopts smaller ids, so node 0's own id returns to it and
node 0, not the top, is elected at n = 2).

## 1. The protocol

`election.py`, `mainLoop(out_addr, my_id, in_port)`: `to_send := my_id`; send it to the successor;
loop forever: `msg := try_receive_int(rec_socket)` (UDP with `settimeout(0.5)`: `None` on a
timeout); if `msg == my_id`: "I am the leader", stop; elif `msg > to_send`: `to_send := msg`; in
every iteration that did not stop, send `to_send` again (retransmission). The environment
(`Leader_Election_3.thy`, `tr3e`): a node's input is a *set* of messages; `tr3e_receive s x msg s'
≡ msg ∈ s (addr x) ∧ s = s'` (a receive does not remove: duplication; a set: reordering; a message
may never be received: loss); `tr3e_send` inserts into the successor's set. The ring is
`Ring_network.thy`'s `example012`: `Zero → One → Two → Zero`, top `Two`.

The properties: `U_0` (`Leader_Election_0.thy`, the trace property of the abstract model: no two
different nodes are elected) and `inv_leader_top` (`Leader_Election_1.thy`, an invariant of the
first refinement, carried down the chain: "only top can be elected leader").

Roles:

- Visible: `observer` (added: the source prints "I am the leader: id").
- Invisible: spec `oracle`; impl `node0 .. node{n-1}`.

Messages: `Id(v)` (node → successor, `send_int`) and `Status { node, leader }` (node → observer,
added: each node's final `leader` flag, the `tr0_state_local` of the abstract model, sent when the
node stops).

Parameters: `n` nodes (the size; ids `0 .. n−1` on the ring `i → i+1 mod n`, `example012` at n = 3,
top `n − 1`); `R = 1` loop iteration after the initial send; `D = 1` duplicated send per node.
Model `bag`.

## 2. The specification program

One invisible `oracle`: `elected := nondet()`; send `Status { node: i, leader: elected && i == n−1 }`
for every `i`. This is `inv_leader_top` together with `U_0`: at most the top is elected, and
possibly nobody (with loss, or a bounded run, an election need not happen; the source proves no
liveness). Graphs: `2 · n!` (the observer's receive order).

## 3. The implementation program

`node_i` (`mainLoop` with the loop bounded): `to_send := i`; `udp_send(to_send)`; `R` times: `msg :=
recv_msg()` (non-blocking, may read ⊥ even when a message is pending: the timeout); `Some(Id(i))`:
`leader := true`, stop; `Some(Id(m))` with `m > to_send`: `to_send := m`; then `udp_send(to_send)`.
Finally `Status { node: i, leader }` to the observer. `udp_send(v)` sends `Id(v)` and, while the
node's duplication budget lasts, may (`nondet()`) send it a second time.

Departures: the unbounded loop is bounded to `R = 1` iteration and the environment's duplication to
`D = 1` extra copy per node (the source's set semantics allows any number of re-receipts); loss is
not explicit, since under `bag` with non-blocking receives a lost message is one that is never
read, which the exploration already contains; the `Status` messages and the `observer` are added;
the two-socket split of the source (receive socket, send socket) is the node's inbox and its
successor's.

## 4. Where they differ, and why the verdict is what it is

**`correct`, CONFORMS.** `to_send` is the maximum id a node has seen and starts at its own id, so
every id node j forwards is at least j. Node i's own id `i < n−1` would have to pass node `n−1`,
whose `to_send ≥ n−1 > i`, so it never comes back to i: no node other than the top receives its own
id, and every `Status` with `leader = true` is the top's. Duplicates and reordering only repeat or
permute ids that satisfy the same bound. So every implementation word is the observer reading `n`
statuses, at most the top's being `true`, in some order: a specification word (the oracle's
`elected` is whether the top was elected; the order is free on both sides; one visible thread, no
causal counts).

**`no-compare`, DOES NOT REFINE**, smallest size n = 2. Schedule: `node0` sends `Id(0)`; `node1`
sends `Id(1)`, receives `Id(0)` and (the fault: `0 < 1` is adopted) sets `to_send := 0` and sends
`Id(0)`; `node0` receives that `Id(0)`, its own id, and is elected; `node1`'s loop is over. Uncovered
word (and the reverse order):

```
observer: R(Status { node: 0, leader: true }) R(Status { node: 1, leader: false })
```

The specification elects only node 1. Component: **signature value**. At `R = 1` the fault violates
`inv_leader_top` but not `U_0`: two leaders need two loop iterations (each node must first adopt
the other's id and then receive its own), which the bound excludes. 24 of the 194 implementation
graphs are uncovered (those in which node 0 is elected).

The candidate list names "a duplicate elects twice" as the source's fault. The source has no such
fault: a node stops at its first election, and `Leader_Election_0.thy` explicitly allows the same
node to be elected more than once ("We allow the same node to be elected multiple times as leader,
but never two different nodes"). Duplication is therefore part of the environment here, not a
fault.

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `correct` | bag | `election.py`'s `mainLoop` | CONFORMS | §4 |
| `no-compare` | bag | `elif msg > to_send` dropped: every other id is adopted | DOES NOT REFINE (n ≥ 2) | node 0 elected |

The source ships no fault. The seeded fault is the catalogue's (a): it removes the Chang–Roberts
filter, the one comparison `inv_leader_top` rests on, and is reachable at the smallest size.

## 6. Files

- `source/`: `python/election.py`, `int_socket.py`, `run_election.py`; `java/Leader.java`,
  `LocalExampleRun.java`, `UDPSocketHelper.java`; `isabelle/Leader_Election_0..4.thy`,
  `Ring_network.thy`; copied verbatim, provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_ring_election_igloo` on TraceForge's shared main; ARGS `<n> [correct|no-compare]`,
  model by `--model bag`. Visible `observer`.
- `java/`: package `bench.ring_election_igloo`, class `RingElection` (args `<spec|impl>,<n>,<fifo|bag>[,<variant>]`),
  `Support` (model argument and thread joiner, copied from `H1Support`). Spawn order: spec
  `observer, oracle`; impl `observer, node0 .. node{n-1}`; the same in Rust. The non-blocking
  receive is `Channel.tryRecv`. Compiled to `java/out/`.
- `out/runs.log`: every command and its output; `out/export/`: the five summary files of §7.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=2 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | n=2 | bag | 202 / 0 | 202 / 0 | yes |
| impl `no-compare` | n=2 | bag | 194 / 0 | 194 / 0 | yes |
| spec | n=3 | bag | 12 / 0 | 12 / 0 | yes |
| impl `correct` | n=3 | bag | 7506 / 0 | 7506 / 0 | yes |
| impl `no-compare` | n=3 | bag | 7314 / 0 | 7314 / 0 | yes |
| spec | n=4 | bag | 48 / 0 | 48 / 0 | yes |
| impl `correct` | n=4 | bag | 343704 / 0 | not exhausted at 600 s | — |
| impl `no-compare` | n=4 | bag | 339096 / 0 | not exhausted at 600 s | — |

Every run that ended is exhausted and the two tools agree; n = 4 is the first size over a minute
(JMC's impl runs reached the 600 s timeout, exit 124, without printing partial counts), so the
sweep stops there. The spec counts are the hand count `2 · n!`; the implementation counts were not
derived by hand (the receive choices over retransmitted and duplicated ids interact), only checked
equal across the tools.

Conformance and the cross-language join, n = 2, model bag:

| check | spec | impl | verdict |
|---|---|---|---|
| `conf` | Rust spec | Rust `correct` | conforms (silence) |
| `conf` | Rust spec | Rust `no-compare` | 24 candidate violations (exit 1) |
| `conf-join` | Rust spec export | Java `correct` export | conforms, 0 uncovered |
| `conf-join` | Rust spec export | Java `no-compare` export | violation, 2 uncovered (`no-slot`: the word of §4, both orders) |
| `conf-join` | Java spec export | Rust `correct` export | conforms, 0 uncovered |

Commands (X = `/local/home/mkhoshechin/tmp/bench-target/release/bench_ring_election_igloo`, built with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target cargo build --release -j 4` in `rust/`;
J = `java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher`,
Java compiled with `javac -d java/out -cp $(cat daavar/jmc/cp.txt) java/bench/ring_election_igloo/*.java`),
for `n ∈ {2, 3, 4}`, `v ∈ {correct, no-compare}`, all under `timeout 600`:

```
X count spec --model bag <n>
X count impl --model bag <n> <v>
J 'bench.ring_election_igloo.RingElection:spec,<n>,bag' must 0 1 <fresh report dir> - 600 </dev/null
J 'bench.ring_election_igloo.RingElection:impl,<n>,bag,<v>' must 0 1 <fresh report dir> - 600 </dev/null
X conf --model bag 2 <v>
X export spec --out out/export/rust-spec-n2.jsonl --model bag --program ring-election-igloo --size n2r1d1 2
X export impl --out out/export/rust-impl-correct-n2.jsonl --model bag --program ring-election-igloo --size n2r1d1 2 correct
J 'bench.ring_election_igloo.RingElection:<side>,2,bag[,<v>]' must 0 1 <fresh report dir> out/export/java-<side>[-<v>]-n2.jsonl 600 </dev/null
daavar/traceforge/target/release/conf-join <spec file> <impl file>
```
