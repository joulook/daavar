# omnipaxos(e, t, cand) — an unmodified OmniPaxos cluster under a harness (`omnipaxos`)

**Communication model:** fifo (`p2p`: per-channel order, the default of `traceforge::program::main`; OmniPaxos
assumes FIFO links: its accept messages carry per-session sequence numbers and an out-of-order one is treated as
lost). **Language:** Rust (TraceForge's Must API), harness in `rust/` (OmniPaxos is a Cargo git dependency, not
copied: `source/` holds only `PROVENANCE.md`); no Java side. **Visible threads:** the clients `c0`, `c1`; the nodes
`n0 n1 n2`, the specification's `log` and the `init` thread are invisible. **Parameters:** `e` = the values each
client proposes (`≥ 1`); `t` = the election timeouts each candidate node may fire; `cand` = the number of candidate
nodes (`n0 … n(cand−1)`, default 3); variant `correct` or `reorder`. ARGS `<variant> <e> <t> [<cand>]`; the
specification reads only `e`. **Source:** OmniPaxos (https://github.com/haraldng/omnipaxos, commit `9c772dc`, crates
`omnipaxos` 0.2.4 and `omnipaxos_storage` 0.2.4, `OmniPaxos<Val, MemoryStorage<Val>>`, unmodified); specification:
the replicated log of `../raft/README.md`, unchanged (the client-visible content of Raft's TLA+ specification `raft.tla` by Diego Ongaro, PhD thesis, Stanford 2014); a second specification derived from the Omni-Paxos paper (Ng, Haridi, Carbone, EuroSys 2023) by a context-free agent is in `benchmarks/omnipaxos-derived/` and is checked against this one in both directions.

Notation: as in `../../synthetic/naive/README.md`; every receive is blocking and takes any message addressed to the
executing thread. `v(i, j) = 1 + i + 2·j` is client `c_i`'s `j`-th value (`c0`: 1, 3, …; `c1`: 2, 4, …), and
`mine_i = {v(i, 0), …, v(i, e−1)}`. Visible messages: `Propose(v)` (client → its node, or → `log`) and
`Committed(v)` (node or `log` → client). Invisible: `Setup(…)` and `Paxos(w)` (an opaque OmniPaxos message `w`:
Ballot Leader Election's `HbRequest`/`HbReply`, Sequence Paxos's `Prepare`, `Promise`, `AcceptSync`, `AcceptDecide`,
`Accepted`, `Decide`, `ProposalForward`, …). A library call is written `op.f(…)` and is opaque.

The clients, the same code in both programs (`D_i` is `n_i` in the implementation, `log` in the specification):

```
Client_i(D_i) ≜ c_i: for j := 0..e−1:  send^fifo(D_i, Propose(v(i, j)))
                     seen := 0
                     while seen < e:
                        Committed(v) := recv^fifo()
                        if v ∈ mine_i:  seen := seen + 1
```

`c_i` sends its `e` values in order, then reads `Committed` messages until it has seen all of its own; the other
client's values are read and not counted. A client ends; nodes and `log` never do, so every execution of either side
ends blocked.

The specification (both variants), the program of `../raft/README.md`, spawn order `log, c0, c1, init`:

```
Spec(e) ≜ Client_0(log) ‖ Client_1(log)
        ‖ init: send^fifo(log, Setup([c0, c1]))
        ‖ log:  cs := ⊥;  early := []
                loop:
                   m := recv^fifo()
                   if m = Setup(xs):  cs := xs   else  early := early · m
                   if cs ≠ ⊥:
                      while early ≠ []:
                         m := pop(early)
                         if m = Propose(v) ∧ nondet({true, false}):
                            send^fifo(c0, Committed(v));  send^fifo(c1, Committed(v))
```

`log` is the decided log: after `init`'s `Setup` it takes each proposal in arrival order and either appends it,
delivering `Committed(v)` to both clients, or loses it. So every client sees a prefix of one sequence, and a client's
own values, sent in order on one FIFO channel, are in that sequence in proposal order.

## The pairs

The implementation, spawn order `n0, n1, n2, c0, c1, init`:

```
Impl(e, t, cand) ≜ n0: Node(0, b_0) ‖ n1: Node(1, b_1) ‖ n2: Node(2, b_2)       b_i = t if i < cand, else 0
                 ‖ Client_0(n0) ‖ Client_1(n1)
                 ‖ init: for i := 0..2:  send^fifo(n_i, Setup(i, [n0, n1, n2], cl_i))   cl_0 = c0, cl_1 = c1, cl_2 = ⊥

Node(i, b) ≜ early := []
             repeat  m := recv^fifo();  if m ≠ Setup(…): early := early · m   until m = Setup(i, ns, cl)
             op := ClusterConfig(configuration_id = 1, nodes {1, 2, 3}, no flexible quorum)
                      .build_for_server(ServerConfig(pid = i+1, defaults), MemoryStorage)
             delivered := 0;  budget := b
             Flush()                                                    -- BLE's first heartbeat round
             loop:
                if budget > 0 ∧ op.get_current_leader() = ⊥ ∧ nondet({true, false}):
                   budget := budget − 1;  op.tick()                     -- one election timeout
                else:
                   if early ≠ []:  m := pop(early)  else  m := recv^fifo()
                   if m = Paxos(w):    op.handle_incoming(w)
                   if m = Propose(v):  op.append(v)                     -- any other message: harness fault
                Flush()
                Deliver()

Flush()   ≜ for w in op.take_outgoing_messages():  send^fifo(ns[w.receiver − 1], Paxos(w))
Deliver() ≜ es := op.read_decided_suffix(delivered)
            if es ≠ ⊥:
               delivered := delivered + |es|
               for each Decided(v) in es, in log order:  if cl ≠ ⊥:  send^fifo(cl, Committed(v))
```

Each node `n_i` waits for its `Setup` (buffering what overtakes it), builds one sans-I/O `OmniPaxos` instance with the
library defaults (election timeout 1 tick, resend 100, batch flush 200, batch size 1) and sends the heartbeat round
the instance queues when built. One iteration either fires an election timeout (an invisible choice, allowed only
while the node knows no leader, at most `b` times; one `tick()` is one Ballot Leader Election heartbeat timeout) or
takes one message: an OmniPaxos message goes to `handle_incoming`, a client's `Propose(v)` to `append`. The harness
keeps no proposal queue: OmniPaxos buffers an entry while no leader is known and forwards it to the leader
(`ProposalForward`) itself. After each step the node sends every outgoing message to the node thread of its receiver,
then reads the decided entries past `delivered` and sends each to its client as `Committed(v)`; `n2` has no client.
With `t < 100` the resend and flush timers never fire. No reconfiguration, snapshots, trimming or crashes; the
library is single-threaded and uses no clock, timer or randomness of its own.

Who can lead: on a heartbeat timeout a node takes the highest ballot replied in the round as leader, and becomes
leader itself only if no replied ballot is higher than its own. At `cand = 1` only `n0` (the lowest initial ballot)
ticks, so `n0` leads exactly in the executions where it times out before reading any `HbReply`; in all others
nothing is decided.

### correct — expected: **conforms** at `t = 0`; **no derived verdict** at `t ≥ 1`

OmniPaxos as is. The implementation has three nodes, leader election, a replicated log per node and decided-suffix
reads; the specification has one `log` and no nodes. What holds: Sequence Paxos keeps one decided log (every node's
decided prefix is a prefix of one sequence), `read_decided_suffix` returns decided entries only, and each node
serves its client in log order, so every client sees a prefix of one sequence; an undecided value is the
specification's lost proposal. What does not hold in general: the specification also keeps each client's own values
in proposal order, and OmniPaxos does not promise it. A follower buffers proposals while it knows no leader,
forwards later ones at once after a `Promise`, and forwards the buffered ones only on the `AcceptSync`
(`sequence_paxos/mod.rs:359–383`, `sequence_paxos/follower.rs:53–84`); so at `2 1 1` the schedule "`n1` buffers
`c1`'s 2, promises, forwards 4, then receives the `AcceptSync` and forwards 2" puts `ProposalForward(4)` before
`ProposalForward(2)`, and the leader may decide 4 before 2: `c1` would see its values out of order, which no
specification execution produces. This schedule is derived from the source, not reproduced by a run.

At `t = 0` (the expectation file's rule, `e ≥ 1`) no node ever times out, no leader is elected, nothing is decided:
both sides lose every proposal, and the clients block with the same words. Measured at `1 0 1`: silence under both
engines and the join (`conforms`, 0 uncovered). At `t ≥ 1` the expectation file has no row; every workstation run
reached the 600 s cap, and the campaign rows decide.

### reorder — expected: **violation** (`e ≥ 2, t ≥ 1`); **conforms** at `t = 0`

```
Deliver() ≜ es := op.read_decided_suffix(delivered)
            if es ≠ ⊥:
               delivered := delivered + |es|
               for each Decided(v) in reverse(es):  if cl ≠ ⊥:  send^fifo(cl, Committed(v))    ← changed
```

A fault of the harness's delivery layer, not of OmniPaxos: each decided batch is handed to the client in reverse log
order. Measured at `2 1 1` (both engines, `--stop-at-first`): `c0: Propose(1) Propose(3) Committed(3) Committed(1)`,
`c1: Propose(2) Propose(4) Committed(3) Committed(1) Committed(2) Committed(4)`. Schedule (from the reported graph):
`n0` times out before any `HbReply` and leads, `n1` promises, `n0` appends `1, 3` and the forwarded `2, 4`; the
`Accepted` for the `AcceptSync` carrying `[1, 3]` makes `n0` decide both at once, the `Decide` makes `n1` decide
both at once, and the fault reverses that batch at both nodes. Uncovered: in the specification `c0`'s `Propose(1)`
precedes `Propose(3)` on the FIFO channel to `log`, so `Committed(1)` precedes `Committed(3)` at every client.
Component: signature. At `t = 0` nothing is decided and the fault is inert (expectation file; measured silence at
`1 0 1`). At `e = 1, t ≥ 1`: unresolved (the `1 1 1` runs reached the 600 s cap), no verdict.

## What it is for

A second refinement check of real software, after `../raft/`: an unmodified Sequence Paxos library with Ballot
Leader Election, against the same specification of a replicated log. The harness keeps no proposal queue, so the
library's own buffering and forwarding are exposed, and `correct` is open at `t ≥ 1`: whether OmniPaxos keeps a
client's proposal order, which the specification promises, is what the campaign runs decide. The implementation's
graphs outnumber the specification's even without a leader (79 200 against 24 at `1 0 1`), so it is a case study
with the sizes reached, not a scaling row.
