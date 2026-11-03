# raft(e, t, cand) — two unmodified Raft libraries under a harness (`raft-rs`)

**Communication model:** fifo (`p2p`: per-channel order; raft-rs and MicroRaft both assume an ordered transport per
peer; the variants are not run under bag). **Languages:** Rust (TraceForge's Must API), harness in `rust/`
(tikv/raft-rs is a Cargo git dependency, not copied: `source/` holds only `PROVENANCE.md` and the spike log); Java
(JMC's Must API) in `java/`, a different library (MicroRaft, vendored unmodified in `java/vendor/`), with the same
visible interface and a twin of the specification. **Visible threads:** the clients `c0`, `c1`; the nodes `n0 n1 n2`,
the specification's `log` and the `init` thread are invisible. **Parameters:** `e` = the values each client proposes
(`≥ 1`); `t` = the election timeouts each candidate node may fire; `cand` = the number of candidate nodes
(`n0 … n(cand−1)`, default 3); variant `correct` or `reorder`. ARGS (Rust) `<variant> <e> <t> [<cand>]`; (Java)
`<side>,<variant>,<e>,<t>[,<cand>]`; the specification reads only `e`. Smallest size `e = 1, t = 1, cand = 1`.
**Source:** tikv/raft-rs (https://github.com/tikv/raft-rs, commit `ad13f3d`, `RawNode<MemStorage>`) and MicroRaft
(https://github.com/MicroRaft/MicroRaft, commit `929dae9`, `RaftNodeImpl`), both unmodified; specification: the
client-visible content of Raft's TLA+ (`raft.tla`, Ongaro).

Notation: as in `../../synthetic/naive/README.md`; every receive is blocking and takes any message addressed to the
executing thread. `v(i, j) = 1 + i + 2·j` is client `c_i`'s `j`-th value (`c0`: 1, 3, …; `c1`: 2, 4, …), and
`mine_i = {v(i, 0), …, v(i, e−1)}`. Visible messages: `Propose(v)` (client → its node, or → `log`) and
`Committed(v)` (node or `log` → client). Invisible: `Setup(…)`, `Raft(w)` (an opaque library message `w`), and, on
the Java side only, `Fwd(v)`. A library call is written `rn.f(…)` and is opaque.

The clients, the same code in every program (`D_i` is `n_i` in the implementation, `log` in the specification):

```
Client_i(D_i) ≜ c_i: for j := 0..e−1:  send^fifo(D_i, Propose(v(i, j)))
                     seen := 0
                     while seen < e:
                        Committed(v) := recv^fifo()
                        if v ∈ mine_i:  seen := seen + 1
```

`c_i` sends its `e` values in order, then reads `Committed` messages until it has seen all of its own; the other
client's values are read and not counted. A client ends; nodes and `log` never do (they loop on a blocking
receive), so every execution of either side ends blocked.

The specification (both variants, both languages):

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

`log` is the committed log: it buffers what arrives before `init`'s `Setup`, then, for each proposal in arrival
order, either appends it and delivers `Committed(v)` to both clients, or loses it (one invisible choice). So every
client sees a prefix of one sequence, and a client's own values, sent in order on one FIFO channel, are in that
sequence in proposal order. Spawn order `log, c0, c1, init`.

## The pairs (Rust: tikv/raft-rs)

The implementation, spawn order `n0, n1, n2, c0, c1, init`:

```
Impl(e, t, cand) ≜ n0: Node(0, b_0) ‖ n1: Node(1, b_1) ‖ n2: Node(2, b_2)       b_i = t if i < cand, else 0
                 ‖ Client_0(n0) ‖ Client_1(n1)
                 ‖ init: for i := 0..2:  send^fifo(n_i, Setup(i, [n0, n1, n2], cl_i))   cl_0 = c0, cl_1 = c1, cl_2 = ⊥

Node(i, b) ≜ early := []
             repeat  m := recv^fifo();  if m ≠ Setup(…): early := early · m   until m = Setup(i, ns, cl)
             rn := RawNode::new(id = i+1, voters {1, 2, 3}, election_tick = 2, heartbeat_tick = 1,
                                randomized election timeout = 2, MemStorage)
             pending := [];  budget := b
             loop:
                if budget > 0 ∧ rn.leader_id = ⊥ ∧ rn.role ≠ Leader ∧ nondet({true, false}):
                   budget := budget − 1                                  -- one election timeout
                   for 1..2:  rn.tick();  if rn.has_ready(): break
                else:
                   if early ≠ []:  m := pop(early)  else  m := recv^fifo()
                   if m = Raft(w):     rn.step(w)
                   if m = Propose(v):  pending := pending · v           -- any other message: harness fault
                while pending ≠ [] ∧ rn.leader_id ≠ ⊥:
                   if rn.propose(head(pending)) = Ok:  pop(pending)  else break
                OnReady()

OnReady() ≜ if rn.has_ready():
               rd := rn.ready()
               SendAll(rd.messages)
               Apply(rd.committed_entries)
               storage.append(rd.entries);  storage.set_hardstate(rd.hs)
               SendAll(rd.persisted_messages)
               light := rn.advance(rd);  storage.set_commit(light.commit_index)
               SendAll(light.messages)
               Apply(light.committed_entries)
               rn.advance_apply()

SendAll(ws) ≜ for w in ws:  send^fifo(ns[w.to − 1], Raft(w))
Apply(es)   ≜ for each normal, non-empty entry with value v in es, in log order:
                 if cl ≠ ⊥:  send^fifo(cl, Committed(v))
```

Each node `n_i` waits for its `Setup` (buffering what overtakes it), builds one `RawNode` and loops. One iteration
either fires an election timeout (an invisible choice, allowed only while the node knows no leader, at most `b` times;
up to two `tick()`s, stopping as soon as raft-rs has something ready) or takes one message: a raft-rs message goes to
`step`, a client's `Propose(v)` is queued. Queued proposals wait until a leader is known, then go to `propose` (raft-rs
itself forwards a follower's proposal to the leader as `MsgPropose`; an `Err` leaves it queued). Then the node drains
`ready()` in raft-rs's order (messages, committed entries, persist entries and hard state, persisted messages,
`advance`, the light ready's messages and committed entries, `advance_apply`) and hands each committed client value to
its client as `Committed(v)`; `n2` has no client. The leader's empty entries are skipped. No snapshots, configuration
changes or crashes. A leader never fires a timeout, so no heartbeat is ever ticked.

### correct — expected: **conforms** (`e ≥ 1, t ≥ 1`)

raft-rs as is. The implementation has three nodes, a wire protocol, election timeouts and a log per node; the
specification has one `log` thread and no nodes (which invisible thread a client talks to is not observed). raft-rs's
committed log is one sequence (State Machine Safety, Log Matching) and each node applies it in index order, so each
client receives a prefix of one sequence; a proposal that is never committed (it reached no leader, or its entry was
overwritten) is the specification's lost proposal. The expectation file gives `conforms` for `e ≥ 1, t ≥ 1`; it is
certified at `e = 1, t = 1, cand = 1` by the spike of 2026-10-31 (in-process stateful `conf`, and the join of the
two exports, 0 uncovered; `source/spike-runs.log`).

### reorder — **exploratory, no verdict**

```
Apply(es) ≜ for each normal, non-empty entry with value v in reverse(es):         ← changed: reverse log order
               if cl ≠ ⊥:  send^fifo(cl, Committed(v))
```

A fault of the harness's delivery layer, not of raft-rs: each batch of committed entries (each of the two `Apply`
calls of `OnReady`) is handed to the client in reverse log order. It would be visible only if one `Apply` call
received two client entries; no schedule is known in which it does (with raft-rs's
`max_apply_unpersisted_log_limit = 0`, `ready()` applies only up to the persisted index, which ruled out the last
candidate schedule), and no argument shows the fault inert. The variant is listed in `exploratory-variants.tsv`: its
rows run as an experiment and are not reported as a benchmark verdict. At `1 1 1` its graph count equals
`correct`'s (1 323 550 blocked), an observation at that size.

## The pairs (Java: MicroRaft)

`java/bench/microraft/` runs a different library, MicroRaft, under a harness of its own; the clients and the
specification are twins of the Rust ones (spec spawn order `log, c0, c1, init`). The implementation, spawn order
`n0, n1, n2, c0, c1`, has no `init` (JMC thread ids exist before any thread starts, so each node is given the ids
directly):

```
ImplJ(e, t, cand) ≜ n0: NodeJ(0, b_0, c0) ‖ n1: NodeJ(1, b_1, c1) ‖ n2: NodeJ(2, b_2, ⊥)    b_i = t if i < cand, else 0
                  ‖ Client_0(n0) ‖ Client_1(n1)

NodeJ(i, b, cl) ≜ raft := RaftNodeImpl(endpoint i, members {0, 1, 2}, executor/transport/store/state machine = this node,
                                       fixed clock at 3600 s, Random(i))
                  tasks := [];  timers := [];  out := [];  applied := []
                  raft.start();  RunTasks()                           -- its PreVoteRequests are dropped, not sent
                  pending := [];  budget := b
                  loop:
                     if budget > 0 ∧ raft.leader = ⊥ ∧ raft.role ≠ LEADER ∧ nondet({true, false}):
                        budget := budget − 1;  Fire()                  -- one election timeout
                     else:
                        m := recv^fifo()
                        if m = Raft(w):     raft.handle(decode(w));  RunTasks()
                        if m = Propose(v):  pending := pending · v
                        if m = Fwd(v):      if raft.leader ≠ ⊥:  Submit(raft.leader, v)     -- else dropped
                     while pending ≠ [] ∧ raft.leader ≠ ⊥:
                        Submit(raft.leader, pop(pending))

Submit(L, v) ≜ if L = i:  raft.replicate(v);  RunTasks()
               else:      send^fifo(n_L, Fwd(v))
Fire()       ≜ due := timers;  timers := []
               for task in due:  task.run();  AfterTask()
               RunTasks()
RunTasks()   ≜ while tasks ≠ []:  task := pop(tasks);  task.run();  AfterTask()
AfterTask()  ≜ for (to, w) in out:  send^fifo(to, Raft(w))
               out := []
               for v in applied:  if cl ≠ ⊥:  send^fifo(cl, Committed(v))
               applied := []

-- the pieces MicroRaft calls back (they only record; no Must event inside the core):
execute(task), submit(task)  ≜ tasks := tasks · task
schedule(task, delay)        ≜ timers := timers · task
send(target, msg)            ≜ out := out · (n_target, encode(msg))    -- a PreVoteRequest outside Fire is dropped
                                                                        -- during start(), a harness fault otherwise
runOperation(index, v)       ≜ applied := applied · v                  -- the new-term marker is not recorded
```

The loop is the Rust one, with MicroRaft's interface: an election timeout runs, once and in scheduling order, every
task the core has scheduled so far (pre-vote, leader-election, heartbeat and report tasks; tasks they schedule wait
for the next firing); each executed task's outgoing messages, then its applied client values (one batch, in log
order) are sent after the task returns. Differences from the Rust harness, each forced by MicroRaft: the start-up
pre-vote round is not delivered (a node campaigns only through a firing, as a raft-rs node only through its ticks;
otherwise `cand` would have no effect); MicroRaft has no proposal forwarding, so the harness forwards `Fwd(v)` to
the known leader, and a node receiving `Fwd(v)` with no known leader drops it; a failed `replicate` loses the value
instead of keeping it queued; the batch is one core task, not one `ready()`. Configuration: MicroRaft defaults,
except `commitCountToTakeSnapshot = 100` and `maxPendingLogEntryCount = 16`; no snapshot, leadership transfer or
crash. The builder is bypassed (`java/harness/io/microraft/impl/MustRaftNodes.java`) because it creates a thread
executor.

### correct — expected: **conforms** (an expectation scoped to `e = 1, t = 1, cand = 1`)

MicroRaft as is. The argument is the Rust one (one committed sequence, applied in index order at every node; a
value whose replication fails is never applied, i.e. lost). Not derived: that every blocked status and every causal
order of the implementation is covered; that is left to the measurement. Measured only partially: JMC's enumeration
does not finish at `1 1 1` in 600 s (39 927 graphs, 4 distinct summaries, all covered by the specification).
`e ≥ 2` is not run and nothing is claimed there (leader changes could put a client's values in the log out of
proposal order through forwarding to different leaders). The expectation file has no MicroRaft row.

### reorder — **violation** (measured at `e = 1, t = 1, cand = 1`)

```
AfterTask() ≜ … ;  for v in reverse(applied):  if cl ≠ ⊥:  send^fifo(cl, Committed(v))    ← changed
```

Each core task's batch of applied values is sent in reverse log order. Unlike for raft-rs, the witness shows one
task applying two client entries at this size. JMC's online check (against either specification export) reports the uncovered words
`c0: S(Propose(1)) R(Committed(1))`, `c1: S(Propose(2)) R(Committed(2))`, both clients done. A hand-derived reading of the witness: the log is `[2, 1]`,
`n0` applies both in one task, reversed to `1, 2`, and `c0` stops after `1`; `n1` applies `2` alone. The
specification delivers one order to both clients: if `1` is first, `c1` reads `Committed(1)` first, if `2` is first
`c0` does. Component: signature. The 600 s export also has the opposite-orders pattern
(`c0: … R(Committed(2)) R(Committed(1))`, `c1: … R(Committed(1)) R(Committed(2))`).

## What it is for

A refinement check of real software: the implementation side is a production Raft library (raft-rs, used by TiKV;
MicroRaft), unmodified, wired to the Must API by a few hundred lines of harness, and the specification is Raft's
client-visible promise, one committed log. The two languages run two different libraries against the same
specification, and the Rust and Java exports join in both directions. Every graph ends blocked and the
implementation's graphs outnumber the specification's by orders of magnitude (24 specification graphs against
1 323 550 raft-rs graphs at `1 1 1`), so the paper reports it as a case study with the sizes reached, not as a
scaling row.
