# raft-rs: tikv/raft-rs (Rust) and MicroRaft (Java), unmodified cores, under the Must API

**Program:** `raft`$(e,t)$: three Raft nodes (tikv/raft-rs core, `RawNode<MemStorage>`, commit
ad13f3d, unmodified), two visible clients `c0 c1`, client `c_i` attached to node `n_i`. Each client
proposes `e` values to its node, then receives `Committed(v)` from it until all of its own values
have been seen. `t` bounds the election timeouts each candidate node may fire (an invisible
`nondet()` choice while no leader is known); `cand` (default 3) is the number of nodes allowed to
time out. Model: `fifo` (per-pair order; raft-rs assumes an ordered transport per peer).

**Expected verdict:** `correct`: **CONFORMS** (certified by the spike of 2026-10-31 at
`e=1 t=1 cand=1`, in-process stateful, sequential; `source/spike-runs.log`). `reorder`: **exploratory,
outside the approved variants** (§2: no verdict, no established schedule; its rows run as an
experiment and are not reported as a benchmark verdict).

**Java side: MicroRaft.** `java/` holds the Java side of the case study: the unmodified core of
MicroRaft (https://github.com/MicroRaft/MicroRaft, commit 929dae9, Apache 2.0, vendored under
`java/vendor/`) under JMC's Must API, with the same visible interface and a twin of the
specification, so the Rust and Java exports join in both directions (§7). The two sides are two
real Raft libraries, not twins of one program.

## 1. The protocol (implementation side)

Each invisible node thread owns one `RawNode`. Its loop: either fire an election timeout
(`tick()` ELECTION_TICK = 2 times; only while `leader_id` is unknown, at most `t` times per
candidate node) or receive one message: a raft-rs wire message (`step`), or a client's
`Propose(v)` (queued until a leader is known, then `propose`). After each step the node drains
`ready()`: sends raft-rs's messages to the peers, appends entries to its `MemStorage`, persists the
hard state, and hands **committed** entries to its client as `Committed(v)` (`apply`). No snapshots,
no configuration changes, no crashes. The harness is `rust/src/main.rs`; raft-rs itself is a git
dependency pinned at its commit, with no patch.

The spec is the client-visible content of Raft's TLA+ specification (`raft.tla`, Ongaro; a copy
is in `plan/benchmarks/related-work-papers/sources/mocket-raft-tla`): one totally ordered committed
log. One invisible `log` thread receives every proposal; each is either appended, and then
delivered as `Committed(v)` to every client in log order, or lost (never delivered). Clients are the
same program on both sides.

## 2. Variants

- `correct`: raft-rs as is.
- `reorder` (**exploratory, outside the charter-approved variants of this directory**): a fault of
  this harness's delivery layer, not of raft-rs: `apply` hands a batch of committed entries to the
  client in reverse log order. It carries no verdict and no reachability argument: no schedule is
  known in which one `apply` call of this harness receives two data entries, and no argument shows
  the fault inert. Review rounds 2–4 rejected three candidate schedules; the last one (a follower
  whose inflight window is full, receiving the missing entry together with a higher commit index)
  fails because, with raft-rs's inherited `max_apply_unpersisted_log_limit = 0`, `ready()` applies
  only up to the persisted index and the entry just received is applied by the following
  `LightReady` in a separate call; that explanation is scoped to that schedule, other batching
  schedules are unresolved. A violation, when one is observed, is a set of client words that no
  execution of the specification produces: the specification delivers one total order of the
  appended values to every client, and that order respects each client's own proposal order, so a
  client observing its own values out of proposal order, or two clients observing the same values
  in opposite orders, are two such patterns (the Java side's measured witness, §7, is a third:
  each client sees only its own value, which no single log order with both values appended
  allows). The rows of this variant run as an experiment; their result is not a benchmark verdict
  of this directory and the tables of the paper do not report it as one. raft-rs requires an
  ordered transport, so the variant is not run under `bag`.

## 3. Counts (plain Must, `count`)

Historical, from the spike of 2026-10-31 (`source/spike-runs.log`, crate without the `<variant>`
argument, one thread, 600 s cap, `--model fifo`; every execution ends blocked because the nodes
loop forever, so "complete" is 0 and the count is the blocked graphs):

| command (spike crate) | graphs (complete / blocked) |
|---|---|
| `count spec --model fifo -- 1 0` | 0 / 24 |
| `count spec --model fifo -- 2 0` | 0 / 480 |
| `count spec --model fifo -- 3 0` | 0 / 8960 |
| `count impl --model fifo -- 1 1 1` | 0 / 1,323,550 |
| `count impl --model fifo -- 1 1 2` | not finished in 600 s |
| `count impl --model fifo -- 1 1 3` | not finished in 600 s |
| `count impl --model fifo -- 2 1 1` | not finished in 600 s |
| `conf --model fifo -- 1 1 1` (stateful, in-process) | conforms, exhausted |
| `export spec` + `export impl` at `1 1 1`, `conf-join` | conforms, 0 uncovered |
| `conf --model fifo -- 1 1 2` | not finished in 600 s |

(Wall times of these runs are in `source/spike-runs.log`, one thread.)

Measurements of this crate (with the `<variant>` argument and the full-`u64` value encoding), from
`out/runs.log` (workstation, one thread, 2026-11-01/02):

| command (this crate) | graphs (complete / blocked) |
|---|---|
| `count spec --model fifo -- correct 1 1 1` | 0 / 24 |
| `count spec --model fifo -- correct 2 1 1` | 0 / 480 |
| `count impl --model fifo -- correct 1 1 1` | 0 / 1,323,550 |
| `count impl --model fifo -- reorder 1 1 1` | 0 / 1,323,550 |

Observed: the spec counts and the `correct` implementation count equal the spike's, and `reorder`
and `correct` have the same count at `1 1 1` (an observation at this size, not derived). Wall
times are in `out/runs.log`. Conformance of this crate (`conf`, the exports and the join,
`reorder`'s verdict) is measured by the campaign rows (§4), not run yet.

## 4. Sizes

`<variant> <e> <t> [<cand>]`. The spike: `correct 1 1 1` finished (certified); `correct 1 1 2` and
larger did not finish in 10 min at one thread (every execution ends blocked: the nodes loop
forever, so every graph counts as blocked, as in `a4-balancer`). Campaign rows: `1 1 1` in the
three TraceForge modes at 16 threads; `1 1 2`, `1 2 1` and `2 1 1` on the 48 h tier.

## 5. What this benchmark is for

A refinement check of real software: the implementation side is an unmodified Raft library used
in production (TiKV), wired to the Must API by a 300-line harness. The paper reports it as a case
study with the sizes reached, not as a scaling row.

## 6. Sources and runs

`source/PROVENANCE.md` (raft-rs commit, the TLA+ source, the spike log); `out/runs.log` (every run of
this crate on the workstation, commands and full output).

## 7. Java side: MicroRaft under JMC

**Family:** real software (case study; the Java side of this directory)  **Source:** MicroRaft
core, https://github.com/MicroRaft/MicroRaft commit `929dae9`, Apache 2.0, vendored unmodified in
`java/vendor/` (`source/PROVENANCE.md`)  **Expected verdict:** `correct`: **CONFORMS**
(hand-derived; JMC's enumeration does not finish at the smallest size in 600 s, so measured only as
*conforms (partial)*); `reorder`: **DOES NOT REFINE** (measured: JMC online finds an uncovered
summary against a complete specification, both against the JMC and the TraceForge export of it).

### 7.1 The protocol

Same visible interface as this directory: two visible clients `c0 c1`; on the implementation
side `c_i` is attached to node `n_i` (three invisible nodes `n0 n1 n2`). Each client sends `e`
values `Propose(v)`, `v = value(i, j) = 1 + i + 2*j` (`c0`: 1, 3, …; `c1`: 2, 4, …), to its node,
then receives `Committed(v)` until all of its own values have been seen. Messages (identical tags
and fields on both sides): `Propose(u64)`, `Committed(u64)`. Model: `fifo` (JMC `p2p`) only.

Arguments (Java, `bench.microraft.MicroRaft:<args>`): `<side>,<variant>,<e>,<t>[,<cand>]`; the
Rust crate takes `<variant> <e> <t> [<cand>]` after `count|export spec|impl --model fifo --`.
`t` = election timeouts each candidate node may fire, `cand` = number of nodes allowed to time
out (`n0..n(cand-1)`, default 3). The spec reads only `e`. Smallest size: `e=1 t=1 cand=1`.

### 7.2 The specification program

A twin of raft-rs's `spec_log` (`rust/src/main.rs`): threads `log, c0, c1,
init` in that spawn order; the clients send their proposals to `log`; `init` sends `log` the
clients' ids (`Setup`, invisible); `log` buffers proposals until `Setup`, then for each proposal in
arrival order makes one invisible choice: deliver `Committed(v)` to `c0` then `c1`, or lose it.
This is the client-visible content of Raft's TLA+ (one committed log, delivered in log order,
proposals may be lost). Spec counts are equal on both tools (§7), and the two spec exports are the
same set (`conf-join` conforms in both directions, §8).

### 7.3 The implementation program

Threads `n0 n1 n2 c0 c1`. Each node thread owns one MicroRaft `RaftNodeImpl` (the unmodified
core: pre-vote, leader election, log replication with flow control, commit, apply) and provides the
four pieces MicroRaft leaves to its user (`java/bench/microraft/Node.java`):

- **executor** (`RaftNodeExecutor`): `execute`/`submit` queue the task; the node thread runs the
  queue to empty after each step; `schedule` records the task as a pending timer.
- **timers**: never fired by time. As in raft-rs's harness, at each loop iteration the node may,
  only while it knows no leader and is not leader and has budget left (`t` per candidate node,
  `cand` candidate nodes), make an invisible `Nondet` choice to fire one *election timeout*: every
  task scheduled so far runs once, in scheduling order (MicroRaft's `PreVoteTimeoutTask`,
  `LeaderElectionTimeoutTask`, `HeartbeatTask`, report task; tasks they schedule wait for the next
  firing). A leader never fires, so leader heartbeats and the leader's backoff-reset timer never
  run (raft-rs's harness does not tick a leader either).
- **transport** (`Transport`): `send` serialises the MicroRaft message into a value record
  (`Wire.java`) and the node thread performs the Must send to the target node's thread after the
  core's task has returned; the receiver rebuilds the message with the core's model factory and
  calls `RaftNode.handle`.
- **store** (`RaftStore`): in memory (a list of what the core persists).
- **state machine** (`StateMachine`): `runOperation` collects the applied client values of the
  running task (one *batch*); after the task the node sends them to its client as `Committed(v)`
  in log order. `getNewTermOperation` gives a marker entry (raft-rs's empty leader entry), never
  delivered. `n2` has no client.
- **proposals**: a `Propose(v)` waits at the node until it knows a leader (raft-rs's harness does
  the same); then the leader calls `replicate(v)`, a follower forwards `Fwd(v)` (invisible) to the
  leader. A node receiving `Fwd(v)` replicates it if leader, forwards it if it knows another leader,
  drops it otherwise (raft-rs's `MsgPropose` handling). MicroRaft has no forwarding of its own
  (`replicate` on a follower fails with `NotLeaderException`).

Configuration: MicroRaft defaults, except `commitCountToTakeSnapshot = 100` and
`maxPendingLogEntryCount = 16` (only to keep the log array small per execution; no size reached
here takes a snapshot or hits the pending limit). Clock: `Clock.fixed` at 3600 s (after the
core's 10 s leader-heartbeat timeout from its initial stamp 0: a node that has heard no leader grants
pre-votes; a node that has heard one keeps it). Random: `new Random(i)` per node (only the election
timeout noise, which no timer here reads).

**What JMC cannot control, checked by reading the vendored core** (grep over `java/vendor` for
threads, locks, `synchronized`, `wait/notify`, executors, `sleep`, `System.currentTimeMillis`,
`nanoTime`, `ThreadLocal`, `*Async`, parallel streams, shuffles):
1. the only thread creator is `DefaultRaftNodeExecutor` (a single-thread scheduled executor). It is
   never used, but `RaftNodeBuilderImpl` constructs one in a field initialiser whatever executor is
   set (a `ThreadGroup` and an executor service, no thread started). The harness therefore bypasses
   the builder: `java/harness/io/microraft/impl/MustRaftNodes.java` (harness code in the core's
   package) calls the package-private `RaftNodeImpl` constructor exactly as `build()` does;
2. time: only through the injectable `Clock` (fixed, above); timers only through the executor;
3. randomness: the injectable `Random`, and one `Collections.shuffle(lifecycleAwareComponents)`
   with the JDK's shared generator, which draws nothing because no harness component implements
   `RaftNodeLifecycleAware` (the list is empty);
4. no lock, no `synchronized`; `volatile` fields are read and written by the owning node thread only.
The core catches `Throwable` around `Transport.send` (`RaftNodeImpl.send`) and around every task
(`RaftNodeStatusAwareTask.run`); JMC's control exceptions must not be raised there, so the harness
raises no Must event inside a core frame (sends happen after the task returns) and a harness fault
found inside a core frame (an unexpected message type, a snapshot call, a pre-vote outside a
firing) is recorded and thrown by the node loop.

Departures from MicroRaft as deployed, each a restriction of the environment, not a change of the
core: (a) `start()` runs one pre-vote round at once (`RaftNodeImpl.runStart` → `runPreVote`); its
`PreVoteRequest`s are not delivered (as if lost), so that a node campaigns only through a firing, as
a raft-rs node does only through its election ticks; without this, `cand` would have no effect, every
node campaigning at start. The loop checks on every run that no other `PreVoteRequest` is sent outside
a firing (a harness fault otherwise). (b) Sends leave after the core's task returns, not during it
(the transport is asynchronous anyway). (c) No leader heartbeat, no backoff-reset timer, no clock
advance (§ above). (d) The impl has no `init` thread: Java thread ids exist before any thread starts,
so the nodes are given each other's ids directly (raft-rs needs `init` because a TraceForge id exists
only after its spawn); `init` is invisible, so the visible interface is unchanged.

### 7.4 Where they differ, and why the verdict is what it is

`correct` (hand-derived): MicroRaft's committed log is one sequence (Raft's State Machine Safety)
and every node applies it in index order, so each client receives, from its node, a prefix of that
one sequence; a value whose replication fails or whose entry is overwritten is never applied
anywhere, which the spec models as lost. Each client's sends come first and its receives send
nothing, so the only causal edges between visible events are from a client's `Propose` to the other
client's `Committed` of that value, and the spec's `log` can produce both causal counts (the
proposal received before or after the other one). At `e=1` the per-client order of own values is
trivial; at `e >= 2` with leader changes a client's own values could reach the log in a different
order than sent (forwarding to different leaders), which the spec does not allow; not explored here
(no size `e >= 2` is reachable by JMC).

`reorder` (measured): the fault reverses a batch of applied values. Witness found by JMC (both
against its own spec export and the TraceForge one; §8): `c0: S(Propose(1)) R(Committed(1))`,
`c1: S(Propose(2)) R(Committed(2))`, both done. The spec delivers one log order to both clients: if
1 is first, `c1` receives `Committed(1)` before `Committed(2)`; if 2 is first, `c0` receives
`Committed(2)` first. Schedule (hand-derived reading of the witness): the log is `[2, 1]`; `n0`
applies both in one batch, reversed to `1, 2`, so `c0` sees 1 first; `n1` applies 2 alone first.
Component: signature (no specification summary has this pair of words: `NoSig`). The 600 s
export of `reorder` has two more uncovered summaries (§8): the same words with the other causal
count, and raft-rs README's witness `c0: R(Committed(2)) R(Committed(1))`,
`c1: R(Committed(1)) R(Committed(2))` (log `[1, 2]`, `n0` applies both in one reversed batch, `n1`
applies them one at a time).

### 7.5 Variants and faults

- `correct`: MicroRaft as is. Expected CONFORMS (hand-derived; measured partial, §8).
- `reorder`: harness delivery fault (not MicroRaft's): each batch of applied values (the values
  applied in one core task, i.e. one `applyLogEntries` call) is sent to the client in reverse log
  order, as raft-rs's `reorder` reverses one `Ready`'s committed entries. Expected and measured
  DOES NOT REFINE at `e=1 t=1 cand=1`.

### 7.6 Files

- `java/bench/microraft/`: `MicroRaft.java` (the `MpProgram`, spec twin, clients), `Node.java`
  (node harness), `Wire.java` (transport encoding).
- `java/vendor/`: the MicroRaft core, unmodified, with upstream `LICENSE` and `NOTICE`.
- `java/harness/io/microraft/impl/MustRaftNodes.java`: the builder replacement (§3).
- `java/shim/org/slf4j/`: a minimal SLF4J API for the core's logging (JMC's classpath has none).
- `source/PROVENANCE.md`; `out/java/runs.log` (every command and its full output); `out/java/export/`
  (the summary files joined in §8).
- No `rust/`: the Rust side is this directory (a different Raft core with the same visible
  interface and the same specification).

Compile as `scripts/setup-node.sh` does: `javac -d <out> -cp "$(cat jmc/cp.txt)" $(find
benchmarks/microraft/java -name '*.java' -not -path '*/out/*')`.

### 7.7 Plain-Must exploration counts

Every execution ends blocked (the nodes and `log` loop forever), so "complete" is 0 and the
count is the blocked graphs. TraceForge: `tf_raft_rs` of this directory (built from
`src/main.rs` md5 `9d781b77…`, tree commit `426ee7b` plus the uncommitted edit present at build
time; see `out/java/runs.log`). JMC: launcher `must 0 1 <report> - 600`.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | e=1 | fifo | 0 / 24 | 0 / 24 | yes |
| spec | e=2 | fifo | 0 / 480 | 0 / 480 | yes |
| spec | e=3 | fifo | 0 / 8960 | 0 / 8960 | yes |
| impl correct | 1 1 1 | fifo | raft-rs: 0 / 1,323,550 (export run) | MicroRaft: not finished in 600 s (45,181 blocked graphs explored, stack not exhausted) | not comparable (different cores) |

The impl rows are different programs (raft-rs vs MicroRaft), so equality is not expected. JMC
explores this program at about 75 graphs/s; the MicroRaft graphs have up to 82 events (77 on
average in the 600 s run). Larger impl sizes were not run (the smallest does not finish).

Commands: `tf_raft_rs count spec --model fifo -- correct <e> 0`;
`MpLauncher 'bench.microraft.MicroRaft:spec,correct,<e>,0' must 0 1 <report> - 600`;
`MpLauncher 'bench.microraft.MicroRaft:impl,correct,1,1,1' must 0 1 <report> - 600`.

### 7.8 Cross-language results (`conf-join`, JMC online)

`conf-join` = `daavar/traceforge/target/release/conf-join spec.jsonl impl.jsonl`. Exports at the
smallest size (spec `e=1`, impl `e=1 t=1 cand=1`), in `out/java/export/`.

| specification | implementation | result |
|---|---|---|
| TraceForge spec (complete, 9 lines) | JMC spec (complete, 9 lines) | conforms, 0 uncovered |
| JMC spec | TraceForge spec | conforms, 0 uncovered |
| JMC spec | TraceForge raft-rs impl `correct` (complete, 1,323,550 graphs) | **conforms**, 0 uncovered |
| TraceForge spec | JMC MicroRaft impl `correct` (aborted at 600 s, 39,927 graphs, 4 summaries) | conforms (partial), 0 uncovered |
| TraceForge spec | JMC MicroRaft impl `reorder` (aborted at 600 s, 42,202 graphs, 7 summaries) | **violation (partial)**, 3 uncovered (no-bucket) |
| JMC online, `--spec` JMC spec, `--stop-at-first` | MicroRaft `reorder` | **reports**: uncovered at iteration 72 (witness §4) |
| JMC online, `--spec` TraceForge spec, `--stop-at-first` | MicroRaft `reorder` | **reports**: uncovered at iteration 72, same witness |

An uncovered summary against a complete specification is a violation whatever the
implementation's enumeration status, so `reorder` is measured DOES NOT REFINE. `correct` on
MicroRaft is only partial: the 600 s enumeration covers 4 distinct summaries, all covered.
