# omnipaxos: haraldng/omnipaxos, unmodified, under the Must API

**Family:** real software (case study, second after `raft-rs`)  **Source:** OmniPaxos
(https://github.com/haraldng/omnipaxos, commit 9c772dc, Apache-2.0; `source/PROVENANCE.md`)
**Expected verdict:** `correct`: **no derived verdict** (§4: one decided sequence, but the library
may forward a client's buffered and later proposals out of arrival order, which the specification
forbids; measured conformance at `1 0 1` only, where nothing is decided; every run at `t >= 1`
reached the 600 s cap on the workstation; the campaign rows at 16 threads and the 48 h tier
decide); `reorder`: **DOES NOT REFINE** at `2 1 1` (measured: both conformance engines report an
uncovered implementation execution, §4: a client's own values delivered out of its proposal order);
at `e = 1` unresolved (not measured, no verdict derived).

**Program:** `omnipaxos`$(e,t)$: three OmniPaxos servers (crate `omnipaxos` 0.2.4, sans-I/O
`OmniPaxos<Val, MemoryStorage<Val>>`, unmodified), two visible clients `c0 c1`, client `c_i`
attached to node `n_i`. Each client proposes `e` values `Propose(v)`, `v = 1 + client + 2*j`, to its
node, then receives `Committed(v)` from it until all of its own values have been seen. `t` bounds the
election timeouts each candidate node may fire (an invisible `nondet()` choice while no leader is
known); `cand` (default 3) is the number of nodes allowed to time out (`n0..n{cand-1}`). Model:
`fifo` (the shared `traceforge::program::main`, whose default is `fifo`; OmniPaxos assumes FIFO
links: its accept messages carry per-session sequence numbers and an out-of-order one is treated as
a lost message). Arguments `<variant> <e> <t> [<cand>]`, as in `raft-rs`.

## 1. The protocol (implementation side)

OmniPaxos is Sequence Paxos (a leader-based multi-Paxos replicated log: Prepare/Promise, then
AcceptSync/AcceptDecide/Accepted/Decide, followers forward proposals to the leader) with Ballot
Leader Election (BLE: rounds of HbRequest/HbReply; a node that ends a round as the holder of the
highest ballot it has seen, and is happy, hands its ballot to Sequence Paxos as the new leader).

Each invisible node thread `n_i` (pid `i+1`) owns one `OmniPaxos` instance built by
`ClusterConfig { configuration_id: 1, nodes: [1,2,3], flexible_quorum: None }.build_for_server(
ServerConfig { pid, ..Default::default() }, MemoryStorage::default())`. Its loop:

- either fire one election timeout: `tick()`, only while `get_current_leader()` is `None` (no
  promised ballot) and at most `t` times per candidate node, chosen by `nondet()`;
- or receive one message: an OmniPaxos message (`handle_incoming`) or a client's `Propose(v)`
  (`append(Val(v))`; OmniPaxos itself buffers the entry while no leader is known, forwards it to the
  leader, and the leader buffers it while in its Prepare phase; the harness keeps no queue).

After every step the node drains `take_outgoing_messages` and sends each message with a Must
`send_msg` to the node thread of `get_receiver()`; then it calls `read_decided_suffix(delivered)`
and sends each returned entry `LogEntry::Decided(Val(v))` to its client as `Committed(v)` (`n2` has
no client). Right after construction the node drains the first BLE heartbeat round, which
`OmniPaxos` queues when it is built.

**Library calls used**, and nothing else: `ClusterConfig::build_for_server`, `ServerConfig::default`,
`MemoryStorage::default`, `OmniPaxos::{tick, handle_incoming, append, take_outgoing_messages,
read_decided_suffix, get_current_leader}`, `Message::get_receiver/get_sender`, the `Entry` trait
(`type Snapshot = NoSnapshot`). Not used: `reconfigure`, `trim`, `snapshot`, `reconnected`,
`try_become_leader`, the `omnipaxos_runtime` crate (async actor layer), the `logging`,
`toml_config`, `unicache`, `macros` and `async_runtime` features, persistent storage. The library
core is single-threaded and sans-I/O (no thread, lock, timer, clock or randomness in `omnipaxos/src`
outside the unused `logging` feature): the only concurrency is the harness's three node threads and
the Must transport between them.

**Timers.** `tick()` advances three logical clocks (`LogicalClock`, fires when its counter equals
the timeout): election (default timeout 1: one `tick()` is one BLE heartbeat timeout, the analogue
of raft-rs's `ELECTION_TICK` ticks), message resend (default 100) and batch flush (default 200,
`batch_size` 1). A node ticks at most `t` times, so for `t < 100` resend and flush never fire
(hand-derived from `util.rs::LogicalClock`); with a reliable FIFO transport no resend is needed.
No reconfiguration, no snapshots or trimming, no crashes.

**Who can lead.** On a heartbeat timeout the Ballot Leader Election compares the ballots replied
in the current round with its own and takes the highest as leader; a candidate becomes leader
itself only if no replied ballot is higher than its own (hand-derived from
`ballot_leader_election.rs::hb_timeout`). Under `cand = 1, t = 1` only `n0` (pid 1, the lowest
initial ballot) ticks, so every replied ballot is higher than its own: `n0` leads exactly in the
executions where it times out before reading any HbReply, and in all others nothing is ever
decided (allowed by the spec: proposals may be lost). With more candidates a higher-pid candidate
can lead after reading lower ballots; that case is not analysed here.

Every execution ends blocked (the node threads loop on `recv_msg_block`), so every graph counts as
blocked, as in `raft-rs` and `a4-balancer`.

## 2. The specification program

The specification program of `benchmarks/raft-rs`, unchanged: one invisible `log` thread receives
every proposal (clients send to it directly; destinations are not observed); each proposal is either
appended, and then sent as `Committed(v)` to every client in log order, or lost. Clients are the same
code on both sides. Consequences used below: one total delivery order shared by all clients, and a
client's own proposals, sent in order on one FIFO channel to `log`, are delivered in that order.

## 3. The implementation program

Threads `n0 n1 n2 c0 c1 init` (main spawns and sends nothing; the invisible `init` sends each node
its `Setup`: pid, peer thread ids, attached client). Invisible messages: `Paxos(Wire(m))` carrying an
OmniPaxos `Message<Val>` unchanged; `Wire`'s equality is that of the message's derived `Debug`
rendering (OmniPaxos's `Message` derives `Clone + Debug`, not `PartialEq`), and its `Debug` prints
`Wire(<kind>, <from>, <to>)`. Visible messages: `Propose(u64)`, `Committed(u64)`.

## 4. Where they differ, and why the verdict is what it is

`correct`: **no derived verdict.** What holds: Sequence Paxos keeps one decided log (every
server's decided prefix is a prefix of one sequence); `read_decided_suffix` returns decided entries
only, and each node serves its client the suffix after what it already served, in log order; so
every client sees a prefix of one sequence, and a value that is never decided is the spec's
"lost". What does not hold in general: the specification also preserves each client's own
proposal order, and OmniPaxos does not promise it. A follower buffers proposals while it knows no
leader, forwards proposals that arrive after a Promise identified the leader at once, and forwards
the buffered ones only on the AcceptSync (pinned source: `sequence_paxos/mod.rs:359–383`,
`sequence_paxos/follower.rs:53–84`); so at `2 1 1` the schedule "`n1` buffers `c1`'s value 2
before the Prepare, promises, receives and forwards value 4, then receives the AcceptSync and
forwards value 2" puts `ProposalForward(4)` before `ProposalForward(2)` on the FIFO link, and the
leader may decide 4 before 2: `c1` would observe its own values out of its proposal order, which
no specification execution produces. This schedule is derived from the source, not reproduced by
a run. The harness keeps no proposal queue, so it exposes the library's behaviour as is (raft-rs's
harness queues proposals until a leader is known, a different submission discipline). Measured:
conformance at `1 0 1` only (no timeouts, nothing decided, §7); every run at `t >= 1` reached the
600 s cap on the workstation. The campaign rows decide `correct` at `t >= 1`; the expectation file
carries no verdict for it.

`reorder` at `2 1 1` (**measured**, `out/runs.log`, both engines, `--stop-at-first --threads 4`):
the reported implementation graph has `c0: Propose(1) Propose(3) Committed(3) Committed(1)` and
`c1: Propose(2) Propose(4) Committed(3) Committed(1) Committed(2) Committed(4)`. Schedule (from the
reported graph): `n0` times out before any HbReply and leads; `n1` promises; `n0` appends `1, 3`
(from `c0`) and `2, 4` (forwarded by `n1`); `n1`'s Accepted for the AcceptSync carrying `[1, 3]`
makes `n0` decide both at once, and the Decide makes `n1` decide both at once; the fault reverses
that batch at both nodes. Uncovered: in the spec `c0`'s `Propose(1)` precedes `Propose(3)` on the
FIFO channel to `log`, so `Committed(1)` precedes `Committed(3)` at every client; component:
**visible-word signature** (`c0`'s word has `Committed(3)` before `Committed(1)`; the comparison of
ordered observations in `conformance/sig.rs` fails, before any causal-order comparison). The graph above is the stateless engine's report, at its
first implementation execution. The stateful engine reported one uncovered summary, also at its first
implementation execution (`spec_blocked: 480`, `impl_blocked: 1`); it attaches no graph, so that it is
the same graph is not shown. Both say "candidate violation" (single-graph cover); a
union of spec graphs cannot cover it either, since no spec graph delivers 3 before 1 (hand-derived).

`reorder` at `1 1 1`: **unresolved** (both conformance runs reached the 600 s cap; no verdict is
derived; the campaign rows decide).

## 5. Variants and faults

- `correct`: OmniPaxos as is. No derived verdict (§4: the library may forward a client's buffered
  and later proposals out of arrival order, which the specification forbids); measured conformance
  at `1 0 1` only; the campaign rows decide at `t >= 1`.
- `reorder`: a fault of this harness's delivery layer, not of OmniPaxos: each `read_decided_suffix`
  batch is handed to the client in reverse log order. DOES NOT REFINE at `2 1 1` (measured);
  unresolved at `1 1 1`.

## 6. Files

- `source/PROVENANCE.md`: the OmniPaxos commit, license, date; no source is copied (git dependency).
- `rust/`: `Cargo.toml` (traceforge path dependency `../../../traceforge/traceforge`; `omnipaxos`
  and `omnipaxos_storage` as git dependencies at rev 9c772dcce5a9652192f45f67d9d2208b306f3bc9,
  `default-features = false`), `src/main.rs` (the harness and the spec).
- `out/runs.log`: every run, command and output; `out/export/`: the two exports at `1 0 1`.
- No `java/` twin (Rust only, as `raft-rs`).

## 7. Plain-Must exploration counts and runs (TraceForge only)

| side | args | model | TraceForge execs / blocked | note |
|---|---|---|---|---|
| spec | `correct 1 1 1` | fifo | 0 / 24 | exhausted (the spec depends on `e` only) |
| impl | `correct 1 0 1` | fifo | 0 / 79200 | exhausted; no timeout, no leader: BLE heartbeat traffic only |
| impl | `correct 1 1 1` | fifo | not finished | `--threads 4`, 600 s cap |

| run | args | result |
|---|---|---|
| `conf --engine stateful` | `correct 1 0 1` | silence (certified conforms) |
| `conf --engine stateless` | `correct 1 0 1` | silence (certified conforms) |
| `export spec` / `export impl`, `conf-join` | `correct 1 0 1` | spec 9 lines from 24 graphs, impl 1 line from 79200 graphs; `verdict: conforms`, `uncovered: 0` |
| `conf --engine stateful --threads 4` | `correct 1 1 1` | not finished, 600 s cap |
| `conf --engine stateless --threads 4` | `correct 1 1 1` | not finished, 600 s cap |
| `export impl --threads 4` | `correct 1 1 1` | not finished, 600 s cap (no file written); so no `conf-join` at `1 1 1` |
| `conf --engine stateful --threads 4` | `reorder 1 1 1` | not finished, 600 s cap |
| `conf --engine stateless --threads 4` | `reorder 1 1 1` | not finished, 600 s cap |
| `conf --engine stateless --stop-at-first --threads 4` | `reorder 2 1 1` | 1 candidate violation (§4), `stop_reason: first-miss` |
| `conf --engine stateful --stop-at-first --threads 4` | `reorder 2 1 1` | 1 candidate violation, 1 distinct uncovered summary, `stop_reason: first-miss` |

`1 0 1` is degenerate (nothing is ever decided) and is listed as the largest size that finished; it
measures the BLE traffic alone. Commands: `$CARGO_TARGET_DIR/release/tf_omnipaxos <subcommand> …`
as each `==` line of `out/runs.log` shows, from this directory, `TMPDIR` set, `timeout 600`, one run
at a time. `out/runs.log` drops the engine's progress and per-batch lines (counted in each run's
`[runner: filtered …]` line); unfiltered copies are outside the tree, in
`/local/home/mkhoshechin/tmp/omnipaxos-build/raw/`.
