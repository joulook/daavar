# broadcast-consensus-civl: context-free Java twin

Class: `bench.broadcast_consensus_civl.BroadcastConsensus` (implements `MpProgram`, public `(String...)` constructor).
Source: `java/bench/broadcast_consensus_civl/BroadcastConsensus.java`.

Argument string: `<spec|impl>,<n>,<fifo|bag>[,<correct|decide-early>]`, e.g.
`bench.broadcast_consensus_civl.BroadcastConsensus:impl,2,bag,decide-early`.
`fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`. Variant defaults to `correct`.
For `spec` the variant is parsed and checked but has no effect (the Rust code does the same).

## Mapping

Messages (Rust: three separate tuple structs, not one enum): sealed interface `Msg extends MpMessage`
with one record each:
- `struct Input(i32)`  -> `@Tag("Input")  record Input(int v)`
- `struct Val(i32)`    -> `@Tag("Val")    record Val(int v)`
- `struct Decide(i32)` -> `@Tag("Decide") record Decide(int d)`

Threads (spawn order as in Rust):
- spec: `oracle` (invisible), then `c0 .. c{n-1}` (visible).
  - `oracle`: n blocking `recv(Input)`, `d = max` starting from `Integer.MIN_VALUE`, then `send Decide(d)` to c0..c{n-1} in order.
- impl: `node0 .. node{n-1}` (invisible), then `c0 .. c{n-1}` (visible).
  - `node_i`: blocking `recv(Input)`; `send Val(v)` to node0..node{n-1} in order (self included);
    `collect` blocking `recv(Val)` (collect = n, or n-1 for `decide-early`), max from `Integer.MIN_VALUE`;
    `send Decide(d)` to `c_i`.
- `c_i` (both): `v = Nondet.chooseBoolean() ? 1 : 0`; `send Input(v)` to oracle / `node_i`; blocking `recv(Decide)`.
- main: invisible, creates every process, starts them in the above order, awaits all; no message events.

Channels: each Rust channel carries exactly one message type and has exactly one receiving thread
(oracle: Input; node_i: Input and Val on two channels; c_i: Decide). In Java each process has one
mailbox and every receive is a typed (type-test) blocking `Channel.recv(model, X.class)`, which selects
the same messages as the Rust per-type channel. All receives in Rust are `recv_msg_block`, so all are
blocking here; there are no non-blocking receives.

`rolesInSpawnOrder()` = `[oracle, c0..]` (spec) or `[node0.., c0..]` (impl); `visibleRoles()` = `[c0 .. c{n-1}]`.

## Doubts and assumptions

1. **Process creation vs. start (main deviation from `MpProcess.spawn`).** Rust creates all channels
   before spawning, so the first-spawned threads (oracle; node_i) can address later ones (c_i). In Java a
   Tid exists only once a process is created, and `MpProcess.spawn` creates and starts in one call; a
   started process can run (and a peer can try to address a not-yet-recorded Tid) before main resumes,
   so filling a Tid array after each `spawn` would be a race. I therefore create every `MpThread`
   (`new MpThread(body, role)`, which mints the task id and raises TASK_CREATED) first, record all Tids,
   then `start()` them in Rust spawn order and `join1()` each. This means all TASK_CREATED events
   precede all starts, unlike `spawn`. Alternative not used: `Tid.of(k+1, role)` relying on the
   "k-th spawned process is task k+1" sentence in MpProgram's Javadoc.
2. **Nondet order.** Rust `if nondet() {1} else {0}`; Java `chooseBoolean()` whose min(S) is `true` (-> 1).
   Same outcome set {0,1}; I do not know whether TraceForge's `nondet()` tries `true` or `false` first.
3. **Program name** `broadcast-consensus-civl` and size `"<n>"` in the header are guesses
   (the Rust `program` struct does not show what TraceForge writes).
4. **Size required.** Rust defaults `n` to 2 when missing; the brief's format makes it mandatory, so I require it.
   n = 1 is accepted; with `decide-early` and n = 1, node0 collects 0 values and decides `Integer.MIN_VALUE`
   (as Rust decides `i32::MIN`).
5. Field names (`v`, `d`) are mine; only tag and field order are encoded.
6. The Rust `Decide(_d)` value is received and discarded by the client; same in Java.

## Counts measured (`must 0 1 report - 600`, plain enumeration)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,bag | 2 | 0 | stack exhausted |
| impl,1,bag | 2 | 0 | stack exhausted |
| spec,2,bag | 8 | 0 | stack exhausted |
| impl,2,bag | 16 | 0 | stack exhausted |
| impl,2,bag,decide-early | 16 | 0 | stack exhausted |
| spec,2,fifo | 8 | 0 | stack exhausted |
| impl,2,fifo | 16 | 0 | stack exhausted |

Hand check (not tuned against anything): spec n=2 = 4 value choices x 2 orders in which the oracle reads
the inputs = 8; impl n=2 = 4 x (2 read orders per node)^2 = 16; decide-early n=2 = 4 x (2 choices of the
one value read per node)^2 = 16.

An export run (spec,2,bag and impl,2,bag) was also done to check that the roles and tags are accepted:
both produced 4 distinct summaries, header `visible:["c0","c1"]`, `model:"asyn"`. (Those export files were
written to /tmp, outside OUT_DIR, and deleted at once; the `report/` and `java/out/` build products were removed.)

Compile/run note: from OUT_DIR the class path file is `../../jmc/cp.txt` (= `daavar/jmc/cp.txt` from the repository root).
