# SwiftPaxos, ballot 0

Sources: only `INTERFACE.md`, `PAPER-EXCERPT.md`, and the API sources supplied under `api/` were inspected. No recovery, leader changes, failures, retransmissions, timers, checkpoints, or shared process state are used. All register states begin at zero. Both `Program` entries construct this implementation; the harness supplies the specification. Only `c0` and `c1` are visible.

## Roles and messages

- Main creates `n0`, `n1`, `n2`, then `l0`, `l1`, then `c0`, `c1`, in that order, followed by the invisible `init`. Main only creates and joins threads. `init` sends each replica and library `Setup { nodes, libs, clients }`. Addresses are immutable local copies. Internal messages consumed before Setup are buffered. Untagged client requests arriving before Setup remain buffered in the Must mailbox.
- Clients use the exact `Req`, `Rep`, `value`, and `client` code from the interface. Each completes q sequential operations and terminates.
- Library `li` assigns `(i, sequence)` identifiers and sends `Propagate(id, request)` to all three replicas. It retains the leader's `Reply { ballot: 0, id, paths, result }`, follower FastAck paths, and follower SlowAcks independently of arrival order. Figure 3 line 3 succeeds with Reply plus n1's matching paths or n1's SlowAck; line 4 succeeds with Reply plus n1's or n2's SlowAck. It returns the leader's original result in the exact external Rep type, once only. The leader's own SlowAck does not count as a follower vote. Late acknowledgements for completed commands are ignored; libraries continue receiving forever after q responses.
- Every replica records Propagate in PREACCEPT and retains the command. n0 and n1 each propose all previously propagated conflicting commands, including executed commands. Two Reads commute; all other pairs of distinct commands conflict. n2 makes no proposal.
- n0 appends the new command to `pending_log`, replays that log on a copy of its committed execution state, sends the new command's result and full dependency paths to its library, then broadcasts its FastAck to all replicas, including itself. This does not mutate the actual register. n1 independently broadcasts its FastAck to every replica and the submitting library. A FastAck carries ballot, sender index, identifier, dependency set, and dependency paths.
- Replicas retain FastAcks and SlowAcks by command and sender, including those received before the command's Propagate. Only the leader's FastAck enables ACCEPT. The command must be PREACCEPT and every proposed dependency must already be ACCEPT or COMMIT (Figure 4 line 23). Upon acceptance, lines 25–29 are implemented literally: dependency disagreement at a fast replica, or slow-quorum membership, adopts the leader's dependencies and broadcasts SlowAck to all replicas and the library; otherwise a path disagreement at a fast replica sends SlowAck only to the library. SlowAck carries ballot, sender index, and identifier. Original proposal paths and votes remain immutable snapshots.
- An ACCEPT command commits only after all its dependencies commit, and after n1 has sent a matching FastAck or SlowAck (fast quorum), or either n1 or n2 has sent SlowAck (slow quorum). ACCEPT already certifies receipt of the leader's FastAck. These are exactly the configured quorums {n0,n1} and {n0,n2}; no follower-only majority is used.
- A committed command executes exactly once, after all direct dependencies execute. Executed dependencies also ensure all transitive dependencies have executed. Write replaces x and yields Ack; Read yields Value(x); Rmw increments x by 10 and yields Result(x). Execution removes that identifier from n0's pending log, even if another independent command executes first. Execution sends no additional response. Replicas receive forever.

## Decisions and source ambiguities

1. **Overlapping quorum membership.** §3.1 says, “For a replica p, we write fast(p, b) and slow(p, b) if p belongs to a fast and a slow quorum of b, respectively.” Every replica belongs to a configured slow quorum, including both fast replicas and the leader. Accordingly `slow(p,0)` is true for all three, and the literal line-25 disjunction makes all replicas broadcast SlowAck when accepting. The prose also says, “A slow quorum replica acts similarly to a fast quorum replica, except that it does not make proposals of its own,” suggesting a possible disjoint-role interpretation. This implementation follows the formal membership definition and literal line 25, rather than silently redefining `slow` as `!fast`. Thus n1 double-votes even for matching proposals, fast agreement can still commit before a SlowAck arrives, and the line-29 branch is present but unreachable in this configuration. n2 never proposes. This ambiguity is material to message counts.

2. **No self dependency; pset uses the new proposal.** §3.3 defines proposals as “the set of the previously received commands that conflict with c.” Although Figure 4 stores c before its line-11 comprehension, c is excluded from its own dependency set. Figure 4 computes pset at line 12 before assigning dep at line 13, but §4.1 says FastAck carries “its proposal ... together with c's dependency paths.” The new dependency set is installed before computing its paths. A non-fast replica has an empty placeholder until it accepts the leader's proposal.

3. **Full paths.** Figure 4 lines 37–39 recursively define pset, and §4.1 says “This issue is solved by trimming the paths.” Trimming and hashing are optimizations; this small model keeps the full root-to-leaf path set, using ordered sets only for canonical equality. An empty dependency set yields the singleton path `[id]`. No paths are recomputed inside an already sent FastAck. There is no cycle-breaking policy: the normal-operation dependency preconditions preserve acyclicity.

4. **Optimistic execution representation.** §4.4 says, “the command’s response can be computed by reading the local copy while taking into account prior uncommitted commands in pending_log.” Replay all not-yet-executed log entries in leader reception order on a temporary integer, returning the last entry's result. The log retains committed but unexecuted entries until line 36, so no effects are omitted or applied twice. No speculative follower Read optimization is used.

5. **Handler scheduling and retained messages.** §4 says handlers execute “atomically once its preconditions are true.” The receive loop saves each incoming message and then runs enabled ACCEPT, COMMIT, and execution handlers to quiescence before its next receive. When multiple such handlers are enabled, `nondet()` chooses among all of them, including independent executable commands. Network arrival order chooses which incoming message is consumed. Each local handler has finite work; delayed preconditions never drop a message or block receipt of another one. Multicasts enumerate destinations in replica index order and then the library, which fixes a transport implementation detail, without creating a total order across channels.

6. **Wire format and transport.** §2 says, “We consider a geo-distributed message-passing system where processes may fail by crashing, but do not behave maliciously.” The paper names Propagate, Reply, FastAck, and SlowAck but does not prescribe Rust representations, message tags, sequence encoding, or buffer types. All protocol/setup traffic uses a cloneable internal `Wire` enum tagged 1 via Must `send_tagged_msg`, received with `recv_tagged_msg_block`. External Req and Rep remain untagged and retain their exact required types. Libraries selectively receive untagged requests, so a late internal acknowledgement cannot be decoded as Req. No threads exchange data through mutexes, atomics, channels outside Must, or shared mutable memory. Ballot fields are explicitly zero and checked at receive. Sender indices are carried in internal messages; this is sufficient under the paper's non-malicious process model.

7. **Clients not collocated.** §3.5 says “Clients that are not co-located with a replica need to wait for an extra message delay to hear the response.” The interface's distinct libraries implement that case: they always apply Figure 3 and never return a local replica's committed execution result directly. IDs are assigned by the library, since the fixed external request has no sequence number. No client has more than one outstanding command.

8. **Retention and termination.** §4.1 says “Standard periodic checkpoints can be used to bound memory usage.” This finite workload retains all replica metadata and performs no garbage collection. The interface, rather than the paper, specifies permanent receive loops after clients finish. Main joins these nonterminating processes; the harness stops receive-blocked executions.

9. **Arguments and model.** The entry validates `<q> [fifo|bag]`, q >= 1. `traceforge::program::main` owns the delivery model, selected with its `--model fifo|bag` option; FIFO is its default. The optional positional model word is accepted and validated but does not override the harness model. No protocol randomness or scheduling is implemented with ordinary random generators.

## Build and requested run

Working directory for all commands: this crate directory.

Required build command:

```sh
cargo build --release -j 2
```

The initial build failed resolving `index.crates.io` (Cargo could not download the registry configuration for the dependency `bitvec`). An offline retry was started with:

```sh
CARGO_NET_OFFLINE=true cargo build --release -j 2
```

The offline build succeeded:

```text
Finished `release` profile [optimized] target(s) in 3m 17s
```

The exact required build command was then rerun successfully (exit status 0):

```text
Finished `release` profile [optimized] target(s) in 0.12s
```

Exactly one invocation of the requested run command was started:

```sh
timeout 600 target/release/bench_swiftpaxos_paper_sol count impl --model fifo -- 1
```

The single run reached the 600-second timeout and exited with status **124**. It did not finish exhaustive exploration and did not print the final `execs=... blocked=...` summary. No assertion failure appeared in the captured output. Its exact last output line was:

```text
Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (2066.12/sec).
```

This is the last reported checkpoint, not an exact count at timeout. Receive-blocked executions are expected because libraries and replicas deliberately never terminate; this count command does not itself establish conformance to an external specification. No second run was performed.
