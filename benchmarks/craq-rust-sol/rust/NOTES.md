# One-chunk 3FS chain replication port

## Per-target read retry cut

The latest INTERFACE.md clarification supersedes the consecutive-attempt rule. Each logical read owns three initially empty collections of failed `(update, commit)` answers, one per target. After receiving NotCommit, the client calls `traceforge::assume(false)` if that target has already returned the same pair anywhere in this read. Otherwise it records the pair and immediately selects and sends another attempt. Successful replies complete the read normally. Histories reset at the next logical read; a different answer or target is still explored unless that target/pair was already seen.

The removed unbounded cycle was the client retry loop's ability to alternate n0/n1 while both stayed at `(update=1, commit=0)`. The former single `previous` answer check allowed this indefinitely. `seen[target]` now records all earlier failed answers, cutting the first recurrence even across intervening attempts. This is the study's explicit per-target cut, not a source retry limit. Source StorageClientImpl.cc:166–173 and :1158–1300 retry kChunkNotCommit immediately; the interface replaces its time budget and adds this exploration cut. With finitely many writes there are finitely many version pairs, and each read can receive any failed pair from each target at most once before a cut. There is no other unbounded active loop; nodes intentionally remain blocked on receive after clients finish.

## Roles and messages

The main entry starts n0, n1, n2, then c0, c1, then the invisible init thread, and only joins threads. init sends one tagged Setup to each node with all five process addresses. Nodes receive Setup before servicing any request. Clients know the target addresses through their spawn arguments. Both Program entries return this implementation; visible names are exactly c0 and c1. Nodes never terminate and keep receiving after clients finish.

Each node owns an independent ChunkReplica: integer content, update version, commit version, initially (0,0,0). There is no shared mutable protocol state. The fixed chain version is 1. The per-chunk lock is represented by `active: Option<Write>`; requests received while active enter a FIFO waiting queue. Reads remain serviceable while a write waits for its successor. Queue order follows request arrival order.

| Message | Sender → receiver | Meaning / handling |
| --- | --- | --- |
| Setup, tag 0 | init → each node | Addresses, consumed once before service. |
| Event::Client(client), tag 1 | client → selected node | Invisible dispatch header; the node then receives the untagged Req from that client. |
| Req::Write(client,value) | client → n0 | Exact visible interface request, sent once; allocate update version under the lock. |
| Event::Update{client,value,version,chain}, tag 1 | n0 → n1; n1 → n2 | Apply the same allocated version and value, hold lock, forward once. |
| Event::Committed{version,checksum,chain}, tag 1 | n2 → n1; n1 → n0 | Validate version, chain and content checksum; commit locally before replying upstream. |
| Rep::Ack(client) | n0 → client | Exact visible response, only after n0 commits. |
| Req::Read(client) | client → selected target | First attempt is visible. Subsequent attempts are explicitly invisible. |
| ReadStatus::NotCommit{update,commit} | node → client | Invisible failure; compare to all earlier answers from this target in this read, cut if already seen, otherwise select again and retry immediately. |
| ReadStatus::Ready | node → client | Invisible type-dispatch header for a successful response. |
| Rep::Value(client,value) | node → client | Exact visible read response. |

Only Req and Rep are visible. Explicit `Visibility::Invisible` annotations suppress dispatch headers, read status, and retry sends/receives. Internal node events are invisible because node threads are not visible. The extra headers are a typed-mailbox adaptation: the supplied API's receive requires a concrete Rust type, while a node must handle both the verbatim Req type and internal forwarding/reply types. All actual exchanges still use Must send/receive operations. No node polls, guesses a message type with nondet, or spawns a request handler.

Clients execute exactly q sequential operations with kind `(i+j)%2` and value `1+i+2*j`. `select_target` uses at most two boolean nondet calls: true chooses n0, false/true n1, false/false n2. These are three distinct alternatives without rejection sampling. After a failed read the client completes the receive before selecting another target. The per-target answer histories reset for every logical read.

`handle_update` applies a full in-memory update and changes update version only. `forward_with_retry` sends once to the fixed successor and returns to the receive loop, retaining active/lock state. At the tail it directly calls `do_commit`. A Committed event resumes the pending write and commits before sending an upstream response. This is an event-loop representation of the suspended source coroutine. There are exactly six spawned threads, independent of q and retry count.

## Function mapping

All source paths below are relative to craq-src. Line numbers refer to the supplied cut, whose README.md was read first.

| 3FS function / source lines | Rust function / representation |
| --- | --- |
| StorageClientImpl::write, client/storage/StorageClientImpl.cc:1992–2000 | client write branch |
| batchWriteWithRetry, :1771–1797 | client write branch, single successful request under the fixed fault-free interface |
| batchWriteWithoutRetry, :1799–1809 | client write branch routing to head |
| sendWriteRequest, :1860–1905 | client write send; implicit full chunk and update version 0 at entry |
| StorageClientImpl::read, :1748–1756 | client read branch |
| batchReadWithRetry, :1577–1640 | client read retry loop |
| batchReadWithoutRetry, :1642–1654 | client read request and select_target |
| sendOpsWithRetry, :1140–1300; isFastRetryError, :166 | client immediate receive/reselect/send loop for NotCommit |
| selectServingTargets, :419–487 | fixed targets vector in run |
| selectRoutingTargetForOps, :490 onward | client head routing / select_target |
| LoadBalanceStrategy::selectTarget, client/storage/TargetSelection.cc:20–40 | select_target; interface replaces balancing details with unrestricted nondeterministic choice |
| HeadTargetStrategy, TargetSelection.cc | client uses head |
| StorageOperator::write, storage/service/StorageOperator.cc:233–283 | storage_node Event::Client / Req::Write branch |
| StorageOperator::update, :284–331 | storage_node Event::Update branch |
| ReliableUpdate::update, storage/service/ReliableUpdate.cc:16–128 | storage_node dispatch and active write; duplicate/cache machinery eliminated as unreachable |
| StorageOperator::handleUpdate, StorageOperator.cc:333–520 | handle_update, active/waiting state, Committed branch, do_commit |
| StorageTarget::lockChunk, storage/store/StorageTarget.h:61–63 | active write plus waiting queue |
| StorageOperator::doUpdate, StorageOperator.cc:521–614 | ChunkReplica::update called by handle_update |
| ReliableForwarding::forwardWithRetry, ReliableForwarding.cc:33–104 | forward_with_retry, a single forward with active state waiting for the reply |
| ReliableForwarding::forward, :106–136 | forward_with_retry tail branch; Committed event consumption |
| ReliableForwarding::doForward, :138–280 | Event::Update construction/send |
| StorageOperator::doCommit, StorageOperator.cc:616–640 | do_commit and ChunkReplica::commit |
| StorageOperator::batchRead, StorageOperator.cc:82–231 | batch_read |
| ChunkReplica::aioPrepareRead, storage/store/ChunkReplica.cc:38–78 | ChunkReplica::aio_prepare_read |
| ChunkReplica::aioFinishRead, :79–104 | ChunkReplica::aio_finish_read |
| ChunkReplica::update, :132–318, especially :215–240 and :303–314 | ChunkReplica::update |
| ChunkReplica::commit, :397–469 | ChunkReplica::commit |
| TargetMap head/successor/tail selection, storage/service/TargetMap.cc:193,221–254 | fixed index checks and successor index+1 |

## Decisions, simplifications, and loop audit

- Fixed chain, version 1, all targets serving: replace TargetMap.cc:193–254 and StorageOperator.cc:338–397 routing/state checks with fixed indices. No membership, resync, leases, failure detector or routing changes are represented, as the interface requires.
- Initial chunk exists, content and both versions zero: use Default metadata. Source ChunkReplica.cc:132 onward supports more creation cases; the interface fixes initialization.
- ChunkReplica.cc:215–240 allocates a head version when input updateVer is zero and otherwise checks committed/stale/missing versions. Full write serialization and exactly-once messages make those error branches unreachable here; assertions require the unique next version and a previously committed chunk. The head increments exactly once under active lock ownership. There is no duplicate write or update resend.
- Successful source updates finish in CLEAN state (ChunkReplica.cc:303–314), while COMMIT requires equal versions (:450 onward). CLEAN is updated-but-uncommitted, distinct from the transient DIRTY disk-I/O state. In-memory update is atomic and cannot fail; transient DIRTY and disk errors are eliminated. Assertions in commit enforce the reachable version rules. No uncommitted content is returned.
- StorageOperator.cc:380–390 specifies waiting for the chunk lock but does not specify waiter arbitration in this cut. FIFO by request reception is the chosen arbitration. Waiting preserves the active write until the successor reply and local commit.
- StorageOperator.cc:521–614, :616–640 use buffers, UpdateJob and asynchronous workers; ChunkReplica.cc:38–104 prepares/finishes disk reads with a version recheck. The port uses synchronous in-memory metadata and reads within a node handler. Prepare/finish cannot have a local update between them, so aio_finish_read's assertion enforces the recheck for this atomic replacement. No asynchronous disk interval or additional disk-read failure is invented.
- Content itself is the checksum: StorageOperator.cc:478–492 compares successor checksum to local checksum; the Committed handler asserts their equality. Fixed full chunk shape removes IDs, offsets, lengths, RDMA and data staging.
- ReliableUpdate.cc:43–121 uses channel locking and cached results for duplicates/retries. Clients have one outstanding operation, writes send once, and messages are delivered once, so duplicate/cache outcomes cannot arise. The chunk's lock still serializes competing clients.
- **Exact forwarding loop eliminated:** ReliableForwarding.cc:45–99 (`for (retryCount=0; !stopped_; ++retryCount)` and its timed inner wait loop at :89–97) is replaced by the one send in forward_with_retry followed by a pending active write. Source :55–62 returns immediately on success or no successor. Under the interface there are no failures or timeouts to enter another iteration, no successor reselection, and no back-off.
- **Client write retry eliminated:** StorageClientImpl.cc:1771–1794 invokes the generic retry routine (:1158–1300). The write branch instead sends once and receives exactly one Ack. There is no timed termination condition or retry loop on writes.
- **Selection-only retry eliminated:** boolean target selection has two conditionals, not a `while` loop. It never repeatedly tosses until an index is accepted.
- **No handler spawning or timers:** only run spawns threads. No thread spawns requests, sleeps, polls, retries a forward, or waits for elapsed time.
- **Read retry retained exactly:** StorageClientImpl.cc:1158–1300 plus :1642–1654 selects and sends again after NotCommit; source's time budget is removed by the interface. The latest clarification cuts a repeated target/version answer anywhere within this logical read, including nonadjacent recurrences. Each attempt selects once, sends once, and receives before retrying; a repeated failed answer calls assume(false).
- Omitted operations and facilities: truncate, extend, remove, syncing, persistent chunk store/update worker, buffers, fault injection, tracing and metrics; see StorageOperator.cc:342–359, :521–640; ChunkReplica.cc:206–214 and :260–301; ReliableForwarding.cc:153–253. These are expressly outside the study.
- The exact external enums and successful receives are retained. Invisible headers are an API representation decision because api/lib.rs:889–900 downcasts a receive to one concrete type, and :1254–1285 offers typed receives with sender/tag filters. The mixed-visibility twins at :1295–1459 keep headers/retry attempts outside observable client traces.
- Nodes remain on receive forever, and main joins them as required. Under plain count this means terminal executions are classified as blocked, even when both clients completed. No source role is ended to inflate the complete count.

## Build and runs

Compile command (in this directory):

```
cargo build --release -j 2
```

Required run commands (in this directory):

```
timeout 600 target/release/bench_craq_sol count impl --model fifo -- 1
timeout 600 target/release/bench_craq_sol count impl --model fifo -- 2
```

The corrected build and final run results are recorded below. The function `traceforge::assume` is deprecated in favor of its macro but remains the requested API and performs the cut. A raised per-thread threshold diagnostic is required if the corrected program warns about label size; no threshold is used as a semantic bound.

Previous consecutive-attempt runs timed out at 600 seconds for both q values, each last reporting 5000 blocked and 0 normally finished executions, without a final count. Their raised-threshold diagnostic (`timeout 60 target/release/bench_craq_sol count impl --model fifo --threshold 2000 -- 1`) also timed out with a size-2001 warning, exposing the same alternating-target cycle. These historical results concern the superseded cut, not the corrected program.

### Corrected build and exhaustive runs

Last build output lines:

```
warning: `bench_craq_sol` (bin "bench_craq_sol") generated 1 warning
    Finished `release` profile [optimized] target(s) in 7.11s
```

Both required commands exited 0 and exhausted well within 600 seconds. Neither emitted a per-thread label threshold warning, so no additional raised-threshold diagnostic was needed for the corrected program.

| q | Command | Final count line | Engine wall time | Stop reason |
| --- | --- | --- | --- | --- |
| 1 | `timeout 600 target/release/bench_craq_sol count impl --model fifo -- 1` | `execs=0 blocked=29` | 34 ms | exhausted |
| 2 | `timeout 600 target/release/bench_craq_sol count impl --model fifo -- 2` | `execs=0 blocked=1222` | 674 ms | exhausted |

For q=1, the last progress line and final count were:

```
Executions attempted so far: 20 total 0 finished normally 20 blocked.
execs=0 blocked=29
```

For q=2:

```
Executions attempted so far: 1000 total 0 finished normally 1000 blocked.
execs=0 blocked=1222
```

The final count lines are followed by the engine's measurement block (`stop_reason: exhausted`, `impl_blocked: 29` / `impl_blocked: 1222`). In each run its last output lines were:

```
sweeps: 
cache_tests: 
cache_hits: 
```

Zero normally finished executions is expected: nodes never terminate and main waits for them, as the interface requires. The blocked count includes both unproductive read cuts and executions reaching the final receive blocks after clients finish; it does not distinguish those categories.
