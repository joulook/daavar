# Pineapple Go port

This crate implements the supplied Go program for the fixed one-key interface. It
does not replace the source with a textbook ABD or Multi-Paxos algorithm. Source
line numbers below refer to the supplied files in `code-src/`. Both `Program`
entries return this implementation; only `c0` and `c1` are visible.

## Roles and startup

* The main thread creates `n0`, `n1`, `n2`, then `c0`, `c1`, in that order. It
  creates the invisible `init` thread, then only joins threads. It never sends or
  receives a message. `init` sends each node one `Setup { nodes, clients }` and
  ends. Nodes buffer requests and protocol messages received before Setup.
* Clients execute the exact interface client function and exact `Req` and `Rep`
  enums. Each waits for its reply before issuing the next request, then ends.
  `c0` uses `n0`; `c1` uses `n1`. The supplied `client` field selects the reply
  address, replacing the Go connection writer stored in `Propose`
  (`genericsmr.go:27-30,274-295,339-353`). Interface Read maps to a GET command
  inside Propose, not the unhandled generic READ network opcode
  (`genericsmr.go:297-303`). Write maps to PUT; Rmw maps to the other-operation
  branch of `handlePropose`.
* Every node stores its own payload, presence bit, shared instance space, next
  unused instance index, RMW numbering, pending RMWs and completion counters.
  Only `n0` has IsLeader=true. All nodes handle all eight protocol messages and
  continue receiving forever. No separate shared-memory executor or network
  thread exists. The leader's execution action is modeled inside its process;
  see the Execute event and non-leader decision below.
* Data starts absent, whose Go map lookup yields payload `(tag=(0,0),value=0)`.
  Presence remains distinct from zero data. The first local Read creates
  `(tag=(0,node_id),value=0)` (`pineapple.go:653-660`). Dynamic ordered maps
  replace Go's fixed 20-million-slot arrays (`pineapple.go:119-129`), preserving
  slot occupancy and allocation (`610-615`). There is no eviction.

## Tags, instances and counting

`Tag` preserves Timestamp and ID (`pineappleproto.go:12-20`). The `larger`
function translates `isLargerTag` exactly (`pineapple.go:147-165`): larger
timestamp wins; identical tags do not; at equal timestamps, n0 accepts any
different ID when its current tag ID is 0; otherwise the larger ID wins. This is
a receiver-dependent comparison, not a universal lexicographic ordering.

Instance numbers are local integers and ABD/RMW share the same slots, exactly
as in the source. Incoming RMW messages can occupy slots and replace commands
of existing local instances without replacing their client bookkeeping
(`pineapple.go:422-450,526-559,610-647`). These collisions are deliberately
retained; no coordinator namespace or forwarding is introduced. The original
proposal retained in bookkeeping determines the client and reply type.

All four rounds count the coordinator implicitly: `OKs + 1 > (3 >> 1)`, so one
remote reply is enough. Replies are counted, not deduplicated by sender; the
transport neither duplicates nor loses messages. Late Get/RMWGet replies are
ignored after their respective done flags. Late ABD Set replies still increment
their counter, with the completed flag preventing duplicate client replies.
RMWSet replies use the source's `rmw_id <= done_up_to` guard. All broadcasts go
to both other replicas in cyclic index order, excluding self, matching
`pineapple.go:199-229,327-366,395-419,496-524`; no quorum-only broadcast replaces
them. Prepare/PrepareReply/Commit/CommitShort (`pineappleproto.go:51-63,94-106`)
are unused by the supplied replica and are not emitted. All ballots and the
default ballot remain zero; outdated-ballot checks are retained. Status is
Preparing, Prepared or Accepted; nothing in these handlers sets Committed.
Unused persistence-only fields and counters are omitted.

## Messages and operations

The internal messages correspond to `pineappleproto.go:22-49,65-92`; field names
are Rust-style. The single key is implicit. Each command is one `Req` instead
of a singleton command slice. No transport RPC byte tags are needed because
Rust message types identify the messages.

* **Get** carries coordinator, instance, write flag and payload. A write sends
  the zero payload. A read sends the coordinator's current payload. A receiver
  of a read adopts the payload if absent or if its receiver-specific comparison
  says it is larger, then returns that data. A write receiver returns zero,
  regardless of its stored data (`pineapple.go:205-217,232-257`).
* **GetReply** carries responder, instance, OK, write flag and payload. The
  coordinator records replies, conditionally adopts a larger payload, and
  increments getOKs. At quorum it compares all received tags with the first
  tag; it adds one if that first tag equals or exceeds the instance's initial
  tag using the original comparison direction. Matching current-tag responders
  enter hasMaxTag for reads. If identicalCount equals receivedCount+1, a Read
  completes immediately. Otherwise the instance becomes Prepared and begins
  Set. Write increments the *current* timestamp and uses its own ID, retaining
  the *current* value (`pineapple.go:261-323`). With three nodes only the first
  remote GetReply is processed before getDone.
* **Set** carries coordinator, instance, write flag and payload. Read writeback
  skips hasMaxTag peers; Write sends to both. Receivers adopt only a strictly
  larger tag, then always answer **SetReply**, carrying only the instance
  (`pineapple.go:327-381`). The coordinator preserves the complete source
  predicate `(setOKs+1 > maxCount && maxCount < N>>1 && maxCount > 0) ||
  setOKs+1 > N>>1` (`383-391`), including its effectively redundant first arm
  when N=3. It completes the original proposal once that condition holds.
* **RmwGet** corresponds to RMWGet and carries initiator (Go LeaderId), instance,
  ballot and command. An unused slot is created Accepted, but its reply payload
  is **zero even if storage holds newer data** (`pineapple.go:428-438`). An
  occupied slot keeps its bookkeeping, replaces its command, becomes Accepted,
  and returns current data (`440-449`). **RmwGetReply** carries instance,
  default ballot and payload. At quorum the coordinator selects data exactly
  as in `470-475`: the loop compares each buffered payload but assigns the
  triggering reply's payload. It marks rmwGetDone, increments the timestamp,
  sets the tag ID to its own ID, applies the interface's `value + 10`, and
  broadcasts RmwSet (`456-490`).
* **RmwSet** carries initiator, instance, ballot, command and payload. It creates
  or updates the Accepted slot, records receivedRMW and conditionally installs
  its payload using the same tag comparison. It answers **RmwSetReply** with
  instance, OK=true and default ballot (`pineapple.go:526-565`). The initiator
  ignores OK and ballot, counts replies, and at quorum stores the instance by
  RMW ID and increments doneUpTo by one, exactly as `568-580`; it does not
  compute a new contiguous prefix or repair holes.
* **Execute** is an invisible self-message scheduling one enabled iteration of
  `executeRMWs` (`pineapple.go:584-608`). Each quorum completion queues one
  Execute; receiving it processes the next pending RMW ID and replies once.
  A missing pending slot fails explicitly, corresponding to Go's nil access.
  The client schedule permits only one outstanding locally initiated operation
  per node, so later local RMWs cannot overtake that node's unexecuted RMW.
  The self-message allows other incoming messages to run before the executor
  and samples data at execution time. Ordering is the harness's receive order;
  no extra scheduling policy is imposed. It also prevents an enabled executor
  from being stranded by an otherwise empty mailbox.

## Explicit adaptations and decisions

1. **Reply mode.** Set Dreply=true conceptually: otherwise neither ABD nor RMW
   replies (`pineapple.go:170,591`; constructor parameters at `95-99`;
   `genericsmr.go:54-55,71-86`). There is no execution-mode choice in the
   interface. No separate command-state execution is added; these handlers
   already mutate the replica's data map. Networking, persistence, clocks,
   batching, Alive tests and master RPC are omitted as requested.
2. **Read value.** Go's ABD completion emits `state.NIL`
   (`pineapple.go:168-178`, specifically `174`). Return `Value(data.value)`
   from the coordinating node at that exact completion action, either the
   optimized Get quorum or the Set quorum. It is not a saved earlier snapshot.
   Write returns Ack at the same source completion points.
3. **RMW function and value.** Replace Go's `+1` TODO with `+10`
   (`pineapple.go:482-484`). Execute itself only replies; it does not apply f
   twice. Go returns NIL in `executeRMWs` (`591-598`, specifically `595`);
   return `Result(data.value)` using the executing replica's current payload.
   Other messages can change this value between modification and execution.
4. **Non-leader RMW reply.** Do not forward: `handlePropose` launches RMWGet on
   every replica without checking IsLeader (`636-647`). Only replica 0 starts
   the Go executor (`718-720`), so another initiator never replies despite
   reaching a Set quorum. As explicitly permitted by the prompt, enable the
   same Execute action on that initiating replica too. It replies directly to
   its attached client, using its own data at execution. Replica 0 never
   answers on behalf of replica 1. The leader designation otherwise only
   affects tag comparison in this failure-free subset.
5. **Missing Write assignment is preserved.** The PUT branch only broadcasts
   Get (`pineapple.go:649-651`); write Get replies are empty (`250-253`), and
   the new payload keeps `r.data[key].Value` (`314-317`). Nothing installs the
   client's proposed value. Therefore Write acknowledges and advances tags
   without installing its requested value. This is source behavior, not an
   interface-value ambiguity being repaired. For q=1 the observed Read value
   is necessarily zero; the operation schedule contains one Write and one Read.
6. **Executor placement and choices.** The Go goroutine shares the replica's
   memory; the message-passing port keeps the state in its node and models its
   enabled execution as a self-message. There are no additional visible or
   internal worker roles. Any ordering left open is resolved by message
   reception, including the executor event; no deterministic priority, sleep,
   polling, or random host scheduling decides a protocol branch.
7. **Heterogeneous mailbox API.** The exact client code sends bare Req, whereas
   init sends Setup and nodes send Protocol. `recv_msg_block<T>` and
   `recv_tagged_msg_block<T>` in `api/lib.rs:1235-1287,1574-1609` require the
   incoming runtime type to already be known; they cannot receive these three
   types as a sum (`expect_msg`, `api/lib.rs:890-901`). The supplied Must API's
   `inbox_with_bounds(1, Some(1))` (`api/lib.rs:1634-1637,1670-1700`) is used
   for node receives to receive exactly one arbitrary typed message, blocking
   when none is available. The resulting Val is downcast locally. This is the
   minimal additional Must receive primitive needed to retain the exact bare
   client messages without adding relay processes or false blocking from
   guessing a sender with nondet. Clients use the required recv_msg_block.
   No shared memory or host synchronization transports a protocol message.
8. **Finite representation.** Interface values and timestamps use u64, IDs and
   instance indices usize, ballots i32. As with Go's finite machine integers,
   no unbounded arithmetic is introduced. The specified small executions do
   not approach overflow. Request and reply IDs/timestamps that only correlate
   network clients are unnecessary with one outstanding request per client.
9. **Arguments/model.** An entry accepts positive q and an optional fifo/bag
   label. Delivery semantics are selected by the standard Program runner's
   `--model` option, as in the requested command. Both entries are the same
   implementation and visible returns precisely c0,c1. The optional label is
   validated; it does not override the harness's model selection.

## Validation

Compile command, in this directory:

```sh
cargo build --release -j 2
```

The initial invocation attempted a crates.io index refresh and failed because
index.crates.io could not be resolved. Retrying with
`CARGO_NET_OFFLINE=true cargo build --release -j 2` succeeded using cached
dependencies: `Finished release profile [optimized] target(s) in 1m 07s`.
After dependency resolution, the exact requested `cargo build --release -j 2`
also exited 0: `Finished release profile [optimized] target(s) in 0.05s`.

Exactly one requested run command, in this directory:

```sh
target/release/bench_pineapple_code_sol count impl --model fifo -- 1
```

The run exited 0 and exhausted exploration, reporting
`execs=0 blocked=6906`. These executions end blocked because nodes keep
receiving forever, as required by the interface. The measurement block reported
`stop_reason: exhausted`. This requested q=1 run exercises the ABD paths;
it contains no RMW and is not runtime validation of the RMW paths.

The last output line (including the measurement block) was `cache_hits: `
(an empty value after the colon). The last protocol count line was
`execs=0 blocked=6906`.
