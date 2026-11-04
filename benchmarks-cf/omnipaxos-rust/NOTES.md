# OmniPaxosRust: Java port of OmniPaxos (Sequence Paxos and Ballot Leader Election)

Source: `java/bench/omnipaxos_rust/OmniPaxosRust.java` (one file). It is ported from
`plan/twins-contextfree/omnipaxos-rust/omnipaxos-src` (`sequence_paxos/{mod,leader,follower}.rs`,
`ballot_leader_election.rs`, `messages.rs`, `omni_paxos.rs`, `util.rs`). The port leaves out
reconfiguration, stopsigns, snapshots, compaction, unicache and flexible quorums.

## Processes (INTERFACE.md)

| role | process | body |
|---|---|---|
| main | the program's `run()` | Spawns `n0 n1 n2` then `c0 c1` with `MpProcess.spawn(role, ...)`. It awaits `c0, c1` and then `n0, n1, n2`. It sends and receives nothing. |
| `ci` (i = 0, 1) | client | Sends `Propose(v(i,j) = 1+i+2j)` to `n_i` for j = 0..e-1, in order. Then it receives `Committed` until it has seen each of its own values once, and ends. |
| `n_k` (pid k+1) | OmniPaxos server | Runs one `OmniPaxos` instance (`SequencePaxos` + `BallotLeaderElection`) in an endless loop (below). It never terminates. |

Arguments: `impl,<e>,<t>,<cand>,<fifo|bag>`. Nodes `n0 .. n(cand-1)` get a timeout budget of `t`.
The other nodes get 0. `fifo` maps to `CommunicationModel.P2P` and `bag` to `ASYN`, for every
send and receive. `programName()` = `omnipaxos_rust`. `size()` = `e=<e>,t=<t>,cand=<cand>,<fifo|bag>`.
Visible roles are `c0` and `c1`.

### Node loop (the user loop of `omni_paxos.rs`)
```
take_outgoing_messages; send them                      // the initial BLE HeartbeatRequests (round 1)
loop:
  if budget > 0 && get_current_leader() == None && Nondet.chooseBoolean():
      budget--; election_timeout()                     // = one tick() with election_tick_timeout = 1
  else:
      m = recv(any); handle_incoming(m)                // Propose -> append(v)
  take_outgoing_messages; send them                    // Sequence Paxos messages first, then BLE (as the library)
  deliver newly decided entries to the client
```

## Messages and handlers (Java record -> Rust)

| Java record (tag) | Rust | sent by | handler (Java -> Rust) |
|---|---|---|---|
| `Propose(value)` | (client API) | client | `append` -> `SequencePaxos::append` / `propose_entry` |
| `Committed(value)` | (`read_decided_suffix`) | node | client counts its own values |
| `HeartbeatRequest(from, round)` | `HeartbeatRequest` | `new_hb_round` | `handleRequest` = `handle_request` (replies with ballot, leader, happy) |
| `HeartbeatReply(from, round, ballot, leader, happy)` | `HeartbeatReply` | `handle_request` | `handleReply` = `handle_reply` (kept only for the current round) |
| `PrepareReq(from, n)` | `PrepareReq` | `reconnected` | `handlePrepareReq` = `handle_preparereq` |
| `Prepare(from, n, decidedIdx, nAccepted, acceptedIdx)` | `Prepare` | `handle_leader`, `send_prepare` | `handlePrepare` = `handle_prepare` |
| `Promise(from, n, nAccepted, decidedIdx, acceptedIdx, hasLogSync, suffix, syncIdx)` | `Promise` (with `Option<LogSync>`) | `handle_prepare` | `handlePromisePrepare` / `handlePromiseAccept` = `handle_promise_prepare` / `handle_promise_accept`, chosen by state as in `SequencePaxos::handle` |
| `AcceptSync(from, n, seqNum, decidedIdx, suffix, syncIdx)` | `AcceptSync` | `send_accsync` | `handleAcceptSync` = `handle_acceptsync` |
| `AcceptDecide(from, n, seqNum, decidedIdx, entries)` | `AcceptDecide` | `send_acceptdecide` | `handleAcceptDecide` = `handle_acceptdecide` |
| `Accepted(from, n, acceptedIdx)` | `Accepted` | `handle_acceptsync`, `reply_accepted` | `handleAccepted` = `handle_accepted` (decides when `is_chosen`, then sends `Decide` or updates a pending `AcceptDecide`) |
| `Decide(from, n, seqNum, decidedIdx)` | `Decide` | `send_decide` | `handleDecide` = `handle_decide` |
| `NotAccepted(from, n)` | `NotAccepted` | `check_valid_ballot` | `handleNotAccepted` = `handle_notaccepted` |
| `ProposalForward(from, entries)` | `ProposalForward` | `forward_proposals` | `handleForwardedProposal` = `handle_forwarded_proposal` |

`Ballot(n, priority, pid)` is ordered by `(n, priority, pid)` and `SeqNum(session, counter)` is
ordered lexicographically, as in the library.

### Leader election (BLE), `ballot_leader_election.rs`
- At construction: `current_ballot = leader = (1, 0, pid)`, `happy = true`, and `new_hb_round()`
  (round 1, requests to both peers).
- `hbTimeout` = `hb_timeout`: `update_leader` (max reply ballot), `update_happiness`,
  `check_takeover`, `new_hb_round`, the promise sync (`seq_paxos_promise > leader`), and it returns
  the ballot when `leader == current_ballot`. `electionTimeout` = `OmniPaxos::election_timeout`
  passes that ballot to `handleLeader` = `handle_leader`. If the ballot beats both `n_leader` and
  the promise, the node becomes `(Leader, Prepare)`, inserts its own promise and sends `Prepare` to
  both peers.
- Majority = 2 for both the prepare and the accept quorum.

### Log replication (Sequence Paxos)
- Leader: on a majority of promises it runs `handleMajorityPromises` = `handle_majority_promises`:
  `sync_log` with the max promise's log sync and the max decided index, then it appends the
  buffered proposals, enters `(Leader, Accept)` and sends `AcceptSync` (`send_accsync`, a new
  sequence session) to every promised follower. A late promise in Accept gets its own
  `AcceptSync`. A proposal in Accept goes to `acceptEntriesLeader` (append, then
  `send_acceptdecide`). In Prepare it is buffered.
- Follower: `handle_prepare` promises and attaches a `LogSync` when it is more up to date. It moves
  to `(Follower, Prepare)` and resets `current_seq_num`. `handle_acceptsync` syncs the log, forwards
  buffered proposals to the leader, moves to `(Follower, Accept)` and replies `Accepted`.
  `handle_acceptdecide` and `handle_decide` check the ballot (`check_valid_ballot`: `NotAccepted`
  if the ballot is lower, `reconnected` / `PrepareReq` if it is higher) and the sequence number
  (`handle_sequence_num`: Expected, Outdated (ignored), DroppedPreceding (`reconnected` -> Recover
  + `PrepareReq`)). Then they append and update the decided index (`update_decided_idx_and_get_accepted_idx`).
- Forwarding: `forwardProposals` = `forward_proposals` sends `ProposalForward` to the promised
  leader (`promise.pid`). If there is no leader, or the node is that leader, it buffers the
  proposals.
- Learning: a node learns the entries up to its `decided_idx`. After every step the node sends
  `Committed(log[i])` to its client for each index i from the last delivered index up to
  `decided_idx`, in order. This stands for the application reading `read_decided_suffix`.

## Decisions (where the source is silent or the model differs)
1. **"Has no leader"** = `OmniPaxos::get_current_leader() == None`, i.e. the Sequence Paxos promise is
   `Ballot::default()` (pid 0). A node with budget can time out only then. The choice is
   `Nondet.chooseBoolean()`, made before each receive. One timeout = one `tick()` with the default
   `election_tick_timeout = 1`, i.e. one `election_timeout()`. The `t` units are one tick each.
2. **Other timers are not modelled.** `resend_message_timeout` (100 ticks) and
   `flush_batch_timeout` (200 ticks) never fire: no message is lost, and with `batch_size = 1`
   there is never anything to flush. For the same reason `cached_promise_message` (used only by
   resends) is not kept.
3. **Non-candidate nodes never tick.** In the library every node ticks. Here only nodes with
   budget call `hb_timeout`. As a result, an execution can end with no leader, e.g. when `n0` has
   already seen replies naming `n2`'s higher ballot (`update_leader`) and so does not elect
   itself. Such an execution ends blocked, with the clients waiting.
4. **Outgoing batch = one step.** `take_outgoing_messages` runs after every handled message or
   timeout, and also once at start for the constructor's heartbeats. So the outgoing caches
   (`latest_accept_meta`, `latest_accepted_meta`) coalesce only within one step. Sequence Paxos
   messages are sent before BLE messages, in push order. `Committed` messages are sent after them.
5. **Storage (not in the given sources) is inferred** as an in-memory log with `batch_size = 1`:
   - `sync_log(n, d, sync)` sets the accepted round to n, truncates the log to `sync_idx` and
     appends the suffix, then sets `decided_idx = min(d, log length)` and returns the log length.
   - An append returns the new log length; an empty `AcceptDecide` appends nothing and gives no
     `Accepted`.
   - `get_suffix(i)` = `log[i..]`. `create_log_sync` never uses snapshots.
6. **Delivery never regresses.** `decided_idx` can be set lower by a sync (as in the library).
   The delivered pointer only grows, so each decided index is reported once.
7. **Fields dropped:** `config_id` (always the same, so the check in `handle_reply` always passes),
   `priority` (always 0, kept in `Ballot` as 0), and `prev_replies` (UI only). An optional
   `LogSync` in `Promise` is the flag `hasLogSync` plus `suffix` and `syncIdx`, with no nulls in
   messages. Messages carry the sender pid (`from`), like `PaxosMessage.from`.
8. **Addresses:** main takes its own id `b` (`MpProcess.self()`). Process i in spawn order then
   has id `b+1+i`, so every process knows every address from the start. Main checks this after
   each spawn and throws on a mismatch. No `Tid` appears in a payload.
9. **Termination:** nodes are ordinary named processes (not daemons, which have no role), so the
   interface's names are kept. Main awaits the clients and then the nodes. Every execution
   therefore ends blocked: nodes on `recv`, main on its await. Under `must` this shows as
   `graphs_blocked`, with `graphs_complete` = 0.
10. **No safety assertion is added**; the program only reproduces the protocol.

## Compile (inside `daavar/benchmarks-cf2/omnipaxos-rust/java`)
```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```
Compiled with no errors and no warnings.

## The run
```
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_rust.OmniPaxosRust:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```
Exit 0. The run stopped on the 600 s timeout with the stack not exhausted: 28348 iterations, all
blocked (`graphs: 0 distinct complete, 28348 distinct blocked; stack not exhausted`), no failure,
47.2 iterations/s, `max_graph_events: 90`. The run wrote `report/` in this directory.
Last output line:
```
max_porf_count: 0
```
