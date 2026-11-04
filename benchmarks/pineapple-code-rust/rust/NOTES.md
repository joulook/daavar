# bench_pineapple_code: Pineapple (one key) ported from the Go replica to TraceForge's Must API

Sources: `code-src/pineapple.go` (replica, ABD Get/Set, RMWGet/RMWSet, handlePropose, executeRMWs),
`code-src/pineappleproto.go` (messages, Tag, Payload), `code-src/genericsmr.go` (clientListener,
ProposeChan, ReplyProposeTS), and `INTERFACE.md`. Line numbers below are of those files.

## Processes

| thread | role | Go counterpart |
|---|---|---|
| `n0.replica`, `n1.replica`, `n2.replica` | the replica: state and every handler, one receive per message | `Replica.Run` loop with its handlers (pineapple.go:711) |
| `n0`, `n1`, `n2` | the node: receives its client's `Req`, hands it to its replica as `Propose`, receives the replica's reply and sends it to the client | `clientListener` (genericsmr.go:275, `ProposeChan <-` at :294) and the client's writer used by `ReplyProposeTS` (:347) |
| `c0`, `c1` | clients, verbatim from INTERFACE.md; `ci` sends to `ni` | the client |
| `init` | sends `Setup { nodes, clients }` to the three replica threads | (none; interface) |

`main` spawns the replica threads (index order), then the nodes (index order), then the clients (index order),
then `init`, and joins the clients and `init`. It sends and receives nothing. Nodes and replica threads never
end; every execution ends with them blocked on receive.

Replica 0 is the leader: `is_leader = (id == 0)` (the master's `BeTheLeader`, pineapple.go:788, ignored).

## State per replica (pineapple.go Replica / Instance / LeaderBookkeeping)

- `data: Option<Payload>`: `r.data[key]`; `None` = key absent (`doesExist` false). A read of an absent key gives the
  zero Payload `{Tag{0,0}, 0}`, as Go's map read does.
- `instance_space: instance number -> Instance` (`initial_tag`, `rmw_id`, `received_data` with the sender's
  ReplicaID, `received_rmw_data`, `received_rmw`, `ballot`, `lb`). Instances created by a remote RMWGet/RMWSet have
  `lb = None`, as in Go.
- `crt_instance = 0`, `crt_rmw_id = 0`, `rmw_done_up_to = -1`, `pending_rmws`, `default_ballot = 0`.
- `Lb`: the client's proposal (client index, op, node thread), `has_max_tag`, `get_oks`, `set_oks`, `get_done`,
  `rmw_get_oks`, `rmw_set_oks`, `rmw_get_done`, `completed`.

`isLargerTag(current, received)` (pineapple.go:150) is ported literally: larger timestamp wins; same timestamp: same
ID -> false; else if this replica is the leader and `current.ID == Id` -> true; else `current.ID < received.ID`.

Quorum tests with N = 3, `N >> 1 = 1`: `x + 1 > 1`, i.e. the first reply completes a phase.

## Messages

Client <-> node: `Req`, `Rep` verbatim from INTERFACE.md.
Node -> replica: `RMsg::Propose(req, node_tid)`. Replica -> node: `Fwd(client_tid, rep)`.
Replica <-> replica (`RMsg`, pineappleproto.go; Key and Command dropped, one key and no receiver reads the command):
`Get{replica_id, instance, write, payload}`, `GetReply{replica_id, instance, ok, write, payload}`,
`Set{replica_id, instance, write, payload}`, `SetReply{instance}`, `RmwGet{leader_id, instance, ballot}`,
`RmwGetReply{instance, ballot, payload}`, `RmwSet{leader_id, instance, ballot, payload}`,
`RmwSetReply{instance, ok, ballot}`; and `Setup` from `init`.
Broadcasts go to `(Id+1) % 3, (Id+2) % 3` (all `Alive`).

## Role by role, message by message

Node `ni`: loop { receive a `Req` from any sender other than its replica (only `ci` sends one); send
`Propose(req, ni)` to `ni.replica`; receive `Fwd(c, rep)` from its replica; send `rep` to `c` }.
`n2` has no client and blocks on its first receive.

Replica, on
- `Setup`: (first one) installs the thread ids; messages received before it are buffered and handled in arrival
  order right after it.
- `Propose(req)` (handlePropose, :610): skip used instance numbers (`while instance_space[crt] exists: crt++`);
  - `Write` (PUT): new instance with lb; bcastGet(write = true, empty payload).
  - `Read` (GET): new instance; if the key is absent set `data = {Tag{0, Id}, 0}` and `initial_tag` to that tag,
    else `initial_tag = data.tag`; bcastGet(write = false, payload = data).
  - `Rmw` (any other op): `rmw_id = crt_rmw_id++`, new instance; bcastRMWGet(ballot 0).
- `Get` (handleGet, :233): read: if absent or `isLargerTag(data.tag, get.tag)` store and return the received payload,
  else return own data; write: return the empty payload. `GetReply{ok = true}` to the sender.
- `GetReply` (handleGetReply, :261): ignored once `get_done`; append; if `isLargerTag(data.tag, reply.tag)` store it;
  `get_oks++`; at the quorum: count replies equal to the first reply's tag, mark replicas holding the own tag in
  `has_max_tag` (read), count +1 if `initial_tag == first` or `isLargerTag(initial_tag, first)`; clear; `get_done`.
  Read with `identical == received + 1`: replyClient (no Set phase). Otherwise, write: `data = {Tag{data.ts + 1, Id},
  data.value}`; bcastSet(write, data) skipping (read only) replicas in `has_max_tag`.
- `Set` (handleSet, :368): store if `isLargerTag(data.tag, set.tag)`; `SetReply` to the sender.
- `SetReply` (handleSetReply, :381): `set_oks++`; the literal condition of :387-388 (its first disjunct needs
  `0 < |has_max_tag| < 1` and never holds); at the quorum replyClient.
- `RmwGet` (handleRMWGet, :422): instance absent: create it (lb none) and reply with the empty payload (:438); else
  reply with own data. `RmwGetReply` to the leader_id.
- `RmwGetReply` (handleRMWGetReply, :456): ignored once `rmw_get_done`; append; at the quorum, for each received
  payload `d`: if `isLargerTag(data.tag, d.tag)` store the current reply's payload (:473, as in Go); clear; done;
  `data = {Tag{data.ts + 1, Id}, f(data.value)}`; bcastRMWSet(payload = data).
- `RmwSet` (handleRMWSet, :526): create the instance if absent; store the payload in the instance; store it in data if
  `isLargerTag(data.tag, payload.tag)`; `RmwSetReply{ok}` to the leader_id.
- `RmwSetReply` (handleRMWSetReply, :568): ignored if `rmw_id <= rmw_done_up_to`; `rmw_set_oks++`; at the quorum
  `pending_rmws[rmw_id] = instance; rmw_done_up_to++`, then executeRMWs: reply to every pending, not yet completed
  RMW.
- replyClient (:168) / executeRMWs (:584): once per instance (`completed`), send `Fwd(client tid, rep)` to the
  node thread that proposed it; that node sends the reply to its client.

## Decisions

1. **Reply values.** Go replies `Value: state.NIL` for every operation (replyClient :174, executeRMWs :595). The
   port replies `Ack` for a Write, `Value(data.value)` for a Read and `Result(data.value)` for an Rmw, where
   `data` is the replica's data at the point where the code completes the operation (the replyClient call in
   handleGetReply :305-306 or handleSetReply :387-389; the quorum in handleRMWSetReply :577-579).
2. **RMW modification.** Go: `newValue := r.data[key].Value + 1 // TODO: update RMW modify` (:483). The port uses
   the interface's `f(v) = v + 10` at that point.
3. **RMWs on replicas 1 and 2 are never answered by the code.** `executeRMWs` runs only on replica 0
   (`if r.Id == 0 { go r.executeRMWs() }`, :718-719), and handlePropose (:610-667) runs the RMW rounds on whichever
   replica received the Propose, with no forwarding to the leader. The port runs the RMW rounds on the receiving
   replica, as the code does, and answers the client on every replica at the quorum of handleRMWSetReply (:577-579),
   with the value of decision 1.
4. **executeRMWs is run at once.** In Go it is a separate polling goroutine (sleeping `CLOCK`); the port calls it
   right after `rmw_done_up_to++` in the same handler step. The reply then travels asynchronously through the node
   thread.
5. **A Write does not store the client's value.** handleGetReply builds the new payload as
   `Payload{Tag: newTag, Value: r.data[key].Value}` (:317); the command's value is never read on the ABD path. The
   port keeps this: a Write bumps the tag and keeps the replica's current value (the `value` of `Req::Write` is
   unused).
6. **Dreply = true**, `Alive` all true, Durable false: replyClient/executeRMWs test `r.Dreply` (:170, :591); without
   it no client is ever answered. Persistence (recordInstanceMetadata, recordCommands, sync), the clock and the
   `onOffProposeChan` batching (:703, :761), beacons, failure handling and the master RPC are left out.
7. **Ballots** are always 0 (handlePropose passes 0, defaultBallot stays 0); the `outdated ballot` panics are kept and
   never fire. `cmds` and `status` are not modelled: no path that decides a message or a reply reads them.
8. **Two threads per node.** The Must API receives one message type per receive, and a client's `Req` (verbatim
   type) and the replica messages are different types; Go's Run loop selects over ProposeChan and the peer
   channels at once. As in genericsmr.go, the client connection is served by its own listener (`ni`), which hands
   requests to the replica loop (`ni.replica`) as `Propose`; then every receive of `ni` is of a known type (a `Req`
   from anyone but its replica, then `Fwd` from its replica: its client waits for the reply in between) and every
   receive of `ni.replica` is an `RMsg`. No `nondet()` is needed: every choice the protocol leaves open is the
   order of message arrival. Consequences: `Setup.nodes` holds the replica threads' ids (the replicas talk to one
   another and are the ones that need `Setup`), the `ni` thread learns its replica's id at spawn, and `main` spawns
   the replica threads before the nodes.
9. **Instance numbers are replica-local** but are carried in RMWGet/RMWSet and used as indices on the receiver, as in
   Go (handleRMWGet :423, handleRMWSet :527); a remote RMWGet/RMWSet can create an instance the receiver's
   handlePropose later skips, or hit the receiver's own instance (then the "reordered" branch, which returns its data).
10. **main** joins the clients and `init` only; joining a node would never return.
11. `Program`: `visible` = `c0`, `c1`; `spec` and `imp` both build this implementation; the program argument is `q`
    (default 1); the model comes from `--model`.

## Build and run

Compile (inside this directory): `cargo build --release -j 2` -> `Finished release profile [optimized]`, no warnings.

Run once (inside this directory): `target/release/bench_pineapple_code count impl --model fifo -- 1`
-> result line `execs=0 blocked=6452` (every execution ends with the nodes blocked on receive), followed by the
measurement block whose last line is `cache_hits: ` (`stop_reason: exhausted`).
