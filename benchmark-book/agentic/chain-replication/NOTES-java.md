# Chain replication, Java (JMC message-passing API)

Source: `bench/chainrep/ChainRep.java`, class `bench.chainrep.ChainRep` (implements `MpProgram`, constructed
from `impl,<N>,<K>,<fifo|bag>`). `fifo` = `CommunicationModel.P2P`, `bag` = `CommunicationModel.ASYN`.

## Messages

| Message | Direction | Fields (declaration order = encoding order) | @Tag |
|---|---|---|---|
| `Update` | client ci -> head s0 | `from` (= i), `value` (= i+1) | `Update` |
| `UpdateReply` | tail -> ci | `value` | `UpdateReply` |
| `Query` | client ci -> tail | `from` (= i) | `Query` |
| `QueryReply` | tail -> ci | `value` | `QueryReply` |
| `Forward` | s_j -> s_{j+1} | `seq`, `from`, `value` | `Forward` |
| `Ack` | s_{j+1} -> s_j | `seq` | `Ack` |

`from` follows the INTERFACE.md addendum (18:10). Field order in `Update` is `from, value`, as INTERFACE.md writes
`Update { from: int, value: int }` (changed from `value, from` in the first delivery, on the coordinator's correction; recompiled, not re-run).

## Paper step -> code

- Client (Fig. 1, T1 seen from the client; INTERFACE.md workload): `client(i, ...)`: send `Update(i, i+1)` to s0,
  blocking receive of `UpdateReply`; send `Query(i)` to s{N-1}, blocking receive of `QueryReply`.
- "Client Request Arrives at Chain" (T1): the server's `Channel.recv` of an `Update` (head) or `Query` (tail).
- "Update Processing ... processed there atomically using replica of objID at the head": `server`, branch
  `Update`: assign sequence number `seq`, set `value := newVal`, append `seq` to `hist` (Hist^head).
- "then state changes are forwarded along a reliable FIFO link to the next element": head sends
  `Forward(seq, from, value)` to s1 and appends `seq` to `sent` ("Whenever server i forwards an update request r to
  its successor, server i also appends r to Sent_i").
- "(where it is handled and forwarded), and so on until the request is handled by the tail": branch `Forward`
  at s_j (j>0): write `value`, append to `hist`; if not tail, forward the same `Forward` to s_{j+1} and append to
  `sent`.
- "Reply Generation. The reply for every request is generated and sent by the tail" / "Request Processed by
  Tail" (T3, update): tail's `Forward` branch sends `UpdateReply(value)` to c{from}.
- "The tail sends an acknowledgement ack(r) to its predecessor when it completes the processing of update
  request r": tail sends `Ack(seq)` to s_{N-2} right after the `UpdateReply`.
- "upon receipt ack(r), a server i deletes r from Sent_i and forwards ack(r) to its predecessor": branch `Ack`:
  checks `r` is the first element of `sent` (FIFO), removes it, forwards `Ack` to s_{j-1} unless head.
- "Query Processing ... processed there atomically using the replica of objID stored at the tail" (T3, query):
  tail's `Query` branch sends `QueryReply(value)` to c{from}; Hist is unchanged.
- Main: creates s0..s{N-1}, then c0..c{K-1} (each `new MpThread(body, role)`, role = its name), fills the address
  book, starts them in the same order, awaits all. Sends and receives nothing.
- With N = 1 the single server is head and tail: it applies the `Update` and replies directly (no Forward/Ack).

## Decisions where the paper (or the interface) is silent

1. Update semantics. Paper: "can be a value V produced in some nondeterministic pre-programmed way involving the
   current value of objId and/or value newVal". Decision: V = newVal (plain write). So `UpdateReply.value` is the
   value just written by that update. No nondeterminism is used in the update function.
2. Sequence numbers on forwarded updates. Paper speaks of "the sequence number sn of the last update request S+
   has received" only in failure handling. Decision: the head numbers updates 1, 2, ... in its processing order;
   `Forward` and `Ack` carry the number.
3. Order of message processing at a server. Prompt: by arrival order unless a choice is needed. A server does one
   blocking receive whose guard accepts every message kind it still expects; which pending message is taken is
   left to the receive (the strategy). No `Nondet` choice is used: there are no failures, retries or timeouts,
   so nothing is "given up" (T2, ignoring a request, is not modelled; the paper: "this transition is presumably
   not taken too frequently", and INTERFACE.md says no server fails).
4. FIFO links. Paper: "forwarded along a reliable FIFO link". Decision: the guard accepts only the `Forward` with
   the next expected `seq` and only the `Ack` with the next expected `seq`, so the chain's links are FIFO in the
   program even under `bag`; under `fifo` this filter never excludes a deliverable message.
5. Termination. The paper's servers run forever. Decision: each server knows K and stops after K requests (head:
   K `Update`s; others: K `Forward`s) and K closings (tail: K `Query`s; others: K `Ack`s). At the end it asserts
   `Sent` is empty and `|Hist| = K`.
6. Addresses. Paper/INTERFACE: every process knows every address from the start. Decision: main constructs all
   process threads first (this mints their task ids), stores their `Tid`s in two arrays captured by the bodies,
   then starts them; so no body runs before the address book is complete. This uses `MpThread`'s public
   constructor + `start()` + `join1()` (what `MpProcess.spawn(role, body)` / `Handle.await()` do internally)
   instead of `MpProcess.spawn`, because `spawn` starts the process before later Tids exist.
7. The Sent list and acks are kept although no server fails (the prompt asks to keep how the tail acknowledges
   updates); the master, reconfiguration and chain extension are omitted (INTERFACE.md).
8. `size()` = `"N,K"` (e.g. `3,2`), `programName()` = `chain-replication`; visible roles = c0..c{K-1}.

## Compile and run

From this directory (`daavar/benchmarks-cf2/chain-replication/java`):

    javac -d out -cp "$(cat ../../../jmc/cp.txt)" $(find . -name '*.java' -not -path './out/*')

(result: exit 0, no warnings). The one exploration run:

    java -Xss4m -cp out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.chainrep.ChainRep:impl,3,2,fifo' must 0 1 report - 600 </dev/null

Exit 0; `graphs: 18 distinct complete, 0 distinct blocked; stack exhausted`; last output line:

    max_porf_count: 0

(full output in `run.log`).
