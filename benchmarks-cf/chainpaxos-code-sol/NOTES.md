# ChainPaxos Java port

The implementation is `java/bench/chainpaxos_code_sol/ChainpaxosCodeSol.java`.
Read `code-src/README.md` first; `INTERFACE.md` supplies the replacements and
external contract. Source references below are paths under `code-src/`, with
original line numbers. No files outside the requested directory were inspected;
the prescribed compiler/launcher use the dependencies named by `cp.txt`.

## Roles and entry

`ChainpaxosCodeSol(String... args)` accepts exactly
`impl,<q>,<mixed|local>,<fifo|bag>`, with positive q. It implements `MpProgram`,
declares program name `chainpaxos`, size `q,variant`, all five roles, and only
`c0,c1` visible. FIFO is JMC `P2P`; bag is `ASYN`. All sends and receives,
including nonblocking receives, use this model.

Main constructs five `MpThread`s with roles `n0,n1,n2,c0,c1` in that order,
obtains their actual API task addresses, then starts them in the same order.
All addresses exist before any process starts. This avoids a setup exchange or
an extra process: main performs no sends or receives. The address list is
initialization data only; process bodies copy it and never mutate it. Every
replica has its own protocol/application state. Main awaits clients first,
then replicas using `join1`, propagating API control flow. The first blocked
replica join can block main permanently, as required by the API. No replica is
a daemon and no replica has a termination path.

Each client sends exactly the interface command for `(i+j)%3`, waits for one
reply of the required type, and advances only after that reply. Write values
are `1+i+2*j`. Client c0 uses n0 and c1 uses n2. There are no extra client
messages, completion notifications, nondeterministic commands, or reply fields.

## Messages and state

All transmitted values are immutable tagged Java records implementing
`MpMessage`; nested payloads are immutable too.

| Message | Fields | Route and meaning |
| --- | --- | --- |
| `Write` | int client, int value | Client to its replica; assign the register. |
| `Read` | int client | Client to its replica; read the register. |
| `Rmw` | int client | Client to its replica; increment by ten and return the new value. |
| `Ack` | none | Origin replica to writing client. |
| `Value` | int value | Origin replica to reading client. |
| `Result` | int value | Origin replica to Rmw client. |
| `Request` | int client, int sequence, message command | Singleton replacement for `OpBatch`/`AppOpBatch`; carried by redirect or ACCEPT. |
| `RedirectRequest` | Request request | n2 front to n0; leader orders the request. |
| `NoOp` | none | Internal instance value; changes no register and produces no client reply. |
| `Accept` | int instance, int counter, message value, int ack | n0 to n1, n1 to n2; accumulated acceptance count and cumulative acknowledged prefix. |
| `AcceptAck` | int instance | n2 directly to n0, acknowledging the entire prefix through that instance. |
| `ReadStarted` | int token | n2 to n0; registers a local read for bounded no-op bookkeeping. |
| `ReadReady` | int token, int value | n2 to n0 after local retirement; closes that read's no-op eligibility. |
| `ReadRelease` | int token, int value | n0 to n2; authorizes forwarding the already computed local read result to c1. |

`Request` and `NoOp` are payloads, not additional standalone exchanges.
For n0's local reads the three bookkeeping steps are local calls, not sends
to another process. Token `2*sequence+client` uniquely names a read; sequences
advance at the connected replica for every client command. With sequential
clients, this supplies the identity formerly supplied by operation/batch IDs
and the front's pending queues (`ChainPaxosMixedFront.java:103–110,161–171`;
`ChainPaxosDelayedFront.java:206–229`). Tokens are internal integers, never raw
process IDs. Actual process addresses use `Tid` only.

Each replica stores instances, acceptance counters, attached reads, the
highest accepted/decided/acknowledged indices (initially -1), and its own integer
register x (initially zero). n0 also stores lastAcceptSent, initially -1.
These initial indices match `ChainPaxosDelayedProto.java:205,213–218,80–83`.

## Request, acceptance, decision, execution

The front and protocol are folded into the replica process. Receiving a
command creates one request immediately; no batching or front timers remain.
For mixed, reads take the same route as writes and Rmw. For local, only Read
uses the read path below; Rmw is a state-changing operation ordered like Write.
Source front submission/forwarding is in `ChainPaxosMixedFront.java:66–81,
103–110,153–155`, and `ChainPaxosDelayedFront.java:92–114,198–200`.
`writesTo` is fixed to n0, so redirect is the front's peer-batch forwarding,
not an invented redirect handler in the source consensus protocol.

n0 directly calls `sendNextAccept` for its own command or a received redirect.
It assigns the next instance and invokes its own acceptance handler with
counter zero and its current acknowledged prefix. This is a local call because
the source explicitly calls `this.uponAcceptMsg`, rather than sending a network
message (`ChainPaxosMixedProto.java:629–668`;
`ChainPaxosDelayedProto.java:648–654,662–692`). No startup election or pending
leadership queue is needed: n0 is already the active quorum leader.

On acceptance, a replica records the value, increments the received count,
advances its accepted index, and **forwards before deciding and before processing
the incoming piggybacked ack**. Thus n0 sends counter 1, n1 sends counter 2,
and n2 observes counter 3. n2 sends `AcceptAck` directly to n0 instead of
circulating an ACCEPT back to n0. The source order and quorum test are
`ChainPaxosMixedProto.java:519–535,549–579` and
`ChainPaxosDelayedProto.java:536–552,566–596`. The forwarded ack is the sender's
previous highestAcknowledged value, not necessarily the incoming ACCEPT's ack.

n1 and n2 decide on receiving quorum count >=2 and execute in increasing
instance order. n0 decides when the tail's cumulative ACK arrives.
`ackInstance(k)` first executes every undecided instance through k, then retires
every newly acknowledged instance through k; older ACKs have no effect.
This preserves `ChainPaxosMixedProto.java:582–617` and
`ChainPaxosDelayedProto.java:599–636`. There is no new broadcast of decisions
or standalone downstream ACK: acknowledged prefixes propagate only in later
ACCEPTs. In particular, n2 may reply to its write/Rmw client before n0 has
executed the same request.

Executing a request updates each replica's own register. Only its originating
replica sends the reply. Write returns Ack, ordered Read returns the current x,
and Rmw updates x then returns Result(x). This corresponds to
`decideAndExecute` (`ChainPaxosMixedProto.java:722–739`;
`ChainPaxosDelayedProto.java:747–764`), front issuer checks
(`ChainPaxosMixedFront.java:161–171`;
`ChainPaxosDelayedFront.java:206–217`), and the application's `local` reply
flag (`app/HashMapApp.java:219–247`). The integer register and Rmw semantics
come from the interface; the original application only defines map reads/writes.

## Local reads and bounded no-ops

A local Read attaches to instance `highestAccepted+1` at the connected replica,
creating an empty placeholder if necessary. It neither redirects nor proposes
itself, and never returns immediately from the current decided state. This is
exactly `ChainPaxosDelayedProto.java:657–659` and
`chainpaxos/utils/InstanceState.java:48–56`.

Only retirement by `ackInstance` makes attached reads ready, after the complete
decision loop. The value is the local x at that point, not a value captured at
attachment or decision. This follows
`ChainPaxosDelayedProto.java:619–635` and
`ChainPaxosDelayedFront.java:220–229`. Application execution and front callbacks
are folded into this handler: there is no separate front thread or extra
scheduling choice between local notification and application execution. The
source leaves those events in separate Babel callbacks; that separation is
the reason this folding is a modeling decision.

The timer replacement uses actual messages rather than a shared counter. n0
learns of remote reads through `ReadStarted`; each registered read supplies at
most one no-op credit. Before blocking, when it has credits, n0 attempts one
nonblocking receive. If it receives input, it handles it first. Otherwise
`Nondet.chooseBoolean()` chooses between consuming one credit and proposing a
no-op, or blocking on receive. The smallest token is consumed; which credit
is charged has no protocol effect. There is never an accepted request waiting
in a separate leader work queue. The API permits a nonblocking receive to
return empty even with messages in transit; such messages have not yet become
pending requests at the leader. A later received event offers another choice.
No endless retry loop follows a negative choice.

ReadReady/ReadRelease form a completion handshake so a remote read cannot finish
at its client while n0 still has an eligible credit for it. The originating
replica computes the value when the source would execute the read; the handshake
delays only transmission of that fixed result. n0 removes the credit before
returning ReadRelease. Thus every emitted no-op is charged to a distinct read
whose client is still waiting. Completed-token tombstones handle ReadReady
overtaking ReadStarted in bag mode. For n0-local reads completion is immediate
and local. This extra handshake and delayed registration restrict no-op timing
and add reply latency; they replace unspecified global bookkeeping for the
interface's bound, not the source's read barrier. The source only has an
unbounded wall-clock timer (`ChainPaxosDelayedProto.java:342–346`) and no global
read count; it resets lastAcceptTime after non-no-op ACKs
(`ChainPaxosDelayedProto.java:613–616`). The interface explicitly replaces that
timer with bounded optional no-ops, so neither that reset nor timer fairness
forces a proposal here. No no-ops are generated in mixed.

An isolated tail read may need an instance for its barrier and then another
instance carrying that barrier's acknowledgment. One read's single no-op may
not suffice. Choosing no no-op can also leave a read blocked. These are retained
finite blocked executions under the interface's optional, bounded timer
replacement; there is no eager read fallback, synthetic ACK, or forced flush.
Replicas keep receiving after clients finish, or while clients are blocked.

## Other explicit reductions and open cases

- The fixed leader/ballot and membership eliminate SeqN fields, prepare/recovery,
  affiliation checks, join/state transfer, failure handling and all membership
  values. `DECIDED` is **not** a normal dissemination step: its only send site is
  a reply to PREPARE (`ChainPaxosMixedProto.java:346–379`;
  `ChainPaxosDelayedProto.java:363–396`). Consequently it is unreachable under
  fixed established leadership, and no Decided message/handler is implemented.
  Source receipt/recovery paths are `ChainPaxosMixedProto.java:462–487` and
  `ChainPaxosDelayedProto.java:479–504`.
- Bag mode exposes an ordering assumption in the TCP source:
  `ChainPaxosMixedProto.java:521–523,723` and
  `ChainPaxosDelayedProto.java:538–540,748` assert contiguous accepts/decisions
  rather than defining gap recovery. This port receives ACCEPTs in the selected
  API model, buffers future instances locally, and runs handlers for the
  contiguous prefix. Reads use the highest *processed contiguous* acceptance.
  This is an explicit completion of the otherwise undefined gap case; it is
  not a switch to FIFO transport. Cumulative tail ACKs can overtake earlier ACKs
  and are safe because the leader has already accepted that entire prefix.
- No retransmissions or duplicate network deliveries are generated. ACCEPTs
  already processed are ignored (matching the fixed-ballot stale/duplicate
  checks in `ChainPaxosMixedProto.java:499–511` and
  `ChainPaxosDelayedProto.java:516–529`). Retired instances are discarded;
  placeholders survive until accepted and acknowledged.
- Source IPC and application callbacks within one replica are ordinary local
  calls. Every exchange between different processes uses Channel.send and
  Channel.recv/tryRecv. Apart from the optional no-op, ordering comes from
  receives; there is no Java randomness, shared protocol state, monitor-based
  communication, timeout, polling thread, or invented termination signal.
- All arithmetic uses the interface's Java int fields; sequence/instance/token
  counters also use int. As with register overflow, bounds approaching integer
  overflow are outside the finite practical model-checking sizes used here.
- The source delayed front refers to MixedProto.PROTOCOL_ID when submitting a
  read (`ChainPaxosDelayedFront.java:140–145`); it is treated as submission to
  the co-located delayed protocol, consistent with its registered handler
  (`ChainPaxosDelayedProto.java:198`) and `onSubmitRead`, rather than modeling
  a missing or separate mixed process.

## Build and the single requested run

From this directory, compilation succeeded (exit 0):

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Exactly one launcher invocation was made, from this directory:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.chainpaxos_code_sol.ChainpaxosCodeSol:impl,1,mixed,fifo' must 0 1 report - 600 </dev/null
```

The command exited 0, explored 4 iterations / 4 distinct blocked graphs and
0 complete graphs, and reported `stop_reason: exhausted`. Blocked classification
is expected because the replicas never terminate. This run exercises only
q=1, mixed, FIFO; it does not establish local/bag correctness. The launcher
also printed a logging-configuration diagnostic about an invalid `level`
attribute; execution continued normally.

The exact last output line was:

```text
max_porf_count: 0
```
