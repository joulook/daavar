# SwiftPaxos ballot-zero Java port

Read `code-src/README.md` first. The implementation is
`java/bench/swiftpaxos_code_sol/SwiftpaxosCodeSol.java`. All source references below
are to the supplied `code-src` tree, with its original line numbers. No recovery
source or files outside this directory were read. The classpath in `cp.txt` is
passed to Java tools as instructed; dependency contents were not inspected.

## Interface and processes

The public `String...` constructor accepts exactly `impl,<q>,<fifo|bag>`, with
q >= 1. `fifo` uses `CommunicationModel.P2P`; `bag` uses `ASYN`. The program
name is `swiftpaxos`, size is the decimal q, and only `c0` and `c1` are visible.
Main creates and starts `n0`, `n1`, `n2`, `l0`, `l1`, `c0`, `c1` in that order,
then joins the processes. It never sends or receives. Main constructs all seven
`MpThread`s and installs their addresses before starting any, so addresses are
available even if a process runs during a later thread's start. Thread creation
records the role with JMC. The only shared program data is this setup address
array, frozen before starts and copied to an immutable list by each process.
Every protocol state variable belongs to one process. All exchanges between
processes use `Channel.send` and `Channel.recv` under the selected model.

The interface replaces the prototype's workload generator, master, networking,
key/value store, and configurable quorum system. There are exactly three
replicas; ballot = 0 and leader = n0 throughout. The fast quorum is exactly
{n0,n1}. A slow quorum requires n0 and either follower. In particular, an n2
fast vote can never count. This resolves `swift/client.go:57-60`'s TODO about
the correct majority, and replaces `swift/swift.go:157-177`'s configurable
quorums. Both followers are eligible slow voters, consistent with
`replica/quorum.go:18-28` (`Majority.Contains` always returns true).

## Clients c0 and c1

For j = 0..q-1, ci sends to li the interface's exact tagged record:
`Write(client,value)` when (i+j)%3 = 0, `Read(client)` when it is 1, or
`Rmw(client)` when it is 2; value = 1+i+2*j. It receives one message, checks
that its type is respectively `Ack`, `Value`, or `Result`, and proceeds.
After q replies it returns. This replaces the generic driver's key generation,
pipelining, timestamps, and statistics (`client/buffer.go:42-65,95` onward),
while retaining its synchronous submit/wait structure
(`client/buffer.go:68-92`). The externally visible record fields, declaration
order, and `@Tag`s are exactly those in `INTERFACE.md`.

## Libraries l0 and l1

A library assigns each request the identifier `(client index, sequence)` with
sequence starting at zero, then sends one `Propagate(id,command)` to each
replica in index order. The interface fixes this zero-based sequence; the Go
send helpers increment their counter before sending
(`client/client.go:198-228`). Propagate represents `GPropose`; no command is
batched. This is the generic client's Fast=true branch
(`client/client.go:175-195`). A client has at most one outstanding command.
The library retains per-id vote state and consumes late messages forever,
including after its client exits.

A library interprets `FastAckClient` as a fast vote with the sender's checksum
(`swift/client.go:132-139`). Only an n0 `Reply` installs the distinguished
leader vote and the returned value (`swift/client.go:243-259`). In its fast
set, a follower vote counts precisely when its checksum equals the Reply's
checksum. Dependency equality is deliberately not required: the Go predicate
computes it for statistics but actually returns checksum equality
(`swift/client.go:80-100`). A pre-Reply follower vote is retained tentatively
and filtered when Reply arrives.

`LightSlowAck` supplies an unconditional slow vote: internally its checksum
is absent. It is attempted in the fast set too, as in
`swift/client.go:204-225`; that absent checksum cannot match a real Reply's
single-key checksum. The slow set's leader vote is the Reply with its checksum
also erased, so either follower's LightSlowAck matches it. Reply alone, or
followers without Reply, never suffice. Acceptance therefore requires Reply
plus a matching n1 fast acknowledgement, or Reply plus a slow acknowledgement
from n1 or n2. On acceptance, the library sends exactly one `Ack`, `Value(v)`,
or `Result(v)` to its client using the leader's returned value, and marks the
id delivered (`swift/client.go:228-259`). It never returns a follower's local
state value. A separate value per id replaces the Go client's single `c.val`;
with one outstanding command this has the same effect and rejects late reuse.
`MAccept` is unused: it belongs to the non-optimistic proxy branch
(`swift/replychan.go:41-58`, `swift/client.go:262-270`).

## Replica options, dependencies, and checksums

Set Exec=true, Dreply=true, optExec=true, fastRead=false, and no slowAddrs.
The source exposes these options without selecting one in the supplied cut
(`swift/swift.go:106-107,129-133,148-153,377-405`);
`swift/replychan.go:41-59` makes optimistic execution the option that supplies
the requested leader Reply and follower FastAckClient protocol. Disabling
fastRead means Read uses the same protocol as the other commands. No speculative
rollback is added: ballot-zero/no-failure operation is the requested scope;
`swift/swift.go:400` explicitly leaves saving old state as a TODO.

Each replica has an integer x initially zero. Write sets x and returns Ack;
Read returns x; Rmw atomically adds ten and returns the new value. Rmw is a
mutator for conflict/dependency purposes, like Write. This replaces the PUT,
GET, SCAN command family and absent-key result in `state/state.go:14-33,75-97,
115-145`; the interface defines the replacement semantics.

The replica uses the actually selected lightKeyInfo algorithm, not the unused
fullKeyInfo (`swift/swift.go:773-781`, `swift/key.go:101-137`). It remembers
lastCmd and lastWrite; a Read takes lastWrite as its dependency, and either
mutator takes lastCmd, then lastCmd is replaced and a mutator replaces
lastWrite. Dependencies are immutable snapshots of those zero-or-one-element
lists and are compared as sets with equal lengths (`swift/defs.go:196-223`).
This retains the prototype's compressed dependency frontier, rather than
expanding it to every earlier conflicting command. Two reads are not direct
dependencies of each other. Rmw updates lastWrite because the interface makes
it conflict with Read. Delivered ids are not removed from these frontiers:
normal operation never calls keyInfo.remove.

The hash computation is retained exactly, including the TODO about commuting
reads (`swift/dpath.go:26`): reads also extend the hash. The checksum is SHA-256
of the previous 32 bytes followed by the client and sequence as two little
endian 32-bit integers (`swift/dpath.go:145-162`), initially 32 zero bytes.
Each process keeps a synced sequence/hash and a list of pending ids. Append
extends the current hash. A leader update removes that id from pending,
advances synced/hash only for a greater sequence, then recomputes the entire
pending suffix (`swift/dpath.go:60-87,100-142`). A list replaces linked nodes
and the node cache; SHA-256 bytes are carried as immutable hexadecimal strings
so messages have value equality. With one key, SHashesEq is just equality of
these single hashes (`swift/dpath.go:165-183`).

A leader FastAck that precedes Propagate is retained for a later log update
(`swift/swift.go:278-283,794-803`). Propagate first records its own checksum
snapshot, then applies that pending update, exactly as the Go event loop does.
The snapshot used in the command's ack is not retroactively changed. This
preserves the behavior around the pipeline TODOs in
`swift/swift.go:280,294,309,336`. The replica-level pending update map subsumes
HashLog.pendingUpd: the log is only updated after this command was appended,
so HashLog's update-before-append branch (`swift/dpath.go:61-64,102-107`) is
unreachable through this normal-operation caller.

## Replica messages and transitions

* `Propagate(id,command)` installs the command after calculating its local
  dependency/checksum. n0 and n1 enter PRE_ACCEPT; n2 saves the proposal but
  does not originate FastAck (`swift/swift.go:355-369`).
* `FastAck(replica,ballot,id,dep,checksum,seqnum)` is broadcast by fast-quorum
  members to the other replicas, with an immediate local handler call.
  n0 assigns increasing sequence numbers starting at zero; n1's unused
  sequence field is zero (`swift/swift.go:381-406`). n1 additionally sends
  `FastAckClient(replica,ballot,id,checksum)` to the submitting library.
  This is the unbatched effect of `swift/batcher.go:52-58,122-142,272-277`.
  n0 sends FastAck, then executes optimistically if its dependencies have
  executed; it sends Reply on execution. The library gets no separate n0
  FastAckClient (`swift/swift.go:392-405`).
* An n0 FastAck received before the command is saved until Propagate; the
  other votes may arrive and be stored first. This replaces the
  afterPropagate deferred call (`swift/swift.go:364,370,417-481,661-663`).
  If the deferred leader handler already selected slowPath, the later
  Propagate does not originate a FastAck (`swift/swift.go:370-375`).
* Processing the leader FastAck enters ACCEPT and inserts its distinguished
  vote in the slow and fast message sets. Both followers send
  `LightSlowAck(replica,0,id)` because both belong to SQ, even if their
  dependencies agree (`swift/swift.go:428-475`). If dependencies differ and
  the command has not already executed, replace them with the leader's.
  Preserve the source ordering: the vote insertion and possible commit
  happen before this replacement (`swift/swift.go:453-463`). Do not silently
  repair this ordering. A non-fast replica starts with an empty descriptor
  dependency, as in the source (`swift/swift.go:363-368,634-637`).
* Each follower sends LightSlowAck to the library and all other replicas,
  then applies it locally unless already delivered. Client-copy-before-peer
  order follows the batcher's direct send order
  (`swift/batcher.go:169-174,230-250`). LightSlowAck means acceptance of the
  leader's dependencies, so replica handlers convert it to a wildcard
  dependency vote (`swift/swift.go:524-530,489-496`). It is entered in both
  slow and fast sets. In particular, n1's slow vote may replace its rejected
  fast vote. The checksum-only client correction at
  `swift/swift.go:438-450` is unreachable here: both followers already satisfy
  sendSlowAck. No additional correction message is fabricated.
* `MSlowAck` is declared with dependencies/checksums (`swift/defs.go:69-75`)
  and registered (`swift/defs.go:322`), but the normal replica loop has no
  receive branch for it (`swift/swift.go:274-342`) and no send constructs it.
  The comment at `swift/swift.go:372` calls a LightSlowAck an MSlowAck.
  Thus the port's actual slow-path wire message is LightSlowAck. It does not
  invent a second, unused SlowAck transport or a new slow-path rule.
* A replica commits when it has the distinguished leader vote and either
  the n1 vote in its fast set or either follower's wildcard slow vote in its
  slow set (`swift/swift.go:499-520,665-684`). A normal fast vote must match
  leader dependencies; a wildcard vote is accepted unconditionally. Replica
  quorum checks do not compare checksums; library quorum checks do.
* `Reply(0,0,id,checksum,value)` is sent only by n0 after executing the
  command, using its saved proposal checksum
  (`swift/replychan.go:49-57,83-97`). Write's internal numeric value is x
  after execution but is discarded by the library when producing Ack.

The Java MsgSet follows `replica/mset.go:45-86`: reject nonmembers; keep a
separate leader message; replace a sender's tentative vote before a leader
is known; filter tentative votes when the leader arrives; reject a duplicate
sender once an accepted vote and leader are present. Each quorum here has
size two, so ready means a leader plus one accepted follower. No quorum is
chosen in advance: message arrival determines which eligible set succeeds.

## Execution, lifetime, and scheduling decisions

START, PRE_ACCEPT, ACCEPT, COMMIT use `swift/defs.go:24-30`. A replica executes
once, only with a proposal and all its current dependencies executed.
Followers require COMMIT; n0 may execute before COMMIT
(`swift/swift.go:533-556`). Execution applies the integer operation locally;
only n0 replies. A committed command waiting on dependencies is retried after
local progress, equivalent to the source successor/deliver notifications
(`swift/swift.go:510-518,558-565,740-743`). No SCC execution or recovery is
invented; a dependency cycle would remain blocked as in this source.

The source explicitly questions delivery without a proposal
(`swift/swift.go:534-541`); this port waits for Propagate and retains the
leader ack until then. The acknowledged FastAck-before-Propose problem and
its source fix (`swift/swift.go:284-299`) are handled by the same per-id
descriptor regardless of which message arrives first.

Go descriptor goroutines, sender, batcher, and reply worker are folded into
one event loop per required role (`swift/swift.go:589-629,702-720`,
`replica/sender.go:32-56`, `swift/replychan.go:34-73`). Each incoming message's
local handler finishes before the next receive; sends remain individual API
events. Broadcast destinations use source index order
(`replica/sender.go:149-158`). The sender excludes itself: the base only has
peer connections (`replica/replica.go:140-164,215-228`), and the Swift handler
already applies its own vote locally (`swift/swift.go:406,477-479`).

When multiple pending commands become executable in one local step, retry
them in descriptor first-arrival order (the LinkedHashMap order), making
repeated passes until none executes. This is an explicit serialization of
the source's asynchronous successor notifications and worker scheduling,
which leave their relative order open (`swift/swift.go:558-565,702-720`).
There is no additional protocol choice of value, dependency, leader, quorum,
or execution order: handlers and local retries use received-message order.
Consequently no explicit Nondet call is needed in this serialization; which
message a receive reads is chosen by JMC's receive operation. No randomness,
wall-clock choice, delay, timer, or Java blocking primitive selects protocol
behavior.

Delivered descriptors remain as tombstones and future votes for them are
ignored (`swift/swift.go:591-593`). In particular, n0's optimistic execution
is terminal locally; it need not subsequently count votes or enter COMMIT
for that delivered id. This avoids retaining the source's worker/pooled
descriptor lifetime interaction (`swift/swift.go:572-580,745-757`,
`swift/replychan.go:68-70`), and does not create a new externally visible
reply. No finite HISTORY_SIZE, pooling, locks, RPC envelope, serialization,
batch coalescing, MOptAcks sentinel, statistics, or shutdown is modeled.
All wire records and their collection fields are immutable values. The
nullable wildcard Vote is private local state, never a sent message.

Libraries and replicas loop forever receiving. The clients terminate. Main
attempts to join every process; a join of a receive-blocked replica blocks
main under JMC's specified await semantics. The harness ends that execution.
No stop messages or daemon roles are added.

## Validation

Compile command, run from this directory:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Compilation succeeded (exit 0).

One launcher invocation, run from this directory (stdout and stderr captured
in `run.log`):

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.swiftpaxos_code_sol.SwiftpaxosCodeSol:impl,1,fifo' must 0 1 report - 600 </dev/null > run.log 2>&1
```

The single invocation exited with status 0 after the requested 600-second
timeout (reported exploration time 600.042 seconds). It explored 36,592
iterations / distinct blocked graphs, zero complete graphs, and reported
`stack not exhausted` and `stop_reason: timeout`. Receive-blocked internal
processes make executions blocked by design; these counts do not establish
that every client finished in every graph. No protocol assertion failure was
reported. The startup log also contains the launcher's logging-configuration
diagnostic `Console contains an invalid element or attribute "level"`.

The run's last output line, verbatim, is:

```text
max_porf_count: 0
```

The full output is in `run.log`. This is bounded protocol exploration without
an external specification, not a refinement proof or exhaustive validation.
The required q=1 workload contains Write and Read; the Rmw branch is compiled
but is not exercised by that invocation.
