# SwiftPaxos, Figures 3 and 4, ballot 0

Source: `java/bench/swiftpaxos_paper_sol/SwiftpaxosPaperSol.java`.
The implementation uses only the supplied message-passing API. There is no
recovery, failure, timeout, retransmission, reconfiguration, or ballot change.
The leader remains n0. The constructor accepts `impl,<q>,<fifo|bag>` with q >= 1;
`fifo` uses P2P and `bag` uses ASYN for every send and receive.

## Roles and state

* Main constructs all seven named MpThreads, wiring their addresses before
  starting any of them. It starts n0, n1, n2, l0, l1, c0, c1 in that order.
  It joins the finite clients first, then the internal processes. It sends and
  receives no messages. Address arrays are initialized before start and never
  changed afterward; protocol state belongs to individual processes.
* c0 and c1 are the only visible roles. Each issues q requests sequentially to
  its own library, exactly following INTERFACE.md's kind and value formulas,
  consumes and checks the response variant, then terminates.
* l0 and l1 implement Figure 3. Each assigns identifiers (client index, local
  sequence number starting at zero), sends Propagate to all three replicas,
  and collects the leader's Reply and the followers' acknowledgements. It returns
  the Reply's result only after n1's matching-path FastAck or SlowAck (the unique
  fast quorum's followers), or a SlowAck from n1 or n2 (a slow quorum's followers).
  Reply and acknowledgements may arrive in either order. Matching is by command
  identifier and ballot. Late messages for completed commands are consumed.
* Every replica owns its register x, initially 0; command table; START,
  PREACCEPT, ACCEPT, COMMIT phases; dependency sets; original proposal paths;
  received FastAck/SlowAck tables; and execution markers. n0 additionally owns
  pending_log. Internal processes always return to blocking receive, including
  after every client has finished.
* On Propagate, n0 and n1 propose all previously propagated conflicting commands,
  excluding the command itself. Two Reads commute; all other pairs conflict.
  Proposals retain executed commands as well as unexecuted commands. n2 records
  the command in PREACCEPT but makes no initial proposal.
* n0 appends each propagated command to pending_log and computes its response
  by replaying that log over a temporary integer initialized from its committed,
  executed register state. This leaves the actual state unchanged. It sends Reply
  before broadcasting FastAck, as in Figure 4.
* A replica processes n0's FastAck only in PREACCEPT and only once every direct
  dependency is ACCEPT or COMMIT (line 23). It then enters ACCEPT and applies
  lines 25–29. These guards are enforced under both delivery models.
* In ACCEPT, matching n1 FastAck dependencies or n1 SlowAck satisfies the fast
  quorum; n1 or n2 SlowAck satisfies a slow quorum. Every direct dependency must
  already be COMMIT before line 32 changes the phase. The leader's vote is
  established by processing its FastAck in line 22, rather than counted again
  in the follower test. This supports spontaneous agreement, double voting by a
  disagreeing fast follower, and the {n0,n2} slow path independently.
* A committed, unexecuted command runs only after every direct dependency has
  executed. Write changes x and has Ack as its result; Read returns x; Rmw adds
  10 and returns the new x. Execution marks the command exactly once. n0 removes
  it from pending_log. Followers never send execution results to libraries.

## Messages

All transmitted values are immutable records implementing MpMessage. Every
record used in a payload has a Tag; collections in transmitted metadata are
immutable copies. Process identities inside messages are Tid values.

| Message | Route | Contents and purpose |
| --- | --- | --- |
| Write / Read / Rmw | ci → li | Exactly the tags and int fields in INTERFACE.md. |
| Ack / Value / Result | li → ci | Exactly the interface reply shapes, derived from n0's Reply. |
| Propagate | li → n0,n1,n2 | Command, its (client,sequence) identifier and original request. |
| Reply | n0 → originating li | Ballot 0, identifier, n0's original dependency paths and optimistic result. Only n0 emits Reply. |
| FastAck | n0 → all replicas; n1 → all replicas and originating li | Ballot 0, sender Tid, identifier, proposed direct dependencies and paths. |
| SlowAck | line 27: replica → all replicas and originating li; line 29: replica → originating li only | Ballot 0, sender Tid and identifier; acknowledges adoption of n0's ordering, without carrying a replacement proposal. |

Broadcasts include the sender, using actual Channel.send and Channel.recv even
for self delivery. No Commit message is introduced: each replica independently
commits using its collected acknowledgements. The client endpoint `q` and
`client(id)` in the paper are the submitting library, not the visible client.

## Decisions and their source sentences

1. **State machine and workload.** §2 says, “The service is defined by a
   deterministic state machine”. The paper leaves the particular service open;
   INTERFACE.md supplies the integer register, conflict relation, client request
   sequence and response variants. These are followed directly. All clients use
   optimistic leader replies; no client is treated as colocated with a replica.
   §3.5 describes the applicable case: “To speed up delivering responses at such
   clients, SwiftPaxos optimistically executes commands at the leader”.

2. **Initial knowledge and startup.** §2 says, “The processes are split into
   replicas, running a distributed service, and clients using it.” The paper does
   not prescribe Java process initialization or address exchange. INTERFACE.md
   grants initial address knowledge and requires main to perform no messaging.
   All MpThreads are constructed before start, allowing API-allocated task IDs
   to be wired before any protocol operation. No setup process is necessary.
   Construction/start order and role names follow the interface. No shared
   mutable protocol state or Java synchronization implements an exchange.

3. **Slow-quorum membership overlaps fast membership.** §3.1 says, “For a
   replica p, we write fast(p, b) and slow(p, b) if p belongs to a fast and a slow
   quorum of b, respectively.” It also says, “Apart from this constraint, slow
   quorums can be any majorities of replicas.” INTERFACE.md fixes FQ =
   {{n0,n1}} and SQ = {{n0,n1},{n0,n2}}. Thus fast is true for n0,n1 and slow
   is literally true for all three replicas, including n0. The excerpt does not
   explicitly say to subtract fast members or the leader from slow membership.
   The implementation follows this literal definition. Consequently the
   `or slow(p,b)` arm of line 25 always applies in this configuration: every
   replica sends line 27's SlowAck after acceptance, even if its original
   dependencies matched. A disagreeing n1 still double votes by first sending
   its own FastAck and then adopting n0's proposal. The matching FastAck path
   can commit without waiting for these SlowAcks. The line 28–29 branch is
   implemented verbatim but is unreachable with these inclusive memberships.
   This is a consequence of the specified quorum membership, not a decision to
   drop the path-only correction rule or classify n1 as slow-only.

4. **Self conflict and pset's root.** Figure 4 says “let D = {id | cmd[id] ▷◁ c}”
   and immediately “let P = pset(id(c))”, before writing dep[id(c)] ← D.
   Taken as literal Java statement ordering this would include the just-stored
   command itself and compute paths from an uninitialized root. §3.3 instead
   specifies “the previously received commands that conflict with c”. We exclude
   the new identifier and compute pset with the freshly proposed D already
   bound as its root. Thus a dependency-free command has exactly {[id]}, and
   every other path is id prepended to a recursively computed dependency path.
   Original path proposals are saved at line 14; paths for later proposals are
   computed from the current dependency graph, including adopted edges.

5. **Full paths and retained history.** §3.5 says, “For a command c, we
   represent the set of c’s dependency paths as a set of ordered lists”. §4.1
   later offers trimming and hashing as optimizations. We use full, exact sets
   of immutable lists and equality, without hashes, truncation or garbage
   collection. This directly implements lines 37–39. §4.1 says, “Standard
   periodic checkpoints can be used to bound memory usage of the protocol”.
   Checkpoint timing is unspecified; none is needed for this finite workload.
   Retaining history preserves all conflict edges for subsequent commands.

6. **Optimistic execution mechanism.** §4.4 says, “For example, the command’s
   response can be computed by reading the local copy while taking into account
   prior uncommitted commands in pending_log, and its side effects can be
   buffered until commit.” We replay pending_log in propagation order on a
   temporary integer, including the new command, and construct that command's
   result. The real register changes only at line 34. Entries are removed only
   at line 36, so committed but unexecuted entries remain in the replay log.
   Two Reads may execute in either order; they do not change the register.

7. **Transport metadata and early messages.** Figure 4 uses “from leader(b)”
   and “from all followers” without defining a wire encoding. Channel.recv
   returns a payload without a sender envelope, so FastAck and SlowAck carry
   the API sender Tid. Reply can only originate at n0, and Propagate identifies
   its library through its command ID. Acknowledgements can arrive before
   Propagate or before their guards become true; they are retained in per-ID
   tables, with START placeholders that make no dependency proposal. SlowAck
   needs no dependency payload because ballot and leader are fixed.

8. **Enabled-handler scheduling.** §4 says, “handlers, each of which executes
   atomically once its preconditions are true”. It does not order simultaneously
   enabled handlers. Propagate is handled when received; acknowledgements are
   collected in receive order. After each message, enabled accept, commit and
   execution handlers are repeatedly considered until none remain. When more
   than one is enabled, Nondet.chooseIndex selects one; a singleton needs no
   choice event. Each handler finishes before another handler is processed,
   including all its sends. The eager local-handler loop precedes the next
   receive. No wall-clock randomness, polling, sleeps or Java scheduling chooses
   a protocol outcome. Quorum checks are existential Boolean tests; multiple
   matching quorum witnesses have identical effects and need no choice.
   Figure 3's “send Propagate(c) to R” and Figure 4's sends to R do not
   prescribe a multicast primitive. A broadcast emits individual API sends in
   replica index order, followed by the originating library when included.
   This is a fixed serialization of the set-valued send, rather than a choice
   of recipient: every listed recipient always receives a message. Interprocess
   delivery ordering remains the harness's choice under the selected model.

9. **Terminal behavior and repeated messages.** Figure 3 says “response(id, r)”
   without prescribing a library lifetime. The interface requires nonterminating
   libraries and replicas. They therefore keep receiving after their useful
   work finishes; libraries consume stale acknowledgements without issuing a
   second response. Phase guards make Propagate/accept/commit processing
   idempotent, and the execution marker implements Exec. Clients terminate
   after q responses. No stop messages or extra protocol exchanges are added.

## Validation

Compiled inside this directory with the requested command (exit status 0):

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

One launcher invocation, inside this directory:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.swiftpaxos_paper_sol.SwiftpaxosPaperSol:impl,1,fifo' must 0 1 report - 600 </dev/null
```

The single invocation exited with status 0 after the checker's 600-second
timeout. It reported 17,450 iterations, all blocked, 0 complete graphs,
17,450 distinct blocked graphs, and `stack not exhausted`. The elapsed checker
time was 600.048 seconds and `stop_reason` was `timeout`. Blocked terminal
graphs are expected because the libraries and replicas never terminate and main
awaits them. The exploration did not finish, so this run does not establish
exhaustive correctness. No specification comparison was requested or run, and
q > 1 and bag delivery were not exercised by this invocation.

The exact last output line was:

```text
max_porf_count: 0
```
