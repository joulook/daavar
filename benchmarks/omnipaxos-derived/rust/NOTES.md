# Specification of Omni-Paxos log replication: derivation notes

Source: `plan/twins-contextfree/omnipaxos-derived/PAPER-EXCERPT.md` (sections 3-5, text extracted from the PDF).
Interface: `plan/twins-contextfree/omnipaxos-derived/INTERFACE.md`. Code: `src/main.rs`.

## The specification (one internal thread `log`)

Let P_i = v(i,0) ... v(i,e-1) be the proposals of client c_i, in sending order.

1. `log` receives `Setup`; proposals that arrive before it are buffered (per client, in arrival order) and
   handled right after it as arrivals.
2. Fate (nondet, one coin per proposal, at its arrival): the proposal is *lost* (never decided) or *queued*
   behind its own client's earlier queued values.
3. Steps (nondet at every point, revision 4): receive the next proposal (while any is still to arrive);
   *place* the head of c0's or of c1's queue at the end of the undecided log suffix S; *decide* the head of
   S, which appends it to the one decided sequence D and sends `Committed(v)` to c0 and to c1; *replay*
   S (S <- S ++ S), at most t × cand times per execution (revision 5). A value can be decided right at its
   arrival or after any number of later arrivals; no decision waits for another proposal.

Allowed: any subset of proposals lost (a client whose value is lost blocks forever); any cross-client
interleaving of first occurrences; a value decided twice (and its undecided successors with it, in order)
after a replay; the two clients reading the same D at different speeds.
Forbidden: a value committed that was never proposed; two clients seeing different orders (no client ever
sees a sequence that is not a prefix of D); a client's later value first decided before its earlier one;
a committed value retracted; more than t × cand replays (an instance bound, not a protocol property).
(Before revision 4, "a value committed twice" was also forbidden.)

## Paper sentences each part follows

- One sequence D, sent to every client in order (step 3); no retraction (duplicates: see revision 4):
  "P2 for Sequence Paxos is thus modified to: If a proposal with sequence v is chosen, then every
  higher-numbered proposal that is chosen has v as a prefix." and "the corresponding invariant should
  guarantee that the log remains consistent and chosen entries must not be removed at a later time."
  Also Figure 3b, Prepare handler P3: "log ← prefix(decidedIdx)" (the decided prefix is never cut), and
  the Decide handler "decidedIdx ← decIdx" (a node learns decided entries as a growing index into its log,
  so it learns them in log order). INTERFACE.md fixes that each node delivers every decided entry to its
  client in decided order.
- Fate: a proposal may never be decided (step 2):
  Figure 3b, "Proposal C from client": "return if stopped()"; "If state = (LEADER, PREPARE) then: insert C
  into buffer"; "If state = (LEADER, ACCEPT) then: append C to the log ... send ⟨Accept, currentRnd, C⟩ to all
  promised followers". There is no branch for a follower, so a proposal arriving at a non-leader is not
  logged. Further, Prepare P3: "if maxProm.accRnd ≠ acceptedRnd then log ← prefix(decidedIdx)" and P4
  "append maxProm.sfx to the log": an appended but not yet chosen entry can be discarded by a new leader.
  "No leader": LE1 "Eventually, every quorum-connected server elects some quorum-connected server" only
  promises *some* leader eventually; nothing makes it the node that holds a given proposal, so no
  particular proposal is guaranteed to be decided.
- Order across clients (step 3): the paper fixes no order between proposals from different servers; the
  log order is the order in which successive leaders append ("New client requests are appended to this
  log and replicated to the promised followers in FIFO order in the Accept phase"), and leadership may
  change between any two appends (BLE: "A failed leader will be detected once its heartbeat is not
  received ... the QC servers will increment their ballot and attempt to become the new leader"; the new
  leader adopts the chosen log: "Instead of adopting the chosen value, Sequence Paxos adopts the chosen
  sequence, that is, the log with all chosen entries").
- Per-client order kept (step 3): "New client requests are appended to this log and replicated to the
  promised followers in FIFO order", with "session-based FIFO perfect links"; buffered requests are
  appended in order after the adopted suffix (P4, P5 "append buffer to the log"); P2c "any proposal issued
  must be a sequence that extends the highest-numbered sequence accepted among a majority of servers".
  An entry enters a log only by its own node's append (once) or by copying a suffix of another log
  (AcceptSync: "log ← prefix(syncIdx), append sfx to the log"), and both preserve relative order.

## Check against Sequence Consensus SC1-SC3 (addendum, appendix)

- SC1 "Validity: If a server decides on a log L then L only contains proposed commands." -> D is built
  only from received `Propose` values (step 2 keeps a subset of them); no other value is ever committed.
- SC2 "Uniform Agreement: For any two servers that decided logs L and L' respectively then one is the
  prefix of the other." -> there is one D; every client is sent D's entries in D's order, so what any two
  clients have seen are prefixes of D, one a prefix of the other.
- SC3 "Integrity: If a server decides on a log L and later decides on L' then L is a strict prefix of L'."
  -> a client's node only extends what it delivered: `log` sends each client D entry by entry, never
  retracting or reordering.

SC1-SC3 are safety properties only; they do not say that anything is decided ("whether" stays a nondet
per proposal). They are also weaker than the spec in two places, both kept from Figure 3b rather than from
SC1-SC3:
- no duplicates: SC1-SC3 allow a log with a command twice; the spec decides each value at most once
  because a command enters a log only by one append at the leader ("append C to the log") and by copying
  suffixes, and the client sends each value once;
- per-client order: SC1-SC3 do not order one client's commands; the spec keeps them in sending order
  ("appended to this log and replicated to the promised followers in FIFO order", P4/P5, AcceptSync).
If the specification were to follow SC1-SC3 alone, it would also have to allow duplicated and reordered
own values; I did not do that, because the protocol of Figure 3b cannot show them.

## Revision 3 (coordinator check, 2026-11-04): decisions no longer wait for all proposals

Before: `log` received all 2e proposals, then chose fates and an interleaving. Every `Committed` was then
causally after every proposal of both clients, which the protocol does not require. Missing behaviours
were, for instance: (a) c1's value decided and delivered to both clients before c0's proposal reached
`log`, with c0's value lost; (b) c0's value decided and delivered to both before c1's proposal was received.
Now each proposal is handled at its arrival (lost or queued), and a queued value may be decided at any
later point (steps 2-3 above). Each client's order and the single sequence D are kept.

Paper basis: Figure 3b, "Proposal C from client", "If state = (LEADER, ACCEPT) then: A1. append C to the
log, set accepted[self] ← |log|; A2. send ⟨Accept, currentRnd, C⟩ to all promised followers", and
"⟨Accepted⟩ from follower f": "if logIdx > decidedIdx AND a majority has accepted logIdx then decidedIdx ←
logIdx, send ⟨Decide, currentRnd, decidedIdx⟩ to all promised followers". The only condition for a
decision is a majority of acceptances of the entry's index; no other client's proposal is consulted. The
paper has no sentence saying "a decision does not wait"; this follows from the handlers having no such
condition. The delay between arrival and decision (so that later arrivals may come before a decision)
follows from the Accept/Accepted round trip and from P1 "insert C into buffer" in the Prepare phase.

Count at e = 1 changed from `execs=10 blocked=15` to `execs=16 blocked=22`.

## Revision 4 (owner's ruling, 2026-11-04): replay of the undecided suffix, duplicates allowed

The owner ruled that the specification follows Figure 3b's pseudocode literally, choosing faithfulness to
the pseudocode over the at-most-once behaviour of the library (the spec of revisions 1-3 decided each
value at most once, and the "Check against SC1-SC3" section above argued from the figure that duplicates
cannot occur; that argument missed the case below and is superseded).

Pseudocode sentences (Figure 3b): the leader's own promise is "insert own promise: (acceptedRnd, |log|,
self, decidedIdx, suffix(decidedIdx)) to promises"; on a majority of promises, "P2. maxProm ← the value
with highest accRnd in promises (and highest logIdx if equal)", "P3. if maxProm.accRnd ≠ acceptedRnd then
log ← prefix(decidedIdx)", "P4. append maxProm.sfx to the log". When maxProm is the leader's own promise,
maxProm.accRnd = acceptedRnd, so P3 does not truncate, and P4 appends suffix(decidedIdx) to a log that
already ends with it: the undecided suffix is replayed behind itself. SC1-SC3 do not forbid this (a
duplicate is still a proposed command; the prefix properties hold).

The reviewer's execution (e = 2; c0 proposes 1, 3 and c1 proposes 2, 4): values 3, 2, 4 are lost; [1] is
accepted by a majority in round n but not yet decided; B becomes leader in a higher round with log [1],
decidedIdx 0, and selects its own promise; P4 makes the log [1, 1]; both entries are decided, and both
clients receive Committed(1) twice.

In the spec: a single ordered undecided suffix S (placed values, in log order) replaces the per-client
decision choice of revision 3; per-client queues keep arrived values until they are placed, so the
cross-client order of first occurrences stays free and decisions still do not wait. A *replay* step
appends S to itself. Note the client loop counts any `Committed` of its own values, so after a replay a
client may terminate having seen one of its values twice and another not at all (at e = 2, D = [1, 1]
lets c0 stop with `seen = 2`).

Decision (the paper is silent on a bound): the protocol allows one replay per suitable leader change, so
any number. A finite instance needs a bound, because a client whose own values are all lost would
otherwise receive an unbounded number of replayed values of the other client. I chose `MAX_REPLAYS = 1`
per execution (revision 5 replaces it by t × cand); it admits the reviewer's execution. Replays of S after a
replay (S ++ S ++ S ++ S, …) and replays at several leader changes are not in the instance. This is a
choice of the instance, not of the paper; it should be raised with the owner if a larger bound is wanted.

Count at e = 1 changed from `execs=16 blocked=22` to `execs=160 blocked=54`. Some executions repeat a
visible behaviour (e.g. placing a value earlier or later without a decision in between).

## Revision 5 (coordinator, 2026-11-04): the replay bound is the instance's election budget

The constant `MAX_REPLAYS = 1` of revision 4 is replaced by an instance parameter: at most t × cand
replays per execution, read from the arguments `<variant> <e> <t> [<cand>]` (cand defaults to 3, t and e
to 1). Reason: a replay (P3/P4) happens only at a leader change, and the instance allows at most t election
timeouts on each of the cand candidate nodes, so t × cand bounds the leader changes and hence the
replays. The paper is silent on any bound; this is the instance's election budget, not a protocol
property. `parse` therefore now reads `<t>` and `<cand>` too (INTERFACE.md said only `<e>` is used; changed
on the coordinator's instruction). Each replay doubles the current undecided suffix; the spec does not
check that a given replay corresponds to a distinct leader change beyond this count.

At `correct 1 1 1` the bound is 1 × 1 = 1 replay, as in revision 4; the count is unchanged
(`execs=160 blocked=54`).

## Decisions where the paper (excerpt) was silent

1. The excerpt has no section 4 prose. The appendix definitions SC1-SC3 were added later (addendum) and
   are mapped below; they changed nothing in `src/main.rs`.
2. Proposals at a non-leader: the figure has no follower branch; I read it literally (not logged, not
   forwarded, not retried). Consequence: per-client order is preserved and any subset may be lost.
   If an implementation forwards proposals to the leader, a client's later value could overtake an
   earlier one (different destinations, no FIFO between them); this spec forbids that.
3. Which proposal subsets are reachable: I assume every subset (with N = 3, K = 2 and leadership free to
   move between the clients' nodes and a third node) and every interleaving of the kept values (leaders
   may alternate between any two appends). The paper does not give timing bounds that exclude any.
4. Every node eventually learns every decided entry (reliable links; Prepare goes "to all peers",
   AcceptSync and Decide go to "all promised followers"), so in the spec each client is sent all of D.
   A protocol run in which a client's node never learns a decided entry (client blocked) is not in the spec.
5. (Superseded by revision 4.) A proposal is never re-proposed by a client; it may be decided more than
   once only through the P3/P4 replay.
6. `log` must know `e` (and, since revision 5, the replay bound); `Setup`'s shape is fixed, so they are
   passed as `spec_log(e, max_replays)` through a closure, the only change to the skeleton besides
   `op_spec` taking the pair `(e, t × cand)` that `parse` returns.
7. Proposals arriving before `Setup` are buffered and attributed to their client by value
   (v(i,j) = 1 + i + 2j); they cannot be decided before `Setup` because `log` does not yet know the
   clients (`Setup` is internal, so this costs no visible behaviour). After `Setup`, proposals are
   received with `recv_msg_block` from either client, in any order the model allows. Some executions are
   duplicates in visible behaviour (e.g. a lost proposal received before or after a decision).
   A lost proposal is chosen lost at its arrival; the moment it is dropped is invisible.
8. Per-client order of `Committed` relies on per-channel FIFO (`--model fifo`); under `bag` the delivery
   order of the `log` → client messages is not enforced, the same as for the protocol's node → client
   channel.
9. (Revised in revision 3.) A decision happens at any point after the value's arrival, in particular
   before other proposals have arrived.

## Build and run

    cd daavar/benchmarks-cf2/omnipaxos-derived/rust
    cargo build --release -j 2        # compiled, 0 warnings
    target/release/bench_omnipaxos_derived count spec --model fifo -- correct 1 1 1

Result line: `execs=160 blocked=54` (revision 4; revision 3 gave `execs=16 blocked=22`, revisions 1-2
`execs=10 blocked=15`). The last output line printed is `cache_hits: ` (the end of the measurement block).
