# Specification of Omni-Paxos log replication: derivation notes

Source: `plan/twins-contextfree/omnipaxos-derived/PAPER-EXCERPT.md` (sections 3-5, text extracted from the PDF).
Interface: `plan/twins-contextfree/omnipaxos-derived/INTERFACE.md`. Code: `src/main.rs`.

## The specification (one internal thread `log`)

Let P_i = v(i,0) ... v(i,e-1) be the proposals of client c_i, in sending order.

1. `log` receives `Setup` and all 2e `Propose` messages.
2. Fate (nondet, one coin per proposal): each v is either *decided* or *never decided*. K_i = the decided
   subsequence of P_i.
3. Order (nondet, one coin whenever both K_0 and K_1 still have entries): the decided sequence D is an
   interleaving of K_0 and K_1 that keeps each K_i in order.
4. For each entry of D, in order, `log` sends `Committed(v)` to c0 and to c1.

Allowed: any subset of proposals lost (a client whose value is lost blocks forever); any cross-client
interleaving of decided entries; the two clients reading the same D at different speeds.
Forbidden: a value committed that was never proposed; a value committed twice; two clients seeing
different orders (no client ever sees a sequence that is not a prefix of D); a client's later value
decided before its earlier one; a committed value retracted (no message can undo a commit).

## Paper sentences each part follows

- One sequence D, sent to every client in order (step 4); no duplicates, no retraction:
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
5. A proposal is decided at most once and never re-proposed: the client sends each value once and no
   handler re-submits; the spec decides each value at most once.
6. `log` must know `e` to know when every Propose has arrived; `Setup`'s shape is fixed, so `e` is passed
   as `spec_log(e)` through a closure (`named("log", move || spec_log(e))`), the only change to the
   skeleton. `parse` uses the second argument `<e>` (default 1).
7. Reception order inside `log` is invisible. Proposals arriving before `Setup` are buffered and
   attributed to their client by value (v(i,j) = 1 + i + 2j); after `Setup` the rest are received per
   client by sender filter (`recv_tagged_msg_block`). The position of `Setup` among receptions still
   multiplies executions (5x at e = 1) without changing visible behaviour.
8. Per-client order of `Committed` relies on per-channel FIFO (`--model fifo`); under `bag` the delivery
   order of the `log` → client messages is not enforced, the same as for the protocol's node → client
   channel.
9. Entries decided are fixed after all proposals are received; when the decision happens relative to the
   clients' sends is invisible (each client sends all its proposals before its first receive).

## Build and run

    cd daavar/benchmarks-cf2/omnipaxos-derived/rust
    cargo build --release -j 2        # compiled, 0 warnings
    target/release/bench_omnipaxos_derived count spec --model fifo -- correct 1 1 1

Result line: `execs=10 blocked=15` (5 decided sequences [], [1], [2], [1,2], [2,1] times 5 Setup positions;
the 3 sequences with a lost value block). The last output line printed is `cache_hits: ` (the end of
the measurement block).
