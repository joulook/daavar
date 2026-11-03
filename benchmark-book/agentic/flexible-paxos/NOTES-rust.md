# bench_fpaxos — Flexible Paxos, single decree (Rust, TraceForge Must API)

Source: `src/main.rs`; build file: `Cargo.toml` (crate `bench_fpaxos`, `[workspace]`, traceforge by path).
Entry: `traceforge::program::main_with_model(Program { visible, spec: imp, imp }, ConsType::Bag)`;
`visible` = `c0 … c{K-1}`; `imp`/`spec` = the same program built from the arguments `<A> <K>`
(defaults 4 and 2). All messages are variants of `enum Msg`.

## Parameters
- A acceptors `a0…a{A-1}`, P = K proposers `p0…p{K-1}`, K clients `c0…c{K-1}`, R = 1 (constant `R`).
- Q1 = floor(A/2)+1, Q2 = A − Q1 + 1. For even A (A = 4): Q1 = 3, Q2 = 2 (section 4.1).
- Proposal number `Ballot = (round, proposer index)`, compared lexicographically (round first).

## Main thread
Spawns (with `thread::Builder::name`) acceptors a0…, then proposers p0…, then clients c0…,
in index order, then joins all of them. It sends and receives nothing.

## Mapping, role by role, message by message
Client `ci` (`client`): sends `Propose { from: i, value: i+1 }` to `pi` (`send_msg`), then waits
for one `Decided { value }` or `Fail {}` on its inbox (`recv_msg_block`).

Proposer `pi` (`proposer`), also the learner of its decision:
- receives the client's `Propose` (`recv_tagged_msg_block`, untagged messages only);
- Phase 1 i: picks `p = (round, i)` (first round 1) and sends `Prepare { p, reply_to }` to every acceptor;
- Phase 1 iii (`collect(p, 1, A, Q1)`): receives replies tagged for this attempt and phase; proceeds on
  Q1 `Promise`s; gives up the phase as soon as more than A − Q1 `Nack`s arrived (a Q1 quorum is then
  impossible);
- Phase 2 i: among the promises of the quorum, takes the value of the highest-numbered accepted proposal;
  if none, its client's value; sends `Propose2 { p, v, reply_to }` (the paper's `propose(p,v)`) to every
  acceptor;
- Phase 2 iii (`collect(p, 2, A, Q2)`): on Q2 `Accept`s it learns that v is decided and replies
  `Decided { value: v }` to `c{from}`; gives up the phase after more than A − Q2 `Nack`s;
- after a failed phase: retries from phase 1 with `round = max(highest round seen in a Nack, own
  round) + 1`, at most R times; after R failed retries replies `Fail {}`;
- finally sends `Done {}` to every acceptor (shutdown, D4).

Acceptor `aj` (`acceptor`), state `promised: Option<Ballot>`, `accepted: Option<(Ballot, value)>`:
- Phase 1 ii: `Prepare { p }`: if no promise yet or `p > promised`: `promised = p`, reply
  `Promise { p, last: accepted }`; else reply `Nack { p, promised }`;
- Phase 2 ii: `Propose2 { p, v }`: if no promise yet or `p >= promised`: `promised = p`,
  `accepted = (p, v)`, reply `Accept { p }`; else reply `Nack { p, promised }`;
- terminates after receiving `Done {}` from all P proposers.
Replies are sent with `send_tagged_msg`, tag = `round*2 + phase`; a proposer's rounds strictly increase,
so the tag names one attempt and phase and the proposer receives only replies of its current phase.

## Decisions where the paper is silent
- D1 (client addresses). INTERFACE: "the reply goes to `c{from}`" and "every process knows every other
  process's address from the start", but clients are spawned after proposers and `Propose` carries only an
  index. Each client's inbox is a named TraceForge channel `("fpaxos-client", i)` (`channel::Builder::
  with_name`), which proposer and client both build from the index; the proposer sends `Decided`/`Fail`
  on it, the client receives from it. Everything else uses thread mailboxes. (Risk: if the harness
  observes only thread-mailbox receives on visible threads, this differs; I could not check that
  within the files I was allowed to read.)
- D2 (proposer addresses for acceptors). Acceptors are spawned before proposers, so `Prepare`/`Propose2`
  carry `reply_to` (the proposer's `thread::current_id()`).
- D3 (what happens on refusal). Paper, phase 1 ii: "If p is the highest proposal number promised, then …
  replies with promise(p',v')" — silent on the other case; 1 iii / 2 iii: "Otherwise, it may try again
  with higher proposal number." The acceptor sends a conflict reply `Nack { p, promised }`; the proposer
  gives up a phase exactly when the Nacks make the quorum unreachable (no failures, all messages are
  delivered, so no timeout/choice is needed; the give-up point is fixed by the order of arrival). No
  `nondet` is used. "Highest proposal number promised" is read as strictly greater than the current
  promise for prepare (`p > promised`), and phase 2 as `p >= promised` (the paper's words).
- D4 (termination). The paper's acceptors never stop. Proposers send `Done {}` to all acceptors when
  finished; an acceptor exits after P `Done`s. Unread requests left at that point are no longer awaited.
- D5 (higher proposal number). Paper: "try again with higher proposal number". Next round =
  max(own round, highest promised round reported in Nacks) + 1; first round is 1.
- D6 (which acceptors). Paper: "sends prepare(p) / propose(p,v) to the acceptors" — sent to all A, and the
  first Q1 (resp. Q2) positive replies form the quorum; replies after the quorum stay unread.
- D7 (phase-2 value). "If more than one proposal was returned … the value associated with the highest
  proposal number" — taken over the promises in the Q1 quorum only.
- D8 (odd A). Section 4.1 covers even A; for odd A, Q2 = A − Q1 + 1 keeps Q1 + Q2 > A.
- D9 (naming). The paper's phase-2 `propose(p,v)` is `Msg::Propose2` because `Propose` is the client's.

## Compile and run
Compile (in this directory):
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-fpaxos-target cargo build --release -j 2`
— finished without errors or warnings shown.

The one plain-exploration run:
`timeout 300 /local/home/mkhoshechin/tmp/cf2-fpaxos-target/release/bench_fpaxos count impl --model bag -- 4 2`
— did not finish within 300 s; it printed nothing, exit 124; last output line: `Terminated`
(from `timeout`). So termination of the full exploration at A=4, K=2 was not observed.
