# SwiftPaxos (paper, normal operation): Rust / TraceForge Must API

Source: PAPER-EXCERPT.md (sections 2-4, Figures 3 and 4); interface: INTERFACE.md.
Scope: ballot 0, leader n0 throughout, no recovery (Figure 5 not implemented), no failures.

## Processes

| name | role | code |
|---|---|---|
| `n0`, `n1`, `n2` | replicas; n0 = leader(0) | `replica(k)` (Figure 4) |
| `l0`, `l1` | client libraries | `library(i)` (Figure 3) |
| `c0`, `c1` | clients (visible) | `client(i, q, node)`, verbatim from INTERFACE.md |
| `init` | sends `Setup` to replicas and libraries | closure in `swiftpaxos` |

`main` spawns n0, n1, n2, l0, l1, c0, c1 (all with `thread::Builder::new().name(..)`), then `init`,
joins the clients and `init`, and sends/receives nothing. Replicas and libraries loop forever on
receive.

Quorums (C2): fast quorum {n0, n1} (follower in it: n1); slow quorums {n0, n1}, {n0, n2}
(followers: n1, resp. n2).

## Messages

Untagged (visible): `Req` client -> library, `Rep` library -> client (verbatim from INTERFACE.md).

Tag 1: `Setup { nodes, libs, clients }` from `init`. A process first receives with the Setup tag,
so messages arriving earlier stay in its mailbox (the required buffering).

Tag 2, type `Proto` (invisible):
- `Propagate { cmd }`, library -> each replica (line 2). `cmd = { id: (client, seq), op }`.
- `Reply { b, id, paths, r }`, leader -> library (line 18).
- `FastAck { b, id, deps, paths, from }`, leader -> R (line 19); n1 -> R ∪ {client} (line 21).
- `SlowAck { b, id, from }`, -> R ∪ {client} (line 27) or -> client only (line 29).
`from` is the sender's replica index (the paper's "from leader(b)" / "from all followers in Q").

## Replica (Figure 4)

State: `bal = 0`, `phase` (absent = START, PREACCEPT, ACCEPT, COMMIT), `cmd`, `dep`, `paths`,
`exec` (Exec), `state` (x), `pending_log` (leader), plus buffers of received acks.
- Propagate(c) (lines 6-21): PREACCEPT, store c; if fast (n0, n1): D = known conflicting commands,
  P = pset(id) (lines 37-39), store both. Leader: append c to pending_log, r = opt_exec, send
  Reply(0, id, P, r) to the client's library, then FastAck(0, id, D, P) to R. n1: FastAck(0, id, D, P)
  to R ∪ {client}. n2: nothing else.
- FastAck from the leader (lines 22-29): buffered until pre `id ∈ Preaccept ∧ D ⊆ Accept ∪ Commit`;
  then ACCEPT; if (fast ∧ dep[id] ≠ D) or n2: dep[id] := D, SlowAck to R ∪ {client}; else if fast ∧
  paths[id] ≠ P: SlowAck to client only.
- FastAck from n1 and SlowAcks are recorded on arrival.
- Commit (lines 30-32): id ∈ Accept, dep[id] ⊆ Commit, and (n1's FastAck with D = dep[id], or
  SlowAck from n1) [fast quorum], or SlowAck from n1 or from n2 [slow quorum] -> COMMIT.
- Execution (lines 33-36): a committed, unexecuted id whose deps are all executed is applied to x;
  the leader removes it from pending_log.
After each received message the replica runs all enabled handlers to a fixpoint.

## Client library (Figure 3)

Waits for an untagged `Req` from its client, makes id = (i, seq), sends Propagate to n0, n1, n2,
then receives `Proto` messages for that id until: Reply(0, id, P, r) from the leader and (n1's
FastAck with paths = P, or SlowAck from n1) [line 3], or Reply and SlowAck from n1 or n2 [line 4].
Then response(id, r): `Ack` for a Write, `Value(r)` for a Read, `Result(r)` for an Rmw.
Messages about earlier ids are dropped.

## Decisions (with the sentence that left each open)

1. **slow(p, b) at line 25 means "slow-quorum replica outside the fast quorum" (n2 only).**
   Paper: "if (fast(p, b) ∧ dep[id] ≠ D) ∨ slow(p, b) then" and "For a replica p, we write fast(p, b)
   and slow(p, b) if p belongs to a fast and a slow quorum of b". Literally every replica is in a
   slow quorum under (C2) ({n0,n1}, {n0,n2}), which would make n0 and n1 always send SlowAck and
   make line 28 dead code. Read with "A slow quorum replica acts similarly to a fast quorum replica,
   except that it does not make proposals of its own" and INTERFACE.md ("`n2` is a slow-quorum
   replica"), I take slow = not fast.
2. **A replica's messages to itself are delivered locally**, queued and handled right after the
   current handler (not through the network). Paper: "send FastAck(bal, id(c), D, P) to R" (R
   includes the sender). The prompt asks for messages "between processes"; self-delivery has no
   network delay to model. This removes the interleavings where a replica's own ack is delayed.
3. **Handler scheduling.** Paper: "each of which executes atomically once its preconditions are
   true". When several handlers are enabled, the replica runs them in a fixed order (local
   self-messages first, then buffered leader FastAcks in arrival order, then commits in id order,
   then one execution), to a fixpoint, after each receive. The only nondeterminism is message
   arrival order; no `nondet()` is used, because no choice the paper leaves open changes the
   outcome (see 4).
4. **Execution choice (line 33).** Paper: "when there exists id ∈ Commit \ Exec with dep[id] ⊆ Exec".
   The smallest such id is executed. Two executable commands at once are non-conflicting (Invariant
   2), i.e. two Reads, so the order does not change x.
5. **Dependency paths are full (untrimmed) pset (lines 37-39)**, compared as sets of lists. Paper
   (§4.1): "This issue is solved by trimming the paths ... up to the last committed or accepted
   transitive dependency" and "one may send hashes". Trimming/hashing is described as a practical
   optimization; I keep the definition. pset carries a visited guard against cycles (none expected:
   preaccepted deps point to earlier-received commands, accepted deps to accepted ones).
6. **paths[id] is computed once, at Propagate (line 14), and not recomputed** when dep[id] is
   overwritten at line 26 (the paper never writes paths again); line 28 compares that stored value.
7. **opt_exec(pending_log, state)** = run pending_log in order from the leader's executed state and
   return the result of its last entry (the new command), state unchanged. Paper (§4.4): "the
   command's response can be computed by reading the local copy while taking into account prior
   uncommitted commands in pending_log". pending_log holds all received, not yet executed commands
   (also non-conflicting Reads; they do not change x). A Write's r is 0 and unused.
8. **The leader's own vote** in a fast/slow quorum is its FastAck to itself (line 22 moving the
   command to ACCEPT); line 30's "from all followers in Q" then needs only n1 (fast) or n1 / n2
   (slow).
9. **Line 30's "D = dep[id]"** applies to the follower FastAck case; SlowAcks carry no D.
10. **Ballot fields** are carried (always 0); messages with b ≠ 0 would be ignored (none exist).
11. **Libraries drop stale messages** (acks/replies for an earlier id) and, when idle, receive only
    their client's next request (by tag `None` and sender = their client), so late acks stay in the
    mailbox. Paper (Figure 3) does not say what the client does with messages after response.
12. **Main joins the clients and `init` only**, not the libraries/replicas (which never end); the
    execution then ends with those blocked on receive. INTERFACE.md: "waits for them".
13. **Cargo.toml** has `version = "0.1.0"` added to the given `[package]` fields.
14. Program arguments: `<q> [<fifo|bag>]`; only q (default 1) is read; the model comes from
    `--model`.

## Compile (inside this directory)

    cargo build --release -j 2

Result: `Finished `release` profile [optimized] target(s)`, no errors and no warnings.

## The one run (inside this directory)

    timeout 600 target/release/bench_swiftpaxos_paper count impl --model fifo -- 1

Timed out (exit 124 after 600 s) without printing `execs=`; the last output line was:

    Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (2610.97/sec).

(Every execution is blocked, as expected: replicas and libraries never terminate. No panic or
assertion message appeared.)
