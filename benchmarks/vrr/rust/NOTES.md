# bench_vrr: Viewstamped Replication, normal operation (Rust, TraceForge Must API)

Crate `bench_vrr`, one source file `src/main.rs`, `Cargo.toml` (traceforge by path, `[workspace]`).
Entry: `main_with_model(Program { visible: c0..c{K-1}, spec: entry, imp: entry }, ConsType::Bag)`;
the program arguments are `<N> <K>` (defaults 3 2). All messages are variants of `enum Msg`.

## Mapping from the paper to the code

State (Figure 2), per replica: view-number = constant `VIEW = 0`, status always normal (not stored:
no view change), `op_number`, `log: Vec<Req>`, `commit_number`, `client_table: BTreeMap<client-id,
ClientEntry{s, result}>`; the service state is `counter` (the up-call is `upcall`, `add(op)`).

Messages (`Msg`):
- `Request { from, op }`  = <REQUEST op, c, s>, c = `from`, s = 1 implicit (one request per client).
- `Reply { result }`      = <REPLY v, s, x>, only x kept (INTERFACE.md).
- `Prepare { v, m, n, k }` = <PREPARE v, m, n, k>; `m` is a `Req { client, s, op }`. Sent tagged with n.
- `PrepareOk { v, n, i }` = <PREPAREOK v, n, i>. Untagged.
- `Commit { v, k }`       = <COMMIT v, k>. Sent tagged with 0 (`COMMIT_TAG`).

Client `ci` (`client`): step 1, `send_msg(r0, Request{from: i, op: i+1})`, then `recv_msg_block` for the Reply.

Primary `r0` (`primary`): a loop over `recv_msg_block::<Msg>()` (messages processed in arrival order)
until it has replied to K clients.
- Request -> step 2: compare s with the client table; s not bigger -> drop, re-sending the stored result
  if it is the latest request and executed. Step 3: `op_number += 1`, append to log, table entry
  `{s, None}`, `Prepare{0, m, op_number, commit_number}` to r1..r{N-1}.
- PrepareOk{n, i} -> step 5: record backup i as having prepared 1..n; while the next uncommitted op has
  f PrepareOks from different backups: execute it (ops in op-number order), `commit_number += 1`,
  send `Reply{result}` to its client, table entry `{s, Some(x)}`.
- Step 6: commits are piggybacked on the next Prepare's k; COMMIT is sent as described below.

Backup `ri`, i >= 1 (`backup`): a loop until it has executed K operations.
- Step 4: receives with `recv_tagged_msg_block`, accepting only the Prepare tagged `op_number + 1` or a
  Commit (tag 0); so Prepares are processed in op-number order and a later one waits in the network.
  On Prepare: `op_number += 1`, append, table entry, `PrepareOk{0, op_number, i}` to r0, and the
  Prepare's k is a learned commit.
- On Commit{k}: k is a learned commit.
- Step 7: while `commit_number < known_commit` and the op is in the log: execute it, `commit_number += 1`,
  table entry with the result; no reply to the client.

Main: spawns r0..r{N-1}, then c0..c{K-1} (`thread::Builder::name`), joins them all; sends/receives nothing.

## Decisions where the paper is silent

1. When the primary sends COMMIT. Paper: "if the primary does not receive a new client request in a
   timely way, it instead informs the backups of the latest commit by sending them a <COMMIT v, k>
   message ... (note that in this case commit-number = op-number)". Decision: after a PrepareOk, if
   commit-number = op-number, the backups have not yet been told this commit-number, and fewer than K
   requests have arrived, the timeout is a choice (`nondet()`); if it fires, COMMIT goes to all backups.
   After the K-th reply no request can come any more, so the primary then sends COMMIT unconditionally
   (if the backups were not already told) and ends. Without this last COMMIT the backups could never
   learn of the last commit (no later PREPARE exists) and could not end after K operations.
2. Counting PREPAREOKs. Paper: PREPAREOK "indicate[s] that this operation and all earlier ones have
   prepared locally", and step 5 "waits for f PREPAREOK messages from different backups". Decision: a
   PrepareOk for n counts backup i for every op 1..n; the primary commits op by op in order.
3. Which messages a replica checks the view of: all replica-to-replica messages carry v = 0 and are
   checked against 0 (always equal; "If the sender is behind/ahead" never arises, no state transfer).
4. Request-number: each client has exactly one request, s = 1, not carried in `Request` (fixed shape);
   the primary uses s = 1 for every Request it receives.
5. Executing at the primary: "after it has executed all earlier operations" — the primary executes in
   op-number order inside the commit loop, so earlier ops are always executed first.
6. Ending: the primary ends after K replies (+ the final COMMIT); a backup ends after executing K ops
   (INTERFACE.md); PrepareOks still in flight to an ended primary are left unreceived.
7. Addresses ("every process knows every other process's address from the start"; main sends nothing).
   Each process computes the addresses from its own id and its spawn position: the process at spawn
   position p (replicas 0..N-1, then clients N..N+K-1) has id base + p (`construct_thread_id`).
   `main` asserts after spawning that every handle's id matches this, so a mismatch fails the run
   instead of misrouting messages.
8. Unexpected message kinds at a role are ignored (`_ => {}`); a client panics if it gets anything but
   a Reply (cannot happen).

## Build and run

Compile (in this directory):
    CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-vrr-target cargo build --release -j 2
Result: Finished `release` profile, 0 warnings.

One plain exploration:
    timeout 300 /local/home/mkhoshechin/tmp/cf2-vrr-target/release/bench_vrr count impl --model bag -- 3 2
Result line: `execs=868 blocked=0` (exit 0, stop_reason: exhausted). The last line printed is the
end of the measurement block, `cache_hits: ` (empty value).
