# Ben-Or consensus: Rust (TraceForge Must API)

Crate `bench_ben_or`, one source file `src/main.rs`, build file `Cargo.toml`.
Parameters: N from the program argument (`-- 3`), t = 1 (`T`), R = 2 (`R`), K = N clients.
Default model Bag (`main_with_model(..., ConsType::Bag)`); `visible` = `c0 … c{N-1}`; `spec` = `imp` = the same entry.

## Mapping from the paper to the code

Roles:
- Main thread (`ben_or`): spawns `p0 … p{N-1}` (named via `thread::Builder::name`), then `c0 … c{N-1}`, then joins all.
  It sends and receives nothing.
- Client `ci` (`client`): `send_msg(pi, Input { from: i, value: i mod 2 })`, then `recv_msg_block` for the reply.
- Process `pi` (`process`): receives `Input` (the paper's "Initial value x_p"), then runs the rounds.

Messages (all variants of `enum Msg`):
- `Input { from, value }`, `Decide { value }`, `Undecided {}`: the interface's client messages.
- `Type1 { r, x }`: the paper's (1, r, x_p), sent with tag `2r`.
- `Type2 { r, v: Some(v) }`: (2, r, v, D); `Type2 { r, v: None }`: (2, r, ?); sent with tag `2r+1`.
  Client→process `Input` is untagged; process→client replies are untagged.

Steps:
- step 0, `r := 1`: `for r in 1..=R`.
- step 1, "Send the message (1, r, x_p) to all processes": `send_tagged_msg(q, tag1(r), Type1 { r, x })` for each
  q in `p0 … p{N-1}`, the sender included.
- step 2, "Wait till N − t messages of type (1, r, *) are received": `N − t` calls to
  `recv_tagged_msg_block` filtered on tag `tag1(r)`. Which N − t messages are taken is the arrival order.
  "If more than N/2 messages have the same value v" → `2*count[v] > N` → send `Type2 { r, Some(v) }` to all, else
  `Type2 { r, None }` to all.
- step 3, "Wait till N − t messages of type (2, r, *) arrive": `N − t` calls to `recv_tagged_msg_block` filtered on
  tag `tag2(r)`.
  (a) at least one D-message (2, r, v, D) → `x = v`;
  (b) more than t D-messages → decide v (`decided = Some(v)`, `Decide { value: v }` sent to `c{from}`);
  (c) otherwise → `x = if nondet() { 1 } else { 0 }` (the coin flip is the API's choice operation).
- step 4, `r := r + 1`, go to step 1: the loop.
- After round R: if the process has not decided, it sends `Undecided {}` to its client. Then the process ends.
  Messages it has not consumed (the N − (N − t) extra messages of each round) are never received, i.e. ignored.

## Decisions where the paper (or the interface) left something open

1. **What "Else" in (c) negates.** Paper: "(a) If there is one D-message (2, r, v, D) then set x_p := v. (b) If
   there are more than t D-messages, then decide v. (c) Else set x_p := 1 or 0 each with probability 1/2."
   Decision: (c) applies only when no D-message was received. When D-messages arrive but at most t, x_p := v
   from (a) and there is no coin flip. "One D-message" is read as "at least one".
2. **Whether all D-messages of a round carry the same v.** The paper does not say. They do: a D-message for v
   needs more than N/2 type-1 messages with value v, and each process sends one type-1 message per round, so
   two different values cannot both have a majority. The code takes the value of any D-message received.
3. **What a process does after it decides.** Paper: "Thus after deciding, a process may no longer change its
   decision" and step 4 "go to step 1", with no exit after deciding. Interface: "Decide { value } when the
   process decides" and "A process ends after replying to its client". These conflict.
   Decision: the process sends `Decide` as soon as it decides. It then keeps running the protocol up to the
   end of round R, as the paper's step 4 says, and ends without sending a second reply.
   Tradeoff: the other reading is that the process stops right after deciding. With N = 3, t = 1 that can
   deadlock. Example: inputs 0,1,0. p0 and p2 receive each other's 0 and send D0. Both receive the two D0
   messages, decide in round 1 and stop. p1 then waits forever in round 2 for N − t = 2 type-1 messages and
   gets only its own. Continuing to round R keeps every round's N − t waits satisfiable. This departs from
   the interface's "ends after replying" for a process that decides before round R.
4. **The sender among "all processes".** The paper does not say. A process sends to itself too, so it
   counts its own message among the N − t.
5. **Rounds of a message.** Paper: "The round number r is attached to the messages of round r, so the
   processes can distinguish between messages from different rounds." Each message carries r and is also
   sent with tag 2r (type 1) or 2r+1 (type 2). The receive operations filter on the tag, so a message of a
   later round waits in the mailbox until its round. The client's `Input` is untagged, and the process's
   first receive filters for untagged messages.
6. **Addresses.** Interface: "Assume every process knows every other process's address from the start."
   The main thread must not send messages, so the addresses go through shared memory. Main writes
   `p0 … p{N-1}`'s ThreadIds into a shared vector after spawning the processes and before spawning any
   client. Each client writes its own ThreadId into its slot before it sends `Input`. A process reads both
   only after it has received its `Input`, so every read is causally after the write it depends on.
7. **Reply target.** The reply goes to `c{from}`, using the `from` field of `Input` as the interface says.

## Build and run

Compile (in this directory):

    CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-benor-target cargo build --release -j 2

Result: finished, 0 warnings.

The one run (plain exploration, smallest parameters N = 3):

    timeout 300 /local/home/mkhoshechin/tmp/cf2-benor-target/release/bench_ben_or count impl --model bag -- 3

Last output line: `Terminated`. `timeout` killed the run at 300 s, exit 143. The program printed no
`execs=… blocked=…` line, so the exploration did not finish within 300 s. It was not run again: one run
was allowed.
