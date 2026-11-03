# bench_hermes (Rust, TraceForge Must API)

Hermes-replicated key, paper §3.2 (reads and writes; no RMWs, no faults, no replays), N replicas
`r0..r{N-1}`, K clients `c0..c{K-1}`, one integer key initially 0. Default model Bag.
Source: `src/main.rs`; build file: `Cargo.toml`.

## Mapping paper -> code

Timestamps: `Ts { version, cid }`, derived `Ord` = lexicographic order of footnote 5.
Messages (one enum `Msg`, all sent with `send_msg`, received with `recv_msg_block`):
- client -> replica: `Write { from, value }`, `Read { from }` (INTERFACE + Addendum: `from` = client index);
  replies `WriteAck {}`, `ReadReply { value }` go to the process `c{from}`.
- replica -> replica: `Inv { ts, value }`, `Ack { ts }`, `Val { ts }` (Figure 3: key omitted, one key).

Replica state per key (`struct Replica`): `state` in {Valid, Invalid, Write, Trans}, `ts`, `value`;
coordinator bookkeeping `pending` (client, ts, ACK count); `stalled` requests.

Client (`fn client`): `Write{from:i, value:i+1}` to `r(i mod N)`, wait `WriteAck`; `Read{from:i}` to
`r((i+1) mod N)`, wait `ReadReply`.

Replica, message by message (`Replica::handle`):
- Reads (`read`): reply with the local value iff Valid, else stall.
- Coordinator, client `Write` (`write`): only if Valid (else stall).
  CTS: `ts = (ts.version+1, own id)`; CINV: set ts/value locally, state Write, send `Inv` to all
  other replicas.
- `Ack` (CACK, `try_complete`): counted if its ts equals the pending write's ts; when N-1 ACKs are in:
  Write -> Valid, Trans -> Invalid; CVAL: `Val{ts}` to all followers; then `WriteAck` to the client.
- `Inv` (FINV/FACK): if received ts > local ts: state -> Trans if Write (or Trans), else Invalid; adopt
  ts and value. Always answer `Ack{same ts}` to the coordinator.
- `Val` (FVAL): Valid iff ts equals local ts, else ignored.
Main (`fn hermes`): spawns r0..r{N-1}, then c0..c{K-1} (named threads), joins them; sends nothing.

## Decisions where the paper is silent

1. Replays / Replay state not implemented. "A request that finds a key in the Invalid state for an
   extended period of time (determined via the mlt timer ...) triggers a write replay." Without faults
   every write completes, so no timer and no Replay; no choice operation (`nondet`) is needed: every
   message is processed in arrival order.
2. Trans receiving a higher INV stays Trans. "transitions the key to the Invalid state (Trans state if
   the key was in the Write or the Replay state)" does not name Trans; Trans keeps the record that an
   own write is in flight.
3. On CACK, a state other than Write/Trans is left unchanged. "the write is completed by transitioning
   the key to the Valid state (Invalid state if the key was in Trans state)". A coordinator in Trans can
   become Valid through the VAL of the higher write before its own ACKs are complete; it stays Valid.
4. A coordinator issues a new write only when Valid and with no own write in flight ("issues a write
   to a key only if it is in the Valid state"); the second condition covers decision 3's case.
5. Stalled requests ("the request is stalled" / "the write is stalled"): queued, retried in arrival
   order whenever the key becomes Valid (after CACK or FVAL).
6. Reply to the client after CVAL is sent ("once all ACKs are received, it is safe to respond to a
   client"; the order between VAL and the client reply is not fixed).
7. ACK destination: the INV's coordinator, read from `ts.cid` (no replays, so cid is the sender; the
   receive does not report the sender).
8. Termination (not in the paper): each replica knows how many messages it will receive in a run
   (its client writes W and reads R, one INV and one VAL per write coordinated elsewhere, N-1 ACKs per
   own write) and stops after processing them; it then asserts no pending write and no stalled request.
9. Addresses: "every process knows every address from the start" is realised by predicting the thread
   ids from the spawn order (main 0, r_j = 1+j, c_i = 1+N+i, `construct_thread_id`); main asserts every
   prediction against the id the spawn returns.

## Build and run

    CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-hermes-target cargo build --release -j 2
    $CARGO_TARGET_DIR/release/bench_hermes count impl --model bag -- 3 2

Build: success, no errors. Run (exit 0): result line `execs=4652 blocked=0`, followed by the
measurement block whose last line is `cache_hits: ` (empty value). The run was started twice: the
first invocation's output was piped through `tail` and showed only the block, so it was repeated to
capture the result line; both are the same plain exploration.
