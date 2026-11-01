# Aneris reliable channel: a FIFO channel over a lossy, duplicating link

**Family:** related-work  **Source:** Aneris (the reliable-communication library of the current
repository),
`related-work-papers/sources/aneris/ml_sources/examples/reliable_communication/client_server_code.ml`
and `examples/messages_in_order/`, Iris spec `aneris/aneris/examples/reliable_communication/spec/api_spec.v`
(CANDIDATES.md N12, record in CATALOGUE-coq-iris.md)  **Expected verdict:** `correct`:
**CONFORMS** (sequence ids and acks turn the lossy, duplicating, reordering link into the FIFO
channel: the receiver gets 1..K in order, once each); `no-seqno`: **DOES NOT REFINE** (a
duplicate is delivered as a new message: the receiver gets `Deliver(1)` twice).

## 1. The protocol

Aneris's reliable channel over UDP: the sending end keeps a send buffer `sbuf` with the lower
bound `sidLB` of its sequence ids and transmits `(sidLB + i, m)` for every buffered message
(`send_from_chan_loop`, every 0.35 s); an acknowledgement `ack` drops the acknowledged prefix
(`prune_sendbuf_at_ack`). The receiving end (`process_data_on_chan`) enqueues a data message only
if its id is the next expected one, `ackId`, drops it otherwise, and always answers
`ack = ackId` (the next expected id). `send c v` enqueues into `sbuf`; `recv c` takes the head of
the receive buffer `rbuf`, waiting while it is empty.

Roles:

- Visible: `sender` (`messages_in_order`'s client half: `send c 1; …; send c K`), `receiver`
  (`recv c` K times; the source asserts `m1 = 1 && m2 = 2 && m3 = 3`).
- Invisible: `ep_s` (the sending endpoint), `ep_r` (the receiving endpoint), on both sides.

The calls `send` and `recv` are local calls into the endpoint in the source (shared queues under a
monitor); here they are request/reply pairs between an app and its endpoint: `Send { v }`
answered by `Queued`, `Recv` answered by `Deliver { v }`. Link messages (invisible):
`Data { seq, v }`, `Ack { ack }`. Values: `sender` sends `v = 1..K`. Parameters: K messages (the
size), L = 1 loss and D = 1 duplication in all (fixed). Smallest interesting size: K = 2.

The connection handshake (`INIT`/`COOKIE`) is dropped: the threads are static, so there is no
connection to set up.

## 2. The specification program

Our p2p channel as a program, under the same model (asyn) as the implementation: `ep_s`
answers each `Send { v }` with `Queued` and forwards `Data { seq, v }` once (`seq` = position);
`ep_r` keeps the data by position and answers each `Recv` with the next value in order, as soon
as both the call and that value have arrived. It is a lossless FIFO channel: `receiver` gets
`Deliver(1), …, Deliver(K)` in this order, and `Deliver(j)` happens after `sender`'s `Send(j)`.
One stub per end (rule 2): the two ends are served independently in the implementation, and one
thread serving both would order `sender`'s calls against `receiver`'s.

## 3. The implementation program

`ep_s` is `send_from_chan_loop` with a window of one: it answers `Send { v }` with `Queued` and
appends `v` to `sbuf`; when nothing is in flight it transmits the head of `sbuf` as
`Data { sidLB, v }`; an `Ack { ack }` with `ack > sidLB` drops `ack - sidLB` messages and lets
the next one go. A transmission may be lost (`nondet()`, at most L = 1 in all): the
retransmission timer then fires and the message is transmitted again (the timer is modelled as
firing exactly when the transmission was lost; spurious timeouts are what the duplication
models). A transmission may be duplicated (`nondet()`, at most D = 1 in all: a second send of the
same packet). `ep_s` ends when K messages were queued and all are acknowledged. `ep_r` is
`process_data_on_chan` plus `recv`: a `Data` whose `seq` equals `ackId` is appended to `rbuf`
and `ackId` incremented, any other is dropped, and every `Data` is answered with `Ack { ackId }`;
each `Recv` is answered with the head of `rbuf` when it is non-empty. `ep_r` ends after K
deliveries. Model: asyn (UDP reorders).

The window of one is a departure from the source, which retransmits the whole buffer every 0.35 s
for ever: with a bounded number of retransmissions, a window of two under asyn can lose a message
for good (the second packet overtakes the first, is dropped as "too fresh", and every
retransmission of it can overtake again), and the receiver would block where the specification
does not. Stop-and-wait keeps exactly one id in flight, so reordering can only bring stale copies,
which `process_data_on_chan` drops.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`correct` conforms.** Visible words: `sender: (S(Send(j)) R(Queued))` for j = 1..K, and
`receiver: (S(Recv) R(Deliver(j)))` for j = 1..K, both sides. Only one id is ever in flight; a
copy of an earlier id is dropped and acknowledged with the current `ackId`, so `rbuf` receives
1..K once each, in order, and the receiver never blocks (every message is transmitted at least
once without loss, because a loss is followed by a retransmission and L = 1). Order: on both
sides `R(Deliver(j))` is after `S(Send(j))` (through `ep_s` and `ep_r`), and each endpoint is one
thread in both programs, so a specification graph whose stubs read their inputs in the order the
implementation's endpoints read the accepted ones has an order contained in the implementation's
(the implementation adds the order of acknowledgements, stale copies, and the window).
**CONFORMS.**

**`no-seqno` does not refine.** `ep_r` ignores the id: every `Data` is enqueued (and counted in
`ackId`). Smallest size K = 2, schedule: `ep_s` transmits `Data(0, 1)` and duplicates it;
`ep_r` reads both copies of it before `Data(1, 2)`, enqueuing 1 twice. Uncovered word:
`receiver: S(Recv) R(Deliver(1)) S(Recv) R(Deliver(1))`; the specification gives the second
`Recv` only `Deliver(2)`. Component: **signature value**. At K = 1 the fault is invisible (the
receiver takes one value, the duplicate stays in `rbuf`). The variant also has blocked graphs
(90 at K = 2): `ep_s` waits for an `Ack` that the miscounted `ackId` never produces; `ep_s` is
invisible, so they add nothing visible.

Reachability was checked with `out/fault-check/` (the same crate with `messages_in_order`'s
assertion in `receiver`: the j-th value is j). Executed results (2026-10-30), one run each, from
`$B/reliable-channel-aneris/out/fault-check` with
`export CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| `no-seqno` fails, K=2 | `cargo run -q -j 2 --offline --release -- impl 2 bag no-seqno` | panic `receiver got [1, 1], expected 1..=2 in order` |
| `no-seqno` fails, K=3 | `cargo run -q -j 2 --offline --release -- impl 3 bag no-seqno` | panic `receiver got [1, 1], expected 1..=3 in order` |
| `correct` holds, K=2 / K=3 | `cargo run -q -j 2 --offline --release -- impl <K> bag correct` | exit 0, `execs=882 blocked=0` / `execs=23800 blocked=0` |
| `spec` holds, K=2 / K=3 | `cargo run -q -j 2 --offline --release -- spec <K> bag` | exit 0, `execs=8 blocked=0` / `execs=48 blocked=0` |

Every claim reproduced; the holding runs explore as many executions as the plain-Must counts in
§7 (`blocked=0` for `correct`: the receiver never blocks). Every command and its output is in
`out/fault-check.log`.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `correct` | `process_data_on_chan` as in the source (`if mid = ackid`) | CONFORMS | §4 |
| `no-seqno` | the id test removed: every `Data` is enqueued and counted (ours, inspired by the catalogue; see below) | DOES NOT REFINE (K ≥ 2) | `receiver` gets `Deliver(1)` twice |

Both against the one `spec`. The source ships no faulty variant. `no-seqno` is a builder-created
mutation distinct from the catalogue's: the catalogue proposes accepting any sequence id **≥** the
expected one, which still rejects stale duplicates and is exposed only by out-of-order fresh
packets; with the window of one (§3) the correct sender never produces such a packet, so that
mutation is not built or exposed here. Removing the check entirely accepts stale duplicates, which
is what the witness shows. The catalogue's other seeded fault (prune the send buffer on any ack) is
not built.

## 6. Files

- `source/`: `client_server_code.ml`, `examples/messages_in_order/messages_in_order_code.ml` and
  `spec/api_spec.v`, copied verbatim, with provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_reliable_channel_aneris`. CLI, the shared main: `<bin>
  count|export|conf|online … --model M -- <K> [variant]`; default K 2, default variant `correct`.
  `--model` left out means `bag`, this benchmark's model, which the crate declares. The
  `out/fault-check` crate is a separate copy, not on the shared main: it keeps the old positional
  CLI `<spec|impl> <size> [fifo|bag|causal] [variant]`, so its commands stand as run. Each thread
  has an inbox channel built by main before any spawn (the endpoints address each other). See
  `_tools/run-rust.sh`.
- `java/`: package `bench.reliable_channel_aneris`, class `ReliableChannel` (args
  `<spec|impl>,<K>,<fifo|bag>,<variant>`), `Support` (the model argument and a thread registry
  that mints every id before any thread starts, as in `ironlock/`). Spawn order: `ep_s, ep_r,
  sender, receiver` on both sides. Visible roles `sender`, `receiver` on both sides and in both
  languages. See `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn) throughout. Size = K (messages), L = 1, D = 1.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | K=2 | bag | 8 / 0 | 8 / 0 | yes |
| spec | K=3 | bag | 48 / 0 | 48 / 0 | yes |
| impl `correct` | K=2 | bag | 882 / 0 | 882 / 0 | yes |
| impl `correct` | K=3 | bag | 23800 / 0 | 23800 / 0 | yes |
| impl `no-seqno` | K=2 | bag | 614 / 90 | 614 / 90 | yes |
| impl `no-seqno` | K=3 | bag | 17248 / 3136 | 17248 / 3136 | yes |

All JMC runs ended with "stack exhausted". No disagreement. K = 4 was not run: K = 3 took more
than a minute on JMC (`out/runs.log`).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`K ∈ {2, 3}`, `v ∈ {correct, no-seqno}`:

```
$B/_tools/run-rust.sh $B/reliable-channel-aneris count spec --model bag -- <K>
$B/_tools/run-rust.sh $B/reliable-channel-aneris count impl --model bag -- <K> <v>
$B/_tools/run-java.sh $B/reliable-channel-aneris bench.reliable_channel_aneris.ReliableChannel:spec,<K>,bag must 100000 600
$B/_tools/run-java.sh $B/reliable-channel-aneris bench.reliable_channel_aneris.ReliableChannel:impl,<K>,bag,<v> must 100000 600
```

Every count command and its output is in `out/runs.log`; every reachability command and its
output is in `out/fault-check.log`.
