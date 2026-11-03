# chan(K) — a reliable FIFO channel over a lossy, duplicating link (`reliable-channel-aneris`)

**In the paper:** Table 2 (conforming): `chan` = variant `correct`, model bag, size 7. Table 3 (violating): `chan-ns` = variant `no-seqno`, model bag, size 7. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication model:** bag (`asyn`: UDP reorders), the model the crate declares. **Languages:** Rust (TraceForge's
Must API), in `rust/`; Java twin in `java/` (`bench.reliable_channel_aneris.ReliableChannel`); context-free Java
port in `java-cf/` (`ReliableChannelAneris`). **Visible threads:** `sender`, `receiver`; the endpoints `ep_s`
(sending end) and `ep_r` (receiving end) are invisible, on both sides. **Parameters:** `K` = the number of messages
(default 2, smallest interesting size 2; `2 ≤ K ≤ 254`, see below); `L = 1` loss and `D = 1` duplication in all
(fixed); variant `correct` or `no-seqno`. The Rust program stores values, sequence ids and `ackId` in bytes, and
`ep_r`'s `ackId` counts every accepted `Data` (at most `K` in `correct`, `K + 1` in `no-seqno`, which also accepts the
duplicate); the pseudocode below (unbounded integers) and the Java twin (`int` fields) describe the Rust program only
within `K ≤ 254`.
**Source:** Aneris, the reliable-communication library (`examples/reliable_communication/client_server_code.ml`,
driven by `examples/messages_in_order/`), Iris spec `aneris/examples/reliable_communication/spec/api_spec.v`.

Notation: as in `../../synthetic/naive/README.md`. Messages: `Send(v)` and its reply `Queued` (the source's
`send c v`), `Recv` and its reply `Deliver(v)` (`recv c`), and the link messages `Data(seq, v)` and `Ack(a)` (`a` =
the next sequence id expected). Every thread has one inbox and every receive accepts any message addressed to it.
The connection handshake is dropped (the threads are static). The applications are common to both sides:

```
Clients(K) ≜ sender:   for v := 1..K:  send^asyn(ep_s, Send(v));  q := recv^asyn()        -- q = Queued
           ‖ receiver: for 1..K:       send^asyn(ep_r, Recv);     d := recv^asyn()        -- d = Deliver(v)
```

`sender` sends `1 … K` through its endpoint, waiting for each call to return; `receiver` asks its endpoint `K` times
for the next value (`messages_in_order`).

The specification (both pairs):

```
Spec(K) ≜ Clients(K)
        ‖ ep_s: for seq := 0..K−1:
                   Send(v) := recv^asyn()
                   send^asyn(sender, Queued)
                   send^asyn(ep_r, Data(seq, v))
        ‖ ep_r: buf := [⊥]^K;  next := 0;  calls := 0
                while next < K:
                   m := recv^asyn()
                   if m = Data(s, v):  buf[s] := v
                   if m = Recv:        calls := calls + 1
                   while calls > 0 ∧ next < K ∧ buf[next] ≠ ⊥:
                      send^asyn(receiver, Deliver(buf[next]));  next := next + 1;  calls := calls − 1
```

A lossless FIFO channel, one stub per end: `ep_s` answers each call and forwards the value once with its position;
`ep_r` keeps the data by position and answers each `Recv` with the next value in order as soon as both have
arrived. `receiver` gets `Deliver(1) … Deliver(K)` in order, and `Deliver(j)` is after `sender`'s `Send(j)`.

## The pairs

### correct — expected: **conforms**

```
Impl(K) ≜ Clients(K)
        ‖ ep_s: sbuf := [];  lb := 0;  queued := 0;  inflight := false;  losses := L;  dups := D
                while queued < K ∨ sbuf ≠ []:
                   m := recv^asyn()
                   if m = Send(v):  send^asyn(sender, Queued);  sbuf := sbuf · v;  queued := queued + 1
                   if m = Ack(a):   if a > lb:  drop the first min(a − lb, |sbuf|) of sbuf;  lb := a;  inflight := false
                   if ¬inflight ∧ sbuf ≠ []:  xmit(lb, head(sbuf));  inflight := true

          xmit(s, v) ≜ while losses > 0 ∧ nondet({true, false}):  losses := losses − 1    -- lost; the timer fires
                       send^asyn(ep_r, Data(s, v))
                       if dups > 0 ∧ nondet({true, false}):  dups := dups − 1;  send^asyn(ep_r, Data(s, v))

        ‖ ep_r: rbuf := [];  ackId := 0;  calls := 0;  delivered := 0
                while delivered < K:
                   m := recv^asyn()
                   if m = Data(s, v):
                      if s = ackId:  rbuf := rbuf · v;  ackId := ackId + 1
                      send^asyn(ep_s, Ack(ackId))
                   if m = Recv:  calls := calls + 1
                   while calls > 0 ∧ rbuf ≠ [] ∧ delivered < K:
                      send^asyn(receiver, Deliver(head(rbuf)));  pop rbuf;  calls := calls − 1;  delivered := delivered + 1
```

`ep_s` is `send_from_chan_loop` with a window of one: it answers each `Send` with `Queued` and buffers the value;
when nothing is in flight it transmits the head of `sbuf` with id `lb` (`sidLB`); an `Ack(a)` with `a > lb` drops
the acknowledged prefix (`prune_sendbuf_at_ack`) and lets the next value go. A transmission may be lost (no event:
the retransmission timer fires and the packet is sent again, at most `L` times in all) and may be duplicated (a
second send, at most `D` times in all). `ep_s` ends when `K` values were queued and all are acknowledged. `ep_r` is
`process_data_on_chan` plus `recv`: a `Data` whose id is `ackId` is appended to `rbuf`, any other is dropped, and
every `Data` is answered with `Ack(ackId)`; each `Recv` is answered with the head of `rbuf`. `ep_r` ends after `K`
deliveries. (The window of one departs from the source, which retransmits the whole buffer forever: with bounded
retransmissions a window of two can lose a packet for good under bag.)

The specification uses sequence positions (`Data(seq, v)`) to put the data back in order; it has no
acknowledgements, losses or duplicates. With one id in flight, reordering can
only bring stale copies, which `ep_r` drops and acknowledges with the current `ackId`; so `rbuf` receives `1 … K`
once each, in order, and `receiver` never blocks (each value is transmitted at least once without loss). Both sides
have the visible words `sender: (S(Send(j)) R(Queued))` and `receiver: (S(Recv) R(Deliver(j)))` for `j = 1 … K`,
with `R(Deliver(j))` after `S(Send(j))`; the implementation adds only invisible order (acknowledgements, stale
copies, the window).

### no-seqno — expected: **violation** (`K ≥ 2`; at `K = 1` the fault is invisible)

```
Impl(K) ≜ the correct implementation with the data step of ep_r
                   if m = Data(s, v):
                      rbuf := rbuf · v;  ackId := ackId + 1                    ← changed: the test s = ackId removed
                      send^asyn(ep_s, Ack(ackId))
```

`ep_r` ignores the id: every `Data` is enqueued and counted. At `K = 2`: `ep_s` transmits `Data(0, 1)` and
duplicates it, and `ep_r` reads both copies before `Data(1, 2)`, enqueuing `1` twice. The uncovered word is
`receiver: S(Recv) R(Deliver(1)) S(Recv) R(Deliver(1))`; the specification answers the second `Recv` only with
`Deliver(2)` (signature value). At `K = 1` the receiver takes one value and the duplicate stays in `rbuf`. The variant
also has blocked graphs (90 at `K = 2`), where `ep_s` waits for an `Ack` the miscounted `ackId` never produces;
`ep_s` is invisible, so they add nothing visible.

## What it is for

A library abstraction: the specification is the FIFO channel the Iris spec promises, the implementation is the
protocol that builds it from an unreliable link. Losses, duplicates and stale copies multiply the implementation's
graphs (882 at `K = 2`, 23 800 at `K = 3`) against the specification's (8, 48) with no new visible behaviour, so the
cover is many-to-one; the fault (dropping the sequence-number test) is the classic duplicate-delivery bug.
