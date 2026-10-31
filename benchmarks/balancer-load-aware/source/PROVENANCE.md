# Provenance

No external source program exists; the benchmark derives from two artefacts of this project:

- `LB-VARIANTS.md`: the designers' note `plan/traceForge/LB-VARIANTS.md` (project plan directory, not
  under version control), snapshot of 2026-10-31; md5 below.
- `a4-balancer-main.rs`: `daavar/benchmarks/a4-balancer/rust/src/main.rs` at Daavar commit 0bc6ed9
  (TraceForge pin 37f37fd); its `a4_spec(.., A4Spec::A4b)` is the specification used here and its
  client/request/reply types define the visible interface.

Adaptations made in this benchmark (relative to those two):
1. `b` dispatchers instead of one (L2-stale uses `b = 2` sharing the servers).
2. Servers end on `Stop` (one per dispatcher) and drain their queue, instead of looping forever, so
   every graph is complete.
3. "Serve now or receive" is a non-blocking receive on a server with a non-empty queue: reading
   nothing serves the oldest request (the blocking encoding deadlocked; its runs are kept in
   `out/runs.log`).
4. A server that receives a re-poll while full serves once before answering (bounded progress).
5. `Reject { s, j }` is a visible message to the client, which then moves on.
a4c72352210af3755867e0a73f9f3f1b  LB-VARIANTS.md
7021ac3ad6138c5017c901606f8ddea8  a4-balancer-main.rs
