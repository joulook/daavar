# Provenance

- Implementation core: https://github.com/tikv/raft-rs, commit ad13f3d90780f53aea2488c6a4b76c0d334bf136
  (2026-05-13), used as a Cargo git dependency with `default-features = false, features = ["protobuf-codec"]`;
  no file of raft-rs is modified or copied. Harness: `rust/src/main.rs` (this directory), written in the
  feasibility spike of 2026-10-31 (`plan/benchmarks/REAL-SOFTWARE-SPIKE.md`), adapted on 2026-11-01 with the
  `<variant>` argument and the `reorder` delivery fault.
- Specification: Raft's TLA+ specification by Diego Ongaro (`raft.tla`), copy at
  `plan/benchmarks/related-work-papers/sources/mocket-raft-tla/raft.tla` (from the Mocket artifact), abstracted
  to its client-visible content (one committed log, deliveries in log order, proposals may be lost).
- Spike measurements: `spike-runs.log` (copied from the spike's `out/runs.log`): in-process stateful
  `conf --model fifo -- 1 1 1` conforms (7m48s, one thread); `conf-join` of the two exports at that size
  conforms, 0 uncovered; `1 1 2` not finished in 10 min (2,000,000 blocked executions).
