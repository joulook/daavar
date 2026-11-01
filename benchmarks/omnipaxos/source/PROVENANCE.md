# Provenance

- Implementation core: https://github.com/haraldng/omnipaxos (OmniPaxos, Sequence Paxos with Ballot
  Leader Election; Harald Ng et al., KTH), commit 9c772dcce5a9652192f45f67d9d2208b306f3bc9
  (committed 2026-10-24, "Bump omnipaxos_runtime to 0.1.2"; read on 2026-11-01 from a shallow clone).
  License: Apache-2.0 (`LICENSE` at the repository root; `license = "Apache-2.0"` in
  `omnipaxos/Cargo.toml` and `omnipaxos_storage/Cargo.toml`). Two crates of that repository are used
  as Cargo git dependencies pinned to the commit, both with `default-features = false`:
  `omnipaxos` 0.2.4 (the sans-I/O core) and `omnipaxos_storage` 0.2.4 (its `MemoryStorage`; it enables
  `omnipaxos`'s `serde` feature). No file of OmniPaxos is modified or copied into this directory.
- Harness: `rust/src/main.rs` (this directory), written on 2026-11-01 after the shape of
  `benchmarks/raft-rs/rust/src/main.rs` (same clients, values, specification program, arguments and
  `reorder` delivery fault).
- Specification: the specification program of `benchmarks/raft-rs` (a replicated log as a client sees
  it: one totally ordered decided log, deliveries in log order, proposals may be lost), unchanged.
  OmniPaxos's own correctness claim for its decided log is the same (Sequence Paxos: all servers'
  decided logs are prefixes of one sequence; `read_decided_suffix` returns decided entries only).
- Measurements: `../out/runs.log`.
