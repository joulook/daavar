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

## Java side (MicroRaft)

# Provenance

## Implementation core (copied, unmodified)

- Upstream: MicroRaft, https://github.com/MicroRaft/MicroRaft, commit
  `929dae98dbc28ffa38b8e316bb93f91153eba70e` (2026-10-18, "fix: reject leader-only reads during
  self-removal"), shallow clone of 2026-11-01.
- Copied: the core module's sources, `microraft/src/main/java/io/microraft/` (116 files, 16,663
  lines), to `java/vendor/io/microraft/`, byte for byte (`diff -r` empty at copy time; the md5 of
  the sorted per-file md5 list is `056eed049567e2b305e991eb102723e3` for both trees). No file is
  edited. The other modules (`microraft-hocon`, `-yaml`, `-metrics`, `-store-sqlite`,
  `-tutorial`) are not used.
- License: Apache License 2.0. `java/vendor/LICENSE` and `java/vendor/NOTICE` are the upstream
  `LICENSE` and `NOTICE` files, copied unmodified (the NOTICE states that `io.microraft` contains
  code originating from Hazelcast IMDG).
- Build dependencies of the core: SLF4J API (runtime) and the JSR-305 / SpotBugs annotations
  (compile time). The annotations (`javax.annotation.Nonnull`, `Nullable`, `Nonnegative`,
  `edu.umd.cs.findbugs.annotations.SuppressFBWarnings`) are on JMC's test runtime classpath
  (`jsr305-3.0.2.jar`, `spotbugs-annotations-4.9.4.jar`, transitive dependencies of JMC), which is
  what the nodes compile against; SLF4J is not, so `java/shim/org/slf4j/{Logger,LoggerFactory}.java`
  are written for this benchmark (not copied from SLF4J): the subset of the interface the core calls,
  debug/info/warn dropped, error printed to stderr.

## Harness (written for this benchmark, 2026-11-01)

- `java/bench/microraft/MicroRaft.java`: the `MpProgram` (arguments, clients, the specification
  twin, spawn order).
- `java/bench/microraft/Node.java`: one node thread: the executor, transport, in-memory store and
  state machine MicroRaft leaves to its user, the bounded election timeouts, proposal forwarding.
- `java/bench/microraft/Wire.java`: the transport's value encoding of MicroRaft messages.
- `java/harness/io/microraft/impl/MustRaftNodes.java`: harness code placed in the core's package
  only to call the package-private `RaftNodeImpl` constructor exactly as `RaftNodeBuilderImpl.build()`
  does (README §3 says why the builder is bypassed).

## The Rust side and the specification

- The Rust side of the case study is `benchmarks/raft-rs` (tikv/raft-rs ad13f3d, unmodified, with its
  own harness and provenance). The specification here is a twin of its `spec_log`
  (`benchmarks/raft-rs/rust/src/main.rs`), itself the client-visible content of Raft's TLA+
  specification (`raft.tla`, Ongaro; see `benchmarks/raft-rs/source/PROVENANCE.md`).
- Runs: `out/java/runs.log` (every command of this directory's measurements, with full output).
