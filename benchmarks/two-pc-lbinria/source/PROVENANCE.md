# Provenance

Copied verbatim (2026-10-28) from the lbinria TwoPhase repository checked out at
`plan/benchmarks/related-work-papers/sources/lbinria-twophase/`, commit
e1467e24f42d6468ec99d3489b30d3665289761c (2026-02-25, https://github.com/lbinria/TwoPhase.git).
Every file was re-compared byte for byte (`cmp`) against the checkout on 2026-10-30.

| file here | path in the source tree |
|---|---|
| `README.md` | `lbinria-twophase/README.md` |
| `run_impl.py` | `lbinria-twophase/run_impl.py` (starts the mailbox server with `"unordered"` boxes) |
| `spec/TwoPhase.tla` | `lbinria-twophase/spec/TwoPhase.tla` (the protocol spec; no `RMChooseToAbort`) |
| `spec/TCommit.tla` | `lbinria-twophase/spec/TCommit.tla` (the abstract spec, `TCConsistent`) |
| `spec/TwoPhaseTrace.tla` | `lbinria-twophase/spec/TwoPhaseTrace.tla` (the trace spec: logged events to actions) |
| `src/main/java/org/lbee/**` | `lbinria-twophase/src/main/java/org/lbee/**` (all 16 Java files: `Client`, `helpers/Helper`, `network/*`, `protocol/*`) |
| `TracesCompact/trace-4RM-BUG-E.ndjson` | `lbinria-twophase/TracesCompact/trace-4RM-BUG-E.ndjson` (a recorded trace of the counting bug: four `TMRcvPrepared`, three `RMPrepare` before `TMCommit`) |

The counting bug is the commented-out line 51 of `protocol/TransactionManager.java`,
`// this.preparedRMs = new ArrayList<>();` (line 52 is the shipped `new HashSet<>()`).

Not modified. The Rust and Java programs in `../rust/` and `../java/` are translations, described in `../README.md`.
