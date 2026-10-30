# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/tlaplus-examples/specifications/ewd998/`, the TLA+
Examples repository (https://github.com/tlaplus/Examples) checked out at commit
`53955cf52c9cbe417b2bde68ab72edf2b9096cfc` (2026-10-26). Every file was compared byte for byte
(`cmp`) against the checkout on 2026-10-30.

| file here | path in the source tree | role |
|---|---|---|
| `AsyncTerminationDetection.tla` | `ewd998/AsyncTerminationDetection.tla` | the abstract spec (`terminated`, `Safe == terminationDetected => terminated`): our oracle |
| `EWD998.tla` | `ewd998/EWD998.tla` | Safra's algorithm, atomic token (`InitiateProbe`, `PassToken`, `RecvMsg` with `color' = black`) |
| `EWD998Chan.tla` | `ewd998/EWD998Chan.tla` | the same with channels (`inbox[n]`, receive from any index): the asyn model |
| `EWD998ChanTrace.md` | `ewd998/EWD998ChanTrace.md` | the implementation diary (the spec/impl divergences found by trace validation) |
| `impl/src/EWD998.java` | `ewd998/impl/src/EWD998.java` | the node loop our detectors follow (352 lines) |
| `impl/src/EWD998App.java`, `Pair.java`, `VectorClock.java` | `ewd998/impl/src/` | launcher and helpers (478 Java lines in all) |

Catalogue record: `related-work-papers/sources/CATALOGUE-tla.md`, "ewd998/"; candidate N14 in
`CANDIDATES.md` §1. The fault `no-black-on-receive` is ours (the catalogue's "drop Rule 3"),
not shipped by the source.
