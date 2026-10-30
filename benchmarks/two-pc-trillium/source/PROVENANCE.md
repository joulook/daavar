# Provenance

Copied verbatim (2026-10-28) from the Aneris/Trillium repository checked out at
`plan/benchmarks/related-work-papers/sources/aneris/`, commit
59ae37946838df77fb2c573803b68462a3af9d0f (2025-10-31, https://github.com/logsem/aneris.git).
Every file was re-compared byte for byte (`cmp`) against the checkout on 2026-10-30. The
directory layout below `source/` is the repository's.

| file here | path in the source tree |
|---|---|
| `ml_sources/examples/transaction_commit/two_phase_code.ml` | `aneris/ml_sources/examples/transaction_commit/two_phase_code.ml` (the OCaml program, 62 lines: `resource_manager`, `transaction_manager`) |
| `aneris/examples/transaction_commit/two_phase_code.v` | same path (the AnerisLang program, generated from the `.ml`) |
| `aneris/examples/transaction_commit/two_phase_runner_code.v` | same path (the runner: 1 TM `tm`, 3 RMs `rm.01`–`rm.03`) |
| `aneris/examples/transaction_commit/tc_model.v` | same path (the Coq `TC_model`, from TLA+ `TCommit`) |
| `aneris/examples/transaction_commit/two_phase_adequacy.v` | same path (`tpc_simulation`: the program refines `TC_model`) |
| `aneris/examples/transaction_commit/Readme.md` | same path |

Not copied (proof files, not needed to read the program): `two_phase_prelude.v`,
`two_phase_rm.v`, `two_phase_tm.v`, `two_phase_runner_proof.v`, `gen_mono_heap.v`.

Not modified. The Rust and Java programs in `../rust/` and `../java/` are translations, described in `../README.md`.
