# Provenance

Copied verbatim (2026-10-28) from the Disel repository checked out at
`plan/benchmarks/related-work-papers/sources/disel/`, commit
4ae4c207ef1a25010d63508f2bbda2d4c197d5ab (2024-07-26, https://github.com/DistributedComponents/disel.git).
Every file was re-compared byte for byte (`cmp`) against the checkout on 2026-10-30.

| file here | path in the source tree |
|---|---|
| `TwoPhaseProtocol.v` | `disel/theories/Examples/TwoPhaseCommit/TwoPhaseProtocol.v` (tags, states, transitions) |
| `TwoPhaseCoordinator.v` | same directory (`coordinator_round`, `coordinator_loop`, with proofs) |
| `TwoPhaseParticipant.v` | same directory (`participant_round`, `participant_with_choices`; tag-filtered `tryrecv` actions) |
| `TwoPhaseInductiveInv.v` | same directory (the invariant `Inv`, `cn_log_agreement`, `pt_log_agreement`) |
| `TwoPhaseInductiveProof.v` | same directory (preservation of `Inv`) |
| `SimpleTPCApp.v` | same directory (the runnable app: `cn = 0`, `pts = [1;2;3]`, `data_seq`, `choice_seq1..3`) |
| `TwoPhaseClient.v` | same directory (a design note for an external client; no code) |

That is the whole directory. Not modified. The Rust and Java programs in `../rust/` and
`../java/` are translations, described in `../README.md`.
