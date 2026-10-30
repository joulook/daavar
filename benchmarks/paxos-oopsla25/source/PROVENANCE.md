# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/oopsla25-artifact/oopsla552-artefact-amd64/refinement-lib/refinement-examples/src/bin/ex05_paxos.rs`
(17,296 bytes, mtime 2025-06-27; md5 `60391296c87efaed9f75368373a60245`).

- Artefact: Bílý, Pereira, Müller, "A Refinement Methodology for Distributed Programs in Rust",
  OOPSLA 2025 (§5.3), artefact doi:10.5281/zenodo.15753818 (submission #552). The artefact is a zip,
  not a git repository, so there is no commit: the local copy is
  `oopsla25-artifact/oopsla552-artefact-amd64.zip`, md5 `5ce144a862977e1db0370960209033ca`.
- Catalogue record: `related-work-papers/sources/CATALOGUE-oopsla25.md`,
  "oopsla25/refinement-examples/src/bin/ex05_paxos.rs: Paxos (paper §5.3)"; planned pair
  `RELATED-BENCHMARKS.tex` §B1.

The program text the benchmark follows: `enum Message` (`Msg1a`, `Msg1b`, `Msg2a`, `Msg2b`),
`run_leader_ballot` (1b collection until `majority = acceptor_count / 2 + 1`, the `max_vote` rule,
`choose_value()` for a free value, `decide()` abandonment), `run_acceptor` (`ballot_greater` on 1a,
`ballot_greater_equal` on 2a, 1b reports `max_vote`). The model (`impl Model for PaxosModel`, the
TLA+-derived `Phase1a … Phase2b`) has no agreement predicate and no learners.
