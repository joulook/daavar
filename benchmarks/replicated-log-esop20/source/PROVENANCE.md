# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-31 from
`plan/benchmarks/related-work-papers/sources/aneris-esop20/examples/`, the examples of
Krogh-Jespersen, Timany, Ohlenbusch, Gregersen, Birkedal, "Aneris: A Mechanised Logic for Modular
Reasoning about Distributed Systems", ESOP 2020 (artifact `https://iris-project.org/artifacts/2020-esop-aneris.tar.gz`,
`aneris-esop20/2020-esop-aneris.tar.gz`, md5 `ee2e1628dc4ee86be2c297b6c8066f45`; Coq 8.9.1, Iris 3.2.0):

| file | lines | md5 |
|---|---|---|
| `replicated_log/rep_log.v` (the program: `db`, `logger`, `client`, `logger_runner`) | 77 | `80ab33d2f03e9ec1654e24162c532f30` |
| `replicated_log/rep_log_proof.v` (the Iris specification: `log_si`, `logger_spec`, `rep_log_safe`) | 728 | `fc70f35da3bfe4606561f1b5903c4685` |
| `two_phase_commit/tpc.v` (`tpc_coordinate`, `tpc_participant_setup`, used by `rep_log.v`) | 35 | `d873f3557e007b5a5d6704d3ab2e892f` |
| `two_phase_commit/tpc_proof.v` (the TPC socket protocols and specs) | 744 | `43ca03d7d3654545b5c1952c29eae01c` |
| `two_phase_commit/tpc_correct.v` (unverified variant, not used) | 40 | `6286390fc4a87004c85cbb242162232d` |
| `library/lib.v` (`listen`, `listen_wait`, `value_of_message`) | 293 | `ea24dd14a10de022445c054f5308975f` |

Catalogue record: `related-work-papers/sources/CATALOGUE-artifacts-2.md`, "AN/replicated_log";
candidate N22 in `CANDIDATES.md` §1b. Not the same program as `benchmarks/replicated-log-aneris`
(N11, the later leader–follower `repdb`).
