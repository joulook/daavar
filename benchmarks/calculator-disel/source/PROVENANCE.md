# Provenance

Copied verbatim (cp -p, no edits) on 2026-10-28 from
`plan/benchmarks/related-work-papers/sources/disel/theories/Examples/Calculator/` (all seven `.v` files).

- Repository: https://github.com/DistributedComponents/disel.git
- Commit: `4ae4c207ef1a25010d63508f2bbda2d4c197d5ab` (2024-07-26)
- Catalogue record: `related-work-papers/sources/CATALOGUE-verdi-disel.md`,
  "disel/theories/Examples/Calculator"; candidate N2 in `CANDIDATES.md` §1.

The program text the benchmark follows: `CalculatorProtocol.v` (messages `req`/`resp`),
`CalculatorServerLib.v` (`blocking_receive_req`, `send_answer`), `SimpleCalculatorServers.v`
(`batched_server`, `memoizing_server`), `DelegatingCalculatorServer.v` (`delegating_server`),
`CalculatorClientLib.v` (`compute_f`, `compute_list_f`), `SimpleCalculatorApp.v` (`f [x; y] = x + y`).
