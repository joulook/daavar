# Provenance

Copied verbatim (cp -p, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/aneris/`:

- `ml/`: `ml_sources/examples/reliable_communication/lib/repdb/repdb_code.ml` (154 lines),
  `.../lib/repdb/log_code.ml` (36 lines), and the client script
  `ml_sources/examples/reliable_communication/examples/repdb_leader_followers/causality_example_code.ml`.
- `coq-spec/`: `aneris/examples/reliable_communication/lib/repdb/spec/*.v` (the Iris
  specification: the leader's log is the global history, a follower's state is a prefix of it).

The path given in the task, `aneris/aneris/examples/repdb/`, does not exist at this commit; the
repdb code is the one above (the catalogue record names the same files).

- Repository: https://github.com/logsem/aneris.git
- Commit: `59ae37946838df77fb2c573803b68462a3af9d0f` (2025-10-31)
- Catalogue record: `related-work-papers/sources/CATALOGUE-coq-iris.md`,
  "aneris/aneris/examples/reliable_communication/lib/repdb: leader-followers replicated DB";
  candidate N11 in `CANDIDATES.md` §1.

The program text the benchmark follows: `client_request_handler_at_leader` (write: `db := insert`,
`log_add_entry ((k,v), n)`, `monitor_signal`, `InjL ()`), `follower_request_handler`
(`log_wait_until log mon req`, then entry `req`), `sync_loop` (request `i`, `assert (i = j)`,
apply, `aux (i + 1)`), `client_request_handler_at_follower` (`map_lookup k !db`), and the
causality example (`node0`: `wr "x" 37; wr "y" 1`; `node1`: wait until `rd "y" = Some 1`, then
`assert (rd "x" = Some 37)`).
