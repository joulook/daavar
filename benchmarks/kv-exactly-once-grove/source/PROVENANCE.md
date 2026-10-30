# Provenance

Copied verbatim (cp -rp, no edits) on 2026-10-28 from
`plan/benchmarks/related-work-papers/sources/gokv/tutorial/kvservice/` (the whole directory:
`server.go`, `client.go`, `kvservice.proto`, the generated `*.gb.go` and `*_gk/` marshalling,
`cmd/`).

- Repository: https://github.com/mit-pdos/gokv.git
- Commit: `d007325b6ee8a52ba16b68facd07cba902214b3f` (2025-05-08)
- Catalogue record: `related-work-papers/sources/CATALOGUE-grove-civl.md`,
  "gokv/tutorial/kvservice"; candidate N6 in `CANDIDATES.md` §1.
- The Perennial proof (the specification in the source) is not on disk
  (`src/program_proof/tutorial/...` in Perennial); the specification program here is the natural
  centralised one: one map server applying each operation once.
