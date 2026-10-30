# Provenance

Copied verbatim (`cp -rp`, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/oopsla25-artifact/oopsla552-artefact-amd64/refinement-lib/case-study-memcached/`:
the whole `src/` tree (18 files) and `Cargo.toml`. `diff -r` against the original reports no difference.

- Artefact: Bílý, Pereira, Müller, "A Refinement Methodology for Distributed Programs in Rust",
  OOPSLA 2025, artefact doi:10.5281/zenodo.15753818 (submission #552). The artefact is a zip, not a
  git repository, so there is no commit: the local copy is
  `oopsla25-artifact/oopsla552-artefact-amd64.zip`, md5 `5ce144a862977e1db0370960209033ca`
  (3,286,357,442 bytes). The library is built against Prusti commit `79d48686`
  (README of the artefact).
- Catalogue record: `related-work-papers/sources/CATALOGUE-oopsla25.md`,
  "oopsla25/case-study-memcached: Memcached (paper §5.1, v1–v3)"; planned pair `RELATED-BENCHMARKS.tex` §B5.
- Only v3 (fine-grained locking: one `MutexWrap<Bucket>` and one `StorageBucket(hash)` guard per
  bucket) is in the artefact.

The program text the benchmark follows: `src/model/mod.rs` (`MemcacheModel::next`: per-connection
FIFO `network_cmd` / `network_res`, `ConState` Idle → HaveCommand → HaveResponse, one `cache`),
`src/model/message.rs` (`cache_process`: the Set/Get semantics), `src/model/guard.rs`
(`StorageBucket(usize)`), `src/bin/memcached.rs` (`handle_client`: read / `Storage::process` / write),
`src/storage/mod.rs` (`Storage::process`: hash the key, lock that bucket only, `set`/`get`).

md5 of the copied files:

```
2fffe9044a9577088073d046adfc2b7f  ./Cargo.toml
617e714c31fb1147e350d4b2f6910622  ./src/bin/memcached.rs
c4fbae025dee4ac11ea3b4591d0853ee  ./src/lib.rs
e6be1abe08d26b4a10004c554dbbcb94  ./src/model/action.rs
d47d5b5a356eaa1cdee5bf458ebcfc90  ./src/model/env_state.rs
3ff22ef422769b61a0859a5f70d38ea5  ./src/model/guard.rs
a9815f657e5f0a4f72ffb8b0e0019d8f  ./src/model/message.rs
231f9a505f48cb60665fe2fb2af74264  ./src/model/mod.rs
586b3af11c05f6271b1830bd55485bbf  ./src/model/state.rs
afc70c8ccecbdc70c7c0a5d8c4bd6137  ./src/model/time.rs
2648d283638fe1c544e111434fe65892  ./src/proof_utils.rs
ed8204978d6db94db5eeac9de2544b7b  ./src/protocol/buffer.rs
147f7bca537b7106128fe9117bfa47ce  ./src/protocol/message.rs
803b388b56f22ad595b084df0eb4e557  ./src/protocol/mod.rs
2875713ddb31f8b69bcb4b56ff73b20a  ./src/protocol/parser.rs
f35fc9a27e8d61cb8f470d4b452626c5  ./src/protocol/protocol.rs
ecb94d0df747c3689f6a3b1e0fbe5d90  ./src/protocol/serialiser.rs
f2939e2e618001c58731c7e4668f827f  ./src/storage/mod.rs
```
