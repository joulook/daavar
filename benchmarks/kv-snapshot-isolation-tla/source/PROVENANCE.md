# Provenance

Copied verbatim (cp -p, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/tlaplus-examples/specifications/KeyValueStore/`:
`KVsnap.tla`, `KeyValueStore.tla`, `ClientCentric.tla`, `MCKVsnap.tla`, `MCKVsnap.cfg`,
`Util.tla`, `README.md`, `LICENSE`. Not copied: `APKeyValueStore.tla`, `KeyValueStore_proof.tla`,
`MCKVS.tla`, `MCKVSSafety*.cfg`, `manifest.json` (the proof and the TLC configurations of
`KeyValueStore.tla`, which the benchmark does not follow).

- Repository: https://github.com/tlaplus/Examples.git
- Commit: `53955cf52c9cbe417b2bde68ab72edf2b9096cfc` (2026-10-26)
- Catalogue record: `related-work-papers/sources/CATALOGUE-tla.md`, "KeyValueStore/: key-value
  store with snapshot isolation", pair (i); candidate N9 in `CANDIDATES.md` §1.

The program text the benchmark follows: `KVsnap.tla` (the PlusCal store `store`, `tx`,
`missed`; `process (t \in TxId)` with labels `START`, `READ`, `UPDATE`, `COMMIT`; the commit
test `missed[self] \intersect write_keys = {}`), and its closing comment "Serializability would
not be satisfied due to write-skew" (`Serialization == CC!Serializability(...)`), which the
`README.md` asks to uncomment to see the counterexample. `KeyValueStore.tla` is, by its own
header, also a snapshot-isolation store ("A simple key-value store exhibiting snapshot
isolation"); the serializable specification program is therefore ours (the catalogue's pair
(i)), not a transcription of `KeyValueStore.tla`. Its `RollbackTx` is what lets our specification
abort a transaction.
