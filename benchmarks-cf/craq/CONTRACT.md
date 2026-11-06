# Observation contract of the 3FS chain-replication transliteration (owner's decision, 2026-11-05 22:30)

The transliteration bench.craq.ChainReplication is an unscored reference. Two contracts apply, both decided by the owner:
1. Logical tasks (as for the Cassandra read-repair transliteration): one process per concurrent activity of the source (the
   target process, the chunk lock, one update task per client channel, one read task per client); pool-worker reuse is not
   modelled.
2. A shared memory location is a sequential process, and its program order is part of the observation: 3FS keeps the chunk's
   metadata in a lock-free concurrent map (ChunkStore.h l.38), so a read that sees the old version is not causally before the
   update in the source; in the port the metadata is served by the target process, which orders such a read before the
   update's completion in its program order (Astra's round-1 finding 1 and the fix round's remaining gap). The specifications (revised 2026-11-06,
   plan/case-studies/3fs/craq/SPEC-FIX-DESIGN.md) no longer serve everything in one object: a write serialiser and one
   version holder per client; they require exactly the causal contract of INTERFACE.md (revision 2026-11-06 23:00:
   writes ordered, a read of version m before the Ack of write m+1, older reads before newer reads' replies), which this
   port's target-process order meets; what is not claimed is that the port's causal order equals the source's at the
   memory level, which no message-passing port of shared-memory
   code can give.
The not-committed read retry is cut per target and the cut ends the read with a visible `Unavailable(i)` reply (INTERFACE.md,
revision 2026-11-06); both specifications admit `Unavailable` for any read with no effect on the state.
