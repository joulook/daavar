# Observation contract of the BookKeeper transliterations (bk: main at 7b66b5d; bk-413: release-4.13.0)

Both transliterations (bench.bookkeeper.BookKeeper, bench.bookkeeper413.BookKeeper413) are unscored references, under the
owner's logical-task contract (as for the Cassandra read-repair and 3FS transliterations, 2026-11-05): one process per
concurrent activity of the source (each PendingAddOp in flight, each recovery read, the ReadLastConfirmedOp, the handles
with their pending-add queue and callback thread, one sequential process per bookie standing for its request processors and
storage, the metadata store as a process with versioned updates); pool-worker reuse is not modelled; a shared data structure
is a sequential process whose program order is part of the observation. The entry numbering of the interface (w1's entries
1..e; Closed carries the last entry's interface number, -1 when empty) is applied at the handle's open callback.
