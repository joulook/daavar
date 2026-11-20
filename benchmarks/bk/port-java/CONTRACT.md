# Observation contract of the BookKeeper transliterations (bk: main at 7b66b5d; bk-413: release-4.13.0)

Both transliterations (bench.bookkeeper.BookKeeper, bench.bookkeeper413.BookKeeper413) are unscored references, under the
owner's logical-task contract (as for the Cassandra read-repair and 3FS transliterations, 2026-11-05): one process per
concurrent activity of the source (each PendingAddOp in flight, each recovery read, the ReadLastConfirmedOp, the handles
with their pending-add queue and callback thread, one sequential process per bookie standing for its request processors and
storage, the metadata store as a process with versioned updates); pool-worker reuse is not modelled; a shared data structure
is a sequential process whose program order is part of the observation. The entry numbering of the interface (w1's entries
1..e; Closed carries the last entry's interface number, -1 when empty) is applied at the handle's open callback.

Executor queue (fix round 3, 2026-11-06, after Astra's rounds 1-3): the handle's ordered executor and its queue are one sequential
process; the receipt of a callback is its submission, the queue runs in order, a task submitted by a running task is appended at
that moment (behind the answers already received, ahead of the ones received later), and every bookie answers the handle on one
channel so that fifo per channel keeps the per-connection order. A dependency created by the receipt of a callback before its
execution is part of the observation under this contract (the same ruling as the 3FS metadata process), not a defect.
