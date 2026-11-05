# Observation contract of the CockroachDB parallel-commits transliteration (bench.cockroach.ParallelCommits, Go sources at d30c905)

An unscored reference under the owner's logical-task contract (as for the Cassandra, 3FS and BookKeeper transliterations): one
process per concurrent activity of the source (tc = the coordinator's interceptor stack with the DistSender; tcx = the explicit-
commit goroutine; h_i = each preventer's push/recover path; r_i = each range as one sequential process standing for its batch
evaluation and its store), the source's dependencies as messages, no modelling of goroutine pools, the stopper or singleflight;
a shared data structure (a range's store, the coordinator's in-flight write set) is a sequential process whose program order is
part of the observation. The shared abstractions of INTERFACE.md replace MVCC, latches, the clock, heartbeats and GC.
