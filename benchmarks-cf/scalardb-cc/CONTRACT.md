# Observation contract of the ScalarDB Consensus Commit transliteration (bench.scalardb.ConsensusCommit, scalar-labs/scalardb at 416cdc2)

An unscored reference under the owner's logical-task contract (as for the Cassandra, 3FS, BookKeeper and CockroachDB
transliterations): one process per concurrent activity of the source (each transaction's handle with its CrudHandler and
CommitHandler; each key's storage as one sequential process standing for the conditional-mutation backend; the coordinator table as
one sequential process; a reader's asynchronous recovery as its own process), the source's dependencies as messages, no modelling of
thread pools or executors; a shared data structure (a storage record, the coordinator row) is a sequential process whose program
order is part of the observation. The shared abstractions of INTERFACE.md replace the storage backends, ids and timestamps, and the
writer's expiry (a nondeterministic choice per call). Late replies are never received (a reply is received only by its request id).
