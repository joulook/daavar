# Observation contract of the Cassandra read-repair transliteration (owner's decision, 2026-11-05 20:15)

The transliteration bench.cassandra_rr.CassandraReadRepair is an unscored reference. It models each Cassandra thread pool
task as a logical task: a replica is one coordinator process, one MUTATION-stage process and one single-use READ-stage process
per read, with the source's dependencies (the memtable's publication to readers, the callbacks' conditions, the read-repair
continuation) as messages and no other program order between tasks. Cassandra's reuse of pool workers, which would add
program-order edges between unrelated tasks run by the same worker, is not modelled: the exported causal order of this
program is that of logical tasks, not of operating-system threads. Astra's reviews (plan/case-studies/cassandra/rr/
translit-review/REVIEW-r3.md) hold this to be the remaining difference to the source; the owner accepted it as the
contract rather than modelling worker reuse (state-space growth, and an arguable pool model).
