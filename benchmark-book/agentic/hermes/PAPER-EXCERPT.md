# Excerpt: Antonios Katsarakis, Vasilis Gavrielatos, M. R. Siavash Katebzadeh, Arpit Joshi, Aleksandar Dragojevic, Boris Grot, Vijay Nagarajan, *Hermes: a Fast, Fault-Tolerant and Linearizable Replication Protocol*, ASPLOS 2020 (arXiv:2001.09804v1), section 3 (3.1 Overview, 3.2 Hermes Protocol in Detail, Figure 3)

Verbatim text (pdftotext, reading order). The reads and writes of §3.2 are the protocol for this exercise (no RMWs, no faults).

```

Hermes is a reliable membership-based broadcasting protocol that offers high throughput and low latency whilst
providing linearizable reads, writes, and RMWs (single-key
transactions). Hermes optimizes for the common case of no
failures [15] and targets intra-datacenter in-memory datastores with a replication degree typical of today’s deployments (3-7 replicas) [48]. As noted in §2.2, the replica count
does not constrain the size of a sharded datastore, since each
shard is replicated independently of other shards. Example
applications that can benefit from Hermes include reliable
datastores [11, 12, 80, 101], lock-services [24, 48] and applications that require high performance, strong consistency
and availability (e.g., [1, 20, 102]).
3.1

Overview

In Hermes, reads complete locally. Writes can be initiated
by any replica and complete fast regardless of conflicts. As
illustrated in Figure 2, a write to a key proceeds as follows:
the replica initiating the write (called coordinator) broadcasts
an Invalidation (INV) message to the rest of the replicas (called
followers) and waits on acknowledgments (ACKs). Once all
ACKs have been received; the write completes via a Validation
(VAL) message broadcast by the coordinator replica.
We now briefly overview the salient features of Hermes
and discuss the specifics in the following subsections.
Invalidations When an INV message is received, the target
key is placed in an Invalid state, meaning that reads to the
key cannot be served. While conceptually similar to a lock
(e.g., in 2PC), the key difference is that with invalidations,
concurrent writes to the same key do not fail and are resolved
in place through the use of logical timestamps as discussed
below. The use of invalidations is inspired by cache coherence protocols, where a cache line in an Invalid state informs
the readers that they must wait for an updated value.
Logical timestamps Each write in Hermes is tagged with
a monotonically-increasing per-key logical timestamp, implemented using Lamport clocks [61] and computed locally
at the coordinator replica. The timestamp is a lexicographically ordered tuple of [v, cid ] combining a key’s version
number (v), which is incremented on every write, with the
node id of the coordinator (cid ). Two or more writes to a
key are concurrent if their execution is initiated by different

Hermes Replication Protocol

ASPLOS ’20, March 16–20, 2020, Lausanne, Switzerland

replicas holding the same timestamp. Non-concurrent writes
to a key are ordered based on their timestamp version, while
concurrent writes from different coordinators (same version)
are ordered via their cid 5 . Uniquely tagged writes allow each
node to locally establish a global order of writes to a key.
High-performance non-conflicting writes Hermes allows for high-performance writes (§2.3) by maximizing concurrency while maintaining low latency. First, writes in Hermes are executed from any replica in a decentralized manner,
eschewing the use of a serialization point (e.g., a leader);
thus reducing the number of network hops and ensuring
load balance. In contrast to approaches that globally order
independent writes for strong consistency (e.g., ZAB – §5.1.1),
Hermes allows writes to different keys to proceed in parallel,
hence achieving inter-key concurrency. This is accomplished
via Hermes’ approach of invalidating all operational replicas
to achieve linearizability. When combined with the logical
timestamps, invalidations permit concurrent writes to the
same key to be correctly linearized at the endpoints; thus,
writes do not appear to conflict, making aborts unnecessary.
Finally, in the absence of a failure, writes in Hermes cost
one and a half round-trips (INV→ACK→VAL); however, the
exposed latency is just a single round-trip for each node.
From the perspective of the coordinator, once all ACKs are
received, it is safe to respond to a client because at this point,
the write is guaranteed to be visible to all live replicas, and
any future read cannot return the old value (i.e., the write
is committed – Figure 2 b ). The followers also observe only
a single round-trip (further optimized in §3.3), which starts
once an INV arrives; at that point, each follower responds
with an ACK and completes the write when a VAL is received.
Safely replayable writes Node and network faults during
a write to a key may leave the key in a permanently Invalid
state in some or all of the nodes. To prevent this, Hermes
allows any invalidated operational replica to replay the write
to completion without violating linearizability. This is accomplished using two mechanisms. First, the new value for
a key is propagated to the replicas in INV messages (Figure 2 a ). Such early value propagation guarantees that every
invalidated node is aware of the new value. Secondly, logical timestamps enable a precise global ordering of writes in
each of the replicas. By combining these ideas, a node that
finds a key in an Invalid state for an extended period can
safely replay a write by taking on a coordinator role and
retransmitting INV messages to the replica ensemble with
the original timestamp (i.e., original version number and cid ),
hence preserving the global write order.
The above features afford the following properties:
≻ Strong consistency: By invalidating all replicas of a key at
the start of a write, Hermes ensures that a key in a Valid
state is guaranteed to hold the most up-to-date value. Hermes

Figure 3. Metadata stored and messages sent by Hermes.
enforces the invariant that a read may complete if and only
if the key is in a Valid state, which provides linearizability.
≻ High performance: Local reads in concert with high performance broadcast-based non-conflicting writes from any
replica help ensure both low latency and high throughput.
≻ Fault tolerance: Hermes uses safely replayable writes to tolerate a range of faults, including message loss, node failures,
and network partitions. As a membership-based protocol,
Hermes is aided by RM to provide a stable group membership
of live nodes in the face of failures and network partitions.
3.2

Hermes Protocol in Detail

Hermes protocol consists of four stable states Valid, Invalid,
Write and Replay and a single transient state Trans. Figure 3
illustrates the format of protocol messages and the metadata
stored at each replica. A detailed protocol transition table,
as well as the T LA+ specification, are available online6 .
The following protocol is slightly simplified in that it only
focuses on reads and writes (omits RMWs) and only deals with
node failures (but not network faults). Resilience to network
faults and RMWs are described in §3.4 and §3.6, respectively.
Reads: A read request is serviced on an operational replica
(i.e., one with an RM lease) by returning the local value of
the requested key if it is in the Valid state. If the key is in
any other state, the request is stalled.
Writes:
Coordinator
A coordinator node issues a write to a key only if it is in
the Valid state; otherwise the write is stalled. To issue and
complete a write, the coordinator node:
• CTS : Updates the key’s local timestamp by incrementing its
version and appending its node id as the cid , and assigns
this new timestamp to the write.
• CINV : Promptly broadcasts an INV message consisting of
the key, the new timestamp (TS) and the value to all replicas and transitions the key to the Write state, whilst applying the new value locally.
• CACK : Once the coordinator receives ACKs from all the live
replicas, the write is completed by transitioning the key to
the Valid state (Invalid state if the key was in Trans state7 ).
6 https://hermes-protocol.com

5 More precisely, a timestamp A: [v , c
A idA ] is higher than a timestamp B:

7 The Trans state indicates a coordinator with a pending write that got

[vB , cidB ], if either vA > vB or vA = vB and cidA > cidB .

invalidated. While not required, the Trans state is useful for tracking when

ASPLOS ’20, March 16–20, 2020, Lausanne, Switzerland

• CVAL : Finally, the coordinator broadcasts a VAL consisting
of the key and the same timestamp to all the followers.
Note that the coordinator waits for ACKs only from the live
replicas as indicated in the membership variable. If a follower fails after an INV has been sent, the coordinator waits
for the ACK from the failed node until the membership is
reliably updated (after the node is detected as failed and the
membership lease expires – §2.4). Once the coordinator is
not missing any more ACKs, it can safely continue the write.
Follower
• FINV : Upon receiving an INV message, a follower compares
the timestamp from the incoming message to its local
timestamp of the key. If the received timestamp is higher
than the local timestamp, the follower transitions the key
to the Invalid state (Trans state if the key was in the Write
or the Replay state) and updates the key’s local timestamp
(both its version and cid ) and value.
• FACK : Irrespective of the result of the timestamp comparison, a follower always responds with an ACK containing
the same timestamp as that in the INV message of the write.
• FVAL : When a follower receives a VAL message, it transitions the key to the Valid state if and only if the received
timestamp is equal to the key’s local timestamp. Otherwise,
the VAL message is simply ignored.
Write Replays: A request that finds a key in the Invalid state
for an extended period of time (determined via the mlt timer,
described in §3.4) triggers a write replay. The node servicing
the request takes on the coordinator role, transitions the key
to the Replay state and begins a write replay by re-executing
steps CINV through CVAL using the TS and value received
with the INV message. Note that the original TS is used in the
replay (i.e., the cid is that of original coordinator) to allow the
write to be correctly linearized. Once the replay is completed,
the key transitions to the Valid state after which the initial
request is serviced.
▷ Formal verification: We expressed Hermes in T LA+ [62]
and model checked the protocol’s reads, writes, RMWs and
replays for safety and absence of deadlocks in the presence
of message reorderings and duplicates, and membership reconfigurations due to crash-stop failures.
```
