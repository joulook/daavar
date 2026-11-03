# Excerpt: Heidi Howard, Dahlia Malkhi, Alexander Spiegelman, *Flexible Paxos: Quorum intersection revisited*, OPODIS 2016 (arXiv:1608.06696v1), sections 2 (Paxos), 3 (FPaxos) and 4.1 (Majority quorums)

Verbatim text (pdftotext, reading order; the figure of section 4.1 omitted).

```
2  Paxos


Paxos

We wish to decide a single value v between a set of processes. The system is
asynchronous, each process may fail and the messages passed between them may
be lost. Each process has one or more roles. We have three roles: the proposer,
a process who wishes to have a particular value chosen, the acceptor, a process
which agrees and persists decided values or the learner, a process wishing to
learn the decided value.
A proposer who has a candidate value will try to propose the value to the
acceptors. If a value has already been chosen, the proposer will instead learn
it. The process of proposing a value has two stages: phase 1 and phase 2, each
phase requires a majority of acceptors to agree in order to proceed. We will now
look at each of these stages in details:
Phase 1 - Prepare & Promise
i A proposer selects a unique proposal number p and sends prepare(p) to the
acceptors.
ii Each acceptor receives prepare(p). If p is the highest proposal number
promised, then p is written to persistent storage and the acceptor replies
with promise(p’,v’). (p’,v’) is the last accepted proposal (if present) where p’
is the proposal number and v’ is the corresponding proposed value.
iii Once the proposer receives promise from the majority of acceptors, it proceeds
to phase two. Otherwise, it may try again with higher proposal number.
Phase 2 - Propose & Accept
i The proposer must now select a value v. If more than one proposal was
returned in phase 1 then it must choose the value associated with the highest
proposal number. If no proposals were returned, then the proposer can
choose its own value for v. The proposer then sends propose(p,v) to the
acceptors.
ii Each acceptor receives a propose(p,v). If p is equal to or greater than the
highest promised proposal number, then the promised proposal number and
accepted proposal is written to persistent storage and the acceptor replies
with accept(p).
iii Once the proposer receives accept(p) from the majority of acceptors, it learns
that the value v is decided. Otherwise, it may try phase 1 again with a
higher proposal number.
Paxos guarantees that once a value is decided, the decision is final and
no different value can be chosen. Paxos will reach agreement provided that
bn/2c + 1 acceptors out of n acceptors are up and are able to communicate.
Proving progress requires us to make some assumptions about the synchrony of
the system, as we cannot guarantee progress in a truly asynchronous systems [7].


3  FPaxos


3

FPaxos

In this section, we observe that the usual description of Paxos (as given in §2)
is more conservative than is necessary. To explain this observation, we will
differentiate between the quorum used by the first phase of Paxos, which we will
refer to as Q1 and the quorum for second phase, referred to as Q2.
Paxos uses majority quorums of acceptors for both Q1 and Q2. By requiring
that quorums contain at least a majority of acceptors we can guarantee that
there will be at least one acceptor in common between any two quorums. Paxos’s
proof of safety and progress is built upon this assumption that all quorums
intersect.
We observe that it is only necessary for phase 1 quorums (Q1 ) and phase
2 quorums (Q2 ) to intersect. There is no need to require that Q1 ’s intersect
with each other nor Q2 ’s intersect with each other. We refer to this as Flexible
Paxos (FPaxos) and it generalizes the Paxos algorithm. If we allow any set of at
least bn/2c + 1 acceptors to form a Q1 or Q2 quorum in FPaxos, then FPaxos
is equivalent to Paxos.
Using this observation, we can make use of many non-intersecting quorum
systems. In its most straight-forward application, we can simply decrease the
size of Q2 at the cost of increasing the size of Q1 quorums.
As we discussed earlier, the second phase of Paxos (replication) is far more
frequent than the first phase (leader election) in Multi-Paxos. Therefore, reducing
the size of Q2 decreases latency in the common case by reducing the number of
acceptors required to participate in replication, improves system tolerance to slow
acceptors and allows us to use disjoint sets of acceptors for higher throughput.
The price we pay for this is requiring more acceptors to participate when we
need to establish a new leader. Whilst electing a new leader is a rare event in
a stable system, if sufficient failures occur that we cannot form a Q1 quorum,
then we cannot make progress until some of the acceptors recover.
Like Paxos, the system is able to make progress provided that at least enough
acceptors are up and able to communicate to form both Q1 and Q2 quorums.
Unlike Paxos, we are able to make progress within a given phase, provided we are
able to form quorums corresponding to that phase. More concretely, if sufficient
failures have occurred such that a proposer can no longer form Q1 quorums
but is able to form the smaller Q2 quorums, the system can continue to safely
make progress until a new leader is required. If the acceptors recover before the
current leader fails, then the system suffers no loss in availability as a result.


Implications

We will now consider the practical implication of observing that quorums intersection is required only between the two phases of Paxos. There already exists
an extensive literature on quorum systems from the fields of databases and data
replication, which can now be more efficiently applied to the field of consensus.
Interesting example systems include weighted voting [9], hierarchies [15] and


crumbling walls [34]. For now however, we will illustrate the utility of FPaxos
by considering three naı̈ve example quorum systems: (1) majority quorums; (2)
simple quorums and (3) grid quorums.

4.1

Majority quorums

Currently, Paxos requires us to use quorums of size n/2 + 1 when the number
of acceptors n is even1 . Using our observation, we can safely reduce the size
of Q2 by one from n/2 + 1 to n/2 and keep Q1 the same. Such a change
would be trivial to implement and by reducing the number of acceptors required
to participate in replication, we can reduce latency and improve throughput.
Furthermore, we have also improved the fault tolerance of the system. As with
Paxos, if at most n/2 − 1 failures occur then we are guaranteed to be able to
make progress. However unlike with Paxos, if exactly n/2 acceptors fail and the
leader is still up then we are able to continue to make progress and suffer no
loss of availability.
Figure 2 shows two example traces of FPaxos with majority quorums in
practice. As the system is comprised of four acceptors, FPaxos uses a majority
(3 acceptors) for Q1 but requires only two acceptors for Q2. In the examples, the
two proposers wish to commit conflicting proposals. In figure 2a, proposer one is
first to execute FPaxos and its value a is committed. Later, proposer two executes
a round of Paxos and learns the value. In figure 2b, both proposers successfully
execute the first phase of FPaxos and simultaneously submit conflicting proposed
```
