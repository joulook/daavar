# Excerpt: Robbert van Renesse, Fred B. Schneider, *Chain Replication for Supporting High Throughput and Availability*, OSDI 2004, sections 2 (A Storage Service Interface, Figures 1–2) and 3 (Chain Replication Protocol, Protocol Details, Coping with Server Failures, Extending a Chain)

Verbatim text (pdftotext, reading order; figures as their captions and text).

```
A Storage Service Interface

Clients of a storage service issue requests for query
and update operations. While it would be possible
to ensure that each request reaching the storage service is guaranteed to be performed, the end-to-end
argument [20] suggests there is little point in doing
so. Clients are better off if the storage service simply generates a reply for each request it receives and
completes, because this allows lost requests and lost
replies to be handled as well: a client re-issues a request if too much time has elapsed without receiving
a reply.
• The reply for query(objId , opts) is derived from
the value of object objId ; options opts characterizes what parts of objId are returned. The
value of objId remains unchanged.
• The reply for update(objId , newVal , opts) depends on options opts and, in the general case,
can be a value V produced in some nondeterministic pre-programmed way involving the current value of objId and/or value newVal ; V then
becomes the new value of objId .1
Query operations are idempotent, but update operations need not be. A client that re-issues a nonidempotent update request must therefore take precautions to ensure the update has not already been

92

State is:
Hist objID : update request sequence
Pending objID : request set
Transitions are:
T1: Client request r arrives:
Pending objID := Pending objID ∪ {r}
T2: Client request r ∈ Pending objID ignored:
Pending objID := Pending objID − {r}
T3: Client request r ∈ Pending objID processed:
Pending objID := Pending objID − {r}
if r = query(objId , opts ) then
reply according options opts based
on Hist objID
else if r = update(objId , newVal , opts) then
Hist objID := Hist objID · r
reply according options opts based
on Hist objID
Figure 1: Client’s View of an Object.

performed. The client might, for example, first issue
a query to determine whether the current value of
the object already reflects the update.
A client request that is lost before reaching the
storage service is indistinguishable to that client
from one that is ignored by the storage service. This
means that clients would not be exposed to a new
failure mode when a storage server exhibits transient
outages during which client requests are ignored. Of
course, acceptable client performance likely would
depend on limiting the frequency and duration of
transient outages.
With chain replication, the duration of each transient outage is far shorter than the time required to
remove a faulty host or to add a new host. So, client
request processing proceeds with minimal disruption
in the face of failure, recovery, and other reconfiguration. Most other replica-management protocols
either block some operations or sacrifice consistency
guarantees following failures and during reconfigurations.
We specify the functionality of our storage service
by giving the client view of an object’s state and of
that object’s state transitions in response to query
and update requests. Figure 1 uses pseudo-code to
give such a specification for an object objID .
The figure defines the state of objID in terms
of two variables: the sequence2 Hist objID of updates that have been performed on objID and a set
Pending objID of unprocessed requests.

OSDI ’04: 6th Symposium on Operating Systems Design and Implementation USENIX Association

updates

queries

HEAD

replies

TAIL

Figure 2: A chain.
Then, the figure lists possible state transitions.
Transition T1 asserts that an arriving client request is added to Pending objID . That some pending requests are ignored is specified by transition
T2—this transition is presumably not taken too frequently. Transition T3 gives a high-level view of
request processing: the request r is first removed
from Pending objID ; query then causes a suitable reply to be produced whereas update also appends r
(denoted by ·) to Hist objID .3

3

Chain Replication Protocol

Servers are assumed to be fail-stop [21]:
• each server halts in response to a failure rather
than making erroneous state transitions, and
• a server’s halted state can be detected by the
environment.
With an object replicated on t servers, as many as
t−1 of the servers can fail without compromising the
object’s availability. The object’s availability is thus
increased to the probability that all servers hosting
that object have failed; simulations in §5.4 explore
this probability for typical storage systems. Henceforth, we assume that at most t − 1 of the servers
replicating an object fail concurrently.
In chain replication, the servers replicating a given
object objID are linearly ordered to form a chain.
(See Figure 2.) The first server in the chain is called
the head, the last server is called the tail, and request
processing is implemented by the servers roughly as
follows:
Reply Generation. The reply for every request is
generated and sent by the tail.
Query Processing. Each query request is directed
to the tail of the chain and processed there
atomically using the replica of objID stored at
the tail.

Update Processing. Each update request is directed to the head of the chain. The request
is processed there atomically using replica of
objID at the head, then state changes are forwarded along a reliable FIFO link to the next
element of the chain (where it is handled and
forwarded), and so on until the request is handled by the tail.
Strong consistency thus follows because query requests and update requests are all processed serially
at a single server (the tail).
Processing a query request involves only a single
server, and that means query is a relatively cheap
operation. But when an update request is processed,
computation done at t − 1 of the t servers does not
contribute to producing the reply and, arguably, is
redundant. The redundant servers do increase the
fault-tolerance, though.
Note that some redundant computation associated with the t − 1 servers is avoided in chain replication because the new value is computed once by
the head and then forwarded down the chain, so
each replica has only to perform a write. This forwarding of state changes also means update can be a
non-deterministic operation—the non-deterministic
choice is made once, by the head.

3.1

Protocol Details

Clients do not directly read or write variables
Hist objID and Pending objID of Figure 1, so we are
free to implement them in any way that is convenient. When chain replication is used to implement
the specification of Figure 1:
• Hist objID is defined to be Hist TobjID , the value
of Hist objID stored by tail T of the chain, and
• Pending objID is defined to be the set of client
requests received by any server in the chain and
not yet processed by the tail.
The chain replication protocols for query processing
and update processing are then shown to satisfy the
specification of Figure 1 by demonstrating how each
state transition made by any server in the chain is
equivalent either to a no-op or to allowed transitions
T1, T2, or T3.
Given the descriptions above for how Hist objID
and Pending objID are implemented by a chain (and
assuming for the moment that failures do not occur),
we observe that the only server transitions affecting
Hist objID and Pending objID are: (i) a server in the
chain receiving a request from a client (which affects
Pending objID ), and (ii) the tail processing a client

USENIX Association OSDI ’04: 6th Symposium on Operating Systems Design and Implementation

93

request (which affects Hist objID ). Since other server
transitions are equivalent to no-ops, it suffices to
show that transitions (i) and (ii) are consistent with
T1 through T3.
Client Request Arrives at Chain. Clients send
requests to either the head (update) or the tail
(query). Receipt of a request r by either adds
r to the set of requests received by a server but
not yet processed by the tail. Thus, receipt of
r by either adds r to Pending objID (as defined
above for a chain), and this is consistent with
T1.
Request Processed by Tail. Execution causes
the request to be removed from the set of
requests received by any replica that have not
yet been processed by the tail, and therefore
it deletes the request from Pending objID (as
defined above for a chain)—the first step of
T3. Moreover, the processing of that request
by tail T uses replica Hist TobjID which, as
defined above, implements Hist objID —and this
is exactly what the remaining steps of T3
specify.

Coping with Server Failures
In response to detecting the failure of a server that is
part of a chain (and, by the fail-stop assumption, all
such failures are detected), the chain is reconfigured
to eliminate the failed server. For this purpose, we
employ a service, called the master, that
• detects failures of servers,
• informs each server in the chain of its new predecessor or new successor in the new chain obtained by deleting the failed server,
• informs clients which server is the head and
which is the tail of the chain.
In what follows, we assume the master is a single
process that never fails. This simplifies the exposition but is not a realistic assumption; our prototype implementation of chain replication actually
replicates a master process on multiple hosts, using
Paxos [16] to coordinate those replicas so they behave in aggregate like a single process that does not
fail.
The master distinguishes three cases: (i) failure
of the head, (ii) failure of the tail, and (iii) failure
of some other server in the chain. The handling
of each, however, depends on the following insight
about how updates are propagated in a chain.

94

Let the server at the head of the chain be labeled
H, the next server be labeled H + 1, etc., through
the tail, which is given label T . Define
Hist iobjID  Hist jobjID
to hold if sequence4 of requests Hist iobjID at the
server with label i is a prefix of sequence Hist jobjID
at the server with label j. Because updates are sent
between elements of a chain over reliable FIFO links,
the sequence of updates received by each server is a
prefix of those received by its successor. So we have:
Update Propagation Invariant. For servers
labeled i and j such that i ≤ j holds (i.e., i is
a predecessor of j in the chain) then:
Hist jobjID  Hist iobjID .
Failure of the Head. This case is handled by the
master removing H from the chain and making the
successor to H the new head of the chain. Such a
successor must exist if our assumption holds that at
most t − 1 servers are faulty.
Changing the chain by deleting H is a transition
and, as such, must be shown to be either a noop or consistent with T1, T2, and/or T3 of Figure 1. This is easily done. Altering the set of
servers in the chain could change the contents of
Pending objID —recall, Pending objID is defined as the
set of requests received by any server in the chain
and not yet processed by the tail, so deleting server
H from the chain has the effect of removing from
Pending objID those requests received by H but not
yet forwarded to a successor. Removing a request
from Pending objID is consistent with transition T2,
so deleting H from the chain is consistent with the
specification in Figure 1.
Failure of the Tail. This case is handled by removing tail T from the chain and making predecessor T − of T the new tail of the chain. As before,
such a predecessor must exist given our assumption
that at most t − 1 server replicas are faulty.
This change to the chain alters the values of
both Pending objID and Hist objID , but does so in
a manner consistent with repeated T3 transitions:
Pending objID decreases in size because Hist TobjID 
−

Hist TobjID (due to the Update Propagation Invariant,
since T − < T holds), so changing the tail from T
to T − potentially increases the set of requests completed by the tail which, by definition, decreases
the set of requests in Pending objID . Moreover, as
required by T3, those update requests completed

OSDI ’04: 6th Symposium on Operating Systems Design and Implementation USENIX Association

by T − but not completed by T do now appear in
Hist objID because with T − now the tail, Hist objID is
−
defined as Hist TobjID .
Failure of Other Servers. Failure of a server S
internal to the chain is handled by deleting S from
the chain. The master first informs S’s successor S +
of the new chain configuration and then informs S’s
predecessor S − . This, however, could cause the Update Propagation Invariant to be invalidated unless
some means is employed to ensure update requests
that S received before failing will still be forwarded
along the chain (since those update requests already
do appear in Hist iobjID for any predecessor i of S).
The obvious candidate to perform this forwarding
is S − , but some bookkeeping and coordination are
now required.
Let U be a set of requests and let <U be a total
ordering on requests in that set. Define a request
sequence r to be consistent with (U, <U ) if (i) all requests in r appear in U and (ii) requests are arranged
in r in ascending order according to <U . Finally, for
request sequences r and r consistent with (U, <U ),
define r ⊕ r to be a sequence of all requests appearing in r or in r such that r ⊕ r is consistent with
(U, <U ) (and therefore requests in sequence r ⊕ r
are ordered according to <U ).
The Update Propagation Invariant is preserved by
requiring that the first thing a replica S − connecting
to a new successor S + does is: send to S + (using
the FIFO link that connects them) those requests in
−
Hist SobjID that might not have reached S + ; only after
those have been sent may S − process and forward
requests that it receives subsequent to assuming its
new chain position.
To this end, each server i maintains a list Senti
of update requests that i has forwarded to some
successor but that might not have been processed
by the tail. The rules for adding and deleting elements on this list are straightforward: Whenever
server i forwards an update request r to its successor, server i also appends r to Senti . The tail sends
an acknowledgement ack(r) to its predecessor when
it completes the processing of update request r. And
upon receipt ack(r), a server i deletes r from Senti
and forwards ack(r) to its predecessor.
A request received by the tail must have been received by all of its predecessors in the chain, so we
can conclude:
Inprocess Requests Invariant. If i ≤ j then
Hist iobjID = Hist jobjID ⊕ Senti .

S−

master

S+

S

1
2
3
4

Figure 3: Space-time diagram for deletion of internal
replica.

Thus, the Update Propagation Invariant will be
maintained if S − , upon receiving notification from
the master that S + is its new successor, first forwards the sequence of requests in SentS − to S + .
Moreover, there is no need for S − to forward the
+
prefix of SentS − that already appears in Hist SobjID .
The protocol whose execution is depicted in Figure 3 embodies this approach (including the optimization of not sending more of the prefix than necessary). Message 1 informs S + of its new role; message 2 acknowledges and informs the master what
is the sequence number sn of the last update request S + has received; message 3 informs S − of its
new role and of sn so S − can compute the suffix of
SentS − to send to S + ; and message 4 carries that
suffix.
Extending a Chain. Failed servers are removed
from chains. But shorter chains tolerate fewer failures, and object availability ultimately could be
compromised if ever there are too many server failures. The solution is to add new servers when chains
get short. Provided the rate at which servers fail is
not too high and adding a new server does not take
too long, then chain length can be kept close to the
desired t servers (so t − 1 further failures are needed
to compromise object availability).
A new server could, in theory, be added anywhere
in a chain. In practice, adding a server T + to the
very end of a chain seems simplist. For a tail T + ,
the value of SentT + is always the empty list, so initializing SentT + is trivial. All that remains is to
+
initialize local object replica Hist TobjID in a way that
satisfies the Update Propagation Invariant.
+
The initialization of Hist TobjID can be accom-

USENIX Association OSDI ’04: 6th Symposium on Operating Systems Design and Implementation

95

plished by having the chain’s current tail T forward
the object replica Hist TobjID it stores to T + . The
forwarding (which may take some time if the object is large) can be concurrent with T ’s processing
query requests from clients and processing updates
from its predecessor, provided each update is also
+
appended to SentT . Since Hist TobjID  Hist TobjID
holds throughout this forwarding, Update Propagation Invariant holds. Therefore, once
+

Hist TobjID = Hist TobjID ⊕ SentT
holds, Inprocess Requests Invariant is established
and T + can begin serving as the chain’s tail:
• T is notified that it no longer is the tail. T
is thereafter free to discard query requests it
receives from clients, but a more sensible policy
is for T to forward such requests to new tail T + .
• Requests in SentT are sent (in sequence) to T + .
• The master is notified that T + is the new tail.
• Clients are notified that query requests should
be directed to T + .

4

```
