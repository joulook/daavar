# Excerpt: Barbara Liskov, James Cowling, *Viewstamped Replication Revisited*, MIT-CSAIL-TR-2012-021, 2012, sections 2 (Background), 3 (Overview), 4 (The VR Protocol: the state of Figure 2) and 4.1 (Normal Operation)

Verbatim text (pdftotext, reading order; page numbers removed). View changes, recovery and reconfiguration (4.2–4.5, 7) are not part of this exercise: the group is fixed and no replica fails.

```
2  Background

This section begins with a discussion of our assumptions
about the environment in which VR runs. Section 2.2
discusses the number of replicas required to provide correct behavior, and Section 2.3 describes how to configure
a system to use VR.
2.1
Assumptions
VR handles crash failures: we assume that the only way
nodes fail is by crashing, so that a machine is either functioning correctly or completely stopped. VR does not
handle Byzantine failures, in which nodes can fail arbitrarily, perhaps due to an attack by a malicious party.
VR is intended to work in an asynchronous network,
like the Internet, in which the non-arrival of a message
indicates nothing about the state of its sender. Messages
might be lost, delivered late or out of order, and delivered
more than once; however, we assume that if sent repeatedly a message will eventually be delivered. In this paper
we assume the network is not under attack by a malicious
party who might spoof messages. If such attacks are a
concern, they can be withstood by using cryptography to
obtain secure channels.
2.2
Figure 1: VR Architecture; the figure shows the configuration when f = 1.
happened in the previous step. In a group of 2f + 1 replicas, f + 1 is the smallest quorum size that will work.
In general a group need not be exactly of size 2f + 1;
if it isn’t, the threshold is the largest f such that 2f + 1
is less than or equal to the group size, K, and a quorum
is of size K − f . However, for a particular threshold f
there is no benefit in having a group of size larger than
2f + 1: a larger group requires larger quorums to ensure
intersection, but does not tolerate more failures. Therefore in the protocol descriptions in this paper we assume
the group size is exactly 2f + 1.
Replica Groups
VR ensures reliability and availability when no more
than a threshold of f replicas are faulty. It does this by
using replica groups of size 2f + 1; this is the minimal
number of replicas in an asynchronous network under the
crash failure model. The rationale for needing this many
replicas is as follows:
We have to be able to carry out a request without waiting for f replicas to participate, since these replicas may
be crashed and unable to reply. However, the f replicas we didn’t hear from might merely be slow to reply,
e.g., because of network congestion. f of the replicas we
did hear from may thus subsequently fail. Therefore we
need to run the protocol with enough replicas to ensure
that even if these f fail, there is a least one replica that
knows about the request. This implies that each step of
the protocol must be processed by f + 1 replicas. These
f + 1 together with the f that may not respond give us
the smallest group size of 2f + 1.
A group of f + 1 replicas is often referred to as a quorum and correctness of the protocol depends on the quorum intersection property: the quorum of replicas that
processes a particular step of the protocol must have a
non-empty intersection with the group of replicas available to handle the next step, since this way we can ensure
that at each next step at least one participant knows what
2.3
Architecture
The architecture for running VR is presented in Figure 1.
The figure shows some client machines that are using
VR, which is running on 3 replicas; thus f = 1 in this
example. Client machines run the user code on top of
the VR proxy. The user code communicates with VR
by making operation calls to the proxy. The proxy then
communicates with the replicas to cause the operation to
be carried out and returns the result to the client when the
operation has completed.
The replicas run code for the service that is being replicated using VR, e.g., a file system. The replicas also run
the VR code. The VR code accepts requests from client
proxies, carries out the protocol, and when the request is
ready to be executed, causes this to happen by making
an up-call to the service code at the replica. The service
code executes the call and returns the result to the VR
code, which sends it in a message to the client proxy that
made the request.
Of the 2f + 1 replicas, only f + 1 need to run the service code. This point is discussed further in Section 6.1.

3  Overview



4  The VR Protocol

This section describes how VR works under the assumption that the group of replicas is fixed. We discuss some
ways to improve performance of the protocols in Section 5 and optimizations in Section 6. The reconfiguration protocol, which allows the group of replicas to
change, is described in Section 7.
Figure 2 shows the state of the VR layer at a replica.
The identity of the primary isn’t recorded in the state but
rather is computed from the view-number and the configuration. The replicas are numbered based on their
IP addresses: the replica with the smallest IP address is
replica 1. The primary is chosen round-robin, starting
with replica 1, as the system moves to new views. The
status indicates what sub-protocol the replica is engaged
in.
The client-side proxy also has state. It records the
configuration and what it believes is the current viewnumber, which allows it to know which replica is currently the primary. Each message sent to the client informs it of the current view-number; this allows the client
to track the primary.
In addition the client records its own client-id and a
current request-number. A client is allowed to have just
one outstanding request at a time. Each request is given a
number by the client and later requests must have larger
numbers than earlier ones; we discuss how clients ensure
this if they fail and recover in Section 4.5. The request
number is used by the replicas to avoid running requests
more than once; it is also used by the client to discard
duplicate responses to its requests.
State machine replication requires that replicas start in
the same initial state, and that operations be deterministic. Given these assumptions, it is easy to see that replicas will end up in the same state if they execute the same
sequence of operations. The challenge for the replication
protocol is to ensure that operations execute in the same
order at all replicas in spite of concurrent requests from
clients and in spite of failures.
VR uses a primary replica to order client requests; the
other replicas are backups that simply accept the order
selected by the primary. Using a primary provides an
easy solution to the ordering requirement, but it also introduces a problem: what happens if the primary fails?
VR’s solution to this problem is to allow different replicas to assume the role of primary over time. The system
moves through a sequence of views. In each view one of
the replicas is selected to be the primary. The backups
monitor the primary, and if it appears to be faulty, they
carry out a view change protocol to select a new primary.
To work correctly across a view change the state of the
system in the next view must reflect all client operations
that were executed in earlier views, in the previously selected order. We support this requirement by having the
primary wait until at least f + 1 replicas (including itself) know about a client request before executing it, and
by initializing the state of a new view by consulting at
least f + 1 replicas. Thus each request is known to a
quorum and the new view starts from a quorum.
VR also provides a way for nodes that have failed to
recover and then continue processing. This is important
since otherwise the number of failed nodes would eventually exceed the threshold. Correct recovery requires
that the recovering replica rejoin the protocol only after it knows a state at least as recent as its state when it
failed, so that it can respond correctly if it is needed for
a quorum. Clearly this requirement could be satisfied by
having each replica record what it knows on disk prior
to each communication. However we do not require the
use of disk for this purpose (and neither did the original
version of VR).
Thus, VR uses three sub-protocols that work together
to ensure correctness:
4.1

4.1  Normal Operation

This section describes how VR works when the primary
isn’t faulty. Replicas participate in processing of client
requests only when their status is normal. This constraint
is critical for correctness as discussed in Section 8.
The protocol description assumes all participating
replicas are in the same view. Every message sent from
one replica to another contains the sender’s current viewnumber. Replicas only process normal protocol messages containing a view-number that matches the viewnumber they know. If the sender is behind, the receiver
drops the message. If the sender is ahead, the replica
performs a state transfer: it requests information it is
missing from the other replicas and uses this information
to bring itself up to date before processing the message.
State transfer is discussed further in Section 5.2.
The request processing protocol works as follows. The
description ignores a number of minor details, such as
re-sending protocol messages that haven’t received responses.
• Normal case processing of user requests.
• View changes to select a new primary.
• Recovery of a failed replica so that it can rejoin the
group.
These sub-protocols are described in detail in the next
section.
• The configuration. This is a sorted array containing
the IP addresses of each of the 2f + 1 replicas.
• The replica number. This is the index into the configuration where this replica’s IP address is stored.
• The current view-number, initially 0.
• The current status, either normal, view-change, or
recovering.
Figure 3: Normal case processing in VR for a configuration with f = 1.
• The op-number assigned to the most recently received request, initially 0.
• The log. This is an array containing op-number
entries. The entries contain the requests that have
been received so far in their assigned order.
number, adds the request to the end of its log, updates the client’s information in the client-table, and
sends a hP REPARE O K v, n, ii message to the primary to indicate that this operation and all earlier
ones have prepared locally.
• The commit-number is the op-number of the most
recently committed operation.
5. The primary waits for f P REPARE O K messages
from different backups; at this point it considers
the operation (and all earlier ones) to be committed. Then, after it has executed all earlier operations
(those assigned smaller op-numbers), the primary
executes the operation by making an up-call to the
service code, and increments its commit-number.
Then it sends a hR EPLY v, s, xi message to the
client; here v is the view-number, s is the number
the client provided in the request, and x is the result
of the up-call. The primary also updates the client’s
entry in the client-table to contain the result.
• The client-table. This records for each client the
number of its most recent request, plus, if the request has been executed, the result sent for that request.
Figure 2: VR state at a replica.
1. The client sends a hR EQUEST op, c, si message to
the primary, where op is the operation (with its arguments) the client wants to run, c is the client-id,
and s is the request-number assigned to the request.
2. When the primary receives the request, it compares
the request-number in the request with the information in the client table. If the request-number s isn’t
bigger than the information in the table it drops the
request, but it will re-send the response if the request is the most recent one from this client and it
has already been executed.
6. Normally the primary informs backups about the
commit when it sends the next P REPARE message;
this is the purpose of the commit-number in the
P REPARE message. However, if the primary does
not receive a new client request in a timely way, it
instead informs the backups of the latest commit by
sending them a hC OMMIT v, ki message, where k
is commit-number (note that in this case commitnumber = op-number).
3. The primary advances op-number, adds the request
to the end of the log, and updates the information
for this client in the client-table to contain the new
request number, s. Then it sends a hP REPARE v, m,
n, ki message to the other replicas, where v is the
current view-number, m is the message it received
from the client, n is the op-number it assigned to
the request, and k is the commit-number.
7. When a backup learns of a commit, it waits until it has the request in its log (which may require
state transfer) and until it has executed all earlier
operations. Then it executes the operation by performing the up-call to the service code, increments
its commit-number, updates the client’s entry in the
client-table, but does not send the reply to the client.
4. Backups process P REPARE messages in order: a
backup won’t accept a prepare with op-number n
until it has entries for all earlier requests in its log.
When a backup i receives a P REPARE message, it
waits until it has entries in its log for all earlier requests (doing state transfer if necessary to get the
missing information). Then it increments its op-
Figure 3 shows the phases of the normal processing
protocol.
If a client doesn’t receive a timely response to a request, it re-sends the request to all replicas. This way
if the group has moved to a later view, its message will
reach the new primary. Backups ignore client requests;
only the primary processes them.
The protocol could be modified to allow backups to
process P REPARE messages out of order in Step 3. However there is no great benefit in doing things this way, and
it complicates the view change protocol. Therefore backups process P REPARE messages in op-number order.
The protocol does not require any writing to disk. For
example, replicas do not need to write the log to disk
when they add the operation to the log. This point is
discussed further in Section 4.3.
The protocol as described above has backups executing operations quickly: information about commits propagates rapidly, and backups execute operations as soon as
they can. A somewhat lazier approach could be used, but
it is important that backups not lag very far behind. The
reason is that when there is a view change, the replica
that becomes the new primary will be unable to execute
new client requests until it is up to date. By executing
operations speedily, we ensure that when a replica takes
over as primary it is able to respond to new client requests with low delay.
4.2
```
