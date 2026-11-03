# Excerpt: Glenn Ricart, Ashok K. Agrawala, *An Optimal Algorithm for Mutual Exclusion in Computer Networks*, CACM 24(1), 1981, section 2 (Algorithm: 2.1 Description, 2.2 Specification)

Verbatim text (pdftotext of a scanned original: a few characters are misread, e.g. `:~` for `:=`, `t h a n` for `than`; the figure of section 2.3 is omitted).

```
2. Algorithm
2.1 Description
A node enters its critical section after all other nodes
have been notified of the request and have sent a reply
granting their permission. A node making an attempt to
invoke mutual exclusion sends a REQUEST message to
all other nodes. Upon receipt of the REQUEST message,
the other node either sends a REPLY immediately or
defers a response until after it leaves its own critical
section.
The algorithm is based on the fact that a node
receiving a REQUEST message can immediately determine whether the requesting node or itself should be
allowed to enter its critical section first. The node originating the REQUEST message is never told the result of
the comparison. A REPLY message is returned immediately if the originator of the REQUEST message has
priority; otherwise, the REPLY is delayed.
The priority order decision is made by comparing a
sequence number present in each REQUEST message.
If the sequence numbers are equal, the node numbers
are compared to determine which will enter first.
2.2 Specification
The network consists of N nodes. Each node executes
an identical algorithm but refers to its own unique node

Each node has three processes to implement the
mutual exclusion:
! TRUE when we cannot reply immediately
Highest_Sequence_Number :~
(l) One is awakened when mutual exclusion is invoked
Maximum (Highest_Sequence_Number, k);
P (Shared_vars);
Defer it :=
Requesting_Critical_Section
AND ((k > Our_sequence_Number)
OR (k = Our_Sequence_Number ANDj > me));
V (Shared_vars);
Comment Defer_it will be TRUE if we have priority over
node j ' s request;
IF Defer it THEN Reply_Deferred[j] := TRUE ELSE
Send_Message (REPLY, j);
on behalf of this node.
(2) Another receives and processes REQUEST messages.
(3) The last receives and processes REPLY messages.
The three processes run asynchronously but operate
on a set of common variables. A semaphore is used to
serialize access to the common variables when necessary.
If a node can generate multiple internal requests for
mutual exclusion, it must have a method for serializing
those requests.
The algorithm is expressed below in an Algol-like
language.
SHARED DATABASE
CONSTANT
me, ! This node's unique number
N; ! The number o f nodes in the network
INTEGER
Our_SequenceNumber,
! The sequence number chosen by a request
! originating at this node
H i g h e s t S e q u e n c e N u m b e r initial (0),
! The highest sequence number seen in any
! REQUEST message sent or received
Outstanding_Reply_Count;
! The number of REPLY messages still
! expected
BOOLEAN
Requesting Critical_Section initial (FALSE),
! TRUE when this node is requesting access
! to its critical section
Reply_Deferred [I:N] initial (FALSE);
! Reply_Deferred [j] is TRUE when this node
! is deferring a REPLY t o j ' s REQUEST message
BINARY S E M A P H O R E
Shared vars initial (1);
! Interlock access to the above shared
! variables when necessary
P R O C E S S W H I C H INVOKES M U T U A L EXCLUSION FOR
THIS NODE
Comment Request Entry to our Critical Section;
P (Shared_vats)
Comment Choose a sequence number;
RequestingCritical_Section := TRUE;
Our_Sequence_Number := Highest_Sequence_Number + l;
V (Shared_vars);
Outstanding_ReplyCount := N - l;
F O R j := I STEP l UNTIL N DO I F j # me THEN
Send_Message(REQUEST(Our_Sequence_Number, me),j);
Comment sent a REQUEST message containing our sequence number and our node number to all other nodes;
Comment Now wait for a REPLY from each of the other nodes;
WAITFOR (Outstanding_Reply_Count = 0);
Comment Critical Section Processing can be performed at this point;
Comment Release the Critical Section;
RequestingCritical_Section := FALSE;
FOR j := l STEP 1 UNTIL N DO
IF Reply_Deferred[j] THEN
BEGIN
Reply_Deferred[j] := FALSE;
Send_Message (REPLY, j);
Comment send a REPLY to node j;
END;
P R O C E S S W H I C H RECEIVES R E Q U E S T (k, j ) M E S S A G E S
Comment k is the sequence number begin requested,
j is the node number making the request;
11
BOOLEAN Defer it
P R O C E S S W H I C H RECEIVES REPLY M E S S A G E S
Outstanding_Reply_Count := Outstanding_Reply_Count - 1;
The REPLY processing can be represented by a decision
table:
Rule number
Condition and action entries
1
2
Receiving node is also requesting the resource
N
Y
Received message's sequence
number compared to ours
3
4
Y
Y
I
I
I
1
Received message's node number
compared to ours
Send REPLY back
x
x i
x
x!
Defer the REQUEST
k
2.3 Example
```
