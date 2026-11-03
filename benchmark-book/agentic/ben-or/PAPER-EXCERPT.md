# Excerpt: Michael Ben-Or, *Another Advantage of Free Choice: Completely Asynchronous Agreement Protocols (Extended Abstract)*, PODC 1983, sections 2 (The Consensus Problem) and 3 (A Consensus Protocol)

Verbatim text (pdftotext of a scanned original: characters are misread in places, e.g. `N - - t` is N − t, `r :-- 1` is r := 1, `zp` is the initial value x_p, `t]`/`tl` are cut line ends; the two variants of step 3 printed one after the other are the protocol's step 3 (a)–(c) for the two theorems; use the first).

```
2. The Consensus Problem
A set of /V asynchronous processes wish to
agree about a binary value. Each process P starts
with a binary input zp, and they all must decide
on a common value. The trivial solution, say,
0 is always chosen, is ruled out by the following
correctness criterion:
(C1) If for all P, z p = v, then the decision must
be v."
A process "decides" by setting a "write-once" output register to be 0 or 1. Thus after deciding, a
process may no longer change its decision.
To reach agreement processes communicate by
means of messages. A message is a pair (P,m),
where P is the name of the destination of the
message and m is its content. The message system
maintains a message buffer M that contains all the
messages send but not yet delivered.
A process P can send the message m to process
Q by performing send(Q, m). This operation adds
the message (Q, m) to the message buffer. Process
R e s e a r c h s u p p o r t e d b y a W e i l m a n n P o s t d o c t o r a l fellowhip and by NSF grant MCS-8006938.
¢rmission to copy without fee all or part of this material is granted
rovided that the copies are not made or distributed for direct
3mmercial advantage, the ACM copyright notice and the title of the
ublication and its date appear, and notice is given that copying is by
ermission of the Association for Computing Machinery. To copy
therwise, or to republish, requires a fee and/or specific permission.
©
1983ACM 0-89791-110-5/83/008/0027
$00.75
27
an infinite run of the system.
A schedule is t-correct if on any infinite run
at most t prodesses make a finite number of step,,
and any message is eventually delivered if th
receiving process makes an infinite number c
steps. Thus the only failure allowed is a proce~

3. A Consensus Protocol
In this section we present a simple probabilist!
consensus protocol. In this protocol the processc
perform "rounds" of exchange of information. 0
each round, if some process decides v, then by t[
next round all the other operating processes wi
decide the same value v. If no process decides the
with some bounded positive probability all tl
operating processes will reach agreement on tl
next round. The round number r is attached I
the messages of round r, so the processes can di
tinguish between messages from different round
A - - Consensus Protocol
Process P: Initial value z p .
step 0: set r :-- 1.
step 1: Send the message (1, r, zp) to all t]
processes.
step 2: Wait till N - - t messages of type (1, r,
are received. If more t h a n N / 2 messages have t]
same value v, then send the message (2, r, v, D)
all processes. Else send the message (2, r, ?) to~
processes.
step 3: Wait till N - - t messages of type (2, r,
arrive.
(a) If there is one D-message (2, r, v, D) then E
X p :--- D.
(b) If there are more t h a n t D-messages, th,
decide v.
(c) Else set x p -~- 1 or 0 each with probability
step 4: Set r :~- r -t- 1 and go to step 1.
(a) ]f there are at least t-}-i D-messages (2, r, v, D),
then set xp : ~ v.
(b) If there are more than ( N + t)/2 D-messages
then decide v.
(c) Else set xp - ' - - 1 or 0 each with probability 51"
step 4: Set r :-- r ~ 1 and go to step 1.
Theorem 1. Let N ~ 2t. For any t-correct
```
