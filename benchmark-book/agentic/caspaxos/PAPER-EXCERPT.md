# Excerpt: Denis Rystsov, *CASPaxos: Replicated State Machines without logs*, arXiv:1802.07000v9 (16 May 2018), sections 2.1 and 2.2

Verbatim text of the two sections that define the protocol (extracted with pdftotext -layout; page headers and two footnote lines removed; the two-column Synod/CASPaxos comparison keeps its layout). The register's clients, proposers and acceptors are the roles.

```
2.1        Synod
An implementation of the Synod protocol is an initializable-only-once distributed register.
When several clients try to initialize it concurrently - at most one client succeeds. Once a
client receives a confirmation, all the follow-up initializations must return the already chosen
value.
    The register belongs to the CP category of the CAP theorem5 , gives up availability when
more than ⌊ N 2−1 ⌋ nodes are down and always preserves consistency (linearizability).
    Each node in the system plays the role either of client, proposer or acceptor.

Clients initiate a request by communicating with a proposer; clients may be stateless, the
    system may have arbitrary numbers of clients.
Proposers perform the initialization by communicating with acceptors. Proposers keep
    minimal state needed to generate unique increasing update IDs (ballot numbers), the
    system may have arbitrary numbers of proposers.


    https://tschottdorf.github.io/single-decree-paxos-tla-compare-and-swap, https://medium.com/@grogepodge/tla-specification-for-gr
    https://en.wikipedia.org/wiki/CAP_theorem


       Acceptors store the accepted value; the system should have 2F + 1 acceptors to tolerate F
          failures.

          It’s convenient to use tuples as ballot numbers. To generate it a proposer combines its
       comparable ID with a local increasing counter: (counter, ID). To compare ballot tuples, we
       should compare the first component of the tuples and use ID only as a tiebreaker.
          When a proposer receives a conflicting message from an acceptor, it should update its
       counter to avoid a conflict in the future.

       2.2    CASPaxos
       An implementation of CASPaxos is a rewritable distributed register. Clients change its
       value by submitting side-effect free functions which take the current state as an argument
       and yield new as a result. Out of the concurrent requests only one can succeed, once a client
       gets a confirmation of the change it’s guaranteed that all future states are its descendants:
       there exists a chain of changes linking them together.
           Just like with Synod, it’s a CP-system, and it requires 2F + 1 nodes to tolerate F failures.
       Also, it uses the same roles: clients, proposers, and acceptors, and a very similar two-phase
       state transition mechanism.
           Let’s review the Synod and CASPaxos protocols step-by-step.


                 Synod                                      CASPaxos
                 A client proposes the val0 value to a      A client submits the f change func-
                 proposer.                                  tion to a proposer.
                 The proposer generates a ballot num-       The proposer generates a ballot num-
                 ber, B, and sends "prepare" messages       ber, B, and sends "prepare" messages
                 containing that number to the accep-       containing that number to the accep-
                 tors.                                      tors.
                 An acceptor                                An acceptor

                 Returns a conflict if it already saw a     Returns a conflict if it already saw a
                 greater ballot number.                     greater ballot number.

                 Persists the ballot number as a            Persists the ballot number as a
                 promise and returns a confirmation ei-     promise and returns a confirmation ei-
                 ther with an empty value (if it hasn’t     ther with an empty value (if it hasn’t
                 accepted any value yet) or with a tu-      accepted any value yet) or with a tu-
                 ple of an accepted value and its ballot    ple of an accepted value and its ballot
                 number.                                    number.

                 The proposer                               The proposer

                 Waits for the F + 1 confirmations          Waits for the F + 1 confirmations

                 If they all contain the empty value,       If they all contain the empty value,
                 then the proposer defines the current      then the proposer defines the current
                 state as val0 otherwise it picks the       state as ∅ otherwise it picks the value
                 value of the tuple with the highest bal-   of the tuple with the highest ballot
                 lot number.                                number.



          Sends the current state along with the    Applies the f function to the current
          generated ballot number B (an "ac-        state and sends the result, new state,
          cept" message) to the acceptors.          along with the generated ballot num-
                                                    ber B (an "accept" message) to the
                                                    acceptors.

          An acceptor                               An acceptor

          Returns a conflict if it already saw a    Returns a conflict if it already saw a
          greater ballot number.                    greater ballot number.

          Erases the promise, marks the re-         Erases the promise, marks the re-
          ceived tuple (ballot number, value) as    ceived tuple (ballot number, value)
          the accepted value and returns a con-     as the accepted value and returns a
          firmation                                 confirmation

          The proposer                              The proposer

          Waits for the F + 1 confirmations         Waits for the F + 1 confirmations.

          Returns the current state the client.     Returns the new state to the client.




    As we see, the CASPaxos’s state transition is almost identical to the Synod’s initializa-
tion, and if we use

                                 x → if x = ∅ then val0 else x
    as the change function then it indeed becomes identical.
    By choosing a different set of change functions, we can turn CASPaxos into a distributed
register and atomically change its value with arbitrary user-defined function in one operation.
For example, the following change functions implement a register supporting CAS.

   To initialize a register with val0 value

                                 x → if x = ∅ then (0, val0 ) else x

   To update a register to value val1 if the current version is 5

                               x → if x = (5, ∗) then (6, val1 ) else x

   To read a value
                                                   x→x

   With this specialization, the protocol is very similar to Bizur[6].

```
