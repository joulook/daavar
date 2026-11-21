# Concepts (draft 1, 2026-11-20)

## Programs, processes, messages
A Daavar program is a closed system of processes that communicate only by messages. Each process has a role name,
local state, and a body that receives, computes and sends. Two language bindings exist: Rust programs run under
TraceForge, Java programs under JMC. Both tools explore every order in which messages can be delivered and every
value a `nondet` choice can take, so one program stands for a set of executions.

A message is an immutable value with a tag and fields; the format document (`format/SPEC.md`) lists the field kinds
both tools encode. Sends never block; receives block until a matching message is in the process's mailbox, and may
select by a guard. In programs that are compared, a guard is a type test only: JMC accepts any predicate over the
message and TraceForge one over sender and tag, so a field test has no counterpart in the other tool. A non-blocking
receive (Rust `recv_msg`, Java `Channel.tryRecv`) returns a message or ⊥ ("nothing"). It may return ⊥ even when a
message is pending; both explorations include that choice. It is not a test for an empty mailbox.

## Communication models
The delivery discipline is a parameter of the run: `fifo` (per-link order; `p2p` in JMC's spelling), `bag`
(unordered; `asyn`), `causal` (`cd`). `causal` is Rust only: JMC's `must` supports `p2p` and `asyn`, and the driver
accepts `causal` only for Rust programs. The check compares programs run under the same model; the headers record it.

## Visible roles, words, statuses
The check does not compare whole executions. It projects each execution onto the visible roles: the sequence of their
sends and receives (the role's **word**; a send records the value sent, not its destination), the terminal
**status** of each visible role (`done`, `blocked` or `err`), and the causal order among the visible events. Waiting
is not a separate status. Neither tool writes `err` at the pinned commits: a failed execution produces no summary
line. The causal order is recorded as **counts**: for each event of a visible role, how many events of each other
visible role causally precede it, including paths through invisible events. Roles that are not visible are
explored but not compared: an executor behind a server, an environment process, a store.

## Specification and implementation
Both are programs. The specification program accepts every behaviour the text allows: where the text leaves a point
open (a SHOULD, a silence, two readings of a sentence), the specification branches with `nondet`. The implementation
program is a faithful port of the code under check. **Conformance** means: for every implementation summary (words,
statuses, counts) there is a specification summary, for the same scenario and the same model, with identical words
and statuses and counts componentwise ≤ the implementation's. Equal words and statuses are required; the counts need
not be equal, because a specification that orders fewer events allows more interleavings. The relation is
one-directional; the reverse direction (does the implementation realise every allowed behaviour) is not required and
not checked.

## Offline and Online
**Offline**: export the specification's behaviour set once, export the implementation's, and join them: the result is
the set of implementation behaviours with no specification match, each a candidate; an execution graph may be
available for it (see Witnesses). The join can run Decide, a
post-pass that confirms each uncovered behaviour as a violation, marks it a false alarm of the summary abstraction,
or leaves it unresolved when its budget runs out.
**Online**: run the implementation against an exported specification set and look each explored behaviour up in it,
optionally stopping at the first miss. A miss is a candidate, which Decide may still find a false alarm. A standalone
Online check needs no implementation export; the driver nevertheless writes one, with graphs, and joins it with
Decide. Twin comparisons use offline exports in both modes (see running.md).

## Complete, bounded, sampled, aborted
An export is **complete** when the tool explored the whole behaviour set of the program at that size. The header
marks other exports `bounded` (a budget reached; JMC also after a stop at the first miss), `sampled` (a random
strategy) or `aborted` (a failure, a timeout or threshold inside the tool, TraceForge's stop at the first miss). This
enumeration field is metadata of the export. A timeout of the driver, which kills the run, is not an enumeration
value; it makes the direction incomplete. A check against an incomplete
reference proves nothing and the driver reports it as incomplete; an incomplete checked export can still show a
violation, but not conformance.

## Twins
Two implementations of the same specification (the Java SDK and the Rust SDK of one protocol) are joined against each
other as well. A behaviour of one that the other never produces is a **difference**. When the specification accepts
both, model acceptance alone establishes neither ambiguity nor a defect of the text: a difference the specification
accepts on both sides is a candidate for an under-specification report after review. Labelled restrictions exclude
behaviours. When the specification rejects one, that is a violation of the text only when a quoted sentence forbids
the behaviour.

## Witnesses
An uncovered behaviour can come with its execution graph: the events of every process with the message edges, taken
from the graph store the export wrote. A graph can be missing: collection was limited or absent, the graph was
refused, or Decide found the behaviour a false alarm (false alarms get no graph). The witness index records the reason
(`limit`, `false-alarm`, `no-store`, `no-graph`, `refused`). The graph can be rendered (DOT or TikZ) and replayed by
the tool that produced it, from the same sources and the same tool commit; the replay re-executes the program along
the recorded choices and stops at the first deviation.
