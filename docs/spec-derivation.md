# Deriving the specification program (draft 1, 2026-11-20)

## From a natural-language specification
1. Collect the sentences that constrain the scenario: the lifecycle of the object (a task, a request), the stream or
   response patterns, cancellation, errors. Quote each one with its section number in `SPEC-DESIGN.md`, beside the
   program fragment that encodes it. A sentence you did not quote is a sentence you did not encode.
2. MUST and MUST NOT restrict: encode them as the only transitions the specification process takes. SHOULD, MAY,
   and every point the text does not mention become `nondet` branches: the specification accepts both outcomes.
3. When a sentence admits two readings (does "the stream MUST begin with the Task object" bind the first item of
   every stream, or of the first stream of a task?), do not choose silently: make the reading a program argument
   and run the check under each. The results table is indexed by reading; a behaviour that is a violation under one
   reading and allowed under the other is reported as under-specification with both outcomes shown.
4. Where the specification program must restrict something the text does not state (to keep the behaviour set
   finite, or to exclude a harness artefact), label the restriction (`R3`), list it, and treat behaviours rejected
   only by a labelled restriction as "not a finding".
5. Test the specification's strength with mutants: deliberately wrong implementation programs that the check must
   reject. A specification that accepts a mutant is too wide.

## From TLA+
The specification process is the client-visible projection of the actions: each action that sends or receives a
protocol message becomes a step; the state variables become local variables; the invariants become assertions or
disappear if they are not observable. Quantified next-state relations become `nondet` over the finite set at the
chosen size. Fairness and liveness are not checked by a finite-execution conformance check; record what was dropped.

## From a P model
Machines become processes, events become messages, `receive` cases become the receive-and-step structure, and
`goto` state changes become local state. P's nondeterministic choice maps directly to `nondet`.

## From a reference implementation
When the only specification is another implementation (a Python SDK, the authors' prototype), the specification
program is a port of it under the modelling guide, with two differences from an ordinary port: its accidental
restrictions (an arbitrary order of two independent sends, a buffer size) are replaced by `nondet` and listed as
"widened", and the port is reviewed against the text where a text exists. Say in the results that the reference is
an implementation, not a text.

## Rust or Java
Write the specification in Rust when you can: TraceForge's exploration is often faster, depending on the workload,
and its stateful exploration engine exists only there. The join (`conf-join`, with the merge join for large sets and
Decide) is part of TraceForge but reads either tool's exports, so a complete Java specification gets the same join,
merge and Decide. A Java specification also works with the Online check; use it when the team's language is Java.

## What a specification program must come with
`SPEC-DESIGN.md`: the quoted sentences, the readings and their arguments, the labelled restrictions, the mutants and
their outcomes, the sizes explored and their counts, and the revision history of the program.
