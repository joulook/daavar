# Interpreting results (draft 1, 2026-11-20)

## The verdict
A direction (implementation against specification, or one implementation against another) ends in one of:
- **conforms**: both exports complete, no uncovered behaviour (or every uncovered behaviour a Decide false alarm);
- **violation**: at least one uncovered behaviour that Decide confirms, against a complete reference (the checked
  export may be incomplete); or a program failure (a panic, a failed assertion) on the checked side, with the
  failure message as the witness;
- **incomplete**: the reference is not complete; or the checked export is incomplete (bounded, sampled, aborted) or
  timed out, and no violation is confirmed; or a candidate is unresolved and none is a violation;
- **refused**: a program or a tool rejected the run (a shape rule, a bad argument, an unsupported value), or the
  reference side failed; fix and rerun.
Directions of an implementation against the specification gate. A mutant's direction gates in reverse: a violation
means the mutant is rejected and counts as conforms, a conforming mutant counts as incomplete. Twin directions are
reported only; with no gating direction the result is REPORT ONLY (exit 2). The gating verdicts combine by
precedence, refused > violation > incomplete > conforms, and the check exits 0 for conforms, 1 for violation, 2 for
incomplete and refused (see running.md).

## Classifying uncovered behaviours
Every uncovered implementation behaviour is read with its witness graph and the map, and tagged with each class
that applies (a behaviour can carry more than one):
- **T, a violation of the text**: the behaviour contradicts a quoted sentence (a status item before the Task object
  the sentence requires first). Report it with the sentence, the source lines from the map, and the witness.
- **D, a difference**: another implementation does not produce it and the text permits it, or the specification
  excludes it only by a labelled restriction (one SDK answers a second cancel with an error, the other with the task;
  an error-only stream rejected by a restriction). Report it with both behaviours; it is under-specification only
  where the text is silent or ambiguous.
- **A, the specification is too narrow**: the text allows the behaviour and the specification program does not (a
  repeated Working status). Widen the specification, record the change as a revision, rerun.
- **C, a harness artefact**: the behaviour comes from the scenario or the port, not the protocol (an unread message at
  shutdown, a missing identifier normalisation). Fix the harness, rerun.
A behaviour rejected only by a labelled restriction of the specification program is listed, not reported.

## Decide
Decide re-examines each uncovered behaviour at the level of orders. It asks whether some linear extension of the
candidate's causal order extends none of the matching specification orders. If one does, the behaviour is a violation
and that extension is the witness. Otherwise it is a false alarm of the summary abstraction: several specification
summaries together cover all its extensions. An exhausted budget gives unresolved. A false alarm is removed from the
count; an unresolved candidate leaves the direction incomplete when no candidate is a violation.

## Twins
Twin cells list the differences with their class tags. Model acceptance alone establishes neither ambiguity nor a
defect of the text: a difference the specification accepts on both sides is a candidate for an under-specification
report after review, with the two readings and what each implementation does. When the specification rejects one,
that side's cell against the specification already reports it.

## The verification round
Before a finding is reported outside the project, an independent reader reproduces it: from the witness graph
(replayed with the tool) and from the source lines named in the map, without the author's notes. A finding that
does not survive this round is not reported.

## Writing the result
One table per scenario, a column per reading, a cell per direction with `uncovered/total` and the class counts; the
provenance line (tool commits, program checksums, sizes, model); a sentence per violation with the quoted text, the
source lines and the witness; the differences, as under-specification candidates after review; the labelled restrictions listed; what was not
checked (the scope: sizes, one request per task, no subscribe path). Numbers come from run outputs, never from memory.
