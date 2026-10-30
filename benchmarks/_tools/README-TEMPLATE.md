# <Benchmark name>

**Family:** <related-work | popl-conf application | synthetic>  **Source:** <paper, file paths under
`related-work-papers/sources/`, or the TraceForge fixture>  **Expected verdict:** **CONFORMS** |
**DOES NOT REFINE** (<why, in one line>)

## 1. The protocol

What the program does, who the roles are (which are visible, which invisible), the messages
and their payloads, the parameters (N clients, K rounds, …) and the smallest interesting size.

## 2. The specification program

What the abstract program does: one server/oracle thread per independently served unit, how
the visible threads' sends and receives look from the outside, what it allows.

## 3. The implementation program

The distributed version: its threads, its messages between invisible threads, how it realises
the specification.

## 4. Where they differ, and why the verdict is what it is

For a conforming pair: why every visible behaviour of the implementation is one of the
specification's (what the invisible machinery hides). For a non-refining pair: the exact
behaviour the implementation has and the specification does not (the schedule, the operations,
the visible word or order that is uncovered), the smallest size at which it appears, and which
component of the check catches it (signature value, signature status, or order).

## 5. Variants and faults

Each variant in this directory: its name, how it differs, its expected verdict and reason.

## 6. Files

- `source/`: the original program(s) in the paper's language, copied verbatim, with provenance.
- `rust/`: the Must-API Rust crate (`spec`/`impl` sides, sizes, model) — see `_tools/run-rust.sh`.
- `java/`: the Must-API Java programs (`MpProgram`s) — see `_tools/run-java.sh`.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|

Commands used, exactly. Any disagreement is reported here, with the smaller tool's partial
counts, not reconciled by editing a program.
