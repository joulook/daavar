# The prompt given to the two agents (recorded verbatim, 2026-11-02, the CASPaxos prompt with the protocol and the entry names replaced)

Two agents, each a fresh session with no project context, received the text below. The only difference
between the two is the LANGUAGE block. The agent could read the listed files and nothing else; it could
compile its program and run it once in plain exploration to see that it starts and ends; it was not told
what the program is for beyond the text below.

---
Implement the Flexible Paxos single-decree consensus described in PAPER-EXCERPT.md as a message-passing program,
following the program interface in INTERFACE.md exactly (roles, names, parameters, the two client
messages, the program entry).

Write it in LANGUAGE, using the message-passing API whose sources are listed under API. Processes are the
API's threads/processes, every exchange between processes is a message sent with the API's send and
received with its receive operations, and any choice the protocol leaves open (which message to process
first, when to give up) is made with the API's choice operation where a choice is needed, otherwise by
the order in which messages arrive.

Keep to the paper: the two phases with their separate quorums Q1 and Q2 (section 4.1), the proposal
numbers, the acceptor's promised and accepted state, how a proposer picks the value for phase 2, and
what it does when a phase does not reach its quorum. Where the paper is silent, decide
yourself and write the decision into NOTES.md next to your code, with the paper's sentence that left it
open.

Deliverables, written only under OUT_DIR:
- the program (one source file, plus a build file if the language needs one),
- NOTES.md: the mapping from the paper's steps to your code (role by role, message by message), every
  decision you made where the paper was silent, and the command you used to compile.

Compile the program. You may run it once in plain exploration with the smallest parameters to see that it
starts and terminates; record the command and its last output line in NOTES.md. Do not read any file other
than those listed, and do not search the web.
---

LANGUAGE (Java agent): Java 17, against JMC's message-passing API.
API (Java agent): daavar/jmc/core/src/main/java/org/mpi_sws/jmc/api/mp/ (MpThread, MpProcess, Channel, Nondet,
Monitors), daavar/jmc/core/src/main/java/org/mpi_sws/jmc/mp/events/ (MpMessage, Tid, CommunicationModel),
daavar/jmc/core/src/test/java/org/mpi_sws/jmc/mp/launch/MpProgram.java and MpLauncher.java,
daavar/jmc/core/src/main/java/org/mpi_sws/jmc/mp/summary/Tag.java and MpValueEncoder.java.
Compile: javac -d java/out -cp "$(cat daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*');
run: java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.fpaxos.FPaxos:impl,4,2,bag' must 0 1 report - 600 </dev/null

LANGUAGE (Rust agent): Rust 2021, against TraceForge's Must API.
API (Rust agent): daavar/traceforge/traceforge/src/lib.rs (the exported functions: send_msg, recv_msg_block,
recv_tagged_msg_block, nondet, thread::spawn, ThreadId, Channel), daavar/traceforge/traceforge/src/program.rs
(Program, main_with_model), and one example of a crate using them: daavar/benchmarks/_tools/rust-template/.
Cargo.toml: [dependencies] traceforge = { path = "../../../traceforge/traceforge" } and [workspace] (as the template).
Compile: cargo build --release -j 2; run: target/release/bench_fpaxos count impl --model bag -- 4 2
