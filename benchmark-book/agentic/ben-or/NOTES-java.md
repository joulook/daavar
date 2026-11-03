# Ben-Or consensus — Java (JMC message-passing API)

Source: `java/bench/ben_or/BenOr.java`, public class `bench.ben_or.BenOr` implementing
`org.mpi_sws.jmc.mp.launch.MpProgram`, constructed from `impl,<N>,<fifo|bag>`
(`fifo` = `CommunicationModel.P2P`, `bag` = `CommunicationModel.ASYN`; every send and receive uses it).
Constants: t = `T` = 1, R = `R` = 2.

## Mapping from the paper to the code

Roles
- Main (`run`): creates the processes p0..p{N-1}, then c0..c{N-1} (`new MpThread(body, role)`), starts
  them in that order, awaits all of them (`join1`). It sends and receives nothing.
- Client ci (`client`): `send(pi, Input(i, i % 2))`, then one blocking receive of `Decide` or `Undecided`.
- Process pi (`process`): receives its `Input` (x_p := value, reply address = c{from}), then runs rounds.

Messages
- `Input(from, value)`, `Decide(value)`, `Undecided()`: the fixed client messages.
- `Type1(r, x)`: the paper's (1, r, x_p).
- `Type2(r, v, d)`: the paper's (2, r, v, D) when `d` is true (a D-message), (2, r, ?) when `d` is false
  (`v` = `NO_VALUE` = -1).

Steps (in `process`)
- step 0 / step 4: `for (r = 1; r <= R; r++)`.
- step 1: send `Type1(r, x)` to every process p0..p{N-1}.
- step 2: N − t blocking selective receives of `Type1` with round r; count the values; if a value v has
  `2*count[v] > N` send `Type2(r, v, true)` to all, else `Type2(r, -1, false)` to all.
- step 3: N − t blocking selective receives of `Type2` with round r; count the D-messages.
  (a) at least one D-message (2, r, v, D): x := v. (b) more than t D-messages: decide v, i.e. send
  `Decide(v)` to the client and end. (c) else x := `Nondet.chooseIndex(2)` (the coin flip as the API's
  choice operation; options 0, 1).
- After round R without a decision: send `Undecided()` to the client and end.

Which message a receive takes is left to the strategy (selective receive with a guard on type and
round); no explicit choice operation is needed for it.

## Decisions where the paper is silent

1. "Send the message (1, r, x_p) to all the processes" — whether "all" includes the sender. Decision:
   yes; a process sends to itself through the channel like to any other process, and its own message
   counts among the N − t.
2. "Wait till N − t messages of type (1, r, *) are received" — what happens to the other messages of
   round r and to messages of later rounds. Decision: the receive is selective on the round, so messages
   of later rounds stay pending until that round; messages of round r beyond the first N − t are never
   received (ignored).
3. "If there is one D-message (2, r, v, D) then x_p := v" — read as "at least one". All D-messages of a
   round carry the same v (a strict majority of N − t ≤ N values can only be one value), so the value of
   any D-message received is used.
4. "Else set x_p := 1 or 0 each with probability 1/2" — also in round R (the coin is part of step 3, and
   round R ends after step 3), even though the value is not used afterwards.
5. "(b) If there are more than t D-messages, then decide v." — the paper says what deciding is (writing
   a write-once register) but not whether the process continues. INTERFACE.md fixes it: the process
   replies `Decide(v)` and ends. Consequence (not changed): a process still running may wait for N − t
   messages of a round that the decided processes no longer send, and block.
6. "More than N/2 messages have the same value v" — tested as `2 * count > N` over the N − t messages
   received (integer arithmetic, no rounding).
7. Addresses: INTERFACE.md says every process knows every other's address from the start. Main creates
   all six threads (minting their ids) before starting any, so every body sees the full address arrays;
   with `MpProcess.spawn` (create-and-start) a process could otherwise reply before main had recorded its
   client's id. The `Tid`s carry the role name, as `MpProcess.spawn` does.
8. Program name `ben_or`, size `N`; roles p0..p{N-1}, c0..c{N-1}; visible roles c0..c{N-1}.

## Compile and the one run

From this directory (`daavar/benchmarks-cf2/ben-or/java`; the classpath file is `daavar/jmc/cp.txt`):

    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: compiled without errors or warnings.

Run (once, plain exploration, smallest parameters):

    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.ben_or.BenOr:impl,3,bag' must 0 1 report - 600 </dev/null

It started and stopped on the 600 s timeout (exit 0): 68637 iterations, 68637 distinct complete graphs,
0 blocked, "stack not exhausted" (the exploration did not complete). Last output line:

    max_porf_count: 0

`java/out` and `report/` were removed afterwards.
