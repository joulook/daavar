# ewd998-termination: Java twin, written without context

Class: `bench.ewd998_termination.Ewd998Termination` (implements `MpProgram`, public `(String...)` constructor).
Argument string: `<spec|impl>,<N>,<fifo|bag>[,<variant>]`, variant `correct` (default) or `no-black-on-receive`
(the variant is validated on both sides, but only the impl uses it, as in the Rust `parse`).
Model: `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`; every send and every receive uses it.

## Mapping

Messages (`sealed interface Msg extends MpMessage`), tag = Rust variant name, fields in Rust order:

| Rust | Java |
|---|---|
| `Pl { from: u8 }` | `@Tag("Pl") record Pl(int from)` |
| `Idle { node: u8, sent: Option<u8>, got: u8 }` | `@Tag("Idle") record Idle(int node, Optional<Integer> sent, int got)` |
| `Tok { q: i8, black: bool }` | `@Tag("Tok") record Tok(int q, boolean black)` |
| `Trm` | `@Tag("Trm") record Trm()` |
| `Stop` | `@Tag("Stop") record Stop()` |

Threads. One inbox per thread = the thread's own `Tid`; a Rust `t[k].send_msg(m)` is
`Channel.send(model, tid_k, m)`, a Rust `rx.recv_msg_block()` is `Channel.recv(model)` (blocking, no guard,
then a type switch; the Rust `panic!` arms are `IllegalStateException`s, unreachable). No non-blocking receives
(the Rust program has none).

- `app<i>` (visible, both sides): `phase` = if nothing sent yet, `Nondet.chooseIndex(n)` (Rust `(0..n).nondet()`),
  `c > 0` sends `Pl(i)` to `det[(i+c)%n]`; then `Idle(i, sent, got)` to `report`. Then at most `n` blocking
  receives: `Trm` returns, `Pl(from)` sets bit `from` in `got` and runs `phase` again.
- spec `oracle` (invisible): keeps the latest report per node (`newer` = more bits in `got`), tests
  `terminated` (copied line for line), then `Nondet.chooseBoolean()` (Rust `nondet()`): true sends `Trm` to
  app0..app(n-1) then `Stop` to det0..det(n-1); either way returns.
- spec `det<j>` (stub, invisible): `Pl(from)` -> forward `Pl(from)` to `app<j>`; `Stop` returns.
- impl `det<i>` (invisible): Safra's rules line for line (counter, black, outstanding, sentCounted, token,
  rounds, `R = 2`); `Option<(i8,bool)>` token held as a nullable local `Tok` (local state only, never sent).
  `no-black-on-receive` drops `black = true` on a received `Pl`.
- spawn order: spec `oracle, det0.., app0..`; impl `det0.., app0..`. Main spawns all, then awaits all in
  spawn order (Rust joins `hs` in order). Main does no message-passing event.
- Roles: every thread gets its Rust thread name as role (`oracle`, `det<j>`, `app<i>`); `rolesInSpawnOrder`
  lists them all; `visibleRoles` = `app0..app(n-1)` (Rust `visible`).

Circular addressing: the threads need each other's ids before any runs, so main constructs every
`MpThread(body, role)` first (in spawn order; the constructor mints the task id), reads the ids into arrays
the bodies capture, then calls `start()` on each in the same order, then `join1()` on each. I used `MpThread`
directly instead of `MpProcess.spawn(role, body)` because `spawn` starts at once and `Handle`'s constructor is
private.

## Doubts and assumptions

1. **`Optional` in `Idle.sent` is not encodable by `MpValueEncoder`** as I read it (it accepts records, lists,
   sets, integers, booleans, strings, enums, `Tid`; anything else is refused). The brief says `Optional` for
   `Option`, so I followed the brief. The plain enumeration does not encode values, so the counts are
   unaffected, but an export or an online check would likely fail on the first `Idle` send (a visible event).
   I do not know the Rust side's encoding of `Option` (`{"t":"Some","f":[x]}` / `{"t":"None","f":[]}`?).
2. **N = 1**: Java `Nondet.chooseIndex(1)` emits no event; I do not know whether the Rust `(0..1).nondet()`
   emits an ND node. If it does, N = 1 graphs differ structurally. N = 2 is the smallest size where every
   choice is a real choice.
3. `programName()` = `ewd998-termination` and `size()` = `"<N>"`: guesses; the Rust program states no name
   or size string in main.rs.
4. `Nondet.chooseBoolean()` orders `true` first; I do not know the Rust `nondet()` bool order (affects only
   exploration order, not the set of graphs).
5. Invisible threads carry roles (`oracle`, `det<j>`) because Rust names them; assumed allowed since
   `rolesInSpawnOrder` is "every process main spawns" and only `visibleRoles` are compared.
6. Rust `u8`/`i8` arithmetic mapped to `int`; no overflow is reachable at small N (counter, q in [-N, N]).
7. Rust's default `N = 3` when the size is missing is not reproduced: the Java argument string requires N.

## Counts measured (`must 0 1 report - 600`, plain enumeration, stack exhausted in every run)

| args | complete | blocked |
|---|---|---|
| `spec,1,bag` | 1 | 1 |
| `impl,1,bag` | 2 | 0 |
| `spec,2,bag` | 80 | 80 |
| `impl,2,bag` | 414 | 902 |
| `impl,2,bag,no-black-on-receive` | 750 | 376 |
| `spec,2,fifo` | 26 | 26 |
| `impl,2,fifo` | 160 | 516 |

No process failure (`IllegalStateException`) was logged in any run; exit status 0. The counts were not
compared with any Rust number.
