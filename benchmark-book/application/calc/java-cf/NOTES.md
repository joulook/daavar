# calculator-disel: context-free Java twin

Class: `bench.calculator_disel.Calculator` (implements `MpProgram`, public `(String...)` constructor).

Argument string: `<spec|impl>,<K>,<fifo|bag>[,<variant>]`
- `K` = requests per client (2 clients), as the Rust `-- <K>`.
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- `variant` (impl side, default `batched` as in Rust `parse`): `batched`, `batched-swap`, `memo`,
  `memo-ignore-args`, `delegating`, `delegating-prev`. Ignored on the spec side (Rust ignores it too).
  An unknown variant is refused in the constructor (Rust panics in `imp`).

## Mapping

Messages (`sealed interface Msg extends MpMessage permits Req, Resp`):
- `Msg::Req { from: ThreadId, x: u8, y: u8 }` -> `@Tag("Req") record Req(Tid from, int x, int y)`
- `Msg::Resp { ans: u8, x: u8, y: u8 }` -> `@Tag("Resp") record Resp(int ans, int x, int y)`
- Rust tag-filtered receives (`TAG_REQ`, `TAG_RESP`) -> type-test blocking receives
  `Channel.recv(model, Req.class)` / `Channel.recv(model, Resp.class)`. All receives blocking, as in Rust.

Threads (main invisible; spawn order identical to Rust; main awaits clients, then servers, in Rust order):
- spec: `server` (N*K times: recv Req, send Resp(f(args), args) to `from`), then `c0`, `c1`.
- clients `c0`,`c1`: `me = self()`; for j in 0..K: send `Req(me, ARGS[(i+j)%2])` to the front, blocking recv Resp (value discarded).
  `ARGS = [(1,2),(2,2)]`, `f(x,y)=x+y`.
- impl `batched`/`batched-swap`: `batcher`, `c0`, `c1`. total/2 batches; each receives 2 Req, prepending
  (`acc.insert(0, r)`), then answers entry e to `from` of entry e (or of entry (e+1)%2 for `-swap`, fault F2).
- impl `memo`/`memo-ignore-args`: `memo`, `c0`, `c1`. Memo table as list of (x,y,value); hit = matching args,
  or (F1) the first entry regardless of args; on miss compute and append.
- impl `delegating`/`delegating-prev`: `memo` (memo_loop(total,false)), `delegator`, `c0`, `c1`. Delegator:
  recv Req, send `Req(me, x, y)` to memo, blocking recv Resp, out = ans (or, F3, previous ans if any),
  `last = Some(ans)`, send `Resp(out, x, y)` to the original requester.

Roles: every process is spawned with its Rust thread name as role; `rolesInSpawnOrder()` lists them per
variant (above); `visibleRoles()` = `c0`, `c1` (the Rust `visible`).
No `Nondet`: the Rust program makes no nondeterministic choice. No non-blocking receives.

## Measured (must, 0 iterations, timeout 600 s, counting mode)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,bag | 2 | 0 | stack exhausted |
| impl,1,bag,batched | 2 | 0 | stack exhausted |
| spec,1,fifo | 2 | 0 | stack exhausted |
| impl,1,fifo,batched | 2 | 0 | stack exhausted |
| impl,1,bag,batched-swap | 2 | 0 | stack exhausted |
| impl,1,bag,memo | 2 | 0 | stack exhausted |
| impl,1,bag,memo-ignore-args | 2 | 0 | stack exhausted |
| impl,1,bag,delegating | 2 | 0 | stack exhausted |
| impl,1,bag,delegating-prev | 2 | 0 | stack exhausted |
| spec,2,bag | 6 | 0 | stack exhausted |
| impl,2,bag,batched | 4 | 0 | stack exhausted |

(Not compared with any Rust count, as the brief requires.)

## Doubts / assumptions

1. `programName()` = `"calculator-disel"` and `size()` = `"K"` (e.g. `"1"`): I do not know what the Rust
   harness writes in the header (crate name is `bench_calculator_disel`; the variant may or may not be part of the size).
2. `fifo`->P2P and `bag`->ASYN is my reading of the model names; the Rust default model is bag, but the
   Java side requires the model explicitly (the contract gives it as a mandatory field).
3. Rust `parse` defaults K to 1 when missing/unparseable; Java requires K and refuses K < 1.
4. Server roles (`server`, `batcher`, `memo`, `delegator`) are given role names because a `Tid` of a
   server occurs in payloads (`Req.from` from the delegator) and the encoder needs a role name for it.
   They are invisible.
5. `u8` mapped to `int`; values stay below 5, so no overflow difference.
6. The memo table is a `List<int[]>` local to the process (not a message value), so its mutability is harmless.
