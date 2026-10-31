# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-31 from
`plan/benchmarks/related-work-papers/sources/kragl-pldi20-artifact/inductive-sequentialization-pldi2020/boogie/Test/civl/inductive-sequentialization/`:

- `2PC.bpl` (553 lines, md5 `950ab2f8c16480db9fe43beb5c86d819`)
- `2PC.bpl.expect` (Boogie's expected output: `69 verified, 0 errors`)

Origin: the Zenodo artifact of Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive
Sequentialization of Asynchronous Programs", PLDI 2020 (record 3754772,
`inductive-sequentialization-pldi2020.zip`, md5 `bf8bae64af4b8beaa8873fd0bc0bc79e`; a Boogie 2.6.4
snapshot). Catalogue record: `related-work-papers/sources/CATALOGUE-artifacts-2.md`,
"KIS/2PC.bpl"; candidate N21 in `CANDIDATES.md` §1b. Not the same protocol as the current Boogie
checkout's `samples/2pc.bpl`.

The program text the benchmark follows: the layer-1 procedures `main` (line 332),
`participant1`, `participant2`, `coordinator1`, `coordinator2` (lines 358–460) over the layer-0
primitives `SEND_REQ`/`RECEIVE_REQ`, `SEND_VOTE`/`RECEIVE_VOTE`, `SEND_DEC`/`RECEIVE_DEC`
(lines 480–517); the specification is the top-layer atomic action `MAIN5` (line 61).
