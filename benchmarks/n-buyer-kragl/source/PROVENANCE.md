# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-31 from
`plan/benchmarks/related-work-papers/sources/kragl-pldi20-artifact/inductive-sequentialization-pldi2020/boogie/Test/civl/inductive-sequentialization/`:

- `NBuyer.bpl` (681 lines, md5 `648b4e1772caaa864eb675a50fd62cda`)
- `NBuyer.bpl.expect` (Boogie's expected output: `127 verified, 0 errors`)

Origin: the Zenodo artifact of Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive
Sequentialization of Asynchronous Programs", PLDI 2020 (record 3754772,
`inductive-sequentialization-pldi2020.zip`, md5 `bf8bae64af4b8beaa8873fd0bc0bc79e`; a Boogie 2.6.4
snapshot). Catalogue record: `related-work-papers/sources/CATALOGUE-artifacts-2.md`,
"KIS/NBuyer.bpl"; candidate N20 in `CANDIDATES.md` §1b.

The program text the benchmark follows: the layer-1 procedures `main`, `sellerInit`,
`sellerFinish`, `firstBuyerInit`, `firstBuyer`, `middleBuyer`, `lastBuyer` (lines 452–573) over
the layer-0 channel primitives (`SEND_*`/`RECEIVE_*`, lines 592–654); the property is
`sellerFinish`'s `assert_sum` (`ASSERT_SUM`: `sum(contribution, 1, n) == price`); the
sequentialization the specification follows is `MAIN3`/`INV3` (the buyers in pid order).
