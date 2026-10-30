# Provenance

Copied verbatim (cp -p, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/gokv/`: `bank/bank.go`, `lockservice/lock_clerk.go`,
`kv/interface.go` (156 lines), into the same relative paths under `source/`.

- Repository: https://github.com/mit-pdos/gokv.git
- Commit: `d007325b6ee8a52ba16b68facd07cba902214b3f` (2025-05-08)
- Catalogue record: `related-work-papers/sources/CATALOGUE-grove-civl.md`, "gokv/bank +
  gokv/lockservice + gokv/kv"; candidate N10 in `CANDIDATES.md` §1.

The program text the benchmark follows: `transfer_internal` (lock both accounts, `Get(from)`,
and if the balance suffices `Put(from, old-amount)`, `Put(to, Get(to)+amount)`, unlock both),
`acquire_two` (unordered: `Lock(l1); Lock(l2)`, the one `transfer_internal` calls, marked
"FIXME: need to add string lt to operational semantics") and `acquire_two_good` (ordered by
`l1 < l2`), `release_two`, `get_total` (lock and read every account in sorted order, then unlock
all), `SimpleAudit` (`panic` unless the total is `BAL_TOTAL`), and `LockClerk.Lock`
(`for ck.kv.ConditionalPut(key, "", "1") != "ok" {}`) / `Unlock` (`ck.kv.Put(key, "")`). The
Perennial proof and the bank's Coq specification are not on disk.
