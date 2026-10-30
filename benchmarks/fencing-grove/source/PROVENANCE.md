# Provenance

Copied verbatim (cp -rp, no edits) on 2026-10-30: the whole directory
`plan/benchmarks/related-work-papers/sources/gokv/fencing/` as `source/fencing/` (`client/`,
`config/`, `ctr/`, `frontend/`, `loopclient/`, `cmd/`, `notes.txt`, `test.py`; 711 lines of Go
excluding the generated `ctr/0_marshal.go`).

- Repository: https://github.com/mit-pdos/gokv.git
- Commit: `d007325b6ee8a52ba16b68facd07cba902214b3f` (2025-05-08)
- Catalogue record: `related-work-papers/sources/CATALOGUE-grove-civl.md`, "gokv/fencing:
  epoch-fenced front end over two counter servers"; candidate N16 in `CANDIDATES.md` §1.

The program text the benchmark follows: `config/server.go` (`AcquireEpoch` waits while
`currHolderActive`, then `currentEpoch += 1`; `HeartbeatListener` clears `currHolderActive` once
the heartbeat expires; `Get` returns the current front end), `frontend/server.go`
(`StartServer` acquires the epoch; `FetchAndIncrement(key)`: `ck1.Get(epoch)` then
`ck1.Put(ret+1, epoch)` under the front end's mutex), `ctr/server.go` (`Get`/`Put` return
`EStale` when `epoch < lastEpoch`, else set `lastEpoch = epoch`), `ctr/client.go` (on `EStale`:
`log.Println(...)`, `primitive.Exit(1)`), `client/client.go` (`FetchAndIncrement`: retry, on an
error fetching the current front end from the config server), and `loopclient/loopclient.go`
(the client assertion `v > lowerBound`). The Perennial proof is not on disk.
