# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/aneris/`, the Aneris repository
(https://github.com/logsem/aneris) checked out at commit
`59ae37946838df77fb2c573803b68462a3af9d0f` (2025-10-31). Every file was compared byte for byte
(`cmp`) against the checkout on 2026-10-30.

| file here | path in the source tree | role |
|---|---|---|
| `client_server_code.ml` | `aneris/ml_sources/examples/reliable_communication/client_server_code.ml` | the channel library (375 lines): `send_from_chan_loop`, `prune_sendbuf_at_ack`, `process_data_on_chan`, `send`, `recv` |
| `examples/messages_in_order/messages_in_order_code.ml` | `aneris/ml_sources/examples/reliable_communication/examples/messages_in_order/messages_in_order_code.ml` | the client our visible threads follow (send 1, 2, 3; receive and assert the order) |
| `spec/api_spec.v` | `aneris/aneris/examples/reliable_communication/spec/api_spec.v` | the Iris specification of the channel API (messages received in order and exactly once) |

Catalogue record: `related-work-papers/sources/CATALOGUE-coq-iris.md`,
"aneris/aneris/examples/reliable_communication"; candidate N12 in `CANDIDATES.md` §1. The source
ships no faulty variant; `no-seqno` is ours, a mutation distinct from the catalogue's "accept any
sequence id ≥ the expected one" (see README §5).
