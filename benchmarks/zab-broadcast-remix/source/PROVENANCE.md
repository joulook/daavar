# Provenance

Copied verbatim (`cp -p`, no edits) on 2026-10-30 from
`plan/benchmarks/related-work-papers/sources/remix/Remix-artifact/apache-zookeeper-3.9.1/zookeeper-specifications/protocol-spec/`,
the Remix artifact (EuroSys 2025), unpacked from `remix/Remix-artifact.zip`
(md5 `9dada12f2041a012ce66f82c060e3ea1`; the artifact is not a git checkout, so there is no
commit). The files are ZooKeeper 3.9.1's vendored protocol specification. Every file was compared
byte for byte (`cmp`) against the unpacked tree on 2026-10-30.

| file here | path in the source tree | role |
|---|---|---|
| `Zab.tla` | `protocol-spec/Zab.tla` (1,251 lines, md5 `922f338597ab5105588c03f24526db06`) | the protocol spec; we follow `LeaderProcessACKEPOCH` (case 1), `FollowerProcessNEWLEADER`, `LeaderProcessACKLD` (case 2), `FollowerProcessCOMMITLD`, `LeaderProcessRequest`, `LeaderBroadcastPROPOSE`/`Broadcast`, `FollowerProcessPROPOSE`, `LeaderProcessACK`/`LeaderTryToCommit`/`DiscardAndBroadcast`, `FollowerProcessCOMMIT` |
| `issues.md` | `protocol-spec/issues.md` | the three documented protocol issues; Issue 1 is the variant `issue1-propose-to-ackld` |
| `doc.md` | `protocol-spec/doc.md` | the authors' notes on the spec's departures from the paper |

Catalogue record: `related-work-papers/sources/CATALOGUE-mocket-remix-oortwijn.md`,
"remix/.../protocol-spec/Zab.tla"; candidate N18 in `CANDIDATES.md` §1.
