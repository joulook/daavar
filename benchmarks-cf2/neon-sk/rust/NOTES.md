# bench_neon_sk: Neon's safekeeper acceptor (unmodified) under TraceForge's Must API

Source: neondatabase/neon fa504217c61bbcaf5c512d75830564541f917f8f, from the cut in
`plan/case-studies/neon/neon-src/` (abbreviations: sk.rs = safekeeper/src/safekeeper.rs, wp.c =
pgxn/neon/walproposer.c, INTERFACE = plan/case-studies/neon/INTERFACE-agents.md, cut README =
plan/case-studies/neon/cut/README.md). Harness: `src/main.rs`. Vendored crate: `vendor/safekeeper` with
the shim crates `vendor/{utils,safekeeper_api,postgres_ffi,postgres_versioninfo,pq_proto,storage_broker}`.

## 1. Roles and messages

Arguments `<variant> <n> <p> <r>` (INTERFACE 1). Spawn order `s0..s{n-1}`, `p0..p{p-1}`, `w0..w{p-1}`, `init`
(INTERFACE 2); `main` spawns and returns (no join, no message event). Visible: `w0..w{p-1}`.

| Process | Code | Receives | Sends |
|---|---|---|---|
| `wi` (visible) | `writer`, INTERFACE 5.1 verbatim | `WRep` from `pi` | `WReq::Append(value(i,j,p))` to `pi`, untagged |
| `pi` (invisible) | `WalProposer` (wp.c port, section 4) | its writer's `WReq` (untagged; `recv_tagged_msg_block` with tag `None`); `PIn` tagged `TAG_P = 1` | `SkIn::Pa`, `SkIn::Fetch` to safekeepers; `WRep` to `wi` |
| `sj` (invisible) | `safekeeper_thread`: one `SafeKeeper<InMemoryState, MemWalStore>` | `SkIn` (untagged) | `PIn` tagged `TAG_P` |
| `init` (invisible) | closure in `harness` | nothing | `SkIn::Setup` to each `sj`, `PIn::Setup` (tagged) to each `pi` |

Invisible messages:
- `Setup { variant, n, r, sks, props, writers }` (thread ids and the instance parameters).
- `SkIn::Pa { from, bytes }`: the v3 wire bytes of a ProposerAcceptorMessage ('g', 'v', 'e', 'a'), encoded by the
  proposer (`ser_greeting`, `ser_vote_request`, `ser_elected`, `ser_append`: wp.c PAMessageSerialize 2226-2275 and
  MembershipConfigurationSerialize 2180-2205), parsed by the acceptor with `ProposerAcceptorMessage::parse(..,
  SK_PROTO_VERSION_3)` (sk.rs:487-605).
- `PIn::Ap { from, bytes }`: the reply of `process_msg` serialised with `AcceptorProposerMessage::serialize(..,
  SK_PROTO_VERSION_3)` (sk.rs:780-819), decoded by the proposer's `AsyncReadMessage` (wp.c:2458-2541, v3 part).
- `PIn::Reset { from }`: `process_msg` returned `Err` (or the bytes did not parse, D7); the safekeeper drops every
  later message from that proposer (INTERFACE 5.3, receive_wal.rs:605).
- `SkIn::Fetch { from, term, lo, hi }` / `PIn::FetchReply { from, data: Option<Vec<u8>> }`: INTERFACE 5.2 step 4;
  answered outside `process_msg` with the WAL store's bytes [lo, hi) iff the acceptor's term equals `term`.

Safekeeper thread (INTERFACE 5.3): first receives its Setup (tag 2, D21), then the proposers' messages; owns
`SafeKeeper::new(TimelineState::new(InMemoryState{initial_state()}), MemWalStore::new(), NodeId(j))`;
`process_msg` is polled once with `std::task::Waker::noop()` (`poll_once`, `Pending` panics); `Ok(Some(m))` ->
reply, `Ok(None)` -> nothing, `Err` -> `Reset` and the connection closed. Library panics are not caught.

Storages (INTERFACE 6): `InMemoryState` is the unit tests' control store (sk.rs:1437-1458) copied;
`MemWalStore` keeps the bytes of [START_LSN, write_lsn), write_lsn = flush_lsn = START_LSN at start, `write_wal`
panics on a wrong start position and `truncate_wal` on a position past write_lsn (the panics of
sim/safekeeper_disk.rs:196-198, 222-227), `flush_wal` sets flush_lsn = write_lsn, `initialize_first_segment` and
`remove_up_to` do nothing. Initial control state `TimelinePersistentState::new(ttid [1u8;16]/[1u8;16],
Configuration::empty(), ServerInfo{PG17, 0, WAL_SEGMENT_SIZE}, START_LSN, START_LSN)`; START_LSN = 0x0100_0000.

## 2. Vendoring

`vendor/LICENSE` is neon's LICENSE (Apache 2.0), md5 3b83ef96387f14655fc854ddc3c6bd57, equal to the source.
Edition 2024 for every vendored crate (the source's safekeeper/Cargo.toml says 2024).

### 2.1 md5 of every vendored file against the source

| Vendored file | md5 | Source file | source md5 | Relation |
|---|---|---|---|---|
| safekeeper/src/safekeeper.rs | 48a143a1884cbc6f348828457a5b82c9 | safekeeper/src/safekeeper.rs | 48a143a1884cbc6f348828457a5b82c9 | byte-identical |
| safekeeper/src/state.rs | 4f82ab39da42bc597b337b6114423a11 | safekeeper/src/state.rs | 4f82ab39da42bc597b337b6114423a11 | byte-identical |
| postgres_versioninfo/src/lib.rs | b529862159ebad6b0564a0a75c7d3fda | libs/postgres_versioninfo/src/lib.rs | b529862159ebad6b0564a0a75c7d3fda | byte-identical (E13) |
| safekeeper/src/control_file.rs | 8ae7e61da33ecfa6d043219815ac45c9 | safekeeper/src/control_file.rs | 557bafce138a20053ae9a98f936d23b8 | cut (E1) |
| safekeeper/src/wal_storage.rs | c13d26b8f4b6718c29ab2af8d0c37dcc | safekeeper/src/wal_storage.rs | dc26737cacbf0bd62b5bf21809f4165a | cut (E2) |
| safekeeper/src/metrics.rs | 073204428313d643e8231826ca03da05 | safekeeper/src/metrics.rs | 93f7cc7280bb6c680a9b9c828e88d6c6 | replaced (E3) |
| safekeeper/src/timeline.rs | 7c014d0ec824ade6e3fd2e606592a156 | safekeeper/src/timeline.rs | 2825f09b0a18f05c1c56ebdb1afe384a | replaced (E4) |
| safekeeper/src/wal_backup_partial.rs | 9df86bb56d9a0ab7fdc7f57fccf2d0fa | safekeeper/src/wal_backup_partial.rs | 6427fe79fad44ccc22a5c06b5d57b044 | replaced (E5) |
| safekeeper/src/lib.rs | 7cc167f8dd1e7c372db21a8e9cd30d5c | (new) | | new (E6) |
| utils/src/lsn.rs | 24915938553e4b9bdf12d9d2b1929004 | libs/utils/src/lsn.rs | 2456576cef29a1a37a6abd0d06cc1630 | cut (E7) |
| utils/src/id.rs | 7a107c03480672ed720525330d19a2e5 | libs/utils/src/id.rs | 14c9c49d3f9f54f51bdbca087b865966 | cut (E8) |
| utils/src/bin_ser.rs | 2c72556b92448a1107d786f24fe1cb4a | libs/utils/src/bin_ser.rs | 5759f6a7d45c5629ba7eba55344284fc | cut (E9) |
| utils/src/pageserver_feedback.rs | 9861b5e2f40293d564930bb1675911e3 | libs/utils/src/pageserver_feedback.rs | 0e2d411ba90c77d34280574460fe1d5f | cut (E10) |
| utils/src/lib.rs | 845e440a43f1baaf242d99ff5542c89e | (new) | | declares the four modules |
| safekeeper_api/src/lib.rs | c6e62158ba7dbdfb8f4de380d1153756 | libs/safekeeper_api/src/lib.rs | e0808eb93bf5f9090724229d85d67bac | cut (E11) |
| safekeeper_api/src/membership.rs | 8e2efae7d1b27990c01c9d18ad31990c | libs/safekeeper_api/src/membership.rs | ad93e5dc6a4141543ac9a3af532a112f | cut: lines 1-161 unchanged (tests 163- dropped) |
| safekeeper_api/src/models.rs | 95dd5d4c25601c43c8895427c8c0b4ec | libs/safekeeper_api/src/models.rs | 2047510cf71fec93834f1ba0c3358b28 | cut (E12) |
| postgres_ffi/src/lib.rs | c31f115d87aee15dfa1b44a0dfc0b1f6 | (written from usage) | | E14 |
| pq_proto/src/lib.rs | 273092f13123f1bdbae49c883bf52bfd | (written from usage) | | E15 |
| storage_broker/src/lib.rs | 9203ee818a782ed7352538b23821a306 | (written from usage) | | E16 |
| */Cargo.toml (7 files) | safekeeper 9376059c…, utils a72bcbc2…, safekeeper_api 01afeb33…, postgres_versioninfo b0f15fe0…, postgres_ffi fdf73b41…, pq_proto 01dcd2d6…, storage_broker f70d5e07… | (new) | | E17 |

The cut files were checked line by line against the source with `diff` (the kept ranges equal the source except the
edits listed below).

### 2.2 Edits (each with its source lines and reason)

- E1 control_file.rs: line 1-2 (module doc) and lines 30-40 (`CHECKSUM_SIZE` and the trait `Storage`) kept verbatim;
  the imports replaced by the five the trait needs (`Future`, `Deref`, `Instant`, `anyhow::Result`,
  `crate::state::TimelinePersistentState`); `FileStorage` (42 onward) deleted. Reason: cut README section 2.
- E2 wal_storage.rs: lines 1-9 (module doc) and 40-76 (the trait `Storage`) kept verbatim; imports replaced by
  `Future`, `anyhow::Result`, `futures::future::BoxFuture`, `postgres_ffi::XLogSegNo`, `utils::lsn::Lsn`,
  `crate::metrics::WalStorageMetrics`; `PhysicalStorage` and the rest deleted. Reason: cut README section 2.
- E3 metrics.rs: replaced. `PROPOSER_ACCEPTOR_MESSAGES_TOTAL` (63) and `MISC_OPERATION_SECONDS` (143) become statics
  whose `with_label_values(..)` returns a value whose `inc()` / `start_timer()` do nothing; `WalStorageMetrics` is
  source lines 479-488 verbatim (struct with `Clone, Default`, its methods 490- dropped). Reason: no Prometheus, no
  clock (INTERFACE 6, R5).
- E4 timeline.rs: replaced by `TimelineError` (405-423 verbatim) and `use utils::id::TenantTimelineId`. Reason:
  state.rs:21 names it; everything else of timeline.rs is outside the cut.
- E5 wal_backup_partial.rs: replaced by `UploadStatus` and `PartialRemoteSegment` (43-68 verbatim, derives and
  fields) and `State` (82-87 verbatim) with their three imports; the `impl PartialRemoteSegment` (70-80) and the rest
  dropped. Reason: state.rs:22, 66 name `State`. (The cut README says 44-87; 43 is the derive line of
  `UploadStatus`, which the copy needs.)
- E6 lib.rs: new; declares the seven modules and re-exports `anyhow, bytes, postgres_ffi, postgres_versioninfo,
  safekeeper_api, utils` so that the harness's Cargo.toml names only traceforge and this crate (D18).
- E7 utils lsn.rs: lines 1-299 (through `AddAssign<u64>`) kept, minus line 6 (`use std::sync::atomic::...`, used
  only by `AtomicLsn`, dropped) and line 11 (`use crate::seqwait::MonotonicCounter`, module not vendored);
  `AtomicLsn`, `RecordLsn`, `LsnSampler` (rand) and tests (300-) dropped.
- E8 utils id.rs: lines 1-368 kept minus line 7 (`use rand::Rng`), 105-110 (`Id::generate`), 183-186
  (`generate` in `id_newtype!`; the cut script removed 185-188, the function and its trailing blank line, which
  removes the same bytes as 183-186, the preceding `}` and blank line and the function's first two lines: the
  lines 183-184 and 187-188 are identical), 316-319 (`TenantTimelineId::generate`); tests (370-) dropped. Reason: `rand` is
  not a permitted dependency and `generate` is not used by the acceptor.
- E9 utils bin_ser.rs: lines 1-262 verbatim (tests dropped).
- E10 utils pageserver_feedback.rs: lines 1-116 kept (struct 19-39, `empty` and `serialize` of the impl from 40) with
  three edits: line 4 `use pq_proto::{PG_EPOCH, read_cstr};` -> `use pq_proto::PG_EPOCH;` (`read_cstr` serves only
  `parse`, dropped); line 31 `#[serde(with = "serde_systemtime")]` deleted (the module `utils::serde_systemtime` is
  not vendored; `SystemTime`'s own serde impl is used; serde of this struct only serves json dumps, never reached);
  a closing `}` appended to end the impl block after `serialize` (`parse` 118- dropped). Unused imports
  (`Duration`, `Buf`, `Bytes`, `trace`, `warn`) left as they are (warnings).
- E11 safekeeper_api lib.rs: lines 1-28 kept minus line 3 (`use const_format::formatcp;`); the `formatcp` constants
  30-34 dropped. Reason: const_format not permitted, constants unused.
- E12 safekeeper_api models.rs: line 1 (module doc), `use serde::{Deserialize, Serialize};`, a new alias `pub type
  TimestampTz = i64;` (replaces `postgres_ffi_types::TimestampTz`; i64 as sk.rs:808 puts it on the wire), lines
  77-98 (`FullTransactionId`, `HotStandbyFeedback`, `INVALID_FULL_TRANSACTION_ID`, `empty`) and 286-291
  (`TimelineTermBumpResponse`) verbatim.
- E13 postgres_versioninfo lib.rs: copied whole (the items the README lists cover nearly all of it; the rest compiles
  with serde, serde_repr and thiserror), so no edit.
- E14 postgres_ffi (written): `MAX_SEND_SIZE = XLOG_BLCKSZ * 16` (131072), `WAL_SEGMENT_SIZE = 16 MiB`,
  `TimeLineID = u32`, `XLogSegNo = u64`, `XLOG_BLCKSZ = 8192` (Postgres defaults; original not fetched).
- E15 pq_proto (written): `SystemId = u64`; `PG_EPOCH` = UNIX_EPOCH + 946684800 s (2000-01-01, the Postgres epoch) as
  a `std::sync::LazyLock<SystemTime>` (pageserver_feedback.rs dereferences it, `*PG_EPOCH`). The cut README lists
  only `SystemId`; `PG_EPOCH` is needed by `PageserverFeedback::empty`/`serialize` (E10).
- E16 storage_broker (written): `proto::SafekeeperTimelineInfo { last_log_term: u64, commit_lsn: u64 }`, the two
  fields sk.rs:1407-1417 reads; never constructed.
- E17 Cargo.toml files: crates.io deps anyhow, byteorder, bytes, futures, hex (feature serde), serde (derive),
  thiserror 1, tracing, bincode 1.3, serde_repr 0.1.20; dev-dependency tokio (macros, rt) for the three
  `#[tokio::test]`s.

safekeeper.rs and state.rs are not edited; nothing in the acceptor is fixed or changed.

## 3. Unit tests (sk.rs:1508-1797) against the vendored crate

Command (in this directory): `TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/neon-rust/target cargo test --offline -j 2 -p safekeeper`

```
running 8 tests
test safekeeper::tests::test_find_highest_common_point_middle ... ok
test safekeeper::tests::test_find_highest_common_point_none ... ok
test safekeeper::tests::test_find_highest_common_point_sk_end ... ok
test safekeeper::tests::test_find_highest_common_point_walprop ... ok
test safekeeper::tests::test_sk_state_bincode_serde_roundtrip ... ok
test safekeeper::tests::test_last_log_term_switch ... ok
test safekeeper::tests::test_non_consecutive_write ... ok
test safekeeper::tests::test_voting ... ok
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```
The three tokio tests run as written (`#[tokio::test]`, tokio as dev-dependency), not rewritten.

## 4. Proposer steps and their walproposer.c sources

Legacy mode (generation 0, `Configuration::empty()`), proto v3. Function names are wp.c's.

| INTERFACE 5.2 step | Rust function | wp.c function and lines |
|---|---|---|
| quorum | `WalProposerCreate` | WalProposerCreate 91-208, quorum 181 (`bad-quorum`: n/2) |
| 1 greeting | `SendProposerGreeting`, `ser_greeting` | SendProposerGreeting 730-746; PAMessageSerialize 'g' 2226-2237 |
| 1 collect terms | `RecvAcceptorGreeting`, `TermsCollected` | RecvAcceptorGreeting 921-1037 (max 995, n_connected 991); TermsCollected legacy 881-891 |
| 2 vote requests | `SendVoteRequest` | RecvAcceptorGreeting 1026-1035; SendVoteRequest 1040-1053; 'v' 2238-2245 |
| 2 late greeting with higher term ends the term | `RecvAcceptorGreeting` | 1006-1011 |
| 2 vote replies | `RecvVoteResponse` | 1056-1112: refusal 1078-1084, Elected to later voters 1105-1110 |
| 3 election and donor | `VotesCollected`, `GetLastLogTerm`, `GetHighestTerm` | VotesCollected legacy 1179-1209 (donor 1190-1198, truncate 1199); 1300-1311 |
| 3 proposer history | `ProcessPropStartPos` | 1334-1434: availableLsn 1373, history 1374-1385; bootstrap 1352-1366 asserted not taken; basebackup check 1397-1432 cut |
| 4 Elected | `HandleElectedProposer`, `SendProposerElected`, `ser_elected` | 1245-1296 (recovery 1250-1258, sync 1259-1263/1278-1290 cut); SendProposerElected 1446-1526 (start point 1466-1505); 'e' 2247-2261 |
| 4 Fetch | `HandleElectedProposer`, `handle` (FetchReply) | replaces neon_walreader / `wal_read` (1718); INTERFACE 5.2 step 4 |
| 5 streaming | `StartStreaming`, `SendMessageToNode`, `BroadcastAppendRequest`, `SendAppendRequests`, `ser_append` | 1533-1555, 1564-1573, 1578-1585, PrepareAppendRequest 1587-1598, SendAppendRequests 1648-1776, 'a' 2262-2276 |
| 5 writer record | `WalProposerBroadcast` | 243-249 |
| 6 append replies | `RecvAppendResponses` | 1787-1844 (generation 1812-1818, higher term 1819-1835) |
| 6 commit | `HandleSafekeeperResponse`, `GetAcknowledgedByQuorumWALPosition`, `CalculateMinFlushLsn`, `process_safekeeper_feedback` | 2095-2176 (commit + broadcast 2101-2106, feedback callback 2114, truncate 2126-2131); 1999-2026 (term-start condition 2018; `no-prev-term-rule` drops it); 1927-1938 |
| 7 Reset | `ShutdownConnection` | 359-375 (no ResetConnection/reconnect: 380-445, 453-488 cut) |
| decoder | `AsyncReadMessage` | 2458-2541 (v3: 'g' 2489, 'v' 2499, 'a' 2518), MembershipConfigurationDeserialize 2417-2440 |
| term end | `term_end` | the FATALs above; INTERFACE 5.2 step 7 |

## 5. Decisions (where the sources leave a choice open)

- D1 Tags: the writer's `Append` is untagged (writer code verbatim); every other message to a proposer is sent with
  `send_tagged_msg(.., TAG_P = 1, ..)`. The proposer receives `WReq` with the predicate `tag.is_none()` and `PIn`
  with `tag == Some(1)` (INTERFACE 4, last bullet).
- D2 Heterogeneous receive. TraceForge's typed receive cannot wait for "a `WReq` or a `PIn`" at once (the type is fixed
  before the receive; `inbox` is out of conformance scope). The proposer therefore: (a) does not read its writer's
  Appends before streaming starts (state Elected and the Fetch done), since they only wait there (INTERFACE 5.2 step 5,
  "Appends that arrive before the election ... wait in order"); (b) while streaming and with Appends still to come
  (it knows `r`), reads the writer when no safekeeper reply is outstanding, and otherwise makes an invisible
  `nondet()` choice between the writer and the safekeepers; (c) after the term ended, reads only the writer and
  answers `Ended`. "Outstanding" is bookkeeping per connection: +1 for each Greeting, VoteRequest, AppendRequest,
  Fetch sent; -1 per reply; 0 after a `Reset`. Elected counts 0 (`handle_elected` returns `Ok(None)` or `Err`,
  sk.rs:1106-1260) and is always followed by an AppendRequest once streaming, so a positive count guarantees that a
  message will come: the `nondet` branch toward the safekeepers never blocks spuriously, and the writer branch never
  blocks (the writer always sends its `r` Appends).
- D3 wp.c's `Assert`s (1085-1086 vote term, 1476 same term same LSN, 1507 start point) are `debug_assert!`: off in
  the release build, as Postgres production builds compile `Assert` out (no USE_ASSERT_CHECKING). The acceptor's
  `assert!`s are untouched and fail the execution.
- D4 Fetch and Reset: a safekeeper drops every message from a proposer after it sent that proposer `Reset`, the Fetch
  included (INTERFACE 5.3). If the donor's connection resets while the Fetch is pending (or the donor is already
  offline when the Fetch would be sent), the proposer's term ends: it cannot obtain the WAL below the term start.
- D5 Fetch reply when the donor's term equals `term` but its WAL does not cover [lo, hi): refused (`None`), which ends
  the term like a term mismatch. A reply's length is asserted to be term start - START_LSN.
- D6 Greeting fields: tenant/timeline id as the 32-hex-digit cstring of [1u8;16] (wp.c sends neon.tenant_id /
  neon.timeline_id strings; sk.rs:497-502 parses `from_str`); `pg_version` = 170000 (a PG17 PG_VERSION_NUM; wp.c:201
  sends PG_VERSION_NUM); system_id 0; wal_seg_size = WAL_SEGMENT_SIZE; empty membership configuration (generation 0,
  no members).
- D7 A message the acceptor's `parse` refuses is treated as an `Err` of `process_msg` (Reset): in the real safekeeper
  a read error also ends the receiving task. Not observed (the encoder writes sk.rs's layout).
- D8 Streaming starts after the Fetch reply (INTERFACE 5.2 steps 4-5), not inside SendProposerElected right after
  the Elected as wp.c:1522-1525 does; a voter that gets its Elected after the Fetch reply starts streaming at once.
  Sks with an Elected sent and no streaming yet are in state `SendElectedFlush`.
- D9 wp.c:2480-2487 resets the connection on an unexpected reply tag; with one FIFO connection per pair, the replies
  arrive in request order, so the harness asserts the tag (harness fault if violated).
- D10 `n_connected` only increases (wp.c:991, as wp.c). The INTERFACE 5.2 step 7 rule "fewer than a quorum still
  connected" counts the safekeepers not in state Offline (i.e. n minus the Resets received), with the variant's
  quorum.
- D11 `VotesCollected` resets `propTermStartLsn`, `donorLastLogTerm`, `truncateLsn` on each call but not `donor`
  (wp.c:1175-1177), ported as is.
- D12 `wrong-donor`: the donor is the first safekeeper whose given vote arrived (`vote_order[0]`); term start LSN and
  donor last-log-term are taken from it; `truncateLsn` stays the max over voters.
- D13 `bad-quorum`: quorum = n/2 for TermsCollected, VotesCollected, GetAcknowledgedByQuorumWALPosition and the
  Reset rule of D10 alike (cut README 7).
- D14 SendAppendRequests keeps wp.c's loop with MAX_SEND_SIZE (131072): here every send is one AppendRequest
  [streamingAt, availableLsn), possibly empty (heartbeat).
- D15 `Committed(k)` is sent where wp.c calls `process_safekeeper_feedback` (wp.c:2114, after the commit broadcast,
  before the truncateLsn update), for every unanswered record whose end LSN is <= commitLsn, in append order;
  k = (end - START_LSN)/8.
- D16 CalculateMinFlushLsn ranges over all n safekeepers, a never-acknowledged one counting 0 (wp.c:1929-1936).
- D17 A record's payload is its value as one little-endian u64 (INTERFACE 3); the proposer's WAL buffer holds
  [START_LSN, availableLsn) (Fetch reply + own records).
- D18 The harness reaches anyhow, bytes, utils, safekeeper_api, postgres_ffi, postgres_versioninfo through re-exports
  of the vendored crate (E6), keeping the Cargo.toml dependencies exactly traceforge + the vendored crate.
- D19 `init` sends Setup to the safekeepers (tagged `TAG_SETUP = 2`, D21) then to the proposers (tagged `TAG_P`);
  writers get `(i, p, r, prop)` at spawn (proposers are spawned before writers) and receive no Setup. The proposer's
  first `PIn` is its Setup (it sends nothing before).
- D20 `SystemTime::now()` (state.rs:149) and `Instant::now()` (`last_persist_at`, never called) are left as in the
  source; neither is read by the acceptor's logic.
- D21 (review round 1) A safekeeper receives its Setup first, by a dedicated receive
  `recv_tagged_msg_block(tag == Some(2))`, and afterwards only untagged messages (the proposers'); a proposer message
  that arrives earlier waits in the mailbox. Before this change the safekeeper received whatever came first and
  buffered proposer messages until its Setup, so the position of Setup among the receives (before or after a
  greeting) was an extra choice of the exploration without any effect: Setup is not protocol traffic, the
  `SafeKeeper` was built only after it and the buffered messages were processed in their receive order. Hence the
  sequences of proposer messages each acceptor processes, with their contents, and therefore every reply, every
  proposer step and every writer word, are the same with and without the change; only executions that differ in
  where Setup sits are merged. The source has the same order: the acceptor exists before it processes the first
  proposer message. With a start LSN, as here, the timeline is created by a creation request before the compute
  writes (state.rs:92-93, "in the normal creaiton; see `TimelineCreateRequest`"); in the simulator the timeline is
  created when the first greeting is processed, before `process_msg` runs on it (sim/safekeeper.rs:340-353 calling
  `create`, 88-124). In both cases timeline setup comes before any protocol processing, which is what the dedicated
  receive enforces. Nothing else in the protocol changed.
- D22 (review round 1) Parameter domain: values and positions are `u8` (INTERFACE 4), so `parse` refuses `p*r > 255`
  (every value 1..p*r and every committed position, at most p*r, then fits); it also refuses `bad-quorum` with
  n/2 = 0 (n = 1), where wp.c:2025's `responses[n - quorum]` would index past the array. A refused instance panics
  in `parse` with the usage text before anything is spawned, like every other invalid argument. The writer code is
  unchanged; the proposer's position cast is now a checked conversion (harness fault if above 255, unreachable
  after the check).
- D23 (review round 1, the coordinator's decision on 9.3; wording corrected in review round 2) Streaming start beyond
  the available end. This is a **deliberate abstraction by the coordinator of a fault path that production does not
  define**, not a reproduction of production C. When a safekeeper's `start_streaming_at` exceeds `availableLsn`
  (SendProposerElected takes the safekeeper's flush_lsn when the last common term is the proposer's own,
  wp.c:1494-1497), `SendAppendRequests` sends the complete AppendRequest header `[begin = streamingAt, end =
  availableLsn)` with no WAL bytes and sets `streamingAt = end`; the acceptor's `parse` refuses `begin_lsn >
  end_lsn` (sk.rs:581-585), which the safekeeper thread maps to `Reset` (D7). Production differs: with
  `Assert(sk->startStreamingAt <= wp->availableLsn)` (1507) and `Assert(endLsn >= beginLsn)` (1590) compiled out,
  SendAppendRequests clamps `endLsn` below `beginLsn` (1676-1683), passes the negative difference to
  `enlargeStringInfo` (1700), assigns it to `int req_len` (1710), adds it to the buffer length (1738) and writes
  that length (1740): with req_len = -8 the 45-byte v3 header becomes a 37-byte write, a truncated header, not a
  complete one. **No conclusion about production behaviour on this path is drawn from the harness.** The harness
  assert stays for `begin < START_LSN`. The path is reached only when two proposers are elected in one term, i.e.
  only under `bad-quorum` (review round 2, finding 2, confirms this and gives a `bad-quorum 2 2 1` schedule): with a
  majority quorum two elections in one term need two vote quorums that intersect in a safekeeper that would have
  voted twice in that term, which `handle_vote_request` refuses (sk.rs:1078-1086). The writer-visible fault of
  `bad-quorum` (two writers told `Committed(1)`, 9.2) occurs before this path (review round 2, finding 7).

## 6. Build

```
cd daavar/benchmarks-cf2/neon-sk/rust
TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/neon-rust/target cargo build --release -j 2
```
The binary is `$CARGO_TARGET_DIR/release/bench_neon_sk` (with the default target dir it is `target/release/bench_neon_sk`).
Cargo.lock was seeded from daavar/traceforge/Cargo.lock; the resolution added bincode 1.3.3, hex 0.4.3,
serde_repr 0.1.21 (plus anyhow, byteorder, tracing-attributes already compatible). See section 8 for what the
fetch downloaded.

## 7. Runs

All on the workstation (16 cores), one thread each (`count` without `--threads`), 2026-11-10,
binary `B=/local/home/mkhoshechin/tmp/claude-build/neon-rust/target/release/bench_neon_sk` built from this
directory as in section 6, each command under `/usr/bin/time` and `timeout 600`. The `correct 3 1 1` run ran alone;
the four `3 2 1` runs ran concurrently (four processes on 16 cores, load about 7). Every run reached the 600 s
limit (exit 124), so none printed `execs=.. blocked=..` or the measurement block; the counts below are the last
progress lines TraceForge printed (every execution ends blocked: safekeepers and proposers never end).

| command | result | wall | peak RSS | last progress line |
|---|---|---|---|---|
| `timeout 600 $B count impl --model fifo -- correct 3 1 1` | **timeout** (not finished in 600 s) | 600.00 s | 5232 KB | `Executions attempted so far: 5000000 total 0 finished normally 5000000 blocked (8417.51/sec).` |
| `timeout 600 $B count impl --model fifo -- correct 3 2 1` | **timeout** | 600.00 s | 5592 KB | `Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (6024.10/sec).` |
| `timeout 600 $B count impl --model fifo -- bad-quorum 3 2 1` | **timeout** | 600.00 s | 6704 KB | `Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (4514.67/sec).` |
| `timeout 600 $B count impl --model fifo -- wrong-donor 3 2 1` | **timeout** | 600.00 s | 5828 KB | `Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (5464.48/sec).` |
| `timeout 600 $B count impl --model fifo -- no-prev-term-rule 3 2 1` | **timeout** | 600.00 s | 6116 KB | `Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (5952.38/sec).` |

Last output lines of each run (stdout+stderr, progress lines are printed at 1, 2, .., 10, 20, .., 10^k, 2*10^k, ..):

```
correct 3 1 1:
Executions attempted so far: 4000000 total 0 finished normally 4000000 blocked (8583.69/sec).
Executions attempted so far: 5000000 total 0 finished normally 5000000 blocked (8417.51/sec).
Command exited with non-zero status 124
wall=600.00 s maxrss=5232 KB

correct 3 2 1:
Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (5970.15/sec).
Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (6024.10/sec).
Command exited with non-zero status 124
wall=600.00 s maxrss=5592 KB

bad-quorum 3 2 1:
Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (4694.84/sec).
Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (4514.67/sec).
Command exited with non-zero status 124
wall=600.00 s maxrss=6704 KB

wrong-donor 3 2 1:
Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (5076.14/sec).
Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (5464.48/sec).
Command exited with non-zero status 124
wall=600.00 s maxrss=5828 KB

no-prev-term-rule 3 2 1:
Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (5763.69/sec).
Executions attempted so far: 3000000 total 0 finished normally 3000000 blocked (5952.38/sec).
Command exited with non-zero status 124
wall=600.00 s maxrss=6116 KB
```

Correction (review round 1): a Rust panic in a model thread (a library `assert!` or a harness `assert!`/`panic!`)
is not absorbed by `count`'s `keep_going_after_error`: it aborts the run with exit 101 and the panic message, as
the `bad-quorum 2 2 1` run of section 9 shows. So the runs above reached no panic in the part of the state space
they explored; they do not establish that none is reachable.

Sanity check (not one of the prompt's runs; a scratch copy of this main.rs in
/local/home/mkhoshechin/tmp/claude-build/neon-rust/probe whose writer additionally asserts on its reply under an
environment variable, same vendored crate): `count impl --model fifo -- correct 1 1 1` with the writer asserting
`Committed` never arrives panicked with `probe: Committed Committed(1)` (a record does commit); with the writer
asserting `Ended` never arrives, `correct 1 1 1` finished with `execs=0 blocked=6` and `correct 1 1 2` with
`execs=0 blocked=24` (one proposer, one safekeeper: no execution ends the term).

## 8. Deviations

- `cargo fetch` (run once to resolve the three new crates) downloaded 19 crates, 19.6 MB in total: bincode, hex,
  serde_repr (the permitted ones) and tracing-attributes (40 KB, a default feature of tracing), plus platform crates
  of traceforge's lock that were missing from the local cache and are never compiled on Linux (windows 0.48.0
  11.9 MB, windows-sys 0.61.2 2.5 MB, the windows_* 0.48.5 target crates ~5 MB, wasi, hermit-abi, redox_syscall,
  r-efi). `cargo fetch` downloads for every target; every later cargo command ran with `--offline` or found
  everything cached.
- The binary is under CARGO_TARGET_DIR (section 6), so runs used that path instead of `target/release/...`.
- Unit tests and the probe in section 7 are additions for checking, not deliverables.

## 9. Review round 1 (Astra, plan/case-studies/neon/review/REVIEW-astra-rust.md)

### 9.1 Findings and changes

| # | Finding | Change |
|---|---|---|
| 1 | MUST-FIX: `p*r > 255` accepted; `value(i,j,p)` and the position cast overflow `u8` | `parse` refuses `p*r > 255` (checked multiply) before anything is spawned; the position cast is `u8::try_from(..)` (harness fault if above 255, unreachable now). Writer code unchanged. D22. |
| 2 | NOTE: vendoring byte-identical, hashes match | none |
| 3 | SHOULD-FIX: E8 range 183-186, E12 range 77-98 | E12 corrected to 77-98 (line 98 is the blank line the file has). E8 now says 183-186, with the note that the cut script removed 185-188 and that both ranges remove the same bytes (lines 183-184 equal 187-188), so the file is unchanged. |
| 4 | NOTE: proposer faithful | none |
| 5 | NOTE: `bad-quorum 1 1 1` gives quorum 0 and indexes past `responses` | `parse` refuses `bad-quorum` when n/2 = 0. D22. |
| 6 | NOTE: interface and lifecycle match | none |
| 7 | NOTE: D2 is bounded arbitration | none |
| 8 | SHOULD-FIX (plan estimates); the setup-before/after-greeting choice is an avoidable factor (8 at 3 1 1) | a safekeeper receives its Setup first by a dedicated tagged receive (`TAG_SETUP = 2`; the proposers' messages stay untagged and wait). D21 argues that no writer-visible behaviour is removed. STUDY-PLAN.md not touched (the coordinator records the estimates). |
| 9 | NOTE: no reachable panic under `correct` at the registered sizes (argument) | none; see 9.3 for a harness assert reached under `bad-quorum` |

src/main.rs after items 1-5 of this round: md5 82dfe1916d39a87ef3d6a426c51aeee5 (after D23: 9.4). Rebuilt with `--offline` (nothing fetched);
the eight unit tests pass again (same output as section 3).

### 9.2 Measurements (workstation, one thread per run, `timeout 600`, binary of 9.1)

| command (`$B count impl --model fifo -- ...`) | result | wall | last line / progress |
|---|---|---|---|
| `correct 1 1 1` | complete | 0.00 s (wall_ms 1) | `execs=0 blocked=3` (6 before item 8) |
| `correct 2 1 1` | complete | 0.08 s (wall_ms 80) | `execs=0 blocked=688` (2752 before item 8 = 688 x 2^2: the setup factor 2 per safekeeper) |
| `correct 2 2 1` | timeout | 600.00 s | `Executions attempted so far: 4000000 total 0 finished normally 4000000 blocked (7259.53/sec).` |
| `bad-quorum 2 2 1` | **harness panic**, exit 101 | 383.30 s | `Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (6688.96/sec).` then `panicked at src/main.rs:833:13: harness fault: streaming range` |
| `correct 3 1 1` | timeout | 600.00 s | `Executions attempted so far: 2000000 total 0 finished normally 2000000 blocked (3696.86/sec).` |

Load: `correct 3 1 1` ran while three other runs (the first `correct 2 2 1`/`bad-quorum 2 2 1` pass and the probe)
were running, which explains its lower rate than in section 7 (3,700/s against 8,400/s alone); its progress line is
therefore not comparable with section 7's. The first pass of `correct 2 2 1` and `bad-quorum 2 2 1` printed only
the exit lines (the progress lines were filtered); both were rerun together, with full logs: those reruns are the
rows above. Refusals checked: `correct 3 2 128` and `bad-quorum 1 1 1` exit with the usage panic.

Two writers told the same position, `bad-quorum 2 2 1`: **yes**. The scratch probe (main.rs of 9.1 with the writer
additionally recording each `Committed(k)` in a per-execution global and exiting on a second writer with the same
k) stopped after 0.71 s: `PROBE: duplicate position 1: w1 and [(0, 1)]` (w0 and w1 both received `Committed(1)`).
The same probe on `correct 2 2 1` ran to the 600 s limit without that event (`1000000` executions explored; not
exhaustive).

### 9.3 A harness assert reached under `bad-quorum 2 2 1` (resolved by D23)

`SendAppendRequests` asserts `end >= begin` (src/main.rs:833). Under `bad-quorum` (quorum 1 of 2), both proposers can
be elected in the same term; a safekeeper whose vote-reply history already contains that term from the other
proposer, with WAL past this proposer's term start, gets `start_streaming_at` = its flush_lsn (wp.c:1494-1497,
the last common term is the own term) which exceeds `availableLsn`. This reading of the schedule is a hand
derivation, not traced. wp.c's `Assert(sk->startStreamingAt <= wp->availableLsn)` (1507) and
`Assert(endLsn >= beginLsn)` (1590) are compiled out in production; there SendAppendRequests clamps `endLsn` to
`availableLsn` below `beginLsn`, sends a header with begin_lsn > end_lsn and a negative `req_len` added to
`outbuf.len` (wp.c:1700, 1710, 1738, 1740), so production writes a truncated header (37 of 45 bytes for
req_len = -8); production does not define this fault path. The coordinator decided (D23) to abstract it: the harness
sends the complete header without WAL, which the acceptor's `parse` refuses (`begin_lsn > end_lsn`, sk.rs:581-585)
-> `Reset` (D7). This is a deliberate abstraction, not production behaviour, and no conclusion about production on
this path is drawn from it. The writer-visible fault of `bad-quorum` (two writers told `Committed(1)`, 9.2) occurs
before this path.

### 9.4 After D23

src/main.rs md5 3c18d1dfc8a0045dc810366b78d22b3f; rebuilt with `--offline`; the eight unit tests pass.
`timeout 600 $B count impl --model fifo -- bad-quorum 2 2 1`: no panic; it reached the 600 s limit (exit 124,
wall 600.00 s, peak RSS 6312 KB), last progress line `Executions attempted so far: 4000000 total 0 finished
normally 4000000 blocked (7874.02/sec).` (it passed the 2,000,000-execution point where the previous binary
panicked). The duplicate `Committed(1)` to w0 and w1 was found by the probe of 9.2 (before D23; D23 changes only
the path after which the probe had already stopped).

### 9.5 Review round 2 (review/REVIEW-astra-rust-r2.md)

Finding 1 (MUST-FIX, wording): D23, the code comment at src/main.rs `SendAppendRequests` and 9.3 now state that
D23 is the coordinator's deliberate abstraction of a fault path production does not define (production writes a
truncated header, wp.c:1700, 1710, 1738, 1740), reached only under `bad-quorum`, and that no conclusion about
production on that path is drawn; the writer-visible fault occurs before it (finding 7). No code behaviour changed:
only the comment. src/main.rs md5 49a3c434c01c4f4182c0017ebecabd5b; `cargo build --offline --release -j 2` compiles.
