//! Neon's safekeeper consensus under TraceForge's Must API (case study `neon_sk`).
//!
//! Implementation side: the acceptor of neondatabase/neon fa50421 (`SafeKeeper<CTRL, WAL>`,
//! safekeeper.rs + state.rs, vendored byte-identical in `vendor/safekeeper`) run unmodified,
//! one invisible thread per safekeeper `s0..s{n-1}`, with in-memory storages; a proposer
//! `p0..p{p-1}` written from Neon's walproposer (pgxn/neon/walproposer.c, legacy mode without
//! generations; the proposer's functions keep walproposer.c's names); the visible writers
//! `w0..w{p-1}` of INTERFACE-agents.md section 5.1 verbatim; an invisible `init` sending the
//! setup messages. Proposer <-> safekeeper messages travel as their v3 wire bytes.
//!
//! ARGS: `<variant> <n> <p> <r>`; variant = correct | bad-quorum | wrong-donor |
//! no-prev-term-rule (every fault in the proposer). `spec` returns the harness too until the
//! coordinator copies the reviewed specification in. See NOTES.md for every decision.

#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use safekeeper::bytes::{Buf, BufMut, Bytes, BytesMut};
use safekeeper::control_file;
use safekeeper::metrics::WalStorageMetrics;
use safekeeper::postgres_ffi::{MAX_SEND_SIZE, WAL_SEGMENT_SIZE, XLogSegNo};
use safekeeper::safekeeper::{ProposerAcceptorMessage, SK_PROTO_VERSION_3, SafeKeeper};
use safekeeper::safekeeper_api::membership::Configuration;
use safekeeper::safekeeper_api::{PgMajorVersion, PgVersionId, ServerInfo};
use safekeeper::state::{TimelinePersistentState, TimelineState};
use safekeeper::utils::id::{NodeId, TenantId, TenantTimelineId, TimelineId};
use safekeeper::utils::lsn::Lsn;
use safekeeper::wal_storage;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

type Prog = traceforge::program::Entry;

// ------------------------------------------------------- interface (verbatim) ----

#[derive(Clone, Debug, PartialEq)] enum WReq { Append(u8) }
#[derive(Clone, Debug, PartialEq)] enum WRep { Committed(u8), Ended }
fn value(i: u8, j: u8, p: u8) -> u8 { 1 + i + p * j }

fn writer(i: u8, p: u8, r: u8, prop: ThreadId) {
    for j in 0..r {
        send_msg(prop, WReq::Append(value(i, j, p)));
    }
    for _ in 0..r {
        let _: WRep = recv_msg_block::<WRep>();
    }
}

// ------------------------------------------------------------ constants ----

/// INTERFACE section 3: entry k is [START_LSN + 8(k-1), START_LSN + 8k).
const START_LSN: u64 = 0x0100_0000;
/// The tag of every message to a proposer except its writer's (which is untagged).
const TAG_P: u32 = 1;
/// The tag of `init`'s Setup to a safekeeper, which the safekeeper receives first (review round 1, finding 8).
const TAG_SETUP: u32 = 2;
/// Tenant and timeline ids, both [1u8; 16] (INTERFACE section 6), as the hex cstring
/// walproposer.c sends (neon.tenant_id / neon.timeline_id).
const ID_HEX: &str = "01010101010101010101010101010101";
/// PG_VERSION_NUM of the compute: a PG17 build (NOTES.md, decision D6).
const PG_VERSION_NUM: u32 = 170000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Variant {
    Correct,
    BadQuorum,
    WrongDonor,
    NoPrevTermRule,
}

// ------------------------------------------------------------- messages ----

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    variant: Variant,
    n: usize,
    r: u8,
    sks: Vec<ThreadId>,
    props: Vec<ThreadId>,
    writers: Vec<ThreadId>,
}

/// Everything a safekeeper thread receives.
#[derive(Clone, Debug, PartialEq)]
enum SkIn {
    Setup(Setup),
    /// A ProposerAcceptorMessage's v3 wire bytes from proposer `from`.
    Pa { from: usize, bytes: Vec<u8> },
    /// The harness's donor read (INTERFACE 5.2 step 4): WAL [lo, hi) if the acceptor term is `term`.
    Fetch { from: usize, term: u64, lo: u64, hi: u64 },
}

/// Everything a proposer receives except its writer's `WReq` (all tagged `TAG_P`).
#[derive(Clone, Debug, PartialEq)]
enum PIn {
    Setup(Setup),
    /// An AcceptorProposerMessage's v3 wire bytes from safekeeper `from`.
    Ap { from: usize, bytes: Vec<u8> },
    /// `process_msg` returned Err: the connection is closed (receive_wal.rs:605).
    Reset { from: usize },
    FetchReply { from: usize, data: Option<Vec<u8>> },
}

// ------------------------------------------------------------ storages ----

/// The control store: the unit tests' `InMemoryState` (safekeeper.rs:1437-1458).
struct InMemoryState {
    persisted_state: TimelinePersistentState,
}

impl control_file::Storage for InMemoryState {
    async fn persist(&mut self, s: &TimelinePersistentState) -> safekeeper::anyhow::Result<()> {
        self.persisted_state = s.clone();
        Ok(())
    }

    fn last_persist_at(&self) -> Instant {
        Instant::now()
    }
}

impl Deref for InMemoryState {
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.persisted_state
    }
}

/// The WAL store that keeps the bytes: the shape of sim/safekeeper_disk.rs:178-267
/// without the WAL decoder (every write ends at a record boundary, INTERFACE section 6 (A)).
struct MemWalStore {
    /// bytes of [START_LSN, write_lsn)
    bytes: Vec<u8>,
    write_lsn: Lsn,
    flush_lsn: Lsn,
}

impl MemWalStore {
    fn new() -> Self {
        MemWalStore { bytes: Vec::new(), write_lsn: Lsn(START_LSN), flush_lsn: Lsn(START_LSN) }
    }

    /// The flushed WAL in [lo, hi), if the store holds all of it.
    fn read(&self, lo: u64, hi: u64) -> Option<Vec<u8>> {
        if lo < START_LSN || hi < lo || hi > self.flush_lsn.0 {
            return None;
        }
        Some(self.bytes[(lo - START_LSN) as usize..(hi - START_LSN) as usize].to_vec())
    }
}

impl wal_storage::Storage for MemWalStore {
    fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }

    fn flush_lsn(&self) -> Lsn {
        self.flush_lsn
    }

    async fn initialize_first_segment(&mut self, _init_lsn: Lsn) -> safekeeper::anyhow::Result<()> {
        Ok(())
    }

    async fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> safekeeper::anyhow::Result<()> {
        // sim/safekeeper_disk.rs:196-198
        if self.write_lsn != startpos {
            panic!("write_wal called with wrong startpos");
        }
        self.bytes.extend_from_slice(buf);
        self.write_lsn += buf.len() as u64;
        Ok(())
    }

    async fn truncate_wal(&mut self, end_pos: Lsn) -> safekeeper::anyhow::Result<()> {
        // sim/safekeeper_disk.rs:222-227
        if end_pos > self.write_lsn || end_pos.0 < START_LSN {
            panic!(
                "truncate_wal called on non-written WAL, write_lsn={}, end_pos={}",
                self.write_lsn, end_pos
            );
        }
        self.bytes.truncate((end_pos.0 - START_LSN) as usize);
        self.write_lsn = end_pos;
        self.flush_lsn = end_pos;
        Ok(())
    }

    async fn flush_wal(&mut self) -> safekeeper::anyhow::Result<()> {
        self.flush_lsn = self.write_lsn;
        Ok(())
    }

    fn remove_up_to(
        &self,
        _segno_up_to: XLogSegNo,
    ) -> Pin<Box<dyn Future<Output = safekeeper::anyhow::Result<()>> + Send + 'static>> {
        Box::pin(async { Ok(()) })
    }

    fn get_metrics(&self) -> WalStorageMetrics {
        WalStorageMetrics::default()
    }
}

/// INTERFACE section 6: `TimelinePersistentState::new(ttid, Configuration::empty(), ServerInfo
/// { PG17, 0, WAL_SEGMENT_SIZE }, START_LSN, START_LSN)` (state.rs:94-151).
fn initial_state() -> TimelinePersistentState {
    let ttid = TenantTimelineId::new(TenantId::from([1u8; 16]), TimelineId::from([1u8; 16]));
    TimelinePersistentState::new(
        &ttid,
        Configuration::empty(),
        ServerInfo {
            pg_version: PgVersionId::from(PgMajorVersion::PG17),
            system_id: 0,
            wal_seg_size: WAL_SEGMENT_SIZE as u32,
        },
        Lsn(START_LSN),
        Lsn(START_LSN),
    )
    .expect("initial timeline state")
}

/// R1: poll once with the no-op waker; `Pending` is a harness fault.
fn poll_once<F: Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("harness fault: process_msg returned Pending"),
    }
}

// ---------------------------------------------------------- safekeeper ----

fn safekeeper_thread(j: usize) {
    // The Setup first, by a dedicated tagged receive: a proposer's message that arrives earlier
    // stays in the mailbox until then (review round 1; NOTES.md D21). The timeline exists before
    // any proposer message is processed, as in the source (the timeline is created on the
    // safekeepers before the compute connects).
    let setup = match recv_tagged_msg_block::<_, SkIn>(|_, tag| tag == Some(TAG_SETUP)) {
        SkIn::Setup(s) => s,
        m => panic!("harness fault: s{j} got {m:?} as its Setup"),
    };
    let ctrl = InMemoryState { persisted_state: initial_state() };
    let mut sk = SafeKeeper::new(TimelineState::new(ctrl), MemWalStore::new(), NodeId(j as u64))
        .expect("SafeKeeper::new");
    let mut closed = vec![false; setup.props.len()];
    loop {
        let m = recv_tagged_msg_block::<_, SkIn>(|_, tag| tag.is_none());
        match m {
            SkIn::Pa { from, bytes } => {
                if closed[from] {
                    continue; // the connection is closed: later messages are dropped
                }
                let res = match ProposerAcceptorMessage::parse(Bytes::from(bytes), SK_PROTO_VERSION_3) {
                    Ok(msg) => poll_once(sk.process_msg(&msg)),
                    Err(e) => Err(e),
                };
                match res {
                    Ok(Some(reply)) => {
                        let mut buf = BytesMut::new();
                        reply.serialize(&mut buf, SK_PROTO_VERSION_3).expect("serialize v3");
                        send_tagged_msg(setup.props[from], TAG_P, PIn::Ap { from: j, bytes: buf.to_vec() });
                    }
                    Ok(None) => {}
                    Err(_) => {
                        closed[from] = true;
                        send_tagged_msg(setup.props[from], TAG_P, PIn::Reset { from: j });
                    }
                }
            }
            SkIn::Fetch { from, term, lo, hi } => {
                if closed[from] {
                    continue;
                }
                let data = if sk.state.acceptor_state.term == term { sk.wal_store.read(lo, hi) } else { None };
                send_tagged_msg(setup.props[from], TAG_P, PIn::FetchReply { from: j, data });
            }
            SkIn::Setup(_) => panic!("harness fault: second Setup at s{j}"),
        }
    }
}

// ------------------------------------------------------------ proposer ----

/// Safekeeper states of walproposer.h (the ones this cut reaches).
#[derive(Clone, Copy, Debug, PartialEq)]
enum SkState {
    Offline,
    HandshakeRecv,
    WaitVoting,
    WaitVerdict,
    WaitElected,
    /// Elected sent, streaming not started (SS_SEND_ELECTED_FLUSH; here: waiting for the Fetch).
    SendElectedFlush,
    Active,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum WpState {
    CollectingTerms,
    Campaign,
    Elected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fetch {
    Undecided,
    Pending,
    Done,
}

#[derive(Clone, Debug, Default)]
struct VoteResponse {
    generation: u32,
    term: u64,
    vote_given: bool,
    flush_lsn: u64,
    truncate_lsn: u64,
    term_history: Vec<(u64, u64)>,
}

#[derive(Clone, Debug, Default)]
struct AppendResponse {
    generation: u32,
    term: u64,
    flush_lsn: u64,
    commit_lsn: u64,
}

#[derive(Clone, Debug)]
struct Safekeeper {
    state: SkState,
    greet_term: u64,
    greet_generation: u32,
    vote_response: VoteResponse,
    append_response: AppendResponse,
    start_streaming_at: u64,
    streaming_at: u64,
    /// Replies still to come on this connection (harness bookkeeping, NOTES.md D2).
    expect: u32,
}

/// The FATAL of walproposer.c: the term ends (INTERFACE 5.2 step 7).
struct Fatal;
type Res = Result<(), Fatal>;

struct WalProposer {
    me: usize,
    variant: Variant,
    n: usize,
    quorum: usize,
    r: u8,
    sk_tids: Vec<ThreadId>,
    writer: ThreadId,
    sk: Vec<Safekeeper>,
    state: WpState,
    n_connected: usize,
    prop_term: u64,
    vote_request_term: u64,
    donor: Option<usize>,
    donor_last_log_term: u64,
    prop_term_start_lsn: u64,
    truncate_lsn: u64,
    commit_lsn: u64,
    available_lsn: u64,
    prop_term_history: Vec<(u64, u64)>,
    /// WAL bytes of [START_LSN, available_lsn) once known (Fetch reply + own records).
    wal: Vec<u8>,
    fetch: Fetch,
    /// variant wrong-donor: the voters in the order their (given) votes arrived.
    vote_order: Vec<usize>,
    /// End LSNs of the received, unanswered records, in append order.
    pending: VecDeque<u64>,
    appends_received: u8,
    ended: bool,
}

impl WalProposer {
    /// WalProposerCreate (wp.c:91-208): quorum wp.c:181.
    fn WalProposerCreate(me: usize, s: &Setup) -> Self {
        let n = s.n;
        let quorum = match s.variant {
            Variant::BadQuorum => n / 2,
            _ => n / 2 + 1,
        };
        let init_sk = Safekeeper {
            state: SkState::HandshakeRecv,
            greet_term: 0,
            greet_generation: 0,
            vote_response: VoteResponse::default(),
            append_response: AppendResponse::default(),
            start_streaming_at: 0,
            streaming_at: 0,
            expect: 0,
        };
        WalProposer {
            me,
            variant: s.variant,
            n,
            quorum,
            r: s.r,
            sk_tids: s.sks.clone(),
            writer: s.writers[me],
            sk: vec![init_sk; n],
            state: WpState::CollectingTerms,
            n_connected: 0,
            prop_term: 0,
            vote_request_term: 0,
            donor: None,
            donor_last_log_term: 0,
            prop_term_start_lsn: 0,
            truncate_lsn: 0,
            commit_lsn: 0,
            available_lsn: 0,
            prop_term_history: Vec::new(),
            wal: Vec::new(),
            fetch: Fetch::Undecided,
            vote_order: Vec::new(),
            pending: VecDeque::new(),
            appends_received: 0,
            ended: false,
        }
    }

    fn send_to_sk(&mut self, j: usize, bytes: Vec<u8>, expects_reply: bool) {
        if expects_reply {
            self.sk[j].expect += 1;
        }
        send_msg(self.sk_tids[j], SkIn::Pa { from: self.me, bytes });
    }

    fn expected(&self) -> u32 {
        self.sk.iter().map(|s| s.expect).sum()
    }

    fn streaming(&self) -> bool {
        self.state == WpState::Elected && self.fetch == Fetch::Done
    }

    // ---- wire: the proposer's side of the v3 layouts (wp.c PAMessageSerialize 2209-2384) ----

    fn put_cstr(buf: &mut BytesMut, s: &str) {
        buf.put_slice(s.as_bytes());
        buf.put_u8(0);
    }

    /// MembershipConfigurationSerialize (wp.c:2179-2205) of the empty legacy configuration.
    fn put_mconf(buf: &mut BytesMut) {
        buf.put_u32(0); // generation = INVALID_GENERATION
        buf.put_u32(0); // members.len
        buf.put_u32(0); // new_members.len
    }

    /// PAMessageSerialize 'g' (wp.c:2226-2236).
    fn ser_greeting() -> Vec<u8> {
        let mut b = BytesMut::new();
        b.put_u8(b'g');
        Self::put_cstr(&mut b, ID_HEX); // tenant_id
        Self::put_cstr(&mut b, ID_HEX); // timeline_id
        Self::put_mconf(&mut b);
        b.put_u32(PG_VERSION_NUM);
        b.put_u64(0); // system_id
        b.put_u32(WAL_SEGMENT_SIZE as u32);
        b.to_vec()
    }

    /// PAMessageSerialize 'v' (wp.c:2237-2244).
    fn ser_vote_request(term: u64) -> Vec<u8> {
        let mut b = BytesMut::new();
        b.put_u8(b'v');
        b.put_u32(0);
        b.put_u64(term);
        b.to_vec()
    }

    /// PAMessageSerialize 'e' (wp.c:2246-2259).
    fn ser_elected(term: u64, start_streaming_at: u64, th: &[(u64, u64)]) -> Vec<u8> {
        let mut b = BytesMut::new();
        b.put_u8(b'e');
        b.put_u32(0);
        b.put_u64(term);
        b.put_u64(start_streaming_at);
        b.put_u32(th.len() as u32);
        for &(t, l) in th {
            b.put_u64(t);
            b.put_u64(l);
        }
        b.to_vec()
    }

    /// PAMessageSerialize 'a' (wp.c:2260-2276) followed by the WAL bytes (wp.c:1697-1722).
    fn ser_append(term: u64, begin: u64, end: u64, commit: u64, truncate: u64, data: &[u8]) -> Vec<u8> {
        let mut b = BytesMut::new();
        b.put_u8(b'a');
        b.put_u32(0);
        b.put_u64(term);
        b.put_u64(begin);
        b.put_u64(end);
        b.put_u64(commit);
        b.put_u64(truncate);
        b.put_slice(data);
        b.to_vec()
    }

    // ---- AsyncReadMessage (wp.c:2458-2612), v3, the tag the sk state expects ----

    fn get_cstr(s: &mut Bytes) -> String {
        let pos = s.iter().position(|x| *x == 0).expect("harness fault: cstring");
        let r = s.split_to(pos);
        s.advance(1);
        String::from_utf8(r.to_vec()).expect("harness fault: utf8")
    }

    fn need(s: &Bytes, k: usize) {
        assert!(s.remaining() >= k, "harness fault: short message from safekeeper");
    }

    /// MembershipConfigurationDeserialize (wp.c:2412-2440); returns the generation.
    fn get_mconf(s: &mut Bytes) -> u32 {
        Self::need(s, 8);
        let generation = s.get_u32();
        let len = s.get_u32();
        for _ in 0..len {
            Self::need(s, 8);
            s.get_u64();
            Self::get_cstr(s);
            Self::need(s, 2);
            s.get_u16();
        }
        Self::need(s, 4);
        let nlen = s.get_u32();
        for _ in 0..nlen {
            Self::need(s, 8);
            s.get_u64();
            Self::get_cstr(s);
            Self::need(s, 2);
            s.get_u16();
        }
        generation
    }

    fn AsyncReadMessage(&mut self, j: usize, bytes: Vec<u8>, expected_tag: u8) {
        let mut s = Bytes::from(bytes);
        Self::need(&s, 1);
        let tag = s.get_u8();
        // wp.c:2481-2487 resets the connection on a tag mismatch; with one FIFO connection per
        // pair and no reconnection this is unreachable here: a harness fault (NOTES.md D9).
        assert_eq!(tag, expected_tag, "harness fault: unexpected tag from s{j}");
        match tag {
            b'g' => {
                Self::need(&s, 8);
                let _node_id = s.get_u64();
                self.sk[j].greet_generation = Self::get_mconf(&mut s);
                Self::need(&s, 8);
                self.sk[j].greet_term = s.get_u64();
            }
            b'v' => {
                Self::need(&s, 4 + 8 + 1 + 8 + 8 + 4);
                let mut v = VoteResponse {
                    generation: s.get_u32(),
                    term: s.get_u64(),
                    vote_given: s.get_u8() != 0,
                    flush_lsn: s.get_u64(),
                    truncate_lsn: s.get_u64(),
                    term_history: Vec::new(),
                };
                let k = s.get_u32();
                for _ in 0..k {
                    Self::need(&s, 16);
                    let t = s.get_u64();
                    let l = s.get_u64();
                    v.term_history.push((t, l));
                }
                self.sk[j].vote_response = v;
            }
            b'a' => {
                Self::need(&s, 4 + 8 * 6);
                let a = AppendResponse {
                    generation: s.get_u32(),
                    term: s.get_u64(),
                    flush_lsn: s.get_u64(),
                    commit_lsn: s.get_u64(),
                };
                let _ts = s.get_i64();
                let _xmin = s.get_u64();
                let _catalog_xmin = s.get_u64();
                // ParsePageserverFeedbackMessage is not ported: the acceptor sends none.
                assert!(s.is_empty(), "harness fault: pageserver feedback in AppendResponse");
                self.sk[j].append_response = a;
            }
            _ => unreachable!(),
        }
        // pq_getmsgend
        assert!(s.is_empty(), "harness fault: trailing bytes from s{j}");
    }

    // ---- the protocol ----

    /// SendProposerGreeting (wp.c:730-746).
    fn SendProposerGreeting(&mut self, j: usize) {
        self.sk[j].state = SkState::HandshakeRecv;
        self.send_to_sk(j, Self::ser_greeting(), true);
    }

    /// TermsCollected, legacy branch (wp.c:881-891).
    fn TermsCollected(&mut self) -> bool {
        let collected = self.n_connected >= self.quorum;
        if collected {
            self.prop_term += 1;
        }
        collected
    }

    /// RecvAcceptorGreeting (wp.c:921-1037).
    fn RecvAcceptorGreeting(&mut self, j: usize, bytes: Vec<u8>) -> Res {
        self.AsyncReadMessage(j, bytes, b'g');
        // wp.c:950-982: adopt a higher mconf (never: generation 0 everywhere)
        if self.sk[j].greet_generation > 0 {
            if self.state >= WpState::Campaign {
                return Err(Fatal);
            }
            panic!("harness fault: generations are disabled in this cut");
        }
        self.sk[j].state = SkState::WaitVoting;
        self.n_connected += 1;
        if self.state == WpState::CollectingTerms {
            self.prop_term = self.prop_term.max(self.sk[j].greet_term);
            if self.TermsCollected() {
                self.state = WpState::Campaign;
                self.vote_request_term = self.prop_term;
            }
        } else if self.sk[j].greet_term > self.prop_term {
            return Err(Fatal); // wp.c:1006-1011
        }
        if self.state != WpState::CollectingTerms {
            for k in 0..self.n {
                if self.sk[k].state == SkState::WaitVoting {
                    self.SendVoteRequest(k);
                }
            }
        }
        Ok(())
    }

    /// SendVoteRequest (wp.c:1040-1053).
    fn SendVoteRequest(&mut self, j: usize) {
        self.sk[j].state = SkState::WaitVerdict;
        self.send_to_sk(j, Self::ser_vote_request(self.vote_request_term), true);
    }

    /// RecvVoteResponse (wp.c:1056-1112).
    fn RecvVoteResponse(&mut self, j: usize, bytes: Vec<u8>) -> Res {
        self.AsyncReadMessage(j, bytes, b'v');
        let v = &self.sk[j].vote_response;
        if !v.vote_given && (v.term > self.prop_term || self.state == WpState::Campaign) {
            return Err(Fatal); // wp.c:1078-1084
        }
        debug_assert!(v.term == self.prop_term); // wp.c:1085 Assert (off in release)
        self.sk[j].state = SkState::WaitElected;
        if self.state == WpState::Campaign {
            self.vote_order.push(j);
            if self.VotesCollected() {
                self.state = WpState::Elected;
                self.HandleElectedProposer()?;
            }
        } else {
            self.SendProposerElected(j);
        }
        Ok(())
    }

    /// VotesCollected, legacy branch (wp.c:1179-1209); donor wp.c:1190-1198.
    fn VotesCollected(&mut self) -> bool {
        self.prop_term_start_lsn = 0;
        self.donor_last_log_term = 0;
        self.truncate_lsn = 0;
        let mut n_ready = 0;
        for i in 0..self.n {
            if self.sk[i].state == SkState::WaitElected {
                n_ready += 1;
                let llt = self.GetLastLogTerm(i);
                if llt > self.donor_last_log_term
                    || (llt == self.donor_last_log_term
                        && self.sk[i].vote_response.flush_lsn > self.prop_term_start_lsn)
                    || self.donor.is_none()
                {
                    self.donor_last_log_term = llt;
                    self.prop_term_start_lsn = self.sk[i].vote_response.flush_lsn;
                    self.donor = Some(i);
                }
                self.truncate_lsn = self.truncate_lsn.max(self.sk[i].vote_response.truncate_lsn);
            }
        }
        if self.variant == Variant::WrongDonor {
            // the fault: the first voter whose vote arrived, not the highest (last_log_term, flush_lsn)
            let d = self.vote_order[0];
            self.donor = Some(d);
            self.donor_last_log_term = self.GetLastLogTerm(d);
            self.prop_term_start_lsn = self.sk[d].vote_response.flush_lsn;
        }
        n_ready >= self.quorum
    }

    /// HandleElectedProposer (wp.c:1245-1296); recovery download and sync-safekeepers cut;
    /// the donor's WAL by `Fetch` (INTERFACE 5.2 step 4).
    fn HandleElectedProposer(&mut self) -> Res {
        self.ProcessPropStartPos();
        debug_assert!(self.prop_term_start_lsn != 0);
        for i in 0..self.n {
            if self.sk[i].state == SkState::WaitElected {
                self.SendProposerElected(i);
            }
        }
        if self.prop_term_start_lsn > START_LSN {
            let d = self.donor.expect("donor");
            if self.sk[d].state == SkState::Offline {
                return Err(Fatal);
            }
            self.fetch = Fetch::Pending;
            self.sk[d].expect += 1;
            send_msg(
                self.sk_tids[d],
                SkIn::Fetch { from: self.me, term: self.prop_term, lo: START_LSN, hi: self.prop_term_start_lsn },
            );
        } else {
            self.fetch = Fetch::Done;
            self.start_streaming_all();
        }
        Ok(())
    }

    fn start_streaming_all(&mut self) {
        for i in 0..self.n {
            if self.sk[i].state == SkState::SendElectedFlush {
                self.StartStreaming(i);
            }
        }
    }

    /// GetHighestTerm (wp.c:1300-1304).
    fn GetHighestTerm(th: &[(u64, u64)]) -> u64 {
        th.last().map(|e| e.0).unwrap_or(0)
    }

    /// GetLastLogTerm (wp.c:1307-1311).
    fn GetLastLogTerm(&self, j: usize) -> u64 {
        Self::GetHighestTerm(&self.sk[j].vote_response.term_history)
    }

    /// ProcessPropStartPos (wp.c:1334-1434): bootstrap branch 1352-1366 not taken, basebackup
    /// check 1397-1432 cut.
    fn ProcessPropStartPos(&mut self) {
        assert!(self.prop_term_start_lsn != 0, "harness fault: bootstrap branch (wp.c:1352)");
        self.available_lsn = self.prop_term_start_lsn;
        let d = self.donor.expect("donor");
        let mut th = self.sk[d].vote_response.term_history.clone();
        th.push((self.prop_term, self.prop_term_start_lsn));
        self.prop_term_history = th;
    }

    /// SendProposerElected (wp.c:1446-1526).
    fn SendProposerElected(&mut self, j: usize) {
        let th = self.sk[j].vote_response.term_history.clone();
        let ph = &self.prop_term_history;
        let mut idx: isize = 0;
        while (idx as usize) < ph.len().min(th.len()) {
            if ph[idx as usize].0 != th[idx as usize].0 {
                break;
            }
            debug_assert!(ph[idx as usize].1 == th[idx as usize].1); // wp.c:1477 Assert
            idx += 1;
        }
        idx -= 1;
        let start = if idx < 0 {
            ph[0].1
        } else if ph[idx as usize].0 == self.prop_term {
            self.sk[j].vote_response.flush_lsn
        } else {
            let i = idx as usize;
            let prop_end = ph[i + 1].1;
            let sk_end = if i + 1 < th.len() { th[i + 1].1 } else { self.sk[j].vote_response.flush_lsn };
            prop_end.min(sk_end)
        };
        debug_assert!(start <= self.available_lsn); // wp.c:1507 Assert
        self.sk[j].start_streaming_at = start;
        let msg = Self::ser_elected(self.prop_term, start, &self.prop_term_history);
        self.send_to_sk(j, msg, false);
        self.sk[j].state = SkState::SendElectedFlush;
        if self.fetch == Fetch::Done {
            self.StartStreaming(j);
        }
    }

    /// StartStreaming (wp.c:1533-1555).
    fn StartStreaming(&mut self, j: usize) {
        self.sk[j].state = SkState::Active;
        self.sk[j].streaming_at = self.sk[j].start_streaming_at;
        self.SendMessageToNode(j);
    }

    /// SendMessageToNode (wp.c:1564-1573).
    fn SendMessageToNode(&mut self, j: usize) {
        debug_assert!(self.sk[j].state == SkState::Active);
        self.SendAppendRequests(j);
    }

    /// BroadcastAppendRequest (wp.c:1578-1584).
    fn BroadcastAppendRequest(&mut self) {
        for i in 0..self.n {
            if self.sk[i].state == SkState::Active {
                self.SendMessageToNode(i);
            }
        }
    }

    /// SendAppendRequests (wp.c:1648-1776) with PrepareAppendRequest (wp.c:1587-1598); `wal_read`
    /// is the bytes the proposer holds.
    fn SendAppendRequests(&mut self, j: usize) {
        let mut sent_anything = false;
        while self.sk[j].streaming_at != self.available_lsn || !sent_anything {
            sent_anything = true;
            let begin = self.sk[j].streaming_at;
            let end = (begin + MAX_SEND_SIZE as u64).min(self.available_lsn);
            assert!(begin >= START_LSN, "harness fault: streaming range");
            // NOTES.md D23: a start beyond availableLsn (two proposers elected in one term, reached only
            // under bad-quorum) gives end < begin. This is the coordinator's deliberate abstraction of a
            // fault path production does not define: with its Asserts (1507, 1590) compiled out, wp.c
            // (1700, 1710, 1738, 1740) writes a truncated header (req_len < 0), not a complete header.
            // Here the complete header goes without WAL, the acceptor's parse refuses it
            // (sk.rs:581-585) -> Reset. No conclusion about production on this path is drawn from it.
            let data = if end >= begin {
                self.wal[(begin - START_LSN) as usize..(end - START_LSN) as usize].to_vec()
            } else {
                Vec::new()
            };
            let msg = Self::ser_append(self.prop_term, begin, end, self.commit_lsn, self.truncate_lsn, &data);
            self.send_to_sk(j, msg, true);
            self.sk[j].streaming_at = end;
        }
    }

    /// WalProposerBroadcast (wp.c:243-249): the writer's record becomes the next 8 bytes.
    fn WalProposerBroadcast(&mut self, v: u8) {
        self.wal.extend_from_slice(&(v as u64).to_le_bytes());
        self.available_lsn += 8;
        self.pending.push_back(self.available_lsn);
        self.BroadcastAppendRequest();
    }

    /// RecvAppendResponses (wp.c:1787-1844), one message.
    fn RecvAppendResponses(&mut self, j: usize, bytes: Vec<u8>) -> Res {
        self.AsyncReadMessage(j, bytes, b'a');
        if self.sk[j].append_response.generation != 0 {
            return Err(Fatal); // wp.c:1813-1818
        }
        if self.sk[j].append_response.term > self.prop_term {
            return Err(Fatal); // wp.c:1819-1835
        }
        self.HandleSafekeeperResponse();
        Ok(())
    }

    /// CalculateMinFlushLsn (wp.c:1927-1938).
    fn CalculateMinFlushLsn(&self) -> u64 {
        self.sk.iter().map(|s| s.append_response.flush_lsn).min().unwrap_or(0)
    }

    /// GetAcknowledgedByQuorumWALPosition, legacy branch (wp.c:1999-2026).
    fn GetAcknowledgedByQuorumWALPosition(&self) -> u64 {
        let mut responses: Vec<u64> = self
            .sk
            .iter()
            .map(|s| {
                let f = s.append_response.flush_lsn;
                if self.variant == Variant::NoPrevTermRule || f >= self.prop_term_start_lsn { f } else { 0 }
            })
            .collect();
        responses.sort();
        responses[self.n - self.quorum]
    }

    /// HandleSafekeeperResponse (wp.c:2095-2176), sync-safekeepers part cut.
    fn HandleSafekeeperResponse(&mut self) {
        let new_commit = self.GetAcknowledgedByQuorumWALPosition();
        if new_commit > self.commit_lsn {
            self.commit_lsn = new_commit;
            self.BroadcastAppendRequest();
        }
        self.process_safekeeper_feedback();
        let candidate = self.CalculateMinFlushLsn().min(self.commit_lsn);
        if candidate > self.truncate_lsn {
            self.truncate_lsn = candidate;
        }
    }

    /// The api callback process_safekeeper_feedback (wp.c:2115): releases the waiting sessions,
    /// here the writer's records the commit LSN covers, in append order.
    fn process_safekeeper_feedback(&mut self) {
        while let Some(&end) = self.pending.front() {
            if end > self.commit_lsn {
                break;
            }
            self.pending.pop_front();
            let k = u8::try_from((end - START_LSN) / 8).expect("harness fault: position above 255");
            send_msg(self.writer, WRep::Committed(k));
        }
    }

    /// ShutdownConnection (wp.c:359-375) on a `Reset`; no reconnection.
    fn ShutdownConnection(&mut self, j: usize) -> Res {
        self.sk[j].state = SkState::Offline;
        self.sk[j].streaming_at = 0;
        self.sk[j].expect = 0;
        if self.fetch == Fetch::Pending && self.donor == Some(j) {
            return Err(Fatal); // the donor's WAL cannot be fetched (NOTES.md D4)
        }
        let connected = self.sk.iter().filter(|s| s.state != SkState::Offline).count();
        if connected < self.quorum {
            return Err(Fatal); // INTERFACE 5.2 step 7
        }
        Ok(())
    }

    fn handle(&mut self, m: PIn) -> Res {
        match m {
            PIn::Ap { from, bytes } => {
                self.sk[from].expect = self.sk[from].expect.saturating_sub(1);
                match self.sk[from].state {
                    SkState::HandshakeRecv => self.RecvAcceptorGreeting(from, bytes),
                    SkState::WaitVerdict => self.RecvVoteResponse(from, bytes),
                    SkState::Active => self.RecvAppendResponses(from, bytes),
                    s => panic!("harness fault: message from s{from} in state {s:?}"),
                }
            }
            PIn::Reset { from } => self.ShutdownConnection(from),
            PIn::FetchReply { from, data } => {
                self.sk[from].expect = self.sk[from].expect.saturating_sub(1);
                assert!(self.fetch == Fetch::Pending && self.donor == Some(from), "harness fault: Fetch reply");
                match data {
                    None => Err(Fatal), // PAS l.292: the donor left the proposer's term
                    Some(d) => {
                        assert_eq!(d.len() as u64, self.prop_term_start_lsn - START_LSN);
                        self.wal = d;
                        self.fetch = Fetch::Done;
                        self.start_streaming_all();
                        Ok(())
                    }
                }
            }
            PIn::Setup(_) => panic!("harness fault: second Setup at p{}", self.me),
        }
    }

    /// The term ends: `Ended` to every unanswered Append; nothing more to the safekeepers.
    fn term_end(&mut self) {
        self.ended = true;
        while self.pending.pop_front().is_some() {
            send_msg(self.writer, WRep::Ended);
        }
    }

    fn recv_writer() -> WReq {
        recv_tagged_msg_block::<_, WReq>(|_, tag| tag.is_none())
    }

    fn recv_sk() -> PIn {
        recv_tagged_msg_block::<_, PIn>(|_, tag| tag == Some(TAG_P))
    }

    fn run(&mut self) -> ! {
        for j in 0..self.n {
            self.SendProposerGreeting(j);
        }
        loop {
            if self.ended {
                let WReq::Append(_) = Self::recv_writer();
                self.appends_received += 1;
                send_msg(self.writer, WRep::Ended);
                continue;
            }
            // Records wait until streaming starts; while streaming, the next message is the
            // writer's or a safekeeper's (NOTES.md D2).
            let take_writer = self.streaming()
                && self.appends_received < self.r
                && (self.expected() == 0 || nondet());
            let res = if take_writer {
                let WReq::Append(v) = Self::recv_writer();
                self.appends_received += 1;
                self.WalProposerBroadcast(v);
                Ok(())
            } else {
                let m = Self::recv_sk();
                self.handle(m)
            };
            if res.is_err() {
                self.term_end();
            }
        }
    }
}

fn proposer_thread(i: usize) {
    let setup = match WalProposer::recv_sk() {
        PIn::Setup(s) => s,
        m => panic!("harness fault: p{i} got {m:?} before its Setup"),
    };
    let mut wp = WalProposer::WalProposerCreate(i, &setup);
    wp.run();
}

// ------------------------------------------------------------- program ----

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn harness(variant: Variant, n: usize, p: usize, r: u8) -> Prog {
    Arc::new(move || {
        let sks: Vec<ThreadId> =
            (0..n).map(|j| named(&format!("s{j}"), move || safekeeper_thread(j)).thread().id()).collect();
        let props: Vec<ThreadId> =
            (0..p).map(|i| named(&format!("p{i}"), move || proposer_thread(i)).thread().id()).collect();
        let writers: Vec<ThreadId> = (0..p)
            .map(|i| {
                let prop = props[i];
                named(&format!("w{i}"), move || writer(i as u8, p as u8, r, prop)).thread().id()
            })
            .collect();
        let setup = Setup { variant, n, r, sks: sks.clone(), props: props.clone(), writers };
        // main performs no message-passing event: an invisible `init` does.
        named("init", move || {
            for s in &sks {
                send_tagged_msg(*s, TAG_SETUP, SkIn::Setup(setup.clone()));
            }
            for q in &props {
                send_tagged_msg(*q, TAG_P, PIn::Setup(setup.clone()));
            }
        });
    })
}

struct Args {
    variant: Variant,
    n: usize,
    p: usize,
    r: u8,
}

fn usage(a: &[String]) -> ! {
    panic!(
        "neon_sk: ARGS are `<variant> <n> <p> <r>` with variant correct|bad-quorum|wrong-donor|no-prev-term-rule, got {a:?}"
    )
}

fn parse(a: &[String]) -> Args {
    if a.len() != 4 {
        usage(a);
    }
    let variant = match a[0].as_str() {
        "correct" => Variant::Correct,
        "bad-quorum" => Variant::BadQuorum,
        "wrong-donor" => Variant::WrongDonor,
        "no-prev-term-rule" => Variant::NoPrevTermRule,
        _ => usage(a),
    };
    let num = |i: usize| -> usize { a[i].parse().unwrap_or_else(|_| usage(a)) };
    let (n, p, r) = (num(1), num(2), num(3));
    // Values and positions are u8 (INTERFACE 4): every value 1..p*r and every committed position
    // must fit, so p*r <= 255 (review round 1, finding 1). bad-quorum's quorum n/2 must not be 0
    // (finding 5: n = 1 would index past the responses in GetAcknowledgedByQuorumWALPosition).
    if n < 1 || p < 1 || r < 1 || p.checked_mul(r).is_none_or(|x| x > 255) || (variant == Variant::BadQuorum && n / 2 == 0) {
        usage(a);
    }
    Args { variant, n, p, r: r as u8 }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| (0..parse(a).p).map(|i| format!("w{i}")).collect(),
        // The harness until the coordinator copies the reviewed specification in.
        spec: &|a| {
            let x = parse(a);
            harness(x.variant, x.n, x.p, x.r)
        },
        imp: &|a| {
            let x = parse(a);
            harness(x.variant, x.n, x.p, x.r)
        },
    })
}
