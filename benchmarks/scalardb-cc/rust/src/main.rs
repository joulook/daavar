//! S_cc: the client-visible content of ScalarDB's Consensus Commit (the TLA+ model CCSpec.tla, Model_1 snapshot:
//! `Consistent`, the atomic commitment of one transaction, as other transactions' reads see it; plus the isolation
//! level of ConsensusCommit l.41-48 and docs/consensus-commit.mdx "Isolation levels"), the hand-written specification
//! of the ScalarDB study. Interface: plan/case-studies/scalardb/INTERFACE.md (message types and client loop verbatim);
//! contract S1-S7 at its end; the reply rules and their clauses are in NOTES.md. One invisible object `cc` holding the
//! committed version chain of every key and, per transaction, its reads, writes and status. Entries: `spec` = this
//! object; `imp` = the same. ARGS `<pattern> <k> <iso> [fifo|bag]`.
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[derive(Clone, Debug, PartialEq)] enum Req { Begin, Read(u8), Write(u8, u8), Commit }
#[allow(dead_code)] // Unknown: kept in the verbatim interface type, never sent by S_cc (S7)
#[derive(Clone, Debug, PartialEq)] enum Rep { Begun, Value(u8, u8), Written(u8), Committed, Aborted, Unknown }

fn client(id: u8, keys: u8, writes: &[u8], h: ThreadId) {
    send_msg(h, Req::Begin); let Rep::Begun = recv_msg_block::<Rep>() else { panic!("Begun expected") };
    for k in 1..=keys {
        send_msg(h, Req::Read(k));
        match recv_msg_block::<Rep>() { Rep::Value(_, _) => {}, Rep::Aborted => return, _ => panic!("Value or Aborted expected") }
    }
    for &k in writes { send_msg(h, Req::Write(k, id)); let Rep::Written(_) = recv_msg_block::<Rep>() else { panic!("Written expected") }; }
    send_msg(h, Req::Commit);
    match recv_msg_block::<Rep>() { Rep::Committed | Rep::Aborted | Rep::Unknown => {}, _ => panic!("Committed, Aborted or Unknown expected") }
}

#[derive(Clone, Debug, PartialEq)]
struct Setup { cs: Vec<ThreadId> }
const SETUP_TAG: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Pattern { Lost, Skew, Both, Mixed, MixedRo }
#[derive(Clone, Copy, Debug, PartialEq)]
enum Iso { Snapshot, Serializable }

/// Write sets of c1, c2 (and c3, empty) for a pattern (INTERFACE.md table).
fn write_sets(p: Pattern) -> Vec<Vec<u8>> {
    match p {
        Pattern::Lost => vec![vec![1], vec![1]],
        Pattern::Skew => vec![vec![1], vec![2]],
        Pattern::Both => vec![vec![1, 2], vec![1, 2]],
        Pattern::Mixed => vec![vec![1, 2], vec![2]],
        Pattern::MixedRo => vec![vec![1, 2], vec![2], vec![]],
    }
}

/// Status of transaction t_i at the object.
#[derive(Clone, Copy, Debug, PartialEq)]
enum St { Idle, Active, Outstanding, Committed, Aborted }

#[derive(Clone, Copy, Debug, PartialEq)]
enum Act { Recv(usize), Decide(usize) }

/// Receive the next untagged request of `from`.
fn recv_from(from: ThreadId) -> Req { recv_tagged_msg_block::<_, Req>(move |s, tag| s == from && tag.is_none()) }

/// The object's state. Transaction ids: 0 = t0 (the setup transaction), i + 1 = t_{i+1} of client c_{i+1}; the value
/// a transaction writes is its id, so a version of a key is named by its writer.
struct Obj {
    keys: usize,
    iso: Iso,
    /// chain[k]: the committed version chain of key k+1, oldest first; starts [0] (t0's value 0).
    chain: Vec<Vec<u8>>,
    st: Vec<St>,
    /// read[i][k]: the writer of the version t_{i+1} read on key k+1.
    read: Vec<Vec<Option<u8>>>,
    /// wr[i]: the keys (1-based) t_{i+1} has written, in order.
    wr: Vec<Vec<u8>>,
    /// reads received so far (the client reads k1..kK in order).
    nread: Vec<usize>,
}

impl Obj {
    fn latest(&self, k: usize) -> u8 { *self.chain[k].last().unwrap() }

    /// S6: a Read(k) may be answered Aborted only while another transaction that writes k has an outstanding commit.
    fn read_may_abort(&self, i: usize, k: u8) -> bool {
        (0..self.st.len()).any(|j| j != i && self.st[j] == St::Outstanding && self.wr[j].contains(&k))
    }

    /// S4 (both levels) and S5 (serializable): may t_{i+1} commit now?
    fn may_commit(&self, i: usize) -> bool {
        let me = (i + 1) as u8;
        // S4 first updater wins: every key written was read at the version still latest.
        for &k in &self.wr[i] {
            let k0 = (k - 1) as usize;
            if self.read[i][k0] != Some(self.latest(k0)) { return false; }
        }
        if self.iso == Iso::Snapshot { return true; }
        // S5: the serialization graph of the committed transactions plus t_{i+1} stays acyclic.
        let n = self.st.len() + 1; // nodes 0 (t0) ..= st.len()
        let ins: Vec<bool> = (0..n).map(|t| t == 0 || t == i + 1 || self.st[t - 1] == St::Committed).collect();
        let mut chains = self.chain.clone();
        for &k in &self.wr[i] { chains[(k - 1) as usize].push(me); }
        let mut adj = vec![vec![false; n]; n];
        for k in 0..self.keys {
            let c = &chains[k];
            for w in c.windows(2) { adj[w[0] as usize][w[1] as usize] = true; } // ww
            for r in 1..n {
                if !ins[r] { continue; }
                let Some(v) = self.read[r - 1][k] else { continue };
                if v as usize != r { adj[v as usize][r] = true; } // wr
                let pos = c.iter().position(|&x| x == v).expect("read version in chain");
                if let Some(&nx) = c.get(pos + 1) { if nx as usize != r { adj[r][nx as usize] = true; } } // rw
            }
        }
        !cyclic(&adj)
    }
}

fn cyclic(adj: &[Vec<bool>]) -> bool {
    let n = adj.len();
    // 0 = unvisited, 1 = on stack, 2 = done
    fn dfs(u: usize, adj: &[Vec<bool>], col: &mut [u8]) -> bool {
        col[u] = 1;
        for v in 0..adj.len() {
            if adj[u][v] && (col[v] == 1 || (col[v] == 0 && dfs(v, adj, col))) { return true; }
        }
        col[u] = 2;
        false
    }
    let mut col = vec![0u8; n];
    (0..n).any(|u| col[u] == 0 && dfs(u, adj, &mut col))
}

/// The object. Enabled actions at each step (one is chosen by nondet; NOTES.md gives the clause of each rule):
/// - Recv(i): receive c_i's next requests (c_i not ended, no Commit of c_i outstanding), up to and including the next
///   Read or Commit (a Begin goes with the Read(1) after it, the Writes go with the Commit after them):
///   `Begin` -> `Begun`; `Write(k, v)` -> recorded, `Written(k)`; `Read(k)` -> `Value(k, latest committed value of k)`,
///   or, if another transaction writing k has an outstanding commit, `Aborted` by nondet (t_i aborts, c_i ends);
///   `Commit` -> the commit is outstanding (no reply yet).
/// - Decide(i): t_i's commit decision and the reply: `Committed` (the writes join the version chains) if S4 and, under
///   serializable, S5 allow it, by nondet; `Aborted` otherwise or by nondet.
/// The loop ends when every client has ended.
fn object(keys: usize, iso: Iso, ws: Vec<Vec<u8>>) {
    let Setup { cs } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let m = cs.len();
    assert!(m == ws.len());
    let mut o = Obj {
        keys, iso,
        chain: vec![vec![0]; keys],
        st: vec![St::Idle; m],
        read: vec![vec![None; keys]; m],
        wr: vec![Vec::new(); m],
        nread: vec![0; m],
    };
    loop {
        let mut acts: Vec<Act> = Vec::new();
        for i in 0..m {
            match o.st[i] {
                St::Idle | St::Active => acts.push(Act::Recv(i)),
                St::Outstanding => acts.push(Act::Decide(i)),
                _ => {}
            }
        }
        if acts.is_empty() { break; }
        let mut chosen = acts[acts.len() - 1];
        for &x in &acts[..acts.len() - 1] { if nondet() { chosen = x; break; } }
        match chosen {
            // One Recv step receives requests of c_i until it has handled a Read or a Commit: a Begin is handled
            // together with the Read(1) that follows it, the Writes together with the Commit (NOTES.md D-r).
            Act::Recv(i) => loop {
                let c = cs[i];
                match recv_from(c) {
                    Req::Begin => { assert!(o.st[i] == St::Idle); o.st[i] = St::Active; send_msg(c, Rep::Begun); continue; }
                    Req::Read(k) => {
                        assert!(o.st[i] == St::Active && k as usize == o.nread[i] + 1 && (k as usize) <= keys);
                        o.nread[i] += 1;
                        if o.read_may_abort(i, k) && nondet() {
                            o.st[i] = St::Aborted;
                            send_msg(c, Rep::Aborted);
                        } else {
                            let v = o.latest((k - 1) as usize);
                            o.read[i][(k - 1) as usize] = Some(v);
                            send_msg(c, Rep::Value(k, v));
                        }
                    }
                    Req::Write(k, v) => {
                        assert!(o.st[i] == St::Active && o.nread[i] == keys && v as usize == i + 1);
                        assert!(ws[i].get(o.wr[i].len()) == Some(&k));
                        o.wr[i].push(k);
                        send_msg(c, Rep::Written(k));
                        continue;
                    }
                    Req::Commit => {
                        assert!(o.st[i] == St::Active && o.nread[i] == keys && o.wr[i] == ws[i]);
                        o.st[i] = St::Outstanding;
                    }
                }
                break;
            },
            Act::Decide(i) => {
                if o.may_commit(i) && nondet() {
                    for &k in &o.wr[i] { o.chain[(k - 1) as usize].push((i + 1) as u8); }
                    o.st[i] = St::Committed;
                    send_msg(cs[i], Rep::Committed);
                } else {
                    o.st[i] = St::Aborted;
                    send_msg(cs[i], Rep::Aborted);
                }
            }
        }
    }
}

type Prog = traceforge::program::Entry;
fn prog<G: Fn() + Send + Sync + 'static>(f: G) -> Prog { Arc::new(f) }
fn named<G: FnOnce() + Send + 'static>(n: &str, f: G) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(p: Pattern, keys: u8, iso: Iso) -> Prog {
    prog(move || {
        let ws = write_sets(p);
        let obj = { let ws = ws.clone(); named("cc", move || object(keys as usize, iso, ws)).thread().id() };
        let cs: Vec<ThreadId> = ws.iter().enumerate().map(|(i, w)| {
            let w = w.clone();
            let id = (i + 1) as u8;
            named(&format!("c{id}"), move || client(id, keys, &w, obj)).thread().id()
        }).collect();
        named("init", move || send_tagged_msg(obj, SETUP_TAG, Setup { cs: cs.clone() }));
    })
}
fn parse(a: &[String]) -> (Pattern, u8, Iso) {
    let fail = |why: String| -> ! {
        eprintln!("scalardb-cc: {why}; ARGS are `<pattern> <k> <iso> [fifo|bag]` with pattern in lost|skew|both|mixed|mixedro, k in 1..2 (k = 1 only with lost), iso in snapshot|serializable; got {a:?}");
        std::process::exit(2)
    };
    if a.len() < 3 || a.len() > 4 { fail("three or four ARGS expected".to_string()); }
    let p = match a[0].as_str() {
        "lost" => Pattern::Lost, "skew" => Pattern::Skew, "both" => Pattern::Both,
        "mixed" => Pattern::Mixed, "mixedro" => Pattern::MixedRo,
        x => fail(format!("unknown pattern `{x}`")),
    };
    let k: u8 = a[1].parse().unwrap_or_else(|_| fail("k is not a number".to_string()));
    if !(1..=2).contains(&k) { fail(format!("k = {k} is not in 1..2")); }
    if k == 1 && p != Pattern::Lost { fail(format!("pattern `{}` needs k = 2", a[0])); }
    let iso = match a[2].as_str() {
        "snapshot" => Iso::Snapshot, "serializable" => Iso::Serializable,
        x => fail(format!("unknown isolation level `{x}`")),
    };
    if a.len() == 4 && a[3] != "fifo" && a[3] != "bag" { fail(format!("unknown delivery `{}`", a[3])); }
    (p, k, iso)
}
fn visible(a: &[String]) -> Vec<String> {
    let (p, _, _) = parse(a);
    (1..=write_sets(p).len()).map(|i| format!("c{i}")).collect()
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &visible,
        spec: &|a| { let (p, k, iso) = parse(a); op_spec(p, k, iso) },
        imp: &|a| { let (p, k, iso) = parse(a); op_spec(p, k, iso) },
    })
}
