//! Benchmark replicated-log-esop20 (candidate N22): the replicated log of Aneris (ESOP 2020,
//! `examples/replicated_log/rep_log.v`): a logger runs two-phase commit (`tpc.v`) over two
//! databases for every client event, against one atomic log. Run by TraceForge's shared main
//! (`traceforge::program`): `count`, `export`, `conf`, `online`.
//!
//! ARGS: `<c> [variant]`; `c >= 1` clients, client i sending the one event `EVENT X`, `EVENT Y`,
//! `EVENT Z`, `EVENT W` (the source has `client1` with `EVENT X` and `client2` with `EVENT Y`);
//! variant (impl side) `correct` (default) or `early-reply` (the logger answers the client right
//! after sending the decision, before collecting the ACKs; the seeded fault, the source ships
//! none). Models: `--model bag` (Aneris's network: unordered, reliable) or `--model fifo`.
//!
//! Visible threads: `client1 .. client<c>`, `auditor` (the same code on both sides). A client sends
//! `Event { from: i, ev }`, receives `Result(v)`, then tells the auditor `Done`. The auditor (added:
//! the source has no reader of the logs) waits for `c` `Done`, sends `Read` twice (impl: to `db1`
//! and `db2`; spec: to `log`) and receives two `Log(s)`.
//! * spec: one invisible `log`: `c + 2` receives; `Event` appends `ev` to its log (string
//!   concatenation, as `fin_handler_log`) and answers `Result(Commit)`; `Read` answers `Log(log)`.
//! * impl: `coordinator` (the source's `logger`, its two sockets one mailbox with type guards:
//!   `Event` on `ip:80`, `Vote`/`Ack` on `ip:1200`), `db1`, `db2` (`db`: `req_handler_log` stores
//!   the event and votes `Commit`, `fin_handler_log` appends on `Commit`, then `Ack`; plus the
//!   added `Read` handler).
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;

const T_EVENT: u32 = 1;
const T_RESULT: u32 = 2;
const T_REQUEST: u32 = 3;
const T_VOTE: u32 = 4;
const T_DEC: u32 = 5;
const T_ACK: u32 = 6;
const T_READ: u32 = 7;
const T_LOG: u32 = 8;
const T_DONE: u32 = 9;

#[derive(Clone, Debug, PartialEq)]
enum Verdict {
    Commit,
    Abort,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> logger: the event, with the sender (Aneris's `ReceiveFrom` gives it).
    Event { from: i32, ev: String },
    /// logger -> client: `"COMMIT"` or `"ABORT"`.
    Result(Verdict),
    /// logger -> db: `"REQUEST_" ^ ev` (the tag is the message's).
    Request(String),
    /// db -> logger: `req_handler_log`'s vote (always `"COMMIT"`).
    Vote(Verdict),
    /// logger -> db: `dec_handler_log`'s decision.
    Dec(Verdict),
    /// db -> logger: `"ACK"`.
    Ack,
    /// auditor -> db (spec: -> log): read the log (added).
    Read,
    /// db (spec: log) -> auditor: the log (added).
    Log(String),
    /// client -> auditor: answered (added).
    Done,
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn tag(m: &Msg) -> u32 {
    match m {
        Msg::Event { .. } => T_EVENT,
        Msg::Result(_) => T_RESULT,
        Msg::Request(_) => T_REQUEST,
        Msg::Vote(_) => T_VOTE,
        Msg::Dec(_) => T_DEC,
        Msg::Ack => T_ACK,
        Msg::Read => T_READ,
        Msg::Log(_) => T_LOG,
        Msg::Done => T_DONE,
    }
}

fn send(tx: &Tx, m: Msg) {
    tx.send_tagged_msg(tag(&m), m);
}

fn recv(rx: &Rx, tags: &'static [u32]) -> Msg {
    rx.recv_tagged_msg_block(move |t| t.is_some_and(|t| tags.contains(&t)))
}

fn event(i: usize) -> String {
    let names = ["X", "Y", "Z", "W", "V", "U"];
    format!("EVENT {}", names[i - 1])
}

// Inboxes: 0 coordinator (spec: log), 1 db1, 2 db2, 3 auditor, 3 + i client i.
const COORD: usize = 0;
const AUDITOR: usize = 3;

/// `client` (one event), then `Done` to the auditor.
fn client(i: usize, rx: Rx, t: Vec<Tx>) {
    send(&t[COORD], Msg::Event { from: i as i32, ev: event(i) });
    let _ = recv(&rx, &[T_RESULT]);
    send(&t[AUDITOR], Msg::Done);
}

/// The added reader: after every client is answered, read both replicas' logs.
fn auditor(c: usize, rx: Rx, dbs: [Tx; 2]) {
    for _ in 0..c {
        let _ = recv(&rx, &[T_DONE]);
    }
    for d in &dbs {
        send(d, Msg::Read);
    }
    for _ in 0..2 {
        let _ = recv(&rx, &[T_LOG]);
    }
}

/// The specification: one atomic log.
fn spec_log(c: usize, rx: Rx, t: Vec<Tx>) {
    let mut log = String::new();
    for _ in 0..c + 2 {
        match recv(&rx, &[T_EVENT, T_READ]) {
            Msg::Event { from, ev } => {
                log.push_str(&ev);
                send(&t[AUDITOR + from as usize], Msg::Result(Verdict::Commit));
            }
            Msg::Read => send(&t[AUDITOR], Msg::Log(log.clone())),
            _ => unreachable!(),
        }
    }
}

/// `logger`: for each client event, `tpc_coordinate` over `[db1; db2]`, then the reply.
fn coordinator(c: usize, early_reply: bool, rx: Rx, t: Vec<Tx>) {
    for _ in 0..c {
        // listen "socket" (ip:80)
        let Msg::Event { from, ev } = recv(&rx, &[T_EVENT]) else { unreachable!() };
        // tpc_coordinate: request to every node, collect `count` messages on ip:1200
        for d in 1..=2 {
            send(&t[d], Msg::Request(ev.clone()));
        }
        let mut all_commit = true;
        for _ in 0..2 {
            if let Msg::Vote(Verdict::Abort) = recv(&rx, &[T_VOTE, T_ACK]) {
                all_commit = false;
            }
        }
        // dec_handler_log: COMMIT iff every vote is COMMIT
        let result = if all_commit { Verdict::Commit } else { Verdict::Abort };
        for d in 1..=2 {
            send(&t[d], Msg::Dec(result.clone()));
        }
        if early_reply {
            send(&t[AUDITOR + from as usize], Msg::Result(result.clone()));
        }
        // collect `count` ACKs on ip:1200
        for _ in 0..2 {
            let _ = recv(&rx, &[T_VOTE, T_ACK]);
        }
        if !early_reply {
            // SendTo "socket" "result" "sender"
            send(&t[AUDITOR + from as usize], Msg::Result(result));
        }
    }
}

/// `db` (`tpc_participant_setup` with `req_handler_log`, `fin_handler_log`) for `c` rounds, and
/// the added `Read` handler: `2c + 1` receives on its one socket.
fn db(c: usize, rx: Rx, t: Vec<Tx>) {
    let mut waiting = String::new();
    let mut log = String::new();
    for _ in 0..2 * c + 1 {
        match recv(&rx, &[T_REQUEST, T_DEC, T_READ]) {
            Msg::Request(ev) => {
                waiting = ev;
                send(&t[COORD], Msg::Vote(Verdict::Commit));
            }
            Msg::Dec(d) => {
                if d == Verdict::Commit {
                    log.push_str(&waiting);
                }
                send(&t[COORD], Msg::Ack);
            }
            Msg::Read => send(&t[AUDITOR], Msg::Log(log.clone())),
            _ => unreachable!(),
        }
    }
}

fn program(c: usize, imp: bool, early_reply: bool) {
    let (tx, rxs): (Vec<Tx>, Vec<Rx>) = (0..4 + c).map(|_| Builder::<Msg>::new().build()).unzip();
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    let mut hs = Vec::new();
    let r0 = rxs[COORD].take().unwrap();
    let t = tx.clone();
    if imp {
        hs.push(named("coordinator", move || coordinator(c, early_reply, r0, t)));
        for d in 1..=2 {
            let r = rxs[d].take().unwrap();
            let t = tx.clone();
            hs.push(named(&format!("db{d}"), move || db(c, r, t)));
        }
    } else {
        hs.push(named("log", move || spec_log(c, r0, t)));
    }
    let ra = rxs[AUDITOR].take().unwrap();
    let dbs = if imp {
        [tx[1].clone(), tx[2].clone()]
    } else {
        [tx[COORD].clone(), tx[COORD].clone()]
    };
    hs.push(named("auditor", move || auditor(c, ra, dbs)));
    for i in 1..=c {
        let r = rxs[AUDITOR + i].take().unwrap();
        let t = tx.clone();
        hs.push(named(&format!("client{i}"), move || client(i, r, t)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

const PROGRAM: &str = "replicated-log-esop20";
const USAGE: &str = "`<c> [correct|early-reply]`, 1 <= c <= 6";

struct Args {
    c: usize,
    early_reply: bool,
}

fn parse(a: &[String]) -> Args {
    if a.is_empty() || a.len() > 2 {
        panic!("{PROGRAM}: ARGS are {USAGE}, got {a:?}");
    }
    let c: usize = a[0]
        .parse()
        .unwrap_or_else(|_| panic!("{PROGRAM}: <c> must be a number; ARGS are {USAGE}"));
    if !(1..=6).contains(&c) {
        panic!("{PROGRAM}: c must be in 1..=6; ARGS are {USAGE}");
    }
    let early_reply = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => false,
        "early-reply" => true,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args { c, early_reply }
}

fn visible(c: usize) -> Vec<String> {
    let mut v: Vec<String> = (1..=c).map(|i| format!("client{i}")).collect();
    v.push("auditor".to_string());
    v
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| visible(parse(a).c),
        spec: &|a| {
            let c = parse(a).c;
            std::sync::Arc::new(move || program(c, false, false))
        },
        imp: &|a| {
            let p = parse(a);
            std::sync::Arc::new(move || program(p.c, true, p.early_reply))
        },
    })
}
