//! Benchmark n-buyer-kragl (candidate N20): the N-buyer protocol of Kragl et al., PLDI 2020
//! (`NBuyer.bpl`), against its sequentialization (`MAIN3`/`INV3`: the buyers act in pid order).
//! Run by TraceForge's shared main (`traceforge::program`): `count`, `export`, `conf`, `online`.
//!
//! ARGS: `<n> [variant]`; `n >= 2` buyers, `PRICE = 2`, `WALLET = 1`; variant (impl side)
//! `correct` (default) or `no-subtract` (a first/middle buyer forwards the remainder it received,
//! not `rem - amount`: the seeded fault, `NBuyer.bpl` ships none). Model: `--model bag` (the
//! source's channels are multisets).
//!
//! Visible thread: `ledger` (added; the source's property is on the ghost `contribution` map and
//! the seller's `assert_sum`): it receives `Contrib { i, a }` from every buyer and `Final(d)` from
//! the seller, in any order, `n + 1` blocking receives.
//! * spec: one invisible `oracle`: for i = 1..n, `a_i` = `min(WALLET, rem)` or 0 (`nondet()`, the
//!   source's `if (*)`), `rem -= a_i`, send `Contrib { i, a: a_i }`; then `Final(sum == PRICE)`.
//! * impl: `seller`, `b1 .. bn` (the source's layer-1 procedures): b1 sends `Req`; the seller
//!   receives it, sends `Quote(PRICE)` to every buyer, receives `Dec(d)` and sends `Final(d)`;
//!   b1 receives its quote, chooses, sends `Contrib` and `Rem(q - a)` to b2; b_i (1 < i < n)
//!   receives `Quote`, `Rem(r)`, chooses, sends `Contrib`, `Rem(r - a)`; b_n receives `Quote`,
//!   `Rem(r)`, chooses, sends `Contrib`, then `Dec(a == r)` to the seller.
//! Every thread has one inbox; receives select by message tag (the Java twin's type tests).
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

const PRICE: i32 = 2;
const WALLET: i32 = 1;

const T_REQ: u32 = 1;
const T_QUOTE: u32 = 2;
const T_REM: u32 = 3;
const T_DEC: u32 = 4;
const T_CONTRIB: u32 = 5;
const T_FINAL: u32 = 6;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// b1 -> seller: `send_req` (no payload).
    Req,
    /// seller -> buyer: `send_quote(i, price)`.
    Quote(i32),
    /// buyer -> next buyer: `send_rem(i + 1, r)`.
    Rem(i32),
    /// last buyer -> seller: `send_dec(amount == rem)`.
    Dec(bool),
    /// buyer -> ledger: `set_contribution(i, a)`, made a message.
    Contrib { i: i32, a: i32 },
    /// seller -> ledger: the decision the seller received.
    Final(bool),
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv(rx: &Rx, tag: u32) -> Msg {
    rx.recv_tagged_msg_block(move |t| t == Some(tag))
}

fn tag(m: &Msg) -> u32 {
    match m {
        Msg::Req => T_REQ,
        Msg::Quote(_) => T_QUOTE,
        Msg::Rem(_) => T_REM,
        Msg::Dec(_) => T_DEC,
        Msg::Contrib { .. } => T_CONTRIB,
        Msg::Final(_) => T_FINAL,
    }
}

fn send(tx: &Tx, m: Msg) {
    tx.send_tagged_msg(tag(&m), m);
}

/// `if (*) { amount := min(wallet, rem); } else { amount := 0; }`
fn choose(rem: i32) -> i32 {
    if nondet() {
        WALLET.min(rem)
    } else {
        0
    }
}

/// The visible observer: `n + 1` receives of anything.
fn ledger(n: usize, rx: Rx) {
    for _ in 0..=n {
        let _m: Msg = rx.recv_msg_block();
    }
}

fn spec(n: usize) {
    let (ltx, lrx) = Builder::<Msg>::new().build();
    let l = named("ledger", move || ledger(n, lrx));
    let o = named("oracle", move || {
        let mut rem = PRICE;
        let mut sum = 0;
        for i in 1..=n as i32 {
            let a = choose(rem);
            rem -= a;
            sum += a;
            send(&ltx, Msg::Contrib { i, a });
        }
        send(&ltx, Msg::Final(sum == PRICE));
    });
    l.join().unwrap();
    o.join().unwrap();
}

fn imp(n: usize, subtract: bool) {
    // inboxes: 0 seller, 1..=n buyers, n + 1 ledger
    let (tx, rxs): (Vec<Tx>, Vec<Rx>) = (0..n + 2).map(|_| Builder::<Msg>::new().build()).unzip();
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    let mut hs = Vec::new();
    let lrx = rxs[n + 1].take().unwrap();
    hs.push(named("ledger", move || ledger(n, lrx)));
    let srx = rxs[0].take().unwrap();
    let t = tx.clone();
    hs.push(named("seller", move || {
        // sellerInit
        let _ = recv(&srx, T_REQ);
        for i in 1..=n {
            send(&t[i], Msg::Quote(PRICE));
        }
        // sellerFinish
        let Msg::Dec(d) = recv(&srx, T_DEC) else { unreachable!() };
        send(&t[n + 1], Msg::Final(d));
    }));
    for i in 1..=n {
        let rx = rxs[i].take().unwrap();
        let t = tx.clone();
        hs.push(named(&format!("b{i}"), move || {
            if i == 1 {
                // firstBuyerInit, firstBuyer
                send(&t[0], Msg::Req);
            }
            let Msg::Quote(q) = recv(&rx, T_QUOTE) else { unreachable!() };
            let r = if i == 1 {
                q
            } else {
                let Msg::Rem(r) = recv(&rx, T_REM) else { unreachable!() };
                r
            };
            let a = choose(r);
            send(&t[n + 1], Msg::Contrib { i: i as i32, a });
            if i < n {
                let fwd = if subtract { r - a } else { r };
                send(&t[i + 1], Msg::Rem(fwd));
            } else {
                // lastBuyer
                send(&t[0], Msg::Dec(a == r));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
}

const PROGRAM: &str = "n-buyer-kragl";
const USAGE: &str = "`<n> [correct|no-subtract]`, n >= 2";

struct Args {
    n: usize,
    subtract: bool,
}

fn parse(a: &[String]) -> Args {
    if a.is_empty() || a.len() > 2 {
        panic!("{PROGRAM}: ARGS are {USAGE}, got {a:?}");
    }
    let n: usize = a[0]
        .parse()
        .unwrap_or_else(|_| panic!("{PROGRAM}: <n> must be a number; ARGS are {USAGE}"));
    if n < 2 {
        panic!("{PROGRAM}: n must be at least 2 (the source's axiom n > 1)");
    }
    let subtract = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => true,
        "no-subtract" => false,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args { n, subtract }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| vec!["ledger".to_string()],
        spec: &|a| {
            let p = parse(a);
            std::sync::Arc::new(move || spec(p.n))
        },
        imp: &|a| {
            let p = parse(a);
            std::sync::Arc::new(move || imp(p.n, p.subtract))
        },
    })
}
