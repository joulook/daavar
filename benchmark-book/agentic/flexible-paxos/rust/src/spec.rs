//! The hand-written specification of single-decree consensus (coordinator, 2026-11-02): one invisible
//! `cons` thread receives the clients' proposals in arrival order; at any point after at least one
//! proposal it may decide (the API's choice) one of the proposals received so far; a client's reply is
//! `Decided { value }` with the decided value, or `Fail {}` (the API's choice; a proposer may give up).
//! Agreement: one value for every `Decided`; validity: a proposed value, proposed before the decision
//! (causally, through `cons`). Visible: the clients `c0..c{K-1}` with the port's `Msg` type. `cons`
//! spawns the clients so that it knows their ids (spawn edges are invisible).
use super::Msg; // the port's own message type: the in-process check compares typed values
use traceforge::thread::{self, ThreadId};
use traceforge::*;

fn pick(n: usize) -> usize {
    // one of 0..n by the API's choice
    for i in 0..n {
        if i + 1 == n || nondet() {
            return i;
        }
    }
    n - 1
}

pub fn spec(acceptors: usize, clients: usize) {
    let _ = acceptors;
    let cons = thread::Builder::new()
        .name("cons".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut hs = Vec::new();
            let mut ids: Vec<ThreadId> = Vec::new();
            for i in 0..clients {
                let idx = i as i32;
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Msg::Propose { from: idx, value: idx + 1 });
                        let _r: Msg = recv_msg_block::<Msg>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut proposals: Vec<i32> = Vec::new();
            let mut pending: Vec<usize> = Vec::new();
            let mut decided: Option<i32> = None;
            let reply_pending = |decided: Option<i32>, pending: &mut Vec<usize>, final_round: bool| {
                let mut keep = Vec::new();
                for &c in pending.iter() {
                    match decided {
                        Some(v) => {
                            if nondet() {
                                send_msg(ids[c], Msg::Decided { value: v });
                            } else {
                                send_msg(ids[c], Msg::Fail {});
                            }
                        }
                        None => {
                            if final_round || nondet() {
                                send_msg(ids[c], Msg::Fail {});
                            } else {
                                keep.push(c);
                            }
                        }
                    }
                }
                *pending = keep;
            };
            for _ in 0..clients {
                let Msg::Propose { from, value } = recv_msg_block::<Msg>() else { unreachable!("a proposal") };
                proposals.push(value);
                pending.push(from as usize);
                if decided.is_none() && nondet() {
                    decided = Some(proposals[pick(proposals.len())]);
                }
                reply_pending(decided, &mut pending, false);
            }
            if decided.is_none() && !pending.is_empty() && nondet() {
                decided = Some(proposals[pick(proposals.len())]);
            }
            reply_pending(decided, &mut pending, true);
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    cons.join().unwrap();
}
