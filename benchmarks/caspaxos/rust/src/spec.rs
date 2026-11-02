//! The hand-written specification of the CASPaxos register (coordinator, 2026-11-02): a linearizable
//! register that applies each client's compare-and-set in some order and answers `Done { value }` with the
//! new value, or `Fail {}`; a failed request may or may not have taken effect (a proposer that gives up
//! after conflicts may still have had its value accepted by a quorum). Visible: the clients `c0..c{K-1}`,
//! with the interface's two messages. Invisible: `reg`, the one register thread, which serves requests in
//! their arrival order (the API's blocking receive picks any pending one) and replies at once. The
//! register spawns the clients so that it knows their ids (the interface's spawn order is for the
//! implementations; the specification's spawn edges are invisible).
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Req {
    Cas { expected: i32, value: i32 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Rep {
    Done { value: i32 },
    Fail {},
}

pub fn spec(acceptors: usize, clients: usize) {
    let _ = acceptors; // the specification has no acceptors
    let reg = thread::Builder::new()
        .name("reg".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut hs = Vec::new();
            let mut ids: Vec<ThreadId> = Vec::new();
            for i in 0..clients {
                let idx = i as i32;
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Req::Cas { expected: idx, value: idx + 1 });
                        let _r: Rep = recv_msg_block::<Rep>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut state: i32 = 0;
            for _ in 0..clients {
                let Req::Cas { expected, value } = recv_msg_block::<Req>();
                let from = ids[expected as usize]; // client i asks "if i then i + 1" (INTERFACE.md)
                let applied = if state == expected { value } else { state };
                if nondet() {
                    state = applied;
                    send_msg(from, Rep::Done { value: state });
                } else {
                    if nondet() {
                        state = applied;
                    }
                    send_msg(from, Rep::Fail {});
                }
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    reg.join().unwrap();
}
