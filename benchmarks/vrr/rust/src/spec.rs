//! The hand-written specification of the replicated counter (coordinator, 2026-11-02): one invisible
//! `svc` thread serves the clients' requests in arrival order (the replicated state machine: every
//! operation passes through the primary, so the causal order exists on both sides) and replies with the
//! counter's new value. Visible: the clients `c0..c{K-1}` with the port's `Msg` type. `svc` spawns the
//! clients so that it knows their ids (spawn edges are invisible).
use super::Msg; // the port's own message type: the in-process check compares typed values
use traceforge::thread::{self, ThreadId};
use traceforge::*;

pub fn spec(replicas: usize, clients: usize) {
    let _ = replicas;
    let svc = thread::Builder::new()
        .name("svc".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut hs = Vec::new();
            let mut ids: Vec<ThreadId> = Vec::new();
            for i in 0..clients {
                let idx = i as i32;
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Msg::Request { from: idx, op: idx + 1 });
                        let _r: Msg = recv_msg_block::<Msg>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut counter: i32 = 0;
            for _ in 0..clients {
                let Msg::Request { from, op } = recv_msg_block::<Msg>() else { unreachable!("a request") };
                counter += op;
                send_msg(ids[from as usize], Msg::Reply { result: counter });
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    svc.join().unwrap();
}
