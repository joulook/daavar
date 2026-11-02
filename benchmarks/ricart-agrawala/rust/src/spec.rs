//! The hand-written specification of mutual exclusion (coordinator, 2026-11-02): one invisible `lock`
//! thread serves the clients' requests in arrival order: an `Enter` is granted at once when the critical
//! section is free, else queued and granted when the holder's `Exit` arrives (any waiting client may be
//! next: the API's choice); `Exit` is answered with `Released` after the lock has passed the section on.
//! Visible: the clients `c0..c{N-1}` with the port's `Msg` type. The lock spawns the clients so that it
//! knows their ids (spawn edges are invisible).
use super::Msg; // the port's own message type: the in-process check compares typed values
use traceforge::thread::{self, ThreadId};
use traceforge::*;

pub fn spec(nodes: usize) {
    let lock = thread::Builder::new()
        .name("lock".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut hs = Vec::new();
            let mut ids: Vec<ThreadId> = Vec::new();
            for i in 0..nodes {
                let idx = i as i32;
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Msg::Enter { from: idx });
                        let _g: Msg = recv_msg_block::<Msg>();
                        send_msg(me, Msg::Exit { from: idx });
                        let _r: Msg = recv_msg_block::<Msg>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut holder: Option<usize> = None;
            let mut waiting: Vec<usize> = Vec::new();
            for _ in 0..(2 * nodes) {
                match recv_msg_block::<Msg>() {
                    Msg::Enter { from } => {
                        let c = from as usize;
                        if holder.is_none() {
                            holder = Some(c);
                            send_msg(ids[c], Msg::Granted {});
                        } else {
                            waiting.push(c);
                        }
                    }
                    Msg::Exit { from } => {
                        let c = from as usize;
                        // pass the section on first (the paper's deferred replies), then release
                        if !waiting.is_empty() {
                            let mut k = 0;
                            while k + 1 < waiting.len() && !nondet() {
                                k += 1;
                            }
                            let next = waiting.remove(k);
                            holder = Some(next);
                            send_msg(ids[next], Msg::Granted {});
                        } else {
                            holder = None;
                        }
                        send_msg(ids[c], Msg::Released {});
                    }
                    _ => unreachable!("a client request"),
                }
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    lock.join().unwrap();
}
