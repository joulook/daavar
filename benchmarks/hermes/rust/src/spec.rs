//! The hand-written specification of the Hermes-replicated key (coordinator, 2026-11-02): one
//! linearizable object served by one invisible thread `key` in arrival order. Visible: the clients
//! `c0..c{K-1}` with the interface's messages (Write/WriteAck, Read/ReadReply). The object
//! thread spawns the clients so that it knows their ids (spawn edges are invisible).
use super::Msg; // the port's own message type: the in-process check compares typed values
use traceforge::thread::{self, ThreadId};
use traceforge::*;

pub fn spec(replicas: usize, clients: usize) {
    let _ = replicas;
    let key = thread::Builder::new()
        .name("key".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut hs = Vec::new();
            let mut ids: Vec<ThreadId> = Vec::new();
            for i in 0..clients {
                let idx = i as i32;
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Msg::Write { from: idx, value: idx + 1 });
                        let _r: Msg = recv_msg_block::<Msg>();
                        send_msg(me, Msg::Read { from: idx });
                        let _r: Msg = recv_msg_block::<Msg>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut value: i32 = 0;
            for _ in 0..(2 * clients) {
                let req = recv_msg_block::<Msg>();
                let from = match &req { Msg::Write { from, .. } | Msg::Read { from } => *from, _ => unreachable!("a client request") };
                let to = ids[from as usize];
                match req {
                    Msg::Write { value: v, .. } => {
                        value = v;
                        send_msg(to, Msg::WriteAck {});
                    }
                    Msg::Read { .. } => send_msg(to, Msg::ReadReply { value }),
                    _ => unreachable!("a client request"),
                }
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    key.join().unwrap();
}
