//! The hand-written specification of the Hermes-replicated key (coordinator, 2026-11-02): one
//! linearizable object served by one invisible thread `key` in arrival order. Visible: the clients
//! `c0..c{K-1}` with the interface's messages (Write/WriteAck, Read/ReadReply). The object
//! thread spawns the clients so that it knows their ids (spawn edges are invisible).
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Req {
    Write { from: i32, value: i32 },
    Read { from: i32 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Rep {
    WriteAck {},
    ReadReply { value: i32 },
}

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
                        send_msg(me, Req::Write { from: idx, value: idx + 1 });
                        let _r: Rep = recv_msg_block::<Rep>();
                        send_msg(me, Req::Read { from: idx });
                        let _r: Rep = recv_msg_block::<Rep>();
                    })
                    .unwrap();
                ids.push(h.thread().id());
                hs.push(h);
            }
            let mut value: i32 = 0;
            for _ in 0..(2 * clients) {
                let req = recv_msg_block::<Req>();
                let from = match &req { Req::Write { from, .. } | Req::Read { from } => *from };
                let to = ids[from as usize];
                match req {
                    Req::Write { value: v, .. } => {
                        value = v;
                        send_msg(to, Rep::WriteAck {});
                    }
                    Req::Read { .. } => send_msg(to, Rep::ReadReply { value }),
                }
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    key.join().unwrap();
}
