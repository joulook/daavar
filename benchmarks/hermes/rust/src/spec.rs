//! The hand-written specification of the Hermes-replicated key (coordinator, 2026-11-02, rev 2).
//! A sequential register would over-order Hermes's local reads: a read served by the register thread
//! puts every later write's acknowledgement causally after the read, while Hermes's local read sends
//! no message (the ABD benchmark's `read-no-writeback` is flagged for exactly that). So: one invisible
//! register `reg` linearises the writes (it receives `Write`, applies it, acknowledges, and broadcasts
//! the new value to every client's stub); one invisible stub `s{i}` per client serves `Read`: it has
//! consumed some prefix of the register's value stream (always at least through its own client's last
//! write, which Hermes guarantees: a write completes only after every replica invalidated), chooses with
//! the API's choice how many further values to consume, and replies with the last value it holds.
//! Visible: the clients `c0..c{K-1}` with the interface's messages (the port's `Msg` type). The stub
//! separates the register's messages from its client's by the sender (tagged receives).
use super::Msg; // the port's own message type: the in-process check compares typed values
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Internal {
    /// the stub's client (sent by the register once the clients exist)
    Register { client: ThreadId },
    /// the register's linearised value stream to a stub
    Snap { value: i32 },
    /// the register's acknowledgement of this stub's client's write, carrying the written value
    Acked { value: i32 },
}

pub fn spec(replicas: usize, clients: usize) {
    let _ = replicas;
    let reg = thread::Builder::new()
        .name("reg".to_string())
        .spawn(move || {
            let me = thread::current_id();
            let mut stub_ids: Vec<ThreadId> = Vec::new();
            let mut hs = Vec::new();
            for i in 0..clients {
                let h = thread::Builder::new()
                    .name(format!("s{i}"))
                    .spawn(move || {
                        let mut client: Option<ThreadId> = None;
                        let mut value: i32 = 0;
                        let mut own: Option<i32> = None;
                        let mut consumed: Vec<i32> = Vec::new();
                        loop {
                            match recv_tagged_msg_block::<_, Internal>(move |t, _| t == me) {
                                Internal::Register { client: c } => client = Some(c),
                                Internal::Acked { value: v } => own = Some(v),
                                Internal::Snap { value: s } => {
                                    value = s;
                                    consumed.push(s);
                                }
                            }
                            if let (Some(_), Some(v)) = (client, own) {
                                if consumed.contains(&v) {
                                    break;
                                }
                            }
                        }
                        let c = client.expect("registered");
                        let Msg::Read { .. } = recv_tagged_msg_block::<_, Msg>(move |t, _| t == c) else {
                            unreachable!("the client's read")
                        };
                        while nondet() {
                            match recv_tagged_msg::<_, Internal>(move |t, _| t == me) {
                                Some(Internal::Snap { value: s }) => value = s,
                                _ => break,
                            }
                        }
                        send_msg(c, Msg::ReadReply { value });
                    })
                    .unwrap();
                stub_ids.push(h.thread().id());
                hs.push(h);
            }
            let mut client_ids: Vec<ThreadId> = Vec::new();
            for i in 0..clients {
                let idx = i as i32;
                let stub = stub_ids[i];
                let h = thread::Builder::new()
                    .name(format!("c{i}"))
                    .spawn(move || {
                        send_msg(me, Msg::Write { from: idx, value: idx + 1 });
                        let _ack: Msg = recv_msg_block::<Msg>();
                        send_msg(stub, Msg::Read { from: idx });
                        let _r: Msg = recv_msg_block::<Msg>();
                    })
                    .unwrap();
                client_ids.push(h.thread().id());
                hs.push(h);
            }
            for i in 0..clients {
                send_msg(stub_ids[i], Internal::Register { client: client_ids[i] });
            }
            for _ in 0..clients {
                let Msg::Write { from, value } = recv_msg_block::<Msg>() else { unreachable!("a write") };
                let i = from as usize;
                send_msg(client_ids[i], Msg::WriteAck {});
                send_msg(stub_ids[i], Internal::Acked { value });
                for s in &stub_ids {
                    send_msg(*s, Internal::Snap { value });
                }
            }
            for h in hs {
                h.join().unwrap();
            }
        })
        .unwrap();
    reg.join().unwrap();
}
