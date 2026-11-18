//! Deliberately wrong servers, used only to check that the specification program is not
//! over-permissive: each must be rejected by the specification's export (some observation
//! uncovered). They share one simple, deterministic base server (task item at the streaming send,
//! one status per report, a cancel answered when the executor's callback returns) and differ from
//! it in one decision each:
//! * 1: on `Working`, a terminal `Status{Completed}` is sent before `Status{Working}`;
//! * 2: a cancel of a completed task is answered `CancelOk` with state `Canceled`;
//! * 3: the stream is closed right after `Status{Working}`, while the task is still working;
//! * 4: the stream is closed at the terminal state without a terminal status (expected to be
//!   ACCEPTED: the text allows "zero or more" status events).

use traceforge::thread::ThreadId;
use traceforge::{recv_msg_block, send_msg};

use crate::env;
use crate::msgs::*;

fn server(client: ThreadId, exec: ThreadId, n: u8) {
    let mut ts: Option<State> = None;
    let mut pending: Option<u32> = None;
    let (mut cb_out, mut shutdown, mut closed) = (0u32, false, false);
    let close = |closed: &mut bool| {
        if !*closed {
            *closed = true;
            send_msg(client, ToClient::StreamClose { req: 1 });
        }
    };
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::SendStream { req } => {
                ts = Some(State::Submitted);
                send_msg(exec, ToExec::Exec { task: t1() });
                send_msg(client, ToClient::TaskItem { req, task: t1(), state: State::Submitted });
            }
            ToServer::Submit { .. } => {}
            ToServer::Working { .. } => {
                if ts.map_or(false, State::terminal) {
                    continue;
                }
                ts = Some(State::Working);
                if n == 1 {
                    send_msg(client, ToClient::Status { req: 1, task: t1(), state: State::Completed, fin: true });
                }
                send_msg(client, ToClient::Status { req: 1, task: t1(), state: State::Working, fin: false });
                if n == 3 {
                    close(&mut closed);
                }
            }
            ToServer::Completed { .. } => {
                if ts.map_or(false, State::terminal) {
                    continue;
                }
                ts = Some(State::Completed);
                if !closed && n != 4 {
                    send_msg(client, ToClient::Status { req: 1, task: t1(), state: State::Completed, fin: true });
                }
                close(&mut closed);
            }
            ToServer::Cancel { req, .. } => match ts {
                Some(State::Completed) => {
                    let m = if n == 2 {
                        ToClient::CancelOk { req, task: t1(), state: State::Canceled }
                    } else {
                        ToClient::CancelErr { req, code: Code::TaskNotCancelable }
                    };
                    send_msg(client, m);
                }
                Some(State::Canceled) => send_msg(client, ToClient::CancelErr { req, code: Code::TaskNotCancelable }),
                _ => {
                    pending = Some(req);
                    cb_out += 1;
                    send_msg(exec, ToExec::CancelCb { task: t1() });
                }
            },
            ToServer::Canceled { .. } => {
                cb_out -= 1;
                if let Some(req) = pending.take() {
                    if ts.map_or(false, State::terminal) {
                        send_msg(client, ToClient::CancelErr { req, code: Code::TaskNotCancelable });
                    } else {
                        ts = Some(State::Canceled);
                        if !closed && n != 4 {
                            send_msg(client, ToClient::Status { req: 1, task: t1(), state: State::Canceled, fin: true });
                        }
                        close(&mut closed);
                        send_msg(client, ToClient::CancelOk { req, task: t1(), state: State::Canceled });
                    }
                }
            }
            ToServer::Get { req, .. } => send_msg(client, ToClient::GetOk { req, task: t1(), state: ts.unwrap() }),
            ToServer::Shutdown => shutdown = true,
        }
        if shutdown && cb_out == 0 {
            send_msg(exec, ToExec::Shutdown);
            return;
        }
    }
}

fn server_n(n: u8) -> fn(ThreadId, ThreadId) {
    match n {
        1 => |c, e| server(c, e, 1),
        2 => |c, e| server(c, e, 2),
        3 => |c, e| server(c, e, 3),
        4 => |c, e| server(c, e, 4),
        _ => panic!("mutant: unknown number {n}"),
    }
}

/// A mutant program: the shared client and executor around mutant `args.mutant`.
pub fn run(args: Args) {
    env::launch(args, server_n(args.mutant));
}
