//! The specification program: the A2A server role for one task, one stream, a cancel racing the
//! task's completion, a read, and optionally a second cancel. Written from the A2A specification
//! text only (a2aproject/A2A 5f931b8a25fe66789f987ec4a55fc16ee9fe41ee: docs/specification.md,
//! specification/a2a.proto, docs/topics/life-of-a-task.md, docs/topics/streaming-and-async.md),
//! never from an SDK. Every behaviour the text leaves open is a `nondet` choice, so the program
//! produces every allowed observation; `spec.md:N` below is docs/specification.md at that pin.
//!
//! The server's traffic with the executor is observed too (every send and receive of the server
//! is). The text says nothing about an executor, so where the server's executor traffic is not
//! fixed by the scenario it is a free choice here: where the Task item is emitted relative to the
//! executor's start, where a cancel callback sits among a step's sends, and whether a cancel of an
//! already terminal task also calls the executor back. A cancel answer may also be held while a
//! callback is outstanding and sent at a later executor report, at the latest when the last
//! callback is answered: the text gives the answer no deadline ("attempts to cancel the specified
//! task and returns its updated state", spec.md:260).
//!
//! Restrictions this program adds to the text, deliberately (they make it narrower than the text):
//! * exactly one status event per state change after creation (the text allows "zero or more");
//! * the stream closes in the step that makes the task terminal, with no final Task item;
//! * `fin` = the state is terminal (the proto has no such field);
//! * no authentication, authorization or system error ever occurs;
//! * a cancel that loses to completion is answered with the not-cancelable error, never with the
//!   completed task;
//! * an executor report that arrives after the task became terminal is received and ignored: no
//!   output and no state change (in particular no error item on the stream);
//! * the executor is started at once on the streaming send; a refused cancel calls nothing back.
//!
//! The server handles one received message per atomic step and never waits inside a step.

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_msg_block, send_msg, Nondet};

use crate::env;
use crate::msgs::*;

const NOT_CANCELABLE: Code = Code::TaskNotCancelable;

/// The stream of request 1.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Strm {
    Unopened,
    Open,
    Closed,
}

/// One send of a step, to the client or to the executor.
enum Out {
    Client(ToClient),
    Exec(ToExec),
}

struct Server {
    client: ThreadId,
    exec: ThreadId,
    /// The task's committed state (`None`: not created). Terminal states are never left
    /// (life-of-a-task.md:79-80, "it cannot restart").
    ts: Option<State>,
    strm: Strm,
    /// The Task item is still owed: the stream "MUST begin with the Task object" (spec.md:187).
    item_owed: bool,
    /// The stream's status events in generation order (for the invariants only).
    evs: Vec<State>,
    /// A cancel accepted while the task is non-terminal and not yet answered.
    pc: Option<u32>,
    /// Cancel callbacks sent and not yet answered by the executor.
    cb_out: u32,
    /// A decided cancel answer not yet sent (held while a callback is outstanding).
    held: Option<ToClient>,
    /// Requests answered so far (each request is answered at most once).
    answered: Vec<u32>,
}

fn task_item() -> ToClient {
    ToClient::TaskItem { req: 1, task: t1(), state: State::Submitted }
}

impl Server {
    fn term(&self) -> bool {
        self.ts.map_or(false, State::terminal)
    }

    /// Sends a step's outputs in order, checking that the stream is open for its items and that
    /// no request is answered twice.
    fn send_all(&mut self, outs: Vec<Out>) {
        for o in outs {
            match o {
                Out::Exec(m) => {
                    if matches!(m, ToExec::CancelCb { .. }) {
                        self.cb_out += 1;
                    }
                    send_msg(self.exec, m);
                }
                Out::Client(m) => {
                    match &m {
                        ToClient::CancelOk { req, .. }
                        | ToClient::CancelErr { req, .. }
                        | ToClient::GetOk { req, .. }
                        | ToClient::Err { req, .. } => {
                            assert!(!self.answered.contains(req), "request {req} answered twice");
                            self.answered.push(*req);
                        }
                        _ => assert!(self.strm == Strm::Open, "stream item after the stream closed: {m:?}"),
                    }
                    let close = matches!(m, ToClient::StreamClose { .. });
                    send_msg(self.client, m);
                    if close {
                        self.strm = Strm::Closed;
                    }
                }
            }
        }
    }

    /// The task reached the terminal state `c`: its status event, then the end of the stream
    /// (spec.md:187, "The stream MUST close when the task reaches a terminal state"), with the
    /// cancel answer `ans`, if any, at any position among them: the text orders the events of
    /// one stream (spec.md:660) but never a stream against another request's answer.
    /// With `may_hold` (a callback is outstanding after this step) the answer may instead be held.
    fn terminal_items(&mut self, ans: Option<ToClient>, c: State, may_hold: bool) -> Vec<Out> {
        assert!(c.terminal());
        let mut items = Vec::new();
        if self.item_owed {
            // A cancel took effect before any executor report: the stream still begins with the
            // Task object.
            self.item_owed = false;
            items.push(Out::Client(task_item()));
        }
        items.push(Out::Client(ToClient::Status { req: 1, task: t1(), state: c, fin: true }));
        items.push(Out::Client(ToClient::StreamClose { req: 1 }));
        if let Some(a) = ans {
            if may_hold && nondet() {
                self.hold(a);
            } else {
                let k = (0..=items.len()).nondet();
                items.insert(k, Out::Client(a));
            }
        }
        self.evs.push(c);
        items
    }

    fn hold(&mut self, a: ToClient) {
        assert!(self.held.is_none(), "one held answer at a time");
        self.held = Some(a);
    }

    /// At an executor report: a held answer is sent now, at any position among the step's sends,
    /// or held further; it must go out once no callback is outstanding (`forced`).
    fn release(&mut self, mut outs: Vec<Out>, forced: bool) -> Vec<Out> {
        if self.held.is_some() && (forced || nondet()) {
            let a = self.held.take().unwrap();
            let k = (0..=outs.len()).nondet();
            outs.insert(k, Out::Client(a));
        }
        outs
    }

    /// Inserts a cancel callback at any position among a step's sends.
    fn with_callback(mut outs: Vec<Out>) -> Vec<Out> {
        let k = (0..=outs.len()).nondet();
        outs.insert(k, Out::Exec(ToExec::CancelCb { task: t1() }));
        outs
    }

    /// The answer to a cancel of a terminal task: not cancelable (spec.md:255, "already
    /// completed, failed, or canceled"; spec.md:477, "Servers MUST return appropriate errors").
    /// On a canceled task the answer may also be the task itself, unchanged: cancel operations
    /// "have the same effect" when repeated (spec.md:473), which constrains the state only.
    fn answer_terminal(&self, req: u32) -> ToClient {
        let s = self.ts.unwrap();
        if s == State::Canceled && nondet() {
            ToClient::CancelOk { req, task: t1(), state: s }
        } else {
            ToClient::CancelErr { req, code: NOT_CANCELABLE }
        }
    }

    /// Invariants after every step.
    fn check(&self) {
        match self.ts {
            None | Some(State::Submitted) => assert!(self.evs.is_empty()),
            Some(State::Working) => assert_eq!(self.evs, vec![State::Working]),
            // A cancel may take effect before the executor's first report (no `Working` event).
            Some(c) => assert!(self.evs == vec![State::Working, c] || self.evs == vec![c]),
        }
        if self.ts.is_some() {
            assert_eq!(self.strm == Strm::Closed, self.term(), "the stream is closed iff terminal");
        }
        if self.ts.map_or(false, |s| s != State::Submitted) {
            assert!(!self.item_owed, "the Task item precedes every status event");
        }
        if self.pc.is_some() {
            assert!(self.cb_out > 0 && !self.term());
        }
        if self.held.is_some() {
            assert!(self.cb_out > 0 && self.term() && self.pc.is_none());
        }
    }

    fn step(&mut self, m: ToServer) {
        let before = self.ts;
        match m {
            ToServer::SendStream { req: 1 } => {
                // A task is created and returned as a stream, which "MUST begin with the Task
                // object" (spec.md:187); processing continues after the operation returns
                // (spec.md:544). The Task item goes out now (before or after the executor is
                // started) or is deferred to the executor's first report.
                assert!(self.ts.is_none(), "one streaming send only");
                self.ts = Some(State::Submitted);
                self.strm = Strm::Open;
                let exec = Out::Exec(ToExec::Exec { task: t1() });
                let outs = match (0..3usize).nondet() {
                    0 => vec![Out::Client(task_item()), exec],
                    1 => vec![exec, Out::Client(task_item())],
                    _ => {
                        self.item_owed = true;
                        vec![exec]
                    }
                };
                self.send_all(outs);
            }
            ToServer::Submit { .. } => {
                // The executor's hand-over: the deferred Task item, if any, goes out now. (After a
                // cancel that took effect before it, the task is terminal and nothing is owed.)
                assert!(self.ts.is_some(), "the hand-over follows the streaming send");
                if self.item_owed {
                    self.item_owed = false;
                    self.send_all(vec![Out::Client(task_item())]);
                }
            }
            ToServer::Working { .. } => {
                assert!(self.ts.is_some(), "the report follows the streaming send");
                if self.term() {
                    // A cancel took effect before the executor reported: terminal states are
                    // absorbing and the stream is closed; no output, no change.
                    self.check();
                    return;
                }
                let mut outs = Vec::new();
                if self.item_owed {
                    // No hand-over came first: the stream still begins with the Task object.
                    self.item_owed = false;
                    outs.push(Out::Client(task_item()));
                }
                self.ts = Some(State::Working);
                self.evs.push(State::Working);
                outs.push(Out::Client(ToClient::Status { req: 1, task: t1(), state: State::Working, fin: false }));
                self.send_all(outs);
            }
            ToServer::Completed { .. } => {
                assert!(!matches!(self.ts, None | Some(State::Submitted)));
                if self.term() {
                    // Terminal states are absorbing; the stream is closed: no output, no change
                    // (a held answer may go out here).
                    let outs = self.release(Vec::new(), false);
                    self.send_all(outs);
                } else if self.pc.is_none() {
                    self.ts = Some(State::Completed);
                    let outs = self.terminal_items(None, State::Completed, false);
                    self.send_all(outs);
                } else if nondet() {
                    // A cancel is pending: "success is not guaranteed" (spec.md:243). Here the
                    // completion takes effect and the pending cancel is refused.
                    let req = self.pc.take().unwrap();
                    self.ts = Some(State::Completed);
                    let outs = self.terminal_items(Some(ToClient::CancelErr { req, code: NOT_CANCELABLE }), State::Completed, true);
                    self.send_all(outs);
                } else {
                    // Here the pending cancel wins: the completion report is not committed and
                    // the executor's `Canceled` (always sent after a callback) commits the cancel.
                }
            }
            ToServer::Cancel { req, .. } => {
                assert!(self.ts.is_some(), "cancel of a task that exists");
                if self.term() {
                    // Nothing to cancel; whether the executor is still called back is not
                    // something the text speaks of.
                    let a = self.answer_terminal(req);
                    let outs = if nondet() {
                        // With a callback the answer may be held until the callback returns.
                        let outs = if nondet() {
                            self.hold(a);
                            Vec::new()
                        } else {
                            vec![Out::Client(a)]
                        };
                        Self::with_callback(outs)
                    } else {
                        vec![Out::Client(a)]
                    };
                    self.send_all(outs);
                } else {
                    // "The server will attempt to cancel the task, but success is not guaranteed
                    // (e.g., [...] cancellation might not be supported at its current stage)"
                    // (spec.md:243). The text names no point at which a cancel takes effect.
                    match (0..3usize).nondet() {
                        0 => {
                            // The cancel takes effect now; the answer is the updated task
                            // (spec.md:260).
                            self.ts = Some(State::Canceled);
                            let outs = self.terminal_items(
                                Some(ToClient::CancelOk { req, task: t1(), state: State::Canceled }),
                                State::Canceled,
                                true,
                            );
                            let outs = Self::with_callback(outs);
                            self.send_all(outs);
                        }
                        1 => {
                            // The cancel is attempted: the outcome is decided by what the
                            // executor reports next.
                            self.pc = Some(req);
                            self.send_all(vec![Out::Exec(ToExec::CancelCb { task: t1() })]);
                        }
                        _ => {
                            // Cancellation not supported at this stage (spec.md:243); the error is
                            // the not-cancelable one (spec.md:255).
                            self.send_all(vec![Out::Client(ToClient::CancelErr { req, code: NOT_CANCELABLE })]);
                        }
                    }
                }
            }
            ToServer::Canceled { .. } => {
                assert!(self.cb_out > 0, "a callback outcome answers a callback");
                self.cb_out -= 1;
                if !self.term() {
                    let req = self.pc.take().expect("an unanswered accepted cancel");
                    self.ts = Some(State::Canceled);
                    let outs = self.terminal_items(
                        Some(ToClient::CancelOk { req, task: t1(), state: State::Canceled }),
                        State::Canceled,
                        false,
                    );
                    self.send_all(outs);
                } else {
                    let forced = self.cb_out == 0;
                    let outs = self.release(Vec::new(), forced);
                    self.send_all(outs);
                }
            }
            ToServer::Get { req, .. } => {
                // The task's current state (spec.md:193).
                let s = self.ts.expect("read of a task that exists");
                self.send_all(vec![Out::Client(ToClient::GetOk { req, task: t1(), state: s })]);
            }
            m => panic!("server: unexpected {m:?}"),
        }
        if let Some(b) = before {
            if b.terminal() {
                assert_eq!(self.ts, Some(b), "a terminal state is never left");
            }
        }
        self.check();
    }
}

/// The specification's server: a receive loop. After the client's `Shutdown` it keeps receiving
/// until every cancel callback has been answered, then tells the executor to end and ends.
fn server(client: ThreadId, exec: ThreadId) {
    let mut s = Server {
        client,
        exec,
        ts: None,
        strm: Strm::Unopened,
        item_owed: false,
        evs: Vec::new(),
        pc: None,
        cb_out: 0,
        held: None,
        answered: Vec::new(),
    };
    let mut shutdown = false;
    loop {
        let m = recv_msg_block::<ToServer>();
        if m == ToServer::Shutdown {
            assert!(!shutdown, "one shutdown");
            shutdown = true;
        } else {
            s.step(m);
        }
        if shutdown && s.cb_out == 0 {
            send_msg(s.exec, ToExec::Shutdown);
            return;
        }
    }
}

/// The specification program: the shared client and executor around the specification's server;
/// in the delegation scenario, two instances of the same server.
pub fn run(args: Args) {
    if args.delegation {
        env::launch2(args, server);
    } else {
        env::launch(args, server);
    }
}
