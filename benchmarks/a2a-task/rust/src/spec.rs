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
//! Two readings of "The stream MUST close when the task reaches a terminal state" (spec.md:187)
//! are both encoded: the terminal state as stored (the stream's tail goes out in the step that
//! commits it) and the terminal state as published on the stream (the tail may be deferred to a
//! later step, the stored state staying unpublished meanwhile). An executor report rejected
//! because the task is already terminal is, while the stream is still open, either ignored or
//! answered on the stream by an error item and the end of the stream: the text neither requires
//! nor forbids that error item, so both are allowed. Status events are optional ("zero or more",
//! spec.md:187): at most one per change of state, except that a repeated working report may be
//! published again (a status event is "sent by the agent to notify the client of a change in a
//! task's status", proto `TaskStatusUpdateEvent`, which does not exclude a repeated one).
//!
//! Restrictions this program adds to the text, deliberately (they make it narrower than the text):
//! * no final Task item before the end of the stream;
//! * `fin` = the state is terminal (the proto has no such field);
//! * no authentication, authorization or system error ever occurs (the error answers are the
//!   not-found answer to a cancel of a task whose Task object has not been returned yet, and the
//!   error item for a rejected executor report);
//! * a cancel that loses to completion is answered with the not-cancelable error, never with the
//!   completed task;
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
    /// The task's committed state (`None`: no streaming send yet). `Submitted` also covers a task
    /// whose creation is not yet visible: until its Task item is sent it may still be reported
    /// not found. Terminal states are never left (life-of-a-task.md:79-80, "it cannot restart").
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
    /// A terminal state committed but not yet published on the stream (the stream is still open).
    unpublished: Option<State>,
    /// Requests answered so far (each request is answered at most once).
    answered: Vec<u32>,
    /// The streaming send was answered with a Message: no task exists (spec.md:185).
    msg_only: bool,
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

    /// The stream's tail for the terminal state `c`: the owed Task item, if any (the stream
    /// "MUST begin with the Task object", spec.md:187), the terminal status event or none, and the
    /// end of the stream.
    fn tail(&mut self, c: State) -> Vec<Out> {
        let mut items = Vec::new();
        if self.item_owed {
            self.item_owed = false;
            items.push(Out::Client(task_item()));
        }
        if nondet() {
            self.evs.push(c);
            items.push(Out::Client(ToClient::Status { req: 1, task: t1(), state: c, fin: true }));
        }
        items.push(Out::Client(ToClient::StreamClose { req: 1 }));
        items
    }

    /// The task reached the terminal state `c`. The stream's tail goes out now (state as stored) or
    /// later (state as published; see [`Self::late`]). The cancel answer `ans`, if any, goes at any
    /// position among the step's stream items: the text orders the events of one stream
    /// (spec.md:660) but never a stream against another request's answer. With `may_hold` (a
    /// callback is outstanding after this step) the answer may instead be held.
    fn terminal_items(&mut self, ans: Option<ToClient>, c: State, may_hold: bool) -> Vec<Out> {
        assert!(c.terminal());
        let mut items = if nondet() {
            self.tail(c)
        } else {
            self.unpublished = Some(c);
            Vec::new()
        };
        if let Some(a) = ans {
            if may_hold && nondet() {
                self.hold(a);
            } else {
                let k = (0..=items.len()).nondet();
                items.insert(k, Out::Client(a));
            }
        }
        items
    }

    /// A later step of a task whose terminal state is not yet published: the tail goes out now or
    /// stays deferred; at an executor report the task rejects (`rejected`), the stream may instead
    /// end with an error item.
    fn late(&mut self, rejected: bool) -> Vec<Out> {
        let c = match self.unpublished {
            Some(c) => c,
            None => return Vec::new(),
        };
        match (0..(if rejected { 3 } else { 2 })).nondet() {
            0 => Vec::new(),
            1 => {
                self.unpublished = None;
                self.tail(c)
            }
            _ => {
                self.unpublished = None;
                let mut items = Vec::new();
                if self.item_owed {
                    self.item_owed = false;
                    items.push(Out::Client(task_item()));
                }
                items.push(Out::Client(ToClient::Err { req: 1, code: ErrCode("rejected report") }));
                items.push(Out::Client(ToClient::StreamClose { req: 1 }));
                items
            }
        }
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
        // The status events are some working statuses followed by at most the terminal state.
        let n = self.evs.len();
        let body = if n > 0 && self.evs[n - 1] != State::Working { &self.evs[..n - 1] } else { &self.evs[..] };
        assert!(body.iter().all(|e| *e == State::Working), "status events {:?}", self.evs);
        match self.ts {
            None | Some(State::Submitted) => assert!(self.evs.is_empty(), "status events {:?}", self.evs),
            Some(State::Working) => assert!(body.len() == n, "status events {:?}", self.evs),
            Some(c) => assert!(body.len() == n || self.evs[n - 1] == c, "status events {:?}", self.evs),
        }
        if self.ts.is_some() && !self.msg_only {
            assert_eq!(
                self.strm == Strm::Closed,
                self.term() && self.unpublished.is_none(),
                "the stream is closed iff the terminal state is published"
            );
        }
        if self.unpublished.is_some() {
            assert!(self.term());
        }
        if self.ts == Some(State::Working) || self.strm == Strm::Closed {
            assert!(!self.item_owed, "the Task item precedes every other stream item");
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
                if self.term() {
                    // Rejected (the task is terminal): the deferred tail, an error item, or nothing;
                    // a held cancel answer may go out at any executor report.
                    let outs = self.late(true);
                    let outs = self.release(outs, false);
                    self.send_all(outs);
                } else if self.item_owed {
                    self.item_owed = false;
                    self.send_all(vec![Out::Client(task_item())]);
                }
            }
            ToServer::Working { .. } => {
                assert!(self.ts.is_some(), "the report follows the streaming send");
                if self.ts == Some(State::Working) {
                    // A second working report changes no state. The stream may carry "zero or more"
                    // status events (spec.md:187), so a repeated working status is allowed, not
                    // required.
                    if nondet() {
                        self.evs.push(State::Working);
                        self.send_all(vec![Out::Client(ToClient::Status { req: 1, task: t1(), state: State::Working, fin: false })]);
                    }
                    self.check();
                    return;
                }
                if self.term() {
                    // A cancel took effect before the executor reported: terminal states are
                    // absorbing; the report is rejected (see `late`); a held answer may go out.
                    let outs = self.late(true);
                    let outs = self.release(outs, false);
                    self.send_all(outs);
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
                if nondet() {
                    self.evs.push(State::Working);
                    outs.push(Out::Client(ToClient::Status { req: 1, task: t1(), state: State::Working, fin: false }));
                }
                self.send_all(outs);
            }
            ToServer::Reply { .. } if !self.term() && self.ts == Some(State::Submitted) && self.item_owed
                && self.pc.is_none() && self.cb_out == 0 =>
            {
                // The agent answered with a Message: "the stream MUST contain exactly one Message
                // object and then close immediately" (spec.md:185); nothing is prepended and no task
                // exists afterwards.
                self.item_owed = false;
                self.msg_only = true;
                self.send_all(vec![
                    Out::Client(ToClient::MessageItem { req: 1 }),
                    Out::Client(ToClient::StreamClose { req: 1 }),
                ]);
            }
            ToServer::Completed { .. } | ToServer::Reply { .. } => {
                // A `Reply` here comes after the Task object was returned (or with a cancel in
                // progress): the stream is a task's stream, so the agent's answer is taken as the
                // task's completion (restriction: no other handling is modelled).
                assert!(self.ts.is_some(), "the report follows the streaming send");
                if self.term() {
                    // Terminal states are absorbing: the report is rejected (see `late`); a held
                    // answer may go out here.
                    let outs = self.late(true);
                    let outs = self.release(outs, false);
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
            ToServer::Cancel { req, .. } if self.msg_only => {
                // No task exists: "The task ID does not exist or is not accessible" (spec.md:256).
                self.send_all(vec![Out::Client(ToClient::Err { req, code: ErrCode("TaskNotFound") })]);
            }
            ToServer::Get { req, .. } if self.msg_only => {
                self.send_all(vec![Out::Client(ToClient::Err { req, code: ErrCode("TaskNotFound") })]);
            }
            ToServer::Cancel { req, .. } => {
                assert!(self.ts.is_some(), "cancel of a task that exists");
                if self.term() {
                    // Nothing to cancel; whether the executor is still called back is not
                    // something the text speaks of.
                    // A deferred tail may go out in this step too.
                    let mut outs = self.late(false);
                    let a = self.answer_terminal(req);
                    let callback = nondet();
                    if callback && nondet() {
                        // With a callback the answer may be held until the callback returns.
                        self.hold(a);
                    } else {
                        let k = (0..=outs.len()).nondet();
                        outs.insert(k, Out::Client(a));
                    }
                    let outs = if callback { Self::with_callback(outs) } else { outs };
                    self.send_all(outs);
                } else {
                    // "The server will attempt to cancel the task, but success is not guaranteed
                    // (e.g., [...] cancellation might not be supported at its current stage)"
                    // (spec.md:243). The text names no point at which a cancel takes effect.
                    // While the Task object has not been returned the task id is not known to exist:
                    // "TaskNotFoundError: The task ID does not exist or is not accessible"
                    // (spec.md:256); the agent "MAY create a new Task" (spec.md:157) and nothing fixes
                    // the creation earlier than the Task object, so the task may not exist yet: the
                    // cancel may then be answered not-found, with no effect on the task.
                    let n = if self.item_owed && self.ts == Some(State::Submitted) { 4 } else { 3 };
                    match (0..n).nondet() {
                        3 => {
                            self.send_all(vec![Out::Client(ToClient::Err { req, code: ErrCode("TaskNotFound") })]);
                        }
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
                    let outs = self.late(false);
                    let outs = self.release(outs, forced);
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
        unpublished: None,
        answered: Vec::new(),
        msg_only: false,
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
