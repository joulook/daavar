//! The specification program: an MCP server (revision 2026-07-28, stdio transport), written from
//! the specification text only (modelcontextprotocol/modelcontextprotocol at
//! d4b317231161ee96d3eefb2448bda4230c76f4c6; `IO` = docs/specification/2026-07-28/basic/transports/
//! stdio.mdx, `CN` = basic/patterns/cancellation.mdx, `PG` = basic/patterns/progress.mdx, `B` =
//! basic/index.mdx), never from an SDK. Every behaviour the text leaves open is a `nondet` choice;
//! the few restrictions narrower than the text are marked "restriction".
//!
//! Sending is the write to the client (stdout). Messages produced by a step go into a per-request
//! outbound queue (first in, first out); after every step a nondeterministic number of queued items
//! is written, interleaving the two requests' queues in any order; a received call is passed to
//! its handler (`Work`) at any point of that flush or of a later one. The rules:
//! * nothing for a request is written after the server has received its cancellation: its queue is
//!   emptied at that receive, and later handler output for it is dropped (IO:81-82 "MUST NOT send
//!   any further messages for it"; only messages already written are excused, CN:86-89);
//! * progress values are relayed unchanged (restriction), each may be dropped (PG:64-66), and a
//!   token's values increase (PG:53-54); progress queued before the response is written before it
//!   (PG:90) by the queue order;
//! * a request that is not cancelled gets its result response (B:27-28 with JSON-RPC 2.0 §5); no
//!   error response is produced (restriction: the tool succeeds);
//! * a cancel of an active request (or of one whose response is not yet written) owes the
//!   handler a `Stop`, written at any later flush or never (IO:81-82 "SHOULD stop work … as soon as
//!   practical"); a cancel of a request whose response was written is ignored towards the client
//!   (CN:77-80, CN:113-117; B:159-160: no response to a notification) and also owes a `Stop`;
//! * on `Shutdown` the server tells both handlers to end, writes nothing else, and ends; queued
//!   items are discarded (harness restriction).
//!
//! Flush rule: the server may leave items queued at the end of a step only if a further message
//! to it is certain (otherwise a client waiting for a queued item would block spuriously, since a
//! queue flush is not an event the server can wait for). The client decides nondeterministically
//! whether to send a second call and whether and when to cancel, so neither is certain. Certain:
//! the first `Call` not yet received; handler1's first `Prog` once its `Work` is sent (sent in every
//! branch); handler2's `Done` once its `Work` is sent; and a further client message once every
//! request received so far has its response written or its cancel received (the client then sends
//! its second call, if it sent one, or `Shutdown`). Restriction: a queued item is never kept across a
//! receive that was not certain.

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_msg_block, send_msg, Nondet};

use crate::msgs::*;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum St {
    Unknown,
    Active,
    Completed,
    Cancelled,
}

/// A choice inside a flush.
#[derive(Clone, Copy)]
enum Act {
    Halt,
    Write(usize),
    Stop(usize),
    Work(usize),
}

struct Server {
    client: ThreadId,
    h: [ThreadId; 2],
    mutant: u8,
    /// The reading of the cancellation rule (0 strict, 1 hand-over, 2 completed exempt, 3 both).
    reading: u8,
    st: [St; 2],
    last: [u32; 2],
    out: [Vec<ToClient>; 2],
    /// A `Stop` owed to the handler (optional: it may never be written).
    stop: [bool; 2],
    /// A received call not yet passed to its handler (the dispatch may come at any later flush point).
    work: [Option<String>; 2],
    /// Mutant 2 only: a progress value 1 held back until value 2 arrives.
    held: [bool; 2],
    // What the flush rule needs to know.
    prog1_seen: bool,
    done2_seen: bool,
    /// The cancellation of the request has been received (under any reading).
    cancel_seen: [bool; 2],
    wres: [bool; 2],
    /// The cancellation of the request has been received (for the written-sequence checks).
    cancel_rx: [bool; 2],
    /// The last progress value written for the request's token (0 = none).
    wlast: [u32; 2],
}

impl Server {
    fn i(r: u32) -> usize {
        (r - 1) as usize
    }

    fn certain(&self) -> bool {
        self.st[0] == St::Unknown
            || (!self.prog1_seen && self.st[0] != St::Unknown && self.work[0].is_none())
            || (!self.done2_seen && self.st[1] != St::Unknown && self.work[1].is_none())
            || (0..2).all(|i| self.st[i] == St::Unknown || self.wres[i] || self.cancel_seen[i])
    }

    fn write(&mut self, i: usize) {
        let m = self.out[i].remove(0);
        if self.mutant == 0 {
            // The written-sequence invariants (the mutants break some of them on purpose).
            assert!(!self.cancel_rx[i], "spec: nothing is written for a request after its cancel was received");
            assert!(!self.wres[i], "spec: nothing is written for a request after its response");
            assert!(self.st[i] != St::Unknown, "spec: nothing is written for a request before its call");
            match &m {
                ToClient::Progress { value, .. } => {
                    assert!(*value > self.wlast[i], "spec: written progress values increase per token");
                    self.wlast[i] = *value;
                }
                ToClient::Result { .. } => {}
                ToClient::Error { .. } => panic!("spec: no error response is produced"),
            }
        }
        if let ToClient::Result { .. } = m {
            self.wres[i] = true;
        }
        send_msg(self.client, m);
    }

    /// Write a nondeterministic sequence of queued items, owed `Stop`s and pending dispatches.
    fn flush(&mut self) {
        loop {
            let mut acts = Vec::new();
            let idle = self.out.iter().all(|q| q.is_empty()) && self.work.iter().all(|w| w.is_none());
            if idle || self.certain() {
                acts.push(Act::Halt);
            }
            for i in 0..2 {
                if self.work[i].is_some() {
                    acts.push(Act::Work(i));
                }
                if !self.out[i].is_empty() {
                    acts.push(Act::Write(i));
                }
                if self.stop[i] {
                    acts.push(Act::Stop(i));
                }
            }
            let a = if acts.len() == 1 { acts[0] } else { acts[(0..acts.len()).nondet()] };
            match a {
                Act::Halt => return,
                Act::Write(i) => self.write(i),
                Act::Stop(i) => {
                    self.stop[i] = false;
                    send_msg(self.h[i], ToHandler::Stop { req: i as u32 + 1 });
                }
                Act::Work(i) => {
                    let token = self.work[i].take().unwrap();
                    send_msg(self.h[i], ToHandler::Work { req: i as u32 + 1, token });
                }
            }
        }
    }

    /// Handle one message; `false` when the server ends.
    fn step(&mut self, m: ToServer) -> bool {
        match m {
            ToServer::Call { req, token } => {
                let i = Self::i(req);
                assert_eq!(self.st[i], St::Unknown, "spec: a request id is used once");
                self.st[i] = St::Active;
                // The call is passed to its handler at some flush point: in this step, before or after
                // queued writes, or later (the text does not say when a server starts the work).
                self.work[i] = Some(token);
            }
            ToServer::Prog { req, token, value } => {
                let i = Self::i(req);
                if req == 1 && value == 1 {
                    self.prog1_seen = true;
                }
                match self.st[i] {
                    St::Active => {
                        assert!(value > self.last[i], "spec: handler progress increases");
                        if self.mutant == 2 {
                            // Mutant 2: value 1 is held and written right after value 2 (decreasing).
                            if value == 1 {
                                self.held[i] = true;
                            } else {
                                self.out[i].push(ToClient::Progress { token: token.clone(), value });
                                if self.held[i] {
                                    self.held[i] = false;
                                    self.out[i].push(ToClient::Progress { token, value: 1 });
                                }
                            }
                            self.last[i] = value;
                        } else if nondet() {
                            // Relayed (PG:33-36); otherwise dropped (PG:64-66). Relaying an item that
                            // the cancel later cuts off unwritten gives the same words as dropping it;
                            // such duplicate executions are counted, not removed (moving the drop into the
                            // flush, the obvious way to remove them, may change which flushes may halt).
                            self.out[i].push(ToClient::Progress { token, value });
                            self.last[i] = value;
                        }
                    }
                    St::Cancelled => {} // IO:81-82: nothing further for a cancelled request.
                    s => panic!("spec: Prog for a request in state {s:?}"),
                }
            }
            ToServer::Done { req } => {
                let i = Self::i(req);
                if req == 2 {
                    self.done2_seen = true;
                }
                if self.mutant == 2 {
                    self.held[i] = false;
                }
                match self.st[i] {
                    St::Active => {
                        self.st[i] = St::Completed;
                        let dropped = self.mutant == 3 && req == 2 && self.st[0] == St::Cancelled;
                        if !dropped {
                            self.out[i].push(ToClient::Result { req });
                        }
                    }
                    St::Cancelled => {
                        if self.mutant == 1 {
                            self.out[i].push(ToClient::Result { req }); // Mutant 1.
                        }
                    }
                    s => panic!("spec: Done for a request in state {s:?}"),
                }
            }
            ToServer::Cancelled { req } => {
                let i = Self::i(req);
                self.cancel_seen[i] = true;
                if matches!(self.st[i], St::Active | St::Completed) {
                    // Readings of "MUST NOT send any further messages" (IO:81-82):
                    // * 0, strict: sending is the write; a queued item is not yet sent, and a request whose
                    //   handler finished before the cancel is still a cancelled request;
                    // * 1: sending is the hand-over to the writer, so items queued before this receive may
                    //   still be written (later handler output for the request is dropped);
                    // * 2: a request whose handler finished before this receive is exempt (CN:77-79 "MAY
                    //   ignore … Processing has already completed"; CN:86-87 the cancel may arrive "after
                    //   a response has already been sent");
                    // * 3: both relaxations.
                    let exempt = self.reading & 2 != 0 && self.st[i] == St::Completed;
                    let keep = self.reading & 1 != 0 || exempt;
                    if keep {
                        if self.st[i] == St::Active {
                            self.st[i] = St::Cancelled;
                        }
                    } else {
                        self.cancel_rx[i] = true;
                        // A queued, unwritten response is not sent: the request counts as cancelled.
                        if self.st[i] == St::Active || !self.out[i].is_empty() {
                            self.st[i] = St::Cancelled;
                        }
                        self.out[i].clear(); // nothing for it written after this receive.
                    }
                    // IO:81 "SHOULD stop work": a `Stop` is owed; the flush writes it at some later
                    // point or never (both readings), so "owed" is not itself a choice.
                    self.stop[i] = true;
                }
                // Unknown or already cancelled: ignored (CN:77-80, CN:113-117).
            }
            ToServer::Shutdown => {
                // Harness restriction: nothing further is written; queued items are discarded.
                send_msg(self.h[0], ToHandler::Shutdown);
                send_msg(self.h[1], ToHandler::Shutdown);
                return false;
            }
        }
        self.flush();
        true
    }
}

/// The specification server (and, with `args.mutant > 0`, the mutants).
pub fn server(client: ThreadId, h1: ThreadId, h2: ThreadId, args: Args) {
    let mut s = Server {
        client,
        h: [h1, h2],
        mutant: args.mutant,
        reading: args.reading,
        st: [St::Unknown; 2],
        last: [0; 2],
        out: [Vec::new(), Vec::new()],
        stop: [false; 2],
        held: [false; 2],
        work: [None, None],
        prog1_seen: false,
        done2_seen: false,
        cancel_seen: [false; 2],
        wres: [false; 2],
        cancel_rx: [false; 2],
        wlast: [0; 2],
    };
    while s.step(recv_msg_block::<ToServer>()) {}
}
