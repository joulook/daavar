//! The implementation program of the FIXED a2a-rs SDK: a copy of `imp.rs` changed only where the
//! two fixes change `a2a-server/src/handler.rs` (both applied): the task-first-stream patch and
//! the message-stream-close patch, each built on a2a-rs `e9b00bc`. Each change is marked
//! `FIX task-first` or `FIX message-close` with the line of the patched `handler.rs` (`fork tf:N`
//! for the task-first patch, `fork mc:N` for the message-close patch). Every other line, and every
//! `handler.rs:N` citation, is `imp.rs`'s, on the unfixed source below.
//!
//! The unfixed program: the server core of a2a-rs (a2aproject/a2a-rs at
//! 700a6a28eb001f6fc32bc9446078ff87bdd18c6c: `a2a-server/src/handler.rs`,
//! `a2a-server/src/task_store/store.rs`, `a2a-server/src/task_store/inmemory.rs`) rewritten as one
//! sequential Must process, run in the shared environment (`env::launch`). Source lines are cited
//! below as `handler.rs:N`, `inmemory.rs:N`.
//!
//! Configuration rewritten: `InMemoryTaskStore::new()` (no terminal-task limit), no push-config
//! store (every push is a no-op, `handler.rs:306-308`), no authorizer, no required extensions, no
//! input-mode restriction, streaming not refused.
//!
//! Concurrency abstracted, nothing else changed: in a2a-rs the execution is driven by a spawned
//! task (`handler.rs:1029`) while each request is its own future; here every incoming message is
//! one atomic step of this process. Waits on the store, registry and snapshot locks happen inside
//! a step. A step ends where a2a-rs waits for the executor: the execute stream's next event
//! (`handler.rs:520`) arrives as `Submit`, `Working`, `Completed` or `Reply`; the cancel stream's
//! next event (`handler.rs:1229`) arrives as `Canceled`, and the cancel that asked for it stays
//! pending until then. `Reply` is taken as the executor answering with a Message and returning:
//! its execute stream yields that one item and ends, so the drive reads the item and then the end
//! of the stream in that step. (An executor that keeps working never ends its stream.)
//!
//! Order of outputs inside one step: in a2a-rs the cancel's answer and the stream travel on
//! separate connections, and nothing orders them: the cancel publishes its item
//! (`handler.rs:1247`), the stream forwards it and ends on its own polls
//! (`handler.rs:466-484`), and the cancel returns its answer on its own future
//! (`handler.rs:1297`). The step that answers a cancel therefore sends the answer at a
//! nondeterministic position among that step's stream items (before, between or after them); the
//! stream items keep their own order.

use traceforge::thread::ThreadId;
use traceforge::{recv_msg_block, send_msg, Nondet};

use crate::env;
use crate::msgs::*;

/// A stored task state. `Failed` exists because a2a-rs writes it on an execution error
/// (`handler.rs:421-422`); in this program every such write is refused by the store's transition
/// guard, so it never reaches a message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum St {
    Submitted,
    Working,
    Completed,
    Canceled,
    Failed,
}

impl St {
    /// `TaskState::is_terminal` (a2a `types.rs:139-144`).
    fn terminal(self) -> bool {
        matches!(self, St::Completed | St::Canceled | St::Failed)
    }

    fn observed(self) -> State {
        match self {
            St::Submitted => State::Submitted,
            St::Working => State::Working,
            St::Completed => State::Completed,
            St::Canceled => State::Canceled,
            St::Failed => panic!("a FAILED task state is never stored in this program"),
        }
    }
}

/// An error of the store or the handler, by its a2a-rs name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Error {
    /// `INVALID_REQUEST`, from the transition guard (`inmemory.rs:102-107`).
    InvalidRequest,
    /// `TASK_NOT_FOUND` (`inmemory.rs:94-97`, `handler.rs:876-881`).
    TaskNotFound,
    /// `TASK_NOT_CANCELABLE` (`inmemory.rs:128-130`, `handler.rs:1183-1184`).
    TaskNotCancelable,
    /// `INVALID_AGENT_RESPONSE`, an executor stream with a Message and task events
    /// (`handler.rs:527-530`).
    InvalidAgentResponse,
}

impl Error {
    fn name(self) -> &'static str {
        match self {
            Error::InvalidRequest => "InvalidRequest",
            Error::TaskNotFound => "TaskNotFound",
            Error::TaskNotCancelable => "TaskNotCancelable",
            Error::InvalidAgentResponse => "InvalidAgentResponse",
        }
    }
}

/// An item of the execution's broadcast (`ExecutionEvent.result`, `handler.rs:27-30`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Item {
    /// `StreamResponse::Task` with the task's state.
    Task(St),
    /// `StreamResponse::StatusUpdate` with the new state.
    Status(St),
    /// `StreamResponse::Message`.
    Message,
    /// An `Err` item.
    Error(Error),
}

/// A cancel waiting for the executor's cancel stream (`handler.rs:1225-1229`).
#[derive(Clone, Copy, Debug)]
struct PendingCancel {
    req: u32,
    /// The execution `cancel_task` found registered (`handler.rs:1212`); it keeps it after the
    /// execution leaves the registry.
    execution: bool,
    /// `current_task` of `cancel_task` (`handler.rs:1226`).
    current: Option<St>,
}

struct Server {
    client: ThreadId,
    exec: ThreadId,
    /// The in-memory store's entry for `t1` (`inmemory.rs:19-23`); `None` = absent.
    store: Option<St>,
    /// The execution's `snapshot_task` state (`handler.rs:33-36`); `None` = no execution started.
    snapshot: Option<St>,
    /// The execution is in the registry (`handler.rs:103`).
    registered: bool,
    /// The streaming send's subscriber is alive: its stream has not ended (`handler.rs:466-487`).
    subscribed: bool,
    /// `drive_execution` is still reading the execute stream (`handler.rs:520`).
    driving: bool,
    /// `current_task` of `drive_execution` (`handler.rs:511-514`).
    drive_current: Option<St>,
    /// `answered` and `tracked` of `drive_execution` (`handler.rs:517-518`): a Message was read, a
    /// task event was read.
    answered: bool,
    tracked: bool,
    pending: Option<PendingCancel>,
    /// While the cancel's answer is being placed: the step's stream items, held back.
    held: Option<Vec<ToClient>>,
    /// FIX task-first (fork tf:1067, tf:1142-1143, tf:522): the task `start_execution` returns
    /// (`prepare_task_for_execution`'s task, taken before the executor runs) and
    /// `open_with_task` holds until the response stream's first item; `None` once that item went
    /// by, or before any streaming send.
    baseline: Option<St>,
}

impl Server {
    fn to_client(&mut self, m: ToClient) {
        match &mut self.held {
            Some(v) => v.push(m),
            None => send_msg(self.client, m),
        }
    }

    /// Sends the step's held stream items with the cancel's answer `answer` at a nondeterministic
    /// position among them (no ordering exists between the two connections).
    fn answer_among_stream(&mut self, answer: ToClient) {
        let items = self.held.take().expect("stream items held");
        let k = if items.is_empty() { 0 } else { (0..items.len() + 1).nondet() };
        let mut answer = Some(answer);
        for (i, m) in items.into_iter().enumerate() {
            if i == k {
                send_msg(self.client, answer.take().unwrap());
            }
            send_msg(self.client, m);
        }
        if let Some(a) = answer {
            send_msg(self.client, a);
        }
    }

    fn to_exec(&self, m: ToExec) {
        send_msg(self.exec, m);
    }

    /// `InMemoryTaskStore::create` (`inmemory.rs:76-90`).
    fn store_create(&mut self, s: St) -> Result<(), Error> {
        if self.store.is_some() {
            // `sanitized_internal_error("task already exists")`: not reachable here.
            panic!("server: task t1 created twice");
        }
        self.store = Some(s);
        Ok(())
    }

    /// `InMemoryTaskStore::update` with its transition guard (`inmemory.rs:92-117`).
    fn store_update(&mut self, s: St) -> Result<(), Error> {
        let Some(stored) = self.store else {
            return Err(Error::TaskNotFound);
        };
        if stored.terminal() && stored != s {
            return Err(Error::InvalidRequest);
        }
        self.store = Some(s);
        Ok(())
    }

    /// `InMemoryTaskStore::begin_cancel` (`inmemory.rs:119-137`).
    fn store_begin_cancel(&mut self) -> Result<St, Error> {
        let Some(stored) = self.store else {
            return Err(Error::TaskNotFound);
        };
        if stored.terminal() {
            return Err(Error::TaskNotCancelable);
        }
        self.store = Some(St::Canceled);
        Ok(St::Canceled)
    }

    /// `load_task` (`handler.rs:876-881`).
    fn load_task(&self) -> Result<St, Error> {
        self.store.ok_or(Error::TaskNotFound)
    }

    /// `save_task` (`handler.rs:321-337`): update, or create when the task is not found.
    fn save_task(&mut self, s: St) -> Result<St, Error> {
        match self.store_update(s) {
            Ok(()) => Ok(s),
            Err(Error::TaskNotFound) => {
                self.store_create(s)?;
                Ok(s)
            }
            Err(e) => Err(e),
        }
    }

    /// `apply_event_to_task` (`handler.rs:357-405`) for the two event kinds used here: a Task item
    /// saves the item's task; a status update saves `current` (or the stored task) with the new
    /// status.
    fn apply_event_to_task(
        &mut self,
        current: Option<St>,
        item: Item,
    ) -> Result<Option<St>, Error> {
        match item {
            Item::Task(s) => self.save_task(s).map(Some),
            Item::Status(s) => {
                if current.is_none() {
                    self.load_task()?;
                }
                self.save_task(s).map(Some)
            }
            // `handler.rs:403`: a Message leaves the store alone.
            Item::Message => Ok(current),
            Item::Error(_) => unreachable!("an executor event is never an error here"),
        }
    }

    /// `is_terminal_event` (`handler.rs:256-262`).
    fn terminal_event(item: Item) -> bool {
        match item {
            Item::Task(s) | Item::Status(s) => s.terminal(),
            Item::Message | Item::Error(_) => false,
        }
    }

    /// `ActiveExecution::publish` (`handler.rs:65-90`) followed by what the subscriber's stream
    /// (`subscription_stream`, `handler.rs:453-503`) does with the item: forward it, and end after
    /// a terminal item or an error (`handler.rs:483-484`), which ends the response
    /// (`StreamClose`). Fixed: the response stream's first item, if a status update, is preceded
    /// by the baseline Task (`open_with_task`, fork tf:513-537); a Message also ends the stream
    /// (fork mc:484-487).
    fn publish(&mut self, item: Item, snapshot: Option<St>) {
        if let Some(s) = snapshot {
            self.snapshot = Some(s);
        }
        if !self.subscribed {
            // `receiver_count() == 0` (`handler.rs:81-83`).
            return;
        }
        // FIX task-first (fork tf:518-537, `open_with_task`): on the stream's first item the held
        // task is taken (`task.take()`, tf:529) and emitted first only if that item is a status
        // (or artifact, not modelled) update (tf:525-528); a first Task, Message or error passes
        // untouched, and later items are never touched.
        if let Some(b) = self.baseline.take() {
            if matches!(item, Item::Status(_)) {
                self.to_client(ToClient::TaskItem { req: 1, task: t1(), state: b.observed() });
            }
        }
        let m = match item {
            Item::Task(s) => ToClient::TaskItem { req: 1, task: t1(), state: s.observed() },
            Item::Status(s) => {
                ToClient::Status { req: 1, task: t1(), state: s.observed(), fin: s.terminal() }
            }
            Item::Message => ToClient::MessageItem { req: 1 },
            Item::Error(e) => ToClient::Err { req: 1, code: ErrCode(e.name()) },
        };
        self.to_client(m);
        // FIX message-close (fork mc:484-487): `Ok(StreamResponse::Message(_)) | Err(_) =>
        // state.done = true`: a delivered Message ends the stream, as an error does.
        let done = match item {
            Item::Error(_) | Item::Message => true,
            _ => Self::terminal_event(item),
        };
        if done {
            self.subscribed = false;
            self.to_client(ToClient::StreamClose { req: 1 });
        }
    }

    /// `ExecutionManager::finish` (`handler.rs:177-185`): one execution exists, so the registered
    /// one is always the one finishing.
    fn finish(&mut self) {
        self.registered = false;
    }

    /// `fail_task` (`handler.rs:414-439`): FAILED unless the task is already terminal; a refused
    /// save yields `None`.
    fn fail_task(&mut self, task: Option<St>) -> Option<St> {
        task.filter(|s| !s.terminal())?;
        self.save_task(St::Failed).ok()
    }

    /// `send_streaming_message` → `start_execution` (`handler.rs:1096-1117`, `991-1034`) for a
    /// message without a task id: a new task, SUBMITTED in the store, a new execution registered
    /// with that snapshot, the stream subscribed before the drive starts, the executor asked to
    /// execute. Nothing is sent to the client: the stream starts with no snapshot
    /// (`subscription_stream(receiver, None, 0)`, `handler.rs:1033`). Fixed: the stream is wrapped
    /// in `open_with_task` with the task as prepared (fork tf:1067, tf:1142-1143).
    fn send_stream(&mut self, req: u32) {
        assert_eq!(req, 1, "server: the streaming send is request 1");
        if self.store.is_some() {
            // a second streaming send would mint a second task id (`handler.rs:890-893`).
            panic!("server: one streaming send per run");
        }
        // `prepare_task_for_execution` (`handler.rs:883-967`).
        self.store_create(St::Submitted).expect("new task");
        // FIX task-first (fork tf:1032, tf:1067, tf:1142-1143): `start_execution` returns the
        // prepared task (here the new SUBMITTED task: this program has no continuation turn), and
        // `send_streaming_message` wraps the stream in `open_with_task(stream, initial_task)`.
        self.baseline = Some(St::Submitted);
        // `ExecutionManager::start` (`handler.rs:116-133`).
        assert!(!self.registered, "server: an execution is already registered");
        self.registered = true;
        self.snapshot = Some(St::Submitted);
        // `active.subscribe()` (`handler.rs:1002`).
        self.subscribed = true;
        // the spawned `drive_execution` (`handler.rs:1029-1031`, `511-515`).
        self.driving = true;
        self.drive_current = self.snapshot;
        self.answered = false;
        self.tracked = false;
        self.to_exec(ToExec::Exec { task: t1() });
    }

    /// One iteration of `drive_execution`'s loop (`handler.rs:520-567`) on the executor event
    /// `item`, and its epilogue when the loop ends (`handler.rs:569-574`).
    fn drive(&mut self, item: Item) {
        if !self.driving {
            // the execute stream was dropped when the loop ended: the event is never read.
            return;
        }
        // `handler.rs:521-523`.
        if self.snapshot.is_some_and(St::terminal) {
            self.drive_end(None);
            return;
        }
        // `handler.rs:527-533`: after a Message any further event, and a Message after a task
        // event, is an invalid agent response.
        let message = matches!(item, Item::Message);
        if self.answered || (message && self.tracked) {
            self.drive_end(Some(Error::InvalidAgentResponse));
            return;
        }
        self.answered = message;
        self.tracked |= !message;
        // `handler.rs:535-560` (push is a no-op).
        match self.apply_event_to_task(self.drive_current, item) {
            Ok(updated) => {
                self.drive_current = updated;
                self.publish(item, updated);
                // A Message is not terminal (`handler.rs:256-262`): the loop reads on and finds
                // the end of the executor's stream (`handler.rs:520`). (Fixed: `publish` already
                // ended the subscriber's stream at the Message, so `drive_end` closes nothing.)
                if Self::terminal_event(item) || message {
                    self.drive_end(None);
                }
            }
            Err(e) => self.drive_end(Some(e)),
        }
    }

    /// `handler.rs:569-574`: on a failure, `fail_task` and the error published; then `finish`.
    /// When the drive returns, the execution's last owners are gone unless a cancel still holds it
    /// (`handler.rs:1212`): the broadcast sender is dropped, the subscriber's `recv` sees `Closed`
    /// and its stream ends (`handler.rs:498`). A stream that a terminal or error item already
    /// ended is not open any more; a held one ends with the cancel's terminal item.
    fn drive_end(&mut self, failure: Option<Error>) {
        if let Some(e) = failure {
            let failed = self.fail_task(self.drive_current);
            self.publish(Item::Error(e), failed);
        }
        self.finish();
        self.driving = false;
        if self.subscribed && !self.pending.is_some_and(|p| p.execution) {
            self.subscribed = false;
            self.to_client(ToClient::StreamClose { req: 1 });
        }
    }

    /// `cancel_task` up to its wait on the executor's cancel stream (`handler.rs:1169-1229`).
    fn cancel(&mut self, req: u32) {
        let task = match self.load_task() {
            Ok(s) => s,
            Err(e) => return self.to_client(ToClient::Err { req, code: ErrCode(e.name()) }),
        };
        // `handler.rs:1180-1185`.
        if task == St::Canceled {
            return self.to_client(ToClient::CancelOk { req, task: t1(), state: task.observed() });
        }
        if task.terminal() {
            return self.to_client(ToClient::CancelErr { req, code: Code::TaskNotCancelable });
        }
        // `handler.rs:1189-1203`: within one step the upfront check and `begin_cancel` see the same
        // store, so its refusal branch is not reached.
        let task = match self.store_begin_cancel() {
            Ok(s) => s,
            Err(e) => panic!("server: begin_cancel refused after the upfront check: {e:?}"),
        };
        assert!(self.pending.is_none(), "server: a second cancel commit on a canceled task");
        // `handler.rs:1212`, `1225-1226`.
        self.pending =
            Some(PendingCancel { req, execution: self.registered, current: Some(task) });
        self.to_exec(ToExec::CancelCb { task: t1() });
    }

    /// The rest of `cancel_task` on the cancel stream's event (`handler.rs:1229-1297`).
    fn canceled(&mut self) {
        let Some(p) = self.pending.take() else {
            panic!("server: a cancel-stream event with no cancel waiting for it");
        };
        let item = Item::Status(St::Canceled);
        // `handler.rs:1233-1235`: a store error ends the cancel with that error.
        let updated = match self.apply_event_to_task(p.current, item) {
            Ok(u) => u,
            Err(e) => {
                return self.to_client(ToClient::Err { req: p.req, code: ErrCode(e.name()) });
            }
        };
        // `handler.rs:1246-1248`; the stream items it causes are held until the answer is placed.
        self.held = Some(Vec::new());
        if p.execution {
            self.publish(item, updated);
        }
        // the event is terminal: the loop ends (`handler.rs:1250-1253`); `finish`
        // (`handler.rs:1275-1277`).
        if p.execution {
            self.finish();
        }
        // `handler.rs:1284`, `1297` (`release` has no waiting subscriptions here).
        let answer = match self.load_task() {
            Ok(s) => ToClient::CancelOk { req: p.req, task: t1(), state: s.observed() },
            Err(e) => ToClient::Err { req: p.req, code: ErrCode(e.name()) },
        };
        self.answer_among_stream(answer);
    }

    /// `get_task` (`handler.rs:1119-1132`).
    fn get(&mut self, req: u32) {
        match self.load_task() {
            Ok(s) => self.to_client(ToClient::GetOk { req, task: t1(), state: s.observed() }),
            Err(e) => self.to_client(ToClient::Err { req, code: ErrCode(e.name()) }),
        }
    }
}

/// The server's receive loop. Ending it belongs to the harness, not to a2a-rs: `Shutdown` from the
/// client is recorded; once it is recorded and no cancel callback is still unanswered, the server
/// forwards `Shutdown` to the executor and returns. Until then every message is handled as usual.
fn server(client: ThreadId, exec: ThreadId) {
    let mut shutdown = false;
    let mut s = Server {
        client,
        exec,
        store: None,
        snapshot: None,
        registered: false,
        subscribed: false,
        driving: false,
        drive_current: None,
        answered: false,
        tracked: false,
        pending: None,
        held: None,
        baseline: None,
    };
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Shutdown => shutdown = true,
            ToServer::SendStream { req } => s.send_stream(req),
            ToServer::Cancel { req, task } => {
                assert_eq!(task, T1);
                s.cancel(req)
            }
            ToServer::Get { req, task } => {
                assert_eq!(task, T1);
                s.get(req)
            }
            ToServer::Submit { task } => {
                assert_eq!(task, T1);
                s.drive(Item::Task(St::Submitted))
            }
            ToServer::Working { task } => {
                assert_eq!(task, T1);
                s.drive(Item::Status(St::Working))
            }
            ToServer::Completed { task } => {
                assert_eq!(task, T1);
                s.drive(Item::Status(St::Completed))
            }
            ToServer::Reply { task } => {
                assert_eq!(task, T1);
                s.drive(Item::Message)
            }
            ToServer::Canceled { task } => {
                assert_eq!(task, T1);
                s.canceled()
            }
        }
        if shutdown && s.pending.is_none() {
            send_msg(s.exec, ToExec::Shutdown);
            return;
        }
    }
}

/// One server (`env::launch`), or two instances of the same server function wired for delegation
/// (`env::launch2`: the first serves the client and a delegating executor, the second serves that
/// executor and the scripted one).
pub fn run(args: Args) {
    if args.delegation {
        env::launch2(args, server);
    } else {
        env::launch(args, server);
    }
}
