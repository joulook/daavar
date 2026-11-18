//! The message vocabulary shared by the specification program and the rewrites: every tag, field
//! name and field order below is a join key and is final; the Java twin mirrors these types field
//! for field. Exported spelling: states and codes are unit variants (`Submitted`, `Working`,
//! `Completed`, `Canceled`; `TaskNotCancelable`); an `Err`'s code is exported as the string `"*"`.
//!
//! Mailboxes: a Must receive is typed (a message of another type is an error), so every message a
//! role can receive is in one enum: `ToServer` for servers, `ToClient` for clients and executors.
//! `ToExec` is another name for `ToClient`, so that a delegating executor (the client of a second
//! server and the executor of a first one) can receive both kinds in one mailbox; the exported tag
//! of a message is its variant name, so this changes no exported observation. Observation: every send and receive of the observed roles `client` and
//! `server` is observed, including the server's traffic with the (unobserved) executor; so the
//! executor's script is part of the compared behaviour and is the same in every program.

/// The task's name: the server names the task after the request that created it (request 1),
/// so no generated identifier appears in a message.
pub const T1: &str = "t1";

/// The task states used here. `Completed` and `Canceled` are terminal.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Submitted,
    Working,
    Completed,
    Canceled,
}

impl State {
    pub fn terminal(self) -> bool {
        matches!(self, State::Completed | State::Canceled)
    }
}

/// The code of a refused cancel.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Code {
    TaskNotCancelable,
}

/// The code of any other error. It carries the implementation's own name for the error, which is
/// not observed: every `ErrCode` is equal to every other and prints as `"*"`, so an observation
/// says only "an error here".
#[derive(Clone, Copy)]
pub struct ErrCode(pub &'static str);

impl PartialEq for ErrCode {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl std::fmt::Debug for ErrCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "\"*\"")
    }
}

/// Everything the server's single mailbox can receive.
#[derive(Clone, PartialEq, Debug)]
pub enum ToServer {
    /// The client's last message, sent after all its requests were answered. The server ends
    /// once every cancel callback it sent has been answered, telling the executor to end first.
    Shutdown,
    // From the client (observed at the client's send).
    /// Send a message and stream the task's updates (request 1).
    SendStream { req: u32 },
    /// Cancel the task (request 2; request 4 is the second cancel of the re-cancel run).
    Cancel { req: u32, task: String },
    /// Read the task's current state (request 3).
    Get { req: u32, task: String },
    // From the executor (internal).
    /// The executor has the task (variant A only: the executor's first report).
    Submit { task: String },
    /// The executor is working on the task.
    Working { task: String },
    /// The executor finished the task.
    Completed { task: String },
    /// The executor stopped the task after a cancel callback.
    Canceled { task: String },
}

/// Everything the client can receive (all from the server).
#[derive(Clone, PartialEq, Debug)]
pub enum ToClient {
    /// A stream item carrying the Task object.
    TaskItem { req: u32, task: String, state: State },
    /// A stream item carrying a status-update event; `fin` is true exactly on a terminal state.
    Status { req: u32, task: String, state: State, fin: bool },
    /// The end of the stream of request `req`.
    StreamClose { req: u32 },
    /// A successful cancel answer: the task as it is after the request.
    CancelOk { req: u32, task: String, state: State },
    /// A cancel refused because the task is not cancelable.
    CancelErr { req: u32, code: Code },
    /// The answer to a read of the task.
    GetOk { req: u32, task: String, state: State },
    /// Any other error, for any request.
    Err { req: u32, code: ErrCode },
    // From a server to its executor.
    /// Start executing the task.
    Exec { task: String },
    /// Cancel callback: stop the task.
    CancelCb { task: String },
    /// The server ends: the executor ends too.
    Shutdown,
}

impl ToClient {
    /// The request this message answers or belongs to.
    pub fn req(&self) -> u32 {
        match self {
            ToClient::TaskItem { req, .. }
            | ToClient::Status { req, .. }
            | ToClient::StreamClose { req }
            | ToClient::CancelOk { req, .. }
            | ToClient::CancelErr { req, .. }
            | ToClient::GetOk { req, .. }
            | ToClient::Err { req, .. } => *req,
            // Executor messages carry no request id.
            ToClient::Exec { .. } | ToClient::CancelCb { .. } | ToClient::Shutdown => 0,
        }
    }
}

/// What a server sends to its executor (`Exec`, `CancelCb`, `Shutdown`): the same type as
/// `ToClient` (see the module comment).
pub type ToExec = ToClient;

/// The program's arguments.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Args {
    /// Variant A (`true`): the executor's first report is `Submit`, then `Working`; variant B
    /// (`false`): its first report is `Working`.
    pub task_first: bool,
    /// `true`: after the read, the client cancels the (now terminal) task a second time.
    pub recancel: bool,
    /// `true`: the delegation scenario (two servers; the first one's executor is a client of the
    /// second).
    pub delegation: bool,
    /// Nonzero: the deliberately wrong server of that number (spec-strength check), impl side.
    pub mutant: u8,
}

pub fn t1() -> String {
    T1.to_string()
}
