//! The message vocabulary shared by the specification program and the rewrites. Every tag, field
//! name and field order below is a join key and is final; the Java twin mirrors these types field
//! for field. Exported spelling: a variant is `{"t":tag,"f":[fields in order]}`, a unit variant
//! `{"t":"Shutdown","f":[]}`; an `Error`'s code is exported as the string `"*"`.
//!
//! Mailboxes: a Must receive is typed, so every message a role can receive is in one enum:
//! `ToServer` for the server, `ToClient` for the client, `ToHandler` for both handlers.
//! Observation: every send and receive of the observed roles `client` and `server` is in the word,
//! including the server's traffic with the (unobserved) handlers.

/// Progress token of request `r`: the client names it `t<r>` (MCP allows a string or a number).
pub fn token(r: u32) -> String {
    format!("t{r}")
}

/// The code of an error response. It carries the implementation's own name for the error, which
/// is not observed: every `ErrCode` equals every other and prints as `"*"`.
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

/// Everything the server's mailbox can receive.
#[derive(Clone, PartialEq, Debug)]
pub enum ToServer {
    /// From the client, last: the server's standard input is closed.
    Shutdown,
    /// From the client: a `tools/call` request with id `req` and `_meta.progressToken = token`.
    Call { req: u32, token: String },
    /// From the client: `notifications/cancelled` with `requestId = req`.
    Cancelled { req: u32 },
    /// From a handler: progress `value` of request `req` (internal).
    Prog { req: u32, token: String, value: u32 },
    /// From a handler: the tool of request `req` returned successfully (internal).
    Done { req: u32 },
}

/// Everything the client can receive (all from the server).
#[derive(Clone, PartialEq, Debug)]
pub enum ToClient {
    /// `notifications/progress` with `progressToken = token`, `progress = value`.
    Progress { token: String, value: u32 },
    /// The result response to request `req`.
    Result { req: u32 },
    /// An error response to request `req` (no path of this scenario produces one).
    Error { req: u32, code: ErrCode },
}

impl ToClient {
    /// The request a message belongs to (progress: the request whose token it carries).
    pub fn req(&self) -> u32 {
        match self {
            ToClient::Progress { token, .. } => token[1..].parse().unwrap(),
            ToClient::Result { req } | ToClient::Error { req, .. } => *req,
        }
    }
}

/// Everything a handler can receive (from the server).
#[derive(Clone, PartialEq, Debug)]
pub enum ToHandler {
    /// Run the tool of request `req`.
    Work { req: u32, token: String },
    /// The server's cancellation signal for request `req`.
    Stop { req: u32 },
    /// The server ends: the handler ends too.
    Shutdown,
}

/// A handler's branch: `A` = `Prog 1`, `Done`; `B` = `Prog 1`, `Prog 2`, `Done`; `C` = `Prog 1`,
/// then keep working (handler1 only); `Any` = chosen nondeterministically.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Branch {
    A,
    B,
    C,
    Any,
}

/// The program's arguments.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Args {
    /// handler1's branch (r1): `A`, `B`, `C` or `Any` (among the three).
    pub h1: Branch,
    /// handler2's branch (r2): `A`, `B` or `Any` (between the two).
    pub h2: Branch,
    /// Nonzero: the deliberately wrong server of that number (spec-strength check), impl side.
    pub mutant: u8,
    /// The specification's reading of the cancellation rule, 0..=3 (spec side only; see spec.rs).
    pub reading: u8,
    /// `true`: the client may also cancel request 2 (default `false`).
    pub cancel2: bool,
}
