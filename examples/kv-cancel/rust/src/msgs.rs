//! The message vocabulary shared by the specification program, the rewrite and the mutant. Every
//! tag, field name and field order is a join key; the Java twin (`KvCancel.java`) mirrors these
//! types field for field. Exported spelling: `{"t":tag,"f":[fields in order]}`, a unit variant
//! `{"t":"Shutdown","f":[]}`.
//!
//! A Must receive is typed, so every message a role can receive is in one enum: `ToServer` for
//! the server, `ToClient` for the client, `ToDisk` for the disk. Every send and receive of the
//! observed roles `client` and `server` is in the word, the server's traffic with the (unobserved)
//! disk included.

/// The one key of the scenario.
pub fn key() -> String {
    "k".to_string()
}

/// Everything the server's mailbox can receive.
#[derive(Clone, PartialEq, Debug)]
pub enum ToServer {
    /// From the client: store `val` under `key`; request id `req`.
    Put { req: u32, key: String, val: u32 },
    /// From the client: cancel request `req`.
    Cancel { req: u32 },
    /// From the disk: the write of request `req` is durable (internal).
    Written { req: u32 },
    /// From the client, last.
    Shutdown,
}

/// Everything the client can receive (all from the server).
#[derive(Clone, PartialEq, Debug)]
pub enum ToClient {
    /// The Put `req` took effect.
    PutOk { req: u32 },
    /// The Put `req` was cancelled: it gets no `PutOk` (the model has no stored state; the
    /// disk's write is still issued and acknowledged, and the server suppresses the reply).
    Canceled { req: u32 },
    /// The cancel of request `req` is acknowledged.
    CancelOk { req: u32 },
    /// The cancel of request `req` is refused (too late, or the store declines).
    CancelErr { req: u32 },
}

/// Everything the disk can receive (from the server).
#[derive(Clone, PartialEq, Debug)]
pub enum ToDisk {
    /// Make `key = val` of request `req` durable.
    Write { req: u32, key: String, val: u32 },
    /// The server ends: the disk ends too.
    Shutdown,
}

/// The program's arguments.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Args {
    /// Nonzero: the deliberately wrong server of that number (impl side only).
    pub mutant: u8,
}
