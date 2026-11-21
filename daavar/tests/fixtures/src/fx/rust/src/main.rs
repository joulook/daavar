//! Driver fixture program (daavar/daavar/tests/fixtures): an echo whose ARGS
//! `<spec|impl>,<mode>` plant each process outcome of DRIVER-DESIGN §4.1.
//!
//! Modes: `ok` (the echo); `bug` (impl only: the server may answer n+1); `panic` (the server
//! panics); `stdassert` (a failed `assert!`, i.e. a panic); `assert` (a failed
//! `traceforge::assert`); `sleep` (the server sleeps an hour: a driver timeout); `badvis` (the
//! visible list names `main`: a refused declaration). Anything else: a `usage:` line and exit 2
//! (the usage convention). Visible threads: `client`, `server`. Model: fifo.

mod msgs;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use msgs::{ToClient, ToServer};
use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Ok,
    Bug,
    Panic,
    StdAssert,
    Assert,
    Sleep,
    BadVis,
}

fn parse(args: &[String]) -> (bool, Mode) {
    let usage = || -> ! {
        eprintln!("usage: tf_fx <subcommand> ... -- <spec|impl>,<ok|bug|panic|stdassert|assert|sleep|badvis>");
        std::process::exit(2)
    };
    if args.len() != 1 {
        usage()
    }
    let f: Vec<&str> = args[0].split(',').collect();
    if f.len() != 2 {
        usage()
    }
    let imp = match f[0] {
        "spec" => false,
        "impl" => true,
        _ => usage(),
    };
    let mode = match f[1] {
        "ok" => Mode::Ok,
        "bug" => Mode::Bug,
        "panic" => Mode::Panic,
        "stdassert" => Mode::StdAssert,
        "assert" => Mode::Assert,
        "sleep" => Mode::Sleep,
        "badvis" => Mode::BadVis,
        _ => usage(),
    };
    (imp, mode)
}

fn server(client: ThreadId, imp: bool, mode: Mode) {
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Ping { n } => {
                match mode {
                    Mode::Panic => panic!("fx: planted panic"),
                    Mode::StdAssert => assert!(n == 0, "fx: planted assert"),
                    Mode::Assert => traceforge::assert(n == 0),
                    Mode::Sleep => std::thread::sleep(Duration::from_secs(3600)),
                    _ => {}
                }
                let m = if imp && mode == Mode::Bug && nondet() { n + 1 } else { n };
                send_msg(client, ToClient::Pong { n: m });
            }
            ToServer::Shutdown => return,
        }
    }
}

fn client(server: ThreadId) {
    send_msg(server, ToServer::Ping { n: 1 });
    let ToClient::Pong { .. } = recv_msg_block::<ToClient>();
    send_msg(server, ToServer::Shutdown);
}

fn entry(imp: bool, mode: Mode) -> Entry {
    Arc::new(move || {
        let named = |n: &str| thread::Builder::new().name(n.to_string());
        let srv = named("server")
            .spawn(move || {
                let me = thread::current().id();
                let named = |n: &str| thread::Builder::new().name(n.to_string());
                let cl = named("client").spawn(move || client(me)).unwrap().thread().id();
                server(cl, imp, mode);
            })
            .unwrap();
        srv.join().unwrap();
    })
}

fn main() -> ExitCode {
    traceforge::program::main_with_model(
        traceforge::program::Program {
            visible: &|a| {
                let mut v = vec!["client".to_string(), "server".to_string()];
                if parse(a).1 == Mode::BadVis {
                    v.push("main".to_string());
                }
                v
            },
            spec: &|a| {
                let (_, mode) = parse(a);
                entry(false, mode)
            },
            imp: &|a| {
                let (_, mode) = parse(a);
                entry(true, mode)
            },
        },
        traceforge::ConsType::FIFO,
    )
}
