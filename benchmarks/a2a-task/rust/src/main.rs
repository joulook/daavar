//! A2A task benchmark: one task, one stream, a cancel racing the task's completion, then a read
//! (and optionally a second cancel), as Must programs run by TraceForge's shared main
//! (`traceforge::program`).
//!
//! * spec: the specification program, from the A2A specification at
//!   a2aproject/A2A 5f931b8a25fe66789f987ec4a55fc16ee9fe41ee (`spec.rs`);
//! * impl: the a2a-rs server core at a2aproject/a2a-rs 700a6a28eb001f6fc32bc9446078ff87bdd18c6c,
//!   rewritten (`imp.rs`).
//!
//! Both run the same client and executor (`env.rs`) and the same messages (`msgs.rs`).
//!
//! ARGS: `<spec|impl|spec2|impl2>,<a|b>[,recancel]` (one comma-separated argument); `spec2`/`impl2`
//! select the delegation scenario (client, serverA, execA, serverB, execB; no recancel).
//! * `a`: the executor hands the task over before reporting `working`; `b`: it reports
//!   `working` first;
//! * `recancel`: after the read, the client cancels the terminal task again.
//!
//! The first field names the side the arguments are meant for; it is checked against nothing
//! (the shared main's subcommand chooses the side), so one ARGS string serves both sides.
//!
//! Visible threads: `client`, `server`. Model: fifo by default.
//! Example: `<bin> count spec --model fifo -- spec,a`.

mod env;
mod imp;
mod msgs;
mod spec;

use std::process::ExitCode;

use msgs::Args;

const PROGRAM: &str = "a2a-task";
const USAGE: &str = "`<spec|impl|spec2|impl2>,<a|b>[,recancel]`";

fn parse(args: &[String]) -> Args {
    if args.len() != 1 {
        panic!("{PROGRAM}: expected 1 argument, got {} ({args:?}); ARGS are {USAGE}", args.len());
    }
    let f: Vec<&str> = args[0].split(',').collect();
    if !(2..=3).contains(&f.len()) {
        panic!("{PROGRAM}: expected 2 or 3 fields in `{}`; ARGS are {USAGE}", args[0]);
    }
    let delegation = match f[0] {
        "spec" | "impl" => false,
        "spec2" | "impl2" => true,
        s => panic!("{PROGRAM}: unknown side `{s}`; ARGS are {USAGE}"),
    };
    let task_first = match f[1] {
        "a" => true,
        "b" => false,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    let recancel = match f.get(2) {
        None => false,
        Some(&"recancel") => true,
        Some(x) => panic!("{PROGRAM}: unknown option `{x}`; ARGS are {USAGE}"),
    };
    if delegation && recancel {
        panic!("{PROGRAM}: no recancel in the delegation scenario; ARGS are {USAGE}");
    }
    Args { task_first, recancel, delegation }
}

fn main() -> ExitCode {
    traceforge::program::main_with_model(
        traceforge::program::Program {
            visible: &|a| {
                let names: &[&str] = if parse(a).delegation {
                    &["client", "serverA", "serverB"]
                } else {
                    &["client", "server"]
                };
                names.iter().map(|n| n.to_string()).collect()
            },
            spec: &|a| env::entry(parse(a), spec::run),
            imp: &|a| env::entry(parse(a), imp::run),
        },
        traceforge::ConsType::FIFO,
    )
}
