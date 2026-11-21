//! kv-cancel: a key-value store with one client whose Put races a Cancel of the same request, as
//! Must programs run by TraceForge's shared main (`traceforge::program`).
//!
//! * spec: the specification program (`spec.rs`);
//! * impl: a correct server (`imp.rs`);
//! * mutant1: `imp.rs` with the injected bug (PutOk after CancelOk), run on the impl side.
//!
//! All run the same client and disk (`env.rs`) and the same messages (`msgs.rs`).
//!
//! ARGS: `<spec|impl|mutant1>` (one argument). The field names the program the arguments are meant
//! for; the shared main's subcommand chooses the side, and the spec side never runs a mutant.
//! Bad arguments print a `usage:` line on stderr and exit 2.
//!
//! Visible threads: `client`, `server`. Model: fifo. Example: `<bin> count spec -- spec`.

mod env;
mod imp;
mod msgs;
mod spec;

use std::process::ExitCode;

use msgs::Args;

const PROGRAM: &str = "kv-cancel";
const USAGE: &str = "usage: <bin> count|export|online spec|impl [options] -- <spec|impl|mutant1>";

/// Reports a bad argument: the reason and the usage line on stderr, exit 2.
fn usage(why: String) -> ! {
    eprintln!("{PROGRAM}: {why}\n{USAGE}");
    std::process::exit(2)
}

fn parse(args: &[String]) -> Args {
    if args.len() != 1 {
        usage(format!("expected 1 argument, got {} ({args:?})", args.len()));
    }
    let mutant = match args[0].as_str() {
        "spec" | "impl" => 0u8,
        "mutant1" => 1,
        s => usage(format!("unknown program `{s}`")),
    };
    Args { mutant }
}

fn main() -> ExitCode {
    traceforge::program::main_with_model(
        traceforge::program::Program {
            visible: &|a| {
                parse(a);
                ["client", "server"].iter().map(|n| n.to_string()).collect()
            },
            spec: &|a| {
                parse(a);
                env::entry(Args { mutant: 0 }, spec::server)
            },
            imp: &|a| env::entry(parse(a), imp::server),
        },
        traceforge::ConsType::FIFO,
    )
}
