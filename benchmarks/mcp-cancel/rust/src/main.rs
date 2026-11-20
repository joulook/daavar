//! MCP cancel benchmark: two tool calls over stdio, a cancel of the first racing its completion,
//! as Must programs run by TraceForge's shared main (`traceforge::program`).
//!
//! * spec: the specification program, from the MCP specification at
//!   modelcontextprotocol/modelcontextprotocol d4b317231161ee96d3eefb2448bda4230c76f4c6, revision
//!   2026-07-28 (`spec.rs`);
//! * impl: the rmcp server core (modelcontextprotocol/rust-sdk 165f8fd5), rewritten (`imp.rs`);
//! * mutant<n> (n = 1..3): a deliberately wrong variant of the specification program, run on the
//!   impl side, to check that the specification rejects it.
//!
//! All run the same client and handlers (`env.rs`) and the same messages (`msgs.rs`).
//!
//! ARGS: `<spec|impl|mutant1|mutant2|mutant3>[,<h1><h2>[,<reading>]]` (one comma-separated argument);
//! `<reading>` is the specification's reading of the cancellation rule, `s0` (strict, the default),
//! `s1`, `s2` or `s3` (see spec.rs); a fourth field `c2` lets the client also cancel request 2. `<h1>`
//! is handler1's branch, `a`, `b`, `c` or `x` (any of the three); `<h2>` is handler2's, `a`, `b` or
//! `x` (either). The default is `xx`: every branch nondeterministic. The first field names the side
//! the arguments are meant for, except `mutant<n>`, which selects the impl-side program.
//!
//! Visible threads: `client`, `server`. Model: fifo. Example: `<bin> count spec --model fifo -- spec,xx`.

mod env;
mod imp;
mod msgs;
mod spec;

use std::process::ExitCode;

use msgs::{Args, Branch};

const PROGRAM: &str = "mcp-cancel";
const USAGE: &str = "`<spec|impl|mutant1|mutant2|mutant3>[,<a|b|c|x><a|b|x>[,s0|s1|s2|s3[,c2]]]`";

fn parse(args: &[String]) -> Args {
    if args.len() != 1 {
        panic!("{PROGRAM}: expected 1 argument, got {} ({args:?}); ARGS are {USAGE}", args.len());
    }
    let f: Vec<&str> = args[0].split(',').collect();
    if !(1..=4).contains(&f.len()) {
        panic!("{PROGRAM}: expected 1 to 4 fields in `{}`; ARGS are {USAGE}", args[0]);
    }
    let mutant = match f[0] {
        "spec" | "impl" => 0u8,
        "mutant1" => 1,
        "mutant2" => 2,
        "mutant3" => 3,
        s => panic!("{PROGRAM}: unknown side `{s}`; ARGS are {USAGE}"),
    };
    let v: Vec<char> = f.get(1).copied().unwrap_or("xx").chars().collect();
    if v.len() != 2 {
        panic!("{PROGRAM}: the variant must be two letters; ARGS are {USAGE}");
    }
    let b = |c: char, third: bool| match c {
        'a' => Branch::A,
        'b' => Branch::B,
        'c' if third => Branch::C,
        'x' => Branch::Any,
        _ => panic!("{PROGRAM}: bad branch `{c}`; ARGS are {USAGE}"),
    };
    let reading = match f.get(2).copied().unwrap_or("s0") {
        "s0" => 0,
        "s1" => 1,
        "s2" => 2,
        "s3" => 3,
        r => panic!("{PROGRAM}: unknown reading `{r}`; ARGS are {USAGE}"),
    };
    let cancel2 = match f.get(3).copied() {
        None => false,
        Some("c2") => true,
        Some(x) => panic!("{PROGRAM}: unknown option `{x}`; ARGS are {USAGE}"),
    };
    Args { h1: b(v[0], true), h2: b(v[1], false), mutant, reading, cancel2 }
}

fn main() -> ExitCode {
    traceforge::program::main_with_model(
        traceforge::program::Program {
            visible: &|_| ["client", "server"].iter().map(|n| n.to_string()).collect(),
            // The specification side never runs a mutant: a mutant is compared against the
            // unmodified specification.
            spec: &|a| env::entry(Args { mutant: 0, ..parse(a) }, spec::server),
            imp: &|a| {
                let p = parse(a);
                if p.mutant > 0 {
                    env::entry(p, spec::server)
                } else {
                    env::entry(p, imp::server)
                }
            },
        },
        traceforge::ConsType::FIFO,
    )
}
