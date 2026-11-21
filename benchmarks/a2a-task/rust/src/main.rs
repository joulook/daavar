//! A2A task benchmark: one task, one stream, a cancel racing the task's completion, then a read
//! (and optionally a second cancel), as Must programs run by TraceForge's shared main
//! (`traceforge::program`).
//!
//! * spec: the specification program, from the A2A specification at
//!   a2aproject/A2A 5f931b8a25fe66789f987ec4a55fc16ee9fe41ee (`spec.rs`);
//! * impl: the a2a-rs server core at a2aproject/a2a-rs 700a6a28eb001f6fc32bc9446078ff87bdd18c6c,
//!   rewritten (`imp.rs`);
//! * implf: the same core with the two proposed fixes applied (task-first stream and
//!   message-stream close), rewritten (`imp_fixed.rs`).
//!
//! Both run the same client and executor (`env.rs`) and the same messages (`msgs.rs`).
//!
//! ARGS: `<spec|impl|spec2|impl2>,<a|b|n>[,recancel]` (one comma-separated argument); `spec2`/`impl2`
//! select the delegation scenario (client, serverA, execA, serverB, execB; no recancel), and
//! `spec2g`/`impl2g` the same with the gated delegation contract (execA cancels downstream only
//! after the downstream `Status{Working}`); `implf`/`impl2f`/`impl2gf` run the fixed SDK
//! (`imp_fixed.rs`) in place of `impl`/`impl2`/`impl2g`, with the same second and third fields;
//! `mutant<n>` (n = 1..4)
//! runs a deliberately wrong one-server program on the impl side (`count impl -- mutant1,a`).
//! * `a`: the executor hands the task over before reporting `working`; `b`: it reports
//!   `working` first; `n`: the executor is the agent's model as a nondeterministic procedure
//!   (`env::call_llm`): a plain Message, or a task reported in any of the scripted ways;
//! * `recancel`: after the read, the client cancels the terminal task again.
//!
//! The first field names the side the arguments are meant for; it is checked against nothing
//! (the shared main's subcommand chooses the side), so one ARGS string serves both sides.
//!
//! Bad arguments print the reason and a `usage:` line on stderr and exit 2 (no panic).
//!
//! Visible threads: `client`, `server`. Model: fifo by default.
//! Example: `<bin> count spec --model fifo -- spec,a`.

mod env;
mod imp;
mod imp_fixed;
mod msgs;
mod mutants;
mod spec;

use std::process::ExitCode;

use msgs::Args;

const PROGRAM: &str = "a2a-task";
const USAGE: &str = "`<spec|impl|implf|spec2|impl2|impl2f|spec2g|impl2g|impl2gf|mutant<n>>,<a|b|n>[,recancel|local|deleg|delegmsg|delegtask[-<s|w>[<c|w>]]]`";

/// Reports a bad argument: the reason and the usage line on stderr, exit 2 (the usage convention:
/// a program never panics on its arguments).
fn usage(why: String) -> ! {
    eprintln!("{PROGRAM}: {why}\nusage: <bin> count|export|online spec|impl [options] -- {USAGE}");
    std::process::exit(2)
}

fn parse(args: &[String]) -> Args {
    if args.len() != 1 {
        usage(format!("expected 1 argument, got {} ({args:?})", args.len()));
    }
    let f: Vec<&str> = args[0].split(',').collect();
    if !(2..=3).contains(&f.len()) {
        usage(format!("expected 2 or 3 fields in `{}`", args[0]));
    }
    let mut mutant = 0u8;
    let gated = matches!(f[0], "spec2g" | "impl2g" | "impl2gf");
    let delegation = match f[0] {
        "spec" | "impl" | "implf" => false,
        "spec2" | "impl2" | "spec2g" | "impl2g" | "impl2f" | "impl2gf" => true,
        s if s.starts_with("mutant") => {
            mutant = s[6..].parse().unwrap_or_else(|_| usage(format!("bad mutant `{s}`")));
            false
        }
        s => usage(format!("unknown side `{s}`")),
    };
    let llm = f[1] == "n";
    let task_first = match f[1] {
        "a" | "n" => true,
        "b" => false,
        v => usage(format!("unknown variant `{v}`")),
    };
    let (recancel, split) = match f.get(2) {
        None => (false, 0),
        Some(&"recancel") => (true, 0),
        Some(&"local") => (false, 1),
        Some(&"deleg") => (false, 2),
        Some(&"delegmsg") => (false, 3),
        Some(x) if x.starts_with("delegtask") => (false, 4),
        Some(x) => usage(format!("unknown option `{x}`")),
    };
    if split > 0 && !(delegation && f[1] == "n") {
        usage(format!("`{}` splits the delegation scenario with `n` only", f[2]));
    }
    if delegation && recancel {
        usage("no recancel in the delegation scenario".to_string());
    }
    // B's task branch: `delegtask-<k>[<c>]`, k = s (Submit first) or w (Working first), c = c
    // (Completed at once) or w (Working, then Completed or nothing more).
    let (mut b_kind, mut b_cont) = (0u8, 0u8);
    if let Some(x) = f.get(2).and_then(|x| x.strip_prefix("delegtask")) {
        let r: Vec<char> = x.strip_prefix('-').unwrap_or(x).chars().collect();
        if !x.is_empty() && (r.is_empty() || r.len() > 2 || !x.starts_with('-')) {
            usage(format!("bad split `{}`", f[2]));
        }
        let pick = |c: char| match c {
            's' | 'c' => 1u8,
            'w' => 2,
            _ => usage(format!("bad split `{}`", f[2])),
        };
        if let Some(&c) = r.first() {
            if c == 'c' {
                usage(format!("bad split `{}`", f[2]));
            }
            b_kind = pick(c);
        }
        if let Some(&c) = r.get(1) {
            if c == 's' {
                usage(format!("bad split `{}`", f[2]));
            }
            b_cont = pick(c);
        }
    }
    Args { task_first, recancel, delegation, mutant, gated, llm, split, b_kind, b_cont }
}

/// The first field selects the fixed SDK (`implf`, `impl2f`, `impl2gf`); `parse` checks the rest.
fn fixed(args: &[String]) -> bool {
    matches!(args[0].split(',').next(), Some("implf" | "impl2f" | "impl2gf"))
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
            imp: &|a| {
                let p = parse(a);
                // On the impl side both forms used to panic during the exploration (`mutants.rs`
                // `server_n` and its `Reply` arm), so they are argument errors there; the spec side
                // ignores the mutant number and runs `spec::run` as before.
                if p.mutant > 4 {
                    usage(format!("unknown mutant `{}` (mutant1 to mutant4)", a[0].split(',').next().unwrap_or("")));
                }
                if p.mutant > 0 && p.llm {
                    usage("the mutants run with the fixed executor scripts only (variant a or b)".to_string());
                }
                if p.mutant > 0 {
                    env::entry(p, mutants::run)
                } else if fixed(a) {
                    env::entry(p, imp_fixed::run)
                } else {
                    env::entry(p, imp::run)
                }
            },
        },
        traceforge::ConsType::FIFO,
    )
}
