"""`daavar new <name> [--impl rust|java|rust,java] [--dir D]` (DRIVER-DESIGN §3.1).

A Rust specification with a Rust implementation, a Java one, or both: a terminating echo (`Ping{1}`, `Pong{1}`,
`Shutdown`) over msgs/env/spec/imp/main.rs with the usage convention, mirrored by a Java `MpProgram`, plus MAP.md,
SPEC-DESIGN.md, README.md and daavar.toml. Names: `[a-z][a-z0-9-]*` gives the crate `tf_<name_>`, the Java package
`bench.<name_>` and the class `CamelCase(name)`. The TraceForge path dependency is relative to the crate
(absolute, with a warning, when no relative path exists). `--spec java` is refused: check accepts a Java
specification, release 1 does not scaffold one. Exit 0, or 2 refused.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from . import DaavarError, manifest

NAME = re.compile(r"[a-z][a-z0-9-]*\Z")
# Hyphens become underscores in the crate and the Java package (`kv-cancel` -> tf_kv_cancel, bench.kv_cancel) and
# split the class name (KvCancel); a name starts with a letter, so no identifier starts with a digit. A package
# segment may not be a Java keyword or literal (final review 7).
JAVA_RESERVED = frozenset("""abstract assert boolean break byte case catch char class const continue default do double
else enum extends final finally float for goto if implements import instanceof int interface long native new package
private protected public return short static strictfp super switch synchronized this throw throws transient try void
volatile while true false null _ var record yield sealed permits non_sealed""".split())


def camel(name: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in name.split("-") if p)


def _rel(target: Path, frm: Path, warn) -> str:
    t, f = os.path.abspath(target), os.path.abspath(frm)
    if os.path.commonpath([t, f]) == os.sep:
        warn(f"no common directory with {t}; the path is absolute (the project is not relocatable)")
        return t
    return os.path.relpath(t, f)


MSGS = '''//! The message vocabulary shared by the specification and the implementation (and mirrored, tag for tag
//! and field for field, by the Java program). Every tag, field name and field order is a join key.

/// Everything the server receives.
#[derive(Clone, PartialEq, Debug)]
pub enum ToServer {
    Ping { n: u32 },
    Shutdown,
}

/// Everything the client receives.
#[derive(Clone, PartialEq, Debug)]
pub enum ToClient {
    Pong { n: u32 },
}
'''

ENV = '''//! The environment both programs share: the client, and how main starts the roles. The client sends
//! `Ping{1}`, waits for its answer, then sends `Shutdown`.

use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, send_msg};

use crate::msgs::{ToClient, ToServer};

pub fn client(server: ThreadId) {
    send_msg(server, ToServer::Ping { n: 1 });
    let ToClient::Pong { .. } = recv_msg_block::<ToClient>();
    send_msg(server, ToServer::Shutdown);
}

/// The server role `server` starts the client role `client` and then runs `server`.
pub fn entry(server: fn(ThreadId)) -> Entry {
    Arc::new(move || {
        let named = |n: &str| thread::Builder::new().name(n.to_string());
        let srv = named("server")
            .spawn(move || {
                let me = thread::current().id();
                let cl = named("client").spawn(move || client(me)).unwrap().thread().id();
                server(cl);
            })
            .unwrap();
        srv.join().unwrap();
    })
}
'''

SPEC = '''//! The specification: every `Ping{n}` is answered with `Pong{n}` (SPEC-DESIGN.md, S1); `Shutdown` ends the
//! server.

use traceforge::thread::ThreadId;
use traceforge::{recv_msg_block, send_msg};

use crate::msgs::{ToClient, ToServer};

pub fn server(client: ThreadId) {
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Ping { n } => send_msg(client, ToClient::Pong { n }),
            ToServer::Shutdown => return,
        }
    }
}
'''

IMP = '''//! The implementation under check, rewritten as a Must program (MAP.md maps it to its source).

use traceforge::thread::ThreadId;
use traceforge::{recv_msg_block, send_msg};

use crate::msgs::{ToClient, ToServer};

pub fn server(client: ThreadId) {
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Ping { n } => send_msg(client, ToClient::Pong { n }),
            ToServer::Shutdown => return,
        }
    }
}
'''

MAIN = '''//! {name}: a specification program and an implementation, run by TraceForge's shared main
//! (`traceforge::program`). ARGS: `<spec|impl>` (one argument). Bad arguments print a `usage:` line on
//! stderr and exit 2 (the usage convention). Visible threads: `client`, `server`. Model: fifo.

mod env;
mod imp;
mod msgs;
mod spec;

use std::process::ExitCode;

const USAGE: &str = "usage: {crate} count|export|online spec|impl [options] -- <spec|impl>";

fn parse(args: &[String]) -> bool {{
    let usage = |why: String| -> ! {{
        eprintln!("{name}: {{why}}");
        eprintln!("{{USAGE}}");
        std::process::exit(2)
    }};
    match args {{
        [a] if a == "spec" => false,
        [a] if a == "impl" => true,
        _ => usage(format!("expected one argument spec or impl, got {{args:?}}")),
    }}
}}

fn main() -> ExitCode {{
    traceforge::program::main_with_model(
        traceforge::program::Program {{
            visible: &|a| {{
                parse(a);
                ["client", "server"].iter().map(|n| n.to_string()).collect()
            }},
            spec: &|a| {{
                parse(a);
                env::entry(spec::server)
            }},
            imp: &|a| {{
                parse(a);
                env::entry(imp::server)
            }},
        }},
        traceforge::ConsType::FIFO,
    )
}}
'''

CARGO = '''[package]
name = "{crate}"
version = "0.1.0"
edition = "2021"

[dependencies]
traceforge = {{ path = "{tf}" }}

[workspace]
'''

JAVA = '''package bench.{pkg};

import org.mpi_sws.jmc.api.mp.Channel;
import org.mpi_sws.jmc.api.mp.MpProcess;
import org.mpi_sws.jmc.mp.events.CommunicationModel;
import org.mpi_sws.jmc.mp.events.MpMessage;
import org.mpi_sws.jmc.mp.events.Tid;
import {api}.MpProgram;
import org.mpi_sws.jmc.mp.summary.Tag;

import java.util.List;

/**
 * {name}: the implementation under check as a JMC Must program, the Java twin of rust/src/imp.rs (MAP.md).
 * Argument: {{@code impl}}. Bad arguments throw {{@link IllegalArgumentException}} (MpLauncher exits 2).
 */
public final class {cls} implements MpProgram {{

    @Tag("Ping")
    public record Ping(int n) implements MpMessage {{}}

    @Tag("Pong")
    public record Pong(int n) implements MpMessage {{}}

    @Tag("Shutdown")
    public record Shutdown() implements MpMessage {{}}

    private static final CommunicationModel M = CommunicationModel.P2P;
    static final String USAGE = "usage: bench.{pkg}.{cls}:impl";

    public {cls}(String... args) {{
        if (args.length != 1 || !args[0].equals("impl")) {{
            throw new IllegalArgumentException("{cls}: expected impl; " + USAGE);
        }}
    }}

    @Override
    public String programName() {{
        return "{name}";
    }}

    @Override
    public String size() {{
        return "impl";
    }}

    @Override
    public CommunicationModel model() {{
        return M;
    }}

    @Override
    public List<String> rolesInSpawnOrder() {{
        return List.of("server", "client");
    }}

    /** Main only creates, starts and awaits the processes. */
    @Override
    public void run() {{
        MpProcess.Pending server = MpProcess.create("server");
        MpProcess.Pending client = MpProcess.create("client");
        Tid s = server.tid();
        Tid c = client.tid();
        List<MpProcess.Handle> hs = List.of(server.start(() -> server(c)), client.start(() -> client(s)));
        for (MpProcess.Handle h : hs) {{
            h.await();
        }}
    }}

    private static void server(Tid client) {{
        while (true) {{
            MpMessage m = Channel.recv(M);
            if (m instanceof Ping p) {{
                Channel.send(M, client, new Pong(p.n()));
            }} else if (m instanceof Shutdown) {{
                return;
            }}
        }}
    }}

    private static void client(Tid server) {{
        Channel.send(M, server, new Ping(1));
        Channel.recv(M);
        Channel.send(M, server, new Shutdown());
    }}
}}
'''

MAP = '''# MAP: {name}

One row per construct of the implementation's source and where the rewrite puts it; every forced deviation
with file:line, why, and its effect on the compared words (docs/modelling.md).

| source (file:line) | rewrite | deviation | effect |
|---|---|---|---|
| (the server's request loop) | `rust/src/imp.rs` `server`{java_row} | none | none |
'''

SPEC_DESIGN = '''# SPEC-DESIGN: {name}

Every sentence of the protocol's document that the specification encodes, quoted, with the code that encodes
it (docs/spec-derivation.md). MUST restricts; SHOULD/MAY and open points become nondeterministic branches.

| id | quoted sentence | encoded in |
|---|---|---|
| S1 | "Every Ping is answered with a Pong carrying the same number." | `rust/src/spec.rs` `server` |
'''

README = '''# {name}

Scaffolded by `daavar new` (DRIVER-DESIGN §3.1): a Rust specification (`rust/src/spec.rs`) and the
implementation under check ({impls}), over a shared vocabulary (`rust/src/msgs.rs`) and environment
(`rust/src/env.rs`). Replace the echo with the protocol: SPEC-DESIGN.md first, then the rewrite and MAP.md.

```
daavar vocab .     # the two sides' tags and arities agree
daavar check .     # exports, joins, verdict: out/latest/VERDICT.md
```
'''

TOML = '''# {name}: scaffolded by `daavar new` (DRIVER-DESIGN §2, Appendix A's schema).
name = "{name}"
model = "fifo"

[programs.spec]
kind = "spec"
lang = "rust"
side = "spec"
args = ["spec"]
{programs}
[scenarios.one]
visible = ["client", "server"]
programs = [{plist}]
directions = [{dlist}]
cells = [{{ label = "base" }}]

[check]
mode = "offline"
decide_budget = 1000000
witness_limit = 20
witness_nearest = 3
merge = false
threads = 1
seed = 1

[budgets]
export_timeout_s = 600
join_timeout_s = 600
java_xmx = "4g"
mem_cap_gb = 0

[tools]
traceforge = "{tf_tool}"
{jmc}'''


def new(name: str, impl: str = "rust", spec: str = "rust", dest: Path | None = None, warn=print, dry: bool = False):
    if not NAME.match(name):
        raise DaavarError(f"new: the name {name!r} must match [a-z][a-z0-9-]*")
    under0 = name.replace("-", "_")
    if under0 in JAVA_RESERVED and "java" in impl.split(","):
        raise DaavarError(f"new: the name {name!r} gives the Java package bench.{under0}, and {under0!r} is a "
                          f"reserved word in Java; choose another name")
    if spec != "rust":
        raise DaavarError("check accepts a Java spec; release 1 does not scaffold one")
    impls = [x for x in impl.split(",") if x]
    if not impls or any(x not in ("rust", "java") for x in impls) or len(set(impls)) != len(impls):
        raise DaavarError(f"new: --impl is rust, java or rust,java, not {impl!r}")
    d = Path(dest) if dest else Path.cwd() / name
    d = Path(os.path.abspath(d))
    if d.exists() and any(d.iterdir()):
        raise DaavarError(f"new: {d} exists and is not empty")
    under = name.replace("-", "_")
    crate, cls = f"tf_{under}", camel(name)
    drv = manifest.driver_checkout()
    tf_checkout = drv / "traceforge"
    jmc_checkout = drv / "jmc"
    if not (tf_checkout / "traceforge" / "Cargo.toml").is_file():
        raise DaavarError(f"new: no TraceForge checkout at {tf_checkout} (the driver's daavar/traceforge)")
    lock_src = drv / "examples" / "kv-cancel" / "rust" / "Cargo.lock"
    if not lock_src.is_file():
        raise DaavarError(f"new: no template lockfile {lock_src}")
    files = {}
    tf_dep = _rel(tf_checkout / "traceforge", d / "rust", warn)
    files["rust/Cargo.toml"] = CARGO.format(crate=crate, tf=tf_dep)
    lock = lock_src.read_text()
    if 'name = "tf_kv_cancel"' not in lock:
        raise DaavarError(f"new: {lock_src} does not name tf_kv_cancel")
    files["rust/Cargo.lock"] = lock.replace('name = "tf_kv_cancel"', f'name = "{crate}"', 1)
    files["rust/src/msgs.rs"] = MSGS
    files["rust/src/env.rs"] = ENV
    files["rust/src/spec.rs"] = SPEC
    files["rust/src/imp.rs"] = IMP
    files["rust/src/main.rs"] = MAIN.format(name=name, crate=crate)
    progs, plist, dlist = "", ['"spec"'], []
    if "rust" in impls:
        progs += '\n[programs.rust]\nkind = "impl"\nlang = "rust"\nside = "impl"\nargs = ["impl"]\n'
        plist.append('"rust"')
        dlist.append('"rust<=spec"')
    jmc = ""
    if "java" in impls:
        api = "org.mpi_sws.jmc.mp.launch"
        files[f"java/bench/{under}/{cls}.java"] = JAVA.format(pkg=under, cls=cls, name=name, api=api)
        progs += (f'\n[programs.java]\nkind = "impl"\nlang = "java"\nclass = "bench.{under}.{cls}"\n'
                  f'args = ["impl"]\n')
        plist.append('"java"')
        dlist.append('"java<=spec"')
        jmc = (f'\n[tools.jmc]\ncheckout = "{_rel(jmc_checkout, d, warn)}"\ncli = "auto"\n'
               f'main = "org.mpi_sws.jmc.mp.launch.MpLauncher"\napi_package = "{api}"\n')
    if len(impls) == 2:
        dlist += ['"java<=rust"', '"rust<=java"']
    files["daavar.toml"] = TOML.format(name=name, programs=progs, plist=", ".join(plist), dlist=", ".join(dlist),
                                       tf_tool=_rel(tf_checkout, d, warn), jmc=jmc)
    java_row = f"; `java/bench/{under}/{cls}.java` `server`" if "java" in impls else ""
    files["MAP.md"] = MAP.format(name=name, java_row=java_row)
    files["SPEC-DESIGN.md"] = SPEC_DESIGN.format(name=name)
    files["README.md"] = README.format(name=name, impls=" and ".join(
        {"rust": "Rust, `rust/src/imp.rs`", "java": f"Java, `java/bench/{under}/{cls}.java`"}[x] for x in impls))
    if dry:
        return d, sorted(files)
    for rel, text in files.items():
        p = d / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    return d
