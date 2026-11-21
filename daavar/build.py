"""`daavar build [<dir>]` (DRIVER-DESIGN §3.2): tool identities, tool builds, project builds, build keys.

Inputs: the manifest's files and the TraceForge path dependency only; `build.rs`, other path dependencies
and `.cargo/config*` are refused. Tool identity: the checkout's commit, exported as TRACEFORGE_COMMIT /
JMC_COMMIT; a dirty checkout is refused without --allow-dirty-tools, with it the identity adds
sha256(diff, untracked). Build key: sha256 of the tool identity, the manifest digest, the compiler versions,
RUSTFLAGS/CARGO_* and the build flags; the artifact goes read-only to the cache under a flock. A second
build with the same key builds nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from . import DaavarError, cache, jmc, manifest, runner, tf

INIT_SCRIPT = Path(__file__).resolve().parent / "print-classpath.gradle"
RUST_FLAGS = ["--release", "--locked"]
JAVAC_FLAGS = ["-d", "-cp"]
ENV_KEYS_EXTRA = ("RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER")
# Cargo-set variables whose values follow from the manifest (Cargo.toml's [package]) and the target names;
# CARGO_MANIFEST_DIR and the like name the build's location, which the key does not record.
CARGO_KEYED_ENV = ("CARGO_CRATE_NAME", "CARGO_BIN_NAME", "CARGO_PRIMARY_PACKAGE")


class BuildError(DaavarError):
    pass


# ---------------------------------------------------------------- tool identity


@dataclass
class ToolId:
    tool: str  # traceforge | jmc
    checkout: Path
    commit: str
    dirty: bool
    digest: str | None = None  # sha256(diff, untracked) of a dirty checkout

    @property
    def identity(self) -> str:
        return self.commit if not self.dirty else f"{self.commit}-dirty-{self.digest[:16]}"

    def as_dict(self) -> dict:
        return {"tool": self.tool, "checkout": str(self.checkout), "commit": self.commit, "dirty": self.dirty,
                "dirty_digest": self.digest, "identity": self.identity}


def _git(checkout: Path, *args) -> bytes:
    code, out, err = runner.capture(["git", "-C", str(checkout), "-c", "core.quotepath=off", *args])
    if code != 0:
        raise BuildError(f"git {' '.join(args)} in {checkout}: exit {code}: {err.decode(errors='replace').strip()}")
    return out


IDENTITY_MARKER = ".daavar-tool-identity.json"


def tool_identity(tool: str, checkout: Path, allow_dirty: bool = False) -> ToolId:
    if not checkout.is_dir():
        raise BuildError(f"the {tool} checkout {checkout} does not exist ([tools] in daavar.toml)")
    marker = checkout / IDENTITY_MARKER
    if marker.is_file() and not (checkout / ".git").exists():
        # A tool restored from a verified snapshot by --from-run (no repository): the recorded identity.
        m = json.loads(marker.read_text())
        if not m.get("dirty_digest") or not allow_dirty:
            raise BuildError(f"{checkout}: a snapshot-restored {tool} needs --allow-dirty-tools")
        return ToolId(tool, checkout, m["commit"], True, m["dirty_digest"])
    try:
        commit = _git(checkout, "rev-parse", "--verify", "HEAD").decode().strip()
    except BuildError as e:
        raise BuildError(f"the {tool} checkout {checkout} is not a git checkout: {e}") from None
    status = _git(checkout, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    if not status:
        return ToolId(tool, checkout, commit, False)
    changed = [x.decode(errors="replace") for x in status.split(b"\0") if x]
    # scripts/print-classpath.gradle (scripts/setup-node.sh) writes an untracked cp.txt at the JMC checkout's top:
    # a build output of that helper, not a source (final review, Opus 2; the driver's own init script writes
    # into the cache, D8).
    generated = {"cp.txt"} if tool == "jmc" else set()
    changed = [c for c in changed if not (c.startswith("?? ") and c[3:] in generated)]
    if not changed:
        return ToolId(tool, checkout, commit, False)
    if not allow_dirty:
        raise BuildError(f"the {tool} checkout {checkout} is dirty ({len(changed)} changed: "
                         f"{', '.join(c[3:] for c in changed[:5])}{' ...' if len(changed) > 5 else ''}); commit or "
                         f"stash, or pass --allow-dirty-tools")
    h = hashlib.sha256()
    h.update(b"diff\0")
    h.update(_git(checkout, "diff", "--binary", "HEAD"))
    h.update(b"\0untracked\0")
    for p in sorted(x for x in _git(checkout, "ls-files", "-z", "--others", "--exclude-standard").split(b"\0") if x):
        f = checkout / os.fsdecode(p)
        h.update(p + b"\0" + (cache.sha256_file(f).encode() if f.is_file() else b"-") + b"\n")
    return ToolId(tool, checkout, commit, True, h.hexdigest())


def git_head_nofork(checkout: Path) -> str | None:
    """HEAD's commit read from the files (no process: for --dry-run), or None."""
    try:
        g = checkout / ".git"
        if g.is_file():
            t = g.read_text().strip()
            if not t.startswith("gitdir:"):
                return None
            g = (checkout / t[len("gitdir:"):].strip()).resolve()
        head = (g / "HEAD").read_text().strip()
        if not head.startswith("ref:"):
            return head
        ref = head[4:].strip()
        common = g
        if (g / "commondir").is_file():
            common = (g / (g / "commondir").read_text().strip()).resolve()
        for base in (g, common):
            if (base / ref).is_file():
                return (base / ref).read_text().strip()
        packed = common / "packed-refs"
        if packed.is_file():
            for l in packed.read_text().splitlines():
                if l.endswith(" " + ref):
                    return l.split()[0]
    except OSError:
        return None
    return None


# ---------------------------------------------------------------- inputs


def _cargo_home() -> Path:
    return Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo").resolve()


def check_rust_inputs(crate: Path, tf_checkout: Path) -> dict:
    """§3.2's input rule for a Rust crate; returns what the build key records (CARGO_HOME's config)."""
    if not (crate / "Cargo.toml").is_file():
        raise BuildError(f"no {crate}/Cargo.toml")
    if not (crate / "Cargo.lock").is_file():
        raise BuildError(f"no {crate}/Cargo.lock: the build is --locked")
    try:
        with open(crate / "Cargo.toml", "rb") as f:
            t = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise BuildError(f"{crate}/Cargo.toml: {e}") from None
    if (crate / "build.rs").exists() or "build" in t.get("package", {}):
        raise BuildError(f"{crate}: a build script (build.rs) is refused: the inputs are the manifest's files")
    want = (tf_checkout / "traceforge").resolve()
    found = False

    def deps(table, where):
        nonlocal found
        for name, spec in (table or {}).items():
            if isinstance(spec, dict) and "path" in spec:
                p = (crate / spec["path"]).resolve()
                if p != want:
                    raise BuildError(f"{crate}/Cargo.toml {where}.{name}: path dependency {spec['path']} is refused; "
                                     f"only TraceForge at {want} is allowed")
                found = True
            if isinstance(spec, dict) and "git" in spec and name == "traceforge":
                raise BuildError(f"{crate}/Cargo.toml: TraceForge must be a path dependency on {want}")

    for k in ("dependencies", "dev-dependencies", "build-dependencies"):
        deps(t.get(k), k)
    for tname, tt in t.get("target", {}).items():
        for k in ("dependencies", "dev-dependencies", "build-dependencies"):
            deps(tt.get(k), f"target.{tname}.{k}")
    for reg, pt in t.get("patch", {}).items():
        deps(pt, f"patch.{reg}")
    deps(t.get("replace"), "replace")
    deps(t.get("workspace", {}).get("dependencies"), "workspace.dependencies")
    if not found:
        raise BuildError(f"{crate}/Cargo.toml does not depend on TraceForge by path ({want})")
    # Every compiled input must be in the manifest (D13): targets under src/, no symbolic links there, no
    # files pulled in by include!/include_str!/include_bytes! or #[path].
    for sect in ("lib", "bin", "example", "test", "bench"):
        entries = t.get(sect, [])
        for e in ([entries] if isinstance(entries, dict) else entries):
            tp = e.get("path") if isinstance(e, dict) else None
            if isinstance(e, dict) and e.get("required-features"):
                raise BuildError(f"{crate}/Cargo.toml [{sect}] {e.get('name', '')}: required-features is refused (a "
                                 f"target Cargo may skip would be copied stale from the target directory)")
            if tp is not None and (not tp.startswith("src/") or not tp.endswith(".rs") or ".." in Path(tp).parts):
                raise BuildError(f"{crate}/Cargo.toml [{sect}] path {tp!r}: a target outside src/**/*.rs is refused "
                                 f"(the manifest covers src/**/*.rs only)")
    src = crate / "src"
    if src.is_symlink():
        raise BuildError(f"{src}: a symbolic link as the source root is refused (source-manifest.sh does not follow it)")
    for dirpath, dirnames, filenames in os.walk(src):
        for n in dirnames + filenames:
            q = Path(dirpath) / n
            if q.is_symlink():
                raise BuildError(f"{q}: a symbolic link under src/ is refused (the manifest skips it)")
        # No token heuristics over the sources (review S1-S6 F2): after the build, Cargo's dep-info is the
        # authority on what the compiler read (check_dep_info).
    if "workspace" not in t:
        raise BuildError(f"{crate}/Cargo.toml has no [workspace] table: the crate must be its own workspace root "
                         f"(an enclosing workspace's profiles, overrides and lock are not in the manifest)")
    ws = t["workspace"] if isinstance(t["workspace"], dict) else {}
    for k in ("members", "default-members"):
        if ws.get(k):
            raise BuildError(f"{crate}/Cargo.toml [workspace] {k}: other workspace members are refused (their "
                             f"sources are not in the manifest)")
    if "workspace" in t.get("package", {}):
        raise BuildError(f"{crate}/Cargo.toml: package.workspace names another workspace; refused")
    for v in ("RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
              "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"):
        if os.environ.get(v):
            raise BuildError(f"{v} is set: a compiler override is refused (the build key records `rustc -vV` of "
                             f"the default compiler)")
    for v in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS"):
        bad = _linker_flag(os.environ.get(v, "").replace("\x1f", " "))
        if bad:
            raise BuildError(f"{v} holds `{bad}`: linker inputs and linker choices are refused (an external object "
                             f"or library is not in the manifest)")
    if os.environ.get("CARGO_BUILD_TARGET"):
        raise BuildError("CARGO_BUILD_TARGET is set: the driver builds for the host only (target/release/<bin>)")
    ch = _cargo_home()
    for d in [crate.resolve(), *crate.resolve().parents]:
        for name in ("config", "config.toml"):
            c = d / ".cargo" / name
            if c.is_file() and c.parent.resolve() != ch:
                raise BuildError(f"{c}: a Cargo configuration file is refused (it changes the build outside the "
                                 f"manifest)")
    home_cfg = {}
    for name in ("config", "config.toml"):
        c = ch / name
        if c.is_file():
            try:
                with open(c, "rb") as f:
                    hc = tomllib.load(f)
            except (OSError, tomllib.TOMLDecodeError) as e:
                raise BuildError(f"{c}: unreadable Cargo configuration: {e}") from None
            for k in ("paths", "patch"):
                if k in hc:
                    raise BuildError(f"{c}: `{k}` overrides dependencies by path; refused (D9)")
            if "target" in hc.get("build", {}):
                raise BuildError(f"{c}: build.target is refused: the driver builds for the host only")
            if "target" in hc:
                raise BuildError(f"{c}: [target] (linker, rustflags, runner) is refused: it changes the build "
                                 f"outside the key")
            flags = hc.get("build", {}).get("rustflags", "")
            bad = _linker_flag(" ".join(flags) if isinstance(flags, list) else str(flags))
            if bad:
                raise BuildError(f"{c}: build.rustflags holds `{bad}`: linker inputs are refused")
            for k in ("rustc", "rustc-wrapper", "rustc-workspace-wrapper"):
                if k in hc.get("build", {}):
                    raise BuildError(f"{c}: build.{k} is refused: a compiler override (the key records the default "
                                     f"compiler's `rustc -vV`)")
            home_cfg[str(c)] = cache.sha256_file(c)
    return home_cfg


def _dep_paths(text: str) -> list:
    """The prerequisites of a Makefile-style dep-info rule (`out: a b\\ c ...`), unescaped."""
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        k = line.find(": ")
        if k < 0:
            continue
        rest, cur, i = line[k + 2:], [], 0
        while i < len(rest):
            ch = rest[i]
            if ch == "\\" and i + 1 < len(rest):
                cur.append(rest[i + 1])
                i += 2
                continue
            if ch == " ":
                if cur:
                    out.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
            i += 1
        if cur:
            out.append("".join(cur))
        break
    return out


def _locked_packages(crate: Path) -> tuple:
    """Cargo.lock's registry packages as `<name>-<version>` and its git revisions."""
    try:
        with open(crate / "Cargo.lock", "rb") as f:
            lock = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise BuildError(f"{crate}/Cargo.lock: {e}") from None
    names, revs = set(), set()
    for p in lock.get("package", []):
        src = p.get("source", "")
        if src.startswith("registry+") or src.startswith("sparse+"):
            names.add(f"{p['name']}-{p['version']}")
        elif src.startswith("git+") and "#" in src:
            revs.add(src.rsplit("#", 1)[1])
    return names, revs


def tracked_files(checkout: Path) -> set:
    """The files a tool identity covers: tracked, plus untracked non-ignored (the dirty digest's input)."""
    out = _git(checkout, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    return {os.fsdecode(x) for x in out.split(b"\0") if x}


def check_dep_info(target: Path, bins: list, crate: Path, allowed_roots: list, manifest_text: str,
                   tool_files: dict | None = None) -> None:
    """The authoritative input check after a build (D13): every file the compiler read for a binary (Cargo's
    `release/<bin>.d`) is a manifest file of the crate or lies under an allowed root (the TraceForge
    checkout, CARGO_HOME); the crate's own compilation (`release/deps/<crate>-*.d`, the newest) has no
    environment dependency other than CARGO_PKG_* and the target names (CARGO_KEYED_ENV). Whatever a macro does, what it read shows up here."""
    files = set(manifest.files_of(manifest_text))
    croot = Path(os.path.realpath(crate))
    # Allowed outside the crate: files of a tool checkout that its identity covers (tool_files: root -> set of
    # relative paths) and Cargo's registry/git sources (pinned by Cargo.lock's checksums and revisions).
    tool_files = {Path(os.path.realpath(k)): v for k, v in (tool_files or {}).items()}
    ch = _cargo_home()
    roots = [ch / "registry" / "src", ch / "git" / "checkouts"]

    locked, revs = _locked_packages(crate)

    def allowed(rp: Path) -> bool:
        for r, fs in tool_files.items():
            if r in rp.parents:
                return rp.relative_to(r).as_posix() in fs
        reg, git = roots
        if reg in rp.parents:  # registry/src/<index>/<name>-<version>/...
            parts = rp.relative_to(reg).parts
            return len(parts) >= 3 and parts[1] in locked
        if git in rp.parents:  # git/checkouts/<name>-<hash>/<rev>/...
            parts = rp.relative_to(git).parts
            return len(parts) >= 3 and any(r.startswith(parts[1]) for r in revs)
        return False
    for n in bins:
        d = target / "release" / f"{n}.d"
        if not d.is_file():
            raise BuildError(f"{d}: no dep-info file after the build")
        for f in _dep_paths(d.read_text(errors="replace")):
            rp = Path(os.path.realpath(f if os.path.isabs(f) else croot / f))
            if rp == croot or croot in rp.parents:
                rel = rp.relative_to(croot).as_posix()
                if rel not in files:
                    raise BuildError(f"{crate}: the build read {rel}, which is not in the manifest (Cargo.toml, "
                                     f"Cargo.lock, src/**/*.rs); refused")
            elif not allowed(rp):
                raise BuildError(f"the build of {n} read {f}, which neither the manifest, a tool identity nor "
                                 f"Cargo.lock covers; refused")
    # Every compilation unit of the crate (binaries and a library, each its own <name>-<hash>.d; a library and
    # a binary may share the name): every dep-info file that lists a crate file.
    newest = {}
    for dp in (target / "release" / "deps").glob("*.d"):
        text = dp.read_text(errors="replace")
        # rustc runs in the package root, so a relative prerequisite (`src/main.rs`) is relative to the crate.
        prereq = [Path(os.path.realpath(x if os.path.isabs(x) else croot / x)) for x in _dep_paths(text)]
        if not any(croot in x.parents for x in prereq):
            continue
        newest[dp.stem] = (dp.stat().st_mtime_ns, text)
    for unit, (_, text) in list(newest.items()):
        for line in text.splitlines():
            if line.startswith("# env-dep:"):
                var = line[len("# env-dep:"):].split("=", 1)[0]
                if not (var.startswith("CARGO_PKG_") or var in CARGO_KEYED_ENV):
                    raise BuildError(f"the crate's unit {unit} depends on the environment variable {var} (env!/"
                                     f"option_env!), which the build key does not record; refused")


def _linker_flag(flags: str) -> str | None:
    """The first rustc flag that brings a linker input or picks a linker (`-C link-arg(s)=`, `-C linker=`,
    `-L`, `-l`, `-Z pre-link-arg(s)`), else None."""
    toks = flags.split()
    for k, t in enumerate(toks):
        joined = t if t not in ("-C", "-Z") or k + 1 >= len(toks) else t + toks[k + 1]
        for bad in ("link-arg", "linker", "pre-link-arg", "link-self-contained"):
            if bad in joined:
                return joined
        if t.startswith("-L") or t.startswith("-l"):
            return t
    return None


def env_subset(env=None) -> dict:
    env = os.environ if env is None else env
    return {k: v for k, v in sorted(env.items())
            if (k.startswith("CARGO_") and k != "CARGO_TARGET_DIR") or k in ENV_KEYS_EXTRA}


def _version(argv, cwd=None) -> str:
    code, out, err = runner.capture(argv, cwd=cwd)
    if code != 0:
        raise BuildError(f"{' '.join(argv)}: exit {code}: {err.decode(errors='replace').strip()}")
    return (out + err).decode(errors="replace").strip()


def java_tools() -> tuple:
    jh = os.environ.get("JAVA_HOME")
    if jh and (Path(jh) / "bin" / "javac").is_file():
        return str(Path(jh) / "bin" / "java"), str(Path(jh) / "bin" / "javac")
    java, javac = shutil.which("java"), shutil.which("javac")
    if not java or not javac:
        raise BuildError("no java/javac (set JAVA_HOME to a JDK 17 or put it on PATH)")
    return java, javac


# ---------------------------------------------------------------- results


@dataclass
class Step:
    name: str
    argv: list
    cwd: str
    env: dict
    built: bool  # False: the cached result was used (a no-op)
    result: dict | None = None


@dataclass
class BuildResult:
    tools: dict = field(default_factory=dict)  # traceforge / jmc -> dict
    artifacts: dict = field(default_factory=dict)  # rust / java -> dict
    steps: list = field(default_factory=list)  # Step
    cached_only: bool = False  # check --no-build: nothing may be built

    @property
    def built_anything(self) -> bool:
        return any(s.built for s in self.steps)

    def as_dict(self) -> dict:
        return {"tools": self.tools, "artifacts": self.artifacts, "built_anything": self.built_anything,
                "steps": [{"name": s.name, "argv": s.argv, "cwd": s.cwd, "env": s.env, "built": s.built,
                           "result": s.result} for s in self.steps]}


def _run_build(step: str, argv, cwd, env, logdir: Path) -> dict:
    r = runner.run(argv, cwd, logdir / f"{step}.out", logdir / f"{step}.err", env_delta=env)
    if r.exit != 0:
        text = r.read_stderr() or r.read_stdout()
        tail = "\n".join(text.splitlines()[-20:])
        raise BuildError(f"{step}: `{' '.join(map(str, argv))}` in {cwd} exited {r.exit}; log {logdir}/{step}.err\n{tail}")
    return r.as_record()


def _keep_failed_logs(tmp: Path, name: str) -> Path:
    d = cache.ensure("logs", f"{name}-{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}")
    for p in tmp.glob("*.out"):
        shutil.copyfile(p, d / p.name)
    for p in tmp.glob("*.err"):
        shutil.copyfile(p, d / p.name)
    return d


# ---------------------------------------------------------------- tools


def conf_join_inputs(target: Path) -> dict:
    """What conf-join's bytes depend on besides the TraceForge identity (final review 3): the compilers, the
    keyed environment and the target directory (rustc embeds absolute source paths)."""
    return {"rustc": _version(["rustc", "-vV"]), "cargo": _version(["cargo", "-V"]), "env": env_subset(),
            "target": str(target)}


def conf_join(tfid: ToolId, res: BuildResult) -> Path:
    d = cache.root() / "tools" / f"traceforge-{tfid.identity}"
    argv = tf.conf_join_build_argv()
    target = cache.root() / "target" / tfid.identity / "traceforge"
    env = {"CARGO_TARGET_DIR": str(target), "TRACEFORGE_COMMIT": tfid.commit}
    if (d / "tool.json").is_file():
        res.steps.append(Step("conf-join", argv, str(tfid.checkout), env, False))
        return d / "conf-join"
    if res.cached_only:
        raise BuildError("--no-build: a needed tool or artifact is not in the cache; run `daavar build` first")
    with cache.lock(f"tool-traceforge-{tfid.identity}"):
        if (d / "tool.json").is_file():
            res.steps.append(Step("conf-join", argv, str(tfid.checkout), env, False))
            return d / "conf-join"
        tmp = cache.tmpdir(d.parent)
        try:
            rec = _run_build("conf-join", argv, tfid.checkout, env, tmp)
            shutil.copyfile(target / "release" / "conf-join", tmp / "conf-join")
            os.chmod(tmp / "conf-join", 0o755)
            (tmp / "tool.json").write_text(json.dumps({**tfid.as_dict(), "build_inputs": conf_join_inputs(target),
                                                      "conf_join_sha256":
                                                      cache.sha256_file(tmp / "conf-join")}, indent=1) + "\n")
            cache.make_readonly(tmp)
            cache.install(tmp, d)
        except BaseException as e:
            if tmp.exists():
                if isinstance(e, BuildError):
                    _keep_failed_logs(tmp, f"conf-join-{tfid.identity}")
                cache.remove(tmp)
            raise
    res.steps.append(Step("conf-join", argv, str(tfid.checkout), env, True, rec))
    return d / "conf-join"


def jmc_tool(jid: ToolId, res: BuildResult) -> dict:
    """JMC's core test classes and classpath (§3.2), copied to tools/jmc-<identity>/."""
    d = cache.root() / "tools" / f"jmc-{jid.identity}"
    gradle = ["./gradlew", "-q", "--no-daemon", "--offline"]
    a1 = gradle + [":core:testClasses"]
    env = {"JMC_COMMIT": jid.commit}

    def done():
        t = json.loads((d / "tool.json").read_text())
        return {"dir": str(d), "classpath": t["classpath"], **jid.as_dict()}

    if (d / "tool.json").is_file():
        res.steps.append(Step("jmc", a1, str(jid.checkout), env, False))
        return done()
    if not (jid.checkout / "gradlew").is_file():
        raise BuildError(f"{jid.checkout}: no gradlew (is [tools.jmc] checkout a JMC checkout?)")
    if res.cached_only:
        raise BuildError("--no-build: a needed tool or artifact is not in the cache; run `daavar build` first")
    with cache.lock(f"tool-jmc-{jid.identity}"):
        if (d / "tool.json").is_file():
            res.steps.append(Step("jmc", a1, str(jid.checkout), env, False))
            return done()
        tmp = cache.tmpdir(d.parent)
        try:
            # Review S1-S6 A4: Gradle runs in a staged copy of the checkout inside the cache (its sources, no
            # .git, no build outputs), so neither core/build nor Gradle's project state is written into the
            # tool checkout. Gradle's user home (its download cache) stays the user's GRADLE_USER_HOME.
            staged = tmp / "src"
            shutil.copytree(jid.checkout, staged, symlinks=True,
                            ignore=shutil.ignore_patterns(".git", "build", ".gradle", "cp.txt"))
            rec = _run_build("gradle-testClasses", a1, staged, env, tmp)
            a2 = gradle + ["-I", str(INIT_SCRIPT), f"-PdaavarCp={tmp / 'cp.raw'}", ":core:daavarPrintTestClasspath"]
            rec2 = _run_build("gradle-classpath", a2, staged, env, tmp)
            steps_done = [("jmc-testClasses", a1, rec), ("jmc-classpath", a2, rec2)]
            raw = (tmp / "cp.raw").read_text().strip().split(":")
            # Every classpath entry inside the staged tree (core's classes, any other subproject's output) is
            # copied into the tool directory, keeping its path below the checkout.
            root = staged.resolve()
            cp = []
            for e in raw:
                p = Path(e)
                if not p.exists():
                    continue
                try:
                    rel = p.resolve().relative_to(root)
                except ValueError:
                    cp.append(str(p))
                    continue
                dst = tmp / "tree" / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if p.is_dir():
                    shutil.copytree(p, dst, symlinks=False)
                else:
                    shutil.copyfile(p, dst)
                cp.append(str(d / "tree" / rel))
            shutil.rmtree(staged)
            (tmp / "cp.txt").write_text(":".join(cp) + "\n")
            # every executable classpath entry outside the tool directory (the Gradle cache's jars) is hashed too
            jars = {e: cache.sha256_file(Path(e)) for e in cp if Path(e).is_file()}
            (tmp / "tool.json").write_text(json.dumps({**jid.as_dict(), "classpath": cp, "jars_sha256": jars,
                                                      "classes_sha256": cache.tree_digest(tmp / "tree"),
                                                      "build_cwd": str(staged)}, indent=1) + "\n")
            cache.make_readonly(tmp)
            cache.install(tmp, d)
        except BaseException as e:
            if tmp.exists():
                if isinstance(e, BuildError):
                    _keep_failed_logs(tmp, f"jmc-{jid.identity}")
                cache.remove(tmp)
            raise
    for name, argv_, r_ in steps_done:  # the real cwd: the staged copy of the checkout (final review 5)
        res.steps.append(Step(name, argv_, r_.get("cwd") or str(staged), env, True, r_))
    return done()


def cli_cache_path(identity: str, main: str) -> Path:
    return cache.root() / "tools" / f"jmc-{identity}.cli-{hashlib.sha256(main.encode()).hexdigest()[:12]}.json"


def resolve_cli(cfg_cli: str, jtool: dict, main: str, res: BuildResult) -> str:
    """§3.7: a configured form is used as is; `auto` probes `--help` once per JMC identity and main class."""
    if cfg_cli != "auto":
        return cfg_cli
    p = cli_cache_path(jtool["identity"], main)
    if p.is_file():
        return json.loads(p.read_text())["cli"]
    java, _ = java_tools()
    argv = jmc.help_probe_argv(java, jtool["classpath"], main)
    tmp = cache.tmpdir(p.parent)
    try:
        r = runner.run(argv, tmp, tmp / "probe.out", tmp / "probe.err", timeout_s=120)
        form = jmc.cli_from_probe(r.exit, r.read_stderr(), r.timed_out)
        if form is None:
            raise BuildError(f"the JMC CLI probe `{' '.join(argv)}` neither succeeded nor refused --help as the "
                             f"positional launcher does (exit {r.exit}); not cached: set [tools.jmc] cli")
        cache.ensure("tools")
        manifest.write(json.dumps({"cli": form, "probe_exit": r.exit, "main": main}) + "\n", p)
    finally:
        cache.remove(tmp)
    res.steps.append(Step("jmc-cli-probe", argv, str(tmp), {}, True, {"exit": r.exit, "cli": form}))
    return form


# ---------------------------------------------------------------- project artifacts


def _artifact(key: str) -> Path:
    return cache.root() / "artifacts" / key


def rust_artifact(cfg, tfid: ToolId, res: BuildResult, daavar: Path | None = None) -> dict:
    crate = cfg.root / "rust"
    home_cfg = check_rust_inputs(crate, tfid.checkout)
    text = manifest.rust(crate, cfg.root, daavar)
    bins = manifest.rust_bins(crate)
    if not bins:
        raise BuildError(f"{crate}: no binary target")
    # Probed in the crate's directory, where cargo runs: a rust-toolchain file there selects the compiler.
    compilers = {"rustc": _version(["rustc", "-vV"], crate), "cargo": _version(["cargo", "-V"], crate)}
    key_obj = {"kind": "rust", "tool": tfid.identity, "manifest": manifest.digest(text), "compilers": compilers,
               "env": env_subset(), "cargo_home_config": home_cfg, "flags": RUST_FLAGS, "bins": bins}
    key = cache.key_of(key_obj)
    target = cache.root() / "target" / tfid.identity / hashlib.sha256(
        str(crate.resolve()).encode()).hexdigest()[:12]
    argv = ["cargo", "build", *RUST_FLAGS]
    env = {"CARGO_TARGET_DIR": str(target), "TRACEFORGE_COMMIT": tfid.commit}
    d = _artifact(key)

    def done(built, rec=None):
        res.steps.append(Step("rust", argv, str(crate), env, built, rec))
        b = json.loads((d / "build.json").read_text())
        return {"key": key, "dir": str(d), "out": str(d / "out"), "bins": {n: str(d / "out" / n) for n in bins},
                "manifest": b["manifest"], "sha256": b["sha256"], "built": built}

    if (d / "build.json").is_file():
        return done(False)
    if res.cached_only:
        raise BuildError("--no-build: a needed tool or artifact is not in the cache; run `daavar build` first")
    with cache.lock(f"artifact-{key}"):
        if (d / "build.json").is_file():
            return done(False)
        tmp = cache.tmpdir(d.parent)
        try:
            (tmp / "out").mkdir()
            sums = {}
            # Builds of other keys share the crate's target directory: compile and copy under its lock.
            with cache.lock(f"target-{tfid.identity}-{target.name}"):
                rec = _run_build("build", argv, crate, env, tmp)
                check_dep_info(target, bins, crate, [], text, {tfid.checkout: tracked_files(tfid.checkout)})
                after = manifest.rust(crate, cfg.root, daavar)
                if after != text:
                    raise BuildError(f"{crate}: the sources changed during the build; build again")
                for n in bins:
                    shutil.copyfile(target / "release" / n, tmp / "out" / n)
                    os.chmod(tmp / "out" / n, 0o755)
            # The manifests after the binaries: a tool treats a manifest older than its binary as missing.
            for n in bins:
                manifest.write(text, tmp / "out" / f"{n}.SOURCE.md5")
            for n in bins:
                sums[n] = cache.sha256_file(tmp / "out" / n)
                sums[f"{n}.SOURCE.md5"] = cache.sha256_file(tmp / "out" / f"{n}.SOURCE.md5")
            (tmp / "build.json").write_text(json.dumps({"key": key, "key_inputs": key_obj, "manifest": text,
                                                        "sha256": sums, "command": rec}, indent=1) + "\n")
            cache.make_readonly(tmp)
            cache.install(tmp, d)
        except BaseException as e:
            if tmp.exists():
                if isinstance(e, BuildError):
                    _keep_failed_logs(tmp, f"rust-{key[:12]}")
                cache.remove(tmp)
            raise
    return done(True, rec)


def java_sources(project: Path) -> list:
    return [project / p for p in manifest._files(project, "java", "*.java", exclude_out=True)]


def java_artifact(cfg, jtool: dict, res: BuildResult, daavar: Path | None = None) -> dict:
    project = cfg.root
    src_text = manifest.java(project, None, project, daavar)
    srcs = java_sources(project)
    if not srcs:
        raise BuildError(f"{project}/java holds no .java file")
    java, javac = java_tools()
    compilers = {"javac": _version([javac, "-version"])}
    key_obj = {"kind": "java", "tool": jtool["identity"], "classpath": jtool["classpath"],
               "manifest": manifest.digest(src_text), "compilers": compilers, "flags": JAVAC_FLAGS}
    key = cache.key_of(key_obj)
    d = _artifact(key)
    cp = ":".join(jtool["classpath"])

    def done(built, rec=None, argv=None):
        res.steps.append(Step("java", argv or [javac, "-d", "<out>", "-cp", "<CP>", "<sources>"], str(project), {},
                              built, rec))
        b = json.loads((d / "build.json").read_text())
        return {"key": key, "dir": str(d), "classes": str(d / "out"), "manifest": b["manifest"],
                "sha256": b["sha256"], "built": built}

    if (d / "build.json").is_file():
        return done(False)
    if res.cached_only:
        raise BuildError("--no-build: a needed tool or artifact is not in the cache; run `daavar build` first")
    with cache.lock(f"artifact-{key}"):
        if (d / "build.json").is_file():
            return done(False)
        tmp = cache.tmpdir(d.parent)
        try:
            out = tmp / "out"
            out.mkdir()
            argv = [javac, "-d", str(out), "-cp", cp, *map(str, srcs)]
            rec = _run_build("build", argv, project, {}, tmp)
            if manifest.java(project, None, project, daavar) != src_text:
                raise BuildError(f"{project}: the sources changed during the build; build again")
            text = manifest.java(project, out, project, daavar)
            manifest.write(text, out / "SOURCE.md5")
            (tmp / "build.json").write_text(json.dumps({"key": key, "key_inputs": key_obj, "manifest": text,
                                                        "sha256": cache.tree_digest(out), "command": rec},
                                                       indent=1) + "\n")
            cache.make_readonly(tmp)
            cache.install(tmp, d)
        except BaseException as e:
            if tmp.exists():
                if isinstance(e, BuildError):
                    _keep_failed_logs(tmp, f"java-{key[:12]}")
                cache.remove(tmp)
            raise
    return done(True, rec, argv)


# ---------------------------------------------------------------- the command


def build(cfg, allow_dirty_tools: bool = False, daavar: Path | None = None, cached_only: bool = False) -> BuildResult:
    res = BuildResult(cached_only=cached_only)
    langs = cfg.langs
    tf_checkout = cfg.tool_path(cfg.tools["traceforge"])
    if not (tf_checkout / "traceforge" / "Cargo.toml").is_file():
        raise BuildError(f"{tf_checkout} is not a TraceForge checkout (no traceforge/Cargo.toml; [tools] traceforge)")
    if "rust" in langs:
        check_rust_inputs(cfg.root / "rust", tf_checkout)
    if "java" in langs and not (cfg.root / "java").is_dir():
        raise BuildError(f"no {cfg.root}/java (the project has Java programs)")
    if "java" in langs and (cfg.root / "java").is_symlink():
        raise BuildError(f"{cfg.root}/java: a symbolic link as the source root is refused (source-manifest.sh "
                         f"does not follow it)")
    tfid = tool_identity("traceforge", tf_checkout, allow_dirty_tools)
    cj = conf_join(tfid, res)
    res.tools["traceforge"] = {**tfid.as_dict(), "conf_join": str(cj)}
    if "java" in langs:
        jc = cfg.tools["jmc"]
        jid = tool_identity("jmc", cfg.tool_path(jc["checkout"]), allow_dirty_tools)
        jt = jmc_tool(jid, res)
        jt["cli"] = resolve_cli(jc["cli"], jt, jc["main"], res)
        jt["main"] = jc["main"]
        res.tools["jmc"] = jt
    if "rust" in langs:
        res.artifacts["rust"] = rust_artifact(cfg, tfid, res, daavar)
    if "java" in langs:
        res.artifacts["java"] = java_artifact(cfg, res.tools["jmc"], res, daavar)
    return res


def dry_run(cfg) -> tuple:
    """(exit, commands): no process runs; identities are read from the checkouts' files. Exit 2 when the
    JMC CLI form is `auto` and no probe is cached (§8)."""
    cmds = []
    tf_checkout = cfg.tool_path(cfg.tools["traceforge"])
    commit = git_head_nofork(tf_checkout) or "<traceforge-commit>"
    cmds.append({"cwd": str(tf_checkout), "env": {"CARGO_TARGET_DIR": f"{cache.root()}/target/{commit}/traceforge",
                                                 "TRACEFORGE_COMMIT": commit}, "argv": tf.conf_join_build_argv()})
    code = 0
    if "java" in cfg.langs:
        jc = cfg.tools["jmc"]
        jco = cfg.tool_path(jc["checkout"])
        jcommit = git_head_nofork(jco) or "<jmc-commit>"
        staged = f"{cache.root()}/tools/.tmp-<id>/src"
        cmds.append({"cwd": str(jco), "env": {}, "argv": ["<copy the checkout without .git, build, .gradle to>",
                                                          staged]})
        cmds.append({"cwd": staged, "env": {"JMC_COMMIT": jcommit},
                     "argv": ["./gradlew", "-q", "--no-daemon", "--offline", ":core:testClasses"]})
        cmds.append({"cwd": staged, "env": {"JMC_COMMIT": jcommit},
                     "argv": ["./gradlew", "-q", "--no-daemon", "--offline", "-I", str(INIT_SCRIPT),
                              "-PdaavarCp=<tmp>/cp.raw", ":core:daavarPrintTestClasspath"]})
        if jc["cli"] == "auto" and not cli_cache_path(jcommit, jc["main"]).is_file():
            code = 2
    if "rust" in cfg.langs:
        cmds.append({"cwd": str(cfg.root / "rust"), "env": {"CARGO_TARGET_DIR": f"{cache.root()}/target/{commit}/<path12>",
                                                           "TRACEFORGE_COMMIT": commit},
                     "argv": ["cargo", "build", *RUST_FLAGS]})
    if "java" in cfg.langs:
        cmds.append({"cwd": str(cfg.root), "env": {}, "argv": ["javac", "-d", "<tmp>", "-cp", "<CP>", "<java/**/*.java>"]})
    return code, cmds

