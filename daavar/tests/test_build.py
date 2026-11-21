"""S6 units (no tool runs here): manifests against source-manifest.sh's output captured on brain03, the input
rule, the read-only cache, the build key, and a second build being a no-op (the build steps stubbed)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import json
import os
import shutil
import threading
import time
import unittest
from unittest import mock

from daavar import build, cache, config, manifest
from daavar.tests.util import DAAVAR, FIXTURES, Scratch

FX = FIXTURES / "src" / "fx"
CAPTURED = FIXTURES / "manifests"


class Manifests(unittest.TestCase):
    def test_rust_equals_the_script(self):
        """fixtures/manifests/fx-rust.SOURCE.md5 is source-manifest.sh's output on brain03 (S6 evidence)."""
        got = manifest.rust(FX / "rust", FX, DAAVAR)
        self.assertEqual(got, (CAPTURED / "fx-rust.SOURCE.md5").read_text())
        self.assertTrue(got.startswith("# root daavar/tests/fixtures/src/fx/rust\n"))

    def test_java_sources_equal_the_script(self):
        script = (CAPTURED / "fx-java.SOURCE.md5").read_text()
        no_classes = "".join(l for l in script.splitlines(keepends=True) if "  classes/" not in l)
        self.assertEqual(manifest.java(FX, None, FX, DAAVAR), no_classes)

    def test_outside_the_checkout(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        p = s.path / "proj"
        shutil.copytree(FX, p)
        text = manifest.rust(p / "rust", p, DAAVAR)
        self.assertTrue(text.startswith("# root project:rust\n"))
        body = (CAPTURED / "fx-rust.SOURCE.md5").read_text().split("\n", 1)[1]
        self.assertEqual(text.split("\n", 1)[1], body)
        self.assertTrue(manifest.java(p, None, p, DAAVAR).startswith("# root project:.\n"))

    def test_symlinked_cargo_files_count(self):
        """source-manifest.sh hashes Cargo.toml/Cargo.lock with `[ -f ]`, which follows a symbolic link."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        p = s.path / "proj"
        shutil.copytree(FX, p)
        (s.path / "real.lock").write_bytes((p / "rust/Cargo.lock").read_bytes())
        (p / "rust/Cargo.lock").unlink()
        (p / "rust/Cargo.lock").symlink_to(s.path / "real.lock")
        self.assertEqual(manifest.rust(p / "rust", p, DAAVAR).split("\n", 1)[1],
                         (CAPTURED / "fx-rust.SOURCE.md5").read_text().split("\n", 1)[1])

    def test_symlinks_and_out_are_skipped(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        p = s.path / "proj"
        shutil.copytree(FX, p)
        (p / "rust/src/link.rs").symlink_to(p / "rust/src/msgs.rs")
        (p / "java/out").mkdir()
        (p / "java/out/Old.java").write_text("class Old {}")
        (p / "java/fx/out").mkdir()
        (p / "java/fx/out/Old.java").write_text("class Old {}")
        self.assertNotIn("link.rs", manifest.rust(p / "rust", p, DAAVAR))
        self.assertNotIn("Old.java", manifest.java(p, None, p, DAAVAR))

    def test_bins(self):
        self.assertEqual(manifest.rust_bins(FX / "rust"), ["tf_fx"])
        s = Scratch()
        self.addCleanup(s.cleanup)
        c = s.path / "c"
        (c / "src" / "bin" / "tool").mkdir(parents=True)
        (c / "src" / "bin" / "tool" / "main.rs").write_text("fn main(){}")
        (c / "src" / "bin" / "two.rs").write_text("fn main(){}")
        (c / "src" / "main.rs").write_text("fn main(){}")
        (c / "Cargo.toml").write_text('[package]\nname = "p"\nversion = "0.1.0"\n')
        self.assertEqual(manifest.rust_bins(c), ["p", "tool", "two"])

    def test_files_of(self):
        f = manifest.files_of((CAPTURED / "fx-rust.SOURCE.md5").read_text())
        self.assertEqual(sorted(f), ["Cargo.lock", "Cargo.toml", "src/main.rs", "src/msgs.rs"])


class Inputs(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.root = self.s.path
        (self.root / "tf" / "traceforge").mkdir(parents=True)
        (self.root / "tf" / "traceforge" / "Cargo.toml").write_text("[package]\nname='traceforge'\n")
        self.crate = self.root / "p" / "rust"
        (self.crate / "src").mkdir(parents=True)
        (self.crate / "src" / "main.rs").write_text("fn main() {}\n")
        (self.crate / "Cargo.lock").write_text("")
        self.toml('traceforge = { path = "../../tf/traceforge" }')

    def tearDown(self):
        self.s.cleanup()

    def toml(self, deps, extra=""):
        (self.crate / "Cargo.toml").write_text(f'[package]\nname = "x"\nversion = "0.1.0"\n{extra}\n'
                                               f'[workspace]\n[dependencies]\n{deps}\n')

    def check(self):
        return build.check_rust_inputs(self.crate, self.root / "tf")

    def test_ok(self):
        self.check()

    def test_refusals(self):
        (self.crate / "build.rs").write_text("")
        self.assertRaisesRegex(build.BuildError, "build script", self.check)
        (self.crate / "build.rs").unlink()
        self.toml('traceforge = { path = "../../tf/traceforge" }\nother = { path = "../other" }')
        self.assertRaisesRegex(build.BuildError, "path dependency ../other is refused", self.check)
        self.toml('traceforge = { path = "../../elsewhere/traceforge" }')
        self.assertRaisesRegex(build.BuildError, "is refused", self.check)
        self.toml('serde = "1"')
        self.assertRaisesRegex(build.BuildError, "does not depend on TraceForge", self.check)
        self.toml('traceforge = { path = "../../tf/traceforge" }', 'build = "gen.rs"')
        self.assertRaisesRegex(build.BuildError, "build script", self.check)
        self.toml('traceforge = { path = "../../tf/traceforge" }')
        (self.root / "p" / ".cargo").mkdir()
        (self.root / "p" / ".cargo" / "config.toml").write_text("")
        self.assertRaisesRegex(build.BuildError, "Cargo configuration file is refused", self.check)
        shutil.rmtree(self.root / "p" / ".cargo")
        (self.crate / "Cargo.lock").unlink()
        self.assertRaisesRegex(build.BuildError, "--locked", self.check)

    def test_unmanifested_inputs_refused(self):
        self.toml('traceforge = { path = "../../tf/traceforge" }\n[[bin]]\nname = "x"\npath = "other/main.rs"')
        self.assertRaisesRegex(build.BuildError, "outside src", self.check)
        self.toml('traceforge = { path = "../../tf/traceforge" }')
        # Review S1-S6 F2: no token heuristics; ordinary sources pass (the dep-info check decides later).
        for src in ("use crate::env::Env as E;", "use crate::env::{Client as C, Server};",
                    "struct F { include: bool }", 'const D: &str = include_str!("../data.txt");'):
            (self.crate / "src" / "data.rs").write_text(src + "\n")
            self.check()
        (self.crate / "src" / "data.rs").unlink()
        (self.crate / "src" / "l.rs").symlink_to(self.crate / "src" / "main.rs")
        self.assertRaisesRegex(build.BuildError, "symbolic link", self.check)

    def test_workspace_and_target(self):
        t = (self.crate / "Cargo.toml").read_text()
        (self.crate / "Cargo.toml").write_text(t.replace("[workspace]\n", ""))
        self.assertRaisesRegex(build.BuildError, "no \\[workspace\\] table", self.check)
        (self.crate / "Cargo.toml").write_text(t)
        with mock.patch.dict(os.environ, {"CARGO_BUILD_TARGET": "x86_64-unknown-linux-musl"}):
            self.assertRaisesRegex(build.BuildError, "CARGO_BUILD_TARGET", self.check)
        with mock.patch.dict(os.environ, {"RUSTC": "/opt/rustc"}):
            self.assertRaisesRegex(build.BuildError, "compiler override", self.check)
        for flags in ("-C link-arg=/abs/helper.o", "-Clinker=/x/cc", "-L /opt/lib", "-lfoo"):
            with mock.patch.dict(os.environ, {"RUSTFLAGS": flags}):
                self.assertRaisesRegex(build.BuildError, "linker", self.check)
        with mock.patch.dict(os.environ, {"RUSTFLAGS": "-C opt-level=3 -C target-cpu=native"}):
            self.check()
        (self.crate / "Cargo.toml").write_text(t.replace("[workspace]\n", '[workspace]\nmembers = ["sub"]\n'))
        self.assertRaisesRegex(build.BuildError, "members", self.check)
        (self.crate / "Cargo.toml").write_text(t + '\n[[bin]]\nname = "x"\npath = "src/main.rs"\nrequired-features = ["f"]\n')
        self.assertRaisesRegex(build.BuildError, "required-features", self.check)

    def test_symlinked_src_root(self):
        real = self.root / "realsrc"
        shutil.move(str(self.crate / "src"), real)
        (self.crate / "src").symlink_to(real)
        self.assertRaisesRegex(build.BuildError, "source root", self.check)

    def test_dep_info(self):
        """Cargo's dep-info decides: a file outside the manifest, the tool's covered files and Cargo's sources,
        or a non-Cargo env dependency of any crate unit, is refused."""
        target = self.root / "target"
        (target / "release" / "deps").mkdir(parents=True)
        tf = self.root / "tf"
        (tf / "traceforge" / "src").mkdir(parents=True, exist_ok=True)
        tfiles = {tf: {"traceforge/src/lib.rs"}}
        ch = self.root / "ch"
        (ch / "registry" / "src" / "i" / "serde-1.0.0").mkdir(parents=True)
        (self.crate / "Cargo.lock").write_text('version = 3\n[[package]]\nname = "serde"\nversion = "1.0.0"\n'
                                               'source = "registry+https://github.com/rust-lang/crates.io-index"\n')
        text = manifest.rust(self.crate, self.root / "p")
        mainrs = str(self.crate / "src" / "main.rs").replace(" ", "\\ ")
        def dep(extra="", env="", lib_env=""):
            (target / "release" / "x.d").write_text(f"{target}/release/x: {mainrs} {tf}/traceforge/src/lib.rs "
                                                    f"{ch}/registry/src/i/serde-1.0.0/lib.rs {extra}\n")
            (target / "release" / "deps" / "x-0123.d").write_text(f"x: {mainrs}\n{env}")
            # rustc writes the crate's own prerequisites relative to the package root
            (target / "release" / "deps" / "xlib-0456.d").write_text(f"xlib: src/main.rs\n{lib_env}")
            # a library with the binary's name: its own <name>-<hash>.d, older than the binary's
            lib = target / "release" / "deps" / "x-0999.d"
            lib.write_text(f"x: src/main.rs\n{lib_env}")
            os.utime(lib, (1, 1))
            (target / "release" / "deps" / "serde-0789.d").write_text(f"serde: {ch}/registry/src/i/serde-1.0.0/lib.rs\n# env-dep:OUT_DIR=x\n")
        def check():
            with mock.patch.dict(os.environ, {"CARGO_HOME": str(ch)}):
                build.check_dep_info(target, ["x"], self.crate, [], text, tfiles)
        dep()
        check()
        dep(env="# env-dep:CARGO_PKG_NAME=x\n# env-dep:CARGO_CRATE_NAME=x\n")
        check()
        for kw, msg in ((dict(extra=str(self.crate / "extra.rs")), "extra.rs, which is not in the manifest"),
                        (dict(extra="/etc/passwd"), "neither the manifest"),
                        (dict(extra=f"{ch}/extra.rs"), "neither the manifest"),
                        (dict(extra=f"{ch}/registry/src/extra.rs"), "neither the manifest"),
                        (dict(extra=f"{ch}/registry/src/i/evil-9.9.9/lib.rs"), "neither the manifest"),
                        (dict(extra=f"{tf}/target/x.rs"), "neither the manifest"),
                        (dict(env="# env-dep:BENCH_MODE=a\n"), "BENCH_MODE"),
                        (dict(env="# env-dep:CARGO_MANIFEST_DIR=/p\n"), "CARGO_MANIFEST_DIR"),
                        (dict(lib_env="# env-dep:BENCH_MODE=a\n"), "unit x")):
            dep(**kw)
            self.assertRaisesRegex(build.BuildError, msg, check)

    def test_cargo_home_path_override_refused(self):
        ch = self.root / "cargo-home"
        ch.mkdir()
        (ch / "config.toml").write_text('paths = ["/somewhere"]\n')
        with mock.patch.dict(os.environ, {"CARGO_HOME": str(ch)}):
            self.assertRaisesRegex(build.BuildError, "overrides dependencies by path", self.check)
            (ch / "config.toml").write_text('[net]\noffline = true\n')
            self.assertEqual(list(self.check()), [str(ch.resolve() / "config.toml")])

    def test_patch_path_refused(self):
        self.toml('traceforge = { path = "../../tf/traceforge" }\n[patch.crates-io]\nserde = { path = "../serde" }')
        self.assertRaisesRegex(build.BuildError, "patch.crates-io.serde", self.check)


class Cache(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()
        self.env = mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(self.s.path / "cache")})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.s.cleanup()

    def test_relative_root_is_absolute(self):
        """Review S1-S6 A1: DAAVAR_CACHE=./driver-cache."""
        with mock.patch.dict(os.environ, {"DAAVAR_CACHE": "./driver-cache"}):
            self.assertTrue(cache.root().is_absolute())
            self.assertEqual(cache.root(), pathlib.Path(os.getcwd()) / "driver-cache")

    def test_read_only(self):
        d = cache.tmpdir(cache.ensure("artifacts"))
        (d / "out").mkdir()
        (d / "out" / "bin").write_text("x")
        os.chmod(d / "out" / "bin", 0o755)
        cache.make_readonly(d)
        final = cache.root() / "artifacts" / "k"
        cache.install(d, final)
        self.assertFalse(os.stat(final / "out" / "bin").st_mode & 0o222)
        self.assertTrue(os.stat(final / "out" / "bin").st_mode & 0o100)  # still executable
        with self.assertRaises(PermissionError):
            (final / "out" / "bin").write_text("y")
        with self.assertRaises(PermissionError):
            (final / "out" / "new").write_text("y")
        cache.remove(final)
        self.assertFalse(final.exists())

    def test_lock_serialises(self):
        order = []

        def worker(n):
            with cache.lock("k"):
                order.append(("in", n))
                time.sleep(0.2)
                order.append(("out", n))

        ts = [threading.Thread(target=worker, args=(n,)) for n in range(3)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        for k in range(0, 6, 2):
            self.assertEqual(order[k][0], "in")
            self.assertEqual(order[k + 1], ("out", order[k][1]))

    def test_key_is_canonical(self):
        self.assertEqual(cache.key_of({"a": 1, "b": [1, 2]}), cache.key_of({"b": [1, 2], "a": 1}))
        self.assertNotEqual(cache.key_of({"a": 1}), cache.key_of({"a": 2}))

    def test_env_subset(self):
        e = {"CARGO_HOME": "/c", "CARGO_TARGET_DIR": "/t", "RUSTFLAGS": "-C x", "PATH": "/bin", "CARGO_NET_OFFLINE": "true"}
        self.assertEqual(build.env_subset(e), {"CARGO_HOME": "/c", "CARGO_NET_OFFLINE": "true", "RUSTFLAGS": "-C x"})


class NoOp(unittest.TestCase):
    """The build steps stubbed: the first build runs cargo once, the second runs no build step at all."""

    def setUp(self):
        self.s = Scratch()
        self.env = mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(self.s.path / "cache")})
        self.env.start()
        self.proj = self.s.path / "proj"
        (self.proj / "tf" / "traceforge").mkdir(parents=True)
        (self.proj / "tf" / "traceforge" / "Cargo.toml").write_text("")
        shutil.copytree(FX / "rust", self.proj / "fx" / "rust")
        t = (self.proj / "fx" / "rust" / "Cargo.toml").read_text()
        (self.proj / "fx" / "rust" / "Cargo.toml").write_text(
            t.replace("../../../../../../traceforge/traceforge", "../../tf/traceforge"))
        (self.proj / "fx" / "daavar.toml").write_text(
            'name = "fx"\nmodel = "fifo"\n[programs.spec]\nkind = "spec"\nlang = "rust"\nside = "spec"\n'
            'args = ["spec", "ok"]\n[programs.rust]\nkind = "impl"\nlang = "rust"\nside = "impl"\n'
            'args = ["impl", "ok"]\n[scenarios.s]\nvisible = ["client", "server"]\nprograms = ["spec", "rust"]\n'
            'directions = ["rust<=spec"]\ncells = [{ label = "ok" }]\n[tools]\ntraceforge = "../tf"\n')
        self.calls = []

    def tearDown(self):
        self.env.stop()
        self.s.cleanup()

    def fake_run(self, step, argv, cwd, env, logdir):
        self.calls.append((step, list(argv)))
        target = pathlib.Path(env["CARGO_TARGET_DIR"]) / "release"
        target.mkdir(parents=True, exist_ok=True)
        for n in ("tf_fx", "conf-join"):
            (target / n).write_text(f"binary {n}")
        crate = pathlib.Path(cwd)
        (target / "tf_fx.d").write_text(f"{target}/tf_fx: {crate}/src/main.rs {crate}/src/msgs.rs\n")
        return {"argv": argv, "exit": 0}

    def test_second_build_is_a_noop(self):
        cfg = config.load(self.proj / "fx")
        tid = build.ToolId("traceforge", (self.proj / "tf").resolve(), "c" * 40, False)
        with mock.patch.object(build, "_run_build", self.fake_run), \
                mock.patch.object(build, "_version", lambda argv, cwd=None: " ".join(argv) + " 1.0"), \
                mock.patch.object(build, "tool_identity", lambda *a, **k: tid), \
                mock.patch.object(build, "tracked_files", lambda c: set()):
            r1 = build.build(cfg)
            self.assertEqual([c[0] for c in self.calls], ["conf-join", "build"])
            self.assertTrue(r1.built_anything)
            art = pathlib.Path(r1.artifacts["rust"]["out"])
            m = (art / "tf_fx.SOURCE.md5").read_text()
            want = (CAPTURED / "fx-rust.SOURCE.md5").read_text().splitlines()
            self.assertEqual(m.splitlines()[0], "# root project:rust")  # the scratch copy is outside the checkout
            self.assertEqual([l for l in m.splitlines()[1:] if "Cargo.toml" not in l],
                             [l for l in want[1:] if "Cargo.toml" not in l])  # this test rewrote Cargo.toml
            self.assertGreaterEqual(os.stat(art / "tf_fx.SOURCE.md5").st_mtime_ns, os.stat(art / "tf_fx").st_mtime_ns)
            self.assertFalse(os.stat(art / "tf_fx").st_mode & 0o222)
            b = json.loads((art.parent / "build.json").read_text())
            self.assertEqual(b["key_inputs"]["tool"], "c" * 40)
            r2 = build.build(cfg)
            self.assertEqual(len(self.calls), 2, "the second build ran a build step")
            self.assertFalse(r2.built_anything)
            self.assertEqual(r2.artifacts["rust"]["key"], r1.artifacts["rust"]["key"])
            # A source change is a new key and one new build.
            p = self.proj / "fx" / "rust" / "src" / "msgs.rs"
            p.write_text(p.read_text() + "\n// changed\n")
            r3 = build.build(cfg)
            self.assertEqual([c[0] for c in self.calls], ["conf-join", "build", "build"])
            self.assertNotEqual(r3.artifacts["rust"]["key"], r1.artifacts["rust"]["key"])

    def test_dry_run_runs_nothing(self):
        cfg = config.load(self.proj / "fx")
        with mock.patch.object(build.runner, "run", side_effect=AssertionError("ran")), \
                mock.patch.object(build.runner, "capture", side_effect=AssertionError("ran")):
            code, cmds = build.dry_run(cfg)
        self.assertEqual(code, 0)
        self.assertEqual(cmds[0]["argv"], ["cargo", "build", "--release", "--locked", "-p", "traceforge", "--bin", "conf-join"])
        self.assertEqual(cmds[-1]["argv"], ["cargo", "build", "--release", "--locked"])


class JmcStaged(unittest.TestCase):
    """Review S1-S6 A4: Gradle runs in a staged copy inside the cache; the checkout is not written."""

    def test_checkout_untouched(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        co = s.path / "jmc"
        (co / "core" / "src").mkdir(parents=True)
        (co / "gradlew").write_text("#!/bin/sh\n")
        (co / "core" / "src" / "A.java").write_text("class A {}")
        (co / ".git").mkdir()
        before = sorted(p.relative_to(co).as_posix() for p in co.rglob("*"))
        seen = []
        jar = s.path / "gradle-cache" / "x.jar"  # an external classpath jar, outside the tool directory
        jar.parent.mkdir()
        jar.write_bytes(b"PK jar")

        def fake(step, argv, cwd, env, logdir):
            seen.append(pathlib.Path(cwd))
            out = pathlib.Path(cwd) / "core" / "build" / "classes" / "java" / "test"
            out.mkdir(parents=True, exist_ok=True)
            (out / "T.class").write_text("x")
            (pathlib.Path(cwd) / ".gradle").mkdir(exist_ok=True)
            for a in argv:
                if a.startswith("-PdaavarCp="):
                    pathlib.Path(a.split("=", 1)[1]).write_text(f"{out}:{jar}")
            return {"exit": 0, "cwd": str(cwd), "argv": list(argv)}

        res = build.BuildResult()
        with mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(s.path / "cache")}), \
                mock.patch.object(build, "_run_build", fake):
            t = build.jmc_tool(build.ToolId("jmc", co, "j" * 40, False), res)
        # Astra 5: both Gradle steps recorded with the staged working directory; the external jar hashed.
        self.assertEqual([st.name for st in res.steps], ["jmc-testClasses", "jmc-classpath"])
        self.assertTrue(all(st.cwd == str(seen[0]) and "/tools/.tmp-" in st.cwd and st.cwd.endswith("/src")
                            for st in res.steps), [st.cwd for st in res.steps])
        tj = json.loads((pathlib.Path(t["dir"]) / "tool.json").read_text())
        self.assertEqual(tj["jars_sha256"], {str(jar): build.cache.sha256_file(jar)})
        self.assertEqual(tj["build_cwd"], str(seen[0]))
        self.assertEqual(tj["classes_sha256"], build.cache.tree_digest(pathlib.Path(t["dir"]) / "tree"))
        self.assertEqual(sorted(p.relative_to(co).as_posix() for p in co.rglob("*")), before)
        self.assertTrue(all(str(c).startswith(str(s.path / "cache")) for c in seen))
        self.assertTrue(t["classpath"][0].startswith(str(s.path / "cache" / "tools")))
        self.assertTrue(pathlib.Path(t["classpath"][0], "T.class").is_file())
        self.assertEqual(t["classpath"][1], str(jar))


class GitFiles(unittest.TestCase):
    def test_head_without_a_process(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        g = s.path / "repo" / ".git"
        (g / "refs" / "heads").mkdir(parents=True)
        (g / "HEAD").write_text("ref: refs/heads/main\n")
        (g / "refs" / "heads" / "main").write_text("a" * 40 + "\n")
        self.assertEqual(build.git_head_nofork(s.path / "repo"), "a" * 40)
        (g / "refs" / "heads" / "main").unlink()
        (g / "packed-refs").write_text("# pack-refs\n" + "b" * 40 + " refs/heads/main\n")
        self.assertEqual(build.git_head_nofork(s.path / "repo"), "b" * 40)
        (s.path / "sub").mkdir()
        (s.path / "sub" / ".git").write_text("gitdir: ../repo/.git\n")
        self.assertEqual(build.git_head_nofork(s.path / "sub"), "b" * 40)


if __name__ == "__main__":
    unittest.main()
