"""Regression tests of the final reviews (Astra driver-final-review-01, Opus REVIEW-driver-final-opus)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))  # the daavar checkout: package daavar

import contextlib
import copy
import io
import json
import os
import shutil
import subprocess
import tomllib
import unittest
from unittest import mock

from daavar import DaavarError, build, check, config, provenance, replay, report, scaffold
from daavar import __main__ as M
from daavar.tests.util import DAAVAR, FIXTURES, Scratch

GITENV = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}


def git(d, *a):
    subprocess.run(["git", "-C", str(d), *a], check=True, env=GITENV, stdin=subprocess.DEVNULL,
                   stdout=subprocess.DEVNULL)


def run_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = M.main(argv)
    return code, out.getvalue(), err.getvalue()


class Replay(unittest.TestCase):
    """Astra 2 / Opus 6: the rebuilt artifact's digest is checked; only the Rust D15 case passes unflagged."""

    def setUp(self):
        self.s = Scratch()
        self.run = self.s.path / "out" / "r"
        self.run.mkdir(parents=True)

    def tearDown(self):
        self.s.cleanup()

    def rebuild(self, lang, new_art, new_jmc=None, **allow):
        prov = {"artifacts": {"rust": {"key": "k", "sha256": {"b": "1", "b.SOURCE.md5": "m"}},
                              "java": {"key": "j", "sha256": "J"}},
                "tools": {"jmc": {"identity": "i", "classes_sha256": "c", "jars_sha256": "x"}}}
        res = build.BuildResult()
        res.artifacts = {lang: new_art}
        d = self.s.path / "jt"
        (d / "tree").mkdir(parents=True, exist_ok=True)
        (d / "tool.json").write_text(json.dumps({"jars_sha256": {}}))
        res.tools = {"jmc": {"identity": (new_jmc or {}).get("identity", "i"), "dir": str(d), "classpath": []}}
        loc = {"project": self.s.path, "daavar": None}
        with mock.patch.object(provenance, "restore", lambda *a: loc), \
                mock.patch.object(config, "load", lambda p: None), \
                mock.patch.object(build, "build", lambda *a, **k: res), \
                mock.patch.object(replay.cache, "tree_digest", lambda p: (new_jmc or {}).get("classes", "c")), \
                mock.patch.object(replay.cache, "key_of", lambda o: (new_jmc or {}).get("jars", "x")):
            return replay._rebuild(self.run, prov, lang, **allow)

    def test_rust_d15_passes(self):
        r = self.rebuild("rust", {"key": "k", "sha256": {"b": "2", "b.SOURCE.md5": "m"}})
        self.assertEqual(r["artifact"]["key"], "k")

    def test_rust_other_key_or_manifest_refused(self):
        with self.assertRaisesRegex(DaavarError, "allow-source-change"):
            self.rebuild("rust", {"key": "k2", "sha256": {"b": "2", "b.SOURCE.md5": "m"}})
        with self.assertRaisesRegex(DaavarError, "SOURCE.md5 differs"):
            self.rebuild("rust", {"key": "k", "sha256": {"b": "1", "b.SOURCE.md5": "m2"}})

    def test_java_mismatch_refused_unless_allowed(self):
        with self.assertRaisesRegex(DaavarError, "Java classes differ"):
            self.rebuild("java", {"key": "j", "sha256": "J2"})
        self.rebuild("java", {"key": "j", "sha256": "J2"}, allow_source=True)
        with self.assertRaisesRegex(DaavarError, "allow-tool-change"):
            self.rebuild("java", {"key": "j", "sha256": "J"}, new_jmc={"classes": "c2"})

    def test_corrupted_standalone_cache_refused(self):
        g = FIXTURES / "cases" / "join-tf-bug" / "files" / "j" / "graphs" / "1.graph.json"
        gj = replay._graph(g)
        with mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(self.s.path / "cache")}):
            d = self.s.path / "cache" / "artifacts" / ("a" * 64)
            (d / "out").mkdir(parents=True)
            (d / "out" / gj["run"]["entry"]).write_text("tampered")
            (d / "build.json").write_text(json.dumps({"key": "a" * 64, "manifest": "# root x\n",
                                                      "sha256": {gj["run"]["entry"]: "0" * 64},
                                                      "key_inputs": {"kind": "rust", "tool": gj["run"]["tool_commit"],
                                                                     "bins": [gj["run"]["entry"]]}}))
            with self.assertRaisesRegex(DaavarError, "does not match its recorded digest"):
                replay.resolve(g, gj)


class StandaloneJava(unittest.TestCase):
    """Astra confirmation 1: a standalone Java replay verifies the cached JMC tool's classes and jars."""

    def test_tool_mismatch_refused_unless_allowed(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        with mock.patch.dict(os.environ, {"DAAVAR_CACHE": str(s.path / "cache")}):
            art = s.path / "cache" / "artifacts" / ("b" * 64)
            (art / "out").mkdir(parents=True)
            (art / "out" / "A.class").write_text("x")
            (art / "build.json").write_text(json.dumps({
                "key": "b" * 64, "manifest": "# root x\n", "sha256": replay.cache.tree_digest(art / "out"),
                "key_inputs": {"kind": "java", "tool": "j" * 40}}))
            td = s.path / "cache" / "tools" / ("jmc-" + "j" * 40)
            (td / "tree").mkdir(parents=True)
            (td / "tree" / "C.class").write_text("c")
            jar = s.path / "x.jar"
            jar.write_text("jar")
            (td / "tool.json").write_text(json.dumps({"classpath": [str(jar)],
                                                      "classes_sha256": replay.cache.tree_digest(td / "tree"),
                                                      "jars_sha256": {str(jar): replay.cache.sha256_file(jar)}}))
            g = {"witness": 1, "run": {"tool": "jmc", "tool_commit": "j" * 40, "entry": "p.A", "args": [],
                                       "source": "unknown"}}
            gp = s.path / "g.graph.json"
            gp.write_text(json.dumps(g) + "\n")
            self.assertEqual(replay.resolve(gp, g)["lang"], "java")
            jar.write_text("tampered")
            with self.assertRaisesRegex(DaavarError, "allow-tool-change"):
                replay.resolve(gp, g)
            self.assertEqual(replay.resolve(gp, g, allow_tool=True)["lang"], "java")
            jar.write_text("jar")
            (td / "tree" / "C.class").write_text("changed")
            with self.assertRaisesRegex(DaavarError, "JMC classes differ"):
                replay.resolve(gp, g)


class FromRunOptions(unittest.TestCase):
    def test_options_refused_and_dry_run_plan(self):
        """Astra confirmation 3: --from-run refuses --no-build, --strict, --no-wait; its dry run lists commands."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        R = s.path / "R"
        (R / "inputs").mkdir(parents=True)
        (R / "provenance.json").write_text(json.dumps({
            "project": {"case": "dirty", "repo": "/repo", "commit": "c" * 40, "root": "/repo/p", "files": {}},
            "tools": {"traceforge": {"checkout": "/tf", "commit": "t" * 40, "dirty": False}}}))
        for flag in ("--no-build", "--strict", "--no-wait", "--allow-dirty-tools"):
            code, out, err = run_main(["check", "--from-run", str(R), flag])
            self.assertEqual(code, 2, flag)
            self.assertIn(flag[2:], err)
        code, out, err = run_main(["check", "--from-run", str(R), "--into", str(s.path / "D"), "--dry-run", "--json"])
        cmds = json.loads(out)["result"]["commands"]
        self.assertEqual(code, 0)
        self.assertIn(["git", "checkout", "-q", "--detach", "c" * 40], [c["argv"] for c in cmds])
        self.assertTrue(any("diff.patch" in " ".join(c["argv"]) for c in cmds))
        self.assertFalse((s.path / "D").exists())


class Verify(unittest.TestCase):
    def test_conf_join_needs_a_move_and_the_same_inputs(self):
        """Astra 3."""
        base = {"artifacts": {}, "tools": {"traceforge": {"identity": "t", "checkout": "/a", "conf_join_sha256": "c",
                                                           "conf_join_inputs": {"rustc": "r", "cargo": "c", "env": {},
                                                                                "target": "/x"}}}}
        n = copy.deepcopy(base)
        n["tools"]["traceforge"]["conf_join_sha256"] = "d"
        cj = lambda nn: {x["name"]: x for x in provenance.verify_artifacts(base, nn, [])}["tool:conf-join"]
        self.assertFalse(cj(n)["allowed"])  # same location
        n["tools"]["traceforge"]["conf_join_inputs"]["target"] = "/y"
        self.assertTrue(cj(n)["allowed"])
        n["tools"]["traceforge"]["conf_join_inputs"]["rustc"] = "r2"
        self.assertFalse(cj(n)["allowed"])  # other compiler


@unittest.skipUnless(shutil.which("git"), "git")
class Git(unittest.TestCase):
    def setUp(self):
        self.s = Scratch()

    def tearDown(self):
        self.s.cleanup()

    def test_classifier_only_edit_is_dirty(self):
        """Astra 4: a tracked classifier edited, or a new untracked one."""
        g = self.s.path / "g"
        (g / "p" / "rust" / "src").mkdir(parents=True)
        (g / "p" / "daavar.toml").write_text("x")
        (g / "p" / "cl.py").write_text("print(1)")
        (g / "p" / "rust" / "src" / "main.rs").write_text("fn main(){}")
        git(g, "init", "-q")
        git(g, "add", "-A")
        git(g, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x")

        class Cfg:
            root = g / "p"
            classify = {"command": "python3 cl.py"}
        (g / "p" / "cl.py").write_text("print(2)")
        inputs = self.s.path / "i1"
        inputs.mkdir()
        rec = provenance.project_inputs(g / "p", inputs, Cfg)
        self.assertEqual(rec["case"], "dirty")
        self.assertIn(b"cl.py", (inputs / "diff.patch").read_bytes())
        git(g / "p", "checkout", "-q", "--", "cl.py")
        Cfg.classify = {"command": "python3 new.py"}
        (g / "p" / "new.py").write_text("print(3)")
        inputs = self.s.path / "i2"
        inputs.mkdir()
        rec = provenance.project_inputs(g / "p", inputs, Cfg)
        self.assertEqual((rec["case"], rec["untracked"]), ("dirty", ["p/new.py"]))

    def test_generated_cp_txt_is_not_dirty(self):
        """Opus 2: the classpath helper's untracked cp.txt at the JMC checkout's top."""
        j = self.s.path / "jmc"
        j.mkdir()
        (j / "f").write_text("x")
        git(j, "init", "-q")
        git(j, "add", "-A")
        git(j, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "x")
        (j / "cp.txt").write_text("a:b")
        self.assertFalse(build.tool_identity("jmc", j).dirty)
        with self.assertRaisesRegex(build.BuildError, "dirty"):
            build.tool_identity("traceforge", j)
        (j / "f").write_text("y")
        with self.assertRaisesRegex(build.BuildError, "dirty"):
            build.tool_identity("jmc", j)


class DryRuns(unittest.TestCase):
    """Astra 6 / Opus 4: the command lines, no process."""

    def test_check_plan(self):
        no = mock.patch.object(check.runner, "run", side_effect=AssertionError("ran"))
        no2 = mock.patch.object(check.runner, "capture", side_effect=AssertionError("ran"))
        with no, no2, mock.patch.object(build.runner, "capture", side_effect=AssertionError("ran")):
            code, out, err = run_main(["check", str(DAAVAR / "examples" / "kv-cancel"), "--dry-run"])
        self.assertEqual((code, out.strip()), (0, "daavar check: exit 0 DRY-RUN"))
        self.assertIn("conf-join", err)
        self.assertIn("export spec --out 'out/<run>/one.impl/exports/spec.jsonl'", err)
        self.assertIn("MpLauncher bench.kv_cancel.KvCancel:impl must 0 1 report", err)
        self.assertIn("witness-join.sh 'out/<run>/one.mutant/exports/spec.jsonl' 'out/<run>/one.mutant/exports/mutant1.jsonl'", err)
        code, out, err = run_main(["check", str(DAAVAR / "examples" / "kv-cancel"), "--dry-run", "--json",
                                   "--mode", "online"])
        cmds = json.loads(out)["result"]["commands"]
        self.assertTrue(any(c["step"].startswith("online one.impl rust") and "online" in c["argv"] for c in cmds))

    def test_new_dry_run_validates_and_writes_nothing(self):
        s = Scratch()
        self.addCleanup(s.cleanup)
        code, out, err = run_main(["new", "x", "--spec", "java", "--dry-run"])
        self.assertEqual(code, 2)
        code, out, err = run_main(["new", "my-x", "--impl", "rust,java", "--dir", str(s.path / "p"), "--dry-run"])
        self.assertEqual(code, 0)
        self.assertIn("java/bench/my_x/MyX.java", err)
        self.assertFalse((s.path / "p").exists())

    def test_render_plan(self):
        g = FIXTURES / "cases" / "join-tf-bug" / "files" / "j" / "graphs"
        cmds = replay.render_plan(g, "tikz", None)
        self.assertEqual([c["argv"][3] for c in cmds], ["--check", "--format"])


class RenderRun(unittest.TestCase):
    def test_empty_graph_dirs_skipped(self):
        """Opus r2 N1: a conforming direction's empty graphs/ is skipped, the others rendered."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        run = s.path / "out" / "r"
        (run / "c" / "joins" / "a-in-s" / "graphs").mkdir(parents=True)
        shutil.copytree(FIXTURES / "cases" / "join-tf-bug" / "files" / "j" / "graphs", run / "c" / "joins" / "b-in-s" / "graphs")
        (run / "provenance.json").write_text("{}")
        code, out, cmds = replay.render(run, "dot", s.path / "o")
        self.assertEqual(code, 0)
        self.assertTrue(any((s.path / "o").glob("*.dot")))
        (run / "c" / "joins" / "b-in-s" / "graphs" / "1.graph.json").unlink()
        with self.assertRaisesRegex(DaavarError, "no witness graphs"):
            replay.render(run, "dot", s.path / "o2")

    def test_into_without_from_run_refused(self):
        """Opus r2 12."""
        code, out, err = run_main(["check", str(DAAVAR / "examples" / "kv-cancel"), "--into", "/x"])
        self.assertEqual(code, 2)


class Scaffold(unittest.TestCase):
    def test_java_keyword_names_refused(self):
        """Astra 7 / Opus 9."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        for n in ("class", "int", "true", "for", "new"):
            with self.assertRaisesRegex(DaavarError, "reserved word in Java"):
                scaffold.new(n, "java", dest=s.path / n)
        scaffold.new("class", "rust", dest=s.path / "class-rust", warn=lambda m: None)  # no Java: allowed


class CheckRefusals(unittest.TestCase):
    def test_refused_check_writes_verdict(self):
        """Opus 3."""
        s = Scratch()
        self.addCleanup(s.cleanup)
        p = s.path / "kv"
        shutil.copytree(DAAVAR / "examples" / "kv-cancel", p, ignore=shutil.ignore_patterns("out", "target"))
        t = (p / "daavar.toml").read_text().replace('"../../traceforge"', json.dumps(str(DAAVAR / "traceforge"))) \
            .replace('"../../jmc"', json.dumps(str(DAAVAR / "jmc")))
        (p / "daavar.toml").write_text(t)
        with mock.patch.object(build, "build", side_effect=build.BuildError("the jmc checkout is dirty")):
            code, out, err = run_main(["check", str(p)])
        self.assertEqual(code, 2)
        run = pathlib.Path(out.split()[-1])
        self.assertTrue((run / "VERDICT.md").read_text().startswith("# kv-cancel: REFUSED"))
        self.assertEqual(json.loads((run / "verdict.json").read_text())["error"], "the jmc checkout is dirty")
        self.assertEqual(os.readlink(p / "out" / "latest"), run.name)

    def test_online_side_spec_refused(self):
        """Opus 5."""
        r = tomllib.loads((DAAVAR / "examples" / "kv-cancel" / "daavar.toml").read_text())
        r["programs"]["m2"] = {"kind": "mutant", "reference": "spec", "lang": "rust", "side": "spec", "args": ["spec"],
                               "cells": ["mutant"]}
        r["scenarios"]["one"]["programs"].append("m2")
        r["scenarios"]["one"]["directions"].append("m2<=spec")
        cfg = config.from_dict(r, DAAVAR / "examples" / "kv-cancel")
        with self.assertRaisesRegex(DaavarError, 'runs side "spec"'):
            check.Check(cfg, check.Options(mode="online")).select()
        check.Check(cfg, check.Options()).select()  # offline: fine


if __name__ == "__main__":
    unittest.main()
